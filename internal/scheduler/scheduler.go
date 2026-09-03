// Package scheduler implements a work-stealing task scheduler: a fixed
// pool of workers, each with its own lock-free local deque, that steal
// work from one another when their own deque runs dry.
//
// Scheduled functions are named per resource kind (see kinds.go: Kind,
// LLMFunc, FetchFunc, ProxyFetchFunc, ChromedpFunc, TwoCaptchaFunc) and
// run through Bulkhead, which owns one Scheduler per Kind so a burst
// against one resource can never starve another (see bulkhead.go). The
// underlying generic Func[T]/Submit machinery in this file is what each
// named kind is a thin wrapper over; it remains exported for ad-hoc use
// outside the known kinds.
//
// Within a single Scheduler, submitted tasks carry a Priority.
// PriorityStandard is served first; PriorityBackground is served after
// it, except that a PriorityBackground task waiting past the
// Scheduler's configured aging threshold is served immediately —
// bounding its worst-case wait regardless of PriorityStandard backlog.
package scheduler

import (
	"context"
	"errors"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ErrStopped is returned via Result.Err by Submit when the scheduler
// has already been (or is concurrently being) stopped, since a task
// accepted after that point would otherwise sit in a queue forever with
// no worker left to run it.
var ErrStopped = errors.New("scheduler: stopped")

// ErrQueueFull is returned via Result.Err by Submit when priority's
// shared queue is already at its configured depth cap
// (Config.MaxStandardQueueDepth or Config.MaxBackgroundQueueDepth).
// This stays a single package-level sentinel rather than one instance
// per Kind or per queue: the caller already knows which Kind and
// Priority it submitted with — it chose the SubmitXxx function and the
// Priority argument itself — so there's no ambiguity a bespoke error
// value would resolve. Wrap it with fmt.Errorf/%w at the call site if a
// caller wants that context attached to the error string.
var ErrQueueFull = errors.New("scheduler: queue full")

// defaultLocalQueueCap is the fixed capacity of each worker's local
// deque. It must be a power of two (see deque.pushBottom). Work beyond
// this per worker spills to the shared queues rather than blocking.
const defaultLocalQueueCap = 256

// queueCap is the starting capacity of both the standard queue and the
// background (aging) queue; each grows on demand.
const queueCap = 64

// taskNode is the scheduler's internal, type-erased unit of work: a
// closure that already captures the caller's Func[T] and writes its
// result into the matching Future[T]. Erasing T here lets the deque
// and the queues — the hot, highly concurrent paths — stay non-generic.
type taskNode struct {
	run func(ctx context.Context)
}

// Func is a unit of schedulable work. It underlies the named per-kind
// function types in kinds.go; use those where the kind is known, and
// Func directly only for ad-hoc scheduling outside the known kinds.
type Func[T any] func(ctx context.Context) (T, error)

// Result is a Func's outcome.
type Result[T any] struct {
	Value T
	Err   error
}

// Future is a handle to a still-running (or already-finished) Func's
// result.
type Future[T any] struct {
	done chan struct{}
	res  Result[T]
}

// Wait blocks until the task completes and returns its result.
func (f *Future[T]) Wait() Result[T] {
	<-f.done
	return f.res
}

// Stats is a snapshot of scheduler activity, read via atomics so it is
// safe to call from any goroutine at any time.
type Stats struct {
	Submitted int64
	Completed int64
	Stolen    int64
	// Aged counts tasks served because they had waited past the aging
	// threshold, rather than through the normal priority order. A
	// consistently high count relative to Submitted is a signal that
	// AgingThreshold or the pool's worker count needs retuning.
	Aged int64
	// RejectedStandard counts PriorityStandard tasks refused with
	// ErrQueueFull because the standard queue was already at
	// Config.MaxStandardQueueDepth. Any non-zero count here is a signal
	// that arrivals are outpacing this pool's drain rate, not just a
	// transient blip — a capped queue rejects instead of growing
	// unbounded, so this is the visibility that trade gives up.
	RejectedStandard int64
	// RejectedBackground is the same signal for PriorityBackground
	// tasks refused against Config.MaxBackgroundQueueDepth.
	RejectedBackground int64
}

// Config configures a Scheduler.
type Config struct {
	// Workers is the number of worker goroutines. <=0 ties the pool
	// size to runtime.NumCPU, the right default for CPU-bound work;
	// IO-bound Funcs should pass a larger explicit count matched to the
	// backing resource's real capacity (see the package doc on Kind).
	Workers int

	// AgingThreshold bounds how long a PriorityBackground task can wait
	// once queued, regardless of PriorityStandard backlog. <=0 disables
	// aging: PriorityBackground tasks are then served only once the
	// Standard queue is empty, with no promotion and no bound.
	AgingThreshold time.Duration

	// AgingJitter, if > 0, subtracts an independent random amount in
	// [0, AgingJitter] from each task's own aging deadline, so a batch
	// of tasks pushed at once doesn't all cross the threshold in the
	// same instant (see agingQueue.push). It only ever pulls a deadline
	// earlier, never later, so AgingThreshold's bound is never widened.
	// Clamped to AgingThreshold if larger.
	AgingJitter time.Duration

	// MaxStandardQueueDepth caps how many PriorityStandard tasks may sit
	// in the standard queue at once — this also covers local-deque
	// overflow spill, which lands in the same queue (see schedule).
	// <=0 means unbounded, matching this scheduler's original behavior.
	// Once the cap is reached, Submit resolves the new task's Future
	// immediately with ErrQueueFull instead of enqueuing it, trading an
	// explicit rejection for the unbounded memory growth an
	// unconstrained queue would otherwise be exposed to under sustained
	// overload.
	MaxStandardQueueDepth int

	// MaxBackgroundQueueDepth is the equivalent cap for the background
	// (aging) queue, configured independently from
	// MaxStandardQueueDepth since a PriorityBackground caller
	// (deliberately infrequent, larger batches) may legitimately need
	// to queue far deeper than PriorityStandard traffic ever should
	// before draining. <=0 means unbounded.
	MaxBackgroundQueueDepth int
}

// Scheduler is a fixed-size pool of work-stealing workers. The zero
// value is not usable; construct one with New.
type Scheduler struct {
	workers    []*worker
	standard   *injector
	background *agingQueue
	wake       chan struct{}
	stop       chan struct{}
	wg         sync.WaitGroup

	// maxStandardDepth and maxBackgroundDepth are copied from Config at
	// construction and never change afterward, so schedule can read
	// them without synchronization.
	maxStandardDepth   int
	maxBackgroundDepth int

	stopped            atomic.Bool
	submitted          atomic.Int64
	completed          atomic.Int64
	stolen             atomic.Int64
	aged               atomic.Int64
	rejectedStandard   atomic.Int64
	rejectedBackground atomic.Int64
}

// New starts a Scheduler per cfg. See Config for field semantics.
func New(cfg Config) *Scheduler {
	numWorkers := cfg.Workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	jitter := cfg.AgingJitter
	if jitter > cfg.AgingThreshold {
		jitter = cfg.AgingThreshold
	}

	s := &Scheduler{
		standard:           newInjector(queueCap),
		background:         newAgingQueue(queueCap, cfg.AgingThreshold, jitter, rand.Uint64(), rand.Uint64()),
		wake:               make(chan struct{}, numWorkers),
		stop:               make(chan struct{}),
		maxStandardDepth:   cfg.MaxStandardQueueDepth,
		maxBackgroundDepth: cfg.MaxBackgroundQueueDepth,
	}

	s.workers = make([]*worker, numWorkers)
	for i := range s.workers {
		s.workers[i] = &worker{
			id:  i,
			s:   s,
			dq:  newDeque(defaultLocalQueueCap),
			rng: rand.New(rand.NewPCG(rand.Uint64(), uint64(i))),
		}
	}

	s.wg.Add(numWorkers)
	for _, w := range s.workers {
		go w.run()
	}
	return s
}

// Submit schedules fn at priority and returns a Future for its result.
// Submit is a free function rather than a method because Go methods
// cannot carry their own type parameters, and each call site may
// schedule a different T against the same, non-generic Scheduler.
//
// If ctx is a context handed to a currently-running task on one of s's
// workers (i.e. fn is spawning a child task), the child is pushed onto
// that worker's own local deque so it can run without contention and
// still be stolen by an idle peer, and priority is ignored — there is
// no queue contention to arbitrate on that path. Otherwise priority
// selects which of s's two shared queues fn lands on.
func Submit[T any](ctx context.Context, s *Scheduler, priority Priority, fn Func[T]) *Future[T] {
	fut := &Future[T]{done: make(chan struct{})}
	if s.stopped.Load() {
		fut.res = Result[T]{Err: ErrStopped}
		close(fut.done)
		return fut
	}

	node := &taskNode{run: func(ctx context.Context) {
		v, err := fn(ctx)
		fut.res = Result[T]{Value: v, Err: err}
		// Completed must be visible to any goroutine that has already
		// observed fut.done closed — bump it before closing, not after
		// worker.run's node.run(ctx) call returns, or Stats().Completed
		// can under-count relative to what Future.Wait() already
		// reported as finished.
		s.completed.Add(1)
		close(fut.done)
	}}
	if !s.schedule(ctx, priority, node) {
		fut.res = Result[T]{Err: ErrQueueFull}
		close(fut.done)
	}
	return fut
}

// schedule routes n to the calling worker's own deque when possible,
// falling back to priority's shared queue, and rings the wake doorbell
// on success. This applies even for a local push: idle workers that
// have already parked only wake on this signal, and have no other way
// to learn that a busy peer just grew its local deque and become
// stealable. Without it, a worker fanning out local work while every
// peer is parked deadlocks — nothing left to wake them.
//
// It reports whether n was actually accepted. false means priority's
// shared queue was already at its configured depth cap
// (Config.MaxStandardQueueDepth / MaxBackgroundQueueDepth); the local
// deque itself has no cap-driven rejection path, since a full local
// deque simply spills to the shared queue instead of failing. On false,
// Submit resolves n's Future with ErrQueueFull itself — n is not
// retried or queued anywhere.
func (s *Scheduler) schedule(ctx context.Context, priority Priority, n *taskNode) bool {
	s.submitted.Add(1)

	if w, ok := workerFromContext(ctx); ok && w.s == s {
		if w.dq.pushBottom(n) {
			s.ringWake()
			return true
		}
		// Local deque full; spill to the shared queues below.
	}

	var ok bool
	if priority == PriorityBackground {
		ok = s.background.push(n, time.Now(), s.maxBackgroundDepth)
	} else {
		ok = s.standard.push(n, s.maxStandardDepth)
	}
	if !ok {
		if priority == PriorityBackground {
			s.rejectedBackground.Add(1)
		} else {
			s.rejectedStandard.Add(1)
		}
		return false
	}
	s.ringWake()
	return true
}

// ringWake wakes one parked worker, if any; it is a no-op if the
// doorbell is already full, since that only means a wake is already
// pending for some other worker to consume.
func (s *Scheduler) ringWake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Stop signals every worker to exit once it finishes the task it is
// currently running, and blocks until they have all exited. Any task
// still sitting in a local deque or a shared queue at that point is
// abandoned — Stop is a shutdown, not a drain. Submit calls that lose
// the race with Stop (concurrent with, or after, this call) resolve
// their Future immediately with ErrStopped instead of being enqueued
// to a pool that will never drain them.
func (s *Scheduler) Stop() {
	s.stopped.Store(true)
	close(s.stop)
	s.wg.Wait()
}

// Stats returns a snapshot of scheduler activity.
func (s *Scheduler) Stats() Stats {
	return Stats{
		Submitted:          s.submitted.Load(),
		Completed:          s.completed.Load(),
		Stolen:             s.stolen.Load(),
		Aged:               s.aged.Load(),
		RejectedStandard:   s.rejectedStandard.Load(),
		RejectedBackground: s.rejectedBackground.Load(),
	}
}
