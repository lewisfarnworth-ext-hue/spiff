package scheduler

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestPool builds a Scheduler and its workers *without* starting any
// run goroutines, so a test can drive findTask/steal/schedule
// deterministically instead of racing the live pool for every task it
// enqueues. threshold <= 0 makes every background push immediately aged.
func newTestPool(workers int, threshold time.Duration) *Scheduler {
	s := &Scheduler{
		standard:   newInjector(queueCap),
		background: newAgingQueue(queueCap, threshold, 0, 1, 2),
		wake:       make(chan struct{}, workers),
		stop:       make(chan struct{}),
	}
	s.workers = make([]*worker, workers)
	for i := range s.workers {
		s.workers[i] = &worker{
			id:  i,
			s:   s,
			dq:  newDeque(defaultLocalQueueCap),
			rng: rand.New(rand.NewPCG(1, uint64(i))),
		}
	}
	return s
}

func TestWorker_ContextRoundTrip(t *testing.T) {
	s := newTestPool(2, time.Hour)
	w := s.workers[1]

	got, ok := workerFromContext(w.context(context.Background()))
	if !ok || got != w {
		t.Fatalf("workerFromContext() = %p, %v, want %p, true", got, ok, w)
	}
}

func TestWorkerFromContext_UntaggedContext(t *testing.T) {
	got, ok := workerFromContext(context.Background())
	if ok || got != nil {
		t.Fatalf("workerFromContext(Background()) = %p, %v, want nil, false", got, ok)
	}
}

// TestWorker_FindTaskPrefersLocalDeque pins the first rung of findTask's
// order: the cache-hot local deque wins even when both shared queues have
// work waiting.
func TestWorker_FindTaskPrefersLocalDeque(t *testing.T) {
	s := newTestPool(1, 0)
	w := s.workers[0]

	local, std, bg := noopNode(), noopNode(), noopNode()
	s.standard.push(std)
	s.background.push(bg, time.Now())
	w.dq.pushBottom(local)

	if got := w.findTask(); got != local {
		t.Fatalf("findTask() = %p, want the local deque's task %p", got, local)
	}
}

// TestWorker_FindTaskAgedBeforeStandard pins the aging promotion: a
// background task past its deadline outranks queued standard work.
func TestWorker_FindTaskAgedBeforeStandard(t *testing.T) {
	s := newTestPool(1, 0) // threshold 0: every push is aged on arrival
	w := s.workers[0]

	std, bg := noopNode(), noopNode()
	s.standard.push(std)
	s.background.push(bg, time.Now())

	if got := w.findTask(); got != bg {
		t.Fatalf("findTask() = %p, want the aged background task %p", got, bg)
	}
	if n := s.Stats().Aged; n != 1 {
		t.Fatalf("Stats().Aged = %d, want 1", n)
	}
	if got := w.findTask(); got != std {
		t.Fatalf("findTask() = %p, want the standard task %p", got, std)
	}
}

// TestWorker_FindTaskStandardBeforeUnagedBackground is the complement:
// with the deadline far off, normal priority order applies.
func TestWorker_FindTaskStandardBeforeUnagedBackground(t *testing.T) {
	s := newTestPool(1, time.Hour)
	w := s.workers[0]

	std, bg := noopNode(), noopNode()
	s.background.push(bg, time.Now())
	s.standard.push(std)

	if got := w.findTask(); got != std {
		t.Fatalf("findTask() = %p, want the standard task %p", got, std)
	}
	if n := s.Stats().Aged; n != 0 {
		t.Fatalf("Stats().Aged = %d, want 0 — nothing was past its deadline", n)
	}
}

// TestWorker_FindTaskBackgroundWhenStandardEmpty covers step 4: once the
// standard queue drains, background work runs at full speed rather than
// idling until its aging deadline.
func TestWorker_FindTaskBackgroundWhenStandardEmpty(t *testing.T) {
	s := newTestPool(1, time.Hour)
	w := s.workers[0]

	bg := noopNode()
	s.background.push(bg, time.Now())

	if got := w.findTask(); got != bg {
		t.Fatalf("findTask() = %p, want the background task %p — it must not wait for its deadline once Standard is empty", got, bg)
	}
	if n := s.Stats().Aged; n != 0 {
		t.Fatalf("Stats().Aged = %d, want 0 — an unconditional pop is not an aged promotion", n)
	}
}

func TestWorker_FindTaskStealsFromPeer(t *testing.T) {
	s := newTestPool(4, time.Hour)
	w := s.workers[0]

	victim := noopNode()
	s.workers[3].dq.pushBottom(victim)

	if got := w.findTask(); got != victim {
		t.Fatalf("findTask() = %p, want the stolen task %p", got, victim)
	}
	if n := s.Stats().Stolen; n != 1 {
		t.Fatalf("Stats().Stolen = %d, want 1", n)
	}
}

func TestWorker_FindTaskNilWhenPoolIsEmpty(t *testing.T) {
	s := newTestPool(4, time.Hour)

	if got := s.workers[0].findTask(); got != nil {
		t.Fatalf("findTask() = %p on a completely empty pool, want nil", got)
	}
}

func TestWorker_StealReturnsNilForSoleWorker(t *testing.T) {
	s := newTestPool(1, time.Hour)
	w := s.workers[0]
	w.dq.pushBottom(noopNode())

	if got := w.steal(); got != nil {
		t.Fatalf("steal() = %p with no peers, want nil", got)
	}
}

// TestWorker_StealNeverTakesFromOwnDeque guards the `idx == w.id` skip:
// a worker's own deque is the owner's LIFO to pop, not a steal target,
// and taking from its top here would race the owner's popBottom.
func TestWorker_StealNeverTakesFromOwnDeque(t *testing.T) {
	s := newTestPool(4, time.Hour)
	w := s.workers[0]
	mine := noopNode()
	w.dq.pushBottom(mine)

	if got := w.steal(); got != nil {
		t.Fatalf("steal() = %p, want nil — only this worker's own deque held work", got)
	}
	if got := w.dq.popBottom(); got != mine {
		t.Fatalf("own deque lost its task to steal(): popBottom() = %p, want %p", got, mine)
	}
}

// TestWorker_StealScansEveryPeer checks the wrap-around in steal's scan:
// wherever the single stocked peer sits relative to the random start
// index, one call must find it.
func TestWorker_StealScansEveryPeer(t *testing.T) {
	const workers = 8
	for victim := 0; victim < workers; victim++ {
		s := newTestPool(workers, time.Hour)
		w := s.workers[0]
		if victim == w.id {
			continue
		}

		want := noopNode()
		s.workers[victim].dq.pushBottom(want)
		if got := w.steal(); got != want {
			t.Fatalf("steal() = %p with work parked on peer %d, want %p", got, victim, want)
		}
	}
}

func TestWorker_ScheduleFromWorkerContextUsesLocalDeque(t *testing.T) {
	s := newTestPool(2, time.Hour)
	w := s.workers[0]

	n := noopNode()
	s.schedule(w.context(context.Background()), PriorityBackground, n)

	if got := w.dq.popBottom(); got != n {
		t.Fatalf("local deque popBottom() = %p, want %p — a nested submit must bypass the shared queues", got, n)
	}
	if got := s.standard.pop(); got != nil {
		t.Fatalf("standard queue = %p, want empty", got)
	}
	if got := s.background.pop(); got != nil {
		t.Fatalf("background queue = %p, want empty — priority is ignored on the local path", got)
	}
}

func TestWorker_ScheduleSpillsWhenLocalDequeFull(t *testing.T) {
	s := newTestPool(2, time.Hour)
	w := s.workers[0]
	for i := 0; i < defaultLocalQueueCap; i++ {
		if !w.dq.pushBottom(noopNode()) {
			t.Fatalf("setup: local deque reported full after only %d pushes", i)
		}
	}

	spilled := noopNode()
	s.schedule(w.context(context.Background()), PriorityStandard, spilled)

	if got := s.standard.pop(); got != spilled {
		t.Fatalf("standard queue pop() = %p, want the spilled task %p", got, spilled)
	}
}

// TestWorker_ScheduleIgnoresForeignSchedulerContext covers schedule's
// `w.s == s` guard: a context carrying a *different* scheduler's worker
// must not push onto that worker's deque, where this scheduler's pool
// would never look for it.
func TestWorker_ScheduleIgnoresForeignSchedulerContext(t *testing.T) {
	foreign := newTestPool(2, time.Hour)
	s := newTestPool(2, time.Hour)

	n := noopNode()
	s.schedule(foreign.workers[0].context(context.Background()), PriorityStandard, n)

	if got := foreign.workers[0].dq.popBottom(); got != nil {
		t.Fatalf("foreign worker's deque = %p, want nil — a task was pushed onto a pool that will never run it", got)
	}
	if got := s.standard.pop(); got != n {
		t.Fatalf("standard queue pop() = %p, want %p", got, n)
	}
}

// TestWorker_ConcurrentFindTaskDeliversEachTaskOnce runs findTask
// concurrently against live producers, mixing all four sources (own
// deque, aged background, standard, steal). Every task must be handed to
// exactly one worker: no duplicate delivery from a lost steal race, and
// none dropped.
//
// workers[0] is a pure victim — it is stocked but never runs a consumer
// loop of its own, so its tasks are reachable only by stealing. That
// makes the steal path deterministic rather than dependent on a peer
// happening to go idle while another still has local work: findTask only
// reaches stealing once both shared queues are dry, by which point a
// self-draining worker has long since emptied its own deque.
func TestWorker_ConcurrentFindTaskDeliversEachTaskOnce(t *testing.T) {
	const workers = 8
	const victimLocal = defaultLocalQueueCap
	const perWorkerLocal = 64
	const producers = 4
	const perProducer = 5_000
	const shared = producers * perProducer
	const total = victimLocal + (workers-1)*perWorkerLocal + shared

	s := newTestPool(workers, 0) // threshold 0: exercise the popAged path
	seen := make([]atomic.Int32, total)
	mark := func(i int) *taskNode {
		return &taskNode{run: func(context.Context) { seen[i].Add(1) }}
	}

	// Stock the deques before any goroutine starts: pushBottom is
	// owner-only, and here the test goroutine stands in for every owner.
	id := 0
	for i, w := range s.workers {
		n := perWorkerLocal
		if i == 0 {
			n = victimLocal
		}
		for j := 0; j < n; j++ {
			if !w.dq.pushBottom(mark(id)) {
				t.Fatalf("setup: worker %d pushBottom reported full after %d pushes", i, j)
			}
			id++
		}
	}

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		lo := id + p*perProducer
		go func() {
			defer wg.Done()
			for i := lo; i < lo+perProducer; i++ {
				if i%2 == 0 {
					s.standard.push(mark(i))
				} else {
					s.background.push(mark(i), time.Now())
				}
			}
		}()
	}

	var delivered atomic.Int64
	deadline := time.Now().Add(30 * time.Second)
	consumers := s.workers[1:]
	wg.Add(len(consumers))
	for _, w := range consumers {
		go func() {
			defer wg.Done()
			ctx := w.context(context.Background())
			for delivered.Load() < total {
				if n := w.findTask(); n != nil {
					n.run(ctx)
					delivered.Add(1)
					continue
				}
				if time.Now().After(deadline) {
					return
				}
			}
		}()
	}
	wg.Wait()

	if got := delivered.Load(); got != total {
		t.Fatalf("delivered = %d, want %d — workers timed out with tasks still queued", got, total)
	}
	for i := range seen {
		if c := seen[i].Load(); c != 1 {
			t.Fatalf("task %d delivered %d times, want exactly 1", i, c)
		}
	}
	stats := s.Stats()
	if stats.Stolen < victimLocal {
		t.Errorf("Stats().Stolen = %d, want >= %d — the victim's deque is reachable only by stealing", stats.Stolen, victimLocal)
	}
	if stats.Aged == 0 {
		t.Error("Stats().Aged = 0, want > 0 — the aged-promotion path was never exercised")
	}
}

// TestWorker_ParkedWorkersWakeOnLateSubmit exercises run's park path: the
// pool goes fully idle, every worker exhausts its idle steal passes and
// blocks on the doorbell, and a task arriving afterwards must still be
// picked up.
func TestWorker_ParkedWorkersWakeOnLateSubmit(t *testing.T) {
	s := New(Config{Workers: 4, AgingThreshold: time.Hour})
	defer s.Stop()

	time.Sleep(50 * time.Millisecond) // let every worker park

	done := make(chan Result[int], 1)
	go func() {
		done <- Submit(context.Background(), s, PriorityBackground, func(context.Context) (int, error) {
			return 7, nil
		}).Wait()
	}()

	select {
	case res := <-done:
		if res.Err != nil || res.Value != 7 {
			t.Fatalf("Wait() = %+v, want {7 <nil>}", res)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no worker woke for a task submitted after the pool went idle")
	}
}

// TestWorker_BlockedParentIsUnblockedByParkedPeers is the liveness case
// schedule's doc comment calls out: a worker pushes children onto its own
// deque and then blocks waiting on them, so it cannot run them itself.
// Unless the local push also rings the doorbell, the parked peers never
// learn there is anything to steal and the pool deadlocks.
func TestWorker_BlockedParentIsUnblockedByParkedPeers(t *testing.T) {
	const children = 32
	s := New(Config{Workers: 4, AgingThreshold: time.Hour})
	defer s.Stop()

	time.Sleep(50 * time.Millisecond) // let every worker park

	root := Submit(context.Background(), s, PriorityStandard, func(ctx context.Context) (int, error) {
		futs := make([]*Future[int], children)
		for i := range futs {
			futs[i] = Submit(ctx, s, PriorityStandard, func(context.Context) (int, error) {
				return 1, nil
			})
		}
		sum := 0
		for _, f := range futs {
			sum += f.Wait().Value
		}
		return sum, nil
	})

	done := make(chan Result[int], 1)
	go func() { done <- root.Wait() }()

	select {
	case res := <-done:
		if res.Err != nil || res.Value != children {
			t.Fatalf("root result = %+v, want {%d <nil>}", res, children)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("blocked parent never completed — parked peers were not woken to steal its children")
	}
}

// TestWorker_RunSurvivesConcurrentSubmitAndStop races Stop against live
// submitters on both priorities. Futures are deliberately not waited on:
// Stop is a shutdown, not a drain, so a task that loses the race is
// abandoned and its Future never resolves.
func TestWorker_RunSurvivesConcurrentSubmitAndStop(t *testing.T) {
	const submitters = 8
	const perSubmitter = 5_000

	s := New(Config{Workers: 8, AgingThreshold: time.Millisecond})

	var wg sync.WaitGroup
	wg.Add(submitters)
	for i := 0; i < submitters; i++ {
		priority := PriorityStandard
		if i%2 == 1 {
			priority = PriorityBackground
		}
		go func() {
			defer wg.Done()
			for j := 0; j < perSubmitter; j++ {
				Submit(context.Background(), s, priority, func(context.Context) (int, error) {
					return 0, nil
				})
			}
		}()
	}

	time.Sleep(2 * time.Millisecond)
	s.Stop()
	wg.Wait()

	if stats := s.Stats(); stats.Completed > stats.Submitted {
		t.Fatalf("Stats() = %+v, want Completed <= Submitted", stats)
	}
}

func BenchmarkWorker_FindTaskLocal(b *testing.B) {
	s := newTestPool(8, time.Hour)
	w := s.workers[0]
	n := noopNode()
	b.ReportAllocs()
	for b.Loop() {
		w.dq.pushBottom(n)
		w.findTask()
	}
}

func BenchmarkWorker_FindTaskStandard(b *testing.B) {
	s := newTestPool(8, time.Hour)
	w := s.workers[0]
	n := noopNode()
	b.ReportAllocs()
	for b.Loop() {
		s.standard.push(n)
		w.findTask()
	}
}

// BenchmarkWorker_FindTaskAged measures the promotion path, which costs
// an extra time.Now plus the deadline compare on top of the queue pop.
func BenchmarkWorker_FindTaskAged(b *testing.B) {
	s := newTestPool(8, 0)
	w := s.workers[0]
	n := noopNode()
	now := time.Now()
	b.ReportAllocs()
	for b.Loop() {
		s.background.push(n, now)
		w.findTask()
	}
}

// BenchmarkWorker_FindTaskIdle is the cost a worker pays to conclude
// there is no work anywhere: both shared queues, then idleSteals full
// peer scans with a runtime.Gosched between them. It is what the
// idleSteals constant trades idle CPU against arrival latency for.
func BenchmarkWorker_FindTaskIdle(b *testing.B) {
	s := newTestPool(8, time.Hour)
	w := s.workers[0]
	b.ReportAllocs()
	for b.Loop() {
		w.findTask()
	}
}

// BenchmarkWorker_StealMiss is one full peer scan that finds nothing —
// the per-pass cost inside findTask's idle loop, without Gosched.
func BenchmarkWorker_StealMiss(b *testing.B) {
	s := newTestPool(8, time.Hour)
	w := s.workers[0]
	b.ReportAllocs()
	for b.Loop() {
		w.steal()
	}
}

// BenchmarkWorker_StealHit measures steady-state successful steals. The
// refill on a miss is amortized over a full restock of every peer, and
// is single-goroutine here so the deque's single-producer constraint
// still holds.
func BenchmarkWorker_StealHit(b *testing.B) {
	const workers = 8
	s := newTestPool(workers, time.Hour)
	w := s.workers[0]
	n := noopNode()

	restock := func() {
		for _, p := range s.workers[1:] {
			for p.dq.pushBottom(n) {
			}
		}
	}
	restock()

	b.ReportAllocs()
	for b.Loop() {
		if w.steal() == nil {
			b.StopTimer()
			restock()
			b.StartTimer()
		}
	}
}

// BenchmarkWorker_FromContextHit is on the hot path: every Submit does
// this lookup to decide between the local deque and the shared queues.
func BenchmarkWorker_FromContextHit(b *testing.B) {
	s := newTestPool(2, time.Hour)
	ctx := s.workers[0].context(context.Background())
	b.ReportAllocs()
	for b.Loop() {
		workerFromContext(ctx)
	}
}

// BenchmarkWorker_FromContextMiss is the same lookup for an external
// Submit, where the value is absent and the whole context chain is
// walked before giving up.
func BenchmarkWorker_FromContextMiss(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		workerFromContext(ctx)
	}
}
