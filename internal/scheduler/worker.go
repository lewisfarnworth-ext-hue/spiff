package scheduler

import (
	"context"
	"math/rand/v2"
	"runtime"
	"time"
)

// idleSteals is how many full steal passes (over every peer worker) a
// worker attempts, backing off with runtime.Gosched between passes,
// before parking. It trades a little idle CPU for lower latency on
// bursty arrivals immediately after the queues go empty.
const idleSteals = 4

// workerCtxKey is the context.Context key a running task's own context
// is tagged with, so a nested Submit call from inside that task can
// detect it is executing on a worker and push the child task directly
// onto that worker's own local deque instead of the shared queues.
type workerCtxKey struct{}

// worker owns one local deque and runs tasks pulled from it, the
// scheduler's shared queues, or stolen from a peer, until the
// scheduler is stopped.
type worker struct {
	id  int
	s   *Scheduler
	dq  *deque
	rng *rand.Rand
}

// context returns ctx tagged so nested Submit calls route to w.
func (w *worker) context(ctx context.Context) context.Context {
	return context.WithValue(ctx, workerCtxKey{}, w)
}

// workerFromContext reports the worker ctx is running on, if any.
func workerFromContext(ctx context.Context) (*worker, bool) {
	w, ok := ctx.Value(workerCtxKey{}).(*worker)
	return w, ok
}

// run is the worker's main loop: it repeatedly executes tasks until the
// scheduler stops, parking (rather than spinning) once no work is
// found anywhere.
//
// A parked worker only ever waits on the plain doorbell (or stop), with
// no deadline-aware timer for the background queue's aging threshold —
// that would be redundant, not just simpler. findTask's step 3 drains
// the background queue unconditionally the instant the standard queue
// is empty, before a worker ever falls through to stealing or parking.
// So the only way a background task can still be waiting past its
// deadline is if the worker is continuously busy running standard
// work — in which case it is never parked, and re-checks popAged for
// free on every loop iteration once it goes looking for its next task.
// A worker can only reach park() once the background queue is already
// empty, at which point there is nothing left to age.
func (w *worker) run() {
	defer w.s.wg.Done()
	ctx := w.context(context.Background())
	for {
		node := w.findTask()
		if node != nil {
			// node.run bumps s.completed itself, before it resolves the
			// task's Future — see Submit's doc comment on why that
			// ordering matters and must not be done here instead.
			node.run(ctx)
			continue
		}

		select {
		case <-w.s.stop:
			return
		case <-w.s.wake:
		}
	}
}

// findTask looks for work in order: this worker's own deque (LIFO,
// cache-hot); the background queue's oldest task, but only if its
// aging deadline has elapsed; the standard queue; the background
// queue's oldest task unconditionally (once Standard is empty, aged or
// not); then stealing from a peer's deque. It returns nil if none was
// found after idleSteals passes.
func (w *worker) findTask() *taskNode {
	if t := w.dq.popBottom(); t != nil {
		return t
	}
	if t := w.s.background.popAged(time.Now()); t != nil {
		w.s.aged.Add(1)
		return t
	}
	if t := w.s.standard.pop(); t != nil {
		return t
	}
	if t := w.s.background.pop(); t != nil {
		return t
	}
	for i := 0; i < idleSteals; i++ {
		if t := w.steal(); t != nil {
			return t
		}
		runtime.Gosched()
	}
	return nil
}

// steal tries once to pop from every peer's deque, starting at a random
// index so workers don't all hammer the same victim in lockstep.
func (w *worker) steal() *taskNode {
	peers := w.s.workers
	n := len(peers)
	if n <= 1 {
		return nil
	}
	start := w.rng.IntN(n)
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if idx == w.id {
			continue
		}
		if t := peers[idx].dq.popTop(); t != nil {
			w.s.stolen.Add(1)
			return t
		}
	}
	return nil
}
