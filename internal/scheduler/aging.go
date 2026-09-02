package scheduler

import (
	"math/rand/v2"
	"sync"
	"time"
)

// agingNode pairs a task with the deadline by which it must be served.
type agingNode struct {
	task     *taskNode
	deadline time.Time
}

// agingQueue is the scheduler's low-priority (Background) FIFO queue.
// Every pushed task gets a deadline of at most threshold from now;
// popAged lets a worker serve the oldest task once its deadline has
// elapsed, bounding its worst-case wait regardless of Standard-queue
// load.
//
// rng is touched only under mu, piggybacking on the lock that already
// serializes push: computing jitter costs nothing beyond the lock push
// already pays, and never needs its own separate synchronization or a
// shared/global source that a bulk-batch insert would otherwise
// contend on.
type agingQueue struct {
	mu        sync.Mutex
	buf       ringBuffer[agingNode]
	threshold time.Duration
	jitter    time.Duration
	rng       *rand.Rand
}

// newAgingQueue returns an agingQueue with room for capacity tasks
// before its first grow. threshold <= 0 disables aging: push still
// records a deadline, but it is always in the past, so popAged always
// succeeds immediately and this queue behaves as a second plain FIFO.
func newAgingQueue(capacity int, threshold, jitter time.Duration, seed1, seed2 uint64) *agingQueue {
	return &agingQueue{
		buf:       ringBuffer[agingNode]{buf: make([]agingNode, capacity)},
		threshold: threshold,
		jitter:    jitter,
		rng:       rand.New(rand.NewPCG(seed1, seed2)),
	}
}

// push enqueues t with a deadline of now + threshold, minus an
// independent random amount in [0, jitter] drawn for this item alone.
// Jitter only ever pulls the deadline earlier, never later, so a batch
// pushed at once is spread across [now+threshold-jitter, now+threshold]
// instead of all crossing the threshold in the same instant, without
// ever widening the "waits at most threshold" bound.
func (q *agingQueue) push(t *taskNode, now time.Time) {
	q.mu.Lock()
	deadline := now.Add(q.threshold)
	if q.jitter > 0 {
		deadline = deadline.Add(-time.Duration(q.rng.Int64N(int64(q.jitter) + 1)))
	}
	q.buf.push(agingNode{task: t, deadline: deadline})
	q.mu.Unlock()
}

// popAged removes and returns the oldest task only if its deadline has
// elapsed; otherwise it returns nil without removing anything.
func (q *agingQueue) popAged(now time.Time) *taskNode {
	q.mu.Lock()
	defer q.mu.Unlock()
	n, ok := q.buf.front()
	if !ok || now.Before(n.deadline) {
		return nil
	}
	n, _ = q.buf.pop()
	return n.task
}

// pop removes and returns the oldest task unconditionally, ignoring
// its deadline. Used once the Standard queue is empty, so Background
// work still runs at full speed when nothing is competing for it.
func (q *agingQueue) pop() *taskNode {
	q.mu.Lock()
	defer q.mu.Unlock()
	n, ok := q.buf.pop()
	if !ok {
		return nil
	}
	return n.task
}
