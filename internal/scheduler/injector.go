package scheduler

import "sync"

// injector is the scheduler's high-priority (Standard) FIFO queue. It is
// deliberately not lock-free: it only serves two low-frequency paths
// (external Submit calls, and overflow when a worker's local deque is
// full), so a mutex is simpler and safer than a lock-free MPMC queue
// without giving up meaningful throughput on the hot path, which stays
// on the per-worker lock-free deque.
type injector struct {
	mu  sync.Mutex
	buf ringBuffer[*taskNode]
}

// newInjector returns an injector with room for capacity tasks before its
// first grow.
func newInjector(capacity int) *injector {
	return &injector{buf: ringBuffer[*taskNode]{buf: make([]*taskNode, capacity)}}
}

// push enqueues t and reports true, unless max > 0 and the queue is
// already holding max tasks, in which case it reports false without
// enqueuing anything.
func (inj *injector) push(t *taskNode, max int) bool {
	inj.mu.Lock()
	defer inj.mu.Unlock()
	if max > 0 && inj.buf.len >= max {
		return false
	}
	inj.buf.push(t)
	return true
}

// pop removes and returns the oldest task, or nil if the injector is
// empty.
func (inj *injector) pop() *taskNode {
	inj.mu.Lock()
	defer inj.mu.Unlock()
	t, ok := inj.buf.pop()
	if !ok {
		return nil
	}
	return t
}
