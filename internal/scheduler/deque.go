package scheduler

import "sync/atomic"

// deque is a bounded, lock-free single-producer/multi-consumer deque
// (Chase-Lev). The owning worker calls pushBottom/popBottom; any other
// worker may call popTop to steal. Capacity is fixed: when full, the
// owner must spill overflow work to the injector rather than resize,
// since a lock-free resize is a substantially more complex algorithm
// than this "for now" scheduler needs.
type deque struct {
	top    atomic.Int64
	bottom atomic.Int64
	buf    []atomic.Pointer[taskNode]
	mask   int64
}

// newDeque returns a deque with room for capacity tasks. capacity must be
// a power of two so index wrapping can use a bitmask instead of a
// division/modulo on every push/pop.
func newDeque(capacity int) *deque {
	return &deque{
		buf:  make([]atomic.Pointer[taskNode], capacity),
		mask: int64(capacity - 1),
	}
}

// pushBottom appends t to the bottom of the deque. It must only be called
// by the owning worker. It reports false if the deque is full, in which
// case the caller should spill t to the injector instead.
func (d *deque) pushBottom(t *taskNode) bool {
	b := d.bottom.Load()
	top := d.top.Load()
	if b-top >= int64(len(d.buf)) {
		return false
	}
	d.buf[b&d.mask].Store(t)
	d.bottom.Store(b + 1)
	return true
}

// popBottom removes and returns the task at the bottom of the deque
// (LIFO), giving the owner cache-friendly access to the task it most
// recently pushed. It must only be called by the owning worker. It
// returns nil if the deque is empty, or if it lost a race with a
// concurrent steal for the last remaining element.
func (d *deque) popBottom() *taskNode {
	b := d.bottom.Load() - 1
	d.bottom.Store(b)
	top := d.top.Load()

	if top > b {
		// Deque was already empty; restore bottom.
		d.bottom.Store(top)
		return nil
	}

	t := d.buf[b&d.mask].Load()
	if top == b {
		// Last element: a thief may be racing us for it via popTop.
		if !d.top.CompareAndSwap(top, top+1) {
			t = nil
		}
		d.bottom.Store(top + 1)
	}
	return t
}

// popTop removes and returns the task at the top of the deque (FIFO),
// stealing the oldest work so the owner's own LIFO locality is left
// undisturbed. It may be called concurrently by any number of thieves.
// It returns nil if the deque is empty or a race for the last element
// was lost.
func (d *deque) popTop() *taskNode {
	top := d.top.Load()
	b := d.bottom.Load()
	if top >= b {
		return nil
	}
	t := d.buf[top&d.mask].Load()
	if !d.top.CompareAndSwap(top, top+1) {
		return nil
	}
	return t
}
