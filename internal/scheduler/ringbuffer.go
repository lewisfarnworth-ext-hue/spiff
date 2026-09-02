package scheduler

// ringBuffer is a growable circular buffer, generic over element type so
// the injector (elements are *taskNode) and the aging queue (elements
// are agingNode, which pairs a *taskNode with its deadline) can share
// the same growth/indexing logic instead of duplicating it. It is not
// itself safe for concurrent use — callers (injector, agingQueue) hold
// their own mutex around every call.
type ringBuffer[T any] struct {
	buf  []T
	head int
	len  int
}

func (r *ringBuffer[T]) push(v T) {
	if r.len == len(r.buf) {
		r.grow()
	}
	r.buf[(r.head+r.len)%len(r.buf)] = v
	r.len++
}

// grow doubles the backing array, copying live elements to start at
// index 0.
func (r *ringBuffer[T]) grow() {
	newCap := 2 * len(r.buf)
	if newCap == 0 {
		newCap = 8
	}
	nb := make([]T, newCap)
	for i := 0; i < r.len; i++ {
		nb[i] = r.buf[(r.head+i)%len(r.buf)]
	}
	r.buf = nb
	r.head = 0
}

// front returns the oldest element without removing it, and reports
// whether the buffer is non-empty.
func (r *ringBuffer[T]) front() (T, bool) {
	if r.len == 0 {
		var zero T
		return zero, false
	}
	return r.buf[r.head], true
}

// pop removes and returns the oldest element. It zeros the vacated slot
// so a popped element (which may hold a pointer, e.g. *taskNode) isn't
// retained by the backing array until that slot is next overwritten.
func (r *ringBuffer[T]) pop() (T, bool) {
	if r.len == 0 {
		var zero T
		return zero, false
	}
	v := r.buf[r.head]
	var zero T
	r.buf[r.head] = zero
	r.head = (r.head + 1) % len(r.buf)
	r.len--
	return v, true
}
