package scheduler

import (
	"sync"
	"testing"
	"time"
)

func TestRingBuffer_ZeroValueIsUsable(t *testing.T) {
	var r ringBuffer[*taskNode]
	n := noopNode()
	r.push(n)

	if got, ok := r.pop(); !ok || got != n {
		t.Fatalf("pop() = %p, %v, want %p, true — the zero value must grow on first push", got, ok, n)
	}
}

func TestRingBuffer_EmptyFrontAndPop(t *testing.T) {
	var r ringBuffer[*taskNode]
	if got, ok := r.front(); ok || got != nil {
		t.Fatalf("front() on empty = %p, %v, want nil, false", got, ok)
	}
	if got, ok := r.pop(); ok || got != nil {
		t.Fatalf("pop() on empty = %p, %v, want nil, false", got, ok)
	}
}

func TestRingBuffer_FrontDoesNotRemove(t *testing.T) {
	r := ringBuffer[*taskNode]{buf: make([]*taskNode, 4)}
	n := noopNode()
	r.push(n)

	for i := 0; i < 3; i++ {
		got, ok := r.front()
		if !ok || got != n {
			t.Fatalf("front() call #%d = %p, %v, want %p, true", i, got, ok, n)
		}
	}
	if r.len != 1 {
		t.Fatalf("len = %d after 3 front() calls, want 1 — front must not remove", r.len)
	}
}

func TestRingBuffer_FIFOAcrossGrow(t *testing.T) {
	const n = 100
	r := ringBuffer[int]{buf: make([]int, 2)}
	for i := 0; i < n; i++ {
		r.push(i)
	}

	for i := 0; i < n; i++ {
		got, ok := r.pop()
		if !ok || got != i {
			t.Fatalf("pop() #%d = %d, %v, want %d, true (FIFO across grow)", i, got, ok, i)
		}
	}
	if _, ok := r.pop(); ok {
		t.Fatal("pop() reported ok on a drained buffer")
	}
}

// TestRingBuffer_GrowWithWrappedHead is the interesting grow case: head
// is past 0 and the live elements straddle the end of the backing array,
// so grow's copy has to unwrap them via the modulo rather than doing a
// straight copy from index 0.
func TestRingBuffer_GrowWithWrappedHead(t *testing.T) {
	const capacity = 4
	r := ringBuffer[int]{buf: make([]int, capacity)}

	// Advance head to 3 so the next pushes wrap around the end.
	for i := 0; i < 3; i++ {
		r.push(i)
		r.pop()
	}
	if r.head != 3 {
		t.Fatalf("setup: head = %d, want 3", r.head)
	}

	// Fill to capacity across the wrap point, then force a grow.
	for i := 0; i < capacity; i++ {
		r.push(100 + i)
	}
	if r.head == 0 {
		t.Fatal("setup: buffer did not wrap, head is still 0")
	}
	r.push(200)

	if r.head != 0 {
		t.Fatalf("head = %d after grow, want 0 — grow must re-base live elements at index 0", r.head)
	}
	for i := 0; i < capacity; i++ {
		got, ok := r.pop()
		if !ok || got != 100+i {
			t.Fatalf("pop() #%d = %d, %v, want %d, true — grow scrambled a wrapped buffer", i, got, ok, 100+i)
		}
	}
	if got, ok := r.pop(); !ok || got != 200 {
		t.Fatalf("pop() of the element that triggered grow = %d, %v, want 200, true", got, ok)
	}
}

func TestRingBuffer_GrowDoublesCapacity(t *testing.T) {
	r := ringBuffer[int]{buf: make([]int, 4)}
	for i := 0; i < 4; i++ {
		r.push(i)
	}
	if got := len(r.buf); got != 4 {
		t.Fatalf("cap = %d after filling exactly to capacity, want 4 — grew a beat too early", got)
	}
	r.push(4)
	if got := len(r.buf); got != 8 {
		t.Fatalf("cap = %d after overflowing, want 8", got)
	}
}

// TestRingBuffer_PopZeroesVacatedSlot guards the documented anti-retention
// behaviour: a popped element must not stay reachable through the backing
// array, or a *taskNode (and everything its closure captures) is pinned
// until that slot is next overwritten.
func TestRingBuffer_PopZeroesVacatedSlot(t *testing.T) {
	r := ringBuffer[*taskNode]{buf: make([]*taskNode, 4)}
	r.push(noopNode())
	r.push(noopNode())

	if _, ok := r.pop(); !ok {
		t.Fatal("pop() reported empty right after two pushes")
	}
	if r.buf[0] != nil {
		t.Fatal("pop() left a stale pointer in the vacated slot")
	}
}

// TestRingBuffer_PopZeroesVacatedSlot_ValueElement covers the same
// retention guarantee for the aging queue's element type, where the
// pointer to zero out is nested inside a struct value rather than being
// the element itself.
func TestRingBuffer_PopZeroesVacatedSlot_ValueElement(t *testing.T) {
	r := ringBuffer[agingNode]{buf: make([]agingNode, 4)}
	r.push(agingNode{task: noopNode(), deadline: time.Now()})

	if _, ok := r.pop(); !ok {
		t.Fatal("pop() reported empty right after a push")
	}
	if r.buf[0].task != nil {
		t.Fatal("pop() left a stale *taskNode inside the vacated slot's struct")
	}
}

// TestRingBuffer_InterleavedPushPopWraps hammers the modulo indexing with
// a long interleaved sequence that drives head around the backing array
// many times, checking FIFO order and len never drift.
func TestRingBuffer_InterleavedPushPopWraps(t *testing.T) {
	const rounds = 10_000
	const window = 5

	r := ringBuffer[int]{buf: make([]int, 8)}
	next, want := 0, 0
	for i := 0; i < rounds; i++ {
		for j := 0; j < window; j++ {
			r.push(next)
			next++
		}
		for j := 0; j < window-1; j++ {
			got, ok := r.pop()
			if !ok || got != want {
				t.Fatalf("round %d: pop() = %d, %v, want %d, true", i, got, ok, want)
			}
			want++
		}
		if r.len != i+1 {
			t.Fatalf("round %d: len = %d, want %d", i, r.len, i+1)
		}
	}
}

// TestRingBuffer_ConcurrentUnderCallerMutex asserts the type's actual
// contract: ringBuffer holds no hidden shared state of its own, so an
// external mutex held by the caller (as injector and agingQueue do) is
// sufficient to make it race-free. Meaningful only under -race.
func TestRingBuffer_ConcurrentUnderCallerMutex(t *testing.T) {
	const goroutines = 8
	const perGoroutine = 20_000

	var mu sync.Mutex
	var r ringBuffer[*taskNode]

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			n := noopNode()
			for j := 0; j < perGoroutine; j++ {
				mu.Lock()
				r.push(n)
				mu.Unlock()

				mu.Lock()
				r.front()
				mu.Unlock()

				mu.Lock()
				r.pop()
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if r.len != 0 {
		t.Fatalf("len = %d after equal pushes and pops, want 0", r.len)
	}
}

func BenchmarkRingBuffer_PushPop(b *testing.B) {
	r := ringBuffer[*taskNode]{buf: make([]*taskNode, 1024)}
	n := noopNode()
	b.ReportAllocs()
	for b.Loop() {
		r.push(n)
		r.pop()
	}
}

// BenchmarkRingBuffer_PushPopValueElement is the agingQueue's element
// type: a struct copied by value on every push and pop, rather than a
// single pointer word.
func BenchmarkRingBuffer_PushPopValueElement(b *testing.B) {
	r := ringBuffer[agingNode]{buf: make([]agingNode, 1024)}
	v := agingNode{task: noopNode(), deadline: time.Now()}
	b.ReportAllocs()
	for b.Loop() {
		r.push(v)
		r.pop()
	}
}

func BenchmarkRingBuffer_Front(b *testing.B) {
	r := ringBuffer[*taskNode]{buf: make([]*taskNode, 8)}
	r.push(noopNode())
	b.ReportAllocs()
	for b.Loop() {
		r.front()
	}
}

// BenchmarkRingBuffer_GrowFromZero measures the amortized cost of filling
// a zero-value buffer, which is dominated by the doubling copies in grow.
func BenchmarkRingBuffer_GrowFromZero(b *testing.B) {
	const items = 1024
	n := noopNode()
	b.ReportAllocs()
	for b.Loop() {
		var r ringBuffer[*taskNode]
		for i := 0; i < items; i++ {
			r.push(n)
		}
	}
}

// BenchmarkRingBuffer_GrowPreallocated is the same fill against a buffer
// sized up front, so the delta against GrowFromZero is what a correct
// initial capacity is worth.
func BenchmarkRingBuffer_GrowPreallocated(b *testing.B) {
	const items = 1024
	n := noopNode()
	b.ReportAllocs()
	for b.Loop() {
		r := ringBuffer[*taskNode]{buf: make([]*taskNode, items)}
		for i := 0; i < items; i++ {
			r.push(n)
		}
	}
}
