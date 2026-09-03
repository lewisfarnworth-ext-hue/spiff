package scheduler

import (
	"sync"
	"testing"
)

func TestInjector_FIFOAcrossGrow(t *testing.T) {
	inj := newInjector(2)
	var pushed []*taskNode
	for i := 0; i < 20; i++ {
		n := noopNode()
		pushed = append(pushed, n)
		inj.push(n, 0)
	}

	for i, want := range pushed {
		got := inj.pop()
		if got != want {
			t.Fatalf("pop() #%d = %p, want %p (FIFO order across grow)", i, got, want)
		}
	}
	if got := inj.pop(); got != nil {
		t.Fatalf("pop() on empty injector = %v, want nil", got)
	}
}

func TestInjector_PopVacatesSlot(t *testing.T) {
	inj := newInjector(4)
	inj.push(noopNode(), 0)
	inj.pop()
	if inj.buf.buf[0] != nil {
		t.Fatal("pop() left a stale pointer in the vacated slot")
	}
}

func TestInjector_ConcurrentPushPop(t *testing.T) {
	const producers = 8
	const perProducer = 5000
	const total = producers * perProducer

	inj := newInjector(16)
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				inj.push(noopNode(), 0)
			}
		}()
	}

	got := 0
	for got < total {
		if inj.pop() != nil {
			got++
		}
	}
	wg.Wait()
}

func TestInjector_RespectsMaxDepth(t *testing.T) {
	const max = 3
	inj := newInjector(8)
	for i := 0; i < max; i++ {
		if !inj.push(noopNode(), max) {
			t.Fatalf("push() #%d reported full before reaching max=%d", i, max)
		}
	}
	if inj.push(noopNode(), max) {
		t.Fatal("push() succeeded once the injector was already at max, want false")
	}

	// Freeing a slot via pop must let the next push through again — the
	// cap bounds depth, not a one-shot lifetime.
	if inj.pop() == nil {
		t.Fatal("pop() = nil, want the oldest task")
	}
	if !inj.push(noopNode(), max) {
		t.Fatal("push() reported full despite freeing a slot via pop()")
	}
}

func TestInjector_ZeroMaxIsUnbounded(t *testing.T) {
	inj := newInjector(2)
	for i := 0; i < 1000; i++ {
		if !inj.push(noopNode(), 0) {
			t.Fatalf("push() #%d reported full with max=0, want unbounded", i)
		}
	}
}

func BenchmarkInjector_PushPop(b *testing.B) {
	inj := newInjector(1024)
	n := noopNode()
	b.ReportAllocs()
	for b.Loop() {
		inj.push(n, 0)
		inj.pop()
	}
}

// BenchmarkInjector_PushPopCapped is the same push/pop with a real cap
// configured, isolating what the depth check itself costs on top of an
// unbounded push (BenchmarkInjector_PushPop) — an int compare under a
// lock already held for the push, so no measurable delta is expected.
func BenchmarkInjector_PushPopCapped(b *testing.B) {
	inj := newInjector(1024)
	n := noopNode()
	b.ReportAllocs()
	for b.Loop() {
		inj.push(n, 1<<20)
		inj.pop()
	}
}
