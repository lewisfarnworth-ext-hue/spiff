package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func noopNode() *taskNode {
	return &taskNode{run: func(context.Context) {}}
}

func TestDeque_PushPopBottom_LIFO(t *testing.T) {
	d := newDeque(8)
	var pushed []*taskNode
	for i := 0; i < 5; i++ {
		n := noopNode()
		pushed = append(pushed, n)
		if !d.pushBottom(n) {
			t.Fatalf("pushBottom(%d) reported full", i)
		}
	}

	for i := len(pushed) - 1; i >= 0; i-- {
		got := d.popBottom()
		if got != pushed[i] {
			t.Fatalf("popBottom() = %p, want %p (LIFO order)", got, pushed[i])
		}
	}
	if got := d.popBottom(); got != nil {
		t.Fatalf("popBottom() on empty deque = %v, want nil", got)
	}
}

func TestDeque_PopTop_FIFO(t *testing.T) {
	d := newDeque(8)
	var pushed []*taskNode
	for i := 0; i < 5; i++ {
		n := noopNode()
		pushed = append(pushed, n)
		d.pushBottom(n)
	}

	for i := 0; i < len(pushed); i++ {
		got := d.popTop()
		if got != pushed[i] {
			t.Fatalf("popTop() = %p, want %p (FIFO order)", got, pushed[i])
		}
	}
	if got := d.popTop(); got != nil {
		t.Fatalf("popTop() on empty deque = %v, want nil", got)
	}
}

func TestDeque_FullReturnsFalse(t *testing.T) {
	d := newDeque(2)
	if !d.pushBottom(noopNode()) {
		t.Fatal("pushBottom(0) reported full early")
	}
	if !d.pushBottom(noopNode()) {
		t.Fatal("pushBottom(1) reported full early")
	}
	if d.pushBottom(noopNode()) {
		t.Fatal("pushBottom(2) on a full deque succeeded, want false")
	}
}

// TestDeque_ConcurrentStealVsOwner hammers a single deque with one
// owner doing pushBottom/popBottom while many thieves popTop, checking
// that every task is delivered to exactly one caller (no duplicate and
// no lost task) despite the races the algorithm is built to survive.
func TestDeque_ConcurrentStealVsOwner(t *testing.T) {
	const total = 200_000
	const thieves = 8

	d := newDeque(1024)
	seen := make([]atomic.Int32, total)
	deliver := func(n *taskNode) {
		if n != nil {
			n.run(context.Background())
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < thieves; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					for {
						n := d.popTop()
						if n == nil {
							return
						}
						deliver(n)
					}
				default:
					deliver(d.popTop())
				}
			}
		}()
	}

	for i := 0; i < total; i++ {
		idx := i
		n := &taskNode{run: func(context.Context) { seen[idx].Add(1) }}
		for !d.pushBottom(n) {
			// Deque full: pop our own to make room, counting it delivered.
			deliver(d.popBottom())
		}
	}
	for {
		got := d.popBottom()
		if got == nil {
			break
		}
		deliver(got)
	}
	close(stop)
	wg.Wait()

	for i := range seen {
		if c := seen[i].Load(); c != 1 {
			t.Fatalf("task %d delivered %d times, want exactly 1", i, c)
		}
	}
}

func BenchmarkDeque_PushPopBottom(b *testing.B) {
	d := newDeque(1024)
	n := noopNode()
	b.ReportAllocs()
	for b.Loop() {
		d.pushBottom(n)
		d.popBottom()
	}
}

func BenchmarkDeque_StealContention(b *testing.B) {
	d := newDeque(1 << 16)
	for i := 0; i < 1<<15; i++ {
		d.pushBottom(noopNode())
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if d.popTop() == nil {
				// Refill from the owner's side so the benchmark keeps
				// measuring steady-state contention instead of draining.
				d.pushBottom(noopNode())
			}
		}
	})
}
