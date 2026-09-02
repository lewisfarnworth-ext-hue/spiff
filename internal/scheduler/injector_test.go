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
		inj.push(n)
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
	inj.push(noopNode())
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
				inj.push(noopNode())
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

func BenchmarkInjector_PushPop(b *testing.B) {
	inj := newInjector(1024)
	n := noopNode()
	b.ReportAllocs()
	for b.Loop() {
		inj.push(n)
		inj.pop()
	}
}
