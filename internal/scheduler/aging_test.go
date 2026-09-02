package scheduler

import (
	"sync"
	"testing"
	"time"
)

// frontDeadline is a white-box test helper: production code never
// needs to peek at a deadline without also popping (see worker.go's
// doc comment on why no deadline-aware park is needed), but tests do,
// to verify push's jitter computation directly.
func (q *agingQueue) frontDeadline() (time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n, ok := q.buf.front()
	if !ok {
		return time.Time{}, false
	}
	return n.deadline, true
}

func TestAgingQueue_FIFOOrder(t *testing.T) {
	q := newAgingQueue(4, time.Hour, 0, 1, 2)
	var pushed []*taskNode
	for i := 0; i < 5; i++ {
		n := noopNode()
		pushed = append(pushed, n)
		q.push(n, time.Now())
	}

	for i, want := range pushed {
		got := q.pop()
		if got != want {
			t.Fatalf("pop() #%d = %p, want %p (FIFO order)", i, got, want)
		}
	}
	if got := q.pop(); got != nil {
		t.Fatalf("pop() on empty queue = %v, want nil", got)
	}
}

func TestAgingQueue_PopAgedOnlyWhenElapsed(t *testing.T) {
	const threshold = 50 * time.Millisecond
	q := newAgingQueue(4, threshold, 0, 1, 2)
	now := time.Now()
	q.push(noopNode(), now)

	if got := q.popAged(now); got != nil {
		t.Fatal("popAged() returned a task before its deadline elapsed")
	}
	if got := q.popAged(now.Add(threshold)); got == nil {
		t.Fatal("popAged() returned nil once the deadline elapsed, want the task")
	}
}

func TestAgingQueue_PopIgnoresDeadline(t *testing.T) {
	q := newAgingQueue(4, time.Hour, 0, 1, 2)
	n := noopNode()
	q.push(n, time.Now())

	if got := q.pop(); got != n {
		t.Fatalf("pop() = %p, want %p — pop must ignore the deadline entirely", got, n)
	}
}

func TestAgingQueue_JitterNeverExceedsThreshold(t *testing.T) {
	const threshold = 100 * time.Millisecond
	const jitter = 40 * time.Millisecond
	q := newAgingQueue(4, threshold, jitter, 1, 2)
	now := time.Now()

	for i := 0; i < 1000; i++ {
		q.push(noopNode(), now)
	}
	for {
		dl, ok := q.frontDeadline()
		if !ok {
			break
		}
		if dl.After(now.Add(threshold)) {
			t.Fatalf("deadline %v exceeds now+threshold %v — jitter must never widen the aging bound", dl, now.Add(threshold))
		}
		if dl.Before(now.Add(threshold - jitter)) {
			t.Fatalf("deadline %v earlier than now+threshold-jitter %v", dl, now.Add(threshold-jitter))
		}
		q.pop()
	}
}

// TestAgingQueue_JitterSpreadsDeadlines is the direct test of the
// thundering-herd fix: a batch pushed at the same instant must not all
// land on exactly the same deadline.
func TestAgingQueue_JitterSpreadsDeadlines(t *testing.T) {
	const threshold = 100 * time.Millisecond
	const jitter = 40 * time.Millisecond
	const n = 200

	q := newAgingQueue(4, threshold, jitter, 1, 2)
	now := time.Now()
	for i := 0; i < n; i++ {
		q.push(noopNode(), now)
	}

	distinct := make(map[time.Time]struct{})
	for i := 0; i < n; i++ {
		dl, ok := q.frontDeadline()
		if !ok {
			t.Fatalf("frontDeadline() empty after only %d of %d pops", i, n)
		}
		distinct[dl] = struct{}{}
		q.pop()
	}
	if len(distinct) < n/4 {
		t.Fatalf("only %d distinct deadlines across %d items pushed at once, jitter isn't spreading them", len(distinct), n)
	}
}

func TestAgingQueue_ZeroJitterIsDeterministic(t *testing.T) {
	const threshold = 100 * time.Millisecond
	q := newAgingQueue(4, threshold, 0, 1, 2)
	now := time.Now()
	q.push(noopNode(), now)

	dl, ok := q.frontDeadline()
	if !ok {
		t.Fatal("frontDeadline() empty right after push")
	}
	if !dl.Equal(now.Add(threshold)) {
		t.Fatalf("deadline = %v, want exactly now+threshold = %v when jitter is 0", dl, now.Add(threshold))
	}
}

func TestAgingQueue_ConcurrentPushPop(t *testing.T) {
	const producers = 8
	const perProducer = 2000
	const total = producers * perProducer

	q := newAgingQueue(16, time.Hour, time.Minute, 1, 2)
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				q.push(noopNode(), time.Now())
			}
		}()
	}

	got := 0
	for got < total {
		if q.pop() != nil {
			got++
		}
	}
	wg.Wait()
}

func BenchmarkAgingQueue_PushPop(b *testing.B) {
	q := newAgingQueue(1024, time.Hour, time.Minute, 1, 2)
	n := noopNode()
	now := time.Now()
	b.ReportAllocs()
	for b.Loop() {
		q.push(n, now)
		q.pop()
	}
}
