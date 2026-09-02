package scheduler

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubmit_ReturnsValue(t *testing.T) {
	s := New(Config{Workers: 4})
	defer s.Stop()

	fut := Submit(context.Background(), s, PriorityStandard, func(context.Context) (int, error) {
		return 42, nil
	})
	res := fut.Wait()
	if res.Err != nil || res.Value != 42 {
		t.Fatalf("Wait() = %+v, want {42 <nil>}", res)
	}
}

func TestSubmit_PropagatesError(t *testing.T) {
	s := New(Config{Workers: 4})
	defer s.Stop()

	wantErr := errors.New("boom")
	fut := Submit(context.Background(), s, PriorityStandard, func(context.Context) (string, error) {
		return "", wantErr
	})
	res := fut.Wait()
	if !errors.Is(res.Err, wantErr) {
		t.Fatalf("Wait().Err = %v, want %v", res.Err, wantErr)
	}
}

func TestSubmit_ManyTasksAllComplete(t *testing.T) {
	s := New(Config{Workers: 4})
	defer s.Stop()

	const n = 10_000
	futs := make([]*Future[int], n)
	for i := 0; i < n; i++ {
		i := i
		futs[i] = Submit(context.Background(), s, PriorityStandard, func(context.Context) (int, error) {
			return i * i, nil
		})
	}
	for i, f := range futs {
		res := f.Wait()
		if res.Err != nil || res.Value != i*i {
			t.Fatalf("task %d result = %+v, want {%d <nil>}", i, res, i*i)
		}
	}

	stats := s.Stats()
	if stats.Submitted != n || stats.Completed != n {
		t.Fatalf("Stats() = %+v, want Submitted=Completed=%d", stats, n)
	}
}

// TestSubmit_NestedSpawnsRunOnOwnerAndAreStolen verifies that a task
// running on a worker can spawn children by capturing the scheduler and
// its own context, that those children land on the spawning worker's
// local deque (not the injector), and — by starving every other worker
// of injected work — that the pool only completes them via stealing.
func TestSubmit_NestedSpawnsRunOnOwnerAndAreStolen(t *testing.T) {
	s := New(Config{Workers: 4})
	defer s.Stop()

	const children = 500
	var done atomic.Int64

	parent := Submit(context.Background(), s, PriorityStandard, func(ctx context.Context) (int, error) {
		for i := 0; i < children; i++ {
			Submit(ctx, s, PriorityStandard, func(context.Context) (struct{}, error) {
				done.Add(1)
				return struct{}{}, nil
			})
		}
		return children, nil
	})
	if res := parent.Wait(); res.Err != nil || res.Value != children {
		t.Fatalf("parent result = %+v, want {%d <nil>}", res, children)
	}

	deadline := time.Now().Add(5 * time.Second)
	for done.Load() != children && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := done.Load(); got != children {
		t.Fatalf("completed children = %d, want %d", got, children)
	}

	if stats := s.Stats(); stats.Stolen == 0 {
		t.Error("Stats().Stolen = 0, want > 0 — expected idle workers to steal the spawned children")
	}
}

// TestScheduler_StandardBeforeBackground verifies that, absent any
// aging, PriorityStandard is always served ahead of PriorityBackground
// when both are queued.
func TestScheduler_StandardBeforeBackground(t *testing.T) {
	s := New(Config{Workers: 1, AgingThreshold: time.Hour})
	defer s.Stop()

	var mu sync.Mutex
	var order []string
	record := func(name string) Func[struct{}] {
		return func(context.Context) (struct{}, error) {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return struct{}{}, nil
		}
	}

	// Block the single worker so both priorities queue up before either
	// is dequeued, making the priority order observable.
	block := make(chan struct{})
	Submit(context.Background(), s, PriorityStandard, func(context.Context) (struct{}, error) {
		<-block
		return struct{}{}, nil
	})
	time.Sleep(20 * time.Millisecond)

	bg := Submit(context.Background(), s, PriorityBackground, record("background"))
	std := Submit(context.Background(), s, PriorityStandard, record("standard"))
	close(block)
	bg.Wait()
	std.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "standard" || order[1] != "background" {
		t.Fatalf("execution order = %v, want [standard background]", order)
	}
}

// TestScheduler_AgingPromotesBackgroundTask verifies the core aging
// guarantee: a PriorityBackground task is served within roughly its
// AgingThreshold even while a continuous PriorityStandard backlog would
// otherwise keep losing every priority check indefinitely. The backlog
// is sized so its full drain time is well past threshold, so a
// background completion time close to threshold — not close to the
// full drain — is only possible if aging actually promoted it.
func TestScheduler_AgingPromotesBackgroundTask(t *testing.T) {
	const threshold = 80 * time.Millisecond
	const perTaskWork = 5 * time.Millisecond
	const standardBacklog = 40 // 200ms of guaranteed work, well past threshold

	s := New(Config{Workers: 1, AgingThreshold: threshold})
	defer s.Stop()

	for i := 0; i < standardBacklog; i++ {
		Submit(context.Background(), s, PriorityStandard, func(context.Context) (struct{}, error) {
			time.Sleep(perTaskWork)
			return struct{}{}, nil
		})
	}

	start := time.Now()
	bg := Submit(context.Background(), s, PriorityBackground, func(context.Context) (struct{}, error) {
		return struct{}{}, nil
	})
	res := bg.Wait()
	elapsed := time.Since(start)

	if res.Err != nil {
		t.Fatalf("background task error = %v", res.Err)
	}
	if elapsed < threshold-10*time.Millisecond {
		t.Fatalf("background task completed after %v, before its ~%v aging threshold — the standard backlog wasn't actually competing with it", elapsed, threshold)
	}
	if fullDrain := time.Duration(standardBacklog) * perTaskWork; elapsed >= fullDrain {
		t.Fatalf("background task took %v — that's the full standard backlog drain time (%v), aging never promoted it ahead of the backlog", elapsed, fullDrain)
	}
}

// TestScheduler_BackgroundRunsImmediatelyWhenStandardIdle verifies that
// PriorityBackground work is not artificially delayed until its aging
// threshold when there is no competing PriorityStandard backlog — the
// threshold is only a bound on how long it waits behind competing
// work, not a minimum delay.
func TestScheduler_BackgroundRunsImmediatelyWhenStandardIdle(t *testing.T) {
	const threshold = 500 * time.Millisecond
	s := New(Config{Workers: 2, AgingThreshold: threshold})
	defer s.Stop()

	start := time.Now()
	res := Submit(context.Background(), s, PriorityBackground, func(context.Context) (struct{}, error) {
		return struct{}{}, nil
	}).Wait()
	elapsed := time.Since(start)

	if res.Err != nil {
		t.Fatalf("background task error = %v", res.Err)
	}
	if elapsed >= threshold/2 {
		t.Fatalf("background task with no competing standard work took %v, want near-immediate (threshold is %v)", elapsed, threshold)
	}
}

func TestStop_UnblocksPromptly(t *testing.T) {
	s := New(Config{Workers: 4})
	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return in time")
	}
}

func TestSubmit_AfterStop_ReturnsErrStopped(t *testing.T) {
	s := New(Config{Workers: 4})
	s.Stop()

	fut := Submit(context.Background(), s, PriorityStandard, func(context.Context) (int, error) {
		t.Fatal("fn should never run: scheduler is stopped")
		return 0, nil
	})
	res := fut.Wait()
	if !errors.Is(res.Err, ErrStopped) {
		t.Fatalf("Wait().Err = %v, want ErrStopped", res.Err)
	}
}

func TestSubmit_ConcurrentExternalSubmitters(t *testing.T) {
	s := New(Config{Workers: runtime.NumCPU()})
	defer s.Stop()

	const submitters = 16
	const perSubmitter = 2000
	var wg sync.WaitGroup
	var completed atomic.Int64

	for i := 0; i < submitters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perSubmitter; j++ {
				Submit(context.Background(), s, PriorityStandard, func(context.Context) (int, error) {
					completed.Add(1)
					return 0, nil
				}).Wait()
			}
		}()
	}
	wg.Wait()

	want := int64(submitters * perSubmitter)
	if got := completed.Load(); got != want {
		t.Fatalf("completed = %d, want %d", got, want)
	}
}

func BenchmarkSubmit_Trivial(b *testing.B) {
	s := New(Config{Workers: 0})
	defer s.Stop()

	b.ReportAllocs()
	for b.Loop() {
		Submit(context.Background(), s, PriorityStandard, func(context.Context) (int, error) {
			return 0, nil
		}).Wait()
	}
}

func BenchmarkSubmit_ParallelExternal(b *testing.B) {
	s := New(Config{Workers: 0})
	defer s.Stop()

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Submit(context.Background(), s, PriorityStandard, func(context.Context) (int, error) {
				return 0, nil
			}).Wait()
		}
	})
}

// BenchmarkSubmit_NestedFanout measures the scheduler's core work-stealing
// path: one root task fans out N children from within a worker.
func BenchmarkSubmit_NestedFanout(b *testing.B) {
	s := New(Config{Workers: 0})
	defer s.Stop()
	const fanout = 64

	b.ReportAllocs()
	for b.Loop() {
		root := Submit(context.Background(), s, PriorityStandard, func(ctx context.Context) (int, error) {
			futs := make([]*Future[int], fanout)
			for i := range futs {
				futs[i] = Submit(ctx, s, PriorityStandard, func(context.Context) (int, error) {
					return 1, nil
				})
			}
			sum := 0
			for _, f := range futs {
				sum += f.Wait().Value
			}
			return sum, nil
		})
		if res := root.Wait(); res.Value != fanout {
			b.Fatalf("root result = %d, want %d", res.Value, fanout)
		}
	}
}
