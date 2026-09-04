package scheduler

import (
	"context"
	"errors"
	"spiff/internal/spifferrs"
	"testing"
	"time"
)

func allKindConfigs(perKind Config) map[Kind]Config {
	cfgs := make(map[Kind]Config, len(AllKinds()))
	for _, k := range AllKinds() {
		cfgs[k] = perKind
	}
	return cfgs
}

func TestNewBulkhead_MissingKindErrors(t *testing.T) {
	cfgs := allKindConfigs(Config{Workers: 1})
	delete(cfgs, KindChromedp)

	_, err := NewBulkhead(cfgs)
	if err == nil {
		t.Fatal("NewBulkhead() with a missing kind = nil error, want an error")
	}
}

func TestNewBulkhead_AllKindsConfigured(t *testing.T) {
	b, err := NewBulkhead(allKindConfigs(Config{Workers: 1}))
	if err != nil {
		t.Fatalf("NewBulkhead() error = %v", err)
	}
	defer b.Stop()

	for _, k := range AllKinds() {
		if b.Pool(k) == nil {
			t.Fatalf("Pool(%q) = nil", k)
		}
	}
}

func TestBulkhead_PoolUnknownKindPanics(t *testing.T) {
	b, err := NewBulkhead(allKindConfigs(Config{Workers: 1}))
	if err != nil {
		t.Fatalf("NewBulkhead() error = %v", err)
	}
	defer b.Stop()

	defer func() {
		if recover() == nil {
			t.Fatal("Pool() with an unknown kind did not panic")
		}
	}()
	b.Pool(Kind("not-a-real-kind"))
}

// TestBulkhead_KindIsolation verifies a burst against one Kind's pool
// cannot consume another Kind's workers — the entire point of
// bulkheading. KindChromedp is starved (0 workers ever free, via a
// permanently-blocking task occupying its single worker); KindFetch,
// sized separately, must still make progress.
func TestBulkhead_KindIsolation(t *testing.T) {
	cfgs := allKindConfigs(Config{Workers: 1})
	b, err := NewBulkhead(cfgs)
	if err != nil {
		t.Fatalf("NewBulkhead() error = %v", err)
	}
	defer b.Stop()

	block := make(chan struct{})
	defer close(block)
	SubmitChromedp(context.Background(), b, PriorityStandard, func(context.Context) (ChromedpResult, error) {
		<-block
		return ChromedpResult{}, nil
	})

	done := make(chan struct{})
	go func() {
		SubmitFetch(context.Background(), b, PriorityStandard, func(context.Context) (FetchResult, error) {
			return FetchResult{StatusCode: 200}, nil
		}).Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("KindFetch task never completed — KindChromedp's stuck worker starved it, bulkheading failed")
	}
}

func TestBulkhead_NamedSubmitRoutesToCorrectPool(t *testing.T) {
	b, err := NewBulkhead(allKindConfigs(Config{Workers: 2}))
	if err != nil {
		t.Fatalf("NewBulkhead() error = %v", err)
	}
	defer b.Stop()

	llm := SubmitLLM(context.Background(), b, PriorityStandard, func(context.Context) (LLMResult, error) {
		return LLMResult{Text: "config"}, nil
	})
	fetch := SubmitFetch(context.Background(), b, PriorityStandard, func(context.Context) (FetchResult, error) {
		return FetchResult{Body: []byte("html"), StatusCode: 200}, nil
	})
	proxy := SubmitProxyFetch(context.Background(), b, PriorityStandard, func(context.Context) (ProxyFetchResult, error) {
		return ProxyFetchResult{StatusCode: 200}, nil
	})
	chrome := SubmitChromedp(context.Background(), b, PriorityStandard, func(context.Context) (ChromedpResult, error) {
		return ChromedpResult{StatusCode: 200}, nil
	})
	captcha := SubmitTwoCaptcha(context.Background(), b, PriorityStandard, func(context.Context) (TwoCaptchaResult, error) {
		return TwoCaptchaResult{StatusCode: 200}, nil
	})

	if res := llm.Wait(); res.Err != nil || res.Value.Text != "config" {
		t.Fatalf("SubmitLLM result = %+v", res)
	}
	if res := fetch.Wait(); res.Err != nil || res.Value.StatusCode != 200 {
		t.Fatalf("SubmitFetch result = %+v", res)
	}
	if res := proxy.Wait(); res.Err != nil || res.Value.StatusCode != 200 {
		t.Fatalf("SubmitProxyFetch result = %+v", res)
	}
	if res := chrome.Wait(); res.Err != nil || res.Value.StatusCode != 200 {
		t.Fatalf("SubmitChromedp result = %+v", res)
	}
	if res := captcha.Wait(); res.Err != nil || res.Value.StatusCode != 200 {
		t.Fatalf("SubmitTwoCaptcha result = %+v", res)
	}

	stats := b.Stats()
	for _, k := range AllKinds() {
		if stats[k].Submitted != 1 || stats[k].Completed != 1 {
			t.Fatalf("Stats()[%q] = %+v, want Submitted=Completed=1", k, stats[k])
		}
	}
}

func TestBulkhead_Stop_StopsEveryPool(t *testing.T) {
	b, err := NewBulkhead(allKindConfigs(Config{Workers: 1}))
	if err != nil {
		t.Fatalf("NewBulkhead() error = %v", err)
	}

	done := make(chan struct{})
	go func() {
		b.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Bulkhead.Stop() did not return in time")
	}

	res := SubmitLLM(context.Background(), b, PriorityStandard, func(context.Context) (LLMResult, error) {
		t.Fatal("fn should never run: bulkhead is stopped")
		return LLMResult{}, nil
	}).Wait()
	if !errors.Is(res.Err, spifferrs.ErrStopped) {
		t.Fatalf("Wait().Err after Bulkhead.Stop() = %v, want ErrStopped", res.Err)
	}
}

func BenchmarkBulkhead_SubmitLLM(b *testing.B) {
	bh, err := NewBulkhead(allKindConfigs(Config{Workers: 0}))
	if err != nil {
		b.Fatalf("NewBulkhead() error = %v", err)
	}
	defer bh.Stop()

	b.ReportAllocs()
	for b.Loop() {
		SubmitLLM(context.Background(), bh, PriorityStandard, func(context.Context) (LLMResult, error) {
			return LLMResult{}, nil
		}).Wait()
	}
}
