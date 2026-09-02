package scheduler

import (
	"fmt"
	"sync"
)

// Kind identifies which bulkheaded pool a task belongs to. Each Kind
// gets its own Scheduler, sized to that resource's own real capacity,
// so a burst against one Kind can never consume workers that belong to
// another — see the package doc for the failure mode this prevents.
type Kind string

const (
	// KindLLM is a call to an LLM API (e.g. config generation or
	// content judgement).
	KindLLM Kind = "llm"
	// KindFetch is a plain, unproxied HTTP fetch.
	KindFetch Kind = "fetch"
	// KindProxyFetch is an HTTP fetch routed through a proxy pool.
	KindProxyFetch Kind = "proxy_fetch"
	// KindChromedp is a locally-hosted headless-browser render.
	KindChromedp Kind = "chromedp"
	// KindTwoCaptcha is a remote, hosted-browser render — a distinct
	// resource from KindChromedp, with its own independent capacity
	// and quota, even though both are "a browser" at a glance.
	KindTwoCaptcha Kind = "two_captcha"
)

// AllKinds returns every known Kind. NewBulkhead requires a Config for
// each of these.
func AllKinds() []Kind {
	return []Kind{KindLLM, KindFetch, KindProxyFetch, KindChromedp, KindTwoCaptcha}
}

// Bulkhead owns one Scheduler per Kind. Submitting through the wrong
// Kind's pool is impossible by construction: the named SubmitXxx
// functions in kinds.go each close over exactly one Kind.
type Bulkhead struct {
	pools map[Kind]*Scheduler
}

// NewBulkhead starts one Scheduler per Kind, using the Config supplied
// for that Kind, and returns an error if any Kind in AllKinds is
// missing from configs. It fails loudly rather than defaulting a
// missing Kind's Config, because a resource pool sized by accident
// (e.g. silently falling back to runtime.NumCPU workers for a
// 2-session browser pool) is exactly the cross-kind starvation failure
// mode bulkheading exists to prevent.
func NewBulkhead(configs map[Kind]Config) (*Bulkhead, error) {
	b := &Bulkhead{pools: make(map[Kind]*Scheduler, len(AllKinds()))}
	for _, k := range AllKinds() {
		cfg, ok := configs[k]
		if !ok {
			return nil, fmt.Errorf("scheduler: missing Config for kind %q", k)
		}
		b.pools[k] = New(cfg)
	}
	return b, nil
}

// Pool returns the Scheduler backing kind. It panics if kind is not
// one of AllKinds, since that is only reachable via a programming
// error — NewBulkhead already validates every known Kind is
// configured, so a valid Bulkhead always has one.
func (b *Bulkhead) Pool(kind Kind) *Scheduler {
	s, ok := b.pools[kind]
	if !ok {
		panic(fmt.Sprintf("scheduler: unknown kind %q", kind))
	}
	return s
}

// Stop stops every pool in parallel and waits for all of them to
// finish. Parallel because each pool's Stop already blocks until its
// own workers drain; stopping pools one at a time would only sum their
// shutdown latencies for no benefit.
func (b *Bulkhead) Stop() {
	var wg sync.WaitGroup
	wg.Add(len(b.pools))
	for _, s := range b.pools {
		go func(s *Scheduler) {
			defer wg.Done()
			s.Stop()
		}(s)
	}
	wg.Wait()
}

// Stats returns a snapshot of every pool's activity, keyed by Kind.
func (b *Bulkhead) Stats() map[Kind]Stats {
	out := make(map[Kind]Stats, len(b.pools))
	for k, s := range b.pools {
		out[k] = s.Stats()
	}
	return out
}
