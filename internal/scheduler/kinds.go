package scheduler

import "context"

// LLMResult is the outcome of an LLMFunc.
type LLMResult struct {
	Text string
}

// LLMFunc calls an LLM — e.g. crawl-source config generation or
// content judgement — and returns its text output.
type LLMFunc func(ctx context.Context) (LLMResult, error)

// SubmitLLM schedules fn on b's KindLLM pool at priority.
func SubmitLLM(ctx context.Context, b *Bulkhead, priority Priority, fn LLMFunc) *Future[LLMResult] {
	return Submit(ctx, b.Pool(KindLLM), priority, Func[LLMResult](fn))
}

// FetchResult is the outcome of a FetchFunc.
type FetchResult struct {
	Body       []byte
	StatusCode int
}

// FetchFunc performs a plain, unproxied HTTP fetch.
type FetchFunc func(ctx context.Context) (FetchResult, error)

// SubmitFetch schedules fn on b's KindFetch pool at priority.
func SubmitFetch(ctx context.Context, b *Bulkhead, priority Priority, fn FetchFunc) *Future[FetchResult] {
	return Submit(ctx, b.Pool(KindFetch), priority, Func[FetchResult](fn))
}

// ProxyFetchResult is the outcome of a ProxyFetchFunc.
type ProxyFetchResult struct {
	Body       []byte
	StatusCode int
}

// ProxyFetchFunc performs an HTTP fetch routed through a proxy pool.
type ProxyFetchFunc func(ctx context.Context) (ProxyFetchResult, error)

// SubmitProxyFetch schedules fn on b's KindProxyFetch pool at priority.
func SubmitProxyFetch(ctx context.Context, b *Bulkhead, priority Priority, fn ProxyFetchFunc) *Future[ProxyFetchResult] {
	return Submit(ctx, b.Pool(KindProxyFetch), priority, Func[ProxyFetchResult](fn))
}

// ChromedpResult is the outcome of a ChromedpFunc.
type ChromedpResult struct {
	HTML       string
	StatusCode int
}

// ChromedpFunc performs a locally-hosted headless-browser render.
type ChromedpFunc func(ctx context.Context) (ChromedpResult, error)

// SubmitChromedp schedules fn on b's KindChromedp pool at priority.
func SubmitChromedp(ctx context.Context, b *Bulkhead, priority Priority, fn ChromedpFunc) *Future[ChromedpResult] {
	return Submit(ctx, b.Pool(KindChromedp), priority, Func[ChromedpResult](fn))
}

// TwoCaptchaResult is the outcome of a TwoCaptchaFunc.
type TwoCaptchaResult struct {
	HTML       string
	StatusCode int
}

// TwoCaptchaFunc performs a remote, 2Captcha-hosted headless-browser
// render — backed by a different, independently-capped resource than
// ChromedpFunc, even though both drive a browser.
type TwoCaptchaFunc func(ctx context.Context) (TwoCaptchaResult, error)

// SubmitTwoCaptcha schedules fn on b's KindTwoCaptcha pool at priority.
func SubmitTwoCaptcha(ctx context.Context, b *Bulkhead, priority Priority, fn TwoCaptchaFunc) *Future[TwoCaptchaResult] {
	return Submit(ctx, b.Pool(KindTwoCaptcha), priority, Func[TwoCaptchaResult](fn))
}
