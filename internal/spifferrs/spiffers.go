package spifferrs

import "errors"

// Sentinel errors for known failure states, matched via errors.Is by callers/tests.
var (
	ErrMethodNotAllowed = errors.New("method not allowed")
	ErrIDGeneration     = errors.New("id generation failed")

	ErrPersist = errors.New("failed to persist body")

	ErrInvalidID = errors.New("invalid id")

	ErrNotFound = errors.New("file not found")

	// ErrDiffTableTooLarge is returned by DiffLinesLCS (and, via its fallback,
	// DiffLines) instead of attempting an allocation beyond maxLCSTableBytes.
	ErrDiffTableTooLarge = errors.New("diff table too large")

	// ErrInvalidURL is returned by Fetcher implementations for a URL that
	// isn't a parseable, absolute http(s) URL.
	ErrInvalidURL = errors.New("invalid url")

	// ErrFetchFailed is returned by Fetcher implementations when dialing,
	// writing the request to, or reading the response from the target
	// fails, or the target responds with a 4xx/5xx status.
	ErrFetchFailed = errors.New("fetch failed")

	// ErrStopped is returned via Result.Err by Submit when the scheduler
	// has already been (or is concurrently being) stopped, since a task
	// accepted after that point would otherwise sit in a queue forever with
	// no worker left to run it.
	ErrStopped = errors.New("scheduler: stopped")

	// ErrQueueFull is returned via Result.Err by Submit when priority's
	// shared queue is already at its configured depth cap
	// (Config.MaxStandardQueueDepth or Config.MaxBackgroundQueueDepth).
	// This stays a single package-level sentinel rather than one instance
	// per Kind or per queue: the caller already knows which Kind and
	// Priority it submitted with — it chose the SubmitXxx function and the
	// Priority argument itself — so there's no ambiguity a bespoke error
	// value would resolve. Wrap it with fmt.Errorf/%w at the call site if a
	// caller wants that context attached to the error string.
	ErrQueueFull = errors.New("scheduler: queue full")
)
