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
)
