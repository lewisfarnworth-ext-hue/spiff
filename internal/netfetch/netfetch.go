// Package netfetch implements ports.Fetcher using net/http.Client — the
// standard, idiomatic way to make an outbound HTTP request with Go's net
// package. It backs the HTTP server's Fetch endpoint.
//
// The Transport is tuned for many-host, high-churn outbound traffic (a
// scheduler fanning out hundreds of concurrent fetches against distinct
// external services around the clock) rather than left on net/http's
// low-concurrency defaults: a larger per-host idle pool so repeat requests
// to the same host reuse a connection instead of redialing, a per-host cap
// so one very active host can't monopolize the pool, and a TLS session
// cache so repeat HTTPS connections can resume instead of paying a full
// handshake.
package netfetch

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"spiff/internal/spifferrs"
)

const (
	defaultTimeout = 10 * time.Second
	dialTimeout    = 10 * time.Second

	// maxIdlePerHost bounds idle connections kept per host; raised well
	// above net/http's default of 2 so bursts to the same external service
	// reuse connections instead of redialing.
	maxIdlePerHost = 16

	// maxConnsPerHost bounds total (dialing+active+idle) connections per
	// host, so a single very active or slow host can't open unbounded
	// simultaneous connections against itself.
	maxConnsPerHost = 64

	// maxIdleConns is the global idle-connection budget across all hosts,
	// sized for a fan-out of hundreds of distinct external services.
	maxIdleConns = 512

	// tlsSessionCacheSize bounds the LRU cache of TLS session tickets, so
	// resumption stays available across a similarly wide host fan-out
	// without evicting actively-used entries.
	tlsSessionCacheSize = 256

	idleConnTimeout       = 90 * time.Second
	tlsHandshakeTimeout   = 10 * time.Second
	expectContinueTimeout = 1 * time.Second
	dialKeepAlive         = 30 * time.Second
)

// Fetcher retrieves a URL's body via an http.Client.
type Fetcher struct {
	Client *http.Client
}

// New returns a Fetcher with a sane request timeout and a Transport tuned
// for sustained, high-concurrency, many-host outbound traffic.
func New() *Fetcher {
	return &Fetcher{
		Client: &http.Client{
			Timeout:   defaultTimeout,
			Transport: newTransport(),
		},
	}
}

func newTransport() *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: dialKeepAlive,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			ClientSessionCache: tls.NewLRUClientSessionCache(tlsSessionCacheSize),
		},
		// ForceAttemptHTTP2 is required as soon as TLSClientConfig is set
		// explicitly: net/http only auto-negotiates HTTP/2 when
		// TLSClientConfig is left nil, unless this is also set.
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdlePerHost,
		MaxConnsPerHost:       maxConnsPerHost,
		IdleConnTimeout:       idleConnTimeout,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ExpectContinueTimeout: expectContinueTimeout,
	}
}

// Fetch issues a GET for rawURL and returns its body. The caller owns the
// returned handle and must Close it. Canceling ctx aborts an in-flight
// fetch without waiting out the Client's blanket timeout.
func (f *Fetcher) Fetch(ctx context.Context, rawURL string) (io.ReadCloser, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, spifferrs.ErrInvalidURL
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, spifferrs.ErrInvalidURL
	}

	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, errors.Join(spifferrs.ErrFetchFailed, err)
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, spifferrs.ErrFetchFailed
	}
	return resp.Body, nil
}
