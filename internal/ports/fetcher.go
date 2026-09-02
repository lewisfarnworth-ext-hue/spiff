// Package ports holds interfaces for external I/O, so callers depend on a
// seam rather than a specific transport implementation.
package ports

import (
	"context"
	"io"
)

// Fetcher retrieves the body at url (e.g. via an HTTP GET). The caller owns
// the returned handle and must Close it. Implementations own the transport
// (net/http, ...) behind this interface. Canceling ctx aborts an in-flight
// fetch.
type Fetcher interface {
	Fetch(ctx context.Context, url string) (io.ReadCloser, error)
}
