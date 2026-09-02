package netfetch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"spiff/internal/spifferrs"
)

func TestFetcher_Fetch(t *testing.T) {
	want := []byte("<html><body>hi</body></html>")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(want)
	}))
	defer srv.Close()

	rc, err := New().Fetch(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestFetcher_Fetch_InvalidURL(t *testing.T) {
	for _, u := range []string{"not-a-url", "ftp://example.com", "", "http://"} {
		if _, err := New().Fetch(context.Background(), u); !errors.Is(err, spifferrs.ErrInvalidURL) {
			t.Errorf("Fetch(%q) error = %v, want ErrInvalidURL", u, err)
		}
	}
}

func TestFetcher_Fetch_UpstreamError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()

	if _, err := New().Fetch(context.Background(), srv.URL); !errors.Is(err, spifferrs.ErrFetchFailed) {
		t.Fatalf("Fetch() error = %v, want ErrFetchFailed", err)
	}
}

// TestFetcher_Fetch_ContextCanceled asserts that Fetch honors ctx rather than
// only ever timing out at the Client's blanket timeout.
func TestFetcher_Fetch_ContextCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := New().Fetch(ctx, srv.URL); !errors.Is(err, spifferrs.ErrFetchFailed) {
		t.Fatalf("Fetch() error = %v, want ErrFetchFailed", err)
	}
}

func TestFetcher_Fetch_ConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := srv.URL
	srv.Close() // closed, so the port is not accepting connections

	if _, err := New().Fetch(context.Background(), addr); !errors.Is(err, spifferrs.ErrFetchFailed) {
		t.Fatalf("Fetch() error = %v, want ErrFetchFailed", err)
	}
}

// TestFetcher_Fetch_Stress fires many concurrent Fetch calls against a real
// local server to shake out races/deadlocks in the shared http.Client (run
// with go test -race).
func TestFetcher_Fetch_Stress(t *testing.T) {
	want := bytes.Repeat([]byte("x"), 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(want)
	}))
	defer srv.Close()

	fetcher := New()
	const goroutines = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			rc, err := fetcher.Fetch(context.Background(), srv.URL)
			if err != nil {
				t.Errorf("Fetch() error = %v", err)
				return
			}
			defer rc.Close()
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Errorf("reading body: %v", err)
				return
			}
			if !bytes.Equal(got, want) {
				t.Errorf("body = %d bytes, want %d bytes matching fixture", len(got), len(want))
			}
		}()
	}
	wg.Wait()
}

func BenchmarkFetcher_Fetch(b *testing.B) {
	body := bytes.Repeat([]byte("x"), 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body)
	}))
	defer srv.Close()
	fetcher := New()

	b.ReportAllocs()
	for b.Loop() {
		rc, err := fetcher.Fetch(context.Background(), srv.URL)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, rc); err != nil {
			b.Fatal(err)
		}
		rc.Close()
	}
}
