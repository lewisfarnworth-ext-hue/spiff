package httphandlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"spiff/internal/netfetch"
	spifffs "spiff/internal/spiff_fs"
	"spiff/internal/spifferrs"
	"sync"
	"testing"
)

const (
	filesRoutePrefix = "/files/"
	compareRoute     = "/compare"
	fetchRoute       = "/fetch"
)

// fakeFetcher implements ports.Fetcher without touching the network, so
// NewFetchHandler tests/benchmarks exercise only the handler's
// persist-and-return logic.
type fakeFetcher struct {
	body []byte
	err  error
}

func (f *fakeFetcher) Fetch(ctx context.Context, url string) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	return io.NopCloser(bytes.NewReader(f.body)), nil
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestUploadHandler(t *testing.T) {
	dir := t.TempDir()
	handler := NewUploadHandler(spifffs.New(dir))

	body := []byte(`{"hello":"world"}`)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	id := rec.Body.String()
	if !uuidPattern.MatchString(id) {
		t.Fatalf("response body = %q, want RFC 4122 v4 UUID", id)
	}

	got, err := os.ReadFile(filepath.Join(dir, id))
	if err != nil {
		t.Fatalf("reading persisted file: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("persisted body = %q, want %q", got, body)
	}
}

func TestUploadHandler_MethodNotAllowed(t *testing.T) {
	handler := NewUploadHandler(spifffs.New(t.TempDir()))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestDownloadHandler(t *testing.T) {
	dir := t.TempDir()
	F := spifffs.New(dir)
	want := []byte(`{"hello":"world"}`)
	id, err := F.SaveFile(bytes.NewReader(want))
	if err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}

	handler := NewDownloadHandler(F, filesRoutePrefix)
	req := httptest.NewRequest(http.MethodGet, filesRoutePrefix+id, nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !bytes.Equal(rec.Body.Bytes(), want) {
		t.Fatalf("body = %q, want %q", rec.Body.Bytes(), want)
	}
}

func TestDownloadHandler_InvalidID(t *testing.T) {
	handler := NewDownloadHandler(spifffs.New(t.TempDir()), filesRoutePrefix)
	req := httptest.NewRequest(http.MethodGet, filesRoutePrefix+"../../etc/passwd", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestDownloadHandler_NotFound(t *testing.T) {
	handler := NewDownloadHandler(spifffs.New(t.TempDir()), filesRoutePrefix)
	req := httptest.NewRequest(http.MethodGet, filesRoutePrefix+"11111111-1111-4111-8111-111111111111", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestDownloadHandler_MethodNotAllowed(t *testing.T) {
	handler := NewDownloadHandler(spifffs.New(t.TempDir()), filesRoutePrefix)
	req := httptest.NewRequest(http.MethodPost, filesRoutePrefix+"anything", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestCompareHandler(t *testing.T) {
	F := spifffs.New(t.TempDir())
	idLeft, err := F.SaveFile(bytes.NewReader([]byte("one\ntwo")))
	if err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}
	idRight, err := F.SaveFile(bytes.NewReader([]byte("one\nTWO")))
	if err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}

	handler := NewCompareHandler(F)
	req := httptest.NewRequest(http.MethodGet, compareRoute+"?left="+idLeft+"&right="+idRight, nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	want := "  one\n- two\n+ TWO\n"
	if rec.Body.String() != want {
		t.Fatalf("body = %q, want %q", rec.Body.String(), want)
	}
}

func TestCompareHandler_InvalidID(t *testing.T) {
	handler := NewCompareHandler(spifffs.New(t.TempDir()))
	req := httptest.NewRequest(http.MethodGet, compareRoute+"?left=not-a-uuid&right=also-not", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestCompareHandler_NotFound(t *testing.T) {
	F := spifffs.New(t.TempDir())
	idLeft, err := F.SaveFile(bytes.NewReader([]byte("hi")))
	if err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}

	handler := NewCompareHandler(F)
	req := httptest.NewRequest(http.MethodGet, compareRoute+"?left="+idLeft+"&right=11111111-1111-4111-8111-111111111111", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestCompareHandler_MethodNotAllowed(t *testing.T) {
	handler := NewCompareHandler(spifffs.New(t.TempDir()))
	req := httptest.NewRequest(http.MethodPost, compareRoute, nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestFetchHandler(t *testing.T) {
	dir := t.TempDir()
	want := []byte("<html><body>hi</body></html>")
	handler := NewFetchHandler(spifffs.New(dir), &fakeFetcher{body: want})

	req := httptest.NewRequest(http.MethodPost, fetchRoute, bytes.NewReader([]byte("http://example.com")))
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !bytes.Equal(rec.Body.Bytes(), want) {
		t.Fatalf("body = %q, want %q", rec.Body.Bytes(), want)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading data dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d persisted files, want 1", len(entries))
	}
	got, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatalf("reading persisted file: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("persisted body = %q, want %q", got, want)
	}
}

func TestFetchHandler_MethodNotAllowed(t *testing.T) {
	handler := NewFetchHandler(spifffs.New(t.TempDir()), &fakeFetcher{})
	req := httptest.NewRequest(http.MethodGet, fetchRoute, nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestFetchHandler_InvalidURL(t *testing.T) {
	handler := NewFetchHandler(spifffs.New(t.TempDir()), &fakeFetcher{err: spifferrs.ErrInvalidURL})
	req := httptest.NewRequest(http.MethodPost, fetchRoute, bytes.NewReader([]byte("not-a-url")))
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestFetchHandler_UpstreamError(t *testing.T) {
	handler := NewFetchHandler(spifffs.New(t.TempDir()), &fakeFetcher{err: spifferrs.ErrFetchFailed})
	req := httptest.NewRequest(http.MethodPost, fetchRoute, bytes.NewReader([]byte("http://example.com")))
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadGateway)
	}
}

// TestFetchHandler_Stress fires many concurrent requests at a shared handler
// backed by a real netfetch.Fetcher hitting a local httptest server, to shake
// out races/deadlocks across the handler, FS, and pools (run with go test -race).
func TestFetchHandler_Stress(t *testing.T) {
	want := bytes.Repeat([]byte("x"), 4096)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(want)
	}))
	defer target.Close()

	handler := NewFetchHandler(spifffs.New(t.TempDir()), netfetch.New())

	const goroutines = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, fetchRoute, bytes.NewReader([]byte(target.URL)))
			rec := httptest.NewRecorder()
			handler(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("status = %d, want %d (body: %s)", rec.Code, http.StatusOK, rec.Body.String())
				return
			}
			if !bytes.Equal(rec.Body.Bytes(), want) {
				t.Errorf("body = %d bytes, want %d bytes matching fixture", rec.Body.Len(), len(want))
			}
		}()
	}
	wg.Wait()
}

var body = bytes.Repeat([]byte("x"), 4096)

func BenchmarkUploadHandler(b *testing.B) {
	handler := NewUploadHandler(spifffs.New(b.TempDir()))

	b.ReportAllocs()
	for b.Loop() {
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handler(rec, req)
	}
}

func BenchmarkDownloadHandler(b *testing.B) {
	F := spifffs.New(b.TempDir())
	id, err := F.SaveFile(bytes.NewReader(body))
	if err != nil {
		b.Fatal(err)
	}
	handler := NewDownloadHandler(F, filesRoutePrefix)

	b.ReportAllocs()
	for b.Loop() {
		req := httptest.NewRequest(http.MethodGet, filesRoutePrefix+id, nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
	}
}

// benchLines builds n distinct lines, used to size the compare benchmark independent of the fixed upload body above.
func benchLines(n int) [][]byte {
	lines := make([][]byte, n)
	for i := range lines {
		lines[i] = []byte(fmt.Sprintf("line-%d", i))
	}
	return lines
}

func BenchmarkCompareHandler(b *testing.B) {
	F := spifffs.New(b.TempDir())
	leftBody := bytes.Join(benchLines(500), []byte("\n"))
	rightLines := benchLines(500)
	rightLines[250] = []byte("line-250-changed")
	rightBody := bytes.Join(rightLines, []byte("\n"))

	idLeft, err := F.SaveFile(bytes.NewReader(leftBody))
	if err != nil {
		b.Fatal(err)
	}
	idRight, err := F.SaveFile(bytes.NewReader(rightBody))
	if err != nil {
		b.Fatal(err)
	}
	handler := NewCompareHandler(F)
	url := compareRoute + "?left=" + idLeft + "&right=" + idRight

	b.ReportAllocs()
	for b.Loop() {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
	}
}

// BenchmarkFetchHandler matches BenchmarkUploadHandler's 4KB fixture, using a
// fakeFetcher to measure the persist-and-return path (FS + pool usage)
// independent of network/transport cost.
func BenchmarkFetchHandler(b *testing.B) {
	handler := NewFetchHandler(spifffs.New(b.TempDir()), &fakeFetcher{body: body})

	b.ReportAllocs()
	for b.Loop() {
		req := httptest.NewRequest(http.MethodPost, fetchRoute, bytes.NewReader([]byte("http://example.com")))
		rec := httptest.NewRecorder()
		handler(rec, req)
	}
}
