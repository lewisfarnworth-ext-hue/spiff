package grpcserver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/codes"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/status"

	"spiff/internal/grpcserver/kitex_gen/spiffpb"
	spifffs "spiff/internal/spiff_fs"
	"spiff/internal/spifferrs"
)

// fakeUploadStream feeds a fixed sequence of chunks to chunkReader/Handler.Upload
// without needing a real network connection. Unused Spiff_UploadServer/streaming.Stream
// methods are inherited (nil) from the embedded interface and are never called
// by the code under test.
type fakeUploadStream struct {
	spiffpb.Spiff_UploadServer
	chunks [][]byte
	i      int

	closedResp *spiffpb.UploadResponse
}

func (f *fakeUploadStream) Recv() (*spiffpb.Chunk, error) {
	if f.i >= len(f.chunks) {
		return nil, io.EOF
	}
	d := f.chunks[f.i]
	f.i++
	return &spiffpb.Chunk{Data: d}, nil
}

func (f *fakeUploadStream) SendAndClose(resp *spiffpb.UploadResponse) error {
	f.closedResp = resp
	return nil
}

// fakeDownloadStream captures everything sent via chunkWriter/Handler.Download.
type fakeDownloadStream struct {
	spiffpb.Spiff_DownloadServer
	buf bytes.Buffer
}

func (f *fakeDownloadStream) Send(c *spiffpb.Chunk) error {
	f.buf.Write(c.Data)
	return nil
}

// discardDownloadStream drops sent chunks instead of buffering them, so
// benchmarks measure chunkWriter/Handler.Download itself rather than a
// growing bytes.Buffer's reallocations.
type discardDownloadStream struct {
	spiffpb.Spiff_DownloadServer
}

func (discardDownloadStream) Send(*spiffpb.Chunk) error { return nil }

// fakeFetcher implements ports.Fetcher without touching the network, so
// Handler.Fetch tests/benchmarks exercise only the handler's persist-and-
// return logic.
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

func chunksOf(body []byte, size int) [][]byte {
	var chunks [][]byte
	for len(body) > 0 {
		n := size
		if n > len(body) {
			n = len(body)
		}
		chunks = append(chunks, body[:n])
		body = body[n:]
	}
	return chunks
}

func TestChunkReader_Read_ReassemblesAcrossBoundaries(t *testing.T) {
	body := bytes.Repeat([]byte("abcdefghij"), 1000) // not a multiple of the read/chunk sizes below
	r := &chunkReader{stream: &fakeUploadStream{chunks: chunksOf(body, 7)}}

	got, err := io.ReadAll(&smallReader{r: r, size: 3})
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("reassembled %d bytes, want %d bytes matching input", len(got), len(body))
	}
}

// smallReader forces io.ReadAll to call Read with small buffers, exercising
// chunkReader's partial-read/leftover-buffer path.
type smallReader struct {
	r    io.Reader
	size int
}

func (s *smallReader) Read(p []byte) (int, error) {
	if len(p) > s.size {
		p = p[:s.size]
	}
	return s.r.Read(p)
}

func TestChunkWriter_Write(t *testing.T) {
	fs := &fakeDownloadStream{}
	w := &chunkWriter{stream: fs}

	body := bytes.Repeat([]byte("x"), 100_000)
	buf := make([]byte, 4096)
	if _, err := io.CopyBuffer(w, bytes.NewReader(body), buf); err != nil {
		t.Fatalf("CopyBuffer() error = %v", err)
	}
	if !bytes.Equal(fs.buf.Bytes(), body) {
		t.Fatalf("got %d bytes, want %d bytes matching input", fs.buf.Len(), len(body))
	}
}

func TestHandler_UploadDownloadRoundTrip(t *testing.T) {
	h := NewHandler(spifffs.New(t.TempDir()), &fakeFetcher{})
	body := []byte("line one\nline two\nline three\n")

	up := &fakeUploadStream{chunks: chunksOf(body, 5)}
	if err := h.Upload(up); err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if up.closedResp == nil || up.closedResp.Id == "" {
		t.Fatalf("Upload() did not send an ID via SendAndClose")
	}

	dl := &fakeDownloadStream{}
	if err := h.Download(&spiffpb.DownloadRequest{Id: up.closedResp.Id}, dl); err != nil {
		t.Fatalf("Download() error = %v", err)
	}
	if !bytes.Equal(dl.buf.Bytes(), body) {
		t.Fatalf("downloaded %q, want %q", dl.buf.Bytes(), body)
	}
}

func TestHandler_Compare(t *testing.T) {
	h := NewHandler(spifffs.New(t.TempDir()), &fakeFetcher{})

	leftUp := &fakeUploadStream{chunks: [][]byte{[]byte("a\nb\nc\n")}}
	if err := h.Upload(leftUp); err != nil {
		t.Fatalf("Upload(left) error = %v", err)
	}
	rightUp := &fakeUploadStream{chunks: [][]byte{[]byte("a\nB\nc\n")}}
	if err := h.Upload(rightUp); err != nil {
		t.Fatalf("Upload(right) error = %v", err)
	}

	resp, err := h.Compare(context.Background(), &spiffpb.CompareRequest{
		LeftId:  leftUp.closedResp.Id,
		RightId: rightUp.closedResp.Id,
	})
	if err != nil {
		t.Fatalf("Compare() error = %v", err)
	}
	if resp.Diff == "" {
		t.Fatalf("Compare() returned an empty diff for differing inputs")
	}
}

func TestHandler_ErrorMapping(t *testing.T) {
	h := NewHandler(spifffs.New(t.TempDir()), &fakeFetcher{})

	t.Run("invalid id", func(t *testing.T) {
		err := h.Download(&spiffpb.DownloadRequest{Id: "not-a-uuid"}, &fakeDownloadStream{})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("not found", func(t *testing.T) {
		err := h.Download(&spiffpb.DownloadRequest{Id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}, &fakeDownloadStream{})
		assertCode(t, err, codes.NotFound)
	})
}

func TestHandler_Fetch(t *testing.T) {
	dir := t.TempDir()
	want := []byte("<html><body>hi</body></html>")
	h := NewHandler(spifffs.New(dir), &fakeFetcher{body: want})

	resp, err := h.Fetch(context.Background(), &spiffpb.FetchRequest{Url: "http://example.com"})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if !bytes.Equal(resp.Html, want) {
		t.Fatalf("Html = %q, want %q", resp.Html, want)
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

func TestHandler_Fetch_ErrorMapping(t *testing.T) {
	t.Run("invalid url", func(t *testing.T) {
		h := NewHandler(spifffs.New(t.TempDir()), &fakeFetcher{err: spifferrs.ErrInvalidURL})
		_, err := h.Fetch(context.Background(), &spiffpb.FetchRequest{Url: "not-a-url"})
		assertCode(t, err, codes.InvalidArgument)
	})

	t.Run("fetch failed", func(t *testing.T) {
		h := NewHandler(spifffs.New(t.TempDir()), &fakeFetcher{err: spifferrs.ErrFetchFailed})
		_, err := h.Fetch(context.Background(), &spiffpb.FetchRequest{Url: "http://example.com"})
		assertCode(t, err, codes.Unavailable)
	})
}

// TestHandler_Fetch_Stress fires many concurrent Fetch calls at a shared
// Handler to shake out races/deadlocks in the FS/pool usage (run with
// go test -race).
func TestHandler_Fetch_Stress(t *testing.T) {
	want := bytes.Repeat([]byte("x"), 4096)
	h := NewHandler(spifffs.New(t.TempDir()), &fakeFetcher{body: want})

	const goroutines = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			resp, err := h.Fetch(context.Background(), &spiffpb.FetchRequest{Url: "http://example.com"})
			if err != nil {
				t.Errorf("Fetch() error = %v", err)
				return
			}
			if !bytes.Equal(resp.Html, want) {
				t.Errorf("Html = %d bytes, want %d bytes matching fixture", len(resp.Html), len(want))
			}
		}()
	}
	wg.Wait()
}

func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("got nil error, want status code %v", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error %v is not a gRPC status error", err)
	}
	if st.Code() != want {
		t.Fatalf("got code %v, want %v", st.Code(), want)
	}
}

var errSentinel = errors.New("sentinel")

func TestToStatus_DefaultsToInternal(t *testing.T) {
	assertCode(t, toStatus("op", errSentinel), codes.Internal)
}

func BenchmarkChunkReader_Read(b *testing.B) {
	body := bytes.Repeat([]byte("x"), 1<<20) // 1MiB
	chunks := chunksOf(body, 32*1024)
	readBuf := make([]byte, 32*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		r := &chunkReader{stream: &fakeUploadStream{chunks: chunks}}
		for {
			_, err := r.Read(readBuf)
			if err != nil {
				if err == io.EOF {
					break
				}
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkChunkWriter_Write(b *testing.B) {
	body := bytes.Repeat([]byte("x"), 1<<20) // 1MiB
	copyBuf := make([]byte, 32*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		w := &chunkWriter{stream: discardDownloadStream{}}
		if _, err := io.CopyBuffer(w, bytes.NewReader(body), copyBuf); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHandler_Upload_InProcess calls Handler.Upload directly against a
// fake stream, bypassing Kitex's protobuf marshaling, HTTP/2 framing, and
// network I/O entirely. It measures the FS + chunk-adapter layer only; see
// BenchmarkLiveGRPC_Upload in live_test.go for a real end-to-end comparison
// against the HTTP server's benchmarks.
func BenchmarkHandler_Upload_InProcess(b *testing.B) {
	fs := spifffs.New(b.TempDir())
	h := NewHandler(fs, &fakeFetcher{})
	body := bytes.Repeat([]byte("x"), 64*1024)
	chunks := chunksOf(body, 32*1024)

	b.ReportAllocs()
	for b.Loop() {
		if err := h.Upload(&fakeUploadStream{chunks: chunks}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHandler_Download_InProcess is the Download counterpart to
// BenchmarkHandler_Upload_InProcess; see its comment for what this does and
// does not measure.
func BenchmarkHandler_Download_InProcess(b *testing.B) {
	fs := spifffs.New(b.TempDir())
	h := NewHandler(fs, &fakeFetcher{})
	body := bytes.Repeat([]byte("x"), 64*1024)
	up := &fakeUploadStream{chunks: chunksOf(body, 32*1024)}
	if err := h.Upload(up); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := h.Download(&spiffpb.DownloadRequest{Id: up.closedResp.Id}, discardDownloadStream{}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHandler_Fetch_InProcess matches BenchmarkFetchHandler's 4KB
// fixture, using a fakeFetcher to measure the persist-and-return path
// (FS + pool usage) independent of network/transport cost; see
// BenchmarkLiveGRPC_Fetch in live_test.go for a real end-to-end comparison.
func BenchmarkHandler_Fetch_InProcess(b *testing.B) {
	fs := spifffs.New(b.TempDir())
	h := NewHandler(fs, &fakeFetcher{body: bytes.Repeat([]byte("x"), 4096)})

	b.ReportAllocs()
	for b.Loop() {
		if _, err := h.Fetch(context.Background(), &spiffpb.FetchRequest{Url: "http://example.com"}); err != nil {
			b.Fatal(err)
		}
	}
}
