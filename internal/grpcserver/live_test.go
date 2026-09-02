package grpcserver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/codes"
	"github.com/cloudwego/kitex/server"

	"spiff/internal/grpcserver/kitex_gen/spiffpb"
	"spiff/internal/grpcserver/kitex_gen/spiffpb/spiff"
	"spiff/internal/netfetch"
	"spiff/internal/ports"
	spifffs "spiff/internal/spiff_fs"
)

// Kitex's default klog logger writes Info-level messages (listen
// announcements, graceful-shutdown start/end, and one "loopyWriter.run
// returning ... EOF" per connection torn down in svr.Stop()) straight to
// stderr. That's routine noise from every live server this file starts
// and stops, and go test forwards it into the same stream as benchmark
// output — where scripts/bench.sh's `tee` captures it into the baseline
// file. Dropping the level to Warn silences it while still surfacing
// anything Kitex considers an actual problem.
func init() {
	klog.SetLevel(klog.LevelWarn)
}

// startLiveServer runs a real Kitex/gRPC server (real protobuf marshaling,
// real HTTP/2 framing, real netpoll-backed network I/O) on a loopback port
// and returns a client dialed against it. Unlike the fake-stream based tests
// above, this exercises the actual wire protocol.
func startLiveServer(tb testing.TB, F *spifffs.FS, fetcher ports.Fetcher) spiff.Client {
	tb.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		tb.Fatalf("resolve addr: %v", err)
	}

	svr := spiff.NewServer(NewHandler(F, fetcher), server.WithServiceAddr(tcpAddr))
	runErr := make(chan error, 1)
	go func() { runErr <- svr.Run() }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		select {
		case err := <-runErr:
			tb.Fatalf("server exited before it started listening: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			tb.Fatalf("server did not start listening on %s in time: %v", addr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cli, err := spiff.NewClient("spiff", client.WithHostPorts(addr))
	if err != nil {
		tb.Fatalf("new client: %v", err)
	}

	tb.Cleanup(func() { svr.Stop() })

	return cli
}

func liveUpload(tb testing.TB, cli spiff.Client, body []byte) string {
	tb.Helper()

	up, err := cli.Upload(context.Background())
	if err != nil {
		tb.Fatalf("upload open: %v", err)
	}
	for _, c := range chunksOf(body, 32*1024) {
		if err := up.Send(&spiffpb.Chunk{Data: c}); err != nil {
			tb.Fatalf("upload send: %v", err)
		}
	}
	resp, err := up.CloseAndRecv()
	if err != nil {
		tb.Fatalf("upload close: %v", err)
	}
	return resp.Id
}

func liveDownload(tb testing.TB, cli spiff.Client, id string) []byte {
	tb.Helper()

	dl, err := cli.Download(context.Background(), &spiffpb.DownloadRequest{Id: id})
	if err != nil {
		tb.Fatalf("download open: %v", err)
	}
	var buf bytes.Buffer
	for {
		chunk, err := dl.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			tb.Fatalf("download recv: %v", err)
		}
		buf.Write(chunk.Data)
	}
	return buf.Bytes()
}

func TestLiveGRPC_RoundTrip(t *testing.T) {
	cli := startLiveServer(t, spifffs.New(t.TempDir()), &fakeFetcher{})

	body := []byte("line one\nline two\nline three\n")
	id := liveUpload(t, cli, body)

	if got := liveDownload(t, cli, id); !bytes.Equal(got, body) {
		t.Fatalf("downloaded %q, want %q", got, body)
	}

	id2 := liveUpload(t, cli, []byte("line one\nline TWO\nline three\n"))
	resp, err := cli.Compare(context.Background(), &spiffpb.CompareRequest{LeftId: id, RightId: id2})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if resp.Diff == "" {
		t.Fatalf("compare returned an empty diff for differing inputs")
	}

	// Error path over the real wire: invalid id must come back as a gRPC status.
	badStream, err := cli.Download(context.Background(), &spiffpb.DownloadRequest{Id: "not-a-uuid"})
	if err == nil {
		_, err = badStream.Recv()
	}
	assertCode(t, err, codes.InvalidArgument)
}

// benchLinesLive builds n distinct lines, mirroring httphandlers' benchLines
// so BenchmarkLiveGRPC_Compare exercises the same-shaped workload.
func benchLinesLive(n int) [][]byte {
	lines := make([][]byte, n)
	for i := range lines {
		lines[i] = []byte(fmt.Sprintf("line-%d", i))
	}
	return lines
}

// BenchmarkLiveGRPC_Upload matches BenchmarkUploadHandler's 4KB payload for a
// fair comparison against the HTTP server.
func BenchmarkLiveGRPC_Upload(b *testing.B) {
	cli := startLiveServer(b, spifffs.New(b.TempDir()), &fakeFetcher{})
	body := bytes.Repeat([]byte("x"), 4096)

	b.ReportAllocs()
	for b.Loop() {
		liveUpload(b, cli, body)
	}
}

// BenchmarkLiveGRPC_Download matches BenchmarkDownloadHandler's 4KB payload.
func BenchmarkLiveGRPC_Download(b *testing.B) {
	cli := startLiveServer(b, spifffs.New(b.TempDir()), &fakeFetcher{})
	id := liveUpload(b, cli, bytes.Repeat([]byte("x"), 4096))

	b.ReportAllocs()
	for b.Loop() {
		liveDownload(b, cli, id)
	}
}

// BenchmarkLiveGRPC_Compare matches BenchmarkCompareHandler's 500-line, one-diff payload.
func BenchmarkLiveGRPC_Compare(b *testing.B) {
	cli := startLiveServer(b, spifffs.New(b.TempDir()), &fakeFetcher{})

	leftLines := benchLinesLive(500)
	rightLines := benchLinesLive(500)
	rightLines[250] = []byte("line-250-changed")

	idLeft := liveUpload(b, cli, bytes.Join(leftLines, []byte("\n")))
	idRight := liveUpload(b, cli, bytes.Join(rightLines, []byte("\n")))

	b.ReportAllocs()
	for b.Loop() {
		if _, err := cli.Compare(context.Background(), &spiffpb.CompareRequest{LeftId: idLeft, RightId: idRight}); err != nil {
			b.Fatal(err)
		}
	}
}

// TestLiveGRPC_Fetch exercises the real end-to-end path: a live Kitex client
// calling a live server whose Handler.Fetch uses the real netfetch.Fetcher
// (net/http client) against a local httptest target.
func TestLiveGRPC_Fetch(t *testing.T) {
	want := []byte("<html><body>live hi</body></html>")
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(want)
	}))
	defer target.Close()

	dir := t.TempDir()
	cli := startLiveServer(t, spifffs.New(dir), netfetch.New())

	resp, err := cli.Fetch(context.Background(), &spiffpb.FetchRequest{Url: target.URL})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if !bytes.Equal(resp.Html, want) {
		t.Fatalf("Html = %q, want %q", resp.Html, want)
	}

	// Error path over the real wire: an invalid url must come back as a gRPC status.
	_, err = cli.Fetch(context.Background(), &spiffpb.FetchRequest{Url: "not-a-url"})
	assertCode(t, err, codes.InvalidArgument)
}

// TestLiveGRPC_Fetch_HTTPS exercises the netfetch TLS path end-to-end
// through the live gRPC server.
func TestLiveGRPC_Fetch_HTTPS(t *testing.T) {
	want := []byte("<html><body>secure live hi</body></html>")
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(want)
	}))
	defer target.Close()

	fetcher := &netfetch.Fetcher{Client: target.Client()}

	cli := startLiveServer(t, spifffs.New(t.TempDir()), fetcher)

	resp, err := cli.Fetch(context.Background(), &spiffpb.FetchRequest{Url: target.URL})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if !bytes.Equal(resp.Html, want) {
		t.Fatalf("Html = %q, want %q", resp.Html, want)
	}
}

// BenchmarkLiveGRPC_Fetch matches the other BenchmarkLiveGRPC_* benchmarks'
// 4KB payload, fetched from a local httptest target through the real
// netfetch.Fetcher.
func BenchmarkLiveGRPC_Fetch(b *testing.B) {
	body := bytes.Repeat([]byte("x"), 4096)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body)
	}))
	defer target.Close()

	cli := startLiveServer(b, spifffs.New(b.TempDir()), netfetch.New())

	b.ReportAllocs()
	for b.Loop() {
		if _, err := cli.Fetch(context.Background(), &spiffpb.FetchRequest{Url: target.URL}); err != nil {
			b.Fatal(err)
		}
	}
}

// TestLiveGRPC_Fetch_Stress fires many concurrent Fetch calls from a live
// client at a live server backed by the real netfetch.Fetcher, to shake
// out races/deadlocks across the whole Kitex + netfetch stack (run with
// go test -race).
func TestLiveGRPC_Fetch_Stress(t *testing.T) {
	want := bytes.Repeat([]byte("x"), 4096)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(want)
	}))
	defer target.Close()

	cli := startLiveServer(t, spifffs.New(t.TempDir()), netfetch.New())

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			resp, err := cli.Fetch(context.Background(), &spiffpb.FetchRequest{Url: target.URL})
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
