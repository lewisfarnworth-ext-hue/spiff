// Package grpcserver exposes spiff_fs.FS over gRPC (via Kitex, running on
// cloudwego/netpoll's HTTP/2 transport), mirroring httphandlers.
package grpcserver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"

	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/codes"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/status"

	"spiff/internal/grpcserver/kitex_gen/spiffpb"
	"spiff/internal/ports"
	spifffs "spiff/internal/spiff_fs"
	"spiff/internal/spifferrs"
	"spiff/internal/utils"
)

// Handler implements spiffpb.Spiff by delegating to a spiff_fs.FS and a
// ports.Fetcher.
type Handler struct {
	F       *spifffs.FS
	Fetcher ports.Fetcher
}

// NewHandler binds a handler to F and fetcher, keeping both injectable for tests.
func NewHandler(F *spifffs.FS, fetcher ports.Fetcher) *Handler {
	return &Handler{F: F, Fetcher: fetcher}
}

// Upload reads chunks from the client stream and saves them under a fresh ID.
func (h *Handler) Upload(stream spiffpb.Spiff_UploadServer) error {
	id, err := h.F.SaveFile(&chunkReader{stream: stream})
	if err != nil {
		return toStatus("upload", err)
	}
	return stream.SendAndClose(&spiffpb.UploadResponse{Id: id})
}

// Download streams a previously saved file's contents back to the client in chunks.
func (h *Handler) Download(req *spiffpb.DownloadRequest, stream spiffpb.Spiff_DownloadServer) error {
	rc, err := h.F.GetSavedFile(req.Id)
	if err != nil {
		return toStatus("download", err)
	}
	defer rc.Close()

	buf := utils.ChunkPool.Get().(*[]byte)
	defer utils.ChunkPool.Put(buf)

	if _, err := io.CopyBuffer(&chunkWriter{stream: stream}, rc, *buf); err != nil {
		return toStatus("download", err)
	}
	return nil
}

// Compare returns a line-level diff between two previously saved files.
func (h *Handler) Compare(ctx context.Context, req *spiffpb.CompareRequest) (*spiffpb.CompareResponse, error) {
	diff, err := h.F.CompareFiles(req.LeftId, req.RightId)
	if err != nil {
		return nil, toStatus("compare", err)
	}
	return &spiffpb.CompareResponse{Diff: diff}, nil
}

// Fetch retrieves req.Url's body, persists it via F, and returns the full body.
func (h *Handler) Fetch(ctx context.Context, req *spiffpb.FetchRequest) (*spiffpb.FetchResponse, error) {
	rc, err := h.Fetcher.Fetch(ctx, req.Url)
	if err != nil {
		return nil, toStatus("fetch", err)
	}
	defer rc.Close()

	// Buffered fully in memory for the same reason as httphandlers.NewFetchHandler:
	// the body must be both persisted and returned to the caller.
	var buf bytes.Buffer
	buf.Grow(32 * 1024) // typical page size; avoids regrowth for the common case
	bufPtr := utils.ChunkPool.Get().(*[]byte)
	defer utils.ChunkPool.Put(bufPtr)
	if _, err := io.CopyBuffer(&buf, rc, *bufPtr); err != nil {
		return nil, toStatus("fetch", err)
	}

	if _, err := h.F.SaveFile(bytes.NewReader(buf.Bytes())); err != nil {
		return nil, toStatus("fetch", err)
	}

	return &spiffpb.FetchResponse{Html: buf.Bytes()}, nil
}

// chunkReader adapts a Spiff_UploadServer stream to an io.Reader, so the
// existing FS.SaveFile(io.Reader) can be reused as-is instead of duplicating
// its buffering/persistence logic for the streaming RPC.
type chunkReader struct {
	stream spiffpb.Spiff_UploadServer
	buf    []byte
}

func (r *chunkReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		chunk, err := r.stream.Recv()
		if err != nil {
			return 0, err
		}
		r.buf = chunk.Data
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// chunkWriter adapts a Spiff_DownloadServer stream to an io.Writer, so
// io.CopyBuffer can stream FS.GetSavedFile's io.ReadCloser without an
// intermediate manual read/send loop.
type chunkWriter struct {
	stream spiffpb.Spiff_DownloadServer
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	// stream.Send marshals p to wire bytes synchronously before returning, so
	// p is safe for the caller (io.CopyBuffer) to reuse once this returns.
	if err := w.stream.Send(&spiffpb.Chunk{Data: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}

// toStatus maps spifferrs sentinel errors to gRPC status codes, mirroring
// httphandlers' HTTP status mapping for the same error set.
func toStatus(op string, err error) error {
	switch {
	case errors.Is(err, spifferrs.ErrInvalidID):
		return status.Err(codes.InvalidArgument, "invalid id")
	case errors.Is(err, spifferrs.ErrNotFound):
		return status.Err(codes.NotFound, "not found")
	case errors.Is(err, spifferrs.ErrDiffTableTooLarge):
		return status.Err(codes.ResourceExhausted, "files too large and too different to compare")
	case errors.Is(err, spifferrs.ErrInvalidURL):
		return status.Err(codes.InvalidArgument, "invalid url")
	case errors.Is(err, spifferrs.ErrFetchFailed):
		return status.Err(codes.Unavailable, "fetch failed")
	default:
		log.Printf("%s failed: %v", op, err)
		return status.Err(codes.Internal, "internal error")
	}
}
