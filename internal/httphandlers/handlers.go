package httphandlers

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"net/http"
	"spiff/internal/ports"
	spifffs "spiff/internal/spiff_fs"
	"spiff/internal/spifferrs"
	"spiff/internal/utils"
	"strings"
)

// NewUploadHandler binds a handler to fs.dataDir, keeping the destination injectable for tests.
func NewUploadHandler(F *spifffs.FS) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, spifferrs.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
			return
		}

		id, err := F.SaveFile(r.Body)
		if err != nil {
			log.Printf("upload failed: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.Write([]byte(id))
	}
}

// NewDownloadHandler binds a handler to dir, keeping the source directory injectable for tests.
func NewDownloadHandler(F *spifffs.FS, filesRoutePrefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, spifferrs.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
			return
		}

		id := strings.TrimPrefix(r.URL.Path, filesRoutePrefix)

		rc, err := F.GetSavedFile(id)
		if err != nil {
			switch {
			case errors.Is(err, spifferrs.ErrInvalidID):
				http.Error(w, "invalid id", http.StatusBadRequest)
			case errors.Is(err, spifferrs.ErrNotFound):
				http.Error(w, "not found", http.StatusNotFound)
			default:
				log.Printf("download failed: %v", err)
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
			return
		}
		defer rc.Close()

		// Reuse the same pooled bufio.Writer as uploads to batch writes to
		// the response and avoid allocating a fresh copy buffer per request.
		bw := utils.WriterPool.Get().(*bufio.Writer)
		bw.Reset(w)
		defer func() {
			bw.Reset(io.Discard)
			utils.WriterPool.Put(bw)
		}()

		w.Header().Set("Content-Type", "application/octet-stream")
		if _, err := io.Copy(bw, rc); err != nil {
			log.Printf("download failed: %v", err)
			return
		}
		bw.Flush()
	}
}

// NewCompareHandler binds a handler to dir, keeping the source directory injectable for tests.
func NewCompareHandler(F *spifffs.FS) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, spifferrs.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
			return
		}

		q := r.URL.Query()
		diff, err := F.CompareFiles(q.Get("left"), q.Get("right"))
		if err != nil {
			switch {
			case errors.Is(err, spifferrs.ErrInvalidID):
				http.Error(w, "invalid id", http.StatusBadRequest)
			case errors.Is(err, spifferrs.ErrNotFound):
				http.Error(w, "not found", http.StatusNotFound)
			case errors.Is(err, spifferrs.ErrDiffTableTooLarge):
				http.Error(w, "files too large and too different to compare", http.StatusRequestEntityTooLarge)
			default:
				log.Printf("compare failed: %v", err)
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte(diff))
	}
}

// NewFetchHandler binds a handler to F and fetcher, keeping both injectable for tests.
// The request body is the raw target URL (no JSON), matching NewUploadHandler's
// raw-body convention.
func NewFetchHandler(F *spifffs.FS, fetcher ports.Fetcher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, spifferrs.ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
			return
		}

		urlBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}

		rc, err := fetcher.Fetch(r.Context(), strings.TrimSpace(string(urlBytes)))
		if err != nil {
			switch {
			case errors.Is(err, spifferrs.ErrInvalidURL):
				http.Error(w, "invalid url", http.StatusBadRequest)
			case errors.Is(err, spifferrs.ErrFetchFailed):
				http.Error(w, "fetch failed", http.StatusBadGateway)
			default:
				log.Printf("fetch failed: %v", err)
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
			return
		}
		defer rc.Close()

		// Buffered fully in memory (rather than streamed like Upload/Download) because
		// the body must be both persisted and returned; sizes are bounded HTML pages, not
		// arbitrary large files, so this mirrors CompareFiles' already-in-memory model.
		var buf bytes.Buffer
		buf.Grow(32 * 1024) // typical page size; avoids regrowth for the common case
		bufPtr := utils.ChunkPool.Get().(*[]byte)
		defer utils.ChunkPool.Put(bufPtr)
		if _, err := io.CopyBuffer(&buf, rc, *bufPtr); err != nil {
			log.Printf("fetch failed: %v", err)
			http.Error(w, "internal error", http.StatusBadGateway)
			return
		}

		if _, err := F.SaveFile(bytes.NewReader(buf.Bytes())); err != nil {
			log.Printf("fetch persist failed: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(buf.Bytes())
	}
}
