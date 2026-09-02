package utils

import (
	"bufio"
	"io"
	"sync"
)

var (
	uuidbufpool = sync.Pool{
		New: func() any {
			return new([16]byte)
		},
	}

	// WriterPool reuses bufio.Writers (and their backing buffers) across requests
	// instead of allocating one per upload, and batches writes to cut syscalls.
	WriterPool = sync.Pool{
		New: func() any {
			return bufio.NewWriterSize(io.Discard, 32*1024)
		},
	}

	// ChunkPool reuses fixed-size buffers for copying streamed file bodies
	// (e.g. gRPC Download) instead of allocating one per call. Pooled as
	// *[]byte per sync.Pool convention, to avoid boxing the slice header.
	ChunkPool = sync.Pool{
		New: func() any {
			b := make([]byte, 32*1024)
			return &b
		},
	}
)
