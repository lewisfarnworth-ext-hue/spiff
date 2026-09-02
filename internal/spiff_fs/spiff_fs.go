package spifffs

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"spiff/internal/spifferrs"
	"spiff/internal/utils"
)

type FS struct {
	dataDir string
}

func New(dir string) *FS {
	return &FS{
		dataDir: dir,
	}
}

func (f FS) DataDir() string {
	return f.dataDir
}

// SaveFile writes body to f.dataDir under a fresh UUID filename and returns that ID.
func (f *FS) SaveFile(body io.Reader) (string, error) {
	id, err := utils.NewUUID()
	if err != nil {
		return "", spifferrs.ErrIDGeneration
	}

	F, err := os.Create(filepath.Join(f.dataDir, id))
	if err != nil {
		return "", errors.Join(spifferrs.ErrPersist, err)
	}
	defer F.Close()

	bw := utils.WriterPool.Get().(*bufio.Writer)
	bw.Reset(F)
	defer func() {
		bw.Reset(io.Discard)
		utils.WriterPool.Put(bw)
	}()

	if _, err := io.Copy(bw, body); err != nil {
		return "", errors.Join(spifferrs.ErrPersist, err)
	}
	if err := bw.Flush(); err != nil {
		return "", errors.Join(spifferrs.ErrPersist)
	}

	return id, nil
}

// GetSavedFile opens a file previously written at dataDir.ID under dir. The
// caller owns the returned handle and must Close it. Returning an
// io.ReadCloser instead of a []byte avoids buffering the whole file in
// memory when the caller only needs to stream it elsewhere.
func (f *FS) GetSavedFile(id string) (io.ReadCloser, error) {
	if !utils.IsValidUUID(id) {
		return nil, spifferrs.ErrInvalidID
	}

	F, err := os.Open(filepath.Join(f.dataDir, id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, spifferrs.ErrNotFound
		}
		return nil, err
	}
	return F, nil
}

// CompareFiles loads two saved files identified by idLeft and idRight
// and returns a line-level diff of their contents, deletions from the left
// file marked '-' and additions from the right marked '+'.
func (f *FS) CompareFiles(idLeft, idRight string) (string, error) {
	L, err := f.GetSavedFile(idLeft)
	if err != nil {
		return "", err
	}
	LLines, err := utils.ReadLines(L)
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", idLeft, err)
	}

	R, err := f.GetSavedFile(idRight)
	if err != nil {
		return "", err
	}
	RLines, err := utils.ReadLines(R)
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", idRight, err)
	}

	ops, err := utils.DiffLines(LLines, RLines)
	if err != nil {
		return "", err
	}
	return utils.FormatDiff(ops), nil
}
