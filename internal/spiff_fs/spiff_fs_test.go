package spifffs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"spiff/internal/spifferrs"
	"spiff/internal/utils"
	"testing"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestFS_SaveFile(t *testing.T) {
	dir := t.TempDir()
	body := []byte("hello, disk")

	id, err := New(dir).SaveFile(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}
	if !uuidPattern.MatchString(id) {
		t.Fatalf("SaveFile() id = %q, want RFC 4122 v4 UUID", id)
	}

	got, err := os.ReadFile(filepath.Join(dir, id))
	if err != nil {
		t.Fatalf("reading persisted file: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("persisted body = %q, want %q", got, body)
	}
}

func TestFS_SaveFile_PersistFailure(t *testing.T) {
	// A nonexistent directory makes os.Create fail, which should surface as ErrPersist.
	_, err := New(filepath.Join(t.TempDir(), "does-not-exist")).SaveFile(bytes.NewReader(nil))
	if !errors.Is(err, spifferrs.ErrPersist) {
		t.Fatalf("SaveFile() error = %v, want ErrPersist", err)
	}
}

func TestFS_GetSavedFile(t *testing.T) {
	dir := t.TempDir()
	want := []byte("hello, disk")
	F := New(dir)
	id, err := F.SaveFile(bytes.NewReader(want))
	if err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}

	rc, err := F.GetSavedFile(id)
	if err != nil {
		t.Fatalf("GetSavedFile() error = %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading from GetSavedFile(): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("GetSavedFile() content = %q, want %q", got, want)
	}
}

func TestFS_GetSavedFile_InvalidID(t *testing.T) {
	F := New(t.TempDir())
	for _, id := range []string{
		"../../etc/passwd",
		"not-a-uuid",
		"",
		"11111111-1111-1111-1111-11111111111", // one char short
	} {
		if _, err := F.GetSavedFile(id); !errors.Is(err, spifferrs.ErrInvalidID) {
			t.Errorf("GetSavedFile(%q) error = %v, want ErrInvalidID", id, err)
		}
	}
}

func TestFS_GetSavedFile_NotFound(t *testing.T) {
	F := New(t.TempDir())
	id, err := utils.NewUUID()
	if err != nil {
		t.Fatalf("NewUUID() error = %v", err)
	}

	if _, err := F.GetSavedFile(id); !errors.Is(err, spifferrs.ErrNotFound) {
		t.Fatalf("GetSavedFile() error = %v, want ErrNotFound", err)
	}
}

func TestFS_CompareFiles(t *testing.T) {
	F := New(t.TempDir())
	idLeft, err := F.SaveFile(bytes.NewReader([]byte("one\ntwo\nthree")))
	if err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}
	idRight, err := F.SaveFile(bytes.NewReader([]byte("one\nTWO\nthree")))
	if err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}

	got, err := F.CompareFiles(idLeft, idRight)
	if err != nil {
		t.Fatalf("CompareFiles() error = %v", err)
	}
	want := "  one\n- two\n+ TWO\n  three\n"
	if got != want {
		t.Fatalf("CompareFiles() = %q, want %q", got, want)
	}
}

func TestFS_CompareFiles_InvalidID(t *testing.T) {
	F := New(t.TempDir())
	id, err := F.SaveFile(bytes.NewReader([]byte("hi")))
	if err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}

	if _, err := F.CompareFiles("not-a-uuid", id); !errors.Is(err, spifferrs.ErrInvalidID) {
		t.Fatalf("CompareFiles() error = %v, want ErrInvalidID", err)
	}
	if _, err := F.CompareFiles(id, "not-a-uuid"); !errors.Is(err, spifferrs.ErrInvalidID) {
		t.Fatalf("CompareFiles() error = %v, want ErrInvalidID", err)
	}
}

func TestFS_CompareFiles_NotFound(t *testing.T) {
	F := New(t.TempDir())
	idLeft, err := F.SaveFile(bytes.NewReader([]byte("hi")))
	if err != nil {
		t.Fatalf("SaveFile() error = %v", err)
	}
	missing, err := utils.NewUUID()
	if err != nil {
		t.Fatalf("NewUUID() error = %v", err)
	}

	if _, err := F.CompareFiles(idLeft, missing); !errors.Is(err, spifferrs.ErrNotFound) {
		t.Fatalf("CompareFiles() error = %v, want ErrNotFound", err)
	}
}

func BenchmarkFS_SaveFile(b *testing.B) {
	F := New(b.TempDir())
	body := bytes.Repeat([]byte("x"), 4096)

	b.ReportAllocs()
	for b.Loop() {
		if _, err := F.SaveFile(bytes.NewReader(body)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFS_GetSavedFile(b *testing.B) {
	F := New(b.TempDir())
	id, err := F.SaveFile(bytes.NewReader(bytes.Repeat([]byte("x"), 4096)))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		rc, err := F.GetSavedFile(id)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, rc); err != nil {
			b.Fatal(err)
		}
		rc.Close()
	}
}

// benchLines builds n distinct lines, used to size the compare benchmark independent of the fixed body used elsewhere.
func benchLines(n int) [][]byte {
	lines := make([][]byte, n)
	for i := range lines {
		lines[i] = []byte(fmt.Sprintf("line-%d", i))
	}
	return lines
}

func BenchmarkFS_CompareFiles(b *testing.B) {
	F := New(b.TempDir())
	leftBody := bytes.Join(benchLines(500), []byte("\n"))
	rightLines := benchLines(500)
	rightLines[250] = []byte("line-250-changed") // one substitution to exercise the LCS backtrack
	rightBody := bytes.Join(rightLines, []byte("\n"))

	idLeft, err := F.SaveFile(bytes.NewReader(leftBody))
	if err != nil {
		b.Fatal(err)
	}
	idRight, err := F.SaveFile(bytes.NewReader(rightBody))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := F.CompareFiles(idLeft, idRight); err != nil {
			b.Fatal(err)
		}
	}
}
