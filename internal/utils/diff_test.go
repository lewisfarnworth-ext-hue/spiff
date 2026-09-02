package utils

import (
	"errors"
	"fmt"
	"math"
	"spiff/internal/spifferrs"
	"testing"
)

var diffLineCases = []struct {
	name string
	a, b []string
	want string
}{
	{
		name: "identical",
		a:    []string{"one", "two", "three"},
		b:    []string{"one", "two", "three"},
		want: "  one\n  two\n  three\n",
	},
	{
		name: "addition",
		a:    []string{"one", "three"},
		b:    []string{"one", "two", "three"},
		want: "  one\n+ two\n  three\n",
	},
	{
		name: "deletion",
		a:    []string{"one", "two", "three"},
		b:    []string{"one", "three"},
		want: "  one\n- two\n  three\n",
	},
	{
		name: "replacement",
		a:    []string{"one", "two"},
		b:    []string{"one", "TWO"},
		want: "  one\n- two\n+ TWO\n",
	},
	{
		name: "both empty",
		a:    nil,
		b:    nil,
		want: "",
	},
	{
		name: "left empty",
		a:    nil,
		b:    []string{"one"},
		want: "+ one\n",
	},
	{
		name: "right empty",
		a:    []string{"one"},
		b:    nil,
		want: "- one\n",
	},
}

func toByteLines(ss []string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func TestDiffLinesLCS(t *testing.T) {
	for _, tt := range diffLineCases {
		t.Run(tt.name, func(t *testing.T) {
			ops, err := DiffLinesLCS(toByteLines(tt.a), toByteLines(tt.b))
			if err != nil {
				t.Fatalf("DiffLinesLCS() error = %v", err)
			}
			if got := FormatDiff(ops); got != tt.want {
				t.Fatalf("DiffLinesLCS() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDiffLinesLCS_ErrorsWhenTableTooLarge(t *testing.T) {
	// Table size scales with len(a)*len(b); nil-content slices are enough
	// to trigger the guard without allocating real data.
	n := maxSafeLCSLines() + 1000 // comfortably over the safe boundary
	a := make([][]byte, n)
	b := make([][]byte, n)

	_, err := DiffLinesLCS(a, b)
	if !errors.Is(err, spifferrs.ErrDiffTableTooLarge) {
		t.Fatalf("DiffLinesLCS() error = %v, want ErrDiffTableTooLarge", err)
	}
}

func TestDiffLinesMyers(t *testing.T) {
	for _, tt := range diffLineCases {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatDiff(DiffLinesMyers(toByteLines(tt.a), toByteLines(tt.b)))
			if got != tt.want {
				t.Fatalf("DiffLinesMyers() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDiffLinesMyers_AlwaysConverges(t *testing.T) {
	// DiffLinesMyers passes an unbounded (len(a)+len(b)) budget, so it must
	// converge even on wholly dissimilar input.
	a := [][]byte{[]byte("a")}
	b := [][]byte{[]byte("b")}
	if _, ok := myersDiff(a, b, len(a)+len(b)); !ok {
		t.Fatal("myersDiff() did not converge within the len(a)+len(b) budget")
	}
}

func TestDiffLines(t *testing.T) {
	for _, tt := range diffLineCases {
		t.Run(tt.name, func(t *testing.T) {
			ops, err := DiffLines(toByteLines(tt.a), toByteLines(tt.b))
			if err != nil {
				t.Fatalf("DiffLines() error = %v", err)
			}
			if got := FormatDiff(ops); got != tt.want {
				t.Fatalf("DiffLines() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDiffLines_FallsBackWhenTooExpensive(t *testing.T) {
	// Two files with no lines in common: the edit distance is len(a)+len(b),
	// far past tooExpensiveLimit, so DiffLines must abandon Myers and match
	// DiffLinesLCS's guaranteed-correct output.
	a := diffBenchLines(2000)
	b := make([][]byte, 2000)
	for i := range b {
		b[i] = []byte(fmt.Sprintf("right-%d", i))
	}

	if _, ok := myersDiff(a, b, tooExpensiveLimit(len(a), len(b))); ok {
		t.Fatal("myersDiff() converged within budget; test input no longer exercises the bailout")
	}

	gotOps, err := DiffLines(a, b)
	if err != nil {
		t.Fatalf("DiffLines() error = %v", err)
	}
	wantOps, err := DiffLinesLCS(a, b)
	if err != nil {
		t.Fatalf("DiffLinesLCS() error = %v", err)
	}
	if got, want := FormatDiff(gotOps), FormatDiff(wantOps); got != want {
		t.Fatalf("DiffLines() fallback output did not match DiffLinesLCS()")
	}
}

func TestDiffLines_ErrorsWhenTableTooLarge(t *testing.T) {
	// Sized so Myers' bounded search bails out (no lines in common) and the
	// LCS fallback it hands off to would exceed maxLCSTableBytes.
	n := maxSafeLCSLines() + 1000
	a := make([][]byte, n)
	b := make([][]byte, n)
	for i := range b {
		b[i] = []byte(fmt.Sprintf("right-%d", i))
	}

	_, err := DiffLines(a, b)
	if !errors.Is(err, spifferrs.ErrDiffTableTooLarge) {
		t.Fatalf("DiffLines() error = %v, want ErrDiffTableTooLarge", err)
	}
}

// diffBenchLines builds n distinct lines, used to size diff benchmarks independent of any fixed body.
func diffBenchLines(n int) [][]byte {
	lines := make([][]byte, n)
	for i := range lines {
		lines[i] = []byte(fmt.Sprintf("line-%d", i))
	}
	return lines
}

func BenchmarkDiffLinesLCS(b *testing.B) {
	a := diffBenchLines(500)
	right := diffBenchLines(500)
	right[250] = []byte("line-250-changed") // one substitution to exercise the LCS backtrack

	b.ReportAllocs()
	for b.Loop() {
		if _, err := DiffLinesLCS(a, right); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiffLines(b *testing.B) {
	a := diffBenchLines(500)
	right := diffBenchLines(500)
	right[250] = []byte("line-250-changed") // one substitution to exercise the backtrack

	b.ReportAllocs()
	for b.Loop() {
		if _, err := DiffLines(a, right); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDiffLinesMyers(b *testing.B) {
	a := diffBenchLines(500)
	right := diffBenchLines(500)
	right[250] = []byte("line-250-changed") // one substitution to exercise the backtrack

	b.ReportAllocs()
	for b.Loop() {
		DiffLinesMyers(a, right)
	}
}

// BenchmarkDiffLinesMyers_LargeSimilar exercises Myers' key advantage over
// the LCS table: cost tracks the edit distance D, not input size N*M. A
// large file with a single changed line keeps D tiny.
func BenchmarkDiffLinesMyers_LargeSimilar(b *testing.B) {
	a := diffBenchLines(5000)
	right := diffBenchLines(5000)
	right[2500] = []byte("line-2500-changed")

	b.ReportAllocs()
	for b.Loop() {
		DiffLinesMyers(a, right)
	}
}

func BenchmarkDiffLinesLCS_LargeSimilar(b *testing.B) {
	a := diffBenchLines(5000)
	right := diffBenchLines(5000)
	right[2500] = []byte("line-2500-changed")

	b.ReportAllocs()
	for b.Loop() {
		if _, err := DiffLinesLCS(a, right); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDiffLines_LargeSimilar exercises the dispatcher's fast path: on
// similar files Myers converges well within budget, so this should track
// BenchmarkDiffLinesMyers_LargeSimilar, not the much slower LCS table.
func BenchmarkDiffLines_LargeSimilar(b *testing.B) {
	a := diffBenchLines(5000)
	right := diffBenchLines(5000)
	right[2500] = []byte("line-2500-changed")

	b.ReportAllocs()
	for b.Loop() {
		if _, err := DiffLines(a, right); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDiffLines_LargeDissimilar exercises the bailout path: no lines
// in common, so Myers exhausts its budget and DiffLines falls back to
// DiffLinesLCS. This should track BenchmarkDiffLinesLCS_LargeSimilar plus a
// small, bounded amount of wasted Myers search — not the unbounded
// DiffLinesMyers cost an equivalent BenchmarkDiffLinesMyers_LargeDissimilar
// would show.
func BenchmarkDiffLines_LargeDissimilar(b *testing.B) {
	a := diffBenchLines(5000)
	right := make([][]byte, 5000)
	for i := range right {
		right[i] = []byte(fmt.Sprintf("right-%d", i))
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := DiffLines(a, right); err != nil {
			b.Fatal(err)
		}
	}
}

// maxSafeLCSLines returns the largest per-file line count whose
// (n+1)x(n+1) LCS table stays within maxLCSTableBytes — the largest
// dissimilar pair DiffLinesLCS (and therefore DiffLines' fallback) can
// compare without returning ErrDiffTableTooLarge.
func maxSafeLCSLines() int {
	return int(math.Sqrt(float64(maxLCSTableBytes)/4)) - 1
}

// ExtraLarge benchmarks simulate comparing genuinely large (~28MB) saved
// files.
//
// The similar-files case is exactly what DiffLines is meant for and is
// benchmarked at the full 28MB target below.
//
// A true 28MB *dissimilar* pair (no lines in common) is not something
// either algorithm can safely finish: DiffLinesMyers unbounded would need
// hours, and the DiffLinesLCS fallback would need roughly a 17TB table
// (see ErrDiffTableTooLarge / maxLCSTableBytes). So dissimilar is split
// into two benchmarks: bailout-only detection at the full 28MB size (cheap
// — it never reaches the LCS fallback), and a full end-to-end comparison
// sized down to the largest input DiffLinesLCS can safely handle.
const extraLargeFileBytes = 28 * 1024 * 1024 // 28MB per file

// extraLargeLineCount approximates how many diffBenchLines-style lines
// ("line-N") make up extraLargeFileBytes, using each line's average width
// on disk (5-byte prefix + up to 7 digits + newline).
var extraLargeLineCount = extraLargeFileBytes / 13

func BenchmarkDiffLinesMyers_ExtraLargeSimilar(b *testing.B) {
	a := diffBenchLines(extraLargeLineCount)
	right := diffBenchLines(extraLargeLineCount)
	right[extraLargeLineCount/2] = []byte("line-changed")

	b.ReportAllocs()
	for b.Loop() {
		DiffLinesMyers(a, right)
	}
}

func BenchmarkDiffLines_ExtraLargeSimilar(b *testing.B) {
	a := diffBenchLines(extraLargeLineCount)
	right := diffBenchLines(extraLargeLineCount)
	right[extraLargeLineCount/2] = []byte("line-changed")

	b.ReportAllocs()
	for b.Loop() {
		if _, err := DiffLines(a, right); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDiffLines_ExtraLargeDissimilar_BailoutOnly measures only the
// cost of detecting that Myers should give up, at the true 28MB/~2.2M-line
// target size with zero lines in common. It deliberately stops at the
// bailout decision (myersDiff, not DiffLines) rather than continuing into
// the DiffLinesLCS fallback, which is not viable at this size.
func BenchmarkDiffLines_ExtraLargeDissimilar_BailoutOnly(b *testing.B) {
	a := diffBenchLines(extraLargeLineCount)
	right := make([][]byte, extraLargeLineCount)
	for i := range right {
		right[i] = []byte(fmt.Sprintf("right-%d", i))
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, ok := myersDiff(a, right, tooExpensiveLimit(len(a), len(right))); ok {
			b.Fatal("myersDiff() converged; benchmark input no longer exercises the bailout")
		}
	}
}

// BenchmarkDiffLines_ExtraLargeDissimilar runs the full comparison
// (bailout + DiffLinesLCS fallback) end to end, sized to the largest input
// that stays within maxLCSTableBytes rather than the full 28MB target,
// since a truly 28MB dissimilar pair would exceed it.
func BenchmarkDiffLines_ExtraLargeDissimilar(b *testing.B) {
	n := maxSafeLCSLines()
	a := diffBenchLines(n)
	right := make([][]byte, n)
	for i := range right {
		right[i] = []byte(fmt.Sprintf("right-%d", i))
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := DiffLines(a, right); err != nil {
			b.Fatal(err)
		}
	}
}
