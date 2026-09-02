package utils

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"spiff/internal/spifferrs"
	"strings"
)

// diffOp marks a diffLine's disposition relative to the "left" file.
type diffOp byte

const (
	diffEqual diffOp = ' '
	diffAdd   diffOp = '+'
	diffDel   diffOp = '-'
)

// diffLine is one line of diff output. line aliases a slice of the
// original file content rather than copying it.
type diffLine struct {
	op   diffOp
	line []byte
}

func ReadLines(rc io.ReadCloser) ([][]byte, error) {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return bytes.Split(data, []byte{'\n'}), nil
}

// formatDiff renders ops as "<op> <line>\n" rows, exactly sized up front so
// strings.Builder never has to regrow its backing buffer.
func FormatDiff(ops []diffLine) string {
	size := 0
	for _, op := range ops {
		size += len(op.line) + 2 // op byte + space + line + newlines (counted below)
	}
	size += len(ops) // trailing '\n' per line

	var sb strings.Builder
	sb.Grow(size)
	for _, op := range ops {
		sb.WriteByte(byte(op.op))
		sb.WriteByte(' ')
		sb.Write(op.line)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// maxLCSTableBytes bounds the flat int32 table DiffLinesLCS allocates.
// Two large, dissimilar-enough files can demand a table in the terabytes
// (e.g. two 28MB files with no lines in common need roughly 17TB); rather
// than let that allocation thrash swap or trigger a diagnostic-free OS OOM
// kill, DiffLinesLCS returns ErrDiffTableTooLarge once the table would
// exceed this bound. 2 GiB is a generous but bounded single allocation for
// this codebase's target (barebones) hardware.
const maxLCSTableBytes = 2 << 30 // 2 GiB

// DiffLinesLCS computes a line-level diff between a and b via the textbook LCS
// dynamic-programming table, then backtracks through it into an edit
// script. O(len(a)*len(b)) time and space: simple, standard algorithm over a
// faster (e.g. Myers) implementation. Returns ErrDiffTableTooLarge instead
// of attempting the table allocation if it would exceed maxLCSTableBytes.
func DiffLinesLCS(a, b [][]byte) ([]diffLine, error) {
	n, m := len(a), len(b)

	if tableBytes := int64(n+1) * int64(m+1) * 4; tableBytes > maxLCSTableBytes {
		return nil, fmt.Errorf("%w: %d x %d lines would need a %d MB table (limit %d MB)",
			spifferrs.ErrDiffTableTooLarge, n, m, tableBytes/(1<<20), int64(maxLCSTableBytes)/(1<<20))
	}

	// One flat (n+1)x(m+1) table instead of [][]int32.
	// This is a single alloc instead of n+1, and has a better cache locality per row scan.
	stride := m + 1
	lcs := make([]int32, (n+1)*stride)
	at := func(i, j int) int32 { return lcs[i*stride+j] }

	for I := n - 1; I >= 0; I-- {
		for J := m - 1; J >= 0; J-- {
			switch {
			case bytes.Equal(a[I], b[J]):
				lcs[I*stride+J] = at(I+1, J+1) + 1
			case at(I+1, J) >= at(I, J+1):
				lcs[I*stride+J] = at(I+1, J)
			default:
				lcs[I*stride+J] = at(I, J+1)
			}
		}
	}

	ops := make([]diffLine, 0, n+m)
	I, J := 0, 0
	for I < n && J < m {
		switch {
		case bytes.Equal(a[I], b[J]):
			ops = append(ops, diffLine{diffEqual, a[I]})
			I++
			J++
		case at(I+1, J) >= at(I, J+1):
			ops = append(ops, diffLine{diffDel, a[I]})
			I++
		default:
			ops = append(ops, diffLine{diffAdd, b[J]})
			J++
		}
	}
	for ; I < n; I++ {
		ops = append(ops, diffLine{diffDel, a[I]})
	}
	for ; J < m; J++ {
		ops = append(ops, diffLine{diffAdd, b[J]})
	}
	return ops, nil
}

// DiffLinesMyers computes a line-level diff between a and b using Myers'
// greedy shortest-edit-script algorithm: O((N+M)*D) time and space, where D
// is the number of differing lines. Unlike DiffLinesLCS' O(N*M) table, cost
// scales with how different the inputs are rather than their raw size, so
// it stays fast on large, mostly-identical files where D is small. It never
// bails out, so on wholly dissimilar inputs (D close to N+M) it degrades
// toward O((N+M)^2) with worse constants than DiffLinesLCS — prefer
// DiffLines for untrusted input pairs.
func DiffLinesMyers(a, b [][]byte) []diffLine {
	ops, _ := myersDiff(a, b, len(a)+len(b)) // unbounded budget always converges
	return ops
}

// tooExpensiveBaseline and the sqrt(n+m) term mirror the "too expensive"
// bailout GNU diffutils/libxdiff use to bound the Myers search: past this
// many edits, its O((n+m)*d) time and O(d*(n+m)) trace memory stop paying
// for themselves against DiffLinesLCS' flat, single-allocation O(n*m)
// worst case.
const tooExpensiveBaseline = 256

func tooExpensiveLimit(n, m int) int {
	return tooExpensiveBaseline + int(math.Sqrt(float64(n+m)))
}

// DiffLines is the recommended general-purpose entry point for comparing
// two arbitrary, possibly-unrelated files: it runs DiffLinesMyers bounded
// by a "too expensive" edit budget, which is fast for the common case of
// comparing similar files, and falls back to DiffLinesLCS's guaranteed
// O(n*m) bound when the inputs turn out too dissimilar for that budget to
// pay off. The only error it can return is DiffLinesLCS's
// ErrDiffTableTooLarge, for inputs too large and too dissimilar for either
// algorithm to compare safely.
func DiffLines(a, b [][]byte) ([]diffLine, error) {
	if ops, ok := myersDiff(a, b, tooExpensiveLimit(len(a), len(b))); ok {
		return ops, nil
	}
	return DiffLinesLCS(a, b)
}

// myersDiff runs Myers' greedy shortest-edit-script search, abandoning it
// once the edit distance would exceed maxD; ok reports whether it converged
// within that budget. DiffLinesMyers passes maxD = len(a)+len(b), the
// worst-case edit distance, so it always converges; DiffLines passes a
// bounded budget so it can cut losses and hand off to DiffLinesLCS instead.
func myersDiff(a, b [][]byte, maxD int) (ops []diffLine, ok bool) {
	n, m := len(a), len(b)
	max := n + m
	if max == 0 {
		return nil, true
	}
	if maxD > max {
		maxD = max
	}

	// v[offset+k] is the furthest-reaching x on diagonal k for the current
	// D; offset lets k range over [-maxD, maxD] with a non-negative index.
	// Sized off maxD, not max = n+m: k never exceeds maxD in absolute value
	// (the round loop below never runs past d = maxD), so a bounded budget
	// keeps this — and every trace snapshot copied from it — O(maxD)
	// instead of O(n+m), which matters once n+m is itself huge (e.g. large
	// files with nothing in common: DiffLinesMyers passes maxD = n+m here,
	// so it's unaffected, but DiffLines' bounded budget now stays cheap).
	offset := maxD
	v := make([]int, 2*maxD+1)

	// trace snapshots v at the start of each round so the backtrack below
	// can replay, in reverse, which diagonal was taken at every step. Most
	// real comparisons converge in a handful of rounds, so start small
	// rather than eagerly paying for the full maxD+1 worst case.
	trace := make([][]int, 0, min(maxD+1, 64))

	d := 0
found:
	for d = 0; d <= maxD; d++ {
		snapshot := make([]int, len(v))
		copy(snapshot, v)
		trace = append(trace, snapshot)

		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && v[offset+k-1] < v[offset+k+1]) {
				x = v[offset+k+1] // came from an insertion (down)
			} else {
				x = v[offset+k-1] + 1 // came from a deletion (right)
			}
			y := x - k

			for x < n && y < m && bytes.Equal(a[x], b[y]) {
				x++
				y++
			}
			v[offset+k] = x

			if x >= n && y >= m {
				break found
			}
		}
	}
	if d > maxD {
		return nil, false // exceeded the budget without converging
	}

	// Backtrack from (n, m) to (0, 0) through the snapshotted diagonals,
	// building the edit script in reverse.
	ops = make([]diffLine, 0, n+m)
	x, y := n, m
	for D := d; D >= 0; D-- {
		v := trace[D]
		k := x - y

		var prevK int
		if k == -D || (k != D && v[offset+k-1] < v[offset+k+1]) {
			prevK = k + 1
		} else {
			prevK = k - 1
		}
		prevX := v[offset+prevK]
		prevY := prevX - prevK

		for x > prevX && y > prevY {
			ops = append(ops, diffLine{diffEqual, a[x-1]})
			x--
			y--
		}

		if D > 0 {
			if x == prevX {
				ops = append(ops, diffLine{diffAdd, b[y-1]})
			} else {
				ops = append(ops, diffLine{diffDel, a[x-1]})
			}
		}

		x, y = prevX, prevY
	}

	for i, j := 0, len(ops)-1; i < j; i, j = i+1, j-1 {
		ops[i], ops[j] = ops[j], ops[i]
	}
	return ops, true
}
