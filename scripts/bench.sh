#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/bench.sh [pkg]            Run + compare against baseline, fail on regression.
#   scripts/bench.sh [pkg] --update   Run + promote result to new baseline.
# pkg defaults to ./...
#
# Env:
#   BENCH_THRESHOLD  Regression threshold in percent (default 10).
#   BENCH_COUNT      Samples per benchmark, passed to -count (default 6).

PKG="./..."
UPDATE=0
for a in "$@"; do
  [[ "$a" == "--update" ]] && UPDATE=1 || PKG="$a"
done

BASELINE_DIR="bench/baseline"
HISTORY_CSV="bench/history.csv"
THRESHOLD_PCT="${BENCH_THRESHOLD:-10}"
COUNT="${BENCH_COUNT:-6}"

mkdir -p "$BASELINE_DIR"
command -v benchstat >/dev/null || { echo "install: go install golang.org/x/perf/cmd/benchstat@latest" >&2; exit 1; }

REF=$(git rev-parse --short HEAD 2>/dev/null || echo "nogit")
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
SLUG=$(echo "$PKG" | tr -c 'a-zA-Z0-9' '_')
BASELINE_FILE="$BASELINE_DIR/${SLUG}.txt"
NEW_FILE=$(mktemp)

echo "Running benchmarks for $PKG (count=$COUNT) ..."
go test -bench=. -benchmem -count="$COUNT" -run=^$ "$PKG" | tee "$NEW_FILE"

[[ -f "$HISTORY_CSV" ]] || echo "timestamp,ref,pkg,name,ns_op,bytes_op,allocs_op" > "$HISTORY_CSV"
awk -v ref="$REF" -v ts="$STAMP" -v pkg="$PKG" \
  '/^Benchmark/ { print ts","ref","pkg","$1","$3","$5","$7 }' "$NEW_FILE" >> "$HISTORY_CSV"

if [[ $UPDATE -eq 1 || ! -s "$BASELINE_FILE" ]]; then
  cp "$NEW_FILE" "$BASELINE_FILE"
  echo "Baseline written to $BASELINE_FILE"
  exit 0
fi

echo
echo "Comparing against baseline ($BASELINE_FILE):"
COMPARISON=$(benchstat "$BASELINE_FILE" "$NEW_FILE")
echo "$COMPARISON"

REGRESSED=$(echo "$COMPARISON" | grep -E '\+[0-9]+\.[0-9]+%' | awk -v t="$THRESHOLD_PCT" -F'+' \
  '{ split($2,a,"%"); if (a[1]+0 > t) print }')

if [[ -n "$REGRESSED" ]]; then
  echo
  echo "Regression(s) exceeding ${THRESHOLD_PCT}%:"
  echo "$REGRESSED"
  exit 1
fi
echo "No regressions beyond ${THRESHOLD_PCT}%."
