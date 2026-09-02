#!/usr/bin/env bash
set -euo pipefail

# Load-tests cmd/http (Upload, Download, Compare, Fetch) with wrk. Starts the
# server itself if one isn't already listening on HOST:PORT.
#
# Usage: scripts/loadtest-http.sh
# Env:   HOST, PORT, DURATION, THREADS, CONNECTIONS, FETCH_ORIGIN_PORT

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8080}"
DURATION="${DURATION:-10s}"
THREADS="${THREADS:-4}"
CONNECTIONS="${CONNECTIONS:-50}"
FETCH_ORIGIN_PORT="${FETCH_ORIGIN_PORT:-8090}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="http://$HOST:$PORT"

command -v wrk >/dev/null || { echo "install: brew install wrk" >&2; exit 1; }
command -v python3 >/dev/null || { echo "install: python3 (used to serve the Fetch load test's origin page)" >&2; exit 1; }

SERVER_PID=""
ORIGIN_PID=""
cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  [[ -n "$ORIGIN_PID" ]] && kill "$ORIGIN_PID" 2>/dev/null || true
}
trap cleanup EXIT

if ! curl -s -o /dev/null -X POST --data-binary "warmup" "$BASE/"; then
  echo "Building & starting HTTP server on :$PORT ..."
  BIN="$(mktemp -d)/spiff-http"
  (cd "$REPO_ROOT" && go build -o "$BIN" ./cmd/http)
  "$BIN" >/tmp/spiff-http-loadtest.log 2>&1 &
  SERVER_PID=$!
  ready=0
  for _ in $(seq 1 100); do
    if curl -s -o /dev/null -X POST --data-binary "warmup" "$BASE/"; then
      ready=1
      break
    fi
    sleep 0.1
  done
  [[ $ready -eq 1 ]] || { echo "server did not become ready in time" >&2; cat /tmp/spiff-http-loadtest.log >&2; exit 1; }
fi

echo "Seeding compare inputs..."
LEFT_BODY=$(seq 1 500 | sed 's/^/line-/')
RIGHT_BODY=$(printf '%s\n' "$LEFT_BODY" | sed '250s/.*/line-250-changed/')
LEFT_ID=$(curl -s -X POST --data-binary "$LEFT_BODY" "$BASE/")
RIGHT_ID=$(curl -s -X POST --data-binary "$RIGHT_BODY" "$BASE/")

echo "Starting Fetch origin server on :$FETCH_ORIGIN_PORT ..."
ORIGIN_DIR="$(mktemp -d)"
head -c 4096 /dev/zero | tr '\0' 'x' > "$ORIGIN_DIR/index.html"
(cd "$ORIGIN_DIR" && python3 -m http.server "$FETCH_ORIGIN_PORT" --bind 127.0.0.1) >/tmp/spiff-fetch-origin.log 2>&1 &
ORIGIN_PID=$!
ORIGIN_URL="http://127.0.0.1:$FETCH_ORIGIN_PORT/index.html"
ready=0
for _ in $(seq 1 100); do
  if curl -s -o /dev/null "$ORIGIN_URL"; then
    ready=1
    break
  fi
  sleep 0.1
done
[[ $ready -eq 1 ]] || { echo "fetch origin server did not become ready in time" >&2; cat /tmp/spiff-fetch-origin.log >&2; exit 1; }

echo
echo "=== Upload (POST /) ==="
wrk -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" -s "$REPO_ROOT/scripts/wrk/upload.lua" "$BASE/"

echo
echo "=== Download (GET /files/{id}) ==="
wrk -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" "$BASE/files/$LEFT_ID"

echo
echo "=== Compare (GET /compare) ==="
wrk -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" "$BASE/compare?left=$LEFT_ID&right=$RIGHT_ID"

echo
echo "=== Fetch (POST /fetch) ==="
ORIGIN_URL="$ORIGIN_URL" wrk -t"$THREADS" -c"$CONNECTIONS" -d"$DURATION" -s "$REPO_ROOT/scripts/wrk/fetch.lua" "$BASE/fetch"
