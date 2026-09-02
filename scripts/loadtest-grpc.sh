#!/usr/bin/env bash
set -euo pipefail

# Load-tests cmd/grpc (Upload, Download, Compare, Fetch) with ghz - wrk's
# equivalent for gRPC, since wrk only speaks HTTP/1.1 and can't drive gRPC's
# HTTP/2 wire protocol. Starts the server itself if one isn't already
# listening on HOST:PORT.
#
# Usage: scripts/loadtest-grpc.sh
# Env:   HOST, PORT, DURATION, CONCURRENCY, FETCH_ORIGIN_PORT

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-9090}"
DURATION="${DURATION:-10s}"
CONCURRENCY="${CONCURRENCY:-50}"
FETCH_ORIGIN_PORT="${FETCH_ORIGIN_PORT:-8090}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IDL_DIR="$REPO_ROOT/idl"
IDL="$IDL_DIR/spiff.proto"
ADDR="$HOST:$PORT"

command -v ghz >/dev/null || { echo "install: brew install ghz" >&2; exit 1; }
command -v grpcurl >/dev/null || { echo "install: brew install grpcurl" >&2; exit 1; }
command -v jq >/dev/null || { echo "install: brew install jq" >&2; exit 1; }
command -v python3 >/dev/null || { echo "install: python3 (used to serve the Fetch load test's origin page)" >&2; exit 1; }

SERVER_PID=""
ORIGIN_PID=""
cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  [[ -n "$ORIGIN_PID" ]] && kill "$ORIGIN_PID" 2>/dev/null || true
}
trap cleanup EXIT

if ! nc -z "$HOST" "$PORT" 2>/dev/null; then
  echo "Building & starting gRPC server on :$PORT ..."
  BIN="$(mktemp -d)/spiff-grpc"
  (cd "$REPO_ROOT" && go build -o "$BIN" ./cmd/grpc)
  "$BIN" >/tmp/spiff-grpc-loadtest.log 2>&1 &
  SERVER_PID=$!
  ready=0
  for _ in $(seq 1 100); do
    if nc -z "$HOST" "$PORT" 2>/dev/null; then
      ready=1
      break
    fi
    sleep 0.1
  done
  [[ $ready -eq 1 ]] || { echo "server did not become ready in time" >&2; cat /tmp/spiff-grpc-loadtest.log >&2; exit 1; }
fi

echo "Seeding compare inputs..."
BODY_B64=$(printf 'x%.0s' $(seq 1 4096) | base64)
LEFT_ID=$(grpcurl -plaintext -import-path "$IDL_DIR" -proto "$IDL" \
  -d "{\"data\":\"${BODY_B64}\"}" "$ADDR" spiff.Spiff/Upload | jq -r .id)

RIGHT_LINES=$(seq 1 500 | sed 's/^/line-/' | sed '250s/.*/line-250-changed/')
RIGHT_B64=$(printf '%s\n' "$RIGHT_LINES" | base64)
RIGHT_ID=$(grpcurl -plaintext -import-path "$IDL_DIR" -proto "$IDL" \
  -d "{\"data\":\"${RIGHT_B64}\"}" "$ADDR" spiff.Spiff/Upload | jq -r .id)

echo
echo "=== Upload ==="
ghz --insecure -c "$CONCURRENCY" -z "$DURATION" \
  --proto "$IDL" --call spiff.Spiff/Upload \
  -d "[{\"data\":\"${BODY_B64}\"}]" \
  "$ADDR"

echo
echo "=== Download ==="
ghz --insecure -c "$CONCURRENCY" -z "$DURATION" \
  --proto "$IDL" --call spiff.Spiff/Download \
  -d "{\"id\":\"${LEFT_ID}\"}" \
  "$ADDR"

echo
echo "=== Compare ==="
ghz --insecure -c "$CONCURRENCY" -z "$DURATION" \
  --proto "$IDL" --call spiff.Spiff/Compare \
  -d "{\"left_id\":\"${LEFT_ID}\",\"right_id\":\"${RIGHT_ID}\"}" \
  "$ADDR"

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
echo "=== Fetch ==="
ghz --insecure -c "$CONCURRENCY" -z "$DURATION" \
  --proto "$IDL" --call spiff.Spiff/Fetch \
  -d "{\"url\":\"${ORIGIN_URL}\"}" \
  "$ADDR"
