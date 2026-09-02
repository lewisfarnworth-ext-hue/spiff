
RFC Demo repo. This code is not in production and most of it is test code.

Some of the practices diverge from typical company standards, as I was testing whether certain libs were fit for use. 

**The scheduler is the only important piece of code in this repo, as it's the only one relevant for the RFC.**

Some of the peripherals (e.g. the TLS transport configuration) could probably be quite fruitful for the final service,
be that signal or new one(s).  

Claude is configured to be double strict with performance. I've benchmarked & load tested everything in this repo. This
was a side-quest that I ran parallel to the the RFC requirements, off the back of a conversation I had with Tomm during
our initial discussions- specifically around benchmarking practices and potential improvements to the current ways of 
working. The current `fetch` is likely frivolous in the grand scheme because I was trying to juice the performance to 
the gills as part of this side quest.

Both the servers are completely frivolous. That was me testing out kitex.

The FS module was created initially to test out the storing & diffing of artifacts, though later discussions in the RFC
showed that artifact storage is probably best left to the consumer, as the artifect pipeline for KR is tightly integrated
with the service. 


# Servers

Two binaries expose the same underlying `spiff_fs.FS` functionality (upload, download, compare, fetch):

- `cmd/http` — HTTP, listens on `:8080`. Run with `go run ./cmd/http`.
- `cmd/grpc` — gRPC, listens on `:9090`. Run with `go run ./cmd/grpc`.

The gRPC server (`internal/grpcserver`) is built on [Kitex](https://github.com/cloudwego/kitex), CloudWeGo's RPC
framework, whose transport is [netpoll](https://github.com/cloudwego/netpoll) (`pkg/remote/trans/nphttp2`). It
speaks genuine gRPC-over-HTTP/2 (not Kitex's proprietary "Kitex-protobuf"), so it's interoperable with any standard
gRPC client — verified against both a Kitex client and `grpcurl`.

## Fetch

Both servers expose a Fetch endpoint: `POST /fetch` (HTTP, raw URL as the body) and `Spiff.Fetch` (gRPC), which
retrieve a URL's body, persist it via `spiff_fs.FS` like Upload does, and return the full body. Both sides share the
same outbound transport behind the `ports.Fetcher` seam (`internal/ports`), which takes a `context.Context` so a
caller can cancel/deadline an individual fetch:

- `internal/netfetch` — backed by `net/http.Client`, with a Transport tuned for sustained, high-concurrency,
  many-host outbound traffic: a larger per-host idle-connection pool, a per-host connection cap, a TLS
  session-ticket cache for handshake resumption, and OS-level TCP keepalive on pooled connections.

The service is defined in `idl/spiff.proto`. To regenerate the Kitex code after changing it:

```sh
cd internal/grpcserver
kitex -module spiff -service spiff -I ../../idl ../../idl/spiff.proto
```

This requires `protoc` (`brew install protobuf`) and the `kitex` tool (`go install github.com/cloudwego/kitex/tool/cmd/kitex@latest`).

## Load testing

`scripts/loadtest-http.sh` and `scripts/loadtest-grpc.sh` run concurrent-load tests against Upload/Download/Compare/Fetch
on each server, using matched payloads (4KB for Upload/Download/Fetch, a 500-line/one-line-diff pair for Compare) so
the two are comparable. Each builds and starts its server if one isn't already running on the expected port, seeds it
with files via `curl`/`grpcurl`, starts a local `python3 -m http.server` as the Fetch load test's origin, then drives
load with:

- HTTP: [`wrk`](https://github.com/wg/wrk) (`brew install wrk`) — wrk only speaks HTTP/1.1.
- gRPC: [`ghz`](https://github.com/bojand/ghz) (`brew install ghz`) — wrk's equivalent for gRPC, since gRPC requires
  HTTP/2, which wrk can't drive. Also requires `grpcurl` and `jq` for seeding (`brew install grpcurl jq`).

```sh
scripts/loadtest-http.sh
scripts/loadtest-grpc.sh
```

Override `DURATION`, `THREADS`/`CONCURRENCY`, `HOST`, `PORT`, or `FETCH_ORIGIN_PORT` via environment variables.

In addition to these external-tool load tests, each Fetch implementation and handler also has a `Test*_Stress`
Go test (`go test -race ./...`) that fires many concurrent requests in-process to catch races/deadlocks — see
`internal/netfetch`, `internal/httphandlers`, and `internal/grpcserver`.

# Useful commands 

## Run all tests & benchmarks
```go
go test -bench . -benchmem -run '^$' ./...
```

## Run all tests & benchmarks for a module

```go
go test -bench . -benchmem ./internal/utils
```

## Benchmark regression tracking

`scripts/bench.sh` wraps `go test -bench` with [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)
to catch performance regressions. It runs each benchmark `-count=6` times, compares the result against a stored
baseline for the target package, and fails if any statistically significant change exceeds a threshold (10% by
default). Results are also appended to `bench/history.csv` as a flat trend log for tracing regressions over time.

Baselines live in `bench/baseline/<pkg>.txt` and are meant to be committed, so a regression shows up as a diff in
code review. Run `scripts/bench.sh ./internal/utils` to check a package against its baseline, or add `--update` to
promote the current run to the new baseline after an intentional performance change. `BENCH_THRESHOLD` and
`BENCH_COUNT` env vars override the defaults.

