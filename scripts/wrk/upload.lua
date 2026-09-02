-- Used by scripts/loadtest-http.sh to load-test POST / (upload). Body size
-- matches the payload used in internal/httphandlers' and internal/grpcserver's
-- Upload/Download benchmarks, so the two load tests stay comparable.
wrk.method = "POST"
wrk.body = string.rep("x", 4096)
wrk.headers["Content-Type"] = "application/octet-stream"
