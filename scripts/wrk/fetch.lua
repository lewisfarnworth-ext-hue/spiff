-- Used by scripts/loadtest-http.sh to load-test POST /fetch. The body is the
-- URL to fetch, read from ORIGIN_URL so loadtest-http.sh can point at
-- whatever port it started the local fetch origin server on.
wrk.method = "POST"
wrk.body = os.getenv("ORIGIN_URL")
