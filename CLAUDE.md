# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Collaboration

You are a **colleague**, not an autonomous agent. Ask before acting on functional changes. Trivial non-behavioral edits 
(comments, formatting, typos in a single file) may proceed without prior approval.

Never commit or push without explicit approval.

### Database

PostgreSQL with pgvector. Use the query builder over raw strings. Understand `$1::text IS NULL OR …` null-handling
patterns before touching queries. Migrations are forward-only — never alter or delete existing migration files.

### Design Patterns

- **Ports & Adapters**: All external I/O behind interfaces in `internal/ports/`
- **Repository Pattern**: Database access through typed repository structs

## Testing

Tests are the behavioral contract. If tests pass but contradict apparent intent, flag the mismatch before changing anything.

### Benchmarking 

When a feature is complete, ensure to write good benchmarks as part of the test suite. Store the files alongside tests,
at the bottom of the `_test.go` files.

## Go Conventions

Follow existing patterns before introducing new ones. When adding a feature that overlaps with existing behavior, check
whether the logic already exists elsewhere and extract common functionality rather than duplicating.


This repository is performance sensitive. Prefer fast code over "clean" code and check any list that's appropriate (shown below).

### When using Go's standard lib check this list 

- Use `strings.Builder` over `fmt.Sprintf(...)` unless Sprintf is **absolutely required**
- Use `sync.Pool` for objects **that make sense to pool**
- Use sentinel errors for known failure states 
- Use sensible memory pre-allocation techniques for slices and maps
- Using interfaces like io.Reader and io.Writer gives you fine-grained control over how data flows. Instead of spinning
  up new buffers every time, reuse existing ones (i.e. `io.CopyBuffer()`) and keep memory usage steady. 
- In extemely hot IO paths, use memory mapping `unix.Mmap()` and operate directly on mapped pages, as opposed to `os.Open()` 

### When writing general Go code, check every item in this list

- Slice for efficient data access instead of copying data into new slices
- Interface boxing is **only** acceptable if: 
   - abstraction is more important than performance (like ports/adapters)
   - values are small 
   - value is used briefly (i.e. logging or interface based sorting)
   - dynamic behaviour is **100% required** to support clarity, reusability or design goals
   - not in a hot path
- Prefer stack allocations over heap allocations. Avoid escapes when:
   - in performance critical paths: Reducing heap usage in tight loops/latency-sensitive code lowers GC pressure and 
     speeds up execution.
   - for short-lived, small objects (These can be efficiently stack allocated without invoking the GC)
   - When you control the full call chain
  Escapes are fine when: 
   - returning values from constructors or factories
   - when an object must outlive the function: if storing data in a global, sending to a goroutine or saving in a struct, escaping is necessary and correct
   - when allocation size is small and infrequent: if the heap allocation isn't in a hot path, the benefit is often negligible
   - when preventing escape hurts readability: writing awkward code to avoid escapes  

### When writing concurrent Go code, check every item in this list

- Use worker pools. If CPU bound, tie to `runtime.NumCPU()`. If IO bound, the pool size can be larger than the number of cores
- Leverage the `sync/atomic` package to:
   - create lock-free data structures
   - reduce lock contention 
   - avoid mutexes where possible
- Use lazy initialization `sync.Once()` for things like:
   - DB connectors
   - caches
   - large in-memory structures
- Prefer immutable data 

### When IO throughput matters, check every item in this list

- Lean on `bufio` to reduce sys calls  
- Use batching when:
  - individual operations are expensive
  - the system benefits from reducing the frequency of external interactions
  - there's tolerance for per-item latency in favour of higher throughput

### Mandatory verification, every run

Before calling any Go code change complete, walk every item in the four checklists above one by one against the
diff. For each item, either apply it or state explicitly why it doesn't apply (e.g. "no concurrency here" or "not a
hot path") — do not silently skip items, and do not defer this check because the change looks small. This applies
even to single-file, "small surface area" changes. If tests/benchmarks are also required by the Testing section,
write those in the same pass, not as a follow-up.
