# kgo: eliminate per-call closure and heap allocation in Decompress

## Summary

Refactors `Decompress` to remove the `rfn` closure that was allocated on every call, and eliminates the `bytes.NewBuffer` heap escape on the user-pooled path for snappy and zstd — the two most common codecs in high-throughput Kafka workloads.

## Motivation

Every call to `Decompress` previously allocated a closure (`rfn`) to defer the clone-vs-return-direct decision, and when a user pool was in use, created a `bytes.NewBuffer(s[:0])` wrapping the pool's slice. Both escaped to the heap on every batch decompressed. For snappy and zstd — which decode directly into a `[]byte` and never need an `io.Writer` — the `bytes.Buffer` wrapper was pure overhead.

## What changed

**Structural split:** `Decompress` is split into an outer method that manages buffer lifecycle and an inner `decompress` dispatcher that delegates to per-codec methods.

**Outer `Decompress`:**
- Acquires a `dst []byte` (from the user pool or the internal `byteBuffers` pool).
- Passes `dst` and, when available, the pooled `*bytes.Buffer` (`out`) to the inner function.
- Handles the clone-vs-return-direct decision in one place — no closure needed.

**Inner per-codec methods:**
- `decompressSnappy(dst, src)` and `decompressZstd(dst, src)` take only a `[]byte` destination. No `bytes.Buffer` involved.
- `decompressGzip(dst, out, src)` and `decompressLz4(dst, out, src)` use the `*bytes.Buffer` for `io.Copy`. When `out` is nil (user-pooled path), they fall back to `bytes.NewBuffer(dst)` — this still allocates, but gzip/lz4 are rarely the hot codec.

## Benchmark results (300 KB decompressed payload, Apple M5 Max)

### Snappy — user pool path

| | master | this PR | delta |
|---|---|---|---|
| **allocs/op** | **2** | **0** | **-2** |
| **B/op** | 64 | 0 | -64 |
| ns/op | 26,400 | 27,000 | ~same |

### Zstd — user pool path

| | master | this PR | delta |
|---|---|---|---|
| **allocs/op** | **2** | **0** | **-2** |
| **B/op** | 70 | 6 | -64 |
| ns/op | 82,000 | 82,000 | ~same |

### Gzip — user pool path

| | master | this PR | delta |
|---|---|---|---|
| **allocs/op** | **4** | **3** | **-1** |
| **B/op** | 139 | 121 | -18 |
| ns/op | 50,500 | 50,000 | ~same |

### Lz4 — user pool path

| | master | this PR | delta |
|---|---|---|---|
| **allocs/op** | **5** | **4** | **-1** |
| **B/op** | 438 | 232 | -206 |
| ns/op | 54,300 | 55,000 | ~same |

The 2 allocs eliminated from snappy/zstd are the `rfn` closure and the `bytes.NewBuffer` wrapper. Gzip and lz4 each drop 1 (the closure) but retain the `bytes.NewBuffer` since they need an `io.Writer` for `io.Copy`. No-pool paths are unchanged.

## Behavioral note

The xerial-framed snappy path without a user pool now passes the pooled buffer's backing slice to `xerialDecode` (previously it passed `nil`, letting `append` allocate fresh and avoiding a clone). The result is now cloned like every other non-user-pooled path. This is one extra copy on a legacy format's cold path — functionally identical, not worth special-casing.
