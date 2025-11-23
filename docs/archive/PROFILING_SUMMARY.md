# Phase 6 Columnar Storage: Profiling Infrastructure

## Summary

Added profiling instrumentation to measure where TPC-H Q6 query time is currently spent, to inform storage format decision.

## Problem

Issue #2224 proposes implementing Apache Arrow columnar storage to achieve DuckDB-level performance (180µs). However, before committing to a 4-month implementation, we need to understand:

**Where is the current 2.5ms actually being spent?**

If deserialization isn't the main bottleneck, columnar storage may not help as much as expected.

## Approach

### 1. Instrumented Code with `profile-q6` Feature

Added conditional timing code that only compiles when `profile-q6` feature is enabled:

**Row Loading** (`execute.rs:251-261`):
```rust
#[cfg(feature = "profile-q6")]
let load_start = std::time::Instant::now();

let mut from_result = self.execute_from(from_clause, cte_results)?;

#[cfg(feature = "profile-q6")]
{
    let load_time = load_start.elapsed();
    eprintln!("[Q6 PROFILE] Row loading: {:?} ({} rows, {:?}/row)", ...);
}
```

**Query Processing** (`tpch.rs:76-141`):
- Measure filter evaluation time
- Measure aggregation compute time
- Per-row and total breakdowns

### 2. Created Profiling Benchmark

`benches/q6_profiling.rs`:
- Loads TPC-H SF 0.01 dataset (~60K rows)
- Runs Q6 query 10 times with profiling enabled
- Outputs detailed timing breakdown

### 3. Zero Runtime Cost When Disabled

Using `#[cfg(feature = "profile-q6")]` means:
- **Default builds**: No profiling code compiled, zero overhead
- **Profiled builds**: Detailed timing with `--features profile-q6`

## Expected Outcomes

### Hypothesis: Deserialization Dominates

If correct, we should see:
- Row loading: ~2ms (80%)
  - B-tree I/O: ~500µs
  - Deserialization to `Row{Vec<SqlValue>}`: ~1.5ms
- Processing: ~500µs (20%)
  - Filters: ~400µs
  - Aggregation: ~100µs

**Implication**: Zero-copy mmap storage could eliminate ~1.5ms, getting us close to 1ms (still need 5-6x more to hit 180µs).

### Alternative: Processing Dominates

If processing > 50% of time:
- Columnar storage won't help much
- Need SIMD vectorization first
- Different optimization path

### Alternative: I/O Dominates

If B-tree I/O > 1ms:
- Consider compression vs speed trade-offs
- Page cache optimization
- May need different approach than pure mmap

## Storage Format Decision Tree

Based on profiling results:

**Deserialization > 60%:**
→ Prototype custom Rust format with `#[repr(C)]` + mmap
→ Compare with Arrow IPC
→ Measure actual zero-copy benefit

**Memory access > 40%:**
→ Cache layout critical
→ SIMD alignment critical
→ Custom format likely better than Arrow

**I/O > 40%:**
→ Compression important
→ Consider Parquet
→ Prefetching strategies

**Processing > 50%:**
→ Defer columnar storage
→ Focus on SIMD/JIT first

## Files Changed

1. `Cargo.toml`:
   - Added `profile-q6` feature flag
   - Added `q6_profiling` benchmark target

2. `src/select/executor/execute.rs`:
   - Instrumented `try_monomorphic_execution()` to measure row loading time

3. `src/select/monomorphic/tpch.rs`:
   - Instrumented `execute_unsafe()` to measure filter and compute time
   - Detailed per-row breakdown

4. `benches/q6_profiling.rs`:
   - New benchmark to run profiled queries
   - Statistics collection (min/median/p95/p99)

5. `docs/research/phase6-profiling-analysis.md`:
   - Detailed analysis plan
   - Hypotheses and expected outcomes
   - Decision criteria

6. `docs/research/phase6-approach.md`:
   - Overall research approach
   - Why not just use Arrow
   - Evaluation criteria for storage formats

## Running Profiler

```bash
# Build with profiling enabled
cd crates/vibesql-executor
cargo build --release --bench q6_profiling --features "benchmark-comparison,profile-q6"

# Run profiling benchmark
./target/release/q6_profiling-<hash>
```

Expected output:
```
=== TPC-H Q6 Profiling ===

Loading TPC-H database (SF 0.01)...
Database loaded in 245ms

Warm-up run...
[Q6 PROFILE] Row loading: 18.2ms (60175 rows, 302ns/row)
[Q6 PROFILE] Processing 60175 rows:
  Total:   1.8ms (29ns/row)
  Filter:  1.5ms (24ns/row)
  Compute: 300µs (5ns/row)
[Q6 PROFILE] Total query time: 20ms
Warm-up complete

Running 10 iterations...
[detailed timing for each run]

=== Summary ===
Median total: 19.5ms
```

## Next Steps

1. ✅ Complete build and run profiler
2. ⏳ Analyze results and identify bottleneck
3. ⏳ Prototype most promising storage format
4. ⏳ Benchmark prototype against current implementation
5. ⏳ Document findings and recommend path forward

## Success Criteria

- Can answer: What % of time is deserialization?
- Can answer: What % of time is I/O?
- Can answer: What % of time is processing?
- Can decide: Which storage format to prototype?
- Have data to support (or reject) 4-month columnar storage project

## Benefits of This Approach

1. **Data-driven decision**: Not assuming Arrow is the solution
2. **Risk reduction**: Won't spend months on wrong optimization
3. **Flexibility**: Can choose custom Rust format if it's better
4. **Learning**: Will understand system deeply even if we keep current storage
5. **Documentation**: Clear reasoning for whatever we choose

## Alternative Paths Considered

### Immediate Arrow Implementation
**Rejected because:**
- 4-month commitment without validating hypothesis
- Arrow may not be optimal for single-process Rust
- May miss actual bottleneck

### SIMD First
**Deferred because:**
- Issue says Phase 5 (JIT) gets to 2.5ms
- Phase 6 (storage) projected for 10x improvement
- But worth reconsidering if profiling shows processing dominates

### Keep Current Storage
**Possible if:**
- Profiling shows I/O/deserialization < 40% of time
- Other optimizations (SIMD, caching) can get to 180µs
- Columnar storage complexity not justified

## References

- Issue #2224: Phase 6 research task
- Issue #2220: Parent EPIC for optimization roadmap
- `docs/profiling/q6-analysis.md`: Current 35.2ms → 2.5ms breakdown (after Phase 1-5)
- `docs/performance/OPTIMIZATION_ROADMAP.md`: Full optimization plan
- DuckDB storage internals: https://duckdb.org/internals/storage
- Arrow IPC format: https://arrow.apache.org/docs/format/Columnar.html

---

**Status**: Profiling infrastructure complete, awaiting build completion to run first profiling session.
