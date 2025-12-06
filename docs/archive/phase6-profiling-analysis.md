# Phase 6: Columnar Storage Research - Profiling Analysis

**Date**: 2025-11-20
**Issue**: #2224
**Goal**: Understand current bottlenecks to inform storage format decision

## Research Question

Before committing to a specific columnar storage format (Arrow IPC, Parquet, or custom), we need to understand:

**Where is the current 2.5ms (after Phase 5) actually spent?**

## Hypotheses

Based on the architecture, time could be spent on:

1. **Disk I/O** - Reading B-tree pages from storage
2. **Deserialization** - Converting bytes → `Row{Vec<SqlValue>}`
3. **Row iteration** - Iterating over Vec of rows
4. **Predicate evaluation** - Date/numeric comparisons (already optimized in monomorphic)
5. **Aggregation** - Summing results (minimal, just f64 addition)

## Profiling Approach

Added instrumentation with `profile-q6` feature to measure:

### 1. Row Loading Time
In `SelectExecutor::try_monomorphic_execution()`:
- Time spent in `execute_from()`
- This includes: B-tree scan + deserialization into `Row` structs

### 2. Processing Time
In `TpchQ6Plan::execute_unsafe()`:
- Filter evaluation time (predicates)
- Compute time (aggregation)

## Expected Results

If hypothesis is correct that deserialization is the bottleneck:
- **Row loading**: ~2ms (80% of time)
  - B-tree page reads: ~500µs
  - Deserialization: ~1.5ms
- **Processing**: ~500µs (20% of time)
  - Filter: ~400µs
  - Compute: ~100µs

This would validate that zero-copy mmap storage could eliminate the 1.5ms deserialization overhead.

## Alternative Outcomes

**If row loading is < 1ms:**
- Bottleneck is elsewhere (processing, memory access patterns)
- Columnar storage may not help as much as expected
- Consider SIMD/vectorization instead

**If row loading is > 2ms:**
- Even more benefit from zero-copy formats
- Strong case for mmap-based approaches

## Next Steps After Profiling

Based on results:

1. **If deserialization dominates (>60%)**:
   - Prototype custom Rust-native format with `#[repr(C)]` and mmap
   - Compare against Arrow IPC
   - Measure actual zero-copy performance

2. **If memory access dominates (>40%)**:
   - Focus on cache-friendly layouts
   - SIMD-aligned buffers critical
   - Custom format may be better than Arrow

3. **If I/O dominates (>40%)**:
   - Consider compression trade-offs
   - Page cache optimization
   - May need buffering strategies

## Instrumentation Details

### Code Changes

1. **Cargo.toml**: Added `profile-q6` feature flag
2. **execute.rs**: Added timing around `execute_from()` call
3. **tpch.rs**: Added timing in `execute_unsafe()` loop
4. **q6_profiling.rs**: New benchmark to run profiled queries

### Running Profiler

```bash
cd crates/vibesql-executor
cargo run --release --features "benchmark-comparison,profile-q6" --bench q6_profiling
```

Output format:
```
[Q6 PROFILE] Row loading: Xms (60175 rows, Yns/row)
[Q6 PROFILE] Processing 60175 rows:
  Total:   Zms (Wns/row)
  Filter:  Ams (Bns/row)
  Compute: Cms (Dns/row)
[Q6 PROFILE] Total query time: Tms
```

## Storage Format Decision Criteria

After profiling, evaluate options:

### Custom Rust Format (Recommended if deserialization > 60%)
**Pros:**
- Perfect control over memory layout
- `#[repr(C, align(64))]` for SIMD
- Direct mmap to typed arrays
- No cross-language overhead

**Cons:**
- Custom tooling needed
- No ecosystem support
- Maintenance burden

### Arrow IPC (Recommended if need compatibility)
**Pros:**
- Standard format with tooling
- Zero-copy mmap support
- Wide ecosystem (DataFusion, etc.)

**Cons:**
- Designed for cross-language (overhead?)
- May not be optimal for Rust patterns
- Less control over layout

### Hybrid Approach (Recommended if I/O > 40%)
**Pros:**
- Row WAL for writes (ACID)
- Columnar snapshots for reads
- Best of both worlds

**Cons:**
- Complexity
- Background compaction needed
- Storage overhead

## Success Metrics

Profile must answer:
- ✅ What % of time is deserialization?
- ✅ What % of time is memory access?
- ✅ What % of time is actual computation?
- ✅ Would zero-copy mmap help significantly?

Target: < 500µs with optimized storage (5x improvement from 2.5ms)
