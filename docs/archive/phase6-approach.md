# Phase 6: Columnar Storage Research Approach

**Issue**: #2224
**Goal**: Research and design columnar storage to achieve DuckDB-level performance (180µs on TPC-H Q6)

## Current Status

- **Current performance**: 2.5ms on Q6 (after Phase 5 optimizations)
- **Target performance**: 180µs (DuckDB baseline)
- **Required speedup**: 14x

## Research-First Approach

Rather than immediately implementing Arrow IPC (as suggested in the issue), we're taking a data-driven approach:

### 1. Profile Current Bottlenecks ✅ (In Progress)

**Why**: Need to understand where 2.5ms is actually spent before choosing storage format

**Method**:
- Added `profile-q6` feature flag for instrumentation
- Measure time split:
  - Row loading (B-tree scan + deserialization)
  - Processing (predicates + aggregation)
- Identify which optimization will have most impact

**Files**:
- `crates/vibesql-executor/src/select/executor/execute.rs` - Load timing
- `crates/vibesql-executor/src/select/monomorphic/tpch.rs` - Processing timing
- `crates/vibesql-executor/benches/q6_profiling.rs` - Profiler

### 2. Evaluate Storage Formats (Next)

Based on profiling results, prototype and compare:

#### Option A: Custom Rust-Native Format (Preferred if deserialization dominates)

**Design**:
```rust
#[repr(C, align(64))]  // SIMD-friendly
struct LineitemColumnar {
    // Each column as contiguous array
    quantities: [f32; BATCH_SIZE],
    prices: [f32; BATCH_SIZE],
    discounts: [f32; BATCH_SIZE],
    shipdates: [i32; BATCH_SIZE],
}

// Direct mmap - zero deserialization!
let data: &[LineitemColumnar] = mmap_file("lineitem.dat");
```

**Advantages**:
- Zero overhead - native Rust types
- Perfect control over SIMD alignment
- No cross-language compatibility tax
- Compile-time guarantees

**Disadvantages**:
- Custom tooling required
- No ecosystem support
- Maintenance burden

#### Option B: Apache Arrow IPC (If need compatibility)

**Advantages**:
- Standard format with tooling
- Ecosystem (DataFusion integration possible)
- Zero-copy mmap support

**Disadvantages**:
- Designed for cross-language (may have overhead)
- Less control over memory layout
- More complex than needed?

#### Option C: Hybrid Row/Column

**Design**:
- Write-Ahead Log (row-based) for ACID
- Background compaction to columnar
- Best of both worlds

**Evaluation Criteria**:
1. Q6 query time (target: < 200µs)
2. Write performance impact
3. Implementation complexity
4. Storage overhead

### 3. Prototype Winner (After Evaluation)

Implement POC of best approach:
- Export TPC-H lineitem to chosen format
- Implement mmap-based scan
- Measure actual performance
- Verify zero-copy claims

**Success Criteria**:
- [ ] Q6 scan < 200µs (match DuckDB)
- [ ] Zero allocations during scan (verify with profiler)
- [ ] Correctness matches current results
- [ ] Clear path to production implementation

### 4. Document Architecture

Create detailed design for production:
- Storage format specification
- Write path (WAL/compaction if hybrid)
- Read path (mmap/caching strategy)
- Migration strategy
- ACID guarantees

## Why Not Just Use Arrow?

Three reasons to validate first:

1. **May be overkill**: Arrow designed for distributed systems and cross-language compatibility. We need fast single-process Rust. Custom format might be 2-3x faster.

2. **Rust advantages**: Rust's zero-cost abstractions and compile-time layout control might enable optimizations Arrow can't provide.

3. **14x is ambitious**: Even with perfect columnar storage, may need SIMD+JIT. Need to understand ALL bottlenecks, not assume storage is the issue.

## Decision Framework

After profiling, choose path:

**If deserialization > 60% of time**:
→ Columnar storage will have major impact
→ Prototype custom format + Arrow IPC
→ Compare performance

**If memory access > 40% of time**:
→ Cache-friendly layout critical
→ SIMD alignment critical
→ Custom format likely better than Arrow

**If I/O > 40% of time**:
→ Compression trade-offs important
→ Page cache optimization needed
→ Consider Parquet with prefetching

**If processing > 50% of time**:
→ Storage format won't help much
→ Need SIMD vectorization first
→ Defer columnar storage

## Timeline

- **Week 1**: Profiling + evaluation (current)
- **Week 2**: Prototype best format
- **Week 3**: Benchmark + document
- **Week 4**: PR with findings + recommendation

No implementation commitment until we have data supporting the approach.

## Success Metrics

This research is successful if we can answer:

1. ✅ Where is the 2.5ms currently spent?
2. ⏳ Which storage format gets closest to 180µs?
3. ⏳ What are the trade-offs (complexity, maintainability)?
4. ⏳ Clear path to production with < 3 months work?

If answer to #2 is "none of them", we've learned columnar storage isn't the bottleneck and saved months of work going down the wrong path.

## References

- Issue #2224: Original problem statement
- `docs/profiling/q6-analysis.md`: Current performance breakdown
- `docs/performance/OPTIMIZATION_ROADMAP.md`: Overall strategy
- DuckDB Storage: https://duckdb.org/internals/storage
- Arrow IPC Spec: https://arrow.apache.org/docs/format/Columnar.html
