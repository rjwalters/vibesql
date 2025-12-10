# Ultra-Optimization Roadmap: From 11x Slower to SQLite-Competitive

**Last Updated**: 2025-11-20
**Current Performance**: Q6 = 35.2ms (11x slower than SQLite, 195x slower than DuckDB)
**Target Performance**: Q6 = 180µs (match DuckDB)
**Total Improvement Needed**: 195x

## Executive Summary

After Phase 1 (row materialization) and Phase 2 (type-specialized predicates), we've achieved a **6.5x improvement** from 230ms to 35.2ms on TPC-H Q6. This document outlines the path to achieve an additional **195x improvement** to match DuckDB performance.

**Key Insight**: The remaining 586ns/row overhead breaks down into:
- SqlValue Boxing: 150ns (26%)
- Cache Misses: 176ns (30%)
- Expression Walking: 100ns (17%)
- Enum Matching: 80ns (14%)
- Allocations: 80ns (13%)

---

## Current State (35.2ms for 60K rows)

### Per-Row Cost Breakdown (586ns/row)

**What Phase 1 Fixed:**
- Eliminated 90% of unnecessary row clones (54K of 60K rows)
- Reduced Vec allocation overhead from 1.5µs/row to 0.15µs/row
- **Savings: ~1.35µs/row**

**What Phase 2 Fixed:**
- Optimized predicate evaluation with type-specialized paths
- Reduced enum matching from 0.5µs/row to 0.15µs/row
- **Savings: ~0.35µs/row**

**What's Still Costing 586ns/row:**

1. **SqlValue Boxing** (estimated ~150ns/row = 26%)
   - Even with lazy projection, we box ~16 columns × 6K passing rows
   - Each Double/Integer wrap: ~10-15ns
   - Aggregate expression evaluation requires materialized SqlValues

2. **Cache Misses** (estimated ~176ns/row = 30%)
   - Random memory access patterns
   - Heap-allocated SqlValues scattered across memory
   - No prefetching opportunities

3. **Expression Tree Walking** (estimated ~100ns/row = 17%)
   - SUM(l_extendedprice * l_discount) per row
   - Function call overhead
   - Dynamic dispatch through SqlValue

4. **Enum Pattern Matching** (estimated ~80ns/row = 14%)
   - Remaining match statements in aggregate path
   - Type checking for multiplication operator

5. **Memory Allocations** (estimated ~80ns/row = 13%)
   - Result Vec growth
   - Temporary allocations in expression evaluation
   - String interning (dates are still strings internally)

---

## Phase 3.5: Monomorphic Execution (Target: 15ms)

**Issue**: #2221
**Impact**: 2.4x speedup (saves 230ns/row)
**Implementation Time**: 2 weeks

### The Problem

Every column access requires enum discrimination, pattern matching, and unwrapping:

```rust
// Current overhead: ~15ns per access
match value {
    SqlValue::Double(d) => d,
    SqlValue::Integer(i) => i as f64,
    _ => panic!("type mismatch")
}
```

For Q6: 60K rows × 4 column accesses = 240K enum matches = **3.6ms overhead**

### The Solution

Generate monomorphic code paths based on schema at query planning time:

```rust
pub struct MonomorphicQ6Plan {
    // Pre-computed column indices
    l_shipdate_idx: usize,      // Column 10, type: DATE
    l_discount_idx: usize,      // Column 6, type: DOUBLE
    l_quantity_idx: usize,      // Column 4, type: DOUBLE
    l_extendedprice_idx: usize, // Column 5, type: DOUBLE
}

impl MonomorphicQ6Plan {
    fn execute(&self, rows: &[Row]) -> f64 {
        let mut sum = 0.0;
        for row in rows {
            unsafe {
                let shipdate = row.get_date_unchecked(self.l_shipdate_idx);
                let discount = row.get_f64_unchecked(self.l_discount_idx);
                // ... direct typed access, no enum matching!
                sum += price * discount;
            }
        }
        sum
    }
}
```

### Implementation Steps

1. **Add Type-Specific Row Accessors**
   ```rust
   impl Row {
       #[inline(always)]
       pub unsafe fn get_f64_unchecked(&self, idx: usize) -> f64 {
           match &self.values[idx] {
               SqlValue::Double(d) => *d,
               SqlValue::Float(f) => *f as f64,
               _ => std::hint::unreachable_unchecked(),
           }
       }
   }
   ```

2. **Create MonomorphicPlan Trait**
   ```rust
   pub trait MonomorphicPlan {
       fn execute(&self, rows: &[Row]) -> Result<Vec<Row>>;
       fn can_handle(stmt: &SelectStatement) -> bool;
   }
   ```

3. **Generate Specialized Plans** for TPC-H queries

4. **Integrate into Query Planner** with fallback to general executor

---

## Phase 3.7: Arena Allocators (Target: 11.5ms)

**Issue**: #2222
**Impact**: 1.3x speedup (saves 80ns/row)
**Implementation Time**: 2 weeks

### The Problem

Every expression evaluation requires heap allocation (~100ns overhead per malloc):

```rust
let temp = Vec::new();  // malloc()
temp.push(value);
// ... use temp ...
// drop(temp)  // free()
```

For Q6: 6K passing rows × multiple allocations = **~0.5ms overhead**

### The Solution

Bump-pointer arena allocator with query-scoped lifetime:

```rust
pub struct QueryArena {
    buffer: Vec<u8>,
    offset: Cell<usize>,
}

impl QueryArena {
    #[inline(always)]
    pub fn alloc<T>(&self, value: T) -> &T {
        let offset = self.offset.get();
        let size = std::mem::size_of::<T>();
        let align = std::mem::align_of::<T>();

        let aligned_offset = (offset + align - 1) & !(align - 1);
        self.offset.set(aligned_offset + size);

        unsafe {
            let ptr = self.buffer.as_ptr().add(aligned_offset) as *mut T;
            std::ptr::write(ptr, value);
            &*ptr
        }
    }
}
```

**Benefits:**
- Allocation: ~1ns (vs ~100ns for malloc)
- Deallocation: 0ns (arena dropped at end)
- Cache locality: sequential allocations
- No fragmentation

### Implementation Steps

1. **Create Arena Module** with QueryArena implementation
2. **Integrate into SelectExecutor** with 10MB default capacity
3. **Replace Vec Allocations** with `arena.alloc_slice()`
4. **Arena-Allocated Expression Results** in evaluator
5. **Pooled Arenas** for concurrent queries

---

## Phase 4: SIMD Batching (Target: 3.8ms)

**Issue**: #2212 (in progress)
**Impact**: 3x speedup (saves 276ns/row)
**Implementation Time**: 4 weeks

### The Problem

Scalar processing: 1 row per CPU instruction, poor cache utilization

### The Solution

Process 1024 rows at once using Arrow RecordBatch + SIMD:

```rust
fn execute_q6_simd(batch: &RecordBatch) -> f64 {
    let shipdate: &Date32Array = batch.column(10).as_any().downcast_ref()?;
    let discount: &Float64Array = batch.column(6).as_any().downcast_ref()?;

    // SIMD predicates - 4-8 values per instruction!
    let date_mask = compute::and(
        compute::ge_scalar(shipdate, DATE_1994)?,
        compute::lt_scalar(shipdate, DATE_1995)?
    )?;

    let products = compute::multiply(price, discount)?;
    // SIMD sum...
}
```

**Already in progress** - see Phase 3 PR #2212

---

## Phase 5: JIT Compilation (Target: 2.5ms) 🎯

**Issue**: #2223 (research)
**Impact**: 1.5x speedup (saves 21ns/row)
**Milestone**: **Beat SQLite by 1.2x!**

### The Problem

Interpreter overhead even with monomorphic execution and SIMD

### The Solution

Compile queries to native machine code using Cranelift:

```rust
use cranelift::prelude::*;

impl QueryCompiler {
    fn compile_q6(&mut self) -> CompiledQuery {
        // Generate native loop
        let loop_block = builder.create_block();

        // Load columns (offsets known at compile time!)
        let shipdate = builder.ins().load(types::I32, row_ptr, SHIPDATE_OFFSET);
        let discount = builder.ins().load(types::F64, row_ptr, DISCOUNT_OFFSET);

        // Generate predicates as native comparisons
        let date_check = builder.ins().icmp_imm(IntCC::GreaterThanOrEqual, shipdate, DATE_1994);

        // Conditional accumulation
        let product = builder.ins().fmul(price, discount);
        let new_sum = builder.ins().fadd(sum, product);

        // Compile to machine code
        self.module.finalize_definitions()?;
    }
}
```

**Benefits:**
- Zero interpreter overhead
- CPU can inline everything
- Perfect branch prediction
- Instruction-level parallelism

---

## Phase 6: Columnar Storage (Target: 180µs) 🚀

**Issue**: #2224 (research)
**Impact**: 10-15x speedup (saves 39ns/row)
**Milestone**: **Match DuckDB!**

### The Problem

Deserialization overhead from row-based B-tree to columnar RecordBatch

**Current flow:**
```
Disk (B-tree rows) → Deserialize → Row structs → Convert → RecordBatch → SIMD
    ↑ 10µs/batch         ↑ 5µs/batch      ↑ 3µs/batch
```

### The Solution

Memory-mapped Arrow IPC files:

```rust
struct ArrowTableFile {
    mmap: Mmap,  // Memory-mapped file
    batches: Vec<BatchOffset>,
}

fn scan_batch(&self, batch_id: usize) -> RecordBatch {
    let offset = self.batches[batch_id];
    let slice = &self.mmap[offset.start..offset.end];

    // Zero-copy! Just pointer arithmetic
    RecordBatch::from_ipc_bytes(slice)?
}
```

**Target flow:**
```
Disk (Arrow IPC) → mmap → RecordBatch → SIMD
    ↑ 0ns (OS handles)   ↑ 1µs/batch
```

**Benefits:**
- Zero deserialization cost
- Perfect cache locality (columnar)
- OS handles I/O (prefetching)
- Direct SIMD access

---

## Performance Projection Table

| Phase | Q6 Time | Per-Row | vs SQLite (3.1ms) | vs DuckDB (180µs) | Cumulative |
|-------|---------|---------|-------------------|-------------------|------------|
| **Baseline** | 230ms | 3.8µs | 74x slower | 1278x slower | - |
| **Phase 1+2** ✅ | 35.2ms | 586ns | 11x slower | 195x slower | 6.5x |
| **+3.5 (Mono)** 🔜 | 15ms | 250ns | 4.8x slower | 83x slower | 15.3x |
| **+3.7 (Arena)** 🔜 | 11.5ms | 192ns | 3.7x slower | 64x slower | 20x |
| **+4 (SIMD)** 🔄 | 3.8ms | 63ns | 1.2x slower | 21x slower | 60x |
| **+5 (JIT)** 📋 | 2.5ms | 42ns | **1.2x faster** ✨ | 14x slower | 92x |
| **+6 (Columnar)** 📋 | 180µs | 3ns | **17x faster** 🚀 | **Match!** 🎯 | **1278x** |

**Legends:**
- ✅ Complete
- 🔜 Next (ready to implement)
- 🔄 In Progress
- 📋 Research Phase

---

## Implementation Timeline

### Near Term (1-2 months) - Quick Wins

**Phase 3.5**: Monomorphic Execution (2 weeks)
- Week 1: Row type accessors + MonomorphicPlan trait
- Week 2: TpchQ6Plan implementation + testing
- **Milestone**: 15ms on Q6 (2.4x improvement)

**Phase 3.7**: Arena Allocators (2 weeks)
- Week 1: QueryArena implementation + integration
- Week 2: Replace allocations + benchmarking
- **Milestone**: 11.5ms on Q6 (1.3x improvement)

**Phase 4**: Complete SIMD (4 weeks)
- Already in progress (#2212)
- **Milestone**: 3.8ms on Q6 (3x improvement, near SQLite!)

**Cumulative**: **20x improvement from baseline** (230ms → 11.5ms)

### Medium Term (3-4 months) - Beat SQLite

**Phase 5**: JIT Compilation (12 weeks total)
- Week 1-2: POC with Cranelift (compile Q6)
- Week 3-4: Benchmarking and decision point
- Week 5-8: Generalize compiler (if POC succeeds)
- Week 9-12: Production integration
- **Milestone**: 2.5ms on Q6 (**beat SQLite by 1.2x!**)

**Cumulative**: **92x improvement from baseline** (230ms → 2.5ms)

### Long Term (6-12 months) - Match DuckDB

**Phase 6**: Columnar Storage (16 weeks)
- Week 1-4: POC (export to Arrow IPC + mmap scan)
- Week 5-10: Hybrid storage design + implementation
- Week 11-14: Background compaction
- Week 15-16: Testing + benchmarking
- **Milestone**: 180µs on Q6 (**match DuckDB!**)

**Cumulative**: **1278x improvement from baseline** (230ms → 180µs)

---

## Success Criteria

### Technical Metrics

- [ ] Phase 3.5 complete: Q6 ≤ 15ms (2.4x from current)
- [ ] Phase 3.7 complete: Q6 ≤ 11.5ms (1.3x from 3.5)
- [ ] Phase 4 complete: Q6 ≤ 4ms (beat SQLite!)
- [ ] Phase 5 complete: Q6 ≤ 3ms (2x faster than SQLite)
- [ ] Phase 6 complete: Q6 ≤ 200µs (match DuckDB)
- [ ] All TPC-H queries show proportional improvements
- [ ] Zero correctness regressions (all tests pass)
- [ ] Memory usage stays reasonable (<2x current)

### Business Impact

- Fast enough for real-time analytics workloads
- Competitive with specialized OLAP databases (DuckDB, ClickHouse)
- Production-ready performance for data warehouse use cases
- Maintained PostgreSQL compatibility and SQL:1999 conformance

---

## Risk Management

### Phase 3.5 Risks (Monomorphic Execution)

**Risk**: Unsafe code bugs leading to undefined behavior
- **Mitigation**: Extensive debug assertions in all `get_*_unchecked()` methods
- **Mitigation**: Run with MIRI for UB detection
- **Mitigation**: Property-based testing with random data

### Phase 3.7 Risks (Arena Allocators)

**Risk**: Arena overflow on large queries
- **Mitigation**: Dynamic growth when capacity exceeded
- **Mitigation**: Configurable arena size per query
- **Mitigation**: Panic with clear error message on overflow

### Phase 5 Risks (JIT Compilation)

**Risk**: Compilation overhead negates runtime gains
- **Mitigation**: Only compile queries executed >10 times
- **Mitigation**: Fast compile times with Cranelift (~1-5ms)
- **Mitigation**: Cache compiled code for prepared statements

### Phase 6 Risks (Columnar Storage)

**Risk**: ACID transaction semantics difficult with immutable Arrow files
- **Mitigation**: Hybrid approach with row-based WAL for writes
- **Mitigation**: Background compaction to Arrow format
- **Mitigation**: MVCC with versioned files (DuckDB approach)

---

## References

### Documentation

- Current profiling: `docs/profiling/q6-analysis.md`
- Parent EPIC: Issue #2220
- Phase implementations: Issues #2221, #2222, #2212, #2223, #2224

### External Resources

- Cranelift: https://cranelift.dev/
- Arrow IPC: https://arrow.apache.org/docs/format/Columnar.html
- DuckDB Storage: https://duckdb.org/docs/internals/storage
- Similar work: HyPer's compilation pipeline, Velox, DataFusion

### Benchmark Commands

```bash
# Run TPC-H Q6 benchmark
cargo bench -p vibesql-executor --bench tpch_benchmark --features sqlite,in-memory-indexes -- q6

# Current results (Phase 1+2 complete):
# VibeSQL: 35.2ms
# SQLite: 3.1ms (11x faster)
# DuckDB: 180µs (195x faster)
```

---

## Conclusion

The path from 35.2ms to 180µs (195x improvement) is technically feasible through a series of well-defined optimizations:

1. **Monomorphic Execution** (2.4x) - Eliminate enum overhead
2. **Arena Allocators** (1.3x) - Eliminate allocation overhead
3. **SIMD Batching** (3x) - Process multiple values per instruction
4. **JIT Compilation** (1.5x) - Eliminate interpreter overhead
5. **Columnar Storage** (10x+) - Eliminate deserialization overhead

The first three phases can be implemented incrementally over 2-3 months with measurable progress at each step. Phases 5-6 require more research but offer the potential to not just match but **exceed** both SQLite and DuckDB performance.

**Bottom Line**: With focused execution, VibeSQL can become one of the fastest SQL databases while maintaining full PostgreSQL compatibility.
