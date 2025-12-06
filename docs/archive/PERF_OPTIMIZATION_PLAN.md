# Performance Optimization Plan: Closing the 100-200x Gap

## Current State
- TPC-H Q3: 453ms (VibeSQL) vs 2.1ms (DuckDB) = **215x gap**
- TPC-H Q10: 425ms vs 3.3ms = **129x gap**
- TPC-H Q12: 325ms vs 2.5ms = **130x gap**

## Root Cause Analysis

### 1. Row Materialization (16x overhead)
Every JOIN forces materialization through `FromData::as_rows()`:
```rust
// Current: O(n) allocations per table in JOIN
let rows = from_result.as_rows(); // Forces Vec<Row> creation
```

Measured: 348ms for materialization vs 21ms for actual Q6 filtering.

### 2. SqlValue Enum Dispatch (5x overhead)
Every value access requires:
```rust
match value {
    SqlValue::Integer(i) => *i,  // Branch for every access
    SqlValue::Bigint(i) => *i,
    // ... 16 variants
}
```

### 3. Memory Allocation Pattern (3x overhead)
- Per-row Vec<SqlValue> allocation
- Per-comparison Vec<bool> bitmap allocation
- HashMap creation per evaluator instance

---

## Optimization Strategy

### Phase 1: Columnar Hash Join (10-20x impact)

**Goal**: Execute hash joins entirely in columnar format without row materialization.

**Current Flow**:
```
Table Scan → Vec<Row> → Build Hash Table (row keys) → Probe (row keys) → Vec<Row>
              ↑ BOTTLENECK                            ↑ BOTTLENECK
```

**Target Flow**:
```
Table Scan → ColumnarBatch → Build Hash Table (column arrays) → SIMD Probe → ColumnarBatch
             ↑ Direct from storage                              ↑ Vectorized
```

**Implementation**:

1. **Columnar Hash Table Builder** (`hash_join/columnar_build.rs`)
   ```rust
   pub struct ColumnarHashTable {
       // Hash buckets pointing to row indices
       buckets: Vec<u32>,
       // Chain links for collision resolution
       next: Vec<u32>,
       // Original column arrays (zero-copy reference)
       key_columns: Vec<&ColumnArray>,
   }

   impl ColumnarHashTable {
       pub fn build(batch: &ColumnarBatch, key_indices: &[usize]) -> Self {
           // SIMD hash computation on contiguous arrays
           let hashes = simd_hash_columns(&batch.columns, key_indices);
           // Build hash table from hashes
           Self::from_hashes(hashes, batch.row_count())
       }
   }
   ```

2. **SIMD Probe Phase** (`hash_join/columnar_probe.rs`)
   ```rust
   pub fn columnar_probe(
       probe_batch: &ColumnarBatch,
       probe_key_indices: &[usize],
       hash_table: &ColumnarHashTable,
   ) -> ProbeResult {
       // SIMD hash computation
       let probe_hashes = simd_hash_columns(&probe_batch.columns, probe_key_indices);

       // Vectorized bucket lookup
       let matches = simd_bucket_lookup(probe_hashes, &hash_table.buckets);

       // Gather matching row indices
       ProbeResult { left_indices, right_indices }
   }
   ```

3. **Integration with Join Executor**
   ```rust
   pub fn hash_join_columnar(
       left: ColumnarBatch,
       right: ColumnarBatch,
       left_keys: &[usize],
       right_keys: &[usize],
   ) -> Result<ColumnarBatch, ExecutorError> {
       // Build on smaller side
       let (build_side, probe_side) = if left.row_count() < right.row_count() {
           (&left, &right)
       } else {
           (&right, &left)
       };

       let hash_table = ColumnarHashTable::build(build_side, build_keys);
       let result = columnar_probe(probe_side, probe_keys, &hash_table);

       // Gather output columns (no row materialization!)
       gather_result_columns(left, right, result)
   }
   ```

**Expected Impact**: 10-20x speedup on multi-table JOINs

---

### Phase 2: Expression Specialization (3-5x impact)

**Goal**: Eliminate SqlValue enum dispatch in hot paths.

**Implementation**:

1. **Type-Specialized Evaluators**
   ```rust
   pub enum SpecializedEvaluator {
       // All integer operations (SUM, COUNT, comparisons)
       IntegerPath {
           columns: Vec<usize>,  // Column indices
           ops: Vec<IntegerOp>,  // Pre-compiled operations
       },
       // All float operations (AVG, arithmetic)
       FloatPath {
           columns: Vec<usize>,
           ops: Vec<FloatOp>,
       },
       // Mixed types (fallback)
       Generic(CombinedExpressionEvaluator),
   }
   ```

2. **Compile-time Expression Analysis**
   ```rust
   fn specialize_expression(expr: &Expression, schema: &Schema) -> SpecializedEvaluator {
       let types = infer_expression_types(expr, schema);

       if types.all_integer() {
           compile_integer_path(expr)
       } else if types.all_numeric() {
           compile_float_path(expr)
       } else {
           SpecializedEvaluator::Generic(create_evaluator(expr))
       }
   }
   ```

3. **SIMD Expression Evaluation on Columns**
   ```rust
   fn eval_integer_sum(column: &[i64]) -> i64 {
       // Auto-vectorizes to SIMD
       column.iter().sum()
   }

   fn eval_integer_filter(column: &[i64], threshold: i64) -> BitVec {
       // SIMD comparison
       simd_ops::gt_i64(column, threshold)
   }
   ```

**Expected Impact**: 3-5x speedup on expression-heavy queries

---

### Phase 3: Memory Allocation Consolidation (2-3x impact)

**Goal**: Eliminate per-row and per-operation allocations.

**Implementation**:

1. **Arena-Based Allocation for All Transient Data**
   ```rust
   pub struct QueryContext {
       arena: QueryArena,
       // Pre-allocated buffers
       bitmap_buffer: &'arena mut [u64],  // Packed bits, not Vec<bool>
       hash_buffer: &'arena mut [u64],
       index_buffer: &'arena mut [u32],
   }
   ```

2. **BitVec for Filter Results**
   ```rust
   // Current: Vec<bool> = 8 bytes per element
   let filter = vec![false; 100_000];  // 100KB

   // Target: Packed bits = 1 bit per element
   let filter = BitVec::with_capacity(100_000);  // 12.5KB
   ```

3. **Buffer Reuse in Pipeline**
   ```rust
   impl ExecutionPipeline {
       fn execute(&self, ctx: &mut QueryContext) -> Result<...> {
           // Reuse ctx.bitmap_buffer across operations
           let filter1 = ctx.eval_filter(pred1);
           let filter2 = ctx.eval_filter(pred2);
           ctx.combine_filters_and(filter1, filter2);  // In-place
       }
   }
   ```

**Expected Impact**: 2-3x speedup, reduced GC pressure

---

### Phase 4: Parallel Execution (2-4x impact)

**Goal**: Utilize all CPU cores for query execution.

**Implementation**:

1. **Morsel-Driven Parallelism**
   ```rust
   // Split large batches into work units
   const MORSEL_SIZE: usize = 1024;

   fn parallel_filter(batch: &ColumnarBatch, predicate: &Predicate) -> BitVec {
       batch.par_chunks(MORSEL_SIZE)
           .map(|morsel| eval_filter_morsel(morsel, predicate))
           .reduce(BitVec::new, |a, b| a.or(&b))
   }
   ```

2. **Parallel Hash Join Build**
   ```rust
   fn parallel_build_hash_table(batch: &ColumnarBatch) -> ColumnarHashTable {
       // Partition by hash prefix
       let partitions = partition_by_hash(batch, num_threads);

       // Build partition-local hash tables in parallel
       let local_tables: Vec<_> = partitions.par_iter()
           .map(|p| ColumnarHashTable::build_local(p))
           .collect();

       // Merge (no locking needed - partitions are disjoint)
       ColumnarHashTable::merge(local_tables)
   }
   ```

**Expected Impact**: 2-4x on multi-core systems

---

## Implementation Order

| Phase | Description | Impact | Effort | Dependencies |
|-------|-------------|--------|--------|--------------|
| **1A** | Columnar hash table builder | 5x | Medium | None |
| **1B** | SIMD probe phase | 5x | Medium | 1A |
| **1C** | Integration with join executor | 2x | Low | 1A, 1B |
| **2A** | Expression type analysis | 2x | Medium | None |
| **2B** | Integer path specialization | 2x | Medium | 2A |
| **3A** | BitVec filter results | 1.5x | Low | None |
| **3B** | Arena integration | 1.5x | Medium | None |
| **4A** | Parallel filter | 2x | Medium | 3A |
| **4B** | Parallel hash build | 2x | Medium | 1A |

**Recommended Order**: 1A → 1B → 1C → 3A → 2A → 2B → 4A → 4B → 3B

---

## Success Metrics

| Query | Current | Target (Phase 1) | Target (All) | DuckDB |
|-------|---------|------------------|--------------|--------|
| Q3 | 453ms | 45ms | 10ms | 2.1ms |
| Q10 | 425ms | 42ms | 10ms | 3.3ms |
| Q12 | 325ms | 32ms | 8ms | 2.5ms |

**Goal**: Close gap from 100-200x to 3-5x (within same order of magnitude as DuckDB)

---

## Files to Modify/Create

### New Files
- `select/join/hash_join/columnar_build.rs` - Columnar hash table builder
- `select/join/hash_join/columnar_probe.rs` - SIMD probe implementation
- `select/join/hash_join/columnar_join.rs` - Integration module
- `evaluator/specialized/integer.rs` - Integer path evaluator
- `evaluator/specialized/float.rs` - Float path evaluator

### Modified Files
- `select/join/mod.rs` - Add columnar join dispatcher
- `select/scan/mod.rs` - Return ColumnarBatch directly from scan
- `select/columnar/batch.rs` - Add gather operations
- `arena.rs` - Extend for new buffer types
- `simd/hashing.rs` - Add column-wise SIMD hashing
