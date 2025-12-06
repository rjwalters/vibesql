# Slow/Stuck Test Files

## Issue
During parallel testing with 64 workers, several workers get stuck at 100% CPU on specific test files. These files appear to contain queries that either infinite loop or are extremely inefficient.

## Identified Problem Files

Based on parallel testing run (2025-11-09):

1. **`select5.test`** - Worker 2
   - Symptoms: Repeated memory warnings (6.48 GB)
   - Status: Stuck in infinite loop or extremely inefficient query
   - Priority: **HIGH** - This is one of the core select tests

2. **`random/expr/slt_good_73.test`** - Worker 3
   - Status: Still running after other workers completed
   - Type: Random expression test

3. **`index/commute/10/slt_good_34.test`** - Worker 4
   - Status: Still running
   - Type: Index commutative property test

4. **`index/random/100/slt_good_0.test`** - Worker 5
   - Status: Still running
   - Type: Random index test (large dataset - 100 rows)

5. **`index/between/100/slt_good_2.test`** - Worker 6
   - Status: Still running
   - Type: BETWEEN operator test (large dataset - 100 rows)

## Pattern Analysis

**Common characteristics:**
- Most are index-related tests (4 out of 5)
- Several involve larger datasets (100 rows vs. 10 rows)
- Memory usage warnings on select5.test suggest unbounded growth

**Test categories affected:**
- Core SELECT tests (select5.test) - Critical
- Index tests - Multiple files
- Random expression tests - At least one file

## Recommended Actions

### Short-term (Immediate)
1. **Add query timeout enforcement** at the query level (not just file level) - **Issue #1037**
2. **Skip known slow files** - Add to a blocklist temporarily
3. **Optimize test execution** to handle large test files efficiently

### Medium-term (Investigation)
1. **Profile select5.test** - Identify specific query causing infinite loop
2. **Review index test implementation** - Check for O(n²) or worse algorithms
3. **Add memory limits** - Kill queries exceeding reasonable memory usage
4. **Optimize expression evaluator** - Improve performance for nested CASE/NULLIF/COALESCE - **Issue #1038**

### Long-term (Fix Root Cause)
1. **Fix the underlying query execution issues** causing infinite loops
2. **Optimize index operations** to handle larger datasets efficiently
3. **Implement progressive timeout** - Queries should timeout faster on subsequent attempts

## Query Timeout Status

Current implementation has:
- ✅ File-level timeout (enforced via executor limits)
- ✅ Time budget check between files
- ⚠️ **Missing**: Query-level timeout within a file

This explains why workers get stuck on individual queries within a file.

## Testing Impact

With 5 workers stuck out of 64:
- **Efficiency loss**: ~8% of compute wasted
- **Time impact**: Tests still complete (queue empties), but workers hang
- **Resource waste**: 5 cores at 100% indefinitely

## Files for Further Investigation

Priority order:
1. `select5.test` - Core functionality, memory issues
2. `random/expr/slt_good_73.test` - **INVESTIGATED** - 15,779 complex expression queries (see #1037, #1038)
3. `index/random/100/slt_good_0.test` - Large dataset index test
4. `index/between/100/slt_good_2.test` - Large dataset BETWEEN test
5. `index/commute/10/slt_good_34.test` - Commutative property test

### Investigation Results

**`select5.test`** (Issue #1036):
- **Root cause**: Cartesian product explosion (64-table joins without predicate pushdown)
- **Memory**: 6.48 GB exhaustion
- **Solution**: Predicate pushdown optimization
- **Status**: Issue created, ready for implementation

**`random/expr/slt_good_73.test`** (Issues #1037, #1038):
- **Root cause**: Volume (15,779 queries) + complexity (681-char nested CASE expressions)
- **Not an infinite loop**: Just extremely slow expression evaluation
- **Estimated runtime**: 20-30 minutes without query-level timeouts
- **Key insight**: Missing query-level timeout allows workers to appear hung
- **Solution**: See issues #1037 (timeouts) and #1038 (performance optimization)
- **Details**: `/tmp/slow_query_investigation/ANALYSIS.md`

**`index/commute/10/slt_good_34.test`** (Issue #1039):
- **Root cause**: 10,000 queries with 2,884-char WHERE clauses
- **Complexity**: 29,590 AND + 21,460 OR operators, 620 subqueries
- **Data**: 10 rows per table
- **Solution**: Boolean expression simplification, short-circuit eval, subquery caching
- **Status**: Issue created

**`index/between/100/slt_good_2.test`** (Related to #1039):
- **Root cause**: Similar to commute but with larger dataset
- **Complexity**: 5,358-char WHERE clauses, 5,000 BETWEEN clauses
- **Data**: 100 rows per table (10x more than commute)
- **Test purpose**: BETWEEN vs range expansion equivalence
- **Solution**: Same as #1039 + potential BETWEEN-specific optimization
- **Status**: Added to #1039 as related file

**`index/random/100/slt_good_0.test`** (Related to #1039):
- **Root cause**: Volume (11,905 queries × 100 rows)
- **Complexity**: Low - simple NULL handling and operator equivalence tests
- **Est. row scans**: ~6 million total
- **Solution**: Query-level timeout (#1037), possible NULL handling optimization
- **Status**: Added to #1039 as related file

**`index/orderby/1000/slt_good_0.test`** (Issue #1197):
- **Root cause**: Infinite loop in ORDER BY operations with large indexed dataset
- **Symptoms**: 100% CPU usage, no progress, stable memory (~49 MB)
- **Data**: 1000 rows (10x larger than most index tests)
- **Test purpose**: ORDER BY operations on indexed columns
- **Solution**: Added to blocklist temporarily while investigating root cause
- **Status**: **BLOCKED** - Currently skipped in parallel test runs
