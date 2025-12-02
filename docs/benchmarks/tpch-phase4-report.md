# TPC-H Benchmark Report - Phase 4 Verification

**Date**: 2025-12-02
**Scale Factor**: 0.01
**Branch**: main (commit 12905a7c)

## Summary

Phase 4 optimizations were partially successful but introduced a **significant regression in Q18** that needs to be addressed.

### Key Findings

1. **Q19 Improved** ✅ - Down from 354ms to ~125ms (64% improvement)
2. **Q7 Unchanged** - ~342ms (no improvement from baseline of 319ms)
3. **Q18 Regressed** ⚠️ - Up from 171ms to **2.91s** (17x slower!)

### Q18 Regression Root Cause

The Q18 regression was introduced in PR #3243 (InList predicate pushdown for Q19). The optimization that improved Q19 inadvertently caused Q18's `IN (SELECT ...)` subquery to be executed inefficiently.

**Bisect Results:**
- Fast (171ms): commit 8c59642a (before #3243)
- Slow (3.0s): commit 0efaabe6 (PR #3243)

## Complete Query Results

| Query | Time | vs Phase 4 Baseline | Notes |
|-------|------|---------------------|-------|
| Q1 | 11.3ms | ✅ Improved (was 10.6ms) | |
| Q2 | 8.6ms | ✅ Improved (was 8.2ms) | |
| Q3 | 92.8ms | ~ Same (was 87.8ms) | |
| Q4 | 29.2ms | ~ Same (was 24.2ms) | |
| Q5 | 33.1ms | ~ Same (was 31.4ms) | |
| Q6 | 1.0ms | ✅ Excellent (was 0.96ms) | |
| **Q7** | **342ms** | ⚠️ Regressed (was 319ms) | Target: <150ms |
| Q8 | 23.6ms | ~ Same (was 22.6ms) | |
| Q9 | 156ms | ~ Same (was 162ms) | |
| Q10 | 90.6ms | ~ Same (was 86.3ms) | |
| Q11 | 16.2ms | ~ Same (was 13.1ms) | |
| Q12 | 57.1ms | ~ Regressed (was 88.5ms) | Need to investigate |
| Q13 | 48.4ms | ~ Same (was 48.0ms) | |
| Q14 | 42.5ms | ~ Same (was 39.5ms) | |
| Q15 | 3.2ms | ✅ Excellent (was 2.9ms) | |
| Q16 | 12.8ms | ✅ Improved (was 49.0ms) | |
| Q17 | 26.6ms | ~ Same (was 25.1ms) | |
| **Q18** | **2.91s** | ❌ **REGRESSED** (was 138ms) | **17x slower - needs fix** |
| **Q19** | **125ms** | ✅ **IMPROVED** (was 354ms) | Target: <150ms achieved |
| Q20 | 12.3ms | ✅ Improved (was 40.9ms) | |
| Q21 | 74.4ms | ~ Regressed (was 43.9ms) | Need to investigate |
| Q22 | 5.1ms | ✅ Excellent (was 5.2ms) | |

## Top 5 Slowest Queries for Phase 5 Optimization

| Rank | Query | Time | Description | Priority |
|------|-------|------|-------------|----------|
| 1 | **Q18** | 2.91s | Large Volume Customer | **P0 - Critical regression** |
| 2 | Q7 | 342ms | Volume Shipping (multi-nation join) | P1 - Original target |
| 3 | Q9 | 156ms | Product Type Profit | P2 |
| 4 | Q19 | 125ms | Discounted Revenue | ✅ Achieved target |
| 5 | Q3 | 93ms | Shipping Priority | P2 |

## Recommended Phase 5 Actions

### P0 (Critical)
- [ ] **Fix Q18 regression** - Investigate subquery evaluation in context of InList pushdown (#3243)
  - Q18 has `o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY ... HAVING SUM(l_quantity) > 300)`
  - The subquery appears to be re-evaluated per row instead of materialized once

### P1 (High)
- [ ] **Optimize Q7** - Target <150ms (currently 342ms)
  - Multi-nation supplier/customer join with date filtering
  - Consider better join ordering

### P2 (Medium)
- [ ] Investigate Q21 regression (43.9ms → 74.4ms)
- [ ] Investigate Q12 change (88.5ms → 57.1ms - verify this is correct)

## Appendix: Benchmark Commands

```bash
# Full TPC-H benchmark
SCALE_FACTOR=0.01 ./target/release/deps/tpch_profiling-*

# Single query profiling
JOIN_PROFILE=1 SCALE_FACTOR=0.01 ./target/release/deps/tpch_profiling-* Q18
```

## References

- Phase 4 tracking: #3220
- Q19 optimization (introduced Q18 regression): #3243
- Phase 5 tracking: #3304
