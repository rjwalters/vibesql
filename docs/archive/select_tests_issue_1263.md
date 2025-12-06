# Root-Level SELECT Tests Analysis (Issue #1263)

## Test Status Summary
- select1.test: ❌ FAILING - Division result hash mismatch
- select2.test: ❌ FAILING - Multi-column NULL result formatting issue
- select3.test: ❌ FAILING - Division result hash mismatch  
- select4.test: ❌ FAILING - Set operations (EXCEPT/UNION) returning wrong row count

---

## Detailed Analysis

### 1. select1.test - Division Result Hash Mismatch

**Query Failing:**
```sql
SELECT a+b*2+c*3+d*4+e*5,
       (a+b+c+d+e)/5
  FROM t1
 ORDER BY 1,2
```

**Issue:**
- The query returns 60 values (30 rows × 2 columns)
- Expected hash: `010239dc0b8e04fa7aa927551a6a231f`
- Actual hash: `c2ae0191f0fdd8db6bd7e78cf722a6b4`

**Root Cause:**
The division `(a+b+c+d+e)/5` returns float values with `.0` formatting (e.g., `102.0`).
The hash mismatch suggests either:
1. The float formatting is incorrect (should be `102` not `102.0`?)
2. The test expectations are based on different computation
3. Precision/rounding issue in float division

**Investigation:**
- Float division is correctly returning results (e.g., 102.0, 107.0, etc.)
- The sqllogictest file specifies `query IR` (Integer, Real) which indicates both columns should be exact types
- Recent change from integer → float division (commit b8d2cacb) was reverted

**Next Steps:**
1. Verify that ALL 30 rows are being generated correctly
2. Check if float formatting should use `.0` or just `102`
3. Investigate if the expected hash values in test files are correct

---

### 2. select2.test - Multi-Column NULL Result Formatting Issue

**Query Failing:**
```sql
SELECT a,
       (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b),
       a+b*2+c*3+d*4+e*5,
       d
  FROM t1
 WHERE a IS NULL
```

**Issue:**
- Expected: Values on separate lines (`NULL`, `1`, `NULL`, `114`, etc.)
- Actual: Values space-separated on same line (`NULL 1 NULL 114`, `NULL 18 NULL 207`)

**Root Cause:**
The test file format shows each value on a separate line (8 lines for 2 rows × 4 columns), but the sqllogictest library is receiving space-separated values per row (2 lines).

This is actually CORRECT behavior for the test runner! The sqllogictest library formats results as space-separated columns.

But the expected format in the test file has newline separators. This suggests:
1. The test file expectations might be malformed, or
2. The sqllogictest library expects a different format than what we're providing

**Investigation:**
- The sqllogictest runner successfully formats results correctly for other tests
- The issue might be specific to how NULL values are formatted
- Or the test file itself has the wrong expected output format

**Next Steps:**
1. Check if test file was edited incorrectly (expected format should be space-separated)
2. Verify with standard sqllogictest format for multi-column output
3. Test with actual NULL values to ensure they're handled correctly

---

### 3. select3.test - Division Result Hash Mismatch

**Similar to select1.test**

**Query Failing:**
```sql
SELECT a+b*2+c*3+d*4+e*5,
       (a+b+c+d+e)/5
  FROM t1
```

**Issue:**
- Expected hash: `2fa2578527a600ef66c3f251e5265621`
- Actual hash: `e6884a1a85d2a8036df3fc9517ea8030`

**Root Cause:** Same as select1 - division result hash mismatch

**Note:** select3 doesn't have ORDER BY, while select1 does. This might affect row ordering and hash values.

---

### 4. select4.test - Set Operations Wrong Row Count

**Query Failing:**
Complex query with multiple EXCEPT/UNION/UNION ALL operations on 9 tables (t1-t9)

**Issue:**
- Expected: 41 rows
- Actual: 17 rows
- Hash expected: `bbf619d1c2aae1fc385fb08bddcae829`
- Hash actual: `9f8293f469ba67afc447d2fe0446ddcc`

**Root Cause:**
The set operations (EXCEPT and UNION) are not returning the correct number of rows.

Possibilities:
1. EXCEPT operation is removing too many rows
2. UNION is deduplicating incorrectly
3. UNION ALL is not aggregating correctly
4. Set operation semantics are different from expected

**Investigation Needed:**
1. Test each EXCEPT and UNION operation in isolation
2. Verify set operation semantics (EXCEPT should remove matching rows, UNION should deduplicate)
3. Check if ordering affects set operations

**Next Steps:**
1. Create a minimal test case with the first SELECT in the EXCEPT chain
2. Then add each EXCEPT/UNION operation one at a time
3. Verify intermediate results against expected row counts

---

## Recommended Fix Strategy

1. **Priority 1 (select2):** Fix the multi-column formatting - might be a simple test file format issue
2. **Priority 2 (select4):** Fix set operations - likely a logic issue that affects multiple features
3. **Priority 3 (select1/3):** Resolve division hash mismatches - might reveal precision issues

## Related Issues
- #1262 - Numeric formatting (related to division result formatting)
- #1261 - Random test analysis (may share error patterns)
- #1260 - ORDER BY issues (select1 has ORDER BY, might be related)
