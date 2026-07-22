# SQL:1999 Gotchas and Edge Cases

This document tracks surprising behaviors, edge cases, and gotchas from the SQL:1999 standard.

## Purpose

SQL:1999 has many subtle behaviors that differ from intuition. This document helps:
- Avoid implementing incorrect semantics
- Handle edge cases correctly
- Understand the "why" behind SQL behavior
- Reference during implementation

## Format

Each gotcha includes:
- **What**: The surprising behavior
- **Why**: Reason from SQL:1999 spec
- **Example**: SQL demonstrating the issue
- **Impact**: How this affects implementation
- **Reference**: SQL:1999 spec section

---

## NULL Handling

### Gotcha 1: NULL = NULL is UNKNOWN, not TRUE

**What**: Comparing NULL to NULL does not return TRUE, it returns UNKNOWN.

**Why**: NULL represents "unknown value". Comparing two unknown values cannot determine equality.

**Example**:
```sql
SELECT * FROM table WHERE column = NULL;  -- Returns 0 rows, even if column contains NULL

SELECT * FROM table WHERE column IS NULL; -- Correct: Returns rows where column is NULL
```

**Impact**:
- Must implement three-valued logic (TRUE, FALSE, UNKNOWN)
- WHERE clause only returns TRUE rows, not UNKNOWN
- Cannot use `= NULL`, must use `IS NULL`

**SQL:1999 Reference**: Part 2, Section 8.2 - Comparison predicate

---

### Gotcha 2: Aggregates Ignore NULL Values

**What**: Aggregate functions (COUNT, SUM, AVG) ignore NULL values, except COUNT(*).

**Why**: Standard defines aggregates to skip NULL, as NULL means "unknown value".

**Example**:
```sql
CREATE TABLE t (x INTEGER);
INSERT INTO t VALUES (1), (2), (NULL), (4);

SELECT COUNT(x) FROM t;    -- Returns 3 (ignores NULL)
SELECT COUNT(*) FROM t;    -- Returns 4 (counts all rows)
SELECT AVG(x) FROM t;      -- Returns 2.33 (7/3, not 7/4)
SELECT SUM(x) FROM t;      -- Returns 7 (ignores NULL)
```

**Impact**:
- COUNT(column) ≠ COUNT(*)
- AVG is SUM / COUNT(column), not SUM / COUNT(*)
- Must handle NULL specially in aggregate implementation

**SQL:1999 Reference**: Part 2, Section 6.16 - Set function specification

---

### Gotcha 3: NULL in Boolean Logic

**What**: Boolean operations with NULL follow three-valued logic rules.

**Why**: NULL means "unknown", so TRUE OR UNKNOWN = TRUE (true regardless), but TRUE AND UNKNOWN = UNKNOWN (depends on unknown).

**Truth Tables**:
```
AND:
         TRUE    FALSE   UNKNOWN
TRUE     TRUE    FALSE   UNKNOWN
FALSE    FALSE   FALSE   FALSE
UNKNOWN  UNKNOWN FALSE   UNKNOWN

OR:
         TRUE    FALSE   UNKNOWN
TRUE     TRUE    TRUE    TRUE
FALSE    TRUE    FALSE   UNKNOWN
UNKNOWN  TRUE    UNKNOWN UNKNOWN

NOT:
TRUE     -> FALSE
FALSE    -> TRUE
UNKNOWN  -> UNKNOWN
```

**Example**:
```sql
SELECT * FROM t WHERE age > 18 OR status = 'active';
-- If age is NULL and status is 'active', row IS returned (TRUE OR UNKNOWN = TRUE)

SELECT * FROM t WHERE age > 18 AND status = 'active';
-- If age is NULL and status is 'active', row NOT returned (UNKNOWN AND TRUE = UNKNOWN)
```

**Impact**: Must implement full three-valued logic engine

**SQL:1999 Reference**: Part 2, Section 4.4 - The boolean type

---

## Boolean Type

### Gotcha 4: IS TRUE vs = TRUE

**What**: `column = TRUE` is different from `column IS TRUE` for NULL handling.

**Why**:
- `= TRUE` returns UNKNOWN when column is NULL
- `IS TRUE` returns FALSE when column is NULL

**Example**:
```sql
CREATE TABLE t (active BOOLEAN);
INSERT INTO t VALUES (TRUE), (FALSE), (NULL);

SELECT * FROM t WHERE active = TRUE;    -- Returns 1 row (TRUE)
SELECT * FROM t WHERE active IS TRUE;   -- Returns 1 row (TRUE)

SELECT * FROM t WHERE active = FALSE;   -- Returns 1 row (FALSE)
SELECT * FROM t WHERE active IS FALSE;  -- Returns 1 row (FALSE)

SELECT * FROM t WHERE active = NULL;    -- Returns 0 rows (comparison with NULL is UNKNOWN)
SELECT * FROM t WHERE active IS NULL;   -- Returns 1 row (NULL)

-- The difference matters in WHERE clauses:
WHERE active = TRUE   -- Excludes NULL rows
WHERE active IS TRUE  -- Also excludes NULL rows, but more explicit
WHERE active         -- Includes TRUE, excludes FALSE and NULL
WHERE active IS NOT FALSE -- Includes TRUE and NULL, excludes FALSE
```

**Impact**:
- IS predicates are safer for boolean comparisons
- Plain boolean in WHERE treats NULL as FALSE

**SQL:1999 Reference**: Part 2, Section 8.7 - Null predicate

---

## String Handling

### Gotcha 5: Trailing Spaces in CHAR vs VARCHAR

**What**: CHAR pads with spaces, VARCHAR does not. Comparison behavior depends on data type.

**Why**: CHAR is fixed-length (pads), VARCHAR is variable-length (no padding).

**Example**:
```sql
CREATE TABLE t (c CHAR(10), v VARCHAR(10));
INSERT INTO t VALUES ('hello', 'hello');

-- In storage:
-- c = 'hello     ' (padded to 10 chars)
-- v = 'hello' (5 chars)

SELECT c = 'hello' FROM t;  -- Depends on comparison semantics
```

**Impact**:
- Must handle CHAR padding correctly
- Comparison rules differ for CHAR vs VARCHAR
- CHAR(n) always stores n characters

**SQL:1999 Reference**: Part 2, Section 4.3 - Character strings

---

### Gotcha 6: LIKE Pattern Matching Edge Cases

**What**: LIKE has subtle escaping and character matching rules.

**Why**: Standard defines specific pattern matching semantics.

**Example**:
```sql
-- Underscore matches any single character
'abc' LIKE 'a_c'  -- TRUE
'ac' LIKE 'a_c'   -- FALSE (needs exactly one character)

-- Percent matches zero or more characters
'abc' LIKE 'a%c'  -- TRUE
'ac' LIKE 'a%c'   -- TRUE (% matches zero characters)

-- Escaping special characters
'a_c' LIKE 'a\_c' ESCAPE '\'  -- TRUE (matches literal underscore)
'abc' LIKE 'a\_c' ESCAPE '\'  -- FALSE
```

**Impact**: Must implement pattern matching with proper escaping

**SQL:1999 Reference**: Part 2, Section 8.5 - LIKE predicate

---

## Joins

### Gotcha 7: OUTER JOIN NULL Handling

**What**: OUTER JOINs produce NULL for unmatched rows, which affects subsequent predicates.

**Why**: Standard defines NULL for missing join partner.

**Example**:
```sql
CREATE TABLE t1 (a INTEGER);
CREATE TABLE t2 (b INTEGER);
INSERT INTO t1 VALUES (1), (2), (3);
INSERT INTO t2 VALUES (2), (3), (4);

SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.b WHERE t2.b > 2;
-- Returns: (3, 3)
-- Row (1, NULL) is filtered out because NULL > 2 is UNKNOWN

SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.b WHERE t2.b > 2 OR t2.b IS NULL;
-- Returns: (1, NULL), (3, 3)
-- Must explicitly handle NULL to preserve left join semantics
```

**Impact**:
- OUTER JOIN + WHERE can eliminate unmatched rows unexpectedly
- Must carefully handle NULL in predicates after OUTER JOIN

**SQL:1999 Reference**: Part 2, Section 7.7 - Joined table

---

### Gotcha 8: NATURAL JOIN Semantics

**What**: NATURAL JOIN implicitly joins on ALL columns with matching names.

**Why**: Standard defines NATURAL JOIN as join on all common columns.

**Example**:
```sql
CREATE TABLE emp (id INTEGER, dept_id INTEGER, name VARCHAR(50));
CREATE TABLE dept (dept_id INTEGER, name VARCHAR(50));

-- NATURAL JOIN joins on BOTH dept_id AND name
SELECT * FROM emp NATURAL JOIN dept;
-- Equivalent to:
SELECT * FROM emp JOIN dept ON emp.dept_id = dept.dept_id AND emp.name = dept.name;

-- This is usually NOT what you want!
```

**Impact**:
- NATURAL JOIN can be surprising
- Generally discouraged in practice
- Must implement correct common-column detection

**SQL:1999 Reference**: Part 2, Section 7.7 - Joined table

---

## Recursive Queries

### Gotcha 9: WITH RECURSIVE Cycle Detection

**What**: Recursive queries can cycle infinitely without proper termination.

**Why**: Standard allows recursion but doesn't mandate cycle detection.

**Example**:
```sql
-- Dangerous: Can cycle infinitely
WITH RECURSIVE bad AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n FROM bad
)
SELECT * FROM bad;  -- Infinite loop!

-- Safe: Terminates
WITH RECURSIVE good AS (
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM good WHERE n < 10
)
SELECT * FROM good;  -- Returns 1..10
```

**Impact**:
- Must implement cycle detection
- Need termination guarantees
- Consider max recursion depth limits

**SQL:1999 Reference**: Part 2, Section 7.12 - Query expression

---

## Type Conversions

### Gotcha 10: CAST Behavior with NULL

**What**: CAST of NULL to any type returns NULL (typed NULL).

**Why**: NULL is preserved across type conversions.

**Example**:
```sql
SELECT CAST(NULL AS INTEGER);   -- Returns NULL (of type INTEGER)
SELECT CAST(NULL AS VARCHAR(10)); -- Returns NULL (of type VARCHAR)

-- This affects type checking:
CREATE TABLE t (x INTEGER);
INSERT INTO t VALUES (CAST(NULL AS VARCHAR(10))); -- Type error or implicit conversion?
```

**Impact**: Must handle typed NULL correctly

**SQL:1999 Reference**: Part 2, Section 6.12 - Cast specification

---

## Grouping and Aggregation

### Gotcha 11: GROUP BY NULL Handling

**What**: NULL values form their own group in GROUP BY.

**Why**: Standard treats all NULLs as belonging to same group.

**Example**:
```sql
CREATE TABLE t (category VARCHAR(10), amount INTEGER);
INSERT INTO t VALUES ('A', 10), ('B', 20), (NULL, 30), (NULL, 40);

SELECT category, SUM(amount) FROM t GROUP BY category;
-- Returns:
-- 'A', 10
-- 'B', 20
-- NULL, 70  (all NULLs grouped together)
```

**Impact**:
- NULL values group together (even though NULL ≠ NULL)
- This is different from equality semantics

**SQL:1999 Reference**: Part 2, Section 7.9 - Group by clause

---

### Gotcha 12: HAVING Without GROUP BY

**What**: HAVING can be used without GROUP BY, treating entire result as one group.

**Why**: Standard allows HAVING on implicit group.

**Example**:
```sql
SELECT SUM(amount) FROM sales HAVING SUM(amount) > 1000;
-- Returns one row if total > 1000, zero rows otherwise
```

**Impact**: Must handle HAVING with and without GROUP BY

**SQL:1999 Reference**: Part 2, Section 7.10 - Having clause

---

## Constraints

### Gotcha 13: CHECK Constraints and NULL

**What**: CHECK constraints with NULL return UNKNOWN, which is treated as TRUE (constraint satisfied).

**Why**: Only FALSE violates a constraint; UNKNOWN is permitted.

**Example**:
```sql
CREATE TABLE t (
  age INTEGER CHECK (age >= 18)
);

INSERT INTO t VALUES (NULL);  -- Succeeds! NULL >= 18 is UNKNOWN, not FALSE
INSERT INTO t VALUES (17);    -- Fails: 17 >= 18 is FALSE
INSERT INTO t VALUES (20);    -- Succeeds: 20 >= 18 is TRUE
```

**Impact**:
- CHECK constraints don't prevent NULL unless explicitly added
- Must use NOT NULL separately if NULL should be rejected

**SQL:1999 Reference**: Part 2, Section 11.7 - Check constraint definition

---

## Order and Sorting

### Gotcha 14: NULL Ordering

**What**: SQL:1999 leaves NULL ordering implementation-defined (before or after non-NULL values).

**Why**: Standard allows databases to choose NULL sort order.

**Example**:
```sql
SELECT * FROM t ORDER BY x;
-- NULL values may appear first or last depending on implementation

-- SQL:2003 added explicit control:
SELECT * FROM t ORDER BY x NULLS FIRST;
SELECT * FROM t ORDER BY x NULLS LAST;
-- But these are SQL:2003, not SQL:1999
```

**Impact**:
- Must choose and document NULL ordering behavior
- Different databases make different choices (PostgreSQL: last, MySQL: first)

**SQL:1999 Reference**: Part 2, Section 7.11 - Order by clause

---

## Transactions

### Gotcha 15: Isolation Level Anomalies

**What**: Lower isolation levels allow specific anomalies (dirty read, non-repeatable read, phantom read).

**Why**: Standard defines trade-offs between consistency and concurrency.

**Isolation Levels**:
- READ UNCOMMITTED: Allows dirty reads
- READ COMMITTED: Prevents dirty reads
- REPEATABLE READ: Prevents non-repeatable reads
- SERIALIZABLE: Prevents all anomalies

**Impact**:
- Must understand and implement each isolation level correctly
- Each level has specific consistency guarantees

**SQL:1999 Reference**: Part 2, Section 4.28 - SQL-transactions

---

## Implementation Notes

### Testing Strategy for Gotchas

For each gotcha:
1. Create explicit test cases
2. Test boundary conditions
3. Compare behavior with PostgreSQL/other databases
4. Document implementation decisions

### Common Implementation Mistakes

1. **Forgetting three-valued logic**: Implementing boolean logic as two-valued
2. **Wrong NULL comparisons**: Using = instead of IS
3. **Incorrect aggregate NULL handling**: Including NULL in COUNT/AVG
4. **Missing cycle detection**: Allowing infinite recursion
5. **Wrong CHAR padding**: Not handling trailing spaces

---

## Related Documents

- [LESSONS_LEARNED.md](./LESSONS_LEARNED.md) - General lessons
- [CHALLENGES.md](CHALLENGES.md) - Major challenges
- SQL1999_RESEARCH.md - Standard research

---

**Last Updated**: 2025-10-25
**Total Gotchas**: 15
**Status**: Growing as we implement features

**Remember**: When in doubt, test against PostgreSQL and check the SQL:1999 spec!
