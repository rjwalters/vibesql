//! Tests for recursive CTE support (issue #4480, #4483, #4485)
//!
//! This module tests recursive Common Table Expressions (CTEs) with both UNION and UNION ALL.
//! Recursive CTEs enable iterative queries that reference themselves, useful for hierarchical data,
//! graph traversal, and series generation.

use vibesql_ast::Statement;
use vibesql_parser::Parser;

use super::super::*;

fn execute_sql(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    let stmt = Parser::parse_sql(sql).map_err(|e| ExecutorError::ParseError(format!("{:?}", e)))?;
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)?;
            Ok(vec![])
        }
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt)?;
            Ok(vec![])
        }
        Statement::Select(select_stmt) => {
            let result = SelectExecutor::new(db).execute(&select_stmt)?;
            Ok(result)
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Unsupported statement type: {:?}",
            stmt
        ))),
    }
}

/// Test 1: Basic UNION vs UNION ALL behavior in recursive CTEs
///
/// UNION should deduplicate rows, UNION ALL should preserve all rows.
#[test]
fn test_recursive_cte_union_vs_union_all() {
    let mut db = vibesql_storage::Database::new();

    // Test UNION: deduplicates
    let query_union = "
        WITH RECURSIVE r AS (
            SELECT 1 AS x
            UNION
            SELECT 1 FROM r WHERE x < 3
        )
        SELECT * FROM r
    ";
    let result_union = execute_sql(&mut db, query_union).unwrap();
    // UNION should deduplicate: only return 1 once
    assert_eq!(result_union.len(), 1, "UNION should deduplicate duplicate value 1");
    assert_eq!(result_union[0].values[0], vibesql_types::SqlValue::Integer(1));

    // Test UNION ALL: preserves all rows (use counter to limit iterations)
    let query_union_all = "
        WITH RECURSIVE r AS (
            SELECT 1 AS x, 0 AS counter
            UNION ALL
            SELECT 1, counter + 1 FROM r WHERE counter < 2
        )
        SELECT x FROM r
    ";
    let result_union_all = execute_sql(&mut db, query_union_all).unwrap();
    // UNION ALL should NOT deduplicate: return all 3 rows (counter 0, 1, 2)
    assert_eq!(result_union_all.len(), 3, "UNION ALL should preserve all duplicates");
    assert_eq!(result_union_all[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result_union_all[1].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result_union_all[2].values[0], vibesql_types::SqlValue::Integer(1));
}

/// Test 2: Deduplication behavior in recursive CTEs with UNION
///
/// Verifies that UNION eliminates duplicate rows during iteration.
/// Example: query that would produce duplicates without deduplication.
#[test]
fn test_recursive_cte_union_deduplication() {
    let mut db = vibesql_storage::Database::new();

    // Create a tree structure where multiple paths lead to the same node
    execute_sql(&mut db, "CREATE TABLE nodes(id INT, parent_id INT)").unwrap();

    // Tree structure:
    //   1
    //  / \
    // 2   3
    //  \ /
    //   4
    // Node 4 is reachable from both node 2 and node 3
    execute_sql(&mut db, "INSERT INTO nodes VALUES (1, NULL)").unwrap();
    execute_sql(&mut db, "INSERT INTO nodes VALUES (2, 1)").unwrap();
    execute_sql(&mut db, "INSERT INTO nodes VALUES (3, 1)").unwrap();
    execute_sql(&mut db, "INSERT INTO nodes VALUES (4, 2)").unwrap();
    execute_sql(&mut db, "INSERT INTO nodes VALUES (4, 3)").unwrap(); // Duplicate edge

    // Traverse from root using UNION (should deduplicate node 4)
    let query = "
        WITH RECURSIVE traverse AS (
            SELECT id FROM nodes WHERE parent_id IS NULL
            UNION
            SELECT n.id FROM nodes n, traverse t WHERE n.parent_id = t.id
        )
        SELECT id FROM traverse ORDER BY id
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Should return nodes: 1, 2, 3, 4 (deduplicated)
    assert_eq!(result.len(), 4, "UNION should deduplicate node 4 reached via multiple paths");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Integer(3));
    assert_eq!(result[3].values[0], vibesql_types::SqlValue::Integer(4));
}

/// Test 3: Multi-column deduplication in recursive CTEs
///
/// Verifies deduplication works correctly with multiple columns.
/// Row (1,2) should be considered equal to another (1,2).
#[test]
fn test_recursive_cte_union_multi_column() {
    let mut db = vibesql_storage::Database::new();

    // Test multi-column deduplication
    let query = "
        WITH RECURSIVE pairs AS (
            SELECT 1 AS a, 2 AS b
            UNION
            SELECT 1, 2 FROM pairs WHERE a < 3
        )
        SELECT * FROM pairs
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Should deduplicate (1, 2) across all iterations
    assert_eq!(result.len(), 1, "UNION should deduplicate multi-column row (1, 2)");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(2));
}

/// Test 4: Early termination when all new rows are duplicates
///
/// Verifies iteration stops when UNION filters out all new rows as duplicates.
/// Should not hit max recursion depth.
#[test]
fn test_recursive_cte_union_early_termination() {
    let mut db = vibesql_storage::Database::new();

    // This query would run forever with UNION ALL, but UNION should terminate early
    let query = "
        WITH RECURSIVE counter AS (
            SELECT 1 AS n
            UNION
            SELECT 1 FROM counter WHERE n < 1000000
        )
        SELECT * FROM counter
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Should terminate immediately because 1 is always a duplicate
    assert_eq!(result.len(), 1, "UNION should terminate early when all new rows are duplicates");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
}

/// Test 5: NULL handling in recursive CTE deduplication
///
/// Verifies NULL values are handled correctly in deduplication.
/// NULL should equal NULL for deduplication purposes (standard set semantics).
#[test]
fn test_recursive_cte_union_null_handling() {
    let mut db = vibesql_storage::Database::new();

    // Test NULL deduplication
    let query = "
        WITH RECURSIVE nulls AS (
            SELECT NULL AS x, 1 AS y
            UNION
            SELECT NULL, 1 FROM nulls WHERE y < 3
        )
        SELECT * FROM nulls
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Should deduplicate (NULL, 1) across iterations
    assert_eq!(result.len(), 1, "UNION should treat NULL = NULL for deduplication");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Null);
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(1));
}

/// Test 6a: Error case - INTERSECT is rejected in recursive CTEs
#[test]
fn test_recursive_cte_rejects_intersect() {
    let mut db = vibesql_storage::Database::new();

    let query = "
        WITH RECURSIVE r AS (
            SELECT 1 AS x
            INTERSECT
            SELECT x+1 FROM r WHERE x < 5
        )
        SELECT * FROM r
    ";

    let result = execute_sql(&mut db, query);

    assert!(result.is_err(), "Expected error for INTERSECT in recursive CTE");
    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("must use UNION or UNION ALL (not INTERSECT or EXCEPT)"),
        "Expected clear error message about unsupported INTERSECT, got: {}",
        err_msg
    );
}

/// Test 6b: Error case - EXCEPT is rejected in recursive CTEs
#[test]
fn test_recursive_cte_rejects_except() {
    let mut db = vibesql_storage::Database::new();

    let query = "
        WITH RECURSIVE r AS (
            SELECT 1 AS x
            EXCEPT
            SELECT x+1 FROM r WHERE x < 5
        )
        SELECT * FROM r
    ";

    let result = execute_sql(&mut db, query);

    assert!(result.is_err(), "Expected error for EXCEPT in recursive CTE");
    let err = result.unwrap_err();
    let err_msg = format!("{:?}", err);
    assert!(
        err_msg.contains("must use UNION or UNION ALL (not INTERSECT or EXCEPT)"),
        "Expected clear error message about unsupported EXCEPT, got: {}",
        err_msg
    );
}

/// Test 7: Example from issue #4483
///
/// The exact example from the original issue that motivated UNION support.
/// Generates a sequence from 1 to 5 using UNION.
#[test]
fn test_recursive_cte_union_issue_4483() {
    let mut db = vibesql_storage::Database::new();

    // The exact query from issue #4483
    let query = "
        WITH RECURSIVE r AS (
            SELECT 1 AS x
            UNION
            SELECT x+1 FROM r WHERE x < 5
        )
        SELECT * FROM r
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Expected: 1, 2, 3, 4, 5
    assert_eq!(result.len(), 5, "Should generate sequence 1-5");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Integer(3));
    assert_eq!(result[3].values[0], vibesql_types::SqlValue::Integer(4));
    assert_eq!(result[4].values[0], vibesql_types::SqlValue::Integer(5));
}

/// Test 8: Hierarchical traversal with UNION
///
/// Tests a realistic use case: traversing an organizational hierarchy.
#[test]
fn test_recursive_cte_union_hierarchy() {
    let mut db = vibesql_storage::Database::new();

    execute_sql(&mut db, "CREATE TABLE employees(id INT, name TEXT, manager_id INT)").unwrap();

    // Org hierarchy:
    //   CEO (1)
    //   ├── VP1 (2)
    //   │   └── Mgr1 (4)
    //   └── VP2 (3)
    //       └── Mgr2 (5)
    execute_sql(&mut db, "INSERT INTO employees VALUES (1, 'CEO', NULL)").unwrap();
    execute_sql(&mut db, "INSERT INTO employees VALUES (2, 'VP1', 1)").unwrap();
    execute_sql(&mut db, "INSERT INTO employees VALUES (3, 'VP2', 1)").unwrap();
    execute_sql(&mut db, "INSERT INTO employees VALUES (4, 'Mgr1', 2)").unwrap();
    execute_sql(&mut db, "INSERT INTO employees VALUES (5, 'Mgr2', 3)").unwrap();

    // Find all employees reporting to CEO (directly or indirectly)
    let query = "
        WITH RECURSIVE subordinates AS (
            SELECT id, name, manager_id
            FROM employees
            WHERE id = 1
            UNION
            SELECT e.id, e.name, e.manager_id
            FROM employees e, subordinates s
            WHERE e.manager_id = s.id
        )
        SELECT id FROM subordinates ORDER BY id
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Should return all 5 employees (CEO + all subordinates)
    assert_eq!(result.len(), 5, "Should find CEO and all subordinates");
    for i in 0..5 {
        assert_eq!(result[i].values[0], vibesql_types::SqlValue::Integer((i + 1) as i64));
    }
}

/// Test 9: Performance - large result set with UNION
///
/// Verifies that UNION can handle larger result sets without excessive memory use.
#[test]
fn test_recursive_cte_union_large_result() {
    let mut db = vibesql_storage::Database::new();

    // Generate sequence 1 to 100
    let query = "
        WITH RECURSIVE seq AS (
            SELECT 1 AS n
            UNION
            SELECT n+1 FROM seq WHERE n < 100
        )
        SELECT COUNT(*) FROM seq
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Should generate 100 rows
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(100));
}

/// Test 10: Mixed data types in multi-column UNION
///
/// Verifies deduplication works with different data types (INT, TEXT, NULL).
#[test]
fn test_recursive_cte_union_mixed_types() {
    let mut db = vibesql_storage::Database::new();

    let query = "
        WITH RECURSIVE mixed AS (
            SELECT 1 AS n, 'hello' AS s, NULL AS x
            UNION
            SELECT 1, 'hello', NULL FROM mixed WHERE n < 3
        )
        SELECT * FROM mixed
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Should deduplicate the row (1, 'hello', NULL)
    assert_eq!(result.len(), 1, "UNION should deduplicate mixed-type rows");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(
        result[0].values[1],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello"))
    );
    assert_eq!(result[0].values[2], vibesql_types::SqlValue::Null);
}

/// Test 11: UNION with complex expressions
///
/// Tests that UNION deduplication works when recursive term has computed values.
#[test]
fn test_recursive_cte_union_computed_values() {
    let mut db = vibesql_storage::Database::new();

    // Fibonacci-like sequence that would produce duplicates without proper deduplication
    let query = "
        WITH RECURSIVE fib AS (
            SELECT 1 AS a, 1 AS b
            UNION
            SELECT b, a+b FROM fib WHERE a < 10
        )
        SELECT a, b FROM fib ORDER BY a, b
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Should generate Fibonacci pairs without duplicates
    // (1,1), (1,2), (2,3), (3,5), (5,8), (8,13)
    assert!(result.len() > 0, "Should generate Fibonacci sequence");

    // Verify first pair
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], vibesql_types::SqlValue::Integer(1));

    // Verify no duplicates (each row should be unique)
    let mut seen = std::collections::HashSet::new();
    for row in &result {
        let key = (row.values[0].clone(), row.values[1].clone());
        assert!(seen.insert(key), "Found duplicate row in UNION result");
    }
}

/// Test 12: Graph cycle detection with UNION
///
/// Tests UNION's ability to prevent infinite loops in cyclic graphs.
#[test]
fn test_recursive_cte_union_cycle_detection() {
    let mut db = vibesql_storage::Database::new();

    execute_sql(&mut db, "CREATE TABLE graph(from_node INT, to_node INT)").unwrap();

    // Create a cycle: 1 -> 2 -> 3 -> 1
    execute_sql(&mut db, "INSERT INTO graph VALUES (1, 2)").unwrap();
    execute_sql(&mut db, "INSERT INTO graph VALUES (2, 3)").unwrap();
    execute_sql(&mut db, "INSERT INTO graph VALUES (3, 1)").unwrap();

    // Traverse the graph starting from node 1
    // UNION should prevent infinite loop by deduplicating visited nodes
    let query = "
        WITH RECURSIVE reachable AS (
            SELECT 1 AS node
            UNION
            SELECT g.to_node
            FROM graph g, reachable r
            WHERE g.from_node = r.node
        )
        SELECT node FROM reachable ORDER BY node
    ";

    let result = execute_sql(&mut db, query).unwrap();

    // Should visit each node exactly once despite cycle
    assert_eq!(result.len(), 3, "UNION should prevent infinite loop in cyclic graph");
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(1));
    assert_eq!(result[1].values[0], vibesql_types::SqlValue::Integer(2));
    assert_eq!(result[2].values[0], vibesql_types::SqlValue::Integer(3));
}

// ===========================================================================
// Issue #5941 — Gap 2 (lazy recursion under LIMIT) and Gap 3 (ORDER BY in the
// recursive term = priority-queue traversal). Cases mirror with1.test 5.x,
// 10.x, and 11.x.
// ===========================================================================

/// Collect the i64 values of a single-column result in row order.
fn ints(rows: &[vibesql_storage::Row]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r.values[0] {
            vibesql_types::SqlValue::Integer(n) => n,
            ref other => panic!("expected integer, got {:?}", other),
        })
        .collect()
}

/// Collect the string values of a single-column result in row order.
fn strs(rows: &[vibesql_storage::Row]) -> Vec<String> {
    rows.iter()
        .map(|r| match &r.values[0] {
            vibesql_types::SqlValue::Varchar(s) => s.to_string(),
            other => panic!("expected varchar, got {:?}", other),
        })
        .collect()
}

/// with1.test 5.1 — an infinite recursive CTE under an outer LIMIT returns
/// exactly the requested rows instead of running to the recursion cap.
#[test]
fn test_recursive_cte_infinite_outer_limit() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH i(x) AS ( VALUES(1) UNION ALL SELECT x+1 FROM i) SELECT x FROM i LIMIT 10",
    )
    .expect("infinite CTE with outer LIMIT must terminate");
    assert_eq!(ints(&rows), vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
}

/// with1.test 5.4 — a circular UNION ALL sequence under an outer LIMIT returns
/// the requested number of rows (the sequence never converges, so only the
/// outer LIMIT can stop it).
#[test]
fn test_recursive_cte_circular_outer_limit() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH i(x) AS ( VALUES(1) UNION ALL SELECT (x+1)%10 FROM i) SELECT x FROM i LIMIT 20",
    )
    .expect("circular CTE with outer LIMIT must terminate");
    assert_eq!(ints(&rows), vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0]);
}

/// with1.test 5.3 — a LIMIT after the UNION ALL caps the TOTAL CTE result
/// (base row included).
#[test]
fn test_recursive_cte_limit_on_recursive_term() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH i(x) AS ( VALUES(1) UNION ALL SELECT x+1 FROM i LIMIT 5) SELECT x FROM i",
    )
    .expect("recursive-term LIMIT must cap the total result");
    assert_eq!(ints(&rows), vec![1, 2, 3, 4, 5]);
}

/// Edge case from the issue test plan: `LIMIT 0` after the UNION ALL returns no
/// rows at all — not even the base row.
#[test]
fn test_recursive_cte_limit_zero_on_recursive_term() {
    let mut db = vibesql_storage::Database::new();
    let rows = execute_sql(
        &mut db,
        "WITH i(x) AS ( VALUES(1) UNION ALL SELECT x+1 FROM i LIMIT 0) SELECT x FROM i",
    )
    .expect("LIMIT 0 must yield an empty result");
    assert!(rows.is_empty(), "LIMIT 0 should drop even the base row, got {:?}", ints(&rows));
}

/// with1.test 5.2.2 — ORDER BY on a UNION ALL recursive term produces a global
/// priority-queue traversal: rows are emitted in sorted order across all
/// recursion levels.
#[test]
fn test_recursive_cte_order_by_priority_queue() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE edge(xfrom, xto, seq)").unwrap();
    for (f, t, s) in [
        (0, 1, 10),
        (1, 2, 20),
        (0, 3, 30),
        (2, 4, 40),
        (3, 4, 40),
        (2, 5, 50),
        (3, 6, 60),
        (5, 7, 70),
        (3, 7, 70),
        (4, 8, 80),
        (7, 8, 80),
        (8, 9, 90),
    ] {
        execute_sql(&mut db, &format!("INSERT INTO edge VALUES({f},{t},{s})")).unwrap();
    }
    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE ancest(id, mtime) AS (
             VALUES(0, 0)
             UNION ALL
             SELECT edge.xto, edge.seq FROM edge, ancest
              WHERE edge.xfrom=ancest.id
              ORDER BY 2
         )
         SELECT id FROM ancest",
    )
    .unwrap();
    // Globally sorted by mtime (seq), FIFO tie-break among equal keys.
    assert_eq!(ints(&rows), vec![0, 1, 2, 3, 4, 4, 5, 6, 7, 7, 8, 8, 8, 8, 9, 9, 9, 9]);
}

/// with1.test 5.2.3 — ORDER BY + LIMIT + OFFSET after the UNION ALL: the
/// LIMIT/OFFSET window the sorted extraction sequence.
#[test]
fn test_recursive_cte_order_by_limit_offset() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE edge(xfrom, xto, seq)").unwrap();
    for (f, t, s) in [
        (0, 1, 10),
        (1, 2, 20),
        (0, 3, 30),
        (2, 4, 40),
        (3, 4, 40),
        (2, 5, 50),
        (3, 6, 60),
        (5, 7, 70),
        (3, 7, 70),
        (4, 8, 80),
        (7, 8, 80),
        (8, 9, 90),
    ] {
        execute_sql(&mut db, &format!("INSERT INTO edge VALUES({f},{t},{s})")).unwrap();
    }
    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE ancest(id, mtime) AS (
             VALUES(0, 0)
             UNION ALL
             SELECT edge.xto, edge.seq FROM edge, ancest
              WHERE edge.xfrom=ancest.id
              ORDER BY 2 LIMIT 4 OFFSET 2
         )
         SELECT id FROM ancest",
    )
    .unwrap();
    assert_eq!(ints(&rows), vec![2, 3, 4, 4]);
}

/// with1.test 11.1 / 11.2 — the org-chart example: `ORDER BY level` yields
/// breadth-first, `ORDER BY level DESC` yields depth-first. Also exercises
/// `level` as a CTE column-list identifier (parser fix).
#[test]
fn test_recursive_cte_bfs_vs_dfs_org_chart() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE org(name TEXT PRIMARY KEY, boss TEXT)").unwrap();
    for (name, boss) in [
        ("Alice", "NULL"),
        ("Bob", "'Alice'"),
        ("Cindy", "'Alice'"),
        ("Dave", "'Bob'"),
        ("Emma", "'Bob'"),
        ("Fred", "'Cindy'"),
        ("Gail", "'Cindy'"),
        ("Harry", "'Dave'"),
        ("Ingrid", "'Dave'"),
        ("Jim", "'Emma'"),
        ("Kate", "'Emma'"),
    ] {
        execute_sql(&mut db, &format!("INSERT INTO org VALUES('{name}', {boss})")).unwrap();
    }

    // 11.1: breadth-first (ORDER BY level ASC).
    let bfs = execute_sql(
        &mut db,
        "WITH RECURSIVE under_alice(name,level) AS (
             VALUES('Alice','0')
             UNION ALL
             SELECT org.name, under_alice.level+1
               FROM org, under_alice
              WHERE org.boss=under_alice.name
              ORDER BY 2
         )
         SELECT name FROM under_alice",
    )
    .unwrap();
    assert_eq!(
        strs(&bfs),
        vec![
            "Alice", "Bob", "Cindy", "Dave", "Emma", "Fred", "Gail", "Harry", "Ingrid", "Jim",
            "Kate"
        ]
    );

    // 11.2: depth-first (ORDER BY level DESC).
    let dfs = execute_sql(
        &mut db,
        "WITH RECURSIVE under_alice(name,level) AS (
             VALUES('Alice','0')
             UNION ALL
             SELECT org.name, under_alice.level+1
               FROM org, under_alice
              WHERE org.boss=under_alice.name
              ORDER BY 2 DESC
         )
         SELECT name FROM under_alice",
    )
    .unwrap();
    assert_eq!(
        strs(&dfs),
        vec![
            "Alice", "Bob", "Dave", "Harry", "Ingrid", "Emma", "Jim", "Kate", "Cindy", "Fred",
            "Gail"
        ]
    );
}

/// with1.test 11.3 — without ORDER BY, the recursive query uses a FIFO, giving
/// a breadth-first search.
#[test]
fn test_recursive_cte_fifo_is_breadth_first() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE org(name TEXT PRIMARY KEY, boss TEXT)").unwrap();
    for (name, boss) in [
        ("Alice", "NULL"),
        ("Bob", "'Alice'"),
        ("Cindy", "'Alice'"),
        ("Dave", "'Bob'"),
        ("Emma", "'Bob'"),
        ("Fred", "'Cindy'"),
    ] {
        execute_sql(&mut db, &format!("INSERT INTO org VALUES('{name}', {boss})")).unwrap();
    }
    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE under_alice(name,level) AS (
             VALUES('Alice','0')
             UNION ALL
             SELECT org.name, under_alice.level+1
               FROM org, under_alice
              WHERE org.boss=under_alice.name
         )
         SELECT name FROM under_alice",
    )
    .unwrap();
    assert_eq!(strs(&rows), vec!["Alice", "Bob", "Cindy", "Dave", "Emma", "Fred"]);
}

/// with1.test 10.7.1 — an ORDER BY term that matches no output column of the
/// recursive query is an error.
#[test]
fn test_recursive_cte_order_by_unknown_column_errors() {
    let mut db = vibesql_storage::Database::new();
    let err = execute_sql(
        &mut db,
        "WITH t(a) AS (
             SELECT 1 AS b UNION ALL SELECT a+1 AS c FROM t WHERE a<5 ORDER BY a
         )
         SELECT * FROM t",
    )
    .unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("does not match any column"),
        "expected ORDER BY resolution error, got: {msg}"
    );
}

/// with1.test 10.7.2 / 10.7.3 — an ORDER BY term may match a SELECT-list alias
/// of either the base term (`b`) or the recursive term (`c`).
#[test]
fn test_recursive_cte_order_by_matches_select_alias() {
    let mut db = vibesql_storage::Database::new();
    let by_base = execute_sql(
        &mut db,
        "WITH t(a) AS (
             SELECT 1 AS b UNION ALL SELECT a+1 AS c FROM t WHERE a<5 ORDER BY b
         )
         SELECT * FROM t",
    )
    .unwrap();
    assert_eq!(ints(&by_base), vec![1, 2, 3, 4, 5]);

    let by_recursive = execute_sql(
        &mut db,
        "WITH t(a) AS (
             SELECT 1 AS b UNION ALL SELECT a+1 AS c FROM t WHERE a<5 ORDER BY c
         )
         SELECT * FROM t",
    )
    .unwrap();
    assert_eq!(ints(&by_recursive), vec![1, 2, 3, 4, 5]);
}

/// with1.test 10.8.1 — an explicit `COLLATE nocase` on a recursive CTE's
/// `ORDER BY` term must be applied by the priority-queue traversal, not
/// silently dropped in favor of a binary comparison.
///
/// Base rows are `(1, 'a')` and `(2, 'B')`. Binary/default ordering sorts
/// `'B'` (0x42) before `'a'` (0x61); `COLLATE nocase` instead sorts `'a'`
/// before `'B'` (case-insensitive alphabetical). The children (`'x'` under
/// `'a'`, `'y'` under `'B'`) follow their parent through the priority queue,
/// so the full emission order distinguishes the two collations unambiguously.
#[test]
fn test_recursive_cte_order_by_explicit_collate_nocase() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE node(id INTEGER, parent INTEGER, name TEXT)").unwrap();
    for (id, parent, name) in
        [(1, "NULL", "'a'"), (2, "NULL", "'B'"), (3, "1", "'x'"), (4, "2", "'y'")]
    {
        execute_sql(&mut db, &format!("INSERT INTO node VALUES({id}, {parent}, {name})")).unwrap();
    }

    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE flat(id, name) AS (
             SELECT id, name FROM node WHERE parent IS NULL
             UNION ALL
             SELECT node.id, node.name FROM node, flat WHERE node.parent = flat.id
             ORDER BY 2 COLLATE nocase
         )
         SELECT name FROM flat",
    )
    .unwrap();
    assert_eq!(
        strs(&rows),
        vec!["a", "B", "x", "y"],
        "COLLATE nocase must order 'a' before 'B' (case-insensitive), not binary order"
    );

    // Sanity check: without the COLLATE override, the same query sorts by
    // binary order at both levels ('B' before 'a' for the roots, 'x' before
    // 'y' for the children — 'x' is 0x78 and 'y' is 0x79), confirming the
    // collation is what flips the result rather than some other
    // traversal-order artifact.
    let rows_binary = execute_sql(
        &mut db,
        "WITH RECURSIVE flat(id, name) AS (
             SELECT id, name FROM node WHERE parent IS NULL
             UNION ALL
             SELECT node.id, node.name FROM node, flat WHERE node.parent = flat.id
             ORDER BY 2
         )
         SELECT name FROM flat",
    )
    .unwrap();
    assert_eq!(strs(&rows_binary), vec!["B", "a", "x", "y"]);
}

/// with1.test 10.8.2 — a `COLLATE nocase` declared on the *base (seed) term's*
/// select-list expression must apply to the recursive CTE's `ORDER BY`, even
/// though the `ORDER BY` term itself has no `COLLATE` and the recursive
/// term's own corresponding expression has none either.
///
/// Mirrors SQLite's `multiSelectCollSeq`: search the compound's arms
/// left-to-right (base term first, then recursive term) for the first
/// explicit `COLLATE` on that output column.
#[test]
fn test_recursive_cte_order_by_collate_declared_on_base_term() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE node(id INTEGER, parent INTEGER, name TEXT)").unwrap();
    for (id, parent, name) in
        [(1, "NULL", "'a'"), (2, "NULL", "'B'"), (3, "1", "'x'"), (4, "2", "'y'")]
    {
        execute_sql(&mut db, &format!("INSERT INTO node VALUES({id}, {parent}, {name})")).unwrap();
    }

    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE flat(id, name) AS (
             SELECT id, name COLLATE nocase FROM node WHERE parent IS NULL
             UNION ALL
             SELECT node.id, node.name FROM node, flat WHERE node.parent = flat.id
             ORDER BY 2
         )
         SELECT name FROM flat",
    )
    .unwrap();
    assert_eq!(
        strs(&rows),
        vec!["a", "B", "x", "y"],
        "the seed term's declared COLLATE nocase must govern ORDER BY even without an explicit \
         COLLATE on the ORDER BY term itself"
    );
}

/// with1.test 10.8.3 — the mirror image of 10.8.2: `COLLATE nocase` declared
/// on the *recursive term's* select-list expression (base term has none)
/// still governs a bare `ORDER BY` with no explicit `COLLATE`.
#[test]
fn test_recursive_cte_order_by_collate_declared_on_recursive_term() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE node(id INTEGER, parent INTEGER, name TEXT)").unwrap();
    for (id, parent, name) in
        [(1, "NULL", "'a'"), (2, "NULL", "'B'"), (3, "1", "'x'"), (4, "2", "'y'")]
    {
        execute_sql(&mut db, &format!("INSERT INTO node VALUES({id}, {parent}, {name})")).unwrap();
    }

    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE flat(id, name) AS (
             SELECT id, name FROM node WHERE parent IS NULL
             UNION ALL
             SELECT node.id, node.name COLLATE nocase FROM node, flat WHERE node.parent = flat.id
             ORDER BY 2
         )
         SELECT name FROM flat",
    )
    .unwrap();
    assert_eq!(
        strs(&rows),
        vec!["a", "B", "x", "y"],
        "the recursive term's declared COLLATE nocase must govern ORDER BY even without an \
         explicit COLLATE on the ORDER BY term itself"
    );
}

/// Circular-reference detection and SQLite-compatible error messages.
///
/// Mirrors with2.test 3.1-3.4 and with1.test 3.1: mutual and self circular
/// references between CTEs must report `circular reference: <name>`, naming the
/// CTE closest to the query that reads the cycle (issue #6189).
#[test]
fn test_cte_circular_reference_errors() {
    let mut db = vibesql_storage::Database::new();

    // Self-reference in a non-recursive body (with2.test 3.1).
    let err =
        execute_sql(&mut db, "WITH i(x, y) AS ( VALUES(1, (SELECT x FROM i)) ) SELECT * FROM i")
            .unwrap_err();
    assert_eq!(err.to_string(), "circular reference: i", "self-ref non-recursive CTE");

    // Mutual cycle i -> j -> k -> i, queried via i (with2.test 3.2).
    let err = execute_sql(
        &mut db,
        "WITH i(x) AS ( SELECT * FROM j ), \
              j(x) AS ( SELECT * FROM k ), \
              k(x) AS ( SELECT * FROM i ) \
         SELECT * FROM i",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "circular reference: i", "3-CTE cycle named by entry i");

    // Same two-CTE cycle read from j must be named by j (with2.test 3.4): the
    // reported name follows the query's entry point, not declaration order.
    let err = execute_sql(
        &mut db,
        "WITH i(x) AS ( SELECT * FROM (SELECT * FROM j) ), \
              j(x) AS ( SELECT * FROM (SELECT * FROM i) ) \
         SELECT * FROM j",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "circular reference: j", "cycle named by query entry j");

    // Cycle whose members are declared in the reverse of reference order is
    // still named by the entry the root reads (with1.test 3.1).
    let err = execute_sql(
        &mut db,
        "WITH tmp2(x) AS ( SELECT * FROM tmp1 ), \
              tmp1(a) AS ( SELECT * FROM tmp2 ) \
         SELECT * FROM tmp1",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "circular reference: tmp1", "cycle named by root entry tmp1");
}

/// Two CTEs sharing a name in one WITH clause are rejected up front with
/// SQLite's `duplicate WITH table name: <name>` wording (issue #6189,
/// with1.test 3.2). A name redefined by a *nested* WITH is a separate scope and
/// must still be accepted (with1.test 3.4).
#[test]
fn test_duplicate_with_table_name_errors() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a)").unwrap();

    let err = execute_sql(
        &mut db,
        "WITH tmp(a) AS (SELECT * FROM t1), tmp(a) AS (SELECT * FROM t1) SELECT * FROM tmp",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "duplicate WITH table name: tmp");

    // A nested WITH may reuse an outer CTE name (different scope): no error.
    execute_sql(&mut db, "CREATE TABLE t3(x)").unwrap();
    execute_sql(&mut db, "CREATE TABLE t4(x)").unwrap();
    execute_sql(&mut db, "INSERT INTO t3 VALUES('T3')").unwrap();
    execute_sql(&mut db, "INSERT INTO t4 VALUES('T4')").unwrap();
    let rows = execute_sql(
        &mut db,
        "WITH tmp AS ( SELECT * FROM t3 ), \
              tmp2 AS ( WITH tmp AS ( SELECT * FROM t4 ) SELECT * FROM tmp ) \
         SELECT * FROM tmp2",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("T4")));
}

/// A recursive term must reference the CTE exactly once, and only as a base
/// table in FROM. Two malformed shapes are rejected before any rows are
/// produced (issue #6189, with1.test 7.4/7.5):
///   - the only self-reference is buried in a subquery -> "circular reference";
///   - a FROM reference plus a subquery reference -> "multiple recursive
///     references".
#[test]
fn test_recursive_term_reference_shape_errors() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE tree(i, p)").unwrap();
    execute_sql(&mut db, "INSERT INTO tree VALUES(1, NULL), (2, 1), (3, 1), (4, 2), (5, 4)")
        .unwrap();

    // Self-reference only inside a WHERE subquery, none in FROM (with1.test 7.4).
    let err = execute_sql(
        &mut db,
        "WITH t(id) AS ( \
            VALUES(2) \
            UNION ALL \
            SELECT i FROM tree WHERE p IN (SELECT id FROM t) \
         ) SELECT id FROM t",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "circular reference: t");

    // Self-reference in FROM *and* in a WHERE subquery (with1.test 7.5).
    let err = execute_sql(
        &mut db,
        "WITH t(id) AS ( \
            VALUES(2) \
            UNION ALL \
            SELECT i FROM tree, t WHERE p = id AND p IN (SELECT id FROM t) \
         ) SELECT id FROM t",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "multiple recursive references: t");

    // Sanity: the well-formed single FROM self-reference still runs (7.3).
    let rows = execute_sql(
        &mut db,
        "WITH t(id) AS ( VALUES(2) UNION ALL SELECT i FROM tree, t WHERE p = id ) \
         SELECT id FROM t ORDER BY id",
    )
    .unwrap();
    let ids: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(
        ids,
        vec![
            vibesql_types::SqlValue::Integer(2),
            vibesql_types::SqlValue::Integer(4),
            vibesql_types::SqlValue::Integer(5),
        ]
    );
}

/// CTE arity validation uses SQLite's `table X has N values for M columns`
/// wording and fires before the UNION-consistency check (issue #6189,
/// with1.test 5.6.x, with3.test 6.0/6.1).
#[test]
fn test_cte_column_count_mismatch_messages() {
    let mut db = vibesql_storage::Database::new();

    // Declared columns exceed base-term values (with1.test 5.6.1).
    let err = execute_sql(&mut db, "WITH i(x, y) AS ( VALUES(1) ) SELECT * FROM i").unwrap_err();
    assert_eq!(err.to_string(), "table i has 1 values for 2 columns");

    // Base term produces more values than declared (with1.test 5.6.2).
    let err = execute_sql(&mut db, "WITH i(x) AS ( VALUES(1,2) ) SELECT * FROM i").unwrap_err();
    assert_eq!(err.to_string(), "table i has 2 values for 1 columns");

    // Arity check precedes the UNION-term consistency check (with1.test 5.6.4).
    let err =
        execute_sql(&mut db, "WITH i(x) AS ( SELECT 1, 2 UNION ALL SELECT 1 ) SELECT * FROM i")
            .unwrap_err();
    assert_eq!(err.to_string(), "table i has 2 values for 1 columns");

    // Empty-body case is still validated statically (with1.test 5.6.3).
    execute_sql(&mut db, "CREATE TABLE t5(a, b)").unwrap();
    let err =
        execute_sql(&mut db, "WITH i(x) AS ( SELECT * FROM t5 ) SELECT * FROM i").unwrap_err();
    assert_eq!(err.to_string(), "table i has 2 values for 1 columns");

    // Recursive term arity mismatch keeps SQLite's verbatim wording with no
    // "Unsupported feature:" prefix (with1.test 5.6.6).
    let err = execute_sql(
        &mut db,
        "WITH i(x) AS ( SELECT 1 UNION ALL SELECT x+1, x*2 FROM i ) SELECT * FROM i",
    )
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "SELECTs to the left and right of UNION ALL do not have the same number of result columns"
    );
}

/// An unreferenced CTE with a column-count mismatch is never evaluated, so it
/// raises no error even in DML statements (issue #6189, with1.test 1.2/1.4).
#[test]
fn test_unreferenced_cte_not_validated() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(x INTEGER, y INTEGER)").unwrap();

    // `x(a)` declares one column over a two-column table but is never read by
    // the INSERT: no arity error.
    execute_sql(&mut db, "WITH x(a) AS ( SELECT * FROM t1 ) INSERT INTO t1 VALUES(1, 2)")
        .expect("unreferenced CTE must not be validated");

    // Likewise as a no-op SELECT that does not reference the CTE.
    let rows = execute_sql(&mut db, "WITH x(a) AS ( SELECT * FROM t1 ) SELECT 10").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(10));
}

/// A recursive CTE whose non-recursive seed is itself a compound of several
/// leading terms (combined with any set operator) must partition into that
/// full seed plus the recursive term(s) — not misfire as a circular reference
/// (issue #6189, with5.test 113/114/131).
#[test]
fn test_recursive_cte_compound_seed() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE link(aa INT, bb INT)").unwrap();
    execute_sql(
        &mut db,
        "INSERT INTO link(aa,bb) VALUES \
         (1,3),(5,3),(7,1),(7,9),(9,9),(5,11),(11,7),(2,4),(4,6),(8,6)",
    )
    .unwrap();

    let ints = |rows: &[vibesql_storage::Row]| -> Vec<i64> {
        rows.iter()
            .map(|r| match &r.values[0] {
                vibesql_types::SqlValue::Integer(i) => *i,
                other => panic!("expected integer, got {:?}", other),
            })
            .collect()
    };

    // with5.test 113: `VALUES(1),(200),(300),(400) INTERSECT VALUES(1)` seed,
    // then two recursive UNION terms. The seed reduces to {1}.
    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE closure(x) AS ( \
            VALUES(1),(200),(300),(400) \
            INTERSECT \
            VALUES(1) \
            UNION \
            SELECT bb FROM closure, link WHERE link.aa=closure.x \
            UNION \
            SELECT aa FROM link, closure WHERE link.bb=closure.x \
         ) SELECT x FROM closure ORDER BY x",
    )
    .unwrap();
    assert_eq!(ints(&rows), vec![1, 3, 5, 7, 9, 11]);

    // with5.test 114: `VALUES(1),(200),(300),(400) UNION ALL VALUES(2)` seed
    // keeps every seed row, then two recursive UNION terms.
    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE closure(x) AS ( \
            VALUES(1),(200),(300),(400) \
            UNION ALL \
            VALUES(2) \
            UNION \
            SELECT bb FROM closure, link WHERE link.aa=closure.x \
            UNION \
            SELECT aa FROM link, closure WHERE link.bb=closure.x \
         ) SELECT x FROM closure ORDER BY x",
    )
    .unwrap();
    assert_eq!(ints(&rows), vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 200, 300, 400]);

    // with5.test 131: compound `SELECT..UNION ALL SELECT..` seed with an ordered
    // LIMIT over the whole recursion.
    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE closure(x) AS ( \
            SELECT 1 AS x \
            UNION ALL \
            SELECT 2 \
            UNION \
            SELECT aa FROM link JOIN closure ON bb=x \
            UNION \
            SELECT bb FROM link JOIN closure ON aa=x \
            ORDER BY x LIMIT 4 \
         ) SELECT * FROM closure",
    )
    .unwrap();
    assert_eq!(ints(&rows), vec![1, 2, 3, 4]);
}

/// When a recursive CTE mixes UNION and UNION ALL across its recursive
/// connectors, SQLite reports a circular reference (issue #6189, with5.test
/// 120/121). A uniform-operator compound seed must NOT trip this.
#[test]
fn test_recursive_cte_mixed_recursive_operators_circular() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE link(aa INT, bb INT)").unwrap();
    execute_sql(&mut db, "INSERT INTO link(aa,bb) VALUES (1,3),(5,3),(7,1)").unwrap();

    // with5.test 120: seed→R1 is UNION ALL but R1→R2 is UNION (mixed).
    let err = execute_sql(
        &mut db,
        "WITH RECURSIVE closure(x) AS ( \
            VALUES(1),(200) \
            UNION ALL \
            VALUES(2) \
            UNION ALL \
            SELECT bb FROM closure, link WHERE link.aa=closure.x \
            UNION \
            SELECT aa FROM link, closure WHERE link.bb=closure.x \
         ) SELECT x FROM closure ORDER BY x",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "circular reference: closure");

    // with5.test 121: seed→R1 is UNION but R1→R2 is UNION ALL (mixed).
    let err = execute_sql(
        &mut db,
        "WITH RECURSIVE closure(x) AS ( \
            VALUES(1),(200) \
            UNION ALL \
            VALUES(2) \
            UNION \
            SELECT bb FROM closure, link WHERE link.aa=closure.x \
            UNION ALL \
            SELECT aa FROM link, closure WHERE link.bb=closure.x \
         ) SELECT x FROM closure ORDER BY x",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "circular reference: closure");
}

/// SQLite distinguishes "multiple references to recursive table" (the name
/// appears more than once in FROM) from "multiple recursive references" (a
/// single FROM ref plus a subquery ref) (issue #6189, with2.test 1.16 vs
/// with1.test 7.5).
#[test]
fn test_recursive_cte_multiple_from_references_message() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE main_t4(x)").unwrap();

    // with2.test 1.16: three FROM references to the recursive table.
    let err = execute_sql(
        &mut db,
        "WITH t4(x) AS ( \
            VALUES(4) \
            UNION ALL \
            SELECT x+1 FROM t4, t4, t4 WHERE x<10 \
         ) SELECT * FROM t4",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "multiple references to recursive table: t4");
}

/// A CTE column-list arity mismatch must be detected even when the seed's
/// select list contains a wildcard with no FROM clause to expand it against.
/// SQLite treats such a wildcard as contributing exactly zero columns to the
/// arity comparison (never "unknown"), so the mismatch error takes priority
/// over the later "no tables specified" error that would otherwise fire once
/// the query actually tries to execute the wildcard (issue #6189,
/// with1.test 13.1/13.2/13.3, confirmed against sqlite3 3.51).
#[test]
fn test_cte_arity_check_with_unresolvable_wildcard_in_seed() {
    let mut db = vibesql_storage::Database::new();

    // Nominal width (1, from the literal `5`) mismatches the declared arity
    // (2) -> the mismatch error fires before the wildcard is ever expanded.
    let err = execute_sql(
        &mut db,
        "WITH RECURSIVE c(i,j) AS (SELECT 5,* UNION ALL SELECT i+1,11 FROM c WHERE i<10) \
         SELECT i FROM c",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "table c has 1 values for 2 columns");

    // Nominal width (1) matches the declared arity (1) -> no mismatch is
    // raised here, so the query falls through to the real "no tables
    // specified" error when it tries to execute the wildcard.
    let err = execute_sql(
        &mut db,
        "WITH RECURSIVE c(i) AS (SELECT 5,* UNION ALL SELECT i+1 FROM c WHERE i<10) \
         SELECT i FROM c",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "no tables specified");

    // A select list that is *entirely* wildcards (nominal width 0) never
    // triggers the mismatch check regardless of declared arity — SQLite
    // always reports "no tables specified" in that case.
    let err = execute_sql(
        &mut db,
        "WITH RECURSIVE c(i) AS (SELECT * UNION ALL SELECT i+1 FROM c WHERE i<10) \
         SELECT i FROM c",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "no tables specified");
}

/// SQLite rejects an aggregate function in the recursive term of a `WITH
/// RECURSIVE` CTE with a dedicated error: each recursive step only ever sees
/// the prior step's single working row, so aggregating "the recursive table"
/// is meaningless (issue #6189, with1.test 16.1).
#[test]
fn test_recursive_cte_rejects_aggregate_in_recursive_term() {
    let mut db = vibesql_storage::Database::new();

    let err = execute_sql(
        &mut db,
        "WITH RECURSIVE i(x) AS (VALUES(1) UNION SELECT count(*) FROM i) SELECT * FROM i",
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "recursive aggregate queries not supported");

    // The same aggregate in the *seed* (non-recursive) term is fine — only the
    // recursive term is restricted.
    let rows = execute_sql(
        &mut db,
        "WITH RECURSIVE i(x) AS (SELECT count(*) UNION ALL SELECT x+1 FROM i WHERE x<3) \
         SELECT x FROM i",
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
}
