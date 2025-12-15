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
