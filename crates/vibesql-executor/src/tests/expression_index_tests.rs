//! Integration tests for expression index query planning (Phase 4)
//!
//! These tests verify that the query planner correctly detects and uses
//! expression indexes (functional indexes) for query optimization.
//!
//! Expression indexes are created on expressions like `lower(email)` rather
//! than just column names. The planner should match WHERE clause expressions
//! to indexed expressions using structural comparison.

use vibesql_ast::{ColumnIdentifier, Expression, IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::select::{
    scan::index_scan::selection::{expression_filters_index_expression, index_column_can_filter},
    SelectExecutor,
};

/// Create a test database with users table
fn create_test_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create users table
    let users_schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("age".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(users_schema).unwrap();

    // Insert test data with mixed case emails
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("ALICE@example.com")),
            SqlValue::Integer(25),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob@Example.COM")),
            SqlValue::Integer(30),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(arcstr::ArcStr::from("charlie@example.com")),
            SqlValue::Integer(25),
            SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
        ]),
    )
    .unwrap();

    db
}

/// Helper to parse an expression from SQL WHERE clause
fn parse_where_expression(sql_predicate: &str) -> Expression {
    let full_sql = format!("SELECT * FROM t WHERE {}", sql_predicate);
    let stmt = Parser::parse_sql(&full_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => select_stmt.where_clause.unwrap(),
        _ => panic!("Expected SELECT statement"),
    }
}

/// Helper to parse an expression from CREATE INDEX
fn parse_index_expression(index_expr: &str) -> Expression {
    let full_sql = format!("CREATE INDEX idx ON t({})", index_expr);
    let stmt = Parser::parse_sql(&full_sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateIndex(create_stmt) => {
            match &create_stmt.columns[0] {
                IndexColumn::Expression { expr, .. } => *expr.clone(),
                IndexColumn::Column { column_name, .. } => {
                    // Return a column reference for simple column indexes
                    Expression::ColumnRef(ColumnIdentifier::simple(column_name.as_str(), false))
                }
            }
        }
        _ => panic!("Expected CREATE INDEX statement"),
    }
}

#[test]
fn test_expression_index_matching_lower() {
    // Test that lower(email) = 'test' matches an index on lower(email)
    let where_expr = parse_where_expression("lower(email) = 'alice@example.com'");
    let index_expr = parse_index_expression("lower(email)");

    assert!(
        expression_filters_index_expression(&where_expr, &index_expr),
        "lower(email) = 'value' should match index on lower(email)"
    );
}

#[test]
fn test_expression_index_matching_upper() {
    // Test that upper(name) = 'TEST' matches an index on upper(name)
    let where_expr = parse_where_expression("upper(name) = 'ALICE'");
    let index_expr = parse_index_expression("upper(name)");

    assert!(
        expression_filters_index_expression(&where_expr, &index_expr),
        "upper(name) = 'value' should match index on upper(name)"
    );
}

#[test]
fn test_expression_index_matching_arithmetic() {
    // Test that (a + b) = 5 matches an index on (a + b)
    let where_expr = parse_where_expression("(age + id) = 30");
    let index_expr = parse_index_expression("(age + id)");

    assert!(
        expression_filters_index_expression(&where_expr, &index_expr),
        "(age + id) = value should match index on (age + id)"
    );
}

#[test]
fn test_expression_index_no_match_different_function() {
    // Test that lower(email) != upper(email)
    let where_expr = parse_where_expression("lower(email) = 'test'");
    let index_expr = parse_index_expression("upper(email)");

    assert!(
        !expression_filters_index_expression(&where_expr, &index_expr),
        "lower(email) should NOT match index on upper(email)"
    );
}

#[test]
fn test_expression_index_no_match_different_column() {
    // Test that lower(email) != lower(name)
    let where_expr = parse_where_expression("lower(email) = 'test'");
    let index_expr = parse_index_expression("lower(name)");

    assert!(
        !expression_filters_index_expression(&where_expr, &index_expr),
        "lower(email) should NOT match index on lower(name)"
    );
}

#[test]
fn test_expression_index_matching_with_and() {
    // Test that AND conditions are searched for matching expressions
    let where_expr = parse_where_expression("age > 20 AND lower(email) = 'test'");
    let index_expr = parse_index_expression("lower(email)");

    assert!(
        expression_filters_index_expression(&where_expr, &index_expr),
        "Expression in AND should be found"
    );
}

#[test]
fn test_index_column_can_filter_expression() {
    // Test the unified index_column_can_filter function with expression indexes
    let where_expr = parse_where_expression("lower(email) = 'alice@example.com'");
    let index_expr = parse_index_expression("lower(email)");

    let index_column =
        IndexColumn::Expression { expr: Box::new(index_expr), direction: OrderDirection::Asc };

    assert!(
        index_column_can_filter(&where_expr, &index_column),
        "index_column_can_filter should work with expression indexes"
    );
}

#[test]
fn test_index_column_can_filter_column() {
    // Test that index_column_can_filter still works with column indexes
    let where_expr = parse_where_expression("email = 'alice@example.com'");

    let index_column = IndexColumn::Column {
        column_name: "email".to_string(),
        direction: OrderDirection::Asc,
        prefix_length: None,
        collation: None,
        is_quoted: false,
    };

    assert!(
        index_column_can_filter(&where_expr, &index_column),
        "index_column_can_filter should work with column indexes"
    );
}

#[test]
#[ignore] // TODO: Enable when storage layer fully supports expression index creation
fn test_expression_index_query_execution() {
    let mut db = create_test_db();

    // Create an expression index on lower(email)
    let lower_email_expr = parse_index_expression("lower(email)");
    db.create_index(
        "idx_users_lower_email".to_string(),
        "users".to_string(),
        false, // not unique
        vec![IndexColumn::Expression {
            expr: Box::new(lower_email_expr),
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query using the expression
    let query = "SELECT id, email FROM users WHERE lower(email) = 'alice@example.com'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should find the row with ALICE@example.com (case-insensitive match via lower())
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], SqlValue::Integer(1));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_expression_index_greater_than() {
    // Test that lower(email) > 'a' matches an index on lower(email)
    let where_expr = parse_where_expression("lower(email) > 'alice'");
    let index_expr = parse_index_expression("lower(email)");

    assert!(
        expression_filters_index_expression(&where_expr, &index_expr),
        "lower(email) > 'value' should match index on lower(email)"
    );
}

#[test]
fn test_expression_index_less_than() {
    // Test that lower(email) < 'z' matches an index on lower(email)
    let where_expr = parse_where_expression("lower(email) < 'zzz'");
    let index_expr = parse_index_expression("lower(email)");

    assert!(
        expression_filters_index_expression(&where_expr, &index_expr),
        "lower(email) < 'value' should match index on lower(email)"
    );
}

#[test]
fn test_expression_index_in_list() {
    // Test that lower(email) IN (...) matches an index on lower(email)
    let where_expr = parse_where_expression("lower(email) IN ('alice', 'bob')");
    let index_expr = parse_index_expression("lower(email)");

    assert!(
        expression_filters_index_expression(&where_expr, &index_expr),
        "lower(email) IN (...) should match index on lower(email)"
    );
}

#[test]
fn test_expression_index_between() {
    // Test that lower(email) BETWEEN 'a' AND 'z' matches an index on lower(email)
    let where_expr = parse_where_expression("lower(email) BETWEEN 'a' AND 'z'");
    let index_expr = parse_index_expression("lower(email)");

    assert!(
        expression_filters_index_expression(&where_expr, &index_expr),
        "lower(email) BETWEEN ... should match index on lower(email)"
    );
}

/// Regression for issue #5784: an expression index must remain FUNCTIONAL after
/// a binary-snapshot reload, not silently return zero rows.
///
/// Before the fix, the snapshot loader re-registered the expression index with
/// an empty body (to avoid a rebuild panic), and the query planner happily used
/// that empty body — so `WHERE r+s = X` returned 0 rows with no error. This test
/// fails on that old behavior (the post-reload query would return 0 rows and the
/// index would be reported as usable) and passes with the rebuild-on-load fix.
#[test]
fn test_expression_index_functional_after_binary_reload() {
    use vibesql_catalog::{ColumnSchema, TableSchema};

    use crate::{
        index_ddl::expression_index::{
            create_expression_index, rebuild_pending_expression_indexes,
        },
        optimizer::index_planner::IndexPlanner,
    };

    // Build a table t3(r, s) with a few rows.
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);
    let schema = TableSchema::new(
        "t3".to_string(),
        vec![
            ColumnSchema::new("r".to_string(), DataType::Integer, true),
            ColumnSchema::new("s".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema.clone()).unwrap();
    // Rows: (1,2)->3, (10,20)->30, (4,4)->8, (2,1)->3  (two rows sum to 3)
    for (r, s) in [(1, 2), (10, 20), (4, 4), (2, 1)] {
        db.insert_row("t3", Row::new(vec![SqlValue::Integer(r), SqlValue::Integer(s)])).unwrap();
    }

    // Create an expression index on (r + s) through the executor so the body is
    // populated with real evaluated keys at CREATE time.
    let expr = parse_index_expression("r + s");
    let columns =
        vec![IndexColumn::Expression { expr: Box::new(expr), direction: OrderDirection::Asc }];
    create_expression_index(&mut db, "t3", "t3", "t3rs", &schema, &columns, false, None).unwrap();

    // Sanity: the index answers WHERE r+s = 3 with the two matching rows.
    let where_expr = parse_where_expression("r + s = 3");
    assert!(
        IndexPlanner::new(&db).can_use_index("t3rs", Some(&where_expr), None),
        "freshly-built expression index should be usable"
    );

    // Persist to a binary snapshot and reload it — this is the reopen path where
    // the loader re-registers the expression index with an empty body.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t3.vbsql");
    db.save_binary(&path).unwrap();
    let mut reloaded = Database::load_binary(&path).unwrap();

    // The reloaded index is registered but pending rebuild (empty body). The
    // planner must DECLINE it so reads fall back to a full scan (never silently
    // wrong).
    assert!(reloaded.index_exists("t3rs"), "expression index should survive reload");
    assert!(
        reloaded.is_index_pending_rebuild("t3rs"),
        "reloaded expression index should be flagged pending rebuild"
    );
    assert!(
        !IndexPlanner::new(&reloaded).can_use_index("t3rs", Some(&where_expr), None),
        "an unbuilt expression index must NOT be used for reads"
    );

    // Even before an explicit rebuild, the query returns CORRECT rows via the
    // full-scan fallback (this is the anti-silent-wrong-answer guarantee).
    let correct_rows = run_r_plus_s_eq_3(&reloaded);
    assert_eq!(
        correct_rows, 2,
        "fallback scan must return the 2 rows where r+s=3, got {correct_rows}"
    );

    // Now perform the REINDEX-on-load and confirm the index becomes functional
    // AND is actually selected for the query. A clean rebuild (issue #6621)
    // reports no warnings — the returned `Vec<String>` is the sole channel
    // for rebuild diagnostics now (no direct stderr write from this function).
    let warnings = rebuild_pending_expression_indexes(&mut reloaded).unwrap();
    assert!(
        warnings.is_empty(),
        "a fully-resolvable rebuild must report no warnings, got: {warnings:?}"
    );
    assert!(
        !reloaded.is_index_pending_rebuild("t3rs"),
        "pending-rebuild flag should clear after rebuild"
    );
    assert!(
        IndexPlanner::new(&reloaded).can_use_index("t3rs", Some(&where_expr), None),
        "rebuilt expression index should be usable (and selected) again"
    );

    // And the query still returns the correct rows, now via the populated index.
    let rebuilt_rows = run_r_plus_s_eq_3(&reloaded);
    assert_eq!(
        rebuilt_rows, 2,
        "after rebuild the index must return the 2 rows where r+s=3, got {rebuilt_rows}"
    );
}

/// Regression for issue #6578: a persisted expression index whose expression
/// cannot be resolved against the table schema (e.g. left over from a
/// `PRAGMA writable_schema=1` escape hatch, or a stale index after a dropped
/// column) must NOT abort the entire `rebuild_pending_expression_indexes`
/// call. SQLite's own documented behavior (matches `quote.test`'s header
/// comment) is that such a schema object persists and loads fine; only
/// actually evaluating the bad expression should error. Before the fix,
/// `rebuild_pending_expression_indexes` propagated the per-index evaluation
/// error with `?`, which — called unconditionally on every CLI database open
/// — turned an otherwise-healthy database permanently unopenable once the
/// table backing the bad index had rows.
#[test]
fn test_rebuild_tolerates_unresolvable_expression_index() {
    use vibesql_catalog::{ColumnSchema, TableSchema};

    use crate::index_ddl::expression_index::rebuild_pending_expression_indexes;

    // Table t1(x, y, z) with one row, mirroring the issue's repro.
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);
    let schema = TableSchema::new(
        "t1".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, true),
            ColumnSchema::new("y".to_string(), DataType::Integer, true),
            ColumnSchema::new("z".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema.clone()).unwrap();
    db.insert_row(
        "t1",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(3)]),
    )
    .unwrap();

    // Register a "bad" expression index (`w1 + 1`, where `w1` is not a column
    // of t1) and a "good" one (`x + y`) exactly the way the binary/JSON
    // snapshot loader would on reload: `Database::create_index` never
    // evaluates the expression for an expression column, it just records the
    // index metadata with an empty body and flags it pending-rebuild (see
    // `IndexManager::create_index`). This reproduces the post-reload state
    // without needing an actual save/load round-trip.
    let bad_expr = parse_index_expression("w1 + 1");
    let bad_columns =
        vec![IndexColumn::Expression { expr: Box::new(bad_expr), direction: OrderDirection::Asc }];
    db.create_index("bad_idx".to_string(), "t1".to_string(), false, bad_columns).unwrap();

    let good_expr = parse_index_expression("x + y");
    let good_columns =
        vec![IndexColumn::Expression { expr: Box::new(good_expr), direction: OrderDirection::Asc }];
    db.create_index("good_idx".to_string(), "t1".to_string(), false, good_columns).unwrap();

    assert!(db.is_index_pending_rebuild("bad_idx"));
    assert!(db.is_index_pending_rebuild("good_idx"));

    // The core regression assertion: rebuilding must NOT propagate the bad
    // index's evaluation error and abort the whole call.
    let result = rebuild_pending_expression_indexes(&mut db);
    assert!(
        result.is_ok(),
        "rebuild_pending_expression_indexes must tolerate an unresolvable \
         expression index rather than hard-failing the whole database load, got: {:?}",
        result.err()
    );

    // Regression for issue #6621: the per-index failure must be surfaced as
    // returned diagnostic data (not written directly to stderr, which would
    // leak into any consumer capturing this call's output, e.g. the CLI under
    // the TCL harness's merged-stream capture). Exactly one warning should be
    // reported, naming the unresolvable index and NOT the sibling that
    // rebuilt fine.
    let warnings = result.unwrap();
    assert_eq!(
        warnings.len(),
        1,
        "expected exactly one warning for the single unresolvable index, got: {warnings:?}"
    );
    assert!(
        warnings[0].contains("bad_idx"),
        "warning should name the unresolvable index 'bad_idx', got: {warnings:?}"
    );
    assert!(
        !warnings[0].contains("good_idx"),
        "warning should not mention the sibling index that rebuilt successfully, got: {warnings:?}"
    );

    // The bad index stays unusable/pending-rebuild forever (the planner
    // already declines a pending-rebuild index, so this is safe) ...
    assert!(
        db.is_index_pending_rebuild("bad_idx"),
        "the unresolvable index should remain flagged pending-rebuild, not silently populated"
    );
    // ... while the good index still gets its chance to rebuild normally.
    assert!(
        !db.is_index_pending_rebuild("good_idx"),
        "a resolvable expression index should still rebuild even when a sibling index fails"
    );

    // And the rest of the database remains fully usable — this is the actual
    // symptom from the issue (a hard open error aborting the whole DB).
    let executor = SelectExecutor::new(&db);
    let stmt = Parser::parse_sql("SELECT x, y, z FROM t1").unwrap();
    let rows = match stmt {
        vibesql_ast::Statement::Select(select_stmt) => executor.execute(&select_stmt).unwrap(),
        _ => panic!("Expected SELECT statement"),
    };
    assert_eq!(rows.len(), 1, "table data must remain intact and queryable");
}

/// Run `SELECT r, s FROM t3 WHERE r + s = 3` and return the row count.
#[cfg(test)]
fn run_r_plus_s_eq_3(db: &Database) -> usize {
    let executor = SelectExecutor::new(db);
    let stmt = Parser::parse_sql("SELECT r, s FROM t3 WHERE r + s = 3").unwrap();
    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            executor.execute(&select_stmt).unwrap().len()
        }
        _ => panic!("Expected SELECT statement"),
    }
}
