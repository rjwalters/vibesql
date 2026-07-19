//! Regression tests for the aggregate `FILTER (WHERE ...)` clause and
//! `nth_value` second-argument coercion (#6191).
//!
//! The columnar aggregate paths lower each aggregate to an `AggregateSpec` that
//! captures only the operation and its source column — a `FILTER` clause used
//! to be silently dropped, so an implicit single-group aggregate like
//! `sum(a) FILTER (WHERE a<5)` aggregated over every row. These tests pin the
//! row-oriented fallback so the filter is honored regardless of GROUP BY.

use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use super::super::*;

/// Helper to execute a SQL statement, returning the result rows.
fn execute_sql(db: &mut Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
        }
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)
                .map_err(|e| format!("Execution error: {:?}", e))?;
            Ok(vec![])
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt)
                .map_err(|e| format!("Execution error: {:?}", e))?;
            Ok(vec![])
        }
        _ => Err("Unsupported statement type".to_string()),
    }
}

fn setup_t1(db: &mut Database) {
    execute_sql(db, "CREATE TABLE t1(a)").unwrap();
    for v in 1..=9 {
        execute_sql(db, &format!("INSERT INTO t1 VALUES({v})")).unwrap();
    }
}

/// `sum(a) FILTER (WHERE a<5)` over the implicit single group must sum only the
/// matching rows (1+2+3+4 = 10), NOT the whole table (45). This is the core
/// #6191 regression — the columnar fast path used to drop the FILTER.
#[test]
fn test_sum_filter_no_group_by() {
    let mut db = Database::new();
    setup_t1(&mut db);

    let rows = execute_sql(&mut db, "SELECT sum(a) FILTER (WHERE a<5) FROM t1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(10));
}

/// A FILTER that no row satisfies must yield NULL for sum (empty aggregate),
/// not 0 and not the unfiltered total.
#[test]
fn test_sum_filter_empty_is_null() {
    let mut db = Database::new();
    setup_t1(&mut db);

    let rows = execute_sql(&mut db, "SELECT sum(a) FILTER (WHERE a>100) FROM t1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Null);
}

/// `count(*) FILTER (WHERE a!=5)` must not take the simple-COUNT(*) fast path
/// (which ignores the filter and returns the full row count of 9); it must
/// count only the 8 rows that pass the filter.
#[test]
fn test_count_star_filter_no_group_by() {
    let mut db = Database::new();
    setup_t1(&mut db);

    let rows = execute_sql(&mut db, "SELECT count(*) FILTER (WHERE a!=5) FROM t1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(8));
}

/// A plain `count(*)` with no FILTER must still use the fast path and return
/// the full row count (guards against over-broadening the fast-path opt-out).
#[test]
fn test_count_star_without_filter_still_counts_all() {
    let mut db = Database::new();
    setup_t1(&mut db);

    let rows = execute_sql(&mut db, "SELECT count(*) FROM t1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(9));
}

/// FILTER must still work alongside GROUP BY (this path already worked; kept as
/// a guard so the row-oriented fix doesn't regress the grouped case).
#[test]
fn test_min_filter_with_group_by() {
    let mut db = Database::new();
    setup_t1(&mut db);

    let rows =
        execute_sql(&mut db, "SELECT min(a) FILTER (WHERE a>3) FROM t1 GROUP BY (a%2) ORDER BY 1")
            .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[0], SqlValue::Integer(4));
    assert_eq!(rows[1].values[0], SqlValue::Integer(5));
}

/// `count(ORDER BY a)` (aggregate ORDER BY, no arguments) is order-independent
/// for count and must keep returning the total row count — a guard against the
/// FILTER fix accidentally routing order-only aggregates to a path that rejects
/// them.
#[test]
fn test_count_with_order_by_no_args() {
    let mut db = Database::new();
    setup_t1(&mut db);

    let rows = execute_sql(&mut db, "SELECT count(ORDER BY a) FROM t1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(9));
}

fn setup_nth(db: &mut Database) {
    execute_sql(db, "CREATE TABLE tv(a, b)").unwrap();
    execute_sql(db, "INSERT INTO tv VALUES(1, 2)").unwrap();
    execute_sql(db, "INSERT INTO tv VALUES(2, 3)").unwrap();
    execute_sql(db, "INSERT INTO tv VALUES(3, 4)").unwrap();
}

/// `nth_value` accepts a second argument that coerces to a positive integer via
/// SQLite's numeric affinity: '2', 2.0 and '2.0' all mean N=2, yielding
/// {NULL, 3, 3} over the running frame.
#[test]
fn test_nth_value_coercible_second_argument() {
    for arg in ["'2'", "2.0", "'2.0'"] {
        let mut db = Database::new();
        setup_nth(&mut db);

        let rows =
            execute_sql(&mut db, &format!("SELECT nth_value(b, {arg}) OVER (ORDER BY a) FROM tv"))
                .unwrap_or_else(|e| panic!("nth_value(b, {arg}) should succeed, got: {e}"));
        assert_eq!(rows.len(), 3, "arg={arg}");
        assert_eq!(rows[0].values[0], SqlValue::Null, "arg={arg}");
        assert_eq!(rows[1].values[0], SqlValue::Integer(3), "arg={arg}");
        assert_eq!(rows[2].values[0], SqlValue::Integer(3), "arg={arg}");
    }
}

/// Invalid `nth_value` second arguments (0, -1, non-integral float, non-numeric
/// or NULL) must all raise SQLite's exact error message.
#[test]
fn test_nth_value_invalid_second_argument() {
    for arg in ["0", "-1", "8.5", "'4ab'", "NULL"] {
        let mut db = Database::new();
        setup_nth(&mut db);

        let err =
            execute_sql(&mut db, &format!("SELECT nth_value(b, {arg}) OVER (ORDER BY a) FROM tv"))
                .expect_err(&format!("nth_value(b, {arg}) should error"));
        assert!(
            err.contains("second argument to nth_value must be a positive integer"),
            "arg={arg} produced unexpected error: {err}"
        );
    }
}
