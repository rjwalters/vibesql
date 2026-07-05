//! Tests for COLLATE handling in MIN/MAX aggregates.
//!
//! Regression coverage for issue #5842 sub-item 1: `min(x COLLATE nocase)` /
//! `max(x COLLATE ...)` must order their operands by the requested collation
//! instead of raw binary bytes. Expected values verified against sqlite3
//! (minmax3 §4).

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn text(s: &str) -> SqlValue {
    SqlValue::Varchar(arcstr::ArcStr::from(s))
}

/// Build a database with `t4(x)` holding the minmax3 §4 fixture rows
/// ('abc' and 'BCD'). Under BINARY collation uppercase sorts before lowercase,
/// so max('abc','BCD')='abc'; under NOCASE, max='BCD'.
fn make_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();

    let create = vibesql_parser::Parser::parse_sql("CREATE TABLE t4(x)").unwrap();
    if let vibesql_ast::Statement::CreateTable(ct) = create {
        vibesql_executor::CreateTableExecutor::execute(&ct, &mut db).unwrap();
    } else {
        panic!("expected CREATE TABLE");
    }

    let insert =
        vibesql_parser::Parser::parse_sql("INSERT INTO t4(x) VALUES ('abc'), ('BCD')").unwrap();
    if let vibesql_ast::Statement::Insert(ins) = insert {
        vibesql_executor::InsertExecutor::execute(&mut db, &ins).unwrap();
    } else {
        panic!("expected INSERT");
    }

    db
}

fn eval_single(db: &vibesql_storage::Database, sql: &str) -> SqlValue {
    let executor = SelectExecutor::new(db);
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        let result = executor.execute_with_columns(&select).unwrap();
        assert_eq!(result.rows.len(), 1, "expected exactly one row for `{sql}`");
        result.rows[0].values[0].clone()
    } else {
        panic!("expected SELECT for `{sql}`");
    }
}

#[test]
fn test_max_binary_vs_nocase_collation() {
    let db = make_db();
    // Default (BINARY): 'a' (0x61) > 'B' (0x42), so max is 'abc'.
    assert_eq!(eval_single(&db, "SELECT max(x) FROM t4"), text("abc"));
    // NOCASE: case-insensitively 'BCD' > 'abc', so max is 'BCD'.
    assert_eq!(eval_single(&db, "SELECT max(x COLLATE nocase) FROM t4"), text("BCD"));
    // An explicit BINARY collation matches the default.
    assert_eq!(eval_single(&db, "SELECT max(x COLLATE binary) FROM t4"), text("abc"));
}

#[test]
fn test_min_binary_vs_nocase_collation() {
    let db = make_db();
    // Default (BINARY): 'B' < 'a', so min is 'BCD'.
    assert_eq!(eval_single(&db, "SELECT min(x) FROM t4"), text("BCD"));
    // NOCASE: case-insensitively 'abc' < 'BCD', so min is 'abc'.
    assert_eq!(eval_single(&db, "SELECT min(x COLLATE nocase) FROM t4"), text("abc"));
}

#[test]
fn test_collation_does_not_leak_between_aggregates() {
    let db = make_db();
    // A collated and an uncollated aggregate in the same SELECT must each use
    // their own comparison (minmax3-4.3 / 4.6).
    let executor = SelectExecutor::new(&db);
    let stmt =
        vibesql_parser::Parser::parse_sql("SELECT max(x), max(x COLLATE nocase) FROM t4").unwrap();
    if let vibesql_ast::Statement::Select(select) = stmt {
        let result = executor.execute_with_columns(&select).unwrap();
        assert_eq!(result.rows[0].values[0], text("abc"));
        assert_eq!(result.rows[0].values[1], text("BCD"));
    } else {
        panic!("expected SELECT");
    }
}
