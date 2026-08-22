//! Prepare-time validation clause coverage (issues #6101 and #6110).
//!
//! Two validators that previously covered only the SELECT-list and WHERE
//! clauses are extended to the remaining SELECT clauses:
//!
//! * #6110 — COLLATE-name validation (`no such collation sequence: <name>`) now also walks ORDER
//!   BY, GROUP BY, HAVING, and JOIN ... ON. Before the fix an unknown collation named there was
//!   silently accepted (a false negative); the three built-ins (BINARY/NOCASE/RTRIM, any
//!   case-spelling) must keep passing in every clause (no false positives).
//! * #6101 — the SELECT-predicate scalar-subquery arity / row-value-misuse walk now also runs over
//!   HAVING and JOIN ... ON, matching SQLite's prepare-time rejection over an empty table.
//!
//! All expected results verified against sqlite3 3.51.0.

use vibesql_executor::SelectExecutor;

fn run_ddl(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert).unwrap();
        }
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

/// Execute a SELECT, returning Ok(row_count) or Err(error message string).
fn try_select(db: &vibesql_storage::Database, sql: &str) -> Result<usize, String> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).map(|rows| rows.len()).map_err(|e| format!("{e}"))
    } else {
        panic!("Expected SELECT statement: {sql}");
    }
}

fn two_col_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_ddl(&mut db, "CREATE TABLE t(a TEXT, b TEXT)");
    run_ddl(&mut db, "CREATE TABLE u(c TEXT, d TEXT)");
    db
}

// ---------------------------------------------------------------------------
// #6110 — COLLATE-name validation across ORDER BY / GROUP BY / HAVING / JOIN ON
// ---------------------------------------------------------------------------

#[test]
fn unknown_collation_in_order_by_errors() {
    let db = two_col_db();
    let err = try_select(&db, "SELECT a FROM t ORDER BY a COLLATE nose").unwrap_err();
    assert!(err.contains("no such collation sequence: nose"), "got: {err}");
}

#[test]
fn unknown_collation_in_group_by_errors() {
    let db = two_col_db();
    let err = try_select(&db, "SELECT a FROM t GROUP BY a COLLATE nose").unwrap_err();
    assert!(err.contains("no such collation sequence: nose"), "got: {err}");
}

#[test]
fn unknown_collation_in_having_errors() {
    let db = two_col_db();
    let err =
        try_select(&db, "SELECT a FROM t GROUP BY a HAVING a COLLATE nose > 'x'").unwrap_err();
    assert!(err.contains("no such collation sequence: nose"), "got: {err}");
}

#[test]
fn unknown_collation_in_join_on_errors() {
    let db = two_col_db();
    let err = try_select(&db, "SELECT a FROM t JOIN u ON a COLLATE nose = c").unwrap_err();
    assert!(err.contains("no such collation sequence: nose"), "got: {err}");
}

#[test]
fn builtin_collations_pass_in_every_clause_and_spelling() {
    // Every built-in in every case-spelling in every newly-covered clause must
    // be accepted (no false positives). Empty tables → 0 rows, never an error.
    let db = two_col_db();
    for name in ["binary", "BINARY", "nocase", "NOCASE", "rtrim", "RTRIM"] {
        for sql in [
            format!("SELECT a FROM t ORDER BY a COLLATE {name}"),
            format!("SELECT a FROM t GROUP BY a COLLATE {name}"),
            format!("SELECT a FROM t GROUP BY a HAVING a COLLATE {name} > 'x'"),
            format!("SELECT a FROM t JOIN u ON a COLLATE {name} = c"),
        ] {
            let res = try_select(&db, &sql);
            assert!(res.is_ok(), "built-in {name} must pass: {sql} -- got {res:?}");
        }
    }
}

// ---------------------------------------------------------------------------
// #6101 — scalar-subquery arity / row-value misuse across HAVING and JOIN ON
// ---------------------------------------------------------------------------

#[test]
fn comparison_multicol_subquery_in_having_is_row_value_misused() {
    let db = two_col_db();
    let err =
        try_select(&db, "SELECT a FROM t GROUP BY a HAVING a < (SELECT b, 2 FROM t)").unwrap_err();
    assert!(err.to_lowercase().contains("row value misused"), "got: {err}");
}

#[test]
fn comparison_multicol_subquery_in_join_on_is_row_value_misused() {
    let db = two_col_db();
    let err = try_select(&db, "SELECT a FROM t JOIN u ON a < (SELECT b, 2 FROM t)").unwrap_err();
    assert!(err.to_lowercase().contains("row value misused"), "got: {err}");
}

#[test]
fn in_multicol_subquery_in_having_is_arity_error() {
    let db = two_col_db();
    let err =
        try_select(&db, "SELECT a FROM t GROUP BY a HAVING a IN (SELECT b, 2 FROM t)").unwrap_err();
    assert!(err.contains("sub-select returns 2 columns"), "got: {err}");
}

#[test]
fn in_multicol_subquery_in_join_on_is_arity_error() {
    let db = two_col_db();
    let err = try_select(&db, "SELECT a FROM t JOIN u ON a IN (SELECT b, 2 FROM t)").unwrap_err();
    assert!(err.contains("sub-select returns 2 columns"), "got: {err}");
}

#[test]
fn valid_single_column_subquery_in_having_and_join_on_passes() {
    // No false positives: a single-column subquery and plain predicates are
    // legal in HAVING and JOIN ON.
    let db = two_col_db();
    assert!(try_select(&db, "SELECT a FROM t GROUP BY a HAVING a < (SELECT b FROM t)").is_ok());
    assert!(try_select(&db, "SELECT a FROM t JOIN u ON a < (SELECT b FROM t)").is_ok());
    assert!(try_select(&db, "SELECT a FROM t JOIN u ON a = c").is_ok());
    assert!(try_select(&db, "SELECT a FROM t GROUP BY a HAVING count(*) > 1").is_ok());
}
