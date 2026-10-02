//! Part of #6174 (altertab3.test 19.2.2): on RENAME's schema re-parse a FROM-less
//! view whose window frame-bound holds a CTE with an unresolvable column reports
//! `no such column` (in source spelling) even when an `IN <table>` right-hand
//! side later in the same expression names a missing table, because SQLite
//! resolves the left operand first.

use vibesql_executor::{AlterTableExecutor, CreateTableExecutor, ViewExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

#[test]
fn frame_cte_missing_column_precedes_in_rhs_missing_table() {
    let mut db = Database::new();
    let sql = "CREATE TABLE a(a, h)";
    let vibesql_ast::Statement::CreateTable(create) = Parser::parse_sql(sql).unwrap() else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, &mut db, Some(sql)).unwrap();

    let view = "CREATE VIEW q AS SELECT 99 WINDOW x AS (RANGE BETWEEN UNBOUNDED PRECEDING AND \
                count(*) OVER (PARTITION BY (WITH c AS (VALUES(LEFT)) VALUES(0)) IN STORED) \
                FOLLOWING)";
    let vibesql_ast::Statement::CreateView(v) = Parser::parse_sql(view).unwrap() else {
        panic!("expected CREATE VIEW");
    };
    ViewExecutor::execute_create_view(&v, &mut db).unwrap();

    let alter = "ALTER TABLE a RENAME TO g";
    let vibesql_ast::Statement::AlterTable(a) = Parser::parse_sql(alter).unwrap() else {
        panic!("expected ALTER");
    };
    let err = AlterTableExecutor::execute_with_source(&a, &mut db, Some(alter))
        .expect_err("RENAME must fail")
        .to_string();
    // Source spelling (`LEFT`) is recovered from the stored CREATE VIEW text,
    // which the CLI path records but this direct-executor path does not.
    assert!(
        err.eq_ignore_ascii_case("error in view q: no such column: LEFT"),
        "unexpected error: {err}"
    );
}
