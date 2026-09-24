//! Tests for ALTER TABLE's dependent-trigger precheck
//! (`alter::drop_column_checks::precheck_schema_objects`) reaching references
//! that live *inside nested scopes* of a trigger body:
//!
//! - a missing table named only inside an expression subquery (scalar / `EXISTS` / `IN`), including
//!   one inside a named `WINDOW` definition or a row-value `UPDATE SET` (altertab3.test 11.2,
//!   23.2);
//! - a bare column reference inside an uncorrelated FROM-less SELECT, which has no relation to
//!   resolve against (altertab3.test 14.2, 26.6).
//!
//! SQLite's schema re-parse on `ALTER TABLE ... RENAME TO` resolves all of
//! these and aborts the ALTER with `error in trigger <name>: <inner>`. The
//! negative tests pin the conservative side: valid triggers whose shape is
//! close to the broken ones must NOT block the ALTER.

use vibesql_ast::Statement;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

fn exec(db: &mut Database, sql: &str) -> Result<String, ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => crate::CreateTableExecutor::execute(&s, db),
        Statement::CreateTrigger(s) => {
            crate::TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
        }
        Statement::AlterTable(s) => crate::alter::AlterTableExecutor::execute(&s, db),
        other => panic!("unexpected statement: {:?}", other),
    }
}

fn rename_err(setup: &[&str], alter: &str) -> String {
    let mut db = Database::new();
    for sql in setup {
        exec(&mut db, sql).unwrap_or_else(|e| panic!("setup `{}` failed: {}", sql, e));
    }
    exec(&mut db, alter).expect_err("ALTER should have been rejected").to_string()
}

fn rename_ok(setup: &[&str], alter: &str) {
    let mut db = Database::new();
    for sql in setup {
        exec(&mut db, sql).unwrap_or_else(|e| panic!("setup `{}` failed: {}", sql, e));
    }
    exec(&mut db, alter).unwrap_or_else(|e| panic!("ALTER `{}` failed: {}", alter, e));
}

// ---------------------------------------------------------------------------
// Missing tables inside expression subqueries
// ---------------------------------------------------------------------------

#[test]
fn missing_table_in_window_definition_subquery_blocks_rename() {
    // altertab3.test 11.2
    let err = rename_err(
        &[
            "CREATE TABLE t1(a, b)",
            "CREATE TRIGGER b AFTER INSERT ON t1 WHEN new.a BEGIN \
             SELECT a, sum() w3 FROM t1 \
             WINDOW b AS (ORDER BY NOT EXISTS(SELECT 1 FROM abc)); END",
        ],
        "ALTER TABLE t1 RENAME TO t1x",
    );
    assert_eq!(err, "error in trigger b: no such table: main.abc");
}

#[test]
fn missing_table_in_row_value_update_subquery_blocks_rename() {
    // altertab3.test 23.2
    let err = rename_err(
        &[
            "CREATE TABLE t1(x)",
            "CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN \
             UPDATE t1 SET (c,d)=((SELECT 1 FROM t1 JOIN t2 ON b=x),1); END",
        ],
        "ALTER TABLE t1 RENAME TO t1x",
    );
    assert_eq!(err, "error in trigger r1: no such table: main.t2");
}

#[test]
fn missing_table_in_where_in_subquery_blocks_rename() {
    let err = rename_err(
        &[
            "CREATE TABLE t1(x)",
            "CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN \
             DELETE FROM t1 WHERE x IN (SELECT y FROM nosuch); END",
        ],
        "ALTER TABLE t1 RENAME TO t1x",
    );
    assert_eq!(err, "error in trigger r1: no such table: main.nosuch");
}

#[test]
fn existing_table_and_cte_in_expression_subqueries_do_not_block_rename() {
    rename_ok(
        &[
            "CREATE TABLE t1(x)",
            "CREATE TABLE t2(y)",
            "CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN \
             DELETE FROM t2 WHERE y IN (WITH c AS (SELECT x FROM t1) SELECT x FROM c) \
             AND EXISTS (SELECT 1 FROM t2); END",
        ],
        "ALTER TABLE t1 RENAME TO t1x",
    );
}

// ---------------------------------------------------------------------------
// Bare columns in uncorrelated FROM-less SELECTs
// ---------------------------------------------------------------------------

#[test]
fn bare_column_in_fromless_trigger_select_blocks_rename() {
    // altertab3.test 14.2
    let err = rename_err(
        &[
            "CREATE TABLE t1(a)",
            "CREATE TABLE t2(b)",
            "CREATE TRIGGER tr AFTER INSERT ON t1 BEGIN \
             SELECT sum() FILTER (WHERE (SELECT sum() FILTER (WHERE 0)) AND a); END",
        ],
        "ALTER TABLE t1 RENAME TO t1x",
    );
    assert_eq!(err, "error in trigger tr: no such column: a");
}

#[test]
fn bare_column_in_fromless_derived_table_blocks_rename() {
    // altertab3.test 26.6
    let err = rename_err(
        &[
            "CREATE TABLE t1(xx)",
            "CREATE TRIGGER xx INSERT ON t1 BEGIN UPDATE t1 SET xx=xx FROM (SELECT xx); END",
        ],
        "ALTER TABLE t1 RENAME TO t2",
    );
    assert_eq!(err, "error in trigger xx: no such column: xx");
}

#[test]
fn valid_fromless_selects_do_not_block_rename() {
    rename_ok(
        &[
            "CREATE TABLE t1(a)",
            "CREATE TABLE t2(b)",
            // NEW.* refs, literals, a WHERE referencing a select-list alias,
            // a quoted identifier (DQS string fallback), and a correlated
            // FROM-less subquery that resolves against its outer FROM.
            "CREATE TRIGGER tr AFTER INSERT ON t1 BEGIN \
             SELECT new.a, 1 AS one WHERE one; \
             SELECT \"hello\"; \
             SELECT (SELECT b) FROM t2; \
             INSERT INTO t2 SELECT new.a + 1; \
             UPDATE t2 SET b = z FROM (SELECT 5 AS z); END",
        ],
        "ALTER TABLE t1 RENAME TO t1x",
    );
}
