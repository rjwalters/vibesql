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
            // The parser represents `count(*)`'s `*` as a bare `ColumnRef("*")`;
            // it must not be mistaken for an unresolvable column in a FROM-less
            // SELECT (`SELECT count(*);` is valid SQLite).
            "CREATE TRIGGER tr_star AFTER INSERT ON t1 BEGIN \
             SELECT count(*); \
             SELECT count(*) OVER (); \
             INSERT INTO t2 SELECT count(*); END",
        ],
        "ALTER TABLE t1 RENAME TO t1x",
    );
}

// ---------------------------------------------------------------------------
// JOIN ... USING columns absent from one side
// ---------------------------------------------------------------------------

#[test]
fn using_column_missing_from_both_sides_in_when_subquery_blocks_rename() {
    // altertab3.test 24.1/24.2
    let err = rename_err(
        &[
            "CREATE TABLE v0(v1)",
            "CREATE TABLE v2(v3 INTEGER)",
            "CREATE TRIGGER x AFTER INSERT ON v2 WHEN \
             ((SELECT v1 AS p FROM v2 JOIN v0 USING (VALUE)) AND 0) \
             BEGIN DELETE FROM v2; END",
        ],
        "ALTER TABLE v0 RENAME TO x",
    );
    assert_eq!(
        err,
        "error in trigger x: cannot join using column VALUE - column not present in both tables"
    );
}

#[test]
fn using_column_missing_from_one_side_in_body_blocks_rename() {
    let err = rename_err(
        &[
            "CREATE TABLE p(id, only_p)",
            "CREATE TABLE q(id)",
            "CREATE TRIGGER tr AFTER INSERT ON p BEGIN \
             SELECT 1 FROM p JOIN q USING (only_p); END",
        ],
        "ALTER TABLE q RENAME TO q2",
    );
    assert_eq!(
        err,
        "error in trigger tr: cannot join using column only_p - column not present in both tables"
    );
}

#[test]
fn valid_using_join_in_trigger_does_not_block_rename() {
    rename_ok(
        &[
            "CREATE TABLE p(id, a)",
            "CREATE TABLE q(id, b)",
            "CREATE TABLE r(id, c)",
            "CREATE TRIGGER tr AFTER INSERT ON p WHEN (SELECT 1 FROM p JOIN q USING (ID)) BEGIN \
             SELECT 1 FROM p JOIN q USING (id) JOIN r USING (id); END",
        ],
        "ALTER TABLE r RENAME TO r2",
    );
}

// ---------------------------------------------------------------------------
// Wildcard SELECT without FROM
// ---------------------------------------------------------------------------

#[test]
fn fromless_wildcard_subquery_in_update_from_blocks_rename() {
    // altertab.test 32.0
    let err = rename_err(
        &[
            "CREATE TABLE t1(x)",
            "CREATE TRIGGER r1 BEFORE INSERT ON t1 BEGIN \
             UPDATE t1 SET x=x FROM (SELECT*); END",
        ],
        "ALTER TABLE t1 RENAME TO x",
    );
    assert_eq!(err, "error in trigger r1: no tables specified");
}

#[test]
fn wildcard_subquery_with_from_does_not_block_rename() {
    rename_ok(
        &[
            "CREATE TABLE t1(x)",
            "CREATE TRIGGER r1 BEFORE INSERT ON t1 BEGIN \
             UPDATE t1 SET x=x FROM (SELECT * FROM t1); END",
        ],
        "ALTER TABLE t1 RENAME TO x",
    );
}

// ---------------------------------------------------------------------------
// ORDER BY terms of compound SELECTs (altertab3.test 18.3)
// ---------------------------------------------------------------------------

#[test]
fn compound_select_order_by_expression_not_in_result_blocks_rename() {
    let err = rename_err(
        &[
            "CREATE TABLE t1(a, b)",
            "CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN \
             SELECT a, b FROM t1 INTERSECT SELECT b, a FROM t1 \
             ORDER BY b IN (SELECT a UNION SELECT b FROM t1); END",
        ],
        "ALTER TABLE t1 RENAME TO t1x",
    );
    assert_eq!(
        err,
        "error in trigger r1: 1st ORDER BY term does not match any column in the result set"
    );
}

#[test]
fn compound_select_order_by_column_or_position_or_result_expr_allows_rename() {
    for order_by in ["b", "2", "a + b", "b COLLATE nocase"] {
        rename_ok(
            &[
                "CREATE TABLE t1(a, b)",
                &format!(
                    "CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN \
                     SELECT a, b, a + b FROM t1 UNION SELECT b, a, 0 FROM t1 \
                     ORDER BY {}; END",
                    order_by
                ),
            ],
            "ALTER TABLE t1 RENAME TO t1x",
        );
    }
}

// ---------------------------------------------------------------------------
// Unresolvable columns in trigger-body SELECTs with a FROM clause, including
// window PARTITION BY / ORDER BY and named WINDOW definitions
// (altertab3.test 7.2.2)
// ---------------------------------------------------------------------------

#[test]
fn missing_column_in_named_window_order_by_blocks_rename() {
    let err = rename_err(
        &[
            "CREATE TABLE t1x(a, b, c)",
            "CREATE TRIGGER AFTER INSERT ON t1x BEGIN \
             SELECT a, rank() OVER w1 FROM t1x \
             WINDOW w1 AS (PARTITION BY b, percent_rank() OVER w1 ORDER BY d); END",
        ],
        "ALTER TABLE t1x RENAME TO t1",
    );
    assert_eq!(err, "error in trigger AFTER: no such column: d");
}

#[test]
fn missing_column_in_trigger_select_blocks_rename() {
    for body in [
        "SELECT d FROM t1",
        "SELECT a FROM t1 WHERE d = 1",
        "SELECT a, rank() OVER (ORDER BY d) FROM t1",
        "SELECT sum(a) OVER (PARTITION BY d) FROM t1",
        "SELECT a FROM t1 WINDOW w AS (ORDER BY d)",
        "INSERT INTO t1 SELECT a, b, d FROM t1",
    ] {
        let err = rename_err(
            &[
                "CREATE TABLE t1(a, b, c)",
                &format!("CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN {}; END", body),
            ],
            "ALTER TABLE t1 RENAME TO t2",
        );
        assert_eq!(err, "error in trigger r1: no such column: d", "body: {body}");
    }
}

#[test]
fn resolvable_trigger_select_allows_rename() {
    for body in [
        // `a(*)`'s `*` placeholder is not a column reference (altertab3.test 13.2).
        "SELECT a(*) OVER (ORDER BY (SELECT 1)) FROM t1",
        "SELECT count(*) FROM t1",
        "SELECT new.a, b, rowid FROM t1",
        "SELECT a AS z FROM t1 ORDER BY z",
        "SELECT a, rank() OVER w FROM t1 WINDOW w AS (PARTITION BY b ORDER BY c)",
        "INSERT INTO t1 SELECT a, b, c FROM t1 WHERE a = new.b",
    ] {
        rename_ok(
            &[
                "CREATE TABLE t1(a, b, c)",
                &format!("CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN {}; END", body),
            ],
            "ALTER TABLE t1 RENAME TO t2",
        );
    }
}
