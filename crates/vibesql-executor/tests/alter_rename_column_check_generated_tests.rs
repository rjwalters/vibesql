//! Part of #6174: `ALTER TABLE ... RENAME COLUMN` must propagate the rename
//! into this table's OWN table-level/column-level CHECK constraints
//! (`schema.check_constraints`) and any OTHER column's `GENERATED ALWAYS AS`
//! expression (`ColumnSchema::generated_expr`) that references the renamed
//! column. Both are stored as parsed `Expression` ASTs resolved by NAME
//! against the CURRENT schema at evaluation time -- unlike PRIMARY KEY/
//! UNIQUE/FOREIGN KEY, which are plain column-name lists that
//! `TableSchema::rename_column` already fixed up.
//!
//! Before this fix, a CHECK constraint or generated column referencing the
//! renamed column broke every subsequent INSERT/UPDATE that evaluated it,
//! either with a spurious `no such column: <old name>` or by silently no
//! longer enforcing the CHECK (since `has_column` on the old name failed and
//! could short-circuit resolution in ways that vary by expression shape) --
//! verified by direct reproduction against sqlite3 3.51.0, which keeps CHECK
//! enforcement and generated-column computation working under the new name.

use vibesql_ast::pretty_print::ToSql;
use vibesql_executor::{
    AlterTableExecutor, CreateTableExecutor, InsertExecutor, SelectExecutor, TriggerExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TABLE");
    let vibesql_ast::Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

fn try_alter(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt = Parser::parse_sql(sql).expect("parse ALTER TABLE");
    let vibesql_ast::Statement::AlterTable(a) = stmt else {
        panic!("expected ALTER TABLE");
    };
    AlterTableExecutor::execute_with_source(&a, db, Some(sql)).map_err(|e| e.to_string())
}

fn alter(db: &mut Database, sql: &str) {
    try_alter(db, sql).unwrap_or_else(|e| panic!("ALTER TABLE failed unexpectedly: {sql}: {e}"));
}

fn create_trigger(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TRIGGER");
    let vibesql_ast::Statement::CreateTrigger(t) = stmt else {
        panic!("expected CREATE TRIGGER");
    };
    TriggerExecutor::create_trigger_with_sql(db, &t, Some(sql)).expect("CREATE TRIGGER");
}

fn try_insert(db: &mut Database, sql: &str) -> Result<usize, String> {
    let stmt = Parser::parse_sql(sql).expect("parse INSERT");
    let vibesql_ast::Statement::Insert(insert) = stmt else {
        panic!("expected INSERT");
    };
    InsertExecutor::execute(db, &insert).map_err(|e| e.to_string())
}

fn insert(db: &mut Database, sql: &str) {
    try_insert(db, sql).unwrap_or_else(|e| panic!("INSERT failed unexpectedly: {sql}: {e}"));
}

fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).expect("parse SELECT");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    result.rows.into_iter().map(|r| r.values.to_vec()).collect()
}

/// altercol.test's evidence-of comments cover the *text* rewrite of a
/// table-level `CHECK(b != '')` (`CHECK(b != '') -> CHECK(d != '')`) but not
/// runtime re-enforcement. This exercises the runtime behavior directly: a
/// row that still violates the CHECK (now spelled against the new column
/// name) must still be rejected with a genuine CHECK-constraint error, not a
/// stale `no such column: b`.
#[test]
fn rename_column_rewrites_table_level_check_constraint() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a INTEGER, b TEXT, c BLOB, CHECK(b != ''))");
    alter(&mut db, "ALTER TABLE t1 RENAME COLUMN b TO d");

    let err = try_insert(&mut db, "INSERT INTO t1(a, d, c) VALUES(1, '', NULL)")
        .expect_err("expected a CHECK constraint violation, not a silent success");
    assert!(
        err.contains("CHECK constraint failed"),
        "expected a CHECK-constraint error (not a stale column-resolution error), got: {err}"
    );

    // A row that satisfies the (rewritten) CHECK must still insert cleanly.
    insert(&mut db, "INSERT INTO t1(a, d, c) VALUES(1, 'x', NULL)");
}

/// Same as above but for an inline column-level CHECK on the renamed column
/// itself (`b INTEGER CHECK(b > 0)`).
#[test]
fn rename_column_rewrites_column_level_check_constraint() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a INTEGER, b INTEGER CHECK(b > 0))");
    alter(&mut db, "ALTER TABLE t1 RENAME COLUMN b TO d");

    let err = try_insert(&mut db, "INSERT INTO t1(a, d) VALUES(1, -1)")
        .expect_err("expected a CHECK constraint violation, not a silent success");
    assert!(
        err.contains("CHECK constraint failed"),
        "expected a CHECK-constraint error (not a stale column-resolution error), got: {err}"
    );

    insert(&mut db, "INSERT INTO t1(a, d) VALUES(1, 5)");
}

/// A `GENERATED ALWAYS AS` expression on a DIFFERENT column that references
/// the renamed column must compute against the new name post-rename.
#[test]
fn rename_column_rewrites_other_columns_generated_expression() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a INTEGER, b INTEGER, c AS (a + b))");
    alter(&mut db, "ALTER TABLE t1 RENAME COLUMN a TO aa");

    insert(&mut db, "INSERT INTO t1(aa, b) VALUES(2, 3)");

    let rows = query(&db, "SELECT c FROM t1");
    assert_eq!(rows, vec![vec![SqlValue::Integer(5)]]);
}

/// Regression guard for the Judge finding on PR #6739: when the rename is
/// ABORTED by a later dependent-object rewrite (here, an ambiguous unqualified
/// column reference in a trigger body), the rollback restores only the schema
/// column name. The CHECK/generated-column AST rewrite must therefore run only
/// after the trigger/view rewrites succeed; otherwise an aborted rename leaves
/// `check_constraints`/`generated_expr` naming the rolled-back-away new
/// column, and every later INSERT fails spuriously. The trigger lives on a
/// DIFFERENT table (`t1`) so inserting into `t3` does not fire its (still
/// ambiguous) body -- the only thing exercised is `t3`'s own CHECK and
/// generated column.
#[test]
fn aborted_rename_column_leaves_check_and_generated_expressions_untouched() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b)");
    create_table(&mut db, "CREATE TABLE t3(e INTEGER, f INTEGER, g AS (e + f), CHECK(e != 99))");
    create_table(&mut db, "CREATE TABLE t4(e, f)");
    // Both t3 and t4 own column `e`; the trigger's WHERE `e` is ambiguous, so
    // renaming t3.e must abort the whole ALTER (sqlite3 3.51.0,
    // legacy_alter_table=OFF).
    create_trigger(
        &mut db,
        "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t1 SET a=1 FROM t3, t4 WHERE e=2; END",
    );

    let err = try_alter(&mut db, "ALTER TABLE t3 RENAME e TO abc")
        .expect_err("ambiguous trigger reference must abort the rename");
    assert!(err.contains("ambiguous column name: e"), "got: {err}");

    let t3 = db.get_table("t3").expect("t3 exists");
    assert!(t3.schema.has_column("e"), "t3.e must survive the aborted rename");
    assert!(!t3.schema.has_column("abc"), "t3.abc must not exist after the aborted rename");

    // The stored CHECK / generated-column ASTs must still name `e`, exactly as
    // before the ALTER. Assert on the stored AST directly: the runtime
    // evaluator is lenient enough about an unresolvable column that the DML
    // checks below alone do not reliably expose a stale `abc` reference.
    let checks: Vec<String> =
        t3.schema.check_constraints.iter().map(|(_, expr)| expr.to_sql()).collect();
    assert_eq!(checks.len(), 1, "checks: {checks:?}");
    assert!(
        !checks[0].contains("abc") && checks[0].contains('e'),
        "CHECK must still reference `e` after the aborted rename, got: {checks:?}"
    );
    let generated = t3
        .schema
        .columns
        .iter()
        .find(|c| c.name == "g")
        .and_then(|c| c.generated_expr.as_ref())
        .expect("t3.g is a generated column")
        .to_sql();
    assert!(
        !generated.contains("abc") && generated.contains('e'),
        "generated column must still reference `e` after the aborted rename, got: {generated}"
    );

    // The CHECK still resolves against `e` and is still enforced.
    insert(&mut db, "INSERT INTO t3(e, f) VALUES(1, 1)");
    let err = try_insert(&mut db, "INSERT INTO t3(e, f) VALUES(99, 1)")
        .expect_err("expected a CHECK constraint violation after the aborted rename");
    assert!(
        err.contains("CHECK constraint failed"),
        "expected a CHECK-constraint error (not a stale column-resolution error), got: {err}"
    );

    // The generated column still computes against `e`.
    let rows = query(&db, "SELECT g FROM t3");
    assert_eq!(rows, vec![vec![SqlValue::Integer(2)]]);
}
