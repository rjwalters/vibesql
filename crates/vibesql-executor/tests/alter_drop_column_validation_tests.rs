//! Regression tests for issue #5784: `ALTER TABLE ... DROP COLUMN` rejects
//! drops that SQLite rejects, using SQLite's verbatim error strings (verified
//! against sqlite3 3.51.0 / `alterdropcol.test`).
//!
//! Covered rejections:
//!   * a UNIQUE column                  -> `cannot drop UNIQUE column: "z"`
//!   * a PRIMARY KEY column             -> `cannot drop PRIMARY KEY column: "x"`
//!   * a plain-index-referenced column  -> `error in index t2y after drop column: no such column:
//!     y`
//!   * an expression-index column       -> `error in index t3rs after drop column: no such column:
//!     s`
//!   * a column of a VIEW               -> `cannot drop column from view "v1"`
//!   * the schema table                 -> `table sqlite_master may not be altered`
//!   * a missing column                 -> `no such column: "d"`
//!   * the only remaining column        -> `cannot drop column "a": no other columns exist`
//!
//! And a positive control: dropping an unconstrained column still succeeds and
//! rewrites the verbatim `sqlite_master.sql` text in place.

use vibesql_executor::{
    AlterTableExecutor, CreateIndexExecutor, CreateTableExecutor, InsertExecutor, SelectExecutor,
    TriggerExecutor, ViewExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TABLE");
    let vibesql_ast::Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

fn create_index(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE INDEX");
    let vibesql_ast::Statement::CreateIndex(idx) = stmt else {
        panic!("expected CREATE INDEX");
    };
    CreateIndexExecutor::execute(&idx, db).expect("CREATE INDEX");
}

fn create_view(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE VIEW");
    let vibesql_ast::Statement::CreateView(view) = stmt else {
        panic!("expected CREATE VIEW");
    };
    ViewExecutor::execute_create_view(&view, db).expect("CREATE VIEW");
}

/// Run an ALTER and return its `Result` (so tests can assert on the error text).
fn try_alter(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt = Parser::parse_sql(sql).expect("parse ALTER");
    let vibesql_ast::Statement::AlterTable(a) = stmt else {
        panic!("expected ALTER TABLE");
    };
    AlterTableExecutor::execute_with_source(&a, db, Some(sql)).map_err(|e| e.to_string())
}

fn drop_column_err(db: &mut Database, sql: &str) -> String {
    try_alter(db, sql).expect_err(&format!("expected DROP COLUMN to fail: {sql}"))
}

fn table_sql(db: &Database, table: &str) -> String {
    let query = format!("SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'");
    let stmt = Parser::parse_sql(&query).expect("parse SELECT");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    assert_eq!(result.rows.len(), 1, "expected one row for table {table}");
    match &result.rows[0].values[0] {
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            s.to_string()
        }
        other => panic!("expected text, got {other:?}"),
    }
}

#[test]
fn drop_unique_column_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t2(x INTEGER PRIMARY KEY, y, z UNIQUE)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t2 DROP COLUMN z"),
        "cannot drop UNIQUE column: \"z\""
    );
}

#[test]
fn drop_column_referenced_by_multi_column_table_unique_is_generic_error() {
    // A multi-column table-level UNIQUE(b, c) is NOT the same rejection as an
    // inline column-level UNIQUE: SQLite only raises `cannot drop UNIQUE
    // column` for the latter. Dropping a column referenced by a multi-column
    // UNIQUE(...) constraint instead falls through to the generic post-drop
    // schema re-parse error, exactly like a table-level CHECK constraint
    // would (verified against sqlite3 3.51.0; alterdropcol2.test 2.2.2).
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE x1(a INTEGER PRIMARY KEY, b, c, UNIQUE(b, c))");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE x1 DROP COLUMN c"),
        "error in table x1 after drop column: no such column: c"
    );
}

#[test]
fn drop_primary_key_column_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t2(x INTEGER PRIMARY KEY, y, z UNIQUE)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t2 DROP COLUMN x"),
        "cannot drop PRIMARY KEY column: \"x\""
    );
}

#[test]
fn drop_column_referenced_by_plain_index_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t2(x INTEGER PRIMARY KEY, y, z UNIQUE)");
    create_index(&mut db, "CREATE INDEX t2y ON t2(y)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t2 DROP COLUMN y"),
        "error in index t2y after drop column: no such column: y"
    );
}

/// quote.test 3.0: a plain index created against a *delimited* identifier
/// (`CREATE INDEX x1 on t1("b")`) earns SQLite's "should this be a string
/// literal in single-quotes?" hint on the DROP-COLUMN dependent-index error
/// (issue #6560). Contrast with `drop_column_referenced_by_plain_index_is_rejected`
/// above, whose unquoted `t2(y)` reference gets no hint.
#[test]
fn drop_column_referenced_by_quoted_plain_index_gets_sqlite_hint() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a,b)");
    create_index(&mut db, "CREATE INDEX x1 on t1(\"b\")");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t1 DROP COLUMN b"),
        "error in index x1 after drop column: no such column: \"b\" - should this be a string literal in single-quotes?"
    );
}

/// quote.test 3.3 (negative control): the hint is keyed on how the INDEX's
/// own `CREATE INDEX` text referenced the column, never on how the dropped
/// column itself was declared. Here the index instead uses a single-quoted
/// alias (`'b'`, SQLite's "string as identifier" fallback quirk, which is
/// NOT a delimited identifier) even though the table column was declared
/// with double quotes -- so no hint.
#[test]
fn drop_column_referenced_by_single_quoted_alias_index_gets_no_hint() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a,\"b\")");
    create_index(&mut db, "CREATE INDEX x1 on t1('b')");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t1 DROP COLUMN b"),
        "error in index x1 after drop column: no such column: b"
    );
}

/// quote.test 3.4: the quoting bit also drives the hint when the dropped
/// column is referenced inside an *expression* index (`"a"||"b"`), not just
/// a direct column reference.
#[test]
fn drop_column_referenced_by_quoted_expression_index_gets_sqlite_hint() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    create_index(&mut db, "CREATE INDEX x1 ON t1(\"a\"||\"b\")");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t1 DROP COLUMN b"),
        "error in index x1 after drop column: no such column: \"b\" - should this be a string literal in single-quotes?"
    );
}

#[test]
fn drop_column_referenced_by_expression_index_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t3(q, r, s)");
    create_index(&mut db, "CREATE INDEX t3rs ON t3(r+s)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t3 DROP COLUMN s"),
        "error in index t3rs after drop column: no such column: s"
    );
}

#[test]
fn drop_column_from_view_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    create_view(&mut db, "CREATE VIEW v1 AS SELECT * FROM t1");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE v1 DROP COLUMN c"),
        "cannot drop column from view \"v1\""
    );
}

#[test]
fn drop_column_from_schema_table_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE sqlite_schema DROP COLUMN sql"),
        "table sqlite_master may not be altered"
    );
}

#[test]
fn drop_missing_column_reports_quoted_name() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    assert_eq!(drop_column_err(&mut db, "ALTER TABLE t1 DROP COLUMN d"), "no such column: \"d\"");
}

#[test]
fn drop_only_remaining_column_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t1 DROP COLUMN a"),
        "cannot drop column \"a\": no other columns exist"
    );
}

#[test]
fn drop_unconstrained_column_succeeds_and_rewrites_schema_text() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    // Dropping an unconstrained middle column succeeds and edits the verbatim
    // CREATE TABLE text in place, matching sqlite3 (whitespace preserved).
    try_alter(&mut db, "ALTER TABLE t1 DROP COLUMN b").expect("DROP COLUMN b");
    assert_eq!(table_sql(&db, "t1"), "CREATE TABLE t1(a, c)");
}

// ---------------------------------------------------------------------------
// Phase 3 (issue #5795): lax view creation + pre-/post-drop schema re-parse
// (alterdropcol.test sections 3 and 5, sqlite3 3.51.0 verbatim messages).
// ---------------------------------------------------------------------------

fn create_trigger(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TRIGGER");
    let vibesql_ast::Statement::CreateTrigger(trigger) = stmt else {
        panic!("expected CREATE TRIGGER");
    };
    TriggerExecutor::create_trigger(db, &trigger).expect("CREATE TRIGGER");
}

fn insert(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse INSERT");
    let vibesql_ast::Statement::Insert(ins) = stmt else {
        panic!("expected INSERT");
    };
    InsertExecutor::execute(db, &ins).expect("INSERT");
}

/// alterdropcol.test 5.2.1: SQLite defers view column resolution, so a view
/// whose SELECT names a not-yet-resolvable column is still created.
#[test]
fn create_view_with_unresolvable_column_is_lax() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE p1(a PRIMARY KEY, b UNIQUE)");
    create_view(&mut db, "CREATE VIEW v1 AS SELECT d, e FROM p1");
    assert!(db.catalog.get_view("v1").is_some(), "lax view v1 should exist");
}

/// alterdropcol.test 5.2.2: the pre-drop schema re-parse covers the *whole*
/// schema — here the broken view selects from p1, unrelated to the altered
/// table c1. No "after drop column" suffix (broken before the drop), and the
/// inner column name is unquoted.
#[test]
fn precheck_rejects_drop_when_unrelated_view_is_broken() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE p1(a PRIMARY KEY, b UNIQUE)");
    create_table(&mut db, "CREATE TABLE c1(x, y)");
    create_view(&mut db, "CREATE VIEW v1 AS SELECT d, e FROM p1");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE c1 DROP COLUMN x"),
        "error in view v1: no such column: d"
    );
    // The failed ALTER must leave c1 untouched.
    assert!(db.get_table("c1").unwrap().schema.has_column("x"));
}

/// alterdropcol.test 5.3.2: a view broken *by* the drop fails the post-check
/// with the "after drop column" suffix, and the drop is not applied.
#[test]
fn postcheck_rejects_drop_that_breaks_dependent_view() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE c1(x, y)");
    create_view(&mut db, "CREATE VIEW v1 AS SELECT x, y FROM c1");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE c1 DROP COLUMN x"),
        "error in view v1 after drop column: no such column: x"
    );
    assert!(db.get_table("c1").unwrap().schema.has_column("x"));
}

/// alterdropcol.test 5.4.2: a trigger referencing a nonexistent `new.<col>`
/// was broken before the drop, so the pre-check reports it without the
/// "after drop column" suffix — and with the `new.` qualifier in the message.
#[test]
fn precheck_rejects_drop_when_trigger_references_missing_new_column() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE p1(a PRIMARY KEY, b UNIQUE)");
    create_table(&mut db, "CREATE TABLE c1(x, y)");
    create_trigger(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON c1 BEGIN \
           INSERT INTO p1 VALUES(new.y, new.xyz); \
         END",
    );
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE c1 DROP COLUMN y"),
        "error in trigger tr: no such column: new.xyz"
    );
    assert!(db.get_table("c1").unwrap().schema.has_column("y"));
}

/// When both a pre-broken trigger and a would-break view exist, the pre-check
/// wins (5.4.2 pattern: v1 references c1.y, which is being dropped, but the
/// already-broken trigger is reported instead).
#[test]
fn precheck_wins_over_postcheck() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE p1(a PRIMARY KEY, b UNIQUE)");
    create_table(&mut db, "CREATE TABLE c1(x, y)");
    create_view(&mut db, "CREATE VIEW v1 AS SELECT x, y FROM c1");
    create_trigger(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON c1 BEGIN \
           INSERT INTO p1 VALUES(new.y, new.xyz); \
         END",
    );
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE c1 DROP COLUMN y"),
        "error in trigger tr: no such column: new.xyz"
    );
}

/// alterdropcol.test 3.1: a *table-level* CHECK referencing the dropped
/// column survives the drop, so the post-check errors instead of silently
/// stripping the constraint.
#[test]
fn postcheck_rejects_drop_that_breaks_table_level_check() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t12(a, b, c, CHECK(c>10))");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t12 DROP COLUMN c"),
        "error in table t12 after drop column: no such column: c"
    );
    assert!(db.get_table("t12").unwrap().schema.has_column("c"));
    // The CHECK constraint must also survive the failed ALTER.
    assert_eq!(db.get_table("t12").unwrap().schema.check_constraints.len(), 1);
}

/// alterdropcol.test 3.2 (positive control): a *column-level* CHECK attached
/// to the dropped column is removed together with the column.
#[test]
fn drop_column_with_own_column_level_check_succeeds() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t13(a, b, c CHECK(c>10))");
    try_alter(&mut db, "ALTER TABLE t13 DROP COLUMN c").expect("DROP COLUMN c");
    let schema = &db.get_table("t13").unwrap().schema;
    assert!(!schema.has_column("c"));
    assert!(schema.check_constraints.is_empty(), "own CHECK dropped with the column");
}

/// A column-level CHECK on a *different* column referencing the dropped one
/// survives the drop and must fail the post-check (SQLite re-parse
/// semantics).
#[test]
fn postcheck_rejects_drop_referenced_by_other_columns_check() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t14(a, b CHECK(c>10), c)");
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE t14 DROP COLUMN c"),
        "error in table t14 after drop column: no such column: c"
    );
}

/// Atomicity control: a failed post-check leaves schema text and row data
/// untouched.
#[test]
fn failed_drop_leaves_schema_and_rows_untouched() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE c1(x, y)");
    insert(&mut db, "INSERT INTO c1 VALUES(1, 2)");
    insert(&mut db, "INSERT INTO c1 VALUES(3, 4)");
    create_view(&mut db, "CREATE VIEW v1 AS SELECT x, y FROM c1");

    drop_column_err(&mut db, "ALTER TABLE c1 DROP COLUMN x");

    assert_eq!(table_sql(&db, "c1"), "CREATE TABLE c1(x, y)");
    let table = db.get_table("c1").unwrap();
    assert_eq!(table.row_count(), 2);
    assert_eq!(table.schema.columns.len(), 2);
}

/// `DROP COLUMN IF EXISTS` on a missing column is a no-op that skips the
/// schema re-parse entirely (even with a broken view present).
#[test]
fn if_exists_noop_skips_schema_recheck() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE p1(a PRIMARY KEY, b UNIQUE)");
    create_table(&mut db, "CREATE TABLE c1(x, y)");
    create_view(&mut db, "CREATE VIEW v1 AS SELECT d, e FROM p1");
    try_alter(&mut db, "ALTER TABLE c1 DROP COLUMN IF EXISTS nosuch")
        .expect("IF EXISTS no-op should succeed despite broken view");
}

/// A view over `SELECT *` adapts to the drop and must not block it
/// (alterdropcol.test 1.6.1 pattern).
#[test]
fn star_view_does_not_block_drop() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    create_view(&mut db, "CREATE VIEW v1 AS SELECT * FROM t1");
    try_alter(&mut db, "ALTER TABLE t1 DROP COLUMN b").expect("DROP COLUMN b");
    assert_eq!(table_sql(&db, "t1"), "CREATE TABLE t1(a, c)");
}

/// A trigger on the altered table whose `new.<col>` reference names the
/// dropped column fails the post-check with the "after drop column" suffix.
#[test]
fn postcheck_rejects_drop_that_breaks_trigger_on_altered_table() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE p1(a PRIMARY KEY, b UNIQUE)");
    create_table(&mut db, "CREATE TABLE c1(x, y)");
    create_trigger(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON c1 BEGIN \
           INSERT INTO p1 VALUES(new.x, new.y); \
         END",
    );
    assert_eq!(
        drop_column_err(&mut db, "ALTER TABLE c1 DROP COLUMN y"),
        "error in trigger tr after drop column: no such column: new.y"
    );
}
