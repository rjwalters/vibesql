//! Regression tests for issue #6305: compound-SELECT `ORDER BY` collation
//! resolution must follow SQLite's `multiSelectCollSeq()` leftmost-arm rule.
//!
//! For a compound `SELECT ... UNION [ALL] SELECT ... ORDER BY N`, SQLite
//! determines the collating sequence used to sort result column N by walking
//! the constituent SELECTs **left to right** and using the first arm whose
//! corresponding expression has a *defined* collating sequence -- an explicit
//! `COLLATE`, or a bare column reference (which always has at least its
//! default `BINARY` collation) -- stopping at the first arm that has one,
//! otherwise falling through to later arms.
//!
//! Two compounding defects existed before the fix (see
//! `crates/vibesql-executor/src/select/executor/set_operations.rs`):
//!
//! 1. Only the leftmost arm was ever consulted for collation, so an explicit
//!    `COLLATE` on a later arm never participated in ORDER BY resolution.
//! 2. The per-arm extractor conflated "no defined collation" (a computed
//!    expression like `a||''`) with "default BINARY" (a bare column
//!    reference without an explicit schema `COLLATE`), both represented as
//!    `None`. Without a tri-state signal, a leftmost-arm walk that correctly
//!    distinguishes "stop here" from "fall through" cannot be implemented.
//!
//! Covers `docs/reference/sqlite/test/with1.test` cases 10.8.4.1/.2/.3.

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
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

/// Run a single-column SELECT and collect the text results (assumes column 0
/// holds `Character`/`Varchar` values).
fn query_strings(db: &vibesql_storage::Database, sql: &str) -> Vec<String> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        let rows = executor.execute(&select_stmt).expect("SELECT execution failed");
        rows.iter()
            .map(|row| match &row.values[0] {
                SqlValue::Character(s) | SqlValue::Varchar(s) => s.to_string(),
                other => panic!("Expected text value, got {:?}", other),
            })
            .collect()
    } else {
        panic!("Expected SELECT statement");
    }
}

fn setup_db() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE tst(a, b)");
    run_stmt(&mut db, "INSERT INTO tst VALUES('a', 'A')");
    run_stmt(&mut db, "INSERT INTO tst VALUES('b', 'B')");
    run_stmt(&mut db, "INSERT INTO tst VALUES('c', 'C')");
    db
}

/// with1.test 10.8.4.1: leftmost arm has an explicit `COLLATE nocase`; that
/// wins outright regardless of what later arms do.
#[test]
fn leftmost_explicit_collate_wins() {
    let db = setup_db();
    let rows = query_strings(
        &db,
        "SELECT a COLLATE nocase FROM tst UNION ALL SELECT b FROM tst ORDER BY 1",
    );
    assert_eq!(rows, vec!["a", "A", "b", "B", "c", "C"]);
}

/// with1.test 10.8.4.2: leftmost arm is a bare column reference, which
/// always has a defined (default BINARY) collation -- so it wins over an
/// explicit `COLLATE nocase` on the second arm. Already correct before the
/// fix; guards against regressing it.
#[test]
fn leftmost_bare_column_beats_later_explicit_collate() {
    let db = setup_db();
    let rows = query_strings(
        &db,
        "SELECT a FROM tst UNION ALL SELECT b COLLATE nocase FROM tst ORDER BY 1",
    );
    assert_eq!(rows, vec!["A", "B", "C", "a", "b", "c"]);
}

/// with1.test 10.8.4.3 (the bug): leftmost arm `a||''` is a computed
/// expression with NO defined collation, so resolution must fall through to
/// the second arm's explicit `COLLATE nocase`.
#[test]
fn leftmost_no_collation_expr_falls_through_to_later_explicit_collate() {
    let db = setup_db();
    let rows = query_strings(
        &db,
        "SELECT a||'' FROM tst UNION ALL SELECT b COLLATE nocase FROM tst ORDER BY 1",
    );
    assert_eq!(rows, vec!["a", "A", "b", "B", "c", "C"]);
}

/// Explicit `ORDER BY 1 COLLATE x` on the ORDER BY term itself must still
/// override any arm-derived collation.
#[test]
fn order_by_term_explicit_collate_overrides_arm_derived_collation() {
    let db = setup_db();
    let rows = query_strings(
        &db,
        "SELECT a||'' FROM tst UNION ALL SELECT b FROM tst ORDER BY 1 COLLATE nocase",
    );
    assert_eq!(rows, vec!["a", "A", "b", "B", "c", "C"]);
}

/// All arms undefined (both sides are computed expressions with no defined
/// collation) -- falls all the way through to BINARY.
#[test]
fn all_arms_undefined_defaults_to_binary() {
    let db = setup_db();
    let rows =
        query_strings(&db, "SELECT a||'' FROM tst UNION ALL SELECT b||'' FROM tst ORDER BY 1");
    assert_eq!(rows, vec!["A", "B", "C", "a", "b", "c"]);
}

/// Three-arm chain: the first defined collation appears on the third arm.
#[test]
fn three_arm_chain_first_defined_collation_is_third_arm() {
    let db = setup_db();
    let rows = query_strings(
        &db,
        "SELECT a||'' FROM tst \
         UNION ALL SELECT b||'' FROM tst \
         UNION ALL SELECT b COLLATE nocase FROM tst \
         ORDER BY 1",
    );
    assert_eq!(rows, vec!["a", "A", "A", "b", "B", "B", "c", "C", "C"]);
}

/// Schema-level `COLLATE NOCASE` column (not an explicit COLLATE in the
/// query) as the leftmost arm's defining collation -- a bare column
/// reference is "defined" via its schema collation just as much as an
/// explicit COLLATE keyword.
#[test]
fn leftmost_schema_collate_nocase_column_wins() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE tst2(a TEXT COLLATE NOCASE, b TEXT)");
    run_stmt(&mut db, "INSERT INTO tst2 VALUES('a', 'A')");
    run_stmt(&mut db, "INSERT INTO tst2 VALUES('b', 'B')");
    run_stmt(&mut db, "INSERT INTO tst2 VALUES('c', 'C')");
    let rows = query_strings(&db, "SELECT a FROM tst2 UNION ALL SELECT b FROM tst2 ORDER BY 1");
    assert_eq!(rows, vec!["a", "A", "b", "B", "c", "C"]);
}

/// UNION (DISTINCT) variant: the same arm-resolved collation used for ORDER
/// BY must also drive set-operation dedup comparisons, so NOCASE-equal rows
/// across arms merge into one.
#[test]
fn union_distinct_dedup_uses_same_resolved_collation() {
    let db = setup_db();
    // The leftmost arm `a||''` has no defined collation, so resolution falls
    // through to the second arm's explicit `COLLATE nocase`. Under that
    // NOCASE collation, first-arm 'a' and second-arm 'A' (the only row
    // admitted by `WHERE b = 'A'`) compare equal and must dedup to one row.
    // UNION's dedup keeps the *last* occurrence on a collation-equal key, so
    // the surviving row's stored value is 'A' (from the second arm).
    let rows = query_strings(
        &db,
        "SELECT a||'' FROM tst UNION SELECT b COLLATE nocase FROM tst WHERE b = 'A' ORDER BY 1",
    );
    assert_eq!(rows, vec!["A", "b", "c"]);
}
