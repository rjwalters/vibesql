//! Regression tests for issue #6555: UNIQUE/PRIMARY KEY index comparison was
//! coercing numeric-looking TEXT values (e.g. `'1'`) to match INTEGER values
//! (e.g. `1`) even on columns with BLOB/TEXT affinity, where SQLite does NOT
//! perform this coercion. This produced a false "UNIQUE constraint failed"
//! and silently-wrong index point-lookups.
//!
//! The fix has two halves:
//! - `vibesql_storage::database::indexes::value_normalization` no longer guesses string->number
//!   coercion for index keys (it now only canonicalizes already-numeric storage classes to
//!   `Double`). Row values reaching the index (via INSERT/UPDATE) are already coerced to their
//!   column's declared affinity before they get there, so this is safe and sufficient for UNIQUE/PK
//!   constraint enforcement.
//! - `vibesql_executor::select::scan::index_scan::predicate::affinity_coercion` coerces
//!   WHERE-clause *literals* (which never go through INSERT/UPDATE coercion) to the target column's
//!   declared affinity before they reach the index, so `WHERE int_col = '123'` still matches
//!   `int_col = 123` on NUMERIC/INTEGER/REAL-affinity columns.

use vibesql_executor::{CreateIndexExecutor, CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn exec(db: &mut Database, sql: &str) -> Result<(), vibesql_executor::ExecutorError> {
    match Parser::parse_sql(sql).expect("test SQL should parse") {
        vibesql_ast::Statement::CreateTable(s) => CreateTableExecutor::execute(&s, db).map(|_| ()),
        vibesql_ast::Statement::CreateIndex(s) => CreateIndexExecutor::execute(&s, db).map(|_| ()),
        vibesql_ast::Statement::Insert(s) => InsertExecutor::execute(db, &s).map(|_| ()),
        other => panic!("unexpected statement in test: {other:?}"),
    }
}

fn select_rows(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    SelectExecutor::new(db).execute(&s).unwrap()
}

/// Issue #6555 primary repro: an implicit PRIMARY KEY index on an untyped
/// (BLOB-affinity) column must not collide INTEGER 1 with TEXT '1'.
#[test]
fn implicit_pk_blob_affinity_accepts_integer_and_text_as_distinct() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE par(p PRIMARY KEY)").unwrap();
    exec(&mut db, "INSERT INTO par VALUES(1)").unwrap();

    let result = exec(&mut db, "INSERT INTO par VALUES('1')");
    assert!(
        result.is_ok(),
        "INTEGER 1 and TEXT '1' must be distinct on a BLOB-affinity PK column, got: {result:?}"
    );

    let rows = select_rows(&db, "SELECT typeof(p), p FROM par ORDER BY typeof(p)");
    assert_eq!(rows.len(), 2, "both rows must be present");
}

/// Same repro via an explicit `CREATE UNIQUE INDEX` (no PRIMARY KEY at all),
/// per the issue's "not PK-specific" note.
#[test]
fn explicit_unique_index_blob_affinity_accepts_integer_and_text_as_distinct() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE par(p)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX par_p_uniq ON par(p)").unwrap();
    exec(&mut db, "INSERT INTO par VALUES(1)").unwrap();

    let result = exec(&mut db, "INSERT INTO par VALUES('1')");
    assert!(
        result.is_ok(),
        "INTEGER 1 and TEXT '1' must be distinct under a UNIQUE index on a BLOB-affinity column, got: {result:?}"
    );

    let rows = select_rows(&db, "SELECT p FROM par");
    assert_eq!(rows.len(), 2);
}

/// A BLOB value (`x'31'`) must also stay distinct from INTEGER 1 and TEXT
/// '1' under the same UNIQUE index.
#[test]
fn explicit_unique_index_blob_literal_is_distinct_from_integer_and_text() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE par(p)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX par_p_uniq ON par(p)").unwrap();
    exec(&mut db, "INSERT INTO par VALUES(1)").unwrap();
    exec(&mut db, "INSERT INTO par VALUES('1')").unwrap();

    let result = exec(&mut db, "INSERT INTO par VALUES(X'31')");
    assert!(
        result.is_ok(),
        "BLOB x'31' must be distinct from INTEGER 1 and TEXT '1', got: {result:?}"
    );

    let rows = select_rows(&db, "SELECT p FROM par");
    assert_eq!(rows.len(), 3);
}

/// Regression guard: INTEGER-affinity PRIMARY KEY columns must still coerce
/// a TEXT literal to match the stored INTEGER value (SQLite's NUMERIC/INTEGER
/// affinity coercion), so the UNIQUE constraint is still correctly enforced.
#[test]
fn integer_affinity_pk_still_collides_integer_and_text() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(1)").unwrap();

    let result = exec(&mut db, "INSERT INTO t VALUES('1')");
    assert!(
        result.is_err(),
        "INTEGER-affinity PK must still coerce '1' to 1 and raise UNIQUE constraint failed"
    );
}

/// Regression guard: `WHERE int_col = '123'` must still match `int_col = 123`
/// via a non-PK UNIQUE index (issue #6555's affinity-aware coercion at the
/// predicate-extraction layer, not the PK hash-map fast path).
#[test]
fn where_clause_text_literal_still_matches_integer_affinity_indexed_column() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(id INTEGER, val INTEGER)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t_val_uniq ON t(val)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(1, 123)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(2, 456)").unwrap();

    let rows = select_rows(&db, "SELECT id FROM t WHERE val = '123'");
    assert_eq!(rows.len(), 1, "TEXT literal '123' must match INTEGER-affinity val=123");
    assert_eq!(rows[0].get(0).unwrap(), &vibesql_types::SqlValue::Integer(1));
}

/// Regression guard: the same coercion applies to range predicates on an
/// INTEGER-affinity indexed column.
#[test]
fn where_clause_text_literal_range_still_matches_integer_affinity_indexed_column() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(id INTEGER, val INTEGER)").unwrap();
    exec(&mut db, "CREATE INDEX t_val_idx ON t(val)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(1, 100)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(2, 200)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(3, 300)").unwrap();

    let rows = select_rows(&db, "SELECT id FROM t WHERE val > '150' ORDER BY id");
    assert_eq!(rows.len(), 2, "TEXT literal bound '150' must be coerced for INTEGER-affinity val");
    assert_eq!(rows[0].get(0).unwrap(), &vibesql_types::SqlValue::Integer(2));
    assert_eq!(rows[1].get(0).unwrap(), &vibesql_types::SqlValue::Integer(3));
}

/// A `WHERE`-clause equality probe on a BLOB/TEXT-affinity indexed column
/// must NOT match a differently-typed stored value.
#[test]
fn where_clause_text_literal_does_not_match_integer_on_blob_affinity_column() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE par(p)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX par_p_uniq ON par(p)").unwrap();
    exec(&mut db, "INSERT INTO par VALUES(1)").unwrap();

    let rows = select_rows(&db, "SELECT p FROM par WHERE p = '1'");
    assert!(rows.is_empty(), "TEXT '1' must not match stored INTEGER 1 on a BLOB-affinity column");

    let rows = select_rows(&db, "SELECT p FROM par WHERE p = 1");
    assert_eq!(rows.len(), 1, "INTEGER 1 literal must still match stored INTEGER 1");
}

/// Composite index mixing a BLOB-affinity column and an INTEGER-affinity
/// column: each column's affinity coercion must be applied independently
/// (not all-or-nothing across the composite key).
#[test]
fn composite_index_mixed_affinity_coerces_per_column() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(blob_col, int_col INTEGER)").unwrap();
    exec(&mut db, "CREATE UNIQUE INDEX t_composite ON t(blob_col, int_col)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(1, 100)").unwrap();

    // Second row uses TEXT '1' for the BLOB-affinity column (distinct key,
    // must be accepted) alongside a TEXT '100' for the INTEGER-affinity
    // column (would collide with row 1's int_col if coerced) but since
    // blob_col differs (1 vs '1'), the composite keys are distinct either way.
    exec(&mut db, "INSERT INTO t VALUES('1', 100)").unwrap();

    let rows = select_rows(&db, "SELECT blob_col, int_col FROM t");
    assert_eq!(rows.len(), 2);

    // WHERE probe: blob_col = '1' (TEXT, must match only the TEXT row) AND
    // int_col = '100' (TEXT literal, must coerce to INTEGER 100 and match).
    let rows = select_rows(&db, "SELECT blob_col FROM t WHERE blob_col = '1' AND int_col = '100'");
    assert_eq!(rows.len(), 1, "composite probe must coerce int_col but not blob_col");
    assert_eq!(
        rows[0].get(0).unwrap(),
        &vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("1"))
    );

    // A composite UNIQUE violation must still be detected when both
    // components collide after their own per-column coercion.
    let result = exec(&mut db, "INSERT INTO t VALUES(1, '100')");
    assert!(
        result.is_err(),
        "int_col '100' must coerce to 100 and collide with the existing (1, 100) composite key"
    );
}

/// Regression guard for the `try_pk_range_scan_with_early_projection` fast
/// path (`select::executor::fast_path::range_scan`), a second, independent
/// call site that builds BETWEEN bounds directly from the WHERE clause
/// rather than through `IndexPredicate`. TEXT literal bounds must still
/// coerce to the INTEGER-affinity PK's declared type.
#[test]
fn fast_path_pk_range_scan_coerces_text_between_bounds() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(pk INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(1, 10)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(2, 20)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(3, 30)").unwrap();

    // Simple column projection (not SELECT *) with a BETWEEN on the PK using
    // TEXT literal bounds is exactly the shape `try_pk_range_scan_with_early_projection`
    // targets.
    let rows = select_rows(&db, "SELECT v FROM t WHERE pk BETWEEN '1' AND '2'");
    assert_eq!(rows.len(), 2, "TEXT literal BETWEEN bounds must coerce to the INTEGER PK affinity");
    let values: Vec<i64> = rows
        .iter()
        .map(|r| match r.get(0).unwrap() {
            vibesql_types::SqlValue::Integer(i) => *i,
            other => panic!("expected Integer, got {other:?}"),
        })
        .collect();
    assert_eq!(values, vec![10, 20]);
}

/// Regression guard for `execute_streaming_aggregate`
/// (`select::executor::fast_path::streaming_agg`), a third independent call
/// site building BETWEEN bounds directly from the WHERE clause for an
/// inline-accumulated aggregate over a PK range scan.
#[test]
fn fast_path_streaming_aggregate_coerces_text_between_bounds() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(pk INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(1, 10)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(2, 20)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(3, 30)").unwrap();

    let rows = select_rows(&db, "SELECT SUM(v) FROM t WHERE pk BETWEEN '1' AND '2'");
    assert_eq!(rows.len(), 1);
    match rows[0].get(0).unwrap() {
        vibesql_types::SqlValue::Integer(sum) => {
            assert_eq!(*sum, 30, "TEXT literal BETWEEN bounds must coerce for streaming aggregate")
        }
        vibesql_types::SqlValue::Double(sum) => {
            assert_eq!(
                *sum, 30.0,
                "TEXT literal BETWEEN bounds must coerce for streaming aggregate"
            )
        }
        other => panic!("expected numeric SUM result, got {other:?}"),
    }
}
