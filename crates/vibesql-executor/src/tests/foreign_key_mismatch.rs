//! Tests for `foreign key mismatch` error class (issue #5084).
//!
//! When an FK references a parent column set that is not covered by a
//! PRIMARY KEY, UNIQUE constraint, or non-partial UNIQUE INDEX, SQLite
//! raises `foreign key mismatch - "<child>" referencing "<parent>"`. The
//! check runs before any row-existence test, so it fires even when the
//! parent table is empty.
//!
//! These tests also cover the RTRIM / NOCASE collation-aware comparison
//! that the FK validators use during row-existence checks.

use vibesql_ast::Statement;
use vibesql_storage::Database;

/// Helper to parse and execute SQL — returns either the formatted result or
/// the `Display` form of the executor error so we can assert on the
/// SQLite-compatible wording.
fn exec_sql(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt =
        vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::Insert(s) => crate::InsertExecutor::execute(db, &s)
            .map(|count| format!("{} row(s) inserted", count))
            .map_err(|e| e.to_string()),
        Statement::CreateIndex(s) => crate::CreateIndexExecutor::execute(&s, db)
            .map(|_| "ok".to_string())
            .map_err(|e| e.to_string()),
        _ => Err(format!("Unsupported statement type: {:?}", sql)),
    }
}

#[test]
fn fk_mismatch_when_parent_column_not_unique() {
    // Mirrors fkey5-11.0/11.1 in SQLite's TCL suite.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE tt(y)").unwrap();
    exec_sql(&mut db, "CREATE TABLE c11(x REFERENCES tt(y))").unwrap();

    // Insert into the child should now fail with the mismatch error even
    // though the parent table is empty — because `tt.y` has no UNIQUE/PK.
    let err = exec_sql(&mut db, "INSERT INTO c11 VALUES(1)").unwrap_err();
    assert!(
        err.contains("foreign key mismatch") && err.contains("\"c11\"") && err.contains("\"tt\""),
        "expected mismatch wording, got: {}",
        err
    );
}

#[test]
fn fk_mismatch_when_only_partial_unique_index_exists() {
    // Mirrors fkey1-6.0/6.1: a UNIQUE INDEX with a WHERE clause does not
    // qualify as a FK target — SQLite's `sqlite3FkLocateIndex` requires the
    // index to cover every parent row. After issue #5181, VibeSQL records
    // the partial-index predicate on `IndexMetadata` and the FK-mismatch
    // checker (`foreign_key_check::parent_has_matching_key`) explicitly
    // rejects partial UNIQUE indexes as FK targets.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE p1(x, y)").unwrap();
    exec_sql(&mut db, "INSERT INTO p1 VALUES(1, 1)").unwrap();
    exec_sql(&mut db, "CREATE TABLE c1(a REFERENCES p1(x))").unwrap();
    // Add a partial unique index on x — this must NOT satisfy the FK target.
    exec_sql(&mut db, "CREATE UNIQUE INDEX p1x ON p1(x) WHERE y<2").unwrap();

    let err = exec_sql(&mut db, "INSERT INTO c1 VALUES(1)").unwrap_err();
    assert!(
        err.contains("foreign key mismatch") && err.contains("\"c1\"") && err.contains("\"p1\""),
        "expected mismatch wording, got: {}",
        err
    );
}

#[test]
fn fk_succeeds_when_full_unique_index_added_after_partial() {
    // fkey1-6.2: starting from the partial-index-only state, adding a full
    // unique index (`p1x2 ON p1(x)`) makes the FK valid. Insert into the
    // child should then succeed.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE p1(x, y)").unwrap();
    exec_sql(&mut db, "INSERT INTO p1 VALUES(1, 1)").unwrap();
    exec_sql(&mut db, "CREATE TABLE c1(a REFERENCES p1(x))").unwrap();
    exec_sql(&mut db, "CREATE UNIQUE INDEX p1x ON p1(x) WHERE y<2").unwrap();
    // FK still fails with only the partial index.
    let err = exec_sql(&mut db, "INSERT INTO c1 VALUES(1)").unwrap_err();
    assert!(err.contains("foreign key mismatch"), "expected mismatch, got: {}", err);

    // Add a full unique index — FK target now satisfied.
    exec_sql(&mut db, "CREATE UNIQUE INDEX p1x2 ON p1(x)").unwrap();
    let r = exec_sql(&mut db, "INSERT INTO c1 VALUES(1)");
    assert!(r.is_ok(), "expected success after full UNIQUE INDEX; got: {:?}", r);
}

#[test]
fn fk_mismatch_resolved_by_creating_unique_index() {
    // After we add a non-partial UNIQUE INDEX on the parent column, the
    // FK insert succeeds. Mirrors fkey1-6.2.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE p1(x, y)").unwrap();
    exec_sql(&mut db, "INSERT INTO p1 VALUES(1, 1)").unwrap();
    exec_sql(&mut db, "CREATE TABLE c1(a REFERENCES p1(x))").unwrap();
    exec_sql(&mut db, "CREATE UNIQUE INDEX p1x2 ON p1(x)").unwrap();

    // Now FK should succeed because p1(x) has a full UNIQUE INDEX.
    let r = exec_sql(&mut db, "INSERT INTO c1 VALUES(1)");
    assert!(r.is_ok(), "expected success after UNIQUE INDEX; got: {:?}", r);
}

#[test]
fn fk_succeeds_with_primary_key_target() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE parent(id INT PRIMARY KEY, name VARCHAR(50))").unwrap();
    exec_sql(&mut db, "INSERT INTO parent VALUES(1, 'a'), (2, 'b')").unwrap();
    exec_sql(&mut db, "CREATE TABLE child(pid INT REFERENCES parent(id))").unwrap();

    let r = exec_sql(&mut db, "INSERT INTO child VALUES(1)");
    assert!(r.is_ok(), "FK against PRIMARY KEY should succeed; got: {:?}", r);
}

#[test]
fn fk_succeeds_with_unique_constraint_target() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE parent(id INT, code VARCHAR(10) UNIQUE)").unwrap();
    exec_sql(&mut db, "INSERT INTO parent VALUES(1, 'A'), (2, 'B')").unwrap();
    exec_sql(&mut db, "CREATE TABLE child(c VARCHAR(10) REFERENCES parent(code))").unwrap();

    let r = exec_sql(&mut db, "INSERT INTO child VALUES('A')");
    assert!(r.is_ok(), "FK against UNIQUE column should succeed; got: {:?}", r);
}

#[test]
fn fk_uses_rtrim_collation_for_parent_match() {
    // Mirrors fkey5-8.4: when the parent column has RTRIM collation, FK
    // value comparison must trim trailing spaces. The child value here is
    // `'abc    '` with trailing spaces, parent has `'abc'` — match should
    // succeed under RTRIM.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(
        &mut db,
        "CREATE TABLE p20(x VARCHAR(10) COLLATE RTRIM PRIMARY KEY, y VARCHAR(20))",
    )
    .unwrap();
    exec_sql(&mut db, "INSERT INTO p20 VALUES('abc', 'Alpha')").unwrap();
    exec_sql(
        &mut db,
        "CREATE TABLE c21(a VARCHAR(20), b VARCHAR(10) REFERENCES p20(x))",
    )
    .unwrap();

    // 'abc    ' should match parent 'abc' under RTRIM collation.
    let r = exec_sql(&mut db, "INSERT INTO c21 VALUES('alpha', 'abc    ')");
    assert!(r.is_ok(), "RTRIM-aware FK should succeed; got: {:?}", r);
}

#[test]
fn fk_uses_nocase_collation_for_parent_match() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(
        &mut db,
        "CREATE TABLE p_nc(x VARCHAR(10) COLLATE NOCASE PRIMARY KEY)",
    )
    .unwrap();
    exec_sql(&mut db, "INSERT INTO p_nc VALUES('Alpha')").unwrap();
    exec_sql(
        &mut db,
        "CREATE TABLE c_nc(a VARCHAR(10) REFERENCES p_nc(x))",
    )
    .unwrap();

    // Lowercase 'alpha' should match parent 'Alpha' under NOCASE.
    let r = exec_sql(&mut db, "INSERT INTO c_nc VALUES('alpha')");
    assert!(r.is_ok(), "NOCASE-aware FK should succeed; got: {:?}", r);
}
