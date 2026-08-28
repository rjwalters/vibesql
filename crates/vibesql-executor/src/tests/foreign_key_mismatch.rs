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
        Statement::Update(s) => crate::UpdateExecutor::execute(&s, db)
            .map(|count| format!("{} row(s) updated", count))
            .map_err(|e| e.to_string()),
        Statement::Delete(s) => crate::DeleteExecutor::execute(&s, db)
            .map(|count| format!("{} row(s) deleted", count))
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

    exec_sql(&mut db, "CREATE TABLE p20(x VARCHAR(10) COLLATE RTRIM PRIMARY KEY, y VARCHAR(20))")
        .unwrap();
    exec_sql(&mut db, "INSERT INTO p20 VALUES('abc', 'Alpha')").unwrap();
    exec_sql(&mut db, "CREATE TABLE c21(a VARCHAR(20), b VARCHAR(10) REFERENCES p20(x))").unwrap();

    // 'abc    ' should match parent 'abc' under RTRIM collation.
    let r = exec_sql(&mut db, "INSERT INTO c21 VALUES('alpha', 'abc    ')");
    assert!(r.is_ok(), "RTRIM-aware FK should succeed; got: {:?}", r);
}

#[test]
fn fk_uses_nocase_collation_for_parent_match() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE p_nc(x VARCHAR(10) COLLATE NOCASE PRIMARY KEY)").unwrap();
    exec_sql(&mut db, "INSERT INTO p_nc VALUES('Alpha')").unwrap();
    exec_sql(&mut db, "CREATE TABLE c_nc(a VARCHAR(10) REFERENCES p_nc(x))").unwrap();

    // Lowercase 'alpha' should match parent 'Alpha' under NOCASE.
    let r = exec_sql(&mut db, "INSERT INTO c_nc VALUES('alpha')");
    assert!(r.is_ok(), "NOCASE-aware FK should succeed; got: {:?}", r);
}

// -----------------------------------------------------------------------
// Statement-prepare-time FK schema validation (e_fkey-20.*, e_fkey-60.*):
// a broken FK schema definition (missing parent table, or a parent key not
// backed by a PK/UNIQUE/non-partial UNIQUE INDEX) must be reported when
// preparing *any* DML against either the child or the parent table — even
// when the statement ends up touching zero rows. EVIDENCE-OF R-45488-08504 /
// R-48391-38472.
// -----------------------------------------------------------------------

#[test]
fn delete_from_child_with_missing_parent_table_errors_even_with_zero_rows() {
    // c(x REFERENCES nosuchtable) — the table is empty, so DELETE FROM c
    // matches zero rows. The per-row FK loop never runs, so only the
    // statement-prepare-time check can catch this.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE c(x REFERENCES nosuchtable)").unwrap();

    let err = exec_sql(&mut db, "DELETE FROM c").unwrap_err();
    assert!(
        err.contains("nosuchtable") || err.to_lowercase().contains("not found"),
        "expected a missing-table error, got: {}",
        err
    );
}

#[test]
fn update_on_child_with_missing_parent_table_errors_even_with_zero_rows() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE c(x REFERENCES nosuchtable, y)").unwrap();

    let err = exec_sql(&mut db, "UPDATE c SET x = 1").unwrap_err();
    assert!(
        err.contains("nosuchtable") || err.to_lowercase().contains("not found"),
        "expected a missing-table error, got: {}",
        err
    );
}

#[test]
fn delete_from_parent_with_mismatched_child_key_errors() {
    // p(a PRIMARY KEY, b) — only `a` is a valid FK target. c references
    // p(b), which is not backed by any PK/UNIQUE — a schema-level mismatch.
    // DELETE FROM p must report it even before any row-existence scan runs.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    // Seed `p` *before* creating the broken child `c`. (A single-row
    // `INSERT INTO p VALUES(...)` would in fact still succeed after `c`
    // exists — see
    // `insert_into_parent_with_mismatched_child_key_depends_on_statement_form`
    // — but seeding first keeps this test independent of that nuance.)
    exec_sql(&mut db, "CREATE TABLE p(a PRIMARY KEY, b)").unwrap();
    exec_sql(&mut db, "INSERT INTO p VALUES(1, 2)").unwrap();
    exec_sql(&mut db, "CREATE TABLE c(x REFERENCES p(b))").unwrap();

    let err = exec_sql(&mut db, "DELETE FROM p").unwrap_err();
    assert!(
        err.contains("foreign key mismatch") && err.contains("\"c\"") && err.contains("\"p\""),
        "expected mismatch wording, got: {}",
        err
    );
}

#[test]
fn update_on_parent_with_mismatched_child_key_errors() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    // Seed `p` before creating the broken child `c` (see the comment in
    // `delete_from_parent_with_mismatched_child_key_errors` above).
    exec_sql(&mut db, "CREATE TABLE p(a PRIMARY KEY, b)").unwrap();
    exec_sql(&mut db, "INSERT INTO p VALUES(1, 2)").unwrap();
    exec_sql(&mut db, "CREATE TABLE c(x REFERENCES p(b))").unwrap();

    let err = exec_sql(&mut db, "UPDATE p SET b = 3").unwrap_err();
    assert!(
        err.contains("foreign key mismatch") && err.contains("\"c\"") && err.contains("\"p\""),
        "expected mismatch wording, got: {}",
        err
    );
}

#[test]
fn insert_into_parent_with_mismatched_child_key_depends_on_statement_form() {
    // Parent-side prepare-time detection is *statement-form sensitive* in real
    // SQLite, not unconditional. SQLite's `sqlite3FkCheck()` skips its entire
    // parent-side loop — including the `sqlite3FkLocateIndex()` call that
    // raises "foreign key mismatch" — for a single-row, top-level,
    // non-multi-write INSERT, on the grounds that "inserting a single row into
    // a parent table cannot cause (or fix) an immediate foreign key
    // violation".
    //
    // This test originally (#6341) asserted that the plain `INSERT INTO p
    // VALUES(1, 2)` form errors. That was an over-broad artifact of VibeSQL
    // firing the descendant walk on every INSERT; empirically verified against
    // real `sqlite3` 3.51.0 with exactly this schema:
    //
    //     sqlite> PRAGMA foreign_keys=ON;
    //     sqlite> CREATE TABLE p(a PRIMARY KEY, b);
    //     sqlite> CREATE TABLE c(x REFERENCES p(b));
    //     sqlite> INSERT INTO p VALUES(1, 2);            -- succeeds
    //     sqlite> INSERT INTO p VALUES(1, 2),(3, 4);     -- foreign key mismatch - "c" referencing
    // "p"     sqlite> INSERT INTO p SELECT 1, 2;             -- foreign key mismatch - "c"
    // referencing "p"
    //
    // SQLite's own conformance suite encodes the same contrast: e_fkey-19.2
    // and e_fkey-21.2 insert into the parent with single-row VALUES and expect
    // success, while e_fkey-20.$tn.6 deliberately switches to
    // `INSERT INTO $ptbl SELECT ?, ?` to get the mismatch. So the assertion
    // below is inverted for the VALUES form and the multi-row / SELECT forms
    // now carry the "parent side is still detected" intent this test was
    // written for (Part of #6170).
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE p(a PRIMARY KEY, b)").unwrap();
    exec_sql(&mut db, "CREATE TABLE c(x REFERENCES p(b))").unwrap();

    // Single-row VALUES: SQLite's skip path — must succeed.
    let r = exec_sql(&mut db, "INSERT INTO p VALUES(1, 2)");
    assert!(r.is_ok(), "single-row INSERT ... VALUES into the parent should succeed, got: {:?}", r);

    // Multi-row VALUES is a multi-write statement in SQLite (the VALUES list is
    // parsed as a compound SELECT) — the broken child is reported.
    let err = exec_sql(&mut db, "INSERT INTO p VALUES(3, 4), (5, 6)").unwrap_err();
    assert!(
        err.contains("foreign key mismatch") && err.contains("\"c\"") && err.contains("\"p\""),
        "expected mismatch wording for multi-row VALUES, got: {}",
        err
    );

    // INSERT ... SELECT is likewise multi-write — the broken child is reported.
    let err = exec_sql(&mut db, "INSERT INTO p SELECT 7, 8").unwrap_err();
    assert!(
        err.contains("foreign key mismatch") && err.contains("\"c\"") && err.contains("\"p\""),
        "expected mismatch wording for INSERT ... SELECT, got: {}",
        err
    );
}

#[test]
fn dml_on_unrelated_table_is_unaffected_by_broken_fk_elsewhere() {
    // A broken FK relationship between p/c must not affect DML against a
    // completely unrelated table `t` — only statements that touch the
    // specific child or parent table involved in the broken relationship.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE p(a PRIMARY KEY, b)").unwrap();
    exec_sql(&mut db, "CREATE TABLE c(x REFERENCES p(b))").unwrap();
    exec_sql(&mut db, "CREATE TABLE t(v)").unwrap();

    let r = exec_sql(&mut db, "INSERT INTO t VALUES(1)");
    assert!(r.is_ok(), "unrelated table INSERT should be unaffected, got: {:?}", r);
    let r = exec_sql(&mut db, "UPDATE t SET v = 2");
    assert!(r.is_ok(), "unrelated table UPDATE should be unaffected, got: {:?}", r);
    let r = exec_sql(&mut db, "DELETE FROM t");
    assert!(r.is_ok(), "unrelated table DELETE should be unaffected, got: {:?}", r);
}

#[test]
fn dml_on_valid_fk_schema_is_unaffected_by_prepare_time_check() {
    // Sanity check: a *valid* FK schema (parent key backed by a real PK)
    // must not be spuriously rejected by the new prepare-time validation.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);

    exec_sql(&mut db, "CREATE TABLE p(a PRIMARY KEY, b)").unwrap();
    exec_sql(&mut db, "CREATE TABLE c(x REFERENCES p(a))").unwrap();
    exec_sql(&mut db, "INSERT INTO p VALUES(1, 2)").unwrap();

    let r = exec_sql(&mut db, "INSERT INTO c VALUES(1)");
    assert!(r.is_ok(), "valid FK should succeed, got: {:?}", r);
    let r = exec_sql(&mut db, "UPDATE p SET b = 3");
    assert!(r.is_ok(), "valid FK UPDATE on parent should succeed, got: {:?}", r);
    let r = exec_sql(&mut db, "DELETE FROM c");
    assert!(r.is_ok(), "valid FK DELETE on child should succeed, got: {:?}", r);
}
