//! End-to-end regression tests for issue #5834: CHECK constraints and
//! FOREIGN KEY constraints (including referential actions) must enforce
//! identically before and after a binary `.vbsql` save/reload — the
//! cross-process reopen path used by the CLI and the TCL harness.
//!
//! Before the fix, the binary catalog load path rebuilt only columns +
//! primary key + `sql_source`, silently dropping
//! `TableSchema::check_constraints` and `TableSchema::foreign_keys`:
//! violating INSERTs succeeded after reopen, `PRAGMA foreign_key_list`
//! returned nothing, and ON DELETE/UPDATE actions never fired — while
//! `sqlite_master.sql` still showed the constraint text. The load path now
//! rehydrates both by re-parsing the persisted `sql_source`.

use vibesql_ast::Statement;
use vibesql_catalog::ReferentialAction;
use vibesql_executor::{
    CreateTableExecutor, DeleteExecutor, InsertExecutor, SelectExecutor, UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Create a table preserving the verbatim source text (issue #5619), the way
/// the CLI/load paths capture it. sql_source is what the reload path
/// re-parses, so tests must go through this entry point.
fn create_with_source(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

/// Execute a DML statement, returning Ok(()) or the error's Display text.
fn exec(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("parse error: {e:?}"))?;
    match stmt {
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        other => panic!("unsupported statement in test: {other:?}"),
    }
}

/// COUNT(*) helper.
fn count(db: &Database, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let stmt = Parser::parse_sql(&sql).expect("parse SELECT");
    let Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    match &result.rows[0].values[0] {
        vibesql_types::SqlValue::Integer(n) => *n,
        vibesql_types::SqlValue::Bigint(n) => *n,
        other => panic!("unexpected COUNT value: {other:?}"),
    }
}

/// Save to a binary `.vbsql` file and reload — the cross-process reopen path.
/// Re-enables FK enforcement on the reloaded handle, mirroring the TCL shim's
/// per-invocation `PRAGMA foreign_keys=ON` replay (the pragma itself is
/// per-connection state in SQLite, not part of the file).
fn reopen_binary(db: &Database, tag: &str) -> Database {
    let path =
        std::env::temp_dir().join(format!("vibesql_5834_{tag}_{}.vbsql", std::process::id()));
    db.save_binary(&path).expect("save_binary");
    let mut reloaded = Database::load_binary(&path).expect("load_binary");
    std::fs::remove_file(&path).ok();
    reloaded.set_foreign_keys_enabled(true);
    reloaded
}

// ---------------------------------------------------------------------------
// FK enforcement after reopen
// ---------------------------------------------------------------------------

#[test]
fn fk_insert_violation_errors_after_reopen() {
    // The exact two-process repro from issue #5834 / the #5783 curator probe.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER)");
    create_with_source(&mut db, "CREATE TABLE t2(c INTEGER REFERENCES t1(a), d INTEGER)");

    let mut db2 = reopen_binary(&db, "insert_violation");

    // Constraint metadata must survive the reload (what PRAGMA
    // foreign_key_list reads).
    let schema = db2.catalog.get_table("t2").expect("t2 exists after reload");
    assert_eq!(schema.foreign_keys.len(), 1, "FK must be rehydrated on reload");
    assert_eq!(schema.foreign_keys[0].parent_table, "t1");
    assert_eq!(schema.foreign_keys[0].parent_column_names, vec!["a".to_string()]);

    // Violating insert must error in the fresh process (SQLite errors here;
    // pre-fix VibeSQL accepted the row).
    let err = exec(&mut db2, "INSERT INTO t2 VALUES(1, 3)")
        .expect_err("orphan insert must fail after reopen");
    assert!(
        err.to_uppercase().contains("FOREIGN KEY"),
        "expected FK violation wording, got: {err}"
    );

    // A satisfying parent row makes the same insert succeed.
    exec(&mut db2, "INSERT INTO t1 VALUES(1, 0)").expect("parent insert");
    exec(&mut db2, "INSERT INTO t2 VALUES(1, 3)").expect("child insert with parent present");
}

#[test]
fn fk_on_delete_cascade_fires_after_reopen() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE p(x INTEGER PRIMARY KEY)");
    create_with_source(
        &mut db,
        "CREATE TABLE c(y INTEGER REFERENCES p(x) ON DELETE CASCADE, z INTEGER)",
    );
    exec(&mut db, "INSERT INTO p VALUES(1)").unwrap();
    exec(&mut db, "INSERT INTO p VALUES(2)").unwrap();
    exec(&mut db, "INSERT INTO c VALUES(1, 10)").unwrap();
    exec(&mut db, "INSERT INTO c VALUES(2, 20)").unwrap();

    let mut db2 = reopen_binary(&db, "cascade");

    // NOTE: deliberately no COUNT(*) on `c` before the DELETE. FK cascade
    // actions mutate the child table without invalidating the columnar
    // cache, so a pre-primed cached COUNT would read stale data. That is a
    // pre-existing in-process bug (present without any save/reload),
    // tracked separately as issue #5876 — this test targets reopen
    // enforcement only.
    exec(&mut db2, "DELETE FROM p WHERE x = 1").expect("parent delete");
    assert_eq!(count(&db2, "c"), 1, "ON DELETE CASCADE must fire after reopen");
    assert_eq!(count(&db2, "p"), 1);
}

#[test]
fn fk_parent_delete_restricted_after_reopen() {
    // Default NO ACTION: deleting a referenced parent row must error.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE p(x INTEGER PRIMARY KEY)");
    create_with_source(&mut db, "CREATE TABLE c(y INTEGER REFERENCES p(x))");
    exec(&mut db, "INSERT INTO p VALUES(1)").unwrap();
    exec(&mut db, "INSERT INTO c VALUES(1)").unwrap();

    let mut db2 = reopen_binary(&db, "restrict");
    let err = exec(&mut db2, "DELETE FROM p WHERE x = 1")
        .expect_err("referenced parent delete must fail after reopen");
    assert!(
        err.to_uppercase().contains("FOREIGN KEY"),
        "expected FK violation wording, got: {err}"
    );
}

#[test]
fn fk_on_update_set_null_fires_after_reopen() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE p(x INTEGER PRIMARY KEY)");
    create_with_source(&mut db, "CREATE TABLE c(y INTEGER REFERENCES p(x) ON UPDATE SET NULL)");
    exec(&mut db, "INSERT INTO p VALUES(1)").unwrap();
    exec(&mut db, "INSERT INTO c VALUES(1)").unwrap();

    let mut db2 = reopen_binary(&db, "set_null");
    exec(&mut db2, "UPDATE p SET x = 5 WHERE x = 1").expect("parent update");

    let sql = "SELECT COUNT(*) FROM c WHERE y IS NULL";
    let Statement::Select(select) = Parser::parse_sql(sql).unwrap() else { unreachable!() };
    let result = SelectExecutor::new(&db2).execute_with_columns(&select).expect("SELECT");
    assert_eq!(
        result.rows[0].values[0],
        vibesql_types::SqlValue::Integer(1),
        "ON UPDATE SET NULL must fire after reopen"
    );
}

#[test]
fn all_referential_actions_round_trip_in_metadata() {
    let actions = [
        ("CASCADE", ReferentialAction::Cascade),
        ("SET NULL", ReferentialAction::SetNull),
        ("SET DEFAULT", ReferentialAction::SetDefault),
        ("RESTRICT", ReferentialAction::Restrict),
        ("NO ACTION", ReferentialAction::NoAction),
    ];

    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE p(x INTEGER PRIMARY KEY)");
    for (i, (sql_action, _)) in actions.iter().enumerate() {
        create_with_source(
            &mut db,
            &format!(
                "CREATE TABLE c{i}(y INTEGER REFERENCES p(x) \
                 ON DELETE {sql_action} ON UPDATE {sql_action})"
            ),
        );
    }

    let db2 = reopen_binary(&db, "actions");
    for (i, (sql_action, expected)) in actions.iter().enumerate() {
        let schema = db2.catalog.get_table(&format!("c{i}")).expect("child table");
        assert_eq!(schema.foreign_keys.len(), 1, "c{i} must keep its FK");
        let fk = &schema.foreign_keys[0];
        assert_eq!(&fk.on_delete, expected, "ON DELETE {sql_action} must survive reload");
        assert_eq!(&fk.on_update, expected, "ON UPDATE {sql_action} must survive reload");
    }
}

#[test]
fn composite_fk_enforces_after_reopen() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE p(a INTEGER, b INTEGER, PRIMARY KEY(a, b))");
    create_with_source(
        &mut db,
        "CREATE TABLE c(x INTEGER, y INTEGER, FOREIGN KEY(x, y) REFERENCES p(a, b))",
    );
    exec(&mut db, "INSERT INTO p VALUES(1, 2)").unwrap();

    let mut db2 = reopen_binary(&db, "composite");

    let schema = db2.catalog.get_table("c").expect("c exists");
    assert_eq!(schema.foreign_keys.len(), 1);
    assert_eq!(schema.foreign_keys[0].column_names, vec!["x".to_string(), "y".to_string()]);

    exec(&mut db2, "INSERT INTO c VALUES(1, 2)").expect("matching composite key");
    let err = exec(&mut db2, "INSERT INTO c VALUES(1, 3)")
        .expect_err("non-matching composite key must fail after reopen");
    assert!(err.to_uppercase().contains("FOREIGN KEY"), "got: {err}");
}

#[test]
fn self_referential_fk_enforces_after_reopen() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(
        &mut db,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES t(id))",
    );
    exec(&mut db, "INSERT INTO t VALUES(1, NULL)").unwrap();

    let mut db2 = reopen_binary(&db, "self_ref");
    exec(&mut db2, "INSERT INTO t VALUES(2, 1)").expect("valid self-ref");
    let err = exec(&mut db2, "INSERT INTO t VALUES(3, 99)")
        .expect_err("dangling self-ref must fail after reopen");
    assert!(err.to_uppercase().contains("FOREIGN KEY"), "got: {err}");
}

#[test]
fn deferrable_initially_deferred_survives_reopen() {
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE p(x INTEGER PRIMARY KEY)");
    create_with_source(
        &mut db,
        "CREATE TABLE c(y INTEGER REFERENCES p(x) DEFERRABLE INITIALLY DEFERRED)",
    );

    let db2 = reopen_binary(&db, "deferred");
    let fk = &db2.catalog.get_table("c").expect("c exists").foreign_keys[0];
    assert!(fk.is_deferrable, "DEFERRABLE must survive reload");
    assert!(fk.initially_deferred, "INITIALLY DEFERRED must survive reload");
}

#[test]
fn child_stored_before_parent_still_resolves_after_reopen() {
    // Table order in the binary file is not creation order; parent column
    // indices must resolve even when the child precedes its parent. Creating
    // the child first also exercises the SQLite behavior of storing FK
    // metadata before the parent exists.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE child(y INTEGER REFERENCES parent(x))");
    create_with_source(&mut db, "CREATE TABLE parent(pad INTEGER, x INTEGER PRIMARY KEY)");
    exec(&mut db, "INSERT INTO parent VALUES(0, 7)").unwrap();

    let mut db2 = reopen_binary(&db, "order");
    exec(&mut db2, "INSERT INTO child VALUES(7)").expect("valid child row");
    let err = exec(&mut db2, "INSERT INTO child VALUES(8)")
        .expect_err("orphan child row must fail after reopen");
    assert!(err.to_uppercase().contains("FOREIGN KEY"), "got: {err}");
}

// ---------------------------------------------------------------------------
// CHECK enforcement after reopen
// ---------------------------------------------------------------------------

#[test]
fn column_level_check_enforces_after_reopen() {
    // Mirrors the istrue-521..524 failure class: CHECK silently dropped on
    // reload, so violating inserts succeeded in a fresh process.
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INTEGER CHECK(a > 0))");
    exec(&mut db, "INSERT INTO t VALUES(1)").unwrap();

    let mut db2 = reopen_binary(&db, "check_col");
    let schema = db2.catalog.get_table("t").expect("t exists");
    assert_eq!(schema.check_constraints.len(), 1, "CHECK must be rehydrated on reload");

    exec(&mut db2, "INSERT INTO t VALUES(2)").expect("passing row");
    let err = exec(&mut db2, "INSERT INTO t VALUES(-1)")
        .expect_err("CHECK violation must fail after reopen");
    assert!(err.to_uppercase().contains("CHECK"), "got: {err}");
    assert_eq!(count(&db2, "t"), 2);
}

#[test]
fn named_table_level_check_enforces_after_reopen() {
    let mut db = Database::new();
    create_with_source(
        &mut db,
        "CREATE TABLE t(a INTEGER, b INTEGER, CONSTRAINT ab_sum CHECK(a + b < 100))",
    );

    let mut db2 = reopen_binary(&db, "check_table");
    let schema = db2.catalog.get_table("t").expect("t exists");
    assert_eq!(schema.check_constraints.len(), 1);
    assert_eq!(schema.check_constraints[0].0, "ab_sum", "explicit CHECK name must survive");

    exec(&mut db2, "INSERT INTO t VALUES(1, 2)").expect("passing row");
    let err = exec(&mut db2, "INSERT INTO t VALUES(60, 60)")
        .expect_err("named CHECK violation must fail after reopen");
    assert!(
        err.to_uppercase().contains("CHECK") && err.contains("ab_sum"),
        "error should name the constraint, got: {err}"
    );
}

#[test]
fn check_enforcement_matches_before_and_after_reopen() {
    // Acceptance criterion: identical enforcement pre/post reopen.
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t(a INTEGER CHECK(a BETWEEN 1 AND 10))");

    let before = exec(&mut db, "INSERT INTO t VALUES(11)").expect_err("violates before save");

    let mut db2 = reopen_binary(&db, "check_parity");
    let after = exec(&mut db2, "INSERT INTO t VALUES(11)").expect_err("violates after reload");

    assert_eq!(before, after, "CHECK errors must be identical before and after reopen");
}
