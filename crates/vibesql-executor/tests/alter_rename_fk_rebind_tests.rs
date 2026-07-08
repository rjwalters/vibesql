//! End-to-end regression tests for issue #5873: `ALTER TABLE <parent> RENAME TO
//! <new>` must re-bind every child table's foreign key that referenced the old
//! parent name — both the in-memory `ForeignKeyConstraint::parent_table` and the
//! verbatim `sqlite_master.sql` `REFERENCES` clause — so cascade enforcement
//! survives the rename and a save/reload round-trip.
//!
//! Before the fix, `execute_rename_table` rewrote only the renamed table's own
//! `sql_source` and its trigger metadata; every child's FK kept pointing at the
//! now-nonexistent old name, silently severing enforcement, and a reload (which
//! rehydrates constraints from `sql_source`, see #5834/#5895) would resurrect the
//! stale binding from the un-rewritten `REFERENCES` text.

use vibesql_ast::Statement;
use vibesql_executor::{
    AlterTableExecutor, CreateTableExecutor, DeleteExecutor, InsertExecutor, SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Create a table preserving the verbatim source text (issue #5619). `sql_source`
/// is what the reload path re-parses, so tests must go through this entry point.
fn create_with_source(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE");
    let Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

/// Run an ALTER, passing the verbatim statement text (the path the CLI uses).
fn alter(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse ALTER");
    let Statement::AlterTable(a) = stmt else {
        panic!("expected ALTER TABLE");
    };
    AlterTableExecutor::execute_with_source(&a, db, Some(sql)).expect("ALTER TABLE");
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

/// Return the single `sql` text for the named table from `sqlite_master`.
fn table_sql(db: &Database, table: &str) -> String {
    let query = format!("SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'");
    let stmt = Parser::parse_sql(&query).expect("parse SELECT");
    let Statement::Select(select) = stmt else {
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

/// Save to a binary `.vbsql` file and reload — the cross-process reopen path.
/// Re-enables FK enforcement on the reloaded handle, mirroring the TCL shim's
/// per-invocation `PRAGMA foreign_keys=ON` replay (per-connection state).
fn reopen_binary(db: &Database, tag: &str) -> Database {
    let path =
        std::env::temp_dir().join(format!("vibesql_5873_{tag}_{}.vbsql", std::process::id()));
    db.save_binary(&path).expect("save_binary");
    let mut reloaded = Database::load_binary(&path).expect("load_binary");
    std::fs::remove_file(&path).ok();
    reloaded.set_foreign_keys_enabled(true);
    reloaded
}

#[test]
fn rename_parent_rebinds_child_fk_metadata_and_sql_source() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE p(id INTEGER PRIMARY KEY)");
    create_with_source(&mut db, "CREATE TABLE c(x REFERENCES p(id) ON DELETE CASCADE)");

    alter(&mut db, "ALTER TABLE p RENAME TO p_new");

    // In-memory FK metadata (what PRAGMA foreign_key_list reads) now points at
    // the new name, in both the storage and catalog copies of the schema.
    let schema = db.catalog.get_table("c").expect("c exists");
    assert_eq!(schema.foreign_keys.len(), 1);
    assert_eq!(
        schema.foreign_keys[0].parent_table, "p_new",
        "child FK parent_table must follow the rename"
    );

    // Verbatim sqlite_master.sql REFERENCES clause is rewritten to the
    // double-quoted new name, matching sqlite3.
    assert_eq!(table_sql(&db, "c"), "CREATE TABLE c(x REFERENCES \"p_new\"(id) ON DELETE CASCADE)");
}

#[test]
fn rename_parent_cascade_fires_in_process() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE p(id INTEGER PRIMARY KEY)");
    create_with_source(&mut db, "CREATE TABLE c(x REFERENCES p(id) ON DELETE CASCADE)");
    exec(&mut db, "INSERT INTO p VALUES(1)").unwrap();
    exec(&mut db, "INSERT INTO c VALUES(1)").unwrap();

    alter(&mut db, "ALTER TABLE p RENAME TO p_new");

    // Deleting the parent under its new name must cascade into the child; the
    // pre-fix behavior left an orphan row (cascade lost, no error).
    exec(&mut db, "DELETE FROM p_new WHERE id = 1").expect("parent delete");
    assert_eq!(count(&db, "c"), 0, "ON DELETE CASCADE must fire after rename");
}

#[test]
fn rename_parent_survives_binary_reload_and_cascades() {
    // The reload path rehydrates FK constraints from sql_source; a stale
    // REFERENCES text would resurrect the old binding. Verify the rewritten text
    // round-trips and enforcement still fires in a fresh handle.
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE p(id INTEGER PRIMARY KEY)");
    create_with_source(&mut db, "CREATE TABLE c(x REFERENCES p(id) ON DELETE CASCADE)");
    exec(&mut db, "INSERT INTO p VALUES(1)").unwrap();
    exec(&mut db, "INSERT INTO c VALUES(1)").unwrap();

    alter(&mut db, "ALTER TABLE p RENAME TO p_new");

    let mut db2 = reopen_binary(&db, "reload_cascade");

    let schema = db2.catalog.get_table("c").expect("c exists after reload");
    assert_eq!(
        schema.foreign_keys[0].parent_table, "p_new",
        "rehydrated FK must bind to the new parent name, not the stale old one"
    );
    assert_eq!(
        table_sql(&db2, "c"),
        "CREATE TABLE c(x REFERENCES \"p_new\"(id) ON DELETE CASCADE)"
    );

    exec(&mut db2, "DELETE FROM p_new WHERE id = 1").expect("parent delete after reopen");
    assert_eq!(count(&db2, "c"), 0, "cascade must fire after rename + reopen");
}

#[test]
fn rename_parent_rebinds_multiple_children() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE p(id INTEGER PRIMARY KEY)");
    create_with_source(&mut db, "CREATE TABLE c1(x REFERENCES p(id))");
    create_with_source(&mut db, "CREATE TABLE c2(y REFERENCES p(id))");

    alter(&mut db, "ALTER TABLE p RENAME TO p_new");

    for child in ["c1", "c2"] {
        let schema = db.catalog.get_table(child).expect("child exists");
        assert_eq!(
            schema.foreign_keys[0].parent_table, "p_new",
            "child {child} must be re-bound to the renamed parent"
        );
    }
}

#[test]
fn rename_parent_rebinds_two_fks_in_one_child() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE p(id INTEGER PRIMARY KEY)");
    create_with_source(&mut db, "CREATE TABLE c(a REFERENCES p(id), b REFERENCES p(id))");

    alter(&mut db, "ALTER TABLE p RENAME TO p_new");

    let schema = db.catalog.get_table("c").expect("c exists");
    assert_eq!(schema.foreign_keys.len(), 2);
    assert!(
        schema.foreign_keys.iter().all(|fk| fk.parent_table == "p_new"),
        "both FKs in the child must be re-bound"
    );
    assert_eq!(
        table_sql(&db, "c"),
        "CREATE TABLE c(a REFERENCES \"p_new\"(id), b REFERENCES \"p_new\"(id))"
    );
}

#[test]
fn rename_self_referential_fk_follows_rename() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    create_with_source(&mut db, "CREATE TABLE p(id INTEGER PRIMARY KEY, pid REFERENCES p(id))");

    alter(&mut db, "ALTER TABLE p RENAME TO p_new");

    // The renamed table's own self-referential FK must point at the new name,
    // and its verbatim REFERENCES clause must be rewritten (the header was
    // already rewritten by the table-name rename; the inline reference was not).
    let schema = db.catalog.get_table("p_new").expect("p_new exists");
    assert_eq!(schema.foreign_keys.len(), 1);
    assert_eq!(schema.foreign_keys[0].parent_table, "p_new");
    assert_eq!(
        table_sql(&db, "p_new"),
        "CREATE TABLE \"p_new\"(id INTEGER PRIMARY KEY, pid REFERENCES \"p_new\"(id))"
    );
}
