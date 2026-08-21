//! Regression tests for fkey2-14.1.4/1.5/1.6 (Part of #6170): SQLite rejects
//! `ALTER TABLE ... ADD COLUMN ... REFERENCES <table> DEFAULT <non-null>` —
//! the added column's default would need to satisfy the FK constraint for
//! every existing row without an FK existence check ever running, which
//! SQLite refuses to do (`sqlite3AlterFinishAddColumn`). But that restriction
//! only fires when `PRAGMA foreign_keys` is actually ON: with
//! `foreign_keys=OFF` the identical ALTER succeeds verbatim (verified against
//! sqlite3 3.51.0).

use vibesql_ast::Statement;
use vibesql_executor::{AlterTableExecutor, CreateTableExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn exec_ddl(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute_with_source(&s, db, Some(sql)).expect("CREATE TABLE");
        }
        Statement::AlterTable(s) => {
            AlterTableExecutor::execute(&s, db).expect("ALTER TABLE");
        }
        other => panic!("unsupported statement in test: {other:?}"),
    }
}

#[test]
fn add_references_column_with_default_errors_when_foreign_keys_on() {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
    exec_ddl(&mut db, "CREATE TABLE t1(a PRIMARY KEY)");
    exec_ddl(&mut db, "CREATE TABLE t2(a, b)");

    let stmt = Parser::parse_sql("ALTER TABLE t2 ADD COLUMN h DEFAULT 'text' REFERENCES t1")
        .expect("parse ADD COLUMN");
    let Statement::AlterTable(alter) = stmt else { panic!("expected ALTER TABLE") };
    let err = AlterTableExecutor::execute(&alter, &mut db)
        .expect_err("REFERENCES column with non-NULL default must error when FKs are enabled");
    assert!(
        err.to_string().contains("Cannot add a REFERENCES column with non-NULL default value"),
        "unexpected error: {err}"
    );

    // The rejected ALTER must not have mutated the schema.
    assert!(
        db.get_table("t2").unwrap().schema.get_column_index("h").is_none(),
        "column h must not have been added after the rejected ALTER"
    );
}

#[test]
fn add_references_column_with_default_succeeds_when_foreign_keys_off() {
    // fkey2-14.1.6: identical ALTER, but with foreign_keys=OFF (the default).
    let mut db = Database::new();
    assert!(!db.foreign_keys_enabled());
    exec_ddl(&mut db, "CREATE TABLE t1(a PRIMARY KEY)");
    exec_ddl(&mut db, "CREATE TABLE t2(a, b)");

    exec_ddl(&mut db, "ALTER TABLE t2 ADD COLUMN h DEFAULT 'text' REFERENCES t1");

    assert!(
        db.get_table("t2").unwrap().schema.get_column_index("h").is_some(),
        "column h must have been added when foreign_keys is OFF"
    );
}
