//! Tests for `ALTER TABLE ... RENAME TO` rewriting the renamed table's *own*
//! qualified references inside its `CREATE TABLE` text (issue #6174,
//! altertab.test 1.4).
//!
//! SQLite (`legacy_alter_table=OFF`) turns `CHECK(t1.a != t1.b)` into
//! `CHECK("t1new".a != "t1new".b)`. Before this fix VibeSQL left the stored
//! text and the in-memory CHECK expression naming `t1`. Every later INSERT
//! then failed with `Invalid table qualifier 't1'`, and a reload re-parsed
//! the same stale text.

use vibesql_ast::Statement;
use vibesql_storage::Database;

fn exec(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("{e:?}"))?;
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute_with_source(&s, db, Some(sql))
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        Statement::Insert(s) => {
            crate::InsertExecutor::execute(db, &s).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::AlterTable(s) => {
            crate::alter::AlterTableExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        other => panic!("unexpected statement: {other:?}"),
    }
}

fn table_sql(db: &Database, name: &str) -> String {
    db.get_table(name)
        .unwrap_or_else(|| panic!("table {name} must exist"))
        .schema
        .sql_source
        .clone()
        .expect("renamed table keeps verbatim SQL")
}

#[test]
fn rename_rewrites_own_check_qualifiers() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b, CHECK(t1.a != t1.b))").unwrap();
    exec(&mut db, "ALTER TABLE t1 RENAME TO t1new").unwrap();

    assert_eq!(
        table_sql(&db, "t1new"),
        "CREATE TABLE \"t1new\"(a, b, CHECK(\"t1new\".a != \"t1new\".b))"
    );

    // The CHECK still works against the renamed table.
    exec(&mut db, "INSERT INTO t1new VALUES(1, 2)").unwrap();
    let err = exec(&mut db, "INSERT INTO t1new VALUES(3, 3)").unwrap_err();
    assert!(err.contains("CHECK constraint failed"), "unexpected error: {err}");
    assert!(err.contains("\"t1new\".a"), "message should echo the rewritten CHECK text: {err}");
    assert_eq!(db.get_table("t1new").unwrap().row_count(), 1);
}

#[test]
fn second_rename_rewrites_previously_quoted_check_qualifiers() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b, CONSTRAINT c1 CHECK(main.t1.a > 0 AND 't1' <> t1.b))")
        .unwrap();
    exec(&mut db, "ALTER TABLE t1 RENAME TO t2").unwrap();
    exec(&mut db, "ALTER TABLE t2 RENAME TO t3").unwrap();

    assert_eq!(
        table_sql(&db, "t3"),
        "CREATE TABLE \"t3\"(a, b, CONSTRAINT c1 CHECK(main.\"t3\".a > 0 AND 't1' <> \"t3\".b))"
    );
    exec(&mut db, "INSERT INTO t3 VALUES(1, 'x')").unwrap();
    assert!(exec(&mut db, "INSERT INTO t3 VALUES(0, 'x')").is_err());
}
