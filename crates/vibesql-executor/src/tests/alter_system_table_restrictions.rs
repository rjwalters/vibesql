//! Tests that `ALTER TABLE` refuses to touch SQLite's own schema/statistics
//! tables (`sqlite_master`/`sqlite_schema`, `sqlite_stat1..4`) and views, with
//! SQLite's exact error wording, for every ALTER sub-command (#6174).
//!
//! sqlite3 3.51.0 reports a single uniform message across every ALTER
//! sub-command for its own system tables:
//!
//! ```text
//! sqlite> ALTER TABLE sqlite_master RENAME TO x;
//! Error: table sqlite_master may not be altered
//! sqlite> ALTER TABLE sqlite_stat1 ADD COLUMN x;
//! Error: table sqlite_stat1 may not be altered
//! ```
//!
//! and a view-specific message for `RENAME TO`:
//!
//! ```text
//! sqlite> CREATE VIEW v1 AS SELECT 1;
//! sqlite> ALTER TABLE v1 RENAME TO v2;
//! Error: view v1 may not be altered
//! ```
//!
//! Before this fix these all fell through to the generic (and wrong)
//! `no such table: <name>`, because none of `sqlite_master` / `sqlite_stat1` /
//! a view are real catalog table entries (alter-2.4, alter-12.2, alter-15.*,
//! altercol-6.1/12.1.2).

use vibesql_ast::Statement;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

fn exec(db: &mut Database, sql: &str) -> Result<String, ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => crate::CreateTableExecutor::execute(&s, db),
        Statement::CreateView(s) => {
            crate::advanced_objects::execute_create_view(&s, db).map(|_| String::new())
        }
        Statement::AlterTable(s) => crate::alter::AlterTableExecutor::execute(&s, db),
        other => panic!("unexpected statement: {:?}", other),
    }
}

#[test]
fn rename_sqlite_master_is_rejected_case_insensitively() {
    let mut db = Database::new();
    let err = exec(&mut db, "ALTER TABLE SqLiTe_master RENAME TO master").unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_master may not be altered");
}

#[test]
fn add_column_to_sqlite_master_is_rejected() {
    let mut db = Database::new();
    let err = exec(&mut db, "ALTER TABLE sqlite_master ADD COLUMN x").unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_master may not be altered");
}

#[test]
fn rename_sqlite_stat1_is_rejected() {
    let mut db = Database::new();
    let err = exec(&mut db, "ALTER TABLE sqlite_stat1 RENAME TO xyz").unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_stat1 may not be altered");
}

#[test]
fn add_column_to_sqlite_stat1_is_rejected() {
    let mut db = Database::new();
    let err = exec(&mut db, "ALTER TABLE sqlite_stat1 ADD COLUMN xyz").unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_stat1 may not be altered");
}

#[test]
fn rename_column_of_sqlite_stat1_is_rejected() {
    let mut db = Database::new();
    let err = exec(&mut db, "ALTER TABLE sqlite_stat1 RENAME tbl TO thetable").unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_stat1 may not be altered");
}

#[test]
fn rename_view_is_rejected_with_view_specific_message() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    exec(&mut db, "CREATE VIEW v1 AS SELECT * FROM t1").unwrap();

    let err = exec(&mut db, "ALTER TABLE v1 RENAME TO v2").unwrap_err();
    assert_eq!(err.to_string(), "view v1 may not be altered");

    // The failed rename must leave the view and its name untouched.
    assert!(db.catalog.get_view("v1").is_some());
}
