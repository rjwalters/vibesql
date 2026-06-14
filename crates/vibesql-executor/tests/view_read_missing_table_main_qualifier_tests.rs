//! A plain `SELECT * FROM <view>` over a view whose body references a
//! since-dropped table must report the missing table schema-qualified
//! (`main.<name>`), matching sqlite3 3.51.0. This is the read-path companion to
//! the INSTEAD OF view-DML fix in #5569 (#5528 / trigger4-3.x). See #5570.
//!
//! A main-schema view body resolves its tables against the database schema, so
//! when one of those tables is missing sqlite3 names it with the implicit
//! `main.` prefix. Verified against sqlite3 3.51.0:
//!
//! ```text
//! sqlite> create table test1(id integer primary key,a);
//! sqlite> create table test2(id integer,b);
//! sqlite> create view test as select test1.id as id,a as a,b as b
//!    ...>   from test1 join test2 on test2.id = test1.id;
//! sqlite> drop table test2;
//! sqlite> select * from test;
//! Error: in prepare, no such table: main.test2
//! ```
//!
//! Two cases stay unqualified, also matching sqlite3 3.51.0:
//!
//! ```text
//! sqlite> select * from missing_table;            -- direct, not via a view
//! Error: in prepare, no such table: missing_table
//! ```

use vibesql_executor::{CreateTableExecutor, SelectExecutor, ViewExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Parse and execute a single DDL statement, panicking on failure.
fn exec_ok(db: &mut Database, sql: &str) {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateView(s) => {
            ViewExecutor::execute_create_view(&s, db).expect("CREATE VIEW failed");
        }
        Statement::DropTable(s) => {
            vibesql_executor::DropTableExecutor::execute(&s, db).expect("DROP TABLE failed");
        }
        other => panic!("unsupported statement in exec_ok: {other:?}"),
    }
}

/// Run a SELECT and return its execution error (panics if it unexpectedly
/// succeeds or fails to parse as a SELECT).
fn select_err(db: &Database, sql: &str) -> vibesql_executor::ExecutorError {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    let vibesql_ast::Statement::Select(select) = stmt else { panic!("expected SELECT") };
    let executor = SelectExecutor::new(db);
    executor.execute(&select).expect_err("expected SELECT to fail")
}

/// Build the trigger4 fixture and drop `test2` so the view body is dangling.
fn setup_dangling_view(db: &mut Database) {
    exec_ok(db, "create table test1(id integer primary key, a)");
    exec_ok(db, "create table test2(id integer, b)");
    exec_ok(
        db,
        "create view test as select test1.id as id, a as a, b as b \
         from test1 join test2 on test2.id = test1.id",
    );
    exec_ok(db, "drop table test2");
}

#[test]
fn select_on_view_with_dropped_body_table_qualifies_main() {
    let mut db = Database::new();
    setup_dangling_view(&mut db);

    let err = select_err(&db, "select * from test");
    assert_eq!(
        err,
        vibesql_executor::ExecutorError::TableNotFound("main.test2".to_string()),
        "SELECT over a view with a dropped body table must report main.test2 (sqlite3 parity)"
    );
}

#[test]
fn direct_select_on_missing_table_stays_unqualified() {
    // A direct `SELECT * FROM missing_table` (not via a view) never reaches the
    // view-body resolution and so stays bare, matching sqlite3 3.51.0 and the
    // scope boundary with #5571.
    let db = Database::new();

    let err = select_err(&db, "select * from missing_table");
    assert_eq!(
        err,
        vibesql_executor::ExecutorError::TableNotFound("missing_table".to_string()),
        "direct SELECT on a missing table must stay unqualified"
    );
}
