//! Subquery-in-WHEN trigger firing tests (#5581).
//!
//! SQLite allows a trigger's WHEN clause to contain a subquery, e.g.
//! `WHEN (SELECT count(*) FROM tbl) = 0`. The trigger fires only when the
//! subquery condition holds, evaluated against the table state as-of trigger
//! firing. This reproduces SQLite's `trigger2-3.2` scenario.
//!
//! Expectations verified against sqlite3 3.51.0:
//! ```text
//! CREATE TABLE tbl (a, b, c, d);
//! CREATE TABLE log (a);
//! INSERT INTO log VALUES (0);
//! CREATE TRIGGER t1 BEFORE INSERT ON tbl WHEN new.a > 20 ...;
//! CREATE TRIGGER t2 BEFORE INSERT ON tbl WHEN (SELECT count(*) FROM tbl) = 0 ...;
//! -- inserting (0,..) into an empty tbl  -> log = 1  (t2 fires)
//! -- inserting (0,..) into a non-empty tbl -> log = 0 (neither fires)
//! -- inserting (200,..)                   -> log = 1  (t1 fires)
//! ```

use vibesql_executor::{
    CreateTableExecutor, InsertExecutor, SelectExecutor, TriggerExecutor, UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec(db: &mut Database, sql: &str) {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for `{sql}`: {e:?}"));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .expect("CREATE TRIGGER failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).expect("UPDATE failed");
        }
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

fn query_scalar_i64(db: &Database, sql: &str) -> i64 {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    let Statement::Select(select) = stmt else { panic!("expected SELECT") };
    let rows = SelectExecutor::new(db).execute(&select).expect("SELECT failed");
    match &rows[0].values[0] {
        SqlValue::Integer(n) => *n,
        other => panic!("expected integer, got {other:?}"),
    }
}

/// trigger2-3.2: a WHEN subquery (`(SELECT count(*) FROM tbl) = 0`) is parsed,
/// evaluated at fire time against current table state, and the trigger fires
/// exactly when the subquery condition holds. Matches sqlite3 3.51.0 `{1 0 1}`.
#[test]
fn when_subquery_fires_per_subquery_result_trigger2_3_2() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE tbl (a, b, c, d);");
    exec(&mut db, "CREATE TABLE log (a);");
    exec(&mut db, "INSERT INTO log VALUES (0);");
    exec(
        &mut db,
        "CREATE TRIGGER t1 BEFORE INSERT ON tbl WHEN new.a > 20 \
         BEGIN UPDATE log SET a = a + 1; END;",
    );
    exec(
        &mut db,
        "CREATE TRIGGER t2 BEFORE INSERT ON tbl WHEN (SELECT count(*) FROM tbl) = 0 \
         BEGIN UPDATE log SET a = a + 1; END;",
    );

    // tbl is empty -> the subquery WHEN of t2 is true -> log becomes 1.
    exec(&mut db, "INSERT INTO tbl VALUES(0, 0, 0, 0);");
    assert_eq!(query_scalar_i64(&db, "SELECT a FROM log;"), 1, "empty-table insert: t2 should fire");
    exec(&mut db, "UPDATE log SET a = 0;");

    // tbl now has a row -> subquery WHEN of t2 is false, a=0 so t1 false -> log stays 0.
    exec(&mut db, "INSERT INTO tbl VALUES(0, 0, 0, 0);");
    assert_eq!(query_scalar_i64(&db, "SELECT a FROM log;"), 0, "non-empty insert: neither fires");
    exec(&mut db, "UPDATE log SET a = 0;");

    // a=200 -> t1 (new.a > 20) fires; t2's subquery WHEN is false -> log becomes 1.
    exec(&mut db, "INSERT INTO tbl VALUES(200, 0, 0, 0);");
    assert_eq!(query_scalar_i64(&db, "SELECT a FROM log;"), 1, "a>20 insert: t1 should fire");
}
