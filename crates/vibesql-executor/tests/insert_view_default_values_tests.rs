//! `INSERT INTO <view> DEFAULT VALUES` with an INSTEAD OF INSERT trigger.
//!
//! A view column has no column-level DEFAULT, so a `DEFAULT` placeholder — from
//! `INSERT INTO view DEFAULT VALUES`, or an explicit `DEFAULT` in a VALUES row —
//! resolves to NULL, and the INSTEAD OF trigger sees `NEW.<col> = NULL`. This
//! matches sqlite3 3.51.0 (triggerC-11.4). Before the fix the placeholder was
//! evaluated directly, raising "DEFAULT keyword is only valid in INSERT VALUES
//! and UPDATE SET clauses". Part of #6176.

use vibesql_executor::{
    CreateTableExecutor, InsertExecutor, SelectExecutor, TriggerExecutor, ViewExecutor,
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
        Statement::CreateView(s) => {
            ViewExecutor::execute_create_view(&s, db).expect("CREATE VIEW failed");
        }
        Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .expect("CREATE TRIGGER failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

/// Reads `log` as `(a, b)` pairs preserving insertion order.
fn read_log(db: &Database) -> Vec<(SqlValue, SqlValue)> {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql("SELECT a, b FROM log;").expect("parse SELECT");
    let Statement::Select(select) = stmt else { panic!("expected SELECT") };
    let rows = SelectExecutor::new(db).execute(&select).expect("SELECT failed");
    rows.iter().map(|r| (r.values[0].clone(), r.values[1].clone())).collect()
}

fn setup(db: &mut Database) {
    exec(db, "CREATE TABLE log(a, b)");
    exec(db, "CREATE TABLE t2(a, b)");
    exec(db, "CREATE VIEW v2 AS SELECT * FROM t2");
    exec(
        db,
        "CREATE TRIGGER tv2 INSTEAD OF INSERT ON v2 \
         BEGIN INSERT INTO log VALUES(new.a, new.b); END",
    );
}

/// triggerC-11.4: `INSERT INTO v2 DEFAULT VALUES` fires the INSTEAD OF trigger
/// with all NEW columns NULL (a view has no column defaults).
#[test]
fn insert_default_values_on_view_fires_trigger_with_nulls() {
    let mut db = Database::new();
    setup(&mut db);

    exec(&mut db, "INSERT INTO v2 DEFAULT VALUES");

    let log = read_log(&db);
    assert_eq!(log, vec![(SqlValue::Null, SqlValue::Null)]);
}

/// An explicit `DEFAULT` in a VALUES row on a view likewise resolves to NULL,
/// while non-DEFAULT positions keep their literal value.
#[test]
fn explicit_default_in_values_on_view_resolves_to_null() {
    let mut db = Database::new();
    setup(&mut db);

    exec(&mut db, "INSERT INTO v2 VALUES(DEFAULT, 5)");

    let log = read_log(&db);
    assert_eq!(log, vec![(SqlValue::Null, SqlValue::Integer(5))]);
}
