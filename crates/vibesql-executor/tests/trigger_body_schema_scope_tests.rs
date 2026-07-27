//! Trigger-body name resolution is scoped to the trigger's own schema
//! (trigger1-3.2..3.5, #6176).
//!
//! SQLite resolves the unqualified table names in a trigger's body against the
//! database the trigger belongs to. A trigger created in the `main` schema
//! therefore resolves an unqualified `t2` to `main.t2` only — it cannot see a
//! `TEMP` table of the same name, and referencing one that exists only in temp
//! fails with `no such table: main.t2`. A `TEMP` trigger keeps ordinary
//! resolution (temp shadows main), so it *can* see the temp table.
//!
//! Expectations verified against sqlite3 3.51.0:
//! ```text
//! CREATE TABLE t1(a, b);
//! CREATE TEMP TABLE t2(x, y);
//! CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN INSERT INTO t2 VALUES(new.a,new.b); END;
//! INSERT INTO t1 VALUES(1, 2);   -- Error: no such table: main.t2   (main trigger)
//!
//! -- but a TEMP trigger can see the temp table:
//! CREATE TEMP TRIGGER r1 AFTER INSERT ON t1 BEGIN INSERT INTO t2 VALUES(new.a,new.b); END;
//! INSERT INTO t1 VALUES(1, 2);   -- ok; temp.t2 now holds (1,2)
//! ```

use vibesql_executor::{CreateTableExecutor, InsertExecutor, SelectExecutor, TriggerExecutor};
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
        other => panic!("unsupported statement in test helper: {other:?}"),
    }
}

/// Run an INSERT and return the error message (expects it to fail).
fn insert_err(db: &mut Database, sql: &str) -> String {
    use vibesql_ast::Statement;
    let Statement::Insert(s) = Parser::parse_sql(sql).expect("parse failed") else {
        panic!("expected INSERT");
    };
    match InsertExecutor::execute(db, &s) {
        Ok(_) => panic!("expected INSERT to fail, but it succeeded"),
        Err(e) => e.to_string(),
    }
}

/// Read the temp `t2` (x, y) rows as flat i64 pairs in insertion order.
fn temp_t2(db: &Database) -> Vec<i64> {
    use vibesql_ast::Statement;
    let Statement::Select(select) = Parser::parse_sql("SELECT x, y FROM temp.t2;").expect("parse")
    else {
        panic!("expected SELECT");
    };
    let rows = SelectExecutor::new(db).execute(&select).expect("SELECT failed");
    rows.iter()
        .flat_map(|r| {
            r.values.iter().map(|v| match v {
                SqlValue::Integer(n) => *n,
                other => panic!("expected integer, got {other:?}"),
            })
        })
        .collect()
}

/// A `main`-schema trigger cannot resolve an unqualified body reference to a
/// TEMP table of the same name; it reports `no such table: main.t2`
/// (trigger1-3.2).
#[test]
fn main_trigger_body_cannot_see_temp_table() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b);");
    exec(&mut db, "CREATE TEMP TABLE t2(x, y);");
    exec(
        &mut db,
        "CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN INSERT INTO t2 VALUES(new.a, new.b); END;",
    );

    let msg = insert_err(&mut db, "INSERT INTO t1 VALUES(1, 2);");
    assert!(
        msg.contains("main.t2"),
        "main trigger must report the unresolved table schema-qualified, got: {msg}"
    );

    // The failed trigger must not have polluted the temp table.
    assert!(temp_t2(&db).is_empty(), "no row should have reached temp.t2");
}

/// A `TEMP`-schema trigger keeps ordinary resolution and *can* insert into the
/// shadowing temp table (trigger1-3.6.2).
#[test]
fn temp_trigger_body_can_see_temp_table() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b);");
    exec(&mut db, "CREATE TEMP TABLE t2(x, y);");
    exec(
        &mut db,
        "CREATE TEMP TRIGGER r1 AFTER INSERT ON t1 \
         BEGIN INSERT INTO t2 VALUES(new.a, new.b); END;",
    );

    exec(&mut db, "INSERT INTO t1 VALUES(1, 2);");
    assert_eq!(temp_t2(&db), vec![1, 2], "temp trigger fills temp.t2 with (1,2)");
}

/// When only a `main` table of the referenced name exists, a `main` trigger
/// resolves to it normally (the suppression only hides the temp schema).
#[test]
fn main_trigger_body_resolves_main_table_normally() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b);");
    exec(&mut db, "CREATE TABLE t2(x, y);");
    exec(
        &mut db,
        "CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN INSERT INTO t2 VALUES(new.a, new.b); END;",
    );

    exec(&mut db, "INSERT INTO t1 VALUES(5, 6);");

    use vibesql_ast::Statement;
    let Statement::Select(select) = Parser::parse_sql("SELECT x, y FROM main.t2;").expect("parse")
    else {
        panic!("expected SELECT");
    };
    let rows = SelectExecutor::new(&db).execute(&select).expect("SELECT failed");
    let vals: Vec<i64> = rows
        .iter()
        .flat_map(|r| {
            r.values.iter().map(|v| match v {
                SqlValue::Integer(n) => *n,
                other => panic!("expected integer, got {other:?}"),
            })
        })
        .collect();
    assert_eq!(vals, vec![5, 6], "main trigger fills main.t2 with (5,6)");
}
