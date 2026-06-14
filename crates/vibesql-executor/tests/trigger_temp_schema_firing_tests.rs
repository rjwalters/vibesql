//! Schema-aware trigger firing for temp vs main triggers (triggerD-3.1/3.2, #5531).
//!
//! When a `temp` table shadows a same-named `main` table, a trigger must fire
//! only for operations on the table it is bound to. SQLite binds a trigger to
//! the table its name resolves to *from the trigger's own schema*: a `main`
//! trigger binds to `main.<table>` (temp is invisible to it), while a `temp`
//! trigger binds to `temp.<table>` if it exists, otherwise to `main.<table>`.
//!
//! Expectations verified against sqlite3 3.51.0:
//! ```text
//! CREATE TABLE t300(x);
//! CREATE TEMP TABLE t300(x);
//! CREATE TABLE t301(y);
//! CREATE TRIGGER main.r300 AFTER INSERT ON t300 BEGIN
//!   INSERT INTO t301 VALUES(10000 + new.x); END;
//! INSERT INTO main.t300 VALUES(3);   -- r300 fires  -> 10003
//! INSERT INTO temp.t300 VALUES(4);   -- r300 does NOT fire
//! SELECT * FROM t301;                -- {10003}     (triggerD-3.1)
//!
//! CREATE TRIGGER temp.r301 AFTER INSERT ON t300 BEGIN
//!   INSERT INTO t301 VALUES(20000 + new.x); END;
//! INSERT INTO main.t300 VALUES(3);   -- r300 fires  -> 10003
//! INSERT INTO temp.t300 VALUES(4);   -- r301 fires  -> 20004
//! SELECT * FROM t301;                -- {10003 20004} (triggerD-3.2)
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

/// Returns the ordered `y` column of `t301` as i64s (insertion order).
fn log_values(db: &Database) -> Vec<i64> {
    use vibesql_ast::Statement;
    let stmt = Parser::parse_sql("SELECT y FROM t301;").expect("parse failed");
    let Statement::Select(select) = stmt else { panic!("expected SELECT") };
    let rows = SelectExecutor::new(db).execute(&select).expect("SELECT failed");
    rows.iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(n) => *n,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect()
}

/// triggerD-3.1: a `main` trigger on a table shadowed by a temp namesake fires
/// only for the `main` table, not for the `temp` table. Matches sqlite3 3.51.0
/// `{10003}`.
#[test]
fn main_trigger_fires_only_on_main_table_triggerd_3_1() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t300(x);");
    exec(&mut db, "CREATE TEMP TABLE t300(x);");
    exec(&mut db, "CREATE TABLE t301(y);");
    exec(
        &mut db,
        "CREATE TRIGGER main.r300 AFTER INSERT ON t300 \
         BEGIN INSERT INTO t301 VALUES(10000 + new.x); END;",
    );

    exec(&mut db, "INSERT INTO main.t300 VALUES(3);");
    exec(&mut db, "INSERT INTO temp.t300 VALUES(4);");

    assert_eq!(
        log_values(&db),
        vec![10003],
        "main.r300 must fire only on main.t300 (3), never on temp.t300 (4)"
    );
}

/// triggerD-3.2: adding a `temp` trigger on the same name; each trigger fires
/// only for the table in its own schema. Matches sqlite3 3.51.0 `{10003 20004}`.
#[test]
fn main_and_temp_triggers_coexist_per_schema_triggerd_3_2() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t300(x);");
    exec(&mut db, "CREATE TEMP TABLE t300(x);");
    exec(&mut db, "CREATE TABLE t301(y);");
    exec(
        &mut db,
        "CREATE TRIGGER main.r300 AFTER INSERT ON t300 \
         BEGIN INSERT INTO t301 VALUES(10000 + new.x); END;",
    );
    exec(
        &mut db,
        "CREATE TRIGGER temp.r301 AFTER INSERT ON t300 \
         BEGIN INSERT INTO t301 VALUES(20000 + new.x); END;",
    );

    exec(&mut db, "INSERT INTO main.t300 VALUES(3);");
    exec(&mut db, "INSERT INTO temp.t300 VALUES(4);");

    assert_eq!(
        log_values(&db),
        vec![10003, 20004],
        "main.r300 fires for main.t300 (10003); temp.r301 fires for temp.t300 (20004)"
    );
}

/// A temp trigger created on a table that exists ONLY in main binds to (and
/// fires for) the main table — temp-schema name resolution is temp-then-main.
/// Matches sqlite3 3.51.0 (`temp.tr AFTER INSERT ON m` fires the main insert).
#[test]
fn temp_trigger_on_main_only_table_fires() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE solo(x);");
    exec(&mut db, "CREATE TABLE t301(y);");
    exec(
        &mut db,
        "CREATE TRIGGER temp.tr AFTER INSERT ON solo \
         BEGIN INSERT INTO t301 VALUES(new.x); END;",
    );

    exec(&mut db, "INSERT INTO solo VALUES(7);");

    assert_eq!(
        log_values(&db),
        vec![7],
        "a temp trigger on a main-only table must bind to and fire for the main table"
    );
}

/// Regression guard: a single main trigger on a non-shadowed table still fires
/// for unqualified inserts (the common single-schema case must not regress).
#[test]
fn plain_main_trigger_still_fires_unqualified() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE base(x);");
    exec(&mut db, "CREATE TABLE t301(y);");
    exec(
        &mut db,
        "CREATE TRIGGER r AFTER INSERT ON base \
         BEGIN INSERT INTO t301 VALUES(new.x); END;",
    );

    exec(&mut db, "INSERT INTO base VALUES(42);");

    assert_eq!(log_values(&db), vec![42], "unqualified insert must still fire the main trigger");
}
