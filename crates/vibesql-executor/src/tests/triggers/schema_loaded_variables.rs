//! Tests for variable references in triggers that reach the firing path
//! (triggerE.test section 2).
//!
//! SQLite rejects a bound parameter / variable in a `CREATE TRIGGER` body at
//! create time (`trigger cannot use variables`) — VibeSQL enforces this in the
//! parser (#5487 / triggerE.test section 1). However, a trigger whose definition
//! is loaded from the schema *without* going through that parser check (in
//! SQLite: `PRAGMA writable_schema = 1; INSERT INTO sqlite_master VALUES('trigger', ...)`)
//! can carry a variable reference. When such a trigger fires, the variable —
//! which has no bind context, since a trigger program is compiled once — must
//! evaluate to **NULL** rather than raising "unbound placeholder" (triggerE.test
//! 2.2.x / 2.3).
//!
//! VibeSQL does not yet support `writable_schema` / direct `INSERT INTO
//! sqlite_master` (it is rejected as a read-only system table), so the full
//! triggerE section 2 cannot run end-to-end through SQL. These tests instead
//! register a `CreateTriggerStmt` AST with a variable body directly via
//! `execute_create_trigger` — exactly the catalog state a schema-loaded trigger
//! would produce — and verify the fire-time variable→NULL semantics that
//! section 2 depends on. See issue #5500.

use vibesql_ast::{
    CreateTriggerStmt, TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming,
};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::{CreateTableExecutor, InsertExecutor, SelectExecutor};

fn exec_sql(db: &mut Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).unwrap();
        }
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).unwrap();
        }
        other => panic!("unexpected statement: {:?}", other),
    }
}

fn select_rows(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::Select(s) => {
            let executor = SelectExecutor::new(db);
            let result = executor.execute_with_columns(&s).unwrap();
            result.rows.into_iter().map(|r| r.values.to_vec()).collect()
        }
        _ => panic!("expected SELECT"),
    }
}

/// triggerE.test 2.2.x: a schema-loaded trigger whose body INSERTs bound
/// parameters (`?1`, `?2`) must, when fired, insert NULL for each parameter
/// rather than raising "unbound placeholder".
#[test]
fn numbered_placeholder_in_trigger_body_fires_as_null() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1 (a INT, b INT);");
    exec_sql(&mut db, "CREATE TABLE t2 (c INT, d INT);");

    // Mirrors the trigger 2.1 stuffs into sqlite_master:
    //   CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN
    //     INSERT INTO t2 VALUES(?1, ?2);
    //   END
    // We build the AST directly (bypassing the parser's create-time rejection),
    // exactly as a writable_schema load would.
    let trigger = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "tr1".to_string(),
        name_source: None,
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "t1".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("INSERT INTO t2 VALUES(?1, ?2);".to_string()),
    };
    crate::advanced_objects::execute_create_trigger(&trigger, &mut db).unwrap();

    // Firing the trigger must not error; ?1/?2 become NULL.
    exec_sql(&mut db, "INSERT INTO t1 VALUES (1, 2);");

    let rows = select_rows(&db, "SELECT c, d FROM t2;");
    assert_eq!(rows, vec![vec![SqlValue::Null, SqlValue::Null]]);
}

/// `?` (anonymous placeholder) in a trigger body also fires as NULL.
#[test]
fn anonymous_placeholder_in_trigger_body_fires_as_null() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1 (a INT);");
    exec_sql(&mut db, "CREATE TABLE t2 (c INT);");

    let trigger = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "tr_anon".to_string(),
        name_source: None,
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "t1".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("INSERT INTO t2 VALUES(?);".to_string()),
    };
    crate::advanced_objects::execute_create_trigger(&trigger, &mut db).unwrap();

    exec_sql(&mut db, "INSERT INTO t1 VALUES (1);");

    let rows = select_rows(&db, "SELECT c FROM t2;");
    assert_eq!(rows, vec![vec![SqlValue::Null]]);
}

/// `:name` (named placeholder) in a trigger body also fires as NULL.
#[test]
fn named_placeholder_in_trigger_body_fires_as_null() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1 (a INT);");
    exec_sql(&mut db, "CREATE TABLE t2 (c INT);");

    let trigger = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "tr_named".to_string(),
        name_source: None,
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "t1".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("INSERT INTO t2 VALUES(:p);".to_string()),
    };
    crate::advanced_objects::execute_create_trigger(&trigger, &mut db).unwrap();

    exec_sql(&mut db, "INSERT INTO t1 VALUES (1);");

    let rows = select_rows(&db, "SELECT c FROM t2;");
    assert_eq!(rows, vec![vec![SqlValue::Null]]);
}

/// triggerE.test 2.3: a variable reference in a schema-loaded trigger's WHEN
/// clause evaluates to NULL. `WHEN ?1 IS NULL` therefore reduces to
/// `NULL IS NULL` (true), and `WHERE c IS ?2` matches rows whose `c IS NULL`.
#[test]
fn variable_in_when_clause_and_body_where_fires_as_null() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t2 (c, d);");
    exec_sql(&mut db, "CREATE TABLE t3 (e, f);");

    // CREATE TRIGGER tr2 AFTER INSERT ON t3 WHEN ?1 IS NULL BEGIN
    //   UPDATE t2 SET c=d WHERE c IS ?2;
    // END
    // The WHEN parses to `?1 IS NULL`; build the AST directly to bypass the
    // create-time variable rejection (which would reject `?1` in WHEN).
    let when = vibesql_ast::Expression::IsNull {
        expr: Box::new(vibesql_ast::Expression::NumberedPlaceholder(1)),
        negated: false,
    };
    let trigger = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "tr2".to_string(),
        name_source: None,
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "t3".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: Some(Box::new(when)),
        triggered_action: TriggerAction::RawSql("UPDATE t2 SET c = d WHERE c IS ?2;".to_string()),
    };
    crate::advanced_objects::execute_create_trigger(&trigger, &mut db).unwrap();

    // Seed t2 with one non-NULL-c row and one NULL-c row.
    exec_sql(&mut db, "INSERT INTO t2 VALUES('x', 'y');");
    exec_sql(&mut db, "INSERT INTO t2 VALUES(NULL, 'z');");

    // Fire: WHEN ?1 IS NULL -> NULL IS NULL -> true, so the trigger runs.
    // Body: UPDATE t2 SET c=d WHERE c IS ?2 -> WHERE c IS NULL -> only the
    // (NULL,'z') row updates, becoming ('z','z'). Matches sqlite3 3.51.0.
    exec_sql(&mut db, "INSERT INTO t3 VALUES(1, 2);");

    let t3 = select_rows(&db, "SELECT e, f FROM t3;");
    assert_eq!(t3, vec![vec![SqlValue::Integer(1), SqlValue::Integer(2)]]);

    let t2 = select_rows(&db, "SELECT c, d FROM t2;");
    assert_eq!(
        t2,
        vec![
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("x")),
                SqlValue::Varchar(arcstr::ArcStr::from("y")),
            ],
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("z")),
                SqlValue::Varchar(arcstr::ArcStr::from("z")),
            ],
        ]
    );
}
