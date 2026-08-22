//! Tests for a schema-qualified `CREATE TRIGGER ... ON <schema>.<table> ...`
//! target and the trigger-body `TableNotFound` schema-qualification it
//! unlocks (issue #6193, e_delete-2.2.1.1 / e_update-2.1.1/2.2.1).
//!
//! `ON main.t7` previously used `parse_identifier()`, which cannot consume a
//! `schema.` prefix and hit a bare `near ".": syntax error` — so no trigger
//! with a schema-qualified ON target could be created at all. Once that
//! parses, a trigger body statement referencing a table that does not exist
//! must report `no such table: main.<name>` (matching sqlite3's schema
//! resolution) rather than the shim-unqualified `Table '<name>' not found`.

use vibesql_ast::Statement;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt =
        vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::CreateTrigger(s) => crate::advanced_objects::execute_create_trigger(&s, db)
            .map(|_| "trigger created".to_string())
            .map_err(|e| e.to_string()),
        Statement::Insert(s) => crate::InsertExecutor::execute(db, &s)
            .map(|count| format!("{} row(s) inserted", count))
            .map_err(|e| e.to_string()),
        other => Err(format!("Unsupported statement type: {:?}", other)),
    }
}

fn query_all(db: &Database, sql: &str) -> Vec<SqlValue> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse select");
    let select = match stmt {
        Statement::Select(s) => s,
        other => panic!("expected SELECT, got {:?}", other),
    };
    let result = crate::SelectExecutor::new(db).execute_with_columns(&select).expect("run select");
    result.rows.iter().flat_map(|r| r.values.clone()).collect()
}

#[test]
fn create_trigger_accepts_main_qualified_on_target_and_fires() {
    // e_delete-2.2.1.1 setup: both the trigger name and its ON target carry a
    // `main.` qualifier. The trigger must be created and actually fire.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t7(a, b)").unwrap();
    exec(&mut db, "CREATE TABLE log(x)").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER main.tr1 AFTER INSERT ON main.t7 BEGIN INSERT INTO log VALUES(1); END;",
    )
    .expect("CREATE TRIGGER with qualified ON target should parse and succeed");

    exec(&mut db, "INSERT INTO t7 VALUES(1, 2)").unwrap();
    assert_eq!(query_all(&db, "SELECT count(*) FROM log"), vec![SqlValue::Integer(1)]);
}

#[test]
fn trigger_body_missing_table_is_qualified_with_main_schema() {
    // e_delete-2.2.1.1: a non-temp trigger's body referencing a table that
    // does not exist anywhere reports `no such table: main.<name>`, matching
    // sqlite3's resolution-in-the-trigger's-own-schema semantics
    // (R-28818-63526).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t7(a, b)").unwrap();
    exec(&mut db, "CREATE TRIGGER main.tr1 AFTER INSERT ON main.t7 BEGIN DELETE FROM t9; END;")
        .unwrap();

    let err = exec(&mut db, "INSERT INTO t7 VALUES(1, 2)").unwrap_err();
    assert_eq!(err, "Table 'main.t9' not found");
}

#[test]
fn temp_trigger_body_missing_table_stays_unqualified() {
    // R-31567-38587: a TEMP trigger's missing target is left bare (no `main.`
    // qualifier), matching sqlite3's TEMP-trigger resolution.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t7(a, b)").unwrap();
    exec(&mut db, "CREATE TEMP TRIGGER tr1 AFTER INSERT ON t7 BEGIN DELETE FROM t9; END;").unwrap();

    let err = exec(&mut db, "INSERT INTO t7 VALUES(1, 2)").unwrap_err();
    assert_eq!(err, "Table 't9' not found");
}
