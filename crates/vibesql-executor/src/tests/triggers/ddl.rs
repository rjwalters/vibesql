//! Tests for trigger DDL operations (CREATE TRIGGER, DROP TRIGGER)

use vibesql_ast::{
    CreateTriggerStmt, DropTriggerStmt, TriggerAction, TriggerEvent, TriggerGranularity,
    TriggerTiming,
};
use vibesql_storage::Database;

use crate::CreateTableExecutor;

#[test]
fn test_create_trigger() {
    let mut db = Database::new();

    // Create a table first (since trigger references it)
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create a trigger
    let stmt = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "my_trigger".to_string(),
        name_source: None,
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "test_table".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    };

    let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create trigger: {:?}", result.err());

    // Verify trigger was created
    let trigger = db.catalog.get_trigger("my_trigger");
    assert!(trigger.is_some(), "Trigger not found after creation");
    let trigger = trigger.unwrap();
    assert_eq!(trigger.name, "my_trigger");
    assert_eq!(trigger.timing, TriggerTiming::Before);
    assert_eq!(trigger.event, TriggerEvent::Insert);
}

#[test]
fn test_create_trigger_duplicate_error() {
    let mut db = Database::new();

    // Create a table first
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create a trigger
    let stmt = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "my_trigger".to_string(),
        name_source: None,
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "test_table".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    };

    let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
    assert!(result.is_ok(), "Failed to create trigger: {:?}", result.err());

    // Try to create another trigger with the same name
    let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
    assert!(result.is_err(), "Should fail when creating duplicate trigger");
}

#[test]
fn test_create_trigger_if_not_exists_is_noop_when_present() {
    // `CREATE TRIGGER IF NOT EXISTS` for an already-existing trigger is a
    // no-op success (SQLite trigger1-1.2.0), not a "trigger already exists"
    // error.
    let mut db = Database::new();

    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    match vibesql_parser::Parser::parse_sql(create_table_sql).unwrap() {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    let stmt = CreateTriggerStmt {
        if_not_exists: true,
        schema: None,
        trigger_name: "my_trigger".to_string(),
        name_source: None,
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "test_table".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("BEGIN SELECT 1; END".to_string()),
    };

    // First create succeeds.
    crate::advanced_objects::execute_create_trigger(&stmt, &mut db)
        .expect("first create should succeed");

    // Second create with IF NOT EXISTS is a no-op success, not an error.
    let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
    assert!(result.is_ok(), "IF NOT EXISTS create should be a no-op: {:?}", result.err());

    // Without IF NOT EXISTS, the same duplicate still errors.
    let strict = CreateTriggerStmt { if_not_exists: false, ..stmt.clone() };
    let result = crate::advanced_objects::execute_create_trigger(&strict, &mut db);
    assert!(result.is_err(), "duplicate without IF NOT EXISTS should error");
}

/// Helper: create an empty test table named `t1` for the wording tests below.
fn create_t1(db: &mut Database) {
    match vibesql_parser::Parser::parse_sql("CREATE TABLE t1 (a INT, b INT);").unwrap() {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, db).unwrap();
        }
        other => panic!("Expected CreateTable, got {:?}", other),
    }
}

/// Helper: parse a CREATE TRIGGER statement string and return the typed AST.
fn parse_create_trigger(sql: &str) -> CreateTriggerStmt {
    match vibesql_parser::Parser::parse_sql(sql).expect("parse failed") {
        vibesql_ast::Statement::CreateTrigger(stmt) => stmt,
        other => panic!("Expected CreateTrigger, got {:?}", other),
    }
}

#[test]
fn test_duplicate_trigger_already_exists_echoes_source_quoting() {
    // SQLite (3.51.0) echoes the trigger name *exactly as written* in the
    // "already exists" error, preserving its quoting form
    // (trigger1-1.2.1/1.2.2/1.2.3):
    //   CREATE TRIGGER  tr1  -> trigger tr1 already exists
    //   CREATE TRIGGER "tr1" -> trigger "tr1" already exists
    //   CREATE TRIGGER [tr1] -> trigger [tr1] already exists
    //
    // VibeSQL surfaces this via `Trigger '<echoed>' already exists`; the TCL
    // conformance shim rewrites it to SQLite's lowercase/unquoted-wrapper form
    // while preserving `<echoed>`. We assert the raw VibeSQL message here.
    let cases = [
        ("CREATE TRIGGER tr1 DELETE ON t1 BEGIN SELECT 1; END;", "Trigger 'tr1' already exists"),
        (
            "CREATE TRIGGER \"tr1\" DELETE ON t1 BEGIN SELECT 1; END;",
            "Trigger '\"tr1\"' already exists",
        ),
        ("CREATE TRIGGER [tr1] DELETE ON t1 BEGIN SELECT 1; END;", "Trigger '[tr1]' already exists"),
    ];

    for (dup_sql, expected) in cases {
        let mut db = Database::new();
        create_t1(&mut db);

        // First create (bare, unquoted) succeeds and registers `tr1`.
        let first = parse_create_trigger("CREATE TRIGGER tr1 DELETE ON t1 BEGIN SELECT 1; END;");
        crate::advanced_objects::execute_create_trigger(&first, &mut db)
            .expect("first create should succeed");

        // Re-creating with the (possibly quoted) duplicate name errors, echoing
        // the source quoting form.
        let dup = parse_create_trigger(dup_sql);
        let err = crate::advanced_objects::execute_create_trigger(&dup, &mut db)
            .expect_err("duplicate trigger must be rejected");
        assert_eq!(err.to_string(), expected, "for SQL: {dup_sql}");
    }
}

#[test]
fn test_create_trigger_on_system_table_wording() {
    // SQLite (3.51.0, trigger1-1.9) rejects CREATE TRIGGER on a system table with
    // exactly "cannot create trigger on system table".
    let mut db = Database::new();

    let stmt = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "tr1".to_string(),
        name_source: None,
        timing: TriggerTiming::After,
        event: TriggerEvent::Update(None),
        table_name: "sqlite_master".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    };

    let err = crate::advanced_objects::execute_create_trigger(&stmt, &mut db)
        .expect_err("trigger on system table must be rejected");
    assert_eq!(err.to_string(), "cannot create trigger on system table");
}

#[test]
fn test_create_instead_of_trigger_on_table_wording() {
    // SQLite (3.51.0, trigger1-1.12): "cannot create INSTEAD OF trigger on table: t1".
    let mut db = Database::new();
    create_t1(&mut db);

    let stmt = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "t1t".to_string(),
        name_source: None,
        timing: TriggerTiming::InsteadOf,
        event: TriggerEvent::Update(None),
        table_name: "t1".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    };

    let err = crate::advanced_objects::execute_create_trigger(&stmt, &mut db)
        .expect_err("INSTEAD OF trigger on a table must be rejected");
    assert_eq!(err.to_string(), "cannot create INSTEAD OF trigger on table: t1");
}

#[test]
fn test_create_before_after_trigger_on_view_wording() {
    // SQLite (3.51.0, trigger1-1.13/1.14):
    //   "cannot create BEFORE trigger on view: v1"
    //   "cannot create AFTER trigger on view: v1"
    let mut db = Database::new();
    create_t1(&mut db);
    match vibesql_parser::Parser::parse_sql("CREATE VIEW v1 AS SELECT * FROM t1;").unwrap() {
        vibesql_ast::Statement::CreateView(stmt) => {
            crate::advanced_objects::execute_create_view(&stmt, &mut db).unwrap();
        }
        other => panic!("Expected CreateView, got {:?}", other),
    }

    for (timing, label) in [(TriggerTiming::Before, "BEFORE"), (TriggerTiming::After, "AFTER")] {
        let stmt = CreateTriggerStmt {
            if_not_exists: false,
            schema: None,
            trigger_name: "v1t".to_string(),
            name_source: None,
            timing,
            event: TriggerEvent::Update(None),
            table_name: "v1".to_string(),
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
        };

        let err = crate::advanced_objects::execute_create_trigger(&stmt, &mut db)
            .expect_err("BEFORE/AFTER trigger on a view must be rejected");
        assert_eq!(err.to_string(), format!("cannot create {} trigger on view: v1", label));
    }
}

#[test]
fn test_drop_trigger() {
    let mut db = Database::new();

    // Create a table first
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // Create a trigger
    let stmt = CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: "my_trigger".to_string(),
        name_source: None,
        timing: TriggerTiming::Before,
        event: TriggerEvent::Insert,
        table_name: "test_table".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    };

    crate::advanced_objects::execute_create_trigger(&stmt, &mut db).unwrap();

    // Verify it was created
    assert!(db.catalog.get_trigger("my_trigger").is_some());

    // Drop the trigger
    let drop_stmt =
        DropTriggerStmt { trigger_name: "my_trigger".to_string(), cascade: false, if_exists: false };

    let result = crate::advanced_objects::execute_drop_trigger(&drop_stmt, &mut db);
    assert!(result.is_ok(), "Failed to drop trigger: {:?}", result.err());

    // Verify it was dropped
    assert!(db.catalog.get_trigger("my_trigger").is_none(), "Trigger still exists after drop");
}

#[test]
fn test_drop_trigger_not_found() {
    let mut db = Database::new();

    let drop_stmt = DropTriggerStmt {
        trigger_name: "nonexistent_trigger".to_string(),
        cascade: false,
        if_exists: false,
    };

    let result = crate::advanced_objects::execute_drop_trigger(&drop_stmt, &mut db);
    assert!(result.is_err(), "Should fail when dropping non-existent trigger");
}

#[test]
fn test_drop_trigger_if_exists_missing_is_noop() {
    let mut db = Database::new();

    let drop_stmt = DropTriggerStmt {
        trigger_name: "nonexistent_trigger".to_string(),
        cascade: false,
        if_exists: true,
    };

    let result = crate::advanced_objects::execute_drop_trigger(&drop_stmt, &mut db);
    assert!(result.is_ok(), "DROP TRIGGER IF EXISTS on missing trigger should be a no-op");
}

#[test]
fn test_create_trigger_all_variations() {
    let mut db = Database::new();

    // Create a table first
    let create_table_sql = "CREATE TABLE test_table (id INT, name VARCHAR(255));";
    let create_table_stmt = vibesql_parser::Parser::parse_sql(create_table_sql).unwrap();
    match create_table_stmt {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }

    // A view is required for the INSTEAD OF variation: SQLite (and now VibeSQL)
    // rejects INSTEAD OF on a table and BEFORE/AFTER on a view, so each timing
    // must target a compatible object.
    match vibesql_parser::Parser::parse_sql("CREATE VIEW test_view AS SELECT * FROM test_table;")
        .unwrap()
    {
        vibesql_ast::Statement::CreateView(stmt) => {
            crate::advanced_objects::execute_create_view(&stmt, &mut db).unwrap();
        }
        other => panic!("Expected CreateView, got {:?}", other),
    }

    // Test different timing values, each on a valid target object.
    let timings = vec![
        (TriggerTiming::Before, "before", "test_table"),
        (TriggerTiming::After, "after", "test_table"),
        (TriggerTiming::InsteadOf, "insteadof", "test_view"),
    ];

    for (timing, suffix, target) in timings {
        let stmt = CreateTriggerStmt {
            if_not_exists: false,
            schema: None,
            trigger_name: format!("trigger_{}", suffix),
            name_source: None,
            timing: timing.clone(),
            event: TriggerEvent::Insert,
            table_name: target.to_string(),
            granularity: TriggerGranularity::Row,
            when_condition: None,
            triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
        };

        let result = crate::advanced_objects::execute_create_trigger(&stmt, &mut db);
        assert!(result.is_ok(), "Failed to create {} trigger: {:?}", suffix, result.err());

        let trigger = db.catalog.get_trigger(&format!("trigger_{}", suffix)).unwrap();
        assert_eq!(trigger.timing, timing);
    }
}

// ---------------------------------------------------------------------------
// Transactional DDL: CREATE TRIGGER participates in the surrounding
// transaction (#5497 / trigger1-1.3).
//
// SQLite (3.51.0) treats trigger DDL as transactional: a `BEGIN; CREATE
// TRIGGER ...; ROLLBACK;` leaves no trigger behind and raises no error,
// while a matching COMMIT keeps it. These tests assert that via live
// catalog introspection through the same `TriggerExecutor` /
// `Database` transaction path the CLI uses.
// ---------------------------------------------------------------------------

fn make_test_table(db: &mut Database) {
    let sql = "CREATE TABLE t1 (a INT, b INT, c INT);";
    match vibesql_parser::Parser::parse_sql(sql).unwrap() {
        vibesql_ast::Statement::CreateTable(stmt) => {
            CreateTableExecutor::execute(&stmt, db).unwrap();
        }
        _ => panic!("Expected CreateTable"),
    }
}

fn create_trigger_stmt(name: &str) -> CreateTriggerStmt {
    CreateTriggerStmt {
        if_not_exists: false,
        schema: None,
        trigger_name: name.to_string(),
        name_source: None,
        timing: TriggerTiming::After,
        event: TriggerEvent::Insert,
        table_name: "t1".to_string(),
        granularity: TriggerGranularity::Row,
        when_condition: None,
        triggered_action: TriggerAction::RawSql("SELECT 1;".to_string()),
    }
}

#[test]
fn test_create_trigger_rolled_back_with_transaction() {
    // BEGIN; CREATE TRIGGER tr2 ...; ROLLBACK; => no error, no trigger left.
    let mut db = Database::new();
    make_test_table(&mut db);

    db.begin_transaction().expect("BEGIN");
    crate::TriggerExecutor::create_trigger(&mut db, &create_trigger_stmt("tr2"))
        .expect("CREATE TRIGGER inside txn");
    // Visible to its own transaction before rollback.
    assert!(db.catalog.get_trigger("tr2").is_some(), "trigger visible inside txn");

    // ROLLBACK must succeed (no "No active transaction to rollback") and undo
    // the catalog mutation.
    db.rollback_transaction().expect("ROLLBACK after CREATE TRIGGER");
    assert!(
        db.catalog.get_trigger("tr2").is_none(),
        "trigger must be gone after ROLLBACK (transactional DDL, #5497)"
    );

    // And re-creating it now succeeds (mirrors trigger1-1.3's second CREATE).
    crate::TriggerExecutor::create_trigger(&mut db, &create_trigger_stmt("tr2"))
        .expect("re-CREATE TRIGGER after rollback");
    assert!(db.catalog.get_trigger("tr2").is_some());
}

#[test]
fn test_create_trigger_kept_after_commit() {
    // BEGIN; CREATE TRIGGER tr2 ...; COMMIT; => trigger persists.
    let mut db = Database::new();
    make_test_table(&mut db);

    db.begin_transaction().expect("BEGIN");
    crate::TriggerExecutor::create_trigger(&mut db, &create_trigger_stmt("tr2"))
        .expect("CREATE TRIGGER inside txn");
    db.commit_transaction().expect("COMMIT after CREATE TRIGGER");

    assert!(
        db.catalog.get_trigger("tr2").is_some(),
        "trigger must survive COMMIT (transactional DDL, #5497)"
    );
}

#[test]
fn test_drop_trigger_rolled_back_with_transaction() {
    // Committed trigger, then BEGIN; DROP TRIGGER; ROLLBACK; => trigger restored.
    let mut db = Database::new();
    make_test_table(&mut db);
    crate::TriggerExecutor::create_trigger(&mut db, &create_trigger_stmt("tr2"))
        .expect("CREATE TRIGGER");

    db.begin_transaction().expect("BEGIN");
    crate::TriggerExecutor::drop_trigger(
        &mut db,
        &DropTriggerStmt { trigger_name: "tr2".to_string(), cascade: false, if_exists: false },
    )
    .expect("DROP TRIGGER inside txn");
    assert!(db.catalog.get_trigger("tr2").is_none(), "trigger dropped inside txn");

    db.rollback_transaction().expect("ROLLBACK after DROP TRIGGER");
    assert!(
        db.catalog.get_trigger("tr2").is_some(),
        "dropped trigger must be restored after ROLLBACK (#5497)"
    );
}

/// ALTER TABLE RENAME must rewrite references to the renamed table inside an
/// existing trigger body, keep `sqlite_master.sql` consistent (verbatim, with
/// only the renamed identifier double-quoted), and the trigger must still fire
/// correctly against the new table name. Mirrors SQLite (legacy_alter_table=OFF)
/// behavior exercised by `altertrig.test`. Regression coverage for issue #5489.
#[test]
fn test_rename_table_rewrites_trigger_body_and_fires() {
    use crate::{AlterTableExecutor, InsertExecutor, SelectExecutor};

    let mut db = Database::new();

    // Set up: t1 fires a trigger that reads from t3; log captures the count.
    for sql in [
        "CREATE TABLE t1 (x INTEGER, d VARCHAR(50));",
        "CREATE TABLE t2 (y INTEGER);",
        "CREATE TABLE t3 (z INTEGER);",
        "CREATE TABLE log (msg VARCHAR(50));",
    ] {
        match vibesql_parser::Parser::parse_sql(sql).unwrap() {
            vibesql_ast::Statement::CreateTable(stmt) => {
                CreateTableExecutor::execute(&stmt, &mut db).unwrap();
            }
            other => panic!("Expected CreateTable, got {:?}", other),
        }
    }

    // Create the trigger preserving the original SQL text (the path the CLI /
    // bindings use) so `sqlite_master.sql` round-trips verbatim.
    let trigger_sql = "CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN\n  \
        INSERT INTO log VALUES('fired-' || (SELECT count(*) FROM t3));\nEND";
    match vibesql_parser::Parser::parse_sql(trigger_sql).unwrap() {
        vibesql_ast::Statement::CreateTrigger(stmt) => {
            crate::TriggerExecutor::create_trigger_with_sql(&mut db, &stmt, Some(trigger_sql))
                .unwrap();
        }
        other => panic!("Expected CreateTrigger, got {:?}", other),
    }

    // Seed t3 so the trigger has something to count.
    for sql in ["INSERT INTO t3 VALUES (1);", "INSERT INTO t3 VALUES (2);"] {
        match vibesql_parser::Parser::parse_sql(sql).unwrap() {
            vibesql_ast::Statement::Insert(stmt) => {
                InsertExecutor::execute(&mut db, &stmt).unwrap();
            }
            other => panic!("Expected Insert, got {:?}", other),
        }
    }

    // Rename t3 -> t5.
    match vibesql_parser::Parser::parse_sql("ALTER TABLE t3 RENAME TO t5;").unwrap() {
        vibesql_ast::Statement::AlterTable(stmt) => {
            AlterTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        other => panic!("Expected AlterTable, got {:?}", other),
    }

    // sqlite_master.sql must show the rewritten reference, double-quoted, with
    // the rest preserved verbatim (no injected BEFORE / FOR EACH ROW).
    let schema = crate::sqlite_schema::execute_sqlite_schema_query(&db.catalog).unwrap();
    let trigger_row = schema
        .rows
        .iter()
        .find(|r| matches!(&r.values[0], vibesql_types::SqlValue::Varchar(s) if s.as_str() == "trigger"))
        .expect("trigger row in sqlite_master");
    let sql_text = match &trigger_row.values[4] {
        vibesql_types::SqlValue::Varchar(s) => s.to_string(),
        other => panic!("expected sql text, got {:?}", other),
    };
    let expected = "CREATE TRIGGER r1 AFTER INSERT ON t1 BEGIN\n  \
        INSERT INTO log VALUES('fired-' || (SELECT count(*) FROM \"t5\"));\nEND";
    assert_eq!(sql_text, expected, "sqlite_master.sql not rewritten to match SQLite");

    // The trigger must still fire against the renamed table. Inserting into t1
    // counts rows in t5 (the former t3), so the logged message is "fired-2".
    match vibesql_parser::Parser::parse_sql("INSERT INTO t1 VALUES (10, 'a');").unwrap() {
        vibesql_ast::Statement::Insert(stmt) => {
            InsertExecutor::execute(&mut db, &stmt).unwrap();
        }
        other => panic!("Expected Insert, got {:?}", other),
    }

    let select = match vibesql_parser::Parser::parse_sql("SELECT msg FROM log;").unwrap() {
        vibesql_ast::Statement::Select(stmt) => stmt,
        other => panic!("Expected Select, got {:?}", other),
    };
    let rows = SelectExecutor::new(&db).execute(&select).expect("select from log");
    assert_eq!(rows.len(), 1, "trigger should have inserted exactly one log row");
    assert_eq!(
        rows[0].values[0],
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("fired-2")),
        "trigger did not fire correctly against the renamed table"
    );
}

/// `CREATE TEMP TRIGGER` parses, registers with the temp schema recorded on the
/// catalog definition, and fires on its target table. Regression coverage for
/// issue #5521 (triggerD TEMP/TEMPORARY support).
#[test]
fn test_create_temp_trigger_parses_records_schema_and_fires() {
    use crate::{InsertExecutor, SelectExecutor};

    let mut db = Database::new();

    for sql in ["CREATE TABLE t1 (x INTEGER);", "CREATE TABLE log (y INTEGER);"] {
        match vibesql_parser::Parser::parse_sql(sql).unwrap() {
            vibesql_ast::Statement::CreateTable(stmt) => {
                CreateTableExecutor::execute(&stmt, &mut db).unwrap();
            }
            other => panic!("Expected CreateTable, got {:?}", other),
        }
    }

    // CREATE TEMP TRIGGER must parse and create successfully.
    let trigger_sql =
        "CREATE TEMP TRIGGER tr1 AFTER INSERT ON t1 BEGIN INSERT INTO log VALUES(new.x + 100); END";
    match vibesql_parser::Parser::parse_sql(trigger_sql).unwrap() {
        vibesql_ast::Statement::CreateTrigger(stmt) => {
            assert_eq!(stmt.schema.as_deref(), Some("temp"), "TEMP modifier sets schema");
            crate::TriggerExecutor::create_trigger(&mut db, &stmt).unwrap();
        }
        other => panic!("Expected CreateTrigger, got {:?}", other),
    }

    // The temp schema is recorded on the stored catalog definition.
    let def = db.catalog.get_trigger("tr1").expect("temp trigger registered");
    assert!(def.is_temp(), "stored trigger must record the temp schema");

    // The temp trigger fires on its target table.
    match vibesql_parser::Parser::parse_sql("INSERT INTO t1 VALUES (5);").unwrap() {
        vibesql_ast::Statement::Insert(stmt) => {
            InsertExecutor::execute(&mut db, &stmt).unwrap();
        }
        other => panic!("Expected Insert, got {:?}", other),
    }

    let select = match vibesql_parser::Parser::parse_sql("SELECT y FROM log;").unwrap() {
        vibesql_ast::Statement::Select(stmt) => stmt,
        other => panic!("Expected Select, got {:?}", other),
    };
    let rows = SelectExecutor::new(&db).execute(&select).expect("select from log");
    assert_eq!(rows.len(), 1, "temp trigger should have fired once");
    assert_eq!(
        rows[0].values[0],
        vibesql_types::SqlValue::Integer(105),
        "temp trigger body did not fire with the inserted NEW row"
    );
}
