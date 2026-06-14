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
        trigger_name: "my_trigger".to_string(),
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
        trigger_name: "my_trigger".to_string(),
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
        trigger_name: "my_trigger".to_string(),
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
        trigger_name: "my_trigger".to_string(),
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

    // Test different timing values
    let timings = vec![
        (TriggerTiming::Before, "before"),
        (TriggerTiming::After, "after"),
        (TriggerTiming::InsteadOf, "insteadof"),
    ];

    for (timing, suffix) in timings {
        let stmt = CreateTriggerStmt {
            if_not_exists: false,
            trigger_name: format!("trigger_{}", suffix),
            timing: timing.clone(),
            event: TriggerEvent::Insert,
            table_name: "test_table".to_string(),
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
        trigger_name: name.to_string(),
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
