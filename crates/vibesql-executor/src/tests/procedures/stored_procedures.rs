//! Basic stored procedure tests (CREATE, DROP, CALL)

use super::*;

#[test]
fn test_create_procedure_simple() {
    let mut db = setup_test_db();

    let proc = CreateProcedureStmt {
        procedure_name: "test_proc".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
    };

    let result = advanced_objects::execute_create_procedure(&proc, &mut db);
    assert!(result.is_ok());

    // Verify procedure was created
    assert!(db.catalog.procedure_exists("test_proc"));
}

#[test]
fn test_drop_procedure_simple() {
    let mut db = setup_test_db();

    let create_proc = CreateProcedureStmt {
        procedure_name: "test_proc".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
    };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();
    assert!(db.catalog.procedure_exists("test_proc"));

    let drop_proc = DropProcedureStmt { procedure_name: "test_proc".to_string(), if_exists: false };

    let result = advanced_objects::execute_drop_procedure(&drop_proc, &mut db);
    assert!(result.is_ok());
    assert!(!db.catalog.procedure_exists("test_proc"));
}

#[test]
fn test_drop_procedure_if_exists_not_found() {
    let mut db = setup_test_db();

    let drop_proc =
        DropProcedureStmt { procedure_name: "nonexistent".to_string(), if_exists: true };

    // Should not error with if_exists
    let result = advanced_objects::execute_drop_procedure(&drop_proc, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_drop_procedure_without_if_exists_not_found() {
    let mut db = setup_test_db();

    let drop_proc =
        DropProcedureStmt { procedure_name: "nonexistent".to_string(), if_exists: false };

    // Should error without if_exists
    let result = advanced_objects::execute_drop_procedure(&drop_proc, &mut db);
    assert!(result.is_err());
}

#[test]
fn test_call_procedure_simple() {
    let mut db = setup_test_db();
    setup_test_table(&mut db);

    let create_proc = CreateProcedureStmt {
        procedure_name: "test_proc".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
    };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt { procedure_name: "test_proc".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    // Phase 2: Empty procedures should execute successfully
    assert!(result.is_ok());
}
