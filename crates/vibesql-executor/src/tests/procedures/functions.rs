//! Function tests (CREATE, DROP)

use super::*;

#[test]
fn test_create_function_simple() {
    let mut db = setup_test_db();

    let func = CreateFunctionStmt {
        function_name: "test_func".to_string(),
        parameters: vec![],
        return_type: DataType::Integer,
        body: ProcedureBody::BeginEnd(vec![]),
        deterministic: None,
        sql_security: None,
        comment: None,
        language: None,
    };

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());

    // Verify function was created
    assert!(db.catalog.function_exists("test_func"));
}

#[test]
fn test_drop_function_simple() {
    let mut db = setup_test_db();

    let create_func = CreateFunctionStmt {
        function_name: "test_func".to_string(),
        parameters: vec![],
        return_type: DataType::Integer,
        body: ProcedureBody::BeginEnd(vec![]),
        deterministic: None,
        sql_security: None,
        comment: None,
        language: None,
    };

    advanced_objects::execute_create_function(&create_func, &mut db).unwrap();
    assert!(db.catalog.function_exists("test_func"));

    let drop_func = DropFunctionStmt { function_name: "test_func".to_string(), if_exists: false };

    let result = advanced_objects::execute_drop_function(&drop_func, &mut db);
    assert!(result.is_ok());
    assert!(!db.catalog.function_exists("test_func"));
}
