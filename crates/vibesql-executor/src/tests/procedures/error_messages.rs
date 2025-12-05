//! Error Message Tests (Issue #1445)

use super::*;

#[test]
fn test_procedure_not_found_error_message() {
    let mut db = setup_test_db();

    // Create some procedures for the suggestions
    let proc1 = CreateProcedureStmt {
        procedure_name: "my_proc".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
    };
    advanced_objects::execute_create_procedure(&proc1, &mut db).unwrap();

    let proc2 = CreateProcedureStmt {
        procedure_name: "test_proc".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
    };
    advanced_objects::execute_create_procedure(&proc2, &mut db).unwrap();

    // Try to call non-existent procedure with similar name
    let call_stmt = CallStmt {
        procedure_name: "my_proce".to_string(), // Typo: should be "my_proc"
        arguments: vec![],
    };

    let result = advanced_objects::execute_call(&call_stmt, &mut db);
    assert!(result.is_err());

    let error_msg = result.unwrap_err().to_string();
    // Should contain the procedure name
    assert!(error_msg.contains("my_proce"));
    // Should list available procedures
    assert!(error_msg.contains("my_proc") || error_msg.contains("test_proc"));
    // Should have a suggestion
    assert!(error_msg.contains("Did you mean"));
}

#[test]
fn test_parameter_count_mismatch_error_message() {
    let mut db = setup_test_db();

    // Create procedure with 2 parameters
    let proc = CreateProcedureStmt {
        procedure_name: "add_numbers".to_string(),
        parameters: vec![
            ProcedureParameter {
                mode: ParameterMode::In,
                name: "a".to_string(),
                data_type: DataType::Integer,
            },
            ProcedureParameter {
                mode: ParameterMode::In,
                name: "b".to_string(),
                data_type: DataType::Integer,
            },
        ],
        body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
    };
    advanced_objects::execute_create_procedure(&proc, &mut db).unwrap();

    // Try to call with only 1 argument
    let call_stmt = CallStmt {
        procedure_name: "add_numbers".to_string(),
        arguments: vec![Expression::Literal(SqlValue::Integer(5))],
    };

    let result = advanced_objects::execute_call(&call_stmt, &mut db);
    assert!(result.is_err());

    let error_msg = result.unwrap_err().to_string();
    // Should mention the procedure name
    assert!(error_msg.contains("add_numbers"));
    // Should show expected count
    assert!(error_msg.contains("2 parameter"));
    // Should show actual count
    assert!(error_msg.contains("1 argument"));
    // Should show parameter signature
    assert!(error_msg.contains("IN a"));
    assert!(error_msg.contains("IN b"));
}

#[test]
fn test_variable_not_found_error_message() {
    use crate::procedural::executor::execute_procedural_statement;
    use crate::procedural::ExecutionContext;

    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    // Set up some variables in context
    ctx.set_variable("x", SqlValue::Integer(1));
    ctx.set_variable("y", SqlValue::Integer(2));
    ctx.set_parameter("count", SqlValue::Integer(10));

    // Try to reference non-existent variable
    let set_stmt = ProceduralStatement::Set {
        name: "result".to_string(), // Variable doesn't exist
        value: Box::new(Expression::Literal(SqlValue::Integer(42))),
    };

    let result = execute_procedural_statement(&set_stmt, &mut ctx, &mut db);
    assert!(result.is_err());

    let error_msg = result.unwrap_err().to_string();
    // Should mention the missing variable
    assert!(error_msg.contains("result"));
    // Should list available variables
    assert!(error_msg.contains("Available variables"));
    // Should contain the variables we defined (may be uppercase)
    let error_upper = error_msg.to_uppercase();
    assert!(error_upper.contains("X"));
    assert!(error_upper.contains("Y"));
    assert!(error_upper.contains("COUNT"));
}

#[test]
fn test_parameter_type_documentation() {
    // This test documents that parameter type mismatches will be caught
    // when we add type checking in parameter binding
    // For now, this is a placeholder for future type checking tests

    // TODO Phase 7: Add parameter type mismatch tests when type checking is implemented
    // Expected error message format:
    // "Parameter 'x' expects INT, got VARCHAR 'hello'"
}
