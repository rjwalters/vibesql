//! Phase 2 Tests: Parameter Binding and Execution

use super::*;

#[test]
fn test_call_procedure_with_in_parameter() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE greet(IN name VARCHAR(50))
    // BEGIN
    //   DECLARE greeting VARCHAR(100);
    //   SET greeting = name;
    // END;
    let create_proc = CreateProcedureStmt {
        procedure_name: "greet".to_string(),
        parameters: vec![ProcedureParameter {
            mode: ParameterMode::In,
            name: "name".to_string(),
            data_type: DataType::Varchar { max_length: Some(50) },
        }],
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "greeting".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "greeting".to_string(),
                value: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
            },
        ]),
        sql_security: None,
        comment: None,
        language: None,
    };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    // CALL greet('Alice');
    let call = CallStmt {
        procedure_name: "greet".to_string(),
        arguments: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Alice")))],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_call_procedure_with_multiple_parameters() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE add_numbers(IN a INT, IN b INT)
    // BEGIN
    //   DECLARE result INT;
    //   SET result = a + b;
    // END;
    let create_proc = CreateProcedureStmt {
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
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "result".to_string(),
                data_type: DataType::Integer,
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "result".to_string(),
                value: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
                }),
            },
        ]),
        sql_security: None,
        comment: None,
        language: None,
    };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    // CALL add_numbers(10, 20);
    let call = CallStmt {
        procedure_name: "add_numbers".to_string(),
        arguments: vec![
            Expression::Literal(SqlValue::Integer(10)),
            Expression::Literal(SqlValue::Integer(20)),
        ],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_declare_with_default_value() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE test_defaults()
    // BEGIN
    //   DECLARE counter INT DEFAULT 42;
    //   DECLARE message VARCHAR(20) DEFAULT 'Hello';
    // END;
    let create_proc = CreateProcedureStmt {
        procedure_name: "test_defaults".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "counter".to_string(),
                data_type: DataType::Integer,
                default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(42)))),
            },
            ProceduralStatement::Declare {
                name: "message".to_string(),
                data_type: DataType::Varchar { max_length: Some(20) },
                default_value: Some(Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Hello"))))),
            },
        ]),
        sql_security: None,
        comment: None,
        language: None,
    };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt { procedure_name: "test_defaults".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_variable_in_expression() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE test_math()
    // BEGIN
    //   DECLARE x INT DEFAULT 10;
    //   DECLARE y INT;
    //   SET y = x * 2;
    // END;
    let create_proc = CreateProcedureStmt {
        procedure_name: "test_math".to_string(),
        parameters: vec![],
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "x".to_string(),
                data_type: DataType::Integer,
                default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(10)))),
            },
            ProceduralStatement::Declare {
                name: "y".to_string(),
                data_type: DataType::Integer,
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "y".to_string(),
                value: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef { table: None, column: "x".to_string() }),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expression::Literal(SqlValue::Integer(2))),
                }),
            },
        ]),
        sql_security: None,
        comment: None,
        language: None,
    };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt { procedure_name: "test_math".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_concat_function_in_procedure() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE test_concat(IN first VARCHAR(50), IN last VARCHAR(50))
    // BEGIN
    //   DECLARE full_name VARCHAR(100);
    //   SET full_name = CONCAT(first, ' ', last);
    // END;
    let create_proc = CreateProcedureStmt {
        procedure_name: "test_concat".to_string(),
        parameters: vec![
            ProcedureParameter {
                mode: ParameterMode::In,
                name: "first".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
            },
            ProcedureParameter {
                mode: ParameterMode::In,
                name: "last".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
            },
        ],
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "full_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "full_name".to_string(),
                value: Box::new(Expression::Function {
                    name: "CONCAT".to_string(),
                    args: vec![
                        Expression::ColumnRef { table: None, column: "first".to_string() },
                        Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(" "))),
                        Expression::ColumnRef { table: None, column: "last".to_string() },
                    ],
                    character_unit: None,
                }),
            },
        ]),
        sql_security: None,
        comment: None,
        language: None,
    };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "test_concat".to_string(),
        arguments: vec![
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("John"))),
            Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Doe"))),
        ],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_parameter_count_mismatch() {
    let mut db = setup_test_db();

    let create_proc = CreateProcedureStmt {
        procedure_name: "needs_param".to_string(),
        parameters: vec![ProcedureParameter {
            mode: ParameterMode::In,
            name: "x".to_string(),
            data_type: DataType::Integer,
        }],
        body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
    };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    // Call with no arguments (expects 1)
    let call = CallStmt { procedure_name: "needs_param".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::ParameterCountMismatch { .. }));
}

#[test]
fn test_out_parameter_not_yet_supported() {
    let mut db = setup_test_db();

    let create_proc = CreateProcedureStmt {
        procedure_name: "test_out".to_string(),
        parameters: vec![ProcedureParameter {
            mode: ParameterMode::Out,
            name: "result".to_string(),
            data_type: DataType::Integer,
        }],
        body: ProcedureBody::BeginEnd(vec![]),
        sql_security: None,
        comment: None,
        language: None,
    };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "test_out".to_string(),
        arguments: vec![Expression::Literal(SqlValue::Integer(0))],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    // OUT parameters require a variable target, not a literal
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::Other(msg) if msg.contains("OUT/INOUT")));
}

#[test]
fn test_procedure_not_found() {
    let mut db = setup_test_db();

    let call = CallStmt { procedure_name: "nonexistent".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::ProcedureNotFound { .. }));
}
