//! Tests for stored function functionality and edge cases

use vibesql_ast::*;
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

use crate::advanced_objects;

// Helper function to create a simple function with defaults for Phase 6 fields
fn create_simple_function(
    name: &str,
    parameters: Vec<FunctionParameter>,
    return_type: DataType,
    body: Vec<ProceduralStatement>,
) -> CreateFunctionStmt {
    CreateFunctionStmt {
        function_name: name.to_string(),
        parameters,
        return_type,
        body: ProcedureBody::BeginEnd(body),
        deterministic: None,
        sql_security: None,
        comment: None,
        language: None,
    }
}

#[test]
fn test_create_function_simple() {
    let mut db = Database::new();

    // CREATE FUNCTION get_answer() RETURNS INT
    // BEGIN
    //   RETURN 42;
    // END;
    let func = create_simple_function(
        "get_answer",
        vec![],
        DataType::Integer,
        vec![ProceduralStatement::Return(Box::new(Expression::Literal(SqlValue::Integer(42))))],
    );

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());

    // Verify function was created
    assert!(db.catalog.function_exists("get_answer"));
}

#[test]
fn test_drop_function_simple() {
    let mut db = Database::new();

    let create_func = create_simple_function(
        "test_func",
        vec![],
        DataType::Integer,
        vec![ProceduralStatement::Return(Box::new(Expression::Literal(SqlValue::Integer(0))))],
    );

    advanced_objects::execute_create_function(&create_func, &mut db).unwrap();
    assert!(db.catalog.function_exists("test_func"));

    let drop_func = DropFunctionStmt { function_name: "test_func".to_string(), if_exists: false };

    let result = advanced_objects::execute_drop_function(&drop_func, &mut db);
    assert!(result.is_ok());
    assert!(!db.catalog.function_exists("test_func"));
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[test]
fn test_function_empty_body_with_return() {
    let mut db = Database::new();

    // CREATE FUNCTION get_zero() RETURNS INT
    // BEGIN
    //   RETURN 0;
    // END;
    let func = create_simple_function(
        "get_zero",
        vec![],
        DataType::Integer,
        vec![ProceduralStatement::Return(Box::new(Expression::Literal(SqlValue::Integer(0))))],
    );

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_function_return_null() {
    let mut db = Database::new();

    // CREATE FUNCTION get_null() RETURNS INT
    // BEGIN
    //   RETURN NULL;
    // END;
    let func = create_simple_function(
        "get_null",
        vec![],
        DataType::Integer,
        vec![ProceduralStatement::Return(Box::new(Expression::Literal(SqlValue::Null)))],
    );

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_function_with_parameter() {
    let mut db = Database::new();

    // CREATE FUNCTION double_value(x INT) RETURNS INT
    // BEGIN
    //   DECLARE result INT;
    //   SET result = x * 2;
    //   RETURN result;
    // END;
    let func = create_simple_function(
        "double_value",
        vec![FunctionParameter { name: "x".to_string(), data_type: DataType::Integer }],
        DataType::Integer,
        vec![
            ProceduralStatement::Declare {
                name: "result".to_string(),
                data_type: DataType::Integer,
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "result".to_string(),
                value: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef { table: None, column: "x".to_string() }),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expression::Literal(SqlValue::Integer(2))),
                }),
            },
            ProceduralStatement::Return(Box::new(Expression::ColumnRef {
                table: None,
                column: "result".to_string(),
            })),
        ],
    );

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_function_parameter_shadowing() {
    let mut db = Database::new();

    // CREATE FUNCTION shadow_test(x INT) RETURNS INT
    // BEGIN
    //   DECLARE x INT DEFAULT 100;  -- Shadows parameter
    //   RETURN x;  -- Returns local variable, not parameter
    // END;
    let func = create_simple_function(
        "shadow_test",
        vec![FunctionParameter { name: "x".to_string(), data_type: DataType::Integer }],
        DataType::Integer,
        vec![
            ProceduralStatement::Declare {
                name: "x".to_string(),
                data_type: DataType::Integer,
                default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(100)))),
            },
            ProceduralStatement::Return(Box::new(Expression::ColumnRef {
                table: None,
                column: "x".to_string(),
            })),
        ],
    );

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_function_with_special_chars() {
    let mut db = Database::new();

    // CREATE FUNCTION `func-with-dash`() RETURNS INT
    // BEGIN
    //   RETURN 42;
    // END;
    let func = create_simple_function(
        "func-with-dash",
        vec![],
        DataType::Integer,
        vec![ProceduralStatement::Return(Box::new(Expression::Literal(SqlValue::Integer(42))))],
    );

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_function_with_spaces_in_name() {
    let mut db = Database::new();

    // CREATE FUNCTION `func with spaces`() RETURNS INT
    // BEGIN
    //   RETURN 42;
    // END;
    let func = create_simple_function(
        "func with spaces",
        vec![],
        DataType::Integer,
        vec![ProceduralStatement::Return(Box::new(Expression::Literal(SqlValue::Integer(42))))],
    );

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_function_multiple_parameters() {
    let mut db = Database::new();

    // CREATE FUNCTION add_three(a INT, b INT, c INT) RETURNS INT
    // BEGIN
    //   DECLARE result INT;
    //   SET result = a + b;
    //   SET result = result + c;
    //   RETURN result;
    // END;
    let func = create_simple_function(
        "add_three",
        vec![
            FunctionParameter { name: "a".to_string(), data_type: DataType::Integer },
            FunctionParameter { name: "b".to_string(), data_type: DataType::Integer },
            FunctionParameter { name: "c".to_string(), data_type: DataType::Integer },
        ],
        DataType::Integer,
        vec![
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
            ProceduralStatement::Set {
                name: "result".to_string(),
                value: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "result".to_string(),
                    }),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expression::ColumnRef { table: None, column: "c".to_string() }),
                }),
            },
            ProceduralStatement::Return(Box::new(Expression::ColumnRef {
                table: None,
                column: "result".to_string(),
            })),
        ],
    );

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_function_varchar_return_type() {
    let mut db = Database::new();

    // CREATE FUNCTION get_greeting(name VARCHAR(50)) RETURNS VARCHAR(100)
    // BEGIN
    //   DECLARE greeting VARCHAR(100);
    //   SET greeting = CONCAT('Hello, ', name);
    //   RETURN greeting;
    // END;
    let func = create_simple_function(
        "get_greeting",
        vec![FunctionParameter {
            name: "name".to_string(),
            data_type: DataType::Varchar { max_length: Some(50) },
        }],
        DataType::Varchar { max_length: Some(100) },
        vec![
            ProceduralStatement::Declare {
                name: "greeting".to_string(),
                data_type: DataType::Varchar { max_length: Some(100) },
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "greeting".to_string(),
                value: Box::new(Expression::Function {
                    name: "CONCAT".to_string(),
                    args: vec![
                        Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Hello, "))),
                        Expression::ColumnRef { table: None, column: "name".to_string() },
                    ],
                    character_unit: None,
                }),
            },
            ProceduralStatement::Return(Box::new(Expression::ColumnRef {
                table: None,
                column: "greeting".to_string(),
            })),
        ],
    );

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_function_long_body() {
    let mut db = Database::new();

    // Create a function with many statements
    let mut statements = vec![];

    statements.push(ProceduralStatement::Declare {
        name: "counter".to_string(),
        data_type: DataType::Integer,
        default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(0)))),
    });

    // Add 50 SET statements
    for i in 1..=50 {
        statements.push(ProceduralStatement::Set {
            name: "counter".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Integer(i))),
        });
    }

    statements.push(ProceduralStatement::Return(Box::new(Expression::ColumnRef {
        table: None,
        column: "counter".to_string(),
    })));

    let func = create_simple_function("long_function", vec![], DataType::Integer, statements);

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_function_with_control_flow() {
    let mut db = Database::new();

    // CREATE FUNCTION conditional_return(x INT) RETURNS INT
    // BEGIN
    //   IF x > 0 THEN
    //     RETURN x;
    //   ELSE
    //     RETURN 0;
    //   END IF;
    // END;
    let func = create_simple_function(
        "conditional_return",
        vec![FunctionParameter { name: "x".to_string(), data_type: DataType::Integer }],
        DataType::Integer,
        vec![ProceduralStatement::If {
            condition: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "x".to_string() }),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(0))),
            }),
            then_statements: vec![ProceduralStatement::Return(Box::new(Expression::ColumnRef {
                table: None,
                column: "x".to_string(),
            }))],
            else_statements: Some(vec![ProceduralStatement::Return(Box::new(
                Expression::Literal(SqlValue::Integer(0)),
            ))]),
        }],
    );

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_function_with_deeply_nested_if() {
    let mut db = Database::new();

    // Create a function with nested IF statements
    let innermost_return =
        ProceduralStatement::Return(Box::new(Expression::Literal(SqlValue::Integer(5))));

    let mut nested_if = ProceduralStatement::If {
        condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        then_statements: vec![innermost_return],
        else_statements: None,
    };

    // Add 5 more levels of nesting
    for i in (0..=4).rev() {
        nested_if = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            then_statements: vec![nested_if],
            else_statements: Some(vec![ProceduralStatement::Return(Box::new(
                Expression::Literal(SqlValue::Integer(i)),
            ))]),
        };
    }

    let func = create_simple_function("nested_func", vec![], DataType::Integer, vec![nested_if]);

    let result = advanced_objects::execute_create_function(&func, &mut db);
    assert!(result.is_ok());
}
