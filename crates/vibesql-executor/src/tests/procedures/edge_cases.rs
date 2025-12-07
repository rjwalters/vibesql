//! Phase 6 Robustness: Edge Case Tests

use super::*;
use crate::procedural::ExecutionContext;

// Edge Case 1: Empty Procedure Body
#[test]
fn test_empty_procedure_body() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE do_nothing()
    // BEGIN
    // END;
    let create_proc = create_empty_procedure("do_nothing");

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    // CALL do_nothing();
    let call = CallStmt { procedure_name: "do_nothing".to_string(), arguments: vec![] };

    // Should succeed with no operations
    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

// Edge Case 2: NULL Parameter Values
#[test]
fn test_null_parameter_in_procedure() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE handle_null(IN x INT)
    // BEGIN
    //   DECLARE result INT;
    //   SET result = x;  -- result becomes NULL
    // END;
    let create_proc = create_procedure_with_in_param(
        "handle_null",
        "x",
        DataType::Integer,
        vec![
            ProceduralStatement::Declare {
                name: "result".to_string(),
                data_type: DataType::Integer,
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "result".to_string(),
                value: Box::new(Expression::ColumnRef { table: None, column: "x".to_string() }),
            },
        ],
    );

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    // CALL handle_null(NULL);
    let call = CallStmt {
        procedure_name: "handle_null".to_string(),
        arguments: vec![Expression::Literal(SqlValue::Null)],
    };

    // Should handle NULL parameter gracefully
    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_declare_with_null_default() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE test_null_default()
    // BEGIN
    //   DECLARE x INT;  -- Defaults to NULL
    //   DECLARE y INT DEFAULT NULL;  -- Explicitly NULL
    // END;
    let create_proc = create_simple_procedure(
        "test_null_default",
        vec![],
        vec![
            ProceduralStatement::Declare {
                name: "x".to_string(),
                data_type: DataType::Integer,
                default_value: None, // Implicitly NULL
            },
            ProceduralStatement::Declare {
                name: "y".to_string(),
                data_type: DataType::Integer,
                default_value: Some(Box::new(Expression::Literal(SqlValue::Null))),
            },
        ],
    );

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt { procedure_name: "test_null_default".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

// Edge Case 3: Parameter Name Conflicts with Variables
#[test]
fn test_parameter_variable_shadowing() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE test_shadowing(IN x INT)
    // BEGIN
    //   DECLARE x INT DEFAULT 10;  -- Shadows parameter
    //   -- This is allowed in SQL but inner scope takes precedence
    // END;
    let create_proc = create_procedure_with_in_param(
        "test_shadowing",
        "x",
        DataType::Integer,
        vec![ProceduralStatement::Declare {
            name: "x".to_string(),
            data_type: DataType::Integer,
            default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(10)))),
        }],
    );

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "test_shadowing".to_string(),
        arguments: vec![Expression::Literal(SqlValue::Integer(5))],
    };

    // Should succeed - local variable shadows parameter
    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

// Edge Case 4: Very Long Procedure Bodies
#[test]
fn test_very_long_procedure_body() {
    let mut db = setup_test_db();

    // Create a procedure with 100 SET statements
    let mut statements = vec![];

    // Declare initial variable
    statements.push(ProceduralStatement::Declare {
        name: "counter".to_string(),
        data_type: DataType::Integer,
        default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(0)))),
    });

    // Add 100 SET statements
    for i in 1..=100 {
        statements.push(ProceduralStatement::Set {
            name: "counter".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Integer(i))),
        });
    }

    let create_proc = create_simple_procedure("long_procedure", vec![], statements);

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt { procedure_name: "long_procedure".to_string(), arguments: vec![] };

    // Should handle long body without issues
    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

// Edge Case 5: Deeply Nested Control Flow
#[test]
fn test_deeply_nested_control_flow() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    ctx.set_variable("result", SqlValue::Integer(0));

    // Create 10 levels of nested IF statements
    let innermost = vec![ProceduralStatement::Set {
        name: "result".to_string(),
        value: Box::new(Expression::Literal(SqlValue::Integer(10))),
    }];

    let mut nested_stmt = ProceduralStatement::If {
        condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        then_statements: innermost.clone(),
        else_statements: None,
    };

    // Nest 9 more levels
    for _ in 0..9 {
        nested_stmt = ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            then_statements: vec![nested_stmt],
            else_statements: None,
        };
    }

    let result =
        crate::procedural::executor::execute_procedural_statement(&nested_stmt, &mut ctx, &mut db);

    assert!(result.is_ok());
    assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Integer(10)));
}

// Edge Case 6: Special Characters in Names
#[test]
fn test_procedure_with_special_chars_in_name() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE `proc-with-dash`()
    // BEGIN
    //   DECLARE result INT DEFAULT 42;
    // END;
    let create_proc = create_simple_procedure(
        "proc-with-dash",
        vec![],
        vec![ProceduralStatement::Declare {
            name: "result".to_string(),
            data_type: DataType::Integer,
            default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(42)))),
        }],
    );

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt { procedure_name: "proc-with-dash".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_procedure_with_spaces_in_name() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE `proc with spaces`()
    let create_proc = create_empty_procedure("proc with spaces");

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt { procedure_name: "proc with spaces".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

// Edge Case 7: Large Numbers in Arithmetic
#[test]
fn test_large_number_arithmetic() {
    let mut db = setup_test_db();

    // CREATE PROCEDURE test_large_numbers()
    // BEGIN
    //   DECLARE large INT DEFAULT 1000000;
    //   DECLARE result INT;
    //   SET result = large * 2;
    // END;
    let create_proc = create_simple_procedure(
        "test_large_numbers",
        vec![],
        vec![
            ProceduralStatement::Declare {
                name: "large".to_string(),
                data_type: DataType::Integer,
                default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(1_000_000)))),
            },
            ProceduralStatement::Declare {
                name: "result".to_string(),
                data_type: DataType::Integer,
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "result".to_string(),
                value: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "large".to_string(),
                    }),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expression::Literal(SqlValue::Integer(2))),
                }),
            },
        ],
    );

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt { procedure_name: "test_large_numbers".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

// Edge Case 8: Multiple Variables with Same Operations
#[test]
fn test_multiple_variable_operations() {
    let mut db = setup_test_db();

    // Test that multiple variables don't interfere with each other
    let create_proc = create_simple_procedure(
        "test_multi_vars",
        vec![],
        vec![
            ProceduralStatement::Declare {
                name: "a".to_string(),
                data_type: DataType::Integer,
                default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(1)))),
            },
            ProceduralStatement::Declare {
                name: "b".to_string(),
                data_type: DataType::Integer,
                default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(2)))),
            },
            ProceduralStatement::Declare {
                name: "c".to_string(),
                data_type: DataType::Integer,
                default_value: Some(Box::new(Expression::Literal(SqlValue::Integer(3)))),
            },
            ProceduralStatement::Set {
                name: "a".to_string(),
                value: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expression::ColumnRef { table: None, column: "c".to_string() }),
                }),
            },
        ],
    );

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt { procedure_name: "test_multi_vars".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

// ============================================================================
// Phase 3: Procedural SELECT INTO Tests
// ============================================================================

#[test]
fn test_procedural_select_into_single_column() {
    let mut db = setup_test_db();
    setup_test_table(&mut db);

    // Insert test data
    db.insert_row(
        "users",
        Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    )
    .unwrap();

    // CREATE PROCEDURE get_user_name(IN user_id INT)
    // BEGIN
    //   DECLARE user_name VARCHAR(50);
    //   SELECT name INTO user_name FROM users WHERE id = user_id;
    // END;
    let create_proc = create_procedure_with_in_param(
        "get_user_name",
        "user_id",
        DataType::Integer,
        vec![
            ProceduralStatement::Declare {
                name: "user_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                default_value: None,
            },
            ProceduralStatement::Sql(Box::new(Statement::Select(Box::new(SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![SelectItem::Expression {
                    expr: Expression::ColumnRef { table: None, column: "name".to_string() },
                    alias: None,
                }],
                into_table: None,
                into_variables: Some(vec!["user_name".to_string()]),
                from: Some(FromClause::Table {
                    name: "users".to_string(),
                    alias: None,
                    column_aliases: None,
                }),
                where_clause: Some(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "user_id".to_string(),
                    }),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            })))),
        ],
    );

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "get_user_name".to_string(),
        arguments: vec![Expression::Literal(SqlValue::Integer(1))],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_procedural_select_into_multiple_columns() {
    let mut db = setup_test_db();
    setup_test_table(&mut db);

    // Insert test data
    db.insert_row(
        "users",
        Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    )
    .unwrap();

    // CREATE PROCEDURE get_user_info(IN user_id INT)
    // BEGIN
    //   DECLARE user_id_out INT;
    //   DECLARE user_name VARCHAR(50);
    //   SELECT id, name INTO user_id_out, user_name FROM users WHERE id = user_id;
    // END;
    let create_proc = create_procedure_with_in_param(
        "get_user_info",
        "user_id",
        DataType::Integer,
        vec![
            ProceduralStatement::Declare {
                name: "user_id_out".to_string(),
                data_type: DataType::Integer,
                default_value: None,
            },
            ProceduralStatement::Declare {
                name: "user_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                default_value: None,
            },
            ProceduralStatement::Sql(Box::new(Statement::Select(Box::new(SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![
                    SelectItem::Expression {
                        expr: Expression::ColumnRef { table: None, column: "id".to_string() },
                        alias: None,
                    },
                    SelectItem::Expression {
                        expr: Expression::ColumnRef { table: None, column: "name".to_string() },
                        alias: None,
                    },
                ],
                into_table: None,
                into_variables: Some(vec!["user_id_out".to_string(), "user_name".to_string()]),
                from: Some(FromClause::Table {
                    name: "users".to_string(),
                    alias: None,
                    column_aliases: None,
                }),
                where_clause: Some(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "user_id".to_string(),
                    }),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            })))),
        ],
    );

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "get_user_info".to_string(),
        arguments: vec![Expression::Literal(SqlValue::Integer(1))],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_ok());
}

#[test]
fn test_procedural_select_into_error_no_rows() {
    let mut db = setup_test_db();
    setup_test_table(&mut db);

    // No data inserted - SELECT INTO will fail

    // CREATE PROCEDURE get_user_name(IN user_id INT)
    // BEGIN
    //   DECLARE user_name VARCHAR(50);
    //   SELECT name INTO user_name FROM users WHERE id = user_id;  -- Should fail: no rows
    // END;
    let create_proc = CreateProcedureStmt {
        procedure_name: "get_user_name".to_string(),
        parameters: vec![ProcedureParameter {
            mode: ParameterMode::In,
            name: "user_id".to_string(),
            data_type: DataType::Integer,
        }],
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "user_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                default_value: None,
            },
            ProceduralStatement::Sql(Box::new(Statement::Select(Box::new(SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![SelectItem::Expression {
                    expr: Expression::ColumnRef { table: None, column: "name".to_string() },
                    alias: None,
                }],
                into_table: None,
                into_variables: Some(vec!["user_name".to_string()]),
                from: Some(FromClause::Table {
                    name: "users".to_string(),
                    alias: None,
                    column_aliases: None,
                }),
                where_clause: Some(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "user_id".to_string(),
                    }),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            })))),
        ]),
        sql_security: None,
        comment: None,
        language: None,
    };

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt {
        procedure_name: "get_user_name".to_string(),
        arguments: vec![Expression::Literal(SqlValue::Integer(1))],
    };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ExecutorError::SelectIntoRowCount { expected: 1, actual: 0 }
    ));
}

#[test]
fn test_procedural_select_into_error_multiple_rows() {
    let mut db = setup_test_db();
    setup_test_table(&mut db);

    // Insert multiple rows
    db.insert_row(
        "users",
        Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
    )
    .unwrap();

    // CREATE PROCEDURE get_all_names()
    // BEGIN
    //   DECLARE user_name VARCHAR(50);
    //   SELECT name INTO user_name FROM users;  -- Should fail: multiple rows
    // END;
    let create_proc = create_simple_procedure(
        "get_all_names",
        vec![],
        vec![
            ProceduralStatement::Declare {
                name: "user_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                default_value: None,
            },
            ProceduralStatement::Sql(Box::new(Statement::Select(Box::new(SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![SelectItem::Expression {
                    expr: Expression::ColumnRef { table: None, column: "name".to_string() },
                    alias: None,
                }],
                into_table: None,
                into_variables: Some(vec!["user_name".to_string()]),
                from: Some(FromClause::Table {
                    name: "users".to_string(),
                    alias: None,
                    column_aliases: None,
                }),
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            })))),
        ],
    );

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt { procedure_name: "get_all_names".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ExecutorError::SelectIntoRowCount { expected: 1, actual: 2 }
    ));
}

#[test]
fn test_procedural_select_into_error_column_count_mismatch() {
    let mut db = setup_test_db();
    setup_test_table(&mut db);

    // Insert test data
    db.insert_row(
        "users",
        Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    )
    .unwrap();

    // CREATE PROCEDURE get_user_info()
    // BEGIN
    //   DECLARE user_name VARCHAR(50);
    //   SELECT id, name INTO user_name FROM users WHERE id = 1;  -- Should fail: 2 columns, 1 variable
    // END;
    let create_proc = create_simple_procedure(
        "get_user_info",
        vec![],
        vec![
            ProceduralStatement::Declare {
                name: "user_name".to_string(),
                data_type: DataType::Varchar { max_length: Some(50) },
                default_value: None,
            },
            ProceduralStatement::Sql(Box::new(Statement::Select(Box::new(SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![
                    SelectItem::Expression {
                        expr: Expression::ColumnRef { table: None, column: "id".to_string() },
                        alias: None,
                    },
                    SelectItem::Expression {
                        expr: Expression::ColumnRef { table: None, column: "name".to_string() },
                        alias: None,
                    },
                ],
                into_table: None,
                into_variables: Some(vec!["user_name".to_string()]),
                from: Some(FromClause::Table {
                    name: "users".to_string(),
                    alias: None,
                    column_aliases: None,
                }),
                where_clause: Some(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                    op: BinaryOperator::Equal,
                    right: Box::new(Expression::Literal(SqlValue::Integer(1))),
                }),
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            })))),
        ],
    );

    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    let call = CallStmt { procedure_name: "get_user_info".to_string(), arguments: vec![] };

    let result = advanced_objects::execute_call(&call, &mut db);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ExecutorError::SelectIntoColumnCount { expected: 1, actual: 2 }
    ));
}
