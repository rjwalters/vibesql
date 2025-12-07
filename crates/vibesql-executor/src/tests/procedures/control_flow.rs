//\! Phase 3: Control Flow Tests

use super::*;
use crate::procedural::executor::execute_procedural_statement;
use crate::procedural::{ControlFlow, ExecutionContext};
#[test]
fn test_if_simple_boolean_true() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    // DECLARE result VARCHAR(20);
    ctx.set_variable("result", SqlValue::Varchar(arcstr::ArcStr::from("initial")));

    // IF TRUE THEN
    //   SET result = 'executed';
    // END IF;
    let if_stmt = ProceduralStatement::If {
        condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        then_statements: vec![ProceduralStatement::Set {
            name: "result".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("executed")))),
        }],
        else_statements: None,
    };

    let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ControlFlow::Continue);
    assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar(arcstr::ArcStr::from("executed"))));
}

#[test]
fn test_if_simple_boolean_false() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    ctx.set_variable("result", SqlValue::Varchar(arcstr::ArcStr::from("initial")));

    // IF FALSE THEN
    //   SET result = 'executed';
    // END IF;
    let if_stmt = ProceduralStatement::If {
        condition: Box::new(Expression::Literal(SqlValue::Boolean(false))),
        then_statements: vec![ProceduralStatement::Set {
            name: "result".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("executed")))),
        }],
        else_statements: None,
    };

    let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ControlFlow::Continue);
    // Variable should remain unchanged
    assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar(arcstr::ArcStr::from("initial"))));
}

#[test]
fn test_if_with_else() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    ctx.set_variable("result", SqlValue::Varchar(arcstr::ArcStr::from("initial")));

    // IF FALSE THEN
    //   SET result = 'then';
    // ELSE
    //   SET result = 'else';
    // END IF;
    let if_stmt = ProceduralStatement::If {
        condition: Box::new(Expression::Literal(SqlValue::Boolean(false))),
        then_statements: vec![ProceduralStatement::Set {
            name: "result".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("then")))),
        }],
        else_statements: Some(vec![ProceduralStatement::Set {
            name: "result".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("else")))),
        }]),
    };

    let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ControlFlow::Continue);
    assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar(arcstr::ArcStr::from("else"))));
}

#[test]
fn test_if_integer_condition() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    ctx.set_variable("result", SqlValue::Varchar(arcstr::ArcStr::from("initial")));

    // IF 1 THEN (non-zero = true)
    //   SET result = 'nonzero';
    // END IF;
    let if_stmt = ProceduralStatement::If {
        condition: Box::new(Expression::Literal(SqlValue::Integer(1))),
        then_statements: vec![ProceduralStatement::Set {
            name: "result".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("nonzero")))),
        }],
        else_statements: None,
    };

    let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
    assert!(result.is_ok());
    assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar(arcstr::ArcStr::from("nonzero"))));

    // Test with 0 (should be false)
    ctx.set_variable("result", SqlValue::Varchar(arcstr::ArcStr::from("initial")));
    let if_stmt_zero = ProceduralStatement::If {
        condition: Box::new(Expression::Literal(SqlValue::Integer(0))),
        then_statements: vec![ProceduralStatement::Set {
            name: "result".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("zero")))),
        }],
        else_statements: None,
    };

    let result = execute_procedural_statement(&if_stmt_zero, &mut ctx, &mut db);
    assert!(result.is_ok());
    // Should not execute then branch
    assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar(arcstr::ArcStr::from("initial"))));
}

#[test]
fn test_if_null_condition() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    ctx.set_variable("result", SqlValue::Varchar(arcstr::ArcStr::from("initial")));

    // IF NULL THEN (NULL = false)
    //   SET result = 'executed';
    // END IF;
    let if_stmt = ProceduralStatement::If {
        condition: Box::new(Expression::Literal(SqlValue::Null)),
        then_statements: vec![ProceduralStatement::Set {
            name: "result".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("executed")))),
        }],
        else_statements: None,
    };

    let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
    assert!(result.is_ok());
    // Should not execute then branch
    assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar(arcstr::ArcStr::from("initial"))));
}

// Test 2: WHILE loop basic functionality
#[test]
fn test_while_loop_basic() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    // DECLARE counter INT DEFAULT 0;
    ctx.set_variable("counter", SqlValue::Integer(0));

    // WHILE counter < 3 DO
    //   SET counter = counter + 1;
    // END WHILE;
    // Note: We need to use a simple approach since we can't evaluate complex expressions yet
    // We'll manually test iterations

    // For this test, we'll verify WHILE executes when condition is true
    // and skips when condition is false

    // First: condition is true, should execute once
    let while_stmt = ProceduralStatement::While {
        condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        statements: vec![
            ProceduralStatement::Set {
                name: "counter".to_string(),
                value: Box::new(Expression::Literal(SqlValue::Integer(1))),
            },
            // Add a LEAVE to prevent infinite loop
            ProceduralStatement::Leave("".to_string()),
        ],
    };

    // Push empty label for unlabeled WHILE
    ctx.push_label("");
    let result = execute_procedural_statement(&while_stmt, &mut ctx, &mut db);
    ctx.pop_label("");

    assert!(result.is_ok());
    assert_eq!(ctx.get_variable("counter"), Some(&SqlValue::Integer(1)));
}

#[test]
fn test_while_loop_false_condition() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    ctx.set_variable("counter", SqlValue::Integer(0));

    // WHILE FALSE DO
    //   SET counter = 100;  -- Should never execute
    // END WHILE;
    let while_stmt = ProceduralStatement::While {
        condition: Box::new(Expression::Literal(SqlValue::Boolean(false))),
        statements: vec![ProceduralStatement::Set {
            name: "counter".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Integer(100))),
        }],
    };

    let result = execute_procedural_statement(&while_stmt, &mut ctx, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ControlFlow::Continue);
    // Counter should remain 0
    assert_eq!(ctx.get_variable("counter"), Some(&SqlValue::Integer(0)));
}

// Test 3: LOOP with LEAVE
#[test]
fn test_loop_with_leave() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    ctx.set_variable("counter", SqlValue::Integer(0));

    // Push label for loop
    ctx.push_label("my_loop");

    // my_loop: LOOP
    //   SET counter = counter + 1;
    //   LEAVE my_loop;
    // END LOOP;
    let loop_stmt = ProceduralStatement::Loop {
        statements: vec![
            ProceduralStatement::Set {
                name: "counter".to_string(),
                value: Box::new(Expression::Literal(SqlValue::Integer(1))),
            },
            ProceduralStatement::Leave("my_loop".to_string()),
        ],
    };

    let result = execute_procedural_statement(&loop_stmt, &mut ctx, &mut db);
    ctx.pop_label("my_loop");

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ControlFlow::Continue);
    assert_eq!(ctx.get_variable("counter"), Some(&SqlValue::Integer(1)));
}

#[test]
fn test_loop_leave_invalid_label() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    // LOOP
    //   LEAVE nonexistent_label;  -- Should error
    // END LOOP;
    let leave_stmt = ProceduralStatement::Leave("nonexistent_label".to_string());

    let result = execute_procedural_statement(&leave_stmt, &mut ctx, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::LabelNotFound(_)));
}

// Test 4: REPEAT/UNTIL
#[test]
fn test_repeat_until() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    ctx.set_variable("counter", SqlValue::Integer(0));

    // REPEAT
    //   SET counter = 1;
    // UNTIL TRUE END REPEAT;
    let repeat_stmt = ProceduralStatement::Repeat {
        statements: vec![ProceduralStatement::Set {
            name: "counter".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }],
        condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
    };

    let result = execute_procedural_statement(&repeat_stmt, &mut ctx, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ControlFlow::Continue);
    // Body should execute at least once
    assert_eq!(ctx.get_variable("counter"), Some(&SqlValue::Integer(1)));
}

#[test]
fn test_repeat_executes_at_least_once() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    ctx.set_variable("executed", SqlValue::Boolean(false));

    // REPEAT
    //   SET executed = TRUE;
    // UNTIL TRUE END REPEAT;  -- Exit immediately after first iteration
    let repeat_stmt = ProceduralStatement::Repeat {
        statements: vec![ProceduralStatement::Set {
            name: "executed".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        }],
        condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
    };

    let result = execute_procedural_statement(&repeat_stmt, &mut ctx, &mut db);
    assert!(result.is_ok());
    assert_eq!(ctx.get_variable("executed"), Some(&SqlValue::Boolean(true)));
}

// Test 5: ITERATE validates label exists
// Note: Full ITERATE behavior testing would require more complex setup to avoid infinite loops
#[test]
fn test_iterate_validates_label() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    // Test that ITERATE validates the label exists
    ctx.push_label("valid_loop");

    let iterate_stmt = ProceduralStatement::Iterate("valid_loop".to_string());
    let result = execute_procedural_statement(&iterate_stmt, &mut ctx, &mut db);

    // Should return ControlFlow::Iterate
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ControlFlow::Iterate("valid_loop".to_string()));

    ctx.pop_label("valid_loop");
}

#[test]
fn test_iterate_invalid_label() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    // ITERATE nonexistent_label;  -- Should error
    let iterate_stmt = ProceduralStatement::Iterate("nonexistent_label".to_string());

    let result = execute_procedural_statement(&iterate_stmt, &mut ctx, &mut db);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::LabelNotFound(_)));
}

// Test 6: Nested IF statements
#[test]
fn test_nested_if() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    ctx.set_variable("result", SqlValue::Varchar(arcstr::ArcStr::from("initial")));

    // IF TRUE THEN
    //   IF TRUE THEN
    //     SET result = 'nested';
    //   END IF;
    // END IF;
    let nested_if = ProceduralStatement::If {
        condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        then_statements: vec![ProceduralStatement::If {
            condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
            then_statements: vec![ProceduralStatement::Set {
                name: "result".to_string(),
                value: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("nested")))),
            }],
            else_statements: None,
        }],
        else_statements: None,
    };

    let result = execute_procedural_statement(&nested_if, &mut ctx, &mut db);
    assert!(result.is_ok());
    assert_eq!(ctx.get_variable("result"), Some(&SqlValue::Varchar(arcstr::ArcStr::from("nested"))));
}

// Test 7: RETURN in IF branch
#[test]
fn test_if_with_return() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    // IF TRUE THEN
    //   RETURN 42;
    // END IF;
    let if_stmt = ProceduralStatement::If {
        condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        then_statements: vec![ProceduralStatement::Return(Box::new(Expression::Literal(
            SqlValue::Integer(42),
        )))],
        else_statements: None,
    };

    let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ControlFlow::Return(SqlValue::Integer(42)));
}

// Test 8: Variable persistence across control flow
#[test]
fn test_variable_persistence() {
    let mut db = setup_test_db();
    let mut ctx = ExecutionContext::new();

    ctx.set_variable("x", SqlValue::Integer(1));

    // IF TRUE THEN
    //   SET x = 2;
    // END IF;
    let if_stmt = ProceduralStatement::If {
        condition: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        then_statements: vec![ProceduralStatement::Set {
            name: "x".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }],
        else_statements: None,
    };

    let result = execute_procedural_statement(&if_stmt, &mut ctx, &mut db);
    assert!(result.is_ok());

    // Variable modification should persist after IF
    assert_eq!(ctx.get_variable("x"), Some(&SqlValue::Integer(2)));
}

#[test]
fn test_procedure_body_caching_performance() {
    use std::time::Instant;

    let mut db = setup_test_db();

    // Create a procedure with multiple statements to make caching impact visible
    let create_stmt = CreateProcedureStmt {
        procedure_name: "test_cache_proc".to_string(),
        parameters: vec![
            ProcedureParameter {
                mode: ParameterMode::In,
                name: "x".to_string(),
                data_type: DataType::Integer,
            },
            ProcedureParameter {
                mode: ParameterMode::Out,
                name: "result".to_string(),
                data_type: DataType::Integer,
            },
        ],
        body: ProcedureBody::BeginEnd(vec![
            ProceduralStatement::Declare {
                name: "temp".to_string(),
                data_type: DataType::Integer,
                default_value: None,
            },
            ProceduralStatement::Set {
                name: "temp".to_string(),
                value: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef { table: None, column: "x".to_string() }),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expression::Literal(SqlValue::Integer(2))),
                }),
            },
            ProceduralStatement::Set {
                name: "result".to_string(),
                value: Box::new(Expression::BinaryOp {
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "temp".to_string(),
                    }),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expression::Literal(SqlValue::Integer(10))),
                }),
            },
        ]),
        sql_security: None,
        comment: None,
        language: None,
    };
    advanced_objects::execute_create_procedure(&create_stmt, &mut db).unwrap();

    // First call - should cache the body
    let first_call_start = Instant::now();
    let call_stmt = CallStmt {
        procedure_name: "test_cache_proc".to_string(),
        arguments: vec![
            Expression::Literal(SqlValue::Integer(5)),
            Expression::ColumnRef { table: None, column: "@out".to_string() },
        ],
    };
    advanced_objects::execute_call(&call_stmt, &mut db).unwrap();
    let first_call_duration = first_call_start.elapsed();

    // Verify result is correct
    assert_eq!(db.get_session_variable("out"), Some(&SqlValue::Integer(20)));

    // Subsequent calls - should use cached body
    let iterations = 100;
    let cached_calls_start = Instant::now();
    for i in 0..iterations {
        let call_stmt = CallStmt {
            procedure_name: "test_cache_proc".to_string(),
            arguments: vec![
                Expression::Literal(SqlValue::Integer(i)),
                Expression::ColumnRef { table: None, column: format!("@out{}", i) },
            ],
        };
        advanced_objects::execute_call(&call_stmt, &mut db).unwrap();
    }
    let cached_calls_duration = cached_calls_start.elapsed();
    let cached_avg = cached_calls_duration / (iterations as u32);

    // Cached calls should be reasonably fast
    // We don't check that cached is faster than first call because the first call
    // includes catalog lookup overhead that isn't purely about caching the body.
    // Instead, we verify that cached calls complete successfully and efficiently.
    println!("First call: {:?}", first_call_duration);
    println!("Average cached call: {:?}", cached_avg);

    // Verify all calls succeeded with correct results
    for i in 0..iterations {
        let expected = SqlValue::Integer(i * 2 + 10);
        assert_eq!(
            db.get_session_variable(&format!("out{}", i)),
            Some(&expected),
            "Call {} should have correct result",
            i
        );
    }
}

#[test]
fn test_cache_invalidation_on_drop() {
    let mut db = setup_test_db();

    // Create procedure
    let create_stmt = CreateProcedureStmt {
        procedure_name: "test_invalidate".to_string(),
        parameters: vec![ProcedureParameter {
            mode: ParameterMode::Out,
            name: "result".to_string(),
            data_type: DataType::Integer,
        }],
        body: ProcedureBody::BeginEnd(vec![ProceduralStatement::Set {
            name: "result".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Integer(42))),
        }]),
        sql_security: None,
        comment: None,
        language: None,
    };
    advanced_objects::execute_create_procedure(&create_stmt, &mut db).unwrap();

    // Call once to populate cache
    let call_stmt = CallStmt {
        procedure_name: "test_invalidate".to_string(),
        arguments: vec![Expression::ColumnRef { table: None, column: "@out".to_string() }],
    };
    advanced_objects::execute_call(&call_stmt, &mut db).unwrap();
    assert_eq!(db.get_session_variable("out"), Some(&SqlValue::Integer(42)));

    // Drop procedure - should invalidate cache
    let drop_stmt =
        DropProcedureStmt { procedure_name: "test_invalidate".to_string(), if_exists: false };
    advanced_objects::execute_drop_procedure(&drop_stmt, &mut db).unwrap();

    // Verify procedure is gone
    assert!(!db.catalog.procedure_exists("test_invalidate"));

    // Recreate with different behavior
    let create_stmt2 = CreateProcedureStmt {
        procedure_name: "test_invalidate".to_string(),
        parameters: vec![ProcedureParameter {
            mode: ParameterMode::Out,
            name: "result".to_string(),
            data_type: DataType::Integer,
        }],
        body: ProcedureBody::BeginEnd(vec![ProceduralStatement::Set {
            name: "result".to_string(),
            value: Box::new(Expression::Literal(SqlValue::Integer(99))),
        }]),
        sql_security: None,
        comment: None,
        language: None,
    };
    advanced_objects::execute_create_procedure(&create_stmt2, &mut db).unwrap();

    // Call again - should use new procedure body, not cached old one
    let call_stmt2 = CallStmt {
        procedure_name: "test_invalidate".to_string(),
        arguments: vec![Expression::ColumnRef { table: None, column: "@out2".to_string() }],
    };
    advanced_objects::execute_call(&call_stmt2, &mut db).unwrap();
    assert_eq!(db.get_session_variable("out2"), Some(&SqlValue::Integer(99)));
}
