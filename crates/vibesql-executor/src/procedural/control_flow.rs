//! Control flow execution for procedural statements
//!
//! Implements:
//! - IF/ELSEIF/ELSE conditionals
//! - WHILE loops
//! - LOOP (infinite loop with LEAVE)
//! - REPEAT/UNTIL loops

use vibesql_ast::ProceduralStatement;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

// Use the evaluate_expression function from the executor module
use super::executor::evaluate_expression;
use crate::{
    errors::ExecutorError,
    procedural::{ControlFlow, ExecutionContext},
};

/// Execute IF statement
pub fn execute_if(
    condition: &vibesql_ast::Expression,
    then_statements: &[ProceduralStatement],
    else_statements: &Option<Vec<ProceduralStatement>>,
    ctx: &mut ExecutionContext,
    db: &mut Database,
) -> Result<ControlFlow, ExecutorError> {
    // Evaluate condition
    let condition_value = evaluate_expression(condition, db, ctx)?;

    // Check if condition is true
    let is_true = match condition_value {
        SqlValue::Boolean(b) => b,
        SqlValue::Integer(i) => i != 0,
        SqlValue::Null => false,
        _ => {
            return Err(ExecutorError::TypeError(format!(
                "IF condition must evaluate to boolean, got {:?}",
                condition_value
            )))
        }
    };

    // Execute appropriate branch
    if is_true {
        for stmt in then_statements {
            let flow = super::executor::execute_procedural_statement(stmt, ctx, db)?;
            if flow != ControlFlow::Continue {
                return Ok(flow);
            }
        }
    } else if let Some(else_stmts) = else_statements {
        for stmt in else_stmts {
            let flow = super::executor::execute_procedural_statement(stmt, ctx, db)?;
            if flow != ControlFlow::Continue {
                return Ok(flow);
            }
        }
    }

    Ok(ControlFlow::Continue)
}

/// Execute WHILE loop
pub fn execute_while(
    condition: &vibesql_ast::Expression,
    statements: &[ProceduralStatement],
    ctx: &mut ExecutionContext,
    db: &mut Database,
) -> Result<ControlFlow, ExecutorError> {
    loop {
        // Evaluate condition
        let condition_value = evaluate_expression(condition, db, ctx)?;

        // Check if condition is true
        let is_true = match condition_value {
            SqlValue::Boolean(b) => b,
            SqlValue::Integer(i) => i != 0,
            SqlValue::Null => false,
            _ => {
                return Err(ExecutorError::TypeError(format!(
                    "WHILE condition must evaluate to boolean, got {:?}",
                    condition_value
                )))
            }
        };

        if !is_true {
            break;
        }

        // Execute loop body
        for stmt in statements {
            let flow = super::executor::execute_procedural_statement(stmt, ctx, db)?;
            match flow {
                ControlFlow::Continue => {}
                ControlFlow::Return(_) => return Ok(flow),
                ControlFlow::Leave(_) => return Ok(flow),
                ControlFlow::Iterate(_) => break, // Continue to next iteration
            }
        }
    }

    Ok(ControlFlow::Continue)
}

/// Execute LOOP (infinite loop until LEAVE)
pub fn execute_loop(
    statements: &[ProceduralStatement],
    ctx: &mut ExecutionContext,
    db: &mut Database,
) -> Result<ControlFlow, ExecutorError> {
    loop {
        for stmt in statements {
            let flow = super::executor::execute_procedural_statement(stmt, ctx, db)?;
            match flow {
                ControlFlow::Continue => {}
                ControlFlow::Return(_) => return Ok(flow),
                ControlFlow::Leave(_) => return Ok(ControlFlow::Continue), // Exit loop
                ControlFlow::Iterate(_) => break,                          /* Continue to next
                                                                             * iteration */
            }
        }
    }
}

/// Execute REPEAT/UNTIL loop
pub fn execute_repeat(
    statements: &[ProceduralStatement],
    condition: &vibesql_ast::Expression,
    ctx: &mut ExecutionContext,
    db: &mut Database,
) -> Result<ControlFlow, ExecutorError> {
    loop {
        // Execute loop body first
        for stmt in statements {
            let flow = super::executor::execute_procedural_statement(stmt, ctx, db)?;
            match flow {
                ControlFlow::Continue => {}
                ControlFlow::Return(_) => return Ok(flow),
                ControlFlow::Leave(_) => return Ok(flow),
                ControlFlow::Iterate(_) => break, // Continue to next iteration
            }
        }

        // Evaluate condition after body
        let condition_value = evaluate_expression(condition, db, ctx)?;

        // Check if condition is true (exit condition)
        let should_exit = match condition_value {
            SqlValue::Boolean(b) => b,
            SqlValue::Integer(i) => i != 0,
            SqlValue::Null => false,
            _ => {
                return Err(ExecutorError::TypeError(format!(
                    "REPEAT UNTIL condition must evaluate to boolean, got {:?}",
                    condition_value
                )))
            }
        };

        if should_exit {
            break;
        }
    }

    Ok(ControlFlow::Continue)
}
