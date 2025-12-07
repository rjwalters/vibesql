//! Execute individual procedural statements
//!
//! Handles execution of:
//! - DECLARE (variable declarations)
//! - SET (variable assignments)
//! - SQL statements (SELECT, INSERT, UPDATE, DELETE, etc.)
//! - RETURN (return from function/procedure)
//! - Control flow (delegated to control_flow module)

use crate::errors::ExecutorError;
use crate::procedural::{ControlFlow, ExecutionContext};
use vibesql_ast::{ProceduralStatement, Statement};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute a procedural statement
pub fn execute_procedural_statement(
    stmt: &ProceduralStatement,
    ctx: &mut ExecutionContext,
    db: &mut Database,
) -> Result<ControlFlow, ExecutorError> {
    match stmt {
        ProceduralStatement::Declare { name, data_type, default_value } => {
            // Declare a local variable
            let value = if let Some(expr) = default_value {
                // Evaluate default value expression
                evaluate_expression(expr, db, ctx)?
            } else {
                // No default, use NULL
                SqlValue::Null
            };

            // Store variable with type checking
            let typed_value = cast_to_type(value, data_type)?;
            ctx.set_variable(name, typed_value);
            Ok(ControlFlow::Continue)
        }

        ProceduralStatement::Set { name, value } => {
            // Set variable, parameter, or session variable value
            let new_value = evaluate_expression(value, db, ctx)?;

            // Check if it's a session variable (starts with @)
            if let Some(var_name) = name.strip_prefix('@') {
                // Strip @ prefix
                db.set_session_variable(var_name, new_value);
            } else if ctx.has_parameter(name) {
                // Try to update parameter first (for OUT/INOUT)
                if let Some(param) = ctx.get_parameter_mut(name) {
                    *param = new_value;
                }
            } else if ctx.has_variable(name) {
                // Update local variable
                ctx.set_variable(name, new_value);
            } else {
                return Err(ExecutorError::VariableNotFound {
                    variable_name: name.clone(),
                    available_variables: ctx.get_available_names(),
                });
            }

            Ok(ControlFlow::Continue)
        }

        ProceduralStatement::Return(expr) => {
            // Evaluate and return the expression
            let value = evaluate_expression(expr, db, ctx)?;
            Ok(ControlFlow::Return(value))
        }

        ProceduralStatement::Leave(label) => {
            // Check if label exists
            if !ctx.has_label(label) {
                return Err(ExecutorError::LabelNotFound(label.clone()));
            }
            Ok(ControlFlow::Leave(label.clone()))
        }

        ProceduralStatement::Iterate(label) => {
            // Check if label exists
            if !ctx.has_label(label) {
                return Err(ExecutorError::LabelNotFound(label.clone()));
            }
            Ok(ControlFlow::Iterate(label.clone()))
        }

        ProceduralStatement::If { condition, then_statements, else_statements } => {
            super::control_flow::execute_if(condition, then_statements, else_statements, ctx, db)
        }

        ProceduralStatement::While { condition, statements } => {
            super::control_flow::execute_while(condition, statements, ctx, db)
        }

        ProceduralStatement::Loop { statements } => {
            super::control_flow::execute_loop(statements, ctx, db)
        }

        ProceduralStatement::Repeat { statements, condition } => {
            super::control_flow::execute_repeat(statements, condition, ctx, db)
        }

        ProceduralStatement::Sql(sql_stmt) => {
            // Execute SQL statement
            execute_sql_statement(sql_stmt, db, ctx)?;
            Ok(ControlFlow::Continue)
        }
    }
}

/// Evaluate an expression in the procedural context
///
/// This function evaluates expressions with access to local variables and parameters.
/// Phase 2 supports simple expressions with variable references and basic operations.
pub fn evaluate_expression(
    expr: &vibesql_ast::Expression,
    _db: &mut Database,
    ctx: &ExecutionContext,
) -> Result<SqlValue, ExecutorError> {
    use vibesql_ast::{BinaryOperator, Expression};

    match expr {
        // Variable, parameter, or session variable reference
        Expression::ColumnRef { table: None, column } => {
            // Check if it's a session variable (starts with @)
            if let Some(var_name) = column.strip_prefix('@') {
                // Strip @ prefix
                _db.get_session_variable(var_name).cloned().ok_or_else(|| {
                    ExecutorError::VariableNotFound {
                        variable_name: format!("@{}", var_name),
                        available_variables: vec![], // Session variables not listed
                    }
                })
            } else {
                // Regular variable or parameter reference
                ctx.get_value(column).cloned().ok_or_else(|| ExecutorError::VariableNotFound {
                    variable_name: column.clone(),
                    available_variables: ctx.get_available_names(),
                })
            }
        }

        // Literal values
        Expression::Literal(value) => Ok(value.clone()),

        // Binary operations (basic arithmetic and comparison)
        Expression::BinaryOp { left, op, right } => {
            let left_val = evaluate_expression(left, _db, ctx)?;
            let right_val = evaluate_expression(right, _db, ctx)?;

            match op {
                BinaryOperator::Plus => {
                    // Simple addition for integers
                    match (left_val, right_val) {
                        (SqlValue::Integer(l), SqlValue::Integer(r)) => {
                            Ok(SqlValue::Integer(l + r))
                        }
                        _ => Err(ExecutorError::TypeError(
                            "Binary operation only supports integers in Phase 2".to_string(),
                        )),
                    }
                }
                BinaryOperator::Minus => match (left_val, right_val) {
                    (SqlValue::Integer(l), SqlValue::Integer(r)) => Ok(SqlValue::Integer(l - r)),
                    _ => Err(ExecutorError::TypeError(
                        "Binary operation only supports integers in Phase 2".to_string(),
                    )),
                },
                BinaryOperator::Multiply => match (left_val, right_val) {
                    (SqlValue::Integer(l), SqlValue::Integer(r)) => Ok(SqlValue::Integer(l * r)),
                    _ => Err(ExecutorError::TypeError(
                        "Binary operation only supports integers in Phase 2".to_string(),
                    )),
                },
                BinaryOperator::GreaterThan => match (left_val, right_val) {
                    (SqlValue::Integer(l), SqlValue::Integer(r)) => Ok(SqlValue::Boolean(l > r)),
                    _ => Err(ExecutorError::TypeError(
                        "Comparison only supports integers in Phase 2".to_string(),
                    )),
                },
                _ => Err(ExecutorError::UnsupportedFeature(format!(
                    "Binary operator {:?} not yet supported in procedural expressions",
                    op
                ))),
            }
        }

        // Function calls - basic support for CONCAT
        Expression::Function { name, args, .. } if name.eq_ignore_ascii_case("CONCAT") => {
            let mut result = String::new();
            for arg in args {
                let val = evaluate_expression(arg, _db, ctx)?;
                result.push_str(&val.to_string());
            }
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(result)))
        }

        // Other expressions not yet supported
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Expression type not yet supported in procedures: {:?}",
            expr
        ))),
    }
}

/// Execute a SQL statement within a procedural context
///
/// **Phase 3 Implementation**
///
/// This function executes SQL statements with access to procedural variables and parameters.
/// Supports:
/// - SELECT with procedural context (results discarded)
/// - Procedural SELECT INTO (results stored in variables)
/// - INSERT/UPDATE/DELETE with procedural variables (requires PR #1565)
fn execute_sql_statement(
    stmt: &Statement,
    db: &mut Database,
    ctx: &mut ExecutionContext,
) -> Result<(), ExecutorError> {
    match stmt {
        Statement::Select(select_stmt) => {
            // Check if this is procedural SELECT INTO (storing results in variables)
            if let Some(into_vars) = &select_stmt.into_variables {
                // Execute SELECT with procedural context
                let executor = crate::SelectExecutor::new_with_procedural_context(db, ctx);
                let results = executor.execute(select_stmt)?;

                // Validate exactly one row returned
                if results.len() != 1 {
                    return Err(ExecutorError::SelectIntoRowCount {
                        expected: 1,
                        actual: results.len(),
                    });
                }

                // Get the single row
                let row = &results[0];

                // Validate column count matches variable count
                if row.values.len() != into_vars.len() {
                    return Err(ExecutorError::SelectIntoColumnCount {
                        expected: into_vars.len(),
                        actual: row.values.len(),
                    });
                }

                // Store results in procedural variables
                for (var_name, value) in into_vars.iter().zip(row.values.iter()) {
                    ctx.set_variable(var_name, value.clone());
                }

                Ok(())
            } else {
                // Regular SELECT (discard results)
                let executor = crate::SelectExecutor::new_with_procedural_context(db, ctx);
                let _results = executor.execute(select_stmt)?;
                Ok(())
            }
        }
        Statement::Insert(insert_stmt) => {
            // Execute INSERT with procedural context
            let _count =
                crate::InsertExecutor::execute_with_procedural_context(db, insert_stmt, ctx)?;
            Ok(())
        }
        Statement::Update(update_stmt) => {
            // Execute UPDATE with procedural context
            let _count =
                crate::UpdateExecutor::execute_with_procedural_context(update_stmt, db, ctx)?;
            Ok(())
        }
        Statement::Delete(delete_stmt) => {
            // Execute DELETE with procedural context
            let _count =
                crate::DeleteExecutor::execute_with_procedural_context(delete_stmt, db, ctx)?;
            Ok(())
        }
        _ => {
            // Other SQL statements (DDL, transactions, etc.) are not supported in procedures
            Err(ExecutorError::UnsupportedFeature(format!(
                "SQL statement type not supported in procedure bodies: {:?}",
                stmt
            )))
        }
    }
}

/// Cast a value to a specific data type
fn cast_to_type(
    value: SqlValue,
    target_type: &vibesql_types::DataType,
) -> Result<SqlValue, ExecutorError> {
    use vibesql_types::DataType;

    // If value is already NULL, return NULL regardless of target type
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    match target_type {
        DataType::Integer => match value {
            SqlValue::Integer(i) => Ok(SqlValue::Integer(i)),
            SqlValue::Bigint(b) => Ok(SqlValue::Integer(b)),
            SqlValue::Smallint(s) => Ok(SqlValue::Integer(s as i64)),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                s.parse::<i64>().map(SqlValue::Integer).map_err(|_| {
                    ExecutorError::TypeError(format!("Cannot convert '{}' to INTEGER", s))
                })
            }
            _ => Err(ExecutorError::TypeError(format!("Cannot convert {:?} to INTEGER", value))),
        },

        DataType::Varchar { .. } | DataType::Character { .. } => {
            // Convert to string
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(value.to_string())))
        }

        DataType::Boolean => match value {
            SqlValue::Boolean(b) => Ok(SqlValue::Boolean(b)),
            SqlValue::Integer(i) => Ok(SqlValue::Boolean(i != 0)),
            SqlValue::Varchar(ref s) | SqlValue::Character(ref s) => {
                let s_upper = s.to_uppercase();
                if s_upper == "TRUE" || s_upper == "T" || s_upper == "1" {
                    Ok(SqlValue::Boolean(true))
                } else if s_upper == "FALSE" || s_upper == "F" || s_upper == "0" {
                    Ok(SqlValue::Boolean(false))
                } else {
                    Err(ExecutorError::TypeError(format!("Cannot convert '{}' to BOOLEAN", s)))
                }
            }
            _ => Err(ExecutorError::TypeError(format!("Cannot convert {:?} to BOOLEAN", value))),
        },

        // For other types, accept the value as-is for now
        _ => Ok(value),
    }
}

// TODO: Add comprehensive integration tests for SELECT with procedural variables
// These tests would verify:
// 1. SELECT with procedural variables in WHERE clause
// 2. SELECT with procedural variables in SELECT list
// 3. SELECT with procedural parameters (IN/OUT/INOUT)
// 4. Nested procedures with variable scoping
//
// For now, the implementation can be verified by:
// - Build succeeds (procedural_context field is properly threaded)
// - Existing procedural tests pass (no regressions)
// - Manual testing with procedures containing SELECT statements
