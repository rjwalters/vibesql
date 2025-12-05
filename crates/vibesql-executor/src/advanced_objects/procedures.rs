//! Executor for PROCEDURE objects and CALL statement (SQL:1999 Feature P001)

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE PROCEDURE statement (SQL:1999 Feature P001)
///
/// Creates a stored procedure in the database catalog with optional characteristics.
///
/// # Parameters
///
/// Procedures support three parameter modes:
/// - **IN**: Input-only parameters (read by procedure)
/// - **OUT**: Output-only parameters (written by procedure to session variables)
/// - **INOUT**: Both input and output (read and written)
///
/// # Characteristics (Phase 6)
///
/// - **SQL SECURITY**: DEFINER (default) or INVOKER
/// - **COMMENT**: Documentation string
/// - **LANGUAGE**: SQL (only supported language)
///
/// # Example
///
/// ```sql
/// CREATE PROCEDURE calculate_stats(
///   IN input_value INT,
///   OUT sum_result INT,
///   OUT count_result INT
/// )
///   SQL SECURITY INVOKER
///   COMMENT 'Calculate sum and count from data_table'
/// BEGIN
///   SELECT SUM(value), COUNT(*)
///   INTO sum_result, count_result
///   FROM data_table
///   WHERE value > input_value;
/// END;
/// ```
///
/// # Errors
///
/// Returns error if:
/// - Procedure name already exists
/// - Invalid parameter types
/// - Body parsing fails
pub fn execute_create_procedure(
    stmt: &CreateProcedureStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    use vibesql_catalog::{ParameterMode, Procedure, ProcedureBody, ProcedureParam, SqlSecurity};

    // Convert AST parameters to catalog parameters
    let catalog_params = stmt
        .parameters
        .iter()
        .map(|param| {
            let mode = match param.mode {
                vibesql_ast::ParameterMode::In => ParameterMode::In,
                vibesql_ast::ParameterMode::Out => ParameterMode::Out,
                vibesql_ast::ParameterMode::InOut => ParameterMode::InOut,
            };
            ProcedureParam { mode, name: param.name.clone(), data_type: param.data_type.clone() }
        })
        .collect();

    // Convert AST body to catalog body
    let catalog_body = match &stmt.body {
        vibesql_ast::ProcedureBody::BeginEnd(statements) => {
            // Store the parsed AST for execution
            ProcedureBody::BeginEnd(statements.clone())
        }
        vibesql_ast::ProcedureBody::RawSql(sql) => ProcedureBody::RawSql(sql.clone()),
    };

    // Convert characteristics (Phase 6)
    let sql_security = stmt
        .sql_security
        .as_ref()
        .map(|sec| match sec {
            vibesql_ast::SqlSecurity::Definer => SqlSecurity::Definer,
            vibesql_ast::SqlSecurity::Invoker => SqlSecurity::Invoker,
        })
        .unwrap_or(SqlSecurity::Definer);

    let procedure =
        if stmt.sql_security.is_some() || stmt.comment.is_some() || stmt.language.is_some() {
            Procedure::with_characteristics(
                stmt.procedure_name.clone(),
                db.catalog.get_current_schema().to_string(),
                catalog_params,
                catalog_body,
                sql_security,
                stmt.comment.clone(),
                stmt.language.clone().unwrap_or_else(|| "SQL".to_string()),
            )
        } else {
            Procedure::new(
                stmt.procedure_name.clone(),
                db.catalog.get_current_schema().to_string(),
                catalog_params,
                catalog_body,
            )
        };

    db.catalog.create_procedure_with_characteristics(procedure)?;
    Ok(())
}

/// Execute DROP PROCEDURE statement (SQL:1999 Feature P001)
pub fn execute_drop_procedure(
    stmt: &DropProcedureStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    // Check if procedure exists
    let procedure_exists = db.catalog.procedure_exists(&stmt.procedure_name);

    // If IF EXISTS is specified and procedure doesn't exist, succeed silently
    if stmt.if_exists && !procedure_exists {
        return Ok(());
    }

    db.catalog.drop_procedure(&stmt.procedure_name)?;
    Ok(())
}

/// Extract variable name from an expression for OUT/INOUT parameter binding
///
/// Valid patterns:
/// - ColumnRef without table qualifier (treated as session variable): @var_name
/// - Function call to session variable function (if we add one)
///
/// Returns the variable name (without @ prefix) or error if expression is not a valid variable reference.
fn extract_variable_name(expr: &Expression) -> Result<String, ExecutorError> {
    match expr {
        Expression::ColumnRef { table: None, column } => {
            // Column reference without table - treat as session variable
            // If it starts with @, strip it; otherwise use as-is
            let var_name = if let Some(stripped) = column.strip_prefix('@') {
                stripped.to_string()
            } else {
                column.clone()
            };
            Ok(var_name)
        }
        Expression::ColumnRef { table: Some(_), column: _ } => {
            Err(ExecutorError::Other(
                "OUT/INOUT parameter target must be a session variable (e.g., @var_name), not a table.column reference".to_string()
            ))
        }
        Expression::Literal(_) => {
            Err(ExecutorError::Other(
                "OUT/INOUT parameter target cannot be a literal value".to_string()
            ))
        }
        _ => {
            Err(ExecutorError::Other(
                "OUT/INOUT parameter target must be a session variable (e.g., @var_name), not a complex expression".to_string()
            ))
        }
    }
}

/// Execute CALL statement (SQL:1999 Feature P001)
///
/// Executes a stored procedure with parameter binding and procedural statement execution.
///
/// # Parameter Binding
///
/// - **IN parameters**: Values are evaluated and passed to the procedure
/// - **OUT parameters**: Must be session variables (@var), initialized to NULL,
///   procedure can assign values that are returned to caller
/// - **INOUT parameters**: Must be session variables (@var), values are passed in
///   and can be modified by the procedure
///
/// # Example
///
/// ```sql
/// -- Create procedure with mixed parameter modes
/// CREATE PROCEDURE update_counter(
///   IN increment INT,
///   INOUT counter INT,
///   OUT message VARCHAR(100)
/// )
/// BEGIN
///   SET counter = counter + increment;
///   SET message = CONCAT('Counter updated to ', counter);
/// END;
///
/// -- Call procedure
/// SET @count = 10;
/// CALL update_counter(5, @count, @msg);
/// SELECT @count, @msg;
/// -- Output: count=15, msg='Counter updated to 15'
/// ```
///
/// # Control Flow
///
/// The procedure body executes sequentially until:
/// - All statements complete (normal exit)
/// - RETURN statement is encountered (early exit)
/// - Error occurs (execution stops)
///
/// After execution, OUT and INOUT parameter values are written back to their
/// corresponding session variables.
///
/// # Errors
///
/// Returns error if:
/// - Procedure not found
/// - Wrong number of arguments
/// - OUT/INOUT parameter is not a session variable
/// - Type mismatch in parameter binding
/// - Execution error in procedure body
pub fn execute_call(stmt: &CallStmt, db: &mut Database) -> Result<(), ExecutorError> {
    use crate::procedural::{execute_procedural_statement, ControlFlow, ExecutionContext};

    // 1. Look up the procedure definition and clone what we need
    let (parameters, body) = {
        let procedure = db.catalog.get_procedure(&stmt.procedure_name);

        match procedure {
            Some(proc) => (proc.parameters.clone(), proc.body.clone()),
            None => {
                // Procedure not found - provide helpful error with suggestions
                let schema_name = db.catalog.get_current_schema().to_string();
                let available_procedures = db.catalog.list_procedures();

                return Err(ExecutorError::ProcedureNotFound {
                    procedure_name: stmt.procedure_name.clone(),
                    schema_name,
                    available_procedures,
                });
            }
        }
    };

    // 2. Validate parameter count
    if stmt.arguments.len() != parameters.len() {
        // Build parameter signature string
        let param_sig = parameters
            .iter()
            .map(|p| {
                let mode = match p.mode {
                    vibesql_catalog::ParameterMode::In => "IN",
                    vibesql_catalog::ParameterMode::Out => "OUT",
                    vibesql_catalog::ParameterMode::InOut => "INOUT",
                };
                format!("{} {} {:?}", mode, p.name, p.data_type)
            })
            .collect::<Vec<_>>()
            .join(", ");

        return Err(ExecutorError::ParameterCountMismatch {
            routine_name: stmt.procedure_name.clone(),
            routine_type: "Procedure".to_string(),
            expected: parameters.len(),
            actual: stmt.arguments.len(),
            parameter_signature: param_sig,
        });
    }

    // 3. Create execution context and bind parameters
    let mut ctx = ExecutionContext::new();

    for (param, arg_expr) in parameters.iter().zip(&stmt.arguments) {
        match param.mode {
            vibesql_catalog::ParameterMode::In => {
                // IN parameter: Evaluate and bind the value
                let value = crate::procedural::executor::evaluate_expression(arg_expr, db, &ctx)?;
                ctx.set_parameter(&param.name, value);
            }
            vibesql_catalog::ParameterMode::Out => {
                // OUT parameter: Initialize to NULL
                ctx.set_parameter(&param.name, vibesql_types::SqlValue::Null);

                // Extract and register target variable name
                let var_name = extract_variable_name(arg_expr)?;
                ctx.register_out_parameter(&param.name, var_name);
            }
            vibesql_catalog::ParameterMode::InOut => {
                // INOUT parameter: Evaluate and bind input value
                let value = crate::procedural::executor::evaluate_expression(arg_expr, db, &ctx)?;
                ctx.set_parameter(&param.name, value);

                // Extract and register target variable name
                let var_name = extract_variable_name(arg_expr)?;
                ctx.register_out_parameter(&param.name, var_name);
            }
        }
    }

    // 4. Execute procedure body
    match &body {
        vibesql_catalog::ProcedureBody::BeginEnd(statements) => {
            // Execute each procedural statement sequentially
            for stmt in statements {
                match execute_procedural_statement(stmt, &mut ctx, db)? {
                    ControlFlow::Continue => {
                        // Continue to next statement
                    }
                    ControlFlow::Return(_) => {
                        // RETURN in procedures exits early (functions will handle return value later)
                        break;
                    }
                    ControlFlow::Leave(_) | ControlFlow::Iterate(_) => {
                        // Control flow statements not yet supported in Phase 2
                        return Err(ExecutorError::UnsupportedFeature(
                            "Control flow (LEAVE/ITERATE) not yet supported in Phase 2".to_string(),
                        ));
                    }
                }
            }
            Ok(())
        }
        vibesql_catalog::ProcedureBody::RawSql(_) => {
            // RawSql fallback - would require parsing
            Err(ExecutorError::UnsupportedFeature(
                "RawSql procedure bodies require parsing (use BEGIN/END block instead)".to_string(),
            ))
        }
    }?;

    // 5. Return output parameter values to session variables
    for (param_name, target_var_name) in ctx.get_out_parameters() {
        // Get the parameter value from the context
        let value = ctx
            .get_parameter(param_name)
            .cloned()
            .ok_or_else(|| ExecutorError::Other(format!("Parameter '{}' not found", param_name)))?;

        // Store in session variable
        db.set_session_variable(target_var_name, value);
    }

    Ok(())
}
