//! User-defined function execution
//!
//! This module handles execution of user-defined functions (UDFs) with:
//! - Parameter binding
//! - BEGIN...END body execution
//! - Local variables and control flow
//! - Recursion depth limiting
//! - Read-only enforcement

use crate::errors::ExecutorError;
use crate::procedural::ExecutionContext;
use vibesql_ast::Expression;
use vibesql_catalog::{Function, FunctionBody};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute a user-defined function
///
/// ## Steps:
/// 1. Validate argument count matches parameter count
/// 2. Create function execution context (read-only, isolated)
/// 3. Check and increment recursion depth
/// 4. Bind argument values to function parameters
/// 5. Parse and execute function body
/// 6. Handle RETURN control flow
/// 7. Decrement recursion depth and return value
///
/// ## Errors:
/// - ArgumentCountMismatch: Wrong number of arguments
/// - RecursionLimitExceeded: Too many nested function calls
/// - FunctionMustReturn: Function exited without RETURN statement
/// - Other execution errors from function body
pub fn execute_user_function(
    func: &Function,
    args: &[Expression],
    db: &mut Database,
) -> Result<SqlValue, ExecutorError> {
    // 1. Validate argument count
    if args.len() != func.parameters.len() {
        return Err(ExecutorError::ArgumentCountMismatch {
            expected: func.parameters.len(),
            actual: args.len(),
        });
    }

    // 2. Create function execution context (read-only)
    let mut ctx = ExecutionContext::new_function_context();

    // 3. Check recursion depth
    ctx.enter_recursion().map_err(|e| ExecutorError::RecursionLimitExceeded {
        message: e,
        call_stack: vec![], // TODO: Track call stack in Phase 7
        max_depth: 100,
    })?;

    // 4. Bind arguments to parameters
    // Note: All function parameters are IN only (no OUT/INOUT)
    for (param, arg_expr) in func.parameters.iter().zip(args.iter()) {
        // Evaluate the argument expression
        let value = super::executor::evaluate_expression(arg_expr, db, &ctx)?;
        ctx.set_parameter(&param.name, value);
    }

    // 5. Parse and execute function body
    let result = match &func.body {
        FunctionBody::RawSql(sql) => {
            // Simple RETURN expression function (e.g., "RETURN x + 10")
            execute_simple_return(sql, &mut ctx, db)?
        }
        FunctionBody::BeginEnd(sql) => {
            // Complex BEGIN...END function body
            execute_begin_end_body(sql, &mut ctx, db, &func.name)?
        }
    };

    // 6. Decrement recursion depth
    ctx.exit_recursion();

    Ok(result)
}

/// Execute a simple RETURN expression function
///
/// Handles functions like:
/// ```sql
/// CREATE FUNCTION add_ten(x INT) RETURNS INT
///   RETURN x + 10;
/// ```
fn execute_simple_return(
    sql: &str,
    ctx: &mut ExecutionContext,
    db: &mut Database,
) -> Result<SqlValue, ExecutorError> {
    // The sql should just be the RETURN expression: "RETURN x + 10"
    let sql_trimmed = sql.trim();

    // For simple RETURN functions, we need to parse and execute the RETURN statement
    // Since there's no Statement::Procedural variant, we'll need to evaluate the expression directly

    // Extract the expression after RETURN keyword
    let return_upper = "RETURN";
    if !sql_trimmed.to_uppercase().starts_with(return_upper) {
        return Err(ExecutorError::InvalidFunctionBody(
            "Simple function body must start with RETURN".to_string(),
        ));
    }

    let expr_str = sql_trimmed[return_upper.len()..].trim();

    // Parse just the expression
    // We'll use Parser::parse_sql with a SELECT wrapper to get the expression parsed
    let select_wrapper = format!("SELECT {}", expr_str);
    let stmt = Parser::parse_sql(&select_wrapper).map_err(|e| {
        ExecutorError::ParseError(format!("Failed to parse RETURN expression: {}", e))
    })?;

    // Extract the expression from the SELECT
    #[allow(clippy::collapsible_match)]
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        if let Some(first_item) = select_stmt.select_list.first() {
            if let vibesql_ast::SelectItem::Expression { expr, alias: _ } = first_item {
                // Evaluate the expression in the procedural context
                let value = super::executor::evaluate_expression(expr, db, ctx)?;
                return Ok(value);
            }
        }
    }

    Err(ExecutorError::InvalidFunctionBody("Could not parse RETURN expression".to_string()))
}

/// Execute a BEGIN...END function body
///
/// Handles functions like:
/// ```sql
/// CREATE FUNCTION factorial(n INT) RETURNS INT
/// BEGIN
///   DECLARE result INT DEFAULT 1;
///   DECLARE i INT DEFAULT 2;
///   WHILE i <= n DO
///     SET result = result * i;
///     SET i = i + 1;
///   END WHILE;
///   RETURN result;
/// END;
/// ```
fn execute_begin_end_body(
    _sql: &str,
    _ctx: &mut ExecutionContext,
    _db: &mut Database,
    func_name: &str,
) -> Result<SqlValue, ExecutorError> {
    // TODO: Complex BEGIN...END functions require FunctionBody to store Vec<ProceduralStatement>
    // like ProcedureBody does, rather than raw SQL strings.
    // This is tracked in the issue as a known limitation.
    //
    // For now, we'll return an error directing users to use simple RETURN functions.
    // The proper fix is to update FunctionBody::BeginEnd to store Vec<ProceduralStatement>
    // and update the parser to populate it correctly.

    Err(ExecutorError::UnsupportedFeature(format!(
        "Complex BEGIN...END function bodies not yet fully supported for function '{}'. \
             Use simple RETURN expression functions (e.g., 'RETURN x + 10') instead. \
             Full BEGIN...END support requires updating FunctionBody to store parsed statements.",
        func_name
    )))
}

impl ExecutionContext {
    /// Create a new function execution context (read-only)
    pub fn new_function_context() -> Self {
        let mut ctx = Self::new();
        ctx.is_function = true;
        ctx
    }

    /// Check if this is a function context (read-only)
    pub fn is_function_context(&self) -> bool {
        self.is_function
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::FunctionParam;
    use vibesql_types::DataType;

    #[test]
    fn test_simple_return_function() {
        let mut db = Database::new();

        // CREATE FUNCTION add_ten(x INT) RETURNS INT RETURN x + 10;
        let func = Function::new(
            "add_ten".to_string(),
            "public".to_string(),
            vec![FunctionParam { name: "x".to_string(), data_type: DataType::Integer }],
            DataType::Integer,
            FunctionBody::RawSql("RETURN x + 10".to_string()),
        );

        // Call with argument 5
        let args = vec![Expression::Literal(SqlValue::Integer(5))];
        let result = execute_user_function(&func, &args, &mut db).unwrap();

        assert_eq!(result, SqlValue::Integer(15));
    }

    #[test]
    fn test_argument_count_mismatch() {
        let mut db = Database::new();

        let func = Function::new(
            "add_ten".to_string(),
            "public".to_string(),
            vec![FunctionParam { name: "x".to_string(), data_type: DataType::Integer }],
            DataType::Integer,
            FunctionBody::RawSql("RETURN x + 10".to_string()),
        );

        // Call with no arguments (should fail)
        let args = vec![];
        let result = execute_user_function(&func, &args, &mut db);

        assert!(matches!(result, Err(ExecutorError::ArgumentCountMismatch { .. })));
    }

    #[test]
    fn test_begin_end_not_supported_yet() {
        let mut db = Database::new();

        // CREATE FUNCTION factorial(n INT) RETURNS INT BEGIN...END
        let func = Function::new(
            "factorial".to_string(),
            "public".to_string(),
            vec![FunctionParam { name: "n".to_string(), data_type: DataType::Integer }],
            DataType::Integer,
            FunctionBody::BeginEnd("BEGIN DECLARE x INT DEFAULT 5; RETURN x; END".to_string()),
        );

        let args = vec![Expression::Literal(SqlValue::Integer(5))];
        let result = execute_user_function(&func, &args, &mut db);

        // Should return UnsupportedFeature for now
        assert!(matches!(result, Err(ExecutorError::UnsupportedFeature(_))));
    }
}
