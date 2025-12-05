//! Executor for FUNCTION objects (SQL:1999 Feature P001)

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE FUNCTION statement (SQL:1999 Feature P001)
///
/// Creates a user-defined function in the database catalog with optional characteristics.
///
/// Functions differ from procedures:
/// - Functions RETURN a value and can be used in expressions
/// - Functions are read-only (cannot modify database tables)
/// - Functions only support IN parameters (no OUT/INOUT)
/// - Functions have recursion depth limiting (max 100 levels)
///
/// # Characteristics (Phase 6)
///
/// - **DETERMINISTIC**: Indicates same input always produces same output
/// - **SQL SECURITY**: DEFINER (default) or INVOKER
/// - **COMMENT**: Documentation string
/// - **LANGUAGE**: SQL (only supported language)
///
/// # Examples
///
/// Simple function with RETURN expression:
/// ```sql
/// CREATE FUNCTION add_ten(x INT) RETURNS INT
///   DETERMINISTIC
///   RETURN x + 10;
/// ```
///
/// Complex function with BEGIN...END body:
/// ```sql
/// CREATE FUNCTION factorial(n INT) RETURNS INT
///   DETERMINISTIC
///   COMMENT 'Calculate factorial of n'
/// BEGIN
///   DECLARE result INT DEFAULT 1;
///   DECLARE i INT DEFAULT 2;
///
///   WHILE i <= n DO
///     SET result = result * i;
///     SET i = i + 1;
///   END WHILE;
///
///   RETURN result;
/// END;
/// ```
///
/// # Errors
///
/// Returns error if:
/// - Function name already exists
/// - Invalid parameter types or return type
/// - Body parsing fails
pub fn execute_create_function(
    stmt: &CreateFunctionStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    use vibesql_catalog::{Function, FunctionBody, FunctionParam, SqlSecurity};

    // Convert AST parameters to catalog parameters
    let catalog_params = stmt
        .parameters
        .iter()
        .map(|param| FunctionParam { name: param.name.clone(), data_type: param.data_type.clone() })
        .collect();

    // Convert AST body to catalog body
    let catalog_body = match &stmt.body {
        vibesql_ast::ProcedureBody::BeginEnd(_) => {
            // For now, store as RawSql. Full execution support comes later.
            FunctionBody::RawSql(format!("{:?}", stmt.body))
        }
        vibesql_ast::ProcedureBody::RawSql(sql) => FunctionBody::RawSql(sql.clone()),
    };

    // Convert characteristics (Phase 6)
    let deterministic = stmt.deterministic.unwrap_or(false);
    let sql_security = stmt
        .sql_security
        .as_ref()
        .map(|sec| match sec {
            vibesql_ast::SqlSecurity::Definer => SqlSecurity::Definer,
            vibesql_ast::SqlSecurity::Invoker => SqlSecurity::Invoker,
        })
        .unwrap_or(SqlSecurity::Definer);

    let function = if stmt.deterministic.is_some()
        || stmt.sql_security.is_some()
        || stmt.comment.is_some()
        || stmt.language.is_some()
    {
        Function::with_characteristics(
            stmt.function_name.clone(),
            db.catalog.get_current_schema().to_string(),
            catalog_params,
            stmt.return_type.clone(),
            catalog_body,
            deterministic,
            sql_security,
            stmt.comment.clone(),
            stmt.language.clone().unwrap_or_else(|| "SQL".to_string()),
        )
    } else {
        Function::new(
            stmt.function_name.clone(),
            db.catalog.get_current_schema().to_string(),
            catalog_params,
            stmt.return_type.clone(),
            catalog_body,
        )
    };

    db.catalog.create_function_with_characteristics(function)?;
    Ok(())
}

/// Execute DROP FUNCTION statement (SQL:1999 Feature P001)
pub fn execute_drop_function(
    stmt: &DropFunctionStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    // Check if function exists
    let function_exists = db.catalog.function_exists(&stmt.function_name);

    // If IF EXISTS is specified and function doesn't exist, succeed silently
    if stmt.if_exists && !function_exists {
        return Ok(());
    }

    db.catalog.drop_function(&stmt.function_name)?;
    Ok(())
}
