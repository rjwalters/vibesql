//! Validation logic for CREATE INDEX statements
//!
//! This module handles pre-creation validation including:
//! - Table and schema existence checks
//! - Column validation for indexed columns
//! - Expression validation (determinism checks, column references)
//! - Prefix length validation
//! - Index name collision checks

use vibesql_ast::{CreateIndexStmt, Expression, IndexColumn};
use vibesql_catalog::TableSchema;
use vibesql_storage::Database;

use crate::{
    errors::ExecutorError, privilege_checker::PrivilegeChecker,
    sqlite_schema::is_sqlite_schema_table,
};

/// Result of validating a CREATE INDEX statement
pub struct ValidationResult {
    /// The table name (without schema prefix)
    pub table_name: String,
    /// Fully qualified table name (schema.table)
    pub qualified_table_name: String,
    /// Cloned table schema for use in index creation
    pub table_schema: TableSchema,
}

/// Validate a CREATE INDEX statement before execution.
///
/// Performs all pre-creation checks:
/// - Parses qualified table name
/// - Checks for sqlite_schema table
/// - Verifies CREATE privilege
/// - Verifies table exists
/// - Validates indexed columns exist
/// - Validates expression determinism
/// - Validates prefix length specifications
/// - Checks for index name collisions
pub fn validate_create_index(
    stmt: &CreateIndexStmt,
    database: &Database,
) -> Result<ValidationResult, ExecutorError> {
    // Parse qualified table name (schema.table or just table)
    let (schema_name, table_name) =
        if let Some((schema_part, table_part)) = stmt.table_name.split_once('.') {
            (schema_part.to_string(), table_part.to_string())
        } else {
            (database.catalog.get_current_schema().to_string(), stmt.table_name.clone())
        };

    // Check if target is sqlite_master/sqlite_schema (read-only system table)
    if is_sqlite_schema_table(&table_name) {
        return Err(ExecutorError::SqliteSystemTableReadOnly {
            table_name: table_name.clone(),
            operation: "indexed".to_string(),
        });
    }

    // Check CREATE privilege on the schema
    PrivilegeChecker::check_create(database, &schema_name)?;

    // Build fully qualified table name for catalog lookups
    let qualified_table_name = format!("{}.{}", schema_name, table_name);

    // Check if table exists
    if !database.catalog.table_exists(&qualified_table_name) {
        return Err(ExecutorError::TableNotFound(qualified_table_name.clone()));
    }

    // Get table schema to validate columns (clone to avoid borrow issues)
    let table_schema = database
        .catalog
        .get_table(&qualified_table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(qualified_table_name.clone()))?
        .clone();

    // Validate no window functions in index expressions or WHERE clause (#4985)
    // Window functions cannot be used in CREATE INDEX statements
    validate_no_window_functions_in_index(stmt)?;

    // Validate expression determinism
    validate_expression_determinism(&stmt.columns)?;

    // Validate that all indexed columns exist in the table
    validate_indexed_columns(&stmt.columns, &table_schema, &qualified_table_name)?;

    // Validate prefix length specifications
    validate_prefix_lengths(&stmt.columns, &table_schema)?;

    // Check if index already exists
    check_index_exists(stmt, database)?;

    // Check SQLite namespace (tables and indexes share namespace)
    check_namespace_collision(&stmt.index_name, database)?;

    Ok(ValidationResult { table_name, qualified_table_name, table_schema })
}

/// Validate that no window functions are used in index expressions or WHERE clause.
///
/// Window functions are not allowed in CREATE INDEX statements because indexes
/// need deterministic expressions that can be computed from row data alone.
fn validate_no_window_functions_in_index(stmt: &CreateIndexStmt) -> Result<(), ExecutorError> {
    // Check index column expressions for window functions
    for index_col in &stmt.columns {
        if let Some(expr) = index_col.get_expression() {
            if let Some(window_name) = crate::select::find_window_function_in_expression(expr) {
                return Err(ExecutorError::MisuseOfWindowFunction { function_name: window_name });
            }
        }
    }

    // Note: Partial indexes (WHERE clause in CREATE INDEX) are not yet supported in the parser,
    // so we don't need to validate window functions in the WHERE clause.
    // When partial index support is added, validation should be added here.

    Ok(())
}

/// Validate that expression indexes use deterministic expressions.
fn validate_expression_determinism(columns: &[IndexColumn]) -> Result<(), ExecutorError> {
    for index_col in columns {
        if let Some(expr) = index_col.get_expression() {
            if !crate::evaluator::expression_hash::ExpressionHasher::is_deterministic_for_index(
                expr,
            ) {
                return Err(ExecutorError::UnsupportedFeature(
                    "Expression indexes must use deterministic expressions. \
                     Non-deterministic functions like RANDOM(), CURRENT_TIMESTAMP, etc. \
                     are not allowed."
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// Validate that all indexed columns exist in the table schema.
fn validate_indexed_columns(
    columns: &[IndexColumn],
    table_schema: &TableSchema,
    qualified_table_name: &str,
) -> Result<(), ExecutorError> {
    for index_col in columns {
        // Validate column-based index columns
        if let Some(col_name) = index_col.column_name() {
            if table_schema.get_column(col_name).is_none() {
                let available_columns =
                    table_schema.columns.iter().map(|c| c.name.clone()).collect();
                return Err(ExecutorError::ColumnNotFound {
                    column_name: col_name.to_string(),
                    table_name: qualified_table_name.to_string(),
                    searched_tables: vec![qualified_table_name.to_string()],
                    available_columns,
                });
            }
        }

        // Validate column references in expressions
        if let Some(expr) = index_col.get_expression() {
            validate_expression_columns(expr, table_schema, qualified_table_name)?;
        }
    }
    Ok(())
}

/// Validate prefix length specifications for indexed columns.
fn validate_prefix_lengths(
    columns: &[IndexColumn],
    table_schema: &TableSchema,
) -> Result<(), ExecutorError> {
    for index_col in columns {
        if let Some(prefix_len) = index_col.prefix_length() {
            // Expression indexes don't support prefix lengths
            if index_col.is_expression() {
                return Err(ExecutorError::InvalidIndexDefinition(
                    "Prefix length cannot be specified for expression indexes".to_string(),
                ));
            }

            let col_name = index_col.column_name().unwrap(); // Safe: not an expression

            // Prefix length must be positive
            if prefix_len == 0 {
                return Err(ExecutorError::InvalidIndexDefinition(format!(
                    "Prefix length must be greater than 0 for column '{}'",
                    col_name
                )));
            }

            // Prefix length should only be used with string columns
            let column = table_schema.get_column(col_name).unwrap(); // Safe: validated above
            match column.data_type {
                vibesql_types::DataType::Varchar { .. }
                | vibesql_types::DataType::Character { .. } => {
                    // Valid string types for prefix indexing
                }
                _ => {
                    return Err(ExecutorError::InvalidIndexDefinition(format!(
                        "Prefix length can only be specified for string columns, but column '{}' has type {:?}",
                        col_name, column.data_type
                    )));
                }
            }

            // Reasonable upper limit check (64KB = 65536 characters)
            const MAX_PREFIX_LENGTH: u64 = 65536;
            if prefix_len > MAX_PREFIX_LENGTH {
                return Err(ExecutorError::InvalidIndexDefinition(format!(
                    "Prefix length {} is too large for column '{}' (maximum: {})",
                    prefix_len, col_name, MAX_PREFIX_LENGTH
                )));
            }
        }
    }
    Ok(())
}

/// Check if an index with the same name already exists.
fn check_index_exists(stmt: &CreateIndexStmt, database: &Database) -> Result<(), ExecutorError> {
    let index_name = &stmt.index_name;
    let index_exists =
        database.index_exists(index_name) || database.spatial_index_exists(index_name);

    if index_exists {
        if stmt.if_not_exists {
            // IF NOT EXISTS is handled by returning early with success message
            // The caller should check this case
            return Ok(());
        } else {
            return Err(ExecutorError::IndexAlreadyExists(index_name.clone()));
        }
    }
    Ok(())
}

/// Check that the index name doesn't collide with existing table names.
fn check_namespace_collision(index_name: &str, database: &Database) -> Result<(), ExecutorError> {
    let normalized_index_name = index_name.to_lowercase();
    for schema in database.catalog.list_schemas() {
        let qualified_name = format!("{}.{}", schema, normalized_index_name);
        if database.catalog.table_exists(&qualified_name) {
            // Use SQLite-compatible error message (exact format required for TCL tests)
            return Err(ExecutorError::SqliteCompatError(format!(
                "there is already a table named {}",
                index_name
            )));
        }
    }
    Ok(())
}

/// Check if an index already exists (for IF NOT EXISTS handling).
pub fn index_already_exists(stmt: &CreateIndexStmt, database: &Database) -> bool {
    let index_name = &stmt.index_name;
    database.index_exists(index_name) || database.spatial_index_exists(index_name)
}

/// Validate that all column references in an expression exist in the table schema.
pub fn validate_expression_columns(
    expr: &Expression,
    table_schema: &TableSchema,
    qualified_table_name: &str,
) -> Result<(), ExecutorError> {
    match expr {
        Expression::ColumnRef(col_id) => {
            let col_name = col_id.column_canonical();
            if table_schema.get_column(col_name).is_none() {
                let available_columns =
                    table_schema.columns.iter().map(|c| c.name.clone()).collect();
                return Err(ExecutorError::ColumnNotFound {
                    column_name: col_name.to_string(),
                    table_name: qualified_table_name.to_string(),
                    searched_tables: vec![qualified_table_name.to_string()],
                    available_columns,
                });
            }
            Ok(())
        }
        Expression::BinaryOp { left, right, .. } => {
            validate_expression_columns(left, table_schema, qualified_table_name)?;
            validate_expression_columns(right, table_schema, qualified_table_name)
        }
        Expression::UnaryOp { expr, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)
        }
        Expression::Function { args, .. } => {
            for arg in args {
                validate_expression_columns(arg, table_schema, qualified_table_name)?;
            }
            Ok(())
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_expression_columns(op, table_schema, qualified_table_name)?;
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    validate_expression_columns(cond, table_schema, qualified_table_name)?;
                }
                validate_expression_columns(&clause.result, table_schema, qualified_table_name)?;
            }
            if let Some(else_expr) = else_result {
                validate_expression_columns(else_expr, table_schema, qualified_table_name)?;
            }
            Ok(())
        }
        Expression::Cast { expr, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)
        }
        Expression::Collate { expr, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)
        }
        Expression::IsNull { expr, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)
        }
        Expression::Between { expr, low, high, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)?;
            validate_expression_columns(low, table_schema, qualified_table_name)?;
            validate_expression_columns(high, table_schema, qualified_table_name)
        }
        Expression::InList { expr, values, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)?;
            for item in values {
                validate_expression_columns(item, table_schema, qualified_table_name)?;
            }
            Ok(())
        }
        Expression::Like { expr, pattern, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)?;
            validate_expression_columns(pattern, table_schema, qualified_table_name)
        }
        // Literals and other self-contained expressions don't reference columns
        Expression::Literal(_)
        | Expression::Wildcard
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_) => Ok(()),
        // For other expression types, conservatively allow them
        // (they may be validated during evaluation)
        _ => Ok(()),
    }
}
