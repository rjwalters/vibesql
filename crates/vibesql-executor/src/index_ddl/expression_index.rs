//! Expression index creation
//!
//! This module handles creation of expression-based indexes (functional indexes).
//! Expression indexes compute index keys by evaluating expressions on each row,
//! rather than using column values directly.

use std::collections::HashSet;

use vibesql_ast::IndexColumn;
use vibesql_catalog::TableSchema;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::{errors::ExecutorError, evaluator::ExpressionEvaluator};

/// Create an expression index by evaluating expressions for each row.
///
/// This function:
/// 1. Scans all rows in the table
/// 2. Evaluates the expression(s) for each row to compute index key values
/// 3. Builds the B-tree index with the computed keys
/// 4. Enforces UNIQUE constraint if specified
pub fn create_expression_index(
    database: &mut Database,
    table_name: &str,
    index_name: &str,
    table_schema: &TableSchema,
    columns: &[IndexColumn],
    unique: bool,
    where_clause: Option<&vibesql_ast::Expression>,
) -> Result<(), ExecutorError> {
    // Get table for scanning
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    // Create expression evaluator for this table's schema.
    // Index context: a date/time function call that resolves the current time
    // ('now', possibly from ROW DATA) or applies 'localtime'/'utc' makes
    // CREATE INDEX fail with "non-deterministic use of <fn>() in an index"
    // (SQLite evaluation-time semantics, date2-310).
    let evaluator = ExpressionEvaluator::new(table_schema)
        .with_schema_context(crate::evaluator::SchemaExprContext::Index);

    // Collect all key-value pairs (key, row_id)
    let mut keys: Vec<(Vec<SqlValue>, usize)> = Vec::new();
    let mut unique_keys: HashSet<Vec<SqlValue>> = HashSet::new();

    // Scan all live rows
    for (row_idx, row) in table.scan_live() {
        // Partial expression index: rows excluded by the WHERE predicate are
        // not in the index, so their index expressions are never evaluated
        // (date2-320: the predicate shields a 'now' row from rejection). A
        // non-deterministic use inside the predicate itself is also rejected.
        if let Some(predicate) = where_clause {
            let in_index = match evaluator.eval(predicate, row) {
                Ok(v) => crate::partial_index_maintenance::is_predicate_truthy(&v),
                Err(e) if e.is_non_deterministic_use() => return Err(e),
                Err(_) => false,
            };
            if !in_index {
                continue;
            }
        }

        // Evaluate each expression/column to build the key
        let mut key_values: Vec<SqlValue> = Vec::new();

        for col in columns {
            let value = if let Some(expr) = col.get_expression() {
                // Expression: evaluate it
                evaluator.eval(expr, row)?
            } else if let Some(col_name) = col.column_name() {
                // Column reference: extract value directly
                let col_idx = table_schema.get_column_index(col_name).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: col_name.to_string(),
                        table_name: table_name.to_string(),
                        searched_tables: vec![table_name.to_string()],
                        available_columns: table_schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    }
                })?;
                row.values[col_idx].clone()
            } else {
                // Should not happen: validated earlier
                return Err(ExecutorError::InvalidIndexDefinition(
                    "Index column must be either a column name or an expression".to_string(),
                ));
            };

            key_values.push(value);
        }

        // UNIQUE constraint validation
        // NULL values are excluded from uniqueness checks (SQL standard)
        if unique {
            let has_null = key_values.iter().any(|v| matches!(v, SqlValue::Null));
            if !has_null {
                if unique_keys.contains(&key_values) {
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "UNIQUE constraint failed: duplicate key in expression index '{}'",
                        index_name
                    )));
                }
                unique_keys.insert(key_values.clone());
            }
        }

        keys.push((key_values, row_idx));
    }

    // Create the index in storage using the pre-computed keys
    database.create_index_with_keys(
        index_name.to_string(),
        table_name.to_string(),
        unique,
        columns.to_vec(),
        keys,
    )?;

    Ok(())
}
