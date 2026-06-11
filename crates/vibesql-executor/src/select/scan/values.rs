//! VALUES clause execution logic
//!
//! Handles execution of VALUES table constructors in FROM clauses
//! by evaluating expressions and creating a derived table.
//!
//! Example: `SELECT * FROM (VALUES(1,'a'), (2,'b')) AS t(x, y)`

use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, schema::CombinedSchema,
};

/// Execute a VALUES table constructor
///
/// Evaluates each expression in each row, validates that all rows have
/// the same number of columns, and creates a derived table with the results.
///
/// # Arguments
///
/// * `rows` - The VALUE rows, each containing expressions for columns
/// * `alias` - The table alias (required)
/// * `column_aliases` - Optional column name overrides
/// * `database` - Optional database reference for expression evaluation (enables
///   function calls, subqueries, etc.)
/// * `cte_results` - Optional CTE context so subqueries inside VALUES rows can
///   reference names bound by an enclosing WITH clause (issue #5353)
pub(crate) fn execute_values(
    rows: &[Vec<vibesql_ast::Expression>],
    alias: &str,
    column_aliases: Option<&Vec<String>>,
    database: Option<&vibesql_storage::Database>,
    cte_results: Option<&std::collections::HashMap<String, crate::select::cte::CteResult>>,
) -> Result<super::FromResult, ExecutorError> {
    // Handle empty VALUES - return empty result with appropriate schema
    if rows.is_empty() {
        let num_columns = column_aliases.map(|ca| ca.len()).unwrap_or(0);
        let column_names: Vec<String> = column_aliases
            .cloned()
            .unwrap_or_else(|| (0..num_columns).map(|i| format!("column{}", i + 1)).collect());
        let column_types = vec![vibesql_types::DataType::Null; num_columns];
        let schema =
            CombinedSchema::from_derived_table(alias.to_string(), column_names, column_types);
        return Ok(super::FromResult::from_rows(schema, vec![]));
    }

    // Determine expected column count from first row
    let expected_columns = rows[0].len();

    // Create an empty schema and row for expression evaluation
    // VALUES expressions should not reference columns, so this is safe
    let empty_schema = CombinedSchema::empty();
    let empty_row = vibesql_storage::Row::new(vec![]);

    // Create evaluator - use database if available for function calls, subqueries, etc.
    let mut evaluator = if let Some(db) = database {
        CombinedExpressionEvaluator::with_database(&empty_schema, db)
    } else {
        CombinedExpressionEvaluator::new(&empty_schema)
    };

    // Thread CTE context so subqueries in VALUES rows can reference names
    // bound by an enclosing WITH clause (issue #5353)
    if let Some(ctes) = cte_results {
        if !ctes.is_empty() {
            evaluator = evaluator.with_cte_context(ctes);
        }
    }

    // Evaluate all rows and collect results
    let mut result_rows = Vec::with_capacity(rows.len());
    for (row_idx, row_exprs) in rows.iter().enumerate() {
        // Validate column count matches
        if row_exprs.len() != expected_columns {
            return Err(ExecutorError::ColumnCountMismatch {
                expected: expected_columns,
                provided: row_exprs.len(),
            });
        }

        // Evaluate each expression in the row
        let mut values = Vec::with_capacity(row_exprs.len());
        for expr in row_exprs {
            let value = evaluator.eval(expr, &empty_row).map_err(|e| {
                ExecutorError::TypeError(format!(
                    "Error evaluating VALUES row {}: {}",
                    row_idx + 1,
                    e
                ))
            })?;
            values.push(value);
        }
        result_rows.push(vibesql_storage::Row::new(values));
    }

    // Derive column names
    let column_names: Vec<String> = if let Some(aliases) = column_aliases {
        // Validate column alias count matches
        if aliases.len() != expected_columns {
            return Err(ExecutorError::ColumnCountMismatch {
                expected: expected_columns,
                provided: aliases.len(),
            });
        }
        aliases.clone()
    } else {
        // Generate default column names: column1, column2, ...
        (0..expected_columns).map(|i| format!("column{}", i + 1)).collect()
    };

    // Infer column types from first row values
    let column_types: Vec<vibesql_types::DataType> = result_rows
        .first()
        .map(|row| row.values.iter().map(|v| v.get_type()).collect())
        .unwrap_or_else(|| vec![vibesql_types::DataType::Null; expected_columns]);

    // Create schema with table alias
    let schema = CombinedSchema::from_derived_table(alias.to_string(), column_names, column_types);

    Ok(super::FromResult::from_rows(schema, result_rows))
}
