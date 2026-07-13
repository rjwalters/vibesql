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
/// * `outer_row` / `outer_schema` - Optional outer-query context so a VALUES row
///   used as a correlated row-value LHS (`(VALUES(b3.a, b3.b)) IN (...)`) can
///   resolve outer column references. Without this the VALUES expressions are
///   evaluated against an empty schema and `b3.a` raises "no such column"
///   (rowvalue §18.2/§18.5, issue #6089).
pub(crate) fn execute_values(
    rows: &[Vec<vibesql_ast::Expression>],
    alias: &str,
    column_aliases: Option<&Vec<String>>,
    database: Option<&vibesql_storage::Database>,
    cte_results: Option<&std::collections::HashMap<String, crate::select::cte::CteResult>>,
    outer_row: Option<&vibesql_storage::Row>,
    outer_schema: Option<&CombinedSchema>,
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

    // Create an empty schema and row as the *primary* evaluation context. A
    // bare VALUES row references no local columns; outer references (issue
    // #6089) resolve through the evaluator's outer_schema/outer_row chain.
    let empty_schema = CombinedSchema::empty();
    let empty_row = vibesql_storage::Row::new(vec![]);

    // Non-empty CTE context so subqueries in VALUES rows can reference names
    // bound by an enclosing WITH clause (issue #5353).
    let cte_ctx = cte_results.filter(|ctes| !ctes.is_empty());

    // Create evaluator - use database if available for function calls,
    // subqueries, etc. When outer context is supplied (a correlated VALUES
    // row-value LHS, issue #6089), build with it so `b3.a` resolves against
    // the enclosing query row rather than raising "no such column".
    let evaluator = match (database, outer_row, outer_schema) {
        (Some(db), Some(orow), Some(oschema)) => match cte_ctx {
            Some(ctes) => CombinedExpressionEvaluator::with_database_and_outer_context_and_cte(
                &empty_schema,
                db,
                orow,
                oschema,
                ctes,
            ),
            None => CombinedExpressionEvaluator::with_database_and_outer_context(
                &empty_schema,
                db,
                orow,
                oschema,
            ),
        },
        (Some(db), _, _) => {
            let mut ev = CombinedExpressionEvaluator::with_database(&empty_schema, db);
            if let Some(ctes) = cte_ctx {
                ev = ev.with_cte_context(ctes);
            }
            ev
        }
        (None, _, _) => {
            let mut ev = CombinedExpressionEvaluator::new(&empty_schema);
            if let Some(ctes) = cte_ctx {
                ev = ev.with_cte_context(ctes);
            }
            ev
        }
    };

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
