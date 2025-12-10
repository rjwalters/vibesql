//! VALUES clause execution logic
//!
//! Handles execution of VALUES table constructors in FROM clauses
//! by evaluating expressions and creating a derived table.
//!
//! Example: `SELECT * FROM (VALUES(1,'a'), (2,'b')) AS t(x, y)`

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Evaluate a simple expression to SqlValue
/// Supports literals and basic expressions. For VALUES clauses,
/// expressions are typically constants.
fn evaluate_values_expression(
    expr: &vibesql_ast::Expression,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        vibesql_ast::Expression::Literal(val) => Ok(val.clone()),
        vibesql_ast::Expression::UnaryOp { op, expr } => {
            let inner_val = evaluate_values_expression(expr)?;
            match op {
                vibesql_ast::UnaryOperator::Minus => match inner_val {
                    vibesql_types::SqlValue::Integer(n) => {
                        Ok(vibesql_types::SqlValue::Integer(-n))
                    }
                    vibesql_types::SqlValue::Bigint(n) => Ok(vibesql_types::SqlValue::Bigint(-n)),
                    vibesql_types::SqlValue::Smallint(n) => {
                        Ok(vibesql_types::SqlValue::Smallint(-n))
                    }
                    vibesql_types::SqlValue::Numeric(n) => {
                        Ok(vibesql_types::SqlValue::Numeric(-n))
                    }
                    vibesql_types::SqlValue::Double(n) => Ok(vibesql_types::SqlValue::Double(-n)),
                    vibesql_types::SqlValue::Float(n) => Ok(vibesql_types::SqlValue::Float(-n)),
                    vibesql_types::SqlValue::Real(n) => Ok(vibesql_types::SqlValue::Real(-n)),
                    vibesql_types::SqlValue::Null => Ok(vibesql_types::SqlValue::Null),
                    _ => Err(ExecutorError::TypeError(format!(
                        "Cannot negate value of type {:?}",
                        inner_val.get_type()
                    ))),
                },
                vibesql_ast::UnaryOperator::Plus => Ok(inner_val),
                vibesql_ast::UnaryOperator::Not => match inner_val {
                    vibesql_types::SqlValue::Boolean(b) => {
                        Ok(vibesql_types::SqlValue::Boolean(!b))
                    }
                    vibesql_types::SqlValue::Null => Ok(vibesql_types::SqlValue::Null),
                    _ => Err(ExecutorError::TypeError(format!(
                        "Cannot apply NOT to value of type {:?}",
                        inner_val.get_type()
                    ))),
                },
                vibesql_ast::UnaryOperator::IsNull => {
                    Ok(vibesql_types::SqlValue::Boolean(inner_val.is_null()))
                }
                vibesql_ast::UnaryOperator::IsNotNull => {
                    Ok(vibesql_types::SqlValue::Boolean(!inner_val.is_null()))
                }
            }
        }
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Expression type {:?} not supported in VALUES clause. Only literals are currently supported.",
            std::any::type_name_of_val(expr)
        ))),
    }
}

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
pub(crate) fn execute_values(
    rows: &[Vec<vibesql_ast::Expression>],
    alias: &str,
    column_aliases: Option<&Vec<String>>,
) -> Result<super::FromResult, ExecutorError> {
    // Handle empty VALUES - return empty result with appropriate schema
    if rows.is_empty() {
        let num_columns = column_aliases.map(|ca| ca.len()).unwrap_or(0);
        let column_names: Vec<String> = column_aliases.cloned().unwrap_or_else(|| {
            (0..num_columns).map(|i| format!("column{}", i + 1)).collect()
        });
        let column_types = vec![vibesql_types::DataType::Null; num_columns];
        let schema = CombinedSchema::from_derived_table(alias.to_string(), column_names, column_types);
        return Ok(super::FromResult::from_rows(schema, vec![]));
    }

    // Determine expected column count from first row
    let expected_columns = rows[0].len();

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
            let value = evaluate_values_expression(expr).map_err(|e| {
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
