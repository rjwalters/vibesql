use std::collections::HashMap;

use crate::select::window::WindowFunctionKey;

/// Project columns from a row based on SELECT list (combined schema version)
pub(crate) fn project_row_combined(
    row: &vibesql_storage::Row,
    columns: &[vibesql_ast::SelectItem],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
    schema: &crate::schema::CombinedSchema,
    window_mapping: &Option<HashMap<WindowFunctionKey, usize>>,
    buffer_pool: &vibesql_storage::QueryBufferPool,
) -> Result<vibesql_storage::Row, crate::errors::ExecutorError> {
    // Use pooled buffer to reduce allocation overhead
    let mut values = buffer_pool.get_value_buffer(columns.len());

    for item in columns {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // SELECT * - include all columns
                // When window functions are present, only include base columns (not appended window
                // values)
                if let Some(mapping) = window_mapping {
                    if !mapping.is_empty() {
                        // Find the minimum window column index to know where base columns end
                        let min_window_col =
                            mapping.values().min().copied().unwrap_or(row.values.len());
                        values.extend(row.values[..min_window_col].iter().cloned());
                    } else {
                        values.extend(row.values.iter().cloned());
                    }
                } else {
                    values.extend(row.values.iter().cloned());
                }
            }
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                // SELECT table.* or SELECT alias.* - include columns from specific table/alias
                // TableKey lookup is case-insensitive
                let result = schema.get_table(qualifier).cloned();

                if let Some((start_index, table_schema)) = result {
                    let num_columns = table_schema.columns.len();
                    let end_index = start_index + num_columns;

                    // When window functions are present, only include base columns
                    let effective_end = if let Some(mapping) = window_mapping {
                        if !mapping.is_empty() {
                            // Find the minimum window column index to know where base columns end
                            let min_window_col =
                                mapping.values().min().copied().unwrap_or(row.values.len());
                            end_index.min(min_window_col)
                        } else {
                            end_index
                        }
                    } else {
                        end_index
                    };

                    // Extract the columns for this table
                    if start_index < effective_end && effective_end <= row.values.len() {
                        values.extend(row.values[start_index..effective_end].iter().cloned());
                    }
                    // If indices are out of bounds, this might be an error, but we'll be silent for
                    // now
                }
                // If table not found, skip silently (this should be caught during column name
                // derivation)
            }
            vibesql_ast::SelectItem::Expression { expr, alias: _ } => {
                // Check if this is a window function expression
                let value = if let Some(mapping) = window_mapping {
                    evaluate_expression_with_windows(expr, row, evaluator, mapping)?
                } else {
                    evaluator.eval(expr, row)?
                };
                values.push(value);
            }
        }
    }

    // Move data to result and return pooled buffer
    // This allows buffer capacity reuse while avoiding clone overhead
    let result_values = std::mem::take(&mut values);
    buffer_pool.return_value_buffer(values);
    Ok(vibesql_storage::Row::new(result_values))
}

/// Evaluate an expression, checking for window functions first
pub(super) fn evaluate_expression_with_windows(
    expr: &vibesql_ast::Expression,
    row: &vibesql_storage::Row,
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
    window_mapping: &HashMap<WindowFunctionKey, usize>,
) -> Result<vibesql_types::SqlValue, crate::errors::ExecutorError> {
    use vibesql_ast::Expression;

    match expr {
        Expression::WindowFunction { function, over } => {
            // Look up the pre-computed value for this window function
            let key = WindowFunctionKey::from_expression(function, over);
            if let Some(&col_idx) = window_mapping.get(&key) {
                // Extract the pre-computed value from the appended column
                let value = row.values.get(col_idx).cloned().ok_or({
                    crate::errors::ExecutorError::ColumnIndexOutOfBounds { index: col_idx }
                })?;
                Ok(value)
            } else {
                Err(crate::errors::ExecutorError::UnsupportedExpression(format!(
                    "Window function not found in mapping: {:?}",
                    expr
                )))
            }
        }
        Expression::BinaryOp { left, right, op } => {
            // For expressions containing window functions in binary operations,
            // we need to recursively substitute window function values
            let left_substituted = substitute_window_functions(left, row, window_mapping)?;
            let right_substituted = substitute_window_functions(right, row, window_mapping)?;

            // Build a new binary expression with substituted values and evaluate it
            let new_expr = Expression::BinaryOp {
                left: Box::new(left_substituted),
                right: Box::new(right_substituted),
                op: *op,
            };
            evaluator.eval(&new_expr, row)
        }
        Expression::UnaryOp { expr: inner, op } => {
            // Similar substitution for unary operations
            let inner_substituted = substitute_window_functions(inner, row, window_mapping)?;
            let new_expr = Expression::UnaryOp { expr: Box::new(inner_substituted), op: *op };
            evaluator.eval(&new_expr, row)
        }
        Expression::Case { .. } => {
            // Substitute window functions in CASE expressions before evaluating
            let substituted = substitute_window_functions(expr, row, window_mapping)?;
            evaluator.eval(&substituted, row)
        }
        Expression::Function { .. } => {
            // Substitute window functions in function arguments before evaluating
            let substituted = substitute_window_functions(expr, row, window_mapping)?;
            evaluator.eval(&substituted, row)
        }
        Expression::IsNull { expr: inner, negated } => {
            // Substitute window functions in IS NULL expressions
            let inner_substituted = substitute_window_functions(inner, row, window_mapping)?;
            let new_expr =
                Expression::IsNull { expr: Box::new(inner_substituted), negated: *negated };
            evaluator.eval(&new_expr, row)
        }
        _ => {
            // For non-window expressions, use the regular evaluator
            evaluator.eval(expr, row)
        }
    }
}

/// Substitute window function expressions with literal values from pre-computed results
fn substitute_window_functions(
    expr: &vibesql_ast::Expression,
    row: &vibesql_storage::Row,
    window_mapping: &HashMap<WindowFunctionKey, usize>,
) -> Result<vibesql_ast::Expression, crate::errors::ExecutorError> {
    use vibesql_ast::Expression;

    match expr {
        Expression::WindowFunction { function, over } => {
            // Look up the pre-computed value and convert to a literal expression
            let key = WindowFunctionKey::from_expression(function, over);
            if let Some(&col_idx) = window_mapping.get(&key) {
                let value = row.values.get(col_idx).cloned().ok_or({
                    crate::errors::ExecutorError::ColumnIndexOutOfBounds { index: col_idx }
                })?;
                Ok(Expression::Literal(value))
            } else {
                Err(crate::errors::ExecutorError::UnsupportedExpression(format!(
                    "Window function not found in mapping: {:?}",
                    expr
                )))
            }
        }
        Expression::BinaryOp { left, right, op } => {
            let left_sub = substitute_window_functions(left, row, window_mapping)?;
            let right_sub = substitute_window_functions(right, row, window_mapping)?;
            Ok(Expression::BinaryOp {
                left: Box::new(left_sub),
                right: Box::new(right_sub),
                op: *op,
            })
        }
        Expression::UnaryOp { expr: inner, op } => {
            let inner_sub = substitute_window_functions(inner, row, window_mapping)?;
            Ok(Expression::UnaryOp { expr: Box::new(inner_sub), op: *op })
        }
        Expression::Function { name, args, character_unit } => {
            let substituted_args: Result<Vec<_>, _> = args
                .iter()
                .map(|arg| substitute_window_functions(arg, row, window_mapping))
                .collect();
            Ok(Expression::Function {
                name: name.clone(),
                args: substituted_args?,
                character_unit: character_unit.clone(),
            })
        }
        Expression::Case { operand, when_clauses, else_result } => {
            let subst_operand = operand
                .as_ref()
                .map(|op| substitute_window_functions(op, row, window_mapping))
                .transpose()?
                .map(Box::new);

            let subst_when: Result<Vec<vibesql_ast::CaseWhen>, crate::ExecutorError> = when_clauses
                .iter()
                .map(|when_clause| {
                    let subst_conditions: Result<
                        Vec<vibesql_ast::Expression>,
                        crate::ExecutorError,
                    > = when_clause
                        .conditions
                        .iter()
                        .map(|cond| substitute_window_functions(cond, row, window_mapping))
                        .collect();

                    Ok(vibesql_ast::CaseWhen {
                        conditions: subst_conditions?,
                        result: substitute_window_functions(
                            &when_clause.result,
                            row,
                            window_mapping,
                        )?,
                    })
                })
                .collect();

            let subst_else = else_result
                .as_ref()
                .map(|e| substitute_window_functions(e, row, window_mapping))
                .transpose()?
                .map(Box::new);

            Ok(Expression::Case {
                operand: subst_operand,
                when_clauses: subst_when?,
                else_result: subst_else,
            })
        }
        Expression::IsNull { expr: inner, negated } => {
            let inner_sub = substitute_window_functions(inner, row, window_mapping)?;
            Ok(Expression::IsNull { expr: Box::new(inner_sub), negated: *negated })
        }
        // For all other expressions (literals, column refs, etc.), no substitution needed
        _ => Ok(expr.clone()),
    }
}

/// Iterator that lazily projects rows based on SELECT list
///
/// This iterator wraps a source iterator and applies projection on-demand,
/// only computing projected values for rows that are actually consumed.
/// This is more efficient than eagerly projecting all rows when LIMIT/OFFSET
/// is present, as it avoids projecting rows that will be discarded.
pub struct SelectProjectionIterator<
    'a,
    I: Iterator<Item = Result<vibesql_storage::Row, crate::errors::ExecutorError>>,
> {
    source: I,
    select_list: Vec<vibesql_ast::SelectItem>,
    evaluator: crate::evaluator::CombinedExpressionEvaluator<'a>,
    input_schema: crate::schema::CombinedSchema,
    window_mapping: Option<HashMap<WindowFunctionKey, usize>>,
    buffer_pool: vibesql_storage::QueryBufferPool,
}

impl<'a, I: Iterator<Item = Result<vibesql_storage::Row, crate::errors::ExecutorError>>>
    SelectProjectionIterator<'a, I>
{
    /// Creates a new SelectProjectionIterator
    ///
    /// # Arguments
    /// * `source` - The source iterator providing rows to project
    /// * `select_list` - The SELECT items to project
    /// * `evaluator` - Expression evaluator for computing projected values
    /// * `input_schema` - Schema of the input rows
    /// * `window_mapping` - Optional mapping of window functions to column indices
    pub fn new(
        source: I,
        select_list: Vec<vibesql_ast::SelectItem>,
        evaluator: crate::evaluator::CombinedExpressionEvaluator<'a>,
        input_schema: crate::schema::CombinedSchema,
        window_mapping: Option<HashMap<WindowFunctionKey, usize>>,
        buffer_pool: vibesql_storage::QueryBufferPool,
    ) -> Self {
        Self { source, select_list, evaluator, input_schema, window_mapping, buffer_pool }
    }
}

impl<'a, I: Iterator<Item = Result<vibesql_storage::Row, crate::errors::ExecutorError>>> Iterator
    for SelectProjectionIterator<'a, I>
{
    type Item = Result<vibesql_storage::Row, crate::errors::ExecutorError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next row from source
        let row = match self.source.next()? {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };

        // Clear CSE cache before projecting this row to prevent values
        // from being incorrectly cached across different rows
        self.evaluator.clear_cse_cache();

        // Project the row using the existing projection function
        let projected = project_row_combined(
            &row,
            &self.select_list,
            &self.evaluator,
            &self.input_schema,
            &self.window_mapping,
            &self.buffer_pool,
        );

        Some(projected)
    }
}
