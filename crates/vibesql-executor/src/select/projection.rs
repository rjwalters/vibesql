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
                // SELECT * - include all columns except hidden ones (from NATURAL JOIN
                // deduplication) When window functions are present, only include
                // base columns (not appended window values)
                //
                // SQL Standard Column Ordering for USING/NATURAL JOINs:
                // According to SQL:2011, SELECT * from JOIN USING should output:
                // 1. The common/USING columns (merged) - appear FIRST
                // 2. Remaining columns from the left table
                // 3. Remaining columns from the right table
                let max_col = if let Some(mapping) = window_mapping {
                    if !mapping.is_empty() {
                        // Find the minimum window column index to know where base columns end
                        mapping.values().min().copied().unwrap_or(row.values.len())
                    } else {
                        row.values.len()
                    }
                } else {
                    row.values.len()
                };

                // Build column indices, reordering for SQL standard USING/NATURAL JOIN output
                // If there are joined columns, put them first
                let column_indices: Vec<usize> = if schema.joined_columns.is_empty() {
                    // No USING/NATURAL JOIN - use natural order
                    (0..max_col).collect()
                } else {
                    // Collect visible column indices with their join status and column name
                    // Store (abs_idx, col_name_lower, is_joined)
                    let mut joined_cols: Vec<(usize, String)> = Vec::new();
                    let mut other_cols: Vec<usize> = Vec::new();

                    // Sort table_schemas by start_index for deterministic iteration order
                    // HashMap iteration is non-deterministic; sorting ensures consistent results
                    let mut sorted_tables: Vec<_> = schema.table_schemas.iter().collect();
                    sorted_tables.sort_by_key(|(_, (start_index, _))| *start_index);

                    // Build a reverse lookup: for each column index, find its name
                    // to check if it's a joined column
                    for (table_id, (start_index, table_schema)) in sorted_tables {
                        // Issue #4786: Skip alias tables in SELECT * expansion.
                        // Alias tables are virtual tables created for parenthesized join expressions
                        // (e.g., `(...) AS j1`). They have start_index=0 which doesn't match the
                        // actual row positions, so including them would produce wrong column values.
                        // They exist only for qualified column resolution (`j1.column`), not for
                        // SELECT * expansion. The base tables' visible columns are used instead.
                        if schema.alias_tables.contains(table_id) {
                            continue;
                        }
                        for (col_idx, col_schema) in table_schema.columns.iter().enumerate() {
                            let abs_idx = start_index + col_idx;
                            if abs_idx >= max_col {
                                continue;
                            }

                            // Skip replacement targets (they're output via hidden column's
                            // position)
                            if schema.column_replacement_map.values().any(|&v| v == abs_idx) {
                                continue;
                            }

                            // Skip right-side USING columns (they're output via the left-side
                            // column with COALESCE applied)
                            if schema.is_using_coalesce_right_side(abs_idx) {
                                continue;
                            }

                            // Check if column should be included
                            // Hidden columns are included if they have a replacement OR
                            // if they are USING columns with coalesce pairs (for COALESCE output)
                            let should_include = if schema.is_column_hidden(abs_idx) {
                                schema.get_column_replacement(abs_idx).is_some()
                                    || schema.get_using_coalesce_right_for_left(abs_idx).is_some()
                            } else {
                                true
                            };

                            if should_include {
                                let col_name_lower = col_schema.name.to_lowercase();
                                // A column should be reordered to the front only if ALL of:
                                // 1. Its name is in joined_columns (from USING/NATURAL JOIN)
                                // 2. It's the FIRST index in a using_coalesce_indices chain
                                // 3. That first index is 0 (meaning the USING is at top level)
                                //
                                // Condition 3 prevents reordering when the USING join is nested
                                // inside a parenthesized expression with an ON join at the outer
                                // level. E.g., for `t3 FULL JOIN (...) AS j1 ON j1.id=t3.id`,
                                // the inner join's coalesced id starts at idx > 0, so it should
                                // NOT be moved to the front of t3's columns.
                                let is_joined = schema.joined_columns.contains(&col_name_lower)
                                    && schema
                                        .using_coalesce_indices
                                        .get(&col_name_lower)
                                        .map_or(false, |indices| {
                                            // Must be first in chain AND chain starts at idx 0
                                            indices.first() == Some(&abs_idx) && abs_idx == 0
                                        });
                                if is_joined {
                                    joined_cols.push((abs_idx, col_name_lower));
                                } else {
                                    other_cols.push(abs_idx);
                                }
                            }
                        }
                    }

                    // Sort each group by index to maintain relative order
                    joined_cols.sort_by_key(|(idx, _)| *idx);
                    other_cols.sort();

                    // Deduplicate joined columns: for chained NATURAL JOINs like
                    // t4 NATURAL RIGHT JOIN t5 NATURAL RIGHT JOIN t6, multiple columns
                    // might be marked as "joined" with the same name (e.g., both t4.id
                    // and t5.id). We should only output ONE column per joined column name.
                    // Keep the first occurrence (which has the lowest index after sorting).
                    let mut seen_joined_columns: std::collections::HashSet<String> =
                        std::collections::HashSet::new();
                    let deduped_joined: Vec<usize> = joined_cols
                        .into_iter()
                        .filter_map(|(idx, col_name)| {
                            if seen_joined_columns.contains(&col_name) {
                                None // Skip duplicate joined column
                            } else {
                                seen_joined_columns.insert(col_name);
                                Some(idx)
                            }
                        })
                        .collect();

                    // Concatenate: joined columns first, then others
                    deduped_joined.into_iter().chain(other_cols).collect()
                };

                // Iterate through reordered columns, handling hidden columns with replacements
                for idx in column_indices {
                    if schema.is_column_hidden(idx) {
                        // For hidden USING columns in OUTER JOINs, check for coalesce chain first
                        // This is needed for 3+ table FULL JOINs where the replacement always
                        // points to the rightmost column, but we need N-way COALESCE semantics
                        // to find the first non-NULL from ANY table in the chain.
                        if let Some(all_indices) = schema.get_all_coalesce_indices_for_column(idx) {
                            // Apply N-way COALESCE: return first non-NULL from entire chain
                            let mut found = false;
                            for &chain_idx in all_indices {
                                if chain_idx < row.values.len() {
                                    let val = &row.values[chain_idx];
                                    if *val != vibesql_types::SqlValue::Null {
                                        values.push(val.clone());
                                        found = true;
                                        break;
                                    }
                                }
                            }
                            if !found {
                                values.push(vibesql_types::SqlValue::Null);
                            }
                        } else if let Some(replacement_idx) = schema.get_column_replacement(idx) {
                            // Simple replacement without coalesce chain (2-table case)
                            if replacement_idx < row.values.len() {
                                values.push(row.values[replacement_idx].clone());
                            }
                        }
                        // If neither coalesce chain nor replacement, skip this hidden column
                    } else {
                        // Check if this column is part of a USING coalesce chain
                        // In FULL OUTER JOIN with USING, the visible column should show
                        // N-way COALESCE to handle unmatched rows from any side in the chain
                        if let Some(all_indices) = schema.get_all_coalesce_indices_for_column(idx) {
                            // Apply N-way COALESCE: return first non-NULL from entire chain
                            let mut found = false;
                            for &chain_idx in all_indices {
                                if chain_idx < row.values.len() {
                                    let val = &row.values[chain_idx];
                                    if *val != vibesql_types::SqlValue::Null {
                                        values.push(val.clone());
                                        found = true;
                                        break;
                                    }
                                }
                            }
                            if !found {
                                values.push(vibesql_types::SqlValue::Null);
                            }
                        } else {
                            values.push(row.values[idx].clone());
                        }
                    }
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
            vibesql_ast::SelectItem::Expression { expr, alias: _, .. } => {
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
