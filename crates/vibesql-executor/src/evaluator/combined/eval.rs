//! Main evaluation entry point for combined expressions

use super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use crate::{errors::ExecutorError, select::WindowFunctionKey};

impl CombinedExpressionEvaluator<'_> {
    /// Evaluate an expression in the context of a combined row
    /// This is the main entry point for expression evaluation
    pub(crate) fn eval(
        &self,
        expr: &vibesql_ast::Expression,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow from deeply nested expressions
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        // CSE: Check cache if enabled and expression is deterministic
        if self.enable_cse
            && super::super::expression_hash::ExpressionHasher::is_deterministic(expr)
        {
            let hash = super::super::expression_hash::ExpressionHasher::hash(expr);

            // Check cache (get requires mut borrow to update LRU order)
            if let Some(cached) = self.cse_cache.borrow_mut().get(&hash) {
                return Ok(cached.clone());
            }

            // Evaluate with depth increment and cache result
            let result = self.with_incremented_depth(|evaluator| evaluator.eval_impl(expr, row))?;
            self.cse_cache.borrow_mut().put(hash, result.clone());
            return Ok(result);
        }

        // Non-cached path: increment depth and evaluate
        self.with_incremented_depth(|evaluator| evaluator.eval_impl(expr, row))
    }

    /// Internal implementation of eval with depth already incremented
    fn eval_impl(
        &self,
        expr: &vibesql_ast::Expression,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        match expr {
            // Literals - just return the value
            vibesql_ast::Expression::Literal(val) => Ok(val.clone()),

            // DEFAULT keyword - not allowed in UPDATE/SELECT expressions
            // DEFAULT is only valid in INSERT VALUES and UPDATE SET
            // This evaluator is used for SELECT and WHERE clauses where DEFAULT is invalid
            vibesql_ast::Expression::Default => Err(ExecutorError::UnsupportedExpression(
                "DEFAULT keyword is only valid in INSERT VALUES and UPDATE SET clauses".to_string(),
            )),

            // Column reference - look up column index (with optional table qualifier)
            vibesql_ast::Expression::ColumnRef { table, column } => {
                // Special case: "*" is a wildcard used in COUNT(*) and is not a real column
                // Return NULL here - the actual COUNT(*) logic handles this specially
                if column == "*" {
                    return Ok(vibesql_types::SqlValue::Null);
                }

                // Check procedural context first (variables/parameters take precedence over table columns)
                // This is only checked when there's no table qualifier, as variables don't have table prefixes
                if table.is_none() {
                    if let Some(proc_ctx) = self.procedural_context {
                        // Try to get value from procedural context (checks variables then parameters)
                        if let Some(value) = proc_ctx.get_value(column) {
                            return Ok(value.clone());
                        }
                    }
                }

                // Try to resolve in inner schema first
                if let Some(col_index) = self.get_column_index_cached(table.as_deref(), column) {
                    return row
                        .get(col_index)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                }

                // If not found in inner schema and outer context exists, try outer schema
                if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
                    if let Some(col_index) = outer_schema.get_column_index(table.as_deref(), column)
                    {
                        return outer_row
                            .get(col_index)
                            .cloned()
                            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                    }
                }

                // Column not found in either schema - collect diagnostic info
                let searched_tables: Vec<String> =
                    self.schema.table_names();
                let mut available_columns = Vec::new();
                for (_start, schema) in self.schema.table_schemas.values() {
                    available_columns.extend(schema.columns.iter().map(|c| c.name.clone()));
                }
                if let Some(outer_schema) = self.outer_schema {
                    for (_start, schema) in outer_schema.table_schemas.values() {
                        available_columns.extend(schema.columns.iter().map(|c| c.name.clone()));
                    }
                }

                Err(ExecutorError::ColumnNotFound {
                    column_name: column.clone(),
                    table_name: table.clone().unwrap_or_else(|| "unknown".to_string()),
                    searched_tables,
                    available_columns,
                })
            }

            // Binary operations
            vibesql_ast::Expression::BinaryOp { left, op, right } => {
                use vibesql_types::SqlValue;

                // Short-circuit evaluation for AND/OR operators
                match op {
                    vibesql_ast::BinaryOperator::And => {
                        let left_val = self.eval(left, row)?;
                        // Short-circuit: if left is false, return false immediately
                        match left_val {
                            SqlValue::Boolean(false) => Ok(SqlValue::Boolean(false)),
                            // For NULL and TRUE, must evaluate right side
                            // SQL three-valued logic:
                            // - NULL AND FALSE = FALSE (not NULL!)
                            // - NULL AND TRUE = NULL
                            // - TRUE AND x = x
                            _ => {
                                let right_val = self.eval(right, row)?;

                                // Special case: NULL AND FALSE = FALSE
                                if matches!(left_val, SqlValue::Null)
                                    && matches!(right_val, SqlValue::Boolean(false))
                                {
                                    return Ok(SqlValue::Boolean(false));
                                }

                                let sql_mode =
                                    self.database.map(|db| db.sql_mode()).unwrap_or_default();
                                ExpressionEvaluator::eval_binary_op_static(
                                    &left_val, op, &right_val, sql_mode,
                                )
                            }
                        }
                    }
                    vibesql_ast::BinaryOperator::Or => {
                        let left_val = self.eval(left, row)?;
                        // Short-circuit: if left is true, return true immediately
                        match left_val {
                            SqlValue::Boolean(true) => Ok(SqlValue::Boolean(true)),
                            // For NULL and FALSE, must evaluate right side
                            // SQL three-valued logic:
                            // - NULL OR TRUE = TRUE (not NULL!)
                            // - NULL OR FALSE = NULL
                            // - FALSE OR x = x
                            _ => {
                                let right_val = self.eval(right, row)?;

                                // Special case: NULL OR TRUE = TRUE
                                if matches!(left_val, SqlValue::Null)
                                    && matches!(right_val, SqlValue::Boolean(true))
                                {
                                    return Ok(SqlValue::Boolean(true));
                                }

                                let sql_mode =
                                    self.database.map(|db| db.sql_mode()).unwrap_or_default();
                                ExpressionEvaluator::eval_binary_op_static(
                                    &left_val, op, &right_val, sql_mode,
                                )
                            }
                        }
                    }
                    // For all other operators, evaluate both sides as before
                    _ => {
                        let left_val = self.eval(left, row)?;
                        let right_val = self.eval(right, row)?;
                        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                        ExpressionEvaluator::eval_binary_op_static(
                            &left_val, op, &right_val, sql_mode,
                        )
                    }
                }
            }

            // CASE expression
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                self.eval_case(operand, when_clauses, else_result, row)
            }

            // IN operator with subquery
            vibesql_ast::Expression::In { expr, subquery, negated } => {
                self.eval_in_subquery(expr, subquery, *negated, row)
            }

            // Scalar subquery - must return exactly one row and one column
            vibesql_ast::Expression::ScalarSubquery(subquery) => {
                self.eval_scalar_subquery(subquery, row)
            }

            // BETWEEN predicate: expr BETWEEN low AND high
            vibesql_ast::Expression::Between { expr, low, high, negated, symmetric } => {
                self.eval_between(expr, low, high, *negated, *symmetric, row)
            }

            // CAST expression: CAST(expr AS data_type)
            vibesql_ast::Expression::Cast { expr, data_type } => {
                self.eval_cast(expr, data_type, row)
            }

            // POSITION expression: POSITION(substring IN string)
            vibesql_ast::Expression::Position { substring, string, character_unit: _ } => {
                self.eval_position(substring, string, row)
            }

            // TRIM expression: TRIM([position] [removal_char FROM] string)
            vibesql_ast::Expression::Trim { position, removal_char, string } => {
                self.eval_trim(position, removal_char, string, row)
            }

            // EXTRACT expression: EXTRACT(field FROM expr)
            vibesql_ast::Expression::Extract { field, expr } => self.eval_extract(field, expr, row),

            // LIKE pattern matching: expr LIKE pattern
            vibesql_ast::Expression::Like { expr, pattern, negated } => {
                self.eval_like(expr, pattern, *negated, row)
            }

            // IN operator with value list: expr IN (val1, val2, ...)
            vibesql_ast::Expression::InList { expr, values, negated } => {
                self.eval_in_list(expr, values, *negated, row)
            }

            // EXISTS predicate: EXISTS (SELECT ...)
            vibesql_ast::Expression::Exists { subquery, negated } => {
                self.eval_exists(subquery, *negated, row)
            }

            // Quantified comparison: expr op ALL/ANY/SOME (SELECT ...)
            vibesql_ast::Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                self.eval_quantified(expr, op, quantifier, subquery, row)
            }

            // IS NULL / IS NOT NULL
            vibesql_ast::Expression::IsNull { expr, negated } => {
                self.eval_is_null(expr, *negated, row)
            }

            // Function expressions - handle scalar functions (not aggregates)
            vibesql_ast::Expression::Function { name, args, character_unit } => {
                self.eval_function(name, args, character_unit, row)
            }

            // Current date/time functions
            vibesql_ast::Expression::CurrentDate => {
                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                super::super::functions::eval_scalar_function("CURRENT_DATE", &[], &None, &sql_mode)
            }
            vibesql_ast::Expression::CurrentTime { precision: _ } => {
                // For now, ignore precision and call existing function
                // Phase 2 will implement precision-aware formatting
                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                super::super::functions::eval_scalar_function("CURRENT_TIME", &[], &None, &sql_mode)
            }
            vibesql_ast::Expression::CurrentTimestamp { precision: _ } => {
                // For now, ignore precision and call existing function
                // Phase 2 will implement precision-aware formatting
                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                super::super::functions::eval_scalar_function(
                    "CURRENT_TIMESTAMP",
                    &[],
                    &None,
                    &sql_mode,
                )
            }

            // Unary operations (delegate to shared function)
            vibesql_ast::Expression::UnaryOp { op, expr } => self.eval_unary(op, expr, row),

            // Window functions - look up pre-computed values
            vibesql_ast::Expression::WindowFunction { function, over } => {
                if let Some(mapping) = self.window_mapping {
                    let key = WindowFunctionKey::from_expression(function, over);
                    if let Some(&col_idx) = mapping.get(&key) {
                        // Extract the pre-computed value from the appended column
                        let value =
                            row.values.get(col_idx).cloned().ok_or({
                                ExecutorError::ColumnIndexOutOfBounds { index: col_idx }
                            })?;
                        Ok(value)
                    } else {
                        Err(ExecutorError::UnsupportedExpression(format!(
                            "Window function not found in mapping: {:?}",
                            expr
                        )))
                    }
                } else {
                    Err(ExecutorError::UnsupportedExpression(
                        "Window functions require window mapping context".to_string(),
                    ))
                }
            }

            // Aggregate functions - should be evaluated in aggregation context
            vibesql_ast::Expression::AggregateFunction { .. } => {
                Err(ExecutorError::UnsupportedExpression(
                    "Aggregate functions should be evaluated in aggregation context".to_string(),
                ))
            }

            // Full-text search
            vibesql_ast::Expression::MatchAgainst { columns, search_modifier, mode } => {
                self.eval_match_against(columns, search_modifier, mode, row)
            }

            // Session variable (@@sql_mode, @@version, etc.)
            vibesql_ast::Expression::SessionVariable { name } => {
                // Get session variable from database metadata
                if let Some(db) = self.database {
                    // Get the session variable value from the database metadata
                    if let Some(value) = db.get_session_variable(name) {
                        Ok(value.clone())
                    } else {
                        // Variable not found - return NULL (MySQL behavior)
                        Ok(vibesql_types::SqlValue::Null)
                    }
                } else {
                    // No database context available
                    Err(ExecutorError::UnsupportedExpression(format!(
                        "Session variable @@{} cannot be evaluated without database context",
                        name
                    )))
                }
            }

            // N-ary conjunction (flattened AND chain)
            // SQL three-valued logic: FALSE dominates, then NULL, then TRUE
            vibesql_ast::Expression::Conjunction(terms) => {
                use vibesql_types::SqlValue;
                let mut has_null = false;
                for term in terms {
                    let val = self.eval(term, row)?;
                    match val {
                        SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                        SqlValue::Boolean(true) => {}
                        SqlValue::Null => has_null = true,
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "Conjunction term is not boolean: {:?}",
                                val
                            )))
                        }
                    }
                }
                if has_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(true))
                }
            }

            // N-ary disjunction (flattened OR chain)
            // SQL three-valued logic: TRUE dominates, then NULL, then FALSE
            vibesql_ast::Expression::Disjunction(terms) => {
                use vibesql_types::SqlValue;
                let mut has_null = false;
                for term in terms {
                    let val = self.eval(term, row)?;
                    match val {
                        SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                        SqlValue::Boolean(false) => {}
                        SqlValue::Null => has_null = true,
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "Disjunction term is not boolean: {:?}",
                                val
                            )))
                        }
                    }
                }
                if has_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(false))
                }
            }

            // Unsupported expressions
            _ => Err(ExecutorError::UnsupportedExpression(format!("{:?}", expr))),
        }
    }

    /// Evaluate a MATCH...AGAINST full-text search expression
    fn eval_match_against(
        &self,
        columns: &[String],
        search_modifier: &vibesql_ast::Expression,
        mode: &vibesql_ast::FulltextMode,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Evaluate the search string
        let search_value = self.eval(search_modifier, row)?;
        let search_string: arcstr::ArcStr = match search_value {
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => s,
            vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Boolean(false)),
            other => arcstr::ArcStr::from(other.to_string().as_str()),
        };

        // Collect text values from the specified columns
        let mut text_values: Vec<arcstr::ArcStr> = Vec::new();
        for column_name in columns {
            // Try to resolve column in inner schema
            let col_value = if let Some(col_index) = self.get_column_index_cached(None, column_name)
            {
                row.get(col_index).cloned()
            } else if let (Some(outer_row), Some(outer_schema)) =
                (self.outer_row, self.outer_schema)
            {
                // Try outer schema if available
                if let Some(col_index) = outer_schema.get_column_index(None, column_name) {
                    outer_row.get(col_index).cloned()
                } else {
                    None
                }
            } else {
                None
            };

            match col_value {
                Some(vibesql_types::SqlValue::Varchar(s))
                | Some(vibesql_types::SqlValue::Character(s)) => text_values.push(s),
                Some(vibesql_types::SqlValue::Null) => {
                    // NULL values are treated as empty strings in MATCH
                    text_values.push(arcstr::ArcStr::from(""));
                }
                Some(other) => text_values.push(arcstr::ArcStr::from(other.to_string().as_str())),
                None => {
                    // Column not found - return false for this match
                    return Ok(vibesql_types::SqlValue::Boolean(false));
                }
            }
        }

        // Perform full-text search
        let result = super::super::expressions::fulltext::eval_match_against(
            &search_string,
            &text_values,
            mode,
        )?;
        Ok(vibesql_types::SqlValue::Boolean(result))
    }
}
