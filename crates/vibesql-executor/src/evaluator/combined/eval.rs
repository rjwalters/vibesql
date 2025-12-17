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
            vibesql_ast::Expression::ColumnRef(col_id) => {
                let schema = col_id.schema_canonical();
                let table = col_id.table_canonical();
                let column = col_id.column_canonical();
                // Display forms preserve original case for error messages (SQLite compatibility)
                let table_display = col_id.table_display();
                let column_display = col_id.column_display();

                // Handle schema qualifier (three-part names like schema.table.column)
                // SQLite schemas: "main" (default), "temp" (temporary tables), or attached database names
                // For our single-database implementation:
                // - "main" is silently accepted (treated as default)
                // - Other schemas return "no such column" error to match SQLite behavior
                if let Some(schema_name) = schema {
                    let schema_lower = schema_name.to_lowercase();
                    if schema_lower != "main" {
                        // SQLite returns "no such column: schema.table.column" for unknown schemas
                        let mut available_columns = Vec::new();
                        for (_start, s) in self.schema.table_schemas.values() {
                            available_columns.extend(s.columns.iter().map(|c| c.name.clone()));
                        }
                        return Err(ExecutorError::ColumnNotFound {
                            column_name: format!(
                                "{}.{}.{}",
                                schema_name,
                                table.unwrap_or(""),
                                column
                            ),
                            table_name: table.map(|t| t.to_string()).unwrap_or_else(|| "unknown".to_string()),
                            searched_tables: self.schema.table_names(),
                            available_columns,
                        });
                    }
                }

                // Special case: "*" is a wildcard used in COUNT(*) and is not a real column
                // Return NULL here - the actual COUNT(*) logic handles this specially
                if column == "*" {
                    return Ok(vibesql_types::SqlValue::Null);
                }

                // Check for ambiguous qualified column references (SQLite compatibility - issue #4507)
                // This must be checked BEFORE resolving the column, as SQLite requires
                // an error when a table alias appears multiple times in the FROM clause.
                // Example: SELECT A.f1 FROM test1 AS A, test1 AS A => "ambiguous column name: A.f1"
                // Use display forms to preserve original case in error messages
                if let Some(table_disp) = table_display {
                    self.schema.validate_qualified_reference(table_disp, column_display)?;
                }

                // SQLite compatibility: Handle ROWID pseudo-column
                // ROWID, _rowid_, and oid are aliases that return the row's unique identifier
                let column_lower = column.to_lowercase();
                if column_lower == "rowid" || column_lower == "_rowid_" || column_lower == "oid" {
                    // First check if schema has a real column with this name
                    if self.get_column_index_cached(table, column).is_none() {
                        // No real column - check if we have a row_id for this table
                        // Use the new get_row_id_for_table method that handles both single-table
                        // and multi-table (JOIN) rows (issue #4370)
                        if let Some(row_id) = row.get_row_id_for_table(table) {
                            return Ok(vibesql_types::SqlValue::Bigint(row_id as i64));
                        }
                        // ROWID not available - return NULL (matches SQLite behavior for derived tables)
                        return Ok(vibesql_types::SqlValue::Null);
                    }
                }

                // Check procedural context first (variables/parameters take precedence over table
                // columns) This is only checked when there's no table qualifier, as
                // variables don't have table prefixes
                if table.is_none() {
                    if let Some(proc_ctx) = self.procedural_context {
                        // Try to get value from procedural context (checks variables then
                        // parameters)
                        if let Some(value) = proc_ctx.get_value(column) {
                            return Ok(value.clone());
                        }
                    }

                    // Check for ambiguous unqualified column references (SQLite compatibility)
                    // This must be checked BEFORE resolving the column, as SQLite requires
                    // an error when a column name exists in multiple joined tables.
                    if self.schema.is_column_ambiguous(column) {
                        return Err(ExecutorError::AmbiguousColumnName {
                            column_name: column.to_string(),
                        });
                    }
                }

                // Try to resolve in inner schema first
                if let Some(col_index) = self.get_column_index_cached(table, column) {
                    return row
                        .get(col_index)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                }

                // If not found in inner schema and outer context exists, try outer schema
                // FIX for issue #4493: Support chained context resolution for deeply nested subqueries
                // First try immediate parent (outer_row + outer_schema)
                if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
                    if let Some(col_index) = outer_schema.get_column_index(table, column)
                    {
                        return outer_row
                            .get(col_index)
                            .cloned()
                            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                    }
                }

                // If still not found, try chaining through outer_context for grandparent and beyond
                // This implements SQLite-style NameContext chaining for arbitrary nesting depth
                if let Some(outer_context) = self.outer_context {
                    // Recursively resolve through the context chain
                    // The outer_context will search its own schema, then its outer schema, etc.
                    if let (Some(outer_row), Some(_)) = (outer_context.outer_row, outer_context.outer_schema) {
                        if let Some(col_index) = outer_context.schema.get_column_index(table, column) {
                            return outer_row
                                .get(col_index)
                                .cloned()
                                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                        }
                        // Continue chaining recursively
                        if let Some(grandparent_context) = outer_context.outer_context {
                            // TODO: This should be a recursive call, but we need to restructure
                            // For now, this provides 3-level nesting support
                            if let (Some(grandparent_row), Some(_)) = (grandparent_context.outer_row, grandparent_context.outer_schema) {
                                if let Some(col_index) = grandparent_context.schema.get_column_index(table, column) {
                                    return grandparent_row
                                        .get(col_index)
                                        .cloned()
                                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                                }
                            }
                        }
                    }
                }

                // Column not found in either schema - collect diagnostic info
                let searched_tables: Vec<String> = self.schema.table_names();
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
                    column_name: column.to_string(),
                    table_name: table.map(|t| t.to_string()).unwrap_or_else(|| "unknown".to_string()),
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

            // IS DISTINCT FROM / IS NOT DISTINCT FROM (SQL:1999)
            vibesql_ast::Expression::IsDistinctFrom { left, right, negated } => {
                self.eval_is_distinct_from(left, right, *negated, row)
            }

            // IS TRUE / IS FALSE / IS UNKNOWN (SQL:1999)
            vibesql_ast::Expression::IsTruthValue { expr, truth_value, negated } => {
                let val = self.eval(expr, row)?;
                // SQL:1999 three-valued logic for IS TRUE/FALSE/UNKNOWN:
                // - IS TRUE: TRUE if expr is TRUE, FALSE if expr is FALSE or UNKNOWN
                // - IS FALSE: TRUE if expr is FALSE, FALSE if expr is TRUE or UNKNOWN
                // - IS UNKNOWN: TRUE if expr is UNKNOWN (NULL), FALSE if expr is TRUE or FALSE
                // - IS NOT X: negates the result
                //
                // SQLite compatibility: integers are treated as booleans
                // - 0 is FALSE
                // - Non-zero integers are TRUE
                // - NULL is UNKNOWN
                let result = match truth_value {
                    vibesql_ast::TruthValue::True => match &val {
                        vibesql_types::SqlValue::Boolean(true) => true,
                        vibesql_types::SqlValue::Integer(n) => *n != 0,
                        vibesql_types::SqlValue::Bigint(n) => *n != 0,
                        vibesql_types::SqlValue::Smallint(n) => *n != 0,
                        _ => false,
                    },
                    vibesql_ast::TruthValue::False => match &val {
                        vibesql_types::SqlValue::Boolean(false) => true,
                        vibesql_types::SqlValue::Integer(0) => true,
                        vibesql_types::SqlValue::Bigint(0) => true,
                        vibesql_types::SqlValue::Smallint(0) => true,
                        _ => false,
                    },
                    vibesql_ast::TruthValue::Unknown => matches!(val, vibesql_types::SqlValue::Null),
                };
                let final_result = if *negated { !result } else { result };
                Ok(vibesql_types::SqlValue::Boolean(final_result))
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
            vibesql_ast::Expression::AggregateFunction { name, .. } => {
                // SQLite-compatible error message for aggregate misuse in execution context
                // This error occurs when an aggregate is evaluated outside of aggregation,
                // such as in ORDER BY clauses of non-aggregate queries.
                // Uses "misuse of aggregate: X()" format (with colon) to match SQLite's expr.c
                Err(ExecutorError::MisuseOfAggregateContext { function_name: name.clone() })
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

            // COLLATE expression - evaluate inner expression (collation affects string comparison)
            // TODO: Full collation support - for now just evaluate the inner expression
            vibesql_ast::Expression::Collate { expr, .. } => self.eval(expr, row),

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
