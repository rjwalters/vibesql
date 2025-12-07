//! Main evaluation entry point and basic expression types

use vibesql_types::SqlValue;

use super::super::core::ExpressionEvaluator;
use crate::errors::ExecutorError;

impl ExpressionEvaluator<'_> {
    /// Evaluate an expression in the context of a row
    #[inline]
    pub fn eval(
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
    #[inline]
    fn eval_impl(
        &self,
        expr: &vibesql_ast::Expression,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        match expr {
            // Literals - just return the value
            vibesql_ast::Expression::Literal(val) => Ok(val.clone()),

            // DEFAULT keyword - not allowed in SELECT/WHERE expressions
            vibesql_ast::Expression::Default => Err(ExecutorError::UnsupportedExpression(
                "DEFAULT keyword is only valid in INSERT VALUES and UPDATE SET clauses".to_string(),
            )),

            // VALUES() function - not allowed in SELECT/WHERE expressions
            vibesql_ast::Expression::DuplicateKeyValue { .. } => Err(ExecutorError::UnsupportedExpression(
                "VALUES() function is only valid in ON DUPLICATE KEY UPDATE clauses".to_string(),
            )),

            // Column reference - look up column index and get value from row
            vibesql_ast::Expression::ColumnRef { table, column } => {
                self.eval_column_ref(table.as_deref(), column, row)
            }

            // Binary operations
            vibesql_ast::Expression::BinaryOp { left, op, right } => {
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

                                self.eval_binary_op(&left_val, op, &right_val)
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

                                self.eval_binary_op(&left_val, op, &right_val)
                            }
                        }
                    }
                    // For all other operators, evaluate both sides as before
                    _ => {
                        let left_val = self.eval(left, row)?;
                        let right_val = self.eval(right, row)?;
                        self.eval_binary_op(&left_val, op, &right_val)
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

            // Scalar subquery
            vibesql_ast::Expression::ScalarSubquery(subquery) => self.eval_scalar_subquery(subquery, row),

            // BETWEEN predicate
            vibesql_ast::Expression::Between { expr, low, high, negated, symmetric } => {
                self.eval_between(expr, low, high, *negated, *symmetric, row)
            }

            // CAST expression
            vibesql_ast::Expression::Cast { expr, data_type } => self.eval_cast(expr, data_type, row),

            // POSITION expression
            vibesql_ast::Expression::Position { substring, string, character_unit: _ } => {
                self.eval_position(substring, string, row)
            }

            // TRIM expression
            vibesql_ast::Expression::Trim { position, removal_char, string } => {
                self.eval_trim(position, removal_char, string, row)
            }

            // EXTRACT expression
            vibesql_ast::Expression::Extract { field, expr } => {
                self.eval_extract(field, expr, row)
            }

            // LIKE pattern matching
            vibesql_ast::Expression::Like { expr, pattern, negated } => {
                self.eval_like(expr, pattern, *negated, row)
            }

            // IN list (value list)
            vibesql_ast::Expression::InList { expr, values, negated } => {
                self.eval_in_list(expr, values, *negated, row)
            }

            // EXISTS predicate
            vibesql_ast::Expression::Exists { subquery, negated } => {
                self.eval_exists(subquery, *negated, row)
            }

            // Quantified comparison (ALL/ANY/SOME)
            vibesql_ast::Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                self.eval_quantified(expr, op, quantifier, subquery, row)
            }

            // Function call
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
                super::super::functions::eval_scalar_function("CURRENT_TIMESTAMP", &[], &None, &sql_mode)
            }

            // INTERVAL expression
            vibesql_ast::Expression::Interval {
                value,
                unit,
                leading_precision: _,
                fractional_precision: _,
            } => {
                // Evaluate the value expression (typically a string literal like '5')
                let interval_value = self.eval(value, row)?;

                // Convert unit to string for the Interval type
                let unit_str = Self::interval_unit_to_string(unit);

                // Create an Interval SqlValue
                // The format is "value unit" (e.g., "5 DAY", "1-6 YEAR TO MONTH")
                let interval_str = format!("{} {}", interval_value, unit_str);
                Ok(SqlValue::Interval(vibesql_types::Interval::new(
                    interval_str,
                )))
            }

            // Unsupported expressions
            vibesql_ast::Expression::Wildcard => Err(ExecutorError::UnsupportedExpression(
                "Wildcard (*) not supported in expressions".to_string(),
            )),

            // Unary operations
            vibesql_ast::Expression::UnaryOp { op, expr } => {
                let val = self.eval(expr, row)?;
                super::operators::eval_unary_op(op, &val)
            }

            vibesql_ast::Expression::IsNull { expr, negated } => {
                let value = self.eval(expr, row)?;
                let is_null = matches!(value, SqlValue::Null);
                let result = if *negated { !is_null } else { is_null };
                Ok(SqlValue::Boolean(result))
            }

            vibesql_ast::Expression::WindowFunction { .. } => Err(ExecutorError::UnsupportedExpression(
                "Window functions should be evaluated separately".to_string(),
            )),

            vibesql_ast::Expression::AggregateFunction { .. } => Err(ExecutorError::UnsupportedExpression(
                "Aggregate functions should be evaluated in aggregation context".to_string(),
            )),

            // NEXT VALUE FOR sequence expression
            // TODO: Implement proper sequence evaluation
            //
            // Requirements for implementation:
            // 1. Sequence catalog objects (CREATE SEQUENCE, DROP SEQUENCE, etc.)
            // 2. Sequence state storage (current value, increment, min/max, cycle, etc.)
            // 3. Mutable access to catalog to advance sequences (architectural change)
            // 4. Thread-safe sequence value generation
            //
            // Current architecture has immutable database references in evaluator.
            // Possible solutions:
            // 1. Use RefCell<Sequence> or Arc<Mutex<Sequence>> for interior mutability
            // 2. Handle NEXT VALUE FOR at statement execution level (INSERT/SELECT)
            // 3. Change evaluator to accept mutable database reference
            // 4. Use a separate sequence manager with thread-safe state
            //
            // Note: Parser and AST support already exists (Expression::NextValue).
            // See SQL:1999 Section 6.13 for sequence expression specification.
            vibesql_ast::Expression::NextValue { sequence_name } => {
                Err(ExecutorError::UnsupportedExpression(format!(
                    "NEXT VALUE FOR {} - Sequence expressions not yet implemented. \
                    Requires sequence catalog infrastructure (CREATE SEQUENCE support), \
                    sequence state management, and mutable catalog access. \
                    Use auto-incrementing primary keys or generate values in application code instead.",
                    sequence_name
                )))
            }

            vibesql_ast::Expression::MatchAgainst { columns, search_modifier, mode } => {
                self.eval_match_against(columns, search_modifier, mode, row)
            }

            // Pseudo-variable (OLD.column, NEW.column in triggers)
            vibesql_ast::Expression::PseudoVariable { pseudo_table, column } => {
                // Resolve pseudo-variable using trigger context
                if let Some(ctx) = self.trigger_context {
                    ctx.resolve_pseudo_var(*pseudo_table, column)
                } else {
                    // This expression type is only valid in trigger context
                    // Return an error if encountered outside triggers
                    Err(ExecutorError::UnsupportedExpression(
                        format!(
                            "Pseudo-variable {}.{} is only valid within trigger bodies",
                            match pseudo_table {
                                vibesql_ast::PseudoTable::Old => "OLD",
                                vibesql_ast::PseudoTable::New => "NEW",
                            },
                            column
                        )
                    ))
                }
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
                        Ok(SqlValue::Null)
                    }
                } else {
                    // No database context available
                    Err(ExecutorError::UnsupportedExpression(
                        format!("Session variable @@{} cannot be evaluated without database context", name)
                    ))
                }
            }

            // Placeholder (?) - must be bound before evaluation
            vibesql_ast::Expression::Placeholder(idx) => {
                Err(ExecutorError::UnsupportedExpression(
                    format!("Unbound placeholder ?{} - placeholders must be bound to values before execution", idx)
                ))
            }

            // Numbered placeholder ($1, $2, etc.) - must be bound before evaluation
            vibesql_ast::Expression::NumberedPlaceholder(idx) => {
                Err(ExecutorError::UnsupportedExpression(
                    format!("Unbound numbered placeholder ${} - placeholders must be bound to values before execution", idx)
                ))
            }

            // Named placeholder (:name) - must be bound before evaluation
            vibesql_ast::Expression::NamedPlaceholder(name) => {
                Err(ExecutorError::UnsupportedExpression(
                    format!("Unbound named placeholder :{} - placeholders must be bound to values before execution", name)
                ))
            }

            // Conjunction (AND) - evaluate all children with short-circuit
            vibesql_ast::Expression::Conjunction(children) => {
                let mut result = SqlValue::Boolean(true);
                for child in children {
                    let val = self.eval(child, row)?;
                    match val {
                        SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                        SqlValue::Null => result = SqlValue::Null,
                        SqlValue::Boolean(true) => {}
                        _ => return Err(ExecutorError::TypeError(
                            format!("Conjunction requires boolean operands, got {:?}", val)
                        )),
                    }
                }
                Ok(result)
            }

            // Disjunction (OR) - evaluate all children with short-circuit
            vibesql_ast::Expression::Disjunction(children) => {
                let mut result = SqlValue::Boolean(false);
                for child in children {
                    let val = self.eval(child, row)?;
                    match val {
                        SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                        SqlValue::Null => result = SqlValue::Null,
                        SqlValue::Boolean(false) => {}
                        _ => return Err(ExecutorError::TypeError(
                            format!("Disjunction requires boolean operands, got {:?}", val)
                        )),
                    }
                }
                Ok(result)
            }
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
            SqlValue::Varchar(s) | SqlValue::Character(s) => s,
            SqlValue::Null => return Ok(SqlValue::Boolean(false)),
            other => arcstr::ArcStr::from(other.to_string().as_str()),
        };

        // Collect text values from the specified columns
        let mut text_values: Vec<arcstr::ArcStr> = Vec::new();
        for column_name in columns {
            match self.eval_column_ref(None, column_name, row) {
                Ok(SqlValue::Varchar(s)) | Ok(SqlValue::Character(s)) => text_values.push(s),
                Ok(SqlValue::Null) => {
                    // NULL values are treated as empty strings in MATCH
                    text_values.push(arcstr::ArcStr::from(""));
                }
                Ok(other) => text_values.push(arcstr::ArcStr::from(other.to_string().as_str())),
                Err(_) => {
                    // Column not found - return false for this match
                    return Ok(SqlValue::Boolean(false));
                }
            }
        }

        // Perform full-text search
        let result = super::fulltext::eval_match_against(&search_string, &text_values, mode)?;
        Ok(SqlValue::Boolean(result))
    }

    /// Evaluate column reference
    #[inline]
    fn eval_column_ref(
        &self,
        table_qualifier: Option<&str>,
        column: &str,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Special case: "*" is a wildcard used in COUNT(*) and is not a real column
        // Return NULL here - the actual COUNT(*) logic handles this specially
        if column == "*" {
            return Ok(vibesql_types::SqlValue::Null);
        }

        // Check procedural context first (variables/parameters take precedence over table columns)
        // This is only checked when there's no table qualifier, as variables don't have table prefixes
        if table_qualifier.is_none() {
            if let Some(proc_ctx) = self.procedural_context {
                // Try to get value from procedural context (checks variables then parameters)
                if let Some(value) = proc_ctx.get_value(column) {
                    return Ok(value.clone());
                }
            }
        }

        // Track which tables we searched for better error messages
        let mut searched_tables = Vec::new();
        let mut available_columns = Vec::new();

        // If table qualifier is provided, validate it matches a known schema
        if let Some(qualifier) = table_qualifier {
            let qualifier_lower = qualifier.to_lowercase();
            let inner_name_lower = self.schema.name.to_lowercase();

            // Check if qualifier matches inner schema
            if qualifier_lower == inner_name_lower {
                // Qualifier matches inner schema - search only there
                searched_tables.push(self.schema.name.clone());
                if let Some(col_index) = self.schema.get_column_index(column) {
                    return row
                        .get(col_index)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                }
            } else if let Some(outer_schema) = self.outer_schema {
                let outer_name_lower = outer_schema.name.to_lowercase();

                // Check if qualifier matches outer schema
                if qualifier_lower == outer_name_lower {
                    // Qualifier matches outer schema - search only there
                    if let Some(outer_row) = self.outer_row {
                        searched_tables.push(outer_schema.name.clone());
                        if let Some(col_index) = outer_schema.get_column_index(column) {
                            return outer_row
                                .get(col_index)
                                .cloned()
                                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
                        }
                    }
                } else {
                    // Qualifier doesn't match any known schema
                    let mut known_tables = vec![self.schema.name.clone()];
                    known_tables.push(outer_schema.name.clone());

                    return Err(ExecutorError::InvalidTableQualifier {
                        qualifier: qualifier.to_string(),
                        column: column.to_string(),
                        available_tables: known_tables,
                    });
                }
            } else {
                // No outer schema and qualifier doesn't match inner schema
                return Err(ExecutorError::InvalidTableQualifier {
                    qualifier: qualifier.to_string(),
                    column: column.to_string(),
                    available_tables: vec![self.schema.name.clone()],
                });
            }

            // If we get here, qualifier was valid but column wasn't found
            available_columns.extend(self.schema.columns.iter().map(|c| c.name.clone()));
            if let Some(outer_schema) = self.outer_schema {
                available_columns.extend(outer_schema.columns.iter().map(|c| c.name.clone()));
            }

            return Err(ExecutorError::ColumnNotFound {
                column_name: column.to_string(),
                table_name: qualifier.to_string(),
                searched_tables,
                available_columns,
            });
        }

        // No qualifier provided - use original search logic (inner first, then outer)
        // Try to resolve in inner schema first
        searched_tables.push(self.schema.name.clone());
        if let Some(col_index) = self.schema.get_column_index(column) {
            return row
                .get(col_index)
                .cloned()
                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
        }

        // If not found in inner schema and outer context exists, try outer schema
        if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
            searched_tables.push(outer_schema.name.clone());
            if let Some(col_index) = outer_schema.get_column_index(column) {
                return outer_row
                    .get(col_index)
                    .cloned()
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index });
            }
        }

        // Column not found - collect available columns for suggestions
        available_columns.extend(self.schema.columns.iter().map(|c| c.name.clone()));
        if let Some(outer_schema) = self.outer_schema {
            available_columns.extend(outer_schema.columns.iter().map(|c| c.name.clone()));
        }

        // Column not found in either schema
        Err(ExecutorError::ColumnNotFound {
            column_name: column.to_string(),
            table_name: table_qualifier.unwrap_or("unknown").to_string(),
            searched_tables,
            available_columns,
        })
    }

    /// Convert IntervalUnit to string representation for Interval SqlValue
    fn interval_unit_to_string(unit: &vibesql_ast::IntervalUnit) -> String {
        use vibesql_ast::IntervalUnit;
        match unit {
            IntervalUnit::Microsecond => "MICROSECOND",
            IntervalUnit::Second => "SECOND",
            IntervalUnit::Minute => "MINUTE",
            IntervalUnit::Hour => "HOUR",
            IntervalUnit::Day => "DAY",
            IntervalUnit::Week => "WEEK",
            IntervalUnit::Month => "MONTH",
            IntervalUnit::Quarter => "QUARTER",
            IntervalUnit::Year => "YEAR",
            IntervalUnit::SecondMicrosecond => "SECOND_MICROSECOND",
            IntervalUnit::MinuteMicrosecond => "MINUTE_MICROSECOND",
            IntervalUnit::MinuteSecond => "MINUTE_SECOND",
            IntervalUnit::HourMicrosecond => "HOUR_MICROSECOND",
            IntervalUnit::HourSecond => "HOUR_SECOND",
            IntervalUnit::HourMinute => "HOUR_MINUTE",
            IntervalUnit::DayMicrosecond => "DAY_MICROSECOND",
            IntervalUnit::DaySecond => "DAY_SECOND",
            IntervalUnit::DayMinute => "DAY_MINUTE",
            IntervalUnit::DayHour => "DAY_HOUR",
            IntervalUnit::YearMonth => "YEAR_MONTH",
        }
        .to_string()
    }
}
