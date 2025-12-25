//! Main evaluation entry point and basic expression types

use vibesql_types::{SqlValue, TypeAffinity};

use super::super::core::ExpressionEvaluator;
use crate::errors::ExecutorError;

impl ExpressionEvaluator<'_> {
    /// Get the SQLite type affinity of an expression if it's a column reference.
    ///
    /// Returns Some(affinity) if the expression is a column reference and we can
    /// determine its declared type from the schema. Returns None for literals,
    /// function calls, and other non-column expressions.
    ///
    /// This is used to implement SQLite's type affinity rules for comparisons:
    /// - TEXT column vs INTEGER literal → convert INTEGER to TEXT, string compare
    /// - Bare column (NONE affinity) vs INTEGER → type ordering (TEXT > INTEGER)
    pub(super) fn get_expression_affinity(
        &self,
        expr: &vibesql_ast::Expression,
    ) -> Option<TypeAffinity> {
        match expr {
            vibesql_ast::Expression::ColumnRef(col_id) => {
                // Look up the column in the schema to get its declared type
                let column_name = col_id.column_canonical();
                if let Some(col_idx) = self.schema.get_column_index(column_name) {
                    let col_schema = &self.schema.columns[col_idx];
                    Some(col_schema.data_type.sqlite_affinity())
                } else {
                    // Column not found in schema - treat as NONE affinity
                    Some(TypeAffinity::None)
                }
            }
            // For COLLATE expressions, get affinity of the inner expression
            vibesql_ast::Expression::Collate { expr, .. } => self.get_expression_affinity(expr),
            // Literals, functions, and other expressions don't have column affinity
            _ => None,
        }
    }

    /// Get the effective collation for an expression.
    ///
    /// Returns the collation from:
    /// 1. Explicit COLLATE clause (highest priority)
    /// 2. Column-level collation from CREATE TABLE definition
    /// 3. None (use default binary collation)
    ///
    /// SQLite documentation states:
    /// "A column's collating function can be specified using the COLLATE clause
    /// in the column definition within the CREATE TABLE statement."
    ///
    /// Explicit COLLATE in the query overrides column-level collation.
    pub(super) fn get_expression_collation(
        &self,
        expr: &vibesql_ast::Expression,
    ) -> Option<String> {
        match expr {
            // Explicit COLLATE has highest priority
            vibesql_ast::Expression::Collate { collation, .. } => Some(collation.clone()),
            // Column reference - look up column's declared collation
            vibesql_ast::Expression::ColumnRef(col_id) => {
                let column_name = col_id.column_canonical();
                if let Some(col_idx) = self.schema.get_column_index(column_name) {
                    self.schema.columns[col_idx].collation.clone()
                } else {
                    None
                }
            }
            // Other expressions don't have intrinsic collation
            _ => None,
        }
    }

    /// Check if an expression is a numeric literal (INTEGER or REAL).
    pub(super) fn is_numeric_literal(&self, expr: &vibesql_ast::Expression) -> bool {
        match expr {
            vibesql_ast::Expression::Literal(val) => {
                matches!(
                    val,
                    SqlValue::Integer(_)
                        | SqlValue::Smallint(_)
                        | SqlValue::Bigint(_)
                        | SqlValue::Unsigned(_)
                        | SqlValue::Float(_)
                        | SqlValue::Real(_)
                        | SqlValue::Double(_)
                        | SqlValue::Numeric(_)
                )
            }
            _ => false,
        }
    }

    /// Check if an expression is a string literal (VARCHAR or CHAR).
    pub(super) fn is_string_literal(&self, expr: &vibesql_ast::Expression) -> bool {
        match expr {
            vibesql_ast::Expression::Literal(val) => {
                matches!(val, SqlValue::Varchar(_) | SqlValue::Character(_))
            }
            _ => false,
        }
    }

    /// Try to convert a string SqlValue to a numeric SqlValue.
    /// Returns the original value if the string doesn't look like a number.
    /// This implements SQLite's NUMERIC affinity coercion rules.
    fn try_coerce_string_to_numeric(val: &SqlValue) -> SqlValue {
        match val {
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                let trimmed = s.trim();
                // Try parsing as integer first
                if let Ok(n) = trimmed.parse::<i64>() {
                    return SqlValue::Integer(n);
                }
                // Try parsing as float (use Double for higher precision)
                if let Ok(n) = trimmed.parse::<f64>() {
                    return SqlValue::Double(n);
                }
                // Not a number - return original value
                val.clone()
            }
            _ => val.clone(),
        }
    }

    /// Apply SQLite affinity rules for comparisons.
    ///
    /// When comparing a TEXT-affinity column to an INTEGER literal, SQLite:
    /// 1. Converts the INTEGER to TEXT
    /// 2. Performs string comparison
    ///
    /// This function returns modified values based on affinity rules.
    pub(super) fn apply_affinity_for_comparison(
        &self,
        left_expr: &vibesql_ast::Expression,
        left_val: SqlValue,
        right_expr: &vibesql_ast::Expression,
        right_val: SqlValue,
    ) -> (SqlValue, SqlValue) {
        let left_affinity = self.get_expression_affinity(left_expr);
        let right_affinity = self.get_expression_affinity(right_expr);

        // Case 1: Left is TEXT column, right is numeric literal
        // Convert the numeric literal to text for string comparison
        if left_affinity == Some(TypeAffinity::Text) && self.is_numeric_literal(right_expr) {
            let right_as_text = match &right_val {
                SqlValue::Integer(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Smallint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Bigint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Unsigned(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Float(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Real(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Double(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Numeric(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                _ => right_val,
            };
            return (left_val, right_as_text);
        }

        // Case 2: Right is TEXT column, left is numeric literal
        // Convert the numeric literal to text for string comparison
        if right_affinity == Some(TypeAffinity::Text) && self.is_numeric_literal(left_expr) {
            let left_as_text = match &left_val {
                SqlValue::Integer(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Smallint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Bigint(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Unsigned(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Float(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Real(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Double(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                SqlValue::Numeric(n) => SqlValue::Varchar(arcstr::ArcStr::from(n.to_string())),
                _ => left_val,
            };
            return (left_as_text, right_val);
        }

        // Case 3: Left is NUMERIC/INTEGER/REAL column, right is string literal
        // Try to convert the string literal to a number for numeric comparison
        // Per SQLite: NUMERIC affinity tries to convert strings to numbers if possible
        let is_left_numeric_affinity = matches!(
            left_affinity,
            Some(TypeAffinity::Numeric) | Some(TypeAffinity::Integer) | Some(TypeAffinity::Real)
        );
        if is_left_numeric_affinity && self.is_string_literal(right_expr) {
            let right_coerced = Self::try_coerce_string_to_numeric(&right_val);
            return (left_val, right_coerced);
        }

        // Case 4: Right is NUMERIC/INTEGER/REAL column, left is string literal
        // Try to convert the string literal to a number for numeric comparison
        let is_right_numeric_affinity = matches!(
            right_affinity,
            Some(TypeAffinity::Numeric) | Some(TypeAffinity::Integer) | Some(TypeAffinity::Real)
        );
        if is_right_numeric_affinity && self.is_string_literal(left_expr) {
            let left_coerced = Self::try_coerce_string_to_numeric(&left_val);
            return (left_coerced, right_val);
        }

        // No affinity conversion needed - use original values
        // This includes:
        // - Bare columns (NONE affinity) vs numeric → type ordering (handled in compare)
        // - Literal vs literal → type ordering (handled in compare)
        // - Same-type comparisons → direct comparison
        (left_val, right_val)
    }

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
            vibesql_ast::Expression::ColumnRef(col_id) => {
                self.eval_column_ref(col_id.schema_canonical(), col_id.table_canonical(), col_id.column_canonical(), row)
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
                        // Check for COLLATE expressions on either side for comparison operators
                        let is_comparison = matches!(
                            op,
                            vibesql_ast::BinaryOperator::Equal
                                | vibesql_ast::BinaryOperator::NotEqual
                                | vibesql_ast::BinaryOperator::LessThan
                                | vibesql_ast::BinaryOperator::LessThanOrEqual
                                | vibesql_ast::BinaryOperator::GreaterThan
                                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                        );

                        // Get effective collation from either side
                        // Priority: explicit COLLATE > column-level collation
                        // Check left side first, then right side
                        let collation = if is_comparison {
                            self.get_expression_collation(left)
                                .or_else(|| self.get_expression_collation(right))
                        } else {
                            None
                        };

                        let left_val = self.eval(left, row)?;
                        let right_val = self.eval(right, row)?;

                        // Apply collation to string values if needed
                        let (left_val, right_val) = if let Some(ref collation_name) = collation {
                            let collation_lower = collation_name.to_lowercase();
                            if collation_lower == "nocase" {
                                // For NOCASE collation, uppercase both string values
                                let left_transformed = match &left_val {
                                    SqlValue::Varchar(s) => SqlValue::Varchar(arcstr::ArcStr::from(s.to_uppercase())),
                                    SqlValue::Character(s) => SqlValue::Character(arcstr::ArcStr::from(s.to_uppercase())),
                                    other => other.clone(),
                                };
                                let right_transformed = match &right_val {
                                    SqlValue::Varchar(s) => SqlValue::Varchar(arcstr::ArcStr::from(s.to_uppercase())),
                                    SqlValue::Character(s) => SqlValue::Character(arcstr::ArcStr::from(s.to_uppercase())),
                                    other => other.clone(),
                                };
                                (left_transformed, right_transformed)
                            } else {
                                // For BINARY or other collations, use values as-is
                                (left_val, right_val)
                            }
                        } else {
                            (left_val, right_val)
                        };

                        // Apply SQLite type affinity rules for comparisons
                        // TEXT column vs INTEGER literal → convert INTEGER to TEXT, string compare
                        let (left_val, right_val) = if is_comparison {
                            self.apply_affinity_for_comparison(left, left_val, right, right_val)
                        } else {
                            (left_val, right_val)
                        };

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

            // GLOB pattern matching (SQLite)
            vibesql_ast::Expression::Glob { expr, pattern, negated } => {
                self.eval_glob(expr, pattern, *negated, row)
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
                self.eval_function(name.display(), args, character_unit, row)
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

            vibesql_ast::Expression::IsDistinctFrom { left, right, negated } => {
                let left_val = self.eval(left, row)?;
                let right_val = self.eval(right, row)?;
                let is_distinct = super::super::core::values_are_distinct(&left_val, &right_val);
                let result = if *negated { !is_distinct } else { is_distinct };
                Ok(SqlValue::Boolean(result))
            }

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
                        SqlValue::Boolean(true) => true,
                        SqlValue::Integer(n) => *n != 0,
                        SqlValue::Bigint(n) => *n != 0,
                        SqlValue::Smallint(n) => *n != 0,
                        _ => false,
                    },
                    vibesql_ast::TruthValue::False => matches!(
                        val,
                        SqlValue::Boolean(false)
                            | SqlValue::Integer(0)
                            | SqlValue::Bigint(0)
                            | SqlValue::Smallint(0)
                    ),
                    vibesql_ast::TruthValue::Unknown => matches!(val, SqlValue::Null),
                };
                let final_result = if *negated { !result } else { result };
                Ok(SqlValue::Boolean(final_result))
            }

            vibesql_ast::Expression::WindowFunction { function, .. } => {
                // Extract function name for SQLite-compatible error message
                // Window functions in WHERE, GROUP BY, or HAVING clauses are misuse
                let function_name = match function {
                    vibesql_ast::WindowFunctionSpec::Aggregate { name, .. }
                    | vibesql_ast::WindowFunctionSpec::Ranking { name, .. }
                    | vibesql_ast::WindowFunctionSpec::Value { name, .. } => name.to_string(),
                };
                Err(ExecutorError::MisuseOfWindowFunction { function_name })
            }

            vibesql_ast::Expression::AggregateFunction { name, .. } => {
                // SQLite-compatible error message for aggregate misuse in execution context
                // This error occurs when an aggregate is evaluated outside of aggregation,
                // such as in ORDER BY clauses of non-aggregate queries.
                // Uses "misuse of aggregate: X()" format (with colon) to match SQLite's expr.c
                Err(ExecutorError::MisuseOfAggregateContext { function_name: name.to_string() })
            }

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

            // Row value constructor - not supported in regular evaluation context
            vibesql_ast::Expression::RowValueConstructor(_) => Err(ExecutorError::UnsupportedExpression(
                "Row value constructors are not supported in this context".to_string(),
            )),

            // COLLATE expression - evaluate inner expression (collation affects string comparison)
            // TODO: Full collation support - for now just evaluate the inner expression
            vibesql_ast::Expression::Collate { expr, .. } => self.eval(expr, row),
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
            match self.eval_column_ref(None, None, column_name, row) {
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
        schema_qualifier: Option<&str>,
        table_qualifier: Option<&str>,
        column: &str,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Handle schema qualifier (three-part names like schema.table.column)
        // SQLite schemas: "main" (default), "temp" (temporary tables), or attached database names
        // For our single-database implementation:
        // - "main" is silently accepted (treated as default)
        // - Other schemas return "no such column" error to match SQLite behavior
        if let Some(schema) = schema_qualifier {
            let schema_lower = schema.to_lowercase();
            if schema_lower != "main" {
                // SQLite returns "no such column: schema.table.column" for unknown schemas
                return Err(ExecutorError::ColumnNotFound {
                    column_name: format!("{}.{}.{}", schema, table_qualifier.unwrap_or(""), column),
                    table_name: self.schema.name.clone(),
                    searched_tables: vec![self.schema.name.clone()],
                    available_columns: self.schema.columns.iter().map(|c| c.name.clone()).collect(),
                });
            }
            // "main" schema - continue with normal resolution
        }

        // Special case: "*" is a wildcard used in COUNT(*) and is not a real column
        // Return NULL here - the actual COUNT(*) logic handles this specially
        if column == "*" {
            return Ok(vibesql_types::SqlValue::Null);
        }

        // SQLite compatibility: Handle ROWID pseudo-column
        // ROWID, _rowid_, and oid are aliases that return the row's unique identifier
        // Note: We check real columns first - real columns take precedence over ROWID
        let column_lower = column.to_lowercase();
        if column_lower == "rowid" || column_lower == "_rowid_" || column_lower == "oid" {
            // First check if schema has a real column with this name
            if self.schema.get_column_index(column).is_none() {
                // Issue #4536: Check for INTEGER PRIMARY KEY alias column
                // If the table has an INTEGER PRIMARY KEY, it acts as an alias for rowid.
                // The column's value IS the rowid, so return that column's value.
                if let Some(ipk_col_idx) = self.schema.rowid_alias_column {
                    return row
                        .get(ipk_col_idx)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: ipk_col_idx });
                }

                // Use get_row_id_for_table to handle both single-table and multi-table (JOIN) rows
                // This fixes issue #4370 where qualified ROWIDs like `t1.rowid` returned NULL in JOINs
                if let Some(row_id) = row.get_row_id_for_table(table_qualifier) {
                    return Ok(vibesql_types::SqlValue::Bigint(row_id as i64));
                }
                // Fall back to evaluator's row_index (for older code paths)
                if let Some(row_index) = self.row_index {
                    return Ok(vibesql_types::SqlValue::Bigint(row_index as i64));
                }
                // ROWID not available - this happens for derived rows without ROWID tracking
                // Return NULL in this case (matching SQLite behavior for derived tables)
                return Ok(vibesql_types::SqlValue::Null);
            }
        }

        // Check procedural context first (variables/parameters take precedence over table columns)
        // This is only checked when there's no table qualifier, as variables don't have table
        // prefixes
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

            // Check if qualifier matches the table alias (SQLite extension: UPDATE t1 AS xyz)
            let alias_lower = self.table_alias.as_ref().map(|a| a.to_lowercase());
            let matches_alias = alias_lower.as_ref().is_some_and(|a| a == &qualifier_lower);

            // Check if qualifier matches inner schema or table alias
            if qualifier_lower == inner_name_lower || matches_alias {
                // Qualifier matches inner schema or alias - search only there
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
                    if let Some(ref alias) = self.table_alias {
                        known_tables.push(alias.clone());
                    }
                    known_tables.push(outer_schema.name.clone());

                    return Err(ExecutorError::InvalidTableQualifier {
                        qualifier: qualifier.to_string(),
                        column: column.to_string(),
                        available_tables: known_tables,
                    });
                }
            } else {
                // No outer schema and qualifier doesn't match inner schema
                let mut known_tables = vec![self.schema.name.clone()];
                if let Some(ref alias) = self.table_alias {
                    known_tables.push(alias.clone());
                }
                return Err(ExecutorError::InvalidTableQualifier {
                    qualifier: qualifier.to_string(),
                    column: column.to_string(),
                    available_tables: known_tables,
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
