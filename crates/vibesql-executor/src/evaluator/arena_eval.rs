//! Arena-aware expression evaluation for prepared statements.
//!
//! This module provides evaluation of arena-allocated expressions with inline
//! placeholder resolution, eliminating the need to convert arena AST to owned
//! AST before evaluation.
//!
//! # Key Benefits
//!
//! - **Zero conversion overhead**: Arena expressions are evaluated directly
//! - **Inline placeholder resolution**: Placeholders are resolved from params during eval
//! - **Near-zero allocation**: Only the final result values are heap-allocated
//!
//! # Usage
//!
//! ```ignore
//! let evaluator = ArenaExpressionEvaluator::new(schema, &params, interner);
//! let result = evaluator.eval(expr, row)?;
//! ```

use vibesql_ast::arena::{ArenaInterner, Expression, ExtendedExpr, Symbol};
use vibesql_ast::BinaryOperator;
use vibesql_catalog::TableSchema;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Evaluates arena-allocated expressions with inline placeholder resolution.
///
/// This evaluator is designed for prepared statement execution where:
/// 1. The statement is stored as arena-allocated AST
/// 2. Parameters are provided at execution time
/// 3. Placeholders are resolved inline during evaluation (no AST cloning)
pub struct ArenaExpressionEvaluator<'a, 'arena, 'params> {
    /// Schema of the current table
    schema: &'a TableSchema,
    /// Bound parameter values for placeholder resolution
    params: &'params [SqlValue],
    /// Database reference for subqueries
    database: Option<&'a Database>,
    /// Current depth in expression tree (stack overflow protection)
    depth: usize,
    /// Interner for resolving symbols to strings
    interner: &'arena ArenaInterner<'arena>,
}

impl<'a, 'arena, 'params> ArenaExpressionEvaluator<'a, 'arena, 'params> {
    /// Create a new arena expression evaluator.
    pub fn new(
        schema: &'a TableSchema,
        params: &'params [SqlValue],
        interner: &'arena ArenaInterner<'arena>,
    ) -> Self {
        Self {
            schema,
            params,
            database: None,
            depth: 0,
            interner,
        }
    }

    /// Create an evaluator with database reference for subqueries.
    pub fn with_database(
        schema: &'a TableSchema,
        params: &'params [SqlValue],
        database: &'a Database,
        interner: &'arena ArenaInterner<'arena>,
    ) -> Self {
        Self {
            schema,
            params,
            database: Some(database),
            depth: 0,
            interner,
        }
    }

    /// Resolve a symbol to its string value.
    #[inline]
    fn resolve(&self, symbol: Symbol) -> &'arena str {
        self.interner.resolve(symbol)
    }

    /// Evaluate an arena expression in the context of a row.
    ///
    /// This method handles all expression types including placeholders,
    /// which are resolved inline from the params slice.
    #[inline]
    pub fn eval(
        &self,
        expr: &Expression<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        // Check depth limit
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        self.eval_impl(expr, row)
    }

    /// Internal implementation of eval.
    fn eval_impl(
        &self,
        expr: &Expression<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        match expr {
            // Literals - just return the value
            Expression::Literal(val) => Ok(val.clone()),

            // PLACEHOLDER RESOLUTION - the key benefit of arena evaluation
            // Instead of returning an error, resolve from params slice
            Expression::Placeholder(idx) => {
                if *idx < self.params.len() {
                    Ok(self.params[*idx].clone())
                } else {
                    Err(ExecutorError::UnsupportedExpression(format!(
                        "Placeholder index {} out of bounds (params len: {})",
                        idx,
                        self.params.len()
                    )))
                }
            }

            // Numbered placeholders ($1, $2, etc.) - 1-indexed
            Expression::NumberedPlaceholder(idx) => {
                let array_idx = idx.saturating_sub(1);
                if array_idx < self.params.len() {
                    Ok(self.params[array_idx].clone())
                } else {
                    Err(ExecutorError::UnsupportedExpression(format!(
                        "Numbered placeholder ${} out of bounds (params len: {})",
                        idx,
                        self.params.len()
                    )))
                }
            }

            // Named placeholders - not supported in positional binding
            Expression::NamedPlaceholder(name) => Err(ExecutorError::UnsupportedExpression(
                format!("Named placeholder :{} requires named binding", self.resolve(*name)),
            )),

            // Column reference
            Expression::ColumnRef { table, column } => {
                let table_str = table.map(|t| self.resolve(t));
                let column_str = self.resolve(*column);
                self.eval_column_ref(table_str, column_str, row)
            }

            // Binary operations (for non-AND/OR or legacy ASTs)
            Expression::BinaryOp { left, op, right } => {
                // Short-circuit evaluation for AND/OR
                match op {
                    BinaryOperator::And => {
                        let left_val = self.with_depth().eval(left, row)?;
                        match left_val {
                            SqlValue::Boolean(false) => Ok(SqlValue::Boolean(false)),
                            _ => {
                                let right_val = self.with_depth().eval(right, row)?;
                                // NULL AND FALSE = FALSE
                                if matches!(left_val, SqlValue::Null)
                                    && matches!(right_val, SqlValue::Boolean(false))
                                {
                                    return Ok(SqlValue::Boolean(false));
                                }
                                self.eval_binary_op(&left_val, op, &right_val)
                            }
                        }
                    }
                    BinaryOperator::Or => {
                        let left_val = self.with_depth().eval(left, row)?;
                        match left_val {
                            SqlValue::Boolean(true) => Ok(SqlValue::Boolean(true)),
                            _ => {
                                let right_val = self.with_depth().eval(right, row)?;
                                // NULL OR TRUE = TRUE
                                if matches!(left_val, SqlValue::Null)
                                    && matches!(right_val, SqlValue::Boolean(true))
                                {
                                    return Ok(SqlValue::Boolean(true));
                                }
                                self.eval_binary_op(&left_val, op, &right_val)
                            }
                        }
                    }
                    _ => {
                        let left_val = self.with_depth().eval(left, row)?;
                        let right_val = self.with_depth().eval(right, row)?;
                        self.eval_binary_op(&left_val, op, &right_val)
                    }
                }
            }

            // Flattened conjunction (AND chain) with short-circuit evaluation
            Expression::Conjunction(terms) => {
                let mut has_null = false;
                for term in terms.iter() {
                    let val = self.with_depth().eval(term, row)?;
                    match val {
                        SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                        SqlValue::Boolean(true) => {}
                        SqlValue::Null => has_null = true,
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(
                                "AND operand must evaluate to BOOLEAN".to_string(),
                            ))
                        }
                    }
                }
                // If any term was NULL and none were FALSE, result is NULL
                if has_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(true))
                }
            }

            // Flattened disjunction (OR chain) with short-circuit evaluation
            Expression::Disjunction(terms) => {
                let mut has_null = false;
                for term in terms.iter() {
                    let val = self.with_depth().eval(term, row)?;
                    match val {
                        SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                        SqlValue::Boolean(false) => {}
                        SqlValue::Null => has_null = true,
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(
                                "OR operand must evaluate to BOOLEAN".to_string(),
                            ))
                        }
                    }
                }
                // If any term was NULL and none were TRUE, result is NULL
                if has_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(false))
                }
            }

            // Unary operations
            Expression::UnaryOp { op, expr: inner } => {
                let val = self.with_depth().eval(inner, row)?;
                crate::evaluator::eval_unary_op(op, &val)
            }

            // IS NULL / IS NOT NULL
            Expression::IsNull { expr: inner, negated } => {
                let value = self.with_depth().eval(inner, row)?;
                let is_null = matches!(value, SqlValue::Null);
                let result = if *negated { !is_null } else { is_null };
                Ok(SqlValue::Boolean(result))
            }

            // Current date/time
            Expression::CurrentDate => {
                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                super::functions::eval_scalar_function("CURRENT_DATE", &[], &None, &sql_mode)
            }

            Expression::CurrentTime { .. } => {
                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                super::functions::eval_scalar_function("CURRENT_TIME", &[], &None, &sql_mode)
            }

            Expression::CurrentTimestamp { .. } => {
                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                super::functions::eval_scalar_function("CURRENT_TIMESTAMP", &[], &None, &sql_mode)
            }

            // Wildcard not supported in expressions
            Expression::Wildcard => Err(ExecutorError::UnsupportedExpression(
                "Wildcard (*) not supported in expressions".to_string(),
            )),

            // DEFAULT keyword
            Expression::Default => Err(ExecutorError::UnsupportedExpression(
                "DEFAULT keyword is only valid in INSERT VALUES and UPDATE SET clauses".to_string(),
            )),

            // Cold-path extended variants
            Expression::Extended(ext) => self.eval_extended(ext, row),
        }
    }

    /// Evaluate an extended expression (cold path variants).
    fn eval_extended(
        &self,
        ext: &ExtendedExpr<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        match ext {
            // IN list
            ExtendedExpr::InList {
                expr: inner,
                values,
                negated,
            } => {
                let val = self.with_depth().eval(inner, row)?;

                // Check if value matches any in list
                for list_val_expr in values.iter() {
                    let list_val = self.with_depth().eval(list_val_expr, row)?;
                    if super::core::values_are_equal(&val, &list_val) {
                        return Ok(SqlValue::Boolean(!*negated));
                    }
                }

                // Value not found in list
                Ok(SqlValue::Boolean(*negated))
            }

            // BETWEEN predicate
            ExtendedExpr::Between {
                expr: inner,
                low,
                high,
                negated,
                symmetric,
            } => {
                let expr_val = self.with_depth().eval(inner, row)?;
                let low_val = self.with_depth().eval(low, row)?;
                let high_val = self.with_depth().eval(high, row)?;

                let sql_mode = self
                    .database
                    .map(|db| db.sql_mode())
                    .unwrap_or_default();

                super::core::eval_between_static(
                    &expr_val, &low_val, &high_val, *negated, *symmetric, sql_mode,
                )
            }

            // LIKE pattern matching
            ExtendedExpr::Like {
                expr: inner,
                pattern,
                negated,
            } => {
                let val = self.with_depth().eval(inner, row)?;
                let pattern_val = self.with_depth().eval(pattern, row)?;

                let val_str = match &val {
                    SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
                    SqlValue::Null => return Ok(SqlValue::Null),
                    _ => return Ok(SqlValue::Boolean(*negated)),
                };

                let pattern_str = match &pattern_val {
                    SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
                    SqlValue::Null => return Ok(SqlValue::Null),
                    _ => return Ok(SqlValue::Boolean(*negated)),
                };

                let matches = super::pattern::like_match(val_str, pattern_str);
                Ok(SqlValue::Boolean(if *negated { !matches } else { matches }))
            }

            // CAST expression
            ExtendedExpr::Cast { expr: inner, data_type } => {
                let val = self.with_depth().eval(inner, row)?;
                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
                super::casting::cast_value(&val, data_type, &sql_mode)
            }

            // Function calls
            ExtendedExpr::Function { name, args, character_unit } => {
                // Evaluate all arguments
                let mut arg_values = Vec::with_capacity(args.len());
                for arg in args.iter() {
                    arg_values.push(self.with_depth().eval(arg, row)?);
                }

                let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();

                // Convert character_unit to the owned type
                let owned_char_unit = character_unit.map(|cu| match cu {
                    vibesql_ast::arena::CharacterUnit::Characters => {
                        vibesql_ast::CharacterUnit::Characters
                    }
                    vibesql_ast::arena::CharacterUnit::Octets => vibesql_ast::CharacterUnit::Octets,
                });

                let name_str = self.resolve(*name);
                super::functions::eval_scalar_function(name_str, &arg_values, &owned_char_unit, &sql_mode)
            }

            // CASE expression
            ExtendedExpr::Case {
                operand,
                when_clauses,
                else_result,
            } => self.eval_case(operand, when_clauses, else_result, row),

            // Aggregate functions need special handling
            ExtendedExpr::AggregateFunction { .. } => Err(ExecutorError::UnsupportedExpression(
                "Aggregate functions should be evaluated in aggregation context".to_string(),
            )),

            // Window functions need special handling
            ExtendedExpr::WindowFunction { .. } => Err(ExecutorError::UnsupportedExpression(
                "Window functions should be evaluated separately".to_string(),
            )),

            // Subqueries - need full executor context
            ExtendedExpr::ScalarSubquery(_) => Err(ExecutorError::UnsupportedExpression(
                "Scalar subqueries not yet supported in arena evaluation".to_string(),
            )),

            ExtendedExpr::In { .. } => Err(ExecutorError::UnsupportedExpression(
                "IN subqueries not yet supported in arena evaluation".to_string(),
            )),

            ExtendedExpr::Exists { .. } => Err(ExecutorError::UnsupportedExpression(
                "EXISTS subqueries not yet supported in arena evaluation".to_string(),
            )),

            ExtendedExpr::QuantifiedComparison { .. } => Err(ExecutorError::UnsupportedExpression(
                "Quantified comparisons not yet supported in arena evaluation".to_string(),
            )),

            // Other unsupported expressions
            ExtendedExpr::Position { .. }
            | ExtendedExpr::Trim { .. }
            | ExtendedExpr::Extract { .. }
            | ExtendedExpr::Interval { .. }
            | ExtendedExpr::DuplicateKeyValue { .. }
            | ExtendedExpr::NextValue { .. }
            | ExtendedExpr::MatchAgainst { .. }
            | ExtendedExpr::PseudoVariable { .. }
            | ExtendedExpr::SessionVariable { .. } => Err(ExecutorError::UnsupportedExpression(
                "Expression type not yet supported in arena evaluation".to_string(),
            )),
        }
    }

    /// Evaluate a binary operation.
    fn eval_binary_op(
        &self,
        left: &SqlValue,
        op: &BinaryOperator,
        right: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
        super::core::eval_binary_op_static(left, op, right, sql_mode)
    }

    /// Evaluate column reference.
    fn eval_column_ref(
        &self,
        _table_qualifier: Option<&str>,
        column: &str,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        // Special case: "*" wildcard
        if column == "*" {
            return Ok(SqlValue::Null);
        }

        // Look up column in schema
        if let Some(col_index) = self.schema.get_column_index(column) {
            row.get(col_index)
                .cloned()
                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index })
        } else {
            Err(ExecutorError::ColumnNotFound {
                column_name: column.to_string(),
                table_name: self.schema.name.clone(),
                searched_tables: vec![self.schema.name.clone()],
                available_columns: self.schema.columns.iter().map(|c| c.name.clone()).collect(),
            })
        }
    }

    /// Evaluate a CASE expression.
    fn eval_case(
        &self,
        operand: &Option<&'arena Expression<'arena>>,
        when_clauses: &bumpalo::collections::Vec<'arena, vibesql_ast::arena::CaseWhen<'arena>>,
        else_result: &Option<&'arena Expression<'arena>>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        // Simple CASE vs searched CASE
        if let Some(operand_expr) = operand {
            // Simple CASE: CASE expr WHEN val1 THEN ... WHEN val2 THEN ... END
            let operand_val = self.with_depth().eval(operand_expr, row)?;

            for when in when_clauses.iter() {
                for condition in when.conditions.iter() {
                    let when_val = self.with_depth().eval(condition, row)?;
                    if super::core::values_are_equal(&operand_val, &when_val) {
                        return self.with_depth().eval(&when.result, row);
                    }
                }
            }
        } else {
            // Searched CASE: CASE WHEN cond1 THEN ... WHEN cond2 THEN ... END
            for when in when_clauses.iter() {
                for condition in when.conditions.iter() {
                    let cond_val = self.with_depth().eval(condition, row)?;
                    if matches!(cond_val, SqlValue::Boolean(true)) {
                        return self.with_depth().eval(&when.result, row);
                    }
                }
            }
        }

        // No match - return ELSE or NULL
        if let Some(else_expr) = else_result {
            self.with_depth().eval(else_expr, row)
        } else {
            Ok(SqlValue::Null)
        }
    }

    /// Create a new evaluator with incremented depth.
    fn with_depth(&self) -> Self {
        Self {
            schema: self.schema,
            params: self.params,
            database: self.database,
            depth: self.depth + 1,
            interner: self.interner,
        }
    }
}

/// Evaluate a WHERE clause from an arena statement.
///
/// Returns true if the row passes the filter.
pub fn eval_arena_where_clause<'arena>(
    where_clause: Option<&Expression<'arena>>,
    schema: &TableSchema,
    params: &[SqlValue],
    row: &Row,
    database: Option<&Database>,
    interner: &'arena ArenaInterner<'arena>,
) -> Result<bool, ExecutorError> {
    let Some(expr) = where_clause else {
        return Ok(true); // No WHERE clause = all rows pass
    };

    let evaluator = if let Some(db) = database {
        ArenaExpressionEvaluator::with_database(schema, params, db, interner)
    } else {
        ArenaExpressionEvaluator::new(schema, params, interner)
    };

    match evaluator.eval(expr, row)? {
        SqlValue::Boolean(b) => Ok(b),
        SqlValue::Null => Ok(false), // NULL in WHERE means row doesn't match
        other => Err(ExecutorError::UnsupportedExpression(format!(
            "WHERE clause must evaluate to BOOLEAN, got {:?}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bumpalo::Bump;
    use vibesql_ast::arena::{ArenaInterner, Expression, ExtendedExpr};
    use vibesql_catalog::ColumnSchema;
    use vibesql_types::DataType;

    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            "test".to_string(),
            vec![
                ColumnSchema::new("ID".to_string(), DataType::Integer, false),
                ColumnSchema::new("NAME".to_string(), DataType::Varchar { max_length: Some(100) }, true),
            ],
        )
    }

    fn create_test_row(id: i64, name: Option<&str>) -> Row {
        Row::new(vec![
            SqlValue::Integer(id),
            name.map(|s| SqlValue::Varchar(s.to_string())).unwrap_or(SqlValue::Null),
        ])
    }

    #[test]
    fn test_literal_evaluation() {
        let arena = Bump::new();
        let interner = ArenaInterner::new(&arena);
        let schema = create_test_schema();
        let params = vec![];
        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);

        let row = create_test_row(1, Some("test"));

        let expr = Expression::Literal(SqlValue::Integer(42));
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_placeholder_resolution() {
        let arena = Bump::new();
        let interner = ArenaInterner::new(&arena);
        let schema = create_test_schema();
        let params = vec![SqlValue::Integer(42), SqlValue::Varchar("hello".to_string())];
        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);

        let row = create_test_row(1, Some("test"));

        // Test placeholder 0
        let expr = Expression::Placeholder(0);
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));

        // Test placeholder 1
        let expr = Expression::Placeholder(1);
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Varchar("hello".to_string()));
    }

    #[test]
    fn test_numbered_placeholder_resolution() {
        let arena = Bump::new();
        let interner = ArenaInterner::new(&arena);
        let schema = create_test_schema();
        let params = vec![SqlValue::Integer(42), SqlValue::Varchar("hello".to_string())];
        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);

        let row = create_test_row(1, Some("test"));

        // Test $1 (maps to params[0])
        let expr = Expression::NumberedPlaceholder(1);
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));

        // Test $2 (maps to params[1])
        let expr = Expression::NumberedPlaceholder(2);
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Varchar("hello".to_string()));
    }

    #[test]
    fn test_column_ref_evaluation() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);
        let schema = create_test_schema();
        let params = vec![];

        // Intern the column name
        let id_sym = interner.intern("ID");

        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);

        let row = create_test_row(1, Some("test"));

        // Test column reference - using interned symbol
        let expr = Expression::ColumnRef {
            table: None,
            column: id_sym,
        };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(1));
    }

    #[test]
    fn test_binary_op_with_placeholder() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);
        let schema = create_test_schema();
        let params = vec![SqlValue::Integer(1)];

        // Intern the column name
        let id_sym = interner.intern("ID");

        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);

        let row = create_test_row(1, Some("test"));

        // Create: id = ?
        let col_ref = arena.alloc(Expression::ColumnRef {
            table: None,
            column: id_sym,
        });
        let placeholder = arena.alloc(Expression::Placeholder(0));

        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: col_ref,
            right: placeholder,
        };

        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_is_null() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);
        let schema = create_test_schema();
        let params = vec![];

        // Intern the column name
        let name_sym = interner.intern("NAME");

        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);

        let row = create_test_row(1, None);

        // name IS NULL (name is at index 1 which is NULL)
        let col_ref = arena.alloc(Expression::ColumnRef {
            table: None,
            column: name_sym,
        });

        let expr = Expression::IsNull {
            expr: col_ref,
            negated: false,
        };

        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }

    #[test]
    fn test_in_list_with_placeholders() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);
        let schema = create_test_schema();
        let params = vec![
            SqlValue::Integer(1),
            SqlValue::Integer(2),
            SqlValue::Integer(3),
        ];

        // Intern the column name
        let id_sym = interner.intern("ID");

        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);

        let row = create_test_row(2, Some("test"));

        // Create: id IN (?, ?, ?)
        let col_ref = arena.alloc(Expression::ColumnRef {
            table: None,
            column: id_sym,
        });

        let mut values = bumpalo::collections::Vec::new_in(&arena);
        values.push(Expression::Placeholder(0));
        values.push(Expression::Placeholder(1));
        values.push(Expression::Placeholder(2));

        let expr = Expression::Extended(arena.alloc(ExtendedExpr::InList {
            expr: col_ref,
            values,
            negated: false,
        }));

        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));
    }
}
