//! Arena-based expression evaluator for zero-allocation prepared statement execution.
//!
//! This module provides `ArenaExpressionEvaluator` which evaluates arena-allocated
//! expressions (`vibesql_ast::arena::Expression`) with inline placeholder resolution.
//!
//! # Performance
//!
//! Unlike the regular evaluator that works with owned AST types, this evaluator:
//! - Works directly with arena-allocated AST references
//! - Resolves placeholders inline without intermediate allocations
//! - Avoids cloning expressions during evaluation
//!
//! # Usage
//!
//! ```text
//! let params = &[SqlValue::Integer(42), SqlValue::Varchar(arcstr::ArcStr::from("hello"))];
//! let evaluator = ArenaExpressionEvaluator::new(schema, params);
//! let result = evaluator.eval(&arena_expr, &row)?;
//! ```

use std::cell::RefCell;
use std::collections::HashMap;

use ahash::AHasher;
use std::hash::{Hash, Hasher};
use vibesql_ast::arena::{
    self as arena_ast, ArenaInterner, Expression as ArenaExpression,
    ExtendedExpr as ArenaExtendedExpr, Symbol,
};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;

/// Maximum expression evaluation depth to prevent stack overflow.
const MAX_ARENA_EXPRESSION_DEPTH: usize = 128;

/// Arena-based expression evaluator for prepared statement execution.
///
/// This evaluator works with arena-allocated expressions and resolves
/// placeholders inline from a provided parameters slice.
pub struct ArenaExpressionEvaluator<'a, 'arena> {
    /// Combined schema for column resolution
    schema: &'a CombinedSchema,
    /// Parameters for placeholder resolution
    params: &'a [SqlValue],
    /// Database reference for subquery execution
    database: Option<&'a vibesql_storage::Database>,
    /// SQL mode for operator semantics
    sql_mode: vibesql_types::SqlMode,
    /// Cache for column lookups to avoid repeated schema traversals
    column_cache: RefCell<HashMap<u64, usize>>,
    /// Current depth in expression tree (for preventing stack overflow)
    depth: usize,
    /// Interner for resolving symbols to strings
    interner: &'arena ArenaInterner<'arena>,
}

impl<'a, 'arena> ArenaExpressionEvaluator<'a, 'arena> {
    /// Create a new arena expression evaluator.
    ///
    /// # Arguments
    ///
    /// * `schema` - Combined schema for column resolution
    /// * `params` - Slice of parameter values for placeholder resolution
    /// * `interner` - Interner for resolving symbols to strings
    pub fn new(
        schema: &'a CombinedSchema,
        params: &'a [SqlValue],
        interner: &'arena ArenaInterner<'arena>,
    ) -> Self {
        ArenaExpressionEvaluator {
            schema,
            params,
            database: None,
            sql_mode: vibesql_types::SqlMode::default(),
            column_cache: RefCell::new(HashMap::new()),
            depth: 0,
            interner,
        }
    }

    /// Create a new arena expression evaluator with database reference.
    ///
    /// # Arguments
    ///
    /// * `schema` - Combined schema for column resolution
    /// * `params` - Slice of parameter values for placeholder resolution
    /// * `database` - Database reference for subquery execution
    /// * `interner` - Interner for resolving symbols to strings
    pub fn with_database(
        schema: &'a CombinedSchema,
        params: &'a [SqlValue],
        database: &'a vibesql_storage::Database,
        interner: &'arena ArenaInterner<'arena>,
    ) -> Self {
        ArenaExpressionEvaluator {
            schema,
            params,
            database: Some(database),
            sql_mode: database.sql_mode(),
            column_cache: RefCell::new(HashMap::new()),
            depth: 0,
            interner,
        }
    }

    /// Resolve a symbol to its string value.
    #[inline]
    fn resolve(&self, symbol: Symbol) -> &'arena str {
        self.interner.resolve(symbol)
    }

    /// Evaluate an arena-allocated expression against a row.
    ///
    /// # Arguments
    ///
    /// * `expr` - Arena-allocated expression to evaluate
    /// * `row` - Row data for column resolution
    ///
    /// # Returns
    ///
    /// The evaluated SqlValue result.
    pub fn eval(
        &self,
        expr: &ArenaExpression<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= MAX_ARENA_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: MAX_ARENA_EXPRESSION_DEPTH,
            });
        }

        self.eval_impl(expr, row)
    }

    /// Internal evaluation implementation.
    fn eval_impl(
        &self,
        expr: &ArenaExpression<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        match expr {
            // Literals - direct return without allocation
            ArenaExpression::Literal(val) => Ok(val.clone()),

            // Placeholder - inline resolution from params slice
            ArenaExpression::Placeholder(idx) => self.params.get(*idx).cloned().ok_or_else(|| {
                ExecutorError::UnsupportedExpression(format!(
                    "Parameter index {} out of bounds (available: {})",
                    idx,
                    self.params.len()
                ))
            }),

            // Numbered placeholder ($1, $2, etc.) - 1-indexed
            ArenaExpression::NumberedPlaceholder(num) => {
                let idx = num.saturating_sub(1);
                self.params.get(idx).cloned().ok_or_else(|| {
                    ExecutorError::UnsupportedExpression(format!(
                        "Parameter ${} out of bounds (available: {})",
                        num,
                        self.params.len()
                    ))
                })
            }

            // Named placeholder - not supported in this evaluator
            ArenaExpression::NamedPlaceholder(name) => {
                Err(ExecutorError::UnsupportedExpression(format!(
                    "Named placeholder '{}' not supported in arena evaluator",
                    self.resolve(*name)
                )))
            }

            // Column reference
            ArenaExpression::ColumnRef { table, column } => {
                let column_str = self.resolve(*column);
                let table_str = table.map(|t| self.resolve(t));

                // Special case: "*" is a wildcard used in COUNT(*)
                if column_str == "*" {
                    return Ok(SqlValue::Null);
                }

                if let Some(col_index) = self.get_column_index_cached(table_str, column_str) {
                    row.get(col_index)
                        .cloned()
                        .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_index })
                } else {
                    Err(ExecutorError::ColumnNotFound {
                        column_name: column_str.to_string(),
                        table_name: table_str
                            .map(|t| t.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        searched_tables: self.schema.table_names(),
                        available_columns: self.get_available_columns(),
                    })
                }
            }

            // Binary operation
            ArenaExpression::BinaryOp { op, left, right } => {
                self.eval_binary_op(*op, left, right, row)
            }

            // Unary operation
            ArenaExpression::UnaryOp { op, expr: inner } => {
                let val = self.eval_with_depth(inner, row)?;
                super::expressions::operators::eval_unary_op(op, &val)
            }

            // IS NULL / IS NOT NULL
            ArenaExpression::IsNull { expr: inner, negated } => {
                let val = self.eval_with_depth(inner, row)?;
                let is_null = matches!(val, SqlValue::Null);
                Ok(SqlValue::Boolean(if *negated { !is_null } else { is_null }))
            }

            // Wildcard (*)
            ArenaExpression::Wildcard => Ok(SqlValue::Null),

            // Current date/time functions - use scalar function path
            ArenaExpression::CurrentDate => {
                super::functions::eval_scalar_function("CURRENT_DATE", &[], &None, &self.sql_mode)
            }
            ArenaExpression::CurrentTime { .. } => {
                super::functions::eval_scalar_function("CURRENT_TIME", &[], &None, &self.sql_mode)
            }
            ArenaExpression::CurrentTimestamp { .. } => super::functions::eval_scalar_function(
                "CURRENT_TIMESTAMP",
                &[],
                &None,
                &self.sql_mode,
            ),

            // DEFAULT keyword
            ArenaExpression::Default => Err(ExecutorError::UnsupportedExpression(
                "DEFAULT keyword is only valid in INSERT VALUES and UPDATE SET clauses".to_string(),
            )),

            // Conjunction and Disjunction - evaluate children
            ArenaExpression::Conjunction(children) => {
                let mut result = SqlValue::Boolean(true);
                for child in children.iter() {
                    let val = self.eval_with_depth(child, row)?;
                    match val {
                        SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                        SqlValue::Null => result = SqlValue::Null,
                        SqlValue::Boolean(true) => {}
                        _ => {
                            return Err(ExecutorError::TypeError(format!(
                                "Conjunction requires boolean operands, got {:?}",
                                val
                            )))
                        }
                    }
                }
                Ok(result)
            }

            ArenaExpression::Disjunction(children) => {
                let mut result = SqlValue::Boolean(false);
                for child in children.iter() {
                    let val = self.eval_with_depth(child, row)?;
                    match val {
                        SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                        SqlValue::Null => result = SqlValue::Null,
                        SqlValue::Boolean(false) => {}
                        _ => {
                            return Err(ExecutorError::TypeError(format!(
                                "Disjunction requires boolean operands, got {:?}",
                                val
                            )))
                        }
                    }
                }
                Ok(result)
            }

            // Cold-path extended variants
            ArenaExpression::Extended(ext) => self.eval_extended(ext, row),
        }
    }

    /// Evaluate an extended expression (cold path variants).
    fn eval_extended(
        &self,
        ext: &ArenaExtendedExpr<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        match ext {
            // Function call
            ArenaExtendedExpr::Function { name, args, character_unit } => {
                let evaluated_args: Result<Vec<SqlValue>, _> =
                    args.iter().map(|arg| self.eval_with_depth(arg, row)).collect();
                let char_unit = character_unit.as_ref().map(|cu| match cu {
                    arena_ast::CharacterUnit::Characters => vibesql_ast::CharacterUnit::Characters,
                    arena_ast::CharacterUnit::Octets => vibesql_ast::CharacterUnit::Octets,
                });
                let name_str = self.resolve(*name);
                super::functions::eval_scalar_function(
                    name_str,
                    &evaluated_args?,
                    &char_unit,
                    &self.sql_mode,
                )
            }

            // Aggregate function - should be pre-computed
            ArenaExtendedExpr::AggregateFunction { name, .. } => {
                Err(ExecutorError::UnsupportedExpression(format!(
                    "Aggregate function '{}' must be pre-computed before arena evaluation",
                    self.resolve(*name)
                )))
            }

            // CASE expression
            ArenaExtendedExpr::Case { operand, when_clauses, else_result } => {
                self.eval_case(operand.as_deref(), when_clauses, else_result.as_deref(), row)
            }

            // BETWEEN predicate
            ArenaExtendedExpr::Between { expr: inner, low, high, negated, symmetric } => {
                let val = self.eval_with_depth(inner, row)?;
                let low_val = self.eval_with_depth(low, row)?;
                let high_val = self.eval_with_depth(high, row)?;
                super::core::eval_between_static(
                    &val,
                    &low_val,
                    &high_val,
                    *negated,
                    *symmetric,
                    self.sql_mode.clone(),
                )
            }

            // IN list
            ArenaExtendedExpr::InList { expr: inner, values, negated } => {
                let val = self.eval_with_depth(inner, row)?;
                if matches!(val, SqlValue::Null) {
                    return Ok(SqlValue::Null);
                }
                let mut found = false;
                let mut has_null = false;
                for list_val in values.iter() {
                    let v = self.eval_with_depth(list_val, row)?;
                    if matches!(v, SqlValue::Null) {
                        has_null = true;
                        continue;
                    }
                    let eq = super::core::eval_binary_op_static(
                        &val,
                        &vibesql_ast::BinaryOperator::Equal,
                        &v,
                        self.sql_mode.clone(),
                    )?;
                    if matches!(eq, SqlValue::Boolean(true)) {
                        found = true;
                        break;
                    }
                }
                if found {
                    Ok(SqlValue::Boolean(!*negated))
                } else if has_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(*negated))
                }
            }

            // LIKE pattern matching
            ArenaExtendedExpr::Like { expr: inner, pattern, negated } => {
                let val = self.eval_with_depth(inner, row)?;
                let pattern_val = self.eval_with_depth(pattern, row)?;
                self.eval_like(&val, &pattern_val, *negated)
            }

            // CAST expression - delegate to casting module
            ArenaExtendedExpr::Cast { expr: inner, data_type } => {
                let val = self.eval_with_depth(inner, row)?;
                super::casting::cast_value(&val, data_type, &self.sql_mode)
            }

            // Subqueries - not supported without conversion to owned types
            ArenaExtendedExpr::ScalarSubquery(_)
            | ArenaExtendedExpr::In { .. }
            | ArenaExtendedExpr::Exists { .. }
            | ArenaExtendedExpr::QuantifiedComparison { .. } => {
                Err(ExecutorError::UnsupportedExpression(
                    "Subqueries in arena expressions require conversion to owned types".to_string(),
                ))
            }

            // Window function - should be pre-computed
            ArenaExtendedExpr::WindowFunction { .. } => Err(ExecutorError::UnsupportedExpression(
                "Window functions must be pre-computed before arena evaluation".to_string(),
            )),

            // POSITION function
            ArenaExtendedExpr::Position { substring, string, .. } => {
                let substr = self.eval_with_depth(substring, row)?;
                let s = self.eval_with_depth(string, row)?;
                self.eval_position(&substr, &s)
            }

            // TRIM function
            ArenaExtendedExpr::Trim { position, removal_char, string } => {
                let s = self.eval_with_depth(string, row)?;
                let remove = match removal_char {
                    Some(expr) => Some(self.eval_with_depth(expr, row)?),
                    None => None,
                };
                self.eval_trim(*position, remove.as_ref(), &s)
            }

            // EXTRACT function - simplified implementation
            ArenaExtendedExpr::Extract { field, expr: inner } => {
                let val = self.eval_with_depth(inner, row)?;
                self.eval_extract(*field, &val)
            }

            // INTERVAL expression - simplified implementation
            ArenaExtendedExpr::Interval { value, .. } => {
                // For now, just evaluate the value expression
                self.eval_with_depth(value, row)
            }

            // Pseudo-variables, session variables, etc. - not supported
            ArenaExtendedExpr::PseudoVariable { .. }
            | ArenaExtendedExpr::SessionVariable { .. }
            | ArenaExtendedExpr::DuplicateKeyValue { .. }
            | ArenaExtendedExpr::NextValue { .. }
            | ArenaExtendedExpr::MatchAgainst { .. } => Err(ExecutorError::UnsupportedExpression(
                "Advanced expression types not supported in arena evaluator".to_string(),
            )),
        }
    }

    /// Evaluate with depth tracking.
    fn eval_with_depth(
        &self,
        expr: &ArenaExpression<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        // Create a new evaluator with incremented depth
        let child = ArenaExpressionEvaluator {
            schema: self.schema,
            params: self.params,
            database: self.database,
            sql_mode: self.sql_mode.clone(),
            column_cache: RefCell::new(HashMap::new()), // Don't share cache across depth
            depth: self.depth + 1,
            interner: self.interner,
        };
        child.eval(expr, row)
    }

    /// Evaluate a binary operation with short-circuit semantics.
    fn eval_binary_op(
        &self,
        op: vibesql_ast::BinaryOperator,
        left: &ArenaExpression<'arena>,
        right: &ArenaExpression<'arena>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        use vibesql_ast::BinaryOperator;

        // Short-circuit evaluation for AND/OR
        match op {
            BinaryOperator::And => {
                let left_val = self.eval_with_depth(left, row)?;
                // Short-circuit: if left is false, return false immediately
                if matches!(left_val, SqlValue::Boolean(false)) {
                    return Ok(SqlValue::Boolean(false));
                }
                let right_val = self.eval_with_depth(right, row)?;
                // NULL AND FALSE = FALSE
                if matches!(left_val, SqlValue::Null)
                    && matches!(right_val, SqlValue::Boolean(false))
                {
                    return Ok(SqlValue::Boolean(false));
                }
                super::core::eval_binary_op_static(
                    &left_val,
                    &op,
                    &right_val,
                    self.sql_mode.clone(),
                )
            }
            BinaryOperator::Or => {
                let left_val = self.eval_with_depth(left, row)?;
                // Short-circuit: if left is true, return true immediately
                if matches!(left_val, SqlValue::Boolean(true)) {
                    return Ok(SqlValue::Boolean(true));
                }
                let right_val = self.eval_with_depth(right, row)?;
                // NULL OR TRUE = TRUE
                if matches!(left_val, SqlValue::Null)
                    && matches!(right_val, SqlValue::Boolean(true))
                {
                    return Ok(SqlValue::Boolean(true));
                }
                super::core::eval_binary_op_static(
                    &left_val,
                    &op,
                    &right_val,
                    self.sql_mode.clone(),
                )
            }
            _ => {
                // Non-short-circuit: evaluate both sides
                let left_val = self.eval_with_depth(left, row)?;
                let right_val = self.eval_with_depth(right, row)?;
                super::core::eval_binary_op_static(
                    &left_val,
                    &op,
                    &right_val,
                    self.sql_mode.clone(),
                )
            }
        }
    }

    /// Evaluate a CASE expression.
    fn eval_case(
        &self,
        operand: Option<&ArenaExpression<'arena>>,
        when_clauses: &bumpalo::collections::Vec<'arena, arena_ast::CaseWhen<'arena>>,
        else_result: Option<&ArenaExpression<'arena>>,
        row: &Row,
    ) -> Result<SqlValue, ExecutorError> {
        // Simple CASE: CASE operand WHEN value THEN result ...
        if let Some(op_expr) = operand {
            let op_val = self.eval_with_depth(op_expr, row)?;
            for when_clause in when_clauses.iter() {
                for condition in when_clause.conditions.iter() {
                    let cond_val = self.eval_with_depth(condition, row)?;
                    if super::core::values_are_equal(&op_val, &cond_val) {
                        return self.eval_with_depth(&when_clause.result, row);
                    }
                }
            }
        } else {
            // Searched CASE: CASE WHEN condition THEN result ...
            for when_clause in when_clauses.iter() {
                for condition in when_clause.conditions.iter() {
                    let cond_val = self.eval_with_depth(condition, row)?;
                    if matches!(cond_val, SqlValue::Boolean(true)) {
                        return self.eval_with_depth(&when_clause.result, row);
                    }
                }
            }
        }

        // No match - return ELSE or NULL
        match else_result {
            Some(else_expr) => self.eval_with_depth(else_expr, row),
            None => Ok(SqlValue::Null),
        }
    }

    /// Evaluate LIKE pattern matching.
    fn eval_like(
        &self,
        value: &SqlValue,
        pattern: &SqlValue,
        negated: bool,
    ) -> Result<SqlValue, ExecutorError> {
        match (value, pattern) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
            (SqlValue::Varchar(s), SqlValue::Varchar(p))
            | (SqlValue::Character(s), SqlValue::Varchar(p))
            | (SqlValue::Varchar(s), SqlValue::Character(p))
            | (SqlValue::Character(s), SqlValue::Character(p)) => {
                let matches = super::pattern::like_match(s, p);
                Ok(SqlValue::Boolean(if negated { !matches } else { matches }))
            }
            _ => Err(ExecutorError::TypeError(format!(
                "LIKE requires string operands, got {:?} and {:?}",
                value, pattern
            ))),
        }
    }

    /// Evaluate POSITION function.
    fn eval_position(
        &self,
        substring: &SqlValue,
        string: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        match (substring, string) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
            (SqlValue::Varchar(sub), SqlValue::Varchar(s))
            | (SqlValue::Character(sub), SqlValue::Varchar(s))
            | (SqlValue::Varchar(sub), SqlValue::Character(s))
            | (SqlValue::Character(sub), SqlValue::Character(s)) => {
                let pos = s.find(&**sub).map(|i| i + 1).unwrap_or(0);
                Ok(SqlValue::Integer(pos as i64))
            }
            _ => Err(ExecutorError::TypeError(format!(
                "POSITION requires string operands, got {:?}",
                substring
            ))),
        }
    }

    /// Evaluate TRIM function.
    fn eval_trim(
        &self,
        position: Option<arena_ast::TrimPosition>,
        removal_char: Option<&SqlValue>,
        string: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        match string {
            SqlValue::Null => Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                let remove_chars: &str = match removal_char {
                    Some(SqlValue::Varchar(r)) | Some(SqlValue::Character(r)) => r,
                    Some(SqlValue::Null) => return Ok(SqlValue::Null),
                    None => " ",
                    _ => {
                        return Err(ExecutorError::TypeError(format!(
                            "TRIM removal character must be string, got {:?}",
                            removal_char
                        )))
                    }
                };
                let result = match position {
                    Some(arena_ast::TrimPosition::Leading) => {
                        s.trim_start_matches(|c| remove_chars.contains(c))
                    }
                    Some(arena_ast::TrimPosition::Trailing) => {
                        s.trim_end_matches(|c| remove_chars.contains(c))
                    }
                    Some(arena_ast::TrimPosition::Both) | None => {
                        s.trim_matches(|c| remove_chars.contains(c))
                    }
                };
                Ok(SqlValue::Varchar(arcstr::ArcStr::from(result)))
            }
            _ => Err(ExecutorError::TypeError(format!(
                "TRIM requires string operand, got {:?}",
                string
            ))),
        }
    }

    /// Evaluate EXTRACT function.
    fn eval_extract(
        &self,
        field: arena_ast::IntervalUnit,
        value: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        use arena_ast::IntervalUnit;

        match value {
            SqlValue::Null => Ok(SqlValue::Null),
            SqlValue::Date(d) => {
                let result: i64 = match field {
                    IntervalUnit::Year => d.year as i64,
                    IntervalUnit::Month => d.month as i64,
                    IntervalUnit::Day => d.day as i64,
                    IntervalUnit::Quarter => (d.month as i64 - 1) / 3 + 1,
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(format!(
                            "EXTRACT {:?} from DATE not supported",
                            field
                        )))
                    }
                };
                Ok(SqlValue::Integer(result))
            }
            SqlValue::Time(t) => {
                let result: i64 = match field {
                    IntervalUnit::Hour => t.hour as i64,
                    IntervalUnit::Minute => t.minute as i64,
                    IntervalUnit::Second => t.second as i64,
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(format!(
                            "EXTRACT {:?} from TIME not supported",
                            field
                        )))
                    }
                };
                Ok(SqlValue::Integer(result))
            }
            SqlValue::Timestamp(ts) => {
                let result: i64 = match field {
                    IntervalUnit::Year => ts.date.year as i64,
                    IntervalUnit::Month => ts.date.month as i64,
                    IntervalUnit::Day => ts.date.day as i64,
                    IntervalUnit::Hour => ts.time.hour as i64,
                    IntervalUnit::Minute => ts.time.minute as i64,
                    IntervalUnit::Second => ts.time.second as i64,
                    IntervalUnit::Quarter => (ts.date.month as i64 - 1) / 3 + 1,
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(format!(
                            "EXTRACT {:?} from TIMESTAMP not supported",
                            field
                        )))
                    }
                };
                Ok(SqlValue::Integer(result))
            }
            _ => Err(ExecutorError::TypeError(format!(
                "EXTRACT requires date/time/timestamp operand, got {:?}",
                value
            ))),
        }
    }

    /// Get column index with caching.
    fn get_column_index_cached(&self, table: Option<&str>, column: &str) -> Option<usize> {
        // Compute hash for cache key
        let mut hasher = AHasher::default();
        table.hash(&mut hasher);
        column.hash(&mut hasher);
        let key = hasher.finish();

        // Check cache
        if let Some(&idx) = self.column_cache.borrow().get(&key) {
            return Some(idx);
        }

        // Look up in schema
        if let Some(idx) = self.schema.get_column_index(table, column) {
            self.column_cache.borrow_mut().insert(key, idx);
            return Some(idx);
        }

        None
    }

    /// Get available columns for error messages.
    fn get_available_columns(&self) -> Vec<String> {
        let mut columns = Vec::new();
        for (_start, schema) in self.schema.table_schemas.values() {
            columns.extend(schema.columns.iter().map(|c| c.name.clone()));
        }
        columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bumpalo::Bump;
    use vibesql_ast::arena::ArenaInterner;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    fn make_schema() -> CombinedSchema {
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(255) },
                true,
            ),
        ];
        let table_schema = TableSchema::new("test".to_string(), columns);
        CombinedSchema::from_table("test".to_string(), table_schema)
    }

    #[test]
    fn test_eval_literal() {
        let arena = Bump::new();
        let interner = ArenaInterner::new(&arena);
        let schema = make_schema();
        let params = vec![];
        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);
        let row = Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);

        let expr = ArenaExpression::Literal(SqlValue::Integer(42));
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));
    }

    #[test]
    fn test_eval_placeholder() {
        let arena = Bump::new();
        let interner = ArenaInterner::new(&arena);
        let schema = make_schema();
        let params = vec![SqlValue::Integer(100), SqlValue::Varchar(arcstr::ArcStr::from("test"))];
        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);
        let row = Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);

        // First placeholder (index 0)
        let expr = ArenaExpression::Placeholder(0);
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(100));

        // Second placeholder (index 1)
        let expr = ArenaExpression::Placeholder(1);
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("test")));
    }

    #[test]
    fn test_eval_column_ref() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);
        let schema = make_schema();
        let params = vec![];

        // Intern the column names (uppercased to match schema lookup)
        let id_sym = interner.intern("ID");
        let name_sym = interner.intern("NAME");

        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);
        let row = Row::new(vec![SqlValue::Integer(42), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]);

        let expr = ArenaExpression::ColumnRef { table: None, column: id_sym };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Integer(42));

        let expr = ArenaExpression::ColumnRef { table: None, column: name_sym };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
    }

    #[test]
    fn test_eval_is_null() {
        let arena = Bump::new();
        let mut interner = ArenaInterner::new(&arena);
        let schema = make_schema();
        let params = vec![];

        // Intern column names (uppercased to match schema lookup)
        let name_sym = interner.intern("NAME");
        let id_sym = interner.intern("ID");

        let evaluator = ArenaExpressionEvaluator::new(&schema, &params, &interner);
        let row = Row::new(vec![SqlValue::Integer(1), SqlValue::Null]);

        let expr = ArenaExpression::IsNull {
            expr: arena.alloc(ArenaExpression::ColumnRef { table: None, column: name_sym }),
            negated: false,
        };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Boolean(true));

        let expr = ArenaExpression::IsNull {
            expr: arena.alloc(ArenaExpression::ColumnRef { table: None, column: id_sym }),
            negated: false,
        };
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(result, SqlValue::Boolean(false));
    }
}
