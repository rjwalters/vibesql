//! Subscription filter evaluation
//!
//! This module provides functionality to evaluate filter expressions against
//! subscription result rows. Filters are SQL WHERE clause expressions that
//! are applied to subscription updates before sending to clients.

use std::collections::HashMap;
use vibesql_ast::Expression;
use vibesql_types::SqlValue;

/// Evaluates a filter expression against result rows.
///
/// The filter expression is a parsed SQL WHERE clause expression. Column
/// references in the expression are resolved against the column names in
/// the result schema.
pub struct SubscriptionFilter {
    /// The parsed filter expression
    expression: Expression,
    /// Column name to index mapping (case-insensitive)
    column_indices: HashMap<String, usize>,
}

impl SubscriptionFilter {
    /// Create a new subscription filter.
    ///
    /// # Arguments
    ///
    /// * `filter_expr` - The filter expression string (SQL WHERE clause body)
    /// * `column_names` - The column names from the subscription query result
    ///
    /// # Returns
    ///
    /// Returns the filter or an error if parsing fails.
    pub fn new(filter_expr: &str, column_names: &[String]) -> Result<Self, String> {
        // Parse the filter expression
        let expression = vibesql_parser::arena_parser::parse_expression_to_owned(filter_expr)
            .map_err(|e| format!("Failed to parse filter expression: {}", e))?;

        // Build column index map (case-insensitive)
        let column_indices: HashMap<String, usize> = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.to_lowercase(), idx))
            .collect();

        Ok(Self { expression, column_indices })
    }

    /// Evaluate the filter against a single row.
    ///
    /// # Arguments
    ///
    /// * `values` - The row values in column order
    ///
    /// # Returns
    ///
    /// Returns `true` if the row matches the filter, `false` otherwise.
    /// Returns `true` on evaluation errors (fail-open to avoid silent data loss).
    pub fn matches(&self, values: &[SqlValue]) -> bool {
        match self.evaluate_expression(&self.expression, values) {
            Ok(SqlValue::Boolean(b)) => b,
            Ok(SqlValue::Null) => false, // NULL is treated as false in WHERE clauses
            Ok(_) => true,               // Non-boolean non-null treated as truthy
            Err(_) => true,              // On error, include the row (fail-open)
        }
    }

    /// Filter a collection of rows, returning only those that match.
    ///
    /// # Arguments
    ///
    /// * `rows` - Iterator of rows (each row is a slice of SqlValue)
    ///
    /// # Returns
    ///
    /// A vector of rows that match the filter.
    pub fn filter_rows<'a, I>(&self, rows: I) -> Vec<&'a [SqlValue]>
    where
        I: Iterator<Item = &'a [SqlValue]>,
    {
        rows.filter(|row| self.matches(row)).collect()
    }

    /// Evaluate an expression against a row.
    fn evaluate_expression(
        &self,
        expr: &Expression,
        values: &[SqlValue],
    ) -> Result<SqlValue, String> {
        match expr {
            Expression::Literal(val) => Ok(val.clone()),

            Expression::ColumnRef { table: _, column } => {
                // Look up column by name (case-insensitive)
                let idx = self
                    .column_indices
                    .get(&column.to_lowercase())
                    .ok_or_else(|| format!("Unknown column: {}", column))?;

                values
                    .get(*idx)
                    .cloned()
                    .ok_or_else(|| format!("Column index {} out of bounds", idx))
            }

            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression(left, values)?;
                let right_val = self.evaluate_expression(right, values)?;
                self.evaluate_binary_op(&left_val, op, &right_val)
            }

            Expression::IsNull { expr, negated } => {
                let val = self.evaluate_expression(expr, values)?;
                let is_null = matches!(val, SqlValue::Null);
                Ok(SqlValue::Boolean(if *negated { !is_null } else { is_null }))
            }

            Expression::Conjunction(exprs) => {
                // AND: all must be true
                for e in exprs {
                    match self.evaluate_expression(e, values)? {
                        SqlValue::Boolean(false) => return Ok(SqlValue::Boolean(false)),
                        SqlValue::Null => return Ok(SqlValue::Null),
                        _ => {}
                    }
                }
                Ok(SqlValue::Boolean(true))
            }

            Expression::Disjunction(exprs) => {
                // OR: any must be true
                let mut has_null = false;
                for e in exprs {
                    match self.evaluate_expression(e, values)? {
                        SqlValue::Boolean(true) => return Ok(SqlValue::Boolean(true)),
                        SqlValue::Null => has_null = true,
                        _ => {}
                    }
                }
                if has_null {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Boolean(false))
                }
            }

            Expression::UnaryOp { op, expr } => {
                let val = self.evaluate_expression(expr, values)?;
                self.evaluate_unary_op(op, &val)
            }

            Expression::InList { expr, values: list_values, negated } => {
                let val = self.evaluate_expression(expr, values)?;
                if matches!(val, SqlValue::Null) {
                    return Ok(SqlValue::Null);
                }

                for item in list_values {
                    let item_val = self.evaluate_expression(item, values)?;
                    if self.values_equal(&val, &item_val) {
                        return Ok(SqlValue::Boolean(!negated));
                    }
                }
                Ok(SqlValue::Boolean(*negated))
            }

            Expression::Between { expr, low, high, negated, .. } => {
                let val = self.evaluate_expression(expr, values)?;
                let low_val = self.evaluate_expression(low, values)?;
                let high_val = self.evaluate_expression(high, values)?;

                if matches!(val, SqlValue::Null)
                    || matches!(low_val, SqlValue::Null)
                    || matches!(high_val, SqlValue::Null)
                {
                    return Ok(SqlValue::Null);
                }

                let ge_low = self.compare_values(&val, &low_val)? >= 0;
                let le_high = self.compare_values(&val, &high_val)? <= 0;
                let in_range = ge_low && le_high;

                Ok(SqlValue::Boolean(if *negated { !in_range } else { in_range }))
            }

            Expression::Like { expr, pattern, negated } => {
                let val = self.evaluate_expression(expr, values)?;
                let pattern_val = self.evaluate_expression(pattern, values)?;

                match (&val, &pattern_val) {
                    (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
                    _ => {
                        let s = self.value_to_string(&val)?;
                        let p = self.value_to_string(&pattern_val)?;
                        let matches = self.like_match(&s, &p);
                        Ok(SqlValue::Boolean(if *negated { !matches } else { matches }))
                    }
                }
            }

            // For unsupported expressions, return an error
            _ => Err(format!("Unsupported filter expression: {:?}", expr)),
        }
    }

    /// Evaluate a binary operation.
    fn evaluate_binary_op(
        &self,
        left: &SqlValue,
        op: &vibesql_ast::BinaryOperator,
        right: &SqlValue,
    ) -> Result<SqlValue, String> {
        use vibesql_ast::BinaryOperator;

        // Handle NULL propagation for most operations
        if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
            // Comparison with NULL returns NULL
            return Ok(SqlValue::Null);
        }

        match op {
            BinaryOperator::Equal => Ok(SqlValue::Boolean(self.values_equal(left, right))),
            BinaryOperator::NotEqual => Ok(SqlValue::Boolean(!self.values_equal(left, right))),
            BinaryOperator::LessThan => {
                Ok(SqlValue::Boolean(self.compare_values(left, right)? < 0))
            }
            BinaryOperator::LessThanOrEqual => {
                Ok(SqlValue::Boolean(self.compare_values(left, right)? <= 0))
            }
            BinaryOperator::GreaterThan => {
                Ok(SqlValue::Boolean(self.compare_values(left, right)? > 0))
            }
            BinaryOperator::GreaterThanOrEqual => {
                Ok(SqlValue::Boolean(self.compare_values(left, right)? >= 0))
            }
            BinaryOperator::And => {
                let l = self.to_bool(left)?;
                let r = self.to_bool(right)?;
                Ok(SqlValue::Boolean(l && r))
            }
            BinaryOperator::Or => {
                let l = self.to_bool(left)?;
                let r = self.to_bool(right)?;
                Ok(SqlValue::Boolean(l || r))
            }
            _ => Err(format!("Unsupported binary operator in filter: {:?}", op)),
        }
    }

    /// Evaluate a unary operation.
    fn evaluate_unary_op(
        &self,
        op: &vibesql_ast::UnaryOperator,
        val: &SqlValue,
    ) -> Result<SqlValue, String> {
        use vibesql_ast::UnaryOperator;

        match op {
            UnaryOperator::Not => {
                if matches!(val, SqlValue::Null) {
                    return Ok(SqlValue::Null);
                }
                let b = self.to_bool(val)?;
                Ok(SqlValue::Boolean(!b))
            }
            UnaryOperator::Minus => match val {
                SqlValue::Null => Ok(SqlValue::Null),
                SqlValue::Integer(i) => Ok(SqlValue::Integer(-i)),
                SqlValue::Bigint(i) => Ok(SqlValue::Bigint(-i)),
                SqlValue::Smallint(i) => Ok(SqlValue::Smallint(-i)),
                SqlValue::Float(f) => Ok(SqlValue::Float(-f)),
                SqlValue::Double(f) => Ok(SqlValue::Double(-f)),
                SqlValue::Numeric(f) => Ok(SqlValue::Numeric(-f)),
                _ => Err("Unary minus requires numeric operand".to_string()),
            },
            UnaryOperator::Plus => Ok(val.clone()),
            // IsNull and IsNotNull are handled by Expression::IsNull
            UnaryOperator::IsNull | UnaryOperator::IsNotNull => {
                Err("IsNull/IsNotNull should be handled by Expression::IsNull".to_string())
            }
        }
    }

    /// Check if two values are equal.
    fn values_equal(&self, left: &SqlValue, right: &SqlValue) -> bool {
        match (left, right) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => false,
            (SqlValue::Integer(a), SqlValue::Integer(b)) => a == b,
            (SqlValue::Bigint(a), SqlValue::Bigint(b)) => a == b,
            (SqlValue::Smallint(a), SqlValue::Smallint(b)) => a == b,
            (SqlValue::Float(a), SqlValue::Float(b)) => (a - b).abs() < f32::EPSILON,
            (SqlValue::Double(a), SqlValue::Double(b)) => (a - b).abs() < f64::EPSILON,
            (SqlValue::Numeric(a), SqlValue::Numeric(b)) => (a - b).abs() < f64::EPSILON,
            // Cross-type numeric comparisons
            (SqlValue::Integer(a), SqlValue::Double(b))
            | (SqlValue::Double(b), SqlValue::Integer(a))
            | (SqlValue::Bigint(a), SqlValue::Double(b))
            | (SqlValue::Double(b), SqlValue::Bigint(a)) => (*a as f64 - b).abs() < f64::EPSILON,
            // String types
            (SqlValue::Character(a), SqlValue::Character(b))
            | (SqlValue::Varchar(a), SqlValue::Varchar(b))
            | (SqlValue::Character(a), SqlValue::Varchar(b))
            | (SqlValue::Varchar(a), SqlValue::Character(b)) => a == b,
            (SqlValue::Boolean(a), SqlValue::Boolean(b)) => a == b,
            _ => false,
        }
    }

    /// Compare two values, returning -1, 0, or 1.
    fn compare_values(&self, left: &SqlValue, right: &SqlValue) -> Result<i32, String> {
        match (left, right) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => Ok(a.cmp(b) as i32),
            (SqlValue::Bigint(a), SqlValue::Bigint(b)) => Ok(a.cmp(b) as i32),
            (SqlValue::Smallint(a), SqlValue::Smallint(b)) => Ok(a.cmp(b) as i32),
            (SqlValue::Float(a), SqlValue::Float(b)) => {
                Ok(a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) as i32)
            }
            (SqlValue::Double(a), SqlValue::Double(b)) => {
                Ok(a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) as i32)
            }
            (SqlValue::Numeric(a), SqlValue::Numeric(b)) => {
                Ok(a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) as i32)
            }
            // Cross-type numeric comparisons
            (SqlValue::Integer(a), SqlValue::Double(b))
            | (SqlValue::Bigint(a), SqlValue::Double(b)) => {
                let a_f = *a as f64;
                Ok(a_f.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal) as i32)
            }
            (SqlValue::Double(a), SqlValue::Integer(b))
            | (SqlValue::Double(a), SqlValue::Bigint(b)) => {
                let b_f = *b as f64;
                Ok(a.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal) as i32)
            }
            // String comparisons
            (SqlValue::Character(a), SqlValue::Character(b))
            | (SqlValue::Varchar(a), SqlValue::Varchar(b))
            | (SqlValue::Character(a), SqlValue::Varchar(b))
            | (SqlValue::Varchar(a), SqlValue::Character(b)) => Ok(a.cmp(b) as i32),
            _ => Err(format!("Cannot compare {:?} and {:?}", left, right)),
        }
    }

    /// Convert a value to boolean.
    fn to_bool(&self, val: &SqlValue) -> Result<bool, String> {
        match val {
            SqlValue::Boolean(b) => Ok(*b),
            SqlValue::Integer(i) => Ok(*i != 0),
            SqlValue::Bigint(i) => Ok(*i != 0),
            SqlValue::Smallint(i) => Ok(*i != 0),
            SqlValue::Float(f) => Ok(*f != 0.0),
            SqlValue::Double(f) => Ok(*f != 0.0),
            SqlValue::Numeric(f) => Ok(*f != 0.0),
            SqlValue::Character(s) | SqlValue::Varchar(s) => Ok(!s.is_empty()),
            SqlValue::Null => Err("Cannot convert NULL to boolean".to_string()),
            _ => Err(format!("Cannot convert {:?} to boolean", val)),
        }
    }

    /// Convert a value to string for LIKE matching.
    fn value_to_string(&self, val: &SqlValue) -> Result<String, String> {
        match val {
            SqlValue::Character(s) | SqlValue::Varchar(s) => Ok(s.to_string()),
            SqlValue::Integer(i) => Ok(i.to_string()),
            SqlValue::Bigint(i) => Ok(i.to_string()),
            _ => Err(format!("Cannot convert {:?} to string for LIKE", val)),
        }
    }

    /// Simple LIKE pattern matching (supports % and _).
    fn like_match(&self, text: &str, pattern: &str) -> bool {
        let chars = pattern.chars().peekable();
        let text_chars = text.chars().peekable();
        self.like_match_recursive(&mut text_chars.clone(), &mut chars.clone())
    }

    #[allow(clippy::only_used_in_recursion)]
    fn like_match_recursive(
        &self,
        text: &mut std::iter::Peekable<std::str::Chars>,
        pattern: &mut std::iter::Peekable<std::str::Chars>,
    ) -> bool {
        loop {
            match (pattern.peek(), text.peek()) {
                (None, None) => return true,
                (None, Some(_)) => return false,
                (Some('%'), _) => {
                    pattern.next();
                    if pattern.peek().is_none() {
                        return true; // % at end matches everything
                    }
                    // Try matching % with zero or more characters
                    let mut text_clone = text.clone();
                    loop {
                        let mut pattern_clone = pattern.clone();
                        if self.like_match_recursive(&mut text_clone.clone(), &mut pattern_clone) {
                            return true;
                        }
                        if text_clone.next().is_none() {
                            return false;
                        }
                    }
                }
                (Some('_'), Some(_)) => {
                    pattern.next();
                    text.next();
                }
                (Some('_'), None) => return false,
                (Some(p), Some(t)) => {
                    if p.to_lowercase().next() == t.to_lowercase().next() {
                        pattern.next();
                        text.next();
                    } else {
                        return false;
                    }
                }
                (Some(_), None) => return false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_equality_filter() {
        let columns = vec!["id".to_string(), "name".to_string(), "status".to_string()];
        let filter = SubscriptionFilter::new("status = 'active'", &columns).unwrap();

        // Row that matches
        let row1 = vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".into()),
            SqlValue::Varchar(arcstr::ArcStr::from("active")),
        ];
        assert!(filter.matches(&row1));

        // Row that doesn't match
        let row2 = vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            SqlValue::Varchar(arcstr::ArcStr::from("inactive")),
        ];
        assert!(!filter.matches(&row2));
    }

    #[test]
    fn test_numeric_comparison() {
        let columns = vec!["id".to_string(), "amount".to_string()];
        let filter = SubscriptionFilter::new("amount > 100", &columns).unwrap();

        let row1 = vec![SqlValue::Integer(1), SqlValue::Integer(150)];
        assert!(filter.matches(&row1));

        let row2 = vec![SqlValue::Integer(2), SqlValue::Integer(50)];
        assert!(!filter.matches(&row2));
    }

    #[test]
    fn test_and_filter() {
        let columns = vec!["status".to_string(), "amount".to_string()];
        let filter =
            SubscriptionFilter::new("status = 'active' AND amount > 50", &columns).unwrap();

        let row1 = vec![SqlValue::Varchar(arcstr::ArcStr::from("active")), SqlValue::Integer(100)];
        assert!(filter.matches(&row1));

        let row2 = vec![SqlValue::Varchar(arcstr::ArcStr::from("active")), SqlValue::Integer(30)];
        assert!(!filter.matches(&row2));

        let row3 = vec![SqlValue::Varchar(arcstr::ArcStr::from("inactive")), SqlValue::Integer(100)];
        assert!(!filter.matches(&row3));
    }

    #[test]
    fn test_or_filter() {
        let columns = vec!["status".to_string()];
        let filter =
            SubscriptionFilter::new("status = 'active' OR status = 'pending'", &columns).unwrap();

        let row1 = vec![SqlValue::Varchar(arcstr::ArcStr::from("active"))];
        assert!(filter.matches(&row1));

        let row2 = vec![SqlValue::Varchar(arcstr::ArcStr::from("pending"))];
        assert!(filter.matches(&row2));

        let row3 = vec![SqlValue::Varchar(arcstr::ArcStr::from("inactive"))];
        assert!(!filter.matches(&row3));
    }

    #[test]
    fn test_null_handling() {
        let columns = vec!["value".to_string()];
        let filter = SubscriptionFilter::new("value IS NULL", &columns).unwrap();

        let row1 = vec![SqlValue::Null];
        assert!(filter.matches(&row1));

        let row2 = vec![SqlValue::Integer(42)];
        assert!(!filter.matches(&row2));
    }

    #[test]
    fn test_is_not_null() {
        let columns = vec!["value".to_string()];
        let filter = SubscriptionFilter::new("value IS NOT NULL", &columns).unwrap();

        let row1 = vec![SqlValue::Integer(42)];
        assert!(filter.matches(&row1));

        let row2 = vec![SqlValue::Null];
        assert!(!filter.matches(&row2));
    }

    #[test]
    fn test_in_list() {
        let columns = vec!["status".to_string()];
        let filter =
            SubscriptionFilter::new("status IN ('active', 'pending')", &columns).unwrap();

        let row1 = vec![SqlValue::Varchar(arcstr::ArcStr::from("active"))];
        assert!(filter.matches(&row1));

        let row2 = vec![SqlValue::Varchar(arcstr::ArcStr::from("pending"))];
        assert!(filter.matches(&row2));

        let row3 = vec![SqlValue::Varchar(arcstr::ArcStr::from("inactive"))];
        assert!(!filter.matches(&row3));
    }

    #[test]
    fn test_between() {
        let columns = vec!["amount".to_string()];
        let filter = SubscriptionFilter::new("amount BETWEEN 10 AND 100", &columns).unwrap();

        let row1 = vec![SqlValue::Integer(50)];
        assert!(filter.matches(&row1));

        let row2 = vec![SqlValue::Integer(10)];
        assert!(filter.matches(&row2));

        let row3 = vec![SqlValue::Integer(100)];
        assert!(filter.matches(&row3));

        let row4 = vec![SqlValue::Integer(5)];
        assert!(!filter.matches(&row4));

        let row5 = vec![SqlValue::Integer(150)];
        assert!(!filter.matches(&row5));
    }

    #[test]
    fn test_like_pattern() {
        let columns = vec!["name".to_string()];
        let filter = SubscriptionFilter::new("name LIKE 'A%'", &columns).unwrap();

        let row1 = vec![SqlValue::Varchar("Alice".into())];
        assert!(filter.matches(&row1));

        let row2 = vec![SqlValue::Varchar(arcstr::ArcStr::from("Bob"))];
        assert!(!filter.matches(&row2));
    }

    #[test]
    fn test_case_insensitive_column() {
        let columns = vec!["STATUS".to_string()];
        let filter = SubscriptionFilter::new("status = 'active'", &columns).unwrap();

        let row = vec![SqlValue::Varchar(arcstr::ArcStr::from("active"))];
        assert!(filter.matches(&row));
    }

    #[test]
    fn test_invalid_filter() {
        let columns = vec!["id".to_string()];
        // Use a truly malformed expression that the parser cannot handle
        let result = SubscriptionFilter::new("SELECT * FROM", &columns);
        assert!(result.is_err());
    }
}
