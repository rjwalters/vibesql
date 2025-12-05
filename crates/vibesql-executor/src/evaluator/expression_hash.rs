//! Expression hashing for common sub-expression elimination (CSE)
//!
//! This module provides structural hashing of expression trees to enable
//! caching and reuse of identical sub-expressions during evaluation.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Computes structural hashes for expression trees
pub struct ExpressionHasher;

impl ExpressionHasher {
    /// Compute structural hash for an expression tree
    ///
    /// Two structurally identical expressions will hash to the same value,
    /// regardless of memory addresses or pointer values.
    ///
    /// # Examples
    ///
    /// ```text
    /// let expr1 = Expression::BinaryOp { /* a + b */ };
    /// let expr2 = Expression::BinaryOp { /* a + b */ };
    /// assert_eq!(ExpressionHasher::hash(&expr1), ExpressionHasher::hash(&expr2));
    /// ```
    pub fn hash(expr: &vibesql_ast::Expression) -> u64 {
        let mut hasher = DefaultHasher::new();
        Self::hash_expression(expr, &mut hasher);
        hasher.finish()
    }

    /// Check if an expression is deterministic (safe to cache)
    ///
    /// Non-deterministic expressions like RAND() or CURRENT_TIMESTAMP should
    /// not be cached as they may produce different values on each evaluation.
    pub fn is_deterministic(expr: &vibesql_ast::Expression) -> bool {
        match expr {
            // Non-deterministic datetime functions
            vibesql_ast::Expression::CurrentDate
            | vibesql_ast::Expression::CurrentTime { .. }
            | vibesql_ast::Expression::CurrentTimestamp { .. } => false,

            // Non-deterministic functions and aggregate functions used as regular functions
            vibesql_ast::Expression::Function { name, args, .. } => {
                let name_upper = name.to_uppercase();
                // List of known non-deterministic functions and aggregate functions
                // (aggregate functions should not be cached at row level)
                if matches!(
                    name_upper.as_str(),
                    "RAND"
                        | "RANDOM"
                        | "NOW"
                        | "CURRENT_DATE"
                        | "CURRENT_TIME"
                        | "CURRENT_TIMESTAMP"
                        | "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                ) {
                    return false;
                }
                // Recursively check arguments
                args.iter().all(Self::is_deterministic)
            }

            // Sequence functions are non-deterministic
            vibesql_ast::Expression::NextValue { .. } => false,

            // Subqueries may be non-deterministic (conservative approach)
            vibesql_ast::Expression::ScalarSubquery(_)
            | vibesql_ast::Expression::In { .. }
            | vibesql_ast::Expression::Exists { .. }
            | vibesql_ast::Expression::QuantifiedComparison { .. } => false,

            // Recursively check sub-expressions
            vibesql_ast::Expression::BinaryOp { left, right, .. } => {
                Self::is_deterministic(left) && Self::is_deterministic(right)
            }

            vibesql_ast::Expression::UnaryOp { expr, .. } => Self::is_deterministic(expr),

            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                // Check operand
                if let Some(op) = operand {
                    if !Self::is_deterministic(op) {
                        return false;
                    }
                }
                // Check all when conditions and results
                for clause in when_clauses {
                    // Check all conditions (there can be multiple per clause)
                    for condition in &clause.conditions {
                        if !Self::is_deterministic(condition) {
                            return false;
                        }
                    }
                    if !Self::is_deterministic(&clause.result) {
                        return false;
                    }
                }
                // Check else result
                if let Some(else_expr) = else_result {
                    if !Self::is_deterministic(else_expr) {
                        return false;
                    }
                }
                true
            }

            vibesql_ast::Expression::Between { expr, low, high, .. } => {
                Self::is_deterministic(expr)
                    && Self::is_deterministic(low)
                    && Self::is_deterministic(high)
            }

            vibesql_ast::Expression::Cast { expr, .. } => Self::is_deterministic(expr),

            vibesql_ast::Expression::Interval { value, .. } => Self::is_deterministic(value),

            vibesql_ast::Expression::InList { expr, values, .. } => {
                Self::is_deterministic(expr) && values.iter().all(Self::is_deterministic)
            }

            vibesql_ast::Expression::Like { expr, pattern, .. } => {
                Self::is_deterministic(expr) && Self::is_deterministic(pattern)
            }

            vibesql_ast::Expression::IsNull { expr, .. } => Self::is_deterministic(expr),

            vibesql_ast::Expression::Position { substring, string, .. } => {
                Self::is_deterministic(substring) && Self::is_deterministic(string)
            }

            vibesql_ast::Expression::Trim { string, removal_char, .. } => {
                Self::is_deterministic(string)
                    && removal_char.as_ref().is_none_or(|c| Self::is_deterministic(c))
            }

            vibesql_ast::Expression::Extract { expr, .. } => Self::is_deterministic(expr),

            // Literals and placeholders are deterministic, but column references, pseudo-variables, and session variables are NOT
            // Column references and pseudo-variables depend on the current row data, so they should not be cached
            // across multiple rows in row-iteration contexts
            // Session variables can change during execution, so they should not be cached
            // Placeholders should be bound to values before evaluation, so treat them as deterministic
            vibesql_ast::Expression::Literal(_)
            | vibesql_ast::Expression::Placeholder(_)
            | vibesql_ast::Expression::NumberedPlaceholder(_)
            | vibesql_ast::Expression::NamedPlaceholder(_) => true,
            vibesql_ast::Expression::ColumnRef { .. }
            | vibesql_ast::Expression::PseudoVariable { .. }
            | vibesql_ast::Expression::SessionVariable { .. } => false,

            // Window and aggregate functions should not be cached at this level
            vibesql_ast::Expression::WindowFunction { .. }
            | vibesql_ast::Expression::AggregateFunction { .. } => false,

            // Wildcards, DEFAULT, and DuplicateKeyValue are special cases
            vibesql_ast::Expression::Wildcard
            | vibesql_ast::Expression::Default
            | vibesql_ast::Expression::DuplicateKeyValue { .. } => true,

            // Conjunction and Disjunction - check all children
            vibesql_ast::Expression::Conjunction(children)
            | vibesql_ast::Expression::Disjunction(children) => {
                children.iter().all(Self::is_deterministic)
            }

            // MATCH AGAINST is deterministic if the search term is constant
            vibesql_ast::Expression::MatchAgainst { .. } => {
                // MATCH AGAINST always operates on columns, which are non-deterministic
                // Conservative: treat as non-deterministic since results depend on table data
                false
            }
        }
    }

    /// Internal recursive hash function
    fn hash_expression(expr: &vibesql_ast::Expression, hasher: &mut DefaultHasher) {
        // Hash the discriminant (which variant of the enum)
        std::mem::discriminant(expr).hash(hasher);

        // Hash the contents based on variant
        match expr {
            vibesql_ast::Expression::Literal(val) => {
                // Hash the SQL value
                Self::hash_sql_value(val, hasher);
            }

            vibesql_ast::Expression::ColumnRef { table, column } => {
                table.hash(hasher);
                column.hash(hasher);
            }

            vibesql_ast::Expression::PseudoVariable { pseudo_table, column } => {
                std::mem::discriminant(pseudo_table).hash(hasher);
                column.hash(hasher);
            }

            vibesql_ast::Expression::BinaryOp { left, op, right } => {
                Self::hash_expression(left, hasher);
                std::mem::discriminant(op).hash(hasher);
                Self::hash_expression(right, hasher);
            }

            vibesql_ast::Expression::UnaryOp { op, expr } => {
                std::mem::discriminant(op).hash(hasher);
                Self::hash_expression(expr, hasher);
            }

            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                if let Some(op) = operand {
                    Self::hash_expression(op, hasher);
                }
                when_clauses.len().hash(hasher);
                for clause in when_clauses {
                    clause.conditions.len().hash(hasher);
                    for condition in &clause.conditions {
                        Self::hash_expression(condition, hasher);
                    }
                    Self::hash_expression(&clause.result, hasher);
                }
                if let Some(else_expr) = else_result {
                    Self::hash_expression(else_expr, hasher);
                }
            }

            vibesql_ast::Expression::Between { expr, low, high, negated, symmetric } => {
                Self::hash_expression(expr, hasher);
                Self::hash_expression(low, hasher);
                Self::hash_expression(high, hasher);
                negated.hash(hasher);
                symmetric.hash(hasher);
            }

            vibesql_ast::Expression::Cast { expr, data_type } => {
                Self::hash_expression(expr, hasher);
                Self::hash_data_type(data_type, hasher);
            }

            vibesql_ast::Expression::InList { expr, values, negated } => {
                Self::hash_expression(expr, hasher);
                values.len().hash(hasher);
                for val in values {
                    Self::hash_expression(val, hasher);
                }
                negated.hash(hasher);
            }

            vibesql_ast::Expression::Like { expr, pattern, negated } => {
                Self::hash_expression(expr, hasher);
                Self::hash_expression(pattern, hasher);
                negated.hash(hasher);
            }

            vibesql_ast::Expression::IsNull { expr, negated } => {
                Self::hash_expression(expr, hasher);
                negated.hash(hasher);
            }

            vibesql_ast::Expression::Function { name, args, character_unit } => {
                name.hash(hasher);
                args.len().hash(hasher);
                for arg in args {
                    Self::hash_expression(arg, hasher);
                }
                if let Some(unit) = character_unit {
                    std::mem::discriminant(unit).hash(hasher);
                }
            }

            vibesql_ast::Expression::Position { substring, string, character_unit } => {
                Self::hash_expression(substring, hasher);
                Self::hash_expression(string, hasher);
                if let Some(unit) = character_unit {
                    std::mem::discriminant(unit).hash(hasher);
                }
            }

            vibesql_ast::Expression::Trim { position, removal_char, string } => {
                if let Some(pos) = position {
                    std::mem::discriminant(pos).hash(hasher);
                }
                if let Some(ch) = removal_char {
                    Self::hash_expression(ch, hasher);
                }
                Self::hash_expression(string, hasher);
            }

            vibesql_ast::Expression::Extract { field, expr } => {
                "EXTRACT".hash(hasher);
                std::mem::discriminant(field).hash(hasher);
                Self::hash_expression(expr, hasher);
            }

            vibesql_ast::Expression::CurrentDate => {
                "CURRENT_DATE".hash(hasher);
            }

            vibesql_ast::Expression::CurrentTime { precision } => {
                "CURRENT_TIME".hash(hasher);
                precision.hash(hasher);
            }

            vibesql_ast::Expression::CurrentTimestamp { precision } => {
                "CURRENT_TIMESTAMP".hash(hasher);
                precision.hash(hasher);
            }

            vibesql_ast::Expression::Interval {
                value,
                unit,
                leading_precision,
                fractional_precision,
            } => {
                "INTERVAL".hash(hasher);
                Self::hash_expression(value, hasher);
                format!("{:?}", unit).hash(hasher);
                leading_precision.hash(hasher);
                fractional_precision.hash(hasher);
            }

            // For subqueries and complex expressions, we still hash them
            // but is_deterministic() will prevent caching
            vibesql_ast::Expression::In { expr, subquery, negated } => {
                Self::hash_expression(expr, hasher);
                format!("{:?}", subquery).hash(hasher); // Simple hash for now
                negated.hash(hasher);
            }

            vibesql_ast::Expression::ScalarSubquery(subquery) => {
                format!("{:?}", subquery).hash(hasher);
            }

            vibesql_ast::Expression::Exists { subquery, negated } => {
                format!("{:?}", subquery).hash(hasher);
                negated.hash(hasher);
            }

            vibesql_ast::Expression::QuantifiedComparison { expr, op, quantifier, subquery } => {
                Self::hash_expression(expr, hasher);
                std::mem::discriminant(op).hash(hasher);
                std::mem::discriminant(quantifier).hash(hasher);
                format!("{:?}", subquery).hash(hasher);
            }

            vibesql_ast::Expression::WindowFunction { .. } => {
                // Hash the debug representation for now
                format!("{:?}", expr).hash(hasher);
            }

            vibesql_ast::Expression::AggregateFunction { .. } => {
                // Hash the debug representation for now
                format!("{:?}", expr).hash(hasher);
            }

            vibesql_ast::Expression::Wildcard => {
                "*".hash(hasher);
            }

            vibesql_ast::Expression::Default => {
                "DEFAULT".hash(hasher);
            }

            vibesql_ast::Expression::DuplicateKeyValue { column } => {
                "DUPLICATE_KEY_VALUE".hash(hasher);
                column.hash(hasher);
            }

            vibesql_ast::Expression::NextValue { sequence_name } => {
                sequence_name.hash(hasher);
            }

            vibesql_ast::Expression::MatchAgainst { columns, search_modifier, mode } => {
                columns.len().hash(hasher);
                for col in columns {
                    col.hash(hasher);
                }
                Self::hash_expression(search_modifier, hasher);
                std::mem::discriminant(mode).hash(hasher);
            }

            vibesql_ast::Expression::SessionVariable { name } => {
                name.hash(hasher);
            }

            vibesql_ast::Expression::Placeholder(idx)
            | vibesql_ast::Expression::NumberedPlaceholder(idx) => {
                idx.hash(hasher);
            }

            vibesql_ast::Expression::NamedPlaceholder(name) => {
                name.hash(hasher);
            }

            vibesql_ast::Expression::Conjunction(children)
            | vibesql_ast::Expression::Disjunction(children) => {
                children.len().hash(hasher);
                for child in children {
                    Self::hash_expression(child, hasher);
                }
            }
        }
    }

    /// Hash a SQL value
    fn hash_sql_value(val: &vibesql_types::SqlValue, hasher: &mut DefaultHasher) {
        std::mem::discriminant(val).hash(hasher);
        match val {
            vibesql_types::SqlValue::Null => {}
            vibesql_types::SqlValue::Integer(i) => i.hash(hasher),
            vibesql_types::SqlValue::Smallint(s) => s.hash(hasher),
            vibesql_types::SqlValue::Bigint(b) => b.hash(hasher),
            vibesql_types::SqlValue::Unsigned(u) => u.hash(hasher),
            vibesql_types::SqlValue::Numeric(n) => n.to_bits().hash(hasher), // Hash float bits
            vibesql_types::SqlValue::Float(f) => f.to_bits().hash(hasher),
            vibesql_types::SqlValue::Real(r) => r.to_bits().hash(hasher),
            vibesql_types::SqlValue::Double(d) => d.to_bits().hash(hasher),
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                s.hash(hasher)
            }
            vibesql_types::SqlValue::Boolean(b) => b.hash(hasher),
            vibesql_types::SqlValue::Date(d) => d.hash(hasher),
            vibesql_types::SqlValue::Time(t) => t.hash(hasher),
            vibesql_types::SqlValue::Timestamp(ts) => ts.hash(hasher),
            vibesql_types::SqlValue::Interval(i) => i.hash(hasher),
            vibesql_types::SqlValue::Vector(v) => {
                // Hash each f32 using to_bits() for consistent hashing
                for f in v.iter() {
                    f.to_bits().hash(hasher);
                }
            }
        }
    }

    /// Hash a data type
    fn hash_data_type(data_type: &vibesql_types::DataType, hasher: &mut DefaultHasher) {
        std::mem::discriminant(data_type).hash(hasher);
        match data_type {
            vibesql_types::DataType::Integer => {}
            vibesql_types::DataType::Smallint => {}
            vibesql_types::DataType::Bigint => {}
            vibesql_types::DataType::Unsigned => {}
            vibesql_types::DataType::Numeric { precision, scale }
            | vibesql_types::DataType::Decimal { precision, scale } => {
                precision.hash(hasher);
                scale.hash(hasher);
            }
            vibesql_types::DataType::Float { precision } => precision.hash(hasher),
            vibesql_types::DataType::Real => {}
            vibesql_types::DataType::DoublePrecision => {}
            vibesql_types::DataType::Varchar { max_length } => max_length.hash(hasher),
            vibesql_types::DataType::Character { length } => length.hash(hasher),
            vibesql_types::DataType::CharacterLargeObject => {}
            vibesql_types::DataType::Name => {}
            vibesql_types::DataType::Boolean => {}
            vibesql_types::DataType::Date => {}
            vibesql_types::DataType::Time { with_timezone } => with_timezone.hash(hasher),
            vibesql_types::DataType::Timestamp { with_timezone } => with_timezone.hash(hasher),
            vibesql_types::DataType::Interval { start_field, end_field } => {
                std::mem::discriminant(start_field).hash(hasher);
                if let Some(ef) = end_field {
                    std::mem::discriminant(ef).hash(hasher);
                }
            }
            vibesql_types::DataType::BinaryLargeObject => {}
            vibesql_types::DataType::Bit { length } => length.hash(hasher),
            vibesql_types::DataType::UserDefined { type_name } => type_name.hash(hasher),
            vibesql_types::DataType::Vector { dimensions } => dimensions.hash(hasher),
            vibesql_types::DataType::Null => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identical_literals_hash_equal() {
        let expr1 = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42));
        let expr2 = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42));
        assert_eq!(ExpressionHasher::hash(&expr1), ExpressionHasher::hash(&expr2));
    }

    #[test]
    fn test_different_literals_hash_different() {
        let expr1 = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42));
        let expr2 = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(43));
        assert_ne!(ExpressionHasher::hash(&expr1), ExpressionHasher::hash(&expr2));
    }

    #[test]
    fn test_identical_binary_ops_hash_equal() {
        let left = Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)));
        let right = Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)));

        let expr1 = vibesql_ast::Expression::BinaryOp {
            left: left.clone(),
            op: vibesql_ast::BinaryOperator::Plus,
            right: right.clone(),
        };

        let expr2 = vibesql_ast::Expression::BinaryOp {
            left,
            op: vibesql_ast::BinaryOperator::Plus,
            right,
        };

        assert_eq!(ExpressionHasher::hash(&expr1), ExpressionHasher::hash(&expr2));
    }

    #[test]
    fn test_literals_are_deterministic() {
        let expr = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42));
        assert!(ExpressionHasher::is_deterministic(&expr));
    }

    #[test]
    fn test_column_refs_are_not_deterministic() {
        // Column references are NOT deterministic because they depend on row data
        // Caching expressions with column references would cause incorrect results
        // when evaluating across multiple rows with different column values
        let expr = vibesql_ast::Expression::ColumnRef { table: None, column: "col1".to_string() };
        assert!(!ExpressionHasher::is_deterministic(&expr));
    }

    #[test]
    fn test_current_timestamp_is_not_deterministic() {
        let expr = vibesql_ast::Expression::CurrentTimestamp { precision: None };
        assert!(!ExpressionHasher::is_deterministic(&expr));
    }

    #[test]
    fn test_rand_function_is_not_deterministic() {
        let expr = vibesql_ast::Expression::Function {
            name: "RAND".to_string(),
            args: vec![],
            character_unit: None,
        };
        assert!(!ExpressionHasher::is_deterministic(&expr));
    }

    #[test]
    fn test_deterministic_function_is_deterministic() {
        let arg = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42));
        let expr = vibesql_ast::Expression::Function {
            name: "ABS".to_string(),
            args: vec![arg],
            character_unit: None,
        };
        assert!(ExpressionHasher::is_deterministic(&expr));
    }
}
