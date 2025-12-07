//! Validation and conversion utilities for ALTER TABLE operations

use vibesql_ast::Expression;
use vibesql_types::{DataType, SqlValue};

use crate::errors::ExecutorError;

/// Evaluate a simple default expression (literals and basic expressions)
/// For more complex expressions, this would need full evaluation context
pub(super) fn evaluate_simple_default(expr: &Expression) -> Result<SqlValue, ExecutorError> {
    match expr {
        Expression::Literal(val) => Ok(val.clone()),
        Expression::UnaryOp { op, expr } => {
            let val = evaluate_simple_default(expr)?;
            match op {
                vibesql_ast::UnaryOperator::Not => match val {
                    SqlValue::Boolean(b) => Ok(SqlValue::Boolean(!b)),
                    SqlValue::Null => Ok(SqlValue::Null),
                    _ => Err(ExecutorError::TypeMismatch {
                        left: val,
                        op: "NOT".to_string(),
                        right: SqlValue::Null,
                    }),
                },
                vibesql_ast::UnaryOperator::Minus => match val {
                    SqlValue::Integer(i) => Ok(SqlValue::Integer(-i)),
                    SqlValue::Numeric(d) => Ok(SqlValue::Numeric(-d)),
                    SqlValue::Null => Ok(SqlValue::Null),
                    _ => Err(ExecutorError::TypeMismatch {
                        left: val,
                        op: "-".to_string(),
                        right: SqlValue::Null,
                    }),
                },
                _ => Err(ExecutorError::UnsupportedExpression(format!(
                    "Unsupported unary operator in default: {:?}",
                    op
                ))),
            }
        }
        _ => Err(ExecutorError::UnsupportedExpression(
            "Complex expressions in DEFAULT not yet supported. Use simple literals.".to_string(),
        )),
    }
}

/// Check if type conversion is safe (widening conversions only)
pub(super) fn is_type_conversion_safe(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        // Same type is always safe
        (a, b) if a == b => true,

        // Integer widenings
        (DataType::Integer, DataType::Bigint) => true,
        (DataType::Smallint, DataType::Integer) => true,
        (DataType::Smallint, DataType::Bigint) => true,

        // Numeric widenings
        (DataType::Integer, DataType::Numeric { .. }) => true,
        (DataType::Smallint, DataType::Numeric { .. }) => true,
        (DataType::Bigint, DataType::Numeric { .. }) => true,
        (
            DataType::Numeric { precision: p1, scale: s1 },
            DataType::Numeric { precision: p2, scale: s2 },
        ) => p2 >= p1 && s2 >= s1,

        // String widenings
        (DataType::Character { length: n1 }, DataType::Character { length: n2 }) => n2 >= n1,
        (
            DataType::Varchar { max_length: Some(n1) },
            DataType::Varchar { max_length: Some(n2) },
        ) => n2 >= n1,
        (DataType::Varchar { max_length: Some(_) }, DataType::Varchar { max_length: None }) => true,
        (DataType::Character { .. }, DataType::Varchar { .. }) => true,
        (DataType::Character { .. }, DataType::CharacterLargeObject) => true,
        (DataType::Varchar { .. }, DataType::CharacterLargeObject) => true,

        // Any to CLOB (always safe, just convert to string)
        (DataType::Integer, DataType::CharacterLargeObject) => true,
        (DataType::Bigint, DataType::CharacterLargeObject) => true,
        (DataType::Smallint, DataType::CharacterLargeObject) => true,
        (DataType::Numeric { .. }, DataType::CharacterLargeObject) => true,
        (DataType::Boolean, DataType::CharacterLargeObject) => true,

        // Otherwise, reject
        _ => false,
    }
}

/// Convert a value to a new type
pub(super) fn convert_value(
    value: SqlValue,
    target_type: &DataType,
) -> Result<SqlValue, ExecutorError> {
    // NULL stays NULL
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    match target_type {
        DataType::Integer => match &value {
            SqlValue::Integer(i) => Ok(SqlValue::Integer(*i)),
            SqlValue::Smallint(s) => Ok(SqlValue::Integer(*s as i64)),
            SqlValue::Bigint(b) => Ok(SqlValue::Integer(*b)), // May truncate
            SqlValue::Varchar(s) | SqlValue::Character(s) => s
                .parse::<i64>()
                .map(SqlValue::Integer)
                .map_err(|_| ExecutorError::TypeConversionError {
                    from: format!("{:?}", value),
                    to: "INTEGER".to_string(),
                }),
            _ => Err(ExecutorError::TypeConversionError {
                from: format!("{:?}", value),
                to: "INTEGER".to_string(),
            }),
        },
        DataType::Bigint => match &value {
            SqlValue::Integer(i) => Ok(SqlValue::Bigint(*i)),
            SqlValue::Smallint(s) => Ok(SqlValue::Bigint(*s as i64)),
            SqlValue::Bigint(b) => Ok(SqlValue::Bigint(*b)),
            _ => Err(ExecutorError::TypeConversionError {
                from: format!("{:?}", value),
                to: "BIGINT".to_string(),
            }),
        },
        DataType::Smallint => match &value {
            SqlValue::Smallint(s) => Ok(SqlValue::Smallint(*s)),
            SqlValue::Integer(i) => Ok(SqlValue::Smallint(*i as i16)), // May truncate
            _ => Err(ExecutorError::TypeConversionError {
                from: format!("{:?}", value),
                to: "SMALLINT".to_string(),
            }),
        },
        DataType::Numeric { .. } | DataType::Decimal { .. } => match &value {
            SqlValue::Numeric(d) => Ok(SqlValue::Numeric(*d)),
            SqlValue::Integer(i) => Ok(SqlValue::Numeric(*i as f64)),
            SqlValue::Smallint(s) => Ok(SqlValue::Numeric(*s as f64)),
            SqlValue::Bigint(b) => Ok(SqlValue::Numeric(*b as f64)),
            _ => Err(ExecutorError::TypeConversionError {
                from: format!("{:?}", value),
                to: "NUMERIC".to_string(),
            }),
        },
        DataType::Varchar { .. } | DataType::Character { .. } | DataType::CharacterLargeObject => {
            // Convert anything to string
            match value {
                SqlValue::Varchar(s) | SqlValue::Character(s) => Ok(SqlValue::Varchar(s)),
                SqlValue::Integer(i) => Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string()))),
                SqlValue::Bigint(b) => Ok(SqlValue::Varchar(arcstr::ArcStr::from(b.to_string()))),
                SqlValue::Smallint(s) => Ok(SqlValue::Varchar(arcstr::ArcStr::from(s.to_string()))),
                SqlValue::Numeric(d) => Ok(SqlValue::Varchar(arcstr::ArcStr::from(d.to_string()))),
                SqlValue::Boolean(b) => Ok(SqlValue::Varchar(arcstr::ArcStr::from(b.to_string()))),
                _ => Ok(SqlValue::Varchar(arcstr::ArcStr::from(format!("{:?}", value)))),
            }
        }
        _ => Err(ExecutorError::TypeConversionError {
            from: format!("{:?}", value),
            to: format!("{:?}", target_type),
        }),
    }
}
