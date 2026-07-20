//! Validation and conversion utilities for ALTER TABLE operations

use vibesql_ast::Expression;
use vibesql_catalog::{IndexMetadata, TableSchema};
use vibesql_types::{DataType, SqlValue};

use crate::errors::ExecutorError;

/// Whether `name` refers to SQLite's schema table (`sqlite_master` /
/// `sqlite_schema`, and the temp variants), which may not be altered. Matched
/// case-insensitively, mirroring SQLite's identifier folding.
pub(super) fn is_sqlite_schema_table(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    matches!(
        n.as_str(),
        "sqlite_master" | "sqlite_schema" | "sqlite_temp_master" | "sqlite_temp_schema"
    )
}

/// Whether `column` participates in any UNIQUE constraint of `schema`
/// (case-insensitive). Used to reject `DROP COLUMN` of a UNIQUE column, matching
/// SQLite's `cannot drop UNIQUE column` rejection.
pub(super) fn column_in_unique_constraint(schema: &TableSchema, column: &str) -> bool {
    schema.unique_constraints.iter().any(|cols| cols.iter().any(|c| c.eq_ignore_ascii_case(column)))
}

/// Whether `index` references `column` — either directly (a plain indexed
/// column) or transitively through an expression (functional) index. Used to
/// reject a `DROP COLUMN` that would leave an index dangling, matching SQLite's
/// `error in index <name> after drop column: no such column: <col>`.
pub(super) fn index_references_column(index: &IndexMetadata, column: &str) -> bool {
    let referenced_in_columns = index.columns.iter().any(|ic| {
        if let Some(name) = ic.column_name() {
            name.eq_ignore_ascii_case(column)
        } else if let Some(expr) = ic.get_expression() {
            expression_references_column(expr, column)
        } else {
            false
        }
    });
    if referenced_in_columns {
        return true;
    }
    // A partial-index predicate can also reference the column.
    index.where_clause.as_ref().is_some_and(|expr| expression_references_column(expr, column))
}

/// Recursively test whether `expr` contains a reference to the (unqualified)
/// column `column`, comparing case-insensitively. Conservative: expression
/// shapes not enumerated here return `false` (they cannot reference the column
/// through a form this checker understands).
pub(super) fn expression_references_column(expr: &Expression, column: &str) -> bool {
    match expr {
        Expression::ColumnRef(col_id) => col_id.column_canonical().eq_ignore_ascii_case(column),
        Expression::BinaryOp { left, right, .. }
        | Expression::IsDistinctFrom { left, right, .. } => {
            expression_references_column(left, column)
                || expression_references_column(right, column)
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            children.iter().any(|c| expression_references_column(c, column))
        }
        Expression::UnaryOp { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::IsTruthValue { expr, .. } => expression_references_column(expr, column),
        Expression::Function { args, .. } => {
            args.iter().any(|a| expression_references_column(a, column))
        }
        Expression::AggregateFunction { args, filter, .. } => {
            args.iter().any(|a| expression_references_column(a, column))
                || filter.as_ref().is_some_and(|f| expression_references_column(f, column))
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|o| expression_references_column(o, column))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(|c| expression_references_column(c, column))
                        || expression_references_column(&w.result, column)
                })
                || else_result.as_ref().is_some_and(|e| expression_references_column(e, column))
        }
        _ => false,
    }
}

/// Evaluate a simple default expression (literals and constant expressions).
///
/// This is the DEFAULT materializer for `ALTER TABLE ... ADD COLUMN ... DEFAULT
/// <expr>`. It delegates to the canonical `evaluate_default_expression` so that
/// ALTER and CREATE/INSERT share one set of SQLite-accurate DEFAULT semantics —
/// unary +/- (including string-to-number coercion, `DEFAULT -'0xFF'` → 0),
/// logical/bitwise NOT, and constant scalar functions — rather than the weaker
/// literals-and-negation-only subset this used to reimplement (literal.test
/// 1.9/1.13, which exercise `+0x7FFFFFFFFFFFFFFF` and `-'0xFF'` as ALTER
/// defaults).
pub(super) fn evaluate_simple_default(expr: &Expression) -> Result<SqlValue, ExecutorError> {
    crate::insert::defaults::evaluate_default_expression(expr)
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
