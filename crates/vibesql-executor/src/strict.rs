//! STRICT table enforcement (SQLite compatibility).
//!
//! Implements the datatype rules for SQLite STRICT tables
//! (<https://sqlite.org/stricttables.html>):
//!
//! - **DDL**: every column of a STRICT table must declare exactly one of the six allowed datatypes
//!   — `INT`, `INTEGER`, `REAL`, `TEXT`, `BLOB`, `ANY` — with no size/precision specifier. A
//!   missing type is `missing datatype for <tbl>.<col>`; any other spelling (including `TEXT(50)`)
//!   is `unknown datatype for <tbl>.<col>: "<text>"`.
//! - **DML**: on INSERT/UPDATE the supplied value must match the column's strict type after the
//!   documented lossless coercions, otherwise `cannot store <VALUE_CLASS> value in <COLUMN_KEYWORD>
//!   column <tbl>.<col>`.
//!
//! The DDL layer (CREATE TABLE / ALTER TABLE ADD COLUMN) lives in
//! `create_table.rs` / `alter.rs`; this module provides the shared helpers.

use vibesql_catalog::StrictType;
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Validate the declared datatypes of every column of a STRICT table and return
/// the per-column [`StrictType`] classification (parallel to `columns`).
///
/// Errors with the verbatim SQLite messages for a missing or unknown datatype.
pub fn classify_strict_columns(
    table_name: &str,
    columns: &[vibesql_ast::ColumnDef],
) -> Result<Vec<StrictType>, ExecutorError> {
    let mut types = Vec::with_capacity(columns.len());
    for col in columns {
        types.push(classify_strict_column(table_name, &col.name, col.type_source.as_deref())?);
    }
    Ok(types)
}

/// Validate a single column's declared datatype for a STRICT table.
pub fn classify_strict_column(
    table_name: &str,
    column_name: &str,
    type_source: Option<&str>,
) -> Result<StrictType, ExecutorError> {
    match type_source {
        None => Err(ExecutorError::SqliteCompatError(format!(
            "missing datatype for {}.{}",
            table_name, column_name
        ))),
        Some(declared) => StrictType::classify(declared).ok_or_else(|| {
            ExecutorError::SqliteCompatError(format!(
                "unknown datatype for {}.{}: \"{}\"",
                table_name, column_name, declared
            ))
        }),
    }
}

/// The SQLite storage-class name of `value` as it appears in the STRICT
/// `cannot store <CLASS> value in ...` error message.
fn value_class_name(value: &SqlValue) -> &'static str {
    match value {
        SqlValue::Integer(_)
        | SqlValue::Smallint(_)
        | SqlValue::Bigint(_)
        | SqlValue::Unsigned(_)
        | SqlValue::Boolean(_) => "INT",
        SqlValue::Real(_) | SqlValue::Float(_) | SqlValue::Double(_) | SqlValue::Numeric(_) => {
            "REAL"
        }
        SqlValue::Character(_) | SqlValue::Varchar(_) => "TEXT",
        SqlValue::Blob(_) | SqlValue::Vector(_) => "BLOB",
        // Temporal types have no native SQLite storage class; they surface as
        // TEXT to the strict checker (they never coerce into a strict column
        // other than ANY, so the exact label only appears in an error).
        SqlValue::Date(_) | SqlValue::Time(_) | SqlValue::Timestamp(_) | SqlValue::Interval(_) => {
            "TEXT"
        }
        SqlValue::Null => "NULL",
    }
}

fn mismatch(value: &SqlValue, st: StrictType, table: &str, col: &str) -> ExecutorError {
    ExecutorError::SqliteCompatError(format!(
        "cannot store {} value in {} column {}.{}",
        value_class_name(value),
        st.keyword(),
        table,
        col
    ))
}

/// Coerce/validate `value` for storage in a STRICT column of type `st`.
///
/// Returns the (possibly coerced) value to store, or a `cannot store ...`
/// error. NULL is always accepted. `ANY` accepts and stores everything as-is.
pub fn enforce_strict_type(
    value: SqlValue,
    st: StrictType,
    table: &str,
    col: &str,
) -> Result<SqlValue, ExecutorError> {
    if value.is_null() {
        return Ok(value);
    }
    match st {
        // ANY imposes no constraint and performs no coercion.
        StrictType::Any => Ok(value),
        StrictType::Int | StrictType::Integer => coerce_to_int(value, st, table, col),
        StrictType::Real => coerce_to_real(value, st, table, col),
        StrictType::Text => coerce_to_text(value, st, table, col),
        StrictType::Blob => coerce_to_blob(value, st, table, col),
    }
}

/// Is `f` an integer value representable in `i64` without loss?
fn real_is_lossless_int(f: f64) -> Option<i64> {
    if f.is_finite() && f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
        Some(f as i64)
    } else {
        None
    }
}

fn coerce_to_int(
    value: SqlValue,
    st: StrictType,
    table: &str,
    col: &str,
) -> Result<SqlValue, ExecutorError> {
    match &value {
        SqlValue::Integer(i) => Ok(SqlValue::Integer(*i)),
        SqlValue::Bigint(i) => Ok(SqlValue::Integer(*i)),
        SqlValue::Smallint(i) => Ok(SqlValue::Integer(*i as i64)),
        SqlValue::Unsigned(u) => Ok(SqlValue::Integer(*u as i64)),
        SqlValue::Boolean(b) => Ok(SqlValue::Integer(*b as i64)),
        // A REAL is accepted only when it has no fractional part (lossless).
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => real_is_lossless_int(*f)
            .map(SqlValue::Integer)
            .ok_or_else(|| mismatch(&value, st, table, col)),
        SqlValue::Float(f) => real_is_lossless_int(*f as f64)
            .map(SqlValue::Integer)
            .ok_or_else(|| mismatch(&value, st, table, col)),
        // A TEXT value is accepted only if it losslessly represents an integer.
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            let t = s.trim();
            if let Ok(i) = t.parse::<i64>() {
                Ok(SqlValue::Integer(i))
            } else if let Some(i) = t.parse::<f64>().ok().and_then(real_is_lossless_int) {
                Ok(SqlValue::Integer(i))
            } else {
                Err(mismatch(&value, st, table, col))
            }
        }
        _ => Err(mismatch(&value, st, table, col)),
    }
}

fn coerce_to_real(
    value: SqlValue,
    st: StrictType,
    table: &str,
    col: &str,
) -> Result<SqlValue, ExecutorError> {
    match &value {
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => Ok(SqlValue::Real(*f)),
        SqlValue::Float(f) => Ok(SqlValue::Real(*f as f64)),
        SqlValue::Integer(i) | SqlValue::Bigint(i) => Ok(SqlValue::Real(*i as f64)),
        SqlValue::Smallint(i) => Ok(SqlValue::Real(*i as f64)),
        SqlValue::Unsigned(u) => Ok(SqlValue::Real(*u as f64)),
        SqlValue::Boolean(b) => Ok(SqlValue::Real(*b as i64 as f64)),
        // TEXT accepted only if it losslessly represents a number.
        SqlValue::Varchar(s) | SqlValue::Character(s) => match s.trim().parse::<f64>() {
            Ok(f) => Ok(SqlValue::Real(f)),
            Err(_) => Err(mismatch(&value, st, table, col)),
        },
        _ => Err(mismatch(&value, st, table, col)),
    }
}

fn coerce_to_text(
    value: SqlValue,
    st: StrictType,
    table: &str,
    col: &str,
) -> Result<SqlValue, ExecutorError> {
    match &value {
        SqlValue::Varchar(_) => Ok(value),
        SqlValue::Character(s) => Ok(SqlValue::Varchar(s.clone())),
        // Integers and reals coerce to their SQLite text representation.
        SqlValue::Integer(_)
        | SqlValue::Bigint(_)
        | SqlValue::Smallint(_)
        | SqlValue::Unsigned(_)
        | SqlValue::Real(_)
        | SqlValue::Double(_)
        | SqlValue::Numeric(_)
        | SqlValue::Float(_)
        | SqlValue::Boolean(_) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(value.to_string().as_str())))
        }
        _ => Err(mismatch(&value, st, table, col)),
    }
}

fn coerce_to_blob(
    value: SqlValue,
    st: StrictType,
    table: &str,
    col: &str,
) -> Result<SqlValue, ExecutorError> {
    match &value {
        SqlValue::Blob(_) => Ok(value),
        _ => Err(mismatch(&value, st, table, col)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v_ok(value: SqlValue, st: StrictType) -> SqlValue {
        enforce_strict_type(value, st, "t", "c").unwrap()
    }

    fn v_err(value: SqlValue, st: StrictType) -> String {
        match enforce_strict_type(value, st, "t", "c") {
            Err(ExecutorError::SqliteCompatError(m)) => m,
            other => panic!("expected strict error, got {:?}", other),
        }
    }

    #[test]
    fn null_always_accepted() {
        for st in [
            StrictType::Int,
            StrictType::Integer,
            StrictType::Real,
            StrictType::Text,
            StrictType::Blob,
            StrictType::Any,
        ] {
            assert!(matches!(v_ok(SqlValue::Null, st), SqlValue::Null));
        }
    }

    #[test]
    fn any_passes_everything_unchanged() {
        assert!(matches!(v_ok(SqlValue::Blob(vec![1, 2]), StrictType::Any), SqlValue::Blob(_)));
        assert!(matches!(v_ok(SqlValue::Integer(7), StrictType::Any), SqlValue::Integer(7)));
        assert!(matches!(
            v_ok(SqlValue::Varchar("x".into()), StrictType::Any),
            SqlValue::Varchar(_)
        ));
    }

    #[test]
    fn int_column_coercions() {
        assert!(matches!(v_ok(SqlValue::Integer(1), StrictType::Int), SqlValue::Integer(1)));
        // Lossless TEXT -> INT
        assert!(matches!(
            v_ok(SqlValue::Varchar("3".into()), StrictType::Int),
            SqlValue::Integer(3)
        ));
        // Lossless REAL -> INT
        assert!(matches!(v_ok(SqlValue::Real(6.0), StrictType::Integer), SqlValue::Integer(6)));
        // Fractional REAL rejected
        assert_eq!(
            v_err(SqlValue::Real(1.2), StrictType::Int),
            "cannot store REAL value in INT column t.c"
        );
        // Non-numeric TEXT rejected
        assert_eq!(
            v_err(SqlValue::Varchar("xyz".into()), StrictType::Int),
            "cannot store TEXT value in INT column t.c"
        );
        // BLOB rejected
        assert_eq!(
            v_err(SqlValue::Blob(vec![1]), StrictType::Int),
            "cannot store BLOB value in INT column t.c"
        );
    }

    #[test]
    fn real_column_coercions() {
        assert!(
            matches!(v_ok(SqlValue::Integer(1), StrictType::Real), SqlValue::Real(f) if f == 1.0)
        );
        assert!(
            matches!(v_ok(SqlValue::Varchar("3".into()), StrictType::Real), SqlValue::Real(f) if f == 3.0)
        );
        assert!(
            matches!(v_ok(SqlValue::Varchar("4.5".into()), StrictType::Real), SqlValue::Real(f) if f == 4.5)
        );
        assert_eq!(
            v_err(SqlValue::Varchar("xyz".into()), StrictType::Real),
            "cannot store TEXT value in REAL column t.c"
        );
        assert_eq!(
            v_err(SqlValue::Blob(vec![1]), StrictType::Real),
            "cannot store BLOB value in REAL column t.c"
        );
    }

    #[test]
    fn text_column_coercions() {
        assert!(matches!(
            v_ok(SqlValue::Varchar("x".into()), StrictType::Text),
            SqlValue::Varchar(_)
        ));
        assert_eq!(v_ok(SqlValue::Integer(4), StrictType::Text).to_string(), "4");
        assert_eq!(v_ok(SqlValue::Real(5.5), StrictType::Text).to_string(), "5.5");
        assert_eq!(
            v_err(SqlValue::Blob(vec![1]), StrictType::Text),
            "cannot store BLOB value in TEXT column t.c"
        );
    }

    #[test]
    fn blob_column_only_accepts_blob() {
        assert!(matches!(v_ok(SqlValue::Blob(vec![1, 2, 3]), StrictType::Blob), SqlValue::Blob(_)));
        assert_eq!(
            v_err(SqlValue::Integer(2), StrictType::Blob),
            "cannot store INT value in BLOB column t.c"
        );
        assert_eq!(
            v_err(SqlValue::Real(1.5), StrictType::Blob),
            "cannot store REAL value in BLOB column t.c"
        );
        assert_eq!(
            v_err(SqlValue::Varchar("x".into()), StrictType::Blob),
            "cannot store TEXT value in BLOB column t.c"
        );
    }

    #[test]
    fn classify_reports_missing_and_unknown() {
        let missing = classify_strict_column("t1", "a", None);
        assert!(
            matches!(missing, Err(ExecutorError::SqliteCompatError(m)) if m == "missing datatype for t1.a")
        );
        let unknown = classify_strict_column("t1", "f", Some("TEXT(50)"));
        assert!(
            matches!(unknown, Err(ExecutorError::SqliteCompatError(m)) if m == "unknown datatype for t1.f: \"TEXT(50)\"")
        );
        assert_eq!(
            classify_strict_column("t1", "a", Some("integer")).unwrap(),
            StrictType::Integer
        );
        assert_eq!(classify_strict_column("t1", "a", Some("any")).unwrap(), StrictType::Any);
    }
}
