//! Type conversions between Python and Rust SqlValue types
//!
//! This module handles bidirectional conversion of values between Python
//! and Rust SqlValue types, supporting DB-API 2.0 conventions.

use arcstr::ArcStr;
use pyo3::{prelude::*, types::PyTuple};

use crate::ProgrammingError;

/// Converts a Rust SqlValue to a Python object
///
/// Maps each SqlValue variant to the appropriate Python type:
/// - Integers → int
/// - Floats → float
/// - Strings → str
/// - Boolean → bool
/// - Temporal types → str (ISO format)
/// - Null → None
pub fn sqlvalue_to_py(py: Python, value: &vibesql_types::SqlValue) -> PyResult<Py<PyAny>> {
    Ok(match value {
        vibesql_types::SqlValue::Integer(i) => (*i).into_pyobject(py)?.into_any().unbind(),
        vibesql_types::SqlValue::Smallint(i) => (*i).into_pyobject(py)?.into_any().unbind(),
        vibesql_types::SqlValue::Bigint(i) => (*i).into_pyobject(py)?.into_any().unbind(),
        vibesql_types::SqlValue::Unsigned(u) => (*u).into_pyobject(py)?.into_any().unbind(),
        vibesql_types::SqlValue::Float(f) => (*f as f64).into_pyobject(py)?.into_any().unbind(),
        vibesql_types::SqlValue::Real(f) => (*f as f64).into_pyobject(py)?.into_any().unbind(),
        vibesql_types::SqlValue::Double(f) => (*f).into_pyobject(py)?.into_any().unbind(),
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            s.into_pyobject(py)?.into_any().unbind()
        }
        vibesql_types::SqlValue::Boolean(b) => b.into_pyobject(py)?.to_owned().into_any().unbind(),
        vibesql_types::SqlValue::Numeric(n) => n.into_pyobject(py)?.into_any().unbind(),
        vibesql_types::SqlValue::Date(d) => d.to_string().into_pyobject(py)?.into_any().unbind(),
        vibesql_types::SqlValue::Time(t) => t.to_string().into_pyobject(py)?.into_any().unbind(),
        vibesql_types::SqlValue::Timestamp(ts) => {
            ts.to_string().into_pyobject(py)?.into_any().unbind()
        }
        vibesql_types::SqlValue::Interval(i) => {
            i.to_string().into_pyobject(py)?.into_any().unbind()
        }
        vibesql_types::SqlValue::Vector(v) => {
            // Convert vector to Python list of floats
            pyo3::types::PyList::new(py, v.iter().map(|f| *f as f64))?.into_any().unbind()
        }
        vibesql_types::SqlValue::Null => py.None(),
    })
}

/// Converts a Python object to a Rust SqlValue
///
/// This is the inverse of sqlvalue_to_py() and supports DB-API 2.0 type conversion.
/// Attempts type conversion in order of likelihood:
/// 1. None → Null
/// 2. Integer types (with range checking for appropriate bit width)
/// 3. Float types
/// 4. String types
/// 5. Boolean types
///
/// # Errors
///
/// Returns ProgrammingError if the Python type cannot be converted to any SqlValue variant.
pub fn py_to_sqlvalue(_py: Python, obj: &Bound<'_, PyAny>) -> PyResult<vibesql_types::SqlValue> {
    // Check for None (SQL NULL)
    if obj.is_none() {
        return Ok(vibesql_types::SqlValue::Null);
    }

    // Try to extract Python types in order of likelihood
    // 1. Integer types
    if let Ok(val) = obj.extract::<i64>() {
        // Check range to determine which integer type to use
        if val >= i16::MIN as i64 && val <= i16::MAX as i64 {
            return Ok(vibesql_types::SqlValue::Smallint(val as i16));
        } else if val >= i32::MIN as i64 && val <= i32::MAX as i64 {
            return Ok(vibesql_types::SqlValue::Integer(val));
        } else {
            return Ok(vibesql_types::SqlValue::Bigint(val));
        }
    }

    // 2. Float types
    if let Ok(val) = obj.extract::<f64>() {
        return Ok(vibesql_types::SqlValue::Double(val));
    }

    // 3. String types
    if let Ok(val) = obj.extract::<String>() {
        return Ok(vibesql_types::SqlValue::Varchar(ArcStr::from(val)));
    }

    // 4. Boolean types
    if let Ok(val) = obj.extract::<bool>() {
        return Ok(vibesql_types::SqlValue::Boolean(val));
    }

    // If no type matched, return an error
    let type_name =
        obj.get_type().name().map(|s| s.to_string()).unwrap_or_else(|_| "unknown".to_string());
    Err(ProgrammingError::new_err(format!(
        "Cannot convert Python type '{}' to SQL value",
        type_name
    )))
}

/// Convert Python parameters to SQL values
///
/// Converts a Python tuple of parameters into a vector of SqlValue objects
/// for SQL substitution. Validates parameter types and returns errors for
/// unsupported types.
///
/// # Errors
///
/// Returns ProgrammingError if any parameter has an invalid type.
pub fn convert_params_to_sql_values(
    py: Python,
    params: &Bound<'_, PyTuple>,
) -> PyResult<Vec<vibesql_types::SqlValue>> {
    let mut sql_values = Vec::new();
    for i in 0..params.len() {
        let py_obj = params.get_item(i)?;
        let sql_value = py_to_sqlvalue(py, &py_obj).map_err(|e| {
            ProgrammingError::new_err(format!(
                "Parameter at position {} has invalid type: {}",
                i, e
            ))
        })?;
        sql_values.push(sql_value);
    }
    Ok(sql_values)
}

/// Substitute placeholders in SQL with actual values
///
/// Replaces ? placeholders in the SQL string with SQL literal values
/// from the provided vector. Handles proper escaping for strings and
/// correct formatting for all SQL types.
///
/// # Example
/// ```text
/// // Input: "SELECT * FROM users WHERE id = ?"
/// // Values: [Integer(42)]
/// // Output: "SELECT * FROM users WHERE id = 42"
/// ```
pub fn substitute_placeholders(sql: &str, sql_values: &[vibesql_types::SqlValue]) -> String {
    let mut result = String::new();
    let mut param_idx = 0;

    for ch in sql.chars() {
        if ch == '?' {
            // Replace ? with the corresponding parameter value as SQL literal
            if param_idx < sql_values.len() {
                let value_str = match &sql_values[param_idx] {
                    vibesql_types::SqlValue::Integer(i) => i.to_string(),
                    vibesql_types::SqlValue::Smallint(i) => i.to_string(),
                    vibesql_types::SqlValue::Bigint(i) => i.to_string(),
                    vibesql_types::SqlValue::Unsigned(u) => u.to_string(),
                    vibesql_types::SqlValue::Float(f) => f.to_string(),
                    vibesql_types::SqlValue::Real(f) => f.to_string(),
                    vibesql_types::SqlValue::Double(f) => f.to_string(),
                    vibesql_types::SqlValue::Numeric(n) => n.to_string(),
                    vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                        // Escape single quotes by doubling them (SQL standard)
                        format!("'{}'", s.replace('\'', "''"))
                    }
                    vibesql_types::SqlValue::Boolean(b) => {
                        if *b { "TRUE" } else { "FALSE" }.to_string()
                    }
                    vibesql_types::SqlValue::Date(s) => format!("DATE '{}'", s),
                    vibesql_types::SqlValue::Time(s) => format!("TIME '{}'", s),
                    vibesql_types::SqlValue::Timestamp(s) => format!("TIMESTAMP '{}'", s),
                    vibesql_types::SqlValue::Interval(s) => format!("INTERVAL '{}'", s),
                    vibesql_types::SqlValue::Vector(v) => {
                        let formatted: Vec<String> = v.iter().map(|f| f.to_string()).collect();
                        format!("[{}]", formatted.join(", "))
                    }
                    vibesql_types::SqlValue::Null => "NULL".to_string(),
                };
                result.push_str(&value_str);
                param_idx += 1;
            }
        } else {
            result.push(ch);
        }
    }

    result
}
