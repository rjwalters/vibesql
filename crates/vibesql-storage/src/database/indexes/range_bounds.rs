// ============================================================================
// Range Bounds - Value increment logic for range operations
// ============================================================================

use vibesql_types::SqlValue;

/// Calculate the next value after a given SqlValue for use as an exclusive upper bound
/// in prefix matching range scans.
///
/// For numeric types, this returns value + 1. For other types, returns None (unbounded).
/// This is used in DiskBacked index prefix matching to efficiently bound the range scan.
///
/// # Examples
/// - Double(10.0) → Some(Double(11.0))
/// - Integer(42) → Some(Integer(43))
/// - Varchar("abc") → None (no natural successor)
pub fn calculate_next_value(value: &SqlValue) -> Option<SqlValue> {
    match value {
        SqlValue::Double(d) => {
            // For doubles, add 1.0. Note: This works for integers represented as doubles
            // which is the normalized form used in indexes.
            Some(SqlValue::Double(d + 1.0))
        }
        SqlValue::Integer(i) => Some(SqlValue::Integer(i + 1)),
        SqlValue::Smallint(i) => Some(SqlValue::Smallint(i + 1)),
        SqlValue::Bigint(i) => Some(SqlValue::Bigint(i + 1)),
        SqlValue::Unsigned(u) => Some(SqlValue::Unsigned(u + 1)),
        SqlValue::Float(f) => Some(SqlValue::Float(f + 1.0)),
        SqlValue::Real(r) => Some(SqlValue::Real(r + 1.0)),
        SqlValue::Numeric(n) => Some(SqlValue::Numeric(n + 1.0)),
        // For non-numeric types, we can't easily calculate a successor
        // Return None to indicate unbounded end
        _ => None,
    }
}

/// Try to increment a SqlValue by the smallest possible amount
/// Returns None if the value is already at its maximum or increment is not supported
///
/// This is used to calculate upper bounds for BTreeMap ranges when doing prefix matching
/// on multi-column indexes. For example, to find all keys where first column <= 90,
/// we can use range(lower..Excluded([91])) instead of range(lower..Unbounded) + manual check.
pub fn try_increment_sqlvalue(value: &SqlValue) -> Option<SqlValue> {
    match value {
        // Integer types: increment by 1, handle overflow
        SqlValue::Integer(i) if *i < i64::MAX => Some(SqlValue::Integer(i + 1)),
        SqlValue::Smallint(i) if *i < i16::MAX => Some(SqlValue::Smallint(i + 1)),
        SqlValue::Bigint(i) if *i < i64::MAX => Some(SqlValue::Bigint(i + 1)),
        SqlValue::Unsigned(u) if *u < u64::MAX => Some(SqlValue::Unsigned(u + 1)),

        // Floating point: increment by smallest representable step
        // For floats, we use nextafter functionality via adding epsilon
        SqlValue::Float(f) if f.is_finite() => {
            // Special case: For zero and very small values, abs() * EPSILON may be 0 or too small.
            // Use f32::MIN_POSITIVE as a fallback minimum increment.
            let increment = (f.abs() * f32::EPSILON).max(f32::MIN_POSITIVE);
            let next = f + increment;
            if next > *f && next.is_finite() {
                Some(SqlValue::Float(next))
            } else {
                None
            }
        }
        SqlValue::Real(f) if f.is_finite() => {
            // Special case: For zero and very small values, abs() * EPSILON may be 0 or too small.
            // Use f32::MIN_POSITIVE as a fallback minimum increment.
            let increment = (f.abs() * f32::EPSILON).max(f32::MIN_POSITIVE);
            let next = f + increment;
            if next > *f && next.is_finite() {
                Some(SqlValue::Real(next))
            } else {
                None
            }
        }
        SqlValue::Double(f) if f.is_finite() => {
            // Special case: For zero and very small values, abs() * EPSILON may be 0 or too small.
            // Use f64::MIN_POSITIVE as a fallback minimum increment.
            let increment = (f.abs() * f64::EPSILON).max(f64::MIN_POSITIVE);
            let next = f + increment;
            if next > *f && next.is_finite() {
                Some(SqlValue::Double(next))
            } else {
                None
            }
        }
        SqlValue::Numeric(f) if f.is_finite() => {
            // Special case: For zero and very small values, abs() * EPSILON may be 0 or too small.
            // Use f64::MIN_POSITIVE as a fallback minimum increment.
            let increment = (f.abs() * f64::EPSILON).max(f64::MIN_POSITIVE);
            let next = f + increment;
            if next > *f && next.is_finite() {
                Some(SqlValue::Numeric(next))
            } else {
                None
            }
        }

        // String types: append a null character to get the next string
        // This works because "\0" is the smallest character
        SqlValue::Varchar(s) => Some(SqlValue::Varchar(arcstr::ArcStr::from(format!("{}\0", s).as_str()))),
        SqlValue::Character(s) => Some(SqlValue::Character(arcstr::ArcStr::from(format!("{}\0", s).as_str()))),

        // Boolean: false < true, so true has no next value
        SqlValue::Boolean(false) => Some(SqlValue::Boolean(true)),
        SqlValue::Boolean(true) => None,

        // Null is always smallest, so it has a next value (any non-null)
        // But we don't have a clear "next" value, so return None
        SqlValue::Null => None,

        // Date/Time types: for now, return None (could implement increment by smallest unit)
        SqlValue::Date(_) | SqlValue::Time(_) | SqlValue::Timestamp(_) | SqlValue::Interval(_) => {
            None
        }

        // All other cases (overflow, already at max, etc.): return None
        _ => None,
    }
}

/// Increment the last element of a prefix key to get a key just past the prefix range
///
/// This is used for unbounded upper range scans - to find all keys with a given prefix,
/// we scan from `[prefix]` to `[prefix_incremented]` (exclusive).
///
/// # Example
/// ```text
/// // Find all keys starting with [1, 2]
/// // We scan from Included([1, 2]) to Excluded([1, 3])
/// let upper = try_increment_sqlvalue_prefix(&vec![SqlValue::Integer(1), SqlValue::Integer(2)]);
/// // Returns Some([SqlValue::Integer(1), SqlValue::Integer(3)])
/// ```
pub fn try_increment_sqlvalue_prefix(prefix: &[SqlValue]) -> Option<Vec<SqlValue>> {
    if prefix.is_empty() {
        return None;
    }
    let mut result = prefix.to_vec();
    let last_idx = result.len() - 1;
    match try_increment_sqlvalue(&result[last_idx]) {
        Some(next_val) => {
            result[last_idx] = next_val;
            Some(result)
        }
        None => None,
    }
}

/// Smart increment that chooses the right strategy based on SQL type
///
/// For actual integer SQL types (Integer, Smallint, Bigint, Unsigned), adds 1.
/// For floating-point SQL types (Float, Real, Double, Numeric), adds epsilon.
///
/// This is needed because multi-column index range scans convert exclusive bounds
/// to inclusive by incrementing the value. For floating-point types, we must add
/// epsilon (not 1.0) because fractional values can exist between any two integers.
/// For example, with FLOAT column: 114.0 < 114.5 < 114.86 < 115.0
/// If we add 1.0 to 114.0 → 115.0, we'd miss rows with values like 114.5, 114.86.
pub fn smart_increment_value(value: &SqlValue) -> Option<SqlValue> {
    match value {
        // Floating-point SQL types: always use epsilon increment
        // Even if the value looks like an integer (114.0), the column type
        // allows fractional values, so we can't skip to the next integer
        SqlValue::Double(_) | SqlValue::Numeric(_) | SqlValue::Float(_) | SqlValue::Real(_) => {
            try_increment_sqlvalue(value)
        }
        // Integer SQL types: use +1 increment (only integers are possible)
        SqlValue::Integer(_)
        | SqlValue::Smallint(_)
        | SqlValue::Bigint(_)
        | SqlValue::Unsigned(_) => calculate_next_value(value),
        // For other types, use calculate_next_value as fallback
        _ => calculate_next_value(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_next_value_double() {
        assert_eq!(calculate_next_value(&SqlValue::Double(10.0)), Some(SqlValue::Double(11.0)));
        assert_eq!(calculate_next_value(&SqlValue::Double(-5.5)), Some(SqlValue::Double(-4.5)));
        assert_eq!(calculate_next_value(&SqlValue::Double(0.0)), Some(SqlValue::Double(1.0)));
    }

    #[test]
    fn test_calculate_next_value_integer() {
        assert_eq!(calculate_next_value(&SqlValue::Integer(42)), Some(SqlValue::Integer(43)));
        assert_eq!(calculate_next_value(&SqlValue::Integer(-10)), Some(SqlValue::Integer(-9)));
        assert_eq!(calculate_next_value(&SqlValue::Integer(0)), Some(SqlValue::Integer(1)));
    }

    #[test]
    fn test_calculate_next_value_smallint() {
        assert_eq!(calculate_next_value(&SqlValue::Smallint(100)), Some(SqlValue::Smallint(101)));
        assert_eq!(calculate_next_value(&SqlValue::Smallint(-1)), Some(SqlValue::Smallint(0)));
    }

    #[test]
    fn test_calculate_next_value_bigint() {
        assert_eq!(
            calculate_next_value(&SqlValue::Bigint(1_000_000_000)),
            Some(SqlValue::Bigint(1_000_000_001))
        );
        assert_eq!(calculate_next_value(&SqlValue::Bigint(-999)), Some(SqlValue::Bigint(-998)));
    }

    #[test]
    fn test_calculate_next_value_unsigned() {
        assert_eq!(calculate_next_value(&SqlValue::Unsigned(0)), Some(SqlValue::Unsigned(1)));
        assert_eq!(calculate_next_value(&SqlValue::Unsigned(999)), Some(SqlValue::Unsigned(1000)));
    }

    #[test]
    fn test_calculate_next_value_float() {
        // Use simpler values for f32 to avoid precision issues
        assert_eq!(calculate_next_value(&SqlValue::Float(3.0)), Some(SqlValue::Float(4.0)));
        assert_eq!(calculate_next_value(&SqlValue::Float(-2.5)), Some(SqlValue::Float(-1.5)));
    }

    #[test]
    fn test_calculate_next_value_real() {
        assert_eq!(calculate_next_value(&SqlValue::Real(7.5)), Some(SqlValue::Real(8.5)));
        assert_eq!(calculate_next_value(&SqlValue::Real(0.0)), Some(SqlValue::Real(1.0)));
    }

    #[test]
    fn test_calculate_next_value_numeric() {
        assert_eq!(
            calculate_next_value(&SqlValue::Numeric(99.99)),
            Some(SqlValue::Numeric(100.99))
        );
        assert_eq!(calculate_next_value(&SqlValue::Numeric(-10.5)), Some(SqlValue::Numeric(-9.5)));
    }

    #[test]
    fn test_calculate_next_value_non_numeric() {
        // Text types
        assert_eq!(calculate_next_value(&SqlValue::Varchar(arcstr::ArcStr::from("abc"))), None);
        assert_eq!(calculate_next_value(&SqlValue::Character(arcstr::ArcStr::from("hello"))), None);

        // NULL
        assert_eq!(calculate_next_value(&SqlValue::Null), None);

        // Boolean
        assert_eq!(calculate_next_value(&SqlValue::Boolean(true)), None);
        assert_eq!(calculate_next_value(&SqlValue::Boolean(false)), None);
    }

    #[test]
    fn test_try_increment_sqlvalue_zero() {
        // Test that zero values are correctly incremented (issue #3359)
        // This was a bug where Double(0.0) would return None because
        // 0.0 * EPSILON = 0.0, causing the increment check to fail.

        // Double zero should increment to MIN_POSITIVE
        let result = try_increment_sqlvalue(&SqlValue::Double(0.0));
        assert!(result.is_some(), "Double(0.0) should increment");
        if let Some(SqlValue::Double(d)) = result {
            assert!(d > 0.0, "Incremented value should be positive");
        }

        // Float zero should increment
        let result = try_increment_sqlvalue(&SqlValue::Float(0.0));
        assert!(result.is_some(), "Float(0.0) should increment");
        if let Some(SqlValue::Float(f)) = result {
            assert!(f > 0.0, "Incremented value should be positive");
        }

        // Real zero should increment
        let result = try_increment_sqlvalue(&SqlValue::Real(0.0));
        assert!(result.is_some(), "Real(0.0) should increment");
        if let Some(SqlValue::Real(r)) = result {
            assert!(r > 0.0, "Incremented value should be positive");
        }

        // Numeric zero should increment
        let result = try_increment_sqlvalue(&SqlValue::Numeric(0.0));
        assert!(result.is_some(), "Numeric(0.0) should increment");
        if let Some(SqlValue::Numeric(n)) = result {
            assert!(n > 0.0, "Incremented value should be positive");
        }

        // Integer zero should increment to 1
        assert_eq!(try_increment_sqlvalue(&SqlValue::Integer(0)), Some(SqlValue::Integer(1)));
    }
}
