// ============================================================================
// Value Normalization - Canonical forms for index operations
// ============================================================================

use std::borrow::Cow;

use vibesql_types::SqlValue;

/// Normalize a SqlValue to a consistent numeric type for comparison in range scans.
/// This ensures that Real, Numeric, Float, Double, Integer, Smallint, Bigint, and Unsigned
/// values can be compared correctly regardless of their underlying type.
///
/// IMPORTANT: This function is also used at index insertion time to normalize all numeric values
/// to a canonical form (Double) before storing in the BTreeMap. This ensures that queries
/// comparing different numeric types (e.g., Real > Numeric) work correctly.
///
/// Uses f64 (Double) instead of f32 (Real) to preserve precision for:
/// - Large integers (Bigint, Unsigned) beyond f32 precision range (> 2^24 ≈ 16 million)
/// - High-precision floating point values (Double, Numeric)
pub fn normalize_for_comparison(value: &SqlValue) -> SqlValue {
    match value {
        SqlValue::Integer(i) => SqlValue::Double(*i as f64),
        SqlValue::Smallint(i) => SqlValue::Double(*i as f64),
        SqlValue::Bigint(i) => SqlValue::Double(*i as f64),
        SqlValue::Unsigned(u) => SqlValue::Double(*u as f64),
        SqlValue::Float(f) => SqlValue::Double(*f as f64),
        SqlValue::Real(r) => SqlValue::Double(*r as f64),
        SqlValue::Double(d) => SqlValue::Double(*d),
        SqlValue::Numeric(n) => SqlValue::Double(*n),
        // For non-numeric types, return as-is
        other => other.clone(),
    }
}

/// Zero-copy normalization using Cow - avoids allocation for non-numeric values.
/// Returns Borrowed for non-numeric types (no clone), Owned for normalized numerics.
#[inline]
pub fn normalize_cow(value: &SqlValue) -> Cow<'_, SqlValue> {
    match value {
        SqlValue::Integer(i) => Cow::Owned(SqlValue::Double(*i as f64)),
        SqlValue::Smallint(i) => Cow::Owned(SqlValue::Double(*i as f64)),
        SqlValue::Bigint(i) => Cow::Owned(SqlValue::Double(*i as f64)),
        SqlValue::Unsigned(u) => Cow::Owned(SqlValue::Double(*u as f64)),
        SqlValue::Float(f) => Cow::Owned(SqlValue::Double(*f as f64)),
        SqlValue::Real(r) => Cow::Owned(SqlValue::Double(*r as f64)),
        SqlValue::Double(d) => Cow::Owned(SqlValue::Double(*d)),
        SqlValue::Numeric(n) => Cow::Owned(SqlValue::Double(*n)),
        // For non-numeric types, borrow without clone
        other => Cow::Borrowed(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_for_comparison_numeric_types() {
        // All numeric types should normalize to Double
        assert_eq!(normalize_for_comparison(&SqlValue::Integer(42)), SqlValue::Double(42.0));
        assert_eq!(normalize_for_comparison(&SqlValue::Smallint(10)), SqlValue::Double(10.0));
        assert_eq!(normalize_for_comparison(&SqlValue::Bigint(1000)), SqlValue::Double(1000.0));
        assert_eq!(normalize_for_comparison(&SqlValue::Unsigned(99)), SqlValue::Double(99.0));
        assert_eq!(
            normalize_for_comparison(&SqlValue::Float(3.14)),
            SqlValue::Double(3.14f32 as f64)
        );
        assert_eq!(normalize_for_comparison(&SqlValue::Real(2.5)), SqlValue::Double(2.5));
        assert_eq!(normalize_for_comparison(&SqlValue::Numeric(123.45)), SqlValue::Double(123.45));
        assert_eq!(normalize_for_comparison(&SqlValue::Double(7.89)), SqlValue::Double(7.89));
    }

    #[test]
    fn test_normalize_for_comparison_non_numeric() {
        // Non-numeric types should be returned as-is
        let text_val = SqlValue::Varchar(arcstr::ArcStr::from("test"));
        assert_eq!(normalize_for_comparison(&text_val), text_val);

        let null_val = SqlValue::Null;
        assert_eq!(normalize_for_comparison(&null_val), null_val);

        let bool_val = SqlValue::Boolean(true);
        assert_eq!(normalize_for_comparison(&bool_val), bool_val);
    }
}
