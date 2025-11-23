use vibesql_types::SqlValue;

/// Compare two SqlValues for ordering
///
/// Handles both same-type and mixed numeric type comparisons by coercing to f64
pub(super) fn compare_values(a: &SqlValue, b: &SqlValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    // Try to extract numeric value as f64 for cross-type comparison
    fn to_f64(v: &SqlValue) -> Option<f64> {
        match v {
            SqlValue::Integer(n) => Some(*n as f64),
            SqlValue::Bigint(n) => Some(*n as f64),
            SqlValue::Smallint(n) => Some(*n as f64),
            SqlValue::Float(n) => Some(*n as f64),
            SqlValue::Double(n) => Some(*n),
            SqlValue::Numeric(n) => n.to_string().parse().ok(),
            SqlValue::Real(n) => Some(*n as f64),
            _ => None,
        }
    }

    match (a, b) {
        // Same-type comparisons (fast path)
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
        (SqlValue::Bigint(a), SqlValue::Bigint(b)) => a.cmp(b),
        (SqlValue::Smallint(a), SqlValue::Smallint(b)) => a.cmp(b),
        (SqlValue::Float(a), SqlValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Double(a), SqlValue::Double(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Numeric(a), SqlValue::Numeric(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Real(a), SqlValue::Real(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Varchar(a), SqlValue::Varchar(b)) => a.cmp(b),
        (SqlValue::Character(a), SqlValue::Character(b)) => a.cmp(b),
        (SqlValue::Date(a), SqlValue::Date(b)) => a.cmp(b),

        // Date-String comparisons: convert string to date format for comparison
        // This handles cases like: date_column >= '1994-01-01'
        (SqlValue::Date(date), SqlValue::Varchar(s)) | (SqlValue::Date(date), SqlValue::Character(s)) => {
            // Compare dates as strings in ISO format (YYYY-MM-DD)
            let date_str = date.to_string();
            date_str.as_str().cmp(s.as_str())
        }
        (SqlValue::Varchar(s), SqlValue::Date(date)) | (SqlValue::Character(s), SqlValue::Date(date)) => {
            // Reverse comparison
            let date_str = date.to_string();
            s.as_str().cmp(date_str.as_str())
        }

        // Mixed numeric types: coerce to f64 with epsilon comparison for floats
        _ => {
            if let (Some(a_f64), Some(b_f64)) = (to_f64(a), to_f64(b)) {
                // Use epsilon comparison for floating point values to handle precision issues
                // This is especially important for Float(0.07) vs Numeric(0.07) comparisons
                const EPSILON: f64 = 1e-9;
                if (a_f64 - b_f64).abs() < EPSILON {
                    Ordering::Equal
                } else if a_f64 < b_f64 {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            } else {
                // Non-numeric mixed types: fall back to Equal (will fail predicate appropriately)
                Ordering::Equal
            }
        }
    }
}
