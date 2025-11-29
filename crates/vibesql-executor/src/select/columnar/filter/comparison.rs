use vibesql_types::{Date, SqlValue};

/// Result of comparing two SqlValues, accounting for NULL semantics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompareResult {
    /// Normal ordering result
    Ordering(std::cmp::Ordering),
    /// At least one value is NULL - comparison is UNKNOWN
    Unknown,
}

impl CompareResult {
    /// Check if comparison result equals a specific ordering
    /// Returns false for Unknown (NULL comparisons always fail in WHERE)
    pub fn equals(&self, expected: std::cmp::Ordering) -> bool {
        match self {
            CompareResult::Ordering(ord) => *ord == expected,
            CompareResult::Unknown => false,
        }
    }

    /// Check if comparison result matches any of the given orderings
    /// Returns false for Unknown (NULL comparisons always fail in WHERE)
    pub fn matches(&self, orderings: &[std::cmp::Ordering]) -> bool {
        match self {
            CompareResult::Ordering(ord) => orderings.contains(ord),
            CompareResult::Unknown => false,
        }
    }
}

/// Compare two SqlValues for ordering
///
/// Handles both same-type and mixed numeric type comparisons by coercing to f64.
/// Returns CompareResult::Unknown if either value is NULL (per SQL standard).
pub(super) fn compare_values(a: &SqlValue, b: &SqlValue) -> CompareResult {
    use std::cmp::Ordering;

    // NULL handling: any comparison involving NULL returns UNKNOWN
    if matches!(a, SqlValue::Null) || matches!(b, SqlValue::Null) {
        return CompareResult::Unknown;
    }

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

    CompareResult::Ordering(match (a, b) {
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

        // Date-String comparisons: parse string to Date for native comparison
        // This handles cases like: date_column >= '1994-01-01'
        // Converting String→Date avoids per-row string allocation (vs Date→String)
        (SqlValue::Date(date), SqlValue::Varchar(s)) | (SqlValue::Date(date), SqlValue::Character(s)) => {
            // Parse string as YYYY-MM-DD and compare as Date
            if let Some(parsed_date) = parse_date_string(s) {
                date.cmp(&parsed_date)
            } else {
                // If parsing fails, fall back to string comparison
                let date_str = date.to_string();
                date_str.as_str().cmp(s.as_str())
            }
        }
        (SqlValue::Varchar(s), SqlValue::Date(date)) | (SqlValue::Character(s), SqlValue::Date(date)) => {
            // Parse string as YYYY-MM-DD and compare as Date
            if let Some(parsed_date) = parse_date_string(s) {
                parsed_date.cmp(date)
            } else {
                // If parsing fails, fall back to string comparison
                let date_str = date.to_string();
                s.as_str().cmp(date_str.as_str())
            }
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
    })
}

/// Parse a date string in YYYY-MM-DD format
///
/// Returns None if parsing fails, allowing callers to fall back to string comparison.
fn parse_date_string(s: &str) -> Option<Date> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    let month: u8 = parts[1].parse().ok()?;
    let day: u8 = parts[2].parse().ok()?;
    Date::new(year, month, day).ok()
}
