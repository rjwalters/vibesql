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
        (SqlValue::Float(a), SqlValue::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Double(a), SqlValue::Double(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Numeric(a), SqlValue::Numeric(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Real(a), SqlValue::Real(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Varchar(a), SqlValue::Varchar(b)) => a.cmp(b),
        (SqlValue::Character(a), SqlValue::Character(b)) => a.cmp(b),
        (SqlValue::Date(a), SqlValue::Date(b)) => a.cmp(b),

        // Date-String comparisons: parse string to Date for native comparison
        // This handles cases like: date_column >= '1994-01-01'
        // Converting String→Date avoids per-row string allocation (vs Date→String)
        (SqlValue::Date(date), SqlValue::Varchar(s))
        | (SqlValue::Date(date), SqlValue::Character(s)) => {
            // Parse string as YYYY-MM-DD and compare as Date
            if let Some(parsed_date) = parse_date_string(s) {
                date.cmp(&parsed_date)
            } else {
                // If parsing fails, fall back to string comparison
                let date_str = date.to_string();
                date_str.as_str().cmp(&**s)
            }
        }
        (SqlValue::Varchar(s), SqlValue::Date(date))
        | (SqlValue::Character(s), SqlValue::Date(date)) => {
            // Parse string as YYYY-MM-DD and compare as Date
            if let Some(parsed_date) = parse_date_string(s) {
                parsed_date.cmp(date)
            } else {
                // If parsing fails, fall back to string comparison
                let date_str = date.to_string();
                (**s).cmp(date_str.as_str())
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
/// Used by both scalar comparison and SIMD filtering paths.
pub(crate) fn parse_date_string(s: &str) -> Option<Date> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    let month: u8 = parts[1].parse().ok()?;
    let day: u8 = parts[2].parse().ok()?;
    Date::new(year, month, day).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test for issue #3360: Float column vs Integer literal comparison
    /// in the columnar filter path
    #[test]
    fn test_float_vs_integer_comparison() {
        let col_value = SqlValue::Float(678.28);
        let pred_value = SqlValue::Integer(85);

        let result = compare_values(&col_value, &pred_value);
        assert_eq!(
            result,
            CompareResult::Ordering(std::cmp::Ordering::Greater),
            "Float(678.28) should be > Integer(85)"
        );
    }

    #[test]
    fn test_float_vs_integer_less_than() {
        let col_value = SqlValue::Float(50.0);
        let pred_value = SqlValue::Integer(85);

        let result = compare_values(&col_value, &pred_value);
        assert_eq!(
            result,
            CompareResult::Ordering(std::cmp::Ordering::Less),
            "Float(50.0) should be < Integer(85)"
        );
    }

    /// Integration test for issue #3360: Full columnar filter path with Float column
    #[test]
    fn test_issue_3360_filter_float_column() {
        use super::super::{
            apply_columnar_filter, create_filter_bitmap, evaluate_predicate, ColumnPredicate,
        };
        use vibesql_storage::Row;

        // Reproduce the exact issue: FLOAT column with integer predicate
        let rows = vec![
            Row::new(vec![SqlValue::Integer(0), SqlValue::Float(678.28)]),
            Row::new(vec![SqlValue::Integer(1), SqlValue::Float(235.64)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Float(465.9)]),
        ];

        // Predicate: col4 > 85 (column_idx=1, which is the Float column)
        let predicates =
            vec![ColumnPredicate::GreaterThan { column_idx: 1, value: SqlValue::Integer(85) }];

        // Test direct evaluation
        for (i, row) in rows.iter().enumerate() {
            let value = row.get(1).unwrap();
            let result = evaluate_predicate(&predicates[0], value);
            assert!(result, "Row {} with value {:?} should pass > 85", i, value);
        }

        // Test bitmap creation
        let bitmap = create_filter_bitmap(rows.len(), &predicates, |row_idx, col_idx| {
            rows.get(row_idx).and_then(|row| row.get(col_idx))
        })
        .unwrap();

        assert_eq!(bitmap, vec![true, true, true], "All rows should pass filter");

        // Test apply_columnar_filter (the actual function used in execution)
        let indices = apply_columnar_filter(&rows, &predicates).unwrap();
        assert_eq!(indices.len(), 3, "All 3 rows should pass filter");
    }
}
