use vibesql_types::{Date, SqlValue};

/// Result of comparing two SqlValues, accounting for NULL semantics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompareResult {
    /// Normal ordering result
    Ordering(std::cmp::Ordering),
    /// At least one value is NULL - comparison is UNKNOWN
    Unknown,
    /// The two values have no defined ordering in this comparator (issue
    /// #5335). The old behavior returned `Ordering::Equal` here, which turned
    /// `=`, `<=`, `>=`, and BETWEEN into tautologies (every row passed) and
    /// `<`, `>`, `!=` into contradictions. Incomparable pairs now
    /// conservatively fail every predicate instead of lying about equality.
    /// Predicate extraction (`predicates.rs`) declines columnar pushdown for
    /// the type combinations known to land here, so the expression evaluator
    /// (with its full coercion/error semantics) handles them instead.
    Incomparable,
}

impl CompareResult {
    /// Check if comparison result equals a specific ordering
    /// Returns false for Unknown (NULL comparisons always fail in WHERE)
    /// and for Incomparable (no defined ordering - conservatively exclude)
    pub fn equals(&self, expected: std::cmp::Ordering) -> bool {
        match self {
            CompareResult::Ordering(ord) => *ord == expected,
            CompareResult::Unknown | CompareResult::Incomparable => false,
        }
    }

    /// Check if comparison result matches any of the given orderings
    /// Returns false for Unknown (NULL comparisons always fail in WHERE)
    /// and for Incomparable (no defined ordering - conservatively exclude)
    pub fn matches(&self, orderings: &[std::cmp::Ordering]) -> bool {
        match self {
            CompareResult::Ordering(ord) => orderings.contains(ord),
            CompareResult::Unknown | CompareResult::Incomparable => false,
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
            SqlValue::Unsigned(n) => Some(*n as f64),
            SqlValue::Float(n) => Some(*n as f64),
            SqlValue::Double(n) => Some(*n),
            SqlValue::Numeric(n) => n.to_string().parse().ok(),
            SqlValue::Real(n) => Some(*n as f64),
            // Booleans are integers (0/1) in SQLite storage semantics
            SqlValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    // Try to coerce a string value to a number (for SQLite NUMERIC affinity)
    fn string_to_f64(v: &SqlValue) -> Option<f64> {
        match v {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.trim().parse().ok(),
            _ => None,
        }
    }

    // Check if value is numeric type
    fn is_numeric(v: &SqlValue) -> bool {
        matches!(
            v,
            SqlValue::Integer(_)
                | SqlValue::Bigint(_)
                | SqlValue::Smallint(_)
                | SqlValue::Float(_)
                | SqlValue::Double(_)
                | SqlValue::Numeric(_)
                | SqlValue::Real(_)
        )
    }

    // Check if value is string type
    fn is_string(v: &SqlValue) -> bool {
        matches!(v, SqlValue::Varchar(_) | SqlValue::Character(_))
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
        // Mixed string types: Character vs Varchar (same underlying Arc<str>)
        (SqlValue::Varchar(a), SqlValue::Character(b))
        | (SqlValue::Character(a), SqlValue::Varchar(b)) => a.cmp(b),
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

        // Same-type temporal comparisons (issue #5335: these previously fell
        // through to the incomparable-types catch-all, which reported Equal
        // for every pair and made e.g. `ts = TIMESTAMP '...'` match all rows)
        (SqlValue::Timestamp(a), SqlValue::Timestamp(b)) => a.cmp(b),
        (SqlValue::Time(a), SqlValue::Time(b)) => a.cmp(b),

        // Timestamp vs string: compare the TEXT renderings lexicographically,
        // matching the expression evaluator semantics from #5329
        // (evaluator/operators/comparison/mod.rs). The Display rendering is
        // 'YYYY-MM-DD HH:MM:SS[.fff]', so for full canonical timestamp
        // strings lexicographic ordering equals temporal ordering; date-only
        // strings compare as text prefixes ('2017-07-08 00:00:00' >
        // '2017-07-08'); unparseable strings compare as text instead of
        // raising a type mismatch, like SQLite.
        (SqlValue::Timestamp(ts), SqlValue::Varchar(s))
        | (SqlValue::Timestamp(ts), SqlValue::Character(s)) => {
            ts.to_string().as_str().cmp(s.as_str())
        }
        (SqlValue::Varchar(s), SqlValue::Timestamp(ts))
        | (SqlValue::Character(s), SqlValue::Timestamp(ts)) => {
            s.as_str().cmp(ts.to_string().as_str())
        }

        // Time vs string: same TEXT-rendering approach as Timestamp
        (SqlValue::Time(t), SqlValue::Varchar(s)) | (SqlValue::Time(t), SqlValue::Character(s)) => {
            t.to_string().as_str().cmp(s.as_str())
        }
        (SqlValue::Varchar(s), SqlValue::Time(t)) | (SqlValue::Character(s), SqlValue::Time(t)) => {
            s.as_str().cmp(t.to_string().as_str())
        }

        // Blob vs Blob: bytewise comparison (SQLite memcmp semantics)
        (SqlValue::Blob(a), SqlValue::Blob(b)) => a.cmp(b),

        // Mixed numeric types: coerce to f64 with epsilon comparison for floats
        _ => {
            // First try direct numeric comparison
            if let (Some(a_f64), Some(b_f64)) = (to_f64(a), to_f64(b)) {
                // Use epsilon comparison for floating point values to handle precision issues
                // This is especially important for Float(0.07) vs Numeric(0.07) comparisons
                const EPSILON: f64 = 1e-9;
                if (a_f64 - b_f64).abs() < EPSILON {
                    return CompareResult::Ordering(Ordering::Equal);
                } else if a_f64 < b_f64 {
                    return CompareResult::Ordering(Ordering::Less);
                } else {
                    return CompareResult::Ordering(Ordering::Greater);
                }
            }

            // SQLite type affinity: numeric vs string comparisons
            // If one side is numeric and other is string that looks like a number,
            // try to coerce the string to a number and compare
            if is_numeric(a) && is_string(b) {
                if let (Some(a_f64), Some(b_f64)) = (to_f64(a), string_to_f64(b)) {
                    const EPSILON: f64 = 1e-9;
                    if (a_f64 - b_f64).abs() < EPSILON {
                        return CompareResult::Ordering(Ordering::Equal);
                    } else if a_f64 < b_f64 {
                        return CompareResult::Ordering(Ordering::Less);
                    } else {
                        return CompareResult::Ordering(Ordering::Greater);
                    }
                }
                // String can't be parsed as number - use SQLite type ordering:
                // INTEGER/REAL < TEXT (numeric is less than text)
                return CompareResult::Ordering(Ordering::Less);
            } else if is_string(a) && is_numeric(b) {
                if let (Some(a_f64), Some(b_f64)) = (string_to_f64(a), to_f64(b)) {
                    const EPSILON: f64 = 1e-9;
                    if (a_f64 - b_f64).abs() < EPSILON {
                        return CompareResult::Ordering(Ordering::Equal);
                    } else if a_f64 < b_f64 {
                        return CompareResult::Ordering(Ordering::Less);
                    } else {
                        return CompareResult::Ordering(Ordering::Greater);
                    }
                }
                // String can't be parsed as number - use SQLite type ordering:
                // TEXT > INTEGER/REAL (text is greater than numeric)
                return CompareResult::Ordering(Ordering::Greater);
            }

            // Non-comparable types: no defined ordering in this comparator.
            // Issue #5335: this used to return Ordering::Equal, which made
            // every equality/range predicate on such pairs a tautology or a
            // contradiction. Report Incomparable so all predicates
            // conservatively fail; predicate extraction declines pushdown for
            // these combinations so the expression evaluator handles them.
            return CompareResult::Incomparable;
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

    /// Test for issue #4684: Integer column vs String literal comparison
    /// SQLite should coerce string '2' to number 2 when comparing with numeric column
    #[test]
    fn test_integer_vs_string_comparison() {
        // Integer(2) should equal Varchar("2") after coercion
        let col_value = SqlValue::Integer(2);
        let pred_value = SqlValue::Varchar(arcstr::ArcStr::from("2"));

        let result = compare_values(&col_value, &pred_value);
        assert_eq!(
            result,
            CompareResult::Ordering(std::cmp::Ordering::Equal),
            "Integer(2) should == Varchar('2')"
        );

        // Integer(2) should be greater than Varchar("1")
        let pred_value_1 = SqlValue::Varchar(arcstr::ArcStr::from("1"));
        let result_gt = compare_values(&col_value, &pred_value_1);
        assert_eq!(
            result_gt,
            CompareResult::Ordering(std::cmp::Ordering::Greater),
            "Integer(2) should > Varchar('1')"
        );

        // Integer(2) should be less than Varchar("3")
        let pred_value_3 = SqlValue::Varchar(arcstr::ArcStr::from("3"));
        let result_lt = compare_values(&col_value, &pred_value_3);
        assert_eq!(
            result_lt,
            CompareResult::Ordering(std::cmp::Ordering::Less),
            "Integer(2) should < Varchar('3')"
        );
    }

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
        use vibesql_storage::Row;

        use super::super::{
            apply_columnar_filter, create_filter_bitmap, evaluate_predicate, ColumnPredicate,
        };

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

    /// Test for index-14.x: SQLite type ordering for TEXT vs INTEGER comparison
    /// When comparing TEXT that can't be parsed as a number with INTEGER,
    /// SQLite uses type ordering: INTEGER < TEXT
    #[test]
    fn test_text_vs_integer_type_ordering() {
        // Empty string can't be parsed as number - should use type ordering
        let text_empty = SqlValue::Varchar(arcstr::ArcStr::from(""));
        let integer = SqlValue::Integer(123);

        // TEXT > INTEGER in SQLite type ordering
        let result = compare_values(&text_empty, &integer);
        assert_eq!(
            result,
            CompareResult::Ordering(std::cmp::Ordering::Greater),
            "Empty string (TEXT) should be > Integer in SQLite type ordering"
        );

        // INTEGER < TEXT in SQLite type ordering
        let result_rev = compare_values(&integer, &text_empty);
        assert_eq!(
            result_rev,
            CompareResult::Ordering(std::cmp::Ordering::Less),
            "Integer should be < empty string (TEXT) in SQLite type ordering"
        );

        // Non-numeric string should also use type ordering
        let text_abc = SqlValue::Varchar(arcstr::ArcStr::from("abc"));
        let result_abc = compare_values(&text_abc, &integer);
        assert_eq!(
            result_abc,
            CompareResult::Ordering(std::cmp::Ordering::Greater),
            "'abc' (TEXT) should be > Integer in SQLite type ordering"
        );
    }

    fn ts(s: &str) -> SqlValue {
        use std::str::FromStr;
        SqlValue::Timestamp(vibesql_types::Timestamp::from_str(s).unwrap())
    }

    fn varchar(s: &str) -> SqlValue {
        SqlValue::Varchar(arcstr::ArcStr::from(s))
    }

    /// Issue #5335: Timestamp vs Timestamp must compare temporally, not fall
    /// through to the catch-all (which used to report Equal for every pair).
    #[test]
    fn test_timestamp_vs_timestamp_comparison() {
        use std::cmp::Ordering;
        let a = ts("2017-07-20 15:30:00");
        let b = ts("2017-07-22 08:00:00");

        assert_eq!(compare_values(&a, &b), CompareResult::Ordering(Ordering::Less));
        assert_eq!(compare_values(&b, &a), CompareResult::Ordering(Ordering::Greater));
        assert_eq!(compare_values(&a, &a), CompareResult::Ordering(Ordering::Equal));
    }

    /// Issue #5335: Timestamp vs string uses TEXT-rendering lexicographic
    /// comparison (#5329 semantics), never the catch-all.
    #[test]
    fn test_timestamp_vs_string_text_rendering() {
        use std::cmp::Ordering;
        let a = ts("2017-07-20 15:30:00");

        // Canonical full rendering: equality
        assert_eq!(
            compare_values(&a, &varchar("2017-07-20 15:30:00")),
            CompareResult::Ordering(Ordering::Equal)
        );
        // Unparseable string: text ordering ('2...' < 'zzz')
        assert_eq!(compare_values(&a, &varchar("zzz")), CompareResult::Ordering(Ordering::Less));
        assert_eq!(compare_values(&varchar("zzz"), &a), CompareResult::Ordering(Ordering::Greater));
        // Date-only string: rendering is longer with equal prefix, so greater
        assert_eq!(
            compare_values(&a, &varchar("2017-07-20")),
            CompareResult::Ordering(Ordering::Greater)
        );
        // Later date-only string: rendering sorts below it
        assert_eq!(
            compare_values(&a, &varchar("2017-07-21")),
            CompareResult::Ordering(Ordering::Less)
        );
    }

    /// Issue #5335: Time vs Time and Time vs string semantics.
    #[test]
    fn test_time_comparisons() {
        use std::{cmp::Ordering, str::FromStr};
        let t1 = SqlValue::Time(vibesql_types::Time::from_str("08:00:00").unwrap());
        let t2 = SqlValue::Time(vibesql_types::Time::from_str("15:30:00").unwrap());

        assert_eq!(compare_values(&t1, &t2), CompareResult::Ordering(Ordering::Less));
        assert_eq!(
            compare_values(&t1, &varchar("08:00:00")),
            CompareResult::Ordering(Ordering::Equal)
        );
        assert_eq!(compare_values(&t1, &varchar("zzz")), CompareResult::Ordering(Ordering::Less));
    }

    /// Issue #5335: the catch-all must no longer report incomparable pairs as
    /// Equal (which made `=`/`<=`/`>=`/BETWEEN tautologies).
    #[test]
    fn test_incomparable_types_not_equal() {
        use std::cmp::Ordering;
        let a = ts("2017-07-20 15:30:00");
        let date = SqlValue::Date(vibesql_types::Date::new(2017, 7, 20).unwrap());

        // Timestamp vs Date has no defined ordering in this comparator
        let result = compare_values(&a, &date);
        assert_eq!(result, CompareResult::Incomparable);
        assert!(!result.equals(Ordering::Equal));
        assert!(!result.matches(&[Ordering::Less, Ordering::Greater]));
        assert!(!result.matches(&[Ordering::Less, Ordering::Equal]));
    }

    /// Issue #5335: Boolean and Blob same-type pairs used to hit the Equal
    /// catch-all too; verify they now compare properly.
    #[test]
    fn test_boolean_and_blob_comparisons() {
        use std::cmp::Ordering;
        assert_eq!(
            compare_values(&SqlValue::Boolean(false), &SqlValue::Boolean(true)),
            CompareResult::Ordering(Ordering::Less)
        );
        assert_eq!(
            compare_values(&SqlValue::Boolean(true), &SqlValue::Boolean(true)),
            CompareResult::Ordering(Ordering::Equal)
        );
        assert_eq!(
            compare_values(&SqlValue::Blob(vec![0x61, 0x62]), &SqlValue::Blob(vec![0x61, 0x63])),
            CompareResult::Ordering(Ordering::Less)
        );
    }

    /// Test evaluate_predicate with text vs integer
    #[test]
    fn test_evaluate_predicate_text_vs_integer() {
        use super::super::evaluation::evaluate_predicate;
        use super::super::predicates::ColumnPredicate;

        // GreaterThan predicate: column > 123
        let predicate =
            ColumnPredicate::GreaterThan { column_idx: 0, value: SqlValue::Integer(123) };

        // Column value is 'abc' (text)
        let value = SqlValue::Varchar(arcstr::ArcStr::from("abc"));
        let result = evaluate_predicate(&predicate, &value);
        assert!(result, "Varchar('abc') should be > Integer(123)");

        // Column value is empty string
        let empty = SqlValue::Varchar(arcstr::ArcStr::from(""));
        let result_empty = evaluate_predicate(&predicate, &empty);
        assert!(result_empty, "Varchar('') should be > Integer(123)");
    }
}
