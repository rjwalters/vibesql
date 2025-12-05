use std::{cmp::Ordering, collections::HashSet};

/// Accumulator for aggregate functions
#[derive(Debug, Clone)]
pub enum AggregateAccumulator {
    Count {
        count: i64,
        distinct: bool,
        seen: Option<HashSet<vibesql_types::SqlValue>>,
    },
    Sum {
        sum: vibesql_types::SqlValue,
        count: i64,
        distinct: bool,
        seen: Option<HashSet<vibesql_types::SqlValue>>,
    },
    Avg {
        sum: vibesql_types::SqlValue,
        count: i64,
        distinct: bool,
        seen: Option<HashSet<vibesql_types::SqlValue>>,
    },
    Min {
        value: Option<vibesql_types::SqlValue>,
        distinct: bool,
        seen: Option<HashSet<vibesql_types::SqlValue>>,
    },
    Max {
        value: Option<vibesql_types::SqlValue>,
        distinct: bool,
        seen: Option<HashSet<vibesql_types::SqlValue>>,
    },
}

impl AggregateAccumulator {
    pub fn new(function_name: &str, distinct: bool) -> Result<Self, crate::errors::ExecutorError> {
        let seen = if distinct { Some(HashSet::new()) } else { None };
        match function_name.to_uppercase().as_str() {
            "COUNT" => Ok(AggregateAccumulator::Count { count: 0, distinct, seen }),
            "SUM" => Ok(AggregateAccumulator::Sum {
                sum: vibesql_types::SqlValue::Integer(0),
                count: 0,
                distinct,
                seen,
            }),
            "AVG" => Ok(AggregateAccumulator::Avg {
                sum: vibesql_types::SqlValue::Integer(0),
                count: 0,
                distinct,
                seen,
            }),
            "MIN" => Ok(AggregateAccumulator::Min { value: None, distinct, seen }),
            "MAX" => Ok(AggregateAccumulator::Max { value: None, distinct, seen }),
            _ => Err(crate::errors::ExecutorError::UnsupportedExpression(format!(
                "Unknown aggregate function: {}",
                function_name
            ))),
        }
    }

    pub fn accumulate(&mut self, value: &vibesql_types::SqlValue) {
        match self {
            // COUNT - counts non-NULL values
            AggregateAccumulator::Count { ref mut count, distinct, seen } => {
                if value.is_null() {
                    return; // Skip NULL values
                }

                if *distinct {
                    // Only count if we haven't seen this value before
                    // Optimization: Check membership before cloning
                    let seen_set = seen.as_mut().unwrap();
                    if !seen_set.contains(value) {
                        seen_set.insert(value.clone());
                        *count += 1;
                    }
                } else {
                    *count += 1;
                }
            }

            // SUM - sums numeric values (all numeric types), ignores NULLs
            AggregateAccumulator::Sum { ref mut sum, ref mut count, distinct, seen } => {
                // Fast path: Skip non-numeric values early
                if value.is_null() || !is_numeric_value(value) {
                    return;
                }

                if *distinct {
                    // Only sum if we haven't seen this value before
                    // Optimization: Check membership before cloning
                    let seen_set = seen.as_mut().unwrap();
                    if !seen_set.contains(value) {
                        seen_set.insert(value.clone());
                        *sum = add_sql_values(sum, value);
                        *count += 1;
                    }
                } else {
                    *sum = add_sql_values(sum, value);
                    *count += 1;
                }
            }

            // AVG - computes average of numeric values (all numeric types), ignores NULLs
            AggregateAccumulator::Avg { ref mut sum, ref mut count, distinct, seen } => {
                // Fast path: Skip non-numeric values early
                if value.is_null() || !is_numeric_value(value) {
                    return;
                }

                if *distinct {
                    // Only include if we haven't seen this value before
                    // Optimization: Check membership before cloning
                    let seen_set = seen.as_mut().unwrap();
                    if !seen_set.contains(value) {
                        seen_set.insert(value.clone());
                        *sum = add_sql_values(sum, value);
                        *count += 1;
                    }
                } else {
                    *sum = add_sql_values(sum, value);
                    *count += 1;
                }
            }

            // MIN - finds minimum value, ignores NULLs
            AggregateAccumulator::Min { value: ref mut current_min, distinct, seen } => {
                if value.is_null() || !is_comparable_value(value) {
                    return; // Skip NULL and unsupported types
                }

                // For MIN with DISTINCT, check if we've seen this value
                if *distinct {
                    let seen_set = seen.as_mut().unwrap();
                    if seen_set.contains(value) {
                        return; // Already seen this value
                    }
                    seen_set.insert(value.clone());
                }

                // Update minimum if needed
                if let Some(ref current) = current_min {
                    if compare_sql_values(value, current) == Ordering::Less {
                        *current_min = Some(value.clone());
                    }
                } else {
                    *current_min = Some(value.clone());
                }
            }

            // MAX - finds maximum value, ignores NULLs
            AggregateAccumulator::Max { value: ref mut current_max, distinct, seen } => {
                if value.is_null() || !is_comparable_value(value) {
                    return; // Skip NULL and unsupported types
                }

                // For MAX with DISTINCT, check if we've seen this value
                if *distinct {
                    let seen_set = seen.as_mut().unwrap();
                    if seen_set.contains(value) {
                        return; // Already seen this value
                    }
                    seen_set.insert(value.clone());
                }

                // Update maximum if needed
                if let Some(ref current) = current_max {
                    if compare_sql_values(value, current) == Ordering::Greater {
                        *current_max = Some(value.clone());
                    }
                } else {
                    *current_max = Some(value.clone());
                }
            }
        }
    }

    pub fn finalize(&self) -> vibesql_types::SqlValue {
        match self {
            AggregateAccumulator::Count { count, .. } => vibesql_types::SqlValue::Integer(*count),
            AggregateAccumulator::Sum { sum, count, .. } => {
                if *count == 0 {
                    vibesql_types::SqlValue::Null
                } else {
                    sum.clone()
                }
            }
            AggregateAccumulator::Avg { sum, count, .. } => {
                if *count == 0 {
                    vibesql_types::SqlValue::Null
                } else {
                    divide_sql_value(sum, *count)
                }
            }
            AggregateAccumulator::Min { value, .. } => {
                value.clone().unwrap_or(vibesql_types::SqlValue::Null)
            }
            AggregateAccumulator::Max { value, .. } => {
                value.clone().unwrap_or(vibesql_types::SqlValue::Null)
            }
        }
    }

    /// Combine two accumulators (for parallel aggregation)
    ///
    /// This method is used during the merge phase of parallel aggregation to combine
    /// thread-local accumulators into a final result. Each aggregate type has specific
    /// combination semantics:
    ///
    /// - COUNT: Sum the counts (or merge seen sets for DISTINCT)
    /// - SUM: Add the sums (or merge seen sets for DISTINCT)
    /// - AVG: Combine sums and counts (or merge seen sets for DISTINCT)
    /// - MIN: Take minimum of minimums
    /// - MAX: Take maximum of maximums
    #[allow(dead_code)]
    pub fn combine(&mut self, other: Self) -> Result<(), crate::errors::ExecutorError> {
        match (self, other) {
            // COUNT: Sum the counts
            (
                AggregateAccumulator::Count { count: c1, distinct: d1, seen: s1 },
                AggregateAccumulator::Count { count: c2, distinct: d2, seen: s2 },
            ) => {
                if *d1 != d2 {
                    return Err(crate::errors::ExecutorError::UnsupportedExpression(
                        "Cannot combine COUNT with different DISTINCT flags".into(),
                    ));
                }

                if *d1 {
                    // DISTINCT: Merge seen sets
                    if let (Some(seen1), Some(seen2)) = (s1, s2) {
                        seen1.extend(seen2);
                        *c1 = seen1.len() as i64;
                    }
                } else {
                    *c1 += c2;
                }
            }

            // SUM: Add the sums
            (
                AggregateAccumulator::Sum { sum: s1, count: c1, distinct: d1, seen: seen1 },
                AggregateAccumulator::Sum { sum: s2, count: c2, distinct: d2, seen: seen2 },
            ) => {
                if *d1 != d2 {
                    return Err(crate::errors::ExecutorError::UnsupportedExpression(
                        "Cannot combine SUM with different DISTINCT flags".into(),
                    ));
                }

                if *d1 {
                    // DISTINCT: Merge seen sets, recalculate sum
                    if let (Some(s1_set), Some(s2_set)) = (seen1, seen2) {
                        s1_set.extend(s2_set);
                        // Recalculate sum and count from merged set
                        *s1 =
                            s1_set.iter().fold(vibesql_types::SqlValue::Integer(0), |acc, val| {
                                add_sql_values(&acc, val)
                            });
                        *c1 = s1_set.len() as i64;
                    }
                } else {
                    *s1 = add_sql_values(s1, &s2);
                    *c1 += c2;
                }
            }

            // AVG: Combine sums and counts
            (
                AggregateAccumulator::Avg { sum: s1, count: c1, distinct: d1, seen: seen1 },
                AggregateAccumulator::Avg { sum: s2, count: c2, distinct: d2, seen: seen2 },
            ) => {
                if *d1 != d2 {
                    return Err(crate::errors::ExecutorError::UnsupportedExpression(
                        "Cannot combine AVG with different DISTINCT flags".into(),
                    ));
                }

                if *d1 {
                    // DISTINCT: Merge seen sets, recalculate
                    if let (Some(s1_set), Some(s2_set)) = (seen1, seen2) {
                        s1_set.extend(s2_set);
                        // Recalculate sum and count from merged set
                        *s1 =
                            s1_set.iter().fold(vibesql_types::SqlValue::Integer(0), |acc, val| {
                                add_sql_values(&acc, val)
                            });
                        *c1 = s1_set.len() as i64;
                    }
                } else {
                    *s1 = add_sql_values(s1, &s2);
                    *c1 += c2;
                }
            }

            // MIN: Take minimum of minimums
            (
                AggregateAccumulator::Min { value: v1, distinct: d1, seen: seen1 },
                AggregateAccumulator::Min { value: v2, distinct: d2, seen: seen2 },
            ) => {
                if *d1 != d2 {
                    return Err(crate::errors::ExecutorError::UnsupportedExpression(
                        "Cannot combine MIN with different DISTINCT flags".into(),
                    ));
                }

                if *d1 {
                    // DISTINCT: Merge seen sets, find minimum from merged set
                    if let (Some(s1_set), Some(s2_set)) = (seen1, seen2) {
                        s1_set.extend(s2_set);
                        // Find minimum from merged set
                        *v1 = s1_set.iter().min_by(|a, b| compare_sql_values(a, b)).cloned();
                    }
                } else {
                    match (v1.as_ref(), v2) {
                        (Some(current), Some(new_val)) => {
                            if compare_sql_values(&new_val, current) == Ordering::Less {
                                *v1 = Some(new_val);
                            }
                        }
                        (None, Some(new_val)) => *v1 = Some(new_val),
                        _ => {}
                    }
                }
            }

            // MAX: Take maximum of maximums
            (
                AggregateAccumulator::Max { value: v1, distinct: d1, seen: seen1 },
                AggregateAccumulator::Max { value: v2, distinct: d2, seen: seen2 },
            ) => {
                if *d1 != d2 {
                    return Err(crate::errors::ExecutorError::UnsupportedExpression(
                        "Cannot combine MAX with different DISTINCT flags".into(),
                    ));
                }

                if *d1 {
                    // DISTINCT: Merge seen sets, find maximum from merged set
                    if let (Some(s1_set), Some(s2_set)) = (seen1, seen2) {
                        s1_set.extend(s2_set);
                        // Find maximum from merged set
                        *v1 = s1_set.iter().max_by(|a, b| compare_sql_values(a, b)).cloned();
                    }
                } else {
                    match (v1.as_ref(), v2) {
                        (Some(current), Some(new_val)) => {
                            if compare_sql_values(&new_val, current) == Ordering::Greater {
                                *v1 = Some(new_val);
                            }
                        }
                        (None, Some(new_val)) => *v1 = Some(new_val),
                        _ => {}
                    }
                }
            }

            _ => {
                return Err(crate::errors::ExecutorError::UnsupportedExpression(
                    "Cannot combine incompatible aggregate types".into(),
                ));
            }
        }

        Ok(())
    }
}

/// Add two SqlValues together, handling all numeric types with type coercion to Numeric
///
/// **Design Decision**: Always returns Numeric (f64) for aggregate operations
///
/// This behavior was established in commit 0aa09d8a (#871) to align with SQLLogicTest
/// expectations, which requires NUMERIC return types for aggregate functions.
///
/// **SQL Standard Notes**:
/// - Different databases handle SUM return types differently:
///   - PostgreSQL: SUM(INTEGER) → BIGINT
///   - MySQL: SUM(INTEGER) → DECIMAL
///   - SQL Server: SUM(INTEGER) → INTEGER
///   - Oracle: Same type as input
/// - SQLLogicTest (the canonical SQL conformance suite) expects NUMERIC
/// - This choice prevents integer overflow and aligns with SQLLogicTest requirements
///
/// See: https://github.com/rjwalters/vibesql/pull/871
fn add_sql_values(
    a: &vibesql_types::SqlValue,
    b: &vibesql_types::SqlValue,
) -> vibesql_types::SqlValue {
    // Use the proper arithmetic addition operator that preserves types
    // Integer + Integer → Integer, Float + anything → Float, etc.
    use crate::evaluator::operators::OperatorRegistry;
    use vibesql_ast::BinaryOperator;

    match OperatorRegistry::eval_binary_op(
        a,
        &BinaryOperator::Plus,
        b,
        vibesql_types::SqlMode::default(),
    ) {
        Ok(result) => result,
        Err(_) => vibesql_types::SqlValue::Null, // If addition fails, return NULL
    }
}

/// Convert SqlValue to f64 for numeric operations
fn sql_value_to_f64(value: &vibesql_types::SqlValue) -> Option<f64> {
    match value {
        vibesql_types::SqlValue::Integer(x) => Some(*x as f64),
        vibesql_types::SqlValue::Smallint(x) => Some(*x as f64),
        vibesql_types::SqlValue::Bigint(x) => Some(*x as f64),
        vibesql_types::SqlValue::Numeric(x) => Some(*x),
        vibesql_types::SqlValue::Float(x) => Some(*x as f64),
        vibesql_types::SqlValue::Real(x) => Some(*x as f64),
        vibesql_types::SqlValue::Double(x) => Some(*x),
        _ => None,
    }
}

/// Fast check if a value is numeric (optimization to avoid full match)
#[inline]
fn is_numeric_value(value: &vibesql_types::SqlValue) -> bool {
    matches!(
        value,
        vibesql_types::SqlValue::Integer(_)
            | vibesql_types::SqlValue::Smallint(_)
            | vibesql_types::SqlValue::Bigint(_)
            | vibesql_types::SqlValue::Numeric(_)
            | vibesql_types::SqlValue::Float(_)
            | vibesql_types::SqlValue::Real(_)
            | vibesql_types::SqlValue::Double(_)
    )
}

/// Fast check if a value is comparable for MIN/MAX (optimization to avoid full match)
#[inline]
fn is_comparable_value(value: &vibesql_types::SqlValue) -> bool {
    matches!(
        value,
        vibesql_types::SqlValue::Integer(_)
            | vibesql_types::SqlValue::Smallint(_)
            | vibesql_types::SqlValue::Bigint(_)
            | vibesql_types::SqlValue::Numeric(_)
            | vibesql_types::SqlValue::Float(_)
            | vibesql_types::SqlValue::Real(_)
            | vibesql_types::SqlValue::Double(_)
            | vibesql_types::SqlValue::Varchar(_)
            | vibesql_types::SqlValue::Character(_)
            | vibesql_types::SqlValue::Boolean(_)
            | vibesql_types::SqlValue::Date(_)
            | vibesql_types::SqlValue::Time(_)
            | vibesql_types::SqlValue::Timestamp(_)
    )
}

/// Divide a SqlValue by an integer count, handling all numeric types
///
/// **Design Decision**: Always returns Numeric (f64) for AVG aggregate function
///
/// This matches the behavior of add_sql_values() and aligns with SQLLogicTest expectations.
/// See add_sql_values() documentation for rationale.
fn divide_sql_value(value: &vibesql_types::SqlValue, count: i64) -> vibesql_types::SqlValue {
    if let Some(sum_f64) = sql_value_to_f64(value) {
        vibesql_types::SqlValue::Numeric(sum_f64 / count as f64)
    } else {
        vibesql_types::SqlValue::Null
    }
}

/// Compare two SqlValues for ordering purposes (SQL ORDER BY semantics)
///
/// Uses the PartialOrd trait implementation with SQL-specific NULL handling:
/// - NULL values sort last (NULLS LAST - SQL:1999 default for ASC)
/// - Incomparable values (type mismatches, NaN) default to Equal for sort stability
pub fn compare_sql_values(a: &vibesql_types::SqlValue, b: &vibesql_types::SqlValue) -> Ordering {
    match (a.is_null(), b.is_null()) {
        // Both NULL - equal
        (true, true) => Ordering::Equal,
        // First is NULL - sorts last (greater)
        (true, false) => Ordering::Greater,
        // Second is NULL - first sorts first (less)
        (false, true) => Ordering::Less,
        // Neither NULL - use PartialOrd trait
        (false, false) => {
            // partial_cmp returns None for incomparable values (type mismatch, NaN)
            // Default to Equal to maintain sort stability
            PartialOrd::partial_cmp(a, b).unwrap_or(Ordering::Equal)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    #[test]
    fn test_combine_count() {
        let mut acc1 = AggregateAccumulator::Count { count: 5, distinct: false, seen: None };
        let acc2 = AggregateAccumulator::Count { count: 3, distinct: false, seen: None };

        acc1.combine(acc2).unwrap();

        match acc1 {
            AggregateAccumulator::Count { count, .. } => assert_eq!(count, 8),
            _ => panic!("Expected Count accumulator"),
        }
    }

    #[test]
    fn test_combine_count_distinct() {
        let mut seen1 = HashSet::new();
        seen1.insert(SqlValue::Integer(1));
        seen1.insert(SqlValue::Integer(2));

        let mut seen2 = HashSet::new();
        seen2.insert(SqlValue::Integer(2));
        seen2.insert(SqlValue::Integer(3));

        let mut acc1 = AggregateAccumulator::Count { count: 2, distinct: true, seen: Some(seen1) };
        let acc2 = AggregateAccumulator::Count { count: 2, distinct: true, seen: Some(seen2) };

        acc1.combine(acc2).unwrap();

        match acc1 {
            AggregateAccumulator::Count { count, seen, .. } => {
                assert_eq!(count, 3); // 1, 2, 3 (deduped)
                assert_eq!(seen.as_ref().unwrap().len(), 3);
            }
            _ => panic!("Expected Count accumulator"),
        }
    }

    #[test]
    fn test_combine_sum() {
        let mut acc1 = AggregateAccumulator::Sum {
            sum: SqlValue::Integer(10),
            count: 3,
            distinct: false,
            seen: None,
        };
        let acc2 = AggregateAccumulator::Sum {
            sum: SqlValue::Integer(5),
            count: 2,
            distinct: false,
            seen: None,
        };

        acc1.combine(acc2).unwrap();

        match acc1 {
            AggregateAccumulator::Sum { sum, count, .. } => {
                assert_eq!(count, 5);
                // Note: add_sql_values now preserves type (Integer + Integer = Integer)
                match sum {
                    SqlValue::Integer(val) => assert_eq!(val, 15),
                    _ => panic!("Expected Integer result from sum"),
                }
            }
            _ => panic!("Expected Sum accumulator"),
        }
    }

    #[test]
    fn test_combine_avg() {
        let mut acc1 = AggregateAccumulator::Avg {
            sum: SqlValue::Integer(100),
            count: 10,
            distinct: false,
            seen: None,
        };
        let acc2 = AggregateAccumulator::Avg {
            sum: SqlValue::Integer(50),
            count: 5,
            distinct: false,
            seen: None,
        };

        acc1.combine(acc2).unwrap();

        match acc1 {
            AggregateAccumulator::Avg { sum, count, .. } => {
                assert_eq!(count, 15);
                // Sum should be 150 (as Integer, type-preserving)
                match sum {
                    SqlValue::Integer(val) => assert_eq!(val, 150),
                    _ => panic!("Expected Integer result"),
                }
            }
            _ => panic!("Expected Avg accumulator"),
        }
    }

    #[test]
    fn test_combine_min() {
        let mut acc1 = AggregateAccumulator::Min {
            value: Some(SqlValue::Integer(5)),
            distinct: false,
            seen: None,
        };
        let acc2 = AggregateAccumulator::Min {
            value: Some(SqlValue::Integer(3)),
            distinct: false,
            seen: None,
        };

        acc1.combine(acc2).unwrap();

        match acc1 {
            AggregateAccumulator::Min { value, .. } => {
                assert_eq!(value, Some(SqlValue::Integer(3)));
            }
            _ => panic!("Expected Min accumulator"),
        }
    }

    #[test]
    fn test_combine_max() {
        let mut acc1 = AggregateAccumulator::Max {
            value: Some(SqlValue::Integer(5)),
            distinct: false,
            seen: None,
        };
        let acc2 = AggregateAccumulator::Max {
            value: Some(SqlValue::Integer(10)),
            distinct: false,
            seen: None,
        };

        acc1.combine(acc2).unwrap();

        match acc1 {
            AggregateAccumulator::Max { value, .. } => {
                assert_eq!(value, Some(SqlValue::Integer(10)));
            }
            _ => panic!("Expected Max accumulator"),
        }
    }

    #[test]
    fn test_combine_incompatible_types_fails() {
        let mut acc1 = AggregateAccumulator::Count { count: 5, distinct: false, seen: None };
        let acc2 = AggregateAccumulator::Sum {
            sum: SqlValue::Integer(10),
            count: 3,
            distinct: false,
            seen: None,
        };

        let result = acc1.combine(acc2);
        assert!(result.is_err());
    }

    #[test]
    fn test_combine_different_distinct_flags_fails() {
        let mut acc1 = AggregateAccumulator::Count { count: 5, distinct: false, seen: None };
        let acc2 =
            AggregateAccumulator::Count { count: 3, distinct: true, seen: Some(HashSet::new()) };

        let result = acc1.combine(acc2);
        assert!(result.is_err());
    }

    #[test]
    fn test_sum_returns_null_for_empty_set() {
        // Create a SUM accumulator
        let acc = AggregateAccumulator::new("SUM", false).unwrap();

        // Don't accumulate any values (empty set)

        // Finalize should return NULL
        let result = acc.finalize();
        assert!(result.is_null(), "SUM over empty set should return NULL, got {:?}", result);
    }

    #[test]
    fn test_sum_returns_null_for_all_nulls() {
        // Create a SUM accumulator
        let mut acc = AggregateAccumulator::new("SUM", false).unwrap();

        // Accumulate only NULL values
        acc.accumulate(&SqlValue::Null);
        acc.accumulate(&SqlValue::Null);
        acc.accumulate(&SqlValue::Null);

        // Finalize should return NULL
        let result = acc.finalize();
        assert!(result.is_null(), "SUM of all NULLs should return NULL, got {:?}", result);
    }

    #[test]
    fn test_sum_returns_zero_when_values_sum_to_zero() {
        // Create a SUM accumulator
        let mut acc = AggregateAccumulator::new("SUM", false).unwrap();

        // Accumulate values that sum to 0
        acc.accumulate(&SqlValue::Integer(5));
        acc.accumulate(&SqlValue::Integer(-5));

        // Finalize should return 0, not NULL
        let result = acc.finalize();
        match result {
            SqlValue::Integer(0) => {} // OK
            _ => panic!("SUM of values that sum to 0 should return 0, got {:?}", result),
        }
    }
}
