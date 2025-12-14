use std::{cmp::Ordering, collections::HashSet, hash::Hash};

/// A tuple of SQL values that can be used as a hash key for multi-argument DISTINCT
/// For example, COUNT(DISTINCT a, b) needs to track unique (a, b) pairs
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlValueTuple(Vec<vibesql_types::SqlValue>);

impl SqlValueTuple {
    pub fn new(values: Vec<vibesql_types::SqlValue>) -> Self {
        Self(values)
    }

    /// Returns true if any value in the tuple is NULL
    pub fn contains_null(&self) -> bool {
        self.0.iter().any(|v| v.is_null())
    }
}

/// Accumulator for aggregate functions
#[derive(Debug, Clone)]
pub enum AggregateAccumulator {
    Count {
        count: i64,
        distinct: bool,
        seen: Option<HashSet<vibesql_types::SqlValue>>,
        /// For multi-argument COUNT(DISTINCT a, b, ...) - track unique tuples
        seen_tuples: Option<HashSet<SqlValueTuple>>,
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
    /// GROUP_CONCAT - Concatenate values with separator (SQLite compatible)
    GroupConcat {
        values: Vec<String>,
        separator: String,
        distinct: bool,
        seen: Option<HashSet<String>>,
    },
    /// TOTAL - Like SUM but returns 0.0 for empty set instead of NULL (SQLite compatible)
    Total {
        sum: f64,
        distinct: bool,
        seen: Option<HashSet<vibesql_types::SqlValue>>,
    },
}

impl AggregateAccumulator {
    pub fn new(function_name: &str, distinct: bool) -> Result<Self, crate::errors::ExecutorError> {
        Self::new_with_separator(function_name, distinct, ",")
    }

    /// Create a new aggregate accumulator with a custom separator for GROUP_CONCAT
    pub fn new_with_separator(
        function_name: &str,
        distinct: bool,
        separator: &str,
    ) -> Result<Self, crate::errors::ExecutorError> {
        let seen = if distinct { Some(HashSet::new()) } else { None };
        match function_name.to_uppercase().as_str() {
            "COUNT" => Ok(AggregateAccumulator::Count {
                count: 0,
                distinct,
                seen,
                seen_tuples: None, // Will be initialized on first tuple accumulation
            }),
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
            "GROUP_CONCAT" => Ok(AggregateAccumulator::GroupConcat {
                values: Vec::new(),
                separator: separator.to_string(),
                distinct,
                seen: if distinct { Some(HashSet::new()) } else { None },
            }),
            "TOTAL" => Ok(AggregateAccumulator::Total { sum: 0.0, distinct, seen }),
            _ => Err(crate::errors::ExecutorError::UnsupportedExpression(format!(
                "Unknown aggregate function: {}",
                function_name
            ))),
        }
    }

    pub fn accumulate(&mut self, value: &vibesql_types::SqlValue) {
        match self {
            // COUNT - counts non-NULL values
            AggregateAccumulator::Count { ref mut count, distinct, seen, .. } => {
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

            // GROUP_CONCAT - concatenates values with separator
            AggregateAccumulator::GroupConcat { ref mut values, distinct, seen, .. } => {
                if value.is_null() {
                    return; // Skip NULL values
                }

                let str_value = sql_value_to_string(value);

                if *distinct {
                    let seen_set = seen.as_mut().unwrap();
                    if !seen_set.contains(&str_value) {
                        seen_set.insert(str_value.clone());
                        values.push(str_value);
                    }
                } else {
                    values.push(str_value);
                }
            }

            // TOTAL - sums numeric values, returns 0.0 for empty set
            AggregateAccumulator::Total { ref mut sum, distinct, seen } => {
                if value.is_null() || !is_numeric_value(value) {
                    return; // Skip NULL and non-numeric values
                }

                if *distinct {
                    let seen_set = seen.as_mut().unwrap();
                    if !seen_set.contains(value) {
                        seen_set.insert(value.clone());
                        if let Some(f) = sql_value_to_f64(value) {
                            *sum += f;
                        }
                    }
                } else if let Some(f) = sql_value_to_f64(value) {
                    *sum += f;
                }
            }
        }
    }

    /// Accumulate a tuple of values for multi-argument COUNT(DISTINCT a, b, ...)
    /// This method is only valid for COUNT with DISTINCT and multiple arguments.
    /// SQLite semantics: If ANY value in the tuple is NULL, the entire tuple is skipped.
    pub fn accumulate_tuple(&mut self, values: Vec<vibesql_types::SqlValue>) {
        match self {
            AggregateAccumulator::Count { ref mut count, distinct, ref mut seen_tuples, .. } => {
                // Multi-arg COUNT(DISTINCT) requires DISTINCT flag
                if !*distinct {
                    // For non-DISTINCT, just count non-NULL tuples
                    // SQLite: If any value is NULL, don't count the tuple
                    if values.iter().any(|v| v.is_null()) {
                        return;
                    }
                    *count += 1;
                    return;
                }

                // Create tuple and check for NULLs
                let tuple = SqlValueTuple::new(values);
                if tuple.contains_null() {
                    return; // Skip tuples with any NULL value
                }

                // Initialize seen_tuples if needed (lazy initialization)
                if seen_tuples.is_none() {
                    *seen_tuples = Some(HashSet::new());
                }

                // Only count if we haven't seen this tuple before
                let seen_set = seen_tuples.as_mut().unwrap();
                if !seen_set.contains(&tuple) {
                    seen_set.insert(tuple);
                    *count += 1;
                }
            }
            _ => {
                // Other aggregates don't support multi-argument form
                // This should be caught earlier by validation
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
                    // SQLite's SUM() preserves integer type for integer inputs
                    // Only TOTAL() always returns REAL (float)
                    // Return the sum as-is, preserving type from accumulation
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
            AggregateAccumulator::GroupConcat { values, separator, .. } => {
                if values.is_empty() {
                    vibesql_types::SqlValue::Null
                } else {
                    vibesql_types::SqlValue::Varchar(values.join(separator).into())
                }
            }
            AggregateAccumulator::Total { sum, .. } => {
                // TOTAL always returns a real number, even for empty set (returns 0.0)
                vibesql_types::SqlValue::Numeric(*sum)
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
                AggregateAccumulator::Count { count: c1, distinct: d1, seen: s1, seen_tuples: st1 },
                AggregateAccumulator::Count { count: c2, distinct: d2, seen: s2, seen_tuples: st2 },
            ) => {
                if *d1 != d2 {
                    return Err(crate::errors::ExecutorError::UnsupportedExpression(
                        "Cannot combine COUNT with different DISTINCT flags".into(),
                    ));
                }

                if *d1 {
                    // DISTINCT: Merge seen sets (single values) or seen_tuples (multi-arg)
                    if let (Some(seen1), Some(seen2)) = (s1, s2) {
                        seen1.extend(seen2);
                        *c1 = seen1.len() as i64;
                    }
                    // Also merge tuple sets for multi-arg COUNT(DISTINCT)
                    if let (Some(st1_set), Some(st2_set)) = (st1, st2) {
                        st1_set.extend(st2_set);
                        *c1 = st1_set.len() as i64;
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

            // GROUP_CONCAT: Concatenate values arrays
            (
                AggregateAccumulator::GroupConcat {
                    values: v1,
                    separator: sep1,
                    distinct: d1,
                    seen: seen1,
                },
                AggregateAccumulator::GroupConcat {
                    values: v2,
                    separator: _sep2,
                    distinct: d2,
                    seen: seen2,
                },
            ) => {
                if *d1 != d2 {
                    return Err(crate::errors::ExecutorError::UnsupportedExpression(
                        "Cannot combine GROUP_CONCAT with different DISTINCT flags".into(),
                    ));
                }

                if *d1 {
                    // DISTINCT: Merge seen sets
                    if let (Some(s1_set), Some(_s2_set)) = (seen1, seen2) {
                        for val in v2 {
                            if !s1_set.contains(&val) {
                                s1_set.insert(val.clone());
                                v1.push(val);
                            }
                        }
                    }
                } else {
                    v1.extend(v2);
                }
                // Keep separator from first accumulator (sep1 already borrowed)
                let _ = sep1;
            }

            // TOTAL: Add the sums
            (
                AggregateAccumulator::Total { sum: s1, distinct: d1, seen: seen1 },
                AggregateAccumulator::Total { sum: s2, distinct: d2, seen: seen2 },
            ) => {
                if *d1 != d2 {
                    return Err(crate::errors::ExecutorError::UnsupportedExpression(
                        "Cannot combine TOTAL with different DISTINCT flags".into(),
                    ));
                }

                if *d1 {
                    // DISTINCT: Merge seen sets, recalculate sum
                    if let (Some(s1_set), Some(s2_set)) = (seen1, seen2) {
                        s1_set.extend(s2_set);
                        *s1 = s1_set.iter().filter_map(sql_value_to_f64).sum();
                    }
                } else {
                    *s1 += s2;
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
    use vibesql_ast::BinaryOperator;

    use crate::evaluator::operators::OperatorRegistry;

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

/// Convert SqlValue to string for GROUP_CONCAT
fn sql_value_to_string(value: &vibesql_types::SqlValue) -> String {
    match value {
        vibesql_types::SqlValue::Null => String::new(),
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            s.to_string()
        }
        vibesql_types::SqlValue::Integer(i) => i.to_string(),
        vibesql_types::SqlValue::Bigint(i) => i.to_string(),
        vibesql_types::SqlValue::Smallint(i) => i.to_string(),
        vibesql_types::SqlValue::Unsigned(u) => u.to_string(),
        vibesql_types::SqlValue::Numeric(n) => n.to_string(),
        vibesql_types::SqlValue::Real(r) => r.to_string(),
        vibesql_types::SqlValue::Double(d) => d.to_string(),
        vibesql_types::SqlValue::Float(f) => f.to_string(),
        vibesql_types::SqlValue::Boolean(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        _ => value.to_string(),
    }
}

/// Divide a SqlValue by an integer count, handling all numeric types
///
/// **Design Decision**: Always returns Numeric (f64) for AVG aggregate function
///
/// This matches the behavior of add_sql_values() and aligns with SQLLogicTest expectations.
/// See add_sql_values() documentation for rationale.
fn divide_sql_value(value: &vibesql_types::SqlValue, count: i64) -> vibesql_types::SqlValue {
    if let Some(sum_f64) = sql_value_to_f64(value) {
        // AVG always returns Double (SQLite's REAL type)
        vibesql_types::SqlValue::Double(sum_f64 / count as f64)
    } else {
        vibesql_types::SqlValue::Null
    }
}

/// Compare two SqlValues for ordering purposes (SQL ORDER BY semantics)
///
/// Implements SQLite type affinity ordering for MIN/MAX aggregates:
/// - NULL values sort last (NULLS LAST - SQL:1999 default for ASC)
/// - Numbers (all integer and float types) < text < other types
/// - Within the same type class, uses natural ordering
pub fn compare_sql_values(a: &vibesql_types::SqlValue, b: &vibesql_types::SqlValue) -> Ordering {
    use vibesql_types::SqlValue;

    match (a.is_null(), b.is_null()) {
        // Both NULL - equal
        (true, true) => Ordering::Equal,
        // First is NULL - sorts last (greater)
        (true, false) => Ordering::Greater,
        // Second is NULL - first sorts first (less)
        (false, true) => Ordering::Less,
        // Neither NULL - compare by type affinity then value
        (false, false) => {
            // Helper to determine type class for SQLite affinity ordering
            // 0 = numeric (integers/reals), 1 = text, 2 = other
            fn type_class(v: &SqlValue) -> u8 {
                match v {
                    SqlValue::Integer(_)
                    | SqlValue::Bigint(_)
                    | SqlValue::Smallint(_)
                    | SqlValue::Unsigned(_)
                    | SqlValue::Float(_)
                    | SqlValue::Double(_)
                    | SqlValue::Numeric(_)
                    | SqlValue::Real(_) => 0,
                    SqlValue::Character(_) | SqlValue::Varchar(_) => 1,
                    _ => 2,
                }
            }

            let class_a = type_class(a);
            let class_b = type_class(b);

            if class_a != class_b {
                // Different type classes - use SQLite affinity ordering
                class_a.cmp(&class_b)
            } else {
                // Same type class - use natural ordering
                // partial_cmp returns None for incomparable values (NaN)
                // Default to Equal to maintain sort stability
                PartialOrd::partial_cmp(a, b).unwrap_or(Ordering::Equal)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_combine_count() {
        let mut acc1 =
            AggregateAccumulator::Count { count: 5, distinct: false, seen: None, seen_tuples: None };
        let acc2 =
            AggregateAccumulator::Count { count: 3, distinct: false, seen: None, seen_tuples: None };

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

        let mut acc1 = AggregateAccumulator::Count {
            count: 2,
            distinct: true,
            seen: Some(seen1),
            seen_tuples: None,
        };
        let acc2 = AggregateAccumulator::Count {
            count: 2,
            distinct: true,
            seen: Some(seen2),
            seen_tuples: None,
        };

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
    fn test_count_distinct_multi_arg() {
        // Test COUNT(DISTINCT a, b) with tuple tracking
        let mut acc = AggregateAccumulator::new("COUNT", true).unwrap();

        // Accumulate some tuples: (1, 1), (1, 2), (2, 1), (1, 1) - last is duplicate
        acc.accumulate_tuple(vec![SqlValue::Integer(1), SqlValue::Integer(1)]);
        acc.accumulate_tuple(vec![SqlValue::Integer(1), SqlValue::Integer(2)]);
        acc.accumulate_tuple(vec![SqlValue::Integer(2), SqlValue::Integer(1)]);
        acc.accumulate_tuple(vec![SqlValue::Integer(1), SqlValue::Integer(1)]); // duplicate

        let result = acc.finalize();
        assert_eq!(result, SqlValue::Integer(3)); // 3 unique tuples
    }

    #[test]
    fn test_count_distinct_multi_arg_with_nulls() {
        // Test that tuples with NULLs are skipped
        let mut acc = AggregateAccumulator::new("COUNT", true).unwrap();

        acc.accumulate_tuple(vec![SqlValue::Integer(1), SqlValue::Integer(1)]);
        acc.accumulate_tuple(vec![SqlValue::Integer(1), SqlValue::Null]); // skipped
        acc.accumulate_tuple(vec![SqlValue::Null, SqlValue::Integer(1)]); // skipped
        acc.accumulate_tuple(vec![SqlValue::Integer(2), SqlValue::Integer(2)]);

        let result = acc.finalize();
        assert_eq!(result, SqlValue::Integer(2)); // Only 2 valid tuples
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
        let mut acc1 =
            AggregateAccumulator::Count { count: 5, distinct: false, seen: None, seen_tuples: None };
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
        let mut acc1 =
            AggregateAccumulator::Count { count: 5, distinct: false, seen: None, seen_tuples: None };
        let acc2 = AggregateAccumulator::Count {
            count: 3,
            distinct: true,
            seen: Some(HashSet::new()),
            seen_tuples: None,
        };

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

        // Finalize should return 0 (as Integer), not NULL
        // SQLite's SUM() preserves integer type for integer inputs
        let result = acc.finalize();
        match result {
            SqlValue::Integer(0) => {} // OK - SUM preserves integer type
            _ => panic!("SUM of integers that sum to 0 should return Integer(0), got {:?}", result),
        }
    }

    #[test]
    fn test_group_concat_basic() {
        let mut acc = AggregateAccumulator::new("GROUP_CONCAT", false).unwrap();

        acc.accumulate(&SqlValue::Varchar("a".into()));
        acc.accumulate(&SqlValue::Varchar("b".into()));
        acc.accumulate(&SqlValue::Varchar("c".into()));

        let result = acc.finalize();
        assert_eq!(result, SqlValue::Varchar("a,b,c".into()));
    }

    #[test]
    fn test_group_concat_with_nulls() {
        let mut acc = AggregateAccumulator::new("GROUP_CONCAT", false).unwrap();

        acc.accumulate(&SqlValue::Varchar("a".into()));
        acc.accumulate(&SqlValue::Null);
        acc.accumulate(&SqlValue::Varchar("c".into()));

        let result = acc.finalize();
        // NULL values should be skipped
        assert_eq!(result, SqlValue::Varchar("a,c".into()));
    }

    #[test]
    fn test_group_concat_empty() {
        let acc = AggregateAccumulator::new("GROUP_CONCAT", false).unwrap();

        let result = acc.finalize();
        // Empty GROUP_CONCAT returns NULL
        assert!(result.is_null());
    }

    #[test]
    fn test_group_concat_distinct() {
        let mut acc = AggregateAccumulator::new("GROUP_CONCAT", true).unwrap();

        acc.accumulate(&SqlValue::Varchar("a".into()));
        acc.accumulate(&SqlValue::Varchar("b".into()));
        acc.accumulate(&SqlValue::Varchar("a".into())); // Duplicate

        let result = acc.finalize();
        // With DISTINCT, should only have "a,b"
        assert_eq!(result, SqlValue::Varchar("a,b".into()));
    }

    #[test]
    fn test_group_concat_with_custom_separator() {
        let mut acc = AggregateAccumulator::new_with_separator("GROUP_CONCAT", false, " - ").unwrap();

        acc.accumulate(&SqlValue::Varchar("a".into()));
        acc.accumulate(&SqlValue::Varchar("b".into()));
        acc.accumulate(&SqlValue::Varchar("c".into()));

        let result = acc.finalize();
        assert_eq!(result, SqlValue::Varchar("a - b - c".into()));
    }

    #[test]
    fn test_group_concat_with_empty_separator() {
        let mut acc = AggregateAccumulator::new_with_separator("GROUP_CONCAT", false, "").unwrap();

        acc.accumulate(&SqlValue::Varchar("a".into()));
        acc.accumulate(&SqlValue::Varchar("b".into()));
        acc.accumulate(&SqlValue::Varchar("c".into()));

        let result = acc.finalize();
        assert_eq!(result, SqlValue::Varchar("abc".into()));
    }

    #[test]
    fn test_total_basic() {
        let mut acc = AggregateAccumulator::new("TOTAL", false).unwrap();

        acc.accumulate(&SqlValue::Integer(1));
        acc.accumulate(&SqlValue::Integer(2));
        acc.accumulate(&SqlValue::Integer(3));

        let result = acc.finalize();
        assert_eq!(result, SqlValue::Numeric(6.0));
    }

    #[test]
    fn test_total_empty_returns_zero() {
        // TOTAL returns 0.0 for empty set (unlike SUM which returns NULL)
        let acc = AggregateAccumulator::new("TOTAL", false).unwrap();

        let result = acc.finalize();
        assert_eq!(result, SqlValue::Numeric(0.0));
    }

    #[test]
    fn test_total_with_nulls() {
        let mut acc = AggregateAccumulator::new("TOTAL", false).unwrap();

        acc.accumulate(&SqlValue::Integer(1));
        acc.accumulate(&SqlValue::Null);
        acc.accumulate(&SqlValue::Integer(2));

        let result = acc.finalize();
        // NULL values should be skipped
        assert_eq!(result, SqlValue::Numeric(3.0));
    }

    #[test]
    fn test_total_all_nulls_returns_zero() {
        // TOTAL of all NULLs returns 0.0 (unlike SUM which returns NULL)
        let mut acc = AggregateAccumulator::new("TOTAL", false).unwrap();

        acc.accumulate(&SqlValue::Null);
        acc.accumulate(&SqlValue::Null);

        let result = acc.finalize();
        assert_eq!(result, SqlValue::Numeric(0.0));
    }
}
