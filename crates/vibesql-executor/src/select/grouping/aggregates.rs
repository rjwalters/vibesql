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
        has_non_integer: bool,
        distinct: bool,
        seen: Option<HashSet<vibesql_types::SqlValue>>,
        /// Set to true if integer overflow occurred during accumulation
        overflow_error: bool,
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
    Total { sum: f64, distinct: bool, seen: Option<HashSet<vibesql_types::SqlValue>> },
    /// JSON_GROUP_ARRAY - Collects values into a JSON array (SQLite compatible)
    JsonGroupArray {
        values: Vec<vibesql_types::SqlValue>,
        distinct: bool,
        seen: Option<HashSet<vibesql_types::SqlValue>>,
    },
    /// MD5SUM - Compute MD5 hash of concatenated values (SQLite TCL test compatible)
    Md5sum { values: Vec<String>, distinct: bool, seen: Option<HashSet<String>> },
    /// PERCENTILE family - median(Y), percentile(Y,P), percentile_cont(Y,F),
    /// percentile_disc(Y,F). Semantics match SQLite's percentile.c extension:
    /// collect all non-NULL Y values, sort, then interpolate at
    /// `fraction * (N-1)`. Errors (invalid/inconsistent fraction, non-numeric
    /// or infinite Y) are deferred and raised at finalize time.
    Percentile {
        /// Collected Y values (non-NULL, finite)
        values: Vec<f64>,
        /// Normalized fraction (0.0..=1.0) captured from the first input row
        fraction: Option<f64>,
        /// Maximum raw fraction: 100.0 for percentile(), 1.0 for the others
        mx_frac: f64,
        /// True for percentile_disc() (no interpolation; picks a[floor(ix)])
        discrete: bool,
        /// True for median() (fixed fraction 0.5, no second argument)
        is_median: bool,
        /// Lowercase function name for SQLite-exact error messages
        name: String,
        /// First deferred error message (SQLite-exact), raised at finalize
        error: Option<String>,
        distinct: bool,
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
                has_non_integer: false,
                distinct,
                seen,
                overflow_error: false,
            }),
            "AVG" => Ok(AggregateAccumulator::Avg {
                sum: vibesql_types::SqlValue::Integer(0),
                count: 0,
                distinct,
                seen,
            }),
            "MIN" => Ok(AggregateAccumulator::Min { value: None, distinct, seen }),
            "MAX" => Ok(AggregateAccumulator::Max { value: None, distinct, seen }),
            "GROUP_CONCAT" | "STRING_AGG" => Ok(AggregateAccumulator::GroupConcat {
                values: Vec::new(),
                separator: separator.to_string(),
                distinct,
                seen: if distinct { Some(HashSet::new()) } else { None },
            }),
            "TOTAL" => Ok(AggregateAccumulator::Total { sum: 0.0, distinct, seen }),
            "JSON_GROUP_ARRAY" => {
                Ok(AggregateAccumulator::JsonGroupArray { values: Vec::new(), distinct, seen })
            }
            "MD5SUM" => Ok(AggregateAccumulator::Md5sum {
                values: Vec::new(),
                distinct,
                seen: if distinct { Some(HashSet::new()) } else { None },
            }),
            "MEDIAN" | "PERCENTILE" | "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
                let upper = function_name.to_uppercase();
                let (mx_frac, discrete, is_median, name) = match upper.as_str() {
                    "PERCENTILE" => (100.0, false, false, "percentile"),
                    "PERCENTILE_CONT" => (1.0, false, false, "percentile_cont"),
                    "PERCENTILE_DISC" => (1.0, true, false, "percentile_disc"),
                    _ => (1.0, false, true, "median"),
                };
                Ok(AggregateAccumulator::Percentile {
                    values: Vec::new(),
                    fraction: None,
                    mx_frac,
                    discrete,
                    is_median,
                    name: name.to_string(),
                    error: None,
                    distinct,
                })
            }
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
            AggregateAccumulator::Sum {
                ref mut sum,
                ref mut count,
                ref mut has_non_integer,
                distinct,
                seen,
                ref mut overflow_error,
            } => {
                // If overflow already occurred, don't process more values
                if *overflow_error {
                    return;
                }

                // Skip NULL values - they don't affect sum or type
                if value.is_null() {
                    return;
                }

                // Try to coerce the value to a numeric type
                // This handles TEXT values that contain integers/reals
                let (coerced_value, is_integer) = match try_coerce_to_numeric(value) {
                    Some((coerced, is_int)) => (coerced, is_int),
                    None => {
                        // Cannot coerce to numeric - skip this value
                        // (non-numeric text like "abc" is treated as 0 and skipped)
                        return;
                    }
                };

                // Track non-integer values for type determination
                // SQLite: If ANY REAL/FLOAT value is encountered, result should be REAL
                // TEXT values that parse as integers are treated as integers
                if !is_integer {
                    *has_non_integer = true;
                }

                if *distinct {
                    // Only sum if we haven't seen this value before
                    // For DISTINCT, use original value for deduplication
                    let seen_set = seen.as_mut().unwrap();
                    if !seen_set.contains(value) {
                        seen_set.insert(value.clone());
                        match add_sql_values(sum, &coerced_value) {
                            Ok(result) => {
                                *sum = result;
                                *count += 1;
                            }
                            Err(crate::errors::ExecutorError::IntegerOverflow) => {
                                *overflow_error = true;
                            }
                            Err(_) => {
                                // Other errors are treated as NULL (existing behavior)
                            }
                        }
                    }
                } else {
                    match add_sql_values(sum, &coerced_value) {
                        Ok(result) => {
                            *sum = result;
                            *count += 1;
                        }
                        Err(crate::errors::ExecutorError::IntegerOverflow) => {
                            *overflow_error = true;
                        }
                        Err(_) => {
                            // Other errors are treated as NULL (existing behavior)
                        }
                    }
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
                        // For AVG, overflow in sum is rare since we divide anyway
                        // Just use the result or skip on error
                        if let Ok(result) = add_sql_values(sum, value) {
                            *sum = result;
                            *count += 1;
                        }
                    }
                } else {
                    if let Ok(result) = add_sql_values(sum, value) {
                        *sum = result;
                        *count += 1;
                    }
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

            // JSON_GROUP_ARRAY - collects values into a JSON array
            AggregateAccumulator::JsonGroupArray { ref mut values, distinct, seen } => {
                // Unlike GROUP_CONCAT, JSON_GROUP_ARRAY includes NULL values
                if *distinct {
                    let seen_set = seen.as_mut().unwrap();
                    if !seen_set.contains(value) {
                        seen_set.insert(value.clone());
                        values.push(value.clone());
                    }
                } else {
                    values.push(value.clone());
                }
            }

            // PERCENTILE family - single-argument form (median). Two-argument
            // forms thread the per-row fraction through accumulate_percentile.
            AggregateAccumulator::Percentile { .. } => {
                self.accumulate_percentile(value, None);
            }

            // MD5SUM - collects string values for MD5 hashing (like GROUP_CONCAT)
            AggregateAccumulator::Md5sum { ref mut values, distinct, seen } => {
                // Skip NULL values like GROUP_CONCAT
                if value.is_null() {
                    return;
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
        }
    }

    /// Accumulate a tuple of values for multi-argument COUNT(DISTINCT a, b, ...)
    /// This method is only valid for COUNT with DISTINCT and multiple arguments.
    /// SQLite semantics: If ANY value in the tuple is NULL, the entire tuple is skipped.
    pub fn accumulate_tuple(&mut self, values: Vec<vibesql_types::SqlValue>) {
        match self {
            AggregateAccumulator::Count {
                ref mut count, distinct, ref mut seen_tuples, ..
            } => {
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

    /// Accumulate one row for the PERCENTILE family of aggregates.
    ///
    /// `fraction_arg` is the per-row second argument (P/F) for
    /// percentile()/percentile_cont()/percentile_disc(); it is `None` for
    /// median(), which uses a fixed fraction of 0.5.
    ///
    /// Mirrors percentStep() in SQLite's ext/misc/percentile.c:
    /// 1. The fraction is validated per row (numeric, in `0..=mx_frac`).
    /// 2. The normalized fraction may differ from the first row's by at most
    ///    0.001, else "not the same for all input rows".
    /// 3. NULL Y values are skipped (NaN is treated as NULL, matching SQLite
    ///    which stores NaN as NULL).
    /// 4. Non-NULL non-numeric Y is an error (by type - no text coercion).
    /// 5. +/-Inf Y is an error.
    ///
    /// Errors are deferred (recorded in `error`) and raised at finalize.
    pub fn accumulate_percentile(
        &mut self,
        value: &vibesql_types::SqlValue,
        fraction_arg: Option<&vibesql_types::SqlValue>,
    ) {
        let AggregateAccumulator::Percentile {
            ref mut values,
            ref mut fraction,
            mx_frac,
            is_median,
            name,
            ref mut error,
            ..
        } = self
        else {
            return;
        };

        if error.is_some() {
            return;
        }

        // Requirement 3: fraction must be numeric and within range (per row).
        let rpct = if *is_median && fraction_arg.is_none() {
            0.5
        } else {
            let coerced = fraction_arg.and_then(percentile_fraction_as_number);
            match coerced {
                Some(p) if (0.0..=*mx_frac).contains(&p) => p / *mx_frac,
                _ => {
                    *error = Some(format!(
                        "the fraction argument to {}() is not between 0.0 and {:.1}",
                        name, mx_frac
                    ));
                    return;
                }
            }
        };

        // Requirement 2: the normalized fraction must match the first row's
        // stored value to within 0.001 (percentSameValue in percentile.c).
        match fraction {
            None => *fraction = Some(rpct),
            Some(first) => {
                let diff = *first - rpct;
                if !(-0.001..=0.001).contains(&diff) {
                    *error = Some(format!(
                        "the fraction argument to {}() is not the same for all input rows",
                        name
                    ));
                    return;
                }
            }
        }

        // Requirement: NULL Y values are ignored.
        if value.is_null() {
            return;
        }

        // Requirement 4: non-NULL Y must be numeric BY TYPE (no coercion -
        // TEXT '50' and blobs are errors, unlike most SQLite functions).
        let Some(y) = percentile_numeric_y(value) else {
            *error = Some(format!("input to {}() is not numeric", name));
            return;
        };

        // SQLite interprets NaN as NULL, so a NaN Y is skipped, not an error.
        if y.is_nan() {
            return;
        }

        // Requirement 5: infinity is an error.
        if y.is_infinite() {
            *error = Some(format!("Inf input to {}()", name));
            return;
        }

        values.push(y);
    }

    pub fn finalize(&self) -> Result<vibesql_types::SqlValue, crate::errors::ExecutorError> {
        match self {
            AggregateAccumulator::Count { count, .. } => {
                Ok(vibesql_types::SqlValue::Integer(*count))
            }
            AggregateAccumulator::Sum { sum, count, has_non_integer, overflow_error, .. } => {
                // Check for overflow error first
                if *overflow_error {
                    return Err(crate::errors::ExecutorError::IntegerOverflow);
                }
                if *count == 0 {
                    Ok(vibesql_types::SqlValue::Null)
                } else if *has_non_integer {
                    // Mixed types (saw NULL, TEXT, REAL, etc.) → return REAL
                    // Convert integer sum to REAL with .0 suffix
                    Ok(match sum {
                        vibesql_types::SqlValue::Integer(v) => {
                            vibesql_types::SqlValue::Double(*v as f64)
                        }
                        vibesql_types::SqlValue::Bigint(v) => {
                            vibesql_types::SqlValue::Double(*v as f64)
                        }
                        vibesql_types::SqlValue::Smallint(v) => {
                            vibesql_types::SqlValue::Double(*v as f64)
                        }
                        _ => sum.clone(), // Already a floating-point type
                    })
                } else {
                    // Pure integer inputs → preserve INTEGER type
                    Ok(sum.clone())
                }
            }
            AggregateAccumulator::Avg { sum, count, .. } => {
                if *count == 0 {
                    Ok(vibesql_types::SqlValue::Null)
                } else {
                    Ok(divide_sql_value(sum, *count))
                }
            }
            AggregateAccumulator::Min { value, .. } => {
                Ok(value.clone().unwrap_or(vibesql_types::SqlValue::Null))
            }
            AggregateAccumulator::Max { value, .. } => {
                Ok(value.clone().unwrap_or(vibesql_types::SqlValue::Null))
            }
            AggregateAccumulator::GroupConcat { values, separator, .. } => {
                if values.is_empty() {
                    Ok(vibesql_types::SqlValue::Null)
                } else {
                    Ok(vibesql_types::SqlValue::Varchar(values.join(separator).into()))
                }
            }
            AggregateAccumulator::Total { sum, .. } => {
                // TOTAL always returns a real number, even for empty set (returns 0.0)
                Ok(vibesql_types::SqlValue::Numeric(*sum))
            }
            AggregateAccumulator::JsonGroupArray { values, .. } => {
                // Convert values to JSON array string
                let json_array = sql_values_to_json_array(values);
                Ok(vibesql_types::SqlValue::Varchar(json_array.into()))
            }
            AggregateAccumulator::Percentile { values, fraction, discrete, error, distinct, .. } => {
                // Deferred step errors surface at finalize time with the
                // SQLite-exact message (SqliteCompatError displays as-is).
                if let Some(msg) = error {
                    return Err(crate::errors::ExecutorError::SqliteCompatError(msg.clone()));
                }
                if values.is_empty() {
                    return Ok(vibesql_types::SqlValue::Null);
                }
                let mut sorted = values.clone();
                // No NaN can be present (skipped during accumulation), so
                // total_cmp == partial order here.
                sorted.sort_by(f64::total_cmp);
                if *distinct {
                    sorted.dedup();
                }
                // percentCompute() in percentile.c: ix = fraction * (N-1);
                // disc -> a[floor(ix)]; cont -> linear interpolation between
                // the two neighbors. Result is always REAL.
                let frac = fraction.unwrap_or(0.5);
                let n = sorted.len();
                let ix = frac * (n - 1) as f64;
                let i1 = ix as usize;
                let vx = if *discrete {
                    sorted[i1]
                } else {
                    let i2 = if ix == i1 as f64 || i1 == n - 1 { i1 } else { i1 + 1 };
                    let v1 = sorted[i1];
                    let v2 = sorted[i2];
                    v1 + (v2 - v1) * (ix - i1 as f64)
                };
                Ok(vibesql_types::SqlValue::Double(vx))
            }
            AggregateAccumulator::Md5sum { values, .. } => {
                use md5::{Digest, Md5};
                // Concatenate all values and compute MD5
                let concatenated = values.join("");
                let mut hasher = Md5::new();
                hasher.update(concatenated.as_bytes());
                let result = hasher.finalize();
                let hash = result.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                Ok(vibesql_types::SqlValue::Varchar(hash.into()))
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
                AggregateAccumulator::Sum {
                    sum: s1,
                    count: c1,
                    has_non_integer: h1,
                    distinct: d1,
                    seen: seen1,
                    overflow_error: o1,
                },
                AggregateAccumulator::Sum {
                    sum: s2,
                    count: c2,
                    has_non_integer: h2,
                    distinct: d2,
                    seen: seen2,
                    overflow_error: o2,
                },
            ) => {
                // Propagate overflow error from either accumulator
                if o2 {
                    *o1 = true;
                    return Ok(());
                }
                if *o1 {
                    return Ok(());
                }

                if *d1 != d2 {
                    return Err(crate::errors::ExecutorError::UnsupportedExpression(
                        "Cannot combine SUM with different DISTINCT flags".into(),
                    ));
                }

                // Combine has_non_integer flags (if either saw non-integer, result has non-integer)
                *h1 = *h1 || h2;

                if *d1 {
                    // DISTINCT: Merge seen sets, recalculate sum
                    if let (Some(s1_set), Some(s2_set)) = (seen1, seen2) {
                        s1_set.extend(s2_set);
                        // Recalculate sum and count from merged set
                        let mut new_sum = vibesql_types::SqlValue::Integer(0);
                        for val in s1_set.iter() {
                            match add_sql_values(&new_sum, val) {
                                Ok(result) => new_sum = result,
                                Err(crate::errors::ExecutorError::IntegerOverflow) => {
                                    *o1 = true;
                                    return Ok(());
                                }
                                Err(_) => {}
                            }
                        }
                        *s1 = new_sum;
                        *c1 = s1_set.len() as i64;
                    }
                } else {
                    match add_sql_values(s1, &s2) {
                        Ok(result) => {
                            *s1 = result;
                            *c1 += c2;
                        }
                        Err(crate::errors::ExecutorError::IntegerOverflow) => {
                            *o1 = true;
                        }
                        Err(_) => {}
                    }
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
                        let mut new_sum = vibesql_types::SqlValue::Integer(0);
                        for val in s1_set.iter() {
                            if let Ok(result) = add_sql_values(&new_sum, val) {
                                new_sum = result;
                            }
                        }
                        *s1 = new_sum;
                        *c1 = s1_set.len() as i64;
                    }
                } else {
                    if let Ok(result) = add_sql_values(s1, &s2) {
                        *s1 = result;
                        *c1 += c2;
                    }
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

            // PERCENTILE family: merge value lists, propagate deferred errors,
            // and enforce fraction consistency across the two halves.
            (
                AggregateAccumulator::Percentile {
                    values: v1,
                    fraction: f1,
                    error: e1,
                    name,
                    ..
                },
                AggregateAccumulator::Percentile {
                    values: v2, fraction: f2, error: e2, ..
                },
            ) => {
                if e1.is_none() {
                    if let Some(msg) = e2 {
                        *e1 = Some(msg);
                    }
                }
                if e1.is_some() {
                    return Ok(());
                }
                if let Some(b) = f2 {
                    match *f1 {
                        None => *f1 = Some(b),
                        Some(a) => {
                            let diff = a - b;
                            if !(-0.001..=0.001).contains(&diff) {
                                *e1 = Some(format!(
                                    "the fraction argument to {}() is not the same for all input rows",
                                    name
                                ));
                                return Ok(());
                            }
                        }
                    }
                }
                v1.extend(v2);
            }

            // MD5SUM: Merge value lists
            (
                AggregateAccumulator::Md5sum { values: v1, distinct: d1, seen: seen1 },
                AggregateAccumulator::Md5sum { values: v2, distinct: d2, seen: seen2 },
            ) => {
                if *d1 != d2 {
                    return Err(crate::errors::ExecutorError::UnsupportedExpression(
                        "Cannot combine MD5SUM with different DISTINCT flags".into(),
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
) -> Result<vibesql_types::SqlValue, crate::errors::ExecutorError> {
    // Use the proper arithmetic addition operator that preserves types
    // Integer + Integer → Integer, Float + anything → Float, etc.
    use vibesql_ast::BinaryOperator;

    use crate::evaluator::operators::OperatorRegistry;

    OperatorRegistry::eval_binary_op(a, &BinaryOperator::Plus, b, vibesql_types::SqlMode::default())
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

/// Coerce the percentile fraction argument (P/F) to a number, mirroring
/// sqlite3_value_numeric_type(): INTEGER/FLOAT pass through, TEXT converts
/// only when it is a complete well-formed numeral, everything else (NULL,
/// blob, non-numeric text) is invalid.
fn percentile_fraction_as_number(value: &vibesql_types::SqlValue) -> Option<f64> {
    use vibesql_types::SqlValue;
    match value {
        SqlValue::Integer(v) => Some(*v as f64),
        SqlValue::Smallint(v) => Some(*v as f64),
        SqlValue::Bigint(v) => Some(*v as f64),
        SqlValue::Unsigned(v) => Some(*v as f64),
        SqlValue::Float(v) => Some(*v as f64),
        SqlValue::Real(v) => Some(*v as f64),
        SqlValue::Double(v) => Some(*v),
        SqlValue::Numeric(v) => Some(*v),
        SqlValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

/// Extract the percentile Y value as f64 when the value's TYPE is numeric.
/// Unlike most SQLite aggregates, percentile does NOT coerce text/blob to a
/// number - a non-NULL non-numeric Y is an error (percentile.c requirement 4).
fn percentile_numeric_y(value: &vibesql_types::SqlValue) -> Option<f64> {
    use vibesql_types::SqlValue;
    match value {
        SqlValue::Integer(v) => Some(*v as f64),
        SqlValue::Smallint(v) => Some(*v as f64),
        SqlValue::Bigint(v) => Some(*v as f64),
        SqlValue::Unsigned(v) => Some(*v as f64),
        SqlValue::Float(v) => Some(*v as f64),
        SqlValue::Real(v) => Some(*v as f64),
        SqlValue::Double(v) => Some(*v),
        SqlValue::Numeric(v) => Some(*v),
        SqlValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
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

/// Try to coerce a SqlValue to a numeric value for SUM
/// Returns Some((coerced_value, is_integer_type)) if coercion is possible
/// Returns None if the value cannot be coerced to a number
fn try_coerce_to_numeric(
    value: &vibesql_types::SqlValue,
) -> Option<(vibesql_types::SqlValue, bool)> {
    match value {
        // Already numeric types - return as-is (preserve original type)
        vibesql_types::SqlValue::Integer(v) => Some((vibesql_types::SqlValue::Integer(*v), true)),
        vibesql_types::SqlValue::Bigint(v) => Some((vibesql_types::SqlValue::Bigint(*v), true)),
        vibesql_types::SqlValue::Smallint(v) => Some((vibesql_types::SqlValue::Smallint(*v), true)),
        vibesql_types::SqlValue::Unsigned(v) => {
            Some((vibesql_types::SqlValue::Integer(*v as i64), true))
        }
        vibesql_types::SqlValue::Float(v) => Some((vibesql_types::SqlValue::Float(*v), false)),
        vibesql_types::SqlValue::Double(v) => Some((vibesql_types::SqlValue::Double(*v), false)),
        vibesql_types::SqlValue::Numeric(v) => Some((vibesql_types::SqlValue::Numeric(*v), false)),
        vibesql_types::SqlValue::Real(v) => Some((vibesql_types::SqlValue::Real(*v), false)),

        // Text types - try to parse as integer first, then as real
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            let trimmed = s.trim();
            // Try to parse as integer first
            if let Ok(i) = trimmed.parse::<i64>() {
                Some((vibesql_types::SqlValue::Integer(i), true))
            } else if let Ok(f) = trimmed.parse::<f64>() {
                // Parsed as real - this is a non-integer type
                Some((vibesql_types::SqlValue::Double(f), false))
            } else {
                // Cannot parse as number - SQLite treats this as 0 for SUM
                // but it doesn't count as a value and doesn't affect type
                None
            }
        }

        // Boolean - treat as 0 or 1 (integer)
        vibesql_types::SqlValue::Boolean(b) => {
            Some((vibesql_types::SqlValue::Integer(if *b { 1 } else { 0 }), true))
        }

        // NULL and other types - cannot coerce
        vibesql_types::SqlValue::Null => None,
        _ => None,
    }
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

/// Escape a string for JSON output, handling all required escape sequences.
fn escape_json_string(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            '\u{0008}' => escaped.push_str("\\b"), // backspace
            '\u{000C}' => escaped.push_str("\\f"), // form feed
            // Other control characters U+0000 through U+001F
            c if c.is_control() && (c as u32) < 0x20 => {
                escaped.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => escaped.push(c),
        }
    }
    escaped
}

/// Convert a single SqlValue to a JSON value representation
fn sql_value_to_json(value: &vibesql_types::SqlValue) -> String {
    use vibesql_types::SqlValue;
    match value {
        SqlValue::Null => "null".to_string(),
        SqlValue::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Bigint(i) => i.to_string(),
        SqlValue::Smallint(i) => i.to_string(),
        SqlValue::Unsigned(u) => u.to_string(),
        SqlValue::Numeric(n) => n.to_string(),
        SqlValue::Real(r) => r.to_string(),
        SqlValue::Double(d) => d.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            let trimmed = s.trim();
            // If the value is already a JSON object or array, embed it directly
            if (trimmed.starts_with('{') && trimmed.ends_with('}'))
                || (trimmed.starts_with('[') && trimmed.ends_with(']'))
            {
                s.to_string()
            } else {
                format!("\"{}\"", escape_json_string(s))
            }
        }
        _ => {
            // For other types, convert to string and quote
            format!("\"{}\"", escape_json_string(&value.to_string()))
        }
    }
}

/// Convert a vector of SqlValues to a JSON array string
fn sql_values_to_json_array(values: &[vibesql_types::SqlValue]) -> String {
    let elements: Vec<String> = values.iter().map(sql_value_to_json).collect();
    format!("[{}]", elements.join(","))
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

/// Compare two SqlValues for ordering purposes with optional collation
///
/// Supports SQLite collations:
/// - BINARY (default): byte-by-byte comparison
/// - NOCASE: case-insensitive comparison for text
/// - RTRIM: trims trailing whitespace before comparison
///
/// For non-text values or when collation is None, falls back to compare_sql_values
pub fn compare_sql_values_with_collation(
    a: &vibesql_types::SqlValue,
    b: &vibesql_types::SqlValue,
    collation: Option<&str>,
) -> Ordering {
    use vibesql_types::SqlValue;

    // Handle NULL values first
    match (a.is_null(), b.is_null()) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Greater,
        (false, true) => return Ordering::Less,
        (false, false) => {}
    }

    // Check if we need to apply collation (only for text values)
    let collation_upper = collation.map(|c| c.to_uppercase());
    match collation_upper.as_deref() {
        Some("NOCASE") => {
            // Case-insensitive comparison for text values
            match (a, b) {
                (SqlValue::Character(s1), SqlValue::Character(s2))
                | (SqlValue::Varchar(s1), SqlValue::Varchar(s2))
                | (SqlValue::Character(s1), SqlValue::Varchar(s2))
                | (SqlValue::Varchar(s1), SqlValue::Character(s2)) => {
                    s1.to_lowercase().cmp(&s2.to_lowercase())
                }
                _ => compare_sql_values(a, b),
            }
        }
        Some("RTRIM") => {
            // Trim trailing whitespace before comparison
            match (a, b) {
                (SqlValue::Character(s1), SqlValue::Character(s2))
                | (SqlValue::Varchar(s1), SqlValue::Varchar(s2))
                | (SqlValue::Character(s1), SqlValue::Varchar(s2))
                | (SqlValue::Varchar(s1), SqlValue::Character(s2)) => {
                    s1.trim_end().cmp(s2.trim_end())
                }
                _ => compare_sql_values(a, b),
            }
        }
        // BINARY or default: use standard comparison
        _ => compare_sql_values(a, b),
    }
}

/// Compare two SqlValues for ordering purposes (SQL ORDER BY semantics)
///
/// Implements SQLite type-class ordering as a genuine total order (#5802):
/// - NULL values sort last (NULLS LAST - SQL:1999 default for ASC; this is
///   the contract MIN/MAX aggregates rely on)
/// - numeric (all integer and float variants, inter-comparable and compared
///   exactly — never through lossy `as f64` casts) < text < blob < other
///   types in deterministic slots
///
/// See [`vibesql_types::total_order_cmp`] for the full ordering definition
/// and its totality/transitivity guarantees.
///
/// Note: For ORDER BY with direction-dependent NULL handling (SQLite behavior),
/// use the NULL ordering logic in apply_order_by() in order.rs instead.
pub fn compare_sql_values(a: &vibesql_types::SqlValue, b: &vibesql_types::SqlValue) -> Ordering {
    match (a.is_null(), b.is_null()) {
        // Both NULL - equal
        (true, true) => Ordering::Equal,
        // First is NULL - sorts last (greater)
        (true, false) => Ordering::Greater,
        // Second is NULL - first sorts first (less)
        (false, true) => Ordering::Less,
        // Neither NULL - delegate to the shared total order
        (false, false) => vibesql_types::total_order_cmp(a, b),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_combine_count() {
        let mut acc1 = AggregateAccumulator::Count {
            count: 5,
            distinct: false,
            seen: None,
            seen_tuples: None,
        };
        let acc2 = AggregateAccumulator::Count {
            count: 3,
            distinct: false,
            seen: None,
            seen_tuples: None,
        };

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

        let result = acc.finalize().unwrap();
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

        let result = acc.finalize().unwrap();
        assert_eq!(result, SqlValue::Integer(2)); // Only 2 valid tuples
    }

    #[test]
    fn test_combine_sum() {
        let mut acc1 = AggregateAccumulator::Sum {
            sum: SqlValue::Integer(10),
            count: 3,
            has_non_integer: false,
            distinct: false,
            seen: None,
            overflow_error: false,
        };
        let acc2 = AggregateAccumulator::Sum {
            sum: SqlValue::Integer(5),
            count: 2,
            has_non_integer: false,
            distinct: false,
            seen: None,
            overflow_error: false,
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
        let mut acc1 = AggregateAccumulator::Count {
            count: 5,
            distinct: false,
            seen: None,
            seen_tuples: None,
        };
        let acc2 = AggregateAccumulator::Sum {
            sum: SqlValue::Integer(10),
            count: 3,
            has_non_integer: false,
            distinct: false,
            seen: None,
            overflow_error: false,
        };

        let result = acc1.combine(acc2);
        assert!(result.is_err());
    }

    #[test]
    fn test_combine_different_distinct_flags_fails() {
        let mut acc1 = AggregateAccumulator::Count {
            count: 5,
            distinct: false,
            seen: None,
            seen_tuples: None,
        };
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
        let result = acc.finalize().unwrap();
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
        let result = acc.finalize().unwrap();
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
        let result = acc.finalize().unwrap();
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

        let result = acc.finalize().unwrap();
        assert_eq!(result, SqlValue::Varchar("a,b,c".into()));
    }

    #[test]
    fn test_group_concat_with_nulls() {
        let mut acc = AggregateAccumulator::new("GROUP_CONCAT", false).unwrap();

        acc.accumulate(&SqlValue::Varchar("a".into()));
        acc.accumulate(&SqlValue::Null);
        acc.accumulate(&SqlValue::Varchar("c".into()));

        let result = acc.finalize().unwrap();
        // NULL values should be skipped
        assert_eq!(result, SqlValue::Varchar("a,c".into()));
    }

    #[test]
    fn test_group_concat_empty() {
        let acc = AggregateAccumulator::new("GROUP_CONCAT", false).unwrap();

        let result = acc.finalize().unwrap();
        // Empty GROUP_CONCAT returns NULL
        assert!(result.is_null());
    }

    #[test]
    fn test_group_concat_distinct() {
        let mut acc = AggregateAccumulator::new("GROUP_CONCAT", true).unwrap();

        acc.accumulate(&SqlValue::Varchar("a".into()));
        acc.accumulate(&SqlValue::Varchar("b".into()));
        acc.accumulate(&SqlValue::Varchar("a".into())); // Duplicate

        let result = acc.finalize().unwrap();
        // With DISTINCT, should only have "a,b"
        assert_eq!(result, SqlValue::Varchar("a,b".into()));
    }

    #[test]
    fn test_group_concat_with_custom_separator() {
        let mut acc =
            AggregateAccumulator::new_with_separator("GROUP_CONCAT", false, " - ").unwrap();

        acc.accumulate(&SqlValue::Varchar("a".into()));
        acc.accumulate(&SqlValue::Varchar("b".into()));
        acc.accumulate(&SqlValue::Varchar("c".into()));

        let result = acc.finalize().unwrap();
        assert_eq!(result, SqlValue::Varchar("a - b - c".into()));
    }

    #[test]
    fn test_group_concat_with_empty_separator() {
        let mut acc = AggregateAccumulator::new_with_separator("GROUP_CONCAT", false, "").unwrap();

        acc.accumulate(&SqlValue::Varchar("a".into()));
        acc.accumulate(&SqlValue::Varchar("b".into()));
        acc.accumulate(&SqlValue::Varchar("c".into()));

        let result = acc.finalize().unwrap();
        assert_eq!(result, SqlValue::Varchar("abc".into()));
    }

    #[test]
    fn test_total_basic() {
        let mut acc = AggregateAccumulator::new("TOTAL", false).unwrap();

        acc.accumulate(&SqlValue::Integer(1));
        acc.accumulate(&SqlValue::Integer(2));
        acc.accumulate(&SqlValue::Integer(3));

        let result = acc.finalize().unwrap();
        assert_eq!(result, SqlValue::Numeric(6.0));
    }

    #[test]
    fn test_total_empty_returns_zero() {
        // TOTAL returns 0.0 for empty set (unlike SUM which returns NULL)
        let acc = AggregateAccumulator::new("TOTAL", false).unwrap();

        let result = acc.finalize().unwrap();
        assert_eq!(result, SqlValue::Numeric(0.0));
    }

    #[test]
    fn test_total_with_nulls() {
        let mut acc = AggregateAccumulator::new("TOTAL", false).unwrap();

        acc.accumulate(&SqlValue::Integer(1));
        acc.accumulate(&SqlValue::Null);
        acc.accumulate(&SqlValue::Integer(2));

        let result = acc.finalize().unwrap();
        // NULL values should be skipped
        assert_eq!(result, SqlValue::Numeric(3.0));
    }

    #[test]
    fn test_total_all_nulls_returns_zero() {
        // TOTAL of all NULLs returns 0.0 (unlike SUM which returns NULL)
        let mut acc = AggregateAccumulator::new("TOTAL", false).unwrap();

        acc.accumulate(&SqlValue::Null);
        acc.accumulate(&SqlValue::Null);

        let result = acc.finalize().unwrap();
        assert_eq!(result, SqlValue::Numeric(0.0));
    }

    #[test]
    fn test_compare_sql_values_cross_type_numeric() {
        // Test cross-type numeric comparison (Integer vs Real)
        // This is critical for ORDER BY with mixed numeric types (#4609)
        use std::cmp::Ordering;

        // Integer -2 should be less than Real 1.23
        assert_eq!(
            compare_sql_values(&SqlValue::Integer(-2), &SqlValue::Real(1.23)),
            Ordering::Less
        );

        // Integer 2 should be greater than Real 1.23
        assert_eq!(
            compare_sql_values(&SqlValue::Integer(2), &SqlValue::Real(1.23)),
            Ordering::Greater
        );

        // Integer 2 should be greater than Integer -2
        assert_eq!(
            compare_sql_values(&SqlValue::Integer(2), &SqlValue::Integer(-2)),
            Ordering::Greater
        );

        // Real -2.0 should be less than Integer 2
        assert_eq!(
            compare_sql_values(&SqlValue::Real(-2.0), &SqlValue::Integer(2)),
            Ordering::Less
        );

        // Same values across types should be equal
        assert_eq!(
            compare_sql_values(&SqlValue::Integer(2), &SqlValue::Real(2.0)),
            Ordering::Equal
        );
    }

    /// Regression test for #5802: the mixed-type comparator must be a genuine
    /// total order. Before the fix, cross-variant numeric pairs (e.g. Integer
    /// vs Bigint, Bigint vs Double) fell back to lossy `as f64` casts while
    /// same-variant pairs compared exactly, producing a non-transitive triple:
    ///
    ///   a = Bigint(2^53), b = Bigint(2^53 + 1), c = Double(2^53 as f64)
    ///   a < b (exact i64), but a == c and b == c (2^53 + 1 rounds to 2^53)
    ///
    /// which made `sort_by` panic with "user-provided comparison function
    /// does not correctly implement a total order".
    #[test]
    fn test_compare_sql_values_transitive_at_f64_precision_boundary() {
        use std::cmp::Ordering;

        let a = SqlValue::Bigint(9_007_199_254_740_992); // 2^53
        let b = SqlValue::Bigint(9_007_199_254_740_993); // 2^53 + 1
        let c = SqlValue::Double(9_007_199_254_740_992.0); // 2^53 exactly

        assert_eq!(compare_sql_values(&a, &b), Ordering::Less);
        assert_eq!(compare_sql_values(&a, &c), Ordering::Equal);
        // Pre-fix this returned Equal (lossy f64 cast), violating transitivity.
        assert_eq!(compare_sql_values(&b, &c), Ordering::Greater);

        // Integer vs Bigint is cross-variant but both are exact integers; the
        // comparison must be exact, not routed through f64.
        assert_eq!(
            compare_sql_values(
                &SqlValue::Integer(9_007_199_254_740_992),
                &SqlValue::Bigint(9_007_199_254_740_993)
            ),
            Ordering::Less
        );

        // NaN must occupy a fixed position in the total order (not collapse to
        // Equal against every numeric).
        let nan = SqlValue::Double(f64::NAN);
        assert_eq!(compare_sql_values(&nan, &nan), Ordering::Equal);
        let one = SqlValue::Integer(1);
        let cmp_nan_one = compare_sql_values(&nan, &one);
        assert_ne!(cmp_nan_one, Ordering::Equal);
        assert_eq!(compare_sql_values(&one, &nan), cmp_nan_one.reverse());
    }

    /// Regression test for #5802: sorting >= 100 mixed-type values (integers,
    /// bigints, doubles at the f64 precision boundary, NaN, text, blobs,
    /// booleans, NULLs) must not panic and must produce SQLite class ordering
    /// (numeric < text < blob) with NULLs last (compare_sql_values contract).
    #[test]
    fn test_compare_sql_values_total_order_mixed_types_sort() {
        use std::cmp::Ordering;

        use vibesql_types::StringValue;

        let mut values = Vec::new();
        for i in 0..8i64 {
            let base = 9_007_199_254_740_992i64; // 2^53
            values.push(SqlValue::Bigint(base));
            values.push(SqlValue::Bigint(base + 1));
            values.push(SqlValue::Integer(base));
            values.push(SqlValue::Integer(base + 1));
            values.push(SqlValue::Double(9_007_199_254_740_992.0));
            values.push(SqlValue::Double(9.007_199_254_740_993e15));
            values.push(SqlValue::Double(f64::NAN));
            values.push(SqlValue::Numeric(1.5));
            values.push(SqlValue::Real(-2.5));
            values.push(SqlValue::Integer(i));
            values.push(SqlValue::Unsigned(u64::MAX));
            values.push(SqlValue::Varchar(StringValue::from(format!("t{i}"))));
            values.push(SqlValue::Blob(vec![i as u8, 0x80]));
            values.push(SqlValue::Boolean(i % 2 == 0));
            values.push(SqlValue::Null);
        }
        assert!(values.len() >= 100, "need >= 100 rows to reproduce the fuzz panic");

        // Pre-fix this panicked: "user-provided comparison function does not
        // correctly implement a total order" (issue #5802 / fuzz.test).
        values.sort_by(compare_sql_values);

        // Class ordering: numeric(0) < text(1) < blob/boolean/other(2+) < NULL(last)
        fn class_of(v: &SqlValue) -> u8 {
            match v {
                SqlValue::Integer(_)
                | SqlValue::Bigint(_)
                | SqlValue::Smallint(_)
                | SqlValue::Unsigned(_)
                | SqlValue::Float(_)
                | SqlValue::Real(_)
                | SqlValue::Double(_)
                | SqlValue::Numeric(_) => 0,
                SqlValue::Character(_) | SqlValue::Varchar(_) => 1,
                SqlValue::Blob(_) => 2,
                SqlValue::Null => 4,
                _ => 3,
            }
        }
        let classes: Vec<u8> = values.iter().map(class_of).collect();
        let mut sorted_classes = classes.clone();
        sorted_classes.sort_unstable();
        assert_eq!(
            classes, sorted_classes,
            "sorted output must group classes as numeric < text < blob < other < NULL"
        );

        // The sort result must be consistent with the comparator on every
        // adjacent pair (sanity check that the order is well-formed).
        for pair in values.windows(2) {
            assert_ne!(
                compare_sql_values(&pair[0], &pair[1]),
                Ordering::Greater,
                "adjacent pair out of order: {:?} > {:?}",
                pair[0],
                pair[1]
            );
        }
    }
}
