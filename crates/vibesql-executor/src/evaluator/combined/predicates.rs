//! Predicate evaluation for combined expressions (BETWEEN, LIKE, IN, EXTRACT)

use super::super::{
    core::{CombinedExpressionEvaluator, ExpressionEvaluator},
    pattern::like_match,
};
use crate::errors::ExecutorError;
use chrono::{Datelike, Timelike};

impl CombinedExpressionEvaluator<'_> {
    /// Evaluate BETWEEN predicate: expr BETWEEN low AND high
    /// Standard BETWEEN: returns false if low > high (per SQLite behavior)
    /// BETWEEN SYMMETRIC: swaps low and high if low > high before evaluation
    /// If negated: expr < low OR expr > high
    ///
    /// NULL handling (SQLite/SQL:1999 three-valued logic):
    /// - NULL propagates through comparisons naturally (x < NULL → NULL, x > NULL → NULL)
    /// - For BETWEEN: (expr >= low) AND (expr <= high)
    ///   - If any comparison is NULL: NULL AND ... → NULL or FALSE (per three-valued AND)
    /// - For NOT BETWEEN: (expr < low) OR (expr > high)
    ///   - If any comparison is NULL: NULL OR ... → TRUE or NULL (per three-valued OR)
    ///   - Example: 93 NOT BETWEEN NULL AND 10 = (NULL) OR (TRUE) = TRUE
    pub(super) fn eval_between(
        &self,
        expr: &vibesql_ast::Expression,
        low: &vibesql_ast::Expression,
        high: &vibesql_ast::Expression,
        negated: bool,
        symmetric: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
        let expr_val = self.eval(expr, row)?;
        let mut low_val = self.eval(low, row)?;
        let mut high_val = self.eval(high, row)?;

        // Check if bounds are reversed (low > high)
        let gt_result = ExpressionEvaluator::eval_binary_op_static(
            &low_val,
            &vibesql_ast::BinaryOperator::GreaterThan,
            &high_val,
            sql_mode.clone(),
        )?;

        if let vibesql_types::SqlValue::Boolean(true) = gt_result {
            if symmetric {
                // For SYMMETRIC: swap bounds to normalize range
                std::mem::swap(&mut low_val, &mut high_val);
            } else {
                // For standard BETWEEN with reversed bounds: return empty set
                // Per SQLite behavior: BETWEEN 57.93 AND 43.23 returns no rows
                // - BETWEEN with reversed bounds returns false
                // - NOT BETWEEN with reversed bounds returns true
                //
                // However, if expr is NULL, we must preserve NULL semantics:
                // - NULL BETWEEN reversed_bounds = NULL (not FALSE)
                // - NOT NULL BETWEEN reversed_bounds = NULL (not TRUE)
                if matches!(expr_val, vibesql_types::SqlValue::Null) {
                    return Ok(vibesql_types::SqlValue::Null);
                }
                return Ok(vibesql_types::SqlValue::Boolean(negated));
            }
        }

        // Check if expr >= low
        let ge_low = ExpressionEvaluator::eval_binary_op_static(
            &expr_val,
            &vibesql_ast::BinaryOperator::GreaterThanOrEqual,
            &low_val,
            sql_mode.clone(),
        )?;

        // Check if expr <= high
        let le_high = ExpressionEvaluator::eval_binary_op_static(
            &expr_val,
            &vibesql_ast::BinaryOperator::LessThanOrEqual,
            &high_val,
            sql_mode.clone(),
        )?;

        // Combine with AND/OR depending on negated
        if negated {
            // NOT BETWEEN: expr < low OR expr > high
            let lt_low = ExpressionEvaluator::eval_binary_op_static(
                &expr_val,
                &vibesql_ast::BinaryOperator::LessThan,
                &low_val,
                sql_mode.clone(),
            )?;
            let gt_high = ExpressionEvaluator::eval_binary_op_static(
                &expr_val,
                &vibesql_ast::BinaryOperator::GreaterThan,
                &high_val,
                sql_mode.clone(),
            )?;
            ExpressionEvaluator::eval_binary_op_static(
                &lt_low,
                &vibesql_ast::BinaryOperator::Or,
                &gt_high,
                sql_mode,
            )
        } else {
            // BETWEEN: expr >= low AND expr <= high
            ExpressionEvaluator::eval_binary_op_static(
                &ge_low,
                &vibesql_ast::BinaryOperator::And,
                &le_high,
                sql_mode,
            )
        }
    }

    /// Evaluate LIKE pattern matching: expr LIKE pattern
    /// Supports wildcards: % (any chars), _ (single char)
    pub(super) fn eval_like(
        &self,
        expr: &vibesql_ast::Expression,
        pattern: &vibesql_ast::Expression,
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let expr_val = self.eval(expr, row)?;
        let pattern_val = self.eval(pattern, row)?;

        // Extract string values
        let text = match expr_val {
            vibesql_types::SqlValue::Varchar(ref s) | vibesql_types::SqlValue::Character(ref s) => {
                s.clone()
            }
            vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: expr_val,
                    op: "LIKE".to_string(),
                    right: pattern_val,
                })
            }
        };

        let pattern_str = match pattern_val {
            vibesql_types::SqlValue::Varchar(ref s) | vibesql_types::SqlValue::Character(ref s) => {
                s.clone()
            }
            vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: expr_val,
                    op: "LIKE".to_string(),
                    right: pattern_val,
                })
            }
        };

        // Perform pattern matching
        let matches = like_match(&text, &pattern_str);

        // Apply negation if needed
        let result = if negated { !matches } else { matches };

        Ok(vibesql_types::SqlValue::Boolean(result))
    }

    /// Evaluate TRIM expression: TRIM([position] [removal_char FROM] string)
    pub(super) fn eval_trim(
        &self,
        position: &Option<vibesql_ast::TrimPosition>,
        removal_char: &Option<Box<vibesql_ast::Expression>>,
        string: &vibesql_ast::Expression,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let string_val = self.eval(string, row)?;

        // Handle NULL string
        if matches!(string_val, vibesql_types::SqlValue::Null) {
            return Ok(vibesql_types::SqlValue::Null);
        }

        // Extract the string value
        let s = match &string_val {
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                &**s
            }
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: string_val.clone(),
                    op: "TRIM".to_string(),
                    right: vibesql_types::SqlValue::Null,
                })
            }
        };

        // Determine the character(s) to remove
        let char_to_remove: String = if let Some(removal_expr) = removal_char {
            let removal_val = self.eval(removal_expr, row)?;

            // Handle NULL removal character
            if matches!(removal_val, vibesql_types::SqlValue::Null) {
                return Ok(vibesql_types::SqlValue::Null);
            }

            match removal_val {
                vibesql_types::SqlValue::Varchar(c) | vibesql_types::SqlValue::Character(c) => c.to_string(),
                _ => {
                    return Err(ExecutorError::TypeMismatch {
                        left: removal_val.clone(),
                        op: "TRIM".to_string(),
                        right: string_val.clone(),
                    })
                }
            }
        } else {
            " ".to_string() // Default to space
        };

        let char_to_remove_str = char_to_remove.as_str();

        // Apply trimming based on position (default is Both)
        let result = match position.as_ref().unwrap_or(&vibesql_ast::TrimPosition::Both) {
            vibesql_ast::TrimPosition::Both => {
                // Trim from both sides
                let mut result = s;
                while result.starts_with(char_to_remove_str) && !result.is_empty() {
                    result = &result[char_to_remove_str.len()..];
                }
                while result.ends_with(char_to_remove_str) && !result.is_empty() {
                    result = &result[..result.len() - char_to_remove_str.len()];
                }
                result.to_string()
            }
            vibesql_ast::TrimPosition::Leading => {
                // Trim from start only
                let mut result = s;
                while result.starts_with(char_to_remove_str) && !result.is_empty() {
                    result = &result[char_to_remove_str.len()..];
                }
                result.to_string()
            }
            vibesql_ast::TrimPosition::Trailing => {
                // Trim from end only
                let mut result = s;
                while result.ends_with(char_to_remove_str) && !result.is_empty() {
                    result = &result[..result.len() - char_to_remove_str.len()];
                }
                result.to_string()
            }
        };

        Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(result)))
    }

    /// Evaluate IN operator with value list: expr IN (val1, val2, ...)
    /// SQL:1999 Section 8.4: IN predicate
    /// Returns TRUE if expr equals any value in the list
    /// Returns FALSE if no match and no NULLs
    /// Returns NULL if no match and list contains NULL
    ///
    /// Special case: empty list returns FALSE for IN, TRUE for NOT IN
    /// (SQLite behavior, SQL:1999 extension)
    ///
    /// Performance optimization: For lists > 3 items, uses HashSet for O(1) lookup
    pub(super) fn eval_in_list(
        &self,
        expr: &vibesql_ast::Expression,
        values: &[vibesql_ast::Expression],
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Empty set optimization (SQLite behavior):
        // If the list is empty, return FALSE for IN, TRUE for NOT IN
        // This is true regardless of whether the left expression is NULL
        // Rationale: No value can match an empty set
        // (SQLite behavior, SQL:1999 extension, not standard SQL)
        if values.is_empty() {
            return Ok(vibesql_types::SqlValue::Boolean(negated));
        }

        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
        let expr_val = self.eval(expr, row)?;

        // If left expression is NULL, result is NULL (per SQL three-valued logic)
        if matches!(expr_val, vibesql_types::SqlValue::Null) {
            return Ok(vibesql_types::SqlValue::Null);
        }

        // For small lists (≤3 items), use linear search to avoid HashSet overhead
        // For larger lists, use HashSet for O(1) lookup performance
        if values.len() <= 3 {
            // Original linear search implementation for small lists
            let mut found_null = false;

            // Check each value in the list
            for value_expr in values {
                let value = self.eval(value_expr, row)?;

                // Track if we encounter NULL
                if matches!(value, vibesql_types::SqlValue::Null) {
                    found_null = true;
                    continue;
                }

                // Compare using equality
                let eq_result = ExpressionEvaluator::eval_binary_op_static(
                    &expr_val,
                    &vibesql_ast::BinaryOperator::Equal,
                    &value,
                    sql_mode.clone(),
                )?;

                // If we found a match, return TRUE (or FALSE if negated)
                if matches!(eq_result, vibesql_types::SqlValue::Boolean(true)) {
                    return Ok(vibesql_types::SqlValue::Boolean(!negated));
                }
            }

            // No match found
            // If we encountered NULL, return NULL (per SQL three-valued logic)
            // Otherwise return FALSE (or TRUE if negated)
            if found_null {
                Ok(vibesql_types::SqlValue::Null)
            } else {
                Ok(vibesql_types::SqlValue::Boolean(negated))
            }
        } else {
            // HashSet optimization for larger lists
            // Evaluate each IN list value once (instead of per row) and collect into HashSet
            // Then use eval_binary_op for comparison to preserve SQL type coercion
            let mut value_set = std::collections::HashSet::new();
            let mut found_null = false;

            // Evaluate all values once and build the HashSet
            for value_expr in values {
                let value = self.eval(value_expr, row)?;

                if matches!(value, vibesql_types::SqlValue::Null) {
                    found_null = true;
                } else {
                    value_set.insert(value);
                }
            }

            // O(n) lookup with SQL type coercion (where n = unique values in list)
            // This preserves correctness while still benefiting from single evaluation of IN list
            for value in &value_set {
                let eq_result = ExpressionEvaluator::eval_binary_op_static(
                    &expr_val,
                    &vibesql_ast::BinaryOperator::Equal,
                    value,
                    sql_mode.clone(),
                )?;

                if matches!(eq_result, vibesql_types::SqlValue::Boolean(true)) {
                    return Ok(vibesql_types::SqlValue::Boolean(!negated));
                }
            }

            // No match found: return NULL if list contained NULL, else FALSE
            if found_null {
                Ok(vibesql_types::SqlValue::Null)
            } else {
                Ok(vibesql_types::SqlValue::Boolean(negated))
            }
        }
    }

    /// Evaluate IS NULL / IS NOT NULL
    pub(super) fn eval_is_null(
        &self,
        expr: &vibesql_ast::Expression,
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let value = self.eval(expr, row)?;
        let is_null = matches!(value, vibesql_types::SqlValue::Null);
        let result = if negated { !is_null } else { is_null };
        Ok(vibesql_types::SqlValue::Boolean(result))
    }

    /// Evaluate POSITION expression: POSITION(substring IN string)
    pub(super) fn eval_position(
        &self,
        substring: &vibesql_ast::Expression,
        string: &vibesql_ast::Expression,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let substring_val = self.eval(substring, row)?;
        let string_val = self.eval(string, row)?;

        match (&substring_val, &string_val) {
            (vibesql_types::SqlValue::Null, _) | (_, vibesql_types::SqlValue::Null) => {
                Ok(vibesql_types::SqlValue::Null)
            }
            (
                vibesql_types::SqlValue::Varchar(needle)
                | vibesql_types::SqlValue::Character(needle),
                vibesql_types::SqlValue::Varchar(haystack)
                | vibesql_types::SqlValue::Character(haystack),
            ) => match haystack.find(&**needle) {
                Some(pos) => Ok(vibesql_types::SqlValue::Integer((pos + 1) as i64)),
                None => Ok(vibesql_types::SqlValue::Integer(0)),
            },
            _ => Err(ExecutorError::TypeMismatch {
                left: substring_val.clone(),
                op: "POSITION".to_string(),
                right: string_val.clone(),
            }),
        }
    }

    /// Evaluate EXTRACT expression: EXTRACT(field FROM expr)
    /// Extracts a date/time component from a datetime expression
    pub(super) fn eval_extract(
        &self,
        field: &vibesql_ast::IntervalUnit,
        expr: &vibesql_ast::Expression,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let datetime_val = self.eval(expr, row)?;

        // Handle NULL
        if matches!(datetime_val, vibesql_types::SqlValue::Null) {
            return Ok(vibesql_types::SqlValue::Null);
        }

        // Extract component based on field type
        use vibesql_ast::IntervalUnit;

        match &datetime_val {
            vibesql_types::SqlValue::Date(d) => {
                let result = match field {
                    IntervalUnit::Year => d.year as i64,
                    IntervalUnit::Month => d.month as i64,
                    IntervalUnit::Day => d.day as i64,
                    IntervalUnit::Quarter => ((d.month - 1) / 3 + 1) as i64,
                    IntervalUnit::Week => {
                        // Calculate ISO week number using chrono
                        let chrono_date =
                            chrono::NaiveDate::from_ymd_opt(d.year, d.month as u32, d.day as u32)
                                .unwrap_or_else(|| {
                                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                                });
                        chrono_date.iso_week().week() as i64
                    }
                    _ => {
                        return Err(ExecutorError::InvalidExtractField {
                            field: format!("{:?}", field),
                            value_type: "DATE".to_string(),
                        })
                    }
                };
                Ok(vibesql_types::SqlValue::Integer(result))
            }

            vibesql_types::SqlValue::Time(t) => {
                let result = match field {
                    IntervalUnit::Hour => t.hour as i64,
                    IntervalUnit::Minute => t.minute as i64,
                    IntervalUnit::Second => t.second as i64,
                    IntervalUnit::Microsecond => (t.nanosecond / 1000) as i64,
                    _ => {
                        return Err(ExecutorError::InvalidExtractField {
                            field: format!("{:?}", field),
                            value_type: "TIME".to_string(),
                        })
                    }
                };
                Ok(vibesql_types::SqlValue::Integer(result))
            }

            vibesql_types::SqlValue::Timestamp(ts) => {
                let result = match field {
                    IntervalUnit::Year => ts.date.year as i64,
                    IntervalUnit::Month => ts.date.month as i64,
                    IntervalUnit::Day => ts.date.day as i64,
                    IntervalUnit::Hour => ts.time.hour as i64,
                    IntervalUnit::Minute => ts.time.minute as i64,
                    IntervalUnit::Second => ts.time.second as i64,
                    IntervalUnit::Microsecond => (ts.time.nanosecond / 1000) as i64,
                    IntervalUnit::Quarter => ((ts.date.month - 1) / 3 + 1) as i64,
                    IntervalUnit::Week => {
                        // Calculate ISO week number using chrono
                        let chrono_date = chrono::NaiveDate::from_ymd_opt(
                            ts.date.year,
                            ts.date.month as u32,
                            ts.date.day as u32,
                        )
                        .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                        chrono_date.iso_week().week() as i64
                    }
                    _ => {
                        return Err(ExecutorError::InvalidExtractField {
                            field: format!("{:?}", field),
                            value_type: "TIMESTAMP".to_string(),
                        })
                    }
                };
                Ok(vibesql_types::SqlValue::Integer(result))
            }

            // For string values, try to parse as date/timestamp
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                // Try to parse as date first
                if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let result = match field {
                        IntervalUnit::Year => date.year() as i64,
                        IntervalUnit::Month => date.month() as i64,
                        IntervalUnit::Day => date.day() as i64,
                        IntervalUnit::Quarter => ((date.month() - 1) / 3 + 1) as i64,
                        IntervalUnit::Week => date.iso_week().week() as i64,
                        _ => {
                            return Err(ExecutorError::InvalidExtractField {
                                field: format!("{:?}", field),
                                value_type: "DATE".to_string(),
                            })
                        }
                    };
                    return Ok(vibesql_types::SqlValue::Integer(result));
                }

                // Try to parse as timestamp
                if let Ok(ts) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    let result = match field {
                        IntervalUnit::Year => ts.year() as i64,
                        IntervalUnit::Month => ts.month() as i64,
                        IntervalUnit::Day => ts.day() as i64,
                        IntervalUnit::Hour => ts.hour() as i64,
                        IntervalUnit::Minute => ts.minute() as i64,
                        IntervalUnit::Second => ts.second() as i64,
                        IntervalUnit::Microsecond => ts.nanosecond() as i64 / 1000,
                        IntervalUnit::Quarter => ((ts.month() - 1) / 3 + 1) as i64,
                        IntervalUnit::Week => ts.iso_week().week() as i64,
                        _ => {
                            return Err(ExecutorError::InvalidExtractField {
                                field: format!("{:?}", field),
                                value_type: "TIMESTAMP".to_string(),
                            })
                        }
                    };
                    return Ok(vibesql_types::SqlValue::Integer(result));
                }

                Err(ExecutorError::TypeMismatch {
                    left: datetime_val.clone(),
                    op: "EXTRACT".to_string(),
                    right: vibesql_types::SqlValue::Null,
                })
            }

            _ => Err(ExecutorError::TypeMismatch {
                left: datetime_val.clone(),
                op: "EXTRACT".to_string(),
                right: vibesql_types::SqlValue::Null,
            }),
        }
    }
}
