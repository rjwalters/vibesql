//! Predicate evaluation methods (BETWEEN, LIKE, IN, POSITION, EXTRACT)

use super::super::{casting::cast_value, core::ExpressionEvaluator, pattern::like_match};
use crate::errors::ExecutorError;
use chrono::{Datelike, Timelike};

impl ExpressionEvaluator<'_> {
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
    #[inline]
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
        let gt_result = Self::eval_binary_op_static(
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

        let ge_low = Self::eval_binary_op_static(
            &expr_val,
            &vibesql_ast::BinaryOperator::GreaterThanOrEqual,
            &low_val,
            sql_mode.clone(),
        )?;

        let le_high = Self::eval_binary_op_static(
            &expr_val,
            &vibesql_ast::BinaryOperator::LessThanOrEqual,
            &high_val,
            sql_mode.clone(),
        )?;

        if negated {
            let lt_low = Self::eval_binary_op_static(
                &expr_val,
                &vibesql_ast::BinaryOperator::LessThan,
                &low_val,
                sql_mode.clone(),
            )?;
            let gt_high = Self::eval_binary_op_static(
                &expr_val,
                &vibesql_ast::BinaryOperator::GreaterThan,
                &high_val,
                sql_mode.clone(),
            )?;
            Self::eval_binary_op_static(
                &lt_low,
                &vibesql_ast::BinaryOperator::Or,
                &gt_high,
                sql_mode,
            )
        } else {
            Self::eval_binary_op_static(
                &ge_low,
                &vibesql_ast::BinaryOperator::And,
                &le_high,
                sql_mode,
            )
        }
    }

    /// Evaluate CAST expression
    #[inline]
    pub(super) fn eval_cast(
        &self,
        expr: &vibesql_ast::Expression,
        data_type: &vibesql_types::DataType,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let value = self.eval(expr, row)?;
        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
        cast_value(&value, data_type, &sql_mode)
    }

    /// Evaluate POSITION expression
    #[inline]
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

    /// Evaluate TRIM expression
    /// TRIM([position] [removal_char FROM] string)
    #[inline]
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
        // Need to extract owned String to avoid lifetime issues
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

    /// Evaluate EXTRACT expression
    /// EXTRACT(field FROM expr) - extracts date/time component from datetime expression
    #[inline]
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

    /// Evaluate LIKE predicate
    #[inline]
    pub(super) fn eval_like(
        &self,
        expr: &vibesql_ast::Expression,
        pattern: &vibesql_ast::Expression,
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let expr_val = self.eval(expr, row)?;
        let pattern_val = self.eval(pattern, row)?;

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

        let matches = like_match(&text, &pattern_str);
        let result = if negated { !matches } else { matches };

        Ok(vibesql_types::SqlValue::Boolean(result))
    }

    /// Evaluate IN list predicate with HashSet optimization
    /// For lists > 3 items, uses HashSet for O(1) lookup instead of O(n) linear scan
    #[inline]
    pub(super) fn eval_in_list(
        &self,
        expr: &vibesql_ast::Expression,
        values: &[vibesql_ast::Expression],
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Handle empty IN list: returns false for IN, true for NOT IN
        // This is per SQLite behavior (SQL:1999 extension, not standard SQL)
        if values.is_empty() {
            return Ok(vibesql_types::SqlValue::Boolean(negated));
        }

        let expr_val = self.eval(expr, row)?;

        // SQL standard behavior for NULL IN (list):
        // - NULL IN (empty list) → FALSE (already handled above)
        // - NULL IN (non-empty list) → NULL (three-valued logic)
        // The IN operator returns NULL when comparing NULL to any value
        if matches!(expr_val, vibesql_types::SqlValue::Null) {
            return Ok(vibesql_types::SqlValue::Null);
        }

        // For small lists (≤3 items), use linear search to avoid HashSet overhead
        // For larger lists, use HashSet for O(1) lookup performance
        if values.len() <= 3 {
            // Original linear search implementation for small lists
            let mut found_null = false;

            for value_expr in values {
                let value = self.eval(value_expr, row)?;

                if matches!(value, vibesql_types::SqlValue::Null) {
                    found_null = true;
                    continue;
                }

                let eq_result =
                    self.eval_binary_op(&expr_val, &vibesql_ast::BinaryOperator::Equal, &value)?;

                if matches!(eq_result, vibesql_types::SqlValue::Boolean(true)) {
                    return Ok(vibesql_types::SqlValue::Boolean(!negated));
                }
            }

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
                let eq_result =
                    self.eval_binary_op(&expr_val, &vibesql_ast::BinaryOperator::Equal, value)?;

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
}
