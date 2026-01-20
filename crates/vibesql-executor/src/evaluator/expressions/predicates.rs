//! Predicate evaluation methods (BETWEEN, LIKE, IN, POSITION, EXTRACT)

use chrono::{Datelike, Timelike};

use super::super::{
    casting::cast_value,
    core::ExpressionEvaluator,
    pattern::{glob_match, like_match},
};
use crate::errors::ExecutorError;

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
        use vibesql_types::SqlValue;

        // Handle row value constructor BETWEEN
        // (a, b) BETWEEN (low_a, low_b) AND (high_a, high_b)
        // is equivalent to: (a, b) >= (low_a, low_b) AND (a, b) <= (high_a, high_b)
        if let (
            vibesql_ast::Expression::RowValueConstructor(expr_exprs),
            vibesql_ast::Expression::RowValueConstructor(low_exprs),
            vibesql_ast::Expression::RowValueConstructor(high_exprs),
        ) = (expr, low, high)
        {
            return self.eval_row_value_between(
                expr_exprs, low_exprs, high_exprs, negated, symmetric, row,
            );
        }

        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();

        // Get effective collation: explicit COLLATE or column-level collation
        let collation = self.get_expression_collation(expr);

        let expr_val = self.eval(expr, row)?;
        let low_val = self.eval(low, row)?;
        let high_val = self.eval(high, row)?;

        // Apply collation transformation if needed
        // NOCASE: uppercase all strings for case-insensitive comparison
        // RTRIM: trim trailing whitespace from all strings
        let transform_for_collation = |val: SqlValue, collation_name: &str| -> SqlValue {
            if collation_name.eq_ignore_ascii_case("nocase") {
                match val {
                    SqlValue::Varchar(s) => {
                        SqlValue::Varchar(arcstr::ArcStr::from(s.to_uppercase()))
                    }
                    SqlValue::Character(s) => {
                        SqlValue::Character(arcstr::ArcStr::from(s.to_uppercase()))
                    }
                    other => other,
                }
            } else if collation_name.eq_ignore_ascii_case("rtrim") {
                match val {
                    SqlValue::Varchar(s) => SqlValue::Varchar(arcstr::ArcStr::from(s.trim_end())),
                    SqlValue::Character(s) => {
                        SqlValue::Character(arcstr::ArcStr::from(s.trim_end()))
                    }
                    other => other,
                }
            } else {
                val
            }
        };

        let (expr_val, low_val, high_val) = if let Some(ref collation_name) = collation {
            (
                transform_for_collation(expr_val, collation_name),
                transform_for_collation(low_val, collation_name),
                transform_for_collation(high_val, collation_name),
            )
        } else {
            (expr_val, low_val, high_val)
        };

        // Apply SQLite type affinity rules for BETWEEN comparisons
        // TEXT column vs INTEGER literal → convert INTEGER to TEXT, string compare
        //
        // IMPORTANT: Apply affinity transformations independently for each comparison.
        // The expr value may be transformed differently when compared to low vs high.
        // Example: '-1' BETWEEN 0 AND col where col is INTEGER:
        //   - For '-1' >= 0: string vs integer → use type ordering (TEXT > INTEGER → TRUE)
        //   - For '-1' <= col: string vs INTEGER column → coerce '-1' to -1 → -1 <= col
        let (expr_val_for_low, mut low_val) =
            self.apply_affinity_for_comparison(expr, expr_val.clone(), low, low_val);
        let (expr_val_for_high, mut high_val) =
            self.apply_affinity_for_comparison(expr, expr_val.clone(), high, high_val);

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
            &expr_val_for_low,
            &vibesql_ast::BinaryOperator::GreaterThanOrEqual,
            &low_val,
            sql_mode.clone(),
        )?;

        let le_high = Self::eval_binary_op_static(
            &expr_val_for_high,
            &vibesql_ast::BinaryOperator::LessThanOrEqual,
            &high_val,
            sql_mode.clone(),
        )?;

        if negated {
            let lt_low = Self::eval_binary_op_static(
                &expr_val_for_low,
                &vibesql_ast::BinaryOperator::LessThan,
                &low_val,
                sql_mode.clone(),
            )?;
            let gt_high = Self::eval_binary_op_static(
                &expr_val_for_high,
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
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => &**s,
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
                vibesql_types::SqlValue::Varchar(c) | vibesql_types::SqlValue::Character(c) => {
                    c.to_string()
                }
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
        escape: &Option<Box<vibesql_ast::Expression>>,
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let expr_val = self.eval(expr, row)?;
        let pattern_val = self.eval(pattern, row)?;

        // Issue #4913: SQLite coerces numeric types to strings for LIKE comparison
        let text = match &expr_val {
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                s.clone()
            }
            vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
            // SQLite coerces numeric types to strings for LIKE comparison
            vibesql_types::SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
            vibesql_types::SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
            vibesql_types::SqlValue::Float(f) => arcstr::ArcStr::from(f.to_string()),
            vibesql_types::SqlValue::Double(f) => arcstr::ArcStr::from(f.to_string()),
            vibesql_types::SqlValue::Real(f) => arcstr::ArcStr::from(f.to_string()),
            // Blob values are coerced to hex representation in SQLite
            vibesql_types::SqlValue::Blob(b) => {
                let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                arcstr::ArcStr::from(hex)
            }
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: expr_val,
                    op: "LIKE".to_string(),
                    right: pattern_val,
                })
            }
        };

        let pattern_str = match &pattern_val {
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                s.clone()
            }
            vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
            // SQLite coerces numeric types to strings for LIKE comparison
            vibesql_types::SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
            vibesql_types::SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
            vibesql_types::SqlValue::Float(f) => arcstr::ArcStr::from(f.to_string()),
            vibesql_types::SqlValue::Double(f) => arcstr::ArcStr::from(f.to_string()),
            vibesql_types::SqlValue::Real(f) => arcstr::ArcStr::from(f.to_string()),
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: expr_val,
                    op: "LIKE".to_string(),
                    right: pattern_val,
                })
            }
        };

        // Evaluate the escape character if provided
        let escape_char = if let Some(escape_expr) = escape {
            let escape_val = self.eval(escape_expr, row)?;
            match escape_val {
                vibesql_types::SqlValue::Varchar(ref s)
                | vibesql_types::SqlValue::Character(ref s) => {
                    let mut chars = s.chars();
                    match (chars.next(), chars.next()) {
                        (Some(c), None) => Some(c), // Exactly one character
                        _ => {
                            // Empty string or multi-character string: error per SQLite
                            return Err(ExecutorError::SqliteCompatError(
                                "ESCAPE expression must be a single character".to_string(),
                            ));
                        }
                    }
                }
                vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
                vibesql_types::SqlValue::Integer(n) => {
                    // Allow single-digit integers as escape character (SQLite compatibility)
                    let s = n.to_string();
                    let mut chars = s.chars();
                    match (chars.next(), chars.next()) {
                        (Some(c), None) => Some(c), // Exactly one character
                        _ => {
                            return Err(ExecutorError::SqliteCompatError(
                                "ESCAPE expression must be a single character".to_string(),
                            ))
                        }
                    }
                }
                _ => {
                    return Err(ExecutorError::TypeMismatch {
                        left: escape_val,
                        op: "ESCAPE".to_string(),
                        right: vibesql_types::SqlValue::Null,
                    })
                }
            }
        } else {
            None
        };

        // Get case_sensitive_like setting from database (default: false = case-insensitive)
        let case_sensitive = self.database.map(|db| db.case_sensitive_like()).unwrap_or(false);
        let matches = like_match(&text, &pattern_str, case_sensitive, escape_char);
        let result = if negated { !matches } else { matches };

        Ok(vibesql_types::SqlValue::Boolean(result))
    }

    /// Evaluate GLOB predicate (SQLite)
    #[inline]
    pub(super) fn eval_glob(
        &self,
        expr: &vibesql_ast::Expression,
        pattern: &vibesql_ast::Expression,
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let expr_val = self.eval(expr, row)?;
        let pattern_val = self.eval(pattern, row)?;

        // Issue #4913: SQLite coerces numeric types to strings for GLOB comparison
        let text = match &expr_val {
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                s.clone()
            }
            vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
            // SQLite coerces numeric types to strings for GLOB comparison
            vibesql_types::SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
            vibesql_types::SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
            vibesql_types::SqlValue::Float(f) => arcstr::ArcStr::from(f.to_string()),
            vibesql_types::SqlValue::Double(f) => arcstr::ArcStr::from(f.to_string()),
            vibesql_types::SqlValue::Real(f) => arcstr::ArcStr::from(f.to_string()),
            // Blob values are coerced to hex representation in SQLite
            vibesql_types::SqlValue::Blob(b) => {
                let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
                arcstr::ArcStr::from(hex)
            }
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: expr_val,
                    op: "GLOB".to_string(),
                    right: pattern_val,
                })
            }
        };

        let pattern_str = match &pattern_val {
            vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
                s.clone()
            }
            vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
            // SQLite coerces numeric types to strings for GLOB comparison
            vibesql_types::SqlValue::Integer(i) => arcstr::ArcStr::from(i.to_string()),
            vibesql_types::SqlValue::Bigint(i) => arcstr::ArcStr::from(i.to_string()),
            vibesql_types::SqlValue::Float(f) => arcstr::ArcStr::from(f.to_string()),
            vibesql_types::SqlValue::Double(f) => arcstr::ArcStr::from(f.to_string()),
            vibesql_types::SqlValue::Real(f) => arcstr::ArcStr::from(f.to_string()),
            _ => {
                return Err(ExecutorError::TypeMismatch {
                    left: expr_val,
                    op: "GLOB".to_string(),
                    right: pattern_val,
                })
            }
        };

        let matches = glob_match(&text, &pattern_str);
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

                // Apply SQLite type affinity rules for IN comparisons
                // Note: IN expressions have different affinity rules than regular comparisons
                let (expr_coerced, value_coerced) = self.apply_affinity_for_in_comparison(
                    expr,
                    expr_val.clone(),
                    value_expr,
                    value,
                );

                let eq_result = self.eval_binary_op(
                    &expr_coerced,
                    &vibesql_ast::BinaryOperator::Equal,
                    &value_coerced,
                )?;

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
            // Apply affinity coercion before adding to the HashSet
            let mut value_set = std::collections::HashSet::new();
            let mut found_null = false;

            // Evaluate all values once, apply affinity, and build the HashSet
            for value_expr in values {
                let value = self.eval(value_expr, row)?;

                if matches!(value, vibesql_types::SqlValue::Null) {
                    found_null = true;
                } else {
                    // Apply SQLite type affinity rules for IN comparisons
                    let (_, value_coerced) = self.apply_affinity_for_in_comparison(
                        expr,
                        expr_val.clone(),
                        value_expr,
                        value,
                    );
                    value_set.insert(value_coerced);
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

    /// Evaluate row value BETWEEN predicate
    ///
    /// `(a, b) BETWEEN (low_a, low_b) AND (high_a, high_b)`
    /// is equivalent to: `(a, b) >= (low_a, low_b) AND (a, b) <= (high_a, high_b)`
    ///
    /// NULL handling follows the standard row value comparison semantics.
    fn eval_row_value_between(
        &self,
        expr_exprs: &[vibesql_ast::Expression],
        low_exprs: &[vibesql_ast::Expression],
        high_exprs: &[vibesql_ast::Expression],
        negated: bool,
        symmetric: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        use vibesql_types::SqlValue;

        // Row values must have the same number of elements
        if expr_exprs.len() != low_exprs.len() || expr_exprs.len() != high_exprs.len() {
            return Err(ExecutorError::UnsupportedExpression(format!(
                "Row value constructor size mismatch in BETWEEN: expr has {} elements, low has {}, high has {}",
                expr_exprs.len(),
                low_exprs.len(),
                high_exprs.len()
            )));
        }

        if expr_exprs.is_empty() {
            return Err(ExecutorError::UnsupportedExpression(
                "Empty row value constructors are not allowed".to_string(),
            ));
        }

        // Determine effective bounds (swap if symmetric and low > high)
        let (effective_low, effective_high) = if symmetric {
            let gt_op = vibesql_ast::BinaryOperator::GreaterThan;
            let low_gt_high = self.eval_row_value_comparison(low_exprs, &gt_op, high_exprs, row)?;

            if matches!(low_gt_high, SqlValue::Boolean(true)) {
                (high_exprs, low_exprs) // Swap
            } else {
                (low_exprs, high_exprs)
            }
        } else {
            (low_exprs, high_exprs)
        };

        if negated {
            // NOT BETWEEN: expr < low OR expr > high
            let lt_op = vibesql_ast::BinaryOperator::LessThan;
            let gt_op = vibesql_ast::BinaryOperator::GreaterThan;

            let lt_low = self.eval_row_value_comparison(expr_exprs, &lt_op, effective_low, row)?;
            let gt_high =
                self.eval_row_value_comparison(expr_exprs, &gt_op, effective_high, row)?;

            // OR logic with three-valued semantics
            match (&lt_low, &gt_high) {
                (SqlValue::Boolean(true), _) | (_, SqlValue::Boolean(true)) => {
                    Ok(SqlValue::Boolean(true))
                }
                (SqlValue::Boolean(false), SqlValue::Boolean(false)) => {
                    Ok(SqlValue::Boolean(false))
                }
                _ => Ok(SqlValue::Null),
            }
        } else {
            // BETWEEN: expr >= low AND expr <= high
            let ge_op = vibesql_ast::BinaryOperator::GreaterThanOrEqual;
            let le_op = vibesql_ast::BinaryOperator::LessThanOrEqual;

            let ge_low = self.eval_row_value_comparison(expr_exprs, &ge_op, effective_low, row)?;
            let le_high =
                self.eval_row_value_comparison(expr_exprs, &le_op, effective_high, row)?;

            // AND logic with three-valued semantics
            match (&ge_low, &le_high) {
                (SqlValue::Boolean(true), SqlValue::Boolean(true)) => Ok(SqlValue::Boolean(true)),
                (SqlValue::Boolean(false), _) | (_, SqlValue::Boolean(false)) => {
                    Ok(SqlValue::Boolean(false))
                }
                _ => Ok(SqlValue::Null),
            }
        }
    }
}
