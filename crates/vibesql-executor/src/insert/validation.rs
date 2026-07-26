use crate::errors::ExecutorError;

/// Result of resolving INSERT column targets
pub struct ResolvedInsertColumns {
    /// Column indices and types for regular columns (schema columns)
    pub columns: Vec<(usize, vibesql_types::DataType)>,
    /// Position of rowid pseudo-column in the input column list, if specified
    /// This allows extracting the rowid value from VALUES for explicit rowid inserts
    pub rowid_position: Option<usize>,
    /// True if rowid_position refers to a separate pseudo-column (rowid, _rowid_, oid)
    /// False if rowid_position refers to an INTEGER PRIMARY KEY column
    /// This affects value count validation - pseudo-columns add 1 to the expected count
    pub rowid_is_pseudo_column: bool,
}

/// Check if a column name is a ROWID pseudo-column (SQLite compatibility)
/// Returns true for "rowid", "_rowid_", "oid" (case-insensitive)
fn is_rowid_pseudo_column(col_name: &str) -> bool {
    let lower = col_name.to_lowercase();
    lower == "rowid" || lower == "_rowid_" || lower == "oid"
}

/// Determine target column indices and types for an INSERT statement,
/// including support for the ROWID pseudo-column (SQLite compatibility)
pub fn resolve_target_columns_with_rowid(
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    specified_columns: &[String],
) -> Result<ResolvedInsertColumns, ExecutorError> {
    if specified_columns.is_empty() {
        // No columns specified: INSERT INTO t VALUES (...)
        // Use all non-generated columns in schema order
        // Generated columns (those with generated_expr) are excluded per SQLite behavior
        let columns: Vec<_> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.generated_expr.is_none())
            .map(|(idx, col)| (idx, col.data_type.clone()))
            .collect();

        // Detect INTEGER PRIMARY KEY column for rowid aliasing
        // When a single-column INTEGER PRIMARY KEY exists, that column's value
        // becomes the explicit rowid. Find its position in the filtered column list.
        let rowid_position = if let Some(ref pk_cols) = schema.primary_key {
            if pk_cols.len() == 1 {
                // Check if the PK column is INTEGER type (rowid alias)
                if let Some(pk_idx) = schema.get_column_index(&pk_cols[0]) {
                    let pk_col = &schema.columns[pk_idx];
                    // INTEGER PRIMARY KEY is a rowid alias if is_exact_integer_type is true
                    if pk_col.is_exact_integer_type {
                        // Find this column's position in the filtered column list
                        columns.iter().position(|(idx, _)| *idx == pk_idx)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(ResolvedInsertColumns {
            columns,
            rowid_position,
            rowid_is_pseudo_column: false, // INTEGER PRIMARY KEY, not a pseudo-column
        })
    } else {
        // Columns specified: INSERT INTO t (col1, col2) VALUES (...)
        // Validate and resolve columns, handling rowid pseudo-column specially
        let mut columns = Vec::new();
        let mut rowid_position = None;
        let mut rowid_is_pseudo_column = false;

        // Get INTEGER PRIMARY KEY column info if it exists (single-column INTEGER PK)
        let integer_pk_col_name = if let Some(ref pk_cols) = schema.primary_key {
            if pk_cols.len() == 1 {
                if let Some(pk_idx) = schema.get_column_index(&pk_cols[0]) {
                    let pk_col = &schema.columns[pk_idx];
                    if pk_col.is_exact_integer_type {
                        Some(pk_cols[0].to_lowercase())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        for (input_idx, col_name) in specified_columns.iter().enumerate() {
            // First check if there's a real column with this name
            if let Some(schema_idx) = schema.get_column_index(col_name) {
                let col = &schema.columns[schema_idx];
                // Check if this is a generated column - cannot INSERT into generated columns
                if col.generated_expr.is_some() {
                    return Err(ExecutorError::CannotInsertIntoGeneratedColumn {
                        column_name: col_name.to_string(),
                    });
                }
                columns.push((schema_idx, col.data_type.clone()));

                // Check if this is the INTEGER PRIMARY KEY column (rowid alias)
                if let Some(ref pk_name) = integer_pk_col_name {
                    if col_name.to_lowercase() == *pk_name {
                        // This column's value in VALUES will be the explicit rowid
                        rowid_position = Some(input_idx);
                        // Not a pseudo-column - it's a real column that aliases rowid
                        rowid_is_pseudo_column = false;
                    }
                }
            } else if is_rowid_pseudo_column(col_name) {
                // It's a rowid pseudo-column - record its position but don't add to columns
                if rowid_position.is_some() {
                    return Err(ExecutorError::UnsupportedExpression(
                        "Multiple rowid columns specified in INSERT".to_string(),
                    ));
                }
                rowid_position = Some(input_idx);
                rowid_is_pseudo_column = true;
            } else {
                // Column not found and not a pseudo-column
                // Use SQLite-compatible error: "table T has no column named C"
                return Err(ExecutorError::InsertNoSuchColumn {
                    table_name: table_name.to_string(),
                    column_name: col_name.to_string(),
                });
            }
        }

        Ok(ResolvedInsertColumns { columns, rowid_position, rowid_is_pseudo_column })
    }
}

/// Validate that each row has the correct number of values
///
/// SQLite checks in this order:
/// 1. All VALUES rows must have the same number of terms
/// 2. The number of terms must match the table column count
pub fn validate_row_column_counts(
    rows: &[Vec<vibesql_ast::Expression>],
    expected_count: usize,
    table_name: &str,
    has_explicit_columns: bool,
) -> Result<(), ExecutorError> {
    // First check: all VALUES rows must have the same number of terms
    if let Some(first_row) = rows.first() {
        let first_len = first_row.len();
        for value_exprs in rows.iter().skip(1) {
            if value_exprs.len() != first_len {
                return Err(ExecutorError::ValuesRowCountMismatch);
            }
        }
    }

    // Second check: number of terms must match expected column count
    for value_exprs in rows.iter() {
        if value_exprs.len() != expected_count {
            return Err(ExecutorError::InsertColumnCountMismatch {
                table_name: table_name.to_string(),
                expected: expected_count,
                provided: value_exprs.len(),
                has_explicit_columns,
            });
        }
    }
    Ok(())
}

/// Coerce a value to match the expected column type
/// Performs automatic type conversions where appropriate
pub fn coerce_value(
    value: vibesql_types::SqlValue,
    expected_type: &vibesql_types::DataType,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    use vibesql_types::{DataType, SqlValue};

    // NULL is valid for any type (NOT NULL constraint checked separately)
    if matches!(value, SqlValue::Null) {
        return Ok(value);
    }

    // Check type compatibility with automatic coercion
    match (&value, expected_type) {
        // Exact matches - no coercion needed
        (SqlValue::Integer(_), DataType::Integer) => Ok(value),
        (SqlValue::Varchar(_), DataType::Varchar { .. }) => Ok(value),
        (SqlValue::Character(_), DataType::Character { .. }) => Ok(value),
        (SqlValue::Boolean(_), DataType::Boolean) => Ok(value),

        // BOOLEAN-declared columns follow SQLite NUMERIC affinity. SQLite has no
        // native boolean storage class — a `BOOLEAN` column gets NUMERIC affinity
        // and stores 0/1 as integers (e.g. `[14_vac] boolean` accepting the
        // integer 0 in table-7.2). Mirror the NUMERIC-affinity arms below so a
        // BOOLEAN column accepts the same inputs a NUMERIC column would, without
        // forcing values into VibeSQL's strict Boolean storage class.
        // Numeric inputs are stored as-is; whole-valued reals collapse to integers.
        (SqlValue::Integer(_) | SqlValue::Bigint(_) | SqlValue::Smallint(_), DataType::Boolean) => {
            Ok(value)
        }
        (SqlValue::Numeric(f) | SqlValue::Double(f), DataType::Boolean) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Integer(*f as i64))
            } else {
                Ok(value)
            }
        }
        (SqlValue::Real(f), DataType::Boolean) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Integer(*f as i64))
            } else {
                Ok(value)
            }
        }
        (SqlValue::Float(f), DataType::Boolean) => {
            let f64_val = *f as f64;
            if f64_val.fract() == 0.0 && f64_val >= i64::MIN as f64 && f64_val <= i64::MAX as f64 {
                Ok(SqlValue::Integer(f64_val as i64))
            } else {
                Ok(SqlValue::Double(f64_val))
            }
        }
        // Text into a BOOLEAN column: NUMERIC affinity tries integer, then real,
        // otherwise the text is kept verbatim (SQLite behavior).
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Boolean) => {
            let trimmed = s.trim();
            if let Ok(i) = trimmed.parse::<i64>() {
                Ok(SqlValue::Integer(i))
            } else if let Ok(f) = trimmed.parse::<f64>() {
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(SqlValue::Integer(f as i64))
                } else {
                    Ok(SqlValue::Double(f))
                }
            } else {
                Ok(value)
            }
        }

        (SqlValue::Float(_), DataType::Float { .. }) => Ok(value),
        (SqlValue::Real(_), DataType::Real) => Ok(value),
        (SqlValue::Double(_), DataType::DoublePrecision) => Ok(value),
        (SqlValue::Date(_), DataType::Date) => Ok(value),
        (SqlValue::Time(_), DataType::Time { .. }) => Ok(value),
        (SqlValue::Timestamp(_), DataType::Timestamp { .. }) => Ok(value),
        (SqlValue::Interval(_), DataType::Interval { .. }) => Ok(value),

        // VARCHAR/CHARACTER → DATE/TIME/TIMESTAMP conversions (implicit casting)
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Date) => {
            s.parse::<vibesql_types::Date>().map(SqlValue::Date).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!("Cannot parse '{}' as DATE: {}", s, e))
            })
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Time { .. }) => {
            s.parse::<vibesql_types::Time>().map(SqlValue::Time).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!("Cannot parse '{}' as TIME: {}", s, e))
            })
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Timestamp { .. }) => {
            s.parse::<vibesql_types::Timestamp>().map(SqlValue::Timestamp).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!(
                    "Cannot parse '{}' as TIMESTAMP: {}",
                    s, e
                ))
            })
        }
        (SqlValue::Smallint(_), DataType::Smallint) => Ok(value),
        (SqlValue::Bigint(_), DataType::Bigint) => Ok(value),
        // NUMERIC affinity: if the value is a whole number, store as integer
        (SqlValue::Numeric(f), DataType::Numeric { .. } | DataType::Decimal { .. }) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Integer(*f as i64))
            } else {
                Ok(value)
            }
        }

        // Numeric literal → Float/Real/Double
        (SqlValue::Numeric(f), DataType::Float { .. }) => Ok(SqlValue::Float(*f as f32)),
        (SqlValue::Numeric(f), DataType::Real) => Ok(SqlValue::Real(*f)), // Real is now f64
        (SqlValue::Numeric(f), DataType::DoublePrecision) => Ok(SqlValue::Double(*f)),

        // Float/Double → Real (and Real/Double → Float, Float/Real → Double):
        // cross-coercion between the IEEE float storage classes. Arithmetic
        // over mixed inputs can produce any of these (e.g. REAL + INTEGER
        // yields Float), and SQLite's REAL affinity accepts them all
        // (date2-604: `INSERT INTO t600(a) VALUES(julianday('now')+10)` into
        // a REAL column must reach CHECK evaluation, not fail coercion).
        (SqlValue::Float(f), DataType::Real) => Ok(SqlValue::Real(*f as f64)),
        (SqlValue::Double(f), DataType::Real) => Ok(SqlValue::Real(*f)),
        (SqlValue::Real(f), DataType::Float { .. }) => Ok(SqlValue::Float(*f as f32)),
        (SqlValue::Double(f), DataType::Float { .. }) => Ok(SqlValue::Float(*f as f32)),
        (SqlValue::Float(f), DataType::DoublePrecision) => Ok(SqlValue::Double(*f as f64)),
        (SqlValue::Real(f), DataType::DoublePrecision) => Ok(SqlValue::Double(*f)),

        // Numeric literal → Integer types
        // SQLite type affinity: try to convert to integer if possible,
        // otherwise keep as numeric (SQLite stores values with actual type, not column affinity)
        (SqlValue::Numeric(f), DataType::Integer) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Integer(*f as i64))
            } else {
                // SQLite affinity: non-integer values stay as REAL in INTEGER column
                Ok(SqlValue::Numeric(*f))
            }
        }
        (SqlValue::Numeric(f), DataType::Smallint) => {
            if f.fract() == 0.0 && *f >= i16::MIN as f64 && *f <= i16::MAX as f64 {
                Ok(SqlValue::Smallint(*f as i16))
            } else {
                // SQLite affinity: non-integer values stay as REAL in SMALLINT column
                Ok(SqlValue::Numeric(*f))
            }
        }
        (SqlValue::Numeric(f), DataType::Bigint) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Bigint(*f as i64))
            } else {
                // SQLite affinity: non-integer values stay as REAL in BIGINT column
                Ok(SqlValue::Numeric(*f))
            }
        }

        // VARCHAR/CHARACTER → Integer types (SQLite type affinity)
        // Try to convert text to integer, otherwise keep as text
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Integer) => {
            let trimmed = s.trim();
            if let Ok(i) = trimmed.parse::<i64>() {
                Ok(SqlValue::Integer(i))
            } else if let Ok(f) = trimmed.parse::<f64>() {
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(SqlValue::Integer(f as i64))
                } else {
                    // Non-integer float stored as REAL
                    Ok(SqlValue::Double(f))
                }
            } else {
                // Can't convert - keep as text (SQLite behavior)
                Ok(value)
            }
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Smallint) => {
            let trimmed = s.trim();
            if let Ok(i) = trimmed.parse::<i16>() {
                Ok(SqlValue::Smallint(i))
            } else if let Ok(f) = trimmed.parse::<f64>() {
                if f.fract() == 0.0 && f >= i16::MIN as f64 && f <= i16::MAX as f64 {
                    Ok(SqlValue::Smallint(f as i16))
                } else {
                    Ok(SqlValue::Double(f))
                }
            } else {
                Ok(value)
            }
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Bigint) => {
            let trimmed = s.trim();
            if let Ok(i) = trimmed.parse::<i64>() {
                Ok(SqlValue::Bigint(i))
            } else if let Ok(f) = trimmed.parse::<f64>() {
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(SqlValue::Bigint(f as i64))
                } else {
                    Ok(SqlValue::Double(f))
                }
            } else {
                Ok(value)
            }
        }

        // VARCHAR/CHARACTER → Float types (SQLite type affinity)
        // Try to convert text to float, otherwise keep as text
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Float { .. }) => {
            let trimmed = s.trim();
            if let Ok(f) = trimmed.parse::<f32>() {
                Ok(SqlValue::Float(f))
            } else {
                Ok(value)
            }
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Real) => {
            // Real is now f64 (SQLite REAL is 8-byte IEEE float)
            let trimmed = s.trim();
            if let Ok(f) = trimmed.parse::<f64>() {
                Ok(SqlValue::Real(f))
            } else {
                Ok(value)
            }
        }
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::DoublePrecision) => {
            let trimmed = s.trim();
            if let Ok(f) = trimmed.parse::<f64>() {
                Ok(SqlValue::Double(f))
            } else {
                Ok(value)
            }
        }

        // VARCHAR/CHARACTER → Numeric types (SQLite type affinity)
        (SqlValue::Varchar(s) | SqlValue::Character(s), DataType::Numeric { .. }) => {
            let trimmed = s.trim();
            if let Ok(i) = trimmed.parse::<i64>() {
                Ok(SqlValue::Integer(i))
            } else if let Ok(f) = trimmed.parse::<f64>() {
                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    Ok(SqlValue::Integer(f as i64))
                } else {
                    Ok(SqlValue::Double(f))
                }
            } else {
                Ok(value)
            }
        }

        // Integer → Float types (safe widening conversion)
        (SqlValue::Integer(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Integer(i), DataType::Real) => Ok(SqlValue::Real(*i as f64)), // Real is now f64
        (SqlValue::Integer(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),
        (SqlValue::Smallint(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Smallint(i), DataType::Real) => Ok(SqlValue::Real(*i as f64)), // Real is now f64
        (SqlValue::Smallint(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),
        (SqlValue::Bigint(i), DataType::Float { .. }) => Ok(SqlValue::Float(*i as f32)),
        (SqlValue::Bigint(i), DataType::Real) => Ok(SqlValue::Real(*i as f64)), // Real is now f64
        (SqlValue::Bigint(i), DataType::DoublePrecision) => Ok(SqlValue::Double(*i as f64)),

        // Integer → Numeric/Decimal (SQLite type affinity - integers can be stored in NUMERIC columns)
        (SqlValue::Integer(i), DataType::Numeric { .. }) => Ok(SqlValue::Integer(*i)),
        (SqlValue::Integer(i), DataType::Decimal { .. }) => Ok(SqlValue::Integer(*i)),
        (SqlValue::Smallint(i), DataType::Numeric { .. }) => Ok(SqlValue::Integer(*i as i64)),
        (SqlValue::Smallint(i), DataType::Decimal { .. }) => Ok(SqlValue::Integer(*i as i64)),
        (SqlValue::Bigint(i), DataType::Numeric { .. }) => Ok(SqlValue::Bigint(*i)),
        (SqlValue::Bigint(i), DataType::Decimal { .. }) => Ok(SqlValue::Bigint(*i)),

        // Float/Real/Double → Numeric/Decimal (SQLite type affinity)
        // SQLite NUMERIC affinity: if the value is a whole number, store as integer
        (SqlValue::Float(f), DataType::Numeric { .. } | DataType::Decimal { .. }) => {
            let f64_val = *f as f64;
            if f64_val.fract() == 0.0 && f64_val >= i64::MIN as f64 && f64_val <= i64::MAX as f64 {
                Ok(SqlValue::Integer(f64_val as i64))
            } else {
                Ok(SqlValue::Double(f64_val))
            }
        }
        (SqlValue::Real(f), DataType::Numeric { .. } | DataType::Decimal { .. }) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Integer(*f as i64))
            } else {
                Ok(SqlValue::Double(*f))
            }
        }
        (SqlValue::Double(f), DataType::Numeric { .. } | DataType::Decimal { .. }) => {
            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                Ok(SqlValue::Integer(*f as i64))
            } else {
                Ok(SqlValue::Double(*f))
            }
        }

        // Integer widening conversions
        (SqlValue::Smallint(i), DataType::Integer) => Ok(SqlValue::Integer(*i as i64)),
        (SqlValue::Smallint(i), DataType::Bigint) => Ok(SqlValue::Bigint(*i as i64)),
        (SqlValue::Integer(i), DataType::Bigint) => Ok(SqlValue::Bigint(*i)),

        // Varchar ↔ Character conversions
        (SqlValue::Varchar(s), DataType::Character { length }) => {
            let s = if s.len() > *length {
                s[..*length].to_string() // Truncate
            } else {
                format!("{:width$}", s, width = length) // Pad with spaces
            };
            Ok(SqlValue::Character(arcstr::ArcStr::from(s)))
        }
        (SqlValue::Character(s), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(s.trim_end()))) // Remove trailing spaces
        }

        // BinaryLargeObject (BLOB) accepts any value type
        // This implements SQLite's type affinity behavior where untyped columns
        // (which default to BLOB affinity) can store values of any type.
        // The value is stored as-is without conversion.
        (_, DataType::BinaryLargeObject) => Ok(value),

        // CharacterLargeObject (CLOB) has TEXT affinity in SQLite (the type name
        // contains "CLOB"). A CLOB column must accept any value by rendering its
        // text form rather than rejecting it as a datatype mismatch — this
        // unblocks the `end clob` column in table-7.2/8.1/8.6, which stores the
        // string `y'all`. Text passes through; numbers and booleans stringify;
        // blobs are stored as-is (SQLite keeps blobs in any column).
        (_, DataType::CharacterLargeObject) => match &value {
            SqlValue::Varchar(_) | SqlValue::Character(_) | SqlValue::Blob(_) => Ok(value),
            SqlValue::Boolean(b) => {
                Ok(SqlValue::Varchar(arcstr::ArcStr::from(if *b { "1" } else { "0" })))
            }
            // REAL -> TEXT keeps SQLite's %!.15g formatting (whole reals retain ".0")
            // via SqlValue's Display impl; integers/temporals stringify canonically.
            _ => Ok(SqlValue::Varchar(arcstr::ArcStr::from(value.to_string()))),
        },

        // UserDefined types: Apply SQLite's type affinity rules based on the type name
        // See https://www.sqlite.org/datatype3.html#determination_of_column_affinity
        //
        // Type affinity is determined by checking substrings in the type name:
        // 1. Contains "INT" → INTEGER affinity
        // 2. Contains "CHAR", "CLOB", "TEXT" → TEXT affinity
        // 3. Contains "BLOB" or no type specified → BLOB affinity (accept any)
        // 4. Contains "REAL", "FLOA", "DOUB" → REAL affinity
        // 5. Otherwise → NUMERIC affinity (try to convert text to number)
        (_, DataType::UserDefined { type_name }) => {
            let type_upper = type_name.to_uppercase();
            let affinity = if type_upper.contains("INT") {
                "INTEGER"
            } else if type_upper.contains("CHAR")
                || type_upper.contains("CLOB")
                || type_upper.contains("TEXT")
            {
                "TEXT"
            } else if type_upper.contains("BLOB") || type_upper.is_empty() {
                "BLOB"
            } else if type_upper.contains("REAL")
                || type_upper.contains("FLOA")
                || type_upper.contains("DOUB")
            {
                "REAL"
            } else {
                // Default: NUMERIC affinity (including NUM, NUMBER, etc.)
                "NUMERIC"
            };

            match affinity {
                "BLOB" => {
                    // BLOB affinity: accept any value as-is
                    Ok(value)
                }
                "TEXT" => {
                    // TEXT affinity: convert to text representation
                    match &value {
                        SqlValue::Varchar(_) | SqlValue::Character(_) => Ok(value),
                        SqlValue::Integer(i) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
                        }
                        SqlValue::Bigint(i) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
                        }
                        SqlValue::Smallint(i) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
                        }
                        // REAL -> TEXT affinity: use SQLite's %!.15g-style real->text
                        // formatting (via SqlValue's Display impl), which preserves the
                        // trailing ".0" for whole reals (e.g. -42.0 -> "-42.0", not "-42").
                        // f64::to_string() would drop it and break triggerC-4.1.4/4.1.5.
                        SqlValue::Numeric(_)
                        | SqlValue::Double(_)
                        | SqlValue::Real(_)
                        | SqlValue::Float(_) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(value.to_string())))
                        }
                        // Temporal values render to their canonical string form (matches
                        // SQLite storing CURRENT_TIME/DATE/TIMESTAMP defaults as TEXT).
                        SqlValue::Date(_)
                        | SqlValue::Time(_)
                        | SqlValue::Timestamp(_)
                        | SqlValue::Interval(_) => {
                            Ok(SqlValue::Varchar(arcstr::ArcStr::from(value.to_string())))
                        }
                        _ => Ok(value), // Other types pass through
                    }
                }
                "INTEGER" => {
                    // INTEGER affinity: try to convert to integer
                    match &value {
                        SqlValue::Integer(_)
                        | SqlValue::Bigint(_)
                        | SqlValue::Smallint(_)
                        | SqlValue::Unsigned(_) => Ok(value),
                        SqlValue::Numeric(f) => {
                            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                                Ok(SqlValue::Integer(*f as i64))
                            } else {
                                // Non-integer stays as-is (SQLite stores actual type)
                                Ok(value)
                            }
                        }
                        SqlValue::Double(f) => {
                            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                                Ok(SqlValue::Integer(*f as i64))
                            } else {
                                Ok(value)
                            }
                        }
                        SqlValue::Real(f) => {
                            let f64_val = *f as f64;
                            if f64_val.fract() == 0.0
                                && f64_val >= i64::MIN as f64
                                && f64_val <= i64::MAX as f64
                            {
                                Ok(SqlValue::Integer(f64_val as i64))
                            } else {
                                Ok(value)
                            }
                        }
                        SqlValue::Varchar(s) | SqlValue::Character(s) => {
                            // Try to parse as integer
                            if let Ok(i) = s.trim().parse::<i64>() {
                                Ok(SqlValue::Integer(i))
                            } else if let Ok(f) = s.trim().parse::<f64>() {
                                // Try as float, convert to int if whole number
                                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64
                                {
                                    Ok(SqlValue::Integer(f as i64))
                                } else {
                                    // Store as text if can't convert to integer
                                    Ok(value)
                                }
                            } else {
                                // Non-numeric text stays as text
                                Ok(value)
                            }
                        }
                        _ => Ok(value), // Other types pass through
                    }
                }
                "REAL" => {
                    // REAL affinity: convert to floating point
                    match &value {
                        SqlValue::Real(_) | SqlValue::Double(_) | SqlValue::Float(_) => Ok(value),
                        SqlValue::Integer(i) => Ok(SqlValue::Double(*i as f64)),
                        SqlValue::Bigint(i) => Ok(SqlValue::Double(*i as f64)),
                        SqlValue::Smallint(i) => Ok(SqlValue::Double(*i as f64)),
                        SqlValue::Numeric(f) => Ok(SqlValue::Double(*f)),
                        SqlValue::Varchar(s) | SqlValue::Character(s) => {
                            // Try to parse as float
                            if let Ok(f) = s.trim().parse::<f64>() {
                                Ok(SqlValue::Double(f))
                            } else {
                                // Non-numeric text stays as text
                                Ok(value)
                            }
                        }
                        _ => Ok(value),
                    }
                }
                "NUMERIC" | _ => {
                    // NUMERIC affinity: try integer first, then real, else keep as text
                    // This is SQLite's behavior for types like NUM, NUMBER, etc.
                    match &value {
                        // Integer-storage-class types pass through unchanged.
                        SqlValue::Integer(_)
                        | SqlValue::Bigint(_)
                        | SqlValue::Smallint(_)
                        | SqlValue::Unsigned(_) => Ok(value),
                        // Floating-point types: NUMERIC affinity losslessly converts a
                        // whole-number REAL to INTEGER storage class (e.g. 6.0 -> 6), same
                        // as SQLite. Without this, a column with an unrecognized declared
                        // type (e.g. `ANY`, which defaults to NUMERIC affinity per the
                        // SQLite algorithm) keeps 6.0 as REAL, so quote()/typeof() diverge
                        // from SQLite (window1.test 29.2, #6191).
                        SqlValue::Numeric(f) => {
                            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                                Ok(SqlValue::Integer(*f as i64))
                            } else {
                                Ok(value)
                            }
                        }
                        SqlValue::Double(f) => {
                            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                                Ok(SqlValue::Integer(*f as i64))
                            } else {
                                Ok(value)
                            }
                        }
                        SqlValue::Real(f) => {
                            let f64_val = *f as f64;
                            if f64_val.fract() == 0.0
                                && f64_val >= i64::MIN as f64
                                && f64_val <= i64::MAX as f64
                            {
                                Ok(SqlValue::Integer(f64_val as i64))
                            } else {
                                Ok(value)
                            }
                        }
                        SqlValue::Float(f) => {
                            let f64_val = *f as f64;
                            if f64_val.fract() == 0.0
                                && f64_val >= i64::MIN as f64
                                && f64_val <= i64::MAX as f64
                            {
                                Ok(SqlValue::Integer(f64_val as i64))
                            } else {
                                Ok(value)
                            }
                        }
                        // Text: try to convert to number
                        SqlValue::Varchar(s) | SqlValue::Character(s) => {
                            let trimmed = s.trim();
                            // Try integer first
                            if let Ok(i) = trimmed.parse::<i64>() {
                                return Ok(SqlValue::Integer(i));
                            }
                            // Try float next
                            if let Ok(f) = trimmed.parse::<f64>() {
                                // SQLite converts to integer if it's a whole number
                                if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64
                                {
                                    return Ok(SqlValue::Integer(f as i64));
                                }
                                return Ok(SqlValue::Double(f));
                            }
                            // Can't convert - keep as text (SQLite behavior)
                            Ok(value)
                        }
                        // Other types pass through
                        _ => Ok(value),
                    }
                }
            }
        }

        // SQLite type affinity: any value can be stored in TEXT columns by converting to string
        // This implements SQLite's behavior where TEXT affinity columns accept any value type
        (SqlValue::Integer(i), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
        }
        (SqlValue::Smallint(i), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
        }
        (SqlValue::Bigint(i), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(i.to_string())))
        }
        // REAL -> TEXT affinity: use SQLite's %!.15g-style real->text formatting
        // (via SqlValue's Display impl) so whole reals keep their trailing ".0"
        // (e.g. -42.0 -> "-42.0"). f64::to_string() would drop it.
        (
            SqlValue::Numeric(_) | SqlValue::Float(_) | SqlValue::Real(_) | SqlValue::Double(_),
            DataType::Varchar { .. },
        ) => Ok(SqlValue::Varchar(arcstr::ArcStr::from(value.to_string()))),
        (SqlValue::Boolean(b), DataType::Varchar { .. }) => {
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(if *b { "1" } else { "0" })))
        }
        // Temporal values → TEXT affinity: SQLite stores CURRENT_TIME / CURRENT_DATE /
        // CURRENT_TIMESTAMP (and any DATE/TIME/TIMESTAMP literal default) as TEXT, so a
        // TEXT-affinity column must accept a temporal value by rendering its canonical
        // string form rather than rejecting it as a datatype mismatch (table-13.2.*).
        (
            SqlValue::Date(_) | SqlValue::Time(_) | SqlValue::Timestamp(_) | SqlValue::Interval(_),
            DataType::Varchar { .. },
        ) => Ok(SqlValue::Varchar(arcstr::ArcStr::from(value.to_string()))),

        // Blob → Any column type (SQLite stores blobs as-is regardless of column affinity)
        // In SQLite, blobs can be stored in any column and retain their blob type
        (SqlValue::Blob(_), _) => Ok(value),

        // Type mismatch
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Type mismatch: expected {:?}, got {:?}",
            expected_type, value
        ))),
    }
}

#[cfg(test)]
mod text_affinity_tests {
    use vibesql_types::{DataType, SqlValue};

    use super::coerce_value;

    fn coerce_to_text(value: SqlValue) -> String {
        let coerced = coerce_value(value, &DataType::Varchar { max_length: None })
            .expect("coercion to TEXT affinity should succeed");
        match coerced {
            SqlValue::Varchar(s) => s.to_string(),
            other => panic!("expected Varchar, got {:?}", other),
        }
    }

    /// REAL -> TEXT affinity must use SQLite's %!.15g-style formatting, which
    /// PRESERVES the trailing ".0" for whole reals (triggerC-4.1.4/4.1.5).
    /// A naive f64::to_string() would render -42.0 as "-42" and break those tests.
    #[test]
    fn whole_real_keeps_trailing_dot_zero() {
        assert_eq!(coerce_to_text(SqlValue::Real(-42.0)), "-42.0");
        assert_eq!(coerce_to_text(SqlValue::Real(8.0)), "8.0");
        assert_eq!(coerce_to_text(SqlValue::Real(45.0)), "45.0");
        assert_eq!(coerce_to_text(SqlValue::Real(0.0)), "0.0");
        assert_eq!(coerce_to_text(SqlValue::Real(-0.0)), "0.0");
        assert_eq!(coerce_to_text(SqlValue::Real(1000000.0)), "1000000.0");
    }

    #[test]
    fn whole_real_keeps_dot_zero_for_all_float_variants() {
        assert_eq!(coerce_to_text(SqlValue::Numeric(-42.0)), "-42.0");
        assert_eq!(coerce_to_text(SqlValue::Double(-42.0)), "-42.0");
        assert_eq!(coerce_to_text(SqlValue::Float(-42.0)), "-42.0");
    }

    /// Fractional reals must NOT be altered (no naive ".0" stripping).
    #[test]
    fn fractional_real_unchanged() {
        assert_eq!(coerce_to_text(SqlValue::Real(1.5)), "1.5");
        assert_eq!(coerce_to_text(SqlValue::Real(-42.4)), "-42.4");
        assert_eq!(coerce_to_text(SqlValue::Real(0.1)), "0.1");
        assert_eq!(coerce_to_text(SqlValue::Real(-0.5)), "-0.5");
        assert_eq!(coerce_to_text(SqlValue::Real(2.5)), "2.5");
    }

    /// Integers keep their plain integer text (no ".0").
    #[test]
    fn integers_have_no_dot_zero() {
        assert_eq!(coerce_to_text(SqlValue::Integer(45)), "45");
        assert_eq!(coerce_to_text(SqlValue::Bigint(-42)), "-42");
        assert_eq!(coerce_to_text(SqlValue::Smallint(7)), "7");
    }
}

#[cfg(test)]
mod boolean_affinity_tests {
    use vibesql_types::{DataType, SqlValue};

    use super::coerce_value;

    fn coerce_to_bool_col(value: SqlValue) -> SqlValue {
        coerce_value(value, &DataType::Boolean)
            .expect("coercion into a BOOLEAN-affinity column should succeed")
    }

    /// SQLite has NUMERIC affinity for BOOLEAN-declared columns and stores
    /// integer 0/1 as integers. The integer must round-trip unchanged
    /// (table-7.2: `INSERT INTO weird(... [14_vac] boolean ...) VALUES(...,0,...)`).
    #[test]
    fn integer_inserts_round_trip() {
        assert_eq!(coerce_to_bool_col(SqlValue::Integer(0)), SqlValue::Integer(0));
        assert_eq!(coerce_to_bool_col(SqlValue::Integer(1)), SqlValue::Integer(1));
        assert_eq!(coerce_to_bool_col(SqlValue::Integer(42)), SqlValue::Integer(42));
        assert_eq!(coerce_to_bool_col(SqlValue::Bigint(7)), SqlValue::Bigint(7));
        assert_eq!(coerce_to_bool_col(SqlValue::Smallint(-3)), SqlValue::Smallint(-3));
    }

    /// Genuine boolean values are preserved (TRUE/FALSE literals, predicate results).
    #[test]
    fn boolean_values_preserved() {
        assert_eq!(coerce_to_bool_col(SqlValue::Boolean(true)), SqlValue::Boolean(true));
        assert_eq!(coerce_to_bool_col(SqlValue::Boolean(false)), SqlValue::Boolean(false));
    }

    /// NULL is accepted for any type (NOT NULL is enforced separately).
    #[test]
    fn null_passes_through() {
        assert_eq!(coerce_to_bool_col(SqlValue::Null), SqlValue::Null);
    }

    /// NUMERIC affinity collapses whole-valued reals to integers and keeps
    /// fractional reals as floating point.
    #[test]
    fn real_inputs_follow_numeric_affinity() {
        assert_eq!(coerce_to_bool_col(SqlValue::Real(1.0)), SqlValue::Integer(1));
        assert_eq!(coerce_to_bool_col(SqlValue::Double(0.0)), SqlValue::Integer(0));
        assert_eq!(coerce_to_bool_col(SqlValue::Numeric(2.0)), SqlValue::Integer(2));
        assert_eq!(coerce_to_bool_col(SqlValue::Real(1.5)), SqlValue::Real(1.5));
    }

    /// Text into a BOOLEAN column converts to a number when possible, else stays text.
    #[test]
    fn text_inputs_follow_numeric_affinity() {
        assert_eq!(
            coerce_to_bool_col(SqlValue::Varchar(arcstr::ArcStr::from("1"))),
            SqlValue::Integer(1)
        );
        assert_eq!(
            coerce_to_bool_col(SqlValue::Varchar(arcstr::ArcStr::from("0"))),
            SqlValue::Integer(0)
        );
        assert_eq!(
            coerce_to_bool_col(SqlValue::Varchar(arcstr::ArcStr::from("abc"))),
            SqlValue::Varchar(arcstr::ArcStr::from("abc"))
        );
    }
}

#[cfg(test)]
mod user_defined_numeric_affinity_tests {
    use vibesql_types::{DataType, SqlValue};

    use super::coerce_value;

    fn coerce_to_any_col(value: SqlValue) -> SqlValue {
        coerce_value(value, &DataType::UserDefined { type_name: "ANY".to_string() })
            .expect("coercion into an unrecognized/ANY-declared column should succeed")
    }

    /// A column declared with an unrecognized type name (e.g. `ANY`) gets
    /// NUMERIC affinity per SQLite's column-affinity algorithm (no INT/CHAR/
    /// CLOB/TEXT/BLOB/REAL/FLOA/DOUB substring match -> falls to rule 5,
    /// NUMERIC). NUMERIC affinity losslessly converts a whole-number REAL to
    /// INTEGER storage class on insert, matching `typeof()`/`quote()` on real
    /// SQLite (verified against sqlite3 3.51.0: `CREATE TABLE t1(d ANY);
    /// INSERT INTO t1 VALUES(6.0); SELECT typeof(d)` -> `integer`).
    /// Previously only text values went through this conversion; already-
    /// numeric REAL/DOUBLE/NUMERIC/FLOAT values passed through unchanged,
    /// which broke window1.test 29.2's `quote(d)`/RANGE-frame comparisons
    /// (#6191).
    #[test]
    fn whole_real_becomes_integer() {
        assert_eq!(coerce_to_any_col(SqlValue::Real(6.0)), SqlValue::Integer(6));
        assert_eq!(coerce_to_any_col(SqlValue::Double(9.0)), SqlValue::Integer(9));
        assert_eq!(coerce_to_any_col(SqlValue::Numeric(1.0)), SqlValue::Integer(1));
        assert_eq!(coerce_to_any_col(SqlValue::Float(-42.0)), SqlValue::Integer(-42));
    }

    /// Fractional reals are NOT coerced to integer and keep their original
    /// floating-point storage class.
    #[test]
    fn fractional_real_stays_real() {
        assert_eq!(coerce_to_any_col(SqlValue::Real(2.5)), SqlValue::Real(2.5));
        assert_eq!(coerce_to_any_col(SqlValue::Double(8.25)), SqlValue::Double(8.25));
        assert_eq!(coerce_to_any_col(SqlValue::Numeric(6.5)), SqlValue::Numeric(6.5));
    }

    /// Non-numeric text and other storage classes pass through unchanged.
    #[test]
    fn non_numeric_text_unchanged() {
        assert_eq!(
            coerce_to_any_col(SqlValue::Varchar(arcstr::ArcStr::from("xyz"))),
            SqlValue::Varchar(arcstr::ArcStr::from("xyz"))
        );
        assert_eq!(coerce_to_any_col(SqlValue::Integer(6)), SqlValue::Integer(6));
    }
}
