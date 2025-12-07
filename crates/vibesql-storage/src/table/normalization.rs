// ============================================================================
// Row Normalization
// ============================================================================

use vibesql_catalog::TableSchema;
use vibesql_types::{DataType, SqlValue};

use crate::{Row, StorageError};

/// Handles row value normalization and validation
pub struct RowNormalizer<'a> {
    schema: &'a TableSchema,
}

impl<'a> RowNormalizer<'a> {
    /// Create a new normalizer for the given schema
    pub fn new(schema: &'a TableSchema) -> Self {
        Self { schema }
    }

    /// Normalize and validate a row according to the schema
    ///
    /// Performs the following operations:
    /// 1. Type checking - verify each value matches column type
    /// 2. NULL checking - verify non-nullable columns have values
    /// 3. Value normalization - CHAR padding/truncation, etc.
    ///
    /// # Returns
    /// * `Ok(Row)` - Normalized row
    /// * `Err(StorageError)` - Type mismatch, NULL violation, or other validation error
    pub fn normalize_and_validate(&self, mut row: Row) -> Result<Row, StorageError> {
        // Validate column count
        if row.len() != self.schema.column_count() {
            return Err(StorageError::ColumnCountMismatch {
                expected: self.schema.column_count(),
                actual: row.len(),
            });
        }

        // Single pass: type check, NULL check, and normalize values
        for (i, column) in self.schema.columns.iter().enumerate() {
            if let Some(value) = row.values.get_mut(i) {
                // NULL checking - verify non-nullable columns have values
                if !column.nullable && *value == SqlValue::Null {
                    return Err(StorageError::NullConstraintViolation {
                        column: column.name.clone(),
                    });
                }

                // Type checking and normalization (skip NULL values)
                if *value != SqlValue::Null {
                    self.validate_and_normalize_value(value, &column.data_type, &column.name)?;
                }
            }
        }

        Ok(row)
    }

    /// Validate that a value matches the expected type and normalize it
    fn validate_and_normalize_value(
        &self,
        value: &mut SqlValue,
        expected_type: &DataType,
        column_name: &str,
    ) -> Result<(), StorageError> {
        match expected_type {
            // Exact numeric types
            DataType::Integer => {
                if !matches!(value, SqlValue::Integer(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "INTEGER".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            DataType::Smallint => {
                if !matches!(value, SqlValue::Smallint(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "SMALLINT".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            DataType::Bigint => {
                if !matches!(value, SqlValue::Bigint(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "BIGINT".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            DataType::Unsigned => {
                if !matches!(value, SqlValue::Unsigned(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "UNSIGNED".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            DataType::Numeric { .. } | DataType::Decimal { .. } => {
                if !matches!(value, SqlValue::Numeric(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "NUMERIC".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            // Approximate numeric types
            DataType::Float { .. } => {
                if !matches!(value, SqlValue::Float(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "FLOAT".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            DataType::Real => {
                if !matches!(value, SqlValue::Real(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "REAL".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            DataType::DoublePrecision => {
                if !matches!(value, SqlValue::Double(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "DOUBLE PRECISION".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            // Character types
            DataType::Character { length } => {
                if let SqlValue::Character(s) = value {
                    *s = Self::normalize_char_value(s, *length).into();
                } else {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: format!("CHAR({})", length),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            DataType::Varchar { max_length } => {
                if let SqlValue::Varchar(s) = value {
                    // Truncate if exceeds max_length
                    if let Some(max_len) = max_length {
                        if s.len() > *max_len {
                            *s = s[..*max_len].into();
                        }
                    }
                } else {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: format!(
                            "VARCHAR({})",
                            max_length.map(|l| l.to_string()).unwrap_or_else(|| "MAX".to_string())
                        ),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            DataType::Name => {
                // NAME is VARCHAR(128) in SQL:1999
                if let SqlValue::Varchar(s) = value {
                    // Truncate to 128 if exceeds
                    if s.len() > 128 {
                        *s = s[..128].into();
                    }
                } else {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "NAME".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            DataType::CharacterLargeObject => {
                // CLOB stored as Varchar
                if !matches!(value, SqlValue::Varchar(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "CLOB".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            // Boolean
            DataType::Boolean => {
                if !matches!(value, SqlValue::Boolean(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "BOOLEAN".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            // Date/Time types
            DataType::Date => {
                // Allow implicit conversion from VARCHAR to DATE
                match value {
                    SqlValue::Date(_) => {
                        // Already correct type
                    }
                    SqlValue::Varchar(s) | SqlValue::Character(s) => {
                        // Try to parse VARCHAR as DATE
                        match s.parse::<vibesql_types::Date>() {
                            Ok(date) => {
                                *value = SqlValue::Date(date);
                            }
                            Err(_) => {
                                return Err(StorageError::TypeMismatch {
                                    column: column_name.to_string(),
                                    expected: format!("DATE (cannot parse '{}')", s),
                                    actual: value.type_name().to_string(),
                                });
                            }
                        }
                    }
                    _ => {
                        return Err(StorageError::TypeMismatch {
                            column: column_name.to_string(),
                            expected: "DATE".to_string(),
                            actual: value.type_name().to_string(),
                        });
                    }
                }
            }
            DataType::Time { .. } => {
                // Allow implicit conversion from VARCHAR to TIME
                match value {
                    SqlValue::Time(_) => {
                        // Already correct type
                    }
                    SqlValue::Varchar(s) | SqlValue::Character(s) => {
                        // Try to parse VARCHAR as TIME
                        match s.parse::<vibesql_types::Time>() {
                            Ok(time) => {
                                *value = SqlValue::Time(time);
                            }
                            Err(_) => {
                                return Err(StorageError::TypeMismatch {
                                    column: column_name.to_string(),
                                    expected: format!("TIME (cannot parse '{}')", s),
                                    actual: value.type_name().to_string(),
                                });
                            }
                        }
                    }
                    _ => {
                        return Err(StorageError::TypeMismatch {
                            column: column_name.to_string(),
                            expected: "TIME".to_string(),
                            actual: value.type_name().to_string(),
                        });
                    }
                }
            }
            DataType::Timestamp { .. } => {
                // Allow implicit conversion from VARCHAR to TIMESTAMP
                match value {
                    SqlValue::Timestamp(_) => {
                        // Already correct type
                    }
                    SqlValue::Varchar(s) | SqlValue::Character(s) => {
                        // Try to parse VARCHAR as TIMESTAMP
                        match s.parse::<vibesql_types::Timestamp>() {
                            Ok(ts) => {
                                *value = SqlValue::Timestamp(ts);
                            }
                            Err(_) => {
                                return Err(StorageError::TypeMismatch {
                                    column: column_name.to_string(),
                                    expected: format!("TIMESTAMP (cannot parse '{}')", s),
                                    actual: value.type_name().to_string(),
                                });
                            }
                        }
                    }
                    _ => {
                        return Err(StorageError::TypeMismatch {
                            column: column_name.to_string(),
                            expected: "TIMESTAMP".to_string(),
                            actual: value.type_name().to_string(),
                        });
                    }
                }
            }
            // Interval type
            DataType::Interval { .. } => {
                if !matches!(value, SqlValue::Interval(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "INTERVAL".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            // Binary types - Note: No SqlValue::Blob variant exists yet
            DataType::BinaryLargeObject => {
                // For now, accept Varchar as a placeholder until BLOB is implemented
                if !matches!(value, SqlValue::Varchar(_)) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "BLOB".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            DataType::Bit { .. } => {
                // BIT type: For now, accept Varchar or Integer as placeholder until proper BIT type is fully implemented
                // VARCHAR can hold binary literals like b'1010', INTEGER can hold numeric values
                if !matches!(
                    value,
                    SqlValue::Varchar(_)
                        | SqlValue::Integer(_)
                        | SqlValue::Bigint(_)
                        | SqlValue::Unsigned(_)
                ) {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: "BIT".to_string(),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            // User-defined types
            #[cfg_attr(not(debug_assertions), allow(unused_variables))]
            DataType::UserDefined { type_name } => {
                // Cannot validate user-defined types without more schema information
                // Accept any value for now
                //
                // TODO: Implement proper user-defined type validation
                //
                // Requirements for implementation:
                // 1. Type catalog to store user-defined type definitions (CREATE TYPE)
                // 2. Type definition metadata (base type, constraints, domain values, etc.)
                // 3. Validation logic for different UDT categories:
                //    - DISTINCT types (validate against base type)
                //    - ENUM types (validate value is in allowed set)
                //    - COMPOSITE types (validate structure and field types)
                //    - DOMAIN types (validate base type and CHECK constraints)
                // 4. Integration with schema catalog to resolve type names
                //
                // For now, we accept any value for user-defined types.
                // Validation will occur at runtime if the type is used in expressions.
                //
                // See SQL:1999 Part 2 (Foundation) Section 4.8 for UDT specification.

                // Log a debug message for development
                #[cfg(debug_assertions)]
                {
                    log::warn!(
                        "Skipping validation for user-defined type '{}' in column '{}'",
                        type_name,
                        column_name
                    );
                }
            }
            // Vector type
            DataType::Vector { dimensions } => {
                if let SqlValue::Vector(v) = value {
                    if v.len() != *dimensions as usize {
                        return Err(StorageError::TypeMismatch {
                            column: column_name.to_string(),
                            expected: format!("VECTOR({})", dimensions),
                            actual: format!("VECTOR({})", v.len()),
                        });
                    }
                } else {
                    return Err(StorageError::TypeMismatch {
                        column: column_name.to_string(),
                        expected: format!("VECTOR({})", dimensions),
                        actual: value.type_name().to_string(),
                    });
                }
            }
            // NULL type
            DataType::Null => {
                // NULL type always accepts NULL values (already checked above)
            }
        }

        Ok(())
    }

    /// Normalize a CHAR value to fixed length
    /// - Pad with spaces if too short
    /// - Truncate if too long
    fn normalize_char_value(value: &str, length: usize) -> String {
        use std::cmp::Ordering;
        match value.len().cmp(&length) {
            Ordering::Less => {
                // Pad with spaces to the right
                format!("{:width$}", value, width = length)
            }
            Ordering::Greater => {
                // Truncate to fixed length
                value[..length].to_string()
            }
            Ordering::Equal => {
                // Exact length - no change needed
                value.to_string()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::ColumnSchema;

    use super::*;

    fn create_test_schema() -> TableSchema {
        let columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("code".to_string(), DataType::Character { length: 5 }, true),
        ];
        TableSchema::with_primary_key("test_table".to_string(), columns, vec!["id".to_string()])
    }

    #[test]
    fn test_normalize_char_padding() {
        let schema = create_test_schema();
        let normalizer = RowNormalizer::new(&schema);

        let row = Row::from_vec(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Character(arcstr::ArcStr::from("ABC")), // Should be padded to 5
            ]);

        let normalized = normalizer.normalize_and_validate(row).unwrap();
        assert_eq!(normalized.values[2], SqlValue::Character(arcstr::ArcStr::from("ABC  ")));
    }

    #[test]
    fn test_normalize_char_truncation() {
        let schema = create_test_schema();
        let normalizer = RowNormalizer::new(&schema);

        let row = Row::from_vec(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Character(arcstr::ArcStr::from("ABCDEFGH")), // Should be truncated to 5
            ]);

        let normalized = normalizer.normalize_and_validate(row).unwrap();
        assert_eq!(normalized.values[2], SqlValue::Character(arcstr::ArcStr::from("ABCDE")));
    }

    #[test]
    fn test_null_constraint_violation() {
        let schema = create_test_schema();
        let normalizer = RowNormalizer::new(&schema);

        let row = Row::from_vec(vec![
                SqlValue::Null, // id is NOT NULL
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Character(arcstr::ArcStr::from("ABC")),
            ]);

        let result = normalizer.normalize_and_validate(row);
        assert!(result.is_err());
        assert!(matches!(result, Err(StorageError::NullConstraintViolation { .. })));
    }

    #[test]
    fn test_type_mismatch() {
        let schema = create_test_schema();
        let normalizer = RowNormalizer::new(&schema);

        let row = Row::from_vec(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("not_an_int")), // Wrong type for id
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Character(arcstr::ArcStr::from("ABC")),
            ]);

        let result = normalizer.normalize_and_validate(row);
        assert!(result.is_err());
        assert!(matches!(result, Err(StorageError::TypeMismatch { .. })));
    }

    #[test]
    fn test_varchar_truncation() {
        let schema = create_test_schema();
        let normalizer = RowNormalizer::new(&schema);

        let long_name = "A".repeat(100); // Exceeds max_length of 50
        let row = Row::from_vec(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from(long_name.clone())),
                SqlValue::Character(arcstr::ArcStr::from("ABC")),
            ]);

        let normalized = normalizer.normalize_and_validate(row).unwrap();
        if let SqlValue::Varchar(name) = &normalized.values[1] {
            assert_eq!(name.len(), 50); // Truncated to max_length
        } else {
            panic!("Expected VARCHAR value");
        }
    }

    #[test]
    fn test_nullable_column_accepts_null() {
        let schema = create_test_schema();
        let normalizer = RowNormalizer::new(&schema);

        let row = Row::from_vec(vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
                SqlValue::Null, // code is nullable
            ]);

        let result = normalizer.normalize_and_validate(row);
        assert!(result.is_ok());
    }
}
