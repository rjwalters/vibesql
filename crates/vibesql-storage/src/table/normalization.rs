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
            // SQLite type affinity: INTEGER columns can store any value type.
            // The column type is a preference, not a constraint. Values that couldn't
            // be converted to the column type are stored in their original form.
            DataType::Integer => {
                // Accept any value type (SQLite type affinity)
                // INTEGER, NUMERIC, DOUBLE, REAL, FLOAT are numeric compatibles
                // VARCHAR, CHARACTER are accepted as-is (couldn't convert in executor)
            }
            DataType::Smallint => {
                // Accept any value type (SQLite type affinity)
            }
            DataType::Bigint => {
                // Accept any value type (SQLite type affinity)
            }
            DataType::Unsigned => {
                // Accept any value type (SQLite type affinity)
            }
            DataType::Numeric { .. } | DataType::Decimal { .. } => {
                // Accept any value type (SQLite type affinity)
                // Values that couldn't be converted stay as text
            }
            // Approximate numeric types - all accept any value (SQLite type affinity)
            DataType::Float { .. } => {
                // FLOAT affinity: accept any value type
            }
            DataType::Real => {
                // REAL affinity: Accept any value type, convert numeric types to Real
                // Real is now f64 (SQLite REAL is 8-byte IEEE float)
                match value {
                    SqlValue::Real(_) => {
                        // Already correct type
                    }
                    SqlValue::Integer(i) => {
                        *value = SqlValue::Real(*i as f64);
                    }
                    SqlValue::Bigint(i) => {
                        *value = SqlValue::Real(*i as f64);
                    }
                    SqlValue::Smallint(i) => {
                        *value = SqlValue::Real(*i as f64);
                    }
                    SqlValue::Double(d) => {
                        *value = SqlValue::Real(*d);
                    }
                    SqlValue::Float(f) => {
                        *value = SqlValue::Real(*f as f64);
                    }
                    SqlValue::Numeric(n) => {
                        *value = SqlValue::Real(*n);
                    }
                    SqlValue::Unsigned(u) => {
                        *value = SqlValue::Real(*u as f64);
                    }
                    _ => {
                        // SQLite affinity: keep non-numeric values as-is
                    }
                }
            }
            DataType::DoublePrecision => {
                // DOUBLE PRECISION affinity: accept any value type
            }
            // Character types - SQLite type affinity allows any value
            DataType::Character { length } => {
                if let SqlValue::Character(s) = value {
                    *s = Self::normalize_char_value(s, *length).into();
                }
                // SQLite affinity: accept any value type
            }
            DataType::Varchar { max_length } => {
                if let SqlValue::Varchar(s) = value {
                    // Truncate if exceeds max_length
                    if let Some(max_len) = max_length {
                        if s.len() > *max_len {
                            *s = s[..*max_len].into();
                        }
                    }
                }
                // SQLite affinity: accept any value type
            }
            DataType::Name => {
                // NAME is VARCHAR(128) in SQL:1999
                if let SqlValue::Varchar(s) = value {
                    // Truncate to 128 if exceeds
                    if s.len() > 128 {
                        *s = s[..128].into();
                    }
                }
                // SQLite affinity: accept any value type
            }
            DataType::CharacterLargeObject => {
                // CLOB: accept any value type (SQLite affinity)
            }
            // Boolean - accept any value (SQLite affinity)
            DataType::Boolean => {
                // SQLite affinity: accept any value type
            }
            // Date/Time types - SQLite affinity: accept any value
            DataType::Date => {
                // Try implicit conversion from VARCHAR to DATE
                match value {
                    SqlValue::Date(_) => {
                        // Already correct type
                    }
                    SqlValue::Varchar(s) | SqlValue::Character(s) => {
                        // Try to parse VARCHAR as DATE, keep as-is if fails
                        if let Ok(date) = s.parse::<vibesql_types::Date>() {
                            *value = SqlValue::Date(date);
                        }
                        // SQLite affinity: keep non-parseable values as text
                    }
                    _ => {
                        // SQLite affinity: accept any value type
                    }
                }
            }
            DataType::Time { .. } => {
                // Try implicit conversion from VARCHAR to TIME
                match value {
                    SqlValue::Time(_) => {
                        // Already correct type
                    }
                    SqlValue::Varchar(s) | SqlValue::Character(s) => {
                        // Try to parse VARCHAR as TIME, keep as-is if fails
                        if let Ok(time) = s.parse::<vibesql_types::Time>() {
                            *value = SqlValue::Time(time);
                        }
                        // SQLite affinity: keep non-parseable values as text
                    }
                    _ => {
                        // SQLite affinity: accept any value type
                    }
                }
            }
            DataType::Timestamp { .. } => {
                // Try implicit conversion from VARCHAR to TIMESTAMP
                match value {
                    SqlValue::Timestamp(_) => {
                        // Already correct type
                    }
                    SqlValue::Varchar(s) | SqlValue::Character(s) => {
                        // Try to parse VARCHAR as TIMESTAMP, keep as-is if fails
                        if let Ok(ts) = s.parse::<vibesql_types::Timestamp>() {
                            *value = SqlValue::Timestamp(ts);
                        }
                        // SQLite affinity: keep non-parseable values as text
                    }
                    _ => {
                        // SQLite affinity: accept any value type
                    }
                }
            }
            // Interval type - SQLite affinity: accept any value
            DataType::Interval { .. } => {
                // SQLite affinity: accept any value type
            }
            // Binary types - implements SQLite's BLOB affinity
            DataType::BinaryLargeObject => {
                // BLOB affinity accepts any value type.
                // This implements SQLite's flexible typing where untyped columns
                // (which default to BLOB affinity) can store values of any type.
                // The value is stored as-is without conversion.
            }
            DataType::Bit { .. } => {
                // BIT type: accept any value (SQLite affinity)
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
            // Vector type - check dimensions if it's a vector, otherwise accept any value
            DataType::Vector { dimensions } => {
                if let SqlValue::Vector(v) = value {
                    if v.len() != *dimensions as usize {
                        return Err(StorageError::TypeMismatch {
                            column: column_name.to_string(),
                            expected: format!("VECTOR({})", dimensions),
                            actual: format!("VECTOR({})", v.len()),
                        });
                    }
                }
                // SQLite affinity: accept any value type
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
    fn test_sqlite_type_affinity() {
        // SQLite type affinity: any value can be stored in any column
        // This implements SQLite's flexible typing where column types are
        // preferences, not constraints. A Varchar value can be stored
        // in an Integer column (it stays as Varchar, not converted).
        let schema = create_test_schema();
        let normalizer = RowNormalizer::new(&schema);

        let row = Row::from_vec(vec![
            SqlValue::Varchar(arcstr::ArcStr::from("not_an_int")), // Varchar in Integer column
            SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            SqlValue::Character(arcstr::ArcStr::from("ABC")),
        ]);

        // SQLite affinity: this should succeed, storing the value as-is
        let result = normalizer.normalize_and_validate(row);
        assert!(result.is_ok(), "SQLite type affinity should accept any value type");

        // Verify the value was stored as-is (Varchar, not converted)
        let normalized = result.unwrap();
        assert!(matches!(normalized.values[0], SqlValue::Varchar(_)));
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
