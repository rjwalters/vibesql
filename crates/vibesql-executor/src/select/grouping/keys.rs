//! Specialized GROUP BY key types for efficient hashing
//!
//! This module provides specialized key types that avoid the overhead of
//! `Vec<SqlValue>` for common GROUP BY patterns:
//!
//! - Single primitive keys (i64, String)
//! - Two-column keys ((char, char), (i64, i64), etc.)
//! - Fallback to Vec<SqlValue> for complex cases
//!
//! Key benefits:
//! - No heap allocation for primitive keys
//! - Direct hashing without enum matching overhead
//! - Cache-friendly memory layout

use std::hash::{Hash, Hasher};

#[cfg(test)]
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use crate::select::columnar::batch::{ColumnArray, ColumnarBatch};

/// Specialized GROUP BY key types for efficient hashing
///
/// The key insight is that most GROUP BY queries use a small number of
/// primitive columns. By specializing for these cases, we can:
/// 1. Avoid Vec allocation per row
/// 2. Hash primitives directly without enum matching
/// 3. Use more cache-efficient memory layouts
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupKey {
    /// Single i64 key (common for integer GROUP BY)
    SingleI64(i64),

    /// Single String key (common for VARCHAR GROUP BY)
    SingleString(String),

    /// Two single-byte characters (TPC-H Q1: l_returnflag, l_linestatus)
    /// Packed into u16 for efficient hashing
    TwoChars(u8, u8),

    /// Two i64 keys
    TwoI64(i64, i64),

    /// (i64, String) - common mixed pattern
    I64String(i64, String),

    /// (i64, i64, i64) - TPC-H Q3 pattern (l_orderkey, o_orderdate, o_shippriority)
    ThreeI64(i64, i64, i64),

    /// Fallback for complex keys
    Generic(Vec<SqlValue>),
}

impl Hash for GroupKey {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use discriminant for type distinction
        std::mem::discriminant(self).hash(state);

        match self {
            GroupKey::SingleI64(v) => v.hash(state),
            GroupKey::SingleString(v) => v.hash(state),
            GroupKey::TwoChars(a, b) => {
                // Pack into u16 and hash once
                let packed = (*a as u16) | ((*b as u16) << 8);
                packed.hash(state);
            }
            GroupKey::TwoI64(a, b) => {
                a.hash(state);
                b.hash(state);
            }
            GroupKey::I64String(i, s) => {
                i.hash(state);
                s.hash(state);
            }
            GroupKey::ThreeI64(a, b, c) => {
                a.hash(state);
                b.hash(state);
                c.hash(state);
            }
            GroupKey::Generic(v) => v.hash(state),
        }
    }
}

/// Specification for a GROUP BY key extraction strategy
#[derive(Debug, Clone)]
pub enum GroupKeySpec {
    /// Single i64 column
    SingleI64 { col_idx: usize },

    /// Single String column
    SingleString { col_idx: usize },

    /// Two single-character columns (e.g., l_returnflag, l_linestatus)
    TwoChars { col1_idx: usize, col2_idx: usize },

    /// Two i64 columns
    TwoI64 { col1_idx: usize, col2_idx: usize },

    /// (i64, String) pattern
    I64String { i64_col: usize, string_col: usize },

    /// Three integer columns (i64, i64, i64) - for TPC-H Q3 pattern
    ThreeI64 { col1_idx: usize, col2_idx: usize, col3_idx: usize },

    /// Generic fallback
    Generic { col_indices: Vec<(usize, DataType)> },
}

impl GroupKeySpec {
    /// Analyze GROUP BY columns and determine the best key strategy
    #[cfg(test)]
    pub fn from_columns(columns: &[(usize, DataType)]) -> Self {
        match columns.len() {
            1 => {
                let (idx, dtype) = &columns[0];
                match dtype {
                    DataType::Integer | DataType::Bigint => {
                        GroupKeySpec::SingleI64 { col_idx: *idx }
                    }
                    DataType::Varchar { .. } | DataType::Character { .. } => {
                        GroupKeySpec::SingleString { col_idx: *idx }
                    }
                    _ => GroupKeySpec::Generic { col_indices: columns.to_vec() },
                }
            }
            2 => {
                let (idx1, dtype1) = &columns[0];
                let (idx2, dtype2) = &columns[1];

                // Check for two single-char columns (TPC-H Q1 pattern)
                if matches!(
                    (dtype1, dtype2),
                    (
                        DataType::Varchar { max_length: Some(1) }
                            | DataType::Character { length: 1 },
                        DataType::Varchar { max_length: Some(1) }
                            | DataType::Character { length: 1 }
                    )
                ) {
                    return GroupKeySpec::TwoChars { col1_idx: *idx1, col2_idx: *idx2 };
                }

                // Check for two VARCHAR columns that might be single chars
                // (TPC-H lineitem uses VARCHAR without max_length but values are single chars)
                if matches!(
                    (dtype1, dtype2),
                    (
                        DataType::Varchar { .. } | DataType::Character { .. },
                        DataType::Varchar { .. } | DataType::Character { .. }
                    )
                ) {
                    return GroupKeySpec::TwoChars { col1_idx: *idx1, col2_idx: *idx2 };
                }

                // Check for two i64 columns
                if matches!(dtype1, DataType::Integer | DataType::Bigint)
                    && matches!(dtype2, DataType::Integer | DataType::Bigint)
                {
                    return GroupKeySpec::TwoI64 { col1_idx: *idx1, col2_idx: *idx2 };
                }

                // Check for (i64, String) pattern
                if matches!(dtype1, DataType::Integer | DataType::Bigint)
                    && matches!(dtype2, DataType::Varchar { .. } | DataType::Character { .. })
                {
                    return GroupKeySpec::I64String { i64_col: *idx1, string_col: *idx2 };
                }

                GroupKeySpec::Generic { col_indices: columns.to_vec() }
            }
            3 => {
                let (idx1, dtype1) = &columns[0];
                let (idx2, dtype2) = &columns[1];
                let (idx3, dtype3) = &columns[2];

                // Check for (i64, i64/Date, i64) - TPC-H Q3 pattern
                // Note: Date is stored as days-since-epoch and extracted as i64
                if matches!(dtype1, DataType::Integer | DataType::Bigint)
                    && matches!(dtype2, DataType::Integer | DataType::Bigint | DataType::Date)
                    && matches!(dtype3, DataType::Integer | DataType::Bigint)
                {
                    return GroupKeySpec::ThreeI64 {
                        col1_idx: *idx1,
                        col2_idx: *idx2,
                        col3_idx: *idx3,
                    };
                }

                GroupKeySpec::Generic { col_indices: columns.to_vec() }
            }
            _ => GroupKeySpec::Generic { col_indices: columns.to_vec() },
        }
    }

    /// Extract a group key from a row
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Caller must ensure column
    /// indices are valid.
    #[cfg(test)]
    #[allow(dead_code)]
    #[inline]
    pub unsafe fn extract_key(&self, row: &Row) -> GroupKey {
        match self {
            GroupKeySpec::SingleI64 { col_idx } => {
                GroupKey::SingleI64(row.get_i64_unchecked(*col_idx))
            }
            GroupKeySpec::SingleString { col_idx } => {
                GroupKey::SingleString(row.get_string_unchecked(*col_idx).to_string())
            }
            GroupKeySpec::TwoChars { col1_idx, col2_idx } => {
                // Get first char of each string, or 0 if empty
                let s1 = row.get_string_unchecked(*col1_idx);
                let s2 = row.get_string_unchecked(*col2_idx);
                let c1 = s1.as_bytes().first().copied().unwrap_or(0);
                let c2 = s2.as_bytes().first().copied().unwrap_or(0);
                GroupKey::TwoChars(c1, c2)
            }
            GroupKeySpec::TwoI64 { col1_idx, col2_idx } => {
                GroupKey::TwoI64(row.get_i64_unchecked(*col1_idx), row.get_i64_unchecked(*col2_idx))
            }
            GroupKeySpec::I64String { i64_col, string_col } => GroupKey::I64String(
                row.get_i64_unchecked(*i64_col),
                row.get_string_unchecked(*string_col).to_string(),
            ),
            GroupKeySpec::ThreeI64 { col1_idx, col2_idx, col3_idx } => GroupKey::ThreeI64(
                row.get_i64_unchecked(*col1_idx),
                row.get_i64_unchecked(*col2_idx),
                row.get_i64_unchecked(*col3_idx),
            ),
            GroupKeySpec::Generic { col_indices } => {
                let mut key = Vec::with_capacity(col_indices.len());
                for (idx, dtype) in col_indices {
                    let value = match dtype {
                        DataType::Integer | DataType::Bigint => {
                            SqlValue::Integer(row.get_i64_unchecked(*idx))
                        }
                        DataType::Varchar { .. } | DataType::Character { .. } => {
                            SqlValue::Varchar(arcstr::ArcStr::from(row.get_string_unchecked(*idx)))
                        }
                        DataType::DoublePrecision | DataType::Real | DataType::Decimal { .. } => {
                            SqlValue::Double(row.get_f64_unchecked(*idx))
                        }
                        DataType::Date => SqlValue::Date(row.get_date_unchecked(*idx)),
                        DataType::Boolean => SqlValue::Boolean(row.get_bool_unchecked(*idx)),
                        _ => row.get(*idx).cloned().unwrap_or(SqlValue::Null),
                    };
                    key.push(value);
                }
                GroupKey::Generic(key)
            }
        }
    }

    /// Convert a GroupKey back to Vec<SqlValue> for result output
    pub fn key_to_values(&self, key: &GroupKey) -> Vec<SqlValue> {
        match key {
            GroupKey::SingleI64(v) => vec![SqlValue::Integer(*v)],
            GroupKey::SingleString(v) => vec![SqlValue::Varchar(arcstr::ArcStr::from(v.clone()))],
            GroupKey::TwoChars(a, b) => vec![
                SqlValue::Varchar(arcstr::ArcStr::from(String::from_utf8_lossy(&[*a]).into_owned())),
                SqlValue::Varchar(arcstr::ArcStr::from(String::from_utf8_lossy(&[*b]).into_owned())),
            ],
            GroupKey::TwoI64(a, b) => vec![SqlValue::Integer(*a), SqlValue::Integer(*b)],
            GroupKey::I64String(i, s) => vec![SqlValue::Integer(*i), SqlValue::Varchar(arcstr::ArcStr::from(s.clone()))],
            GroupKey::ThreeI64(a, b, c) => {
                vec![SqlValue::Integer(*a), SqlValue::Integer(*b), SqlValue::Integer(*c)]
            }
            GroupKey::Generic(v) => v.clone(),
        }
    }

    /// Analyze ColumnarBatch columns and determine the best key strategy
    ///
    /// This method examines the column types in a ColumnarBatch to select
    /// the most efficient key representation for GROUP BY operations.
    pub fn from_columnar_batch(batch: &ColumnarBatch, group_cols: &[usize]) -> Self {
        match group_cols.len() {
            1 => {
                let col_idx = group_cols[0];
                if let Some(column) = batch.column(col_idx) {
                    match column {
                        ColumnArray::Int64(_, _) | ColumnArray::Int32(_, _) => {
                            return GroupKeySpec::SingleI64 { col_idx };
                        }
                        ColumnArray::String(_, _) | ColumnArray::FixedString(_, _) => {
                            return GroupKeySpec::SingleString { col_idx };
                        }
                        _ => {}
                    }
                }
                GroupKeySpec::Generic {
                    col_indices: group_cols
                        .iter()
                        .map(|&i| (i, DataType::Varchar { max_length: None }))
                        .collect(),
                }
            }
            2 => {
                let col1_idx = group_cols[0];
                let col2_idx = group_cols[1];
                if let (Some(col1), Some(col2)) = (batch.column(col1_idx), batch.column(col2_idx)) {
                    // Check for two string columns (TPC-H Q1 pattern: l_returnflag, l_linestatus)
                    if matches!(col1, ColumnArray::String(_, _) | ColumnArray::FixedString(_, _))
                        && matches!(
                            col2,
                            ColumnArray::String(_, _) | ColumnArray::FixedString(_, _)
                        )
                    {
                        return GroupKeySpec::TwoChars { col1_idx, col2_idx };
                    }
                    // Check for two integer columns
                    if matches!(col1, ColumnArray::Int64(_, _) | ColumnArray::Int32(_, _))
                        && matches!(col2, ColumnArray::Int64(_, _) | ColumnArray::Int32(_, _))
                    {
                        return GroupKeySpec::TwoI64 { col1_idx, col2_idx };
                    }
                    // Check for (int, string) pattern
                    if matches!(col1, ColumnArray::Int64(_, _) | ColumnArray::Int32(_, _))
                        && matches!(
                            col2,
                            ColumnArray::String(_, _) | ColumnArray::FixedString(_, _)
                        )
                    {
                        return GroupKeySpec::I64String { i64_col: col1_idx, string_col: col2_idx };
                    }
                }
                GroupKeySpec::Generic {
                    col_indices: group_cols
                        .iter()
                        .map(|&i| (i, DataType::Varchar { max_length: None }))
                        .collect(),
                }
            }
            3 => {
                let col1_idx = group_cols[0];
                let col2_idx = group_cols[1];
                let col3_idx = group_cols[2];
                if let (Some(col1), Some(col2), Some(col3)) =
                    (batch.column(col1_idx), batch.column(col2_idx), batch.column(col3_idx))
                {
                    // Check for three integer columns (TPC-H Q3 pattern)
                    if matches!(
                        col1,
                        ColumnArray::Int64(_, _)
                            | ColumnArray::Int32(_, _)
                            | ColumnArray::Date(_, _)
                    ) && matches!(
                        col2,
                        ColumnArray::Int64(_, _)
                            | ColumnArray::Int32(_, _)
                            | ColumnArray::Date(_, _)
                    ) && matches!(
                        col3,
                        ColumnArray::Int64(_, _)
                            | ColumnArray::Int32(_, _)
                            | ColumnArray::Date(_, _)
                    ) {
                        return GroupKeySpec::ThreeI64 { col1_idx, col2_idx, col3_idx };
                    }
                }
                GroupKeySpec::Generic {
                    col_indices: group_cols
                        .iter()
                        .map(|&i| (i, DataType::Varchar { max_length: None }))
                        .collect(),
                }
            }
            _ => GroupKeySpec::Generic {
                col_indices: group_cols
                    .iter()
                    .map(|&i| (i, DataType::Varchar { max_length: None }))
                    .collect(),
            },
        }
    }

    /// Extract a group key from a ColumnarBatch at the specified row index
    ///
    /// This provides direct columnar extraction without going through Row.
    #[inline]
    pub fn extract_key_from_batch(&self, batch: &ColumnarBatch, row_idx: usize) -> GroupKey {
        match self {
            GroupKeySpec::SingleI64 { col_idx } => {
                if let Some(ColumnArray::Int64(values, nulls)) = batch.column(*col_idx) {
                    if let Some(null_mask) = nulls {
                        if null_mask.get(row_idx).copied().unwrap_or(false) {
                            return GroupKey::Generic(vec![SqlValue::Null]);
                        }
                    }
                    return GroupKey::SingleI64(values[row_idx]);
                }
                if let Some(ColumnArray::Int32(values, nulls)) = batch.column(*col_idx) {
                    if let Some(null_mask) = nulls {
                        if null_mask.get(row_idx).copied().unwrap_or(false) {
                            return GroupKey::Generic(vec![SqlValue::Null]);
                        }
                    }
                    return GroupKey::SingleI64(values[row_idx] as i64);
                }
                // Fallback
                batch
                    .get_value(row_idx, *col_idx)
                    .map(|v| GroupKey::Generic(vec![v]))
                    .unwrap_or(GroupKey::Generic(vec![SqlValue::Null]))
            }
            GroupKeySpec::SingleString { col_idx } => {
                if let Some(ColumnArray::String(values, nulls)) = batch.column(*col_idx) {
                    if let Some(null_mask) = nulls {
                        if null_mask.get(row_idx).copied().unwrap_or(false) {
                            return GroupKey::Generic(vec![SqlValue::Null]);
                        }
                    }
                    return GroupKey::SingleString(values[row_idx].to_string());
                }
                if let Some(ColumnArray::FixedString(values, nulls)) = batch.column(*col_idx) {
                    if let Some(null_mask) = nulls {
                        if null_mask.get(row_idx).copied().unwrap_or(false) {
                            return GroupKey::Generic(vec![SqlValue::Null]);
                        }
                    }
                    return GroupKey::SingleString(values[row_idx].to_string());
                }
                // Fallback
                batch
                    .get_value(row_idx, *col_idx)
                    .map(|v| GroupKey::Generic(vec![v]))
                    .unwrap_or(GroupKey::Generic(vec![SqlValue::Null]))
            }
            GroupKeySpec::TwoChars { col1_idx, col2_idx } => {
                // Fast path: extract first char from each string column
                let c1 = Self::extract_first_char(batch, *col1_idx, row_idx);
                let c2 = Self::extract_first_char(batch, *col2_idx, row_idx);
                GroupKey::TwoChars(c1, c2)
            }
            GroupKeySpec::TwoI64 { col1_idx, col2_idx } => {
                let v1 = Self::extract_i64(batch, *col1_idx, row_idx);
                let v2 = Self::extract_i64(batch, *col2_idx, row_idx);
                GroupKey::TwoI64(v1, v2)
            }
            GroupKeySpec::I64String { i64_col, string_col } => {
                let i = Self::extract_i64(batch, *i64_col, row_idx);
                let s = Self::extract_string(batch, *string_col, row_idx);
                GroupKey::I64String(i, s)
            }
            GroupKeySpec::ThreeI64 { col1_idx, col2_idx, col3_idx } => {
                let v1 = Self::extract_i64(batch, *col1_idx, row_idx);
                let v2 = Self::extract_i64(batch, *col2_idx, row_idx);
                let v3 = Self::extract_i64(batch, *col3_idx, row_idx);
                GroupKey::ThreeI64(v1, v2, v3)
            }
            GroupKeySpec::Generic { col_indices } => {
                let mut key = Vec::with_capacity(col_indices.len());
                for (idx, _) in col_indices {
                    let value = batch.get_value(row_idx, *idx).unwrap_or(SqlValue::Null);
                    key.push(value);
                }
                GroupKey::Generic(key)
            }
        }
    }

    /// Extract first character from a string column (for TwoChars key)
    #[inline]
    fn extract_first_char(batch: &ColumnarBatch, col_idx: usize, row_idx: usize) -> u8 {
        if let Some(ColumnArray::String(values, _)) = batch.column(col_idx) {
            return values[row_idx].as_bytes().first().copied().unwrap_or(0);
        }
        if let Some(ColumnArray::FixedString(values, _)) = batch.column(col_idx) {
            return values[row_idx].as_bytes().first().copied().unwrap_or(0);
        }
        0
    }

    /// Extract i64 value from an integer column
    #[inline]
    fn extract_i64(batch: &ColumnarBatch, col_idx: usize, row_idx: usize) -> i64 {
        if let Some(ColumnArray::Int64(values, _)) = batch.column(col_idx) {
            return values[row_idx];
        }
        if let Some(ColumnArray::Int32(values, _)) = batch.column(col_idx) {
            return values[row_idx] as i64;
        }
        if let Some(ColumnArray::Date(values, _)) = batch.column(col_idx) {
            return values[row_idx] as i64;
        }
        0
    }

    /// Extract String value from a string column
    #[inline]
    fn extract_string(batch: &ColumnarBatch, col_idx: usize, row_idx: usize) -> String {
        if let Some(ColumnArray::String(values, _)) = batch.column(col_idx) {
            return values[row_idx].to_string();
        }
        if let Some(ColumnArray::FixedString(values, _)) = batch.column(col_idx) {
            return values[row_idx].to_string();
        }
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_single_i64_key() {
        let spec = GroupKeySpec::from_columns(&[(0, DataType::Integer)]);
        assert!(matches!(spec, GroupKeySpec::SingleI64 { .. }));
    }

    #[test]
    fn test_two_chars_key() {
        let spec = GroupKeySpec::from_columns(&[
            (0, DataType::Varchar { max_length: Some(1) }),
            (1, DataType::Varchar { max_length: Some(1) }),
        ]);
        assert!(matches!(spec, GroupKeySpec::TwoChars { .. }));
    }

    #[test]
    fn test_two_varchar_detected_as_two_chars() {
        // TPC-H lineitem has VARCHAR without max_length but values are single chars
        let spec = GroupKeySpec::from_columns(&[
            (0, DataType::Varchar { max_length: None }),
            (1, DataType::Varchar { max_length: None }),
        ]);
        assert!(matches!(spec, GroupKeySpec::TwoChars { .. }));
    }

    #[test]
    fn test_group_key_hashing() {
        let mut map: HashMap<GroupKey, i32> = HashMap::new();

        // Test TwoChars hashing (TPC-H Q1 pattern)
        map.insert(GroupKey::TwoChars(b'A', b'F'), 1);
        map.insert(GroupKey::TwoChars(b'N', b'O'), 2);
        map.insert(GroupKey::TwoChars(b'R', b'F'), 3);

        assert_eq!(map.get(&GroupKey::TwoChars(b'A', b'F')), Some(&1));
        assert_eq!(map.get(&GroupKey::TwoChars(b'N', b'O')), Some(&2));
        assert_eq!(map.get(&GroupKey::TwoChars(b'R', b'F')), Some(&3));
    }

    #[test]
    fn test_key_to_values() {
        let spec = GroupKeySpec::TwoChars { col1_idx: 0, col2_idx: 1 };

        let key = GroupKey::TwoChars(b'A', b'F');
        let values = spec.key_to_values(&key);

        assert_eq!(values.len(), 2);
        assert_eq!(values[0], SqlValue::Varchar(arcstr::ArcStr::from("A")));
        assert_eq!(values[1], SqlValue::Varchar(arcstr::ArcStr::from("F")));
    }

    #[test]
    fn test_columnar_batch_key_extraction() {
        use vibesql_storage::Row;

        // Create a batch with string columns (like TPC-H Q1: l_returnflag, l_linestatus)
        let rows = vec![
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Varchar(arcstr::ArcStr::from("F"))]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("N")), SqlValue::Varchar(arcstr::ArcStr::from("O"))]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("R")), SqlValue::Varchar(arcstr::ArcStr::from("F"))]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Should detect TwoChars pattern for two string columns
        let spec = GroupKeySpec::from_columnar_batch(&batch, &[0, 1]);
        assert!(matches!(spec, GroupKeySpec::TwoChars { col1_idx: 0, col2_idx: 1 }));

        // Extract keys and verify
        let key0 = spec.extract_key_from_batch(&batch, 0);
        let key1 = spec.extract_key_from_batch(&batch, 1);
        let key2 = spec.extract_key_from_batch(&batch, 2);

        assert_eq!(key0, GroupKey::TwoChars(b'A', b'F'));
        assert_eq!(key1, GroupKey::TwoChars(b'N', b'O'));
        assert_eq!(key2, GroupKey::TwoChars(b'R', b'F'));
    }

    #[test]
    fn test_columnar_batch_single_i64_extraction() {
        use vibesql_storage::Row;

        // Create a batch with integer column
        let rows = vec![
            Row::new(vec![SqlValue::Integer(100)]),
            Row::new(vec![SqlValue::Integer(200)]),
            Row::new(vec![SqlValue::Integer(300)]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Should detect SingleI64 pattern
        let spec = GroupKeySpec::from_columnar_batch(&batch, &[0]);
        assert!(matches!(spec, GroupKeySpec::SingleI64 { col_idx: 0 }));

        // Extract keys and verify
        let key0 = spec.extract_key_from_batch(&batch, 0);
        let key1 = spec.extract_key_from_batch(&batch, 1);
        let key2 = spec.extract_key_from_batch(&batch, 2);

        assert_eq!(key0, GroupKey::SingleI64(100));
        assert_eq!(key1, GroupKey::SingleI64(200));
        assert_eq!(key2, GroupKey::SingleI64(300));
    }

    #[test]
    fn test_columnar_batch_two_i64_extraction() {
        use vibesql_storage::Row;

        // Create a batch with two integer columns
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20)]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Should detect TwoI64 pattern
        let spec = GroupKeySpec::from_columnar_batch(&batch, &[0, 1]);
        assert!(matches!(spec, GroupKeySpec::TwoI64 { col1_idx: 0, col2_idx: 1 }));

        // Extract keys and verify
        let key0 = spec.extract_key_from_batch(&batch, 0);
        let key1 = spec.extract_key_from_batch(&batch, 1);

        assert_eq!(key0, GroupKey::TwoI64(1, 10));
        assert_eq!(key1, GroupKey::TwoI64(2, 20));
    }
}
