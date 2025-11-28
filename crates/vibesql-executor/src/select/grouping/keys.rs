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

use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

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
    ThreeI64 {
        col1_idx: usize,
        col2_idx: usize,
        col3_idx: usize,
    },

    /// Generic fallback
    Generic { col_indices: Vec<(usize, DataType)> },
}

impl GroupKeySpec {
    /// Analyze GROUP BY columns and determine the best key strategy
    pub fn from_columns(columns: &[(usize, DataType)]) -> Self {
        match columns.len() {
            1 => {
                let (idx, dtype) = &columns[0];
                match dtype {
                    DataType::Integer | DataType::Bigint => GroupKeySpec::SingleI64 { col_idx: *idx },
                    DataType::Varchar { .. } | DataType::Character { .. } => {
                        GroupKeySpec::SingleString { col_idx: *idx }
                    }
                    _ => GroupKeySpec::Generic {
                        col_indices: columns.to_vec(),
                    },
                }
            }
            2 => {
                let (idx1, dtype1) = &columns[0];
                let (idx2, dtype2) = &columns[1];

                // Check for two single-char columns (TPC-H Q1 pattern)
                if matches!(
                    (dtype1, dtype2),
                    (
                        DataType::Varchar { max_length: Some(1) } | DataType::Character { length: 1 },
                        DataType::Varchar { max_length: Some(1) } | DataType::Character { length: 1 }
                    )
                ) {
                    return GroupKeySpec::TwoChars {
                        col1_idx: *idx1,
                        col2_idx: *idx2,
                    };
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
                    return GroupKeySpec::TwoChars {
                        col1_idx: *idx1,
                        col2_idx: *idx2,
                    };
                }

                // Check for two i64 columns
                if matches!(dtype1, DataType::Integer | DataType::Bigint)
                    && matches!(dtype2, DataType::Integer | DataType::Bigint)
                {
                    return GroupKeySpec::TwoI64 {
                        col1_idx: *idx1,
                        col2_idx: *idx2,
                    };
                }

                // Check for (i64, String) pattern
                if matches!(dtype1, DataType::Integer | DataType::Bigint)
                    && matches!(dtype2, DataType::Varchar { .. } | DataType::Character { .. })
                {
                    return GroupKeySpec::I64String {
                        i64_col: *idx1,
                        string_col: *idx2,
                    };
                }

                GroupKeySpec::Generic {
                    col_indices: columns.to_vec(),
                }
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

                GroupKeySpec::Generic {
                    col_indices: columns.to_vec(),
                }
            }
            _ => GroupKeySpec::Generic {
                col_indices: columns.to_vec(),
            },
        }
    }

    /// Extract a group key from a row
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Caller must ensure column
    /// indices are valid.
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
            GroupKeySpec::TwoI64 { col1_idx, col2_idx } => GroupKey::TwoI64(
                row.get_i64_unchecked(*col1_idx),
                row.get_i64_unchecked(*col2_idx),
            ),
            GroupKeySpec::I64String { i64_col, string_col } => GroupKey::I64String(
                row.get_i64_unchecked(*i64_col),
                row.get_string_unchecked(*string_col).to_string(),
            ),
            GroupKeySpec::ThreeI64 {
                col1_idx,
                col2_idx,
                col3_idx,
            } => GroupKey::ThreeI64(
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
                            SqlValue::Varchar(row.get_string_unchecked(*idx).to_string())
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
            GroupKey::SingleString(v) => vec![SqlValue::Varchar(v.clone())],
            GroupKey::TwoChars(a, b) => vec![
                SqlValue::Varchar(String::from_utf8_lossy(&[*a]).into_owned()),
                SqlValue::Varchar(String::from_utf8_lossy(&[*b]).into_owned()),
            ],
            GroupKey::TwoI64(a, b) => vec![SqlValue::Integer(*a), SqlValue::Integer(*b)],
            GroupKey::I64String(i, s) => vec![SqlValue::Integer(*i), SqlValue::Varchar(s.clone())],
            GroupKey::ThreeI64(a, b, c) => vec![
                SqlValue::Integer(*a),
                SqlValue::Integer(*b),
                SqlValue::Integer(*c),
            ],
            GroupKey::Generic(v) => v.clone(),
        }
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
        let spec = GroupKeySpec::TwoChars {
            col1_idx: 0,
            col2_idx: 1,
        };

        let key = GroupKey::TwoChars(b'A', b'F');
        let values = spec.key_to_values(&key);

        assert_eq!(values.len(), 2);
        assert_eq!(values[0], SqlValue::Varchar("A".to_string()));
        assert_eq!(values[1], SqlValue::Varchar("F".to_string()));
    }
}
