//! Column data storage types.
//!
//! This module provides the `ColumnData` enum for storing typed column data
//! with NULL bitmap.
//!
//! ## Zero-Copy Design
//!
//! Column data uses `Arc<Vec<T>>` for all arrays, enabling:
//! - Zero-copy sharing between storage and executor layers
//! - O(1) clone operations (reference count bump instead of data copy)
//! - Cache-friendly columnar data that can be shared across query executions

use std::sync::Arc;

use vibesql_types::{Date, Interval, SqlValue, Time, Timestamp};

/// Typed column data with NULL bitmap
///
/// Each variant stores a vector of non-NULL values and a separate bitmap
/// indicating which positions are NULL. This design:
/// - Avoids Option<T> overhead (16 bytes vs 8 bytes for f64)
/// - Enables direct SIMD operations on value vectors
/// - Provides O(1) NULL checks via bitmap
/// - Uses Arc for zero-copy sharing with executor layer
/// - String columns use Arc<str> for O(1) cloning
#[derive(Debug, Clone)]
pub enum ColumnData {
    /// 64-bit signed integers
    Int64 { values: Arc<Vec<i64>>, nulls: Arc<Vec<bool>> },
    /// 64-bit floating point
    Float64 { values: Arc<Vec<f64>>, nulls: Arc<Vec<bool>> },
    /// Variable-length strings (using Arc<str> for O(1) cloning)
    String { values: Arc<Vec<Arc<str>>>, nulls: Arc<Vec<bool>> },
    /// Boolean values
    Bool { values: Arc<Vec<bool>>, nulls: Arc<Vec<bool>> },
    /// Date values
    Date { values: Arc<Vec<Date>>, nulls: Arc<Vec<bool>> },
    /// Time values
    Time { values: Arc<Vec<Time>>, nulls: Arc<Vec<bool>> },
    /// Timestamp values
    Timestamp { values: Arc<Vec<Timestamp>>, nulls: Arc<Vec<bool>> },
    /// Interval values
    Interval { values: Arc<Vec<Interval>>, nulls: Arc<Vec<bool>> },
    /// Vector values (for AI/ML workloads)
    Vector { values: Arc<Vec<Vec<f32>>>, nulls: Arc<Vec<bool>> },
    /// Blob values (binary data)
    Blob { values: Arc<Vec<Vec<u8>>>, nulls: Arc<Vec<bool>> },
}

#[allow(clippy::type_complexity)]
impl ColumnData {
    /// Get the number of values in this column (including NULLs)
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Int64 { nulls, .. } => nulls.len(),
            ColumnData::Float64 { nulls, .. } => nulls.len(),
            ColumnData::String { nulls, .. } => nulls.len(),
            ColumnData::Bool { nulls, .. } => nulls.len(),
            ColumnData::Date { nulls, .. } => nulls.len(),
            ColumnData::Time { nulls, .. } => nulls.len(),
            ColumnData::Timestamp { nulls, .. } => nulls.len(),
            ColumnData::Interval { nulls, .. } => nulls.len(),
            ColumnData::Vector { nulls, .. } => nulls.len(),
            ColumnData::Blob { nulls, .. } => nulls.len(),
        }
    }

    /// Check if the column is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Estimate the memory size of this column in bytes
    ///
    /// This is used for memory budgeting in the columnar cache.
    /// The estimate includes:
    /// - Value storage (type-specific size * element count)
    /// - NULL bitmap (1 byte per element, not packed)
    /// - Vec overhead (capacity, length, pointer)
    pub fn size_in_bytes(&self) -> usize {
        const VEC_OVERHEAD: usize = 3 * std::mem::size_of::<usize>(); // ptr, len, cap

        match self {
            ColumnData::Int64 { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<i64>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Float64 { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<f64>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::String { values, nulls } => {
                // For Arc<str>, we need to account for the Arc overhead
                // plus the actual string data on the heap
                let arc_overhead = std::mem::size_of::<Arc<str>>(); // ptr + refcount
                let string_data: usize = values.iter().map(|s| s.len()).sum();
                VEC_OVERHEAD * 2
                    + values.capacity() * arc_overhead
                    + string_data
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Bool { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<bool>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Date { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<Date>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Time { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<Time>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Timestamp { values, nulls } => {
                VEC_OVERHEAD * 2
                    + values.capacity() * std::mem::size_of::<Timestamp>()
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Interval { values, nulls } => {
                // Interval contains a String, so we need to account for that
                let interval_overhead = std::mem::size_of::<Interval>();
                let string_data: usize = values.iter().map(|i| i.value.capacity()).sum();
                VEC_OVERHEAD * 2
                    + values.capacity() * interval_overhead
                    + string_data
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Vector { values, nulls } => {
                // Vector contains Vec<f32>, so we need to account for each inner vector
                let vec_overhead = std::mem::size_of::<Vec<f32>>();
                let vector_data: usize =
                    values.iter().map(|v| v.capacity() * std::mem::size_of::<f32>()).sum();
                VEC_OVERHEAD * 2
                    + values.capacity() * vec_overhead
                    + vector_data
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
            ColumnData::Blob { values, nulls } => {
                // Blob contains Vec<u8>, so we need to account for each inner vector
                let vec_overhead = std::mem::size_of::<Vec<u8>>();
                let blob_data: usize = values.iter().map(|v| v.capacity()).sum();
                VEC_OVERHEAD * 2
                    + values.capacity() * vec_overhead
                    + blob_data
                    + nulls.capacity() * std::mem::size_of::<bool>()
            }
        }
    }

    /// Check if the value at the given index is NULL
    pub fn is_null(&self, index: usize) -> bool {
        match self {
            ColumnData::Int64 { nulls, .. } => nulls[index],
            ColumnData::Float64 { nulls, .. } => nulls[index],
            ColumnData::String { nulls, .. } => nulls[index],
            ColumnData::Bool { nulls, .. } => nulls[index],
            ColumnData::Date { nulls, .. } => nulls[index],
            ColumnData::Time { nulls, .. } => nulls[index],
            ColumnData::Timestamp { nulls, .. } => nulls[index],
            ColumnData::Interval { nulls, .. } => nulls[index],
            ColumnData::Vector { nulls, .. } => nulls[index],
            ColumnData::Blob { nulls, .. } => nulls[index],
        }
    }

    /// Get the SQL value at the given index (converts back to SqlValue)
    pub fn get(&self, index: usize) -> SqlValue {
        if self.is_null(index) {
            return SqlValue::Null;
        }

        match self {
            ColumnData::Int64 { values, .. } => SqlValue::Integer(values[index]),
            ColumnData::Float64 { values, .. } => SqlValue::Double(values[index]),
            ColumnData::String { values, .. } => {
                SqlValue::Varchar(arcstr::ArcStr::from(values[index].as_ref()))
            }
            ColumnData::Bool { values, .. } => SqlValue::Boolean(values[index]),
            ColumnData::Date { values, .. } => SqlValue::Date(values[index]),
            ColumnData::Time { values, .. } => SqlValue::Time(values[index]),
            ColumnData::Timestamp { values, .. } => SqlValue::Timestamp(values[index]),
            ColumnData::Interval { values, .. } => SqlValue::Interval(values[index].clone()),
            ColumnData::Vector { values, .. } => SqlValue::Vector(values[index].clone()),
            ColumnData::Blob { values, .. } => SqlValue::Blob(values[index].clone()),
        }
    }

    /// Get the underlying Arc for i64 values (zero-copy sharing with executor)
    pub fn as_i64_arc(&self) -> Option<(&Arc<Vec<i64>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Int64 { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for f64 values (zero-copy sharing with executor)
    pub fn as_f64_arc(&self) -> Option<(&Arc<Vec<f64>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Float64 { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for string values (zero-copy sharing with executor)
    pub fn as_string_arc(&self) -> Option<(&Arc<Vec<Arc<str>>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::String { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for bool values (zero-copy sharing with executor)
    pub fn as_bool_arc(&self) -> Option<(&Arc<Vec<bool>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Bool { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for date values (zero-copy sharing with executor)
    pub fn as_date_arc(&self) -> Option<(&Arc<Vec<Date>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Date { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for timestamp values (zero-copy sharing with executor)
    pub fn as_timestamp_arc(&self) -> Option<(&Arc<Vec<Timestamp>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Timestamp { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for time values (zero-copy sharing with executor)
    pub fn as_time_arc(&self) -> Option<(&Arc<Vec<Time>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Time { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Get the underlying Arc for interval values (zero-copy sharing with executor)
    pub fn as_interval_arc(&self) -> Option<(&Arc<Vec<Interval>>, &Arc<Vec<bool>>)> {
        match self {
            ColumnData::Interval { values, nulls } => Some((values, nulls)),
            _ => None,
        }
    }

    /// Append a SQL value to this column, mutating the underlying vectors in place.
    ///
    /// Uses `Arc::make_mut` to get mutable access to the underlying vectors.
    /// If there are no other references (strong count == 1), this is zero-copy.
    /// If there are outstanding read snapshots, the vectors are cloned on first write
    /// (copy-on-write semantics), preserving snapshot isolation.
    ///
    /// # Arguments
    /// * `value` - The SQL value to append
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(String)` if the value type doesn't match the column type
    pub fn push_value(&mut self, value: &SqlValue) -> Result<(), String> {
        match self {
            ColumnData::Int64 { values, nulls } => match value {
                SqlValue::Integer(v) => {
                    Arc::make_mut(values).push(*v);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Bigint(v) => {
                    Arc::make_mut(values).push(*v);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Smallint(v) => {
                    Arc::make_mut(values).push(*v as i64);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Null => {
                    Arc::make_mut(values).push(0);
                    Arc::make_mut(nulls).push(true);
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected Int64, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::Float64 { values, nulls } => match value {
                SqlValue::Float(v) => {
                    Arc::make_mut(values).push(*v as f64);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Double(v) => {
                    Arc::make_mut(values).push(*v);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Real(v) => {
                    Arc::make_mut(values).push(*v as f64);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Numeric(v) => {
                    Arc::make_mut(values).push(*v);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Unsigned(v) => {
                    Arc::make_mut(values).push(*v as f64);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Null => {
                    Arc::make_mut(values).push(0.0);
                    Arc::make_mut(nulls).push(true);
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected Float64, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::String { values, nulls } => match value {
                SqlValue::Varchar(v) => {
                    Arc::make_mut(values).push(Arc::from(v.as_str()));
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Character(v) => {
                    Arc::make_mut(values).push(Arc::from(v.as_str()));
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Null => {
                    Arc::make_mut(values).push(Arc::from(""));
                    Arc::make_mut(nulls).push(true);
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected String, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::Bool { values, nulls } => match value {
                SqlValue::Boolean(v) => {
                    Arc::make_mut(values).push(*v);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Null => {
                    Arc::make_mut(values).push(false);
                    Arc::make_mut(nulls).push(true);
                }
                other => {
                    return Err(format!("Type mismatch: expected Bool, got {}", other.type_name()));
                }
            },
            ColumnData::Date { values, nulls } => match value {
                SqlValue::Date(v) => {
                    Arc::make_mut(values).push(*v);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Null => {
                    Arc::make_mut(values).push(Date::new(1970, 1, 1).unwrap());
                    Arc::make_mut(nulls).push(true);
                }
                other => {
                    return Err(format!("Type mismatch: expected Date, got {}", other.type_name()));
                }
            },
            ColumnData::Time { values, nulls } => match value {
                SqlValue::Time(v) => {
                    Arc::make_mut(values).push(*v);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Null => {
                    Arc::make_mut(values).push(Time::new(0, 0, 0, 0).unwrap());
                    Arc::make_mut(nulls).push(true);
                }
                other => {
                    return Err(format!("Type mismatch: expected Time, got {}", other.type_name()));
                }
            },
            ColumnData::Timestamp { values, nulls } => match value {
                SqlValue::Timestamp(v) => {
                    Arc::make_mut(values).push(*v);
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Null => {
                    let date = Date::new(1970, 1, 1).unwrap();
                    let time = Time::new(0, 0, 0, 0).unwrap();
                    Arc::make_mut(values).push(Timestamp::new(date, time));
                    Arc::make_mut(nulls).push(true);
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected Timestamp, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::Interval { values, nulls } => match value {
                SqlValue::Interval(v) => {
                    Arc::make_mut(values).push(v.clone());
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Null => {
                    Arc::make_mut(values).push(Interval::new("0".to_string()));
                    Arc::make_mut(nulls).push(true);
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected Interval, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::Vector { values, nulls } => match value {
                SqlValue::Vector(v) => {
                    Arc::make_mut(values).push(v.clone());
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Null => {
                    Arc::make_mut(values).push(Vec::new());
                    Arc::make_mut(nulls).push(true);
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected Vector, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::Blob { values, nulls } => match value {
                SqlValue::Blob(v) => {
                    Arc::make_mut(values).push(v.clone());
                    Arc::make_mut(nulls).push(false);
                }
                SqlValue::Null => {
                    Arc::make_mut(values).push(Vec::new());
                    Arc::make_mut(nulls).push(true);
                }
                other => {
                    return Err(format!("Type mismatch: expected Blob, got {}", other.type_name()));
                }
            },
        }
        Ok(())
    }

    /// Remove the value at the given index from this column.
    ///
    /// Uses swap-remove semantics for O(1) removal when order doesn't matter,
    /// but this method preserves order using regular remove (O(n)).
    /// For deletion-heavy workloads, consider using a deletion bitmap instead.
    ///
    /// # Arguments
    /// * `index` - The index of the value to remove
    ///
    /// # Panics
    /// Panics if `index` is out of bounds
    pub fn remove_at(&mut self, index: usize) {
        match self {
            ColumnData::Int64 { values, nulls } => {
                Arc::make_mut(values).remove(index);
                Arc::make_mut(nulls).remove(index);
            }
            ColumnData::Float64 { values, nulls } => {
                Arc::make_mut(values).remove(index);
                Arc::make_mut(nulls).remove(index);
            }
            ColumnData::String { values, nulls } => {
                Arc::make_mut(values).remove(index);
                Arc::make_mut(nulls).remove(index);
            }
            ColumnData::Bool { values, nulls } => {
                Arc::make_mut(values).remove(index);
                Arc::make_mut(nulls).remove(index);
            }
            ColumnData::Date { values, nulls } => {
                Arc::make_mut(values).remove(index);
                Arc::make_mut(nulls).remove(index);
            }
            ColumnData::Time { values, nulls } => {
                Arc::make_mut(values).remove(index);
                Arc::make_mut(nulls).remove(index);
            }
            ColumnData::Timestamp { values, nulls } => {
                Arc::make_mut(values).remove(index);
                Arc::make_mut(nulls).remove(index);
            }
            ColumnData::Interval { values, nulls } => {
                Arc::make_mut(values).remove(index);
                Arc::make_mut(nulls).remove(index);
            }
            ColumnData::Vector { values, nulls } => {
                Arc::make_mut(values).remove(index);
                Arc::make_mut(nulls).remove(index);
            }
            ColumnData::Blob { values, nulls } => {
                Arc::make_mut(values).remove(index);
                Arc::make_mut(nulls).remove(index);
            }
        }
    }

    /// Update the value at the given index in this column.
    ///
    /// Uses `Arc::make_mut` for copy-on-write semantics.
    ///
    /// # Arguments
    /// * `index` - The index of the value to update
    /// * `value` - The new SQL value
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(String)` if the value type doesn't match the column type
    pub fn set_value(&mut self, index: usize, value: &SqlValue) -> Result<(), String> {
        match self {
            ColumnData::Int64 { values, nulls } => match value {
                SqlValue::Integer(v) => {
                    Arc::make_mut(values)[index] = *v;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Bigint(v) => {
                    Arc::make_mut(values)[index] = *v;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Smallint(v) => {
                    Arc::make_mut(values)[index] = *v as i64;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Null => {
                    Arc::make_mut(values)[index] = 0;
                    Arc::make_mut(nulls)[index] = true;
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected Int64, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::Float64 { values, nulls } => match value {
                SqlValue::Float(v) => {
                    Arc::make_mut(values)[index] = *v as f64;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Double(v) => {
                    Arc::make_mut(values)[index] = *v;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Real(v) => {
                    Arc::make_mut(values)[index] = *v as f64;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Numeric(v) => {
                    Arc::make_mut(values)[index] = *v;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Unsigned(v) => {
                    Arc::make_mut(values)[index] = *v as f64;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Null => {
                    Arc::make_mut(values)[index] = 0.0;
                    Arc::make_mut(nulls)[index] = true;
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected Float64, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::String { values, nulls } => match value {
                SqlValue::Varchar(v) => {
                    Arc::make_mut(values)[index] = Arc::from(v.as_str());
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Character(v) => {
                    Arc::make_mut(values)[index] = Arc::from(v.as_str());
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Null => {
                    Arc::make_mut(values)[index] = Arc::from("");
                    Arc::make_mut(nulls)[index] = true;
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected String, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::Bool { values, nulls } => match value {
                SqlValue::Boolean(v) => {
                    Arc::make_mut(values)[index] = *v;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Null => {
                    Arc::make_mut(values)[index] = false;
                    Arc::make_mut(nulls)[index] = true;
                }
                other => {
                    return Err(format!("Type mismatch: expected Bool, got {}", other.type_name()));
                }
            },
            ColumnData::Date { values, nulls } => match value {
                SqlValue::Date(v) => {
                    Arc::make_mut(values)[index] = *v;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Null => {
                    Arc::make_mut(values)[index] = Date::new(1970, 1, 1).unwrap();
                    Arc::make_mut(nulls)[index] = true;
                }
                other => {
                    return Err(format!("Type mismatch: expected Date, got {}", other.type_name()));
                }
            },
            ColumnData::Time { values, nulls } => match value {
                SqlValue::Time(v) => {
                    Arc::make_mut(values)[index] = *v;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Null => {
                    Arc::make_mut(values)[index] = Time::new(0, 0, 0, 0).unwrap();
                    Arc::make_mut(nulls)[index] = true;
                }
                other => {
                    return Err(format!("Type mismatch: expected Time, got {}", other.type_name()));
                }
            },
            ColumnData::Timestamp { values, nulls } => match value {
                SqlValue::Timestamp(v) => {
                    Arc::make_mut(values)[index] = *v;
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Null => {
                    let date = Date::new(1970, 1, 1).unwrap();
                    let time = Time::new(0, 0, 0, 0).unwrap();
                    Arc::make_mut(values)[index] = Timestamp::new(date, time);
                    Arc::make_mut(nulls)[index] = true;
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected Timestamp, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::Interval { values, nulls } => match value {
                SqlValue::Interval(v) => {
                    Arc::make_mut(values)[index] = v.clone();
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Null => {
                    Arc::make_mut(values)[index] = Interval::new("0".to_string());
                    Arc::make_mut(nulls)[index] = true;
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected Interval, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::Vector { values, nulls } => match value {
                SqlValue::Vector(v) => {
                    Arc::make_mut(values)[index] = v.clone();
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Null => {
                    Arc::make_mut(values)[index] = Vec::new();
                    Arc::make_mut(nulls)[index] = true;
                }
                other => {
                    return Err(format!(
                        "Type mismatch: expected Vector, got {}",
                        other.type_name()
                    ));
                }
            },
            ColumnData::Blob { values, nulls } => match value {
                SqlValue::Blob(v) => {
                    Arc::make_mut(values)[index] = v.clone();
                    Arc::make_mut(nulls)[index] = false;
                }
                SqlValue::Null => {
                    Arc::make_mut(values)[index] = Vec::new();
                    Arc::make_mut(nulls)[index] = true;
                }
                other => {
                    return Err(format!("Type mismatch: expected Blob, got {}", other.type_name()));
                }
            },
        }
        Ok(())
    }
}

#[cfg(test)]
mod blob_roundtrip_tests {
    //! Issue #6033 (Stage 3): JSONB blobs are stored as `SqlValue::Blob` and the
    //! columnar path must preserve their bytes exactly through scan (and, by
    //! extension, checkpoint reload, which serializes `ColumnData` through the
    //! same `Vec<u8>` representation with no text coercion). These tests encode
    //! the byte-exactness the curator verified manually with a 2000-row table.

    use super::*;
    use crate::{
        columnar::{builder::ColumnBuilder, table::ColumnarTable, types::ColumnTypeClass},
        Row,
    };

    /// A representative spread of blob payloads, including a JSONB-style header
    /// byte (0x8B) as produced by `jsonb('[...]')`, an empty blob, and a blob
    /// containing embedded NUL and high bytes that a text codec would mangle.
    fn sample_blobs(n: usize) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| {
                let b = (i % 256) as u8;
                vec![0x8B, b, 0x00, 0xFF, b.wrapping_add(1), b.wrapping_mul(3)]
            })
            .collect()
    }

    /// Direct `ColumnData::Blob` scan path: bytes pushed through the builder come
    /// back out of `ColumnData::get` byte-identical (no re-encode; `get` clones
    /// the stored `Vec<u8>`).
    #[test]
    fn column_data_blob_scan_is_byte_identical() {
        let blobs = sample_blobs(2000);
        let mut builder = ColumnBuilder::new(ColumnTypeClass::Blob, blobs.len());
        for b in &blobs {
            builder.push(&SqlValue::Blob(b.clone())).unwrap();
        }
        let column = builder.build();
        assert_eq!(column.len(), blobs.len());

        for (i, expected) in blobs.iter().enumerate() {
            assert!(!column.is_null(i), "row {i} must be non-null");
            match column.get(i) {
                SqlValue::Blob(got) => assert_eq!(
                    &got, expected,
                    "blob bytes at row {i} must survive the columnar scan byte-identically"
                ),
                other => panic!("row {i}: expected Blob, got {other:?}"),
            }
        }
    }

    /// A NULL blob round-trips as NULL, and a zero-length blob stays a
    /// zero-length blob (not conflated with NULL).
    #[test]
    fn column_data_blob_null_and_empty_are_distinct() {
        let mut builder = ColumnBuilder::new(ColumnTypeClass::Blob, 3);
        builder.push(&SqlValue::Blob(vec![0x8B, 0x01])).unwrap();
        builder.push(&SqlValue::Null).unwrap();
        builder.push(&SqlValue::Blob(Vec::new())).unwrap();
        let column = builder.build();

        assert!(!column.is_null(0));
        assert_eq!(column.get(0), SqlValue::Blob(vec![0x8B, 0x01]));
        assert!(column.is_null(1), "explicit NULL blob must stay NULL");
        assert!(!column.is_null(2), "empty blob is not NULL");
        assert_eq!(column.get(2), SqlValue::Blob(Vec::new()));
    }

    /// Full `ColumnarTable::from_rows` -> `to_rows` round-trip over a
    /// columnar-sized table (2000 rows) with a Blob column: every blob comes back
    /// byte-identical. This exercises the same conversion path a large table
    /// takes on the way into columnar storage.
    #[test]
    fn columnar_table_blob_column_roundtrip_2000_rows() {
        let blobs = sample_blobs(2000);
        let rows: Vec<Row> = blobs
            .iter()
            .enumerate()
            .map(|(i, b)| Row::new(vec![SqlValue::Integer(i as i64), SqlValue::Blob(b.clone())]))
            .collect();
        let names = vec!["id".to_string(), "doc".to_string()];

        let table = ColumnarTable::from_rows(&rows, &names).unwrap();

        // The inferred column type must be Blob (not String/text).
        match table.get_column("doc") {
            Some(ColumnData::Blob { .. }) => {}
            other => panic!("expected a Blob column for 'doc', got {other:?}"),
        }

        let out = table.to_rows();
        assert_eq!(out.len(), rows.len());
        for (i, (row, expected)) in out.iter().zip(blobs.iter()).enumerate() {
            match row.get(1) {
                Some(SqlValue::Blob(got)) => assert_eq!(
                    got, expected,
                    "blob bytes at row {i} must survive from_rows/to_rows byte-identically"
                ),
                other => panic!("row {i}: expected Blob, got {other:?}"),
            }
        }
    }
}
