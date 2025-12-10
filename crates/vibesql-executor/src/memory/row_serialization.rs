//! Row serialization for disk spilling
//!
//! This module provides compact binary serialization for rows and sort keys,
//! optimized for external sort and aggregate operations.
//!
//! # Format
//!
//! Each row is serialized as:
//! ```text
//! [num_values: u16] [value1] [value2] ... [valueN]
//! ```
//!
//! Each value is serialized as:
//! ```text
//! [type_tag: u8] [data...]
//! ```
//!
//! # Design Decisions
//!
//! - **No length prefix for entire row**: We write rows sequentially and read them back
//!   in chunks, so we don't need random access within a run.
//! - **Compact type tags**: Single byte discriminant for common types
//! - **Little-endian**: Matches most modern hardware for zero-copy potential

use std::io::{self, Read, Write};
use vibesql_types::{Interval, SqlValue};

/// Type tags for serialized values
mod tags {
    pub const NULL: u8 = 0;
    pub const INTEGER: u8 = 1;
    pub const SMALLINT: u8 = 2;
    pub const BIGINT: u8 = 3;
    pub const UNSIGNED: u8 = 4;
    pub const NUMERIC: u8 = 5;
    pub const FLOAT: u8 = 6;
    pub const REAL: u8 = 7;
    pub const DOUBLE: u8 = 8;
    pub const BOOLEAN_FALSE: u8 = 9;
    pub const BOOLEAN_TRUE: u8 = 10;
    pub const CHARACTER: u8 = 11;
    pub const VARCHAR: u8 = 12;
    pub const DATE: u8 = 13;
    pub const TIME: u8 = 14;
    pub const TIMESTAMP: u8 = 15;
    pub const INTERVAL: u8 = 16;
    pub const VECTOR: u8 = 17;
}

/// Serialize a single SqlValue to the writer
pub fn serialize_value<W: Write>(value: &SqlValue, writer: &mut W) -> io::Result<()> {
    match value {
        SqlValue::Null => {
            writer.write_all(&[tags::NULL])?;
        }
        SqlValue::Integer(v) => {
            writer.write_all(&[tags::INTEGER])?;
            writer.write_all(&v.to_le_bytes())?;
        }
        SqlValue::Smallint(v) => {
            writer.write_all(&[tags::SMALLINT])?;
            writer.write_all(&v.to_le_bytes())?;
        }
        SqlValue::Bigint(v) => {
            writer.write_all(&[tags::BIGINT])?;
            writer.write_all(&v.to_le_bytes())?;
        }
        SqlValue::Unsigned(v) => {
            writer.write_all(&[tags::UNSIGNED])?;
            writer.write_all(&v.to_le_bytes())?;
        }
        SqlValue::Numeric(v) => {
            writer.write_all(&[tags::NUMERIC])?;
            writer.write_all(&v.to_le_bytes())?;
        }
        SqlValue::Float(v) => {
            writer.write_all(&[tags::FLOAT])?;
            writer.write_all(&v.to_le_bytes())?;
        }
        SqlValue::Real(v) => {
            writer.write_all(&[tags::REAL])?;
            writer.write_all(&v.to_le_bytes())?;
        }
        SqlValue::Double(v) => {
            writer.write_all(&[tags::DOUBLE])?;
            writer.write_all(&v.to_le_bytes())?;
        }
        SqlValue::Boolean(false) => {
            writer.write_all(&[tags::BOOLEAN_FALSE])?;
        }
        SqlValue::Boolean(true) => {
            writer.write_all(&[tags::BOOLEAN_TRUE])?;
        }
        SqlValue::Character(s) | SqlValue::Varchar(s) => {
            let tag = if matches!(value, SqlValue::Character(_)) {
                tags::CHARACTER
            } else {
                tags::VARCHAR
            };
            writer.write_all(&[tag])?;
            let bytes = s.as_bytes();
            let len = bytes.len() as u32;
            writer.write_all(&len.to_le_bytes())?;
            writer.write_all(bytes)?;
        }
        SqlValue::Date(d) => {
            writer.write_all(&[tags::DATE])?;
            // Serialize date components: year (i32), month (u8), day (u8)
            writer.write_all(&d.year.to_le_bytes())?;
            writer.write_all(&[d.month])?;
            writer.write_all(&[d.day])?;
        }
        SqlValue::Time(t) => {
            writer.write_all(&[tags::TIME])?;
            // Serialize time components: hour (u8), minute (u8), second (u8), nanosecond (u32)
            writer.write_all(&[t.hour])?;
            writer.write_all(&[t.minute])?;
            writer.write_all(&[t.second])?;
            writer.write_all(&t.nanosecond.to_le_bytes())?;
        }
        SqlValue::Timestamp(ts) => {
            writer.write_all(&[tags::TIMESTAMP])?;
            // Serialize date and time components
            writer.write_all(&ts.date.year.to_le_bytes())?;
            writer.write_all(&[ts.date.month])?;
            writer.write_all(&[ts.date.day])?;
            writer.write_all(&[ts.time.hour])?;
            writer.write_all(&[ts.time.minute])?;
            writer.write_all(&[ts.time.second])?;
            writer.write_all(&ts.time.nanosecond.to_le_bytes())?;
        }
        SqlValue::Interval(i) => {
            writer.write_all(&[tags::INTERVAL])?;
            // Serialize interval as string (the original string value preserves all info)
            let bytes = i.value.as_bytes();
            let len = bytes.len() as u32;
            writer.write_all(&len.to_le_bytes())?;
            writer.write_all(bytes)?;
        }
        SqlValue::Vector(v) => {
            writer.write_all(&[tags::VECTOR])?;
            let len = v.len() as u32;
            writer.write_all(&len.to_le_bytes())?;
            for f in v {
                writer.write_all(&f.to_le_bytes())?;
            }
        }
    }
    Ok(())
}

/// Deserialize a single SqlValue from the reader
pub fn deserialize_value<R: Read>(reader: &mut R) -> io::Result<SqlValue> {
    let mut tag = [0u8; 1];
    reader.read_exact(&mut tag)?;

    match tag[0] {
        tags::NULL => Ok(SqlValue::Null),
        tags::INTEGER => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            Ok(SqlValue::Integer(i64::from_le_bytes(buf)))
        }
        tags::SMALLINT => {
            let mut buf = [0u8; 2];
            reader.read_exact(&mut buf)?;
            Ok(SqlValue::Smallint(i16::from_le_bytes(buf)))
        }
        tags::BIGINT => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            Ok(SqlValue::Bigint(i64::from_le_bytes(buf)))
        }
        tags::UNSIGNED => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            Ok(SqlValue::Unsigned(u64::from_le_bytes(buf)))
        }
        tags::NUMERIC => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            Ok(SqlValue::Numeric(f64::from_le_bytes(buf)))
        }
        tags::FLOAT => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            Ok(SqlValue::Float(f32::from_le_bytes(buf)))
        }
        tags::REAL => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            Ok(SqlValue::Real(f32::from_le_bytes(buf)))
        }
        tags::DOUBLE => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            Ok(SqlValue::Double(f64::from_le_bytes(buf)))
        }
        tags::BOOLEAN_FALSE => Ok(SqlValue::Boolean(false)),
        tags::BOOLEAN_TRUE => Ok(SqlValue::Boolean(true)),
        tags::CHARACTER | tags::VARCHAR => {
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut str_buf = vec![0u8; len];
            reader.read_exact(&mut str_buf)?;

            let s = String::from_utf8(str_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            if tag[0] == tags::CHARACTER {
                Ok(SqlValue::Character(s.into()))
            } else {
                Ok(SqlValue::Varchar(s.into()))
            }
        }
        tags::DATE => {
            let mut year_buf = [0u8; 4];
            let mut month_buf = [0u8; 1];
            let mut day_buf = [0u8; 1];
            reader.read_exact(&mut year_buf)?;
            reader.read_exact(&mut month_buf)?;
            reader.read_exact(&mut day_buf)?;
            let year = i32::from_le_bytes(year_buf);
            let month = month_buf[0];
            let day = day_buf[0];
            Ok(SqlValue::Date(
                vibesql_types::Date::new(year, month, day)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            ))
        }
        tags::TIME => {
            let mut hour_buf = [0u8; 1];
            let mut minute_buf = [0u8; 1];
            let mut second_buf = [0u8; 1];
            let mut nano_buf = [0u8; 4];
            reader.read_exact(&mut hour_buf)?;
            reader.read_exact(&mut minute_buf)?;
            reader.read_exact(&mut second_buf)?;
            reader.read_exact(&mut nano_buf)?;
            Ok(SqlValue::Time(
                vibesql_types::Time::new(
                    hour_buf[0],
                    minute_buf[0],
                    second_buf[0],
                    u32::from_le_bytes(nano_buf),
                )
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            ))
        }
        tags::TIMESTAMP => {
            let mut year_buf = [0u8; 4];
            let mut month_buf = [0u8; 1];
            let mut day_buf = [0u8; 1];
            let mut hour_buf = [0u8; 1];
            let mut minute_buf = [0u8; 1];
            let mut second_buf = [0u8; 1];
            let mut nano_buf = [0u8; 4];
            reader.read_exact(&mut year_buf)?;
            reader.read_exact(&mut month_buf)?;
            reader.read_exact(&mut day_buf)?;
            reader.read_exact(&mut hour_buf)?;
            reader.read_exact(&mut minute_buf)?;
            reader.read_exact(&mut second_buf)?;
            reader.read_exact(&mut nano_buf)?;
            let date = vibesql_types::Date::new(
                i32::from_le_bytes(year_buf),
                month_buf[0],
                day_buf[0],
            )
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let time = vibesql_types::Time::new(
                hour_buf[0],
                minute_buf[0],
                second_buf[0],
                u32::from_le_bytes(nano_buf),
            )
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)))
        }
        tags::INTERVAL => {
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut str_buf = vec![0u8; len];
            reader.read_exact(&mut str_buf)?;

            let s = String::from_utf8(str_buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            Ok(SqlValue::Interval(Interval::new(s)))
        }
        tags::VECTOR => {
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut v = Vec::with_capacity(len);
            for _ in 0..len {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                v.push(f32::from_le_bytes(buf));
            }
            Ok(SqlValue::Vector(v))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown type tag: {}", tag[0]),
        )),
    }
}

/// Serialize a row to the writer
///
/// Format: [num_values: u16] [value1] [value2] ...
pub fn serialize_row<W: Write>(row: &vibesql_storage::Row, writer: &mut W) -> io::Result<()> {
    let num_values = row.values.len() as u16;
    writer.write_all(&num_values.to_le_bytes())?;

    for value in &row.values {
        serialize_value(value, writer)?;
    }

    Ok(())
}

/// Deserialize a row from the reader
pub fn deserialize_row<R: Read>(reader: &mut R) -> io::Result<vibesql_storage::Row> {
    let mut len_buf = [0u8; 2];
    reader.read_exact(&mut len_buf)?;
    let num_values = u16::from_le_bytes(len_buf) as usize;

    let mut values = Vec::with_capacity(num_values);
    for _ in 0..num_values {
        values.push(deserialize_value(reader)?);
    }

    Ok(vibesql_storage::Row::from_vec(values))
}

/// Serialize a row with its sort keys
///
/// Format: [row] [num_keys: u16] [key1_value] [key1_dir: u8] ...
pub fn serialize_row_with_keys<W: Write>(
    row: &vibesql_storage::Row,
    sort_keys: &[(SqlValue, vibesql_ast::OrderDirection)],
    writer: &mut W,
) -> io::Result<()> {
    serialize_row(row, writer)?;

    let num_keys = sort_keys.len() as u16;
    writer.write_all(&num_keys.to_le_bytes())?;

    for (value, direction) in sort_keys {
        serialize_value(value, writer)?;
        let dir_byte = match direction {
            vibesql_ast::OrderDirection::Asc => 0u8,
            vibesql_ast::OrderDirection::Desc => 1u8,
        };
        writer.write_all(&[dir_byte])?;
    }

    Ok(())
}

/// Deserialize a row with its sort keys
pub fn deserialize_row_with_keys<R: Read>(
    reader: &mut R,
) -> io::Result<(vibesql_storage::Row, Vec<(SqlValue, vibesql_ast::OrderDirection)>)> {
    let row = deserialize_row(reader)?;

    let mut len_buf = [0u8; 2];
    reader.read_exact(&mut len_buf)?;
    let num_keys = u16::from_le_bytes(len_buf) as usize;

    let mut keys = Vec::with_capacity(num_keys);
    for _ in 0..num_keys {
        let value = deserialize_value(reader)?;
        let mut dir_buf = [0u8; 1];
        reader.read_exact(&mut dir_buf)?;
        let direction = if dir_buf[0] == 0 {
            vibesql_ast::OrderDirection::Asc
        } else {
            vibesql_ast::OrderDirection::Desc
        };
        keys.push((value, direction));
    }

    Ok((row, keys))
}

/// Estimate the serialized size of a row in bytes
///
/// Used for memory budget tracking when deciding whether to spill.
pub fn estimate_serialized_size(row: &vibesql_storage::Row) -> usize {
    let mut size = 2; // num_values header

    for value in &row.values {
        size += estimate_value_size(value);
    }

    size
}

/// Estimate the serialized size of a value
fn estimate_value_size(value: &SqlValue) -> usize {
    match value {
        SqlValue::Null => 1,
        SqlValue::Integer(_) | SqlValue::Bigint(_) => 1 + 8,
        SqlValue::Smallint(_) => 1 + 2,
        SqlValue::Unsigned(_) | SqlValue::Numeric(_) | SqlValue::Double(_) => 1 + 8,
        SqlValue::Float(_) | SqlValue::Real(_) => 1 + 4,
        SqlValue::Boolean(_) => 1,
        SqlValue::Character(s) | SqlValue::Varchar(s) => 1 + 4 + s.len(),
        SqlValue::Date(_) => 1 + 4 + 1 + 1, // tag + year + month + day
        SqlValue::Time(_) => 1 + 1 + 1 + 1 + 4, // tag + hour + minute + second + nanosecond
        SqlValue::Timestamp(_) => 1 + 4 + 1 + 1 + 1 + 1 + 1 + 4, // tag + date + time components
        SqlValue::Interval(i) => 1 + 4 + i.value.len(), // tag + length + string
        SqlValue::Vector(v) => 1 + 4 + (v.len() * 4),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_roundtrip_null() {
        let value = SqlValue::Null;
        let mut buf = Vec::new();
        serialize_value(&value, &mut buf).unwrap();

        let mut reader = Cursor::new(buf);
        let result = deserialize_value(&mut reader).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_roundtrip_integer() {
        let value = SqlValue::Integer(12345);
        let mut buf = Vec::new();
        serialize_value(&value, &mut buf).unwrap();

        let mut reader = Cursor::new(buf);
        let result = deserialize_value(&mut reader).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_roundtrip_string() {
        let value = SqlValue::Varchar("hello world".into());
        let mut buf = Vec::new();
        serialize_value(&value, &mut buf).unwrap();

        let mut reader = Cursor::new(buf);
        let result = deserialize_value(&mut reader).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_roundtrip_date() {
        let value = SqlValue::Date(vibesql_types::Date::new(2024, 3, 15).unwrap());
        let mut buf = Vec::new();
        serialize_value(&value, &mut buf).unwrap();

        let mut reader = Cursor::new(buf);
        let result = deserialize_value(&mut reader).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_roundtrip_vector() {
        let vec_data = vec![1.0f32, 2.0f32, 3.0f32, 4.0f32];
        let value = SqlValue::Vector(vec_data.clone());
        let mut buf = Vec::new();
        serialize_value(&value, &mut buf).unwrap();

        let mut reader = Cursor::new(buf);
        let result = deserialize_value(&mut reader).unwrap();

        // SqlValue's PartialEq doesn't cover Vector, so compare manually
        match result {
            SqlValue::Vector(v) => assert_eq!(v, vec_data),
            _ => panic!("Expected Vector variant"),
        }
    }

    #[test]
    fn test_roundtrip_row() {
        let row = vibesql_storage::Row::from_vec(vec![
            SqlValue::Integer(42),
            SqlValue::Varchar("test".into()),
            SqlValue::Double(3.14),
            SqlValue::Null,
        ]);

        let mut buf = Vec::new();
        serialize_row(&row, &mut buf).unwrap();

        let mut reader = Cursor::new(buf);
        let result = deserialize_row(&mut reader).unwrap();
        assert_eq!(result, row);
    }

    #[test]
    fn test_roundtrip_row_with_keys() {
        let row = vibesql_storage::Row::from_vec(vec![
            SqlValue::Integer(42),
            SqlValue::Varchar("test".into()),
        ]);
        let keys = vec![
            (SqlValue::Integer(42), vibesql_ast::OrderDirection::Asc),
            (SqlValue::Varchar("test".into()), vibesql_ast::OrderDirection::Desc),
        ];

        let mut buf = Vec::new();
        serialize_row_with_keys(&row, &keys, &mut buf).unwrap();

        let mut reader = Cursor::new(buf);
        let (result_row, result_keys) = deserialize_row_with_keys(&mut reader).unwrap();
        assert_eq!(result_row, row);
        assert_eq!(result_keys.len(), keys.len());
        assert_eq!(result_keys[0].0, keys[0].0);
        assert_eq!(result_keys[1].0, keys[1].0);
    }

    #[test]
    fn test_size_estimation() {
        let row = vibesql_storage::Row::from_vec(vec![
            SqlValue::Integer(42),    // 1 + 8 = 9
            SqlValue::Varchar("hi".into()), // 1 + 4 + 2 = 7
        ]);

        let estimated = estimate_serialized_size(&row);
        let mut buf = Vec::new();
        serialize_row(&row, &mut buf).unwrap();

        // Estimated should be close to actual
        assert_eq!(estimated, 2 + 9 + 7); // header + values
        assert_eq!(buf.len(), estimated);
    }
}
