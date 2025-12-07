// ============================================================================
// SqlValue Serialization
// ============================================================================
//
// Handles serialization and deserialization of SQL values with type tags.

use std::io::{Read, Write};

use vibesql_types::{Date, Interval, SqlValue, Time, Timestamp};

use super::{format::TypeTag, io::*};
use crate::StorageError;

pub fn write_sql_value<W: Write>(writer: &mut W, value: &SqlValue) -> Result<(), StorageError> {
    match value {
        SqlValue::Null => {
            writer
                .write_all(&[TypeTag::Null as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        SqlValue::Smallint(n) => {
            writer
                .write_all(&[TypeTag::Smallint as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_i16(writer, *n)?;
        }
        SqlValue::Integer(n) => {
            writer
                .write_all(&[TypeTag::Integer as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_i64(writer, *n)?;
        }
        SqlValue::Bigint(n) => {
            writer
                .write_all(&[TypeTag::Bigint as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_i64(writer, *n)?;
        }
        SqlValue::Unsigned(n) => {
            writer
                .write_all(&[TypeTag::Unsigned as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_u64(writer, *n)?;
        }
        SqlValue::Numeric(f) => {
            writer
                .write_all(&[TypeTag::Numeric as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_f64(writer, *f)?;
        }
        SqlValue::Float(f) => {
            writer
                .write_all(&[TypeTag::Float as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_f32(writer, *f)?;
        }
        SqlValue::Real(f) => {
            writer
                .write_all(&[TypeTag::Real as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_f32(writer, *f)?;
        }
        SqlValue::Double(f) => {
            writer
                .write_all(&[TypeTag::Double as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_f64(writer, *f)?;
        }
        SqlValue::Character(s) | SqlValue::Varchar(s) => {
            let tag = if matches!(value, SqlValue::Character(_)) {
                TypeTag::Character
            } else {
                TypeTag::Varchar
            };
            writer
                .write_all(&[tag as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, s)?;
        }
        SqlValue::Boolean(b) => {
            writer
                .write_all(&[TypeTag::Boolean as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_bool(writer, *b)?;
        }
        SqlValue::Date(d) => {
            writer
                .write_all(&[TypeTag::Date as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, &d.to_string())?;
        }
        SqlValue::Time(t) => {
            writer
                .write_all(&[TypeTag::Time as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, &t.to_string())?;
        }
        SqlValue::Timestamp(ts) => {
            writer
                .write_all(&[TypeTag::Timestamp as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, &ts.to_string())?;
        }
        SqlValue::Interval(i) => {
            writer
                .write_all(&[TypeTag::Interval as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, &i.to_string())?;
        }
        SqlValue::Vector(v) => {
            writer
                .write_all(&[TypeTag::Vector as u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            // Write vector length
            write_u32(writer, v.len() as u32)?;
            // Write each f32 value
            for &val in v.iter() {
                write_f32(writer, val)?;
            }
        }
    }
    Ok(())
}

pub fn read_sql_value<R: Read>(reader: &mut R) -> Result<SqlValue, StorageError> {
    let tag = read_u8(reader)?;
    let type_tag = TypeTag::from_u8(tag)?;

    match type_tag {
        TypeTag::Null => Ok(SqlValue::Null),
        TypeTag::Smallint => Ok(SqlValue::Smallint(read_i16(reader)?)),
        TypeTag::Integer => Ok(SqlValue::Integer(read_i64(reader)?)),
        TypeTag::Bigint => Ok(SqlValue::Bigint(read_i64(reader)?)),
        TypeTag::Unsigned => Ok(SqlValue::Unsigned(read_u64(reader)?)),
        TypeTag::Numeric => Ok(SqlValue::Numeric(read_f64(reader)?)),
        TypeTag::Float => Ok(SqlValue::Float(read_f32(reader)?)),
        TypeTag::Real => Ok(SqlValue::Real(read_f32(reader)?)),
        TypeTag::Double => Ok(SqlValue::Double(read_f64(reader)?)),
        TypeTag::Character => Ok(SqlValue::Character(arcstr::ArcStr::from(read_string(reader)?))),
        TypeTag::Varchar => Ok(SqlValue::Varchar(arcstr::ArcStr::from(read_string(reader)?))),
        TypeTag::Boolean => Ok(SqlValue::Boolean(read_bool(reader)?)),
        TypeTag::Date => {
            let s = read_string(reader)?;
            let date = s
                .parse::<Date>()
                .map_err(|e| StorageError::NotImplemented(format!("Invalid date: {}", e)))?;
            Ok(SqlValue::Date(date))
        }
        TypeTag::Time => {
            let s = read_string(reader)?;
            let time = s
                .parse::<Time>()
                .map_err(|e| StorageError::NotImplemented(format!("Invalid time: {}", e)))?;
            Ok(SqlValue::Time(time))
        }
        TypeTag::Timestamp => {
            let s = read_string(reader)?;
            let timestamp = s
                .parse::<Timestamp>()
                .map_err(|e| StorageError::NotImplemented(format!("Invalid timestamp: {}", e)))?;
            Ok(SqlValue::Timestamp(timestamp))
        }
        TypeTag::Interval => {
            let s = read_string(reader)?;
            let interval = s
                .parse::<Interval>()
                .map_err(|e| StorageError::NotImplemented(format!("Invalid interval: {}", e)))?;
            Ok(SqlValue::Interval(interval))
        }
        TypeTag::Vector => {
            let len = read_u32(reader)? as usize;
            let mut vec = Vec::with_capacity(len);
            for _ in 0..len {
                vec.push(read_f32(reader)?);
            }
            Ok(SqlValue::Vector(vec))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_value_roundtrip() {
        let test_values = vec![
            SqlValue::Null,
            SqlValue::Integer(42),
            SqlValue::Varchar(arcstr::ArcStr::from("test")),
            SqlValue::Boolean(true),
            SqlValue::Float(3.14),
        ];

        let mut buf = Vec::new();
        for val in &test_values {
            write_sql_value(&mut buf, val).unwrap();
        }

        let mut reader = &buf[..];
        for expected in test_values {
            let actual = read_sql_value(&mut reader).unwrap();
            assert_eq!(actual, expected);
        }
    }
}
