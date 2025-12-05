//! DataType Serialization Utilities
//!
//! This module provides serialization and deserialization functions for DataType
//! used in B+ tree metadata persistence.

use vibesql_types::{DataType, IntervalField};

use crate::StorageError;

/// Serialize a DataType to bytes
///
/// Returns the number of bytes written
pub(crate) fn serialize_datatype(
    data_type: &DataType,
    buffer: &mut [u8],
) -> Result<usize, StorageError> {
    let offset = 0;

    let bytes_written = match data_type {
        DataType::Integer => {
            buffer[offset] = 1;
            1
        }
        DataType::Smallint => {
            buffer[offset] = 2;
            1
        }
        DataType::Bigint => {
            buffer[offset] = 3;
            1
        }
        DataType::Unsigned => {
            buffer[offset] = 4;
            1
        }
        DataType::Numeric { precision, scale } => {
            buffer[offset] = 5;
            buffer[offset + 1] = *precision;
            buffer[offset + 2] = *scale;
            3
        }
        DataType::Decimal { precision, scale } => {
            buffer[offset] = 6;
            buffer[offset + 1] = *precision;
            buffer[offset + 2] = *scale;
            3
        }
        DataType::Float { precision } => {
            buffer[offset] = 7;
            buffer[offset + 1] = *precision;
            2
        }
        DataType::Real => {
            buffer[offset] = 8;
            1
        }
        DataType::DoublePrecision => {
            buffer[offset] = 9;
            1
        }
        DataType::Character { length } => {
            buffer[offset] = 10;
            buffer[offset + 1..offset + 3].copy_from_slice(&(*length as u16).to_le_bytes());
            3
        }
        DataType::Varchar { max_length } => {
            buffer[offset] = 11;
            let len = max_length.unwrap_or(255) as u16;
            buffer[offset + 1..offset + 3].copy_from_slice(&len.to_le_bytes());
            3
        }
        DataType::CharacterLargeObject => {
            buffer[offset] = 12;
            1
        }
        DataType::Name => {
            buffer[offset] = 13;
            1
        }
        DataType::Boolean => {
            buffer[offset] = 14;
            1
        }
        DataType::Date => {
            buffer[offset] = 15;
            1
        }
        DataType::Time { with_timezone } => {
            buffer[offset] = 16;
            buffer[offset + 1] = if *with_timezone { 1 } else { 0 };
            2
        }
        DataType::Timestamp { with_timezone } => {
            buffer[offset] = 17;
            buffer[offset + 1] = if *with_timezone { 1 } else { 0 };
            2
        }
        DataType::Interval { start_field, end_field } => {
            buffer[offset] = 18;
            buffer[offset + 1] = interval_field_to_u8(start_field);
            buffer[offset + 2] = end_field.as_ref().map(interval_field_to_u8).unwrap_or(255);
            3
        }
        DataType::BinaryLargeObject => {
            buffer[offset] = 19;
            1
        }
        DataType::Bit { length } => {
            buffer[offset] = 22;
            let len = length.unwrap_or(1) as u16;
            buffer[offset + 1..offset + 3].copy_from_slice(&len.to_le_bytes());
            3
        }
        DataType::UserDefined { type_name } => {
            buffer[offset] = 20;
            let name_bytes = type_name.as_bytes();
            let len = name_bytes.len().min(255);
            buffer[offset + 1] = len as u8;
            buffer[offset + 2..offset + 2 + len].copy_from_slice(&name_bytes[..len]);
            2 + len
        }
        DataType::Vector { dimensions } => {
            buffer[offset] = 23;
            buffer[offset + 1..offset + 5].copy_from_slice(&dimensions.to_le_bytes());
            5
        }
        DataType::Null => {
            buffer[offset] = 21;
            1
        }
    };

    Ok(bytes_written)
}

/// Deserialize a DataType from bytes
///
/// Returns (DataType, bytes_read)
pub(crate) fn deserialize_datatype(buffer: &[u8]) -> Result<(DataType, usize), StorageError> {
    let type_tag = buffer[0];
    let mut offset = 1;

    let data_type = match type_tag {
        1 => DataType::Integer,
        2 => DataType::Smallint,
        3 => DataType::Bigint,
        4 => DataType::Unsigned,
        5 => {
            let precision = buffer[offset];
            let scale = buffer[offset + 1];
            offset += 2;
            DataType::Numeric { precision, scale }
        }
        6 => {
            let precision = buffer[offset];
            let scale = buffer[offset + 1];
            offset += 2;
            DataType::Decimal { precision, scale }
        }
        7 => {
            let precision = buffer[offset];
            offset += 1;
            DataType::Float { precision }
        }
        8 => DataType::Real,
        9 => DataType::DoublePrecision,
        10 => {
            let len_bytes: [u8; 2] = buffer[offset..offset + 2].try_into().unwrap();
            let length = u16::from_le_bytes(len_bytes) as usize;
            offset += 2;
            DataType::Character { length }
        }
        11 => {
            let len_bytes: [u8; 2] = buffer[offset..offset + 2].try_into().unwrap();
            let max_length = u16::from_le_bytes(len_bytes);
            offset += 2;
            DataType::Varchar { max_length: Some(max_length as usize) }
        }
        12 => DataType::CharacterLargeObject,
        13 => DataType::Name,
        14 => DataType::Boolean,
        15 => DataType::Date,
        16 => {
            let with_timezone = buffer[offset] != 0;
            offset += 1;
            DataType::Time { with_timezone }
        }
        17 => {
            let with_timezone = buffer[offset] != 0;
            offset += 1;
            DataType::Timestamp { with_timezone }
        }
        18 => {
            let start_field = u8_to_interval_field(buffer[offset]);
            let end_field_u8 = buffer[offset + 1];
            let end_field =
                if end_field_u8 == 255 { None } else { Some(u8_to_interval_field(end_field_u8)) };
            offset += 2;
            DataType::Interval { start_field, end_field }
        }
        19 => DataType::BinaryLargeObject,
        22 => {
            let len_bytes: [u8; 2] = buffer[offset..offset + 2].try_into().unwrap();
            let length_value = u16::from_le_bytes(len_bytes) as usize;
            offset += 2;
            DataType::Bit { length: Some(length_value) }
        }
        20 => {
            let len = buffer[offset] as usize;
            offset += 1;
            let type_name = String::from_utf8_lossy(&buffer[offset..offset + len]).to_string();
            offset += len;
            DataType::UserDefined { type_name }
        }
        23 => {
            let dim_bytes: [u8; 4] = buffer[offset..offset + 4].try_into().unwrap();
            let dimensions = u32::from_le_bytes(dim_bytes);
            offset += 4;
            DataType::Vector { dimensions }
        }
        21 => DataType::Null,
        _ => return Err(StorageError::IoError(format!("Invalid DataType tag: {}", type_tag))),
    };

    Ok((data_type, offset))
}

/// Convert IntervalField to u8 for serialization
fn interval_field_to_u8(field: &IntervalField) -> u8 {
    match field {
        IntervalField::Year => 0,
        IntervalField::Month => 1,
        IntervalField::Day => 2,
        IntervalField::Hour => 3,
        IntervalField::Minute => 4,
        IntervalField::Second => 5,
    }
}

/// Convert u8 to IntervalField for deserialization
fn u8_to_interval_field(value: u8) -> IntervalField {
    match value {
        0 => IntervalField::Year,
        1 => IntervalField::Month,
        2 => IntervalField::Day,
        3 => IntervalField::Hour,
        4 => IntervalField::Minute,
        5 => IntervalField::Second,
        _ => IntervalField::Year, // Default fallback
    }
}
