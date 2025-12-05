// ============================================================================
// Window Function Serialization
// ============================================================================
//
// Handles serialization of window functions, specs, frames, and bounds.

use super::super::io::*;
use crate::StorageError;
use std::io::{Read, Write};
use vibesql_ast::{FrameBound, FrameUnit, WindowFrame, WindowFunctionSpec, WindowSpec};

pub(super) fn write_window_function_spec<W: Write>(
    writer: &mut W,
    spec: &WindowFunctionSpec,
) -> Result<(), StorageError> {
    match spec {
        WindowFunctionSpec::Aggregate { name, args } => {
            writer
                .write_all(&[0u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, name)?;
            write_u32(writer, args.len() as u32)?;
            for arg in args {
                super::write_expression(writer, arg)?;
            }
        }
        WindowFunctionSpec::Ranking { name, args } => {
            writer
                .write_all(&[1u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, name)?;
            write_u32(writer, args.len() as u32)?;
            for arg in args {
                super::write_expression(writer, arg)?;
            }
        }
        WindowFunctionSpec::Value { name, args } => {
            writer
                .write_all(&[2u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            write_string(writer, name)?;
            write_u32(writer, args.len() as u32)?;
            for arg in args {
                super::write_expression(writer, arg)?;
            }
        }
    }
    Ok(())
}

pub(super) fn read_window_function_spec<R: Read>(
    reader: &mut R,
) -> Result<WindowFunctionSpec, StorageError> {
    let tag = read_u8(reader)?;
    match tag {
        0 => {
            let name = read_string(reader)?;
            let arg_count = read_u32(reader)?;
            let mut args = Vec::new();
            for _ in 0..arg_count {
                args.push(super::read_expression(reader)?);
            }
            Ok(WindowFunctionSpec::Aggregate { name, args })
        }
        1 => {
            let name = read_string(reader)?;
            let arg_count = read_u32(reader)?;
            let mut args = Vec::new();
            for _ in 0..arg_count {
                args.push(super::read_expression(reader)?);
            }
            Ok(WindowFunctionSpec::Ranking { name, args })
        }
        2 => {
            let name = read_string(reader)?;
            let arg_count = read_u32(reader)?;
            let mut args = Vec::new();
            for _ in 0..arg_count {
                args.push(super::read_expression(reader)?);
            }
            Ok(WindowFunctionSpec::Value { name, args })
        }
        _ => {
            Err(StorageError::NotImplemented(format!("Unknown window function spec tag: {}", tag)))
        }
    }
}

pub(super) fn write_window_spec<W: Write>(
    writer: &mut W,
    spec: &WindowSpec,
) -> Result<(), StorageError> {
    // Write partition_by
    write_bool(writer, spec.partition_by.is_some())?;
    if let Some(partition_exprs) = &spec.partition_by {
        write_u32(writer, partition_exprs.len() as u32)?;
        for expr in partition_exprs {
            super::write_expression(writer, expr)?;
        }
    }

    // Write order_by - note: OrderByItem serialization not implemented yet
    // For now, we'll serialize as empty (this is a limitation)
    write_bool(writer, false)?;

    // Write frame
    write_bool(writer, spec.frame.is_some())?;
    if let Some(frame) = &spec.frame {
        write_window_frame(writer, frame)?;
    }

    Ok(())
}

pub(super) fn read_window_spec<R: Read>(reader: &mut R) -> Result<WindowSpec, StorageError> {
    // Read partition_by
    let has_partition_by = read_bool(reader)?;
    let partition_by = if has_partition_by {
        let count = read_u32(reader)?;
        let mut exprs = Vec::new();
        for _ in 0..count {
            exprs.push(super::read_expression(reader)?);
        }
        Some(exprs)
    } else {
        None
    };

    // Read order_by
    let has_order_by = read_bool(reader)?;
    if has_order_by {
        return Err(StorageError::NotImplemented(
            "OrderBy in window spec not yet supported".to_string(),
        ));
    }

    // Read frame
    let has_frame = read_bool(reader)?;
    let frame = if has_frame { Some(read_window_frame(reader)?) } else { None };

    Ok(WindowSpec { partition_by, order_by: None, frame })
}

fn write_window_frame<W: Write>(writer: &mut W, frame: &WindowFrame) -> Result<(), StorageError> {
    // Write unit
    let unit_tag = match frame.unit {
        FrameUnit::Rows => 0u8,
        FrameUnit::Range => 1,
    };
    writer
        .write_all(&[unit_tag])
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Write start bound
    write_frame_bound(writer, &frame.start)?;

    // Write end bound
    write_bool(writer, frame.end.is_some())?;
    if let Some(end) = &frame.end {
        write_frame_bound(writer, end)?;
    }

    Ok(())
}

fn read_window_frame<R: Read>(reader: &mut R) -> Result<WindowFrame, StorageError> {
    // Read unit
    let unit_tag = read_u8(reader)?;
    let unit = match unit_tag {
        0 => FrameUnit::Rows,
        1 => FrameUnit::Range,
        _ => {
            return Err(StorageError::NotImplemented(format!(
                "Unknown frame unit tag: {}",
                unit_tag
            )))
        }
    };

    // Read start bound
    let start = read_frame_bound(reader)?;

    // Read end bound
    let has_end = read_bool(reader)?;
    let end = if has_end { Some(read_frame_bound(reader)?) } else { None };

    Ok(WindowFrame { unit, start, end })
}

fn write_frame_bound<W: Write>(writer: &mut W, bound: &FrameBound) -> Result<(), StorageError> {
    match bound {
        FrameBound::UnboundedPreceding => {
            writer
                .write_all(&[0u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        FrameBound::Preceding(expr) => {
            writer
                .write_all(&[1u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            super::write_expression(writer, expr)?;
        }
        FrameBound::CurrentRow => {
            writer
                .write_all(&[2u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        FrameBound::Following(expr) => {
            writer
                .write_all(&[3u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            super::write_expression(writer, expr)?;
        }
        FrameBound::UnboundedFollowing => {
            writer
                .write_all(&[4u8])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
    }
    Ok(())
}

fn read_frame_bound<R: Read>(reader: &mut R) -> Result<FrameBound, StorageError> {
    let tag = read_u8(reader)?;
    match tag {
        0 => Ok(FrameBound::UnboundedPreceding),
        1 => {
            let expr = Box::new(super::read_expression(reader)?);
            Ok(FrameBound::Preceding(expr))
        }
        2 => Ok(FrameBound::CurrentRow),
        3 => {
            let expr = Box::new(super::read_expression(reader)?);
            Ok(FrameBound::Following(expr))
        }
        4 => Ok(FrameBound::UnboundedFollowing),
        _ => Err(StorageError::NotImplemented(format!("Unknown frame bound tag: {}", tag))),
    }
}
