// ============================================================================
// Expression Serialization Module
// ============================================================================
//
// This module provides modular expression serialization broken down into:
// - macros: Declarative macros for reducing boilerplate
// - operators: Binary and unary operator serialization
// - types: SQL type serialization (CharacterUnit, TrimPosition, etc.)
// - window: Window function, spec, and frame serialization
// - case: CASE WHEN expression serialization

#[macro_use]
mod macros;
mod case;
mod operators;
mod types;
mod window;

use crate::StorageError;
use std::io::{Read, Write};
use vibesql_ast::Expression;

// Import helper functions from submodules
use case::{read_case_when, write_case_when};
use operators::{
    read_binary_operator, read_unary_operator, write_binary_operator, write_unary_operator,
};
use types::{
    read_character_unit, read_fulltext_mode, read_interval_unit, read_pseudo_table,
    read_trim_position, write_character_unit, write_fulltext_mode, write_interval_unit,
    write_pseudo_table, write_trim_position,
};
use window::{
    read_window_function_spec, read_window_spec, write_window_function_spec, write_window_spec,
};

use super::{io::*, value::*};

/// Expression variant tags for serialization
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprTag {
    Literal = 0x00,
    ColumnRef = 0x01,
    BinaryOp = 0x02,
    UnaryOp = 0x03,
    Function = 0x04,
    AggregateFunction = 0x05,
    IsNull = 0x06,
    Wildcard = 0x07,
    Case = 0x08,
    ScalarSubquery = 0x09,
    In = 0x0A,
    InList = 0x0B,
    Between = 0x0C,
    Cast = 0x0D,
    Position = 0x0E,
    Trim = 0x0F,
    Like = 0x10,
    Exists = 0x11,
    QuantifiedComparison = 0x12,
    CurrentDate = 0x13,
    CurrentTime = 0x14,
    CurrentTimestamp = 0x15,
    Interval = 0x16,
    Default = 0x17,
    DuplicateKeyValue = 0x18,
    WindowFunction = 0x19,
    NextValue = 0x1A,
    MatchAgainst = 0x1B,
    PseudoVariable = 0x1C,
    SessionVariable = 0x1D,
    Extract = 0x1E,
    Conjunction = 0x1F,
    Disjunction = 0x20,
}

impl ExprTag {
    fn from_u8(tag: u8) -> Result<Self, StorageError> {
        match tag {
            0x00 => Ok(ExprTag::Literal),
            0x01 => Ok(ExprTag::ColumnRef),
            0x02 => Ok(ExprTag::BinaryOp),
            0x03 => Ok(ExprTag::UnaryOp),
            0x04 => Ok(ExprTag::Function),
            0x05 => Ok(ExprTag::AggregateFunction),
            0x06 => Ok(ExprTag::IsNull),
            0x07 => Ok(ExprTag::Wildcard),
            0x08 => Ok(ExprTag::Case),
            0x09 => Ok(ExprTag::ScalarSubquery),
            0x0A => Ok(ExprTag::In),
            0x0B => Ok(ExprTag::InList),
            0x0C => Ok(ExprTag::Between),
            0x0D => Ok(ExprTag::Cast),
            0x0E => Ok(ExprTag::Position),
            0x0F => Ok(ExprTag::Trim),
            0x10 => Ok(ExprTag::Like),
            0x11 => Ok(ExprTag::Exists),
            0x12 => Ok(ExprTag::QuantifiedComparison),
            0x13 => Ok(ExprTag::CurrentDate),
            0x14 => Ok(ExprTag::CurrentTime),
            0x15 => Ok(ExprTag::CurrentTimestamp),
            0x16 => Ok(ExprTag::Interval),
            0x17 => Ok(ExprTag::Default),
            0x18 => Ok(ExprTag::DuplicateKeyValue),
            0x19 => Ok(ExprTag::WindowFunction),
            0x1A => Ok(ExprTag::NextValue),
            0x1B => Ok(ExprTag::MatchAgainst),
            0x1C => Ok(ExprTag::PseudoVariable),
            0x1D => Ok(ExprTag::SessionVariable),
            0x1E => Ok(ExprTag::Extract),
            0x1F => Ok(ExprTag::Conjunction),
            0x20 => Ok(ExprTag::Disjunction),
            _ => {
                Err(StorageError::NotImplemented(format!("Unknown expression tag: 0x{:02X}", tag)))
            }
        }
    }
}

// Helper macro to reduce write_all boilerplate
macro_rules! write_tag {
    ($writer:expr, $tag:expr) => {
        $writer
            .write_all(&[$tag as u8])
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?
    };
}

pub fn write_expression<W: Write>(writer: &mut W, expr: &Expression) -> Result<(), StorageError> {
    match expr {
        Expression::Literal(value) => {
            write_tag!(writer, ExprTag::Literal);
            write_sql_value(writer, value)?;
        }
        Expression::ColumnRef { table, column } => {
            write_tag!(writer, ExprTag::ColumnRef);
            write_bool(writer, table.is_some())?;
            if let Some(t) = table {
                write_string(writer, t)?;
            }
            write_string(writer, column)?;
        }
        Expression::BinaryOp { op, left, right } => {
            write_tag!(writer, ExprTag::BinaryOp);
            write_binary_operator(writer, op)?;
            write_expression(writer, left)?;
            write_expression(writer, right)?;
        }
        Expression::UnaryOp { op, expr } => {
            write_tag!(writer, ExprTag::UnaryOp);
            write_unary_operator(writer, op)?;
            write_expression(writer, expr)?;
        }
        Expression::Function { name, args, character_unit } => {
            write_tag!(writer, ExprTag::Function);
            write_string(writer, name)?;
            write_u32(writer, args.len() as u32)?;
            for arg in args {
                write_expression(writer, arg)?;
            }
            write_bool(writer, character_unit.is_some())?;
            if let Some(unit) = character_unit {
                write_character_unit(writer, unit)?;
            }
        }
        Expression::AggregateFunction { name, distinct, args } => {
            write_tag!(writer, ExprTag::AggregateFunction);
            write_string(writer, name)?;
            write_bool(writer, *distinct)?;
            write_u32(writer, args.len() as u32)?;
            for arg in args {
                write_expression(writer, arg)?;
            }
        }
        Expression::IsNull { expr, negated } => {
            write_tag!(writer, ExprTag::IsNull);
            write_expression(writer, expr)?;
            write_bool(writer, *negated)?;
        }
        Expression::Wildcard => {
            write_tag!(writer, ExprTag::Wildcard);
        }
        Expression::Case { operand, when_clauses, else_result } => {
            write_tag!(writer, ExprTag::Case);
            write_bool(writer, operand.is_some())?;
            if let Some(op) = operand {
                write_expression(writer, op)?;
            }
            write_u32(writer, when_clauses.len() as u32)?;
            for when_clause in when_clauses {
                write_case_when(writer, when_clause)?;
            }
            write_bool(writer, else_result.is_some())?;
            if let Some(else_expr) = else_result {
                write_expression(writer, else_expr)?;
            }
        }
        Expression::ScalarSubquery(_) => {
            return Err(StorageError::NotImplemented(
                "ScalarSubquery serialization not yet implemented".to_string(),
            ));
        }
        Expression::In { .. } => {
            return Err(StorageError::NotImplemented(
                "IN with subquery serialization not yet implemented".to_string(),
            ));
        }
        Expression::InList { expr, values, negated } => {
            write_tag!(writer, ExprTag::InList);
            write_expression(writer, expr)?;
            write_u32(writer, values.len() as u32)?;
            for value in values {
                write_expression(writer, value)?;
            }
            write_bool(writer, *negated)?;
        }
        Expression::Between { expr, low, high, negated, symmetric } => {
            write_tag!(writer, ExprTag::Between);
            write_expression(writer, expr)?;
            write_expression(writer, low)?;
            write_expression(writer, high)?;
            write_bool(writer, *negated)?;
            write_bool(writer, *symmetric)?;
        }
        Expression::Cast { expr, data_type } => {
            write_tag!(writer, ExprTag::Cast);
            write_expression(writer, expr)?;
            let type_str = crate::persistence::save::format_data_type(data_type);
            write_string(writer, &type_str)?;
        }
        Expression::Position { substring, string, character_unit } => {
            write_tag!(writer, ExprTag::Position);
            write_expression(writer, substring)?;
            write_expression(writer, string)?;
            write_bool(writer, character_unit.is_some())?;
            if let Some(unit) = character_unit {
                write_character_unit(writer, unit)?;
            }
        }
        Expression::Trim { position, removal_char, string } => {
            write_tag!(writer, ExprTag::Trim);
            write_bool(writer, position.is_some())?;
            if let Some(pos) = position {
                write_trim_position(writer, pos)?;
            }
            write_bool(writer, removal_char.is_some())?;
            if let Some(char_expr) = removal_char {
                write_expression(writer, char_expr)?;
            }
            write_expression(writer, string)?;
        }
        Expression::Like { expr, pattern, negated } => {
            write_tag!(writer, ExprTag::Like);
            write_expression(writer, expr)?;
            write_expression(writer, pattern)?;
            write_bool(writer, *negated)?;
        }
        Expression::Exists { .. } => {
            return Err(StorageError::NotImplemented(
                "EXISTS serialization not yet implemented".to_string(),
            ));
        }
        Expression::QuantifiedComparison { .. } => {
            return Err(StorageError::NotImplemented(
                "Quantified comparison serialization not yet implemented".to_string(),
            ));
        }
        Expression::CurrentDate => {
            write_tag!(writer, ExprTag::CurrentDate);
        }
        Expression::CurrentTime { precision } => {
            write_tag!(writer, ExprTag::CurrentTime);
            write_bool(writer, precision.is_some())?;
            if let Some(p) = precision {
                write_u32(writer, *p)?;
            }
        }
        Expression::CurrentTimestamp { precision } => {
            write_tag!(writer, ExprTag::CurrentTimestamp);
            write_bool(writer, precision.is_some())?;
            if let Some(p) = precision {
                write_u32(writer, *p)?;
            }
        }
        Expression::Interval { value, unit, leading_precision, fractional_precision } => {
            write_tag!(writer, ExprTag::Interval);
            write_expression(writer, value)?;
            write_interval_unit(writer, unit)?;
            write_bool(writer, leading_precision.is_some())?;
            if let Some(p) = leading_precision {
                write_u32(writer, *p)?;
            }
            write_bool(writer, fractional_precision.is_some())?;
            if let Some(p) = fractional_precision {
                write_u32(writer, *p)?;
            }
        }
        Expression::Default => {
            write_tag!(writer, ExprTag::Default);
        }
        Expression::DuplicateKeyValue { column } => {
            write_tag!(writer, ExprTag::DuplicateKeyValue);
            write_string(writer, column)?;
        }
        Expression::WindowFunction { function, over } => {
            write_tag!(writer, ExprTag::WindowFunction);
            write_window_function_spec(writer, function)?;
            write_window_spec(writer, over)?;
        }
        Expression::NextValue { sequence_name } => {
            write_tag!(writer, ExprTag::NextValue);
            write_string(writer, sequence_name)?;
        }
        Expression::MatchAgainst { columns, search_modifier, mode } => {
            write_tag!(writer, ExprTag::MatchAgainst);
            write_u32(writer, columns.len() as u32)?;
            for col in columns {
                write_string(writer, col)?;
            }
            write_expression(writer, search_modifier)?;
            write_fulltext_mode(writer, mode)?;
        }
        Expression::PseudoVariable { pseudo_table, column } => {
            write_tag!(writer, ExprTag::PseudoVariable);
            write_pseudo_table(writer, pseudo_table)?;
            write_string(writer, column)?;
        }
        Expression::SessionVariable { name } => {
            write_tag!(writer, ExprTag::SessionVariable);
            write_string(writer, name)?;
        }
        Expression::Extract { field, expr } => {
            write_tag!(writer, ExprTag::Extract);
            write_interval_unit(writer, field)?;
            write_expression(writer, expr)?;
        }
        Expression::Placeholder(_) => {
            // Placeholders should be bound to values before storage serialization
            return Err(StorageError::NotImplemented(
                "Placeholder expressions must be bound to values before serialization".to_string(),
            ));
        }
        Expression::NumberedPlaceholder(_) => {
            // Numbered placeholders should be bound to values before storage serialization
            return Err(StorageError::NotImplemented(
                "NumberedPlaceholder expressions must be bound to values before serialization"
                    .to_string(),
            ));
        }
        Expression::NamedPlaceholder(_) => {
            // Named placeholders should be bound to values before storage serialization
            return Err(StorageError::NotImplemented(
                "NamedPlaceholder expressions must be bound to values before serialization"
                    .to_string(),
            ));
        }
        Expression::Conjunction(children) => {
            write_tag!(writer, ExprTag::Conjunction);
            write_u32(writer, children.len() as u32)?;
            for child in children {
                write_expression(writer, child)?;
            }
        }
        Expression::Disjunction(children) => {
            write_tag!(writer, ExprTag::Disjunction);
            write_u32(writer, children.len() as u32)?;
            for child in children {
                write_expression(writer, child)?;
            }
        }
    }
    Ok(())
}

pub fn read_expression<R: Read>(reader: &mut R) -> Result<Expression, StorageError> {
    let tag = read_u8(reader)?;
    let expr_tag = ExprTag::from_u8(tag)?;

    match expr_tag {
        ExprTag::Literal => {
            let value = read_sql_value(reader)?;
            Ok(Expression::Literal(value))
        }
        ExprTag::ColumnRef => {
            let has_table = read_bool(reader)?;
            let table = if has_table { Some(read_string(reader)?) } else { None };
            let column = read_string(reader)?;
            Ok(Expression::ColumnRef { table, column })
        }
        ExprTag::BinaryOp => {
            let op = read_binary_operator(reader)?;
            let left = Box::new(read_expression(reader)?);
            let right = Box::new(read_expression(reader)?);
            Ok(Expression::BinaryOp { op, left, right })
        }
        ExprTag::UnaryOp => {
            let op = read_unary_operator(reader)?;
            let expr = Box::new(read_expression(reader)?);
            Ok(Expression::UnaryOp { op, expr })
        }
        ExprTag::Function => {
            let name = read_string(reader)?;
            let arg_count = read_u32(reader)?;
            let mut args = Vec::new();
            for _ in 0..arg_count {
                args.push(read_expression(reader)?);
            }
            let has_unit = read_bool(reader)?;
            let character_unit = if has_unit { Some(read_character_unit(reader)?) } else { None };
            Ok(Expression::Function { name, args, character_unit })
        }
        ExprTag::AggregateFunction => {
            let name = read_string(reader)?;
            let distinct = read_bool(reader)?;
            let arg_count = read_u32(reader)?;
            let mut args = Vec::new();
            for _ in 0..arg_count {
                args.push(read_expression(reader)?);
            }
            Ok(Expression::AggregateFunction { name, distinct, args })
        }
        ExprTag::IsNull => {
            let expr = Box::new(read_expression(reader)?);
            let negated = read_bool(reader)?;
            Ok(Expression::IsNull { expr, negated })
        }
        ExprTag::Wildcard => Ok(Expression::Wildcard),
        ExprTag::Case => {
            let has_operand = read_bool(reader)?;
            let operand = if has_operand { Some(Box::new(read_expression(reader)?)) } else { None };
            let when_count = read_u32(reader)?;
            let mut when_clauses = Vec::new();
            for _ in 0..when_count {
                when_clauses.push(read_case_when(reader)?);
            }
            let has_else = read_bool(reader)?;
            let else_result =
                if has_else { Some(Box::new(read_expression(reader)?)) } else { None };
            Ok(Expression::Case { operand, when_clauses, else_result })
        }
        ExprTag::ScalarSubquery => Err(StorageError::NotImplemented(
            "ScalarSubquery deserialization not yet implemented".to_string(),
        )),
        ExprTag::In => Err(StorageError::NotImplemented(
            "IN with subquery deserialization not yet implemented".to_string(),
        )),
        ExprTag::InList => {
            let expr = Box::new(read_expression(reader)?);
            let value_count = read_u32(reader)?;
            let mut values = Vec::new();
            for _ in 0..value_count {
                values.push(read_expression(reader)?);
            }
            let negated = read_bool(reader)?;
            Ok(Expression::InList { expr, values, negated })
        }
        ExprTag::Between => {
            let expr = Box::new(read_expression(reader)?);
            let low = Box::new(read_expression(reader)?);
            let high = Box::new(read_expression(reader)?);
            let negated = read_bool(reader)?;
            let symmetric = read_bool(reader)?;
            Ok(Expression::Between { expr, low, high, negated, symmetric })
        }
        ExprTag::Cast => {
            let expr = Box::new(read_expression(reader)?);
            let type_str = read_string(reader)?;
            let data_type = super::catalog::parse_data_type(&type_str)?;
            Ok(Expression::Cast { expr, data_type })
        }
        ExprTag::Position => {
            let substring = Box::new(read_expression(reader)?);
            let string = Box::new(read_expression(reader)?);
            let has_unit = read_bool(reader)?;
            let character_unit = if has_unit { Some(read_character_unit(reader)?) } else { None };
            Ok(Expression::Position { substring, string, character_unit })
        }
        ExprTag::Trim => {
            let has_position = read_bool(reader)?;
            let position = if has_position { Some(read_trim_position(reader)?) } else { None };
            let has_removal_char = read_bool(reader)?;
            let removal_char =
                if has_removal_char { Some(Box::new(read_expression(reader)?)) } else { None };
            let string = Box::new(read_expression(reader)?);
            Ok(Expression::Trim { position, removal_char, string })
        }
        ExprTag::Like => {
            let expr = Box::new(read_expression(reader)?);
            let pattern = Box::new(read_expression(reader)?);
            let negated = read_bool(reader)?;
            Ok(Expression::Like { expr, pattern, negated })
        }
        ExprTag::Exists => Err(StorageError::NotImplemented(
            "EXISTS deserialization not yet implemented".to_string(),
        )),
        ExprTag::QuantifiedComparison => Err(StorageError::NotImplemented(
            "Quantified comparison deserialization not yet implemented".to_string(),
        )),
        ExprTag::CurrentDate => Ok(Expression::CurrentDate),
        ExprTag::CurrentTime => {
            let has_precision = read_bool(reader)?;
            let precision = if has_precision { Some(read_u32(reader)?) } else { None };
            Ok(Expression::CurrentTime { precision })
        }
        ExprTag::CurrentTimestamp => {
            let has_precision = read_bool(reader)?;
            let precision = if has_precision { Some(read_u32(reader)?) } else { None };
            Ok(Expression::CurrentTimestamp { precision })
        }
        ExprTag::Interval => {
            let value = Box::new(read_expression(reader)?);
            let unit = read_interval_unit(reader)?;
            let has_leading_precision = read_bool(reader)?;
            let leading_precision =
                if has_leading_precision { Some(read_u32(reader)?) } else { None };
            let has_fractional_precision = read_bool(reader)?;
            let fractional_precision =
                if has_fractional_precision { Some(read_u32(reader)?) } else { None };
            Ok(Expression::Interval { value, unit, leading_precision, fractional_precision })
        }
        ExprTag::Default => Ok(Expression::Default),
        ExprTag::DuplicateKeyValue => {
            let column = read_string(reader)?;
            Ok(Expression::DuplicateKeyValue { column })
        }
        ExprTag::WindowFunction => {
            let function = read_window_function_spec(reader)?;
            let over = read_window_spec(reader)?;
            Ok(Expression::WindowFunction { function, over })
        }
        ExprTag::NextValue => {
            let sequence_name = read_string(reader)?;
            Ok(Expression::NextValue { sequence_name })
        }
        ExprTag::MatchAgainst => {
            let col_count = read_u32(reader)?;
            let mut columns = Vec::new();
            for _ in 0..col_count {
                columns.push(read_string(reader)?);
            }
            let search_modifier = Box::new(read_expression(reader)?);
            let mode = read_fulltext_mode(reader)?;
            Ok(Expression::MatchAgainst { columns, search_modifier, mode })
        }
        ExprTag::PseudoVariable => {
            let pseudo_table = read_pseudo_table(reader)?;
            let column = read_string(reader)?;
            Ok(Expression::PseudoVariable { pseudo_table, column })
        }
        ExprTag::SessionVariable => {
            let name = read_string(reader)?;
            Ok(Expression::SessionVariable { name })
        }
        ExprTag::Extract => {
            let field = read_interval_unit(reader)?;
            let expr = Box::new(read_expression(reader)?);
            Ok(Expression::Extract { field, expr })
        }
        ExprTag::Conjunction => {
            let count = read_u32(reader)?;
            let mut children = Vec::with_capacity(count as usize);
            for _ in 0..count {
                children.push(read_expression(reader)?);
            }
            Ok(Expression::Conjunction(children))
        }
        ExprTag::Disjunction => {
            let count = read_u32(reader)?;
            let mut children = Vec::with_capacity(count as usize);
            for _ in 0..count {
                children.push(read_expression(reader)?);
            }
            Ok(Expression::Disjunction(children))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{BinaryOperator, CaseWhen};
    use vibesql_types::SqlValue;

    #[test]
    fn test_literal_roundtrip() {
        let expr = Expression::Literal(SqlValue::Integer(42));
        let mut buf = Vec::new();
        write_expression(&mut buf, &expr).unwrap();

        let mut reader = &buf[..];
        let result = read_expression(&mut reader).unwrap();
        assert_eq!(result, expr);
    }

    #[test]
    fn test_column_ref_roundtrip() {
        let expr =
            Expression::ColumnRef { table: Some("users".to_string()), column: "id".to_string() };
        let mut buf = Vec::new();
        write_expression(&mut buf, &expr).unwrap();

        let mut reader = &buf[..];
        let result = read_expression(&mut reader).unwrap();
        assert_eq!(result, expr);
    }

    #[test]
    fn test_binary_op_roundtrip() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Plus,
            left: Box::new(Expression::Literal(SqlValue::Integer(1))),
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        };
        let mut buf = Vec::new();
        write_expression(&mut buf, &expr).unwrap();

        let mut reader = &buf[..];
        let result = read_expression(&mut reader).unwrap();
        assert_eq!(result, expr);
    }

    #[test]
    fn test_function_roundtrip() {
        let expr = Expression::Function {
            name: "UPPER".to_string(),
            args: vec![Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("test")))],
            character_unit: None,
        };
        let mut buf = Vec::new();
        write_expression(&mut buf, &expr).unwrap();

        let mut reader = &buf[..];
        let result = read_expression(&mut reader).unwrap();
        assert_eq!(result, expr);
    }

    #[test]
    fn test_case_roundtrip() {
        let expr = Expression::Case {
            operand: None,
            when_clauses: vec![CaseWhen {
                conditions: vec![Expression::Literal(SqlValue::Boolean(true))],
                result: Expression::Literal(SqlValue::Integer(1)),
            }],
            else_result: Some(Box::new(Expression::Literal(SqlValue::Integer(0)))),
        };
        let mut buf = Vec::new();
        write_expression(&mut buf, &expr).unwrap();

        let mut reader = &buf[..];
        let result = read_expression(&mut reader).unwrap();
        assert_eq!(result, expr);
    }
}
