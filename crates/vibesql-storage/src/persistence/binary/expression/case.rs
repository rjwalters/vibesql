// ============================================================================
// CASE Expression Serialization
// ============================================================================
//
// Handles serialization of CASE WHEN expressions.

use super::super::io::*;
use crate::StorageError;
use std::io::{Read, Write};
use vibesql_ast::CaseWhen;

pub(super) fn write_case_when<W: Write>(
    writer: &mut W,
    when: &CaseWhen,
) -> Result<(), StorageError> {
    write_u32(writer, when.conditions.len() as u32)?;
    for condition in &when.conditions {
        super::write_expression(writer, condition)?;
    }
    super::write_expression(writer, &when.result)?;
    Ok(())
}

pub(super) fn read_case_when<R: Read>(reader: &mut R) -> Result<CaseWhen, StorageError> {
    let condition_count = read_u32(reader)?;
    let mut conditions = Vec::new();
    for _ in 0..condition_count {
        conditions.push(super::read_expression(reader)?);
    }
    let result = super::read_expression(reader)?;
    Ok(CaseWhen { conditions, result })
}
