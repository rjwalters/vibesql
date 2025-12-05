//! Tests for stored procedure and function functionality

use super::common::setup_users_table as setup_test_table;
use vibesql_ast::*;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::advanced_objects;
use crate::errors::ExecutorError;

// Test fixtures and helpers to reduce duplication

/// Creates a fresh test database instance
pub(crate) fn setup_test_db() -> Database {
    Database::new()
}

/// Creates a simple procedure with minimal boilerplate
pub(crate) fn create_simple_procedure(
    name: &str,
    parameters: Vec<ProcedureParameter>,
    body: Vec<ProceduralStatement>,
) -> CreateProcedureStmt {
    CreateProcedureStmt {
        procedure_name: name.to_string(),
        parameters,
        body: ProcedureBody::BeginEnd(body),
        sql_security: None,
        comment: None,
        language: None,
    }
}

/// Creates an empty procedure (useful for basic existence tests)
pub(crate) fn create_empty_procedure(name: &str) -> CreateProcedureStmt {
    create_simple_procedure(name, vec![], vec![])
}

/// Creates a procedure with a single IN parameter
pub(crate) fn create_procedure_with_in_param(
    name: &str,
    param_name: &str,
    param_type: DataType,
    body: Vec<ProceduralStatement>,
) -> CreateProcedureStmt {
    create_simple_procedure(
        name,
        vec![ProcedureParameter {
            mode: ParameterMode::In,
            name: param_name.to_string(),
            data_type: param_type,
        }],
        body,
    )
}

/// Creates a simple function with minimal boilerplate
#[allow(dead_code)]
pub(crate) fn create_simple_function(
    name: &str,
    parameters: Vec<FunctionParameter>,
    return_type: DataType,
    body: Vec<ProceduralStatement>,
) -> CreateFunctionStmt {
    CreateFunctionStmt {
        function_name: name.to_string(),
        parameters,
        return_type,
        body: ProcedureBody::BeginEnd(body),
        deterministic: None,
        sql_security: None,
        comment: None,
        language: None,
    }
}

/// Creates an empty function (useful for basic existence tests)
#[allow(dead_code)]
pub(crate) fn create_empty_function(name: &str, return_type: DataType) -> CreateFunctionStmt {
    create_simple_function(name, vec![], return_type, vec![])
}

mod control_flow;
mod edge_cases;
mod error_messages;
mod functions;
mod parameters;
mod stored_procedures;
