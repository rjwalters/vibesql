//! Prepared statement AST types (SQL:1999 Feature E141)
//!
//! This module defines the AST types for PREPARE, EXECUTE, and DEALLOCATE statements.

use crate::Expression;

/// PREPARE statement for creating a prepared statement
///
/// SQL Standard Syntax:
///   PREPARE statement_name FROM sql_string
///
/// PostgreSQL Extended Syntax:
///   PREPARE statement_name [(data_type, ...)] AS preparable_stmt
///
/// Examples:
///   PREPARE my_select FROM 'SELECT * FROM users WHERE id = $1'
///   PREPARE my_insert(int, text) AS INSERT INTO users VALUES ($1, $2)
#[derive(Debug, Clone, PartialEq)]
pub struct PrepareStmt {
    /// Name of the prepared statement
    pub name: String,
    /// Optional list of parameter data types (PostgreSQL extended syntax)
    pub param_types: Option<Vec<String>>,
    /// The SQL statement to prepare (stored as the raw SQL string or the parsed statement)
    pub statement: PreparedStatementBody,
}

/// The body of a prepared statement
#[derive(Debug, Clone, PartialEq)]
pub enum PreparedStatementBody {
    /// SQL string literal (standard SQL FROM syntax)
    SqlString(String),
    /// Parsed statement (PostgreSQL AS syntax)
    /// The statement is stored as a boxed Statement to avoid circular dependency
    /// It will be parsed when the PREPARE is executed
    ParsedStatement(Box<crate::Statement>),
}

/// EXECUTE statement for executing a prepared statement
///
/// SQL Standard Syntax:
///   EXECUTE statement_name [USING value_list]
///
/// PostgreSQL Extended Syntax:
///   EXECUTE statement_name [(param_value, ...)]
///
/// Examples:
///   EXECUTE my_select
///   EXECUTE my_insert USING 1, 'Alice'
///   EXECUTE my_insert(1, 'Alice')
#[derive(Debug, Clone, PartialEq)]
pub struct ExecuteStmt {
    /// Name of the prepared statement to execute
    pub name: String,
    /// Parameter values to bind
    pub params: Vec<Expression>,
}

/// DEALLOCATE statement for removing a prepared statement
///
/// SQL Standard Syntax:
///   DEALLOCATE [PREPARE] statement_name
///
/// PostgreSQL Extension:
///   DEALLOCATE ALL
///
/// Examples:
///   DEALLOCATE my_select
///   DEALLOCATE PREPARE my_insert
///   DEALLOCATE ALL
#[derive(Debug, Clone, PartialEq)]
pub struct DeallocateStmt {
    /// The target to deallocate
    pub target: DeallocateTarget,
}

/// Target for DEALLOCATE statement
#[derive(Debug, Clone, PartialEq)]
pub enum DeallocateTarget {
    /// Deallocate a specific named prepared statement
    Name(String),
    /// Deallocate all prepared statements in the session
    All,
}
