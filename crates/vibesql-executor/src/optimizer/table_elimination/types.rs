//! Type definitions for table elimination optimizer pass

use vibesql_ast::Expression;

/// Info about a table in FROM clause
#[derive(Debug, Clone)]
pub(super) struct TableInfo {
    pub name: String,
    pub alias: Option<String>,
}

/// Info about an eliminated table
#[derive(Debug)]
pub(super) struct EliminatedTable {
    pub name: String,
    pub alias: Option<String>,
    pub filter: Option<Expression>,
}
