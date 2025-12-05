//! View definitions for SQL views

use vibesql_ast::SelectStmt;

/// View definition stored in the catalog
#[derive(Debug, Clone)]
pub struct ViewDefinition {
    /// Name of the view
    pub name: String,
    /// Optional column names for the view
    pub columns: Option<Vec<String>>,
    /// The SELECT query that defines the view
    pub query: SelectStmt,
    /// Whether WITH CHECK OPTION is enabled
    pub with_check_option: bool,
    /// Optional SQL definition string (for persistence/serialization)
    /// When None, we can fallback to Debug format of the query AST
    pub sql_definition: Option<String>,
}

impl ViewDefinition {
    /// Create a new view definition
    pub fn new(
        name: String,
        columns: Option<Vec<String>>,
        query: SelectStmt,
        with_check_option: bool,
    ) -> Self {
        ViewDefinition { name, columns, query, with_check_option, sql_definition: None }
    }

    /// Create a new view definition with SQL definition string
    pub fn new_with_sql(
        name: String,
        columns: Option<Vec<String>>,
        query: SelectStmt,
        with_check_option: bool,
        sql_definition: String,
    ) -> Self {
        ViewDefinition {
            name,
            columns,
            query,
            with_check_option,
            sql_definition: Some(sql_definition),
        }
    }
}
