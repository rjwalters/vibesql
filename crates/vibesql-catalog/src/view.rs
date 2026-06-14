//! View definitions for SQL views

use vibesql_ast::SelectStmt;

/// View definition stored in the catalog
#[derive(Debug, Clone)]
pub struct ViewDefinition {
    /// Name of the view
    pub name: String,
    /// Schema the view belongs to. `None` means the default (main) schema.
    /// A `Some("temp")` value is used for `CREATE TEMP VIEW` / `CREATE
    /// TEMPORARY VIEW`, mirroring the `schema` tag on triggers (#5532) and
    /// indexes (#5513). Temp views are surfaced by `sqlite_temp_master` and
    /// excluded from `sqlite_master` (#5541).
    pub schema: Option<String>,
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
        ViewDefinition {
            name,
            schema: None,
            columns,
            query,
            with_check_option,
            sql_definition: None,
        }
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
            schema: None,
            columns,
            query,
            with_check_option,
            sql_definition: Some(sql_definition),
        }
    }

    /// Set the schema this view belongs to (builder-style).
    ///
    /// `None` keeps the view in the default (main) schema. A `Some("temp")`
    /// value places it in the session temp schema so it is surfaced via
    /// `sqlite_temp_master` rather than `sqlite_master`. The schema name is
    /// stored verbatim; the temp check compares it case-insensitively.
    pub fn with_schema(mut self, schema: Option<String>) -> Self {
        self.schema = schema;
        self
    }

    /// Returns true if this view lives in the temp schema.
    pub fn is_temp(&self) -> bool {
        self.schema
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("temp"))
    }
}
