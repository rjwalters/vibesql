/// Column definition in a table schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: vibesql_types::DataType,
    pub nullable: bool,
    pub default_value: Option<vibesql_ast::Expression>,
    /// Generated/computed column expression (AS(expression) syntax)
    pub generated_expr: Option<vibesql_ast::Expression>,
    /// Column-level collation (e.g., "nocase", "binary", "rtrim")
    /// When set, comparisons and ORDER BY involving this column use this collation
    pub collation: Option<String>,
    /// SQLite rowid alias eligibility flag.
    /// True only when the original type declaration was exactly "INTEGER" (case-insensitive).
    /// In SQLite, only `INTEGER PRIMARY KEY` is a rowid alias, not `INT PRIMARY KEY`.
    pub is_exact_integer_type: bool,
}

impl ColumnSchema {
    pub fn new(name: String, data_type: vibesql_types::DataType, nullable: bool) -> Self {
        // Default is_exact_integer_type to true for DataType::Integer.
        // This ensures programmatically-created INTEGER columns get the standard "INTEGER"
        // output (not "INT") and are eligible for SQLite's rowid alias behavior.
        let is_exact_integer_type = matches!(data_type, vibesql_types::DataType::Integer);
        ColumnSchema {
            name,
            data_type,
            nullable,
            default_value: None,
            generated_expr: None,
            is_exact_integer_type,
            collation: None,
        }
    }

    /// Set the nullable property
    pub fn set_nullable(&mut self, nullable: bool) {
        self.nullable = nullable;
    }

    /// Set the default value
    pub fn set_default(&mut self, default: vibesql_ast::Expression) {
        self.default_value = Some(default);
    }

    /// Drop the default value
    pub fn drop_default(&mut self) {
        self.default_value = None;
    }
}
