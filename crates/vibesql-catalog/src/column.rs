/// Column definition in a table schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: vibesql_types::DataType,
    pub nullable: bool,
    pub default_value: Option<vibesql_ast::Expression>,
    /// Generated/computed column expression (AS(expression) syntax)
    pub generated_expr: Option<vibesql_ast::Expression>,
}

impl ColumnSchema {
    pub fn new(name: String, data_type: vibesql_types::DataType, nullable: bool) -> Self {
        ColumnSchema { name, data_type, nullable, default_value: None, generated_expr: None }
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
