/// Foreign key constraint definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKeyConstraint {
    pub name: Option<String>,
    pub column_names: Vec<String>,
    pub column_indices: Vec<usize>,
    pub parent_table: String,
    pub parent_column_names: Vec<String>,
    pub parent_column_indices: Vec<usize>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
}

/// Referential action for foreign key constraints.
#[derive(Debug, Clone, PartialEq)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}
