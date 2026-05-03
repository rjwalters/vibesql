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
    /// Whether the constraint is DEFERRABLE (SQL:1999 / SQLite).
    ///
    /// When `true`, the constraint can be deferred until COMMIT-time. When
    /// `false` (the default for SQLite-style FKs), enforcement is immediate.
    ///
    /// Phase C1 of #5085 only stores this metadata; the runtime enforcement
    /// timing change ships in Phase C2.
    pub is_deferrable: bool,
    /// Whether the deferrable constraint is INITIALLY DEFERRED.
    ///
    /// Only meaningful when `is_deferrable` is `true`. `true` corresponds to
    /// `INITIALLY DEFERRED`; `false` corresponds to `INITIALLY IMMEDIATE`
    /// (SQL default for deferrable constraints).
    ///
    /// Phase C1 of #5085 only stores this metadata; the runtime enforcement
    /// timing change ships in Phase C2.
    pub initially_deferred: bool,
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
