use std::collections::HashMap;

use crate::{column::ColumnSchema, foreign_key::ForeignKeyConstraint};

/// Storage format for tables
///
/// Tables can be stored in row-oriented (default) or columnar format.
/// Re-exported from vibesql_ast for convenience.
pub use vibesql_ast::StorageFormat;

/// Table schema definition.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    /// Cache for O(1) column name to index lookup
    column_index_cache: HashMap<String, usize>,
    /// Primary key column names (None if no primary key, Some(vec) for single or composite key)
    pub primary_key: Option<Vec<String>>,
    /// Unique constraints - each inner vec represents a unique constraint (can be single or
    /// composite)
    pub unique_constraints: Vec<Vec<String>>,
    /// Check constraints - each tuple is (constraint_name, check_expression)
    pub check_constraints: Vec<(String, vibesql_ast::Expression)>,
    /// Foreign key constraints
    pub foreign_keys: Vec<ForeignKeyConstraint>,
    /// Storage format for this table (row-oriented or columnar)
    pub storage_format: StorageFormat,
}

impl TableSchema {
    pub fn new(name: String, columns: Vec<ColumnSchema>) -> Self {
        // Store columns by exact name for case-sensitive lookups.
        // The parser normalizes unquoted identifiers to uppercase, so case-insensitive
        // matching for regular identifiers works automatically. Delimited identifiers
        // (quoted with "") preserve exact case per SQL:1999 Section 5.2.
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            storage_format: StorageFormat::default(),
        }
    }

    /// Create a table schema with a primary key
    pub fn with_primary_key(
        name: String,
        columns: Vec<ColumnSchema>,
        primary_key: Vec<String>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: Some(primary_key),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            storage_format: StorageFormat::default(),
        }
    }

    /// Create a table schema with unique constraints
    pub fn with_unique_constraints(
        name: String,
        columns: Vec<ColumnSchema>,
        unique_constraints: Vec<Vec<String>>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: None,
            unique_constraints,
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            storage_format: StorageFormat::default(),
        }
    }

    /// Create a table schema with foreign key constraints
    pub fn with_foreign_keys(
        name: String,
        columns: Vec<ColumnSchema>,
        foreign_keys: Vec<ForeignKeyConstraint>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys,
            storage_format: StorageFormat::default(),
        }
    }

    /// Create a table schema with both primary key and unique constraints
    pub fn with_all_constraints(
        name: String,
        columns: Vec<ColumnSchema>,
        primary_key: Option<Vec<String>>,
        unique_constraints: Vec<Vec<String>>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key,
            unique_constraints,
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            storage_format: StorageFormat::default(),
        }
    }

    /// Create a table schema with all constraint types
    pub fn with_all_constraint_types(
        name: String,
        columns: Vec<ColumnSchema>,
        primary_key: Option<Vec<String>>,
        unique_constraints: Vec<Vec<String>>,
        check_constraints: Vec<(String, vibesql_ast::Expression)>,
        foreign_keys: Vec<ForeignKeyConstraint>,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key,
            unique_constraints,
            check_constraints,
            foreign_keys,
            storage_format: StorageFormat::default(),
        }
    }

    /// Create a table schema with storage format
    pub fn with_storage_format(
        name: String,
        columns: Vec<ColumnSchema>,
        storage_format: StorageFormat,
    ) -> Self {
        let column_index_cache: HashMap<String, usize> =
            columns.iter().enumerate().map(|(idx, col)| (col.name.clone(), idx)).collect();

        TableSchema {
            name,
            columns,
            column_index_cache,
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            storage_format,
        }
    }

    /// Set the storage format for this table
    pub fn set_storage_format(&mut self, storage_format: StorageFormat) {
        self.storage_format = storage_format;
    }

    /// Check if this table uses columnar storage
    pub fn is_columnar(&self) -> bool {
        matches!(self.storage_format, StorageFormat::Columnar)
    }

    /// Get column by name.
    /// Uses exact case matching. The parser normalizes unquoted identifiers to uppercase,
    /// so case-insensitive matching for regular identifiers works automatically.
    /// Delimited identifiers preserve exact case per SQL:1999 Section 5.2.
    pub fn get_column(&self, name: &str) -> Option<&ColumnSchema> {
        self.get_column_index(name).map(|idx| &self.columns[idx])
    }

    /// Get column index by name.
    /// First tries exact case match to support delimited identifiers (SQL:1999 Section 5.2).
    /// Falls back to case-insensitive search for backward compatibility with tests
    /// that create schemas directly without parser normalization.
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        // First, try exact case match (supports delimited identifiers correctly)
        if let Some(&idx) = self.column_index_cache.get(name) {
            return Some(idx);
        }
        // Fallback: case-insensitive search for backward compatibility
        // This handles cases where tests create columns with lowercase names
        // but SQL queries normalize to uppercase
        let name_lower = name.to_lowercase();
        self.column_index_cache
            .iter()
            .find(|(k, _)| k.to_lowercase() == name_lower)
            .map(|(_, &idx)| idx)
    }

    /// Get number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get the indices of primary key columns
    pub fn get_primary_key_indices(&self) -> Option<Vec<usize>> {
        self.primary_key.as_ref().map(|pk_cols| {
            pk_cols.iter().filter_map(|col_name| self.get_column_index(col_name)).collect()
        })
    }

    /// Get the indices for all unique constraints
    /// Returns a vector where each element is a vector of column indices for one unique constraint
    pub fn get_unique_constraint_indices(&self) -> Vec<Vec<usize>> {
        self.unique_constraints
            .iter()
            .map(|constraint_cols| {
                constraint_cols
                    .iter()
                    .filter_map(|col_name| self.get_column_index(col_name))
                    .collect()
            })
            .collect()
    }

    /// Add a column to the table schema
    pub fn add_column(&mut self, column: ColumnSchema) -> Result<(), crate::CatalogError> {
        if self.get_column(&column.name).is_some() {
            return Err(crate::CatalogError::ColumnAlreadyExists(column.name));
        }
        let index = self.columns.len();
        self.column_index_cache.insert(column.name.clone(), index);
        self.columns.push(column);
        Ok(())
    }

    /// Remove a column from the table schema by index
    pub fn remove_column(&mut self, index: usize) -> Result<(), crate::CatalogError> {
        if index >= self.columns.len() {
            return Err(crate::CatalogError::ColumnNotFound {
                column_name: "index out of bounds".to_string(),
                table_name: self.name.clone(),
            });
        }
        let removed_column = self.columns.remove(index);

        // Rebuild the column index cache since indices have shifted
        self.column_index_cache.clear();
        for (idx, col) in self.columns.iter().enumerate() {
            self.column_index_cache.insert(col.name.clone(), idx);
        }

        // Remove from primary key if present
        if let Some(ref mut pk) = self.primary_key {
            pk.retain(|col_name| col_name != &removed_column.name);
            if pk.is_empty() {
                self.primary_key = None;
            }
        }

        // Remove from unique constraints
        self.unique_constraints = self
            .unique_constraints
            .iter()
            .filter_map(|constraint| {
                let filtered: Vec<String> = constraint
                    .iter()
                    .filter(|col_name| *col_name != &removed_column.name)
                    .cloned()
                    .collect();
                if filtered.is_empty() {
                    None
                } else {
                    Some(filtered)
                }
            })
            .collect();

        // Remove foreign keys that reference the removed column
        self.foreign_keys = self
            .foreign_keys
            .iter()
            .filter(|fk| !fk.column_names.contains(&removed_column.name))
            .cloned()
            .collect();

        // Remove check constraints that reference the removed column
        self.check_constraints.retain(|(_name, expr)| {
            !Self::expression_references_column(expr, &removed_column.name)
        });

        Ok(())
    }

    /// Check if a column exists
    pub fn has_column(&self, name: &str) -> bool {
        self.get_column(name).is_some()
    }

    /// Check if a column is part of the primary key
    pub fn is_column_in_primary_key(&self, column_name: &str) -> bool {
        self.primary_key.as_ref().is_some_and(|pk| pk.contains(&column_name.to_string()))
    }

    /// Set nullable property for a column by index
    pub fn set_column_nullable(
        &mut self,
        index: usize,
        nullable: bool,
    ) -> Result<(), crate::CatalogError> {
        if index >= self.columns.len() {
            return Err(crate::CatalogError::ColumnNotFound {
                column_name: "index out of bounds".to_string(),
                table_name: self.name.clone(),
            });
        }
        self.columns[index].set_nullable(nullable);
        Ok(())
    }

    /// Set default value for a column by index
    pub fn set_column_default(
        &mut self,
        index: usize,
        default: vibesql_ast::Expression,
    ) -> Result<(), crate::CatalogError> {
        if index >= self.columns.len() {
            return Err(crate::CatalogError::ColumnNotFound {
                column_name: "index out of bounds".to_string(),
                table_name: self.name.clone(),
            });
        }
        self.columns[index].set_default(default);
        Ok(())
    }

    /// Drop default value for a column by index
    pub fn drop_column_default(&mut self, index: usize) -> Result<(), crate::CatalogError> {
        if index >= self.columns.len() {
            return Err(crate::CatalogError::ColumnNotFound {
                column_name: "index out of bounds".to_string(),
                table_name: self.name.clone(),
            });
        }
        self.columns[index].drop_default();
        Ok(())
    }

    /// Add a check constraint
    pub fn add_check_constraint(
        &mut self,
        name: String,
        expr: vibesql_ast::Expression,
    ) -> Result<(), crate::CatalogError> {
        // Check if constraint name already exists
        if self.check_constraints.iter().any(|(n, _)| n == &name) {
            return Err(crate::CatalogError::ConstraintAlreadyExists(name));
        }
        self.check_constraints.push((name, expr));
        Ok(())
    }

    /// Add a unique constraint
    pub fn add_unique_constraint(
        &mut self,
        columns: Vec<String>,
    ) -> Result<(), crate::CatalogError> {
        // Verify all columns exist
        for col_name in &columns {
            if !self.has_column(col_name) {
                return Err(crate::CatalogError::ColumnNotFound {
                    column_name: col_name.clone(),
                    table_name: self.name.clone(),
                });
            }
        }
        self.unique_constraints.push(columns);
        Ok(())
    }

    /// Add a foreign key constraint
    pub fn add_foreign_key(
        &mut self,
        foreign_key: ForeignKeyConstraint,
    ) -> Result<(), crate::CatalogError> {
        // Verify all columns exist
        for col_name in &foreign_key.column_names {
            if !self.has_column(col_name) {
                return Err(crate::CatalogError::ColumnNotFound {
                    column_name: col_name.clone(),
                    table_name: self.name.clone(),
                });
            }
        }
        self.foreign_keys.push(foreign_key);
        Ok(())
    }

    /// Remove a check constraint by name
    pub fn drop_check_constraint(&mut self, name: &str) -> Result<(), crate::CatalogError> {
        let original_len = self.check_constraints.len();
        self.check_constraints.retain(|(n, _)| n != name);
        if self.check_constraints.len() == original_len {
            return Err(crate::CatalogError::ConstraintNotFound(name.to_string()));
        }
        Ok(())
    }

    /// Remove a unique constraint by column names
    pub fn drop_unique_constraint(
        &mut self,
        columns: &[String],
    ) -> Result<(), crate::CatalogError> {
        let original_len = self.unique_constraints.len();
        self.unique_constraints.retain(|constraint| constraint != columns);
        if self.unique_constraints.len() == original_len {
            return Err(crate::CatalogError::ConstraintNotFound(format!("{:?}", columns)));
        }
        Ok(())
    }

    /// Remove a foreign key constraint by name
    pub fn drop_foreign_key(&mut self, name: &str) -> Result<(), crate::CatalogError> {
        let original_len = self.foreign_keys.len();
        self.foreign_keys.retain(|fk| fk.name.as_deref() != Some(name));
        if self.foreign_keys.len() == original_len {
            return Err(crate::CatalogError::ConstraintNotFound(name.to_string()));
        }
        Ok(())
    }

    /// Check if an expression references a specific column
    fn expression_references_column(expr: &vibesql_ast::Expression, column_name: &str) -> bool {
        match expr {
            vibesql_ast::Expression::ColumnRef { column, .. } => column == column_name,
            vibesql_ast::Expression::BinaryOp { left, right, .. } => {
                Self::expression_references_column(left, column_name)
                    || Self::expression_references_column(right, column_name)
            }
            vibesql_ast::Expression::Conjunction(children)
            | vibesql_ast::Expression::Disjunction(children) => {
                children.iter().any(|child| Self::expression_references_column(child, column_name))
            }
            vibesql_ast::Expression::UnaryOp { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::Function { args, .. }
            | vibesql_ast::Expression::AggregateFunction { args, .. } => {
                args.iter().any(|arg| Self::expression_references_column(arg, column_name))
            }
            vibesql_ast::Expression::IsNull { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                // Check operand
                if let Some(op) = operand {
                    if Self::expression_references_column(op, column_name) {
                        return true;
                    }
                }
                // Check when clauses
                for clause in when_clauses {
                    // Check all conditions in this clause
                    if clause
                        .conditions
                        .iter()
                        .any(|cond| Self::expression_references_column(cond, column_name))
                    {
                        return true;
                    }
                    // Check result
                    if Self::expression_references_column(&clause.result, column_name) {
                        return true;
                    }
                }
                // Check else result
                if let Some(else_expr) = else_result {
                    if Self::expression_references_column(else_expr, column_name) {
                        return true;
                    }
                }
                false
            }
            vibesql_ast::Expression::ScalarSubquery(_) | vibesql_ast::Expression::Exists { .. } => {
                // Subqueries can reference columns, but for now we'll be conservative
                // and not remove check constraints with subqueries
                false
            }
            vibesql_ast::Expression::In { expr, .. }
            | vibesql_ast::Expression::InList { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::Between { expr, low, high, .. } => {
                Self::expression_references_column(expr, column_name)
                    || Self::expression_references_column(low, column_name)
                    || Self::expression_references_column(high, column_name)
            }
            vibesql_ast::Expression::WindowFunction { function, over } => {
                // Check function arguments
                let func_refs_column = match function {
                    vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                    | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                    | vibesql_ast::WindowFunctionSpec::Value { args, .. } => {
                        args.iter().any(|arg| Self::expression_references_column(arg, column_name))
                    }
                };
                if func_refs_column {
                    return true;
                }

                // Check partition by
                if let Some(partition_exprs) = &over.partition_by {
                    if partition_exprs
                        .iter()
                        .any(|expr| Self::expression_references_column(expr, column_name))
                    {
                        return true;
                    }
                }

                // Check order by
                if let Some(order_items) = &over.order_by {
                    if order_items
                        .iter()
                        .any(|item| Self::expression_references_column(&item.expr, column_name))
                    {
                        return true;
                    }
                }

                false
            }
            vibesql_ast::Expression::Cast { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::Position { substring, string, .. } => {
                Self::expression_references_column(substring, column_name)
                    || Self::expression_references_column(string, column_name)
            }
            vibesql_ast::Expression::Trim { removal_char, string, .. } => {
                removal_char
                    .as_ref()
                    .is_some_and(|e| Self::expression_references_column(e, column_name))
                    || Self::expression_references_column(string, column_name)
            }
            vibesql_ast::Expression::Extract { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::Like { expr, pattern, .. } => {
                Self::expression_references_column(expr, column_name)
                    || Self::expression_references_column(pattern, column_name)
            }
            vibesql_ast::Expression::QuantifiedComparison { expr, .. } => {
                Self::expression_references_column(expr, column_name)
            }
            vibesql_ast::Expression::DuplicateKeyValue { column } => column == column_name,
            vibesql_ast::Expression::PseudoVariable { column, .. } => column == column_name,
            // These don't reference columns
            vibesql_ast::Expression::Interval { value, .. } => {
                // INTERVAL expressions may contain column references in the value
                Self::expression_references_column(value, column_name)
            }
            vibesql_ast::Expression::Literal(_)
            | vibesql_ast::Expression::Placeholder(_)
            | vibesql_ast::Expression::NumberedPlaceholder(_)
            | vibesql_ast::Expression::NamedPlaceholder(_)
            | vibesql_ast::Expression::Wildcard
            | vibesql_ast::Expression::CurrentDate
            | vibesql_ast::Expression::CurrentTime { .. }
            | vibesql_ast::Expression::CurrentTimestamp { .. }
            | vibesql_ast::Expression::Default
            | vibesql_ast::Expression::NextValue { .. }
            | vibesql_ast::Expression::SessionVariable { .. }
            | vibesql_ast::Expression::MatchAgainst { .. } => false,
        }
    }
}
