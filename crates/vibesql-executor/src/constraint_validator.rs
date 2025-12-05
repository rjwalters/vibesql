//! Constraint validation logic for DDL operations
//!
//! This module provides reusable constraint validation that can be used by
//! CREATE TABLE, ALTER TABLE, and other DDL executors.

#![allow(clippy::new_without_default)]

use vibesql_ast::{
    ColumnConstraintKind, ColumnDef, Expression, TableConstraint, TableConstraintKind,
};
use vibesql_catalog::{ColumnSchema, TableSchema};

use crate::errors::ExecutorError;

/// Result of processing constraints
pub struct ConstraintResult {
    /// Primary key column names (if any)
    pub primary_key: Option<Vec<String>>,
    /// UNIQUE constraints (each Vec<String> is a set of columns)
    pub unique_constraints: Vec<Vec<String>>,
    /// CHECK constraints (name, expression pairs)
    pub check_constraints: Vec<(String, Expression)>,
    /// Columns that should be marked as NOT NULL
    pub not_null_columns: Vec<String>,
}

impl ConstraintResult {
    /// Create an empty constraint result
    pub fn new() -> Self {
        Self {
            primary_key: None,
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            not_null_columns: Vec::new(),
        }
    }
}

/// Constraint validator for table creation and alteration
pub struct ConstraintValidator;

impl ConstraintValidator {
    /// Process all constraints from column definitions and table constraints
    ///
    /// # Arguments
    ///
    /// * `columns` - The column definitions from the DDL statement
    /// * `table_constraints` - The table-level constraints
    ///
    /// # Returns
    ///
    /// A `ConstraintResult` containing all processed constraints, or an error if validation fails
    ///
    /// # Errors
    ///
    /// Returns `ExecutorError::MultiplePrimaryKeys` if multiple PRIMARY KEY constraints are defined
    pub fn process_constraints(
        columns: &[ColumnDef],
        table_constraints: &[TableConstraint],
    ) -> Result<ConstraintResult, ExecutorError> {
        let mut result = ConstraintResult::new();
        let mut constraint_counter = 0;

        // Track if we've seen a primary key at column level
        let mut has_column_level_pk = false;

        // Process column-level constraints
        for col_def in columns {
            for constraint in &col_def.constraints {
                match &constraint.kind {
                    ColumnConstraintKind::PrimaryKey => {
                        if has_column_level_pk {
                            return Err(ExecutorError::MultiplePrimaryKeys);
                        }
                        if result.primary_key.is_some() {
                            return Err(ExecutorError::MultiplePrimaryKeys);
                        }
                        result.primary_key = Some(vec![col_def.name.clone()]);
                        result.not_null_columns.push(col_def.name.clone());
                        has_column_level_pk = true;
                    }
                    ColumnConstraintKind::Unique => {
                        result.unique_constraints.push(vec![col_def.name.clone()]);
                    }
                    ColumnConstraintKind::Check(expr) => {
                        let constraint_name = format!("check_{}", constraint_counter);
                        constraint_counter += 1;
                        result.check_constraints.push((constraint_name, (**expr).clone()));
                    }
                    ColumnConstraintKind::NotNull => {
                        result.not_null_columns.push(col_def.name.clone());
                    }
                    ColumnConstraintKind::References { .. } => {
                        // Foreign key constraints are handled separately
                        // during INSERT/UPDATE/DELETE operations
                    }
                    ColumnConstraintKind::AutoIncrement => {
                        // AUTO_INCREMENT is handled in create_table.rs by creating
                        // an internal sequence and setting the default value
                        // No constraint validation needed here
                    }
                    ColumnConstraintKind::Key => {
                        // KEY is a MySQL-specific index marker
                        // For MVP, we parse it but don't enforce indexing behavior
                        // No constraint validation needed here
                    }
                }
            }
        }

        // Process table-level constraints
        for table_constraint in table_constraints {
            match &table_constraint.kind {
                TableConstraintKind::PrimaryKey { columns: pk_cols } => {
                    // Only allow one PRIMARY KEY constraint total (column-level OR table-level)
                    if result.primary_key.is_some() {
                        return Err(ExecutorError::MultiplePrimaryKeys);
                    }
                    // Extract column names from IndexColumn structs
                    let column_names: Vec<String> =
                        pk_cols.iter().map(|c| c.column_name.clone()).collect();
                    result.primary_key = Some(column_names.clone());
                    // All PK columns must be NOT NULL
                    for col_name in &column_names {
                        if !result.not_null_columns.contains(col_name) {
                            result.not_null_columns.push(col_name.clone());
                        }
                    }
                }
                TableConstraintKind::Unique { columns } => {
                    // Extract column names from IndexColumn structs
                    let column_names: Vec<String> =
                        columns.iter().map(|c| c.column_name.clone()).collect();
                    result.unique_constraints.push(column_names);
                }
                TableConstraintKind::Check { expr } => {
                    let constraint_name = format!("check_{}", constraint_counter);
                    constraint_counter += 1;
                    result.check_constraints.push((constraint_name, (**expr).clone()));
                }
                TableConstraintKind::ForeignKey { .. } => {
                    // Foreign key constraints are handled separately
                    // during INSERT/UPDATE/DELETE operations
                }
                TableConstraintKind::Fulltext { .. } => {
                    // FULLTEXT index constraints are handled separately
                    // during table creation/schema updates
                    // TODO: Implement FULLTEXT index tracking
                }
            }
        }

        Ok(result)
    }

    /// Apply constraint results to a mutable column list
    ///
    /// This updates column nullability based on NOT NULL and PRIMARY KEY constraints
    ///
    /// # Arguments
    ///
    /// * `columns` - The column schemas to update
    /// * `constraint_result` - The constraint processing results
    pub fn apply_to_columns(columns: &mut [ColumnSchema], constraint_result: &ConstraintResult) {
        // Mark NOT NULL columns as non-nullable
        for col_name in &constraint_result.not_null_columns {
            if let Some(col) = columns.iter_mut().find(|c| c.name == *col_name) {
                col.nullable = false;
            }
        }
    }

    /// Apply constraint results to a table schema
    ///
    /// This sets the primary key, unique constraints, and check constraints on the schema
    ///
    /// # Arguments
    ///
    /// * `table_schema` - The table schema to update
    /// * `constraint_result` - The constraint processing results
    pub fn apply_to_schema(table_schema: &mut TableSchema, constraint_result: &ConstraintResult) {
        // Set primary key
        if let Some(pk) = &constraint_result.primary_key {
            table_schema.primary_key = Some(pk.clone());
        }

        // Set unique constraints
        table_schema.unique_constraints = constraint_result.unique_constraints.clone();

        // Set check constraints
        table_schema.check_constraints = constraint_result.check_constraints.clone();
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::ColumnConstraint;
    use vibesql_types::DataType;

    use super::*;

    fn make_column_def(name: &str, constraint_kinds: Vec<ColumnConstraintKind>) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Integer,
            nullable: true,
            constraints: constraint_kinds
                .into_iter()
                .map(|kind| ColumnConstraint { name: None, kind })
                .collect(),
            default_value: None,
            comment: None,
        }
    }

    #[test]
    fn test_column_level_primary_key() {
        let columns = vec![make_column_def("id", vec![ColumnConstraintKind::PrimaryKey])];
        let result = ConstraintValidator::process_constraints(&columns, &[]).unwrap();

        assert_eq!(result.primary_key, Some(vec!["id".to_string()]));
        assert!(result.not_null_columns.contains(&"id".to_string()));
    }

    #[test]
    fn test_table_level_primary_key() {
        let columns = vec![make_column_def("id", vec![]), make_column_def("tenant_id", vec![])];
        let constraints = vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![
                    vibesql_ast::IndexColumn {
                        column_name: "id".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                    vibesql_ast::IndexColumn {
                        column_name: "tenant_id".to_string(),
                        direction: vibesql_ast::OrderDirection::Asc,
                        prefix_length: None,
                    },
                ],
            },
        }];

        let result = ConstraintValidator::process_constraints(&columns, &constraints).unwrap();

        assert_eq!(result.primary_key, Some(vec!["id".to_string(), "tenant_id".to_string()]));
        assert!(result.not_null_columns.contains(&"id".to_string()));
        assert!(result.not_null_columns.contains(&"tenant_id".to_string()));
    }

    #[test]
    fn test_multiple_primary_keys_fails() {
        let columns = vec![make_column_def("id", vec![ColumnConstraintKind::PrimaryKey])];
        let constraints = vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::PrimaryKey {
                columns: vec![vibesql_ast::IndexColumn {
                    column_name: "id".to_string(),
                    direction: vibesql_ast::OrderDirection::Asc,
                    prefix_length: None,
                }],
            },
        }];

        let result = ConstraintValidator::process_constraints(&columns, &constraints);
        assert!(matches!(result, Err(ExecutorError::MultiplePrimaryKeys)));
    }

    #[test]
    fn test_unique_constraints() {
        let columns = vec![
            make_column_def("email", vec![ColumnConstraintKind::Unique]),
            make_column_def("username", vec![]),
        ];
        let constraints = vec![TableConstraint {
            name: None,
            kind: TableConstraintKind::Unique {
                columns: vec![vibesql_ast::IndexColumn {
                    column_name: "username".to_string(),
                    direction: vibesql_ast::OrderDirection::Asc,
                    prefix_length: None,
                }],
            },
        }];

        let result = ConstraintValidator::process_constraints(&columns, &constraints).unwrap();

        assert_eq!(result.unique_constraints.len(), 2);
        assert!(result.unique_constraints.contains(&vec!["email".to_string()]));
        assert!(result.unique_constraints.contains(&vec!["username".to_string()]));
    }

    #[test]
    fn test_check_constraints() {
        use vibesql_types::SqlValue;

        let check_expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "age".to_string() }),
            op: vibesql_ast::BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(0))),
        };

        let columns = vec![make_column_def(
            "age",
            vec![ColumnConstraintKind::Check(Box::new(check_expr.clone()))],
        )];

        let result = ConstraintValidator::process_constraints(&columns, &[]).unwrap();

        assert_eq!(result.check_constraints.len(), 1);
        assert_eq!(result.check_constraints[0].1, check_expr);
    }

    #[test]
    fn test_apply_to_columns() {
        let mut columns = vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, true),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ];

        let mut result = ConstraintResult::new();
        result.not_null_columns.push("id".to_string());

        ConstraintValidator::apply_to_columns(&mut columns, &result);

        assert!(!columns[0].nullable); // id should be NOT NULL
        assert!(columns[1].nullable); // name should still be nullable
    }
}
