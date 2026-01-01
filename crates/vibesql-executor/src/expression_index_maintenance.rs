//! Expression index maintenance for DML operations
//!
//! This module provides helper functions to maintain expression indexes during
//! INSERT, UPDATE, and DELETE operations. The storage layer can only handle
//! column-based indexes directly; expression indexes require the executor to
//! pre-compute key values using the ExpressionEvaluator.

use std::collections::HashMap;

use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::evaluator::ExpressionEvaluator;

/// Maintain expression indexes after inserting a row
///
/// This function evaluates expressions for all expression indexes on the table
/// and updates the indexes with the computed key values.
///
/// # Arguments
/// * `db` - Database reference
/// * `table_name` - Name of the table
/// * `row` - The inserted row
/// * `row_index` - Index of the inserted row in the table
pub fn maintain_expression_indexes_for_insert(
    db: &mut Database,
    table_name: &str,
    row: &vibesql_storage::Row,
    row_index: usize,
) {
    // Check if there are any expression indexes
    if !db.has_expression_indexes(table_name) {
        return;
    }

    // Get the table schema
    let table_schema = match db.catalog.get_table(table_name) {
        Some(schema) => schema.clone(),
        None => return,
    };

    // Get expression indexes for this table
    let expression_indexes = db.get_expression_indexes_for_table(table_name);
    if expression_indexes.is_empty() {
        return;
    }

    // Create expression evaluator
    let evaluator = ExpressionEvaluator::new(&table_schema);

    // Compute expression keys for each index
    let mut expression_keys: HashMap<String, Vec<SqlValue>> = HashMap::new();

    for (index_name, metadata) in expression_indexes {
        let mut key_values = Vec::new();

        for col in &metadata.columns {
            let value = if let Some(expr) = col.get_expression() {
                // Evaluate the expression
                match evaluator.eval(expr, row) {
                    Ok(v) => v,
                    Err(e) => {
                        log::warn!(
                            "Failed to evaluate expression for index '{}' during insert: {:?}",
                            index_name,
                            e
                        );
                        SqlValue::Null
                    }
                }
            } else if let Some(col_name) = col.column_name() {
                // Regular column reference
                if let Some(col_idx) = table_schema.get_column_index(col_name) {
                    row.values[col_idx].clone()
                } else {
                    SqlValue::Null
                }
            } else {
                SqlValue::Null
            };
            key_values.push(value);
        }

        expression_keys.insert(index_name, key_values);
    }

    // Update expression indexes
    db.add_to_expression_indexes_for_insert(table_name, row_index, &expression_keys);
}

/// Maintain expression indexes after updating a row
///
/// This function evaluates expressions for all expression indexes on the table
/// and updates the indexes with the computed key values.
///
/// # Arguments
/// * `db` - Database reference
/// * `table_name` - Name of the table
/// * `old_row` - The row before update
/// * `new_row` - The row after update
/// * `row_index` - Index of the row in the table
pub fn maintain_expression_indexes_for_update(
    db: &mut Database,
    table_name: &str,
    old_row: &vibesql_storage::Row,
    new_row: &vibesql_storage::Row,
    row_index: usize,
) {
    // Check if there are any expression indexes
    if !db.has_expression_indexes(table_name) {
        return;
    }

    // Get the table schema
    let table_schema = match db.catalog.get_table(table_name) {
        Some(schema) => schema.clone(),
        None => return,
    };

    // Get expression indexes for this table
    let expression_indexes = db.get_expression_indexes_for_table(table_name);
    if expression_indexes.is_empty() {
        return;
    }

    // Create expression evaluator
    let evaluator = ExpressionEvaluator::new(&table_schema);

    // Compute expression keys for each index (both old and new)
    let mut old_expression_keys: HashMap<String, Vec<SqlValue>> = HashMap::new();
    let mut new_expression_keys: HashMap<String, Vec<SqlValue>> = HashMap::new();

    for (index_name, metadata) in expression_indexes {
        let mut old_key_values = Vec::new();
        let mut new_key_values = Vec::new();

        for col in &metadata.columns {
            let (old_value, new_value) = if let Some(expr) = col.get_expression() {
                // Evaluate the expression on both old and new rows
                let old_val = match evaluator.eval(expr, old_row) {
                    Ok(v) => v,
                    Err(e) => {
                        log::warn!(
                            "Failed to evaluate expression for index '{}' (old row) during update: {:?}",
                            index_name,
                            e
                        );
                        SqlValue::Null
                    }
                };
                let new_val = match evaluator.eval(expr, new_row) {
                    Ok(v) => v,
                    Err(e) => {
                        log::warn!(
                            "Failed to evaluate expression for index '{}' (new row) during update: {:?}",
                            index_name,
                            e
                        );
                        SqlValue::Null
                    }
                };
                (old_val, new_val)
            } else if let Some(col_name) = col.column_name() {
                // Regular column reference
                if let Some(col_idx) = table_schema.get_column_index(col_name) {
                    (old_row.values[col_idx].clone(), new_row.values[col_idx].clone())
                } else {
                    (SqlValue::Null, SqlValue::Null)
                }
            } else {
                (SqlValue::Null, SqlValue::Null)
            };
            old_key_values.push(old_value);
            new_key_values.push(new_value);
        }

        old_expression_keys.insert(index_name.clone(), old_key_values);
        new_expression_keys.insert(index_name, new_key_values);
    }

    // Update expression indexes
    db.update_expression_indexes_for_update(
        table_name,
        row_index,
        &old_expression_keys,
        &new_expression_keys,
    );
}

/// Maintain expression indexes after deleting a row
///
/// This function evaluates expressions for all expression indexes on the table
/// and removes the computed key values from the indexes.
///
/// # Arguments
/// * `db` - Database reference
/// * `table_name` - Name of the table
/// * `row` - The deleted row
/// * `row_index` - Index of the deleted row in the table
pub fn maintain_expression_indexes_for_delete(
    db: &mut Database,
    table_name: &str,
    row: &vibesql_storage::Row,
    row_index: usize,
) {
    // Check if there are any expression indexes
    if !db.has_expression_indexes(table_name) {
        return;
    }

    // Get the table schema
    let table_schema = match db.catalog.get_table(table_name) {
        Some(schema) => schema.clone(),
        None => return,
    };

    // Get expression indexes for this table
    let expression_indexes = db.get_expression_indexes_for_table(table_name);
    if expression_indexes.is_empty() {
        return;
    }

    // Create expression evaluator
    let evaluator = ExpressionEvaluator::new(&table_schema);

    // Compute expression keys for each index
    let mut expression_keys: HashMap<String, Vec<SqlValue>> = HashMap::new();

    for (index_name, metadata) in expression_indexes {
        let mut key_values = Vec::new();

        for col in &metadata.columns {
            let value = if let Some(expr) = col.get_expression() {
                // Evaluate the expression
                match evaluator.eval(expr, row) {
                    Ok(v) => v,
                    Err(e) => {
                        log::warn!(
                            "Failed to evaluate expression for index '{}' during delete: {:?}",
                            index_name,
                            e
                        );
                        SqlValue::Null
                    }
                }
            } else if let Some(col_name) = col.column_name() {
                // Regular column reference
                if let Some(col_idx) = table_schema.get_column_index(col_name) {
                    row.values[col_idx].clone()
                } else {
                    SqlValue::Null
                }
            } else {
                SqlValue::Null
            };
            key_values.push(value);
        }

        expression_keys.insert(index_name, key_values);
    }

    // Update expression indexes
    db.update_expression_indexes_for_delete(table_name, row_index, &expression_keys);
}

/// Rebuild expression indexes after compaction
///
/// When table compaction occurs (e.g., after bulk deletes), all row indices change.
/// Regular indexes are rebuilt by the storage layer, but expression indexes require
/// the executor to evaluate expressions. This function clears and rebuilds all
/// expression indexes for the given table.
///
/// # Arguments
/// * `db` - Database reference
/// * `table_name` - Name of the table
pub fn rebuild_expression_indexes_after_compaction(db: &mut Database, table_name: &str) {
    // Check if there are any expression indexes
    if !db.has_expression_indexes(table_name) {
        return;
    }

    // Get the table schema and rows
    let table_schema = match db.catalog.get_table(table_name) {
        Some(schema) => schema.clone(),
        None => return,
    };

    let rows: Vec<vibesql_storage::Row> = match db.get_table(table_name) {
        Some(table) => table.scan().to_vec(),
        None => return,
    };

    // Get expression indexes for this table and clone metadata to avoid borrow conflict
    let expression_indexes: Vec<(String, vibesql_storage::database::indexes::IndexMetadata)> = db
        .get_expression_indexes_for_table(table_name)
        .into_iter()
        .map(|(name, metadata)| (name, metadata.clone()))
        .collect();
    if expression_indexes.is_empty() {
        return;
    }

    // Create expression evaluator
    let evaluator = ExpressionEvaluator::new(&table_schema);

    // Clear existing expression index data
    db.clear_expression_index_data(table_name);

    // Rebuild expression indexes by inserting all rows
    for (row_index, row) in rows.iter().enumerate() {
        let mut expression_keys: HashMap<String, Vec<SqlValue>> = HashMap::new();

        for (index_name, metadata) in &expression_indexes {
            let mut key_values = Vec::new();

            for col in &metadata.columns {
                let value = if let Some(expr) = col.get_expression() {
                    match evaluator.eval(expr, row) {
                        Ok(v) => v,
                        Err(e) => {
                            log::warn!(
                                "Failed to evaluate expression for index '{}' during rebuild: {:?}",
                                index_name,
                                e
                            );
                            SqlValue::Null
                        }
                    }
                } else if let Some(col_name) = col.column_name() {
                    if let Some(col_idx) = table_schema.get_column_index(col_name) {
                        row.values[col_idx].clone()
                    } else {
                        SqlValue::Null
                    }
                } else {
                    SqlValue::Null
                };
                key_values.push(value);
            }

            expression_keys.insert(index_name.clone(), key_values);
        }

        // Add entries to expression indexes
        db.add_to_expression_indexes_for_insert(table_name, row_index, &expression_keys);
    }
}

// Unit tests for expression index maintenance are deferred to integration tests.
// The core functionality is tested through the create_index tests which exercise
// the expression index creation with pre-computed keys.
