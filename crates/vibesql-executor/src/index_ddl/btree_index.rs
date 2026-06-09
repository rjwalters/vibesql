//! B-tree index creation
//!
//! This module handles creation of B-tree indexes, including:
//! - Standard column-based indexes
//! - Unique indexes
//! - Multi-column composite indexes
//! - Expression indexes (delegated to expression_index module)
//! - Partial indexes (with a WHERE predicate filter at build time)

use std::collections::HashSet;

use vibesql_ast::{CreateIndexStmt, IndexColumn};
use vibesql_catalog::TableSchema;
use vibesql_storage::Database;

use super::expression_index::create_expression_index;
use crate::{
    errors::ExecutorError, evaluator::ExpressionEvaluator,
    partial_index_maintenance::is_predicate_truthy,
};

/// Create a B-tree index on a table.
///
/// Handles both column-based and expression indexes.
/// Returns success message on completion.
pub fn create_btree_index(
    database: &mut Database,
    stmt: &CreateIndexStmt,
    table_name: &str,
    qualified_table_name: &str,
    table_schema: &TableSchema,
    unique: bool,
) -> Result<String, ExecutorError> {
    let index_name = &stmt.index_name;

    // Check if this is an expression index
    let has_expression = stmt.columns.iter().any(|col| col.is_expression());

    // Compute column indices early (before mutable borrows)
    // For expression indexes, we use 0xFFFFFFFF to indicate computed columns
    let column_indices: Vec<u32> = stmt
        .columns
        .iter()
        .map(|col| {
            if let Some(name) = col.column_name() {
                table_schema.get_column_index(name).map(|idx| idx as u32).unwrap_or(0xFFFFFFFF)
            } else {
                0xFFFFFFFF // Expression column marker
            }
        })
        .collect();

    // Convert AST IndexColumn to catalog IndexedColumn
    let catalog_columns = convert_to_catalog_columns(&stmt.columns);

    // Add to catalog first (use unqualified table name as stored in catalog).
    // Partial indexes carry their WHERE predicate through to the catalog so
    // downstream code (FK-mismatch checker, index-scan selection) can
    // distinguish them from full-coverage indexes.
    let index_metadata = vibesql_catalog::IndexMetadata::new(
        index_name.clone(),
        table_name.to_string(),
        vibesql_catalog::IndexType::BTree,
        catalog_columns,
        unique,
    )
    .with_where_clause(stmt.where_clause.as_ref().map(|expr| (**expr).clone()));
    database.catalog.add_index(index_metadata)?;

    // Create the B-tree index
    if has_expression {
        // Expression index: pre-compute keys using ExpressionEvaluator
        // (Expression partial indexes are not yet supported — the partial
        // predicate is ignored here. The catalog still records the
        // where_clause, but `is_partial()` on the storage metadata will be
        // false because expression indexes go through a different storage
        // path. This is acceptable for now; the planner's
        // `IndexedColumn::is_expression()` check already excludes expression
        // indexes from most query-time paths.)
        create_expression_index(
            database,
            table_name,
            index_name,
            table_schema,
            &stmt.columns,
            unique,
        )?;
    } else if let Some(where_expr) = stmt.where_clause.as_deref() {
        // Partial column-only index: evaluate the predicate against each
        // existing row and only insert matching rows into the index body.
        let table_rows: Vec<vibesql_storage::Row> = match database.get_table(table_name) {
            Some(table) => table.scan().to_vec(),
            None => Vec::new(),
        };
        let evaluator = ExpressionEvaluator::new(table_schema);
        let mut included: HashSet<usize> = HashSet::with_capacity(table_rows.len());
        for (row_idx, row) in table_rows.iter().enumerate() {
            match evaluator.eval(where_expr, row) {
                Ok(v) if is_predicate_truthy(&v) => {
                    included.insert(row_idx);
                }
                Ok(_) => {}
                Err(e) => {
                    log::warn!(
                        "Failed to evaluate partial-index '{}' predicate on row {}: {:?}; \
                         treating row as not-in-index",
                        index_name,
                        row_idx,
                        e
                    );
                }
            }
        }
        database.create_index_partial(
            index_name.clone(),
            table_name.to_string(),
            unique,
            stmt.columns.clone(),
            Box::new(where_expr.clone()),
            &included,
        )?;
    } else {
        // Column-only index: use existing storage API
        database.create_index(
            index_name.clone(),
            table_name.to_string(),
            unique,
            stmt.columns.clone(),
        )?;
    }

    // Emit WAL entry for persistence
    database.emit_wal_create_index(
        index_name_to_id(index_name),
        index_name,
        qualified_table_name,
        column_indices,
        unique,
    );

    Ok(format!("Index '{}' created successfully on table '{}'", index_name, qualified_table_name))
}

/// Convert AST IndexColumn to catalog IndexedColumn.
fn convert_to_catalog_columns(columns: &[IndexColumn]) -> Vec<vibesql_catalog::IndexedColumn> {
    columns
        .iter()
        .map(|col| {
            let order = match col.direction() {
                vibesql_ast::OrderDirection::Asc => vibesql_catalog::SortOrder::Ascending,
                vibesql_ast::OrderDirection::Desc => vibesql_catalog::SortOrder::Descending,
            };

            if let Some(expr) = col.get_expression() {
                // Expression index: store the expression for later use
                vibesql_catalog::IndexedColumn::new_expression(expr.clone(), order)
            } else if let Some(prefix_len) = col.prefix_length() {
                vibesql_catalog::IndexedColumn::new_column_with_prefix(
                    col.column_name().unwrap().to_string(),
                    order,
                    prefix_len,
                )
            } else {
                vibesql_catalog::IndexedColumn::new_column(
                    col.column_name().unwrap().to_string(),
                    order,
                )
            }
        })
        .collect()
}

/// Compute an index ID from index name using hash (for consistent mapping).
pub fn index_name_to_id(name: &str) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut hasher);
    hasher.finish() as u32
}
