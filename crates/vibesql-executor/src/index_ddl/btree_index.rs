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
    // Tag the owning schema (e.g. `main` or a session temp schema) so a
    // temp-table index is separable from a main-table index. See issue #5513.
    .with_schema(super::schema_of_qualified(qualified_table_name))
    .with_where_clause(stmt.where_clause.as_ref().map(|expr| (**expr).clone()));
    database.catalog.add_index(index_metadata)?;

    // Create the B-tree index. On build failure the catalog entry added above
    // must be rolled back — otherwise a failed CREATE INDEX (e.g. SQLite's
    // evaluation-time "non-deterministic use of <fn>() in an index"
    // rejection, date2-310/410) would leave a phantom catalog entry that
    // blocks re-creating the index under the same name (date2-320/420).
    let build_result = build_btree_index_body(
        database,
        stmt,
        table_name,
        index_name,
        table_schema,
        unique,
        has_expression,
    );
    if let Err(e) = build_result {
        // Roll back the catalog entry using the schema-qualified table name so
        // we drop exactly the index just added (not a same-named main index when
        // the target was a temp table). See issue #5513.
        let _ = database.catalog.drop_index(qualified_table_name, index_name);
        return Err(e);
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

/// Build the physical B-tree index body (expression, partial, or plain
/// column index). Factored out of [`create_btree_index`] so a build failure
/// can roll back the already-added catalog entry.
fn build_btree_index_body(
    database: &mut Database,
    stmt: &CreateIndexStmt,
    table_name: &str,
    index_name: &str,
    table_schema: &TableSchema,
    unique: bool,
    has_expression: bool,
) -> Result<(), ExecutorError> {
    if has_expression {
        // Expression index: pre-compute keys using ExpressionEvaluator.
        // A partial predicate filters rows at build time; the catalog records
        // the where_clause, but `is_partial()` on the storage metadata will
        // be false because expression indexes go through a different storage
        // path. This is acceptable for now; the planner's
        // `IndexedColumn::is_expression()` check already excludes expression
        // indexes from most query-time paths.
        create_expression_index(
            database,
            table_name,
            index_name,
            table_schema,
            &stmt.columns,
            unique,
            stmt.where_clause.as_deref(),
        )?;
    } else if let Some(where_expr) = stmt.where_clause.as_deref() {
        // Partial column-only index: evaluate the predicate against each
        // existing LIVE row and only insert matching rows into the index
        // body. Deleted rows must not be evaluated — a tombstoned row could
        // otherwise still trigger the non-deterministic rejection below
        // (date2-420 deletes the 'now' row and expects the CREATE to pass).
        let table_rows: Vec<(usize, vibesql_storage::Row)> = match database.get_table(table_name) {
            Some(table) => table.scan_live().map(|(idx, row)| (idx, row.clone())).collect(),
            None => Vec::new(),
        };
        // Index context: SQLite rejects non-deterministic date/time uses in
        // a partial index's WHERE predicate at evaluation time, so the
        // CREATE INDEX build fails when an existing row triggers one
        // (date2-410: row data 'now' under WHERE date(b) BETWEEN ...).
        let evaluator = ExpressionEvaluator::new(table_schema)
            .with_schema_context(crate::evaluator::SchemaExprContext::Index);
        let mut included: HashSet<usize> = HashSet::with_capacity(table_rows.len());
        for (row_idx, row) in table_rows.iter().map(|(idx, row)| (*idx, row)) {
            match evaluator.eval(where_expr, row) {
                Ok(v) if is_predicate_truthy(&v) => {
                    included.insert(row_idx);
                }
                Ok(_) => {}
                // The non-deterministic rejection must PROPAGATE and fail the
                // CREATE INDEX; other evaluation errors keep the lenient
                // not-in-index treatment.
                Err(e) if e.is_non_deterministic_use() => return Err(e),
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
            index_name.to_string(),
            table_name.to_string(),
            unique,
            stmt.columns.clone(),
            Box::new(where_expr.clone()),
            &included,
        )?;
    } else {
        // Column-only index: use existing storage API
        database.create_index(
            index_name.to_string(),
            table_name.to_string(),
            unique,
            stmt.columns.clone(),
        )?;
    }
    Ok(())
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
                .with_collation(col.collation().map(|c| c.to_string()))
            } else {
                vibesql_catalog::IndexedColumn::new_column(
                    col.column_name().unwrap().to_string(),
                    order,
                )
                .with_collation(col.collation().map(|c| c.to_string()))
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
