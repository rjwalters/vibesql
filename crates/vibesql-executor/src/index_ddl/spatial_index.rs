//! Spatial index creation
//!
//! This module handles creation of spatial/R-tree indexes for geometry data.
//! Spatial indexes use minimum bounding rectangles (MBRs) for efficient
//! spatial queries.

use vibesql_ast::CreateIndexStmt;
use vibesql_catalog::TableSchema;
use vibesql_storage::{
    index::{extract_mbr_from_sql_value, SpatialIndex, SpatialIndexEntry},
    Database, SpatialIndexMetadata,
};

use super::btree_index::index_name_to_id;
use crate::errors::ExecutorError;

/// Create a spatial index on a table.
///
/// Spatial indexes must be on exactly one geometry column.
/// Returns success message on completion.
pub fn create_spatial_index(
    database: &mut Database,
    stmt: &CreateIndexStmt,
    table_name: &str,
    qualified_table_name: &str,
    table_schema: &TableSchema,
) -> Result<String, ExecutorError> {
    let index_name = &stmt.index_name;

    // Spatial index validation: must be exactly 1 column
    if stmt.columns.len() != 1 {
        return Err(ExecutorError::InvalidIndexDefinition(
            "SPATIAL indexes must be defined on exactly one column".to_string(),
        ));
    }

    let column_name = stmt.columns[0].expect_column_name();

    // Get the column index
    let col_idx = table_schema.get_column_index(column_name).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: column_name.to_string(),
            table_name: qualified_table_name.to_string(),
            searched_tables: vec![qualified_table_name.to_string()],
            available_columns: table_schema.columns.iter().map(|c| c.name.clone()).collect(),
        }
    })?;

    // Extract MBRs from all existing rows. Resolve via `qualified_table_name`
    // (e.g. `aux.t`), not the bare `table_name` — a bare lookup follows
    // storage's own temp-then-current-then-attached search order and can
    // silently land on an unrelated same-named table (e.g. `main.t`) instead
    // of the table the validator already resolved the index against,
    // building the spatial index from the wrong rows (issue #6502, the
    // spatial/vector analogue of #6487).
    let table = database
        .get_table(qualified_table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(qualified_table_name.to_string()))?;

    let mut entries = Vec::new();
    // Use scan_live() to skip deleted rows and get correct physical indices
    for (row_idx, row) in table.scan_live() {
        let geom_value = &row.values[col_idx];

        // Extract MBR from geometry value (skip NULLs and invalid geometries)
        if let Some(mbr) = extract_mbr_from_sql_value(geom_value) {
            entries.push(SpatialIndexEntry { row_id: row_idx, mbr });
        }
    }

    // Build spatial index via bulk_load (more efficient than incremental inserts)
    let spatial_index = SpatialIndex::bulk_load(column_name.to_string(), entries);

    // Add to catalog first (use unqualified table name as stored in catalog)
    let index_metadata = vibesql_catalog::IndexMetadata::new(
        index_name.clone(),
        table_name.to_string(),
        vibesql_catalog::IndexType::RTree,
        vec![vibesql_catalog::IndexedColumn::new_column(
            column_name.to_string(),
            vibesql_catalog::SortOrder::Ascending,
        )],
        false,
    )
    // Tag the owning schema so a temp-table index is separable (#5513).
    .with_schema(super::schema_of_qualified(qualified_table_name));
    database.catalog.add_index(index_metadata)?;

    // Store in database (use unqualified table name for storage metadata).
    // Tag the owning schema so a temp-table spatial index and a same-named
    // main-table spatial index coexist as distinct storage entries (#5558),
    // mirroring the B-tree schema-tagging in #5540. The schema is the same one
    // the catalog records above.
    let metadata = SpatialIndexMetadata {
        index_name: index_name.clone(),
        table_name: table_name.to_string(),
        column_name: column_name.to_string(),
        schema: super::schema_of_qualified(qualified_table_name).to_string(),
        created_at: Some(chrono::Utc::now()),
    };

    database.create_spatial_index(metadata, spatial_index)?;

    // Emit WAL entry for persistence (spatial indexes are never unique)
    database.emit_wal_create_index(
        index_name_to_id(index_name),
        index_name,
        qualified_table_name,
        vec![col_idx as u32],
        false,
    );

    Ok(format!(
        "Spatial index '{}' created successfully on table '{}'",
        index_name, qualified_table_name
    ))
}
