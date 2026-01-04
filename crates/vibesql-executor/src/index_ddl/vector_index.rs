//! Vector index creation
//!
//! This module handles creation of vector indexes for approximate nearest neighbor search:
//! - IVFFlat (Inverted File with Flat quantization)
//! - HNSW (Hierarchical Navigable Small World)

use vibesql_ast::{CreateIndexStmt, VectorDistanceMetric};
use vibesql_catalog::TableSchema;
use vibesql_storage::Database;
use vibesql_types::DataType;

use super::btree_index::index_name_to_id;
use crate::errors::ExecutorError;

/// Create an IVFFlat index on a vector column.
///
/// IVFFlat indexes partition vectors into clusters (lists) for efficient
/// approximate nearest neighbor search.
pub fn create_ivfflat_index(
    database: &mut Database,
    stmt: &CreateIndexStmt,
    table_name: &str,
    qualified_table_name: &str,
    table_schema: &TableSchema,
    metric: VectorDistanceMetric,
    lists: u32,
) -> Result<String, ExecutorError> {
    let index_name = &stmt.index_name;

    // IVFFlat index validation: must be exactly 1 column (vector column)
    if stmt.columns.len() != 1 {
        return Err(ExecutorError::InvalidIndexDefinition(
            "IVFFlat indexes must be defined on exactly one vector column".to_string(),
        ));
    }

    let column_name = stmt.columns[0].expect_column_name();

    // Get the column index and validate it's a vector type
    let col_idx = table_schema.get_column_index(column_name).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: column_name.to_string(),
            table_name: qualified_table_name.to_string(),
            searched_tables: vec![qualified_table_name.to_string()],
            available_columns: table_schema.columns.iter().map(|c| c.name.clone()).collect(),
        }
    })?;

    // Validate column type is VECTOR
    let col_type = &table_schema.columns[col_idx].data_type;
    let dimensions = match col_type {
        DataType::Vector { dimensions } => *dimensions as usize,
        _ => {
            return Err(ExecutorError::InvalidIndexDefinition(format!(
                "IVFFlat indexes can only be created on VECTOR columns, but '{}' has type {:?}",
                column_name, col_type
            )));
        }
    };

    // Convert AST metric to catalog metric
    let catalog_metric = convert_metric_to_catalog(metric);

    // Add to catalog first
    let index_metadata = vibesql_catalog::IndexMetadata::new(
        index_name.clone(),
        table_name.to_string(),
        vibesql_catalog::IndexType::IVFFlat { metric: catalog_metric, lists },
        vec![vibesql_catalog::IndexedColumn::new_column(
            column_name.to_string(),
            vibesql_catalog::SortOrder::Ascending, // Not meaningful for vector indexes
        )],
        false, // IVFFlat indexes are never unique
    );
    database.catalog.add_index(index_metadata)?;

    // Create the IVFFlat index in storage
    database.create_ivfflat_index(
        index_name.clone(),
        table_name.to_string(),
        column_name.to_string(),
        col_idx,
        dimensions,
        lists as usize,
        metric,
    )?;

    // Emit WAL entry for persistence (IVFFlat indexes are never unique)
    database.emit_wal_create_index(
        index_name_to_id(index_name),
        index_name,
        qualified_table_name,
        vec![col_idx as u32],
        false,
    );

    Ok(format!(
        "IVFFlat index '{}' created successfully on table '{}' column '{}'",
        index_name, qualified_table_name, column_name
    ))
}

/// Create an HNSW index on a vector column.
///
/// HNSW (Hierarchical Navigable Small World) indexes build a multi-layer graph
/// for efficient approximate nearest neighbor search with high recall.
pub fn create_hnsw_index(
    database: &mut Database,
    stmt: &CreateIndexStmt,
    table_name: &str,
    qualified_table_name: &str,
    table_schema: &TableSchema,
    metric: VectorDistanceMetric,
    m: u32,
    ef_construction: u32,
) -> Result<String, ExecutorError> {
    let index_name = &stmt.index_name;

    // HNSW index validation: must be exactly 1 column (vector column)
    if stmt.columns.len() != 1 {
        return Err(ExecutorError::InvalidIndexDefinition(
            "HNSW indexes must be defined on exactly one vector column".to_string(),
        ));
    }

    let column_name = stmt.columns[0].expect_column_name();

    // Get the column index and validate it's a vector type
    let col_idx = table_schema.get_column_index(column_name).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: column_name.to_string(),
            table_name: qualified_table_name.to_string(),
            searched_tables: vec![qualified_table_name.to_string()],
            available_columns: table_schema.columns.iter().map(|c| c.name.clone()).collect(),
        }
    })?;

    // Validate column type is VECTOR
    let col_type = &table_schema.columns[col_idx].data_type;
    let dimensions = match col_type {
        DataType::Vector { dimensions } => *dimensions as usize,
        _ => {
            return Err(ExecutorError::InvalidIndexDefinition(format!(
                "HNSW indexes can only be created on VECTOR columns, but '{}' has type {:?}",
                column_name, col_type
            )));
        }
    };

    // Convert AST metric to catalog metric
    let catalog_metric = convert_metric_to_catalog(metric);

    // Add to catalog first
    let index_metadata = vibesql_catalog::IndexMetadata::new(
        index_name.clone(),
        table_name.to_string(),
        vibesql_catalog::IndexType::Hnsw { metric: catalog_metric, m, ef_construction },
        vec![vibesql_catalog::IndexedColumn::new_column(
            column_name.to_string(),
            vibesql_catalog::SortOrder::Ascending, // Not meaningful for vector indexes
        )],
        false, // HNSW indexes are never unique
    );
    database.catalog.add_index(index_metadata)?;

    // Create the HNSW index in storage
    database.create_hnsw_index(
        index_name.clone(),
        table_name.to_string(),
        column_name.to_string(),
        col_idx,
        dimensions,
        m,
        ef_construction,
        metric,
    )?;

    // Emit WAL entry for persistence (HNSW indexes are never unique)
    database.emit_wal_create_index(
        index_name_to_id(index_name),
        index_name,
        qualified_table_name,
        vec![col_idx as u32],
        false,
    );

    Ok(format!(
        "HNSW index '{}' created successfully on table '{}' column '{}'",
        index_name, qualified_table_name, column_name
    ))
}

/// Convert AST VectorDistanceMetric to catalog VectorDistanceMetric.
fn convert_metric_to_catalog(metric: VectorDistanceMetric) -> vibesql_catalog::VectorDistanceMetric {
    match metric {
        VectorDistanceMetric::L2 => vibesql_catalog::VectorDistanceMetric::L2,
        VectorDistanceMetric::Cosine => vibesql_catalog::VectorDistanceMetric::Cosine,
        VectorDistanceMetric::InnerProduct => vibesql_catalog::VectorDistanceMetric::InnerProduct,
    }
}
