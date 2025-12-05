//! SIMD filtering optimization using Apache Arrow
//!
//! This module provides vectorized filtering for large datasets using Apache Arrow's
//! columnar format and SIMD operations.

use crate::{
    errors::ExecutorError,
    select::vectorized::{
        filter_record_batch_simd, record_batch_to_rows, rows_to_record_batch, VECTORIZE_THRESHOLD,
    },
};

/// Try to apply SIMD filtering to rows using Apache Arrow
///
/// Returns (rows, used_simd) where:
/// - rows: The filtered rows (or original rows if SIMD not applicable)
/// - used_simd: true if SIMD was used, false if fallback to row-based is needed
///
/// SIMD path is used when:
/// - Row count >= VECTORIZE_THRESHOLD (100 rows)
/// - WHERE clause is a simple predicate (not too complex)
/// - All column types are supported by Arrow (Int64, Float64, Utf8, Boolean)
pub(super) fn try_simd_filter(
    rows: Vec<vibesql_storage::Row>,
    where_expr: &vibesql_ast::Expression,
    schema: &crate::schema::CombinedSchema,
) -> Result<(Vec<vibesql_storage::Row>, bool), ExecutorError> {
    let simd_debug = std::env::var("SIMD_DEBUG").is_ok();

    // Only use SIMD for datasets >= threshold
    if rows.len() < VECTORIZE_THRESHOLD {
        if simd_debug {
            eprintln!("[SIMD] Skipping: {} rows < {} threshold", rows.len(), VECTORIZE_THRESHOLD);
        }
        return Ok((rows, false));
    }

    if simd_debug {
        eprintln!("[SIMD] Attempting SIMD filter on {} rows", rows.len());
    }

    // Extract column names from combined schema (in order)
    let mut column_names = vec![String::new(); schema.total_columns];
    for (start_idx, table_schema) in schema.table_schemas.values() {
        for (col_idx, col) in table_schema.columns.iter().enumerate() {
            column_names[start_idx + col_idx] = col.name.clone();
        }
    }

    // Try to convert rows to RecordBatch
    // If this fails (e.g., unsupported types), fall back to row-based filtering
    let batch = match rows_to_record_batch(&rows, &column_names) {
        Ok(batch) => {
            if simd_debug {
                eprintln!("[SIMD] Successfully converted to RecordBatch");
            }
            batch
        }
        Err(e) => {
            if simd_debug {
                eprintln!("[SIMD] Failed to convert to RecordBatch: {}", e);
            }
            // Conversion failed (unsupported types, etc.) - fall back to row-based
            return Ok((rows, false));
        }
    };

    // Apply SIMD filter
    // If this fails (e.g., complex predicates), fall back to row-based filtering
    let filtered_batch = match filter_record_batch_simd(&batch, where_expr) {
        Ok(filtered) => {
            if simd_debug {
                eprintln!(
                    "[SIMD] Filter succeeded: {} rows -> {} rows",
                    batch.num_rows(),
                    filtered.num_rows()
                );
            }
            filtered
        }
        Err(e) => {
            if simd_debug {
                eprintln!("[SIMD] Filter failed: {}", e);
            }
            // SIMD filter failed (complex predicate, etc.) - fall back to row-based
            return Ok((rows, false));
        }
    };

    // Convert filtered RecordBatch back to rows
    let filtered_rows = record_batch_to_rows(&filtered_batch)?;

    if simd_debug {
        eprintln!("[SIMD] SIMD path completed successfully");
    }

    Ok((filtered_rows, true))
}
