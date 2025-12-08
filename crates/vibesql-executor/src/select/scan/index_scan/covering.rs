//! Covering index scan execution
//!
//! Implements index-only scans where all SELECT columns are part of the index key.
//! This eliminates table fetches entirely, reading data directly from the index.
//!
//! # Performance Impact
//!
//! For queries like TPC-C Stock-Level:
//! ```sql
//! SELECT s_i_id FROM stock WHERE s_w_id = ? AND s_quantity < ?
//! ```
//! With index `idx_stock_quantity(s_w_id, s_quantity, s_i_id)`:
//! - Without covering scan: O(log n + k) index lookup + k table fetches
//! - With covering scan: O(log n + k) index lookup only (no table access)
//!
//! This is critical for queries where:
//! - k (matching rows) is large (e.g., ~300 for Stock-Level)
//! - Only a subset of columns are needed (just s_i_id)
//! - Those columns happen to be in the index key

use std::collections::HashMap;

use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use crate::{
    errors::ExecutorError,
    schema::CombinedSchema,
    select::{cte::CteResult, scan::FromResult},
};

use super::predicate::{
    extract_prefix_equality_predicates, extract_prefix_with_trailing_range, PrefixWithRangeResult,
};

/// Result of checking if an index covers all needed columns
#[derive(Debug)]
pub(crate) struct CoveringIndexInfo {
    /// Mapping from needed column name to its position in the index key
    pub column_to_key_position: HashMap<String, usize>,
}

/// Check if an index covers all the needed columns
///
/// Returns Some(CoveringIndexInfo) if the index key contains all needed columns,
/// None otherwise.
///
/// # Arguments
/// * `index_column_names` - Column names in the index key (in order)
/// * `needed_columns` - Column names needed for the SELECT result
///
/// # Example
/// ```rust,no_run
/// // This is a crate-internal function, example for documentation only
/// // Index: (s_w_id, s_quantity, s_i_id)
/// // Needed: [s_i_id]
/// // let info = check_covering_index(&["s_w_id", "s_quantity", "s_i_id"], &["s_i_id"]);
/// // assert!(info.is_some());
/// // assert_eq!(info.unwrap().column_to_key_position["s_i_id"], 2);
/// ```
pub(crate) fn check_covering_index(
    index_column_names: &[&str],
    needed_columns: &[String],
) -> Option<CoveringIndexInfo> {
    let mut column_to_key_position = HashMap::new();

    for needed_col in needed_columns {
        let needed_lower = needed_col.to_ascii_lowercase();
        let mut found = false;
        for (pos, &index_col) in index_column_names.iter().enumerate() {
            if index_col.to_ascii_lowercase() == needed_lower {
                column_to_key_position.insert(needed_col.clone(), pos);
                found = true;
                break;
            }
        }
        if !found {
            // Column not in index - can't use covering scan
            return None;
        }
    }

    Some(CoveringIndexInfo { column_to_key_position })
}

/// Execute a covering index scan (index-only scan)
///
/// This function retrieves rows directly from index keys without accessing the table.
/// It should only be called when `check_covering_index` returns Some.
///
/// # Arguments
/// * `table_name` - Name of the table
/// * `index_name` - Name of the index to use
/// * `where_clause` - Optional WHERE clause expression
/// * `needed_columns` - Columns needed for SELECT (must be covered by index)
/// * `covering_info` - Mapping from column names to index key positions
/// * `database` - Database reference
/// * `_cte_results` - CTE context (unused for covering scans)
///
/// # Returns
/// Result rows constructed directly from index keys, with schema containing
/// only the needed columns.
pub(super) fn execute_covering_index_scan(
    table_name: &str,
    index_name: &str,
    alias: Option<&String>,
    where_clause: Option<&vibesql_ast::Expression>,
    needed_columns: &[String],
    covering_info: &CoveringIndexInfo,
    database: &Database,
    _cte_results: &HashMap<String, CteResult>,
) -> Result<FromResult, ExecutorError> {
    // Get index metadata and data
    let index_metadata = database
        .get_index(index_name)
        .ok_or_else(|| ExecutorError::IndexNotFound(index_name.to_string()))?;

    let index_data = database
        .get_index_data(index_name)
        .ok_or_else(|| ExecutorError::IndexNotFound(index_name.to_string()))?;

    // Get table schema for building result schema
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    // Get index column names for predicate extraction
    let index_column_names: Vec<&str> =
        index_metadata.columns.iter().map(|col| col.column_name.as_str()).collect();

    // Try to extract prefix + trailing range (most common pattern for covering scans)
    // e.g., WHERE s_w_id = 1 AND s_quantity < 10
    let prefix_with_range: Option<PrefixWithRangeResult> =
        where_clause.and_then(|expr| extract_prefix_with_trailing_range(expr, &index_column_names));

    // Try prefix-only if no range found
    let prefix_only = if prefix_with_range.is_none() {
        where_clause.and_then(|expr| extract_prefix_equality_predicates(expr, &index_column_names))
    } else {
        None
    };

    // Execute covering scan based on predicate type
    let covering_results: Vec<(Vec<SqlValue>, Vec<usize>)> = if let Some(ref pr) = prefix_with_range
    {
        // Prefix + range scan
        index_data.prefix_range_scan_covering(
            &pr.prefix_key,
            pr.lower_bound.as_ref(),
            pr.inclusive_lower,
            pr.upper_bound.as_ref(),
            pr.inclusive_upper,
        )
    } else if let Some(ref po) = prefix_only {
        // Prefix-only scan
        index_data.prefix_scan_covering(&po.prefix_key)
    } else {
        // No suitable predicate found - fall back to empty
        // (Caller should check this case and use regular scan instead)
        Vec::new()
    };

    // Build rows from index keys
    // For each (key_values, row_indices), we create one row per row_index
    // but all rows share the same key values
    let mut rows = Vec::new();

    for (key_values, row_indices) in covering_results {
        // For covering scans, we don't need row_indices since we're reading
        // all data from the key. But we need one output row per index entry.
        // Note: For non-unique indexes, row_indices.len() may be > 1
        let _row_count = row_indices.len();

        // Extract only the needed column values from the key
        // The key contains all indexed columns in order
        let projected_values: Vec<SqlValue> = needed_columns
            .iter()
            .map(|col| {
                let key_pos = covering_info.column_to_key_position[col];
                key_values[key_pos].clone()
            })
            .collect();

        // Create one row per row_index (handles non-unique indexes)
        for _ in row_indices {
            rows.push(Row::from_vec(projected_values.clone()));
        }
    }

    // Build schema with only the needed columns
    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let mut result_columns = Vec::new();

    for col_name in needed_columns {
        // Find column in table schema
        if let Some(col_schema) =
            table.schema.columns.iter().find(|c| c.name.eq_ignore_ascii_case(col_name))
        {
            result_columns.push(col_schema.clone());
        }
    }

    // Create schema with only the projected columns
    let result_schema =
        vibesql_catalog::TableSchema::new(effective_name.clone(), result_columns);

    let schema = CombinedSchema::from_table(effective_name, result_schema);

    // Return result marked as WHERE-filtered since index handled the predicate
    Ok(FromResult::from_rows_where_filtered(schema, rows, None))
}

/// Try to execute a covering index scan if possible
///
/// This is a convenience function that checks if a covering scan is possible
/// and executes it if so. Returns None if covering scan is not applicable.
///
/// # When this returns Some:
/// - The index covers all needed columns
/// - A suitable prefix/range predicate was extracted
/// - Rows were successfully constructed from index keys
///
/// # When this returns None:
/// - The index doesn't cover all needed columns
/// - No suitable predicate could be extracted
/// - Caller should fall back to regular index scan
pub(in crate::select) fn try_covering_index_scan(
    table_name: &str,
    index_name: &str,
    alias: Option<&String>,
    where_clause: Option<&vibesql_ast::Expression>,
    needed_columns: &[String],
    database: &Database,
    cte_results: &HashMap<String, CteResult>,
) -> Result<Option<FromResult>, ExecutorError> {
    // Get index metadata
    let index_metadata = match database.get_index(index_name) {
        Some(m) => m,
        None => return Ok(None),
    };

    // Get index column names
    let index_column_names: Vec<&str> =
        index_metadata.columns.iter().map(|col| col.column_name.as_str()).collect();

    // Check if index covers all needed columns
    let covering_info = match check_covering_index(&index_column_names, needed_columns) {
        Some(info) => info,
        None => return Ok(None),
    };

    // Issue #1618: If there's a WHERE clause, we must be able to push it down to this index.
    // Otherwise, we'd return wrong results (0 rows when predicate doesn't match index columns).
    // A covering scan without predicate pushdown would require post-filtering which defeats
    // the purpose and can silently return incorrect results.
    if let Some(expr) = where_clause {
        let prefix_with_range = extract_prefix_with_trailing_range(expr, &index_column_names);
        let prefix_only = if prefix_with_range.is_none() {
            extract_prefix_equality_predicates(expr, &index_column_names)
        } else {
            None
        };

        // If neither predicate extraction worked, this index can't be used for covering scan
        if prefix_with_range.is_none() && prefix_only.is_none() {
            return Ok(None);
        }
    }

    // Execute covering scan
    let result = execute_covering_index_scan(
        table_name,
        index_name,
        alias,
        where_clause,
        needed_columns,
        &covering_info,
        database,
        cte_results,
    )?;

    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_covering_index_all_columns_present() {
        let index_cols = vec!["s_w_id", "s_quantity", "s_i_id"];
        let needed = vec!["s_i_id".to_string()];

        let result = check_covering_index(&index_cols, &needed);
        assert!(result.is_some());

        let info = result.unwrap();
        assert_eq!(info.column_to_key_position["s_i_id"], 2);
    }

    #[test]
    fn test_check_covering_index_multiple_columns() {
        let index_cols = vec!["s_w_id", "s_quantity", "s_i_id"];
        let needed = vec!["s_w_id".to_string(), "s_i_id".to_string()];

        let result = check_covering_index(&index_cols, &needed);
        assert!(result.is_some());

        let info = result.unwrap();
        assert_eq!(info.column_to_key_position["s_w_id"], 0);
        assert_eq!(info.column_to_key_position["s_i_id"], 2);
    }

    #[test]
    fn test_check_covering_index_missing_column() {
        let index_cols = vec!["s_w_id", "s_quantity"];
        let needed = vec!["s_i_id".to_string()]; // Not in index

        let result = check_covering_index(&index_cols, &needed);
        assert!(result.is_none());
    }

    #[test]
    fn test_check_covering_index_case_insensitive() {
        let index_cols = vec!["S_W_ID", "S_QUANTITY", "S_I_ID"];
        let needed = vec!["s_i_id".to_string()];

        let result = check_covering_index(&index_cols, &needed);
        assert!(result.is_some());
    }
}
