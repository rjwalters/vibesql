//! Utility functions for join reordering

use std::collections::HashMap;
use vibesql_ast::FromClause;
use crate::schema::CombinedSchema;

/// Check if join reordering optimization should be applied
///
/// ## Time-Bounded Search (Default)
///
/// The optimizer uses time-bounded anytime search with a configurable budget
/// (default: 1000ms). This enables optimization for queries of all sizes:
/// - Small queries (2-6 tables): Complete exhaustively in <1ms
/// - Medium queries (7-8 tables): Usually complete within budget
/// - Large queries (9+ tables): Get partial optimization (better than none!)
///
/// The time budget can be configured via JOIN_REORDER_TIME_BUDGET_MS environment variable.
///
/// ## Benefits vs Table-Count Limits
///
/// Previous approach (hard 8-table limit):
/// - 3-8 table joins: Find optimal ordering via exhaustive search with pruning
/// - 9+ table joins: Previously received NO optimization, now get partial
///   optimization within time budget
///
/// The time budget prevents pathological cases while enabling better plans
/// for complex queries that need optimization most.
pub(crate) fn should_apply_join_reordering(table_count: usize) -> bool {
    // Must have at least 2 tables for reordering to be beneficial
    if table_count < 2 {
        return false;
    }

    // Allow opt-out via environment variable if needed
    std::env::var("JOIN_REORDER_DISABLED").is_err()
}

/// Count the number of tables in a FROM clause (including nested joins)
pub(crate) fn count_tables_in_from(from: &FromClause) -> usize {
    match from {
        FromClause::Table { .. } => 1,
        FromClause::Subquery { .. } => 1,
        FromClause::Join { left, right, .. } => {
            count_tables_in_from(left) + count_tables_in_from(right)
        }
    }
}

/// Check if all joins in the tree are CROSS joins (comma-list syntax)
///
/// Join reordering changes column ordering, so we only apply it to implicit CROSS joins
/// from comma-list syntax (FROM t1, t2, t3). Explicit INNER/LEFT/RIGHT joins must
/// preserve their declared ordering.
///
/// Note: CROSS JOINs with ON conditions are NOT valid comma-list syntax and should
/// not be reordered. This ensures `CROSS JOIN ... ON` goes through the normal path
/// where the appropriate error is raised (CROSS JOIN does not support ON clause).
pub(crate) fn all_joins_are_cross(from: &FromClause) -> bool {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } => true,
        FromClause::Join { left, right, join_type, condition, .. } => {
            // Must be CROSS join type AND have no ON condition
            // CROSS JOIN with ON clause is invalid and should not be reordered
            matches!(join_type, vibesql_ast::JoinType::Cross)
                && condition.is_none()
                && all_joins_are_cross(left)
                && all_joins_are_cross(right)
        }
    }
}

/// Get table abbreviation for matching column prefixes
///
/// TPC-H uses abbreviations for compound table names:
/// - "ps" → "partsupp" (partsupp is "part" + "supplier" abbreviated)
/// - "c" → "customer", "l" → "lineitem", etc. (standard prefixes)
///
/// This enables matching abbreviation-style column prefixes (e.g., ps_partkey → partsupp)
pub(super) fn get_table_abbreviation(table_name: &str) -> String {
    let table_lower = table_name.to_lowercase();

    // Known TPC-H table abbreviations
    match table_lower.as_str() {
        "partsupp" => "ps".to_string(),  // Special case: compound abbreviation
        _ => {
            // Default: use first letter as abbreviation
            table_name.chars().next()
                .map(|c| c.to_lowercase().to_string())
                .unwrap_or_default()
        }
    }
}

/// Build a reordered combined schema with tables in original order
///
/// Takes the current schema (with tables in optimal order) and reconstructs it
/// with tables in the original FROM clause order.
pub(super) fn build_reordered_schema(
    current_schema: &CombinedSchema,
    original_order: &[String],
    _optimal_order: &[String],
) -> CombinedSchema {
    let mut new_table_schemas = HashMap::new();
    let mut current_position = 0;

    // Walk through original order and rebuild schema with correct positions
    for table_name in original_order {
        let table_lower = table_name.to_lowercase();

        // Find this table's schema in the current (optimally ordered) schema
        // Try exact match first, then case-insensitive
        let table_schema = current_schema
            .table_schemas
            .get(table_name)
            .or_else(|| {
                current_schema.table_schemas.iter().find_map(|(k, v): (&String, &(usize, vibesql_catalog::TableSchema))| {
                    if k.to_lowercase() == table_lower {
                        Some(v)
                    } else {
                        None
                    }
                })
            })
            .map(|(_, schema): &(usize, vibesql_catalog::TableSchema)| schema.clone());

        if let Some(schema) = table_schema {
            let col_count = schema.columns.len();
            new_table_schemas.insert(table_name.clone(), (current_position, schema));
            current_position += col_count;
        }
    }

    CombinedSchema { table_schemas: new_table_schemas, total_columns: current_position }
}

/// Build a column permutation to restore original table ordering
///
/// Given:
/// - Original table order: [tab0, tab2, tab1]
/// - Optimal execution order: [tab1, tab0, tab2]
/// - Column counts: {tab0: 3, tab1: 3, tab2: 3}
///
/// Returns permutation mapping current positions to original positions:
/// - Current: [tab1.col0, tab1.col1, tab1.col2, tab0.col0, tab0.col1, tab0.col2, tab2.col0, tab2.col1, tab2.col2]
/// - Target:  [tab0.col0, tab0.col1, tab0.col2, tab2.col0, tab2.col1, tab2.col2, tab1.col0, tab1.col1, tab1.col2]
/// - Permutation: [3, 4, 5, 6, 7, 8, 0, 1, 2]
pub(super) fn build_column_permutation(
    original_order: &[String],
    optimal_order: &[String],
    column_counts: &HashMap<String, usize>,
) -> Vec<usize> {
    // Build position map: table name -> starting column index in optimal order
    let mut optimal_positions: HashMap<String, usize> = HashMap::new();
    let mut current_position = 0;
    for table in optimal_order {
        optimal_positions.insert(table.clone(), current_position);
        current_position += column_counts.get(table).unwrap_or(&0);
    }

    // Build permutation by walking through original order
    let mut permutation = Vec::new();
    for table in original_order {
        let table_lower = table.to_lowercase();
        let start_pos = optimal_positions.get(&table_lower).unwrap_or(&0);
        let col_count = column_counts.get(&table_lower).unwrap_or(&0);

        // Add all column indices for this table
        for i in 0..*col_count {
            permutation.push(start_pos + i);
        }
    }

    permutation
}
