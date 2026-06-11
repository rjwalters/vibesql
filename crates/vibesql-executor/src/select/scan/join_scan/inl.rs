//! Index Nested Loop (INL) join implementation
//!
//! This module implements the Index Nested Loop join strategy for SEMI joins.
//! INL is used when the left side is small and an index exists on the right table.

use std::collections::{HashMap, HashSet};

use ahash::AHashSet;

use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator,
    optimizer::where_pushdown::flatten_conjuncts, schema::CombinedSchema,
};

use super::semi_anti::parse_semi_join_condition;

/// Slot in a lookup key template
#[derive(Debug, Clone)]
pub(super) enum KeySlot {
    Constant(vibesql_types::SqlValue),
    JoinKey,
}

/// Try to execute a SEMI join using Index Nested Loop (INL) strategy.
///
/// This optimization is used when:
/// 1. The left side is small (< INL_BASE_THRESHOLD rows)
/// 2. The right side is a simple table (not a subquery or join)
/// 3. There's an equi-join condition on a column with an index
///
/// For each distinct join key from the left side, we do point lookups on the
/// right table instead of scanning all matching rows.
///
/// Returns Some(result) if INL was used successfully, None to fall back to hash join.
pub(super) fn try_index_nested_loop_semi_join(
    left_result: &super::super::FromResult,
    right_from: &vibesql_ast::FromClause,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
) -> Result<Option<super::super::FromResult>, ExecutorError> {
    // Must have a join condition
    let cond = match condition {
        Some(c) => c,
        None => return Ok(None),
    };

    // Right side must be a simple table (not a join or subquery)
    let (right_table_name, _right_alias) = match right_from {
        vibesql_ast::FromClause::Table { name, alias, .. } => (name.clone(), alias.clone()),
        _ => return Ok(None), // Complex right side, can't use INL
    };

    // Get the right table
    let right_table = match database.get_table(&right_table_name) {
        Some(t) => t,
        None => return Ok(None),
    };

    // Parse the join condition to extract:
    // 1. The equi-join columns (left_col = right_col)
    // 2. Additional filter predicates on the right table
    let (equi_join, right_filters) =
        match parse_semi_join_condition(cond, left_result, &right_table_name) {
            Some(parsed) => parsed,
            None => return Ok(None),
        };

    // Check if the right table has a usable index for point lookups
    // We need an index that starts with the join key column
    let pk_columns = match &right_table.schema.primary_key {
        Some(cols) => cols.clone(),
        None => return Ok(None), // No primary key, can't use INL
    };

    // For TPC-C Stock-Level: STOCK has PK (s_w_id, s_i_id)
    // The join is on s_i_id, but we also have s_w_id = ? in the filter
    // So we can build a composite key for point lookups

    // Try to extract constant predicates from right_filters that can be combined with join key
    let constant_prefix =
        extract_constant_prefix_for_pk(&right_filters, &pk_columns, &equi_join.right_col);

    // Build the index lookup key template
    // For Stock-Level: key = [s_w_id (from filter), s_i_id (from join)]
    let lookup_key_template =
        match build_lookup_key_template(&pk_columns, &equi_join.right_col, &constant_prefix) {
            Some(template) => template,
            None => return Ok(None),
        };

    // Get the primary key index
    let pk_index = match right_table.primary_key_index() {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // Get left column index for extracting join keys
    let left_col_idx = match find_column_index(&left_result.schema, &equi_join.left_col) {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // Build evaluator for right-side filter (residual predicates after PK lookup)
    let right_schema =
        CombinedSchema::from_table(right_table_name.clone(), right_table.schema.clone());
    let residual_filter = build_residual_filter(&right_filters, &pk_columns);
    let evaluator = residual_filter
        .as_ref()
        .map(|_| CombinedExpressionEvaluator::with_database(&right_schema, database));

    // Collect distinct join keys from left side
    let left_slice = left_result.as_slice();
    let mut seen_keys: AHashSet<vibesql_types::SqlValue> =
        AHashSet::with_capacity(left_slice.len());
    let mut matching_keys: AHashSet<vibesql_types::SqlValue> = AHashSet::new();

    for left_row in left_slice {
        let join_key = &left_row.values[left_col_idx];

        // Skip NULL join keys
        if *join_key == vibesql_types::SqlValue::Null {
            continue;
        }

        // Skip duplicate keys (we only need one match for SEMI join)
        if seen_keys.contains(join_key) {
            continue;
        }
        seen_keys.insert(join_key.clone());

        // Build the full lookup key
        let lookup_key: Vec<vibesql_types::SqlValue> = lookup_key_template
            .iter()
            .map(|slot| match slot {
                KeySlot::Constant(v) => v.clone(),
                KeySlot::JoinKey => join_key.clone(),
            })
            .collect();

        // Do point lookup
        // Issue #3790: Use get_row() which returns None for deleted rows
        if let Some(&row_idx) = pk_index.get(&lookup_key) {
            let right_row = match right_table.get_row(row_idx) {
                Some(row) => row,
                None => continue, // Row deleted or invalid
            };

            // Apply residual filter if any
            let passes = if let (Some(filter), Some(eval)) = (&residual_filter, &evaluator) {
                eval.clear_cse_cache();
                matches!(eval.eval(filter, right_row), Ok(vibesql_types::SqlValue::Boolean(true)))
            } else {
                true // No residual filter
            };

            if passes {
                matching_keys.insert(join_key.clone());
            }
        }
    }

    // Build result: all left rows whose join key is in matching_keys
    let result_rows: Vec<vibesql_storage::Row> = left_slice
        .iter()
        .filter(|row| {
            let key = &row.values[left_col_idx];
            matching_keys.contains(key)
        })
        .cloned()
        .collect();

    Ok(Some(super::super::FromResult::from_rows(left_result.schema.clone(), result_rows)))
}

/// Prefix-scan based semi-join for cases where an index exists but not all key columns
/// are covered by the join condition.
///
/// This handles cases like TPC-H Q4:
/// - Join on `o_orderkey = l_orderkey`
/// - Index on `(l_orderkey, l_linenumber)`
/// - Additional filter: `l_commitdate < l_receiptdate`
///
/// Instead of building a hash table from 60K lineitem rows, we do ~500 prefix scans
/// (one per qualifying order), which is much faster when the left side is small.
pub(super) fn try_prefix_scan_semi_join(
    left_result: &super::super::FromResult,
    right_from: &vibesql_ast::FromClause,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
) -> Result<Option<super::super::FromResult>, ExecutorError> {
    let debug = std::env::var("PREFIX_SEMI_DEBUG").is_ok();

    // Must have a join condition
    let cond = match condition {
        Some(c) => c,
        None => return Ok(None),
    };

    // Right side must be a simple table (not a join or subquery)
    let (right_table_name, _right_alias) = match right_from {
        vibesql_ast::FromClause::Table { name, alias, .. } => (name.clone(), alias.clone()),
        _ => return Ok(None),
    };

    // Get the right table
    let right_table = match database.get_table(&right_table_name) {
        Some(t) => t,
        None => return Ok(None),
    };

    // Parse the join condition to extract equi-join columns and additional filters
    let (equi_join, right_filters) =
        match parse_semi_join_condition(cond, left_result, &right_table_name) {
            Some(parsed) => parsed,
            None => return Ok(None),
        };

    if debug {
        eprintln!(
            "[PREFIX_SEMI] left_rows={}, join on {}={}, right_filters={:?}",
            left_result.as_slice().len(),
            equi_join.left_col,
            equi_join.right_col,
            right_filters
        );
    }

    // Find an index on the right table that starts with the join key column
    let right_col_upper = equi_join.right_col.to_lowercase();
    let index_names = database.list_indexes_for_table(&right_table_name);

    // Find an index where the first column is the join key
    let mut usable_index_name: Option<String> = None;
    for index_name in &index_names {
        if let Some(idx_metadata) = database.get_index(index_name) {
            if let Some(first_col) = idx_metadata.columns.first() {
                if first_col.expect_column_name().to_lowercase() == right_col_upper {
                    // Partial indexes (CREATE INDEX ... WHERE expr) are usable
                    // only when the right-side filters structurally imply the
                    // index predicate: since PR #5323 the index body excludes
                    // predicate-false rows, so an ungated probe would silently
                    // drop semi-join matches (issue #5330). The right-side
                    // filters are sound implication context because every
                    // probed row is post-filtered with them below — a row
                    // missing from the index body fails the (implied) filters
                    // and could never have produced a match anyway. With no
                    // right-side filters, partial indexes are skipped outright.
                    if !crate::optimizer::predicate_implication::partial_index_usable(
                        database,
                        index_name,
                        right_filters.as_ref(),
                    ) {
                        continue;
                    }
                    usable_index_name = Some(index_name.clone());
                    break;
                }
            }
        }
    }

    let index_name = match usable_index_name {
        Some(name) => name,
        None => {
            if debug {
                eprintln!(
                    "[PREFIX_SEMI] No index starting with {} on {}",
                    equi_join.right_col, right_table_name
                );
            }
            return Ok(None);
        }
    };

    let index = match database.get_index_data(&index_name) {
        Some(idx) => idx,
        None => return Ok(None),
    };

    if debug {
        eprintln!(
            "[PREFIX_SEMI] Using index {} for prefix scan on {}",
            index_name, right_table_name
        );
    }

    // Get left column index for extracting join keys
    let left_col_idx = match find_column_index(&left_result.schema, &equi_join.left_col) {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // Build evaluator for right-side filter (e.g., l_commitdate < l_receiptdate)
    let right_schema =
        CombinedSchema::from_table(right_table_name.clone(), right_table.schema.clone());
    let evaluator = right_filters
        .as_ref()
        .map(|_| CombinedExpressionEvaluator::with_database(&right_schema, database));

    // Collect distinct join keys from left side
    let left_slice = left_result.as_slice();
    let mut seen_keys: AHashSet<vibesql_types::SqlValue> =
        AHashSet::with_capacity(left_slice.len());
    let mut matching_keys: AHashSet<vibesql_types::SqlValue> = AHashSet::new();

    let start = std::time::Instant::now();

    for left_row in left_slice {
        let join_key = &left_row.values[left_col_idx];

        // Skip NULL join keys
        if *join_key == vibesql_types::SqlValue::Null {
            continue;
        }

        // Skip duplicate keys (we only need one match for SEMI join)
        if seen_keys.contains(join_key) {
            continue;
        }
        seen_keys.insert(join_key.clone());

        // Do prefix lookup: find all rows where the first index column = join_key
        let row_indices = index.prefix_multi_lookup(std::slice::from_ref(join_key));

        // Check if any of the matching rows pass the additional filter
        for row_idx in row_indices {
            // Get the row data
            let right_row = match right_table.get_row(row_idx) {
                Some(row) => row,
                None => continue, // Row deleted or invalid
            };

            // Apply filter if any
            let passes = if let (Some(filter), Some(eval)) = (&right_filters, &evaluator) {
                eval.clear_cse_cache();
                matches!(eval.eval(filter, right_row), Ok(vibesql_types::SqlValue::Boolean(true)))
            } else {
                true // No filter
            };

            if passes {
                matching_keys.insert(join_key.clone());
                break; // Semi-join: we only need one match per left row
            }
        }
    }

    if debug {
        let elapsed = start.elapsed();
        eprintln!(
            "[PREFIX_SEMI] {} distinct keys, {} matches, {:?} elapsed",
            seen_keys.len(),
            matching_keys.len(),
            elapsed
        );
    }

    // Build result: all left rows whose join key is in matching_keys
    let result_rows: Vec<vibesql_storage::Row> = left_slice
        .iter()
        .filter(|row| {
            let key = &row.values[left_col_idx];
            matching_keys.contains(key)
        })
        .cloned()
        .collect();

    Ok(Some(super::super::FromResult::from_rows(left_result.schema.clone(), result_rows)))
}

/// Extract constant values from right filters that match PK columns.
/// Returns a map from column name to constant value.
fn extract_constant_prefix_for_pk(
    right_filters: &Option<vibesql_ast::Expression>,
    pk_columns: &[String],
    join_key_col: &str,
) -> HashMap<String, vibesql_types::SqlValue> {
    let mut constants: HashMap<String, vibesql_types::SqlValue> = HashMap::new();

    let filter = match right_filters {
        Some(f) => f,
        None => return constants,
    };

    // Flatten and look for equality predicates with constants
    let conjuncts = flatten_conjuncts(filter);
    let join_key_upper = join_key_col.to_lowercase();

    for pred in conjuncts {
        if let vibesql_ast::Expression::BinaryOp {
            left,
            op: vibesql_ast::BinaryOperator::Equal,
            right,
        } = &pred
        {
            // Check if left is column and right is literal
            if let (
                vibesql_ast::Expression::ColumnRef(col_id),
                vibesql_ast::Expression::Literal(value),
            ) = (left.as_ref(), right.as_ref())
            {
                let column = col_id.column_canonical();
                let col_upper = column.to_lowercase();
                // Only add if it's a PK column and not the join key
                if pk_columns.iter().any(|pk| pk.to_lowercase() == col_upper)
                    && col_upper != join_key_upper
                {
                    constants.insert(col_upper, value.clone());
                }
            }

            // Check reverse: right is column and left is literal
            if let (
                vibesql_ast::Expression::Literal(value),
                vibesql_ast::Expression::ColumnRef(col_id),
            ) = (left.as_ref(), right.as_ref())
            {
                let column = col_id.column_canonical();
                let col_upper = column.to_lowercase();
                if pk_columns.iter().any(|pk| pk.to_lowercase() == col_upper)
                    && col_upper != join_key_upper
                {
                    constants.insert(col_upper, value.clone());
                }
            }
        }
    }

    constants
}

/// Build a lookup key template based on PK columns.
fn build_lookup_key_template(
    pk_columns: &[String],
    join_key_col: &str,
    constant_prefix: &HashMap<String, vibesql_types::SqlValue>,
) -> Option<Vec<KeySlot>> {
    let join_key_upper = join_key_col.to_lowercase();
    let mut template = Vec::with_capacity(pk_columns.len());

    for pk_col in pk_columns {
        let pk_upper = pk_col.to_lowercase();

        if pk_upper == join_key_upper {
            template.push(KeySlot::JoinKey);
        } else if let Some(value) = constant_prefix.get(&pk_upper) {
            template.push(KeySlot::Constant(value.clone()));
        } else {
            // Missing a PK component, can't use INL
            return None;
        }
    }

    Some(template)
}

/// Build residual filter (predicates not covered by PK lookup)
fn build_residual_filter(
    right_filters: &Option<vibesql_ast::Expression>,
    pk_columns: &[String],
) -> Option<vibesql_ast::Expression> {
    let filter = match right_filters {
        Some(f) => f,
        None => return None,
    };

    let pk_upper: HashSet<String> = pk_columns.iter().map(|s| s.to_lowercase()).collect();
    let conjuncts = flatten_conjuncts(filter);

    // Keep predicates that are NOT equality on PK columns
    let residual: Vec<vibesql_ast::Expression> = conjuncts
        .into_iter()
        .filter(|pred| {
            // Check if this is an equality predicate on a PK column
            if let vibesql_ast::Expression::BinaryOp {
                left,
                op: vibesql_ast::BinaryOperator::Equal,
                right,
            } = pred
            {
                // Check if it's col = literal or literal = col
                if let vibesql_ast::Expression::ColumnRef(col_id) = left.as_ref() {
                    if pk_upper.contains(&col_id.column_canonical().to_lowercase())
                        && matches!(right.as_ref(), vibesql_ast::Expression::Literal(_))
                    {
                        return false; // Filter out, covered by PK lookup
                    }
                }
                if let vibesql_ast::Expression::ColumnRef(col_id) = right.as_ref() {
                    if pk_upper.contains(&col_id.column_canonical().to_lowercase())
                        && matches!(left.as_ref(), vibesql_ast::Expression::Literal(_))
                    {
                        return false; // Filter out, covered by PK lookup
                    }
                }
            }
            true // Keep this predicate
        })
        .collect();

    crate::optimizer::combine_with_and(residual)
}

/// Find the index of a column in the schema
fn find_column_index(schema: &CombinedSchema, col_name: &str) -> Option<usize> {
    let col_upper = col_name.to_lowercase();
    let mut offset = 0;

    for (_, table_schema) in schema.table_schemas.values() {
        for (idx, col) in table_schema.columns.iter().enumerate() {
            if col.name.to_lowercase() == col_upper {
                return Some(offset + idx);
            }
        }
        offset += table_schema.columns.len();
    }

    None
}
