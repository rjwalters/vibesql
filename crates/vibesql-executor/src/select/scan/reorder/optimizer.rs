//! Main join reordering optimization logic

use super::{graph, predicates, utils};
use crate::{
    errors::ExecutorError,
    schema::CombinedSchema,
    select::{
        cte::CteResult,
        join::{nested_loop_join, JoinOrderAnalyzer, JoinOrderSearch},
        scan::{derived::execute_derived_table, table::execute_table_scan, FromResult},
        SelectResult,
    },
    timeout::TimeoutContext,
};
use std::collections::{HashMap, HashSet};
use vibesql_ast::{Expression, FromClause};

/// Check if join profiling is enabled via environment variable
fn join_profile_enabled() -> bool {
    std::env::var("JOIN_PROFILE").is_ok()
}

/// Apply join reordering optimization to a multi-table join
///
/// This function:
/// 1. Flattens the join tree to extract all tables
/// 2. Analyzes join conditions and WHERE predicates
/// 3. Uses cost-based search to find optimal join order
/// 4. Builds and executes joins in the optimal order
/// 5. Restores original column ordering to preserve query semantics
pub(crate) fn execute_with_join_reordering<F>(
    from: &FromClause,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&Expression>,
    outer_row: Option<&vibesql_storage::Row>,
    outer_schema: Option<&CombinedSchema>,
    execute_subquery: F,
) -> Result<FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<SelectResult, ExecutorError> + Copy,
{
    // Step 1: Flatten join tree to extract all tables
    let mut table_refs = Vec::new();
    graph::flatten_join_tree(from, &mut table_refs);

    // Step 2: Extract all join conditions with their types
    let mut join_conditions = Vec::new();
    let mut join_conditions_with_types = Vec::new();
    graph::extract_all_conditions(from, &mut join_conditions);
    graph::extract_conditions_with_types(from, &mut join_conditions_with_types);

    // Step 3: Build analyzer with table names (preserving original order)
    let table_names: Vec<String> =
        table_refs.iter().map(|t| t.alias.clone().unwrap_or_else(|| t.name.clone())).collect();

    // Step 3.5: Build schema-based column-to-table mapping
    // This uses actual database schema to resolve unqualified column references
    let column_to_table =
        utils::build_column_to_table_map(database, &table_names, &table_refs, cte_results);

    // Create analyzer with schema-based column resolution
    let mut analyzer = JoinOrderAnalyzer::with_column_map(column_to_table.clone());
    analyzer.register_tables(table_names.clone());

    // Combine table names into a set for predicate analysis (normalize to lowercase)
    let table_set: HashSet<String> = table_names.iter().map(|t| t.to_lowercase()).collect();

    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
        eprintln!(
            "[JOIN_REORDER] Schema-based column mapping: {} columns resolved from {} tables",
            column_to_table.len(),
            table_names.len()
        );
        if column_to_table.is_empty() && !table_names.is_empty() {
            eprintln!(
                "[JOIN_REORDER] Warning: No schema columns found for tables: {:?}",
                table_names
            );
        }
    }

    // Step 4: Analyze join conditions to extract edges with their join types
    for condition_with_type in &join_conditions_with_types {
        analyzer.analyze_predicate_with_type(
            &condition_with_type.condition,
            &table_set,
            condition_with_type.join_type.clone(),
        );
    }

    // Step 5: Analyze WHERE clause predicates if available
    // Also extract WHERE clause equijoins for join execution
    let where_equijoins = if let Some(where_expr) = where_clause {
        analyzer.analyze_predicate(where_expr, &table_set);

        // Debug logging
        if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
            eprintln!("[JOIN_REORDER] WHERE clause present: {:?}", where_expr);
            eprintln!("[JOIN_REORDER] Table set: {:?}", table_set);
        }

        // Extract equijoin conditions from WHERE clause using schema-based column resolution
        let equijoins = predicates::extract_where_equijoins_with_schema(
            where_expr,
            &table_set,
            &column_to_table,
        );

        if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
            eprintln!("[JOIN_REORDER] Extracted {} WHERE equijoins", equijoins.len());
        }

        equijoins
    } else {
        if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
            eprintln!("[JOIN_REORDER] No WHERE clause");
        }
        Vec::new()
    };

    // Step 6: Add WHERE equijoins to join_conditions for execution
    // This ensures WHERE clause equijoins are used during join execution, not just for optimization
    join_conditions.extend(where_equijoins);

    // Step 6.5: Extract table-local predicates for cardinality estimation
    // Use schema-based column resolution to handle unqualified columns like `p_name LIKE '%green%'`
    let mut table_local_predicates = if let Some(where_expr) = where_clause {
        predicates::extract_table_local_predicates_with_schema(
            where_expr,
            &table_set,
            &column_to_table,
        )
    } else {
        HashMap::new()
    };

    // Also extract IN predicates from OR expressions
    if let Some(where_expr) = where_clause {
        for (table, preds) in predicates::extract_in_predicates_from_or(where_expr, &table_set) {
            table_local_predicates.entry(table).or_default().extend(preds);
        }
    }

    // Extract common single-table predicates from OR branches (e.g., TPC-H Q19)
    // This handles cases like `l_shipmode IN ('AIR', 'AIR REG')` that appear in all OR branches
    if let Some(where_expr) = where_clause {
        for (table, preds) in predicates::extract_common_or_predicates_with_schema(
            where_expr,
            &table_set,
            &column_to_table,
        ) {
            table_local_predicates.entry(table).or_default().extend(preds);
        }
    }

    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() && !table_local_predicates.is_empty() {
        eprintln!(
            "[JOIN_REORDER] Table-local predicates: {:?}",
            table_local_predicates.keys().collect::<Vec<_>>()
        );
    }

    // Step 6.6: Build alias-to-table mapping for cardinality estimation
    // This is critical for queries with table aliases (e.g., "nation n1, nation n2" in TPC-H Q7)
    // where we need to resolve the alias to the actual table name for database lookups
    let alias_to_table: HashMap<String, String> = table_refs
        .iter()
        .map(|t| {
            let key = t.alias.clone().unwrap_or_else(|| t.name.clone()).to_lowercase();
            (key, t.name.clone())
        })
        .collect();

    // Step 7: Use search to find optimal join order (with real statistics + selectivity)
    let optimizer_start = std::time::Instant::now();
    let search = JoinOrderSearch::from_analyzer_with_predicates(
        &analyzer,
        database,
        &table_local_predicates,
        &alias_to_table,
    );
    let optimal_order = search.find_optimal_order();
    let optimizer_time = optimizer_start.elapsed();

    // Log the reordering decision (optional, for debugging)
    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
        eprintln!("[JOIN_REORDER] Original order: {:?}", table_names);
        eprintln!("[JOIN_REORDER] Optimal order:  {:?}", optimal_order);
        eprintln!(
            "[JOIN_REORDER] Join conditions (including WHERE equijoins): {}",
            join_conditions.len()
        );
    }

    // Profiling: Track times for each phase
    let profile = join_profile_enabled();
    let mut scan_times: Vec<(String, std::time::Duration)> = Vec::new();
    let mut join_times: Vec<(String, std::time::Duration, usize, usize)> = Vec::new();
    if profile {
        eprintln!("[JOIN_PROFILE] Optimizer time: {:?}", optimizer_time);
    }

    // Step 8: Build a map from table name to TableRef for easy lookup
    // IMPORTANT: Normalize keys to lowercase to match analyzer's normalization
    let table_map: HashMap<String, graph::TableRef> = table_refs
        .into_iter()
        .map(|t| {
            let key = t.alias.clone().unwrap_or_else(|| t.name.clone()).to_lowercase();
            (key, t)
        })
        .collect();

    // Step 9: Track column count per table for later column reordering
    let mut table_column_counts: HashMap<String, usize> = HashMap::new();

    // Step 10: Execute tables in optimal order, joining them sequentially
    let mut result: Option<FromResult> = None;
    let mut joined_tables: HashSet<String> = HashSet::new();
    let mut applied_conditions: HashSet<usize> = HashSet::new();

    for table_name in &optimal_order {
        let table_ref = table_map.get(table_name).ok_or_else(|| {
            ExecutorError::UnsupportedFeature(format!("Table not found in map: {}", table_name))
        })?;

        // Execute this table with table-local predicates for early filtering
        // Build a combined predicate from table-local predicates for this specific table
        // Use the actual table name (table_ref.name) for column qualification so that
        // PredicatePlan can correctly identify the predicates as table-local
        let table_filter = table_local_predicates
            .get(&table_name.to_lowercase())
            .and_then(|preds| utils::combine_predicates_with_qualification(preds, &table_ref.name));

        let scan_start = std::time::Instant::now();
        let table_result = if table_ref.is_subquery {
            if let Some(subquery) = &table_ref.subquery {
                execute_derived_table(
                    subquery,
                    table_name,
                    table_ref.column_aliases.as_ref(),
                    execute_subquery,
                )?
            } else {
                return Err(ExecutorError::UnsupportedFeature(
                    "Subquery reference missing query".to_string(),
                ));
            }
        } else {
            // Use table-local predicates instead of full WHERE clause for early filtering
            // This allows pushing down filters like `l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'`
            // to the table scan, significantly reducing rows before joins
            // Note: LIMIT pushdown is None here because this is for join intermediate results
            execute_table_scan(
                &table_ref.name,
                table_ref.alias.as_ref(),
                cte_results,
                database,
                table_filter.as_ref(),
                None,
                None,
                outer_row,
                outer_schema,
            )?
        };
        let scan_time = scan_start.elapsed();
        if profile {
            let scan_rows = table_result.data.as_slice().len();
            scan_times.push((format!("{} ({} rows)", table_name, scan_rows), scan_time));
        }

        // Record the column count for this table (using table_schemas to get column info)
        let col_count = if let Some((_, schema)) = table_result.schema.table_schemas.get(table_name)
        {
            schema.columns.len()
        } else {
            table_result.schema.total_columns
        };
        table_column_counts.insert(table_name.clone(), col_count);

        // Join with previous result (if any)
        if let Some(prev_result) = result {
            // Extract join conditions that connect this table to already-joined tables
            let mut applicable_conditions: Vec<Expression> = Vec::new();

            for (idx, condition) in join_conditions.iter().enumerate() {
                // Skip conditions we've already applied
                if applied_conditions.contains(&idx) {
                    continue;
                }

                // Extract tables referenced in this condition using schema-based column resolution
                let mut referenced_tables = HashSet::new();
                graph::extract_referenced_tables_with_schema(
                    condition,
                    &mut referenced_tables,
                    &table_set,
                    &column_to_table,
                );

                // Check if condition connects the new table with any already-joined table
                // Condition is applicable if it references the new table AND at least one joined table
                let references_new_table = referenced_tables.contains(&table_name.to_lowercase());
                let references_joined_table =
                    referenced_tables.iter().any(|t| joined_tables.contains(t));

                if references_new_table && references_joined_table {
                    applicable_conditions.push(condition.clone());
                    applied_conditions.insert(idx);
                } else if column_to_table.is_empty()
                    && joined_tables.len() == 1
                    && referenced_tables.is_empty()
                {
                    // CTE fallback: When column_to_table is empty (CTE results), include condition
                    // for 2-table joins since it was already extracted as a WHERE equijoin.
                    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                        eprintln!("[JOIN_REORDER] CTE fallback: including condition {:?} for 2-table join", condition);
                    }
                    applicable_conditions.push(condition.clone());
                    applied_conditions.insert(idx);
                }
            }

            // Debug logging for applicable conditions
            if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
                eprintln!(
                    "[JOIN_REORDER] Joining {} to {:?}, found {} applicable conditions",
                    table_name,
                    joined_tables,
                    applicable_conditions.len()
                );
                eprintln!("[JOIN_REORDER]   join_conditions total: {}", join_conditions.len());
                for (idx, cond) in join_conditions.iter().enumerate() {
                    if !applied_conditions.contains(&idx) {
                        let mut refs = HashSet::new();
                        graph::extract_referenced_tables_with_schema(
                            cond,
                            &mut refs,
                            &table_set,
                            &column_to_table,
                        );
                        eprintln!("[JOIN_REORDER]   cond[{}] refs: {:?}, new_table: {}, matches_new: {}, matches_joined: {}",
                            idx, refs, table_name.to_lowercase(),
                            refs.contains(&table_name.to_lowercase()),
                            refs.iter().any(|t| joined_tables.contains(t)));
                    }
                }
            }

            // Always use INNER join for comma-list joins, even when applicable_conditions is empty.
            // This allows nested_loop_join to find equijoins from WHERE clause and use hash join.
            // Using CROSS join would trigger memory limit checks for large Cartesian products.
            let join_type = &vibesql_ast::JoinType::Inner;

            // Note: Using default timeout context - proper timeout propagation is a future improvement
            let timeout_ctx = TimeoutContext::new_default();
            let left_rows = prev_result.data.as_slice().len();
            let right_rows = table_result.data.as_slice().len();
            let join_start = std::time::Instant::now();
            // Issue #3562: Pass CTE context so post-join filters with IN subqueries can resolve CTEs
            result = Some(nested_loop_join(
                prev_result,
                table_result,
                join_type,
                &None, // No ON condition (using additional_equijoins instead)
                false, // Not a NATURAL JOIN
                database,
                &applicable_conditions, // Pass only the applicable conditions for this join
                &timeout_ctx,
                cte_results,
            )?);
            let join_time = join_start.elapsed();
            let result_rows = result.as_ref().map(|r| r.data.as_slice().len()).unwrap_or(0);
            if profile {
                join_times.push((
                    table_name.clone(),
                    join_time,
                    left_rows * right_rows,
                    result_rows,
                ));
            }
        } else {
            result = Some(table_result);
        }

        // Mark this table as joined
        joined_tables.insert(table_name.to_lowercase());
    }

    let result =
        result.ok_or_else(|| ExecutorError::UnsupportedFeature("No tables in join".to_string()))?;

    // Step 11: Restore original column ordering if needed
    // Build column permutation: map from current position to target position
    let column_permutation =
        utils::build_column_permutation(&table_names, &optimal_order, &table_column_counts);

    // Reorder rows according to the permutation
    let reorder_start = std::time::Instant::now();
    let rows = result.data.into_rows();
    let reordered_rows: Vec<vibesql_storage::Row> = rows
        .into_iter()
        .map(|row| {
            let mut new_values = Vec::with_capacity(row.values.len());
            for &idx in &column_permutation {
                new_values.push(row.values[idx].clone());
            }
            vibesql_storage::Row::new(new_values)
        })
        .collect();
    let reorder_time = reorder_start.elapsed();

    // Print profiling summary
    if profile {
        eprintln!("[JOIN_PROFILE] === Multi-way JOIN Timing Breakdown ===");
        let total_scan: std::time::Duration = scan_times.iter().map(|(_, t)| *t).sum();
        let total_join: std::time::Duration = join_times.iter().map(|(_, t, _, _)| *t).sum();
        eprintln!("[JOIN_PROFILE] Table scans ({} tables):", scan_times.len());
        for (name, time) in &scan_times {
            eprintln!("[JOIN_PROFILE]   {} scan: {:?}", name, time);
        }
        eprintln!("[JOIN_PROFILE] Total scan time: {:?}", total_scan);
        eprintln!("[JOIN_PROFILE] Joins ({} joins):", join_times.len());
        for (name, time, cartesian, result_rows) in &join_times {
            eprintln!(
                "[JOIN_PROFILE]   Join {}: {:?} (cartesian={}, result={})",
                name, time, cartesian, result_rows
            );
        }
        eprintln!("[JOIN_PROFILE] Total join time: {:?}", total_join);
        eprintln!(
            "[JOIN_PROFILE] Column reorder: {:?} ({} rows)",
            reorder_time,
            reordered_rows.len()
        );
        eprintln!(
            "[JOIN_PROFILE] Grand total (scan+join+reorder): {:?}",
            total_scan + total_join + reorder_time
        );
    }

    // Build a new combined schema with tables in original order
    let new_schema = utils::build_reordered_schema(&result.schema, &table_names, &optimal_order);

    // Return result with reordered data and schema
    Ok(FromResult::from_rows(new_schema, reordered_rows))
}
