//! Main join reordering optimization logic

use super::{graph, predicates, utils};
use crate::{
    debug_output::{Category, DebugEvent, JsonValue},
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

/// Check if verbose join reorder logging is enabled
fn join_reorder_verbose() -> bool {
    std::env::var("JOIN_REORDER_VERBOSE").is_ok()
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

    if join_reorder_verbose() {
        DebugEvent::new(Category::Optimizer, "column_mapping", "JOIN_REORDER")
            .text(format!(
                "Schema-based column mapping: {} columns resolved from {} tables",
                column_to_table.len(),
                table_names.len()
            ))
            .field_int("columns_resolved", column_to_table.len() as i64)
            .field_int("table_count", table_names.len() as i64)
            .field_str_array("tables", &table_names)
            .emit();
        if column_to_table.is_empty() && !table_names.is_empty() {
            DebugEvent::new(Category::Optimizer, "column_mapping_warning", "JOIN_REORDER")
                .text(format!(
                    "Warning: No schema columns found for tables: {:?}",
                    table_names
                ))
                .field_str_array("tables", &table_names)
                .emit();
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
        if join_reorder_verbose() {
            DebugEvent::new(Category::Optimizer, "where_clause_analysis", "JOIN_REORDER")
                .text(format!("WHERE clause present: {:?}", where_expr))
                .field_bool("has_where_clause", true)
                .field_int("table_count", table_set.len() as i64)
                .emit();
        }

        // Extract equijoin conditions from WHERE clause using schema-based column resolution
        let equijoins = predicates::extract_where_equijoins_with_schema(
            where_expr,
            &table_set,
            &column_to_table,
        );

        if join_reorder_verbose() {
            DebugEvent::new(Category::Optimizer, "where_equijoins", "JOIN_REORDER")
                .text(format!("Extracted {} WHERE equijoins", equijoins.len()))
                .field_int("equijoin_count", equijoins.len() as i64)
                .emit();
        }

        equijoins
    } else {
        if join_reorder_verbose() {
            DebugEvent::new(Category::Optimizer, "where_clause_analysis", "JOIN_REORDER")
                .text("No WHERE clause")
                .field_bool("has_where_clause", false)
                .emit();
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

    if join_reorder_verbose() && !table_local_predicates.is_empty() {
        let predicate_tables: Vec<String> =
            table_local_predicates.keys().cloned().collect();
        DebugEvent::new(Category::Optimizer, "table_local_predicates", "JOIN_REORDER")
            .text(format!(
                "Table-local predicates: {:?}",
                predicate_tables
            ))
            .field_str_array("tables_with_predicates", &predicate_tables)
            .field_int("predicate_table_count", predicate_tables.len() as i64)
            .emit();
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
    if join_reorder_verbose() {
        DebugEvent::new(Category::Optimizer, "join_order_decision", "JOIN_REORDER")
            .text(format!(
                "Original order: {:?}, Optimal order: {:?}",
                table_names, optimal_order
            ))
            .field_str_array("original_order", &table_names)
            .field_str_array("optimal_order", &optimal_order)
            .field_int("join_condition_count", join_conditions.len() as i64)
            .field_duration_us("optimizer_time_us", optimizer_time)
            .field_bool("order_changed", table_names != optimal_order)
            .emit();
    }

    // Profiling: Track times for each phase
    let profile = join_profile_enabled();
    let mut scan_times: Vec<(String, std::time::Duration)> = Vec::new();
    let mut join_times: Vec<(String, std::time::Duration, usize, usize)> = Vec::new();
    if profile {
        DebugEvent::new(Category::Execution, "optimizer_time", "JOIN_PROFILE")
            .text(format!("Optimizer time: {:?}", optimizer_time))
            .field_duration_us("optimizer_time_us", optimizer_time)
            .field_duration_ms("optimizer_time_ms", optimizer_time)
            .emit();
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
            // Note: column_aliases is None - join reordering doesn't preserve column aliases
            execute_table_scan(
                &table_ref.name,
                table_ref.alias.as_ref(),
                None, // column_aliases not supported in join reordering
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
        let col_count = if let Some((_, schema)) = table_result.schema.get_table(table_name) {
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
        let total_scan: std::time::Duration = scan_times.iter().map(|(_, t)| *t).sum();
        let total_join: std::time::Duration = join_times.iter().map(|(_, t, _, _)| *t).sum();
        let grand_total = total_scan + total_join + reorder_time;

        // Build structured scan data for JSON
        let scan_data: Vec<(String, JsonValue)> = scan_times
            .iter()
            .map(|(name, time)| {
                (
                    name.clone(),
                    JsonValue::Object(vec![
                        ("duration_us".to_string(), JsonValue::Int(time.as_micros() as i64)),
                        ("duration_ms".to_string(), JsonValue::Number(time.as_secs_f64() * 1000.0)),
                    ]),
                )
            })
            .collect();

        // Build structured join data for JSON
        let join_data: Vec<(String, JsonValue)> = join_times
            .iter()
            .map(|(name, time, cartesian, result_rows)| {
                (
                    name.clone(),
                    JsonValue::Object(vec![
                        ("duration_us".to_string(), JsonValue::Int(time.as_micros() as i64)),
                        ("duration_ms".to_string(), JsonValue::Number(time.as_secs_f64() * 1000.0)),
                        ("cartesian_product".to_string(), JsonValue::Int(*cartesian as i64)),
                        ("result_rows".to_string(), JsonValue::Int(*result_rows as i64)),
                    ]),
                )
            })
            .collect();

        // Emit structured profiling summary
        DebugEvent::new(Category::Execution, "join_profile_summary", "JOIN_PROFILE")
            .text(format!(
                "=== Multi-way JOIN Timing Breakdown ===\n\
                 Table scans ({} tables): {:?}\n\
                 Total join time: {:?}\n\
                 Column reorder: {:?} ({} rows)\n\
                 Grand total: {:?}",
                scan_times.len(),
                total_scan,
                total_join,
                reorder_time,
                reordered_rows.len(),
                grand_total
            ))
            .field_int("table_count", scan_times.len() as i64)
            .field_int("join_count", join_times.len() as i64)
            .field_duration_us("total_scan_time_us", total_scan)
            .field_duration_us("total_join_time_us", total_join)
            .field_duration_us("reorder_time_us", reorder_time)
            .field_duration_us("grand_total_us", grand_total)
            .field_int("result_row_count", reordered_rows.len() as i64)
            .field("scans", JsonValue::Object(scan_data))
            .field("joins", JsonValue::Object(join_data))
            .emit();
    }

    // Build a new combined schema with tables in original order
    let new_schema = utils::build_reordered_schema(&result.schema, &table_names, &optimal_order);

    // Return result with reordered data and schema
    Ok(FromResult::from_rows(new_schema, reordered_rows))
}

/// Execute a semi/anti join with inner cross joins, including the derived table in reordering
///
/// Pattern: `(cross_joins) SEMI/ANTI JOIN derived_table`
///
/// This function:
/// 1. Extracts the inner cross joins (left side)
/// 2. Executes the derived table (right side) first
/// 3. Includes the derived table in join reordering
/// 4. Uses the semi-join condition to position the derived table early
/// 5. Applies the semi-join filter early to reduce intermediate results
///
/// ## Example: TPC-H Q18
///
/// Input: `(customer × orders × lineitem) SEMI JOIN __in_agg ON o_orderkey = l_orderkey`
///
/// Without this optimization:
/// 1. Join customer × orders × lineitem → 60000 rows
/// 2. Semi-join with __in_agg → 0 rows
///
/// With this optimization:
/// 1. Execute __in_agg → 0 rows
/// 2. Semi-join orders with __in_agg → 0 rows  (early filter!)
/// 3. Join remaining tables → 0 rows
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_with_semi_join_reordering<F>(
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
    // Extract the semi/anti join structure
    let (inner_from, derived_query, derived_alias, semi_join_type, semi_condition) = match from {
        FromClause::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } if matches!(join_type, vibesql_ast::JoinType::Semi | vibesql_ast::JoinType::Anti) => {
            // Extract the subquery from the right side
            match right.as_ref() {
                FromClause::Subquery { query, alias, .. } => {
                    (left.as_ref(), query, alias, join_type, condition)
                }
                _ => {
                    return Err(ExecutorError::UnsupportedFeature(
                        "execute_with_semi_join_reordering: right side must be subquery"
                            .to_string(),
                    ));
                }
            }
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "execute_with_semi_join_reordering called with non-semi-join".to_string(),
            ));
        }
    };

    let profile = join_profile_enabled();

    if profile || std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!(
            "[SEMI_JOIN_REORDER] Starting semi-join reordering for derived table: {}",
            derived_alias
        );
    }

    // Step 1: Execute the derived table first
    // This is the subquery result (e.g., order keys with quantity > 300)
    let derived_start = std::time::Instant::now();
    let derived_result = execute_derived_table(derived_query, derived_alias, None, execute_subquery)?;
    let derived_time = derived_start.elapsed();
    let derived_row_count = derived_result.data.as_slice().len();

    if profile {
        eprintln!(
            "[SEMI_JOIN_REORDER] Derived table '{}' executed: {} rows in {:?}",
            derived_alias, derived_row_count, derived_time
        );
    }

    // Step 2: Short-circuit optimization - if derived table is empty AND we have a semi-join,
    // the result must be empty (semi-join requires a match in the right side)
    if derived_row_count == 0 && matches!(*semi_join_type, vibesql_ast::JoinType::Semi) {
        if profile {
            eprintln!(
                "[SEMI_JOIN_REORDER] Short-circuit: empty derived table means empty semi-join result"
            );
        }

        // Build schema from inner tables without executing the expensive joins
        // We still need the correct schema structure for the result
        let inner_schema = build_inner_schema_without_execution(
            inner_from,
            database,
            cte_results,
            outer_row,
            outer_schema,
            execute_subquery,
        )?;

        return Ok(FromResult::from_rows(inner_schema, vec![]));
    }

    // Step 3: For non-empty derived tables, identify which inner table the semi-join filters
    // and apply the filter early before the expensive cross-joins
    let target_table = identify_semi_join_target_table(semi_condition, inner_from);

    if profile {
        eprintln!(
            "[SEMI_JOIN_REORDER] Semi-join target table: {:?}",
            target_table
        );
    }

    // Step 4: If we can identify a target table, apply the semi-join early
    if let Some(target_name) = target_table {
        return execute_with_early_semi_join(
            inner_from,
            &target_name,
            derived_result,
            semi_join_type,
            semi_condition,
            cte_results,
            database,
            where_clause,
            outer_row,
            outer_schema,
            execute_subquery,
            profile,
        );
    }

    // Step 5: Fallback - apply join reordering then semi-join at end
    let inner_start = std::time::Instant::now();
    let inner_result = execute_with_join_reordering(
        inner_from,
        cte_results,
        database,
        where_clause,
        outer_row,
        outer_schema,
        execute_subquery,
    )?;
    let inner_time = inner_start.elapsed();
    let inner_row_count = inner_result.data.as_slice().len();

    if profile {
        eprintln!(
            "[SEMI_JOIN_REORDER] Inner join completed: {} rows in {:?}",
            inner_row_count, inner_time
        );
    }

    // Step 6: Apply the semi/anti join between inner result and derived table
    let timeout_ctx = TimeoutContext::new_default();
    let semi_start = std::time::Instant::now();
    let result = crate::select::join::nested_loop_join(
        inner_result,
        derived_result,
        semi_join_type,
        semi_condition,
        false, // not natural
        database,
        &[], // no additional equijoins
        &timeout_ctx,
        cte_results,
    )?;
    let semi_time = semi_start.elapsed();

    if profile {
        eprintln!(
            "[SEMI_JOIN_REORDER] Semi-join applied: {} rows in {:?}",
            result.data.as_slice().len(),
            semi_time
        );
        eprintln!(
            "[SEMI_JOIN_REORDER] Total time: {:?}",
            derived_time + inner_time + semi_time
        );
    }

    Ok(result)
}

/// Build schema for inner tables without executing the expensive joins.
/// Used for short-circuit optimization when derived table is empty.
#[allow(clippy::too_many_arguments)]
fn build_inner_schema_without_execution<F>(
    inner_from: &FromClause,
    database: &vibesql_storage::Database,
    cte_results: &HashMap<String, CteResult>,
    _outer_row: Option<&vibesql_storage::Row>,
    _outer_schema: Option<&CombinedSchema>,
    _execute_subquery: F,
) -> Result<CombinedSchema, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<SelectResult, ExecutorError> + Copy,
{
    // Flatten the join tree to get all table references
    let mut table_refs = Vec::new();
    super::graph::flatten_join_tree(inner_from, &mut table_refs);

    // Build combined schema from all tables
    let mut result_schema: Option<CombinedSchema> = None;

    for table_ref in &table_refs {
        let table_name = &table_ref.name;
        let alias = table_ref.alias.as_ref().unwrap_or(table_name).clone();

        // Check CTEs first (CteResult is a tuple: (TableSchema, Arc<Vec<Row>>))
        if let Some((cte_schema, _)) = cte_results.get(&table_name.to_lowercase()) {
            let schema = cte_schema.clone();
            match result_schema.take() {
                None => result_schema = Some(CombinedSchema::from_table(alias, schema)),
                Some(existing) => result_schema = Some(CombinedSchema::combine(existing, alias, schema)),
            }
            continue;
        }

        // Get table schema from database
        if let Some(table) = database.get_table(table_name) {
            let schema = table.schema.clone();
            match result_schema.take() {
                None => result_schema = Some(CombinedSchema::from_table(alias, schema)),
                Some(existing) => result_schema = Some(CombinedSchema::combine(existing, alias, schema)),
            }
        }
    }

    result_schema.ok_or_else(|| {
        ExecutorError::UnsupportedFeature("No tables found in inner FROM clause".to_string())
    })
}

/// Identify which inner table the semi-join condition references.
/// Returns the table name/alias if found.
///
/// For Q18, the condition is: `o_orderkey = __in_agg_0.l_orderkey`
/// This should return "orders" because o_orderkey references the orders table.
fn identify_semi_join_target_table(
    condition: &Option<Expression>,
    inner_from: &FromClause,
) -> Option<String> {
    let condition = condition.as_ref()?;

    // Extract table names from inner_from
    let mut table_refs = Vec::new();
    super::graph::flatten_join_tree(inner_from, &mut table_refs);
    let table_names: Vec<String> = table_refs
        .iter()
        .map(|t| t.alias.clone().unwrap_or_else(|| t.name.clone()).to_lowercase())
        .collect();

    // Find column references in the condition that belong to inner tables
    find_inner_table_in_condition(condition, &table_names)
}

/// Find which inner table is referenced in the semi-join condition
fn find_inner_table_in_condition(expr: &Expression, inner_tables: &[String]) -> Option<String> {
    match expr {
        Expression::BinaryOp { left, right, .. } => {
            // Check both sides
            if let Some(t) = find_inner_table_in_condition(left, inner_tables) {
                return Some(t);
            }
            find_inner_table_in_condition(right, inner_tables)
        }
        Expression::ColumnRef { table: Some(t), column, .. } => {
            let t_lower = t.to_lowercase();
            if inner_tables.contains(&t_lower) {
                return Some(t_lower);
            }
            // Also try to infer from column naming convention (o_ → orders)
            infer_table_from_column(column, inner_tables)
        }
        Expression::ColumnRef { table: None, column, .. } => {
            // Try to infer from column naming convention
            infer_table_from_column(column, inner_tables)
        }
        _ => None,
    }
}

/// Infer table from column naming convention (e.g., o_orderkey → orders)
fn infer_table_from_column(column: &str, inner_tables: &[String]) -> Option<String> {
    let col_lower = column.to_lowercase();

    // Common TPC-H naming conventions
    let prefix_to_table = [
        ("o_", "orders"),
        ("c_", "customer"),
        ("l_", "lineitem"),
        ("p_", "part"),
        ("s_", "supplier"),
        ("ps_", "partsupp"),
        ("n_", "nation"),
        ("r_", "region"),
    ];

    for (prefix, table) in &prefix_to_table {
        if col_lower.starts_with(prefix) && inner_tables.contains(&table.to_string()) {
            return Some(table.to_string());
        }
    }

    None
}

/// Execute with early semi-join application to the target table.
///
/// Instead of: (T1 × T2 × T3) SEMI JOIN derived
/// We do: T1 × (T2 SEMI JOIN derived) × T3
/// where T2 is the target table referenced by the semi-join condition.
#[allow(clippy::too_many_arguments)]
fn execute_with_early_semi_join<F>(
    inner_from: &FromClause,
    target_table: &str,
    derived_result: FromResult,
    semi_join_type: &vibesql_ast::JoinType,
    semi_condition: &Option<Expression>,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&Expression>,
    outer_row: Option<&vibesql_storage::Row>,
    outer_schema: Option<&CombinedSchema>,
    execute_subquery: F,
    profile: bool,
) -> Result<FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<SelectResult, ExecutorError> + Copy,
{
    // Step 1: Flatten the inner join tree
    let mut table_refs = Vec::new();
    super::graph::flatten_join_tree(inner_from, &mut table_refs);

    // Step 2: Find the target table index
    let target_idx = table_refs.iter().position(|t| {
        let name = t.alias.as_ref().unwrap_or(&t.name).to_lowercase();
        name == target_table.to_lowercase()
    });

    let target_idx = match target_idx {
        Some(idx) => idx,
        None => {
            // Target table not found, fall back to default execution
            if profile {
                eprintln!(
                    "[SEMI_JOIN_REORDER] Target table '{}' not found in inner tables, falling back",
                    target_table
                );
            }
            let inner_result = execute_with_join_reordering(
                inner_from,
                cte_results,
                database,
                where_clause,
                outer_row,
                outer_schema,
                execute_subquery,
            )?;

            let timeout_ctx = TimeoutContext::new_default();
            return crate::select::join::nested_loop_join(
                inner_result,
                derived_result,
                semi_join_type,
                semi_condition,
                false,
                database,
                &[],
                &timeout_ctx,
                cte_results,
            );
        }
    };

    let target_ref = &table_refs[target_idx];

    if profile {
        eprintln!(
            "[SEMI_JOIN_REORDER] Applying early semi-join to table '{}' at index {}",
            target_table, target_idx
        );
    }

    // Step 3: Execute the target table scan
    let target_scan_start = std::time::Instant::now();
    let target_result = execute_table_scan(
        &target_ref.name,
        target_ref.alias.as_ref(),
        None, // column_aliases not supported in join reordering
        cte_results,
        database,
        None, // Don't push WHERE predicates yet - we'll handle them in join reordering
        None,
        None,
        outer_row,
        outer_schema,
    )?;
    let target_scan_time = target_scan_start.elapsed();

    if profile {
        eprintln!(
            "[SEMI_JOIN_REORDER] Target table scan: {} rows in {:?}",
            target_result.data.as_slice().len(),
            target_scan_time
        );
    }

    // Step 4: Apply semi-join to the target table
    let timeout_ctx = TimeoutContext::new_default();
    let semi_join_start = std::time::Instant::now();
    let filtered_result = crate::select::join::nested_loop_join(
        target_result,
        derived_result,
        semi_join_type,
        semi_condition,
        false,
        database,
        &[],
        &timeout_ctx,
        cte_results,
    )?;
    let semi_join_time = semi_join_start.elapsed();

    let filtered_count = filtered_result.data.as_slice().len();
    if profile {
        eprintln!(
            "[SEMI_JOIN_REORDER] Early semi-join applied: {} rows in {:?}",
            filtered_count, semi_join_time
        );
    }

    // Step 5: If filtered result is empty, short-circuit
    if filtered_count == 0 {
        if profile {
            eprintln!("[SEMI_JOIN_REORDER] Early semi-join produced 0 rows, short-circuiting");
        }

        // Build full schema for all inner tables
        let full_schema = build_inner_schema_without_execution(
            inner_from,
            database,
            cte_results,
            outer_row,
            outer_schema,
            execute_subquery,
        )?;

        return Ok(FromResult::from_rows(full_schema, vec![]));
    }

    // Step 6: Execute remaining tables and join with filtered result
    // We need to build the join order excluding the target table (which we've already filtered)
    let remaining_tables: Vec<_> = table_refs
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != target_idx)
        .map(|(_, t)| t.clone())
        .collect();

    if profile {
        eprintln!(
            "[SEMI_JOIN_REORDER] Joining filtered result with {} remaining tables",
            remaining_tables.len()
        );
    }

    // Build column_to_table map for schema-based equijoin extraction
    let table_names: Vec<String> = table_refs
        .iter()
        .map(|t| t.alias.as_ref().unwrap_or(&t.name).to_lowercase())
        .collect();
    let column_to_table =
        utils::build_column_to_table_map(database, &table_names, &table_refs, cte_results);

    // Track accumulated tables (starts with target table)
    let target_alias = table_refs[target_idx]
        .alias
        .as_ref()
        .unwrap_or(&table_refs[target_idx].name)
        .to_lowercase();
    let mut accumulated_tables: HashSet<String> = HashSet::new();
    accumulated_tables.insert(target_alias);

    // Start with the filtered result
    let mut accumulated = filtered_result;

    // Execute and join remaining tables
    for table_ref in &remaining_tables {
        let table_start = std::time::Instant::now();
        let table_result = execute_table_scan(
            &table_ref.name,
            table_ref.alias.as_ref(),
            None, // column_aliases not supported in join reordering
            cte_results,
            database,
            None,
            None,
            None,
            outer_row,
            outer_schema,
        )?;
        let table_scan_time = table_start.elapsed();

        let new_table_alias = table_ref
            .alias
            .as_ref()
            .unwrap_or(&table_ref.name)
            .to_lowercase();

        if profile {
            eprintln!(
                "[SEMI_JOIN_REORDER] Scanned '{}': {} rows in {:?}",
                new_table_alias,
                table_result.data.as_slice().len(),
                table_scan_time
            );
        }

        // Extract equijoin predicates from WHERE clause that connect accumulated tables to new table
        let equijoins = if let Some(where_expr) = where_clause {
            // Build table set including accumulated tables and the new table
            let mut join_table_set = accumulated_tables.clone();
            join_table_set.insert(new_table_alias.clone());

            // Extract equijoins that reference both sides
            let all_equijoins = predicates::extract_where_equijoins_with_schema(
                where_expr,
                &join_table_set,
                &column_to_table,
            );

            // Filter to only include equijoins that connect accumulated tables to the new table
            let applicable_equijoins: Vec<_> = all_equijoins
                .into_iter()
                .filter(|eq| {
                    let mut referenced = HashSet::new();
                    graph::extract_referenced_tables_with_schema(
                        eq,
                        &mut referenced,
                        &join_table_set,
                        &column_to_table,
                    );
                    // Keep if references the new table AND at least one accumulated table
                    referenced.contains(&new_table_alias)
                        && referenced.iter().any(|t| accumulated_tables.contains(t))
                })
                .collect();

            if profile && !applicable_equijoins.is_empty() {
                eprintln!(
                    "[SEMI_JOIN_REORDER] Found {} equijoin predicates for joining '{}'",
                    applicable_equijoins.len(),
                    new_table_alias
                );
            }

            applicable_equijoins
        } else {
            Vec::new()
        };

        // Use Inner join with equijoins if available, otherwise Cross join
        let (join_type, join_condition) = if !equijoins.is_empty() {
            (vibesql_ast::JoinType::Inner, None)
        } else {
            (vibesql_ast::JoinType::Cross, None)
        };

        // Join with accumulated result using extracted equijoin predicates
        let join_start = std::time::Instant::now();
        accumulated = crate::select::join::nested_loop_join(
            accumulated,
            table_result,
            &join_type,
            &join_condition,
            false,
            database,
            &equijoins,
            &timeout_ctx,
            cte_results,
        )?;
        let join_time = join_start.elapsed();

        // Add new table to accumulated set
        accumulated_tables.insert(new_table_alias.clone());

        if profile {
            eprintln!(
                "[SEMI_JOIN_REORDER] After join with '{}': {} rows in {:?}",
                new_table_alias,
                accumulated.data.as_slice().len(),
                join_time
            );
        }
    }

    // Note: WHERE clause filtering is handled by the caller (select executor)
    // We return the joined result and let the standard pipeline apply WHERE predicates

    Ok(accumulated)
}
