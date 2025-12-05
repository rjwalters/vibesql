//! IN subquery evaluation
//!
//! This module handles evaluation of IN and NOT IN predicates with subqueries,
//! including index-based optimization for simple cases.
//!
//! ## Optimization for Non-Correlated IN Subqueries
//!
//! For non-correlated IN subqueries (those that don't reference outer query columns),
//! results are cached and a HashSet is built for O(1) membership lookups. This is
//! critical for queries like TPC-H Q18 where the IN subquery contains GROUP BY/HAVING
//! and must be evaluated many times against different outer rows.

use super::super::super::core::{CombinedExpressionEvaluator, ExpressionEvaluator};
use super::schema_utils::{
    build_merged_outer_row, build_merged_outer_schema, compute_select_list_column_count,
};
use crate::errors::ExecutorError;
use crate::evaluator::caching::compute_subquery_hash;
use std::cell::RefCell;
use std::collections::HashSet;

/// Cached HashSet entry for IN subquery optimization
/// Built once from subquery rows, then reused for O(1) membership checks
#[derive(Clone)]
struct InSubqueryHashSetEntry {
    /// Whether any NULL values exist in the subquery result
    has_null: bool,
    /// Pre-built HashSet of non-NULL values for O(1) membership checks
    value_set: HashSet<vibesql_types::SqlValue>,
}

thread_local! {
    /// Thread-local cache for IN subquery HashSets
    /// Maps subquery hash -> (has_null, value_set)
    /// This cache is separate from the row cache to avoid modifying the core evaluator structure
    static IN_SUBQUERY_HASHSET_CACHE: RefCell<lru::LruCache<u64, InSubqueryHashSetEntry>> =
        RefCell::new(lru::LruCache::new(std::num::NonZeroUsize::new(1000).unwrap()));
}

/// Clear the thread-local IN subquery HashSet cache.
///
/// This is useful for benchmarks and long-running processes to prevent memory
/// accumulation. The cache is automatically bounded by LRU eviction, but for
/// memory-sensitive scenarios (like TPC-DS benchmarks), explicit clearing can
/// help reduce memory pressure between query batches.
///
/// # Example
///
/// ```text
/// use vibesql_executor::clear_in_subquery_cache;
///
/// // Run a batch of queries...
///
/// // Clear the cache to release memory
/// clear_in_subquery_cache();
/// ```
pub fn clear_in_subquery_cache() {
    IN_SUBQUERY_HASHSET_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// Build a HashSet from subquery result rows for O(1) membership checks
fn build_hashset_from_rows(rows: &[vibesql_storage::Row]) -> InSubqueryHashSetEntry {
    let mut has_null = false;
    let mut value_set = HashSet::with_capacity(rows.len());

    for row in rows {
        if let Some(val) = row.values.first() {
            if matches!(val, vibesql_types::SqlValue::Null) {
                has_null = true;
            } else {
                value_set.insert(val.clone());
            }
        }
    }

    InSubqueryHashSetEntry { has_null, value_set }
}

impl CombinedExpressionEvaluator<'_> {
    /// Evaluate IN operator with subquery
    /// SQL:1999 Section 8.4: IN predicate with subquery
    ///
    /// # Implementation Note
    ///
    /// For non-correlated IN subqueries, we build a HashSet from the subquery results
    /// for O(1) membership lookups. This provides massive speedup for queries like
    /// TPC-H Q18 where the IN subquery has GROUP BY/HAVING and is checked many times.
    ///
    /// For same-type comparisons (the common case), HashSet.contains() works correctly.
    /// For cross-type comparisons (rare), we fall back to SQL equality semantics.
    pub(in crate::evaluator::combined) fn eval_in_subquery(
        &self,
        expr: &vibesql_ast::Expression,
        subquery: &vibesql_ast::SelectStmt,
        negated: bool,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "IN with subquery requires database reference".to_string(),
        ))?;
        let sql_mode = database.sql_mode();

        // Evaluate the left-hand expression
        let expr_val = self.eval(expr, row)?;

        // Phase 3 optimization: Try index-aware execution for simple uncorrelated subqueries
        // Only applies to simple SELECT column FROM table WHERE ... queries
        // This is tried first because it's the fastest when applicable
        if can_use_index_for_in_subquery(subquery, database) {
            let index_result_opt = try_index_optimized_in_subquery(
                &expr_val,
                subquery,
                negated,
                database,
                sql_mode.clone(),
            )?;
            if let Some(index_result) = index_result_opt {
                return Ok(index_result);
            }
            // If index optimization fails, fall through to caching/regular execution
        }

        // Check if this is a non-correlated subquery that can be cached
        let is_correlated = crate::correlation::is_correlated(subquery, self.schema);

        if !is_correlated {
            // Non-correlated subquery - use HashSet optimization for O(1) lookups
            let cache_key = compute_subquery_hash(subquery);

            // Try to get or build the HashSet cache entry
            let hashset_entry = IN_SUBQUERY_HASHSET_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();

                // Check if we already have a HashSet for this subquery
                if let Some(entry) = cache.get(&cache_key) {
                    return Some(entry.clone());
                }

                // Not in HashSet cache - check if rows are in the row cache
                if let Some(rows) = self.subquery_cache.borrow().peek(&cache_key) {
                    // Validate column count before building HashSet
                    let column_count = if !rows.is_empty() {
                        rows[0].values.len()
                    } else {
                        // For empty sets, we can still build an empty HashSet
                        1
                    };

                    if column_count == 1 {
                        let entry = build_hashset_from_rows(rows);
                        cache.put(cache_key, entry.clone());
                        return Some(entry);
                    }
                }

                None
            });

            if let Some(entry) = hashset_entry {
                // Fast path: use HashSet for O(1) lookup
                return eval_in_with_hashset(&expr_val, &entry, negated, sql_mode);
            }

            // HashSet not cached yet - need to execute the subquery first
            let cached_result = self.subquery_cache.borrow().peek(&cache_key).cloned();

            let rows = if let Some(cached_rows) = cached_result {
                cached_rows
            } else {
                // Cache miss - execute and cache
                // Pass CTE context for queries referencing CTEs from outer scope (#3044)
                let select_executor = if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_cte_and_depth(
                        database, cte_ctx, self.depth,
                    )
                } else {
                    crate::select::SelectExecutor::new_with_depth(database, self.depth)
                };
                let rows = select_executor.execute(subquery)?;
                self.subquery_cache.borrow_mut().put(cache_key, rows.clone());
                rows
            };

            // Validate column count
            // Issue #3562: Pass CTE context so CTEs can be resolved when computing column count
            let column_count = if !rows.is_empty() {
                rows[0].values.len()
            } else {
                compute_select_list_column_count(subquery, database, self.cte_context)?
            };

            if column_count != 1 {
                return Err(ExecutorError::SubqueryColumnCountMismatch {
                    expected: 1,
                    actual: column_count,
                });
            }

            // Build and cache the HashSet
            let entry = build_hashset_from_rows(&rows);
            IN_SUBQUERY_HASHSET_CACHE.with(|cache| {
                cache.borrow_mut().put(cache_key, entry.clone());
            });

            // Use the HashSet for evaluation
            return eval_in_with_hashset(&expr_val, &entry, negated, sql_mode);
        }

        // Correlated subquery - execute with outer context (can't cache, use linear search)
        let merged_schema = if !self.schema.table_schemas.is_empty() {
            Some(build_merged_outer_schema(self.schema, self.outer_schema))
        } else {
            None
        };

        let merged_row = if merged_schema.is_some() {
            Some(build_merged_outer_row(row, self.outer_row))
        } else {
            None
        };

        // Pass CTE context for queries referencing CTEs from outer scope (#3044)
        let select_executor =
            if let (Some(ref schema), Some(ref outer_row)) = (&merged_schema, &merged_row) {
                if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_outer_and_cte_and_depth(
                        database, outer_row, schema, cte_ctx, self.depth,
                    )
                } else {
                    crate::select::SelectExecutor::new_with_outer_context_and_depth(
                        database, outer_row, schema, self.depth,
                    )
                }
            } else if let Some(cte_ctx) = self.cte_context {
                crate::select::SelectExecutor::new_with_cte_and_depth(database, cte_ctx, self.depth)
            } else {
                crate::select::SelectExecutor::new(database)
            };
        let rows = select_executor.execute(subquery)?;

        // Validate column count for correlated subqueries
        // Issue #3562: Pass CTE context so CTEs can be resolved when computing column count
        let column_count = if !rows.is_empty() {
            rows[0].values.len()
        } else {
            compute_select_list_column_count(subquery, database, self.cte_context)?
        };

        if column_count != 1 {
            return Err(ExecutorError::SubqueryColumnCountMismatch {
                expected: 1,
                actual: column_count,
            });
        }

        // For correlated subqueries, use linear search (they change each time)
        eval_in_linear(&expr_val, &rows, negated, sql_mode)
    }
}

/// Evaluate IN predicate using HashSet for O(1) membership check
fn eval_in_with_hashset(
    expr_val: &vibesql_types::SqlValue,
    entry: &InSubqueryHashSetEntry,
    negated: bool,
    sql_mode: vibesql_types::SqlMode,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    // Handle NULL expression
    if matches!(expr_val, vibesql_types::SqlValue::Null) {
        if entry.value_set.is_empty() && !entry.has_null {
            // NULL IN (empty set) → FALSE for IN, TRUE for NOT IN
            return Ok(vibesql_types::SqlValue::Boolean(negated));
        }
        // NULL IN (any non-empty set or set with NULL) → NULL
        return Ok(vibesql_types::SqlValue::Null);
    }

    // Fast path: direct HashSet lookup for same-type matches
    // This works for most cases where the expr_val type matches the subquery result type
    if entry.value_set.contains(expr_val) {
        return Ok(vibesql_types::SqlValue::Boolean(!negated));
    }

    // Slow path: check for cross-type equality (e.g., Integer vs Float)
    // Only needed if the direct lookup failed and types might differ
    for value in &entry.value_set {
        // Only do expensive comparison if types differ
        if std::mem::discriminant(expr_val) != std::mem::discriminant(value) {
            let eq_result = ExpressionEvaluator::eval_binary_op_static(
                expr_val,
                &vibesql_ast::BinaryOperator::Equal,
                value,
                sql_mode.clone(),
            )?;

            if matches!(eq_result, vibesql_types::SqlValue::Boolean(true)) {
                return Ok(vibesql_types::SqlValue::Boolean(!negated));
            }
        }
    }

    // No match found
    if entry.has_null {
        Ok(vibesql_types::SqlValue::Null)
    } else {
        Ok(vibesql_types::SqlValue::Boolean(negated))
    }
}

/// Evaluate IN predicate using linear search (fallback for correlated subqueries)
fn eval_in_linear(
    expr_val: &vibesql_types::SqlValue,
    rows: &[vibesql_storage::Row],
    negated: bool,
    sql_mode: vibesql_types::SqlMode,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    // Handle NULL expression
    if matches!(expr_val, vibesql_types::SqlValue::Null) {
        if rows.is_empty() {
            return Ok(vibesql_types::SqlValue::Boolean(negated));
        }

        // Check if any row contains NULL
        for row in rows {
            if let Some(val) = row.values.first() {
                if matches!(val, vibesql_types::SqlValue::Null) {
                    return Ok(vibesql_types::SqlValue::Null);
                }
            }
        }
        return Ok(vibesql_types::SqlValue::Null);
    }

    let mut found_null = false;

    for row in rows {
        let subquery_val = row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

        if matches!(subquery_val, vibesql_types::SqlValue::Null) {
            found_null = true;
            continue;
        }

        let eq_result = ExpressionEvaluator::eval_binary_op_static(
            expr_val,
            &vibesql_ast::BinaryOperator::Equal,
            subquery_val,
            sql_mode.clone(),
        )?;

        if matches!(eq_result, vibesql_types::SqlValue::Boolean(true)) {
            return Ok(vibesql_types::SqlValue::Boolean(!negated));
        }
    }

    if found_null {
        Ok(vibesql_types::SqlValue::Null)
    } else {
        Ok(vibesql_types::SqlValue::Boolean(negated))
    }
}

/// Check if a subquery can use index optimization for IN evaluation
///
/// Returns true if the subquery is:
/// - Simple SELECT column FROM table WHERE ... (no joins, no aggregates, no subqueries)
/// - Single table access
/// - Projected column exists and is indexed
fn can_use_index_for_in_subquery(
    subquery: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> bool {
    // Must have a FROM clause with single table
    let table_name = match &subquery.from {
        Some(vibesql_ast::FromClause::Table { name, .. }) => name,
        _ => return false, // Joins, subqueries, or no FROM clause
    };

    // Must not have GROUP BY, HAVING, LIMIT, OFFSET, or DISTINCT
    if subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.limit.is_some()
        || subquery.offset.is_some()
        || subquery.distinct
    {
        return false;
    }

    // Must project exactly one column (not wildcard, not multiple columns)
    if subquery.select_list.len() != 1 {
        return false;
    }

    // Get the projected column name
    #[allow(clippy::collapsible_match)]
    let column_name = match &subquery.select_list[0] {
        vibesql_ast::SelectItem::Expression { expr, .. } => {
            match expr {
                vibesql_ast::Expression::ColumnRef { column, .. } => column,
                _ => return false, // Expressions, functions, etc.
            }
        }
        _ => return false, // Wildcards
    };

    // Check if an index exists that covers this column
    let indexes = database.list_indexes_for_table(table_name);
    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            // Check if first indexed column matches our projected column
            if let Some(first_col) = index_metadata.columns.first() {
                if &first_col.column_name == column_name {
                    return true;
                }
            }
        }
    }

    false
}

/// Try to evaluate IN subquery using index optimization
///
/// Returns Some(result) if index optimization succeeds, None to fall back to regular execution
fn try_index_optimized_in_subquery(
    expr_val: &vibesql_types::SqlValue,
    subquery: &vibesql_ast::SelectStmt,
    negated: bool,
    database: &vibesql_storage::Database,
    sql_mode: vibesql_types::SqlMode,
) -> Result<Option<vibesql_types::SqlValue>, ExecutorError> {
    // Extract table and column names (already validated by can_use_index_for_in_subquery)
    let table_name = match &subquery.from {
        Some(vibesql_ast::FromClause::Table { name, .. }) => name,
        _ => return Ok(None),
    };

    #[allow(clippy::collapsible_match)]
    let column_name = match &subquery.select_list[0] {
        vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
            vibesql_ast::Expression::ColumnRef { column, .. } => column,
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    // Find the best index for this column
    let indexes = database.list_indexes_for_table(table_name);
    let mut selected_index: Option<String> = None;

    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            if let Some(first_col) = index_metadata.columns.first() {
                if &first_col.column_name == column_name {
                    selected_index = Some(index_name.clone());
                    break;
                }
            }
        }
    }

    let index_name = match selected_index {
        Some(name) => name,
        None => return Ok(None),
    };

    // Get index data
    let index_data = match database.get_index_data(&index_name) {
        Some(data) => data,
        None => return Ok(None),
    };

    let table = match database.get_table(table_name) {
        Some(t) => t,
        None => return Ok(None),
    };

    // Use index to get all values efficiently
    // Two strategies based on presence of WHERE clause:
    // 1. With WHERE: Extract predicate, use index scan with filtering
    // 2. Without WHERE: Scan all values from index

    let values_set = if let Some(where_expr) = &subquery.where_clause {
        // Strategy 1: Use index scan with predicate pushdown
        use crate::select::scan::index_scan::predicate::extract_index_predicate;

        // Try to extract index predicate (range or IN)
        if let Some(index_pred) = extract_index_predicate(where_expr, column_name) {
            // Get row indices using index predicate
            let row_indices: Vec<usize> = match index_pred {
                crate::select::scan::index_scan::predicate::IndexPredicate::Range(range) => {
                    index_data.range_scan(
                        range.start.as_ref(),
                        range.end.as_ref(),
                        range.inclusive_start,
                        range.inclusive_end,
                    )
                }
                crate::select::scan::index_scan::predicate::IndexPredicate::In(vals) => {
                    index_data.multi_lookup(&vals)
                }
            };

            // Fetch actual column values from matched rows
            let all_rows = table.scan();
            let column_index =
                table.schema.columns.iter().position(|col| col.name == *column_name).ok_or_else(
                    || ExecutorError::ColumnNotFound {
                        column_name: column_name.clone(),
                        table_name: table_name.clone(),
                        searched_tables: vec![table_name.clone()],
                        available_columns: table
                            .schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    },
                )?;

            let mut values = std::collections::HashSet::new();
            for row_idx in row_indices {
                if let Some(row) = all_rows.get(row_idx) {
                    if let Some(value) = row.values.get(column_index) {
                        values.insert(value.clone());
                    }
                }
            }
            values
        } else {
            // WHERE clause exists but can't use index - fall back
            return Ok(None);
        }
    } else {
        // Strategy 2: No WHERE clause - scan all indexed values
        // This is still faster than full subquery execution if we can read from index
        let all_rows = table.scan();
        let column_index = table
            .schema
            .columns
            .iter()
            .position(|col| col.name == *column_name)
            .ok_or_else(|| ExecutorError::ColumnNotFound {
                column_name: column_name.clone(),
                table_name: table_name.clone(),
                searched_tables: vec![table_name.clone()],
                available_columns: table.schema.columns.iter().map(|c| c.name.clone()).collect(),
            })?;

        // Collect all distinct values from the column
        let mut values = std::collections::HashSet::new();
        for row in all_rows {
            if let Some(value) = row.values.get(column_index) {
                values.insert(value.clone());
            }
        }
        values
    };

    // Now check if expr_val is in the set (same logic as original implementation)
    // Handle NULL cases per SQL standard
    if matches!(expr_val, vibesql_types::SqlValue::Null) {
        if values_set.is_empty() {
            return Ok(Some(vibesql_types::SqlValue::Boolean(negated)));
        }

        if values_set.contains(&vibesql_types::SqlValue::Null) {
            return Ok(Some(vibesql_types::SqlValue::Null));
        }

        return Ok(Some(vibesql_types::SqlValue::Null));
    }

    let mut found_null = false;
    for value in &values_set {
        if matches!(value, vibesql_types::SqlValue::Null) {
            found_null = true;
            continue;
        }

        // Compare using equality
        let eq_result = ExpressionEvaluator::eval_binary_op_static(
            expr_val,
            &vibesql_ast::BinaryOperator::Equal,
            value,
            sql_mode.clone(),
        )?;

        if matches!(eq_result, vibesql_types::SqlValue::Boolean(true)) {
            return Ok(Some(vibesql_types::SqlValue::Boolean(!negated)));
        }
    }

    // No match found
    if found_null {
        Ok(Some(vibesql_types::SqlValue::Null))
    } else {
        Ok(Some(vibesql_types::SqlValue::Boolean(negated)))
    }
}
