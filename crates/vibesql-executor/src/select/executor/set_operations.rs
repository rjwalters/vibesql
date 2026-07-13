//! Set operations execution (UNION, INTERSECT, EXCEPT)
//!
//! This module handles set operation execution including:
//! - Executing chains of set operations left-to-right
//! - Sorting set operation results with ORDER BY
//! - Collation handling for set operation comparisons
//! - Column name extraction and alias resolution for UNION chains

use std::{cmp::Ordering, collections::HashMap};

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    select::{
        cte::CteResult, grouping::compare_sql_values_with_collation, helpers::estimate_result_size,
        set_operations::apply_set_operation,
    },
};

impl SelectExecutor<'_> {
    /// Extract collation information from a SELECT list.
    ///
    /// Returns a Vec of Option<String> where each element corresponds to a SELECT item.
    /// Collation is determined by:
    /// 1. Explicit COLLATE clause in the SELECT expression (e.g., `a COLLATE NOCASE`)
    /// 2. Column's schema collation if the expression is a simple column reference
    ///
    /// The optional `database` and `from_clause` parameters enable schema-based collation lookup.
    pub(super) fn extract_collations_from_select_list_with_schema(
        select_list: &[vibesql_ast::SelectItem],
        database: Option<&vibesql_storage::Database>,
        from_clause: Option<&vibesql_ast::FromClause>,
    ) -> Vec<Option<String>> {
        select_list
            .iter()
            .map(|item| {
                match item {
                    // Check if the item is an expression with a COLLATE clause
                    vibesql_ast::SelectItem::Expression {
                        expr: vibesql_ast::Expression::Collate { collation, .. },
                        ..
                    } => Some(collation.clone()),
                    // Check if it's a column reference - look up schema collation
                    vibesql_ast::SelectItem::Expression {
                        expr: vibesql_ast::Expression::ColumnRef(col_id),
                        ..
                    } => {
                        if let (Some(db), Some(_from)) = (database, from_clause) {
                            // Try to find the column's collation from the table schema
                            let table_name = col_id.table_canonical();
                            let column_name = col_id.column_canonical();
                            // If table is qualified, look it up directly
                            if let Some(table_name) = table_name {
                                if let Some(table) = db.get_table(table_name) {
                                    for col in &table.schema.columns {
                                        if col.name.eq_ignore_ascii_case(column_name) {
                                            return col.collation.clone();
                                        }
                                    }
                                }
                            } else {
                                // If table wasn't qualified, search all tables in FROM
                                if let Some(collation) = Self::find_column_collation_in_from(
                                    db,
                                    from_clause.unwrap(),
                                    column_name,
                                ) {
                                    return Some(collation);
                                }
                            }
                        }
                        None
                    }
                    _ => None,
                }
            })
            .collect()
    }

    /// Find a column's collation by searching through a FROM clause
    fn find_column_collation_in_from(
        database: &vibesql_storage::Database,
        from_clause: &vibesql_ast::FromClause,
        column_name: &str,
    ) -> Option<String> {
        match from_clause {
            vibesql_ast::FromClause::Table { name, .. } => {
                if let Some(table) = database.get_table(name) {
                    for col in &table.schema.columns {
                        if col.name.eq_ignore_ascii_case(column_name) {
                            return col.collation.clone();
                        }
                    }
                }
                None
            }
            vibesql_ast::FromClause::Join { left, right, .. } => {
                Self::find_column_collation_in_from(database, left, column_name)
                    .or_else(|| Self::find_column_collation_in_from(database, right, column_name))
            }
            _ => None,
        }
    }

    /// Extract collation information from a SELECT list (legacy version without schema lookup).
    #[allow(dead_code)]
    pub(super) fn extract_collations_from_select_list(
        select_list: &[vibesql_ast::SelectItem],
    ) -> Vec<Option<String>> {
        Self::extract_collations_from_select_list_with_schema(select_list, None, None)
    }

    /// Execute a chain of set operations left-to-right
    ///
    /// SQL set operations are left-associative, so:
    /// A EXCEPT B EXCEPT C should evaluate as (A EXCEPT B) EXCEPT C
    ///
    /// The parser creates a right-recursive AST structure, but we need to execute left-to-right.
    ///
    /// The `collations` parameter specifies the collation for each column from the leftmost
    /// SELECT statement in the set operation chain. This is used for collation-aware comparisons.
    ///
    /// The `expected_col_count` parameter is the column count from the leftmost SELECT,
    /// computed from AST for schema-level validation even when results are empty.
    pub(super) fn execute_set_operations(
        &self,
        mut left_results: Vec<vibesql_storage::Row>,
        set_op: &vibesql_ast::SetOperation,
        cte_results: &HashMap<String, CteResult>,
        collations: &[Option<String>],
        expected_col_count: Option<usize>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Execute the immediate right query WITHOUT its set operations
        // This prevents right-recursive evaluation
        let right_stmt = &set_op.right;

        // Issue #4602: Validate column count at schema level BEFORE execution
        // This catches mismatches even when result sets are empty
        // Use expected_col_count (from AST), or fall back to actual results
        let left_col_count = expected_col_count.unwrap_or_else(|| {
            if !left_results.is_empty() {
                left_results[0].values.len()
            } else {
                0 // Can't determine column count
            }
        });
        if left_col_count > 0 {
            // Try to compute right side column count from AST
            // Skip validation if we can't compute it (e.g., complex subqueries)
            if let Ok(right_col_count) = super::nonagg::compute_select_list_column_count(
                right_stmt,
                self.database,
                Some(cte_results),
            ) {
                if right_col_count != left_col_count {
                    let operator = match (&set_op.op, set_op.all) {
                        (vibesql_ast::SetOperator::Union, true) => "UNION ALL",
                        (vibesql_ast::SetOperator::Union, false) => "UNION",
                        (vibesql_ast::SetOperator::Intersect, true) => "INTERSECT ALL",
                        (vibesql_ast::SetOperator::Intersect, false) => "INTERSECT",
                        (vibesql_ast::SetOperator::Except, true) => "EXCEPT ALL",
                        (vibesql_ast::SetOperator::Except, false) => "EXCEPT",
                    };
                    return Err(ExecutorError::SetOperationColumnMismatch {
                        operator: operator.to_string(),
                    });
                }
            }
        }

        let has_aggregates =
            self.has_aggregates(&right_stmt.select_list) || right_stmt.having.is_some();
        let has_group_by = right_stmt.group_by.is_some();

        // Resolve SELECT aliases in WHERE clause BEFORE predicate pushdown (SQLite extension)
        let resolved_where = right_stmt.where_clause.as_ref().map(|where_expr| {
            crate::select::order::resolve_where_aliases(where_expr, &right_stmt.select_list)
        });

        let right_results = if has_aggregates || has_group_by {
            self.execute_with_aggregation(right_stmt, cte_results)?
        } else if let Some(from_clause) = &right_stmt.from {
            // Note: LIMIT is None for set operation sides - it's applied after the set operation
            // Pass select_list for table elimination optimization (#3556)
            let from_result = self.execute_from_with_where(
                from_clause,
                cte_results,
                resolved_where.as_ref(),
                right_stmt.order_by.as_deref(),
                None,
                Some(&right_stmt.select_list),
            )?;
            self.execute_without_aggregation(right_stmt, from_result, cte_results)?
        } else if let Some(values_rows) = &right_stmt.values {
            // Handle standalone VALUES in set operation right side (Issue #4546)
            // Thread CTE context so subqueries in VALUES rows can reference
            // names bound by an enclosing WITH clause (#5353); fall back to
            // the outer context when no local CTEs were materialized.
            let cte_ctx =
                if !cte_results.is_empty() { Some(cte_results) } else { self.cte_context };
            let from_result = crate::select::scan::values::execute_values(
                values_rows,
                "_values_",
                None,
                Some(self.database),
                cte_ctx,
                self.outer_row,
                self.outer_schema,
            )?;
            from_result.into_rows()
        } else {
            self.execute_select_without_from(right_stmt, cte_results)?
        };

        // Track memory for right result before set operation
        let right_size = estimate_result_size(&right_results);
        self.track_memory_allocation(right_size)?;

        // Apply the current operation with collation-aware comparison
        left_results = apply_set_operation(left_results, right_results, set_op, collations)?;

        // Track memory for combined result after set operation
        let combined_size = estimate_result_size(&left_results);
        self.track_memory_allocation(combined_size)?;

        // If the right side has more set operations, continue processing them
        // This creates the left-to-right evaluation: ((A op B) op C) op D
        if let Some(next_set_op) = &right_stmt.set_operation {
            left_results = self.execute_set_operations(
                left_results,
                next_set_op,
                cte_results,
                collations,
                expected_col_count, // Pass through the expected column count
            )?;
        }

        Ok(left_results)
    }

    /// Resolve a column name to an index in set operation ORDER BY
    ///
    /// Tries to match by:
    /// 1. Numeric column reference (e.g., "1" -> column 0)
    /// 2. Alias name from first branch's column info
    /// 3. Alias name from any UNION branch (SQLite compatibility)
    fn resolve_order_by_column(
        column: &str,
        column_info: &[(String, Option<String>)],
        all_union_aliases: &[Vec<Option<String>>],
    ) -> Option<usize> {
        // Try to parse as numeric column reference first
        if let Ok(n) = column.parse::<usize>() {
            return Some(n.saturating_sub(1));
        }

        // Try to match by alias name first, then by original expression name
        // Use case-insensitive matching for SQLite compatibility
        let col_idx = column_info.iter().position(|(alias, orig)| {
            alias.eq_ignore_ascii_case(column)
                || orig.as_ref().is_some_and(|o| o.eq_ignore_ascii_case(column))
        });

        // If not found, check if any branch has this alias (SQLite compatibility)
        // SQLite allows ORDER BY to reference aliases from any UNION branch
        col_idx.or_else(|| {
            // Check all positions in all branches
            for branch_aliases in all_union_aliases {
                for (idx, alias_opt) in branch_aliases.iter().enumerate() {
                    if let Some(alias) = alias_opt {
                        if alias.eq_ignore_ascii_case(column) {
                            return Some(idx);
                        }
                    }
                }
            }
            None
        })
    }

    /// Sort set operation results by ORDER BY clause
    ///
    /// ORDER BY on a UNION/INTERSECT/EXCEPT can use:
    /// 1. Column positions (e.g., ORDER BY 1, 2 DESC)
    /// 2. Column names from the first SELECT (e.g., ORDER BY x)
    /// 3. Original column names from expressions (e.g., ORDER BY a when SELECT a AS x)
    ///
    /// The all_union_aliases parameter provides expanded column names (with wildcards resolved)
    /// from all UNION branches for name resolution.
    ///
    /// The column_collations parameter provides collations inherited from the first SELECT's
    /// column definitions (e.g., columns defined with COLLATE NOCASE in CREATE TABLE).
    pub(super) fn sort_set_operation_results(
        &self,
        mut rows: Vec<vibesql_storage::Row>,
        order_by: &[vibesql_ast::OrderByItem],
        select_list: &[vibesql_ast::SelectItem],
        all_union_aliases: &[Vec<Option<String>>], /* Aliases from all UNION branches (wildcards
                                                    * expanded) */
        column_collations: &[Option<String>], // Inherited collations from first SELECT columns
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Build column name map from the first branch's aliases (with wildcards already expanded)
        // The all_union_aliases already contains expanded column names from collect_union_aliases
        let column_info: Vec<(String, Option<String>)> =
            if let Some(first_branch) = all_union_aliases.first() {
                first_branch
                    .iter()
                    .map(|name_opt| {
                        let name = name_opt.clone().unwrap_or_else(|| "?column?".to_string());
                        (name, None) // Original name is the same as alias for expanded wildcards
                    })
                    .collect()
            } else {
                Vec::new()
            };

        // Use the provided column_collations which were extracted with schema lookup
        // This properly handles COLLATE NOCASE from column definitions
        let inherited_collations = column_collations;

        // Parse order_by items and resolve to column indices
        // (column_index, is_desc, nulls_first, collation)
        let mut sort_columns: Vec<(usize, bool, bool, Option<String>)> = Vec::new();
        for (term_index, item) in order_by.iter().enumerate() {
            // Extract column index and optional collation (explicit COLLATE in ORDER BY)
            let (col_idx_opt, explicit_collation) = match &item.expr {
                // Numeric literal: ORDER BY 1
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(n)) => {
                    (Some((*n as usize).saturating_sub(1)), None) // 1-based to 0-based
                }
                // Column reference: ORDER BY x or ORDER BY a
                vibesql_ast::Expression::ColumnRef(col_id) => {
                    let column = col_id.column_canonical();
                    (Self::resolve_order_by_column(column, &column_info, all_union_aliases), None)
                }
                // COLLATE expression: ORDER BY x COLLATE nocase
                // Extract the expression and resolve it
                vibesql_ast::Expression::Collate { expr, collation } => {
                    let col_idx = match expr.as_ref() {
                        vibesql_ast::Expression::ColumnRef(col_id) => {
                            let column = col_id.column_canonical();
                            Self::resolve_order_by_column(column, &column_info, all_union_aliases)
                        }
                        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(n)) => {
                            Some((*n as usize).saturating_sub(1))
                        }
                        // For complex expressions inside COLLATE, match against select_list
                        other_expr => select_list.iter().enumerate().find_map(|(idx, item)| {
                            if let vibesql_ast::SelectItem::Expression {
                                expr: select_expr, ..
                            } = item
                            {
                                if select_expr == other_expr {
                                    return Some(idx);
                                }
                            }
                            None
                        }),
                    };
                    (col_idx, Some(collation.clone()))
                }
                // Complex expressions (aggregates, arithmetic, etc.): match against first SELECT's
                // select_list SQLite allows ORDER BY to reference expressions from
                // the first SELECT e.g., SELECT count(*) FROM t UNION SELECT n FROM
                // t2 ORDER BY count(*)
                other_expr => {
                    let col_idx = select_list.iter().enumerate().find_map(|(idx, item)| {
                        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                            if expr == other_expr {
                                return Some(idx);
                            }
                        }
                        None
                    });
                    (col_idx, None)
                }
            };

            if let Some(col_idx) = col_idx_opt {
                // Validate column index is within bounds
                if col_idx >= column_info.len() {
                    return Err(ExecutorError::OrderByOutOfRange {
                        term_position: term_index + 1,
                        column_number: (col_idx + 1) as i64,
                        select_list_len: column_info.len(),
                    });
                }
                let is_desc = item.direction == vibesql_ast::OrderDirection::Desc;
                // Determine NULL ordering:
                // - If explicitly specified via NULLS FIRST/LAST, use that
                // - Default: SQLite treats NULL as the smallest value:
                //   - ASC order: NULL comes first (before all other values)
                //   - DESC order: NULL comes last (after all other values)
                let nulls_first = match item.nulls_order {
                    Some(vibesql_ast::NullsOrder::First) => true,
                    Some(vibesql_ast::NullsOrder::Last) => false,
                    None => !is_desc, // SQLite default: NULL is smallest
                };
                // Use explicit collation if specified, otherwise inherit from column schema
                let collation = explicit_collation
                    .or_else(|| inherited_collations.get(col_idx).cloned().flatten());
                sort_columns.push((col_idx, is_desc, nulls_first, collation));
            } else {
                // ORDER BY term doesn't match any column in the result set
                return Err(ExecutorError::OrderByTermNotInResultSet {
                    term_position: term_index + 1,
                });
            }
        }

        if sort_columns.is_empty() {
            return Ok(rows);
        }

        rows.sort_by(|a, b| {
            for (col_idx, is_desc, nulls_first, collation) in &sort_columns {
                let val_a = a.values.get(*col_idx);
                let val_b = b.values.get(*col_idx);

                // Handle NULLs according to nulls_first setting
                let cmp = match (
                    val_a.map(|v| v.is_null()).unwrap_or(true),
                    val_b.map(|v| v.is_null()).unwrap_or(true),
                ) {
                    (true, true) => Ordering::Equal,
                    (true, false) => {
                        if *nulls_first {
                            return Ordering::Less; // NULL sorts before non-NULL
                        } else {
                            return Ordering::Greater; // NULL sorts after non-NULL
                        }
                    }
                    (false, true) => {
                        if *nulls_first {
                            return Ordering::Greater; // non-NULL sorts after NULL
                        } else {
                            return Ordering::Less; // non-NULL sorts before NULL
                        }
                    }
                    (false, false) => {
                        // Compare non-NULL values
                        match (val_a, val_b) {
                            (Some(a_val), Some(b_val)) => {
                                // Apply collation if specified
                                let cmp = compare_sql_values_with_collation(
                                    a_val,
                                    b_val,
                                    collation.as_deref(),
                                );
                                if *is_desc {
                                    cmp.reverse()
                                } else {
                                    cmp
                                }
                            }
                            _ => Ordering::Equal, // Shouldn't happen since we checked is_null above
                        }
                    }
                };

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        Ok(rows)
    }
}

/// Extract column names from a FROM clause for wildcard expansion
/// Returns column names from all tables in the FROM clause in order
pub(super) fn extract_column_names_from_from(
    database: &vibesql_storage::Database,
    from_clause: Option<&vibesql_ast::FromClause>,
) -> Vec<String> {
    fn extract_from_clause(
        database: &vibesql_storage::Database,
        from: &vibesql_ast::FromClause,
        columns: &mut Vec<String>,
    ) {
        match from {
            vibesql_ast::FromClause::Table { name, .. } => {
                // Look up the table in the database and get its column names
                if let Some(table) = database.get_table(name) {
                    for col in &table.schema.columns {
                        columns.push(col.name.clone());
                    }
                }
            }
            vibesql_ast::FromClause::Join { left, right, .. } => {
                // For joins, collect columns from both sides
                extract_from_clause(database, left, columns);
                extract_from_clause(database, right, columns);
            }
            vibesql_ast::FromClause::Subquery { .. } => {
                // Subqueries are complex - we'd need to analyze the subquery
                // For now, leave as unknown
            }
            vibesql_ast::FromClause::Values { .. } => {
                // VALUES clause doesn't have named columns
            }
            vibesql_ast::FromClause::TableFunction { .. } => {
                // Table functions have no statically known named columns here
            }
        }
    }

    let mut columns = Vec::new();
    if let Some(from) = from_clause {
        extract_from_clause(database, from, &mut columns);
    }
    columns
}

/// Collect aliases and column names from a SELECT's select_list
/// Expands wildcards using the database schema when possible
pub(super) fn collect_select_aliases(
    database: &vibesql_storage::Database,
    stmt: &vibesql_ast::SelectStmt,
) -> Vec<Option<String>> {
    let mut aliases = Vec::new();

    // Pre-compute column names from FROM clause for wildcard expansion
    let from_columns = extract_column_names_from_from(database, stmt.from.as_ref());
    let mut from_col_idx = 0;

    for item in &stmt.select_list {
        match item {
            vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                // Use explicit alias if present, otherwise derive from expression
                let name = if let Some(a) = alias {
                    Some(a.clone())
                } else {
                    // Derive from expression - use column name for ColumnRef
                    match expr {
                        vibesql_ast::Expression::ColumnRef(col_id) => {
                            Some(col_id.column_canonical().to_string())
                        }
                        _ => None,
                    }
                };
                aliases.push(name);
            }
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // Expand wildcard to actual column names from FROM clause
                if from_col_idx < from_columns.len() {
                    // Push all remaining columns from FROM clause
                    for col_name in &from_columns[from_col_idx..] {
                        aliases.push(Some(col_name.to_string()));
                    }
                    from_col_idx = from_columns.len();
                } else {
                    // Fallback: no FROM clause info available
                    aliases.push(None);
                }
            }
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Expand qualified wildcard (e.g., t.*) to columns from that table
                if let Some(table) = database.get_table(qualifier) {
                    for col in &table.schema.columns {
                        aliases.push(Some(col.name.clone()));
                    }
                } else {
                    // Table not found - might be an alias, fallback to unknown
                    aliases.push(None);
                }
            }
        }
    }

    aliases
}

/// Collect aliases and column names from all SELECT statements in a UNION chain
/// Returns a vec where each element is a vec of aliases/column names for that SELECT's columns
///
/// For each SELECT item, this returns:
/// - The explicit alias if present (e.g., "x" in SELECT col AS x)
/// - The column name if it's a ColumnRef without an alias (e.g., "col" in SELECT col)
/// - For wildcards: The actual column names from the table schema
/// - None for complex expressions without aliases
pub(super) fn collect_union_aliases(
    database: &vibesql_storage::Database,
    stmt: &vibesql_ast::SelectStmt,
) -> Vec<Vec<Option<String>>> {
    let mut all_aliases = Vec::new();

    // Collect from the main SELECT (with wildcard expansion)
    all_aliases.push(collect_select_aliases(database, stmt));

    // Recursively collect from set operations
    let mut current_set_op = stmt.set_operation.as_ref();
    while let Some(set_op) = current_set_op {
        all_aliases.push(collect_select_aliases(database, &set_op.right));
        current_set_op = set_op.right.set_operation.as_ref();
    }

    all_aliases
}
