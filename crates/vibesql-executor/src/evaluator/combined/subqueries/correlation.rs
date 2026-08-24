//! Correlation detection for subqueries
//!
//! This module provides utilities to detect and extract correlated column
//! references from subqueries. Correlated subqueries reference columns from
//! outer queries, which affects caching and execution strategies.

/// Check whether an unqualified column resolves to one of the subquery's own
/// inner tables by consulting the actual database schema.
///
/// SQL name resolution is innermost-scope-first: an unqualified column that
/// exists on a table in the subquery's own FROM clause binds to that inner
/// table, NOT to a same-named column in the outer query. The correlation walk
/// must therefore treat such a column as a local (non-correlation) reference.
///
/// Historically this check only consulted the *outer* schema (issue #4761),
/// which handled the self-join case (inner table name == outer table name) but
/// silently failed whenever the inner and outer tables were *different* tables:
/// the inner table is never present in the outer schema, so the lookup always
/// returned `None` and the column was misclassified as an outer correlation
/// reference (issue #5880). Passing the `Database` lets us inspect the inner
/// table's real column list and apply innermost-scope-first resolution.
fn unqualified_column_in_inner_tables(
    database: Option<&vibesql_storage::Database>,
    subquery_tables: &[String],
    column: &str,
) -> bool {
    let Some(db) = database else {
        return false;
    };
    subquery_tables.iter().any(|table_name| {
        db.get_table(table_name).is_some_and(|table| table.schema.has_column(column))
    })
}

/// Whether `column` names a rowid pseudo-column (`rowid`, `_rowid_`, `oid`).
///
/// A correlated subquery may reference the *outer* table's rowid, e.g.
/// `(SELECT y FROM d2 WHERE d2.rowid = d1.rowid)`. The outer `d1.rowid` is not a
/// real column in the outer schema, so `get_column_index` returns `None` and the
/// naive walk drops it — producing an empty correlation set, an identical cache
/// key for every outer row, and thus the first row's result for all rows
/// (rowvalue9 4.tn.4 / 4.tn.6, issue #6045). Rowid correlation refs are tracked
/// separately and resolved from the outer `Row`'s `row_id` / `row_ids`.
fn is_rowid_alias(column: &str) -> bool {
    let c = column.to_ascii_lowercase();
    c == "rowid" || c == "_rowid_" || c == "oid"
}

/// Extract correlation values from the current row for correlated subquery caching
///
/// For correlated subqueries, we need to identify which columns from the outer query
/// are referenced within the subquery, and extract their values from the current row.
/// These values, combined with the subquery hash, form a composite cache key.
///
/// # Implementation
///
/// This function collects all column references in the subquery that belong to the
/// outer schema (not the subquery's own FROM clause). The values of these columns
/// from the current row are extracted and returned in a deterministic order.
///
/// The optional `database` is consulted to resolve unqualified column names
/// against the subquery's own inner tables (innermost-scope-first resolution,
/// issue #5880). When `None`, the function falls back to the outer-schema-only
/// heuristic, which correctly handles self-joins but may over-report
/// correlation for distinct inner/outer tables that share a column name.
///
/// # Returns
///
/// A Vec of (column_name, value) pairs representing the correlation columns and their
/// values in the current row. Returns None if extraction fails (e.g., column not found).
///
/// # Example
///
/// For TPC-H Q2:
/// ```sql
/// SELECT ... WHERE ps_supplycost = (
///     SELECT MIN(ps_supplycost)
///     FROM partsupp, ...
///     WHERE p_partkey = ps_partkey  -- Correlation: references outer p_partkey
/// )
/// ```
/// Returns: vec![("p_partkey", <value_from_current_row>)]
pub(super) fn extract_correlation_values(
    subquery: &vibesql_ast::SelectStmt,
    row: &vibesql_storage::Row,
    outer_schema: &crate::schema::CombinedSchema,
    database: Option<&vibesql_storage::Database>,
) -> Option<Vec<(String, vibesql_types::SqlValue)>> {
    // Get all tables in the subquery's FROM clause
    let subquery_tables = extract_table_names_from_from_clause(subquery.from.as_ref());

    // Collect correlation column references
    let mut correlation_refs = std::collections::BTreeSet::new(); // Use BTreeSet for deterministic ordering
    collect_correlation_refs(
        subquery,
        outer_schema,
        &subquery_tables,
        database,
        &mut correlation_refs,
    );

    // Extract values from the current row
    let mut correlation_values = Vec::new();
    for (table, column) in correlation_refs {
        match outer_schema.get_column_index(table.as_deref(), &column) {
            Some(idx) => {
                if let Some(value) = row.values.get(idx) {
                    let full_name = if let Some(t) = &table {
                        format!("{}.{}", t, column)
                    } else {
                        column.clone()
                    };
                    correlation_values.push((full_name, value.clone()));
                } else {
                    return None; // Column index out of bounds
                }
            }
            None => {
                // A rowid correlation ref to the outer table isn't a real column,
                // so it has no schema index — resolve it from the outer row's
                // tracked row-id instead (issue #6045). Per-table `row_ids` win
                // for joined outer rows; a single-table outer row uses `row_id`.
                if is_rowid_alias(&column) {
                    let rowid = table
                        .as_deref()
                        .and_then(|t| {
                            row.row_ids.as_ref().and_then(|m| m.get(&t.to_lowercase()).copied())
                        })
                        .or(row.row_id);
                    match rowid {
                        Some(id) => {
                            let full_name = if let Some(t) = &table {
                                format!("{}.{}", t, column)
                            } else {
                                column.clone()
                            };
                            correlation_values
                                .push((full_name, vibesql_types::SqlValue::Integer(id as i64)));
                        }
                        // Correlated on a rowid we can't resolve for this row:
                        // return None so the caller skips caching rather than
                        // caching under a colliding key.
                        None => return None,
                    }
                } else {
                    return None; // Column not found in outer schema
                }
            }
        }
    }

    Some(correlation_values)
}

/// Extract table names from a FROM clause (helper for correlation detection)
fn extract_table_names_from_from_clause(from: Option<&vibesql_ast::FromClause>) -> Vec<String> {
    let mut tables = Vec::new();
    if let Some(from_clause) = from {
        extract_table_names_recursive(from_clause, &mut tables);
    }
    tables
}

/// Recursively extract table names from a FROM clause
fn extract_table_names_recursive(from: &vibesql_ast::FromClause, tables: &mut Vec<String>) {
    match from {
        vibesql_ast::FromClause::Table { name, alias, .. } => {
            tables.push(alias.clone().unwrap_or_else(|| name.clone()));
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            extract_table_names_recursive(left, tables);
            extract_table_names_recursive(right, tables);
        }
        vibesql_ast::FromClause::Subquery { alias, .. } => {
            tables.push(alias.clone());
        }
        vibesql_ast::FromClause::Values { alias, .. } => {
            tables.push(alias.clone());
        }
        vibesql_ast::FromClause::TableFunction { alias, .. } => {
            if let Some(a) = alias {
                tables.push(a.clone());
            }
        }
    }
}

/// Collect all correlation column references from a subquery
fn collect_correlation_refs(
    subquery: &vibesql_ast::SelectStmt,
    outer_schema: &crate::schema::CombinedSchema,
    subquery_tables: &[String],
    database: Option<&vibesql_storage::Database>,
    refs: &mut std::collections::BTreeSet<(Option<String>, String)>,
) {
    // Check WHERE clause
    if let Some(where_clause) = &subquery.where_clause {
        collect_correlation_refs_from_expr(
            where_clause,
            outer_schema,
            subquery_tables,
            database,
            refs,
        );
    }

    // Check SELECT list
    for item in &subquery.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
        }
    }

    // Check HAVING clause
    if let Some(having) = &subquery.having {
        collect_correlation_refs_from_expr(having, outer_schema, subquery_tables, database, refs);
    }

    // Check GROUP BY
    if let Some(group_by) = &subquery.group_by {
        for expr in group_by.all_expressions() {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
        }
    }

    // Check a top-level VALUES form (`(VALUES(x),(x))` as a scalar subquery).
    // These rows are stored in `subquery.values`, not in a FROM clause, so
    // without this check a correlated VALUES scalar subquery is treated as
    // uncorrelated, cached once, and returns the same value for every outer
    // row (e.g. `SELECT (VALUES(x),(x)) FROM t1` yielding 1,1 instead of 1,2).
    // (values.test §6.1)
    if let Some(rows) = &subquery.values {
        for row in rows {
            for expr in row {
                collect_correlation_refs_from_expr(
                    expr,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
        }
    }

    // FIX for issue #4994: Check FROM clause for derived tables that reference outer columns
    // This is critical for queries like:
    //   SELECT x+1 FROM (SELECT f/d AS x FROM t2 JOIN t3 ON d*a=f)
    // where 'a' in the JOIN condition references an outer table t1.
    // Without this check, the correlation values would be empty, causing incorrect caching.
    if let Some(from) = &subquery.from {
        collect_correlation_refs_from_from_clause(
            from,
            outer_schema,
            subquery_tables,
            database,
            refs,
        );
    }

    // FIX for issue #4749: Also check set operations (INTERSECT, UNION, EXCEPT)
    // When a subquery contains a set operation, the right side may reference
    // outer columns that need to be included in correlation detection.
    // For example: SELECT 123 INTERSECT SELECT c2 FROM t4
    // where c2 references an outer table t2, not t4.
    if let Some(set_op) = &subquery.set_operation {
        // Collect tables from the set operation's right side to know what's "inner"
        let mut all_subquery_tables = subquery_tables.to_vec();
        extract_table_names_recursive_from_select(&set_op.right, &mut all_subquery_tables);

        // Recursively collect correlation refs from the right side of the set operation
        collect_correlation_refs(&set_op.right, outer_schema, &all_subquery_tables, database, refs);
    }
}

/// Recursively collect correlation column references from a FROM clause
///
/// This handles derived tables (subqueries in FROM) and JOIN conditions
/// that may reference columns from outer queries.
fn collect_correlation_refs_from_from_clause(
    from: &vibesql_ast::FromClause,
    outer_schema: &crate::schema::CombinedSchema,
    subquery_tables: &[String],
    database: Option<&vibesql_storage::Database>,
    refs: &mut std::collections::BTreeSet<(Option<String>, String)>,
) {
    match from {
        vibesql_ast::FromClause::Table { .. } => {
            // Simple table reference, no correlation refs
        }
        vibesql_ast::FromClause::Subquery { query, alias, .. } => {
            // For derived tables, we need to:
            // 1. Extract the tables defined within this derived table
            // 2. Recursively check for correlation refs, passing through the outer schema
            let mut nested_tables = subquery_tables.to_vec();
            nested_tables.push(alias.clone());
            // Also add tables from within the derived table's FROM clause
            if let Some(nested_from) = &query.from {
                extract_table_names_recursive(nested_from, &mut nested_tables);
            }
            // Recursively collect correlation refs from the derived table
            collect_correlation_refs(query, outer_schema, &nested_tables, database, refs);
        }
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
            // Check both sides of the join
            collect_correlation_refs_from_from_clause(
                left,
                outer_schema,
                subquery_tables,
                database,
                refs,
            );
            collect_correlation_refs_from_from_clause(
                right,
                outer_schema,
                subquery_tables,
                database,
                refs,
            );
            // Check the join condition for correlation refs
            if let Some(cond) = condition {
                collect_correlation_refs_from_expr(
                    cond,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
        }
        vibesql_ast::FromClause::Values { rows, .. } => {
            // Check VALUES expressions for correlation refs
            for row in rows {
                for expr in row {
                    collect_correlation_refs_from_expr(
                        expr,
                        outer_schema,
                        subquery_tables,
                        database,
                        refs,
                    );
                }
            }
        }
        vibesql_ast::FromClause::TableFunction { args, .. } => {
            // Check table function arguments for correlation refs
            for expr in args {
                collect_correlation_refs_from_expr(
                    expr,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
        }
    }
}

/// Helper to extract table names from a SelectStmt's FROM clause
fn extract_table_names_recursive_from_select(
    stmt: &vibesql_ast::SelectStmt,
    tables: &mut Vec<String>,
) {
    if let Some(from_clause) = &stmt.from {
        extract_table_names_recursive(from_clause, tables);
    }
    // Recursively handle chained set operations
    if let Some(set_op) = &stmt.set_operation {
        extract_table_names_recursive_from_select(&set_op.right, tables);
    }
}

/// Collect correlation column references from an expression
fn collect_correlation_refs_from_expr(
    expr: &vibesql_ast::Expression,
    outer_schema: &crate::schema::CombinedSchema,
    subquery_tables: &[String],
    database: Option<&vibesql_storage::Database>,
    refs: &mut std::collections::BTreeSet<(Option<String>, String)>,
) {
    match expr {
        vibesql_ast::Expression::ColumnRef(col_id) => {
            let table = col_id.table_canonical();
            let column = col_id.column_canonical();
            // Check if this column reference belongs to the outer schema
            if let Some(table_name) = table {
                let table_lower = table_name.to_lowercase();
                if !subquery_tables.iter().any(|t| t.to_lowercase() == table_lower) {
                    // Not in subquery's tables, check if in outer schema
                    if outer_schema.get_column_index(Some(table_name), column).is_some() {
                        refs.insert((Some(table_name.to_string()), column.to_string()));
                    } else if is_rowid_alias(column)
                        && outer_schema.table_schemas.keys().any(|t| t.canonical() == table_lower)
                    {
                        // Outer-table rowid correlation (e.g. `d2.rowid = d1.rowid`
                        // where d1 is the outer table). Track it so its per-row
                        // value goes into the cache key (issue #6045).
                        refs.insert((Some(table_name.to_string()), column.to_string()));
                    }
                }
            } else {
                // Unqualified reference - check if it's in outer schema
                // If subquery_tables is empty (no FROM clause), any column ref
                // MUST be from the outer schema since there's no internal schema
                //
                // FIX for issue #4761: If subquery has tables in its FROM clause, and any
                // of those table names also exist in the outer schema, we must check if the
                // column could belong to the subquery's own tables. If the column exists in
                // a table that both the subquery and outer query reference (same table name),
                // the column resolves to the subquery's scope first (innermost scope wins).
                // We should NOT add it as a correlation reference.
                //
                // FIX for issue #5880: The outer-schema-only check above only catches the
                // self-join case (inner table name == outer table name). When the inner and
                // outer tables are *different* tables that happen to share a column name
                // (e.g. `SELECT MIN(x) FROM u WHERE u.y = y` with outer table `t`), the inner
                // table `u` is never present in the outer schema, so the check misses it and
                // `y` is wrongly reported as an outer correlation reference. Consult the actual
                // database schema so an unqualified name that exists on an inner table resolves
                // innermost-first and is NOT a correlation ref.
                let column_exists_in_subquery_tables = subquery_tables
                    .iter()
                    .any(|t| outer_schema.get_column_index(Some(t), column).is_some())
                    || unqualified_column_in_inner_tables(database, subquery_tables, column);

                if !column_exists_in_subquery_tables {
                    // Column doesn't exist in any subquery table, might be from outer
                    if outer_schema.get_column_index(None, column).is_some() {
                        refs.insert((None, column.to_string()));
                    }
                }
                // If column_exists_in_subquery_tables is true, the column resolves to
                // the subquery's own scope, not a correlation reference
            }
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            collect_correlation_refs_from_expr(left, outer_schema, subquery_tables, database, refs);
            collect_correlation_refs_from_expr(
                right,
                outer_schema,
                subquery_tables,
                database,
                refs,
            );
        }
        vibesql_ast::Expression::UnaryOp { expr, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
        }
        // Row-value constructors, conjunctions, and disjunctions are flat lists of
        // sub-expressions; without recursing into them a correlated column buried
        // in `(a, b) = (x, y)` (or `a = x AND b = y` normalized to a Conjunction)
        // is never collected. That produces an empty correlation set and thus an
        // identical cache key for every outer row, so a correlated scalar subquery
        // returns the first outer row's result to all rows (issue #6045).
        vibesql_ast::Expression::RowValueConstructor(children)
        | vibesql_ast::Expression::Conjunction(children)
        | vibesql_ast::Expression::Disjunction(children) => {
            for child in children {
                collect_correlation_refs_from_expr(
                    child,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
        }
        vibesql_ast::Expression::Collate { expr, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
        }
        vibesql_ast::Expression::IsDistinctFrom { left, right, .. } => {
            collect_correlation_refs_from_expr(left, outer_schema, subquery_tables, database, refs);
            collect_correlation_refs_from_expr(
                right,
                outer_schema,
                subquery_tables,
                database,
                refs,
            );
        }
        vibesql_ast::Expression::IsTruthValue { expr, .. }
        | vibesql_ast::Expression::Extract { expr, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
        }
        vibesql_ast::Expression::Glob { expr, pattern, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
            collect_correlation_refs_from_expr(
                pattern,
                outer_schema,
                subquery_tables,
                database,
                refs,
            );
        }
        vibesql_ast::Expression::Function { args, .. }
        | vibesql_ast::Expression::AggregateFunction { args, .. } => {
            for arg in args {
                collect_correlation_refs_from_expr(
                    arg,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
        }
        vibesql_ast::Expression::IsNull { expr, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_correlation_refs_from_expr(
                    op,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
            for when in when_clauses {
                for condition in &when.conditions {
                    collect_correlation_refs_from_expr(
                        condition,
                        outer_schema,
                        subquery_tables,
                        database,
                        refs,
                    );
                }
                collect_correlation_refs_from_expr(
                    &when.result,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
            if let Some(else_expr) = else_result {
                collect_correlation_refs_from_expr(
                    else_expr,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
            for val in values {
                collect_correlation_refs_from_expr(
                    val,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
            collect_correlation_refs_from_expr(low, outer_schema, subquery_tables, database, refs);
            collect_correlation_refs_from_expr(high, outer_schema, subquery_tables, database, refs);
        }
        vibesql_ast::Expression::Like { expr, pattern, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
            collect_correlation_refs_from_expr(
                pattern,
                outer_schema,
                subquery_tables,
                database,
                refs,
            );
        }
        vibesql_ast::Expression::Cast { expr, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
        }
        vibesql_ast::Expression::Position { substring, string, .. } => {
            collect_correlation_refs_from_expr(
                substring,
                outer_schema,
                subquery_tables,
                database,
                refs,
            );
            collect_correlation_refs_from_expr(
                string,
                outer_schema,
                subquery_tables,
                database,
                refs,
            );
        }
        vibesql_ast::Expression::Trim { removal_char, string, .. } => {
            if let Some(c) = removal_char {
                collect_correlation_refs_from_expr(
                    c,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
            collect_correlation_refs_from_expr(
                string,
                outer_schema,
                subquery_tables,
                database,
                refs,
            );
        }
        vibesql_ast::Expression::Interval { value, .. } => {
            collect_correlation_refs_from_expr(
                value,
                outer_schema,
                subquery_tables,
                database,
                refs,
            );
        }
        // Nested subqueries: recursively collect correlation refs from their contents
        // FIX for issue #4618: Previously we skipped nested subqueries, which caused
        // incorrect caching when an outer subquery contained a nested subquery that
        // referenced outer columns (e.g., SELECT (SELECT (SELECT a)) FROM t1).
        vibesql_ast::Expression::ScalarSubquery(subq)
        | vibesql_ast::Expression::Exists { subquery: subq, .. } => {
            // Recursively collect correlation refs from the nested subquery
            collect_correlation_refs(subq, outer_schema, subquery_tables, database, refs);
        }
        vibesql_ast::Expression::In { subquery: subq, expr, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
            collect_correlation_refs(subq, outer_schema, subquery_tables, database, refs);
        }
        vibesql_ast::Expression::QuantifiedComparison { expr, subquery, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, database, refs);
            collect_correlation_refs(subquery, outer_schema, subquery_tables, database, refs);
        }
        // Window functions: check args, PARTITION BY, ORDER BY for outer column refs.
        // Without this, queries like `(SELECT min(a) OVER ())` over a correlated `a`
        // would compute an empty correlation set, producing the same cache key for
        // every outer row and returning the first row's value to all subsequent rows
        // (window4.test 12.1 — issue #5104).
        vibesql_ast::Expression::WindowFunction { function, over } => {
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            for arg in args {
                collect_correlation_refs_from_expr(
                    arg,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
            // The aggregate FILTER predicate can reference outer columns, e.g.
            // `sum(total) FILTER (WHERE sales.emp != outer.emp) OVER (...)`
            // inside a correlated scalar subquery (window1-10.8). Omitting it
            // yields an empty correlation set, an identical cache key for every
            // outer row, and thus the first row's result for all rows.
            if let vibesql_ast::WindowFunctionSpec::Aggregate { filter: Some(filter), .. } =
                function
            {
                collect_correlation_refs_from_expr(
                    filter,
                    outer_schema,
                    subquery_tables,
                    database,
                    refs,
                );
            }
            if let Some(partition_by) = &over.partition_by {
                for p in partition_by {
                    collect_correlation_refs_from_expr(
                        p,
                        outer_schema,
                        subquery_tables,
                        database,
                        refs,
                    );
                }
            }
            if let Some(order_by) = &over.order_by {
                for ob_item in order_by {
                    collect_correlation_refs_from_expr(
                        &ob_item.expr,
                        outer_schema,
                        subquery_tables,
                        database,
                        refs,
                    );
                }
            }
        }
        // Literals and other expressions don't contribute to correlation
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{BinaryOperator, Expression, FromClause, SelectItem, SelectStmt};

    use super::*;
    use crate::schema::CombinedSchema;

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(name, false))
    }

    fn outer_schema_xy() -> CombinedSchema {
        let columns = vec![
            vibesql_catalog::ColumnSchema {
                name: "x".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
            vibesql_catalog::ColumnSchema {
                name: "y".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                is_exact_integer_type: false,
                collation: None,
            },
        ];
        CombinedSchema::from_table(
            "a2".to_string(),
            vibesql_catalog::TableSchema::new("a2".to_string(), columns),
        )
    }

    fn subquery_with_where(where_clause: Expression) -> SelectStmt {
        SelectStmt {
            hints: Vec::new(),
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::Literal(vibesql_types::SqlValue::Integer(1)),
                alias: None,
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: Some(FromClause::Table {
                index_hint: None,
                name: "a1".to_string(),
                alias: None,
                column_aliases: None,
                quoted: false,
            }),
            where_clause: Some(where_clause),
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        }
    }

    /// Root Cause #2 (issue #6045): a row-value `(a, b) = (x, y)` WHERE clause
    /// must have BOTH outer columns (x, y) collected as correlation values.
    /// Previously `RowValueConstructor` fell through the match and neither was
    /// collected, producing an identical cache key for every outer row.
    #[test]
    fn row_value_where_extracts_both_outer_columns() {
        let outer = outer_schema_xy();
        // (a, b) = (x, y): a,b are inner (a1); x,y are outer (a2).
        let where_clause = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::RowValueConstructor(vec![col("a"), col("b")])),
            right: Box::new(Expression::RowValueConstructor(vec![col("x"), col("y")])),
        };
        let subquery = subquery_with_where(where_clause);

        // Outer row a2: x=10, y=20.
        let row = vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(10),
            vibesql_types::SqlValue::Integer(20),
        ]);

        let values = extract_correlation_values(&subquery, &row, &outer, None)
            .expect("correlation values should extract");
        // Both x and y must be present with their per-row values.
        assert!(
            values.iter().any(|(n, v)| n == "x" && *v == vibesql_types::SqlValue::Integer(10)),
            "x correlation value missing: {:?}",
            values
        );
        assert!(
            values.iter().any(|(n, v)| n == "y" && *v == vibesql_types::SqlValue::Integer(20)),
            "y correlation value missing: {:?}",
            values
        );
    }

    /// A conjunction `a = x AND b = y` (the non-row-value equivalent) also
    /// collects both outer columns — guards the Conjunction arm added alongside
    /// the RowValueConstructor arm.
    #[test]
    fn conjunction_where_extracts_both_outer_columns() {
        let outer = outer_schema_xy();
        let eq = |l: Expression, r: Expression| Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(l),
            right: Box::new(r),
        };
        let where_clause =
            Expression::Conjunction(vec![eq(col("a"), col("x")), eq(col("b"), col("y"))]);
        let subquery = subquery_with_where(where_clause);
        let row = vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Integer(7),
            vibesql_types::SqlValue::Integer(9),
        ]);
        let values = extract_correlation_values(&subquery, &row, &outer, None)
            .expect("correlation values should extract");
        assert!(values.iter().any(|(n, _)| n == "x"));
        assert!(values.iter().any(|(n, _)| n == "y"));
    }
}
