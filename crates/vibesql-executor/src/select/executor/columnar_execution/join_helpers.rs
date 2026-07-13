//! JOIN helper functions for columnar execution
//!
//! This module contains free functions that support the JOIN execution path,
//! including join tree flattening, condition extraction, and schema building.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{BinaryOperator, Expression, FromClause, JoinType};

use crate::{errors::ExecutorError, schema::CombinedSchema, select::columnar};

/// Check if a FROM clause only contains join types supported by the columnar path
///
/// Supported join types:
/// - INNER JOIN (explicit `JOIN ... ON` syntax)
/// - CROSS JOIN (comma-separated tables `FROM a, b`)
/// - LEFT OUTER JOIN (preserves all left rows, NULLs for unmatched right)
/// - RIGHT OUTER JOIN (preserves all right rows, NULLs for unmatched left)
///
/// FULL OUTER, SEMI, and ANTI joins are not yet supported in the columnar path.
pub(super) fn is_columnar_supported_join(from: &FromClause) -> bool {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } | FromClause::Values { .. } => true,
        // Table functions are not yet executable via the columnar path.
        FromClause::TableFunction { .. } => false,
        FromClause::Join { left, right, join_type, .. } => {
            matches!(
                join_type,
                JoinType::Inner | JoinType::Cross | JoinType::LeftOuter | JoinType::RightOuter
            ) && is_columnar_supported_join(left)
                && is_columnar_supported_join(right)
        }
    }
}

/// Check if a FROM clause contains a CROSS JOIN with a join condition
///
/// CROSS JOIN with ON condition is semantically invalid SQL.
/// CROSS JOIN with USING clause or NATURAL CROSS JOIN should be treated as INNER JOIN
/// and require special handling that the columnar path doesn't support.
///
/// We detect these cases to fall back to regular execution which handles them correctly.
pub(super) fn has_cross_join_with_on_condition(from: &FromClause) -> bool {
    match from {
        FromClause::Table { .. }
        | FromClause::Subquery { .. }
        | FromClause::Values { .. }
        | FromClause::TableFunction { .. } => false,
        FromClause::Join { left, right, join_type, condition, using_columns, natural, .. } => {
            // CROSS JOIN with any join condition (ON, USING, or NATURAL) should fall back
            // to regular execution path which handles filtering and column deduplication
            if matches!(join_type, JoinType::Cross)
                && (condition.is_some() || using_columns.is_some() || *natural)
            {
                return true;
            }
            // Recursively check children
            has_cross_join_with_on_condition(left) || has_cross_join_with_on_condition(right)
        }
    }
}

/// Simple table reference: (name, alias, is_subquery)
pub(super) type SimpleTableRef = (String, Option<String>, bool);

/// Flatten a join tree into a list of simple table references
pub(super) fn flatten_join_tree_simple(from: &FromClause, tables: &mut Vec<SimpleTableRef>) {
    match from {
        FromClause::Table { name, alias, .. } => {
            tables.push((name.clone(), alias.clone(), false));
        }
        FromClause::Subquery { alias, .. } => {
            tables.push((alias.clone(), Some(alias.clone()), true));
        }
        FromClause::Values { alias, .. } => {
            tables.push((alias.clone(), Some(alias.clone()), true));
        }
        FromClause::TableFunction { .. } => {
            // Guarded by is_columnar_supported_join (returns false for TVFs), so
            // the columnar flatten path is never reached with a table function.
            unreachable!("table functions are not executable via the columnar path (JSON1 Phase 3)")
        }
        FromClause::Join { left, right, .. } => {
            flatten_join_tree_simple(left, tables);
            flatten_join_tree_simple(right, tables);
        }
    }
}

/// Flatten a join tree into a list of table references with their join types.
///
/// The first table in the list has no join type (it's the leftmost table in the tree).
/// Each subsequent table has the JoinType that connects it to the previously joined tables.
///
/// For a query like `FROM a INNER JOIN b ON ... LEFT JOIN c ON ...`, this produces:
/// - (a_info, None)
/// - (b_info, Some(Inner))
/// - (c_info, Some(LeftOuter))
pub(super) fn flatten_join_tree_with_types(
    from: &FromClause,
    tables: &mut Vec<(SimpleTableRef, Option<JoinType>)>,
) {
    match from {
        FromClause::Table { name, alias, .. } => {
            tables.push(((name.clone(), alias.clone(), false), None));
        }
        FromClause::Subquery { alias, .. } => {
            tables.push(((alias.clone(), Some(alias.clone()), true), None));
        }
        FromClause::Values { alias, .. } => {
            tables.push(((alias.clone(), Some(alias.clone()), true), None));
        }
        FromClause::TableFunction { .. } => {
            unreachable!("table functions are not executable via the columnar path (JSON1 Phase 3)")
        }
        FromClause::Join { left, right, join_type, .. } => {
            flatten_join_tree_with_types(left, tables);
            // The right side of this join node gets the join type
            match right.as_ref() {
                FromClause::Table { name, alias, .. } => {
                    tables.push(((name.clone(), alias.clone(), false), Some(join_type.clone())));
                }
                FromClause::Subquery { alias, .. } => {
                    tables.push((
                        (alias.clone(), Some(alias.clone()), true),
                        Some(join_type.clone()),
                    ));
                }
                FromClause::Values { alias, .. } => {
                    tables.push((
                        (alias.clone(), Some(alias.clone()), true),
                        Some(join_type.clone()),
                    ));
                }
                FromClause::TableFunction { .. } => {
                    unreachable!(
                        "table functions are not executable via the columnar path (JSON1 Phase 3)"
                    )
                }
                FromClause::Join { .. } => {
                    // Nested join on the right side - flatten it but mark the first
                    // entry with this join's type
                    let start_idx = tables.len();
                    flatten_join_tree_with_types(right, tables);
                    // Override the join type of the first table from the nested join
                    if start_idx < tables.len() {
                        tables[start_idx].1 = Some(join_type.clone());
                    }
                }
            }
        }
    }
}

/// Equi-join condition: left_table.left_column = right_table.right_column
#[derive(Debug, Clone)]
pub(super) struct EquiJoinCondition {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_table: Option<String>,
    pub right_column: String,
}

/// Extract join conditions from a FROM clause (ON conditions)
pub(super) fn extract_join_conditions(from: &FromClause, conditions: &mut Vec<EquiJoinCondition>) {
    match from {
        FromClause::Table { .. }
        | FromClause::Subquery { .. }
        | FromClause::Values { .. }
        | FromClause::TableFunction { .. } => {}
        FromClause::Join { left, right, condition, join_type, .. } => {
            // Handle INNER, CROSS, LEFT OUTER, and RIGHT OUTER joins in columnar path
            // FULL OUTER, SEMI, and ANTI joins are not supported
            if !matches!(
                join_type,
                JoinType::Inner | JoinType::Cross | JoinType::LeftOuter | JoinType::RightOuter
            ) {
                return;
            }

            // Extract ON conditions (CROSS joins typically don't have ON conditions -
            // their join predicates are in the WHERE clause which is handled separately)
            if let Some(cond) = condition {
                extract_equijoin_conditions(cond, conditions);
            }

            extract_join_conditions(left, conditions);
            extract_join_conditions(right, conditions);
        }
    }
}

/// Check whether any ON clause in the join tree carries a residual
/// (non-equi-join) conjunct that the columnar fast path would silently drop.
///
/// The columnar join path extracts only `col = col` equi-join key pairs from
/// each ON clause (see [`extract_equijoin_conditions`]) and ignores every other
/// sub-expression. A compound ON clause such as `ON t1.b = t2.x AND t1.c = 1`
/// has a residual conjunct (`t1.c = 1`) that is never forwarded to the columnar
/// probe, so key matches that should be NULL-padded are wrongly emitted with
/// matched right-side values (issue #5702).
///
/// This affects INNER, LEFT OUTER, and RIGHT OUTER columnar joins — all of
/// which dispatch through the same chain. When this returns `true`, the caller
/// must fall back to the row-based join path, which evaluates residual ON
/// conjuncts correctly during the probe.
///
/// A pure equi-join ON clause — even one that is an AND of multiple `col = col`
/// conditions (e.g. `ON a.x = b.x AND a.y = b.y`) — returns `false` and stays
/// on the columnar fast path.
pub(super) fn join_tree_has_residual_on_conjuncts(from: &FromClause) -> bool {
    match from {
        FromClause::Table { .. }
        | FromClause::Subquery { .. }
        | FromClause::Values { .. }
        | FromClause::TableFunction { .. } => false,
        FromClause::Join { left, right, condition, join_type, .. } => {
            // Only the join types handled by the columnar path matter here;
            // unsupported types bail out earlier via is_columnar_supported_join.
            if matches!(
                join_type,
                JoinType::Inner | JoinType::Cross | JoinType::LeftOuter | JoinType::RightOuter
            ) {
                if let Some(cond) = condition {
                    if !on_expression_is_pure_equijoin(cond) {
                        return true;
                    }
                }
            }
            join_tree_has_residual_on_conjuncts(left) || join_tree_has_residual_on_conjuncts(right)
        }
    }
}

/// Returns `true` iff every top-level AND conjunct of `expr` is a pure
/// equi-join condition `col = col` (the only shape the columnar probe honors).
///
/// Any other conjunct (a literal comparison like `t1.c = 1`, an inequality like
/// `t2.x > 0`, a boolean literal like `true`, a function call, etc.) makes this
/// return `false`, signaling that the columnar path would drop it.
fn on_expression_is_pure_equijoin(expr: &Expression) -> bool {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            on_expression_is_pure_equijoin(left) && on_expression_is_pure_equijoin(right)
        }
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Mirror the extraction rule in extract_equijoin_conditions:
            // a pure equi-join is `col = col` with unqualified-schema columns.
            if let (Expression::ColumnRef(left_col_id), Expression::ColumnRef(right_col_id)) =
                (left.as_ref(), right.as_ref())
            {
                left_col_id.schema_canonical().is_none()
                    && right_col_id.schema_canonical().is_none()
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Extract equi-join conditions from an expression
pub(super) fn extract_equijoin_conditions(
    expr: &Expression,
    conditions: &mut Vec<EquiJoinCondition>,
) {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            extract_equijoin_conditions(left, conditions);
            extract_equijoin_conditions(right, conditions);
        }
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Check if this is col1 = col2 (equi-join)
            if let (Expression::ColumnRef(left_col_id), Expression::ColumnRef(right_col_id)) =
                (left.as_ref(), right.as_ref())
            {
                if left_col_id.schema_canonical().is_none()
                    && right_col_id.schema_canonical().is_none()
                {
                    conditions.push(EquiJoinCondition {
                        left_table: left_col_id.table_canonical().map(|t| t.to_string()),
                        left_column: left_col_id.column_canonical().to_string(),
                        right_table: right_col_id.table_canonical().map(|t| t.to_string()),
                        right_column: right_col_id.column_canonical().to_string(),
                    });
                }
            }
        }
        _ => {}
    }
}

/// Check that every top-level AND conjunct of the WHERE clause is fully
/// consumed by the columnar join pipeline: either a pure equi-join condition
/// (`col = col`, which becomes a hash-join key) or a conjunct that
/// `extract_column_predicates` can represent columnarly.
///
/// Any other conjunct (e.g. a comparison wrapped in a unary operator like
/// `+zY == iB`, an OR, a row-value comparison, ...) would be *silently
/// dropped* by the extraction in `extract_non_join_predicates`, producing
/// over-returned rows. When this returns `false` the caller must fall back to
/// row-oriented execution, which evaluates the full WHERE expression per row.
pub(super) fn where_clause_fully_covered(
    expr: &Expression,
    schema: &CombinedSchema,
    case_sensitive_like: bool,
) -> bool {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            where_clause_fully_covered(left, schema, case_sensitive_like)
                && where_clause_fully_covered(right, schema, case_sensitive_like)
        }
        Expression::Conjunction(children) => children
            .iter()
            .all(|child| where_clause_fully_covered(child, schema, case_sensitive_like)),
        // Pure equi-join conjunct: consumed as a hash-join key (mirrors the
        // extraction rule in extract_equijoin_conditions, including the
        // unqualified-schema requirement — a schema-qualified col = col is
        // neither extracted as a join key nor kept as a filter, so it must
        // trigger the fallback).
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            if let (Expression::ColumnRef(left_col_id), Expression::ColumnRef(right_col_id)) =
                (left.as_ref(), right.as_ref())
            {
                left_col_id.schema_canonical().is_none()
                    && right_col_id.schema_canonical().is_none()
            } else {
                columnar::extract_column_predicates(expr, schema, case_sensitive_like).is_some()
            }
        }
        // A constant-folded always-true WHERE needs no filtering.
        Expression::Literal(vibesql_types::SqlValue::Boolean(true)) => true,
        _ => columnar::extract_column_predicates(expr, schema, case_sensitive_like).is_some(),
    }
}

/// Extract non-join predicates (conditions that aren't col1 = col2)
pub(super) fn extract_non_join_predicates(
    expr: &Expression,
    schema: &CombinedSchema,
    case_sensitive_like: bool,
) -> Option<Vec<columnar::ColumnPredicate>> {
    let mut predicates = Vec::new();
    extract_non_join_predicates_recursive(expr, schema, case_sensitive_like, &mut predicates);
    if predicates.is_empty() {
        None
    } else {
        Some(predicates)
    }
}

fn extract_non_join_predicates_recursive(
    expr: &Expression,
    schema: &CombinedSchema,
    case_sensitive_like: bool,
    predicates: &mut Vec<columnar::ColumnPredicate>,
) {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            extract_non_join_predicates_recursive(left, schema, case_sensitive_like, predicates);
            extract_non_join_predicates_recursive(right, schema, case_sensitive_like, predicates);
        }
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Skip column = column (join conditions)
            if matches!(left.as_ref(), Expression::ColumnRef(_))
                && matches!(right.as_ref(), Expression::ColumnRef(_))
            {
                return;
            }
            // Try to extract as column predicate
            if let Some(pred) =
                columnar::extract_column_predicates(expr, schema, case_sensitive_like)
            {
                predicates.extend(pred);
            }
        }
        _ => {
            // Try to extract other predicates
            if let Some(pred) =
                columnar::extract_column_predicates(expr, schema, case_sensitive_like)
            {
                predicates.extend(pred);
            }
        }
    }
}

/// Build a combined schema from multiple table batches
pub(super) fn build_combined_schema(
    batches: &[(String, Option<String>, columnar::ColumnarBatch, vibesql_catalog::TableSchema)],
) -> CombinedSchema {
    let mut combined = CombinedSchema {
        table_schemas: HashMap::new(),
        total_columns: 0,
        hidden_columns: HashSet::new(),
        always_hidden_columns: HashSet::new(),
        outer_schema: None,
        duplicate_aliases: HashSet::new(),
        joined_columns: HashSet::new(),
        using_coalesce_indices: HashMap::new(),
        column_replacement_map: HashMap::new(),
        alias_tables: HashSet::new(),
        shadowed_tables: HashMap::new(),
    };

    for (table_name, alias, _batch, schema) in batches {
        let name = alias.as_ref().unwrap_or(table_name);
        combined.insert_table(name.clone(), combined.total_columns, schema.clone());
        combined.total_columns += schema.columns.len();
    }

    combined
}

/// Check if a column exists in any of the given tables
pub(super) fn is_column_in_tables(column: &str, tables: &[&str], schema: &CombinedSchema) -> bool {
    tables.iter().any(|t| is_column_in_table(column, t, schema))
}

/// Check if a column exists in a specific table
pub(super) fn is_column_in_table(column: &str, table: &str, schema: &CombinedSchema) -> bool {
    // TableKey lookup is case-insensitive
    if let Some((_, table_schema)) = schema.get_table(table) {
        table_schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case(column))
    } else {
        false
    }
}

/// Resolve join column indices for the current join operation
pub(super) fn resolve_join_column_indices(
    cond: &EquiJoinCondition,
    joined_tables: &[&str],
    new_table: &str,
    new_table_schema: &vibesql_catalog::TableSchema,
    combined_schema: &CombinedSchema,
) -> Result<(usize, usize), ExecutorError> {
    // Determine which side refers to joined tables vs new table
    let left_in_joined = cond.left_table.as_deref().map_or_else(
        || is_column_in_tables(&cond.left_column, joined_tables, combined_schema),
        |t| joined_tables.iter().any(|jt| jt.eq_ignore_ascii_case(t)),
    );

    // Keep each column's table qualifier paired with it through the swap
    let ((left_table, left_col), (right_table, right_col)) = if left_in_joined {
        (
            (cond.left_table.as_deref(), &cond.left_column),
            (cond.right_table.as_deref(), &cond.right_column),
        )
    } else {
        (
            (cond.right_table.as_deref(), &cond.right_column),
            (cond.left_table.as_deref(), &cond.left_column),
        )
    };

    // Ambiguity guard (issue #5870): an unqualified column that exists in 2+ joined
    // tables (e.g. `id` in `ON id=aid`) must NOT silently resolve to the leftmost
    // match. Returning AmbiguousColumnName makes the columnar path fall back to the
    // row-oriented path, which raises the same error through the full evaluator —
    // matching SQLite's "ambiguous column name: id". USING/NATURAL join keys are
    // exempt inside is_column_ambiguous (issue #4517), so they are unaffected.
    if left_table.is_none() && combined_schema.is_column_ambiguous(left_col) {
        return Err(ExecutorError::AmbiguousColumnName { column_name: left_col.clone() });
    }
    if right_table.is_none() && combined_schema.is_column_ambiguous(right_col) {
        return Err(ExecutorError::AmbiguousColumnName { column_name: right_col.clone() });
    }

    // Find the joined-side column index in the combined schema.
    //
    // Issue #5819: the table qualifier MUST be passed through here. Dropping it
    // (a `None` qualifier) made `get_column_index` fall back to leftmost-name
    // matching, so a qualified ref like `b1.id` in a 3+-table join resolved to
    // the FIRST table's `id` column whenever the first table shared the column
    // name — silently joining on the wrong column (0 rows for inner joins,
    // dropped matches for LEFT JOIN chains).
    let left_idx = combined_schema.get_column_index(left_table, left_col).ok_or_else(|| {
        ExecutorError::ColumnNotFound {
            column_name: left_col.clone(),
            table_name: left_table.unwrap_or("").to_string(),
            searched_tables: joined_tables.iter().map(|s| s.to_string()).collect(),
            available_columns: vec![],
        }
    })?;

    // Find right column index in the new table.
    //
    // When the right side carries a qualifier, it must actually refer to the
    // new table; otherwise the condition was mis-classified (e.g. both sides
    // belong to already-joined tables) and joining on it would be incorrect.
    if let Some(rt) = right_table {
        if !rt.eq_ignore_ascii_case(new_table) {
            return Err(ExecutorError::ColumnNotFound {
                column_name: right_col.clone(),
                table_name: rt.to_string(),
                searched_tables: vec![new_table.to_string()],
                available_columns: new_table_schema
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect(),
            });
        }
    }
    let right_idx = new_table_schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(right_col))
        .ok_or_else(|| ExecutorError::ColumnNotFound {
            column_name: right_col.clone(),
            table_name: new_table.to_string(),
            searched_tables: vec![new_table.to_string()],
            available_columns: new_table_schema.columns.iter().map(|c| c.name.clone()).collect(),
        })?;

    Ok((left_idx, right_idx))
}

/// Extract a single table name from a FROM clause if it's a simple table reference
///
/// Returns None if the FROM clause contains JOINs, subqueries, or other complex constructs.
pub(super) fn extract_single_table_name(from_clause: &FromClause) -> Option<String> {
    match from_clause {
        FromClause::Table { name, .. } => Some(name.clone()),
        FromClause::Join { .. } => None, // JOINs not supported in native columnar path
        FromClause::Subquery { .. } => None, // Subqueries not supported
        FromClause::Values { .. } => None, // VALUES not supported
        FromClause::TableFunction { .. } => None, // Table functions not supported
    }
}

/// Extract table name and optional alias from a FROM clause if it's a simple table reference
///
/// Returns (table_name, alias) where alias is the alias if specified, otherwise None.
/// Returns None if the FROM clause contains JOINs, subqueries, or other complex constructs.
///
/// # Issue #4111
/// The alias (if present) must be used as the schema key, since queries reference
/// columns using the alias (e.g., `J.I_CURRENT_PRICE` in `FROM item J`).
pub(super) fn extract_table_name_and_alias(
    from_clause: &FromClause,
) -> Option<(String, Option<String>)> {
    match from_clause {
        FromClause::Table { name, alias, .. } => Some((name.clone(), alias.clone())),
        FromClause::Join { .. } => None, // JOINs not supported in native columnar path
        FromClause::Subquery { .. } => None, // Subqueries not supported
        FromClause::Values { .. } => None, // VALUES not supported
        FromClause::TableFunction { .. } => None, // Table functions not supported
    }
}

#[cfg(test)]
mod residual_on_conjunct_tests {
    use super::join_tree_has_residual_on_conjuncts;
    use vibesql_parser::Parser;

    fn from_clause_of(sql: &str) -> vibesql_ast::FromClause {
        match Parser::parse_sql(sql) {
            Ok(vibesql_ast::Statement::Select(select)) => {
                select.from.expect("query must have a FROM clause")
            }
            other => panic!("expected SELECT, got {:?}", other),
        }
    }

    /// A single `col = col` equi-join ON clause stays on the columnar path.
    #[test]
    fn pure_single_equijoin_has_no_residual() {
        let from = from_clause_of("SELECT * FROM t1 JOIN t2 ON t1.b = t2.x");
        assert!(!join_tree_has_residual_on_conjuncts(&from));
    }

    /// An AND of multiple `col = col` equi-joins is still a pure equi-join and
    /// must NOT trigger the bail-out (regression guard for the guard itself).
    #[test]
    fn and_of_equijoins_has_no_residual() {
        let from = from_clause_of("SELECT * FROM t1 JOIN t2 ON t1.b = t2.x AND t1.c = t2.y");
        assert!(!join_tree_has_residual_on_conjuncts(&from));
    }

    /// A literal-comparison conjunct (`t1.c = 1`) is a residual -> bail out.
    #[test]
    fn literal_equality_conjunct_is_residual() {
        let from = from_clause_of("SELECT * FROM t1 JOIN t2 ON t1.b = t2.x AND t1.c = 1");
        assert!(join_tree_has_residual_on_conjuncts(&from));
    }

    /// An inequality conjunct (`t2.x > 0`) is a residual -> bail out.
    #[test]
    fn inequality_conjunct_is_residual() {
        let from = from_clause_of("SELECT * FROM t1 JOIN t2 ON t1.b = t2.x AND t2.x > 0");
        assert!(join_tree_has_residual_on_conjuncts(&from));
    }

    /// LEFT OUTER join with a compound ON clause is detected.
    #[test]
    fn left_outer_compound_on_is_residual() {
        let from = from_clause_of("SELECT * FROM t1 LEFT JOIN t2 ON t1.b = t2.x AND t1.c = 1");
        assert!(join_tree_has_residual_on_conjuncts(&from));
    }

    /// A residual buried in a nested join tree is still detected.
    #[test]
    fn residual_in_nested_join_is_detected() {
        let from = from_clause_of(
            "SELECT * FROM t1 JOIN t2 ON t1.b = t2.x JOIN t3 ON t2.y = t3.z AND t3.w = 5",
        );
        assert!(join_tree_has_residual_on_conjuncts(&from));
    }

    /// A multi-table chain of pure equi-joins stays columnar.
    #[test]
    fn nested_pure_equijoins_have_no_residual() {
        let from = from_clause_of("SELECT * FROM t1 JOIN t2 ON t1.b = t2.x JOIN t3 ON t2.y = t3.z");
        assert!(!join_tree_has_residual_on_conjuncts(&from));
    }
}
