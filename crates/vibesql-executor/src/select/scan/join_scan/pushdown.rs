//! Right-table predicate pushdown for JOIN operations
//!
//! This module handles pushing predicates down to the right-side scan
//! for SEMI/ANTI joins and filtering nullable-side predicates for OUTER joins.

use std::collections::HashSet;

use crate::optimizer::{
    combine_with_and,
    where_pushdown::{extract_referenced_tables_branch, flatten_conjuncts},
};

use super::predicates::{extract_table_names_from_from_clause, predicate_references_only_tables};

/// Extract right-table-only predicates from a join condition for SEMI/ANTI join pushdown
///
/// For IN subquery to SEMI JOIN conversions, the subquery's WHERE clause predicates are
/// combined into the JOIN condition. This function extracts predicates that only reference
/// the right-side table(s) so they can be pushed down to the right-side scan for index selection.
///
/// Example: For `WHERE ol_i_id IN (SELECT s_i_id FROM stock WHERE s_w_id = ? AND s_quantity < ?)`
/// The JOIN condition is: `ol_i_id = s_i_id AND s_w_id = ? AND s_quantity < ?`
/// This function extracts: `s_w_id = ? AND s_quantity < ?` (right-only predicates)
///
/// Returns None if no right-only predicates can be extracted.
pub(super) fn extract_right_only_predicates(
    right_from: &vibesql_ast::FromClause,
    condition: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
) -> Option<vibesql_ast::Expression> {
    // Get the table name(s) from the right-side FROM clause
    let right_tables = extract_table_names_from_from_clause(right_from);
    if right_tables.is_empty() {
        return None;
    }

    // We need to build a minimal schema to analyze predicates
    // For each table, get its schema from the database
    let mut right_table_set: HashSet<String> = HashSet::new();
    let mut schema_builder = crate::schema::SchemaBuilder::new();

    for table_name in &right_tables {
        // Normalize table name for lookup
        let normalized = table_name.to_lowercase();
        if let Some(table) = database.get_table(&normalized) {
            // Add table to schema builder (duplicates are tracked, not rejected)
            schema_builder.add_table(table_name.clone(), table.schema.clone());
            right_table_set.insert(table_name.clone());
            // Also add normalized version
            right_table_set.insert(normalized);
        }
    }

    if right_table_set.is_empty() {
        return None;
    }

    let right_schema = schema_builder.build();

    // Flatten the condition into conjuncts (AND-separated predicates)
    let conjuncts = flatten_conjuncts(condition);

    // Filter to keep only predicates that reference only right-side tables
    let right_only_predicates: Vec<vibesql_ast::Expression> = conjuncts
        .into_iter()
        .filter(|pred| {
            // extract_referenced_tables_branch returns Option<HashSet<String>>
            // None means the expression couldn't be analyzed (skip it)
            // Some(empty set) means no tables referenced (skip it)
            // Some(non-empty set) - check if all tables are in right_table_set
            match extract_referenced_tables_branch(pred, &right_schema) {
                Some(ref tables) if !tables.is_empty() => tables.iter().all(|t| {
                    let t_lower = t.to_lowercase();
                    right_table_set.contains(t) || right_table_set.contains(&t_lower)
                }),
                _ => false,
            }
        })
        .collect();

    if right_only_predicates.is_empty() {
        return None;
    }

    // Combine predicates with AND
    combine_with_and(right_only_predicates)
}

/// Filter out predicates that reference ONLY the nullable side of an outer join.
///
/// For LEFT/FULL OUTER JOIN, the right side is "nullable" - it can produce NULL values
/// for unmatched rows. Predicates like `right.col IS NULL` test for these join-produced NULLs,
/// not for NULL values stored in the table. If we push such predicates to the right-side scan,
/// they filter on stored NULLs instead of join-produced NULLs, causing incorrect results.
///
/// This function takes a WHERE clause and returns a modified version with nullable-side-only
/// predicates removed. These predicates will be evaluated post-join instead.
///
/// Example: For `LEFT JOIN t2 ON t1.id = t2.tid WHERE t2.tid IS NULL`
/// - Input: `t2.tid IS NULL`
/// - Output: None (the entire predicate references only the nullable side)
///
/// Example: For `LEFT JOIN t2 ON t1.id = t2.tid WHERE t1.id > 5 AND t2.tid IS NULL`
/// - Input: `t1.id > 5 AND t2.tid IS NULL`
/// - Output: `t1.id > 5` (keep left-side predicate, remove right-side predicate)
pub(super) fn filter_out_nullable_side_predicates(
    nullable_side_from: &vibesql_ast::FromClause,
    where_expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
) -> Option<vibesql_ast::Expression> {
    // Get the table name(s) from the nullable-side FROM clause
    let nullable_tables = extract_table_names_from_from_clause(nullable_side_from);
    if nullable_tables.is_empty() {
        // No tables to filter, return original expression
        return Some(where_expr.clone());
    }

    // Build a schema and table set for the nullable side
    let mut nullable_table_set: HashSet<String> = HashSet::new();
    let mut schema_builder = crate::schema::SchemaBuilder::new();

    for table_name in &nullable_tables {
        // Normalize table name for lookup
        let normalized = table_name.to_lowercase();
        if let Some(table) = database.get_table(&normalized) {
            // Add table to schema builder (duplicates are tracked, not rejected)
            schema_builder.add_table(table_name.clone(), table.schema.clone());
            nullable_table_set.insert(table_name.clone());
            // Also add normalized version for case-insensitive matching
            nullable_table_set.insert(normalized);
        } else {
            // Table not found in database - might be a subquery alias
            // Still add to the set so we can check column references
            nullable_table_set.insert(table_name.clone());
            nullable_table_set.insert(normalized);
        }
    }

    let nullable_schema = schema_builder.build();

    // Flatten the WHERE clause into conjuncts (AND-separated predicates)
    let conjuncts = flatten_conjuncts(where_expr);

    // Filter to KEEP predicates that do NOT reference only nullable-side tables
    // (i.e., remove predicates that reference ONLY the nullable side)
    let kept_predicates: Vec<vibesql_ast::Expression> = conjuncts
        .into_iter()
        .filter(|pred| {
            // Issue #5709: An equijoin between two distinct columns (e.g. `t1.b = t2.b`)
            // never tests a join-produced NULL relationship. When both columns live on the
            // nullable side, the equality constrains rows *within* that side's own subtree
            // (e.g. `t1 INNER JOIN t2 ON true`); it is not a "nullable-side predicate" in the
            // sense this function guards against. Stripping it would turn that inner join into
            // a cartesian product and silently drop the WHERE constraint. Keep it so the
            // nullable-side scan can apply it (as a join key or post-join filter).
            //
            // A `col = NULL` literal comparison is NOT a column-to-column equijoin and is left
            // to the table-reference analysis below (it always evaluates false in SQL anyway).
            if is_column_to_column_equijoin(pred) {
                return true;
            }
            // extract_referenced_tables_branch returns Option<HashSet<String>>
            // None means the expression couldn't be analyzed (keep it, evaluate post-join)
            // Some(empty set) means no tables referenced (keep it - e.g., constant expression)
            // Some(non-empty set) - check if ALL tables are in nullable_table_set
            //   If ALL are nullable-side-only -> REMOVE (don't keep)
            //   If ANY are from other tables -> KEEP
            match extract_referenced_tables_branch(pred, &nullable_schema) {
                Some(ref tables) if !tables.is_empty() => {
                    // Check if ALL referenced tables are in the nullable set
                    let all_nullable = tables.iter().all(|t| {
                        let t_lower = t.to_lowercase();
                        nullable_table_set.contains(t) || nullable_table_set.contains(&t_lower)
                    });
                    // KEEP if NOT all-nullable (i.e., at least one non-nullable table referenced)
                    !all_nullable
                }
                Some(_) => true, // Empty set - keep (no table refs, e.g., literals)
                None => {
                    // Couldn't analyze - check if predicate mentions nullable table columns
                    // For safety, keep predicates we can't analyze and evaluate post-join
                    // But first, do a simple check for column references
                    !predicate_references_only_tables(pred, &nullable_table_set)
                }
            }
        })
        .collect();

    // Combine remaining predicates with AND (returns None if empty)
    combine_with_and(kept_predicates)
}

/// Returns true if `pred` is an equality comparison between two distinct column
/// references (i.e. `a.x = b.y`), as opposed to a column/literal comparison.
///
/// Such equijoins are safe to push into the nullable side's subtree because they
/// constrain rows that already coexist there; they never test a NULL produced by
/// the outer join itself. See `filter_out_nullable_side_predicates` and #5709.
fn is_column_to_column_equijoin(pred: &vibesql_ast::Expression) -> bool {
    matches!(
        pred,
        vibesql_ast::Expression::BinaryOp {
            op: vibesql_ast::BinaryOperator::Equal,
            left,
            right,
        } if matches!(**left, vibesql_ast::Expression::ColumnRef(_))
            && matches!(**right, vibesql_ast::Expression::ColumnRef(_))
    )
}
