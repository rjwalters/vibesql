//! Column position extraction, validation, and wildcard expansion for ORDER BY.
//!
//! This module handles:
//! - Extracting numeric column positions from ORDER BY expressions (`ORDER BY 1`, `ORDER BY +N`)
//! - Validating column positions are within range
//! - Resolving positions to column names for aggregate queries
//! - Wildcard expansion for position resolution

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Result of extracting a column position from an ORDER BY expression
#[derive(Debug, Clone, Copy)]
pub(crate) enum ColumnPositionResult {
    /// A valid positive column position (1-indexed)
    Position(i64),
    /// A negative column position (always invalid, but we track it for error reporting)
    Negative(i64),
    /// Not a column position expression
    NotAPosition,
}

/// True when `value` can be a positional ORDER BY / GROUP BY column ordinal.
///
/// SQLite only treats an integer ORDER BY / GROUP BY term as a positional
/// column ordinal when its magnitude fits in a signed 32-bit int — i.e.
/// `-2147483647 ..= 2147483647` (`|value| <= i32::MAX`). A literal outside that
/// range — e.g. `ORDER BY -2147483648`, `ORDER BY -2147483649`, or
/// `ORDER BY 4294967296` — is parsed as an ordinary constant expression
/// instead, so no positional range check applies and the query runs (ordering
/// by a constant is a no-op). Note `i32::MIN` (`-2147483648`) is deliberately
/// excluded: SQLite treats it as a constant, not an ordinal. This mirrors
/// SQLite's `resolveOrderByTermToExprList` behaviour and keeps VibeSQL from
/// raising a spurious "term out of range" error for out-of-range ordinals
/// (#6071).
fn fits_ordinal_width(value: i64) -> bool {
    value.unsigned_abs() <= i32::MAX as u64
}

/// Extracts a numeric column position from an ORDER BY expression.
///
/// Handles:
/// - `ORDER BY N` - Integer literal → Position(N)
/// - `ORDER BY +N` - Unary plus with integer → Position(N) (#4418)
/// - `ORDER BY -N` - Unary minus with integer → Negative(N)
/// - Other expressions → NotAPosition
///
/// An integer literal whose value does not fit in a signed 32-bit int is NOT a
/// column ordinal (SQLite treats it as a constant expression); such terms
/// classify as [`ColumnPositionResult::NotAPosition`] so no range check runs
/// (#6071).
pub(crate) fn extract_column_position(expr: &vibesql_ast::Expression) -> ColumnPositionResult {
    match expr {
        // Direct integer literal: ORDER BY 1, ORDER BY 2, etc.
        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(pos)) => {
            if fits_ordinal_width(*pos) {
                ColumnPositionResult::Position(*pos)
            } else {
                ColumnPositionResult::NotAPosition
            }
        }
        // Unary operator with integer
        vibesql_ast::Expression::UnaryOp { op, expr } => {
            if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(pos)) =
                expr.as_ref()
            {
                match op {
                    // ORDER BY +N: treat as ORDER BY N (#4418)
                    vibesql_ast::UnaryOperator::Plus if fits_ordinal_width(*pos) => {
                        ColumnPositionResult::Position(*pos)
                    }
                    // ORDER BY -N: an in-range negative ordinal is always out
                    // of range (< 1); a negative literal whose magnitude exceeds
                    // i32::MAX (e.g. -2147483648, -2147483649) is a constant
                    // expression, not an ordinal.
                    vibesql_ast::UnaryOperator::Minus if fits_ordinal_width(*pos) => {
                        ColumnPositionResult::Negative(*pos)
                    }
                    _ => ColumnPositionResult::NotAPosition,
                }
            } else {
                ColumnPositionResult::NotAPosition
            }
        }
        _ => ColumnPositionResult::NotAPosition,
    }
}

/// Validates a column position and returns an error if out of range.
pub(crate) fn validate_column_position(
    pos: i64,
    column_count: usize,
    term_index: usize,
) -> Result<usize, ExecutorError> {
    if pos <= 0 || (pos as usize) > column_count {
        return Err(ExecutorError::OrderByOutOfRange {
            term_position: term_index + 1, // Convert to 1-indexed for error message
            column_number: pos,
            select_list_len: column_count,
        });
    }
    Ok((pos as usize) - 1) // Convert to 0-indexed
}

/// Resolves a column position to a column name for aggregate queries.
///
/// For aggregate queries, we simply look up the alias/expression at the given position
/// in the SELECT list without wildcard expansion (aggregates don't typically use wildcards).
pub(crate) fn resolve_position_to_column_name(
    idx: usize,
    select_list: &[vibesql_ast::SelectItem],
) -> Option<String> {
    if let vibesql_ast::SelectItem::Expression { expr, alias, .. } = &select_list[idx] {
        Some(if let Some(alias_name) = alias {
            alias_name.clone()
        } else if let vibesql_ast::Expression::ColumnRef(col_id) = expr {
            col_id.column_canonical().to_string()
        } else if let vibesql_ast::Expression::AggregateFunction { name, .. } = expr {
            // For aggregate functions without alias, use the function name (lowercase)
            // This matches the schema column name generated by the aggregate executor
            name.to_lowercase()
        } else {
            format!("col{}", idx + 1)
        })
    } else {
        None
    }
}

/// Result of resolving a column position with wildcard expansion
pub(crate) enum ResolvedPosition<'a> {
    /// The expression at the position (for regular SELECT items)
    Expression(&'a vibesql_ast::Expression),
    /// A column resolved from a wildcard expansion.
    ///
    /// The column is qualified with the canonical name of the table it came
    /// from (when known) so downstream name resolution does not trip the
    /// ambiguity check when the same column name exists in multiple tables
    /// (issue #5231: `SELECT * FROM b, (SELECT x FROM a) ORDER BY 1` must not
    /// fail with "ambiguous column name: x").
    ColumnName {
        /// Canonical table name the column belongs to, if known
        table: Option<String>,
        /// The column name
        column: String,
    },
    /// An already-built expression resolved from a wildcard expansion.
    ///
    /// Used for USING/NATURAL OUTER-JOIN columns whose `SELECT *` output is a
    /// COALESCE over several base-table columns. Sorting by such a positional
    /// reference must use the merged (coalesced) value, not a single base-table
    /// column (issue #5657).
    OwnedExpression(vibesql_ast::Expression),
    /// Position not found (shouldn't happen if validation passed)
    NotFound,
}

/// Convert a `SELECT *` output column's COALESCE chain (a list of absolute row
/// indices) into a [`ResolvedPosition`] for ORDER BY.
///
/// - A single-index chain resolves to a table-qualified column name, matching
///   the existing wildcard behaviour (and keeping #5231's ambiguity fix).
/// - A multi-index chain resolves to a `COALESCE(t1.c, t2.c, ...)` expression so
///   sorting uses the merged USING/NATURAL OUTER-JOIN value (issue #5657).
fn resolve_chain_to_position<'a>(
    chain: &[usize],
    schema: &crate::schema::CombinedSchema,
) -> ResolvedPosition<'a> {
    // Build a qualified ColumnRef for one absolute index, if resolvable.
    let column_ref_for = |idx: usize| -> Option<vibesql_ast::Expression> {
        schema.table_column_for_index(idx).map(|(table, column)| {
            vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                &table, true, &column, true,
            ))
        })
    };

    match chain {
        [] => ResolvedPosition::NotFound,
        [idx] => match schema.table_column_for_index(*idx) {
            Some((table, column)) => ResolvedPosition::ColumnName { table: Some(table), column },
            None => ResolvedPosition::NotFound,
        },
        _ => {
            let args: Vec<vibesql_ast::Expression> =
                chain.iter().filter_map(|&idx| column_ref_for(idx)).collect();
            match args.len() {
                0 => ResolvedPosition::NotFound,
                1 => ResolvedPosition::OwnedExpression(args.into_iter().next().unwrap()),
                _ => ResolvedPosition::OwnedExpression(vibesql_ast::Expression::Function {
                    name: vibesql_ast::FunctionIdentifier::new("coalesce"),
                    args,
                    character_unit: None,
                }),
            }
        }
    }
}

/// Resolves a column position to an expression or column name, handling wildcard expansion.
///
/// This handles SELECT lists that may contain wildcards (`*`) or qualified wildcards (`table.*`)
/// which expand to multiple columns. The position is 0-indexed.
pub(crate) fn resolve_position_with_wildcards<'a>(
    target_col: usize,
    select_list: &'a [vibesql_ast::SelectItem],
    schema: Option<&CombinedSchema>,
) -> ResolvedPosition<'a> {
    let mut current_col = 0;

    for item in select_list {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // Prefer join-aware expansion that mirrors SELECT * output,
                // including USING/NATURAL OUTER-JOIN coalescing (issue #5657).
                if let Some(s) = schema {
                    if let Some(chains) = s.wildcard_output_chains() {
                        let wildcard_cols = chains.len();
                        if target_col < current_col + wildcard_cols {
                            let offset_in_wildcard = target_col - current_col;
                            let chain = &chains[offset_in_wildcard];
                            return resolve_chain_to_position(chain, s);
                        }
                        current_col += wildcard_cols;
                        continue;
                    }
                }

                // Count how many columns this wildcard expands to
                let wildcard_cols = if let Some(s) = schema {
                    s.table_schemas.values().map(|(_, ts)| ts.columns.len()).sum()
                } else {
                    1
                };

                if target_col < current_col + wildcard_cols {
                    // The position is within this wildcard's expanded columns
                    if let Some(s) = schema {
                        let offset_in_wildcard = target_col - current_col;
                        // Collect all columns from all tables in schema order,
                        // tracking which table each column belongs to so the
                        // result can be table-qualified (issue #5231)
                        let mut all_columns: Vec<(usize, String, String)> = Vec::new();
                        for (table_id, (start_idx, table_schema)) in &s.table_schemas {
                            for (i, col) in table_schema.columns.iter().enumerate() {
                                all_columns.push((
                                    start_idx + i,
                                    table_id.table_canonical().to_string(),
                                    col.name.clone(),
                                ));
                            }
                        }
                        all_columns.sort_by_key(|(idx, _, _)| *idx);

                        if offset_in_wildcard < all_columns.len() {
                            let (_, table, column) = all_columns[offset_in_wildcard].clone();
                            return ResolvedPosition::ColumnName { table: Some(table), column };
                        }
                    }
                    return ResolvedPosition::NotFound;
                }
                current_col += wildcard_cols;
            }
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Count how many columns this qualified wildcard expands to
                let qualified_cols = if let Some(s) = schema {
                    s.get_table(qualifier).map(|(_, ts)| ts.columns.len()).unwrap_or(1)
                } else {
                    1
                };

                if target_col < current_col + qualified_cols {
                    // The position is within this qualified wildcard's columns
                    if let Some(s) = schema {
                        if let Some((_, table_schema)) = s.get_table(qualifier) {
                            let offset = target_col - current_col;
                            if offset < table_schema.columns.len() {
                                // Qualify with the (canonicalized) qualifier so
                                // downstream resolution is unambiguous (#5231).
                                // get_table resolves via TableIdentifier::unquoted,
                                // so lowercase folding matches its lookup.
                                return ResolvedPosition::ColumnName {
                                    table: Some(qualifier.to_ascii_lowercase()),
                                    column: table_schema.columns[offset].name.clone(),
                                };
                            }
                        }
                    }
                    return ResolvedPosition::NotFound;
                }
                current_col += qualified_cols;
            }
            vibesql_ast::SelectItem::Expression { expr, .. } => {
                if target_col == current_col {
                    return ResolvedPosition::Expression(expr);
                }
                current_col += 1;
            }
        }
    }

    ResolvedPosition::NotFound
}

/// Count the number of columns in a SELECT list after wildcard expansion
///
/// This correctly handles:
/// - Regular expressions: count as 1 column each
/// - Wildcard (`SELECT *`): expands to all columns from the schema
/// - Qualified wildcards (`SELECT table.*`): expands to all columns from that table
///
/// # Arguments
/// * `select_list` - The SELECT list items
/// * `schema` - Optional schema for wildcard expansion. If None, wildcards count as 1.
pub(crate) fn count_select_columns(
    select_list: &[vibesql_ast::SelectItem],
    schema: Option<&CombinedSchema>,
) -> usize {
    let mut count = 0;
    for item in select_list {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // Expand wildcard to all columns from schema
                if let Some(s) = schema {
                    // Prefer the join-aware output-column count so positional
                    // ORDER BY validation matches the actual SELECT * output
                    // (hidden/coalesced columns collapse to one) (issue #5657).
                    if let Some(chains) = s.wildcard_output_chains() {
                        count += chains.len();
                    } else {
                        // Count all columns from all tables in the schema
                        for (_, table_schema) in s.table_schemas.values() {
                            count += table_schema.columns.len();
                        }
                    }
                } else {
                    // No schema available, count as 1
                    count += 1;
                }
            }
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Expand qualified wildcard to all columns from the specified table
                if let Some(s) = schema {
                    if let Some((_, table_schema)) = s.get_table(qualifier) {
                        count += table_schema.columns.len();
                    } else {
                        // Table not found, count as 1 (error will be caught elsewhere)
                        count += 1;
                    }
                } else {
                    // No schema available, count as 1
                    count += 1;
                }
            }
            vibesql_ast::SelectItem::Expression { .. } => {
                count += 1;
            }
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{Expression, UnaryOperator};
    use vibesql_types::SqlValue;

    fn int_lit(n: i64) -> Expression {
        Expression::Literal(SqlValue::Integer(n))
    }

    fn unary(op: UnaryOperator, n: i64) -> Expression {
        Expression::UnaryOp { op, expr: Box::new(int_lit(n)) }
    }

    #[test]
    fn positive_literals_within_i32_are_positions() {
        assert!(matches!(extract_column_position(&int_lit(1)), ColumnPositionResult::Position(1)));
        assert!(matches!(
            extract_column_position(&int_lit(i32::MAX as i64)),
            ColumnPositionResult::Position(2147483647)
        ));
    }

    #[test]
    fn zero_is_a_position_and_validated_out_of_range() {
        // ORDER BY 0 is an ordinal candidate; range validation rejects it.
        assert!(matches!(extract_column_position(&int_lit(0)), ColumnPositionResult::Position(0)));
    }

    #[test]
    fn unary_plus_within_i32_is_a_position() {
        assert!(matches!(
            extract_column_position(&unary(UnaryOperator::Plus, 2)),
            ColumnPositionResult::Position(2)
        ));
    }

    #[test]
    fn small_negative_is_negative_ordinal() {
        // ORDER BY -1 / -2 are in-range ordinal candidates (always invalid,
        // reported as out of range by validation).
        assert!(matches!(
            extract_column_position(&unary(UnaryOperator::Minus, 1)),
            ColumnPositionResult::Negative(1)
        ));
        assert!(matches!(
            extract_column_position(&unary(UnaryOperator::Minus, i32::MAX as i64)),
            ColumnPositionResult::Negative(2147483647)
        ));
    }

    #[test]
    fn positive_literal_above_i32_max_is_not_a_position() {
        // 2147483648 exceeds i32::MAX: SQLite treats it as a constant
        // expression, not an ordinal (#6071) — no range check applies.
        assert!(matches!(
            extract_column_position(&int_lit(i32::MAX as i64 + 1)),
            ColumnPositionResult::NotAPosition
        ));
        assert!(matches!(
            extract_column_position(&int_lit(4_294_967_297)),
            ColumnPositionResult::NotAPosition
        ));
    }

    #[test]
    fn negative_literal_at_or_below_i32_min_is_not_a_position() {
        // -2147483648 (i32::MIN) and -2147483649 exceed the ordinal magnitude
        // (|value| > i32::MAX): constant expressions, not ordinals (#6071).
        assert!(matches!(
            extract_column_position(&unary(UnaryOperator::Minus, i32::MAX as i64 + 1)),
            ColumnPositionResult::NotAPosition
        ));
        assert!(matches!(
            extract_column_position(&unary(UnaryOperator::Minus, 2_147_483_649)),
            ColumnPositionResult::NotAPosition
        ));
        assert!(matches!(
            extract_column_position(&unary(UnaryOperator::Minus, 9_999_999_999_999)),
            ColumnPositionResult::NotAPosition
        ));
    }

    #[test]
    fn fits_ordinal_width_boundaries() {
        assert!(fits_ordinal_width(0));
        assert!(fits_ordinal_width(1));
        assert!(fits_ordinal_width(i32::MAX as i64)); // 2147483647
        assert!(fits_ordinal_width(-(i32::MAX as i64))); // -2147483647
        assert!(!fits_ordinal_width(i32::MAX as i64 + 1)); // 2147483648
        assert!(!fits_ordinal_width(i32::MIN as i64)); // -2147483648
        assert!(!fits_ordinal_width(i64::MAX));
        assert!(!fits_ordinal_width(i64::MIN));
    }
}
