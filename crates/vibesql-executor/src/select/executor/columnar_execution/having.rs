//! HAVING clause evaluation for columnar GROUP BY results
//!
//! This module provides evaluation of HAVING conditions on the results
//! of columnar GROUP BY execution. The key insight is that after GROUP BY
//! completes, aggregate values are already computed and stored in result rows.
//!
//! ## Result Row Structure
//!
//! After columnar GROUP BY, each result row has:
//! ```text
//! [group_key_col_0, group_key_col_1, ..., agg_0, agg_1, ...]
//! ```
//!
//! ## HAVING Evaluation Strategy
//!
//! 1. Map aggregate function references in HAVING to their column positions
//! 2. Evaluate the HAVING expression using the pre-computed aggregate values
//! 3. Filter out rows where HAVING evaluates to false

use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::{SqlMode, SqlValue};

use crate::{
    errors::ExecutorError,
    evaluator::casting::cast_value,
    schema::CombinedSchema,
    select::columnar::{AggregateOp, AggregateSource, AggregateSpec},
};

/// Filter GROUP BY results using a HAVING clause
///
/// This function evaluates the HAVING expression on each result row from columnar
/// GROUP BY and filters out rows that don't match the condition.
///
/// # Arguments
///
/// * `rows` - Result rows from columnar GROUP BY (group keys + aggregates)
/// * `having_expr` - The HAVING clause expression
/// * `group_col_count` - Number of GROUP BY columns (prefix of each row)
/// * `aggregates` - The aggregate specs (in same order as aggregate columns in rows)
/// * `schema` - Schema for resolving column references
///
/// # Returns
///
/// Filtered rows where HAVING condition is true
pub fn apply_having_filter(
    rows: Vec<Row>,
    having_expr: &Expression,
    group_col_count: usize,
    aggregates: &[AggregateSpec],
    schema: &CombinedSchema,
) -> Result<Vec<Row>, ExecutorError> {
    if rows.is_empty() {
        return Ok(rows);
    }

    let total = rows.len();
    let mut filtered = Vec::with_capacity(total);

    for row in rows {
        let include =
            evaluate_having_on_result_row(having_expr, &row, group_col_count, aggregates, schema)?;

        if include {
            filtered.push(row);
        }
    }

    log::debug!("HAVING filter: {} of {} rows passed", filtered.len(), total);

    Ok(filtered)
}

/// Evaluate a HAVING expression on a single result row
///
/// The result row structure is: [group_cols..., aggregate_values...]
/// We need to map aggregate references in the expression to their positions.
fn evaluate_having_on_result_row(
    expr: &Expression,
    row: &Row,
    group_col_count: usize,
    aggregates: &[AggregateSpec],
    schema: &CombinedSchema,
) -> Result<bool, ExecutorError> {
    let result = evaluate_expr_on_result_row(expr, row, group_col_count, aggregates, schema)?;
    is_truthy(&result)
}

/// Evaluate an expression against a result row, resolving aggregate references
fn evaluate_expr_on_result_row(
    expr: &Expression,
    row: &Row,
    group_col_count: usize,
    aggregates: &[AggregateSpec],
    schema: &CombinedSchema,
) -> Result<SqlValue, ExecutorError> {
    match expr {
        // Aggregate function - look up pre-computed value in result row
        Expression::AggregateFunction { name, args, distinct, .. } => {
            if *distinct {
                return Err(ExecutorError::UnsupportedFeature(
                    "DISTINCT aggregates in HAVING not supported in columnar path".to_string(),
                ));
            }

            let op = match name.to_uppercase().as_str() {
                "SUM" => AggregateOp::Sum,
                "COUNT" => AggregateOp::Count,
                "AVG" => AggregateOp::Avg,
                "MIN" => AggregateOp::Min,
                "MAX" => AggregateOp::Max,
                _ => {
                    return Err(ExecutorError::UnsupportedFeature(format!(
                        "Aggregate function {} not supported in columnar HAVING",
                        name
                    )));
                }
            };

            // Find the aggregate source
            let source = if op == AggregateOp::Count && args.is_empty() {
                AggregateSource::CountStar
            } else if args.len() == 1 {
                match &args[0] {
                    // `COUNT(*)` parses as a ColumnRef whose canonical name is
                    // "*", not `Expression::Wildcard`. `extract_aggregates`
                    // records it as `CountStar`, so HAVING must resolve it the
                    // same way to match the computed aggregate spec (Issue #6009).
                    Expression::ColumnRef(col_id) if col_id.column_canonical() == "*" => {
                        AggregateSource::CountStar
                    }
                    Expression::ColumnRef(col_id) => {
                        if let Some(idx) = schema
                            .get_column_index(col_id.table_canonical(), col_id.column_canonical())
                        {
                            AggregateSource::Column(idx)
                        } else {
                            // Try expression source for complex args
                            AggregateSource::Expression(args[0].clone())
                        }
                    }
                    Expression::Wildcard => AggregateSource::CountStar,
                    other => AggregateSource::Expression(other.clone()),
                }
            } else {
                return Err(ExecutorError::UnsupportedFeature(
                    "Multi-argument aggregates not supported in columnar HAVING".to_string(),
                ));
            };

            // Find matching aggregate in the spec list
            let agg_idx = aggregates
                .iter()
                .position(|spec| spec.op == op && sources_match(&spec.source, &source))
                .ok_or_else(|| {
                    ExecutorError::Other(format!(
                        "Aggregate {}({:?}) not found in computed aggregates",
                        name, source
                    ))
                })?;

            // Aggregate values start after group columns
            let value_idx = group_col_count + agg_idx;
            if value_idx < row.values.len() {
                Ok(row.values[value_idx].clone())
            } else {
                Err(ExecutorError::Other(format!(
                    "Aggregate index {} out of bounds (row has {} values)",
                    value_idx,
                    row.values.len()
                )))
            }
        }

        // Binary operations - evaluate both sides
        Expression::BinaryOp { left, op, right } => {
            let left_val =
                evaluate_expr_on_result_row(left, row, group_col_count, aggregates, schema)?;
            let right_val =
                evaluate_expr_on_result_row(right, row, group_col_count, aggregates, schema)?;
            crate::evaluator::ExpressionEvaluator::eval_binary_op_static(
                &left_val,
                op,
                &right_val,
                SqlMode::default(),
            )
        }

        // Conjunction (AND chain)
        Expression::Conjunction(exprs) => {
            for e in exprs {
                let val = evaluate_expr_on_result_row(e, row, group_col_count, aggregates, schema)?;
                if !is_truthy(&val)? {
                    return Ok(SqlValue::Boolean(false));
                }
            }
            Ok(SqlValue::Boolean(true))
        }

        // Disjunction (OR chain)
        Expression::Disjunction(exprs) => {
            for e in exprs {
                let val = evaluate_expr_on_result_row(e, row, group_col_count, aggregates, schema)?;
                if is_truthy(&val)? {
                    return Ok(SqlValue::Boolean(true));
                }
            }
            Ok(SqlValue::Boolean(false))
        }

        // Unary operations
        Expression::UnaryOp { op, expr: inner } => {
            let inner_val =
                evaluate_expr_on_result_row(inner, row, group_col_count, aggregates, schema)?;
            crate::evaluator::eval_unary_op(op, &inner_val)
        }

        // Literal values
        Expression::Literal(lit) => Ok(lit.clone()),

        // Column references in HAVING (must be GROUP BY columns)
        // Note: The result row has group columns first, then aggregate values.
        // The group columns are stored in GROUP BY clause order, which may not match
        // schema order. We fall back to row-based for this case as a temporary fix.
        Expression::ColumnRef(col_id) => {
            // Column references in HAVING with GROUP BY require knowing the mapping
            // from GROUP BY expression order to result row positions. Since the columnar
            // path currently doesn't have this mapping, fall back to row-based execution.
            Err(ExecutorError::Other(format!(
                "Column reference {}.{} in HAVING not supported in columnar path - falling back to row-based",
                col_id.table_canonical().unwrap_or(""),
                col_id.column_canonical()
            )))
        }

        // Subquery variants - not supported in columnar path
        Expression::ScalarSubquery(_)
        | Expression::In { .. }
        | Expression::Exists { .. }
        | Expression::QuantifiedComparison { .. } => Err(ExecutorError::UnsupportedFeature(
            "Subqueries in HAVING not supported in columnar path".to_string(),
        )),

        // IN list
        Expression::InList { expr, values, negated } => {
            let val = evaluate_expr_on_result_row(expr, row, group_col_count, aggregates, schema)?;

            // SQL three-valued logic for IN:
            // - NULL IN (list) -> NULL (unless list is empty)
            // - x IN (NULL, ...) -> TRUE if x matches any non-NULL, else NULL
            if matches!(val, SqlValue::Null) {
                // NULL IN (empty list) -> FALSE per SQLite behavior
                if values.is_empty() {
                    return Ok(SqlValue::Boolean(*negated));
                }
                // NULL IN (non-empty list) -> NULL
                return Ok(SqlValue::Null);
            }

            let mut found = false;
            let mut found_null = false;
            for v in values {
                let list_val =
                    evaluate_expr_on_result_row(v, row, group_col_count, aggregates, schema)?;

                // Track if we encounter NULL in the list
                if matches!(list_val, SqlValue::Null) {
                    found_null = true;
                    continue;
                }

                if crate::evaluator::ExpressionEvaluator::values_are_equal(&val, &list_val) {
                    found = true;
                    break;
                }
            }

            // SQL three-valued logic:
            // - If found a match: return TRUE (or FALSE if negated)
            // - If not found but list contains NULL: return NULL
            // - If not found and no NULL: return FALSE (or TRUE if negated)
            if found {
                Ok(SqlValue::Boolean(!negated))
            } else if found_null {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Boolean(*negated))
            }
        }

        // BETWEEN
        Expression::Between { expr, low, high, negated, .. } => {
            let val = evaluate_expr_on_result_row(expr, row, group_col_count, aggregates, schema)?;
            let low_val =
                evaluate_expr_on_result_row(low, row, group_col_count, aggregates, schema)?;
            let high_val =
                evaluate_expr_on_result_row(high, row, group_col_count, aggregates, schema)?;

            let ge_low = crate::evaluator::ExpressionEvaluator::eval_binary_op_static(
                &val,
                &vibesql_ast::BinaryOperator::GreaterThanOrEqual,
                &low_val,
                SqlMode::default(),
            )?;
            let le_high = crate::evaluator::ExpressionEvaluator::eval_binary_op_static(
                &val,
                &vibesql_ast::BinaryOperator::LessThanOrEqual,
                &high_val,
                SqlMode::default(),
            )?;

            // SQL three-valued logic: if either comparison is NULL, the result is NULL
            // (unless one is definitively false, which would make the whole thing false)
            let in_range = match (&ge_low, &le_high) {
                // Both are true booleans -> true
                (SqlValue::Boolean(true), SqlValue::Boolean(true)) => SqlValue::Boolean(true),
                // One is definitively false -> false (regardless of NULL on other side)
                (SqlValue::Boolean(false), _) | (_, SqlValue::Boolean(false)) => {
                    SqlValue::Boolean(false)
                }
                // Any NULL with non-false -> NULL
                (SqlValue::Null, _) | (_, SqlValue::Null) => SqlValue::Null,
                // Treat other truthy values as true
                _ => {
                    let left_truthy = is_truthy(&ge_low)?;
                    let right_truthy = is_truthy(&le_high)?;
                    SqlValue::Boolean(left_truthy && right_truthy)
                }
            };

            // Apply negation with NULL propagation
            match in_range {
                SqlValue::Null => Ok(SqlValue::Null),
                SqlValue::Boolean(b) => Ok(SqlValue::Boolean(if *negated { !b } else { b })),
                other => {
                    // Should not happen but handle gracefully
                    let b = is_truthy(&other)?;
                    Ok(SqlValue::Boolean(if *negated { !b } else { b }))
                }
            }
        }

        // IS NULL / IS NOT NULL
        Expression::IsNull { expr, negated } => {
            let val = evaluate_expr_on_result_row(expr, row, group_col_count, aggregates, schema)?;
            let is_null = matches!(val, SqlValue::Null);
            Ok(SqlValue::Boolean(if *negated { !is_null } else { is_null }))
        }

        // CASE expression
        Expression::Case { operand, when_clauses, else_result } => {
            match operand {
                Some(operand_expr) => {
                    // Simple CASE: CASE x WHEN v THEN result
                    let operand_val = evaluate_expr_on_result_row(
                        operand_expr,
                        row,
                        group_col_count,
                        aggregates,
                        schema,
                    )?;

                    for when_clause in when_clauses {
                        for condition in &when_clause.conditions {
                            let when_val = evaluate_expr_on_result_row(
                                condition,
                                row,
                                group_col_count,
                                aggregates,
                                schema,
                            )?;
                            // Strict (no cross-type guessing) equality via the
                            // same `eval_binary_op_static` `=` comparator this
                            // file already uses for BinaryOp/IN/BETWEEN, not
                            // the permissive `values_are_equal` used for
                            // hash-join keys. A bare TEXT literal carries no
                            // affinity here, so `HAVING CASE c WHEN '2' THEN
                            // ...` must not match an INTEGER c=2 against TEXT
                            // '2' (e_expr-23.1.6, same fix as the main
                            // evaluators).
                            if matches!(
                                crate::evaluator::ExpressionEvaluator::eval_binary_op_static(
                                    &operand_val,
                                    &vibesql_ast::BinaryOperator::Equal,
                                    &when_val,
                                    SqlMode::default(),
                                )?,
                                SqlValue::Boolean(true)
                            ) {
                                return evaluate_expr_on_result_row(
                                    &when_clause.result,
                                    row,
                                    group_col_count,
                                    aggregates,
                                    schema,
                                );
                            }
                        }
                    }

                    if let Some(else_expr) = else_result {
                        evaluate_expr_on_result_row(
                            else_expr,
                            row,
                            group_col_count,
                            aggregates,
                            schema,
                        )
                    } else {
                        Ok(SqlValue::Null)
                    }
                }
                None => {
                    // Searched CASE: CASE WHEN cond THEN result
                    for when_clause in when_clauses {
                        for condition in &when_clause.conditions {
                            let cond_val = evaluate_expr_on_result_row(
                                condition,
                                row,
                                group_col_count,
                                aggregates,
                                schema,
                            )?;
                            if is_truthy(&cond_val)? {
                                return evaluate_expr_on_result_row(
                                    &when_clause.result,
                                    row,
                                    group_col_count,
                                    aggregates,
                                    schema,
                                );
                            }
                        }
                    }

                    if let Some(else_expr) = else_result {
                        evaluate_expr_on_result_row(
                            else_expr,
                            row,
                            group_col_count,
                            aggregates,
                            schema,
                        )
                    } else {
                        Ok(SqlValue::Null)
                    }
                }
            }
        }

        // Function call (non-aggregate) - not currently supported in columnar HAVING
        // This is rare in practice and would require access to the database for sql_mode
        Expression::Function { name, .. } => Err(ExecutorError::UnsupportedFeature(format!(
            "Scalar function {} in HAVING not supported in columnar path",
            name
        ))),

        // Cast expression
        Expression::Cast { expr, data_type } => {
            let val = evaluate_expr_on_result_row(expr, row, group_col_count, aggregates, schema)?;
            cast_value(&val, data_type, &SqlMode::default())
        }

        // Other expressions not supported
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Expression type {:?} not supported in columnar HAVING",
            std::mem::discriminant(expr)
        ))),
    }
}

/// Check if two aggregate sources match
fn sources_match(spec_source: &AggregateSource, having_source: &AggregateSource) -> bool {
    match (spec_source, having_source) {
        (AggregateSource::Column(a), AggregateSource::Column(b)) => a == b,
        (AggregateSource::CountStar, AggregateSource::CountStar) => true,
        (AggregateSource::Expression(a), AggregateSource::Expression(b)) => {
            // Compare expressions structurally
            expressions_equal(a, b)
        }
        _ => false,
    }
}

/// Check if two expressions are structurally equal
fn expressions_equal(a: &Expression, b: &Expression) -> bool {
    match (a, b) {
        (Expression::ColumnRef(col_id1), Expression::ColumnRef(col_id2))
            if col_id1.schema_canonical().is_none() && col_id2.schema_canonical().is_none() =>
        {
            col_id1.table_canonical() == col_id2.table_canonical()
                && col_id1.column_canonical() == col_id2.column_canonical()
        }
        (
            Expression::BinaryOp { left: l1, op: o1, right: r1 },
            Expression::BinaryOp { left: l2, op: o2, right: r2 },
        ) => o1 == o2 && expressions_equal(l1, l2) && expressions_equal(r1, r2),
        (Expression::Literal(l1), Expression::Literal(l2)) => l1 == l2,
        _ => false,
    }
}

/// Check if a SqlValue is truthy (evaluates to true in boolean context)
///
/// Delegates to the shared SQLite truthiness helper so the columnar HAVING
/// path agrees with the row-based one: strings/blobs coerce via the
/// leading-numeric parse (`'first'` → falsy, `'1'`/`X'31'` → truthy)
/// rather than "any non-NULL value is truthy". (#5803)
fn is_truthy(value: &SqlValue) -> Result<bool, ExecutorError> {
    Ok(crate::evaluator::operators::is_truthy(value))
}
