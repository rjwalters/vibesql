//! Runtime JSON subtype recovery for `subtype(X)`.
//!
//! SQLite tags values produced by JSON functions with an internal *JSON
//! subtype* (the integer 74, ASCII `'J'`). `subtype(X)` returns that tag, and
//! several conformance tests gate on it (json102-1600/1610/1620). VibeSQL does
//! not store the subtype on the [`SqlValue`] itself (that would be a pervasive
//! change to a type matched in hundreds of places), so this module recovers the
//! subtype *structurally* from the argument expression combined with its
//! evaluated value:
//!
//! - `->` (`JsonExtract`): JSON subtype whenever the result is non-NULL.
//! - `->>` (`JsonExtractText`): never JSON subtype.
//! - `json()`, `json_array()`, `json_object()`, `json_quote()`, and the insert/
//!   replace/set/remove/patch mutation functions (plus `jsonb_*` aliases): JSON
//!   subtype whenever the result is non-NULL. `json_quote()` always returns valid
//!   JSON text and SQLite tags it with subtype 74 unconditionally, so — for
//!   `subtype()` purposes — it is an unconditional JSON producer here. (It is
//!   deliberately *not* treated as an embedding producer in `special.rs` /
//!   `arena.rs`, which keeps json_quote's quoted result quoting when passed into
//!   another JSON function.)
//! - `json_extract()`: conditional — JSON subtype only when the result is a JSON
//!   container (array/object), matching SQLite's behaviour of tagging only
//!   container extractions.
//! - `CAST(inner AS <text type>)`: recurses into `inner`, so
//!   `subtype(CAST(json('[1,2]') AS TEXT))` reports the subtype of `json('[1,2]')`
//!   (SQLite preserves the JSON subtype across a text-typed CAST).
//! - a scalar subquery `(SELECT expr ...)` with a single projected expression:
//!   recurses into that projected expression, so `subtype((SELECT json('[1,2]')))`
//!   reports the subtype of `json('[1,2]')`. The recursion is best-effort — if the
//!   projected expression cannot be evaluated against the current row (e.g. it
//!   references a subquery-local column) the recursion yields no subtype rather
//!   than erroring.
//! - `if()` / `iif()` / `coalesce()` / `ifnull()` / `nullif()` / `CASE`: the
//!   subtype of the branch actually selected at runtime (recurses; covers
//!   json102-1620's `subtype(if(json_valid(x), x->y))`).
//! - anything else: the value's own runtime subtype marker (see
//!   [`json_funcs::sql_value_is_json_subtyped`]) — but only when the argument
//!   expression is *eligible* (see [`expr_runtime_json_subtype_eligible`]). This
//!   lets a container `value` column from json_each/json_tree keep its subtype
//!   through an opaque column reference (json101-5.10) while an ordinary
//!   `CHAR(n)` column read never picks it up (issue #6007).
//!
//! The three evaluator front-ends (`ExpressionEvaluator`,
//! `CombinedExpressionEvaluator`, and the arena evaluator) all delegate here via
//! a closure that performs their own value evaluation, so the rules live in one
//! place.

use vibesql_ast::{BinaryOperator, CaseWhen, Expression, SelectItem, SelectStmt};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;
use crate::evaluator::functions::sqlite_compat::json_funcs;
use crate::evaluator::operators::is_truthy;

/// SQLite's JSON subtype tag value (ASCII `'J'`).
pub(crate) const JSON_SUBTYPE_TAG: i64 = 74;

/// Function names whose result is *always* JSON-subtyped when non-NULL (for
/// `subtype()` recovery purposes).
///
/// `json_quote` is included: it always emits valid JSON text and SQLite tags it
/// with subtype 74 unconditionally. Note this is broader than the *embedding*
/// producer set in `special.rs` / `arena.rs` — json_quote is intentionally not an
/// embedding producer there, so its quoted output keeps quoting when passed into
/// another JSON function.
fn is_unconditional_json_producer(name: &str) -> bool {
    matches!(
        name,
        "json"
            | "json_array"
            | "json_object"
            | "json_quote"
            | "json_insert"
            | "json_replace"
            | "json_set"
            | "json_remove"
            | "json_patch"
            | "jsonb"
            | "jsonb_array"
            | "jsonb_object"
            | "jsonb_insert"
            | "jsonb_replace"
            | "jsonb_set"
            | "jsonb_remove"
            | "jsonb_patch"
    )
}

/// Does this declared type qualify as a *text* target for the `CAST(inner AS
/// <text type>)` subtype pass-through? SQLite preserves the JSON subtype across a
/// text-typed CAST (`CAST(json('[1,2]') AS TEXT)` keeps subtype 74).
fn data_type_is_text_cast_target(ty: &vibesql_types::DataType) -> bool {
    data_type_is_string(ty)
}

/// Does this SQL text value hold a JSON *container* (array or object)? SQLite's
/// conditional-subtype producers (`json_extract`, `->`) carry the JSON subtype
/// only for container results, never for extracted scalars.
pub(crate) fn value_is_json_container(value: &SqlValue) -> bool {
    match value {
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            matches!(
                json_funcs::parse_json_relaxed(s.as_str()),
                Ok(serde_json::Value::Array(_)) | Ok(serde_json::Value::Object(_))
            )
        }
        _ => false,
    }
}

/// Is the argument expression eligible to carry the *runtime* JSON subtype
/// marker (see [`json_funcs::sql_value_is_json_subtyped`])?
///
/// The runtime marker rides on [`SqlValue::Character`], which is also how an
/// ordinary fixed-width `CHAR(n)` column materialises. To keep the marker from
/// mis-firing on a CHAR column holding container-shaped text (issue #6007), a
/// bare read of a column declared with a real string type (CHAR/VARCHAR) is
/// *not* eligible: its value quotes even when it parses as a JSON container,
/// matching SQLite. Everything else — literals, function results, arithmetic,
/// and dynamically-typed columns such as a json_each/json_tree `value` column
/// (declared `DataType::Null`) — stays eligible, so json101-5.10 still embeds
/// the container.
///
/// `column_is_declared_string` resolves a column reference (by its optional
/// table qualifier + canonical name) to `true` when the column's *declared*
/// type is a real string type. It returns `false` for dynamically-typed
/// (TVF/derived) columns and for unresolved references, both of which remain
/// eligible.
pub(crate) fn expr_runtime_json_subtype_eligible<R>(
    expr: &Expression,
    column_is_declared_string: &R,
) -> bool
where
    R: Fn(Option<&str>, &str) -> bool,
{
    match expr {
        Expression::ColumnRef(col) => {
            !column_is_declared_string(col.table_canonical(), col.column_canonical())
        }
        _ => true,
    }
}

/// Does this declared column type quote container-shaped text (i.e. is it a real
/// string type: CHAR/VARCHAR/CLOB)? A `CHAR(n)` column read must never pick up
/// the runtime JSON subtype marker (issue #6007).
pub(crate) fn data_type_is_string(ty: &vibesql_types::DataType) -> bool {
    use vibesql_types::DataType;
    matches!(
        ty,
        DataType::Character { .. }
            | DataType::Varchar { .. }
            | DataType::CharacterLargeObject
            | DataType::Name
    )
}

/// Evaluate `subtype(X)`: `Integer(74)` when the argument carries the JSON
/// subtype at runtime, `Integer(0)` otherwise. `eval` evaluates a sub-expression
/// against the current row using the caller's evaluator.
/// `column_is_declared_string` resolves whether a column reference is declared
/// with a real string type (used to gate the runtime marker; see
/// [`expr_runtime_json_subtype_eligible`] and issue #6007).
pub(crate) fn eval_subtype<F, R>(
    args: &[Expression],
    eval: &F,
    column_is_declared_string: &R,
) -> Result<SqlValue, ExecutorError>
where
    F: Fn(&Expression) -> Result<SqlValue, ExecutorError>,
    R: Fn(Option<&str>, &str) -> bool,
{
    if args.len() != 1 {
        return Err(ExecutorError::WrongNumberOfArguments { function_name: "subtype".to_string() });
    }
    let is_json = expr_json_subtype(&args[0], eval, column_is_declared_string)?;
    Ok(SqlValue::Integer(if is_json { JSON_SUBTYPE_TAG } else { 0 }))
}

/// The evaluator closure the structural walk uses, as a trait object.
///
/// Using `&dyn Fn` (rather than a generic `F`) is deliberate: the scalar-subquery
/// arm wraps `eval` in a fresh error-swallowing closure and recurses, which — with
/// a generic parameter — would instantiate a new type on every recursion and blow
/// the monomorphization recursion limit. A single trait-object instantiation
/// recurses freely.
type EvalFn<'a> = dyn Fn(&Expression) -> Result<SqlValue, ExecutorError> + 'a;
/// The column-type resolver, as a trait object (see [`EvalFn`]).
type ColumnIsStringFn<'a> = dyn Fn(Option<&str>, &str) -> bool + 'a;

/// Structurally determine whether `expr` evaluates to a JSON-subtyped value.
///
/// Generic entry point: erases `eval` / `column_is_declared_string` to trait
/// objects and delegates to [`expr_json_subtype_dyn`].
fn expr_json_subtype<F, R>(
    expr: &Expression,
    eval: &F,
    column_is_declared_string: &R,
) -> Result<bool, ExecutorError>
where
    F: Fn(&Expression) -> Result<SqlValue, ExecutorError>,
    R: Fn(Option<&str>, &str) -> bool,
{
    expr_json_subtype_dyn(expr, eval, column_is_declared_string)
}

/// Trait-object body of [`expr_json_subtype`] (single instantiation; see
/// [`EvalFn`]).
fn expr_json_subtype_dyn(
    expr: &Expression,
    eval: &EvalFn<'_>,
    column_is_declared_string: &ColumnIsStringFn<'_>,
) -> Result<bool, ExecutorError> {
    Ok(match expr {
        // `->` always yields the JSON subtype for a non-NULL result; `->>`
        // never does.
        Expression::BinaryOp { op: BinaryOperator::JsonExtract, .. } => {
            !matches!(eval(expr)?, SqlValue::Null)
        }
        Expression::BinaryOp { op: BinaryOperator::JsonExtractText, .. } => false,
        Expression::Function { name, args, .. } => {
            let canon = name.canonical();
            if is_unconditional_json_producer(canon) {
                !matches!(eval(expr)?, SqlValue::Null)
            } else if matches!(canon, "json_extract" | "jsonb_extract") {
                value_is_json_container(&eval(expr)?)
            } else if matches!(canon, "if" | "iif") {
                match selected_conditional_branch(args, eval)? {
                    Some(branch) => expr_json_subtype_dyn(branch, eval, column_is_declared_string)?,
                    None => false,
                }
            } else if matches!(canon, "coalesce" | "ifnull") {
                let mut result = false;
                for arg in args {
                    if !matches!(eval(arg)?, SqlValue::Null) {
                        result = expr_json_subtype_dyn(arg, eval, column_is_declared_string)?;
                        break;
                    }
                }
                result
            } else if canon == "nullif" && args.len() == 2 {
                expr_json_subtype_dyn(&args[0], eval, column_is_declared_string)?
            } else {
                false
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            match selected_case_branch(
                operand.as_deref(),
                when_clauses,
                else_result.as_deref(),
                eval,
            )? {
                Some(branch) => expr_json_subtype_dyn(branch, eval, column_is_declared_string)?,
                None => false,
            }
        }
        // `CAST(inner AS <text type>)` preserves the JSON subtype of `inner`
        // (e.g. `CAST(json('[1,2]') AS TEXT)` is still subtype 74). Recurse into
        // `inner`; a non-text cast (INTEGER/REAL/...) drops the subtype.
        Expression::Cast { expr: inner, data_type } if data_type_is_text_cast_target(data_type) => {
            expr_json_subtype_dyn(inner, eval, column_is_declared_string)?
        }
        // Scalar subquery with a single projected expression: recurse into that
        // projected expression, so `subtype((SELECT json('[1,2]')))` reports the
        // subtype of `json('[1,2]')`. Best-effort: if the projected expression
        // cannot be evaluated against the current row (e.g. it references a
        // subquery-local column) the recursion yields no subtype rather than
        // erroring.
        Expression::ScalarSubquery(select) => match single_projected_expression(select) {
            Some(projected) => {
                let guarded = |e: &Expression| match eval(e) {
                    Ok(v) => Ok(v),
                    Err(_) => Ok(SqlValue::Null),
                };
                expr_json_subtype_dyn(projected, &guarded, column_is_declared_string)?
            }
            None => false,
        },
        // A runtime value already carrying the subtype marker (a container
        // `value` column from json_each/json_tree reached via a column ref) —
        // but only when the argument expression is eligible, so a plain
        // `CHAR(n)` column read never reports the JSON subtype (issue #6007).
        _ => {
            // `column_is_declared_string` is a `&dyn Fn`; a reference to it still
            // implements `Fn`, so pass `&column_is_declared_string` to the
            // generic helper.
            expr_runtime_json_subtype_eligible(expr, &column_is_declared_string)
                && json_funcs::sql_value_is_json_subtyped(&eval(expr)?)
        }
    })
}

/// For an `if`/`iif` call, return the branch expression selected for the current
/// row (the value paired with the first truthy condition, or the trailing ELSE).
/// Supports the 2-arg `if(X, Y)` form (implicit `ELSE NULL`) and odd-arity
/// CASE-chains.
fn selected_conditional_branch<'a>(
    args: &'a [Expression],
    eval: &EvalFn<'_>,
) -> Result<Option<&'a Expression>, ExecutorError> {
    let mut i = 0;
    while i + 1 < args.len() {
        if is_truthy(&eval(&args[i])?) {
            return Ok(Some(&args[i + 1]));
        }
        i += 2;
    }
    if !args.len().is_multiple_of(2) {
        Ok(Some(&args[args.len() - 1]))
    } else {
        Ok(None)
    }
}

/// For a CASE expression, return the branch (THEN or ELSE) selected for the
/// current row, or `None` when no branch matches and there is no ELSE.
fn selected_case_branch<'a>(
    operand: Option<&'a Expression>,
    when_clauses: &'a [CaseWhen],
    else_result: Option<&'a Expression>,
    eval: &EvalFn<'_>,
) -> Result<Option<&'a Expression>, ExecutorError> {
    let operand_val = match operand {
        Some(op) => Some(eval(op)?),
        None => None,
    };
    for wc in when_clauses {
        for cond in &wc.conditions {
            let matched = match &operand_val {
                Some(ov) => {
                    let cv = eval(cond)?;
                    crate::evaluator::core::values_are_equal(ov, &cv)
                }
                None => is_truthy(&eval(cond)?),
            };
            if matched {
                return Ok(Some(&wc.result));
            }
        }
    }
    Ok(else_result)
}

/// Return the single projected expression of a scalar subquery, or `None` when
/// the projection is not exactly one plain expression (a wildcard, a set
/// operation, or a multi-column projection all disqualify — none can produce a
/// scalar JSON value whose subtype we can recover structurally).
fn single_projected_expression(select: &SelectStmt) -> Option<&Expression> {
    if select.set_operation.is_some() {
        return None;
    }
    match select.select_list.as_slice() {
        [SelectItem::Expression { expr, .. }] => Some(expr),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{BinaryOperator, Expression, FunctionIdentifier};

    use super::*;

    fn lit(v: SqlValue) -> Expression {
        Expression::Literal(v)
    }

    fn func(name: &str, args: Vec<Expression>) -> Expression {
        Expression::Function { name: FunctionIdentifier::new(name), args, character_unit: None }
    }

    fn arrow(op: BinaryOperator, left: Expression, right: Expression) -> Expression {
        Expression::BinaryOp { op, left: Box::new(left), right: Box::new(right) }
    }

    /// `CAST(inner AS <data_type>)`.
    fn cast(inner: Expression, data_type: vibesql_types::DataType) -> Expression {
        Expression::Cast { expr: Box::new(inner), data_type }
    }

    /// A scalar subquery whose sole projected item is `projected`
    /// (`(SELECT projected)`), with no FROM/WHERE/etc.
    fn scalar_subquery(projected: Expression) -> Expression {
        let select = SelectStmt {
            with_clause: None,
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: projected,
                alias: None,
                source_text: None,
            }],
            into_table: None,
            into_variables: None,
            from: None,
            where_clause: None,
            group_by: None,
            having: None,
            window_definitions: None,
            order_by: None,
            limit: None,
            offset: None,
            set_operation: None,
            values: None,
        };
        Expression::ScalarSubquery(Box::new(select))
    }

    /// A closure that evaluates literals and the JSON `->`/`->>` operators over
    /// a fixed document, standing in for a real evaluator. This lets us exercise
    /// the structural subtype rules without a full executor.
    fn eval_expr(expr: &Expression) -> Result<SqlValue, ExecutorError> {
        match expr {
            Expression::Literal(v) => Ok(v.clone()),
            Expression::BinaryOp { op, left, right } => {
                let l = eval_expr(left)?;
                let r = eval_expr(right)?;
                json_funcs::eval_json_arrow(&l, &r, matches!(op, BinaryOperator::JsonExtractText))
            }
            Expression::Function { name, args, .. } => {
                let vals: Vec<SqlValue> = args.iter().map(eval_expr).collect::<Result<_, _>>()?;
                match name.canonical() {
                    "json" => json_funcs::json(&vals),
                    "json_extract" => json_funcs::json_extract(&vals),
                    "json_valid" => json_funcs::json_valid(&vals),
                    "json_quote" => json_funcs::json_quote(&vals),
                    "if" | "iif" => {
                        // Minimal 2/3-arg conditional for the tests.
                        if crate::evaluator::operators::is_truthy(&vals[0]) {
                            Ok(vals[1].clone())
                        } else if vals.len() >= 3 {
                            Ok(vals[2].clone())
                        } else {
                            Ok(SqlValue::Null)
                        }
                    }
                    other => panic!("eval_expr: unhandled function {other}"),
                }
            }
            // Minimal CAST for the tests: a text-typed cast returns the inner
            // value's text unchanged (identity for JSON string values); the
            // structural subtype pass-through is what we actually exercise.
            Expression::Cast { expr: inner, .. } => eval_expr(inner),
            // Minimal scalar-subquery eval for the tests: evaluate the single
            // projected expression against the (empty) outer row.
            Expression::ScalarSubquery(select) => match single_projected_expression(select) {
                Some(projected) => eval_expr(projected),
                None => Ok(SqlValue::Null),
            },
            other => panic!("eval_expr: unhandled expr {other:?}"),
        }
    }

    /// Test stub: no column is treated as declared-string, so every argument
    /// stays eligible for the runtime marker. The CHAR-column disqualification
    /// is exercised end-to-end by the executor integration tests instead
    /// (`json_char_column_subtype_tests.rs`).
    fn no_string_columns(_table: Option<&str>, _column: &str) -> bool {
        false
    }

    fn is_json(expr: Expression) -> bool {
        matches!(
            eval_subtype(&[expr], &eval_expr, &no_string_columns).unwrap(),
            SqlValue::Integer(74)
        )
    }

    #[test]
    fn arrow_operator_is_json_when_non_null() {
        // '[7,8]' -> 0  => JSON subtype (container)
        assert!(is_json(arrow(
            BinaryOperator::JsonExtract,
            lit(SqlValue::Varchar("[7,8]".into())),
            lit(SqlValue::Integer(0)),
        )));
        // '[7,8]' -> 5  => NULL result => not JSON
        assert!(!is_json(arrow(
            BinaryOperator::JsonExtract,
            lit(SqlValue::Varchar("[7,8]".into())),
            lit(SqlValue::Integer(5)),
        )));
    }

    #[test]
    fn arrow_text_operator_never_json() {
        assert!(!is_json(arrow(
            BinaryOperator::JsonExtractText,
            lit(SqlValue::Varchar(r#"{"a":[1,2]}"#.into())),
            lit(SqlValue::Varchar("$.a".into())),
        )));
    }

    #[test]
    fn json_producer_is_json() {
        assert!(is_json(func("json", vec![lit(SqlValue::Varchar("[1,2]".into()))])));
    }

    #[test]
    fn json_extract_is_json_only_for_containers() {
        // json_extract('{"a":[1,2]}','$.a') -> [1,2] (container) => JSON
        assert!(is_json(func(
            "json_extract",
            vec![
                lit(SqlValue::Varchar(r#"{"a":[1,2]}"#.into())),
                lit(SqlValue::Varchar("$.a".into())),
            ],
        )));
        // json_extract('{"a":123}','$.a') -> 123 (scalar) => not JSON
        assert!(!is_json(func(
            "json_extract",
            vec![
                lit(SqlValue::Varchar(r#"{"a":123}"#.into())),
                lit(SqlValue::Varchar("$.a".into())),
            ],
        )));
    }

    #[test]
    fn plain_text_and_number_not_json() {
        assert!(!is_json(lit(SqlValue::Varchar("[1,2]".into()))));
        assert!(!is_json(lit(SqlValue::Integer(5))));
    }

    // --- issue #6021 divergence table: each of these was `0`, SQLite reports 74 ---

    #[test]
    fn json_quote_is_unconditionally_json() {
        // subtype(json_quote('[1,2]')) => 74 (SQLite tags json_quote output
        // unconditionally). Previously container-only -> false negative.
        assert!(is_json(func("json_quote", vec![lit(SqlValue::Varchar("[1,2]".into()))])));
        // A scalar argument is still JSON-subtyped (json_quote always emits valid
        // JSON text): subtype(json_quote('a')) => 74.
        assert!(is_json(func("json_quote", vec![lit(SqlValue::Varchar("a".into()))])));
        assert!(is_json(func("json_quote", vec![lit(SqlValue::Integer(5))])));
    }

    #[test]
    fn cast_to_text_preserves_json_subtype() {
        // subtype(CAST(json('[1,2]') AS TEXT)) => 74.
        assert!(is_json(cast(
            func("json", vec![lit(SqlValue::Varchar("[1,2]".into()))]),
            vibesql_types::DataType::Varchar { max_length: None },
        )));
        // CAST(inner AS CHAR(n)) / CLOB / NAME all pass through too.
        assert!(is_json(cast(
            func("json", vec![lit(SqlValue::Varchar("[1,2]".into()))]),
            vibesql_types::DataType::Character { length: 16 },
        )));
    }

    #[test]
    fn cast_to_non_text_drops_json_subtype() {
        // A non-text CAST target is not a JSON producer: subtype drops to 0.
        assert!(!is_json(cast(
            func("json", vec![lit(SqlValue::Varchar("[1,2]".into()))]),
            vibesql_types::DataType::Integer,
        )));
    }

    #[test]
    fn scalar_subquery_projected_expr_preserves_json_subtype() {
        // subtype((SELECT json('[1,2]'))) => 74.
        assert!(is_json(scalar_subquery(func(
            "json",
            vec![lit(SqlValue::Varchar("[1,2]".into()))],
        ))));
        // subtype((SELECT json_quote('[1,2]'))) => 74 (recursion reaches the new
        // unconditional json_quote rule through the subquery boundary).
        assert!(is_json(scalar_subquery(func(
            "json_quote",
            vec![lit(SqlValue::Varchar("[1,2]".into()))],
        ))));
        // A plain-text projection is not JSON-subtyped.
        assert!(!is_json(scalar_subquery(lit(SqlValue::Varchar("[1,2]".into())))));
    }

    #[test]
    fn nested_boundary_producers_compose() {
        // subtype(CAST((SELECT json('[1,2]')) AS TEXT)) => 74: CAST and
        // subquery recursion compose.
        assert!(is_json(cast(
            scalar_subquery(func("json", vec![lit(SqlValue::Varchar("[1,2]".into()))])),
            vibesql_types::DataType::Varchar { max_length: None },
        )));
    }

    #[test]
    fn if_recurses_into_selected_branch() {
        // json102-1620 shape: subtype(if(json_valid(x), x->y)).
        // Truthy condition -> recurse into the `->` branch (container -> JSON).
        assert!(is_json(func(
            "if",
            vec![
                func("json_valid", vec![lit(SqlValue::Varchar("[1,2]".into()))]),
                arrow(
                    BinaryOperator::JsonExtract,
                    lit(SqlValue::Varchar("[1,2]".into())),
                    lit(SqlValue::Integer(0)),
                ),
            ],
        )));
        // Falsy condition, 2-arg form -> implicit ELSE NULL -> not JSON.
        assert!(!is_json(func(
            "if",
            vec![
                lit(SqlValue::Integer(0)),
                func("json", vec![lit(SqlValue::Varchar("[1,2]".into()))]),
            ],
        )));
    }

    #[test]
    fn character_marker_is_json() {
        // A container `value` column reaching subtype() as a bare value.
        assert!(is_json(lit(SqlValue::Character("[1,2,3]".into()))));
        // A scalar-string Character (json101-5.11 atom) is not.
        assert!(!is_json(lit(SqlValue::Character("hello".into()))));
    }

    #[test]
    fn subtype_wrong_arity_errors() {
        assert!(eval_subtype(&[], &eval_expr, &no_string_columns).is_err());
        assert!(eval_subtype(
            &[lit(SqlValue::Integer(1)), lit(SqlValue::Integer(2))],
            &eval_expr,
            &no_string_columns
        )
        .is_err());
    }
}
