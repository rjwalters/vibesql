//! Helper functions for SELECT query execution

use indexmap::IndexSet;

/// Estimate the memory size of a result set in bytes
///
/// Used for memory limit tracking during query execution.
/// Samples a subset of rows to avoid O(n) overhead on large result sets.
pub(super) fn estimate_result_size(rows: &[vibesql_storage::Row]) -> usize {
    if rows.is_empty() {
        return 0;
    }

    // For small result sets, measure exactly
    if rows.len() <= 100 {
        return rows.iter().map(|r| r.estimated_size_bytes()).sum();
    }

    // For large result sets, sample and extrapolate
    // Sample first 10, middle 10, and last 10 rows
    let sample_size = 30;
    let step = rows.len() / sample_size;
    let sample_total: usize =
        rows.iter().step_by(step.max(1)).take(sample_size).map(|r| r.estimated_size_bytes()).sum();

    let avg_row_size = sample_total / sample_size.min(rows.len());
    avg_row_size * rows.len()
}

/// Apply DISTINCT to remove duplicate rows
///
/// Uses an IndexSet to track unique rows while preserving insertion order.
/// This ensures deterministic results that match SQLite's behavior.
/// This requires SqlValue to implement Hash and Eq, which we've implemented
/// with SQL semantics:
/// - NULL == NULL for grouping
/// - NaN == NaN for grouping
///
/// # Optimization
///
/// Checks containment BEFORE cloning to avoid unnecessary allocations.
/// Only clones row values when the row is actually unique and needs to be stored.
/// This reduces cloning from O(n) to O(unique rows), which is significant
/// when there are many duplicate values (common in DISTINCT queries).
pub(super) fn apply_distinct(rows: Vec<vibesql_storage::Row>) -> Vec<vibesql_storage::Row> {
    if rows.is_empty() {
        return Vec::new();
    }

    // Track seen values - IndexSet maintains insertion order for deterministic results
    // Pre-allocate assuming ~50% unique rows as a reasonable default for DISTINCT queries
    let mut seen: IndexSet<vibesql_storage::RowValues> = IndexSet::with_capacity(rows.len() / 2);
    let mut result = Vec::with_capacity(rows.len() / 2);

    for row in rows {
        // Check containment first (no clone needed for lookup)
        // Only clone if the row is actually new and needs to be stored
        if !seen.contains(&row.values) {
            seen.insert(row.values.clone());
            result.push(row);
        }
        // Duplicates are skipped without any cloning
    }

    result
}

/// Evaluate a LIMIT expression to a usize value
///
/// SQLite compatibility: any negative LIMIT means unlimited (returns usize::MAX)
/// Callers should treat usize::MAX as "no limit".
pub(crate) fn evaluate_limit_offset_expr(
    expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
    clause_name: &str,
) -> Result<usize, crate::ExecutorError> {
    let raw = evaluate_limit_offset_expr_raw(expr, database)?;
    if clause_name == "OFFSET" {
        // Negative offset is treated as 0
        Ok(if raw < 0 { 0 } else { raw as usize })
    } else {
        // Negative limit means unlimited
        Ok(if raw < 0 { usize::MAX } else { raw as usize })
    }
}

/// Evaluate a LIMIT or OFFSET expression to an i64 value
///
/// Evaluates arbitrary expressions (e.g., `5+3`, `(SELECT 10)`) at runtime
/// and returns the integer result. Negative values are allowed for SQLite
/// compatibility - the caller will interpret them appropriately.
fn evaluate_limit_offset_expr_raw(
    expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
) -> Result<i64, crate::ExecutorError> {
    use crate::evaluator::ExpressionEvaluator;

    // Pre-validate column references in LIMIT/OFFSET expressions (#5092).
    // LIMIT/OFFSET expressions are evaluated against an empty row/schema, so
    // any column reference is unresolvable. SQLite reports this as
    // "no such column: X". This pre-check must run BEFORE the evaluator,
    // otherwise the evaluator's WindowFunction / AggregateFunction arms can
    // fire first and produce a misleading "misuse of window function ..."
    // error for queries like:
    //   SELECT count(*) FROM t1 LIMIT nth_value(x, 1) OVER ();
    // The unresolved column `x` should be reported before any window-misuse
    // diagnosis runs.
    let mut col_refs = Vec::new();
    crate::select::executor::validation::extract_column_refs(expr, &mut col_refs);
    if let Some(first) = col_refs.into_iter().next() {
        let column_ref = match first.table {
            Some(t) => format!("{}.{}", t, first.column),
            None => first.column,
        };
        return Err(crate::ExecutorError::NoSuchColumn { column_ref });
    }

    // Create empty schema and row for expression evaluation
    let empty_schema = vibesql_catalog::TableSchema::new("".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::with_database(&empty_schema, database);
    let empty_row = vibesql_storage::Row::new(vec![]);

    // Evaluate the expression
    let value = evaluator.eval(expr, &empty_row)?;

    // Convert to integer using SQLite's LIMIT/OFFSET affinity rules
    coerce_limit_offset_to_i64(value)
}

/// Coerce an evaluated LIMIT/OFFSET value to i64 using SQLite semantics.
///
/// Shared by both LIMIT/OFFSET evaluation sites (this module and
/// `select::executor::utils::eval_limit_offset_expr`). SQLite applies
/// numeric affinity to LIMIT/OFFSET expressions:
///
/// - INTEGER values are used as-is
/// - BOOLEAN → 0/1 (EXISTS/IN results behave as integers; #5803)
/// - REAL values are accepted iff exactly integral: `LIMIT 2.0` → 2, `LIMIT 56.1` errors
/// - TEXT is accepted iff the *whole* string (after trimming whitespace) is a numeric literal
///   representing an integral value: `'2'`, `' 2 '`, `'+2'`, `'2.0'` are accepted; `'The'`,
///   `'2abc'`, `'2.5'` error. Note this is deliberately NOT the leading-prefix parse used for
///   truthiness — SQLite raises "datatype mismatch" for partial parses here
/// - NULL, BLOB and everything else error with SQLite's exact wording `datatype mismatch` (#5804)
pub(in crate::select) fn coerce_limit_offset_to_i64(
    value: vibesql_types::SqlValue,
) -> Result<i64, crate::ExecutorError> {
    use vibesql_types::SqlValue;

    fn integral_f64(f: f64) -> Option<i64> {
        if f.is_finite() && f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
            Some(f as i64)
        } else {
            None
        }
    }

    // SQLite raises exactly `datatype mismatch` (SQLITE_MISMATCH) for a
    // non-integral LIMIT/OFFSET value. SqliteCompatError renders verbatim,
    // with no prefix (#5804).
    let err = || crate::ExecutorError::SqliteCompatError("datatype mismatch".to_string());

    match &value {
        SqlValue::Integer(n) => Ok(*n),
        SqlValue::Smallint(n) => Ok(*n as i64),
        SqlValue::Bigint(n) => Ok(*n),
        SqlValue::Unsigned(n) => i64::try_from(*n).map_err(|_| err()),
        // SQLite has no boolean type: EXISTS/IN results behave as integers 0/1
        // e.g. SELECT 1 LIMIT EXISTS(SELECT 1) returns one row
        SqlValue::Boolean(b) => Ok(i64::from(*b)),
        SqlValue::Float(f) => integral_f64(*f as f64).ok_or_else(err),
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => {
            integral_f64(*f).ok_or_else(err)
        }
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            let trimmed = s.trim();
            if let Ok(n) = trimmed.parse::<i64>() {
                Ok(n)
            } else if let Ok(f) = trimmed.parse::<f64>() {
                integral_f64(f).ok_or_else(err)
            } else {
                Err(err())
            }
        }
        SqlValue::Null => Err(err()),
        _ => Err(err()),
    }
}

/// Evaluate optional LIMIT expression to Option<usize>
/// SQLite compatibility: any negative LIMIT means unlimited (returns None)
pub(super) fn evaluate_limit(
    limit: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
) -> Result<Option<usize>, crate::ExecutorError> {
    match limit.as_ref().map(|e| evaluate_limit_offset_expr_raw(e, database)).transpose()? {
        Some(n) if n < 0 => Ok(None), // Any negative value means unlimited
        Some(n) => Ok(Some(n as usize)),
        None => Ok(None),
    }
}

/// Evaluate optional OFFSET expression to Option<usize>
/// SQLite compatibility: any negative OFFSET is treated as 0 (no offset)
pub(super) fn evaluate_offset(
    offset: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
) -> Result<Option<usize>, crate::ExecutorError> {
    match offset.as_ref().map(|e| evaluate_limit_offset_expr_raw(e, database)).transpose()? {
        Some(n) if n < 0 => Ok(Some(0)), // Negative offset treated as 0
        Some(n) => Ok(Some(n as usize)),
        None => Ok(None),
    }
}

/// Apply LIMIT and OFFSET to a result set
pub(super) fn apply_limit_offset(
    rows: Vec<vibesql_storage::Row>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<vibesql_storage::Row> {
    let start = offset.unwrap_or(0);
    if start >= rows.len() {
        return Vec::new();
    }

    let max_take = rows.len() - start;
    let take = limit.unwrap_or(max_take).min(max_take);

    rows.into_iter().skip(start).take(take).collect()
}

/// Apply LIMIT and OFFSET to an iterator without forcing full materialization
///
/// This is more efficient than `apply_limit_offset` when the input is a lazy iterator,
/// as it only materializes the rows that will actually be returned.
///
/// # Performance (Issue #4060)
///
/// For `SELECT * FROM t LIMIT 10` on a 10,000 row table:
/// - `apply_limit_offset(iter.collect(), Some(10), None)`: clones 10,000 rows, keeps 10
/// - `apply_limit_offset_iter(iter, Some(10), None)`: clones only 10 rows
///
/// # Arguments
///
/// * `iter` - Iterator over rows (can be lazy or materialized)
/// * `limit` - Maximum number of rows to return (None = unlimited)
/// * `offset` - Number of rows to skip from the start (None = 0)
#[allow(dead_code)]
pub(super) fn apply_limit_offset_iter<I>(
    iter: I,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<vibesql_storage::Row>
where
    I: Iterator<Item = vibesql_storage::Row>,
{
    let start = offset.unwrap_or(0);

    // Skip offset rows
    let iter = iter.skip(start);

    // Apply limit if specified
    match limit {
        Some(n) => iter.take(n).collect(),
        None => iter.collect(),
    }
}

#[cfg(test)]
mod tests {
    use vibesql_types::SqlValue;

    use super::coerce_limit_offset_to_i64;

    fn coerce(value: SqlValue) -> Result<i64, crate::ExecutorError> {
        coerce_limit_offset_to_i64(value)
    }

    fn text(s: &str) -> SqlValue {
        SqlValue::Varchar(arcstr::ArcStr::from(s))
    }

    // Issue #5803 item 4: SQLite LIMIT/OFFSET numeric-affinity coercion.

    #[test]
    fn integers_and_booleans_accepted() {
        assert_eq!(coerce(SqlValue::Integer(2)).unwrap(), 2);
        assert_eq!(coerce(SqlValue::Integer(-1)).unwrap(), -1);
        assert_eq!(coerce(SqlValue::Boolean(true)).unwrap(), 1);
        assert_eq!(coerce(SqlValue::Boolean(false)).unwrap(), 0);
        assert_eq!(coerce(SqlValue::Smallint(3)).unwrap(), 3);
        assert_eq!(coerce(SqlValue::Bigint(4)).unwrap(), 4);
    }

    #[test]
    fn integral_reals_accepted_fractional_rejected() {
        assert_eq!(coerce(SqlValue::Double(2.0)).unwrap(), 2);
        assert_eq!(coerce(SqlValue::Real(0.0)).unwrap(), 0);
        assert!(coerce(SqlValue::Double(56.1)).is_err());
        assert!(coerce(SqlValue::Real(2.5)).is_err());
        assert!(coerce(SqlValue::Double(f64::NAN)).is_err());
        assert!(coerce(SqlValue::Double(f64::INFINITY)).is_err());
    }

    #[test]
    fn numeric_strings_accepted_full_string_parse() {
        assert_eq!(coerce(text("2")).unwrap(), 2);
        assert_eq!(coerce(text("2.0")).unwrap(), 2);
        assert_eq!(coerce(text(" 2 ")).unwrap(), 2);
        assert_eq!(coerce(text("+2")).unwrap(), 2);
        assert_eq!(coerce(text("-3")).unwrap(), -3);
    }

    #[test]
    fn non_numeric_and_partial_strings_rejected() {
        // Full-string affinity parse, NOT the leading-prefix truthiness parse:
        // SQLite raises "datatype mismatch" for these.
        assert!(coerce(text("The")).is_err());
        assert!(coerce(text("2abc")).is_err());
        assert!(coerce(text("2.5")).is_err());
        assert!(coerce(text("")).is_err());
        assert!(coerce(text("inf")).is_err());
        assert!(coerce(text("nan")).is_err());
    }

    #[test]
    fn null_and_blob_rejected() {
        assert!(coerce(SqlValue::Null).is_err());
        // LIMIT X'32' keeps erroring (SQLite: datatype mismatch)
        assert!(coerce(SqlValue::Blob(b"2".to_vec())).is_err());
    }

    #[test]
    fn rejection_uses_sqlite_exact_wording() {
        // #5804: SQLite raises exactly `datatype mismatch` (no prefix) for a
        // non-integral LIMIT/OFFSET value.
        for v in [SqlValue::Double(56.1), text("The"), SqlValue::Null] {
            assert_eq!(coerce(v).unwrap_err().to_string(), "datatype mismatch");
        }
    }
}
