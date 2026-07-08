//! Auto-vectorized filtering for columnar batches
//!
//! Uses the centralized simd_ops module for consistent, optimized operations.
//!
//! ## Module Structure
//!
//! - `comparison` - Numeric comparison operations (i64, i32, f64)
//! - `string` - String-specific filter operations
//! - `mask` - Filter mask application to batches
//! - `conversion` - Value type conversion utilities
//!
//! ## Parallel Execution
//!
//! For large batches (>1000 rows on 8+ cores), filter mask creation is parallelized
//! using rayon. Each thread processes a range of rows, and masks are combined.

mod comparison;
mod conversion;
mod mask;
mod string;

use comparison::{
    evaluate_predicate_f64_packed, evaluate_predicate_f64_simd, evaluate_predicate_i32_packed,
    evaluate_predicate_i32_simd, evaluate_predicate_i64_packed, evaluate_predicate_i64_simd,
};
use mask::apply_filter_mask;
#[cfg(feature = "parallel")]
use rayon::prelude::*;
use string::evaluate_predicate_string_batch;
use vibesql_types::SqlValue;

use super::{
    batch::{ColumnArray, ColumnarBatch},
    filter::{evaluate_column_compare, ColumnPredicate, CompareOp},
    simd_ops::{self, PackedMask},
};
use crate::errors::ExecutorError;
#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;

/// Check if any value in a predicate is NULL
/// Per SQL standard, any comparison with NULL returns UNKNOWN (treated as false in WHERE)
fn predicate_contains_null(predicate: &ColumnPredicate) -> bool {
    match predicate {
        ColumnPredicate::LessThan { value, .. }
        | ColumnPredicate::GreaterThan { value, .. }
        | ColumnPredicate::GreaterThanOrEqual { value, .. }
        | ColumnPredicate::LessThanOrEqual { value, .. }
        | ColumnPredicate::Equal { value, .. }
        | ColumnPredicate::NotEqual { value, .. } => matches!(value, SqlValue::Null),
        ColumnPredicate::Between { low, high, .. } => {
            matches!(low, SqlValue::Null) || matches!(high, SqlValue::Null)
        }
        // LIKE patterns don't have NULL values in the pattern itself
        ColumnPredicate::Like { .. } => false,
        // IN/NOT IN implement SQL three-valued logic for NULL list elements
        // in the kernels themselves (issue #5341): a NULL element never
        // matches for IN, and poisons NOT IN (the result is never TRUE).
        // Don't short-circuit to all-false here — doing so excluded
        // genuinely matching rows for `x IN (a, NULL)`.
        ColumnPredicate::InList { .. } => false,
        // Column-to-column comparisons don't have literal NULLs
        // (NULL columns are handled during evaluation)
        ColumnPredicate::ColumnCompare { .. } => false,
        // Null tests carry no literal; they are evaluated from the null bitmap
        // and must NOT be short-circuited to an all-false mask.
        ColumnPredicate::IsNull { .. } | ColumnPredicate::IsNotNull { .. } => false,
        // Computed-column comparison handles NULL propagation per-row (a NULL
        // derived value is non-matching); a NULL constant is caught below.
        // Do NOT short-circuit the whole predicate to all-false here.
        ColumnPredicate::ComputedCompare { value, .. } => matches!(value, SqlValue::Null),
    }
}

/// Evaluate a computed-column comparison over rows `[start, end)` of the batch
/// (issue #5994). Materializes the derived arithmetic value per row via the
/// row-path evaluator (`DerivedExpr::evaluate_row`), then compares it against
/// the constant with the shared value-comparison semantics. A NULL derived
/// value (from NULL/absent inputs) is non-matching, matching the row path.
fn evaluate_computed_compare_range(
    batch: &ColumnarBatch,
    expr: &super::filter::DerivedExpr,
    op: CompareOp,
    value: &SqlValue,
    start: usize,
    end: usize,
) -> Result<Vec<bool>, ExecutorError> {
    use super::filter::evaluate_computed_compare_value;

    let mut result = Vec::with_capacity(end - start);
    for row_idx in start..end {
        // Fetch resolves NULL/absent to SqlValue::Null so arithmetic
        // propagates NULL; propagate real errors (e.g. missing column).
        let mut fetch_err: Option<ExecutorError> = None;
        let derived = {
            let mut fetch = |col_idx: usize| -> Option<SqlValue> {
                match batch.get_value(row_idx, col_idx) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        fetch_err = Some(e);
                        None
                    }
                }
            };
            expr.evaluate_row(&mut fetch)
        };
        if let Some(e) = fetch_err {
            return Err(e);
        }
        let passes = match derived {
            Ok(d) => evaluate_computed_compare_value(&d, op, value),
            Err(_) => false,
        };
        result.push(passes);
    }
    Ok(result)
}

/// If `predicate` is a null test (`IS NULL` / `IS NOT NULL`), evaluate it
/// directly from the referenced column's null bitmap over `[start, end)` and
/// return the resulting boolean mask. Returns `None` for any other predicate.
///
/// The mask is derived entirely from the null bitmap — no value comparison is
/// performed. A column with no null bitmap has no NULLs, so `IS NULL` yields an
/// all-false mask and `IS NOT NULL` an all-true mask. `Mixed` columns store
/// NULLs inline as `SqlValue::Null` (no bitmap), so they are resolved via
/// `get_value` to preserve correctness.
fn evaluate_null_predicate_range(
    batch: &ColumnarBatch,
    predicate: &ColumnPredicate,
    start: usize,
    end: usize,
) -> Result<Option<Vec<bool>>, ExecutorError> {
    let (column_idx, want_null) = match predicate {
        ColumnPredicate::IsNull { column_idx } => (*column_idx, true),
        ColumnPredicate::IsNotNull { column_idx } => (*column_idx, false),
        _ => return Ok(None),
    };

    let column = batch.column(column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: column_idx,
        batch_columns: batch.column_count(),
    })?;

    let range_len = end - start;

    if column.is_mixed() {
        // Mixed columns keep NULLs inline; consult get_value per row.
        let mut mask = Vec::with_capacity(range_len);
        for row_idx in start..end {
            let is_null = batch.get_value(row_idx, column_idx)? == SqlValue::Null;
            mask.push(is_null == want_null);
        }
        return Ok(Some(mask));
    }

    let mask = match column.null_bitmap() {
        // No null bitmap ⇒ no NULLs: IS NULL is all-false, IS NOT NULL all-true.
        None => vec![!want_null; range_len],
        Some(bitmap) => bitmap[start..end].iter().map(|&is_null| is_null == want_null).collect(),
    };
    Ok(Some(mask))
}

/// Apply SIMD-accelerated filtering to a columnar batch
///
/// Returns a new batch containing only the rows that pass all predicates.
/// Uses SIMD operations for numeric columns when possible, falls back to
/// scalar evaluation for other column types.
///
/// # Arguments
///
/// * `batch` - The columnar batch to filter
/// * `predicates` - Column-based predicates to evaluate
///
/// # Returns
///
/// A new ColumnarBatch containing only rows that pass all predicates
pub fn simd_filter_batch(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<ColumnarBatch, ExecutorError> {
    if predicates.is_empty() {
        // No predicates: return a clone of the batch
        return Ok(batch.clone());
    }

    // Create filter bitmap using SIMD operations (auto-parallelizes for large batches)
    let filter_mask = simd_create_filter_mask_auto(batch, predicates)?;

    // Apply filter mask to batch
    apply_filter_mask(batch, &filter_mask)
}

/// Apply SIMD-accelerated filtering with automatic parallelization
///
/// For large batches, this uses parallel execution to create the filter mask.
/// The decision is based on hardware-aware heuristics from ParallelConfig.
///
/// # Performance
///
/// - Small batches (<1000 rows): Single-threaded SIMD (avoids parallel overhead)
/// - Large batches (>=1000 rows on 8+ cores): Parallel filter mask creation
///
/// Parallel speedup:
/// - 4 cores: ~3x speedup on 100K+ rows
/// - 8 cores: ~6x speedup on 100K+ rows
/// - 16 cores: ~12x speedup on 1M+ rows
#[cfg(feature = "parallel")]
pub fn simd_filter_batch_parallel(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<ColumnarBatch, ExecutorError> {
    if predicates.is_empty() {
        return Ok(batch.clone());
    }

    let row_count = batch.row_count();
    let config = ParallelConfig::global();

    // Use parallel path for large batches
    if config.should_parallelize_scan(row_count) {
        let filter_mask = simd_create_filter_mask_parallel(batch, predicates)?;
        apply_filter_mask(batch, &filter_mask)
    } else {
        // Small batch: use sequential SIMD path
        let filter_mask = simd_create_filter_mask(batch, predicates)?;
        apply_filter_mask(batch, &filter_mask)
    }
}

/// Create a filter mask and return the indices of passing rows
///
/// This is the preferred function for late materialization optimization.
/// Instead of returning a filtered batch (which requires row reconstruction),
/// this returns only the indices of rows that pass all predicates.
///
/// # Performance
///
/// This function enables the "late materialization" pattern:
/// 1. Filter on columnar data (SIMD-accelerated)
/// 2. Return only indices of passing rows
/// 3. Clone only the rows that passed (not all rows)
///
/// For TPC-H Q10 with 60K lineitem rows where 20K pass:
/// - Old: Clone 60K rows, filter, clone 20K passing rows = 80K clones
/// - New: Filter, clone only 20K passing rows = 20K clones (75% reduction)
pub fn simd_filter_to_indices(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<Vec<usize>, ExecutorError> {
    let mask = simd_create_filter_mask(batch, predicates)?;
    Ok(mask
        .into_iter()
        .enumerate()
        .filter_map(|(idx, passes)| if passes { Some(idx) } else { None })
        .collect())
}

/// Create a filter mask using SIMD operations where possible
///
/// Returns a Vec<bool> where true means the row passes all predicates.
/// This function uses SIMD operations for numeric columns (i64/f64) and
/// falls back to scalar evaluation for other types.
///
/// This function is public to enable fused filter+aggregate optimization,
/// where the filter mask is used directly for aggregation without creating
/// an intermediate filtered batch.
pub fn simd_create_filter_mask(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<Vec<bool>, ExecutorError> {
    let row_count = batch.row_count();

    // Start with all rows passing
    let mut mask = vec![true; row_count];

    // Evaluate each predicate and AND the results using vectorized operation
    for predicate in predicates {
        let predicate_mask = evaluate_predicate_simd(batch, predicate)?;

        // Vectorized AND with existing mask
        simd_ops::and_masks_inplace(&mut mask, &predicate_mask);
    }

    Ok(mask)
}

/// Create a filter mask using packed bitmasks for improved efficiency.
///
/// This function provides 8x memory reduction compared to `simd_create_filter_mask`
/// by using packed bitmasks (1 bit per row) instead of Vec<bool> (1 byte per row).
///
/// # Performance Benefits
///
/// - **8x memory reduction**: For 6M rows, uses 750KB instead of 6MB
/// - **Native SIMD bitwise AND**: Combining predicates uses hardware SIMD instructions
/// - **Better cache utilization**: Smaller footprint means better cache hit rates
/// - **Faster popcount**: `count_ones()` maps to hardware popcount instruction
///
/// # Usage
///
/// This function is designed for use with fused filter+aggregate optimization:
///
/// ```text
/// let filter_mask = simd_create_filter_mask_packed(batch, predicates)?;
/// let sum = simd_ops::sum_f64_packed_filtered(values, &filter_mask);
/// let count = filter_mask.count_ones();
/// ```
pub fn simd_create_filter_mask_packed(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<PackedMask, ExecutorError> {
    let row_count = batch.row_count();

    // Start with all rows passing
    let mut mask = PackedMask::new_all_set(row_count);

    // Evaluate each predicate and AND the results
    for predicate in predicates {
        let predicate_mask = evaluate_predicate_simd_packed(batch, predicate)?;

        // Bitwise AND - this is a native SIMD operation
        mask.and_inplace(&predicate_mask);
    }

    Ok(mask)
}

/// Auto-selecting filter mask creation that chooses parallel or sequential
/// based on batch size and hardware capabilities.
///
/// This is the recommended entry point for filter mask creation as it
/// automatically selects the optimal execution strategy.
pub fn simd_create_filter_mask_auto(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<Vec<bool>, ExecutorError> {
    #[cfg(feature = "parallel")]
    {
        let row_count = batch.row_count();
        let config = ParallelConfig::global();

        if config.should_parallelize_scan(row_count) {
            return simd_create_filter_mask_parallel(batch, predicates);
        }
    }

    // Sequential path
    simd_create_filter_mask(batch, predicates)
}

/// Create a filter mask using parallel execution for large batches.
///
/// Partitions the row range into chunks and evaluates predicates in parallel
/// using rayon's parallel iterators. Results are combined into a single mask.
///
/// # Performance
///
/// For 6M rows on 16 cores with TPC-H Q6 predicates:
/// - Sequential: ~84ms
/// - Parallel: ~8ms (10x speedup)
///
/// The parallel overhead is ~50µs, so small batches (<1000 rows) should use
/// the sequential path.
#[cfg(feature = "parallel")]
pub fn simd_create_filter_mask_parallel(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<Vec<bool>, ExecutorError> {
    let row_count = batch.row_count();

    if row_count == 0 {
        return Ok(Vec::new());
    }

    // Determine chunk size for parallel processing
    // Target: each thread gets at least 10K rows for efficiency
    let num_threads = rayon::current_num_threads();
    let chunk_size = (row_count / num_threads).clamp(10_000, 100_000);

    // For very small batches, fall back to sequential
    if row_count < chunk_size * 2 {
        return simd_create_filter_mask(batch, predicates);
    }

    // Create ranges for parallel processing
    let ranges: Vec<(usize, usize)> = (0..row_count)
        .step_by(chunk_size)
        .map(|start| {
            let end = (start + chunk_size).min(row_count);
            (start, end)
        })
        .collect();

    // Process each range in parallel
    let partial_masks: Vec<Result<Vec<bool>, ExecutorError>> = ranges
        .par_iter()
        .map(|(start, end)| evaluate_predicates_for_range(batch, predicates, *start, *end))
        .collect();

    // Combine partial masks into final result
    let mut final_mask = Vec::with_capacity(row_count);
    for partial in partial_masks {
        final_mask.extend(partial?);
    }

    Ok(final_mask)
}

/// Evaluate all predicates for a range of rows
#[cfg(feature = "parallel")]
fn evaluate_predicates_for_range(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
    start: usize,
    end: usize,
) -> Result<Vec<bool>, ExecutorError> {
    let range_len = end - start;

    // Start with all rows in range passing
    let mut mask = vec![true; range_len];

    for predicate in predicates {
        // NULL handling
        if predicate_contains_null(predicate) {
            return Ok(vec![false; range_len]);
        }

        let predicate_mask = evaluate_predicate_for_range(batch, predicate, start, end)?;

        // AND with existing mask
        for (m, p) in mask.iter_mut().zip(predicate_mask.iter()) {
            *m = *m && *p;
        }
    }

    Ok(mask)
}

/// Evaluate a single predicate for a range of rows
#[cfg(feature = "parallel")]
fn evaluate_predicate_for_range(
    batch: &ColumnarBatch,
    predicate: &ColumnPredicate,
    start: usize,
    end: usize,
) -> Result<Vec<bool>, ExecutorError> {
    // Handle null tests directly from the null bitmap.
    if let Some(mask) = evaluate_null_predicate_range(batch, predicate, start, end)? {
        return Ok(mask);
    }

    // Handle ColumnCompare specially
    if let ColumnPredicate::ColumnCompare { left_column_idx, op, right_column_idx } = predicate {
        return evaluate_column_compare_range(
            batch,
            *left_column_idx,
            *op,
            *right_column_idx,
            start,
            end,
        );
    }

    // Handle computed-column comparison (issue #5994).
    if let ColumnPredicate::ComputedCompare { expr, op, value, .. } = predicate {
        return evaluate_computed_compare_range(batch, expr, *op, value, start, end);
    }

    let column_idx = match predicate {
        ColumnPredicate::LessThan { column_idx, .. }
        | ColumnPredicate::GreaterThan { column_idx, .. }
        | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
        | ColumnPredicate::LessThanOrEqual { column_idx, .. }
        | ColumnPredicate::Equal { column_idx, .. }
        | ColumnPredicate::NotEqual { column_idx, .. }
        | ColumnPredicate::Between { column_idx, .. }
        | ColumnPredicate::Like { column_idx, .. }
        | ColumnPredicate::InList { column_idx, .. } => *column_idx,
        ColumnPredicate::ColumnCompare { .. }
        | ColumnPredicate::IsNull { .. }
        | ColumnPredicate::IsNotNull { .. }
        | ColumnPredicate::ComputedCompare { .. } => unreachable!(),
    };

    let column = batch.column(column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: column_idx,
        batch_columns: batch.column_count(),
    })?;

    match column {
        ColumnArray::Int64(values, nulls) => {
            let values_slice = &values[start..end];
            let nulls_slice = nulls.as_ref().map(|n| &n[start..end] as &[bool]);
            comparison::evaluate_predicate_i64_simd(predicate, values_slice, nulls_slice)
        }
        ColumnArray::Float64(values, nulls) => {
            let values_slice = &values[start..end];
            let nulls_slice = nulls.as_ref().map(|n| &n[start..end] as &[bool]);
            comparison::evaluate_predicate_f64_simd(predicate, values_slice, nulls_slice)
        }
        ColumnArray::Date(values, nulls) => {
            let values_slice = &values[start..end];
            let nulls_slice = nulls.as_ref().map(|n| &n[start..end] as &[bool]);
            comparison::evaluate_predicate_i32_simd(predicate, values_slice, nulls_slice)
        }
        ColumnArray::Timestamp(values, nulls) => {
            let values_slice = &values[start..end];
            let nulls_slice = nulls.as_ref().map(|n| &n[start..end] as &[bool]);
            // Issue #5335: Timestamp columns need temporal semantics, not the
            // INTEGER-affinity semantics of the i64 kernel
            comparison::evaluate_predicate_timestamp_simd(predicate, values_slice, nulls_slice)
        }
        ColumnArray::String(values, nulls) => {
            let values_slice = &values[start..end];
            let nulls_slice = nulls.as_ref().map(|n| &n[start..end] as &[bool]);
            string::evaluate_predicate_string_batch(predicate, values_slice, nulls_slice)
        }
        ColumnArray::FixedString(values, nulls) => {
            let values_slice = &values[start..end];
            let nulls_slice = nulls.as_ref().map(|n| &n[start..end] as &[bool]);
            string::evaluate_predicate_string_batch(predicate, values_slice, nulls_slice)
        }
        _ => {
            // Scalar fallback for other types
            let range_len = end - start;
            let mut result = Vec::with_capacity(range_len);
            for row_idx in start..end {
                let value = batch.get_value(row_idx, column_idx)?;
                if value == SqlValue::Null {
                    result.push(false);
                } else {
                    result.push(super::filter::evaluate_predicate(predicate, &value));
                }
            }
            Ok(result)
        }
    }
}

/// Evaluate column-to-column comparison for a range of rows
#[cfg(feature = "parallel")]
fn evaluate_column_compare_range(
    batch: &ColumnarBatch,
    left_column_idx: usize,
    op: CompareOp,
    right_column_idx: usize,
    start: usize,
    end: usize,
) -> Result<Vec<bool>, ExecutorError> {
    let left_column =
        batch.column(left_column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: left_column_idx,
            batch_columns: batch.column_count(),
        })?;

    let right_column =
        batch.column(right_column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: right_column_idx,
            batch_columns: batch.column_count(),
        })?;

    match (left_column, right_column) {
        (
            ColumnArray::Date(left_values, left_nulls),
            ColumnArray::Date(right_values, right_nulls),
        ) => {
            let left_slice = &left_values[start..end];
            let right_slice = &right_values[start..end];
            let left_nulls_slice = left_nulls.as_ref().map(|n| &n[start..end]);
            let right_nulls_slice = right_nulls.as_ref().map(|n| &n[start..end]);
            evaluate_column_compare_i32_range(
                left_slice,
                left_nulls_slice,
                op,
                right_slice,
                right_nulls_slice,
            )
        }
        (
            ColumnArray::Int64(left_values, left_nulls),
            ColumnArray::Int64(right_values, right_nulls),
        ) => {
            let left_slice = &left_values[start..end];
            let right_slice = &right_values[start..end];
            let left_nulls_slice = left_nulls.as_ref().map(|n| &n[start..end]);
            let right_nulls_slice = right_nulls.as_ref().map(|n| &n[start..end]);
            evaluate_column_compare_i64_range(
                left_slice,
                left_nulls_slice,
                op,
                right_slice,
                right_nulls_slice,
            )
        }
        (
            ColumnArray::Float64(left_values, left_nulls),
            ColumnArray::Float64(right_values, right_nulls),
        ) => {
            let left_slice = &left_values[start..end];
            let right_slice = &right_values[start..end];
            let left_nulls_slice = left_nulls.as_ref().map(|n| &n[start..end]);
            let right_nulls_slice = right_nulls.as_ref().map(|n| &n[start..end]);
            evaluate_column_compare_f64_range(
                left_slice,
                left_nulls_slice,
                op,
                right_slice,
                right_nulls_slice,
            )
        }
        _ => {
            // Scalar fallback
            let range_len = end - start;
            let mut result = Vec::with_capacity(range_len);
            for row_idx in start..end {
                let left_val = batch.get_value(row_idx, left_column_idx)?;
                let right_val = batch.get_value(row_idx, right_column_idx)?;
                result.push(evaluate_column_compare(op, Some(&left_val), Some(&right_val)));
            }
            Ok(result)
        }
    }
}

#[cfg(feature = "parallel")]
fn evaluate_column_compare_i32_range(
    left_values: &[i32],
    left_nulls: Option<&[bool]>,
    op: CompareOp,
    right_values: &[i32],
    right_nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let len = left_values.len();
    let mut result = vec![false; len];
    let has_left_nulls = left_nulls.is_some();
    let has_right_nulls = right_nulls.is_some();

    for i in 0..len {
        let is_null = (has_left_nulls && left_nulls.unwrap()[i])
            || (has_right_nulls && right_nulls.unwrap()[i]);
        if is_null {
            continue;
        }
        result[i] = match op {
            CompareOp::LessThan => left_values[i] < right_values[i],
            CompareOp::GreaterThan => left_values[i] > right_values[i],
            CompareOp::LessThanOrEqual => left_values[i] <= right_values[i],
            CompareOp::GreaterThanOrEqual => left_values[i] >= right_values[i],
            CompareOp::Equal => left_values[i] == right_values[i],
            CompareOp::NotEqual => left_values[i] != right_values[i],
        };
    }
    Ok(result)
}

#[cfg(feature = "parallel")]
fn evaluate_column_compare_i64_range(
    left_values: &[i64],
    left_nulls: Option<&[bool]>,
    op: CompareOp,
    right_values: &[i64],
    right_nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let len = left_values.len();
    let mut result = vec![false; len];
    let has_left_nulls = left_nulls.is_some();
    let has_right_nulls = right_nulls.is_some();

    for i in 0..len {
        let is_null = (has_left_nulls && left_nulls.unwrap()[i])
            || (has_right_nulls && right_nulls.unwrap()[i]);
        if is_null {
            continue;
        }
        result[i] = match op {
            CompareOp::LessThan => left_values[i] < right_values[i],
            CompareOp::GreaterThan => left_values[i] > right_values[i],
            CompareOp::LessThanOrEqual => left_values[i] <= right_values[i],
            CompareOp::GreaterThanOrEqual => left_values[i] >= right_values[i],
            CompareOp::Equal => left_values[i] == right_values[i],
            CompareOp::NotEqual => left_values[i] != right_values[i],
        };
    }
    Ok(result)
}

#[cfg(feature = "parallel")]
fn evaluate_column_compare_f64_range(
    left_values: &[f64],
    left_nulls: Option<&[bool]>,
    op: CompareOp,
    right_values: &[f64],
    right_nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let len = left_values.len();
    let mut result = vec![false; len];
    let has_left_nulls = left_nulls.is_some();
    let has_right_nulls = right_nulls.is_some();

    for i in 0..len {
        let is_null = (has_left_nulls && left_nulls.unwrap()[i])
            || (has_right_nulls && right_nulls.unwrap()[i]);
        if is_null {
            continue;
        }
        result[i] = match op {
            CompareOp::LessThan => left_values[i] < right_values[i],
            CompareOp::GreaterThan => left_values[i] > right_values[i],
            CompareOp::LessThanOrEqual => left_values[i] <= right_values[i],
            CompareOp::GreaterThanOrEqual => left_values[i] >= right_values[i],
            CompareOp::Equal => left_values[i] == right_values[i],
            CompareOp::NotEqual => left_values[i] != right_values[i],
        };
    }
    Ok(result)
}

/// Evaluate a single predicate returning a packed mask
fn evaluate_predicate_simd_packed(
    batch: &ColumnarBatch,
    predicate: &ColumnPredicate,
) -> Result<PackedMask, ExecutorError> {
    // NULL handling: any comparison with NULL returns false (UNKNOWN in SQL)
    if predicate_contains_null(predicate) {
        return Ok(PackedMask::new_all_clear(batch.row_count()));
    }

    // Handle null tests directly from the null bitmap.
    if let Some(bool_mask) = evaluate_null_predicate_range(batch, predicate, 0, batch.row_count())?
    {
        return Ok(PackedMask::from_bool_slice(&bool_mask));
    }

    // Handle ColumnCompare specially - needs two columns
    if let ColumnPredicate::ColumnCompare { left_column_idx, op, right_column_idx } = predicate {
        let bool_mask =
            evaluate_column_compare_simd(batch, *left_column_idx, *op, *right_column_idx)?;
        return Ok(PackedMask::from_bool_slice(&bool_mask));
    }

    // Handle computed-column comparison (issue #5994).
    if let ColumnPredicate::ComputedCompare { expr, op, value, .. } = predicate {
        let bool_mask =
            evaluate_computed_compare_range(batch, expr, *op, value, 0, batch.row_count())?;
        return Ok(PackedMask::from_bool_slice(&bool_mask));
    }

    let column_idx = match predicate {
        ColumnPredicate::LessThan { column_idx, .. }
        | ColumnPredicate::GreaterThan { column_idx, .. }
        | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
        | ColumnPredicate::LessThanOrEqual { column_idx, .. }
        | ColumnPredicate::Equal { column_idx, .. }
        | ColumnPredicate::NotEqual { column_idx, .. }
        | ColumnPredicate::Between { column_idx, .. }
        | ColumnPredicate::Like { column_idx, .. }
        | ColumnPredicate::InList { column_idx, .. } => *column_idx,
        ColumnPredicate::ColumnCompare { .. }
        | ColumnPredicate::IsNull { .. }
        | ColumnPredicate::IsNotNull { .. }
        | ColumnPredicate::ComputedCompare { .. } => unreachable!(), // Handled above
    };

    let column = batch.column(column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: column_idx,
        batch_columns: batch.column_count(),
    })?;

    match column {
        // Packed path for i64 columns
        ColumnArray::Int64(values, nulls) => {
            evaluate_predicate_i64_packed(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // Packed path for f64 columns
        ColumnArray::Float64(values, nulls) => {
            evaluate_predicate_f64_packed(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // Packed path for Date columns (i32)
        ColumnArray::Date(values, nulls) => {
            evaluate_predicate_i32_packed(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // Packed path for Timestamp columns (i64 microseconds; issue #5335:
        // temporal semantics, not the INTEGER-affinity i64 kernel)
        ColumnArray::Timestamp(values, nulls) => comparison::evaluate_predicate_timestamp_packed(
            predicate,
            values,
            nulls.as_ref().map(|n| n.as_slice()),
        ),

        // For other types, fall back to Vec<bool> and convert
        _ => {
            let bool_mask = evaluate_predicate_simd(batch, predicate)?;
            Ok(PackedMask::from_bool_slice(&bool_mask))
        }
    }
}

/// Evaluate a single predicate using SIMD operations when possible
fn evaluate_predicate_simd(
    batch: &ColumnarBatch,
    predicate: &ColumnPredicate,
) -> Result<Vec<bool>, ExecutorError> {
    // NULL handling: any comparison with NULL returns false (UNKNOWN in SQL)
    // Return all-false mask immediately if predicate contains NULL literal
    if predicate_contains_null(predicate) {
        return Ok(vec![false; batch.row_count()]);
    }

    // Handle null tests directly from the null bitmap.
    if let Some(bool_mask) = evaluate_null_predicate_range(batch, predicate, 0, batch.row_count())?
    {
        return Ok(bool_mask);
    }

    // Handle ColumnCompare specially - needs two columns
    if let ColumnPredicate::ColumnCompare { left_column_idx, op, right_column_idx } = predicate {
        return evaluate_column_compare_simd(batch, *left_column_idx, *op, *right_column_idx);
    }

    // Handle computed-column comparison (issue #5994).
    if let ColumnPredicate::ComputedCompare { expr, op, value, .. } = predicate {
        return evaluate_computed_compare_range(batch, expr, *op, value, 0, batch.row_count());
    }

    let column_idx = match predicate {
        ColumnPredicate::LessThan { column_idx, .. }
        | ColumnPredicate::GreaterThan { column_idx, .. }
        | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
        | ColumnPredicate::LessThanOrEqual { column_idx, .. }
        | ColumnPredicate::Equal { column_idx, .. }
        | ColumnPredicate::NotEqual { column_idx, .. }
        | ColumnPredicate::Between { column_idx, .. }
        | ColumnPredicate::Like { column_idx, .. }
        | ColumnPredicate::InList { column_idx, .. } => *column_idx,
        ColumnPredicate::ColumnCompare { .. }
        | ColumnPredicate::IsNull { .. }
        | ColumnPredicate::IsNotNull { .. }
        | ColumnPredicate::ComputedCompare { .. } => unreachable!(), // Handled above
    };

    let column = batch.column(column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: column_idx,
        batch_columns: batch.column_count(),
    })?;

    match column {
        // SIMD path for i64 columns
        ColumnArray::Int64(values, nulls) => {
            evaluate_predicate_i64_simd(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // SIMD path for f64 columns
        ColumnArray::Float64(values, nulls) => {
            evaluate_predicate_f64_simd(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // SIMD path for Date columns (i32 - days since epoch)
        ColumnArray::Date(values, nulls) => {
            evaluate_predicate_i32_simd(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // SIMD path for Timestamp columns (i64 microseconds; issue #5335:
        // temporal semantics, not the INTEGER-affinity i64 kernel)
        ColumnArray::Timestamp(values, nulls) => comparison::evaluate_predicate_timestamp_simd(
            predicate,
            values,
            nulls.as_ref().map(|n| n.as_slice()),
        ),

        // Batch string operations for String columns
        ColumnArray::String(values, nulls) => {
            evaluate_predicate_string_batch(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // Batch string operations for FixedString columns
        ColumnArray::FixedString(values, nulls) => {
            evaluate_predicate_string_batch(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // Scalar fallback for other column types
        _ => evaluate_predicate_scalar(batch, predicate, column_idx),
    }
}

/// Evaluate a column-to-column comparison using vectorized operations where possible
///
/// This is the main optimization for predicates like `l_commitdate < l_receiptdate` in TPC-H Q4.
/// For Date columns (i32), this uses a tight scalar loop that the compiler can auto-vectorize.
fn evaluate_column_compare_simd(
    batch: &ColumnarBatch,
    left_column_idx: usize,
    op: CompareOp,
    right_column_idx: usize,
) -> Result<Vec<bool>, ExecutorError> {
    let row_count = batch.row_count();

    let left_column =
        batch.column(left_column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: left_column_idx,
            batch_columns: batch.column_count(),
        })?;

    let right_column =
        batch.column(right_column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: right_column_idx,
            batch_columns: batch.column_count(),
        })?;

    // Try SIMD path for matching column types
    match (left_column, right_column) {
        // Date columns (i32) - common case for TPC-H Q4 (l_commitdate < l_receiptdate)
        (
            ColumnArray::Date(left_values, left_nulls),
            ColumnArray::Date(right_values, right_nulls),
        ) => evaluate_column_compare_i32_simd(
            left_values,
            left_nulls.as_ref().map(|n| n.as_slice()),
            op,
            right_values,
            right_nulls.as_ref().map(|n| n.as_slice()),
        ),

        // Int64 columns
        (
            ColumnArray::Int64(left_values, left_nulls),
            ColumnArray::Int64(right_values, right_nulls),
        ) => evaluate_column_compare_i64_simd(
            left_values,
            left_nulls.as_ref().map(|n| n.as_slice()),
            op,
            right_values,
            right_nulls.as_ref().map(|n| n.as_slice()),
        ),

        // Float64 columns
        (
            ColumnArray::Float64(left_values, left_nulls),
            ColumnArray::Float64(right_values, right_nulls),
        ) => evaluate_column_compare_f64_simd(
            left_values,
            left_nulls.as_ref().map(|n| n.as_slice()),
            op,
            right_values,
            right_nulls.as_ref().map(|n| n.as_slice()),
        ),

        // Timestamp columns (i64)
        (
            ColumnArray::Timestamp(left_values, left_nulls),
            ColumnArray::Timestamp(right_values, right_nulls),
        ) => evaluate_column_compare_i64_simd(
            left_values,
            left_nulls.as_ref().map(|n| n.as_slice()),
            op,
            right_values,
            right_nulls.as_ref().map(|n| n.as_slice()),
        ),

        // Fallback to scalar evaluation for other types or mismatched types
        _ => {
            let mut result = Vec::with_capacity(row_count);
            for row_idx in 0..row_count {
                let left_val = batch.get_value(row_idx, left_column_idx)?;
                let right_val = batch.get_value(row_idx, right_column_idx)?;
                let passes = evaluate_column_compare(op, Some(&left_val), Some(&right_val));
                result.push(passes);
            }
            Ok(result)
        }
    }
}

/// Evaluate column-to-column comparison for i32 arrays (Date columns)
/// Uses a tight loop that LLVM can auto-vectorize
fn evaluate_column_compare_i32_simd(
    left_values: &[i32],
    left_nulls: Option<&[bool]>,
    op: CompareOp,
    right_values: &[i32],
    right_nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let len = left_values.len();
    let mut result = vec![false; len];

    // Pre-compute null masks
    let has_left_nulls = left_nulls.is_some();
    let has_right_nulls = right_nulls.is_some();

    match op {
        CompareOp::LessThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] < right_values[i];
            }
        }
        CompareOp::GreaterThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] > right_values[i];
            }
        }
        CompareOp::LessThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] <= right_values[i];
            }
        }
        CompareOp::GreaterThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] >= right_values[i];
            }
        }
        CompareOp::Equal => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] == right_values[i];
            }
        }
        CompareOp::NotEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] != right_values[i];
            }
        }
    }

    Ok(result)
}

/// Evaluate column-to-column comparison for i64 arrays
fn evaluate_column_compare_i64_simd(
    left_values: &[i64],
    left_nulls: Option<&[bool]>,
    op: CompareOp,
    right_values: &[i64],
    right_nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let len = left_values.len();
    let mut result = vec![false; len];

    let has_left_nulls = left_nulls.is_some();
    let has_right_nulls = right_nulls.is_some();

    match op {
        CompareOp::LessThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] < right_values[i];
            }
        }
        CompareOp::GreaterThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] > right_values[i];
            }
        }
        CompareOp::LessThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] <= right_values[i];
            }
        }
        CompareOp::GreaterThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] >= right_values[i];
            }
        }
        CompareOp::Equal => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] == right_values[i];
            }
        }
        CompareOp::NotEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] != right_values[i];
            }
        }
    }

    Ok(result)
}

/// Evaluate column-to-column comparison for f64 arrays
fn evaluate_column_compare_f64_simd(
    left_values: &[f64],
    left_nulls: Option<&[bool]>,
    op: CompareOp,
    right_values: &[f64],
    right_nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let len = left_values.len();
    let mut result = vec![false; len];

    let has_left_nulls = left_nulls.is_some();
    let has_right_nulls = right_nulls.is_some();

    match op {
        CompareOp::LessThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] < right_values[i];
            }
        }
        CompareOp::GreaterThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] > right_values[i];
            }
        }
        CompareOp::LessThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] <= right_values[i];
            }
        }
        CompareOp::GreaterThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] >= right_values[i];
            }
        }
        CompareOp::Equal => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] == right_values[i];
            }
        }
        CompareOp::NotEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] != right_values[i];
            }
        }
    }

    Ok(result)
}

/// Scalar fallback for non-numeric columns
fn evaluate_predicate_scalar(
    batch: &ColumnarBatch,
    predicate: &ColumnPredicate,
    column_idx: usize,
) -> Result<Vec<bool>, ExecutorError> {
    let row_count = batch.row_count();
    let mut result = Vec::with_capacity(row_count);

    for row_idx in 0..row_count {
        let value = batch.get_value(row_idx, column_idx)?;

        // NULL values always fail
        if value == SqlValue::Null {
            result.push(false);
            continue;
        }

        let passes = super::filter::evaluate_predicate(predicate, &value);
        result.push(passes);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use vibesql_storage::Row;

    use super::*;

    #[test]
    fn test_simd_filter_i64() {
        // Create a batch with i64 column
        let rows = vec![
            Row::new(vec![SqlValue::Integer(5)]),
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(15)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(25)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: column_0 < 18
        let predicates =
            vec![ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(18) }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        assert_eq!(filtered.row_count(), 3);
        assert_eq!(filtered.get_value(0, 0).unwrap(), SqlValue::Integer(5));
        assert_eq!(filtered.get_value(1, 0).unwrap(), SqlValue::Integer(10));
        assert_eq!(filtered.get_value(2, 0).unwrap(), SqlValue::Integer(15));
    }

    #[test]
    fn test_simd_filter_f64() {
        // Create a batch with f64 column
        let rows = vec![
            Row::new(vec![SqlValue::Double(0.04)]),
            Row::new(vec![SqlValue::Double(0.05)]),
            Row::new(vec![SqlValue::Double(0.06)]),
            Row::new(vec![SqlValue::Double(0.07)]),
            Row::new(vec![SqlValue::Double(0.08)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: column_0 BETWEEN 0.05 AND 0.07
        let predicates = vec![ColumnPredicate::Between {
            column_idx: 0,
            low: SqlValue::Double(0.05),
            high: SqlValue::Double(0.07),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        assert_eq!(filtered.row_count(), 3);
        assert_eq!(filtered.get_value(0, 0).unwrap(), SqlValue::Double(0.05));
        assert_eq!(filtered.get_value(1, 0).unwrap(), SqlValue::Double(0.06));
        assert_eq!(filtered.get_value(2, 0).unwrap(), SqlValue::Double(0.07));
    }

    #[test]
    fn test_simd_filter_multiple_predicates() {
        // Create a batch with two columns
        let rows = vec![
            Row::new(vec![SqlValue::Integer(5), SqlValue::Double(0.04)]),
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(0.05)]),
            Row::new(vec![SqlValue::Integer(15), SqlValue::Double(0.06)]),
            Row::new(vec![SqlValue::Integer(20), SqlValue::Double(0.07)]),
            Row::new(vec![SqlValue::Integer(25), SqlValue::Double(0.08)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: column_0 > 10 AND column_1 < 0.07
        let predicates = vec![
            ColumnPredicate::GreaterThan { column_idx: 0, value: SqlValue::Integer(10) },
            ColumnPredicate::LessThan { column_idx: 1, value: SqlValue::Double(0.07) },
        ];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should only match row 2 (15, 0.06)
        assert_eq!(filtered.row_count(), 1);
        assert_eq!(filtered.get_value(0, 0).unwrap(), SqlValue::Integer(15));
        assert_eq!(filtered.get_value(0, 1).unwrap(), SqlValue::Double(0.06));
    }

    #[test]
    fn test_simd_filter_date_less_than() {
        use vibesql_types::Date;

        // Create a batch with date column
        let rows = vec![
            Row::new(vec![SqlValue::Date(Date { year: 1994, month: 1, day: 1 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1995, month: 6, day: 15 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1996, month: 12, day: 31 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1997, month: 3, day: 10 })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date < 1996-01-01
        let predicates = vec![ColumnPredicate::LessThan {
            column_idx: 0,
            value: SqlValue::Date(Date { year: 1996, month: 1, day: 1 }),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match first two rows (1994-01-01, 1995-06-15)
        assert_eq!(filtered.row_count(), 2);
    }

    #[test]
    fn test_simd_filter_date_between() {
        use vibesql_types::Date;

        // Create a batch with date column
        let rows = vec![
            Row::new(vec![SqlValue::Date(Date { year: 1994, month: 1, day: 1 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1995, month: 6, day: 15 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1996, month: 12, day: 31 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1997, month: 3, day: 10 })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date BETWEEN 1995-01-01 AND 1996-12-31
        let predicates = vec![ColumnPredicate::Between {
            column_idx: 0,
            low: SqlValue::Date(Date { year: 1995, month: 1, day: 1 }),
            high: SqlValue::Date(Date { year: 1996, month: 12, day: 31 }),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match middle two rows (1995-06-15, 1996-12-31)
        assert_eq!(filtered.row_count(), 2);
    }

    #[test]
    fn test_simd_filter_date_with_nulls() {
        use vibesql_types::Date;

        // Create a batch with date column including NULLs
        let rows = vec![
            Row::new(vec![SqlValue::Date(Date { year: 1994, month: 1, day: 1 })]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Date(Date { year: 1996, month: 12, day: 31 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1997, month: 3, day: 10 })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date >= 1996-01-01
        let predicates = vec![ColumnPredicate::GreaterThanOrEqual {
            column_idx: 0,
            value: SqlValue::Date(Date { year: 1996, month: 1, day: 1 }),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match last two rows (1996-12-31, 1997-03-10), NULLs excluded
        assert_eq!(filtered.row_count(), 2);
    }

    #[test]
    fn test_simd_filter_date_equal() {
        use vibesql_types::Date;

        // Create a batch with date column
        let rows = vec![
            Row::new(vec![SqlValue::Date(Date { year: 1994, month: 1, day: 1 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1995, month: 6, day: 15 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1995, month: 6, day: 15 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1997, month: 3, day: 10 })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date = 1995-06-15
        let predicates = vec![ColumnPredicate::Equal {
            column_idx: 0,
            value: SqlValue::Date(Date { year: 1995, month: 6, day: 15 }),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match two middle rows
        assert_eq!(filtered.row_count(), 2);
    }

    #[test]
    fn test_between_f64_bug() {
        // Create a batch with f64 column - matching the issue test case
        let rows = vec![
            Row::new(vec![SqlValue::Double(0.02)]), // Should pass BETWEEN 0.02 AND 0.03
            Row::new(vec![SqlValue::Double(0.03)]), // Should pass BETWEEN 0.02 AND 0.03
            Row::new(vec![SqlValue::Double(0.025)]), // Should pass BETWEEN 0.02 AND 0.03
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: column_0 BETWEEN 0.02 AND 0.03
        let predicates = vec![ColumnPredicate::Between {
            column_idx: 0,
            low: SqlValue::Double(0.02),
            high: SqlValue::Double(0.03),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // All 3 rows should pass
        assert_eq!(filtered.row_count(), 3, "All rows should pass BETWEEN 0.02 AND 0.03");
    }

    /// Reproduce the exact issue #2857 scenario
    /// The bug appears when combining:
    /// - Multiple predicates including date range and BETWEEN on float
    /// - Expression aggregate SUM(amount * fee)
    #[test]
    fn test_issue_2857_scenario() {
        use vibesql_types::Date;

        // Match the test case exactly:
        // TXN_DATE (Date), AMOUNT (Double), FEE (Double)
        // (2024-01-10, 1000.0, 0.02)
        // (2024-01-15, 2000.0, 0.03)
        // (2024-01-20, 1500.0, 0.025)
        let rows = vec![
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 10 }),
                SqlValue::Double(1000.0),
                SqlValue::Double(0.02),
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 15 }),
                SqlValue::Double(2000.0),
                SqlValue::Double(0.03),
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 20 }),
                SqlValue::Double(1500.0),
                SqlValue::Double(0.025),
            ]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Predicates from the query:
        // WHERE txn_date >= '2024-01-01'
        //   AND txn_date < '2024-02-01'
        //   AND fee BETWEEN 0.02 AND 0.03
        let predicates = vec![
            ColumnPredicate::GreaterThanOrEqual {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 1, day: 1 }),
            },
            ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 2, day: 1 }),
            },
            ColumnPredicate::Between {
                column_idx: 2,
                low: SqlValue::Double(0.02),
                high: SqlValue::Double(0.03),
            },
        ];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // All 3 rows should pass all predicates:
        // - All dates are in Jan 2024
        // - All fees are in [0.02, 0.03] range
        assert_eq!(filtered.row_count(), 3, "All rows should pass all predicates");
    }

    /// Test with SqlValue::Numeric predicates (what the parser generates)
    #[test]
    fn test_issue_2857_with_numeric_predicates() {
        use vibesql_types::Date;

        // Data uses SqlValue::Double (from storage)
        let rows = vec![
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 10 }),
                SqlValue::Double(1000.0),
                SqlValue::Double(0.02), // Data is Double
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 15 }),
                SqlValue::Double(2000.0),
                SqlValue::Double(0.03),
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 20 }),
                SqlValue::Double(1500.0),
                SqlValue::Double(0.025),
            ]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Predicates use SqlValue::Numeric (from parser)
        // This matches what happens in real queries
        let predicates = vec![
            ColumnPredicate::GreaterThanOrEqual {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 1, day: 1 }),
            },
            ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 2, day: 1 }),
            },
            ColumnPredicate::Between {
                column_idx: 2,
                low: SqlValue::Numeric(0.02), // Parser generates Numeric!
                high: SqlValue::Numeric(0.03),
            },
        ];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // All 3 rows should pass
        assert_eq!(
            filtered.row_count(),
            3,
            "All rows should pass (Numeric predicate on Double column)"
        );
    }

    /// Test using from_storage_columnar which is the path used by native columnar execution
    #[test]
    fn test_issue_2857_from_storage_columnar() {
        use vibesql_types::Date;

        // Create rows the same way the integration test does
        let rows = vec![
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 10 }),
                SqlValue::Double(1000.0),
                SqlValue::Double(0.02),
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 15 }),
                SqlValue::Double(2000.0),
                SqlValue::Double(0.03),
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 20 }),
                SqlValue::Double(1500.0),
                SqlValue::Double(0.025),
            ]),
        ];

        let column_names = vec!["TXN_DATE".to_string(), "AMOUNT".to_string(), "FEE".to_string()];
        let storage_columnar =
            vibesql_storage::ColumnarTable::from_rows(&rows, &column_names).unwrap();

        // This is the path used by try_native_columnar_execution
        let batch = ColumnarBatch::from_storage_columnar(&storage_columnar).unwrap();

        // Predicates exactly like what the parser would generate
        // Note: Date predicates come from 'YYYY-MM-DD' string literals parsed to SqlValue::Date
        let predicates = vec![
            ColumnPredicate::GreaterThanOrEqual {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 1, day: 1 }),
            },
            ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 2, day: 1 }),
            },
            ColumnPredicate::Between {
                column_idx: 2,
                low: SqlValue::Numeric(0.02), // Parser generates Numeric
                high: SqlValue::Numeric(0.03),
            },
        ];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // All 3 rows should pass all predicates
        assert_eq!(filtered.row_count(), 3, "All rows should pass from_storage_columnar path");
    }

    /// Test column-to-column comparison for Date columns (TPC-H Q4 pattern)
    #[test]
    fn test_simd_filter_column_compare_date() {
        use vibesql_types::Date;

        // Test case: l_commitdate < l_receiptdate (TPC-H Q4 pattern)
        // Two date columns where we compare values within the same row
        let rows = vec![
            // Row 0: commitdate < receiptdate -> passes
            Row::new(vec![
                SqlValue::Date(Date { year: 1994, month: 6, day: 10 }), // commitdate
                SqlValue::Date(Date { year: 1994, month: 6, day: 20 }), // receiptdate
            ]),
            // Row 1: commitdate == receiptdate -> fails
            Row::new(vec![
                SqlValue::Date(Date { year: 1994, month: 6, day: 15 }),
                SqlValue::Date(Date { year: 1994, month: 6, day: 15 }),
            ]),
            // Row 2: commitdate > receiptdate -> fails
            Row::new(vec![
                SqlValue::Date(Date { year: 1994, month: 6, day: 25 }),
                SqlValue::Date(Date { year: 1994, month: 6, day: 20 }),
            ]),
            // Row 3: commitdate < receiptdate -> passes
            Row::new(vec![
                SqlValue::Date(Date { year: 1995, month: 1, day: 5 }),
                SqlValue::Date(Date { year: 1995, month: 3, day: 10 }),
            ]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: col0 < col1 (commitdate < receiptdate)
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::LessThan,
            right_column_idx: 1,
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match rows 0 and 3
        assert_eq!(filtered.row_count(), 2);
    }

    /// Test column-to-column comparison for Int64 columns
    #[test]
    fn test_simd_filter_column_compare_int64() {
        let rows = vec![
            // Row 0: 5 < 10 -> passes
            Row::new(vec![SqlValue::Integer(5), SqlValue::Integer(10)]),
            // Row 1: 10 < 10 -> fails
            Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(10)]),
            // Row 2: 15 < 10 -> fails
            Row::new(vec![SqlValue::Integer(15), SqlValue::Integer(10)]),
            // Row 3: 3 < 20 -> passes
            Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(20)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::LessThan,
            right_column_idx: 1,
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match rows 0 and 3
        assert_eq!(filtered.row_count(), 2);
    }

    /// Test column-to-column comparison with NULL handling
    #[test]
    fn test_simd_filter_column_compare_with_nulls() {
        use vibesql_types::Date;

        let rows = vec![
            // Row 0: commitdate < receiptdate -> passes
            Row::new(vec![
                SqlValue::Date(Date { year: 1994, month: 6, day: 10 }),
                SqlValue::Date(Date { year: 1994, month: 6, day: 20 }),
            ]),
            // Row 1: NULL < receiptdate -> fails (NULL comparison)
            Row::new(vec![SqlValue::Null, SqlValue::Date(Date { year: 1994, month: 6, day: 20 })]),
            // Row 2: commitdate < NULL -> fails (NULL comparison)
            Row::new(vec![SqlValue::Date(Date { year: 1994, month: 6, day: 10 }), SqlValue::Null]),
            // Row 3: commitdate < receiptdate -> passes
            Row::new(vec![
                SqlValue::Date(Date { year: 1995, month: 1, day: 5 }),
                SqlValue::Date(Date { year: 1995, month: 3, day: 10 }),
            ]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::LessThan,
            right_column_idx: 1,
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match rows 0 and 3 (NULL rows fail)
        assert_eq!(filtered.row_count(), 2);
    }

    /// Test different comparison operators for column-to-column
    #[test]
    fn test_simd_filter_column_compare_operators() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(5), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(15), SqlValue::Integer(10)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Test Equal
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::Equal,
            right_column_idx: 1,
        }];
        let filtered = simd_filter_batch(&batch, &predicates).unwrap();
        assert_eq!(filtered.row_count(), 1); // Only row 1

        // Test GreaterThan
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::GreaterThan,
            right_column_idx: 1,
        }];
        let filtered = simd_filter_batch(&batch, &predicates).unwrap();
        assert_eq!(filtered.row_count(), 1); // Only row 2

        // Test GreaterThanOrEqual
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::GreaterThanOrEqual,
            right_column_idx: 1,
        }];
        let filtered = simd_filter_batch(&batch, &predicates).unwrap();
        assert_eq!(filtered.row_count(), 2); // Rows 1 and 2

        // Test LessThanOrEqual
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::LessThanOrEqual,
            right_column_idx: 1,
        }];
        let filtered = simd_filter_batch(&batch, &predicates).unwrap();
        assert_eq!(filtered.row_count(), 2); // Rows 0 and 1

        // Test NotEqual
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::NotEqual,
            right_column_idx: 1,
        }];
        let filtered = simd_filter_batch(&batch, &predicates).unwrap();
        assert_eq!(filtered.row_count(), 2); // Rows 0 and 2
    }

    // ── IS NULL / IS NOT NULL vectorized mask tests ─────────────────────────

    /// Scalar reference: evaluate a null test row-by-row via `get_value`.
    fn scalar_null_mask(batch: &ColumnarBatch, column_idx: usize, want_null: bool) -> Vec<bool> {
        (0..batch.row_count())
            .map(|row_idx| {
                let is_null = batch.get_value(row_idx, column_idx).unwrap() == SqlValue::Null;
                is_null == want_null
            })
            .collect()
    }

    fn assert_null_masks_match_scalar(batch: &ColumnarBatch, column_idx: usize) {
        let is_null_pred = ColumnPredicate::IsNull { column_idx };
        let is_not_null_pred = ColumnPredicate::IsNotNull { column_idx };

        // Vectorized (non-packed), packed, and auto paths must all agree with
        // the scalar reference over the null bitmap.
        let vec_is_null = simd_create_filter_mask(batch, &[is_null_pred.clone()]).unwrap();
        let vec_is_not_null = simd_create_filter_mask(batch, &[is_not_null_pred.clone()]).unwrap();
        assert_eq!(vec_is_null, scalar_null_mask(batch, column_idx, true));
        assert_eq!(vec_is_not_null, scalar_null_mask(batch, column_idx, false));

        let packed_is_null =
            simd_create_filter_mask_packed(batch, &[is_null_pred]).unwrap().to_bool_vec();
        let packed_is_not_null =
            simd_create_filter_mask_packed(batch, &[is_not_null_pred]).unwrap().to_bool_vec();
        assert_eq!(&packed_is_null[..batch.row_count()], &vec_is_null[..]);
        assert_eq!(&packed_is_not_null[..batch.row_count()], &vec_is_not_null[..]);

        // IS NULL and IS NOT NULL must be exact complements.
        for (a, b) in vec_is_null.iter().zip(vec_is_not_null.iter()) {
            assert_ne!(a, b);
        }
    }

    #[test]
    fn test_is_null_sparse_bitmap() {
        // First row concrete so the column infers Int64 (not Mixed), giving a
        // dense/sparse null bitmap with a mix of null and non-null rows.
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Integer(3)]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Integer(5)]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let is_null =
            simd_create_filter_mask(&batch, &[ColumnPredicate::IsNull { column_idx: 0 }]).unwrap();
        assert_eq!(is_null, vec![false, true, false, true, false]);
        assert_null_masks_match_scalar(&batch, 0);
    }

    #[test]
    fn test_is_null_absent_bitmap() {
        // No NULLs at all: from_rows produces a None null bitmap. IS NULL must be
        // all-false and IS NOT NULL all-true, computed without touching values.
        let rows: Vec<Row> = (0..8).map(|i| Row::new(vec![SqlValue::Integer(i)])).collect();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();
        assert!(batch.column(0).unwrap().null_bitmap().is_none());

        let is_null =
            simd_create_filter_mask(&batch, &[ColumnPredicate::IsNull { column_idx: 0 }]).unwrap();
        let is_not_null =
            simd_create_filter_mask(&batch, &[ColumnPredicate::IsNotNull { column_idx: 0 }])
                .unwrap();
        assert_eq!(is_null, vec![false; 8]);
        assert_eq!(is_not_null, vec![true; 8]);
        assert_null_masks_match_scalar(&batch, 0);
    }

    #[test]
    fn test_is_null_all_null_bitmap() {
        // Dense (all-true) bitmap: first row concrete then all NULL forces a
        // bitmap; use a wider column so column 0 stays Int64.
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Null]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();
        let is_null =
            simd_create_filter_mask(&batch, &[ColumnPredicate::IsNull { column_idx: 0 }]).unwrap();
        assert_eq!(is_null, vec![false, true, true]);
        assert_null_masks_match_scalar(&batch, 0);
    }

    #[test]
    fn test_is_null_string_column() {
        let rows = vec![
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("a"))]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("c"))]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();
        assert_null_masks_match_scalar(&batch, 0);
    }

    #[test]
    fn test_is_null_mixed_column() {
        // Heterogeneous values force a Mixed column (no null bitmap). The mask
        // must still be correct via the per-row get_value path.
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("two"))]),
            Row::new(vec![SqlValue::Null]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();
        assert!(batch.column(0).unwrap().is_mixed());
        let is_null =
            simd_create_filter_mask(&batch, &[ColumnPredicate::IsNull { column_idx: 0 }]).unwrap();
        assert_eq!(is_null, vec![false, false, true]);
        assert_null_masks_match_scalar(&batch, 0);
    }

    #[test]
    fn test_is_null_compound_and_with_value_compare() {
        // col0 IS NOT NULL AND col1 > 10 — the null-test mask must AND with the
        // value-comparison mask (which already treats null as non-matching).
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(20)]), // notnull & 20>10 -> T
            Row::new(vec![SqlValue::Null, SqlValue::Integer(30)]),       // null      -> F
            Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(5)]),  // notnull & 5>10  -> F
            Row::new(vec![SqlValue::Integer(4), SqlValue::Null]),        // col1 null -> F
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();
        let predicates = vec![
            ColumnPredicate::IsNotNull { column_idx: 0 },
            ColumnPredicate::GreaterThan { column_idx: 1, value: SqlValue::Integer(10) },
        ];
        let mask = simd_create_filter_mask(&batch, &predicates).unwrap();
        assert_eq!(mask, vec![true, false, false, false]);
    }

    #[test]
    fn test_is_null_full_filter_roundtrip() {
        // simd_filter_batch (packed auto path) must materialize exactly the
        // NULL rows for IS NULL and the non-NULL rows for IS NOT NULL.
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Integer(3)]),
            Row::new(vec![SqlValue::Null]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let only_null =
            simd_filter_batch(&batch, &[ColumnPredicate::IsNull { column_idx: 0 }]).unwrap();
        assert_eq!(only_null.row_count(), 2);
        assert_eq!(only_null.get_value(0, 0).unwrap(), SqlValue::Null);
        assert_eq!(only_null.get_value(1, 0).unwrap(), SqlValue::Null);

        let only_non_null =
            simd_filter_batch(&batch, &[ColumnPredicate::IsNotNull { column_idx: 0 }]).unwrap();
        assert_eq!(only_non_null.row_count(), 2);
        assert_eq!(only_non_null.get_value(0, 0).unwrap(), SqlValue::Integer(1));
        assert_eq!(only_non_null.get_value(1, 0).unwrap(), SqlValue::Integer(3));
    }
}
