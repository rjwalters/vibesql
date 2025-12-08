//! Probe phase implementations for columnar hash joins
//!
//! This module provides probe functions for different join types:
//! - Inner join probe
//! - Left outer join probe
//! - Right outer join probe

use crate::errors::ExecutorError;
use crate::select::columnar::ColumnArray;

use super::hash_table::ColumnarHashTable;
use super::output::{JoinIndices, LeftOuterJoinIndices, RightOuterJoinIndices};

/// Check if a value is NULL according to the null bitmap
#[inline]
pub(crate) fn is_null(nulls: &Option<std::sync::Arc<Vec<bool>>>, idx: usize) -> bool {
    nulls.as_ref().is_some_and(|n| n.get(idx).copied().unwrap_or(false))
}

/// Probe phase: find all matching pairs for inner join
///
/// NULL handling: NULL keys never match in equi-joins (NULL = NULL is NULL, not true).
/// Both left and right NULL keys are skipped during probe.
///
/// # Bloom Filter Optimization
///
/// This function uses the Bloom filter (if available) to quickly reject probe keys
/// that cannot possibly match any build-side keys, avoiding expensive hash table lookups.
pub(crate) fn probe_columnar(
    hash_table: &ColumnarHashTable,
    left_key: &ColumnArray,
    right_key: &ColumnArray,
) -> Result<JoinIndices, ExecutorError> {
    let mut left_indices = Vec::new();
    let mut right_indices = Vec::new();

    match (left_key, right_key) {
        (
            ColumnArray::Int64(left_values, left_nulls),
            ColumnArray::Int64(right_values, right_nulls),
        ) => {
            for (left_idx, &key) in left_values.iter().enumerate() {
                // Skip NULL left keys - NULLs never match in equi-joins
                if is_null(left_nulls, left_idx) {
                    continue;
                }

                // BLOOM FILTER OPTIMIZATION: Quick rejection of non-matching keys
                if !hash_table.bloom_might_contain_i64(key) {
                    continue; // Definitely no match
                }

                for right_idx in hash_table.probe_i64(key, right_values) {
                    // Skip NULL right keys
                    if is_null(right_nulls, right_idx as usize) {
                        continue;
                    }
                    left_indices.push(left_idx as u32);
                    right_indices.push(right_idx);
                }
            }
        }
        (
            ColumnArray::String(left_values, left_nulls),
            ColumnArray::String(right_values, right_nulls),
        ) => {
            for (left_idx, key) in left_values.iter().enumerate() {
                // Skip NULL left keys - NULLs never match in equi-joins
                if is_null(left_nulls, left_idx) {
                    continue;
                }

                // BLOOM FILTER OPTIMIZATION: Quick rejection of non-matching keys
                if !hash_table.bloom_might_contain_string(key) {
                    continue; // Definitely no match
                }

                for right_idx in hash_table.probe_string(key, right_values) {
                    // Skip NULL right keys
                    if is_null(right_nulls, right_idx as usize) {
                        continue;
                    }
                    left_indices.push(left_idx as u32);
                    right_indices.push(right_idx);
                }
            }
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "Columnar hash join probe not supported for this column type combination"
                    .to_string(),
            ));
        }
    }

    Ok(JoinIndices { left_indices, right_indices })
}

/// Probe phase for LEFT OUTER join: find matches and preserve unmatched left rows
///
/// # Bloom Filter Optimization
///
/// Uses the Bloom filter to quickly reject non-matching keys. Unlike inner join,
/// left outer join preserves unmatched rows, so Bloom filter rejection doesn't
/// affect correctness - it just avoids unnecessary hash table lookups.
pub(crate) fn probe_columnar_left_outer(
    hash_table: &ColumnarHashTable,
    left_key: &ColumnArray,
    right_key: &ColumnArray,
    left_row_count: usize,
) -> Result<LeftOuterJoinIndices, ExecutorError> {
    let mut left_indices = Vec::new();
    let mut right_indices = Vec::new();
    let mut left_matched = vec![false; left_row_count];

    match (left_key, right_key) {
        (ColumnArray::Int64(left_values, left_nulls), ColumnArray::Int64(right_values, _)) => {
            for (left_idx, &key) in left_values.iter().enumerate() {
                // Skip NULL keys - they never match but still output with NULLs
                let is_null = left_nulls.as_ref().map(|n| n[left_idx]).unwrap_or(false);
                if is_null {
                    continue; // Will be handled as unmatched
                }

                // BLOOM FILTER OPTIMIZATION: Quick rejection of non-matching keys
                if !hash_table.bloom_might_contain_i64(key) {
                    continue; // Definitely no match, will be output as unmatched
                }

                let mut found_match = false;
                for right_idx in hash_table.probe_i64(key, right_values) {
                    left_indices.push(left_idx as u32);
                    right_indices.push(right_idx);
                    found_match = true;
                }
                if found_match {
                    left_matched[left_idx] = true;
                }
            }
        }
        (ColumnArray::String(left_values, left_nulls), ColumnArray::String(right_values, _)) => {
            for (left_idx, key) in left_values.iter().enumerate() {
                let is_null = left_nulls.as_ref().map(|n| n[left_idx]).unwrap_or(false);
                if is_null {
                    continue;
                }

                // BLOOM FILTER OPTIMIZATION: Quick rejection of non-matching keys
                if !hash_table.bloom_might_contain_string(key) {
                    continue; // Definitely no match, will be output as unmatched
                }

                let mut found_match = false;
                for right_idx in hash_table.probe_string(key, right_values) {
                    left_indices.push(left_idx as u32);
                    right_indices.push(right_idx);
                    found_match = true;
                }
                if found_match {
                    left_matched[left_idx] = true;
                }
            }
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "Columnar LEFT OUTER hash join probe not supported for this column type combination".to_string()
            ));
        }
    }

    // Add unmatched left rows with NULL marker for right side
    for (left_idx, &matched) in left_matched.iter().enumerate() {
        if !matched {
            left_indices.push(left_idx as u32);
            right_indices.push(u32::MAX); // NULL marker
        }
    }

    Ok(LeftOuterJoinIndices { left_indices, right_indices })
}

/// Probe phase for RIGHT OUTER join: find matches and preserve unmatched right rows
///
/// # Bloom Filter Optimization
///
/// Uses the Bloom filter to quickly reject non-matching keys. Unlike inner join,
/// right outer join preserves unmatched rows, so Bloom filter rejection doesn't
/// affect correctness - it just avoids unnecessary hash table lookups.
pub(crate) fn probe_columnar_right_outer(
    hash_table: &ColumnarHashTable,
    right_key: &ColumnArray,
    left_key: &ColumnArray,
    right_row_count: usize,
) -> Result<RightOuterJoinIndices, ExecutorError> {
    let mut left_indices = Vec::new();
    let mut right_indices = Vec::new();
    let mut right_matched = vec![false; right_row_count];

    match (right_key, left_key) {
        (ColumnArray::Int64(right_values, right_nulls), ColumnArray::Int64(left_values, _)) => {
            for (right_idx, &key) in right_values.iter().enumerate() {
                let is_null = right_nulls.as_ref().map(|n| n[right_idx]).unwrap_or(false);
                if is_null {
                    continue;
                }

                // BLOOM FILTER OPTIMIZATION: Quick rejection of non-matching keys
                if !hash_table.bloom_might_contain_i64(key) {
                    continue; // Definitely no match, will be output as unmatched
                }

                let mut found_match = false;
                for left_idx in hash_table.probe_i64(key, left_values) {
                    left_indices.push(left_idx);
                    right_indices.push(right_idx as u32);
                    found_match = true;
                }
                if found_match {
                    right_matched[right_idx] = true;
                }
            }
        }
        (ColumnArray::String(right_values, right_nulls), ColumnArray::String(left_values, _)) => {
            for (right_idx, key) in right_values.iter().enumerate() {
                let is_null = right_nulls.as_ref().map(|n| n[right_idx]).unwrap_or(false);
                if is_null {
                    continue;
                }

                // BLOOM FILTER OPTIMIZATION: Quick rejection of non-matching keys
                if !hash_table.bloom_might_contain_string(key) {
                    continue; // Definitely no match, will be output as unmatched
                }

                let mut found_match = false;
                for left_idx in hash_table.probe_string(key, left_values) {
                    left_indices.push(left_idx);
                    right_indices.push(right_idx as u32);
                    found_match = true;
                }
                if found_match {
                    right_matched[right_idx] = true;
                }
            }
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "Columnar RIGHT OUTER hash join probe not supported for this column type combination".to_string()
            ));
        }
    }

    // Add unmatched right rows with NULL marker for left side
    for (right_idx, &matched) in right_matched.iter().enumerate() {
        if !matched {
            left_indices.push(u32::MAX); // NULL marker
            right_indices.push(right_idx as u32);
        }
    }

    Ok(RightOuterJoinIndices { left_indices, right_indices })
}
