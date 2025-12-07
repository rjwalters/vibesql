//! Row-to-column extraction utilities
//!
//! This module provides functions to extract typed column arrays from row-based
//! data, enabling columnar hash operations on row-based inputs.

use super::hash_table::{ColumnarHashTable, CompositeIntHashTable};

/// Extract a single column from rows as a typed array (for integer columns)
///
/// This enables using columnar hash operations on row-based data.
/// Returns None if the column contains non-integer values or NULLs.
pub fn extract_i64_column(rows: &[vibesql_storage::Row], col_idx: usize) -> Option<Vec<i64>> {
    let mut values = Vec::with_capacity(rows.len());

    for row in rows {
        match row.values.get(col_idx) {
            Some(vibesql_types::SqlValue::Integer(v)) => values.push(*v),
            Some(vibesql_types::SqlValue::Bigint(v)) => values.push(*v),
            Some(vibesql_types::SqlValue::Smallint(v)) => values.push(*v as i64),
            _ => return None, // Non-integer or NULL value
        }
    }

    Some(values)
}

/// Extract multiple integer columns from rows as typed arrays
///
/// Returns None if any column contains non-integer values or NULLs.
pub fn extract_multi_i64_columns(
    rows: &[vibesql_storage::Row],
    col_indices: &[usize],
) -> Option<Vec<Vec<i64>>> {
    let mut columns: Vec<Vec<i64>> =
        col_indices.iter().map(|_| Vec::with_capacity(rows.len())).collect();

    for row in rows {
        for (out_idx, &col_idx) in col_indices.iter().enumerate() {
            match row.values.get(col_idx) {
                Some(vibesql_types::SqlValue::Integer(v)) => columns[out_idx].push(*v),
                Some(vibesql_types::SqlValue::Bigint(v)) => columns[out_idx].push(*v),
                Some(vibesql_types::SqlValue::Smallint(v)) => columns[out_idx].push(*v as i64),
                _ => return None, // Non-integer or NULL value
            }
        }
    }

    Some(columns)
}

/// Hash join using columnar hash table on row-based data
///
/// This function provides a fast path for integer equi-joins by:
/// 1. Extracting join columns as typed i64 arrays
/// 2. Using the columnar hash table for O(1) lookups without SqlValue dispatch
/// 3. Returning index pairs for row combination
///
/// Returns None if the join columns are not integer types.
pub fn hash_join_indices_columnar(
    build_rows: &[vibesql_storage::Row],
    probe_rows: &[vibesql_storage::Row],
    build_col_idx: usize,
    probe_col_idx: usize,
) -> Option<Vec<(usize, usize)>> {
    // Extract join columns as typed arrays
    let build_keys = extract_i64_column(build_rows, build_col_idx)?;
    let probe_keys = extract_i64_column(probe_rows, probe_col_idx)?;

    // Build hash table on build side
    let hash_table = ColumnarHashTable::build_from_i64(&build_keys);

    // Probe and collect matching index pairs
    let estimated_capacity = probe_keys.len().min(100_000);
    let mut join_pairs = Vec::with_capacity(estimated_capacity);

    for (probe_idx, &probe_key) in probe_keys.iter().enumerate() {
        for build_idx in hash_table.probe_i64(probe_key, &build_keys) {
            join_pairs.push((build_idx as usize, probe_idx));
        }
    }

    Some(join_pairs)
}

/// Multi-column hash join using columnar hash table on row-based data
///
/// This function provides a fast path for multi-column integer equi-joins by:
/// 1. Extracting join columns as typed i64 arrays (avoiding SqlValue enum dispatch)
/// 2. Using the composite columnar hash table with pre-computed hashes
/// 3. Returning index pairs for row combination
///
/// Returns None if any join columns are not integer types.
pub fn hash_join_indices_columnar_multi(
    build_rows: &[vibesql_storage::Row],
    probe_rows: &[vibesql_storage::Row],
    build_col_indices: &[usize],
    probe_col_indices: &[usize],
) -> Option<Vec<(usize, usize)>> {
    // Extract join columns as typed arrays
    let build_columns = extract_multi_i64_columns(build_rows, build_col_indices)?;
    let probe_columns = extract_multi_i64_columns(probe_rows, probe_col_indices)?;

    // Convert to slice references for the hash table
    let build_col_refs: Vec<&[i64]> = build_columns.iter().map(|c| c.as_slice()).collect();
    let probe_col_refs: Vec<&[i64]> = probe_columns.iter().map(|c| c.as_slice()).collect();

    // Build hash table on build side
    let hash_table = CompositeIntHashTable::build_from_multi_i64(&build_col_refs);

    // Probe and collect matching index pairs
    let estimated_capacity = probe_rows.len().min(100_000);
    let mut join_pairs = Vec::with_capacity(estimated_capacity);

    let num_cols = probe_col_indices.len();
    let mut probe_key = vec![0i64; num_cols];

    for probe_idx in 0..probe_rows.len() {
        // Extract probe keys for this row
        for (col_idx, col) in probe_col_refs.iter().enumerate() {
            probe_key[col_idx] = col[probe_idx];
        }

        for build_idx in hash_table.probe_multi_i64(&probe_key, &build_col_refs) {
            join_pairs.push((build_idx as usize, probe_idx));
        }
    }

    Some(join_pairs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_storage::Row;
    use vibesql_types::SqlValue;

    #[test]
    fn test_hash_join_indices_columnar_multi() {
        // Build rows: (a, b) pairs
        let build_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(30)]),
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]), // Duplicate key
        ];

        // Probe rows: (a, b) pairs
        let probe_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]), // Matches build[0] and build[3]
            Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20)]), // Matches build[1]
            Row::new(vec![SqlValue::Integer(99), SqlValue::Integer(99)]), // No match
        ];

        let pairs =
            hash_join_indices_columnar_multi(&build_rows, &probe_rows, &[0, 1], &[0, 1]).unwrap();

        // Should have 3 pairs: (0,0), (3,0), (1,1)
        assert_eq!(pairs.len(), 3);

        // Probe row 0 matches build rows 0 and 3
        let matches_for_probe_0: Vec<_> = pairs.iter().filter(|(_, p)| *p == 0).collect();
        assert_eq!(matches_for_probe_0.len(), 2);

        // Probe row 1 matches build row 1
        let matches_for_probe_1: Vec<_> = pairs.iter().filter(|(_, p)| *p == 1).collect();
        assert_eq!(matches_for_probe_1.len(), 1);
        assert_eq!(matches_for_probe_1[0].0, 1);

        // Probe row 2 has no matches
        let matches_for_probe_2: Vec<_> = pairs.iter().filter(|(_, p)| *p == 2).collect();
        assert_eq!(matches_for_probe_2.len(), 0);
    }

    #[test]
    fn test_hash_join_indices_columnar_multi_with_non_integer() {
        // Build rows with string column (should fall back to None)
        let build_rows =
            vec![Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("a")), SqlValue::Integer(10)])];

        let probe_rows =
            vec![Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("a")), SqlValue::Integer(10)])];

        // Should return None because not all columns are integers
        let result = hash_join_indices_columnar_multi(&build_rows, &probe_rows, &[0, 1], &[0, 1]);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_multi_i64_columns() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Bigint(100), SqlValue::Smallint(10)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Bigint(200), SqlValue::Smallint(20)]),
        ];

        // Extract columns 0 and 2
        let columns = extract_multi_i64_columns(&rows, &[0, 2]).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0], vec![1i64, 2]);
        assert_eq!(columns[1], vec![10i64, 20]);

        // Extract all columns
        let columns = extract_multi_i64_columns(&rows, &[0, 1, 2]).unwrap();
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0], vec![1i64, 2]);
        assert_eq!(columns[1], vec![100i64, 200]);
        assert_eq!(columns[2], vec![10i64, 20]);
    }
}
