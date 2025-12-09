// ============================================================================
// Unit Tests for Prefix Match Operations
// ============================================================================

use std::collections::BTreeMap;
use vibesql_types::SqlValue;

use crate::database::indexes::index_metadata::IndexData;
use crate::database::indexes::value_normalization::normalize_for_comparison;

/// Helper to create an InMemory IndexData with test data
/// Note: Keys are normalized to match how real indexes store data
fn create_test_index_data(entries: Vec<(Vec<SqlValue>, Vec<usize>)>) -> IndexData {
    let mut data = BTreeMap::new();
    for (key, row_indices) in entries {
        // Normalize keys like real index insertion does
        let normalized_key: Vec<SqlValue> = key.iter().map(normalize_for_comparison).collect();
        data.insert(normalized_key, row_indices);
    }
    IndexData::InMemory { data, pending_deletions: Vec::new() }
}

// ========================================================================
// prefix_scan() Tests - InMemory
// ========================================================================

#[test]
fn test_prefix_scan_single_column_match() {
    // Index on (a, b) - look for rows where a=1
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(30)], vec![2]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(10)], vec![3]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(20)], vec![4]),
    ]);

    // Prefix [1] should match rows 0, 1, 2
    let results = index.prefix_scan(&[SqlValue::Integer(1)]);
    assert_eq!(results, vec![0, 1, 2]);
}

#[test]
fn test_prefix_scan_two_column_prefix() {
    // Index on (a, b, c) - look for rows where a=1 AND b=5
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(100)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(200)], vec![1]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(300)], vec![2]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(6), SqlValue::Integer(100)], vec![3]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(5), SqlValue::Integer(100)], vec![4]),
    ]);

    // Prefix [1, 5] should match rows 0, 1, 2
    let results = index.prefix_scan(&[SqlValue::Integer(1), SqlValue::Integer(5)]);
    assert_eq!(results, vec![0, 1, 2]);
}

#[test]
fn test_prefix_scan_exact_match() {
    // When prefix length equals key length, it's an exact match
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
    ]);

    // Exact match [1, 10]
    let results = index.prefix_scan(&[SqlValue::Integer(1), SqlValue::Integer(10)]);
    assert_eq!(results, vec![0]);
}

#[test]
fn test_prefix_scan_no_match() {
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(20)], vec![1]),
    ]);

    // No rows where a=3
    let results = index.prefix_scan(&[SqlValue::Integer(3)]);
    assert!(results.is_empty());
}

#[test]
fn test_prefix_scan_single_row() {
    let index = create_test_index_data(vec![(
        vec![SqlValue::Integer(1), SqlValue::Integer(10)],
        vec![0],
    )]);

    let results = index.prefix_scan(&[SqlValue::Integer(1)]);
    assert_eq!(results, vec![0]);
}

#[test]
fn test_prefix_scan_multiple_rows_per_key() {
    // Non-unique index: multiple row indices per key
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0, 5, 10]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1, 6]),
    ]);

    let results = index.prefix_scan(&[SqlValue::Integer(1)]);
    assert_eq!(results, vec![0, 5, 10, 1, 6]);
}

// ========================================================================
// Edge Cases
// ========================================================================

#[test]
fn test_prefix_scan_empty_prefix() {
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(20)], vec![1]),
    ]);

    // Empty prefix matches everything - returns all rows
    let results = index.prefix_scan(&[]);
    assert_eq!(results.len(), 2);
    assert!(results.contains(&0));
    assert!(results.contains(&1));
}

#[test]
fn test_prefix_scan_prefix_longer_than_key() {
    // Index has 2-column keys, but we search with 3-column prefix
    let index = create_test_index_data(vec![(
        vec![SqlValue::Integer(1), SqlValue::Integer(10)],
        vec![0],
    )]);

    // Prefix longer than key cannot match
    let results = index.prefix_scan(&[
        SqlValue::Integer(1),
        SqlValue::Integer(10),
        SqlValue::Integer(100),
    ]);
    assert!(results.is_empty());
}

#[test]
fn test_prefix_scan_empty_index() {
    let index = create_test_index_data(vec![]);

    let results = index.prefix_scan(&[SqlValue::Integer(1)]);
    assert!(results.is_empty());
}

#[test]
fn test_prefix_scan_with_string_keys() {
    let index = create_test_index_data(vec![
        (vec![SqlValue::Varchar(arcstr::ArcStr::from("a")), SqlValue::Integer(1)], vec![0]),
        (vec![SqlValue::Varchar(arcstr::ArcStr::from("a")), SqlValue::Integer(2)], vec![1]),
        (vec![SqlValue::Varchar(arcstr::ArcStr::from("b")), SqlValue::Integer(1)], vec![2]),
    ]);

    let results = index.prefix_scan(&[SqlValue::Varchar(arcstr::ArcStr::from("a"))]);
    assert_eq!(results, vec![0, 1]);
}

#[test]
fn test_prefix_scan_with_mixed_types() {
    // Multi-column index with different types
    let index = create_test_index_data(vec![
        (
            vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("x")),
                SqlValue::Boolean(true),
            ],
            vec![0],
        ),
        (
            vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("x")),
                SqlValue::Boolean(false),
            ],
            vec![1],
        ),
        (
            vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(arcstr::ArcStr::from("y")),
                SqlValue::Boolean(true),
            ],
            vec![2],
        ),
    ]);

    // Match on [1, "x"] - order depends on BTreeMap key ordering (false < true)
    let results =
        index.prefix_scan(&[SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("x"))]);
    assert_eq!(results.len(), 2);
    assert!(results.contains(&0));
    assert!(results.contains(&1));
}

#[test]
fn test_prefix_scan_numeric_type_normalization() {
    // Test that different numeric types are normalized correctly
    // Index uses Integer, but we search with a different numeric type
    let index = create_test_index_data(vec![
        (vec![SqlValue::Double(1.0), SqlValue::Double(10.0)], vec![0]),
        (vec![SqlValue::Double(1.0), SqlValue::Double(20.0)], vec![1]),
        (vec![SqlValue::Double(2.0), SqlValue::Double(10.0)], vec![2]),
    ]);

    // Search with Integer(1) should match Double(1.0) after normalization
    let results = index.prefix_scan(&[SqlValue::Integer(1)]);
    assert_eq!(results, vec![0, 1]);
}

// ========================================================================
// prefix_scan_batch() Tests
// ========================================================================

#[test]
fn test_prefix_scan_batch_basic() {
    // Index on (w_id, d_id, o_id) - like TPC-C NEW_ORDER table
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(101)], vec![1]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(100)], vec![2]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(101)], vec![3]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(3), SqlValue::Integer(100)], vec![4]),
    ]);

    // Batch lookup for districts 1 and 2
    let prefixes = vec![
        vec![SqlValue::Integer(1), SqlValue::Integer(1)],
        vec![SqlValue::Integer(1), SqlValue::Integer(2)],
    ];

    let results = index.prefix_scan_batch(&prefixes);

    // Should have 2 results (one for each prefix that has matches)
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (0, vec![0, 1])); // prefix 0 matches rows 0, 1
    assert_eq!(results[1], (1, vec![2, 3])); // prefix 1 matches rows 2, 3
}

#[test]
fn test_prefix_scan_batch_some_empty() {
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(1)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(3)], vec![2]),
    ]);

    // Batch lookup - prefix at index 1 has no matches
    let prefixes = vec![
        vec![SqlValue::Integer(1), SqlValue::Integer(1)],
        vec![SqlValue::Integer(1), SqlValue::Integer(2)], // No match
        vec![SqlValue::Integer(1), SqlValue::Integer(3)],
    ];

    let results = index.prefix_scan_batch(&prefixes);

    // Only prefixes 0 and 2 have matches
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (0, vec![0]));
    assert_eq!(results[1], (2, vec![2]));
}

#[test]
fn test_prefix_scan_batch_all_empty() {
    let index = create_test_index_data(vec![(
        vec![SqlValue::Integer(1), SqlValue::Integer(1)],
        vec![0],
    )]);

    let prefixes = vec![
        vec![SqlValue::Integer(2), SqlValue::Integer(1)],
        vec![SqlValue::Integer(3), SqlValue::Integer(1)],
    ];

    let results = index.prefix_scan_batch(&prefixes);
    assert!(results.is_empty());
}

#[test]
fn test_prefix_scan_batch_empty_input() {
    let index = create_test_index_data(vec![(
        vec![SqlValue::Integer(1), SqlValue::Integer(1)],
        vec![0],
    )]);

    let results = index.prefix_scan_batch(&[]);
    assert!(results.is_empty());
}

#[test]
fn test_prefix_scan_batch_tpcc_like() {
    // Simulate TPC-C Delivery transaction: lookup all districts for a warehouse
    // Index: (NO_W_ID, NO_D_ID, NO_O_ID)
    let mut entries = Vec::new();
    let w_id = 1;

    // Create data for 10 districts, each with varying number of new orders
    for d_id in 1..=10 {
        for o_id in 1..=(d_id * 2) {
            // District 1 has 2 orders, district 2 has 4, etc.
            let key =
                vec![SqlValue::Integer(w_id), SqlValue::Integer(d_id), SqlValue::Integer(o_id)];
            entries.push((key, vec![((d_id - 1) * 10 + o_id - 1) as usize]));
        }
    }

    let index = create_test_index_data(entries);

    // Batch prefix lookup for all 10 districts
    let prefixes: Vec<Vec<SqlValue>> =
        (1..=10).map(|d| vec![SqlValue::Integer(w_id), SqlValue::Integer(d)]).collect();

    let results = index.prefix_scan_batch(&prefixes);

    // All 10 districts should have matches
    assert_eq!(results.len(), 10);

    // Verify each district has the expected number of rows
    for (idx, rows) in &results {
        let d_id = *idx as i64 + 1;
        let expected_count = (d_id * 2) as usize;
        assert_eq!(
            rows.len(),
            expected_count,
            "District {} should have {} orders",
            d_id,
            expected_count
        );
    }
}

// ========================================================================
// prefix_multi_lookup() Tests
// ========================================================================

#[test]
fn test_prefix_multi_lookup_basic() {
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(10)], vec![2]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(20)], vec![3]),
        (vec![SqlValue::Integer(3), SqlValue::Integer(10)], vec![4]),
    ]);

    // Look up a=1 OR a=2
    let results = index.prefix_multi_lookup(&[SqlValue::Integer(1), SqlValue::Integer(2)]);

    // Should find rows 0, 1 (a=1) and 2, 3 (a=2)
    assert_eq!(results.len(), 4);
    assert!(results.contains(&0));
    assert!(results.contains(&1));
    assert!(results.contains(&2));
    assert!(results.contains(&3));
}

#[test]
fn test_prefix_multi_lookup_with_duplicates() {
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(20)], vec![1]),
    ]);

    // Duplicates in input should be deduplicated
    let results = index.prefix_multi_lookup(&[
        SqlValue::Integer(1),
        SqlValue::Integer(1),
        SqlValue::Integer(2),
    ]);

    assert_eq!(results.len(), 2);
    assert!(results.contains(&0));
    assert!(results.contains(&1));
}

// ========================================================================
// prefix_scan_first() Tests - InMemory
// ========================================================================

#[test]
fn test_prefix_scan_first_basic() {
    // Index on (w_id, d_id, o_id) - TPC-C style
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(200)], vec![1]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(300)], vec![2]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(100)], vec![3]),
    ]);

    // Prefix [1, 1] should return first matching row (row 0)
    let result = index.prefix_scan_first(&[SqlValue::Integer(1), SqlValue::Integer(1)]);
    assert_eq!(result, Some(0));
}

#[test]
fn test_prefix_scan_first_no_match() {
    let index = create_test_index_data(vec![(
        vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)],
        vec![0],
    )]);

    // Prefix [2, 1] should return None (no match)
    let result = index.prefix_scan_first(&[SqlValue::Integer(2), SqlValue::Integer(1)]);
    assert_eq!(result, None);
}

#[test]
fn test_prefix_scan_first_empty_prefix() {
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1)], vec![0]),
        (vec![SqlValue::Integer(2)], vec![1]),
    ]);

    // Empty prefix should return first row in index
    let result = index.prefix_scan_first(&[]);
    assert_eq!(result, Some(0));
}

#[test]
fn test_prefix_scan_first_returns_minimum_third_column() {
    // Index on (w_id, d_id, o_id)
    // This tests the TPC-C Delivery query pattern:
    // SELECT no_o_id FROM new_order WHERE no_w_id = 1 AND no_d_id = 5 ORDER BY no_o_id LIMIT 1
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(300)], vec![3]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(100)], vec![1]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(5), SqlValue::Integer(200)], vec![2]),
    ]);

    // Prefix [1, 5] should return row with minimum o_id (100) = row 1
    // BTreeMap stores in sorted order, so [1, 5, 100] comes first
    let result = index.prefix_scan_first(&[SqlValue::Integer(1), SqlValue::Integer(5)]);
    assert_eq!(result, Some(1)); // Row index for o_id=100
}

// ========================================================================
// prefix_scan_limit() Tests - LIMIT pushdown optimization (#3253)
// ========================================================================

#[test]
fn test_prefix_scan_limit_forward_with_limit() {
    // Index on (w_id, d_id, o_id) - find first 2 orders for a customer
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(101)], vec![1]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(102)], vec![2]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(103)], vec![3]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(100)], vec![4]),
    ]);

    // LIMIT 2 in forward order (ASC)
    let results = index.prefix_scan_limit(
        &[SqlValue::Integer(1), SqlValue::Integer(1)],
        Some(2),
        false, // ASC
    );
    assert_eq!(results, vec![0, 1]); // First 2 orders
}

#[test]
fn test_prefix_scan_limit_reverse_with_limit() {
    // Index on (w_id, d_id, o_id) - find most recent order (DESC LIMIT 1)
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(101)], vec![1]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(102)], vec![2]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(103)], vec![3]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(100)], vec![4]),
    ]);

    // LIMIT 1 in reverse order (DESC) - most recent order
    let results = index.prefix_scan_limit(
        &[SqlValue::Integer(1), SqlValue::Integer(1)],
        Some(1),
        true, // DESC
    );
    assert_eq!(results, vec![3]); // Last order (o_id=103)
}

#[test]
fn test_prefix_scan_limit_reverse_all() {
    // Get all rows in reverse order (no limit)
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(30)], vec![2]),
    ]);

    // No limit, reverse order
    let results = index.prefix_scan_limit(
        &[SqlValue::Integer(1)],
        None,
        true, // DESC
    );
    assert_eq!(results, vec![2, 1, 0]); // Reverse order
}

#[test]
fn test_prefix_scan_limit_no_limit_no_reverse() {
    // Falls back to regular prefix_scan
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(10)], vec![2]),
    ]);

    let results = index.prefix_scan_limit(&[SqlValue::Integer(1)], None, false);
    assert_eq!(results, vec![0, 1]);
}

#[test]
fn test_prefix_scan_limit_tpcc_order_status() {
    // Simulate TPC-C Order-Status: Find most recent order for a customer
    // Customer has 30 orders, we need just the last one (ORDER BY o_id DESC LIMIT 1)
    let mut entries = Vec::new();
    let w_id = 1;
    let d_id = 5;
    let c_id = 42;

    // Create 30 orders for the customer
    for o_id in 1..=30 {
        let key = vec![
            SqlValue::Integer(w_id),
            SqlValue::Integer(d_id),
            SqlValue::Integer(c_id),
            SqlValue::Integer(o_id),
        ];
        entries.push((key, vec![o_id as usize - 1]));
    }

    let index = create_test_index_data(entries);

    // Most recent order (ORDER BY o_id DESC LIMIT 1)
    let results = index.prefix_scan_limit(
        &[SqlValue::Integer(w_id), SqlValue::Integer(d_id), SqlValue::Integer(c_id)],
        Some(1),
        true, // DESC
    );

    // Should get the last order (o_id=30, row_idx=29)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], 29);
}

#[test]
fn test_prefix_scan_limit_empty_result() {
    let index = create_test_index_data(vec![(
        vec![SqlValue::Integer(1), SqlValue::Integer(10)],
        vec![0],
    )]);

    // No match
    let results = index.prefix_scan_limit(&[SqlValue::Integer(2)], Some(5), true);
    assert!(results.is_empty());
}

#[test]
fn test_prefix_scan_limit_less_than_limit() {
    // When there are fewer matching rows than the limit
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![1]),
    ]);

    // LIMIT 10 but only 2 rows match
    let results = index.prefix_scan_limit(&[SqlValue::Integer(1)], Some(10), false);
    assert_eq!(results, vec![0, 1]); // All matching rows
}

// ========================================================================
// Skip-Scan Tests
// ========================================================================

#[test]
fn test_get_distinct_first_column_values() {
    // Index on (region, date) with 3 regions
    let index = create_test_index_data(vec![
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("East")),
                SqlValue::Integer(20240101),
            ],
            vec![0],
        ),
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("East")),
                SqlValue::Integer(20240102),
            ],
            vec![1],
        ),
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("West")),
                SqlValue::Integer(20240101),
            ],
            vec![2],
        ),
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("West")),
                SqlValue::Integer(20240103),
            ],
            vec![3],
        ),
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("North")),
                SqlValue::Integer(20240102),
            ],
            vec![4],
        ),
    ]);

    let distinct = index.get_distinct_first_column_values();

    // Should have 3 distinct regions (East, North, West in sorted order)
    assert_eq!(distinct.len(), 3);
    // BTreeMap is sorted, so order depends on SqlValue ordering
}

#[test]
fn test_skip_scan_equality_basic() {
    // Index on (region, date) - query: WHERE date = 20240101
    let index = create_test_index_data(vec![
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("East")),
                SqlValue::Integer(20240101),
            ],
            vec![0],
        ),
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("East")),
                SqlValue::Integer(20240102),
            ],
            vec![1],
        ),
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("West")),
                SqlValue::Integer(20240101),
            ],
            vec![2],
        ),
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("West")),
                SqlValue::Integer(20240103),
            ],
            vec![3],
        ),
    ]);

    // Skip-scan for date = 20240101 (column index 1)
    let results = index.skip_scan_equality(1, &SqlValue::Integer(20240101));

    // Should find rows 0 and 2 (East and West with date 20240101)
    assert_eq!(results.len(), 2);
    assert!(results.contains(&0));
    assert!(results.contains(&2));
}

#[test]
fn test_skip_scan_equality_no_match() {
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(20)], vec![1]),
    ]);

    // Skip-scan for second column = 99 (no match)
    let results = index.skip_scan_equality(1, &SqlValue::Integer(99));
    assert!(results.is_empty());
}

#[test]
fn test_skip_scan_range_basic() {
    // Index on (category, price) - query: WHERE price BETWEEN 10 AND 20
    let index = create_test_index_data(vec![
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("Electronics")),
                SqlValue::Integer(5),
            ],
            vec![0],
        ),
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("Electronics")),
                SqlValue::Integer(15),
            ],
            vec![1],
        ),
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("Books")),
                SqlValue::Integer(8),
            ],
            vec![2],
        ),
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("Books")),
                SqlValue::Integer(12),
            ],
            vec![3],
        ),
        (
            vec![
                SqlValue::Varchar(arcstr::ArcStr::from("Books")),
                SqlValue::Integer(25),
            ],
            vec![4],
        ),
    ]);

    // Skip-scan for price BETWEEN 10 AND 20 (column index 1)
    let results = index.skip_scan_range(
        1,
        Some(&SqlValue::Integer(10)),
        true,  // inclusive lower
        Some(&SqlValue::Integer(20)),
        true, // inclusive upper
    );

    // Should find rows 1 (Electronics, 15) and 3 (Books, 12)
    assert_eq!(results.len(), 2);
    assert!(results.contains(&1));
    assert!(results.contains(&3));
}

#[test]
fn test_skip_scan_range_exclusive_bounds() {
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(1), SqlValue::Integer(10)], vec![0]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(15)], vec![1]),
        (vec![SqlValue::Integer(1), SqlValue::Integer(20)], vec![2]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(10)], vec![3]),
        (vec![SqlValue::Integer(2), SqlValue::Integer(15)], vec![4]),
    ]);

    // Skip-scan for second column > 10 AND < 20 (exclusive)
    let results = index.skip_scan_range(
        1,
        Some(&SqlValue::Integer(10)),
        false, // exclusive lower
        Some(&SqlValue::Integer(20)),
        false, // exclusive upper
    );

    // Should find rows 1 and 4 (both with value 15)
    assert_eq!(results.len(), 2);
    assert!(results.contains(&1));
    assert!(results.contains(&4));
}

#[test]
fn test_skip_scan_column_zero_falls_back() {
    // When filter_column_idx is 0, should use regular lookup
    let index = create_test_index_data(vec![
        (vec![SqlValue::Integer(10), SqlValue::Integer(1)], vec![0]),
        (vec![SqlValue::Integer(20), SqlValue::Integer(2)], vec![1]),
    ]);

    // Skip-scan on column 0 should fall back to regular lookup
    let results = index.skip_scan_equality(0, &SqlValue::Integer(10));
    assert_eq!(results.len(), 1);
    assert!(results.contains(&0));
}

#[test]
fn test_skip_scan_tpcc_like_scenario() {
    // Simulate TPC-C scenario: Index on (w_id, d_id, o_id)
    // Query: WHERE d_id = 5 (filtering on second column)
    let mut entries = Vec::new();
    let mut row_idx = 0;

    // Create data for 3 warehouses, 10 districts each
    for w_id in 1..=3 {
        for d_id in 1..=10 {
            for o_id in 1..=5 {
                let key = vec![
                    SqlValue::Integer(w_id),
                    SqlValue::Integer(d_id),
                    SqlValue::Integer(o_id),
                ];
                entries.push((key, vec![row_idx]));
                row_idx += 1;
            }
        }
    }

    let index = create_test_index_data(entries);

    // Skip-scan for d_id = 5 (column index 1)
    let results = index.skip_scan_equality(1, &SqlValue::Integer(5));

    // Should find 15 rows (3 warehouses * 5 orders each for district 5)
    assert_eq!(results.len(), 15);

    // Verify the pattern: rows for (1,5,*), (2,5,*), (3,5,*)
    // Each warehouse has orders 1-5 for district 5
    // Row indices: (w_id - 1) * 50 + (d_id - 1) * 5 + (o_id - 1)
    // For d_id=5: w=1: 20-24, w=2: 70-74, w=3: 120-124
    for w_id in 1..=3 {
        for o_id in 1..=5 {
            let expected_row = ((w_id - 1) * 50 + (5 - 1) * 5 + (o_id - 1)) as usize;
            assert!(
                results.contains(&expected_row),
                "Expected row {} for w_id={}, d_id=5, o_id={}",
                expected_row,
                w_id,
                o_id
            );
        }
    }
}
