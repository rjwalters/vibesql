use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use super::{
    build::*,
    inner::{hash_join_inner, hash_join_inner_multi},
    outer::hash_join_left_outer,
    FromResult,
};
use crate::schema::CombinedSchema;

/// Helper to create a simple FromResult for testing
fn create_test_from_result(
    table_name: &str,
    columns: Vec<(&str, DataType)>,
    rows: Vec<Vec<SqlValue>>,
) -> FromResult {
    let schema = TableSchema::new(
        table_name.to_string(),
        columns
            .iter()
            .map(|(name, dtype)| {
                ColumnSchema::new(
                    name.to_string(),
                    dtype.clone(),
                    true, // nullable
                )
            })
            .collect(),
    );

    let combined_schema = CombinedSchema::from_table(table_name.to_string(), schema);

    let rows = rows.into_iter().map(Row::new).collect();

    FromResult::from_rows(combined_schema, rows)
}

#[test]
fn test_hash_join_simple() {
    // Left table: users(id, name)
    let left = create_test_from_result(
        "users",
        vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
            vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))],
        ],
    );

    // Right table: orders(user_id, amount)
    let right = create_test_from_result(
        "orders",
        vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(100)],
            vec![SqlValue::Integer(2), SqlValue::Integer(200)],
            vec![SqlValue::Integer(1), SqlValue::Integer(150)],
        ],
    );

    // Join on users.id = orders.user_id (column 0 from both sides)
    let mut result = hash_join_inner(left, right, 0, 0).unwrap();

    // Should have 3 rows (user 1 has 2 orders, user 2 has 1 order, user 3 has no orders)
    assert_eq!(result.rows().len(), 3);

    // Verify combined rows have correct structure (4 columns: id, name, user_id, amount)
    for row in result.rows() {
        assert_eq!(row.values.len(), 4);
    }

    // Check specific matches
    // Alice (id=1) should appear twice (2 orders)
    let alice_orders: Vec<_> =
        result.rows().iter().filter(|r| r.values[0] == SqlValue::Integer(1)).collect();
    assert_eq!(alice_orders.len(), 2);

    // Bob (id=2) should appear once (1 order)
    let bob_orders: Vec<_> =
        result.rows().iter().filter(|r| r.values[0] == SqlValue::Integer(2)).collect();
    assert_eq!(bob_orders.len(), 1);

    // Charlie (id=3) should not appear (no orders)
    let charlie_orders: Vec<_> =
        result.rows().iter().filter(|r| r.values[0] == SqlValue::Integer(3)).collect();
    assert_eq!(charlie_orders.len(), 0);
}

#[test]
fn test_hash_join_null_values() {
    // Left table with NULL id
    let left = create_test_from_result(
        "users",
        vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("Unknown"))],
        ],
    );

    // Right table with NULL user_id
    let right = create_test_from_result(
        "orders",
        vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(100)],
            vec![SqlValue::Null, SqlValue::Integer(200)],
        ],
    );

    let mut result = hash_join_inner(left, right, 0, 0).unwrap();

    // Only one match: Alice (id=1) with order (user_id=1)
    // NULLs should not match each other in equi-joins
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values[0], SqlValue::Integer(1)); // user id
    assert_eq!(result.rows()[0].values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice"))); // user name
    assert_eq!(result.rows()[0].values[2], SqlValue::Integer(1)); // order user_id
    assert_eq!(result.rows()[0].values[3], SqlValue::Integer(100)); // order amount
}

#[test]
fn test_hash_join_no_matches() {
    // Left table
    let left = create_test_from_result(
        "users",
        vec![("id", DataType::Integer)],
        vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]],
    );

    // Right table with non-matching ids
    let right = create_test_from_result(
        "orders",
        vec![("user_id", DataType::Integer)],
        vec![vec![SqlValue::Integer(3)], vec![SqlValue::Integer(4)]],
    );

    let mut result = hash_join_inner(left, right, 0, 0).unwrap();

    // No matches
    assert_eq!(result.rows().len(), 0);
}

#[test]
fn test_hash_join_empty_tables() {
    // Left table (empty)
    let left = create_test_from_result("users", vec![("id", DataType::Integer)], vec![]);

    // Right table (empty)
    let right = create_test_from_result("orders", vec![("user_id", DataType::Integer)], vec![]);

    let mut result = hash_join_inner(left, right, 0, 0).unwrap();

    // No rows
    assert_eq!(result.rows().len(), 0);
}

#[test]
fn test_hash_join_duplicate_keys() {
    // Left table with duplicate ids
    let left = create_test_from_result(
        "users",
        vec![("id", DataType::Integer), ("type", DataType::Varchar { max_length: Some(10) })],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("admin"))],
            vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("user"))],
        ],
    );

    // Right table with duplicate user_ids
    let right = create_test_from_result(
        "orders",
        vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(100)],
            vec![SqlValue::Integer(1), SqlValue::Integer(200)],
        ],
    );

    let mut result = hash_join_inner(left, right, 0, 0).unwrap();

    // Cartesian product of matching keys: 2 left rows * 2 right rows = 4 results
    assert_eq!(result.rows().len(), 4);

    // All should have id=1
    for row in result.rows() {
        assert_eq!(row.values[0], SqlValue::Integer(1));
    }
}

// Tests for parallel hash table building

fn create_test_rows(count: usize) -> Vec<vibesql_storage::Row> {
    (0..count)
        .map(|i| vibesql_storage::Row::from_vec(vec![
                SqlValue::Integer(i as i64 % 100), // Keys with duplicates
                SqlValue::Varchar(arcstr::ArcStr::from(format!("value{}", i))),
            ]))
        .collect()
}

#[test]
fn test_build_hash_table_sequential_basic() {
    let build_rows = create_test_rows(100);
    let hash_table = build_hash_table_sequential(&build_rows, 0);

    // Should have 100 unique keys (0-99)
    assert_eq!(hash_table.len(), 100);

    // Each key should have 1 row index
    for (key, row_indices) in hash_table.iter() {
        assert_eq!(row_indices.len(), 1);
        assert_eq!(build_rows[row_indices[0]].values[0], *key);
    }
}

#[test]
fn test_build_hash_table_sequential_with_duplicates() {
    let rows = create_test_rows(1000); // 1000 rows with keys 0-99 (10 duplicates each)
    let hash_table = build_hash_table_sequential(&rows, 0);

    // Should have 100 unique keys
    assert_eq!(hash_table.len(), 100);

    // Each key should have 10 rows
    for (_, rows) in hash_table.iter() {
        assert_eq!(rows.len(), 10);
    }
}

#[test]
fn test_build_hash_table_sequential_null_values() {
    let rows = vec![
        vibesql_storage::Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("one"))]),
        vibesql_storage::Row::from_vec(vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("null1"))]),
        vibesql_storage::Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("two"))]),
        vibesql_storage::Row::from_vec(vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("null2"))]),
    ];

    let hash_table = build_hash_table_sequential(&rows, 0);

    // Should only have 2 keys (NULLs are skipped)
    assert_eq!(hash_table.len(), 2);
    assert!(hash_table.contains_key(&SqlValue::Integer(1)));
    assert!(hash_table.contains_key(&SqlValue::Integer(2)));
    assert!(!hash_table.contains_key(&SqlValue::Null));
}

#[test]
fn test_parallel_sequential_equivalence_small() {
    // Small dataset - should use sequential path in parallel version
    let rows = create_test_rows(100);

    let seq_table = build_hash_table_sequential(&rows, 0);
    let par_table = build_hash_table_parallel(&rows, 0);

    // Should produce identical results
    assert_eq!(seq_table.len(), par_table.len());

    for (key, seq_rows) in seq_table.iter() {
        let par_rows = par_table.get(key).expect("Key should exist in parallel table");
        assert_eq!(seq_rows.len(), par_rows.len());
    }
}

#[test]
fn test_parallel_sequential_equivalence_large() {
    // Large dataset - should use parallel path
    let build_rows = create_test_rows(10000); // Well above threshold (5000)

    let seq_table = build_hash_table_sequential(&build_rows, 0);
    let par_table = build_hash_table_parallel(&build_rows, 0);

    // Should produce identical results
    assert_eq!(seq_table.len(), par_table.len());

    for (key, seq_indices) in seq_table.iter() {
        let par_indices = par_table.get(key).expect("Key should exist in parallel table");
        assert_eq!(seq_indices.len(), par_indices.len(), "Row count mismatch for key {:?}", key);

        // Verify all row indices are present (order may differ)
        for &seq_idx in seq_indices {
            assert!(
                par_indices
                    .iter()
                    .any(|&par_idx| build_rows[par_idx].values == build_rows[seq_idx].values),
                "Row not found in parallel table"
            );
        }
    }
}

#[test]
fn test_parallel_with_null_values() {
    let rows = vec![
        vibesql_storage::Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("one"))]),
        vibesql_storage::Row::from_vec(vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("null1"))]),
        vibesql_storage::Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("two"))]),
        vibesql_storage::Row::from_vec(vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("null2"))]),
        vibesql_storage::Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("one_dup"))]),
    ];

    let par_table = build_hash_table_parallel(&rows, 0);

    // Should only have 2 keys (NULLs are skipped)
    assert_eq!(par_table.len(), 2);
    assert!(par_table.contains_key(&SqlValue::Integer(1)));
    assert!(par_table.contains_key(&SqlValue::Integer(2)));
    assert!(!par_table.contains_key(&SqlValue::Null));

    // Key 1 should have 2 rows
    assert_eq!(par_table.get(&SqlValue::Integer(1)).unwrap().len(), 2);
}

#[test]
fn test_parallel_hash_join_integration() {
    // Integration test: Create large tables and verify parallel join works correctly

    // Left table: 6000 rows (above join threshold of 5000)
    let left_rows: Vec<Vec<SqlValue>> = (0..6000)
        .map(|i| vec![SqlValue::Integer(i % 100), SqlValue::Varchar(arcstr::ArcStr::from(format!("left{}", i)))])
        .collect();

    let left = create_test_from_result(
        "large_left",
        vec![("id", DataType::Integer), ("data", DataType::Varchar { max_length: Some(50) })],
        left_rows,
    );

    // Right table: 6000 rows
    let right_rows: Vec<Vec<SqlValue>> = (0..6000)
        .map(|i| vec![SqlValue::Integer(i % 100), SqlValue::Varchar(arcstr::ArcStr::from(format!("right{}", i)))])
        .collect();

    let right = create_test_from_result(
        "large_right",
        vec![("id", DataType::Integer), ("data", DataType::Varchar { max_length: Some(50) })],
        right_rows,
    );

    let mut result = hash_join_inner(left, right, 0, 0).unwrap();

    // Each key (0-99) appears 60 times on left and 60 times on right
    // So we expect 100 keys * 60 * 60 = 360,000 result rows
    assert_eq!(result.rows().len(), 360_000);

    // Verify combined row structure
    for row in result.rows() {
        assert_eq!(row.values.len(), 4); // 2 columns from left + 2 from right
    }
}

// Tests for hash_join_left_outer

#[test]
fn test_hash_join_left_outer_basic() {
    // Test basic LEFT OUTER JOIN with matched left rows
    // Left table: users(id, name)
    let left = create_test_from_result(
        "users",
        vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
            vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))],
        ],
    );

    // Right table: orders(user_id, amount)
    let right = create_test_from_result(
        "orders",
        vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(100)],
            vec![SqlValue::Integer(2), SqlValue::Integer(200)],
        ],
    );

    let mut result = hash_join_left_outer(left, right, 0, 0).unwrap();

    // Should have 3 rows: Alice with order, Bob with order, Charlie with NULLs
    assert_eq!(result.rows().len(), 3);

    // Verify Alice (id=1) has matching order
    let alice_row = result.rows().iter().find(|r| r.values[0] == SqlValue::Integer(1)).unwrap();
    assert_eq!(alice_row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("Alice")));
    assert_eq!(alice_row.values[2], SqlValue::Integer(1)); // user_id
    assert_eq!(alice_row.values[3], SqlValue::Integer(100)); // amount

    // Verify Bob (id=2) has matching order
    let bob_row = result.rows().iter().find(|r| r.values[0] == SqlValue::Integer(2)).unwrap();
    assert_eq!(bob_row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
    assert_eq!(bob_row.values[2], SqlValue::Integer(2)); // user_id
    assert_eq!(bob_row.values[3], SqlValue::Integer(200)); // amount

    // Verify Charlie (id=3) has NULL-padded right side
    let charlie_row = result.rows().iter().find(|r| r.values[0] == SqlValue::Integer(3)).unwrap();
    assert_eq!(charlie_row.values[1], SqlValue::Varchar(arcstr::ArcStr::from("Charlie")));
    assert_eq!(charlie_row.values[2], SqlValue::Null); // user_id
    assert_eq!(charlie_row.values[3], SqlValue::Null); // amount
}

#[test]
fn test_hash_join_left_outer_unmatched_left_rows() {
    // Test LEFT OUTER JOIN where all left rows are unmatched
    let left = create_test_from_result(
        "users",
        vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
        ],
    );

    // Right table with non-matching ids
    let right = create_test_from_result(
        "orders",
        vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
        vec![
            vec![SqlValue::Integer(99), SqlValue::Integer(100)],
            vec![SqlValue::Integer(98), SqlValue::Integer(200)],
        ],
    );

    let mut result = hash_join_left_outer(left, right, 0, 0).unwrap();

    // Should preserve all left rows with NULL-padded right side
    assert_eq!(result.rows().len(), 2);

    // All rows should have NULLs for right columns
    for row in result.rows() {
        assert_eq!(row.values.len(), 4);
        assert_eq!(row.values[2], SqlValue::Null); // user_id
        assert_eq!(row.values[3], SqlValue::Null); // amount
    }
}

#[test]
fn test_hash_join_left_outer_multiple_matches() {
    // Test LEFT OUTER JOIN where left rows match multiple right rows
    let left = create_test_from_result(
        "users",
        vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
        ],
    );

    // Right table with multiple orders for Alice
    let right = create_test_from_result(
        "orders",
        vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(100)],
            vec![SqlValue::Integer(1), SqlValue::Integer(150)],
            vec![SqlValue::Integer(1), SqlValue::Integer(200)],
        ],
    );

    let mut result = hash_join_left_outer(left, right, 0, 0).unwrap();

    // Alice should appear 3 times (one for each order), Bob once with NULLs
    assert_eq!(result.rows().len(), 4);

    // Count Alice's rows
    let alice_orders: Vec<_> =
        result.rows().iter().filter(|r| r.values[0] == SqlValue::Integer(1)).collect();
    assert_eq!(alice_orders.len(), 3);

    // Verify all Alice rows have matching amounts
    let amounts: Vec<_> = alice_orders.iter().map(|r| r.values[3].clone()).collect();
    assert!(amounts.contains(&SqlValue::Integer(100)));
    assert!(amounts.contains(&SqlValue::Integer(150)));
    assert!(amounts.contains(&SqlValue::Integer(200)));

    // Bob should have one row with NULLs
    let bob_rows: Vec<_> =
        result.rows().iter().filter(|r| r.values[0] == SqlValue::Integer(2)).collect();
    assert_eq!(bob_rows.len(), 1);
    assert_eq!(bob_rows[0].values[2], SqlValue::Null);
    assert_eq!(bob_rows[0].values[3], SqlValue::Null);
}

#[test]
fn test_hash_join_left_outer_null_keys() {
    // Test LEFT OUTER JOIN with NULL keys in left table
    let left = create_test_from_result(
        "users",
        vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("Unknown1"))],
            vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("Unknown2"))],
        ],
    );

    let right = create_test_from_result(
        "orders",
        vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(100)],
            vec![SqlValue::Null, SqlValue::Integer(200)], // NULL in right (shouldn't match)
        ],
    );

    let mut result = hash_join_left_outer(left, right, 0, 0).unwrap();

    // Should have 3 rows: Alice matched, both Unknowns with NULLs
    assert_eq!(result.rows().len(), 3);

    // Alice should have matching order
    let alice_rows: Vec<_> =
        result.rows().iter().filter(|r| r.values[0] == SqlValue::Integer(1)).collect();
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].values[3], SqlValue::Integer(100));

    // Both Unknown users should have NULL right sides (NULLs don't match)
    let null_key_rows: Vec<_> =
        result.rows().iter().filter(|r| r.values[0] == SqlValue::Null).collect();
    assert_eq!(null_key_rows.len(), 2);
    for row in null_key_rows {
        assert_eq!(row.values[2], SqlValue::Null); // user_id
        assert_eq!(row.values[3], SqlValue::Null); // amount
    }
}

#[test]
fn test_hash_join_left_outer_empty_right_table() {
    // Test LEFT OUTER JOIN with empty right table
    let left = create_test_from_result(
        "users",
        vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))],
            vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))],
        ],
    );

    let right = create_test_from_result(
        "orders",
        vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
        vec![],
    );

    let mut result = hash_join_left_outer(left, right, 0, 0).unwrap();

    // Should preserve all left rows with NULL-padded right side
    assert_eq!(result.rows().len(), 2);

    // All rows should have NULLs for right columns
    for row in result.rows() {
        assert_eq!(row.values.len(), 4);
        assert_eq!(row.values[2], SqlValue::Null);
        assert_eq!(row.values[3], SqlValue::Null);
    }
}

#[test]
fn test_hash_join_left_outer_empty_left_table() {
    // Test LEFT OUTER JOIN with empty left table
    let left = create_test_from_result(
        "users",
        vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
        vec![],
    );

    let right = create_test_from_result(
        "orders",
        vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(100)],
            vec![SqlValue::Integer(2), SqlValue::Integer(200)],
        ],
    );

    let mut result = hash_join_left_outer(left, right, 0, 0).unwrap();

    // Should produce empty result (no left rows to preserve)
    assert_eq!(result.rows().len(), 0);
}

// Tests for multi-column hash join

#[test]
fn test_hash_join_inner_multi_two_columns() {
    // Test multi-column hash join with composite key (2 columns)
    // Left table: sales(region_id, product_id, amount)
    let left = create_test_from_result(
        "sales",
        vec![
            ("region_id", DataType::Integer),
            ("product_id", DataType::Integer),
            ("amount", DataType::Integer),
        ],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(100), SqlValue::Integer(500)],
            vec![SqlValue::Integer(1), SqlValue::Integer(200), SqlValue::Integer(300)],
            vec![SqlValue::Integer(2), SqlValue::Integer(100), SqlValue::Integer(400)],
            vec![SqlValue::Integer(2), SqlValue::Integer(300), SqlValue::Integer(200)],
        ],
    );

    // Right table: inventory(region_id, product_id, stock)
    let right = create_test_from_result(
        "inventory",
        vec![
            ("region_id", DataType::Integer),
            ("product_id", DataType::Integer),
            ("stock", DataType::Integer),
        ],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(100), SqlValue::Integer(50)],
            vec![SqlValue::Integer(1), SqlValue::Integer(200), SqlValue::Integer(30)],
            vec![SqlValue::Integer(2), SqlValue::Integer(200), SqlValue::Integer(20)], // No match
        ],
    );

    // Join on (region_id, product_id)
    let mut result = hash_join_inner_multi(
        left,
        right,
        &[0, 1], // left column indices
        &[0, 1], // right column indices
    )
    .unwrap();

    // Should have 2 matches:
    // (1, 100) from left matches (1, 100) from right
    // (1, 200) from left matches (1, 200) from right
    assert_eq!(result.rows().len(), 2);

    // Verify combined row structure (6 columns total)
    for row in result.rows() {
        assert_eq!(row.values.len(), 6);
    }

    // Verify the (1, 100) match
    let match_1_100: Vec<_> = result
        .rows()
        .iter()
        .filter(|r| r.values[0] == SqlValue::Integer(1) && r.values[1] == SqlValue::Integer(100))
        .collect();
    assert_eq!(match_1_100.len(), 1);
    assert_eq!(match_1_100[0].values[2], SqlValue::Integer(500)); // amount from left
    assert_eq!(match_1_100[0].values[5], SqlValue::Integer(50)); // stock from right

    // Verify the (1, 200) match
    let match_1_200: Vec<_> = result
        .rows()
        .iter()
        .filter(|r| r.values[0] == SqlValue::Integer(1) && r.values[1] == SqlValue::Integer(200))
        .collect();
    assert_eq!(match_1_200.len(), 1);
    assert_eq!(match_1_200[0].values[2], SqlValue::Integer(300)); // amount from left
    assert_eq!(match_1_200[0].values[5], SqlValue::Integer(30)); // stock from right
}

#[test]
fn test_hash_join_inner_multi_null_in_composite_key() {
    // Test that NULLs in composite keys don't match
    let left = create_test_from_result(
        "left",
        vec![("a", DataType::Integer), ("b", DataType::Integer)],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(2)],
            vec![SqlValue::Integer(1), SqlValue::Null], // NULL in second key
            vec![SqlValue::Null, SqlValue::Integer(2)], // NULL in first key
        ],
    );

    let right = create_test_from_result(
        "right",
        vec![("x", DataType::Integer), ("y", DataType::Integer)],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(2)],
            vec![SqlValue::Integer(1), SqlValue::Null], // NULL shouldn't match
            vec![SqlValue::Null, SqlValue::Integer(2)], // NULL shouldn't match
        ],
    );

    let mut result = hash_join_inner_multi(left, right, &[0, 1], &[0, 1]).unwrap();

    // Only one match: (1, 2) = (1, 2)
    // NULLs don't match
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].values[0], SqlValue::Integer(1));
    assert_eq!(result.rows()[0].values[1], SqlValue::Integer(2));
}

#[test]
fn test_hash_join_inner_multi_duplicate_composite_keys() {
    // Test composite keys with duplicates
    let left = create_test_from_result(
        "left",
        vec![
            ("a", DataType::Integer),
            ("b", DataType::Integer),
            ("data", DataType::Varchar { max_length: Some(10) }),
        ],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("L1"))],
            vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("L2"))], // Duplicate key
        ],
    );

    let right = create_test_from_result(
        "right",
        vec![
            ("x", DataType::Integer),
            ("y", DataType::Integer),
            ("info", DataType::Varchar { max_length: Some(10) }),
        ],
        vec![
            vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("R1"))],
            vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("R2"))], // Duplicate key
        ],
    );

    let mut result = hash_join_inner_multi(left, right, &[0, 1], &[0, 1]).unwrap();

    // Should have 4 results (2 left rows × 2 right rows for matching composite key)
    assert_eq!(result.rows().len(), 4);

    // Verify all combinations exist
    let result_data: Vec<_> =
        result.rows().iter().map(|r| (r.values[2].clone(), r.values[5].clone())).collect();

    assert!(result_data
        .contains(&(SqlValue::Varchar(arcstr::ArcStr::from("L1")), SqlValue::Varchar(arcstr::ArcStr::from("R1")))));
    assert!(result_data
        .contains(&(SqlValue::Varchar(arcstr::ArcStr::from("L1")), SqlValue::Varchar(arcstr::ArcStr::from("R2")))));
    assert!(result_data
        .contains(&(SqlValue::Varchar(arcstr::ArcStr::from("L2")), SqlValue::Varchar(arcstr::ArcStr::from("R1")))));
    assert!(result_data
        .contains(&(SqlValue::Varchar(arcstr::ArcStr::from("L2")), SqlValue::Varchar(arcstr::ArcStr::from("R2")))));
}

#[test]
fn test_composite_key_build_and_hash() {
    // Test composite key building and hashing
    let rows = vec![
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("a")),
            SqlValue::Integer(100),
        ]),
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(arcstr::ArcStr::from("b")),
            SqlValue::Integer(200),
        ]),
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(arcstr::ArcStr::from("a")),
            SqlValue::Integer(300),
        ]), // Duplicate key
    ];

    // Build hash table with composite key (columns 0 and 1)
    let hash_table = build_hash_table_composite_sequential(&rows, &[0, 1]);

    // Should have 2 unique composite keys
    assert_eq!(hash_table.len(), 2);

    // Key (1, "a") should have 2 row indices
    let key_1_a = CompositeKey(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("a"))]);
    let indices = hash_table.get(&key_1_a).unwrap();
    assert_eq!(indices.len(), 2);
    assert!(indices.contains(&0));
    assert!(indices.contains(&2));

    // Key (2, "b") should have 1 row index
    let key_2_b = CompositeKey(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("b"))]);
    let indices = hash_table.get(&key_2_b).unwrap();
    assert_eq!(indices.len(), 1);
    assert!(indices.contains(&1));
}

#[test]
fn test_composite_key_with_nulls() {
    // Test that rows with NULL in composite key are skipped
    let rows = vec![
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("a"))]),
        Row::new(vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("b"))]), // NULL in first column
        Row::new(vec![SqlValue::Integer(2), SqlValue::Null]),               // NULL in second column
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("c"))]),
    ];

    let hash_table = build_hash_table_composite_sequential(&rows, &[0, 1]);

    // Only 2 keys should be in the hash table (rows with NULLs skipped)
    assert_eq!(hash_table.len(), 2);
}
