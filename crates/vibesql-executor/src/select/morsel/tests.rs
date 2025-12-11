//! Tests for morsel-driven parallel execution.

use std::cmp::Ordering as CmpOrdering;

use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use super::config::{
    DEFAULT_MORSEL_SIZE, MAX_MORSEL_SIZE, MIN_MORSEL_SIZE,
};
use super::{
    create_morsels, global_config, morsel_filter, morsel_map, morsel_parallel_filter,
    morsel_parallel_group, morsel_parallel_map, morsel_parallel_reduce, morsel_parallel_sort,
    morsel_sort_by, Morsel, MorselConfig,
};

fn create_test_rows(count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            Row::from_vec(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("row{}", i))),
            ])
        })
        .collect()
}

#[test]
fn test_morsel_creation() {
    let morsel = Morsel::new(100, 50);
    assert_eq!(morsel.start_idx(), 100);
    assert_eq!(morsel.row_count(), 50);
    assert_eq!(morsel.end_idx(), 150);
}

#[test]
fn test_morsel_rows_extraction() {
    let rows = create_test_rows(100);
    let morsel = Morsel::new(10, 20);
    let extracted = morsel.rows(&rows);

    assert_eq!(extracted.len(), 20);
    assert!(matches!(extracted[0].values[0], SqlValue::Integer(10)));
    assert!(matches!(extracted[19].values[0], SqlValue::Integer(29)));
}

#[test]
fn test_create_morsels() {
    let morsels = create_morsels(1000, 300);
    assert_eq!(morsels.len(), 4); // 300 + 300 + 300 + 100

    assert_eq!(morsels[0].start_idx(), 0);
    assert_eq!(morsels[0].row_count(), 300);
    assert_eq!(morsels[3].start_idx(), 900);
    assert_eq!(morsels[3].row_count(), 100);
}

#[test]
fn test_morsel_filter_small_dataset() {
    let config = MorselConfig::new(100);
    let rows = create_test_rows(50); // Below morsel size

    let filtered = morsel_parallel_filter(
        &rows,
        &config,
        |row| matches!(row.values[0], SqlValue::Integer(x) if x % 2 == 0),
    );

    assert_eq!(filtered.len(), 25); // 0, 2, 4, ..., 48
}

#[test]
fn test_morsel_filter_large_dataset() {
    let config = MorselConfig::new(100);
    let rows = create_test_rows(1000); // Multiple morsels

    let filtered = morsel_parallel_filter(
        &rows,
        &config,
        |row| matches!(row.values[0], SqlValue::Integer(x) if x % 2 == 0),
    );

    assert_eq!(filtered.len(), 500); // Even numbers

    // Verify order is preserved
    for (i, row) in filtered.iter().enumerate() {
        let expected = (i * 2) as i64;
        assert!(matches!(row.values[0], SqlValue::Integer(x) if x == expected));
    }
}

#[test]
fn test_morsel_map() {
    let config = MorselConfig::new(100);
    let rows = create_test_rows(500);

    let transformed = morsel_parallel_map(&rows, &config, |row| {
        let mut new_row = row.clone();
        if let SqlValue::Integer(x) = row.values[0] {
            new_row.values[0] = SqlValue::Integer(x * 2);
        }
        new_row
    });

    assert_eq!(transformed.len(), 500);

    // Verify transformation and order
    for (i, row) in transformed.iter().enumerate() {
        let expected = (i * 2) as i64;
        assert!(matches!(row.values[0], SqlValue::Integer(x) if x == expected));
    }
}

#[test]
fn test_morsel_reduce() {
    let config = MorselConfig::new(100);
    let rows = create_test_rows(500);

    // Sum all integer values
    let sum = morsel_parallel_reduce(
        &rows,
        &config,
        |morsel_rows| {
            morsel_rows
                .iter()
                .map(|r| if let SqlValue::Integer(x) = r.values[0] { x } else { 0 })
                .sum::<i64>()
        },
        |a, b| a + b,
        0i64,
    );

    // Sum of 0..500 = 499 * 500 / 2 = 124750
    assert_eq!(sum, 124750);
}

#[test]
fn test_morsel_filter_empty_input() {
    let config = MorselConfig::new(100);
    let rows: Vec<Row> = Vec::new();

    let filtered = morsel_parallel_filter(&rows, &config, |_| true);
    assert!(filtered.is_empty());
}

#[test]
fn test_global_config() {
    let config = global_config();
    assert!(config.morsel_size >= 1000);
}

#[test]
fn test_convenience_functions() {
    let rows = create_test_rows(100);

    let filtered =
        morsel_filter(&rows, |row| matches!(row.values[0], SqlValue::Integer(x) if x < 10));
    assert_eq!(filtered.len(), 10);

    let mapped = morsel_map(&rows, |row| row.clone());
    assert_eq!(mapped.len(), 100);
}

#[test]
fn test_morsel_parallel_group() {
    use ahash::AHashMap;

    let config = MorselConfig::new(100);
    // Create rows with values 0..500, grouped by modulo 10
    let rows = create_test_rows(500);

    let groups = morsel_parallel_group(
        &rows,
        &config,
        |row| {
            // Group by value mod 10
            if let SqlValue::Integer(x) = row.values[0] {
                vec![SqlValue::Integer(x % 10)]
            } else {
                vec![SqlValue::Null]
            }
        },
        |a: AHashMap<Vec<SqlValue>, Vec<Row>>,
         b: AHashMap<Vec<SqlValue>, Vec<Row>>|
         -> AHashMap<Vec<SqlValue>, Vec<Row>> {
            let mut result = a;
            for (key, mut rows) in b {
                result.entry(key).or_default().append(&mut rows);
            }
            result
        },
    );

    // Should have 10 groups (0..9)
    assert_eq!(groups.len(), 10);

    // Each group should have 50 rows
    for (_, group_rows) in groups.iter() {
        assert_eq!(group_rows.len(), 50);
    }
}

// ==================== Morsel Sort Tests ====================

#[test]
fn test_morsel_sort_empty_input() {
    let config = MorselConfig::new(100);
    let rows: Vec<Row> = Vec::new();

    let sorted =
        morsel_parallel_sort(&rows, &config, |a, b| match (&a.values[0], &b.values[0]) {
            (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
            _ => CmpOrdering::Equal,
        });

    assert!(sorted.is_empty());
}

#[test]
fn test_morsel_sort_small_dataset() {
    let config = MorselConfig::new(100);
    // Create rows in reverse order: 49, 48, ..., 1, 0
    let rows: Vec<Row> = (0..50)
        .rev()
        .map(|i| {
            Row::from_vec(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("row{}", i))),
            ])
        })
        .collect();

    let sorted =
        morsel_parallel_sort(&rows, &config, |a, b| match (&a.values[0], &b.values[0]) {
            (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
            _ => CmpOrdering::Equal,
        });

    // Verify sorted in ascending order
    assert_eq!(sorted.len(), 50);
    for (i, row) in sorted.iter().enumerate() {
        assert!(matches!(row.values[0], SqlValue::Integer(x) if x == i as i64));
    }
}

#[test]
fn test_morsel_sort_large_dataset() {
    let config = MorselConfig::new(100); // Small morsel size to force multiple morsels
                                         // Create rows in reverse order: 999, 998, ..., 1, 0
    let rows: Vec<Row> = (0..1000)
        .rev()
        .map(|i| {
            Row::from_vec(vec![
                SqlValue::Integer(i as i64),
                SqlValue::Varchar(arcstr::ArcStr::from(format!("row{}", i))),
            ])
        })
        .collect();

    let sorted =
        morsel_parallel_sort(&rows, &config, |a, b| match (&a.values[0], &b.values[0]) {
            (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
            _ => CmpOrdering::Equal,
        });

    // Verify sorted in ascending order
    assert_eq!(sorted.len(), 1000);
    for (i, row) in sorted.iter().enumerate() {
        assert!(matches!(row.values[0], SqlValue::Integer(x) if x == i as i64));
    }
}

#[test]
fn test_morsel_sort_descending() {
    let config = MorselConfig::new(100);
    // Create rows in ascending order: 0, 1, 2, ..., 499
    let rows = create_test_rows(500);

    let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
        // Descending order
        match (&a.values[0], &b.values[0]) {
            (SqlValue::Integer(x), SqlValue::Integer(y)) => y.cmp(x),
            _ => CmpOrdering::Equal,
        }
    });

    // Verify sorted in descending order
    assert_eq!(sorted.len(), 500);
    for (i, row) in sorted.iter().enumerate() {
        let expected = (499 - i) as i64;
        assert!(matches!(row.values[0], SqlValue::Integer(x) if x == expected));
    }
}

#[test]
fn test_morsel_sort_with_nulls() {
    let config = MorselConfig::new(100);
    // Create rows with some NULLs interspersed
    let mut rows: Vec<Row> = Vec::new();
    for i in 0..200 {
        if i % 10 == 0 {
            rows.push(Row::from_vec(vec![SqlValue::Null]));
        } else {
            rows.push(Row::from_vec(vec![SqlValue::Integer(i as i64)]));
        }
    }

    // Sort with NULLs last
    let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
        match (&a.values[0], &b.values[0]) {
            (SqlValue::Null, SqlValue::Null) => CmpOrdering::Equal,
            (SqlValue::Null, _) => CmpOrdering::Greater, // NULL sorts last
            (_, SqlValue::Null) => CmpOrdering::Less,
            (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
            _ => CmpOrdering::Equal,
        }
    });

    // Verify: non-NULL values sorted first, then NULLs
    assert_eq!(sorted.len(), 200);

    // Count NULLs (should be 20: 0, 10, 20, ..., 190)
    let null_count = sorted.iter().filter(|r| r.values[0] == SqlValue::Null).count();
    assert_eq!(null_count, 20);

    // Verify NULLs are at the end
    for row in sorted.iter().skip(180) {
        assert_eq!(row.values[0], SqlValue::Null);
    }

    // Verify non-NULLs are sorted ascending before NULLs
    let mut last_val = -1i64;
    for row in sorted.iter().take(180) {
        if let SqlValue::Integer(x) = row.values[0] {
            assert!(x > last_val, "Values should be ascending: {} > {}", x, last_val);
            last_val = x;
        }
    }
}

#[test]
fn test_morsel_sort_multi_key() {
    let config = MorselConfig::new(50);
    // Create rows with two columns: group (0-9) and value (0-99)
    // Multiple rows per group to test stable-like behavior
    let mut rows: Vec<Row> = Vec::new();
    for i in 0..100 {
        rows.push(Row::from_vec(vec![
            SqlValue::Integer((i % 10) as i64), // Group
            SqlValue::Integer(i as i64),        // Value
        ]));
    }

    // Sort by group ASC, then by value DESC within group
    let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
        let group_a = match &a.values[0] {
            SqlValue::Integer(x) => *x,
            _ => 0,
        };
        let group_b = match &b.values[0] {
            SqlValue::Integer(x) => *x,
            _ => 0,
        };
        let val_a = match &a.values[1] {
            SqlValue::Integer(x) => *x,
            _ => 0,
        };
        let val_b = match &b.values[1] {
            SqlValue::Integer(x) => *x,
            _ => 0,
        };

        match group_a.cmp(&group_b) {
            CmpOrdering::Equal => val_b.cmp(&val_a), // DESC within group
            other => other,
        }
    });

    assert_eq!(sorted.len(), 100);

    // Verify: groups are in order 0-9, and within each group values are descending
    let mut current_group = 0i64;
    let mut last_val_in_group = i64::MAX;
    for row in sorted.iter() {
        let group = match &row.values[0] {
            SqlValue::Integer(x) => *x,
            _ => 0,
        };
        let val = match &row.values[1] {
            SqlValue::Integer(x) => *x,
            _ => 0,
        };

        if group != current_group {
            assert!(group > current_group, "Groups should be ascending");
            current_group = group;
            last_val_in_group = i64::MAX;
        }
        assert!(val < last_val_in_group, "Values within group should be descending");
        last_val_in_group = val;
    }
}

#[test]
fn test_morsel_sort_by_convenience() {
    // Create rows in reverse order
    let rows: Vec<Row> =
        (0..100).rev().map(|i| Row::from_vec(vec![SqlValue::Integer(i as i64)])).collect();

    let sorted = morsel_sort_by(&rows, |a, b| match (&a.values[0], &b.values[0]) {
        (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
        _ => CmpOrdering::Equal,
    });

    assert_eq!(sorted.len(), 100);
    for (i, row) in sorted.iter().enumerate() {
        assert!(matches!(row.values[0], SqlValue::Integer(x) if x == i as i64));
    }
}

#[test]
fn test_morsel_sort_single_morsel() {
    // Test with exactly one morsel worth of data
    let config = MorselConfig::new(100);
    let rows: Vec<Row> =
        (0..100).rev().map(|i| Row::from_vec(vec![SqlValue::Integer(i as i64)])).collect();

    let sorted =
        morsel_parallel_sort(&rows, &config, |a, b| match (&a.values[0], &b.values[0]) {
            (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
            _ => CmpOrdering::Equal,
        });

    assert_eq!(sorted.len(), 100);
    for (i, row) in sorted.iter().enumerate() {
        assert!(matches!(row.values[0], SqlValue::Integer(x) if x == i as i64));
    }
}

#[test]
fn test_morsel_sort_all_equal() {
    let config = MorselConfig::new(50);
    // All rows have the same value
    let rows: Vec<Row> = (0..200).map(|_| Row::from_vec(vec![SqlValue::Integer(42)])).collect();

    let sorted =
        morsel_parallel_sort(&rows, &config, |a, b| match (&a.values[0], &b.values[0]) {
            (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
            _ => CmpOrdering::Equal,
        });

    assert_eq!(sorted.len(), 200);
    for row in sorted.iter() {
        assert!(matches!(row.values[0], SqlValue::Integer(42)));
    }
}

// ============================================
// Adaptive sizing tests
// ============================================

#[test]
fn test_for_row_width_wide_rows() {
    // Wide rows (500 bytes) should use smaller morsels
    let config = MorselConfig::for_row_width(500);
    // 2MB / 500 bytes = 4096 rows, but clamped to MIN_MORSEL_SIZE (10,000)
    assert_eq!(config.morsel_size, MIN_MORSEL_SIZE);
}

#[test]
fn test_for_row_width_narrow_rows() {
    // Narrow rows (20 bytes) should use larger morsels
    let config = MorselConfig::for_row_width(20);
    // 2MB / 20 bytes = 104,857 rows, but clamped to MAX_MORSEL_SIZE (100,000)
    assert_eq!(config.morsel_size, MAX_MORSEL_SIZE);
}

#[test]
fn test_for_row_width_medium_rows() {
    // Medium rows (100 bytes) - typical case
    let config = MorselConfig::for_row_width(100);
    // 2MB / 100 bytes = 20,971 rows
    assert_eq!(config.morsel_size, 20_971);
}

#[test]
fn test_for_row_width_zero_bytes() {
    // Zero bytes should be treated as 1 byte (avoid division by zero)
    let config = MorselConfig::for_row_width(0);
    // 2MB / 1 byte = way more than MAX, clamped to MAX_MORSEL_SIZE
    assert_eq!(config.morsel_size, MAX_MORSEL_SIZE);
}

#[test]
fn test_for_schema_narrow() {
    // Schema with just integers - narrow rows
    let schema = [DataType::Integer, DataType::Integer];
    let config = MorselConfig::for_schema(&schema);
    // Row overhead (24) + 2 * (8 + 4) = 24 + 24 = 48 bytes
    // 2MB / 48 = ~43,690, within bounds
    assert!(config.morsel_size > 40_000 && config.morsel_size < 50_000);
}

#[test]
fn test_for_schema_wide() {
    // Schema with varchars - wider rows
    let schema = [
        DataType::Integer,
        DataType::Varchar { max_length: Some(200) },
        DataType::Varchar { max_length: Some(200) },
    ];
    let config = MorselConfig::for_schema(&schema);
    // Row overhead (24) + (8+4) + 2*(8+16+200) = 24 + 12 + 448 = 484 bytes
    // Should result in smaller morsels due to wide rows
    assert!(config.morsel_size <= DEFAULT_MORSEL_SIZE);
}

#[test]
fn test_for_schema_empty() {
    // Empty schema should use default
    let schema: [DataType; 0] = [];
    let config = MorselConfig::for_schema(&schema);
    assert_eq!(config.morsel_size, DEFAULT_MORSEL_SIZE);
}

#[test]
fn test_for_selectivity_low() {
    // Low selectivity (1%) should use larger morsels
    let config = MorselConfig::for_selectivity(0.01);
    // 50,000 / 0.01 = 5,000,000, clamped to MAX_MORSEL_SIZE * 2 = 200,000
    assert_eq!(config.morsel_size, MAX_MORSEL_SIZE * 2);
}

#[test]
fn test_for_selectivity_high() {
    // High selectivity (90%) should use default morsels
    let config = MorselConfig::for_selectivity(0.90);
    assert_eq!(config.morsel_size, DEFAULT_MORSEL_SIZE);
}

#[test]
fn test_for_selectivity_medium() {
    // Medium-low selectivity (5%) should scale appropriately
    let config = MorselConfig::for_selectivity(0.05);
    // 50,000 / 0.05 = 1,000,000, clamped to MAX_MORSEL_SIZE * 2 = 200,000
    assert_eq!(config.morsel_size, MAX_MORSEL_SIZE * 2);
}

#[test]
fn test_for_selectivity_boundary() {
    // At 10% boundary, should still use default
    let config = MorselConfig::for_selectivity(0.10);
    assert_eq!(config.morsel_size, DEFAULT_MORSEL_SIZE);

    // Just below 10% should scale up
    let config = MorselConfig::for_selectivity(0.09);
    assert!(config.morsel_size > DEFAULT_MORSEL_SIZE);
}

#[test]
fn test_adaptive_schema_only() {
    // With schema but no selectivity
    let schema = [DataType::Integer, DataType::Bigint];
    let config = MorselConfig::adaptive(&schema, None);
    // Should be same as for_schema
    let expected = MorselConfig::for_schema(&schema);
    assert_eq!(config.morsel_size, expected.morsel_size);
}

#[test]
fn test_adaptive_with_selectivity() {
    // With schema and low selectivity
    let schema = [DataType::Integer, DataType::Bigint];
    let config = MorselConfig::adaptive(&schema, Some(0.01));
    // Should be larger than schema-only due to low selectivity
    let schema_only = MorselConfig::for_schema(&schema);
    assert!(config.morsel_size > schema_only.morsel_size);
}

#[test]
fn test_adaptive_high_selectivity() {
    // With schema and high selectivity - should be same as schema-only
    let schema = [DataType::Integer, DataType::Bigint];
    let config = MorselConfig::adaptive(&schema, Some(0.90));
    let schema_only = MorselConfig::for_schema(&schema);
    assert_eq!(config.morsel_size, schema_only.morsel_size);
}

#[test]
fn test_data_type_size_estimates() {
    // Test a few key type size estimates
    assert_eq!(DataType::Integer.estimated_size_bytes(), 8 + 4); // enum + value
    assert_eq!(DataType::Bigint.estimated_size_bytes(), 8 + 8);
    assert_eq!(DataType::Boolean.estimated_size_bytes(), 8 + 1);

    // VARCHAR with max_length
    let varchar = DataType::Varchar { max_length: Some(100) };
    assert_eq!(varchar.estimated_size_bytes(), 8 + 16 + 100); // enum + arcstr + chars

    // Vector type
    let vector = DataType::Vector { dimensions: 128 };
    assert_eq!(vector.estimated_size_bytes(), 8 + 24 + 128 * 4); // enum + vec header + floats
}
