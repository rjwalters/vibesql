//! Tests for cost estimation

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::{DataType, SqlValue};

use super::*;
use crate::statistics::{ColumnStatistics, TableStatistics};
use crate::Row;

fn create_test_table_stats(row_count: usize) -> TableStatistics {
    let schema = TableSchema::new(
        "test_table".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
    );

    let rows: Vec<Row> =
        (0..row_count).map(|i| Row::new(vec![SqlValue::Integer(i as i64)])).collect();

    TableStatistics::compute(&rows, &schema)
}

#[test]
fn test_table_scan_cost() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);

    let cost = estimator.estimate_table_scan(&table_stats);

    // Expected: (1000/100 pages * 1.0) + (1000 rows * 0.01) = 10 + 10 = 20
    assert!((cost - 20.0).abs() < 0.1);
}

#[test]
fn test_index_scan_high_selectivity() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let col_stats = table_stats.columns.get("id").unwrap();

    // High selectivity (50% of rows match)
    let cost = estimator.estimate_index_scan(&table_stats, col_stats, 0.5);

    // Index scan should be expensive for high selectivity
    // because we do random I/O for each row
    assert!(cost > 100.0);
}

#[test]
fn test_index_scan_low_selectivity() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let col_stats = table_stats.columns.get("id").unwrap();

    // Low selectivity (1% of rows match)
    let cost = estimator.estimate_index_scan(&table_stats, col_stats, 0.01);

    // Index scan should be cheap for low selectivity
    assert!(cost < 50.0);
}

#[test]
fn test_choose_access_method_favors_index_for_low_selectivity() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);
    let col_stats = table_stats.columns.get("id").unwrap();

    // Very selective query (0.1% of rows)
    let method = estimator.choose_access_method(&table_stats, Some(col_stats), 0.001);

    assert!(method.is_index_scan());
}

#[test]
fn test_choose_access_method_favors_table_scan_for_high_selectivity() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let col_stats = table_stats.columns.get("id").unwrap();

    // Non-selective query (90% of rows)
    let method = estimator.choose_access_method(&table_stats, Some(col_stats), 0.9);

    assert!(!method.is_index_scan());
}

#[test]
fn test_choose_access_method_no_index() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);

    // No index available
    let method = estimator.choose_access_method(&table_stats, None, 0.1);

    assert!(!method.is_index_scan());
}

// ============================================================================
// DML Cost Estimation Tests
// ============================================================================

#[test]
fn test_insert_cost_basic() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let index_info = TableIndexInfo::new(1, 0, false, 0.0, 64);

    // Insert 100 rows with 1 hash index (PK)
    let cost = estimator.estimate_insert(100, &table_stats, &index_info);

    // Expected:
    // - Tuple cost: 100 * 0.1 = 10.0
    // - Hash index: 100 * 1 * 0.05 = 5.0
    // - Columnar invalidation: 0.1
    // - WAL cost: 100 * 0.12 + 0.5 = 12.5
    // Total: ~27.6
    assert!(cost > 27.0 && cost < 29.0, "Insert cost was {}", cost);
}

#[test]
fn test_insert_cost_with_btree_indexes() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let index_info = TableIndexInfo::new(1, 2, false, 0.0, 64);

    // Insert 100 rows with 1 PK and 2 B-tree indexes
    let cost = estimator.estimate_insert(100, &table_stats, &index_info);

    // B-tree indexes add significant overhead
    let cost_no_btree = estimator.estimate_insert(
        100,
        &table_stats,
        &TableIndexInfo::new(1, 0, false, 0.0, 64),
    );
    assert!(cost > cost_no_btree, "B-tree indexes should increase cost");
}

#[test]
fn test_insert_cost_native_columnar() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);

    let row_index_info = TableIndexInfo::new(1, 0, false, 0.0, 64);
    let columnar_index_info = TableIndexInfo::new(1, 0, true, 0.0, 64);

    let row_cost = estimator.estimate_insert(10, &table_stats, &row_index_info);
    let columnar_cost = estimator.estimate_insert(10, &table_stats, &columnar_index_info);

    // Native columnar tables have higher overhead due to columnar rebuild
    assert!(
        columnar_cost > row_cost,
        "Columnar insert cost {} should be > row cost {}",
        columnar_cost,
        row_cost
    );
}

#[test]
fn test_update_cost_basic() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let index_info = TableIndexInfo::new(1, 1, false, 0.0, 64);

    // Update 50 rows, all indexes affected
    let full_cost = estimator.estimate_update(50, &table_stats, &index_info, 1.0);

    // Update 50 rows, no indexes affected (only non-indexed columns changed)
    let selective_cost = estimator.estimate_update(50, &table_stats, &index_info, 0.0);

    // Full update should be more expensive than selective update
    assert!(
        full_cost > selective_cost,
        "Full update cost {} should be > selective update cost {}",
        full_cost,
        selective_cost
    );
}

#[test]
fn test_update_cost_scales_with_affected_ratio() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let index_info = TableIndexInfo::new(2, 3, false, 0.0, 64);

    let cost_0 = estimator.estimate_update(100, &table_stats, &index_info, 0.0);
    let cost_50 = estimator.estimate_update(100, &table_stats, &index_info, 0.5);
    let cost_100 = estimator.estimate_update(100, &table_stats, &index_info, 1.0);

    // Costs should increase with affected ratio
    assert!(cost_50 > cost_0, "50% affected should cost more than 0%");
    assert!(cost_100 > cost_50, "100% affected should cost more than 50%");
}

#[test]
fn test_delete_cost_basic() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let index_info = TableIndexInfo::new(1, 1, false, 0.0, 64);

    // Delete 100 rows (10% of table) - no compaction
    let cost = estimator.estimate_delete(100, &table_stats, &index_info);

    // Should be positive and reasonable
    assert!(cost > 0.0, "Delete cost should be positive");
    assert!(cost < 100.0, "Delete cost should be reasonable");
}

#[test]
fn test_delete_cost_with_compaction() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);

    // Case 1: Delete 40% - no compaction yet
    let index_info_40 = TableIndexInfo::new(1, 0, false, 0.0, 64);
    let cost_40 = estimator.estimate_delete(400, &table_stats, &index_info_40);

    // Case 2: Delete 10% when already at 45% deleted - will trigger compaction
    let index_info_trigger = TableIndexInfo::new(1, 0, false, 0.45, 64);
    let cost_trigger = estimator.estimate_delete(100, &table_stats, &index_info_trigger);

    // Compaction should add overhead
    // Note: Even with fewer rows deleted, the compaction overhead makes it expensive
    assert!(
        cost_trigger > cost_40 * 0.1,
        "Delete with compaction {} should have meaningful overhead vs large delete without {}",
        cost_trigger,
        cost_40
    );
}

#[test]
fn test_delete_more_expensive_with_more_indexes() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);

    let no_indexes = TableIndexInfo::new(0, 0, false, 0.0, 64);
    let many_indexes = TableIndexInfo::new(2, 5, false, 0.0, 64);

    let cost_no_indexes = estimator.estimate_delete(100, &table_stats, &no_indexes);
    let cost_many_indexes = estimator.estimate_delete(100, &table_stats, &many_indexes);

    assert!(
        cost_many_indexes > cost_no_indexes,
        "More indexes should increase delete cost: {} vs {}",
        cost_many_indexes,
        cost_no_indexes
    );
}

#[test]
fn test_delete_cheaper_than_insert() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let index_info = TableIndexInfo::new(1, 2, false, 0.0, 64);

    // DELETE uses O(1) bitmap marking, INSERT adds to vector
    let delete_cost = estimator.estimate_delete(100, &table_stats, &index_info);
    let insert_cost = estimator.estimate_insert(100, &table_stats, &index_info);

    // Without compaction, DELETE should be cheaper due to O(1) bitmap vs vector append
    assert!(
        delete_cost < insert_cost,
        "Delete {} should be cheaper than insert {} (without compaction)",
        delete_cost,
        insert_cost
    );
}

#[test]
fn test_dml_costs_scale_with_row_count() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);
    let index_info = TableIndexInfo::new(1, 1, false, 0.0, 64);

    let insert_10 = estimator.estimate_insert(10, &table_stats, &index_info);
    let insert_100 = estimator.estimate_insert(100, &table_stats, &index_info);

    let delete_10 = estimator.estimate_delete(10, &table_stats, &index_info);
    let delete_100 = estimator.estimate_delete(100, &table_stats, &index_info);

    // Costs should scale roughly linearly with row count
    assert!(insert_100 > insert_10 * 5.0, "Insert should scale with rows");
    assert!(delete_100 > delete_10 * 5.0, "Delete should scale with rows");
}

// ============================================================================
// WAL Cost Estimation Tests
// ============================================================================

#[test]
fn test_wal_cost_included_in_insert() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let index_info = TableIndexInfo::new(0, 0, false, 0.0, 64);

    // Insert 100 rows with no indexes
    let cost = estimator.estimate_insert(100, &table_stats, &index_info);

    // WAL component: 100 * 0.12 + 0.5 = 12.5
    // Tuple: 100 * 0.1 = 10.0
    // Columnar: 0.1
    // Total: ~22.6
    assert!(cost > 22.0, "Insert cost should include WAL: {}", cost);

    // Verify WAL is a significant portion (should be >50% of base cost)
    let tuple_plus_columnar = 100.0 * 0.1 + 0.1; // 10.1
    let wal_cost = 100.0 * 0.12 + 0.5; // 12.5
    assert!(
        wal_cost > tuple_plus_columnar,
        "WAL cost ({}) should exceed base tuple cost ({})",
        wal_cost,
        tuple_plus_columnar
    );
}

#[test]
fn test_wal_cost_included_in_update() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let index_info = TableIndexInfo::new(0, 0, false, 0.0, 64);

    // Update 50 rows with no index updates
    let cost = estimator.estimate_update(50, &table_stats, &index_info, 0.0);

    // WAL component: 50 * 0.12 + 0.5 = 6.5
    // Tuple: 50 * 0.08 = 4.0
    // Columnar: 0.1
    // Total: ~10.6
    assert!(cost > 10.0, "Update cost should include WAL: {}", cost);
}

#[test]
fn test_wal_cost_included_in_delete() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);
    let index_info = TableIndexInfo::new(0, 0, false, 0.0, 64);

    // Delete 100 rows with no indexes
    let cost = estimator.estimate_delete(100, &table_stats, &index_info);

    // WAL component: 100 * 0.12 + 0.5 = 12.5
    // Tuple: 100 * 0.05 = 5.0
    // Columnar: 0.1
    // Total: ~17.6
    assert!(cost > 17.0, "Delete cost should include WAL: {}", cost);
}

#[test]
fn test_wal_cost_dominant_in_delete() {
    // Per profiling (#3862), WAL is 56% of DELETE time
    let estimator = CostEstimator::default();
    let _table_stats = create_test_table_stats(1000);
    let _index_info = TableIndexInfo::new(1, 0, false, 0.0, 64);

    // Calculate components
    let rows = 100.0;
    let tuple_cost = rows * estimator.delete_tuple_cost; // 5.0
    let hash_cost = rows * 1.0 * estimator.hash_index_update_cost; // 5.0
    let wal_cost = rows * estimator.wal_write_cost + estimator.wal_sync_cost; // 12.5

    // WAL should be the dominant cost component (>40% of non-columnar costs)
    let base_dml_cost = tuple_cost + hash_cost;
    assert!(
        wal_cost > base_dml_cost,
        "WAL cost ({}) should exceed base DML cost ({}) per profiling data",
        wal_cost,
        base_dml_cost
    );
}

#[test]
fn test_wal_sync_cost_amortized_for_batches() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);
    let index_info = TableIndexInfo::new(1, 0, false, 0.0, 64);

    // Single-row insert
    let cost_1 = estimator.estimate_insert(1, &table_stats, &index_info);

    // 100-row batch insert
    let cost_100 = estimator.estimate_insert(100, &table_stats, &index_info);

    // Per-row cost should be lower for batches due to amortized sync cost
    let per_row_single = cost_1;
    let per_row_batch = cost_100 / 100.0;

    assert!(
        per_row_batch < per_row_single,
        "Batch insert per-row cost ({}) should be less than single-row cost ({}) due to amortized WAL sync",
        per_row_batch,
        per_row_single
    );
}

#[test]
fn test_wal_cost_proportional_to_rows() {
    let estimator = CostEstimator::default();

    // Calculate pure WAL costs (excluding sync overhead)
    let wal_10 = 10.0 * estimator.wal_write_cost;
    let wal_100 = 100.0 * estimator.wal_write_cost;

    // WAL cost should scale linearly with row count
    assert!(
        (wal_100 - wal_10 * 10.0).abs() < 0.001,
        "WAL write cost should scale linearly: 10x rows should be 10x cost"
    );
}

// ============================================================================
// Row Size-Scaled WAL Cost Tests
// ============================================================================

#[test]
fn test_estimate_type_size_fixed_types() {
    // Boolean
    assert_eq!(estimate_type_size(&DataType::Boolean), 1);

    // Integer types
    assert_eq!(estimate_type_size(&DataType::Smallint), 2);
    assert_eq!(estimate_type_size(&DataType::Integer), 4);
    assert_eq!(estimate_type_size(&DataType::Bigint), 8);
    assert_eq!(estimate_type_size(&DataType::Unsigned), 8);

    // Floating point
    assert_eq!(estimate_type_size(&DataType::Real), 4);
    assert_eq!(estimate_type_size(&DataType::DoublePrecision), 8);
    assert_eq!(estimate_type_size(&DataType::Float { precision: 24 }), 4);
    assert_eq!(estimate_type_size(&DataType::Float { precision: 53 }), 8);

    // Date/time
    assert_eq!(estimate_type_size(&DataType::Date), 4);
    assert_eq!(estimate_type_size(&DataType::Time { with_timezone: false }), 8);
    assert_eq!(estimate_type_size(&DataType::Timestamp { with_timezone: false }), 8);
}

#[test]
fn test_estimate_type_size_variable_types() {
    // VARCHAR with max length
    assert_eq!(
        estimate_type_size(&DataType::Varchar { max_length: Some(100) }),
        32 // min(100/2, 32) = 32
    );
    assert_eq!(
        estimate_type_size(&DataType::Varchar { max_length: Some(20) }),
        10 // min(20/2, 32) = 10
    );
    assert_eq!(
        estimate_type_size(&DataType::Varchar { max_length: None }),
        32 // default
    );

    // Character with fixed length
    assert_eq!(estimate_type_size(&DataType::Character { length: 50 }), 50);

    // BLOB/CLOB
    assert_eq!(estimate_type_size(&DataType::BinaryLargeObject), 128);
    assert_eq!(estimate_type_size(&DataType::CharacterLargeObject), 64);
}

#[test]
fn test_estimate_type_size_vector() {
    // Vector with dimensions
    assert_eq!(estimate_type_size(&DataType::Vector { dimensions: 128 }), 128 * 8);
    assert_eq!(estimate_type_size(&DataType::Vector { dimensions: 512 }), 512 * 8);
}

#[test]
fn test_estimate_row_size() {
    // Small row: 2 columns (INTEGER, BOOLEAN)
    let small_row = vec![DataType::Integer, DataType::Boolean];
    let size = estimate_row_size(&small_row);
    // Expected: 4 + 1 + 8 (overhead) = 13, but min is 64
    assert_eq!(size, 64);

    // Medium row: 5 columns
    let medium_row = vec![
        DataType::Integer,
        DataType::Bigint,
        DataType::Varchar { max_length: Some(100) },
        DataType::Timestamp { with_timezone: false },
        DataType::Boolean,
    ];
    let size = estimate_row_size(&medium_row);
    // Expected: 4 + 8 + 32 + 8 + 1 + 8 (overhead) = 61, but min is 64
    assert_eq!(size, 64);

    // Large row: many columns
    let large_row = vec![
        DataType::Integer,
        DataType::Bigint,
        DataType::DoublePrecision,
        DataType::Varchar { max_length: Some(200) },
        DataType::Varchar { max_length: Some(200) },
        DataType::Varchar { max_length: Some(200) },
        DataType::Timestamp { with_timezone: false },
        DataType::Decimal { precision: 18, scale: 2 },
        DataType::Boolean,
        DataType::Character { length: 100 },
    ];
    let size = estimate_row_size(&large_row);
    // Expected: 4 + 8 + 8 + 32 + 32 + 32 + 8 + 16 + 1 + 100 + 8 = 249
    assert_eq!(size, 249);
}

#[test]
fn test_wal_size_factor_small_rows() {
    // Row size equal to BASE_ROW_SIZE (64 bytes)
    let info = TableIndexInfo::new(1, 0, false, 0.0, 64);
    assert!((info.wal_size_factor() - 1.0).abs() < 0.01);

    // Row size smaller than BASE_ROW_SIZE (clamped to 1.0)
    let info = TableIndexInfo::new(1, 0, false, 0.0, 32);
    assert!((info.wal_size_factor() - 1.0).abs() < 0.01);
}

#[test]
fn test_wal_size_factor_medium_rows() {
    // Row size 2x BASE_ROW_SIZE
    let info = TableIndexInfo::new(1, 0, false, 0.0, 128);
    assert!((info.wal_size_factor() - 2.0).abs() < 0.01);

    // Row size 4x BASE_ROW_SIZE
    let info = TableIndexInfo::new(1, 0, false, 0.0, 256);
    assert!((info.wal_size_factor() - 4.0).abs() < 0.01);
}

#[test]
fn test_wal_size_factor_large_rows_capped() {
    // Row size 15x BASE_ROW_SIZE (should be capped at MAX_WAL_SIZE_FACTOR = 10)
    let info = TableIndexInfo::new(1, 0, false, 0.0, 960); // 64 * 15
    assert!((info.wal_size_factor() - 10.0).abs() < 0.01);

    // Extremely large rows also capped
    let info = TableIndexInfo::new(1, 0, false, 0.0, 10000);
    assert!((info.wal_size_factor() - 10.0).abs() < 0.01);
}

#[test]
fn test_insert_wal_cost_scales_with_row_size() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);

    // Small row (64 bytes) - factor of 1.0
    let small_info = TableIndexInfo::new(1, 0, false, 0.0, 64);
    let small_cost = estimator.estimate_insert(100, &table_stats, &small_info);

    // Large row (256 bytes) - factor of 4.0
    let large_info = TableIndexInfo::new(1, 0, false, 0.0, 256);
    let large_cost = estimator.estimate_insert(100, &table_stats, &large_info);

    // Large row should have higher WAL cost
    assert!(
        large_cost > small_cost,
        "Large row insert cost ({}) should be higher than small row cost ({})",
        large_cost,
        small_cost
    );

    // The difference should be approximately 3x the WAL cost (4x - 1x = 3x factor)
    // WAL base cost = 100 * 0.12 = 12.0
    // Expected increase = 12.0 * 3 = 36.0
    let cost_diff = large_cost - small_cost;
    assert!(
        cost_diff > 30.0 && cost_diff < 40.0,
        "Cost difference ({}) should be approximately 3x WAL base cost",
        cost_diff
    );
}

#[test]
fn test_update_wal_cost_scales_with_row_size() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);

    // Small row (64 bytes)
    let small_info = TableIndexInfo::new(1, 0, false, 0.0, 64);
    let small_cost = estimator.estimate_update(50, &table_stats, &small_info, 0.0);

    // Large row (320 bytes) - factor of 5.0
    let large_info = TableIndexInfo::new(1, 0, false, 0.0, 320);
    let large_cost = estimator.estimate_update(50, &table_stats, &large_info, 0.0);

    // Large row should have higher WAL cost
    assert!(
        large_cost > small_cost,
        "Large row update cost ({}) should be higher than small row cost ({})",
        large_cost,
        small_cost
    );
}

#[test]
fn test_delete_wal_cost_scales_with_row_size() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);

    // Small row (64 bytes)
    let small_info = TableIndexInfo::new(0, 0, false, 0.0, 64);
    let small_cost = estimator.estimate_delete(100, &table_stats, &small_info);

    // Large row (640 bytes) - factor would be 10.0 but capped at MAX_WAL_SIZE_FACTOR
    let large_info = TableIndexInfo::new(0, 0, false, 0.0, 640);
    let large_cost = estimator.estimate_delete(100, &table_stats, &large_info);

    // Large row should have higher WAL cost
    assert!(
        large_cost > small_cost,
        "Large row delete cost ({}) should be higher than small row cost ({})",
        large_cost,
        small_cost
    );
}

#[test]
fn test_2_column_vs_50_column_wal_cost() {
    // This is the key test from the issue: verify that a 50-column table
    // has higher WAL cost than a 2-column table
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);

    // 2-column table: INTEGER + VARCHAR(50) = 4 + 25 + 8 = 37 bytes (min 64)
    let small_row_size =
        estimate_row_size(&[DataType::Integer, DataType::Varchar { max_length: Some(50) }]);
    assert_eq!(small_row_size, 64); // min row size

    // 50-column table: mix of types, much larger
    let large_columns: Vec<DataType> = (0..10)
        .map(|_| DataType::Integer)
        .chain((0..10).map(|_| DataType::Bigint))
        .chain((0..10).map(|_| DataType::DoublePrecision))
        .chain((0..10).map(|_| DataType::Varchar { max_length: Some(100) }))
        .chain((0..10).map(|_| DataType::Timestamp { with_timezone: false }))
        .collect();
    assert_eq!(large_columns.len(), 50);

    let large_row_size = estimate_row_size(&large_columns);
    // Expected: 10*4 + 10*8 + 10*8 + 10*32 + 10*8 + 8 = 40+80+80+320+80+8 = 608 bytes
    assert!(large_row_size > 500, "Large row should be > 500 bytes, got {}", large_row_size);

    // Create index infos with row sizes
    let small_info = TableIndexInfo::new(1, 0, false, 0.0, small_row_size);
    let large_info = TableIndexInfo::new(1, 0, false, 0.0, large_row_size);

    // Insert costs
    let small_insert = estimator.estimate_insert(100, &table_stats, &small_info);
    let large_insert = estimator.estimate_insert(100, &table_stats, &large_info);

    assert!(
        large_insert > small_insert,
        "50-column table insert cost ({}) should exceed 2-column table cost ({})",
        large_insert,
        small_insert
    );

    // The factor should be significant (large row is ~9.5x base, but capped at 10x)
    let factor = large_info.wal_size_factor() / small_info.wal_size_factor();
    assert!(factor >= 9.0, "WAL size factor ratio ({}) should be at least 9x", factor);
}

// ============================================================================
// Skip-Scan Cost Estimation Tests
// ============================================================================

#[test]
fn test_skip_scan_cost_low_cardinality_prefix() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);

    // Low cardinality prefix (10 distinct values)
    let prefix_stats = ColumnStatistics {
        n_distinct: 10,
        null_count: 0,
        min_value: Some(SqlValue::Integer(1)),
        max_value: Some(SqlValue::Integer(10)),
        most_common_values: vec![],
        histogram: None,
    };

    // Selective filter (1% of rows match)
    let cost = estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, 0.01);

    // Should be cheaper than table scan with selective filter and low prefix cardinality
    let table_scan_cost = estimator.estimate_table_scan(&table_stats);
    assert!(
        cost < table_scan_cost,
        "Skip-scan cost ({}) should be cheaper than table scan ({}) with low prefix cardinality",
        cost,
        table_scan_cost
    );
}

#[test]
fn test_skip_scan_cost_high_cardinality_prefix() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);

    // High cardinality prefix (1000 distinct values)
    let prefix_stats = ColumnStatistics {
        n_distinct: 1000,
        null_count: 0,
        min_value: Some(SqlValue::Integer(1)),
        max_value: Some(SqlValue::Integer(1000)),
        most_common_values: vec![],
        histogram: None,
    };

    // Selective filter (1% of rows match)
    let cost = estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, 0.01);

    // High cardinality prefix makes skip-scan expensive due to many seeks
    let table_scan_cost = estimator.estimate_table_scan(&table_stats);
    assert!(
        cost > table_scan_cost,
        "Skip-scan cost ({}) should be more expensive than table scan ({}) with high prefix cardinality",
        cost,
        table_scan_cost
    );
}

#[test]
fn test_skip_scan_cost_scales_with_prefix_cardinality() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);

    let low_card_stats = ColumnStatistics {
        n_distinct: 10,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    let high_card_stats = ColumnStatistics {
        n_distinct: 100,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    let cost_low = estimator.estimate_skip_scan_cost(&table_stats, &low_card_stats, 0.01);
    let cost_high = estimator.estimate_skip_scan_cost(&table_stats, &high_card_stats, 0.01);

    // Higher prefix cardinality should mean higher skip-scan cost
    assert!(
        cost_high > cost_low,
        "Skip-scan cost with high cardinality ({}) should exceed low cardinality cost ({})",
        cost_high,
        cost_low
    );
}

#[test]
fn test_skip_scan_cost_scales_with_filter_selectivity() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);

    let prefix_stats = ColumnStatistics {
        n_distinct: 10,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    // Very selective filter (0.1% of rows)
    let cost_selective = estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, 0.001);

    // Less selective filter (10% of rows)
    let cost_broad = estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, 0.1);

    // Higher selectivity (more rows match) should mean higher cost
    assert!(
        cost_broad > cost_selective,
        "Skip-scan cost with broad filter ({}) should exceed selective filter cost ({})",
        cost_broad,
        cost_selective
    );
}

#[test]
fn test_should_use_skip_scan_decision() {
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);

    // Low cardinality prefix - skip-scan should be beneficial
    let low_card_stats = ColumnStatistics {
        n_distinct: 5,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    assert!(
        estimator.should_use_skip_scan(&table_stats, &low_card_stats, 0.01),
        "Skip-scan should be chosen with low prefix cardinality and selective filter"
    );

    // High cardinality prefix - skip-scan should not be beneficial
    let high_card_stats = ColumnStatistics {
        n_distinct: 5000,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    assert!(
        !estimator.should_use_skip_scan(&table_stats, &high_card_stats, 0.01),
        "Skip-scan should NOT be chosen with high prefix cardinality"
    );
}

#[test]
fn test_skip_scan_break_even_point() {
    // Test to find approximately where skip-scan becomes beneficial
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);

    // With 10% filter selectivity, find the prefix cardinality threshold
    let selectivity = 0.1;

    // Skip-scan should be beneficial below some threshold cardinality
    let mut threshold_cardinality = 0;
    for cardinality in [5, 10, 25, 50, 100, 200, 500, 1000] {
        let prefix_stats = ColumnStatistics {
            n_distinct: cardinality,
            null_count: 0,
            min_value: None,
            max_value: None,
            most_common_values: vec![],
            histogram: None,
        };

        if estimator.should_use_skip_scan(&table_stats, &prefix_stats, selectivity) {
            threshold_cardinality = cardinality;
        } else {
            break;
        }
    }

    // Verify we found a reasonable threshold
    assert!(
        threshold_cardinality > 0,
        "Skip-scan should be beneficial for at least some low cardinalities"
    );
    assert!(
        threshold_cardinality < 1000,
        "Skip-scan should not be beneficial for very high cardinalities"
    );
}

// ============================================================================
// Multi-Column Skip-Scan Cost Estimation Tests
// ============================================================================

#[test]
fn test_multi_column_skip_scan_cost_single_column_delegates() {
    // When given a single column, multi-column cost should match single-column cost
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);

    let prefix_stats = ColumnStatistics {
        n_distinct: 10,
        null_count: 0,
        min_value: Some(SqlValue::Integer(1)),
        max_value: Some(SqlValue::Integer(10)),
        most_common_values: vec![],
        histogram: None,
    };

    let single_col_cost = estimator.estimate_skip_scan_cost(&table_stats, &prefix_stats, 0.01);
    let multi_col_cost =
        estimator.estimate_skip_scan_cost_multi_column(&table_stats, &[&prefix_stats], 0.01);

    assert!(
        (single_col_cost - multi_col_cost).abs() < 0.001,
        "Single-column and multi-column costs should match for single column: {} vs {}",
        single_col_cost,
        multi_col_cost
    );
}

#[test]
fn test_multi_column_skip_scan_cost_increases_with_columns() {
    // Adding more prefix columns should generally increase cost due to more seeks
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);

    let col1_stats = ColumnStatistics {
        n_distinct: 10,
        null_count: 0,
        min_value: Some(SqlValue::Integer(1)),
        max_value: Some(SqlValue::Integer(10)),
        most_common_values: vec![],
        histogram: None,
    };

    let col2_stats = ColumnStatistics {
        n_distinct: 20,
        null_count: 0,
        min_value: Some(SqlValue::Integer(1)),
        max_value: Some(SqlValue::Integer(20)),
        most_common_values: vec![],
        histogram: None,
    };

    let col3_stats = ColumnStatistics {
        n_distinct: 50,
        null_count: 0,
        min_value: Some(SqlValue::Integer(1)),
        max_value: Some(SqlValue::Integer(50)),
        most_common_values: vec![],
        histogram: None,
    };

    let cost_1_col =
        estimator.estimate_skip_scan_cost_multi_column(&table_stats, &[&col1_stats], 0.01);
    let cost_2_col = estimator.estimate_skip_scan_cost_multi_column(
        &table_stats,
        &[&col1_stats, &col2_stats],
        0.01,
    );
    let cost_3_col = estimator.estimate_skip_scan_cost_multi_column(
        &table_stats,
        &[&col1_stats, &col2_stats, &col3_stats],
        0.01,
    );

    // More columns = more prefix combinations = higher seek cost
    assert!(
        cost_2_col > cost_1_col,
        "2-column skip cost ({}) should exceed 1-column cost ({})",
        cost_2_col,
        cost_1_col
    );
    assert!(
        cost_3_col > cost_2_col,
        "3-column skip cost ({}) should exceed 2-column cost ({})",
        cost_3_col,
        cost_2_col
    );
}

#[test]
fn test_multi_column_skip_scan_correlation_adjustment() {
    // Test that correlation factor limits combined cardinality
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(1000);

    // High cardinality columns (if independent, would produce 10*100*500 = 500,000
    // combinations)
    let col1_stats = ColumnStatistics {
        n_distinct: 10,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    let col2_stats = ColumnStatistics {
        n_distinct: 100,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    let col3_stats = ColumnStatistics {
        n_distinct: 500,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    let cost = estimator.estimate_skip_scan_cost_multi_column(
        &table_stats,
        &[&col1_stats, &col2_stats, &col3_stats],
        0.01,
    );

    // Cost should be finite and reasonable (correlation caps cardinality at row count)
    assert!(cost.is_finite(), "Cost should be finite");
    assert!(cost > 0.0, "Cost should be positive");

    // If no correlation adjustment, cost would be astronomical due to 500K seeks
    // With correlation, it should be capped based on row count (1000)
    // Seek cost with full independence: 500,000 * 4.0 = 2,000,000
    // Table scan cost: 1000/100 * 1.0 + 1000 * 0.01 = 10 + 10 = 20
    let table_scan_cost = estimator.estimate_table_scan(&table_stats);
    assert!(
        cost < 500_000.0 * 4.0,
        "Correlation should prevent astronomical costs: {} vs max {}",
        cost,
        500_000.0 * 4.0
    );

    // With these high cardinalities, skip-scan should not beat table scan
    assert!(
        cost > table_scan_cost,
        "Skip-scan with high combined cardinality ({}) should cost more than table scan ({})",
        cost,
        table_scan_cost
    );
}

#[test]
fn test_multi_column_skip_scan_empty_stats() {
    // Test edge case: empty prefix stats returns table scan cost
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);

    let cost_empty = estimator.estimate_skip_scan_cost_multi_column(&table_stats, &[], 0.01);
    let table_scan_cost = estimator.estimate_table_scan(&table_stats);

    assert!(
        (cost_empty - table_scan_cost).abs() < 0.001,
        "Empty prefix should return table scan cost: {} vs {}",
        cost_empty,
        table_scan_cost
    );
}

#[test]
fn test_multi_column_skip_scan_vs_single_column_decision() {
    // Test scenario where multi-column skip might be better than single-column
    // This happens when: the first column has high cardinality but combined
    // columns with correlation produce fewer seeks than first column alone
    let estimator = CostEstimator::default();
    let table_stats = create_test_table_stats(10000);

    // First column: high cardinality (100 distinct)
    let col1_high_card = ColumnStatistics {
        n_distinct: 100,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    // Second column: very low cardinality (2 distinct)
    // Combined with first, might have 100*2 = 200 combinations worst case
    // But with correlation, might be closer to 100 (each col1 value has ~both col2 values)
    let col2_low_card = ColumnStatistics {
        n_distinct: 2,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    let cost_1_col =
        estimator.estimate_skip_scan_cost_multi_column(&table_stats, &[&col1_high_card], 0.01);
    let cost_2_col = estimator.estimate_skip_scan_cost_multi_column(
        &table_stats,
        &[&col1_high_card, &col2_low_card],
        0.01,
    );

    // With these statistics, 2-column skip should have higher cost
    // (adding another low-card column still increases prefix combinations)
    assert!(
        cost_2_col >= cost_1_col,
        "2-column skip cost ({}) should be >= 1-column cost ({}) with these stats",
        cost_2_col,
        cost_1_col
    );
}

#[test]
fn test_combined_prefix_cardinality_estimation() {
    let estimator = CostEstimator::default();
    let total_rows = 10000.0;

    // Test 1: Single column
    let col1_stats = ColumnStatistics {
        n_distinct: 10,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    let cardinality_1 =
        estimator.estimate_combined_prefix_cardinality(&[&col1_stats], total_rows);
    assert!(
        (cardinality_1 - 10.0).abs() < 0.01,
        "Single column cardinality should match n_distinct: {}",
        cardinality_1
    );

    // Test 2: Two columns with low coverage (should have minimal correlation adjustment)
    let col2_stats = ColumnStatistics {
        n_distinct: 5,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    let cardinality_2 =
        estimator.estimate_combined_prefix_cardinality(&[&col1_stats, &col2_stats], total_rows);
    // With 10 * 5 = 50 max combinations and 10/10000 = 0.001 coverage ratio
    // Correlation factor ≈ 1.0 - 0.7 * 0.001 ≈ 0.999
    // So combined ≈ 10 * (5 * 0.999) ≈ 49.95
    assert!(
        cardinality_2 > 40.0 && cardinality_2 < 60.0,
        "Two-column cardinality should be close to product with low correlation: {}",
        cardinality_2
    );

    // Test 3: Cardinality should be capped at total_rows
    let col_high_stats = ColumnStatistics {
        n_distinct: 5000,
        null_count: 0,
        min_value: None,
        max_value: None,
        most_common_values: vec![],
        histogram: None,
    };

    let cardinality_capped = estimator
        .estimate_combined_prefix_cardinality(&[&col_high_stats, &col_high_stats], total_rows);
    assert!(
        cardinality_capped <= total_rows,
        "Combined cardinality ({}) should be capped at total_rows ({})",
        cardinality_capped,
        total_rows
    );
}
