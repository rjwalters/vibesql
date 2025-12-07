//! Parallel execution heuristics and configuration
//!
//! This module provides intelligent, hardware-aware parallelism that automatically
//! determines when to use parallel execution based on:
//! - Available CPU cores
//! - Row count
//! - Operation type
//! - User overrides via PARALLEL_THRESHOLD environment variable
//!
//! Note: This module is only compiled when the `parallel` feature is enabled.
//! In WASM builds, this feature is disabled as parallelism provides no benefit.

use std::sync::OnceLock;

/// Global parallel configuration, initialized once on first access
static PARALLEL_CONFIG: OnceLock<ParallelConfig> = OnceLock::new();

/// Configuration for parallel execution decisions
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Number of threads available (from rayon)
    #[allow(dead_code)]
    pub num_threads: usize,
    /// Thresholds for different operations based on hardware tier
    pub thresholds: ParallelThresholds,
}

/// Operation-specific row count thresholds for parallel execution
#[derive(Debug, Clone, Copy)]
pub struct ParallelThresholds {
    /// Threshold for scan/filter operations
    pub scan_filter: usize,
    /// Threshold for aggregation operations
    pub aggregate: usize,
    /// Threshold for join operations
    pub join: usize,
    /// Threshold for sort operations
    pub sort: usize,
}

impl ParallelConfig {
    /// Get or initialize the global parallel configuration
    pub fn global() -> &'static ParallelConfig {
        PARALLEL_CONFIG.get_or_init(Self::detect)
    }

    /// Detect hardware capabilities and create appropriate configuration
    fn detect() -> Self {
        let num_threads = rayon::current_num_threads();

        // Check for user override of threshold
        let thresholds = if let Ok(threshold_str) = std::env::var("PARALLEL_THRESHOLD") {
            Self::parse_threshold_override(&threshold_str)
        } else {
            // Auto-detect based on hardware
            Self::thresholds_for_hardware(num_threads)
        };

        ParallelConfig { num_threads, thresholds }
    }

    /// Parse PARALLEL_THRESHOLD environment variable
    /// Supports:
    /// - Numbers: "5000" -> custom threshold
    /// - "max" or "disabled" -> effectively disable parallelism
    fn parse_threshold_override(threshold_str: &str) -> ParallelThresholds {
        let threshold_str = threshold_str.trim().to_lowercase();

        if threshold_str == "max" || threshold_str == "disabled" {
            // Effectively disable by setting impossibly high threshold
            ParallelThresholds {
                scan_filter: usize::MAX,
                aggregate: usize::MAX,
                join: usize::MAX,
                sort: usize::MAX,
            }
        } else if let Ok(threshold) = threshold_str.parse::<usize>() {
            // Use custom threshold for all operations
            ParallelThresholds {
                scan_filter: threshold,
                aggregate: threshold,
                join: threshold,
                sort: threshold,
            }
        } else {
            // Invalid value, fall back to auto-detection
            Self::thresholds_for_hardware(rayon::current_num_threads())
        }
    }

    /// Determine appropriate thresholds based on hardware tier
    fn thresholds_for_hardware(num_threads: usize) -> ParallelThresholds {
        match num_threads {
            // Single core: never parallelize
            1 => ParallelThresholds {
                scan_filter: usize::MAX,
                aggregate: usize::MAX,
                join: usize::MAX,
                sort: usize::MAX,
            },
            // 2-3 cores: very conservative (most overhead from parallel coordination)
            2..=3 => ParallelThresholds {
                scan_filter: 10_000,
                aggregate: 12_500,
                join: 15_000,
                sort: 15_000,
            },
            // 4-7 cores: moderate thresholds - lowered for better 100-1000 row performance
            4..=7 => ParallelThresholds {
                scan_filter: 2_500,
                aggregate: 3_750,
                join: 5_000,
                sort: 5_000,
            },
            // 8+ cores: aggressive thresholds - lowered significantly for 100-1000 row datasets
            // Modern multi-core hardware benefits from earlier parallelization
            _ => ParallelThresholds {
                scan_filter: 1_000,
                aggregate: 1_500,
                join: 2_500,
                sort: 2_000,
            },
        }
    }

    /// Check if parallel execution should be used for a scan/filter operation
    pub fn should_parallelize_scan(&self, row_count: usize) -> bool {
        row_count >= self.thresholds.scan_filter
    }

    /// Check if parallel execution should be used for an aggregation operation
    #[allow(dead_code)]
    pub fn should_parallelize_aggregate(&self, row_count: usize) -> bool {
        row_count >= self.thresholds.aggregate
    }

    /// Check if parallel execution should be used for a join operation
    #[allow(dead_code)]
    pub fn should_parallelize_join(&self, row_count: usize) -> bool {
        row_count >= self.thresholds.join
    }

    /// Check if parallel execution should be used for a sort operation
    #[allow(dead_code)]
    pub fn should_parallelize_sort(&self, row_count: usize) -> bool {
        row_count >= self.thresholds.sort
    }
}

//
// Parallel Scan Operations
//

use rayon::prelude::*;
use vibesql_storage::Row;

/// Parallel scan with filtering predicate.
///
/// Applies a predicate function to each row and returns only those rows
/// where the predicate returns `true`. Automatically uses parallel execution
/// when beneficial based on hardware capabilities and row count.
///
/// # Arguments
/// * `rows` - Input rows to scan
/// * `predicate` - Function to test each row (returns true to keep, false to filter out)
///
/// # Returns
/// Vector of rows that satisfy the predicate
///
/// # Example
/// ```text
/// // Filter rows where column 0 > 100
/// let filtered = parallel_scan_filter(rows, |row| {
///     matches!(row.values[0], SqlValue::Integer(x) if x > 100)
/// });
/// ```
#[allow(dead_code)]
pub fn parallel_scan_filter<F>(rows: &[Row], predicate: F) -> Vec<Row>
where
    F: Fn(&Row) -> bool + Sync + Send,
{
    let config = ParallelConfig::global();

    if config.should_parallelize_scan(rows.len()) {
        // Parallel path: use rayon for efficient parallel filtering
        rows.par_iter().filter(|row| predicate(row)).cloned().collect()
    } else {
        // Sequential fallback for small datasets
        rows.iter().filter(|row| predicate(row)).cloned().collect()
    }
}

/// Parallel scan with transformation function.
///
/// Applies a transformation function to each row. Can be used for projection
/// (selecting specific columns) or other row transformations. Automatically
/// uses parallel execution when beneficial.
///
/// # Arguments
/// * `rows` - Input rows to scan
/// * `transform` - Function to transform each row (must not panic or return Err)
///
/// # Returns
/// Vector of transformed rows
///
/// # Example
/// ```text
/// // Project first two columns only
/// let projected = parallel_scan_map(rows, |row| {
///     Row {
///         values: row.values[..2].to_vec()
///     }
/// });
/// ```
#[allow(dead_code)]
pub fn parallel_scan_map<F>(rows: &[Row], transform: F) -> Vec<Row>
where
    F: Fn(&Row) -> Row + Sync + Send,
{
    let config = ParallelConfig::global();

    if config.should_parallelize_scan(rows.len()) {
        // Parallel path: use rayon for efficient parallel mapping
        rows.par_iter().map(&transform).collect()
    } else {
        // Sequential fallback for small datasets
        rows.iter().map(transform).collect()
    }
}

/// Parallel scan with filter-map for combined filtering and transformation.
///
/// Applies a function that can both filter (return None) and transform (return Some).
/// This is more efficient than separate filter + map operations.
///
/// # Arguments
/// * `rows` - Input rows to scan
/// * `filter_map` - Function that returns Some(row) to keep/transform, None to filter out
///
/// # Returns
/// Vector of transformed rows (where function returned Some)
///
/// # Example
/// ```text
/// // Filter AND project in one pass
/// let result = parallel_scan_filter_map(rows, |row| {
///     if matches!(row.values[0], SqlValue::Integer(x) if x > 100) {
///         Some(Row { values: row.values[..2].to_vec() })
///     } else {
///         None
///     }
/// });
/// ```
#[allow(dead_code)]
pub fn parallel_scan_filter_map<F>(rows: &[Row], filter_map: F) -> Vec<Row>
where
    F: Fn(&Row) -> Option<Row> + Sync + Send,
{
    let config = ParallelConfig::global();

    if config.should_parallelize_scan(rows.len()) {
        // Parallel path: use rayon for efficient parallel filter-map
        rows.par_iter().filter_map(&filter_map).collect()
    } else {
        // Sequential fallback for small datasets
        rows.iter().filter_map(filter_map).collect()
    }
}

/// Parallel scan that simply materializes a slice into a Vec.
///
/// When row count exceeds threshold, uses parallel copying for better
/// memory bandwidth utilization on large datasets.
///
/// # Arguments
/// * `rows` - Input row slice to materialize
///
/// # Returns
/// Vector containing clones of all input rows
pub fn parallel_scan_materialize(rows: &[Row]) -> Vec<Row> {
    let config = ParallelConfig::global();

    if config.should_parallelize_scan(rows.len()) {
        // Parallel path: use rayon for efficient parallel cloning
        rows.par_iter().cloned().collect()
    } else {
        // Sequential fallback for small datasets
        rows.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_detection() {
        let config = ParallelConfig::detect();
        // Should detect at least 1 thread
        assert!(config.num_threads >= 1);
    }

    #[test]
    fn test_threshold_override_custom_value() {
        // Test the parse function directly to avoid global state issues
        let thresholds = ParallelConfig::parse_threshold_override("5000");
        assert_eq!(thresholds.scan_filter, 5000);
        assert_eq!(thresholds.aggregate, 5000);
    }

    #[test]
    fn test_threshold_override_max() {
        // Test the parse function directly to avoid global state issues
        let thresholds = ParallelConfig::parse_threshold_override("max");
        assert_eq!(thresholds.scan_filter, usize::MAX);
        assert_eq!(thresholds.aggregate, usize::MAX);
    }

    #[test]
    fn test_threshold_override_disabled() {
        // Test the parse function directly to avoid global state issues
        let thresholds = ParallelConfig::parse_threshold_override("disabled");
        assert_eq!(thresholds.scan_filter, usize::MAX);
    }

    #[test]
    fn test_single_core_never_parallelizes() {
        let thresholds = ParallelConfig::thresholds_for_hardware(1);
        assert_eq!(thresholds.scan_filter, usize::MAX);
    }

    #[test]
    fn test_conservative_thresholds_2_3_cores() {
        let thresholds = ParallelConfig::thresholds_for_hardware(2);
        assert_eq!(thresholds.scan_filter, 10_000);

        let thresholds = ParallelConfig::thresholds_for_hardware(3);
        assert_eq!(thresholds.scan_filter, 10_000);
    }

    #[test]
    fn test_moderate_thresholds_4_7_cores() {
        let thresholds = ParallelConfig::thresholds_for_hardware(4);
        assert_eq!(thresholds.scan_filter, 2_500);

        let thresholds = ParallelConfig::thresholds_for_hardware(7);
        assert_eq!(thresholds.scan_filter, 2_500);
    }

    #[test]
    fn test_aggressive_thresholds_8_plus_cores() {
        let thresholds = ParallelConfig::thresholds_for_hardware(8);
        assert_eq!(thresholds.scan_filter, 1_000);

        let thresholds = ParallelConfig::thresholds_for_hardware(16);
        assert_eq!(thresholds.scan_filter, 1_000);
    }

    #[test]
    fn test_should_parallelize_scan() {
        // Simulate 8+ core system
        let config = ParallelConfig {
            num_threads: 8,
            thresholds: ParallelConfig::thresholds_for_hardware(8),
        };

        // Below threshold
        assert!(!config.should_parallelize_scan(500));

        // At threshold
        assert!(config.should_parallelize_scan(1_000));

        // Above threshold
        assert!(config.should_parallelize_scan(10_000));
    }

    #[test]
    fn test_invalid_threshold_override_falls_back_to_auto() {
        // Test the parse function directly to avoid global state issues
        let thresholds = ParallelConfig::parse_threshold_override("invalid");

        // Should fall back to auto-detection based on current hardware
        let auto_thresholds = ParallelConfig::thresholds_for_hardware(rayon::current_num_threads());
        assert_eq!(thresholds.scan_filter, auto_thresholds.scan_filter);
    }

    // Tests for parallel scan functions

    fn create_test_rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|i| Row::from_vec(vec![
                    vibesql_types::SqlValue::Integer(i as i64),
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("row{}", i))),
                ]))
            .collect()
    }

    #[test]
    fn test_parallel_scan_filter() {
        let rows = create_test_rows(100);

        // Filter for even numbers
        let filtered = parallel_scan_filter(
            &rows,
            |row| matches!(row.values[0], vibesql_types::SqlValue::Integer(x) if x % 2 == 0),
        );

        // Should have 50 even numbers (0, 2, 4, ..., 98)
        assert_eq!(filtered.len(), 50);

        // Verify first few results
        assert!(matches!(filtered[0].values[0], vibesql_types::SqlValue::Integer(0)));
        assert!(matches!(filtered[1].values[0], vibesql_types::SqlValue::Integer(2)));
        assert!(matches!(filtered[2].values[0], vibesql_types::SqlValue::Integer(4)));
    }

    #[test]
    fn test_parallel_scan_filter_empty() {
        let rows = create_test_rows(100);

        // Filter that matches nothing
        let filtered = parallel_scan_filter(&rows, |_| false);

        assert_eq!(filtered.len(), 0);
    }

    #[test]
    fn test_parallel_scan_filter_all() {
        let rows = create_test_rows(100);

        // Filter that matches everything
        let filtered = parallel_scan_filter(&rows, |_| true);

        assert_eq!(filtered.len(), 100);
    }

    #[test]
    fn test_parallel_scan_map() {
        let rows = create_test_rows(10);

        // Double all integers
        let transformed = parallel_scan_map(&rows, |row| {
            let mut new_row = row.clone();
            if let vibesql_types::SqlValue::Integer(x) = row.values[0] {
                new_row.values[0] = vibesql_types::SqlValue::Integer(x * 2);
            }
            new_row
        });

        assert_eq!(transformed.len(), 10);
        assert!(matches!(transformed[0].values[0], vibesql_types::SqlValue::Integer(0)));
        assert!(matches!(transformed[1].values[0], vibesql_types::SqlValue::Integer(2)));
        assert!(matches!(transformed[5].values[0], vibesql_types::SqlValue::Integer(10)));
    }

    #[test]
    fn test_parallel_scan_filter_map() {
        let rows = create_test_rows(100);

        // Filter for even numbers AND double them
        let result = parallel_scan_filter_map(&rows, |row| {
            if let vibesql_types::SqlValue::Integer(x) = row.values[0] {
                if x % 2 == 0 {
                    let mut new_row = row.clone();
                    new_row.values[0] = vibesql_types::SqlValue::Integer(x * 2);
                    return Some(new_row);
                }
            }
            None
        });

        // Should have 50 even numbers, doubled
        assert_eq!(result.len(), 50);
        assert!(matches!(result[0].values[0], vibesql_types::SqlValue::Integer(0)));
        assert!(matches!(result[1].values[0], vibesql_types::SqlValue::Integer(4))); // 2*2
        assert!(matches!(result[2].values[0], vibesql_types::SqlValue::Integer(8)));
        // 4*2
    }

    #[test]
    fn test_parallel_scan_materialize() {
        let rows = create_test_rows(50);

        let materialized = parallel_scan_materialize(&rows);

        assert_eq!(materialized.len(), 50);
        // Verify content is identical
        for (i, row) in materialized.iter().enumerate() {
            assert!(matches!(row.values[0], vibesql_types::SqlValue::Integer(x) if x == i as i64));
        }
    }

    #[test]
    fn test_parallel_scan_small_dataset_uses_sequential() {
        // With small dataset, should use sequential path
        // (This is implicit in the implementation, but we test correctness)
        let rows = create_test_rows(10); // Well below any threshold

        let filtered = parallel_scan_filter(
            &rows,
            |row| matches!(row.values[0], vibesql_types::SqlValue::Integer(x) if x < 5),
        );

        assert_eq!(filtered.len(), 5);
    }

    #[test]
    fn test_parallel_scan_preserves_row_structure() {
        let rows = vec![
            Row::from_vec(vec![
                    vibesql_types::SqlValue::Integer(1),
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("test")),
                    vibesql_types::SqlValue::Null,
                ]),
            Row::from_vec(vec![
                    vibesql_types::SqlValue::Integer(2),
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("test2")),
                    vibesql_types::SqlValue::Double(3.5),
                ]),
        ];

        let filtered = parallel_scan_filter(&rows, |_| true);

        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].values.len(), 3);
        assert_eq!(filtered[1].values.len(), 3);
        assert!(matches!(filtered[0].values[2], vibesql_types::SqlValue::Null));
        assert!(matches!(filtered[1].values[2], vibesql_types::SqlValue::Double(x) if x == 3.5));
    }
}
