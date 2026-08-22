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

/// Configuration for parallel execution decisions
#[derive(Debug, Clone, Copy)]
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
    ///
    /// Note: This uses OnceLock for the user-specified threshold override,
    /// but re-checks rayon::current_num_threads() each time to handle
    /// custom thread pools created after initial configuration.
    pub fn global() -> Self {
        // Cache the user threshold override (or None) - this is checked once
        static THRESHOLD_OVERRIDE: OnceLock<Option<ParallelThresholds>> = OnceLock::new();
        let override_thresholds = THRESHOLD_OVERRIDE.get_or_init(|| {
            std::env::var("PARALLEL_THRESHOLD").ok().map(|s| Self::parse_threshold_override(&s))
        });

        // Always check current thread count - this allows custom pools to work
        let num_threads = rayon::current_num_threads();

        let thresholds = match override_thresholds {
            Some(t) => *t,
            None => Self::thresholds_for_hardware(num_threads),
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
            .map(|i| {
                Row::from_vec(vec![
                    vibesql_types::SqlValue::Integer(i as i64),
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("row{}", i))),
                ])
            })
            .collect()
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
}
