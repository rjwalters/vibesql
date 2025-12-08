//! Table-level statistics

use super::histogram::BucketStrategy;
use super::{ColumnStatistics, SampleMetadata, SamplingConfig};
use instant::SystemTime;
use rand::SeedableRng;
use std::collections::HashMap;

/// Statistics for an entire table
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Total number of rows
    pub row_count: usize,

    /// Per-column statistics
    pub columns: HashMap<String, ColumnStatistics>,

    /// Timestamp when stats were last updated
    pub last_updated: SystemTime,

    /// Whether stats are stale (need recomputation)
    pub is_stale: bool,

    /// Sampling metadata (Phase 5.2)
    /// None if no sampling was used (small table)
    pub sample_metadata: Option<SampleMetadata>,

    /// Average row size in bytes (computed from sampled data)
    ///
    /// This provides actual row size measurements that account for:
    /// - Real string/varchar fill ratios (not heuristic estimates)
    /// - Actual NULL prevalence
    /// - True BLOB/CLOB sizes
    ///
    /// Used by DML cost estimation to scale WAL write costs.
    /// None if statistics were estimated from schema (no actual data sampled).
    pub avg_row_bytes: Option<f64>,
}

impl TableStatistics {
    /// Create estimated statistics with basic column estimates
    ///
    /// This method provides reasonable defaults for column statistics without
    /// requiring a full ANALYZE scan. It uses data type information to generate
    /// basic statistics using conservative heuristics.
    ///
    /// # Heuristics Used
    /// - **Boolean columns**: n_distinct = 2
    /// - **Integer/Smallint/Bigint/Unsigned columns**: n_distinct = sqrt(row_count) (conservative)
    /// - **Float/Real/DoublePrecision columns**: n_distinct = sqrt(row_count) to 100 (high cardinality)
    /// - **Varchar/Character/Name columns**: n_distinct = row_count * 0.5 (assume moderate uniqueness)
    /// - **Date/Timestamp/Time columns**: n_distinct = row_count * 0.8 (high cardinality)
    /// - **Numeric/Decimal columns**: n_distinct = sqrt(row_count) (moderate)
    /// - **Nullable columns**: null_count ≈ row_count * 0.01 (1% estimated nulls)
    /// - **Non-nullable columns**: null_count = 0
    /// - **All columns**: is_stale = true (clearly marked as estimates)
    ///
    /// # Arguments
    /// * `row_count` - Total number of rows in the table
    /// * `schema` - Table schema with column definitions
    ///
    /// # Example
    /// ```text
    /// let stats = TableStatistics::estimate_from_schema(5000, &schema);
    /// // Boolean col: n_distinct = 2
    /// // Integer col: n_distinct = sqrt(5000) ≈ 70
    /// // Varchar col: n_distinct = 2500
    /// // All columns: is_stale = true
    /// ```
    pub fn estimate_from_schema(
        row_count: usize,
        schema: &vibesql_catalog::TableSchema,
    ) -> Self {
        use vibesql_types::DataType;

        let mut columns = std::collections::HashMap::new();

        for col in &schema.columns {
            let n_distinct = match &col.data_type {
                DataType::Boolean => 2,
                DataType::Integer | DataType::Smallint | DataType::Bigint | DataType::Unsigned => {
                    // Conservative: sqrt(row_count) - typically much less than actual cardinality
                    ((row_count as f64).sqrt() as usize).max(1)
                }
                DataType::Float { .. } | DataType::Real | DataType::DoublePrecision => {
                    // Floating point often has high cardinality
                    // Use sqrt but ensure at least 100 and at most row_count
                    let sqrt_count = (row_count as f64).sqrt() as usize;
                    sqrt_count.max(100).min(row_count)
                }
                DataType::Numeric { .. } | DataType::Decimal { .. } => {
                    // Numeric with precision/scale: moderate cardinality
                    ((row_count as f64).sqrt() as usize).max(1)
                }
                DataType::Varchar { .. }
                | DataType::Character { .. }
                | DataType::Name
                | DataType::CharacterLargeObject => {
                    // String columns: assume moderate uniqueness (50%)
                    ((row_count as f64 * 0.5) as usize).max(1)
                }
                DataType::Date | DataType::Timestamp { .. } | DataType::Time { .. } => {
                    // Temporal types: high cardinality
                    ((row_count as f64 * 0.8) as usize).max(1)
                }
                _ => {
                    // Other types: conservative estimate
                    ((row_count as f64).sqrt() as usize).max(1)
                }
            };

            // Estimate null fraction based on nullability
            let null_count = if col.nullable {
                // Estimate 1% nulls for nullable columns
                ((row_count as f64 * 0.01) as usize).max(0)
            } else {
                0
            };

            let col_stats = ColumnStatistics {
                n_distinct: n_distinct.max(1), // At least 1 distinct value
                null_count,
                min_value: None,    // No range info without scanning
                max_value: None,
                most_common_values: Vec::new(), // No MCVs without scanning
                histogram: None,    // No histogram without scanning
            };

            columns.insert(col.name.clone(), col_stats);
        }

        TableStatistics {
            row_count,
            columns,
            last_updated: SystemTime::now(),
            is_stale: true, // Clearly marked as estimates
            sample_metadata: None,
            avg_row_bytes: None, // No actual data sampled
        }
    }

    /// Compute statistics by scanning the table
    pub fn compute(rows: &[crate::Row], schema: &vibesql_catalog::TableSchema) -> Self {
        Self::compute_with_config(rows, schema, None, false, 100, BucketStrategy::EqualDepth)
    }

    /// Compute statistics with sampling (Phase 5.2) and histogram support (Phase 5.1)
    ///
    /// # Arguments
    /// * `rows` - All table rows
    /// * `schema` - Table schema
    /// * `sampling_config` - Optional sampling configuration (None = adaptive)
    /// * `enable_histograms` - Whether to build histograms
    /// * `histogram_buckets` - Number of histogram buckets
    /// * `bucket_strategy` - Histogram bucketing strategy
    pub fn compute_with_config(
        rows: &[crate::Row],
        schema: &vibesql_catalog::TableSchema,
        sampling_config: Option<SamplingConfig>,
        enable_histograms: bool,
        histogram_buckets: usize,
        bucket_strategy: BucketStrategy,
    ) -> Self {
        use super::sampling::sample_rows;
        let total_rows = rows.len();
        let config = sampling_config.unwrap_or_else(SamplingConfig::adaptive);

        // Determine if sampling is needed
        let (sample_size, should_sample) = config.determine_sample_size(total_rows);

        // Sample rows if needed (Phase 5.2)
        let mut rng = rand::rngs::StdRng::from_os_rng();
        let sampled_rows =
            if should_sample { sample_rows(rows, &config, &mut rng) } else { rows.to_vec() };

        // Create sample metadata
        let sample_metadata = if should_sample {
            Some(SampleMetadata::new(total_rows, sample_size, true, config.confidence_level))
        } else {
            None
        };

        // Compute column statistics on the sample
        let mut columns = HashMap::new();
        for (idx, column) in schema.columns.iter().enumerate() {
            let col_stats = ColumnStatistics::compute_with_histogram(
                &sampled_rows,
                idx,
                enable_histograms,
                histogram_buckets,
                bucket_strategy.clone(),
            );
            columns.insert(column.name.clone(), col_stats);
        }

        // Compute average row size from sampled data
        let avg_row_bytes = if sampled_rows.is_empty() {
            None
        } else {
            let total_bytes: usize =
                sampled_rows.iter().map(|row| row.estimated_size_bytes()).sum();
            Some(total_bytes as f64 / sampled_rows.len() as f64)
        };

        TableStatistics {
            row_count: total_rows,
            columns,
            last_updated: SystemTime::now(),
            is_stale: false,
            sample_metadata,
            avg_row_bytes,
        }
    }

    /// Compute statistics using adaptive sampling (Phase 5.2 convenience method)
    ///
    /// This automatically:
    /// - Uses full scan for small tables (< 1000 rows)
    /// - Uses 10% sample for medium tables (1K-100K rows)
    /// - Uses fixed 10K sample for large tables (> 100K rows)
    pub fn compute_sampled(rows: &[crate::Row], schema: &vibesql_catalog::TableSchema) -> Self {
        Self::compute_with_config(
            rows,
            schema,
            Some(SamplingConfig::adaptive()),
            false,
            100,
            BucketStrategy::EqualDepth,
        )
    }

    /// Compute statistics with both sampling and histograms enabled
    pub fn compute_full_featured(
        rows: &[crate::Row],
        schema: &vibesql_catalog::TableSchema,
    ) -> Self {
        Self::compute_with_config(
            rows,
            schema,
            Some(SamplingConfig::adaptive()),
            true, // Enable histograms
            100,  // 100 buckets
            BucketStrategy::EqualDepth,
        )
    }

    /// Create estimated statistics from table metadata without full ANALYZE
    ///
    /// This provides a fallback for cost estimation when detailed statistics
    /// aren't available (i.e., ANALYZE hasn't been run). It uses the table's
    /// row count and provides conservative defaults for other fields.
    ///
    /// # Use Cases
    /// - DML cost estimation when ANALYZE hasn't been run
    /// - Quick cost comparisons before detailed statistics are available
    ///
    /// # Limitations
    /// - No per-column statistics (empty columns map)
    /// - No histogram data
    /// - Marked as stale to indicate these are estimates
    ///
    /// # Example
    /// ```text
    /// let table_stats = table.get_statistics()
    ///     .cloned()
    ///     .unwrap_or_else(|| TableStatistics::estimate_from_row_count(table.row_count()));
    /// ```
    pub fn estimate_from_row_count(row_count: usize) -> Self {
        TableStatistics {
            row_count,
            columns: HashMap::new(), // No per-column stats without ANALYZE
            last_updated: SystemTime::now(),
            is_stale: true, // Mark as stale since these are estimates
            sample_metadata: None,
            avg_row_bytes: None, // No actual data sampled
        }
    }

    /// Mark statistics as stale after significant data changes
    pub fn mark_stale(&mut self) {
        self.is_stale = true;
    }

    /// Check if statistics should be recomputed
    ///
    /// Returns true if stats are marked stale or too old
    pub fn needs_refresh(&self) -> bool {
        self.is_stale
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Row;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::{DataType, SqlValue};

    #[test]
    fn test_table_statistics() {
        let schema = TableSchema::new(
            "test_table".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    true,
                ),
            ],
        );

        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
        ];

        let stats = TableStatistics::compute(&rows, &schema);

        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.columns.len(), 2);
        assert!(!stats.is_stale);

        // Check column stats
        let id_stats = stats.columns.get("id").unwrap();
        assert_eq!(id_stats.n_distinct, 3);

        let name_stats = stats.columns.get("name").unwrap();
        assert_eq!(name_stats.n_distinct, 2); // Alice, Bob
    }

    #[test]
    fn test_mark_stale() {
        let schema = TableSchema::new("test".to_string(), vec![]);

        let mut stats = TableStatistics::compute(&[], &schema);
        assert!(!stats.is_stale);
        assert!(!stats.needs_refresh());

        stats.mark_stale();
        assert!(stats.is_stale);
        assert!(stats.needs_refresh());
    }

    #[test]
    fn test_estimate_from_schema_basic() {
        let schema = TableSchema::new(
            "test_table".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    true,
                ),
                ColumnSchema::new("active".to_string(), DataType::Boolean, false),
            ],
        );

        let stats = TableStatistics::estimate_from_schema(1000, &schema);

        assert_eq!(stats.row_count, 1000);
        assert!(stats.is_stale); // Should be marked as estimates
        assert_eq!(stats.columns.len(), 3);

        // Check Integer column: n_distinct = sqrt(1000) ≈ 31
        let id_stats = stats.columns.get("id").unwrap();
        assert_eq!(id_stats.n_distinct, 31); // sqrt(1000) ≈ 31
        assert_eq!(id_stats.null_count, 0); // Non-nullable

        // Check Varchar column: n_distinct = 1000 * 0.5 = 500
        let name_stats = stats.columns.get("name").unwrap();
        assert_eq!(name_stats.n_distinct, 500); // Moderate uniqueness
        assert!(name_stats.null_count > 0); // Nullable, so ~1% nulls = ~10

        // Check Boolean column: n_distinct = 2
        let active_stats = stats.columns.get("active").unwrap();
        assert_eq!(active_stats.n_distinct, 2);
        assert_eq!(active_stats.null_count, 0); // Non-nullable
    }

    #[test]
    fn test_estimate_from_schema_various_types() {
        let schema = TableSchema::new(
            "test_table".to_string(),
            vec![
                ColumnSchema::new("bool_col".to_string(), DataType::Boolean, false),
                ColumnSchema::new("int_col".to_string(), DataType::Integer, false),
                ColumnSchema::new("float_col".to_string(), DataType::Float { precision: 24 }, false),
                ColumnSchema::new("date_col".to_string(), DataType::Date, false),
                ColumnSchema::new(
                    "nullable_col".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    true,
                ),
            ],
        );

        let stats = TableStatistics::estimate_from_schema(10000, &schema);

        // Boolean: 2 distinct
        assert_eq!(stats.columns.get("bool_col").unwrap().n_distinct, 2);

        // Integer: sqrt(10000) = 100
        assert_eq!(stats.columns.get("int_col").unwrap().n_distinct, 100);

        // Float: high cardinality (at least 100)
        let float_ndv = stats.columns.get("float_col").unwrap().n_distinct;
        assert!(float_ndv >= 100);

        // Date: high cardinality (80%)
        let date_ndv = stats.columns.get("date_col").unwrap().n_distinct;
        assert!(date_ndv > 5000);

        // Nullable: should have some null_count
        let nullable_stats = stats.columns.get("nullable_col").unwrap();
        assert!(nullable_stats.null_count > 0);
    }

    #[test]
    fn test_estimate_from_schema_empty_table() {
        let schema = TableSchema::new(
            "empty_table".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        let stats = TableStatistics::estimate_from_schema(0, &schema);
        assert_eq!(stats.row_count, 0);
        assert!(stats.is_stale);

        // Even with 0 rows, should have at least 1 distinct value estimate
        let id_stats = stats.columns.get("id").unwrap();
        assert!(id_stats.n_distinct >= 1);
    }

    #[test]
    fn test_estimate_from_row_count() {
        // Test the fallback statistics method
        let stats = TableStatistics::estimate_from_row_count(1000);

        assert_eq!(stats.row_count, 1000);
        assert!(stats.columns.is_empty()); // No per-column stats without ANALYZE
        assert!(stats.is_stale); // Marked as stale since these are estimates
        assert!(stats.sample_metadata.is_none());
        assert!(stats.needs_refresh()); // Should indicate refresh is needed
    }

    #[test]
    fn test_estimate_from_row_count_zero_rows() {
        // Test with empty table
        let stats = TableStatistics::estimate_from_row_count(0);

        assert_eq!(stats.row_count, 0);
        assert!(stats.is_stale);
    }

    // ============================================================================
    // avg_row_bytes Tests (Issue #3980)
    // ============================================================================

    #[test]
    fn test_avg_row_bytes_computed_from_actual_data() {
        let schema = TableSchema::new(
            "test_table".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
        );

        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))]),
        ];

        let stats = TableStatistics::compute(&rows, &schema);

        // avg_row_bytes should be computed from actual data
        assert!(stats.avg_row_bytes.is_some());
        let avg_bytes = stats.avg_row_bytes.unwrap();
        // Should be positive and reasonable (Row struct + values)
        assert!(avg_bytes > 0.0, "avg_row_bytes should be positive: {}", avg_bytes);
    }

    #[test]
    fn test_avg_row_bytes_none_for_schema_estimates() {
        let schema = TableSchema::new(
            "test_table".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
        );

        // estimate_from_schema should NOT have avg_row_bytes (no actual data)
        let stats = TableStatistics::estimate_from_schema(1000, &schema);
        assert!(
            stats.avg_row_bytes.is_none(),
            "estimate_from_schema should not have avg_row_bytes"
        );

        // estimate_from_row_count should NOT have avg_row_bytes
        let stats = TableStatistics::estimate_from_row_count(1000);
        assert!(
            stats.avg_row_bytes.is_none(),
            "estimate_from_row_count should not have avg_row_bytes"
        );
    }

    #[test]
    fn test_avg_row_bytes_none_for_empty_table() {
        let schema = TableSchema::new(
            "empty_table".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );

        // Empty table should have avg_row_bytes = None
        let stats = TableStatistics::compute(&[], &schema);
        assert!(
            stats.avg_row_bytes.is_none(),
            "Empty table should have avg_row_bytes = None"
        );
    }

    #[test]
    fn test_avg_row_bytes_varies_with_string_length() {
        let schema = TableSchema::new(
            "test_table".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "data".to_string(),
                    DataType::Varchar { max_length: Some(1000) },
                    false,
                ),
            ],
        );

        // Short strings
        let short_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("a"))]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("b"))]),
        ];
        let short_stats = TableStatistics::compute(&short_rows, &schema);

        // Long strings
        let long_string = "x".repeat(500);
        let long_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from(&long_string))]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from(&long_string))]),
        ];
        let long_stats = TableStatistics::compute(&long_rows, &schema);

        // Long strings should result in larger avg_row_bytes
        let short_avg = short_stats.avg_row_bytes.unwrap();
        let long_avg = long_stats.avg_row_bytes.unwrap();
        assert!(
            long_avg > short_avg,
            "Long strings ({}) should have larger avg_row_bytes than short strings ({})",
            long_avg,
            short_avg
        );
    }
}
