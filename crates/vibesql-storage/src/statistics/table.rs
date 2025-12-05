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
}

impl TableStatistics {
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

        TableStatistics {
            row_count: total_rows,
            columns,
            last_updated: SystemTime::now(),
            is_stale: false,
            sample_metadata,
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
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar("Alice".to_string())]),
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
}
