//! Column-level statistics for selectivity estimation

use super::histogram::{BucketStrategy, Histogram};
use std::collections::HashMap;
use vibesql_types::SqlValue;

/// Statistics for a single column
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    /// Number of distinct values (cardinality)
    pub n_distinct: usize,

    /// Number of NULL values
    pub null_count: usize,

    /// Minimum value (for range queries)
    pub min_value: Option<SqlValue>,

    /// Maximum value (for range queries)
    pub max_value: Option<SqlValue>,

    /// Most common values with their frequencies (top 10)
    pub most_common_values: Vec<(SqlValue, f64)>,

    /// Optional histogram for improved selectivity estimation (Phase 5.1)
    pub histogram: Option<Histogram>,
}

impl ColumnStatistics {
    /// Compute statistics for a column by scanning all rows
    pub fn compute(rows: &[crate::Row], column_idx: usize) -> Self {
        Self::compute_with_histogram(rows, column_idx, false, 100, BucketStrategy::EqualDepth)
    }

    /// Compute statistics with optional histogram support
    ///
    /// # Arguments
    /// * `rows` - The rows to analyze
    /// * `column_idx` - Index of the column
    /// * `enable_histogram` - Whether to build a histogram
    /// * `num_buckets` - Number of histogram buckets (default: 100)
    /// * `bucket_strategy` - Histogram bucketing strategy
    pub fn compute_with_histogram(
        rows: &[crate::Row],
        column_idx: usize,
        enable_histogram: bool,
        num_buckets: usize,
        bucket_strategy: BucketStrategy,
    ) -> Self {
        let mut distinct_values = std::collections::HashSet::new();
        let mut null_count = 0;
        let mut min_value: Option<SqlValue> = None;
        let mut max_value: Option<SqlValue> = None;
        let mut value_counts: HashMap<SqlValue, usize> = HashMap::new();
        let mut non_null_values = Vec::new();

        for row in rows {
            if column_idx >= row.values.len() {
                continue;
            }

            let value = &row.values[column_idx];

            if value.is_null() {
                null_count += 1;
                continue;
            }

            distinct_values.insert(value.clone());
            *value_counts.entry(value.clone()).or_insert(0) += 1;
            non_null_values.push(value.clone());

            // Track min/max
            match (&min_value, &max_value) {
                (None, None) => {
                    min_value = Some(value.clone());
                    max_value = Some(value.clone());
                }
                (Some(min), Some(max)) => {
                    if value < min {
                        min_value = Some(value.clone());
                    }
                    if value > max {
                        max_value = Some(value.clone());
                    }
                }
                _ => unreachable!(),
            }
        }

        // Extract most common values (top 10)
        let total_non_null = rows.len() - null_count;
        let mut mcvs: Vec<_> = value_counts
            .into_iter()
            .map(|(val, count)| {
                let frequency =
                    if total_non_null > 0 { count as f64 / total_non_null as f64 } else { 0.0 };
                (val, frequency)
            })
            .collect();
        mcvs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        mcvs.truncate(10);

        // Build histogram if enabled and we have enough data
        let histogram = if enable_histogram && non_null_values.len() > 10 {
            Some(Histogram::create(&non_null_values, num_buckets, bucket_strategy))
        } else {
            None
        };

        ColumnStatistics {
            n_distinct: distinct_values.len(),
            null_count,
            min_value,
            max_value,
            most_common_values: mcvs,
            histogram,
        }
    }

    /// Estimate selectivity of equality predicate: col = value
    ///
    /// Returns fraction of rows expected to match (0.0 to 1.0)
    /// Uses histogram if available for improved accuracy (Phase 5.1)
    pub fn estimate_eq_selectivity(&self, value: &SqlValue) -> f64 {
        if value.is_null() {
            return 0.0; // NULLs don't match in SQL equality
        }

        // Check if value is in most common values
        if let Some((_, freq)) = self.most_common_values.iter().find(|(v, _)| v == value) {
            return *freq;
        }

        // Use histogram if available (Phase 5.1)
        if let Some(ref histogram) = self.histogram {
            return histogram.estimate_equality_selectivity(value);
        }

        // Fallback: assume uniform distribution for non-MCVs
        if self.n_distinct > 0 {
            1.0 / (self.n_distinct as f64)
        } else {
            0.0
        }
    }

    /// Estimate selectivity of inequality: col != value
    pub fn estimate_ne_selectivity(&self, value: &SqlValue) -> f64 {
        1.0 - self.estimate_eq_selectivity(value)
    }

    /// Estimate selectivity of range predicate: col > value or col < value
    ///
    /// Uses histogram if available (Phase 5.1), otherwise falls back to
    /// min/max-based linear interpolation (assumes uniform distribution)
    pub fn estimate_range_selectivity(&self, value: &SqlValue, operator: &str) -> f64 {
        // Use histogram if available for better accuracy (Phase 5.1)
        if let Some(ref histogram) = self.histogram {
            return histogram.estimate_range_selectivity(operator, value);
        }

        // Fallback to min/max-based estimation
        match (operator, &self.min_value, &self.max_value) {
            (">", Some(min), Some(max)) | (">=", Some(min), Some(max)) => {
                if value < min {
                    return 1.0; // All values satisfy
                }
                if value >= max {
                    return if operator == ">=" && value == max {
                        1.0 / (self.n_distinct as f64)
                    } else {
                        0.0
                    };
                }
                // Linear interpolation (rough estimate)
                0.33
            }
            ("<", Some(min), Some(max)) | ("<=", Some(min), Some(max)) => {
                if value > max {
                    return 1.0;
                }
                if value <= min {
                    return if operator == "<=" && value == min {
                        1.0 / (self.n_distinct as f64)
                    } else {
                        0.0
                    };
                }
                // Linear interpolation
                0.33
            }
            _ => 0.33, // Default fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Row;
    use vibesql_types::SqlValue;

    #[test]
    fn test_column_statistics_basic() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("a"))]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("b"))]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("a"))]),
            Row::new(vec![SqlValue::Integer(4), SqlValue::Null]),
        ];

        // Column 0 (integers)
        let stats = ColumnStatistics::compute(&rows, 0);
        assert_eq!(stats.n_distinct, 4);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_value, Some(SqlValue::Integer(1)));
        assert_eq!(stats.max_value, Some(SqlValue::Integer(4)));

        // Column 1 (text with duplicate 'a')
        let stats = ColumnStatistics::compute(&rows, 1);
        assert_eq!(stats.n_distinct, 2); // 'a' and 'b'
        assert_eq!(stats.null_count, 1);

        // Most common value should be 'a' (2/3 = 66.7%)
        assert_eq!(stats.most_common_values.len(), 2);
        assert_eq!(stats.most_common_values[0].0, SqlValue::Varchar(arcstr::ArcStr::from("a")));
        assert!((stats.most_common_values[0].1 - 0.667).abs() < 0.01);
    }

    #[test]
    fn test_selectivity_estimation() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Integer(3)]),
            Row::new(vec![SqlValue::Integer(4)]),
            Row::new(vec![SqlValue::Integer(5)]),
        ];

        let stats = ColumnStatistics::compute(&rows, 0);

        // Equality selectivity (uniform distribution)
        let sel = stats.estimate_eq_selectivity(&SqlValue::Integer(3));
        assert!((sel - 0.2).abs() < 0.01); // 1/5 = 20%

        // NULL selectivity
        let sel = stats.estimate_eq_selectivity(&SqlValue::Null);
        assert_eq!(sel, 0.0);
    }
}
