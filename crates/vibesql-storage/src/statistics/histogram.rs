//! Histogram-based selectivity estimation for non-uniform distributions
//!
//! Histograms provide more accurate selectivity estimates than assuming
//! uniform distribution, especially for skewed data.

use vibesql_types::SqlValue;

/// Histogram for tracking value distribution
#[derive(Debug, Clone)]
pub struct Histogram {
    /// Histogram buckets
    pub buckets: Vec<HistogramBucket>,

    /// Strategy used to create buckets
    pub bucket_strategy: BucketStrategy,

    /// Total number of rows represented
    pub total_rows: usize,
}

/// A single bucket in the histogram
#[derive(Debug, Clone)]
pub struct HistogramBucket {
    /// Lower bound of this bucket (inclusive)
    pub lower_bound: SqlValue,

    /// Upper bound of this bucket (inclusive)
    pub upper_bound: SqlValue,

    /// Fraction of rows in this bucket (0.0 to 1.0)
    pub frequency: f64,

    /// Number of distinct values within this bucket
    pub distinct_count: usize,
}

/// Strategy for dividing data into histogram buckets
#[derive(Debug, Clone, PartialEq)]
pub enum BucketStrategy {
    /// Equal-width buckets: each bucket covers same value range
    /// Good for uniformly distributed data
    EqualWidth,

    /// Equal-depth buckets: each bucket contains same number of rows
    /// Good for skewed data
    EqualDepth,

    /// Hybrid: mix of equal-width and equal-depth
    /// Balances accuracy for both uniform and skewed data
    Hybrid,
}

impl Histogram {
    /// Create a histogram from sample data
    ///
    /// # Arguments
    /// * `values` - Sample values (should not include NULLs)
    /// * `num_buckets` - Number of buckets to create (default: 100)
    /// * `strategy` - Bucketing strategy to use
    pub fn create(values: &[SqlValue], num_buckets: usize, strategy: BucketStrategy) -> Self {
        if values.is_empty() {
            return Self { buckets: Vec::new(), bucket_strategy: strategy, total_rows: 0 };
        }

        let buckets = match strategy {
            BucketStrategy::EqualWidth => Self::create_equal_width_buckets(values, num_buckets),
            BucketStrategy::EqualDepth => Self::create_equal_depth_buckets(values, num_buckets),
            BucketStrategy::Hybrid => Self::create_hybrid_buckets(values, num_buckets),
        };

        Self { buckets, bucket_strategy: strategy, total_rows: values.len() }
    }

    /// Create equal-width buckets
    fn create_equal_width_buckets(values: &[SqlValue], num_buckets: usize) -> Vec<HistogramBucket> {
        if values.is_empty() || num_buckets == 0 {
            return Vec::new();
        }

        let mut sorted_values = values.to_vec();
        sorted_values.sort();

        let min_val = &sorted_values[0];
        let max_val = &sorted_values[sorted_values.len() - 1];

        // Can't create equal-width buckets if all values are the same
        if min_val == max_val {
            return vec![HistogramBucket {
                lower_bound: min_val.clone(),
                upper_bound: max_val.clone(),
                frequency: 1.0,
                distinct_count: 1,
            }];
        }

        // For numeric types, we can calculate bucket boundaries
        // For now, we'll use a simplified approach with equal-depth as fallback
        Self::create_equal_depth_buckets(values, num_buckets)
    }

    /// Create equal-depth buckets (each bucket has roughly equal number of rows)
    fn create_equal_depth_buckets(values: &[SqlValue], num_buckets: usize) -> Vec<HistogramBucket> {
        if values.is_empty() || num_buckets == 0 {
            return Vec::new();
        }

        let mut sorted_values = values.to_vec();
        sorted_values.sort();

        // Special case: if all values are the same, create a single bucket
        if sorted_values[0] == sorted_values[sorted_values.len() - 1] {
            return vec![HistogramBucket {
                lower_bound: sorted_values[0].clone(),
                upper_bound: sorted_values[0].clone(),
                frequency: 1.0,
                distinct_count: 1,
            }];
        }

        let target_bucket_size = (values.len() as f64 / num_buckets as f64).ceil() as usize;
        let mut buckets = Vec::new();

        let mut i = 0;
        while i < sorted_values.len() {
            let start_idx = i;
            let end_idx = (i + target_bucket_size).min(sorted_values.len());

            let lower_bound = sorted_values[start_idx].clone();
            let upper_bound = sorted_values[end_idx - 1].clone();

            // Count distinct values in this range
            let mut distinct_values = std::collections::HashSet::new();
            for value in &sorted_values[start_idx..end_idx] {
                distinct_values.insert(value.clone());
            }

            let bucket_rows = end_idx - start_idx;
            let frequency = bucket_rows as f64 / values.len() as f64;

            buckets.push(HistogramBucket {
                lower_bound,
                upper_bound,
                frequency,
                distinct_count: distinct_values.len(),
            });

            i = end_idx;
        }

        buckets
    }

    /// Create hybrid buckets (combination of equal-width and equal-depth)
    fn create_hybrid_buckets(values: &[SqlValue], num_buckets: usize) -> Vec<HistogramBucket> {
        // For now, use equal-depth as it works better for skewed data
        // In the future, we could detect skew and choose strategy accordingly
        Self::create_equal_depth_buckets(values, num_buckets)
    }

    /// Estimate selectivity for equality predicate: col = value
    pub fn estimate_equality_selectivity(&self, value: &SqlValue) -> f64 {
        if self.buckets.is_empty() {
            return 0.0;
        }

        // Find the bucket containing this value
        for bucket in &self.buckets {
            if value >= &bucket.lower_bound && value <= &bucket.upper_bound {
                // Assume uniform distribution within the bucket
                if bucket.distinct_count > 0 {
                    return bucket.frequency / bucket.distinct_count as f64;
                }
            }
        }

        // Value not in any bucket
        0.0
    }

    /// Estimate selectivity for range predicate: col > value, col >= value, col < value, col <= value
    pub fn estimate_range_selectivity(&self, operator: &str, value: &SqlValue) -> f64 {
        if self.buckets.is_empty() {
            return 0.0;
        }

        let mut selectivity = 0.0;

        match operator {
            ">" | ">=" => {
                // Sum frequency of all buckets above the value
                for bucket in &self.buckets {
                    if &bucket.lower_bound > value {
                        // Entire bucket is above the value
                        selectivity += bucket.frequency;
                    } else if &bucket.upper_bound >= value {
                        // Value is within this bucket, estimate partial selectivity
                        // Assume uniform distribution within bucket
                        let partial = if operator == ">" {
                            // Exclude the equality case
                            bucket.frequency * 0.5 // Rough estimate
                        } else {
                            // Include the equality case
                            bucket.frequency * 0.5 + self.estimate_equality_selectivity(value)
                        };
                        selectivity += partial;
                    }
                }
            }
            "<" | "<=" => {
                // Sum frequency of all buckets below the value
                for bucket in &self.buckets {
                    if &bucket.upper_bound < value {
                        // Entire bucket is below the value
                        selectivity += bucket.frequency;
                    } else if &bucket.lower_bound <= value {
                        // Value is within this bucket, estimate partial selectivity
                        let partial = if operator == "<" {
                            // Exclude the equality case
                            bucket.frequency * 0.5
                        } else {
                            // Include the equality case
                            bucket.frequency * 0.5 + self.estimate_equality_selectivity(value)
                        };
                        selectivity += partial;
                    }
                }
            }
            _ => return 0.33, // Default fallback
        }

        selectivity.clamp(0.0, 1.0)
    }

    /// Estimate selectivity for BETWEEN predicate: col BETWEEN lower AND upper
    pub fn estimate_between_selectivity(&self, lower: &SqlValue, upper: &SqlValue) -> f64 {
        if self.buckets.is_empty() {
            return 0.0;
        }

        let mut selectivity = 0.0;

        for bucket in &self.buckets {
            // Check if bucket overlaps with [lower, upper] range
            if &bucket.upper_bound < lower || &bucket.lower_bound > upper {
                // No overlap
                continue;
            }

            if &bucket.lower_bound >= lower && &bucket.upper_bound <= upper {
                // Bucket is fully contained in range
                selectivity += bucket.frequency;
            } else {
                // Partial overlap - estimate fraction
                // This is a simplified estimate; more sophisticated logic
                // would calculate exact overlap based on value distribution
                selectivity += bucket.frequency * 0.5;
            }
        }

        selectivity.clamp(0.0, 1.0)
    }

    /// Get the number of buckets
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Check if histogram is empty
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    #[test]
    fn test_histogram_equal_depth() {
        // Create skewed data: many 1s, some 2s, few 3s
        let mut values = Vec::new();
        for _ in 0..70 {
            values.push(SqlValue::Integer(1));
        }
        for _ in 0..20 {
            values.push(SqlValue::Integer(2));
        }
        for _ in 0..10 {
            values.push(SqlValue::Integer(3));
        }

        let histogram = Histogram::create(&values, 3, BucketStrategy::EqualDepth);

        assert_eq!(histogram.bucket_count(), 3);
        assert_eq!(histogram.total_rows, 100);

        // Each bucket should have roughly equal frequency (33.3%)
        for bucket in &histogram.buckets {
            assert!((bucket.frequency - 0.33).abs() < 0.1);
        }
    }

    #[test]
    fn test_histogram_equality_selectivity() {
        let values = vec![
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Integer(1),
            SqlValue::Integer(2),
            SqlValue::Integer(3),
        ];

        let histogram = Histogram::create(&values, 2, BucketStrategy::EqualDepth);

        // Estimate selectivity for value 1 (appears 3 times out of 5)
        let sel = histogram.estimate_equality_selectivity(&SqlValue::Integer(1));

        // Should be roughly 60% (3/5), but depends on bucketing
        assert!(sel > 0.0);
        assert!(sel <= 1.0);
    }

    #[test]
    fn test_histogram_range_selectivity() {
        let values: Vec<SqlValue> = (1..=100).map(SqlValue::Integer).collect();

        let histogram = Histogram::create(&values, 10, BucketStrategy::EqualDepth);

        // Estimate selectivity for values > 50
        let sel = histogram.estimate_range_selectivity(">", &SqlValue::Integer(50));

        // Should be roughly 50%
        assert!((sel - 0.5).abs() < 0.2);
    }

    #[test]
    fn test_histogram_between_selectivity() {
        let values: Vec<SqlValue> = (1..=100).map(SqlValue::Integer).collect();

        let histogram = Histogram::create(&values, 10, BucketStrategy::EqualDepth);

        // Estimate selectivity for BETWEEN 25 AND 75
        let sel =
            histogram.estimate_between_selectivity(&SqlValue::Integer(25), &SqlValue::Integer(75));

        // Should be roughly 50% (50 values out of 100)
        assert!((sel - 0.5).abs() < 0.3);
    }

    #[test]
    fn test_empty_histogram() {
        let histogram = Histogram::create(&[], 10, BucketStrategy::EqualDepth);

        assert!(histogram.is_empty());
        assert_eq!(histogram.bucket_count(), 0);
        assert_eq!(histogram.estimate_equality_selectivity(&SqlValue::Integer(1)), 0.0);
    }

    #[test]
    fn test_single_value_histogram() {
        let values = vec![SqlValue::Integer(42); 100];

        let histogram = Histogram::create(&values, 10, BucketStrategy::EqualDepth);

        assert_eq!(histogram.bucket_count(), 1);
        assert_eq!(histogram.buckets[0].distinct_count, 1);
        assert_eq!(histogram.buckets[0].frequency, 1.0);
    }
}
