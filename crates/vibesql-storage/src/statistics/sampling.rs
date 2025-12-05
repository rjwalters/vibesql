//! Statistical sampling for large table statistics
//!
//! Provides efficient sampling methods for computing statistics on large tables
//! without requiring a full table scan. Implements various sampling strategies:
//! - Random sampling: Randomly select rows
//! - Systematic sampling: Every Nth row
//! - Reservoir sampling: Single-pass streaming sample
//! - Adaptive sampling: Choose sample size based on table size

use rand::prelude::IndexedRandom;
use rand::Rng;

/// Configuration for statistical sampling
#[derive(Debug, Clone)]
pub struct SamplingConfig {
    /// Sample size strategy
    pub sample_size: SampleSize,

    /// Sampling method to use
    pub method: SamplingMethod,

    /// Statistical confidence level (e.g., 0.95 for 95% confidence)
    pub confidence_level: f64,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            sample_size: SampleSize::Adaptive,
            method: SamplingMethod::Random,
            confidence_level: 0.95,
        }
    }
}

/// Sample size specification
#[derive(Debug, Clone, PartialEq)]
pub enum SampleSize {
    /// Exact number of rows to sample
    RowCount(usize),

    /// Percentage of table (0.0 to 1.0)
    Percentage(f64),

    /// Automatically determine sample size based on table size
    ///
    /// Strategy:
    /// - Small tables (< 1000 rows): No sampling, use all rows
    /// - Medium tables (1K-100K rows): 10% sample
    /// - Large tables (> 100K rows): Fixed 10K row sample
    Adaptive,
}

/// Sampling method
#[derive(Debug, Clone, PartialEq)]
pub enum SamplingMethod {
    /// Pure random sampling (requires index-based access)
    Random,

    /// Every Nth row (systematic sampling)
    Systematic,

    /// Reservoir sampling (single-pass, constant memory)
    Reservoir,
}

impl SamplingConfig {
    /// Create a new sampling configuration
    pub fn new(sample_size: SampleSize, method: SamplingMethod) -> Self {
        Self { sample_size, method, confidence_level: 0.95 }
    }

    /// Create configuration for adaptive sampling (default)
    pub fn adaptive() -> Self {
        Self::default()
    }

    /// Create configuration for percentage-based sampling
    pub fn percentage(pct: f64) -> Self {
        Self {
            sample_size: SampleSize::Percentage(pct),
            method: SamplingMethod::Random,
            confidence_level: 0.95,
        }
    }

    /// Create configuration for fixed-size sampling
    pub fn fixed(count: usize) -> Self {
        Self {
            sample_size: SampleSize::RowCount(count),
            method: SamplingMethod::Random,
            confidence_level: 0.95,
        }
    }

    /// Determine actual sample size for a given table
    ///
    /// Returns (sample_size, should_sample) tuple
    /// - sample_size: Number of rows to sample
    /// - should_sample: Whether sampling is needed (false for small tables)
    pub fn determine_sample_size(&self, total_rows: usize) -> (usize, bool) {
        match &self.sample_size {
            SampleSize::RowCount(count) => {
                let sample_size = (*count).min(total_rows);
                (sample_size, sample_size < total_rows)
            }
            SampleSize::Percentage(pct) => {
                let sample_size = (total_rows as f64 * pct).ceil() as usize;
                let sample_size = sample_size.min(total_rows).max(1);
                (sample_size, sample_size < total_rows)
            }
            SampleSize::Adaptive => {
                // Adaptive strategy based on table size
                if total_rows < 1000 {
                    // Small table: no sampling
                    (total_rows, false)
                } else if total_rows < 100_000 {
                    // Medium table: 10% sample
                    let sample_size = (total_rows / 10).max(1000);
                    (sample_size, true)
                } else {
                    // Large table: fixed 10K sample
                    (10_000, true)
                }
            }
        }
    }
}

/// Sample rows from a table using the specified sampling method
pub fn sample_rows<T: Clone>(rows: &[T], config: &SamplingConfig, rng: &mut impl Rng) -> Vec<T> {
    let (sample_size, should_sample) = config.determine_sample_size(rows.len());

    if !should_sample {
        // No sampling needed, return all rows
        return rows.to_vec();
    }

    match config.method {
        SamplingMethod::Random => random_sample(rows, sample_size, rng),
        SamplingMethod::Systematic => systematic_sample(rows, sample_size),
        SamplingMethod::Reservoir => reservoir_sample(rows, sample_size, rng),
    }
}

/// Random sampling: randomly select n rows
fn random_sample<T: Clone>(rows: &[T], n: usize, rng: &mut impl Rng) -> Vec<T> {
    let n = n.min(rows.len());
    let indices: Vec<usize> = (0..rows.len()).collect();
    let sampled_indices = indices.choose_multiple(rng, n);

    sampled_indices.map(|&i| rows[i].clone()).collect()
}

/// Systematic sampling: every Nth row
fn systematic_sample<T: Clone>(rows: &[T], n: usize) -> Vec<T> {
    let n = n.min(rows.len());
    if n == 0 {
        return Vec::new();
    }

    let step = rows.len() / n;
    if step == 0 {
        return rows.to_vec();
    }

    rows.iter().step_by(step).take(n).cloned().collect()
}

/// Reservoir sampling: single-pass, constant memory
///
/// Knuth's Algorithm R - processes stream in one pass,
/// maintaining a reservoir of k samples with equal probability
fn reservoir_sample<T: Clone>(rows: &[T], k: usize, rng: &mut impl Rng) -> Vec<T> {
    let k = k.min(rows.len());
    if k == 0 {
        return Vec::new();
    }

    let mut reservoir: Vec<T> = rows.iter().take(k).cloned().collect();

    for (i, row) in rows.iter().enumerate().skip(k) {
        // Randomly decide whether to include this element
        let j = rng.random_range(0..=i);
        if j < k {
            reservoir[j] = row.clone();
        }
    }

    reservoir
}

/// Metadata about a sample
#[derive(Debug, Clone)]
pub struct SampleMetadata {
    /// Total number of rows in the table
    pub total_rows: usize,

    /// Number of rows in the sample
    pub sample_size: usize,

    /// Sampling ratio (sample_size / total_rows)
    pub sampling_ratio: f64,

    /// Whether sampling was actually used
    pub sampled: bool,

    /// Confidence level for statistical estimates
    pub confidence_level: f64,
}

impl SampleMetadata {
    /// Create metadata for a sample
    pub fn new(
        total_rows: usize,
        sample_size: usize,
        sampled: bool,
        confidence_level: f64,
    ) -> Self {
        let sampling_ratio =
            if total_rows > 0 { sample_size as f64 / total_rows as f64 } else { 1.0 };

        Self { total_rows, sample_size, sampling_ratio, sampled, confidence_level }
    }

    /// Extrapolate a count from sample to full table
    ///
    /// Given a count in the sample, estimate the count in the full table
    pub fn extrapolate_count(&self, sample_count: usize) -> usize {
        if !self.sampled || self.sampling_ratio == 0.0 {
            return sample_count;
        }

        (sample_count as f64 / self.sampling_ratio).round() as usize
    }

    /// Extrapolate a frequency/selectivity from sample to full table
    ///
    /// Frequencies typically don't need adjustment, but we apply
    /// a small confidence adjustment for small samples
    pub fn extrapolate_frequency(&self, sample_frequency: f64) -> f64 {
        if !self.sampled {
            return sample_frequency;
        }

        // For small samples, be more conservative with estimates
        // Apply Laplace smoothing for small samples
        if self.sample_size < 100 {
            let smoothing = 1.0 / self.sample_size as f64;
            (sample_frequency + smoothing) / (1.0 + smoothing)
        } else {
            sample_frequency
        }
    }

    /// Estimate the standard error for a proportion
    ///
    /// Used to provide confidence intervals for selectivity estimates
    pub fn standard_error(&self, proportion: f64) -> f64 {
        if !self.sampled || self.sample_size == 0 {
            return 0.0;
        }

        // Standard error of proportion: sqrt(p(1-p)/n)
        // With finite population correction
        let fpc = if self.total_rows > self.sample_size {
            ((self.total_rows - self.sample_size) as f64 / (self.total_rows - 1) as f64).sqrt()
        } else {
            1.0
        };

        (proportion * (1.0 - proportion) / self.sample_size as f64).sqrt() * fpc
    }

    /// Get confidence interval for a proportion estimate
    ///
    /// Returns (lower_bound, upper_bound) for the confidence level
    pub fn confidence_interval(&self, proportion: f64) -> (f64, f64) {
        if !self.sampled {
            return (proportion, proportion);
        }

        // Use normal approximation (valid for n*p > 5 and n*(1-p) > 5)
        // Z-score for 95% confidence is approximately 1.96
        let z_score = match self.confidence_level {
            x if x >= 0.99 => 2.576,
            x if x >= 0.95 => 1.96,
            x if x >= 0.90 => 1.645,
            _ => 1.96, // default to 95%
        };

        let se = self.standard_error(proportion);
        let margin = z_score * se;

        let lower = (proportion - margin).max(0.0);
        let upper = (proportion + margin).min(1.0);

        (lower, upper)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_sample_size_adaptive() {
        let config = SamplingConfig::adaptive();

        // Small table: no sampling
        let (size, should_sample) = config.determine_sample_size(500);
        assert_eq!(size, 500);
        assert!(!should_sample);

        // Medium table: 10% sample
        let (size, should_sample) = config.determine_sample_size(50_000);
        assert_eq!(size, 5_000);
        assert!(should_sample);

        // Large table: fixed 10K sample
        let (size, should_sample) = config.determine_sample_size(1_000_000);
        assert_eq!(size, 10_000);
        assert!(should_sample);
    }

    #[test]
    fn test_sample_size_percentage() {
        let config = SamplingConfig::percentage(0.1);

        let (size, should_sample) = config.determine_sample_size(1000);
        assert_eq!(size, 100);
        assert!(should_sample);

        // Edge case: percentage results in 0
        let (size, should_sample) = config.determine_sample_size(5);
        assert_eq!(size, 1); // Clamped to minimum 1
        assert!(should_sample);
    }

    #[test]
    fn test_sample_size_fixed() {
        let config = SamplingConfig::fixed(100);

        let (size, should_sample) = config.determine_sample_size(1000);
        assert_eq!(size, 100);
        assert!(should_sample);

        // Edge case: sample size larger than table
        let (size, should_sample) = config.determine_sample_size(50);
        assert_eq!(size, 50);
        assert!(!should_sample);
    }

    #[test]
    fn test_random_sample() {
        let mut rng = StdRng::seed_from_u64(42);
        let rows: Vec<i32> = (0..100).collect();

        let config = SamplingConfig::fixed(10);
        let sample = sample_rows(&rows, &config, &mut rng);

        assert_eq!(sample.len(), 10);
        // All sampled values should be in range
        for val in sample {
            assert!((0..100).contains(&val));
        }
    }

    #[test]
    fn test_systematic_sample() {
        let rows: Vec<i32> = (0..100).collect();
        let sample = systematic_sample(&rows, 10);

        assert_eq!(sample.len(), 10);
        // Systematic sampling should be evenly spaced
        assert_eq!(sample[0], 0);
        assert_eq!(sample[1], 10);
        assert_eq!(sample[9], 90);
    }

    #[test]
    fn test_reservoir_sample() {
        let mut rng = StdRng::seed_from_u64(42);
        let rows: Vec<i32> = (0..100).collect();
        let sample = reservoir_sample(&rows, 10, &mut rng);

        assert_eq!(sample.len(), 10);
        // All sampled values should be valid
        for val in sample {
            assert!((0..100).contains(&val));
        }
    }

    #[test]
    fn test_sample_metadata_extrapolation() {
        // 10% sample (1000 out of 10000)
        let metadata = SampleMetadata::new(10_000, 1_000, true, 0.95);

        // Count extrapolation
        assert_eq!(metadata.extrapolate_count(100), 1_000);
        assert_eq!(metadata.extrapolate_count(50), 500);

        // Frequency extrapolation (should be mostly unchanged)
        let freq = metadata.extrapolate_frequency(0.5);
        assert!((freq - 0.5).abs() < 0.1);
    }

    #[test]
    fn test_confidence_interval() {
        let metadata = SampleMetadata::new(10_000, 1_000, true, 0.95);

        // For p=0.5 with n=1000, standard error ≈ 0.0158
        // 95% CI margin ≈ 1.96 * 0.0158 ≈ 0.031
        let (lower, upper) = metadata.confidence_interval(0.5);

        assert!(lower < 0.5);
        assert!(upper > 0.5);
        assert!((upper - lower) < 0.1); // Reasonable interval width
    }

    #[test]
    fn test_no_sampling_for_small_table() {
        let mut rng = StdRng::seed_from_u64(42);
        let rows: Vec<i32> = (0..50).collect();

        let config = SamplingConfig::adaptive();
        let sample = sample_rows(&rows, &config, &mut rng);

        // Should return all rows for small table
        assert_eq!(sample.len(), 50);
        assert_eq!(sample, rows);
    }
}
