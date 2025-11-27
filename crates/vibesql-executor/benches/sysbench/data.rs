//! Sysbench Data Generator
//!
//! This module provides data generation utilities for sysbench OLTP benchmarks.
//! The data format matches the standard sysbench `sbtest` table schema.

use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

/// Sysbench data generator that produces deterministic pseudo-random data.
pub struct SysbenchData {
    /// Number of rows to generate (sysbench --table-size equivalent)
    pub table_size: usize,
    /// Random number generator for deterministic data
    rng: ChaCha8Rng,
}

impl SysbenchData {
    /// Create a new sysbench data generator.
    ///
    /// # Arguments
    /// * `table_size` - Number of rows per sbtest table (default sysbench is 10000)
    pub fn new(table_size: usize) -> Self {
        Self {
            table_size,
            rng: ChaCha8Rng::seed_from_u64(42), // Deterministic seed
        }
    }

    /// Generate the `c` column value (120-char string).
    /// Sysbench uses repeated patterns like "###########-###########-..."
    pub fn generate_c(&mut self) -> String {
        // Sysbench generates 11 groups of 10 random digits separated by hyphens
        let mut parts = Vec::with_capacity(11);
        for _ in 0..11 {
            let digits: String = (0..10)
                .map(|_| char::from_digit(self.rng.random_range(0..10), 10).unwrap())
                .collect();
            parts.push(digits);
        }
        parts.join("-")
    }

    /// Generate the `pad` column value (60-char string).
    /// Sysbench uses a similar pattern but shorter.
    pub fn generate_pad(&mut self) -> String {
        // Sysbench generates 5 groups of 10 random digits separated by hyphens
        let mut parts = Vec::with_capacity(5);
        for _ in 0..5 {
            let digits: String = (0..10)
                .map(|_| char::from_digit(self.rng.random_range(0..10), 10).unwrap())
                .collect();
            parts.push(digits);
        }
        parts.join("-")
    }

    /// Generate random IDs for point select queries.
    /// Returns `count` random IDs in the range [1, table_size].
    pub fn random_ids(&mut self, count: usize) -> Vec<i64> {
        (0..count)
            .map(|_| self.rng.random_range(1..=self.table_size as i64))
            .collect()
    }

    /// Generate a random range for range queries.
    /// Returns (start_id, end_id) where end_id = start_id + range_size - 1.
    pub fn random_range(&mut self, range_size: usize) -> (i64, i64) {
        let max_start = (self.table_size - range_size + 1).max(1);
        let start = self.rng.random_range(1..=max_start as i64);
        (start, start + range_size as i64 - 1)
    }
}
