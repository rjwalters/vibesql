//! Sysbench Data Generation
//!
//! Generates data matching the standard sysbench OLTP schema.
//! The sysbench sbtest table has the following structure:
//!
//! CREATE TABLE sbtest1 (
//!   id INTEGER PRIMARY KEY,
//!   k INTEGER NOT NULL DEFAULT 0,
//!   c CHAR(120) NOT NULL DEFAULT '',
//!   padding CHAR(60) NOT NULL DEFAULT ''  -- Note: renamed from 'pad' (SQL keyword)
//! );
//! CREATE INDEX k_1 ON sbtest1(k);
//!
//! - `id`: Sequential primary key
//! - `k`: Random integer (used for secondary index lookups)
//! - `c`: String of 120 chars with format "###-###-...-###"
//! - `padding`: String of 60 chars with format "###-###-...-###"

use rand::prelude::*;
use rand_chacha::ChaCha8Rng;

/// Default table size (number of rows) - matches sysbench default
pub const DEFAULT_TABLE_SIZE: usize = 10_000;

/// Sysbench data generator
pub struct SysbenchData {
    rng: ChaCha8Rng,
    table_size: usize,
    next_id: i64,
}

impl SysbenchData {
    /// Create a new sysbench data generator
    ///
    /// # Arguments
    /// * `table_size` - Number of rows to generate (default: 10,000)
    pub fn new(table_size: usize) -> Self {
        Self {
            rng: ChaCha8Rng::seed_from_u64(42), // Deterministic seed for reproducibility
            table_size,
            next_id: 1,
        }
    }

    /// Get the table size
    pub fn table_size(&self) -> usize {
        self.table_size
    }

    /// Generate the next row for sbtest table
    /// Returns (id, k, c, pad) tuple
    pub fn next_row(&mut self) -> Option<(i64, i64, String, String)> {
        if self.next_id as usize > self.table_size {
            return None;
        }

        let id = self.next_id;
        self.next_id += 1;

        // k is a random integer in [1, table_size]
        let k = self.rng.random_range(1..=self.table_size as i64);

        // c is a 120-char string with format "###-###-...-###" (11 groups of 10 chars)
        let c = self.generate_c_string();

        // pad is a 60-char string with format "###-###-...-###" (5 groups of 11 chars + padding)
        let pad = self.generate_pad_string();

        Some((id, k, c, pad))
    }

    /// Generate the 'c' column value (120 chars)
    /// Format: 11 groups of "XXXXXXXXXX-" where X is a digit, last group has no hyphen
    fn generate_c_string(&mut self) -> String {
        let mut s = String::with_capacity(120);
        for i in 0..11 {
            // Generate 10 random digits
            for _ in 0..10 {
                s.push((b'0' + self.rng.random_range(0..10)) as char);
            }
            if i < 10 {
                s.push('-');
            }
        }
        s
    }

    /// Generate the 'pad' column value (60 chars)
    /// Format: 5 groups of "XXXXXXXXXX-" where X is a digit, padded to 60 chars
    fn generate_pad_string(&mut self) -> String {
        let mut s = String::with_capacity(60);
        for i in 0..5 {
            // Generate 10 random digits
            for _ in 0..10 {
                s.push((b'0' + self.rng.random_range(0..10)) as char);
            }
            if i < 4 {
                s.push('-');
            }
        }
        // Pad to 60 chars with spaces
        while s.len() < 60 {
            s.push(' ');
        }
        s
    }

    /// Generate a random ID for point lookups (1 to table_size)
    pub fn random_id(&mut self) -> i64 {
        self.rng.random_range(1..=self.table_size as i64)
    }

    /// Generate a random k value for secondary index lookups
    pub fn random_k(&mut self) -> i64 {
        self.rng.random_range(1..=self.table_size as i64)
    }

    /// Generate random IDs for point select queries.
    /// Returns `count` random IDs in the range [1, table_size].
    pub fn random_ids(&mut self, count: usize) -> Vec<i64> {
        (0..count).map(|_| self.rng.random_range(1..=self.table_size as i64)).collect()
    }

    /// Generate a random range for range queries.
    /// Returns (start_id, end_id) where end_id = start_id + range_size - 1.
    /// If the table is smaller than the range size, uses the full table range.
    pub fn random_range(&mut self, range_size: usize) -> (i64, i64) {
        // Use saturating_sub to avoid underflow when table_size < range_size
        let effective_range = range_size.min(self.table_size);
        let max_start = self.table_size.saturating_sub(effective_range).saturating_add(1).max(1);
        let start = self.rng.random_range(1..=max_start as i64);
        (start, start + effective_range as i64 - 1)
    }

    /// Reset the generator for re-iteration
    pub fn reset(&mut self) {
        self.rng = ChaCha8Rng::seed_from_u64(42);
        self.next_id = 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_generation() {
        let mut data = SysbenchData::new(100);

        // Generate first row
        let row = data.next_row().unwrap();
        assert_eq!(row.0, 1); // id starts at 1
        assert!(row.1 >= 1 && row.1 <= 100); // k in range
        assert_eq!(row.2.len(), 120); // c is 120 chars
        assert_eq!(row.3.len(), 60); // pad is 60 chars
    }

    #[test]
    fn test_deterministic() {
        let mut data1 = SysbenchData::new(100);
        let mut data2 = SysbenchData::new(100);

        // Same seed should produce same data
        let row1 = data1.next_row().unwrap();
        let row2 = data2.next_row().unwrap();
        assert_eq!(row1, row2);
    }

    #[test]
    fn test_table_size() {
        let mut data = SysbenchData::new(10);

        // Should generate exactly 10 rows
        for i in 1..=10 {
            let row = data.next_row();
            assert!(row.is_some());
            assert_eq!(row.unwrap().0, i);
        }

        // 11th call should return None
        assert!(data.next_row().is_none());
    }

    #[test]
    fn test_random_ids() {
        let mut data = SysbenchData::new(100);
        let ids = data.random_ids(5);
        assert_eq!(ids.len(), 5);
        for id in ids {
            assert!(id >= 1 && id <= 100);
        }
    }

    #[test]
    fn test_random_range() {
        let mut data = SysbenchData::new(100);
        let (start, end) = data.random_range(10);
        assert!(start >= 1);
        assert!(end <= 100);
        assert_eq!(end - start + 1, 10);
    }
}
