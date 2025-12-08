//! Hash table structures for columnar join operations
//!
//! This module provides hash table implementations optimized for columnar data:
//! - `ColumnarHashTable`: Single-column hash table with optional Bloom filter
//! - `CompositeIntHashTable`: Multi-column integer hash table
//!
//! # Bloom Filter Optimization
//!
//! The hash tables can optionally build a Bloom filter alongside the main hash
//! structure. During probe, the Bloom filter can quickly reject keys that are
//! definitely not in the table, avoiding expensive hash table lookups.

use crate::errors::ExecutorError;
use crate::select::columnar::ColumnArray;
use crate::select::join::BloomFilter;

/// Minimum number of build-side rows to enable Bloom filter optimization.
const BLOOM_FILTER_MIN_ROWS: usize = 100;

/// Target false positive rate for Bloom filter (1%).
const BLOOM_FILTER_FPR: f64 = 0.01;

/// Hash table for columnar join operations
///
/// Uses a two-level structure:
/// - `buckets`: Maps hash value to first entry index
/// - `entries`: Linked list of (row_index, next_entry) pairs
///
/// This structure is more cache-friendly than HashMap<Key, Vec<usize>> because:
/// - No per-bucket Vec allocation
/// - Entries are stored contiguously
/// - Better cache utilization during probe
///
/// # Bloom Filter Optimization
///
/// When the table has enough rows (>= BLOOM_FILTER_MIN_ROWS), a Bloom filter
/// is built alongside the hash table. Use `bloom_might_contain_*` methods to
/// quickly reject probe keys before doing full hash table lookups.
pub struct ColumnarHashTable {
    /// Number of hash buckets (power of 2)
    bucket_count: usize,
    /// Bucket array: bucket[hash % bucket_count] = first entry index (or u32::MAX if empty)
    buckets: Vec<u32>,
    /// Entry array: entries[i] = (row_index, next_entry_index)
    entries: Vec<(u32, u32)>,
    /// Optional Bloom filter for quick rejection of non-matching keys
    bloom_filter: Option<BloomFilter>,
}

impl ColumnarHashTable {
    /// Build a hash table from an integer column
    ///
    /// This is the fast path for integer-keyed joins (most common in TPC-H).
    pub fn build_from_i64(values: &[i64]) -> Self {
        let row_count = values.len();

        // Size buckets to ~2x entries for good load factor
        let bucket_count = (row_count * 2).next_power_of_two().max(16);
        let bucket_mask = bucket_count - 1;

        // Initialize buckets to empty (u32::MAX)
        let mut buckets = vec![u32::MAX; bucket_count];

        // Pre-allocate entries
        let mut entries = Vec::with_capacity(row_count);

        // Create Bloom filter if we have enough rows
        let bloom_disabled = std::env::var("VIBESQL_DISABLE_BLOOM_FILTER").is_ok();
        let mut bloom_filter = if !bloom_disabled && row_count >= BLOOM_FILTER_MIN_ROWS {
            Some(BloomFilter::new(row_count, BLOOM_FILTER_FPR))
        } else {
            None
        };

        // Build hash table
        for (row_idx, &value) in values.iter().enumerate() {
            // Simple hash for i64: mix the bits
            let hash = Self::hash_i64(value);
            let bucket_idx = (hash as usize) & bucket_mask;

            // Insert into Bloom filter
            if let Some(ref mut bf) = bloom_filter {
                bf.insert_hash(hash);
            }

            // Insert into linked list at this bucket
            let prev_head = buckets[bucket_idx];
            entries.push((row_idx as u32, prev_head));
            buckets[bucket_idx] = entries.len() as u32 - 1;
        }

        Self { bucket_count, buckets, entries, bloom_filter }
    }

    /// Build a hash table from a string column
    pub fn build_from_string(values: &[std::sync::Arc<str>]) -> Self {
        let row_count = values.len();
        let bucket_count = (row_count * 2).next_power_of_two().max(16);
        let bucket_mask = bucket_count - 1;

        let mut buckets = vec![u32::MAX; bucket_count];
        let mut entries = Vec::with_capacity(row_count);

        // Create Bloom filter if we have enough rows
        let bloom_disabled = std::env::var("VIBESQL_DISABLE_BLOOM_FILTER").is_ok();
        let mut bloom_filter = if !bloom_disabled && row_count >= BLOOM_FILTER_MIN_ROWS {
            Some(BloomFilter::new(row_count, BLOOM_FILTER_FPR))
        } else {
            None
        };

        for (row_idx, value) in values.iter().enumerate() {
            let hash = Self::hash_string(value);
            let bucket_idx = (hash as usize) & bucket_mask;

            // Insert into Bloom filter
            if let Some(ref mut bf) = bloom_filter {
                bf.insert_hash(hash);
            }

            let prev_head = buckets[bucket_idx];
            entries.push((row_idx as u32, prev_head));
            buckets[bucket_idx] = entries.len() as u32 - 1;
        }

        Self { bucket_count, buckets, entries, bloom_filter }
    }

    /// Build hash table from a ColumnArray
    pub fn build_from_column(column: &ColumnArray) -> Result<Self, ExecutorError> {
        match column {
            ColumnArray::Int64(values, _nulls) => Ok(Self::build_from_i64(values)),
            ColumnArray::Float64(values, _nulls) => Ok(Self::build_from_f64(values)),
            ColumnArray::String(values, _nulls) => Ok(Self::build_from_string(values)),
            ColumnArray::Date(values, _nulls) => Ok(Self::build_from_i32(values)),
            ColumnArray::Timestamp(values, _nulls) => Ok(Self::build_from_i64(values)),
            _ => Err(ExecutorError::UnsupportedFeature(
                "Columnar hash join not supported for this column type".to_string(),
            )),
        }
    }

    /// Build from i32 values (dates)
    fn build_from_i32(values: &[i32]) -> Self {
        let row_count = values.len();
        let bucket_count = (row_count * 2).next_power_of_two().max(16);
        let bucket_mask = bucket_count - 1;

        let mut buckets = vec![u32::MAX; bucket_count];
        let mut entries = Vec::with_capacity(row_count);

        // Create Bloom filter if we have enough rows
        let bloom_disabled = std::env::var("VIBESQL_DISABLE_BLOOM_FILTER").is_ok();
        let mut bloom_filter = if !bloom_disabled && row_count >= BLOOM_FILTER_MIN_ROWS {
            Some(BloomFilter::new(row_count, BLOOM_FILTER_FPR))
        } else {
            None
        };

        for (row_idx, &value) in values.iter().enumerate() {
            let hash = Self::hash_i64(value as i64);
            let bucket_idx = (hash as usize) & bucket_mask;

            // Insert into Bloom filter
            if let Some(ref mut bf) = bloom_filter {
                bf.insert_hash(hash);
            }

            let prev_head = buckets[bucket_idx];
            entries.push((row_idx as u32, prev_head));
            buckets[bucket_idx] = entries.len() as u32 - 1;
        }

        Self { bucket_count, buckets, entries, bloom_filter }
    }

    /// Build from f64 values
    fn build_from_f64(values: &[f64]) -> Self {
        let row_count = values.len();
        let bucket_count = (row_count * 2).next_power_of_two().max(16);
        let bucket_mask = bucket_count - 1;

        let mut buckets = vec![u32::MAX; bucket_count];
        let mut entries = Vec::with_capacity(row_count);

        // Create Bloom filter if we have enough rows
        let bloom_disabled = std::env::var("VIBESQL_DISABLE_BLOOM_FILTER").is_ok();
        let mut bloom_filter = if !bloom_disabled && row_count >= BLOOM_FILTER_MIN_ROWS {
            Some(BloomFilter::new(row_count, BLOOM_FILTER_FPR))
        } else {
            None
        };

        for (row_idx, &value) in values.iter().enumerate() {
            let hash = Self::hash_f64(value);
            let bucket_idx = (hash as usize) & bucket_mask;

            // Insert into Bloom filter
            if let Some(ref mut bf) = bloom_filter {
                bf.insert_hash(hash);
            }

            let prev_head = buckets[bucket_idx];
            entries.push((row_idx as u32, prev_head));
            buckets[bucket_idx] = entries.len() as u32 - 1;
        }

        Self { bucket_count, buckets, entries, bloom_filter }
    }

    /// Probe the hash table with an i64 key, returning matching row indices
    #[inline]
    pub fn probe_i64<'a>(
        &'a self,
        key: i64,
        build_values: &'a [i64],
    ) -> impl Iterator<Item = u32> + 'a {
        let hash = Self::hash_i64(key);
        let bucket_idx = (hash as usize) & (self.bucket_count - 1);

        HashTableIter {
            entries: &self.entries,
            current: self.buckets[bucket_idx],
            key_checker: move |row_idx: u32| build_values[row_idx as usize] == key,
        }
    }

    /// Probe the hash table with a string key
    #[inline]
    pub fn probe_string<'a>(
        &'a self,
        key: &'a str,
        build_values: &'a [std::sync::Arc<str>],
    ) -> impl Iterator<Item = u32> + 'a {
        let hash = Self::hash_string(key);
        let bucket_idx = (hash as usize) & (self.bucket_count - 1);

        HashTableIter {
            entries: &self.entries,
            current: self.buckets[bucket_idx],
            key_checker: move |row_idx: u32| &*build_values[row_idx as usize] == key,
        }
    }

    /// Fast hash function for i64 (FxHash-style)
    #[inline(always)]
    pub(crate) fn hash_i64(value: i64) -> u64 {
        const K: u64 = 0x517cc1b727220a95;
        let mut h = value as u64;
        h = h.wrapping_mul(K);
        h ^= h >> 32;
        h
    }

    /// Hash function for f64
    #[inline(always)]
    fn hash_f64(value: f64) -> u64 {
        Self::hash_i64(value.to_bits() as i64)
    }

    /// Hash function for strings (FNV-1a style)
    #[inline(always)]
    pub(crate) fn hash_string(value: &str) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET;
        for byte in value.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    /// Check if an i64 key might be in the hash table using the Bloom filter.
    ///
    /// Returns `true` if the key MIGHT be in the table (possible false positive).
    /// Returns `false` if the key is DEFINITELY NOT in the table.
    /// Returns `true` if no Bloom filter is available (conservative).
    #[inline]
    pub fn bloom_might_contain_i64(&self, key: i64) -> bool {
        match &self.bloom_filter {
            Some(bf) => bf.might_contain_hash(Self::hash_i64(key)),
            None => true, // No Bloom filter, assume might contain
        }
    }

    /// Check if a string key might be in the hash table using the Bloom filter.
    #[inline]
    pub fn bloom_might_contain_string(&self, key: &str) -> bool {
        match &self.bloom_filter {
            Some(bf) => bf.might_contain_hash(Self::hash_string(key)),
            None => true, // No Bloom filter, assume might contain
        }
    }

    /// Returns true if this hash table has a Bloom filter enabled.
    #[inline]
    pub fn has_bloom_filter(&self) -> bool {
        self.bloom_filter.is_some()
    }
}

/// Iterator over hash table entries matching a key
pub(crate) struct HashTableIter<'a, F> {
    entries: &'a [(u32, u32)],
    current: u32,
    key_checker: F,
}

impl<'a, F: Fn(u32) -> bool> Iterator for HashTableIter<'a, F> {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while self.current != u32::MAX {
            let (row_idx, next) = self.entries[self.current as usize];
            self.current = next;

            if (self.key_checker)(row_idx) {
                return Some(row_idx);
            }
        }
        None
    }
}

/// Hash table for multi-column integer joins
///
/// Uses pre-computed composite hashes for efficient multi-column lookups.
/// This avoids SqlValue enum dispatch and Vec allocation for each key.
pub struct CompositeIntHashTable {
    /// Number of hash buckets (power of 2)
    bucket_count: usize,
    /// Bucket array: bucket[hash % bucket_count] = first entry index (or u32::MAX if empty)
    buckets: Vec<u32>,
    /// Entry array: entries[i] = (row_index, next_entry_index)
    entries: Vec<(u32, u32)>,
    /// Number of key columns
    num_cols: usize,
}

impl CompositeIntHashTable {
    /// Hash multiple i64 values into a single u64
    #[inline(always)]
    fn hash_composite(values: &[i64]) -> u64 {
        const K: u64 = 0x517cc1b727220a95;
        let mut h: u64 = 0xcbf29ce484222325; // FNV offset basis

        for &v in values {
            h ^= v as u64;
            h = h.wrapping_mul(K);
            h ^= h >> 32;
        }
        h
    }

    /// Build a hash table from multiple integer columns
    ///
    /// Fast path for multi-column integer-keyed joins (common in TPC-H Q3, Q7, Q10).
    pub fn build_from_multi_i64(columns: &[&[i64]]) -> Self {
        if columns.is_empty() {
            return Self {
                bucket_count: 16,
                buckets: vec![u32::MAX; 16],
                entries: Vec::new(),
                num_cols: 0,
            };
        }

        let row_count = columns[0].len();
        let num_cols = columns.len();

        // Size buckets to ~2x entries for good load factor
        let bucket_count = (row_count * 2).next_power_of_two().max(16);
        let bucket_mask = bucket_count - 1;

        // Initialize buckets to empty (u32::MAX)
        let mut buckets = vec![u32::MAX; bucket_count];

        // Pre-allocate entries
        let mut entries = Vec::with_capacity(row_count);

        // Build hash table
        let mut key_values = vec![0i64; num_cols];
        for row_idx in 0..row_count {
            // Extract key values for this row
            for (col_idx, col) in columns.iter().enumerate() {
                key_values[col_idx] = col[row_idx];
            }

            let hash = Self::hash_composite(&key_values);
            let bucket_idx = (hash as usize) & bucket_mask;

            // Insert into linked list at this bucket
            let prev_head = buckets[bucket_idx];
            entries.push((row_idx as u32, prev_head));
            buckets[bucket_idx] = entries.len() as u32 - 1;
        }

        Self { bucket_count, buckets, entries, num_cols }
    }

    /// Probe the hash table with multiple i64 keys
    #[inline]
    pub fn probe_multi_i64<'a>(
        &'a self,
        probe_keys: &'a [i64],
        build_columns: &'a [&'a [i64]],
    ) -> impl Iterator<Item = u32> + 'a {
        let hash = Self::hash_composite(probe_keys);
        let bucket_idx = (hash as usize) & (self.bucket_count - 1);
        let num_cols = self.num_cols;

        CompositeHashTableIter {
            entries: &self.entries,
            current: self.buckets[bucket_idx],
            probe_keys,
            build_columns,
            num_cols,
        }
    }
}

/// Iterator over composite hash table entries matching a multi-column key
struct CompositeHashTableIter<'a> {
    entries: &'a [(u32, u32)],
    current: u32,
    probe_keys: &'a [i64],
    build_columns: &'a [&'a [i64]],
    num_cols: usize,
}

impl<'a> Iterator for CompositeHashTableIter<'a> {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while self.current != u32::MAX {
            let (row_idx, next) = self.entries[self.current as usize];
            self.current = next;

            // Check if all columns match
            let mut matches = true;
            for col_idx in 0..self.num_cols {
                if self.build_columns[col_idx][row_idx as usize] != self.probe_keys[col_idx] {
                    matches = false;
                    break;
                }
            }

            if matches {
                return Some(row_idx);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_table_build_and_probe() {
        let values = vec![10i64, 20, 30, 20, 40, 20];
        let ht = ColumnarHashTable::build_from_i64(&values);
        // Probe for 20 - should find indices 1, 3, 5
        let matches: Vec<u32> = ht.probe_i64(20, &values).collect();
        assert_eq!(matches.len(), 3);
        assert!(matches.contains(&1) && matches.contains(&3) && matches.contains(&5));
        // Probe for 10 - should find index 0
        let matches: Vec<u32> = ht.probe_i64(10, &values).collect();
        assert_eq!(matches, vec![0]);
        // Probe for 99 - should find nothing
        assert!(ht.probe_i64(99, &values).collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn test_composite_int_hash_table_build_and_probe() {
        let col1 = vec![1i64, 2, 3, 1];
        let col2 = vec![10i64, 20, 30, 10]; // Row 0 and 3 have same composite key (1, 10)
        let columns: Vec<&[i64]> = vec![col1.as_slice(), col2.as_slice()];
        let ht = CompositeIntHashTable::build_from_multi_i64(&columns);
        // Probe for (1, 10) - should find indices 0 and 3
        let matches: Vec<u32> = ht.probe_multi_i64(&[1i64, 10], &columns).collect();
        assert_eq!(matches.len(), 2);
        assert!(matches.contains(&0) && matches.contains(&3));
        // Probe for (2, 20) - should find index 1
        let matches: Vec<u32> = ht.probe_multi_i64(&[2i64, 20], &columns).collect();
        assert_eq!(matches, vec![1]);
        // Probe for (1, 20) - partial match should find nothing
        assert!(ht.probe_multi_i64(&[1i64, 20], &columns).collect::<Vec<_>>().is_empty());
        // Probe for (99, 99) - should find nothing
        assert!(ht.probe_multi_i64(&[99i64, 99], &columns).collect::<Vec<_>>().is_empty());
    }
}
