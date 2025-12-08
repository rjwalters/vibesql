//! Bloom filter implementation for hash join optimization
//!
//! A Bloom filter is a probabilistic data structure that can quickly determine
//! whether an element is definitely NOT in a set (no false negatives) or
//! POSSIBLY in a set (with configurable false positive rate).
//!
//! This implementation is used to optimize hash joins by quickly rejecting
//! probe-side rows that cannot possibly match any build-side rows, avoiding
//! expensive hash table lookups.
//!
//! # How It Works
//!
//! During hash join:
//! 1. **Build phase**: While building the hash table, also populate a Bloom filter
//!    with the join keys
//! 2. **Probe phase**: Before probing the hash table, check the Bloom filter
//!    - If Bloom filter says "definitely not present" → skip the row (no hash lookup)
//!    - If Bloom filter says "maybe present" → do the actual hash lookup
//!
//! # Performance Benefits
//!
//! - Bloom filter check is O(1) with excellent cache behavior
//! - For selective joins (where most probe rows don't match), this eliminates
//!   most hash table lookups
//! - Memory overhead is small (typically 10 bits per element for 1% false positive rate)
//!
//! # References
//!
//! - SQLite uses this optimization via `WHERE_BLOOMFILTER` flag
//! - See: `docs/reference/sqlite/src/whereInt.h` line 661

use std::hash::{Hash, Hasher};

use ahash::AHasher;

/// Bloom filter for join optimization
///
/// Uses double hashing to generate multiple hash indices from a single hash value,
/// avoiding the need to compute multiple independent hash functions.
///
/// # Algorithm
///
/// For k hash functions, we compute:
/// - h1 = lower 32 bits of hash
/// - h2 = upper 32 bits of hash
/// - index_i = (h1 + i * h2) % num_bits for i in 0..k
///
/// This provides good distribution while only computing one hash.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array stored as u64 words for efficient operations
    bits: Vec<u64>,
    /// Number of hash functions (k)
    num_hashes: u8,
    /// Number of bits in the filter (m)
    num_bits: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter sized for the expected number of elements
    /// with the target false positive rate.
    ///
    /// # Arguments
    ///
    /// * `expected_elements` - Expected number of elements to insert
    /// * `false_positive_rate` - Target false positive rate (e.g., 0.01 for 1%)
    ///
    /// # Panics
    ///
    /// Panics if `false_positive_rate` is not in the range (0.0, 1.0).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create filter for 10,000 elements with 1% false positive rate
    /// let filter = BloomFilter::new(10_000, 0.01);
    /// ```
    pub fn new(expected_elements: usize, false_positive_rate: f64) -> Self {
        assert!(
            false_positive_rate > 0.0 && false_positive_rate < 1.0,
            "false_positive_rate must be in (0.0, 1.0)"
        );

        // Handle edge case of zero elements
        let n = expected_elements.max(1) as f64;
        let p = false_positive_rate;

        // Optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let ln2_sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let m = (-n * p.ln() / ln2_sq).ceil() as usize;

        // Ensure at least 64 bits and round up to next u64 boundary
        let num_bits = m.max(64);
        let num_words = num_bits.div_ceil(64);
        let num_bits = num_words * 64;

        // Optimal number of hash functions: k = (m/n) * ln(2)
        let k = ((num_bits as f64 / n) * std::f64::consts::LN_2).round() as u8;
        let num_hashes = k.clamp(1, 16); // Limit to reasonable range

        Self { bits: vec![0u64; num_words], num_hashes, num_bits }
    }

    /// Create a Bloom filter with explicit parameters.
    ///
    /// This is useful for testing or when you want precise control over the filter size.
    ///
    /// # Arguments
    ///
    /// * `num_bits` - Number of bits in the filter (will be rounded up to multiple of 64)
    /// * `num_hashes` - Number of hash functions to use
    #[allow(dead_code)]
    pub fn with_params(num_bits: usize, num_hashes: u8) -> Self {
        let num_bits = num_bits.max(64);
        let num_words = num_bits.div_ceil(64);
        let num_bits = num_words * 64;
        let num_hashes = num_hashes.clamp(1, 16);

        Self { bits: vec![0u64; num_words], num_hashes, num_bits }
    }

    /// Insert a value into the Bloom filter.
    ///
    /// After insertion, `might_contain` will always return `true` for this value.
    #[inline]
    #[allow(dead_code)]
    pub fn insert<T: Hash>(&mut self, value: &T) {
        let hash = self.compute_hash(value);
        self.insert_hash(hash);
    }

    /// Insert a pre-computed hash value into the Bloom filter.
    ///
    /// This is useful when you already have a hash value (e.g., from the hash table).
    #[inline]
    pub fn insert_hash(&mut self, hash: u64) {
        let (h1, h2) = self.split_hash(hash);

        for i in 0..self.num_hashes as u64 {
            let bit_index = self.get_bit_index(h1, h2, i);
            let word_index = bit_index / 64;
            let bit_offset = bit_index % 64;
            self.bits[word_index] |= 1u64 << bit_offset;
        }
    }

    /// Check if a value might be in the set.
    ///
    /// Returns:
    /// - `false` if the value is definitely NOT in the set (no false negatives)
    /// - `true` if the value MIGHT be in the set (possible false positive)
    #[inline]
    #[allow(dead_code)]
    pub fn might_contain<T: Hash>(&self, value: &T) -> bool {
        let hash = self.compute_hash(value);
        self.might_contain_hash(hash)
    }

    /// Check if a pre-computed hash value might be in the set.
    #[inline]
    pub fn might_contain_hash(&self, hash: u64) -> bool {
        let (h1, h2) = self.split_hash(hash);

        for i in 0..self.num_hashes as u64 {
            let bit_index = self.get_bit_index(h1, h2, i);
            let word_index = bit_index / 64;
            let bit_offset = bit_index % 64;
            if (self.bits[word_index] & (1u64 << bit_offset)) == 0 {
                return false; // Definitely not in set
            }
        }
        true // Might be in set
    }

    /// Get the memory usage of this Bloom filter in bytes.
    #[allow(dead_code)]
    pub fn memory_bytes(&self) -> usize {
        self.bits.len() * 8
    }

    /// Get the number of bits in the filter.
    #[allow(dead_code)]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Get the number of hash functions used.
    #[allow(dead_code)]
    pub fn num_hashes(&self) -> u8 {
        self.num_hashes
    }

    /// Compute hash using AHash (already a dependency in the crate).
    #[inline]
    fn compute_hash<T: Hash>(&self, value: &T) -> u64 {
        let mut hasher = AHasher::default();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Split a 64-bit hash into two 32-bit values for double hashing.
    #[inline]
    fn split_hash(&self, hash: u64) -> (u64, u64) {
        let h1 = hash & 0xFFFF_FFFF;
        let h2 = hash >> 32;
        // Ensure h2 is odd to guarantee full period when used as increment
        (h1, h2 | 1)
    }

    /// Get the bit index for the i-th hash function using double hashing.
    #[inline]
    fn get_bit_index(&self, h1: u64, h2: u64, i: u64) -> usize {
        let combined = h1.wrapping_add(i.wrapping_mul(h2));
        (combined % self.num_bits as u64) as usize
    }
}

/// Statistics about Bloom filter effectiveness during a join operation.
#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub struct BloomFilterStats {
    /// Number of rows checked against the Bloom filter
    pub rows_checked: u64,
    /// Number of rows rejected by the Bloom filter (true negatives)
    pub rows_rejected: u64,
    /// Number of rows that passed the Bloom filter check
    pub rows_passed: u64,
}

#[allow(dead_code)]
impl BloomFilterStats {
    /// Create new empty statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a row was rejected by the Bloom filter.
    #[inline]
    pub fn record_rejected(&mut self) {
        self.rows_checked += 1;
        self.rows_rejected += 1;
    }

    /// Record that a row passed the Bloom filter check.
    #[inline]
    pub fn record_passed(&mut self) {
        self.rows_checked += 1;
        self.rows_passed += 1;
    }

    /// Get the rejection rate (fraction of rows rejected by Bloom filter).
    #[allow(dead_code)]
    pub fn rejection_rate(&self) -> f64 {
        if self.rows_checked == 0 {
            0.0
        } else {
            self.rows_rejected as f64 / self.rows_checked as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let mut filter = BloomFilter::new(1000, 0.01);

        // Insert values 0..1000
        for i in 0..1000i64 {
            filter.insert(&i);
        }

        // All inserted values should be found (no false negatives)
        for i in 0..1000i64 {
            assert!(filter.might_contain(&i), "False negative for {}", i);
        }
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let n = 10_000;
        let target_fpr = 0.01;
        let mut filter = BloomFilter::new(n, target_fpr);

        // Insert n values
        for i in 0..n as i64 {
            filter.insert(&i);
        }

        // Check n values that were NOT inserted
        let mut false_positives = 0;
        let test_range = n as i64..(2 * n as i64);
        for i in test_range.clone() {
            if filter.might_contain(&i) {
                false_positives += 1;
            }
        }

        let actual_fpr = false_positives as f64 / n as f64;

        // Allow 3x the target rate to account for variance
        assert!(
            actual_fpr < target_fpr * 3.0,
            "False positive rate {} is too high (target {})",
            actual_fpr,
            target_fpr
        );
    }

    #[test]
    fn test_bloom_filter_with_strings() {
        let mut filter = BloomFilter::new(100, 0.01);

        let values = vec!["apple", "banana", "cherry", "date", "elderberry"];
        for v in &values {
            filter.insert(v);
        }

        for v in &values {
            assert!(filter.might_contain(v), "False negative for {}", v);
        }

        // "fig" was not inserted, might be a false positive
        // We don't assert on this since it's probabilistic
    }

    #[test]
    fn test_bloom_filter_empty() {
        let filter = BloomFilter::new(100, 0.01);

        // Empty filter should reject everything
        for i in 0..100i64 {
            assert!(!filter.might_contain(&i), "Empty filter should reject {}", i);
        }
    }

    #[test]
    fn test_bloom_filter_small() {
        // Test with very small filter
        let mut filter = BloomFilter::with_params(64, 3);

        filter.insert(&42i64);
        assert!(filter.might_contain(&42i64));
    }

    #[test]
    fn test_bloom_filter_hash_insert() {
        let mut filter = BloomFilter::new(100, 0.01);

        // Test hash-based insert/query
        let hash = 0x123456789ABCDEF0u64;
        filter.insert_hash(hash);
        assert!(filter.might_contain_hash(hash));
    }

    #[test]
    fn test_bloom_filter_stats() {
        let mut stats = BloomFilterStats::new();

        stats.record_rejected();
        stats.record_rejected();
        stats.record_passed();

        assert_eq!(stats.rows_checked, 3);
        assert_eq!(stats.rows_rejected, 2);
        assert_eq!(stats.rows_passed, 1);
        assert!((stats.rejection_rate() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_bloom_filter_memory() {
        // 10K elements with 1% FPR should use ~12KB (9.6 bits/element)
        let filter = BloomFilter::new(10_000, 0.01);
        let memory = filter.memory_bytes();

        // Should be roughly 10K * 10 bits / 8 = 12.5KB
        // Allow some variance due to rounding
        assert!(memory >= 10_000, "Memory {} is too small", memory);
        assert!(memory <= 20_000, "Memory {} is too large", memory);
    }

    #[test]
    fn test_bloom_filter_sizing() {
        // Verify optimal sizing
        let filter = BloomFilter::new(1000, 0.01);

        // For n=1000, p=0.01:
        // m = -n * ln(p) / (ln(2)^2) ≈ 9585 bits ≈ 1198 bytes
        // k = (m/n) * ln(2) ≈ 6.6 ≈ 7 hash functions
        assert!(filter.num_bits() >= 9000);
        assert!(filter.num_bits() <= 15000);
        assert!(filter.num_hashes() >= 5);
        assert!(filter.num_hashes() <= 10);
    }
}
