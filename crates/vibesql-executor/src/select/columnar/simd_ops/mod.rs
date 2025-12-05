//! Auto-vectorized SIMD operations for columnar data processing.
//!
//! # Performance Architecture
//!
//! These functions are structured to enable LLVM auto-vectorization. They achieve
//! equivalent performance to explicit SIMD (e.g., the `wide` crate) without the
//! complexity of platform-specific code.
//!
//! ## Why This Pattern Works
//!
//! LLVM can auto-vectorize loops when:
//! 1. Loop bounds are known or predictable
//! 2. Memory access is sequential
//! 3. Operations are independent across lanes
//!
//! The 4-accumulator pattern breaks loop-carried dependencies, allowing LLVM to
//! use SIMD registers effectively:
//!
//! ```text
//! // BAD: Single accumulator creates dependency chain
//! for x in data { sum += x; }  // Each add waits for previous
//!
//! // GOOD: Four accumulators enable parallel execution
//! for chunk in data.chunks(4) {
//!     s0 += chunk[0];  // These four adds can execute
//!     s1 += chunk[1];  // simultaneously in SIMD lanes
//!     s2 += chunk[2];
//!     s3 += chunk[3];
//! }
//! ```
//!
//! ## Benchmark Results (10M elements, Apple Silicon)
//!
//! | Operation | wide crate | auto-vectorized | naive iter |
//! |-----------|------------|-----------------|------------|
//! | sum_f64   | 2.0 ms     | 2.0 ms (1.0x)   | 7.8 ms     |
//! | min_f64   | 1.5 ms     | 1.5 ms (1.0x)   | 1.4 ms     |
//!
//! ## WARNING
//!
//! DO NOT "simplify" these functions to use `.iter().sum()` or similar patterns.
//! While cleaner-looking, they can be 3-4x slower due to floating-point
//! associativity constraints preventing vectorization.
//!
//! If you need to modify these functions, run the SIMD benchmark first:
//! ```bash
//! cargo bench --bench tpch -- Q6
//! ```

mod aggregate;
mod comparison;
mod logical;

pub use aggregate::*;
pub use comparison::*;
pub use logical::*;

// ============================================================================
// PACKED BITMASK TYPE
// ============================================================================
// Using packed bitmasks (1 bit per row) instead of Vec<bool> (1 byte per row)
// provides 8x memory reduction and enables native SIMD bitwise operations.

/// A packed bitmask where each bit represents whether a row passes a filter.
///
/// # Memory Efficiency
///
/// For 6M rows (lineitem at TPC-H SF 1):
/// - `Vec<bool>`: 6 MB per predicate
/// - `PackedMask`: 750 KB per predicate (8x reduction)
///
/// # Performance Benefits
///
/// - Bitwise AND is a native SIMD instruction (`vpand` on x86, `and` on ARM)
/// - Better cache utilization due to reduced memory footprint
/// - Faster popcount using `count_ones()` intrinsic
///
/// # Implementation
///
/// Bits are stored in little-endian order within each u64 word:
/// - Bit 0 of word 0 = row 0
/// - Bit 63 of word 0 = row 63
/// - Bit 0 of word 1 = row 64
/// - etc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackedMask {
    /// The packed bits. Each u64 holds 64 row flags.
    words: Vec<u64>,
    /// The actual number of rows (since the last word may be partial).
    len: usize,
}

impl PackedMask {
    /// Creates a new mask with all bits set (all rows pass).
    #[inline]
    pub fn new_all_set(len: usize) -> Self {
        if len == 0 {
            return Self { words: vec![], len: 0 };
        }
        let num_words = len.div_ceil(64);
        let mut words = vec![u64::MAX; num_words];

        // Clear unused bits in the last word
        let remainder = len % 64;
        if remainder != 0 {
            words[num_words - 1] = (1u64 << remainder) - 1;
        }

        Self { words, len }
    }

    /// Creates a new mask with all bits clear (no rows pass).
    #[inline]
    pub fn new_all_clear(len: usize) -> Self {
        if len == 0 {
            return Self { words: vec![], len: 0 };
        }
        let num_words = len.div_ceil(64);
        Self { words: vec![0; num_words], len }
    }

    /// Returns the number of rows this mask represents.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if this mask is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Gets the value of the bit at the given index.
    #[inline]
    pub fn get(&self, index: usize) -> bool {
        debug_assert!(index < self.len, "index out of bounds");
        let word_idx = index / 64;
        let bit_idx = index % 64;
        (self.words[word_idx] >> bit_idx) & 1 == 1
    }

    /// Sets the bit at the given index.
    #[inline]
    pub fn set(&mut self, index: usize, value: bool) {
        debug_assert!(index < self.len, "index out of bounds");
        let word_idx = index / 64;
        let bit_idx = index % 64;
        if value {
            self.words[word_idx] |= 1u64 << bit_idx;
        } else {
            self.words[word_idx] &= !(1u64 << bit_idx);
        }
    }

    /// Performs in-place bitwise AND with another mask.
    ///
    /// This is the key operation for combining multiple predicates.
    /// Uses native bitwise AND which maps directly to SIMD instructions.
    #[inline]
    pub fn and_inplace(&mut self, other: &PackedMask) {
        debug_assert_eq!(self.len, other.len, "mask lengths must match");

        // Process in chunks of 4 for potential auto-vectorization
        let chunks = self.words.len() / 4;
        for i in 0..chunks {
            let base = i * 4;
            self.words[base] &= other.words[base];
            self.words[base + 1] &= other.words[base + 1];
            self.words[base + 2] &= other.words[base + 2];
            self.words[base + 3] &= other.words[base + 3];
        }

        // Handle remainder
        for i in (chunks * 4)..self.words.len() {
            self.words[i] &= other.words[i];
        }
    }

    /// Performs in-place bitwise OR with another mask.
    ///
    /// This is used for combining multiple predicates in IN lists.
    /// Uses native bitwise OR which maps directly to SIMD instructions.
    #[inline]
    pub fn or_inplace(&mut self, other: &PackedMask) {
        debug_assert_eq!(self.len, other.len, "mask lengths must match");

        // Process in chunks of 4 for potential auto-vectorization
        let chunks = self.words.len() / 4;
        for i in 0..chunks {
            let base = i * 4;
            self.words[base] |= other.words[base];
            self.words[base + 1] |= other.words[base + 1];
            self.words[base + 2] |= other.words[base + 2];
            self.words[base + 3] |= other.words[base + 3];
        }

        // Handle remainder
        for i in (chunks * 4)..self.words.len() {
            self.words[i] |= other.words[i];
        }
    }

    /// Returns a new mask with all bits inverted.
    ///
    /// This is used for NOT IN operations.
    #[inline]
    pub fn not(&self) -> PackedMask {
        let mut result = PackedMask { words: vec![0; self.words.len()], len: self.len };

        // Process in chunks of 4 for potential auto-vectorization
        let chunks = self.words.len() / 4;
        for i in 0..chunks {
            let base = i * 4;
            result.words[base] = !self.words[base];
            result.words[base + 1] = !self.words[base + 1];
            result.words[base + 2] = !self.words[base + 2];
            result.words[base + 3] = !self.words[base + 3];
        }

        // Handle remainder
        for i in (chunks * 4)..self.words.len() {
            result.words[i] = !self.words[i];
        }

        // Clear unused bits in the last word
        let remainder = self.len % 64;
        let num_words = result.words.len();
        if remainder != 0 && num_words > 0 {
            result.words[num_words - 1] &= (1u64 << remainder) - 1;
        }

        result
    }

    /// Counts the number of set bits (rows that pass the filter).
    ///
    /// Uses the `count_ones()` intrinsic which maps to hardware popcount.
    #[inline]
    pub fn count_ones(&self) -> usize {
        // Use 4-accumulator pattern for better throughput
        let chunks = self.words.len() / 4;
        let (mut c0, mut c1, mut c2, mut c3) = (0u32, 0u32, 0u32, 0u32);

        for i in 0..chunks {
            let base = i * 4;
            c0 += self.words[base].count_ones();
            c1 += self.words[base + 1].count_ones();
            c2 += self.words[base + 2].count_ones();
            c3 += self.words[base + 3].count_ones();
        }

        let mut total = (c0 + c1 + c2 + c3) as usize;
        for i in (chunks * 4)..self.words.len() {
            total += self.words[i].count_ones() as usize;
        }
        total
    }

    /// Creates a packed mask from a boolean slice.
    ///
    /// Used for compatibility with existing code during migration.
    #[inline]
    pub fn from_bool_slice(slice: &[bool]) -> Self {
        let len = slice.len();
        if len == 0 {
            return Self { words: vec![], len: 0 };
        }

        let num_words = len.div_ceil(64);
        let mut words = vec![0u64; num_words];

        // Process 64 bits at a time
        for (word_idx, word) in words.iter_mut().enumerate() {
            let start = word_idx * 64;
            let end = (start + 64).min(len);
            let mut bits = 0u64;
            for (bit_idx, &value) in slice[start..end].iter().enumerate() {
                if value {
                    bits |= 1u64 << bit_idx;
                }
            }
            *word = bits;
        }

        Self { words, len }
    }

    /// Converts this packed mask back to a boolean vector.
    ///
    /// Used for compatibility with existing code during migration.
    #[inline]
    pub fn to_bool_vec(&self) -> Vec<bool> {
        let mut result = Vec::with_capacity(self.len);
        for i in 0..self.len {
            result.push(self.get(i));
        }
        result
    }

    /// Returns a reference to the underlying word slice.
    #[inline]
    pub fn words(&self) -> &[u64] {
        &self.words
    }

    /// Creates a PackedMask from raw words and length.
    ///
    /// This is used internally by comparison functions that build masks.
    /// The caller must ensure the words are valid for the given length.
    #[inline]
    pub(crate) fn from_words(words: Vec<u64>, len: usize) -> Self {
        Self { words, len }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packed_mask_new_all_set() {
        let mask = PackedMask::new_all_set(100);
        assert_eq!(mask.len(), 100);
        for i in 0..100 {
            assert!(mask.get(i), "bit {} should be set", i);
        }

        // Empty mask
        let empty = PackedMask::new_all_set(0);
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
    }

    #[test]
    fn test_packed_mask_new_all_clear() {
        let mask = PackedMask::new_all_clear(100);
        assert_eq!(mask.len(), 100);
        for i in 0..100 {
            assert!(!mask.get(i), "bit {} should be clear", i);
        }
    }

    #[test]
    fn test_packed_mask_set_get() {
        let mut mask = PackedMask::new_all_clear(128);

        // Set some bits
        mask.set(0, true);
        mask.set(63, true);
        mask.set(64, true);
        mask.set(127, true);

        assert!(mask.get(0));
        assert!(mask.get(63));
        assert!(mask.get(64));
        assert!(mask.get(127));
        assert!(!mask.get(1));
        assert!(!mask.get(62));
        assert!(!mask.get(65));
    }

    #[test]
    fn test_packed_mask_and_inplace() {
        let mut mask_a = PackedMask::new_all_set(128);
        let mut mask_b = PackedMask::new_all_clear(128);

        // Set every other bit in mask_b
        for i in (0..128).step_by(2) {
            mask_b.set(i, true);
        }

        mask_a.and_inplace(&mask_b);

        for i in 0..128 {
            if i % 2 == 0 {
                assert!(mask_a.get(i), "bit {} should be set (even)", i);
            } else {
                assert!(!mask_a.get(i), "bit {} should be clear (odd)", i);
            }
        }
    }

    #[test]
    fn test_packed_mask_count_ones() {
        let mask = PackedMask::new_all_set(100);
        assert_eq!(mask.count_ones(), 100);

        let mask = PackedMask::new_all_clear(100);
        assert_eq!(mask.count_ones(), 0);

        let mut mask = PackedMask::new_all_clear(128);
        for i in (0..128).step_by(2) {
            mask.set(i, true);
        }
        assert_eq!(mask.count_ones(), 64);
    }

    #[test]
    fn test_packed_mask_from_bool_slice() {
        let bools = vec![true, false, true, true, false, true];
        let mask = PackedMask::from_bool_slice(&bools);

        assert_eq!(mask.len(), 6);
        assert!(mask.get(0));
        assert!(!mask.get(1));
        assert!(mask.get(2));
        assert!(mask.get(3));
        assert!(!mask.get(4));
        assert!(mask.get(5));
        assert_eq!(mask.count_ones(), 4);
    }

    #[test]
    fn test_packed_mask_to_bool_vec() {
        let mut mask = PackedMask::new_all_clear(6);
        mask.set(0, true);
        mask.set(2, true);
        mask.set(3, true);
        mask.set(5, true);

        let bools = mask.to_bool_vec();
        assert_eq!(bools, vec![true, false, true, true, false, true]);
    }

    #[test]
    fn test_packed_mask_round_trip() {
        let original = vec![true, false, true, true, false, true, false, false, true];
        let mask = PackedMask::from_bool_slice(&original);
        let result = mask.to_bool_vec();
        assert_eq!(original, result);
    }

    #[test]
    fn test_packed_mask_partial_word() {
        // Test with sizes that don't fill a complete u64 word
        for size in [1, 7, 15, 63, 65, 100, 127, 129] {
            let mask = PackedMask::new_all_set(size);
            assert_eq!(mask.count_ones(), size, "failed for size {}", size);

            let mask = PackedMask::new_all_clear(size);
            assert_eq!(mask.count_ones(), 0, "failed for size {}", size);

            // Test conversion round-trip
            let bools = vec![true; size];
            let mask = PackedMask::from_bool_slice(&bools);
            assert_eq!(mask.count_ones(), size, "round-trip failed for size {}", size);
            assert_eq!(mask.to_bool_vec(), bools, "round-trip failed for size {}", size);
        }
    }
}
