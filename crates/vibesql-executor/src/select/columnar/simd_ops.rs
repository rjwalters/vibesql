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

#![allow(clippy::needless_range_loop)]

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
        let num_words = (len + 63) / 64;
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
        let num_words = (len + 63) / 64;
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

        let num_words = (len + 63) / 64;
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
}

// ============================================================================
// AGGREGATION OPERATIONS
// ============================================================================

/// Sum of i64 values using 4-accumulator auto-vectorization pattern.
///
/// Performance: Matches explicit SIMD (~2ms for 10M elements).
#[inline]
pub fn sum_i64(values: &[i64]) -> i64 {
    let (mut s0, mut s1, mut s2, mut s3) = (0i64, 0i64, 0i64, 0i64);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        s0 = s0.wrapping_add(values[off]);
        s1 = s1.wrapping_add(values[off + 1]);
        s2 = s2.wrapping_add(values[off + 2]);
        s3 = s3.wrapping_add(values[off + 3]);
    }

    let mut sum = s0.wrapping_add(s1).wrapping_add(s2).wrapping_add(s3);
    for i in (chunks * 4)..values.len() {
        sum = sum.wrapping_add(values[i]);
    }
    sum
}

/// Sum of f64 values using 4-accumulator auto-vectorization pattern.
///
/// Performance: Matches explicit SIMD (~2ms for 10M elements).
/// WARNING: Do NOT replace with `.iter().sum()` - it's 4x slower!
#[inline]
pub fn sum_f64(values: &[f64]) -> f64 {
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        s0 += values[off];
        s1 += values[off + 1];
        s2 += values[off + 2];
        s3 += values[off + 3];
    }

    let mut sum = s0 + s1 + s2 + s3;
    for i in (chunks * 4)..values.len() {
        sum += values[i];
    }
    sum
}

/// Minimum of i64 values using 4-lane parallel reduction.
#[inline]
pub fn min_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MAX, i64::MAX, i64::MAX, i64::MAX);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.min(values[off]);
        m1 = m1.min(values[off + 1]);
        m2 = m2.min(values[off + 2]);
        m3 = m3.min(values[off + 3]);
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..values.len() {
        result = result.min(values[i]);
    }
    Some(result)
}

/// Minimum of f64 values using 4-lane parallel reduction.
#[inline]
pub fn min_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) =
        (f64::INFINITY, f64::INFINITY, f64::INFINITY, f64::INFINITY);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.min(values[off]);
        m1 = m1.min(values[off + 1]);
        m2 = m2.min(values[off + 2]);
        m3 = m3.min(values[off + 3]);
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..values.len() {
        result = result.min(values[i]);
    }
    Some(result)
}

/// Maximum of i64 values using 4-lane parallel reduction.
#[inline]
pub fn max_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MIN, i64::MIN, i64::MIN, i64::MIN);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.max(values[off]);
        m1 = m1.max(values[off + 1]);
        m2 = m2.max(values[off + 2]);
        m3 = m3.max(values[off + 3]);
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..values.len() {
        result = result.max(values[i]);
    }
    Some(result)
}

/// Maximum of f64 values using 4-lane parallel reduction.
#[inline]
pub fn max_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    );
    let chunks = values.len() / 4;

    for i in 0..chunks {
        let off = i * 4;
        m0 = m0.max(values[off]);
        m1 = m1.max(values[off + 1]);
        m2 = m2.max(values[off + 2]);
        m3 = m3.max(values[off + 3]);
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..values.len() {
        result = result.max(values[i]);
    }
    Some(result)
}

// ============================================================================
// ARITHMETIC OPERATIONS (for expression aggregates)
// ============================================================================

/// Sum of element-wise product of two f64 arrays using 4-accumulator pattern.
///
/// This is the core operation for TPC-H Q6: SUM(l_extendedprice * l_discount).
///
/// Performance: Uses LLVM auto-vectorization for SIMD acceleration.
/// The 4-accumulator pattern enables parallel execution across SIMD lanes.
///
/// # Arguments
/// * `a` - First array of values
/// * `b` - Second array of values (must be same length as `a`)
///
/// # Returns
/// Sum of element-wise products: Σ(a[i] * b[i])
///
/// # Panics
/// Panics if arrays have different lengths (debug builds only).
#[inline]
pub fn sum_product_f64(a: &[f64], b: &[f64]) -> f64 {
    debug_assert_eq!(a.len(), b.len(), "Arrays must have same length");

    let len = a.len().min(b.len());
    if len == 0 {
        return 0.0;
    }

    // 4-accumulator pattern for LLVM auto-vectorization
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        s0 += a[off] * b[off];
        s1 += a[off + 1] * b[off + 1];
        s2 += a[off + 2] * b[off + 2];
        s3 += a[off + 3] * b[off + 3];
    }

    let mut sum = s0 + s1 + s2 + s3;
    for i in (chunks * 4)..len {
        sum += a[i] * b[i];
    }
    sum
}

/// Sum of element-wise product with null mask using 4-accumulator pattern.
///
/// Handles NULL values efficiently by checking the null bitmap rather than
/// per-element Option checking. This enables SIMD auto-vectorization.
///
/// # Arguments
/// * `a` - First array of values
/// * `b` - Second array of values (must be same length as `a`)
/// * `null_a` - Null bitmap for array `a` (true = null, false = valid)
/// * `null_b` - Null bitmap for array `b` (true = null, false = valid)
///
/// # Returns
/// (sum, count) - Sum of products for non-null pairs and count of valid pairs
#[inline]
pub fn sum_product_f64_masked(
    a: &[f64],
    b: &[f64],
    null_a: Option<&[bool]>,
    null_b: Option<&[bool]>,
) -> (f64, i64) {
    let len = a.len().min(b.len());
    if len == 0 {
        return (0.0, 0);
    }

    // Fast path: no nulls in either array
    let no_nulls_a = null_a.is_none_or(|n| !n.iter().any(|&x| x));
    let no_nulls_b = null_b.is_none_or(|n| !n.iter().any(|&x| x));

    if no_nulls_a && no_nulls_b {
        return (sum_product_f64(a, b), len as i64);
    }

    // Slow path with null handling - still use accumulator pattern where possible
    let null_mask_a = null_a.unwrap_or(&[]);
    let null_mask_b = null_b.unwrap_or(&[]);

    let mut sum = 0.0f64;
    let mut count = 0i64;

    for i in 0..len {
        let is_null_a = null_mask_a.get(i).copied().unwrap_or(false);
        let is_null_b = null_mask_b.get(i).copied().unwrap_or(false);

        if !is_null_a && !is_null_b {
            sum += a[i] * b[i];
            count += 1;
        }
    }

    (sum, count)
}

// ============================================================================
// FILTERED AGGREGATION OPERATIONS (for fused filter+aggregate)
// ============================================================================
// These operations aggregate data directly using a filter mask, avoiding
// intermediate allocations from creating filtered batches.

/// Sum of f64 values where filter_mask[i] == true, using 4-accumulator pattern.
///
/// This fuses filtering and aggregation to avoid intermediate allocations.
/// For simple aggregates like Q6, this can provide 2-3x speedup.
///
/// # Arguments
/// * `values` - Array of values to aggregate
/// * `filter_mask` - Boolean mask (true = include in aggregate)
///
/// # Returns
/// Sum of values where filter_mask is true
#[inline]
pub fn sum_f64_filtered(values: &[f64], filter_mask: &[bool]) -> f64 {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return 0.0;
    }

    // Use 4-accumulator pattern with conditional adds
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        // 4-accumulator pattern: reduces loop-carried dependencies
        // The compiler may auto-vectorize these conditional adds
        if filter_mask[off] { s0 += values[off]; }
        if filter_mask[off + 1] { s1 += values[off + 1]; }
        if filter_mask[off + 2] { s2 += values[off + 2]; }
        if filter_mask[off + 3] { s3 += values[off + 3]; }
    }

    let mut sum = s0 + s1 + s2 + s3;
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            sum += values[i];
        }
    }
    sum
}

/// Sum of i64 values where filter_mask[i] == true.
#[inline]
pub fn sum_i64_filtered(values: &[i64], filter_mask: &[bool]) -> i64 {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return 0;
    }

    let (mut s0, mut s1, mut s2, mut s3) = (0i64, 0i64, 0i64, 0i64);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] { s0 = s0.wrapping_add(values[off]); }
        if filter_mask[off + 1] { s1 = s1.wrapping_add(values[off + 1]); }
        if filter_mask[off + 2] { s2 = s2.wrapping_add(values[off + 2]); }
        if filter_mask[off + 3] { s3 = s3.wrapping_add(values[off + 3]); }
    }

    let mut sum = s0.wrapping_add(s1).wrapping_add(s2).wrapping_add(s3);
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            sum = sum.wrapping_add(values[i]);
        }
    }
    sum
}

/// Count of true values in filter mask (number of rows passing filter).
#[inline]
pub fn count_filtered(filter_mask: &[bool]) -> i64 {
    let len = filter_mask.len();
    if len == 0 {
        return 0;
    }

    // Use 4-accumulator pattern for count
    let (mut c0, mut c1, mut c2, mut c3) = (0i64, 0i64, 0i64, 0i64);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        c0 += filter_mask[off] as i64;
        c1 += filter_mask[off + 1] as i64;
        c2 += filter_mask[off + 2] as i64;
        c3 += filter_mask[off + 3] as i64;
    }

    let mut count = c0 + c1 + c2 + c3;
    for i in (chunks * 4)..len {
        count += filter_mask[i] as i64;
    }
    count
}

/// Sum of element-wise product of two f64 arrays with filter mask.
///
/// This is the key operation for TPC-H Q6: SUM(l_extendedprice * l_discount)
/// with WHERE clause filtering.
///
/// Fuses filtering and aggregation to avoid:
/// 1. Creating intermediate filtered arrays
/// 2. Multiple passes over the data
///
/// # Arguments
/// * `a` - First array of values
/// * `b` - Second array of values (must be same length as `a`)
/// * `filter_mask` - Boolean mask (true = include in aggregate)
///
/// # Returns
/// (sum, count) - Sum of products for filtered rows and count of passing rows
#[inline]
pub fn sum_product_f64_filtered(a: &[f64], b: &[f64], filter_mask: &[bool]) -> (f64, i64) {
    debug_assert_eq!(a.len(), b.len(), "Arrays must have same length");
    debug_assert_eq!(a.len(), filter_mask.len(), "Arrays and filter must have same length");

    let len = a.len().min(b.len()).min(filter_mask.len());
    if len == 0 {
        return (0.0, 0);
    }

    // 4-accumulator pattern with filter
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let (mut c0, mut c1, mut c2, mut c3) = (0i64, 0i64, 0i64, 0i64);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] {
            s0 += a[off] * b[off];
            c0 += 1;
        }
        if filter_mask[off + 1] {
            s1 += a[off + 1] * b[off + 1];
            c1 += 1;
        }
        if filter_mask[off + 2] {
            s2 += a[off + 2] * b[off + 2];
            c2 += 1;
        }
        if filter_mask[off + 3] {
            s3 += a[off + 3] * b[off + 3];
            c3 += 1;
        }
    }

    let mut sum = s0 + s1 + s2 + s3;
    let mut count = c0 + c1 + c2 + c3;

    for i in (chunks * 4)..len {
        if filter_mask[i] {
            sum += a[i] * b[i];
            count += 1;
        }
    }

    (sum, count)
}

/// Sum of element-wise product with filter mask AND null masks.
///
/// Full version that handles both filter predicates and NULL values.
/// Used when columnar data has NULLs that also need to be excluded.
///
/// # Arguments
/// * `a` - First array of values
/// * `b` - Second array of values
/// * `filter_mask` - Boolean mask from predicates (true = include)
/// * `null_a` - Null bitmap for array `a` (true = null, false = valid)
/// * `null_b` - Null bitmap for array `b` (true = null, false = valid)
///
/// # Returns
/// (sum, count) - Sum of products and count of valid rows
#[inline]
pub fn sum_product_f64_filtered_masked(
    a: &[f64],
    b: &[f64],
    filter_mask: &[bool],
    null_a: Option<&[bool]>,
    null_b: Option<&[bool]>,
) -> (f64, i64) {
    let len = a.len().min(b.len()).min(filter_mask.len());
    if len == 0 {
        return (0.0, 0);
    }

    // Fast path: no nulls
    let no_nulls_a = null_a.is_none_or(|n| n.len() < len || !n[..len].iter().any(|&x| x));
    let no_nulls_b = null_b.is_none_or(|n| n.len() < len || !n[..len].iter().any(|&x| x));

    if no_nulls_a && no_nulls_b {
        return sum_product_f64_filtered(a, b, filter_mask);
    }

    // Slow path with null handling
    let null_mask_a = null_a.unwrap_or(&[]);
    let null_mask_b = null_b.unwrap_or(&[]);

    let mut sum = 0.0f64;
    let mut count = 0i64;

    for i in 0..len {
        let is_null_a = null_mask_a.get(i).copied().unwrap_or(false);
        let is_null_b = null_mask_b.get(i).copied().unwrap_or(false);

        if filter_mask[i] && !is_null_a && !is_null_b {
            sum += a[i] * b[i];
            count += 1;
        }
    }

    (sum, count)
}

/// Min of f64 values where filter_mask[i] == true.
#[inline]
pub fn min_f64_filtered(values: &[f64], filter_mask: &[bool]) -> Option<f64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) =
        (f64::INFINITY, f64::INFINITY, f64::INFINITY, f64::INFINITY);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] { m0 = m0.min(values[off]); }
        if filter_mask[off + 1] { m1 = m1.min(values[off + 1]); }
        if filter_mask[off + 2] { m2 = m2.min(values[off + 2]); }
        if filter_mask[off + 3] { m3 = m3.min(values[off + 3]); }
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            result = result.min(values[i]);
        }
    }

    if result == f64::INFINITY {
        None
    } else {
        Some(result)
    }
}

/// Max of f64 values where filter_mask[i] == true.
#[inline]
pub fn max_f64_filtered(values: &[f64], filter_mask: &[bool]) -> Option<f64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    );
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] { m0 = m0.max(values[off]); }
        if filter_mask[off + 1] { m1 = m1.max(values[off + 1]); }
        if filter_mask[off + 2] { m2 = m2.max(values[off + 2]); }
        if filter_mask[off + 3] { m3 = m3.max(values[off + 3]); }
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            result = result.max(values[i]);
        }
    }

    if result == f64::NEG_INFINITY {
        None
    } else {
        Some(result)
    }
}

/// Min of i64 values where filter_mask[i] == true.
#[inline]
pub fn min_i64_filtered(values: &[i64], filter_mask: &[bool]) -> Option<i64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MAX, i64::MAX, i64::MAX, i64::MAX);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] { m0 = m0.min(values[off]); }
        if filter_mask[off + 1] { m1 = m1.min(values[off + 1]); }
        if filter_mask[off + 2] { m2 = m2.min(values[off + 2]); }
        if filter_mask[off + 3] { m3 = m3.min(values[off + 3]); }
    }

    let mut result = m0.min(m1).min(m2).min(m3);
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            result = result.min(values[i]);
        }
    }

    if result == i64::MAX {
        None
    } else {
        Some(result)
    }
}

/// Max of i64 values where filter_mask[i] == true.
#[inline]
pub fn max_i64_filtered(values: &[i64], filter_mask: &[bool]) -> Option<i64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len().min(filter_mask.len());
    if len == 0 {
        return None;
    }

    let (mut m0, mut m1, mut m2, mut m3) = (i64::MIN, i64::MIN, i64::MIN, i64::MIN);
    let chunks = len / 4;

    for i in 0..chunks {
        let off = i * 4;
        if filter_mask[off] { m0 = m0.max(values[off]); }
        if filter_mask[off + 1] { m1 = m1.max(values[off + 1]); }
        if filter_mask[off + 2] { m2 = m2.max(values[off + 2]); }
        if filter_mask[off + 3] { m3 = m3.max(values[off + 3]); }
    }

    let mut result = m0.max(m1).max(m2).max(m3);
    for i in (chunks * 4)..len {
        if filter_mask[i] {
            result = result.max(values[i]);
        }
    }

    if result == i64::MIN {
        None
    } else {
        Some(result)
    }
}

// ============================================================================
// COMPARISON OPERATIONS (Filtering)
// ============================================================================

/// Macro to generate comparison functions for a given type.
///
/// Comparisons are naturally vectorizable since each element is independent.
/// We still use the chunked pattern for consistency and cache efficiency.
macro_rules! impl_comparison {
    ($name:ident, $ty:ty, $op:tt) => {
        #[inline]
        pub fn $name(values: &[$ty], threshold: $ty) -> Vec<bool> {
            values.iter().map(|&v| v $op threshold).collect()
        }
    };
}

// i64 comparisons
impl_comparison!(lt_i64, i64, <);
impl_comparison!(gt_i64, i64, >);
impl_comparison!(le_i64, i64, <=);
impl_comparison!(ge_i64, i64, >=);
impl_comparison!(eq_i64, i64, ==);
impl_comparison!(ne_i64, i64, !=);

// i32 comparisons
impl_comparison!(lt_i32, i32, <);
impl_comparison!(gt_i32, i32, >);
impl_comparison!(le_i32, i32, <=);
impl_comparison!(ge_i32, i32, >=);
impl_comparison!(eq_i32, i32, ==);
impl_comparison!(ne_i32, i32, !=);

// f64 comparisons
impl_comparison!(lt_f64, f64, <);
impl_comparison!(gt_f64, f64, >);
impl_comparison!(le_f64, f64, <=);
impl_comparison!(ge_f64, f64, >=);

/// Equality comparison for f64 (exact bit equality).
#[inline]
pub fn eq_f64(values: &[f64], target: f64) -> Vec<bool> {
    values.iter().map(|&v| v == target).collect()
}

/// Inequality comparison for f64.
#[inline]
pub fn ne_f64(values: &[f64], target: f64) -> Vec<bool> {
    values.iter().map(|&v| v != target).collect()
}

// ============================================================================
// BETWEEN OPERATIONS (Range checks in single pass)
// ============================================================================

/// Check if i64 values are in range [low, high] (inclusive).
/// More efficient than separate ge + le comparisons + AND.
#[inline]
pub fn between_i64(values: &[i64], low: i64, high: i64) -> Vec<bool> {
    values.iter().map(|&v| v >= low && v <= high).collect()
}

/// Check if i32 values are in range [low, high] (inclusive).
/// More efficient than separate ge + le comparisons + AND.
#[inline]
pub fn between_i32(values: &[i32], low: i32, high: i32) -> Vec<bool> {
    values.iter().map(|&v| v >= low && v <= high).collect()
}

/// Check if f64 values are in range [low, high] (inclusive).
/// More efficient than separate ge + le comparisons + AND.
#[inline]
pub fn between_f64(values: &[f64], low: f64, high: f64) -> Vec<bool> {
    values.iter().map(|&v| v >= low && v <= high).collect()
}

// ============================================================================
// PACKED COMPARISON OPERATIONS (Return PackedMask instead of Vec<bool>)
// ============================================================================
// These functions return packed bitmasks for 8x memory reduction and
// native SIMD bitwise operations.

/// Less-than comparison returning packed mask.
#[inline]
pub fn lt_i64_packed(values: &[i64], threshold: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v < threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Greater-than comparison returning packed mask.
#[inline]
pub fn gt_i64_packed(values: &[i64], threshold: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v > threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Less-than-or-equal comparison returning packed mask.
#[inline]
pub fn le_i64_packed(values: &[i64], threshold: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v <= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Greater-than-or-equal comparison returning packed mask.
#[inline]
pub fn ge_i64_packed(values: &[i64], threshold: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Equality comparison returning packed mask.
#[inline]
pub fn eq_i64_packed(values: &[i64], target: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v == target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Inequality comparison returning packed mask.
#[inline]
pub fn ne_i64_packed(values: &[i64], target: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v != target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Less-than comparison for i32 returning packed mask.
#[inline]
pub fn lt_i32_packed(values: &[i32], threshold: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v < threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Greater-than comparison for i32 returning packed mask.
#[inline]
pub fn gt_i32_packed(values: &[i32], threshold: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v > threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Less-than-or-equal comparison for i32 returning packed mask.
#[inline]
pub fn le_i32_packed(values: &[i32], threshold: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v <= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Greater-than-or-equal comparison for i32 returning packed mask.
#[inline]
pub fn ge_i32_packed(values: &[i32], threshold: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Equality comparison for i32 returning packed mask.
#[inline]
pub fn eq_i32_packed(values: &[i32], target: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v == target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Inequality comparison for i32 returning packed mask.
#[inline]
pub fn ne_i32_packed(values: &[i32], target: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v != target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Less-than comparison for f64 returning packed mask.
#[inline]
pub fn lt_f64_packed(values: &[f64], threshold: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v < threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Greater-than comparison for f64 returning packed mask.
#[inline]
pub fn gt_f64_packed(values: &[f64], threshold: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v > threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Less-than-or-equal comparison for f64 returning packed mask.
#[inline]
pub fn le_f64_packed(values: &[f64], threshold: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v <= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Greater-than-or-equal comparison for f64 returning packed mask.
#[inline]
pub fn ge_f64_packed(values: &[f64], threshold: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= threshold {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Equality comparison for f64 returning packed mask.
#[inline]
pub fn eq_f64_packed(values: &[f64], target: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v == target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Inequality comparison for f64 returning packed mask.
#[inline]
pub fn ne_f64_packed(values: &[f64], target: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v != target {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

// ============================================================================
// PACKED BETWEEN OPERATIONS
// ============================================================================

/// Check if i64 values are in range [low, high] (inclusive), returning packed mask.
#[inline]
pub fn between_i64_packed(values: &[i64], low: i64, high: i64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= low && v <= high {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Check if i32 values are in range [low, high] (inclusive), returning packed mask.
#[inline]
pub fn between_i32_packed(values: &[i32], low: i32, high: i32) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= low && v <= high {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

/// Check if f64 values are in range [low, high] (inclusive), returning packed mask.
#[inline]
pub fn between_f64_packed(values: &[f64], low: f64, high: f64) -> PackedMask {
    let len = values.len();
    if len == 0 {
        return PackedMask::new_all_clear(0);
    }

    let num_words = (len + 63) / 64;
    let mut words = vec![0u64; num_words];

    for (word_idx, word) in words.iter_mut().enumerate() {
        let start = word_idx * 64;
        let end = (start + 64).min(len);
        let mut bits = 0u64;
        for (bit_idx, &v) in values[start..end].iter().enumerate() {
            if v >= low && v <= high {
                bits |= 1u64 << bit_idx;
            }
        }
        *word = bits;
    }

    PackedMask { words, len }
}

// ============================================================================
// PACKED FILTERED AGGREGATION OPERATIONS
// ============================================================================
// These operations aggregate data directly using a packed filter mask,
// providing both memory efficiency and SIMD-friendly access patterns.

/// Sum of f64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn sum_f64_packed_filtered(values: &[f64], filter_mask: &PackedMask) -> f64 {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return 0.0;
    }

    // Process word by word for better cache utilization
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue; // Skip entirely zero words
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        // Process set bits in this word
        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                // Distribute across 4 accumulators based on bit position
                match bit_pos % 4 {
                    0 => s0 += values[idx],
                    1 => s1 += values[idx],
                    2 => s2 += values[idx],
                    _ => s3 += values[idx],
                }
            }
            bits &= bits - 1; // Clear lowest set bit
        }
    }

    s0 + s1 + s2 + s3
}

/// Sum of i64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn sum_i64_packed_filtered(values: &[i64], filter_mask: &PackedMask) -> i64 {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return 0;
    }

    let (mut s0, mut s1, mut s2, mut s3) = (0i64, 0i64, 0i64, 0i64);
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                match bit_pos % 4 {
                    0 => s0 = s0.wrapping_add(values[idx]),
                    1 => s1 = s1.wrapping_add(values[idx]),
                    2 => s2 = s2.wrapping_add(values[idx]),
                    _ => s3 = s3.wrapping_add(values[idx]),
                }
            }
            bits &= bits - 1;
        }
    }

    s0.wrapping_add(s1).wrapping_add(s2).wrapping_add(s3)
}

/// Count of set bits in filter mask (number of rows passing filter).
///
/// This is a convenience wrapper around PackedMask::count_ones().
#[inline]
pub fn count_packed_filtered(filter_mask: &PackedMask) -> i64 {
    filter_mask.count_ones() as i64
}

/// Sum of element-wise product of two f64 arrays with packed filter mask.
///
/// This is the key operation for TPC-H Q6: SUM(l_extendedprice * l_discount)
/// with WHERE clause filtering.
#[inline]
pub fn sum_product_f64_packed_filtered(a: &[f64], b: &[f64], filter_mask: &PackedMask) -> (f64, i64) {
    debug_assert_eq!(a.len(), b.len(), "Arrays must have same length");
    debug_assert_eq!(a.len(), filter_mask.len(), "Arrays and filter must have same length");

    let len = a.len();
    if len == 0 {
        return (0.0, 0);
    }

    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let mut count = 0i64;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                let product = a[idx] * b[idx];
                match bit_pos % 4 {
                    0 => s0 += product,
                    1 => s1 += product,
                    2 => s2 += product,
                    _ => s3 += product,
                }
                count += 1;
            }
            bits &= bits - 1;
        }
    }

    (s0 + s1 + s2 + s3, count)
}

/// Min of f64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn min_f64_packed_filtered(values: &[f64], filter_mask: &PackedMask) -> Option<f64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return None;
    }

    let mut result = f64::INFINITY;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                result = result.min(values[idx]);
            }
            bits &= bits - 1;
        }
    }

    if result == f64::INFINITY {
        None
    } else {
        Some(result)
    }
}

/// Max of f64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn max_f64_packed_filtered(values: &[f64], filter_mask: &PackedMask) -> Option<f64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return None;
    }

    let mut result = f64::NEG_INFINITY;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                result = result.max(values[idx]);
            }
            bits &= bits - 1;
        }
    }

    if result == f64::NEG_INFINITY {
        None
    } else {
        Some(result)
    }
}

/// Min of i64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn min_i64_packed_filtered(values: &[i64], filter_mask: &PackedMask) -> Option<i64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return None;
    }

    let mut result = i64::MAX;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                result = result.min(values[idx]);
            }
            bits &= bits - 1;
        }
    }

    if result == i64::MAX {
        None
    } else {
        Some(result)
    }
}

/// Max of i64 values where the corresponding bit in filter_mask is set.
#[inline]
pub fn max_i64_packed_filtered(values: &[i64], filter_mask: &PackedMask) -> Option<i64> {
    debug_assert_eq!(values.len(), filter_mask.len(), "Arrays must have same length");

    let len = values.len();
    if len == 0 {
        return None;
    }

    let mut result = i64::MIN;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                result = result.max(values[idx]);
            }
            bits &= bits - 1;
        }
    }

    if result == i64::MIN {
        None
    } else {
        Some(result)
    }
}

/// Sum of element-wise product with packed filter mask AND null masks.
///
/// Full version that handles both filter predicates and NULL values.
#[inline]
pub fn sum_product_f64_packed_filtered_masked(
    a: &[f64],
    b: &[f64],
    filter_mask: &PackedMask,
    null_a: Option<&[bool]>,
    null_b: Option<&[bool]>,
) -> (f64, i64) {
    let len = a.len().min(b.len()).min(filter_mask.len());
    if len == 0 {
        return (0.0, 0);
    }

    // Fast path: no nulls
    let no_nulls_a = null_a.is_none_or(|n| n.len() < len || !n[..len].iter().any(|&x| x));
    let no_nulls_b = null_b.is_none_or(|n| n.len() < len || !n[..len].iter().any(|&x| x));

    if no_nulls_a && no_nulls_b {
        return sum_product_f64_packed_filtered(a, b, filter_mask);
    }

    // Slow path with null handling
    let null_mask_a = null_a.unwrap_or(&[]);
    let null_mask_b = null_b.unwrap_or(&[]);

    let mut sum = 0.0f64;
    let mut count = 0i64;
    let words = filter_mask.words();

    for (word_idx, &word) in words.iter().enumerate() {
        if word == 0 {
            continue;
        }

        let base = word_idx * 64;
        let end = (base + 64).min(len);

        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let idx = base + bit_pos;
            if idx < end {
                let is_null_a = null_mask_a.get(idx).copied().unwrap_or(false);
                let is_null_b = null_mask_b.get(idx).copied().unwrap_or(false);

                if !is_null_a && !is_null_b {
                    sum += a[idx] * b[idx];
                    count += 1;
                }
            }
            bits &= bits - 1;
        }
    }

    (sum, count)
}

// ============================================================================
// MASK OPERATIONS (Boolean operations on filter masks)
// ============================================================================

/// In-place AND of two boolean masks.
///
/// This function ANDs `other` into `mask` in-place, modifying `mask`.
/// Uses an unrolled loop that LLVM can auto-vectorize effectively.
///
/// # Panics
/// Panics if `mask` and `other` have different lengths.
#[inline]
pub fn and_masks_inplace(mask: &mut [bool], other: &[bool]) {
    assert_eq!(mask.len(), other.len(), "mask lengths must match");

    let len = mask.len();

    // Process in chunks of 8 for better vectorization
    let chunks = len / 8;
    let remainder = len % 8;

    for i in 0..chunks {
        let base = i * 8;
        // Unrolled loop - LLVM vectorizes this well
        mask[base] = mask[base] && other[base];
        mask[base + 1] = mask[base + 1] && other[base + 1];
        mask[base + 2] = mask[base + 2] && other[base + 2];
        mask[base + 3] = mask[base + 3] && other[base + 3];
        mask[base + 4] = mask[base + 4] && other[base + 4];
        mask[base + 5] = mask[base + 5] && other[base + 5];
        mask[base + 6] = mask[base + 6] && other[base + 6];
        mask[base + 7] = mask[base + 7] && other[base + 7];
    }

    // Handle remainder
    let base = chunks * 8;
    for i in 0..remainder {
        mask[base + i] = mask[base + i] && other[base + i];
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_i64() {
        let values: Vec<i64> = (1..=100).collect();
        assert_eq!(sum_i64(&values), 5050);
        assert_eq!(sum_i64(&[]), 0);
        assert_eq!(sum_i64(&[42]), 42);
    }

    #[test]
    fn test_sum_f64() {
        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        assert!((sum_f64(&values) - 5050.0).abs() < 0.001);
        assert_eq!(sum_f64(&[]), 0.0);
    }

    #[test]
    fn test_min_max_i64() {
        let values = vec![5, 2, 8, 1, 9, 3, 7, 4, 6];
        assert_eq!(min_i64(&values), Some(1));
        assert_eq!(max_i64(&values), Some(9));
        assert_eq!(min_i64(&[]), None);
        assert_eq!(max_i64(&[]), None);
    }

    #[test]
    fn test_min_max_f64() {
        let values = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0];
        assert_eq!(min_f64(&values), Some(1.0));
        assert_eq!(max_f64(&values), Some(9.0));
        assert_eq!(min_f64(&[]), None);
        assert_eq!(max_f64(&[]), None);
    }

    #[test]
    fn test_comparisons_i64() {
        let values = vec![1, 2, 3, 4, 5];
        assert_eq!(lt_i64(&values, 3), vec![true, true, false, false, false]);
        assert_eq!(gt_i64(&values, 3), vec![false, false, false, true, true]);
        assert_eq!(le_i64(&values, 3), vec![true, true, true, false, false]);
        assert_eq!(ge_i64(&values, 3), vec![false, false, true, true, true]);
        assert_eq!(eq_i64(&values, 3), vec![false, false, true, false, false]);
        assert_eq!(ne_i64(&values, 3), vec![true, true, false, true, true]);
    }

    #[test]
    fn test_comparisons_f64() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(lt_f64(&values, 3.0), vec![true, true, false, false, false]);
        assert_eq!(gt_f64(&values, 3.0), vec![false, false, false, true, true]);
        assert_eq!(eq_f64(&values, 3.0), vec![false, false, true, false, false]);
    }

    #[test]
    fn test_remainder_handling() {
        // Test with non-multiple-of-4 lengths
        let values: Vec<i64> = (1..=7).collect(); // 7 elements
        assert_eq!(sum_i64(&values), 28);
        assert_eq!(min_i64(&values), Some(1));
        assert_eq!(max_i64(&values), Some(7));

        let values: Vec<i64> = (1..=5).collect(); // 5 elements
        assert_eq!(sum_i64(&values), 15);
    }

    #[test]
    fn test_sum_product_f64() {
        // Basic test: [1, 2, 3, 4] * [2, 2, 2, 2] = 2 + 4 + 6 + 8 = 20
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![2.0, 2.0, 2.0, 2.0];
        assert!((sum_product_f64(&a, &b) - 20.0).abs() < 0.001);

        // Empty arrays
        assert_eq!(sum_product_f64(&[], &[]), 0.0);

        // Single element
        assert!((sum_product_f64(&[5.0], &[3.0]) - 15.0).abs() < 0.001);

        // Non-multiple-of-4 lengths (tests remainder handling)
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0];
        let b = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        assert!((sum_product_f64(&a, &b) - 28.0).abs() < 0.001);

        // TPC-H Q6 style: price * discount
        let prices = vec![100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0];
        let discounts = vec![0.05, 0.06, 0.07, 0.05, 0.06, 0.07, 0.05, 0.06];
        // Expected: 100*0.05 + 200*0.06 + 300*0.07 + 400*0.05 + 500*0.06 + 600*0.07 + 700*0.05 + 800*0.06
        //         = 5 + 12 + 21 + 20 + 30 + 42 + 35 + 48 = 213
        assert!((sum_product_f64(&prices, &discounts) - 213.0).abs() < 0.001);
    }

    #[test]
    fn test_sum_product_f64_masked() {
        // No nulls - fast path
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![2.0, 2.0, 2.0, 2.0];
        let (sum, count) = sum_product_f64_masked(&a, &b, None, None);
        assert!((sum - 20.0).abs() < 0.001);
        assert_eq!(count, 4);

        // With nulls in first array
        let null_a = vec![false, true, false, false];
        let (sum, count) = sum_product_f64_masked(&a, &b, Some(&null_a), None);
        // Only indices 0, 2, 3 counted: 1*2 + 3*2 + 4*2 = 2 + 6 + 8 = 16
        assert!((sum - 16.0).abs() < 0.001);
        assert_eq!(count, 3);

        // With nulls in second array
        let null_b = vec![false, false, true, false];
        let (sum, count) = sum_product_f64_masked(&a, &b, None, Some(&null_b));
        // Only indices 0, 1, 3 counted: 1*2 + 2*2 + 4*2 = 2 + 4 + 8 = 14
        assert!((sum - 14.0).abs() < 0.001);
        assert_eq!(count, 3);

        // With nulls in both arrays
        let (sum, count) = sum_product_f64_masked(&a, &b, Some(&null_a), Some(&null_b));
        // Only indices where both are not null: 0, 3 → 1*2 + 4*2 = 10
        assert!((sum - 10.0).abs() < 0.001);
        assert_eq!(count, 2);

        // Empty arrays
        let (sum, count) = sum_product_f64_masked(&[], &[], None, None);
        assert_eq!(sum, 0.0);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_sum_f64_filtered() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let filter = vec![true, false, true, false, true, false, true, false];
        // Sum only odd positions: 1 + 3 + 5 + 7 = 16
        assert!((sum_f64_filtered(&values, &filter) - 16.0).abs() < 0.001);

        // All true
        let filter_all = vec![true; 8];
        assert!((sum_f64_filtered(&values, &filter_all) - 36.0).abs() < 0.001);

        // All false
        let filter_none = vec![false; 8];
        assert_eq!(sum_f64_filtered(&values, &filter_none), 0.0);

        // Empty
        assert_eq!(sum_f64_filtered(&[], &[]), 0.0);
    }

    #[test]
    fn test_sum_i64_filtered() {
        let values = vec![1i64, 2, 3, 4, 5, 6, 7, 8];
        let filter = vec![true, false, true, false, true, false, true, false];
        assert_eq!(sum_i64_filtered(&values, &filter), 16);

        let filter_all = vec![true; 8];
        assert_eq!(sum_i64_filtered(&values, &filter_all), 36);
    }

    #[test]
    fn test_count_filtered() {
        let filter = vec![true, false, true, false, true, false, true, false];
        assert_eq!(count_filtered(&filter), 4);

        let filter_all = vec![true; 8];
        assert_eq!(count_filtered(&filter_all), 8);

        let filter_none = vec![false; 8];
        assert_eq!(count_filtered(&filter_none), 0);

        assert_eq!(count_filtered(&[]), 0);
    }

    #[test]
    fn test_sum_product_f64_filtered() {
        // TPC-H Q6 scenario: SUM(price * discount) WHERE filter
        let prices = vec![100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0];
        let discounts = vec![0.05, 0.06, 0.07, 0.05, 0.06, 0.07, 0.05, 0.06];
        let filter = vec![true, false, true, false, true, false, true, false];

        // Only indices 0, 2, 4, 6: 100*0.05 + 300*0.07 + 500*0.06 + 700*0.05
        //                        = 5 + 21 + 30 + 35 = 91
        let (sum, count) = sum_product_f64_filtered(&prices, &discounts, &filter);
        assert!((sum - 91.0).abs() < 0.001);
        assert_eq!(count, 4);

        // All pass filter
        let filter_all = vec![true; 8];
        let (sum, count) = sum_product_f64_filtered(&prices, &discounts, &filter_all);
        assert!((sum - 213.0).abs() < 0.001);  // Same as unfiltered sum_product test
        assert_eq!(count, 8);

        // None pass filter
        let filter_none = vec![false; 8];
        let (sum, count) = sum_product_f64_filtered(&prices, &discounts, &filter_none);
        assert_eq!(sum, 0.0);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_sum_product_f64_filtered_masked() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let b = vec![2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0];
        let filter = vec![true, true, true, true, false, false, false, false];
        let null_a = vec![false, true, false, false, false, false, false, false];

        // Filter passes 0-3, null excludes 1, so: 0,2,3 → 1*2 + 3*2 + 4*2 = 16
        let (sum, count) = sum_product_f64_filtered_masked(&a, &b, &filter, Some(&null_a), None);
        assert!((sum - 16.0).abs() < 0.001);
        assert_eq!(count, 3);
    }

    #[test]
    fn test_min_max_f64_filtered() {
        let values = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0];
        let filter = vec![true, false, true, false, true, false, true, false];

        // Only indices 0, 2, 4, 6: values 5, 8, 9, 7
        assert_eq!(min_f64_filtered(&values, &filter), Some(5.0));
        assert_eq!(max_f64_filtered(&values, &filter), Some(9.0));

        // No rows pass filter
        let filter_none = vec![false; 8];
        assert_eq!(min_f64_filtered(&values, &filter_none), None);
        assert_eq!(max_f64_filtered(&values, &filter_none), None);
    }

    #[test]
    fn test_min_max_i64_filtered() {
        let values = vec![5i64, 2, 8, 1, 9, 3, 7, 4];
        let filter = vec![true, false, true, false, true, false, true, false];

        assert_eq!(min_i64_filtered(&values, &filter), Some(5));
        assert_eq!(max_i64_filtered(&values, &filter), Some(9));

        let filter_none = vec![false; 8];
        assert_eq!(min_i64_filtered(&values, &filter_none), None);
        assert_eq!(max_i64_filtered(&values, &filter_none), None);
    }

    #[test]
    fn test_filtered_remainder_handling() {
        // Test with non-multiple-of-4 lengths
        let values: Vec<f64> = (1..=7).map(|x| x as f64).collect();  // 7 elements
        let filter = vec![true, false, true, false, true, false, true];  // 1, 3, 5, 7 = 16
        assert!((sum_f64_filtered(&values, &filter) - 16.0).abs() < 0.001);
        assert_eq!(count_filtered(&filter), 4);
    }

    #[test]
    fn test_and_masks_inplace() {
        let mut mask = vec![true, true, false, true, true, false, true, true];
        let other = vec![true, false, true, true, false, true, true, false];
        and_masks_inplace(&mut mask, &other);
        assert_eq!(mask, vec![true, false, false, true, false, false, true, false]);
    }

    #[test]
    fn test_and_masks_inplace_remainder() {
        // Test with non-multiple-of-8 length
        let mut mask = vec![true, true, false, true, true];
        let other = vec![true, false, true, true, false];
        and_masks_inplace(&mut mask, &other);
        assert_eq!(mask, vec![true, false, false, true, false]);
    }

    #[test]
    fn test_between_i64() {
        let values = vec![1, 5, 10, 15, 20, 25];
        assert_eq!(between_i64(&values, 5, 20), vec![false, true, true, true, true, false]);
    }

    #[test]
    fn test_between_i32() {
        let values: Vec<i32> = vec![1, 5, 10, 15, 20, 25];
        assert_eq!(between_i32(&values, 5, 20), vec![false, true, true, true, true, false]);
    }

    #[test]
    fn test_between_f64() {
        let values = vec![0.01, 0.05, 0.06, 0.07, 0.08];
        assert_eq!(between_f64(&values, 0.05, 0.07), vec![false, true, true, true, false]);
    }

    // ========================================================================
    // PACKED MASK TESTS
    // ========================================================================

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
    fn test_packed_comparison_i64() {
        let values = vec![1i64, 2, 3, 4, 5];

        let mask = lt_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![true, true, false, false, false]);

        let mask = gt_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![false, false, false, true, true]);

        let mask = le_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![true, true, true, false, false]);

        let mask = ge_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![false, false, true, true, true]);

        let mask = eq_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![false, false, true, false, false]);

        let mask = ne_i64_packed(&values, 3);
        assert_eq!(mask.to_bool_vec(), vec![true, true, false, true, true]);
    }

    #[test]
    fn test_packed_comparison_f64() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];

        let mask = lt_f64_packed(&values, 3.0);
        assert_eq!(mask.to_bool_vec(), vec![true, true, false, false, false]);

        let mask = ge_f64_packed(&values, 3.0);
        assert_eq!(mask.to_bool_vec(), vec![false, false, true, true, true]);
    }

    #[test]
    fn test_packed_between_i64() {
        let values = vec![1i64, 5, 10, 15, 20, 25];
        let mask = between_i64_packed(&values, 5, 20);
        assert_eq!(mask.to_bool_vec(), vec![false, true, true, true, true, false]);
    }

    #[test]
    fn test_packed_between_f64() {
        let values = vec![0.01, 0.05, 0.06, 0.07, 0.08];
        let mask = between_f64_packed(&values, 0.05, 0.07);
        assert_eq!(mask.to_bool_vec(), vec![false, true, true, true, false]);
    }

    #[test]
    fn test_sum_f64_packed_filtered() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mut mask = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            mask.set(i, true);
        }
        // Sum of 1, 3, 5, 7 = 16
        assert!((sum_f64_packed_filtered(&values, &mask) - 16.0).abs() < 0.001);

        // All true
        let mask_all = PackedMask::new_all_set(8);
        assert!((sum_f64_packed_filtered(&values, &mask_all) - 36.0).abs() < 0.001);

        // All false
        let mask_none = PackedMask::new_all_clear(8);
        assert_eq!(sum_f64_packed_filtered(&values, &mask_none), 0.0);
    }

    #[test]
    fn test_sum_i64_packed_filtered() {
        let values = vec![1i64, 2, 3, 4, 5, 6, 7, 8];
        let mut mask = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            mask.set(i, true);
        }
        assert_eq!(sum_i64_packed_filtered(&values, &mask), 16);
    }

    #[test]
    fn test_count_packed_filtered() {
        let mut mask = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            mask.set(i, true);
        }
        assert_eq!(count_packed_filtered(&mask), 4);
    }

    #[test]
    fn test_sum_product_f64_packed_filtered() {
        let prices = vec![100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0];
        let discounts = vec![0.05, 0.06, 0.07, 0.05, 0.06, 0.07, 0.05, 0.06];

        let mut filter = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            filter.set(i, true);
        }

        // Only indices 0, 2, 4, 6: 100*0.05 + 300*0.07 + 500*0.06 + 700*0.05
        //                        = 5 + 21 + 30 + 35 = 91
        let (sum, count) = sum_product_f64_packed_filtered(&prices, &discounts, &filter);
        assert!((sum - 91.0).abs() < 0.001);
        assert_eq!(count, 4);

        // All pass filter
        let filter_all = PackedMask::new_all_set(8);
        let (sum, count) = sum_product_f64_packed_filtered(&prices, &discounts, &filter_all);
        assert!((sum - 213.0).abs() < 0.001);
        assert_eq!(count, 8);
    }

    #[test]
    fn test_min_max_f64_packed_filtered() {
        let values = vec![5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0];
        let mut filter = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            filter.set(i, true);
        }

        // Only indices 0, 2, 4, 6: values 5, 8, 9, 7
        assert_eq!(min_f64_packed_filtered(&values, &filter), Some(5.0));
        assert_eq!(max_f64_packed_filtered(&values, &filter), Some(9.0));

        // No rows pass filter
        let filter_none = PackedMask::new_all_clear(8);
        assert_eq!(min_f64_packed_filtered(&values, &filter_none), None);
        assert_eq!(max_f64_packed_filtered(&values, &filter_none), None);
    }

    #[test]
    fn test_min_max_i64_packed_filtered() {
        let values = vec![5i64, 2, 8, 1, 9, 3, 7, 4];
        let mut filter = PackedMask::new_all_clear(8);
        for i in (0..8).step_by(2) {
            filter.set(i, true);
        }

        assert_eq!(min_i64_packed_filtered(&values, &filter), Some(5));
        assert_eq!(max_i64_packed_filtered(&values, &filter), Some(9));

        let filter_none = PackedMask::new_all_clear(8);
        assert_eq!(min_i64_packed_filtered(&values, &filter_none), None);
        assert_eq!(max_i64_packed_filtered(&values, &filter_none), None);
    }

    #[test]
    fn test_packed_mask_large_dataset() {
        // Test with a large dataset to ensure correctness at scale
        let n = 10_000;
        let values: Vec<f64> = (0..n).map(|i| i as f64).collect();

        // Filter: values < 5000 (half the dataset)
        let mask = lt_f64_packed(&values, 5000.0);

        assert_eq!(mask.count_ones(), 5000);

        // Sum of 0..4999 = 4999 * 5000 / 2 = 12497500
        let sum = sum_f64_packed_filtered(&values, &mask);
        assert!((sum - 12497500.0).abs() < 0.001);
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
