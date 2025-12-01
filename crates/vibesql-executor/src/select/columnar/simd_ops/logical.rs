//! Logical operations for mask manipulation.
//!
//! This module provides vectorized boolean operations on filter masks.

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
