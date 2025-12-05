//! Selection bitmap utilities for vectorized filtering
//!
//! Provides efficient bitmap-based row selection for batch predicate evaluation.
//! Bitmaps allow SIMD-friendly operations and better branch prediction.

/// A selection bitmap for marking which rows pass a predicate
///
/// Uses Vec<bool> for simplicity. Future optimization: use bit-packed representation
/// for better cache utilization (8x compression).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SelectionBitmap {
    bits: Vec<bool>,
}

#[allow(dead_code)]
impl SelectionBitmap {
    /// Create a new bitmap with all bits set to `value`
    #[inline]
    pub fn new(size: usize, value: bool) -> Self {
        Self { bits: vec![value; size] }
    }

    /// Create a bitmap with all bits set to true (all rows selected)
    #[inline]
    pub fn all_true(size: usize) -> Self {
        Self::new(size, true)
    }

    /// Create a bitmap with all bits set to false (no rows selected)
    #[inline]
    #[allow(dead_code)]
    pub fn all_false(size: usize) -> Self {
        Self::new(size, false)
    }

    /// Get the value of a bit at the given index
    #[inline]
    #[allow(dead_code)]
    pub fn get(&self, index: usize) -> bool {
        self.bits[index]
    }

    /// Set the value of a bit at the given index
    #[inline]
    pub fn set(&mut self, index: usize, value: bool) {
        self.bits[index] = value;
    }

    /// Get the number of bits in the bitmap
    #[inline]
    pub fn len(&self) -> usize {
        self.bits.len()
    }

    /// Check if the bitmap is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    /// Count the number of true bits (selected rows)
    #[inline]
    pub fn count_true(&self) -> usize {
        self.bits.iter().filter(|&&b| b).count()
    }

    /// Get an iterator over the bits
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &bool> {
        self.bits.iter()
    }

    /// Apply this bitmap to filter a slice of rows
    ///
    /// Returns a new Vec containing only rows where the corresponding bit is true.
    /// This is the core filtering operation after predicate evaluation.
    pub fn filter_rows<T>(&self, rows: &[T]) -> Vec<T>
    where
        T: Clone,
    {
        debug_assert_eq!(self.len(), rows.len(), "Bitmap length must match row count");

        rows.iter()
            .zip(self.bits.iter())
            .filter_map(|(row, &keep)| if keep { Some(row.clone()) } else { None })
            .collect()
    }

    /// Apply this bitmap to filter rows into a pre-allocated buffer
    ///
    /// More efficient than filter_rows when you have a reusable buffer.
    /// The buffer will be cleared and filled with filtered rows.
    pub fn filter_rows_into<T>(&self, rows: &[T], buffer: &mut Vec<T>)
    where
        T: Clone,
    {
        debug_assert_eq!(self.len(), rows.len(), "Bitmap length must match row count");

        buffer.clear();
        buffer.reserve(self.count_true());

        for (row, &keep) in rows.iter().zip(self.bits.iter()) {
            if keep {
                buffer.push(row.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_creation() {
        let bitmap = SelectionBitmap::all_true(5);
        assert_eq!(bitmap.len(), 5);
        assert_eq!(bitmap.count_true(), 5);

        let bitmap = SelectionBitmap::all_false(3);
        assert_eq!(bitmap.len(), 3);
        assert_eq!(bitmap.count_true(), 0);
    }

    #[test]
    fn test_bitmap_set_get() {
        let mut bitmap = SelectionBitmap::all_true(4);
        assert!(bitmap.get(2));

        bitmap.set(2, false);
        assert!(!bitmap.get(2));
        assert_eq!(bitmap.count_true(), 3);
    }

    #[test]
    fn test_filter_rows() {
        let rows = vec![1, 2, 3, 4, 5];
        let mut bitmap = SelectionBitmap::all_false(5);
        bitmap.set(1, true); // Keep row 2
        bitmap.set(3, true); // Keep row 4

        let filtered = bitmap.filter_rows(&rows);
        assert_eq!(filtered, vec![2, 4]);
    }

    #[test]
    fn test_filter_rows_into() {
        let rows = vec![10, 20, 30, 40];
        let mut bitmap = SelectionBitmap::all_true(4);
        bitmap.set(1, false); // Drop row 20
        bitmap.set(2, false); // Drop row 30

        let mut buffer = Vec::new();
        bitmap.filter_rows_into(&rows, &mut buffer);
        assert_eq!(buffer, vec![10, 40]);
    }
}
