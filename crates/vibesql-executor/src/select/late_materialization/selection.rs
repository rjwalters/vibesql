//! Selection Vector Implementation
//!
//! A selection vector tracks which rows qualify from a source dataset,
//! enabling late materialization by deferring actual data access.

use std::ops::Range;

/// A selection vector tracks qualifying row indices
///
/// This is the core data structure for late materialization. Instead of
/// copying row data, we track which row indices qualify and only materialize
/// when needed.
///
/// # Memory Layout
///
/// Uses `u32` indices (4 bytes each) instead of full rows (potentially
/// hundreds of bytes each). For a 1M row table filtered to 10K rows:
/// - Old: 1M × sizeof(Row) = potentially gigabytes
/// - New: 10K × 4 bytes = 40KB
///
/// # Example
///
/// ```text
/// // Create from filter bitmap
/// let selection = SelectionVector::from_bitmap(&[true, false, true, true, false]);
/// assert_eq!(selection.len(), 3);
/// assert_eq!(selection.indices(), &[0, 2, 3]);
///
/// // Iterate over qualifying indices
/// for idx in selection.iter() {
///     let row = &source_rows[idx as usize];
///     // Process row...
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SelectionVector {
    /// Indices of qualifying rows (sorted, unique)
    indices: Vec<u32>,
}

impl SelectionVector {
    /// Create an empty selection vector
    #[inline]
    pub fn empty() -> Self {
        Self { indices: Vec::new() }
    }

    /// Create a selection vector that selects all rows in range [0, count)
    #[inline]
    pub fn all(count: usize) -> Self {
        Self { indices: (0..count as u32).collect() }
    }

    /// Create a selection vector from a range
    #[inline]
    pub fn from_range(range: Range<usize>) -> Self {
        Self { indices: (range.start as u32..range.end as u32).collect() }
    }

    /// Create from pre-computed indices (assumes sorted, unique)
    #[inline]
    pub fn from_indices(indices: Vec<u32>) -> Self {
        Self { indices }
    }

    /// Create from a boolean filter bitmap
    ///
    /// This is the most common creation path after evaluating predicates.
    ///
    /// # Example
    ///
    /// ```text
    /// let bitmap = vec![true, false, true, true, false];
    /// let selection = SelectionVector::from_bitmap(&bitmap);
    /// assert_eq!(selection.indices(), &[0, 2, 3]);
    /// ```
    pub fn from_bitmap(bitmap: &[bool]) -> Self {
        let mut indices = Vec::with_capacity(bitmap.len() / 4); // Estimate 25% selectivity

        for (i, &selected) in bitmap.iter().enumerate() {
            if selected {
                indices.push(i as u32);
            }
        }

        Self { indices }
    }

    /// Create from a bitmap with pre-allocated capacity hint
    ///
    /// Use when you have an estimate of how many rows will match.
    pub fn from_bitmap_with_capacity(bitmap: &[bool], capacity_hint: usize) -> Self {
        let mut indices = Vec::with_capacity(capacity_hint);

        for (i, &selected) in bitmap.iter().enumerate() {
            if selected {
                indices.push(i as u32);
            }
        }

        Self { indices }
    }

    /// Number of selected rows
    #[inline]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Check if no rows are selected
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Get the underlying indices slice
    #[inline]
    pub fn indices(&self) -> &[u32] {
        &self.indices
    }

    /// Consume and return the indices vector
    #[inline]
    pub fn into_indices(self) -> Vec<u32> {
        self.indices
    }

    /// Get index at position
    #[inline]
    pub fn get(&self, pos: usize) -> Option<u32> {
        self.indices.get(pos).copied()
    }

    /// Iterate over selected indices
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.indices.iter().copied()
    }

    /// Intersect with another selection vector (AND operation)
    ///
    /// Returns indices that appear in both selections. Useful for
    /// combining multiple filter conditions.
    pub fn intersect(&self, other: &SelectionVector) -> SelectionVector {
        // Both are sorted, so we can do a merge-style intersection
        let mut result = Vec::with_capacity(self.len().min(other.len()));
        let mut i = 0;
        let mut j = 0;

        while i < self.indices.len() && j < other.indices.len() {
            match self.indices[i].cmp(&other.indices[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    result.push(self.indices[i]);
                    i += 1;
                    j += 1;
                }
            }
        }

        SelectionVector { indices: result }
    }

    /// Union with another selection vector (OR operation)
    ///
    /// Returns indices that appear in either selection.
    pub fn union(&self, other: &SelectionVector) -> SelectionVector {
        // Both are sorted, so we can do a merge-style union
        let mut result = Vec::with_capacity(self.len() + other.len());
        let mut i = 0;
        let mut j = 0;

        while i < self.indices.len() && j < other.indices.len() {
            match self.indices[i].cmp(&other.indices[j]) {
                std::cmp::Ordering::Less => {
                    result.push(self.indices[i]);
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    result.push(other.indices[j]);
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    result.push(self.indices[i]);
                    i += 1;
                    j += 1;
                }
            }
        }

        // Append remaining
        result.extend_from_slice(&self.indices[i..]);
        result.extend_from_slice(&other.indices[j..]);

        SelectionVector { indices: result }
    }

    /// Remap indices through another selection
    ///
    /// If `self` contains indices [0, 2, 3] and `base` contains [10, 20, 30, 40, 50],
    /// returns [10, 30, 40] (indices 0, 2, 3 from base).
    ///
    /// This is used when chaining operations: if we filter a filtered result,
    /// we need to map back to original row indices.
    pub fn remap(&self, base: &SelectionVector) -> SelectionVector {
        let remapped: Vec<u32> =
            self.indices.iter().filter_map(|&idx| base.get(idx as usize)).collect();

        SelectionVector { indices: remapped }
    }

    /// Create a dense bitmap representation
    ///
    /// Returns a boolean vector where `result[i] = true` iff `i` is in the selection.
    /// `total_count` is the total number of rows in the source.
    pub fn to_bitmap(&self, total_count: usize) -> Vec<bool> {
        let mut bitmap = vec![false; total_count];
        for &idx in &self.indices {
            if (idx as usize) < total_count {
                bitmap[idx as usize] = true;
            }
        }
        bitmap
    }

    /// Filter this selection based on a predicate applied to a slice of data
    ///
    /// This is useful for applying additional filters without materializing rows.
    pub fn filter<T, F>(&self, data: &[T], predicate: F) -> SelectionVector
    where
        F: Fn(&T) -> bool,
    {
        let filtered: Vec<u32> =
            self.indices.iter().filter(|&&idx| predicate(&data[idx as usize])).copied().collect();

        SelectionVector { indices: filtered }
    }

    /// Apply a function to each selected index, collecting results
    pub fn map<T, F>(&self, mut f: F) -> Vec<T>
    where
        F: FnMut(u32) -> T,
    {
        self.indices.iter().map(|&idx| f(idx)).collect()
    }

    /// Selectivity ratio (selected / total)
    ///
    /// Returns a value between 0.0 and 1.0 indicating what fraction
    /// of the total rows are selected.
    #[inline]
    pub fn selectivity(&self, total_count: usize) -> f64 {
        if total_count == 0 {
            0.0
        } else {
            self.len() as f64 / total_count as f64
        }
    }

    /// Compact representation check
    ///
    /// Returns true if this selection represents a contiguous range,
    /// which allows for more efficient processing.
    pub fn is_contiguous(&self) -> bool {
        if self.indices.len() <= 1 {
            return true;
        }

        let first = self.indices[0];
        let last = self.indices[self.indices.len() - 1];
        (last - first + 1) as usize == self.indices.len()
    }

    /// Get the range if this selection is contiguous
    pub fn as_range(&self) -> Option<Range<usize>> {
        if self.is_contiguous() && !self.indices.is_empty() {
            Some(self.indices[0] as usize..self.indices[self.indices.len() - 1] as usize + 1)
        } else {
            None
        }
    }
}

impl Default for SelectionVector {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<Vec<u32>> for SelectionVector {
    fn from(indices: Vec<u32>) -> Self {
        Self::from_indices(indices)
    }
}

impl From<Vec<usize>> for SelectionVector {
    fn from(indices: Vec<usize>) -> Self {
        Self::from_indices(indices.into_iter().map(|i| i as u32).collect())
    }
}

impl IntoIterator for SelectionVector {
    type Item = u32;
    type IntoIter = std::vec::IntoIter<u32>;

    fn into_iter(self) -> Self::IntoIter {
        self.indices.into_iter()
    }
}

impl<'a> IntoIterator for &'a SelectionVector {
    type Item = &'a u32;
    type IntoIter = std::slice::Iter<'a, u32>;

    fn into_iter(self) -> Self::IntoIter {
        self.indices.iter()
    }
}

#[cfg(test)]
mod selection_tests {
    use super::*;

    #[test]
    fn test_from_bitmap() {
        let bitmap = vec![true, false, true, true, false, true];
        let selection = SelectionVector::from_bitmap(&bitmap);

        assert_eq!(selection.len(), 4);
        assert_eq!(selection.indices(), &[0, 2, 3, 5]);
    }

    #[test]
    fn test_empty() {
        let selection = SelectionVector::empty();
        assert!(selection.is_empty());
        assert_eq!(selection.len(), 0);
    }

    #[test]
    fn test_all() {
        let selection = SelectionVector::all(5);
        assert_eq!(selection.len(), 5);
        assert_eq!(selection.indices(), &[0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_intersect() {
        let a = SelectionVector::from_indices(vec![1, 2, 4, 6, 8]);
        let b = SelectionVector::from_indices(vec![2, 3, 4, 5, 6]);

        let result = a.intersect(&b);
        assert_eq!(result.indices(), &[2, 4, 6]);
    }

    #[test]
    fn test_union() {
        let a = SelectionVector::from_indices(vec![1, 3, 5]);
        let b = SelectionVector::from_indices(vec![2, 3, 4]);

        let result = a.union(&b);
        assert_eq!(result.indices(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_remap() {
        let base = SelectionVector::from_indices(vec![10, 20, 30, 40, 50]);
        let child = SelectionVector::from_indices(vec![0, 2, 4]); // Select indices 0, 2, 4 from base

        let remapped = child.remap(&base);
        assert_eq!(remapped.indices(), &[10, 30, 50]);
    }

    #[test]
    fn test_to_bitmap() {
        let selection = SelectionVector::from_indices(vec![1, 3, 4]);
        let bitmap = selection.to_bitmap(6);

        assert_eq!(bitmap, vec![false, true, false, true, true, false]);
    }

    #[test]
    fn test_is_contiguous() {
        let contiguous = SelectionVector::from_indices(vec![2, 3, 4, 5]);
        let non_contiguous = SelectionVector::from_indices(vec![2, 4, 6]);

        assert!(contiguous.is_contiguous());
        assert!(!non_contiguous.is_contiguous());
    }

    #[test]
    fn test_as_range() {
        let contiguous = SelectionVector::from_indices(vec![2, 3, 4, 5]);
        assert_eq!(contiguous.as_range(), Some(2..6));

        let non_contiguous = SelectionVector::from_indices(vec![2, 4, 6]);
        assert_eq!(non_contiguous.as_range(), None);
    }

    #[test]
    fn test_selectivity() {
        let selection = SelectionVector::from_indices(vec![0, 5, 10]);
        assert!((selection.selectivity(100) - 0.03).abs() < 0.001);
        assert!((selection.selectivity(30) - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_filter() {
        let data = vec![10, 20, 30, 40, 50];
        let selection = SelectionVector::from_indices(vec![0, 2, 4]);

        let filtered = selection.filter(&data, |&x| x > 20);
        assert_eq!(filtered.indices(), &[2, 4]); // indices where data > 20
    }
}
