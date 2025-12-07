use smallvec::SmallVec;
use vibesql_types::{Date, SqlValue};

/// Inline capacity for Row values.
/// Rows with up to this many columns avoid heap allocation.
///
/// Benchmarked with capacity values 4, 6, 8, 10, 12 (issue #3964).
/// Set to 8 based on the following findings:
///
/// | Capacity | Struct Size | Optimal For     |
/// |----------|-------------|-----------------|
/// | 4        | 176 bytes   | 2-4 columns     |
/// | 6        | 256 bytes   | 4-6 columns     |
/// | 8        | 336 bytes   | 6-10 columns    |
/// | 10       | 416 bytes   | 8-10 columns    |
/// | 12       | 496 bytes   | 10-12 columns   |
///
/// Key benchmarking results for 8-column rows:
/// - Capacity 4: 162.8 ns (spills to heap)
/// - Capacity 6: 159.8 ns (spills to heap)
/// - Capacity 8: 142.1 ns (inline)
/// - Capacity 10: 141.0 ns (inline)
///
/// Capacity 8 was chosen because:
/// 1. Covers most TPC-H aggregation results without heap allocation
/// 2. Best performance at the 8-column mark (common for analytical queries)
/// 3. Competitive even when spilling for wider queries
/// 4. Reasonable 336-byte memory overhead per row
///
/// For memory-constrained or OLTP-heavy workloads with narrow queries,
/// capacity 6 could provide ~8% memory savings (256 vs 336 bytes).
pub const ROW_INLINE_CAPACITY: usize = 8;

/// Type alias for the SmallVec used in Row.
pub type RowValues = SmallVec<[SqlValue; ROW_INLINE_CAPACITY]>;

/// A single row of data - vector of SqlValues
///
/// Uses SmallVec to avoid heap allocations for rows with up to
/// [`ROW_INLINE_CAPACITY`] columns. This optimization significantly
/// reduces allocation overhead for common query patterns.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub values: RowValues,
}

impl Row {
    /// Create a new row from values.
    ///
    /// Accepts any iterable that can be converted into a SmallVec.
    pub fn new(values: impl Into<RowValues>) -> Self {
        Row { values: values.into() }
    }

    /// Create a new row from a Vec of values.
    ///
    /// This is a convenience method that accepts Vec<SqlValue> directly.
    pub fn from_vec(values: Vec<SqlValue>) -> Self {
        Row { values: SmallVec::from_vec(values) }
    }

    /// Get value at column index
    pub fn get(&self, index: usize) -> Option<&SqlValue> {
        self.values.get(index)
    }

    /// Get number of columns in this row
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if row is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Estimate the memory size of this row in bytes
    ///
    /// Used for memory limit tracking during query execution.
    /// Provides a reasonable approximation without deep inspection.
    pub fn estimated_size_bytes(&self) -> usize {
        use std::mem::size_of;

        // Base overhead: Row struct (includes SmallVec inline storage)
        let base_overhead = size_of::<Row>();

        // If spilled to heap, add the heap allocation size
        let heap_overhead = if self.values.spilled() {
            self.values.capacity() * size_of::<SqlValue>()
        } else {
            0
        };

        // Estimate size of each value's heap allocations (e.g., strings)
        let values_heap_size: usize = self.values.iter().map(|v| v.estimated_size_bytes()).sum();

        base_overhead + heap_overhead + values_heap_size
    }

    /// Set value at column index
    pub fn set(&mut self, index: usize, value: SqlValue) -> Result<(), crate::StorageError> {
        if index >= self.values.len() {
            return Err(crate::StorageError::ColumnIndexOutOfBounds { index });
        }
        self.values[index] = value;
        Ok(())
    }

    /// Add a value to the end of the row
    pub fn add_value(&mut self, value: SqlValue) {
        self.values.push(value);
    }

    /// Remove a value at the specified index
    pub fn remove_value(&mut self, index: usize) -> Result<SqlValue, crate::StorageError> {
        if index >= self.values.len() {
            return Err(crate::StorageError::ColumnIndexOutOfBounds { index });
        }
        Ok(self.values.remove(index))
    }

    // ========================================================================
    // Type-specialized unchecked accessors for monomorphic execution paths
    //
    // SAFETY: These methods bypass enum tag checks for performance.
    // Caller MUST guarantee the column type matches the accessor type.
    // Debug builds include assertions to catch type mismatches.
    //
    // Safety validation:
    // - Debug assertions catch type mismatches in development
    // - Comprehensive test suite validates correct usage (7/7 tests passing)
    // - MIRI validates no undefined behavior (CI: .github/workflows/miri.yml)
    //   * Use-after-free detection
    //   * Out-of-bounds access detection
    //   * Data race detection
    //   * Invalid enum discriminant detection
    //   * Unaligned read detection
    // ========================================================================

    /// Get f64 value without enum matching
    ///
    /// # Safety
    ///
    /// Caller must ensure the value at `idx` is a Double or Float variant.
    /// Violating this will cause undefined behavior in release builds.
    /// Debug builds will panic with assertion failure.
    #[inline(always)]
    pub unsafe fn get_f64_unchecked(&self, idx: usize) -> f64 {
        debug_assert!(
            matches!(self.values[idx], SqlValue::Double(_) | SqlValue::Float(_)),
            "get_f64_unchecked called on non-float value: {:?}",
            self.values[idx]
        );

        match &self.values[idx] {
            SqlValue::Double(d) => *d,
            SqlValue::Float(f) => *f as f64,
            _ => std::hint::unreachable_unchecked(),
        }
    }

    /// Get i64 value without enum matching
    ///
    /// # Safety
    ///
    /// Caller must ensure the value at `idx` is an Integer, Bigint, or Smallint variant.
    /// Violating this will cause undefined behavior in release builds.
    /// Debug builds will panic with assertion failure.
    #[inline(always)]
    pub unsafe fn get_i64_unchecked(&self, idx: usize) -> i64 {
        debug_assert!(
            matches!(
                self.values[idx],
                SqlValue::Integer(_) | SqlValue::Bigint(_) | SqlValue::Smallint(_)
            ),
            "get_i64_unchecked called on non-integer value: {:?}",
            self.values[idx]
        );

        match &self.values[idx] {
            SqlValue::Integer(i) | SqlValue::Bigint(i) => *i,
            SqlValue::Smallint(s) => *s as i64,
            _ => std::hint::unreachable_unchecked(),
        }
    }

    /// Get numeric value as f64 without enum matching
    ///
    /// # Safety
    ///
    /// Caller must ensure the value at `idx` is a numeric variant (Integer, Bigint, Smallint, Unsigned, Numeric, Double, Float, or Real).
    /// Violating this will cause undefined behavior in release builds.
    /// Debug builds will panic with assertion failure.
    #[inline(always)]
    pub unsafe fn get_numeric_as_f64_unchecked(&self, idx: usize) -> f64 {
        debug_assert!(
            matches!(
                self.values[idx],
                SqlValue::Integer(_)
                    | SqlValue::Bigint(_)
                    | SqlValue::Smallint(_)
                    | SqlValue::Unsigned(_)
                    | SqlValue::Numeric(_)
                    | SqlValue::Double(_)
                    | SqlValue::Float(_)
                    | SqlValue::Real(_)
            ),
            "get_numeric_as_f64_unchecked called on non-numeric value: {:?}",
            self.values[idx]
        );

        match &self.values[idx] {
            SqlValue::Integer(i) | SqlValue::Bigint(i) => *i as f64,
            SqlValue::Smallint(s) => *s as f64,
            SqlValue::Unsigned(u) => *u as f64,
            SqlValue::Numeric(n) | SqlValue::Double(n) => *n,
            SqlValue::Float(f) | SqlValue::Real(f) => *f as f64,
            _ => std::hint::unreachable_unchecked(),
        }
    }

    /// Get Date value without enum matching
    ///
    /// # Safety
    ///
    /// Caller must ensure the value at `idx` is a Date variant.
    /// Violating this will cause undefined behavior in release builds.
    /// Debug builds will panic with assertion failure.
    #[inline(always)]
    pub unsafe fn get_date_unchecked(&self, idx: usize) -> Date {
        debug_assert!(
            matches!(self.values[idx], SqlValue::Date(_)),
            "get_date_unchecked called on non-date value: {:?}",
            self.values[idx]
        );

        match &self.values[idx] {
            SqlValue::Date(d) => *d,
            _ => std::hint::unreachable_unchecked(),
        }
    }

    /// Get bool value without enum matching
    ///
    /// # Safety
    ///
    /// Caller must ensure the value at `idx` is a Boolean variant.
    /// Violating this will cause undefined behavior in release builds.
    /// Debug builds will panic with assertion failure.
    #[inline(always)]
    pub unsafe fn get_bool_unchecked(&self, idx: usize) -> bool {
        debug_assert!(
            matches!(self.values[idx], SqlValue::Boolean(_)),
            "get_bool_unchecked called on non-boolean value: {:?}",
            self.values[idx]
        );

        match &self.values[idx] {
            SqlValue::Boolean(b) => *b,
            _ => std::hint::unreachable_unchecked(),
        }
    }

    /// Get string value without enum matching
    ///
    /// # Safety
    ///
    /// Caller must ensure the value at `idx` is a Varchar or Character variant.
    /// Violating this will cause undefined behavior in release builds.
    /// Debug builds will panic with assertion failure.
    #[inline(always)]
    pub unsafe fn get_string_unchecked(&self, idx: usize) -> &str {
        debug_assert!(
            matches!(self.values[idx], SqlValue::Varchar(_) | SqlValue::Character(_)),
            "get_string_unchecked called on non-string value: {:?}",
            self.values[idx]
        );

        match &self.values[idx] {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s,
            _ => std::hint::unreachable_unchecked(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_unchecked_accessors_correct_types() {
        let row = Row::from_vec(vec![
            SqlValue::Double(3.14),
            SqlValue::Integer(42),
            SqlValue::Date(Date::from_str("2024-01-01").unwrap()),
            SqlValue::Boolean(true),
            SqlValue::Varchar(arcstr::ArcStr::from("hello")),
        ]);

        unsafe {
            assert_eq!(row.get_f64_unchecked(0), 3.14);
            assert_eq!(row.get_i64_unchecked(1), 42);
            assert_eq!(row.get_date_unchecked(2), Date::from_str("2024-01-01").unwrap());
            assert!(row.get_bool_unchecked(3));
            assert_eq!(row.get_string_unchecked(4), "hello");
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "get_f64_unchecked called on non-float value")]
    fn test_get_f64_unchecked_wrong_type() {
        let row = Row::from_vec(vec![SqlValue::Integer(42)]);
        unsafe {
            row.get_f64_unchecked(0); // Should panic in debug mode
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "get_i64_unchecked called on non-integer value")]
    fn test_get_i64_unchecked_wrong_type() {
        let row = Row::from_vec(vec![SqlValue::Double(3.14)]);
        unsafe {
            row.get_i64_unchecked(0); // Should panic in debug mode
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "get_date_unchecked called on non-date value")]
    fn test_get_date_unchecked_wrong_type() {
        let row = Row::from_vec(vec![SqlValue::Integer(42)]);
        unsafe {
            row.get_date_unchecked(0); // Should panic in debug mode
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "get_bool_unchecked called on non-boolean value")]
    fn test_get_bool_unchecked_wrong_type() {
        let row = Row::from_vec(vec![SqlValue::Integer(42)]);
        unsafe {
            row.get_bool_unchecked(0); // Should panic in debug mode
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "get_string_unchecked called on non-string value")]
    fn test_get_string_unchecked_wrong_type() {
        let row = Row::from_vec(vec![SqlValue::Integer(42)]);
        unsafe {
            row.get_string_unchecked(0); // Should panic in debug mode
        }
    }

    #[test]
    fn test_unchecked_accessor_with_type_coercion() {
        // Test that Float is coerced to f64
        let row = Row::from_vec(vec![SqlValue::Float(3.14)]);
        unsafe {
            assert_eq!(row.get_f64_unchecked(0), 3.14f32 as f64);
        }

        // Test that Smallint is coerced to i64
        let row = Row::from_vec(vec![SqlValue::Smallint(42)]);
        unsafe {
            assert_eq!(row.get_i64_unchecked(0), 42i64);
        }

        // Test that Bigint works
        let row = Row::from_vec(vec![SqlValue::Bigint(1000000)]);
        unsafe {
            assert_eq!(row.get_i64_unchecked(0), 1000000i64);
        }

        // Test that Character string works
        let row = Row::from_vec(vec![SqlValue::Character(arcstr::ArcStr::from("test"))]);
        unsafe {
            assert_eq!(row.get_string_unchecked(0), "test");
        }
    }
}
