use std::collections::HashMap;

use smallvec::SmallVec;
use vibesql_types::{Date, SqlValue};

/// Transaction identifier type for MVCC version fields.
///
/// `TxnId` is currently a monotonically increasing `u64`, allocated by the
/// transaction manager at `BEGIN` time. The value `0` (`PRE_MVCC_TXN_ID`) is a
/// reserved sentinel meaning "this row was written before the MVCC machinery
/// existed and should be treated as always-committed." This is what we stamp on
/// rows recovered from v6 `.vbsql` files (see Phase 1a of #5136 — the
/// persistence format upgrade from v6 to v7 added `xmin`/`xmax` to each row).
///
/// In future phases (1b/1c/1d) this alias may become a newtype if we need
/// stronger type discipline; keeping it a transparent alias for now to avoid
/// touching the ~1,800 `Row::new`/`Row {}` construction sites in this Phase 1a
/// PR.
pub type TxnId = u64;

/// Sentinel `TxnId` for rows that pre-date MVCC (v6 `.vbsql` files, or rows
/// written when the `mvcc_enabled` feature flag is off). Rows stamped with
/// `xmin = PRE_MVCC_TXN_ID` are considered visible to every transaction —
/// equivalent to "committed before any active txn started."
pub const PRE_MVCC_TXN_ID: TxnId = 0;

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
///
/// # MVCC Version Fields (Phase 1a of #5136)
///
/// Every row carries two version fields used by Multi-Version Concurrency
/// Control:
///
/// - [`xmin`](Self::xmin): the transaction that created this row version.
/// - [`xmax`](Self::xmax): the transaction that deleted this row version, or `None` if the row is
///   still live.
///
/// Phase 1a only adds these fields and threads them through serialization;
/// no executor code reads or writes them yet, so all constructors default
/// `xmin = PRE_MVCC_TXN_ID` (= 0) and `xmax = None`. This keeps the existing
/// ~1,800 `Row::new` / `Row { ... }` call sites compiling unchanged.
/// Phase 1b will introduce `Row::visible_to(&TxnSnapshot)`; Phase 1c will
/// stamp non-zero `xmin`/`xmax` from the write-path executors.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub values: RowValues,
    /// Optional row ID for SQLite ROWID compatibility
    /// This is set during table scans and used to support ROWID, _rowid_, and oid pseudo-columns
    pub row_id: Option<u64>,
    /// Row IDs per table for JOIN support (issue #4370)
    /// Maps table name (lowercase) to row ID for qualified ROWID references like `t1.rowid`
    /// Only populated for joined rows; single-table rows use `row_id` field instead.
    pub row_ids: Option<HashMap<String, u64>>,
    /// MVCC: transaction id that created this row version.
    ///
    /// Defaults to [`PRE_MVCC_TXN_ID`] (= 0) for rows constructed via the
    /// public constructors and for rows recovered from pre-v7 (`v6`) `.vbsql`
    /// files. The sentinel means "always committed, visible to every snapshot."
    /// Phase 1c will stamp the active transaction id here on INSERT/UPDATE.
    pub xmin: TxnId,
    /// MVCC: transaction id that deleted/superseded this row version.
    ///
    /// `None` means the row is still live. A row with `xmax = Some(t)` is
    /// considered deleted by transaction `t` and only visible to snapshots
    /// that don't yet see `t` as committed. Phase 1c will stamp this on
    /// UPDATE (old version) and DELETE.
    pub xmax: Option<TxnId>,
}

impl Row {
    /// Create a new row from values.
    ///
    /// Accepts any iterable that can be converted into a SmallVec.
    ///
    /// MVCC version fields default to the pre-MVCC sentinels
    /// (`xmin = PRE_MVCC_TXN_ID`, `xmax = None`) — see the [`Row`] doc comment.
    pub fn new(values: impl Into<RowValues>) -> Self {
        Row {
            values: values.into(),
            row_id: None,
            row_ids: None,
            xmin: PRE_MVCC_TXN_ID,
            xmax: None,
        }
    }

    /// Create a new row from a Vec of values.
    ///
    /// This is a convenience method that accepts Vec<SqlValue> directly.
    ///
    /// MVCC version fields default to the pre-MVCC sentinels — see [`Row::new`].
    pub fn from_vec(values: Vec<SqlValue>) -> Self {
        Row {
            values: SmallVec::from_vec(values),
            row_id: None,
            row_ids: None,
            xmin: PRE_MVCC_TXN_ID,
            xmax: None,
        }
    }

    /// Create a new row with a specific row ID
    ///
    /// Used during table scans to preserve ROWID for SQLite compatibility.
    pub fn with_row_id(values: impl Into<RowValues>, row_id: u64) -> Self {
        Row {
            values: values.into(),
            row_id: Some(row_id),
            row_ids: None,
            xmin: PRE_MVCC_TXN_ID,
            xmax: None,
        }
    }

    /// Create a new row with per-table row IDs (for JOIN results)
    ///
    /// Used when combining rows from multiple tables in a JOIN.
    pub fn with_row_ids(values: impl Into<RowValues>, row_ids: HashMap<String, u64>) -> Self {
        Row {
            values: values.into(),
            row_id: None,
            row_ids: if row_ids.is_empty() { None } else { Some(row_ids) },
            xmin: PRE_MVCC_TXN_ID,
            xmax: None,
        }
    }

    /// Get row ID for a specific table (case-insensitive lookup)
    ///
    /// For single-table rows, returns `row_id` if `table_name` is None or matches.
    /// For joined rows, looks up in the `row_ids` map by table name.
    pub fn get_row_id_for_table(&self, table_name: Option<&str>) -> Option<u64> {
        // Try table-specific row_ids first (for joined rows)
        if let Some(ref row_ids) = self.row_ids {
            if let Some(name) = table_name {
                let name_lower = name.to_lowercase();
                return row_ids.get(&name_lower).copied();
            }
            // No table specified, return the first row_id if there's exactly one
            if row_ids.len() == 1 {
                return row_ids.values().next().copied();
            }
            // Multiple tables and no qualifier - return None (ambiguous)
            return None;
        }

        // Fall back to single row_id (for non-joined rows)
        self.row_id
    }

    /// Set the row ID
    pub fn set_row_id(&mut self, row_id: u64) {
        self.row_id = Some(row_id);
    }

    /// Combine two rows for JOIN operations, preserving ROWIDs for each table
    ///
    /// This method is used by all JOIN implementations to properly track ROWIDs
    /// from both left and right tables, enabling qualified ROWID references like
    /// `t1.rowid` and `t2.rowid` in the result set.
    ///
    /// # Arguments
    /// * `left` - The left row in the join
    /// * `right` - The right row in the join
    /// * `left_table_names` - Table names for the left side (for ROWID mapping)
    /// * `right_table_names` - Table names for the right side (for ROWID mapping)
    pub fn combine_for_join(
        left: &Row,
        right: &Row,
        left_table_names: &[String],
        right_table_names: &[String],
    ) -> Row {
        let mut combined_values = Vec::with_capacity(left.values.len() + right.values.len());
        combined_values.extend_from_slice(&left.values);
        combined_values.extend_from_slice(&right.values);

        // Merge ROWIDs from both rows
        let mut combined_row_ids = HashMap::new();

        // Add left row's ROWIDs
        if let Some(ref row_ids) = left.row_ids {
            combined_row_ids.extend(row_ids.iter().map(|(k, v)| (k.clone(), *v)));
        } else if let Some(row_id) = left.row_id {
            for name in left_table_names {
                combined_row_ids.insert(name.to_lowercase(), row_id);
            }
        }

        // Add right row's ROWIDs
        if let Some(ref row_ids) = right.row_ids {
            combined_row_ids.extend(row_ids.iter().map(|(k, v)| (k.clone(), *v)));
        } else if let Some(row_id) = right.row_id {
            for name in right_table_names {
                combined_row_ids.insert(name.to_lowercase(), row_id);
            }
        }

        if combined_row_ids.is_empty() {
            Row::new(combined_values)
        } else {
            Row::with_row_ids(combined_values, combined_row_ids)
        }
    }

    /// Get the row ID if set
    pub fn get_row_id(&self) -> Option<u64> {
        self.row_id
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
        let heap_overhead =
            if self.values.spilled() { self.values.capacity() * size_of::<SqlValue>() } else { 0 };

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
    /// Caller must ensure the value at `idx` is a numeric variant (Integer, Bigint, Smallint,
    /// Unsigned, Numeric, Double, Float, or Real). Violating this will cause undefined behavior
    /// in release builds. Debug builds will panic with assertion failure.
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
            SqlValue::Numeric(n) | SqlValue::Double(n) | SqlValue::Real(n) => *n, /* Real is now */
            // f64
            SqlValue::Float(f) => *f as f64,
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

    // ========================================================================
    // MVCC Visibility (Phase 1b of #5136)
    //
    // These methods interpret the `xmin`/`xmax` fields added in Phase 1a
    // against a captured [`TxnSnapshot`]. Currently called only from unit
    // tests — Phase 1d will wire `visible_to` into the scan boundary in
    // `Table::scan*`. See `crate::mvcc` for the full design notes.
    // ========================================================================

    /// Returns `true` if this row version is visible to a reader holding
    /// `snapshot` under snapshot isolation.
    ///
    /// See [`crate::mvcc`] for the full predicate contract. In summary,
    /// the row is visible iff all three hold:
    ///
    /// 1. The creator (`xmin`) was committed-as-of the snapshot: `xmin <= snapshot.xmax_committed`
    ///    AND `xmin` not in `snapshot.in_progress`. The pre-MVCC sentinel [`PRE_MVCC_TXN_ID`] (= 0)
    ///    is always treated as committed.
    /// 2. (Implied by clause 1) If the creator is the pre-MVCC sentinel, clause 1 is satisfied
    ///    trivially.
    /// 3. The deleter (`xmax`), if any, is **not** committed-as-of the snapshot. Concretely, the
    ///    row is visible if `xmax.is_none()`, or `xmax > snapshot.xmin_active` (delete happened by
    ///    a transaction that started after our snapshot's oldest concurrent peer), or `xmax` is in
    ///    `snapshot.in_progress` (deleter was still running at snapshot time).
    ///
    /// # Phase 1b note
    ///
    /// This is a pure-function predicate — it doesn't read any global
    /// state. Phase 1c starts producing rows with non-sentinel
    /// `xmin`/`xmax`; Phase 1d starts calling this from the scan path.
    /// Until then, every row created via [`Row::new`] / [`Row::from_vec`]
    /// has `xmin = PRE_MVCC_TXN_ID, xmax = None`, so this method always
    /// returns `true` for them.
    #[inline]
    pub fn visible_to(&self, snapshot: &crate::mvcc::TxnSnapshot) -> bool {
        // Clause 1: creator must be committed-as-of the snapshot.
        // The pre-MVCC sentinel is always committed (see TxnSnapshot::is_committed).
        if !snapshot.is_committed(self.xmin) {
            return false;
        }

        // Clause 3: if there's a deleter, it must NOT be committed-as-of
        // the snapshot for us to still see the row.
        match self.xmax {
            None => true,
            Some(deleter) => {
                // Row is still visible if:
                //   - the deleter started after our snapshot's xmin_active (so the delete can't
                //     have been committed before us), OR
                //   - the deleter was in_progress at snapshot time, OR
                //   - the deleter is the pre-MVCC sentinel — which would be bizarre (xmax = 0 means
                //     "deleted by no one") but is handled defensively by treating
                //     sentinel-as-committed, making the row invisible. This matches "PRE_MVCC means
                //     definitely committed" and avoids accidental visibility.
                if deleter > snapshot.xmin_active {
                    return true;
                }
                if snapshot.is_in_progress(deleter) {
                    return true;
                }
                // Deleter is committed-as-of snapshot → row is invisible.
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

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

    // ========================================================================
    // MVCC visibility predicate tests (Phase 1b of #5136)
    // ========================================================================

    mod visibility {
        use std::collections::HashSet;

        use super::super::*;
        use crate::mvcc::TxnSnapshot;

        /// Helper: build a row with explicit MVCC fields.
        fn row_with(xmin: TxnId, xmax: Option<TxnId>) -> Row {
            let mut r = Row::new(vec![SqlValue::Integer(0)]);
            r.xmin = xmin;
            r.xmax = xmax;
            r
        }

        /// Helper: snapshot with `in_progress` set built from a slice.
        fn snap(xmin_active: TxnId, xmax_committed: TxnId, in_progress: &[TxnId]) -> TxnSnapshot {
            TxnSnapshot::new(xmin_active, xmax_committed, in_progress.iter().copied().collect())
        }

        #[test]
        fn pre_mvcc_row_visible_under_empty_snapshot() {
            // Legacy rows (xmin = 0, xmax = None) — the constructor default —
            // are visible to every snapshot, including the empty one used
            // by pre-MVCC code paths and auto-commit reads.
            let r = Row::new(vec![SqlValue::Integer(42)]);
            assert!(r.visible_to(&TxnSnapshot::empty()));
        }

        #[test]
        fn pre_mvcc_row_visible_under_active_snapshot() {
            let r = row_with(PRE_MVCC_TXN_ID, None);
            let s = snap(5, 10, &[5, 7]);
            assert!(r.visible_to(&s));
        }

        #[test]
        fn row_visible_when_creator_committed_pre_snapshot() {
            // Writer txn 3 committed before snapshot (xmax_committed = 10),
            // not in in_progress → visible.
            let r = row_with(3, None);
            let s = snap(5, 10, &[5, 7]);
            assert!(r.visible_to(&s));
        }

        #[test]
        fn row_invisible_when_creator_in_progress() {
            // Writer txn 7 was still running at snapshot time → invisible
            // (this is the snapshot-isolation rule: concurrent writers don't
            // affect each other's reads).
            let r = row_with(7, None);
            let s = snap(5, 10, &[5, 7]);
            assert!(!r.visible_to(&s));
        }

        #[test]
        fn row_invisible_when_creator_after_snapshot_high_watermark() {
            // Writer txn 20 > xmax_committed = 10 → creator hadn't committed
            // when we took the snapshot → invisible.
            let r = row_with(20, None);
            let s = snap(5, 10, &[5, 7]);
            assert!(!r.visible_to(&s));
        }

        #[test]
        fn row_visible_when_creator_equals_high_watermark() {
            // Edge case: xmin == xmax_committed should be visible (committed
            // exactly at the boundary).
            let r = row_with(10, None);
            let s = snap(5, 10, &[5, 7]);
            assert!(r.visible_to(&s));
        }

        #[test]
        fn row_invisible_when_deleter_committed_pre_snapshot() {
            // Creator committed (xmin = 3 <= 10), deleter committed (xmax =
            // Some(4) <= xmin_active = 5, not in_progress) → row is gone as
            // far as this snapshot is concerned.
            let r = row_with(3, Some(4));
            let s = snap(5, 10, &[5, 7]);
            assert!(!r.visible_to(&s));
        }

        #[test]
        fn row_visible_when_deleter_after_snapshot_oldest_active() {
            // Deleter xmax = 9 > xmin_active = 5 → delete happened after
            // our snapshot's oldest concurrent peer started, so we don't
            // see the delete yet.
            let r = row_with(3, Some(9));
            let s = snap(5, 10, &[5, 7]);
            assert!(r.visible_to(&s));
        }

        #[test]
        fn row_visible_when_deleter_still_in_progress() {
            // Deleter xmax = 5 is in in_progress → the delete is still
            // mid-flight at snapshot time → we still see the row.
            let r = row_with(3, Some(5));
            let s = snap(5, 10, &[5, 7]);
            assert!(r.visible_to(&s));
        }

        #[test]
        fn row_visible_when_deleter_eq_xmin_active_but_in_progress() {
            // Boundary: deleter == xmin_active and is in in_progress → the
            // in_progress check is what saves us (the > xmin_active check
            // alone would be false).
            let r = row_with(3, Some(5));
            let s = snap(5, 10, &[5]);
            assert!(r.visible_to(&s));
        }

        #[test]
        fn row_invisible_when_deleter_eq_xmin_active_not_in_progress() {
            // Boundary: deleter == xmin_active but NOT in in_progress.
            // This is a slightly artificial case (xmin_active should be the
            // *lowest* active id, so if 5 isn't active, xmin_active should
            // be > 5 — but we still cover the predicate behavior).
            let r = row_with(3, Some(5));
            let s = snap(5, 10, &[7]);
            assert!(!r.visible_to(&s));
        }

        #[test]
        fn updated_row_chain_only_new_version_visible() {
            // Models an UPDATE: txn 6 superseded a row by stamping xmax = 6
            // on the old version and inserting a new version with xmin = 6.
            // From a snapshot taken AFTER txn 6 committed, only the new
            // version should be visible.
            let old = row_with(3, Some(6));
            let new = row_with(6, None);
            let s = snap(10, 10, &[]); // No concurrent activity.
            assert!(!old.visible_to(&s));
            assert!(new.visible_to(&s));
        }

        #[test]
        fn updated_row_chain_concurrent_snapshot_sees_old_version() {
            // Same setup as above, but our snapshot was taken WHILE txn 6
            // was still in-progress. We should see the old version (the
            // delete is from a concurrent writer), not the new version.
            let old = row_with(3, Some(6));
            let new = row_with(6, None);
            let mut in_progress = HashSet::new();
            in_progress.insert(6);
            let s = TxnSnapshot::new(6, 5, in_progress);
            assert!(old.visible_to(&s));
            assert!(!new.visible_to(&s));
        }

        #[test]
        fn empty_snapshot_hides_all_mvcc_rows() {
            // The empty snapshot has xmax_committed = 0, so any non-sentinel
            // xmin is > xmax_committed → invisible. This is the conservative
            // default: code that hasn't migrated to capture real snapshots
            // sees only pre-MVCC rows.
            assert!(!row_with(1, None).visible_to(&TxnSnapshot::empty()));
            assert!(!row_with(42, None).visible_to(&TxnSnapshot::empty()));
            assert!(row_with(0, None).visible_to(&TxnSnapshot::empty()));
        }

        #[test]
        fn deleter_with_pre_mvcc_sentinel_treated_as_committed() {
            // Defensive case: xmax = Some(0) is bizarre (means "deleted by
            // nobody") but we treat the sentinel as definitely-committed,
            // so the row is invisible. This avoids accidental visibility
            // from buggy callers stamping `Some(0)` instead of `None`.
            let r = row_with(1, Some(0));
            let s = snap(5, 10, &[]);
            // xmax = 0 < xmin_active = 5, not in_progress → invisible.
            assert!(!r.visible_to(&s));
        }

        #[test]
        fn writer_aborted_modeling_note() {
            // Phase 1b deliberately does NOT carry an `aborted` set on
            // TxnSnapshot. The current `TransactionManager::rollback`
            // restores `original_tables` wholesale, which discards any
            // xmin/xmax stamping the aborted txn did. So at the predicate
            // level there is no "writer aborted" case to test — the rows
            // simply don't exist after rollback.
            //
            // This test exists to document the design decision. Phase 1c
            // will either confirm "revert on abort" or introduce the
            // `aborted` set and a corresponding predicate clause.
            let r = row_with(3, None);
            let s = snap(5, 10, &[]);
            assert!(r.visible_to(&s));
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
