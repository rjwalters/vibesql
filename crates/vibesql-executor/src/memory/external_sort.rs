//! External sort for memory-bounded ORDER BY execution
//!
//! Implements a two-phase external merge sort:
//!
//! 1. **Sort Phase**: Process input in chunks, sort each chunk in memory, and spill sorted runs to
//!    disk when memory is exhausted.
//!
//! 2. **Merge Phase**: K-way merge of sorted runs, streaming results with bounded memory usage.
//!
//! # Algorithm
//!
//! ```text
//! Input rows
//!     │
//!     ▼
//! ┌────────────────────────────────────────┐
//! │        Phase 1: Create Sorted Runs     │
//! │  ┌─────────────┐  ┌─────────────┐      │
//! │  │ In-memory   │  │  Spilled    │      │
//! │  │ buffer      │──▶  run file   │      │
//! │  └─────────────┘  └─────────────┘      │
//! └────────────────────────────────────────┘
//!     │
//!     ▼
//! ┌────────────────────────────────────────┐
//! │       Phase 2: K-way Merge             │
//! │  Run1 ──┐                              │
//! │  Run2 ──┼──▶ Min-heap ──▶ Output       │
//! │  Run3 ──┘                              │
//! └────────────────────────────────────────┘
//! ```
//!
//! # Design Decisions
//!
//! - **Chunk-based processing**: Sorts morsel-sized chunks to maintain cache locality
//! - **Lazy spilling**: Only spills when memory pressure exceeds threshold
//! - **Streaming merge**: Outputs rows incrementally without materializing all results
//! - **Stable sort**: Preserves relative order of equal elements

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    io::{self, Cursor},
    sync::Arc,
};

use vibesql_ast::OrderDirection;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::{
    row_serialization::{deserialize_row_with_keys, serialize_row_with_keys},
    MemoryController, MemoryReservation, SpillFile,
};

/// Sort key for a row: the evaluated ORDER BY values with their directions
pub type SortKey = Vec<(SqlValue, OrderDirection)>;

/// A row with its evaluated sort keys
pub type RowWithKeys = (Row, SortKey);

/// Configuration for external sort
#[derive(Debug, Clone)]
pub struct ExternalSortConfig {
    /// Maximum rows per in-memory sorted run before considering spill
    pub max_run_size: usize,

    /// Number of runs to merge at once in k-way merge
    pub merge_fanout: usize,
}

impl Default for ExternalSortConfig {
    fn default() -> Self {
        Self {
            max_run_size: 50_000, // ~50K rows per run
            merge_fanout: 16,     // 16-way merge
        }
    }
}

/// A sorted run, either in memory or spilled to disk
enum SortRun {
    /// In-memory sorted run (used when mixing memory/disk runs in merge)
    #[allow(dead_code)]
    InMemory(Vec<RowWithKeys>),

    /// Spilled to disk
    OnDisk { file: SpillFile, row_count: usize },
}

impl SortRun {
    /// Get the number of rows in this run
    fn row_count(&self) -> usize {
        match self {
            SortRun::InMemory(rows) => rows.len(),
            SortRun::OnDisk { row_count, .. } => *row_count,
        }
    }
}

/// External sort operator
///
/// Implements memory-bounded sorting with disk spilling.
pub struct ExternalSort {
    /// Memory reservation for this operator
    reservation: MemoryReservation,

    /// Configuration
    config: ExternalSortConfig,

    /// Current in-memory buffer being accumulated
    buffer: Vec<RowWithKeys>,

    /// Estimated memory used by current buffer
    buffer_memory: usize,

    /// Completed sorted runs
    runs: Vec<SortRun>,

    /// Comparison function for sort keys
    comparator: SortKeyComparator,
}

/// Comparator for sort keys
#[derive(Clone)]
struct SortKeyComparator;

impl SortKeyComparator {
    /// Compare two sort keys
    fn compare(&self, a: &SortKey, b: &SortKey) -> Ordering {
        for ((val_a, dir), (val_b, _)) in a.iter().zip(b.iter()) {
            // Handle NULLs: always sort last regardless of ASC/DESC
            let cmp = match (val_a.is_null(), val_b.is_null()) {
                (true, true) => Ordering::Equal,
                (true, false) => return Ordering::Greater, // NULL always sorts last
                (false, true) => return Ordering::Less,    // non-NULL always sorts first
                (false, false) => {
                    // Compare non-NULL values, respecting direction
                    let base_cmp = compare_sql_values(val_a, val_b);
                    match dir {
                        OrderDirection::Asc => base_cmp,
                        OrderDirection::Desc => base_cmp.reverse(),
                    }
                }
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

/// Compare two SqlValue instances
fn compare_sql_values(a: &SqlValue, b: &SqlValue) -> Ordering {
    use SqlValue::*;
    match (a, b) {
        (Integer(x), Integer(y)) => x.cmp(y),
        (Smallint(x), Smallint(y)) => x.cmp(y),
        (Bigint(x), Bigint(y)) => x.cmp(y),
        (Unsigned(x), Unsigned(y)) => x.cmp(y),
        (Float(x), Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Real(x), Real(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Double(x), Double(y)) | (Numeric(x), Numeric(y)) => {
            x.partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        (Character(x), Character(y)) | (Varchar(x), Varchar(y)) => x.cmp(y),
        (Character(x), Varchar(y)) | (Varchar(x), Character(y)) => x.as_str().cmp(y.as_str()),
        (Boolean(x), Boolean(y)) => x.cmp(y),
        (Date(x), Date(y)) => x.cmp(y),
        (Time(x), Time(y)) => x.cmp(y),
        (Timestamp(x), Timestamp(y)) => x.cmp(y),
        (Interval(x), Interval(y)) => x.cmp(y),
        _ => Ordering::Equal, // Type mismatch - treat as equal
    }
}

impl ExternalSort {
    /// Create a new external sort operator
    pub fn new(controller: &Arc<MemoryController>) -> Self {
        Self::with_config(controller, ExternalSortConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(controller: &Arc<MemoryController>, config: ExternalSortConfig) -> Self {
        Self {
            reservation: controller.create_reservation(),
            config,
            buffer: Vec::new(),
            buffer_memory: 0,
            runs: Vec::new(),
            comparator: SortKeyComparator,
        }
    }

    /// Add a row with its sort keys to the sort operator
    ///
    /// May trigger a spill if memory is exhausted.
    pub fn add_row(&mut self, row: Row, sort_keys: SortKey) -> io::Result<()> {
        // Estimate memory for this row
        let row_memory = row.estimated_size_bytes()
            + sort_keys.iter().map(|(v, _)| v.estimated_size_bytes()).sum::<usize>()
            + std::mem::size_of::<RowWithKeys>();

        // Check if we need to spill
        if !self.reservation.try_grow(row_memory) {
            // Memory exhausted - sort and spill current buffer
            self.spill_buffer()?;

            // Try to reserve again
            if !self.reservation.try_grow(row_memory) {
                // Still can't fit - this single row exceeds available memory
                // This shouldn't happen with reasonable memory limits
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "single row exceeds available memory budget",
                ));
            }
        }

        self.buffer.push((row, sort_keys));
        self.buffer_memory += row_memory;

        // Check if buffer reached max run size
        if self.buffer.len() >= self.config.max_run_size {
            self.spill_buffer()?;
        }

        Ok(())
    }

    /// Sort and spill the current buffer to disk
    fn spill_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Sort the buffer
        let comparator = self.comparator.clone();
        self.buffer.sort_by(|a, b| comparator.compare(&a.1, &b.1));

        // Create spill file
        let temp_dir = self.reservation.temp_directory();
        let mut spill_file = SpillFile::with_suffix(temp_dir, "sort_run")?;

        // Serialize all rows to the spill file
        let row_count = self.buffer.len();
        let mut spill_buf = Vec::new();

        for (row, keys) in &self.buffer {
            serialize_row_with_keys(row, keys, &mut spill_buf)?;
        }

        spill_file.write_all(&spill_buf)?;
        spill_file.flush()?;

        // Record spill metrics
        self.reservation.record_spill(spill_buf.len());

        // Release memory
        self.reservation.shrink(self.buffer_memory);
        self.buffer.clear();
        self.buffer_memory = 0;

        // Add spilled run
        self.runs.push(SortRun::OnDisk { file: spill_file, row_count });

        Ok(())
    }

    /// Finalize sorting and return an iterator over sorted results
    ///
    /// This completes the sort phase and prepares for the merge phase.
    pub fn finish(mut self) -> io::Result<SortedIterator> {
        // Sort any remaining buffer
        if !self.buffer.is_empty() {
            let comparator = self.comparator.clone();
            self.buffer.sort_by(|a, b| comparator.compare(&a.1, &b.1));

            // Keep in memory if we have no spilled runs
            if self.runs.is_empty() {
                // All data fits in memory - return directly
                return Ok(SortedIterator::InMemory(InMemoryIterator::new(
                    self.buffer,
                    self.reservation,
                )));
            }

            // We have spilled runs - spill this buffer too for consistent merge
            self.spill_buffer()?;
        }

        // If no runs, return empty iterator
        if self.runs.is_empty() {
            return Ok(SortedIterator::InMemory(InMemoryIterator::new(
                Vec::new(),
                self.reservation,
            )));
        }

        // Create merge iterator over all runs
        Ok(SortedIterator::Merge(MergeIterator::new(self.runs, self.comparator, self.reservation)?))
    }

    /// Get the number of runs created so far
    pub fn run_count(&self) -> usize {
        self.runs.len() + if self.buffer.is_empty() { 0 } else { 1 }
    }

    /// Get total rows added
    pub fn total_rows(&self) -> usize {
        self.runs.iter().map(|r| r.row_count()).sum::<usize>() + self.buffer.len()
    }
}

/// Iterator over sorted results
pub enum SortedIterator {
    /// All data fit in memory
    InMemory(InMemoryIterator),

    /// Merge iterator over spilled runs
    Merge(MergeIterator),
}

impl Iterator for SortedIterator {
    type Item = io::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SortedIterator::InMemory(iter) => iter.next(),
            SortedIterator::Merge(iter) => iter.next(),
        }
    }
}

/// Iterator over in-memory sorted results
pub struct InMemoryIterator {
    rows: std::vec::IntoIter<RowWithKeys>,
    #[allow(dead_code)]
    reservation: MemoryReservation, // Keeps memory reserved until iterator is dropped
}

impl InMemoryIterator {
    fn new(rows: Vec<RowWithKeys>, reservation: MemoryReservation) -> Self {
        Self { rows: rows.into_iter(), reservation }
    }
}

impl Iterator for InMemoryIterator {
    type Item = io::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(|(row, _keys)| Ok(row))
    }
}

/// K-way merge iterator over spilled sorted runs
pub struct MergeIterator {
    /// Min-heap for k-way merge
    heap: BinaryHeap<MergeEntry>,

    /// Run readers (one per spilled run)
    readers: Vec<RunReader>,

    /// Comparator for sort keys
    comparator: SortKeyComparator,

    /// Memory reservation
    #[allow(dead_code)]
    reservation: MemoryReservation,
}

/// Entry in the merge heap
struct MergeEntry {
    /// The row
    row: Row,

    /// Sort key for comparison
    key: SortKey,

    /// Index of the run this came from
    run_index: usize,

    /// Comparator reference for ordering
    comparator: SortKeyComparator,
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.comparator.compare(&self.key, &other.key) == Ordering::Equal
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap (BinaryHeap is a max-heap)
        self.comparator.compare(&other.key, &self.key)
    }
}

/// Reader for a spilled sorted run
struct RunReader {
    /// Cursor over spill file data (loaded into memory)
    cursor: Cursor<Vec<u8>>,

    /// Remaining rows to read
    remaining: usize,
}

impl RunReader {
    fn new(mut file: SpillFile, row_count: usize) -> io::Result<Self> {
        file.prepare_for_read()?;
        let data = file.read_to_vec()?;
        Ok(Self { cursor: Cursor::new(data), remaining: row_count })
    }

    fn read_next(&mut self) -> io::Result<Option<RowWithKeys>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let (row, keys) = deserialize_row_with_keys(&mut self.cursor)?;
        Ok(Some((row, keys)))
    }
}

impl MergeIterator {
    fn new(
        runs: Vec<SortRun>,
        comparator: SortKeyComparator,
        reservation: MemoryReservation,
    ) -> io::Result<Self> {
        let mut readers = Vec::with_capacity(runs.len());
        let mut heap = BinaryHeap::new();

        for (run_index, run) in runs.into_iter().enumerate() {
            match run {
                SortRun::InMemory(rows) => {
                    // Shouldn't happen in merge phase, but handle anyway
                    for (row, key) in rows {
                        heap.push(MergeEntry {
                            row,
                            key,
                            run_index,
                            comparator: comparator.clone(),
                        });
                    }
                    readers.push(RunReader { cursor: Cursor::new(Vec::new()), remaining: 0 });
                }
                SortRun::OnDisk { file, row_count } => {
                    let mut reader = RunReader::new(file, row_count)?;

                    // Read first entry from this run
                    if let Some((row, key)) = reader.read_next()? {
                        heap.push(MergeEntry {
                            row,
                            key,
                            run_index,
                            comparator: comparator.clone(),
                        });
                    }

                    readers.push(reader);
                }
            }
        }

        Ok(Self { heap, readers, comparator, reservation })
    }
}

impl Iterator for MergeIterator {
    type Item = io::Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        // Pop smallest entry from heap
        let entry = self.heap.pop()?;

        // Read next entry from the same run
        match self.readers[entry.run_index].read_next() {
            Ok(Some((row, key))) => {
                self.heap.push(MergeEntry {
                    row,
                    key,
                    run_index: entry.run_index,
                    comparator: self.comparator.clone(),
                });
            }
            Ok(None) => {
                // This run is exhausted
            }
            Err(e) => {
                return Some(Err(e));
            }
        }

        Some(Ok(entry.row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_controller() -> Arc<MemoryController> {
        Arc::new(MemoryController::with_budget(1024 * 1024)) // 1MB for tests
    }

    #[test]
    fn test_in_memory_sort() {
        let controller = make_test_controller();
        let mut sorter = ExternalSort::new(&controller);

        // Add rows in reverse order
        for i in (0..100).rev() {
            let row = Row::from_vec(vec![SqlValue::Integer(i)]);
            let keys = vec![(SqlValue::Integer(i), OrderDirection::Asc)];
            sorter.add_row(row, keys).unwrap();
        }

        // Finish and collect results
        let results: Vec<Row> = sorter.finish().unwrap().map(|r| r.unwrap()).collect();

        assert_eq!(results.len(), 100);
        for (i, row) in results.iter().enumerate() {
            assert_eq!(row.values[0], SqlValue::Integer(i as i64));
        }
    }

    #[test]
    fn test_descending_sort() {
        let controller = make_test_controller();
        let mut sorter = ExternalSort::new(&controller);

        // Add rows in ascending order
        for i in 0..50 {
            let row = Row::from_vec(vec![SqlValue::Integer(i)]);
            let keys = vec![(SqlValue::Integer(i), OrderDirection::Desc)];
            sorter.add_row(row, keys).unwrap();
        }

        let results: Vec<Row> = sorter.finish().unwrap().map(|r| r.unwrap()).collect();

        assert_eq!(results.len(), 50);
        // Should be in descending order: 49, 48, ..., 0
        for (i, row) in results.iter().enumerate() {
            assert_eq!(row.values[0], SqlValue::Integer(49 - i as i64));
        }
    }

    #[test]
    fn test_null_handling() {
        let controller = make_test_controller();
        let mut sorter = ExternalSort::new(&controller);

        // Add rows with NULLs
        sorter
            .add_row(
                Row::from_vec(vec![SqlValue::Integer(2)]),
                vec![(SqlValue::Integer(2), OrderDirection::Asc)],
            )
            .unwrap();
        sorter
            .add_row(
                Row::from_vec(vec![SqlValue::Null]),
                vec![(SqlValue::Null, OrderDirection::Asc)],
            )
            .unwrap();
        sorter
            .add_row(
                Row::from_vec(vec![SqlValue::Integer(1)]),
                vec![(SqlValue::Integer(1), OrderDirection::Asc)],
            )
            .unwrap();

        let results: Vec<Row> = sorter.finish().unwrap().map(|r| r.unwrap()).collect();

        // ASC: 1, 2, NULL (NULLs last)
        assert_eq!(results[0].values[0], SqlValue::Integer(1));
        assert_eq!(results[1].values[0], SqlValue::Integer(2));
        assert_eq!(results[2].values[0], SqlValue::Null);
    }

    #[test]
    fn test_multi_key_sort() {
        let controller = make_test_controller();
        let mut sorter = ExternalSort::new(&controller);

        // Add rows to test multi-key sorting
        // (a=1, b=2), (a=1, b=1), (a=2, b=1)
        sorter
            .add_row(
                Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(2)]),
                vec![
                    (SqlValue::Integer(1), OrderDirection::Asc),
                    (SqlValue::Integer(2), OrderDirection::Asc),
                ],
            )
            .unwrap();
        sorter
            .add_row(
                Row::from_vec(vec![SqlValue::Integer(1), SqlValue::Integer(1)]),
                vec![
                    (SqlValue::Integer(1), OrderDirection::Asc),
                    (SqlValue::Integer(1), OrderDirection::Asc),
                ],
            )
            .unwrap();
        sorter
            .add_row(
                Row::from_vec(vec![SqlValue::Integer(2), SqlValue::Integer(1)]),
                vec![
                    (SqlValue::Integer(2), OrderDirection::Asc),
                    (SqlValue::Integer(1), OrderDirection::Asc),
                ],
            )
            .unwrap();

        let results: Vec<Row> = sorter.finish().unwrap().map(|r| r.unwrap()).collect();

        // Expected order: (1,1), (1,2), (2,1)
        assert_eq!(results[0].values[0], SqlValue::Integer(1));
        assert_eq!(results[0].values[1], SqlValue::Integer(1));
        assert_eq!(results[1].values[0], SqlValue::Integer(1));
        assert_eq!(results[1].values[1], SqlValue::Integer(2));
        assert_eq!(results[2].values[0], SqlValue::Integer(2));
        assert_eq!(results[2].values[1], SqlValue::Integer(1));
    }

    #[test]
    fn test_spill_and_merge() {
        // Use very small memory to force spilling
        let controller = Arc::new(MemoryController::with_budget(4096)); // 4KB
        let config = ExternalSortConfig {
            max_run_size: 10, // Very small runs
            ..Default::default()
        };

        let mut sorter = ExternalSort::with_config(&controller, config);

        // Add enough rows to force multiple spills
        for i in (0..50).rev() {
            let row = Row::from_vec(vec![SqlValue::Integer(i)]);
            let keys = vec![(SqlValue::Integer(i), OrderDirection::Asc)];
            sorter.add_row(row, keys).unwrap();
        }

        // Should have created multiple runs
        assert!(sorter.run_count() > 1, "Expected multiple runs from spilling");

        // Finish and verify sorted order
        let results: Vec<Row> = sorter.finish().unwrap().map(|r| r.unwrap()).collect();

        assert_eq!(results.len(), 50);
        for (i, row) in results.iter().enumerate() {
            assert_eq!(row.values[0], SqlValue::Integer(i as i64));
        }
    }

    #[test]
    fn test_empty_sort() {
        let controller = make_test_controller();
        let sorter = ExternalSort::new(&controller);

        let results: Vec<Row> = sorter.finish().unwrap().map(|r| r.unwrap()).collect();
        assert!(results.is_empty());
    }

    #[test]
    fn test_string_sort() {
        let controller = make_test_controller();
        let mut sorter = ExternalSort::new(&controller);

        sorter
            .add_row(
                Row::from_vec(vec![SqlValue::Varchar("charlie".into())]),
                vec![(SqlValue::Varchar("charlie".into()), OrderDirection::Asc)],
            )
            .unwrap();
        sorter
            .add_row(
                Row::from_vec(vec![SqlValue::Varchar("alpha".into())]),
                vec![(SqlValue::Varchar("alpha".into()), OrderDirection::Asc)],
            )
            .unwrap();
        sorter
            .add_row(
                Row::from_vec(vec![SqlValue::Varchar("bravo".into())]),
                vec![(SqlValue::Varchar("bravo".into()), OrderDirection::Asc)],
            )
            .unwrap();

        let results: Vec<Row> = sorter.finish().unwrap().map(|r| r.unwrap()).collect();

        assert_eq!(results[0].values[0], SqlValue::Varchar("alpha".into()));
        assert_eq!(results[1].values[0], SqlValue::Varchar("bravo".into()));
        assert_eq!(results[2].values[0], SqlValue::Varchar("charlie".into()));
    }
}
