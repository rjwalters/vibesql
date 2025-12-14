//! Morsel-driven parallel sorting with k-way merge.
//!
//! This module provides parallel sorting using morsel-driven execution:
//! 1. Phase 1 (Parallel): Workers steal morsels and sort them locally
//! 2. Phase 2 (Sequential): K-way merge of sorted morsels using a min-heap

use std::{
    cmp::Ordering as CmpOrdering,
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crossbeam_deque::{Injector, Worker};
use vibesql_storage::Row;

use super::{
    config::{global_config, MorselConfig},
    create_morsels, morsel_debug_enabled, steal_morsel, Morsel, MorselResultsOrdered,
};

/// Morsel-driven parallel sort with work-stealing.
///
/// Sorts rows using a two-phase approach:
/// 1. **Phase 1 (Parallel)**: Workers steal morsels and sort them locally
/// 2. **Phase 2 (Sequential)**: K-way merge of sorted morsels using a min-heap
///
/// This provides dynamic load balancing during the sort phase while maintaining
/// O(n log k) merge complexity where k = number of morsels.
///
/// # Arguments
///
/// - `rows`: Source data to sort
/// - `config`: Morsel configuration
/// - `compare`: Comparison function for ordering rows
///
/// # Returns
///
/// Vector of rows sorted according to the comparison function.
///
/// # Example
///
/// ```text
/// let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
///     a.values[0].partial_cmp(&b.values[0]).unwrap_or(CmpOrdering::Equal)
/// });
/// ```
pub fn morsel_parallel_sort<F>(rows: &[Row], config: &MorselConfig, compare: F) -> Vec<Row>
where
    F: Fn(&Row, &Row) -> CmpOrdering + Sync + Send,
{
    if rows.is_empty() {
        return Vec::new();
    }

    // Use sort-specific morsel size (larger for better merge efficiency)
    let morsel_size = config.sort_size;

    // For small datasets, sort directly (avoid morsel overhead)
    if rows.len() < morsel_size {
        let mut result = rows.to_vec();
        result.sort_by(&compare);
        return result;
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), morsel_size);
    let morsel_count = morsels.len();

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Sort: {} morsels for {} rows (size={})",
            morsel_count,
            rows.len(),
            morsel_size
        );
    }

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage: (morsel_index, sorted_rows)
    // We track the original morsel order for deterministic merging
    let results: MorselResultsOrdered = Arc::new(Mutex::new(Vec::with_capacity(morsel_count)));
    let morsel_index = Arc::new(AtomicUsize::new(0));

    // Phase 1: Parallel sort of each morsel
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let compare_ref = &compare;
            let results_ref = results.clone();
            let morsel_index_ref = morsel_index.clone();

            s.spawn(move |_| {
                let worker: Worker<Morsel> = Worker::new_fifo();

                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    // Get a unique index for this morsel (for merge ordering)
                    let idx = morsel_index_ref.fetch_add(1, Ordering::SeqCst);

                    // Clone and sort the morsel's rows
                    let mut sorted: Vec<Row> = m.rows(rows).to_vec();
                    sorted.sort_by(compare_ref);

                    if morsel_debug_enabled() {
                        eprintln!("[MORSEL] Thread sorted morsel {} ({} rows)", idx, sorted.len());
                    }

                    results_ref.lock().unwrap().push((idx, sorted));
                }
            });
        }
    });

    // Extract sorted morsels
    let mut sorted_morsels = Arc::try_unwrap(results)
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    // Sort by morsel index to ensure deterministic merge order
    sorted_morsels.sort_by_key(|(idx, _)| *idx);

    if morsel_debug_enabled() {
        eprintln!("[MORSEL] Sort phase 1 complete: {} sorted morsels", sorted_morsels.len());
    }

    // Phase 2: K-way merge using a min-heap
    // For single morsel, return directly
    if sorted_morsels.len() == 1 {
        return sorted_morsels.pop().unwrap().1;
    }

    // Convert to owned vectors for merging
    let sorted_chunks: Vec<Vec<Row>> = sorted_morsels.into_iter().map(|(_, rows)| rows).collect();

    // Use k-way merge with min-heap
    let result = kway_merge(sorted_chunks, &compare);

    if morsel_debug_enabled() {
        eprintln!("[MORSEL] Sort complete: {} total rows", result.len());
    }

    result
}

/// K-way merge of sorted vectors using a min-heap.
///
/// Merges k sorted vectors into a single sorted vector in O(n log k) time.
fn kway_merge<F>(sorted_chunks: Vec<Vec<Row>>, compare: &F) -> Vec<Row>
where
    F: Fn(&Row, &Row) -> CmpOrdering,
{
    if sorted_chunks.is_empty() {
        return Vec::new();
    }

    if sorted_chunks.len() == 1 {
        return sorted_chunks.into_iter().next().unwrap();
    }

    // Calculate total size for pre-allocation
    let total_size: usize = sorted_chunks.iter().map(|c| c.len()).sum();
    let mut result = Vec::with_capacity(total_size);

    // Create iterators for each chunk
    let mut iters: Vec<std::vec::IntoIter<Row>> =
        sorted_chunks.into_iter().map(|c| c.into_iter()).collect();

    // Initialize heap with first element from each non-empty chunk
    // We use a max-heap with reversed comparison to simulate min-heap
    let mut heap: BinaryHeap<MergeItem<F>> = BinaryHeap::new();

    for (chunk_idx, iter) in iters.iter_mut().enumerate() {
        if let Some(row) = iter.next() {
            heap.push(MergeItem { row, chunk_idx, compare });
        }
    }

    // Merge by repeatedly taking the smallest element
    while let Some(MergeItem { row, chunk_idx, .. }) = heap.pop() {
        result.push(row);

        // Add next element from the same chunk
        if let Some(next_row) = iters[chunk_idx].next() {
            heap.push(MergeItem { row: next_row, chunk_idx, compare });
        }
    }

    result
}

/// Helper struct for k-way merge heap.
///
/// Implements Ord to work with BinaryHeap (which is a max-heap by default).
/// We reverse the comparison to get min-heap behavior.
struct MergeItem<'a, F> {
    row: Row,
    chunk_idx: usize,
    compare: &'a F,
}

impl<'a, F> PartialEq for MergeItem<'a, F>
where
    F: Fn(&Row, &Row) -> CmpOrdering,
{
    fn eq(&self, other: &Self) -> bool {
        (self.compare)(&self.row, &other.row) == CmpOrdering::Equal
    }
}

impl<'a, F> Eq for MergeItem<'a, F> where F: Fn(&Row, &Row) -> CmpOrdering {}

impl<'a, F> PartialOrd for MergeItem<'a, F>
where
    F: Fn(&Row, &Row) -> CmpOrdering,
{
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl<'a, F> Ord for MergeItem<'a, F>
where
    F: Fn(&Row, &Row) -> CmpOrdering,
{
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reverse comparison for min-heap behavior
        // BinaryHeap is a max-heap, so we flip the comparison
        (self.compare)(&other.row, &self.row)
    }
}

/// Convenience function: morsel sort using global config.
pub fn morsel_sort_by<F>(rows: &[Row], compare: F) -> Vec<Row>
where
    F: Fn(&Row, &Row) -> CmpOrdering + Sync + Send,
{
    morsel_parallel_sort(rows, global_config(), compare)
}
