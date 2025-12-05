//! Bulk loading operations for B+ Tree
//!
//! This module provides efficient bottom-up construction of B+ trees from
//! sorted data, which is approximately 10x faster than incremental inserts.
//!
//! # Performance Optimization
//!
//! Bulk loading uses deferred sync mode to avoid expensive `fsync` calls on
//! every page write. Instead, pages are written to the OS buffer cache and
//! a single `sync` call is made at the end of the bulk load operation.
//! This can reduce bulk load time from minutes to seconds for large datasets.

use std::sync::Arc;
use vibesql_types::DataType;

use crate::page::{PageId, PageManager, PAGE_SIZE};
use crate::StorageError;

use super::super::super::serialize::{write_internal_node_no_sync, write_leaf_node_no_sync};
use super::super::super::{calculate_degree, estimate_max_key_size};
use super::super::structure::{InternalNode, Key, LeafNode};
use super::BTreeIndex;

impl BTreeIndex {
    /// Build B+ tree from pre-sorted data using bottom-up construction
    ///
    /// This is significantly faster than incremental inserts for large datasets
    /// because it avoids node splitting and builds optimally packed nodes.
    ///
    /// # Arguments
    /// * `sorted_entries` - Pre-sorted key-value pairs (must be sorted by key)
    ///   Duplicate keys are automatically grouped together.
    /// * `key_schema` - Data types of key columns
    /// * `page_manager` - Page manager for disk I/O
    ///
    /// # Returns
    /// A new B+ tree index with all entries loaded
    ///
    /// # Performance
    /// - Sorting: O(n log n) if not already sorted
    /// - Grouping: O(n)
    /// - Leaf construction: O(n)
    /// - Internal node construction: O(n)
    /// - Total: O(n log n) dominated by sorting
    ///
    /// Approximately 10x faster than incremental insert for 100K+ rows
    pub fn bulk_load(
        sorted_entries: Vec<(Key, super::super::structure::RowId)>,
        key_schema: Vec<DataType>,
        page_manager: Arc<PageManager>,
    ) -> Result<Self, StorageError> {
        let degree = calculate_degree(&key_schema);
        let metadata_page_id = 0;

        // Handle empty case
        if sorted_entries.is_empty() {
            return Self::new(page_manager, key_schema);
        }

        // Calculate max row_ids per entry based on page constraints
        // Page layout: header(3) + entry(key + varint + row_ids * 8) + prev/next leaf(16)
        // For a single entry to fit, we need: 3 + key_size + 5 + row_ids * 8 + 16 <= PAGE_SIZE
        // Using conservative varint estimate of 5 bytes for large counts
        let key_size = estimate_max_key_size(&key_schema);
        let fixed_overhead = 3 + key_size + 5 + 16; // header + key + varint + leaf pointers
        let max_row_ids_per_entry = if fixed_overhead >= PAGE_SIZE {
            1 // Degenerate case - very large keys
        } else {
            (PAGE_SIZE - fixed_overhead) / 8
        };
        // Use 80% of max to leave room for serialization overhead
        let max_row_ids_per_entry = (max_row_ids_per_entry * 4 / 5).max(1);

        // Group entries by key to support non-unique indexes
        // This converts Vec<(Key, RowId)> to Vec<(Key, Vec<RowId>)>
        let mut grouped_entries: Vec<(Key, Vec<super::super::structure::RowId>)> = Vec::new();

        for (key, row_id) in sorted_entries {
            if let Some((last_key, row_ids)) = grouped_entries.last_mut() {
                if last_key == &key {
                    // Same key, append row_id
                    row_ids.push(row_id);
                    continue;
                }
            }
            // New key, create new entry
            grouped_entries.push((key, vec![row_id]));
        }

        // Split entries with too many row_ids into multiple entries
        // This ensures each entry can fit within a single page
        let mut chunked_entries: Vec<(Key, Vec<super::super::structure::RowId>)> = Vec::new();
        for (key, row_ids) in grouped_entries {
            if row_ids.len() <= max_row_ids_per_entry {
                chunked_entries.push((key, row_ids));
            } else {
                // Split into chunks
                for chunk in row_ids.chunks(max_row_ids_per_entry) {
                    chunked_entries.push((key.clone(), chunk.to_vec()));
                }
            }
        }
        let grouped_entries = chunked_entries;

        // Calculate leaf capacity based on typical entry sizes (most entries have 1 row_id)
        // Entry size for single row_id: key + 1 byte varint + 8 bytes row_id
        let typical_entry_size = key_size + 1 + 8;
        let available_for_entries = PAGE_SIZE.saturating_sub(19); // header + prev/next leaf
        let leaf_capacity = (available_for_entries / typical_entry_size).max(1);
        // Apply 75% fill factor
        let leaf_capacity = ((leaf_capacity * 3) / 4).max(1);

        // 1. Build leaf level
        let mut leaf_page_ids = Vec::new();
        let mut prev_leaf_page_id: Option<PageId> = None;

        let mut entries_iter = grouped_entries.into_iter().peekable();

        while entries_iter.peek().is_some() {
            // Allocate page for new leaf
            let page_id = page_manager.allocate_page()?;
            let mut leaf = LeafNode::new(page_id);

            // Fill leaf node based on actual serialized size
            let mut current_size = 0;
            let max_size = available_for_entries;

            while let Some((_key, row_ids)) = entries_iter.peek() {
                // Calculate actual size of this entry
                // varint encoding: 1 byte for counts < 128, 2 bytes for < 16384, etc.
                let varint_size = if row_ids.len() < 128 {
                    1
                } else if row_ids.len() < 16384 {
                    2
                } else {
                    3
                };
                let entry_size = key_size + varint_size + row_ids.len() * 8;

                // Check if we have room for this entry
                if current_size > 0 && current_size + entry_size > max_size {
                    break; // This entry would overflow the page
                }

                // Add the entry
                current_size += entry_size;
                let (key, row_ids) = entries_iter.next().unwrap();
                leaf.entries.push((key, row_ids));

                // Also respect the target fill factor (approx leaf_capacity entries)
                if leaf.entries.len() >= leaf_capacity {
                    break;
                }
            }

            // Update doubly-linked list pointers
            if let Some(prev_id) = prev_leaf_page_id {
                // Update prev_leaf pointer of current leaf
                leaf.prev_leaf = prev_id;

                // Update the previous leaf's next_leaf pointer
                // Read previous leaf, update it, and write it back (no sync)
                let mut prev_leaf =
                    super::super::super::serialize::read_leaf_node(&page_manager, prev_id)?;
                prev_leaf.next_leaf = page_id;
                write_leaf_node_no_sync(&page_manager, &prev_leaf, degree)?;
            }

            // Write current leaf to disk (no sync - will sync once at end)
            write_leaf_node_no_sync(&page_manager, &leaf, degree)?;

            leaf_page_ids.push(page_id);
            prev_leaf_page_id = Some(page_id);
        }

        // 2. Build internal levels bottom-up
        let mut current_level = leaf_page_ids;
        let mut height = 1; // Start with leaf level

        while current_level.len() > 1 {
            height += 1;
            let mut next_level = Vec::new();

            // Calculate internal node capacity (similar fill factor)
            let internal_capacity = (degree * 3) / 4;
            let internal_capacity = internal_capacity.max(2); // At least 2 children

            let mut level_iter = current_level.into_iter().peekable();

            while level_iter.peek().is_some() {
                // Allocate page for new internal node
                let page_id = page_manager.allocate_page()?;
                let mut internal = InternalNode::new(page_id);

                // Add first child without a separator key
                if let Some(child_page_id) = level_iter.next() {
                    internal.children.push(child_page_id);
                }

                // Add remaining children with separator keys
                for _ in 1..internal_capacity {
                    if let Some(child_page_id) = level_iter.next() {
                        // Read first key from child node
                        let first_key = read_first_key_from_page(&page_manager, child_page_id)?;
                        internal.keys.push(first_key);
                        internal.children.push(child_page_id);
                    } else {
                        break;
                    }
                }

                // Write internal node to disk (no sync - will sync once at end)
                write_internal_node_no_sync(&page_manager, &internal, degree)?;

                next_level.push(page_id);
            }

            current_level = next_level;
        }

        // 3. Root is the single remaining node
        let root_page_id = current_level[0];

        let index =
            BTreeIndex { root_page_id, key_schema, degree, height, page_manager, metadata_page_id };

        // Save metadata without syncing (we'll sync once at the end)
        index.save_metadata_no_sync()?;

        // Sync all page writes to disk in a single fsync call
        // This is the key optimization: instead of syncing after every page write,
        // we batch all writes and sync once at the end
        index.page_manager.sync()?;

        Ok(index)
    }
}

/// Helper function to read the first key from a page (either internal or leaf)
pub(super) fn read_first_key_from_page(
    page_manager: &Arc<PageManager>,
    page_id: PageId,
) -> Result<Key, StorageError> {
    let page = page_manager.read_page(page_id)?;

    // Read page type (first byte)
    let page_type = page.data[0];

    if page_type == super::super::super::PAGE_TYPE_LEAF {
        let leaf = super::super::super::serialize::read_leaf_node(page_manager, page_id)?;
        if leaf.entries.is_empty() {
            return Err(StorageError::IoError("Leaf node has no entries".to_string()));
        }
        Ok(leaf.entries[0].0.clone())
    } else if page_type == super::super::super::PAGE_TYPE_INTERNAL {
        let internal = super::super::super::serialize::read_internal_node(page_manager, page_id)?;
        if internal.keys.is_empty() {
            // Internal node with only one child - need to recurse
            if internal.children.is_empty() {
                return Err(StorageError::IoError(
                    "Internal node has no keys or children".to_string(),
                ));
            }
            return read_first_key_from_page(page_manager, internal.children[0]);
        }
        Ok(internal.keys[0].clone())
    } else {
        Err(StorageError::IoError(format!("Invalid page type: {}", page_type)))
    }
}
