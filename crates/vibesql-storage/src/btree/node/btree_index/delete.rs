//! Delete operations for B+ Tree
//!
//! This module handles key deletion with support for both bulk delete (all row IDs)
//! and specific row ID delete operations, with automatic tree rebalancing.
//!
//! ## Batch Delete Optimization
//!
//! The `delete_batch` method provides significant performance improvements for
//! multi-row deletions by:
//! - Sorting keys for sequential leaf access
//! - Traversing leaves via `next_leaf` links instead of from-root traversals
//! - Batching rebalancing operations per leaf
//! - Single root collapse check at the end

use crate::StorageError;

use super::super::structure::{Key, LeafNode, RowId};
use super::BTreeIndex;

impl BTreeIndex {
    /// Delete all row IDs for a key from the B+ tree
    ///
    /// For non-unique indexes, this removes the key and **all** associated row IDs
    /// in a single operation. If you need to remove only specific row IDs while
    /// keeping others, you must implement that logic at a higher level.
    ///
    /// Implements full multi-level tree deletion with node merging and rebalancing.
    /// When a deletion causes a leaf node to become underfull, it will try to borrow
    /// entries from sibling nodes or merge with a sibling if borrowing isn't possible.
    ///
    /// # Arguments
    /// * `key` - The key to delete (removes all associated row_ids)
    ///
    /// # Returns
    /// * `Ok(true)` if the key was found and deleted (all row IDs removed)
    /// * `Ok(false)` if the key was not found
    /// * `Err(_)` if an I/O error occurred
    ///
    /// # Performance
    /// - O(log n) to find key + O(log n) for potential rebalancing
    /// - All row IDs for the key are removed regardless of count
    ///
    /// # Example
    /// ```text
    /// use vibesql_types::SqlValue;
    ///
    /// // Insert duplicate keys
    /// index.insert(vec![SqlValue::Integer(42)], 1)?;
    /// index.insert(vec![SqlValue::Integer(42)], 2)?;
    /// index.insert(vec![SqlValue::Integer(42)], 3)?;
    ///
    /// // Delete removes ALL row IDs for key 42
    /// let deleted = index.delete(&vec![SqlValue::Integer(42)])?;
    /// assert!(deleted); // true - key was found and removed
    ///
    /// // Subsequent lookup returns empty
    /// let rows = index.lookup(&vec![SqlValue::Integer(42)])?;
    /// assert!(rows.is_empty());
    /// ```
    ///
    /// # Algorithm
    /// 1. Find the leaf node containing the key
    /// 2. Delete all row_ids for the key from the leaf (single operation)
    /// 3. If leaf becomes underfull, try to borrow from sibling or merge
    /// 4. Propagate rebalancing up the tree if necessary
    /// 5. Collapse the root if it has only one child
    pub fn delete(&mut self, key: &Key) -> Result<bool, StorageError> {
        // Handle single-level tree (root is leaf)
        if self.height == 1 {
            let mut root_leaf = self.read_leaf_node(self.root_page_id)?;
            let deleted = root_leaf.delete_all(key);
            if deleted {
                self.write_leaf_node(&root_leaf)?;
            }
            return Ok(deleted);
        }

        // Multi-level tree: find leaf and track path
        let (mut leaf, path) = self.find_leaf_path(key)?;

        // Delete all row_ids for the key from leaf
        if !leaf.delete_all(key) {
            return Ok(false); // Key not found
        }

        // Write leaf back
        self.write_leaf_node(&leaf)?;

        // Check if rebalancing needed
        if leaf.is_underfull(self.degree) {
            self.rebalance_leaf(leaf, path)?;
        }

        // Check if root should be collapsed
        self.maybe_collapse_root()?;

        Ok(true)
    }

    /// Delete a specific row_id for a key from the B+ tree
    ///
    /// Unlike `delete()` which removes all row_ids for a key, this method removes
    /// only the specified row_id. If this is the last row_id for the key, the key
    /// will be removed entirely from the index.
    ///
    /// Implements full multi-level tree deletion with node merging and rebalancing.
    /// When a deletion causes a leaf node to become underfull, it will try to borrow
    /// entries from sibling nodes or merge with a sibling if borrowing isn't possible.
    ///
    /// # Arguments
    /// * `key` - The key to search for
    /// * `row_id` - The specific row_id to remove
    ///
    /// # Returns
    /// * `Ok(true)` if the row_id was found and deleted
    /// * `Ok(false)` if the key or row_id was not found
    /// * `Err(_)` if an I/O error occurred
    ///
    /// # Algorithm
    /// 1. Find the leaf node containing the key
    /// 2. Delete the specific row_id from the leaf
    /// 3. If leaf becomes underfull, try to borrow from sibling or merge
    /// 4. Propagate rebalancing up the tree if necessary
    /// 5. Collapse the root if it has only one child
    ///
    /// # Use Cases
    /// - UPDATE operations: Remove old row_id from old key when indexed column changes
    /// - DELETE operations: Remove specific row when multiple rows share the same key
    /// - Partial cleanup: Remove stale entries without affecting duplicates
    pub fn delete_specific(&mut self, key: &Key, row_id: RowId) -> Result<bool, StorageError> {
        // Handle single-level tree (root is leaf)
        if self.height == 1 {
            let mut root_leaf = self.read_leaf_node(self.root_page_id)?;
            let deleted = root_leaf.delete(key, row_id);
            if deleted {
                self.write_leaf_node(&root_leaf)?;
            }
            return Ok(deleted);
        }

        // Multi-level tree: find leaf and track path
        let (mut leaf, path) = self.find_leaf_path(key)?;

        // Delete specific row_id from leaf
        if !leaf.delete(key, row_id) {
            return Ok(false); // Key or row_id not found
        }

        // Write leaf back
        self.write_leaf_node(&leaf)?;

        // Check if rebalancing needed
        if leaf.is_underfull(self.degree) {
            self.rebalance_leaf(leaf, path)?;
        }

        // Check if root should be collapsed
        self.maybe_collapse_root()?;

        Ok(true)
    }

    /// Delete multiple (key, row_id) pairs from the B+ tree in batch
    ///
    /// This method is significantly more efficient than calling `delete_specific` in a loop
    /// when deleting multiple entries. It optimizes by:
    ///
    /// 1. **Sorting entries by key**: Enables sequential leaf traversal
    /// 2. **Sequential leaf access**: Uses `next_leaf` links instead of from-root traversals
    /// 3. **Batched leaf writes**: Multiple deletions per leaf before writing
    /// 4. **Deferred rebalancing**: Rebalances only when moving to next leaf
    /// 5. **Single root collapse**: Checks root collapse once at the end
    ///
    /// # Arguments
    /// * `entries` - Slice of (key, row_id) pairs to delete
    ///
    /// # Returns
    /// * `Ok(count)` - Number of entries successfully deleted
    /// * `Err(_)` - If an I/O error occurred
    ///
    /// # Performance
    /// - Individual deletes: O(n * log m) where n = deletions, m = keys in tree
    /// - Batch delete: O(n + log m) for sorted keys in same/adjacent leaves
    ///
    /// # Example
    /// ```text
    /// use vibesql_types::SqlValue;
    ///
    /// // Delete multiple index entries in batch
    /// let entries_to_delete = vec![
    ///     (vec![SqlValue::Integer(1)], 10),
    ///     (vec![SqlValue::Integer(2)], 20),
    ///     (vec![SqlValue::Integer(3)], 30),
    /// ];
    /// let deleted = index.delete_batch(&entries_to_delete)?;
    /// assert_eq!(deleted, 3);
    /// ```
    pub fn delete_batch(&mut self, entries: &[(Key, RowId)]) -> Result<usize, StorageError> {
        if entries.is_empty() {
            return Ok(0);
        }

        // Sort entries by key for sequential leaf access
        let mut sorted_entries: Vec<_> = entries.to_vec();
        sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

        let mut deleted_count = 0;

        // Handle single-level tree (root is leaf) - simple case
        if self.height == 1 {
            let mut root_leaf = self.read_leaf_node(self.root_page_id)?;
            let mut modified = false;

            for (key, row_id) in &sorted_entries {
                if root_leaf.delete(key, *row_id) {
                    deleted_count += 1;
                    modified = true;
                }
            }

            if modified {
                self.write_leaf_node(&root_leaf)?;
            }
            return Ok(deleted_count);
        }

        // Multi-level tree: use optimized batch traversal
        deleted_count = self.delete_batch_multi_level(&sorted_entries)?;

        // Single root collapse check at the end
        self.maybe_collapse_root()?;

        Ok(deleted_count)
    }

    /// Internal batch delete for multi-level trees
    ///
    /// Traverses leaves sequentially using next_leaf links and batches operations.
    fn delete_batch_multi_level(
        &mut self,
        sorted_entries: &[(Key, RowId)],
    ) -> Result<usize, StorageError> {
        if sorted_entries.is_empty() {
            return Ok(0);
        }

        let mut deleted_count = 0;
        let mut entry_idx = 0;

        // Find the first leaf containing the first key
        let first_key = &sorted_entries[0].0;
        let (mut current_leaf, mut current_path) = self.find_leaf_path(first_key)?;
        let mut leaf_modified = false;

        while entry_idx < sorted_entries.len() {
            let (key, row_id) = &sorted_entries[entry_idx];

            // Check if current key might be in this leaf
            // A key is in this leaf if:
            // - The leaf is empty (unlikely but possible after deletions)
            // - The key >= first entry's key (or leaf is empty)
            // - The key < next leaf's first key (or no next leaf)
            let key_might_be_here = self.key_might_be_in_leaf(key, &current_leaf);

            if key_might_be_here {
                // Try to delete from current leaf
                if current_leaf.delete(key, *row_id) {
                    deleted_count += 1;
                    leaf_modified = true;
                }
                entry_idx += 1;
            } else {
                // Key is beyond this leaf - need to move to next leaf
                // First, handle the current leaf if modified
                if leaf_modified {
                    self.write_leaf_node(&current_leaf)?;

                    // Check rebalancing for this leaf
                    if current_leaf.is_underfull(self.degree) {
                        self.rebalance_leaf(current_leaf.clone(), current_path.clone())?;
                    }
                    leaf_modified = false;
                }

                // Move to the next leaf that might contain the key
                if current_leaf.next_leaf == super::super::super::NULL_PAGE_ID {
                    // No more leaves - remaining entries don't exist
                    break;
                }

                // Check if we should traverse via next_leaf or find_leaf_path
                // Use next_leaf for adjacent keys, find_leaf_path for big jumps
                let next_leaf = self.read_leaf_node(current_leaf.next_leaf)?;

                if let Some((first_next_key, _)) = next_leaf.entries.first() {
                    if key >= first_next_key {
                        // Key might be in next leaf - use sequential traversal
                        // Need to get the path for the next leaf for potential rebalancing
                        (current_leaf, current_path) = self.find_leaf_path(key)?;
                    } else {
                        // Key is between current and next leaf - it doesn't exist
                        // Skip to next entry
                        entry_idx += 1;
                    }
                } else {
                    // Next leaf is empty - use find_leaf_path
                    (current_leaf, current_path) = self.find_leaf_path(key)?;
                }
            }
        }

        // Handle final leaf if modified
        if leaf_modified {
            self.write_leaf_node(&current_leaf)?;

            if current_leaf.is_underfull(self.degree) {
                self.rebalance_leaf(current_leaf, current_path)?;
            }
        }

        Ok(deleted_count)
    }

    /// Check if a key might be in the given leaf node
    ///
    /// Returns true if the key could potentially be in this leaf based on
    /// the leaf's entries and structure. Returns false if the key is definitely
    /// not in this leaf (e.g., key is greater than all entries and there's a next leaf).
    fn key_might_be_in_leaf(&self, key: &Key, leaf: &LeafNode) -> bool {
        if leaf.entries.is_empty() {
            // Empty leaf can't contain the key, but we should check next leaf
            return false;
        }

        // Check if key is less than the first entry - it can't be here
        if key < &leaf.entries[0].0 {
            return false;
        }

        // Check if key is greater than the last entry
        if let Some((last_key, _)) = leaf.entries.last() {
            if key > last_key {
                // Key is beyond this leaf's range
                // It might be in the next leaf if one exists
                return leaf.next_leaf == super::super::super::NULL_PAGE_ID;
            }
        }

        // Key is within the range of this leaf
        true
    }
}
