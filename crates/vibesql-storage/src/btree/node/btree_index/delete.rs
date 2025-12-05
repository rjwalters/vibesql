//! Delete operations for B+ Tree
//!
//! This module handles key deletion with support for both bulk delete (all row IDs)
//! and specific row ID delete operations, with automatic tree rebalancing.

use crate::StorageError;

use super::super::structure::{Key, RowId};
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
    /// ```ignore
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
}
