//! Insert operations for B+ Tree
//!
//! This module handles key insertion with support for duplicate keys (non-unique indexes),
//! node splitting, and propagation of splits up the tree.

use crate::page::PageId;
use crate::StorageError;

use super::super::structure::{InternalNode, Key, LeafNode, RowId};
use super::BTreeIndex;

impl BTreeIndex {
    /// Navigate to leaf node that should contain the key, returning the path from root
    ///
    /// Returns (leaf_node, path) where path is Vec<(PageId, child_index)>
    /// The path tracks which child was taken at each internal node level
    pub(super) fn find_leaf_path(
        &self,
        key: &Key,
    ) -> Result<(LeafNode, Vec<(PageId, usize)>), StorageError> {
        let mut path = Vec::new();
        let mut current_page_id = self.root_page_id;

        // Navigate down the tree
        for _ in 0..self.height - 1 {
            // Read internal node
            let internal = self.read_internal_node(current_page_id)?;

            // Find which child to follow
            let child_idx = internal.find_child_index(key);
            path.push((current_page_id, child_idx));

            // Move to child
            current_page_id = internal.children[child_idx];
        }

        // Read leaf node
        let leaf = self.read_leaf_node(current_page_id)?;

        Ok((leaf, path))
    }

    /// Insert a key-value pair into the B+ tree
    ///
    /// This method fully supports duplicate keys for non-unique indexes. If the key
    /// already exists, the row_id is appended to the existing Vec of row IDs for
    /// that key. The operation always succeeds (unless there's an I/O error).
    ///
    /// # Arguments
    /// * `key` - The key to insert
    /// * `row_id` - The row ID associated with this key
    ///
    /// # Returns
    /// Ok(()) if successful, or StorageError if I/O error occurs
    ///
    /// # Performance
    /// - **New key**: O(log n) to find position + O(degree) for potential split
    /// - **Duplicate key**: O(log n) to find + O(1) to append to existing Vec
    ///
    /// # Example
    /// ```text
    /// use vibesql_types::SqlValue;
    ///
    /// // Insert first occurrence of key 42
    /// index.insert(vec![SqlValue::Integer(42)], 100)?;
    ///
    /// // Insert duplicate key - appends to existing entry
    /// index.insert(vec![SqlValue::Integer(42)], 200)?;
    ///
    /// // Lookup returns both row IDs
    /// let rows = index.lookup(&vec![SqlValue::Integer(42)])?;
    /// assert_eq!(rows, vec![100, 200]);
    /// ```
    ///
    /// # Implementation
    /// This method supports duplicate keys for non-unique indexes:
    /// 1. Finding the appropriate leaf node
    /// 2. Inserting into the leaf (appending if key exists)
    /// 3. Handling splits that may propagate up the tree
    /// 4. Creating a new root if split reaches the original root
    pub fn insert(&mut self, key: Key, row_id: RowId) -> Result<(), StorageError> {
        // Special case: height 1 means root is a leaf
        if self.height == 1 {
            // Read root as leaf
            let mut root_leaf = self.read_leaf_node(self.root_page_id)?;

            // Insert (always succeeds, supports duplicates)
            root_leaf.insert(key.clone(), row_id);

            // Check if leaf is now full and needs splitting
            if root_leaf.is_full(self.degree) {
                // Allocate page for right sibling
                let new_page_id = self.page_manager.allocate_page()?;

                // Split the leaf
                let (split_key, right_leaf) = root_leaf.split(new_page_id);

                // Update the original next neighbor's prev_leaf pointer if it exists
                if right_leaf.next_leaf != super::super::super::NULL_PAGE_ID {
                    let mut next_neighbor = self.read_leaf_node(right_leaf.next_leaf)?;
                    next_neighbor.prev_leaf = right_leaf.page_id;
                    self.write_leaf_node(&next_neighbor)?;
                }

                // Write both leaves
                self.write_leaf_node(&root_leaf)?;
                self.write_leaf_node(&right_leaf)?;

                // Create new root (special case: first split increases height from 1 to 2)
                self.create_new_root(split_key, root_leaf.page_id, right_leaf.page_id)?;
            } else {
                // No split needed, just write leaf back
                self.write_leaf_node(&root_leaf)?;
            }

            return Ok(());
        }

        // Multi-level tree case
        // Find the target leaf and path from root
        let (mut leaf, path) = self.find_leaf_path(&key)?;

        // Insert into leaf (always succeeds, supports duplicates)
        leaf.insert(key.clone(), row_id);

        // Check if leaf is full and needs splitting
        if leaf.is_full(self.degree) {
            // Allocate page for right sibling
            let new_page_id = self.page_manager.allocate_page()?;

            // Split the leaf
            let (split_key, right_leaf) = leaf.split(new_page_id);

            // Update the original next neighbor's prev_leaf pointer if it exists
            if right_leaf.next_leaf != super::super::super::NULL_PAGE_ID {
                let mut next_neighbor = self.read_leaf_node(right_leaf.next_leaf)?;
                next_neighbor.prev_leaf = right_leaf.page_id;
                self.write_leaf_node(&next_neighbor)?;
            }

            // Write both leaves
            self.write_leaf_node(&leaf)?;
            self.write_leaf_node(&right_leaf)?;

            // Propagate split upward
            self.propagate_split(path, split_key, right_leaf.page_id)?;
        } else {
            // No split needed, just write leaf back
            self.write_leaf_node(&leaf)?;
        }

        Ok(())
    }

    /// Propagate a split upward through internal nodes
    ///
    /// This is called after a child node splits, and needs to insert the split key
    /// into parent nodes, potentially causing cascading splits up to the root
    pub(super) fn propagate_split(
        &mut self,
        mut path: Vec<(PageId, usize)>,
        mut split_key: Key,
        mut right_page_id: PageId,
    ) -> Result<(), StorageError> {
        // Work backwards up the tree
        while let Some((parent_page_id, _child_idx)) = path.pop() {
            let mut parent = self.read_internal_node(parent_page_id)?;

            // Insert the split key and right child into parent
            parent.insert_child(split_key.clone(), right_page_id);

            // Check if parent is now full and needs splitting
            if parent.is_full(self.degree) {
                // Allocate new page for right sibling
                let new_page_id = self.page_manager.allocate_page()?;

                // Split the parent
                let (middle_key, right_parent) = parent.split(new_page_id);

                // Write both nodes back
                self.write_internal_node(&parent)?;
                self.write_internal_node(&right_parent)?;

                // Continue propagating split upward
                split_key = middle_key;
                right_page_id = right_parent.page_id;
            } else {
                // No more splits needed, write parent and done
                self.write_internal_node(&parent)?;
                return Ok(());
            }
        }

        // If we get here, split propagated all the way to root
        // Need to create new root
        self.create_new_root(split_key, self.root_page_id, right_page_id)?;

        Ok(())
    }

    /// Create a new root node when a split propagates to the original root
    ///
    /// This increases the tree height by 1
    pub(super) fn create_new_root(
        &mut self,
        split_key: Key,
        left_child: PageId,
        right_child: PageId,
    ) -> Result<(), StorageError> {
        // Allocate new page for the new root
        let new_root_page_id = self.page_manager.allocate_page()?;

        // Create new internal node as root
        let mut new_root = InternalNode::new(new_root_page_id);

        // Add left child, split key, and right child
        new_root.children.push(left_child);
        new_root.keys.push(split_key);
        new_root.children.push(right_child);

        // Write new root to disk
        self.write_internal_node(&new_root)?;

        // Update index metadata
        self.root_page_id = new_root_page_id;
        self.height += 1;
        self.save_metadata()?;

        Ok(())
    }
}
