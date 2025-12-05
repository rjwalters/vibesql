//! Rebalancing operations for B+ Tree
//!
//! This module handles tree rebalancing after deletions, including borrowing
//! from siblings, merging nodes, and collapsing the root.

use crate::page::PageId;
use crate::StorageError;

use super::super::structure::{InternalNode, LeafNode};
use super::BTreeIndex;

impl BTreeIndex {
    /// Rebalance a leaf node after deletion
    ///
    /// Tries to borrow from siblings first, then merges if necessary.
    pub(super) fn rebalance_leaf(
        &mut self,
        leaf: LeafNode,
        path: Vec<(PageId, usize)>,
    ) -> Result<(), StorageError> {
        if path.is_empty() {
            // Leaf is root, no rebalancing needed
            return Ok(());
        }

        let (parent_id, child_idx) = *path.last().unwrap();
        let mut parent = self.read_internal_node(parent_id)?;

        // Try to borrow from sibling
        if self.try_borrow_leaf(&leaf, &mut parent, child_idx)? {
            // Successfully borrowed
            self.write_internal_node(&parent)?;
            return Ok(());
        }

        // Must merge with sibling
        self.merge_leaf(leaf, &mut parent, child_idx)?;
        self.write_internal_node(&parent)?;

        // Check if parent is now underfull
        if parent.children.len() < self.degree / 2 && path.len() > 1 {
            // Propagate rebalancing up
            self.propagate_rebalance_delete(path)?;
        }

        Ok(())
    }

    /// Try to borrow entries from sibling leaf nodes
    ///
    /// Returns true if successfully borrowed, false if no sibling has spare entries.
    fn try_borrow_leaf(
        &mut self,
        leaf: &LeafNode,
        parent: &mut InternalNode,
        idx: usize,
    ) -> Result<bool, StorageError> {
        // Try left sibling
        if idx > 0 {
            let mut left_sibling = self.read_leaf_node(parent.children[idx - 1])?;
            if left_sibling.entries.len() > self.degree / 2 {
                // Borrow last entry from left sibling
                let borrowed = left_sibling.entries.pop().unwrap();

                let mut updated_leaf = leaf.clone();
                updated_leaf.entries.insert(0, borrowed);

                // Update parent separator key
                parent.keys[idx - 1] = updated_leaf.entries[0].0.clone();

                // Write nodes back
                self.write_leaf_node(&left_sibling)?;
                self.write_leaf_node(&updated_leaf)?;

                return Ok(true);
            }
        }

        // Try right sibling
        if idx < parent.children.len() - 1 {
            let mut right_sibling = self.read_leaf_node(parent.children[idx + 1])?;
            if right_sibling.entries.len() > self.degree / 2 {
                // Borrow first entry from right sibling
                let borrowed = right_sibling.entries.remove(0);

                let mut updated_leaf = leaf.clone();
                updated_leaf.entries.push(borrowed);

                // Update parent separator key
                parent.keys[idx] = right_sibling.entries[0].0.clone();

                // Write nodes back
                self.write_leaf_node(&right_sibling)?;
                self.write_leaf_node(&updated_leaf)?;

                return Ok(true);
            }
        }

        Ok(false) // No sibling has enough entries to borrow
    }

    /// Merge underfull leaf with sibling
    fn merge_leaf(
        &mut self,
        leaf: LeafNode,
        parent: &mut InternalNode,
        idx: usize,
    ) -> Result<(), StorageError> {
        // Prefer merging with left sibling
        if idx > 0 {
            let mut left_sibling = self.read_leaf_node(parent.children[idx - 1])?;

            // Merge leaf into left sibling
            left_sibling.entries.extend(leaf.entries);
            left_sibling.next_leaf = leaf.next_leaf;

            self.write_leaf_node(&left_sibling)?;

            // Remove from parent
            parent.children.remove(idx);
            parent.keys.remove(idx - 1);

            // Deallocate merged node
            self.page_manager.deallocate_page(leaf.page_id)?;
        } else {
            // Merge with right sibling
            let right_sibling = self.read_leaf_node(parent.children[idx + 1])?;

            // Merge right into leaf
            let mut updated_leaf = leaf.clone();
            updated_leaf.entries.extend(right_sibling.entries);
            updated_leaf.next_leaf = right_sibling.next_leaf;

            self.write_leaf_node(&updated_leaf)?;

            // Remove from parent
            parent.children.remove(idx + 1);
            parent.keys.remove(idx);

            // Deallocate merged node
            self.page_manager.deallocate_page(right_sibling.page_id)?;
        }

        Ok(())
    }

    /// Propagate rebalancing up the tree after deletion
    pub(super) fn propagate_rebalance_delete(
        &mut self,
        path: Vec<(PageId, usize)>,
    ) -> Result<(), StorageError> {
        // Process from bottom to top (skip the last entry which we already handled)
        for i in (0..path.len() - 1).rev() {
            let (parent_id, child_idx) = path[i];
            let (current_id, _) = path[i + 1];

            let mut parent = self.read_internal_node(parent_id)?;
            let current = self.read_internal_node(current_id)?;

            // Check if current node is underfull
            if current.children.len() < self.degree / 2 {
                // Try to borrow or merge at internal node level
                if !self.try_borrow_internal(&current, &mut parent, child_idx)? {
                    self.merge_internal(current, &mut parent, child_idx)?;
                }
                self.write_internal_node(&parent)?;
            } else {
                // Parent is OK, stop propagating
                self.write_internal_node(&parent)?;
                break;
            }
        }

        Ok(())
    }

    /// Try to borrow entries from sibling internal nodes
    fn try_borrow_internal(
        &mut self,
        node: &InternalNode,
        parent: &mut InternalNode,
        idx: usize,
    ) -> Result<bool, StorageError> {
        // Try left sibling
        if idx > 0 {
            let mut left_sibling = self.read_internal_node(parent.children[idx - 1])?;
            if left_sibling.children.len() > self.degree / 2 {
                // Borrow last child and key from left sibling
                let borrowed_child = left_sibling.children.pop().unwrap();
                let borrowed_key = left_sibling.keys.pop().unwrap();

                let mut updated_node = node.clone();
                updated_node.children.insert(0, borrowed_child);
                updated_node.keys.insert(0, parent.keys[idx - 1].clone());

                // Update parent separator key
                parent.keys[idx - 1] = borrowed_key;

                // Write nodes back
                self.write_internal_node(&left_sibling)?;
                self.write_internal_node(&updated_node)?;

                return Ok(true);
            }
        }

        // Try right sibling
        if idx < parent.children.len() - 1 {
            let mut right_sibling = self.read_internal_node(parent.children[idx + 1])?;
            if right_sibling.children.len() > self.degree / 2 {
                // Borrow first child and key from right sibling
                let borrowed_child = right_sibling.children.remove(0);
                let borrowed_key = right_sibling.keys.remove(0);

                let mut updated_node = node.clone();
                updated_node.children.push(borrowed_child);
                updated_node.keys.push(parent.keys[idx].clone());

                // Update parent separator key
                parent.keys[idx] = borrowed_key;

                // Write nodes back
                self.write_internal_node(&right_sibling)?;
                self.write_internal_node(&updated_node)?;

                return Ok(true);
            }
        }

        Ok(false) // No sibling has enough entries to borrow
    }

    /// Merge underfull internal node with sibling
    fn merge_internal(
        &mut self,
        node: InternalNode,
        parent: &mut InternalNode,
        idx: usize,
    ) -> Result<(), StorageError> {
        // Prefer merging with left sibling
        if idx > 0 {
            let mut left_sibling = self.read_internal_node(parent.children[idx - 1])?;

            // Merge node into left sibling
            // Include the parent's separator key
            left_sibling.keys.push(parent.keys[idx - 1].clone());
            left_sibling.keys.extend(node.keys);
            left_sibling.children.extend(node.children);

            self.write_internal_node(&left_sibling)?;

            // Remove from parent
            parent.children.remove(idx);
            parent.keys.remove(idx - 1);

            // Deallocate merged node
            self.page_manager.deallocate_page(node.page_id)?;
        } else {
            // Merge with right sibling
            let right_sibling = self.read_internal_node(parent.children[idx + 1])?;

            // Merge right into node
            let mut updated_node = node.clone();
            updated_node.keys.push(parent.keys[idx].clone());
            updated_node.keys.extend(right_sibling.keys);
            updated_node.children.extend(right_sibling.children);

            self.write_internal_node(&updated_node)?;

            // Remove from parent
            parent.children.remove(idx + 1);
            parent.keys.remove(idx);

            // Deallocate merged node
            self.page_manager.deallocate_page(right_sibling.page_id)?;
        }

        Ok(())
    }

    /// Check if root should be collapsed (height decrease)
    pub(super) fn maybe_collapse_root(&mut self) -> Result<(), StorageError> {
        if self.height > 1 {
            let root = self.read_internal_node(self.root_page_id)?;
            if root.children.len() == 1 {
                // Collapse root
                let old_root_id = self.root_page_id;
                self.root_page_id = root.children[0];
                self.height -= 1;
                self.page_manager.deallocate_page(old_root_id)?;
                self.save_metadata()?;
            }
        }

        Ok(())
    }
}
