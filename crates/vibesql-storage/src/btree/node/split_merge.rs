//! B+ Tree Node Splitting and Merging
//!
//! This module implements node restructuring operations:
//! - Node splitting (when nodes become too full)
//! - Node merging (for future use in delete operations)
//! - Rebalancing algorithms

use super::structure::{InternalNode, Key, LeafNode};

impl InternalNode {
    /// Split this internal node into two nodes
    ///
    /// Returns (middle_key, new_right_node)
    pub fn split(&mut self, new_page_id: u64) -> (Key, InternalNode) {
        let mid = self.keys.len() / 2;

        // Middle key moves up to parent
        let middle_key = self.keys[mid].clone();

        // Create right node with upper half of keys and children
        let mut right_node = InternalNode::new(new_page_id);
        right_node.keys = self.keys.split_off(mid + 1);
        right_node.children = self.children.split_off(mid + 1);

        // Remove middle key from left node
        self.keys.pop();

        (middle_key, right_node)
    }
}

impl LeafNode {
    /// Split this leaf node into two nodes
    ///
    /// Returns (middle_key, new_right_node)
    ///
    /// Note: This function maintains the doubly-linked list by setting both
    /// next_leaf and prev_leaf pointers. However, if self.next_leaf was pointing
    /// to another node before the split, that node's prev_leaf needs to be updated
    /// separately (by the caller) to point to the new right node.
    pub fn split(&mut self, new_page_id: u64) -> (Key, LeafNode) {
        let mid = self.entries.len() / 2;

        // Create right node with upper half of entries
        let mut right_node = LeafNode::new(new_page_id);
        right_node.entries = self.entries.split_off(mid);

        // Update doubly-linked list pointers
        // Right node's next points to what left's next was
        right_node.next_leaf = self.next_leaf;
        // Right node's prev points back to left node
        right_node.prev_leaf = self.page_id;
        // Left's next now points to the new right node
        self.next_leaf = new_page_id;
        // Left's prev remains unchanged (still points to its original predecessor)

        // Middle key is the first key of right node (copy, not move)
        let middle_key = right_node.entries[0].0.clone();

        (middle_key, right_node)
    }
}
