//! B+ Tree Node Operations
//!
//! This module implements basic node operations:
//! - Key search and traversal
//! - Insertion and deletion
//! - Key/value manipulation

use crate::page::PageId;

use super::structure::{InternalNode, Key, LeafNode, RowId};

impl InternalNode {
    /// Find the child index for a given key
    ///
    /// Returns the index of the child that should contain the key
    pub fn find_child_index(&self, key: &Key) -> usize {
        // Binary search to find the appropriate child
        match self.keys.binary_search(key) {
            Ok(idx) => idx + 1, // Key found, go to right child
            Err(idx) => idx,    // Key not found, idx is the insertion point
        }
    }

    /// Insert a key and child into this internal node
    ///
    /// Assumes the node is not full (caller should check)
    pub fn insert_child(&mut self, key: Key, child_page_id: PageId) {
        // Find insertion point
        let idx = match self.keys.binary_search(&key) {
            Ok(idx) | Err(idx) => idx,
        };

        // Insert key and child
        self.keys.insert(idx, key);
        self.children.insert(idx + 1, child_page_id);
    }
}

impl LeafNode {
    /// Insert a key-value pair into this leaf node
    ///
    /// Always succeeds, appending the row_id to the list for the given key.
    /// Supports duplicate keys for non-unique indexes.
    pub fn insert(&mut self, key: Key, row_id: RowId) -> bool {
        match self.entries.binary_search_by_key(&&key, |(k, _)| k) {
            Ok(idx) => {
                // Key exists, append row_id to the existing Vec
                self.entries[idx].1.push(row_id);
                true
            }
            Err(idx) => {
                // Key doesn't exist, insert new entry with Vec containing single row_id
                self.entries.insert(idx, (key, vec![row_id]));
                true
            }
        }
    }

    /// Search for a key in this leaf node
    ///
    /// Returns a reference to the Vec of row_ids if found
    #[allow(dead_code)]
    pub fn search(&self, key: &Key) -> Option<&Vec<RowId>> {
        self.entries.binary_search_by_key(&key, |(k, _)| k).ok().map(|idx| &self.entries[idx].1)
    }

    /// Delete a specific row_id for a key from this leaf node
    ///
    /// If this was the last row_id for the key, removes the key entirely.
    /// Returns true if the row_id was found and deleted, false otherwise.
    #[allow(dead_code)]
    pub fn delete(&mut self, key: &Key, row_id: RowId) -> bool {
        match self.entries.binary_search_by_key(&key, |(k, _)| k) {
            Ok(idx) => {
                let row_ids = &mut self.entries[idx].1;
                if let Some(pos) = row_ids.iter().position(|&id| id == row_id) {
                    row_ids.remove(pos);
                    // If this was the last row_id, remove the key entry entirely
                    if row_ids.is_empty() {
                        self.entries.remove(idx);
                    }
                    true
                } else {
                    false // row_id not found for this key
                }
            }
            Err(_) => false, // Key not found
        }
    }

    /// Delete all row_ids for a key from this leaf node
    ///
    /// Returns true if the key was found and deleted, false otherwise.
    pub fn delete_all(&mut self, key: &Key) -> bool {
        match self.entries.binary_search_by_key(&key, |(k, _)| k) {
            Ok(idx) => {
                self.entries.remove(idx);
                true
            }
            Err(_) => false,
        }
    }
}
