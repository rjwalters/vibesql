//! B+ Tree Node Structures
//!
//! This module defines the core data structures for B+ tree nodes:
//! - Type aliases for keys and row IDs
//! - Internal node structure
//! - Leaf node structure

use crate::page::PageId;
use vibesql_types::SqlValue;

use super::super::NULL_PAGE_ID;

/// Type alias for multi-column keys (compatible with existing IndexData)
pub type Key = Vec<SqlValue>;

/// Type alias for row identifiers (row index in table)
pub type RowId = usize;

/// Internal node in the B+ tree
///
/// Internal nodes store keys and page IDs pointing to child nodes.
/// They do not store actual data values, only routing information.
#[derive(Debug, Clone)]
pub struct InternalNode {
    /// Page ID of this node
    pub page_id: PageId,
    /// Separator keys (length = num_children - 1)
    pub keys: Vec<Key>,
    /// Child page IDs (length = num_children)
    pub children: Vec<PageId>,
}

impl InternalNode {
    /// Create a new internal node
    pub fn new(page_id: PageId) -> Self {
        InternalNode { page_id, keys: Vec::new(), children: Vec::new() }
    }

    /// Check if the node is full (needs splitting)
    ///
    /// This method will be used in Part 2b for tree-level insert operations
    #[allow(dead_code)]
    pub fn is_full(&self, degree: usize) -> bool {
        self.children.len() >= degree
    }
}

/// Leaf node in the B+ tree
///
/// Leaf nodes store the actual key-value pairs (key -> Vec<row_id>).
/// Each key can map to multiple row IDs to support non-unique indexes.
/// They maintain a doubly-linked list structure via next_leaf and prev_leaf
/// for efficient forward and reverse range scans.
#[derive(Debug, Clone)]
pub struct LeafNode {
    /// Page ID of this node
    pub page_id: PageId,
    /// Key-value entries (sorted by key), supporting multiple row_ids per key for non-unique indexes
    pub entries: Vec<(Key, Vec<RowId>)>,
    /// Page ID of next leaf node (for forward range scans), or NULL_PAGE_ID
    pub next_leaf: PageId,
    /// Page ID of previous leaf node (for reverse range scans), or NULL_PAGE_ID
    pub prev_leaf: PageId,
}

impl LeafNode {
    /// Create a new leaf node
    pub fn new(page_id: PageId) -> Self {
        LeafNode { page_id, entries: Vec::new(), next_leaf: NULL_PAGE_ID, prev_leaf: NULL_PAGE_ID }
    }

    /// Check if the node is full (needs splitting)
    ///
    /// This method will be used in Part 2b for tree-level insert operations
    #[allow(dead_code)]
    pub fn is_full(&self, degree: usize) -> bool {
        self.entries.len() >= degree
    }

    /// Check if node is underfull (needs merging)
    ///
    /// This method will be used in Part 2b for tree-level delete operations
    #[allow(dead_code)]
    pub fn is_underfull(&self, degree: usize) -> bool {
        self.entries.len() < degree / 2
    }
}
