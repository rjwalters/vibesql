//! B+ Tree Index Implementation
//!
//! This module is organized into focused submodules:
//! - `bulk_load`: Efficient bottom-up tree construction from sorted data
//! - `insert`: Key insertion with split propagation
//! - `delete`: Key deletion with rebalancing
//! - `query`: Lookups, range scans, and multi-key queries
//! - `rebalance`: Tree rebalancing after deletions

use std::sync::Arc;
use vibesql_types::DataType;

use crate::page::{PageId, PageManager};
use crate::StorageError;

use super::super::calculate_degree;
use super::datatype_serialization::{deserialize_datatype, serialize_datatype};
use super::structure::{InternalNode, LeafNode};

// Submodules
mod bulk_load;
mod insert;
mod delete;
mod query;
mod rebalance;

/// B+ Tree Index with disk-backed storage
///
/// This structure provides a disk-backed B+ tree index that maintains
/// sorted key-value mappings with efficient range query support.
///
/// ## Non-Unique Index Support
///
/// This B+ tree fully supports duplicate keys for non-unique indexes. Multiple
/// rows can have the same key value, and all row IDs are stored efficiently
/// within the same B+ tree entry.
///
/// ## Storage Format
///
/// Leaf nodes store entries as `Vec<(Key, Vec<RowId>)>`, allowing multiple
/// row IDs per key. The serialization format per entry is:
/// ```text
/// key_len (2 bytes) → key_values (variable) → num_row_ids (2 bytes) → row_id × num_row_ids (8 bytes each)
/// ```
///
/// ## Performance Characteristics
///
/// - **Insert**: O(log n + k) where n = number of unique keys, k = duplicates for existing key
/// - **Lookup**: O(log n + k) where k = number of duplicates to return
/// - **Range Scan**: O(log n + m + k) where m = keys in range, k = total duplicates
/// - **Delete**: O(log n) to find key, removes all associated row IDs
/// - **Storage Overhead**: 2 bytes per unique key (for row count) + 8 bytes per row ID
#[derive(Debug)]
pub struct BTreeIndex {
    /// Page ID of the root node
    pub(super) root_page_id: PageId,
    /// Key schema (data types of key columns)
    pub(super) key_schema: Vec<DataType>,
    /// Maximum degree (fanout) of the tree
    pub(super) degree: usize,
    /// Height of the tree (0 = empty, 1 = root is leaf)
    pub(super) height: usize,
    /// Page manager for disk I/O
    pub(super) page_manager: Arc<PageManager>,
    /// Metadata page ID (always page 0)
    /// This field will be used when supporting multiple B+ trees with different metadata pages
    #[allow(dead_code)]
    pub(super) metadata_page_id: PageId,
}

impl BTreeIndex {
    /// Create a new B+ tree index
    ///
    /// Creates a new disk-backed B+ tree index that supports both unique and
    /// non-unique keys. For non-unique indexes, duplicate keys are automatically
    /// grouped together, with multiple row IDs stored per key.
    ///
    /// # Arguments
    /// * `page_manager` - Page manager for disk I/O
    /// * `key_schema` - Data types of key columns
    ///
    /// # Returns
    /// A new B+ tree index with an empty root
    ///
    /// # Example
    /// ```ignore
    /// use std::sync::Arc;
    /// use vibesql_storage::btree::BTreeIndex;
    /// use vibesql_storage::page::PageManager;
    /// use vibesql_types::DataType;
    ///
    /// let page_manager = Arc::new(PageManager::new_in_memory(4096)?);
    /// let index = BTreeIndex::new(page_manager, vec![DataType::Integer])?;
    /// ```
    pub fn new(
        page_manager: Arc<PageManager>,
        key_schema: Vec<DataType>,
    ) -> Result<Self, StorageError> {
        let degree = calculate_degree(&key_schema);
        let metadata_page_id = 0;

        // Allocate page for root node
        let root_page_id = page_manager.allocate_page()?;

        // Create empty root leaf node
        let root_leaf = LeafNode::new(root_page_id);

        let index = BTreeIndex {
            root_page_id,
            key_schema,
            degree,
            height: 1,
            page_manager,
            metadata_page_id,
        };

        // Write empty root to disk
        index.write_leaf_node(&root_leaf)?;

        // Save metadata
        index.save_metadata()?;

        Ok(index)
    }

    /// Load an existing B+ tree index from disk
    ///
    /// # Arguments
    /// * `page_manager` - Page manager for disk I/O
    ///
    /// # Returns
    /// Loaded B+ tree index
    pub fn load(page_manager: Arc<PageManager>) -> Result<Self, StorageError> {
        let metadata_page = page_manager.read_page(0)?;

        // Parse metadata
        let mut offset = 0;

        // Read root_page_id
        let root_page_id_bytes: [u8; 8] =
            metadata_page.data[offset..offset + 8].try_into().unwrap();
        let root_page_id = u64::from_le_bytes(root_page_id_bytes);
        offset += 8;

        // Read degree
        let degree_bytes: [u8; 2] = metadata_page.data[offset..offset + 2].try_into().unwrap();
        let degree = u16::from_le_bytes(degree_bytes) as usize;
        offset += 2;

        // Read height
        let height_bytes: [u8; 2] = metadata_page.data[offset..offset + 2].try_into().unwrap();
        let height = u16::from_le_bytes(height_bytes) as usize;
        offset += 2;

        // Read key_schema length
        let schema_len_bytes: [u8; 2] =
            metadata_page.data[offset..offset + 2].try_into().unwrap();
        let schema_len = u16::from_le_bytes(schema_len_bytes) as usize;
        offset += 2;

        // Read key_schema using deserialize_datatype
        let mut key_schema = Vec::new();
        for _ in 0..schema_len {
            let (data_type, bytes_read) = deserialize_datatype(&metadata_page.data[offset..])?;
            key_schema.push(data_type);
            offset += bytes_read;
        }

        Ok(BTreeIndex {
            root_page_id,
            key_schema,
            degree,
            height,
            page_manager,
            metadata_page_id: 0,
        })
    }

    /// Serialize metadata to a page (without writing to disk)
    fn serialize_metadata(&self) -> Result<crate::page::Page, StorageError> {
        let mut metadata_page = self.page_manager.read_page(0)?;
        let mut offset = 0;

        // Write root_page_id
        metadata_page.data[offset..offset + 8]
            .copy_from_slice(&self.root_page_id.to_le_bytes());
        offset += 8;

        // Write degree
        metadata_page.data[offset..offset + 2]
            .copy_from_slice(&(self.degree as u16).to_le_bytes());
        offset += 2;

        // Write height
        metadata_page.data[offset..offset + 2]
            .copy_from_slice(&(self.height as u16).to_le_bytes());
        offset += 2;

        // Write key_schema length
        metadata_page.data[offset..offset + 2]
            .copy_from_slice(&(self.key_schema.len() as u16).to_le_bytes());
        offset += 2;

        // Write key_schema using serialize_datatype
        for data_type in &self.key_schema {
            let bytes_written = serialize_datatype(data_type, &mut metadata_page.data[offset..])?;
            offset += bytes_written;
        }

        metadata_page.mark_dirty();
        Ok(metadata_page)
    }

    /// Save metadata to page 0 with immediate sync
    pub(super) fn save_metadata(&self) -> Result<(), StorageError> {
        let mut page = self.serialize_metadata()?;
        self.page_manager.write_page(&mut page)?;
        Ok(())
    }

    /// Save metadata to page 0 without syncing
    ///
    /// Use this for bulk operations. Call `page_manager.sync()` after all writes.
    pub(super) fn save_metadata_no_sync(&self) -> Result<(), StorageError> {
        let mut page = self.serialize_metadata()?;
        self.page_manager.write_page_no_sync(&mut page)?;
        Ok(())
    }

    /// Get the degree of this B+ tree
    pub fn degree(&self) -> usize {
        self.degree
    }

    /// Get the height of this B+ tree
    pub fn height(&self) -> usize {
        self.height
    }

    /// Get the root page ID
    pub fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    // Node I/O wrappers

    /// Write an internal node to disk
    ///
    /// This method will be used in Part 2b for tree-level operations
    #[allow(dead_code)]
    pub(crate) fn write_internal_node(&self, node: &InternalNode) -> Result<(), StorageError> {
        super::super::serialize::write_internal_node(&self.page_manager, node, self.degree)
    }

    pub(crate) fn write_leaf_node(&self, node: &LeafNode) -> Result<(), StorageError> {
        super::super::serialize::write_leaf_node(&self.page_manager, node, self.degree)
    }

    /// Read an internal node from disk
    ///
    /// This method will be used in Part 2b for tree-level operations
    #[allow(dead_code)]
    pub(crate) fn read_internal_node(&self, page_id: PageId) -> Result<InternalNode, StorageError> {
        super::super::serialize::read_internal_node(&self.page_manager, page_id)
    }

    /// Read a leaf node from disk
    ///
    /// This method will be used in Part 2b for tree-level operations
    #[allow(dead_code)]
    pub(crate) fn read_leaf_node(&self, page_id: PageId) -> Result<LeafNode, StorageError> {
        super::super::serialize::read_leaf_node(&self.page_manager, page_id)
    }
}
