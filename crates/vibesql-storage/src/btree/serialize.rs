//! Serialization and Deserialization for B+ Tree Nodes
//!
//! This module handles converting B+ tree nodes to/from disk pages.
//! It reuses the existing SqlValue serialization from persistence/binary/value.rs

use std::io::{Cursor, Write};
use std::sync::Arc;

use crate::page::{Page, PageId, PageManager, PAGE_SIZE};
use crate::persistence::binary::value::{read_sql_value, write_sql_value};
use crate::StorageError;

use super::node::{InternalNode, LeafNode};
use super::{PAGE_TYPE_INTERNAL, PAGE_TYPE_LEAF};

/// Write a variable-length encoded unsigned integer
/// Uses MSB-based encoding: 0xxxxxxx = single byte, 1xxxxxxx = more bytes follow
/// This reduces storage for small values (≤127) from 2 bytes to 1 byte
fn write_varint(cursor: &mut Cursor<&mut [u8]>, mut value: usize) -> Result<(), StorageError> {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80; // Set MSB to indicate more bytes follow
        }

        cursor
            .write_all(&[byte])
            .map_err(|e| StorageError::IoError(format!("Failed to write varint: {}", e)))?;

        if value == 0 {
            break;
        }
    }
    Ok(())
}

/// Read a variable-length encoded unsigned integer
fn read_varint(cursor: &mut Cursor<&[u8]>) -> Result<usize, StorageError> {
    let mut value = 0usize;
    let mut shift = 0;

    loop {
        let mut byte = [0u8; 1];
        std::io::Read::read_exact(cursor, &mut byte)
            .map_err(|e| StorageError::IoError(format!("Failed to read varint: {}", e)))?;

        let byte = byte[0];
        value |= ((byte & 0x7F) as usize) << shift;

        if byte & 0x80 == 0 {
            break; // MSB not set, this is the last byte
        }

        shift += 7;

        // Prevent overflow - usize can hold at most 9 bytes (for 64-bit) or 5 bytes (for 32-bit)
        if shift >= std::mem::size_of::<usize>() * 8 {
            return Err(StorageError::IoError("Varint overflow: value too large".to_string()));
        }
    }

    Ok(value)
}

/// Serialize an internal node to a page (without writing to disk)
fn serialize_internal_node(node: &InternalNode) -> Result<Page, StorageError> {
    let mut page = Page::new(node.page_id);
    let mut cursor = Cursor::new(&mut page.data[..]);

    // Write page type
    cursor
        .write_all(&[PAGE_TYPE_INTERNAL])
        .map_err(|e| StorageError::IoError(format!("Failed to write page type: {}", e)))?;

    // Write number of keys
    let num_keys = node.keys.len() as u16;
    cursor
        .write_all(&num_keys.to_le_bytes())
        .map_err(|e| StorageError::IoError(format!("Failed to write num_keys: {}", e)))?;

    // Write keys
    for key in &node.keys {
        // Write key length (number of SqlValues in the key)
        let key_len = key.len() as u16;
        cursor
            .write_all(&key_len.to_le_bytes())
            .map_err(|e| StorageError::IoError(format!("Failed to write key_len: {}", e)))?;

        // Write each SqlValue in the key
        for value in key {
            write_sql_value(&mut cursor, value)?;
        }
    }

    // Write children page IDs
    for &child_page_id in &node.children {
        cursor
            .write_all(&child_page_id.to_le_bytes())
            .map_err(|e| StorageError::IoError(format!("Failed to write child_page_id: {}", e)))?;
    }

    // Check if we exceeded page size
    let bytes_written = cursor.position() as usize;
    if bytes_written > PAGE_SIZE {
        return Err(StorageError::IoError(format!(
            "Internal node exceeds page size: {} > {}",
            bytes_written, PAGE_SIZE
        )));
    }

    page.mark_dirty();
    Ok(page)
}

/// Write an internal node to disk with immediate sync
pub fn write_internal_node(
    page_manager: &Arc<PageManager>,
    node: &InternalNode,
    _degree: usize,
) -> Result<(), StorageError> {
    let mut page = serialize_internal_node(node)?;
    page_manager.write_page(&mut page)?;
    Ok(())
}

/// Write an internal node to disk without syncing
///
/// Use this for bulk operations. Call `page_manager.sync()` after all writes.
pub fn write_internal_node_no_sync(
    page_manager: &Arc<PageManager>,
    node: &InternalNode,
    _degree: usize,
) -> Result<(), StorageError> {
    let mut page = serialize_internal_node(node)?;
    page_manager.write_page_no_sync(&mut page)?;
    Ok(())
}

/// Read an internal node from disk
pub fn read_internal_node(
    page_manager: &Arc<PageManager>,
    page_id: PageId,
) -> Result<InternalNode, StorageError> {
    let page = page_manager.read_page(page_id)?;
    let mut cursor = Cursor::new(&page.data[..]);

    // Read and verify page type
    let mut page_type = [0u8; 1];
    std::io::Read::read_exact(&mut cursor, &mut page_type)
        .map_err(|e| StorageError::IoError(format!("Failed to read page type: {}", e)))?;

    if page_type[0] != PAGE_TYPE_INTERNAL {
        return Err(StorageError::IoError(format!(
            "Expected internal node, got page type {}",
            page_type[0]
        )));
    }

    // Read number of keys
    let mut num_keys_bytes = [0u8; 2];
    std::io::Read::read_exact(&mut cursor, &mut num_keys_bytes)
        .map_err(|e| StorageError::IoError(format!("Failed to read num_keys: {}", e)))?;
    let num_keys = u16::from_le_bytes(num_keys_bytes) as usize;

    let mut node = InternalNode::new(page_id);

    // Read keys
    for _ in 0..num_keys {
        // Read key length
        let mut key_len_bytes = [0u8; 2];
        std::io::Read::read_exact(&mut cursor, &mut key_len_bytes)
            .map_err(|e| StorageError::IoError(format!("Failed to read key_len: {}", e)))?;
        let key_len = u16::from_le_bytes(key_len_bytes) as usize;

        // Read each SqlValue in the key
        let mut key = Vec::with_capacity(key_len);
        for _ in 0..key_len {
            let value = read_sql_value(&mut cursor)?;
            key.push(value);
        }
        node.keys.push(key);
    }

    // Read children page IDs (num_keys + 1 children)
    for _ in 0..=num_keys {
        let mut page_id_bytes = [0u8; 8];
        std::io::Read::read_exact(&mut cursor, &mut page_id_bytes)
            .map_err(|e| StorageError::IoError(format!("Failed to read child_page_id: {}", e)))?;
        let child_page_id = u64::from_le_bytes(page_id_bytes);
        node.children.push(child_page_id);
    }

    Ok(node)
}

/// Serialize a leaf node to a page (without writing to disk)
fn serialize_leaf_node(node: &LeafNode) -> Result<Page, StorageError> {
    let mut page = Page::new(node.page_id);
    let mut cursor = Cursor::new(&mut page.data[..]);

    // Write page type
    cursor
        .write_all(&[PAGE_TYPE_LEAF])
        .map_err(|e| StorageError::IoError(format!("Failed to write page type: {}", e)))?;

    // Write number of entries
    let num_entries = node.entries.len() as u16;
    cursor
        .write_all(&num_entries.to_le_bytes())
        .map_err(|e| StorageError::IoError(format!("Failed to write num_entries: {}", e)))?;

    // Write entries
    for (key, row_ids) in &node.entries {
        // Write key length
        let key_len = key.len() as u16;
        cursor
            .write_all(&key_len.to_le_bytes())
            .map_err(|e| StorageError::IoError(format!("Failed to write key_len: {}", e)))?;

        // Write each SqlValue in the key
        for value in key {
            write_sql_value(&mut cursor, value)?;
        }

        // Write number of row_ids for this key using varint encoding
        write_varint(&mut cursor, row_ids.len())?;

        // Write each row_id
        for &row_id in row_ids {
            cursor
                .write_all(&(row_id as u64).to_le_bytes())
                .map_err(|e| StorageError::IoError(format!("Failed to write row_id: {}", e)))?;
        }
    }

    // Write next_leaf pointer
    cursor
        .write_all(&node.next_leaf.to_le_bytes())
        .map_err(|e| StorageError::IoError(format!("Failed to write next_leaf: {}", e)))?;

    // Write prev_leaf pointer
    cursor
        .write_all(&node.prev_leaf.to_le_bytes())
        .map_err(|e| StorageError::IoError(format!("Failed to write prev_leaf: {}", e)))?;

    // Check if we exceeded page size
    let bytes_written = cursor.position() as usize;
    if bytes_written > PAGE_SIZE {
        return Err(StorageError::IoError(format!(
            "Leaf node exceeds page size: {} > {}",
            bytes_written, PAGE_SIZE
        )));
    }

    page.mark_dirty();
    Ok(page)
}

/// Write a leaf node to disk with immediate sync
pub fn write_leaf_node(
    page_manager: &Arc<PageManager>,
    node: &LeafNode,
    _degree: usize,
) -> Result<(), StorageError> {
    let mut page = serialize_leaf_node(node)?;
    page_manager.write_page(&mut page)?;
    Ok(())
}

/// Write a leaf node to disk without syncing
///
/// Use this for bulk operations. Call `page_manager.sync()` after all writes.
pub fn write_leaf_node_no_sync(
    page_manager: &Arc<PageManager>,
    node: &LeafNode,
    _degree: usize,
) -> Result<(), StorageError> {
    let mut page = serialize_leaf_node(node)?;
    page_manager.write_page_no_sync(&mut page)?;
    Ok(())
}

/// Read a leaf node from disk
pub fn read_leaf_node(
    page_manager: &Arc<PageManager>,
    page_id: PageId,
) -> Result<LeafNode, StorageError> {
    let page = page_manager.read_page(page_id)?;
    let mut cursor = Cursor::new(&page.data[..]);

    // Read and verify page type
    let mut page_type = [0u8; 1];
    std::io::Read::read_exact(&mut cursor, &mut page_type)
        .map_err(|e| StorageError::IoError(format!("Failed to read page type: {}", e)))?;

    if page_type[0] != PAGE_TYPE_LEAF {
        return Err(StorageError::IoError(format!(
            "Expected leaf node, got page type {}",
            page_type[0]
        )));
    }

    // Read number of entries
    let mut num_entries_bytes = [0u8; 2];
    std::io::Read::read_exact(&mut cursor, &mut num_entries_bytes)
        .map_err(|e| StorageError::IoError(format!("Failed to read num_entries: {}", e)))?;
    let num_entries = u16::from_le_bytes(num_entries_bytes) as usize;

    let mut node = LeafNode::new(page_id);

    // Read entries
    for _ in 0..num_entries {
        // Read key length
        let mut key_len_bytes = [0u8; 2];
        std::io::Read::read_exact(&mut cursor, &mut key_len_bytes)
            .map_err(|e| StorageError::IoError(format!("Failed to read key_len: {}", e)))?;
        let key_len = u16::from_le_bytes(key_len_bytes) as usize;

        // Read each SqlValue in the key
        let mut key = Vec::with_capacity(key_len);
        for _ in 0..key_len {
            let value = read_sql_value(&mut cursor)?;
            key.push(value);
        }

        // Read number of row_ids for this key using varint decoding
        let num_row_ids = read_varint(&mut cursor)?;

        // Read each row_id
        let mut row_ids = Vec::with_capacity(num_row_ids);
        for _ in 0..num_row_ids {
            let mut row_id_bytes = [0u8; 8];
            std::io::Read::read_exact(&mut cursor, &mut row_id_bytes)
                .map_err(|e| StorageError::IoError(format!("Failed to read row_id: {}", e)))?;
            let row_id = u64::from_le_bytes(row_id_bytes) as usize;
            row_ids.push(row_id);
        }

        node.entries.push((key, row_ids));
    }

    // Read next_leaf pointer
    let mut next_leaf_bytes = [0u8; 8];
    std::io::Read::read_exact(&mut cursor, &mut next_leaf_bytes)
        .map_err(|e| StorageError::IoError(format!("Failed to read next_leaf: {}", e)))?;
    node.next_leaf = u64::from_le_bytes(next_leaf_bytes);

    // Read prev_leaf pointer
    let mut prev_leaf_bytes = [0u8; 8];
    std::io::Read::read_exact(&mut cursor, &mut prev_leaf_bytes)
        .map_err(|e| StorageError::IoError(format!("Failed to read prev_leaf: {}", e)))?;
    node.prev_leaf = u64::from_le_bytes(prev_leaf_bytes);

    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageManager;
    use tempfile::TempDir;
    use vibesql_types::SqlValue;

    #[test]
    fn test_serialize_deserialize_internal_node() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

        let page_id = page_manager.allocate_page().unwrap();

        // Create internal node
        let mut node = InternalNode::new(page_id);
        node.keys = vec![
            vec![SqlValue::Integer(10)],
            vec![SqlValue::Integer(20)],
            vec![SqlValue::Integer(30)],
        ];
        node.children = vec![2, 3, 4, 5];

        // Write node
        write_internal_node(&page_manager, &node, 100).unwrap();

        // Read node back
        let loaded_node = read_internal_node(&page_manager, page_id).unwrap();

        // Verify
        assert_eq!(loaded_node.page_id, page_id);
        assert_eq!(loaded_node.keys.len(), 3);
        assert_eq!(loaded_node.children.len(), 4);
        assert_eq!(loaded_node.keys[0][0], SqlValue::Integer(10));
        assert_eq!(loaded_node.keys[1][0], SqlValue::Integer(20));
        assert_eq!(loaded_node.keys[2][0], SqlValue::Integer(30));
        assert_eq!(loaded_node.children, vec![2, 3, 4, 5]);
    }

    #[test]
    fn test_serialize_deserialize_leaf_node() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

        let page_id = page_manager.allocate_page().unwrap();

        // Create leaf node
        let mut node = LeafNode::new(page_id);
        node.entries = vec![
            (vec![SqlValue::Integer(5)], vec![100]),
            (vec![SqlValue::Integer(10)], vec![200]),
            (vec![SqlValue::Integer(15)], vec![300]),
        ];
        node.next_leaf = 42;

        // Write node
        write_leaf_node(&page_manager, &node, 100).unwrap();

        // Read node back
        let loaded_node = read_leaf_node(&page_manager, page_id).unwrap();

        // Verify
        assert_eq!(loaded_node.page_id, page_id);
        assert_eq!(loaded_node.entries.len(), 3);
        assert_eq!(loaded_node.entries[0], (vec![SqlValue::Integer(5)], vec![100]));
        assert_eq!(loaded_node.entries[1], (vec![SqlValue::Integer(10)], vec![200]));
        assert_eq!(loaded_node.entries[2], (vec![SqlValue::Integer(15)], vec![300]));
        assert_eq!(loaded_node.next_leaf, 42);
    }

    #[test]
    fn test_serialize_multi_column_key() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

        let page_id = page_manager.allocate_page().unwrap();

        // Create leaf node with multi-column keys
        let mut node = LeafNode::new(page_id);
        node.entries = vec![
            (vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))], vec![100]),
            (vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))], vec![200]),
        ];
        node.next_leaf = 0;

        // Write node
        write_leaf_node(&page_manager, &node, 100).unwrap();

        // Read node back
        let loaded_node = read_leaf_node(&page_manager, page_id).unwrap();

        // Verify
        assert_eq!(loaded_node.entries.len(), 2);
        assert_eq!(
            loaded_node.entries[0].0,
            vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]
        );
        assert_eq!(
            loaded_node.entries[1].0,
            vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]
        );
    }

    #[test]
    fn test_varint_encoding_edge_cases() {
        // Test varint encoding for edge case values
        let test_cases = vec![
            (0, vec![0x00]),                 // Minimum value
            (1, vec![0x01]),                 // Small value
            (127, vec![0x7F]),               // Maximum single-byte value (0xxxxxxx)
            (128, vec![0x80, 0x01]),         // Minimum two-byte value
            (255, vec![0xFF, 0x01]),         // Two-byte value
            (16383, vec![0xFF, 0x7F]),       // Maximum two-byte value
            (16384, vec![0x80, 0x80, 0x01]), // Minimum three-byte value
        ];

        for (value, expected_bytes) in test_cases {
            // Test encoding
            let mut buffer = [0u8; 10];
            let mut cursor = Cursor::new(&mut buffer[..]);
            write_varint(&mut cursor, value).unwrap();

            let bytes_written = cursor.position() as usize;
            assert_eq!(
                &buffer[..bytes_written],
                &expected_bytes[..],
                "Encoding failed for value {}",
                value
            );

            // Test decoding
            let mut cursor = Cursor::new(&buffer[..]);
            let decoded = read_varint(&mut cursor).unwrap();
            assert_eq!(decoded, value, "Decoding failed for value {}", value);
        }
    }

    #[test]
    fn test_leaf_node_with_duplicate_keys() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

        let page_id = page_manager.allocate_page().unwrap();

        // Create leaf node with duplicate keys (multiple row_ids per key)
        let mut node = LeafNode::new(page_id);
        node.entries = vec![
            (vec![SqlValue::Integer(5)], vec![100, 101, 102]), // 3 duplicates
            (vec![SqlValue::Integer(10)], vec![200]),          // 1 row_id
            (vec![SqlValue::Integer(15)], vec![300, 301]),     // 2 duplicates
        ];
        node.next_leaf = 0;

        // Write node
        write_leaf_node(&page_manager, &node, 100).unwrap();

        // Read node back
        let loaded_node = read_leaf_node(&page_manager, page_id).unwrap();

        // Verify
        assert_eq!(loaded_node.entries.len(), 3);
        assert_eq!(loaded_node.entries[0], (vec![SqlValue::Integer(5)], vec![100, 101, 102]));
        assert_eq!(loaded_node.entries[1], (vec![SqlValue::Integer(10)], vec![200]));
        assert_eq!(loaded_node.entries[2], (vec![SqlValue::Integer(15)], vec![300, 301]));
    }

    #[test]
    fn test_varint_reduces_storage_overhead() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

        // Create a node with many single row_id entries (common case)
        let mut node = LeafNode::new(page_manager.allocate_page().unwrap());
        for i in 0..100 {
            node.entries.push((vec![SqlValue::Integer(i)], vec![i as usize]));
        }
        node.next_leaf = 0;

        // Write and verify it doesn't exceed page size
        let result = write_leaf_node(&page_manager, &node, 100);
        assert!(
            result.is_ok(),
            "Node with 100 single-row_id entries should fit in a page with varint encoding"
        );
    }
}
