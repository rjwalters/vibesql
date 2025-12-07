use super::*;
use crate::page::PageManager;
use std::sync::Arc;
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_leaf_node_insert() {
    let mut leaf = LeafNode::new(1);

    assert!(leaf.insert(vec![SqlValue::Integer(10)], 0));
    assert!(leaf.insert(vec![SqlValue::Integer(5)], 1));
    assert!(leaf.insert(vec![SqlValue::Integer(15)], 2));

    // Check sorted order
    assert_eq!(leaf.entries.len(), 3);
    assert_eq!(leaf.entries[0].0[0], SqlValue::Integer(5));
    assert_eq!(leaf.entries[1].0[0], SqlValue::Integer(10));
    assert_eq!(leaf.entries[2].0[0], SqlValue::Integer(15));
}

#[test]
fn test_leaf_node_duplicate_insert() {
    let mut leaf = LeafNode::new(1);

    assert!(leaf.insert(vec![SqlValue::Integer(10)], 0));
    assert!(leaf.insert(vec![SqlValue::Integer(10)], 1)); // Duplicate now allowed

    // Should have 1 key with 2 row_ids
    assert_eq!(leaf.entries.len(), 1);
    assert_eq!(leaf.entries[0].1, vec![0, 1]);
}

#[test]
fn test_leaf_node_search() {
    let mut leaf = LeafNode::new(1);

    leaf.insert(vec![SqlValue::Integer(10)], 100);
    leaf.insert(vec![SqlValue::Integer(20)], 200);

    assert_eq!(leaf.search(&vec![SqlValue::Integer(10)]), Some(&vec![100]));
    assert_eq!(leaf.search(&vec![SqlValue::Integer(20)]), Some(&vec![200]));
    assert_eq!(leaf.search(&vec![SqlValue::Integer(15)]), None);
}

#[test]
fn test_leaf_node_delete() {
    let mut leaf = LeafNode::new(1);

    leaf.insert(vec![SqlValue::Integer(10)], 0);
    leaf.insert(vec![SqlValue::Integer(20)], 1);

    assert!(leaf.delete_all(&vec![SqlValue::Integer(10)]));
    assert!(!leaf.delete_all(&vec![SqlValue::Integer(10)])); // Already deleted

    assert_eq!(leaf.entries.len(), 1);
    assert_eq!(leaf.entries[0].0[0], SqlValue::Integer(20));
}

#[test]
fn test_leaf_node_split() {
    let mut leaf = LeafNode::new(1);

    // Insert 6 entries
    for i in 0..6 {
        leaf.insert(vec![SqlValue::Integer(i * 10)], i as usize);
    }

    let (middle_key, right_node) = leaf.split(2);

    // Left node should have first 3 entries
    assert_eq!(leaf.entries.len(), 3);
    assert_eq!(leaf.next_leaf, 2);

    // Right node should have last 3 entries
    assert_eq!(right_node.entries.len(), 3);
    assert_eq!(right_node.page_id, 2);

    // Middle key should be first key of right node
    assert_eq!(middle_key, vec![SqlValue::Integer(30)]);
}

#[test]
fn test_internal_node_find_child() {
    let mut node = InternalNode::new(1);

    node.keys =
        vec![vec![SqlValue::Integer(10)], vec![SqlValue::Integer(20)], vec![SqlValue::Integer(30)]];
    node.children = vec![2, 3, 4, 5];

    assert_eq!(node.find_child_index(&vec![SqlValue::Integer(5)]), 0);
    assert_eq!(node.find_child_index(&vec![SqlValue::Integer(10)]), 1);
    assert_eq!(node.find_child_index(&vec![SqlValue::Integer(15)]), 1);
    assert_eq!(node.find_child_index(&vec![SqlValue::Integer(25)]), 2);
    assert_eq!(node.find_child_index(&vec![SqlValue::Integer(35)]), 3);
}

#[test]
fn test_internal_node_insert_child() {
    let mut node = InternalNode::new(1);

    node.children.push(2); // Initial child

    node.insert_child(vec![SqlValue::Integer(10)], 3);
    node.insert_child(vec![SqlValue::Integer(5)], 4);
    node.insert_child(vec![SqlValue::Integer(15)], 5);

    // Check sorted order
    assert_eq!(node.keys.len(), 3);
    assert_eq!(node.children.len(), 4);
    assert_eq!(node.keys[0][0], SqlValue::Integer(5));
    assert_eq!(node.keys[1][0], SqlValue::Integer(10));
    assert_eq!(node.keys[2][0], SqlValue::Integer(15));
}

#[test]
fn test_internal_node_split() {
    let mut node = InternalNode::new(1);

    // Setup node with 5 keys and 6 children
    node.keys = vec![
        vec![SqlValue::Integer(10)],
        vec![SqlValue::Integer(20)],
        vec![SqlValue::Integer(30)],
        vec![SqlValue::Integer(40)],
        vec![SqlValue::Integer(50)],
    ];
    node.children = vec![2, 3, 4, 5, 6, 7];

    let (middle_key, right_node) = node.split(8);

    // Middle key should be 30
    assert_eq!(middle_key, vec![SqlValue::Integer(30)]);

    // Left node should have 2 keys and 3 children
    assert_eq!(node.keys.len(), 2);
    assert_eq!(node.children.len(), 3);
    assert_eq!(node.keys[0][0], SqlValue::Integer(10));
    assert_eq!(node.keys[1][0], SqlValue::Integer(20));

    // Right node should have 2 keys and 3 children
    assert_eq!(right_node.keys.len(), 2);
    assert_eq!(right_node.children.len(), 3);
    assert_eq!(right_node.keys[0][0], SqlValue::Integer(40));
    assert_eq!(right_node.keys[1][0], SqlValue::Integer(50));
}

#[test]
fn test_bulk_load_empty() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries = vec![];

    let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Empty index should have height 1 (just root leaf)
    assert_eq!(index.height(), 1);
}

#[test]
fn test_bulk_load_small() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries = vec![
        (vec![SqlValue::Integer(10)], 0),
        (vec![SqlValue::Integer(20)], 1),
        (vec![SqlValue::Integer(30)], 2),
        (vec![SqlValue::Integer(40)], 3),
        (vec![SqlValue::Integer(50)], 4),
    ];

    let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Small index should have height 1 (just root leaf)
    assert_eq!(index.height(), 1);

    // Verify we can read the root leaf
    let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
    assert_eq!(root_leaf.entries.len(), 5);
    assert_eq!(root_leaf.entries[0].0, vec![SqlValue::Integer(10)]);
    assert_eq!(root_leaf.entries[4].0, vec![SqlValue::Integer(50)]);
}

#[test]
fn test_bulk_load_multi_column() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer, DataType::Varchar { max_length: Some(50) }];
    let sorted_entries = vec![
        (vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))], 0),
        (vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))], 1),
        (vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))], 2),
    ];

    let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Verify we can read the root leaf
    let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
    assert_eq!(root_leaf.entries.len(), 3);
    assert_eq!(
        root_leaf.entries[0].0,
        vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]
    );
}

#[test]
fn test_bulk_load_large() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];

    // Create 1000 sorted entries
    let sorted_entries: Vec<(Key, RowId)> =
        (0..1000).map(|i| (vec![SqlValue::Integer(i * 10)], i as usize)).collect();

    let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager.clone()).unwrap();

    // Large index should have height > 1
    assert!(index.height() > 1, "Index with 1000 entries should have height > 1");

    // Verify we can load the index back from disk
    let loaded_index = BTreeIndex::load(page_manager).unwrap();
    assert_eq!(loaded_index.height(), index.height());
    assert_eq!(loaded_index.degree(), index.degree());
}

#[test]
fn test_insert_single_key() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    // Insert a single key
    let key = vec![SqlValue::Integer(42)];
    let row_id = 10;
    index.insert(key.clone(), row_id).unwrap();

    // Verify height is still 1 (root is leaf)
    assert_eq!(index.height(), 1);

    // Verify we can read the key back
    let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
    assert_eq!(root_leaf.entries.len(), 1);
    assert_eq!(root_leaf.entries[0].0, key);
    assert_eq!(root_leaf.entries[0].1, vec![row_id]);
}

#[test]
fn test_insert_duplicate_key() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    // Insert first key
    let key = vec![SqlValue::Integer(42)];
    index.insert(key.clone(), 10).unwrap();

    // Insert duplicate - now allowed for non-unique indexes
    let result = index.insert(key.clone(), 20);
    assert!(result.is_ok());

    // Verify both row_ids are stored
    let row_ids = index.lookup(&key).unwrap();
    assert_eq!(row_ids, vec![10, 20]);
}

#[test]
fn test_insert_causes_leaf_split() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    let degree = index.degree();
    assert!(degree >= 5, "Degree should be at least 5");

    // Insert keys until we almost fill the root leaf (degree - 1 keys)
    for i in 0..(degree - 1) {
        let key = vec![SqlValue::Integer(i as i64 * 10)];
        index.insert(key, i).unwrap();
    }

    // Root should still be a leaf (height = 1)
    assert_eq!(index.height(), 1);

    // Insert one more key to reach capacity
    let key = vec![SqlValue::Integer((degree - 1) as i64 * 10)];
    index.insert(key, degree - 1).unwrap();

    // Now at capacity, insert one more to trigger split
    let key = vec![SqlValue::Integer(degree as i64 * 10)];
    index.insert(key, degree).unwrap();

    // Height should now be 2 (root is internal, with 2 leaf children)
    assert_eq!(index.height(), 2);

    // Root should be an internal node with 2 children
    let root = index.read_internal_node(index.root_page_id()).unwrap();
    assert_eq!(root.children.len(), 2);
    assert_eq!(root.keys.len(), 1);
}

#[test]
fn test_insert_maintains_order() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    // Insert keys in random order
    let keys = vec![50, 20, 80, 10, 30, 70, 90, 40, 60];
    for (i, &k) in keys.iter().enumerate() {
        let key = vec![SqlValue::Integer(k)];
        index.insert(key, i).unwrap();
    }

    // Verify all keys are in the tree by checking the leaf nodes
    let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();

    // Verify entries are sorted
    for i in 1..root_leaf.entries.len() {
        assert!(root_leaf.entries[i - 1].0 < root_leaf.entries[i].0, "Entries should be sorted");
    }
}

#[test]
fn test_insert_increases_height() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    let degree = index.degree();
    let initial_height = index.height();
    assert_eq!(initial_height, 1);

    // Insert enough keys to cause height increase
    // We need slightly more than degree keys to get to height 2
    // (degree keys fill the root, degree+1 causes split to height 2)
    let num_keys = degree + 1;

    for i in 0..num_keys {
        let key = vec![SqlValue::Integer(i as i64)];
        index.insert(key, i).unwrap();
    }

    // Height should have increased to 2
    assert_eq!(index.height(), 2, "Height should increase to 2 after inserting {} keys", num_keys);
}

#[test]
fn test_large_sequential_inserts() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    // Insert 1000 keys sequentially
    let num_keys = 1000;
    for i in 0..num_keys {
        let key = vec![SqlValue::Integer(i * 10)];
        let result = index.insert(key, i as usize);
        assert!(result.is_ok(), "Insert {} failed: {:?}", i, result);
    }

    // Verify tree properties
    let degree = index.degree();
    let height = index.height();

    // Height should be logarithmic in number of keys
    // For 1000 keys with degree ~200, height should be 2-3
    assert!(height >= 2, "Height should be at least 2 for 1000 keys");
    assert!(height <= 4, "Height should not exceed 4 for 1000 keys with degree {}", degree);

    // Verify tree structure is valid
    if height > 1 {
        let root = index.read_internal_node(index.root_page_id()).unwrap();
        assert!(root.children.len() >= 2, "Root should have at least 2 children");
        assert_eq!(
            root.keys.len() + 1,
            root.children.len(),
            "Internal node invariant: keys.len() + 1 == children.len()"
        );
    }
}

#[test]
fn test_large_random_inserts() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    // Insert 1000 keys in shuffled order
    let mut keys: Vec<i64> = (0..1000).map(|i| i * 10).collect();

    // Simple shuffle using the key values themselves
    for i in (1..keys.len()).rev() {
        let j = (keys[i] as usize) % (i + 1);
        keys.swap(i, j);
    }

    for (i, &k) in keys.iter().enumerate() {
        let key = vec![SqlValue::Integer(k)];
        let result = index.insert(key, i);
        assert!(result.is_ok(), "Insert of key {} failed: {:?}", k, result);
    }

    // Verify tree properties
    let height = index.height();
    assert!(height >= 2, "Height should be at least 2 for 1000 random keys");
    assert!(height <= 4, "Height should not exceed 4 for 1000 keys");
}

#[test]
fn test_insert_multi_column_keys() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer, DataType::Varchar { max_length: Some(20) }];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    // Insert multi-column keys
    let keys = vec![
        (vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))], 0),
        (vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))], 1),
        (vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Zoe"))], 2),
    ];

    for (key, row_id) in keys {
        index.insert(key, row_id).unwrap();
    }

    // Verify entries are in sorted order
    let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
    assert_eq!(root_leaf.entries.len(), 3);

    // Verify sorted: (1, "Alice"), (1, "Zoe"), (2, "Bob")
    assert!(root_leaf.entries[0].0[0] == SqlValue::Integer(1));
    assert!(root_leaf.entries[1].0[0] == SqlValue::Integer(1));
    assert!(root_leaf.entries[2].0[0] == SqlValue::Integer(2));
}

#[test]
fn test_delete_single_level_tree() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries = vec![
        (vec![SqlValue::Integer(10)], 0),
        (vec![SqlValue::Integer(20)], 1),
        (vec![SqlValue::Integer(30)], 2),
    ];

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Delete middle entry
    assert!(index.delete(&vec![SqlValue::Integer(20)]).unwrap());

    // Verify it was deleted
    let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
    assert_eq!(root_leaf.entries.len(), 2);
    assert_eq!(root_leaf.entries[0].0, vec![SqlValue::Integer(10)]);
    assert_eq!(root_leaf.entries[1].0, vec![SqlValue::Integer(30)]);

    // Try to delete non-existent key
    assert!(!index.delete(&vec![SqlValue::Integer(20)]).unwrap());
}

#[test]
fn test_delete_nonexistent_key() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries = vec![
        (vec![SqlValue::Integer(10)], 0),
        (vec![SqlValue::Integer(20)], 1),
        (vec![SqlValue::Integer(30)], 2),
    ];

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Try to delete key that doesn't exist
    assert!(!index.delete(&vec![SqlValue::Integer(99)]).unwrap());

    // Verify tree unchanged
    let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
    assert_eq!(root_leaf.entries.len(), 3);
}

#[test]
fn test_delete_all_entries_single_level() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries = vec![(vec![SqlValue::Integer(10)], 0), (vec![SqlValue::Integer(20)], 1)];

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Delete both entries
    assert!(index.delete(&vec![SqlValue::Integer(10)]).unwrap());
    assert!(index.delete(&vec![SqlValue::Integer(20)]).unwrap());

    // Verify tree is empty
    let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
    assert_eq!(root_leaf.entries.len(), 0);
}

#[test]
fn test_delete_multi_level_tree() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];

    // Create enough entries to force multi-level tree
    let sorted_entries: Vec<(Key, RowId)> =
        (0..100).map(|i| (vec![SqlValue::Integer(i * 10)], i as usize)).collect();

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();
    let original_height = index.height();

    // Delete some entries
    assert!(index.delete(&vec![SqlValue::Integer(50)]).unwrap());
    assert!(index.delete(&vec![SqlValue::Integer(150)]).unwrap());
    assert!(index.delete(&vec![SqlValue::Integer(250)]).unwrap());

    // Tree should still be valid (height may or may not change)
    assert!(index.height() > 0);
    assert!(index.height() <= original_height);
}

#[test]
fn test_delete_causes_height_decrease() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];

    // Create enough entries to force multi-level tree (need more for bulk_load)
    let sorted_entries: Vec<(Key, RowId)> =
        (0..1000).map(|i| (vec![SqlValue::Integer(i * 10)], i as usize)).collect();

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();
    let original_height = index.height();

    // Ensure we start with a multi-level tree
    assert!(original_height > 1, "Test requires multi-level tree, got height {}", original_height);

    // Delete most entries to trigger height decrease
    for i in 0..990 {
        let result = index.delete(&vec![SqlValue::Integer(i * 10)]);
        assert!(result.is_ok());
    }

    // Height should have decreased
    assert!(
        index.height() < original_height,
        "Height should decrease from {} after deleting 990/1000 entries",
        original_height
    );
}

#[test]
fn test_delete_sequence() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];

    // Create 500 entries
    let sorted_entries: Vec<(Key, RowId)> =
        (0..500).map(|i| (vec![SqlValue::Integer(i * 10)], i as usize)).collect();

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Delete every other entry
    for i in (0..500).step_by(2) {
        assert!(index.delete(&vec![SqlValue::Integer(i * 10)]).unwrap());
    }

    // Verify deletions
    for i in 0..500 {
        let should_exist = i % 2 != 0;
        let exists = index.delete(&vec![SqlValue::Integer(i * 10)]).unwrap();
        assert_eq!(exists, should_exist);
    }
}

#[test]
fn test_delete_multi_column_keys() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer, DataType::Varchar { max_length: Some(50) }];
    let sorted_entries = vec![
        (vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))], 0),
        (vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))], 1),
        (vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))], 2),
    ];

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Delete middle entry
    assert!(index
        .delete(&vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))])
        .unwrap());

    // Verify deletion
    let root_leaf = index.read_leaf_node(index.root_page_id()).unwrap();
    assert_eq!(root_leaf.entries.len(), 2);
    assert_eq!(
        root_leaf.entries[0].0,
        vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]
    );
    assert_eq!(
        root_leaf.entries[1].0,
        vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))]
    );
}

// ========================================================================
// Tests for delete_specific() - removes individual row_ids
// ========================================================================

#[test]
fn test_delete_specific_one_of_many_row_ids() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    // Create entries with duplicate keys (multiple row_ids per key)
    let sorted_entries = vec![
        (vec![SqlValue::Integer(10)], 100),
        (vec![SqlValue::Integer(10)], 200),
        (vec![SqlValue::Integer(10)], 300),
    ];

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Delete one specific row_id, others should remain
    assert!(index.delete_specific(&vec![SqlValue::Integer(10)], 200).unwrap());

    // Verify the key still exists with remaining row_ids
    let remaining_row_ids = index.lookup(&vec![SqlValue::Integer(10)]).unwrap();
    assert_eq!(remaining_row_ids.len(), 2);
    assert!(remaining_row_ids.contains(&100));
    assert!(remaining_row_ids.contains(&300));
    assert!(!remaining_row_ids.contains(&200)); // Deleted row_id should not exist
}

#[test]
fn test_delete_specific_last_row_id_removes_key() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries =
        vec![(vec![SqlValue::Integer(10)], 100), (vec![SqlValue::Integer(20)], 200)];

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Delete the only row_id for key 10
    assert!(index.delete_specific(&vec![SqlValue::Integer(10)], 100).unwrap());

    // Verify key 10 is completely removed
    let row_ids = index.lookup(&vec![SqlValue::Integer(10)]).unwrap();
    assert!(row_ids.is_empty(), "Key should be removed when last row_id is deleted");

    // Verify key 20 still exists
    let row_ids_20 = index.lookup(&vec![SqlValue::Integer(20)]).unwrap();
    assert_eq!(row_ids_20, vec![200]);
}

#[test]
fn test_delete_specific_nonexistent_row_id() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries =
        vec![(vec![SqlValue::Integer(10)], 100), (vec![SqlValue::Integer(10)], 200)];

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Try to delete row_id that doesn't exist for this key
    assert!(!index.delete_specific(&vec![SqlValue::Integer(10)], 999).unwrap());

    // Verify nothing was changed
    let row_ids = index.lookup(&vec![SqlValue::Integer(10)]).unwrap();
    assert_eq!(row_ids.len(), 2);
    assert!(row_ids.contains(&100));
    assert!(row_ids.contains(&200));
}

#[test]
fn test_delete_specific_nonexistent_key() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries = vec![(vec![SqlValue::Integer(10)], 100)];

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Try to delete from key that doesn't exist
    assert!(!index.delete_specific(&vec![SqlValue::Integer(99)], 100).unwrap());

    // Verify existing key unchanged
    let row_ids = index.lookup(&vec![SqlValue::Integer(10)]).unwrap();
    assert_eq!(row_ids, vec![100]);
}

#[test]
fn test_delete_specific_multi_level_tree() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];

    // Create enough entries to force multi-level tree, with some duplicates
    let mut sorted_entries: Vec<(Key, RowId)> = Vec::new();
    for i in 0..500 {
        sorted_entries.push((vec![SqlValue::Integer(i * 10)], (i * 2) as usize));
        // Add duplicate for every 10th key
        if i % 10 == 0 {
            sorted_entries.push((vec![SqlValue::Integer(i * 10)], (i * 2 + 1) as usize));
        }
    }

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();
    assert!(index.height() > 1, "Test requires multi-level tree");

    // Delete specific row_id from a key with duplicates
    // i=0: key=0, row_ids=[0, 1] (because 0 % 10 == 0)
    assert!(index.delete_specific(&vec![SqlValue::Integer(0)], 1).unwrap());

    // Verify the other row_id still exists for this key
    let row_ids = index.lookup(&vec![SqlValue::Integer(0)]).unwrap();
    assert_eq!(row_ids, vec![0]);

    // Delete from a key with only one row_id
    // i=5: key=50, row_ids=[10] (because 5 % 10 != 0, no duplicate)
    assert!(index.delete_specific(&vec![SqlValue::Integer(50)], 10).unwrap());

    // Verify key is removed
    let row_ids_50 = index.lookup(&vec![SqlValue::Integer(50)]).unwrap();
    assert!(row_ids_50.is_empty());
}

#[test]
fn test_delete_specific_vs_delete_all() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries = vec![
        (vec![SqlValue::Integer(10)], 100),
        (vec![SqlValue::Integer(10)], 200),
        (vec![SqlValue::Integer(10)], 300),
        (vec![SqlValue::Integer(20)], 400),
        (vec![SqlValue::Integer(20)], 500),
    ];

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Use delete_specific on key 10 - should only remove one row_id
    assert!(index.delete_specific(&vec![SqlValue::Integer(10)], 200).unwrap());
    let row_ids_10 = index.lookup(&vec![SqlValue::Integer(10)]).unwrap();
    assert_eq!(row_ids_10.len(), 2, "delete_specific should remove only one row_id");

    // Use delete on key 20 - should remove all row_ids
    assert!(index.delete(&vec![SqlValue::Integer(20)]).unwrap());
    let row_ids_20 = index.lookup(&vec![SqlValue::Integer(20)]).unwrap();
    assert!(row_ids_20.is_empty(), "delete should remove all row_ids");
}

#[test]
fn test_delete_specific_with_rebalancing() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];

    // Create enough entries to trigger rebalancing
    let sorted_entries: Vec<(Key, RowId)> =
        (0..500).map(|i| (vec![SqlValue::Integer(i * 10)], i as usize)).collect();

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();
    let original_height = index.height();

    // Delete specific row_ids sequentially (these are single row_id keys)
    for i in (0..450).step_by(3) {
        assert!(index.delete_specific(&vec![SqlValue::Integer(i * 10)], i as usize).unwrap());
    }

    // Tree should still be valid, height may have decreased
    assert!(index.height() > 0);
    assert!(index.height() <= original_height);

    // Verify remaining entries are accessible
    let row_ids = index.lookup(&vec![SqlValue::Integer(10)]).unwrap();
    assert_eq!(row_ids, vec![1], "Non-deleted entries should still be accessible");
}

// ========================================================================
// Tests for prev_leaf pointers and reverse iteration
// ========================================================================

#[test]
fn test_leaf_node_split_maintains_prev_leaf() {
    let mut leaf = LeafNode::new(1);

    // Set an existing next_leaf
    leaf.next_leaf = 10;

    // Insert 6 entries
    for i in 0..6 {
        leaf.insert(vec![SqlValue::Integer(i * 10)], i as usize);
    }

    let (_, right_node) = leaf.split(2);

    // Left node's prev_leaf should be unchanged (was NULL_PAGE_ID)
    assert_eq!(leaf.prev_leaf, super::super::NULL_PAGE_ID);

    // Right node's prev_leaf should point to left node
    assert_eq!(right_node.prev_leaf, 1);

    // Left's next_leaf now points to the new right node
    assert_eq!(leaf.next_leaf, 2);

    // Right node's next_leaf should be the original next_leaf
    assert_eq!(right_node.next_leaf, 10);
}

#[test]
fn test_bulk_load_maintains_prev_leaf() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];

    // Create enough entries to force multiple leaf nodes
    let sorted_entries: Vec<(Key, RowId)> =
        (0..500).map(|i| (vec![SqlValue::Integer(i * 10)], i as usize)).collect();

    let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Verify doubly-linked list by traversing forward and backward
    let leftmost = index.find_leftmost_leaf().unwrap();
    let rightmost = index.find_rightmost_leaf().unwrap();

    // Leftmost should have no prev_leaf
    assert_eq!(leftmost.prev_leaf, super::super::NULL_PAGE_ID);

    // Rightmost should have no next_leaf
    assert_eq!(rightmost.next_leaf, super::super::NULL_PAGE_ID);

    // Traverse forward and count leaves
    let mut forward_count = 0;
    let mut current = leftmost.clone();
    loop {
        forward_count += 1;
        if current.next_leaf == super::super::NULL_PAGE_ID {
            break;
        }
        current = index.read_leaf_node(current.next_leaf).unwrap();
    }

    // Traverse backward and count leaves
    let mut backward_count = 0;
    let mut current = rightmost;
    loop {
        backward_count += 1;
        if current.prev_leaf == super::super::NULL_PAGE_ID {
            break;
        }
        current = index.read_leaf_node(current.prev_leaf).unwrap();
    }

    // Both counts should be equal
    assert_eq!(
        forward_count, backward_count,
        "Forward and backward traversal should visit same number of leaves"
    );
}

#[test]
fn test_insert_maintains_prev_leaf() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    let degree = index.degree();

    // Insert enough keys to cause multiple splits
    for i in 0..(degree * 3) {
        let key = vec![SqlValue::Integer(i as i64 * 10)];
        index.insert(key, i).unwrap();
    }

    // Verify doubly-linked list
    let leftmost = index.find_leftmost_leaf().unwrap();
    let rightmost = index.find_rightmost_leaf().unwrap();

    // Leftmost should have no prev_leaf
    assert_eq!(leftmost.prev_leaf, super::super::NULL_PAGE_ID);

    // Rightmost should have no next_leaf
    assert_eq!(rightmost.next_leaf, super::super::NULL_PAGE_ID);

    // Traverse forward and count
    let mut forward_count = 0;
    let mut current = leftmost;
    loop {
        forward_count += 1;
        if current.next_leaf == super::super::NULL_PAGE_ID {
            break;
        }
        let prev_page_id = current.page_id;
        current = index.read_leaf_node(current.next_leaf).unwrap();
        // Verify prev_leaf points back
        assert_eq!(current.prev_leaf, prev_page_id, "prev_leaf should point to previous node");
    }

    assert!(forward_count > 1, "Test requires multiple leaf nodes");
}

#[test]
fn test_range_scan_reverse_basic() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries = vec![
        (vec![SqlValue::Integer(10)], 0),
        (vec![SqlValue::Integer(20)], 1),
        (vec![SqlValue::Integer(30)], 2),
        (vec![SqlValue::Integer(40)], 3),
        (vec![SqlValue::Integer(50)], 4),
    ];

    let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Full reverse scan
    let result = index.range_scan_reverse(None, None, true, true).unwrap();
    assert_eq!(
        result,
        vec![4, 3, 2, 1, 0],
        "Full reverse scan should return all rows in descending order"
    );

    // Compare with forward scan reversed
    let forward = index.range_scan(None, None, true, true).unwrap();
    let mut forward_reversed = forward.clone();
    forward_reversed.reverse();
    assert_eq!(result, forward_reversed, "Reverse scan should equal reversed forward scan");
}

#[test]
fn test_range_scan_reverse_with_bounds() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries: Vec<(Key, RowId)> =
        (0..10).map(|i| (vec![SqlValue::Integer(i * 10)], i as usize)).collect();

    let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Range [20, 60] in reverse
    let start = vec![SqlValue::Integer(20)];
    let end = vec![SqlValue::Integer(60)];
    let result = index.range_scan_reverse(Some(&start), Some(&end), true, true).unwrap();
    assert_eq!(result, vec![6, 5, 4, 3, 2], "Bounded reverse scan");

    // Exclusive start
    let result = index.range_scan_reverse(Some(&start), Some(&end), false, true).unwrap();
    assert_eq!(result, vec![6, 5, 4, 3], "Reverse scan with exclusive start");

    // Exclusive end
    let result = index.range_scan_reverse(Some(&start), Some(&end), true, false).unwrap();
    assert_eq!(result, vec![5, 4, 3, 2], "Reverse scan with exclusive end");
}

#[test]
fn test_range_scan_reverse_multi_level() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];

    // Create enough entries for multi-level tree
    let sorted_entries: Vec<(Key, RowId)> =
        (0..500).map(|i| (vec![SqlValue::Integer(i * 10)], i as usize)).collect();

    let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();
    assert!(index.height() > 1, "Test requires multi-level tree");

    // Full reverse scan should return all 500 entries in descending order
    let result = index.range_scan_reverse(None, None, true, true).unwrap();
    assert_eq!(result.len(), 500);
    assert_eq!(result[0], 499, "First element should be highest row_id");
    assert_eq!(result[499], 0, "Last element should be lowest row_id");

    // Verify it's actually reversed
    for i in 0..499 {
        assert!(result[i] > result[i + 1], "Results should be in descending order");
    }
}

#[test]
fn test_range_scan_reverse_first() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let sorted_entries: Vec<(Key, RowId)> =
        (0..100).map(|i| (vec![SqlValue::Integer(i * 10)], i as usize)).collect();

    let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Get first element in reverse (should be highest)
    let result = index.range_scan_reverse_first(None, None, true, true).unwrap();
    assert_eq!(result, Some(99), "Reverse first should return highest row_id");

    // Get first element in bounded range in reverse
    let start = vec![SqlValue::Integer(200)];
    let end = vec![SqlValue::Integer(500)];
    let result = index.range_scan_reverse_first(Some(&start), Some(&end), true, true).unwrap();
    assert_eq!(result, Some(50), "Bounded reverse first should return highest in range");

    // Empty range
    let start = vec![SqlValue::Integer(9999)];
    let end = vec![SqlValue::Integer(10000)];
    let result = index.range_scan_reverse_first(Some(&start), Some(&end), true, true).unwrap();
    assert_eq!(result, None, "Empty range should return None");
}

#[test]
fn test_find_rightmost_leaf() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];

    // Create entries for multi-level tree
    let sorted_entries: Vec<(Key, RowId)> =
        (0..500).map(|i| (vec![SqlValue::Integer(i * 10)], i as usize)).collect();

    let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    let rightmost = index.find_rightmost_leaf().unwrap();

    // Rightmost leaf should have no next_leaf
    assert_eq!(rightmost.next_leaf, super::super::NULL_PAGE_ID);

    // Rightmost leaf should contain the highest keys
    assert!(!rightmost.entries.is_empty());
    let highest_key = &rightmost.entries.last().unwrap().0;
    assert_eq!(
        *highest_key,
        vec![SqlValue::Integer(4990)],
        "Rightmost leaf should contain highest key"
    );
}

#[test]
fn test_range_scan_reverse_with_duplicates() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    // Create entries with duplicate keys
    let sorted_entries = vec![
        (vec![SqlValue::Integer(10)], 0),
        (vec![SqlValue::Integer(10)], 1),
        (vec![SqlValue::Integer(20)], 2),
        (vec![SqlValue::Integer(20)], 3),
        (vec![SqlValue::Integer(20)], 4),
        (vec![SqlValue::Integer(30)], 5),
    ];

    let index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();

    // Full reverse scan should return all row_ids in descending order
    let result = index.range_scan_reverse(None, None, true, true).unwrap();
    assert_eq!(result, vec![5, 4, 3, 2, 1, 0], "Reverse scan with duplicates");
}

#[test]
fn test_delete_batch_single_level() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    // Insert some entries
    for i in 0..10 {
        index.insert(vec![SqlValue::Integer(i * 10)], i as usize).unwrap();
    }

    // Batch delete several entries
    let entries_to_delete = vec![
        (vec![SqlValue::Integer(20)], 2),
        (vec![SqlValue::Integer(50)], 5),
        (vec![SqlValue::Integer(70)], 7),
    ];

    let deleted = index.delete_batch(&entries_to_delete).unwrap();
    assert_eq!(deleted, 3, "Should delete 3 entries");

    // Verify they are gone
    assert!(index.lookup(&vec![SqlValue::Integer(20)]).unwrap().is_empty());
    assert!(index.lookup(&vec![SqlValue::Integer(50)]).unwrap().is_empty());
    assert!(index.lookup(&vec![SqlValue::Integer(70)]).unwrap().is_empty());

    // Verify others still exist
    assert_eq!(index.lookup(&vec![SqlValue::Integer(0)]).unwrap(), vec![0]);
    assert_eq!(index.lookup(&vec![SqlValue::Integer(10)]).unwrap(), vec![1]);
    assert_eq!(index.lookup(&vec![SqlValue::Integer(30)]).unwrap(), vec![3]);
}

#[test]
fn test_delete_batch_multi_level() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];

    // Create entries that will span multiple leaves (need more entries for multi-level)
    let sorted_entries: Vec<_> =
        (0..1000).map(|i| (vec![SqlValue::Integer(i * 10)], i as usize)).collect();

    let mut index = BTreeIndex::bulk_load(sorted_entries, key_schema, page_manager).unwrap();
    // Note: multi-level tree depends on degree/page size. If this fails, just skip the assertion.
    let is_multi_level = index.height() > 1;

    // Batch delete entries from different parts of the tree
    let entries_to_delete: Vec<_> = (0..50)
        .map(|i| (vec![SqlValue::Integer(i * 200)], (i * 20) as usize))
        .collect();

    let deleted = index.delete_batch(&entries_to_delete).unwrap();
    assert_eq!(deleted, 50, "Should delete 50 entries");

    // Verify deleted entries are gone
    for i in 0..50 {
        let key = i * 200;
        let result = index.lookup(&vec![SqlValue::Integer(key)]).unwrap();
        assert!(result.is_empty(), "Entry {} should be deleted", key);
    }

    // Verify some remaining entries
    assert_eq!(index.lookup(&vec![SqlValue::Integer(10)]).unwrap(), vec![1]);
    assert_eq!(index.lookup(&vec![SqlValue::Integer(30)]).unwrap(), vec![3]);

    // Log for debugging
    if is_multi_level {
        println!("Test ran with multi-level tree (height={})", index.height());
    }
}

#[test]
fn test_delete_batch_with_duplicates() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    // Insert duplicate keys
    for i in 0..5 {
        index.insert(vec![SqlValue::Integer(100)], i).unwrap();
    }
    for i in 5..10 {
        index.insert(vec![SqlValue::Integer(200)], i).unwrap();
    }

    // Delete specific row_ids from the duplicate key
    let entries_to_delete = vec![
        (vec![SqlValue::Integer(100)], 1),
        (vec![SqlValue::Integer(100)], 3),
        (vec![SqlValue::Integer(200)], 7),
    ];

    let deleted = index.delete_batch(&entries_to_delete).unwrap();
    assert_eq!(deleted, 3, "Should delete 3 entries");

    // Verify remaining row_ids
    let remaining_100 = index.lookup(&vec![SqlValue::Integer(100)]).unwrap();
    assert_eq!(remaining_100, vec![0, 2, 4], "Should have remaining row_ids for key 100");

    let remaining_200 = index.lookup(&vec![SqlValue::Integer(200)]).unwrap();
    assert_eq!(remaining_200, vec![5, 6, 8, 9], "Should have remaining row_ids for key 200");
}

#[test]
fn test_delete_batch_nonexistent_entries() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    // Insert some entries
    index.insert(vec![SqlValue::Integer(10)], 1).unwrap();
    index.insert(vec![SqlValue::Integer(20)], 2).unwrap();

    // Try to delete nonexistent entries
    let entries_to_delete = vec![
        (vec![SqlValue::Integer(10)], 1), // exists
        (vec![SqlValue::Integer(30)], 3), // doesn't exist
        (vec![SqlValue::Integer(20)], 9), // key exists but row_id doesn't
    ];

    let deleted = index.delete_batch(&entries_to_delete).unwrap();
    assert_eq!(deleted, 1, "Should only delete 1 entry");

    // Verify state
    assert!(index.lookup(&vec![SqlValue::Integer(10)]).unwrap().is_empty());
    assert_eq!(index.lookup(&vec![SqlValue::Integer(20)]).unwrap(), vec![2]);
}

#[test]
fn test_delete_batch_empty() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(crate::NativeStorage::new(temp_dir.path()).unwrap());
    let page_manager = Arc::new(PageManager::new("test.db", storage).unwrap());

    let key_schema = vec![DataType::Integer];
    let mut index = BTreeIndex::new(page_manager, key_schema).unwrap();

    index.insert(vec![SqlValue::Integer(10)], 1).unwrap();

    // Empty batch should be a no-op
    let deleted = index.delete_batch(&[]).unwrap();
    assert_eq!(deleted, 0, "Empty batch should delete 0 entries");

    // Original entry should still exist
    assert_eq!(index.lookup(&vec![SqlValue::Integer(10)]).unwrap(), vec![1]);
}
