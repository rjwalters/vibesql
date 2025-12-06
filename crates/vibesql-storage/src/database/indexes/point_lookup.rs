// ============================================================================
// Point Lookup - Single-value equality operations
// ============================================================================

use vibesql_types::SqlValue;

use super::index_metadata::{acquire_btree_lock, IndexData};
use super::value_normalization::{normalize_cow, normalize_for_comparison};

impl IndexData {
    /// Optimized single-key lookup - avoids Vec allocation for common single-column index case.
    ///
    /// # Arguments
    /// * `key` - Single key value to look up (will be normalized for consistent comparison)
    ///
    /// # Returns
    /// Owned vector of row indices if key exists, None otherwise
    ///
    /// # Performance
    /// This method avoids the Vec<SqlValue> allocation that `get()` requires, making it
    /// significantly faster for single-column index lookups (the common case for primary keys).
    #[inline]
    pub fn get_single(&self, key: &SqlValue) -> Option<Vec<usize>> {
        // Use Cow to avoid cloning non-numeric values
        let normalized_key = normalize_cow(key);

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                // Create a temporary slice for lookup without Vec allocation
                let key_slice: &[SqlValue] = std::slice::from_ref(normalized_key.as_ref());
                data.get(key_slice).map(|row_indices| {
                    // Apply lazy adjustment for pending deletions
                    if pending_deletions.is_empty() {
                        row_indices.clone()
                    } else {
                        row_indices
                            .iter()
                            .map(|&row_idx| {
                                let decrement = pending_deletions.partition_point(|&d| d < row_idx);
                                row_idx - decrement
                            })
                            .collect()
                    }
                })
            }
            IndexData::DiskBacked { btree, .. } => {
                // For disk-backed, we still need a Vec for the API, but this is less common
                let key_vec = vec![normalized_key.into_owned()];
                match acquire_btree_lock(btree) {
                    Ok(guard) => match guard.lookup(&key_vec) {
                        Ok(row_ids) if !row_ids.is_empty() => Some(row_ids),
                        Ok(_) => None,
                        Err(e) => {
                            log::warn!("BTreeIndex lookup failed in get_single: {}", e);
                            None
                        }
                    },
                    Err(e) => {
                        log::warn!("BTreeIndex lock acquisition failed in get_single: {}", e);
                        None
                    }
                }
            }
            IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => None,
        }
    }

    /// Optimized single-key existence check - avoids Vec allocation.
    ///
    /// # Arguments
    /// * `key` - Single key value to check (will be normalized for consistent comparison)
    ///
    /// # Returns
    /// true if key exists, false otherwise
    ///
    /// # Performance
    /// This method avoids the Vec<SqlValue> allocation that `contains_key()` requires.
    #[inline]
    pub fn contains_key_single(&self, key: &SqlValue) -> bool {
        let normalized_key = normalize_cow(key);

        match self {
            IndexData::InMemory { data, .. } => {
                let key_slice: &[SqlValue] = std::slice::from_ref(normalized_key.as_ref());
                data.contains_key(key_slice)
            }
            IndexData::DiskBacked { btree, .. } => {
                let key_vec = vec![normalized_key.into_owned()];
                match acquire_btree_lock(btree) {
                    Ok(guard) => match guard.lookup(&key_vec) {
                        Ok(row_ids) => !row_ids.is_empty(),
                        Err(e) => {
                            log::warn!("BTreeIndex lookup failed in contains_key_single: {}", e);
                            false
                        }
                    },
                    Err(e) => {
                        log::warn!(
                            "BTreeIndex lock acquisition failed in contains_key_single: {}",
                            e
                        );
                        false
                    }
                }
            }
            IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => false,
        }
    }

    /// Lookup exact key in the index
    ///
    /// # Arguments
    /// * `key` - Key to look up (values will be normalized for consistent comparison)
    ///
    /// # Returns
    /// Owned vector of row indices if key exists, None otherwise
    ///
    /// # Note
    /// This is the primary point-lookup API for index queries.
    /// Returns owned data to support both in-memory (cloned) and disk-backed (loaded) indexes.
    /// Values are normalized (e.g., Integer -> Double) to match insertion-time normalization.
    ///
    /// # Performance
    /// For single-column indexes, prefer `get_single()` which avoids Vec allocation.
    pub fn get(&self, key: &[SqlValue]) -> Option<Vec<usize>> {
        // Fast path for single-key lookups (common case)
        if key.len() == 1 {
            return self.get_single(&key[0]);
        }

        // Multi-column key path: normalize all key values
        let normalized_key: Vec<SqlValue> = key.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data, pending_deletions } => {
                data.get(&normalized_key).map(|row_indices| {
                    // Apply lazy adjustment for pending deletions
                    if pending_deletions.is_empty() {
                        row_indices.clone()
                    } else {
                        row_indices
                            .iter()
                            .map(|&row_idx| {
                                let decrement = pending_deletions.partition_point(|&d| d < row_idx);
                                row_idx - decrement
                            })
                            .collect()
                    }
                })
            }
            IndexData::DiskBacked { btree, .. } => {
                // Safely acquire lock and perform lookup
                match acquire_btree_lock(btree) {
                    Ok(guard) => {
                        match guard.lookup(&normalized_key) {
                            Ok(row_ids) if !row_ids.is_empty() => Some(row_ids),
                            Ok(_) => None, // Empty result means key not found
                            Err(e) => {
                                log::warn!("BTreeIndex lookup failed in get: {}", e);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("BTreeIndex lock acquisition failed in get: {}", e);
                        None
                    }
                }
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support point lookups - use search() method instead
                None
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support point lookups - use search() method instead
                None
            }
        }
    }

    /// Check if a key exists in the index
    ///
    /// # Arguments
    /// * `key` - Key to check (values will be normalized for consistent comparison)
    ///
    /// # Returns
    /// true if key exists, false otherwise
    ///
    /// # Note
    /// Used primarily for UNIQUE constraint validation.
    /// Values are normalized (e.g., Integer -> Double) to match insertion-time normalization.
    ///
    /// # Performance
    /// For single-column indexes, prefer `contains_key_single()` which avoids Vec allocation.
    pub fn contains_key(&self, key: &[SqlValue]) -> bool {
        // Fast path for single-key lookups (common case)
        if key.len() == 1 {
            return self.contains_key_single(&key[0]);
        }

        // Multi-column key path: normalize all key values
        let normalized_key: Vec<SqlValue> = key.iter().map(normalize_for_comparison).collect();

        match self {
            IndexData::InMemory { data, .. } => data.contains_key(&normalized_key),
            IndexData::DiskBacked { btree, .. } => {
                // Safely acquire lock and check if key exists
                match acquire_btree_lock(btree) {
                    Ok(guard) => match guard.lookup(&normalized_key) {
                        Ok(row_ids) => !row_ids.is_empty(),
                        Err(e) => {
                            log::warn!("BTreeIndex lookup failed in contains_key: {}", e);
                            false
                        }
                    },
                    Err(e) => {
                        log::warn!("BTreeIndex lock acquisition failed in contains_key: {}", e);
                        false
                    }
                }
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support contains_key - use search() method instead
                false
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support contains_key - use search() method instead
                false
            }
        }
    }

    /// Lookup multiple values in the index (for IN predicates)
    ///
    /// # Arguments
    /// * `values` - List of values to look up
    ///
    /// # Returns
    /// Vector of row indices that match any of the values
    ///
    /// # Performance
    /// Uses Cow normalization to avoid cloning non-numeric values, and slice::from_ref
    /// to avoid Vec allocation per key lookup.
    pub fn multi_lookup(&self, values: &[SqlValue]) -> Vec<usize> {
        match self {
            IndexData::InMemory { data, pending_deletions } => {
                // Deduplicate values to avoid returning duplicate rows
                // For example, WHERE a IN (10, 10, 20) should only look up 10 once
                let mut unique_values: Vec<&SqlValue> = values.iter().collect();
                unique_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                unique_values.dedup();

                let mut matching_row_indices = Vec::new();

                for value in unique_values {
                    // Use Cow to avoid cloning non-numeric values
                    let normalized_value = normalize_cow(value);
                    // Use slice::from_ref to avoid Vec allocation per key
                    let search_key: &[SqlValue] = std::slice::from_ref(normalized_value.as_ref());
                    if let Some(row_indices) = data.get(search_key) {
                        matching_row_indices.extend(row_indices);
                    }
                }

                // Apply lazy adjustment for pending deletions
                if !pending_deletions.is_empty() {
                    for row_idx in &mut matching_row_indices {
                        let decrement = pending_deletions.partition_point(|&d| d < *row_idx);
                        *row_idx -= decrement;
                    }
                }

                // Return row indices in the order they were collected from BTreeMap
                // For IN predicates, we collect results for each value in the order
                // specified. We should NOT sort by row index as that would destroy
                // the semantic ordering of the results.
                matching_row_indices
            }
            IndexData::DiskBacked { btree, .. } => {
                // Deduplicate values to avoid returning duplicate rows
                let mut unique_values: Vec<&SqlValue> = values.iter().collect();
                unique_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                unique_values.dedup();

                // For disk-backed, we need Vec<Vec<SqlValue>> for the API
                // but we still use normalize_cow to reduce cloning
                let keys: Vec<Vec<SqlValue>> = unique_values
                    .iter()
                    .map(|v| vec![normalize_cow(v).into_owned()])
                    .collect();

                // Safely acquire lock and call BTreeIndex::multi_lookup
                match acquire_btree_lock(btree) {
                    Ok(guard) => guard.multi_lookup(&keys).unwrap_or_else(|_| vec![]),
                    Err(e) => {
                        // Log error and return empty result set
                        log::warn!("BTreeIndex lock acquisition failed in multi_lookup: {}", e);
                        vec![]
                    }
                }
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support multi_lookup - use search() method instead
                vec![]
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support multi_lookup - use search() method instead
                vec![]
            }
        }
    }

    /// Get an iterator over all key-value pairs in the index
    ///
    /// # Returns
    /// Iterator yielding owned (key, row_indices) pairs
    ///
    /// # Note
    /// For in-memory indexes, iteration is in sorted key order (BTreeMap ordering).
    /// This method enables index scanning operations without exposing internal data structures.
    /// Returns owned data to support both in-memory (cloned) and disk-backed (loaded) indexes.
    ///
    /// **Note**: For disk-backed indexes, this requires a full B+ tree scan and is expensive.
    /// Currently returns an empty iterator as the BTreeIndex doesn't expose key-level iteration.
    /// Most use cases should use `values()` for full scans or `multi_lookup()` for specific keys.
    pub fn iter(&self) -> Box<dyn Iterator<Item = (Vec<SqlValue>, Vec<usize>)> + '_> {
        match self {
            IndexData::InMemory { data, pending_deletions } => {
                let pending = pending_deletions.clone();
                Box::new(data.iter().map(move |(k, v)| {
                    let adjusted_v = if pending.is_empty() {
                        v.clone()
                    } else {
                        v.iter()
                            .map(|&row_idx| {
                                let decrement = pending.partition_point(|&d| d < row_idx);
                                row_idx - decrement
                            })
                            .collect()
                    };
                    (k.clone(), adjusted_v)
                }))
            }
            IndexData::DiskBacked { .. } => {
                // BTreeIndex doesn't currently expose an API for iterating over (key, row_ids) pairs
                // This would require adding a scan API that preserves key groupings
                // For now, return empty iterator since this method is rarely used
                // Callers should use values() for full scans or lookup()/multi_lookup() for point queries
                log::warn!("DiskBacked iter() is not yet implemented - use values() instead");
                Box::new(std::iter::empty())
            }
            IndexData::IVFFlat { .. } => {
                // IVFFlat indexes don't support iteration - use search() method instead
                Box::new(std::iter::empty())
            }
            IndexData::Hnsw { .. } => {
                // HNSW indexes don't support iteration - use search() method instead
                Box::new(std::iter::empty())
            }
        }
    }

    /// Get an iterator over all row index vectors in the index
    ///
    /// # Returns
    /// Iterator yielding owned row index vectors
    ///
    /// # Note
    /// This method is used for full index scans where we need all row indices
    /// regardless of the key values. Returns owned data to support both in-memory
    /// (cloned) and disk-backed (loaded from disk) indexes.
    pub fn values(&self) -> Box<dyn Iterator<Item = Vec<usize>> + '_> {
        match self {
            IndexData::InMemory { data, pending_deletions } => {
                let pending = pending_deletions.clone();
                Box::new(data.values().map(move |v| {
                    if pending.is_empty() {
                        v.clone()
                    } else {
                        v.iter()
                            .map(|&row_idx| {
                                let decrement = pending.partition_point(|&d| d < row_idx);
                                row_idx - decrement
                            })
                            .collect()
                    }
                }))
            }
            IndexData::DiskBacked { btree, .. } => {
                // Perform a full range scan to get all values
                // Use range_scan with no bounds to scan entire index
                match acquire_btree_lock(btree) {
                    Ok(guard) => {
                        match guard.range_scan(None, None, true, true) {
                            Ok(all_row_ids) => {
                                // Group row_ids by their appearance (BTree returns them in key order)
                                // For full scan, we just need all row IDs, so wrap in a single Vec
                                Box::new(std::iter::once(all_row_ids))
                            }
                            Err(e) => {
                                log::warn!("BTreeIndex range_scan failed in values: {}", e);
                                Box::new(std::iter::empty())
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("BTreeIndex lock acquisition failed in values: {}", e);
                        Box::new(std::iter::empty())
                    }
                }
            }
            IndexData::IVFFlat { index } => {
                // Return all row IDs stored in the IVFFlat index
                let all_row_ids: Vec<usize> = index.all_row_ids();
                Box::new(std::iter::once(all_row_ids))
            }
            IndexData::Hnsw { index } => {
                // Return all row IDs stored in the HNSW index
                let all_row_ids: Vec<usize> = index.all_row_ids();
                Box::new(std::iter::once(all_row_ids))
            }
        }
    }
}
