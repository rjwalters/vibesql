//! Query operations for B+ Tree
//!
//! This module handles key lookups, range scans, and multi-key queries
//! with full support for non-unique indexes (duplicate keys).

use crate::StorageError;

use super::super::structure::{Key, LeafNode, RowId};
use super::BTreeIndex;

impl BTreeIndex {
    /// Look up all row IDs for a given key
    ///
    /// For non-unique indexes, this returns all row IDs associated with the key.
    /// For unique indexes, the Vec will contain at most one element.
    ///
    /// # Arguments
    /// * `key` - The key to search for
    ///
    /// # Returns
    /// * Vector of row_ids associated with this key (empty Vec if key not found)
    ///
    /// # Performance
    /// - O(log n) to find the key (where n = number of unique keys)
    /// - O(k) to clone row IDs (where k = number of duplicates)
    /// - Total: O(log n + k)
    ///
    /// # Example
    /// ```text
    /// use vibesql_types::SqlValue;
    ///
    /// // Insert duplicate keys
    /// index.insert(vec![SqlValue::Integer(42)], 1)?;
    /// index.insert(vec![SqlValue::Integer(42)], 2)?;
    /// index.insert(vec![SqlValue::Integer(42)], 3)?;
    ///
    /// // Lookup returns all row IDs
    /// let rows = index.lookup(&vec![SqlValue::Integer(42)])?;
    /// assert_eq!(rows, vec![1, 2, 3]);
    ///
    /// // Nonexistent key returns empty Vec
    /// let rows = index.lookup(&vec![SqlValue::Integer(99)])?;
    /// assert!(rows.is_empty());
    /// ```
    ///
    /// # Algorithm
    /// 1. Navigate to the appropriate leaf node using find_leaf_path
    /// 2. Search for the key in the leaf node
    /// 3. Return all row_ids associated with the key (or empty Vec if not found)
    pub fn lookup(&self, key: &Key) -> Result<Vec<RowId>, StorageError> {
        // Find the leaf that would contain this key
        let (mut current_leaf, _) = self.find_leaf_path(key)?;

        let mut result = Vec::new();

        // Collect all row_ids from entries matching this key
        // This handles the case where large row_id lists are split across multiple entries
        loop {
            // Use binary search to find the first entry with matching key
            let start_idx =
                current_leaf.entries.binary_search_by(|(k, _)| k.cmp(key)).unwrap_or_else(|i| i);

            // Collect all consecutive entries with matching key
            for idx in start_idx..current_leaf.entries.len() {
                let (entry_key, row_ids) = &current_leaf.entries[idx];
                if entry_key == key {
                    result.extend(row_ids.iter().copied());
                } else {
                    // Past the key, we're done with this leaf
                    // If entry_key > key, we've gone past and can return
                    return Ok(result);
                }
            }

            // Check if we need to continue to the next leaf
            // This is needed when entries with the same key span multiple leaves
            if current_leaf.next_leaf == super::super::super::NULL_PAGE_ID {
                break;
            }

            // Check if next leaf might have more entries with this key
            // by looking at the first entry of the next leaf
            let next_leaf = self.read_leaf_node(current_leaf.next_leaf)?;
            if let Some((first_key, _)) = next_leaf.entries.first() {
                if first_key == key {
                    current_leaf = next_leaf;
                    continue;
                }
            }
            // Next leaf doesn't start with our key, we're done
            break;
        }

        Ok(result)
    }

    /// Perform a range scan on the B+ tree
    ///
    /// Returns all row_ids for keys in the specified range [start_key, end_key].
    /// The range can be inclusive or exclusive on either end based on the parameters.
    ///
    /// For non-unique indexes with duplicate keys, all row IDs for each key in the
    /// range are included in the result. The row IDs are returned in key order, with
    /// all duplicates for each key grouped together.
    ///
    /// # Arguments
    /// * `start_key` - Optional lower bound key (None = start from beginning)
    /// * `end_key` - Optional upper bound key (None = scan to end)
    /// * `inclusive_start` - Whether start_key is inclusive (default: true)
    /// * `inclusive_end` - Whether end_key is inclusive (default: true)
    ///
    /// # Returns
    /// Vector of row_ids in sorted key order (includes all duplicates)
    ///
    /// # Performance
    /// - O(log n) to find starting leaf (where n = number of unique keys)
    /// - O(m + k) to scan through results (where m = keys in range, k = total duplicates)
    /// - Total: O(log n + m + k)
    ///
    /// # Example
    /// ```text
    /// use vibesql_types::SqlValue;
    ///
    /// // Insert some data with duplicates
    /// index.insert(vec![SqlValue::Integer(10)], 1)?;
    /// index.insert(vec![SqlValue::Integer(20)], 2)?;
    /// index.insert(vec![SqlValue::Integer(20)], 3)?; // duplicate
    /// index.insert(vec![SqlValue::Integer(30)], 4)?;
    ///
    /// // Range scan [15, 25] returns both row IDs for key 20
    /// let rows = index.range_scan(
    ///     Some(&vec![SqlValue::Integer(15)]),
    ///     Some(&vec![SqlValue::Integer(25)]),
    ///     true,
    ///     true
    /// )?;
    /// assert_eq!(rows, vec![2, 3]); // Both duplicates included
    /// ```
    ///
    /// # Algorithm
    /// 1. Find the starting leaf node
    /// 2. Iterate through leaf nodes using next_leaf pointers
    /// 3. Collect all row_ids (including duplicates) within the range
    /// 4. Stop when reaching the end key or end of tree
    pub fn range_scan(
        &self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> Result<Vec<RowId>, StorageError> {
        let mut result = Vec::new();

        // Find starting point
        let mut current_leaf = if let Some(start) = start_key {
            // Find leaf containing start_key
            let (leaf, _) = self.find_leaf_path(start)?;
            leaf
        } else {
            // Start from leftmost leaf
            self.find_leftmost_leaf()?
        };

        // Track whether we've started collecting results (passed start_key check)
        // Once true, we can skip redundant start_key comparisons
        let mut started = start_key.is_none();

        // Scan through leaves
        loop {
            // Process entries in current leaf
            for (key, row_ids) in &current_leaf.entries {
                // Check if we're past the end key
                if let Some(end) = end_key {
                    let cmp = key.cmp(end);
                    if cmp == std::cmp::Ordering::Greater {
                        // Past end key, stop
                        return Ok(result);
                    }
                    if cmp == std::cmp::Ordering::Equal && !inclusive_end {
                        // At end key but not inclusive, stop
                        return Ok(result);
                    }
                }

                // Check if we're before the start key (only if we haven't started yet)
                if !started {
                    if let Some(start) = start_key {
                        let cmp = key.cmp(start);
                        if cmp == std::cmp::Ordering::Less {
                            // Before start key, skip
                            continue;
                        }
                        if cmp == std::cmp::Ordering::Equal && !inclusive_start {
                            // At start key but not inclusive, skip
                            continue;
                        }
                        // We've now passed the start_key check
                        started = true;
                    }
                }

                // Key is in range, add all row_ids
                result.extend(row_ids.iter().copied());
            }

            // Move to next leaf
            if current_leaf.next_leaf == super::super::super::NULL_PAGE_ID {
                // No more leaves
                break;
            }
            current_leaf = self.read_leaf_node(current_leaf.next_leaf)?;
        }

        Ok(result)
    }

    /// Perform a range scan returning only the first matching row
    ///
    /// This is an optimized version of range_scan for queries with LIMIT 1.
    /// It returns immediately after finding the first matching row instead of
    /// collecting all rows in the range.
    ///
    /// # Arguments
    /// * `start_key` - Optional lower bound key (None = start from beginning)
    /// * `end_key` - Optional upper bound key (None = scan to end)
    /// * `inclusive_start` - Whether start_key is inclusive
    /// * `inclusive_end` - Whether end_key is inclusive
    ///
    /// # Returns
    /// The first row_id in the range, or None if no matching rows
    ///
    /// # Performance
    /// O(log n) - only finds the first leaf and returns immediately
    ///
    /// # Example
    /// ```text
    /// // TPC-C Delivery: find oldest new_order for a district
    /// // Only returns the first (minimum) order ID
    /// let first = index.range_scan_first(
    ///     Some(&vec![w_id, d_id]),  // prefix start
    ///     Some(&vec![w_id, d_id + 1]),  // prefix end
    ///     true, false
    /// )?;
    /// ```
    pub fn range_scan_first(
        &self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> Result<Option<RowId>, StorageError> {
        // Find starting point
        let mut current_leaf = if let Some(start) = start_key {
            let (leaf, _) = self.find_leaf_path(start)?;
            leaf
        } else {
            self.find_leftmost_leaf()?
        };

        let mut started = start_key.is_none();

        // Scan through leaves looking for first match
        loop {
            for (key, row_ids) in &current_leaf.entries {
                // Check if we're past the end key
                if let Some(end) = end_key {
                    let cmp = key.cmp(end);
                    if cmp == std::cmp::Ordering::Greater {
                        return Ok(None);
                    }
                    if cmp == std::cmp::Ordering::Equal && !inclusive_end {
                        return Ok(None);
                    }
                }

                // Check if we're before the start key
                if !started {
                    if let Some(start) = start_key {
                        let cmp = key.cmp(start);
                        if cmp == std::cmp::Ordering::Less {
                            continue;
                        }
                        if cmp == std::cmp::Ordering::Equal && !inclusive_start {
                            continue;
                        }
                        started = true;
                    }
                }

                // Found a key in range - return immediately
                if let Some(&first_row) = row_ids.first() {
                    return Ok(Some(first_row));
                }
            }

            // Move to next leaf
            if current_leaf.next_leaf == super::super::super::NULL_PAGE_ID {
                break;
            }
            current_leaf = self.read_leaf_node(current_leaf.next_leaf)?;
        }

        Ok(None)
    }

    /// Lookup multiple keys in the B+ tree (for IN predicates)
    ///
    /// # Arguments
    /// * `keys` - List of keys to look up
    ///
    /// # Returns
    /// Vector of row_ids for all keys that were found
    ///
    /// # Algorithm
    /// For each key, perform a lookup and collect all row_ids.
    /// This is a simple implementation that performs individual lookups.
    /// A more optimized version could sort keys and batch lookups by leaf node.
    pub fn multi_lookup(&self, keys: &[Key]) -> Result<Vec<RowId>, StorageError> {
        let mut result = Vec::new();

        for key in keys {
            let row_ids = self.lookup(key)?;
            result.extend(row_ids);
        }

        Ok(result)
    }

    /// Find the leftmost (first) leaf node in the tree
    ///
    /// # Returns
    /// The leftmost leaf node
    pub(crate) fn find_leftmost_leaf(&self) -> Result<LeafNode, StorageError> {
        let mut current_page_id = self.root_page_id;

        // Navigate down the tree always taking the leftmost child
        for _ in 0..self.height - 1 {
            let internal = self.read_internal_node(current_page_id)?;
            if internal.children.is_empty() {
                return Err(StorageError::IoError("Internal node has no children".to_string()));
            }
            // Always take first child (leftmost)
            current_page_id = internal.children[0];
        }

        // Read the leftmost leaf
        self.read_leaf_node(current_page_id)
    }

    /// Find the rightmost (last) leaf node in the tree
    ///
    /// # Returns
    /// The rightmost leaf node
    pub(crate) fn find_rightmost_leaf(&self) -> Result<LeafNode, StorageError> {
        let mut current_page_id = self.root_page_id;

        // Navigate down the tree always taking the rightmost child
        for _ in 0..self.height - 1 {
            let internal = self.read_internal_node(current_page_id)?;
            if internal.children.is_empty() {
                return Err(StorageError::IoError("Internal node has no children".to_string()));
            }
            // Always take last child (rightmost)
            current_page_id = *internal.children.last().unwrap();
        }

        // Read the rightmost leaf
        self.read_leaf_node(current_page_id)
    }

    /// Perform a reverse range scan on the B+ tree
    ///
    /// Returns all row_ids for keys in the specified range [start_key, end_key]
    /// in descending key order. This uses the prev_leaf pointers for efficient
    /// backward traversal.
    ///
    /// # Arguments
    /// * `start_key` - Optional lower bound key (None = start from beginning)
    /// * `end_key` - Optional upper bound key (None = scan to end)
    /// * `inclusive_start` - Whether start_key is inclusive
    /// * `inclusive_end` - Whether end_key is inclusive
    ///
    /// # Returns
    /// Vector of row_ids in descending key order
    ///
    /// # Performance
    /// - O(log n) to find the ending leaf
    /// - O(m + k) to scan backwards through results
    /// - Total: O(log n + m + k)
    pub fn range_scan_reverse(
        &self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> Result<Vec<RowId>, StorageError> {
        let mut result = Vec::new();

        // Find ending point (we start from the highest key and work backwards)
        let mut current_leaf = if let Some(end) = end_key {
            // Find leaf containing end_key
            let (leaf, _) = self.find_leaf_path(end)?;
            leaf
        } else {
            // Start from rightmost leaf
            self.find_rightmost_leaf()?
        };

        // Track whether we've started collecting results (passed end_key check)
        let mut started = end_key.is_none();

        // Scan through leaves in reverse order
        loop {
            // Process entries in current leaf in reverse order
            for (key, row_ids) in current_leaf.entries.iter().rev() {
                // Check if we're before the start key (in reverse, this means we've gone too far back)
                if let Some(start) = start_key {
                    let cmp = key.cmp(start);
                    if cmp == std::cmp::Ordering::Less {
                        // Before start key, stop
                        return Ok(result);
                    }
                    if cmp == std::cmp::Ordering::Equal && !inclusive_start {
                        // At start key but not inclusive, stop
                        return Ok(result);
                    }
                }

                // Check if we're past the end key (only if we haven't started yet)
                if !started {
                    if let Some(end) = end_key {
                        let cmp = key.cmp(end);
                        if cmp == std::cmp::Ordering::Greater {
                            // Past end key, skip
                            continue;
                        }
                        if cmp == std::cmp::Ordering::Equal && !inclusive_end {
                            // At end key but not inclusive, skip
                            continue;
                        }
                        // We've now passed the end_key check
                        started = true;
                    }
                }

                // Key is in range, add all row_ids in reverse order
                result.extend(row_ids.iter().rev());
            }

            // Move to previous leaf
            if current_leaf.prev_leaf == super::super::super::NULL_PAGE_ID {
                // No more leaves
                break;
            }
            current_leaf = self.read_leaf_node(current_leaf.prev_leaf)?;
        }

        Ok(result)
    }

    /// Perform a reverse range scan returning only the first matching row
    ///
    /// This is an optimized version of range_scan_reverse for queries with LIMIT 1.
    /// It returns immediately after finding the first matching row (highest key).
    ///
    /// # Arguments
    /// * `start_key` - Optional lower bound key (None = start from beginning)
    /// * `end_key` - Optional upper bound key (None = scan to end)
    /// * `inclusive_start` - Whether start_key is inclusive
    /// * `inclusive_end` - Whether end_key is inclusive
    ///
    /// # Returns
    /// The first row_id in descending key order, or None if no matching rows
    ///
    /// # Performance
    /// O(log n) - only finds the ending leaf and returns immediately
    pub fn range_scan_reverse_first(
        &self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> Result<Option<RowId>, StorageError> {
        // Find ending point
        let mut current_leaf = if let Some(end) = end_key {
            let (leaf, _) = self.find_leaf_path(end)?;
            leaf
        } else {
            self.find_rightmost_leaf()?
        };

        let mut started = end_key.is_none();

        // Scan through leaves in reverse looking for first match
        loop {
            for (key, row_ids) in current_leaf.entries.iter().rev() {
                // Check if we're before the start key
                if let Some(start) = start_key {
                    let cmp = key.cmp(start);
                    if cmp == std::cmp::Ordering::Less {
                        return Ok(None);
                    }
                    if cmp == std::cmp::Ordering::Equal && !inclusive_start {
                        return Ok(None);
                    }
                }

                // Check if we're past the end key
                if !started {
                    if let Some(end) = end_key {
                        let cmp = key.cmp(end);
                        if cmp == std::cmp::Ordering::Greater {
                            continue;
                        }
                        if cmp == std::cmp::Ordering::Equal && !inclusive_end {
                            continue;
                        }
                        started = true;
                    }
                }

                // Found a key in range - return immediately (last element for DESC order)
                if let Some(&last_row) = row_ids.last() {
                    return Ok(Some(last_row));
                }
            }

            // Move to previous leaf
            if current_leaf.prev_leaf == super::super::super::NULL_PAGE_ID {
                break;
            }
            current_leaf = self.read_leaf_node(current_leaf.prev_leaf)?;
        }

        Ok(None)
    }

    /// Get distinct N-column prefixes from a composite index
    ///
    /// This generalizes single-column skip-scan to multi-column skip-scan.
    /// It returns all distinct N-column prefixes, which are then used for
    /// targeted lookups when filtering on columns beyond the Nth position.
    ///
    /// # Arguments
    /// * `num_columns` - Number of prefix columns to extract (1 = first column only)
    ///
    /// # Returns
    /// Vector of distinct N-column prefixes in sorted order
    ///
    /// # Performance
    /// O(n) where n is the number of unique keys - must scan all leaves
    pub fn get_distinct_prefix_values(
        &self,
        num_columns: usize,
    ) -> Result<Vec<Vec<vibesql_types::SqlValue>>, StorageError> {
        if num_columns == 0 {
            return Ok(vec![vec![]]); // Empty prefix matches everything
        }

        let mut prefixes = Vec::new();
        let mut last_prefix: Option<Vec<vibesql_types::SqlValue>> = None;

        // Start from leftmost leaf
        let mut current_leaf = self.find_leftmost_leaf()?;

        loop {
            for (key, _) in &current_leaf.entries {
                if key.len() < num_columns {
                    continue; // Key doesn't have enough columns
                }

                let prefix: Vec<vibesql_types::SqlValue> = key[..num_columns].to_vec();
                let is_new = match &last_prefix {
                    None => true,
                    Some(last) => prefix != *last,
                };
                if is_new {
                    prefixes.push(prefix.clone());
                    last_prefix = Some(prefix);
                }
            }

            // Move to next leaf
            if current_leaf.next_leaf == super::super::super::NULL_PAGE_ID {
                break;
            }
            current_leaf = self.read_leaf_node(current_leaf.next_leaf)?;
        }

        Ok(prefixes)
    }

    /// Get distinct values of the first column in a composite index
    ///
    /// This is a convenience method that calls `get_distinct_prefix_values(1)`
    /// and flattens the result to single values.
    ///
    /// # Returns
    /// Vector of distinct first-column values in sorted order
    ///
    /// # Performance
    /// O(n) where n is the number of unique keys - must scan all leaves
    pub fn get_distinct_first_column_values(
        &self,
    ) -> Result<Vec<vibesql_types::SqlValue>, StorageError> {
        let prefixes = self.get_distinct_prefix_values(1)?;
        Ok(prefixes
            .into_iter()
            .filter_map(|mut p| p.pop())
            .collect())
    }

    /// Skip-scan equality lookup on a non-prefix column
    ///
    /// Scans all entries and returns rows where the specified column matches.
    ///
    /// # Arguments
    /// * `filter_column_idx` - Index of the column to filter on (0-based)
    /// * `filter_value` - Value to match
    ///
    /// # Returns
    /// Vector of row indices matching the filter
    ///
    /// # Performance
    /// O(n) where n is total entries - scans all leaves
    pub fn skip_scan_equality(
        &self,
        filter_column_idx: usize,
        filter_value: &vibesql_types::SqlValue,
    ) -> Result<Vec<RowId>, StorageError> {
        let mut result = Vec::new();

        // Start from leftmost leaf
        let mut current_leaf = self.find_leftmost_leaf()?;

        loop {
            for (key, row_ids) in &current_leaf.entries {
                if key.len() > filter_column_idx && &key[filter_column_idx] == filter_value {
                    result.extend(row_ids.iter().copied());
                }
            }

            // Move to next leaf
            if current_leaf.next_leaf == super::super::super::NULL_PAGE_ID {
                break;
            }
            current_leaf = self.read_leaf_node(current_leaf.next_leaf)?;
        }

        Ok(result)
    }

    /// Skip-scan range lookup on a non-prefix column
    ///
    /// Scans all entries and returns rows where the specified column is in range.
    ///
    /// # Arguments
    /// * `filter_column_idx` - Index of the column to filter on (0-based)
    /// * `lower_bound` - Optional lower bound
    /// * `inclusive_lower` - Whether lower bound is inclusive
    /// * `upper_bound` - Optional upper bound
    /// * `inclusive_upper` - Whether upper bound is inclusive
    ///
    /// # Returns
    /// Vector of row indices matching the range filter
    pub fn skip_scan_range(
        &self,
        filter_column_idx: usize,
        lower_bound: Option<&vibesql_types::SqlValue>,
        inclusive_lower: bool,
        upper_bound: Option<&vibesql_types::SqlValue>,
        inclusive_upper: bool,
    ) -> Result<Vec<RowId>, StorageError> {
        let mut result = Vec::new();

        // Start from leftmost leaf
        let mut current_leaf = self.find_leftmost_leaf()?;

        loop {
            for (key, row_ids) in &current_leaf.entries {
                if key.len() > filter_column_idx {
                    let key_val = &key[filter_column_idx];

                    // Check bounds
                    let passes_lower = match lower_bound {
                        None => true,
                        Some(lb) => {
                            if inclusive_lower {
                                key_val >= lb
                            } else {
                                key_val > lb
                            }
                        }
                    };

                    let passes_upper = match upper_bound {
                        None => true,
                        Some(ub) => {
                            if inclusive_upper {
                                key_val <= ub
                            } else {
                                key_val < ub
                            }
                        }
                    };

                    if passes_lower && passes_upper {
                        result.extend(row_ids.iter().copied());
                    }
                }
            }

            // Move to next leaf
            if current_leaf.next_leaf == super::super::super::NULL_PAGE_ID {
                break;
            }
            current_leaf = self.read_leaf_node(current_leaf.next_leaf)?;
        }

        Ok(result)
    }
}
