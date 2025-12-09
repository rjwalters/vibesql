// ============================================================================
// Distinct Value Operations - Extract unique prefixes from indexes
// ============================================================================
//
// These methods support skip-scan optimization by extracting distinct values
// from index prefix columns.

use vibesql_types::SqlValue;

use crate::database::indexes::index_metadata::{acquire_btree_lock, IndexData};

impl IndexData {
    /// Skip-scan: Get distinct N-column prefixes from the index
    ///
    /// This generalizes single-column skip-scan to multi-column skip-scan.
    /// It returns all distinct N-column prefixes, which are then used for
    /// targeted lookups when filtering on columns beyond the Nth position.
    ///
    /// # Arguments
    /// * `num_columns` - Number of prefix columns to extract (1 = first column only)
    ///
    /// # Performance
    /// O(k) where k is the number of distinct N-column prefixes.
    /// For BTreeMap, this iterates through all keys and collects unique prefixes.
    ///
    /// # Returns
    /// Vector of distinct N-column prefixes
    ///
    /// # Example
    /// ```text
    /// // Index on (country, region, city, date) with data:
    /// // [US, East, NYC, 2024-01-01], [US, East, NYC, 2024-01-02],
    /// // [US, East, Boston, 2024-01-01], [US, West, LA, 2024-01-01]
    /// let prefixes = index_data.get_distinct_prefix_values(2);
    /// // Returns: [[US, East], [US, West]]
    /// let prefixes = index_data.get_distinct_prefix_values(3);
    /// // Returns: [[US, East, Boston], [US, East, NYC], [US, West, LA]]
    /// ```
    pub fn get_distinct_prefix_values(&self, num_columns: usize) -> Vec<Vec<SqlValue>> {
        if num_columns == 0 {
            return vec![vec![]]; // Empty prefix matches everything
        }

        match self {
            IndexData::InMemory { data, .. } => {
                let mut prefixes = Vec::new();
                let mut last_prefix: Option<Vec<SqlValue>> = None;

                for key in data.keys() {
                    if key.len() < num_columns {
                        continue; // Key doesn't have enough columns
                    }

                    let prefix: Vec<SqlValue> = key[..num_columns].to_vec();
                    let is_new = match &last_prefix {
                        None => true,
                        Some(last) => prefix != *last,
                    };
                    if is_new {
                        prefixes.push(prefix.clone());
                        last_prefix = Some(prefix);
                    }
                }

                prefixes
            }
            IndexData::DiskBacked { btree, .. } => {
                // For disk-backed indexes, we need to scan all keys
                match acquire_btree_lock(btree) {
                    Ok(guard) => guard
                        .get_distinct_prefix_values(num_columns)
                        .unwrap_or_else(|_| vec![]),
                    Err(e) => {
                        log::warn!(
                            "BTreeIndex lock acquisition failed in get_distinct_prefix_values: {}",
                            e
                        );
                        vec![]
                    }
                }
            }
            IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                // Vector indexes don't support skip-scan
                vec![]
            }
        }
    }

    /// Skip-scan: Get distinct values of the first column in the index
    ///
    /// This is a convenience method that calls `get_distinct_prefix_values(1)`
    /// and flattens the result to single values.
    ///
    /// # Performance
    /// O(k) where k is the number of distinct first-column values.
    /// For BTreeMap, this iterates through all keys and collects unique first elements.
    ///
    /// # Returns
    /// Vector of distinct values for the first index column
    ///
    /// # Example
    /// ```text
    /// // Index on (region, date, id) with data:
    /// // [East, 2024-01-01, 1], [East, 2024-01-02, 2], [West, 2024-01-01, 3]
    /// let prefixes = index_data.get_distinct_first_column_values();
    /// // Returns: [East, West]
    /// ```
    pub fn get_distinct_first_column_values(&self) -> Vec<SqlValue> {
        match self {
            IndexData::InMemory { data, .. } => {
                let mut distinct_values = Vec::new();
                let mut last_first_col: Option<SqlValue> = None;

                for key in data.keys() {
                    if let Some(first_val) = key.first() {
                        let is_new = match &last_first_col {
                            None => true,
                            Some(last) => first_val != last,
                        };
                        if is_new {
                            distinct_values.push(first_val.clone());
                            last_first_col = Some(first_val.clone());
                        }
                    }
                }

                distinct_values
            }
            IndexData::DiskBacked { btree, .. } => {
                // For disk-backed indexes, we need to scan all keys
                // This is less efficient but maintains correctness
                match acquire_btree_lock(btree) {
                    Ok(guard) => {
                        guard.get_distinct_first_column_values().unwrap_or_else(|_| vec![])
                    }
                    Err(e) => {
                        log::warn!(
                            "BTreeIndex lock acquisition failed in get_distinct_first_column_values: {}",
                            e
                        );
                        vec![]
                    }
                }
            }
            IndexData::IVFFlat { .. } | IndexData::Hnsw { .. } => {
                // Vector indexes don't support skip-scan
                vec![]
            }
        }
    }
}
