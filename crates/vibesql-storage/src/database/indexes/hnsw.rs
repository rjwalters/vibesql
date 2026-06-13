//! HNSW (Hierarchical Navigable Small World) index for approximate nearest neighbor search
//!
//! This implements the HNSW algorithm from Malkov & Yashunin (2016):
//! - Multi-layer graph where each layer is a proximity graph
//! - Upper layers have fewer nodes for fast traversal
//! - Lower layers have more nodes for precision
//! - Greedy search from top layer down to bottom
//!
//! Parameters:
//! - `m`: Maximum number of connections per node (default 16)
//! - `ef_construction`: Size of dynamic candidate list during construction (default 64)
//! - `ef_search`: Size of dynamic candidate list during search (default 40)
//!
//! Advantages over IVFFlat:
//! - No training required (incremental inserts)
//! - Better recall at same speed
//! - Better suited for dynamic datasets
//! - Industry standard for production vector search

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
};

use vibesql_ast::VectorDistanceMetric;

/// HNSW index structure for approximate nearest neighbor search
#[derive(Debug, Clone)]
pub struct HnswIndex {
    /// All vectors stored in the index, indexed by row_id
    vectors: HashMap<usize, Vec<f64>>,
    /// Graph layers: layers[level][node_id] = Vec<neighbor_ids>
    /// Level 0 is the bottom (most connections), higher levels have fewer nodes
    layers: Vec<HashMap<usize, Vec<usize>>>,
    /// Maximum layer for each node
    node_levels: HashMap<usize, usize>,
    /// Entry point (node at highest layer)
    entry_point: Option<usize>,
    /// Maximum level in the graph
    max_level: usize,
    /// Distance metric used for similarity calculations
    metric: VectorDistanceMetric,
    /// Number of dimensions in each vector
    dimensions: usize,
    /// Maximum number of connections per node at layer 0
    m: usize,
    /// Maximum connections per node at layers > 0 (typically m / 2)
    m_max0: usize,
    /// Size of dynamic candidate list during construction
    ef_construction: usize,
    /// Size of dynamic candidate list during search
    ef_search: usize,
    /// Level multiplier for probabilistic layer assignment (1 / ln(m))
    ml: f64,
    /// Number of nodes removed since the last full rebuild (tombstone counter).
    ///
    /// Lazy `remove` unlinks a node but does not re-optimize the connectivity of
    /// the affected neighborhoods, so repeated deletes thin the proximity graph
    /// and degrade recall over time. This counter drives the auto-compaction
    /// trigger: once the deleted ratio exceeds `compaction_threshold`, the graph
    /// is rebuilt from the live vectors (which restores recall) and this resets
    /// to 0. Analogous to the table-level >50% compaction trigger.
    removed_count: usize,
    /// Deleted-ratio threshold that triggers an automatic graph rebuild.
    ///
    /// When `removed_count / (live + removed) > compaction_threshold`, `remove`
    /// rebuilds the graph from the current live vectors. Defaults to
    /// [`DEFAULT_COMPACTION_THRESHOLD`] (0.5), mirroring `Table` compaction.
    compaction_threshold: f64,
    /// When `false`, `remove` never auto-triggers a rebuild; callers must invoke
    /// [`HnswIndex::compact`] explicitly. Enabled by default.
    auto_compact: bool,
    /// Set to `true` whenever a full graph rebuild occurs (auto or explicit).
    ///
    /// Lets tests assert that compaction actually happened (a rebuild-happened
    /// flag) without depending on wall-clock timing. Cleared by
    /// [`HnswIndex::take_compacted`].
    compacted: bool,
}

/// Default deleted-ratio threshold that triggers an automatic HNSW rebuild.
///
/// Mirrors the table-level >50% compaction trigger: once more than half of the
/// nodes that have ever been live are tombstoned, the proximity graph is rebuilt
/// from the surviving vectors to restore recall.
pub const DEFAULT_COMPACTION_THRESHOLD: f64 = 0.5;

/// Result entry for nearest neighbor search
#[derive(Clone, Debug)]
struct Candidate {
    node_id: usize,
    distance: f64,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // For min-heap (smallest distance first)
        other.distance.partial_cmp(&self.distance).unwrap_or(Ordering::Equal)
    }
}

/// Max-heap variant for maintaining worst candidates
#[derive(Clone, Debug)]
struct MaxCandidate {
    node_id: usize,
    distance: f64,
}

impl PartialEq for MaxCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for MaxCandidate {}

impl PartialOrd for MaxCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MaxCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // For max-heap (largest distance first)
        self.distance.partial_cmp(&other.distance).unwrap_or(Ordering::Equal)
    }
}

impl HnswIndex {
    /// Create a new HNSW index with the specified parameters
    ///
    /// # Arguments
    /// * `dimensions` - Number of dimensions in each vector
    /// * `m` - Maximum connections per node (default 16)
    /// * `ef_construction` - Build-time search width (default 64)
    /// * `metric` - Distance metric to use
    pub fn new(
        dimensions: usize,
        m: u32,
        ef_construction: u32,
        metric: VectorDistanceMetric,
    ) -> Self {
        let m = m as usize;
        Self {
            vectors: HashMap::new(),
            layers: vec![HashMap::new()],
            node_levels: HashMap::new(),
            entry_point: None,
            max_level: 0,
            metric,
            dimensions,
            m,
            m_max0: m * 2, // Layer 0 can have more connections
            ef_construction: ef_construction as usize,
            ef_search: 40,             // Default ef_search
            ml: 1.0 / (m as f64).ln(), // Level multiplier
            removed_count: 0,
            compaction_threshold: DEFAULT_COMPACTION_THRESHOLD,
            auto_compact: true,
            compacted: false,
        }
    }

    /// Set the ef_search parameter for query-time accuracy/speed tradeoff
    pub fn set_ef_search(&mut self, ef_search: usize) {
        self.ef_search = ef_search.max(1);
    }

    /// Get the ef_search parameter
    pub fn ef_search(&self) -> usize {
        self.ef_search
    }

    /// Get the m parameter
    pub fn m(&self) -> usize {
        self.m
    }

    /// Get the ef_construction parameter
    pub fn ef_construction(&self) -> usize {
        self.ef_construction
    }

    /// Get the metric used by this index
    pub fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    /// Get the number of dimensions of vectors in this index
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Build the index from a set of vectors
    ///
    /// This inserts all vectors into the HNSW graph structure.
    pub fn build(&mut self, vectors: Vec<(usize, Vec<f64>)>) -> Result<(), String> {
        for (row_id, vector) in vectors {
            self.insert(row_id, vector)?;
        }
        Ok(())
    }

    /// Insert a single vector into the index
    pub fn insert(&mut self, row_id: usize, vector: Vec<f64>) -> Result<(), String> {
        if vector.len() != self.dimensions {
            return Err(format!(
                "Vector has {} dimensions, expected {}",
                vector.len(),
                self.dimensions
            ));
        }

        // Assign random level to new node
        let level = self.random_level();

        // Ensure we have enough layers
        while self.layers.len() <= level {
            self.layers.push(HashMap::new());
        }

        // Store vector
        self.vectors.insert(row_id, vector.clone());
        self.node_levels.insert(row_id, level);

        // Initialize empty neighbor lists at all levels for this node
        for l in 0..=level {
            self.layers[l].insert(row_id, Vec::new());
        }

        // Handle first insertion
        if self.entry_point.is_none() {
            self.entry_point = Some(row_id);
            self.max_level = level;
            return Ok(());
        }

        let entry_point = self.entry_point.unwrap();

        // Search from top layer down to level+1, using ef=1
        let mut ep = entry_point;
        for l in (level + 1..=self.max_level).rev() {
            let nearest = self.search_layer(&vector, ep, 1, l);
            if !nearest.is_empty() {
                ep = nearest[0].0;
            }
        }

        // Search and connect at levels from level down to 0
        for l in (0..=level.min(self.max_level)).rev() {
            let candidates = self.search_layer(&vector, ep, self.ef_construction, l);

            if !candidates.is_empty() {
                ep = candidates[0].0; // Update entry point for next layer
            }

            // Select neighbors using simple heuristic
            let m_max = if l == 0 { self.m_max0 } else { self.m };
            let neighbors = self.select_neighbors(&candidates, m_max);

            // Connect new node to neighbors
            if let Some(neighbor_list) = self.layers[l].get_mut(&row_id) {
                neighbor_list.clear();
                neighbor_list.extend(neighbors.iter().map(|n| n.0));
            }

            // Add reverse connections from neighbors to new node
            // First pass: add connections and collect those that need pruning
            let mut to_prune: Vec<usize> = Vec::new();
            for (neighbor_id, _) in &neighbors {
                if let Some(neighbor_neighbors) = self.layers[l].get_mut(neighbor_id) {
                    neighbor_neighbors.push(row_id);

                    // Mark for pruning if necessary
                    if neighbor_neighbors.len() > m_max {
                        to_prune.push(*neighbor_id);
                    }
                }
            }

            // Second pass: prune those that exceeded max connections
            // This avoids borrowing self.layers and self (for prune_neighbors) simultaneously
            for neighbor_id in to_prune {
                if let Some(neighbor_neighbors) = self.layers[l].get(&neighbor_id) {
                    let pruned = self.prune_neighbors(neighbor_id, neighbor_neighbors, l);
                    if let Some(nn) = self.layers[l].get_mut(&neighbor_id) {
                        *nn = pruned;
                    }
                }
            }
        }

        // Update entry point if new node is at higher level
        if level > self.max_level {
            self.entry_point = Some(row_id);
            self.max_level = level;
        }

        Ok(())
    }

    /// Remove a vector from the index.
    ///
    /// This is a *lazy unlink*: the node is dropped from every layer and its
    /// reverse edges are pruned, but the connectivity of the affected
    /// neighborhoods is not re-optimized. Removal keeps correctness (deleted
    /// nodes become unreachable and are never returned by `search`) but, over
    /// many deletes, thins the proximity graph and degrades recall.
    ///
    /// To counteract that, each removal bumps a tombstone counter; once the
    /// deleted ratio exceeds `compaction_threshold` the graph is automatically
    /// rebuilt from the surviving vectors (see [`HnswIndex::compact`]), which
    /// restores recall. Auto-compaction can be disabled with
    /// [`HnswIndex::set_auto_compact`] in favor of an explicit
    /// [`HnswIndex::compact`] call from the maintenance layer.
    pub fn remove(&mut self, row_id: usize) {
        if self.unlink(row_id) {
            self.removed_count += 1;
            if self.auto_compact && self.should_compact() {
                self.compact();
            }
        }
    }

    /// Unlink a single node from the graph without touching the tombstone
    /// counter or triggering compaction.
    ///
    /// Returns `true` if a node was actually removed (i.e. it existed).
    fn unlink(&mut self, row_id: usize) -> bool {
        // Remove from vectors
        let existed = self.vectors.remove(&row_id).is_some();

        // Get node level
        let level = match self.node_levels.remove(&row_id) {
            Some(l) => l,
            None => return existed,
        };

        // Remove from all layers
        for l in 0..=level {
            // Remove node's neighbor list
            self.layers[l].remove(&row_id);

            // Remove reverse connections from neighbors
            for neighbors in self.layers[l].values_mut() {
                neighbors.retain(|&id| id != row_id);
            }
        }

        // Update entry point if necessary
        if self.entry_point == Some(row_id) {
            // Find new entry point at highest level
            self.entry_point = None;
            for l in (0..self.layers.len()).rev() {
                if let Some(&new_ep) = self.layers[l].keys().next() {
                    self.entry_point = Some(new_ep);
                    self.max_level = l;
                    break;
                }
            }
        }

        existed
    }

    /// Whether the deleted ratio has crossed the compaction threshold.
    ///
    /// The ratio is `removed_count / (live + removed_count)` — the fraction of
    /// nodes that have ever been live which are now tombstoned. Mirrors
    /// `Table::should_compact`'s deleted-ratio test.
    pub fn should_compact(&self) -> bool {
        let total = self.vectors.len() + self.removed_count;
        if total == 0 {
            return false;
        }
        (self.removed_count as f64 / total as f64) > self.compaction_threshold
    }

    /// Rebuild the proximity graph from the current live vectors.
    ///
    /// Compaction discards the degraded graph (layers, neighbor lists, entry
    /// point, level assignments) and reconstructs it from scratch using only the
    /// surviving vectors via the standard [`HnswIndex::build`]/`insert` path —
    /// the same full reconstruction used to (re)build an index. This restores
    /// the small-world connectivity that lazy `remove` erodes, recovering recall.
    /// The tombstone counter is reset to 0 and the rebuild-happened flag is set.
    ///
    /// Transaction safety: `HnswIndex` is a deep-`Clone` value (all owned
    /// `HashMap`/`Vec` state), so the #5419 copy-on-write `Operations` snapshot
    /// captures a full copy of the pre-mutation index. A ROLLBACK restores that
    /// snapshot wholesale, undoing a compaction (or any mutation) — unlike the
    /// shallow-`Arc` disk-backed B-tree path, no separate undo log is required.
    pub fn compact(&mut self) {
        // Snapshot live vectors before tearing down the graph.
        let live: Vec<(usize, Vec<f64>)> =
            self.vectors.iter().map(|(&id, v)| (id, v.clone())).collect();

        // Reset graph state to an empty index, preserving configuration.
        self.vectors.clear();
        self.layers = vec![HashMap::new()];
        self.node_levels.clear();
        self.entry_point = None;
        self.max_level = 0;
        self.removed_count = 0;

        // Reconstruct from live vectors using the standard insertion path.
        for (row_id, vector) in live {
            // Dimensions were validated on the original insert; ignore errors so
            // compaction is infallible and never poisons the index.
            let _ = self.insert(row_id, vector);
        }

        self.compacted = true;
    }

    /// Number of tombstoned (removed) nodes since the last full rebuild.
    pub fn removed_count(&self) -> usize {
        self.removed_count
    }

    /// Current deleted-ratio compaction threshold.
    pub fn compaction_threshold(&self) -> f64 {
        self.compaction_threshold
    }

    /// Configure the deleted-ratio threshold that triggers auto-compaction.
    ///
    /// Clamped to `[0.0, 1.0]`. A value of `1.0` effectively disables the
    /// threshold trigger (the ratio can never strictly exceed 1.0).
    pub fn set_compaction_threshold(&mut self, threshold: f64) {
        self.compaction_threshold = threshold.clamp(0.0, 1.0);
    }

    /// Enable or disable automatic compaction on `remove`.
    ///
    /// When disabled, callers drive compaction explicitly via
    /// [`HnswIndex::compact`] (e.g. from a maintenance pass).
    pub fn set_auto_compact(&mut self, enabled: bool) {
        self.auto_compact = enabled;
    }

    /// Whether automatic compaction on `remove` is enabled.
    pub fn auto_compact(&self) -> bool {
        self.auto_compact
    }

    /// Take and clear the rebuild-happened flag.
    ///
    /// Returns `true` if a full rebuild (auto or explicit) occurred since the
    /// last call. Useful for tests/maintenance to observe that compaction fired
    /// without relying on timing.
    pub fn take_compacted(&mut self) -> bool {
        std::mem::replace(&mut self.compacted, false)
    }

    /// Perform approximate nearest neighbor search
    ///
    /// Returns the row IDs of the k nearest neighbors, ordered by distance
    pub fn search(&self, query: &[f64], k: usize) -> Result<Vec<(usize, f64)>, String> {
        if query.len() != self.dimensions {
            return Err(format!(
                "Query has {} dimensions, expected {}",
                query.len(),
                self.dimensions
            ));
        }

        if self.entry_point.is_none() || self.vectors.is_empty() {
            return Ok(Vec::new());
        }

        let entry_point = self.entry_point.unwrap();

        // Search from top layer down to layer 1 with ef=1
        let mut ep = entry_point;
        for l in (1..=self.max_level).rev() {
            let nearest = self.search_layer(query, ep, 1, l);
            if !nearest.is_empty() {
                ep = nearest[0].0;
            }
        }

        // Search layer 0 with ef=max(ef_search, k)
        let ef = self.ef_search.max(k);
        let candidates = self.search_layer(query, ep, ef, 0);

        // Return top k results
        Ok(candidates.into_iter().take(k).collect())
    }

    /// Search a single layer starting from entry point
    ///
    /// Returns `ef` nearest neighbors from this layer
    fn search_layer(
        &self,
        query: &[f64],
        entry_point: usize,
        ef: usize,
        level: usize,
    ) -> Vec<(usize, f64)> {
        let mut visited = HashSet::new();
        let mut candidates: BinaryHeap<Candidate> = BinaryHeap::new(); // Min-heap
        let mut results: BinaryHeap<MaxCandidate> = BinaryHeap::new(); // Max-heap for worst

        // Get entry point distance
        let ep_vec = match self.vectors.get(&entry_point) {
            Some(v) => v,
            None => return Vec::new(),
        };
        let ep_dist = self.compute_distance(query, ep_vec);

        visited.insert(entry_point);
        candidates.push(Candidate { node_id: entry_point, distance: ep_dist });
        results.push(MaxCandidate { node_id: entry_point, distance: ep_dist });

        while let Some(current) = candidates.pop() {
            // Get worst distance in results
            let worst_dist = results.peek().map(|c| c.distance).unwrap_or(f64::INFINITY);

            // If current candidate is worse than worst result, we're done
            if current.distance > worst_dist {
                break;
            }

            // Get neighbors at this level
            let neighbors = match self.layers.get(level).and_then(|l| l.get(&current.node_id)) {
                Some(n) => n,
                None => continue,
            };

            for &neighbor_id in neighbors {
                if visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);

                let neighbor_vec = match self.vectors.get(&neighbor_id) {
                    Some(v) => v,
                    None => continue,
                };
                let neighbor_dist = self.compute_distance(query, neighbor_vec);

                let worst_dist = results.peek().map(|c| c.distance).unwrap_or(f64::INFINITY);

                if results.len() < ef || neighbor_dist < worst_dist {
                    candidates.push(Candidate { node_id: neighbor_id, distance: neighbor_dist });
                    results.push(MaxCandidate { node_id: neighbor_id, distance: neighbor_dist });

                    // Keep only ef results
                    while results.len() > ef {
                        results.pop();
                    }
                }
            }
        }

        // Convert max-heap to sorted results (smallest distance first)
        let mut result_vec: Vec<(usize, f64)> =
            results.into_iter().map(|c| (c.node_id, c.distance)).collect();
        result_vec.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        result_vec
    }

    /// Select neighbors from candidates (simple heuristic)
    fn select_neighbors(&self, candidates: &[(usize, f64)], m: usize) -> Vec<(usize, f64)> {
        // Simple: take the m closest
        candidates.iter().take(m).cloned().collect()
    }

    /// Prune neighbors list to maintain max connections
    fn prune_neighbors(&self, node_id: usize, neighbors: &[usize], _level: usize) -> Vec<usize> {
        let node_vec = match self.vectors.get(&node_id) {
            Some(v) => v,
            None => return neighbors.to_vec(),
        };

        let m_max = if _level == 0 { self.m_max0 } else { self.m };

        // Compute distances and sort
        let mut with_dist: Vec<(usize, f64)> = neighbors
            .iter()
            .filter_map(|&n| self.vectors.get(&n).map(|v| (n, self.compute_distance(node_vec, v))))
            .collect();

        with_dist.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        // Keep only m_max closest
        with_dist.into_iter().take(m_max).map(|(id, _)| id).collect()
    }

    /// Generate random level for new node using exponential distribution
    fn random_level(&self) -> usize {
        // Use simple random generation
        // In production, should use a proper RNG
        let rand_val: f64 = rand::random();
        let level = (-rand_val.ln() * self.ml).floor() as usize;
        level.min(16) // Cap at reasonable max level
    }

    /// Compute distance between two vectors based on the configured metric
    fn compute_distance(&self, a: &[f64], b: &[f64]) -> f64 {
        match self.metric {
            VectorDistanceMetric::L2 => self.l2_distance(a, b),
            VectorDistanceMetric::Cosine => self.cosine_distance(a, b),
            VectorDistanceMetric::InnerProduct => self.inner_product_distance(a, b),
        }
    }

    /// Compute L2 (Euclidean) distance
    #[inline]
    fn l2_distance(&self, a: &[f64], b: &[f64]) -> f64 {
        a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum::<f64>().sqrt()
    }

    /// Compute cosine distance (1 - cosine similarity)
    #[inline]
    fn cosine_distance(&self, a: &[f64], b: &[f64]) -> f64 {
        let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f64 = a.iter().map(|x| x.powi(2)).sum::<f64>().sqrt();
        let norm_b: f64 = b.iter().map(|x| x.powi(2)).sum::<f64>().sqrt();

        if norm_a == 0.0 || norm_b == 0.0 {
            1.0 // Max distance for zero vectors
        } else {
            1.0 - (dot / (norm_a * norm_b))
        }
    }

    /// Compute inner product distance (negative inner product for distance ordering)
    #[inline]
    fn inner_product_distance(&self, a: &[f64], b: &[f64]) -> f64 {
        // For inner product, larger values are more similar
        // So we negate to make it a distance (smaller = more similar)
        -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f64>()
    }

    /// Get total number of vectors in the index
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Get all row IDs stored in the index
    pub fn all_row_ids(&self) -> Vec<usize> {
        self.vectors.keys().cloned().collect()
    }

    /// Get the number of layers in the graph
    pub fn num_layers(&self) -> usize {
        self.layers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_basic() {
        let mut index = HnswIndex::new(3, 4, 16, VectorDistanceMetric::L2);

        // Build index with some vectors
        let vectors = vec![
            (0, vec![1.0, 0.0, 0.0]),
            (1, vec![0.0, 1.0, 0.0]),
            (2, vec![0.0, 0.0, 1.0]),
            (3, vec![1.0, 1.0, 0.0]),
        ];

        index.build(vectors).unwrap();
        assert_eq!(index.len(), 4);
    }

    #[test]
    fn test_hnsw_search_l2() {
        let mut index = HnswIndex::new(2, 4, 16, VectorDistanceMetric::L2);

        let vectors = vec![
            (0, vec![0.0, 0.0]),
            (1, vec![1.0, 0.0]),
            (2, vec![0.0, 1.0]),
            (3, vec![1.0, 1.0]),
        ];

        index.build(vectors).unwrap();

        // Search near origin - should find (0,0) first
        let results = index.search(&[0.1, 0.1], 2).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 0); // (0,0) should be closest
    }

    #[test]
    fn test_hnsw_search_cosine() {
        let mut index = HnswIndex::new(2, 4, 16, VectorDistanceMetric::Cosine);

        let vectors = vec![
            (0, vec![1.0, 0.0]),
            (1, vec![0.0, 1.0]),
            (2, vec![0.707, 0.707]), // 45 degrees
        ];

        index.build(vectors).unwrap();

        // Search for vector similar to (1, 0) - should find it first
        let results = index.search(&[1.0, 0.1], 2).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_hnsw_insert_remove() {
        let mut index = HnswIndex::new(2, 4, 16, VectorDistanceMetric::L2);

        let vectors = vec![(0, vec![0.0, 0.0]), (1, vec![1.0, 1.0])];

        index.build(vectors).unwrap();
        assert_eq!(index.len(), 2);

        // Insert new vector
        index.insert(2, vec![0.5, 0.5]).unwrap();
        assert_eq!(index.len(), 3);

        // Remove vector
        index.remove(1);
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn test_hnsw_empty_index() {
        let index = HnswIndex::new(3, 4, 16, VectorDistanceMetric::L2);

        assert!(index.is_empty());

        let results = index.search(&[1.0, 2.0, 3.0], 5).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_hnsw_dimension_mismatch() {
        let mut index = HnswIndex::new(3, 4, 16, VectorDistanceMetric::L2);

        // Insert vector with wrong dimensions
        let result = index.insert(0, vec![1.0, 2.0]);
        assert!(result.is_err());

        // Search with wrong dimensions
        index.insert(0, vec![1.0, 2.0, 3.0]).unwrap();
        let result = index.search(&[1.0, 2.0], 5);
        assert!(result.is_err());
    }

    #[test]
    fn test_hnsw_ef_search() {
        let mut index = HnswIndex::new(2, 4, 16, VectorDistanceMetric::L2);
        assert_eq!(index.ef_search(), 40); // Default

        index.set_ef_search(100);
        assert_eq!(index.ef_search(), 100);

        // Minimum ef_search is 1
        index.set_ef_search(0);
        assert_eq!(index.ef_search(), 1);
    }

    #[test]
    fn test_hnsw_larger_dataset() {
        let mut index = HnswIndex::new(4, 8, 32, VectorDistanceMetric::L2);

        // Create 100 vectors
        let vectors: Vec<(usize, Vec<f64>)> = (0..100)
            .map(|i| {
                let x = (i % 10) as f64;
                let y = (i / 10) as f64;
                (i, vec![x, y, x * 0.1, y * 0.1])
            })
            .collect();

        index.build(vectors).unwrap();
        assert_eq!(index.len(), 100);

        // Search for something near (5, 5, 0.5, 0.5)
        let results = index.search(&[5.0, 5.0, 0.5, 0.5], 5).unwrap();
        assert_eq!(results.len(), 5);

        // First result should be (5, 5) which is row 55
        assert_eq!(results[0].0, 55);
    }

    // ---- Compaction / recall regression tests (#5454) ----

    /// Deterministic pseudo-random generator (xorshift64) so the recall
    /// dataset is reproducible across runs without depending on `rand`.
    struct DetRng(u64);
    impl DetRng {
        fn new(seed: u64) -> Self {
            DetRng(seed.max(1))
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        /// Uniform f64 in [0, 1).
        fn next_f64(&mut self) -> f64 {
            (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    /// Build a deterministic dataset of `n` vectors of `dim` dimensions.
    fn deterministic_dataset(n: usize, dim: usize, seed: u64) -> Vec<(usize, Vec<f64>)> {
        let mut rng = DetRng::new(seed);
        (0..n)
            .map(|i| {
                let v: Vec<f64> = (0..dim).map(|_| rng.next_f64()).collect();
                (i, v)
            })
            .collect()
    }

    /// Brute-force ground-truth: the `k` nearest *live* row ids to `query`
    /// (L2), restricted to `live`.
    fn brute_force_knn(
        dataset: &[(usize, Vec<f64>)],
        live: &HashSet<usize>,
        query: &[f64],
        k: usize,
    ) -> Vec<usize> {
        let mut scored: Vec<(usize, f64)> = dataset
            .iter()
            .filter(|(id, _)| live.contains(id))
            .map(|(id, v)| {
                let d: f64 =
                    query.iter().zip(v.iter()).map(|(a, b)| (a - b).powi(2)).sum::<f64>().sqrt();
                (*id, d)
            })
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        scored.into_iter().take(k).map(|(id, _)| id).collect()
    }

    /// Average recall@k of `index` over `num_queries` queries drawn from the
    /// live vectors, against brute-force ground truth.
    fn measure_recall(
        index: &HnswIndex,
        dataset: &[(usize, Vec<f64>)],
        live: &HashSet<usize>,
        k: usize,
        num_queries: usize,
    ) -> f64 {
        let live_ids: Vec<usize> =
            dataset.iter().map(|(id, _)| *id).filter(|id| live.contains(id)).collect();
        if live_ids.is_empty() {
            return 1.0;
        }

        let mut total_hits = 0usize;
        let mut total_truth = 0usize;
        for qi in 0..num_queries {
            // Use an existing live vector as the query (its own neighborhood).
            let qid = live_ids[qi % live_ids.len()];
            let query = &dataset[qid].1;

            let truth = brute_force_knn(dataset, live, query, k);
            let truth_set: HashSet<usize> = truth.iter().copied().collect();

            let approx = index.search(query, k).unwrap();
            let hits = approx.iter().filter(|(id, _)| truth_set.contains(id)).count();

            total_hits += hits;
            total_truth += truth.len();
        }

        if total_truth == 0 {
            1.0
        } else {
            total_hits as f64 / total_truth as f64
        }
    }

    #[test]
    fn test_recall_degrades_without_compaction_then_restored() {
        const N: usize = 1500;
        const DIM: usize = 16;
        const K: usize = 10;
        const QUERIES: usize = 100;
        // A small ef_search makes the graph's navigability (small-world
        // property) — exactly what lazy unlink erodes — the limiting factor for
        // recall, so degradation is observable rather than masked by an
        // over-wide search beam.
        const EF_SEARCH: usize = 12;

        let dataset = deterministic_dataset(N, DIM, 0xC0FFEE);

        // Delete a large, *scattered* fraction so removed nodes (including
        // graph hubs / entry-point candidates) are interleaved with survivors,
        // fragmenting the survivor subgraph the way a real delete-heavy
        // workload does. Keep id `i` iff `i % 5 == 0` -> delete 80%.
        let live: HashSet<usize> = (0..N).filter(|i| i % 5 == 0).collect();
        let to_delete: Vec<usize> = (0..N).filter(|i| i % 5 != 0).collect();

        // --- Index A: deletes WITHOUT compaction (auto disabled) ---
        let mut idx_no_compact = HnswIndex::new(DIM, 16, 64, VectorDistanceMetric::L2);
        idx_no_compact.set_ef_search(EF_SEARCH);
        idx_no_compact.set_auto_compact(false);
        idx_no_compact.build(dataset.clone()).unwrap();
        for &id in &to_delete {
            idx_no_compact.remove(id);
        }
        assert!(!idx_no_compact.take_compacted(), "auto-compaction must be disabled here");
        assert_eq!(idx_no_compact.len(), live.len());

        let recall_degraded = measure_recall(&idx_no_compact, &dataset, &live, K, QUERIES);

        // --- Index B: same deletes then explicit compaction ---
        let mut idx_compact = HnswIndex::new(DIM, 16, 64, VectorDistanceMetric::L2);
        idx_compact.set_ef_search(EF_SEARCH);
        idx_compact.set_auto_compact(false);
        idx_compact.build(dataset.clone()).unwrap();
        for &id in &to_delete {
            idx_compact.remove(id);
        }
        idx_compact.compact();
        assert!(idx_compact.take_compacted(), "compact() must set the rebuild flag");
        assert_eq!(idx_compact.len(), live.len());
        assert_eq!(idx_compact.removed_count(), 0, "compaction resets the tombstone counter");

        let recall_compacted = measure_recall(&idx_compact, &dataset, &live, K, QUERIES);

        // --- Reference: a freshly-built index over only the live vectors. ---
        // Acceptance criterion (issue): post-compaction recall stays within an
        // acceptable bound of a freshly-built index.
        let mut idx_fresh = HnswIndex::new(DIM, 16, 64, VectorDistanceMetric::L2);
        idx_fresh.set_ef_search(EF_SEARCH);
        let fresh_vectors: Vec<(usize, Vec<f64>)> =
            dataset.iter().filter(|(id, _)| live.contains(id)).cloned().collect();
        idx_fresh.build(fresh_vectors).unwrap();
        let recall_fresh = measure_recall(&idx_fresh, &dataset, &live, K, QUERIES);

        // Correctness invariant for BOTH degraded and compacted indexes: no
        // deleted id ever returned, and every live vector is findable.
        for &id in live.iter().take(100) {
            let q = &dataset[id].1;
            let res_a = idx_no_compact.search(q, K).unwrap();
            let res_b = idx_compact.search(q, K).unwrap();
            assert!(res_a.iter().all(|(r, _)| live.contains(r)), "deleted id leaked (no-compact)");
            assert!(res_b.iter().all(|(r, _)| live.contains(r)), "deleted id leaked (compact)");
            assert!(res_b.iter().any(|(r, _)| *r == id), "live vector not findable post-compact");
        }

        eprintln!(
            "recall@{K} (ef_search={EF_SEARCH}, {} live / {} deleted): \
             degraded(no-compact)={:.3}  compacted={:.3}  fresh-rebuild={:.3}",
            live.len(),
            to_delete.len(),
            recall_degraded,
            recall_compacted,
            recall_fresh
        );

        // The feature does its job:
        // 1. Lazy deletes measurably degrade recall.
        assert!(
            recall_degraded < recall_fresh - 0.02,
            "expected measurable recall degradation: degraded={:.3} not < fresh={:.3}",
            recall_degraded,
            recall_fresh
        );
        // 2. Compaction restores recall to (essentially) the fresh-rebuild level.
        assert!(
            recall_compacted >= recall_fresh - 0.02,
            "compaction did not restore recall: compacted={:.3} fresh={:.3}",
            recall_compacted,
            recall_fresh
        );
        // 3. Compaction is a strict improvement over the degraded graph.
        assert!(
            recall_compacted > recall_degraded,
            "compaction must improve recall: {:.3} <= {:.3}",
            recall_compacted,
            recall_degraded
        );
    }

    #[test]
    fn test_auto_compaction_triggers_at_threshold() {
        const N: usize = 100;
        const DIM: usize = 4;

        let dataset = deterministic_dataset(N, DIM, 42);
        let mut index = HnswIndex::new(DIM, 16, 64, VectorDistanceMetric::L2);
        // Default: auto_compact = true, threshold = 0.5.
        assert!(index.auto_compact());
        assert_eq!(index.compaction_threshold(), DEFAULT_COMPACTION_THRESHOLD);
        index.build(dataset.clone()).unwrap();

        // Remove just under half (49 of 100): 49/100 = 0.49 <= 0.5 -> no rebuild.
        for id in 0..49 {
            index.remove(id);
        }
        assert!(!index.take_compacted(), "should not compact below threshold");
        assert_eq!(index.removed_count(), 49);

        // One more delete: 50/100 = 0.50, still not > 0.5 -> no rebuild yet.
        index.remove(49);
        assert!(!index.take_compacted(), "ratio == threshold must not trigger");

        // Next delete: 51/101 (live=50,removed=51) > 0.5 -> rebuild fires.
        index.remove(50);
        assert!(index.take_compacted(), "should auto-compact once ratio exceeds threshold");
        assert_eq!(index.removed_count(), 0, "rebuild resets tombstone counter");
        assert_eq!(index.len(), 49, "live vectors preserved across rebuild");
    }

    #[test]
    fn test_post_compaction_search_correctness() {
        const N: usize = 200;
        const DIM: usize = 6;

        let dataset = deterministic_dataset(N, DIM, 7);
        let mut index = HnswIndex::new(DIM, 16, 64, VectorDistanceMetric::L2);
        index.set_auto_compact(false);
        index.build(dataset.clone()).unwrap();

        // Delete every even id (including id 0 / likely entry point candidates).
        let mut live = HashSet::new();
        for id in 0..N {
            if id % 2 == 0 {
                index.remove(id);
            } else {
                live.insert(id);
            }
        }
        index.compact();

        assert_eq!(index.len(), live.len());
        // Every live vector finds itself; no deleted (even) id ever appears.
        for &id in &live {
            let q = &dataset[id].1;
            let res = index.search(q, 5).unwrap();
            assert!(res.iter().any(|(r, _)| *r == id), "live id {id} not findable");
            assert!(res.iter().all(|(r, _)| *r % 2 == 1), "deleted (even) id returned");
        }
    }

    #[test]
    fn test_explicit_compact_vs_auto_disabled() {
        let dataset = deterministic_dataset(60, 4, 99);
        let mut index = HnswIndex::new(4, 16, 64, VectorDistanceMetric::L2);
        index.set_auto_compact(false);
        index.build(dataset).unwrap();

        // Delete well past threshold but with auto disabled: no rebuild.
        for id in 0..50 {
            index.remove(id);
        }
        assert!(!index.take_compacted());
        assert_eq!(index.removed_count(), 50);
        assert!(index.should_compact(), "ratio is above threshold");

        // Maintenance layer drives it explicitly.
        index.compact();
        assert!(index.take_compacted());
        assert_eq!(index.removed_count(), 0);
        assert_eq!(index.len(), 10);
    }

    #[test]
    fn test_compaction_rolls_back_via_cow_clone() {
        // HNSW is a deep-Clone value, so the #5419 COW Operations snapshot
        // captures a full copy. Simulate a transaction: snapshot (clone),
        // mutate+compact, then ROLLBACK by restoring the snapshot.
        let dataset = deterministic_dataset(80, 4, 0xABCD);
        let mut index = HnswIndex::new(4, 16, 64, VectorDistanceMetric::L2);
        index.build(dataset.clone()).unwrap();
        let original_len = index.len();
        let original_removed = index.removed_count();

        // Arm COW snapshot (deep clone of pre-mutation state).
        let snapshot = index.clone();

        // Mutate within the "transaction": delete enough to force compaction.
        for id in 0..60 {
            index.remove(id);
        }
        index.compact();
        assert!(index.take_compacted());
        assert_ne!(index.len(), original_len, "transaction changed live count");

        // ROLLBACK: restore the COW snapshot.
        index = snapshot;

        // State is fully restored — including all originally-deleted vectors.
        assert_eq!(index.len(), original_len);
        assert_eq!(index.removed_count(), original_removed);
        for id in 0..60 {
            let q = &dataset[id].1;
            let res = index.search(q, 3).unwrap();
            assert!(
                res.iter().any(|(r, _)| *r == id),
                "rolled-back vector {id} should be searchable again"
            );
        }
    }
}
