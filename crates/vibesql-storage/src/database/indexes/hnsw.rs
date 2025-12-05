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

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

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
}

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

    /// Remove a vector from the index
    ///
    /// Note: This is a lazy removal - we remove the node from the graph but don't
    /// restructure to maintain optimal connectivity. For heavy deletion workloads,
    /// consider rebuilding the index periodically.
    pub fn remove(&mut self, row_id: usize) {
        // Remove from vectors
        self.vectors.remove(&row_id);

        // Get node level
        let level = match self.node_levels.remove(&row_id) {
            Some(l) => l,
            None => return,
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
}
