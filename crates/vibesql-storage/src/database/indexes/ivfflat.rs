//! IVFFlat (Inverted File with Flat quantization) index for approximate nearest neighbor search
//!
//! This implements the IVFFlat algorithm:
//! 1. Partition vectors into clusters using k-means
//! 2. At query time, find the nearest cluster(s) and search only those
//!
//! Parameters:
//! - `lists`: Number of clusters (default 100)
//! - `probes`: Number of clusters to search at query time (default 1)

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use vibesql_ast::VectorDistanceMetric;

/// IVFFlat index structure for approximate nearest neighbor search
#[derive(Debug, Clone)]
pub struct IVFFlatIndex {
    /// Cluster centroids - shape: (num_lists, dimensions)
    centroids: Vec<Vec<f64>>,
    /// Inverted lists - for each cluster, store (row_id, vector) pairs
    inverted_lists: Vec<Vec<(usize, Vec<f64>)>>,
    /// Distance metric used for similarity calculations
    metric: VectorDistanceMetric,
    /// Number of dimensions in each vector
    dimensions: usize,
    /// Number of probes (clusters to search) at query time
    probes: usize,
    /// Whether the index has been trained (has valid centroids)
    trained: bool,
}

/// Result entry for nearest neighbor search
#[derive(Clone)]
struct SearchResult {
    row_id: usize,
    distance: f64,
}

impl PartialEq for SearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for SearchResult {}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for max-heap (we want smallest distances at top)
        other.distance.partial_cmp(&self.distance).unwrap_or(Ordering::Equal)
    }
}

impl IVFFlatIndex {
    /// Create a new IVFFlat index with the specified parameters
    pub fn new(dimensions: usize, num_lists: u32, metric: VectorDistanceMetric) -> Self {
        Self {
            centroids: Vec::with_capacity(num_lists as usize),
            inverted_lists: vec![Vec::new(); num_lists as usize],
            metric,
            dimensions,
            probes: 1, // Default to searching 1 cluster
            trained: false,
        }
    }

    /// Set the number of probes for query time
    pub fn set_probes(&mut self, probes: usize) {
        self.probes = probes.max(1);
    }

    /// Get the number of probes
    pub fn probes(&self) -> usize {
        self.probes
    }

    /// Get the metric used by this index
    pub fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    /// Get the number of lists (clusters)
    pub fn num_lists(&self) -> usize {
        self.inverted_lists.len()
    }

    /// Check if the index is trained
    pub fn is_trained(&self) -> bool {
        self.trained
    }

    /// Build/train the index from a set of vectors
    ///
    /// This performs k-means clustering to find centroids, then assigns
    /// each vector to its nearest cluster.
    pub fn build(&mut self, vectors: Vec<(usize, Vec<f64>)>) -> Result<(), String> {
        if vectors.is_empty() {
            self.trained = true;
            return Ok(());
        }

        let num_lists = self.inverted_lists.len();

        // Validate dimensions
        for (row_id, vec) in &vectors {
            if vec.len() != self.dimensions {
                return Err(format!(
                    "Vector at row {} has {} dimensions, expected {}",
                    row_id,
                    vec.len(),
                    self.dimensions
                ));
            }
        }

        // Run k-means to find centroids
        self.centroids = self.kmeans_clustering(&vectors, num_lists)?;

        // Clear existing inverted lists
        for list in &mut self.inverted_lists {
            list.clear();
        }

        // Assign each vector to its nearest centroid
        for (row_id, vector) in vectors {
            let nearest_list = self.find_nearest_centroid(&vector);
            self.inverted_lists[nearest_list].push((row_id, vector));
        }

        self.trained = true;
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

        if !self.trained || self.centroids.is_empty() {
            // Index not trained yet - just store in first list
            // The index should be rebuilt after sufficient data is added
            self.inverted_lists[0].push((row_id, vector));
        } else {
            let nearest_list = self.find_nearest_centroid(&vector);
            self.inverted_lists[nearest_list].push((row_id, vector));
        }

        Ok(())
    }

    /// Remove a vector from the index
    pub fn remove(&mut self, row_id: usize) {
        for list in &mut self.inverted_lists {
            list.retain(|(id, _)| *id != row_id);
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

        if !self.trained || self.centroids.is_empty() {
            // Not trained - do exhaustive search on all vectors
            return self.exhaustive_search(query, k);
        }

        // Find the nearest `probes` centroids
        let nearest_centroids = self.find_nearest_centroids(query, self.probes);

        // Search only the selected lists
        let mut heap: BinaryHeap<SearchResult> = BinaryHeap::new();

        for list_idx in nearest_centroids {
            for (row_id, vector) in &self.inverted_lists[list_idx] {
                let distance = self.compute_distance(query, vector);

                if heap.len() < k {
                    heap.push(SearchResult { row_id: *row_id, distance });
                } else if let Some(worst) = heap.peek() {
                    if distance < worst.distance {
                        heap.pop();
                        heap.push(SearchResult { row_id: *row_id, distance });
                    }
                }
            }
        }

        // Convert heap to sorted results
        let mut results: Vec<_> = heap.into_iter().map(|r| (r.row_id, r.distance)).collect();
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        Ok(results)
    }

    /// Perform exhaustive search (when index is not trained)
    fn exhaustive_search(&self, query: &[f64], k: usize) -> Result<Vec<(usize, f64)>, String> {
        let mut heap: BinaryHeap<SearchResult> = BinaryHeap::new();

        for list in &self.inverted_lists {
            for (row_id, vector) in list {
                let distance = self.compute_distance(query, vector);

                if heap.len() < k {
                    heap.push(SearchResult { row_id: *row_id, distance });
                } else if let Some(worst) = heap.peek() {
                    if distance < worst.distance {
                        heap.pop();
                        heap.push(SearchResult { row_id: *row_id, distance });
                    }
                }
            }
        }

        let mut results: Vec<_> = heap.into_iter().map(|r| (r.row_id, r.distance)).collect();
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        Ok(results)
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
    fn l2_distance(&self, a: &[f64], b: &[f64]) -> f64 {
        a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum::<f64>().sqrt()
    }

    /// Compute cosine distance (1 - cosine similarity)
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
    fn inner_product_distance(&self, a: &[f64], b: &[f64]) -> f64 {
        // For inner product, larger values are more similar
        // So we negate to make it a distance (smaller = more similar)
        -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f64>()
    }

    /// Find the index of the nearest centroid to a vector
    fn find_nearest_centroid(&self, vector: &[f64]) -> usize {
        let mut best_idx = 0;
        let mut best_dist = f64::INFINITY;

        for (idx, centroid) in self.centroids.iter().enumerate() {
            let dist = self.compute_distance(vector, centroid);
            if dist < best_dist {
                best_dist = dist;
                best_idx = idx;
            }
        }

        best_idx
    }

    /// Find the indices of the k nearest centroids
    fn find_nearest_centroids(&self, vector: &[f64], k: usize) -> Vec<usize> {
        let mut distances: Vec<(usize, f64)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(idx, centroid)| (idx, self.compute_distance(vector, centroid)))
            .collect();

        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        distances.into_iter().take(k.min(self.centroids.len())).map(|(idx, _)| idx).collect()
    }

    /// Perform k-means clustering to find centroids
    fn kmeans_clustering(
        &self,
        vectors: &[(usize, Vec<f64>)],
        k: usize,
    ) -> Result<Vec<Vec<f64>>, String> {
        let n = vectors.len();

        if n == 0 {
            return Ok(Vec::new());
        }

        // Use fewer clusters if we don't have enough data points
        let actual_k = k.min(n);

        if actual_k == 0 {
            return Ok(Vec::new());
        }

        // Initialize centroids using k-means++ initialization
        let mut centroids = self.kmeans_plusplus_init(vectors, actual_k);

        // Run Lloyd's algorithm for a fixed number of iterations
        const MAX_ITERATIONS: usize = 50;
        const CONVERGENCE_THRESHOLD: f64 = 1e-6;

        for _ in 0..MAX_ITERATIONS {
            // Assign each vector to nearest centroid
            let mut assignments = vec![Vec::new(); actual_k];

            for (_, vector) in vectors {
                let mut best_idx = 0;
                let mut best_dist = f64::INFINITY;

                for (idx, centroid) in centroids.iter().enumerate() {
                    let dist = self.compute_distance(vector, centroid);
                    if dist < best_dist {
                        best_dist = dist;
                        best_idx = idx;
                    }
                }

                assignments[best_idx].push(vector);
            }

            // Compute new centroids
            let mut new_centroids = Vec::with_capacity(actual_k);
            let mut max_shift = 0.0f64;

            for (idx, assigned) in assignments.iter().enumerate() {
                if assigned.is_empty() {
                    // Keep old centroid if no vectors assigned
                    new_centroids.push(centroids[idx].clone());
                } else {
                    // Compute mean of assigned vectors
                    let mut new_centroid = vec![0.0; self.dimensions];
                    for vector in assigned {
                        for (i, val) in vector.iter().enumerate() {
                            new_centroid[i] += val;
                        }
                    }
                    for val in &mut new_centroid {
                        *val /= assigned.len() as f64;
                    }

                    // Track convergence
                    let shift = self.l2_distance(&centroids[idx], &new_centroid);
                    max_shift = max_shift.max(shift);

                    new_centroids.push(new_centroid);
                }
            }

            centroids = new_centroids;

            // Check for convergence
            if max_shift < CONVERGENCE_THRESHOLD {
                break;
            }
        }

        Ok(centroids)
    }

    /// Initialize centroids using k-means++ algorithm
    fn kmeans_plusplus_init(&self, vectors: &[(usize, Vec<f64>)], k: usize) -> Vec<Vec<f64>> {
        let n = vectors.len();
        if n == 0 || k == 0 {
            return Vec::new();
        }

        let mut centroids = Vec::with_capacity(k);
        let mut used_indices = std::collections::HashSet::new();

        // Choose first centroid randomly (use first vector for determinism)
        centroids.push(vectors[0].1.clone());
        used_indices.insert(0);

        // Choose remaining centroids
        for _ in 1..k {
            // Compute squared distances to nearest centroid for each point
            let distances: Vec<f64> = vectors
                .iter()
                .map(|(_, v)| {
                    centroids.iter().map(|c| self.l2_distance(v, c)).fold(f64::INFINITY, f64::min)
                })
                .collect();

            // Find the point with maximum distance to nearest centroid
            let (max_idx, _) = distances
                .iter()
                .enumerate()
                .filter(|(idx, _)| !used_indices.contains(idx))
                .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                .unwrap_or((0, &0.0));

            centroids.push(vectors[max_idx].1.clone());
            used_indices.insert(max_idx);
        }

        centroids
    }

    /// Get total number of vectors in the index
    pub fn len(&self) -> usize {
        self.inverted_lists.iter().map(|l| l.len()).sum()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.inverted_lists.iter().all(|l| l.is_empty())
    }

    /// Get all row IDs stored in the index
    pub fn all_row_ids(&self) -> Vec<usize> {
        self.inverted_lists.iter().flat_map(|list| list.iter().map(|(row_id, _)| *row_id)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ivfflat_basic() {
        let mut index = IVFFlatIndex::new(3, 2, VectorDistanceMetric::L2);

        // Build index with some vectors
        let vectors = vec![
            (0, vec![1.0, 0.0, 0.0]),
            (1, vec![0.0, 1.0, 0.0]),
            (2, vec![0.0, 0.0, 1.0]),
            (3, vec![1.0, 1.0, 0.0]),
        ];

        index.build(vectors).unwrap();
        assert!(index.is_trained());
        assert_eq!(index.len(), 4);
    }

    #[test]
    fn test_ivfflat_search_l2() {
        let mut index = IVFFlatIndex::new(2, 2, VectorDistanceMetric::L2);

        let vectors = vec![
            (0, vec![0.0, 0.0]),
            (1, vec![1.0, 0.0]),
            (2, vec![0.0, 1.0]),
            (3, vec![1.0, 1.0]),
        ];

        index.build(vectors).unwrap();
        index.set_probes(2); // Search all clusters for exact results

        // Search near origin - should find (0,0) first
        let results = index.search(&[0.1, 0.1], 2).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 0); // (0,0) should be closest
    }

    #[test]
    fn test_ivfflat_search_cosine() {
        let mut index = IVFFlatIndex::new(2, 2, VectorDistanceMetric::Cosine);

        let vectors = vec![
            (0, vec![1.0, 0.0]),
            (1, vec![0.0, 1.0]),
            (2, vec![0.707, 0.707]), // 45 degrees
        ];

        index.build(vectors).unwrap();
        index.set_probes(2);

        // Search for vector similar to (1, 0) - should find it first
        let results = index.search(&[1.0, 0.1], 2).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn test_ivfflat_insert_remove() {
        let mut index = IVFFlatIndex::new(2, 2, VectorDistanceMetric::L2);

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
    fn test_ivfflat_empty_index() {
        let mut index = IVFFlatIndex::new(3, 4, VectorDistanceMetric::L2);

        index.build(vec![]).unwrap();
        assert!(index.is_trained());
        assert!(index.is_empty());

        let results = index.search(&[1.0, 2.0, 3.0], 5).unwrap();
        assert!(results.is_empty());
    }
}
