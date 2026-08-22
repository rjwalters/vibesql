//! HNSW recall@k quality benchmark
//!
//! Promotes the in-test recall measurement from
//! `test_recall_degrades_without_compaction_then_restored` (#5454 / PR #5461)
//! into the benchmark suite so HNSW recall under delete-heavy workloads is
//! tracked over time alongside the TPC / sysbench suites and recall
//! regressions are caught automatically. See #5466.
//!
//! Unlike the latency-oriented benchmarks, this benchmark reports a *quality*
//! metric: recall@k (the fraction of the true k-nearest neighbours an HNSW
//! search returns) versus brute-force ground truth. It measures three states
//! across a sweep of `ef_search` and delete-ratio so degradation and
//! restoration are both visible:
//!
//!   - `fresh`             : index built over only the live vectors (the best-case reference an ANN
//!     index can achieve).
//!   - `degraded`          : index built over the full dataset, then a fraction of vectors lazily
//!     deleted *without* compaction — tombstones erode the small-world graph and recall drops.
//!   - `compacted`         : same deletes, then `compact()` rebuilds the graph from survivors —
//!     recall is restored to ~`fresh`.
//!
//! The dataset is fully deterministic (seeded xorshift64, no `rand`) so recall
//! is reproducible run-to-run and the benchmark is not timing-flaky.
//!
//! Run directly:
//!   cargo bench --package vibesql-storage --bench hnsw_recall_benchmark
//!
//! Or via the suite:
//!   make benchmark-hnsw
//!   ./scripts/bench --test=hnsw
//!
//! Output is a plain table parsed by `scripts/process_results.py` (the
//! `HnswRecallParser`) and stored in the dogfooded results DB
//! (`hnsw_recall_results`).
//!
//! Environment variables:
//!   HNSW_RECALL_DATASET_SIZE - number of base vectors (default: 3000)
//!   HNSW_RECALL_DIM          - vector dimensionality (default: 16)
//!   HNSW_RECALL_K            - recall@k neighbour count (default: 10)
//!   HNSW_RECALL_QUERIES      - number of probe queries per measurement (default: 100)

use std::collections::HashSet;

use vibesql_ast::VectorDistanceMetric;
use vibesql_storage::database::indexes::HnswIndex;

// HNSW graph parameters (match the unit test so benchmark and test agree).
const M: u32 = 16;
const EF_CONSTRUCTION: u32 = 64;

/// Deterministic pseudo-random generator (xorshift64) so the recall dataset is
/// reproducible across runs without depending on `rand`. Mirrors `DetRng` in
/// the hnsw unit tests — the benchmark harness forbids nondeterminism.
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

/// Brute-force ground-truth: the `k` nearest *live* row ids to `query` (L2),
/// restricted to `live`.
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
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.into_iter().take(k).map(|(id, _)| id).collect()
}

/// Average recall@k of `index` over `num_queries` queries drawn from the live
/// vectors, against brute-force ground truth.
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

/// One row of recall measurements for a given (ef_search, delete_ratio) point.
struct RecallPoint {
    ef_search: usize,
    delete_ratio: f64,
    live_count: usize,
    deleted_count: usize,
    recall_fresh: f64,
    recall_degraded: f64,
    recall_compacted: f64,
}

/// Run all three states (fresh / degraded / compacted) for a single
/// (`ef_search`, `delete_ratio`) configuration over `dataset`.
fn run_point(
    dataset: &[(usize, Vec<f64>)],
    dim: usize,
    k: usize,
    queries: usize,
    ef_search: usize,
    delete_ratio: f64,
) -> RecallPoint {
    let n = dataset.len();

    // Deterministically pick a scattered set of survivors so removed nodes
    // (including graph hubs / entry-point candidates) are interleaved with
    // survivors, fragmenting the survivor subgraph the way a real delete-heavy
    // workload does. `keep_every = round(1 / (1 - ratio))`; keep id iff
    // i % keep_every == 0.
    let keep_every = ((1.0 / (1.0 - delete_ratio)).round() as usize).max(1);
    let live: HashSet<usize> = (0..n).filter(|i| i % keep_every == 0).collect();
    let to_delete: Vec<usize> = (0..n).filter(|i| i % keep_every != 0).collect();

    // --- fresh: built over only the live vectors (reference) ---
    let mut idx_fresh = HnswIndex::new(dim, M, EF_CONSTRUCTION, VectorDistanceMetric::L2);
    idx_fresh.set_ef_search(ef_search);
    let fresh_vectors: Vec<(usize, Vec<f64>)> =
        dataset.iter().filter(|(id, _)| live.contains(id)).cloned().collect();
    idx_fresh.build(fresh_vectors).unwrap();
    let recall_fresh = measure_recall(&idx_fresh, dataset, &live, k, queries);

    // --- degraded: full build, lazy deletes, NO compaction ---
    let mut idx_degraded = HnswIndex::new(dim, M, EF_CONSTRUCTION, VectorDistanceMetric::L2);
    idx_degraded.set_ef_search(ef_search);
    idx_degraded.set_auto_compact(false);
    idx_degraded.build(dataset.to_vec()).unwrap();
    for &id in &to_delete {
        idx_degraded.remove(id);
    }
    let recall_degraded = measure_recall(&idx_degraded, dataset, &live, k, queries);

    // --- compacted: same deletes, then explicit compaction ---
    let mut idx_compacted = HnswIndex::new(dim, M, EF_CONSTRUCTION, VectorDistanceMetric::L2);
    idx_compacted.set_ef_search(ef_search);
    idx_compacted.set_auto_compact(false);
    idx_compacted.build(dataset.to_vec()).unwrap();
    for &id in &to_delete {
        idx_compacted.remove(id);
    }
    idx_compacted.compact();
    let recall_compacted = measure_recall(&idx_compacted, dataset, &live, k, queries);

    RecallPoint {
        ef_search,
        delete_ratio,
        live_count: live.len(),
        deleted_count: to_delete.len(),
        recall_fresh,
        recall_degraded,
        recall_compacted,
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn main() {
    let n = env_usize("HNSW_RECALL_DATASET_SIZE", 3000);
    let dim = env_usize("HNSW_RECALL_DIM", 16);
    let k = env_usize("HNSW_RECALL_K", 10);
    let queries = env_usize("HNSW_RECALL_QUERIES", 100);

    // A small ef_search makes the graph's navigability (small-world property) —
    // exactly what lazy unlink erodes — the limiting factor for recall, so
    // degradation is observable. We also sweep a wider beam to confirm the
    // tradeoff. delete_ratio sweeps a moderate and a heavy delete fraction.
    let ef_search_sweep = [12usize, 40];
    let delete_ratio_sweep = [0.5f64, 0.8];

    let dataset = deterministic_dataset(n, dim, 0xC0FFEE);

    println!("=== HNSW Recall@{k} Benchmark ===");
    println!(
        "Dataset: {n} vectors, dim={dim}, queries={queries}, M={M}, ef_construction={EF_CONSTRUCTION}"
    );
    println!();
    println!("--- VibeSQL Recall Results ---");
    println!(
        "{:<10} {:>12} {:>8} {:>9} {:>10} {:>10} {:>10}",
        "EfSearch", "DeleteRatio", "Live", "Deleted", "Fresh", "Degraded", "Compacted"
    );
    println!("{:-<10} {:->12} {:->8} {:->9} {:->10} {:->10} {:->10}", "", "", "", "", "", "", "");

    let mut points = Vec::new();
    for &ef_search in &ef_search_sweep {
        for &delete_ratio in &delete_ratio_sweep {
            let p = run_point(&dataset, dim, k, queries, ef_search, delete_ratio);
            println!(
                "{:<10} {:>12.2} {:>8} {:>9} {:>10.4} {:>10.4} {:>10.4}",
                p.ef_search,
                p.delete_ratio,
                p.live_count,
                p.deleted_count,
                p.recall_fresh,
                p.recall_degraded,
                p.recall_compacted
            );
            points.push(p);
        }
    }

    println!();

    // Sanity invariants so a smoke run fails loudly if recall behaviour breaks.
    // These are deterministic given the seeded dataset, so they are safe to
    // assert in the benchmark itself (not timing-dependent).
    for p in &points {
        assert!(
            p.recall_fresh > 0.0 && p.recall_fresh <= 1.0,
            "fresh recall out of range: {:.4}",
            p.recall_fresh
        );
        assert!(
            p.recall_compacted >= p.recall_degraded - 1e-9,
            "compaction regressed recall at ef_search={} ratio={:.2}: compacted={:.4} < degraded={:.4}",
            p.ef_search,
            p.delete_ratio,
            p.recall_compacted,
            p.recall_degraded
        );
        assert!(
            p.recall_compacted >= p.recall_fresh - 0.05,
            "compaction did not restore recall at ef_search={} ratio={:.2}: compacted={:.4} fresh={:.4}",
            p.ef_search,
            p.delete_ratio,
            p.recall_compacted,
            p.recall_fresh
        );
    }

    println!("HNSW recall benchmark complete ({} configurations).", points.len());
}
