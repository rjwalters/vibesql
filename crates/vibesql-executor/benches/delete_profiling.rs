//! DELETE Operation Profiling Benchmark
//!
//! This benchmark is designed for flamegraph profiling of DELETE operations.
//! It runs DELETE operations in a tight loop to maximize sample coverage.
//!
//! Usage:
//! ```bash
//! cargo flamegraph --bench delete_profiling --deterministic -o delete_flamegraph.svg
//! ```

mod sysbench;

use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;
use sysbench::schema::load_vibesql;
use sysbench::SysbenchData;
use vibesql_executor::{PreparedStatement, PreparedStatementCache, SessionMut};
use vibesql_storage::Database as VibeDB;
use vibesql_types::SqlValue;

/// Table size for profiling (smaller for faster iteration)
const TABLE_SIZE: usize = 10_000;

/// Number of DELETE operations to run for profiling
const NUM_DELETE_OPS: usize = 5_000;

/// Pre-prepared statements for DELETE operations
struct DeleteStatements {
    delete: Arc<PreparedStatement>,
    insert: Arc<PreparedStatement>,
    cache: Arc<PreparedStatementCache>,
}

impl DeleteStatements {
    fn new(db: &VibeDB) -> Self {
        let cache = Arc::new(PreparedStatementCache::default_cache());
        let session = vibesql_executor::Session::with_shared_cache(db, Arc::clone(&cache));

        Self {
            delete: session.prepare("DELETE FROM sbtest1 WHERE id = ?").unwrap(),
            insert: session
                .prepare("INSERT INTO sbtest1 (id, k, c, padding) VALUES (?, ?, ?, ?)")
                .unwrap(),
            cache,
        }
    }
}

/// Generate a 120-char 'c' column value
fn generate_c_string() -> String {
    let mut rng = ChaCha8Rng::seed_from_u64(rand::random());
    let mut s = String::with_capacity(120);
    for i in 0..11 {
        for _ in 0..10 {
            s.push((b'0' + rng.random_range(0..10)) as char);
        }
        if i < 10 {
            s.push('-');
        }
    }
    s
}

/// Generate a 60-char 'pad' column value
fn generate_pad_string() -> String {
    let mut rng = ChaCha8Rng::seed_from_u64(rand::random());
    let mut s = String::with_capacity(60);
    for i in 0..5 {
        for _ in 0..10 {
            s.push((b'0' + rng.random_range(0..10)) as char);
        }
        if i < 4 {
            s.push('-');
        }
    }
    while s.len() < 60 {
        s.push(' ');
    }
    s
}

fn main() {
    println!("DELETE Operation Profiling Benchmark");
    println!("=====================================");
    println!("Table size: {} rows", TABLE_SIZE);
    println!("Delete operations: {}", NUM_DELETE_OPS);
    println!();

    // Phase 1: Load database (not profiled in main timing)
    println!("Phase 1: Loading database...");
    let start = Instant::now();
    let mut db = load_vibesql(TABLE_SIZE);
    println!("  Database loaded in {:?}", start.elapsed());

    // Prepare statements
    let stmts = DeleteStatements::new(&db);
    let mut data_gen = SysbenchData::new(TABLE_SIZE);
    let mut rng = ChaCha8Rng::seed_from_u64(42);

    // Keep track of IDs - some will be deleted, some re-inserted
    let mut next_id = (TABLE_SIZE + 1) as i64;

    // Warmup
    println!();
    println!("Phase 2: Warmup (100 operations)...");
    {
        let mut session = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));
        for _ in 0..100 {
            // Delete a random row
            let delete_id = rng.random_range(1..=next_id - 1);
            session
                .execute_prepared_mut(&stmts.delete, &[SqlValue::Integer(delete_id)])
                .ok();

            // Insert a new row to maintain table size
            let k = data_gen.random_k();
            let c = generate_c_string();
            let pad = generate_pad_string();
            session
                .execute_prepared_mut(
                    &stmts.insert,
                    &[
                        SqlValue::Integer(next_id),
                        SqlValue::Integer(k),
                        SqlValue::Varchar(arcstr::ArcStr::from(c)),
                        SqlValue::Varchar(arcstr::ArcStr::from(pad)),
                    ],
                )
                .unwrap();
            next_id += 1;
        }
    }
    println!("  Warmup complete");

    // Phase 3: Profile DELETE operations (this is the hot path)
    println!();
    println!("Phase 3: Profiling {} DELETE operations...", NUM_DELETE_OPS);
    println!("  (Flamegraph samples should focus on this phase)");

    let profile_start = Instant::now();

    // Run DELETE operations in a tight loop for profiling
    {
        let mut session = SessionMut::with_shared_cache(&mut db, Arc::clone(&stmts.cache));

        for i in 0..NUM_DELETE_OPS {
            // Delete a random existing row
            let delete_id = rng.random_range(1..=next_id - 1);
            let result = session.execute_prepared_mut(&stmts.delete, &[SqlValue::Integer(delete_id)]);
            let _ = black_box(result);

            // Re-insert a row to maintain table size (so we can keep deleting)
            let k = data_gen.random_k();
            let c = generate_c_string();
            let pad = generate_pad_string();
            session
                .execute_prepared_mut(
                    &stmts.insert,
                    &[
                        SqlValue::Integer(next_id),
                        SqlValue::Integer(k),
                        SqlValue::Varchar(arcstr::ArcStr::from(c)),
                        SqlValue::Varchar(arcstr::ArcStr::from(pad)),
                    ],
                )
                .unwrap();
            next_id += 1;

            // Progress every 1000 ops
            if (i + 1) % 1000 == 0 {
                print!(".");
                use std::io::Write;
                std::io::stdout().flush().ok();
            }
        }
    }
    println!();

    let profile_duration = profile_start.elapsed();

    println!();
    println!("Results:");
    println!("  Total time: {:?}", profile_duration);
    println!(
        "  Per-operation: {:?}",
        profile_duration / NUM_DELETE_OPS as u32
    );
    println!(
        "  Operations/sec: {:.0}",
        NUM_DELETE_OPS as f64 / profile_duration.as_secs_f64()
    );
    println!();
    println!("Note: This includes both DELETE and INSERT operations.");
    println!("For pure DELETE profiling, look at 'delete' functions in the flamegraph.");
}
