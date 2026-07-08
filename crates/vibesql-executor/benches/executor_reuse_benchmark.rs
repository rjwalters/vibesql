//! Executor-reuse microbenchmark (issue #5758)
//!
//! Measures the relative cost of constructing a fresh `SelectExecutor::new(db)` per query
//! versus reusing a single executor across queries via the existing
//! `SelectExecutor::reset_for_reuse()` reset path.
//!
//! ## Why a *relative* (same-run) ratio
//!
//! Absolute localhost throughput numbers are untrustworthy on a loaded developer machine
//! (the machine is always doing other work), so this bench reports only the **same-run**
//! with-reuse / without-reuse queries-per-second ratio. Both halves run back-to-back in the
//! same process against the identical loaded database, so shared noise (background load, CPU
//! frequency scaling) cancels out of the ratio.
//!
//! ## What construction actually costs on the point-lookup path
//!
//! As of current main, both heavy allocations inside `SelectExecutor` are lazy `OnceCell`s:
//! the 10MB `QueryArena` and the aggregate-cache `HashMap` are NOT allocated until a query
//! that needs them runs. For the point-lookup target (`SELECT w_tax FROM warehouse WHERE
//! w_id = 1`) neither is ever allocated, so reuse can only recover the per-query
//! `Instant::now()` clock read plus cheap stack initialization of ~15 fields. The aggregate
//! query is included to probe the regime where reuse *can* additionally recover arena /
//! HashMap reuse (Option B in the issue).
//!
//! ## Run
//!
//! ```text
//! cargo bench -p vibesql-executor --bench executor_reuse_benchmark --no-run
//! ./target/release/deps/executor_reuse_benchmark-*
//! ```
//!
//! Environment variables:
//!   EXEC_REUSE_ITERS   queries per measured loop (default: 100000)
//!   EXEC_REUSE_TRIALS  number of interleaved trials to take the median of (default: 7)
//!   EXEC_REUSE_SCALE   TPC-C scale factor for the loaded DB (default: 0.01, micro)

mod harness;
mod tpcc;

use std::{env, hint::black_box, time::Instant};

use tpcc::schema::load_vibesql;
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;

/// Parse a SQL string into an owned `SelectStmt`, panicking on anything else.
fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        other => panic!("Failed to parse SELECT statement {sql:?}: {other:?}"),
    }
}

/// Run `iters` executions constructing a fresh executor each time. Returns elapsed seconds.
fn run_without_reuse(
    db: &vibesql_storage::Database,
    stmt: &vibesql_ast::SelectStmt,
    iters: usize,
) -> f64 {
    let start = Instant::now();
    for _ in 0..iters {
        let executor = SelectExecutor::new(db);
        let rows = executor.execute(stmt).expect("query failed");
        black_box(&rows);
    }
    start.elapsed().as_secs_f64()
}

/// Run `iters` executions reusing one executor, resetting it between queries.
/// Returns elapsed seconds.
fn run_with_reuse(
    db: &vibesql_storage::Database,
    stmt: &vibesql_ast::SelectStmt,
    iters: usize,
) -> f64 {
    let mut executor = SelectExecutor::new(db);
    let start = Instant::now();
    for _ in 0..iters {
        // Reset all borrowed-context fields + memory counters + arena/cache to initial state
        // before each reuse, exactly as a per-session pool would between statements.
        executor.reset_for_reuse();
        let rows = executor.execute(stmt).expect("query failed");
        black_box(&rows);
    }
    start.elapsed().as_secs_f64()
}

/// Confirm that reuse produces byte-identical results to fresh construction for `stmt`.
/// A reuse path that produces even one wrong result is an automatic no-ship, so the bench
/// itself asserts equivalence before reporting any timing.
fn assert_reuse_equivalent(db: &vibesql_storage::Database, stmt: &vibesql_ast::SelectStmt) {
    let fresh = SelectExecutor::new(db).execute(stmt).expect("fresh query failed");

    let mut reused = SelectExecutor::new(db);
    reused.reset_for_reuse();
    let r1 = reused.execute(stmt).expect("reused query (1) failed");
    reused.reset_for_reuse();
    let r2 = reused.execute(stmt).expect("reused query (2) failed");

    assert_eq!(fresh, r1, "reuse result differs from fresh construction");
    assert_eq!(r1, r2, "consecutive reuse results differ (stale context bleed)");
}

/// Median of a slice of f64 (sorts a copy).
fn median(values: &[f64]) -> f64 {
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n == 0 {
        return f64::NAN;
    }
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// Measure one query both ways across `trials` interleaved trials and print the ratio.
fn measure(label: &str, sql: &str, db: &vibesql_storage::Database, iters: usize, trials: usize) {
    let stmt = parse_select(sql);

    // Correctness gate: reuse must be equivalent to fresh construction.
    assert_reuse_equivalent(db, &stmt);

    // Warmup (one untimed pass each way) to settle caches / branch predictors.
    let _ = run_without_reuse(db, &stmt, iters / 10 + 1);
    let _ = run_with_reuse(db, &stmt, iters / 10 + 1);

    let mut without_qps = Vec::with_capacity(trials);
    let mut with_qps = Vec::with_capacity(trials);

    for _ in 0..trials {
        // Interleave the two paths within each trial so slow drift affects both equally.
        let t_without = run_without_reuse(db, &stmt, iters);
        let t_with = run_with_reuse(db, &stmt, iters);
        without_qps.push(iters as f64 / t_without);
        with_qps.push(iters as f64 / t_with);
    }

    let without_med = median(&without_qps);
    let with_med = median(&with_qps);
    let ratio = with_med / without_med;

    println!("\n--- {label} ---");
    println!("  SQL:                {sql}");
    println!("  Iterations/loop:    {iters}");
    println!("  Trials (median of): {trials}");
    println!("  Without reuse:      {without_med:>12.0} queries/sec (median)");
    println!("  With reuse:         {with_med:>12.0} queries/sec (median)");
    println!("  Relative speedup:   {ratio:.4}x  ({:+.2}%)", (ratio - 1.0) * 100.0);
    if (ratio - 1.0).abs() < 0.03 {
        println!("  Verdict:            NULL RESULT (within +/-3% noise band)");
    } else if ratio > 1.0 {
        println!("  Verdict:            reuse FASTER");
    } else {
        println!("  Verdict:            reuse SLOWER");
    }
}

fn main() {
    let iters: usize =
        env::var("EXEC_REUSE_ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let trials: usize =
        env::var("EXEC_REUSE_TRIALS").ok().and_then(|s| s.parse().ok()).unwrap_or(7);
    let scale: f64 = env::var("EXEC_REUSE_SCALE").ok().and_then(|s| s.parse().ok()).unwrap_or(0.01);

    eprintln!("=== Executor Reuse Microbenchmark (issue #5758) ===");
    eprintln!("Loading TPC-C database (SF {scale})...");
    let load_start = Instant::now();
    let db = load_vibesql(scale);
    eprintln!("Loaded in {:?}", load_start.elapsed());
    eprintln!(
        "Reporting same-run with-reuse / without-reuse queries/sec ratios \
         (absolute numbers are NOT comparable across machines/runs)."
    );

    // Point-lookup target from the issue: no arena, no aggregate cache -> reuse can only
    // recover Instant::now() + struct init.
    measure(
        "Point lookup (issue target)",
        "SELECT w_tax FROM warehouse WHERE w_id = 1",
        &db,
        iters,
        trials,
    );

    // Arena/aggregate-using path: exercises the aggregate cache (and potentially the arena),
    // the regime where reuse could additionally recover lazy-allocated state across queries.
    measure(
        "Aggregate over stock (arena/cache path)",
        "SELECT COUNT(*), SUM(s_quantity), MIN(s_quantity), MAX(s_quantity) \
         FROM stock WHERE s_w_id = 1",
        &db,
        iters / 10 + 1,
        trials,
    );

    println!("\nDone. Interpret only the relative ratios above.");
}
