//! Main VibeSqlDB adapter implementation.
//!
//! This module provides the thin glue code that ties together all the specialized
//! modules (pooling, caching, batching, timing, execution) into a cohesive
//! SQLLogicTest database adapter.

use std::{env, time::{Duration, Instant}};
use async_trait::async_trait;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
use tokio::time::timeout;
use vibesql_storage::Database;

use super::{
    batching::BatchingManager,
    cache::CacheManager,
    executor,
    pool,
    timing::{detect_statement_type, truncate_sql, TimingTracker},
};
use super::super::execution::TestError;

/// Main database adapter for SQLLogicTest runner.
pub struct VibeSqlDB {
    db: Database,
    query_count: usize,
    verbose: bool,
    worker_id: Option<usize>,
    current_file: Option<String>,
    file_start_time: Option<Instant>,
    query_timeout_ms: u64,
    timed_out_queries: usize,
    cache_manager: CacheManager,
    timing_tracker: TimingTracker,
    batching_manager: BatchingManager,
}

impl VibeSqlDB {
    pub fn new() -> Self {
        let verbose = env::var("SQLLOGICTEST_VERBOSE")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        let worker_id = env::var("SQLLOGICTEST_WORKER_ID").ok().and_then(|s| s.parse().ok());

        let query_timeout_ms = env::var("SQLLOGICTEST_QUERY_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500); // Default: 500ms per query

        // Query result cache: enabled by default, can be disabled via env var
        let cache_enabled = env::var("SQLLOGICTEST_CACHE_ENABLED")
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true); // Enabled by default

        let cache_size = env::var("SQLLOGICTEST_CACHE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10000); // Default: 10,000 entries

        // Statement timing: enabled by default, configurable
        let timing_enabled = env::var("SQLLOGICTEST_TIMING")
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true); // Enabled by default

        let slow_query_threshold_ms = env::var("SQLLOGICTEST_SLOW_THRESHOLD_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000); // Default: 1000ms (1 second)

        // INSERT batching: enabled by default for performance, can be disabled via env var
        let insert_batching_enabled = env::var("SQLLOGICTEST_INSERT_BATCHING")
            .map(|v| v != "0" && v.to_lowercase() != "false")
            .unwrap_or(true); // Enabled by default

        Self {
            db: pool::get_pooled_database(),
            query_count: 0,
            verbose,
            worker_id,
            current_file: None,
            file_start_time: None,
            query_timeout_ms,
            timed_out_queries: 0,
            cache_manager: CacheManager::new(cache_enabled, cache_size),
            timing_tracker: TimingTracker::new(timing_enabled, slow_query_threshold_ms),
            batching_manager: BatchingManager::new(insert_batching_enabled, verbose),
        }
    }

    #[allow(dead_code)]
    fn start_test_file(&mut self, file_path: &str) {
        self.current_file = Some(file_path.to_string());
        self.file_start_time = Some(Instant::now());
        self.query_count = 0;

        if let Some(worker_id) = self.worker_id {
            eprintln!("[Worker {}] Starting: {}", worker_id, file_path);
        } else {
            eprintln!("Starting: {}", file_path);
        }
    }

    #[allow(dead_code)]
    fn finish_test_file(&self, result: &Result<(), TestError>) {
        if let (Some(file_path), Some(start_time)) = (&self.current_file, &self.file_start_time) {
            let elapsed = start_time.elapsed();
            let elapsed_secs = elapsed.as_secs_f64();

            match result {
                Ok(_) => {
                    if let Some(worker_id) = self.worker_id {
                        eprintln!("[Worker {}] ✓ {} ({:.2}s)", worker_id, file_path, elapsed_secs);
                    } else {
                        eprintln!("✓ {} ({:.2}s)", file_path, elapsed_secs);
                    }
                }
                Err(e) => {
                    if let Some(worker_id) = self.worker_id {
                        eprintln!(
                            "[Worker {}] ✗ {} ({:.2}s): {}",
                            worker_id, file_path, elapsed_secs, e
                        );
                    } else {
                        eprintln!("✗ {} ({:.2}s): {}", file_path, elapsed_secs, e);
                    }
                }
            }
        }
    }

    /// Execute SQL asynchronously (wrapper for query execution).
    async fn execute_sql_async(
        &mut self,
        sql: &str,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        executor::execute_sql(
            sql,
            &mut self.db,
            &mut self.cache_manager,
            &mut self.batching_manager,
        )
    }
}

#[async_trait]
impl AsyncDB for VibeSqlDB {
    type Error = TestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.query_count += 1;

        // Log query progress if verbose mode enabled
        if self.verbose {
            let log_interval = env::var("SQLLOGICTEST_LOG_QUERY_INTERVAL")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100);

            if self.query_count % log_interval == 0 {
                eprintln!("  Query {}: {}", self.query_count, truncate_sql(sql, 60));
            }
        }

        // Start timing if enabled
        let stmt_start = if self.timing_tracker.enabled {
            Some(Instant::now())
        } else {
            None
        };
        let stmt_type = if self.timing_tracker.enabled {
            Some(detect_statement_type(sql))
        } else {
            None
        };

        // Execute query with per-query timeout
        let timeout_duration = Duration::from_millis(self.query_timeout_ms);
        let result = match timeout(timeout_duration, self.execute_sql_async(sql)).await {
            Ok(result) => result,
            Err(_) => {
                self.timed_out_queries += 1;
                eprintln!(
                    "⏱️  Query timeout ({}ms): Query {}: {}",
                    self.query_timeout_ms, self.query_count, truncate_sql(sql, 80)
                );

                // Log timeout stats if verbose
                if self.verbose {
                    eprintln!("  Total timed out queries so far: {}", self.timed_out_queries);
                }

                // Skip the timed-out query and continue
                Ok(DBOutput::Rows { types: vec![], rows: vec![] })
            }
        };

        // Record timing if enabled
        if let (Some(start), Some(stype)) = (stmt_start, stmt_type) {
            let elapsed = start.elapsed();
            self.timing_tracker.record(stype, elapsed, sql);
        }

        result
    }

    async fn shutdown(&mut self) {
        // Commit any pending implicit transaction before shutdown
        if let Err(e) = self.batching_manager.commit_if_needed(&mut self.db) {
            eprintln!("Warning: Failed to commit implicit transaction on shutdown: {}", e);
        }

        // Log statement timing summary
        self.timing_tracker.print_summary();

        // Log final timeout statistics
        if self.timed_out_queries > 0 {
            eprintln!("📊 Query Timeout Summary:");
            eprintln!("  Total queries executed: {}", self.query_count);
            eprintln!("  Queries that timed out: {}", self.timed_out_queries);
            eprintln!("  Timeout per query: {}ms", self.query_timeout_ms);
        }

        // Log cache statistics if cache was enabled
        self.cache_manager.print_summary();
    }
}

impl Drop for VibeSqlDB {
    fn drop(&mut self) {
        // Return database to thread-local pool for reuse
        pool::return_to_pool(std::mem::take(&mut self.db));
    }
}
