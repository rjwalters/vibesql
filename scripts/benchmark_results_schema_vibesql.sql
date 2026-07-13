-- Benchmark Results Database Schema
-- Extension to dogfooding database for tracking performance over time

-- Benchmark Runs: Metadata about each benchmark execution
CREATE TABLE IF NOT EXISTS benchmark_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TEXT NOT NULL,
    git_commit TEXT,
    git_branch TEXT,
    benchmark_suite TEXT NOT NULL CHECK (benchmark_suite IN ('tpch', 'tpcc', 'tpcds', 'sysbench', 'hnsw', 'sqllogictest_suite', 'custom')),
    scale_factor TEXT,
    timeout_secs INTEGER,
    total_queries INTEGER,
    passed_queries INTEGER,
    failed_queries INTEGER,
    timeout_queries INTEGER,
    notes TEXT,
    -- machine_tag: identifies the host that produced this run so localhost
    -- numbers can be distinguished from dedicated-hardware runs. Nullable;
    -- historical rows read back as NULL ("unknown/local").
    machine_tag TEXT
);

-- TPC-C Results: Transaction-specific metrics for OLTP workloads
CREATE TABLE IF NOT EXISTS tpcc_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    database_engine TEXT NOT NULL,  -- 'vibesql', 'sqlite', 'duckdb'
    transaction_type TEXT NOT NULL, -- 'new_order', 'payment', 'order_status', 'delivery', 'stock_level', 'mixed'
    transaction_count INTEGER NOT NULL,
    avg_latency_us REAL,
    total_duration_ms INTEGER,
    transactions_per_second REAL,
    successful_transactions INTEGER,
    failed_transactions INTEGER,
    FOREIGN KEY (run_id) REFERENCES benchmark_runs(run_id)
);

-- Sysbench Results: OLTP latency metrics
CREATE TABLE IF NOT EXISTS sysbench_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    database_engine TEXT NOT NULL,  -- 'vibesql', 'sqlite', 'duckdb'
    test_name TEXT NOT NULL,        -- 'point_select', 'insert', 'read_write'
    table_size INTEGER,
    mean_time_ns REAL,
    std_dev_ns REAL,
    median_time_ns REAL,
    iterations INTEGER,
    FOREIGN KEY (run_id) REFERENCES benchmark_runs(run_id)
);

-- HNSW Recall Results: vector-index quality metrics (recall@k) for
-- delete-heavy workloads. Unlike the latency-oriented tables above this stores
-- a *quality* metric: the fraction of true k-nearest neighbours returned by an
-- approximate HNSW search vs brute-force ground truth, measured across three
-- index states (fresh build / degraded-by-lazy-deletes / post-compaction) and
-- a sweep of ef_search + delete ratio. See #5466.
CREATE TABLE IF NOT EXISTS hnsw_recall_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    database_engine TEXT NOT NULL,  -- 'vibesql'
    k INTEGER NOT NULL,             -- recall@k neighbour count
    dataset_size INTEGER,           -- number of base vectors
    dimensions INTEGER,             -- vector dimensionality
    -- NB: named `ef_search_param` (not `ef_search`) because EF_SEARCH is a
    -- reserved keyword in VibeSQL's CREATE INDEX ... WITH (...) syntax.
    ef_search_param INTEGER,        -- query-time search beam width
    delete_ratio REAL,             -- fraction of vectors lazily deleted
    live_count INTEGER,            -- surviving vectors
    deleted_count INTEGER,         -- removed vectors
    index_state TEXT NOT NULL CHECK (index_state IN ('fresh', 'degraded', 'compacted')),
    recall REAL NOT NULL,          -- measured recall@k in [0, 1]
    FOREIGN KEY (run_id) REFERENCES benchmark_runs(run_id)
);

-- Benchmark Results: Individual query/test performance measurements
CREATE TABLE IF NOT EXISTS benchmark_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    database_engine TEXT NOT NULL,  -- 'vibesql', 'sqlite', 'duckdb', 'mysql'
    query_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('passed', 'failed', 'timeout', 'error', 'incomplete')),
    parse_time_ms REAL,
    executor_creation_time_ms REAL,
    execution_time_ms REAL,
    total_time_ms REAL,
    row_count INTEGER,
    error_message TEXT,
    FOREIGN KEY (run_id) REFERENCES benchmark_runs(run_id)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_benchmark_results_run ON benchmark_results(run_id);
CREATE INDEX IF NOT EXISTS idx_benchmark_results_query ON benchmark_results(query_name);
CREATE INDEX IF NOT EXISTS idx_benchmark_results_status ON benchmark_results(status);
CREATE INDEX IF NOT EXISTS idx_benchmark_results_engine ON benchmark_results(database_engine);
CREATE INDEX IF NOT EXISTS idx_benchmark_runs_timestamp ON benchmark_runs(timestamp);
CREATE INDEX IF NOT EXISTS idx_benchmark_runs_suite ON benchmark_runs(benchmark_suite);

-- View: Latest benchmark run summary
CREATE VIEW IF NOT EXISTS latest_benchmark_summary AS
SELECT
    run_id,
    timestamp,
    benchmark_suite,
    git_commit,
    git_branch,
    total_queries,
    passed_queries,
    failed_queries,
    timeout_queries,
    ROUND(passed_queries * 100.0 / total_queries, 1) as pass_rate,
    notes
FROM benchmark_runs
ORDER BY run_id DESC
LIMIT 1;

-- View: Performance trend for each query over time
CREATE VIEW IF NOT EXISTS query_performance_trend AS
SELECT
    br.query_name,
    br.run_id,
    brun.timestamp,
    brun.git_commit,
    br.total_time_ms,
    br.execution_time_ms,
    br.status,
    LAG(br.total_time_ms) OVER (PARTITION BY br.query_name ORDER BY brun.timestamp) as prev_total_time_ms,
    LAG(br.execution_time_ms) OVER (PARTITION BY br.query_name ORDER BY brun.timestamp) as prev_execution_time_ms,
    CASE
        WHEN LAG(br.total_time_ms) OVER (PARTITION BY br.query_name ORDER BY brun.timestamp) IS NULL THEN NULL
        ELSE ROUND((br.total_time_ms - LAG(br.total_time_ms) OVER (PARTITION BY br.query_name ORDER BY brun.timestamp)) * 100.0 /
                   LAG(br.total_time_ms) OVER (PARTITION BY br.query_name ORDER BY brun.timestamp), 2)
    END as pct_change
FROM benchmark_results br
JOIN benchmark_runs brun ON br.run_id = brun.run_id
WHERE br.status = 'passed'
ORDER BY br.query_name, brun.timestamp;

-- View: Queries with significant performance regressions (>10% slower)
CREATE VIEW IF NOT EXISTS performance_regressions AS
SELECT
    query_name,
    run_id,
    timestamp,
    git_commit,
    execution_time_ms as current_time_ms,
    prev_execution_time_ms,
    pct_change,
    CASE
        WHEN pct_change >= 50 THEN 'P0 - Critical'
        WHEN pct_change >= 25 THEN 'P1 - High'
        WHEN pct_change >= 10 THEN 'P2 - Medium'
        ELSE 'P3 - Low'
    END as severity
FROM query_performance_trend
WHERE pct_change > 10
ORDER BY pct_change DESC;

-- View: Queries with significant performance improvements (>10% faster)
CREATE VIEW IF NOT EXISTS performance_improvements AS
SELECT
    query_name,
    run_id,
    timestamp,
    git_commit,
    execution_time_ms as current_time_ms,
    prev_execution_time_ms,
    ABS(pct_change) as pct_improvement
FROM query_performance_trend
WHERE pct_change < -10
ORDER BY pct_change ASC;

-- View: TPC-H query statistics across all runs
CREATE VIEW IF NOT EXISTS tpch_query_stats AS
SELECT
    br.query_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN br.status = 'passed' THEN 1 ELSE 0 END) as passed_runs,
    SUM(CASE WHEN br.status = 'timeout' THEN 1 ELSE 0 END) as timeout_runs,
    SUM(CASE WHEN br.status IN ('failed', 'error') THEN 1 ELSE 0 END) as failed_runs,
    ROUND(AVG(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END), 2) as avg_execution_ms,
    ROUND(MIN(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END), 2) as min_execution_ms,
    ROUND(MAX(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END), 2) as max_execution_ms,
    ROUND(
        (MAX(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END) -
         MIN(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END)) * 100.0 /
        NULLIF(MIN(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END), 0),
        2
    ) as variability_pct
FROM benchmark_results br
JOIN benchmark_runs brun ON br.run_id = brun.run_id
WHERE brun.benchmark_suite = 'tpch'
GROUP BY br.query_name
ORDER BY br.query_name;

-- View: Compare latest benchmark run to baseline
CREATE VIEW IF NOT EXISTS benchmark_comparison AS
WITH latest AS (
    SELECT run_id FROM benchmark_runs ORDER BY run_id DESC LIMIT 1
),
baseline AS (
    SELECT run_id FROM benchmark_runs ORDER BY run_id ASC LIMIT 1
)
SELECT
    latest_br.query_name,
    baseline_br.execution_time_ms as baseline_time_ms,
    latest_br.execution_time_ms as current_time_ms,
    ROUND((latest_br.execution_time_ms - baseline_br.execution_time_ms) * 100.0 /
          NULLIF(baseline_br.execution_time_ms, 0), 2) as pct_change,
    CASE
        WHEN latest_br.execution_time_ms < baseline_br.execution_time_ms THEN 'Improved'
        WHEN latest_br.execution_time_ms > baseline_br.execution_time_ms THEN 'Regressed'
        ELSE 'Unchanged'
    END as trend
FROM benchmark_results latest_br
JOIN benchmark_results baseline_br ON latest_br.query_name = baseline_br.query_name
WHERE latest_br.run_id = (SELECT run_id FROM latest)
  AND baseline_br.run_id = (SELECT run_id FROM baseline)
  AND latest_br.status = 'passed'
  AND baseline_br.status = 'passed'
ORDER BY pct_change DESC;

-- Indexes for TPC-C results
CREATE INDEX IF NOT EXISTS idx_tpcc_results_run ON tpcc_results(run_id);
CREATE INDEX IF NOT EXISTS idx_tpcc_results_engine ON tpcc_results(database_engine);
CREATE INDEX IF NOT EXISTS idx_tpcc_results_txn_type ON tpcc_results(transaction_type);

-- Indexes for Sysbench results
CREATE INDEX IF NOT EXISTS idx_sysbench_results_run ON sysbench_results(run_id);
CREATE INDEX IF NOT EXISTS idx_sysbench_results_engine ON sysbench_results(database_engine);
CREATE INDEX IF NOT EXISTS idx_sysbench_results_test ON sysbench_results(test_name);

-- Indexes for HNSW recall results
CREATE INDEX IF NOT EXISTS idx_hnsw_recall_results_run ON hnsw_recall_results(run_id);
CREATE INDEX IF NOT EXISTS idx_hnsw_recall_results_state ON hnsw_recall_results(index_state);

-- View: Latest HNSW recall benchmark summary
CREATE VIEW IF NOT EXISTS latest_hnsw_recall_summary AS
SELECT
    hr.database_engine,
    hr.k,
    hr.ef_search_param,
    hr.delete_ratio,
    hr.index_state,
    ROUND(hr.recall, 4) as recall,
    hr.live_count,
    hr.deleted_count,
    brun.timestamp,
    brun.git_commit
FROM hnsw_recall_results hr
JOIN benchmark_runs brun ON hr.run_id = brun.run_id
WHERE brun.run_id = (SELECT MAX(run_id) FROM benchmark_runs WHERE benchmark_suite = 'hnsw')
ORDER BY hr.ef_search_param, hr.delete_ratio, hr.index_state;

-- View: HNSW recall trend over time (per ef_search / delete_ratio / state)
CREATE VIEW IF NOT EXISTS hnsw_recall_trend AS
SELECT
    hr.ef_search_param,
    hr.delete_ratio,
    hr.index_state,
    hr.run_id,
    brun.timestamp,
    brun.git_commit,
    hr.recall,
    LAG(hr.recall) OVER (
        PARTITION BY hr.ef_search_param, hr.delete_ratio, hr.index_state
        ORDER BY brun.timestamp
    ) as prev_recall
FROM hnsw_recall_results hr
JOIN benchmark_runs brun ON hr.run_id = brun.run_id
ORDER BY hr.ef_search_param, hr.delete_ratio, hr.index_state, brun.timestamp;

-- View: Latest TPC-C benchmark summary
CREATE VIEW IF NOT EXISTS latest_tpcc_summary AS
SELECT
    tr.database_engine,
    tr.transaction_type,
    tr.transaction_count,
    tr.transactions_per_second,
    ROUND(tr.avg_latency_us, 2) as avg_latency_us,
    tr.successful_transactions,
    tr.failed_transactions,
    br.timestamp,
    br.git_commit
FROM tpcc_results tr
JOIN benchmark_runs br ON tr.run_id = br.run_id
WHERE br.run_id = (SELECT MAX(run_id) FROM benchmark_runs WHERE benchmark_suite = 'tpcc')
ORDER BY tr.database_engine, tr.transaction_type;

-- View: TPC-C performance comparison across engines
CREATE VIEW IF NOT EXISTS tpcc_engine_comparison AS
SELECT
    tr.transaction_type,
    MAX(CASE WHEN tr.database_engine = 'vibesql' THEN tr.transactions_per_second END) as vibesql_tps,
    MAX(CASE WHEN tr.database_engine = 'sqlite' THEN tr.transactions_per_second END) as sqlite_tps,
    MAX(CASE WHEN tr.database_engine = 'duckdb' THEN tr.transactions_per_second END) as duckdb_tps,
    MAX(CASE WHEN tr.database_engine = 'vibesql' THEN tr.avg_latency_us END) as vibesql_latency_us,
    MAX(CASE WHEN tr.database_engine = 'sqlite' THEN tr.avg_latency_us END) as sqlite_latency_us,
    MAX(CASE WHEN tr.database_engine = 'duckdb' THEN tr.avg_latency_us END) as duckdb_latency_us
FROM tpcc_results tr
JOIN benchmark_runs br ON tr.run_id = br.run_id
WHERE br.run_id = (SELECT MAX(run_id) FROM benchmark_runs WHERE benchmark_suite = 'tpcc')
GROUP BY tr.transaction_type
ORDER BY tr.transaction_type;

-- View: Latest Sysbench benchmark summary
CREATE VIEW IF NOT EXISTS latest_sysbench_summary AS
SELECT
    sr.database_engine,
    sr.test_name,
    sr.table_size,
    ROUND(sr.mean_time_ns / 1000.0, 2) as mean_time_us,
    ROUND(sr.std_dev_ns / 1000.0, 2) as std_dev_us,
    sr.iterations,
    br.timestamp,
    br.git_commit
FROM sysbench_results sr
JOIN benchmark_runs br ON sr.run_id = br.run_id
WHERE br.run_id = (SELECT MAX(run_id) FROM benchmark_runs WHERE benchmark_suite = 'sysbench')
ORDER BY sr.database_engine, sr.test_name;

-- View: Sysbench performance comparison across engines
CREATE VIEW IF NOT EXISTS sysbench_engine_comparison AS
SELECT
    sr.test_name,
    sr.table_size,
    MAX(CASE WHEN sr.database_engine = 'vibesql' THEN sr.mean_time_ns / 1000.0 END) as vibesql_us,
    MAX(CASE WHEN sr.database_engine = 'sqlite' THEN sr.mean_time_ns / 1000.0 END) as sqlite_us,
    MAX(CASE WHEN sr.database_engine = 'duckdb' THEN sr.mean_time_ns / 1000.0 END) as duckdb_us,
    ROUND(MAX(CASE WHEN sr.database_engine = 'vibesql' THEN sr.mean_time_ns END) * 1.0 /
          NULLIF(MAX(CASE WHEN sr.database_engine = 'sqlite' THEN sr.mean_time_ns END), 0), 2) as vibesql_vs_sqlite
FROM sysbench_results sr
JOIN benchmark_runs br ON sr.run_id = br.run_id
WHERE br.run_id = (SELECT MAX(run_id) FROM benchmark_runs WHERE benchmark_suite = 'sysbench')
GROUP BY sr.test_name, sr.table_size
ORDER BY sr.test_name;

-- View: Latest TPC-DS benchmark summary
CREATE VIEW IF NOT EXISTS latest_tpcds_summary AS
SELECT
    br.query_name,
    br.status,
    ROUND(br.execution_time_ms, 2) as execution_time_ms,
    ROUND(br.total_time_ms, 2) as total_time_ms,
    brun.timestamp,
    brun.git_commit
FROM benchmark_results br
JOIN benchmark_runs brun ON br.run_id = brun.run_id
WHERE brun.run_id = (SELECT MAX(run_id) FROM benchmark_runs WHERE benchmark_suite = 'tpcds')
ORDER BY br.query_name;

-- View: TPC-DS query statistics across all runs
CREATE VIEW IF NOT EXISTS tpcds_query_stats AS
SELECT
    br.query_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN br.status = 'passed' THEN 1 ELSE 0 END) as passed_runs,
    SUM(CASE WHEN br.status = 'timeout' THEN 1 ELSE 0 END) as timeout_runs,
    SUM(CASE WHEN br.status IN ('failed', 'error') THEN 1 ELSE 0 END) as failed_runs,
    ROUND(AVG(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END), 2) as avg_execution_ms,
    ROUND(MIN(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END), 2) as min_execution_ms,
    ROUND(MAX(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END), 2) as max_execution_ms,
    ROUND(
        (MAX(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END) -
         MIN(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END)) * 100.0 /
        NULLIF(MIN(CASE WHEN br.status = 'passed' THEN br.execution_time_ms ELSE NULL END), 0),
        2
    ) as variability_pct
FROM benchmark_results br
JOIN benchmark_runs brun ON br.run_id = brun.run_id
WHERE brun.benchmark_suite = 'tpcds'
GROUP BY br.query_name
ORDER BY br.query_name;

-- View: TPC-DS performance trend
CREATE VIEW IF NOT EXISTS tpcds_performance_trend AS
SELECT
    br.query_name,
    br.run_id,
    brun.timestamp,
    brun.git_commit,
    br.execution_time_ms,
    br.status,
    LAG(br.execution_time_ms) OVER (PARTITION BY br.query_name ORDER BY brun.timestamp) as prev_execution_time_ms,
    CASE
        WHEN LAG(br.execution_time_ms) OVER (PARTITION BY br.query_name ORDER BY brun.timestamp) IS NULL THEN NULL
        ELSE ROUND((br.execution_time_ms - LAG(br.execution_time_ms) OVER (PARTITION BY br.query_name ORDER BY brun.timestamp)) * 100.0 /
                   LAG(br.execution_time_ms) OVER (PARTITION BY br.query_name ORDER BY brun.timestamp), 2)
    END as pct_change
FROM benchmark_results br
JOIN benchmark_runs brun ON br.run_id = brun.run_id
WHERE brun.benchmark_suite = 'tpcds' AND br.status = 'passed'
ORDER BY br.query_name, brun.timestamp;
