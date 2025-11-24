-- Benchmark Results Database Schema
-- Extension to dogfooding database for tracking performance over time

-- Benchmark Runs: Metadata about each benchmark execution
CREATE TABLE IF NOT EXISTS benchmark_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    git_commit TEXT,
    git_branch TEXT,
    benchmark_suite TEXT NOT NULL CHECK (benchmark_suite IN ('tpch', 'sqllogictest_suite', 'custom')),
    scale_factor TEXT,
    timeout_secs INTEGER,
    total_queries INTEGER,
    passed_queries INTEGER,
    failed_queries INTEGER,
    timeout_queries INTEGER,
    notes TEXT
);

-- Benchmark Results: Individual query/test performance measurements
CREATE TABLE IF NOT EXISTS benchmark_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    query_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('passed', 'failed', 'timeout', 'error')),
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
