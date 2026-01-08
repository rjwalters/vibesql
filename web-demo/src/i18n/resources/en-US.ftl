# VibeSQL Web UI - English (US)

# Page titles
page-title = VibeSQL - AI-Powered SQL:1999 Database
demo-title = VibeSQL Demo
benchmarks-title = Performance Benchmarks - VibeSQL
benchmarks-heading = VibeSQL - Performance Benchmarks
conformance-title = Conformance Report - VibeSQL
conformance-heading = Conformance Report
conformance-subtitle = SQL:1999 Standards Compliance Testing

# Navigation
nav-showcase = SQL:1999 Showcase
nav-conformance = View sqltest Results
nav-sqllogictest = View SQLLogicTest Results

# Editor section
editor-title = SQL Editor
editor-storage = Storage
editor-storage-init = Initializing...
editor-execute = Execute Query

# Results section
results-title = Results
results-empty = Execute a query to see results
results-loading = Loading...
results-rows = { $count } { $count ->
    [one] row
   *[other] rows
}
results-rows-with-time = { $count } { $count ->
    [one] row
   *[other] rows
} ({ $time }ms)
results-copy = Copy to clipboard
results-export = Export CSV
results-limit-warning = Showing first { $limit } of { $total } rows. Use LIMIT clause to refine your query.

# Examples sidebar
examples-title = Examples
examples-basic = Basic Queries
examples-advanced = Advanced Queries

# Database selector
db-select-label = Database

# Footer
footer-tagline = VibeSQL - SQL:1999 Database in WebAssembly
footer-deployed = Deployed: { $date }

# Theme
theme-toggle-dark = Switch to dark mode
theme-toggle-light = Switch to light mode

# Locale
locale-select = Select language

# Messages
msg-query-success = Query executed successfully
msg-rows-affected = { $count } { $count ->
    [one] row
   *[other] rows
} affected

# Errors
error-generic = An error occurred
error-query-failed = Query failed
error-no-databases = No databases available

# Loading states
loading-initializing-theme = Initializing theme
loading-preparing-editor = Preparing editor
loading-database-engine = Loading database engine
loading-setting-up-ui = Setting up user interface
loading-editor = Loading editor...
loading-compliance-data = Loading compliance data...
loading-conformance-report = Loading conformance report...

# Editor
editor-placeholder = Enter SQL query here... (Ctrl+Enter or Cmd+Enter to execute)

# Navigation links
nav-challenge = SQL Vibe Coding Challenge
nav-terminal = SQL Terminal Demo
nav-compliance = SQL Test Compliance Report
nav-benchmarks = Performance Benchmarks
nav-trends = Performance Trends
nav-github = GitHub Repository
nav-home = Home

# Trends page
trends-title = Performance Trends - VibeSQL
trends-heading = VibeSQL - Performance Trends
trends-total-runs = Total Benchmark Runs
trends-across-suites = across all suites
trends-date-range = Date Range
trends-first-to-last = first to last run
trends-latest-commit = Latest Commit
trends-most-recent = most recent benchmark
trends-generated = Generated
trends-last-export = last data export

# Results
results-success-zero = Query executed successfully (0 rows)
results-null = NULL

# Help Modal
help-title = Keyboard Shortcuts & Help
help-close = Close
help-editor-shortcuts = Editor Shortcuts
help-navigation = Navigation
help-results-actions = Results Actions
help-tips = Tips
help-shortcut-execute = Execute current query
help-shortcut-comment = Toggle line comment
help-shortcut-indent = Indent selection
help-shortcut-show-help = Show this help dialog
help-shortcut-close-help = Close help dialog
help-action-copy = Copy to clipboard
help-action-copy-desc = Copy results as tab-separated values
help-action-export = Export CSV
help-action-export-desc = Download results as CSV file
help-tip-limit = Results are limited to 1,000 rows for performance. Use LIMIT clause to refine queries.
help-tip-time = Execution time is shown with query results.
help-tip-syntax = The editor supports SQL syntax highlighting and auto-completion.
help-tip-theme = Toggle between light/dark themes using the theme button.
help-got-it = Got it!

# Showcase Navigation
showcase-title = Core SQL:1999 Showcase
showcase-description = Explore the implemented SQL:1999 Core features interactively
showcase-complete = { $percent }% Complete
showcase-categories = Feature Categories
showcase-legend = Status Legend
showcase-status-implemented = Fully Implemented
showcase-status-partial = Partially Implemented
showcase-status-planned = Planned

# Showcase category labels
showcase-cat-compliance = Compliance Dashboard
showcase-cat-data-types = Data Types
showcase-cat-dml = DML Operations
showcase-cat-predicates = Predicates & Operators
showcase-cat-joins = JOINs
showcase-cat-subqueries = Subqueries
showcase-cat-aggregates = Aggregates & GROUP BY
showcase-cat-ddl = DDL & Constraints

# Common showcase elements
showcase-interactive-examples = Interactive Examples
showcase-try-example = Try This Example
showcase-progress = { $implemented } of { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Category
showcase-table-description = Description
showcase-table-syntax = Syntax
showcase-table-use-case = Use Case

# Status labels
status-implemented = Implemented
status-partial = Partial
status-planned = Planned

# Aggregates Showcase
aggregates-title = SQL Aggregates and GROUP BY
aggregates-description = Core SQL:1999 aggregate functions and grouping capabilities
aggregates-reference = Aggregate Functions Reference
aggregates-table-function = Function
aggregates-progress-type = functions
aggregates-ex-basic = Basic Aggregate Functions
aggregates-ex-group-single = GROUP BY (Single Column)
aggregates-ex-group-multiple = GROUP BY (Multiple Columns)
aggregates-ex-having = HAVING Clause
aggregates-ex-orderby = ORDER BY with Aggregates
aggregates-ex-null = NULL Handling in Aggregates

# DML Operations Showcase
dml-title = DML Operations (Data Manipulation Language)
dml-description = Core SQL:1999 operations for querying and modifying data
dml-reference = DML Operations Reference
dml-table-operation = Operation
dml-progress-type = operations
dml-ex-select-basic = SELECT - Basic Queries
dml-ex-select-ordering = SELECT - Ordering and Limiting
dml-ex-insert = INSERT Operations
dml-ex-update = UPDATE Operations
dml-ex-delete = DELETE Operations
dml-ex-combined = Combined CRUD Workflow

# Data Types Showcase
datatypes-title = Core SQL:1999 Data Types
datatypes-description = Explore the fundamental data types defined in the SQL:1999 Core specification
datatypes-reference = Data Type Reference
datatypes-table-type = Type Name
datatypes-table-example = Example Values
datatypes-table-spec = Specification
datatypes-progress-type = types
datatypes-ex-numeric = Working with Numeric Types
datatypes-ex-null = NULL Handling & Three-Valued Logic
datatypes-ex-comparisons = Type Comparisons & Operations

# JOINs Showcase
joins-title = SQL JOINs
joins-description = Core SQL:1999 JOIN operations for combining data from multiple tables
joins-reference = JOIN Types Reference
joins-table-type = JOIN Type
joins-progress-type = JOIN types
joins-category-suffix = JOINs
joins-ex-sample = Sample Data Setup
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Multi-table JOIN

# Predicates Showcase
predicates-title = Predicates and Operators
predicates-description = SQL:1999 predicates for filtering and logical operations
predicates-reference = Predicates Reference
predicates-table-predicate = Predicate
predicates-progress-type = predicates
predicates-ex-comparison = Comparison Operators
predicates-ex-between = BETWEEN and Range Predicates
predicates-ex-null = NULL Predicates and Three-Valued Logic
predicates-ex-boolean = Boolean Logic (AND, OR, NOT)
predicates-ex-in = IN Predicate with Subqueries
predicates-ex-combined = Combined Predicate Operations

# Subqueries Showcase
subqueries-title = SQL Subqueries
subqueries-description = Core SQL:1999 subquery capabilities for nested query operations
subqueries-reference = Subquery Types Reference
subqueries-table-type = Subquery Type
subqueries-progress-type = subquery types
subqueries-ex-scalar-select = Scalar Subquery in SELECT
subqueries-ex-scalar-where = Scalar Subquery in WHERE
subqueries-ex-derived = Derived Tables (Subquery in FROM)
subqueries-ex-in = IN Predicate with Subquery
subqueries-ex-correlated = Correlated Subqueries
subqueries-ex-nested = Nested Subqueries

# =============================================================================
# Benchmarks Page
# =============================================================================

# Section headers
bench-section-embedded = Embedded
bench-section-server = Server
bench-results-title = Benchmark Results
bench-perf-comparison = Performance Comparison
bench-methodology-title = Methodology
bench-analysis-roadmap = Analysis & Roadmap

# Summary cards
bench-vs-sqlite = vs SQLite
bench-vs-duckdb = vs DuckDB
bench-vs-mysql = vs MySQL
bench-ops-tested = Operations Tested
bench-last-updated = Last Updated
bench-avg-speedup = average speedup
bench-from-main = from main branch
bench-loading = Loading...
bench-na = N/A
bench-faster = { $value }x faster
bench-slower = { $value }x slower
bench-speedup = { $value }x
bench-startup-time-label = startup time
bench-download-size = download size
bench-uncompressed = uncompressed
bench-size-metrics = size metrics
bench-failed = FAILED
bench-failed-title = Query failed (timeout or error)
bench-no-wasm-data = No WASM data available
bench-no-server-data = No Sysbench server benchmark data available
bench-no-server-data-hint = Server benchmarks require running sysbench_server with MySQL comparison enabled.

# Table headers
bench-table-operation = Operation
bench-table-query = Query
bench-table-vibesql = VibeSQL
bench-table-vibesql-server = VibeSQL Server
bench-table-sqlite = SQLite
bench-table-duckdb = DuckDB
bench-table-mysql = MySQL
bench-table-loading = Loading benchmark results...
bench-vibesql-server-title = VibeSQL via PostgreSQL wire protocol

# Common benchmark terms
bench-hardware = Hardware
bench-benchmark-framework = Benchmark Framework
bench-scale-factor = Scale Factor
bench-data = Data
bench-databases-tested = Databases Tested
bench-execution-mode = Execution Mode
bench-measurement = Measurement
bench-workload = Workload
bench-transaction-mix = Transaction Mix
bench-warehouses = Warehouses
bench-concurrency = Concurrency
bench-acid-compliance = ACID Compliance
bench-mode = Mode
bench-workload-types = Workload Types
bench-table-size = Table Size
bench-index-types = Index Types
bench-operations = Operations
bench-databases = Databases
bench-protocol-overhead = Protocol Overhead
bench-binary-size = Binary Size
bench-startup-time = Startup Time
bench-peak-memory = Peak Memory
bench-schema = Schema
bench-query-count = Query Count
bench-query-types = Query Types
bench-sql-features = SQL Features
bench-wasm-size = WASM Size
bench-wasm-gzip = WASM (gzip)
bench-wasm-brotli = WASM (brotli)

# TPC-H specific
bench-tpch-name = TPC-H
bench-tpch-title = TPC-H Decision Support Benchmark
bench-tpch-description = These benchmarks use the industry-standard <strong>TPC-H benchmark suite</strong>, which simulates real-world decision support workloads with complex analytical queries involving aggregations, joins, subqueries, and sorting.
bench-tpch-ops-label = TPC-H queries
bench-tpch-note-intro = All benchmarks measure end-to-end query execution time including parsing, planning, execution, and result materialization. This represents <strong>real-world SQL engine performance</strong> for analytical workloads.
bench-tpch-note-queries = <strong>Note:</strong> TPC-H queries test different aspects of SQL performance: simple aggregations (Q1, Q6), complex joins (Q2-Q5, Q7-Q10), subqueries (Q11-Q15), and advanced analytics (Q16-Q22). Hover over query names in the table above for descriptions.

# TPC-H Discussion
bench-tpch-disc-excels-title = Where VibeSQL Excels
bench-tpch-disc-excels = VibeSQL shows strong performance on <strong>scan-heavy aggregation queries</strong> (Q1, Q6, Q14, Q15, Q20) where our columnar execution engine and SIMD-accelerated aggregations shine. These queries involve filtering large tables and computing aggregates without complex join patterns.
bench-tpch-disc-targets-title = Current Optimization Targets
bench-tpch-disc-targets = Multi-way join queries (Q3, Q5, Q7-Q10, Q18, Q19, Q21) currently show SQLite ahead. The primary bottleneck is our hash join implementation, which doesn't yet employ the same level of optimization as SQLite's decades-refined B-tree joins. Specific areas under active development:
bench-tpch-disc-join-ordering = Improved cardinality estimation for better join order selection
bench-tpch-disc-hash-sizing = Adaptive hash table growth and spill-to-disk for large joins
bench-tpch-disc-vectorized = Batch processing in the join inner loop to improve cache utilization
bench-tpch-disc-inl-joins = Leveraging B-tree indexes when beneficial
bench-tpch-disc-path-title = Path to Leadership
bench-tpch-disc-path = VibeSQL's architecture is designed for modern hardware with features like columnar storage, vectorized execution, and lock-free concurrency. As these optimizations mature, we expect VibeSQL to achieve consistent leadership across all TPC-H queries. The fundamental design supports parallelism and SIMD that traditional row-store databases cannot easily retrofit.

# TPC-H Query Descriptions
bench-tpch-q1 = Pricing Summary Report - Aggregate pricing with GROUP BY and ORDER BY
bench-tpch-q2 = Minimum Cost Supplier - 3-table JOIN with ORDER BY and LIMIT
bench-tpch-q3 = Shipping Priority - 3-table JOIN with aggregation
bench-tpch-q4 = Order Priority Checking - Correlated EXISTS subquery
bench-tpch-q5 = Local Supplier Volume - 6-table JOIN with complex filtering
bench-tpch-q6 = Forecasting Revenue Change - WHERE filters with BETWEEN and SUM
bench-tpch-q7 = Volume Shipping - 6-table JOIN with SUBSTR and date filtering
bench-tpch-q8 = National Market Share - 7-table JOIN with CASE expressions
bench-tpch-q9 = Product Type Profit Measure - 4-table JOIN with aggregation
bench-tpch-q10 = Returned Item Reporting - 4-table JOIN with TOP-N LIMIT
bench-tpch-q11 = Important Stock Identification - Subquery in HAVING clause
bench-tpch-q12 = Shipping Modes Priority - CASE aggregation with date logic
bench-tpch-q13 = Customer Distribution - LEFT OUTER JOIN with subquery
bench-tpch-q14 = Promotion Effect - Conditional aggregation with CASE
bench-tpch-q15 = Top Supplier - Nested subqueries with MAX
bench-tpch-q16 = Parts/Supplier Relationship - NOT IN subquery with DISTINCT
bench-tpch-q17 = Small-Quantity-Order Revenue - Correlated subquery in WHERE
bench-tpch-q18 = Large Volume Customer - GROUP BY with HAVING
bench-tpch-q19 = Discounted Revenue - Complex OR conditions
bench-tpch-q20 = Potential Part Promotion - IN subquery with GROUP BY/HAVING
bench-tpch-q21 = Suppliers Who Kept Orders Waiting - Multi-table EXISTS
bench-tpch-q22 = Global Sales Opportunity - SUBSTR with NOT EXISTS subquery

# TPC-DS specific
bench-tpcds-name = TPC-DS
bench-tpcds-title = TPC-DS Decision Support Benchmark
bench-tpcds-description = <strong>TPC-DS</strong> is the successor to TPC-H, featuring 99 queries that model a modern decision support system with significantly more complex query patterns including multiple fact tables, snow-flake schema, and advanced SQL features.
bench-tpcds-ops-label = TPC-DS queries
bench-tpcds-note-intro = TPC-DS queries are substantially more complex than TPC-H, testing advanced SQL features like window functions, common table expressions (WITH clause), and complex join patterns across multiple fact and dimension tables.
bench-tpcds-note-remaining = <strong>Note:</strong> All 99 TPC-DS queries pass, demonstrating comprehensive SQL:1999 feature support including INTERSECT, EXCEPT, window functions, CTEs, and complex subqueries.

# TPC-DS Discussion
bench-tpcds-disc-coverage-title = SQL:1999 Feature Coverage
bench-tpcds-disc-coverage = TPC-DS exercises the most demanding SQL features. VibeSQL passes <strong>all 99 queries</strong>, demonstrating complete coverage of SQL:1999 including ROLLUP, CUBE, GROUPING(), window functions with complex framing, recursive CTEs, and INTERSECT/EXCEPT set operations.
bench-tpcds-disc-optimization-title = Complex Query Optimization
bench-tpcds-disc-optimization = TPC-DS queries often join 10+ tables with correlated subqueries. Current focus areas:
bench-tpcds-disc-cte = Intelligent decision between materialized and inline CTEs
bench-tpcds-disc-decorrelation = Converting correlated subqueries to joins when beneficial
bench-tpcds-disc-star = Fact-dimension join ordering for analytical patterns
bench-tpcds-disc-toward-title = Complete TPC-DS Coverage
bench-tpcds-disc-toward = With all 99 queries passing, VibeSQL demonstrates production-ready SQL:1999 compliance for complex analytical workloads. Recent additions of INTERSECT and EXCEPT set operations completed full TPC-DS coverage, implemented as efficient hash-based operators.
bench-tpcds-disc-sqlite-title = SQLite Comparison Note
bench-tpcds-disc-sqlite = SQLite cannot execute 12 of the 99 TPC-DS queries (Q2, Q5, Q14, Q17, Q18, Q22, Q36, Q67, Q70, Q77, Q80, Q86) due to missing SQL:1999 OLAP features: <strong>ROLLUP/CUBE</strong> grouping sets, the <strong>GROUPING()</strong> function, and <strong>STDDEV_SAMP()</strong>. These queries are skipped in SQLite benchmarks. VibeSQL and DuckDB support all 99 queries.

# TPC-C specific
bench-tpcc-name = TPC-C
bench-tpcc-title = TPC-C Online Transaction Processing Benchmark
bench-tpcc-description = The <strong>TPC-C benchmark</strong> simulates a complete order-entry environment with a mix of complex transactions including order entry, payment processing, order status queries, delivery processing, and stock level monitoring.
bench-tpcc-ops-label = TPC-C transactions
bench-tpcc-transactions-label = transactions executed
bench-tpcc-note-intro = TPC-C measures transactions per minute (tpmC) and tests the database's ability to handle concurrent transactions with complex business logic. This benchmark is critical for evaluating <strong>transactional workload performance</strong>.
bench-tpcc-note-results = <strong>Note:</strong> Results show average transaction latency. Lower is better. TPC-C is particularly demanding for write-heavy workloads with strict consistency requirements.

# TPC-C Transaction Descriptions
bench-tpcc-new-order = New Order - Complex transaction with inventory checks and order creation
bench-tpcc-payment = Payment - Update customer balance and warehouse/district totals
bench-tpcc-order-status = Order Status - Read-only query for customer order history
bench-tpcc-delivery = Delivery - Batch processing of pending orders
bench-tpcc-stock-level = Stock Level - Count items below threshold in recent orders

# TPC-C Discussion
bench-tpcc-disc-faster-title = { $speedup } Faster Than SQLite
bench-tpcc-disc-faster = VibeSQL achieves <strong>~{ $vibesqlTps } transactions per second</strong> compared to SQLite's ~{ $sqliteTps } TPS, a { $speedup } improvement. This speedup comes from our lock-free MVCC architecture that avoids SQLite's coarse-grained locking on every write operation.
bench-tpcc-disc-dominates-title = Why VibeSQL Dominates OLTP
bench-tpcc-disc-lockfree = MVCC allows readers and writers to proceed concurrently without blocking
bench-tpcc-disc-optimistic = Transactions only conflict at commit time, not during execution
bench-tpcc-disc-btree = Purpose-built index structure optimized for in-memory workloads
bench-tpcc-disc-prepared = Query plans are compiled once and reused
bench-tpcc-disc-scaling-title = Scaling Further
bench-tpcc-disc-scaling = Current results are single-threaded. VibeSQL's architecture supports multi-threaded transaction processing, and we expect improved scaling as we add parallel execution support.
bench-tpcc-disc-duckdb-title = Why DuckDB Lags on OLTP
bench-tpcc-disc-duckdb = DuckDB achieves only ~{ $duckdbTps } TPS on TPC-C ({ $duckdbVsVibesql } slower than VibeSQL, { $duckdbVsSqlite } slower than SQLite). This is expected: DuckDB is an <strong>analytical (OLAP) database</strong> optimized for large batch operations, not single-row transactions. Its columnar storage format excels at scanning millions of rows but adds overhead for point lookups and small updates that dominate OLTP workloads like TPC-C.

# Sysbench Embedded specific
bench-sysbench-embedded-name = Sysbench (Embedded)
bench-sysbench-embedded-title = Sysbench Micro-Benchmarks (Embedded)
bench-sysbench-embedded-description = <strong>Sysbench</strong> provides focused micro-benchmarks that isolate specific database operations. These tests measure raw performance for fundamental operations without the complexity of full transaction workloads.
bench-sysbench-embedded-ops-label = Sysbench operations
bench-sysbench-embedded-note = Embedded mode runs the database in-process with zero network overhead, ideal for single-process applications where minimal latency is critical.

# Sysbench Operation Descriptions
bench-sysbench-point-select = Point Select - Single row lookup by primary key
bench-sysbench-insert = Insert - Insert new rows into table
bench-sysbench-update-index = Update Index - Update indexed column (k = k + 1)
bench-sysbench-update-non-index = Update Non-Index - Update non-indexed column
bench-sysbench-delete = Delete - Remove rows by primary key
bench-sysbench-range-queries = Range Queries - Simple, SUM, ORDER BY, and DISTINCT range scans

# Sysbench Embedded Discussion
bench-sysbench-emb-disc-point-title = Point Lookups: { $pointRatio } Gap
bench-sysbench-emb-disc-point = VibeSQL's point selects run at <strong>~{ $pointVibesqlUs }µs vs SQLite's ~{ $pointSqliteUs }µs</strong>. This { $pointRatio } gap represents our primary OLTP optimization target - we're investigating B-tree node layout and lock-free read paths to close this gap.
bench-sysbench-emb-disc-index-title = Index Updates: { $indexRatio } Gap
bench-sysbench-emb-disc-index = VibeSQL's indexed updates run at <strong>~{ $indexVibesqlUs }µs vs SQLite's ~{ $indexSqliteUs }µs</strong>. This is an area for optimization as our MVCC design adds overhead for index maintenance that we're working to reduce.
bench-sysbench-emb-disc-improve-title = Areas for Improvement
bench-sysbench-emb-disc-bulk = SQLite's batch insert path is highly optimized; we're adding batched B-tree operations
bench-sysbench-emb-disc-nonindex = Non-indexed updates show VibeSQL at ~{ $nonIndexVibesqlUs }µs vs SQLite's ~{ $nonIndexSqliteUs }µs
bench-sysbench-emb-disc-deletes = Delete operations show VibeSQL at ~{ $deleteVibesqlUs }µs vs SQLite's ~{ $deleteSqliteUs }µs
bench-sysbench-emb-disc-architecture-title = Architectural Trade-offs
bench-sysbench-emb-disc-architecture = VibeSQL's hybrid architecture targets both OLTP and OLAP workloads. Our B-tree storage provides SQLite-competitive point lookup performance, while columnar execution handles analytical queries efficiently. This differs from pure OLAP databases like DuckDB that optimize exclusively for bulk operations at the cost of single-row latency.

# Sysbench Server specific
bench-sysbench-server-name = Sysbench (Server)
bench-sysbench-server-title = Sysbench Micro-Benchmarks (Server)
bench-sysbench-server-description = <strong>Sysbench</strong> server benchmarks compare VibeSQL Server (PostgreSQL wire protocol) against MySQL, measuring performance for multi-client database deployments.
bench-sysbench-server-ops-label = Sysbench operations
bench-sysbench-server-note = Server mode uses the PostgreSQL wire protocol, enabling multi-client access and compatibility with existing PostgreSQL tooling and drivers.

# Sysbench Server Discussion
bench-sysbench-srv-disc-protocol-title = PostgreSQL Wire Protocol
bench-sysbench-srv-disc-protocol = VibeSQL Server implements the PostgreSQL wire protocol, enabling compatibility with existing PostgreSQL drivers and tools. This adds ~10-50µs of protocol overhead per query compared to embedded mode, but enables multi-client deployments.
bench-sysbench-srv-disc-mysql-title = MySQL Comparison
bench-sysbench-srv-disc-mysql = Server benchmarks compare against MySQL to evaluate VibeSQL as a drop-in replacement for traditional client-server databases. VibeSQL Server outperforms MySQL across all Sysbench operations, with speedups ranging from <strong>2.4x</strong> (range queries) to <strong>12.8x</strong> (indexed updates).
bench-sysbench-srv-disc-perf-title = Why VibeSQL Server is Faster
bench-sysbench-srv-disc-perf-arch = VibeSQL's architecture differs fundamentally from MySQL's traditional RDBMS design
bench-sysbench-srv-disc-perf-storage = VibeSQL uses an in-memory columnar storage engine optimized for analytical and OLTP workloads, avoiding MySQL's disk-based InnoDB page management overhead
bench-sysbench-srv-disc-perf-locking = No heavyweight row-level locking or MVCC bookkeeping—VibeSQL uses lightweight concurrency control designed for modern multi-core CPUs
bench-sysbench-srv-disc-perf-protocol = Efficient PostgreSQL wire protocol implementation with minimal serialization overhead compared to MySQL's protocol
bench-sysbench-srv-disc-perf-writes = Write operations (inserts/updates) show the largest gains (<strong>8-12x</strong>) because VibeSQL avoids MySQL's redo log, undo log, and doublewrite buffer synchronization
bench-sysbench-srv-disc-perf-reads = Read operations show smaller but consistent gains (<strong>2-3x</strong>) due to cache-efficient columnar access patterns and zero disk I/O
bench-sysbench-srv-disc-roadmap-title = Server Roadmap
bench-sysbench-srv-disc-pooling = Reduce connection establishment overhead for high-throughput scenarios
bench-sysbench-srv-disc-caching = Server-side caching of query plans across connections
bench-sysbench-srv-disc-extended = Full PostgreSQL extended query protocol support for batch operations

# TPC-H Server specific
bench-tpch-server-name = TPC-H (Server)
bench-tpch-server-title = TPC-H Analytical Benchmark (Server)
bench-tpch-server-description = <strong>TPC-H server benchmarks</strong> compare VibeSQL Server (PostgreSQL wire protocol) against MySQL for analytical query workloads, measuring OLAP performance in client-server deployments.
bench-tpch-server-ops-label = TPC-H queries
bench-tpch-server-note-intro = Server benchmarks test the <strong>PostgreSQL wire protocol</strong> implementation, measuring end-to-end query latency including network overhead.
bench-tpch-server-note-queries = Queries test complex JOINs, subqueries, and aggregations typical of business intelligence workloads.

# TPC-H Server Discussion
bench-tpch-srv-disc-protocol-title = PostgreSQL Wire Protocol
bench-tpch-srv-disc-protocol = VibeSQL Server speaks the PostgreSQL wire protocol, enabling use of standard PostgreSQL drivers and tools. This benchmark measures full end-to-end latency including protocol overhead.
bench-tpch-srv-disc-comparison-title = MySQL Comparison
bench-tpch-srv-disc-comparison = Comparing against MySQL provides a baseline for traditional client-server databases on analytical workloads. VibeSQL's columnar execution engine provides advantages for complex aggregations and joins.
bench-tpch-srv-disc-roadmap-title = Server OLAP Roadmap
bench-tpch-srv-disc-prepared = Reuse compiled query plans across connections
bench-tpch-srv-disc-pooling = Efficient connection handling for high-throughput scenarios
bench-tpch-srv-disc-scale = Testing larger datasets (SF 0.1, SF 1.0) for production-scale validation

# TPC-C Server specific
bench-tpcc-server-name = TPC-C (Server)
bench-tpcc-server-title = TPC-C OLTP Benchmark (Server)
bench-tpcc-server-description = <strong>TPC-C server benchmarks</strong> compare VibeSQL Server (PostgreSQL wire protocol) against MySQL for OLTP transaction workloads, measuring throughput for multi-client database deployments.
bench-tpcc-server-ops-label = TPC-C transactions
bench-tpcc-server-note-intro = Server benchmarks test the <strong>PostgreSQL wire protocol</strong> implementation, measuring transaction throughput including network overhead.
bench-tpcc-server-note-results = Results report transactions per second (TPS) for the standard TPC-C transaction mix.
bench-tpcc-mixed = Mixed Workload - Standard TPC-C transaction mix (45% New-Order, 43% Payment, 4% Order-Status, 4% Delivery, 4% Stock-Level)

# TPC-C Server Discussion
bench-tpcc-srv-disc-protocol-title = PostgreSQL Wire Protocol
bench-tpcc-srv-disc-protocol = VibeSQL Server speaks the PostgreSQL wire protocol, enabling use of standard PostgreSQL drivers and tools. This benchmark measures full end-to-end transaction latency including protocol overhead.
bench-tpcc-srv-disc-comparison-title = MySQL Comparison
bench-tpcc-srv-disc-comparison = Comparing against MySQL provides a baseline for traditional client-server databases on OLTP workloads. MySQL is the industry standard for transactional workloads, and TPC-C is MySQL's strength.
bench-tpcc-srv-disc-roadmap-title = Server OLTP Roadmap
bench-tpcc-srv-disc-prepared = Reuse compiled query plans across connections
bench-tpcc-srv-disc-pooling = Efficient connection handling for high-throughput scenarios
bench-tpcc-srv-disc-parallel = Multi-client concurrent transaction processing
bench-bullet-prepared-stmts = Prepared statements
bench-bullet-larger-scale = Larger scale factors
bench-bullet-parallel-clients = Parallel clients

# Footprint Embedded specific
bench-footprint-embedded-name = Footprint (Embedded)
bench-footprint-embedded-title = Native Binary Footprint
bench-footprint-embedded-description = <strong>Embedded footprint benchmarks</strong> measure the resource efficiency of native database binaries, comparing binary size, cold startup time, and peak memory usage.
bench-footprint-embedded-ops-label = databases compared
bench-footprint-embedded-note = Native binary footprint is critical for <strong>embedded and edge deployments</strong> where binary size, startup latency, and memory consumption directly impact deployment feasibility.

# Footprint Embedded Descriptions
bench-footprint-binary-size = Binary Size - Size of the compiled database binary on disk
bench-footprint-startup-time = Startup Time - Time to cold-start and execute first query
bench-footprint-peak-memory = Peak Memory - Maximum resident set size during initialization

# Footprint Embedded Discussion
bench-footprint-emb-disc-size-title = Binary Size: Middle Ground
bench-footprint-emb-disc-size = VibeSQL at <strong>~{ $vibesqlBinaryMb }MB</strong> sits between SQLite (~{ $sqliteBinaryMb }MB) and DuckDB (~{ $duckdbBinaryMb }MB). This reflects our choice to include advanced features (window functions, CTEs, columnar execution) while keeping the binary manageable for embedded deployments.
bench-footprint-emb-disc-startup-title = Startup Time
bench-footprint-emb-disc-startup = VibeSQL achieves <strong>~{ $vibesqlStartupMs }ms cold startup</strong>, compared to SQLite (~{ $sqliteStartupMs }ms) and DuckDB (~{ $duckdbStartupMs }ms). Our minimal initialization path loads only essential metadata structures on startup.
bench-footprint-emb-disc-memory-title = Memory Efficiency
bench-footprint-emb-disc-memory = Peak memory during startup is ~7MB for VibeSQL vs ~3MB for SQLite and ~11MB for DuckDB. The difference from SQLite reflects our more sophisticated query optimizer and columnar execution infrastructure that's allocated upfront.
bench-footprint-emb-disc-roadmap-title = Size Reduction Roadmap
bench-footprint-emb-disc-flags = Compile-time feature selection to exclude unused functionality
bench-footprint-emb-disc-lto = Whole-program link-time optimization for dead code elimination
bench-footprint-emb-disc-modular = Separate core engine from optional features (e.g., window functions)

# Footprint Server/WASM specific
bench-footprint-server-name = Footprint (Server/WASM)
bench-footprint-server-title = WASM Footprint
bench-footprint-server-description = <strong>WASM footprint benchmarks</strong> measure the WebAssembly module size for browser deployment, critical for web applications where download size impacts user experience.
bench-footprint-server-ops-label = deployment targets
bench-footprint-server-note = WASM sizes are critical for <strong>web deployments</strong> where download time directly impacts time-to-interactive. Gzip sizes are most relevant as browsers automatically decompress gzip content.
bench-footprint-server-note2 = <strong>Note:</strong> VibeSQL WASM is designed for minimal download size while maintaining full SQL:1999 compliance in the browser.

# Footprint Server Descriptions
bench-footprint-wasm-size = WASM Size - Size of the WebAssembly module for browser deployment
bench-footprint-wasm-gzip = WASM (gzip) - Compressed size for web delivery

# Footprint Server Discussion
bench-footprint-srv-disc-wasm-title = WASM: { $wasmSizeGzipMb }MB Compressed
bench-footprint-srv-disc-wasm = VibeSQL's WebAssembly module compresses to <strong>~{ $wasmSizeGzipMb }MB gzipped</strong>, enabling fast initial page loads. This is a full SQL:1999 database with window functions, CTEs, and ACID transactions running entirely in the browser.
bench-footprint-srv-disc-included-title = What's Included
bench-footprint-srv-disc-parser = Complete SQL parser and query optimizer
bench-footprint-srv-disc-btree = B-tree storage engine with MVCC
bench-footprint-srv-disc-window = Window functions and advanced aggregations
bench-footprint-srv-disc-cte = Common table expressions (WITH clause)
bench-footprint-srv-disc-acid = Full ACID transaction support
bench-footprint-srv-disc-benefits-title = Browser Deployment Benefits
bench-footprint-srv-disc-benefits = Running SQL in the browser eliminates round-trip latency to servers, enables offline-first applications, and keeps sensitive data on the user's device. VibeSQL's WASM build is designed for this use case with minimal dependencies and efficient memory usage.
bench-footprint-srv-disc-roadmap-title = WASM Roadmap
bench-footprint-srv-disc-streaming = Start executing while the module downloads
bench-footprint-srv-disc-indexeddb = Durable storage across browser sessions
bench-footprint-srv-disc-worker = Run queries off the main thread for responsive UIs

# Bullet point labels (used with descriptions)
bench-bullet-join-ordering = Join ordering
bench-bullet-hash-sizing = Hash table sizing
bench-bullet-vectorized = Vectorized joins
bench-bullet-inl-joins = Index-nested-loop joins
bench-bullet-cte-materialization = CTE materialization
bench-bullet-decorrelation = Subquery decorrelation
bench-bullet-star-optimization = Star schema optimization
bench-bullet-lock-free = Lock-free reads
bench-bullet-optimistic = Optimistic concurrency
bench-bullet-btree = In-memory B-tree
bench-bullet-prepared = Prepared statement caching
bench-bullet-bulk-inserts = Bulk inserts
bench-bullet-non-indexed = Non-indexed updates
bench-bullet-deletes = Deletes
bench-bullet-connection-pooling = Connection pooling
bench-bullet-stmt-caching = Prepared statement caching
bench-bullet-extended-protocol = Extended query protocol
bench-bullet-concurrency = Lightweight concurrency
bench-bullet-protocol = Protocol efficiency
bench-bullet-writes = Write operations
bench-bullet-reads = Read operations
bench-bullet-feature-flags = Feature flags
bench-bullet-lto = LTO optimization
bench-bullet-modular = Modular builds
bench-bullet-streaming = Streaming compilation
bench-bullet-indexeddb = IndexedDB persistence
bench-bullet-worker = Worker thread support

# =============================================================================
# Conformance Page
# =============================================================================

# Overview section
conformance-sql-conformance = SQL Conformance
conformance-testing-against = Testing against SQLLogicTest - the industry standard SQL test suite
conformance-full-pass-rate = 100% File Pass Rate Achieved!
conformance-tests-passing = Tests Passing
conformance-files-passing = Files Passing
conformance-loading = Loading conformance report...
conformance-error-loading = Error Loading Report
conformance-no-data = No conformance data available

# Category breakdown
conformance-category-title = Test Coverage by Category
conformance-category-header = Category
conformance-pass-rate-header = Pass Rate
conformance-progress-header = Progress
conformance-tests-header = Tests
conformance-cat-select = SELECT Queries
conformance-cat-aggregates = Aggregates
conformance-cat-joins = JOINs
conformance-cat-expressions = Expressions
conformance-cat-subqueries = Subqueries
conformance-cat-index = Index Operations
conformance-cat-ddl = DDL Statements
conformance-cat-evidence = Evidence Tests
conformance-cat-random = Random Tests
conformance-cat-other = Other Tests

# Timeline
conformance-timeline-title = Pass Rate History
conformance-timeline-desc = Conformance progress over the last 90 days
conformance-timeline-loading = Loading chart data...

# Milestones
conformance-milestones-title = Milestones

# Running tests locally
conformance-running-locally-title = Running Tests Locally
conformance-run-sqltest = # Run SQL:1999 conformance tests
conformance-run-sqllogictest = # Run SQLLogicTest suite (takes hours)
conformance-generate-coverage = # Generate coverage report
conformance-open-coverage = # Open coverage report

# Legacy sqltest section
conformance-sqltest-title = sqltest Results
conformance-sqltest-desc = Results from <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">sqltest</a> - a community-maintained BNF-driven conformance test suite derived from the SQL:1999 standard, containing 739 tests covering Core and Foundation features.
conformance-overall-pass-rate = Overall Pass Rate
conformance-tests-of-passing = { $passed } of { $total } tests passing
conformance-passed = Passed
conformance-failed = Failed
conformance-errors = Errors
conformance-test-coverage = Test Coverage
conformance-core-features = Core Features (E-Series)
conformance-additional-features = Additional Features

# Feature codes
conformance-e011 = Numeric data types
conformance-e021 = Character string types
conformance-e031 = Identifiers
conformance-e051 = Basic query specification
conformance-e061 = Basic predicates and search conditions
conformance-e071 = Basic query expressions
conformance-e081 = Basic privileges
conformance-e091 = Set functions
conformance-e101 = Basic data manipulation
conformance-e111 = Single row SELECT statement
conformance-e121 = Basic cursor support
conformance-e131 = Null value support
conformance-e141 = Basic integrity constraints
conformance-e151 = Transaction support
conformance-e161 = SQL comments
conformance-f031 = Basic schema manipulation

# SQLLogicTest section
conformance-slt-title = SQLLogicTest Results
conformance-slt-desc = Results from the comprehensive <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">SQLLogicTest</a> suite containing ~{ $testCases } tests across { $testFiles } test files from the official SQLite corpus.
conformance-files-of-passing = { $passed } of { $total } test files passing
conformance-test-categories = Test Categories
conformance-slt-select = SELECT Tests
conformance-slt-evidence = Evidence Tests
conformance-slt-index = Index Tests
conformance-slt-random = Random Tests
conformance-slt-ddl = DDL Tests
conformance-slt-other = Other Tests
conformance-slt-note = <strong>Note:</strong> SQLLogicTest provides a different perspective from sqltest. While sqltest focuses on BNF grammar conformance from the SQL:1999 specification, SQLLogicTest contains millions of real-world SQL queries testing practical correctness across a wide range of scenarios.

# Explanation section
conformance-explanation-title = Understanding Our Test Suites
conformance-what-is-sqltest = What is sqltest?
conformance-sqltest-explanation = <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">sqltest</a> is a community-maintained test suite by Elliot Chance that provides BNF-driven conformance tests derived from the SQL:1999 standard. It contains 739 tests covering Core and Foundation features across E-series and F-series test categories. This suite tests whether our implementation conforms to the SQL:1999 grammar specification.
conformance-what-is-slt = What is SQLLogicTest?
conformance-slt-explanation = <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">SQLLogicTest</a> is a comprehensive test suite originally developed for SQLite, containing ~{ $testCases } SQL test cases across { $testFiles } test files. It tests practical correctness by running real-world queries and validating results. This suite focuses on semantic correctness and edge cases rather than pure grammar conformance.
conformance-how-complement = How do they complement each other?
conformance-sqltest-validates = <span class="font-medium">sqltest (BNF-driven):</span> Validates grammar conformance to SQL:1999 standard specifications
conformance-slt-validates = <span class="font-medium">SQLLogicTest (Result-driven):</span> Validates semantic correctness with millions of real queries
conformance-coverage-point = <span class="font-medium">Coverage:</span> sqltest covers 739 standard feature tests; SQLLogicTest covers practical scenarios
conformance-philosophy-point = <span class="font-medium">Philosophy:</span> sqltest says "can you parse this?"; SQLLogicTest says "does this work correctly?"
conformance-what-is-core = What is SQL:1999 Core?
conformance-core-explanation = SQL:1999 Core is the official mandatory feature set defined in the SQL:1999 (ISO/IEC 9075:1999) standard. It consists of approximately 169 required features that any database claiming Core compliance must implement. Official Core compliance is verified through the NIST SQL Test Suite, not community test suites.
conformance-what-mean = What do our pass rates mean?
conformance-pass-rates-mean = Our <strong>{ $sqltestRate }% sqltest pass rate</strong> ({ $sqltestPassed }/{ $sqltestTotal } tests) demonstrates strong SQL:1999 grammar conformance. { $sltInfo } Together, these results indicate comprehensive SQL:1999 compliance, though they do not constitute official Core certification.
conformance-slt-pass-info = Our <strong>{ $sltRate }% SQLLogicTest pass rate</strong> ({ $sltPassed }/{ $sltTotal } test files) shows we handle real-world queries correctly.
conformance-bottom-line = <strong>Bottom Line:</strong> We use two complementary test suites to ensure both standards conformance (sqltest) and practical correctness (SQLLogicTest). High pass rates in both demonstrate serious SQL:1999 implementation quality, though formal Core certification would require testing against official NIST suites.

# Failing tests section
conformance-failing-tests-title = Failing Tests
conformance-failing-tests-desc = The following tests are currently failing. Click to expand details.
conformance-view-failing = View failing test details ({ $count } tests)
conformance-error-label = Error:

# PostgreSQL Regression Tests section
conformance-pgsql-title = PostgreSQL Regression Tests
conformance-pgsql-desc = Results from running <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">PostgreSQL's regression test suite</a> - the canonical test suite used to validate PostgreSQL compatibility.
conformance-pgsql-tests-passing = tests passing
conformance-pgsql-tests-excluded = tests excluded
conformance-pgsql-pass-rate = Pass Rate
conformance-pgsql-excluded-reason = Excluded tests use PostgreSQL-specific features not applicable to VibeSQL
conformance-pgsql-note = <strong>Note:</strong> PostgreSQL regression tests validate SQL behavior against PostgreSQL's reference implementation. Excluded tests involve PostgreSQL-specific features like system catalogs, procedural languages, or extension modules.

# SQLite TCL Test Suite section
conformance-tcl-title = SQLite TCL Test Suite
conformance-tcl-desc = Results from SQLite's canonical <a href="https://www.sqlite.org/testing.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">TCL test suite</a> containing { $fileCount } test files. This suite is the gold standard for SQLite compatibility testing.
conformance-tcl-overall-rate = Overall Pass Rate
conformance-tcl-tests-passing = { $passed } of { $total } tests passing
conformance-tcl-passed = Passed
conformance-tcl-failed = Failed
conformance-tcl-skipped = Skipped
conformance-tcl-total = Total
conformance-tcl-categories-title = Test Categories
conformance-tcl-category = Category
conformance-tcl-rate = Rate
conformance-tcl-progress = Progress
conformance-tcl-tests = Tests
conformance-tcl-common-failures = Common Failures
conformance-tcl-failure-patterns = Top { $count } failure patterns by occurrence count
conformance-tcl-about-title = About TCL Tests:
conformance-tcl-about-text = SQLite's TCL test suite is the canonical conformance test for SQLite compatibility. It tests specific SQLite behaviors, quirks, and edge cases that may not be covered by standard SQL test suites. High pass rates here indicate strong SQLite compatibility for application migration scenarios.

# Metadata
conformance-generated = Generated:
conformance-commit = Commit:
conformance-status = Status:

# =============================================================================
# Challenge Page
# =============================================================================

# Page title and header
challenge-page-title = SQL Vibe Coding Challenge - VibeSQL
challenge-header = SQL Vibe Coding Challenge

# Hero section
challenge-hero-title = The SQL Vibe Coding Challenge
challenge-hero-subtitle = An objective benchmark for multi-agent software development. Build a SQL database from scratch. Pass 6 million tests. Win the trophy.
challenge-btn-start = Start Building
challenge-btn-trophy = See The Trophy
challenge-btn-leaderboard = Leaderboard

# Key Insight callout
challenge-insight-title = The Only Metric That Matters: Calendar Time
challenge-insight-text = Commits and lines of code are proxies. What matters is <strong>days to completion</strong>. Can 1,000 agents working in parallel beat 100 agents? Does your orchestration framework maintain productivity as you scale? This benchmark will tell you.

# The Challenge section
challenge-section-title = The Challenge
challenge-objective-title = Objective
challenge-objective-text = Build a SQL database engine from scratch that passes the <a href="https://www.sqlite.org/sqllogictest/" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">SQLLogicTest suite</a>. This is the same test suite used to validate SQLite, DuckDB, and other production databases.
challenge-success-title = Success Criteria
challenge-success-pass-rate = 100% pass rate on SQLLogicTest suite
challenge-success-assertions = ~6 million individual test assertions
challenge-success-files = All 622 test files passing
# Execution Boundary Rule
challenge-rule-boundary-title = The Rule: Execution Boundary
challenge-rule-boundary-text = Existing database systems may be studied, benchmarked, and used for external analysis, but they must never cross the execution boundary of your submitted system.
challenge-rule-outside-title = Outside the Boundary (Allowed)
challenge-rule-study = Study source code and algorithms
challenge-rule-benchmark = Benchmark to identify slow query classes
challenge-rule-compare = Compare query plans and behaviors
challenge-rule-guide = Use to guide design decisions
challenge-rule-inside-title = Inside the Boundary (Disqualified)
challenge-rule-execute = Execute queries via existing engines
challenge-rule-fallback = Use as fallback for unsupported features
challenge-rule-oracle = Use as correctness oracle during tests
challenge-rule-link = Link, embed, or invoke at runtime
challenge-rule-test = Key test: Removing SQLite must not change whether your engine builds, runs, or passes tests.

# Do/Don't Examples
challenge-examples-title = Do / Don't Examples
challenge-examples-do = DO (Allowed)
challenge-examples-dont = DON'T (Disqualified)
challenge-do-1-title = Read SQLite source code to understand B-tree implementation
challenge-do-1-why = Study and learn from existing implementations
challenge-do-2-title = Run DuckDB to benchmark JOIN performance and identify optimization targets
challenge-do-2-why = External analysis to guide your implementation
challenge-do-3-title = Compare your query plan output against PostgreSQL's EXPLAIN
challenge-do-3-why = Learning tool that doesn't affect your execution
challenge-do-4-title = Use scripts that call SQLite to pre-compute expected test outputs
challenge-do-4-why = Offline reference data, not runtime dependency
challenge-dont-1-title = Fall back to SQLite for window functions "temporarily"
challenge-dont-1-why = Crosses execution boundary, even if planned for removal
challenge-dont-2-title = Run queries through SQLite to verify correctness during tests
challenge-dont-2-why = Using as oracle means tests depend on external engine
challenge-dont-3-title = Link against libsqlite3 for "just the parser"
challenge-dont-3-why = Embedded dependency violates clean-room implementation
challenge-dont-4-title = Shell out to DuckDB for complex aggregation queries
challenge-dont-4-why = Delegation of execution, regardless of method
challenge-faq-title = Common Question
challenge-faq-q = Q: Can I use SQLite during development?
challenge-faq-a = A: Yes — for reading code, benchmarking, and analysis. No — for executing queries, validating correctness, or acting as part of your system.

# The Trophy section
challenge-trophy-title = The Trophy
challenge-trophy-name = The Vibe Coding Trophy
challenge-trophy-desc = A physical trophy will be awarded to each record holder. The design reflects the spirit of "vibe coding" — a gold-plated wand mounted on walnut with brass nameplates.
challenge-trophy-claim = <strong>Your name goes on the trophy</strong> when you beat the current record by at least 5%.
challenge-rules-title = Award Rules
challenge-rule-improve = <strong>5% improvement required</strong> — beat the previous record by at least 5% (in calendar days) to claim the trophy
challenge-rule-public = <strong>Public repository</strong> — your code must be publicly available for verification
challenge-rule-pass = <strong>100% pass rate</strong> — all 622 SQLLogicTest files must pass
challenge-rule-git = <strong>Verifiable git history</strong> — first commit date to 100% pass rate determines your time
challenge-record-title = Current Record Holder
challenge-record-days = { $days } days
challenge-record-name = VibeSQL (Baseline)
challenge-record-date = October - November 2025
challenge-record-beat = Beat this by 5%? That's <strong>24 days or less</strong> to claim the trophy.

# Why This Challenge section
challenge-why-title = Why This Challenge?
challenge-why-objective-title = Objective Measurement
challenge-why-objective-text = No subjective code reviews. Either the tests pass or they don't. 6 million assertions leave no room for ambiguity.
challenge-why-complexity-title = Real Complexity
challenge-why-complexity-text = SQL databases require parsers, optimizers, and execution engines. This isn't a toy problem—it's production-grade engineering.
challenge-why-time-title = Time Is Truth
challenge-why-time-text = Calendar days to completion is the ultimate metric. Does parallelizing to 1,000 agents help? Now you can find out.

# Get Started section
challenge-start-title = Get Started
challenge-start-intro = Start from scratch in any language, or use one of our seed repos for convenience. Each seed includes the SQLLogicTest suite, a test runner, and CI workflow.
challenge-seed-title = Seed Repos
challenge-seed-optional = (optional)
challenge-seed-rust-desc = Cargo build system, zero-cost abstractions, memory safety without GC.
challenge-seed-cpp-desc = CMake build system, maximum performance, full control over memory.
challenge-seed-go-desc = Simple toolchain, fast compilation, excellent concurrency primitives.
challenge-seed-fork = Fork on GitHub →
challenge-step1-title = Start Your Project
challenge-step1-text = Create a new repo from scratch, or fork a seed above for a head start. Get the <a href="https://www.sqlite.org/sqllogictest/" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">SQLLogicTest suite</a>. Your first commit starts the clock.
challenge-step2-title = Build Your Database
challenge-step2-text = Implement a SQL parser, query executor, and storage engine. Use any AI tools—Claude, Copilot, or your own agents. Run <code class="bg-gray-200 dark:bg-gray-700 px-1 rounded">make test</code> to track progress.
challenge-step3-title = Hit 100% and Submit
challenge-step3-text = When all 622 test files pass, open an issue at <a href="https://github.com/vibesql-challenge/submissions" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">vibesql-challenge/submissions</a> with your repo link and commit hashes. Beat 25 days to join the leaderboard.

# Explore VibeSQL section
challenge-explore-title = Explore VibeSQL
challenge-explore-demo-title = Try the Demo
challenge-explore-demo-text = Run SQL queries in your browser using the WebAssembly build.
challenge-explore-conformance-title = Conformance Report
challenge-explore-conformance-text = Detailed breakdown of SQL:1999 standards compliance.
challenge-explore-benchmarks-title = Performance Benchmarks
challenge-explore-benchmarks-text = TPC-H, TPC-C, and other benchmarks vs SQLite and DuckDB.

# Footer
challenge-footer = VibeSQL - SQL:1999 Database in WebAssembly
