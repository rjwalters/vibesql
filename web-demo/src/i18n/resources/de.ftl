# VibeSQL Web UI - Deutsch

# Page titles
page-title = VibeSQL - KI-gestützte SQL:1999 Datenbank
demo-title = VibeSQL Demo
benchmarks-title = Leistungsbenchmarks - VibeSQL
benchmarks-heading = VibeSQL - Leistungsbenchmarks
conformance-title = Konformitätsbericht - VibeSQL
conformance-heading = Konformitätsbericht
conformance-subtitle = SQL:1999 Standardkonformitätstest

# Navigation
nav-showcase = SQL:1999 Showcase
nav-conformance = sqltest-Ergebnisse anzeigen
nav-sqllogictest = SQLLogicTest-Ergebnisse anzeigen

# Editor section
editor-title = SQL-Editor
editor-storage = Speicher
editor-storage-init = Initialisierung...
editor-execute = Abfrage ausführen

# Results section
results-title = Ergebnisse
results-empty = Führen Sie eine Abfrage aus, um Ergebnisse zu sehen
results-loading = Laden...
results-rows = { $count } { $count ->
    [one] Zeile
   *[other] Zeilen
}
results-rows-with-time = { $count } { $count ->
    [one] Zeile
   *[other] Zeilen
} ({ $time }ms)
results-copy = In Zwischenablage kopieren
results-export = Als CSV exportieren
results-limit-warning = Zeige die ersten { $limit } von { $total } Zeilen. Verwenden Sie LIMIT, um Ihre Abfrage einzugrenzen.

# Examples sidebar
examples-title = Beispiele
examples-basic = Grundlegende Abfragen
examples-advanced = Erweiterte Abfragen

# Database selector
db-select-label = Datenbank

# Footer
footer-tagline = VibeSQL - SQL:1999 Datenbank in WebAssembly
footer-deployed = Bereitgestellt: { $date }

# Theme
theme-toggle-dark = Zum Dunkelmodus wechseln
theme-toggle-light = Zum Hellmodus wechseln

# Locale
locale-select = Sprache auswählen

# Messages
msg-query-success = Abfrage erfolgreich ausgeführt
msg-rows-affected = { $count } { $count ->
    [one] Zeile betroffen
   *[other] Zeilen betroffen
}

# Errors
error-generic = Ein Fehler ist aufgetreten
error-query-failed = Abfrage fehlgeschlagen

# Editor
editor-placeholder = SQL-Abfrage hier eingeben... (Strg+Enter oder Cmd+Enter zum Ausführen)

# Navigation links
nav-terminal = SQL-Terminal Demo
nav-compliance = SQL-Konformitätsbericht
nav-benchmarks = Leistungsbenchmarks
nav-github = GitHub Repository
nav-home = Startseite

# Results
results-success-zero = Abfrage erfolgreich ausgeführt (0 Zeilen)
results-null = NULL

# Help Modal
help-title = Tastenkürzel & Hilfe
help-close = Schließen
help-editor-shortcuts = Editor-Tastenkürzel
help-navigation = Navigation
help-results-actions = Ergebnis-Aktionen
help-tips = Tipps
help-shortcut-execute = Aktuelle Abfrage ausführen
help-shortcut-comment = Zeilenkommentar umschalten
help-shortcut-indent = Auswahl einrücken
help-shortcut-show-help = Diesen Hilfedialog anzeigen
help-shortcut-close-help = Hilfedialog schließen
help-action-copy = In Zwischenablage kopieren
help-action-copy-desc = Ergebnisse als tabulatorgetrennte Werte kopieren
help-action-export = Als CSV exportieren
help-action-export-desc = Ergebnisse als CSV-Datei herunterladen
help-tip-limit = Ergebnisse sind auf 1.000 Zeilen begrenzt. Verwenden Sie LIMIT, um Abfragen einzugrenzen.
help-tip-time = Die Ausführungszeit wird mit den Abfrageergebnissen angezeigt.
help-tip-syntax = Der Editor unterstützt SQL-Syntaxhervorhebung und Autovervollständigung.
help-tip-theme = Wechseln Sie zwischen Hell-/Dunkelmodus mit der Thema-Schaltfläche.
help-got-it = Verstanden!

# Showcase Navigation
showcase-title = SQL:1999 Core Showcase
showcase-description = Erkunden Sie die implementierten SQL:1999 Core-Funktionen interaktiv
showcase-complete = { $percent }% Abgeschlossen
showcase-categories = Funktionskategorien
showcase-legend = Statuslegende
showcase-status-implemented = Vollständig Implementiert
showcase-status-partial = Teilweise Implementiert
showcase-status-planned = Geplant

# Showcase category labels
showcase-cat-compliance = Konformitäts-Dashboard
showcase-cat-data-types = Datentypen
showcase-cat-dml = DML-Operationen
showcase-cat-predicates = Prädikate & Operatoren
showcase-cat-joins = JOINs
showcase-cat-subqueries = Unterabfragen
showcase-cat-aggregates = Aggregate & GROUP BY
showcase-cat-ddl = DDL & Constraints

# Common showcase elements
showcase-interactive-examples = Interaktive Beispiele
showcase-try-example = Beispiel Ausprobieren
showcase-progress = { $implemented } von { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Kategorie
showcase-table-description = Beschreibung
showcase-table-syntax = Syntax
showcase-table-use-case = Anwendungsfall

# Status labels
status-implemented = Implementiert
status-partial = Teilweise
status-planned = Geplant

# Aggregates Showcase
aggregates-title = SQL-Aggregate und GROUP BY
aggregates-description = SQL:1999 Core Aggregatfunktionen und Gruppierungsfähigkeiten
aggregates-reference = Aggregatfunktionen-Referenz
aggregates-table-function = Funktion
aggregates-progress-type = Funktionen
aggregates-ex-basic = Grundlegende Aggregatfunktionen
aggregates-ex-group-single = GROUP BY (Einzelne Spalte)
aggregates-ex-group-multiple = GROUP BY (Mehrere Spalten)
aggregates-ex-having = HAVING-Klausel
aggregates-ex-orderby = ORDER BY mit Aggregaten
aggregates-ex-null = NULL-Behandlung in Aggregaten

# DML Operations Showcase
dml-title = DML-Operationen (Datenmanipulationssprache)
dml-description = SQL:1999 Core-Operationen zum Abfragen und Ändern von Daten
dml-reference = DML-Operationen Referenz
dml-table-operation = Operation
dml-progress-type = Operationen
dml-ex-select-basic = SELECT - Grundlegende Abfragen
dml-ex-select-ordering = SELECT - Sortierung und Limitierung
dml-ex-insert = INSERT-Operationen
dml-ex-update = UPDATE-Operationen
dml-ex-delete = DELETE-Operationen
dml-ex-combined = Kombinierter CRUD-Workflow

# Data Types Showcase
datatypes-title = SQL:1999 Core Datentypen
datatypes-description = Erkunden Sie die grundlegenden Datentypen der SQL:1999 Core-Spezifikation
datatypes-reference = Datentypen-Referenz
datatypes-table-type = Typname
datatypes-table-example = Beispielwerte
datatypes-table-spec = Spezifikation
datatypes-progress-type = Typen
datatypes-ex-numeric = Arbeiten mit numerischen Typen
datatypes-ex-null = NULL-Behandlung & Dreiwertige Logik
datatypes-ex-comparisons = Typvergleiche & Operationen

# JOINs Showcase
joins-title = SQL JOINs
joins-description = SQL:1999 Core JOIN-Operationen zum Kombinieren von Daten aus mehreren Tabellen
joins-reference = JOIN-Typen Referenz
joins-table-type = JOIN-Typ
joins-progress-type = JOIN-Typen
joins-category-suffix = JOINs
joins-ex-sample = Beispieldaten-Setup
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Multi-Tabellen JOIN

# Predicates Showcase
predicates-title = Prädikate und Operatoren
predicates-description = SQL:1999 Prädikate für Filterung und logische Operationen
predicates-reference = Prädikaten-Referenz
predicates-table-predicate = Prädikat
predicates-progress-type = Prädikate
predicates-ex-comparison = Vergleichsoperatoren
predicates-ex-between = BETWEEN und Bereichsprädikate
predicates-ex-null = NULL-Prädikate und Dreiwertige Logik
predicates-ex-boolean = Boolesche Logik (AND, OR, NOT)
predicates-ex-in = IN-Prädikat mit Unterabfragen
predicates-ex-combined = Kombinierte Prädikat-Operationen

# Subqueries Showcase
subqueries-title = SQL-Unterabfragen
subqueries-description = SQL:1999 Core Unterabfrage-Fähigkeiten für verschachtelte Abfrageoperationen
subqueries-reference = Unterabfragetypen-Referenz
subqueries-table-type = Unterabfragetyp
subqueries-progress-type = Unterabfragetypen
subqueries-ex-scalar-select = Skalare Unterabfrage in SELECT
subqueries-ex-scalar-where = Skalare Unterabfrage in WHERE
subqueries-ex-derived = Abgeleitete Tabellen (Unterabfrage in FROM)
subqueries-ex-in = IN-Prädikat mit Unterabfrage
subqueries-ex-correlated = Korrelierte Unterabfragen
subqueries-ex-nested = Verschachtelte Unterabfragen

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

# Table headers
bench-table-operation = Operation
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
bench-tpcds-note-remaining = <strong>Note:</strong> Remaining unsupported queries require features like INTERSECT/EXCEPT or specific date arithmetic functions not yet implemented.

# TPC-DS Discussion
bench-tpcds-disc-coverage-title = SQL:1999 Feature Coverage
bench-tpcds-disc-coverage = TPC-DS exercises the most demanding SQL features. VibeSQL passes <strong>88 of 99 queries</strong>, demonstrating broad coverage of SQL:1999 including ROLLUP, CUBE, GROUPING(), window functions with complex framing, and recursive CTEs. The remaining queries require INTERSECT/EXCEPT set operations.
bench-tpcds-disc-optimization-title = Complex Query Optimization
bench-tpcds-disc-optimization = TPC-DS queries often join 10+ tables with correlated subqueries. Current focus areas:
bench-tpcds-disc-cte = Intelligent decision between materialized and inline CTEs
bench-tpcds-disc-decorrelation = Converting correlated subqueries to joins when beneficial
bench-tpcds-disc-star = Fact-dimension join ordering for analytical patterns
bench-tpcds-disc-toward-title = Toward 99/99
bench-tpcds-disc-toward = INTERSECT and EXCEPT are planned additions that will enable the remaining queries. These set operations fit naturally into our existing query algebra and will be implemented as hash-based operators similar to our DISTINCT processing.

# TPC-C specific
bench-tpcc-name = TPC-C
bench-tpcc-title = TPC-C Online Transaction Processing Benchmark
bench-tpcc-description = The <strong>TPC-C benchmark</strong> simulates a complete order-entry environment with a mix of complex transactions including order entry, payment processing, order status queries, delivery processing, and stock level monitoring.
bench-tpcc-ops-label = TPC-C transactions
bench-tpcc-note-intro = TPC-C measures transactions per minute (tpmC) and tests the database's ability to handle concurrent transactions with complex business logic. This benchmark is critical for evaluating <strong>transactional workload performance</strong>.
bench-tpcc-note-results = <strong>Note:</strong> Results show average transaction latency. Lower is better. TPC-C is particularly demanding for write-heavy workloads with strict consistency requirements.

# TPC-C Transaction Descriptions
bench-tpcc-new-order = New Order - Complex transaction with inventory checks and order creation
bench-tpcc-payment = Payment - Update customer balance and warehouse/district totals
bench-tpcc-order-status = Order Status - Read-only query for customer order history
bench-tpcc-delivery = Delivery - Batch processing of pending orders
bench-tpcc-stock-level = Stock Level - Count items below threshold in recent orders

# TPC-C Discussion
bench-tpcc-disc-faster-title = 42x Faster Than SQLite
bench-tpcc-disc-faster = VibeSQL achieves <strong>~79,000 transactions per second</strong> compared to SQLite's ~1,900 TPS, a 42x improvement. This dramatic speedup comes from our lock-free MVCC architecture that avoids SQLite's coarse-grained locking on every write operation.
bench-tpcc-disc-dominates-title = Why VibeSQL Dominates OLTP
bench-tpcc-disc-lockfree = MVCC allows readers and writers to proceed concurrently without blocking
bench-tpcc-disc-optimistic = Transactions only conflict at commit time, not during execution
bench-tpcc-disc-btree = Purpose-built index structure optimized for in-memory workloads
bench-tpcc-disc-prepared = Query plans are compiled once and reused
bench-tpcc-disc-scaling-title = Scaling Further
bench-tpcc-disc-scaling = Current results are single-threaded. VibeSQL's architecture supports multi-threaded transaction processing, and we expect near-linear scaling as we add parallel execution support. Our goal is to achieve 500K+ TPS on modern multi-core hardware.

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
bench-sysbench-emb-disc-point-title = Point Lookups: VibeSQL Leads
bench-sysbench-emb-disc-point = VibeSQL's direct API achieves <strong>~137ns per point select</strong>, matching SQLite and vastly outperforming DuckDB (~140µs). Our B-tree implementation is optimized for single-row lookups with minimal pointer chasing and cache-friendly node layouts.
bench-sysbench-emb-disc-index-title = Index Updates: 2x Faster
bench-sysbench-emb-disc-index = VibeSQL's indexed updates run at <strong>~740ns vs SQLite's ~1.6µs</strong>. Our MVCC design allows in-place index updates without write-ahead logging overhead for each operation.
bench-sysbench-emb-disc-improve-title = Areas for Improvement
bench-sysbench-emb-disc-bulk = SQLite's batch insert path is highly optimized; we're adding batched B-tree operations
bench-sysbench-emb-disc-nonindex = Full table scans for non-indexed columns need predicate pushdown optimization
bench-sysbench-emb-disc-deletes = Our tombstone-based deletion has cleanup overhead; compaction improvements are planned
bench-sysbench-emb-disc-duckdb-title = DuckDB Comparison
bench-sysbench-emb-disc-duckdb = DuckDB is optimized for analytical workloads, not micro-operations. Its 100-1000x slower results here reflect architectural choices (columnar storage, vectorized execution) that trade single-row latency for bulk throughput. VibeSQL targets both use cases.

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
bench-sysbench-srv-disc-mysql = Server benchmarks compare against MySQL to evaluate VibeSQL as a drop-in replacement for traditional client-server databases. Results vary by operation type, with VibeSQL showing advantages in read-heavy workloads.
bench-sysbench-srv-disc-roadmap-title = Server Roadmap
bench-sysbench-srv-disc-pooling = Reduce connection establishment overhead for high-throughput scenarios
bench-sysbench-srv-disc-caching = Server-side caching of query plans across connections
bench-sysbench-srv-disc-extended = Full PostgreSQL extended query protocol support for batch operations

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
bench-footprint-emb-disc-size = VibeSQL at <strong>~17MB</strong> sits between SQLite (~5MB) and DuckDB (~45MB). This reflects our choice to include advanced features (window functions, CTEs, columnar execution) while keeping the binary manageable for embedded deployments.
bench-footprint-emb-disc-startup-title = Startup: Fastest Cold Start
bench-footprint-emb-disc-startup = VibeSQL achieves <strong>~7.7ms cold startup</strong>, slightly faster than SQLite (~8.2ms) and significantly faster than DuckDB (~14.6ms). Our minimal initialization path loads only essential metadata structures on startup.
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
bench-footprint-srv-disc-wasm-title = WASM: 2.2MB Compressed
bench-footprint-srv-disc-wasm = VibeSQL's WebAssembly module compresses to <strong>~2.2MB gzipped</strong>, enabling fast initial page loads. This is a full SQL:1999 database with window functions, CTEs, and ACID transactions running entirely in the browser.
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
bench-bullet-feature-flags = Feature flags
bench-bullet-lto = LTO optimization
bench-bullet-modular = Modular builds
bench-bullet-streaming = Streaming compilation
bench-bullet-indexeddb = IndexedDB persistence
bench-bullet-worker = Worker thread support
