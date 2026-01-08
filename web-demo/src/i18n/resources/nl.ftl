# VibeSQL Web UI - Nederlands

# Page titles
page-title = VibeSQL - AI-aangedreven SQL:1999 Database
demo-title = VibeSQL Demo
benchmarks-title = Prestatiebenchmarks - VibeSQL
benchmarks-heading = VibeSQL - Prestatiebenchmarks
conformance-title = Conformiteitsrapport - VibeSQL
conformance-heading = Conformiteitsrapport
conformance-subtitle = SQL:1999 Standaard Conformiteitstest

# Navigation
nav-showcase = SQL:1999 Showcase
nav-conformance = Bekijk sqltest Resultaten
nav-sqllogictest = Bekijk SQLLogicTest Resultaten

# Editor section
editor-title = SQL Editor
editor-storage = Opslag
editor-storage-init = Initialiseren...
editor-execute = Query Uitvoeren

# Results section
results-title = Resultaten
results-empty = Voer een query uit om resultaten te zien
results-loading = Laden...
results-rows = { $count } { $count ->
    [one] rij
   *[other] rijen
}
results-rows-with-time = { $count } { $count ->
    [one] rij
   *[other] rijen
} ({ $time }ms)
results-copy = Kopiëren naar klembord
results-export = CSV Exporteren
results-limit-warning = Toont de eerste { $limit } van { $total } rijen. Gebruik LIMIT om uw query te verfijnen.

# Examples sidebar
examples-title = Voorbeelden
examples-basic = Basis Queries
examples-advanced = Geavanceerde Queries

# Database selector
db-select-label = Database

# Footer
footer-tagline = VibeSQL - SQL:1999 Database in WebAssembly
footer-deployed = Geïmplementeerd: { $date }

# Theme
theme-toggle-dark = Schakel naar donkere modus
theme-toggle-light = Schakel naar lichte modus

# Locale
locale-select = Selecteer taal

# Messages
msg-query-success = Query succesvol uitgevoerd
msg-rows-affected = { $count } { $count ->
    [one] rij beïnvloed
   *[other] rijen beïnvloed
}

# Errors
error-generic = Er is een fout opgetreden
error-query-failed = Query mislukt
error-no-databases = Geen databases beschikbaar

# Loading states
loading-initializing-theme = Thema wordt geïnitialiseerd
loading-preparing-editor = Editor wordt voorbereid
loading-database-engine = Database-engine wordt geladen
loading-setting-up-ui = Gebruikersinterface wordt ingesteld
loading-editor = Editor wordt geladen...
loading-compliance-data = Nalevingsgegevens worden geladen...
loading-conformance-report = Conformiteitsrapport wordt geladen...

# Editor
editor-placeholder = Voer SQL-query hier in... (Ctrl+Enter of Cmd+Enter om uit te voeren)

# Navigation links
nav-terminal = SQL Terminal Demo
nav-compliance = SQL Test Conformiteitsrapport
nav-benchmarks = Prestatiebenchmarks
nav-github = GitHub Repository
nav-home = Home
nav-trends = Prestatietrends

# Trends page
trends-title = Prestatietrends - VibeSQL
trends-heading = VibeSQL - Prestatietrends
trends-total-runs = Totale Benchmark Uitvoeringen
trends-across-suites = over alle suites
trends-date-range = Datumbereik
trends-first-to-last = eerste tot laatste uitvoering
trends-latest-commit = Laatste Commit
trends-most-recent = meest recente benchmark
trends-generated = Gegenereerd
trends-last-export = laatste data-export

# Results
results-success-zero = Query succesvol uitgevoerd (0 rijen)
results-null = NULL

# Help Modal
help-title = Sneltoetsen & Hulp
help-close = Sluiten
help-editor-shortcuts = Editor Sneltoetsen
help-navigation = Navigatie
help-results-actions = Resultaat Acties
help-tips = Tips
help-shortcut-execute = Huidige query uitvoeren
help-shortcut-comment = Regelcommentaar wisselen
help-shortcut-indent = Selectie inspringen
help-shortcut-show-help = Dit hulpdialoog tonen
help-shortcut-close-help = Hulpdialoog sluiten
help-action-copy = Kopiëren naar klembord
help-action-copy-desc = Resultaten kopiëren als tab-gescheiden waarden
help-action-export = CSV Exporteren
help-action-export-desc = Resultaten downloaden als CSV-bestand
help-tip-limit = Resultaten zijn beperkt tot 1.000 rijen voor prestaties. Gebruik LIMIT om queries te verfijnen.
help-tip-time = Uitvoeringstijd wordt getoond bij queryresultaten.
help-tip-syntax = De editor ondersteunt SQL-syntaxiskleuring en automatische aanvulling.
help-tip-theme = Schakel tussen lichte/donkere modus met de themaknop.
help-got-it = Begrepen!

# Showcase Navigation
showcase-title = SQL:1999 Core Showcase
showcase-description = Verken de geïmplementeerde SQL:1999 Core functies interactief
showcase-complete = { $percent }% Voltooid
showcase-categories = Functiecategorieën
showcase-legend = Statuslegende
showcase-status-implemented = Volledig Geïmplementeerd
showcase-status-partial = Gedeeltelijk Geïmplementeerd
showcase-status-planned = Gepland

# Showcase category labels
showcase-cat-compliance = Conformiteitsdashboard
showcase-cat-data-types = Gegevenstypen
showcase-cat-dml = DML-operaties
showcase-cat-predicates = Predicaten & Operatoren
showcase-cat-joins = JOINs
showcase-cat-subqueries = Subqueries
showcase-cat-aggregates = Aggregaten & GROUP BY
showcase-cat-ddl = DDL & Beperkingen

# Common showcase elements
showcase-interactive-examples = Interactieve Voorbeelden
showcase-try-example = Dit Voorbeeld Proberen
showcase-progress = { $implemented } van { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Categorie
showcase-table-description = Beschrijving
showcase-table-syntax = Syntaxis
showcase-table-use-case = Gebruiksscenario

# Status labels
status-implemented = Geïmplementeerd
status-partial = Gedeeltelijk
status-planned = Gepland

# Aggregates Showcase
aggregates-title = SQL Aggregaten en GROUP BY
aggregates-description = SQL:1999 Core aggregaatfuncties en groeperingsmogelijkheden
aggregates-reference = Aggregaatfuncties Referentie
aggregates-table-function = Functie
aggregates-progress-type = functies
aggregates-ex-basic = Basis Aggregaatfuncties
aggregates-ex-group-single = GROUP BY (Enkele Kolom)
aggregates-ex-group-multiple = GROUP BY (Meerdere Kolommen)
aggregates-ex-having = HAVING Clausule
aggregates-ex-orderby = ORDER BY met Aggregaten
aggregates-ex-null = NULL Afhandeling in Aggregaten

# DML Operations Showcase
dml-title = DML-operaties (Data Manipulation Language)
dml-description = SQL:1999 Core operaties voor het bevragen en wijzigen van gegevens
dml-reference = DML-operaties Referentie
dml-table-operation = Operatie
dml-progress-type = operaties
dml-ex-select-basic = SELECT - Basisqueries
dml-ex-select-ordering = SELECT - Sorteren en Beperken
dml-ex-insert = INSERT Operaties
dml-ex-update = UPDATE Operaties
dml-ex-delete = DELETE Operaties
dml-ex-combined = Gecombineerde CRUD Workflow

# Data Types Showcase
datatypes-title = SQL:1999 Core Gegevenstypen
datatypes-description = Verken de fundamentele gegevenstypen gedefinieerd in de SQL:1999 Core specificatie
datatypes-reference = Gegevenstypen Referentie
datatypes-table-type = Typenaam
datatypes-table-example = Voorbeeldwaarden
datatypes-table-spec = Specificatie
datatypes-progress-type = typen
datatypes-ex-numeric = Werken met Numerieke Typen
datatypes-ex-null = NULL Afhandeling & Driewaardige Logica
datatypes-ex-comparisons = Typevergelijkingen & Operaties

# JOINs Showcase
joins-title = SQL JOINs
joins-description = SQL:1999 Core JOIN-operaties voor het combineren van gegevens uit meerdere tabellen
joins-reference = JOIN-typen Referentie
joins-table-type = JOIN Type
joins-progress-type = JOIN-typen
joins-category-suffix = JOINs
joins-ex-sample = Voorbeeldgegevens Setup
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Multi-tabel JOIN

# Predicates Showcase
predicates-title = Predicaten en Operatoren
predicates-description = SQL:1999 predicaten voor filtering en logische operaties
predicates-reference = Predicaten Referentie
predicates-table-predicate = Predicaat
predicates-progress-type = predicaten
predicates-ex-comparison = Vergelijkingsoperatoren
predicates-ex-between = BETWEEN en Bereikpredicaten
predicates-ex-null = NULL Predicaten en Driewaardige Logica
predicates-ex-boolean = Booleaanse Logica (AND, OR, NOT)
predicates-ex-in = IN Predicaat met Subqueries
predicates-ex-combined = Gecombineerde Predicaatoperaties

# Subqueries Showcase
subqueries-title = SQL Subqueries
subqueries-description = SQL:1999 Core subquerymogelijkheden voor geneste queryoperaties
subqueries-reference = Subquerytypen Referentie
subqueries-table-type = Subquerytype
subqueries-progress-type = subquerytypen
subqueries-ex-scalar-select = Scalaire Subquery in SELECT
subqueries-ex-scalar-where = Scalaire Subquery in WHERE
subqueries-ex-derived = Afgeleide Tabellen (Subquery in FROM)
subqueries-ex-in = IN Predicaat met Subquery
subqueries-ex-correlated = Gecorreleerde Subqueries
subqueries-ex-nested = Geneste Subqueries

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
bench-no-wasm-data = Geen WASM-gegevens beschikbaar
bench-no-server-data = Geen Sysbench server benchmark gegevens beschikbaar
bench-no-server-data-hint = Server benchmarks vereisen het uitvoeren van sysbench_server met MySQL vergelijking ingeschakeld.

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
bench-tpcc-disc-faster-title = { $speedup } Faster Than SQLite
bench-tpcc-disc-faster = VibeSQL achieves <strong>~{ $vibesqlTps } transactions per second</strong> compared to SQLite's ~{ $sqliteTps } TPS, a 42x improvement. This dramatic speedup comes from our lock-free MVCC architecture that avoids SQLite's coarse-grained locking on every write operation.
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
bench-sysbench-emb-disc-point-title = Point Lookups: { $pointRatio } Gap
bench-sysbench-emb-disc-point = VibeSQL's point selects run at <strong>~{ $pointVibesqlUs }µs vs SQLite's ~{ $pointSqliteUs }µs</strong>. This { $pointRatio } gap represents our primary OLTP optimization target - we're investigating B-tree node layout and lock-free read paths to close this gap.
bench-sysbench-emb-disc-index-title = Index Updates: { $indexRatio } Gap
bench-sysbench-emb-disc-index = VibeSQL's indexed updates run at <strong>~{ $indexVibesqlUs }µs vs SQLite's ~{ $indexSqliteUs }µs</strong>. This is an area for optimization as our MVCC design adds overhead for index maintenance that we're working to reduce.
bench-sysbench-emb-disc-improve-title = Areas for Improvement
bench-sysbench-emb-disc-bulk = SQLite's batch insert path is highly optimized; we're adding batched B-tree operations
bench-sysbench-emb-disc-nonindex = Non-indexed updates show VibeSQL at ~{ $nonIndexVibesqlUs }µs vs SQLite's ~{ $nonIndexSqliteUs }µs
bench-sysbench-emb-disc-deletes = Delete operations show VibeSQL at ~{ $deleteVibesqlUs }µs vs SQLite's ~{ $deleteSqliteUs }µs
bench-sysbench-emb-disc-architecture-title = Architecturale Afwegingen
bench-sysbench-emb-disc-architecture = De hybride architectuur van VibeSQL richt zich op zowel OLTP- als OLAP-workloads. Onze B-tree opslag biedt SQLite-competitieve puntopzoekprestaties, terwijl kolomgeoriënteerde uitvoering analytische queries efficiënt afhandelt. Dit verschilt van pure OLAP-databases zoals DuckDB die exclusief optimaliseren voor bulkoperaties ten koste van latentie op enkele rijen.

# Sysbench Server specific
bench-sysbench-server-name = Sysbench (Server)
bench-sysbench-server-title = Sysbench Micro-Benchmarks (Server)
bench-sysbench-server-description = <strong>Sysbench</strong> server benchmarks compare VibeSQL Server (PostgreSQL wire protocol) against MySQL, measuring performance for multi-client database deployments.
bench-sysbench-server-ops-label = Sysbench operations
bench-sysbench-server-note = Server mode uses the PostgreSQL wire protocol, enabling multi-client access and compatibility with existing PostgreSQL tooling and drivers.

# Sysbench Server Discussion
bench-sysbench-srv-disc-protocol-title = PostgreSQL Wire Protocol
bench-sysbench-srv-disc-protocol = VibeSQL Server implements the PostgreSQL wire protocol, enabling compatibility with existing PostgreSQL drivers and tools. This adds ~10-50µs of protocol overhead per query compared to embedded mode, but enables multi-client deployments.
bench-sysbench-srv-disc-mysql-title = MySQL Vergelijking
bench-sysbench-srv-disc-mysql = Server benchmarks vergelijken met MySQL om VibeSQL te evalueren als directe vervanging voor traditionele client-server databases. VibeSQL Server presteert beter dan MySQL op alle Sysbench operaties, met versnellingen van <strong>2,4x</strong> (range queries) tot <strong>12,8x</strong> (geïndexeerde updates).
bench-sysbench-srv-disc-perf-title = Waarom VibeSQL Server sneller is
bench-sysbench-srv-disc-perf-arch = De architectuur van VibeSQL verschilt fundamenteel van het traditionele RDBMS-ontwerp van MySQL
bench-sysbench-srv-disc-perf-storage = VibeSQL gebruikt een in-memory kolomgeoriënteerde storage engine geoptimaliseerd voor analytische en OLTP workloads, waarbij de overhead van MySQL's schijfgebaseerd InnoDB paginabeheer wordt vermeden
bench-sysbench-srv-disc-perf-locking = Geen zware row-level locking of MVCC-administratie—VibeSQL gebruikt lichtgewicht concurrency control ontworpen voor moderne multi-core CPU's
bench-sysbench-srv-disc-perf-protocol = Efficiënte PostgreSQL wire protocol implementatie met minimale serialisatie-overhead vergeleken met het MySQL protocol
bench-sysbench-srv-disc-perf-writes = Schrijfoperaties (inserts/updates) tonen de grootste winst (<strong>8-12x</strong>) omdat VibeSQL MySQL's redo log, undo log en doublewrite buffer synchronisatie vermijdt
bench-sysbench-srv-disc-perf-reads = Leesoperaties tonen kleinere maar consistente winst (<strong>2-3x</strong>) dankzij cache-efficiënte kolomgeoriënteerde toegangspatronen en nul disk I/O
bench-sysbench-srv-disc-roadmap-title = Server Roadmap
bench-sysbench-srv-disc-pooling = Reduce connection establishment overhead for high-throughput scenarios
bench-sysbench-srv-disc-caching = Server-side caching of query plans across connections
bench-sysbench-srv-disc-extended = Volledige PostgreSQL extended query protocol ondersteuning voor batch operaties

# TPC-H Server specifiek
bench-tpch-server-name = TPC-H (Server)
bench-tpch-server-title = TPC-H Analytische Benchmark (Server)
bench-tpch-server-description = <strong>TPC-H server benchmarks</strong> vergelijken VibeSQL Server (PostgreSQL protocol) met MySQL voor analytische query workloads, waarbij OLAP prestaties worden gemeten in client-server implementaties.
bench-tpch-server-ops-label = TPC-H queries
bench-tpch-server-note-intro = Server benchmarks testen de <strong>PostgreSQL protocol</strong> implementatie, waarbij end-to-end query latentie wordt gemeten inclusief netwerk overhead.
bench-tpch-server-note-queries = Queries testen complexe JOINs, subqueries en aggregaties die typisch zijn voor business intelligence workloads.

# TPC-H Server Discussie
bench-tpch-srv-disc-protocol-title = PostgreSQL Protocol
bench-tpch-srv-disc-protocol = VibeSQL Server spreekt het PostgreSQL protocol, waardoor standaard PostgreSQL drivers en tools kunnen worden gebruikt. Deze benchmark meet de volledige end-to-end latentie inclusief protocol overhead.
bench-tpch-srv-disc-comparison-title = MySQL Vergelijking
bench-tpch-srv-disc-comparison = Vergelijking met MySQL biedt een baseline voor traditionele client-server databases bij analytische workloads. De kolomgeoriënteerde uitvoeringsengine van VibeSQL biedt voordelen voor complexe aggregaties en joins.
bench-tpch-srv-disc-roadmap-title = Server OLAP Roadmap
bench-tpch-srv-disc-prepared = Hergebruik van gecompileerde query plannen over verbindingen
bench-tpch-srv-disc-pooling = Efficiënte verbindingsafhandeling voor high-throughput scenario's
bench-tpch-srv-disc-scale = Testen van grotere datasets (SF 0.1, SF 1.0) voor productie-schaal validatie

# TPC-C Server specifiek
bench-tpcc-server-name = TPC-C (Server)
bench-tpcc-server-title = TPC-C OLTP Benchmark (Server)
bench-tpcc-server-description = <strong>TPC-C server benchmarks</strong> vergelijken VibeSQL Server (PostgreSQL protocol) met MySQL voor OLTP transactie workloads, waarbij throughput wordt gemeten voor multi-client database implementaties.
bench-tpcc-server-ops-label = TPC-C transacties
bench-tpcc-server-note-intro = Server benchmarks testen de <strong>PostgreSQL protocol</strong> implementatie, waarbij transactie throughput wordt gemeten inclusief netwerk overhead.
bench-tpcc-server-note-results = Resultaten rapporteren transacties per seconde (TPS) voor de standaard TPC-C transactie mix.
bench-tpcc-mixed = Gemengde Workload - Standaard TPC-C transactie mix (45% Nieuwe-Order, 43% Betaling, 4% Order-Status, 4% Levering, 4% Voorraad-Niveau)

# TPC-C Server Discussie
bench-tpcc-srv-disc-protocol-title = PostgreSQL Protocol
bench-tpcc-srv-disc-protocol = VibeSQL Server spreekt het PostgreSQL protocol, waardoor standaard PostgreSQL drivers en tools kunnen worden gebruikt. Deze benchmark meet de volledige end-to-end transactie latentie inclusief protocol overhead.
bench-tpcc-srv-disc-comparison-title = MySQL Vergelijking
bench-tpcc-srv-disc-comparison = Vergelijking met MySQL biedt een baseline voor traditionele client-server databases bij OLTP workloads. MySQL is de industriestandaard voor transactionele workloads, en TPC-C is MySQL's sterke punt.
bench-tpcc-srv-disc-roadmap-title = Server OLTP Roadmap
bench-tpcc-srv-disc-prepared = Hergebruik van gecompileerde query plannen over verbindingen
bench-tpcc-srv-disc-pooling = Efficiënte verbindingsafhandeling voor high-throughput scenario's
bench-tpcc-srv-disc-parallel = Gelijktijdige multi-client transactieverwerking

# Footprint Embedded specifiek
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
bench-footprint-emb-disc-startup-title = Startup: Fastest Cold Start
bench-footprint-emb-disc-startup = VibeSQL achieves <strong>~7.7ms cold startup</strong>, slightly faster than SQLite (~{ $sqliteStartupMs }ms) and significantly faster than DuckDB (~{ $duckdbStartupMs }ms). Our minimal initialization path loads only essential metadata structures on startup.
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
bench-bullet-indexeddb = IndexedDB persistentie
bench-bullet-worker = Worker thread ondersteuning
bench-bullet-prepared-stmts = Prepared statements
bench-bullet-larger-scale = Grotere schaalfactoren
bench-bullet-parallel-clients = Parallelle clients

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

bench-table-query = Query
bench-tpcc-disc-duckdb = DuckDB behaalt slechts ~385 TPS op TPC-C (60x langzamer dan VibeSQL, 12x langzamer dan SQLite). Dit is verwacht: DuckDB is een <strong>analytische (OLAP) database</strong> geoptimaliseerd voor grote batch-operaties, niet voor transacties op enkele rijen. Het kolomgeoriënteerde opslagformaat blinkt uit in het scannen van miljoenen rijen, maar voegt overhead toe voor puntopzoekingen en kleine updates die OLTP-workloads zoals TPC-C domineren.
bench-tpcc-disc-duckdb-title = Waarom DuckDB Achterloopt op OLTP
bench-tpcc-transactions-label = transactions executed

# Conformance page (English placeholders)
conformance-additional-features = Additional Features
conformance-bottom-line = <strong>Bottom Line:</strong> We use two complementary test suites to ensure both standards conformance (sqltest) and practical correctness (SQLLogicTest). High pass rates in both demonstrate serious SQL:1999 implementation quality, though formal Core certification would require testing against official NIST suites.
conformance-commit = Commit:
conformance-core-explanation = SQL:1999 Core is the official mandatory feature set defined in the SQL:1999 (ISO/IEC 9075:1999) standard. It consists of approximately 169 required features that any database claiming Core compliance must implement. Official Core compliance is verified through the NIST SQL Test Suite, not community test suites.
conformance-core-features = Core Features (E-Series)
conformance-coverage-point = <span class="font-medium">Coverage:</span> sqltest covers 739 standard feature tests; SQLLogicTest covers practical scenarios
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
conformance-error-label = Error:
conformance-errors = Errors
conformance-explanation-title = Understanding Our Test Suites
conformance-f031 = Basic schema manipulation
conformance-failed = Failed
conformance-failing-tests-desc = The following tests are currently failing. Click to expand details.
conformance-failing-tests-title = Failing Tests
conformance-files-of-passing = { $passed } of { $total } test files passing
conformance-generated = Generated:
conformance-how-complement = How do they complement each other?
conformance-overall-pass-rate = Overall Pass Rate
conformance-pass-rates-mean = Our <strong>{ $sqltestRate }% sqltest pass rate</strong> ({ $sqltestPassed }/{ $sqltestTotal } tests) demonstrates strong SQL:1999 grammar conformance. { $sltInfo } Together, these results indicate comprehensive SQL:1999 compliance, though they do not constitute official Core certification.
conformance-passed = Passed
conformance-philosophy-point = <span class="font-medium">Philosophy:</span> sqltest says "can you parse this?"; SQLLogicTest says "does this work correctly?"
conformance-slt-ddl = DDL Tests
conformance-slt-desc = Results from the comprehensive <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">SQLLogicTest</a> suite containing ~{ $testCases } tests across { $testFiles } test files from the official SQLite corpus.
conformance-slt-evidence = Evidence Tests
conformance-slt-explanation = <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">SQLLogicTest</a> is a comprehensive test suite originally developed for SQLite, containing ~{ $testCases } SQL test cases across { $testFiles } test files. It tests practical correctness by running real-world queries and validating results. This suite focuses on semantic correctness and edge cases rather than pure grammar conformance.
conformance-slt-index = Index Tests
conformance-slt-note = <strong>Note:</strong> SQLLogicTest provides a different perspective from sqltest. While sqltest focuses on BNF grammar conformance from the SQL:1999 specification, SQLLogicTest contains millions of real-world SQL queries testing practical correctness across a wide range of scenarios.
conformance-slt-other = Other Tests
conformance-slt-pass-info = Our <strong>{ $sltRate }% SQLLogicTest pass rate</strong> ({ $sltPassed }/{ $sltTotal } test files) shows we handle real-world queries correctly.
conformance-slt-random = Random Tests
conformance-slt-select = SELECT Tests
conformance-slt-title = SQLLogicTest Results
conformance-slt-validates = <span class="font-medium">SQLLogicTest (Result-driven):</span> Validates semantic correctness with millions of real queries
conformance-sqltest-desc = Results from <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">sqltest</a> - a community-maintained BNF-driven conformance test suite derived from the SQL:1999 standard, containing 739 tests covering Core and Foundation features.
conformance-sqltest-explanation = <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">sqltest</a> is a community-maintained test suite by Elliot Chance that provides BNF-driven conformance tests derived from the SQL:1999 standard. It contains 739 tests covering Core and Foundation features across E-series and F-series test categories. This suite tests whether our implementation conforms to the SQL:1999 grammar specification.
conformance-sqltest-title = sqltest Results
conformance-sqltest-validates = <span class="font-medium">sqltest (BNF-driven):</span> Validates grammar conformance to SQL:1999 standard specifications
conformance-status = Status:
conformance-test-categories = Test Categories
conformance-test-coverage = Test Coverage
conformance-tests-of-passing = { $passed } of { $total } tests passing
conformance-view-failing = View failing test details ({ $count } tests)
conformance-what-is-core = What is SQL:1999 Core?
conformance-what-is-slt = What is SQLLogicTest?
conformance-what-is-sqltest = What is sqltest?
conformance-what-mean = What do our pass rates mean?

# PostgreSQL Regression Tests
conformance-pgsql-title = PostgreSQL Regressietests
conformance-pgsql-desc = Resultaten van het uitvoeren van de <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">PostgreSQL regressietest suite</a> - de canonieke test suite voor het valideren van PostgreSQL-compatibiliteit.
conformance-pgsql-tests-passing = tests geslaagd
conformance-pgsql-tests-excluded = tests uitgesloten
conformance-pgsql-pass-rate = Slaagpercentage
conformance-pgsql-excluded-reason = Uitgesloten tests gebruiken PostgreSQL-specifieke functies die niet van toepassing zijn op VibeSQL
conformance-pgsql-note = <strong>Opmerking:</strong> PostgreSQL regressietests valideren SQL-gedrag tegen de PostgreSQL referentie-implementatie. Uitgesloten tests betreffen PostgreSQL-specifieke functies zoals systeemcatalogi, procedurele talen of uitbreidingsmodules.

# SQLite TCL Test Suite Sectie
conformance-tcl-title = SQLite TCL Test Suite
conformance-tcl-desc = Resultaten van de canonieke <a href="https://www.sqlite.org/testing.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">TCL test suite</a> van SQLite met { $fileCount } testbestanden. Deze suite is de gouden standaard voor SQLite-compatibiliteitstests.
conformance-tcl-overall-rate = Algemeen Slaagpercentage
conformance-tcl-tests-passing = { $passed } van { $total } tests geslaagd
conformance-tcl-passed = Geslaagd
conformance-tcl-failed = Mislukt
conformance-tcl-skipped = Overgeslagen
conformance-tcl-total = Totaal
conformance-tcl-categories-title = Testcategorieën
conformance-tcl-category = Categorie
conformance-tcl-rate = Percentage
conformance-tcl-progress = Voortgang
conformance-tcl-tests = Tests
conformance-tcl-common-failures = Veelvoorkomende Fouten
conformance-tcl-failure-patterns = Top { $count } foutpatronen op basis van aantal voorkomens
conformance-tcl-about-title = Over TCL Tests:
conformance-tcl-about-text = De TCL test suite van SQLite is de canonieke conformiteitstest voor SQLite-compatibiliteit. Het test specifiek SQLite-gedrag, eigenaardigheden en randgevallen die mogelijk niet worden gedekt door standaard SQL test suites. Hoge slaagpercentages hier wijzen op sterke SQLite-compatibiliteit voor applicatiemigratiescenario's.

# =============================================================================
# Challenge Page
# =============================================================================

# Page title and header
challenge-page-title = SQL Vibe Coding Challenge - VibeSQL
challenge-header = SQL Vibe Coding Challenge

# Hero section
challenge-hero-title = De SQL Vibe Coding Challenge
challenge-hero-subtitle = Een objectieve benchmark voor multi-agent softwareontwikkeling. Bouw een SQL-database vanaf nul. Slaag voor 6 miljoen tests. Win de trofee.
challenge-btn-start = Begin met Bouwen
challenge-btn-trophy = Bekijk de Trofee
challenge-btn-leaderboard = Ranglijst

# Key Insight callout
challenge-insight-title = De Enige Maatstaf die Telt: Kalendertijd
challenge-insight-text = Commits en regels code zijn indicatoren. Wat telt is <strong>dagen tot voltooiing</strong>. Kan 1.000 agents parallel werken sneller dan 100 agents? Behoudt jouw orkestratie-framework productiviteit bij schalen? Deze benchmark geeft het antwoord.

# The Challenge section
challenge-section-title = De Uitdaging
challenge-objective-title = Doel
challenge-objective-text = Bouw een SQL-database-engine vanaf nul die slaagt voor de <a href="https://www.sqlite.org/sqllogictest/" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">SQLLogicTest suite</a>. Dit is dezelfde testsuite die wordt gebruikt om SQLite, DuckDB en andere productiedatabases te valideren.
challenge-success-title = Succescriteria
challenge-success-pass-rate = 100% slaagpercentage op SQLLogicTest suite
challenge-success-assertions = ~6 miljoen individuele testbeweringen
challenge-success-files = Alle 622 testbestanden geslaagd
challenge-constraints-title = Beperkingen
challenge-constraint-parser = <strong>Geen bestaande SQL-parserbibliotheken</strong> — bouw je eigen parser
challenge-constraint-engine = <strong>Geen bestaande query-engines</strong> — implementeer uitvoering vanaf nul
challenge-constraint-libs = <strong>Geen database-specifieke bibliotheken</strong> — gebruik alleen algemene bibliotheken
challenge-allowed-title = Toegestaan
challenge-allowed-lang = Elke programmeertaal
challenge-allowed-ai = Elk AI-orkestratieframework
challenge-allowed-human = Menselijke interventie (onbeperkt)
challenge-allowed-libs = Algemene bibliotheken (datastructuren, I/O, etc.)

# The Trophy section
challenge-trophy-title = De Trofee
challenge-trophy-name = De Vibe Coding Trofee
challenge-trophy-desc = Een fysieke trofee wordt uitgereikt aan elke recordhouder. Het ontwerp weerspiegelt de geest van "vibe coding" — een vergulde toverstok gemonteerd op walnoot met koperen naamplaatjes.
challenge-trophy-claim = <strong>Jouw naam komt op de trofee</strong> wanneer je het huidige record met minstens 5% verslaat.
challenge-rules-title = Toekenningsregels
challenge-rule-improve = <strong>5% verbetering vereist</strong> — versla het vorige record met minstens 5% (in kalenderdagen) om de trofee te claimen
challenge-rule-public = <strong>Publieke repository</strong> — je code moet publiek beschikbaar zijn voor verificatie
challenge-rule-pass = <strong>100% slaagpercentage</strong> — alle 622 SQLLogicTest-bestanden moeten slagen
challenge-rule-git = <strong>Verifieerbare git-geschiedenis</strong> — eerste commitdatum tot 100% slaagpercentage bepaalt je tijd
challenge-record-title = Huidige Recordhouder
challenge-record-days = { $days } dagen
challenge-record-name = VibeSQL (Baseline)
challenge-record-date = Oktober - November 2025
challenge-record-beat = Dit verslaan met 5%? Dat is <strong>24 dagen of minder</strong> om de trofee te claimen.

# Why This Challenge section
challenge-why-title = Waarom Deze Uitdaging?
challenge-why-objective-title = Objectieve Meting
challenge-why-objective-text = Geen subjectieve codebeoordelingen. De tests slagen of niet. 6 miljoen beweringen laten geen ruimte voor ambiguïteit.
challenge-why-complexity-title = Echte Complexiteit
challenge-why-complexity-text = SQL-databases vereisen parsers, optimizers en uitvoeringsengines. Dit is geen speelgoedprobleem—het is productie-niveau engineering.
challenge-why-time-title = Tijd is Waarheid
challenge-why-time-text = Kalenderdagen tot voltooiing is de ultieme maatstaf. Helpt parallelliseren naar 1.000 agents? Nu kun je het ontdekken.

# Get Started section
challenge-start-title = Aan de Slag
challenge-start-intro = Begin vanaf nul in elke taal, of gebruik een van onze seed-repo's voor gemak. Elke seed bevat de SQLLogicTest suite, een testrunner en CI-workflow.
challenge-seed-title = Seed Repo's
challenge-seed-optional = (optioneel)
challenge-seed-rust-desc = Cargo buildsysteem, zero-cost abstracties, geheugenbeveiliging zonder GC.
challenge-seed-cpp-desc = CMake buildsysteem, maximale prestaties, volledige controle over geheugen.
challenge-seed-go-desc = Eenvoudige toolchain, snelle compilatie, uitstekende concurrency-primitieven.
challenge-seed-fork = Fork op GitHub →
challenge-step1-title = Start Je Project
challenge-step1-text = Maak een nieuwe repo vanaf nul, of fork een seed hierboven voor een voorsprong. Haal de <a href="https://www.sqlite.org/sqllogictest/" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">SQLLogicTest suite</a>. Je eerste commit start de klok.
challenge-step2-title = Bouw Je Database
challenge-step2-text = Implementeer een SQL-parser, query-executor en storage-engine. Gebruik alle AI-tools—Claude, Copilot of je eigen agents. Voer <code class="bg-gray-200 dark:bg-gray-700 px-1 rounded">make test</code> uit om voortgang te volgen.
challenge-step3-title = Bereik 100% en Dien In
challenge-step3-text = Wanneer alle 622 testbestanden slagen, open een issue op <a href="https://github.com/vibesql-challenge/submissions" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">vibesql-challenge/submissions</a> met je repo-link en commit-hashes. Versla 25 dagen om op de ranglijst te komen.

# Explore VibeSQL section
challenge-explore-title = Ontdek VibeSQL
challenge-explore-demo-title = Probeer de Demo
challenge-explore-demo-text = Voer SQL-queries uit in je browser met de WebAssembly-build.
challenge-explore-conformance-title = Conformiteitsrapport
challenge-explore-conformance-text = Gedetailleerde uitsplitsing van SQL:1999 standaardconformiteit.
challenge-explore-benchmarks-title = Prestatiebenchmarks
challenge-explore-benchmarks-text = TPC-H, TPC-C en andere benchmarks vs SQLite en DuckDB.

# Footer
challenge-footer = VibeSQL - SQL:1999 Database in WebAssembly
