# VibeSQL Web UI - Svenska

# Page titles
page-title = VibeSQL - AI-driven SQL:1999 Databas
demo-title = VibeSQL Demo
benchmarks-title = Prestandabenchmarks - VibeSQL
benchmarks-heading = VibeSQL - Prestandabenchmarks
conformance-title = Överensstämmelserapport - VibeSQL
conformance-heading = Överensstämmelserapport
conformance-subtitle = SQL:1999 Standardöverensstämmelsetest

# Navigation
nav-showcase = SQL:1999 Showcase
nav-conformance = Visa sqltest-resultat
nav-sqllogictest = Visa SQLLogicTest-resultat

# Editor section
editor-title = SQL-editor
editor-storage = Lagring
editor-storage-init = Initierar...
editor-execute = Kör fråga

# Results section
results-title = Resultat
results-empty = Kör en fråga för att se resultat
results-loading = Laddar...
results-rows = { $count } { $count ->
    [one] rad
   *[other] rader
}
results-rows-with-time = { $count } { $count ->
    [one] rad
   *[other] rader
} ({ $time }ms)
results-copy = Kopiera till urklipp
results-export = Exportera CSV
results-limit-warning = Visar de första { $limit } av { $total } rader. Använd LIMIT för att förfina din fråga.

# Examples sidebar
examples-title = Exempel
examples-basic = Grundläggande frågor
examples-advanced = Avancerade frågor

# Database selector
db-select-label = Databas

# Footer
footer-tagline = VibeSQL - SQL:1999 Databas i WebAssembly
footer-deployed = Distribuerad: { $date }

# Theme
theme-toggle-dark = Byt till mörkt läge
theme-toggle-light = Byt till ljust läge

# Locale
locale-select = Välj språk

# Messages
msg-query-success = Frågan kördes framgångsrikt
msg-rows-affected = { $count } { $count ->
    [one] rad påverkad
   *[other] rader påverkade
}

# Errors
error-generic = Ett fel uppstod
error-query-failed = Frågan misslyckades
error-no-databases = Inga databaser tillgängliga

# Loading states
loading-initializing-theme = Initierar tema
loading-preparing-editor = Förbereder editor
loading-database-engine = Laddar databasmotor
loading-setting-up-ui = Konfigurerar användargränssnitt
loading-editor = Laddar editor...
loading-compliance-data = Laddar efterlevnadsdata...
loading-conformance-report = Laddar efterlevnadsrapport...

# Editor
editor-placeholder = Skriv SQL-fråga här... (Ctrl+Enter eller Cmd+Enter för att köra)

# Navigation links
nav-terminal = SQL-terminaldemo
nav-compliance = SQL-testöverensstämmelserapport
nav-benchmarks = Prestandabenchmarks
nav-github = GitHub-arkiv
nav-home = Hem

# Results
results-success-zero = Frågan kördes framgångsrikt (0 rader)
results-null = NULL

# Help Modal
help-title = Tangentbordsgenvägar & Hjälp
help-close = Stäng
help-editor-shortcuts = Editorgenvägar
help-navigation = Navigering
help-results-actions = Resultatåtgärder
help-tips = Tips
help-shortcut-execute = Kör aktuell fråga
help-shortcut-comment = Växla radkommentar
help-shortcut-indent = Indentera markering
help-shortcut-show-help = Visa denna hjälpdialog
help-shortcut-close-help = Stäng hjälpdialog
help-action-copy = Kopiera till urklipp
help-action-copy-desc = Kopiera resultat som tabbseparerade värden
help-action-export = Exportera CSV
help-action-export-desc = Ladda ner resultat som CSV-fil
help-tip-limit = Resultat begränsade till 1 000 rader för prestanda. Använd LIMIT för att förfina frågor.
help-tip-time = Exekveringstid visas med frågeresultat.
help-tip-syntax = Editorn stöder SQL-syntaxmarkering och autokomplettering.
help-tip-theme = Växla mellan ljust/mörkt läge med temaknappen.
help-got-it = Uppfattat!

# Showcase Navigation
showcase-title = SQL:1999 Core Showcase
showcase-description = Utforska implementerade SQL:1999 Core-funktioner interaktivt
showcase-complete = { $percent }% färdigt
showcase-categories = Funktionskategorier
showcase-legend = Statusförklaring
showcase-status-implemented = Fullt implementerat
showcase-status-partial = Delvis implementerat
showcase-status-planned = Planerat

# Showcase category labels
showcase-cat-compliance = Överensstämmelsepanel
showcase-cat-data-types = Datatyper
showcase-cat-dml = DML-operationer
showcase-cat-predicates = Predikat & Operatorer
showcase-cat-joins = JOIN
showcase-cat-subqueries = Underfrågor
showcase-cat-aggregates = Aggregat & GROUP BY
showcase-cat-ddl = DDL & Begränsningar

# Common showcase elements
showcase-interactive-examples = Interaktiva exempel
showcase-try-example = Prova detta exempel
showcase-progress = { $implemented } av { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Kategori
showcase-table-description = Beskrivning
showcase-table-syntax = Syntax
showcase-table-use-case = Användningsfall

# Status labels
status-implemented = Implementerat
status-partial = Delvis
status-planned = Planerat

# Aggregates Showcase
aggregates-title = SQL Aggregat och GROUP BY
aggregates-description = SQL:1999 Core aggregatfunktioner och grupperingsmöjligheter
aggregates-reference = Aggregatfunktionsreferens
aggregates-table-function = Funktion
aggregates-progress-type = funktioner
aggregates-ex-basic = Grundläggande aggregatfunktioner
aggregates-ex-group-single = GROUP BY (en kolumn)
aggregates-ex-group-multiple = GROUP BY (flera kolumner)
aggregates-ex-having = HAVING-klausul
aggregates-ex-orderby = ORDER BY med aggregat
aggregates-ex-null = NULL-hantering i aggregat

# DML Operations Showcase
dml-title = DML-operationer (datamanipuleringsspråk)
dml-description = SQL:1999 Core operationer för att fråga och modifiera data
dml-reference = DML-operationsreferens
dml-table-operation = Operation
dml-progress-type = operationer
dml-ex-select-basic = SELECT - grundläggande frågor
dml-ex-select-ordering = SELECT - sortering och begränsning
dml-ex-insert = INSERT-operationer
dml-ex-update = UPDATE-operationer
dml-ex-delete = DELETE-operationer
dml-ex-combined = Kombinerat CRUD-arbetsflöde

# Data Types Showcase
datatypes-title = SQL:1999 Core Datatyper
datatypes-description = Utforska de grundläggande datatyperna definierade i SQL:1999 Core-specifikationen
datatypes-reference = Datatypsreferens
datatypes-table-type = Typnamn
datatypes-table-example = Exempelvärden
datatypes-table-spec = Specifikation
datatypes-progress-type = typer
datatypes-ex-numeric = Arbeta med numeriska typer
datatypes-ex-null = NULL-hantering & Trevärd logik
datatypes-ex-comparisons = Typjämförelser & Operationer

# JOINs Showcase
joins-title = SQL JOIN
joins-description = SQL:1999 Core JOIN-operationer för att kombinera data från flera tabeller
joins-reference = JOIN-typreferens
joins-table-type = JOIN-typ
joins-progress-type = JOIN-typer
joins-category-suffix = JOIN
joins-ex-sample = Exempeldatainställning
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Flertabells-JOIN

# Predicates Showcase
predicates-title = Predikat och Operatorer
predicates-description = SQL:1999 predikat för filtrering och logiska operationer
predicates-reference = Predikatreferens
predicates-table-predicate = Predikat
predicates-progress-type = predikat
predicates-ex-comparison = Jämförelseoperatorer
predicates-ex-between = BETWEEN och intervallpredikat
predicates-ex-null = NULL-predikat och trevärd logik
predicates-ex-boolean = Boolesk logik (AND, OR, NOT)
predicates-ex-in = IN-predikat med underfrågor
predicates-ex-combined = Kombinerade predikatoperationer

# Subqueries Showcase
subqueries-title = SQL Underfrågor
subqueries-description = SQL:1999 Core underfrågefunktioner för nästlade frågeoperationer
subqueries-reference = Underfrågetypreferens
subqueries-table-type = Underfrågetyp
subqueries-progress-type = underfrågetyper
subqueries-ex-scalar-select = Skalär underfråga i SELECT
subqueries-ex-scalar-where = Skalär underfråga i WHERE
subqueries-ex-derived = Härledda tabeller (underfråga i FROM)
subqueries-ex-in = IN-predikat med underfråga
subqueries-ex-correlated = Korrelerade underfrågor
subqueries-ex-nested = Nästlade underfrågor

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
bench-no-wasm-data = Ingen WASM-data tillgänglig
bench-no-server-data = Ingen Sysbench server benchmark-data tillgänglig
bench-no-server-data-hint = Server-benchmarks kräver att sysbench_server körs med MySQL-jämförelse aktiverad.

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
bench-sysbench-srv-disc-mysql = Server benchmarks compare against MySQL to evaluate VibeSQL as a drop-in replacement for traditional client-server databases. Results vary by operation type, with VibeSQL showing advantages in read-heavy workloads.
bench-sysbench-srv-disc-roadmap-title = Server Roadmap
bench-sysbench-srv-disc-pooling = Reduce connection establishment overhead for high-throughput scenarios
bench-sysbench-srv-disc-caching = Server-side caching of query plans across connections
bench-sysbench-srv-disc-extended = Fullständigt PostgreSQL extended query protocol-stöd för batch-operationer

# TPC-H Server specifik
bench-tpch-server-name = TPC-H (Server)
bench-tpch-server-title = TPC-H Analytiskt Benchmark (Server)
bench-tpch-server-description = <strong>TPC-H server benchmarks</strong> jämför VibeSQL Server (PostgreSQL-protokoll) med MySQL för analytiska frågearbetsbelastningar, och mäter OLAP-prestanda i klient-server-distributioner.
bench-tpch-server-ops-label = TPC-H-frågor
bench-tpch-server-note-intro = Server-benchmarks testar <strong>PostgreSQL-protokollets</strong> implementering och mäter slut-till-slut frågelatens inklusive nätverksoverhead.
bench-tpch-server-note-queries = Frågor testar komplexa JOINs, underfrågor och aggregeringar som är typiska för business intelligence-arbetsbelastningar.

# TPC-H Server Diskussion
bench-tpch-srv-disc-protocol-title = PostgreSQL-protokoll
bench-tpch-srv-disc-protocol = VibeSQL Server talar PostgreSQL-protokollet, vilket möjliggör användning av standard PostgreSQL-drivrutiner och verktyg. Detta benchmark mäter fullständig slut-till-slut latens inklusive protokolloverhead.
bench-tpch-srv-disc-comparison-title = MySQL-jämförelse
bench-tpch-srv-disc-comparison = Jämförelse med MySQL ger en baslinje för traditionella klient-server-databaser vid analytiska arbetsbelastningar. VibeSQLs kolumnorienterade exekveringsmotor ger fördelar för komplexa aggregeringar och joins.
bench-tpch-srv-disc-roadmap-title = Server OLAP Roadmap
bench-tpch-srv-disc-prepared = Återanvända kompilerade frågeplaner över anslutningar
bench-tpch-srv-disc-pooling = Effektiv anslutningshantering för scenarier med hög genomströmning
bench-tpch-srv-disc-scale = Testning av större dataset (SF 0.1, SF 1.0) för produktionsskalevalidering

# TPC-C Server specifik
bench-tpcc-server-name = TPC-C (Server)
bench-tpcc-server-title = TPC-C OLTP Benchmark (Server)
bench-tpcc-server-description = <strong>TPC-C server benchmarks</strong> jämför VibeSQL Server (PostgreSQL-protokoll) med MySQL för OLTP-transaktionsarbetsbelastningar, och mäter genomströmning för multi-klient databasdistributioner.
bench-tpcc-server-ops-label = TPC-C-transaktioner
bench-tpcc-server-note-intro = Server-benchmarks testar <strong>PostgreSQL-protokollets</strong> implementering och mäter transaktionsgenomströmning inklusive nätverksoverhead.
bench-tpcc-server-note-results = Resultaten rapporterar transaktioner per sekund (TPS) för standard TPC-C transaktionsmixen.
bench-tpcc-mixed = Blandad arbetsbelastning - Standard TPC-C transaktionsmix (45% Ny-Order, 43% Betalning, 4% Order-Status, 4% Leverans, 4% Lager-Nivå)

# TPC-C Server Diskussion
bench-tpcc-srv-disc-protocol-title = PostgreSQL-protokoll
bench-tpcc-srv-disc-protocol = VibeSQL Server talar PostgreSQL-protokollet, vilket möjliggör användning av standard PostgreSQL-drivrutiner och verktyg. Detta benchmark mäter fullständig slut-till-slut transaktionslatens inklusive protokolloverhead.
bench-tpcc-srv-disc-comparison-title = MySQL-jämförelse
bench-tpcc-srv-disc-comparison = Jämförelse med MySQL ger en baslinje för traditionella klient-server-databaser vid OLTP-arbetsbelastningar. MySQL är branschstandarden för transaktionsarbetsbelastningar, och TPC-C är MySQLs styrka.
bench-tpcc-srv-disc-roadmap-title = Server OLTP Roadmap
bench-tpcc-srv-disc-prepared = Återanvända kompilerade frågeplaner över anslutningar
bench-tpcc-srv-disc-pooling = Effektiv anslutningshantering för scenarier med hög genomströmning
bench-tpcc-srv-disc-parallel = Samtidig multi-klient transaktionsbearbetning

# Footprint Embedded specifik
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
bench-bullet-worker = Worker thread-stöd
bench-bullet-prepared-stmts = Preparerade satser
bench-bullet-larger-scale = Större skalfaktorer
bench-bullet-parallel-clients = Parallella klienter

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
bench-tpcc-disc-duckdb = DuckDB achieves only ~385 TPS on TPC-C (60x slower than VibeSQL, 12x slower than SQLite). This is expected: DuckDB is an <strong>analytical (OLAP) database</strong> optimized for large batch operations, not single-row transactions. Its columnar storage format excels at scanning millions of rows but adds overhead for point lookups and small updates that dominate OLTP workloads like TPC-C.
bench-tpcc-disc-duckdb-title = Why DuckDB Lags on OLTP
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
conformance-slt-desc = Results from the comprehensive <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">SQLLogicTest</a> suite containing ~5.9 million tests across 623 test files from the official SQLite corpus.
conformance-slt-evidence = Evidence Tests
conformance-slt-explanation = <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">SQLLogicTest</a> is a comprehensive test suite originally developed for SQLite, containing ~5.9 million SQL test cases across 623 test files. It tests practical correctness by running real-world queries and validating results. This suite focuses on semantic correctness and edge cases rather than pure grammar conformance.
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
conformance-pgsql-title = PostgreSQL Regressionstester
conformance-pgsql-desc = Resultat från körning av <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">PostgreSQL:s regressionstestsvit</a> - den kanoniska testsviten för att validera PostgreSQL-kompatibilitet.
conformance-pgsql-tests-passing = tester godkända
conformance-pgsql-tests-excluded = tester exkluderade
conformance-pgsql-pass-rate = Godkännandegrad
conformance-pgsql-excluded-reason = Exkluderade tester använder PostgreSQL-specifika funktioner som inte är tillämpliga på VibeSQL
conformance-pgsql-note = <strong>Obs:</strong> PostgreSQL regressionstester validerar SQL-beteende mot PostgreSQL:s referensimplementation. Exkluderade tester involverar PostgreSQL-specifika funktioner som systemkataloger, procedurspråk eller tilläggsmoduler.
