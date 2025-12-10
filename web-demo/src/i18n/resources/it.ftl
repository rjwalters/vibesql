# VibeSQL Web UI - Italiano

# Page titles
page-title = VibeSQL - Database SQL:1999 con IA
demo-title = Demo VibeSQL
benchmarks-title = Benchmark Prestazionali - VibeSQL
benchmarks-heading = VibeSQL - Benchmark Prestazionali
conformance-title = Report di Conformità - VibeSQL
conformance-heading = Report di Conformità
conformance-subtitle = Test di Conformità allo Standard SQL:1999

# Navigation
nav-showcase = Showcase SQL:1999
nav-conformance = Vedi risultati sqltest
nav-sqllogictest = Vedi risultati SQLLogicTest

# Editor section
editor-title = Editor SQL
editor-storage = Archiviazione
editor-storage-init = Inizializzazione...
editor-execute = Esegui Query

# Results section
results-title = Risultati
results-empty = Esegui una query per vedere i risultati
results-loading = Caricamento...
results-rows = { $count } { $count ->
    [one] riga
   *[other] righe
}
results-rows-with-time = { $count } { $count ->
    [one] riga
   *[other] righe
} ({ $time }ms)
results-copy = Copia negli appunti
results-export = Esporta CSV
results-limit-warning = Visualizzazione delle prime { $limit } di { $total } righe. Usa LIMIT per affinare la tua query.

# Examples sidebar
examples-title = Esempi
examples-basic = Query di Base
examples-advanced = Query Avanzate

# Database selector
db-select-label = Database

# Footer
footer-tagline = VibeSQL - Database SQL:1999 in WebAssembly
footer-deployed = Distribuito: { $date }

# Theme
theme-toggle-dark = Passa alla modalità scura
theme-toggle-light = Passa alla modalità chiara

# Locale
locale-select = Seleziona lingua

# Messages
msg-query-success = Query eseguita con successo
msg-rows-affected = { $count } { $count ->
    [one] riga interessata
   *[other] righe interessate
}

# Errors
error-generic = Si è verificato un errore
error-query-failed = Query fallita
error-no-databases = Nessun database disponibile

# Loading states
loading-initializing-theme = Inizializzazione del tema
loading-preparing-editor = Preparazione dell'editor
loading-database-engine = Caricamento del motore database
loading-setting-up-ui = Configurazione dell'interfaccia utente
loading-editor = Caricamento dell'editor...
loading-compliance-data = Caricamento dei dati di conformità...
loading-conformance-report = Caricamento del rapporto di conformità...

# Editor
editor-placeholder = Inserisci la tua query SQL qui... (Ctrl+Invio o Cmd+Invio per eseguire)

# Navigation links
nav-terminal = Demo Terminale SQL
nav-compliance = Report di Conformità SQL
nav-benchmarks = Benchmark Prestazionali
nav-github = Repository GitHub
nav-home = Home
nav-trends = Tendenze Prestazionali

# Trends page
trends-title = Tendenze Prestazionali - VibeSQL
trends-heading = VibeSQL - Tendenze Prestazionali
trends-total-runs = Esecuzioni Totali
trends-across-suites = su tutte le suite
trends-date-range = Intervallo di Date
trends-first-to-last = dalla prima all'ultima esecuzione
trends-latest-commit = Ultimo Commit
trends-most-recent = benchmark più recente
trends-generated = Generato
trends-last-export = ultima esportazione dati

# Results
results-success-zero = Query eseguita con successo (0 righe)
results-null = NULL

# Help Modal
help-title = Scorciatoie da Tastiera e Aiuto
help-close = Chiudi
help-editor-shortcuts = Scorciatoie Editor
help-navigation = Navigazione
help-results-actions = Azioni Risultati
help-tips = Suggerimenti
help-shortcut-execute = Esegui query corrente
help-shortcut-comment = Attiva/disattiva commento riga
help-shortcut-indent = Indenta selezione
help-shortcut-show-help = Mostra questa finestra di aiuto
help-shortcut-close-help = Chiudi finestra di aiuto
help-action-copy = Copia negli appunti
help-action-copy-desc = Copia i risultati come valori separati da tabulazione
help-action-export = Esporta CSV
help-action-export-desc = Scarica i risultati come file CSV
help-tip-limit = I risultati sono limitati a 1.000 righe per prestazioni. Usa LIMIT per affinare le query.
help-tip-time = Il tempo di esecuzione è mostrato con i risultati della query.
help-tip-syntax = L'editor supporta l'evidenziazione della sintassi SQL e il completamento automatico.
help-tip-theme = Passa tra tema chiaro/scuro usando il pulsante tema.
help-got-it = Capito!

# Showcase Navigation
showcase-title = Showcase SQL:1999 Core
showcase-description = Esplora interattivamente le funzionalità SQL:1999 Core implementate
showcase-complete = { $percent }% Completato
showcase-categories = Categorie Funzionalità
showcase-legend = Legenda Stati
showcase-status-implemented = Completamente Implementato
showcase-status-partial = Parzialmente Implementato
showcase-status-planned = Pianificato

# Showcase category labels
showcase-cat-compliance = Dashboard Conformità
showcase-cat-data-types = Tipi di Dati
showcase-cat-dml = Operazioni DML
showcase-cat-predicates = Predicati e Operatori
showcase-cat-joins = JOIN
showcase-cat-subqueries = Subquery
showcase-cat-aggregates = Aggregati e GROUP BY
showcase-cat-ddl = DDL e Vincoli

# Common showcase elements
showcase-interactive-examples = Esempi Interattivi
showcase-try-example = Prova Questo Esempio
showcase-progress = { $implemented } di { $total } { $type } ({ $percent }%)
showcase-table-status = Stato
showcase-table-category = Categoria
showcase-table-description = Descrizione
showcase-table-syntax = Sintassi
showcase-table-use-case = Caso d'Uso

# Status labels
status-implemented = Implementato
status-partial = Parziale
status-planned = Pianificato

# Aggregates Showcase
aggregates-title = Aggregati SQL e GROUP BY
aggregates-description = Funzioni di aggregazione SQL:1999 Core e capacità di raggruppamento
aggregates-reference = Riferimento Funzioni di Aggregazione
aggregates-table-function = Funzione
aggregates-progress-type = funzioni
aggregates-ex-basic = Funzioni di Aggregazione Base
aggregates-ex-group-single = GROUP BY (Colonna Singola)
aggregates-ex-group-multiple = GROUP BY (Colonne Multiple)
aggregates-ex-having = Clausola HAVING
aggregates-ex-orderby = ORDER BY con Aggregati
aggregates-ex-null = Gestione NULL negli Aggregati

# DML Operations Showcase
dml-title = Operazioni DML (Linguaggio di Manipolazione Dati)
dml-description = Operazioni SQL:1999 Core per interrogare e modificare dati
dml-reference = Riferimento Operazioni DML
dml-table-operation = Operazione
dml-progress-type = operazioni
dml-ex-select-basic = SELECT - Query Base
dml-ex-select-ordering = SELECT - Ordinamento e Limitazione
dml-ex-insert = Operazioni INSERT
dml-ex-update = Operazioni UPDATE
dml-ex-delete = Operazioni DELETE
dml-ex-combined = Workflow CRUD Combinato

# Data Types Showcase
datatypes-title = Tipi di Dati SQL:1999 Core
datatypes-description = Esplora i tipi di dati fondamentali definiti nella specifica SQL:1999 Core
datatypes-reference = Riferimento Tipi di Dati
datatypes-table-type = Nome Tipo
datatypes-table-example = Valori di Esempio
datatypes-table-spec = Specifica
datatypes-progress-type = tipi
datatypes-ex-numeric = Lavorare con Tipi Numerici
datatypes-ex-null = Gestione NULL e Logica a Tre Valori
datatypes-ex-comparisons = Confronti e Operazioni sui Tipi

# JOINs Showcase
joins-title = JOIN SQL
joins-description = Operazioni JOIN SQL:1999 Core per combinare dati da più tabelle
joins-reference = Riferimento Tipi di JOIN
joins-table-type = Tipo JOIN
joins-progress-type = tipi di JOIN
joins-category-suffix = JOIN
joins-ex-sample = Setup Dati di Esempio
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = JOIN Multi-tabella

# Predicates Showcase
predicates-title = Predicati e Operatori
predicates-description = Predicati SQL:1999 per filtraggio e operazioni logiche
predicates-reference = Riferimento Predicati
predicates-table-predicate = Predicato
predicates-progress-type = predicati
predicates-ex-comparison = Operatori di Confronto
predicates-ex-between = BETWEEN e Predicati di Intervallo
predicates-ex-null = Predicati NULL e Logica a Tre Valori
predicates-ex-boolean = Logica Booleana (AND, OR, NOT)
predicates-ex-in = Predicato IN con Subquery
predicates-ex-combined = Operazioni Predicati Combinati

# Subqueries Showcase
subqueries-title = Subquery SQL
subqueries-description = Capacità di subquery SQL:1999 Core per operazioni di query annidate
subqueries-reference = Riferimento Tipi di Subquery
subqueries-table-type = Tipo Subquery
subqueries-progress-type = tipi di subquery
subqueries-ex-scalar-select = Subquery Scalare in SELECT
subqueries-ex-scalar-where = Subquery Scalare in WHERE
subqueries-ex-derived = Tabelle Derivate (Subquery in FROM)
subqueries-ex-in = Predicato IN con Subquery
subqueries-ex-correlated = Subquery Correlate
subqueries-ex-nested = Subquery Annidate

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
bench-no-wasm-data = Nessun dato WASM disponibile
bench-no-server-data = Nessun dato benchmark server Sysbench disponibile
bench-no-server-data-hint = I benchmark server richiedono l'esecuzione di sysbench_server con il confronto MySQL abilitato.

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
bench-sysbench-srv-disc-extended = Supporto completo del protocollo extended query PostgreSQL per operazioni batch

# TPC-H Server specifico
bench-tpch-server-name = TPC-H (Server)
bench-tpch-server-title = Benchmark Analitico TPC-H (Server)
bench-tpch-server-description = I <strong>benchmark server TPC-H</strong> confrontano VibeSQL Server (protocollo PostgreSQL) con MySQL per carichi di lavoro di query analitiche, misurando le prestazioni OLAP nelle implementazioni client-server.
bench-tpch-server-ops-label = query TPC-H
bench-tpch-server-note-intro = I benchmark server testano l'implementazione del <strong>protocollo PostgreSQL</strong>, misurando la latenza delle query end-to-end incluso l'overhead di rete.
bench-tpch-server-note-queries = Le query testano JOIN complessi, sottoquery e aggregazioni tipiche dei carichi di lavoro di business intelligence.

# Discussione TPC-H Server
bench-tpch-srv-disc-protocol-title = Protocollo PostgreSQL
bench-tpch-srv-disc-protocol = VibeSQL Server parla il protocollo PostgreSQL, consentendo l'uso di driver e strumenti PostgreSQL standard. Questo benchmark misura la latenza end-to-end completa incluso l'overhead del protocollo.
bench-tpch-srv-disc-comparison-title = Confronto con MySQL
bench-tpch-srv-disc-comparison = Il confronto con MySQL fornisce una baseline per i database client-server tradizionali sui carichi di lavoro analitici. Il motore di esecuzione colonnare di VibeSQL offre vantaggi per aggregazioni e join complessi.
bench-tpch-srv-disc-roadmap-title = Roadmap OLAP Server
bench-tpch-srv-disc-prepared = Riutilizzo dei piani di query compilati tra le connessioni
bench-tpch-srv-disc-pooling = Gestione efficiente delle connessioni per scenari ad alto throughput
bench-tpch-srv-disc-scale = Test di dataset più grandi (SF 0.1, SF 1.0) per validazione su scala di produzione

# TPC-C Server specifico
bench-tpcc-server-name = TPC-C (Server)
bench-tpcc-server-title = Benchmark OLTP TPC-C (Server)
bench-tpcc-server-description = I <strong>benchmark server TPC-C</strong> confrontano VibeSQL Server (protocollo PostgreSQL) con MySQL per carichi di lavoro transazionali OLTP, misurando il throughput per implementazioni database multi-client.
bench-tpcc-server-ops-label = transazioni TPC-C
bench-tpcc-server-note-intro = I benchmark server testano l'implementazione del <strong>protocollo PostgreSQL</strong>, misurando il throughput transazionale incluso l'overhead di rete.
bench-tpcc-server-note-results = I risultati riportano transazioni al secondo (TPS) per il mix di transazioni TPC-C standard.
bench-tpcc-mixed = Carico Misto - Mix di transazioni TPC-C standard (45% Nuovo-Ordine, 43% Pagamento, 4% Stato-Ordine, 4% Consegna, 4% Livello-Stock)

# Discussione TPC-C Server
bench-tpcc-srv-disc-protocol-title = Protocollo PostgreSQL
bench-tpcc-srv-disc-protocol = VibeSQL Server parla il protocollo PostgreSQL, consentendo l'uso di driver e strumenti PostgreSQL standard. Questo benchmark misura la latenza transazionale end-to-end completa incluso l'overhead del protocollo.
bench-tpcc-srv-disc-comparison-title = Confronto con MySQL
bench-tpcc-srv-disc-comparison = Il confronto con MySQL fornisce una baseline per i database client-server tradizionali sui carichi di lavoro OLTP. MySQL è lo standard del settore per i carichi di lavoro transazionali, e TPC-C è il punto di forza di MySQL.
bench-tpcc-srv-disc-roadmap-title = Roadmap OLTP Server
bench-tpcc-srv-disc-prepared = Riutilizzo dei piani di query compilati tra le connessioni
bench-tpcc-srv-disc-pooling = Gestione efficiente delle connessioni per scenari ad alto throughput
bench-tpcc-srv-disc-parallel = Elaborazione concorrente di transazioni multi-client

# Footprint Embedded specifico
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
bench-bullet-indexeddb = Persistenza IndexedDB
bench-bullet-worker = Supporto worker thread
bench-bullet-prepared-stmts = Prepared statement
bench-bullet-larger-scale = Fattori di scala maggiori
bench-bullet-parallel-clients = Client paralleli

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
conformance-pgsql-title = Test di Regressione PostgreSQL
conformance-pgsql-desc = Risultati dell'esecuzione della <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">suite di test di regressione PostgreSQL</a> - la suite di test canonica utilizzata per validare la compatibilità con PostgreSQL.
conformance-pgsql-tests-passing = test superati
conformance-pgsql-tests-excluded = test esclusi
conformance-pgsql-pass-rate = Tasso di Superamento
conformance-pgsql-excluded-reason = I test esclusi utilizzano funzionalità specifiche di PostgreSQL non applicabili a VibeSQL
conformance-pgsql-note = <strong>Nota:</strong> I test di regressione PostgreSQL validano il comportamento SQL rispetto all'implementazione di riferimento PostgreSQL. I test esclusi riguardano funzionalità specifiche di PostgreSQL come cataloghi di sistema, linguaggi procedurali o moduli di estensione.
