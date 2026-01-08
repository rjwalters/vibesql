# VibeSQL Web UI - Polski

# Page titles
page-title = VibeSQL - Baza danych SQL:1999 z AI
demo-title = Demo VibeSQL
benchmarks-title = Testy Wydajności - VibeSQL
benchmarks-heading = VibeSQL - Testy Wydajności
conformance-title = Raport Zgodności - VibeSQL
conformance-heading = Raport Zgodności
conformance-subtitle = Testy Zgodności ze Standardem SQL:1999

# Navigation
nav-showcase = Prezentacja SQL:1999
nav-conformance = Zobacz wyniki sqltest
nav-sqllogictest = Zobacz wyniki SQLLogicTest

# Editor section
editor-title = Edytor SQL
editor-storage = Pamięć
editor-storage-init = Inicjalizacja...
editor-execute = Wykonaj zapytanie

# Results section
results-title = Wyniki
results-empty = Wykonaj zapytanie, aby zobaczyć wyniki
results-loading = Ładowanie...
results-rows = { $count } { $count ->
    [one] wiersz
    [few] wiersze
   *[other] wierszy
}
results-rows-with-time = { $count } { $count ->
    [one] wiersz
    [few] wiersze
   *[other] wierszy
} ({ $time }ms)
results-copy = Kopiuj do schowka
results-export = Eksportuj CSV
results-limit-warning = Wyświetlanie pierwszych { $limit } z { $total } wierszy. Użyj LIMIT, aby zawęzić zapytanie.

# Examples sidebar
examples-title = Przykłady
examples-basic = Podstawowe zapytania
examples-advanced = Zaawansowane zapytania

# Database selector
db-select-label = Baza danych

# Footer
footer-tagline = VibeSQL - Baza danych SQL:1999 w WebAssembly
footer-deployed = Wdrożono: { $date }

# Theme
theme-toggle-dark = Przełącz na tryb ciemny
theme-toggle-light = Przełącz na tryb jasny

# Locale
locale-select = Wybierz język

# Messages
msg-query-success = Zapytanie wykonane pomyślnie
msg-rows-affected = { $count } { $count ->
    [one] wiersz zmieniony
    [few] wiersze zmienione
   *[other] wierszy zmienionych
}

# Errors
error-generic = Wystąpił błąd
error-query-failed = Zapytanie nie powiodło się
error-no-databases = Brak dostępnych baz danych

# Loading states
loading-initializing-theme = Inicjalizacja motywu
loading-preparing-editor = Przygotowywanie edytora
loading-database-engine = Ładowanie silnika bazy danych
loading-setting-up-ui = Konfigurowanie interfejsu użytkownika
loading-editor = Ładowanie edytora...
loading-compliance-data = Ładowanie danych zgodności...
loading-conformance-report = Ładowanie raportu zgodności...

# Editor
editor-placeholder = Wprowadź zapytanie SQL tutaj... (Ctrl+Enter lub Cmd+Enter aby wykonać)

# Navigation links
nav-terminal = Demo terminala SQL
nav-compliance = Raport zgodności SQL
nav-benchmarks = Testy wydajności
nav-github = Repozytorium GitHub
nav-home = Strona główna

# Results
results-success-zero = Zapytanie wykonane pomyślnie (0 wierszy)
results-null = NULL

# Help Modal
help-title = Skróty klawiszowe i pomoc
help-close = Zamknij
help-editor-shortcuts = Skróty edytora
help-navigation = Nawigacja
help-results-actions = Akcje wyników
help-tips = Wskazówki
help-shortcut-execute = Wykonaj bieżące zapytanie
help-shortcut-comment = Przełącz komentarz linii
help-shortcut-indent = Wcięcie zaznaczenia
help-shortcut-show-help = Pokaż to okno pomocy
help-shortcut-close-help = Zamknij okno pomocy
help-action-copy = Kopiuj do schowka
help-action-copy-desc = Kopiuj wyniki jako wartości rozdzielone tabulatorem
help-action-export = Eksportuj CSV
help-action-export-desc = Pobierz wyniki jako plik CSV
help-tip-limit = Wyniki ograniczone do 1000 wierszy dla wydajności. Użyj LIMIT do zawężenia zapytań.
help-tip-time = Czas wykonania wyświetlany jest z wynikami zapytania.
help-tip-syntax = Edytor obsługuje podświetlanie składni SQL i autouzupełnianie.
help-tip-theme = Przełączaj między jasnym/ciemnym trybem za pomocą przycisku motywu.
help-got-it = Rozumiem!

# Showcase Navigation
showcase-title = Prezentacja SQL:1999 Core
showcase-description = Interaktywne odkrywanie zaimplementowanych funkcji SQL:1999 Core
showcase-complete = { $percent }% ukończone
showcase-categories = Kategorie funkcji
showcase-legend = Legenda statusów
showcase-status-implemented = W pełni zaimplementowane
showcase-status-partial = Częściowo zaimplementowane
showcase-status-planned = Planowane

# Showcase category labels
showcase-cat-compliance = Panel zgodności
showcase-cat-data-types = Typy danych
showcase-cat-dml = Operacje DML
showcase-cat-predicates = Predykaty i operatory
showcase-cat-joins = JOIN
showcase-cat-subqueries = Podzapytania
showcase-cat-aggregates = Agregaty i GROUP BY
showcase-cat-ddl = DDL i ograniczenia

# Common showcase elements
showcase-interactive-examples = Interaktywne przykłady
showcase-try-example = Wypróbuj ten przykład
showcase-progress = { $implemented } z { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Kategoria
showcase-table-description = Opis
showcase-table-syntax = Składnia
showcase-table-use-case = Przypadek użycia

# Status labels
status-implemented = Zaimplementowane
status-partial = Częściowe
status-planned = Planowane

# Aggregates Showcase
aggregates-title = Agregaty SQL i GROUP BY
aggregates-description = Funkcje agregujące SQL:1999 Core i możliwości grupowania
aggregates-reference = Dokumentacja funkcji agregujących
aggregates-table-function = Funkcja
aggregates-progress-type = funkcji
aggregates-ex-basic = Podstawowe funkcje agregujące
aggregates-ex-group-single = GROUP BY (jedna kolumna)
aggregates-ex-group-multiple = GROUP BY (wiele kolumn)
aggregates-ex-having = Klauzula HAVING
aggregates-ex-orderby = ORDER BY z agregatami
aggregates-ex-null = Obsługa NULL w agregatach

# DML Operations Showcase
dml-title = Operacje DML (język manipulacji danymi)
dml-description = Operacje SQL:1999 Core do zapytań i modyfikacji danych
dml-reference = Dokumentacja operacji DML
dml-table-operation = Operacja
dml-progress-type = operacji
dml-ex-select-basic = SELECT - podstawowe zapytania
dml-ex-select-ordering = SELECT - sortowanie i ograniczanie
dml-ex-insert = Operacje INSERT
dml-ex-update = Operacje UPDATE
dml-ex-delete = Operacje DELETE
dml-ex-combined = Połączony workflow CRUD

# Data Types Showcase
datatypes-title = Typy danych SQL:1999 Core
datatypes-description = Odkryj podstawowe typy danych zdefiniowane w specyfikacji SQL:1999 Core
datatypes-reference = Dokumentacja typów danych
datatypes-table-type = Nazwa typu
datatypes-table-example = Przykładowe wartości
datatypes-table-spec = Specyfikacja
datatypes-progress-type = typów
datatypes-ex-numeric = Praca z typami numerycznymi
datatypes-ex-null = Obsługa NULL i logika trójwartościowa
datatypes-ex-comparisons = Porównania typów i operacje

# JOINs Showcase
joins-title = SQL JOIN
joins-description = Operacje JOIN SQL:1999 Core do łączenia danych z wielu tabel
joins-reference = Dokumentacja typów JOIN
joins-table-type = Typ JOIN
joins-progress-type = typów JOIN
joins-category-suffix = JOIN
joins-ex-sample = Konfiguracja przykładowych danych
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Wielotabelowy JOIN

# Predicates Showcase
predicates-title = Predykaty i operatory
predicates-description = Predykaty SQL:1999 do filtrowania i operacji logicznych
predicates-reference = Dokumentacja predykatów
predicates-table-predicate = Predykat
predicates-progress-type = predykatów
predicates-ex-comparison = Operatory porównania
predicates-ex-between = BETWEEN i predykaty zakresu
predicates-ex-null = Predykaty NULL i logika trójwartościowa
predicates-ex-boolean = Logika boolowska (AND, OR, NOT)
predicates-ex-in = Predykat IN z podzapytaniami
predicates-ex-combined = Połączone operacje predykatów

# Subqueries Showcase
subqueries-title = Podzapytania SQL
subqueries-description = Możliwości podzapytań SQL:1999 Core dla zagnieżdżonych operacji
subqueries-reference = Dokumentacja typów podzapytań
subqueries-table-type = Typ podzapytania
subqueries-progress-type = typów podzapytań
subqueries-ex-scalar-select = Skalarne podzapytanie w SELECT
subqueries-ex-scalar-where = Skalarne podzapytanie w WHERE
subqueries-ex-derived = Tabele pochodne (podzapytanie w FROM)
subqueries-ex-in = Predykat IN z podzapytaniem
subqueries-ex-correlated = Skorelowane podzapytania
subqueries-ex-nested = Zagnieżdżone podzapytania

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
bench-no-wasm-data = Brak dostępnych danych WASM
bench-no-server-data = Brak dostępnych danych benchmarku serwera Sysbench
bench-no-server-data-hint = Benchmarki serwerowe wymagają uruchomienia sysbench_server z włączonym porównaniem MySQL.

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
bench-sysbench-emb-disc-architecture-title = Kompromisy architektoniczne
bench-sysbench-emb-disc-architecture = Hybrydowa architektura VibeSQL jest ukierunkowana zarówno na obciążenia OLTP, jak i OLAP. Nasze B-tree zapewnia wydajność wyszukiwania punktowego na poziomie SQLite, podczas gdy kolumnowe wykonywanie efektywnie obsługuje zapytania analityczne. To różni się od czystych baz danych OLAP, takich jak DuckDB, które optymalizują wyłącznie pod kątem operacji masowych kosztem opóźnień pojedynczych wierszy.

# Sysbench Server specific
bench-sysbench-server-name = Sysbench (Server)
bench-sysbench-server-title = Sysbench Micro-Benchmarks (Server)
bench-sysbench-server-description = <strong>Sysbench</strong> server benchmarks compare VibeSQL Server (PostgreSQL wire protocol) against MySQL, measuring performance for multi-client database deployments.
bench-sysbench-server-ops-label = Sysbench operations
bench-sysbench-server-note = Server mode uses the PostgreSQL wire protocol, enabling multi-client access and compatibility with existing PostgreSQL tooling and drivers.

# Sysbench Server Discussion
bench-sysbench-srv-disc-protocol-title = PostgreSQL Wire Protocol
bench-sysbench-srv-disc-protocol = VibeSQL Server implements the PostgreSQL wire protocol, enabling compatibility with existing PostgreSQL drivers and tools. This adds ~10-50µs of protocol overhead per query compared to embedded mode, but enables multi-client deployments.
bench-sysbench-srv-disc-mysql-title = Porównanie z MySQL
bench-sysbench-srv-disc-mysql = Benchmarki serwerowe porównują z MySQL, aby ocenić VibeSQL jako zamiennik tradycyjnych baz danych klient-serwer. VibeSQL Server przewyższa MySQL we wszystkich operacjach Sysbench, z przyspieszeniami od <strong>2,4x</strong> (zapytania zakresowe) do <strong>12,8x</strong> (indeksowane aktualizacje).
bench-sysbench-srv-disc-perf-title = Dlaczego VibeSQL Server jest szybszy
bench-sysbench-srv-disc-perf-arch = Architektura VibeSQL zasadniczo różni się od tradycyjnego projektu RDBMS MySQL
bench-sysbench-srv-disc-perf-storage = VibeSQL używa kolumnowego silnika przechowywania w pamięci zoptymalizowanego dla obciążeń analitycznych i OLTP, unikając narzutu zarządzania stronami InnoDB opartego na dysku
bench-sysbench-srv-disc-perf-locking = Bez ciężkiego blokowania na poziomie wierszy ani księgowości MVCC—VibeSQL używa lekkiej kontroli współbieżności zaprojektowanej dla nowoczesnych procesorów wielordzeniowych
bench-sysbench-srv-disc-perf-protocol = Efektywna implementacja protokołu PostgreSQL z minimalnym narzutem serializacji w porównaniu z protokołem MySQL
bench-sysbench-srv-disc-perf-writes = Operacje zapisu (wstawiania/aktualizacje) pokazują największe zyski (<strong>8-12x</strong>), ponieważ VibeSQL unika synchronizacji dziennika redo, dziennika undo i bufora podwójnego zapisu MySQL
bench-sysbench-srv-disc-perf-reads = Operacje odczytu pokazują mniejsze, ale spójne zyski (<strong>2-3x</strong>) dzięki efektywnym wzorcom dostępu kolumnowego i zerowemu I/O dyskowym
bench-sysbench-srv-disc-roadmap-title = Mapa drogowa serwera
bench-sysbench-srv-disc-pooling = Reduce connection establishment overhead for high-throughput scenarios
bench-sysbench-srv-disc-caching = Server-side caching of query plans across connections
bench-sysbench-srv-disc-extended = Pełna obsługa rozszerzonego protokołu zapytań PostgreSQL dla operacji wsadowych

# TPC-H Server specyficzne
bench-tpch-server-name = TPC-H (Serwer)
bench-tpch-server-title = Benchmark analityczny TPC-H (Serwer)
bench-tpch-server-description = <strong>Benchmarki serwerowe TPC-H</strong> porównują VibeSQL Server (protokół PostgreSQL) z MySQL dla obciążeń zapytań analitycznych, mierząc wydajność OLAP w wdrożeniach klient-serwer.
bench-tpch-server-ops-label = zapytań TPC-H
bench-tpch-server-note-intro = Benchmarki serwerowe testują implementację <strong>protokołu PostgreSQL</strong>, mierząc opóźnienie zapytań end-to-end włącznie z narzutem sieciowym.
bench-tpch-server-note-queries = Zapytania testują złożone JOINy, podzapytania i agregacje typowe dla obciążeń business intelligence.

# Dyskusja TPC-H Server
bench-tpch-srv-disc-protocol-title = Protokół PostgreSQL
bench-tpch-srv-disc-protocol = VibeSQL Server mówi protokołem PostgreSQL, umożliwiając użycie standardowych sterowników i narzędzi PostgreSQL. Ten benchmark mierzy pełne opóźnienie end-to-end włącznie z narzutem protokołu.
bench-tpch-srv-disc-comparison-title = Porównanie z MySQL
bench-tpch-srv-disc-comparison = Porównanie z MySQL zapewnia linię bazową dla tradycyjnych baz danych klient-serwer przy obciążeniach analitycznych. Kolumnowy silnik wykonawczy VibeSQL zapewnia przewagi dla złożonych agregacji i złączeń.
bench-tpch-srv-disc-roadmap-title = Mapa drogowa serwerowego OLAP
bench-tpch-srv-disc-prepared = Ponowne użycie skompilowanych planów zapytań między połączeniami
bench-tpch-srv-disc-pooling = Efektywna obsługa połączeń dla scenariuszy o wysokiej przepustowości
bench-tpch-srv-disc-scale = Testowanie większych zbiorów danych (SF 0.1, SF 1.0) dla walidacji na skalę produkcyjną

# TPC-C Server specyficzne
bench-tpcc-server-name = TPC-C (Serwer)
bench-tpcc-server-title = Benchmark OLTP TPC-C (Serwer)
bench-tpcc-server-description = <strong>Benchmarki serwerowe TPC-C</strong> porównują VibeSQL Server (protokół PostgreSQL) z MySQL dla obciążeń transakcyjnych OLTP, mierząc przepustowość dla wieloklienckich wdrożeń baz danych.
bench-tpcc-server-ops-label = transakcji TPC-C
bench-tpcc-server-note-intro = Benchmarki serwerowe testują implementację <strong>protokołu PostgreSQL</strong>, mierząc przepustowość transakcyjną włącznie z narzutem sieciowym.
bench-tpcc-server-note-results = Wyniki raportują transakcje na sekundę (TPS) dla standardowego miksu transakcji TPC-C.
bench-tpcc-mixed = Obciążenie mieszane - Standardowy miks transakcji TPC-C (45% Nowe-Zamówienie, 43% Płatność, 4% Status-Zamówienia, 4% Dostawa, 4% Poziom-Zapasów)

# Dyskusja TPC-C Server
bench-tpcc-srv-disc-protocol-title = Protokół PostgreSQL
bench-tpcc-srv-disc-protocol = VibeSQL Server mówi protokołem PostgreSQL, umożliwiając użycie standardowych sterowników i narzędzi PostgreSQL. Ten benchmark mierzy pełne opóźnienie transakcyjne end-to-end włącznie z narzutem protokołu.
bench-tpcc-srv-disc-comparison-title = Porównanie z MySQL
bench-tpcc-srv-disc-comparison = Porównanie z MySQL zapewnia linię bazową dla tradycyjnych baz danych klient-serwer przy obciążeniach OLTP. MySQL jest standardem branżowym dla obciążeń transakcyjnych, a TPC-C jest mocną stroną MySQL.
bench-tpcc-srv-disc-roadmap-title = Mapa drogowa serwerowego OLTP
bench-tpcc-srv-disc-prepared = Ponowne użycie skompilowanych planów zapytań między połączeniami
bench-tpcc-srv-disc-pooling = Efektywna obsługa połączeń dla scenariuszy o wysokiej przepustowości
bench-tpcc-srv-disc-parallel = Równoległe przetwarzanie wieloklienckich transakcji

# Footprint Embedded specyficzne
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

bench-table-query = Query
bench-tpcc-disc-duckdb-title = Dlaczego DuckDB pozostaje w tyle w OLTP
bench-tpcc-disc-duckdb = DuckDB osiąga jedynie ~385 TPS w TPC-C (60x wolniej niż VibeSQL, 12x wolniej niż SQLite). Jest to oczekiwane: DuckDB to <strong>analityczna (OLAP) baza danych</strong> zoptymalizowana pod kątem dużych operacji wsadowych, a nie transakcji na pojedynczych wierszach. Jej kolumnowy format przechowywania doskonale radzi sobie ze skanowaniem milionów wierszy, ale dodaje narzut dla wyszukiwań punktowych i małych aktualizacji, które dominują w obciążeniach OLTP takich jak TPC-C.
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
conformance-pgsql-title = Testy Regresyjne PostgreSQL
conformance-pgsql-desc = Wyniki uruchomienia <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">pakietu testów regresyjnych PostgreSQL</a> - kanonicznego zestawu testów używanego do walidacji zgodności z PostgreSQL.
conformance-pgsql-tests-passing = testów zaliczonych
conformance-pgsql-tests-excluded = testów wykluczonych
conformance-pgsql-pass-rate = Wskaźnik Zaliczenia
conformance-pgsql-excluded-reason = Wykluczone testy używają funkcji specyficznych dla PostgreSQL, które nie mają zastosowania w VibeSQL
conformance-pgsql-note = <strong>Uwaga:</strong> Testy regresyjne PostgreSQL walidują zachowanie SQL w porównaniu z referencyjną implementacją PostgreSQL. Wykluczone testy dotyczą funkcji specyficznych dla PostgreSQL, takich jak katalogi systemowe, języki proceduralne lub moduły rozszerzeń.

# Sekcja zestawu testów SQLite TCL
conformance-tcl-title = Zestaw testów SQLite TCL
conformance-tcl-desc = Wyniki kanonicznego <a href="https://www.sqlite.org/testing.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">zestawu testów TCL</a> SQLite zawierającego { $fileCount } plików testowych. Ten zestaw jest złotym standardem dla testów kompatybilności SQLite.
conformance-tcl-overall-rate = Ogólny Wskaźnik Zaliczenia
conformance-tcl-tests-passing = { $passed } z { $total } testów zaliczonych
conformance-tcl-passed = Zaliczone
conformance-tcl-failed = Niezaliczone
conformance-tcl-skipped = Pominięte
conformance-tcl-total = Razem
conformance-tcl-categories-title = Kategorie Testów
conformance-tcl-category = Kategoria
conformance-tcl-rate = Wskaźnik
conformance-tcl-progress = Postęp
conformance-tcl-tests = Testy
conformance-tcl-common-failures = Częste Błędy
conformance-tcl-failure-patterns = Top { $count } wzorców błędów według liczby wystąpień
conformance-tcl-about-title = O testach TCL:
conformance-tcl-about-text = Zestaw testów TCL SQLite jest kanonicznym testem zgodności dla kompatybilności SQLite. Testuje specyficzne zachowania SQLite, osobliwości i przypadki graniczne, które mogą nie być objęte standardowymi zestawami testów SQL. Wysokie wskaźniki zaliczenia tutaj wskazują na silną kompatybilność SQLite dla scenariuszy migracji aplikacji.

# =============================================================================
# Challenge Page
# =============================================================================

# Page title and header
challenge-page-title = SQL Vibe Coding Challenge - VibeSQL
challenge-header = SQL Vibe Coding Challenge

# Hero section
challenge-hero-title = SQL Vibe Coding Challenge
challenge-hero-subtitle = Obiektywny benchmark dla wieloagentowego rozwoju oprogramowania. Zbuduj bazę danych SQL od podstaw. Zdaj 6 milionów testów. Wygraj trofeum.
challenge-btn-start = Zacznij Budować
challenge-btn-trophy = Zobacz Trofeum
challenge-btn-leaderboard = Ranking

# Key Insight callout
challenge-insight-title = Jedyna Metryka, Która Się Liczy: Czas Kalendarzowy
challenge-insight-text = Commity i linie kodu to wskaźniki zastępcze. Liczy się <strong>liczba dni do ukończenia</strong>. Czy 1000 agentów pracujących równolegle pokona 100 agentów? Czy twój framework orkiestracji utrzymuje produktywność przy skalowaniu? Ten benchmark da ci odpowiedź.

# The Challenge section
challenge-section-title = Wyzwanie
challenge-objective-title = Cel
challenge-objective-text = Zbuduj silnik bazy danych SQL od podstaw, który zda <a href="https://www.sqlite.org/sqllogictest/" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">zestaw testów SQLLogicTest</a>. Jest to ten sam zestaw testów używany do walidacji SQLite, DuckDB i innych produkcyjnych baz danych.
challenge-success-title = Kryteria Sukcesu
challenge-success-pass-rate = 100% wskaźnik zdawalności w zestawie SQLLogicTest
challenge-success-assertions = ~6 milionów indywidualnych asercji testowych
challenge-success-files = Wszystkie 622 pliki testowe zaliczone
challenge-constraints-title = Ograniczenia
challenge-constraint-parser = <strong>Brak istniejących bibliotek parserów SQL</strong> — zbuduj własny parser
challenge-constraint-engine = <strong>Brak istniejących silników zapytań</strong> — zaimplementuj wykonywanie od podstaw
challenge-constraint-libs = <strong>Brak bibliotek specyficznych dla baz danych</strong> — używaj tylko bibliotek ogólnego przeznaczenia
challenge-allowed-title = Dozwolone
challenge-allowed-lang = Dowolny język programowania
challenge-allowed-ai = Dowolny framework orkiestracji AI
challenge-allowed-human = Interwencja ludzka (bez ograniczeń)
challenge-allowed-libs = Biblioteki ogólnego przeznaczenia (struktury danych, I/O, itp.)

# The Trophy section
challenge-trophy-title = Trofeum
challenge-trophy-name = Trofeum Vibe Coding
challenge-trophy-desc = Fizyczne trofeum zostanie wręczone każdemu rekordziście. Design odzwierciedla ducha "vibe coding" — pozłacana różdżka zamontowana na orzechu włoskim z mosiężnymi tabliczkami.
challenge-trophy-claim = <strong>Twoje imię znajdzie się na trofeum</strong>, gdy pobijesz obecny rekord o co najmniej 5%.
challenge-rules-title = Zasady Przyznawania
challenge-rule-improve = <strong>Wymagana poprawa o 5%</strong> — pobij poprzedni rekord o co najmniej 5% (w dniach kalendarzowych), aby zdobyć trofeum
challenge-rule-public = <strong>Publiczne repozytorium</strong> — twój kod musi być publicznie dostępny do weryfikacji
challenge-rule-pass = <strong>100% wskaźnik zdawalności</strong> — wszystkie 622 pliki SQLLogicTest muszą przejść
challenge-rule-git = <strong>Weryfikowalna historia git</strong> — data pierwszego commita do 100% zdawalności określa twój czas
challenge-record-title = Obecny Rekordzista
challenge-record-days = { $days } dni
challenge-record-name = VibeSQL (Bazowy)
challenge-record-date = Październik - Listopad 2025
challenge-record-beat = Pobić to o 5%? To <strong>24 dni lub mniej</strong>, aby zdobyć trofeum.

# Why This Challenge section
challenge-why-title = Dlaczego To Wyzwanie?
challenge-why-objective-title = Obiektywny Pomiar
challenge-why-objective-text = Żadnych subiektywnych przeglądów kodu. Testy przechodzą albo nie. 6 milionów asercji nie pozostawia miejsca na dwuznaczność.
challenge-why-complexity-title = Prawdziwa Złożoność
challenge-why-complexity-text = Bazy danych SQL wymagają parserów, optymalizatorów i silników wykonawczych. To nie jest zabawkowy problem—to inżynieria na poziomie produkcyjnym.
challenge-why-time-title = Czas Jest Prawdą
challenge-why-time-text = Dni kalendarzowe do ukończenia to ostateczna metryka. Czy równoległe uruchomienie 1000 agentów pomaga? Teraz możesz się przekonać.

# Get Started section
challenge-start-title = Rozpocznij
challenge-start-intro = Zacznij od zera w dowolnym języku lub użyj jednego z naszych repozytoriów startowych dla wygody. Każde z nich zawiera zestaw SQLLogicTest, runner testów i przepływ pracy CI.
challenge-seed-title = Repozytoria Startowe
challenge-seed-optional = (opcjonalne)
challenge-seed-rust-desc = System budowania Cargo, abstrakcje bez kosztów, bezpieczeństwo pamięci bez GC.
challenge-seed-cpp-desc = System budowania CMake, maksymalna wydajność, pełna kontrola nad pamięcią.
challenge-seed-go-desc = Prosty toolchain, szybka kompilacja, doskonałe prymitywy współbieżności.
challenge-seed-fork = Forkuj na GitHub →
challenge-step1-title = Rozpocznij Swój Projekt
challenge-step1-text = Utwórz nowe repozytorium od zera lub forkuj repozytorium startowe powyżej. Pobierz <a href="https://www.sqlite.org/sqllogictest/" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">zestaw SQLLogicTest</a>. Twój pierwszy commit uruchamia zegar.
challenge-step2-title = Zbuduj Swoją Bazę Danych
challenge-step2-text = Zaimplementuj parser SQL, executor zapytań i silnik przechowywania. Używaj dowolnych narzędzi AI—Claude, Copilot lub własnych agentów. Uruchom <code class="bg-gray-200 dark:bg-gray-700 px-1 rounded">make test</code>, aby śledzić postęp.
challenge-step3-title = Osiągnij 100% i Zgłoś
challenge-step3-text = Gdy wszystkie 622 pliki testowe przejdą, otwórz issue na <a href="https://github.com/vibesql-challenge/submissions" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">vibesql-challenge/submissions</a> z linkiem do repozytorium i hashami commitów. Pobij 25 dni, aby dołączyć do rankingu.

# Explore VibeSQL section
challenge-explore-title = Odkryj VibeSQL
challenge-explore-demo-title = Wypróbuj Demo
challenge-explore-demo-text = Uruchamiaj zapytania SQL w przeglądarce za pomocą kompilacji WebAssembly.
challenge-explore-conformance-title = Raport Zgodności
challenge-explore-conformance-text = Szczegółowy podział zgodności ze standardami SQL:1999.
challenge-explore-benchmarks-title = Benchmarki Wydajności
challenge-explore-benchmarks-text = TPC-H, TPC-C i inne benchmarki vs SQLite i DuckDB.

# Footer
challenge-footer = VibeSQL - Baza Danych SQL:1999 w WebAssembly
