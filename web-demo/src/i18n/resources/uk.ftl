# VibeSQL Web UI - Українська

# Page titles
page-title = VibeSQL - База даних SQL:1999 з ШІ
demo-title = Демо VibeSQL
benchmarks-title = Тести продуктивності - VibeSQL
benchmarks-heading = VibeSQL - Тести продуктивності
conformance-title = Звіт про відповідність - VibeSQL
conformance-heading = Звіт про відповідність
conformance-subtitle = Тестування відповідності стандарту SQL:1999

# Navigation
nav-showcase = Демонстрація SQL:1999
nav-conformance = Результати sqltest
nav-sqllogictest = Результати SQLLogicTest

# Editor section
editor-title = Редактор SQL
editor-storage = Сховище
editor-storage-init = Ініціалізація...
editor-execute = Виконати запит

# Results section
results-title = Результати
results-empty = Виконайте запит для перегляду результатів
results-loading = Завантаження...
results-rows = { $count } { $count ->
    [one] рядок
    [few] рядки
   *[other] рядків
}
results-rows-with-time = { $count } { $count ->
    [one] рядок
    [few] рядки
   *[other] рядків
} ({ $time }мс)
results-copy = Копіювати в буфер обміну
results-export = Експорт у CSV
results-limit-warning = Показано перші { $limit } з { $total } рядків. Використовуйте LIMIT для уточнення запиту.

# Examples sidebar
examples-title = Приклади
examples-basic = Базові запити
examples-advanced = Розширені запити

# Database selector
db-select-label = База даних

# Footer
footer-tagline = VibeSQL - База даних SQL:1999 у WebAssembly
footer-deployed = Розгорнуто: { $date }

# Theme
theme-toggle-dark = Перейти на темну тему
theme-toggle-light = Перейти на світлу тему

# Locale
locale-select = Вибрати мову

# Messages
msg-query-success = Запит виконано успішно
msg-rows-affected = { $count } { $count ->
    [one] рядок змінено
    [few] рядки змінено
   *[other] рядків змінено
}

# Errors
error-generic = Сталася помилка
error-query-failed = Помилка виконання запиту
error-no-databases = Немає доступних баз даних

# Loading states
loading-initializing-theme = Ініціалізація теми
loading-preparing-editor = Підготовка редактора
loading-database-engine = Завантаження рушія бази даних
loading-setting-up-ui = Налаштування інтерфейсу користувача
loading-editor = Завантаження редактора...
loading-compliance-data = Завантаження даних відповідності...
loading-conformance-report = Завантаження звіту про відповідність...

# Editor
editor-placeholder = Введіть SQL-запит тут... (Ctrl+Enter або Cmd+Enter для виконання)

# Navigation links
nav-terminal = Демо SQL-терміналу
nav-compliance = Звіт про відповідність SQL
nav-benchmarks = Тести продуктивності
nav-github = Репозиторій GitHub
nav-home = Головна

# Results
results-success-zero = Запит виконано успішно (0 рядків)
results-null = NULL

# Help Modal
help-title = Гарячі клавіші та довідка
help-close = Закрити
help-editor-shortcuts = Гарячі клавіші редактора
help-navigation = Навігація
help-results-actions = Дії з результатами
help-tips = Поради
help-shortcut-execute = Виконати поточний запит
help-shortcut-comment = Перемкнути коментар рядка
help-shortcut-indent = Відступ виділення
help-shortcut-show-help = Показати цю довідку
help-shortcut-close-help = Закрити довідку
help-action-copy = Копіювати в буфер обміну
help-action-copy-desc = Копіювати результати як значення, розділені табуляцією
help-action-export = Експорт у CSV
help-action-export-desc = Завантажити результати як CSV-файл
help-tip-limit = Результати обмежені 1000 рядками для продуктивності. Використовуйте LIMIT для уточнення запитів.
help-tip-time = Час виконання відображається з результатами запиту.
help-tip-syntax = Редактор підтримує підсвічування синтаксису SQL та автозавершення.
help-tip-theme = Перемикайтесь між світлою/темною темою за допомогою кнопки теми.
help-got-it = Зрозуміло!

# Showcase Navigation
showcase-title = Демонстрація SQL:1999 Core
showcase-description = Інтерактивне вивчення реалізованих функцій SQL:1999 Core
showcase-complete = { $percent }% завершено
showcase-categories = Категорії функцій
showcase-legend = Легенда статусів
showcase-status-implemented = Повністю реалізовано
showcase-status-partial = Частково реалізовано
showcase-status-planned = Заплановано

# Showcase category labels
showcase-cat-compliance = Панель відповідності
showcase-cat-data-types = Типи даних
showcase-cat-dml = DML-операції
showcase-cat-predicates = Предикати та оператори
showcase-cat-joins = JOIN
showcase-cat-subqueries = Підзапити
showcase-cat-aggregates = Агрегати та GROUP BY
showcase-cat-ddl = DDL та обмеження

# Common showcase elements
showcase-interactive-examples = Інтерактивні приклади
showcase-try-example = Спробувати приклад
showcase-progress = { $implemented } з { $total } { $type } ({ $percent }%)
showcase-table-status = Статус
showcase-table-category = Категорія
showcase-table-description = Опис
showcase-table-syntax = Синтаксис
showcase-table-use-case = Приклад використання

# Status labels
status-implemented = Реалізовано
status-partial = Частково
status-planned = Заплановано

# Aggregates Showcase
aggregates-title = Агрегати SQL та GROUP BY
aggregates-description = Агрегатні функції SQL:1999 Core та можливості групування
aggregates-reference = Довідник агрегатних функцій
aggregates-table-function = Функція
aggregates-progress-type = функцій
aggregates-ex-basic = Базові агрегатні функції
aggregates-ex-group-single = GROUP BY (одна колонка)
aggregates-ex-group-multiple = GROUP BY (кілька колонок)
aggregates-ex-having = Умова HAVING
aggregates-ex-orderby = ORDER BY з агрегатами
aggregates-ex-null = Обробка NULL в агрегатах

# DML Operations Showcase
dml-title = DML-операції (мова маніпулювання даними)
dml-description = Операції SQL:1999 Core для запиту та зміни даних
dml-reference = Довідник DML-операцій
dml-table-operation = Операція
dml-progress-type = операцій
dml-ex-select-basic = SELECT - базові запити
dml-ex-select-ordering = SELECT - сортування та обмеження
dml-ex-insert = Операції INSERT
dml-ex-update = Операції UPDATE
dml-ex-delete = Операції DELETE
dml-ex-combined = Комбінований робочий процес CRUD

# Data Types Showcase
datatypes-title = Типи даних SQL:1999 Core
datatypes-description = Вивчення фундаментальних типів даних специфікації SQL:1999 Core
datatypes-reference = Довідник типів даних
datatypes-table-type = Ім'я типу
datatypes-table-example = Приклади значень
datatypes-table-spec = Специфікація
datatypes-progress-type = типів
datatypes-ex-numeric = Робота з числовими типами
datatypes-ex-null = Обробка NULL та тризначна логіка
datatypes-ex-comparisons = Порівняння типів та операції

# JOINs Showcase
joins-title = SQL JOIN
joins-description = Операції JOIN SQL:1999 Core для об'єднання даних з кількох таблиць
joins-reference = Довідник типів JOIN
joins-table-type = Тип JOIN
joins-progress-type = типів JOIN
joins-category-suffix = JOIN
joins-ex-sample = Налаштування тестових даних
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Багатотабличний JOIN

# Predicates Showcase
predicates-title = Предикати та оператори
predicates-description = Предикати SQL:1999 для фільтрації та логічних операцій
predicates-reference = Довідник предикатів
predicates-table-predicate = Предикат
predicates-progress-type = предикатів
predicates-ex-comparison = Оператори порівняння
predicates-ex-between = BETWEEN та предикати діапазону
predicates-ex-null = Предикати NULL та тризначна логіка
predicates-ex-boolean = Булева логіка (AND, OR, NOT)
predicates-ex-in = Предикат IN з підзапитами
predicates-ex-combined = Комбіновані операції з предикатами

# Subqueries Showcase
subqueries-title = SQL-підзапити
subqueries-description = Можливості підзапитів SQL:1999 Core для вкладених запитів
subqueries-reference = Довідник типів підзапитів
subqueries-table-type = Тип підзапиту
subqueries-progress-type = типів підзапитів
subqueries-ex-scalar-select = Скалярний підзапит в SELECT
subqueries-ex-scalar-where = Скалярний підзапит в WHERE
subqueries-ex-derived = Похідні таблиці (підзапит в FROM)
subqueries-ex-in = Предикат IN з підзапитом
subqueries-ex-correlated = Корельовані підзапити
subqueries-ex-nested = Вкладені підзапити

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
bench-no-wasm-data = Дані WASM недоступні
bench-no-server-data = Дані бенчмарку сервера Sysbench недоступні
bench-no-server-data-hint = Серверні бенчмарки вимагають запуску sysbench_server з увімкненим порівнянням MySQL.

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
bench-sysbench-emb-disc-architecture-title = Архітектурні компроміси
bench-sysbench-emb-disc-architecture = Гібридна архітектура VibeSQL орієнтована як на OLTP, так і на OLAP навантаження. Наше B-tree сховище забезпечує продуктивність точкових запитів на рівні SQLite, тоді як колонкове виконання ефективно обробляє аналітичні запити. Це відрізняється від чистих OLAP баз даних, таких як DuckDB, які оптимізовані виключно для масових операцій на шкоду затримці одиночних рядків.

# Sysbench Server specific
bench-sysbench-server-name = Sysbench (Server)
bench-sysbench-server-title = Sysbench Micro-Benchmarks (Server)
bench-sysbench-server-description = <strong>Sysbench</strong> server benchmarks compare VibeSQL Server (PostgreSQL wire protocol) against MySQL, measuring performance for multi-client database deployments.
bench-sysbench-server-ops-label = Sysbench operations
bench-sysbench-server-note = Server mode uses the PostgreSQL wire protocol, enabling multi-client access and compatibility with existing PostgreSQL tooling and drivers.

# Sysbench Server Discussion
bench-sysbench-srv-disc-protocol-title = PostgreSQL Wire Protocol
bench-sysbench-srv-disc-protocol = VibeSQL Server implements the PostgreSQL wire protocol, enabling compatibility with existing PostgreSQL drivers and tools. This adds ~10-50µs of protocol overhead per query compared to embedded mode, but enables multi-client deployments.
bench-sysbench-srv-disc-mysql-title = Порівняння з MySQL
bench-sysbench-srv-disc-mysql = Серверні бенчмарки порівнюють з MySQL для оцінки VibeSQL як заміни традиційних клієнт-серверних баз даних. VibeSQL Server перевершує MySQL у всіх операціях Sysbench з прискоренням від <strong>2.4x</strong> (діапазонні запити) до <strong>12.8x</strong> (індексовані оновлення).
bench-sysbench-srv-disc-perf-title = Чому VibeSQL Server швидший
bench-sysbench-srv-disc-perf-arch = Архітектура VibeSQL принципово відрізняється від традиційного дизайну СУБД MySQL
bench-sysbench-srv-disc-perf-storage = VibeSQL використовує колонковий рушій зберігання в пам'яті, оптимізований для аналітичних та OLTP навантажень, уникаючи накладних витрат управління сторінками InnoDB на диску
bench-sysbench-srv-disc-perf-locking = Без важкого блокування на рівні рядків або обліку MVCC — VibeSQL використовує легковісний контроль паралелізму для сучасних багатоядерних процесорів
bench-sysbench-srv-disc-perf-protocol = Ефективна реалізація протоколу PostgreSQL з мінімальними накладними витратами серіалізації порівняно з протоколом MySQL
bench-sysbench-srv-disc-perf-writes = Операції запису (вставки/оновлення) показують найбільший приріст (<strong>8-12x</strong>), оскільки VibeSQL уникає синхронізації redo-логу, undo-логу та буфера подвійного запису MySQL
bench-sysbench-srv-disc-perf-reads = Операції читання показують менший, але стабільний приріст (<strong>2-3x</strong>) завдяки ефективним колонковим патернам доступу та нульовому дисковому вводу-виводу
bench-sysbench-srv-disc-roadmap-title = Дорожня карта сервера
bench-sysbench-srv-disc-pooling = Reduce connection establishment overhead for high-throughput scenarios
bench-sysbench-srv-disc-caching = Server-side caching of query plans across connections
bench-sysbench-srv-disc-extended = Повна підтримка розширеного протоколу запитів PostgreSQL для пакетних операцій

# TPC-H Server специфічні
bench-tpch-server-name = TPC-H (Сервер)
bench-tpch-server-title = Аналітичний бенчмарк TPC-H (Сервер)
bench-tpch-server-description = <strong>Серверні бенчмарки TPC-H</strong> порівнюють VibeSQL Server (протокол PostgreSQL) з MySQL для аналітичних навантажень, вимірюючи продуктивність OLAP у клієнт-серверних розгортаннях.
bench-tpch-server-ops-label = запитів TPC-H
bench-tpch-server-note-intro = Серверні бенчмарки тестують реалізацію <strong>протоколу PostgreSQL</strong>, вимірюючи наскрізну затримку запитів включаючи мережеві накладні витрати.
bench-tpch-server-note-queries = Запити тестують складні JOIN, підзапити та агрегації, типові для бізнес-аналітики.

# Обговорення TPC-H Server
bench-tpch-srv-disc-protocol-title = Протокол PostgreSQL
bench-tpch-srv-disc-protocol = VibeSQL Server говорить протоколом PostgreSQL, дозволяючи використовувати стандартні драйвери та інструменти PostgreSQL. Цей бенчмарк вимірює повну наскрізну затримку включаючи накладні витрати протоколу.
bench-tpch-srv-disc-comparison-title = Порівняння з MySQL
bench-tpch-srv-disc-comparison = Порівняння з MySQL забезпечує базову лінію для традиційних клієнт-серверних баз даних на аналітичних навантаженнях. Колонковий движок виконання VibeSQL забезпечує переваги для складних агрегацій та об'єднань.
bench-tpch-srv-disc-roadmap-title = Дорожня карта серверного OLAP
bench-tpch-srv-disc-prepared = Повторне використання скомпільованих планів запитів між з'єднаннями
bench-tpch-srv-disc-pooling = Ефективна обробка з'єднань для сценаріїв з високою пропускною здатністю
bench-tpch-srv-disc-scale = Тестування більших наборів даних (SF 0.1, SF 1.0) для перевірки на виробничому масштабі

# TPC-C Server специфічні
bench-tpcc-server-name = TPC-C (Сервер)
bench-tpcc-server-title = OLTP бенчмарк TPC-C (Сервер)
bench-tpcc-server-description = <strong>Серверні бенчмарки TPC-C</strong> порівнюють VibeSQL Server (протокол PostgreSQL) з MySQL для транзакційних OLTP навантажень, вимірюючи пропускну здатність для багатоклієнтських розгортань баз даних.
bench-tpcc-server-ops-label = транзакцій TPC-C
bench-tpcc-server-note-intro = Серверні бенчмарки тестують реалізацію <strong>протоколу PostgreSQL</strong>, вимірюючи транзакційну пропускну здатність включаючи мережеві накладні витрати.
bench-tpcc-server-note-results = Результати показують транзакції за секунду (TPS) для стандартного міксу транзакцій TPC-C.
bench-tpcc-mixed = Змішане навантаження - Стандартний мікс транзакцій TPC-C (45% Нове-Замовлення, 43% Оплата, 4% Статус-Замовлення, 4% Доставка, 4% Рівень-Запасів)

# Обговорення TPC-C Server
bench-tpcc-srv-disc-protocol-title = Протокол PostgreSQL
bench-tpcc-srv-disc-protocol = VibeSQL Server говорить протоколом PostgreSQL, дозволяючи використовувати стандартні драйвери та інструменти PostgreSQL. Цей бенчмарк вимірює повну наскрізну транзакційну затримку включаючи накладні витрати протоколу.
bench-tpcc-srv-disc-comparison-title = Порівняння з MySQL
bench-tpcc-srv-disc-comparison = Порівняння з MySQL забезпечує базову лінію для традиційних клієнт-серверних баз даних на OLTP навантаженнях. MySQL є галузевим стандартом для транзакційних навантажень, і TPC-C є сильною стороною MySQL.
bench-tpcc-srv-disc-roadmap-title = Дорожня карта серверного OLTP
bench-tpcc-srv-disc-prepared = Повторне використання скомпільованих планів запитів між з'єднаннями
bench-tpcc-srv-disc-pooling = Ефективна обробка з'єднань для сценаріїв з високою пропускною здатністю
bench-tpcc-srv-disc-parallel = Паралельна обробка багатоклієнтських транзакцій

# Footprint Embedded специфічні
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
bench-tpcc-disc-duckdb-title = Чому DuckDB відстає в OLTP
bench-tpcc-disc-duckdb = DuckDB досягає лише ~385 TPS в TPC-C (у 60 разів повільніше за VibeSQL, у 12 разів повільніше за SQLite). Це очікувано: DuckDB — це <strong>аналітична (OLAP) база даних</strong>, оптимізована для великих пакетних операцій, а не для транзакцій з одиночними рядками. Її колонковий формат зберігання чудово справляється зі скануванням мільйонів рядків, але додає накладні витрати для точкових запитів і дрібних оновлень, які переважають в OLTP-навантаженнях типу TPC-C.
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
conformance-pgsql-title = Регресійні тести PostgreSQL
conformance-pgsql-desc = Результати запуску <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">набору регресійних тестів PostgreSQL</a> - канонічного набору тестів для перевірки сумісності з PostgreSQL.
conformance-pgsql-tests-passing = тестів пройдено
conformance-pgsql-tests-excluded = тестів виключено
conformance-pgsql-pass-rate = Показник Успішності
conformance-pgsql-excluded-reason = Виключені тести використовують специфічні для PostgreSQL функції, які не застосовуються до VibeSQL
conformance-pgsql-note = <strong>Примітка:</strong> Регресійні тести PostgreSQL перевіряють поведінку SQL порівняно з еталонною реалізацією PostgreSQL. Виключені тести стосуються специфічних для PostgreSQL функцій, таких як системні каталоги, процедурні мови або модулі розширень.

# Розділ тестового набору SQLite TCL
conformance-tcl-title = Тестовий набір SQLite TCL
conformance-tcl-desc = Результати канонічного <a href="https://www.sqlite.org/testing.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">тестового набору TCL</a> SQLite, що містить { $fileCount } тестових файлів. Цей набір є золотим стандартом для тестування сумісності з SQLite.
conformance-tcl-overall-rate = Загальний Відсоток Проходження
conformance-tcl-tests-passing = { $passed } з { $total } тестів пройдено
conformance-tcl-passed = Пройдено
conformance-tcl-failed = Не пройдено
conformance-tcl-skipped = Пропущено
conformance-tcl-total = Всього
conformance-tcl-categories-title = Категорії Тестів
conformance-tcl-category = Категорія
conformance-tcl-rate = Показник
conformance-tcl-progress = Прогрес
conformance-tcl-tests = Тести
conformance-tcl-common-failures = Часті Помилки
conformance-tcl-failure-patterns = Топ { $count } шаблонів помилок за кількістю появ
conformance-tcl-about-title = Про тести TCL:
conformance-tcl-about-text = Тестовий набір TCL від SQLite є канонічним тестом відповідності для сумісності з SQLite. Він тестує специфічну поведінку SQLite, особливості та граничні випадки, які можуть не покриватися стандартними наборами SQL-тестів. Високий відсоток проходження тут вказує на сильну сумісність з SQLite для сценаріїв міграції додатків.

# =============================================================================
# Challenge Page
# =============================================================================

# Page title and header
challenge-page-title = Виклик SQL Vibe Coding - VibeSQL
challenge-header = Виклик SQL Vibe Coding

# Hero section
challenge-hero-title = Виклик SQL Vibe Coding
challenge-hero-subtitle = Об'єктивний бенчмарк для мультиагентної розробки програмного забезпечення. Побудуйте базу даних SQL з нуля. Пройдіть 6 мільйонів тестів. Виграйте трофей.
challenge-btn-start = Почати Будувати
challenge-btn-trophy = Переглянути Трофей
challenge-btn-leaderboard = Таблиця Лідерів

# Key Insight callout
challenge-insight-title = Єдина Метрика, Що Має Значення: Календарний Час
challenge-insight-text = Коміти та рядки коду — це лише проксі. Важливо <strong>скільки днів до завершення</strong>. Чи можуть 1000 агентів, що працюють паралельно, перемогти 100 агентів? Чи зберігає ваш фреймворк оркестрації продуктивність при масштабуванні? Цей бенчмарк вам скаже.

# The Challenge section
challenge-section-title = Виклик
challenge-objective-title = Мета
challenge-objective-text = Побудуйте движок бази даних SQL з нуля, який пройде <a href="https://www.sqlite.org/sqllogictest/" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">набір SQLLogicTest</a>. Це той самий набір тестів, що використовується для валідації SQLite, DuckDB та інших продакшн баз даних.
challenge-success-title = Критерії Успіху
challenge-success-pass-rate = 100% успішність на наборі SQLLogicTest
challenge-success-assertions = ~6 мільйонів індивідуальних тестових твердень
challenge-success-files = Всі 622 тестових файли пройдено
challenge-constraints-title = Обмеження
challenge-constraint-parser = <strong>Жодних існуючих бібліотек SQL-парсерів</strong> — створіть власний парсер
challenge-constraint-engine = <strong>Жодних існуючих движків запитів</strong> — реалізуйте виконання з нуля
challenge-constraint-libs = <strong>Жодних бібліотек, специфічних для баз даних</strong> — використовуйте лише бібліотеки загального призначення
challenge-allowed-title = Дозволено
challenge-allowed-lang = Будь-яка мова програмування
challenge-allowed-ai = Будь-який фреймворк AI-оркестрації
challenge-allowed-human = Людське втручання (без обмежень)
challenge-allowed-libs = Бібліотеки загального призначення (структури даних, I/O тощо)

# The Trophy section
challenge-trophy-title = Трофей
challenge-trophy-name = Трофей Vibe Coding
challenge-trophy-desc = Фізичний трофей буде вручено кожному власнику рекорду. Дизайн відображає дух "vibe coding" — позолочений жезл, встановлений на горіховому дереві з латунними табличками.
challenge-trophy-claim = <strong>Ваше ім'я буде на трофеї</strong>, коли ви переб'єте поточний рекорд мінімум на 5%.
challenge-rules-title = Правила Нагородження
challenge-rule-improve = <strong>Потрібне покращення на 5%</strong> — переб'єте попередній рекорд мінімум на 5% (у календарних днях), щоб отримати трофей
challenge-rule-public = <strong>Публічний репозиторій</strong> — ваш код повинен бути публічно доступним для перевірки
challenge-rule-pass = <strong>100% успішність</strong> — всі 622 файли SQLLogicTest повинні пройти
challenge-rule-git = <strong>Перевірювана історія git</strong> — дата першого коміту до 100% успішності визначає ваш час
challenge-record-title = Поточний Власник Рекорду
challenge-record-days = { $days } днів
challenge-record-name = VibeSQL (Базовий)
challenge-record-date = Жовтень - Листопад 2025
challenge-record-beat = Побити це на 5%? Це <strong>{ $target } днів або менше</strong> для отримання трофею.

# Why This Challenge section
challenge-why-title = Чому Цей Виклик?
challenge-why-objective-title = Об'єктивне Вимірювання
challenge-why-objective-text = Жодних суб'єктивних оглядів коду. Тести або проходять, або ні. 6 мільйонів твердень не залишають місця для неоднозначності.
challenge-why-complexity-title = Реальна Складність
challenge-why-complexity-text = Бази даних SQL вимагають парсерів, оптимізаторів та движків виконання. Це не іграшкова проблема — це інженерія продакшн-рівня.
challenge-why-time-title = Час — Це Правда
challenge-why-time-text = Календарні дні до завершення — це остаточна метрика. Чи допомагає розпаралелювання до 1000 агентів? Тепер ви можете дізнатися.

# Get Started section
challenge-start-title = Почати
challenge-start-intro = Почніть з нуля на будь-якій мові або використайте один з наших seed-репозиторіїв для зручності. Кожен seed включає набір SQLLogicTest, test runner та CI workflow.
challenge-seed-title = Seed Репозиторії
challenge-seed-optional = (опціонально)
challenge-seed-rust-desc = Система збірки Cargo, абстракції з нульовою вартістю, безпека пам'яті без GC.
challenge-seed-cpp-desc = Система збірки CMake, максимальна продуктивність, повний контроль над пам'яттю.
challenge-seed-go-desc = Простий toolchain, швидка компіляція, відмінні примітиви конкурентності.
challenge-seed-fork = Fork на GitHub →
challenge-step1-title = Запустіть Свій Проект
challenge-step1-text = Створіть новий репозиторій з нуля або зробіть fork seed вище для швидкого старту. Отримайте <a href="https://www.sqlite.org/sqllogictest/" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">набір SQLLogicTest</a>. Ваш перший коміт запускає годинник.
challenge-step2-title = Побудуйте Свою Базу Даних
challenge-step2-text = Реалізуйте SQL-парсер, виконавець запитів та движок зберігання. Використовуйте будь-які AI-інструменти — Claude, Copilot або власних агентів. Запустіть <code class="bg-gray-200 dark:bg-gray-700 px-1 rounded">make test</code> для відстеження прогресу.
challenge-step3-title = Досягніть 100% та Подайте
challenge-step3-text = Коли всі 622 тестових файли пройдуть, відкрийте issue на <a href="https://github.com/vibesql-challenge/submissions" class="text-blue-600 dark:text-blue-400 hover:underline" target="_blank" rel="noopener">vibesql-challenge/submissions</a> з посиланням на репозиторій та хешами комітів. Переб'єте 25 днів, щоб потрапити в таблицю лідерів.

# Explore VibeSQL section
challenge-explore-title = Досліджуйте VibeSQL
challenge-explore-demo-title = Спробуйте Демо
challenge-explore-demo-text = Виконуйте SQL-запити у браузері за допомогою WebAssembly збірки.
challenge-explore-conformance-title = Звіт про Відповідність
challenge-explore-conformance-text = Детальний аналіз відповідності стандарту SQL:1999.
challenge-explore-benchmarks-title = Бенчмарки Продуктивності
challenge-explore-benchmarks-text = TPC-H, TPC-C та інші бенчмарки проти SQLite та DuckDB.

# Footer
challenge-footer = VibeSQL - База Даних SQL:1999 у WebAssembly

# Navigation
nav-challenge = Виклик SQL Vibe Coding
nav-trends = Тренди Продуктивності

# Trends page
trends-title = Тренди Продуктивності - VibeSQL
trends-heading = VibeSQL - Тренди Продуктивності
trends-total-runs = Загальна Кількість Запусків Бенчмарків
trends-across-suites = по всіх наборах
trends-date-range = Діапазон Дат
trends-first-to-last = від першого до останнього запуску
trends-latest-commit = Останній Коміт
trends-most-recent = найновіший бенчмарк
trends-generated = Згенеровано
trends-last-export = останній експорт даних

# Bullet points
bench-bullet-prepared-stmts = Підготовлені запити
bench-bullet-larger-scale = Більші фактори масштабу
bench-bullet-parallel-clients = Паралельні клієнти
