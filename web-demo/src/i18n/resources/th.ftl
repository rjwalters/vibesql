# VibeSQL Web UI - ไทย

# Page titles
page-title = VibeSQL - ฐานข้อมูล SQL:1999 ที่ขับเคลื่อนด้วย AI
demo-title = สาธิต VibeSQL
benchmarks-title = เกณฑ์มาตรฐานประสิทธิภาพ - VibeSQL
benchmarks-heading = VibeSQL - เกณฑ์มาตรฐานประสิทธิภาพ
conformance-title = รายงานความสอดคล้อง - VibeSQL
conformance-heading = รายงานความสอดคล้อง
conformance-subtitle = การทดสอบความสอดคล้องกับมาตรฐาน SQL:1999

# Navigation
nav-showcase = โชว์เคส SQL:1999
nav-conformance = ดูผลลัพธ์ sqltest
nav-sqllogictest = ดูผลลัพธ์ SQLLogicTest

# Editor section
editor-title = ตัวแก้ไข SQL
editor-storage = ที่เก็บข้อมูล
editor-storage-init = กำลังเริ่มต้น...
editor-execute = รันคิวรี

# Results section
results-title = ผลลัพธ์
results-empty = รันคิวรีเพื่อดูผลลัพธ์
results-loading = กำลังโหลด...
results-rows = { $count } แถว
results-rows-with-time = { $count } แถว ({ $time }ms)
results-copy = คัดลอกไปยังคลิปบอร์ด
results-export = ส่งออก CSV
results-limit-warning = แสดง { $limit } แถวแรกจากทั้งหมด { $total } แถว ใช้ LIMIT เพื่อปรับแต่งคิวรีของคุณ

# Examples sidebar
examples-title = ตัวอย่าง
examples-basic = คิวรีพื้นฐาน
examples-advanced = คิวรีขั้นสูง

# Database selector
db-select-label = ฐานข้อมูล

# Footer
footer-tagline = VibeSQL - ฐานข้อมูล SQL:1999 ใน WebAssembly
footer-deployed = ใช้งาน: { $date }

# Theme
theme-toggle-dark = เปลี่ยนเป็นโหมดมืด
theme-toggle-light = เปลี่ยนเป็นโหมดสว่าง

# Locale
locale-select = เลือกภาษา

# Messages
msg-query-success = รันคิวรีสำเร็จ
msg-rows-affected = { $count } แถวได้รับผลกระทบ

# Errors
error-generic = เกิดข้อผิดพลาด
error-query-failed = คิวรีล้มเหลว
error-no-databases = ไม่มีฐานข้อมูลที่ใช้ได้

# Loading states
loading-initializing-theme = กำลังเริ่มต้นธีม
loading-preparing-editor = กำลังเตรียมตัวแก้ไข
loading-database-engine = กำลังโหลดเอ็นจิ้นฐานข้อมูล
loading-setting-up-ui = กำลังตั้งค่าอินเทอร์เฟซผู้ใช้
loading-editor = กำลังโหลดตัวแก้ไข...
loading-compliance-data = กำลังโหลดข้อมูลการปฏิบัติตามกฎ...
loading-conformance-report = กำลังโหลดรายงานความสอดคล้อง...

# Editor
editor-placeholder = ป้อนคิวรี SQL ที่นี่... (Ctrl+Enter หรือ Cmd+Enter เพื่อรัน)

# Navigation links
nav-terminal = สาธิตเทอร์มินัล SQL
nav-compliance = รายงานความสอดคล้อง SQL
nav-benchmarks = เกณฑ์มาตรฐานประสิทธิภาพ
nav-github = คลัง GitHub
nav-home = หน้าแรก

# Results
results-success-zero = รันคิวรีสำเร็จ (0 แถว)
results-null = NULL

# Help Modal
help-title = ปุ่มลัดและความช่วยเหลือ
help-close = ปิด
help-editor-shortcuts = ปุ่มลัดตัวแก้ไข
help-navigation = การนำทาง
help-results-actions = การดำเนินการผลลัพธ์
help-tips = เคล็ดลับ
help-shortcut-execute = รันคิวรีปัจจุบัน
help-shortcut-comment = สลับคอมเมนต์บรรทัด
help-shortcut-indent = เยื้องส่วนที่เลือก
help-shortcut-show-help = แสดงหน้าต่างช่วยเหลือนี้
help-shortcut-close-help = ปิดหน้าต่างช่วยเหลือ
help-action-copy = คัดลอกไปยังคลิปบอร์ด
help-action-copy-desc = คัดลอกผลลัพธ์เป็นค่าที่คั่นด้วยแท็บ
help-action-export = ส่งออก CSV
help-action-export-desc = ดาวน์โหลดผลลัพธ์เป็นไฟล์ CSV
help-tip-limit = ผลลัพธ์จำกัดที่ 1,000 แถวเพื่อประสิทธิภาพ ใช้ LIMIT เพื่อปรับแต่งคิวรี
help-tip-time = เวลาดำเนินการแสดงพร้อมกับผลลัพธ์คิวรี
help-tip-syntax = ตัวแก้ไขรองรับการเน้นไวยากรณ์ SQL และการเติมอัตโนมัติ
help-tip-theme = สลับระหว่างโหมดสว่าง/มืดด้วยปุ่มธีม
help-got-it = เข้าใจแล้ว!

# Showcase Navigation
showcase-title = โชว์เคส SQL:1999 Core
showcase-description = สำรวจฟีเจอร์ SQL:1999 Core ที่ใช้งานได้แบบโต้ตอบ
showcase-complete = { $percent }% เสร็จสมบูรณ์
showcase-categories = หมวดหมู่ฟีเจอร์
showcase-legend = คำอธิบายสถานะ
showcase-status-implemented = ใช้งานได้เต็มรูปแบบ
showcase-status-partial = ใช้งานได้บางส่วน
showcase-status-planned = วางแผนไว้

# Showcase category labels
showcase-cat-compliance = แดชบอร์ดความสอดคล้อง
showcase-cat-data-types = ประเภทข้อมูล
showcase-cat-dml = การดำเนินการ DML
showcase-cat-predicates = เพรดิเคตและโอเปอเรเตอร์
showcase-cat-joins = JOIN
showcase-cat-subqueries = ซับคิวรี
showcase-cat-aggregates = การรวมและ GROUP BY
showcase-cat-ddl = DDL และข้อจำกัด

# Common showcase elements
showcase-interactive-examples = ตัวอย่างแบบโต้ตอบ
showcase-try-example = ลองตัวอย่างนี้
showcase-progress = { $implemented } จาก { $total } { $type } ({ $percent }%)
showcase-table-status = สถานะ
showcase-table-category = หมวดหมู่
showcase-table-description = คำอธิบาย
showcase-table-syntax = ไวยากรณ์
showcase-table-use-case = กรณีใช้งาน

# Status labels
status-implemented = ใช้งานได้
status-partial = บางส่วน
status-planned = วางแผนไว้

# Aggregates Showcase
aggregates-title = การรวม SQL และ GROUP BY
aggregates-description = ฟังก์ชันรวม SQL:1999 Core และความสามารถในการจัดกลุ่ม
aggregates-reference = คู่มือฟังก์ชันรวม
aggregates-table-function = ฟังก์ชัน
aggregates-progress-type = ฟังก์ชัน
aggregates-ex-basic = ฟังก์ชันรวมพื้นฐาน
aggregates-ex-group-single = GROUP BY (คอลัมน์เดียว)
aggregates-ex-group-multiple = GROUP BY (หลายคอลัมน์)
aggregates-ex-having = ประโยค HAVING
aggregates-ex-orderby = ORDER BY กับการรวม
aggregates-ex-null = การจัดการ NULL ในการรวม

# DML Operations Showcase
dml-title = การดำเนินการ DML (ภาษาจัดการข้อมูล)
dml-description = การดำเนินการ SQL:1999 Core สำหรับการสืบค้นและแก้ไขข้อมูล
dml-reference = คู่มือการดำเนินการ DML
dml-table-operation = การดำเนินการ
dml-progress-type = การดำเนินการ
dml-ex-select-basic = SELECT - คิวรีพื้นฐาน
dml-ex-select-ordering = SELECT - การเรียงลำดับและจำกัด
dml-ex-insert = การดำเนินการ INSERT
dml-ex-update = การดำเนินการ UPDATE
dml-ex-delete = การดำเนินการ DELETE
dml-ex-combined = เวิร์กโฟลว์ CRUD รวม

# Data Types Showcase
datatypes-title = ประเภทข้อมูล SQL:1999 Core
datatypes-description = สำรวจประเภทข้อมูลพื้นฐานที่กำหนดในข้อกำหนด SQL:1999 Core
datatypes-reference = คู่มือประเภทข้อมูล
datatypes-table-type = ชื่อประเภท
datatypes-table-example = ค่าตัวอย่าง
datatypes-table-spec = ข้อกำหนด
datatypes-progress-type = ประเภท
datatypes-ex-numeric = การทำงานกับประเภทตัวเลข
datatypes-ex-null = การจัดการ NULL และตรรกะสามค่า
datatypes-ex-comparisons = การเปรียบเทียบประเภทและการดำเนินการ

# JOINs Showcase
joins-title = SQL JOIN
joins-description = การดำเนินการ JOIN SQL:1999 Core สำหรับการรวมข้อมูลจากหลายตาราง
joins-reference = คู่มือประเภท JOIN
joins-table-type = ประเภท JOIN
joins-progress-type = ประเภท JOIN
joins-category-suffix = JOIN
joins-ex-sample = การตั้งค่าข้อมูลตัวอย่าง
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = JOIN หลายตาราง

# Predicates Showcase
predicates-title = เพรดิเคตและโอเปอเรเตอร์
predicates-description = เพรดิเคต SQL:1999 สำหรับการกรองและการดำเนินการเชิงตรรกะ
predicates-reference = คู่มือเพรดิเคต
predicates-table-predicate = เพรดิเคต
predicates-progress-type = เพรดิเคต
predicates-ex-comparison = โอเปอเรเตอร์เปรียบเทียบ
predicates-ex-between = BETWEEN และเพรดิเคตช่วง
predicates-ex-null = เพรดิเคต NULL และตรรกะสามค่า
predicates-ex-boolean = ตรรกะบูลีน (AND, OR, NOT)
predicates-ex-in = เพรดิเคต IN กับซับคิวรี
predicates-ex-combined = การดำเนินการเพรดิเคตรวม

# Subqueries Showcase
subqueries-title = ซับคิวรี SQL
subqueries-description = ความสามารถซับคิวรี SQL:1999 Core สำหรับการดำเนินการคิวรีซ้อน
subqueries-reference = คู่มือประเภทซับคิวรี
subqueries-table-type = ประเภทซับคิวรี
subqueries-progress-type = ประเภทซับคิวรี
subqueries-ex-scalar-select = ซับคิวรีสเกลาร์ใน SELECT
subqueries-ex-scalar-where = ซับคิวรีสเกลาร์ใน WHERE
subqueries-ex-derived = ตารางที่ได้มา (ซับคิวรีใน FROM)
subqueries-ex-in = เพรดิเคต IN กับซับคิวรี
subqueries-ex-correlated = ซับคิวรีสัมพันธ์
subqueries-ex-nested = ซับคิวรีซ้อน

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
