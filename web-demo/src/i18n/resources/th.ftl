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
bench-no-wasm-data = ไม่มีข้อมูล WASM
bench-no-server-data = ไม่มีข้อมูล Sysbench server benchmark
bench-no-server-data-hint = Server benchmarks ต้องเรียกใช้ sysbench_server โดยเปิดใช้งานการเปรียบเทียบ MySQL

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
bench-tpch-description = เกณฑ์มาตรฐานเหล่านี้ใช้ <strong>ชุดเกณฑ์มาตรฐาน TPC-H</strong> ที่เป็นมาตรฐานอุตสาหกรรม ซึ่งจำลองภาระงานสนับสนุนการตัดสินใจในโลกจริงด้วยการสืบค้นเชิงวิเคราะห์ที่ซับซ้อนที่เกี่ยวข้องกับการรวม การเชื่อม การสืบค้นย่อย และการเรียงลำดับ
bench-tpch-ops-label = TPC-H queries
bench-tpch-note-intro = เกณฑ์มาตรฐานทั้งหมดวัดเวลาการดำเนินการสืบค้นแบบครบวงจรรวมถึงการแยกวิเคราะห์ การวางแผน การดำเนินการ และการสร้างผลลัพธ์ นี่แสดงถึง <strong>ประสิทธิภาพเครื่องมือ SQL ในโลกจริง</strong> สำหรับภาระงานเชิงวิเคราะห์
bench-tpch-note-queries = <strong>หมายเหตุ:</strong> การสืบค้น TPC-H ทดสอบแง่มุมต่างๆ ของประสิทธิภาพ SQL: การรวมแบบง่าย (Q1, Q6) การเชื่อมที่ซับซ้อน (Q2-Q5, Q7-Q10) การสืบค้นย่อย (Q11-Q15) และการวิเคราะห์ขั้นสูง (Q16-Q22) เลื่อนเมาส์ไปที่ชื่อการสืบค้นในตารางด้านบนเพื่อดูคำอธิบาย

# TPC-H Discussion
bench-tpch-disc-excels-title = จุดที่ VibeSQL โดดเด่น
bench-tpch-disc-excels = VibeSQL แสดงประสิทธิภาพที่แข็งแกร่งใน <strong>การสืบค้นการรวมที่เน้นการสแกน</strong> (Q1, Q6, Q14, Q15, Q20) ที่เครื่องมือดำเนินการแบบคอลัมน์และการรวมที่เร่งความเร็วด้วย SIMD ของเราโดดเด่น การสืบค้นเหล่านี้เกี่ยวข้องกับการกรองตารางขนาดใหญ่และการคำนวณการรวมโดยไม่มีรูปแบบการเชื่อมที่ซับซ้อน
bench-tpch-disc-targets-title = เป้าหมายการเพิ่มประสิทธิภาพปัจจุบัน
bench-tpch-disc-targets = การสืบค้นการเชื่อมหลายทาง (Q3, Q5, Q7-Q10, Q18, Q19, Q21) ปัจจุบันแสดงให้เห็นว่า SQLite นำหน้า ปัญหาคอขวดหลักคือการใช้งานแฮชจอยน์ของเรา ซึ่งยังไม่ได้ใช้ระดับการเพิ่มประสิทธิภาพเดียวกับการเชื่อม B-tree ที่ SQLite ปรับปรุงมาหลายทศวรรษ พื้นที่เฉพาะที่กำลังพัฒนาอย่างแข็งขัน:
bench-tpch-disc-join-ordering = การประมาณคาร์ดินาลิตี้ที่ดีขึ้นสำหรับการเลือกลำดับการเชื่อมที่ดีกว่า
bench-tpch-disc-hash-sizing = การเติบโตของตารางแฮชแบบปรับตัวและการล้นไปยังดิสก์สำหรับการเชื่อมขนาดใหญ่
bench-tpch-disc-vectorized = การประมวลผลแบบแบตช์ในลูปภายในของการเชื่อมเพื่อปรับปรุงการใช้แคช
bench-tpch-disc-inl-joins = ใช้ประโยชน์จากดัชนี B-tree เมื่อเป็นประโยชน์
bench-tpch-disc-path-title = เส้นทางสู่การเป็นผู้นำ
bench-tpch-disc-path = สถาปัตยกรรมของ VibeSQL ได้รับการออกแบบสำหรับฮาร์ดแวร์สมัยใหม่ด้วยคุณสมบัติเช่นการจัดเก็บแบบคอลัมน์ การดำเนินการแบบเวกเตอร์ และการทำงานพร้อมกันแบบไม่ล็อก เมื่อการเพิ่มประสิทธิภาพเหล่านี้เติบโตเต็มที่ เราคาดหวังว่า VibeSQL จะบรรลุความเป็นผู้นำอย่างสม่ำเสมอในการสืบค้น TPC-H ทั้งหมด

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
bench-tpcds-description = <strong>TPC-DS</strong> คือผู้สืบทอดของ TPC-H โดยมี 99 การสืบค้นที่จำลองระบบสนับสนุนการตัดสินใจสมัยใหม่ด้วยรูปแบบการสืบค้นที่ซับซ้อนมากขึ้นรวมถึงตารางข้อเท็จจริงหลายตาราง สคีมาสโนว์เฟลก และคุณสมบัติ SQL ขั้นสูง
bench-tpcds-ops-label = TPC-DS queries
bench-tpcds-note-intro = การสืบค้น TPC-DS ซับซ้อนกว่า TPC-H อย่างมาก โดยทดสอบคุณสมบัติ SQL ขั้นสูงเช่นฟังก์ชันหน้าต่าง นิพจน์ตารางทั่วไป (คำสั่ง WITH) และรูปแบบการเชื่อมที่ซับซ้อนข้ามตารางข้อเท็จจริงและมิติหลายตาราง
bench-tpcds-note-remaining = <strong>หมายเหตุ:</strong> การสืบค้น TPC-DS ทั้ง 99 รายการผ่าน แสดงให้เห็นการสนับสนุนคุณสมบัติ SQL:1999 ที่ครอบคลุมรวมถึง INTERSECT, EXCEPT, ฟังก์ชันหน้าต่าง, CTE และการสืบค้นย่อยที่ซับซ้อน

# TPC-DS Discussion
bench-tpcds-disc-coverage-title = ความครอบคลุมคุณสมบัติ SQL:1999
bench-tpcds-disc-coverage = TPC-DS ทดสอบคุณสมบัติ SQL ที่ต้องการมากที่สุด VibeSQL ผ่าน <strong>การสืบค้นทั้ง 99 รายการ</strong> แสดงให้เห็นความครอบคลุมที่สมบูรณ์ของ SQL:1999 รวมถึง ROLLUP, CUBE, GROUPING(), ฟังก์ชันหน้าต่างพร้อมการจัดเฟรมที่ซับซ้อน, CTE แบบเรียกซ้ำ และการดำเนินการเซต INTERSECT/EXCEPT
bench-tpcds-disc-optimization-title = การเพิ่มประสิทธิภาพการสืบค้นที่ซับซ้อน
bench-tpcds-disc-optimization = การสืบค้น TPC-DS มักเชื่อมตารางมากกว่า 10 ตารางพร้อมการสืบค้นย่อยที่สัมพันธ์กัน พื้นที่โฟกัสปัจจุบัน:
bench-tpcds-disc-cte = การตัดสินใจอัจฉริยะระหว่าง CTE ที่ถูกสร้างวัตถุและแบบอินไลน์
bench-tpcds-disc-decorrelation = การแปลงการสืบค้นย่อยที่สัมพันธ์กันเป็นการเชื่อมเมื่อเป็นประโยชน์
bench-tpcds-disc-star = การจัดลำดับการเชื่อมข้อเท็จจริง-มิติสำหรับรูปแบบการวิเคราะห์
bench-tpcds-disc-toward-title = ความครอบคลุม TPC-DS ที่สมบูรณ์
bench-tpcds-disc-toward = ด้วยการสืบค้นทั้ง 99 รายการผ่าน VibeSQL แสดงให้เห็นความสอดคล้อง SQL:1999 ที่พร้อมใช้งานจริงสำหรับภาระงานเชิงวิเคราะห์ที่ซับซ้อน การเพิ่มการดำเนินการเซต INTERSECT และ EXCEPT ล่าสุดทำให้ความครอบคลุม TPC-DS เสร็จสมบูรณ์
bench-tpcds-disc-sqlite-title = หมายเหตุการเปรียบเทียบ SQLite
bench-tpcds-disc-sqlite = SQLite ไม่สามารถดำเนินการ 12 จาก 99 การสืบค้น TPC-DS (Q2, Q5, Q14, Q17, Q18, Q22, Q36, Q67, Q70, Q77, Q80, Q86) เนื่องจากคุณสมบัติ OLAP ของ SQL:1999 ที่ขาดหายไป: ชุดการจัดกลุ่ม <strong>ROLLUP/CUBE</strong>, ฟังก์ชัน <strong>GROUPING()</strong> และ <strong>STDDEV_SAMP()</strong> การสืบค้นเหล่านี้ถูกข้ามในเกณฑ์มาตรฐาน SQLite VibeSQL และ DuckDB สนับสนุนการสืบค้นทั้ง 99 รายการ

# TPC-C specific
bench-tpcc-name = TPC-C
bench-tpcc-title = TPC-C Online Transaction Processing Benchmark
bench-tpcc-description = <strong>เกณฑ์มาตรฐาน TPC-C</strong> จำลองสภาพแวดล้อมการป้อนคำสั่งซื้อที่สมบูรณ์ด้วยการผสมผสานของธุรกรรมที่ซับซ้อนรวมถึงการป้อนคำสั่งซื้อ การประมวลผลการชำระเงิน การสืบค้นสถานะคำสั่งซื้อ การประมวลผลการจัดส่ง และการตรวจสอบระดับสต็อก
bench-tpcc-ops-label = TPC-C transactions
bench-tpcc-note-intro = TPC-C วัดธุรกรรมต่อนาที (tpmC) และทดสอบความสามารถของฐานข้อมูลในการจัดการธุรกรรมพร้อมกันด้วยตรรกะทางธุรกิจที่ซับซ้อน เกณฑ์มาตรฐานนี้มีความสำคัญอย่างยิ่งสำหรับการประเมิน <strong>ประสิทธิภาพภาระงานธุรกรรม</strong>
bench-tpcc-note-results = <strong>หมายเหตุ:</strong> ผลลัพธ์แสดงเวลาแฝงของธุรกรรมโดยเฉลี่ย ต่ำกว่าดีกว่า TPC-C มีความต้องการเป็นพิเศษสำหรับภาระงานที่เขียนมากพร้อมข้อกำหนดความสอดคล้องที่เข้มงวด

# TPC-C Transaction Descriptions
bench-tpcc-new-order = New Order - Complex transaction with inventory checks and order creation
bench-tpcc-payment = Payment - Update customer balance and warehouse/district totals
bench-tpcc-order-status = Order Status - Read-only query for customer order history
bench-tpcc-delivery = Delivery - Batch processing of pending orders
bench-tpcc-stock-level = Stock Level - Count items below threshold in recent orders

# TPC-C Discussion
bench-tpcc-disc-faster-title = เร็วกว่า SQLite 5 เท่า
bench-tpcc-disc-faster = VibeSQL บรรลุ <strong>ประมาณ 23,000 ธุรกรรมต่อวินาที</strong> เปรียบเทียบกับ ~4,500 TPS ของ SQLite เป็นการปรับปรุง 5 เท่า ความเร็วนี้มาจากสถาปัตยกรรม MVCC แบบไม่ล็อกของเราที่หลีกเลี่ยงการล็อกแบบหยาบของ SQLite ในทุกการดำเนินการเขียน
bench-tpcc-disc-dominates-title = เหตุใด VibeSQL จึงครอง OLTP
bench-tpcc-disc-lockfree = MVCC อนุญาตให้ผู้อ่านและผู้เขียนดำเนินการพร้อมกันโดยไม่มีการบล็อก
bench-tpcc-disc-optimistic = ธุรกรรมขัดแย้งกันเฉพาะเวลาคอมมิตเท่านั้น ไม่ใช่ระหว่างการดำเนินการ
bench-tpcc-disc-btree = โครงสร้างดัชนีที่สร้างขึ้นเฉพาะและเพิ่มประสิทธิภาพสำหรับภาระงานในหน่วยความจำ
bench-tpcc-disc-prepared = แผนการสืบค้นถูกคอมไพล์ครั้งเดียวและนำกลับมาใช้ใหม่
bench-tpcc-disc-scaling-title = การขยายตัวต่อไป
bench-tpcc-disc-scaling = ผลลัพธ์ปัจจุบันเป็นแบบเธรดเดียว สถาปัตยกรรมของ VibeSQL สนับสนุนการประมวลผลธุรกรรมแบบหลายเธรด และเราคาดหวังการขยายตัวที่ดีขึ้นเมื่อเราเพิ่มการสนับสนุนการดำเนินการแบบขนาน
bench-tpcc-disc-duckdb-title = เหตุใด DuckDB จึงตามหลังใน OLTP
bench-tpcc-disc-duckdb = DuckDB บรรลุเพียง ~385 TPS บน TPC-C (ช้ากว่า VibeSQL 60 เท่า ช้ากว่า SQLite 12 เท่า) นี่เป็นสิ่งที่คาดหวัง: DuckDB เป็น <strong>ฐานข้อมูลเชิงวิเคราะห์ (OLAP)</strong> ที่เพิ่มประสิทธิภาพสำหรับการดำเนินการแบตช์ขนาดใหญ่ ไม่ใช่ธุรกรรมแถวเดียว

# Sysbench Embedded specific
bench-sysbench-embedded-name = Sysbench (Embedded)
bench-sysbench-embedded-title = Sysbench Micro-Benchmarks (Embedded)
bench-sysbench-embedded-description = <strong>Sysbench</strong> ให้ไมโครเบนช์มาร์กที่มุ่งเน้นซึ่งแยกการดำเนินการฐานข้อมูลเฉพาะ การทดสอบเหล่านี้วัดประสิทธิภาพดิบสำหรับการดำเนินการพื้นฐานโดยไม่มีความซับซ้อนของภาระงานธุรกรรมเต็มรูปแบบ
bench-sysbench-embedded-ops-label = Sysbench operations
bench-sysbench-embedded-note = โหมดฝังตัวรันฐานข้อมูลภายในกระบวนการโดยไม่มีค่าใช้จ่ายเครือข่าย เหมาะสำหรับแอปพลิเคชันกระบวนการเดียวที่เวลาแฝงต่ำสุดเป็นสิ่งสำคัญ

# Sysbench Operation Descriptions
bench-sysbench-point-select = Point Select - Single row lookup by primary key
bench-sysbench-insert = Insert - Insert new rows into table
bench-sysbench-update-index = Update Index - Update indexed column (k = k + 1)
bench-sysbench-update-non-index = Update Non-Index - Update non-indexed column
bench-sysbench-delete = Delete - Remove rows by primary key
bench-sysbench-range-queries = Range Queries - Simple, SUM, ORDER BY, and DISTINCT range scans

# Sysbench Embedded Discussion
bench-sysbench-emb-disc-point-title = การค้นหาจุด: เท่าเทียมกัน
bench-sysbench-emb-disc-point = การเลือกจุดของ VibeSQL ทำงานที่ <strong>~0.37µs</strong> เทียบเท่ากับ ~0.36µs ของ SQLite การใช้งาน B-tree ของเราได้รับการเพิ่มประสิทธิภาพสำหรับการค้นหาแถวเดียวด้วยการไล่ตามตัวชี้น้อยที่สุดและเลย์เอาต์โหนดที่เป็นมิตรกับแคช
bench-sysbench-emb-disc-index-title = การอัปเดตดัชนี: มีช่องว่างสำหรับการปรับปรุง
bench-sysbench-emb-disc-index = การอัปเดตที่มีดัชนีของ VibeSQL ทำงานที่ <strong>~4.3µs เทียบกับ ~1.7µs ของ SQLite</strong> นี่คือพื้นที่สำหรับการเพิ่มประสิทธิภาพเนื่องจากการออกแบบ MVCC ของเราเพิ่มค่าใช้จ่ายสำหรับการบำรุงรักษาดัชนีที่เรากำลังทำงานเพื่อลด
bench-sysbench-emb-disc-improve-title = พื้นที่สำหรับการปรับปรุง
bench-sysbench-emb-disc-bulk = เส้นทางการแทรกแบตช์ของ SQLite ได้รับการเพิ่มประสิทธิภาพสูง; เรากำลังเพิ่มการดำเนินการ B-tree แบบแบตช์
bench-sysbench-emb-disc-nonindex = การอัปเดตที่ไม่มีดัชนีแสดง VibeSQL ที่ ~1.9µs เทียบกับ SQLite ~1.4µs - ใกล้เคียงกัน
bench-sysbench-emb-disc-deletes = การดำเนินการลบปรับปรุงอย่างมาก: ตอนนี้ ~5.5µs เทียบกับ SQLite ~3.8µs (เดิม 1183µs)
bench-sysbench-emb-disc-duckdb-title = DuckDB Comparison
bench-sysbench-emb-disc-duckdb = DuckDB is optimized for analytical workloads, not micro-operations. Its 100-1000x slower results here reflect architectural choices (columnar storage, vectorized execution) that trade single-row latency for bulk throughput. VibeSQL targets both use cases.
bench-sysbench-emb-disc-architecture-title = การแลกเปลี่ยนทางสถาปัตยกรรม
bench-sysbench-emb-disc-architecture = สถาปัตยกรรมไฮบริดของ VibeSQL มุ่งเป้าไปที่ภาระงานทั้ง OLTP และ OLAP การจัดเก็บ B-tree ของเราให้ประสิทธิภาพการค้นหาจุดที่แข่งขันกับ SQLite ในขณะที่การดำเนินการแบบคอลัมน์จัดการการสืบค้นเชิงวิเคราะห์อย่างมีประสิทธิภาพ

# Sysbench Server specific
bench-sysbench-server-name = Sysbench (Server)
bench-sysbench-server-title = Sysbench Micro-Benchmarks (Server)
bench-sysbench-server-description = เบนช์มาร์กเซิร์ฟเวอร์ <strong>Sysbench</strong> เปรียบเทียบ VibeSQL Server (โปรโตคอลสาย PostgreSQL) กับ MySQL โดยวัดประสิทธิภาพสำหรับการติดตั้งฐานข้อมูลหลายไคลเอนต์
bench-sysbench-server-ops-label = Sysbench operations
bench-sysbench-server-note = โหมดเซิร์ฟเวอร์ใช้โปรโตคอลสาย PostgreSQL ทำให้สามารถเข้าถึงหลายไคลเอนต์และความเข้ากันได้กับเครื่องมือและไดรเวอร์ PostgreSQL ที่มีอยู่

# Sysbench Server Discussion
bench-sysbench-srv-disc-protocol-title = โปรโตคอลสาย PostgreSQL
bench-sysbench-srv-disc-protocol = VibeSQL Server ใช้โปรโตคอลสาย PostgreSQL ทำให้เข้ากันได้กับไดรเวอร์และเครื่องมือ PostgreSQL ที่มีอยู่ สิ่งนี้เพิ่มค่าใช้จ่ายโปรโตคอล ~10-50µs ต่อการสืบค้นเมื่อเทียบกับโหมดฝังตัว แต่เปิดใช้งานการติดตั้งหลายไคลเอนต์
bench-sysbench-srv-disc-mysql-title = การเปรียบเทียบ MySQL
bench-sysbench-srv-disc-mysql = เบนช์มาร์กเซิร์ฟเวอร์เปรียบเทียบกับ MySQL เพื่อประเมิน VibeSQL เป็นตัวแทนที่ดรอปอินสำหรับฐานข้อมูลไคลเอนต์-เซิร์ฟเวอร์แบบดั้งเดิม ผลลัพธ์แตกต่างกันตามประเภทการดำเนินการ โดย VibeSQL แสดงข้อได้เปรียบในภาระงานที่อ่านมาก
bench-sysbench-srv-disc-roadmap-title = แผนงานเซิร์ฟเวอร์
bench-sysbench-srv-disc-pooling = ลดค่าใช้จ่ายการสร้างการเชื่อมต่อสำหรับสถานการณ์ทรูพุตสูง
bench-sysbench-srv-disc-caching = การแคชแผนการสืบค้นฝั่งเซิร์ฟเวอร์ข้ามการเชื่อมต่อ
bench-sysbench-srv-disc-extended = การสนับสนุนโปรโตคอลการสืบค้นขยายเต็มรูปแบบของ PostgreSQL สำหรับการดำเนินการแบตช์

# TPC-H Server เฉพาะ
bench-tpch-server-name = TPC-H (เซิร์ฟเวอร์)
bench-tpch-server-title = เบนช์มาร์กวิเคราะห์ TPC-H (เซิร์ฟเวอร์)
bench-tpch-server-description = <strong>เบนช์มาร์กเซิร์ฟเวอร์ TPC-H</strong> เปรียบเทียบ VibeSQL Server (โปรโตคอล PostgreSQL) กับ MySQL สำหรับเวิร์กโหลดการสืบค้นวิเคราะห์ วัดประสิทธิภาพ OLAP ในการใช้งานไคลเอนต์-เซิร์ฟเวอร์
bench-tpch-server-ops-label = การสืบค้น TPC-H
bench-tpch-server-note-intro = เบนช์มาร์กเซิร์ฟเวอร์ทดสอบการใช้งาน <strong>โปรโตคอล PostgreSQL</strong> วัดความล่าช้าของการสืบค้นแบบ end-to-end รวมถึง overhead ของเครือข่าย
bench-tpch-server-note-queries = การสืบค้นทดสอบ JOIN ที่ซับซ้อน ซับเควรี และการรวมข้อมูลที่เป็นลักษณะเฉพาะของเวิร์กโหลด business intelligence

# การอภิปราย TPC-H Server
bench-tpch-srv-disc-protocol-title = โปรโตคอล PostgreSQL
bench-tpch-srv-disc-protocol = VibeSQL Server พูดโปรโตคอล PostgreSQL ช่วยให้ใช้ไดรเวอร์และเครื่องมือ PostgreSQL มาตรฐานได้ เบนช์มาร์กนี้วัดความล่าช้าแบบ end-to-end เต็มรูปแบบรวมถึง overhead ของโปรโตคอล
bench-tpch-srv-disc-comparison-title = การเปรียบเทียบกับ MySQL
bench-tpch-srv-disc-comparison = การเปรียบเทียบกับ MySQL ให้เส้นฐานสำหรับฐานข้อมูลไคลเอนต์-เซิร์ฟเวอร์แบบดั้งเดิมบนเวิร์กโหลดวิเคราะห์ เอนจินการทำงานแบบคอลัมน์ของ VibeSQL ให้ข้อได้เปรียบสำหรับการรวมข้อมูลและการ join ที่ซับซ้อน
bench-tpch-srv-disc-roadmap-title = แผนงานเซิร์ฟเวอร์ OLAP
bench-tpch-srv-disc-prepared = นำแผนการสืบค้นที่คอมไพล์แล้วมาใช้ซ้ำข้ามการเชื่อมต่อ
bench-tpch-srv-disc-pooling = การจัดการการเชื่อมต่อที่มีประสิทธิภาพสำหรับสถานการณ์ทรูพุตสูง
bench-tpch-srv-disc-scale = การทดสอบชุดข้อมูลขนาดใหญ่ (SF 0.1, SF 1.0) สำหรับการตรวจสอบระดับการผลิต

# TPC-C Server เฉพาะ
bench-tpcc-server-name = TPC-C (เซิร์ฟเวอร์)
bench-tpcc-server-title = เบนช์มาร์ก OLTP TPC-C (เซิร์ฟเวอร์)
bench-tpcc-server-description = <strong>เบนช์มาร์กเซิร์ฟเวอร์ TPC-C</strong> เปรียบเทียบ VibeSQL Server (โปรโตคอล PostgreSQL) กับ MySQL สำหรับเวิร์กโหลดธุรกรรม OLTP วัดทรูพุตสำหรับการใช้งานฐานข้อมูลหลายไคลเอนต์
bench-tpcc-server-ops-label = ธุรกรรม TPC-C
bench-tpcc-server-note-intro = เบนช์มาร์กเซิร์ฟเวอร์ทดสอบการใช้งาน <strong>โปรโตคอล PostgreSQL</strong> วัดทรูพุตธุรกรรมรวมถึง overhead ของเครือข่าย
bench-tpcc-server-note-results = ผลลัพธ์รายงานธุรกรรมต่อวินาที (TPS) สำหรับมิกซ์ธุรกรรม TPC-C มาตรฐาน
bench-tpcc-mixed = เวิร์กโหลดผสม - มิกซ์ธุรกรรม TPC-C มาตรฐาน (45% คำสั่งซื้อใหม่, 43% การชำระเงิน, 4% สถานะคำสั่ง, 4% การจัดส่ง, 4% ระดับสต็อก)

# การอภิปราย TPC-C Server
bench-tpcc-srv-disc-protocol-title = โปรโตคอล PostgreSQL
bench-tpcc-srv-disc-protocol = VibeSQL Server พูดโปรโตคอล PostgreSQL ช่วยให้ใช้ไดรเวอร์และเครื่องมือ PostgreSQL มาตรฐานได้ เบนช์มาร์กนี้วัดความล่าช้าธุรกรรมแบบ end-to-end เต็มรูปแบบรวมถึง overhead ของโปรโตคอล
bench-tpcc-srv-disc-comparison-title = การเปรียบเทียบกับ MySQL
bench-tpcc-srv-disc-comparison = การเปรียบเทียบกับ MySQL ให้เส้นฐานสำหรับฐานข้อมูลไคลเอนต์-เซิร์ฟเวอร์แบบดั้งเดิมบนเวิร์กโหลด OLTP MySQL เป็นมาตรฐานอุตสาหกรรมสำหรับเวิร์กโหลดธุรกรรม และ TPC-C เป็นจุดแข็งของ MySQL
bench-tpcc-srv-disc-roadmap-title = แผนงานเซิร์ฟเวอร์ OLTP
bench-tpcc-srv-disc-prepared = นำแผนการสืบค้นที่คอมไพล์แล้วมาใช้ซ้ำข้ามการเชื่อมต่อ
bench-tpcc-srv-disc-pooling = การจัดการการเชื่อมต่อที่มีประสิทธิภาพสำหรับสถานการณ์ทรูพุตสูง
bench-tpcc-srv-disc-parallel = การประมวลผลธุรกรรมพร้อมกันหลายไคลเอนต์

# Footprint Embedded เฉพาะ
bench-footprint-embedded-name = Footprint (Embedded)
bench-footprint-embedded-title = Native Binary Footprint
bench-footprint-embedded-description = <strong>เบนช์มาร์กฟุตพริ้นท์แบบฝังตัว</strong> วัดประสิทธิภาพทรัพยากรของไบนารีฐานข้อมูลเนทีฟ เปรียบเทียบขนาดไบนารี เวลาเริ่มต้นเย็น และการใช้หน่วยความจำสูงสุด
bench-footprint-embedded-ops-label = databases compared
bench-footprint-embedded-note = ฟุตพริ้นท์ไบนารีเนทีฟมีความสำคัญอย่างยิ่งสำหรับ <strong>การติดตั้งแบบฝังตัวและขอบ</strong> ที่ขนาดไบนารี เวลาแฝงการเริ่มต้น และการใช้หน่วยความจำส่งผลโดยตรงต่อความเป็นไปได้ในการติดตั้ง

# Footprint Embedded Descriptions
bench-footprint-binary-size = Binary Size - Size of the compiled database binary on disk
bench-footprint-startup-time = Startup Time - Time to cold-start and execute first query
bench-footprint-peak-memory = Peak Memory - Maximum resident set size during initialization

# Footprint Embedded Discussion
bench-footprint-emb-disc-size-title = ขนาดไบนารี: ทางสายกลาง
bench-footprint-emb-disc-size = VibeSQL ที่ <strong>~17MB</strong> อยู่ระหว่าง SQLite (~5MB) และ DuckDB (~45MB) สิ่งนี้สะท้อนถึงการเลือกของเราที่จะรวมคุณสมบัติขั้นสูง (ฟังก์ชันหน้าต่าง, CTE, การดำเนินการแบบคอลัมน์) ในขณะที่รักษาไบนารีให้จัดการได้สำหรับการติดตั้งแบบฝังตัว
bench-footprint-emb-disc-startup-title = การเริ่มต้น: การเริ่มต้นเย็นที่เร็วที่สุด
bench-footprint-emb-disc-startup = VibeSQL บรรลุ <strong>การเริ่มต้นเย็น ~6ms</strong> เร็วกว่า SQLite (~6.5ms) และเร็วกว่า DuckDB (~13ms) อย่างมาก เส้นทางการเริ่มต้นขั้นต่ำของเราโหลดเฉพาะโครงสร้างเมตาดาต้าที่จำเป็นเมื่อเริ่มต้น
bench-footprint-emb-disc-memory-title = ประสิทธิภาพหน่วยความจำ
bench-footprint-emb-disc-memory = หน่วยความจำสูงสุดระหว่างการเริ่มต้นคือ ~7MB สำหรับ VibeSQL เทียบกับ ~3MB สำหรับ SQLite และ ~11MB สำหรับ DuckDB ความแตกต่างจาก SQLite สะท้อนถึงตัวเพิ่มประสิทธิภาพการสืบค้นที่ซับซ้อนกว่าและโครงสร้างพื้นฐานการดำเนินการแบบคอลัมน์ที่จัดสรรล่วงหน้าของเรา
bench-footprint-emb-disc-roadmap-title = แผนงานการลดขนาด
bench-footprint-emb-disc-flags = การเลือกคุณสมบัติเวลาคอมไพล์เพื่อยกเว้นฟังก์ชันการทำงานที่ไม่ได้ใช้
bench-footprint-emb-disc-lto = การเพิ่มประสิทธิภาพเวลาลิงก์ของโปรแกรมทั้งหมดสำหรับการกำจัดโค้ดที่ตาย
bench-footprint-emb-disc-modular = แยกเครื่องยนต์หลักออกจากคุณสมบัติเสริม (เช่น ฟังก์ชันหน้าต่าง)

# Footprint Server/WASM specific
bench-footprint-server-name = Footprint (Server/WASM)
bench-footprint-server-title = WASM Footprint
bench-footprint-server-description = <strong>เบนช์มาร์กฟุตพริ้นท์ WASM</strong> วัดขนาดโมดูล WebAssembly สำหรับการติดตั้งเบราว์เซอร์ มีความสำคัญสำหรับเว็บแอปพลิเคชันที่ขนาดดาวน์โหลดส่งผลต่อประสบการณ์ผู้ใช้
bench-footprint-server-ops-label = deployment targets
bench-footprint-server-note = ขนาด WASM มีความสำคัญสำหรับ <strong>การติดตั้งเว็บ</strong> ที่เวลาดาวน์โหลดส่งผลโดยตรงต่อเวลาจนถึงการโต้ตอบ ขนาด Gzip เกี่ยวข้องมากที่สุดเนื่องจากเบราว์เซอร์จะคลายการบีบอัดเนื้อหา gzip โดยอัตโนมัติ
bench-footprint-server-note2 = <strong>หมายเหตุ:</strong> VibeSQL WASM ได้รับการออกแบบสำหรับขนาดดาวน์โหลดขั้นต่ำในขณะที่รักษาความสอดคล้อง SQL:1999 เต็มรูปแบบในเบราว์เซอร์

# Footprint Server Descriptions
bench-footprint-wasm-size = WASM Size - Size of the WebAssembly module for browser deployment
bench-footprint-wasm-gzip = WASM (gzip) - Compressed size for web delivery

# Footprint Server Discussion
bench-footprint-srv-disc-wasm-title = WASM: 1.5MB บีบอัด
bench-footprint-srv-disc-wasm = โมดูล WebAssembly ของ VibeSQL บีบอัดเป็น <strong>~1.5MB gzipped</strong> ทำให้สามารถโหลดหน้าเริ่มต้นได้อย่างรวดเร็ว นี่คือฐานข้อมูล SQL:1999 เต็มรูปแบบพร้อมฟังก์ชันหน้าต่าง, CTE และธุรกรรม ACID ที่ทำงานทั้งหมดในเบราว์เซอร์
bench-footprint-srv-disc-included-title = สิ่งที่รวมอยู่
bench-footprint-srv-disc-parser = ตัวแยกวิเคราะห์ SQL และตัวเพิ่มประสิทธิภาพการสืบค้นที่สมบูรณ์
bench-footprint-srv-disc-btree = เครื่องยนต์จัดเก็บ B-tree พร้อม MVCC
bench-footprint-srv-disc-window = ฟังก์ชันหน้าต่างและการรวมขั้นสูง
bench-footprint-srv-disc-cte = นิพจน์ตารางทั่วไป (คำสั่ง WITH)
bench-footprint-srv-disc-acid = การสนับสนุนธุรกรรม ACID เต็มรูปแบบ
bench-footprint-srv-disc-benefits-title = ประโยชน์ของการติดตั้งเบราว์เซอร์
bench-footprint-srv-disc-benefits = การรัน SQL ในเบราว์เซอร์ขจัดเวลาแฝงไปกลับไปยังเซิร์ฟเวอร์ เปิดใช้งานแอปพลิเคชันออฟไลน์เป็นอันดับแรก และเก็บข้อมูลที่ละเอียดอ่อนไว้บนอุปกรณ์ของผู้ใช้ การสร้าง WASM ของ VibeSQL ได้รับการออกแบบสำหรับกรณีการใช้งานนี้ด้วยการพึ่งพาขั้นต่ำและการใช้หน่วยความจำที่มีประสิทธิภาพ
bench-footprint-srv-disc-roadmap-title = แผนงาน WASM
bench-footprint-srv-disc-streaming = เริ่มดำเนินการในขณะที่โมดูลกำลังดาวน์โหลด
bench-footprint-srv-disc-indexeddb = การจัดเก็บถาวรข้ามเซสชันเบราว์เซอร์
bench-footprint-srv-disc-worker = รันการสืบค้นนอกเธรดหลักสำหรับ UI ที่ตอบสนอง

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
bench-tpcc-disc-duckdb = DuckDB บรรลุเพียง ~385 TPS บน TPC-C (ช้ากว่า VibeSQL 60 เท่า ช้ากว่า SQLite 12 เท่า) นี่เป็นสิ่งที่คาดหวัง: DuckDB เป็น <strong>ฐานข้อมูลเชิงวิเคราะห์ (OLAP)</strong> ที่เพิ่มประสิทธิภาพสำหรับการดำเนินการแบตช์ขนาดใหญ่ ไม่ใช่ธุรกรรมแถวเดียว รูปแบบการจัดเก็บแบบคอลัมน์โดดเด่นในการสแกนหลายล้านแถว แต่เพิ่มค่าใช้จ่ายสำหรับการค้นหาจุดและการอัปเดตขนาดเล็กที่ครอบงำภาระงาน OLTP เช่น TPC-C
bench-tpcc-disc-duckdb-title = เหตุใด DuckDB จึงตามหลังใน OLTP
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
conformance-pgsql-title = PostgreSQL Regression Tests
conformance-pgsql-desc = Results from running <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">PostgreSQL's regression test suite</a> - the canonical test suite used to validate PostgreSQL compatibility.
conformance-pgsql-tests-passing = tests passing
conformance-pgsql-tests-excluded = tests excluded
conformance-pgsql-pass-rate = Pass Rate
conformance-pgsql-excluded-reason = Excluded tests use PostgreSQL-specific features not applicable to VibeSQL
conformance-pgsql-note = <strong>Note:</strong> PostgreSQL regression tests validate SQL behavior against PostgreSQL's reference implementation. Excluded tests involve PostgreSQL-specific features like system catalogs, procedural languages, or extension modules.
