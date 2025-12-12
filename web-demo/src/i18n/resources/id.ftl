# VibeSQL Web UI - Bahasa Indonesia

# Page titles
page-title = VibeSQL - Database SQL:1999 Bertenaga AI
demo-title = Demo VibeSQL
benchmarks-title = Benchmark Kinerja - VibeSQL
benchmarks-heading = VibeSQL - Benchmark Kinerja
conformance-title = Laporan Kesesuaian - VibeSQL
conformance-heading = Laporan Kesesuaian
conformance-subtitle = Pengujian Kepatuhan Standar SQL:1999

# Navigation
nav-showcase = Showcase SQL:1999
nav-conformance = Lihat Hasil sqltest
nav-sqllogictest = Lihat Hasil SQLLogicTest

# Editor section
editor-title = Editor SQL
editor-storage = Penyimpanan
editor-storage-init = Menginisialisasi...
editor-execute = Jalankan Query

# Results section
results-title = Hasil
results-empty = Jalankan query untuk melihat hasil
results-loading = Memuat...
results-rows = { $count } baris
results-rows-with-time = { $count } baris ({ $time }ms)
results-copy = Salin ke clipboard
results-export = Ekspor CSV
results-limit-warning = Menampilkan { $limit } baris pertama dari { $total }. Gunakan LIMIT untuk menyaring query Anda.

# Examples sidebar
examples-title = Contoh
examples-basic = Query Dasar
examples-advanced = Query Lanjutan

# Database selector
db-select-label = Database

# Footer
footer-tagline = VibeSQL - Database SQL:1999 di WebAssembly
footer-deployed = Diterapkan: { $date }

# Theme
theme-toggle-dark = Beralih ke mode gelap
theme-toggle-light = Beralih ke mode terang

# Locale
locale-select = Pilih bahasa

# Messages
msg-query-success = Query berhasil dijalankan
msg-rows-affected = { $count } baris terpengaruh

# Errors
error-generic = Terjadi kesalahan
error-query-failed = Query gagal
error-no-databases = Tidak ada database tersedia

# Loading states
loading-initializing-theme = Menginisialisasi tema
loading-preparing-editor = Menyiapkan editor
loading-database-engine = Memuat mesin database
loading-setting-up-ui = Menyiapkan antarmuka pengguna
loading-editor = Memuat editor...
loading-compliance-data = Memuat data kepatuhan...
loading-conformance-report = Memuat laporan kesesuaian...

# Editor
editor-placeholder = Masukkan query SQL di sini... (Ctrl+Enter atau Cmd+Enter untuk menjalankan)

# Navigation links
nav-terminal = Demo Terminal SQL
nav-compliance = Laporan Kepatuhan Uji SQL
nav-benchmarks = Benchmark Kinerja
nav-github = Repositori GitHub
nav-home = Beranda

# Results
results-success-zero = Query berhasil dijalankan (0 baris)
results-null = NULL

# Help Modal
help-title = Pintasan Keyboard & Bantuan
help-close = Tutup
help-editor-shortcuts = Pintasan Editor
help-navigation = Navigasi
help-results-actions = Aksi Hasil
help-tips = Tips
help-shortcut-execute = Jalankan query saat ini
help-shortcut-comment = Aktifkan/nonaktifkan komentar baris
help-shortcut-indent = Indentasi pilihan
help-shortcut-show-help = Tampilkan dialog bantuan ini
help-shortcut-close-help = Tutup dialog bantuan
help-action-copy = Salin ke clipboard
help-action-copy-desc = Salin hasil sebagai nilai yang dipisahkan tab
help-action-export = Ekspor CSV
help-action-export-desc = Unduh hasil sebagai file CSV
help-tip-limit = Hasil dibatasi hingga 1.000 baris untuk kinerja. Gunakan LIMIT untuk menyaring query.
help-tip-time = Waktu eksekusi ditampilkan dengan hasil query.
help-tip-syntax = Editor mendukung penyorotan sintaks SQL dan pelengkapan otomatis.
help-tip-theme = Beralih antara mode terang/gelap menggunakan tombol tema.
help-got-it = Mengerti!

# Showcase Navigation
showcase-title = Showcase SQL:1999 Core
showcase-description = Jelajahi fitur SQL:1999 Core yang diimplementasikan secara interaktif
showcase-complete = { $percent }% Selesai
showcase-categories = Kategori Fitur
showcase-legend = Legenda Status
showcase-status-implemented = Sepenuhnya Diimplementasikan
showcase-status-partial = Diimplementasikan Sebagian
showcase-status-planned = Direncanakan

# Showcase category labels
showcase-cat-compliance = Dasbor Kesesuaian
showcase-cat-data-types = Tipe Data
showcase-cat-dml = Operasi DML
showcase-cat-predicates = Predikat & Operator
showcase-cat-joins = JOIN
showcase-cat-subqueries = Subquery
showcase-cat-aggregates = Agregat & GROUP BY
showcase-cat-ddl = DDL & Batasan

# Common showcase elements
showcase-interactive-examples = Contoh Interaktif
showcase-try-example = Coba Contoh Ini
showcase-progress = { $implemented } dari { $total } { $type } ({ $percent }%)
showcase-table-status = Status
showcase-table-category = Kategori
showcase-table-description = Deskripsi
showcase-table-syntax = Sintaks
showcase-table-use-case = Kasus Penggunaan

# Status labels
status-implemented = Diimplementasikan
status-partial = Sebagian
status-planned = Direncanakan

# Aggregates Showcase
aggregates-title = Agregat SQL dan GROUP BY
aggregates-description = Fungsi agregat SQL:1999 Core dan kemampuan pengelompokan
aggregates-reference = Referensi Fungsi Agregat
aggregates-table-function = Fungsi
aggregates-progress-type = fungsi
aggregates-ex-basic = Fungsi Agregat Dasar
aggregates-ex-group-single = GROUP BY (Kolom Tunggal)
aggregates-ex-group-multiple = GROUP BY (Beberapa Kolom)
aggregates-ex-having = Klausa HAVING
aggregates-ex-orderby = ORDER BY dengan Agregat
aggregates-ex-null = Penanganan NULL dalam Agregat

# DML Operations Showcase
dml-title = Operasi DML (Bahasa Manipulasi Data)
dml-description = Operasi SQL:1999 Core untuk query dan memodifikasi data
dml-reference = Referensi Operasi DML
dml-table-operation = Operasi
dml-progress-type = operasi
dml-ex-select-basic = SELECT - Query Dasar
dml-ex-select-ordering = SELECT - Pengurutan dan Pembatasan
dml-ex-insert = Operasi INSERT
dml-ex-update = Operasi UPDATE
dml-ex-delete = Operasi DELETE
dml-ex-combined = Alur Kerja CRUD Gabungan

# Data Types Showcase
datatypes-title = Tipe Data SQL:1999 Core
datatypes-description = Jelajahi tipe data fundamental yang didefinisikan dalam spesifikasi SQL:1999 Core
datatypes-reference = Referensi Tipe Data
datatypes-table-type = Nama Tipe
datatypes-table-example = Nilai Contoh
datatypes-table-spec = Spesifikasi
datatypes-progress-type = tipe
datatypes-ex-numeric = Bekerja dengan Tipe Numerik
datatypes-ex-null = Penanganan NULL & Logika Tiga Nilai
datatypes-ex-comparisons = Perbandingan & Operasi Tipe

# JOINs Showcase
joins-title = JOIN SQL
joins-description = Operasi JOIN SQL:1999 Core untuk menggabungkan data dari beberapa tabel
joins-reference = Referensi Tipe JOIN
joins-table-type = Tipe JOIN
joins-progress-type = tipe JOIN
joins-category-suffix = JOIN
joins-ex-sample = Pengaturan Data Sampel
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = JOIN Multi-tabel

# Predicates Showcase
predicates-title = Predikat dan Operator
predicates-description = Predikat SQL:1999 untuk penyaringan dan operasi logis
predicates-reference = Referensi Predikat
predicates-table-predicate = Predikat
predicates-progress-type = predikat
predicates-ex-comparison = Operator Perbandingan
predicates-ex-between = BETWEEN dan Predikat Rentang
predicates-ex-null = Predikat NULL dan Logika Tiga Nilai
predicates-ex-boolean = Logika Boolean (AND, OR, NOT)
predicates-ex-in = Predikat IN dengan Subquery
predicates-ex-combined = Operasi Predikat Gabungan

# Subqueries Showcase
subqueries-title = Subquery SQL
subqueries-description = Kemampuan subquery SQL:1999 Core untuk operasi query bersarang
subqueries-reference = Referensi Tipe Subquery
subqueries-table-type = Tipe Subquery
subqueries-progress-type = tipe subquery
subqueries-ex-scalar-select = Subquery Skalar dalam SELECT
subqueries-ex-scalar-where = Subquery Skalar dalam WHERE
subqueries-ex-derived = Tabel Turunan (Subquery dalam FROM)
subqueries-ex-in = Predikat IN dengan Subquery
subqueries-ex-correlated = Subquery Berkorelasi
subqueries-ex-nested = Subquery Bersarang

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
bench-no-wasm-data = Data WASM tidak tersedia
bench-no-server-data = Data benchmark server Sysbench tidak tersedia
bench-no-server-data-hint = Benchmark server memerlukan menjalankan sysbench_server dengan perbandingan MySQL diaktifkan.

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
bench-tpch-description = Benchmark ini menggunakan <strong>suite benchmark TPC-H</strong> standar industri, yang mensimulasikan beban kerja dukungan keputusan dunia nyata dengan kueri analitis kompleks yang melibatkan agregasi, join, subkueri, dan pengurutan.
bench-tpch-ops-label = TPC-H queries
bench-tpch-note-intro = Semua benchmark mengukur waktu eksekusi kueri end-to-end termasuk parsing, perencanaan, eksekusi, dan materialisasi hasil. Ini mewakili <strong>kinerja mesin SQL dunia nyata</strong> untuk beban kerja analitis.
bench-tpch-note-queries = <strong>Catatan:</strong> Kueri TPC-H menguji berbagai aspek kinerja SQL: agregasi sederhana (Q1, Q6), join kompleks (Q2-Q5, Q7-Q10), subkueri (Q11-Q15), dan analitik lanjutan (Q16-Q22). Arahkan kursor ke nama kueri di tabel di atas untuk deskripsi.

# TPC-H Discussion
bench-tpch-disc-excels-title = Di Mana VibeSQL Unggul
bench-tpch-disc-excels = VibeSQL menunjukkan kinerja kuat pada <strong>kueri agregasi scan-heavy</strong> (Q1, Q6, Q14, Q15, Q20) di mana mesin eksekusi kolumnar dan agregasi yang dipercepat SIMD kami bersinar. Kueri ini melibatkan pemfilteran tabel besar dan penghitungan agregat tanpa pola join yang kompleks.
bench-tpch-disc-targets-title = Target Optimisasi Saat Ini
bench-tpch-disc-targets = Kueri join multi-arah (Q3, Q5, Q7-Q10, Q18, Q19, Q21) saat ini menunjukkan SQLite lebih unggul. Hambatan utama adalah implementasi hash join kami, yang belum menggunakan tingkat optimisasi yang sama seperti join B-tree SQLite yang telah disempurnakan selama puluhan tahun. Area spesifik dalam pengembangan aktif:
bench-tpch-disc-join-ordering = Estimasi kardinalitas yang ditingkatkan untuk pemilihan urutan join yang lebih baik
bench-tpch-disc-hash-sizing = Pertumbuhan tabel hash adaptif dan spill-to-disk untuk join besar
bench-tpch-disc-vectorized = Pemrosesan batch dalam loop dalam join untuk meningkatkan pemanfaatan cache
bench-tpch-disc-inl-joins = Memanfaatkan indeks B-tree ketika menguntungkan
bench-tpch-disc-path-title = Jalan Menuju Kepemimpinan
bench-tpch-disc-path = Arsitektur VibeSQL dirancang untuk perangkat keras modern dengan fitur seperti penyimpanan kolumnar, eksekusi vektor, dan konkurensi bebas kunci. Seiring optimisasi ini matang, kami mengharapkan VibeSQL mencapai kepemimpinan konsisten di semua kueri TPC-H.

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
bench-tpcds-description = <strong>TPC-DS</strong> adalah penerus TPC-H, menampilkan 99 kueri yang memodelkan sistem dukungan keputusan modern dengan pola kueri yang jauh lebih kompleks termasuk beberapa tabel fakta, skema snow-flake, dan fitur SQL lanjutan.
bench-tpcds-ops-label = TPC-DS queries
bench-tpcds-note-intro = Kueri TPC-DS jauh lebih kompleks daripada TPC-H, menguji fitur SQL lanjutan seperti fungsi window, ekspresi tabel umum (klausa WITH), dan pola join kompleks di beberapa tabel fakta dan dimensi.
bench-tpcds-note-remaining = <strong>Catatan:</strong> Semua 99 kueri TPC-DS lulus, menunjukkan dukungan fitur SQL:1999 yang komprehensif termasuk INTERSECT, EXCEPT, fungsi window, CTE, dan subkueri kompleks.

# TPC-DS Discussion
bench-tpcds-disc-coverage-title = Cakupan Fitur SQL:1999
bench-tpcds-disc-coverage = TPC-DS menguji fitur SQL yang paling menuntut. VibeSQL lulus <strong>semua 99 kueri</strong>, menunjukkan cakupan lengkap SQL:1999 termasuk ROLLUP, CUBE, GROUPING(), fungsi window dengan framing kompleks, CTE rekursif, dan operasi set INTERSECT/EXCEPT.
bench-tpcds-disc-optimization-title = Optimisasi Kueri Kompleks
bench-tpcds-disc-optimization = Kueri TPC-DS sering menggabungkan 10+ tabel dengan subkueri terkorelasi. Area fokus saat ini:
bench-tpcds-disc-cte = Keputusan cerdas antara CTE yang dimaterialisasi dan inline
bench-tpcds-disc-decorrelation = Mengonversi subkueri terkorelasi ke join ketika menguntungkan
bench-tpcds-disc-star = Pengurutan join fakta-dimensi untuk pola analitis
bench-tpcds-disc-toward-title = Cakupan TPC-DS Lengkap
bench-tpcds-disc-toward = Dengan semua 99 kueri lulus, VibeSQL menunjukkan kepatuhan SQL:1999 yang siap produksi untuk beban kerja analitis kompleks. Penambahan terbaru operasi set INTERSECT dan EXCEPT menyelesaikan cakupan TPC-DS penuh.
bench-tpcds-disc-sqlite-title = Catatan Perbandingan SQLite
bench-tpcds-disc-sqlite = SQLite tidak dapat mengeksekusi 12 dari 99 kueri TPC-DS (Q2, Q5, Q14, Q17, Q18, Q22, Q36, Q67, Q70, Q77, Q80, Q86) karena fitur OLAP SQL:1999 yang hilang: set pengelompokan <strong>ROLLUP/CUBE</strong>, fungsi <strong>GROUPING()</strong>, dan <strong>STDDEV_SAMP()</strong>. Kueri ini dilewati dalam benchmark SQLite. VibeSQL dan DuckDB mendukung semua 99 kueri.

# TPC-C specific
bench-tpcc-name = TPC-C
bench-tpcc-title = TPC-C Online Transaction Processing Benchmark
bench-tpcc-description = <strong>Benchmark TPC-C</strong> mensimulasikan lingkungan entri pesanan lengkap dengan campuran transaksi kompleks termasuk entri pesanan, pemrosesan pembayaran, kueri status pesanan, pemrosesan pengiriman, dan pemantauan tingkat stok.
bench-tpcc-ops-label = TPC-C transactions
bench-tpcc-note-intro = TPC-C mengukur transaksi per menit (tpmC) dan menguji kemampuan database untuk menangani transaksi bersamaan dengan logika bisnis kompleks. Benchmark ini sangat penting untuk mengevaluasi <strong>kinerja beban kerja transaksional</strong>.
bench-tpcc-note-results = <strong>Catatan:</strong> Hasil menunjukkan latensi transaksi rata-rata. Lebih rendah lebih baik. TPC-C sangat menuntut untuk beban kerja write-heavy dengan persyaratan konsistensi yang ketat.

# TPC-C Transaction Descriptions
bench-tpcc-new-order = New Order - Complex transaction with inventory checks and order creation
bench-tpcc-payment = Payment - Update customer balance and warehouse/district totals
bench-tpcc-order-status = Order Status - Read-only query for customer order history
bench-tpcc-delivery = Delivery - Batch processing of pending orders
bench-tpcc-stock-level = Stock Level - Count items below threshold in recent orders

# TPC-C Discussion
bench-tpcc-disc-faster-title = 5x Lebih Cepat dari SQLite
bench-tpcc-disc-faster = VibeSQL mencapai <strong>~23.000 transaksi per detik</strong> dibandingkan ~4.500 TPS SQLite, peningkatan 5x. Percepatan ini berasal dari arsitektur MVCC bebas kunci kami yang menghindari penguncian kasar SQLite pada setiap operasi tulis.
bench-tpcc-disc-dominates-title = Mengapa VibeSQL Mendominasi OLTP
bench-tpcc-disc-lockfree = MVCC memungkinkan pembaca dan penulis melanjutkan secara bersamaan tanpa pemblokiran
bench-tpcc-disc-optimistic = Transaksi hanya berkonflik pada waktu commit, bukan selama eksekusi
bench-tpcc-disc-btree = Struktur indeks yang dibangun khusus dan dioptimalkan untuk beban kerja dalam memori
bench-tpcc-disc-prepared = Rencana kueri dikompilasi sekali dan digunakan kembali
bench-tpcc-disc-scaling-title = Skalabilitas Lebih Lanjut
bench-tpcc-disc-scaling = Hasil saat ini adalah single-threaded. Arsitektur VibeSQL mendukung pemrosesan transaksi multi-threaded, dan kami mengharapkan skalabilitas yang lebih baik saat kami menambahkan dukungan eksekusi paralel.

# Sysbench Embedded specific
bench-sysbench-embedded-name = Sysbench (Embedded)
bench-sysbench-embedded-title = Sysbench Micro-Benchmarks (Embedded)
bench-sysbench-embedded-description = <strong>Sysbench</strong> menyediakan micro-benchmark terfokus yang mengisolasi operasi database tertentu. Tes ini mengukur kinerja mentah untuk operasi fundamental tanpa kompleksitas beban kerja transaksi penuh.
bench-sysbench-embedded-ops-label = Sysbench operations
bench-sysbench-embedded-note = Mode embedded menjalankan database dalam proses tanpa overhead jaringan, ideal untuk aplikasi proses tunggal di mana latensi minimal sangat penting.

# Sysbench Operation Descriptions
bench-sysbench-point-select = Point Select - Single row lookup by primary key
bench-sysbench-insert = Insert - Insert new rows into table
bench-sysbench-update-index = Update Index - Update indexed column (k = k + 1)
bench-sysbench-update-non-index = Update Non-Index - Update non-indexed column
bench-sysbench-delete = Delete - Remove rows by primary key
bench-sysbench-range-queries = Range Queries - Simple, SUM, ORDER BY, and DISTINCT range scans

# Sysbench Embedded Discussion
bench-sysbench-emb-disc-point-title = Pencarian Titik: Setara
bench-sysbench-emb-disc-point = Point select VibeSQL berjalan pada <strong>~0.37µs</strong>, menyamai SQLite ~0.36µs. Implementasi B-tree kami dioptimalkan untuk pencarian baris tunggal dengan pengejaran pointer minimal dan tata letak node yang ramah cache.
bench-sysbench-emb-disc-index-title = Pembaruan Indeks: Ruang untuk Perbaikan
bench-sysbench-emb-disc-index = Pembaruan terindeks VibeSQL berjalan pada <strong>~4.3µs vs ~1.7µs SQLite</strong>. Ini adalah area untuk optimisasi karena desain MVCC kami menambahkan overhead untuk pemeliharaan indeks yang sedang kami kerjakan untuk dikurangi.
bench-sysbench-emb-disc-improve-title = Area untuk Perbaikan
bench-sysbench-emb-disc-bulk = Jalur batch insert SQLite sangat dioptimalkan; kami menambahkan operasi B-tree batch
bench-sysbench-emb-disc-nonindex = Pembaruan non-indeks menunjukkan VibeSQL pada ~1.9µs vs SQLite ~1.4µs - mendekati paritas
bench-sysbench-emb-disc-deletes = Operasi hapus meningkat drastis: sekarang ~5.5µs vs SQLite ~3.8µs (sebelumnya 1183µs)
bench-sysbench-emb-disc-architecture-title = Trade-off Arsitektur
bench-sysbench-emb-disc-architecture = Arsitektur hybrid VibeSQL menargetkan beban kerja OLTP dan OLAP. Penyimpanan B-tree kami menyediakan kinerja pencarian titik yang kompetitif dengan SQLite, sementara eksekusi kolumnar menangani kueri analitis secara efisien.

# Sysbench Server specific
bench-sysbench-server-name = Sysbench (Server)
bench-sysbench-server-title = Sysbench Micro-Benchmarks (Server)
bench-sysbench-server-description = Benchmark <strong>Sysbench</strong> server membandingkan VibeSQL Server (protokol wire PostgreSQL) dengan MySQL, mengukur kinerja untuk deployment database multi-klien.
bench-sysbench-server-ops-label = Sysbench operations
bench-sysbench-server-note = Mode server menggunakan protokol wire PostgreSQL, memungkinkan akses multi-klien dan kompatibilitas dengan tooling dan driver PostgreSQL yang ada.

# Sysbench Server Discussion
bench-sysbench-srv-disc-protocol-title = Protokol Wire PostgreSQL
bench-sysbench-srv-disc-protocol = VibeSQL Server mengimplementasikan protokol wire PostgreSQL, memungkinkan kompatibilitas dengan driver dan tool PostgreSQL yang ada. Ini menambahkan ~10-50µs overhead protokol per kueri dibandingkan mode embedded, tetapi memungkinkan deployment multi-klien.
bench-sysbench-srv-disc-mysql-title = Perbandingan MySQL
bench-sysbench-srv-disc-mysql = Benchmark server membandingkan dengan MySQL untuk mengevaluasi VibeSQL sebagai pengganti drop-in untuk database client-server tradisional. VibeSQL Server mengungguli MySQL di semua operasi Sysbench, dengan percepatan dari <strong>2,4x</strong> (kueri range) hingga <strong>12,8x</strong> (update terindeks).
bench-sysbench-srv-disc-perf-title = Mengapa VibeSQL Server Lebih Cepat
bench-sysbench-srv-disc-perf-arch = Arsitektur VibeSQL berbeda secara fundamental dari desain RDBMS tradisional MySQL
bench-sysbench-srv-disc-perf-storage = VibeSQL menggunakan mesin penyimpanan kolumnar in-memory yang dioptimalkan untuk beban kerja analitik dan OLTP, menghindari overhead manajemen halaman InnoDB berbasis disk MySQL
bench-sysbench-srv-disc-perf-locking = Tidak ada penguncian berat tingkat baris atau pencatatan MVCC—VibeSQL menggunakan kontrol konkurensi ringan yang dirancang untuk CPU multi-core modern
bench-sysbench-srv-disc-perf-protocol = Implementasi protokol wire PostgreSQL yang efisien dengan overhead serialisasi minimal dibandingkan protokol MySQL
bench-sysbench-srv-disc-perf-writes = Operasi tulis (insert/update) menunjukkan keuntungan terbesar (<strong>8-12x</strong>) karena VibeSQL menghindari sinkronisasi redo log, undo log, dan doublewrite buffer MySQL
bench-sysbench-srv-disc-perf-reads = Operasi baca menunjukkan keuntungan lebih kecil tapi konsisten (<strong>2-3x</strong>) berkat pola akses kolumnar yang efisien cache dan nol I/O disk
bench-sysbench-srv-disc-roadmap-title = Roadmap Server
bench-sysbench-srv-disc-pooling = Mengurangi overhead pembentukan koneksi untuk skenario throughput tinggi
bench-sysbench-srv-disc-caching = Caching rencana kueri sisi server di seluruh koneksi
bench-sysbench-srv-disc-extended = Dukungan protokol kueri diperluas PostgreSQL penuh untuk operasi batch

# TPC-H Server specific
bench-tpch-server-name = TPC-H (Server)
bench-tpch-server-title = TPC-H Analytical Benchmark (Server)
bench-tpch-server-description = <strong>Benchmark server TPC-H</strong> membandingkan VibeSQL Server (protokol wire PostgreSQL) dengan MySQL untuk beban kerja kueri analitis, mengukur kinerja OLAP dalam deployment client-server.
bench-tpch-server-ops-label = Kueri TPC-H
bench-tpch-server-note-intro = Benchmark server menguji implementasi <strong>protokol wire PostgreSQL</strong>, mengukur latensi kueri end-to-end termasuk overhead jaringan.
bench-tpch-server-note-queries = Kueri menguji JOIN kompleks, subkueri, dan agregasi yang tipikal dari beban kerja business intelligence.

# TPC-H Server Discussion
bench-tpch-srv-disc-protocol-title = Protokol Wire PostgreSQL
bench-tpch-srv-disc-protocol = VibeSQL Server berbicara protokol wire PostgreSQL, memungkinkan penggunaan driver dan tool PostgreSQL standar. Benchmark ini mengukur latensi end-to-end penuh termasuk overhead protokol.
bench-tpch-srv-disc-comparison-title = Perbandingan MySQL
bench-tpch-srv-disc-comparison = Perbandingan dengan MySQL memberikan baseline untuk database client-server tradisional pada beban kerja analitis. Mesin eksekusi kolumnar VibeSQL memberikan keunggulan untuk agregasi dan join kompleks.
bench-tpch-srv-disc-roadmap-title = Roadmap Server OLAP
bench-tpch-srv-disc-prepared = Menggunakan kembali rencana kueri yang dikompilasi di seluruh koneksi
bench-tpch-srv-disc-pooling = Penanganan koneksi efisien untuk skenario throughput tinggi
bench-tpch-srv-disc-scale = Menguji dataset yang lebih besar (SF 0.1, SF 1.0) untuk validasi skala produksi

# TPC-C Server specific
bench-tpcc-server-name = TPC-C (Server)
bench-tpcc-server-title = TPC-C OLTP Benchmark (Server)
bench-tpcc-server-description = <strong>Benchmark server TPC-C</strong> membandingkan VibeSQL Server (protokol wire PostgreSQL) dengan MySQL untuk beban kerja transaksi OLTP, mengukur throughput untuk deployment database multi-klien.
bench-tpcc-server-ops-label = Transaksi TPC-C
bench-tpcc-server-note-intro = Benchmark server menguji implementasi <strong>protokol wire PostgreSQL</strong>, mengukur throughput transaksi termasuk overhead jaringan.
bench-tpcc-server-note-results = Hasil melaporkan transaksi per detik (TPS) untuk campuran transaksi TPC-C standar.
bench-tpcc-mixed = Beban Kerja Campuran - Campuran transaksi TPC-C standar (45% New-Order, 43% Payment, 4% Order-Status, 4% Delivery, 4% Stock-Level)

# TPC-C Server Discussion
bench-tpcc-srv-disc-protocol-title = Protokol Wire PostgreSQL
bench-tpcc-srv-disc-protocol = VibeSQL Server berbicara protokol wire PostgreSQL, memungkinkan penggunaan driver dan tool PostgreSQL standar. Benchmark ini mengukur latensi transaksi end-to-end penuh termasuk overhead protokol.
bench-tpcc-srv-disc-comparison-title = Perbandingan MySQL
bench-tpcc-srv-disc-comparison = Perbandingan dengan MySQL memberikan baseline untuk database client-server tradisional pada beban kerja OLTP. MySQL adalah standar industri untuk beban kerja transaksional, dan TPC-C adalah kekuatan MySQL.
bench-tpcc-srv-disc-roadmap-title = Roadmap Server OLTP
bench-tpcc-srv-disc-prepared = Menggunakan kembali rencana kueri yang dikompilasi di seluruh koneksi
bench-tpcc-srv-disc-pooling = Penanganan koneksi efisien untuk skenario throughput tinggi
bench-tpcc-srv-disc-parallel = Pemrosesan transaksi bersamaan multi-klien

# Footprint Embedded specific
bench-footprint-embedded-name = Footprint (Embedded)
bench-footprint-embedded-title = Native Binary Footprint
bench-footprint-embedded-description = <strong>Benchmark footprint embedded</strong> mengukur efisiensi sumber daya binary database native, membandingkan ukuran binary, waktu startup dingin, dan penggunaan memori puncak.
bench-footprint-embedded-ops-label = databases compared
bench-footprint-embedded-note = Footprint binary native sangat penting untuk <strong>deployment embedded dan edge</strong> di mana ukuran binary, latensi startup, dan konsumsi memori secara langsung memengaruhi kelayakan deployment.

# Footprint Embedded Descriptions
bench-footprint-binary-size = Binary Size - Size of the compiled database binary on disk
bench-footprint-startup-time = Startup Time - Time to cold-start and execute first query
bench-footprint-peak-memory = Peak Memory - Maximum resident set size during initialization

# Footprint Embedded Discussion
bench-footprint-emb-disc-size-title = Ukuran Binary: Jalan Tengah
bench-footprint-emb-disc-size = VibeSQL pada <strong>~17MB</strong> berada di antara SQLite (~5MB) dan DuckDB (~45MB). Ini mencerminkan pilihan kami untuk menyertakan fitur lanjutan (fungsi window, CTE, eksekusi kolumnar) sambil menjaga binary dapat dikelola untuk deployment embedded.
bench-footprint-emb-disc-startup-title = Startup: Cold Start Tercepat
bench-footprint-emb-disc-startup = VibeSQL mencapai <strong>cold startup ~6ms</strong>, lebih cepat dari SQLite (~6.5ms) dan jauh lebih cepat dari DuckDB (~13ms). Jalur inisialisasi minimal kami hanya memuat struktur metadata penting saat startup.
bench-footprint-emb-disc-memory-title = Efisiensi Memori
bench-footprint-emb-disc-memory = Memori puncak selama startup adalah ~7MB untuk VibeSQL vs ~3MB untuk SQLite dan ~11MB untuk DuckDB. Perbedaan dari SQLite mencerminkan optimizer kueri kami yang lebih canggih dan infrastruktur eksekusi kolumnar yang dialokasikan di awal.
bench-footprint-emb-disc-roadmap-title = Roadmap Pengurangan Ukuran
bench-footprint-emb-disc-flags = Pemilihan fitur waktu kompilasi untuk mengecualikan fungsionalitas yang tidak digunakan
bench-footprint-emb-disc-lto = Optimisasi link-time seluruh program untuk eliminasi kode mati
bench-footprint-emb-disc-modular = Memisahkan mesin inti dari fitur opsional (misalnya, fungsi window)

# Footprint Server/WASM specific
bench-footprint-server-name = Footprint (Server/WASM)
bench-footprint-server-title = WASM Footprint
bench-footprint-server-description = <strong>Benchmark footprint WASM</strong> mengukur ukuran modul WebAssembly untuk deployment browser, penting untuk aplikasi web di mana ukuran unduhan memengaruhi pengalaman pengguna.
bench-footprint-server-ops-label = deployment targets
bench-footprint-server-note = Ukuran WASM sangat penting untuk <strong>deployment web</strong> di mana waktu unduh secara langsung memengaruhi time-to-interactive. Ukuran Gzip paling relevan karena browser secara otomatis mendekompresi konten gzip.
bench-footprint-server-note2 = <strong>Catatan:</strong> VibeSQL WASM dirancang untuk ukuran unduhan minimal sambil mempertahankan kepatuhan SQL:1999 penuh di browser.

# Footprint Server Descriptions
bench-footprint-wasm-size = WASM Size - Size of the WebAssembly module for browser deployment
bench-footprint-wasm-gzip = WASM (gzip) - Compressed size for web delivery

# Footprint Server Discussion
bench-footprint-srv-disc-wasm-title = WASM: 1.5MB Terkompresi
bench-footprint-srv-disc-wasm = Modul WebAssembly VibeSQL dikompresi menjadi <strong>~1.5MB gzipped</strong>, memungkinkan pemuatan halaman awal yang cepat. Ini adalah database SQL:1999 lengkap dengan fungsi window, CTE, dan transaksi ACID yang berjalan sepenuhnya di browser.
bench-footprint-srv-disc-included-title = Yang Termasuk
bench-footprint-srv-disc-parser = Parser SQL lengkap dan optimizer kueri
bench-footprint-srv-disc-btree = Mesin penyimpanan B-tree dengan MVCC
bench-footprint-srv-disc-window = Fungsi window dan agregasi lanjutan
bench-footprint-srv-disc-cte = Ekspresi tabel umum (klausa WITH)
bench-footprint-srv-disc-acid = Dukungan transaksi ACID penuh
bench-footprint-srv-disc-benefits-title = Manfaat Deployment Browser
bench-footprint-srv-disc-benefits = Menjalankan SQL di browser menghilangkan latensi round-trip ke server, memungkinkan aplikasi offline-first, dan menjaga data sensitif di perangkat pengguna. Build WASM VibeSQL dirancang untuk kasus penggunaan ini dengan dependensi minimal dan penggunaan memori yang efisien.
bench-footprint-srv-disc-roadmap-title = Roadmap WASM
bench-footprint-srv-disc-streaming = Mulai mengeksekusi saat modul sedang diunduh
bench-footprint-srv-disc-indexeddb = Penyimpanan tahan lama di seluruh sesi browser
bench-footprint-srv-disc-worker = Jalankan kueri di luar thread utama untuk UI yang responsif

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
bench-bullet-prepared-stmts = Prepared statements
bench-bullet-larger-scale = Faktor skala lebih besar
bench-bullet-parallel-clients = Klien paralel

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
bench-tpcc-disc-duckdb = DuckDB hanya mencapai ~385 TPS di TPC-C (60x lebih lambat dari VibeSQL, 12x lebih lambat dari SQLite). Ini diharapkan: DuckDB adalah <strong>database analitis (OLAP)</strong> yang dioptimalkan untuk operasi batch besar, bukan transaksi baris tunggal.
bench-tpcc-disc-duckdb-title = Mengapa DuckDB Tertinggal di OLTP
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
conformance-pgsql-title = Tes Regresi PostgreSQL
conformance-pgsql-desc = Hasil menjalankan <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">suite tes regresi PostgreSQL</a> - suite tes kanonik yang digunakan untuk memvalidasi kompatibilitas PostgreSQL.
conformance-pgsql-tests-passing = tes lulus
conformance-pgsql-tests-excluded = tes dikecualikan
conformance-pgsql-pass-rate = Tingkat Kelulusan
conformance-pgsql-excluded-reason = Tes yang dikecualikan menggunakan fitur khusus PostgreSQL yang tidak berlaku untuk VibeSQL
conformance-pgsql-note = <strong>Catatan:</strong> Tes regresi PostgreSQL memvalidasi perilaku SQL terhadap implementasi referensi PostgreSQL. Tes yang dikecualikan melibatkan fitur khusus PostgreSQL seperti katalog sistem, bahasa prosedural, atau modul ekstensi.

# Bagian Suite Tes TCL SQLite
conformance-tcl-title = Suite Tes TCL SQLite
conformance-tcl-desc = Hasil dari <a href="https://www.sqlite.org/testing.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">suite tes TCL</a> kanonik SQLite yang berisi { $fileCount } file tes. Suite ini adalah standar emas untuk pengujian kompatibilitas SQLite.
conformance-tcl-overall-rate = Tingkat Kelulusan Keseluruhan
conformance-tcl-tests-passing = { $passed } dari { $total } tes lulus
conformance-tcl-passed = Lulus
conformance-tcl-failed = Gagal
conformance-tcl-skipped = Dilewati
conformance-tcl-total = Total
conformance-tcl-categories-title = Kategori Tes
conformance-tcl-category = Kategori
conformance-tcl-rate = Tingkat
conformance-tcl-progress = Kemajuan
conformance-tcl-tests = Tes
conformance-tcl-common-failures = Kegagalan Umum
conformance-tcl-failure-patterns = { $count } pola kegagalan teratas berdasarkan jumlah kejadian
conformance-tcl-about-title = Tentang Tes TCL:
conformance-tcl-about-text = Suite tes TCL SQLite adalah tes kesesuaian kanonik untuk kompatibilitas SQLite. Ini menguji perilaku SQLite tertentu, keunikan, dan kasus tepi yang mungkin tidak tercakup oleh suite tes SQL standar. Tingkat kelulusan tinggi di sini menunjukkan kompatibilitas SQLite yang kuat untuk skenario migrasi aplikasi.
