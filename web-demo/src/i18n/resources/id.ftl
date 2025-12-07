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
