# VibeSQL Web UI - Türkçe

# Page titles
page-title = VibeSQL - Yapay Zeka Destekli SQL:1999 Veritabanı
demo-title = VibeSQL Demo
benchmarks-title = Performans Karşılaştırmaları - VibeSQL
benchmarks-heading = VibeSQL - Performans Karşılaştırmaları
conformance-title = Uyumluluk Raporu - VibeSQL
conformance-heading = Uyumluluk Raporu
conformance-subtitle = SQL:1999 Standart Uyumluluk Testi

# Navigation
nav-showcase = SQL:1999 Vitrin
nav-conformance = sqltest Sonuçlarını Görüntüle
nav-sqllogictest = SQLLogicTest Sonuçlarını Görüntüle

# Editor section
editor-title = SQL Düzenleyici
editor-storage = Depolama
editor-storage-init = Başlatılıyor...
editor-execute = Sorguyu Çalıştır

# Results section
results-title = Sonuçlar
results-empty = Sonuçları görmek için bir sorgu çalıştırın
results-loading = Yükleniyor...
results-rows = { $count } satır
results-rows-with-time = { $count } satır ({ $time }ms)
results-copy = Panoya kopyala
results-export = CSV Dışa Aktar
results-limit-warning = { $total } satırın ilk { $limit } tanesi gösteriliyor. Sorgunuzu daraltmak için LIMIT kullanın.

# Examples sidebar
examples-title = Örnekler
examples-basic = Temel Sorgular
examples-advanced = Gelişmiş Sorgular

# Database selector
db-select-label = Veritabanı

# Footer
footer-tagline = VibeSQL - WebAssembly'de SQL:1999 Veritabanı
footer-deployed = Dağıtım: { $date }

# Theme
theme-toggle-dark = Karanlık moda geç
theme-toggle-light = Aydınlık moda geç

# Locale
locale-select = Dil seçin

# Messages
msg-query-success = Sorgu başarıyla çalıştırıldı
msg-rows-affected = { $count } satır etkilendi

# Errors
error-generic = Bir hata oluştu
error-query-failed = Sorgu başarısız oldu
error-no-databases = Kullanılabilir veritabanı yok

# Loading states
loading-initializing-theme = Tema başlatılıyor
loading-preparing-editor = Düzenleyici hazırlanıyor
loading-database-engine = Veritabanı motoru yükleniyor
loading-setting-up-ui = Kullanıcı arayüzü ayarlanıyor
loading-editor = Düzenleyici yükleniyor...
loading-compliance-data = Uyumluluk verileri yükleniyor...
loading-conformance-report = Uygunluk raporu yükleniyor...

# Editor
editor-placeholder = SQL sorgusunu buraya girin... (Çalıştırmak için Ctrl+Enter veya Cmd+Enter)

# Navigation links
nav-terminal = SQL Terminal Demo
nav-compliance = SQL Test Uyumluluk Raporu
nav-benchmarks = Performans Karşılaştırmaları
nav-github = GitHub Deposu
nav-home = Ana Sayfa

# Results
results-success-zero = Sorgu başarıyla çalıştırıldı (0 satır)
results-null = NULL

# Help Modal
help-title = Klavye Kısayolları ve Yardım
help-close = Kapat
help-editor-shortcuts = Düzenleyici Kısayolları
help-navigation = Navigasyon
help-results-actions = Sonuç Eylemleri
help-tips = İpuçları
help-shortcut-execute = Mevcut sorguyu çalıştır
help-shortcut-comment = Satır yorumunu değiştir
help-shortcut-indent = Seçimi girintile
help-shortcut-show-help = Bu yardım penceresini göster
help-shortcut-close-help = Yardım penceresini kapat
help-action-copy = Panoya kopyala
help-action-copy-desc = Sonuçları sekmeyle ayrılmış değerler olarak kopyala
help-action-export = CSV Dışa Aktar
help-action-export-desc = Sonuçları CSV dosyası olarak indir
help-tip-limit = Sonuçlar performans için 1.000 satırla sınırlıdır. Sorguları daraltmak için LIMIT kullanın.
help-tip-time = Yürütme süresi sorgu sonuçlarıyla birlikte gösterilir.
help-tip-syntax = Düzenleyici SQL sözdizimi vurgulamayı ve otomatik tamamlamayı destekler.
help-tip-theme = Tema düğmesiyle açık/koyu mod arasında geçiş yapın.
help-got-it = Anladım!

# Showcase Navigation
showcase-title = SQL:1999 Core Vitrini
showcase-description = Uygulanan SQL:1999 Core özelliklerini etkileşimli olarak keşfedin
showcase-complete = { $percent }% Tamamlandı
showcase-categories = Özellik Kategorileri
showcase-legend = Durum Açıklaması
showcase-status-implemented = Tam Olarak Uygulandı
showcase-status-partial = Kısmen Uygulandı
showcase-status-planned = Planlandı

# Showcase category labels
showcase-cat-compliance = Uyumluluk Paneli
showcase-cat-data-types = Veri Türleri
showcase-cat-dml = DML İşlemleri
showcase-cat-predicates = Yüklemler ve İşleçler
showcase-cat-joins = JOIN
showcase-cat-subqueries = Alt Sorgular
showcase-cat-aggregates = Agregalar ve GROUP BY
showcase-cat-ddl = DDL ve Kısıtlamalar

# Common showcase elements
showcase-interactive-examples = Etkileşimli Örnekler
showcase-try-example = Bu Örneği Dene
showcase-progress = { $total } { $type } içinden { $implemented } ({ $percent }%)
showcase-table-status = Durum
showcase-table-category = Kategori
showcase-table-description = Açıklama
showcase-table-syntax = Sözdizimi
showcase-table-use-case = Kullanım Durumu

# Status labels
status-implemented = Uygulandı
status-partial = Kısmi
status-planned = Planlandı

# Aggregates Showcase
aggregates-title = SQL Agregalar ve GROUP BY
aggregates-description = SQL:1999 Core agregat fonksiyonları ve gruplama yetenekleri
aggregates-reference = Agregat Fonksiyonları Referansı
aggregates-table-function = Fonksiyon
aggregates-progress-type = fonksiyon
aggregates-ex-basic = Temel Agregat Fonksiyonları
aggregates-ex-group-single = GROUP BY (Tek Sütun)
aggregates-ex-group-multiple = GROUP BY (Birden Fazla Sütun)
aggregates-ex-having = HAVING Cümlesi
aggregates-ex-orderby = Agregatlarla ORDER BY
aggregates-ex-null = Agregatlarda NULL İşleme

# DML Operations Showcase
dml-title = DML İşlemleri (Veri İşleme Dili)
dml-description = Verileri sorgulamak ve değiştirmek için SQL:1999 Core işlemleri
dml-reference = DML İşlemleri Referansı
dml-table-operation = İşlem
dml-progress-type = işlem
dml-ex-select-basic = SELECT - Temel Sorgular
dml-ex-select-ordering = SELECT - Sıralama ve Sınırlama
dml-ex-insert = INSERT İşlemleri
dml-ex-update = UPDATE İşlemleri
dml-ex-delete = DELETE İşlemleri
dml-ex-combined = Birleşik CRUD İş Akışı

# Data Types Showcase
datatypes-title = SQL:1999 Core Veri Türleri
datatypes-description = SQL:1999 Core spesifikasyonunda tanımlanan temel veri türlerini keşfedin
datatypes-reference = Veri Türleri Referansı
datatypes-table-type = Tür Adı
datatypes-table-example = Örnek Değerler
datatypes-table-spec = Spesifikasyon
datatypes-progress-type = tür
datatypes-ex-numeric = Sayısal Türlerle Çalışma
datatypes-ex-null = NULL İşleme ve Üç Değerli Mantık
datatypes-ex-comparisons = Tür Karşılaştırmaları ve İşlemler

# JOINs Showcase
joins-title = SQL JOIN
joins-description = Birden fazla tablodan verileri birleştirmek için SQL:1999 Core JOIN işlemleri
joins-reference = JOIN Türleri Referansı
joins-table-type = JOIN Türü
joins-progress-type = JOIN türü
joins-category-suffix = JOIN
joins-ex-sample = Örnek Veri Kurulumu
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = Çok Tablolu JOIN

# Predicates Showcase
predicates-title = Yüklemler ve İşleçler
predicates-description = Filtreleme ve mantıksal işlemler için SQL:1999 yüklemleri
predicates-reference = Yüklemler Referansı
predicates-table-predicate = Yüklem
predicates-progress-type = yüklem
predicates-ex-comparison = Karşılaştırma İşleçleri
predicates-ex-between = BETWEEN ve Aralık Yüklemleri
predicates-ex-null = NULL Yüklemleri ve Üç Değerli Mantık
predicates-ex-boolean = Boolean Mantığı (AND, OR, NOT)
predicates-ex-in = Alt Sorgularla IN Yüklemi
predicates-ex-combined = Birleşik Yüklem İşlemleri

# Subqueries Showcase
subqueries-title = SQL Alt Sorgular
subqueries-description = İç içe sorgu işlemleri için SQL:1999 Core alt sorgu yetenekleri
subqueries-reference = Alt Sorgu Türleri Referansı
subqueries-table-type = Alt Sorgu Türü
subqueries-progress-type = alt sorgu türü
subqueries-ex-scalar-select = SELECT'te Skaler Alt Sorgu
subqueries-ex-scalar-where = WHERE'de Skaler Alt Sorgu
subqueries-ex-derived = Türetilmiş Tablolar (FROM'da Alt Sorgu)
subqueries-ex-in = Alt Sorguyla IN Yüklemi
subqueries-ex-correlated = İlişkili Alt Sorgular
subqueries-ex-nested = İç İçe Alt Sorgular

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
bench-no-wasm-data = WASM verisi mevcut değil
bench-no-server-data = Sysbench sunucu benchmark verisi mevcut değil
bench-no-server-data-hint = Sunucu benchmarkları, MySQL karşılaştırması etkinleştirilmiş olarak sysbench_server çalıştırmayı gerektirir.

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
bench-tpch-description = Bu kıyaslamalar, toplamalar, birleştirmeler, alt sorgular ve sıralama içeren karmaşık analitik sorgularla gerçek dünya karar destek iş yüklerini simüle eden endüstri standardı <strong>TPC-H kıyaslama paketi</strong>ni kullanır.
bench-tpch-ops-label = TPC-H queries
bench-tpch-note-intro = Tüm kıyaslamalar, ayrıştırma, planlama, yürütme ve sonuç materyalizasyonu dahil uçtan uca sorgu yürütme süresini ölçer. Bu, analitik iş yükleri için <strong>gerçek dünya SQL motoru performansını</strong> temsil eder.
bench-tpch-note-queries = <strong>Not:</strong> TPC-H sorguları SQL performansının farklı yönlerini test eder: basit toplamalar (Q1, Q6), karmaşık birleştirmeler (Q2-Q5, Q7-Q10), alt sorgular (Q11-Q15) ve gelişmiş analitik (Q16-Q22). Açıklamalar için yukarıdaki tablodaki sorgu adlarının üzerine gelin.

# TPC-H Discussion
bench-tpch-disc-excels-title = VibeSQL'in Öne Çıktığı Alanlar
bench-tpch-disc-excels = VibeSQL, sütunlu yürütme motorumuz ve SIMD hızlandırmalı toplamalarımızın parladığı <strong>tarama yoğun toplama sorgularında</strong> (Q1, Q6, Q14, Q15, Q20) güçlü performans gösterir. Bu sorgular, karmaşık birleştirme kalıpları olmadan büyük tabloları filtrelemeyi ve toplamları hesaplamayı içerir.
bench-tpch-disc-targets-title = Mevcut Optimizasyon Hedefleri
bench-tpch-disc-targets = Çok yollu birleştirme sorguları (Q3, Q5, Q7-Q10, Q18, Q19, Q21) şu anda SQLite'ın önde olduğunu gösteriyor. Birincil darboğaz, henüz SQLite'ın on yıllarca geliştirilmiş B-tree birleştirmeleriyle aynı optimizasyon seviyesini kullanmayan hash birleştirme uygulamamızdır. Aktif geliştirme altındaki özel alanlar:
bench-tpch-disc-join-ordering = Daha iyi birleştirme sırası seçimi için geliştirilmiş kardinalite tahmini
bench-tpch-disc-hash-sizing = Büyük birleştirmeler için uyarlanabilir hash tablosu büyümesi ve diske taşma
bench-tpch-disc-vectorized = Önbellek kullanımını iyileştirmek için birleştirme iç döngüsünde toplu işleme
bench-tpch-disc-inl-joins = Faydalı olduğunda B-tree indekslerinden yararlanma
bench-tpch-disc-path-title = Liderliğe Giden Yol
bench-tpch-disc-path = VibeSQL'in mimarisi, sütunlu depolama, vektörize yürütme ve kilitsiz eşzamanlılık gibi özelliklerle modern donanım için tasarlanmıştır. Bu optimizasyonlar olgunlaştıkça, VibeSQL'in tüm TPC-H sorgularında tutarlı liderlik elde etmesini bekliyoruz.

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
bench-tpcds-description = <strong>TPC-DS</strong>, TPC-H'nin halefidir ve birden fazla olgu tablosu, kar tanesi şeması ve gelişmiş SQL özellikleri dahil olmak üzere çok daha karmaşık sorgu kalıplarını modelleyen 99 sorgu içerir.
bench-tpcds-ops-label = TPC-DS queries
bench-tpcds-note-intro = TPC-DS sorguları TPC-H'den önemli ölçüde daha karmaşıktır, pencere fonksiyonları, ortak tablo ifadeleri (WITH yan tümcesi) ve birden fazla olgu ve boyut tablosu arasında karmaşık birleştirme kalıpları gibi gelişmiş SQL özelliklerini test eder.
bench-tpcds-note-remaining = <strong>Not:</strong> Tüm 99 TPC-DS sorgusu başarılı olur, INTERSECT, EXCEPT, pencere fonksiyonları, CTE'ler ve karmaşık alt sorgular dahil kapsamlı SQL:1999 özellik desteğini gösterir.

# TPC-DS Discussion
bench-tpcds-disc-coverage-title = SQL:1999 Özellik Kapsamı
bench-tpcds-disc-coverage = TPC-DS en zorlu SQL özelliklerini test eder. VibeSQL, ROLLUP, CUBE, GROUPING(), karmaşık çerçeveleme ile pencere fonksiyonları, özyinelemeli CTE'ler ve INTERSECT/EXCEPT küme işlemleri dahil olmak üzere SQL:1999'un tam kapsamını gösteren <strong>tüm 99 sorguyu</strong> başarıyla geçer.
bench-tpcds-disc-optimization-title = Karmaşık Sorgu Optimizasyonu
bench-tpcds-disc-optimization = TPC-DS sorguları genellikle ilişkili alt sorgularla 10'dan fazla tabloyu birleştirir. Mevcut odak alanları:
bench-tpcds-disc-cte = Materyalize edilmiş ve satır içi CTE'ler arasında akıllı karar
bench-tpcds-disc-decorrelation = Faydalı olduğunda ilişkili alt sorguları birleştirmelere dönüştürme
bench-tpcds-disc-star = Analitik kalıplar için olgu-boyut birleştirme sıralaması
bench-tpcds-disc-toward-title = Tam TPC-DS Kapsamı
bench-tpcds-disc-toward = Tüm 99 sorgunun başarılı olmasıyla, VibeSQL karmaşık analitik iş yükleri için üretime hazır SQL:1999 uyumluluğunu gösterir. INTERSECT ve EXCEPT küme işlemlerinin son eklenmesi tam TPC-DS kapsamını tamamladı.
bench-tpcds-disc-sqlite-title = SQLite Karşılaştırma Notu
bench-tpcds-disc-sqlite = SQLite, eksik SQL:1999 OLAP özellikleri nedeniyle 99 TPC-DS sorgusundan 12'sini (Q2, Q5, Q14, Q17, Q18, Q22, Q36, Q67, Q70, Q77, Q80, Q86) yürütemez: <strong>ROLLUP/CUBE</strong> gruplama kümeleri, <strong>GROUPING()</strong> fonksiyonu ve <strong>STDDEV_SAMP()</strong>. Bu sorgular SQLite kıyaslamalarında atlanır. VibeSQL ve DuckDB tüm 99 sorguyu destekler.

# TPC-C specific
bench-tpcc-name = TPC-C
bench-tpcc-title = TPC-C Online Transaction Processing Benchmark
bench-tpcc-description = <strong>TPC-C kıyaslaması</strong>, sipariş girişi, ödeme işleme, sipariş durumu sorguları, teslimat işleme ve stok seviyesi izleme dahil karmaşık işlemlerin bir karışımıyla eksiksiz bir sipariş girişi ortamını simüle eder.
bench-tpcc-ops-label = TPC-C transactions
bench-tpcc-note-intro = TPC-C dakikadaki işlemleri (tpmC) ölçer ve veritabanının karmaşık iş mantığıyla eşzamanlı işlemleri işleme yeteneğini test eder. Bu kıyaslama, <strong>işlemsel iş yükü performansını</strong> değerlendirmek için kritik öneme sahiptir.
bench-tpcc-note-results = <strong>Not:</strong> Sonuçlar ortalama işlem gecikmesini gösterir. Düşük olan daha iyidir. TPC-C, katı tutarlılık gereksinimleri olan yazma yoğun iş yükleri için özellikle zorludur.

# TPC-C Transaction Descriptions
bench-tpcc-new-order = New Order - Complex transaction with inventory checks and order creation
bench-tpcc-payment = Payment - Update customer balance and warehouse/district totals
bench-tpcc-order-status = Order Status - Read-only query for customer order history
bench-tpcc-delivery = Delivery - Batch processing of pending orders
bench-tpcc-stock-level = Stock Level - Count items below threshold in recent orders

# TPC-C Discussion
bench-tpcc-disc-faster-title = SQLite'tan 5 Kat Daha Hızlı
bench-tpcc-disc-faster = VibeSQL, SQLite'ın ~4.500 TPS'sine kıyasla <strong>saniyede yaklaşık 23.000 işlem</strong> elde eder, 5 kat iyileşme. Bu hız artışı, her yazma işleminde SQLite'ın kaba taneli kilitlemesini önleyen kilitsiz MVCC mimarimizden gelir.
bench-tpcc-disc-dominates-title = VibeSQL Neden OLTP'de Baskın
bench-tpcc-disc-lockfree = MVCC, okuyucuların ve yazıcıların engellemeden eşzamanlı olarak ilerlemesine izin verir
bench-tpcc-disc-optimistic = İşlemler yalnızca taahhüt zamanında çakışır, yürütme sırasında değil
bench-tpcc-disc-btree = Bellek içi iş yükleri için optimize edilmiş amaca yönelik indeks yapısı
bench-tpcc-disc-prepared = Sorgu planları bir kez derlenir ve yeniden kullanılır
bench-tpcc-disc-scaling-title = Daha Fazla Ölçeklendirme
bench-tpcc-disc-scaling = Mevcut sonuçlar tek iş parçacıklıdır. VibeSQL'in mimarisi çok iş parçacıklı işlem işlemeyi destekler ve paralel yürütme desteği ekledikçe gelişmiş ölçeklendirme bekliyoruz.

# Sysbench Embedded specific
bench-sysbench-embedded-name = Sysbench (Embedded)
bench-sysbench-embedded-title = Sysbench Micro-Benchmarks (Embedded)
bench-sysbench-embedded-description = <strong>Sysbench</strong>, belirli veritabanı işlemlerini izole eden odaklanmış mikro-kıyaslamalar sağlar. Bu testler, tam işlem iş yüklerinin karmaşıklığı olmadan temel işlemler için ham performansı ölçer.
bench-sysbench-embedded-ops-label = Sysbench operations
bench-sysbench-embedded-note = Gömülü mod, veritabanını ağ yükü olmadan işlem içinde çalıştırır, minimum gecikmenin kritik olduğu tek işlem uygulamaları için idealdir.

# Sysbench Operation Descriptions
bench-sysbench-point-select = Point Select - Single row lookup by primary key
bench-sysbench-insert = Insert - Insert new rows into table
bench-sysbench-update-index = Update Index - Update indexed column (k = k + 1)
bench-sysbench-update-non-index = Update Non-Index - Update non-indexed column
bench-sysbench-delete = Delete - Remove rows by primary key
bench-sysbench-range-queries = Range Queries - Simple, SUM, ORDER BY, and DISTINCT range scans

# Sysbench Embedded Discussion
bench-sysbench-emb-disc-point-title = Nokta Aramaları: Eşitlik
bench-sysbench-emb-disc-point = VibeSQL'in nokta seçimleri <strong>~0.37µs</strong>'de çalışır, SQLite'ın ~0.36µs'sine eşittir. B-tree uygulamamız, minimum işaretçi izleme ve önbellek dostu düğüm düzenleri ile tek satırlık aramalar için optimize edilmiştir.
bench-sysbench-emb-disc-index-title = İndeks Güncellemeleri: İyileştirme Alanı
bench-sysbench-emb-disc-index = VibeSQL'in indeksli güncellemeleri <strong>SQLite'ın ~1.7µs'sine karşı ~4.3µs</strong>'de çalışır. Bu, MVCC tasarımımızın indeks bakımı için ek yük eklediği ve azaltmak için çalıştığımız bir optimizasyon alanıdır.
bench-sysbench-emb-disc-improve-title = İyileştirme Alanları
bench-sysbench-emb-disc-bulk = SQLite'ın toplu ekleme yolu yüksek düzeyde optimize edilmiştir; toplu B-tree işlemleri ekliyoruz
bench-sysbench-emb-disc-nonindex = İndekssiz güncellemeler VibeSQL'i ~1.9µs'de SQLite'ın ~1.4µs'sine karşı gösterir - eşitliğe yakın
bench-sysbench-emb-disc-deletes = Silme işlemleri önemli ölçüde iyileşti: şimdi SQLite'ın ~3.8µs'sine karşı ~5.5µs (önceden 1183µs)
bench-sysbench-emb-disc-duckdb-title = DuckDB Karşılaştırması
bench-sysbench-emb-disc-duckdb = DuckDB, mikro işlemler için değil, analitik iş yükleri için optimize edilmiştir. Buradaki 100-1000 kat daha yavaş sonuçları, tek satır gecikmesini toplu verimlilik için takas eden mimari seçimleri (sütunlu depolama, vektörize yürütme) yansıtır. VibeSQL her iki kullanım durumunu da hedefler.
bench-sysbench-emb-disc-architecture-title = Mimari Ödünleşimler
bench-sysbench-emb-disc-architecture = VibeSQL'in hibrit mimarisi hem OLTP hem de OLAP iş yüklerini hedefler. B-tree depolamamız SQLite ile rekabetçi nokta arama performansı sağlarken, sütunlu yürütme analitik sorguları verimli bir şekilde işler.

# Sysbench Server specific
bench-sysbench-server-name = Sysbench (Server)
bench-sysbench-server-title = Sysbench Micro-Benchmarks (Server)
bench-sysbench-server-description = <strong>Sysbench</strong> sunucu kıyaslamaları, çok istemcili veritabanı dağıtımları için performansı ölçerek VibeSQL Server'ı (PostgreSQL tel protokolü) MySQL ile karşılaştırır.
bench-sysbench-server-ops-label = Sysbench operations
bench-sysbench-server-note = Sunucu modu, çok istemcili erişim ve mevcut PostgreSQL araçları ve sürücüleriyle uyumluluk sağlayan PostgreSQL tel protokolünü kullanır.

# Sysbench Server Discussion
bench-sysbench-srv-disc-protocol-title = PostgreSQL Tel Protokolü
bench-sysbench-srv-disc-protocol = VibeSQL Server, mevcut PostgreSQL sürücüleri ve araçlarıyla uyumluluk sağlayan PostgreSQL tel protokolünü uygular. Bu, gömülü moda kıyasla sorgu başına ~10-50µs protokol yükü ekler, ancak çok istemcili dağıtımları etkinleştirir.
bench-sysbench-srv-disc-mysql-title = MySQL Karşılaştırması
bench-sysbench-srv-disc-mysql = Sunucu kıyaslamaları, geleneksel istemci-sunucu veritabanları için VibeSQL'i bir drop-in değiştirme olarak değerlendirmek için MySQL ile karşılaştırır. VibeSQL Server tüm Sysbench işlemlerinde MySQL'i geride bırakır, <strong>2,4x</strong> (aralık sorguları) ile <strong>12,8x</strong> (indeksli güncellemeler) arasında hız artışları sağlar.
bench-sysbench-srv-disc-perf-title = Neden VibeSQL Server Daha Hızlı
bench-sysbench-srv-disc-perf-arch = VibeSQL'in mimarisi MySQL'in geleneksel RDBMS tasarımından temelden farklıdır
bench-sysbench-srv-disc-perf-storage = VibeSQL, analitik ve OLTP iş yükleri için optimize edilmiş bellek içi sütunsal depolama motoru kullanır, MySQL'in disk tabanlı InnoDB sayfa yönetimi yükünden kaçınır
bench-sysbench-srv-disc-perf-locking = Ağır satır seviyesi kilitleme veya MVCC muhasebesi yok—VibeSQL modern çok çekirdekli CPU'lar için tasarlanmış hafif eşzamanlılık kontrolü kullanır
bench-sysbench-srv-disc-perf-protocol = MySQL protokolüne kıyasla minimum serileştirme yükü ile verimli PostgreSQL tel protokolü uygulaması
bench-sysbench-srv-disc-perf-writes = Yazma işlemleri (ekleme/güncelleme) en büyük kazanımları gösterir (<strong>8-12x</strong>) çünkü VibeSQL, MySQL'in redo log, undo log ve doublewrite buffer senkronizasyonundan kaçınır
bench-sysbench-srv-disc-perf-reads = Okuma işlemleri, önbellek verimli sütunsal erişim kalıpları ve sıfır disk I/O sayesinde daha küçük ama tutarlı kazanımlar gösterir (<strong>2-3x</strong>)
bench-sysbench-srv-disc-roadmap-title = Sunucu Yol Haritası
bench-sysbench-srv-disc-pooling = Yüksek verimlilik senaryoları için bağlantı kurma yükünü azaltma
bench-sysbench-srv-disc-caching = Bağlantılar arasında sunucu taraflı sorgu planı önbellekleme
bench-sysbench-srv-disc-extended = Toplu işlemler için tam PostgreSQL genişletilmiş sorgu protokolü desteği

# TPC-H Server specific
bench-tpch-server-name = TPC-H (Sunucu)
bench-tpch-server-title = TPC-H Analitik Benchmark (Sunucu)
bench-tpch-server-description = <strong>TPC-H sunucu benchmarkları</strong>, analitik sorgu iş yükleri için VibeSQL Sunucu'yu (PostgreSQL tel protokolü) MySQL ile karşılaştırarak istemci-sunucu dağıtımlarında OLAP performansını ölçer.
bench-tpch-server-ops-label = TPC-H sorguları
bench-tpch-server-note-intro = Sunucu benchmarkları, ağ yükü dahil uçtan uca sorgu gecikmesini ölçerek <strong>PostgreSQL tel protokolü</strong> uygulamasını test eder.
bench-tpch-server-note-queries = Sorgular, iş zekası iş yüklerinde tipik olan karmaşık JOIN'leri, alt sorguları ve toplamları test eder.

# TPC-H Server Discussion
bench-tpch-srv-disc-protocol-title = PostgreSQL Tel Protokolü
bench-tpch-srv-disc-protocol = VibeSQL Sunucu, standart PostgreSQL sürücüleri ve araçlarının kullanımını sağlayan PostgreSQL tel protokolünü konuşur. Bu benchmark, protokol yükü dahil tam uçtan uca gecikmeyi ölçer.
bench-tpch-srv-disc-comparison-title = MySQL Karşılaştırması
bench-tpch-srv-disc-comparison = MySQL ile karşılaştırma, analitik iş yüklerinde geleneksel istemci-sunucu veritabanları için bir temel sağlar. VibeSQL'in sütunlu yürütme motoru, karmaşık toplamlar ve birleştirmeler için avantajlar sağlar.
bench-tpch-srv-disc-roadmap-title = Sunucu OLAP Yol Haritası
bench-tpch-srv-disc-prepared = Bağlantılar arasında derlenmiş sorgu planlarını yeniden kullan
bench-tpch-srv-disc-pooling = Yüksek verimlilik senaryoları için verimli bağlantı yönetimi
bench-tpch-srv-disc-scale = Üretim ölçeğinde doğrulama için daha büyük veri setlerini test etme (SF 0.1, SF 1.0)

# TPC-C Server specific
bench-tpcc-server-name = TPC-C (Sunucu)
bench-tpcc-server-title = TPC-C OLTP Benchmark (Sunucu)
bench-tpcc-server-description = <strong>TPC-C sunucu benchmarkları</strong>, OLTP işlem iş yükleri için VibeSQL Sunucu'yu (PostgreSQL tel protokolü) MySQL ile karşılaştırarak çok istemcili veritabanı dağıtımları için verimi ölçer.
bench-tpcc-server-ops-label = TPC-C işlemleri
bench-tpcc-server-note-intro = Sunucu benchmarkları, ağ yükü dahil işlem verimini ölçerek <strong>PostgreSQL tel protokolü</strong> uygulamasını test eder.
bench-tpcc-server-note-results = Sonuçlar, standart TPC-C işlem karışımı için saniyedeki işlemleri (TPS) raporlar.
bench-tpcc-mixed = Karışık İş Yükü - Standart TPC-C işlem karışımı (%45 Yeni Sipariş, %43 Ödeme, %4 Sipariş Durumu, %4 Teslimat, %4 Stok Seviyesi)

# TPC-C Server Discussion
bench-tpcc-srv-disc-protocol-title = PostgreSQL Tel Protokolü
bench-tpcc-srv-disc-protocol = VibeSQL Sunucu, standart PostgreSQL sürücüleri ve araçlarının kullanımını sağlayan PostgreSQL tel protokolünü konuşur. Bu benchmark, protokol yükü dahil tam uçtan uca işlem gecikmesini ölçer.
bench-tpcc-srv-disc-comparison-title = MySQL Karşılaştırması
bench-tpcc-srv-disc-comparison = MySQL ile karşılaştırma, OLTP iş yüklerinde geleneksel istemci-sunucu veritabanları için bir temel sağlar. MySQL, işlemsel iş yükleri için endüstri standardıdır ve TPC-C, MySQL'in güçlü yönüdür.
bench-tpcc-srv-disc-roadmap-title = Sunucu OLTP Yol Haritası
bench-tpcc-srv-disc-prepared = Bağlantılar arasında derlenmiş sorgu planlarını yeniden kullan
bench-tpcc-srv-disc-pooling = Yüksek verimlilik senaryoları için verimli bağlantı yönetimi
bench-tpcc-srv-disc-parallel = Çok istemcili eşzamanlı işlem işleme

# Footprint Embedded specific
bench-footprint-embedded-name = Footprint (Embedded)
bench-footprint-embedded-title = Native Binary Footprint
bench-footprint-embedded-description = <strong>Gömülü ayak izi kıyaslamaları</strong>, ikili boyut, soğuk başlatma süresi ve en yüksek bellek kullanımını karşılaştırarak yerel veritabanı ikili dosyalarının kaynak verimliliğini ölçer.
bench-footprint-embedded-ops-label = databases compared
bench-footprint-embedded-note = Yerel ikili ayak izi, ikili boyut, başlatma gecikmesi ve bellek tüketiminin dağıtım fizibilitesini doğrudan etkilediği <strong>gömülü ve kenar dağıtımları</strong> için kritik öneme sahiptir.

# Footprint Embedded Descriptions
bench-footprint-binary-size = Binary Size - Size of the compiled database binary on disk
bench-footprint-startup-time = Startup Time - Time to cold-start and execute first query
bench-footprint-peak-memory = Peak Memory - Maximum resident set size during initialization

# Footprint Embedded Discussion
bench-footprint-emb-disc-size-title = İkili Boyut: Orta Yol
bench-footprint-emb-disc-size = VibeSQL <strong>~{ $vibesqlBinaryMb }MB</strong> ile SQLite (~{ $sqliteBinaryMb }MB) ve DuckDB (~{ $duckdbBinaryMb }MB) arasında yer alır. Bu, gömülü dağıtımlar için ikili dosyayı yönetilebilir tutarken gelişmiş özellikleri (pencere fonksiyonları, CTE'ler, sütunlu yürütme) dahil etme seçimimizi yansıtır.
bench-footprint-emb-disc-startup-title = Başlatma: En Hızlı Soğuk Başlatma
bench-footprint-emb-disc-startup = VibeSQL, SQLite'tan (~6.5ms) daha hızlı ve DuckDB'den (~13ms) önemli ölçüde daha hızlı <strong>~6ms soğuk başlatma</strong> elde eder. Minimum başlatma yolumuz, başlatmada yalnızca temel meta veri yapılarını yükler.
bench-footprint-emb-disc-memory-title = Bellek Verimliliği
bench-footprint-emb-disc-memory = Başlatma sırasında en yüksek bellek VibeSQL için ~7MB, SQLite için ~3MB ve DuckDB için ~11MB'dir. SQLite'tan fark, daha sofistike sorgu optimize edicimizi ve önceden tahsis edilmiş sütunlu yürütme altyapımızı yansıtır.
bench-footprint-emb-disc-roadmap-title = Boyut Azaltma Yol Haritası
bench-footprint-emb-disc-flags = Kullanılmayan işlevselliği hariç tutmak için derleme zamanı özellik seçimi
bench-footprint-emb-disc-lto = Ölü kod eliminasyonu için tüm program bağlama zamanı optimizasyonu
bench-footprint-emb-disc-modular = Çekirdek motoru isteğe bağlı özelliklerden ayırma (örneğin, pencere fonksiyonları)

# Footprint Server/WASM specific
bench-footprint-server-name = Footprint (Server/WASM)
bench-footprint-server-title = WASM Footprint
bench-footprint-server-description = <strong>WASM ayak izi kıyaslamaları</strong>, indirme boyutunun kullanıcı deneyimini etkilediği web uygulamaları için kritik olan tarayıcı dağıtımı için WebAssembly modül boyutunu ölçer.
bench-footprint-server-ops-label = deployment targets
bench-footprint-server-note = WASM boyutları, indirme süresinin etkileşim süresini doğrudan etkilediği <strong>web dağıtımları</strong> için kritik öneme sahiptir. Tarayıcılar gzip içeriğini otomatik olarak açtığından Gzip boyutları en alakalı olanlardır.
bench-footprint-server-note2 = <strong>Not:</strong> VibeSQL WASM, tarayıcıda tam SQL:1999 uyumluluğunu korurken minimum indirme boyutu için tasarlanmıştır.

# Footprint Server Descriptions
bench-footprint-wasm-size = WASM Size - Size of the WebAssembly module for browser deployment
bench-footprint-wasm-gzip = WASM (gzip) - Compressed size for web delivery

# Footprint Server Discussion
bench-footprint-srv-disc-wasm-title = WASM: 1.5MB Sıkıştırılmış
bench-footprint-srv-disc-wasm = VibeSQL'in WebAssembly modülü <strong>~{ $wasmSizeGzipMb }MB gzipped</strong> olarak sıkıştırılır ve hızlı ilk sayfa yüklemelerini sağlar. Bu, pencere fonksiyonları, CTE'ler ve ACID işlemleri ile tamamen tarayıcıda çalışan tam bir SQL:1999 veritabanıdır.
bench-footprint-srv-disc-included-title = Dahil Olanlar
bench-footprint-srv-disc-parser = Tam SQL ayrıştırıcı ve sorgu optimize edici
bench-footprint-srv-disc-btree = MVCC ile B-tree depolama motoru
bench-footprint-srv-disc-window = Pencere fonksiyonları ve gelişmiş toplamalar
bench-footprint-srv-disc-cte = Ortak tablo ifadeleri (WITH yan tümcesi)
bench-footprint-srv-disc-acid = Tam ACID işlem desteği
bench-footprint-srv-disc-benefits-title = Tarayıcı Dağıtım Avantajları
bench-footprint-srv-disc-benefits = SQL'i tarayıcıda çalıştırmak sunuculara gidiş-dönüş gecikmesini ortadan kaldırır, çevrimdışı öncelikli uygulamaları etkinleştirir ve hassas verileri kullanıcının cihazında tutar. VibeSQL'in WASM yapısı, minimum bağımlılıklar ve verimli bellek kullanımı ile bu kullanım durumu için tasarlanmıştır.
bench-footprint-srv-disc-roadmap-title = WASM Yol Haritası
bench-footprint-srv-disc-streaming = Modül indirilirken yürütmeye başlama
bench-footprint-srv-disc-indexeddb = Tarayıcı oturumları arasında kalıcı depolama
bench-footprint-srv-disc-worker = Duyarlı kullanıcı arayüzleri için ana iş parçacığı dışında sorgu çalıştırma

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
bench-bullet-prepared-stmts = Hazırlanmış ifadeler
bench-bullet-larger-scale = Daha büyük ölçek faktörleri
bench-bullet-parallel-clients = Paralel istemciler

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
bench-tpcc-disc-duckdb = DuckDB, TPC-C'de yalnızca ~385 TPS elde eder (VibeSQL'den 60 kat daha yavaş, SQLite'tan 12 kat daha yavaş). Bu beklenen bir durumdur: DuckDB, tek satırlık işlemler için değil, büyük toplu işlemler için optimize edilmiş bir <strong>analitik (OLAP) veritabanıdır</strong>.
bench-tpcc-disc-duckdb-title = DuckDB Neden OLTP'de Geride
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
conformance-pgsql-title = PostgreSQL Regresyon Testleri
conformance-pgsql-desc = <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">PostgreSQL regresyon test paketi</a> çalıştırma sonuçları - PostgreSQL uyumluluğunu doğrulamak için kullanılan kanonik test paketi.
conformance-pgsql-tests-passing = test geçti
conformance-pgsql-tests-excluded = test hariç tutuldu
conformance-pgsql-pass-rate = Geçme Oranı
conformance-pgsql-excluded-reason = Hariç tutulan testler, VibeSQL için geçerli olmayan PostgreSQL'e özgü özellikler kullanır
conformance-pgsql-note = <strong>Not:</strong> PostgreSQL regresyon testleri, SQL davranışını PostgreSQL referans uygulamasına göre doğrular. Hariç tutulan testler, sistem katalogları, prosedürel diller veya uzantı modülleri gibi PostgreSQL'e özgü özellikler içerir.

# SQLite TCL Test Paketi Bölümü
conformance-tcl-title = SQLite TCL Test Paketi
conformance-tcl-desc = { $fileCount } test dosyası içeren SQLite'ın standart <a href="https://www.sqlite.org/testing.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">TCL test paketi</a> sonuçları. Bu paket, SQLite uyumluluk testleri için altın standarttır.
conformance-tcl-overall-rate = Genel Geçme Oranı
conformance-tcl-tests-passing = { $total } testten { $passed } tanesi geçti
conformance-tcl-passed = Geçti
conformance-tcl-failed = Başarısız
conformance-tcl-skipped = Atlandı
conformance-tcl-total = Toplam
conformance-tcl-categories-title = Test Kategorileri
conformance-tcl-category = Kategori
conformance-tcl-rate = Oran
conformance-tcl-progress = İlerleme
conformance-tcl-tests = Testler
conformance-tcl-common-failures = Yaygın Hatalar
conformance-tcl-failure-patterns = Oluşum sayısına göre en sık { $count } hata kalıbı
conformance-tcl-about-title = TCL Testleri Hakkında:
conformance-tcl-about-text = SQLite'ın TCL test paketi, SQLite uyumluluğu için standart uygunluk testidir. Standart SQL test paketleri tarafından kapsanmayabilecek belirli SQLite davranışlarını, tuhaflıkları ve uç durumları test eder. Burada yüksek geçme oranları, uygulama taşıma senaryoları için güçlü SQLite uyumluluğunu gösterir.

# =============================================================================
# Homepage
# =============================================================================

home-page-title = VibeSQL — Hız İçin Tasarlanmış Saf Rust SQL Veritabanı

# Hero section
home-hero-title = Hız için tasarlanmış<br>saf Rust SQL veritabanı
home-hero-subtitle = VibeSQL, depolama verimliliği yerine performansı tercih eder. Hibrit satır + sütunlu depolama, vektörize yürütme ve sıfır unsafe kod. Belleğe sığan veri setleri için optimize edilmiştir.
home-hero-subtext = SQL:1999 uyumlu. Nativ, WebAssembly ve gömülü kütüphane olarak çalışır.
home-btn-demo = Tarayıcıda Dene
home-btn-github = GitHub
home-btn-crates = crates.io

# Why VibeSQL section
home-why-title = Neden VibeSQL?
home-hybrid-title = Hibrit Depolama
home-hybrid-text = Tek motorda hem satır hem sütunlu depolama. Hızlı nokta aramaları ve OLTP için satır formatı. Vektörize yürütme ile analitik taramalar için sütunlu format. Seçim yapmanıza gerek yok.
home-speed-title = Depolama Yerine Hız
home-speed-text = Bilinçli olarak disk alanını sorgu performansı için feda eder. Yedekli depolama düzenleri, agresif önbellekleme ve önceden hesaplanmış indeksler, küçük veritabanlarının mümkün olan en hızlı şekilde çalışmasını sağlar.
home-rust-title = Saf Rust, Sıfır Unsafe
home-rust-text = Tamamen güvenli Rust ile yazılmıştır. C bağımlılığı yok, FFI yok, unsafe blokları yok. Aynı kod tabanından nativ ikili dosyalara ve WebAssembly'ye derlenir.

# Architecture section
home-arch-title = Mimari
home-pipeline-title = Sorgu İşlem Hattı
home-pipeline-parser = <strong>Ayrıştırıcı</strong> — Tam SQL:1999 dilbilgisi, arena tahsisli AST
home-pipeline-planner = <strong>Planlayıcı</strong> — Birleştirme yeniden sıralamalı maliyet tabanlı optimize edici
home-pipeline-executor = <strong>Yürütücü</strong> — Toplu işleme ile vektörize yürütme
home-pipeline-storage = <strong>Depolama</strong> — B-tree indeksli hibrit satır/sütunlu depolama
home-features-title = Temel Özellikler
home-feature-window = Pencere fonksiyonları (ROW_NUMBER, RANK, LEAD/LAG, NTILE, ...)
home-feature-cte = Ortak tablo ifadeleri (WITH, özyinelemeli CTE'ler)
home-feature-subquery = Alt sorgular (ilişkili, EXISTS, IN, skaler)
home-feature-join = Tam JOIN desteği (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL)
home-feature-triggers = Tetikleyiciler, görünümler, yabancı anahtarlar, CHECK kısıtlamaları
home-feature-wasm = OPFS kalıcı depolamalı WASM hedefi

# Performance section
home-perf-title = Performans
home-perf-full = Tam kıyaslamalar →
home-stat-tpch-label = TPC-H Sorgu Başarısı
home-stat-tpch-sub = Karar destek kıyaslaması
home-stat-conformance-label = SQLLogicTest Geçme Oranı
home-stat-conformance-sub = 6M+ test doğrulaması
home-stat-tpcds-label = TPC-DS Sorgu Başarısı
home-stat-tpcds-sub = Karmaşık analitik kıyaslaması
home-perf-note = SQLite, DuckDB ve MySQL ile eşdeğer iş yüklerinde kıyaslanmıştır. <a href="benchmarks.html" class="text-blue-600 dark:text-blue-400 hover:underline">Tam sonuçları görün.</a>

# Get Started section
home-start-title = Başlayın
home-demo-title = Etkileşimli Demo
home-demo-text = Tarayıcınızda SQL sorguları çalıştırın. OPFS ile kalıcı depolamaya sahip, WebAssembly'ye derlenmiş tam veritabanı motoru. Kurulum gerektirmez.
home-install-title = Kurulum
home-install-cargo = Cargo
home-install-library = Kütüphane olarak

# Explore section
home-explore-conformance-title = Uyumluluk Raporu
home-explore-conformance-text = 622 test dosyası genelinde SQL:1999 standart uyumluluğunun ayrıntılı dökümü.
home-explore-bench-title = Performans Kıyaslamaları
home-explore-bench-text = SQLite, DuckDB ve MySQL'e karşı TPC-H, TPC-DS, TPC-C ve Sysbench sonuçları.

# Footer
home-footer = VibeSQL — Hız için tasarlanmış saf Rust SQL veritabanı

# Navigation
nav-trends = Performans Trendleri

# Trends page
trends-title = Performans Trendleri - VibeSQL
trends-heading = VibeSQL - Performans Trendleri
trends-total-runs = Toplam Kıyaslama Çalıştırması
trends-across-suites = tüm paketlerde
trends-date-range = Tarih Aralığı
trends-first-to-last = ilkten son çalıştırmaya
trends-latest-commit = Son Commit
trends-most-recent = en son kıyaslama
trends-generated = Oluşturuldu
trends-last-export = son veri dışa aktarımı

# Bullet points
bench-bullet-prepared-stmts = Hazırlanmış ifadeler
bench-bullet-larger-scale = Daha büyük ölçek faktörleri
bench-bullet-parallel-clients = Paralel istemciler
