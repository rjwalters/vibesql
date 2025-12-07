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
