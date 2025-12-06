# VibeSQL Katalog Hata Mesajları - Türkçe
# Bu dosya, vibesql-catalog crate'i için tüm hata mesajlarını içerir.

# =============================================================================
# Tablo Hataları
# =============================================================================

catalog-table-already-exists = '{ $name }' tablosu zaten mevcut
catalog-table-not-found = '{ $table_name }' tablosu bulunamadı

# =============================================================================
# Sütun Hataları
# =============================================================================

catalog-column-already-exists = '{ $name }' sütunu zaten mevcut
catalog-column-not-found = '{ $table_name }' tablosunda '{ $column_name }' sütunu bulunamadı

# =============================================================================
# Şema Hataları
# =============================================================================

catalog-schema-already-exists = '{ $name }' şeması zaten mevcut
catalog-schema-not-found = '{ $name }' şeması bulunamadı
catalog-schema-not-empty = '{ $name }' şeması boş değil

# =============================================================================
# Rol Hataları
# =============================================================================

catalog-role-already-exists = '{ $name }' rolü zaten mevcut
catalog-role-not-found = '{ $name }' rolü bulunamadı

# =============================================================================
# Alan Hataları
# =============================================================================

catalog-domain-already-exists = '{ $name }' alanı zaten mevcut
catalog-domain-not-found = '{ $name }' alanı bulunamadı
catalog-domain-in-use = '{ $domain_name }' alanı hâlâ { $count } sütun tarafından kullanılıyor: { $columns }

# =============================================================================
# Sıra Hataları
# =============================================================================

catalog-sequence-already-exists = '{ $name }' sırası zaten mevcut
catalog-sequence-not-found = '{ $name }' sırası bulunamadı
catalog-sequence-in-use = '{ $sequence_name }' sırası hâlâ { $count } sütun tarafından kullanılıyor: { $columns }

# =============================================================================
# Tip Hataları
# =============================================================================

catalog-type-already-exists = '{ $name }' tipi zaten mevcut
catalog-type-not-found = '{ $name }' tipi bulunamadı
catalog-type-in-use = '{ $name }' tipi hâlâ bir veya daha fazla tablo tarafından kullanılıyor

# =============================================================================
# Karşılaştırma ve Karakter Seti Hataları
# =============================================================================

catalog-collation-already-exists = '{ $name }' karşılaştırması zaten mevcut
catalog-collation-not-found = '{ $name }' karşılaştırması bulunamadı
catalog-character-set-already-exists = '{ $name }' karakter seti zaten mevcut
catalog-character-set-not-found = '{ $name }' karakter seti bulunamadı
catalog-translation-already-exists = '{ $name }' çevirisi zaten mevcut
catalog-translation-not-found = '{ $name }' çevirisi bulunamadı

# =============================================================================
# Görünüm Hataları
# =============================================================================

catalog-view-already-exists = '{ $name }' görünümü zaten mevcut
catalog-view-not-found = '{ $name }' görünümü bulunamadı
catalog-view-in-use = '{ $view_name }' görünümü veya tablosu hâlâ { $count } görünüm tarafından kullanılıyor: { $views }

# =============================================================================
# Tetikleyici Hataları
# =============================================================================

catalog-trigger-already-exists = '{ $name }' tetikleyicisi zaten mevcut
catalog-trigger-not-found = '{ $name }' tetikleyicisi bulunamadı

# =============================================================================
# Onaylama Hataları
# =============================================================================

catalog-assertion-already-exists = '{ $name }' onaylaması zaten mevcut
catalog-assertion-not-found = '{ $name }' onaylaması bulunamadı

# =============================================================================
# Fonksiyon ve Prosedür Hataları
# =============================================================================

catalog-function-already-exists = '{ $name }' fonksiyonu zaten mevcut
catalog-function-not-found = '{ $name }' fonksiyonu bulunamadı
catalog-procedure-already-exists = '{ $name }' prosedürü zaten mevcut
catalog-procedure-not-found = '{ $name }' prosedürü bulunamadı

# =============================================================================
# Kısıtlama Hataları
# =============================================================================

catalog-constraint-already-exists = '{ $name }' kısıtlaması zaten mevcut
catalog-constraint-not-found = '{ $name }' kısıtlaması bulunamadı

# =============================================================================
# Dizin Hataları
# =============================================================================

catalog-index-already-exists = '{ $table_name }' tablosunda '{ $index_name }' dizini zaten mevcut
catalog-index-not-found = '{ $table_name }' tablosunda '{ $index_name }' dizini bulunamadı

# =============================================================================
# Yabancı Anahtar Hataları
# =============================================================================

catalog-circular-foreign-key = '{ $table_name }' tablosu için döngüsel yabancı anahtar bağımlılığı algılandı: { $message }
