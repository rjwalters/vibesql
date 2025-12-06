# VibeSQL Depolama Hata Mesajları - Türkçe
# Bu dosya, vibesql-storage crate'i için tüm hata mesajlarını içerir.

# =============================================================================
# Tablo Hataları
# =============================================================================

storage-table-not-found = '{ $name }' tablosu bulunamadı

# =============================================================================
# Sütun Hataları
# =============================================================================

storage-column-count-mismatch = Sütun sayısı uyuşmazlığı: beklenen { $expected }, verilen { $actual }
storage-column-index-out-of-bounds = Sütun dizini { $index } sınır dışında
storage-column-not-found = '{ $table_name }' tablosunda '{ $column_name }' sütunu bulunamadı

# =============================================================================
# Dizin Hataları
# =============================================================================

storage-index-already-exists = '{ $name }' dizini zaten mevcut
storage-index-not-found = '{ $name }' dizini bulunamadı
storage-invalid-index-column = { $message }

# =============================================================================
# Kısıtlama Hataları
# =============================================================================

storage-null-constraint-violation = NOT NULL kısıtlaması ihlali: '{ $column }' sütunu NULL olamaz
storage-unique-constraint-violation = { $message }

# =============================================================================
# Tip Hataları
# =============================================================================

storage-type-mismatch = '{ $column }' sütununda tip uyuşmazlığı: beklenen { $expected }, verilen { $actual }

# =============================================================================
# İşlem ve Katalog Hataları
# =============================================================================

storage-catalog-error = Katalog hatası: { $message }
storage-transaction-error = İşlem hatası: { $message }
storage-row-not-found = Satır bulunamadı

# =============================================================================
# G/Ç ve Sayfa Hataları
# =============================================================================

storage-io-error = G/Ç hatası: { $message }
storage-invalid-page-size = Geçersiz sayfa boyutu: beklenen { $expected }, verilen { $actual }
storage-invalid-page-id = Geçersiz sayfa kimliği: { $page_id }
storage-lock-error = Kilit hatası: { $message }

# =============================================================================
# Bellek Hataları
# =============================================================================

storage-memory-budget-exceeded = Bellek bütçesi aşıldı: { $used } bayt kullanılıyor, bütçe { $budget } bayt
storage-no-index-to-evict = Çıkarılacak dizin yok (tüm dizinler zaten disk destekli)

# =============================================================================
# Genel Hatalar
# =============================================================================

storage-not-implemented = Uygulanmadı: { $message }
storage-other = { $message }
