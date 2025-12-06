# VibeSQL Yürütücü Hata Mesajları - Türkçe
# Bu dosya, vibesql-executor crate'i için tüm hata mesajlarını içerir.

# =============================================================================
# Tablo Hataları
# =============================================================================

executor-table-not-found = '{ $name }' tablosu bulunamadı
executor-table-already-exists = '{ $name }' tablosu zaten mevcut

# =============================================================================
# Sütun Hataları
# =============================================================================

executor-column-not-found-simple = '{ $table_name }' tablosunda '{ $column_name }' sütunu bulunamadı
executor-column-not-found-searched = '{ $column_name }' sütunu bulunamadı (aranan tablolar: { $searched_tables })
executor-column-not-found-with-available = '{ $column_name }' sütunu bulunamadı (aranan tablolar: { $searched_tables }). Mevcut sütunlar: { $available_columns }
executor-invalid-table-qualifier = '{ $column }' sütunu için geçersiz tablo niteleyicisi '{ $qualifier }'. Mevcut tablolar: { $available_tables }
executor-column-already-exists = '{ $name }' sütunu zaten mevcut
executor-column-index-out-of-bounds = Sütun dizini { $index } sınır dışında

# =============================================================================
# Dizin Hataları
# =============================================================================

executor-index-not-found = '{ $name }' dizini bulunamadı
executor-index-already-exists = '{ $name }' dizini zaten mevcut
executor-invalid-index-definition = Geçersiz dizin tanımı: { $message }

# =============================================================================
# Tetikleyici Hataları
# =============================================================================

executor-trigger-not-found = '{ $name }' tetikleyicisi bulunamadı
executor-trigger-already-exists = '{ $name }' tetikleyicisi zaten mevcut

# =============================================================================
# Şema Hataları
# =============================================================================

executor-schema-not-found = '{ $name }' şeması bulunamadı
executor-schema-already-exists = '{ $name }' şeması zaten mevcut
executor-schema-not-empty = '{ $name }' şeması silinemez: şema boş değil

# =============================================================================
# Rol ve İzin Hataları
# =============================================================================

executor-role-not-found = '{ $name }' rolü bulunamadı
executor-permission-denied = İzin reddedildi: '{ $role }' rolü { $object } üzerinde { $privilege } yetkisine sahip değil
executor-dependent-privileges-exist = Bağımlı yetkiler mevcut: { $message }

# =============================================================================
# Tip Hataları
# =============================================================================

executor-type-not-found = '{ $name }' tipi bulunamadı
executor-type-already-exists = '{ $name }' tipi zaten mevcut
executor-type-in-use = '{ $name }' tipi silinemez: tip hâlâ kullanımda
executor-type-mismatch = Tip uyuşmazlığı: { $left } { $op } { $right }
executor-type-error = Tip hatası: { $message }
executor-cast-error = { $from_type } tipinden { $to_type } tipine dönüştürülemez
executor-type-conversion-error = { $from } değeri { $to } tipine dönüştürülemez

# =============================================================================
# İfade ve Sorgu Hataları
# =============================================================================

executor-division-by-zero = Sıfıra bölme
executor-invalid-where-clause = Geçersiz WHERE cümlesi: { $message }
executor-unsupported-expression = Desteklenmeyen ifade: { $message }
executor-unsupported-feature = Desteklenmeyen özellik: { $message }
executor-parse-error = Ayrıştırma hatası: { $message }

# =============================================================================
# Alt Sorgu Hataları
# =============================================================================

executor-subquery-returned-multiple-rows = Skaler alt sorgu { $actual } satır döndürdü, beklenen { $expected }
executor-subquery-column-count-mismatch = Alt sorgu { $actual } sütun döndürdü, beklenen { $expected }
executor-column-count-mismatch = Türetilmiş sütun listesi { $provided } sütun içeriyor ancak sorgu { $expected } sütun üretiyor

# =============================================================================
# Kısıtlama Hataları
# =============================================================================

executor-constraint-violation = Kısıtlama ihlali: { $message }
executor-multiple-primary-keys = Birden fazla PRIMARY KEY kısıtlamasına izin verilmez
executor-cannot-drop-column = Sütun silinemez: { $message }
executor-constraint-not-found = '{ $table_name }' tablosunda '{ $constraint_name }' kısıtlaması bulunamadı

# =============================================================================
# Kaynak Sınır Hataları
# =============================================================================

executor-expression-depth-exceeded = İfade derinlik sınırı aşıldı: { $depth } > { $max_depth } (yığın taşmasını önler)
executor-query-timeout-exceeded = Sorgu zaman aşımı aşıldı: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Satır işleme sınırı aşıldı: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Bellek sınırı aşıldı: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Prosedürel/Değişken Hataları
# =============================================================================

executor-variable-not-found-simple = '{ $variable_name }' değişkeni bulunamadı
executor-variable-not-found-with-available = '{ $variable_name }' değişkeni bulunamadı. Mevcut değişkenler: { $available_variables }
executor-label-not-found = '{ $name }' etiketi bulunamadı

# =============================================================================
# SELECT INTO Hataları
# =============================================================================

executor-select-into-row-count = Prosedürel SELECT INTO tam olarak { $expected } satır döndürmeli, { $actual } satır{ $plural } döndü
executor-select-into-column-count = Prosedürel SELECT INTO sütun sayısı uyuşmazlığı: { $expected } değişken{ $expected_plural } ancak sorgu { $actual } sütun{ $actual_plural } döndürdü

# =============================================================================
# Prosedür ve Fonksiyon Hataları
# =============================================================================

executor-procedure-not-found-simple = '{ $schema_name }' şemasında '{ $procedure_name }' prosedürü bulunamadı
executor-procedure-not-found-with-available = '{ $schema_name }' şemasında '{ $procedure_name }' prosedürü bulunamadı
    .available = Mevcut prosedürler: { $available_procedures }
executor-procedure-not-found-with-suggestion = '{ $schema_name }' şemasında '{ $procedure_name }' prosedürü bulunamadı
    .available = Mevcut prosedürler: { $available_procedures }
    .suggestion = '{ $suggestion }' mi demek istediniz?

executor-function-not-found-simple = '{ $schema_name }' şemasında '{ $function_name }' fonksiyonu bulunamadı
executor-function-not-found-with-available = '{ $schema_name }' şemasında '{ $function_name }' fonksiyonu bulunamadı
    .available = Mevcut fonksiyonlar: { $available_functions }
executor-function-not-found-with-suggestion = '{ $schema_name }' şemasında '{ $function_name }' fonksiyonu bulunamadı
    .available = Mevcut fonksiyonlar: { $available_functions }
    .suggestion = '{ $suggestion }' mi demek istediniz?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' { $expected } parametre{ $expected_plural } bekliyor ({ $parameter_signature }), { $actual } argüman{ $actual_plural } verildi
executor-parameter-type-mismatch = '{ $parameter_name }' parametresi { $expected_type } bekliyor, { $actual_type } '{ $actual_value }' verildi
executor-argument-count-mismatch = Argüman sayısı uyuşmazlığı: beklenen { $expected }, verilen { $actual }

executor-recursion-limit-exceeded = Maksimum özyineleme derinliği ({ $max_depth }) aşıldı: { $message }
executor-recursion-call-stack = Çağrı yığını:
executor-function-must-return = Fonksiyon bir değer döndürmelidir
executor-invalid-control-flow = Geçersiz kontrol akışı: { $message }
executor-invalid-function-body = Geçersiz fonksiyon gövdesi: { $message }
executor-function-read-only-violation = Fonksiyon salt okunur ihlali: { $message }

# =============================================================================
# EXTRACT Hataları
# =============================================================================

executor-invalid-extract-field = { $value_type } değerinden { $field } çıkarılamaz

# =============================================================================
# Sütunlu/Arrow Hataları
# =============================================================================

executor-arrow-downcast-error = Arrow dizisi { $expected_type } tipine dönüştürülemedi ({ $context })
executor-columnar-type-mismatch-binary = { $operation } için uyumsuz tipler: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = { $operation } için uyumsuz tip: { $left_type }
executor-simd-operation-failed = SIMD { $operation } başarısız oldu: { $reason }
executor-columnar-column-not-found = Sütun dizini { $column_index } sınır dışında (toplu iş { $batch_columns } sütun içeriyor)
executor-columnar-column-not-found-by-name = Sütun bulunamadı: { $column_name }
executor-columnar-length-mismatch = { $context } içinde sütun uzunluğu uyuşmazlığı: beklenen { $expected }, verilen { $actual }
executor-unsupported-array-type = { $operation } için desteklenmeyen dizi tipi: { $array_type }

# =============================================================================
# Uzamsal Hatalar
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } { $expected } bekliyor, { $actual } verildi

# =============================================================================
# İmleç Hataları
# =============================================================================

executor-cursor-already-exists = '{ $name }' imleci zaten mevcut
executor-cursor-not-found = '{ $name }' imleci bulunamadı
executor-cursor-already-open = '{ $name }' imleci zaten açık
executor-cursor-not-open = '{ $name }' imleci açık değil
executor-cursor-not-scrollable = '{ $name }' imleci kaydırılabilir değil (SCROLL belirtilmedi)

# =============================================================================
# Depolama ve Genel Hatalar
# =============================================================================

executor-storage-error = Depolama hatası: { $message }
executor-other = { $message }
