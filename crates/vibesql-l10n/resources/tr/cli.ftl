# VibeSQL CLI Yerelleştirme - Türkçe
# Bu dosya, VibeSQL komut satırı arayüzü için tüm kullanıcı odaklı metinleri içerir.

# =============================================================================
# REPL Banner ve Temel Mesajlar
# =============================================================================

cli-banner = VibeSQL v{ $version } - SQL:1999 TAM Uyumluluk Veritabanı
cli-help-hint = Yardım için \help, çıkmak için \quit yazın
cli-goodbye = Hoşça kalın!

# =============================================================================
# Komut Yardım Metinleri (Clap Argümanları)
# =============================================================================

cli-about = VibeSQL - SQL:1999 TAM Uyumluluk Veritabanı

cli-long-about = VibeSQL komut satırı arayüzü

    KULLANIM MODLARI:
      Etkileşimli REPL:     vibesql (--database <DOSYA>)
      Komut Çalıştırma:     vibesql -c "SELECT * FROM users"
      Dosya Çalıştırma:     vibesql -f script.sql
      stdin'den Çalıştırma: cat data.sql | vibesql
      Tip Oluşturma:        vibesql codegen --schema schema.sql --output types.ts

    ETKİLEŞİMLİ REPL:
      -c, -f veya yönlendirilmiş girdi olmadan başlatıldığında, VibeSQL readline
      desteği, komut geçmişi ve aşağıdaki gibi meta komutlarla etkileşimli bir
      REPL'e girer:
        \d (tablo)  - Tabloyu tanımla veya tüm tabloları listele
        \dt         - Tabloları listele
        \f <format> - Çıktı formatını ayarla
        \copy       - CSV/JSON içe/dışa aktar
        \help       - Tüm REPL komutlarını göster

    ALT KOMUTLAR:
      codegen           Veritabanı şemasından TypeScript tipleri oluştur

    YAPILANDIRMA:
      Ayarlar ~/.vibesqlrc dosyasında (TOML formatı) yapılandırılabilir.
      Bölümler: display, database, history, query

    ÖRNEKLER:
      # Bellek içi veritabanı ile etkileşimli REPL başlat
      vibesql

      # Kalıcı veritabanı dosyası kullan
      vibesql --database mydata.db

      # Tek komut çalıştır
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # SQL betik dosyası çalıştır
      vibesql -f schema.sql -v

      # CSV'den veri içe aktar
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # Sorgu sonuçlarını JSON olarak dışa aktar
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # Şema dosyasından TypeScript tipleri oluştur
      vibesql codegen --schema schema.sql --output src/types.ts

      # Çalışan veritabanından TypeScript tipleri oluştur
      vibesql codegen --database mydata.db --output src/types.ts

# Argüman yardım metinleri
arg-database-help = Veritabanı dosya yolu (belirtilmezse bellek içi veritabanı kullanılır)
arg-file-help = Dosyadan SQL komutlarını çalıştır
arg-command-help = SQL komutunu doğrudan çalıştır ve çık
arg-stdin-help = stdin'den SQL komutlarını oku (yönlendirildiğinde otomatik algılanır)
arg-verbose-help = Dosya/stdin çalıştırma sırasında ayrıntılı çıktı göster
arg-format-help = Sorgu sonuçları için çıktı formatı
arg-lang-help = Görüntüleme dilini ayarla (ör. en-US, es, ja)

# =============================================================================
# Codegen Alt Komutu
# =============================================================================

codegen-about = Veritabanı şemasından TypeScript tipleri oluştur

codegen-long-about = Bir VibeSQL veritabanı şemasından TypeScript tip tanımları oluşturur.

    Bu komut, veritabanındaki tüm tablolar için TypeScript arayüzleri ve
    çalışma zamanı tip kontrolü ve IDE desteği için meta veri nesneleri oluşturur.

    GİRDİ KAYNAKLARI:
      --database <DOSYA>  Mevcut veritabanı dosyasından oluştur
      --schema <DOSYA>    SQL şema dosyasından oluştur (CREATE TABLE ifadeleri)

    ÇIKTI:
      --output <DOSYA>    Oluşturulan tipleri bu dosyaya yaz (varsayılan: types.ts)

    SEÇENEKLER:
      --camel-case        Sütun adlarını camelCase'e dönüştür
      --no-metadata       Tablolar meta veri nesnesini oluşturma

    ÖRNEKLER:
      # Veritabanı dosyasından
      vibesql codegen --database mydata.db --output src/db/types.ts

      # SQL şema dosyasından
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # camelCase özellik adlarıyla
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = CREATE TABLE ifadeleri içeren SQL şema dosyası
codegen-output-help = Oluşturulan TypeScript için çıktı dosya yolu
codegen-camel-case-help = Sütun adlarını camelCase'e dönüştür
codegen-no-metadata-help = Tablo meta veri nesnesini oluşturmayı atla

codegen-from-schema = Şema dosyasından TypeScript tipleri oluşturuluyor: { $path }
codegen-from-database = Veritabanından TypeScript tipleri oluşturuluyor: { $path }
codegen-written = TypeScript tipleri şuraya yazıldı: { $path }
codegen-error-no-source = --database veya --schema belirtilmelidir.
    Kullanım bilgisi için 'vibesql codegen --help' kullanın.

# =============================================================================
# Meta Komutlar Yardımı (\help çıktısı)
# =============================================================================

help-title = Meta komutlar:
help-describe = \d (tablo)      - Tabloyu tanımla veya tüm tabloları listele
help-tables = \dt             - Tabloları listele
help-schemas = \ds             - Şemaları listele
help-indexes = \di             - Dizinleri listele
help-roles = \du             - Rolleri/kullanıcıları listele
help-format = \f <format>     - Çıktı formatını ayarla (table, json, csv, markdown, html)
help-timing = \timing         - Sorgu zamanlamasını aç/kapat
help-copy-to = \copy <tablo> TO <dosya>   - Tabloyu CSV/JSON dosyasına dışa aktar
help-copy-from = \copy <tablo> FROM <dosya> - CSV dosyasını tabloya içe aktar
help-save = \save (dosya)   - Veritabanını SQL döküm dosyasına kaydet
help-errors = \errors         - Son hata geçmişini göster
help-help = \h, \help      - Bu yardımı göster
help-quit = \q, \quit      - Çık

help-sql-title = SQL İnceleme:
help-show-tables = SHOW TABLES                  - Tüm tabloları listele
help-show-databases = SHOW DATABASES               - Tüm şemaları/veritabanlarını listele
help-show-columns = SHOW COLUMNS FROM <tablo>    - Tablo sütunlarını göster
help-show-index = SHOW INDEX FROM <tablo>      - Tablo dizinlerini göster
help-show-create = SHOW CREATE TABLE <tablo>    - CREATE TABLE ifadesini göster
help-describe-sql = DESCRIBE <tablo>             - SHOW COLUMNS için takma ad

help-examples-title = Örnekler:
help-example-create = CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));
help-example-insert = INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
help-example-select = SELECT * FROM users;
help-example-show-tables = SHOW TABLES;
help-example-show-columns = SHOW COLUMNS FROM users;
help-example-describe = DESCRIBE users;
help-example-format-json = \f json
help-example-format-md = \f markdown
help-example-copy-to = \copy users TO '/tmp/users.csv'
help-example-copy-from = \copy users FROM '/tmp/users.csv'
help-example-copy-json = \copy users TO '/tmp/users.json'
help-example-errors = \errors

# =============================================================================
# Durum Mesajları
# =============================================================================

format-changed = Çıktı formatı ayarlandı: { $format }
database-saved = Veritabanı kaydedildi: { $path }
no-database-file = Hata: Veritabanı dosyası belirtilmedi. \save <dosyaadı> kullanın veya --database bayrağı ile başlatın

# =============================================================================
# Hata Görüntüleme
# =============================================================================

no-errors = Bu oturumda hata yok.
recent-errors = Son hatalar:

# =============================================================================
# Betik Çalıştırma Mesajları
# =============================================================================

script-no-statements = Betikte SQL ifadesi bulunamadı
script-executing = İfade { $current } / { $total } çalıştırılıyor...
script-error = İfade { $index } çalıştırılırken hata: { $error }
script-summary-title = === Betik Çalıştırma Özeti ===
script-total = Toplam ifade: { $count }
script-successful = Başarılı: { $count }
script-failed = Başarısız: { $count }
script-failed-error = { $count } ifade başarısız oldu

# =============================================================================
# Çıktı Biçimlendirme
# =============================================================================

rows-with-time = { $count } satır ({ $time }s)
rows-count = { $count } satır

# =============================================================================
# Uyarılar
# =============================================================================

warning-config-load = Uyarı: Yapılandırma dosyası yüklenemedi: { $error }
warning-auto-save-failed = Uyarı: Veritabanı otomatik kaydetme başarısız: { $error }
warning-save-on-exit-failed = Uyarı: Çıkışta veritabanı kaydetme başarısız: { $error }

# =============================================================================
# Dosya İşlemleri
# =============================================================================

file-read-error = '{ $path }' dosyası okunamadı: { $error }
stdin-read-error = stdin'den okunamadı: { $error }
database-load-error = Veritabanı yüklenemedi: { $error }
