# VibeSQL CLI Localization - Indonesian (id)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - Database dengan Kepatuhan PENUH SQL:1999
cli-help-hint = Ketik \help untuk bantuan, \quit untuk keluar
cli-goodbye = Sampai jumpa!

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - Database dengan Kepatuhan PENUH SQL:1999

cli-long-about = Antarmuka baris perintah VibeSQL

    MODE PENGGUNAAN:
      REPL interaktif:       vibesql (--database <FILE>)
      Jalankan perintah:     vibesql -c "SELECT * FROM users"
      Jalankan file:         vibesql -f script.sql
      Jalankan dari stdin:   cat data.sql | vibesql
      Hasilkan tipe:         vibesql codegen --schema schema.sql --output types.ts

    REPL INTERAKTIF:
      Ketika dijalankan tanpa -c, -f, atau input pipa, VibeSQL masuk ke REPL
      interaktif dengan dukungan readline, riwayat perintah, dan meta-perintah seperti:
        \d (tabel)    - Jelaskan tabel atau daftar semua tabel
        \dt           - Daftar tabel
        \f <format>   - Atur format output
        \copy         - Impor/ekspor CSV/JSON
        \help         - Tampilkan semua perintah REPL

    SUBPERINTAH:
      codegen           Hasilkan tipe TypeScript dari skema database

    KONFIGURASI:
      Pengaturan dapat dikonfigurasi di ~/.vibesqlrc (format TOML).
      Bagian: display, database, history, query

    CONTOH:
      # Mulai REPL interaktif dengan database di memori
      vibesql

      # Gunakan file database persisten
      vibesql --database mydata.db

      # Jalankan perintah tunggal
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # Jalankan file skrip SQL
      vibesql -f schema.sql -v

      # Impor data dari CSV
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # Ekspor hasil query sebagai JSON
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # Hasilkan tipe TypeScript dari file skema
      vibesql codegen --schema schema.sql --output src/types.ts

      # Hasilkan tipe TypeScript dari database yang berjalan
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = Path file database (jika tidak ditentukan, gunakan database di memori)
arg-file-help = Jalankan perintah SQL dari file
arg-command-help = Jalankan perintah SQL langsung dan keluar
arg-stdin-help = Baca perintah SQL dari stdin (terdeteksi otomatis saat di-pipe)
arg-verbose-help = Tampilkan output detail selama eksekusi file/stdin
arg-format-help = Format output untuk hasil query
arg-lang-help = Atur bahasa tampilan (mis. en-US, es, ja)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = Hasilkan tipe TypeScript dari skema database

codegen-long-about = Hasilkan definisi tipe TypeScript dari skema database VibeSQL.

    Perintah ini membuat antarmuka TypeScript untuk semua tabel di database,
    bersama dengan objek metadata untuk pemeriksaan tipe runtime dan dukungan IDE.

    SUMBER INPUT:
      --database <FILE>  Hasilkan dari file database yang ada
      --schema <FILE>    Hasilkan dari file skema SQL (pernyataan CREATE TABLE)

    OUTPUT:
      --output <FILE>    Tulis tipe yang dihasilkan ke file ini (default: types.ts)

    OPSI:
      --camel-case       Konversi nama kolom ke camelCase
      --no-metadata      Lewati pembuatan objek metadata tabel

    CONTOH:
      # Dari file database
      vibesql codegen --database mydata.db --output src/db/types.ts

      # Dari file skema SQL
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # Dengan nama properti camelCase
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = File skema SQL yang berisi pernyataan CREATE TABLE
codegen-output-help = Path file output untuk TypeScript yang dihasilkan
codegen-camel-case-help = Konversi nama kolom ke camelCase
codegen-no-metadata-help = Lewati pembuatan objek metadata tabel

codegen-from-schema = Menghasilkan tipe TypeScript dari file skema: { $path }
codegen-from-database = Menghasilkan tipe TypeScript dari database: { $path }
codegen-written = Tipe TypeScript ditulis ke: { $path }
codegen-error-no-source = Harus menentukan --database atau --schema.
    Gunakan 'vibesql codegen --help' untuk informasi penggunaan.

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = Meta-perintah:
help-describe = \d (tabel)      - Jelaskan tabel atau daftar semua tabel
help-tables = \dt             - Daftar tabel
help-schemas = \ds             - Daftar skema
help-indexes = \di             - Daftar indeks
help-roles = \du             - Daftar peran/pengguna
help-format = \f <format>     - Atur format output (table, json, csv, markdown, html)
help-timing = \timing         - Aktifkan/nonaktifkan pengukuran waktu query
help-copy-to = \copy <tabel> TO <file>   - Ekspor tabel ke file CSV/JSON
help-copy-from = \copy <tabel> FROM <file> - Impor file CSV ke tabel
help-save = \save (file)    - Simpan database ke file dump SQL
help-errors = \errors         - Tampilkan riwayat kesalahan terkini
help-help = \h, \help      - Tampilkan bantuan ini
help-quit = \q, \quit      - Keluar

help-sql-title = Introspeksi SQL:
help-show-tables = SHOW TABLES                  - Daftar semua tabel
help-show-databases = SHOW DATABASES               - Daftar semua skema/database
help-show-columns = SHOW COLUMNS FROM <tabel>    - Tampilkan kolom tabel
help-show-index = SHOW INDEX FROM <tabel>      - Tampilkan indeks tabel
help-show-create = SHOW CREATE TABLE <tabel>    - Tampilkan pernyataan CREATE TABLE
help-describe-sql = DESCRIBE <tabel>             - Alias untuk SHOW COLUMNS

help-examples-title = Contoh:
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
# Status Messages
# =============================================================================

format-changed = Format output diatur ke: { $format }
database-saved = Database disimpan ke: { $path }
no-database-file = Kesalahan: Tidak ada file database yang ditentukan. Gunakan \save <namafile> atau mulai dengan flag --database

# =============================================================================
# Error Display
# =============================================================================

no-errors = Tidak ada kesalahan dalam sesi ini.
recent-errors = Kesalahan terkini:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = Tidak ada pernyataan SQL yang ditemukan dalam skrip
script-executing = Menjalankan pernyataan { $current } dari { $total }...
script-error = Kesalahan menjalankan pernyataan { $index }: { $error }
script-summary-title = === Ringkasan Eksekusi Skrip ===
script-total = Total pernyataan: { $count }
script-successful = Berhasil: { $count }
script-failed = Gagal: { $count }
script-failed-error = { $count } pernyataan gagal

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } baris dalam hasil ({ $time }s)
rows-count = { $count } baris

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = Peringatan: Tidak dapat memuat file konfigurasi: { $error }
warning-auto-save-failed = Peringatan: Gagal menyimpan database secara otomatis: { $error }
warning-save-on-exit-failed = Peringatan: Gagal menyimpan database saat keluar: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = Gagal membaca file '{ $path }': { $error }
stdin-read-error = Gagal membaca dari stdin: { $error }
database-load-error = Gagal memuat database: { $error }
