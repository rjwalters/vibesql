# VibeSQL CLI Localization - Vietnamese (Tiếng Việt)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - Cơ sở dữ liệu tuân thủ SQL:1999 FULL
cli-help-hint = Gõ \help để xem trợ giúp, \quit để thoát
cli-goodbye = Tạm biệt!

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - Cơ sở dữ liệu tuân thủ SQL:1999 FULL

cli-long-about = Giao diện dòng lệnh VibeSQL

    CHẾ ĐỘ SỬ DỤNG:
      REPL tương tác:         vibesql (--database <FILE>)
      Thực thi lệnh:          vibesql -c "SELECT * FROM users"
      Thực thi file:          vibesql -f script.sql
      Thực thi từ stdin:      cat data.sql | vibesql
      Tạo kiểu TypeScript:    vibesql codegen --schema schema.sql --output types.ts

    REPL TƯƠNG TÁC:
      Khi khởi động không có -c, -f, hoặc input qua pipe, VibeSQL vào chế độ REPL
      tương tác với hỗ trợ readline, lịch sử lệnh và các meta-command như:
        \d (table)  - Mô tả bảng hoặc liệt kê tất cả bảng
        \dt         - Liệt kê các bảng
        \f <format> - Đặt định dạng đầu ra
        \copy       - Nhập/xuất CSV/JSON
        \help       - Hiển thị tất cả lệnh REPL

    SUBCOMMANDS:
      codegen           Tạo kiểu TypeScript từ schema cơ sở dữ liệu

    CẤU HÌNH:
      Cài đặt có thể được cấu hình trong ~/.vibesqlrc (định dạng TOML).
      Các phần: display, database, history, query

    VÍ DỤ:
      # Khởi động REPL tương tác với cơ sở dữ liệu trong bộ nhớ
      vibesql

      # Sử dụng file cơ sở dữ liệu lưu trữ
      vibesql --database mydata.db

      # Thực thi một lệnh đơn
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # Chạy file script SQL
      vibesql -f schema.sql -v

      # Nhập dữ liệu từ CSV
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # Xuất kết quả truy vấn dạng JSON
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # Tạo kiểu TypeScript từ file schema
      vibesql codegen --schema schema.sql --output src/types.ts

      # Tạo kiểu TypeScript từ cơ sở dữ liệu đang chạy
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = Đường dẫn file cơ sở dữ liệu (nếu không chỉ định, sử dụng cơ sở dữ liệu trong bộ nhớ)
arg-file-help = Thực thi lệnh SQL từ file
arg-command-help = Thực thi lệnh SQL trực tiếp và thoát
arg-stdin-help = Đọc lệnh SQL từ stdin (tự động phát hiện khi pipe)
arg-verbose-help = Hiển thị đầu ra chi tiết khi thực thi file/stdin
arg-format-help = Định dạng đầu ra cho kết quả truy vấn
arg-lang-help = Đặt ngôn ngữ hiển thị (ví dụ: en-US, es, ja, vi)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = Tạo kiểu TypeScript từ schema cơ sở dữ liệu

codegen-long-about = Tạo định nghĩa kiểu TypeScript từ schema cơ sở dữ liệu VibeSQL.

    Lệnh này tạo các interface TypeScript cho tất cả các bảng trong cơ sở dữ liệu,
    cùng với các đối tượng metadata để kiểm tra kiểu runtime và hỗ trợ IDE.

    NGUỒN ĐẦU VÀO:
      --database <FILE>  Tạo từ file cơ sở dữ liệu hiện có
      --schema <FILE>    Tạo từ file schema SQL (câu lệnh CREATE TABLE)

    ĐẦU RA:
      --output <FILE>    Ghi các kiểu được tạo vào file này (mặc định: types.ts)

    TÙY CHỌN:
      --camel-case       Chuyển tên cột sang camelCase
      --no-metadata      Bỏ qua tạo đối tượng metadata bảng

    VÍ DỤ:
      # Từ file cơ sở dữ liệu
      vibesql codegen --database mydata.db --output src/db/types.ts

      # Từ file schema SQL
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # Với tên thuộc tính camelCase
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = File schema SQL chứa các câu lệnh CREATE TABLE
codegen-output-help = Đường dẫn file đầu ra cho TypeScript được tạo
codegen-camel-case-help = Chuyển tên cột sang camelCase
codegen-no-metadata-help = Bỏ qua tạo đối tượng metadata bảng

codegen-from-schema = Đang tạo kiểu TypeScript từ file schema: { $path }
codegen-from-database = Đang tạo kiểu TypeScript từ cơ sở dữ liệu: { $path }
codegen-written = Đã ghi kiểu TypeScript vào: { $path }
codegen-error-no-source = Phải chỉ định --database hoặc --schema.
    Sử dụng 'vibesql codegen --help' để xem thông tin sử dụng.

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = Meta-commands:
help-describe = \d (table)      - Mô tả bảng hoặc liệt kê tất cả bảng
help-tables = \dt             - Liệt kê các bảng
help-schemas = \ds             - Liệt kê các schema
help-indexes = \di             - Liệt kê các index
help-roles = \du             - Liệt kê các vai trò/người dùng
help-format = \f <format>     - Đặt định dạng đầu ra (table, json, csv, markdown, html)
help-timing = \timing         - Bật/tắt đo thời gian truy vấn
help-copy-to = \copy <table> TO <file>   - Xuất bảng ra file CSV/JSON
help-copy-from = \copy <table> FROM <file> - Nhập file CSV vào bảng
help-save = \save (file)    - Lưu cơ sở dữ liệu ra file SQL dump
help-errors = \errors         - Hiển thị lịch sử lỗi gần đây
help-help = \h, \help      - Hiển thị trợ giúp này
help-quit = \q, \quit      - Thoát

help-sql-title = Truy vấn SQL:
help-show-tables = SHOW TABLES                  - Liệt kê tất cả bảng
help-show-databases = SHOW DATABASES               - Liệt kê tất cả schema/database
help-show-columns = SHOW COLUMNS FROM <table>    - Hiển thị các cột của bảng
help-show-index = SHOW INDEX FROM <table>      - Hiển thị các index của bảng
help-show-create = SHOW CREATE TABLE <table>    - Hiển thị câu lệnh CREATE TABLE
help-describe-sql = DESCRIBE <table>             - Tương đương SHOW COLUMNS

help-examples-title = Ví dụ:
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

format-changed = Đã đặt định dạng đầu ra thành: { $format }
database-saved = Đã lưu cơ sở dữ liệu vào: { $path }
no-database-file = Lỗi: Chưa chỉ định file cơ sở dữ liệu. Sử dụng \save <filename> hoặc khởi động với cờ --database

# =============================================================================
# Error Display
# =============================================================================

no-errors = Không có lỗi trong phiên này.
recent-errors = Các lỗi gần đây:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = Không tìm thấy câu lệnh SQL trong script
script-executing = Đang thực thi câu lệnh { $current } trên { $total }...
script-error = Lỗi khi thực thi câu lệnh { $index }: { $error }
script-summary-title = === Tóm tắt thực thi Script ===
script-total = Tổng số câu lệnh: { $count }
script-successful = Thành công: { $count }
script-failed = Thất bại: { $count }
script-failed-error = { $count } câu lệnh thất bại

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } hàng trong tập kết quả ({ $time }s)
rows-count = { $count } hàng

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = Cảnh báo: Không thể tải file cấu hình: { $error }
warning-auto-save-failed = Cảnh báo: Không thể tự động lưu cơ sở dữ liệu: { $error }
warning-save-on-exit-failed = Cảnh báo: Không thể lưu cơ sở dữ liệu khi thoát: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = Không thể đọc file '{ $path }': { $error }
stdin-read-error = Không thể đọc từ stdin: { $error }
database-load-error = Không thể tải cơ sở dữ liệu: { $error }
