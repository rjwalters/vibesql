# VibeSQL CLI Localization - Thai (ภาษาไทย)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - ฐานข้อมูลที่รองรับ SQL:1999 FULL Compliance
cli-help-hint = พิมพ์ \help เพื่อดูความช่วยเหลือ, \quit เพื่อออก
cli-goodbye = ลาก่อน!

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - ฐานข้อมูลที่รองรับ SQL:1999 FULL Compliance

cli-long-about = อินเทอร์เฟซบรรทัดคำสั่ง VibeSQL

    โหมดการใช้งาน:
      REPL แบบโต้ตอบ:     vibesql (--database <FILE>)
      รันคำสั่ง:           vibesql -c "SELECT * FROM users"
      รันไฟล์:            vibesql -f script.sql
      รันจาก stdin:       cat data.sql | vibesql
      สร้างประเภท:        vibesql codegen --schema schema.sql --output types.ts

    REPL แบบโต้ตอบ:
      เมื่อเริ่มต้นโดยไม่มี -c, -f, หรือ piped input, VibeSQL จะเข้าสู่ REPL แบบโต้ตอบ
      พร้อมการรองรับ readline, ประวัติคำสั่ง และ meta-commands เช่น:
        \d (table)  - อธิบายตารางหรือแสดงรายการตารางทั้งหมด
        \dt         - แสดงรายการตาราง
        \f <format> - ตั้งค่ารูปแบบผลลัพธ์
        \copy       - นำเข้า/ส่งออก CSV/JSON
        \help       - แสดงคำสั่ง REPL ทั้งหมด

    SUBCOMMANDS:
      codegen           สร้างประเภท TypeScript จาก schema ฐานข้อมูล

    การกำหนดค่า:
      ตั้งค่าได้ใน ~/.vibesqlrc (รูปแบบ TOML)
      ส่วน: display, database, history, query

    ตัวอย่าง:
      # เริ่ม REPL แบบโต้ตอบกับฐานข้อมูลในหน่วยความจำ
      vibesql

      # ใช้ไฟล์ฐานข้อมูลแบบถาวร
      vibesql --database mydata.db

      # รันคำสั่งเดียว
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # รันไฟล์สคริปต์ SQL
      vibesql -f schema.sql -v

      # นำเข้าข้อมูลจาก CSV
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # ส่งออกผลลัพธ์เป็น JSON
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # สร้างประเภท TypeScript จากไฟล์ schema
      vibesql codegen --schema schema.sql --output src/types.ts

      # สร้างประเภท TypeScript จากฐานข้อมูลที่ทำงานอยู่
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = พาธไฟล์ฐานข้อมูล (หากไม่ระบุ จะใช้ฐานข้อมูลในหน่วยความจำ)
arg-file-help = รันคำสั่ง SQL จากไฟล์
arg-command-help = รันคำสั่ง SQL โดยตรงและออก
arg-stdin-help = อ่านคำสั่ง SQL จาก stdin (ตรวจจับอัตโนมัติเมื่อ piped)
arg-verbose-help = แสดงผลลัพธ์ละเอียดระหว่างการรันไฟล์/stdin
arg-format-help = รูปแบบผลลัพธ์สำหรับการ query
arg-lang-help = ตั้งค่าภาษาที่แสดง (เช่น en-US, es, ja, th)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = สร้างประเภท TypeScript จาก schema ฐานข้อมูล

codegen-long-about = สร้างคำจำกัดความประเภท TypeScript จาก schema ฐานข้อมูล VibeSQL

    คำสั่งนี้สร้าง TypeScript interfaces สำหรับตารางทั้งหมดในฐานข้อมูล
    พร้อมกับ metadata objects สำหรับการตรวจสอบประเภทและการรองรับ IDE

    แหล่งข้อมูล:
      --database <FILE>  สร้างจากไฟล์ฐานข้อมูลที่มีอยู่
      --schema <FILE>    สร้างจากไฟล์ SQL schema (คำสั่ง CREATE TABLE)

    ผลลัพธ์:
      --output <FILE>    เขียนประเภทที่สร้างไปยังไฟล์นี้ (ค่าเริ่มต้น: types.ts)

    ตัวเลือก:
      --camel-case       แปลงชื่อคอลัมน์เป็น camelCase
      --no-metadata      ข้ามการสร้าง tables metadata object

    ตัวอย่าง:
      # จากไฟล์ฐานข้อมูล
      vibesql codegen --database mydata.db --output src/db/types.ts

      # จากไฟล์ SQL schema
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # กับชื่อ property เป็น camelCase
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = ไฟล์ SQL schema ที่มีคำสั่ง CREATE TABLE
codegen-output-help = พาธไฟล์ผลลัพธ์สำหรับ TypeScript ที่สร้าง
codegen-camel-case-help = แปลงชื่อคอลัมน์เป็น camelCase
codegen-no-metadata-help = ข้ามการสร้าง table metadata object

codegen-from-schema = กำลังสร้างประเภท TypeScript จากไฟล์ schema: { $path }
codegen-from-database = กำลังสร้างประเภท TypeScript จากฐานข้อมูล: { $path }
codegen-written = เขียนประเภท TypeScript ไปยัง: { $path }
codegen-error-no-source = ต้องระบุ --database หรือ --schema
    ใช้ 'vibesql codegen --help' สำหรับข้อมูลการใช้งาน

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = Meta-commands:
help-describe = \d (table)      - อธิบายตารางหรือแสดงรายการตารางทั้งหมด
help-tables = \dt             - แสดงรายการตาราง
help-schemas = \ds             - แสดงรายการ schemas
help-indexes = \di             - แสดงรายการ indexes
help-roles = \du             - แสดงรายการ roles/users
help-format = \f <format>     - ตั้งค่ารูปแบบผลลัพธ์ (table, json, csv, markdown, html)
help-timing = \timing         - สลับการแสดงเวลา query
help-copy-to = \copy <table> TO <file>   - ส่งออกตารางไปยังไฟล์ CSV/JSON
help-copy-from = \copy <table> FROM <file> - นำเข้าไฟล์ CSV ไปยังตาราง
help-save = \save (file)    - บันทึกฐานข้อมูลเป็น SQL dump file
help-errors = \errors         - แสดงประวัติข้อผิดพลาดล่าสุด
help-help = \h, \help      - แสดงความช่วยเหลือนี้
help-quit = \q, \quit      - ออก

help-sql-title = SQL Introspection:
help-show-tables = SHOW TABLES                  - แสดงรายการตารางทั้งหมด
help-show-databases = SHOW DATABASES               - แสดงรายการ schemas/databases ทั้งหมด
help-show-columns = SHOW COLUMNS FROM <table>    - แสดงคอลัมน์ของตาราง
help-show-index = SHOW INDEX FROM <table>      - แสดง indexes ของตาราง
help-show-create = SHOW CREATE TABLE <table>    - แสดงคำสั่ง CREATE TABLE
help-describe-sql = DESCRIBE <table>             - ชื่อย่อของ SHOW COLUMNS

help-examples-title = ตัวอย่าง:
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

format-changed = ตั้งค่ารูปแบบผลลัพธ์เป็น: { $format }
database-saved = บันทึกฐานข้อมูลไปยัง: { $path }
no-database-file = ข้อผิดพลาด: ไม่ได้ระบุไฟล์ฐานข้อมูล ใช้ \save <filename> หรือเริ่มต้นด้วย --database flag

# =============================================================================
# Error Display
# =============================================================================

no-errors = ไม่มีข้อผิดพลาดใน session นี้
recent-errors = ข้อผิดพลาดล่าสุด:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = ไม่พบคำสั่ง SQL ในสคริปต์
script-executing = กำลังรันคำสั่งที่ { $current } จาก { $total }...
script-error = ข้อผิดพลาดในการรันคำสั่งที่ { $index }: { $error }
script-summary-title = === สรุปการรันสคริปต์ ===
script-total = คำสั่งทั้งหมด: { $count }
script-successful = สำเร็จ: { $count }
script-failed = ล้มเหลว: { $count }
script-failed-error = { $count } คำสั่งล้มเหลว

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } แถวในผลลัพธ์ ({ $time } วินาที)
rows-count = { $count } แถว

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = คำเตือน: ไม่สามารถโหลดไฟล์การตั้งค่า: { $error }
warning-auto-save-failed = คำเตือน: การบันทึกอัตโนมัติล้มเหลว: { $error }
warning-save-on-exit-failed = คำเตือน: การบันทึกเมื่อออกล้มเหลว: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = ไม่สามารถอ่านไฟล์ '{ $path }': { $error }
stdin-read-error = ไม่สามารถอ่านจาก stdin: { $error }
database-load-error = ไม่สามารถโหลดฐานข้อมูล: { $error }
