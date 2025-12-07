# VibeSQL Web UI - ไทย

# Page titles
page-title = VibeSQL - ฐานข้อมูล SQL:1999 ที่ขับเคลื่อนด้วย AI
demo-title = สาธิต VibeSQL
benchmarks-title = เกณฑ์มาตรฐานประสิทธิภาพ - VibeSQL
benchmarks-heading = VibeSQL - เกณฑ์มาตรฐานประสิทธิภาพ
conformance-title = รายงานความสอดคล้อง - VibeSQL
conformance-heading = รายงานความสอดคล้อง
conformance-subtitle = การทดสอบความสอดคล้องกับมาตรฐาน SQL:1999

# Navigation
nav-showcase = โชว์เคส SQL:1999
nav-conformance = ดูผลลัพธ์ sqltest
nav-sqllogictest = ดูผลลัพธ์ SQLLogicTest

# Editor section
editor-title = ตัวแก้ไข SQL
editor-storage = ที่เก็บข้อมูล
editor-storage-init = กำลังเริ่มต้น...
editor-execute = รันคิวรี

# Results section
results-title = ผลลัพธ์
results-empty = รันคิวรีเพื่อดูผลลัพธ์
results-loading = กำลังโหลด...
results-rows = { $count } แถว
results-rows-with-time = { $count } แถว ({ $time }ms)
results-copy = คัดลอกไปยังคลิปบอร์ด
results-export = ส่งออก CSV
results-limit-warning = แสดง { $limit } แถวแรกจากทั้งหมด { $total } แถว ใช้ LIMIT เพื่อปรับแต่งคิวรีของคุณ

# Examples sidebar
examples-title = ตัวอย่าง
examples-basic = คิวรีพื้นฐาน
examples-advanced = คิวรีขั้นสูง

# Database selector
db-select-label = ฐานข้อมูล

# Footer
footer-tagline = VibeSQL - ฐานข้อมูล SQL:1999 ใน WebAssembly
footer-deployed = ใช้งาน: { $date }

# Theme
theme-toggle-dark = เปลี่ยนเป็นโหมดมืด
theme-toggle-light = เปลี่ยนเป็นโหมดสว่าง

# Locale
locale-select = เลือกภาษา

# Messages
msg-query-success = รันคิวรีสำเร็จ
msg-rows-affected = { $count } แถวได้รับผลกระทบ

# Errors
error-generic = เกิดข้อผิดพลาด
error-query-failed = คิวรีล้มเหลว

# Editor
editor-placeholder = ป้อนคิวรี SQL ที่นี่... (Ctrl+Enter หรือ Cmd+Enter เพื่อรัน)

# Navigation links
nav-terminal = สาธิตเทอร์มินัล SQL
nav-compliance = รายงานความสอดคล้อง SQL
nav-benchmarks = เกณฑ์มาตรฐานประสิทธิภาพ
nav-github = คลัง GitHub
nav-home = หน้าแรก

# Results
results-success-zero = รันคิวรีสำเร็จ (0 แถว)
results-null = NULL

# Help Modal
help-title = ปุ่มลัดและความช่วยเหลือ
help-close = ปิด
help-editor-shortcuts = ปุ่มลัดตัวแก้ไข
help-navigation = การนำทาง
help-results-actions = การดำเนินการผลลัพธ์
help-tips = เคล็ดลับ
help-shortcut-execute = รันคิวรีปัจจุบัน
help-shortcut-comment = สลับคอมเมนต์บรรทัด
help-shortcut-indent = เยื้องส่วนที่เลือก
help-shortcut-show-help = แสดงหน้าต่างช่วยเหลือนี้
help-shortcut-close-help = ปิดหน้าต่างช่วยเหลือ
help-action-copy = คัดลอกไปยังคลิปบอร์ด
help-action-copy-desc = คัดลอกผลลัพธ์เป็นค่าที่คั่นด้วยแท็บ
help-action-export = ส่งออก CSV
help-action-export-desc = ดาวน์โหลดผลลัพธ์เป็นไฟล์ CSV
help-tip-limit = ผลลัพธ์จำกัดที่ 1,000 แถวเพื่อประสิทธิภาพ ใช้ LIMIT เพื่อปรับแต่งคิวรี
help-tip-time = เวลาดำเนินการแสดงพร้อมกับผลลัพธ์คิวรี
help-tip-syntax = ตัวแก้ไขรองรับการเน้นไวยากรณ์ SQL และการเติมอัตโนมัติ
help-tip-theme = สลับระหว่างโหมดสว่าง/มืดด้วยปุ่มธีม
help-got-it = เข้าใจแล้ว!

# Showcase Navigation
showcase-title = โชว์เคส SQL:1999 Core
showcase-description = สำรวจฟีเจอร์ SQL:1999 Core ที่ใช้งานได้แบบโต้ตอบ
showcase-complete = { $percent }% เสร็จสมบูรณ์
showcase-categories = หมวดหมู่ฟีเจอร์
showcase-legend = คำอธิบายสถานะ
showcase-status-implemented = ใช้งานได้เต็มรูปแบบ
showcase-status-partial = ใช้งานได้บางส่วน
showcase-status-planned = วางแผนไว้

# Showcase category labels
showcase-cat-compliance = แดชบอร์ดความสอดคล้อง
showcase-cat-data-types = ประเภทข้อมูล
showcase-cat-dml = การดำเนินการ DML
showcase-cat-predicates = เพรดิเคตและโอเปอเรเตอร์
showcase-cat-joins = JOIN
showcase-cat-subqueries = ซับคิวรี
showcase-cat-aggregates = การรวมและ GROUP BY
showcase-cat-ddl = DDL และข้อจำกัด

# Common showcase elements
showcase-interactive-examples = ตัวอย่างแบบโต้ตอบ
showcase-try-example = ลองตัวอย่างนี้
showcase-progress = { $implemented } จาก { $total } { $type } ({ $percent }%)
showcase-table-status = สถานะ
showcase-table-category = หมวดหมู่
showcase-table-description = คำอธิบาย
showcase-table-syntax = ไวยากรณ์
showcase-table-use-case = กรณีใช้งาน

# Status labels
status-implemented = ใช้งานได้
status-partial = บางส่วน
status-planned = วางแผนไว้

# Aggregates Showcase
aggregates-title = การรวม SQL และ GROUP BY
aggregates-description = ฟังก์ชันรวม SQL:1999 Core และความสามารถในการจัดกลุ่ม
aggregates-reference = คู่มือฟังก์ชันรวม
aggregates-table-function = ฟังก์ชัน
aggregates-progress-type = ฟังก์ชัน
aggregates-ex-basic = ฟังก์ชันรวมพื้นฐาน
aggregates-ex-group-single = GROUP BY (คอลัมน์เดียว)
aggregates-ex-group-multiple = GROUP BY (หลายคอลัมน์)
aggregates-ex-having = ประโยค HAVING
aggregates-ex-orderby = ORDER BY กับการรวม
aggregates-ex-null = การจัดการ NULL ในการรวม

# DML Operations Showcase
dml-title = การดำเนินการ DML (ภาษาจัดการข้อมูล)
dml-description = การดำเนินการ SQL:1999 Core สำหรับการสืบค้นและแก้ไขข้อมูล
dml-reference = คู่มือการดำเนินการ DML
dml-table-operation = การดำเนินการ
dml-progress-type = การดำเนินการ
dml-ex-select-basic = SELECT - คิวรีพื้นฐาน
dml-ex-select-ordering = SELECT - การเรียงลำดับและจำกัด
dml-ex-insert = การดำเนินการ INSERT
dml-ex-update = การดำเนินการ UPDATE
dml-ex-delete = การดำเนินการ DELETE
dml-ex-combined = เวิร์กโฟลว์ CRUD รวม

# Data Types Showcase
datatypes-title = ประเภทข้อมูล SQL:1999 Core
datatypes-description = สำรวจประเภทข้อมูลพื้นฐานที่กำหนดในข้อกำหนด SQL:1999 Core
datatypes-reference = คู่มือประเภทข้อมูล
datatypes-table-type = ชื่อประเภท
datatypes-table-example = ค่าตัวอย่าง
datatypes-table-spec = ข้อกำหนด
datatypes-progress-type = ประเภท
datatypes-ex-numeric = การทำงานกับประเภทตัวเลข
datatypes-ex-null = การจัดการ NULL และตรรกะสามค่า
datatypes-ex-comparisons = การเปรียบเทียบประเภทและการดำเนินการ

# JOINs Showcase
joins-title = SQL JOIN
joins-description = การดำเนินการ JOIN SQL:1999 Core สำหรับการรวมข้อมูลจากหลายตาราง
joins-reference = คู่มือประเภท JOIN
joins-table-type = ประเภท JOIN
joins-progress-type = ประเภท JOIN
joins-category-suffix = JOIN
joins-ex-sample = การตั้งค่าข้อมูลตัวอย่าง
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = JOIN หลายตาราง

# Predicates Showcase
predicates-title = เพรดิเคตและโอเปอเรเตอร์
predicates-description = เพรดิเคต SQL:1999 สำหรับการกรองและการดำเนินการเชิงตรรกะ
predicates-reference = คู่มือเพรดิเคต
predicates-table-predicate = เพรดิเคต
predicates-progress-type = เพรดิเคต
predicates-ex-comparison = โอเปอเรเตอร์เปรียบเทียบ
predicates-ex-between = BETWEEN และเพรดิเคตช่วง
predicates-ex-null = เพรดิเคต NULL และตรรกะสามค่า
predicates-ex-boolean = ตรรกะบูลีน (AND, OR, NOT)
predicates-ex-in = เพรดิเคต IN กับซับคิวรี
predicates-ex-combined = การดำเนินการเพรดิเคตรวม

# Subqueries Showcase
subqueries-title = ซับคิวรี SQL
subqueries-description = ความสามารถซับคิวรี SQL:1999 Core สำหรับการดำเนินการคิวรีซ้อน
subqueries-reference = คู่มือประเภทซับคิวรี
subqueries-table-type = ประเภทซับคิวรี
subqueries-progress-type = ประเภทซับคิวรี
subqueries-ex-scalar-select = ซับคิวรีสเกลาร์ใน SELECT
subqueries-ex-scalar-where = ซับคิวรีสเกลาร์ใน WHERE
subqueries-ex-derived = ตารางที่ได้มา (ซับคิวรีใน FROM)
subqueries-ex-in = เพรดิเคต IN กับซับคิวรี
subqueries-ex-correlated = ซับคิวรีสัมพันธ์
subqueries-ex-nested = ซับคิวรีซ้อน
