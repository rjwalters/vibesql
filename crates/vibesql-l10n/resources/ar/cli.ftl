# VibeSQL CLI Localization - Arabic (ar)
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL الإصدار { $version } - قاعدة بيانات متوافقة مع SQL:1999 FULL
cli-help-hint = اكتب \help للمساعدة، \quit للخروج
cli-goodbye = إلى اللقاء!

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - قاعدة بيانات متوافقة مع SQL:1999 FULL

cli-long-about = واجهة سطر أوامر VibeSQL

    أوضاع الاستخدام:
      REPL تفاعلي:        vibesql (--database <FILE>)
      تنفيذ أمر:          vibesql -c "SELECT * FROM users"
      تنفيذ ملف:          vibesql -f script.sql
      تنفيذ من stdin:     cat data.sql | vibesql
      توليد الأنواع:      vibesql codegen --schema schema.sql --output types.ts

    REPL التفاعلي:
      عند التشغيل بدون -c أو -f أو إدخال موجه، يدخل VibeSQL في REPL تفاعلي
      مع دعم readline وسجل الأوامر وأوامر ميتا مثل:
        \d (table)  - وصف الجدول أو عرض جميع الجداول
        \dt         - عرض الجداول
        \f <format> - تعيين تنسيق الإخراج
        \copy       - استيراد/تصدير CSV/JSON
        \help       - عرض جميع أوامر REPL

    الأوامر الفرعية:
      codegen           توليد أنواع TypeScript من مخطط قاعدة البيانات

    الإعدادات:
      يمكن تكوين الإعدادات في ~/.vibesqlrc (تنسيق TOML).
      الأقسام: display، database، history، query

    أمثلة:
      # بدء REPL تفاعلي مع قاعدة بيانات في الذاكرة
      vibesql

      # استخدام ملف قاعدة بيانات دائم
      vibesql --database mydata.db

      # تنفيذ أمر واحد
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # تشغيل ملف SQL
      vibesql -f schema.sql -v

      # استيراد بيانات من CSV
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # تصدير نتائج الاستعلام كـ JSON
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # توليد أنواع TypeScript من ملف مخطط
      vibesql codegen --schema schema.sql --output src/types.ts

      # توليد أنواع TypeScript من قاعدة بيانات قيد التشغيل
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = مسار ملف قاعدة البيانات (إذا لم يُحدد، تُستخدم قاعدة بيانات في الذاكرة)
arg-file-help = تنفيذ أوامر SQL من ملف
arg-command-help = تنفيذ أمر SQL مباشرة والخروج
arg-stdin-help = قراءة أوامر SQL من stdin (يُكتشف تلقائياً عند التوجيه)
arg-verbose-help = عرض مخرجات تفصيلية أثناء تنفيذ الملف/stdin
arg-format-help = تنسيق الإخراج لنتائج الاستعلام
arg-lang-help = تعيين لغة العرض (مثال: en-US، es، ja)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = توليد أنواع TypeScript من مخطط قاعدة البيانات

codegen-long-about = توليد تعريفات أنواع TypeScript من مخطط قاعدة بيانات VibeSQL.

    ينشئ هذا الأمر واجهات TypeScript لجميع الجداول في قاعدة البيانات،
    إلى جانب كائنات البيانات الوصفية لفحص الأنواع في وقت التشغيل ودعم IDE.

    مصادر الإدخال:
      --database <FILE>  التوليد من ملف قاعدة بيانات موجود
      --schema <FILE>    التوليد من ملف مخطط SQL (عبارات CREATE TABLE)

    الإخراج:
      --output <FILE>    كتابة الأنواع المولدة إلى هذا الملف (الافتراضي: types.ts)

    الخيارات:
      --camel-case       تحويل أسماء الأعمدة إلى camelCase
      --no-metadata      تخطي توليد كائن البيانات الوصفية للجداول

    أمثلة:
      # من ملف قاعدة بيانات
      vibesql codegen --database mydata.db --output src/db/types.ts

      # من ملف مخطط SQL
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # مع أسماء خصائص camelCase
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = ملف مخطط SQL يحتوي على عبارات CREATE TABLE
codegen-output-help = مسار ملف الإخراج لـ TypeScript المولد
codegen-camel-case-help = تحويل أسماء الأعمدة إلى camelCase
codegen-no-metadata-help = تخطي توليد كائن البيانات الوصفية للجدول

codegen-from-schema = توليد أنواع TypeScript من ملف المخطط: { $path }
codegen-from-database = توليد أنواع TypeScript من قاعدة البيانات: { $path }
codegen-written = تم كتابة أنواع TypeScript إلى: { $path }
codegen-error-no-source = يجب تحديد --database أو --schema.
    استخدم 'vibesql codegen --help' لمعلومات الاستخدام.

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = الأوامر الميتا:
help-describe = \d (table)      - وصف الجدول أو عرض جميع الجداول
help-tables = \dt             - عرض الجداول
help-schemas = \ds             - عرض المخططات
help-indexes = \di             - عرض الفهارس
help-roles = \du             - عرض الأدوار/المستخدمين
help-format = \f <format>     - تعيين تنسيق الإخراج (table، json، csv، markdown، html)
help-timing = \timing         - تبديل توقيت الاستعلام
help-copy-to = \copy <table> TO <file>   - تصدير الجدول إلى ملف CSV/JSON
help-copy-from = \copy <table> FROM <file> - استيراد ملف CSV إلى الجدول
help-save = \save (file)    - حفظ قاعدة البيانات إلى ملف تفريغ SQL
help-errors = \errors         - عرض سجل الأخطاء الأخيرة
help-help = \h, \help      - عرض هذه المساعدة
help-quit = \q, \quit      - خروج

help-sql-title = استعلام SQL:
help-show-tables = SHOW TABLES                  - عرض جميع الجداول
help-show-databases = SHOW DATABASES               - عرض جميع المخططات/قواعد البيانات
help-show-columns = SHOW COLUMNS FROM <table>    - عرض أعمدة الجدول
help-show-index = SHOW INDEX FROM <table>      - عرض فهارس الجدول
help-show-create = SHOW CREATE TABLE <table>    - عرض عبارة CREATE TABLE
help-describe-sql = DESCRIBE <table>             - اسم مستعار لـ SHOW COLUMNS

help-examples-title = أمثلة:
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

format-changed = تم تعيين تنسيق الإخراج إلى: { $format }
database-saved = تم حفظ قاعدة البيانات إلى: { $path }
no-database-file = خطأ: لم يتم تحديد ملف قاعدة البيانات. استخدم \save <filename> أو ابدأ مع علامة --database

# =============================================================================
# Error Display
# =============================================================================

no-errors = لا توجد أخطاء في هذه الجلسة.
recent-errors = الأخطاء الأخيرة:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = لم يتم العثور على عبارات SQL في السكريبت
script-executing = تنفيذ العبارة { $current } من { $total }...
script-error = خطأ في تنفيذ العبارة { $index }: { $error }
script-summary-title = === ملخص تنفيذ السكريبت ===
script-total = إجمالي العبارات: { $count }
script-successful = ناجحة: { $count }
script-failed = فاشلة: { $count }
script-failed-error = فشلت { $count } عبارة/عبارات

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } صف/صفوف في المجموعة ({ $time } ثانية)
rows-count = { $count } صف/صفوف

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = تحذير: تعذر تحميل ملف الإعدادات: { $error }
warning-auto-save-failed = تحذير: فشل الحفظ التلقائي لقاعدة البيانات: { $error }
warning-save-on-exit-failed = تحذير: فشل حفظ قاعدة البيانات عند الخروج: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = فشل قراءة الملف '{ $path }': { $error }
stdin-read-error = فشل القراءة من stdin: { $error }
database-load-error = فشل تحميل قاعدة البيانات: { $error }
