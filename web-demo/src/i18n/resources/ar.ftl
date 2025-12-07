# VibeSQL Web UI - العربية

# Page titles
page-title = VibeSQL - قاعدة بيانات SQL:1999 مدعومة بالذكاء الاصطناعي
demo-title = عرض VibeSQL
benchmarks-title = معايير الأداء - VibeSQL
benchmarks-heading = VibeSQL - معايير الأداء
conformance-title = تقرير المطابقة - VibeSQL
conformance-heading = تقرير المطابقة
conformance-subtitle = اختبار الامتثال لمعايير SQL:1999

# Navigation
nav-showcase = عرض SQL:1999
nav-conformance = عرض نتائج sqltest
nav-sqllogictest = عرض نتائج SQLLogicTest

# Editor section
editor-title = محرر SQL
editor-storage = التخزين
editor-storage-init = جاري التهيئة...
editor-execute = تنفيذ الاستعلام

# Results section
results-title = النتائج
results-empty = نفذ استعلاماً لرؤية النتائج
results-loading = جاري التحميل...
results-rows = { $count } صف
results-rows-with-time = { $count } صف ({ $time } مللي ثانية)
results-copy = نسخ إلى الحافظة
results-export = تصدير CSV
results-limit-warning = عرض أول { $limit } من { $total } صف. استخدم LIMIT لتحسين استعلامك.

# Examples sidebar
examples-title = أمثلة
examples-basic = استعلامات أساسية
examples-advanced = استعلامات متقدمة

# Database selector
db-select-label = قاعدة البيانات

# Footer
footer-tagline = VibeSQL - قاعدة بيانات SQL:1999 في WebAssembly
footer-deployed = تاريخ النشر: { $date }

# Theme
theme-toggle-dark = التبديل إلى الوضع الداكن
theme-toggle-light = التبديل إلى الوضع الفاتح

# Locale
locale-select = اختر اللغة

# Messages
msg-query-success = تم تنفيذ الاستعلام بنجاح
msg-rows-affected = تأثر { $count } صف

# Errors
error-generic = حدث خطأ
error-query-failed = فشل الاستعلام

# Editor
editor-placeholder = أدخل استعلام SQL هنا... (Ctrl+Enter أو Cmd+Enter للتنفيذ)

# Navigation links
nav-terminal = عرض طرفية SQL
nav-compliance = تقرير المطابقة SQL
nav-benchmarks = معايير الأداء
nav-github = مستودع GitHub
nav-home = الرئيسية

# Results
results-success-zero = تم تنفيذ الاستعلام بنجاح (0 صفوف)
results-null = فارغ

# Help Modal
help-title = اختصارات لوحة المفاتيح والمساعدة
help-close = إغلاق
help-editor-shortcuts = اختصارات المحرر
help-navigation = التنقل
help-results-actions = إجراءات النتائج
help-tips = نصائح
help-shortcut-execute = تنفيذ الاستعلام الحالي
help-shortcut-comment = تبديل تعليق السطر
help-shortcut-indent = مسافة بادئة للتحديد
help-shortcut-show-help = إظهار نافذة المساعدة
help-shortcut-close-help = إغلاق نافذة المساعدة
help-action-copy = نسخ إلى الحافظة
help-action-copy-desc = نسخ النتائج كقيم مفصولة بعلامات التبويب
help-action-export = تصدير CSV
help-action-export-desc = تنزيل النتائج كملف CSV
help-tip-limit = النتائج محدودة بـ 1,000 صف للأداء. استخدم LIMIT لتحسين الاستعلامات.
help-tip-time = يُعرض وقت التنفيذ مع نتائج الاستعلام.
help-tip-syntax = يدعم المحرر تمييز بناء جملة SQL والإكمال التلقائي.
help-tip-theme = بدّل بين الوضع الفاتح/الداكن باستخدام زر السمة.
help-got-it = فهمت!

# Showcase Navigation
showcase-title = عرض SQL:1999 الأساسي
showcase-description = استكشف ميزات SQL:1999 الأساسية المُنفذة بشكل تفاعلي
showcase-complete = { $percent }% مكتمل
showcase-categories = فئات الميزات
showcase-legend = مفتاح الحالة
showcase-status-implemented = مُنفذ بالكامل
showcase-status-partial = مُنفذ جزئياً
showcase-status-planned = مُخطط له

# Showcase category labels
showcase-cat-compliance = لوحة المطابقة
showcase-cat-data-types = أنواع البيانات
showcase-cat-dml = عمليات DML
showcase-cat-predicates = المسندات والعوامل
showcase-cat-joins = الربط
showcase-cat-subqueries = الاستعلامات الفرعية
showcase-cat-aggregates = التجميعات و GROUP BY
showcase-cat-ddl = DDL والقيود

# Common showcase elements
showcase-interactive-examples = أمثلة تفاعلية
showcase-try-example = جرب هذا المثال
showcase-progress = { $implemented } من { $total } { $type } ({ $percent }%)
showcase-table-status = الحالة
showcase-table-category = الفئة
showcase-table-description = الوصف
showcase-table-syntax = البناء
showcase-table-use-case = حالة الاستخدام

# Status labels
status-implemented = مُنفذ
status-partial = جزئي
status-planned = مُخطط

# Aggregates Showcase
aggregates-title = تجميعات SQL و GROUP BY
aggregates-description = دوال التجميع SQL:1999 الأساسية وإمكانيات التجميع
aggregates-reference = مرجع دوال التجميع
aggregates-table-function = الدالة
aggregates-progress-type = دالة
aggregates-ex-basic = دوال التجميع الأساسية
aggregates-ex-group-single = GROUP BY (عمود واحد)
aggregates-ex-group-multiple = GROUP BY (أعمدة متعددة)
aggregates-ex-having = جملة HAVING
aggregates-ex-orderby = ORDER BY مع التجميعات
aggregates-ex-null = معالجة NULL في التجميعات

# DML Operations Showcase
dml-title = عمليات DML (لغة معالجة البيانات)
dml-description = عمليات SQL:1999 الأساسية للاستعلام وتعديل البيانات
dml-reference = مرجع عمليات DML
dml-table-operation = العملية
dml-progress-type = عملية
dml-ex-select-basic = SELECT - استعلامات أساسية
dml-ex-select-ordering = SELECT - الترتيب والتحديد
dml-ex-insert = عمليات INSERT
dml-ex-update = عمليات UPDATE
dml-ex-delete = عمليات DELETE
dml-ex-combined = سير عمل CRUD المجمع

# Data Types Showcase
datatypes-title = أنواع البيانات SQL:1999 الأساسية
datatypes-description = استكشف أنواع البيانات الأساسية المحددة في مواصفات SQL:1999
datatypes-reference = مرجع أنواع البيانات
datatypes-table-type = اسم النوع
datatypes-table-example = قيم المثال
datatypes-table-spec = المواصفات
datatypes-progress-type = نوع
datatypes-ex-numeric = العمل مع الأنواع الرقمية
datatypes-ex-null = معالجة NULL والمنطق ثلاثي القيم
datatypes-ex-comparisons = مقارنات وعمليات الأنواع

# JOINs Showcase
joins-title = روابط SQL
joins-description = عمليات JOIN SQL:1999 الأساسية لدمج البيانات من جداول متعددة
joins-reference = مرجع أنواع JOIN
joins-table-type = نوع JOIN
joins-progress-type = نوع JOIN
joins-category-suffix = الروابط
joins-ex-sample = إعداد بيانات العينة
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = JOIN متعدد الجداول

# Predicates Showcase
predicates-title = المسندات والعوامل
predicates-description = مسندات SQL:1999 للتصفية والعمليات المنطقية
predicates-reference = مرجع المسندات
predicates-table-predicate = المسند
predicates-progress-type = مسند
predicates-ex-comparison = عوامل المقارنة
predicates-ex-between = BETWEEN ومسندات النطاق
predicates-ex-null = مسندات NULL والمنطق ثلاثي القيم
predicates-ex-boolean = المنطق البولي (AND, OR, NOT)
predicates-ex-in = مسند IN مع الاستعلامات الفرعية
predicates-ex-combined = عمليات المسندات المجمعة

# Subqueries Showcase
subqueries-title = الاستعلامات الفرعية SQL
subqueries-description = إمكانيات الاستعلامات الفرعية SQL:1999 الأساسية للاستعلامات المتداخلة
subqueries-reference = مرجع أنواع الاستعلامات الفرعية
subqueries-table-type = نوع الاستعلام الفرعي
subqueries-progress-type = نوع استعلام فرعي
subqueries-ex-scalar-select = استعلام فرعي قياسي في SELECT
subqueries-ex-scalar-where = استعلام فرعي قياسي في WHERE
subqueries-ex-derived = جداول مشتقة (استعلام فرعي في FROM)
subqueries-ex-in = مسند IN مع استعلام فرعي
subqueries-ex-correlated = استعلامات فرعية مترابطة
subqueries-ex-nested = استعلامات فرعية متداخلة
