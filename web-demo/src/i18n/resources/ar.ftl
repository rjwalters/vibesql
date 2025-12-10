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
error-no-databases = لا توجد قواعد بيانات متاحة

# Loading states
loading-initializing-theme = جارٍ تهيئة السمة
loading-preparing-editor = جارٍ تحضير المحرر
loading-database-engine = جارٍ تحميل محرك قاعدة البيانات
loading-setting-up-ui = جارٍ إعداد واجهة المستخدم
loading-editor = جارٍ تحميل المحرر...
loading-compliance-data = جارٍ تحميل بيانات التوافق...
loading-conformance-report = جارٍ تحميل تقرير المطابقة...

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

# =============================================================================
# Benchmarks Page
# =============================================================================

# Section headers
bench-section-embedded = Embedded
bench-section-server = Server
bench-results-title = Benchmark Results
bench-perf-comparison = Performance Comparison
bench-methodology-title = Methodology
bench-analysis-roadmap = Analysis & Roadmap

# Summary cards
bench-vs-sqlite = vs SQLite
bench-vs-duckdb = vs DuckDB
bench-vs-mysql = vs MySQL
bench-ops-tested = Operations Tested
bench-last-updated = Last Updated
bench-avg-speedup = average speedup
bench-from-main = from main branch
bench-loading = Loading...
bench-na = N/A
bench-faster = { $value }x faster
bench-slower = { $value }x slower
bench-speedup = { $value }x
bench-startup-time-label = startup time
bench-download-size = download size
bench-uncompressed = uncompressed
bench-size-metrics = size metrics
bench-failed = FAILED
bench-failed-title = Query failed (timeout or error)
bench-no-wasm-data = لا تتوفر بيانات WASM
bench-no-server-data = لا تتوفر بيانات مقاييس أداء خادم Sysbench
bench-no-server-data-hint = تتطلب مقاييس أداء الخادم تشغيل sysbench_server مع تمكين مقارنة MySQL.

# Table headers
bench-table-operation = Operation
bench-table-vibesql = VibeSQL
bench-table-vibesql-server = VibeSQL Server
bench-table-sqlite = SQLite
bench-table-duckdb = DuckDB
bench-table-mysql = MySQL
bench-table-loading = Loading benchmark results...
bench-vibesql-server-title = VibeSQL via PostgreSQL wire protocol

# Common benchmark terms
bench-hardware = Hardware
bench-benchmark-framework = Benchmark Framework
bench-scale-factor = Scale Factor
bench-data = Data
bench-databases-tested = Databases Tested
bench-execution-mode = Execution Mode
bench-measurement = Measurement
bench-workload = Workload
bench-transaction-mix = Transaction Mix
bench-warehouses = Warehouses
bench-concurrency = Concurrency
bench-acid-compliance = ACID Compliance
bench-mode = Mode
bench-workload-types = Workload Types
bench-table-size = Table Size
bench-index-types = Index Types
bench-operations = Operations
bench-databases = Databases
bench-protocol-overhead = Protocol Overhead
bench-binary-size = Binary Size
bench-startup-time = Startup Time
bench-peak-memory = Peak Memory
bench-schema = Schema
bench-query-count = Query Count
bench-query-types = Query Types
bench-sql-features = SQL Features
bench-wasm-size = WASM Size
bench-wasm-gzip = WASM (gzip)
bench-wasm-brotli = WASM (brotli)

# TPC-H specific
bench-tpch-name = TPC-H
bench-tpch-title = TPC-H Decision Support Benchmark
bench-tpch-description = تستخدم هذه المقاييس المعيارية <strong>مجموعة اختبارات TPC-H</strong> القياسية الصناعية، والتي تحاكي أعباء عمل دعم القرار الواقعية مع استعلامات تحليلية معقدة تتضمن التجميعات والربط والاستعلامات الفرعية والفرز.
bench-tpch-ops-label = TPC-H queries
bench-tpch-note-intro = تقيس جميع المقاييس المعيارية وقت تنفيذ الاستعلام من البداية إلى النهاية بما في ذلك التحليل والتخطيط والتنفيذ وتجسيد النتائج. يمثل هذا <strong>أداء محرك SQL الواقعي</strong> لأعباء العمل التحليلية.
bench-tpch-note-queries = <strong>ملاحظة:</strong> تختبر استعلامات TPC-H جوانب مختلفة من أداء SQL: التجميعات البسيطة (Q1، Q6)، الربط المعقد (Q2-Q5، Q7-Q10)، الاستعلامات الفرعية (Q11-Q15)، والتحليلات المتقدمة (Q16-Q22). مرر المؤشر فوق أسماء الاستعلامات في الجدول أعلاه للحصول على الأوصاف.

# TPC-H Discussion
bench-tpch-disc-excels-title = أين يتفوق VibeSQL
bench-tpch-disc-excels = يُظهر VibeSQL أداءً قوياً في <strong>استعلامات التجميع كثيفة المسح</strong> (Q1، Q6، Q14، Q15، Q20) حيث يتألق محرك التنفيذ العمودي والتجميعات المسرّعة بـ SIMD. تتضمن هذه الاستعلامات تصفية الجداول الكبيرة وحساب التجميعات بدون أنماط ربط معقدة.
bench-tpch-disc-targets-title = أهداف التحسين الحالية
bench-tpch-disc-targets = تُظهر استعلامات الربط المتعددة (Q3، Q5، Q7-Q10، Q18، Q19، Q21) حالياً تفوق SQLite. العائق الرئيسي هو تنفيذ ربط التجزئة لدينا، الذي لم يستخدم بعد نفس مستوى التحسين مثل عمليات ربط B-tree المُحسّنة على مدى عقود في SQLite. المجالات قيد التطوير النشط:
bench-tpch-disc-join-ordering = تحسين تقدير الأصل لاختيار ترتيب ربط أفضل
bench-tpch-disc-hash-sizing = نمو جدول التجزئة التكيفي والتجاوز إلى القرص للربط الكبير
bench-tpch-disc-vectorized = معالجة الدفعات في حلقة الربط الداخلية لتحسين استخدام ذاكرة التخزين المؤقت
bench-tpch-disc-inl-joins = الاستفادة من فهارس B-tree عند الفائدة
bench-tpch-disc-path-title = المسار نحو الريادة
bench-tpch-disc-path = تم تصميم بنية VibeSQL للأجهزة الحديثة مع ميزات مثل التخزين العمودي والتنفيذ المتجه والتزامن بدون قفل. مع نضج هذه التحسينات، نتوقع أن يحقق VibeSQL ريادة مستمرة عبر جميع استعلامات TPC-H. يدعم التصميم الأساسي التوازي و SIMD الذي لا تستطيع قواعد البيانات التقليدية ذات التخزين الصفي إضافته بسهولة.

# TPC-H Query Descriptions
bench-tpch-q1 = Pricing Summary Report - Aggregate pricing with GROUP BY and ORDER BY
bench-tpch-q2 = Minimum Cost Supplier - 3-table JOIN with ORDER BY and LIMIT
bench-tpch-q3 = Shipping Priority - 3-table JOIN with aggregation
bench-tpch-q4 = Order Priority Checking - Correlated EXISTS subquery
bench-tpch-q5 = Local Supplier Volume - 6-table JOIN with complex filtering
bench-tpch-q6 = Forecasting Revenue Change - WHERE filters with BETWEEN and SUM
bench-tpch-q7 = Volume Shipping - 6-table JOIN with SUBSTR and date filtering
bench-tpch-q8 = National Market Share - 7-table JOIN with CASE expressions
bench-tpch-q9 = Product Type Profit Measure - 4-table JOIN with aggregation
bench-tpch-q10 = Returned Item Reporting - 4-table JOIN with TOP-N LIMIT
bench-tpch-q11 = Important Stock Identification - Subquery in HAVING clause
bench-tpch-q12 = Shipping Modes Priority - CASE aggregation with date logic
bench-tpch-q13 = Customer Distribution - LEFT OUTER JOIN with subquery
bench-tpch-q14 = Promotion Effect - Conditional aggregation with CASE
bench-tpch-q15 = Top Supplier - Nested subqueries with MAX
bench-tpch-q16 = Parts/Supplier Relationship - NOT IN subquery with DISTINCT
bench-tpch-q17 = Small-Quantity-Order Revenue - Correlated subquery in WHERE
bench-tpch-q18 = Large Volume Customer - GROUP BY with HAVING
bench-tpch-q19 = Discounted Revenue - Complex OR conditions
bench-tpch-q20 = Potential Part Promotion - IN subquery with GROUP BY/HAVING
bench-tpch-q21 = Suppliers Who Kept Orders Waiting - Multi-table EXISTS
bench-tpch-q22 = Global Sales Opportunity - SUBSTR with NOT EXISTS subquery

# TPC-DS specific
bench-tpcds-name = TPC-DS
bench-tpcds-title = TPC-DS Decision Support Benchmark
bench-tpcds-description = <strong>TPC-DS</strong> هو خليفة TPC-H، ويتميز بـ 99 استعلاماً تنمذج نظام دعم قرار حديث مع أنماط استعلام أكثر تعقيداً بما في ذلك جداول حقائق متعددة ومخطط ندفة الثلج وميزات SQL المتقدمة.
bench-tpcds-ops-label = TPC-DS queries
bench-tpcds-note-intro = استعلامات TPC-DS أكثر تعقيداً بشكل كبير من TPC-H، وتختبر ميزات SQL المتقدمة مثل دوال النافذة وتعبيرات الجداول المشتركة (جملة WITH) وأنماط الربط المعقدة عبر جداول الحقائق والأبعاد المتعددة.
bench-tpcds-note-remaining = <strong>ملاحظة:</strong> تنجح جميع استعلامات TPC-DS البالغ عددها 99، مما يدل على دعم شامل لميزات SQL:1999 بما في ذلك INTERSECT و EXCEPT ودوال النافذة و CTEs والاستعلامات الفرعية المعقدة.

# TPC-DS Discussion
bench-tpcds-disc-coverage-title = تغطية ميزات SQL:1999
bench-tpcds-disc-coverage = يختبر TPC-DS أكثر ميزات SQL تطلباً. ينجح VibeSQL في <strong>جميع الاستعلامات الـ 99</strong>، مما يدل على تغطية كاملة لـ SQL:1999 بما في ذلك ROLLUP و CUBE و GROUPING() ودوال النافذة مع التأطير المعقد و CTEs العودية وعمليات المجموعات INTERSECT/EXCEPT.
bench-tpcds-disc-optimization-title = تحسين الاستعلامات المعقدة
bench-tpcds-disc-optimization = غالباً ما تربط استعلامات TPC-DS أكثر من 10 جداول مع استعلامات فرعية مترابطة. مجالات التركيز الحالية:
bench-tpcds-disc-cte = القرار الذكي بين CTEs المُجسّدة والمضمّنة
bench-tpcds-disc-decorrelation = تحويل الاستعلامات الفرعية المترابطة إلى عمليات ربط عند الفائدة
bench-tpcds-disc-star = ترتيب ربط الحقائق والأبعاد للأنماط التحليلية
bench-tpcds-disc-toward-title = تغطية TPC-DS الكاملة
bench-tpcds-disc-toward = مع نجاح جميع الاستعلامات الـ 99، يُظهر VibeSQL امتثالاً جاهزاً للإنتاج لـ SQL:1999 لأعباء العمل التحليلية المعقدة. أكملت الإضافات الأخيرة لعمليات المجموعات INTERSECT و EXCEPT تغطية TPC-DS الكاملة، مُنفّذة كعوامل قائمة على التجزئة فعّالة.
bench-tpcds-disc-sqlite-title = ملاحظة مقارنة SQLite
bench-tpcds-disc-sqlite = لا يستطيع SQLite تنفيذ 12 من استعلامات TPC-DS الـ 99 (Q2، Q5، Q14، Q17، Q18، Q22، Q36، Q67، Q70، Q77، Q80، Q86) بسبب غياب ميزات OLAP في SQL:1999: مجموعات التجميع <strong>ROLLUP/CUBE</strong>، ودالة <strong>GROUPING()</strong>، و <strong>STDDEV_SAMP()</strong>. يتم تخطي هذه الاستعلامات في مقاييس SQLite. يدعم VibeSQL و DuckDB جميع الاستعلامات الـ 99.

# TPC-C specific
bench-tpcc-name = TPC-C
bench-tpcc-title = TPC-C Online Transaction Processing Benchmark
bench-tpcc-description = يحاكي <strong>مقياس TPC-C</strong> بيئة إدخال طلبات كاملة مع مزيج من المعاملات المعقدة بما في ذلك إدخال الطلبات ومعالجة المدفوعات واستعلامات حالة الطلب ومعالجة التسليم ومراقبة مستوى المخزون.
bench-tpcc-ops-label = TPC-C transactions
bench-tpcc-note-intro = يقيس TPC-C المعاملات في الدقيقة (tpmC) ويختبر قدرة قاعدة البيانات على التعامل مع المعاملات المتزامنة مع منطق أعمال معقد. هذا المقياس حاسم لتقييم <strong>أداء أعباء العمل المعاملاتية</strong>.
bench-tpcc-note-results = <strong>ملاحظة:</strong> تُظهر النتائج متوسط زمن استجابة المعاملة. الأقل أفضل. TPC-C متطلب بشكل خاص لأعباء العمل كثيفة الكتابة مع متطلبات اتساق صارمة.

# TPC-C Transaction Descriptions
bench-tpcc-new-order = New Order - Complex transaction with inventory checks and order creation
bench-tpcc-payment = Payment - Update customer balance and warehouse/district totals
bench-tpcc-order-status = Order Status - Read-only query for customer order history
bench-tpcc-delivery = Delivery - Batch processing of pending orders
bench-tpcc-stock-level = Stock Level - Count items below threshold in recent orders

# TPC-C Discussion
bench-tpcc-disc-faster-title = أسرع بـ 5 مرات من SQLite
bench-tpcc-disc-faster = يحقق VibeSQL <strong>حوالي 23,000 معاملة في الثانية</strong> مقارنة بـ 4,500 معاملة في الثانية لـ SQLite، تحسن بمقدار 5 مرات. يأتي هذا التسريع من بنية MVCC بدون قفل التي تتجنب القفل الخشن في SQLite على كل عملية كتابة.
bench-tpcc-disc-dominates-title = لماذا يهيمن VibeSQL على OLTP
bench-tpcc-disc-lockfree = يسمح MVCC للقراء والكتاب بالتقدم بشكل متزامن بدون حظر
bench-tpcc-disc-optimistic = تتعارض المعاملات فقط في وقت الالتزام، وليس أثناء التنفيذ
bench-tpcc-disc-btree = هيكل فهرس مُصمم خصيصاً ومُحسّن لأعباء العمل في الذاكرة
bench-tpcc-disc-prepared = يتم تجميع خطط الاستعلام مرة واحدة وإعادة استخدامها
bench-tpcc-disc-scaling-title = التوسع أكثر
bench-tpcc-disc-scaling = النتائج الحالية أحادية الخيط. تدعم بنية VibeSQL معالجة المعاملات متعددة الخيوط، ونتوقع توسعاً محسناً مع إضافة دعم التنفيذ المتوازي.
bench-tpcc-disc-duckdb-title = لماذا يتأخر DuckDB في OLTP
bench-tpcc-disc-duckdb = يحقق DuckDB فقط حوالي 385 معاملة في الثانية على TPC-C (أبطأ بـ 60 مرة من VibeSQL، أبطأ بـ 12 مرة من SQLite). هذا متوقع: DuckDB هي <strong>قاعدة بيانات تحليلية (OLAP)</strong> مُحسّنة للعمليات الدفعية الكبيرة، وليس المعاملات أحادية الصف. يتفوق تنسيق التخزين العمودي في مسح ملايين الصفوف لكنه يضيف عبئاً للبحث النقطي والتحديثات الصغيرة التي تهيمن على أعباء عمل OLTP مثل TPC-C.

# Sysbench Embedded specific
bench-sysbench-embedded-name = Sysbench (Embedded)
bench-sysbench-embedded-title = Sysbench Micro-Benchmarks (Embedded)
bench-sysbench-embedded-description = يوفر <strong>Sysbench</strong> مقاييس مركزة دقيقة تعزل عمليات قاعدة البيانات المحددة. تقيس هذه الاختبارات الأداء الخام للعمليات الأساسية بدون تعقيد أعباء العمل المعاملاتية الكاملة.
bench-sysbench-embedded-ops-label = Sysbench operations
bench-sysbench-embedded-note = يُشغّل الوضع المضمن قاعدة البيانات داخل العملية بدون عبء شبكة، مثالي للتطبيقات أحادية العملية حيث الحد الأدنى من زمن الاستجابة أمر بالغ الأهمية.

# Sysbench Operation Descriptions
bench-sysbench-point-select = Point Select - Single row lookup by primary key
bench-sysbench-insert = Insert - Insert new rows into table
bench-sysbench-update-index = Update Index - Update indexed column (k = k + 1)
bench-sysbench-update-non-index = Update Non-Index - Update non-indexed column
bench-sysbench-delete = Delete - Remove rows by primary key
bench-sysbench-range-queries = Range Queries - Simple, SUM, ORDER BY, and DISTINCT range scans

# Sysbench Embedded Discussion
bench-sysbench-emb-disc-point-title = البحث النقطي: على قدم المساواة
bench-sysbench-emb-disc-point = تعمل عمليات الاختيار النقطي في VibeSQL بسرعة <strong>حوالي 0.37 ميكروثانية</strong>، مطابقة لـ SQLite بـ 0.36 ميكروثانية. تم تحسين تنفيذ B-tree لدينا للبحث عن صف واحد مع الحد الأدنى من تتبع المؤشرات وتخطيطات العقد الصديقة للتخزين المؤقت.
bench-sysbench-emb-disc-index-title = تحديثات الفهرس: مجال للتحسين
bench-sysbench-emb-disc-index = تعمل تحديثات الفهرس في VibeSQL بسرعة <strong>حوالي 4.3 ميكروثانية مقابل 1.7 ميكروثانية لـ SQLite</strong>. هذا مجال للتحسين حيث يضيف تصميم MVCC لدينا عبئاً لصيانة الفهرس نعمل على تقليله.
bench-sysbench-emb-disc-improve-title = مجالات التحسين
bench-sysbench-emb-disc-bulk = مسار الإدراج الدفعي في SQLite مُحسّن للغاية؛ نضيف عمليات B-tree دفعية
bench-sysbench-emb-disc-nonindex = تُظهر التحديثات غير المفهرسة VibeSQL بـ 1.9 ميكروثانية مقابل 1.4 ميكروثانية لـ SQLite - قريبة من التكافؤ
bench-sysbench-emb-disc-deletes = تحسنت عمليات الحذف بشكل كبير: الآن حوالي 5.5 ميكروثانية مقابل 3.8 ميكروثانية لـ SQLite (كانت 1183 ميكروثانية سابقاً)
bench-sysbench-emb-disc-duckdb-title = DuckDB Comparison
bench-sysbench-emb-disc-duckdb = DuckDB is optimized for analytical workloads, not micro-operations. Its 100-1000x slower results here reflect architectural choices (columnar storage, vectorized execution) that trade single-row latency for bulk throughput. VibeSQL targets both use cases.
bench-sysbench-emb-disc-architecture-title = المقايضات المعمارية
bench-sysbench-emb-disc-architecture = تستهدف البنية الهجينة لـ VibeSQL أعباء عمل OLTP و OLAP. يوفر تخزين B-tree لدينا أداء بحث نقطي منافساً لـ SQLite، بينما يتعامل التنفيذ العمودي مع الاستعلامات التحليلية بكفاءة. يختلف هذا عن قواعد بيانات OLAP الصرفة مثل DuckDB التي تُحسّن حصرياً للعمليات الدفعية على حساب زمن استجابة الصف الواحد.

# Sysbench Server specific
bench-sysbench-server-name = Sysbench (Server)
bench-sysbench-server-title = Sysbench Micro-Benchmarks (Server)
bench-sysbench-server-description = تقارن مقاييس <strong>Sysbench</strong> للخادم خادم VibeSQL (بروتوكول PostgreSQL السلكي) مع MySQL، لقياس الأداء لعمليات نشر قواعد البيانات متعددة العملاء.
bench-sysbench-server-ops-label = Sysbench operations
bench-sysbench-server-note = يستخدم وضع الخادم بروتوكول PostgreSQL السلكي، مما يتيح الوصول متعدد العملاء والتوافق مع أدوات ومحركات PostgreSQL الحالية.

# Sysbench Server Discussion
bench-sysbench-srv-disc-protocol-title = بروتوكول PostgreSQL السلكي
bench-sysbench-srv-disc-protocol = يُنفّذ خادم VibeSQL بروتوكول PostgreSQL السلكي، مما يتيح التوافق مع محركات وأدوات PostgreSQL الحالية. يضيف هذا حوالي 10-50 ميكروثانية من عبء البروتوكول لكل استعلام مقارنة بالوضع المضمن، لكنه يُمكّن عمليات النشر متعددة العملاء.
bench-sysbench-srv-disc-mysql-title = مقارنة MySQL
bench-sysbench-srv-disc-mysql = تقارن مقاييس الخادم مع MySQL لتقييم VibeSQL كبديل مباشر لقواعد البيانات التقليدية العميل-الخادم. تختلف النتائج حسب نوع العملية، مع إظهار VibeSQL مزايا في أعباء العمل كثيفة القراءة.
bench-sysbench-srv-disc-roadmap-title = خارطة طريق الخادم
bench-sysbench-srv-disc-pooling = تقليل عبء إنشاء الاتصال لسيناريوهات الإنتاجية العالية
bench-sysbench-srv-disc-caching = التخزين المؤقت لخطط الاستعلام على جانب الخادم عبر الاتصالات
bench-sysbench-srv-disc-extended = دعم بروتوكول الاستعلام الموسع الكامل لـ PostgreSQL للعمليات الدفعية

# TPC-H Server خاص
bench-tpch-server-name = TPC-H (الخادم)
bench-tpch-server-title = مقياس أداء TPC-H التحليلي (الخادم)
bench-tpch-server-description = تقارن <strong>مقاييس أداء خادم TPC-H</strong> VibeSQL Server (بروتوكول PostgreSQL) مع MySQL لأحمال العمل التحليلية، وتقيس أداء OLAP في عمليات النشر العميل-الخادم.
bench-tpch-server-ops-label = استعلامات TPC-H
bench-tpch-server-note-intro = تختبر مقاييس أداء الخادم تنفيذ <strong>بروتوكول PostgreSQL</strong>، وتقيس زمن الوصول الشامل للاستعلام بما في ذلك الحمل الزائد للشبكة.
bench-tpch-server-note-queries = تختبر الاستعلامات JOINs المعقدة والاستعلامات الفرعية والتجميعات النموذجية لأحمال عمل ذكاء الأعمال.

# مناقشة TPC-H Server
bench-tpch-srv-disc-protocol-title = بروتوكول PostgreSQL
bench-tpch-srv-disc-protocol = يتحدث VibeSQL Server بروتوكول PostgreSQL، مما يتيح استخدام برامج تشغيل وأدوات PostgreSQL القياسية. يقيس هذا المقياس زمن الوصول الشامل الكامل بما في ذلك الحمل الزائد للبروتوكول.
bench-tpch-srv-disc-comparison-title = مقارنة مع MySQL
bench-tpch-srv-disc-comparison = توفر المقارنة مع MySQL خط أساس لقواعد البيانات التقليدية العميل-الخادم على أحمال العمل التحليلية. يوفر محرك التنفيذ العمودي لـ VibeSQL مزايا للتجميعات والربط المعقدة.
bench-tpch-srv-disc-roadmap-title = خارطة طريق خادم OLAP
bench-tpch-srv-disc-prepared = إعادة استخدام خطط الاستعلام المترجمة عبر الاتصالات
bench-tpch-srv-disc-pooling = معالجة الاتصالات الفعالة لسيناريوهات الإنتاجية العالية
bench-tpch-srv-disc-scale = اختبار مجموعات بيانات أكبر (SF 0.1، SF 1.0) للتحقق من صحة مقياس الإنتاج

# TPC-C Server خاص
bench-tpcc-server-name = TPC-C (الخادم)
bench-tpcc-server-title = مقياس أداء TPC-C OLTP (الخادم)
bench-tpcc-server-description = تقارن <strong>مقاييس أداء خادم TPC-C</strong> VibeSQL Server (بروتوكول PostgreSQL) مع MySQL لأحمال عمل معاملات OLTP، وتقيس الإنتاجية لعمليات نشر قواعد البيانات متعددة العملاء.
bench-tpcc-server-ops-label = معاملات TPC-C
bench-tpcc-server-note-intro = تختبر مقاييس أداء الخادم تنفيذ <strong>بروتوكول PostgreSQL</strong>، وتقيس إنتاجية المعاملات بما في ذلك الحمل الزائد للشبكة.
bench-tpcc-server-note-results = تُبلغ النتائج عن المعاملات في الثانية (TPS) لمزيج معاملات TPC-C القياسي.
bench-tpcc-mixed = حمل عمل مختلط - مزيج معاملات TPC-C القياسي (45% طلب جديد، 43% دفع، 4% حالة الطلب، 4% التسليم، 4% مستوى المخزون)

# مناقشة TPC-C Server
bench-tpcc-srv-disc-protocol-title = بروتوكول PostgreSQL
bench-tpcc-srv-disc-protocol = يتحدث VibeSQL Server بروتوكول PostgreSQL، مما يتيح استخدام برامج تشغيل وأدوات PostgreSQL القياسية. يقيس هذا المقياس زمن الوصول الشامل الكامل للمعاملات بما في ذلك الحمل الزائد للبروتوكول.
bench-tpcc-srv-disc-comparison-title = مقارنة مع MySQL
bench-tpcc-srv-disc-comparison = توفر المقارنة مع MySQL خط أساس لقواعد البيانات التقليدية العميل-الخادم على أحمال عمل OLTP. MySQL هو المعيار الصناعي لأحمال العمل المعاملاتية، و TPC-C هو نقطة قوة MySQL.
bench-tpcc-srv-disc-roadmap-title = خارطة طريق خادم OLTP
bench-tpcc-srv-disc-prepared = إعادة استخدام خطط الاستعلام المترجمة عبر الاتصالات
bench-tpcc-srv-disc-pooling = معالجة الاتصالات الفعالة لسيناريوهات الإنتاجية العالية
bench-tpcc-srv-disc-parallel = معالجة المعاملات المتزامنة متعددة العملاء

# Footprint Embedded خاص
bench-footprint-embedded-name = Footprint (Embedded)
bench-footprint-embedded-title = Native Binary Footprint
bench-footprint-embedded-description = تقيس <strong>مقاييس البصمة المضمنة</strong> كفاءة موارد ملفات قاعدة البيانات الأصلية، مقارنة حجم الملف الثنائي ووقت البدء البارد واستخدام الذاكرة القصوى.
bench-footprint-embedded-ops-label = databases compared
bench-footprint-embedded-note = البصمة الثنائية الأصلية حاسمة لـ <strong>عمليات النشر المضمنة والحافة</strong> حيث يؤثر حجم الملف الثنائي وزمن استجابة البدء واستهلاك الذاكرة مباشرة على جدوى النشر.

# Footprint Embedded Descriptions
bench-footprint-binary-size = Binary Size - Size of the compiled database binary on disk
bench-footprint-startup-time = Startup Time - Time to cold-start and execute first query
bench-footprint-peak-memory = Peak Memory - Maximum resident set size during initialization

# Footprint Embedded Discussion
bench-footprint-emb-disc-size-title = حجم الملف الثنائي: حل وسط
bench-footprint-emb-disc-size = يقع VibeSQL بـ <strong>حوالي 17 ميجابايت</strong> بين SQLite (حوالي 5 ميجابايت) و DuckDB (حوالي 45 ميجابايت). يعكس هذا اختيارنا لتضمين ميزات متقدمة (دوال النافذة، CTEs، التنفيذ العمودي) مع الحفاظ على الملف الثنائي قابلاً للإدارة لعمليات النشر المضمنة.
bench-footprint-emb-disc-startup-title = البدء: أسرع بدء بارد
bench-footprint-emb-disc-startup = يحقق VibeSQL <strong>بدءاً بارداً بحوالي 6 مللي ثانية</strong>، أسرع من SQLite (حوالي 6.5 مللي ثانية) وأسرع بكثير من DuckDB (حوالي 13 مللي ثانية). يُحمّل مسار التهيئة الأدنى لدينا فقط هياكل البيانات الوصفية الأساسية عند البدء.
bench-footprint-emb-disc-memory-title = كفاءة الذاكرة
bench-footprint-emb-disc-memory = ذروة الذاكرة أثناء البدء هي حوالي 7 ميجابايت لـ VibeSQL مقابل حوالي 3 ميجابايت لـ SQLite و حوالي 11 ميجابايت لـ DuckDB. يعكس الفرق عن SQLite مُحسّن الاستعلامات الأكثر تطوراً والبنية التحتية للتنفيذ العمودي المخصصة مسبقاً.
bench-footprint-emb-disc-roadmap-title = خارطة طريق تقليل الحجم
bench-footprint-emb-disc-flags = اختيار الميزات في وقت الترجمة لاستبعاد الوظائف غير المستخدمة
bench-footprint-emb-disc-lto = تحسين وقت الربط للبرنامج الكامل لإزالة الكود الميت
bench-footprint-emb-disc-modular = فصل المحرك الأساسي عن الميزات الاختيارية (مثل دوال النافذة)

# Footprint Server/WASM specific
bench-footprint-server-name = Footprint (Server/WASM)
bench-footprint-server-title = WASM Footprint
bench-footprint-server-description = تقيس <strong>مقاييس بصمة WASM</strong> حجم وحدة WebAssembly لنشر المتصفح، وهو أمر حاسم لتطبيقات الويب حيث يؤثر حجم التنزيل على تجربة المستخدم.
bench-footprint-server-ops-label = deployment targets
bench-footprint-server-note = أحجام WASM حاسمة لـ <strong>عمليات نشر الويب</strong> حيث يؤثر وقت التنزيل مباشرة على الوقت حتى التفاعل. أحجام Gzip هي الأكثر صلة حيث تقوم المتصفحات تلقائياً بفك ضغط محتوى gzip.
bench-footprint-server-note2 = <strong>ملاحظة:</strong> تم تصميم WASM لـ VibeSQL لأدنى حجم تنزيل مع الحفاظ على الامتثال الكامل لـ SQL:1999 في المتصفح.

# Footprint Server Descriptions
bench-footprint-wasm-size = WASM Size - Size of the WebAssembly module for browser deployment
bench-footprint-wasm-gzip = WASM (gzip) - Compressed size for web delivery

# Footprint Server Discussion
bench-footprint-srv-disc-wasm-title = WASM: 1.5 ميجابايت مضغوط
bench-footprint-srv-disc-wasm = تُضغط وحدة WebAssembly لـ VibeSQL إلى <strong>حوالي 1.5 ميجابايت بتنسيق gzip</strong>، مما يُمكّن تحميلات الصفحة الأولية السريعة. هذه قاعدة بيانات SQL:1999 كاملة مع دوال النافذة و CTEs ومعاملات ACID تعمل بالكامل في المتصفح.
bench-footprint-srv-disc-included-title = ما هو مضمن
bench-footprint-srv-disc-parser = محلل SQL كامل ومُحسّن استعلامات
bench-footprint-srv-disc-btree = محرك تخزين B-tree مع MVCC
bench-footprint-srv-disc-window = دوال النافذة والتجميعات المتقدمة
bench-footprint-srv-disc-cte = تعبيرات الجداول المشتركة (جملة WITH)
bench-footprint-srv-disc-acid = دعم معاملات ACID الكامل
bench-footprint-srv-disc-benefits-title = فوائد النشر في المتصفح
bench-footprint-srv-disc-benefits = يُلغي تشغيل SQL في المتصفح زمن الرحلة ذهاباً وإياباً إلى الخوادم، ويُمكّن التطبيقات التي تعمل بدون اتصال أولاً، ويحتفظ بالبيانات الحساسة على جهاز المستخدم. تم تصميم بناء WASM لـ VibeSQL لحالة الاستخدام هذه مع الحد الأدنى من التبعيات والاستخدام الفعال للذاكرة.
bench-footprint-srv-disc-roadmap-title = خارطة طريق WASM
bench-footprint-srv-disc-streaming = البدء بالتنفيذ أثناء تنزيل الوحدة
bench-footprint-srv-disc-indexeddb = تخزين دائم عبر جلسات المتصفح
bench-footprint-srv-disc-worker = تشغيل الاستعلامات خارج الخيط الرئيسي للواجهات المستجيبة

# Bullet point labels (used with descriptions)
bench-bullet-join-ordering = Join ordering
bench-bullet-hash-sizing = Hash table sizing
bench-bullet-vectorized = Vectorized joins
bench-bullet-inl-joins = Index-nested-loop joins
bench-bullet-cte-materialization = CTE materialization
bench-bullet-decorrelation = Subquery decorrelation
bench-bullet-star-optimization = Star schema optimization
bench-bullet-lock-free = Lock-free reads
bench-bullet-optimistic = Optimistic concurrency
bench-bullet-btree = In-memory B-tree
bench-bullet-prepared = Prepared statement caching
bench-bullet-bulk-inserts = Bulk inserts
bench-bullet-non-indexed = Non-indexed updates
bench-bullet-deletes = Deletes
bench-bullet-connection-pooling = Connection pooling
bench-bullet-stmt-caching = Prepared statement caching
bench-bullet-extended-protocol = Extended query protocol
bench-bullet-feature-flags = Feature flags
bench-bullet-lto = LTO optimization
bench-bullet-modular = Modular builds
bench-bullet-streaming = Streaming compilation
bench-bullet-indexeddb = IndexedDB persistence
bench-bullet-worker = Worker thread support

# =============================================================================
# Conformance Page
# =============================================================================

# Overview section
conformance-sql-conformance = SQL Conformance
conformance-testing-against = Testing against SQLLogicTest - the industry standard SQL test suite
conformance-full-pass-rate = 100% File Pass Rate Achieved!
conformance-tests-passing = Tests Passing
conformance-files-passing = Files Passing
conformance-loading = Loading conformance report...
conformance-error-loading = Error Loading Report
conformance-no-data = No conformance data available

# Category breakdown
conformance-category-title = Test Coverage by Category
conformance-category-header = Category
conformance-pass-rate-header = Pass Rate
conformance-progress-header = Progress
conformance-tests-header = Tests
conformance-cat-select = SELECT Queries
conformance-cat-aggregates = Aggregates
conformance-cat-joins = JOINs
conformance-cat-expressions = Expressions
conformance-cat-subqueries = Subqueries
conformance-cat-index = Index Operations
conformance-cat-ddl = DDL Statements
conformance-cat-evidence = Evidence Tests
conformance-cat-random = Random Tests
conformance-cat-other = Other Tests

# Timeline
conformance-timeline-title = Pass Rate History
conformance-timeline-desc = Conformance progress over the last 90 days
conformance-timeline-loading = Loading chart data...

# Milestones
conformance-milestones-title = Milestones

# Running tests locally
conformance-running-locally-title = Running Tests Locally
conformance-run-sqltest = # Run SQL:1999 conformance tests
conformance-run-sqllogictest = # Run SQLLogicTest suite (takes hours)
conformance-generate-coverage = # Generate coverage report
conformance-open-coverage = # Open coverage report

bench-table-query = Query
bench-tpcc-disc-duckdb = DuckDB achieves only ~385 TPS on TPC-C (60x slower than VibeSQL, 12x slower than SQLite). This is expected: DuckDB is an <strong>analytical (OLAP) database</strong> optimized for large batch operations, not single-row transactions. Its columnar storage format excels at scanning millions of rows but adds overhead for point lookups and small updates that dominate OLTP workloads like TPC-C.
bench-tpcc-disc-duckdb-title = Why DuckDB Lags on OLTP
bench-tpcc-transactions-label = transactions executed

# Conformance page (English placeholders)
conformance-additional-features = Additional Features
conformance-bottom-line = <strong>Bottom Line:</strong> We use two complementary test suites to ensure both standards conformance (sqltest) and practical correctness (SQLLogicTest). High pass rates in both demonstrate serious SQL:1999 implementation quality, though formal Core certification would require testing against official NIST suites.
conformance-commit = Commit:
conformance-core-explanation = SQL:1999 Core is the official mandatory feature set defined in the SQL:1999 (ISO/IEC 9075:1999) standard. It consists of approximately 169 required features that any database claiming Core compliance must implement. Official Core compliance is verified through the NIST SQL Test Suite, not community test suites.
conformance-core-features = Core Features (E-Series)
conformance-coverage-point = <span class="font-medium">Coverage:</span> sqltest covers 739 standard feature tests; SQLLogicTest covers practical scenarios
conformance-e011 = Numeric data types
conformance-e021 = Character string types
conformance-e031 = Identifiers
conformance-e051 = Basic query specification
conformance-e061 = Basic predicates and search conditions
conformance-e071 = Basic query expressions
conformance-e081 = Basic privileges
conformance-e091 = Set functions
conformance-e101 = Basic data manipulation
conformance-e111 = Single row SELECT statement
conformance-e121 = Basic cursor support
conformance-e131 = Null value support
conformance-e141 = Basic integrity constraints
conformance-e151 = Transaction support
conformance-e161 = SQL comments
conformance-error-label = Error:
conformance-errors = Errors
conformance-explanation-title = Understanding Our Test Suites
conformance-f031 = Basic schema manipulation
conformance-failed = Failed
conformance-failing-tests-desc = The following tests are currently failing. Click to expand details.
conformance-failing-tests-title = Failing Tests
conformance-files-of-passing = { $passed } of { $total } test files passing
conformance-generated = Generated:
conformance-how-complement = How do they complement each other?
conformance-overall-pass-rate = Overall Pass Rate
conformance-pass-rates-mean = Our <strong>{ $sqltestRate }% sqltest pass rate</strong> ({ $sqltestPassed }/{ $sqltestTotal } tests) demonstrates strong SQL:1999 grammar conformance. { $sltInfo } Together, these results indicate comprehensive SQL:1999 compliance, though they do not constitute official Core certification.
conformance-passed = Passed
conformance-philosophy-point = <span class="font-medium">Philosophy:</span> sqltest says "can you parse this?"; SQLLogicTest says "does this work correctly?"
conformance-slt-ddl = DDL Tests
conformance-slt-desc = Results from the comprehensive <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">SQLLogicTest</a> suite containing ~5.9 million tests across 623 test files from the official SQLite corpus.
conformance-slt-evidence = Evidence Tests
conformance-slt-explanation = <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">SQLLogicTest</a> is a comprehensive test suite originally developed for SQLite, containing ~5.9 million SQL test cases across 623 test files. It tests practical correctness by running real-world queries and validating results. This suite focuses on semantic correctness and edge cases rather than pure grammar conformance.
conformance-slt-index = Index Tests
conformance-slt-note = <strong>Note:</strong> SQLLogicTest provides a different perspective from sqltest. While sqltest focuses on BNF grammar conformance from the SQL:1999 specification, SQLLogicTest contains millions of real-world SQL queries testing practical correctness across a wide range of scenarios.
conformance-slt-other = Other Tests
conformance-slt-pass-info = Our <strong>{ $sltRate }% SQLLogicTest pass rate</strong> ({ $sltPassed }/{ $sltTotal } test files) shows we handle real-world queries correctly.
conformance-slt-random = Random Tests
conformance-slt-select = SELECT Tests
conformance-slt-title = SQLLogicTest Results
conformance-slt-validates = <span class="font-medium">SQLLogicTest (Result-driven):</span> Validates semantic correctness with millions of real queries
conformance-sqltest-desc = Results from <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">sqltest</a> - a community-maintained BNF-driven conformance test suite derived from the SQL:1999 standard, containing 739 tests covering Core and Foundation features.
conformance-sqltest-explanation = <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">sqltest</a> is a community-maintained test suite by Elliot Chance that provides BNF-driven conformance tests derived from the SQL:1999 standard. It contains 739 tests covering Core and Foundation features across E-series and F-series test categories. This suite tests whether our implementation conforms to the SQL:1999 grammar specification.
conformance-sqltest-title = sqltest Results
conformance-sqltest-validates = <span class="font-medium">sqltest (BNF-driven):</span> Validates grammar conformance to SQL:1999 standard specifications
conformance-status = Status:
conformance-test-categories = Test Categories
conformance-test-coverage = Test Coverage
conformance-tests-of-passing = { $passed } of { $total } tests passing
conformance-view-failing = View failing test details ({ $count } tests)
conformance-what-is-core = What is SQL:1999 Core?
conformance-what-is-slt = What is SQLLogicTest?
conformance-what-is-sqltest = What is sqltest?
conformance-what-mean = What do our pass rates mean?

# PostgreSQL Regression Tests
conformance-pgsql-title = اختبارات الانحدار لـ PostgreSQL
conformance-pgsql-desc = نتائج تشغيل <a href="https://www.postgresql.org/docs/current/regress.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">مجموعة اختبارات الانحدار لـ PostgreSQL</a> - مجموعة الاختبارات القياسية المستخدمة للتحقق من توافق PostgreSQL.
conformance-pgsql-tests-passing = اختبارات ناجحة
conformance-pgsql-tests-excluded = اختبارات مستبعدة
conformance-pgsql-pass-rate = معدل النجاح
conformance-pgsql-excluded-reason = الاختبارات المستبعدة تستخدم ميزات خاصة بـ PostgreSQL غير قابلة للتطبيق على VibeSQL
conformance-pgsql-note = <strong>ملاحظة:</strong> تتحقق اختبارات الانحدار لـ PostgreSQL من سلوك SQL مقارنة بالتنفيذ المرجعي لـ PostgreSQL. تتضمن الاختبارات المستبعدة ميزات خاصة بـ PostgreSQL مثل كتالوجات النظام واللغات الإجرائية ووحدات الإضافات.

# قسم مجموعة اختبارات SQLite TCL
conformance-tcl-title = مجموعة اختبارات SQLite TCL
conformance-tcl-desc = نتائج <a href="https://www.sqlite.org/testing.html" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">مجموعة اختبارات TCL</a> القياسية من SQLite التي تحتوي على { $fileCount } ملف اختبار. هذه المجموعة هي المعيار الذهبي لاختبارات التوافق مع SQLite.
conformance-tcl-overall-rate = معدل النجاح الإجمالي
conformance-tcl-tests-passing = { $passed } من { $total } اختبار ناجح
conformance-tcl-passed = ناجح
conformance-tcl-failed = فاشل
conformance-tcl-skipped = تم تخطيه
conformance-tcl-total = الإجمالي
conformance-tcl-categories-title = فئات الاختبارات
conformance-tcl-category = الفئة
conformance-tcl-rate = المعدل
conformance-tcl-progress = التقدم
conformance-tcl-tests = الاختبارات
conformance-tcl-common-failures = الإخفاقات الشائعة
conformance-tcl-failure-patterns = أعلى { $count } أنماط فشل حسب عدد الحدوث
conformance-tcl-about-title = حول اختبارات TCL:
conformance-tcl-about-text = مجموعة اختبارات TCL من SQLite هي اختبار المطابقة القياسي لتوافق SQLite. تختبر سلوكيات SQLite المحددة والخصائص والحالات الحدية التي قد لا تغطيها مجموعات اختبار SQL القياسية. تشير معدلات النجاح العالية هنا إلى توافق قوي مع SQLite لسيناريوهات ترحيل التطبيقات.
