# VibeSQL Web UI - 简体中文

# Page titles
page-title = VibeSQL - AI驱动的SQL:1999数据库
demo-title = VibeSQL 演示
benchmarks-title = 性能基准测试 - VibeSQL
benchmarks-heading = VibeSQL - 性能基准测试
conformance-title = 合规性报告 - VibeSQL
conformance-heading = 合规性报告
conformance-subtitle = SQL:1999 标准合规性测试

# Navigation
nav-showcase = SQL:1999 展示
nav-conformance = 查看 sqltest 结果
nav-sqllogictest = 查看 SQLLogicTest 结果

# Editor section
editor-title = SQL 编辑器
editor-storage = 存储
editor-storage-init = 初始化中...
editor-execute = 执行查询

# Results section
results-title = 结果
results-empty = 执行查询以查看结果
results-loading = 加载中...
results-rows = { $count } 行
results-rows-with-time = { $count } 行（{ $time }毫秒）
results-copy = 复制到剪贴板
results-export = 导出 CSV
results-limit-warning = 显示 { $total } 行中的前 { $limit } 行。使用 LIMIT 子句来优化查询。

# Examples sidebar
examples-title = 示例
examples-basic = 基本查询
examples-advanced = 高级查询

# Database selector
db-select-label = 数据库

# Footer
footer-tagline = VibeSQL - WebAssembly 中的 SQL:1999 数据库
footer-deployed = 部署时间：{ $date }

# Theme
theme-toggle-dark = 切换到深色模式
theme-toggle-light = 切换到浅色模式

# Locale
locale-select = 选择语言

# Messages
msg-query-success = 查询执行成功
msg-rows-affected = { $count } 行受影响

# Errors
error-generic = 发生错误
error-query-failed = 查询失败

# Editor
editor-placeholder = 在此输入SQL查询... (Ctrl+Enter 或 Cmd+Enter 执行)

# Navigation links
nav-terminal = SQL终端演示
nav-compliance = SQL测试合规报告
nav-benchmarks = 性能基准测试
nav-github = GitHub 仓库
nav-home = 首页

# Results
results-success-zero = 查询执行成功（0行）
results-null = 空值

# Help Modal
help-title = 键盘快捷键和帮助
help-close = 关闭
help-editor-shortcuts = 编辑器快捷键
help-navigation = 导航
help-results-actions = 结果操作
help-tips = 提示
help-shortcut-execute = 执行当前查询
help-shortcut-comment = 切换行注释
help-shortcut-indent = 缩进选择
help-shortcut-show-help = 显示此帮助对话框
help-shortcut-close-help = 关闭帮助对话框
help-action-copy = 复制到剪贴板
help-action-copy-desc = 以制表符分隔值复制结果
help-action-export = 导出 CSV
help-action-export-desc = 将结果下载为CSV文件
help-tip-limit = 出于性能考虑，结果限制为1,000行。使用LIMIT子句优化查询。
help-tip-time = 执行时间随查询结果一起显示。
help-tip-syntax = 编辑器支持SQL语法高亮和自动补全。
help-tip-theme = 使用主题按钮在浅色/深色模式之间切换。
help-got-it = 明白了！

# Showcase Navigation
showcase-title = SQL:1999核心展示
showcase-description = 交互式探索已实现的SQL:1999核心功能
showcase-complete = 已完成 { $percent }%
showcase-categories = 功能类别
showcase-legend = 状态图例
showcase-status-implemented = 完全实现
showcase-status-partial = 部分实现
showcase-status-planned = 计划中

# Showcase category labels
showcase-cat-compliance = 合规性仪表板
showcase-cat-data-types = 数据类型
showcase-cat-dml = DML 操作
showcase-cat-predicates = 谓词和运算符
showcase-cat-joins = 连接
showcase-cat-subqueries = 子查询
showcase-cat-aggregates = 聚合和 GROUP BY
showcase-cat-ddl = DDL 和约束

# Common showcase elements
showcase-interactive-examples = 交互式示例
showcase-try-example = 试试这个例子
showcase-progress = { $total } { $type }中的 { $implemented } 个 ({ $percent }%)
showcase-table-status = 状态
showcase-table-category = 类别
showcase-table-description = 描述
showcase-table-syntax = 语法
showcase-table-use-case = 用例

# Status labels
status-implemented = 已实现
status-partial = 部分
status-planned = 计划中

# Aggregates Showcase
aggregates-title = SQL 聚合和 GROUP BY
aggregates-description = SQL:1999 核心聚合函数和分组功能
aggregates-reference = 聚合函数参考
aggregates-table-function = 函数
aggregates-progress-type = 函数
aggregates-ex-basic = 基本聚合函数
aggregates-ex-group-single = GROUP BY（单列）
aggregates-ex-group-multiple = GROUP BY（多列）
aggregates-ex-having = HAVING 子句
aggregates-ex-orderby = 带聚合的 ORDER BY
aggregates-ex-null = 聚合中的 NULL 处理

# DML Operations Showcase
dml-title = DML 操作（数据操作语言）
dml-description = 用于查询和修改数据的 SQL:1999 核心操作
dml-reference = DML 操作参考
dml-table-operation = 操作
dml-progress-type = 操作
dml-ex-select-basic = SELECT - 基本查询
dml-ex-select-ordering = SELECT - 排序和限制
dml-ex-insert = INSERT 操作
dml-ex-update = UPDATE 操作
dml-ex-delete = DELETE 操作
dml-ex-combined = 组合 CRUD 工作流

# Data Types Showcase
datatypes-title = SQL:1999 核心数据类型
datatypes-description = 探索 SQL:1999 核心规范中定义的基本数据类型
datatypes-reference = 数据类型参考
datatypes-table-type = 类型名称
datatypes-table-example = 示例值
datatypes-table-spec = 规范
datatypes-progress-type = 类型
datatypes-ex-numeric = 使用数值类型
datatypes-ex-null = NULL 处理和三值逻辑
datatypes-ex-comparisons = 类型比较和操作

# JOINs Showcase
joins-title = SQL 连接
joins-description = 用于组合多个表数据的 SQL:1999 核心 JOIN 操作
joins-reference = JOIN 类型参考
joins-table-type = JOIN 类型
joins-progress-type = JOIN 类型
joins-category-suffix = 连接
joins-ex-sample = 示例数据设置
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = 多表 JOIN

# Predicates Showcase
predicates-title = 谓词和运算符
predicates-description = 用于过滤和逻辑操作的 SQL:1999 谓词
predicates-reference = 谓词参考
predicates-table-predicate = 谓词
predicates-progress-type = 谓词
predicates-ex-comparison = 比较运算符
predicates-ex-between = BETWEEN 和范围谓词
predicates-ex-null = NULL 谓词和三值逻辑
predicates-ex-boolean = 布尔逻辑（AND、OR、NOT）
predicates-ex-in = 带子查询的 IN 谓词
predicates-ex-combined = 组合谓词操作

# Subqueries Showcase
subqueries-title = SQL 子查询
subqueries-description = 用于嵌套查询操作的 SQL:1999 核心子查询功能
subqueries-reference = 子查询类型参考
subqueries-table-type = 子查询类型
subqueries-progress-type = 子查询类型
subqueries-ex-scalar-select = SELECT 中的标量子查询
subqueries-ex-scalar-where = WHERE 中的标量子查询
subqueries-ex-derived = 派生表（FROM 中的子查询）
subqueries-ex-in = 带子查询的 IN 谓词
subqueries-ex-correlated = 相关子查询
subqueries-ex-nested = 嵌套子查询

# =============================================================================
# 基准测试页面
# =============================================================================

# 部分标题
bench-section-embedded = 嵌入式
bench-section-server = 服务器
bench-results-title = 基准测试结果
bench-perf-comparison = 性能比较
bench-methodology-title = 测试方法
bench-analysis-roadmap = 分析与路线图

# 摘要卡片
bench-vs-sqlite = vs SQLite
bench-vs-duckdb = vs DuckDB
bench-vs-mysql = vs MySQL
bench-ops-tested = 测试的操作
bench-last-updated = 最后更新
bench-avg-speedup = 平均加速比
bench-from-main = 来自 main 分支
bench-loading = 加载中...
bench-na = N/A
bench-faster = 快 { $value } 倍
bench-slower = 慢 { $value } 倍
bench-speedup = { $value } 倍
bench-startup-time-label = 启动时间
bench-download-size = 下载大小
bench-uncompressed = 未压缩
bench-size-metrics = 大小指标
bench-failed = 失败
bench-failed-title = 查询失败（超时或错误）
bench-no-wasm-data = 无可用的 WASM 数据

# 表头
bench-table-operation = 操作
bench-table-vibesql = VibeSQL
bench-table-vibesql-server = VibeSQL Server
bench-table-sqlite = SQLite
bench-table-duckdb = DuckDB
bench-table-mysql = MySQL
bench-table-loading = 正在加载基准测试结果...
bench-vibesql-server-title = 通过 PostgreSQL 协议的 VibeSQL

# 通用基准测试术语
bench-hardware = 硬件
bench-benchmark-framework = 基准测试框架
bench-scale-factor = 规模因子
bench-data = 数据
bench-databases-tested = 测试的数据库
bench-execution-mode = 执行模式
bench-measurement = 测量
bench-workload = 工作负载
bench-transaction-mix = 事务组合
bench-warehouses = 仓库数
bench-concurrency = 并发性
bench-acid-compliance = ACID 合规性
bench-mode = 模式
bench-workload-types = 工作负载类型
bench-table-size = 表大小
bench-index-types = 索引类型
bench-operations = 操作
bench-databases = 数据库
bench-protocol-overhead = 协议开销
bench-binary-size = 二进制大小
bench-startup-time = 启动时间
bench-peak-memory = 峰值内存
bench-schema = 模式
bench-query-count = 查询数量
bench-query-types = 查询类型
bench-sql-features = SQL 功能
bench-wasm-size = WASM 大小
bench-wasm-gzip = WASM (gzip)
bench-wasm-brotli = WASM (brotli)

# TPC-H 相关
bench-tpch-name = TPC-H
bench-tpch-title = TPC-H 决策支持基准测试
bench-tpch-description = 这些基准测试使用行业标准的 <strong>TPC-H 基准测试套件</strong>，它通过涉及聚合、连接、子查询和排序的复杂分析查询来模拟实际的决策支持工作负载。
bench-tpch-ops-label = TPC-H 查询
bench-tpch-note-intro = 所有基准测试都测量端到端的查询执行时间，包括解析、规划、执行和结果物化。这代表了分析工作负载的<strong>实际 SQL 引擎性能</strong>。
bench-tpch-note-queries = <strong>注意：</strong>TPC-H 查询测试 SQL 性能的不同方面：简单聚合（Q1、Q6）、复杂连接（Q2-Q5、Q7-Q10）、子查询（Q11-Q15）和高级分析（Q16-Q22）。将鼠标悬停在上表的查询名称上可查看描述。

# TPC-H 讨论
bench-tpch-disc-excels-title = VibeSQL 的优势
bench-tpch-disc-excels = VibeSQL 在<strong>扫描密集型聚合查询</strong>（Q1、Q6、Q14、Q15、Q20）上表现出色，这些查询中我们的列式执行引擎和 SIMD 加速聚合发挥了优势。这些查询涉及过滤大型表并计算聚合，而无需复杂的连接模式。
bench-tpch-disc-targets-title = 当前优化目标
bench-tpch-disc-targets = 多路连接查询（Q3、Q5、Q7-Q10、Q18、Q19、Q21）目前 SQLite 领先。主要瓶颈是我们的哈希连接实现，尚未采用与 SQLite 经过数十年改进的 B-tree 连接相同级别的优化。正在积极开发的特定领域：
bench-tpch-disc-join-ordering = 改进基数估计以获得更好的连接顺序选择
bench-tpch-disc-hash-sizing = 自适应哈希表增长和大型连接的磁盘溢出
bench-tpch-disc-vectorized = 连接内循环中的批处理以提高缓存利用率
bench-tpch-disc-inl-joins = 在有益时利用 B-tree 索引
bench-tpch-disc-path-title = 通往领先的道路
bench-tpch-disc-path = VibeSQL 的架构专为现代硬件设计，具有列式存储、向量化执行和无锁并发等功能。随着这些优化的成熟，我们预计 VibeSQL 将在所有 TPC-H 查询中实现一致的领先地位。基础设计支持传统行存储数据库无法轻易改造的并行性和 SIMD。

# TPC-H 查询描述
bench-tpch-q1 = 定价汇总报告 - 使用 GROUP BY 和 ORDER BY 的价格聚合
bench-tpch-q2 = 最低成本供应商 - 带 ORDER BY 和 LIMIT 的 3 表 JOIN
bench-tpch-q3 = 发货优先级 - 带聚合的 3 表 JOIN
bench-tpch-q4 = 订单优先级检查 - 相关 EXISTS 子查询
bench-tpch-q5 = 本地供应商销量 - 带复杂过滤的 6 表 JOIN
bench-tpch-q6 = 预测收入变化 - 带 BETWEEN 和 SUM 的 WHERE 过滤
bench-tpch-q7 = 批量发货 - 带 SUBSTR 和日期过滤的 6 表 JOIN
bench-tpch-q8 = 国内市场份额 - 带 CASE 表达式的 7 表 JOIN
bench-tpch-q9 = 产品类型利润测量 - 带聚合的 4 表 JOIN
bench-tpch-q10 = 退货报告 - 带 TOP-N LIMIT 的 4 表 JOIN
bench-tpch-q11 = 重要库存识别 - HAVING 子句中的子查询
bench-tpch-q12 = 发货模式优先级 - 带日期逻辑的 CASE 聚合
bench-tpch-q13 = 客户分布 - 带子查询的 LEFT OUTER JOIN
bench-tpch-q14 = 促销效果 - 带 CASE 的条件聚合
bench-tpch-q15 = 顶级供应商 - 带 MAX 的嵌套子查询
bench-tpch-q16 = 零件/供应商关系 - 带 DISTINCT 的 NOT IN 子查询
bench-tpch-q17 = 小批量订单收入 - WHERE 中的相关子查询
bench-tpch-q18 = 大批量客户 - 带 HAVING 的 GROUP BY
bench-tpch-q19 = 折扣收入 - 复杂的 OR 条件
bench-tpch-q20 = 潜在零件促销 - 带 GROUP BY/HAVING 的 IN 子查询
bench-tpch-q21 = 延迟订单的供应商 - 多表 EXISTS
bench-tpch-q22 = 全球销售机会 - 带 NOT EXISTS 子查询的 SUBSTR

# TPC-DS 相关
bench-tpcds-name = TPC-DS
bench-tpcds-title = TPC-DS 决策支持基准测试
bench-tpcds-description = <strong>TPC-DS</strong> 是 TPC-H 的继任者，包含 99 个查询，模拟具有更复杂查询模式的现代决策支持系统，包括多个事实表、雪花模式和高级 SQL 功能。
bench-tpcds-ops-label = TPC-DS 查询
bench-tpcds-note-intro = TPC-DS 查询比 TPC-H 复杂得多，测试高级 SQL 功能，如窗口函数、公共表表达式（WITH 子句）以及跨多个事实表和维度表的复杂连接模式。
bench-tpcds-note-remaining = <strong>注意：</strong>剩余不支持的查询需要尚未实现的功能，如 INTERSECT/EXCEPT 或特定的日期算术函数。

# TPC-DS 讨论
bench-tpcds-disc-coverage-title = SQL:1999 功能覆盖
bench-tpcds-disc-coverage = TPC-DS 测试最苛刻的 SQL 功能。VibeSQL 通过了 <strong>99 个查询中的 88 个</strong>，展示了 SQL:1999 的广泛覆盖，包括 ROLLUP、CUBE、GROUPING()、具有复杂框架的窗口函数和递归 CTE。剩余查询需要 INTERSECT/EXCEPT 集合操作。
bench-tpcds-disc-optimization-title = 复杂查询优化
bench-tpcds-disc-optimization = TPC-DS 查询通常连接 10+ 个带有相关子查询的表。当前重点领域：
bench-tpcds-disc-cte = 在物化 CTE 和内联 CTE 之间做出明智决策
bench-tpcds-disc-decorrelation = 在有益时将相关子查询转换为连接
bench-tpcds-disc-star = 分析模式的事实-维度连接排序
bench-tpcds-disc-toward-title = 迈向 99/99
bench-tpcds-disc-toward = INTERSECT 和 EXCEPT 是计划添加的功能，将启用剩余查询。这些集合操作自然地融入我们现有的查询代数，并将作为类似于 DISTINCT 处理的基于哈希的运算符实现。

# TPC-C 相关
bench-tpcc-name = TPC-C
bench-tpcc-title = TPC-C 在线事务处理基准测试
bench-tpcc-description = <strong>TPC-C 基准测试</strong>模拟完整的订单录入环境，包含复杂事务的组合，包括订单录入、付款处理、订单状态查询、交付处理和库存水平监控。
bench-tpcc-ops-label = TPC-C 事务
bench-tpcc-note-intro = TPC-C 测量每分钟事务数（tpmC）并测试数据库处理具有复杂业务逻辑的并发事务的能力。此基准测试对于评估<strong>事务工作负载性能</strong>至关重要。
bench-tpcc-note-results = <strong>注意：</strong>结果显示平均事务延迟。越低越好。TPC-C 对具有严格一致性要求的写密集型工作负载尤其苛刻。

# TPC-C 事务描述
bench-tpcc-new-order = 新订单 - 包含库存检查和订单创建的复杂事务
bench-tpcc-payment = 付款 - 更新客户余额和仓库/地区总额
bench-tpcc-order-status = 订单状态 - 客户订单历史的只读查询
bench-tpcc-delivery = 交付 - 待处理订单的批处理
bench-tpcc-stock-level = 库存水平 - 统计最近订单中低于阈值的商品

# TPC-C 讨论
bench-tpcc-disc-faster-title = 比 SQLite 快 42 倍
bench-tpcc-disc-faster = VibeSQL 实现了<strong>每秒约 79,000 个事务</strong>，而 SQLite 约为 1,900 TPS，提升了 42 倍。这种显著的加速来自我们的无锁 MVCC 架构，避免了 SQLite 在每次写操作上的粗粒度锁定。
bench-tpcc-disc-dominates-title = VibeSQL 在 OLTP 中占主导地位的原因
bench-tpcc-disc-lockfree = MVCC 允许读者和写者在不阻塞的情况下并发进行
bench-tpcc-disc-optimistic = 事务仅在提交时发生冲突，而不是在执行期间
bench-tpcc-disc-btree = 专为内存工作负载优化的专用索引结构
bench-tpcc-disc-prepared = 查询计划编译一次后重复使用
bench-tpcc-disc-scaling-title = 进一步扩展
bench-tpcc-disc-scaling = 当前结果是单线程的。VibeSQL 的架构支持多线程事务处理，我们预计随着添加并行执行支持，将实现近线性扩展。我们的目标是在现代多核硬件上达到 500K+ TPS。

# Sysbench 嵌入式相关
bench-sysbench-embedded-name = Sysbench（嵌入式）
bench-sysbench-embedded-title = Sysbench 微基准测试（嵌入式）
bench-sysbench-embedded-description = <strong>Sysbench</strong> 提供专注的微基准测试，隔离特定的数据库操作。这些测试测量基本操作的原始性能，而没有完整事务工作负载的复杂性。
bench-sysbench-embedded-ops-label = Sysbench 操作
bench-sysbench-embedded-note = 嵌入式模式在进程内运行数据库，零网络开销，非常适合最小延迟至关重要的单进程应用程序。

# Sysbench 操作描述
bench-sysbench-point-select = 点查询 - 按主键查找单行
bench-sysbench-insert = 插入 - 向表中插入新行
bench-sysbench-update-index = 更新索引 - 更新索引列（k = k + 1）
bench-sysbench-update-non-index = 更新非索引 - 更新非索引列
bench-sysbench-delete = 删除 - 按主键删除行
bench-sysbench-range-queries = 范围查询 - 简单、SUM、ORDER BY 和 DISTINCT 范围扫描

# Sysbench 嵌入式讨论
bench-sysbench-emb-disc-point-title = 点查询：VibeSQL 领先
bench-sysbench-emb-disc-point = VibeSQL 的直接 API 实现了<strong>每次点查询约 137ns</strong>，与 SQLite 相当，大幅超越 DuckDB（约 140µs）。我们的 B-tree 实现针对单行查找进行了优化，具有最小的指针追踪和缓存友好的节点布局。
bench-sysbench-emb-disc-index-title = 索引更新：快 2 倍
bench-sysbench-emb-disc-index = VibeSQL 的索引更新在<strong>约 740ns vs SQLite 的约 1.6µs</strong>下运行。我们的 MVCC 设计允许就地索引更新，无需每次操作的预写日志开销。
bench-sysbench-emb-disc-improve-title = 改进领域
bench-sysbench-emb-disc-bulk = SQLite 的批量插入路径高度优化；我们正在添加批量 B-tree 操作
bench-sysbench-emb-disc-nonindex = 非索引列的全表扫描需要谓词下推优化
bench-sysbench-emb-disc-deletes = 我们基于墓碑的删除有清理开销；计划进行压缩改进
bench-sysbench-emb-disc-duckdb-title = DuckDB 比较
bench-sysbench-emb-disc-duckdb = DuckDB 针对分析工作负载进行了优化，而不是微操作。这里 100-1000 倍较慢的结果反映了以单行延迟换取批量吞吐量的架构选择（列式存储、向量化执行）。VibeSQL 同时针对两种用例。

# Sysbench 服务器相关
bench-sysbench-server-name = Sysbench（服务器）
bench-sysbench-server-title = Sysbench 微基准测试（服务器）
bench-sysbench-server-description = <strong>Sysbench</strong> 服务器基准测试将 VibeSQL Server（PostgreSQL 协议）与 MySQL 进行比较，测量多客户端数据库部署的性能。
bench-sysbench-server-ops-label = Sysbench 操作
bench-sysbench-server-note = 服务器模式使用 PostgreSQL 协议，实现多客户端访问以及与现有 PostgreSQL 工具和驱动程序的兼容性。

# Sysbench 服务器讨论
bench-sysbench-srv-disc-protocol-title = PostgreSQL 协议
bench-sysbench-srv-disc-protocol = VibeSQL Server 实现了 PostgreSQL 协议，实现与现有 PostgreSQL 驱动程序和工具的兼容性。与嵌入式模式相比，这增加了每个查询约 10-50µs 的协议开销，但支持多客户端部署。
bench-sysbench-srv-disc-mysql-title = MySQL 比较
bench-sysbench-srv-disc-mysql = 服务器基准测试与 MySQL 进行比较，以评估 VibeSQL 作为传统客户端-服务器数据库的直接替代品。结果因操作类型而异，VibeSQL 在读密集型工作负载中显示出优势。
bench-sysbench-srv-disc-roadmap-title = 服务器路线图
bench-sysbench-srv-disc-pooling = 减少高吞吐量场景的连接建立开销
bench-sysbench-srv-disc-caching = 跨连接的查询计划服务器端缓存
bench-sysbench-srv-disc-extended = 完整的 PostgreSQL 扩展查询协议支持批量操作

# 占用空间嵌入式相关
bench-footprint-embedded-name = 占用空间（嵌入式）
bench-footprint-embedded-title = 原生二进制占用空间
bench-footprint-embedded-description = <strong>嵌入式占用空间基准测试</strong>测量原生数据库二进制文件的资源效率，比较二进制大小、冷启动时间和峰值内存使用量。
bench-footprint-embedded-ops-label = 比较的数据库
bench-footprint-embedded-note = 原生二进制占用空间对于<strong>嵌入式和边缘部署</strong>至关重要，其中二进制大小、启动延迟和内存消耗直接影响部署可行性。

# 占用空间嵌入式描述
bench-footprint-binary-size = 二进制大小 - 磁盘上编译后的数据库二进制文件大小
bench-footprint-startup-time = 启动时间 - 冷启动并执行第一个查询的时间
bench-footprint-peak-memory = 峰值内存 - 初始化期间的最大常驻集大小

# 占用空间嵌入式讨论
bench-footprint-emb-disc-size-title = 二进制大小：中等
bench-footprint-emb-disc-size = VibeSQL 约 <strong>17MB</strong>，介于 SQLite（约 5MB）和 DuckDB（约 45MB）之间。这反映了我们选择包含高级功能（窗口函数、CTE、列式执行），同时保持二进制对嵌入式部署可管理。
bench-footprint-emb-disc-startup-title = 启动：最快的冷启动
bench-footprint-emb-disc-startup = VibeSQL 实现了<strong>约 7.7ms 的冷启动</strong>，略快于 SQLite（约 8.2ms），显著快于 DuckDB（约 14.6ms）。我们的最小初始化路径在启动时仅加载必要的元数据结构。
bench-footprint-emb-disc-memory-title = 内存效率
bench-footprint-emb-disc-memory = 启动时的峰值内存：VibeSQL 约 7MB vs SQLite 约 3MB 和 DuckDB 约 11MB。与 SQLite 的差异反映了我们更复杂的查询优化器和预先分配的列式执行基础设施。
bench-footprint-emb-disc-roadmap-title = 大小缩减路线图
bench-footprint-emb-disc-flags = 编译时功能选择以排除未使用的功能
bench-footprint-emb-disc-lto = 全程序链接时优化以消除死代码
bench-footprint-emb-disc-modular = 将核心引擎与可选功能（例如窗口函数）分离

# 占用空间服务器/WASM 相关
bench-footprint-server-name = 占用空间（服务器/WASM）
bench-footprint-server-title = WASM 占用空间
bench-footprint-server-description = <strong>WASM 占用空间基准测试</strong>测量浏览器部署的 WebAssembly 模块大小，这对下载大小影响用户体验的 Web 应用程序至关重要。
bench-footprint-server-ops-label = 部署目标
bench-footprint-server-note = WASM 大小对于<strong>Web 部署</strong>至关重要，其中下载时间直接影响可交互时间。Gzip 大小最相关，因为浏览器会自动解压 gzip 内容。
bench-footprint-server-note2 = <strong>注意：</strong>VibeSQL WASM 设计为最小下载大小，同时保持浏览器中完整的 SQL:1999 合规性。

# 占用空间服务器描述
bench-footprint-wasm-size = WASM 大小 - 浏览器部署的 WebAssembly 模块大小
bench-footprint-wasm-gzip = WASM (gzip) - Web 交付的压缩大小

# 占用空间服务器讨论
bench-footprint-srv-disc-wasm-title = WASM：2.2MB 压缩
bench-footprint-srv-disc-wasm = VibeSQL 的 WebAssembly 模块压缩后<strong>约 2.2MB gzip</strong>，实现快速的初始页面加载。这是一个完整的 SQL:1999 数据库，具有窗口函数、CTE 和 ACID 事务，完全在浏览器中运行。
bench-footprint-srv-disc-included-title = 包含内容
bench-footprint-srv-disc-parser = 完整的 SQL 解析器和查询优化器
bench-footprint-srv-disc-btree = 带 MVCC 的 B-tree 存储引擎
bench-footprint-srv-disc-window = 窗口函数和高级聚合
bench-footprint-srv-disc-cte = 公共表表达式（WITH 子句）
bench-footprint-srv-disc-acid = 完整的 ACID 事务支持
bench-footprint-srv-disc-benefits-title = 浏览器部署优势
bench-footprint-srv-disc-benefits = 在浏览器中运行 SQL 消除了到服务器的往返延迟，支持离线优先应用程序，并将敏感数据保留在用户设备上。VibeSQL 的 WASM 构建针对此用例设计，具有最小依赖项和高效的内存使用。
bench-footprint-srv-disc-roadmap-title = WASM 路线图
bench-footprint-srv-disc-streaming = 在模块下载时开始执行
bench-footprint-srv-disc-indexeddb = 跨浏览器会话的持久存储
bench-footprint-srv-disc-worker = 在主线程外运行查询以实现响应式 UI

# 要点标签（与描述一起使用）
bench-bullet-join-ordering = 连接排序
bench-bullet-hash-sizing = 哈希表大小调整
bench-bullet-vectorized = 向量化连接
bench-bullet-inl-joins = 索引嵌套循环连接
bench-bullet-cte-materialization = CTE 物化
bench-bullet-decorrelation = 子查询去相关
bench-bullet-star-optimization = 星型模式优化
bench-bullet-lock-free = 无锁读取
bench-bullet-optimistic = 乐观并发
bench-bullet-btree = 内存 B-tree
bench-bullet-prepared = 预处理语句缓存
bench-bullet-bulk-inserts = 批量插入
bench-bullet-non-indexed = 非索引更新
bench-bullet-deletes = 删除
bench-bullet-connection-pooling = 连接池
bench-bullet-stmt-caching = 预处理语句缓存
bench-bullet-extended-protocol = 扩展查询协议
bench-bullet-feature-flags = 功能标志
bench-bullet-lto = LTO 优化
bench-bullet-modular = 模块化构建
bench-bullet-streaming = 流式编译
bench-bullet-indexeddb = IndexedDB 持久化
bench-bullet-worker = 工作线程支持

# =============================================================================
# 合规性页面
# =============================================================================

# 概述部分
conformance-sql-conformance = SQL 合规性
conformance-testing-against = 针对 SQLLogicTest 进行测试 - 行业标准 SQL 测试套件
conformance-full-pass-rate = 达成 100% 文件通过率！
conformance-tests-passing = 测试通过
conformance-files-passing = 文件通过
conformance-loading = 正在加载合规性报告...
conformance-error-loading = 加载报告错误
conformance-no-data = 无可用的合规性数据

# 类别细分
conformance-category-title = 按类别的测试覆盖率
conformance-category-header = 类别
conformance-pass-rate-header = 通过率
conformance-progress-header = 进度
conformance-tests-header = 测试
conformance-cat-select = SELECT 查询
conformance-cat-aggregates = 聚合
conformance-cat-joins = JOIN
conformance-cat-expressions = 表达式
conformance-cat-subqueries = 子查询
conformance-cat-index = 索引操作
conformance-cat-ddl = DDL 语句
conformance-cat-evidence = 证据测试
conformance-cat-random = 随机测试
conformance-cat-other = 其他测试

# 时间线
conformance-timeline-title = 通过率历史
conformance-timeline-desc = 过去 90 天的合规性进展
conformance-timeline-loading = 正在加载图表数据...

# 里程碑
conformance-milestones-title = 里程碑

# 本地运行测试
conformance-running-locally-title = 本地运行测试
conformance-run-sqltest = # 运行 SQL:1999 合规性测试
conformance-run-sqllogictest = # 运行 SQLLogicTest 套件（需要数小时）
conformance-generate-coverage = # 生成覆盖率报告
conformance-open-coverage = # 打开覆盖率报告

