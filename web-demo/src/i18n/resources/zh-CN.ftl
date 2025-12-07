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
