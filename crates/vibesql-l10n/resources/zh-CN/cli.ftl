# VibeSQL CLI 本地化 - 简体中文
# 此文件包含 VibeSQL 命令行界面的所有用户可见字符串。

# =============================================================================
# REPL 横幅和基本消息
# =============================================================================

cli-banner = VibeSQL v{ $version } - 完全符合 SQL:1999 标准的数据库
cli-help-hint = 输入 \help 获取帮助，\quit 退出
cli-goodbye = 再见！
locale-changed = 语言已更改为 { $locale }

# =============================================================================
# 命令帮助文本（Clap 参数）
# =============================================================================

cli-about = VibeSQL - 完全符合 SQL:1999 标准的数据库

cli-long-about = VibeSQL 命令行界面

    使用模式：
      交互式 REPL：       vibesql (--database <文件>)
      执行命令：          vibesql -c "SELECT * FROM users"
      执行文件：          vibesql -f script.sql
      从标准输入执行：    cat data.sql | vibesql
      生成类型：          vibesql codegen --schema schema.sql --output types.ts

    交互式 REPL：
      不使用 -c、-f 或管道输入启动时，VibeSQL 进入交互式 REPL，
      支持 readline、命令历史和元命令，如：
        \d (表名)    - 描述表或列出所有表
        \dt          - 列出表
        \f <格式>    - 设置输出格式
        \copy        - 导入/导出 CSV/JSON
        \help        - 显示所有 REPL 命令

    子命令：
      codegen           从数据库模式生成 TypeScript 类型

    配置：
      可在 ~/.vibesqlrc（TOML 格式）中配置设置。
      配置节：display、database、history、query

    示例：
      # 使用内存数据库启动交互式 REPL
      vibesql

      # 使用持久化数据库文件
      vibesql --database mydata.db

      # 执行单个命令
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # 运行 SQL 脚本文件
      vibesql -f schema.sql -v

      # 从 CSV 导入数据
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # 将查询结果导出为 JSON
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # 从模式文件生成 TypeScript 类型
      vibesql codegen --schema schema.sql --output src/types.ts

      # 从运行中的数据库生成 TypeScript 类型
      vibesql codegen --database mydata.db --output src/types.ts

# 参数帮助字符串
arg-database-help = 数据库文件路径（未指定时使用内存数据库）
arg-file-help = 从文件执行 SQL 命令
arg-command-help = 直接执行 SQL 命令并退出
arg-stdin-help = 从标准输入读取 SQL 命令（管道输入时自动检测）
arg-verbose-help = 执行文件/标准输入时显示详细输出
arg-format-help = 查询结果的输出格式
arg-lang-help = 设置显示语言（例如：en-US、es、zh-CN）

# =============================================================================
# Codegen 子命令
# =============================================================================

codegen-about = 从数据库模式生成 TypeScript 类型

codegen-long-about = 从 VibeSQL 数据库模式生成 TypeScript 类型定义。

    此命令为数据库中的所有表创建 TypeScript 接口，
    以及用于运行时类型检查和 IDE 支持的元数据对象。

    输入源：
      --database <文件>  从现有数据库文件生成
      --schema <文件>    从 SQL 模式文件生成（CREATE TABLE 语句）

    输出：
      --output <文件>    将生成的类型写入此文件（默认：types.ts）

    选项：
      --camel-case       将列名转换为驼峰命名
      --no-metadata      跳过生成表元数据对象

    示例：
      # 从数据库文件
      vibesql codegen --database mydata.db --output src/db/types.ts

      # 从 SQL 模式文件
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # 使用驼峰命名属性名
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = 包含 CREATE TABLE 语句的 SQL 模式文件
codegen-output-help = 生成的 TypeScript 输出文件路径
codegen-camel-case-help = 将列名转换为驼峰命名
codegen-no-metadata-help = 跳过生成表元数据对象

codegen-from-schema = 正在从模式文件生成 TypeScript 类型：{ $path }
codegen-from-database = 正在从数据库生成 TypeScript 类型：{ $path }
codegen-written = TypeScript 类型已写入：{ $path }
codegen-error-no-source = 必须指定 --database 或 --schema。
    使用 'vibesql codegen --help' 获取使用信息。

# =============================================================================
# 元命令帮助（\help 输出）
# =============================================================================

help-title = 元命令：
help-describe = \d (表名)      - 描述表或列出所有表
help-tables = \dt             - 列出表
help-schemas = \ds             - 列出模式
help-indexes = \di             - 列出索引
help-roles = \du             - 列出角色/用户
help-format = \f <格式>       - 设置输出格式（table、json、csv、markdown、html）
help-timing = \timing         - 切换查询计时
help-copy-to = \copy <表> TO <文件>   - 将表导出到 CSV/JSON 文件
help-copy-from = \copy <表> FROM <文件> - 从 CSV 文件导入到表
help-save = \save (文件)    - 将数据库保存为 SQL 转储文件
help-errors = \errors         - 显示最近的错误历史
help-help = \h, \help      - 显示此帮助
help-quit = \q, \quit      - 退出

help-sql-title = SQL 自省：
help-show-tables = SHOW TABLES                  - 列出所有表
help-show-databases = SHOW DATABASES               - 列出所有模式/数据库
help-show-columns = SHOW COLUMNS FROM <表>       - 显示表的列
help-show-index = SHOW INDEX FROM <表>         - 显示表的索引
help-show-create = SHOW CREATE TABLE <表>       - 显示 CREATE TABLE 语句
help-describe-sql = DESCRIBE <表>                - SHOW COLUMNS 的别名

help-examples-title = 示例：
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
# 状态消息
# =============================================================================

format-changed = 输出格式已设置为：{ $format }
database-saved = 数据库已保存到：{ $path }
no-database-file = 错误：未指定数据库文件。请使用 \save <文件名> 或使用 --database 参数启动

# =============================================================================
# 错误显示
# =============================================================================

no-errors = 本次会话无错误。
recent-errors = 最近的错误：

# =============================================================================
# 脚本执行消息
# =============================================================================

script-no-statements = 脚本中未找到 SQL 语句
script-executing = 正在执行第 { $current } 条语句，共 { $total } 条...
script-error = 执行第 { $index } 条语句时出错：{ $error }
script-summary-title = === 脚本执行摘要 ===
script-total = 总语句数：{ $count }
script-successful = 成功：{ $count }
script-failed = 失败：{ $count }
script-failed-error = { $count } 条语句执行失败

# =============================================================================
# 输出格式
# =============================================================================

rows-with-time = { $count } 行（{ $time } 秒）
rows-count = { $count } 行

# =============================================================================
# 警告
# =============================================================================

warning-config-load = 警告：无法加载配置文件：{ $error }
warning-auto-save-failed = 警告：自动保存数据库失败：{ $error }
warning-save-on-exit-failed = 警告：退出时保存数据库失败：{ $error }

# =============================================================================
# 文件操作
# =============================================================================

file-read-error = 读取文件 '{ $path }' 失败：{ $error }
stdin-read-error = 从标准输入读取失败：{ $error }
database-load-error = 加载数据库失败：{ $error }
