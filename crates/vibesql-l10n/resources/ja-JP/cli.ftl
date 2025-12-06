# VibeSQL CLI ローカライゼーション - 日本語
# This file contains all user-facing strings for the VibeSQL command-line interface.

# =============================================================================
# REPL Banner and Basic Messages
# =============================================================================

cli-banner = VibeSQL v{ $version } - SQL:1999 完全準拠データベース
cli-help-hint = \help でヘルプを表示、\quit で終了
cli-goodbye = さようなら！

# =============================================================================
# Command Help Text (Clap Arguments)
# =============================================================================

cli-about = VibeSQL - SQL:1999 完全準拠データベース

cli-long-about = VibeSQL コマンドラインインターフェース

    使用方法:
      対話型 REPL:         vibesql (--database <FILE>)
      コマンド実行:        vibesql -c "SELECT * FROM users"
      ファイル実行:        vibesql -f script.sql
      標準入力から実行:    cat data.sql | vibesql
      型生成:              vibesql codegen --schema schema.sql --output types.ts

    対話型 REPL:
      -c、-f、またはパイプ入力なしで起動すると、VibeSQL は対話型 REPL を開始します。
      readline サポート、コマンド履歴、およびメタコマンドが利用可能です:
        \d (table)  - テーブルの説明またはテーブル一覧
        \dt         - テーブル一覧
        \f <format> - 出力形式を設定
        \copy       - CSV/JSON のインポート/エクスポート
        \help       - すべての REPL コマンドを表示

    サブコマンド:
      codegen           データベーススキーマから TypeScript 型を生成

    設定:
      ~/.vibesqlrc (TOML 形式) で設定できます。
      セクション: display, database, history, query

    例:
      # インメモリデータベースで対話型 REPL を開始
      vibesql

      # 永続的なデータベースファイルを使用
      vibesql --database mydata.db

      # 単一コマンドを実行
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # SQL スクリプトファイルを実行
      vibesql -f schema.sql -v

      # CSV からデータをインポート
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # クエリ結果を JSON としてエクスポート
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # スキーマファイルから TypeScript 型を生成
      vibesql codegen --schema schema.sql --output src/types.ts

      # 実行中のデータベースから TypeScript 型を生成
      vibesql codegen --database mydata.db --output src/types.ts

# Argument help strings
arg-database-help = データベースファイルのパス (未指定の場合、インメモリデータベースを使用)
arg-file-help = ファイルから SQL コマンドを実行
arg-command-help = SQL コマンドを直接実行して終了
arg-stdin-help = 標準入力から SQL コマンドを読み取り (パイプ時に自動検出)
arg-verbose-help = ファイル/標準入力実行時に詳細出力を表示
arg-format-help = クエリ結果の出力形式
arg-lang-help = 表示言語を設定 (例: en-US, es, ja)

# =============================================================================
# Codegen Subcommand
# =============================================================================

codegen-about = データベーススキーマから TypeScript 型を生成

codegen-long-about = VibeSQL データベーススキーマから TypeScript 型定義を生成します。

    このコマンドは、データベース内のすべてのテーブルに対して TypeScript インターフェースを作成し、
    ランタイム型チェックと IDE サポートのためのメタデータオブジェクトも生成します。

    入力ソース:
      --database <FILE>  既存のデータベースファイルから生成
      --schema <FILE>    SQL スキーマファイルから生成 (CREATE TABLE 文)

    出力:
      --output <FILE>    生成された型をこのファイルに書き込み (デフォルト: types.ts)

    オプション:
      --camel-case       カラム名を camelCase に変換
      --no-metadata      テーブルメタデータオブジェクトの生成をスキップ

    例:
      # データベースファイルから
      vibesql codegen --database mydata.db --output src/db/types.ts

      # SQL スキーマファイルから
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # camelCase プロパティ名で
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = CREATE TABLE 文を含む SQL スキーマファイル
codegen-output-help = 生成された TypeScript の出力ファイルパス
codegen-camel-case-help = カラム名を camelCase に変換
codegen-no-metadata-help = テーブルメタデータオブジェクトの生成をスキップ

codegen-from-schema = スキーマファイルから TypeScript 型を生成中: { $path }
codegen-from-database = データベースから TypeScript 型を生成中: { $path }
codegen-written = TypeScript 型を書き込みました: { $path }
codegen-error-no-source = --database または --schema を指定する必要があります。
    使用方法は 'vibesql codegen --help' を参照してください。

# =============================================================================
# Meta-commands Help (\help output)
# =============================================================================

help-title = メタコマンド:
help-describe = \d (table)      - テーブルの説明またはテーブル一覧
help-tables = \dt             - テーブル一覧
help-schemas = \ds             - スキーマ一覧
help-indexes = \di             - インデックス一覧
help-roles = \du             - ロール/ユーザー一覧
help-format = \f <format>     - 出力形式を設定 (table, json, csv, markdown, html)
help-timing = \timing         - クエリ時間表示の切り替え
help-copy-to = \copy <table> TO <file>   - テーブルを CSV/JSON ファイルにエクスポート
help-copy-from = \copy <table> FROM <file> - CSV ファイルをテーブルにインポート
help-save = \save (file)    - データベースを SQL ダンプファイルに保存
help-errors = \errors         - 最近のエラー履歴を表示
help-help = \h, \help      - このヘルプを表示
help-quit = \q, \quit      - 終了

help-sql-title = SQL イントロスペクション:
help-show-tables = SHOW TABLES                  - すべてのテーブルを表示
help-show-databases = SHOW DATABASES               - すべてのスキーマ/データベースを表示
help-show-columns = SHOW COLUMNS FROM <table>    - テーブルのカラムを表示
help-show-index = SHOW INDEX FROM <table>      - テーブルのインデックスを表示
help-show-create = SHOW CREATE TABLE <table>    - CREATE TABLE 文を表示
help-describe-sql = DESCRIBE <table>             - SHOW COLUMNS のエイリアス

help-examples-title = 例:
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

format-changed = 出力形式を設定しました: { $format }
database-saved = データベースを保存しました: { $path }
no-database-file = エラー: データベースファイルが指定されていません。\save <filename> を使用するか、--database フラグで起動してください

# =============================================================================
# Error Display
# =============================================================================

no-errors = このセッションでエラーはありません。
recent-errors = 最近のエラー:

# =============================================================================
# Script Execution Messages
# =============================================================================

script-no-statements = スクリプトに SQL 文が見つかりません
script-executing = 文を実行中 { $current } / { $total }...
script-error = 文 { $index } の実行エラー: { $error }
script-summary-title = === スクリプト実行サマリー ===
script-total = 総文数: { $count }
script-successful = 成功: { $count }
script-failed = 失敗: { $count }
script-failed-error = { $count } 件の文が失敗しました

# =============================================================================
# Output Formatting
# =============================================================================

rows-with-time = { $count } 行 ({ $time }秒)
rows-count = { $count } 行

# =============================================================================
# Warnings
# =============================================================================

warning-config-load = 警告: 設定ファイルを読み込めませんでした: { $error }
warning-auto-save-failed = 警告: データベースの自動保存に失敗しました: { $error }
warning-save-on-exit-failed = 警告: 終了時のデータベース保存に失敗しました: { $error }

# =============================================================================
# File Operations
# =============================================================================

file-read-error = ファイル '{ $path }' の読み取りに失敗しました: { $error }
stdin-read-error = 標準入力からの読み取りに失敗しました: { $error }
database-load-error = データベースの読み込みに失敗しました: { $error }
