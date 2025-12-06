# VibeSQL CLI ローカライゼーション - 日本語
# このファイルには、VibeSQL コマンドラインインターフェースのすべてのユーザー向け文字列が含まれています。

# =============================================================================
# REPL バナーと基本メッセージ
# =============================================================================

cli-banner = VibeSQL v{ $version } - SQL:1999 FULL準拠データベース
cli-help-hint = \helpでヘルプを表示、\quitで終了
cli-goodbye = さようなら！
locale-changed = 言語を{ $locale }に変更しました

# =============================================================================
# コマンドヘルプテキスト（Clap引数）
# =============================================================================

cli-about = VibeSQL - SQL:1999 FULL準拠データベース

cli-long-about = VibeSQL コマンドラインインターフェース

    使用モード:
      対話型REPL:      vibesql (--database <ファイル>)
      コマンド実行:    vibesql -c "SELECT * FROM users"
      ファイル実行:    vibesql -f script.sql
      標準入力から実行: cat data.sql | vibesql
      型生成:          vibesql codegen --schema schema.sql --output types.ts

    対話型REPL:
      -c、-f、またはパイプ入力なしで起動すると、VibeSQLは以下の機能を持つ
      対話型REPLに入ります：
        \d (テーブル)  - テーブルの詳細表示またはすべてのテーブル一覧
        \dt            - テーブル一覧
        \f <形式>      - 出力形式の設定
        \copy          - CSV/JSONのインポート/エクスポート
        \help          - すべてのREPLコマンドを表示

    サブコマンド:
      codegen          データベーススキーマからTypeScript型を生成

    設定:
      設定は ~/.vibesqlrc（TOML形式）で構成できます。
      セクション: display、database、history、query

    使用例:
      # インメモリデータベースで対話型REPLを開始
      vibesql

      # 永続データベースファイルを使用
      vibesql --database mydata.db

      # 単一コマンドを実行
      vibesql -c "CREATE TABLE users (id INT, name VARCHAR(100))"

      # SQLスクリプトファイルを実行
      vibesql -f schema.sql -v

      # CSVからデータをインポート
      echo "\copy users FROM 'data.csv'" | vibesql --database mydata.db

      # クエリ結果をJSONでエクスポート
      vibesql -d mydata.db -c "SELECT * FROM users" --format json

      # スキーマファイルからTypeScript型を生成
      vibesql codegen --schema schema.sql --output src/types.ts

      # 実行中のデータベースからTypeScript型を生成
      vibesql codegen --database mydata.db --output src/types.ts

# 引数ヘルプ文字列
arg-database-help = データベースファイルパス（指定しない場合はインメモリデータベースを使用）
arg-file-help = ファイルからSQLコマンドを実行
arg-command-help = SQLコマンドを直接実行して終了
arg-stdin-help = 標準入力からSQLコマンドを読み込む（パイプ時に自動検出）
arg-verbose-help = ファイル/標準入力実行時に詳細出力を表示
arg-format-help = クエリ結果の出力形式
arg-lang-help = 表示言語を設定（例: en-US、es、ja）

# =============================================================================
# Codegenサブコマンド
# =============================================================================

codegen-about = データベーススキーマからTypeScript型を生成

codegen-long-about = VibeSQLデータベーススキーマからTypeScript型定義を生成します。

    このコマンドは、データベース内のすべてのテーブルに対するTypeScriptインターフェースと、
    ランタイム型チェックとIDEサポートのためのメタデータオブジェクトを作成します。

    入力ソース:
      --database <ファイル>  既存のデータベースファイルから生成
      --schema <ファイル>    SQLスキーマファイル（CREATE TABLE文）から生成

    出力:
      --output <ファイル>    生成された型をこのファイルに書き込む（デフォルト: types.ts）

    オプション:
      --camel-case           カラム名をキャメルケースに変換
      --no-metadata          テーブルメタデータオブジェクトの生成をスキップ

    使用例:
      # データベースファイルから
      vibesql codegen --database mydata.db --output src/db/types.ts

      # SQLスキーマファイルから
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # キャメルケースのプロパティ名で
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = CREATE TABLE文を含むSQLスキーマファイル
codegen-output-help = 生成されたTypeScriptの出力ファイルパス
codegen-camel-case-help = カラム名をキャメルケースに変換
codegen-no-metadata-help = テーブルメタデータオブジェクトの生成をスキップ

codegen-from-schema = スキーマファイルからTypeScript型を生成中: { $path }
codegen-from-database = データベースからTypeScript型を生成中: { $path }
codegen-written = TypeScript型を書き込みました: { $path }
codegen-error-no-source = --database または --schema を指定する必要があります。
    使用方法は 'vibesql codegen --help' を参照してください。

# =============================================================================
# メタコマンドヘルプ（\help出力）
# =============================================================================

help-title = メタコマンド:
help-describe = \d (テーブル)      - テーブルの詳細表示またはすべてのテーブル一覧
help-tables = \dt                - テーブル一覧
help-schemas = \ds                - スキーマ一覧
help-indexes = \di                - インデックス一覧
help-roles = \du                - ロール/ユーザー一覧
help-format = \f <形式>          - 出力形式を設定（table、json、csv、markdown、html）
help-timing = \timing            - クエリ時間計測の切り替え
help-copy-to = \copy <テーブル> TO <ファイル>   - テーブルをCSV/JSONファイルにエクスポート
help-copy-from = \copy <テーブル> FROM <ファイル> - CSVファイルをテーブルにインポート
help-save = \save (ファイル)    - データベースをSQLダンプファイルに保存
help-errors = \errors            - 最近のエラー履歴を表示
help-help = \h, \help          - このヘルプを表示
help-quit = \q, \quit          - 終了

help-sql-title = SQLイントロスペクション:
help-show-tables = SHOW TABLES                  - すべてのテーブルを一覧表示
help-show-databases = SHOW DATABASES               - すべてのスキーマ/データベースを一覧表示
help-show-columns = SHOW COLUMNS FROM <テーブル> - テーブルのカラムを表示
help-show-index = SHOW INDEX FROM <テーブル>   - テーブルのインデックスを表示
help-show-create = SHOW CREATE TABLE <テーブル> - CREATE TABLE文を表示
help-describe-sql = DESCRIBE <テーブル>          - SHOW COLUMNSのエイリアス

help-examples-title = 使用例:
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
# ステータスメッセージ
# =============================================================================

format-changed = 出力形式を設定しました: { $format }
database-saved = データベースを保存しました: { $path }
no-database-file = エラー: データベースファイルが指定されていません。\save <ファイル名> を使用するか、--database フラグで起動してください

# =============================================================================
# エラー表示
# =============================================================================

no-errors = このセッションにエラーはありません。
recent-errors = 最近のエラー:

# =============================================================================
# スクリプト実行メッセージ
# =============================================================================

script-no-statements = スクリプトにSQL文が見つかりません
script-executing = 文 { $current }/{ $total } を実行中...
script-error = 文 { $index } の実行エラー: { $error }
script-summary-title = === スクリプト実行サマリー ===
script-total = 合計文数: { $count }
script-successful = 成功: { $count }
script-failed = 失敗: { $count }
script-failed-error = { $count } 件の文が失敗しました

# =============================================================================
# 出力フォーマット
# =============================================================================

rows-with-time = { $count } 行（{ $time }秒）
rows-count = { $count } 行

# =============================================================================
# 警告
# =============================================================================

warning-config-load = 警告: 設定ファイルを読み込めませんでした: { $error }
warning-auto-save-failed = 警告: データベースの自動保存に失敗しました: { $error }
warning-save-on-exit-failed = 警告: 終了時のデータベース保存に失敗しました: { $error }

# =============================================================================
# ファイル操作
# =============================================================================

file-read-error = ファイル '{ $path }' の読み込みに失敗しました: { $error }
stdin-read-error = 標準入力からの読み込みに失敗しました: { $error }
database-load-error = データベースの読み込みに失敗しました: { $error }
