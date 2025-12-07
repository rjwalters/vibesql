# VibeSQL Web UI - 日本語

# Page titles
page-title = VibeSQL - AI搭載 SQL:1999 データベース
demo-title = VibeSQL デモ
benchmarks-title = パフォーマンスベンチマーク - VibeSQL
benchmarks-heading = VibeSQL - パフォーマンスベンチマーク
conformance-title = 準拠レポート - VibeSQL
conformance-heading = 準拠レポート
conformance-subtitle = SQL:1999標準準拠テスト

# Navigation
nav-showcase = SQL:1999 ショーケース
nav-conformance = sqltestの結果を表示
nav-sqllogictest = SQLLogicTestの結果を表示

# Editor section
editor-title = SQLエディタ
editor-storage = ストレージ
editor-storage-init = 初期化中...
editor-execute = クエリを実行

# Results section
results-title = 結果
results-empty = クエリを実行すると結果が表示されます
results-loading = 読み込み中...
results-rows = { $count }行
results-rows-with-time = { $count }行 ({ $time }ms)
results-copy = クリップボードにコピー
results-export = CSVでエクスポート
results-limit-warning = { $total }行中、最初の{ $limit }行を表示しています。LIMIT句でクエリを絞り込んでください。

# Examples sidebar
examples-title = サンプル
examples-basic = 基本クエリ
examples-advanced = 高度なクエリ

# Database selector
db-select-label = データベース

# Footer
footer-tagline = VibeSQL - WebAssembly上のSQL:1999データベース
footer-deployed = デプロイ日: { $date }

# Theme
theme-toggle-dark = ダークモードに切り替え
theme-toggle-light = ライトモードに切り替え

# Locale
locale-select = 言語を選択

# Messages
msg-query-success = クエリが正常に実行されました
msg-rows-affected = { $count }行が影響を受けました

# Errors
error-generic = エラーが発生しました
error-query-failed = クエリが失敗しました

# Editor
editor-placeholder = SQLクエリを入力してください... (Ctrl+EnterまたはCmd+Enterで実行)

# Navigation links
nav-terminal = SQLターミナルデモ
nav-compliance = SQLテスト準拠レポート
nav-benchmarks = パフォーマンスベンチマーク
nav-github = GitHubリポジトリ
nav-home = ホーム

# Results
results-success-zero = クエリが正常に実行されました（0行）
results-null = NULL

# Help Modal
help-title = キーボードショートカットとヘルプ
help-close = 閉じる
help-editor-shortcuts = エディタショートカット
help-navigation = ナビゲーション
help-results-actions = 結果アクション
help-tips = ヒント
help-shortcut-execute = 現在のクエリを実行
help-shortcut-comment = 行コメントを切り替え
help-shortcut-indent = 選択範囲をインデント
help-shortcut-show-help = このヘルプダイアログを表示
help-shortcut-close-help = ヘルプダイアログを閉じる
help-action-copy = クリップボードにコピー
help-action-copy-desc = タブ区切り値として結果をコピー
help-action-export = CSVエクスポート
help-action-export-desc = CSVファイルとして結果をダウンロード
help-tip-limit = パフォーマンスのため結果は1,000行に制限されています。LIMIT句でクエリを絞り込んでください。
help-tip-time = 実行時間はクエリ結果と共に表示されます。
help-tip-syntax = エディタはSQL構文ハイライトと自動補完をサポートしています。
help-tip-theme = テーマボタンでライト/ダークモードを切り替えできます。
help-got-it = 了解!

# Showcase Navigation
showcase-title = SQL:1999 Coreショーケース
showcase-description = 実装されたSQL:1999 Core機能をインタラクティブに探索
showcase-complete = { $percent }% 完了
showcase-categories = 機能カテゴリ
showcase-legend = ステータス凡例
showcase-status-implemented = 完全実装
showcase-status-partial = 部分実装
showcase-status-planned = 計画中

# Showcase category labels
showcase-cat-compliance = 準拠ダッシュボード
showcase-cat-data-types = データ型
showcase-cat-dml = DML操作
showcase-cat-predicates = 述語と演算子
showcase-cat-joins = JOIN
showcase-cat-subqueries = サブクエリ
showcase-cat-aggregates = 集約とGROUP BY
showcase-cat-ddl = DDLと制約

# Common showcase elements
showcase-interactive-examples = インタラクティブ例
showcase-try-example = この例を試す
showcase-progress = { $total }{ $type }中{ $implemented }件 ({ $percent }%)
showcase-table-status = ステータス
showcase-table-category = カテゴリ
showcase-table-description = 説明
showcase-table-syntax = 構文
showcase-table-use-case = ユースケース

# Status labels
status-implemented = 実装済み
status-partial = 部分的
status-planned = 計画中

# Aggregates Showcase
aggregates-title = SQL集約とGROUP BY
aggregates-description = SQL:1999 Core集約関数とグループ化機能
aggregates-reference = 集約関数リファレンス
aggregates-table-function = 関数
aggregates-progress-type = 関数
aggregates-ex-basic = 基本的な集約関数
aggregates-ex-group-single = GROUP BY（単一列）
aggregates-ex-group-multiple = GROUP BY（複数列）
aggregates-ex-having = HAVING句
aggregates-ex-orderby = 集約でのORDER BY
aggregates-ex-null = 集約でのNULL処理

# DML Operations Showcase
dml-title = DML操作（データ操作言語）
dml-description = データのクエリと変更のためのSQL:1999 Core操作
dml-reference = DML操作リファレンス
dml-table-operation = 操作
dml-progress-type = 操作
dml-ex-select-basic = SELECT - 基本クエリ
dml-ex-select-ordering = SELECT - 並べ替えと制限
dml-ex-insert = INSERT操作
dml-ex-update = UPDATE操作
dml-ex-delete = DELETE操作
dml-ex-combined = 統合CRUDワークフロー

# Data Types Showcase
datatypes-title = SQL:1999 Coreデータ型
datatypes-description = SQL:1999 Core仕様で定義された基本データ型を探索
datatypes-reference = データ型リファレンス
datatypes-table-type = 型名
datatypes-table-example = 例の値
datatypes-table-spec = 仕様
datatypes-progress-type = 型
datatypes-ex-numeric = 数値型の操作
datatypes-ex-null = NULL処理と3値論理
datatypes-ex-comparisons = 型の比較と操作

# JOINs Showcase
joins-title = SQL JOIN
joins-description = 複数テーブルからデータを結合するSQL:1999 Core JOIN操作
joins-reference = JOIN型リファレンス
joins-table-type = JOIN型
joins-progress-type = JOIN型
joins-category-suffix = JOIN
joins-ex-sample = サンプルデータセットアップ
joins-ex-inner = INNER JOIN
joins-ex-left = LEFT OUTER JOIN
joins-ex-right = RIGHT OUTER JOIN
joins-ex-full = FULL OUTER JOIN
joins-ex-cross = CROSS JOIN
joins-ex-multi = 複数テーブルJOIN

# Predicates Showcase
predicates-title = 述語と演算子
predicates-description = フィルタリングと論理操作のためのSQL:1999述語
predicates-reference = 述語リファレンス
predicates-table-predicate = 述語
predicates-progress-type = 述語
predicates-ex-comparison = 比較演算子
predicates-ex-between = BETWEENと範囲述語
predicates-ex-null = NULL述語と3値論理
predicates-ex-boolean = ブール論理（AND, OR, NOT）
predicates-ex-in = サブクエリでのIN述語
predicates-ex-combined = 組み合わせ述語操作

# Subqueries Showcase
subqueries-title = SQLサブクエリ
subqueries-description = ネストされたクエリ操作のためのSQL:1999 Coreサブクエリ機能
subqueries-reference = サブクエリ型リファレンス
subqueries-table-type = サブクエリ型
subqueries-progress-type = サブクエリ型
subqueries-ex-scalar-select = SELECTでのスカラーサブクエリ
subqueries-ex-scalar-where = WHEREでのスカラーサブクエリ
subqueries-ex-derived = 派生テーブル（FROMでのサブクエリ）
subqueries-ex-in = サブクエリでのIN述語
subqueries-ex-correlated = 相関サブクエリ
subqueries-ex-nested = ネストされたサブクエリ
