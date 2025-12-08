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

# =============================================================================
# ベンチマークページ
# =============================================================================

# セクションヘッダー
bench-section-embedded = 組み込み
bench-section-server = サーバー
bench-results-title = ベンチマーク結果
bench-perf-comparison = パフォーマンス比較
bench-methodology-title = 測定方法
bench-analysis-roadmap = 分析とロードマップ

# サマリーカード
bench-vs-sqlite = vs SQLite
bench-vs-duckdb = vs DuckDB
bench-vs-mysql = vs MySQL
bench-ops-tested = テスト済み操作
bench-last-updated = 最終更新
bench-avg-speedup = 平均高速化率
bench-from-main = mainブランチから
bench-loading = 読み込み中...
bench-na = N/A
bench-faster = { $value }倍高速
bench-slower = { $value }倍低速
bench-speedup = { $value }倍
bench-startup-time-label = 起動時間
bench-download-size = ダウンロードサイズ
bench-uncompressed = 非圧縮
bench-size-metrics = サイズ指標
bench-failed = 失敗
bench-failed-title = クエリ失敗（タイムアウトまたはエラー）
bench-no-wasm-data = WASMデータがありません

# テーブルヘッダー
bench-table-operation = 操作
bench-table-query = クエリ
bench-table-vibesql = VibeSQL
bench-table-vibesql-server = VibeSQL Server
bench-table-sqlite = SQLite
bench-table-duckdb = DuckDB
bench-table-mysql = MySQL
bench-table-loading = ベンチマーク結果を読み込み中...
bench-vibesql-server-title = PostgreSQLワイヤープロトコル経由のVibeSQL

# 共通ベンチマーク用語
bench-hardware = ハードウェア
bench-benchmark-framework = ベンチマークフレームワーク
bench-scale-factor = スケールファクター
bench-data = データ
bench-databases-tested = テスト対象データベース
bench-execution-mode = 実行モード
bench-measurement = 測定
bench-workload = ワークロード
bench-transaction-mix = トランザクション構成
bench-warehouses = ウェアハウス数
bench-concurrency = 同時実行性
bench-acid-compliance = ACID準拠
bench-mode = モード
bench-workload-types = ワークロードタイプ
bench-table-size = テーブルサイズ
bench-index-types = インデックスタイプ
bench-operations = 操作
bench-databases = データベース
bench-protocol-overhead = プロトコルオーバーヘッド
bench-binary-size = バイナリサイズ
bench-startup-time = 起動時間
bench-peak-memory = ピークメモリ
bench-schema = スキーマ
bench-query-count = クエリ数
bench-query-types = クエリタイプ
bench-sql-features = SQL機能
bench-wasm-size = WASMサイズ
bench-wasm-gzip = WASM (gzip)
bench-wasm-brotli = WASM (brotli)

# TPC-H固有
bench-tpch-name = TPC-H
bench-tpch-title = TPC-H 意思決定支援ベンチマーク
bench-tpch-description = これらのベンチマークは業界標準の<strong>TPC-Hベンチマークスイート</strong>を使用しています。集約、結合、サブクエリ、ソートを含む複雑な分析クエリで実際の意思決定支援ワークロードをシミュレートします。
bench-tpch-ops-label = TPC-Hクエリ
bench-tpch-note-intro = すべてのベンチマークは、解析、計画、実行、結果の具体化を含むエンドツーエンドのクエリ実行時間を測定します。これは分析ワークロードにおける<strong>実際のSQLエンジンパフォーマンス</strong>を表しています。
bench-tpch-note-queries = <strong>注:</strong> TPC-Hクエリは、SQLパフォーマンスの異なる側面をテストします：単純な集約（Q1、Q6）、複雑な結合（Q2-Q5、Q7-Q10）、サブクエリ（Q11-Q15）、高度な分析（Q16-Q22）。上の表のクエリ名にマウスを合わせると説明が表示されます。

# TPC-H ディスカッション
bench-tpch-disc-excels-title = VibeSQLが優れている点
bench-tpch-disc-excels = VibeSQLは<strong>スキャン集約型クエリ</strong>（Q1、Q6、Q14、Q15、Q20）で強力なパフォーマンスを発揮します。これらのクエリでは、列指向実行エンジンとSIMD加速集約が力を発揮します。大規模テーブルのフィルタリングと複雑な結合パターンなしでの集約計算が含まれます。
bench-tpch-disc-targets-title = 現在の最適化対象
bench-tpch-disc-targets = 複数テーブル結合クエリ（Q3、Q5、Q7-Q10、Q18、Q19、Q21）は現在SQLiteがリードしています。主なボトルネックはハッシュ結合実装で、SQLiteの数十年にわたって改良されたB-tree結合と同レベルの最適化がまだ適用されていません。現在積極的に開発中の領域：
bench-tpch-disc-join-ordering = より良い結合順序選択のためのカーディナリティ推定の改善
bench-tpch-disc-hash-sizing = 大規模結合のための適応型ハッシュテーブル拡張とディスクスピル
bench-tpch-disc-vectorized = キャッシュ利用率向上のための結合内部ループでのバッチ処理
bench-tpch-disc-inl-joins = 有益な場合のB-treeインデックス活用
bench-tpch-disc-path-title = リーダーシップへの道
bench-tpch-disc-path = VibeSQLのアーキテクチャは、列指向ストレージ、ベクトル化実行、ロックフリー並行性などの機能を備え、最新のハードウェア向けに設計されています。これらの最適化が成熟するにつれて、VibeSQLはすべてのTPC-Hクエリで一貫したリーダーシップを達成すると期待しています。基本設計は、従来の行ストアデータベースでは簡単に後付けできない並列処理とSIMDをサポートしています。

# TPC-H クエリ説明
bench-tpch-q1 = 価格要約レポート - GROUP BYとORDER BYによる価格集約
bench-tpch-q2 = 最小コスト仕入先 - ORDER BYとLIMITを含む3テーブルJOIN
bench-tpch-q3 = 出荷優先度 - 集約を含む3テーブルJOIN
bench-tpch-q4 = 注文優先度チェック - 相関EXISTSサブクエリ
bench-tpch-q5 = 地域仕入先売上 - 複雑なフィルタリングを含む6テーブルJOIN
bench-tpch-q6 = 収益変化予測 - BETWEENとSUMを含むWHEREフィルター
bench-tpch-q7 = 出荷量 - SUBSTRと日付フィルタリングを含む6テーブルJOIN
bench-tpch-q8 = 国内市場シェア - CASE式を含む7テーブルJOIN
bench-tpch-q9 = 製品タイプ利益測定 - 集約を含む4テーブルJOIN
bench-tpch-q10 = 返品レポート - TOP-N LIMITを含む4テーブルJOIN
bench-tpch-q11 = 重要在庫識別 - HAVING句内のサブクエリ
bench-tpch-q12 = 出荷モード優先度 - 日付ロジックを含むCASE集約
bench-tpch-q13 = 顧客分布 - サブクエリを含むLEFT OUTER JOIN
bench-tpch-q14 = プロモーション効果 - CASEによる条件付き集約
bench-tpch-q15 = トップ仕入先 - MAXを含むネストされたサブクエリ
bench-tpch-q16 = 部品/仕入先関係 - DISTINCTを含むNOT INサブクエリ
bench-tpch-q17 = 少量注文収益 - WHERE内の相関サブクエリ
bench-tpch-q18 = 大口顧客 - HAVINGを含むGROUP BY
bench-tpch-q19 = 割引収益 - 複雑なOR条件
bench-tpch-q20 = 潜在的部品プロモーション - GROUP BY/HAVINGを含むINサブクエリ
bench-tpch-q21 = 注文を遅延させた仕入先 - 複数テーブルEXISTS
bench-tpch-q22 = グローバル販売機会 - NOT EXISTSサブクエリを含むSUBSTR

# TPC-DS固有
bench-tpcds-name = TPC-DS
bench-tpcds-title = TPC-DS 意思決定支援ベンチマーク
bench-tpcds-description = <strong>TPC-DS</strong>はTPC-Hの後継で、複数のファクトテーブル、スノーフレークスキーマ、高度なSQL機能を含む、より複雑なクエリパターンで現代の意思決定支援システムをモデル化した99のクエリを特徴としています。
bench-tpcds-ops-label = TPC-DSクエリ
bench-tpcds-note-intro = TPC-DSクエリはTPC-Hよりも大幅に複雑で、ウィンドウ関数、共通テーブル式（WITH句）、複数のファクトテーブルとディメンションテーブルにまたがる複雑な結合パターンなど、高度なSQL機能をテストします。
bench-tpcds-note-remaining = <strong>注:</strong> 残りのサポートされていないクエリには、まだ実装されていないINTERSECT/EXCEPTや特定の日付算術関数などの機能が必要です。

# TPC-DS ディスカッション
bench-tpcds-disc-coverage-title = SQL:1999機能カバレッジ
bench-tpcds-disc-coverage = TPC-DSは最も要求の厳しいSQL機能を検証します。VibeSQLは<strong>99クエリ中88</strong>をパスし、ROLLUP、CUBE、GROUPING()、複雑なフレーミングを持つウィンドウ関数、再帰CTEを含むSQL:1999の幅広いカバレッジを実証しています。残りのクエリにはINTERSECT/EXCEPT集合演算が必要です。
bench-tpcds-disc-optimization-title = 複雑なクエリ最適化
bench-tpcds-disc-optimization = TPC-DSクエリは、相関サブクエリを含む10以上のテーブルを結合することがよくあります。現在の重点領域：
bench-tpcds-disc-cte = マテリアライズCTEとインラインCTEの賢明な選択
bench-tpcds-disc-decorrelation = 有益な場合に相関サブクエリを結合に変換
bench-tpcds-disc-star = 分析パターンのためのファクト-ディメンション結合順序
bench-tpcds-disc-toward-title = 99/99に向けて
bench-tpcds-disc-toward = INTERSECTとEXCEPTは、残りのクエリを有効にする計画された追加機能です。これらの集合演算は既存のクエリ代数に自然に適合し、DISTINCT処理と同様のハッシュベースの演算子として実装されます。

# TPC-C固有
bench-tpcc-name = TPC-C
bench-tpcc-title = TPC-C オンライントランザクション処理ベンチマーク
bench-tpcc-description = <strong>TPC-Cベンチマーク</strong>は、注文入力、支払い処理、注文状況クエリ、配送処理、在庫レベル監視を含む複雑なトランザクションの組み合わせで、完全な注文入力環境をシミュレートします。
bench-tpcc-ops-label = TPC-Cトランザクション
bench-tpcc-transactions-label = 実行されたトランザクション
bench-tpcc-note-intro = TPC-Cは1分あたりのトランザクション数（tpmC）を測定し、複雑なビジネスロジックを持つ並行トランザクションを処理するデータベースの能力をテストします。このベンチマークは<strong>トランザクションワークロードパフォーマンス</strong>の評価に重要です。
bench-tpcc-note-results = <strong>注:</strong> 結果は平均トランザクションレイテンシを示しています。低いほど良いです。TPC-Cは、厳格な一貫性要件を持つ書き込み集中型ワークロードに対して特に要求が厳しいです。

# TPC-C トランザクション説明
bench-tpcc-new-order = 新規注文 - 在庫チェックと注文作成を含む複雑なトランザクション
bench-tpcc-payment = 支払い - 顧客残高とウェアハウス/地区合計の更新
bench-tpcc-order-status = 注文状況 - 顧客注文履歴の読み取り専用クエリ
bench-tpcc-delivery = 配送 - 保留中の注文のバッチ処理
bench-tpcc-stock-level = 在庫レベル - 最近の注文でしきい値を下回る品目のカウント

# TPC-C ディスカッション
bench-tpcc-disc-faster-title = SQLiteより42倍高速
bench-tpcc-disc-faster = VibeSQLは、SQLiteの約1,900 TPSと比較して<strong>約79,000トランザクション/秒</strong>を達成し、42倍の改善です。この劇的な高速化は、すべての書き込み操作でSQLiteの粗粒度ロックを回避するロックフリーMVCCアーキテクチャによるものです。
bench-tpcc-disc-dominates-title = VibeSQLがOLTPで優位な理由
bench-tpcc-disc-lockfree = MVCCにより、リーダーとライターがブロックなしで同時に進行可能
bench-tpcc-disc-optimistic = トランザクションは実行中ではなくコミット時にのみ競合
bench-tpcc-disc-btree = インメモリワークロード向けに最適化された専用インデックス構造
bench-tpcc-disc-prepared = クエリプランは一度コンパイルされ再利用
bench-tpcc-disc-scaling-title = さらなるスケーリング
bench-tpcc-disc-scaling = 現在の結果はシングルスレッドです。VibeSQLのアーキテクチャはマルチスレッドトランザクション処理をサポートしており、並列実行サポートを追加することでほぼ線形のスケーリングが期待されます。目標は最新のマルチコアハードウェアで500K以上のTPSを達成することです。
bench-tpcc-disc-duckdb-title = DuckDBがOLTPで遅れる理由
bench-tpcc-disc-duckdb = DuckDBはTPC-Cで約385 TPSしか達成できません（VibeSQLより60倍遅く、SQLiteより12倍遅い）。これは予想通りの結果です：DuckDBは大規模なバッチ操作向けに最適化された<strong>分析用（OLAP）データベース</strong>であり、単一行トランザクション向けではありません。カラムナーストレージ形式は数百万行のスキャンに優れていますが、TPC-Cのようなワークロードで支配的なポイント検索や小規模更新にはオーバーヘッドが発生します。

# Sysbench組み込み固有
bench-sysbench-embedded-name = Sysbench（組み込み）
bench-sysbench-embedded-title = Sysbenchマイクロベンチマーク（組み込み）
bench-sysbench-embedded-description = <strong>Sysbench</strong>は、特定のデータベース操作を分離するフォーカスされたマイクロベンチマークを提供します。これらのテストは、完全なトランザクションワークロードの複雑さなしに、基本的な操作の生のパフォーマンスを測定します。
bench-sysbench-embedded-ops-label = Sysbench操作
bench-sysbench-embedded-note = 組み込みモードは、ネットワークオーバーヘッドなしでプロセス内でデータベースを実行し、最小レイテンシが重要な単一プロセスアプリケーションに最適です。

# Sysbench操作説明
bench-sysbench-point-select = ポイント選択 - プライマリキーによる単一行検索
bench-sysbench-insert = 挿入 - テーブルへの新しい行の挿入
bench-sysbench-update-index = インデックス更新 - インデックス付き列の更新（k = k + 1）
bench-sysbench-update-non-index = 非インデックス更新 - 非インデックス列の更新
bench-sysbench-delete = 削除 - プライマリキーによる行の削除
bench-sysbench-range-queries = 範囲クエリ - シンプル、SUM、ORDER BY、DISTINCTの範囲スキャン

# Sysbench組み込みディスカッション
bench-sysbench-emb-disc-point-title = ポイント検索: VibeSQLがリード
bench-sysbench-emb-disc-point = VibeSQLの直接APIは<strong>ポイント選択あたり約137ns</strong>を達成し、SQLiteに匹敵し、DuckDB（約140µs）を大幅に上回ります。B-tree実装は、最小限のポインタチェイスとキャッシュフレンドリーなノードレイアウトで単一行検索に最適化されています。
bench-sysbench-emb-disc-index-title = インデックス更新: 2倍高速
bench-sysbench-emb-disc-index = VibeSQLのインデックス更新は<strong>約740ns vs SQLiteの約1.6µs</strong>で実行されます。MVCC設計により、各操作のwrite-aheadロギングオーバーヘッドなしでインプレースインデックス更新が可能です。
bench-sysbench-emb-disc-improve-title = 改善領域
bench-sysbench-emb-disc-bulk = SQLiteのバッチ挿入パスは高度に最適化されており、バッチB-tree操作を追加中
bench-sysbench-emb-disc-nonindex = 非インデックス列のフルテーブルスキャンにはプレディケートプッシュダウン最適化が必要
bench-sysbench-emb-disc-deletes = トゥームストーンベースの削除にはクリーンアップオーバーヘッドがあり、コンパクション改善を計画中
bench-sysbench-emb-disc-duckdb-title = DuckDB比較
bench-sysbench-emb-disc-duckdb = DuckDBは分析ワークロード向けに最適化されており、マイクロ操作向けではありません。ここでの100-1000倍遅い結果は、単一行レイテンシをバルクスループットと引き換えにするアーキテクチャの選択（列指向ストレージ、ベクトル化実行）を反映しています。VibeSQLは両方のユースケースを対象としています。
bench-sysbench-emb-disc-architecture-title = Architectural Trade-offs
bench-sysbench-emb-disc-architecture = VibeSQL's hybrid architecture targets both OLTP and OLAP workloads. Our B-tree storage provides SQLite-competitive point lookup performance, while columnar execution handles analytical queries efficiently. This differs from pure OLAP databases like DuckDB that optimize exclusively for bulk operations at the cost of single-row latency.

# Sysbenchサーバー固有
bench-sysbench-server-name = Sysbench（サーバー）
bench-sysbench-server-title = Sysbenchマイクロベンチマーク（サーバー）
bench-sysbench-server-description = <strong>Sysbench</strong>サーバーベンチマークは、VibeSQL Server（PostgreSQLワイヤープロトコル）とMySQLを比較し、マルチクライアントデータベースデプロイメントのパフォーマンスを測定します。
bench-sysbench-server-ops-label = Sysbench操作
bench-sysbench-server-note = サーバーモードはPostgreSQLワイヤープロトコルを使用し、マルチクライアントアクセスと既存のPostgreSQLツールやドライバーとの互換性を実現します。

# Sysbenchサーバーディスカッション
bench-sysbench-srv-disc-protocol-title = PostgreSQLワイヤープロトコル
bench-sysbench-srv-disc-protocol = VibeSQL ServerはPostgreSQLワイヤープロトコルを実装し、既存のPostgreSQLドライバーやツールとの互換性を実現します。これにより、組み込みモードと比較してクエリあたり約10-50µsのプロトコルオーバーヘッドが追加されますが、マルチクライアントデプロイメントが可能になります。
bench-sysbench-srv-disc-mysql-title = MySQL比較
bench-sysbench-srv-disc-mysql = サーバーベンチマークは、従来のクライアントサーバーデータベースの代替としてVibeSQLを評価するためにMySQLと比較します。結果は操作タイプによって異なり、VibeSQLは読み取り集中型ワークロードで優位性を示しています。
bench-sysbench-srv-disc-roadmap-title = サーバーロードマップ
bench-sysbench-srv-disc-pooling = 高スループットシナリオの接続確立オーバーヘッドを削減
bench-sysbench-srv-disc-caching = 接続間でのクエリプランのサーバーサイドキャッシング
bench-sysbench-srv-disc-extended = バッチ操作のための完全なPostgreSQL拡張クエリプロトコルサポート

# フットプリント組み込み固有
bench-footprint-embedded-name = フットプリント（組み込み）
bench-footprint-embedded-title = ネイティブバイナリフットプリント
bench-footprint-embedded-description = <strong>組み込みフットプリントベンチマーク</strong>は、バイナリサイズ、コールドスタートアップ時間、ピークメモリ使用量を比較して、ネイティブデータベースバイナリのリソース効率を測定します。
bench-footprint-embedded-ops-label = 比較データベース
bench-footprint-embedded-note = ネイティブバイナリフットプリントは、バイナリサイズ、起動レイテンシ、メモリ消費がデプロイメントの実現可能性に直接影響する<strong>組み込みおよびエッジデプロイメント</strong>にとって重要です。

# フットプリント組み込み説明
bench-footprint-binary-size = バイナリサイズ - ディスク上のコンパイル済みデータベースバイナリのサイズ
bench-footprint-startup-time = 起動時間 - コールドスタートして最初のクエリを実行するまでの時間
bench-footprint-peak-memory = ピークメモリ - 初期化中の最大常駐セットサイズ

# フットプリント組み込みディスカッション
bench-footprint-emb-disc-size-title = バイナリサイズ: 中間
bench-footprint-emb-disc-size = VibeSQLは<strong>約17MB</strong>で、SQLite（約5MB）とDuckDB（約45MB）の間に位置しています。これは、高度な機能（ウィンドウ関数、CTE、列指向実行）を含めつつ、組み込みデプロイメント向けにバイナリを管理可能に保つという選択を反映しています。
bench-footprint-emb-disc-startup-title = 起動: 最速のコールドスタート
bench-footprint-emb-disc-startup = VibeSQLは<strong>約7.7msのコールドスタート</strong>を達成し、SQLite（約8.2ms）よりわずかに速く、DuckDB（約14.6ms）より大幅に速いです。最小限の初期化パスは、起動時に必須のメタデータ構造のみをロードします。
bench-footprint-emb-disc-memory-title = メモリ効率
bench-footprint-emb-disc-memory = 起動時のピークメモリは、VibeSQLが約7MB、SQLiteが約3MB、DuckDBが約11MBです。SQLiteとの差は、事前に割り当てられるより洗練されたクエリオプティマイザと列指向実行インフラストラクチャを反映しています。
bench-footprint-emb-disc-roadmap-title = サイズ削減ロードマップ
bench-footprint-emb-disc-flags = 未使用機能を除外するためのコンパイル時機能選択
bench-footprint-emb-disc-lto = デッドコード除去のためのプログラム全体のリンク時最適化
bench-footprint-emb-disc-modular = コアエンジンとオプション機能（例：ウィンドウ関数）の分離

# フットプリントサーバー/WASM固有
bench-footprint-server-name = フットプリント（サーバー/WASM）
bench-footprint-server-title = WASMフットプリント
bench-footprint-server-description = <strong>WASMフットプリントベンチマーク</strong>は、ダウンロードサイズがユーザーエクスペリエンスに影響するWebアプリケーションにとって重要な、ブラウザデプロイメント用のWebAssemblyモジュールサイズを測定します。
bench-footprint-server-ops-label = デプロイメントターゲット
bench-footprint-server-note = WASMサイズは、ダウンロード時間がインタラクティブになるまでの時間に直接影響する<strong>Webデプロイメント</strong>にとって重要です。ブラウザはgzipコンテンツを自動的に解凍するため、gzipサイズが最も関連性があります。
bench-footprint-server-note2 = <strong>注:</strong> VibeSQL WASMは、ブラウザでの完全なSQL:1999準拠を維持しながら、最小ダウンロードサイズ向けに設計されています。

# フットプリントサーバー説明
bench-footprint-wasm-size = WASMサイズ - ブラウザデプロイメント用WebAssemblyモジュールのサイズ
bench-footprint-wasm-gzip = WASM (gzip) - Web配信用の圧縮サイズ

# フットプリントサーバーディスカッション
bench-footprint-srv-disc-wasm-title = WASM: 2.2MB圧縮
bench-footprint-srv-disc-wasm = VibeSQLのWebAssemblyモジュールは<strong>gzip圧縮で約2.2MB</strong>で、高速な初期ページロードを実現します。これは、ウィンドウ関数、CTE、ACIDトランザクションを備えた完全なSQL:1999データベースがブラウザ内で完全に動作します。
bench-footprint-srv-disc-included-title = 含まれるもの
bench-footprint-srv-disc-parser = 完全なSQLパーサーとクエリオプティマイザ
bench-footprint-srv-disc-btree = MVCCを備えたB-treeストレージエンジン
bench-footprint-srv-disc-window = ウィンドウ関数と高度な集約
bench-footprint-srv-disc-cte = 共通テーブル式（WITH句）
bench-footprint-srv-disc-acid = 完全なACIDトランザクションサポート
bench-footprint-srv-disc-benefits-title = ブラウザデプロイメントの利点
bench-footprint-srv-disc-benefits = ブラウザでSQLを実行することで、サーバーへのラウンドトリップレイテンシが排除され、オフラインファーストアプリケーションが可能になり、機密データがユーザーのデバイスに保持されます。VibeSQLのWASMビルドは、最小限の依存関係と効率的なメモリ使用でこのユースケース向けに設計されています。
bench-footprint-srv-disc-roadmap-title = WASMロードマップ
bench-footprint-srv-disc-streaming = モジュールのダウンロード中に実行を開始
bench-footprint-srv-disc-indexeddb = ブラウザセッション間の永続ストレージ
bench-footprint-srv-disc-worker = レスポンシブUIのためのメインスレッド外でのクエリ実行

# 箇条書きラベル（説明と共に使用）
bench-bullet-join-ordering = 結合順序
bench-bullet-hash-sizing = ハッシュテーブルサイジング
bench-bullet-vectorized = ベクトル化結合
bench-bullet-inl-joins = インデックスネステッドループ結合
bench-bullet-cte-materialization = CTEマテリアライゼーション
bench-bullet-decorrelation = サブクエリ非相関化
bench-bullet-star-optimization = スタースキーマ最適化
bench-bullet-lock-free = ロックフリー読み取り
bench-bullet-optimistic = 楽観的同時実行制御
bench-bullet-btree = インメモリB-tree
bench-bullet-prepared = プリペアドステートメントキャッシング
bench-bullet-bulk-inserts = バルク挿入
bench-bullet-non-indexed = 非インデックス更新
bench-bullet-deletes = 削除
bench-bullet-connection-pooling = コネクションプーリング
bench-bullet-stmt-caching = プリペアドステートメントキャッシング
bench-bullet-extended-protocol = 拡張クエリプロトコル
bench-bullet-feature-flags = 機能フラグ
bench-bullet-lto = LTO最適化
bench-bullet-modular = モジュラービルド
bench-bullet-streaming = ストリーミングコンパイル
bench-bullet-indexeddb = IndexedDB永続化
bench-bullet-worker = ワーカースレッドサポート

# =============================================================================
# 準拠ページ
# =============================================================================

# 概要セクション
conformance-sql-conformance = SQL準拠
conformance-testing-against = 業界標準のSQLテストスイートであるSQLLogicTestに対するテスト
conformance-full-pass-rate = 100%ファイルパス率達成！
conformance-tests-passing = テストパス
conformance-files-passing = ファイルパス
conformance-loading = 準拠レポートを読み込み中...
conformance-error-loading = レポート読み込みエラー
conformance-no-data = 準拠データがありません

# カテゴリ内訳
conformance-category-title = カテゴリ別テストカバレッジ
conformance-category-header = カテゴリ
conformance-pass-rate-header = パス率
conformance-progress-header = 進捗
conformance-tests-header = テスト
conformance-cat-select = SELECTクエリ
conformance-cat-aggregates = 集約
conformance-cat-joins = JOIN
conformance-cat-expressions = 式
conformance-cat-subqueries = サブクエリ
conformance-cat-index = インデックス操作
conformance-cat-ddl = DDLステートメント
conformance-cat-evidence = エビデンステスト
conformance-cat-random = ランダムテスト
conformance-cat-other = その他のテスト

# タイムライン
conformance-timeline-title = パス率履歴
conformance-timeline-desc = 過去90日間の準拠進捗
conformance-timeline-loading = チャートデータを読み込み中...

# マイルストーン
conformance-milestones-title = マイルストーン

# ローカルでのテスト実行
conformance-running-locally-title = ローカルでのテスト実行
conformance-run-sqltest = # SQL:1999準拠テストを実行
conformance-run-sqllogictest = # SQLLogicTestスイートを実行（数時間かかります）
conformance-generate-coverage = # カバレッジレポートを生成
conformance-open-coverage = # カバレッジレポートを開く

# sqltest セクション
conformance-sqltest-title = sqltest 結果
conformance-sqltest-desc = <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">sqltest</a>の結果 - SQL:1999標準から派生したBNF駆動の適合テストスイートで、739のテストがコア機能と基盤機能をカバーしています。
conformance-overall-pass-rate = 全体合格率
conformance-tests-of-passing = { $passed } / { $total } テスト合格
conformance-passed = 合格
conformance-failed = 失敗
conformance-errors = エラー
conformance-test-coverage = テストカバレッジ
conformance-core-features = コア機能（Eシリーズ）
conformance-additional-features = 追加機能

# 機能コード
conformance-e011 = 数値データ型
conformance-e021 = 文字列型
conformance-e031 = 識別子
conformance-e051 = 基本クエリ指定
conformance-e061 = 基本述語と検索条件
conformance-e071 = 基本クエリ式
conformance-e081 = 基本権限
conformance-e091 = 集合関数
conformance-e101 = 基本データ操作
conformance-e111 = 単一行SELECT文
conformance-e121 = 基本カーソルサポート
conformance-e131 = NULL値サポート
conformance-e141 = 基本整合性制約
conformance-e151 = トランザクションサポート
conformance-e161 = SQLコメント
conformance-f031 = 基本スキーマ操作

# SQLLogicTest セクション
conformance-slt-title = SQLLogicTest 結果
conformance-slt-desc = 公式SQLiteコーパスから623のテストファイルにわたる約590万のテストを含む包括的な<a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline">SQLLogicTest</a>スイートの結果。
conformance-files-of-passing = { $passed } / { $total } テストファイル合格
conformance-test-categories = テストカテゴリ
conformance-slt-select = SELECTテスト
conformance-slt-evidence = 証拠テスト
conformance-slt-index = インデックステスト
conformance-slt-random = ランダムテスト
conformance-slt-ddl = DDLテスト
conformance-slt-other = その他のテスト
conformance-slt-note = <strong>注:</strong> SQLLogicTestはsqltestとは異なる視点を提供します。sqltestがSQL:1999仕様のBNF文法適合性に焦点を当てているのに対し、SQLLogicTestは幅広いシナリオで実際の正確性をテストする数百万の実世界SQLクエリを含んでいます。

# 説明セクション
conformance-explanation-title = テストスイートの理解
conformance-what-is-sqltest = sqltestとは？
conformance-sqltest-explanation = <a href="https://github.com/elliotchance/sqltest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">sqltest</a>は、Elliot Chanceによるコミュニティ維持のテストスイートで、SQL:1999標準から派生したBNF駆動の適合テストを提供します。EシリーズとFシリーズのテストカテゴリにわたるコア機能と基盤機能をカバーする739のテストを含んでいます。このスイートは、私たちの実装がSQL:1999文法仕様に準拠しているかどうかをテストします。
conformance-what-is-slt = SQLLogicTestとは？
conformance-slt-explanation = <a href="https://github.com/dolthub/sqllogictest" target="_blank" class="text-blue-600 dark:text-blue-400 hover:underline font-medium">SQLLogicTest</a>は、もともとSQLite用に開発された包括的なテストスイートで、623のテストファイルにわたる約590万のSQLテストケースを含んでいます。実世界のクエリを実行して結果を検証することで実際の正確性をテストします。このスイートは純粋な文法適合性よりも意味的な正確性とエッジケースに焦点を当てています。
conformance-how-complement = どのように補完し合うか？
conformance-sqltest-validates = <span class="font-medium">sqltest（BNF駆動）:</span> SQL:1999標準仕様への文法適合性を検証
conformance-slt-validates = <span class="font-medium">SQLLogicTest（結果駆動）:</span> 数百万の実クエリで意味的正確性を検証
conformance-coverage-point = <span class="font-medium">カバレッジ:</span> sqltestは739の標準機能テストをカバー、SQLLogicTestは実用的なシナリオをカバー
conformance-philosophy-point = <span class="font-medium">哲学:</span> sqltestは「これを解析できるか？」を問い、SQLLogicTestは「これは正しく動作するか？」を問う
conformance-what-is-core = SQL:1999 Coreとは？
conformance-core-explanation = SQL:1999 Coreは、SQL:1999（ISO/IEC 9075:1999）標準で定義された公式の必須機能セットです。Core準拠を主張するデータベースが実装しなければならない約169の必須機能で構成されています。公式のCore準拠は、コミュニティテストスイートではなく、NIST SQLテストスイートを通じて検証されます。
conformance-what-mean = 合格率の意味は？
conformance-pass-rates-mean = 私たちの<strong>{ $sqltestRate }% sqltest合格率</strong>（{ $sqltestPassed }/{ $sqltestTotal }テスト）は、強力なSQL:1999文法適合性を示しています。{ $sltInfo } これらの結果は包括的なSQL:1999準拠を示していますが、公式のCore認証を構成するものではありません。
conformance-slt-pass-info = 私たちの<strong>{ $sltRate }% SQLLogicTest合格率</strong>（{ $sltPassed }/{ $sltTotal }テストファイル）は、実世界のクエリを正しく処理できることを示しています。
conformance-bottom-line = <strong>結論:</strong> 私たちは、標準適合性（sqltest）と実際の正確性（SQLLogicTest）の両方を確保するために、2つの補完的なテストスイートを使用しています。両方での高い合格率は、真剣なSQL:1999実装品質を示していますが、正式なCore認証には公式NISTスイートに対するテストが必要です。

# 失敗テストセクション
conformance-failing-tests-title = 失敗テスト
conformance-failing-tests-desc = 以下のテストが現在失敗しています。詳細を展開するにはクリックしてください。
conformance-view-failing = 失敗テストの詳細を表示（{ $count }テスト）
conformance-error-label = エラー:

# メタデータ
conformance-generated = 生成日:
conformance-commit = コミット:
conformance-status = ステータス:
