# VibeSQL エグゼキューターエラーメッセージ - 日本語
# このファイルには、vibesql-executor crateのすべてのエラーメッセージが含まれています。

# =============================================================================
# テーブルエラー
# =============================================================================

executor-table-not-found = テーブル '{ $name }' が見つかりません
executor-table-already-exists = テーブル '{ $name }' はすでに存在します

# =============================================================================
# カラムエラー
# =============================================================================

executor-column-not-found-simple = テーブル '{ $table_name }' にカラム '{ $column_name }' が見つかりません
executor-column-not-found-searched = カラム '{ $column_name }' が見つかりません（検索したテーブル: { $searched_tables }）
executor-column-not-found-with-available = カラム '{ $column_name }' が見つかりません（検索したテーブル: { $searched_tables }）。利用可能なカラム: { $available_columns }
executor-invalid-table-qualifier = カラム '{ $column }' に対する無効なテーブル修飾子 '{ $qualifier }'。利用可能なテーブル: { $available_tables }
executor-column-already-exists = カラム '{ $name }' はすでに存在します
executor-column-index-out-of-bounds = カラムインデックス { $index } が範囲外です

# =============================================================================
# インデックスエラー
# =============================================================================

executor-index-not-found = インデックス '{ $name }' が見つかりません
executor-index-already-exists = インデックス '{ $name }' はすでに存在します
executor-invalid-index-definition = 無効なインデックス定義: { $message }

# =============================================================================
# トリガーエラー
# =============================================================================

executor-trigger-not-found = トリガー '{ $name }' が見つかりません
executor-trigger-already-exists = トリガー '{ $name }' はすでに存在します

# =============================================================================
# スキーマエラー
# =============================================================================

executor-schema-not-found = スキーマ '{ $name }' が見つかりません
executor-schema-already-exists = スキーマ '{ $name }' はすでに存在します
executor-schema-not-empty = スキーマ '{ $name }' を削除できません: スキーマが空ではありません

# =============================================================================
# ロールと権限エラー
# =============================================================================

executor-role-not-found = ロール '{ $name }' が見つかりません
executor-permission-denied = 権限拒否: ロール '{ $role }' には { $object } に対する { $privilege } 権限がありません
executor-dependent-privileges-exist = 依存する権限が存在します: { $message }

# =============================================================================
# 型エラー
# =============================================================================

executor-type-not-found = 型 '{ $name }' が見つかりません
executor-type-already-exists = 型 '{ $name }' はすでに存在します
executor-type-in-use = 型 '{ $name }' を削除できません: 型がまだ使用中です
executor-type-mismatch = 型の不一致: { $left } { $op } { $right }
executor-type-error = 型エラー: { $message }
executor-cast-error = { $from_type } を { $to_type } にキャストできません
executor-type-conversion-error = { $from } を { $to } に変換できません

# =============================================================================
# 式とクエリエラー
# =============================================================================

executor-division-by-zero = ゼロによる除算
executor-invalid-where-clause = 無効なWHERE句: { $message }
executor-unsupported-expression = サポートされていない式: { $message }
executor-unsupported-feature = サポートされていない機能: { $message }
executor-parse-error = 解析エラー: { $message }

# =============================================================================
# サブクエリエラー
# =============================================================================

executor-subquery-returned-multiple-rows = スカラーサブクエリが { $actual } 行を返しました（期待値: { $expected }）
executor-subquery-column-count-mismatch = サブクエリが { $actual } カラムを返しました（期待値: { $expected }）
executor-column-count-mismatch = 派生カラムリストには { $provided } カラムがありますが、クエリは { $expected } カラムを生成します

# =============================================================================
# 制約エラー
# =============================================================================

executor-constraint-violation = 制約違反: { $message }
executor-multiple-primary-keys = 複数のPRIMARY KEY制約は許可されていません
executor-cannot-drop-column = カラムを削除できません: { $message }
executor-constraint-not-found = テーブル '{ $table_name }' に制約 '{ $constraint_name }' が見つかりません

# =============================================================================
# リソース制限エラー
# =============================================================================

executor-expression-depth-exceeded = 式の深さ制限を超えました: { $depth } > { $max_depth }（スタックオーバーフローを防止）
executor-query-timeout-exceeded = クエリタイムアウトを超えました: { $elapsed_seconds }秒 > { $max_seconds }秒
executor-row-limit-exceeded = 行処理制限を超えました: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = メモリ制限を超えました: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# プロシージャ/変数エラー
# =============================================================================

executor-variable-not-found-simple = 変数 '{ $variable_name }' が見つかりません
executor-variable-not-found-with-available = 変数 '{ $variable_name }' が見つかりません。利用可能な変数: { $available_variables }
executor-label-not-found = ラベル '{ $name }' が見つかりません

# =============================================================================
# SELECT INTOエラー
# =============================================================================

executor-select-into-row-count = プロシージャルSELECT INTOは正確に { $expected } 行を返す必要がありますが、{ $actual } 行{ $plural }を取得しました
executor-select-into-column-count = プロシージャルSELECT INTOのカラム数不一致: { $expected } 個{ $expected_plural }の変数がありますが、クエリは { $actual } カラム{ $actual_plural }を返しました

# =============================================================================
# プロシージャと関数エラー
# =============================================================================

executor-procedure-not-found-simple = スキーマ '{ $schema_name }' にプロシージャ '{ $procedure_name }' が見つかりません
executor-procedure-not-found-with-available = スキーマ '{ $schema_name }' にプロシージャ '{ $procedure_name }' が見つかりません
    .available = 利用可能なプロシージャ: { $available_procedures }
executor-procedure-not-found-with-suggestion = スキーマ '{ $schema_name }' にプロシージャ '{ $procedure_name }' が見つかりません
    .available = 利用可能なプロシージャ: { $available_procedures }
    .suggestion = '{ $suggestion }' のことですか？

executor-function-not-found-simple = スキーマ '{ $schema_name }' に関数 '{ $function_name }' が見つかりません
executor-function-not-found-with-available = スキーマ '{ $schema_name }' に関数 '{ $function_name }' が見つかりません
    .available = 利用可能な関数: { $available_functions }
executor-function-not-found-with-suggestion = スキーマ '{ $schema_name }' に関数 '{ $function_name }' が見つかりません
    .available = 利用可能な関数: { $available_functions }
    .suggestion = '{ $suggestion }' のことですか？

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' は { $expected } 個{ $expected_plural }のパラメータ（{ $parameter_signature }）を期待しますが、{ $actual } 個{ $actual_plural }の引数を受け取りました
executor-parameter-type-mismatch = パラメータ '{ $parameter_name }' は { $expected_type } を期待しますが、{ $actual_type } '{ $actual_value }' を受け取りました
executor-argument-count-mismatch = 引数の数が一致しません: 期待値 { $expected }、取得値 { $actual }

executor-recursion-limit-exceeded = 最大再帰深度（{ $max_depth }）を超えました: { $message }
executor-recursion-call-stack = コールスタック:
executor-function-must-return = 関数は値を返す必要があります
executor-invalid-control-flow = 無効な制御フロー: { $message }
executor-invalid-function-body = 無効な関数本体: { $message }
executor-function-read-only-violation = 関数の読み取り専用違反: { $message }

# =============================================================================
# EXTRACTエラー
# =============================================================================

executor-invalid-extract-field = { $value_type } 型の値から { $field } を抽出できません

# =============================================================================
# カラムナー/Arrowエラー
# =============================================================================

executor-arrow-downcast-error = Arrow配列を { $expected_type } にダウンキャストできませんでした（{ $context }）
executor-columnar-type-mismatch-binary = { $operation } の型が互換性がありません: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = { $operation } の型が互換性がありません: { $left_type }
executor-simd-operation-failed = SIMD { $operation } が失敗しました: { $reason }
executor-columnar-column-not-found = カラムインデックス { $column_index } が範囲外です（バッチには { $batch_columns } カラムがあります）
executor-columnar-column-not-found-by-name = カラムが見つかりません: { $column_name }
executor-columnar-length-mismatch = { $context } でカラム長が一致しません: 期待値 { $expected }、取得値 { $actual }
executor-unsupported-array-type = { $operation } でサポートされていない配列型: { $array_type }

# =============================================================================
# 空間エラー
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } は { $expected } を期待しますが、{ $actual } を受け取りました

# =============================================================================
# カーソルエラー
# =============================================================================

executor-cursor-already-exists = カーソル '{ $name }' はすでに存在します
executor-cursor-not-found = カーソル '{ $name }' が見つかりません
executor-cursor-already-open = カーソル '{ $name }' はすでに開いています
executor-cursor-not-open = カーソル '{ $name }' は開いていません
executor-cursor-not-scrollable = カーソル '{ $name }' はスクロール可能ではありません（SCROLLが指定されていません）

# =============================================================================
# ストレージと一般エラー
# =============================================================================

executor-storage-error = ストレージエラー: { $message }
executor-other = { $message }
