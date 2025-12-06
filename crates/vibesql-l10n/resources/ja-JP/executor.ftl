# VibeSQL Executor エラーメッセージ - 日本語
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = テーブル '{ $name }' が見つかりません
executor-table-already-exists = テーブル '{ $name }' は既に存在します

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = テーブル '{ $table_name }' にカラム '{ $column_name }' が見つかりません
executor-column-not-found-searched = カラム '{ $column_name }' が見つかりません (検索したテーブル: { $searched_tables })
executor-column-not-found-with-available = カラム '{ $column_name }' が見つかりません (検索したテーブル: { $searched_tables })。利用可能なカラム: { $available_columns }
executor-invalid-table-qualifier = カラム '{ $column }' の無効なテーブル修飾子 '{ $qualifier }'。利用可能なテーブル: { $available_tables }
executor-column-already-exists = カラム '{ $name }' は既に存在します
executor-column-index-out-of-bounds = カラムインデックス { $index } が範囲外です

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = インデックス '{ $name }' が見つかりません
executor-index-already-exists = インデックス '{ $name }' は既に存在します
executor-invalid-index-definition = 無効なインデックス定義: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = トリガー '{ $name }' が見つかりません
executor-trigger-already-exists = トリガー '{ $name }' は既に存在します

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = スキーマ '{ $name }' が見つかりません
executor-schema-already-exists = スキーマ '{ $name }' は既に存在します
executor-schema-not-empty = スキーマ '{ $name }' を削除できません: スキーマが空ではありません

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = ロール '{ $name }' が見つかりません
executor-permission-denied = 権限が拒否されました: ロール '{ $role }' は { $object } に対する { $privilege } 権限がありません
executor-dependent-privileges-exist = 依存する権限が存在します: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = 型 '{ $name }' が見つかりません
executor-type-already-exists = 型 '{ $name }' は既に存在します
executor-type-in-use = 型 '{ $name }' を削除できません: 型は使用中です
executor-type-mismatch = 型の不一致: { $left } { $op } { $right }
executor-type-error = 型エラー: { $message }
executor-cast-error = { $from_type } から { $to_type } へキャストできません
executor-type-conversion-error = { $from } から { $to } へ変換できません

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = ゼロ除算
executor-invalid-where-clause = 無効な WHERE 句: { $message }
executor-unsupported-expression = サポートされていない式: { $message }
executor-unsupported-feature = サポートされていない機能: { $message }
executor-parse-error = パースエラー: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = スカラサブクエリが { $actual } 行を返しました (期待値: { $expected })
executor-subquery-column-count-mismatch = サブクエリが { $actual } カラムを返しました (期待値: { $expected })
executor-column-count-mismatch = 派生カラムリストは { $provided } カラムですが、クエリは { $expected } カラムを生成します

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = 制約違反: { $message }
executor-multiple-primary-keys = 複数の PRIMARY KEY 制約は許可されていません
executor-cannot-drop-column = カラムを削除できません: { $message }
executor-constraint-not-found = テーブル '{ $table_name }' に制約 '{ $constraint_name }' が見つかりません

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = 式の深さ制限を超えました: { $depth } > { $max_depth } (スタックオーバーフロー防止)
executor-query-timeout-exceeded = クエリタイムアウトを超えました: { $elapsed_seconds }秒 > { $max_seconds }秒
executor-row-limit-exceeded = 行処理制限を超えました: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = メモリ制限を超えました: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = 変数 '{ $variable_name }' が見つかりません
executor-variable-not-found-with-available = 変数 '{ $variable_name }' が見つかりません。利用可能な変数: { $available_variables }
executor-label-not-found = ラベル '{ $name }' が見つかりません

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = プロシージャル SELECT INTO は正確に { $expected } 行を返す必要がありますが、{ $actual } 行{ $plural }を取得しました
executor-select-into-column-count = プロシージャル SELECT INTO のカラム数不一致: { $expected } 変数{ $expected_plural }ですが、クエリは { $actual } カラム{ $actual_plural }を返しました

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = スキーマ '{ $schema_name }' にプロシージャ '{ $procedure_name }' が見つかりません
executor-procedure-not-found-with-available = スキーマ '{ $schema_name }' にプロシージャ '{ $procedure_name }' が見つかりません
    .available = 利用可能なプロシージャ: { $available_procedures }
executor-procedure-not-found-with-suggestion = スキーマ '{ $schema_name }' にプロシージャ '{ $procedure_name }' が見つかりません
    .available = 利用可能なプロシージャ: { $available_procedures }
    .suggestion = もしかして '{ $suggestion }' ですか？

executor-function-not-found-simple = スキーマ '{ $schema_name }' に関数 '{ $function_name }' が見つかりません
executor-function-not-found-with-available = スキーマ '{ $schema_name }' に関数 '{ $function_name }' が見つかりません
    .available = 利用可能な関数: { $available_functions }
executor-function-not-found-with-suggestion = スキーマ '{ $schema_name }' に関数 '{ $function_name }' が見つかりません
    .available = 利用可能な関数: { $available_functions }
    .suggestion = もしかして '{ $suggestion }' ですか？

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' は { $expected } 個のパラメータ{ $expected_plural } ({ $parameter_signature }) を期待しますが、{ $actual } 個の引数{ $actual_plural }が渡されました
executor-parameter-type-mismatch = パラメータ '{ $parameter_name }' は { $expected_type } を期待しますが、{ $actual_type } '{ $actual_value }' が渡されました
executor-argument-count-mismatch = 引数の数が一致しません: 期待値 { $expected }、実際 { $actual }

executor-recursion-limit-exceeded = 最大再帰深度 ({ $max_depth }) を超えました: { $message }
executor-recursion-call-stack = コールスタック:
executor-function-must-return = 関数は値を返す必要があります
executor-invalid-control-flow = 無効な制御フロー: { $message }
executor-invalid-function-body = 無効な関数本体: { $message }
executor-function-read-only-violation = 関数の読み取り専用違反: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = { $value_type } 値から { $field } を抽出できません

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Arrow 配列を { $expected_type } にダウンキャストできませんでした ({ $context })
executor-columnar-type-mismatch-binary = { $operation } の互換性のない型: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = { $operation } の互換性のない型: { $left_type }
executor-simd-operation-failed = SIMD { $operation } が失敗しました: { $reason }
executor-columnar-column-not-found = カラムインデックス { $column_index } が範囲外です (バッチには { $batch_columns } カラムがあります)
executor-columnar-column-not-found-by-name = カラムが見つかりません: { $column_name }
executor-columnar-length-mismatch = { $context } でカラム長が一致しません: 期待値 { $expected }、実際 { $actual }
executor-unsupported-array-type = { $operation } でサポートされていない配列型: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } は { $expected } を期待しますが、{ $actual } が渡されました

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = カーソル '{ $name }' は既に存在します
executor-cursor-not-found = カーソル '{ $name }' が見つかりません
executor-cursor-already-open = カーソル '{ $name }' は既に開いています
executor-cursor-not-open = カーソル '{ $name }' は開いていません
executor-cursor-not-scrollable = カーソル '{ $name }' はスクロールできません (SCROLL が指定されていません)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = ストレージエラー: { $message }
executor-other = { $message }
