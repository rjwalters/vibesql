# VibeSQL Catalog エラーメッセージ - 日本語
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = テーブル '{ $name }' は既に存在します
catalog-table-not-found = テーブル '{ $table_name }' が見つかりません

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = カラム '{ $name }' は既に存在します
catalog-column-not-found = テーブル '{ $table_name }' にカラム '{ $column_name }' が見つかりません

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = スキーマ '{ $name }' は既に存在します
catalog-schema-not-found = スキーマ '{ $name }' が見つかりません
catalog-schema-not-empty = スキーマ '{ $name }' は空ではありません

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = ロール '{ $name }' は既に存在します
catalog-role-not-found = ロール '{ $name }' が見つかりません

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = ドメイン '{ $name }' は既に存在します
catalog-domain-not-found = ドメイン '{ $name }' が見つかりません
catalog-domain-in-use = ドメイン '{ $domain_name }' は { $count } 個のカラムで使用中です: { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = シーケンス '{ $name }' は既に存在します
catalog-sequence-not-found = シーケンス '{ $name }' が見つかりません
catalog-sequence-in-use = シーケンス '{ $sequence_name }' は { $count } 個のカラムで使用中です: { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = 型 '{ $name }' は既に存在します
catalog-type-not-found = 型 '{ $name }' が見つかりません
catalog-type-in-use = 型 '{ $name }' は 1 つ以上のテーブルで使用中です

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = 照合順序 '{ $name }' は既に存在します
catalog-collation-not-found = 照合順序 '{ $name }' が見つかりません
catalog-character-set-already-exists = 文字セット '{ $name }' は既に存在します
catalog-character-set-not-found = 文字セット '{ $name }' が見つかりません
catalog-translation-already-exists = 変換 '{ $name }' は既に存在します
catalog-translation-not-found = 変換 '{ $name }' が見つかりません

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = ビュー '{ $name }' は既に存在します
catalog-view-not-found = ビュー '{ $name }' が見つかりません
catalog-view-in-use = ビューまたはテーブル '{ $view_name }' は { $count } 個のビューで使用中です: { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = トリガー '{ $name }' は既に存在します
catalog-trigger-not-found = トリガー '{ $name }' が見つかりません

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = アサーション '{ $name }' は既に存在します
catalog-assertion-not-found = アサーション '{ $name }' が見つかりません

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = 関数 '{ $name }' は既に存在します
catalog-function-not-found = 関数 '{ $name }' が見つかりません
catalog-procedure-already-exists = プロシージャ '{ $name }' は既に存在します
catalog-procedure-not-found = プロシージャ '{ $name }' が見つかりません

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = 制約 '{ $name }' は既に存在します
catalog-constraint-not-found = 制約 '{ $name }' が見つかりません

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = テーブル '{ $table_name }' のインデックス '{ $index_name }' は既に存在します
catalog-index-not-found = テーブル '{ $table_name }' のインデックス '{ $index_name }' が見つかりません

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = テーブル '{ $table_name }' で循環外部キー依存が検出されました: { $message }
