# VibeSQL Storage エラーメッセージ - 日本語
# This file contains all error messages for the vibesql-storage crate.

# =============================================================================
# Table Errors
# =============================================================================

storage-table-not-found = テーブル '{ $name }' が見つかりません

# =============================================================================
# Column Errors
# =============================================================================

storage-column-count-mismatch = カラム数の不一致: 期待値 { $expected }、実際 { $actual }
storage-column-index-out-of-bounds = カラムインデックス { $index } が範囲外です
storage-column-not-found = テーブル '{ $table_name }' にカラム '{ $column_name }' が見つかりません

# =============================================================================
# Index Errors
# =============================================================================

storage-index-already-exists = インデックス '{ $name }' は既に存在します
storage-index-not-found = インデックス '{ $name }' が見つかりません
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint Errors
# =============================================================================

storage-null-constraint-violation = NOT NULL 制約違反: カラム '{ $column }' は NULL にできません
storage-unique-constraint-violation = { $message }

# =============================================================================
# Type Errors
# =============================================================================

storage-type-mismatch = カラム '{ $column }' の型不一致: 期待値 { $expected }、実際 { $actual }

# =============================================================================
# Transaction and Catalog Errors
# =============================================================================

storage-catalog-error = カタログエラー: { $message }
storage-transaction-error = トランザクションエラー: { $message }
storage-row-not-found = 行が見つかりません

# =============================================================================
# I/O and Page Errors
# =============================================================================

storage-io-error = I/O エラー: { $message }
storage-invalid-page-size = 無効なページサイズ: 期待値 { $expected }、実際 { $actual }
storage-invalid-page-id = 無効なページ ID: { $page_id }
storage-lock-error = ロックエラー: { $message }

# =============================================================================
# Memory Errors
# =============================================================================

storage-memory-budget-exceeded = メモリ予算超過: 使用中 { $used } バイト、予算 { $budget } バイト
storage-no-index-to-evict = 退避可能なインデックスがありません (すべてのインデックスは既にディスクバック)

# =============================================================================
# General Errors
# =============================================================================

storage-not-implemented = 未実装: { $message }
storage-other = { $message }
