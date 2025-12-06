# VibeSQL Storage Error Messages - Vietnamese (Tiếng Việt)
# This file contains all error messages for the vibesql-storage crate.

# =============================================================================
# Table Errors
# =============================================================================

storage-table-not-found = Không tìm thấy bảng '{ $name }'

# =============================================================================
# Column Errors
# =============================================================================

storage-column-count-mismatch = Số cột không khớp: mong đợi { $expected }, nhận được { $actual }
storage-column-index-out-of-bounds = Chỉ số cột { $index } vượt quá giới hạn
storage-column-not-found = Không tìm thấy cột '{ $column_name }' trong bảng '{ $table_name }'

# =============================================================================
# Index Errors
# =============================================================================

storage-index-already-exists = Index '{ $name }' đã tồn tại
storage-index-not-found = Không tìm thấy index '{ $name }'
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint Errors
# =============================================================================

storage-null-constraint-violation = Vi phạm ràng buộc NOT NULL: cột '{ $column }' không thể là NULL
storage-unique-constraint-violation = { $message }

# =============================================================================
# Type Errors
# =============================================================================

storage-type-mismatch = Không khớp kiểu trong cột '{ $column }': mong đợi { $expected }, nhận được { $actual }

# =============================================================================
# Transaction and Catalog Errors
# =============================================================================

storage-catalog-error = Lỗi catalog: { $message }
storage-transaction-error = Lỗi giao dịch: { $message }
storage-row-not-found = Không tìm thấy hàng

# =============================================================================
# I/O and Page Errors
# =============================================================================

storage-io-error = Lỗi I/O: { $message }
storage-invalid-page-size = Kích thước trang không hợp lệ: mong đợi { $expected }, nhận được { $actual }
storage-invalid-page-id = ID trang không hợp lệ: { $page_id }
storage-lock-error = Lỗi khóa: { $message }

# =============================================================================
# Memory Errors
# =============================================================================

storage-memory-budget-exceeded = Vượt quá ngân sách bộ nhớ: đang sử dụng { $used } bytes, ngân sách là { $budget } bytes
storage-no-index-to-evict = Không có index nào để xóa bỏ (tất cả index đã được lưu trên đĩa)

# =============================================================================
# General Errors
# =============================================================================

storage-not-implemented = Chưa được triển khai: { $message }
storage-other = { $message }
