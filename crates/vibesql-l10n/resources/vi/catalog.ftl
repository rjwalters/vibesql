# VibeSQL Catalog Error Messages - Vietnamese (Tiếng Việt)
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = Bảng '{ $name }' đã tồn tại
catalog-table-not-found = Không tìm thấy bảng '{ $table_name }'

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = Cột '{ $name }' đã tồn tại
catalog-column-not-found = Không tìm thấy cột '{ $column_name }' trong bảng '{ $table_name }'

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = Schema '{ $name }' đã tồn tại
catalog-schema-not-found = Không tìm thấy schema '{ $name }'
catalog-schema-not-empty = Schema '{ $name }' không trống

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = Vai trò '{ $name }' đã tồn tại
catalog-role-not-found = Không tìm thấy vai trò '{ $name }'

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = Domain '{ $name }' đã tồn tại
catalog-domain-not-found = Không tìm thấy domain '{ $name }'
catalog-domain-in-use = Domain '{ $domain_name }' vẫn đang được sử dụng bởi { $count } cột: { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = Sequence '{ $name }' đã tồn tại
catalog-sequence-not-found = Không tìm thấy sequence '{ $name }'
catalog-sequence-in-use = Sequence '{ $sequence_name }' vẫn đang được sử dụng bởi { $count } cột: { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = Kiểu '{ $name }' đã tồn tại
catalog-type-not-found = Không tìm thấy kiểu '{ $name }'
catalog-type-in-use = Kiểu '{ $name }' vẫn đang được sử dụng bởi một hoặc nhiều bảng

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = Collation '{ $name }' đã tồn tại
catalog-collation-not-found = Không tìm thấy collation '{ $name }'
catalog-character-set-already-exists = Bộ ký tự '{ $name }' đã tồn tại
catalog-character-set-not-found = Không tìm thấy bộ ký tự '{ $name }'
catalog-translation-already-exists = Bản dịch '{ $name }' đã tồn tại
catalog-translation-not-found = Không tìm thấy bản dịch '{ $name }'

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = View '{ $name }' đã tồn tại
catalog-view-not-found = Không tìm thấy view '{ $name }'
catalog-view-in-use = View hoặc bảng '{ $view_name }' vẫn đang được sử dụng bởi { $count } view: { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = Trigger '{ $name }' đã tồn tại
catalog-trigger-not-found = Không tìm thấy trigger '{ $name }'

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = Assertion '{ $name }' đã tồn tại
catalog-assertion-not-found = Không tìm thấy assertion '{ $name }'

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = Hàm '{ $name }' đã tồn tại
catalog-function-not-found = Không tìm thấy hàm '{ $name }'
catalog-procedure-already-exists = Thủ tục '{ $name }' đã tồn tại
catalog-procedure-not-found = Không tìm thấy thủ tục '{ $name }'

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = Ràng buộc '{ $name }' đã tồn tại
catalog-constraint-not-found = Không tìm thấy ràng buộc '{ $name }'

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = Index '{ $index_name }' trên bảng '{ $table_name }' đã tồn tại
catalog-index-not-found = Không tìm thấy index '{ $index_name }' trên bảng '{ $table_name }'

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = Phát hiện phụ thuộc khóa ngoại vòng cho bảng '{ $table_name }': { $message }
