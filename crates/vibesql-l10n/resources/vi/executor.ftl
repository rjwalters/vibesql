# VibeSQL Executor Error Messages - Vietnamese (Tiếng Việt)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = Không tìm thấy bảng '{ $name }'
executor-table-already-exists = Bảng '{ $name }' đã tồn tại

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = Không tìm thấy cột '{ $column_name }' trong bảng '{ $table_name }'
executor-column-not-found-searched = Không tìm thấy cột '{ $column_name }' (các bảng đã tìm kiếm: { $searched_tables })
executor-column-not-found-with-available = Không tìm thấy cột '{ $column_name }' (các bảng đã tìm kiếm: { $searched_tables }). Các cột có sẵn: { $available_columns }
executor-invalid-table-qualifier = Định danh bảng không hợp lệ '{ $qualifier }' cho cột '{ $column }'. Các bảng có sẵn: { $available_tables }
executor-column-already-exists = Cột '{ $name }' đã tồn tại
executor-column-index-out-of-bounds = Chỉ số cột { $index } vượt quá giới hạn

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = Không tìm thấy index '{ $name }'
executor-index-already-exists = Index '{ $name }' đã tồn tại
executor-invalid-index-definition = Định nghĩa index không hợp lệ: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = Không tìm thấy trigger '{ $name }'
executor-trigger-already-exists = Trigger '{ $name }' đã tồn tại

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = Không tìm thấy schema '{ $name }'
executor-schema-already-exists = Schema '{ $name }' đã tồn tại
executor-schema-not-empty = Không thể xóa schema '{ $name }': schema không trống

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = Không tìm thấy vai trò '{ $name }'
executor-permission-denied = Từ chối quyền: vai trò '{ $role }' thiếu quyền { $privilege } trên { $object }
executor-dependent-privileges-exist = Tồn tại quyền phụ thuộc: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = Không tìm thấy kiểu '{ $name }'
executor-type-already-exists = Kiểu '{ $name }' đã tồn tại
executor-type-in-use = Không thể xóa kiểu '{ $name }': kiểu vẫn đang được sử dụng
executor-type-mismatch = Không khớp kiểu: { $left } { $op } { $right }
executor-type-error = Lỗi kiểu: { $message }
executor-cast-error = Không thể chuyển đổi { $from_type } sang { $to_type }
executor-type-conversion-error = Không thể chuyển đổi { $from } sang { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = Chia cho số không
executor-invalid-where-clause = Mệnh đề WHERE không hợp lệ: { $message }
executor-unsupported-expression = Biểu thức không được hỗ trợ: { $message }
executor-unsupported-feature = Tính năng không được hỗ trợ: { $message }
executor-parse-error = Lỗi phân tích cú pháp: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = Subquery vô hướng trả về { $actual } hàng, mong đợi { $expected }
executor-subquery-column-count-mismatch = Subquery trả về { $actual } cột, mong đợi { $expected }
executor-column-count-mismatch = Danh sách cột dẫn xuất có { $provided } cột nhưng truy vấn tạo ra { $expected } cột

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = Vi phạm ràng buộc: { $message }
executor-multiple-primary-keys = Không cho phép nhiều ràng buộc PRIMARY KEY
executor-cannot-drop-column = Không thể xóa cột: { $message }
executor-constraint-not-found = Không tìm thấy ràng buộc '{ $constraint_name }' trong bảng '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = Vượt quá giới hạn độ sâu biểu thức: { $depth } > { $max_depth } (ngăn tràn ngăn xếp)
executor-query-timeout-exceeded = Vượt quá thời gian chờ truy vấn: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Vượt quá giới hạn xử lý hàng: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Vượt quá giới hạn bộ nhớ: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = Không tìm thấy biến '{ $variable_name }'
executor-variable-not-found-with-available = Không tìm thấy biến '{ $variable_name }'. Các biến có sẵn: { $available_variables }
executor-label-not-found = Không tìm thấy nhãn '{ $name }'

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = SELECT INTO thủ tục phải trả về đúng { $expected } hàng, nhận được { $actual } hàng{ $plural }
executor-select-into-column-count = Số cột SELECT INTO thủ tục không khớp: { $expected } biến{ $expected_plural } nhưng truy vấn trả về { $actual } cột{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = Không tìm thấy thủ tục '{ $procedure_name }' trong schema '{ $schema_name }'
executor-procedure-not-found-with-available = Không tìm thấy thủ tục '{ $procedure_name }' trong schema '{ $schema_name }'
    .available = Các thủ tục có sẵn: { $available_procedures }
executor-procedure-not-found-with-suggestion = Không tìm thấy thủ tục '{ $procedure_name }' trong schema '{ $schema_name }'
    .available = Các thủ tục có sẵn: { $available_procedures }
    .suggestion = Có phải bạn muốn nói '{ $suggestion }'?

executor-function-not-found-simple = Không tìm thấy hàm '{ $function_name }' trong schema '{ $schema_name }'
executor-function-not-found-with-available = Không tìm thấy hàm '{ $function_name }' trong schema '{ $schema_name }'
    .available = Các hàm có sẵn: { $available_functions }
executor-function-not-found-with-suggestion = Không tìm thấy hàm '{ $function_name }' trong schema '{ $schema_name }'
    .available = Các hàm có sẵn: { $available_functions }
    .suggestion = Có phải bạn muốn nói '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' mong đợi { $expected } tham số{ $expected_plural } ({ $parameter_signature }), nhận được { $actual } đối số{ $actual_plural }
executor-parameter-type-mismatch = Tham số '{ $parameter_name }' mong đợi { $expected_type }, nhận được { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Số đối số không khớp: mong đợi { $expected }, nhận được { $actual }

executor-recursion-limit-exceeded = Vượt quá độ sâu đệ quy tối đa ({ $max_depth }): { $message }
executor-recursion-call-stack = Ngăn xếp gọi:
executor-function-must-return = Hàm phải trả về giá trị
executor-invalid-control-flow = Luồng điều khiển không hợp lệ: { $message }
executor-invalid-function-body = Thân hàm không hợp lệ: { $message }
executor-function-read-only-violation = Vi phạm chỉ đọc của hàm: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = Không thể trích xuất { $field } từ giá trị kiểu { $value_type }

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Không thể chuyển đổi mảng Arrow sang { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = Kiểu không tương thích cho { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Kiểu không tương thích cho { $operation }: { $left_type }
executor-simd-operation-failed = SIMD { $operation } thất bại: { $reason }
executor-columnar-column-not-found = Chỉ số cột { $column_index } vượt quá giới hạn (batch có { $batch_columns } cột)
executor-columnar-column-not-found-by-name = Không tìm thấy cột: { $column_name }
executor-columnar-length-mismatch = Độ dài cột không khớp trong { $context }: mong đợi { $expected }, nhận được { $actual }
executor-unsupported-array-type = Kiểu mảng không được hỗ trợ cho { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } mong đợi { $expected }, nhận được { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = Con trỏ '{ $name }' đã tồn tại
executor-cursor-not-found = Không tìm thấy con trỏ '{ $name }'
executor-cursor-already-open = Con trỏ '{ $name }' đã được mở
executor-cursor-not-open = Con trỏ '{ $name }' chưa được mở
executor-cursor-not-scrollable = Con trỏ '{ $name }' không thể cuộn (SCROLL không được chỉ định)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = Lỗi lưu trữ: { $message }
executor-other = { $message }
