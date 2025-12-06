# VibeSQL Executor Error Messages - Thai (ภาษาไทย)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = ไม่พบตาราง '{ $name }'
executor-table-already-exists = ตาราง '{ $name }' มีอยู่แล้ว

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = ไม่พบคอลัมน์ '{ $column_name }' ในตาราง '{ $table_name }'
executor-column-not-found-searched = ไม่พบคอลัมน์ '{ $column_name }' (ค้นหาในตาราง: { $searched_tables })
executor-column-not-found-with-available = ไม่พบคอลัมน์ '{ $column_name }' (ค้นหาในตาราง: { $searched_tables }) คอลัมน์ที่มี: { $available_columns }
executor-invalid-table-qualifier = Table qualifier '{ $qualifier }' ไม่ถูกต้องสำหรับคอลัมน์ '{ $column }' ตารางที่มี: { $available_tables }
executor-column-already-exists = คอลัมน์ '{ $name }' มีอยู่แล้ว
executor-column-index-out-of-bounds = ดัชนีคอลัมน์ { $index } เกินขอบเขต

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = ไม่พบ Index '{ $name }'
executor-index-already-exists = Index '{ $name }' มีอยู่แล้ว
executor-invalid-index-definition = คำจำกัดความ Index ไม่ถูกต้อง: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = ไม่พบ Trigger '{ $name }'
executor-trigger-already-exists = Trigger '{ $name }' มีอยู่แล้ว

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = ไม่พบ Schema '{ $name }'
executor-schema-already-exists = Schema '{ $name }' มีอยู่แล้ว
executor-schema-not-empty = ไม่สามารถลบ schema '{ $name }': schema ไม่ว่าง

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = ไม่พบ Role '{ $name }'
executor-permission-denied = ปฏิเสธการอนุญาต: role '{ $role }' ไม่มีสิทธิ์ { $privilege } บน { $object }
executor-dependent-privileges-exist = มีสิทธิ์ที่ขึ้นอยู่กับ: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = ไม่พบประเภท '{ $name }'
executor-type-already-exists = ประเภท '{ $name }' มีอยู่แล้ว
executor-type-in-use = ไม่สามารถลบประเภท '{ $name }': ประเภทยังถูกใช้งานอยู่
executor-type-mismatch = ประเภทไม่ตรงกัน: { $left } { $op } { $right }
executor-type-error = ข้อผิดพลาดประเภท: { $message }
executor-cast-error = ไม่สามารถแปลง { $from_type } เป็น { $to_type }
executor-type-conversion-error = ไม่สามารถแปลง { $from } เป็น { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = หารด้วยศูนย์
executor-invalid-where-clause = WHERE clause ไม่ถูกต้อง: { $message }
executor-unsupported-expression = Expression ที่ไม่รองรับ: { $message }
executor-unsupported-feature = ฟีเจอร์ที่ไม่รองรับ: { $message }
executor-parse-error = ข้อผิดพลาดการแยกวิเคราะห์: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = Scalar subquery คืนค่า { $actual } แถว คาดหวัง { $expected }
executor-subquery-column-count-mismatch = Subquery คืนค่า { $actual } คอลัมน์ คาดหวัง { $expected }
executor-column-count-mismatch = Derived column list มี { $provided } คอลัมน์ แต่ query สร้าง { $expected } คอลัมน์

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = ละเมิด Constraint: { $message }
executor-multiple-primary-keys = ไม่อนุญาตให้มี PRIMARY KEY constraints หลายตัว
executor-cannot-drop-column = ไม่สามารถลบคอลัมน์: { $message }
executor-constraint-not-found = ไม่พบ Constraint '{ $constraint_name }' ในตาราง '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = เกินขีดจำกัดความลึกของ expression: { $depth } > { $max_depth } (ป้องกัน stack overflow)
executor-query-timeout-exceeded = เกินเวลาที่กำหนดสำหรับ query: { $elapsed_seconds } วินาที > { $max_seconds } วินาที
executor-row-limit-exceeded = เกินขีดจำกัดการประมวลผลแถว: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = เกินขีดจำกัดหน่วยความจำ: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = ไม่พบตัวแปร '{ $variable_name }'
executor-variable-not-found-with-available = ไม่พบตัวแปร '{ $variable_name }' ตัวแปรที่มี: { $available_variables }
executor-label-not-found = ไม่พบ Label '{ $name }'

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = Procedural SELECT INTO ต้องคืนค่า { $expected } แถวพอดี ได้รับ { $actual } แถว{ $plural }
executor-select-into-column-count = จำนวนคอลัมน์ SELECT INTO ไม่ตรงกัน: { $expected } ตัวแปร{ $expected_plural } แต่ query คืนค่า { $actual } คอลัมน์{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = ไม่พบ Procedure '{ $procedure_name }' ใน schema '{ $schema_name }'
executor-procedure-not-found-with-available = ไม่พบ Procedure '{ $procedure_name }' ใน schema '{ $schema_name }'
    .available = Procedures ที่มี: { $available_procedures }
executor-procedure-not-found-with-suggestion = ไม่พบ Procedure '{ $procedure_name }' ใน schema '{ $schema_name }'
    .available = Procedures ที่มี: { $available_procedures }
    .suggestion = หมายถึง '{ $suggestion }' หรือไม่?

executor-function-not-found-simple = ไม่พบ Function '{ $function_name }' ใน schema '{ $schema_name }'
executor-function-not-found-with-available = ไม่พบ Function '{ $function_name }' ใน schema '{ $schema_name }'
    .available = Functions ที่มี: { $available_functions }
executor-function-not-found-with-suggestion = ไม่พบ Function '{ $function_name }' ใน schema '{ $schema_name }'
    .available = Functions ที่มี: { $available_functions }
    .suggestion = หมายถึง '{ $suggestion }' หรือไม่?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' คาดหวัง { $expected } พารามิเตอร์{ $expected_plural } ({ $parameter_signature }) ได้รับ { $actual } อาร์กิวเมนต์{ $actual_plural }
executor-parameter-type-mismatch = พารามิเตอร์ '{ $parameter_name }' คาดหวัง { $expected_type } ได้รับ { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = จำนวนอาร์กิวเมนต์ไม่ตรงกัน: คาดหวัง { $expected } ได้รับ { $actual }

executor-recursion-limit-exceeded = เกินความลึก recursion สูงสุด ({ $max_depth }): { $message }
executor-recursion-call-stack = Call stack:
executor-function-must-return = Function ต้องคืนค่า
executor-invalid-control-flow = Control flow ไม่ถูกต้อง: { $message }
executor-invalid-function-body = Function body ไม่ถูกต้อง: { $message }
executor-function-read-only-violation = ละเมิด Function read-only: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = ไม่สามารถแยก { $field } จากค่าประเภท { $value_type }

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = ไม่สามารถ downcast Arrow array เป็น { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = ประเภทไม่เข้ากันสำหรับ { $operation }: { $left_type } กับ { $right_type }
executor-columnar-type-mismatch-unary = ประเภทไม่เข้ากันสำหรับ { $operation }: { $left_type }
executor-simd-operation-failed = SIMD { $operation } ล้มเหลว: { $reason }
executor-columnar-column-not-found = ดัชนีคอลัมน์ { $column_index } เกินขอบเขต (batch มี { $batch_columns } คอลัมน์)
executor-columnar-column-not-found-by-name = ไม่พบคอลัมน์: { $column_name }
executor-columnar-length-mismatch = ความยาวคอลัมน์ไม่ตรงกันใน { $context }: คาดหวัง { $expected } ได้รับ { $actual }
executor-unsupported-array-type = ประเภท array ที่ไม่รองรับสำหรับ { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } คาดหวัง { $expected } ได้รับ { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = Cursor '{ $name }' มีอยู่แล้ว
executor-cursor-not-found = ไม่พบ Cursor '{ $name }'
executor-cursor-already-open = Cursor '{ $name }' เปิดอยู่แล้ว
executor-cursor-not-open = Cursor '{ $name }' ไม่ได้เปิด
executor-cursor-not-scrollable = Cursor '{ $name }' ไม่สามารถเลื่อนได้ (ไม่ได้ระบุ SCROLL)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = ข้อผิดพลาด Storage: { $message }
executor-other = { $message }
