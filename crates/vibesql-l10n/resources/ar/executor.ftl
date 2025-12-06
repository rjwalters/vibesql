# VibeSQL Executor Error Messages - Arabic (ar)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = الجدول '{ $name }' غير موجود
executor-table-already-exists = الجدول '{ $name }' موجود بالفعل

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = العمود '{ $column_name }' غير موجود في الجدول '{ $table_name }'
executor-column-not-found-searched = العمود '{ $column_name }' غير موجود (الجداول التي تم البحث فيها: { $searched_tables })
executor-column-not-found-with-available = العمود '{ $column_name }' غير موجود (الجداول التي تم البحث فيها: { $searched_tables }). الأعمدة المتاحة: { $available_columns }
executor-invalid-table-qualifier = معرّف جدول غير صالح '{ $qualifier }' للعمود '{ $column }'. الجداول المتاحة: { $available_tables }
executor-column-already-exists = العمود '{ $name }' موجود بالفعل
executor-column-index-out-of-bounds = فهرس العمود { $index } خارج النطاق

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = الفهرس '{ $name }' غير موجود
executor-index-already-exists = الفهرس '{ $name }' موجود بالفعل
executor-invalid-index-definition = تعريف فهرس غير صالح: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = المشغّل '{ $name }' غير موجود
executor-trigger-already-exists = المشغّل '{ $name }' موجود بالفعل

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = المخطط '{ $name }' غير موجود
executor-schema-already-exists = المخطط '{ $name }' موجود بالفعل
executor-schema-not-empty = لا يمكن حذف المخطط '{ $name }': المخطط ليس فارغاً

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = الدور '{ $name }' غير موجود
executor-permission-denied = تم رفض الإذن: الدور '{ $role }' يفتقر إلى صلاحية { $privilege } على { $object }
executor-dependent-privileges-exist = توجد صلاحيات تابعة: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = النوع '{ $name }' غير موجود
executor-type-already-exists = النوع '{ $name }' موجود بالفعل
executor-type-in-use = لا يمكن حذف النوع '{ $name }': النوع لا يزال قيد الاستخدام
executor-type-mismatch = عدم تطابق النوع: { $left } { $op } { $right }
executor-type-error = خطأ في النوع: { $message }
executor-cast-error = لا يمكن تحويل { $from_type } إلى { $to_type }
executor-type-conversion-error = لا يمكن تحويل { $from } إلى { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = القسمة على صفر
executor-invalid-where-clause = شرط WHERE غير صالح: { $message }
executor-unsupported-expression = تعبير غير مدعوم: { $message }
executor-unsupported-feature = ميزة غير مدعومة: { $message }
executor-parse-error = خطأ في التحليل: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = أعاد الاستعلام الفرعي القياسي { $actual } صف/صفوف، المتوقع { $expected }
executor-subquery-column-count-mismatch = أعاد الاستعلام الفرعي { $actual } عمود/أعمدة، المتوقع { $expected }
executor-column-count-mismatch = قائمة الأعمدة المشتقة تحتوي على { $provided } عمود/أعمدة لكن الاستعلام ينتج { $expected } عمود/أعمدة

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = انتهاك القيد: { $message }
executor-multiple-primary-keys = لا يُسمح بقيود PRIMARY KEY متعددة
executor-cannot-drop-column = لا يمكن حذف العمود: { $message }
executor-constraint-not-found = القيد '{ $constraint_name }' غير موجود في الجدول '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = تم تجاوز حد عمق التعبير: { $depth } > { $max_depth } (يمنع تجاوز المكدس)
executor-query-timeout-exceeded = تم تجاوز مهلة الاستعلام: { $elapsed_seconds } ثانية > { $max_seconds } ثانية
executor-row-limit-exceeded = تم تجاوز حد معالجة الصفوف: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = تم تجاوز حد الذاكرة: { $used_gb } جيجابايت > { $max_gb } جيجابايت

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = المتغير '{ $variable_name }' غير موجود
executor-variable-not-found-with-available = المتغير '{ $variable_name }' غير موجود. المتغيرات المتاحة: { $available_variables }
executor-label-not-found = التسمية '{ $name }' غير موجودة

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = يجب أن يُعيد SELECT INTO الإجرائي بالضبط { $expected } صف، تم الحصول على { $actual } صف{ $plural }
executor-select-into-column-count = عدم تطابق عدد أعمدة SELECT INTO الإجرائي: { $expected } متغير{ $expected_plural } لكن الاستعلام أعاد { $actual } عمود{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = الإجراء '{ $procedure_name }' غير موجود في المخطط '{ $schema_name }'
executor-procedure-not-found-with-available = الإجراء '{ $procedure_name }' غير موجود في المخطط '{ $schema_name }'
    .available = الإجراءات المتاحة: { $available_procedures }
executor-procedure-not-found-with-suggestion = الإجراء '{ $procedure_name }' غير موجود في المخطط '{ $schema_name }'
    .available = الإجراءات المتاحة: { $available_procedures }
    .suggestion = هل تقصد '{ $suggestion }'؟

executor-function-not-found-simple = الدالة '{ $function_name }' غير موجودة في المخطط '{ $schema_name }'
executor-function-not-found-with-available = الدالة '{ $function_name }' غير موجودة في المخطط '{ $schema_name }'
    .available = الدوال المتاحة: { $available_functions }
executor-function-not-found-with-suggestion = الدالة '{ $function_name }' غير موجودة في المخطط '{ $schema_name }'
    .available = الدوال المتاحة: { $available_functions }
    .suggestion = هل تقصد '{ $suggestion }'؟

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' يتوقع { $expected } معلمة{ $expected_plural } ({ $parameter_signature })، تم الحصول على { $actual } وسيطة{ $actual_plural }
executor-parameter-type-mismatch = المعلمة '{ $parameter_name }' تتوقع { $expected_type }، تم الحصول على { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = عدم تطابق عدد الوسائط: المتوقع { $expected }، تم الحصول على { $actual }

executor-recursion-limit-exceeded = تم تجاوز الحد الأقصى لعمق التكرار ({ $max_depth }): { $message }
executor-recursion-call-stack = مكدس الاستدعاءات:
executor-function-must-return = يجب أن تُعيد الدالة قيمة
executor-invalid-control-flow = تدفق تحكم غير صالح: { $message }
executor-invalid-function-body = جسم دالة غير صالح: { $message }
executor-function-read-only-violation = انتهاك للقراءة فقط في الدالة: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = لا يمكن استخراج { $field } من قيمة من نوع { $value_type }

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = فشل تحويل مصفوفة Arrow إلى { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = أنواع غير متوافقة لـ { $operation }: { $left_type } مقابل { $right_type }
executor-columnar-type-mismatch-unary = نوع غير متوافق لـ { $operation }: { $left_type }
executor-simd-operation-failed = فشلت عملية SIMD { $operation }: { $reason }
executor-columnar-column-not-found = فهرس العمود { $column_index } خارج النطاق (الدفعة تحتوي على { $batch_columns } عمود/أعمدة)
executor-columnar-column-not-found-by-name = العمود غير موجود: { $column_name }
executor-columnar-length-mismatch = عدم تطابق طول العمود في { $context }: المتوقع { $expected }، تم الحصول على { $actual }
executor-unsupported-array-type = نوع مصفوفة غير مدعوم لـ { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } يتوقع { $expected }، تم الحصول على { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = المؤشر '{ $name }' موجود بالفعل
executor-cursor-not-found = المؤشر '{ $name }' غير موجود
executor-cursor-already-open = المؤشر '{ $name }' مفتوح بالفعل
executor-cursor-not-open = المؤشر '{ $name }' غير مفتوح
executor-cursor-not-scrollable = المؤشر '{ $name }' غير قابل للتمرير (لم يتم تحديد SCROLL)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = خطأ في التخزين: { $message }
executor-other = { $message }
