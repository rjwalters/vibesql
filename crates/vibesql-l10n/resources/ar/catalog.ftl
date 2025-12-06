# VibeSQL Catalog Error Messages - Arabic (ar)
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = الجدول '{ $name }' موجود بالفعل
catalog-table-not-found = الجدول '{ $table_name }' غير موجود

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = العمود '{ $name }' موجود بالفعل
catalog-column-not-found = العمود '{ $column_name }' غير موجود في الجدول '{ $table_name }'

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = المخطط '{ $name }' موجود بالفعل
catalog-schema-not-found = المخطط '{ $name }' غير موجود
catalog-schema-not-empty = المخطط '{ $name }' ليس فارغاً

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = الدور '{ $name }' موجود بالفعل
catalog-role-not-found = الدور '{ $name }' غير موجود

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = النطاق '{ $name }' موجود بالفعل
catalog-domain-not-found = النطاق '{ $name }' غير موجود
catalog-domain-in-use = النطاق '{ $domain_name }' لا يزال قيد الاستخدام بواسطة { $count } عمود/أعمدة: { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = التسلسل '{ $name }' موجود بالفعل
catalog-sequence-not-found = التسلسل '{ $name }' غير موجود
catalog-sequence-in-use = التسلسل '{ $sequence_name }' لا يزال قيد الاستخدام بواسطة { $count } عمود/أعمدة: { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = النوع '{ $name }' موجود بالفعل
catalog-type-not-found = النوع '{ $name }' غير موجود
catalog-type-in-use = النوع '{ $name }' لا يزال قيد الاستخدام بواسطة جدول واحد أو أكثر

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = الترتيب '{ $name }' موجود بالفعل
catalog-collation-not-found = الترتيب '{ $name }' غير موجود
catalog-character-set-already-exists = مجموعة الأحرف '{ $name }' موجودة بالفعل
catalog-character-set-not-found = مجموعة الأحرف '{ $name }' غير موجودة
catalog-translation-already-exists = الترجمة '{ $name }' موجودة بالفعل
catalog-translation-not-found = الترجمة '{ $name }' غير موجودة

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = العرض '{ $name }' موجود بالفعل
catalog-view-not-found = العرض '{ $name }' غير موجود
catalog-view-in-use = العرض أو الجدول '{ $view_name }' لا يزال قيد الاستخدام بواسطة { $count } عرض/عروض: { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = المشغّل '{ $name }' موجود بالفعل
catalog-trigger-not-found = المشغّل '{ $name }' غير موجود

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = التأكيد '{ $name }' موجود بالفعل
catalog-assertion-not-found = التأكيد '{ $name }' غير موجود

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = الدالة '{ $name }' موجودة بالفعل
catalog-function-not-found = الدالة '{ $name }' غير موجودة
catalog-procedure-already-exists = الإجراء '{ $name }' موجود بالفعل
catalog-procedure-not-found = الإجراء '{ $name }' غير موجود

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = القيد '{ $name }' موجود بالفعل
catalog-constraint-not-found = القيد '{ $name }' غير موجود

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = الفهرس '{ $index_name }' على الجدول '{ $table_name }' موجود بالفعل
catalog-index-not-found = الفهرس '{ $index_name }' على الجدول '{ $table_name }' غير موجود

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = تم اكتشاف تبعية مفتاح خارجي دائرية للجدول '{ $table_name }': { $message }
