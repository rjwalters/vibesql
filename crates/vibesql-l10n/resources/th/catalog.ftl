# VibeSQL Catalog Error Messages - Thai (ภาษาไทย)
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = ตาราง '{ $name }' มีอยู่แล้ว
catalog-table-not-found = ไม่พบตาราง '{ $table_name }'

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = คอลัมน์ '{ $name }' มีอยู่แล้ว
catalog-column-not-found = ไม่พบคอลัมน์ '{ $column_name }' ในตาราง '{ $table_name }'

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = Schema '{ $name }' มีอยู่แล้ว
catalog-schema-not-found = ไม่พบ Schema '{ $name }'
catalog-schema-not-empty = Schema '{ $name }' ไม่ว่าง

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = Role '{ $name }' มีอยู่แล้ว
catalog-role-not-found = ไม่พบ Role '{ $name }'

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = Domain '{ $name }' มีอยู่แล้ว
catalog-domain-not-found = ไม่พบ Domain '{ $name }'
catalog-domain-in-use = Domain '{ $domain_name }' ยังถูกใช้งานโดย { $count } คอลัมน์: { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = Sequence '{ $name }' มีอยู่แล้ว
catalog-sequence-not-found = ไม่พบ Sequence '{ $name }'
catalog-sequence-in-use = Sequence '{ $sequence_name }' ยังถูกใช้งานโดย { $count } คอลัมน์: { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = ประเภท '{ $name }' มีอยู่แล้ว
catalog-type-not-found = ไม่พบประเภท '{ $name }'
catalog-type-in-use = ประเภท '{ $name }' ยังถูกใช้งานโดยตารางหนึ่งหรือมากกว่า

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = Collation '{ $name }' มีอยู่แล้ว
catalog-collation-not-found = ไม่พบ Collation '{ $name }'
catalog-character-set-already-exists = Character set '{ $name }' มีอยู่แล้ว
catalog-character-set-not-found = ไม่พบ Character set '{ $name }'
catalog-translation-already-exists = Translation '{ $name }' มีอยู่แล้ว
catalog-translation-not-found = ไม่พบ Translation '{ $name }'

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = View '{ $name }' มีอยู่แล้ว
catalog-view-not-found = ไม่พบ View '{ $name }'
catalog-view-in-use = View หรือตาราง '{ $view_name }' ยังถูกใช้งานโดย { $count } view(s): { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = Trigger '{ $name }' มีอยู่แล้ว
catalog-trigger-not-found = ไม่พบ Trigger '{ $name }'

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = Assertion '{ $name }' มีอยู่แล้ว
catalog-assertion-not-found = ไม่พบ Assertion '{ $name }'

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = Function '{ $name }' มีอยู่แล้ว
catalog-function-not-found = ไม่พบ Function '{ $name }'
catalog-procedure-already-exists = Procedure '{ $name }' มีอยู่แล้ว
catalog-procedure-not-found = ไม่พบ Procedure '{ $name }'

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = Constraint '{ $name }' มีอยู่แล้ว
catalog-constraint-not-found = ไม่พบ Constraint '{ $name }'

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = Index '{ $index_name }' บนตาราง '{ $table_name }' มีอยู่แล้ว
catalog-index-not-found = ไม่พบ Index '{ $index_name }' บนตาราง '{ $table_name }'

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = ตรวจพบ circular foreign key dependency สำหรับตาราง '{ $table_name }': { $message }
