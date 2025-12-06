# VibeSQL Storage Error Messages - Thai (ภาษาไทย)
# This file contains all error messages for the vibesql-storage crate.

# =============================================================================
# Table Errors
# =============================================================================

storage-table-not-found = ไม่พบตาราง '{ $name }'

# =============================================================================
# Column Errors
# =============================================================================

storage-column-count-mismatch = จำนวนคอลัมน์ไม่ตรงกัน: คาดหวัง { $expected } ได้รับ { $actual }
storage-column-index-out-of-bounds = ดัชนีคอลัมน์ { $index } เกินขอบเขต
storage-column-not-found = ไม่พบคอลัมน์ '{ $column_name }' ในตาราง '{ $table_name }'

# =============================================================================
# Index Errors
# =============================================================================

storage-index-already-exists = Index '{ $name }' มีอยู่แล้ว
storage-index-not-found = ไม่พบ Index '{ $name }'
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint Errors
# =============================================================================

storage-null-constraint-violation = ละเมิด NOT NULL constraint: คอลัมน์ '{ $column }' ไม่สามารถเป็น NULL
storage-unique-constraint-violation = { $message }

# =============================================================================
# Type Errors
# =============================================================================

storage-type-mismatch = ประเภทไม่ตรงกันในคอลัมน์ '{ $column }': คาดหวัง { $expected } ได้รับ { $actual }

# =============================================================================
# Transaction and Catalog Errors
# =============================================================================

storage-catalog-error = ข้อผิดพลาด Catalog: { $message }
storage-transaction-error = ข้อผิดพลาด Transaction: { $message }
storage-row-not-found = ไม่พบแถว

# =============================================================================
# I/O and Page Errors
# =============================================================================

storage-io-error = ข้อผิดพลาด I/O: { $message }
storage-invalid-page-size = ขนาดหน้าไม่ถูกต้อง: คาดหวัง { $expected } ได้รับ { $actual }
storage-invalid-page-id = Page ID ไม่ถูกต้อง: { $page_id }
storage-lock-error = ข้อผิดพลาด Lock: { $message }

# =============================================================================
# Memory Errors
# =============================================================================

storage-memory-budget-exceeded = เกินงบประมาณหน่วยความจำ: ใช้ { $used } ไบต์ งบประมาณคือ { $budget } ไบต์
storage-no-index-to-evict = ไม่มี index ที่สามารถลบออกได้ (indexes ทั้งหมดเป็น disk-backed แล้ว)

# =============================================================================
# General Errors
# =============================================================================

storage-not-implemented = ยังไม่ได้ implement: { $message }
storage-other = { $message }
