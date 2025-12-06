# VibeSQL Storage Error Messages - English (US)
# This file contains all error messages for the vibesql-storage crate.

# =============================================================================
# Table Errors
# =============================================================================

storage-table-not-found = Table '{ $name }' not found

# =============================================================================
# Column Errors
# =============================================================================

storage-column-count-mismatch = Column count mismatch: expected { $expected }, got { $actual }
storage-column-index-out-of-bounds = Column index { $index } out of bounds
storage-column-not-found = Column '{ $column_name }' not found in table '{ $table_name }'

# =============================================================================
# Index Errors
# =============================================================================

storage-index-already-exists = Index '{ $name }' already exists
storage-index-not-found = Index '{ $name }' not found
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint Errors
# =============================================================================

storage-null-constraint-violation = NOT NULL constraint violation: column '{ $column }' cannot be NULL
storage-unique-constraint-violation = { $message }

# =============================================================================
# Type Errors
# =============================================================================

storage-type-mismatch = Type mismatch in column '{ $column }': expected { $expected }, got { $actual }

# =============================================================================
# Transaction and Catalog Errors
# =============================================================================

storage-catalog-error = Catalog error: { $message }
storage-transaction-error = Transaction error: { $message }
storage-row-not-found = Row not found

# =============================================================================
# I/O and Page Errors
# =============================================================================

storage-io-error = I/O error: { $message }
storage-invalid-page-size = Invalid page size: expected { $expected }, got { $actual }
storage-invalid-page-id = Invalid page ID: { $page_id }
storage-lock-error = Lock error: { $message }

# =============================================================================
# Memory Errors
# =============================================================================

storage-memory-budget-exceeded = Memory budget exceeded: using { $used } bytes, budget is { $budget } bytes
storage-no-index-to-evict = No index available to evict (all indexes are already disk-backed)

# =============================================================================
# General Errors
# =============================================================================

storage-not-implemented = Not implemented: { $message }
storage-other = { $message }
