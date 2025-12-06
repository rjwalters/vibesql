# VibeSQL Storage Error Messages - Swedish (Svenska)
# This file contains all error messages for the vibesql-storage crate.

# =============================================================================
# Table Errors
# =============================================================================

storage-table-not-found = Tabellen '{ $name }' hittades inte

# =============================================================================
# Column Errors
# =============================================================================

storage-column-count-mismatch = Kolumnantal matchar inte: förväntade { $expected }, fick { $actual }
storage-column-index-out-of-bounds = Kolumnindex { $index } utanför gränserna
storage-column-not-found = Kolumnen '{ $column_name }' hittades inte i tabellen '{ $table_name }'

# =============================================================================
# Index Errors
# =============================================================================

storage-index-already-exists = Indexet '{ $name }' finns redan
storage-index-not-found = Indexet '{ $name }' hittades inte
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint Errors
# =============================================================================

storage-null-constraint-violation = NOT NULL-villkorsöverträdelse: kolumnen '{ $column }' kan inte vara NULL
storage-unique-constraint-violation = { $message }

# =============================================================================
# Type Errors
# =============================================================================

storage-type-mismatch = Typmatchningsfel i kolumnen '{ $column }': förväntade { $expected }, fick { $actual }

# =============================================================================
# Transaction and Catalog Errors
# =============================================================================

storage-catalog-error = Katalogfel: { $message }
storage-transaction-error = Transaktionsfel: { $message }
storage-row-not-found = Rad hittades inte

# =============================================================================
# I/O and Page Errors
# =============================================================================

storage-io-error = I/O-fel: { $message }
storage-invalid-page-size = Ogiltig sidstorlek: förväntade { $expected }, fick { $actual }
storage-invalid-page-id = Ogiltigt sid-ID: { $page_id }
storage-lock-error = Låsfel: { $message }

# =============================================================================
# Memory Errors
# =============================================================================

storage-memory-budget-exceeded = Minnesbudget överskriden: använder { $used } byte, budget är { $budget } byte
storage-no-index-to-evict = Inget index tillgängligt att avlägsna (alla index är redan diskbaserade)

# =============================================================================
# General Errors
# =============================================================================

storage-not-implemented = Inte implementerat: { $message }
storage-other = { $message }
