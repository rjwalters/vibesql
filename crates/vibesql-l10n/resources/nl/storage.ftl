# VibeSQL Storage Error Messages - Dutch (Nederlands)
# This file contains all error messages for the vibesql-storage crate.

# =============================================================================
# Table Errors
# =============================================================================

storage-table-not-found = Tabel '{ $name }' niet gevonden

# =============================================================================
# Column Errors
# =============================================================================

storage-column-count-mismatch = Kolomaantal komt niet overeen: verwacht { $expected }, kreeg { $actual }
storage-column-index-out-of-bounds = Kolomindex { $index } buiten bereik
storage-column-not-found = Kolom '{ $column_name }' niet gevonden in tabel '{ $table_name }'

# =============================================================================
# Index Errors
# =============================================================================

storage-index-already-exists = Index '{ $name }' bestaat al
storage-index-not-found = Index '{ $name }' niet gevonden
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint Errors
# =============================================================================

storage-null-constraint-violation = NOT NULL-constraintschending: kolom '{ $column }' kan niet NULL zijn
storage-unique-constraint-violation = { $message }

# =============================================================================
# Type Errors
# =============================================================================

storage-type-mismatch = Type komt niet overeen in kolom '{ $column }': verwacht { $expected }, kreeg { $actual }

# =============================================================================
# Transaction and Catalog Errors
# =============================================================================

storage-catalog-error = Catalogusfout: { $message }
storage-transaction-error = Transactiefout: { $message }
storage-row-not-found = Rij niet gevonden

# =============================================================================
# I/O and Page Errors
# =============================================================================

storage-io-error = I/O-fout: { $message }
storage-invalid-page-size = Ongeldige paginagrootte: verwacht { $expected }, kreeg { $actual }
storage-invalid-page-id = Ongeldig pagina-ID: { $page_id }
storage-lock-error = Vergrendelingsfout: { $message }

# =============================================================================
# Memory Errors
# =============================================================================

storage-memory-budget-exceeded = Geheugenbudget overschreden: gebruikt { $used } bytes, budget is { $budget } bytes
storage-no-index-to-evict = Geen index beschikbaar om te verwijderen (alle indexen zijn al schijfgebaseerd)

# =============================================================================
# General Errors
# =============================================================================

storage-not-implemented = Niet geïmplementeerd: { $message }
storage-other = { $message }
