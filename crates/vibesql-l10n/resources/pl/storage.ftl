# VibeSQL Storage Error Messages - Polski (Polish)
# This file contains all error messages for the vibesql-storage crate.

# =============================================================================
# Table Errors
# =============================================================================

storage-table-not-found = Nie znaleziono tabeli '{ $name }'

# =============================================================================
# Column Errors
# =============================================================================

storage-column-count-mismatch = Niezgodność liczby kolumn: oczekiwano { $expected }, otrzymano { $actual }
storage-column-index-out-of-bounds = Indeks kolumny { $index } poza zakresem
storage-column-not-found = Nie znaleziono kolumny '{ $column_name }' w tabeli '{ $table_name }'

# =============================================================================
# Index Errors
# =============================================================================

storage-index-already-exists = Indeks '{ $name }' już istnieje
storage-index-not-found = Nie znaleziono indeksu '{ $name }'
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint Errors
# =============================================================================

storage-null-constraint-violation = Naruszenie ograniczenia NOT NULL: kolumna '{ $column }' nie może być NULL
storage-unique-constraint-violation = { $message }

# =============================================================================
# Type Errors
# =============================================================================

storage-type-mismatch = Niezgodność typu w kolumnie '{ $column }': oczekiwano { $expected }, otrzymano { $actual }

# =============================================================================
# Transaction and Catalog Errors
# =============================================================================

storage-catalog-error = Błąd katalogu: { $message }
storage-transaction-error = Błąd transakcji: { $message }
storage-row-not-found = Nie znaleziono wiersza

# =============================================================================
# I/O and Page Errors
# =============================================================================

storage-io-error = Błąd I/O: { $message }
storage-invalid-page-size = Nieprawidłowy rozmiar strony: oczekiwano { $expected }, otrzymano { $actual }
storage-invalid-page-id = Nieprawidłowy identyfikator strony: { $page_id }
storage-lock-error = Błąd blokady: { $message }

# =============================================================================
# Memory Errors
# =============================================================================

storage-memory-budget-exceeded = Przekroczono budżet pamięci: używane { $used } bajtów, budżet wynosi { $budget } bajtów
storage-no-index-to-evict = Brak indeksu do usunięcia (wszystkie indeksy są już oparte na dysku)

# =============================================================================
# General Errors
# =============================================================================

storage-not-implemented = Nie zaimplementowano: { $message }
storage-other = { $message }
