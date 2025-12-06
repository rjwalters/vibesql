# VibeSQL Storage Error Messages - Italian (it)
# This file contains all error messages for the vibesql-storage crate.

# =============================================================================
# Table Errors
# =============================================================================

storage-table-not-found = Tabella '{ $name }' non trovata

# =============================================================================
# Column Errors
# =============================================================================

storage-column-count-mismatch = Numero colonne non corrispondente: previste { $expected }, ottenute { $actual }
storage-column-index-out-of-bounds = Indice colonna { $index } fuori dai limiti
storage-column-not-found = Colonna '{ $column_name }' non trovata nella tabella '{ $table_name }'

# =============================================================================
# Index Errors
# =============================================================================

storage-index-already-exists = L'indice '{ $name }' esiste già
storage-index-not-found = Indice '{ $name }' non trovato
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint Errors
# =============================================================================

storage-null-constraint-violation = Violazione vincolo NOT NULL: la colonna '{ $column }' non può essere NULL
storage-unique-constraint-violation = { $message }

# =============================================================================
# Type Errors
# =============================================================================

storage-type-mismatch = Tipo non corrispondente nella colonna '{ $column }': previsto { $expected }, ottenuto { $actual }

# =============================================================================
# Transaction and Catalog Errors
# =============================================================================

storage-catalog-error = Errore del catalogo: { $message }
storage-transaction-error = Errore della transazione: { $message }
storage-row-not-found = Riga non trovata

# =============================================================================
# I/O and Page Errors
# =============================================================================

storage-io-error = Errore I/O: { $message }
storage-invalid-page-size = Dimensione pagina non valida: prevista { $expected }, ottenuta { $actual }
storage-invalid-page-id = ID pagina non valido: { $page_id }
storage-lock-error = Errore di lock: { $message }

# =============================================================================
# Memory Errors
# =============================================================================

storage-memory-budget-exceeded = Budget di memoria superato: in uso { $used } byte, budget di { $budget } byte
storage-no-index-to-evict = Nessun indice disponibile per l'eviction (tutti gli indici sono già su disco)

# =============================================================================
# General Errors
# =============================================================================

storage-not-implemented = Non implementato: { $message }
storage-other = { $message }
