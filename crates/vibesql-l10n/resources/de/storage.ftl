# VibeSQL Storage-Fehlermeldungen - Deutsch
# Diese Datei enthält alle Fehlermeldungen für das vibesql-storage-Crate.

# =============================================================================
# Tabellenfehler
# =============================================================================

storage-table-not-found = Tabelle '{ $name }' nicht gefunden

# =============================================================================
# Spaltenfehler
# =============================================================================

storage-column-count-mismatch = Spaltenanzahl-Konflikt: erwartet { $expected }, erhielt { $actual }
storage-column-index-out-of-bounds = Spaltenindex { $index } außerhalb des gültigen Bereichs
storage-column-not-found = Spalte '{ $column_name }' in Tabelle '{ $table_name }' nicht gefunden

# =============================================================================
# Indexfehler
# =============================================================================

storage-index-already-exists = Index '{ $name }' existiert bereits
storage-index-not-found = Index '{ $name }' nicht gefunden
storage-invalid-index-column = { $message }

# =============================================================================
# Constraint-Fehler
# =============================================================================

storage-null-constraint-violation = NOT NULL-Constraint-Verletzung: Spalte '{ $column }' darf nicht NULL sein
storage-unique-constraint-violation = { $message }

# =============================================================================
# Typfehler
# =============================================================================

storage-type-mismatch = Typenkonflikt in Spalte '{ $column }': erwartet { $expected }, erhielt { $actual }

# =============================================================================
# Transaktions- und Katalogfehler
# =============================================================================

storage-catalog-error = Katalogfehler: { $message }
storage-transaction-error = Transaktionsfehler: { $message }
storage-row-not-found = Zeile nicht gefunden

# =============================================================================
# E/A- und Seitenfehler
# =============================================================================

storage-io-error = E/A-Fehler: { $message }
storage-invalid-page-size = Ungültige Seitengröße: erwartet { $expected }, erhielt { $actual }
storage-invalid-page-id = Ungültige Seiten-ID: { $page_id }
storage-lock-error = Sperrfehler: { $message }

# =============================================================================
# Speicherfehler
# =============================================================================

storage-memory-budget-exceeded = Speicherbudget überschritten: verwendet { $used } Bytes, Budget ist { $budget } Bytes
storage-no-index-to-evict = Kein Index zum Verdrängen verfügbar (alle Indizes sind bereits festplattenbasiert)

# =============================================================================
# Allgemeine Fehler
# =============================================================================

storage-not-implemented = Nicht implementiert: { $message }
storage-other = { $message }
