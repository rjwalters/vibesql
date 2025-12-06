# VibeSQL Catalog Error Messages - Polski (Polish)
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = Tabela '{ $name }' już istnieje
catalog-table-not-found = Nie znaleziono tabeli '{ $table_name }'

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = Kolumna '{ $name }' już istnieje
catalog-column-not-found = Nie znaleziono kolumny '{ $column_name }' w tabeli '{ $table_name }'

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = Schemat '{ $name }' już istnieje
catalog-schema-not-found = Nie znaleziono schematu '{ $name }'
catalog-schema-not-empty = Schemat '{ $name }' nie jest pusty

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = Rola '{ $name }' już istnieje
catalog-role-not-found = Nie znaleziono roli '{ $name }'

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = Domena '{ $name }' już istnieje
catalog-domain-not-found = Nie znaleziono domeny '{ $name }'
catalog-domain-in-use = Domena '{ $domain_name }' jest nadal używana przez { $count } kolumn(y): { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = Sekwencja '{ $name }' już istnieje
catalog-sequence-not-found = Nie znaleziono sekwencji '{ $name }'
catalog-sequence-in-use = Sekwencja '{ $sequence_name }' jest nadal używana przez { $count } kolumn(y): { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = Typ '{ $name }' już istnieje
catalog-type-not-found = Nie znaleziono typu '{ $name }'
catalog-type-in-use = Typ '{ $name }' jest nadal używany przez jedną lub więcej tabel

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = Zestawienie '{ $name }' już istnieje
catalog-collation-not-found = Nie znaleziono zestawienia '{ $name }'
catalog-character-set-already-exists = Zestaw znaków '{ $name }' już istnieje
catalog-character-set-not-found = Nie znaleziono zestawu znaków '{ $name }'
catalog-translation-already-exists = Translacja '{ $name }' już istnieje
catalog-translation-not-found = Nie znaleziono translacji '{ $name }'

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = Widok '{ $name }' już istnieje
catalog-view-not-found = Nie znaleziono widoku '{ $name }'
catalog-view-in-use = Widok lub tabela '{ $view_name }' jest nadal używany przez { $count } widok(ów): { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = Wyzwalacz '{ $name }' już istnieje
catalog-trigger-not-found = Nie znaleziono wyzwalacza '{ $name }'

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = Asercja '{ $name }' już istnieje
catalog-assertion-not-found = Nie znaleziono asercji '{ $name }'

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = Funkcja '{ $name }' już istnieje
catalog-function-not-found = Nie znaleziono funkcji '{ $name }'
catalog-procedure-already-exists = Procedura '{ $name }' już istnieje
catalog-procedure-not-found = Nie znaleziono procedury '{ $name }'

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = Ograniczenie '{ $name }' już istnieje
catalog-constraint-not-found = Nie znaleziono ograniczenia '{ $name }'

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = Indeks '{ $index_name }' na tabeli '{ $table_name }' już istnieje
catalog-index-not-found = Nie znaleziono indeksu '{ $index_name }' na tabeli '{ $table_name }'

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = Wykryto cykliczną zależność klucza obcego dla tabeli '{ $table_name }': { $message }
