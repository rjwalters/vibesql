# VibeSQL Catalog Error Messages - Italian (it)
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = La tabella '{ $name }' esiste già
catalog-table-not-found = Tabella '{ $table_name }' non trovata

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = La colonna '{ $name }' esiste già
catalog-column-not-found = Colonna '{ $column_name }' non trovata nella tabella '{ $table_name }'

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = Lo schema '{ $name }' esiste già
catalog-schema-not-found = Schema '{ $name }' non trovato
catalog-schema-not-empty = Lo schema '{ $name }' non è vuoto

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = Il ruolo '{ $name }' esiste già
catalog-role-not-found = Ruolo '{ $name }' non trovato

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = Il dominio '{ $name }' esiste già
catalog-domain-not-found = Dominio '{ $name }' non trovato
catalog-domain-in-use = Il dominio '{ $domain_name }' è ancora in uso da { $count } colonna/e: { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = La sequenza '{ $name }' esiste già
catalog-sequence-not-found = Sequenza '{ $name }' non trovata
catalog-sequence-in-use = La sequenza '{ $sequence_name }' è ancora in uso da { $count } colonna/e: { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = Il tipo '{ $name }' esiste già
catalog-type-not-found = Tipo '{ $name }' non trovato
catalog-type-in-use = Il tipo '{ $name }' è ancora in uso da una o più tabelle

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = La collazione '{ $name }' esiste già
catalog-collation-not-found = Collazione '{ $name }' non trovata
catalog-character-set-already-exists = Il set di caratteri '{ $name }' esiste già
catalog-character-set-not-found = Set di caratteri '{ $name }' non trovato
catalog-translation-already-exists = La traduzione '{ $name }' esiste già
catalog-translation-not-found = Traduzione '{ $name }' non trovata

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = La vista '{ $name }' esiste già
catalog-view-not-found = Vista '{ $name }' non trovata
catalog-view-in-use = La vista o tabella '{ $view_name }' è ancora in uso da { $count } vista/e: { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = Il trigger '{ $name }' esiste già
catalog-trigger-not-found = Trigger '{ $name }' non trovato

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = L'asserzione '{ $name }' esiste già
catalog-assertion-not-found = Asserzione '{ $name }' non trovata

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = La funzione '{ $name }' esiste già
catalog-function-not-found = Funzione '{ $name }' non trovata
catalog-procedure-already-exists = La procedura '{ $name }' esiste già
catalog-procedure-not-found = Procedura '{ $name }' non trovata

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = Il vincolo '{ $name }' esiste già
catalog-constraint-not-found = Vincolo '{ $name }' non trovato

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = L'indice '{ $index_name }' sulla tabella '{ $table_name }' esiste già
catalog-index-not-found = Indice '{ $index_name }' sulla tabella '{ $table_name }' non trovato

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = Rilevata dipendenza circolare di chiave esterna per la tabella '{ $table_name }': { $message }
