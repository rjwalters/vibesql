# VibeSQL Catalog Error Messages - Dutch (Nederlands)
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = Tabel '{ $name }' bestaat al
catalog-table-not-found = Tabel '{ $table_name }' niet gevonden

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = Kolom '{ $name }' bestaat al
catalog-column-not-found = Kolom '{ $column_name }' niet gevonden in tabel '{ $table_name }'

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = Schema '{ $name }' bestaat al
catalog-schema-not-found = Schema '{ $name }' niet gevonden
catalog-schema-not-empty = Schema '{ $name }' is niet leeg

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = Rol '{ $name }' bestaat al
catalog-role-not-found = Rol '{ $name }' niet gevonden

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = Domein '{ $name }' bestaat al
catalog-domain-not-found = Domein '{ $name }' niet gevonden
catalog-domain-in-use = Domein '{ $domain_name }' is nog in gebruik door { $count } kolom(men): { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = Sequentie '{ $name }' bestaat al
catalog-sequence-not-found = Sequentie '{ $name }' niet gevonden
catalog-sequence-in-use = Sequentie '{ $sequence_name }' is nog in gebruik door { $count } kolom(men): { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = Type '{ $name }' bestaat al
catalog-type-not-found = Type '{ $name }' niet gevonden
catalog-type-in-use = Type '{ $name }' is nog in gebruik door een of meer tabellen

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = Collatie '{ $name }' bestaat al
catalog-collation-not-found = Collatie '{ $name }' niet gevonden
catalog-character-set-already-exists = Tekenset '{ $name }' bestaat al
catalog-character-set-not-found = Tekenset '{ $name }' niet gevonden
catalog-translation-already-exists = Vertaling '{ $name }' bestaat al
catalog-translation-not-found = Vertaling '{ $name }' niet gevonden

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = View '{ $name }' bestaat al
catalog-view-not-found = View '{ $name }' niet gevonden
catalog-view-in-use = View of tabel '{ $view_name }' is nog in gebruik door { $count } view(s): { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = Trigger '{ $name }' bestaat al
catalog-trigger-not-found = Trigger '{ $name }' niet gevonden

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = Assertie '{ $name }' bestaat al
catalog-assertion-not-found = Assertie '{ $name }' niet gevonden

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = Functie '{ $name }' bestaat al
catalog-function-not-found = Functie '{ $name }' niet gevonden
catalog-procedure-already-exists = Procedure '{ $name }' bestaat al
catalog-procedure-not-found = Procedure '{ $name }' niet gevonden

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = Constraint '{ $name }' bestaat al
catalog-constraint-not-found = Constraint '{ $name }' niet gevonden

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = Index '{ $index_name }' op tabel '{ $table_name }' bestaat al
catalog-index-not-found = Index '{ $index_name }' op tabel '{ $table_name }' niet gevonden

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = Circulaire foreign key-afhankelijkheid gedetecteerd voor tabel '{ $table_name }': { $message }
