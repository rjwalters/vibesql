# VibeSQL Catalog Error Messages - Swedish (Svenska)
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = Tabellen '{ $name }' finns redan
catalog-table-not-found = Tabellen '{ $table_name }' hittades inte

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = Kolumnen '{ $name }' finns redan
catalog-column-not-found = Kolumnen '{ $column_name }' hittades inte i tabellen '{ $table_name }'

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = Schemat '{ $name }' finns redan
catalog-schema-not-found = Schemat '{ $name }' hittades inte
catalog-schema-not-empty = Schemat '{ $name }' är inte tomt

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = Rollen '{ $name }' finns redan
catalog-role-not-found = Rollen '{ $name }' hittades inte

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = Domänen '{ $name }' finns redan
catalog-domain-not-found = Domänen '{ $name }' hittades inte
catalog-domain-in-use = Domänen '{ $domain_name }' används fortfarande av { $count } kolumn(er): { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = Sekvensen '{ $name }' finns redan
catalog-sequence-not-found = Sekvensen '{ $name }' hittades inte
catalog-sequence-in-use = Sekvensen '{ $sequence_name }' används fortfarande av { $count } kolumn(er): { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = Typen '{ $name }' finns redan
catalog-type-not-found = Typen '{ $name }' hittades inte
catalog-type-in-use = Typen '{ $name }' används fortfarande av en eller flera tabeller

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = Kollationen '{ $name }' finns redan
catalog-collation-not-found = Kollationen '{ $name }' hittades inte
catalog-character-set-already-exists = Teckenuppsättningen '{ $name }' finns redan
catalog-character-set-not-found = Teckenuppsättningen '{ $name }' hittades inte
catalog-translation-already-exists = Översättningen '{ $name }' finns redan
catalog-translation-not-found = Översättningen '{ $name }' hittades inte

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = Vyn '{ $name }' finns redan
catalog-view-not-found = Vyn '{ $name }' hittades inte
catalog-view-in-use = Vyn eller tabellen '{ $view_name }' används fortfarande av { $count } vy(er): { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = Triggern '{ $name }' finns redan
catalog-trigger-not-found = Triggern '{ $name }' hittades inte

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = Påståendet '{ $name }' finns redan
catalog-assertion-not-found = Påståendet '{ $name }' hittades inte

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = Funktionen '{ $name }' finns redan
catalog-function-not-found = Funktionen '{ $name }' hittades inte
catalog-procedure-already-exists = Proceduren '{ $name }' finns redan
catalog-procedure-not-found = Proceduren '{ $name }' hittades inte

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = Villkoret '{ $name }' finns redan
catalog-constraint-not-found = Villkoret '{ $name }' hittades inte

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = Indexet '{ $index_name }' på tabellen '{ $table_name }' finns redan
catalog-index-not-found = Indexet '{ $index_name }' på tabellen '{ $table_name }' hittades inte

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = Cirkulärt främmandenyckelsberoende upptäckt för tabellen '{ $table_name }': { $message }
