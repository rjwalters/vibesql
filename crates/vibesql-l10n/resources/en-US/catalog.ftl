# VibeSQL Catalog Error Messages - English (US)
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = Table '{ $name }' already exists
catalog-table-not-found = Table '{ $table_name }' not found

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = Column '{ $name }' already exists
catalog-column-not-found = Column '{ $column_name }' not found in table '{ $table_name }'

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = Schema '{ $name }' already exists
catalog-schema-not-found = Schema '{ $name }' not found
catalog-schema-not-empty = Schema '{ $name }' is not empty

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = Role '{ $name }' already exists
catalog-role-not-found = Role '{ $name }' not found

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = Domain '{ $name }' already exists
catalog-domain-not-found = Domain '{ $name }' not found
catalog-domain-in-use = Domain '{ $domain_name }' is still in use by { $count } column(s): { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = Sequence '{ $name }' already exists
catalog-sequence-not-found = Sequence '{ $name }' not found
catalog-sequence-in-use = Sequence '{ $sequence_name }' is still in use by { $count } column(s): { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = Type '{ $name }' already exists
catalog-type-not-found = Type '{ $name }' not found
catalog-type-in-use = Type '{ $name }' is still in use by one or more tables

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = Collation '{ $name }' already exists
catalog-collation-not-found = Collation '{ $name }' not found
catalog-character-set-already-exists = Character set '{ $name }' already exists
catalog-character-set-not-found = Character set '{ $name }' not found
catalog-translation-already-exists = Translation '{ $name }' already exists
catalog-translation-not-found = Translation '{ $name }' not found

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = View '{ $name }' already exists
catalog-view-not-found = View '{ $name }' not found
catalog-view-in-use = View or table '{ $view_name }' is still in use by { $count } view(s): { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = Trigger '{ $name }' already exists
catalog-trigger-not-found = Trigger '{ $name }' not found

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = Assertion '{ $name }' already exists
catalog-assertion-not-found = Assertion '{ $name }' not found

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = Function '{ $name }' already exists
catalog-function-not-found = Function '{ $name }' not found
catalog-procedure-already-exists = Procedure '{ $name }' already exists
catalog-procedure-not-found = Procedure '{ $name }' not found

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = Constraint '{ $name }' already exists
catalog-constraint-not-found = Constraint '{ $name }' not found

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = Index '{ $index_name }' on table '{ $table_name }' already exists
catalog-index-not-found = Index '{ $index_name }' on table '{ $table_name }' not found

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = Circular foreign key dependency detected for table '{ $table_name }': { $message }
