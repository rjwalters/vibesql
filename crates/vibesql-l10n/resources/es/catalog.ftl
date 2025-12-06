# VibeSQL Catalog Mensajes de Error - Español
# This file contains all error messages for the vibesql-catalog crate.

# =============================================================================
# Table Errors
# =============================================================================

catalog-table-already-exists = La tabla '{ $name }' ya existe
catalog-table-not-found = Tabla '{ $table_name }' no encontrada

# =============================================================================
# Column Errors
# =============================================================================

catalog-column-already-exists = La columna '{ $name }' ya existe
catalog-column-not-found = Columna '{ $column_name }' no encontrada en la tabla '{ $table_name }'

# =============================================================================
# Schema Errors
# =============================================================================

catalog-schema-already-exists = El esquema '{ $name }' ya existe
catalog-schema-not-found = Esquema '{ $name }' no encontrado
catalog-schema-not-empty = El esquema '{ $name }' no está vacío

# =============================================================================
# Role Errors
# =============================================================================

catalog-role-already-exists = El rol '{ $name }' ya existe
catalog-role-not-found = Rol '{ $name }' no encontrado

# =============================================================================
# Domain Errors
# =============================================================================

catalog-domain-already-exists = El dominio '{ $name }' ya existe
catalog-domain-not-found = Dominio '{ $name }' no encontrado
catalog-domain-in-use = El dominio '{ $domain_name }' todavía está en uso por { $count } columna(s): { $columns }

# =============================================================================
# Sequence Errors
# =============================================================================

catalog-sequence-already-exists = La secuencia '{ $name }' ya existe
catalog-sequence-not-found = Secuencia '{ $name }' no encontrada
catalog-sequence-in-use = La secuencia '{ $sequence_name }' todavía está en uso por { $count } columna(s): { $columns }

# =============================================================================
# Type Errors
# =============================================================================

catalog-type-already-exists = El tipo '{ $name }' ya existe
catalog-type-not-found = Tipo '{ $name }' no encontrado
catalog-type-in-use = El tipo '{ $name }' todavía está en uso por una o más tablas

# =============================================================================
# Collation and Character Set Errors
# =============================================================================

catalog-collation-already-exists = La collation '{ $name }' ya existe
catalog-collation-not-found = Collation '{ $name }' no encontrada
catalog-character-set-already-exists = El conjunto de caracteres '{ $name }' ya existe
catalog-character-set-not-found = Conjunto de caracteres '{ $name }' no encontrado
catalog-translation-already-exists = La traducción '{ $name }' ya existe
catalog-translation-not-found = Traducción '{ $name }' no encontrada

# =============================================================================
# View Errors
# =============================================================================

catalog-view-already-exists = La vista '{ $name }' ya existe
catalog-view-not-found = Vista '{ $name }' no encontrada
catalog-view-in-use = La vista o tabla '{ $view_name }' todavía está en uso por { $count } vista(s): { $views }

# =============================================================================
# Trigger Errors
# =============================================================================

catalog-trigger-already-exists = El trigger '{ $name }' ya existe
catalog-trigger-not-found = Trigger '{ $name }' no encontrado

# =============================================================================
# Assertion Errors
# =============================================================================

catalog-assertion-already-exists = La aserción '{ $name }' ya existe
catalog-assertion-not-found = Aserción '{ $name }' no encontrada

# =============================================================================
# Function and Procedure Errors
# =============================================================================

catalog-function-already-exists = La función '{ $name }' ya existe
catalog-function-not-found = Función '{ $name }' no encontrada
catalog-procedure-already-exists = El procedimiento '{ $name }' ya existe
catalog-procedure-not-found = Procedimiento '{ $name }' no encontrado

# =============================================================================
# Constraint Errors
# =============================================================================

catalog-constraint-already-exists = La restricción '{ $name }' ya existe
catalog-constraint-not-found = Restricción '{ $name }' no encontrada

# =============================================================================
# Index Errors
# =============================================================================

catalog-index-already-exists = El índice '{ $index_name }' en la tabla '{ $table_name }' ya existe
catalog-index-not-found = Índice '{ $index_name }' en la tabla '{ $table_name }' no encontrado

# =============================================================================
# Foreign Key Errors
# =============================================================================

catalog-circular-foreign-key = Se detectó dependencia circular de clave foránea para la tabla '{ $table_name }': { $message }
