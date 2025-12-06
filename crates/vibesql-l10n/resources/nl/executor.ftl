# VibeSQL Executor Error Messages - Dutch (Nederlands)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = Tabel '{ $name }' niet gevonden
executor-table-already-exists = Tabel '{ $name }' bestaat al

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = Kolom '{ $column_name }' niet gevonden in tabel '{ $table_name }'
executor-column-not-found-searched = Kolom '{ $column_name }' niet gevonden (doorzochte tabellen: { $searched_tables })
executor-column-not-found-with-available = Kolom '{ $column_name }' niet gevonden (doorzochte tabellen: { $searched_tables }). Beschikbare kolommen: { $available_columns }
executor-invalid-table-qualifier = Ongeldige tabelkwalificatie '{ $qualifier }' voor kolom '{ $column }'. Beschikbare tabellen: { $available_tables }
executor-column-already-exists = Kolom '{ $name }' bestaat al
executor-column-index-out-of-bounds = Kolomindex { $index } buiten bereik

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = Index '{ $name }' niet gevonden
executor-index-already-exists = Index '{ $name }' bestaat al
executor-invalid-index-definition = Ongeldige indexdefinitie: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = Trigger '{ $name }' niet gevonden
executor-trigger-already-exists = Trigger '{ $name }' bestaat al

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = Schema '{ $name }' niet gevonden
executor-schema-already-exists = Schema '{ $name }' bestaat al
executor-schema-not-empty = Kan schema '{ $name }' niet verwijderen: schema is niet leeg

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = Rol '{ $name }' niet gevonden
executor-permission-denied = Toegang geweigerd: rol '{ $role }' mist { $privilege } privilege op { $object }
executor-dependent-privileges-exist = Afhankelijke privileges bestaan: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = Type '{ $name }' niet gevonden
executor-type-already-exists = Type '{ $name }' bestaat al
executor-type-in-use = Kan type '{ $name }' niet verwijderen: type is nog in gebruik
executor-type-mismatch = Type komt niet overeen: { $left } { $op } { $right }
executor-type-error = Typefout: { $message }
executor-cast-error = Kan { $from_type } niet converteren naar { $to_type }
executor-type-conversion-error = Kan { $from } niet converteren naar { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = Deling door nul
executor-invalid-where-clause = Ongeldige WHERE-clausule: { $message }
executor-unsupported-expression = Niet-ondersteunde expressie: { $message }
executor-unsupported-feature = Niet-ondersteunde functie: { $message }
executor-parse-error = Parseerfout: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = Scalaire subquery retourneerde { $actual } rijen, verwacht { $expected }
executor-subquery-column-count-mismatch = Subquery retourneerde { $actual } kolommen, verwacht { $expected }
executor-column-count-mismatch = Afgeleide kolommenlijst heeft { $provided } kolommen maar query produceert { $expected } kolommen

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = Constraintschending: { $message }
executor-multiple-primary-keys = Meerdere PRIMARY KEY-constraints zijn niet toegestaan
executor-cannot-drop-column = Kan kolom niet verwijderen: { $message }
executor-constraint-not-found = Constraint '{ $constraint_name }' niet gevonden in tabel '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = Expressiedieptelimiet overschreden: { $depth } > { $max_depth } (voorkomt stack overflow)
executor-query-timeout-exceeded = Querytime-out overschreden: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Rijverwerkingslimiet overschreden: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Geheugenlimiet overschreden: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = Variabele '{ $variable_name }' niet gevonden
executor-variable-not-found-with-available = Variabele '{ $variable_name }' niet gevonden. Beschikbare variabelen: { $available_variables }
executor-label-not-found = Label '{ $name }' niet gevonden

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = Procedurele SELECT INTO moet precies { $expected } rij retourneren, kreeg { $actual } rij{ $plural }
executor-select-into-column-count = Procedurele SELECT INTO kolomaantal komt niet overeen: { $expected } variabele{ $expected_plural } maar query retourneerde { $actual } kolom{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = Procedure '{ $procedure_name }' niet gevonden in schema '{ $schema_name }'
executor-procedure-not-found-with-available = Procedure '{ $procedure_name }' niet gevonden in schema '{ $schema_name }'
    .available = Beschikbare procedures: { $available_procedures }
executor-procedure-not-found-with-suggestion = Procedure '{ $procedure_name }' niet gevonden in schema '{ $schema_name }'
    .available = Beschikbare procedures: { $available_procedures }
    .suggestion = Bedoelde u '{ $suggestion }'?

executor-function-not-found-simple = Functie '{ $function_name }' niet gevonden in schema '{ $schema_name }'
executor-function-not-found-with-available = Functie '{ $function_name }' niet gevonden in schema '{ $schema_name }'
    .available = Beschikbare functies: { $available_functions }
executor-function-not-found-with-suggestion = Functie '{ $function_name }' niet gevonden in schema '{ $schema_name }'
    .available = Beschikbare functies: { $available_functions }
    .suggestion = Bedoelde u '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' verwacht { $expected } parameter{ $expected_plural } ({ $parameter_signature }), kreeg { $actual } argument{ $actual_plural }
executor-parameter-type-mismatch = Parameter '{ $parameter_name }' verwacht { $expected_type }, kreeg { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Argumentaantal komt niet overeen: verwacht { $expected }, kreeg { $actual }

executor-recursion-limit-exceeded = Maximale recursiediepte ({ $max_depth }) overschreden: { $message }
executor-recursion-call-stack = Aanroepstack:
executor-function-must-return = Functie moet een waarde retourneren
executor-invalid-control-flow = Ongeldige controlestroom: { $message }
executor-invalid-function-body = Ongeldige functiebody: { $message }
executor-function-read-only-violation = Functie alleen-lezen schending: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = Kan { $field } niet extraheren uit { $value_type } waarde

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Downcasten van Arrow-array naar { $expected_type } mislukt ({ $context })
executor-columnar-type-mismatch-binary = Incompatibele types voor { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Incompatibel type voor { $operation }: { $left_type }
executor-simd-operation-failed = SIMD { $operation } mislukt: { $reason }
executor-columnar-column-not-found = Kolomindex { $column_index } buiten bereik (batch heeft { $batch_columns } kolommen)
executor-columnar-column-not-found-by-name = Kolom niet gevonden: { $column_name }
executor-columnar-length-mismatch = Kolomlengte komt niet overeen in { $context }: verwacht { $expected }, kreeg { $actual }
executor-unsupported-array-type = Niet-ondersteund arraytype voor { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } verwacht { $expected }, kreeg { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = Cursor '{ $name }' bestaat al
executor-cursor-not-found = Cursor '{ $name }' niet gevonden
executor-cursor-already-open = Cursor '{ $name }' is al geopend
executor-cursor-not-open = Cursor '{ $name }' is niet geopend
executor-cursor-not-scrollable = Cursor '{ $name }' is niet scrollbaar (SCROLL niet opgegeven)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = Opslagfout: { $message }
executor-other = { $message }
