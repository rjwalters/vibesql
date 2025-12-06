# VibeSQL Executor Error Messages - Swedish (Svenska)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = Tabellen '{ $name }' hittades inte
executor-table-already-exists = Tabellen '{ $name }' finns redan

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = Kolumnen '{ $column_name }' hittades inte i tabellen '{ $table_name }'
executor-column-not-found-searched = Kolumnen '{ $column_name }' hittades inte (sökta tabeller: { $searched_tables })
executor-column-not-found-with-available = Kolumnen '{ $column_name }' hittades inte (sökta tabeller: { $searched_tables }). Tillgängliga kolumner: { $available_columns }
executor-invalid-table-qualifier = Ogiltig tabellkvalificerare '{ $qualifier }' för kolumn '{ $column }'. Tillgängliga tabeller: { $available_tables }
executor-column-already-exists = Kolumnen '{ $name }' finns redan
executor-column-index-out-of-bounds = Kolumnindex { $index } utanför gränserna

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = Indexet '{ $name }' hittades inte
executor-index-already-exists = Indexet '{ $name }' finns redan
executor-invalid-index-definition = Ogiltig indexdefinition: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = Triggern '{ $name }' hittades inte
executor-trigger-already-exists = Triggern '{ $name }' finns redan

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = Schemat '{ $name }' hittades inte
executor-schema-already-exists = Schemat '{ $name }' finns redan
executor-schema-not-empty = Kan inte ta bort schemat '{ $name }': schemat är inte tomt

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = Rollen '{ $name }' hittades inte
executor-permission-denied = Åtkomst nekad: rollen '{ $role }' saknar { $privilege }-behörighet på { $object }
executor-dependent-privileges-exist = Beroende behörigheter finns: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = Typen '{ $name }' hittades inte
executor-type-already-exists = Typen '{ $name }' finns redan
executor-type-in-use = Kan inte ta bort typen '{ $name }': typen används fortfarande
executor-type-mismatch = Typmatchningsfel: { $left } { $op } { $right }
executor-type-error = Typfel: { $message }
executor-cast-error = Kan inte konvertera { $from_type } till { $to_type }
executor-type-conversion-error = Kan inte konvertera { $from } till { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = Division med noll
executor-invalid-where-clause = Ogiltig WHERE-sats: { $message }
executor-unsupported-expression = Uttryck stöds inte: { $message }
executor-unsupported-feature = Funktion stöds inte: { $message }
executor-parse-error = Tolkningsfel: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = Skalär underfråga returnerade { $actual } rader, förväntade { $expected }
executor-subquery-column-count-mismatch = Underfråga returnerade { $actual } kolumner, förväntade { $expected }
executor-column-count-mismatch = Härledd kolumnlista har { $provided } kolumner men frågan producerar { $expected } kolumner

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = Villkorsöverträdelse: { $message }
executor-multiple-primary-keys = Flera PRIMARY KEY-villkor är inte tillåtna
executor-cannot-drop-column = Kan inte ta bort kolumn: { $message }
executor-constraint-not-found = Villkoret '{ $constraint_name }' hittades inte i tabellen '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = Uttrycksdjupgräns överskriden: { $depth } > { $max_depth } (förhindrar stackspill)
executor-query-timeout-exceeded = Frågetimeout överskriden: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Radbehandlingsgräns överskriden: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Minnesgräns överskriden: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = Variabeln '{ $variable_name }' hittades inte
executor-variable-not-found-with-available = Variabeln '{ $variable_name }' hittades inte. Tillgängliga variabler: { $available_variables }
executor-label-not-found = Etiketten '{ $name }' hittades inte

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = Procedural SELECT INTO måste returnera exakt { $expected } rad, fick { $actual } rad{ $plural }
executor-select-into-column-count = Procedural SELECT INTO kolumnantal matchar inte: { $expected } variabel{ $expected_plural } men frågan returnerade { $actual } kolumn{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = Proceduren '{ $procedure_name }' hittades inte i schemat '{ $schema_name }'
executor-procedure-not-found-with-available = Proceduren '{ $procedure_name }' hittades inte i schemat '{ $schema_name }'
    .available = Tillgängliga procedurer: { $available_procedures }
executor-procedure-not-found-with-suggestion = Proceduren '{ $procedure_name }' hittades inte i schemat '{ $schema_name }'
    .available = Tillgängliga procedurer: { $available_procedures }
    .suggestion = Menade du '{ $suggestion }'?

executor-function-not-found-simple = Funktionen '{ $function_name }' hittades inte i schemat '{ $schema_name }'
executor-function-not-found-with-available = Funktionen '{ $function_name }' hittades inte i schemat '{ $schema_name }'
    .available = Tillgängliga funktioner: { $available_functions }
executor-function-not-found-with-suggestion = Funktionen '{ $function_name }' hittades inte i schemat '{ $schema_name }'
    .available = Tillgängliga funktioner: { $available_functions }
    .suggestion = Menade du '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' förväntar { $expected } parameter{ $expected_plural } ({ $parameter_signature }), fick { $actual } argument{ $actual_plural }
executor-parameter-type-mismatch = Parametern '{ $parameter_name }' förväntar { $expected_type }, fick { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Argumentantal matchar inte: förväntade { $expected }, fick { $actual }

executor-recursion-limit-exceeded = Maximalt rekursionsdjup ({ $max_depth }) överskridet: { $message }
executor-recursion-call-stack = Anropsstack:
executor-function-must-return = Funktionen måste returnera ett värde
executor-invalid-control-flow = Ogiltigt kontrollflöde: { $message }
executor-invalid-function-body = Ogiltig funktionskropp: { $message }
executor-function-read-only-violation = Funktionens skrivskyddsöverträdelse: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = Kan inte extrahera { $field } från { $value_type }-värde

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Kunde inte konvertera Arrow-array till { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = Inkompatibla typer för { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Inkompatibel typ för { $operation }: { $left_type }
executor-simd-operation-failed = SIMD { $operation } misslyckades: { $reason }
executor-columnar-column-not-found = Kolumnindex { $column_index } utanför gränserna (batchen har { $batch_columns } kolumner)
executor-columnar-column-not-found-by-name = Kolumnen hittades inte: { $column_name }
executor-columnar-length-mismatch = Kolumnlängd matchar inte i { $context }: förväntade { $expected }, fick { $actual }
executor-unsupported-array-type = Arraytyp stöds inte för { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } förväntar { $expected }, fick { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = Markören '{ $name }' finns redan
executor-cursor-not-found = Markören '{ $name }' hittades inte
executor-cursor-already-open = Markören '{ $name }' är redan öppen
executor-cursor-not-open = Markören '{ $name }' är inte öppen
executor-cursor-not-scrollable = Markören '{ $name }' är inte rullbar (SCROLL ej angiven)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = Lagringsfel: { $message }
executor-other = { $message }
