# VibeSQL Executor Error Messages - Italian (it)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = Tabella '{ $name }' non trovata
executor-table-already-exists = La tabella '{ $name }' esiste già

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = Colonna '{ $column_name }' non trovata nella tabella '{ $table_name }'
executor-column-not-found-searched = Colonna '{ $column_name }' non trovata (tabelle cercate: { $searched_tables })
executor-column-not-found-with-available = Colonna '{ $column_name }' non trovata (tabelle cercate: { $searched_tables }). Colonne disponibili: { $available_columns }
executor-invalid-table-qualifier = Qualificatore di tabella '{ $qualifier }' non valido per la colonna '{ $column }'. Tabelle disponibili: { $available_tables }
executor-column-already-exists = La colonna '{ $name }' esiste già
executor-column-index-out-of-bounds = Indice colonna { $index } fuori dai limiti

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = Indice '{ $name }' non trovato
executor-index-already-exists = L'indice '{ $name }' esiste già
executor-invalid-index-definition = Definizione indice non valida: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = Trigger '{ $name }' non trovato
executor-trigger-already-exists = Il trigger '{ $name }' esiste già

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = Schema '{ $name }' non trovato
executor-schema-already-exists = Lo schema '{ $name }' esiste già
executor-schema-not-empty = Impossibile eliminare lo schema '{ $name }': lo schema non è vuoto

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = Ruolo '{ $name }' non trovato
executor-permission-denied = Permesso negato: il ruolo '{ $role }' non ha il privilegio { $privilege } su { $object }
executor-dependent-privileges-exist = Esistono privilegi dipendenti: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = Tipo '{ $name }' non trovato
executor-type-already-exists = Il tipo '{ $name }' esiste già
executor-type-in-use = Impossibile eliminare il tipo '{ $name }': il tipo è ancora in uso
executor-type-mismatch = Tipo non corrispondente: { $left } { $op } { $right }
executor-type-error = Errore di tipo: { $message }
executor-cast-error = Impossibile convertire { $from_type } in { $to_type }
executor-type-conversion-error = Impossibile convertire { $from } in { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = Divisione per zero
executor-invalid-where-clause = Clausola WHERE non valida: { $message }
executor-unsupported-expression = Espressione non supportata: { $message }
executor-unsupported-feature = Funzionalità non supportata: { $message }
executor-parse-error = Errore di parsing: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = La subquery scalare ha restituito { $actual } righe, prevista { $expected }
executor-subquery-column-count-mismatch = La subquery ha restituito { $actual } colonne, previste { $expected }
executor-column-count-mismatch = La lista colonne derivata ha { $provided } colonne ma la query produce { $expected } colonne

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = Violazione del vincolo: { $message }
executor-multiple-primary-keys = Non sono ammessi vincoli PRIMARY KEY multipli
executor-cannot-drop-column = Impossibile eliminare la colonna: { $message }
executor-constraint-not-found = Vincolo '{ $constraint_name }' non trovato nella tabella '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = Limite di profondità espressione superato: { $depth } > { $max_depth } (previene overflow dello stack)
executor-query-timeout-exceeded = Timeout query superato: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Limite di elaborazione righe superato: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Limite di memoria superato: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = Variabile '{ $variable_name }' non trovata
executor-variable-not-found-with-available = Variabile '{ $variable_name }' non trovata. Variabili disponibili: { $available_variables }
executor-label-not-found = Etichetta '{ $name }' non trovata

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = SELECT INTO procedurale deve restituire esattamente { $expected } riga, ottenute { $actual } righ{ $plural }
executor-select-into-column-count = Numero colonne SELECT INTO procedurale non corrispondente: { $expected } variabil{ $expected_plural } ma la query ha restituito { $actual } colonn{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = Procedura '{ $procedure_name }' non trovata nello schema '{ $schema_name }'
executor-procedure-not-found-with-available = Procedura '{ $procedure_name }' non trovata nello schema '{ $schema_name }'
    .available = Procedure disponibili: { $available_procedures }
executor-procedure-not-found-with-suggestion = Procedura '{ $procedure_name }' non trovata nello schema '{ $schema_name }'
    .available = Procedure disponibili: { $available_procedures }
    .suggestion = Forse intendevi '{ $suggestion }'?

executor-function-not-found-simple = Funzione '{ $function_name }' non trovata nello schema '{ $schema_name }'
executor-function-not-found-with-available = Funzione '{ $function_name }' non trovata nello schema '{ $schema_name }'
    .available = Funzioni disponibili: { $available_functions }
executor-function-not-found-with-suggestion = Funzione '{ $function_name }' non trovata nello schema '{ $schema_name }'
    .available = Funzioni disponibili: { $available_functions }
    .suggestion = Forse intendevi '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' richiede { $expected } parametr{ $expected_plural } ({ $parameter_signature }), ricevuti { $actual } argoment{ $actual_plural }
executor-parameter-type-mismatch = Il parametro '{ $parameter_name }' richiede { $expected_type }, ricevuto { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Numero argomenti non corrispondente: previsti { $expected }, ricevuti { $actual }

executor-recursion-limit-exceeded = Profondità massima di ricorsione ({ $max_depth }) superata: { $message }
executor-recursion-call-stack = Stack delle chiamate:
executor-function-must-return = La funzione deve restituire un valore
executor-invalid-control-flow = Flusso di controllo non valido: { $message }
executor-invalid-function-body = Corpo della funzione non valido: { $message }
executor-function-read-only-violation = Violazione sola lettura della funzione: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = Impossibile estrarre { $field } da valore di tipo { $value_type }

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Impossibile eseguire downcast dell'array Arrow a { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = Tipi incompatibili per { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Tipo incompatibile per { $operation }: { $left_type }
executor-simd-operation-failed = Operazione SIMD { $operation } fallita: { $reason }
executor-columnar-column-not-found = Indice colonna { $column_index } fuori dai limiti (il batch ha { $batch_columns } colonne)
executor-columnar-column-not-found-by-name = Colonna non trovata: { $column_name }
executor-columnar-length-mismatch = Lunghezza colonna non corrispondente in { $context }: prevista { $expected }, ottenuta { $actual }
executor-unsupported-array-type = Tipo array non supportato per { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } richiede { $expected }, ricevuto { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = Il cursore '{ $name }' esiste già
executor-cursor-not-found = Cursore '{ $name }' non trovato
executor-cursor-already-open = Il cursore '{ $name }' è già aperto
executor-cursor-not-open = Il cursore '{ $name }' non è aperto
executor-cursor-not-scrollable = Il cursore '{ $name }' non è scorrevole (SCROLL non specificato)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = Errore di archiviazione: { $message }
executor-other = { $message }
