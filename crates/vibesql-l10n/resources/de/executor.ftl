# VibeSQL Executor-Fehlermeldungen - Deutsch
# Diese Datei enthält alle Fehlermeldungen für das vibesql-executor-Crate.

# =============================================================================
# Tabellenfehler
# =============================================================================

executor-table-not-found = Tabelle '{ $name }' nicht gefunden
executor-table-already-exists = Tabelle '{ $name }' existiert bereits

# =============================================================================
# Spaltenfehler
# =============================================================================

executor-column-not-found-simple = Spalte '{ $column_name }' in Tabelle '{ $table_name }' nicht gefunden
executor-column-not-found-searched = Spalte '{ $column_name }' nicht gefunden (durchsuchte Tabellen: { $searched_tables })
executor-column-not-found-with-available = Spalte '{ $column_name }' nicht gefunden (durchsuchte Tabellen: { $searched_tables }). Verfügbare Spalten: { $available_columns }
executor-invalid-table-qualifier = Ungültiger Tabellenqualifizierer '{ $qualifier }' für Spalte '{ $column }'. Verfügbare Tabellen: { $available_tables }
executor-column-already-exists = Spalte '{ $name }' existiert bereits
executor-column-index-out-of-bounds = Spaltenindex { $index } außerhalb des gültigen Bereichs

# =============================================================================
# Indexfehler
# =============================================================================

executor-index-not-found = Index '{ $name }' nicht gefunden
executor-index-already-exists = Index '{ $name }' existiert bereits
executor-invalid-index-definition = Ungültige Indexdefinition: { $message }

# =============================================================================
# Trigger-Fehler
# =============================================================================

executor-trigger-not-found = Trigger '{ $name }' nicht gefunden
executor-trigger-already-exists = Trigger '{ $name }' existiert bereits

# =============================================================================
# Schema-Fehler
# =============================================================================

executor-schema-not-found = Schema '{ $name }' nicht gefunden
executor-schema-already-exists = Schema '{ $name }' existiert bereits
executor-schema-not-empty = Schema '{ $name }' kann nicht gelöscht werden: Schema ist nicht leer

# =============================================================================
# Rollen- und Berechtigungsfehler
# =============================================================================

executor-role-not-found = Rolle '{ $name }' nicht gefunden
executor-permission-denied = Berechtigung verweigert: Rolle '{ $role }' fehlt { $privilege }-Berechtigung auf { $object }
executor-dependent-privileges-exist = Abhängige Berechtigungen existieren: { $message }

# =============================================================================
# Typfehler
# =============================================================================

executor-type-not-found = Typ '{ $name }' nicht gefunden
executor-type-already-exists = Typ '{ $name }' existiert bereits
executor-type-in-use = Typ '{ $name }' kann nicht gelöscht werden: Typ wird noch verwendet
executor-type-mismatch = Typenkonflikt: { $left } { $op } { $right }
executor-type-error = Typfehler: { $message }
executor-cast-error = Konvertierung von { $from_type } nach { $to_type } nicht möglich
executor-type-conversion-error = Konvertierung von { $from } nach { $to } nicht möglich

# =============================================================================
# Ausdrucks- und Abfragefehler
# =============================================================================

executor-division-by-zero = Division durch Null
executor-invalid-where-clause = Ungültige WHERE-Klausel: { $message }
executor-unsupported-expression = Nicht unterstützter Ausdruck: { $message }
executor-unsupported-feature = Nicht unterstützte Funktion: { $message }
executor-parse-error = Parserfehler: { $message }

# =============================================================================
# Unterabfragefehler
# =============================================================================

executor-subquery-returned-multiple-rows = Skalare Unterabfrage lieferte { $actual } Zeilen, erwartet wurden { $expected }
executor-subquery-column-count-mismatch = Unterabfrage lieferte { $actual } Spalten, erwartet wurden { $expected }
executor-column-count-mismatch = Abgeleitete Spaltenliste hat { $provided } Spalten, aber Abfrage erzeugt { $expected } Spalten

# =============================================================================
# Constraint-Fehler
# =============================================================================

executor-constraint-violation = Constraint-Verletzung: { $message }
executor-multiple-primary-keys = Mehrere PRIMARY KEY-Constraints sind nicht erlaubt
executor-cannot-drop-column = Spalte kann nicht gelöscht werden: { $message }
executor-constraint-not-found = Constraint '{ $constraint_name }' in Tabelle '{ $table_name }' nicht gefunden

# =============================================================================
# Ressourcenlimit-Fehler
# =============================================================================

executor-expression-depth-exceeded = Ausdruckstiefenlimit überschritten: { $depth } > { $max_depth } (verhindert Stack-Überlauf)
executor-query-timeout-exceeded = Abfrage-Timeout überschritten: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Zeilenverarbeitungslimit überschritten: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Speicherlimit überschritten: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Prozedurale/Variablen-Fehler
# =============================================================================

executor-variable-not-found-simple = Variable '{ $variable_name }' nicht gefunden
executor-variable-not-found-with-available = Variable '{ $variable_name }' nicht gefunden. Verfügbare Variablen: { $available_variables }
executor-label-not-found = Label '{ $name }' nicht gefunden

# =============================================================================
# SELECT INTO-Fehler
# =============================================================================

executor-select-into-row-count = Prozedurales SELECT INTO muss genau { $expected } Zeile zurückgeben, erhielt { $actual } Zeile{ $plural }
executor-select-into-column-count = Prozedurales SELECT INTO Spaltenanzahl-Konflikt: { $expected } Variable{ $expected_plural }, aber Abfrage lieferte { $actual } Spalte{ $actual_plural }

# =============================================================================
# Prozedur- und Funktionsfehler
# =============================================================================

executor-procedure-not-found-simple = Prozedur '{ $procedure_name }' im Schema '{ $schema_name }' nicht gefunden
executor-procedure-not-found-with-available = Prozedur '{ $procedure_name }' im Schema '{ $schema_name }' nicht gefunden
    .available = Verfügbare Prozeduren: { $available_procedures }
executor-procedure-not-found-with-suggestion = Prozedur '{ $procedure_name }' im Schema '{ $schema_name }' nicht gefunden
    .available = Verfügbare Prozeduren: { $available_procedures }
    .suggestion = Meinten Sie '{ $suggestion }'?

executor-function-not-found-simple = Funktion '{ $function_name }' im Schema '{ $schema_name }' nicht gefunden
executor-function-not-found-with-available = Funktion '{ $function_name }' im Schema '{ $schema_name }' nicht gefunden
    .available = Verfügbare Funktionen: { $available_functions }
executor-function-not-found-with-suggestion = Funktion '{ $function_name }' im Schema '{ $schema_name }' nicht gefunden
    .available = Verfügbare Funktionen: { $available_functions }
    .suggestion = Meinten Sie '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' erwartet { $expected } Parameter{ $expected_plural } ({ $parameter_signature }), erhielt { $actual } Argument{ $actual_plural }
executor-parameter-type-mismatch = Parameter '{ $parameter_name }' erwartet { $expected_type }, erhielt { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Argumentanzahl-Konflikt: erwartet { $expected }, erhielt { $actual }

executor-recursion-limit-exceeded = Maximale Rekursionstiefe ({ $max_depth }) überschritten: { $message }
executor-recursion-call-stack = Aufrufstapel:
executor-function-must-return = Funktion muss einen Wert zurückgeben
executor-invalid-control-flow = Ungültiger Kontrollfluss: { $message }
executor-invalid-function-body = Ungültiger Funktionskörper: { $message }
executor-function-read-only-violation = Funktions-Schreibschutzverletzung: { $message }

# =============================================================================
# EXTRACT-Fehler
# =============================================================================

executor-invalid-extract-field = Extraktion von { $field } aus { $value_type }-Wert nicht möglich

# =============================================================================
# Spalten-/Arrow-Fehler
# =============================================================================

executor-arrow-downcast-error = Arrow-Array konnte nicht zu { $expected_type } umgewandelt werden ({ $context })
executor-columnar-type-mismatch-binary = Inkompatible Typen für { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Inkompatibler Typ für { $operation }: { $left_type }
executor-simd-operation-failed = SIMD-{ $operation } fehlgeschlagen: { $reason }
executor-columnar-column-not-found = Spaltenindex { $column_index } außerhalb des gültigen Bereichs (Batch hat { $batch_columns } Spalten)
executor-columnar-column-not-found-by-name = Spalte nicht gefunden: { $column_name }
executor-columnar-length-mismatch = Spaltenlängen-Konflikt in { $context }: erwartet { $expected }, erhielt { $actual }
executor-unsupported-array-type = Nicht unterstützter Array-Typ für { $operation }: { $array_type }

# =============================================================================
# Räumliche Fehler
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } erwartet { $expected }, erhielt { $actual }

# =============================================================================
# Cursor-Fehler
# =============================================================================

executor-cursor-already-exists = Cursor '{ $name }' existiert bereits
executor-cursor-not-found = Cursor '{ $name }' nicht gefunden
executor-cursor-already-open = Cursor '{ $name }' ist bereits geöffnet
executor-cursor-not-open = Cursor '{ $name }' ist nicht geöffnet
executor-cursor-not-scrollable = Cursor '{ $name }' ist nicht scrollbar (SCROLL nicht angegeben)

# =============================================================================
# Speicher- und Allgemeine Fehler
# =============================================================================

executor-storage-error = Speicherfehler: { $message }
executor-other = { $message }
