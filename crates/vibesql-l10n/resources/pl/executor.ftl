# VibeSQL Executor Error Messages - Polski (Polish)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = Nie znaleziono tabeli '{ $name }'
executor-table-already-exists = Tabela '{ $name }' już istnieje

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = Nie znaleziono kolumny '{ $column_name }' w tabeli '{ $table_name }'
executor-column-not-found-searched = Nie znaleziono kolumny '{ $column_name }' (przeszukane tabele: { $searched_tables })
executor-column-not-found-with-available = Nie znaleziono kolumny '{ $column_name }' (przeszukane tabele: { $searched_tables }). Dostępne kolumny: { $available_columns }
executor-invalid-table-qualifier = Nieprawidłowy kwalifikator tabeli '{ $qualifier }' dla kolumny '{ $column }'. Dostępne tabele: { $available_tables }
executor-column-already-exists = Kolumna '{ $name }' już istnieje
executor-column-index-out-of-bounds = Indeks kolumny { $index } poza zakresem

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = Nie znaleziono indeksu '{ $name }'
executor-index-already-exists = Indeks '{ $name }' już istnieje
executor-invalid-index-definition = Nieprawidłowa definicja indeksu: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = Nie znaleziono wyzwalacza '{ $name }'
executor-trigger-already-exists = Wyzwalacz '{ $name }' już istnieje

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = Nie znaleziono schematu '{ $name }'
executor-schema-already-exists = Schemat '{ $name }' już istnieje
executor-schema-not-empty = Nie można usunąć schematu '{ $name }': schemat nie jest pusty

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = Nie znaleziono roli '{ $name }'
executor-permission-denied = Odmowa dostępu: rola '{ $role }' nie ma uprawnienia { $privilege } do { $object }
executor-dependent-privileges-exist = Istnieją zależne uprawnienia: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = Nie znaleziono typu '{ $name }'
executor-type-already-exists = Typ '{ $name }' już istnieje
executor-type-in-use = Nie można usunąć typu '{ $name }': typ jest nadal używany
executor-type-mismatch = Niezgodność typów: { $left } { $op } { $right }
executor-type-error = Błąd typu: { $message }
executor-cast-error = Nie można rzutować { $from_type } na { $to_type }
executor-type-conversion-error = Nie można przekonwertować { $from } na { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = Dzielenie przez zero
executor-invalid-where-clause = Nieprawidłowa klauzula WHERE: { $message }
executor-unsupported-expression = Nieobsługiwane wyrażenie: { $message }
executor-unsupported-feature = Nieobsługiwana funkcjonalność: { $message }
executor-parse-error = Błąd parsowania: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = Podzapytanie skalarne zwróciło { $actual } wierszy, oczekiwano { $expected }
executor-subquery-column-count-mismatch = Podzapytanie zwróciło { $actual } kolumn, oczekiwano { $expected }
executor-column-count-mismatch = Lista kolumn pochodnych ma { $provided } kolumn, ale zapytanie produkuje { $expected } kolumn

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = Naruszenie ograniczenia: { $message }
executor-multiple-primary-keys = Wiele ograniczeń PRIMARY KEY jest niedozwolonych
executor-cannot-drop-column = Nie można usunąć kolumny: { $message }
executor-constraint-not-found = Nie znaleziono ograniczenia '{ $constraint_name }' w tabeli '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = Przekroczono limit głębokości wyrażenia: { $depth } > { $max_depth } (zapobiega przepełnieniu stosu)
executor-query-timeout-exceeded = Przekroczono limit czasu zapytania: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Przekroczono limit przetwarzania wierszy: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Przekroczono limit pamięci: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = Nie znaleziono zmiennej '{ $variable_name }'
executor-variable-not-found-with-available = Nie znaleziono zmiennej '{ $variable_name }'. Dostępne zmienne: { $available_variables }
executor-label-not-found = Nie znaleziono etykiety '{ $name }'

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = Proceduralne SELECT INTO musi zwrócić dokładnie { $expected } wiersz, otrzymano { $actual } wiersz{ $plural }
executor-select-into-column-count = Niezgodność liczby kolumn w proceduralnym SELECT INTO: { $expected } zmienn{ $expected_plural }, ale zapytanie zwróciło { $actual } kolumn{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = Nie znaleziono procedury '{ $procedure_name }' w schemacie '{ $schema_name }'
executor-procedure-not-found-with-available = Nie znaleziono procedury '{ $procedure_name }' w schemacie '{ $schema_name }'
    .available = Dostępne procedury: { $available_procedures }
executor-procedure-not-found-with-suggestion = Nie znaleziono procedury '{ $procedure_name }' w schemacie '{ $schema_name }'
    .available = Dostępne procedury: { $available_procedures }
    .suggestion = Czy chodziło o '{ $suggestion }'?

executor-function-not-found-simple = Nie znaleziono funkcji '{ $function_name }' w schemacie '{ $schema_name }'
executor-function-not-found-with-available = Nie znaleziono funkcji '{ $function_name }' w schemacie '{ $schema_name }'
    .available = Dostępne funkcje: { $available_functions }
executor-function-not-found-with-suggestion = Nie znaleziono funkcji '{ $function_name }' w schemacie '{ $schema_name }'
    .available = Dostępne funkcje: { $available_functions }
    .suggestion = Czy chodziło o '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' oczekuje { $expected } parametr{ $expected_plural } ({ $parameter_signature }), otrzymano { $actual } argument{ $actual_plural }
executor-parameter-type-mismatch = Parametr '{ $parameter_name }' oczekuje { $expected_type }, otrzymano { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Niezgodność liczby argumentów: oczekiwano { $expected }, otrzymano { $actual }

executor-recursion-limit-exceeded = Przekroczono maksymalną głębokość rekurencji ({ $max_depth }): { $message }
executor-recursion-call-stack = Stos wywołań:
executor-function-must-return = Funkcja musi zwrócić wartość
executor-invalid-control-flow = Nieprawidłowy przepływ sterowania: { $message }
executor-invalid-function-body = Nieprawidłowe ciało funkcji: { $message }
executor-function-read-only-violation = Naruszenie trybu tylko do odczytu funkcji: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = Nie można wyodrębnić { $field } z wartości typu { $value_type }

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Nie udało się rzutować tablicy Arrow na { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = Niekompatybilne typy dla { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Niekompatybilny typ dla { $operation }: { $left_type }
executor-simd-operation-failed = Operacja SIMD { $operation } nie powiodła się: { $reason }
executor-columnar-column-not-found = Indeks kolumny { $column_index } poza zakresem (partia ma { $batch_columns } kolumn)
executor-columnar-column-not-found-by-name = Nie znaleziono kolumny: { $column_name }
executor-columnar-length-mismatch = Niezgodność długości kolumny w { $context }: oczekiwano { $expected }, otrzymano { $actual }
executor-unsupported-array-type = Nieobsługiwany typ tablicy dla { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } oczekuje { $expected }, otrzymano { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = Kursor '{ $name }' już istnieje
executor-cursor-not-found = Nie znaleziono kursora '{ $name }'
executor-cursor-already-open = Kursor '{ $name }' jest już otwarty
executor-cursor-not-open = Kursor '{ $name }' nie jest otwarty
executor-cursor-not-scrollable = Kursor '{ $name }' nie jest przewijalny (nie określono SCROLL)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = Błąd przechowywania: { $message }
executor-other = { $message }
