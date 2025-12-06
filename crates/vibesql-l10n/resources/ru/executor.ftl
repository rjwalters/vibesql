# VibeSQL Executor Error Messages - Russian (ru)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = Таблица '{ $name }' не найдена
executor-table-already-exists = Таблица '{ $name }' уже существует

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = Столбец '{ $column_name }' не найден в таблице '{ $table_name }'
executor-column-not-found-searched = Столбец '{ $column_name }' не найден (искали в таблицах: { $searched_tables })
executor-column-not-found-with-available = Столбец '{ $column_name }' не найден (искали в таблицах: { $searched_tables }). Доступные столбцы: { $available_columns }
executor-invalid-table-qualifier = Недопустимый квалификатор таблицы '{ $qualifier }' для столбца '{ $column }'. Доступные таблицы: { $available_tables }
executor-column-already-exists = Столбец '{ $name }' уже существует
executor-column-index-out-of-bounds = Индекс столбца { $index } выходит за пределы

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = Индекс '{ $name }' не найден
executor-index-already-exists = Индекс '{ $name }' уже существует
executor-invalid-index-definition = Недопустимое определение индекса: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = Триггер '{ $name }' не найден
executor-trigger-already-exists = Триггер '{ $name }' уже существует

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = Схема '{ $name }' не найдена
executor-schema-already-exists = Схема '{ $name }' уже существует
executor-schema-not-empty = Невозможно удалить схему '{ $name }': схема не пуста

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = Роль '{ $name }' не найдена
executor-permission-denied = Доступ запрещён: роль '{ $role }' не имеет привилегии { $privilege } на { $object }
executor-dependent-privileges-exist = Существуют зависимые привилегии: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = Тип '{ $name }' не найден
executor-type-already-exists = Тип '{ $name }' уже существует
executor-type-in-use = Невозможно удалить тип '{ $name }': тип всё ещё используется
executor-type-mismatch = Несоответствие типов: { $left } { $op } { $right }
executor-type-error = Ошибка типа: { $message }
executor-cast-error = Невозможно преобразовать { $from_type } в { $to_type }
executor-type-conversion-error = Невозможно конвертировать { $from } в { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = Деление на ноль
executor-invalid-where-clause = Недопустимое условие WHERE: { $message }
executor-unsupported-expression = Неподдерживаемое выражение: { $message }
executor-unsupported-feature = Неподдерживаемая функция: { $message }
executor-parse-error = Ошибка разбора: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = Скалярный подзапрос вернул { $actual } строк, ожидалось { $expected }
executor-subquery-column-count-mismatch = Подзапрос вернул { $actual } столбцов, ожидалось { $expected }
executor-column-count-mismatch = Производный список столбцов содержит { $provided } столбцов, но запрос возвращает { $expected } столбцов

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = Нарушение ограничения: { $message }
executor-multiple-primary-keys = Несколько ограничений PRIMARY KEY не допускается
executor-cannot-drop-column = Невозможно удалить столбец: { $message }
executor-constraint-not-found = Ограничение '{ $constraint_name }' не найдено в таблице '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = Превышен лимит глубины выражения: { $depth } > { $max_depth } (предотвращение переполнения стека)
executor-query-timeout-exceeded = Превышен таймаут запроса: { $elapsed_seconds }с > { $max_seconds }с
executor-row-limit-exceeded = Превышен лимит обработки строк: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Превышен лимит памяти: { $used_gb } ГБ > { $max_gb } ГБ

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = Переменная '{ $variable_name }' не найдена
executor-variable-not-found-with-available = Переменная '{ $variable_name }' не найдена. Доступные переменные: { $available_variables }
executor-label-not-found = Метка '{ $name }' не найдена

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = Процедурный SELECT INTO должен возвращать ровно { $expected } строку, получено { $actual } строк{ $plural }
executor-select-into-column-count = Несоответствие количества столбцов в процедурном SELECT INTO: { $expected } переменных{ $expected_plural }, но запрос вернул { $actual } столбцов{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = Процедура '{ $procedure_name }' не найдена в схеме '{ $schema_name }'
executor-procedure-not-found-with-available = Процедура '{ $procedure_name }' не найдена в схеме '{ $schema_name }'
    .available = Доступные процедуры: { $available_procedures }
executor-procedure-not-found-with-suggestion = Процедура '{ $procedure_name }' не найдена в схеме '{ $schema_name }'
    .available = Доступные процедуры: { $available_procedures }
    .suggestion = Возможно, вы имели в виду '{ $suggestion }'?

executor-function-not-found-simple = Функция '{ $function_name }' не найдена в схеме '{ $schema_name }'
executor-function-not-found-with-available = Функция '{ $function_name }' не найдена в схеме '{ $schema_name }'
    .available = Доступные функции: { $available_functions }
executor-function-not-found-with-suggestion = Функция '{ $function_name }' не найдена в схеме '{ $schema_name }'
    .available = Доступные функции: { $available_functions }
    .suggestion = Возможно, вы имели в виду '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' ожидает { $expected } параметр{ $expected_plural } ({ $parameter_signature }), получено { $actual } аргумент{ $actual_plural }
executor-parameter-type-mismatch = Параметр '{ $parameter_name }' ожидает { $expected_type }, получено { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Несоответствие количества аргументов: ожидалось { $expected }, получено { $actual }

executor-recursion-limit-exceeded = Превышена максимальная глубина рекурсии ({ $max_depth }): { $message }
executor-recursion-call-stack = Стек вызовов:
executor-function-must-return = Функция должна возвращать значение
executor-invalid-control-flow = Недопустимый поток управления: { $message }
executor-invalid-function-body = Недопустимое тело функции: { $message }
executor-function-read-only-violation = Нарушение режима только для чтения функции: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = Невозможно извлечь { $field } из значения типа { $value_type }

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Не удалось преобразовать массив Arrow в { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = Несовместимые типы для { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Несовместимый тип для { $operation }: { $left_type }
executor-simd-operation-failed = Операция SIMD { $operation } не удалась: { $reason }
executor-columnar-column-not-found = Индекс столбца { $column_index } выходит за пределы (пакет содержит { $batch_columns } столбцов)
executor-columnar-column-not-found-by-name = Столбец не найден: { $column_name }
executor-columnar-length-mismatch = Несоответствие длины столбца в { $context }: ожидалось { $expected }, получено { $actual }
executor-unsupported-array-type = Неподдерживаемый тип массива для { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } ожидает { $expected }, получено { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = Курсор '{ $name }' уже существует
executor-cursor-not-found = Курсор '{ $name }' не найден
executor-cursor-already-open = Курсор '{ $name }' уже открыт
executor-cursor-not-open = Курсор '{ $name }' не открыт
executor-cursor-not-scrollable = Курсор '{ $name }' не является прокручиваемым (SCROLL не указан)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = Ошибка хранилища: { $message }
executor-other = { $message }
