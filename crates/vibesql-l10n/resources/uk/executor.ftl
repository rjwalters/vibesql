# VibeSQL Executor Error Messages - Ukrainian (Українська)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = Таблицю '{ $name }' не знайдено
executor-table-already-exists = Таблиця '{ $name }' вже існує

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = Колонку '{ $column_name }' не знайдено в таблиці '{ $table_name }'
executor-column-not-found-searched = Колонку '{ $column_name }' не знайдено (пошук у таблицях: { $searched_tables })
executor-column-not-found-with-available = Колонку '{ $column_name }' не знайдено (пошук у таблицях: { $searched_tables }). Доступні колонки: { $available_columns }
executor-invalid-table-qualifier = Некоректний кваліфікатор таблиці '{ $qualifier }' для колонки '{ $column }'. Доступні таблиці: { $available_tables }
executor-column-already-exists = Колонка '{ $name }' вже існує
executor-column-index-out-of-bounds = Індекс колонки { $index } виходить за межі

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = Індекс '{ $name }' не знайдено
executor-index-already-exists = Індекс '{ $name }' вже існує
executor-invalid-index-definition = Некоректне визначення індексу: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = Тригер '{ $name }' не знайдено
executor-trigger-already-exists = Тригер '{ $name }' вже існує

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = Схему '{ $name }' не знайдено
executor-schema-already-exists = Схема '{ $name }' вже існує
executor-schema-not-empty = Неможливо видалити схему '{ $name }': схема не порожня

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = Роль '{ $name }' не знайдено
executor-permission-denied = Доступ заборонено: роль '{ $role }' не має привілею { $privilege } на { $object }
executor-dependent-privileges-exist = Існують залежні привілеї: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = Тип '{ $name }' не знайдено
executor-type-already-exists = Тип '{ $name }' вже існує
executor-type-in-use = Неможливо видалити тип '{ $name }': тип ще використовується
executor-type-mismatch = Невідповідність типів: { $left } { $op } { $right }
executor-type-error = Помилка типу: { $message }
executor-cast-error = Неможливо перетворити { $from_type } в { $to_type }
executor-type-conversion-error = Неможливо конвертувати { $from } в { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = Ділення на нуль
executor-invalid-where-clause = Некоректна умова WHERE: { $message }
executor-unsupported-expression = Непідтримуваний вираз: { $message }
executor-unsupported-feature = Непідтримувана функція: { $message }
executor-parse-error = Помилка парсингу: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = Скалярний підзапит повернув { $actual } рядків, очікувався { $expected }
executor-subquery-column-count-mismatch = Підзапит повернув { $actual } колонок, очікувалось { $expected }
executor-column-count-mismatch = Похідний список колонок має { $provided } колонок, але запит повертає { $expected } колонок

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = Порушення обмеження: { $message }
executor-multiple-primary-keys = Декілька обмежень PRIMARY KEY не дозволено
executor-cannot-drop-column = Неможливо видалити колонку: { $message }
executor-constraint-not-found = Обмеження '{ $constraint_name }' не знайдено в таблиці '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = Перевищено ліміт глибини виразу: { $depth } > { $max_depth } (запобігання переповненню стеку)
executor-query-timeout-exceeded = Перевищено ліміт часу запиту: { $elapsed_seconds }с > { $max_seconds }с
executor-row-limit-exceeded = Перевищено ліміт обробки рядків: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Перевищено ліміт пам'яті: { $used_gb } ГБ > { $max_gb } ГБ

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = Змінну '{ $variable_name }' не знайдено
executor-variable-not-found-with-available = Змінну '{ $variable_name }' не знайдено. Доступні змінні: { $available_variables }
executor-label-not-found = Мітку '{ $name }' не знайдено

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = Процедурний SELECT INTO повинен повернути рівно { $expected } рядок, отримано { $actual } рядків{ $plural }
executor-select-into-column-count = Невідповідність кількості колонок у процедурному SELECT INTO: { $expected } змінних{ $expected_plural }, але запит повернув { $actual } колонок{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = Процедуру '{ $procedure_name }' не знайдено в схемі '{ $schema_name }'
executor-procedure-not-found-with-available = Процедуру '{ $procedure_name }' не знайдено в схемі '{ $schema_name }'
    .available = Доступні процедури: { $available_procedures }
executor-procedure-not-found-with-suggestion = Процедуру '{ $procedure_name }' не знайдено в схемі '{ $schema_name }'
    .available = Доступні процедури: { $available_procedures }
    .suggestion = Можливо, ви мали на увазі '{ $suggestion }'?

executor-function-not-found-simple = Функцію '{ $function_name }' не знайдено в схемі '{ $schema_name }'
executor-function-not-found-with-available = Функцію '{ $function_name }' не знайдено в схемі '{ $schema_name }'
    .available = Доступні функції: { $available_functions }
executor-function-not-found-with-suggestion = Функцію '{ $function_name }' не знайдено в схемі '{ $schema_name }'
    .available = Доступні функції: { $available_functions }
    .suggestion = Можливо, ви мали на увазі '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' очікує { $expected } параметрів{ $expected_plural } ({ $parameter_signature }), отримано { $actual } аргументів{ $actual_plural }
executor-parameter-type-mismatch = Параметр '{ $parameter_name }' очікує { $expected_type }, отримано { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Невідповідність кількості аргументів: очікувалось { $expected }, отримано { $actual }

executor-recursion-limit-exceeded = Перевищено максимальну глибину рекурсії ({ $max_depth }): { $message }
executor-recursion-call-stack = Стек викликів:
executor-function-must-return = Функція повинна повернути значення
executor-invalid-control-flow = Некоректний потік керування: { $message }
executor-invalid-function-body = Некоректне тіло функції: { $message }
executor-function-read-only-violation = Порушення режиму тільки для читання функції: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = Неможливо витягнути { $field } зі значення типу { $value_type }

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Не вдалося привести тип Arrow масиву до { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = Несумісні типи для { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Несумісний тип для { $operation }: { $left_type }
executor-simd-operation-failed = SIMD операція { $operation } не вдалася: { $reason }
executor-columnar-column-not-found = Індекс колонки { $column_index } виходить за межі (batch має { $batch_columns } колонок)
executor-columnar-column-not-found-by-name = Колонку не знайдено: { $column_name }
executor-columnar-length-mismatch = Невідповідність довжини колонки в { $context }: очікувалось { $expected }, отримано { $actual }
executor-unsupported-array-type = Непідтримуваний тип масиву для { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } очікує { $expected }, отримано { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = Курсор '{ $name }' вже існує
executor-cursor-not-found = Курсор '{ $name }' не знайдено
executor-cursor-already-open = Курсор '{ $name }' вже відкритий
executor-cursor-not-open = Курсор '{ $name }' не відкритий
executor-cursor-not-scrollable = Курсор '{ $name }' не підтримує прокрутку (SCROLL не вказано)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = Помилка сховища: { $message }
executor-other = { $message }
