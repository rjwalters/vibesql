# VibeSQL Executor Mensajes de Error - Español
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = Tabla '{ $name }' no encontrada
executor-table-already-exists = La tabla '{ $name }' ya existe

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = Columna '{ $column_name }' no encontrada en la tabla '{ $table_name }'
executor-column-not-found-searched = Columna '{ $column_name }' no encontrada (tablas buscadas: { $searched_tables })
executor-column-not-found-with-available = Columna '{ $column_name }' no encontrada (tablas buscadas: { $searched_tables }). Columnas disponibles: { $available_columns }
executor-invalid-table-qualifier = Calificador de tabla inválido '{ $qualifier }' para la columna '{ $column }'. Tablas disponibles: { $available_tables }
executor-column-already-exists = La columna '{ $name }' ya existe
executor-column-index-out-of-bounds = Índice de columna { $index } fuera de límites

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = Índice '{ $name }' no encontrado
executor-index-already-exists = El índice '{ $name }' ya existe
executor-invalid-index-definition = Definición de índice inválida: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = Trigger '{ $name }' no encontrado
executor-trigger-already-exists = El trigger '{ $name }' ya existe

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = Esquema '{ $name }' no encontrado
executor-schema-already-exists = El esquema '{ $name }' ya existe
executor-schema-not-empty = No se puede eliminar el esquema '{ $name }': el esquema no está vacío

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = Rol '{ $name }' no encontrado
executor-permission-denied = Permiso denegado: el rol '{ $role }' carece del privilegio { $privilege } en { $object }
executor-dependent-privileges-exist = Existen privilegios dependientes: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = Tipo '{ $name }' no encontrado
executor-type-already-exists = El tipo '{ $name }' ya existe
executor-type-in-use = No se puede eliminar el tipo '{ $name }': el tipo está en uso
executor-type-mismatch = Discrepancia de tipo: { $left } { $op } { $right }
executor-type-error = Error de tipo: { $message }
executor-cast-error = No se puede convertir { $from_type } a { $to_type }
executor-type-conversion-error = No se puede convertir { $from } a { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = División por cero
executor-invalid-where-clause = Cláusula WHERE inválida: { $message }
executor-unsupported-expression = Expresión no soportada: { $message }
executor-unsupported-feature = Característica no soportada: { $message }
executor-parse-error = Error de análisis: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = La subconsulta escalar devolvió { $actual } filas, se esperaba { $expected }
executor-subquery-column-count-mismatch = La subconsulta devolvió { $actual } columnas, se esperaba { $expected }
executor-column-count-mismatch = La lista de columnas derivadas tiene { $provided } columnas pero la consulta produce { $expected } columnas

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = Violación de restricción: { $message }
executor-multiple-primary-keys = No se permiten múltiples restricciones PRIMARY KEY
executor-cannot-drop-column = No se puede eliminar la columna: { $message }
executor-constraint-not-found = Restricción '{ $constraint_name }' no encontrada en la tabla '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = Se excedió el límite de profundidad de expresión: { $depth } > { $max_depth } (previene desbordamiento de pila)
executor-query-timeout-exceeded = Se excedió el tiempo límite de consulta: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Se excedió el límite de procesamiento de filas: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Se excedió el límite de memoria: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = Variable '{ $variable_name }' no encontrada
executor-variable-not-found-with-available = Variable '{ $variable_name }' no encontrada. Variables disponibles: { $available_variables }
executor-label-not-found = Etiqueta '{ $name }' no encontrada

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = El SELECT INTO procedural debe devolver exactamente { $expected } fila, obtuvo { $actual } fila{ $plural }
executor-select-into-column-count = Discrepancia en el conteo de columnas del SELECT INTO procedural: { $expected } variable{ $expected_plural } pero la consulta devolvió { $actual } columna{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = Procedimiento '{ $procedure_name }' no encontrado en el esquema '{ $schema_name }'
executor-procedure-not-found-with-available = Procedimiento '{ $procedure_name }' no encontrado en el esquema '{ $schema_name }'
    .available = Procedimientos disponibles: { $available_procedures }
executor-procedure-not-found-with-suggestion = Procedimiento '{ $procedure_name }' no encontrado en el esquema '{ $schema_name }'
    .available = Procedimientos disponibles: { $available_procedures }
    .suggestion = ¿Quiso decir '{ $suggestion }'?

executor-function-not-found-simple = Función '{ $function_name }' no encontrada en el esquema '{ $schema_name }'
executor-function-not-found-with-available = Función '{ $function_name }' no encontrada en el esquema '{ $schema_name }'
    .available = Funciones disponibles: { $available_functions }
executor-function-not-found-with-suggestion = Función '{ $function_name }' no encontrada en el esquema '{ $schema_name }'
    .available = Funciones disponibles: { $available_functions }
    .suggestion = ¿Quiso decir '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' espera { $expected } parámetro{ $expected_plural } ({ $parameter_signature }), obtuvo { $actual } argumento{ $actual_plural }
executor-parameter-type-mismatch = El parámetro '{ $parameter_name }' espera { $expected_type }, obtuvo { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Discrepancia en el conteo de argumentos: esperado { $expected }, obtenido { $actual }

executor-recursion-limit-exceeded = Se excedió la profundidad máxima de recursión ({ $max_depth }): { $message }
executor-recursion-call-stack = Pila de llamadas:
executor-function-must-return = La función debe devolver un valor
executor-invalid-control-flow = Flujo de control inválido: { $message }
executor-invalid-function-body = Cuerpo de función inválido: { $message }
executor-function-read-only-violation = Violación de solo lectura de función: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = No se puede extraer { $field } del valor de tipo { $value_type }

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Falló la conversión del arreglo Arrow a { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = Tipos incompatibles para { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Tipo incompatible para { $operation }: { $left_type }
executor-simd-operation-failed = La operación SIMD { $operation } falló: { $reason }
executor-columnar-column-not-found = Índice de columna { $column_index } fuera de límites (el lote tiene { $batch_columns } columnas)
executor-columnar-column-not-found-by-name = Columna no encontrada: { $column_name }
executor-columnar-length-mismatch = Discrepancia de longitud de columna en { $context }: esperado { $expected }, obtenido { $actual }
executor-unsupported-array-type = Tipo de arreglo no soportado para { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } espera { $expected }, obtuvo { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = El cursor '{ $name }' ya existe
executor-cursor-not-found = Cursor '{ $name }' no encontrado
executor-cursor-already-open = El cursor '{ $name }' ya está abierto
executor-cursor-not-open = El cursor '{ $name }' no está abierto
executor-cursor-not-scrollable = El cursor '{ $name }' no es desplazable (SCROLL no especificado)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = Error de almacenamiento: { $message }
executor-other = { $message }
