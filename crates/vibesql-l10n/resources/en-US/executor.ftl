# VibeSQL Executor Error Messages - English (US)
# This file contains all error messages for the vibesql-executor crate.

# =============================================================================
# Table Errors
# =============================================================================

executor-table-not-found = Table '{ $name }' not found
executor-table-already-exists = Table '{ $name }' already exists

# =============================================================================
# Column Errors
# =============================================================================

executor-column-not-found-simple = Column '{ $column_name }' not found in table '{ $table_name }'
executor-column-not-found-searched = Column '{ $column_name }' not found (searched tables: { $searched_tables })
executor-column-not-found-with-available = Column '{ $column_name }' not found (searched tables: { $searched_tables }). Available columns: { $available_columns }
executor-invalid-table-qualifier = Invalid table qualifier '{ $qualifier }' for column '{ $column }'. Available tables: { $available_tables }
executor-column-already-exists = Column '{ $name }' already exists
executor-column-index-out-of-bounds = Column index { $index } out of bounds

# =============================================================================
# Index Errors
# =============================================================================

executor-index-not-found = Index '{ $name }' not found
executor-index-already-exists = Index '{ $name }' already exists
executor-invalid-index-definition = Invalid index definition: { $message }

# =============================================================================
# Trigger Errors
# =============================================================================

executor-trigger-not-found = Trigger '{ $name }' not found
executor-trigger-already-exists = Trigger '{ $name }' already exists

# =============================================================================
# Schema Errors
# =============================================================================

executor-schema-not-found = Schema '{ $name }' not found
executor-schema-already-exists = Schema '{ $name }' already exists
executor-schema-not-empty = Cannot drop schema '{ $name }': schema is not empty

# =============================================================================
# Role and Permission Errors
# =============================================================================

executor-role-not-found = Role '{ $name }' not found
executor-permission-denied = Permission denied: role '{ $role }' lacks { $privilege } privilege on { $object }
executor-dependent-privileges-exist = Dependent privileges exist: { $message }

# =============================================================================
# Type Errors
# =============================================================================

executor-type-not-found = Type '{ $name }' not found
executor-type-already-exists = Type '{ $name }' already exists
executor-type-in-use = Cannot drop type '{ $name }': type is still in use
executor-type-mismatch = Type mismatch: { $left } { $op } { $right }
executor-type-error = Type error: { $message }
executor-cast-error = Cannot cast { $from_type } to { $to_type }
executor-type-conversion-error = Cannot convert { $from } to { $to }

# =============================================================================
# Expression and Query Errors
# =============================================================================

executor-division-by-zero = Division by zero
executor-invalid-where-clause = Invalid WHERE clause: { $message }
executor-unsupported-expression = Unsupported expression: { $message }
executor-unsupported-feature = Unsupported feature: { $message }
executor-parse-error = Parse error: { $message }

# =============================================================================
# Subquery Errors
# =============================================================================

executor-subquery-returned-multiple-rows = Scalar subquery returned { $actual } rows, expected { $expected }
executor-subquery-column-count-mismatch = Subquery returned { $actual } columns, expected { $expected }
executor-column-count-mismatch = Derived column list has { $provided } columns but query produces { $expected } columns

# =============================================================================
# Constraint Errors
# =============================================================================

executor-constraint-violation = Constraint violation: { $message }
executor-multiple-primary-keys = Multiple PRIMARY KEY constraints are not allowed
executor-cannot-drop-column = Cannot drop column: { $message }
executor-constraint-not-found = Constraint '{ $constraint_name }' not found in table '{ $table_name }'

# =============================================================================
# Resource Limit Errors
# =============================================================================

executor-expression-depth-exceeded = Expression depth limit exceeded: { $depth } > { $max_depth } (prevents stack overflow)
executor-query-timeout-exceeded = Query timeout exceeded: { $elapsed_seconds }s > { $max_seconds }s
executor-row-limit-exceeded = Row processing limit exceeded: { $rows_processed } > { $max_rows }
executor-memory-limit-exceeded = Memory limit exceeded: { $used_gb } GB > { $max_gb } GB

# =============================================================================
# Procedural/Variable Errors
# =============================================================================

executor-variable-not-found-simple = Variable '{ $variable_name }' not found
executor-variable-not-found-with-available = Variable '{ $variable_name }' not found. Available variables: { $available_variables }
executor-label-not-found = Label '{ $name }' not found

# =============================================================================
# SELECT INTO Errors
# =============================================================================

executor-select-into-row-count = Procedural SELECT INTO must return exactly { $expected } row, got { $actual } row{ $plural }
executor-select-into-column-count = Procedural SELECT INTO column count mismatch: { $expected } variable{ $expected_plural } but query returned { $actual } column{ $actual_plural }

# =============================================================================
# Procedure and Function Errors
# =============================================================================

executor-procedure-not-found-simple = Procedure '{ $procedure_name }' not found in schema '{ $schema_name }'
executor-procedure-not-found-with-available = Procedure '{ $procedure_name }' not found in schema '{ $schema_name }'
    .available = Available procedures: { $available_procedures }
executor-procedure-not-found-with-suggestion = Procedure '{ $procedure_name }' not found in schema '{ $schema_name }'
    .available = Available procedures: { $available_procedures }
    .suggestion = Did you mean '{ $suggestion }'?

executor-function-not-found-simple = Function '{ $function_name }' not found in schema '{ $schema_name }'
executor-function-not-found-with-available = Function '{ $function_name }' not found in schema '{ $schema_name }'
    .available = Available functions: { $available_functions }
executor-function-not-found-with-suggestion = Function '{ $function_name }' not found in schema '{ $schema_name }'
    .available = Available functions: { $available_functions }
    .suggestion = Did you mean '{ $suggestion }'?

executor-parameter-count-mismatch = { $routine_type } '{ $routine_name }' expects { $expected } parameter{ $expected_plural } ({ $parameter_signature }), got { $actual } argument{ $actual_plural }
executor-parameter-type-mismatch = Parameter '{ $parameter_name }' expects { $expected_type }, got { $actual_type } '{ $actual_value }'
executor-argument-count-mismatch = Argument count mismatch: expected { $expected }, got { $actual }

executor-recursion-limit-exceeded = Maximum recursion depth ({ $max_depth }) exceeded: { $message }
executor-recursion-call-stack = Call stack:
executor-function-must-return = Function must return a value
executor-invalid-control-flow = Invalid control flow: { $message }
executor-invalid-function-body = Invalid function body: { $message }
executor-function-read-only-violation = Function read-only violation: { $message }

# =============================================================================
# EXTRACT Errors
# =============================================================================

executor-invalid-extract-field = Cannot extract { $field } from { $value_type } value

# =============================================================================
# Columnar/Arrow Errors
# =============================================================================

executor-arrow-downcast-error = Failed to downcast Arrow array to { $expected_type } ({ $context })
executor-columnar-type-mismatch-binary = Incompatible types for { $operation }: { $left_type } vs { $right_type }
executor-columnar-type-mismatch-unary = Incompatible type for { $operation }: { $left_type }
executor-simd-operation-failed = SIMD { $operation } failed: { $reason }
executor-columnar-column-not-found = Column index { $column_index } out of bounds (batch has { $batch_columns } columns)
executor-columnar-column-not-found-by-name = Column not found: { $column_name }
executor-columnar-length-mismatch = Column length mismatch in { $context }: expected { $expected }, got { $actual }
executor-unsupported-array-type = Unsupported array type for { $operation }: { $array_type }

# =============================================================================
# Spatial Errors
# =============================================================================

executor-spatial-geometry-error = { $function_name }: { $message }
executor-spatial-operation-failed = { $function_name }: { $message }
executor-spatial-argument-error = { $function_name } expects { $expected }, got { $actual }

# =============================================================================
# Cursor Errors
# =============================================================================

executor-cursor-already-exists = Cursor '{ $name }' already exists
executor-cursor-not-found = Cursor '{ $name }' not found
executor-cursor-already-open = Cursor '{ $name }' is already open
executor-cursor-not-open = Cursor '{ $name }' is not open
executor-cursor-not-scrollable = Cursor '{ $name }' is not scrollable (SCROLL not specified)

# =============================================================================
# Storage and General Errors
# =============================================================================

executor-storage-error = Storage error: { $message }
executor-other = { $message }
