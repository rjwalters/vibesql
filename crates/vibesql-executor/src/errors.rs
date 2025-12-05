#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorError {
    TableNotFound(String),
    TableAlreadyExists(String),
    ColumnNotFound {
        column_name: String,
        table_name: String,
        searched_tables: Vec<String>,
        available_columns: Vec<String>,
    },
    InvalidTableQualifier {
        qualifier: String,
        column: String,
        available_tables: Vec<String>,
    },
    ColumnAlreadyExists(String),
    IndexNotFound(String),
    IndexAlreadyExists(String),
    InvalidIndexDefinition(String),
    TriggerNotFound(String),
    TriggerAlreadyExists(String),
    SchemaNotFound(String),
    SchemaAlreadyExists(String),
    SchemaNotEmpty(String),
    RoleNotFound(String),
    TypeNotFound(String),
    TypeAlreadyExists(String),
    TypeInUse(String),
    DependentPrivilegesExist(String),
    PermissionDenied {
        role: String,
        privilege: String,
        object: String,
    },
    ColumnIndexOutOfBounds {
        index: usize,
    },
    TypeMismatch {
        left: vibesql_types::SqlValue,
        op: String,
        right: vibesql_types::SqlValue,
    },
    DivisionByZero,
    InvalidWhereClause(String),
    UnsupportedExpression(String),
    UnsupportedFeature(String),
    StorageError(String),
    SubqueryReturnedMultipleRows {
        expected: usize,
        actual: usize,
    },
    SubqueryColumnCountMismatch {
        expected: usize,
        actual: usize,
    },
    ColumnCountMismatch {
        expected: usize,
        provided: usize,
    },
    CastError {
        from_type: String,
        to_type: String,
    },
    TypeConversionError {
        from: String,
        to: String,
    },
    ConstraintViolation(String),
    MultiplePrimaryKeys,
    CannotDropColumn(String),
    ConstraintNotFound {
        constraint_name: String,
        table_name: String,
    },
    /// Expression evaluation exceeded maximum recursion depth
    /// This prevents stack overflow from deeply nested expressions or subqueries
    ExpressionDepthExceeded {
        depth: usize,
        max_depth: usize,
    },
    /// Query exceeded maximum execution time
    QueryTimeoutExceeded {
        elapsed_seconds: u64,
        max_seconds: u64,
    },
    /// Query exceeded maximum row processing limit
    RowLimitExceeded {
        rows_processed: usize,
        max_rows: usize,
    },
    /// Query exceeded maximum memory limit
    MemoryLimitExceeded {
        used_bytes: usize,
        max_bytes: usize,
    },
    /// Variable not found in procedural context (with available variables)
    VariableNotFound {
        variable_name: String,
        available_variables: Vec<String>,
    },
    /// Label not found in procedural context
    LabelNotFound(String),
    /// Procedural SELECT INTO returned wrong number of rows (must be exactly 1)
    SelectIntoRowCount {
        expected: usize,
        actual: usize,
    },
    /// Procedural SELECT INTO column count doesn't match variable count
    SelectIntoColumnCount {
        expected: usize,
        actual: usize,
    },
    /// Procedure not found (with suggestions)
    ProcedureNotFound {
        procedure_name: String,
        schema_name: String,
        available_procedures: Vec<String>,
    },
    /// Function not found (with suggestions)
    FunctionNotFound {
        function_name: String,
        schema_name: String,
        available_functions: Vec<String>,
    },
    /// Parameter count mismatch with details
    ParameterCountMismatch {
        routine_name: String,
        routine_type: String, // "Procedure" or "Function"
        expected: usize,
        actual: usize,
        parameter_signature: String,
    },
    /// Parameter type mismatch with details
    ParameterTypeMismatch {
        parameter_name: String,
        expected_type: String,
        actual_type: String,
        actual_value: String,
    },
    /// Type error in expression evaluation
    TypeError(String),
    /// Function argument count mismatch
    ArgumentCountMismatch {
        expected: usize,
        actual: usize,
    },
    /// Recursion limit exceeded in function/procedure calls (with call stack)
    RecursionLimitExceeded {
        message: String,
        call_stack: Vec<String>,
        max_depth: usize,
    },
    /// Function must return a value but did not
    FunctionMustReturn,
    /// Invalid control flow (e.g., LEAVE/ITERATE outside of loop)
    InvalidControlFlow(String),
    /// Invalid function body syntax
    InvalidFunctionBody(String),
    /// Function attempted to modify data (read-only violation)
    FunctionReadOnlyViolation(String),
    /// Parse error
    ParseError(String),
    /// Invalid EXTRACT field for the given value type
    InvalidExtractField {
        field: String,
        value_type: String,
    },
    /// Arrow array downcast failed (columnar execution)
    ArrowDowncastError {
        expected_type: String,
        context: String,
    },
    /// Type mismatch in columnar operations
    ColumnarTypeMismatch {
        operation: String,
        left_type: String,
        right_type: Option<String>,
    },
    /// SIMD operation failed (returned None or error)
    SimdOperationFailed {
        operation: String,
        reason: String,
    },
    /// Column not found in batch by index
    ColumnarColumnNotFound {
        column_index: usize,
        batch_columns: usize,
    },
    /// Column not found in batch by name
    ColumnarColumnNotFoundByName {
        column_name: String,
    },
    /// Column length mismatch in batch operations
    ColumnarLengthMismatch {
        context: String,
        expected: usize,
        actual: usize,
    },
    /// Unsupported array type for columnar operation
    UnsupportedArrayType {
        operation: String,
        array_type: String,
    },
    /// Invalid or incompatible geometry type in spatial function
    SpatialGeometryError {
        function_name: String,
        message: String,
    },
    /// Spatial operation failed (distance, intersection, etc.)
    SpatialOperationFailed {
        function_name: String,
        message: String,
    },
    /// Spatial function argument count or type mismatch
    SpatialArgumentError {
        function_name: String,
        expected: String,
        actual: String,
    },
    /// Cursor already exists with this name
    CursorAlreadyExists(String),
    /// Cursor not found
    CursorNotFound(String),
    /// Cursor is already open
    CursorAlreadyOpen(String),
    /// Cursor is not open (must OPEN before FETCH)
    CursorNotOpen(String),
    /// Cursor does not support backward movement (not declared with SCROLL)
    CursorNotScrollable(String),
    Other(String),
}

/// Find the closest matching string using simple Levenshtein distance
fn find_closest_match<'a>(target: &str, candidates: &'a [String]) -> Option<&'a String> {
    if candidates.is_empty() {
        return None;
    }

    let target_lower = target.to_lowercase();

    // First check for exact case-insensitive match
    if let Some(exact) = candidates.iter().find(|c| c.to_lowercase() == target_lower) {
        return Some(exact);
    }

    // Calculate Levenshtein distance for each candidate
    let mut best_match: Option<(&String, usize)> = None;

    for candidate in candidates {
        let distance = levenshtein_distance(&target_lower, &candidate.to_lowercase());

        // Only suggest if distance is small relative to the target length
        // (e.g., within 2 edits or 30% of the length)
        let max_distance = (target.len() / 3).max(2);

        if distance <= max_distance {
            if let Some((_, best_distance)) = best_match {
                if distance < best_distance {
                    best_match = Some((candidate, distance));
                }
            } else {
                best_match = Some((candidate, distance));
            }
        }
    }

    best_match.map(|(s, _)| s)
}

/// Calculate Levenshtein distance between two strings
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let len1 = s1.len();
    let len2 = s2.len();

    if len1 == 0 {
        return len2;
    }
    if len2 == 0 {
        return len1;
    }

    let mut prev_row: Vec<usize> = (0..=len2).collect();
    let mut curr_row = vec![0; len2 + 1];

    for (i, c1) in s1.chars().enumerate() {
        curr_row[0] = i + 1;

        for (j, c2) in s2.chars().enumerate() {
            let cost = if c1 == c2 { 0 } else { 1 };
            curr_row[j + 1] = (curr_row[j] + 1).min(prev_row[j + 1] + 1).min(prev_row[j] + cost);
        }

        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[len2]
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorError::TableNotFound(name) => write!(f, "Table '{}' not found", name),
            ExecutorError::TableAlreadyExists(name) => write!(f, "Table '{}' already exists", name),
            ExecutorError::ColumnNotFound {
                column_name,
                table_name,
                searched_tables,
                available_columns,
            } => {
                if searched_tables.is_empty() {
                    write!(f, "Column '{}' not found in table '{}'", column_name, table_name)
                } else if available_columns.is_empty() {
                    write!(
                        f,
                        "Column '{}' not found (searched tables: {})",
                        column_name,
                        searched_tables.join(", ")
                    )
                } else {
                    write!(
                        f,
                        "Column '{}' not found (searched tables: {}). Available columns: {}",
                        column_name,
                        searched_tables.join(", "),
                        available_columns.join(", ")
                    )
                }
            }
            ExecutorError::InvalidTableQualifier { qualifier, column, available_tables } => {
                write!(
                    f,
                    "Invalid table qualifier '{}' for column '{}'. Available tables: {}",
                    qualifier,
                    column,
                    available_tables.join(", ")
                )
            }
            ExecutorError::ColumnAlreadyExists(name) => {
                write!(f, "Column '{}' already exists", name)
            }
            ExecutorError::IndexNotFound(name) => write!(f, "Index '{}' not found", name),
            ExecutorError::IndexAlreadyExists(name) => write!(f, "Index '{}' already exists", name),
            ExecutorError::InvalidIndexDefinition(msg) => {
                write!(f, "Invalid index definition: {}", msg)
            }
            ExecutorError::TriggerNotFound(name) => write!(f, "Trigger '{}' not found", name),
            ExecutorError::TriggerAlreadyExists(name) => {
                write!(f, "Trigger '{}' already exists", name)
            }
            ExecutorError::SchemaNotFound(name) => write!(f, "Schema '{}' not found", name),
            ExecutorError::SchemaAlreadyExists(name) => {
                write!(f, "Schema '{}' already exists", name)
            }
            ExecutorError::SchemaNotEmpty(name) => {
                write!(f, "Cannot drop schema '{}': schema is not empty", name)
            }
            ExecutorError::RoleNotFound(name) => write!(f, "Role '{}' not found", name),
            ExecutorError::TypeNotFound(name) => write!(f, "Type '{}' not found", name),
            ExecutorError::TypeAlreadyExists(name) => write!(f, "Type '{}' already exists", name),
            ExecutorError::TypeInUse(name) => {
                write!(f, "Cannot drop type '{}': type is still in use", name)
            }
            ExecutorError::DependentPrivilegesExist(msg) => {
                write!(f, "Dependent privileges exist: {}", msg)
            }
            ExecutorError::PermissionDenied { role, privilege, object } => {
                write!(
                    f,
                    "Permission denied: role '{}' lacks {} privilege on {}",
                    role, privilege, object
                )
            }
            ExecutorError::ColumnIndexOutOfBounds { index } => {
                write!(f, "Column index {} out of bounds", index)
            }
            ExecutorError::TypeMismatch { left, op, right } => {
                write!(f, "Type mismatch: {:?} {} {:?}", left, op, right)
            }
            ExecutorError::DivisionByZero => write!(f, "Division by zero"),
            ExecutorError::InvalidWhereClause(msg) => write!(f, "Invalid WHERE clause: {}", msg),
            ExecutorError::UnsupportedExpression(msg) => {
                write!(f, "Unsupported expression: {}", msg)
            }
            ExecutorError::UnsupportedFeature(msg) => write!(f, "Unsupported feature: {}", msg),
            ExecutorError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            ExecutorError::SubqueryReturnedMultipleRows { expected, actual } => {
                write!(f, "Scalar subquery returned {} rows, expected {}", actual, expected)
            }
            ExecutorError::SubqueryColumnCountMismatch { expected, actual } => {
                write!(f, "Subquery returned {} columns, expected {}", actual, expected)
            }
            ExecutorError::ColumnCountMismatch { expected, provided } => {
                write!(
                    f,
                    "Derived column list has {} columns but query produces {} columns",
                    provided, expected
                )
            }
            ExecutorError::CastError { from_type, to_type } => {
                write!(f, "Cannot cast {} to {}", from_type, to_type)
            }
            ExecutorError::TypeConversionError { from, to } => {
                write!(f, "Cannot convert {} to {}", from, to)
            }
            ExecutorError::ConstraintViolation(msg) => {
                write!(f, "Constraint violation: {}", msg)
            }
            ExecutorError::MultiplePrimaryKeys => {
                write!(f, "Multiple PRIMARY KEY constraints are not allowed")
            }
            ExecutorError::CannotDropColumn(msg) => {
                write!(f, "Cannot drop column: {}", msg)
            }
            ExecutorError::ConstraintNotFound { constraint_name, table_name } => {
                write!(f, "Constraint '{}' not found in table '{}'", constraint_name, table_name)
            }
            ExecutorError::ExpressionDepthExceeded { depth, max_depth } => {
                write!(
                    f,
                    "Expression depth limit exceeded: {} > {} (prevents stack overflow)",
                    depth, max_depth
                )
            }
            ExecutorError::QueryTimeoutExceeded { elapsed_seconds, max_seconds } => {
                write!(f, "Query timeout exceeded: {}s > {}s", elapsed_seconds, max_seconds)
            }
            ExecutorError::RowLimitExceeded { rows_processed, max_rows } => {
                write!(f, "Row processing limit exceeded: {} > {}", rows_processed, max_rows)
            }
            ExecutorError::MemoryLimitExceeded { used_bytes, max_bytes } => {
                write!(
                    f,
                    "Memory limit exceeded: {:.2} GB > {:.2} GB",
                    *used_bytes as f64 / 1024.0 / 1024.0 / 1024.0,
                    *max_bytes as f64 / 1024.0 / 1024.0 / 1024.0
                )
            }
            ExecutorError::VariableNotFound { variable_name, available_variables } => {
                if available_variables.is_empty() {
                    write!(f, "Variable '{}' not found", variable_name)
                } else {
                    write!(
                        f,
                        "Variable '{}' not found. Available variables: {}",
                        variable_name,
                        available_variables.join(", ")
                    )
                }
            }
            ExecutorError::LabelNotFound(name) => {
                write!(f, "Label '{}' not found", name)
            }
            ExecutorError::SelectIntoRowCount { expected, actual } => {
                write!(
                    f,
                    "Procedural SELECT INTO must return exactly {} row, got {} row{}",
                    expected,
                    actual,
                    if *actual == 1 { "" } else { "s" }
                )
            }
            ExecutorError::SelectIntoColumnCount { expected, actual } => {
                write!(
                    f,
                    "Procedural SELECT INTO column count mismatch: {} variable{} but query returned {} column{}",
                    expected,
                    if *expected == 1 { "" } else { "s" },
                    actual,
                    if *actual == 1 { "" } else { "s" }
                )
            }
            ExecutorError::ProcedureNotFound {
                procedure_name,
                schema_name,
                available_procedures,
            } => {
                if available_procedures.is_empty() {
                    write!(
                        f,
                        "Procedure '{}' not found in schema '{}'",
                        procedure_name, schema_name
                    )
                } else {
                    // Check for similar names using simple edit distance
                    let suggestion = find_closest_match(procedure_name, available_procedures);
                    if let Some(similar) = suggestion {
                        write!(
                            f,
                            "Procedure '{}' not found in schema '{}'\nAvailable procedures: {}\nDid you mean '{}'?",
                            procedure_name,
                            schema_name,
                            available_procedures.join(", "),
                            similar
                        )
                    } else {
                        write!(
                            f,
                            "Procedure '{}' not found in schema '{}'\nAvailable procedures: {}",
                            procedure_name,
                            schema_name,
                            available_procedures.join(", ")
                        )
                    }
                }
            }
            ExecutorError::FunctionNotFound { function_name, schema_name, available_functions } => {
                if available_functions.is_empty() {
                    write!(f, "Function '{}' not found in schema '{}'", function_name, schema_name)
                } else {
                    let suggestion = find_closest_match(function_name, available_functions);
                    if let Some(similar) = suggestion {
                        write!(
                            f,
                            "Function '{}' not found in schema '{}'\nAvailable functions: {}\nDid you mean '{}'?",
                            function_name,
                            schema_name,
                            available_functions.join(", "),
                            similar
                        )
                    } else {
                        write!(
                            f,
                            "Function '{}' not found in schema '{}'\nAvailable functions: {}",
                            function_name,
                            schema_name,
                            available_functions.join(", ")
                        )
                    }
                }
            }
            ExecutorError::ParameterCountMismatch {
                routine_name,
                routine_type,
                expected,
                actual,
                parameter_signature,
            } => {
                write!(
                    f,
                    "{} '{}' expects {} parameter{} ({}), got {} argument{}",
                    routine_type,
                    routine_name,
                    expected,
                    if *expected == 1 { "" } else { "s" },
                    parameter_signature,
                    actual,
                    if *actual == 1 { "" } else { "s" }
                )
            }
            ExecutorError::ParameterTypeMismatch {
                parameter_name,
                expected_type,
                actual_type,
                actual_value,
            } => {
                write!(
                    f,
                    "Parameter '{}' expects {}, got {} '{}'",
                    parameter_name, expected_type, actual_type, actual_value
                )
            }
            ExecutorError::TypeError(msg) => {
                write!(f, "Type error: {}", msg)
            }
            ExecutorError::ArgumentCountMismatch { expected, actual } => {
                write!(f, "Argument count mismatch: expected {}, got {}", expected, actual)
            }
            ExecutorError::RecursionLimitExceeded { message, call_stack, max_depth } => {
                write!(f, "Maximum recursion depth ({}) exceeded: {}", max_depth, message)?;
                if !call_stack.is_empty() {
                    write!(f, "\nCall stack:")?;
                    for call in call_stack {
                        write!(f, "\n  {}", call)?;
                    }
                }
                Ok(())
            }
            ExecutorError::FunctionMustReturn => {
                write!(f, "Function must return a value")
            }
            ExecutorError::InvalidControlFlow(msg) => {
                write!(f, "Invalid control flow: {}", msg)
            }
            ExecutorError::InvalidFunctionBody(msg) => {
                write!(f, "Invalid function body: {}", msg)
            }
            ExecutorError::FunctionReadOnlyViolation(msg) => {
                write!(f, "Function read-only violation: {}", msg)
            }
            ExecutorError::ParseError(msg) => {
                write!(f, "Parse error: {}", msg)
            }
            ExecutorError::InvalidExtractField { field, value_type } => {
                write!(f, "Cannot extract {} from {} value", field, value_type)
            }
            ExecutorError::ArrowDowncastError { expected_type, context } => {
                write!(f, "Failed to downcast Arrow array to {} ({})", expected_type, context)
            }
            ExecutorError::ColumnarTypeMismatch { operation, left_type, right_type } => {
                if let Some(right) = right_type {
                    write!(f, "Incompatible types for {}: {} vs {}", operation, left_type, right)
                } else {
                    write!(f, "Incompatible type for {}: {}", operation, left_type)
                }
            }
            ExecutorError::SimdOperationFailed { operation, reason } => {
                write!(f, "SIMD {} failed: {}", operation, reason)
            }
            ExecutorError::ColumnarColumnNotFound { column_index, batch_columns } => {
                write!(
                    f,
                    "Column index {} out of bounds (batch has {} columns)",
                    column_index, batch_columns
                )
            }
            ExecutorError::ColumnarColumnNotFoundByName { column_name } => {
                write!(f, "Column not found: {}", column_name)
            }
            ExecutorError::ColumnarLengthMismatch { context, expected, actual } => {
                write!(
                    f,
                    "Column length mismatch in {}: expected {}, got {}",
                    context, expected, actual
                )
            }
            ExecutorError::UnsupportedArrayType { operation, array_type } => {
                write!(f, "Unsupported array type for {}: {}", operation, array_type)
            }
            ExecutorError::SpatialGeometryError { function_name, message } => {
                write!(f, "{}: {}", function_name, message)
            }
            ExecutorError::SpatialOperationFailed { function_name, message } => {
                write!(f, "{}: {}", function_name, message)
            }
            ExecutorError::SpatialArgumentError { function_name, expected, actual } => {
                write!(f, "{} expects {}, got {}", function_name, expected, actual)
            }
            ExecutorError::CursorAlreadyExists(name) => {
                write!(f, "Cursor '{}' already exists", name)
            }
            ExecutorError::CursorNotFound(name) => {
                write!(f, "Cursor '{}' not found", name)
            }
            ExecutorError::CursorAlreadyOpen(name) => {
                write!(f, "Cursor '{}' is already open", name)
            }
            ExecutorError::CursorNotOpen(name) => {
                write!(f, "Cursor '{}' is not open", name)
            }
            ExecutorError::CursorNotScrollable(name) => {
                write!(f, "Cursor '{}' is not scrollable (SCROLL not specified)", name)
            }
            ExecutorError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ExecutorError {}

impl From<vibesql_storage::StorageError> for ExecutorError {
    fn from(err: vibesql_storage::StorageError) -> Self {
        match err {
            vibesql_storage::StorageError::TableNotFound(name) => {
                ExecutorError::TableNotFound(name)
            }
            vibesql_storage::StorageError::IndexAlreadyExists(name) => {
                ExecutorError::IndexAlreadyExists(name)
            }
            vibesql_storage::StorageError::IndexNotFound(name) => {
                ExecutorError::IndexNotFound(name)
            }
            vibesql_storage::StorageError::ColumnCountMismatch { expected, actual } => {
                ExecutorError::ColumnCountMismatch { expected, provided: actual }
            }
            vibesql_storage::StorageError::ColumnIndexOutOfBounds { index } => {
                ExecutorError::ColumnIndexOutOfBounds { index }
            }
            vibesql_storage::StorageError::CatalogError(msg) => ExecutorError::StorageError(msg),
            vibesql_storage::StorageError::TransactionError(msg) => {
                ExecutorError::StorageError(msg)
            }
            vibesql_storage::StorageError::RowNotFound => {
                ExecutorError::StorageError("Row not found".to_string())
            }
            vibesql_storage::StorageError::NullConstraintViolation { column } => {
                ExecutorError::ConstraintViolation(format!(
                    "NOT NULL constraint violation: column '{}' cannot be NULL",
                    column
                ))
            }
            vibesql_storage::StorageError::TypeMismatch { column, expected, actual } => {
                ExecutorError::StorageError(format!(
                    "Type mismatch in column '{}': expected {}, got {}",
                    column, expected, actual
                ))
            }
            vibesql_storage::StorageError::ColumnNotFound { column_name, table_name } => {
                ExecutorError::StorageError(format!(
                    "Column '{}' not found in table '{}'",
                    column_name, table_name
                ))
            }
            vibesql_storage::StorageError::UniqueConstraintViolation(msg) => {
                ExecutorError::ConstraintViolation(msg)
            }
            vibesql_storage::StorageError::InvalidIndexColumn(msg) => {
                ExecutorError::StorageError(msg)
            }
            vibesql_storage::StorageError::NotImplemented(msg) => {
                ExecutorError::StorageError(format!("Not implemented: {}", msg))
            }
            vibesql_storage::StorageError::IoError(msg) => {
                ExecutorError::StorageError(format!("I/O error: {}", msg))
            }
            vibesql_storage::StorageError::InvalidPageSize { expected, actual } => {
                ExecutorError::StorageError(format!(
                    "Invalid page size: expected {}, got {}",
                    expected, actual
                ))
            }
            vibesql_storage::StorageError::InvalidPageId(page_id) => {
                ExecutorError::StorageError(format!("Invalid page ID: {}", page_id))
            }
            vibesql_storage::StorageError::LockError(msg) => {
                ExecutorError::StorageError(format!("Lock error: {}", msg))
            }
            vibesql_storage::StorageError::MemoryBudgetExceeded { used, budget } => {
                ExecutorError::StorageError(format!(
                    "Memory budget exceeded: using {} bytes, budget is {} bytes",
                    used, budget
                ))
            }
            vibesql_storage::StorageError::NoIndexToEvict => ExecutorError::StorageError(
                "No index available to evict (all indexes are already disk-backed)".to_string(),
            ),
            vibesql_storage::StorageError::Other(msg) => ExecutorError::StorageError(msg),
        }
    }
}

impl From<vibesql_catalog::CatalogError> for ExecutorError {
    fn from(err: vibesql_catalog::CatalogError) -> Self {
        match err {
            vibesql_catalog::CatalogError::TableAlreadyExists(name) => {
                ExecutorError::TableAlreadyExists(name)
            }
            vibesql_catalog::CatalogError::TableNotFound { table_name } => {
                ExecutorError::TableNotFound(table_name)
            }
            vibesql_catalog::CatalogError::ColumnAlreadyExists(name) => {
                ExecutorError::ColumnAlreadyExists(name)
            }
            vibesql_catalog::CatalogError::ColumnNotFound { column_name, table_name } => {
                ExecutorError::ColumnNotFound {
                    column_name,
                    table_name,
                    searched_tables: vec![],
                    available_columns: vec![],
                }
            }
            vibesql_catalog::CatalogError::SchemaNotFound(name) => {
                ExecutorError::SchemaNotFound(name)
            }
            vibesql_catalog::CatalogError::SchemaAlreadyExists(name) => {
                ExecutorError::SchemaAlreadyExists(name)
            }
            vibesql_catalog::CatalogError::SchemaNotEmpty(name) => {
                ExecutorError::SchemaNotEmpty(name)
            }
            vibesql_catalog::CatalogError::RoleAlreadyExists(name) => {
                ExecutorError::StorageError(format!("Role '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::RoleNotFound(name) => ExecutorError::RoleNotFound(name),
            // Advanced SQL:1999 objects
            vibesql_catalog::CatalogError::DomainAlreadyExists(name) => {
                ExecutorError::Other(format!("Domain '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::DomainNotFound(name) => {
                ExecutorError::Other(format!("Domain '{}' not found", name))
            }
            vibesql_catalog::CatalogError::DomainInUse { domain_name, dependent_columns } => {
                ExecutorError::Other(format!(
                    "Domain '{}' is still in use by {} column(s): {}",
                    domain_name,
                    dependent_columns.len(),
                    dependent_columns
                        .iter()
                        .map(|(t, c)| format!("{}.{}", t, c))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            }
            vibesql_catalog::CatalogError::SequenceAlreadyExists(name) => {
                ExecutorError::Other(format!("Sequence '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::SequenceNotFound(name) => {
                ExecutorError::Other(format!("Sequence '{}' not found", name))
            }
            vibesql_catalog::CatalogError::SequenceInUse { sequence_name, dependent_columns } => {
                ExecutorError::Other(format!(
                    "Sequence '{}' is still in use by {} column(s): {}",
                    sequence_name,
                    dependent_columns.len(),
                    dependent_columns
                        .iter()
                        .map(|(t, c)| format!("{}.{}", t, c))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            }
            vibesql_catalog::CatalogError::TypeAlreadyExists(name) => {
                ExecutorError::TypeAlreadyExists(name)
            }
            vibesql_catalog::CatalogError::TypeNotFound(name) => ExecutorError::TypeNotFound(name),
            vibesql_catalog::CatalogError::TypeInUse(name) => ExecutorError::TypeInUse(name),
            vibesql_catalog::CatalogError::CollationAlreadyExists(name) => {
                ExecutorError::Other(format!("Collation '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::CollationNotFound(name) => {
                ExecutorError::Other(format!("Collation '{}' not found", name))
            }
            vibesql_catalog::CatalogError::CharacterSetAlreadyExists(name) => {
                ExecutorError::Other(format!("Character set '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::CharacterSetNotFound(name) => {
                ExecutorError::Other(format!("Character set '{}' not found", name))
            }
            vibesql_catalog::CatalogError::TranslationAlreadyExists(name) => {
                ExecutorError::Other(format!("Translation '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::TranslationNotFound(name) => {
                ExecutorError::Other(format!("Translation '{}' not found", name))
            }
            vibesql_catalog::CatalogError::ViewAlreadyExists(name) => {
                ExecutorError::Other(format!("View '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::ViewNotFound(name) => {
                ExecutorError::Other(format!("View '{}' not found", name))
            }
            vibesql_catalog::CatalogError::ViewInUse { view_name, dependent_views } => {
                ExecutorError::Other(format!(
                    "View or table '{}' is still in use by {} view(s): {}",
                    view_name,
                    dependent_views.len(),
                    dependent_views.join(", ")
                ))
            }
            vibesql_catalog::CatalogError::TriggerAlreadyExists(name) => {
                ExecutorError::TriggerAlreadyExists(name)
            }
            vibesql_catalog::CatalogError::TriggerNotFound(name) => {
                ExecutorError::TriggerNotFound(name)
            }
            vibesql_catalog::CatalogError::AssertionAlreadyExists(name) => {
                ExecutorError::Other(format!("Assertion '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::AssertionNotFound(name) => {
                ExecutorError::Other(format!("Assertion '{}' not found", name))
            }
            vibesql_catalog::CatalogError::FunctionAlreadyExists(name) => {
                ExecutorError::Other(format!("Function '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::FunctionNotFound(name) => {
                ExecutorError::Other(format!("Function '{}' not found", name))
            }
            vibesql_catalog::CatalogError::ProcedureAlreadyExists(name) => {
                ExecutorError::Other(format!("Procedure '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::ProcedureNotFound(name) => {
                ExecutorError::Other(format!("Procedure '{}' not found", name))
            }
            vibesql_catalog::CatalogError::ConstraintAlreadyExists(name) => {
                ExecutorError::ConstraintViolation(format!("Constraint '{}' already exists", name))
            }
            vibesql_catalog::CatalogError::ConstraintNotFound(name) => {
                ExecutorError::ConstraintNotFound {
                    constraint_name: name,
                    table_name: "unknown".to_string(),
                }
            }
            vibesql_catalog::CatalogError::IndexAlreadyExists { index_name, table_name } => {
                ExecutorError::IndexAlreadyExists(format!("{} on table {}", index_name, table_name))
            }
            vibesql_catalog::CatalogError::IndexNotFound { index_name, table_name } => {
                ExecutorError::IndexNotFound(format!("{} on table {}", index_name, table_name))
            }
            vibesql_catalog::CatalogError::CircularForeignKey { table_name, message } => {
                ExecutorError::ConstraintViolation(format!(
                    "Circular foreign key dependency on table '{}': {}",
                    table_name, message
                ))
            }
        }
    }
}
