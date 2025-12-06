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
        use vibesql_l10n::vibe_msg;
        match self {
            ExecutorError::TableNotFound(name) => {
                write!(f, "{}", vibe_msg!("executor-table-not-found", name = name.as_str()))
            }
            ExecutorError::TableAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("executor-table-already-exists", name = name.as_str()))
            }
            ExecutorError::ColumnNotFound {
                column_name,
                table_name,
                searched_tables,
                available_columns,
            } => {
                if searched_tables.is_empty() {
                    write!(f, "{}", vibe_msg!("executor-column-not-found-simple", column_name = column_name.as_str(), table_name = table_name.as_str()))
                } else if available_columns.is_empty() {
                    let searched = searched_tables.join(", ");
                    write!(f, "{}", vibe_msg!("executor-column-not-found-searched", column_name = column_name.as_str(), searched_tables = searched.as_str()))
                } else {
                    let searched = searched_tables.join(", ");
                    let available = available_columns.join(", ");
                    write!(f, "{}", vibe_msg!("executor-column-not-found-with-available", column_name = column_name.as_str(), searched_tables = searched.as_str(), available_columns = available.as_str()))
                }
            }
            ExecutorError::InvalidTableQualifier { qualifier, column, available_tables } => {
                let available = available_tables.join(", ");
                write!(f, "{}", vibe_msg!("executor-invalid-table-qualifier", qualifier = qualifier.as_str(), column = column.as_str(), available_tables = available.as_str()))
            }
            ExecutorError::ColumnAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("executor-column-already-exists", name = name.as_str()))
            }
            ExecutorError::IndexNotFound(name) => {
                write!(f, "{}", vibe_msg!("executor-index-not-found", name = name.as_str()))
            }
            ExecutorError::IndexAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("executor-index-already-exists", name = name.as_str()))
            }
            ExecutorError::InvalidIndexDefinition(msg) => {
                write!(f, "{}", vibe_msg!("executor-invalid-index-definition", message = msg.as_str()))
            }
            ExecutorError::TriggerNotFound(name) => {
                write!(f, "{}", vibe_msg!("executor-trigger-not-found", name = name.as_str()))
            }
            ExecutorError::TriggerAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("executor-trigger-already-exists", name = name.as_str()))
            }
            ExecutorError::SchemaNotFound(name) => {
                write!(f, "{}", vibe_msg!("executor-schema-not-found", name = name.as_str()))
            }
            ExecutorError::SchemaAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("executor-schema-already-exists", name = name.as_str()))
            }
            ExecutorError::SchemaNotEmpty(name) => {
                write!(f, "{}", vibe_msg!("executor-schema-not-empty", name = name.as_str()))
            }
            ExecutorError::RoleNotFound(name) => {
                write!(f, "{}", vibe_msg!("executor-role-not-found", name = name.as_str()))
            }
            ExecutorError::TypeNotFound(name) => {
                write!(f, "{}", vibe_msg!("executor-type-not-found", name = name.as_str()))
            }
            ExecutorError::TypeAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("executor-type-already-exists", name = name.as_str()))
            }
            ExecutorError::TypeInUse(name) => {
                write!(f, "{}", vibe_msg!("executor-type-in-use", name = name.as_str()))
            }
            ExecutorError::DependentPrivilegesExist(msg) => {
                write!(f, "{}", vibe_msg!("executor-dependent-privileges-exist", message = msg.as_str()))
            }
            ExecutorError::PermissionDenied { role, privilege, object } => {
                write!(f, "{}", vibe_msg!("executor-permission-denied", role = role.as_str(), privilege = privilege.as_str(), object = object.as_str()))
            }
            ExecutorError::ColumnIndexOutOfBounds { index } => {
                write!(f, "{}", vibe_msg!("executor-column-index-out-of-bounds", index = *index as i64))
            }
            ExecutorError::TypeMismatch { left, op, right } => {
                let left_str = format!("{:?}", left);
                let right_str = format!("{:?}", right);
                write!(f, "{}", vibe_msg!("executor-type-mismatch", left = left_str.as_str(), op = op.as_str(), right = right_str.as_str()))
            }
            ExecutorError::DivisionByZero => {
                write!(f, "{}", vibe_msg!("executor-division-by-zero"))
            }
            ExecutorError::InvalidWhereClause(msg) => {
                write!(f, "{}", vibe_msg!("executor-invalid-where-clause", message = msg.as_str()))
            }
            ExecutorError::UnsupportedExpression(msg) => {
                write!(f, "{}", vibe_msg!("executor-unsupported-expression", message = msg.as_str()))
            }
            ExecutorError::UnsupportedFeature(msg) => {
                write!(f, "{}", vibe_msg!("executor-unsupported-feature", message = msg.as_str()))
            }
            ExecutorError::StorageError(msg) => {
                write!(f, "{}", vibe_msg!("executor-storage-error", message = msg.as_str()))
            }
            ExecutorError::SubqueryReturnedMultipleRows { expected, actual } => {
                write!(f, "{}", vibe_msg!("executor-subquery-returned-multiple-rows", expected = *expected as i64, actual = *actual as i64))
            }
            ExecutorError::SubqueryColumnCountMismatch { expected, actual } => {
                write!(f, "{}", vibe_msg!("executor-subquery-column-count-mismatch", expected = *expected as i64, actual = *actual as i64))
            }
            ExecutorError::ColumnCountMismatch { expected, provided } => {
                write!(f, "{}", vibe_msg!("executor-column-count-mismatch", expected = *expected as i64, provided = *provided as i64))
            }
            ExecutorError::CastError { from_type, to_type } => {
                write!(f, "{}", vibe_msg!("executor-cast-error", from_type = from_type.as_str(), to_type = to_type.as_str()))
            }
            ExecutorError::TypeConversionError { from, to } => {
                write!(f, "{}", vibe_msg!("executor-type-conversion-error", from = from.as_str(), to = to.as_str()))
            }
            ExecutorError::ConstraintViolation(msg) => {
                write!(f, "{}", vibe_msg!("executor-constraint-violation", message = msg.as_str()))
            }
            ExecutorError::MultiplePrimaryKeys => {
                write!(f, "{}", vibe_msg!("executor-multiple-primary-keys"))
            }
            ExecutorError::CannotDropColumn(msg) => {
                write!(f, "{}", vibe_msg!("executor-cannot-drop-column", message = msg.as_str()))
            }
            ExecutorError::ConstraintNotFound { constraint_name, table_name } => {
                write!(f, "{}", vibe_msg!("executor-constraint-not-found", constraint_name = constraint_name.as_str(), table_name = table_name.as_str()))
            }
            ExecutorError::ExpressionDepthExceeded { depth, max_depth } => {
                write!(f, "{}", vibe_msg!("executor-expression-depth-exceeded", depth = *depth as i64, max_depth = *max_depth as i64))
            }
            ExecutorError::QueryTimeoutExceeded { elapsed_seconds, max_seconds } => {
                write!(f, "{}", vibe_msg!("executor-query-timeout-exceeded", elapsed_seconds = *elapsed_seconds as i64, max_seconds = *max_seconds as i64))
            }
            ExecutorError::RowLimitExceeded { rows_processed, max_rows } => {
                write!(f, "{}", vibe_msg!("executor-row-limit-exceeded", rows_processed = *rows_processed as i64, max_rows = *max_rows as i64))
            }
            ExecutorError::MemoryLimitExceeded { used_bytes, max_bytes } => {
                let used_gb = format!("{:.2}", *used_bytes as f64 / 1024.0 / 1024.0 / 1024.0);
                let max_gb = format!("{:.2}", *max_bytes as f64 / 1024.0 / 1024.0 / 1024.0);
                write!(f, "{}", vibe_msg!("executor-memory-limit-exceeded", used_gb = used_gb.as_str(), max_gb = max_gb.as_str()))
            }
            ExecutorError::VariableNotFound { variable_name, available_variables } => {
                if available_variables.is_empty() {
                    write!(f, "{}", vibe_msg!("executor-variable-not-found-simple", variable_name = variable_name.as_str()))
                } else {
                    let available = available_variables.join(", ");
                    write!(f, "{}", vibe_msg!("executor-variable-not-found-with-available", variable_name = variable_name.as_str(), available_variables = available.as_str()))
                }
            }
            ExecutorError::LabelNotFound(name) => {
                write!(f, "{}", vibe_msg!("executor-label-not-found", name = name.as_str()))
            }
            ExecutorError::SelectIntoRowCount { expected, actual } => {
                let plural = if *actual == 1 { "" } else { "s" };
                write!(f, "{}", vibe_msg!("executor-select-into-row-count", expected = *expected as i64, actual = *actual as i64, plural = plural))
            }
            ExecutorError::SelectIntoColumnCount { expected, actual } => {
                let expected_plural = if *expected == 1 { "" } else { "s" };
                let actual_plural = if *actual == 1 { "" } else { "s" };
                write!(f, "{}", vibe_msg!("executor-select-into-column-count", expected = *expected as i64, expected_plural = expected_plural, actual = *actual as i64, actual_plural = actual_plural))
            }
            ExecutorError::ProcedureNotFound {
                procedure_name,
                schema_name,
                available_procedures,
            } => {
                if available_procedures.is_empty() {
                    write!(f, "{}", vibe_msg!("executor-procedure-not-found-simple", procedure_name = procedure_name.as_str(), schema_name = schema_name.as_str()))
                } else {
                    let suggestion = find_closest_match(procedure_name, available_procedures);
                    if let Some(similar) = suggestion {
                        // Use multi-line format with suggestion
                        write!(f, "{}\nAvailable procedures: {}\nDid you mean '{}'?",
                            vibe_msg!("executor-procedure-not-found-simple", procedure_name = procedure_name.as_str(), schema_name = schema_name.as_str()),
                            available_procedures.join(", "),
                            similar)
                    } else {
                        write!(f, "{}\nAvailable procedures: {}",
                            vibe_msg!("executor-procedure-not-found-simple", procedure_name = procedure_name.as_str(), schema_name = schema_name.as_str()),
                            available_procedures.join(", "))
                    }
                }
            }
            ExecutorError::FunctionNotFound { function_name, schema_name, available_functions } => {
                if available_functions.is_empty() {
                    write!(f, "{}", vibe_msg!("executor-function-not-found-simple", function_name = function_name.as_str(), schema_name = schema_name.as_str()))
                } else {
                    let suggestion = find_closest_match(function_name, available_functions);
                    if let Some(similar) = suggestion {
                        write!(f, "{}\nAvailable functions: {}\nDid you mean '{}'?",
                            vibe_msg!("executor-function-not-found-simple", function_name = function_name.as_str(), schema_name = schema_name.as_str()),
                            available_functions.join(", "),
                            similar)
                    } else {
                        write!(f, "{}\nAvailable functions: {}",
                            vibe_msg!("executor-function-not-found-simple", function_name = function_name.as_str(), schema_name = schema_name.as_str()),
                            available_functions.join(", "))
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
                let expected_plural = if *expected == 1 { "" } else { "s" };
                let actual_plural = if *actual == 1 { "" } else { "s" };
                write!(f, "{}", vibe_msg!("executor-parameter-count-mismatch",
                    routine_type = routine_type.as_str(),
                    routine_name = routine_name.as_str(),
                    expected = *expected as i64,
                    expected_plural = expected_plural,
                    parameter_signature = parameter_signature.as_str(),
                    actual = *actual as i64,
                    actual_plural = actual_plural))
            }
            ExecutorError::ParameterTypeMismatch {
                parameter_name,
                expected_type,
                actual_type,
                actual_value,
            } => {
                write!(f, "{}", vibe_msg!("executor-parameter-type-mismatch",
                    parameter_name = parameter_name.as_str(),
                    expected_type = expected_type.as_str(),
                    actual_type = actual_type.as_str(),
                    actual_value = actual_value.as_str()))
            }
            ExecutorError::TypeError(msg) => {
                write!(f, "{}", vibe_msg!("executor-type-error", message = msg.as_str()))
            }
            ExecutorError::ArgumentCountMismatch { expected, actual } => {
                write!(f, "{}", vibe_msg!("executor-argument-count-mismatch", expected = *expected as i64, actual = *actual as i64))
            }
            ExecutorError::RecursionLimitExceeded { message, call_stack, max_depth } => {
                write!(f, "{}", vibe_msg!("executor-recursion-limit-exceeded", max_depth = *max_depth as i64, message = message.as_str()))?;
                if !call_stack.is_empty() {
                    write!(f, "\n{}", vibe_msg!("executor-recursion-call-stack"))?;
                    for call in call_stack {
                        write!(f, "\n  {}", call)?;
                    }
                }
                Ok(())
            }
            ExecutorError::FunctionMustReturn => {
                write!(f, "{}", vibe_msg!("executor-function-must-return"))
            }
            ExecutorError::InvalidControlFlow(msg) => {
                write!(f, "{}", vibe_msg!("executor-invalid-control-flow", message = msg.as_str()))
            }
            ExecutorError::InvalidFunctionBody(msg) => {
                write!(f, "{}", vibe_msg!("executor-invalid-function-body", message = msg.as_str()))
            }
            ExecutorError::FunctionReadOnlyViolation(msg) => {
                write!(f, "{}", vibe_msg!("executor-function-read-only-violation", message = msg.as_str()))
            }
            ExecutorError::ParseError(msg) => {
                write!(f, "{}", vibe_msg!("executor-parse-error", message = msg.as_str()))
            }
            ExecutorError::InvalidExtractField { field, value_type } => {
                write!(f, "{}", vibe_msg!("executor-invalid-extract-field", field = field.as_str(), value_type = value_type.as_str()))
            }
            ExecutorError::ArrowDowncastError { expected_type, context } => {
                write!(f, "{}", vibe_msg!("executor-arrow-downcast-error", expected_type = expected_type.as_str(), context = context.as_str()))
            }
            ExecutorError::ColumnarTypeMismatch { operation, left_type, right_type } => {
                if let Some(right) = right_type {
                    write!(f, "{}", vibe_msg!("executor-columnar-type-mismatch-binary", operation = operation.as_str(), left_type = left_type.as_str(), right_type = right.as_str()))
                } else {
                    write!(f, "{}", vibe_msg!("executor-columnar-type-mismatch-unary", operation = operation.as_str(), left_type = left_type.as_str()))
                }
            }
            ExecutorError::SimdOperationFailed { operation, reason } => {
                write!(f, "{}", vibe_msg!("executor-simd-operation-failed", operation = operation.as_str(), reason = reason.as_str()))
            }
            ExecutorError::ColumnarColumnNotFound { column_index, batch_columns } => {
                write!(f, "{}", vibe_msg!("executor-columnar-column-not-found", column_index = *column_index as i64, batch_columns = *batch_columns as i64))
            }
            ExecutorError::ColumnarColumnNotFoundByName { column_name } => {
                write!(f, "{}", vibe_msg!("executor-columnar-column-not-found-by-name", column_name = column_name.as_str()))
            }
            ExecutorError::ColumnarLengthMismatch { context, expected, actual } => {
                write!(f, "{}", vibe_msg!("executor-columnar-length-mismatch", context = context.as_str(), expected = *expected as i64, actual = *actual as i64))
            }
            ExecutorError::UnsupportedArrayType { operation, array_type } => {
                write!(f, "{}", vibe_msg!("executor-unsupported-array-type", operation = operation.as_str(), array_type = array_type.as_str()))
            }
            ExecutorError::SpatialGeometryError { function_name, message } => {
                write!(f, "{}", vibe_msg!("executor-spatial-geometry-error", function_name = function_name.as_str(), message = message.as_str()))
            }
            ExecutorError::SpatialOperationFailed { function_name, message } => {
                write!(f, "{}", vibe_msg!("executor-spatial-operation-failed", function_name = function_name.as_str(), message = message.as_str()))
            }
            ExecutorError::SpatialArgumentError { function_name, expected, actual } => {
                write!(f, "{}", vibe_msg!("executor-spatial-argument-error", function_name = function_name.as_str(), expected = expected.as_str(), actual = actual.as_str()))
            }
            ExecutorError::CursorAlreadyExists(name) => {
                write!(f, "{}", vibe_msg!("executor-cursor-already-exists", name = name.as_str()))
            }
            ExecutorError::CursorNotFound(name) => {
                write!(f, "{}", vibe_msg!("executor-cursor-not-found", name = name.as_str()))
            }
            ExecutorError::CursorAlreadyOpen(name) => {
                write!(f, "{}", vibe_msg!("executor-cursor-already-open", name = name.as_str()))
            }
            ExecutorError::CursorNotOpen(name) => {
                write!(f, "{}", vibe_msg!("executor-cursor-not-open", name = name.as_str()))
            }
            ExecutorError::CursorNotScrollable(name) => {
                write!(f, "{}", vibe_msg!("executor-cursor-not-scrollable", name = name.as_str()))
            }
            ExecutorError::Other(msg) => {
                write!(f, "{}", vibe_msg!("executor-other", message = msg.as_str()))
            }
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
