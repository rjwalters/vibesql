//! Validation logic for CREATE INDEX statements
//!
//! This module handles pre-creation validation including:
//! - Table and schema existence checks
//! - Column validation for indexed columns
//! - Expression validation (determinism checks, column references)
//! - Prefix length validation
//! - Index name collision checks

use vibesql_ast::{CreateIndexStmt, Expression, IndexColumn};
use vibesql_catalog::TableSchema;
use vibesql_storage::Database;

use crate::{
    errors::ExecutorError,
    privilege_checker::PrivilegeChecker,
    sqlite_schema::{is_reserved_object_name, is_sqlite_schema_table},
};

/// Result of validating a CREATE INDEX statement
pub struct ValidationResult {
    /// The table name (without schema prefix)
    pub table_name: String,
    /// Fully qualified table name (schema.table)
    pub qualified_table_name: String,
    /// Cloned table schema for use in index creation
    pub table_schema: TableSchema,
}

/// Resolve the schema a CREATE INDEX statement's new index (and its target
/// table) belongs to, WITHOUT validating that the schema actually exists.
///
/// - When `stmt.schema` is present (`CREATE INDEX schema.i1 ON t(x)`), SQLite qualifies the *index*
///   name with the schema, not the table name — the target table is then resolved within that exact
///   schema, with no temp-shadows-main search. `temp` maps to this session's temp schema (`CREATE
///   INDEX temp.i1` behaves like `CREATE INDEX i1` on a table already known to live in temp). See
///   issue #6366.
/// - Otherwise, `stmt.table_name` may still itself carry a legacy embedded `schema.table` spelling
///   from the pre-#6366 (non-parser) construction path some direct-executor tests use.
/// - With neither, fall back to the pre-existing unqualified resolution (temp shadows main).
///
/// Callers that must reject an unrecognized `stmt.schema` qualifier with
/// SQLite's `unknown database <name>` wording do so themselves — this helper
/// only resolves the *target* schema so it can be shared with the "does an
/// index with this name already exist" checks, which must not error out
/// early for an IF NOT EXISTS-style probe.
fn resolve_index_target_schema(stmt: &CreateIndexStmt, database: &Database) -> String {
    if let Some(explicit_schema) = &stmt.schema {
        if explicit_schema.eq_ignore_ascii_case(vibesql_catalog::TEMP_SCHEMA) {
            database.catalog.temp_schema_name().to_string()
        } else {
            explicit_schema.clone()
        }
    } else if let Some((schema_part, _)) = stmt.table_name.split_once('.') {
        schema_part.to_string()
    } else {
        database
            .catalog
            .resolve_table_schema_name(&stmt.table_name)
            .unwrap_or_else(|| database.catalog.get_current_schema().to_string())
    }
}

/// Validate a CREATE INDEX statement before execution.
///
/// Performs all pre-creation checks:
/// - Parses qualified table name
/// - Checks for sqlite_schema table
/// - Verifies CREATE privilege
/// - Verifies table exists
/// - Validates indexed columns exist
/// - Validates expression determinism
/// - Validates prefix length specifications
/// - Checks for index name collisions
pub fn validate_create_index(
    stmt: &CreateIndexStmt,
    database: &Database,
) -> Result<ValidationResult, ExecutorError> {
    // Resolve the schema this index (and its target table) belongs to, and
    // the target table's unqualified name. See `resolve_index_target_schema`
    // for the full resolution-order rationale (issue #6366).
    let schema_name = resolve_index_target_schema(stmt, database);

    // An explicit `schema.` qualifier on the index name must name an
    // existing (or ATTACHed, #6310) schema; otherwise SQLite rejects it with
    // `unknown database <name>`, echoing the qualifier exactly as written.
    if let Some(explicit_schema) = &stmt.schema {
        if !database.catalog.schema_exists(&schema_name) {
            return Err(ExecutorError::SqliteCompatError(format!(
                "unknown database {}",
                explicit_schema
            )));
        }
    }

    let table_name = if stmt.schema.is_some() {
        // The real parser never produces a dotted `ON table` when the index
        // name itself carries a schema qualifier (`ON` always parses a bare
        // identifier) — SQLite's grammar does not allow a schema-qualified
        // table name in CREATE INDEX at all.
        stmt.table_name.clone()
    } else if let Some((_, table_part)) = stmt.table_name.split_once('.') {
        table_part.to_string()
    } else {
        stmt.table_name.clone()
    };

    // Check if target is sqlite_master/sqlite_schema (read-only system table).
    // `table_name` above already discarded any embedded schema qualifier, so
    // the alias is checked separately via `schema_name` (resolved above,
    // covering both the explicit `stmt.schema`-qualified index route and the
    // legacy embedded `schema.table` spelling): the bare/`main.`-qualified
    // forms match when `schema_name` is `main`, and `<alias>.sqlite_master`/
    // `<alias>.sqlite_schema` match when `schema_name` is a currently-attached
    // alias — matching SQLite's uniform rejection regardless of which live
    // alias qualifies the name (issue #6451). A qualifier that is neither
    // `main` nor a live attachment must NOT trigger this guard; it falls
    // through to ordinary table resolution ("no such table"/"unknown
    // database"), matching SQLite for a stale/unknown alias.
    if is_sqlite_schema_table(&table_name)
        && (schema_name.eq_ignore_ascii_case(vibesql_catalog::DEFAULT_SCHEMA)
            || database.catalog.is_attached_schema(&schema_name))
    {
        return Err(ExecutorError::SqliteSystemTableReadOnly {
            table_name: table_name.clone(),
            operation: "indexed".to_string(),
        });
    }

    // `sqlite_sequence` (AUTOINCREMENT bookkeeping, issue #6173) may not be
    // indexed either, matching sqlite3 3.51.0 (autoinc-1.3.1): `table
    // sqlite_sequence may not be indexed`.
    if crate::autoincrement::is_sqlite_sequence_table(&table_name) {
        return Err(ExecutorError::SqliteSystemTableReadOnly {
            table_name: table_name.clone(),
            operation: "indexed".to_string(),
        });
    }

    // Reject user attempts to create an index with a reserved name. Like
    // CREATE TABLE, SQLite forbids the `sqlite_` prefix for user objects and
    // errors `object name reserved for internal use: <name>` (sqlite3 3.51.0,
    // index-18.4). `stmt.index_name` preserves the user's original spelling, so
    // it is echoed verbatim. The engine's own `sqlite_autoindex_*` indexes are
    // built via a dedicated catalog path that bypasses this validator, so they
    // are unaffected (issue #5614).
    if is_reserved_object_name(&stmt.index_name) {
        return Err(ExecutorError::SqliteCompatError(format!(
            "object name reserved for internal use: {}",
            stmt.index_name
        )));
    }

    // Check CREATE privilege on the schema
    PrivilegeChecker::check_create(database, &schema_name)?;

    // Build fully qualified table name for catalog lookups
    let qualified_table_name = format!("{}.{}", schema_name, table_name);

    // Check if table exists
    if !database.catalog.table_exists(&qualified_table_name) {
        return Err(ExecutorError::TableNotFound(qualified_table_name.clone()));
    }

    // Get table schema to validate columns (clone to avoid borrow issues)
    let table_schema = database
        .catalog
        .get_table(&qualified_table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(qualified_table_name.clone()))?
        .clone();

    // Validate no window functions in index expressions or WHERE clause (#4985)
    // Window functions cannot be used in CREATE INDEX statements
    validate_no_window_functions_in_index(stmt)?;

    // Validate expression determinism
    validate_expression_determinism(&stmt.columns)?;

    // Column-existence validation (indexed columns/expressions and the
    // partial-index WHERE clause) is SQLite's normal bind-time strictness.
    // `PRAGMA writable_schema=ON` is a schema-repair escape hatch that lets
    // an application write a schema object referencing columns that don't
    // (yet) exist — SQLite loads such a schema from disk without complaint
    // (quote.test 2.2: `CREATE INDEX i2 ON t1(x, y, z||"abc")` /
    // `CREATE INDEX i4 ON t1(x) WHERE z="w"` succeed under writable_schema
    // even though `"abc"`/`"w"` don't name columns of `t1`). Gated on the
    // session pragma so default (writable_schema=OFF) strictness is
    // unchanged, matching the analogous CHECK-constraint gate in
    // `create_table.rs`.
    if !database.writable_schema() {
        // Validate that all indexed columns exist in the table
        validate_indexed_columns(&stmt.columns, &table_schema, &qualified_table_name)?;

        // Validate that the partial-index WHERE clause only references
        // existing columns. SQLite resolves this at CREATE INDEX time, not
        // deferred to per-row evaluation at build/DML time (quote.test
        // 2.1.4: `CREATE INDEX i4 ON t1(x) WHERE z="w"` raises `no such
        // column: "w" - should this be a string literal in single-quotes?`
        // immediately, even against an empty table with no rows to scan).
        if let Some(where_expr) = &stmt.where_clause {
            validate_expression_columns(where_expr, &table_schema, &qualified_table_name)?;
        }
    }

    // Validate prefix length specifications
    validate_prefix_lengths(&stmt.columns, &table_schema)?;

    // Check if index already exists (scoped to the target schema — #5540: a
    // temp index and a main index can share a name, so existence is per-schema).
    check_index_exists(stmt, database, &schema_name)?;

    // Check SQLite namespace (tables and indexes share namespace)
    check_namespace_collision(&stmt.index_name, database)?;

    Ok(ValidationResult { table_name, qualified_table_name, table_schema })
}

/// Validate that no window functions are used in index expressions or WHERE clause.
///
/// Window functions are not allowed in CREATE INDEX statements because indexes
/// need deterministic expressions that can be computed from row data alone.
fn validate_no_window_functions_in_index(stmt: &CreateIndexStmt) -> Result<(), ExecutorError> {
    // Check index column expressions for window functions
    for index_col in &stmt.columns {
        if let Some(expr) = index_col.get_expression() {
            if let Some(window_name) = crate::select::find_window_function_in_expression(expr) {
                return Err(ExecutorError::MisuseOfWindowFunction { function_name: window_name });
            }
        }
    }

    // Check the partial-index WHERE clause for window-function misuse (#5091).
    // Window functions are never legal here, even when the rest of the
    // partial-index pipeline is unimplemented.
    if let Some(where_expr) = &stmt.where_clause {
        if let Some(window_name) = crate::select::find_window_function_in_expression(where_expr) {
            return Err(ExecutorError::MisuseOfWindowFunction { function_name: window_name });
        }
    }

    Ok(())
}

/// Validate that expression indexes use deterministic expressions.
fn validate_expression_determinism(columns: &[IndexColumn]) -> Result<(), ExecutorError> {
    for index_col in columns {
        if let Some(expr) = index_col.get_expression() {
            if !crate::evaluator::expression_hash::ExpressionHasher::is_deterministic_for_index(
                expr,
            ) {
                return Err(ExecutorError::UnsupportedFeature(
                    "Expression indexes must use deterministic expressions. \
                     Non-deterministic functions like RANDOM(), CURRENT_TIMESTAMP, etc. \
                     are not allowed."
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// Validate that all indexed columns exist in the table schema.
fn validate_indexed_columns(
    columns: &[IndexColumn],
    table_schema: &TableSchema,
    qualified_table_name: &str,
) -> Result<(), ExecutorError> {
    for index_col in columns {
        // Validate column-based index columns. SQLite's error here is `no
        // such column: <name>`, matching the sibling expression-column path
        // below — with the same "should this be a string literal in
        // single-quotes?" hint appended when the original `CREATE INDEX`
        // column reference was a delimited identifier (double-quoted,
        // backtick, or bracket), e.g. `CREATE INDEX i3 ON t1("w")`
        // (quote.test 2.1.3). A single-quoted string used as an identifier
        // (SQLite's "string as identifier" fallback quirk) does NOT earn the
        // hint — see `IndexColumn::is_quoted`'s doc comment (issue #6560).
        if let Some(col_name) = index_col.column_name() {
            if table_schema.get_column(col_name).is_none() {
                let column_ref = if index_col.is_quoted() {
                    format!("\"{}\" - should this be a string literal in single-quotes?", col_name)
                } else {
                    col_name.to_string()
                };
                return Err(ExecutorError::NoSuchColumn { column_ref });
            }
        }

        // Validate column references in expressions
        if let Some(expr) = index_col.get_expression() {
            validate_expression_columns(expr, table_schema, qualified_table_name)?;
        }
    }
    Ok(())
}

/// Validate prefix length specifications for indexed columns.
fn validate_prefix_lengths(
    columns: &[IndexColumn],
    table_schema: &TableSchema,
) -> Result<(), ExecutorError> {
    for index_col in columns {
        if let Some(prefix_len) = index_col.prefix_length() {
            // Expression indexes don't support prefix lengths
            if index_col.is_expression() {
                return Err(ExecutorError::InvalidIndexDefinition(
                    "Prefix length cannot be specified for expression indexes".to_string(),
                ));
            }

            let col_name = index_col.column_name().unwrap(); // Safe: not an expression

            // Prefix length must be positive
            if prefix_len == 0 {
                return Err(ExecutorError::InvalidIndexDefinition(format!(
                    "Prefix length must be greater than 0 for column '{}'",
                    col_name
                )));
            }

            // Prefix length should only be used with string columns
            let column = table_schema.get_column(col_name).unwrap(); // Safe: validated above
            match column.data_type {
                vibesql_types::DataType::Varchar { .. }
                | vibesql_types::DataType::Character { .. } => {
                    // Valid string types for prefix indexing
                }
                _ => {
                    return Err(ExecutorError::InvalidIndexDefinition(format!(
                        "Prefix length can only be specified for string columns, but column '{}' has type {:?}",
                        col_name, column.data_type
                    )));
                }
            }

            // Reasonable upper limit check (64KB = 65536 characters)
            const MAX_PREFIX_LENGTH: u64 = 65536;
            if prefix_len > MAX_PREFIX_LENGTH {
                return Err(ExecutorError::InvalidIndexDefinition(format!(
                    "Prefix length {} is too large for column '{}' (maximum: {})",
                    prefix_len, col_name, MAX_PREFIX_LENGTH
                )));
            }
        }
    }
    Ok(())
}

/// Check if an index with the same name already exists in the target schema.
///
/// Schema-aware (#5540 B-tree, #5558 spatial): existence is scoped to
/// `schema_name` (the resolved owning schema of the target table) so a `temp.i`
/// index can be created while a `main.i` index already exists, matching SQLite.
fn check_index_exists(
    stmt: &CreateIndexStmt,
    database: &Database,
    schema_name: &str,
) -> Result<(), ExecutorError> {
    let index_name = &stmt.index_name;
    // Probe the exact schema's index via a schema-qualified lookup. A
    // main-schema index keeps a bare storage key, which the storage resolver
    // also reaches through the `main.i` qualifier. Spatial indexes are
    // schema-aware too (#5558), so scope their check to the same qualifier.
    let qualified = format!("{}.{}", schema_name, index_name);
    let index_exists =
        database.get_index(&qualified).is_some() || database.spatial_index_exists(&qualified);

    if index_exists {
        if stmt.if_not_exists {
            // IF NOT EXISTS is handled by returning early with success message
            // The caller should check this case
            return Ok(());
        } else {
            return Err(ExecutorError::IndexAlreadyExists(index_name.clone()));
        }
    }
    Ok(())
}

/// Check that the index name doesn't collide with existing table names.
fn check_namespace_collision(index_name: &str, database: &Database) -> Result<(), ExecutorError> {
    let normalized_index_name = index_name.to_lowercase();
    for schema in database.catalog.list_schemas() {
        let qualified_name = format!("{}.{}", schema, normalized_index_name);
        if database.catalog.table_exists(&qualified_name) {
            // Use SQLite-compatible error message (exact format required for TCL tests)
            return Err(ExecutorError::SqliteCompatError(format!(
                "there is already a table named {}",
                index_name
            )));
        }
    }
    Ok(())
}

/// Check if an index already exists (for IF NOT EXISTS handling).
///
/// Schema-aware (#5540): scoped to the target table's owning schema so a temp
/// index does not short-circuit on a same-named main index (and vice versa).
/// An explicit `schema.` qualifier on the index name (#6366) is honored the
/// same way `validate_create_index` resolves it.
pub fn index_already_exists(stmt: &CreateIndexStmt, database: &Database) -> bool {
    let index_name = &stmt.index_name;
    let schema_name = resolve_index_target_schema(stmt, database);
    let qualified = format!("{}.{}", schema_name, index_name);
    database.get_index(&qualified).is_some() || database.spatial_index_exists(&qualified)
}

/// Validate that all column references in an expression exist in the table schema.
pub fn validate_expression_columns(
    expr: &Expression,
    table_schema: &TableSchema,
    qualified_table_name: &str,
) -> Result<(), ExecutorError> {
    match expr {
        Expression::ColumnRef(col_id) => {
            let col_name = col_id.column_canonical();
            if table_schema.get_column(col_name).is_none() {
                // SQLite appends "- should this be a string literal in
                // single-quotes?" when the unresolved reference is an
                // unqualified, delimited (double-quoted/backtick/bracket)
                // identifier — the same ambiguity the CHECK-constraint
                // resolver flags (quote.test 2.1.2/2.1.4: `CREATE INDEX ...
                // ON t1(x, y, z||"abc")` / `... WHERE z="w"` under
                // SQLITE_DBCONFIG_DQS_DDL=0).
                return Err(ExecutorError::NoSuchColumn {
                    column_ref: crate::constraint_validator::quoted_column_display(col_id),
                });
            }
            Ok(())
        }
        Expression::BinaryOp { left, right, .. } => {
            validate_expression_columns(left, table_schema, qualified_table_name)?;
            validate_expression_columns(right, table_schema, qualified_table_name)
        }
        Expression::UnaryOp { expr, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)
        }
        Expression::Function { args, .. } => {
            for arg in args {
                validate_expression_columns(arg, table_schema, qualified_table_name)?;
            }
            Ok(())
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_expression_columns(op, table_schema, qualified_table_name)?;
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    validate_expression_columns(cond, table_schema, qualified_table_name)?;
                }
                validate_expression_columns(&clause.result, table_schema, qualified_table_name)?;
            }
            if let Some(else_expr) = else_result {
                validate_expression_columns(else_expr, table_schema, qualified_table_name)?;
            }
            Ok(())
        }
        Expression::Cast { expr, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)
        }
        Expression::Collate { expr, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)
        }
        Expression::IsNull { expr, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)
        }
        Expression::Between { expr, low, high, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)?;
            validate_expression_columns(low, table_schema, qualified_table_name)?;
            validate_expression_columns(high, table_schema, qualified_table_name)
        }
        Expression::InList { expr, values, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)?;
            for item in values {
                validate_expression_columns(item, table_schema, qualified_table_name)?;
            }
            Ok(())
        }
        Expression::Like { expr, pattern, .. } => {
            validate_expression_columns(expr, table_schema, qualified_table_name)?;
            validate_expression_columns(pattern, table_schema, qualified_table_name)
        }
        // Literals and other self-contained expressions don't reference columns
        Expression::Literal(_)
        | Expression::Wildcard
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_) => Ok(()),
        // For other expression types, conservatively allow them
        // (they may be validated during evaluation)
        _ => Ok(()),
    }
}
