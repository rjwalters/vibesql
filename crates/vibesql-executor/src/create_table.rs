//! CREATE TABLE statement execution

use vibesql_ast::{CreateTableStmt, IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableIdentifier, TableSchema};
use vibesql_storage::Database;
use vibesql_types::{DataType, TypeAffinity};

use crate::{
    constraint_validator::ConstraintValidator, errors::ExecutorError,
    privilege_checker::PrivilegeChecker, SelectExecutor,
};

/// Executor for CREATE TABLE statements
pub struct CreateTableExecutor;

impl CreateTableExecutor {
    /// Execute a CREATE TABLE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The CREATE TABLE statement AST node
    /// * `database` - The database to create the table in
    ///
    /// # Returns
    ///
    /// Success message or error
    ///
    /// # Examples
    ///
    /// ```
    /// use vibesql_ast::{ColumnDef, CreateTableStmt};
    /// use vibesql_executor::CreateTableExecutor;
    /// use vibesql_storage::Database;
    /// use vibesql_types::DataType;
    ///
    /// let mut db = Database::new();
    /// let stmt = CreateTableStmt { temporary: false,
    ///     if_not_exists: false,
    ///     table_name: "users".to_string(),
    ///     columns: vec![
    ///         ColumnDef {
    ///             name: "id".to_string(),
    ///             data_type: DataType::Integer,
    ///             nullable: false,
    ///             constraints: vec![],
    ///             default_value: None,
    ///             comment: None,
    ///             generated_expr: None, is_exact_integer_type: false, type_source: None,
    ///         },
    ///         ColumnDef {
    ///             name: "name".to_string(),
    ///             data_type: DataType::Varchar { max_length: Some(255) },
    ///             nullable: true,
    ///             constraints: vec![],
    ///             default_value: None,
    ///             comment: None,
    ///             generated_expr: None, is_exact_integer_type: false, type_source: None,
    ///         },
    ///     ],
    ///     table_constraints: vec![],
    ///     table_options: vec![],
    ///     quoted: false,
    ///     name_source: None,
    ///     as_query: None, without_rowid: false, strict: false,
    /// };
    ///
    /// let result = CreateTableExecutor::execute(&stmt, &mut db);
    /// assert!(result.is_ok());
    /// ```
    pub fn execute(
        stmt: &CreateTableStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        Self::execute_impl(stmt, database, false, None)
    }

    /// Execute a `CREATE TABLE` statement (user-facing path), recording the
    /// verbatim original source text.
    ///
    /// SQLite stores the byte-for-byte original `CREATE TABLE` statement in
    /// `sqlite_master.sql` (whitespace and formatting preserved). When
    /// `sql_source` is `Some`, it is stamped onto the catalog `TableSchema` so
    /// that `sqlite_master`/`sqlite_schema` returns it verbatim instead of a
    /// reconstruction. The trailing semicolon (if any) is stripped by
    /// `TableSchema::set_sql_source` to match SQLite. Pass `None` when the source
    /// text is unavailable (e.g. AST built programmatically); callers then fall
    /// back to reconstructing the SQL. See issue #5619.
    ///
    /// This is the UNTRUSTED path — it keeps the #5614 reserved-name and #5553
    /// duplicate-column guards. For replaying a persisted dump use
    /// [`execute_for_load_with_source`].
    pub fn execute_with_source(
        stmt: &CreateTableStmt,
        database: &mut Database,
        sql_source: Option<&str>,
    ) -> Result<String, ExecutorError> {
        Self::execute_impl(stmt, database, false, sql_source)
    }

    /// Execute a CREATE TABLE statement on a TRUSTED (engine-internal) path.
    ///
    /// This is used when replaying the engine's own persisted schema (e.g.
    /// `load_sql_dump` / consensus state-machine replay), not for user-issued
    /// DDL. A persisted dump is engine-internal data that was already validated
    /// when first created, so it must always round-trip — it must NOT be
    /// re-rejected by the user-facing conformance guards.
    ///
    /// Specifically this skips:
    /// - the reserved `sqlite_`-prefix guard, so a table that legitimately
    ///   reached the catalog (e.g. a pre-#5614 DB where ALTER TABLE RENAME TO
    ///   `sqlite_x` was still accepted) reloads cleanly instead of bricking the
    ///   database (issue #5614);
    /// - the duplicate-column guard, for the same backward-compat reason — a
    ///   schema that was once persisted must remain loadable.
    ///
    /// All other validation (namespace collisions, etc.) still applies, matching
    /// the prior behavior of the load path for #5613.
    pub fn execute_for_load(
        stmt: &CreateTableStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        Self::execute_impl(stmt, database, true, None)
    }

    /// Trusted load path (see [`execute_for_load`]) that ALSO records the
    /// verbatim original `CREATE TABLE` source text for `sqlite_master.sql`.
    ///
    /// This is the entry point used when reloading a persisted `.sql` dump: it
    /// must bypass the user-facing guards (#5614/#5553) so a previously
    /// persisted schema always round-trips, while still preserving the byte-for-
    /// byte original source so `sqlite_master.sql` keeps the user's formatting
    /// across a save/reload cycle (issue #5619).
    pub fn execute_for_load_with_source(
        stmt: &CreateTableStmt,
        database: &mut Database,
        sql_source: Option<&str>,
    ) -> Result<String, ExecutorError> {
        Self::execute_impl(stmt, database, true, sql_source)
    }

    fn execute_impl(
        stmt: &CreateTableStmt,
        database: &mut Database,
        trusted: bool,
        sql_source: Option<&str>,
    ) -> Result<String, ExecutorError> {
        // Parse qualified table name (schema.table or just table)
        // For TEMP tables, use the session-specific temp schema (SQLite compatibility)
        let (schema_name, table_name, identifier) = if stmt.temporary {
            // Temporary table - use session-specific temp schema
            // Each session gets its own temp schema (e.g., "temp_1", "temp_2")
            // for isolation between database connections
            let temp_schema = database.catalog.temp_schema_name();
            let id = TableIdentifier::qualified(temp_schema, false, &stmt.table_name, stmt.quoted);
            (temp_schema.to_string(), stmt.table_name.clone(), id)
        } else if let Some((schema_part, table_part)) = stmt.table_name.split_once('.') {
            // Schema-qualified table name - use qualified identifier
            // Note: We use stmt.quoted for both parts since the parser combined them
            // In a future iteration, CREATE TABLE could also store schema/table quoted status separately
            if schema_part.eq_ignore_ascii_case(vibesql_catalog::TEMP_SCHEMA) {
                // SQLite compatibility: the "temp" schema qualifier maps to this
                // session's temp schema, so `CREATE TABLE temp.t(...)` creates a
                // temporary table exactly like `CREATE TEMP TABLE t(...)`. Without
                // this mapping the raw "temp" schema name is looked up verbatim and
                // fails with SchemaNotFound("temp").
                let temp_schema = database.catalog.temp_schema_name();
                let id = TableIdentifier::qualified(temp_schema, false, table_part, stmt.quoted);
                (temp_schema.to_string(), table_part.to_string(), id)
            } else {
                let id =
                    TableIdentifier::qualified(schema_part, stmt.quoted, table_part, stmt.quoted);
                (schema_part.to_string(), table_part.to_string(), id)
            }
        } else {
            // Simple table name - use current schema
            let id = TableIdentifier::new(&stmt.table_name, stmt.quoted);
            (database.catalog.get_current_schema().to_string(), stmt.table_name.clone(), id)
        };

        // The reserved-name and duplicate-column guards are USER-facing
        // conformance checks (issue #5614). They are skipped on the trusted
        // load/replay path so the engine's own persisted dump always reloads —
        // see `execute_for_load`. SQLite checks these BEFORE resolving the
        // database qualifier (e_createtable-1.1.1.3/1.1.1.4:
        // `CREATE TABLE auxa."sqlite__"(x, y)` reports the reserved-name error
        // even though `auxa` is never attached), so this block runs ahead of
        // the "unknown database" check below.
        if !trusted {
            // Reject user attempts to create a table with a reserved name. SQLite
            // reserves the `sqlite_` prefix for its own schema objects and errors
            // `object name reserved for internal use: <name>` (sqlite3 3.51.0),
            // echoing the name exactly as the user spelled it (dequoted, original
            // case — e.g. `SQLITE_foo`). `table_name` already holds that verbatim
            // bare-name spelling. This guard is on the user-facing executor only;
            // the engine's internal sqlite_-prefixed objects are created via
            // dedicated catalog APIs and never pass through here (issue #5614).
            if crate::sqlite_schema::is_reserved_object_name(&table_name) {
                return Err(ExecutorError::SqliteCompatError(format!(
                    "object name reserved for internal use: {}",
                    table_name
                )));
            }

            // Reject duplicate column names. SQLite compares column names
            // case-insensitively (#5553) and errors `duplicate column name: <name>`,
            // echoing the *second* (colliding) occurrence's original spelling —
            // `CREATE TABLE t(a, A)` reports `duplicate column name: A`
            // (sqlite3 3.51.0). The AST preserves each column's as-written casing.
            for (i, col) in stmt.columns.iter().enumerate() {
                if stmt.columns[..i].iter().any(|prev| prev.name.eq_ignore_ascii_case(&col.name)) {
                    return Err(ExecutorError::SqliteCompatError(format!(
                        "duplicate column name: {}",
                        col.name
                    )));
                }
            }
        }

        // SQLite compatibility: a database-qualifier that has not been
        // ATTACHed (or otherwise does not exist) fails with "unknown database
        // <name>" (e.g. `CREATE TABLE george.t1(x)` when `george` was never
        // attached) rather than the internal catalog error text. This mirrors
        // sqlite3's wording for qualified DDL against an unknown database
        // (e_createtable-1.2.1.x). ATTACH itself is not supported by VibeSQL
        // (single-file engine), so any non-main/non-temp qualifier the user
        // supplies reaches this path.
        if !database.catalog.schema_exists(&schema_name) {
            return Err(ExecutorError::SqliteCompatError(format!(
                "unknown database {}",
                schema_name
            )));
        }

        // Check CREATE privilege on the schema
        PrivilegeChecker::check_create(database, &schema_name)?;

        // Handle CREATE TABLE AS SELECT syntax
        if let Some(query) = &stmt.as_query {
            return Self::execute_create_as_select(
                database,
                &table_name,
                &schema_name,
                identifier,
                stmt.if_not_exists,
                stmt.name_source.as_deref(),
                query,
            );
        }

        // Check if table already exists in the target schema using SQL:1999 identifier semantics
        // For CREATE TABLE, we only check the target schema (not temp schema)
        // Temp tables can shadow main tables, but we allow creating in main even if temp exists
        // Use table_exists_by_identifier which keys on the canonical (case-folded)
        // form. SQLite case-folds identifiers regardless of quoting (issue #5553),
        // so `CREATE TABLE "TBL1"` collides with an existing `tbl1`.
        if database.catalog.table_exists_by_identifier(&identifier) {
            if stmt.if_not_exists {
                // IF NOT EXISTS - silently return success without creating the table
                return Ok(format!(
                    "Table '{}' already exists in schema '{}' (skipped)",
                    table_name, schema_name
                ));
            }
            // A bare `CREATE TABLE` over an existing name is an error. SQLite
            // echoes the table name *exactly as written in the source*,
            // preserving its quoting form and casing — `table "tbl1" already
            // exists` / `table [tbl1] already exists` (sqlite3 3.51.0). The
            // parser captures that verbatim spelling in `name_source`; fall back
            // to the schema-qualified normalized name when it is unavailable
            // (e.g. programmatically-built AST). Mirrors the CREATE TRIGGER
            // mechanism from #5538.
            let echoed = stmt
                .name_source
                .clone()
                .unwrap_or_else(|| format!("{}.{}", schema_name, identifier.display()));
            return Err(ExecutorError::TableAlreadyExists(echoed));
        }

        // SQLite places tables, indexes, and views in ONE object namespace per
        // schema. A `CREATE TABLE <name>` must therefore fail when an index of
        // that name already exists, reporting `there is already an index named
        // <name>` (sqlite3 3.51.0). This mirrors the symmetric CREATE INDEX
        // check (`there is already a table named <name>`) and—critically—keeps
        // the on-disk schema reloadable: previously VibeSQL accepted the
        // colliding table, then the DDL-replay on the next open hit its own
        // collision and bricked the database (issue #5613).
        //
        // The check is schema-aware (a temp index does not collide with a main
        // table) and case-insensitive (#5553 identifier folding), reusing the
        // canonical identifier's table-name component for the comparison.
        //
        // Note: IF NOT EXISTS does NOT suppress this cross-type collision —
        // sqlite3 still raises `there is already an index named X` (verified
        // against 3.51.0). IF NOT EXISTS only silences a same-type (table)
        // collision, handled above.
        if database.catalog.index_name_exists_in_schema(&schema_name, identifier.table_canonical())
        {
            // Echo the name as the user spelled it (sqlite3 prints the bare
            // index name without schema qualification).
            return Err(ExecutorError::SqliteCompatError(format!(
                "there is already an index named {}",
                table_name
            )));
        }

        // Check for AUTO_INCREMENT constraints
        // MySQL allows only one AUTO_INCREMENT column per table
        let auto_increment_columns: Vec<&str> = stmt
            .columns
            .iter()
            .filter(|col_def| {
                col_def
                    .constraints
                    .iter()
                    .any(|c| matches!(c.kind, vibesql_ast::ColumnConstraintKind::AutoIncrement))
            })
            .map(|col_def| col_def.name.as_str())
            .collect();

        if auto_increment_columns.len() > 1 {
            return Err(ExecutorError::ConstraintViolation(
                "Only one AUTO_INCREMENT column allowed per table".to_string(),
            ));
        }

        // Convert AST ColumnDef → Catalog ColumnSchema
        let mut columns: Vec<ColumnSchema> = stmt
            .columns
            .iter()
            .map(|col_def| {
                // AUTO_INCREMENT/AUTOINCREMENT columns get no synthetic default
                // expression: NULL-fill for the INTEGER PRIMARY KEY (rowid
                // alias) column is handled by the dedicated
                // `apply_default_values_with_batch_context` IPK path, which
                // consults the real `sqlite_sequence` table for AUTOINCREMENT
                // tables (issue #6173) — this runs BEFORE the generic
                // default-value pass ever inspects this column, so a
                // default_value here would be unreachable dead code.
                let default_value = col_def.default_value.as_ref().map(|expr| (**expr).clone());

                // Extract column-level collation from constraints
                let collation = col_def.constraints.iter().find_map(|c| {
                    if let vibesql_ast::ColumnConstraintKind::Collate(coll) = &c.kind {
                        Some(coll.clone())
                    } else {
                        None
                    }
                });

                ColumnSchema {
                    name: col_def.name.clone(),
                    data_type: col_def.data_type.clone(),
                    nullable: col_def.nullable,
                    default_value,
                    generated_expr: col_def.generated_expr.as_ref().map(|expr| (**expr).clone()),
                    collation,
                    is_exact_integer_type: col_def.is_exact_integer_type,
                }
            })
            .collect();

        // Process constraints using the constraint validator
        let constraint_result = ConstraintValidator::process_constraints(
            &table_name,
            &stmt.columns,
            &stmt.table_constraints,
        )?;

        // Apply constraint results to columns (updates nullability)
        ConstraintValidator::apply_to_columns(&mut columns, &constraint_result);

        // Create TableSchema with unqualified name
        let mut table_schema = TableSchema::new(table_name.clone(), columns);

        // Preserve the verbatim original CREATE TABLE text for sqlite_master.sql
        // (SQLite stores it byte-for-byte; see issue #5619). The trailing
        // semicolon is stripped by set_sql_source to match SQLite.
        //
        // One deviation from byte-for-byte: SQLite never records the database
        // qualifier, so `CREATE TABLE main.t1(a, b)` is stored as
        // `CREATE TABLE t1(a, b)` (alter3-1.4/1.5). Strip any `<schema>.` prefix
        // from the table name before recording the source.
        if let Some(src) = sql_source {
            match crate::alter_rewrite::strip_schema_qualifier(src) {
                Some(stripped) => table_schema.set_sql_source(&stripped),
                None => table_schema.set_sql_source(src),
            }
        }

        // Apply WITHOUT ROWID flag from AST (SQLite compatibility)
        table_schema.without_rowid = stmt.without_rowid;

        // Apply STRICT flag + per-column strict types (SQLite STRICT tables,
        // issue #5837). Validation errors (missing / unknown datatype) fire here
        // — before the table is inserted into the catalog — so an invalid STRICT
        // CREATE never leaves a half-formed table behind.
        if stmt.strict {
            let strict_types = crate::strict::classify_strict_columns(&table_name, &stmt.columns)?;
            table_schema.strict = true;
            table_schema.strict_types = strict_types;
        }

        // Apply constraint results to schema (sets PK, unique, and check constraints)
        ConstraintValidator::apply_to_schema(&mut table_schema, &constraint_result);

        // Resolve CHECK constraint column references against the new table.
        // SQLite rejects a CREATE TABLE whose CHECK names an unknown column
        // (check-3.3 `CHECK(q<x)`) or a foreign table (check-3.5 `CHECK(t2.x<x)`)
        // before any table is created, so validate here — after the columns and
        // check constraints are known but before the table is inserted into the
        // catalog.
        crate::constraint_validator::validate_check_constraint_columns(
            &table_schema.name,
            &table_schema.columns,
            &table_schema.check_constraints,
        )?;

        // Reject a circular dependency among generated columns (gencol1-8.20,
        // issue #6173): `c1 AS(c0+c2), c2 AS(c1)`. Checked here too, before the
        // table is inserted into the catalog, so a cyclic definition never
        // creates a half-formed table that later blows up one row at a time.
        crate::constraint_validator::validate_generated_column_cycles(
            &table_schema.name,
            &table_schema.columns,
        )?;

        // WITHOUT ROWID tables must have a PRIMARY KEY (SQLite requirement, Issue #4953).
        // SQLite's exact wording is "PRIMARY KEY missing on table <name>" (verbatim,
        // no prefix) — tableopts.test tableopt-1.1 (issue #6173).
        if stmt.without_rowid && table_schema.primary_key.is_none() {
            return Err(ExecutorError::SqliteCompatError(format!(
                "PRIMARY KEY missing on table {}",
                table_schema.name
            )));
        }

        // AUTOINCREMENT is meaningless on a WITHOUT ROWID table (there is no
        // rowid to auto-generate) — SQLite rejects it at CREATE time (issue
        // #6173, tableopts-1.1b). Checked before rowid-alias detection since a
        // WITHOUT ROWID table never gets a `rowid_alias_column` regardless.
        if stmt.without_rowid && !auto_increment_columns.is_empty() {
            return Err(ExecutorError::SqliteCompatError(
                "AUTOINCREMENT not allowed on WITHOUT ROWID tables".to_string(),
            ));
        }

        // Detect INTEGER PRIMARY KEY for SQLite rowid aliasing (Issue #4536)
        // In SQLite, a single-column PRIMARY KEY with exactly "INTEGER" type is an alias for rowid.
        // The column's value IS the rowid, and SELECT rowid returns this column's value.
        // IMPORTANT: Only exact "INTEGER" qualifies - "INT" does NOT (even though both parse to DataType::Integer)
        if let Some(pk_cols) = &table_schema.primary_key {
            if pk_cols.len() == 1 {
                if let Some(col_idx) = table_schema.get_column_index(&pk_cols[0]) {
                    let col = &table_schema.columns[col_idx];
                    // Only exact "INTEGER" type qualifies for rowid aliasing, not "INT"
                    if matches!(col.data_type, DataType::Integer) && col.is_exact_integer_type {
                        table_schema.set_rowid_alias_column(Some(col_idx));
                    }
                }
            }
        }

        // AUTOINCREMENT requires the column to be *the* single-column INTEGER
        // PRIMARY KEY / rowid alias (SQLite: "AUTOINCREMENT is only allowed on
        // an INTEGER PRIMARY KEY", issue #6173, autoinc-7.2). This also
        // rejects AUTOINCREMENT on a composite PRIMARY KEY or on a PK column
        // whose type isn't exactly INTEGER (e.g. `x TEXT PRIMARY KEY
        // AUTOINCREMENT`), since neither becomes the rowid alias above.
        if let Some(auto_inc_col) = auto_increment_columns.first() {
            let is_rowid_alias = table_schema
                .rowid_alias_column
                .is_some_and(|idx| table_schema.columns[idx].name == *auto_inc_col);
            if !is_rowid_alias {
                return Err(ExecutorError::SqliteCompatError(
                    "AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY".to_string(),
                ));
            }
            table_schema.is_autoincrement = true;
        }

        // Check for STORAGE table option and apply storage format
        for option in &stmt.table_options {
            if let vibesql_ast::TableOption::Storage(format) = option {
                table_schema.set_storage_format(*format);
            }
        }

        // Process foreign key constraints from table_constraints
        for constraint in &stmt.table_constraints {
            if let vibesql_ast::TableConstraintKind::ForeignKey {
                columns: fk_columns,
                references_table,
                references_columns,
                on_delete,
                on_update,
                deferral,
            } = &constraint.kind
            {
                // A table-level FOREIGN KEY(...) clause's child and parent column
                // lists must be the same length when a parent column list is given
                // at all (an omitted parent list defaults to the parent's PRIMARY
                // KEY and is checked elsewhere). SQLite rejects a mismatch at CREATE
                // TABLE time (table.test table-10.9/table-10.10, issue #6173):
                //   FOREIGN KEY(b,c) REFERENCES t4(x)     -- 2 vs 1
                //   FOREIGN KEY(b,c) REFERENCES t4(x,y,z) -- 2 vs 3
                //
                // This count check runs BEFORE resolving the child column names
                // to indices: SQLite reports the column-count mismatch even when
                // one of the child columns does not exist in this table (e_fkey-
                // 54.B: `FOREIGN KEY(c,b) REFERENCES t2(d)` on a table with no
                // column `c` still reports the count mismatch, not "unknown
                // column c"). Only when the parent column list is entirely
                // omitted (no count to compare against) does the unknown-child-
                // column check below get to run first (e_fkey-54.A).
                if !references_columns.is_empty() && references_columns.len() != fk_columns.len() {
                    return Err(ExecutorError::SqliteCompatError(
                        "number of columns in foreign key does not match the number of columns in the referenced table"
                            .to_string(),
                    ));
                }

                // Resolve column indices for FK columns. SQLite reports a
                // dedicated "unknown column ... in foreign key definition"
                // message here (distinct from the generic "no such column"
                // used elsewhere) -- e.g. `FOREIGN KEY(rowid) REFERENCES t1(a)`
                // on a table with no column literally named `rowid`
                // (fkey2-10.2.1).
                let column_indices: Vec<usize> = fk_columns
                    .iter()
                    .map(|col_name| {
                        table_schema.get_column_index(col_name).ok_or_else(|| {
                            ExecutorError::UnknownColumnInForeignKeyDefinition {
                                column_name: col_name.to_string(),
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // Lookup parent table to get parent column indices
                // If the parent table doesn't exist yet, use placeholder indices.
                // SQLite stores FK metadata at CREATE TABLE time even if the
                // referenced table doesn't exist yet.
                let parent_schema = database.catalog.get_table(references_table);

                let parent_column_indices: Vec<usize> = references_columns
                    .iter()
                    .map(|col_name| {
                        parent_schema
                            .as_ref()
                            .and_then(|s| s.get_column_index(col_name))
                            .unwrap_or(0)
                    })
                    .collect();

                // Convert ReferentialAction from AST to catalog type
                let convert_action = |action: &Option<vibesql_ast::ReferentialAction>| match action
                    .as_ref()
                    .unwrap_or(&vibesql_ast::ReferentialAction::NoAction)
                {
                    vibesql_ast::ReferentialAction::Cascade => {
                        vibesql_catalog::ReferentialAction::Cascade
                    }
                    vibesql_ast::ReferentialAction::SetNull => {
                        vibesql_catalog::ReferentialAction::SetNull
                    }
                    vibesql_ast::ReferentialAction::SetDefault => {
                        vibesql_catalog::ReferentialAction::SetDefault
                    }
                    vibesql_ast::ReferentialAction::Restrict => {
                        vibesql_catalog::ReferentialAction::Restrict
                    }
                    vibesql_ast::ReferentialAction::NoAction => {
                        vibesql_catalog::ReferentialAction::NoAction
                    }
                };

                // If no parent columns specified, pad with empty strings to match
                // the FK column count. SQLite stores empty "to" in PRAGMA foreign_key_list
                // when no column list is given after REFERENCES.
                let effective_parent_names = if references_columns.is_empty() {
                    vec![String::new(); fk_columns.len()]
                } else {
                    references_columns.clone()
                };
                let effective_parent_indices = if parent_column_indices.is_empty() {
                    vec![0; fk_columns.len()]
                } else {
                    parent_column_indices
                };

                let (is_deferrable, initially_deferred) = deferral
                    .map(|d| (d.is_deferrable, d.initially_deferred))
                    .unwrap_or((false, false));

                let fk = vibesql_catalog::ForeignKeyConstraint {
                    name: constraint.name.clone(),
                    column_names: fk_columns.clone(),
                    column_indices,
                    parent_table: references_table.clone(),
                    parent_column_names: effective_parent_names,
                    parent_column_indices: effective_parent_indices,
                    on_delete: convert_action(on_delete),
                    on_update: convert_action(on_update),
                    is_deferrable,
                    initially_deferred,
                };

                table_schema.add_foreign_key(fk)?;
            }
        }

        // Process column-level REFERENCES constraints
        // These are parsed as ColumnConstraintKind::References but need to be
        // converted into ForeignKeyConstraint entries in the schema.
        // Note: In SQLite, FK constraints are stored as metadata at CREATE TABLE time
        // even if the referenced table doesn't exist yet. Validation only happens
        // at INSERT/UPDATE/DELETE time when PRAGMA foreign_keys=ON.
        // SQLite assigns FK IDs in reverse column order for column-level constraints,
        // so we collect them first and then add in reverse order.
        let mut column_level_fks = Vec::new();
        for col_def in &stmt.columns {
            for constraint in &col_def.constraints {
                if let vibesql_ast::ColumnConstraintKind::References {
                    table: ref_table,
                    column: ref_column,
                    on_delete,
                    on_update,
                    deferral,
                } = &constraint.kind
                {
                    let col_idx =
                        table_schema.get_column_index(&col_def.name).ok_or_else(|| {
                            ExecutorError::ColumnNotFound {
                                column_name: col_def.name.clone(),
                                table_name: table_name.clone(),
                                searched_tables: vec![table_name.clone()],
                                available_columns: table_schema
                                    .columns
                                    .iter()
                                    .map(|c| c.name.clone())
                                    .collect(),
                            }
                        })?;

                    // Try to lookup parent table to resolve column references.
                    // If the parent table doesn't exist yet, store the FK with
                    // the column name but use placeholder index 0.
                    let parent_schema = database.catalog.get_table(ref_table);

                    let (parent_col_name, parent_col_idx) = if let Some(col) = ref_column {
                        // Explicit column reference
                        let idx = parent_schema
                            .as_ref()
                            .and_then(|s| s.get_column_index(col))
                            .unwrap_or(0);
                        (col.clone(), idx)
                    } else {
                        // No column specified - store empty string to match SQLite behavior.
                        // PRAGMA foreign_key_list shows empty "to" column for implicit PK refs.
                        // The actual PK column is resolved at enforcement time.
                        (String::new(), 0)
                    };

                    let convert_action =
                        |action: &Option<vibesql_ast::ReferentialAction>| match action
                            .as_ref()
                            .unwrap_or(&vibesql_ast::ReferentialAction::NoAction)
                        {
                            vibesql_ast::ReferentialAction::Cascade => {
                                vibesql_catalog::ReferentialAction::Cascade
                            }
                            vibesql_ast::ReferentialAction::SetNull => {
                                vibesql_catalog::ReferentialAction::SetNull
                            }
                            vibesql_ast::ReferentialAction::SetDefault => {
                                vibesql_catalog::ReferentialAction::SetDefault
                            }
                            vibesql_ast::ReferentialAction::Restrict => {
                                vibesql_catalog::ReferentialAction::Restrict
                            }
                            vibesql_ast::ReferentialAction::NoAction => {
                                vibesql_catalog::ReferentialAction::NoAction
                            }
                        };

                    let (is_deferrable, initially_deferred) = deferral
                        .map(|d| (d.is_deferrable, d.initially_deferred))
                        .unwrap_or((false, false));

                    let fk = vibesql_catalog::ForeignKeyConstraint {
                        name: constraint.name.clone(),
                        column_names: vec![col_def.name.clone()],
                        column_indices: vec![col_idx],
                        parent_table: ref_table.clone(),
                        parent_column_names: vec![parent_col_name],
                        parent_column_indices: vec![parent_col_idx],
                        on_delete: convert_action(on_delete),
                        on_update: convert_action(on_update),
                        is_deferrable,
                        initially_deferred,
                    };

                    column_level_fks.push(fk);
                }
            }
        }
        // Add column-level FKs in reverse order to match SQLite's FK ID assignment
        for fk in column_level_fks.into_iter().rev() {
            table_schema.add_foreign_key(fk)?;
        }

        // If creating in a non-current schema, temporarily switch to it
        let original_schema = database.catalog.get_current_schema().to_string();
        let needs_schema_switch = schema_name != original_schema;

        if needs_schema_switch {
            database
                .catalog
                .set_current_schema(&schema_name)
                .map_err(|e| ExecutorError::StorageError(format!("Schema error: {:?}", e)))?;
        }

        // Create table using Database API with TableIdentifier (handles both catalog and storage)
        // Note: identifier was created at the start of this function with proper quoted semantics
        let result = database
            .create_table_with_identifier(table_schema.clone(), identifier.clone())
            .map_err(|e| ExecutorError::StorageError(e.to_string()));

        // Check if table creation succeeded before creating indexes
        result?;

        // Auto-create indexes for PRIMARY KEY and UNIQUE constraints
        Self::create_implicit_indexes(database, &table_name, &table_schema)?;

        // Lazily create the real `sqlite_sequence` table (issue #6173) the
        // first time an AUTOINCREMENT table exists in this schema. Runs AFTER
        // the table itself is created so `sqlite_master`/`sqlite_sequence`
        // list in creation order (`t1` then `sqlite_sequence`, matching
        // sqlite3 3.51.0 autoinc-1.2) — `sqlite_sequence` gets no row for
        // this table yet; that happens lazily on the table's first INSERT.
        if table_schema.is_autoincrement {
            // The current schema was switched to the target schema above (for a
            // TEMP table it is this session's temp schema), so resolving the
            // just-created table's owning schema yields the right database —
            // its `sqlite_sequence` is created there, not always in `main`
            // (autoinc-4.x, issue #6173).
            crate::autoincrement::ensure_sqlite_sequence_table(database, &table_name)?;
        }

        // Restore original schema if we switched
        if needs_schema_switch {
            database
                .catalog
                .set_current_schema(&original_schema)
                .map_err(|e| ExecutorError::StorageError(format!("Schema error: {:?}", e)))?;
        }

        // Return success message
        Ok(format!("Table '{}' created successfully in schema '{}'", table_name, schema_name))
    }

    /// Create implicit indexes for PRIMARY KEY and UNIQUE constraints
    ///
    /// Production databases automatically create B-tree indexes for these constraints
    /// to enable efficient query optimization. This function replicates that behavior.
    fn create_implicit_indexes(
        database: &mut Database,
        table_name: &str,
        table_schema: &TableSchema,
    ) -> Result<(), ExecutorError> {
        // Counter for SQLite-compatible auto-index naming: sqlite_autoindex_<table>_<n>
        let mut autoindex_counter = 1;

        // Auto-create PRIMARY KEY index
        // Skip autoindex for INTEGER PRIMARY KEY - it's an alias for rowid
        // and doesn't need a separate B-tree index (matches SQLite behavior)
        if let Some(pk_cols) = &table_schema.primary_key {
            if table_schema.rowid_alias_column.is_none() {
                // WITHOUT ROWID tables: the PK *is* the table B-tree in SQLite and
                // gets no `sqlite_autoindex_*` slot. We still materialize a real
                // unique index to enforce/plan the PK, but give it an internal name
                // outside the autoindex namespace and do NOT consume an ordinal, so
                // UNIQUE constraints on the same table start at `_1` (issue #5882).
                let index_name = if table_schema.without_rowid {
                    format!("{}{}", vibesql_catalog::WITHOUT_ROWID_PK_INDEX_PREFIX, table_name)
                } else {
                    let name = format!("sqlite_autoindex_{}_{}", table_name, autoindex_counter);
                    autoindex_counter += 1;
                    name
                };

                // Create IndexColumn specs for the PRIMARY KEY columns
                let index_columns: Vec<IndexColumn> = pk_cols
                    .iter()
                    .map(|col_name| IndexColumn::Column {
                        column_name: col_name.to_string(),
                        direction: OrderDirection::Asc,
                        prefix_length: None,
                        collation: None,
                    })
                    .collect();

                // Add to catalog first
                let index_metadata = vibesql_catalog::IndexMetadata::new(
                    index_name.clone(),
                    table_name.to_string(),
                    vibesql_catalog::IndexType::BTree,
                    index_columns
                        .iter()
                        .map(|col| {
                            vibesql_catalog::IndexedColumn::new_column(
                                col.expect_column_name().to_string(),
                                vibesql_catalog::SortOrder::Ascending,
                            )
                        })
                        .collect(),
                    true, // unique
                );
                database
                    .catalog
                    .add_index(index_metadata)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                // Create the actual B-tree index
                database
                    .create_index(index_name, table_name.to_string(), true, index_columns)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            }
        }

        // Auto-create UNIQUE constraint indexes
        for unique_cols in &table_schema.unique_constraints {
            let index_name = format!("sqlite_autoindex_{}_{}", table_name, autoindex_counter);
            autoindex_counter += 1;

            // Create IndexColumn specs for the UNIQUE columns
            let index_columns: Vec<IndexColumn> = unique_cols
                .iter()
                .map(|col_name| IndexColumn::Column {
                    column_name: col_name.to_string(),
                    direction: OrderDirection::Asc,
                    prefix_length: None,
                    collation: None,
                })
                .collect();

            // Add to catalog first
            let index_metadata = vibesql_catalog::IndexMetadata::new(
                index_name.clone(),
                table_name.to_string(),
                vibesql_catalog::IndexType::BTree,
                index_columns
                    .iter()
                    .map(|col| {
                        vibesql_catalog::IndexedColumn::new_column(
                            col.expect_column_name().to_string(),
                            vibesql_catalog::SortOrder::Ascending,
                        )
                    })
                    .collect(),
                true, // unique
            );
            database
                .catalog
                .add_index(index_metadata)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

            // Create the actual B-tree index
            database
                .create_index(index_name, table_name.to_string(), true, index_columns)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        }

        Ok(())
    }

    /// Execute CREATE TABLE ... AS SELECT
    ///
    /// Creates a new table with schema derived from the SELECT result,
    /// and populates it with the query results.
    fn execute_create_as_select(
        database: &mut Database,
        table_name: &str,
        schema_name: &str,
        identifier: TableIdentifier,
        if_not_exists: bool,
        name_source: Option<&str>,
        query: &vibesql_ast::SelectStmt,
    ) -> Result<String, ExecutorError> {
        // Check if table already exists
        if database.catalog.table_exists_by_identifier(&identifier) {
            if if_not_exists {
                return Ok(format!(
                    "Table '{}' already exists in schema '{}' (skipped)",
                    table_name, schema_name
                ));
            }
            // Echo the source quoting form when available (see the bare CREATE
            // TABLE path above for rationale); fall back to the schema-qualified
            // normalized name for programmatically-built ASTs.
            let echoed = name_source
                .map(str::to_string)
                .unwrap_or_else(|| format!("{}.{}", schema_name, identifier.display()));
            return Err(ExecutorError::TableAlreadyExists(echoed));
        }

        // Cross-type namespace collision: a CTAS name already used by an index
        // must fail with `there is already an index named <name>`, just like the
        // bare CREATE TABLE path. Keeps the schema reloadable (issue #5613).
        if database.catalog.index_name_exists_in_schema(schema_name, identifier.table_canonical()) {
            return Err(ExecutorError::SqliteCompatError(format!(
                "there is already an index named {}",
                table_name
            )));
        }

        // Execute the SELECT query to get results
        let rows = SelectExecutor::new(database).execute(query)?;

        // Derive column names + SQLite type affinities from the SELECT list
        // (expanding wildcards if needed). The affinity is taken from the source
        // column for a direct column reference and is BLOB/None for any other
        // expression (function, arithmetic, literal, ...). This mirrors how
        // SQLite stamps the generated `sqlite_master.sql` for CREATE TABLE AS
        // SELECT.
        let column_info = Self::derive_ctas_columns(&query.select_list, &query.from, database)?;

        // Derive column schema from the first row (if any) or default to BLOB
        let columns: Vec<ColumnSchema> = column_info
            .iter()
            .enumerate()
            .map(|(idx, (col_name, _affinity))| {
                // Try to infer data type from the first row if available
                let data_type = if !rows.is_empty() && idx < rows[0].values.len() {
                    Self::infer_data_type(&rows[0].values[idx])
                } else {
                    // No rows or column - default to BLOB affinity
                    DataType::BinaryLargeObject
                };

                ColumnSchema {
                    name: col_name.to_string(),
                    data_type,
                    nullable: true, // Default to nullable for CTAS
                    default_value: None,
                    generated_expr: None,
                    collation: None,              // CTAS doesn't preserve collation
                    is_exact_integer_type: false, // CTAS doesn't preserve exact type
                }
            })
            .collect();

        // Create the table schema, stamping the SQLite-faithful
        // `sqlite_master.sql` text. Without this the schema falls back to the
        // auto-generated form which emits unquoted, un-reparseable identifiers
        // (e.g. keyword or apostrophe-bearing column names) and inferred, rather
        // than affinity-derived, type codes.
        let mut table_schema = TableSchema::new(table_name.to_string(), columns);
        table_schema.set_sql_source(Self::generate_ctas_sql_source(table_name, &column_info));

        // Create the table
        database
            .create_table_with_identifier(table_schema, identifier)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        // Insert the result rows into the new table
        let row_count = rows.len();
        for row in rows {
            database
                .insert_row(table_name, row)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        }

        Ok(format!(
            "Table '{}' created successfully in schema '{}' with {} rows",
            table_name, schema_name, row_count
        ))
    }

    /// Derive `(column name, SQLite type affinity)` pairs from a CTAS SELECT
    /// list, expanding wildcards using the database schema.
    ///
    /// The affinity drives the type code emitted into `sqlite_master.sql`:
    /// a direct column reference (including a wildcard-expanded one) carries the
    /// source column's affinity; every other expression carries BLOB/None
    /// affinity (SQLite emits no type at all for non-column expressions).
    fn derive_ctas_columns(
        select_list: &[vibesql_ast::SelectItem],
        from: &Option<vibesql_ast::FromClause>,
        database: &Database,
    ) -> Result<Vec<(String, TypeAffinity)>, ExecutorError> {
        let mut columns = Vec::new();
        let mut counter = 0;

        for item in select_list {
            match item {
                vibesql_ast::SelectItem::Wildcard { .. } => {
                    // A `SELECT * FROM (VALUES ...)` source has no catalog table
                    // to look up. SQLite names the resulting columns `column1`,
                    // `column2`, ... (or the explicit `AS t(a,b)` aliases) and
                    // gives them no declared type (BLOB/None affinity). Handle
                    // this before the table-name path, which cannot resolve a
                    // VALUES clause (values.test 17.1).
                    if let Some(vibesql_ast::FromClause::Values { rows, column_aliases, .. }) = from
                    {
                        let aliases = column_aliases.as_deref().unwrap_or(&[]);
                        let width = if !aliases.is_empty() {
                            aliases.len()
                        } else {
                            rows.first().map(|r| r.len()).unwrap_or(0)
                        };
                        for i in 0..width {
                            let name = aliases
                                .get(i)
                                .cloned()
                                .unwrap_or_else(|| format!("column{}", i + 1));
                            columns.push((name, TypeAffinity::None));
                        }
                        continue;
                    }
                    // Expand wildcard using the FROM clause tables
                    let table_names = Self::get_table_names_from_from(from)?;
                    for table_name in table_names {
                        if let Some(schema) = database.catalog.get_table(&table_name) {
                            for col in &schema.columns {
                                columns.push((col.name.clone(), col.data_type.sqlite_affinity()));
                            }
                        } else if let Some(view) = database.catalog.get_view(&table_name) {
                            // A view has no declared column affinity of its own
                            // (SQLite: `CREATE TABLE ... AS SELECT * FROM <view>`
                            // stamps every column with no type), so every
                            // expanded column gets `TypeAffinity::None` — same
                            // as the `resolve_ctas_affinity` fallback below for
                            // a bare `SELECT viewcol` (issue #6172,
                            // affinity3.test 200).
                            for name in Self::view_output_column_names(view)? {
                                columns.push((name, TypeAffinity::None));
                            }
                        } else {
                            return Err(ExecutorError::TableNotFound(table_name));
                        }
                    }
                }
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                    // Expand table.* using the specific table's schema
                    if let Some(schema) = database.catalog.get_table(qualifier) {
                        for col in &schema.columns {
                            columns.push((col.name.clone(), col.data_type.sqlite_affinity()));
                        }
                    } else if let Some(view) = database.catalog.get_view(qualifier) {
                        for name in Self::view_output_column_names(view)? {
                            columns.push((name, TypeAffinity::None));
                        }
                    } else {
                        return Err(ExecutorError::TableNotFound(qualifier.clone()));
                    }
                }
                vibesql_ast::SelectItem::Expression { expr, alias, source_text } => {
                    let name = if let Some(alias) = alias {
                        alias.clone()
                    } else {
                        // Derive a name from the expression, preferring the
                        // original SQL source text so expression columns are
                        // named after the expression the way SQLite does
                        // (e.g. `max(b+c)`, `b+c`) rather than `column1`.
                        Self::derive_column_name_from_expr(expr, source_text, &mut counter)
                    };
                    let affinity = Self::resolve_ctas_affinity(expr, from, database);
                    columns.push((name, affinity));
                }
            }
        }

        Ok(columns)
    }

    /// Determine the SQLite type affinity a CTAS result column inherits.
    ///
    /// Only a bare column reference (optionally aliased) inherits its source
    /// column's affinity; SQLite assigns no declared type — hence BLOB/None
    /// affinity — to any other expression. When the reference cannot be resolved
    /// (unknown table/alias, computed source) we also fall back to None.
    fn resolve_ctas_affinity(
        expr: &vibesql_ast::Expression,
        from: &Option<vibesql_ast::FromClause>,
        database: &Database,
    ) -> TypeAffinity {
        let vibesql_ast::Expression::ColumnRef(col_id) = expr else {
            return TypeAffinity::None;
        };
        let column = col_id.column_canonical();

        let table_names = Self::get_table_names_from_from(from).unwrap_or_default();
        // Prefer the qualifier's table when present, but fall back to a scan of
        // every FROM table (handles table aliases, which the qualifier names but
        // the catalog does not).
        for table_name in &table_names {
            if let Some(schema) = database.catalog.get_table(table_name) {
                if let Some(col) = schema.columns.iter().find(|c| c.name == column) {
                    return col.data_type.sqlite_affinity();
                }
            }
        }
        TypeAffinity::None
    }

    /// Build the `sqlite_master.sql` text for a CREATE TABLE ... AS SELECT,
    /// replicating SQLite's `createTableStmt`: identifiers are double-quoted
    /// only when required, each column carries an affinity-derived type code
    /// (`""`/`TEXT`/`NUM`/`INT`/`REAL`), and the layout is compact or pretty
    /// depending on a length heuristic.
    fn generate_ctas_sql_source(table_name: &str, columns: &[(String, TypeAffinity)]) -> String {
        // Length heuristic (SQLite: n<50 → single line, else one column per
        // line). `ctas_ident_length` matches SQLite's `identLength`, which
        // always budgets for surrounding quotes plus any doubled `"`.
        let mut n = 0usize;
        for (name, _) in columns {
            n += Self::ctas_ident_length(name) + 5;
        }
        n += Self::ctas_ident_length(table_name);
        let (sep_first, sep_rest, end) =
            if n < 50 { ("", ",", ")") } else { ("\n  ", ",\n  ", "\n)") };

        let mut sql = String::from("CREATE TABLE ");
        Self::ctas_ident_put(&mut sql, table_name);
        sql.push('(');
        for (i, (name, affinity)) in columns.iter().enumerate() {
            sql.push_str(if i == 0 { sep_first } else { sep_rest });
            Self::ctas_ident_put(&mut sql, name);
            sql.push_str(Self::ctas_affinity_type(*affinity));
        }
        sql.push_str(end);
        sql
    }

    /// SQLite `azType[]`: the type token (with its leading space) emitted for a
    /// CTAS column of the given affinity. BLOB/None emits no type.
    fn ctas_affinity_type(affinity: TypeAffinity) -> &'static str {
        match affinity {
            TypeAffinity::None => "",
            TypeAffinity::Text => " TEXT",
            TypeAffinity::Numeric => " NUM",
            TypeAffinity::Integer => " INT",
            TypeAffinity::Real => " REAL",
        }
    }

    /// SQLite `identLength`: char count + one per embedded `"` (doubled on
    /// output) + 2 for the surrounding quotes (always budgeted).
    fn ctas_ident_length(name: &str) -> usize {
        name.chars().count() + name.chars().filter(|c| *c == '"').count() + 2
    }

    /// SQLite `identPut`: append `name`, double-quoting it only when required —
    /// empty, starts with a digit, contains a non alphanumeric/underscore
    /// character, or is a reserved keyword.
    fn ctas_ident_put(out: &mut String, name: &str) {
        if Self::ctas_needs_quote(name) {
            out.push('"');
            for c in name.chars() {
                out.push(c);
                if c == '"' {
                    out.push('"');
                }
            }
            out.push('"');
        } else {
            out.push_str(name);
        }
    }

    /// Whether `name` must be double-quoted to survive a re-parse as an
    /// identifier (see `ctas_ident_put`).
    fn ctas_needs_quote(name: &str) -> bool {
        match name.chars().next() {
            None => return true,
            Some(c) if c.is_ascii_digit() => return true,
            Some(_) => {}
        }
        if name.chars().any(|c| !(c.is_ascii_alphanumeric() || c == '_')) {
            return true;
        }
        vibesql_parser::is_keyword(name)
    }

    /// Extract table names from a FROM clause
    fn get_table_names_from_from(
        from: &Option<vibesql_ast::FromClause>,
    ) -> Result<Vec<String>, ExecutorError> {
        let mut names = Vec::new();

        match from {
            None => {
                // No FROM clause - can't expand wildcard
                return Err(ExecutorError::UnsupportedFeature(
                    "CREATE TABLE AS SELECT * requires a FROM clause".to_string(),
                ));
            }
            Some(vibesql_ast::FromClause::Table { name, .. }) => {
                names.push(name.clone());
            }
            Some(vibesql_ast::FromClause::Join { left, right, .. }) => {
                // Recursively get tables from join
                names.extend(Self::get_table_names_from_from(&Some(*left.clone()))?);
                names.extend(Self::get_table_names_from_from(&Some(*right.clone()))?);
            }
            Some(vibesql_ast::FromClause::Subquery { alias, .. }) => {
                // For derived tables (subqueries), we can't easily expand *
                // because we'd need to recursively process the subquery
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "CREATE TABLE AS SELECT * from subquery '{}' not supported - please specify columns explicitly",
                    alias
                )));
            }
            Some(vibesql_ast::FromClause::Values { alias, .. }) => {
                // VALUES clause - can't determine column names from schema
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "CREATE TABLE AS SELECT * from VALUES '{}' not supported - please specify columns explicitly",
                    alias
                )));
            }
            Some(vibesql_ast::FromClause::TableFunction { name, .. }) => {
                // Table function - can't determine column names from schema
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "CREATE TABLE AS SELECT * from table function '{}' not supported - please specify columns explicitly",
                    name
                )));
            }
        }

        Ok(names)
    }

    /// Resolve a view's exposed output column names for wildcard expansion in
    /// a CTAS select list (`SELECT * FROM <view>` / `SELECT <view>.*`).
    ///
    /// Uses the view's explicit column list, which `CREATE VIEW` derives
    /// eagerly (by executing the body once) whenever it isn't already
    /// supplied explicitly. The rare "lax view" case (issue #5795) where
    /// column resolution is deferred to query time has no statically known
    /// column list, so it is reported as unsupported here rather than
    /// re-executing the view body from a schema-only derivation path.
    fn view_output_column_names(
        view: &vibesql_catalog::ViewDefinition,
    ) -> Result<Vec<String>, ExecutorError> {
        view.columns.clone().ok_or_else(|| {
            ExecutorError::UnsupportedFeature(format!(
                "CREATE TABLE AS SELECT * from view '{}' with unresolved columns not supported",
                view.name
            ))
        })
    }

    /// Derive a column name from a CTAS select-list expression.
    ///
    /// SQLite names a result column after the original text of the
    /// expression when there is no explicit alias (e.g. `SELECT max(b+c)`
    /// yields a column named `max(b+c)`, `SELECT b+c` yields `b+c`). For a
    /// bare column reference the name is just the column name. Only when no
    /// source text is available (e.g. a programmatically-built AST) do we
    /// fall back to the synthetic `columnN` placeholder.
    fn derive_column_name_from_expr(
        expr: &vibesql_ast::Expression,
        source_text: &Option<String>,
        counter: &mut usize,
    ) -> String {
        // A bare column reference is always named after the column itself,
        // matching the regular SELECT result-column naming (short mode).
        if let vibesql_ast::Expression::ColumnRef(col_id) = expr {
            return col_id.column_canonical().to_string();
        }

        // For every other expression (functions, aggregates, arithmetic,
        // literals, ...) SQLite uses the original SQL text of the
        // select-list item as the column name.
        if let Some(src) = source_text {
            return src.clone();
        }

        // No source text available (programmatically-built AST): fall back to
        // the previous best-effort naming.
        match expr {
            vibesql_ast::Expression::Function { name, .. } => {
                // Use the function name as the column name
                name.to_string().to_lowercase()
            }
            _ => {
                *counter += 1;
                format!("column{}", counter)
            }
        }
    }

    /// Infer DataType from an SqlValue
    fn infer_data_type(value: &vibesql_types::SqlValue) -> DataType {
        use vibesql_types::SqlValue;
        match value {
            SqlValue::Null => DataType::BinaryLargeObject,
            SqlValue::Boolean(_) => DataType::Boolean,
            SqlValue::Integer(_) => DataType::Integer,
            SqlValue::Bigint(_) => DataType::Bigint,
            SqlValue::Smallint(_) => DataType::Smallint,
            SqlValue::Unsigned(_) => DataType::Unsigned,
            SqlValue::Float(_) | SqlValue::Real(_) => DataType::Real,
            SqlValue::Double(_) | SqlValue::Numeric(_) => DataType::DoublePrecision,
            SqlValue::Character(_) => DataType::Character { length: 255 },
            SqlValue::Varchar(_) => DataType::Varchar { max_length: None },
            SqlValue::Date(_) => DataType::Date,
            SqlValue::Time(_) => DataType::Time { with_timezone: false },
            SqlValue::Timestamp(_) => DataType::Timestamp { with_timezone: false },
            SqlValue::Interval(_) => DataType::Interval {
                start_field: vibesql_types::IntervalField::Day,
                end_field: None,
            },
            SqlValue::Vector(v) => DataType::Vector { dimensions: v.len() as u32 },
            SqlValue::Blob(_) => DataType::BinaryLargeObject,
        }
    }
}

#[cfg(test)]
mod ctas_sql_source_tests {
    use super::CreateTableExecutor as E;
    use vibesql_types::TypeAffinity::{Integer, None as Blob, Numeric, Real, Text};

    fn col(name: &str, aff: vibesql_types::TypeAffinity) -> (String, vibesql_types::TypeAffinity) {
        (name.to_string(), aff)
    }

    #[test]
    fn compact_layout_matches_sqlite() {
        // Short schema (n < 50) → single line, no spaces after commas, affinity
        // type codes, BLOB affinity emits no type. Verified against sqlite3.
        let cols = vec![
            col("a", Integer),
            col("b", Text),
            col("c", Real),
            col("d", Blob),
            col("e", Numeric),
        ];
        assert_eq!(
            E::generate_ctas_sql_source("t2s", &cols),
            "CREATE TABLE t2s(a INT,b TEXT,c REAL,d,e NUM)"
        );
    }

    #[test]
    fn pretty_layout_quotes_keywords_and_digits() {
        // Long schema (n >= 50) → one column per line, keyword/digit-leading
        // identifiers double-quoted. Matches sqlite3 table-8.1.1.
        let cols = vec![
            col("desc", Text),
            col("asc", Text),
            col("key", Integer),
            col("14_vac", Numeric),
            col("fuzzy_dog_12", Text),
            col("begin", Blob),
            col("end", Text),
        ];
        let expected = "CREATE TABLE t2(\n  \"desc\" TEXT,\n  \"asc\" TEXT,\n  \"key\" INT,\n  \"14_vac\" NUM,\n  fuzzy_dog_12 TEXT,\n  \"begin\",\n  \"end\" TEXT\n)";
        assert_eq!(E::generate_ctas_sql_source("t2", &cols), expected);
    }

    #[test]
    fn quotes_table_name_and_expression_columns() {
        // Embedded double-quote in the table name is doubled; an expression
        // column name with special chars is quoted and carries no type.
        // Matches sqlite3 table-8.3.1.
        let cols = vec![col("cnt", Blob), col("max(b+c)", Blob)];
        assert_eq!(
            E::generate_ctas_sql_source("t4\"abc", &cols),
            "CREATE TABLE \"t4\"\"abc\"(cnt,\"max(b+c)\")"
        );
    }

    #[test]
    fn apostrophe_alias_is_quoted_not_dropped() {
        // Apostrophe-bearing alias must round-trip as a double-quoted
        // identifier (data-loss-adjacent regression guard).
        let cols = vec![col("it's", Integer)];
        assert_eq!(E::generate_ctas_sql_source("t5", &cols), "CREATE TABLE t5(\"it's\" INT)");
    }

    #[test]
    fn dotted_column_name_is_quoted() {
        // A `.` forces quoting; single short column stays compact. table-8.9.
        let cols = vec![col("col.1", Text)];
        assert_eq!(E::generate_ctas_sql_source("t11", &cols), "CREATE TABLE t11(\"col.1\" TEXT)");
    }

    #[test]
    fn needs_quote_rules() {
        assert!(!E::ctas_needs_quote("fuzzy_dog_12"));
        assert!(!E::ctas_needs_quote("_x"));
        assert!(E::ctas_needs_quote("key")); // keyword
        assert!(E::ctas_needs_quote("desc")); // keyword
        assert!(E::ctas_needs_quote("14_vac")); // leading digit
        assert!(E::ctas_needs_quote("col.1")); // dot
        assert!(E::ctas_needs_quote("it's")); // apostrophe
        assert!(E::ctas_needs_quote("")); // empty
    }
}
