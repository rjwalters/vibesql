//! Database Persistence - Loading SQL Dumps
//!
//! Provides functionality for loading databases from SQL dump files.
//! This mirrors the save functionality in vibesql-storage but requires
//! the executor layer since it needs to parse and execute SQL statements.

use std::path::Path;

use vibesql_storage::Database;

use crate::{
    CreateIndexExecutor, CreateTableExecutor, ExecutorError, InsertExecutor, RoleExecutor,
    SchemaExecutor, TriggerExecutor, UpdateExecutor, ViewExecutor,
};

/// Load database from SQL dump file
///
/// Reads SQL dump, parses statements, and executes them to recreate database state.
/// This is the shared implementation used by CLI, Python bindings, and other consumers.
///
/// # Arguments
/// * `path` - Path to the SQL dump file
///
/// # Returns
/// A new Database instance with the loaded state
///
/// # Errors
/// Returns error if:
/// - File cannot be read
/// - File is not valid SQL dump format (e.g., binary SQLite file)
/// - SQL parsing fails
/// - Statement execution fails
///
/// # Example
/// ```no_run
/// # use vibesql_executor::load_sql_dump;
/// let db = load_sql_dump("database.sql").unwrap();
/// ```
pub fn load_sql_dump<P: AsRef<Path>>(path: P) -> Result<Database, ExecutorError> {
    // Read the SQL dump file using storage utility
    let sql_content = vibesql_storage::read_sql_dump(&path).map_err(|e| {
        ExecutorError::Other(format!("Failed to read database file {:?}: {}", path.as_ref(), e))
    })?;

    // Split into individual statements using storage utility
    let statements = vibesql_storage::parse_sql_statements(&sql_content)
        .map_err(|e| ExecutorError::Other(format!("Failed to parse SQL dump: {}", e)))?;

    // Create a new database to populate
    let mut db = Database::new();

    // Execute each statement
    for (idx, stmt_sql) in statements.iter().enumerate() {
        // Skip empty statements and comments
        let trimmed = stmt_sql.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }

        // Parse the statement
        let statement = vibesql_parser::Parser::parse_sql(trimmed).map_err(|e| {
            ExecutorError::Other(format!(
                "Failed to parse statement {} in {:?}: {}\nStatement: {}",
                idx + 1,
                path.as_ref(),
                e,
                truncate_for_error(trimmed, 100)
            ))
        })?;

        // Execute the statement
        execute_statement_for_load(&mut db, statement, trimmed).map_err(|e| {
            ExecutorError::Other(format!(
                "Failed to execute statement {} in {:?}: {}\nStatement: {}",
                idx + 1,
                path.as_ref(),
                e,
                truncate_for_error(trimmed, 100)
            ))
        })?;
    }

    Ok(db)
}

/// Execute a single statement during database load
///
/// Only DDL and INSERT statements are supported during load.
/// Other statement types will return an error.
fn execute_statement_for_load(
    db: &mut Database,
    statement: vibesql_ast::Statement,
    original_sql: &str,
) -> Result<(), ExecutorError> {
    match statement {
        vibesql_ast::Statement::CreateSchema(schema_stmt) => {
            // Skip built-in schemas (main and all temp schemas) - they already exist
            // This handles backward compatibility with old SQL dumps
            let schema_name = schema_stmt.schema_name.to_lowercase();
            if schema_name != vibesql_catalog::DEFAULT_SCHEMA
                && !vibesql_catalog::Catalog::is_temp_schema(&schema_name)
            {
                SchemaExecutor::execute_create_schema(&schema_stmt, db)?;
            }
        }
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            // Trusted replay of the engine's own persisted dump. A dump must
            // always round-trip, so we bypass the user-facing reserved-name and
            // duplicate-column guards: a `sqlite_`-prefixed table that
            // legitimately reached the catalog before #5614 (via the then-open
            // ALTER TABLE RENAME gap) must still reload rather than brick the
            // database. User-issued CREATE TABLE still goes through
            // `CreateTableExecutor::execute`, which keeps the guards (issue #5614).
            //
            // We use the trusted+verbatim variant so the reload ALSO preserves
            // the byte-for-byte original CREATE TABLE text for sqlite_master.sql
            // (issue #5619). The bypassed guards (#5614/#5553) and the verbatim
            // source capture are complementary: a persisted dump must reload AND
            // keep the user's original formatting after a save/reload cycle.
            CreateTableExecutor::execute_for_load_with_source(
                &create_stmt,
                db,
                Some(original_sql),
            )?;
        }
        vibesql_ast::Statement::CreateIndex(index_stmt) => {
            CreateIndexExecutor::execute(&index_stmt, db)?;
        }
        vibesql_ast::Statement::CreateView(mut view_stmt) => {
            // Store original SQL for sqlite_master compatibility
            view_stmt.sql_definition = Some(original_sql.to_string());
            ViewExecutor::execute_create_view(&view_stmt, db)?;
        }
        vibesql_ast::Statement::CreateTrigger(trigger_stmt) => {
            // Preserve the original SQL on the catalog TriggerDefinition so that
            // a subsequent save_sql_dump can re-emit the trigger verbatim. Without
            // this, triggers would survive one round-trip (because we just executed
            // CREATE TRIGGER) but would be lost on the *next* save because the
            // catalog entry would have no sql_definition.
            TriggerExecutor::create_trigger_with_sql(db, &trigger_stmt, Some(original_sql))?;
        }
        vibesql_ast::Statement::CreateRole(role_stmt) => {
            RoleExecutor::execute_create_role(&role_stmt, db)?;
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt)?;
        }
        vibesql_ast::Statement::Update(update_stmt) => {
            UpdateExecutor::execute(&update_stmt, db)?;
        }
        _ => {
            return Err(ExecutorError::Other(format!(
                "Statement type not supported in database load: {:?}",
                statement
            )));
        }
    }
    Ok(())
}

/// Truncate a string for error messages
fn truncate_for_error(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write};
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_load_simple_database() {
        // Create a temporary SQL dump file
        let temp_file = NamedTempFile::new().unwrap();
        let sql_dump = r#"
-- Test database
CREATE TABLE users (id INTEGER, name VARCHAR(50));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
"#;

        fs::write(temp_file.path(), sql_dump).unwrap();

        // Load the database
        let db = load_sql_dump(temp_file.path().to_str().unwrap()).unwrap();

        // Verify the table exists (note: identifiers are uppercased)
        assert!(db.get_table("USERS").is_some());

        // Verify data was loaded
        let table = db.get_table("USERS").unwrap();
        assert_eq!(table.row_count(), 2);
        // temp_file is automatically cleaned up on drop
    }

    #[test]
    fn test_load_with_schema() {
        let temp_file = NamedTempFile::new().unwrap();
        // Note: Schema-qualified INSERT statements not yet supported by parser
        // So we create the table in a schema but insert without schema qualification
        let sql_dump = r#"
CREATE SCHEMA test_schema;
CREATE TABLE test_schema.products (id INTEGER, price REAL);
"#;

        fs::write(temp_file.path(), sql_dump).unwrap();

        let db = load_sql_dump(temp_file.path().to_str().unwrap()).unwrap();

        // Verify table exists in schema (case-insensitive lookup)
        assert!(db.get_table("test_schema.products").is_some());
    }

    #[test]
    fn test_load_nonexistent_file() {
        let result = load_sql_dump("/tmp/nonexistent_file.sql");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn test_load_invalid_sql() {
        let temp_file = NamedTempFile::new().unwrap();
        fs::write(temp_file.path(), "THIS IS NOT VALID SQL;").unwrap();

        let result = load_sql_dump(temp_file.path().to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to parse"));
    }

    #[test]
    fn test_load_binary_file_error() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut file = fs::File::create(temp_file.path()).unwrap();
        file.write_all(b"SQLite format 3\0").unwrap();
        file.write_all(&[0xFF, 0xFE, 0xFD]).unwrap();

        let result = load_sql_dump(temp_file.path().to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("binary SQLite database"));
    }

    #[test]
    fn test_load_with_indexes() {
        let temp_file = NamedTempFile::new().unwrap();
        let sql_dump = r#"
CREATE TABLE employees (id INTEGER, name VARCHAR(100), dept VARCHAR(50));
INSERT INTO employees VALUES (1, 'Alice', 'Engineering');
INSERT INTO employees VALUES (2, 'Bob', 'Sales');
CREATE INDEX idx_dept ON employees (dept Asc);
"#;

        fs::write(temp_file.path(), sql_dump).unwrap();

        let db = load_sql_dump(temp_file.path().to_str().unwrap()).unwrap();

        // Verify table and data (note: identifiers are uppercased)
        assert!(db.get_table("EMPLOYEES").is_some());
        let table = db.get_table("EMPLOYEES").unwrap();
        assert_eq!(table.row_count(), 2);

        // Verify index exists (note: identifiers are uppercased)
        assert!(db.get_index("IDX_DEPT").is_some());
    }

    #[test]
    fn test_table_index_namespace_collision_round_trips_cleanly() {
        // Regression for issue #5613. Before the fix, CREATE TABLE accepted a
        // name already used by an index. The resulting saved schema then failed
        // to reload because the DDL-replay (this very `load_sql_dump`) hit its
        // own collision and aborted, bricking the database. With the namespace
        // check in place, the collision is rejected up-front, so a database can
        // never persist a state it cannot itself reload.
        //
        // Here we build a *valid* schema (table + index with distinct names plus
        // a colliding-but-rejected attempt), save it, and confirm the dump
        // reloads cleanly via the same replay path.
        let mut db = Database::new();

        let create_t2 = vibesql_parser::Parser::parse_sql("CREATE TABLE test2(one text)").unwrap();
        if let vibesql_ast::Statement::CreateTable(s) = create_t2 {
            CreateTableExecutor::execute(&s, &mut db).unwrap();
        }
        let create_ix =
            vibesql_parser::Parser::parse_sql("CREATE INDEX test3 ON test2(one)").unwrap();
        if let vibesql_ast::Statement::CreateIndex(s) = create_ix {
            CreateIndexExecutor::execute(&s, &mut db).unwrap();
        }

        // The colliding CREATE TABLE is rejected (never persisted).
        let collide = vibesql_parser::Parser::parse_sql("CREATE TABLE test3(two text)").unwrap();
        if let vibesql_ast::Statement::CreateTable(s) = collide {
            let err = CreateTableExecutor::execute(&s, &mut db).unwrap_err();
            assert_eq!(err.to_string(), "there is already an index named test3");
        }

        // Save the (clean) schema and reload it through the replay path.
        let temp_file = NamedTempFile::new().unwrap();
        db.save_sql_dump(temp_file.path()).unwrap();

        let reloaded = load_sql_dump(temp_file.path().to_str().unwrap())
            .expect("schema must reload without bricking (issue #5613)");

        // Both objects survived the round-trip; the bogus table never appeared.
        assert!(reloaded.get_table("test2").is_some());
        assert!(reloaded.get_index("test3").is_some());
    }

    #[test]
    fn test_dump_with_table_index_collision_is_rejected_on_load() {
        // Defense in depth: even a hand-authored dump that puts a CREATE INDEX
        // and a same-named CREATE TABLE in one file must be rejected on load
        // with the SQLite-compatible wording — not silently accepted into an
        // unloadable state.
        let temp_file = NamedTempFile::new().unwrap();
        let sql_dump = r#"
CREATE TABLE test2 (one TEXT);
CREATE INDEX test3 ON test2 (one);
CREATE TABLE test3 (two TEXT);
"#;
        fs::write(temp_file.path(), sql_dump).unwrap();

        let err = load_sql_dump(temp_file.path().to_str().unwrap())
            .expect_err("colliding dump must be rejected, not bricked")
            .to_string();
        assert!(
            err.contains("there is already an index named test3"),
            "expected index-collision wording, got: {}",
            err
        );
    }

    #[test]
    fn test_load_dump_with_sqlite_prefixed_table_reloads_cleanly() {
        // Regression for issue #5614. Before the fix, a database that contained a
        // `sqlite_`-prefixed *user* table (reachable via the then-unguarded
        // ALTER TABLE RENAME TO) would dump as `CREATE TABLE sqlite_t3 (...)`,
        // and on reload the new user-facing reserved-name guard would reject the
        // engine's own dump — permanently bricking the database. The trusted
        // load path (`execute_for_load`) must reconstruct whatever was
        // legitimately persisted, so such a dump reloads cleanly.
        let temp_file = NamedTempFile::new().unwrap();
        let sql_dump = r#"
CREATE TABLE sqlite_t3 (a BLOB, b BLOB, c BLOB);
INSERT INTO sqlite_t3 VALUES (1, 2, 3);
"#;
        fs::write(temp_file.path(), sql_dump).unwrap();

        let db = load_sql_dump(temp_file.path().to_str().unwrap())
            .expect("a persisted sqlite_-prefixed table must reload, not brick (issue #5614)");

        // The table and its data survived the trusted reload.
        let table = db.get_table("sqlite_t3").expect("sqlite_t3 must exist after reload");
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn test_user_create_sqlite_prefixed_table_still_rejected() {
        // The #5614 feature must remain intact on the USER path: a user-issued
        // CREATE TABLE with a reserved `sqlite_` name is still rejected. Only the
        // trusted load/replay path bypasses the guard.
        let mut db = Database::new();
        let stmt = vibesql_parser::Parser::parse_sql("CREATE TABLE sqlite_foo (x INTEGER)").unwrap();
        if let vibesql_ast::Statement::CreateTable(s) = stmt {
            let err = CreateTableExecutor::execute(&s, &mut db).unwrap_err();
            assert_eq!(err.to_string(), "object name reserved for internal use: sqlite_foo");
        } else {
            panic!("expected CreateTable");
        }
    }

    #[test]
    fn test_alter_rename_to_reserved_name_rejected() {
        // Regression for issue #5614 fix #2 (sqlite3 3.51.0 alter-2.5): renaming
        // a table to a reserved `sqlite_`-prefixed name must error
        // `object name reserved for internal use: <name>`, preventing a
        // `sqlite_`-prefixed user table from ever being persisted (the root
        // enabler of the reload-brick regression).
        use crate::AlterTableExecutor;

        let mut db = Database::new();
        let create = vibesql_parser::Parser::parse_sql("CREATE TABLE t3(a, b, c)").unwrap();
        if let vibesql_ast::Statement::CreateTable(s) = create {
            CreateTableExecutor::execute(&s, &mut db).unwrap();
        }

        let alter =
            vibesql_parser::Parser::parse_sql("ALTER TABLE t3 RENAME TO sqlite_t3").unwrap();
        if let vibesql_ast::Statement::AlterTable(s) = alter {
            let err = AlterTableExecutor::execute(&s, &mut db).unwrap_err();
            assert_eq!(err.to_string(), "object name reserved for internal use: sqlite_t3");
        } else {
            panic!("expected AlterTable");
        }

        // The rename was rejected, so the original table is intact and no
        // sqlite_-prefixed table leaked into the catalog.
        assert!(db.get_table("t3").is_some());
        assert!(db.get_table("sqlite_t3").is_none());
    }

    #[test]
    fn test_load_with_roles() {
        let temp_file = NamedTempFile::new().unwrap();
        let sql_dump = r#"
CREATE ROLE admin;
CREATE ROLE user;
CREATE TABLE data (id INTEGER);
"#;

        fs::write(temp_file.path(), sql_dump).unwrap();

        let db = load_sql_dump(temp_file.path().to_str().unwrap()).unwrap();

        // Verify table exists (note: identifiers are uppercased)
        assert!(db.get_table("DATA").is_some());
    }

    // Issue #5618: a dump → reload round-trip must preserve the *original*
    // declared spelling that `sqlite_master.name` / `PRAGMA table_info` echo,
    // even though lookups remain case-insensitive (#5553). Before the fix, the
    // dump emitted the lowercase canonical catalog key, and a keyword name like
    // `"create"` was written unquoted and re-lexed back to the keyword `CREATE`.
    #[test]
    fn test_dump_reload_preserves_original_case_and_quoted_keywords() {
        let mut db = Database::new();
        for sql in [
            "CREATE TABLE \"create\" (f1 INT)",
            "CREATE TABLE big (a, b)",
            "CREATE TABLE \"MixedCase\" (\"ColA\" INT, \"B\" INT)",
        ] {
            let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
            if let vibesql_ast::Statement::CreateTable(s) = stmt {
                CreateTableExecutor::execute(&s, &mut db).unwrap();
            } else {
                panic!("expected CreateTable for: {sql}");
            }
        }

        let temp_file = NamedTempFile::new().unwrap();
        db.save_sql_dump(temp_file.path()).unwrap();
        let reloaded = load_sql_dump(temp_file.path().to_str().unwrap()).unwrap();

        // Table name echo preserves the original spelling. Lookup is
        // case-insensitive, so resolve via the canonical key, then assert the
        // stored display name.
        let create_tbl = reloaded.get_table("create").expect("quoted-keyword table must survive");
        assert_eq!(
            create_tbl.schema.name, "create",
            "quoted keyword table name must round-trip verbatim (not folded to CREATE)"
        );

        let mixed = reloaded.get_table("mixedcase").expect("mixed-case table must survive");
        assert_eq!(mixed.schema.name, "MixedCase", "mixed-case table name must be preserved");
        // Column-name echo preserves original case too.
        assert_eq!(mixed.schema.columns[0].name, "ColA");
        assert_eq!(mixed.schema.columns[1].name, "B");

        let big = reloaded.get_table("big").expect("table must survive");
        assert_eq!(big.schema.name, "big");
    }
}
