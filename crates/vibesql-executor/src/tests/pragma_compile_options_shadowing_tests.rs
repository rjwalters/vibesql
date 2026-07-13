//! Tests for `pragma_compile_options` synthetic-table shadowing precedence (#6030)
//!
//! `pragma_compile_options` is an eponymous system table VibeSQL synthesizes
//! (single `compile_options TEXT` column, zero rows). Because `pragma_*` names
//! are not reserved at `CREATE TABLE` time, a user may create a real table of
//! the same name. In that case the real table must take precedence for
//! `SELECT` (matching SQLite, where an eponymous virtual table is shadowed by a
//! same-named real table). Once the real table is dropped, the synthetic table
//! re-engages.

#[cfg(test)]
mod tests {
    use vibesql_parser::Parser;
    use vibesql_storage::Database;

    use crate::{CreateTableExecutor, DropTableExecutor, InsertExecutor, SelectExecutor};

    fn create_real_pragma_table(db: &mut Database) {
        let sql = "CREATE TABLE pragma_compile_options (x INTEGER)";
        let stmt = Parser::parse_sql(sql).expect("Failed to parse CREATE TABLE");
        if let vibesql_ast::Statement::CreateTable(create_stmt) = stmt {
            CreateTableExecutor::execute(&create_stmt, db).expect("Failed to create table");
        } else {
            panic!("Expected CreateTable statement");
        }
    }

    fn insert_row(db: &mut Database, sql: &str) {
        let stmt = Parser::parse_sql(sql).expect("Failed to parse INSERT");
        if let vibesql_ast::Statement::Insert(insert_stmt) = stmt {
            InsertExecutor::execute(db, &insert_stmt).expect("Failed to insert row");
        } else {
            panic!("Expected Insert statement");
        }
    }

    fn run_select(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
        let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
        if let vibesql_ast::Statement::Select(select_stmt) = stmt {
            let executor = SelectExecutor::new(db);
            executor.execute(&select_stmt).expect("Failed to execute SELECT")
        } else {
            panic!("Expected Select statement");
        }
    }

    /// A real user table named `pragma_compile_options` wins over the synthetic
    /// eponymous table. This is the core repro from #6030.
    #[test]
    fn test_real_table_shadows_synthetic() {
        let mut db = Database::new();
        create_real_pragma_table(&mut db);
        insert_row(&mut db, "INSERT INTO pragma_compile_options VALUES (42)");

        let rows = run_select(&db, "SELECT * FROM pragma_compile_options");

        // Real table wins: one row with the user's value 42, not the synthetic
        // zero-row table.
        assert_eq!(rows.len(), 1, "expected the real user table's row, got the synthetic table");
        assert_eq!(rows[0].values.len(), 1);
        assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(42));
    }

    /// Case-insensitive naming: the real table wins even when referenced with
    /// different casing than it was created with. `get_table` folds identifiers
    /// the same way `is_pragma_compile_options_table` does.
    #[test]
    fn test_real_table_shadows_synthetic_case_insensitive() {
        let mut db = Database::new();
        create_real_pragma_table(&mut db);
        insert_row(&mut db, "INSERT INTO pragma_compile_options VALUES (7)");

        let rows = run_select(&db, "SELECT * FROM PRAGMA_COMPILE_OPTIONS");
        assert_eq!(rows.len(), 1, "case-insensitive reference should still hit the real table");
        assert_eq!(rows[0].values[0], vibesql_types::SqlValue::Integer(7));
    }

    /// With no real table present, the synthetic zero-row, single-`compile_options`
    /// -column table is returned (fallback intact).
    #[test]
    fn test_synthetic_fallback_when_no_real_table() {
        let db = Database::new();

        let rows = run_select(&db, "SELECT * FROM pragma_compile_options");
        assert_eq!(rows.len(), 0, "synthetic pragma_compile_options table must have zero rows");
    }

    /// The `db exists {SELECT 1 FROM pragma_compile_options WHERE compile_options=...}`
    /// probe json101.test relies on still routes to an empty result on a fresh db.
    #[test]
    fn test_synthetic_fallback_where_probe_empty() {
        let db = Database::new();

        let rows = run_select(
            &db,
            "SELECT 1 FROM pragma_compile_options WHERE compile_options = 'ENABLE_JSON1'",
        );
        assert_eq!(rows.len(), 0, "synthetic table has no rows, so the WHERE probe is empty");
    }

    /// Dropping the real table re-engages the synthetic fallback.
    #[test]
    fn test_drop_real_table_reverts_to_synthetic() {
        let mut db = Database::new();
        create_real_pragma_table(&mut db);
        insert_row(&mut db, "INSERT INTO pragma_compile_options VALUES (99)");

        // Real table wins while it exists.
        let rows = run_select(&db, "SELECT * FROM pragma_compile_options");
        assert_eq!(rows.len(), 1);

        // Drop it.
        let drop_stmt = vibesql_ast::DropTableStmt {
            table_name: "pragma_compile_options".to_string(),
            if_exists: false,
            quoted: false,
        };
        DropTableExecutor::execute(&drop_stmt, &mut db).expect("Failed to drop table");

        // Synthetic fallback re-engages: zero rows again.
        let rows = run_select(&db, "SELECT * FROM pragma_compile_options");
        assert_eq!(rows.len(), 0, "after DROP, the synthetic zero-row table must return");
    }
}
