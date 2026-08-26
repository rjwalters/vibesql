//! Regression coverage for `CREATE VIEW temp.<name> AS ...` unqualified
//! table-name resolution (altercol.test 16.2.1/16.2.3).
//!
//! `CREATE VIEW temp.v AS ...` (dot-qualified into the temp schema, no `TEMP`
//! keyword) is exactly as much a temp view as `CREATE TEMP VIEW v AS ...` —
//! both are homed in the session's temp schema. The view-body
//! unqualified-resolution restriction (`#6485`,
//! `Catalog::scoped_unqualified_resolution_restriction`) previously keyed
//! its "is this a temp view, so leave resolution unrestricted?" decision off
//! `stmt.temporary` alone — true only for the `TEMP`/`TEMPORARY` keyword
//! spelling — so a dot-qualified temp view's body got hard-scoped to the temp
//! schema only, and any unqualified reference to a `main` table inside it
//! failed to resolve at all.

#[cfg(test)]
mod tests {
    use vibesql_ast::Statement;
    use vibesql_parser::Parser;
    use vibesql_storage::Database;

    use crate::{advanced_objects, readonly::ReadOnlyQuery, CreateTableExecutor, InsertExecutor};

    fn exec_ok(db: &mut Database, sql: &str) {
        let stmt =
            Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {sql:?}: {e:?}"));
        match stmt {
            Statement::CreateTable(s) => {
                CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
            }
            Statement::Insert(s) => {
                InsertExecutor::execute(db, &s).expect("INSERT failed");
            }
            other => panic!("Unsupported setup statement: {other:?}"),
        }
    }

    /// `CREATE VIEW temp.v AS SELECT a FROM t1` (dot-qualified, no `TEMP`
    /// keyword) must resolve the unqualified `main` table `t1` in its body —
    /// the same as `CREATE TEMP VIEW v AS SELECT a FROM t1` already does.
    #[test]
    fn dot_qualified_temp_view_resolves_main_table() {
        let mut db = Database::new();
        exec_ok(&mut db, "CREATE TABLE t1(a, b, c)");
        exec_ok(&mut db, "INSERT INTO t1 VALUES (1, 2, 3)");

        let stmt = match Parser::parse_sql("CREATE VIEW temp.v5 AS SELECT a FROM t1") {
            Ok(Statement::CreateView(s)) => s,
            other => panic!("expected CreateView, got {other:?}"),
        };
        advanced_objects::execute_create_view(&stmt, &mut db)
            .expect("CREATE VIEW temp.v5 AS SELECT a FROM t1 should resolve t1 in main schema");

        let result = db.query("SELECT * FROM v5").expect("SELECT * FROM v5 should succeed");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values.len(), 1);
    }

    /// Sanity check that the `CREATE TEMP VIEW` keyword spelling continues to
    /// behave identically to the dot-qualified spelling above.
    #[test]
    fn temp_keyword_view_resolves_main_table() {
        let mut db = Database::new();
        exec_ok(&mut db, "CREATE TABLE t1(a, b, c)");
        exec_ok(&mut db, "INSERT INTO t1 VALUES (1, 2, 3)");

        let stmt = match Parser::parse_sql("CREATE TEMP VIEW v5 AS SELECT a FROM t1") {
            Ok(Statement::CreateView(s)) => s,
            other => panic!("expected CreateView, got {other:?}"),
        };
        advanced_objects::execute_create_view(&stmt, &mut db)
            .expect("CREATE TEMP VIEW v5 AS SELECT a FROM t1 should resolve t1 in main schema");

        let result = db.query("SELECT * FROM v5").expect("SELECT * FROM v5 should succeed");
        assert_eq!(result.rows.len(), 1);
    }

    /// A plain (non-temp) view must still be restricted to its own schema —
    /// this fix must not loosen the `#6485` restriction for ordinary `main`
    /// views, only correct which views count as "temp".
    #[test]
    fn main_view_does_not_fall_back_to_temp_schema() {
        let mut db = Database::new();
        exec_ok(&mut db, "CREATE TABLE temp.t1(a, b, c)");
        exec_ok(&mut db, "INSERT INTO temp.t1 VALUES (1, 2, 3)");

        // No `main.t1` exists, only `temp.t1` — a main-schema view body must
        // NOT silently fall back to resolving the temp-only table, whether
        // that surfaces as a CREATE-time error (table entirely unresolved) or
        // is deferred to query time (SQLite's lax view-creation semantics for
        // a merely-unresolved column, #5795) — either way, the temp table's
        // rows must never be exposed through this main-schema view.
        let stmt = match Parser::parse_sql("CREATE VIEW v1 AS SELECT a FROM t1") {
            Ok(Statement::CreateView(s)) => s,
            other => panic!("expected CreateView, got {other:?}"),
        };
        if advanced_objects::execute_create_view(&stmt, &mut db).is_ok() {
            assert!(
                db.query("SELECT * FROM v1").is_err(),
                "#6485: a main-schema view body must not resolve an unqualified name against \
                 a same-named TEMP table"
            );
        }
    }
}
