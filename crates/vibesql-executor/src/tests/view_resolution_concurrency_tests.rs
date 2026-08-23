//! Concurrency regression coverage for the view-body unqualified-resolution
//! restriction (#6485, review of PR #6506).
//!
//! Executing a non-TEMP view's body restricts unqualified table-name
//! resolution to the view's owning schema for the duration of that nested
//! execution. The first cut of that fix stored the restriction as a
//! `RwLock<Option<String>>` **field on `Catalog`** — catalog-global mutable
//! state toggled with a set-then-restore pair. That is `Sync` at the type
//! level but not actually correct under concurrency: `SharedDatabase`
//! (`crate::readonly`) is explicitly designed to let many readers execute
//! `SELECT`s simultaneously against one shared `&Database` — which the HTTP
//! server does per request — so two concurrent view-body executions
//! interleaved their set/restore pairs. The reviewer reproduced the
//! restriction getting *permanently stuck* at `Some("main")`, after which
//! every subsequent unrelated query on that database mis-resolved (an
//! 8-thread run produced `errors=119999`).
//!
//! The restriction is now thread-local and established through an RAII guard
//! ([`vibesql_catalog::Catalog::scoped_unqualified_resolution_restriction`]),
//! so it is confined to exactly the call stack that set it. These tests are
//! the regression fence for that: they would fail (with a flood of "no such
//! table" errors and a non-`None` leftover restriction) against the shared
//! `RwLock` field, and they also fail if the guard is ever dropped in favor
//! of a manual restore that an early return can skip.

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use vibesql_ast::Statement;
    use vibesql_parser::Parser;
    use vibesql_storage::Database;

    use crate::{
        advanced_objects, readonly::ReadOnlyQuery, CreateTableExecutor, DropTableExecutor,
        InsertExecutor,
    };

    /// Execute a setup statement that is expected to succeed.
    fn exec_ok(db: &mut Database, sql: &str) {
        let stmt =
            Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {sql:?}: {e:?}"));
        match stmt {
            Statement::CreateTable(s) => {
                CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
            }
            Statement::CreateView(s) => {
                advanced_objects::execute_create_view(&s, db).expect("CREATE VIEW failed");
            }
            Statement::Insert(s) => {
                InsertExecutor::execute(db, &s).expect("INSERT failed");
            }
            Statement::DropTable(s) => {
                DropTableExecutor::execute(&s, db).expect("DROP TABLE failed");
            }
            other => panic!("Unsupported setup statement: {other:?}"),
        }
    }

    /// A `main` table `t1`, a `main` view `nv` over it, and an unrelated TEMP
    /// table `tt` that only resolves when the restriction is *not* active.
    ///
    /// `tt` is the canary: it lives solely in the session's temp schema, so a
    /// restriction leaked from `nv`'s body onto a shared/pooled cell makes
    /// `SELECT * FROM tt` fail with "no such table" — exactly the corruption
    /// of unrelated queries the reviewer observed.
    fn create_test_db() -> Database {
        let mut db = Database::new();
        exec_ok(&mut db, "CREATE TABLE t1(a INT, b INT)");
        exec_ok(&mut db, "INSERT INTO t1 VALUES (1, 2)");
        exec_ok(&mut db, "CREATE VIEW nv AS SELECT * FROM t1");
        exec_ok(&mut db, "CREATE TEMP TABLE tt(c INT)");
        exec_ok(&mut db, "INSERT INTO temp.tt VALUES (7)");
        db
    }

    /// The restriction must not be shared between threads executing against
    /// one `&Database`: concurrent view-body executions must not corrupt each
    /// other or leak a stuck restriction into later unrelated queries.
    #[test]
    fn view_body_restriction_is_not_shared_across_concurrent_readers() {
        let db = create_test_db();

        const THREADS: usize = 8;
        const ITERATIONS: usize = 400;

        let errors = AtomicUsize::new(0);
        let mismatches = AtomicUsize::new(0);
        let stuck_after = AtomicUsize::new(0);

        let db_ref = &db;
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    for i in 0..ITERATIONS {
                        // Alternate the view query (which activates the
                        // restriction) with a temp-table query (which is only
                        // resolvable while the restriction is inactive), so a
                        // leak between the two is observed immediately.
                        let (sql, expected) = if i % 2 == 0 {
                            ("SELECT * FROM nv", 2)
                        } else {
                            ("SELECT * FROM tt", 1)
                        };
                        match db_ref.query(sql) {
                            Ok(result) => {
                                if result.rows.len() != 1 || result.rows[0].values.len() != expected
                                {
                                    mismatches.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }

                    // The restriction is per-thread, so each worker checks its
                    // own cell: after a balanced run of set/restore pairs it
                    // must be back to `None`. A stuck value here is the
                    // thread-scoped form of the reviewer's
                    // `stuck_restriction_after=Some("main")`.
                    if db_ref.catalog.unqualified_resolution_restricted_to().is_some() {
                        stuck_after.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });

        assert_eq!(
            errors.load(Ordering::Relaxed),
            0,
            "#6506: concurrent readers sharing one Database must not error — a shared \
             view-body resolution restriction makes unrelated queries fail with 'no such table'"
        );
        assert_eq!(
            mismatches.load(Ordering::Relaxed),
            0,
            "#6506: concurrent readers must not observe rows resolved against the wrong schema"
        );
        assert_eq!(
            stuck_after.load(Ordering::Relaxed),
            0,
            "#6506: the resolution restriction must be back to None after every view-body \
             execution completes, never left stuck on a schema"
        );
        assert_eq!(
            db.catalog.unqualified_resolution_restricted_to(),
            None,
            "#6506: the coordinating thread's restriction must never have been touched at all"
        );
    }

    /// The RAII guard must restore the previous restriction even when the
    /// guarded execution fails — otherwise the leaked restriction poisons
    /// every later query on the same (pooled, long-lived) thread.
    #[test]
    fn failed_view_body_execution_restores_the_previous_restriction() {
        let mut db = create_test_db();
        // Drop the base table out from under `nv`. SQLite (and VibeSQL) allow
        // this — the view survives and only fails when selected — so
        // `SELECT * FROM nv` now drives the guarded view-body execution down
        // its error path, which must still restore the restriction.
        exec_ok(&mut db, "DROP TABLE t1");

        assert!(
            db.query("SELECT * FROM nv").is_err(),
            "selecting through a view whose base table was dropped must fail"
        );

        assert_eq!(
            db.catalog.unqualified_resolution_restricted_to(),
            None,
            "#6506: a failing view-body execution must still restore the previous restriction"
        );

        // And the database is still usable afterward: the temp-only table
        // resolves, which it could not if the restriction had leaked.
        assert!(
            db.query("SELECT * FROM tt").is_ok(),
            "#6506: an unrelated query must still resolve after a failed view-body execution"
        );
    }
}
