//! INSERT uniqueness enforcement honors per-key-part COLLATE in PRIMARY KEY /
//! UNIQUE key lists (issue #5881).
//!
//! Before this fix, `PRIMARY KEY(a COLLATE nocase)` (and the `UNIQUE(a COLLATE
//! nocase)` table-constraint variant) parsed the COLLATE token but never applied
//! it when detecting duplicate keys on INSERT: a case-variant duplicate was
//! wrongly accepted. These tests pin the SQLite-compatible behavior: the
//! collation declared on the key part (falling back to the column's declared
//! collation, then BINARY) is honored when checking uniqueness.

use vibesql_executor::{CreateTableExecutor, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Run a CREATE TABLE followed by INSERTs. Returns the result of the LAST
/// statement so callers can assert success/failure of the final INSERT.
fn run(sql: &str) -> Result<usize, vibesql_executor::ExecutorError> {
    let mut db = Database::new();
    let mut last: Result<usize, vibesql_executor::ExecutorError> = Ok(0);
    for stmt_sql in sql.split(';') {
        let trimmed = stmt_sql.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("parse");
        last = match stmt {
            vibesql_ast::Statement::CreateTable(c) => {
                CreateTableExecutor::execute(&c, &mut db).expect("CREATE TABLE");
                Ok(0)
            }
            vibesql_ast::Statement::Insert(i) => InsertExecutor::execute(&mut db, &i),
            other => panic!("unexpected statement: {other:?}"),
        };
    }
    last
}

/// Count rows physically present in `table` after a run.
fn row_count(sql: &str, table: &str) -> usize {
    let mut db = Database::new();
    for stmt_sql in sql.split(';') {
        let trimmed = stmt_sql.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("parse");
        match stmt {
            vibesql_ast::Statement::CreateTable(c) => {
                CreateTableExecutor::execute(&c, &mut db).expect("CREATE TABLE");
            }
            vibesql_ast::Statement::Insert(i) => {
                // Ignore INSERT errors so we can count how many rows survived.
                let _ = InsertExecutor::execute(&mut db, &i);
            }
            other => panic!("unexpected statement: {other:?}"),
        }
    }
    db.get_table(table).expect("table").scan().len()
}

fn is_unique_violation(result: &Result<usize, vibesql_executor::ExecutorError>) -> bool {
    matches!(result, Err(e) if e.to_string().contains("UNIQUE constraint failed"))
}

#[test]
fn nocase_pk_rejects_case_variant_duplicate() {
    // The exact reproducer from issue #5881.
    let result = run("CREATE TABLE t(a, b, PRIMARY KEY(a COLLATE nocase)) WITHOUT ROWID;
         INSERT INTO t VALUES('a', 1);
         INSERT INTO t VALUES('A', 2)");
    assert!(is_unique_violation(&result), "expected UNIQUE violation, got {result:?}");
    assert_eq!(
        row_count(
            "CREATE TABLE t(a, b, PRIMARY KEY(a COLLATE nocase)) WITHOUT ROWID;
             INSERT INTO t VALUES('a', 1);
             INSERT INTO t VALUES('A', 2)",
            "t",
        ),
        1,
        "only the first row should survive"
    );
}

#[test]
fn nocase_unique_constraint_rejects_case_variant_duplicate() {
    let result = run("CREATE TABLE t(a, b, UNIQUE(a COLLATE nocase));
         INSERT INTO t VALUES('hello', 1);
         INSERT INTO t VALUES('HELLO', 2)");
    assert!(is_unique_violation(&result), "expected UNIQUE violation, got {result:?}");
}

#[test]
fn binary_pk_still_allows_case_variants() {
    // Default (BINARY) PK: 'a' and 'A' are distinct — both insert.
    let result = run("CREATE TABLE t(a, b, PRIMARY KEY(a)) WITHOUT ROWID;
         INSERT INTO t VALUES('a', 1);
         INSERT INTO t VALUES('A', 2)");
    assert!(result.is_ok(), "BINARY PK must accept case variants, got {result:?}");
    assert_eq!(
        row_count(
            "CREATE TABLE t(a, b, PRIMARY KEY(a)) WITHOUT ROWID;
             INSERT INTO t VALUES('a', 1);
             INSERT INTO t VALUES('A', 2)",
            "t",
        ),
        2,
    );
}

#[test]
fn nocase_pk_allows_genuinely_distinct_values() {
    let result = run("CREATE TABLE t(a, b, PRIMARY KEY(a COLLATE nocase)) WITHOUT ROWID;
         INSERT INTO t VALUES('abc', 1);
         INSERT INTO t VALUES('xyz', 2)");
    assert!(result.is_ok(), "distinct values must insert, got {result:?}");
}

#[test]
fn nocase_pk_rejects_case_variant_within_a_single_batch() {
    // Both rows arrive in one multi-row VALUES list — the batch dedup path must
    // also honor the collation.
    let result = run("CREATE TABLE t(a, b, PRIMARY KEY(a COLLATE nocase)) WITHOUT ROWID;
         INSERT INTO t VALUES('a', 1), ('A', 2)");
    assert!(is_unique_violation(&result), "expected batch UNIQUE violation, got {result:?}");
}

#[test]
fn rtrim_unique_treats_trailing_space_variants_as_equal() {
    let result = run("CREATE TABLE t(a, UNIQUE(a COLLATE rtrim));
         INSERT INTO t VALUES('x');
         INSERT INTO t VALUES('x   ')");
    assert!(is_unique_violation(&result), "RTRIM should collide 'x'/'x   ', got {result:?}");
}

#[test]
fn key_part_collation_falls_back_to_column_declared_collation() {
    // No explicit key-part COLLATE, but the column itself is declared NOCASE:
    // the effective PK collation must fall back to the column's collation.
    let result = run("CREATE TABLE t(a TEXT COLLATE nocase, b, PRIMARY KEY(a)) WITHOUT ROWID;
         INSERT INTO t VALUES('a', 1);
         INSERT INTO t VALUES('A', 2)");
    assert!(is_unique_violation(&result), "expected fallback to column NOCASE, got {result:?}");
}

#[test]
fn composite_pk_with_one_nocase_part() {
    // ('a',1) and ('A',1) collide (nocase on a, binary on b).
    let collide = run("CREATE TABLE t(a, b, PRIMARY KEY(a COLLATE nocase, b)) WITHOUT ROWID;
         INSERT INTO t VALUES('a', 1);
         INSERT INTO t VALUES('A', 1)");
    assert!(is_unique_violation(&collide), "composite ('A',1) must collide, got {collide:?}");

    // ('a',1) and ('A',2) do NOT collide — the binary second part differs.
    let distinct = run("CREATE TABLE t(a, b, PRIMARY KEY(a COLLATE nocase, b)) WITHOUT ROWID;
         INSERT INTO t VALUES('a', 1);
         INSERT INTO t VALUES('A', 2)");
    assert!(distinct.is_ok(), "composite ('A',2) must be distinct, got {distinct:?}");
}

#[test]
fn nocase_pk_reports_qualified_column_in_error() {
    let result = run("CREATE TABLE t(a, b, PRIMARY KEY(a COLLATE nocase)) WITHOUT ROWID;
         INSERT INTO t VALUES('a', 1);
         INSERT INTO t VALUES('A', 2)");
    let msg = result.expect_err("should fail").to_string();
    assert!(msg.contains("UNIQUE constraint failed: t.a"), "unexpected message: {msg}");
}

#[test]
fn nocase_unique_allows_distinct_and_preserves_binary_numeric_keys() {
    // Sanity: a non-text key under a nocase-declared constraint is compared by
    // exact equality (collation only affects text), so distinct integers insert.
    let mut db = Database::new();
    for stmt_sql in [
        "CREATE TABLE t(a, UNIQUE(a COLLATE nocase))",
        "INSERT INTO t VALUES(1)",
        "INSERT INTO t VALUES(2)",
    ] {
        let stmt = Parser::parse_sql(stmt_sql).expect("parse");
        match stmt {
            vibesql_ast::Statement::CreateTable(c) => {
                CreateTableExecutor::execute(&c, &mut db).expect("CREATE TABLE");
            }
            vibesql_ast::Statement::Insert(i) => {
                InsertExecutor::execute(&mut db, &i).expect("INSERT");
            }
            _ => unreachable!(),
        }
    }
    let rows = db.get_table("t").expect("table").scan();
    assert_eq!(rows.len(), 2);
    // Confirm both integer values are present.
    let mut vals: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.values.first() {
            Some(SqlValue::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    vals.sort_unstable();
    assert_eq!(vals, vec![1, 2]);
}
