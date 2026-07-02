//! Regression tests for issue #5802 (found by SQLite's fuzz.test): sorting a
//! mixed-type column (integers, bigints, doubles, text, blobs, NULLs) with
//! roughly 65 or more rows panicked with "user-provided comparison function
//! does not correctly implement a total order".
//!
//! Root cause: the ORDER BY comparator compared same-variant numerics exactly
//! (i64 cmp) but fell back to lossy `as f64` casts for cross-variant numeric
//! pairs (Integer vs Bigint, Bigint vs Double, ...). At the f64 precision
//! boundary (|x| >= 2^53) this produced non-transitive triples, e.g.
//! Bigint(2^53) < Bigint(2^53+1) exactly, while both compared Equal to
//! Double(2^53 as f64). NaN also collapsed to Equal against every numeric.
//!
//! The original fuzz reproduction only fired after WAL crash recovery because
//! the recovered database faithfully preserves the fuzzer-created mix of
//! internal SqlValue variants (Integer vs Bigint vs Double), a mix that plain
//! SQL literals do not recreate (all i64 literals parse to the same variant).
//! This test injects the variant mix directly through the storage API and then
//! sorts through the real SELECT ... ORDER BY path.

use vibesql_executor::{CreateTableExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{SqlValue, StringValue};

fn setup_db() -> Database {
    let mut db = Database::new();

    let create = Parser::parse_sql("CREATE TABLE def(c)").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    // 112 rows (matching the fuzz.test reproduction size) with an adversarial
    // mix of SqlValue variants around the f64 precision boundary.
    let base = 9_007_199_254_740_992i64; // 2^53
    let mut values = Vec::new();
    for i in 0..8i64 {
        values.push(SqlValue::Bigint(base));
        values.push(SqlValue::Bigint(base + 1));
        values.push(SqlValue::Integer(base));
        values.push(SqlValue::Integer(base + 1));
        values.push(SqlValue::Double(9_007_199_254_740_992.0)); // 2^53 exactly
        values.push(SqlValue::Double(9.007_199_254_740_993e15));
        values.push(SqlValue::Integer(-2_147_483_648));
        values.push(SqlValue::Integer(i));
        values.push(SqlValue::Unsigned(u64::MAX));
        values.push(SqlValue::Real(0.5 + i as f64));
        values.push(SqlValue::Varchar(StringValue::from(format!("text-{i}"))));
        values.push(SqlValue::Blob(vec![i as u8, 0xff, 0x00]));
        values.push(SqlValue::Blob((-2_147_483_648i64).to_string().into_bytes()));
        values.push(SqlValue::Null);
    }
    assert_eq!(values.len(), 112);

    for v in values {
        db.insert_row("def", Row::new(vec![v])).unwrap();
    }

    db
}

fn execute_query(db: &Database, sql: &str) -> Vec<Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Type class in SQLite ordering: NULL < numeric < text < blob.
fn class_of(v: &SqlValue) -> u8 {
    match v {
        SqlValue::Null => 0,
        SqlValue::Integer(_)
        | SqlValue::Bigint(_)
        | SqlValue::Smallint(_)
        | SqlValue::Unsigned(_)
        | SqlValue::Float(_)
        | SqlValue::Real(_)
        | SqlValue::Double(_)
        | SqlValue::Numeric(_) => 1,
        SqlValue::Character(_) | SqlValue::Varchar(_) => 2,
        SqlValue::Blob(_) => 3,
        other => panic!("unexpected value class in result: {other:?}"),
    }
}

#[test]
fn order_by_mixed_type_column_does_not_panic() {
    let db = setup_db();

    // Pre-fix: panicked inside sort with "user-provided comparison function
    // does not correctly implement a total order".
    let rows = execute_query(&db, "SELECT ALL c FROM def ORDER BY 1 ASC");
    assert_eq!(rows.len(), 112);

    // ASC default in SQLite: NULLs first, then numeric < text < blob.
    let classes: Vec<u8> = rows.iter().map(|r| class_of(&r.values[0])).collect();
    let mut sorted = classes.clone();
    sorted.sort_unstable();
    assert_eq!(classes, sorted, "classes must appear as NULL < numeric < text < blob");

    // Exact integer ordering at the f64 precision boundary must be preserved:
    // every occurrence of 2^53 (Integer/Bigint/Double alike) must come before
    // every occurrence of 2^53 + 1.
    let pos_2p53: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter_map(|(i, r)| match &r.values[0] {
            SqlValue::Integer(n) | SqlValue::Bigint(n) if *n == 9_007_199_254_740_992 => Some(i),
            SqlValue::Double(d) if *d == 9_007_199_254_740_992.0 => Some(i),
            _ => None,
        })
        .collect();
    let pos_2p53_plus_1: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter_map(|(i, r)| match &r.values[0] {
            SqlValue::Integer(n) | SqlValue::Bigint(n) if *n == 9_007_199_254_740_993 => Some(i),
            _ => None,
        })
        .collect();
    assert!(!pos_2p53.is_empty() && !pos_2p53_plus_1.is_empty());
    let max_2p53 = pos_2p53.iter().max().unwrap();
    let min_2p53_plus_1 = pos_2p53_plus_1.iter().min().unwrap();
    assert!(max_2p53 < min_2p53_plus_1, "2^53 (any variant) must sort strictly before 2^53 + 1");
}

#[test]
fn order_by_mixed_type_column_desc_does_not_panic() {
    let db = setup_db();

    let rows = execute_query(&db, "SELECT ALL c FROM def ORDER BY 1 DESC");
    assert_eq!(rows.len(), 112);

    // DESC default in SQLite: blob > text > numeric, NULLs last.
    let classes: Vec<u8> = rows.iter().map(|r| class_of(&r.values[0])).collect();
    let mut sorted = classes.clone();
    sorted.sort_unstable();
    sorted.reverse();
    assert_eq!(classes, sorted, "DESC classes must appear as blob > text > numeric > NULL");
}
