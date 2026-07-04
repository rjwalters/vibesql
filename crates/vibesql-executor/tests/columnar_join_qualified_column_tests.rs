//! Regression tests for issue #5819
//!
//! In a join of 3+ tables, a qualified column reference (`b1.id`) used in a
//! join predicate between the 2nd and 3rd tables resolved to the FIRST table's
//! same-named column when the first table also had a column of that name.
//!
//! Root cause: the columnar join fast path
//! (`columnar_execution::join_helpers::resolve_join_column_indices`) dropped
//! the table qualifier and resolved the joined-side join column with
//! `CombinedSchema::get_column_index(None, col)`, which falls back to
//! leftmost-name matching. With the ubiquitous `id`-on-every-table schema
//! shape, `b1.id` resolved to `a1.id`, silently joining on the wrong column:
//! inner/comma joins returned 0 rows and LEFT JOIN chains lost matches
//! (NULL-padded rows that should have matched).
//!
//! The row-oriented join path was always correct; these tests pin the fixed
//! behavior regardless of which execution path is chosen.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

fn run(db: &Database, sql: &str) -> Vec<Row> {
    let select = parse_select(sql);
    SelectExecutor::new(db).execute(&select).unwrap()
}

/// Build the schema/data from the issue #5819 reproduction.
///
/// ```sql
/// CREATE TABLE a1(id INTEGER PRIMARY KEY, v);
/// CREATE TABLE b1(id INTEGER PRIMARY KEY, aid, v);
/// CREATE TABLE c1(id INTEGER PRIMARY KEY, bid, v);
/// INSERT INTO a1 VALUES(1,'a1'),(2,'a2'),(3,'a3');
/// INSERT INTO b1 VALUES(10,1,'b1'),(11,2,'b2');
/// INSERT INTO c1 VALUES(100,10,'c1');
/// ```
///
/// Every table has an `id` column, so a qualified `b1.id` in the second join
/// predicate used to misresolve to `a1.id`.
fn setup_issue_5819_db() -> Database {
    let mut db = Database::new();

    let a1_schema = TableSchema::with_primary_key(
        "A1".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("v".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(a1_schema).unwrap();

    let b1_schema = TableSchema::with_primary_key(
        "B1".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("aid".to_string(), DataType::Integer, true),
            ColumnSchema::new("v".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(b1_schema).unwrap();

    let c1_schema = TableSchema::with_primary_key(
        "C1".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("bid".to_string(), DataType::Integer, true),
            ColumnSchema::new("v".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(c1_schema).unwrap();

    for (id, v) in [(1, "a1"), (2, "a2"), (3, "a3")] {
        db.insert_row("A1", Row::new(vec![SqlValue::Integer(id), SqlValue::Varchar(v.into())]))
            .unwrap();
    }
    for (id, aid, v) in [(10, 1, "b1"), (11, 2, "b2")] {
        db.insert_row(
            "B1",
            Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Integer(aid),
                SqlValue::Varchar(v.into()),
            ]),
        )
        .unwrap();
    }
    db.insert_row(
        "C1",
        Row::new(vec![
            SqlValue::Integer(100),
            SqlValue::Integer(10),
            SqlValue::Varchar("c1".into()),
        ]),
    )
    .unwrap();

    db
}

/// Q1 from the issue: LEFT JOIN chain. The bug NULL-padded the c1 columns of
/// the first row (b1.id=10 matched c1.bid=10, but the misresolved key a1.id=1
/// found no match).
///
/// sqlite3 reference:
/// ```
/// 1|10|100
/// 2|11|NULL
/// 3|NULL|NULL
/// ```
#[test]
fn test_left_join_chain_qualified_id_keeps_match() {
    let db = setup_issue_5819_db();

    let sql = "SELECT a1.id, b1.id, c1.id FROM a1 LEFT JOIN b1 ON b1.aid=a1.id \
               LEFT JOIN c1 ON c1.bid=b1.id ORDER BY 1";
    let result = run(&db, sql);

    assert_eq!(result.len(), 3, "LEFT JOIN chain must preserve all 3 a1 rows");

    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Integer(10));
    assert_eq!(
        result[0].values[2],
        SqlValue::Integer(100),
        "c1 match must not be lost (bug: c1.bid=b1.id misresolved b1.id to a1.id)"
    );

    assert_eq!(result[1].values[0], SqlValue::Integer(2));
    assert_eq!(result[1].values[1], SqlValue::Integer(11));
    assert_eq!(result[1].values[2], SqlValue::Null);

    assert_eq!(result[2].values[0], SqlValue::Integer(3));
    assert_eq!(result[2].values[1], SqlValue::Null);
    assert_eq!(result[2].values[2], SqlValue::Null);
}

/// Q2 from the issue: explicit INNER JOIN chain. The bug returned 0 rows.
///
/// sqlite3 reference: `1|10|100`
#[test]
fn test_inner_join_chain_qualified_id() {
    let db = setup_issue_5819_db();

    let sql = "SELECT a1.id, b1.id, c1.id FROM a1 JOIN b1 ON b1.aid=a1.id \
               JOIN c1 ON c1.bid=b1.id";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1, "inner join chain must return exactly 1 row (bug: 0 rows)");
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Integer(10));
    assert_eq!(result[0].values[2], SqlValue::Integer(100));
}

/// Q2 with the join predicate written in the flipped order (`b1.id=c1.bid`).
/// Both orders must resolve the qualifier correctly.
#[test]
fn test_inner_join_chain_qualified_id_flipped_predicate() {
    let db = setup_issue_5819_db();

    let sql = "SELECT a1.id, b1.id, c1.id FROM a1 JOIN b1 ON a1.id=b1.aid \
               JOIN c1 ON b1.id=c1.bid";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Integer(10));
    assert_eq!(result[0].values[2], SqlValue::Integer(100));
}

/// Q3 from the issue: comma join + WHERE (reorder path). The bug returned 0 rows.
///
/// sqlite3 reference: `1|10|100`
#[test]
fn test_comma_join_where_qualified_id() {
    let db = setup_issue_5819_db();

    let sql = "SELECT a1.id, b1.id, c1.id FROM a1, b1, c1 \
               WHERE b1.aid=a1.id AND c1.bid=b1.id";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1, "comma join must return exactly 1 row (bug: 0 rows)");
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Integer(10));
    assert_eq!(result[0].values[2], SqlValue::Integer(100));
}

/// The misresolution proof from the issue: with an extra a1 row whose id equals
/// c1.bid (10), the buggy path paired a1.id=10 with c1.bid=10 while every b1
/// column was effectively unmatched. After the fix the only row is the correct
/// one.
///
/// sqlite3 reference: `1|10|100|10`
#[test]
fn test_inner_join_chain_with_decoy_a1_row() {
    let mut db = setup_issue_5819_db();
    db.insert_row("A1", Row::new(vec![SqlValue::Integer(10), SqlValue::Varchar("a10".into())]))
        .unwrap();

    let sql = "SELECT a1.id, b1.id, c1.id, c1.bid FROM a1 JOIN b1 ON b1.aid=a1.id \
               JOIN c1 ON c1.bid=b1.id";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(1), "must join via b1.id, not a1.id=10");
    assert_eq!(result[0].values[1], SqlValue::Integer(10));
    assert_eq!(result[0].values[2], SqlValue::Integer(100));
    assert_eq!(result[0].values[3], SqlValue::Integer(10));
}

/// Aliased tables: the qualifier is the alias, not the base table name.
#[test]
fn test_inner_join_chain_with_aliases() {
    let db = setup_issue_5819_db();

    let sql = "SELECT x.id, y.id, z.id FROM a1 x JOIN b1 y ON y.aid=x.id \
               JOIN c1 z ON z.bid=y.id";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Integer(10));
    assert_eq!(result[0].values[2], SqlValue::Integer(100));
}

/// Duplicate NON-primary-key column name (`w` on all three tables): the fix
/// must not be specific to rowid-alias `id` columns.
#[test]
fn test_duplicate_non_pk_column_name() {
    let mut db = Database::new();

    for (table, cols) in
        [("TA", vec!["w", "v"]), ("TB", vec!["w", "aw", "v"]), ("TC", vec!["w", "bw", "v"])]
    {
        let schema = TableSchema::new(
            table.to_string(),
            cols.iter()
                .map(|c| {
                    if *c == "v" {
                        ColumnSchema::new(
                            c.to_string(),
                            DataType::Varchar { max_length: None },
                            true,
                        )
                    } else {
                        ColumnSchema::new(c.to_string(), DataType::Integer, true)
                    }
                })
                .collect(),
        );
        db.create_table(schema).unwrap();
    }

    for (w, v) in [(1, "a1"), (2, "a2"), (3, "a3")] {
        db.insert_row("TA", Row::new(vec![SqlValue::Integer(w), SqlValue::Varchar(v.into())]))
            .unwrap();
    }
    for (w, aw, v) in [(10, 1, "b1"), (11, 2, "b2")] {
        db.insert_row(
            "TB",
            Row::new(vec![
                SqlValue::Integer(w),
                SqlValue::Integer(aw),
                SqlValue::Varchar(v.into()),
            ]),
        )
        .unwrap();
    }
    db.insert_row(
        "TC",
        Row::new(vec![
            SqlValue::Integer(100),
            SqlValue::Integer(10),
            SqlValue::Varchar("c1".into()),
        ]),
    )
    .unwrap();

    let sql = "SELECT ta.w, tb.w, tc.w FROM ta JOIN tb ON tb.aw=ta.w \
               JOIN tc ON tc.bw=tb.w";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1, "duplicate non-PK column names must resolve by qualifier");
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Integer(10));
    assert_eq!(result[0].values[2], SqlValue::Integer(100));
}

/// Control case that already worked pre-fix (duplicate name only on tables 2
/// and 3; table 1 lacks it): must keep working.
#[test]
fn test_duplicate_only_on_tables_2_and_3_still_correct() {
    let mut db = Database::new();

    let t1 = TableSchema::new(
        "T1".to_string(),
        vec![
            ColumnSchema::new("k".to_string(), DataType::Integer, true),
            ColumnSchema::new("v".to_string(), DataType::Varchar { max_length: None }, true),
        ],
    );
    db.create_table(t1).unwrap();
    let t2 = TableSchema::new(
        "T2".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, true),
            ColumnSchema::new("kref".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(t2).unwrap();
    let t3 = TableSchema::new(
        "T3".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, true),
            ColumnSchema::new("t2ref".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(t3).unwrap();

    db.insert_row("T1", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("x".into())]))
        .unwrap();
    db.insert_row("T2", Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(1)])).unwrap();
    db.insert_row("T3", Row::new(vec![SqlValue::Integer(100), SqlValue::Integer(10)])).unwrap();

    let sql = "SELECT t1.k, t2.id, t3.id FROM t1 JOIN t2 ON t2.kref=t1.k \
               JOIN t3 ON t3.t2ref=t2.id";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Integer(10));
    assert_eq!(result[0].values[2], SqlValue::Integer(100));
}

/// 4-table chain where every table has an `id` column: each hop's qualified
/// join key must resolve against the correct table.
#[test]
fn test_four_table_chain_shared_id() {
    let mut db = setup_issue_5819_db();
    let d1_schema = TableSchema::with_primary_key(
        "D1".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("cid".to_string(), DataType::Integer, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(d1_schema).unwrap();
    db.insert_row("D1", Row::new(vec![SqlValue::Integer(1000), SqlValue::Integer(100)])).unwrap();

    let sql = "SELECT a1.id, b1.id, c1.id, d1.id FROM a1 JOIN b1 ON b1.aid=a1.id \
               JOIN c1 ON c1.bid=b1.id JOIN d1 ON d1.cid=c1.id";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1, "4-table chain must return exactly 1 row");
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Integer(10));
    assert_eq!(result[0].values[2], SqlValue::Integer(100));
    assert_eq!(result[0].values[3], SqlValue::Integer(1000));
}

/// Comma-join spelled with qualifiers on both sides in mixed order, plus the
/// decoy row, exercising the WHERE-equijoin extraction on the reorder path.
#[test]
fn test_comma_join_with_decoy_row() {
    let mut db = setup_issue_5819_db();
    db.insert_row("A1", Row::new(vec![SqlValue::Integer(10), SqlValue::Varchar("a10".into())]))
        .unwrap();

    let sql = "SELECT a1.id, b1.id, c1.id FROM a1, b1, c1 \
               WHERE a1.id=b1.aid AND b1.id=c1.bid";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(1));
    assert_eq!(result[0].values[1], SqlValue::Integer(10));
    assert_eq!(result[0].values[2], SqlValue::Integer(100));
}
