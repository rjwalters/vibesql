//! Regression tests for issue #6575: index range-scan false-equality for an
//! out-of-f64-safe-integer-precision INTEGER literal compared against a
//! REAL-affinity indexed column.
//!
//! `crates/vibesql-storage/src/database/indexes/value_normalization.rs`'s
//! `normalize_for_comparison` casts every numeric `SqlValue` to `Double` via
//! a lossy `as f64` cast before it's used as a BTreeMap range-scan bound.
//! When the *same* lossy cast independently rounds both a WHERE-clause
//! INTEGER literal and (at INSERT time) a REAL-affinity column's stored
//! value to the *same* `Double`, an exclusive bound (`col > literal`) was
//! treating the row's key as equal to the bound and excluding it — even
//! though the true (unrounded) comparison shows the row's value is strictly
//! greater than the literal (because the rounding moved the stored value
//! up). The fix corrects the inclusive/exclusive flag based on an exact
//! comparison between the original literal and its rounded `Double`
//! (`vibesql_storage::database::indexes::value_normalization::normalize_bound_for_range_scan`).
//!
//! Every test below asserts that the **indexed** (UNIQUE, so a BTreeMap
//! range-scan handles the WHERE clause) and **unindexed** (full scan through
//! the general expression evaluator) queries agree, since the general
//! evaluator was already correct per the issue's repro.
//!
//! # Issue #6588: skip-scan and covering-index range scans
//!
//! PR #6587 fixed the false-exclusion/inclusion bug above for
//! `IndexData::range_scan` / `range_scan_limit` / `range_scan_streaming` by
//! routing every call site through `normalize_range_bounds` /
//! `normalize_bound_for_range_scan`, which correct the inclusive/exclusive
//! flag using `total_order_cmp` *before* the lossy `as f64` cast is used as a
//! scan bound. Two structurally identical composite-index scan paths still
//! called `normalize_for_comparison` directly and shared the same bug:
//! `IndexData::skip_scan_range`
//! (`crates/vibesql-storage/src/database/indexes/prefix_match/skip_scan.rs`)
//! and `IndexData::prefix_range_scan_covering` /
//! `prefix_bounded_scan_covering`
//! (`crates/vibesql-storage/src/database/indexes/prefix_match/covering.rs`).
//! Both are fixed below the same way, and the tests below cover them.
//!
//! ## Why these tests call `IndexData` methods directly instead of
//! asserting a top-level `SELECT ... WHERE ...` row count
//!
//! Reaching these two code paths from an ordinary top-level SQL query and
//! observing *only* this fix's effect is confounded by two separate,
//! pre-existing issues discovered while writing this regression test (both
//! out of scope for #6588):
//!
//! 1. `SelectExecutor::execute_fast_path`'s OLTP shortcut chain
//!    (`crates/vibesql-executor/src/select/executor/fast_path/mod.rs`) tries
//!    `try_secondary_index_lookup_fast` *before* `try_covering_index_scan_fast`, and the former
//!    matches (and unconditionally returns) for any WHERE clause with an equality predicate on a
//!    leading index column — exactly the shape a covering-index range scan needs to apply at all —
//!    so `try_covering_index_scan_fast` (and therefore `prefix_range_scan_covering` /
//!    `prefix_bounded_scan_covering`) is not reachable today from a plain top-level `SELECT` when a
//!    matching index exists.
//! 2. Both `try_secondary_index_lookup_fast`'s and `execute_skip_scan`'s post-filters re-evaluate
//!    the *entire* WHERE clause (including the out-of-precision comparison) via
//!    `crate::evaluator::compiled::CompiledPredicate`, whose own `compare_range`
//!    (`crates/vibesql-executor/src/evaluator/compiled.rs`) casts integer literals to `f64` with
//!    the same lossy, uncorrected cast that this issue's fix corrects for the index bound — an
//!    independent precision bug in a different module that would silently re-exclude the very row
//!    this fix restores, masking the fix's effect in an end-to-end row-count assertion regardless
//!    of whether the index-level bug is fixed.
//!
//! Calling `IndexData::skip_scan_range` / `prefix_range_scan_covering` /
//! `prefix_bounded_scan_covering` directly (via the public
//! `Database::get_index_data`) isolates exactly the code this issue fixes,
//! while still inserting data through the real executor (`INSERT`) so REAL
//! affinity coercion applies identically to production. The
//! `*_plan_is_selected_for_out_of_precision_range_predicate` tests below
//! additionally confirm via `EXPLAIN` / `EXPLAIN QUERY PLAN` that the
//! optimizer genuinely chooses a skip-scan / covering-index plan for these
//! query shapes, rather than falling back to a plain range scan.

use vibesql_executor::{
    CreateIndexExecutor, CreateTableExecutor, ExplainExecutor, InsertExecutor, SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn exec(db: &mut Database, sql: &str) -> Result<(), vibesql_executor::ExecutorError> {
    match Parser::parse_sql(sql).expect("test SQL should parse") {
        vibesql_ast::Statement::CreateTable(s) => CreateTableExecutor::execute(&s, db).map(|_| ()),
        vibesql_ast::Statement::CreateIndex(s) => CreateIndexExecutor::execute(&s, db).map(|_| ()),
        vibesql_ast::Statement::Insert(s) => InsertExecutor::execute(db, &s).map(|_| ()),
        other => panic!("unexpected statement in test: {other:?}"),
    }
}

fn select_rows(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    SelectExecutor::new(db).execute(&s).unwrap()
}

/// Primary repro from issue #6575: `3175546974276630385 < c0` on a
/// REAL-affinity UNIQUE-indexed column must return the row, matching the
/// (already-correct) unindexed full-scan behavior.
#[test]
fn indexed_exclusive_lower_bound_matches_unindexed_for_out_of_precision_literal() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (3175546974276630385)").unwrap();

    // Sanity: the general evaluator (no index involvement) agrees the
    // literal is less than the stored (rounded) value.
    let eval_rows = select_rows(&db, "SELECT 3175546974276630385 < c0 FROM t0");
    assert_eq!(eval_rows.len(), 1);
    assert_eq!(eval_rows[0].get(0).unwrap(), &vibesql_types::SqlValue::Boolean(true));

    // The indexed WHERE-clause path must agree: exactly 1 row.
    let where_rows = select_rows(&db, "SELECT 1 FROM t0 WHERE 3175546974276630385 < c0");
    assert_eq!(
        where_rows.len(),
        1,
        "indexed range scan must not falsely exclude the row due to precision-rounding equality"
    );
}

/// Same repro, phrased as `c0 > literal` (column on the left) rather than
/// `literal < c0`, to cover both `extract_range_predicate` branches.
#[test]
fn indexed_exclusive_lower_bound_column_on_left() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (3175546974276630385)").unwrap();

    let rows = select_rows(&db, "SELECT 1 FROM t0 WHERE c0 > 3175546974276630385");
    assert_eq!(rows.len(), 1);
}

/// `col >= literal` must NOT match when the literal rounds *up* past the
/// stored value and the true value is strictly less than the literal is not
/// the case here — this exercises the inclusive lower-bound branch, which
/// should already have matched before the fix (no false-equality risk) and
/// must continue to match after it.
#[test]
fn indexed_inclusive_lower_bound_out_of_precision_literal_still_matches() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (3175546974276630385)").unwrap();

    let rows = select_rows(&db, "SELECT 1 FROM t0 WHERE c0 >= 3175546974276630385");
    assert_eq!(rows.len(), 1, "inclusive lower bound must still find the row");
}

/// Upper-bound mirror: a literal that rounds *down* when cast to `Double`
/// must still satisfy a strict `<` comparison against the (larger, exact)
/// literal for a row whose stored value is the rounded-down double.
#[test]
fn indexed_exclusive_upper_bound_matches_unindexed_for_out_of_precision_literal() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(c0 REAL UNIQUE)").unwrap();
    // 2^53 + 1 rounds DOWN to 2^53 when cast through f64 (INTEGER -> REAL
    // affinity coercion at INSERT time uses the same lossy `as f64` cast).
    exec(&mut db, "INSERT INTO t1(c0) VALUES (9007199254740992)").unwrap(); // 2^53, stored exactly

    let literal = "9007199254740993"; // 2^53 + 1, rounds down to 2^53 as f64

    let eval_rows = select_rows(&db, &format!("SELECT c0 < {literal} FROM t1"));
    assert_eq!(eval_rows.len(), 1);
    assert_eq!(eval_rows[0].get(0).unwrap(), &vibesql_types::SqlValue::Boolean(true));

    let where_rows = select_rows(&db, &format!("SELECT 1 FROM t1 WHERE c0 < {literal}"));
    assert_eq!(
        where_rows.len(),
        1,
        "indexed range scan must not falsely exclude a row whose exact value is < the literal"
    );
}

/// A row whose value is *not* on the boundary must still be excluded by an
/// out-of-precision-literal bound (no over-broadening from the fix).
#[test]
fn indexed_bound_still_excludes_non_matching_rows() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (3175546974276630385)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (1.0)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (-3175546974276630385)").unwrap();

    let rows = select_rows(&db, "SELECT c0 FROM t0 WHERE c0 > 3175546974276630385");
    assert_eq!(rows.len(), 1, "only the boundary row should qualify, not the smaller rows");
}

// ============================================================================
// Issue #6588: IndexData::skip_scan_range
// ============================================================================

/// Primary repro for `skip_scan_range`: an exclusive lower bound
/// (`b > 3175546974276630385`) on the non-prefix column of a composite
/// index `(a, b)` must not falsely exclude a row whose rounded `Double`
/// value is genuinely greater than the exact literal.
#[test]
fn skip_scan_range_exclusive_lower_bound_matches_out_of_precision_literal() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t_skip0(a INTEGER, b REAL)").unwrap();
    exec(&mut db, "INSERT INTO t_skip0(a, b) VALUES (0, 3175546974276630385)").unwrap();
    exec(&mut db, "CREATE INDEX idx_t_skip0_ab ON t_skip0(a, b)").unwrap();

    // Sanity: the general (non-indexed) evaluator agrees the literal is less
    // than the stored (rounded) value, exactly as in the plain range_scan
    // repro above.
    let eval_rows = select_rows(&db, "SELECT 3175546974276630385 < b FROM t_skip0 WHERE a = 0");
    assert_eq!(eval_rows.len(), 1);
    assert_eq!(eval_rows[0].get(0).unwrap(), &vibesql_types::SqlValue::Boolean(true));

    let index_data = db.get_index_data("idx_t_skip0_ab").expect("index data must exist");

    // filter_column_idx = 1: column `b`, the non-prefix column of composite
    // index (a, b). Exclusive lower bound (`b > literal`).
    let matches = index_data.skip_scan_range(
        1,
        Some(&vibesql_types::SqlValue::Integer(3175546974276630385)),
        false,
        None,
        true,
    );

    assert_eq!(
        matches.len(),
        1,
        "skip_scan_range must not falsely exclude the boundary row due to precision-rounding equality"
    );
}

/// Upper-bound mirror: a literal that rounds *down* when cast to `Double`
/// must still satisfy a strict `<` comparison for a row whose stored value
/// is exactly the rounded-down double.
#[test]
fn skip_scan_range_exclusive_upper_bound_matches_out_of_precision_literal() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t_skip1(a INTEGER, b REAL)").unwrap();
    // 2^53, stored exactly. 2^53 + 1 rounds DOWN to 2^53 when cast to f64.
    exec(&mut db, "INSERT INTO t_skip1(a, b) VALUES (0, 9007199254740992)").unwrap();
    exec(&mut db, "CREATE INDEX idx_t_skip1_ab ON t_skip1(a, b)").unwrap();

    let literal = "9007199254740993"; // 2^53 + 1, rounds down to 2^53 as f64
    let eval_rows = select_rows(&db, &format!("SELECT b < {literal} FROM t_skip1 WHERE a = 0"));
    assert_eq!(eval_rows.len(), 1);
    assert_eq!(eval_rows[0].get(0).unwrap(), &vibesql_types::SqlValue::Boolean(true));

    let index_data = db.get_index_data("idx_t_skip1_ab").unwrap();
    let matches = index_data.skip_scan_range(
        1,
        None,
        true,
        Some(&vibesql_types::SqlValue::Integer(9_007_199_254_740_993)),
        false,
    );

    assert_eq!(
        matches.len(),
        1,
        "skip_scan_range must not falsely exclude a row whose exact value is < the literal"
    );
}

/// A row whose value is not on the boundary must still be excluded (no
/// over-broadening from the fix).
#[test]
fn skip_scan_range_still_excludes_non_matching_rows() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t_skip2(a INTEGER, b REAL)").unwrap();
    exec(&mut db, "INSERT INTO t_skip2(a, b) VALUES (0, 3175546974276630385)").unwrap();
    exec(&mut db, "INSERT INTO t_skip2(a, b) VALUES (1, 1.0)").unwrap();
    exec(&mut db, "INSERT INTO t_skip2(a, b) VALUES (2, -3175546974276630385)").unwrap();
    exec(&mut db, "CREATE INDEX idx_t_skip2_ab ON t_skip2(a, b)").unwrap();

    let index_data = db.get_index_data("idx_t_skip2_ab").unwrap();
    let matches = index_data.skip_scan_range(
        1,
        Some(&vibesql_types::SqlValue::Integer(3175546974276630385)),
        false,
        None,
        true,
    );

    assert_eq!(matches.len(), 1, "only the boundary row should qualify, not the smaller rows");
}

/// Confirms the query optimizer genuinely selects a skip-scan plan (via
/// `IndexPlanner::plan_skip_scan`) for a composite-index range predicate on
/// a REAL-affinity non-prefix column with no filter on the prefix column,
/// so the `skip_scan_range` fix above is reachable through
/// `execute_skip_scan`, not just through direct `IndexData` calls.
#[test]
fn skip_scan_plan_is_selected_for_out_of_precision_range_predicate() {
    let mut db = Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "t_skip3".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "a".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "b".to_string(),
                vibesql_types::DataType::Real,
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // 5000 rows over 5 distinct prefix values (low prefix cardinality) makes
    // skip-scan cost-beneficial vs. a table scan; row 0 carries the
    // out-of-precision boundary value.
    for i in 0..5000_i64 {
        let a = i % 5;
        let b = if i == 0 { 3175546974276630385.0_f64 } else { (i as f64) * 3.7 };
        db.insert_row(
            "t_skip3",
            vibesql_storage::Row::new(vec![
                vibesql_types::SqlValue::Integer(a),
                vibesql_types::SqlValue::Real(b),
            ]),
        )
        .unwrap();
    }

    db.create_index(
        "idx_t_skip3_ab".to_string(),
        "t_skip3".to_string(),
        false,
        vec![
            vibesql_ast::IndexColumn::Column {
                column_name: "a".to_string(),
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            },
            vibesql_ast::IndexColumn::Column {
                column_name: "b".to_string(),
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
                collation: None,
                is_quoted: false,
            },
        ],
    )
    .unwrap();

    if let Some(table) = db.get_table_mut("t_skip3") {
        table.analyze();
    }

    let explain_sql = "EXPLAIN SELECT a, b FROM t_skip3 WHERE b > 3175546974276630385";
    let stmt = Parser::parse_sql(explain_sql).expect("EXPLAIN should parse");
    let vibesql_ast::Statement::Explain(explain_stmt) = stmt else {
        panic!("expected EXPLAIN statement");
    };
    let output = ExplainExecutor::execute(&explain_stmt, &db).expect("EXPLAIN failed").to_text();

    assert!(output.contains("Skip Scan"), "expected a Skip Scan plan, got:\n{output}");
}

// ============================================================================
// Issue #6588: IndexData::prefix_range_scan_covering /
// prefix_bounded_scan_covering
// ============================================================================

/// Primary repro for `prefix_range_scan_covering`: an equality prefix (`a =
/// 1`) plus an exclusive lower bound on the trailing column
/// (`b > 3175546974276630385`) must not falsely exclude a row whose rounded
/// `Double` value is genuinely greater than the exact literal.
#[test]
fn prefix_range_scan_covering_exclusive_lower_bound_matches_out_of_precision_literal() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t_cov0(a INTEGER, b REAL)").unwrap();
    exec(&mut db, "INSERT INTO t_cov0(a, b) VALUES (1, 3175546974276630385)").unwrap();
    exec(&mut db, "CREATE INDEX idx_t_cov0_ab ON t_cov0(a, b)").unwrap();

    let eval_rows = select_rows(&db, "SELECT 3175546974276630385 < b FROM t_cov0 WHERE a = 1");
    assert_eq!(eval_rows.len(), 1);
    assert_eq!(eval_rows[0].get(0).unwrap(), &vibesql_types::SqlValue::Boolean(true));

    let index_data = db.get_index_data("idx_t_cov0_ab").expect("index data must exist");

    let results = index_data.prefix_range_scan_covering(
        &[vibesql_types::SqlValue::Integer(1)],
        Some(&vibesql_types::SqlValue::Integer(3175546974276630385)),
        false,
        None,
        true,
    );

    assert_eq!(
        results.len(),
        1,
        "prefix_range_scan_covering must not falsely exclude the boundary row due to \
         precision-rounding equality"
    );
}

/// A row whose value is not on the boundary must still be excluded by
/// `prefix_range_scan_covering` (no over-broadening from the fix).
#[test]
fn prefix_range_scan_covering_still_excludes_non_matching_rows() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t_cov1(a INTEGER, b REAL)").unwrap();
    exec(&mut db, "INSERT INTO t_cov1(a, b) VALUES (1, 3175546974276630385)").unwrap();
    exec(&mut db, "INSERT INTO t_cov1(a, b) VALUES (1, 1.0)").unwrap();
    exec(&mut db, "CREATE INDEX idx_t_cov1_ab ON t_cov1(a, b)").unwrap();

    let index_data = db.get_index_data("idx_t_cov1_ab").unwrap();
    let results = index_data.prefix_range_scan_covering(
        &[vibesql_types::SqlValue::Integer(1)],
        Some(&vibesql_types::SqlValue::Integer(3175546974276630385)),
        false,
        None,
        true,
    );

    assert_eq!(results.len(), 1, "only the boundary row should qualify, not the smaller row");
}

/// Primary repro for `prefix_bounded_scan_covering` (upper-bound-only
/// signature): a literal that rounds *down* when cast to `Double` must
/// still satisfy a strict `<` comparison for a row whose stored value is
/// exactly the rounded-down double.
#[test]
fn prefix_bounded_scan_covering_exclusive_upper_bound_matches_out_of_precision_literal() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t_cov2(a INTEGER, b REAL)").unwrap();
    // 2^53, stored exactly. 2^53 + 1 rounds DOWN to 2^53 when cast to f64.
    exec(&mut db, "INSERT INTO t_cov2(a, b) VALUES (1, 9007199254740992)").unwrap();
    exec(&mut db, "CREATE INDEX idx_t_cov2_ab ON t_cov2(a, b)").unwrap();

    let literal = "9007199254740993"; // 2^53 + 1, rounds down to 2^53 as f64
    let eval_rows = select_rows(&db, &format!("SELECT b < {literal} FROM t_cov2 WHERE a = 1"));
    assert_eq!(eval_rows.len(), 1);
    assert_eq!(eval_rows[0].get(0).unwrap(), &vibesql_types::SqlValue::Boolean(true));

    let index_data = db.get_index_data("idx_t_cov2_ab").unwrap();
    let results = index_data.prefix_bounded_scan_covering(
        &[vibesql_types::SqlValue::Integer(1)],
        &vibesql_types::SqlValue::Integer(9_007_199_254_740_993),
        false,
    );

    assert_eq!(
        results.len(),
        1,
        "prefix_bounded_scan_covering must not falsely exclude a row whose exact value is < \
         the literal"
    );
}

/// Confirms the query optimizer genuinely selects a covering-index plan
/// (`SEARCH ... USING COVERING INDEX ...`) for a query answerable entirely
/// from an index whose trailing column carries a bounded range predicate on
/// a REAL-affinity column, so `prefix_range_scan_covering` /
/// `prefix_bounded_scan_covering` are reachable plan choices for this query
/// shape (see the module-level doc comment for why the fast-path runtime
/// currently routes an equivalent top-level `SELECT` elsewhere instead).
#[test]
fn covering_index_plan_is_selected_for_out_of_precision_range_predicate() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t_cov3(a INTEGER, b REAL)").unwrap();
    exec(&mut db, "INSERT INTO t_cov3(a, b) VALUES (1, 3175546974276630385)").unwrap();
    exec(&mut db, "INSERT INTO t_cov3(a, b) VALUES (1, 1.0)").unwrap();
    exec(&mut db, "INSERT INTO t_cov3(a, b) VALUES (2, 2.0)").unwrap();
    exec(&mut db, "CREATE INDEX idx_t_cov3_ab ON t_cov3(a, b)").unwrap();

    let explain_sql =
        "EXPLAIN QUERY PLAN SELECT a, b FROM t_cov3 WHERE a = 1 AND b > 3175546974276630385";
    let stmt = Parser::parse_sql(explain_sql).expect("EXPLAIN QUERY PLAN should parse");
    let vibesql_ast::Statement::Explain(explain_stmt) = stmt else {
        panic!("expected EXPLAIN statement");
    };
    let output =
        ExplainExecutor::execute(&explain_stmt, &db).expect("EXPLAIN failed").to_sqlite_eqp();

    assert!(
        output.contains("COVERING INDEX"),
        "expected the optimizer to choose a covering-index plan (query is answerable entirely \
         from index columns a, b), got:\n{output}"
    );
}

// ===========================================================================
// Issue #6586: equality / IN-list point-lookup false-equality
// ===========================================================================

/// Primary repro from issue #6586: `c0 = 3175546974276630385` on a
/// REAL-affinity UNIQUE-indexed column must return **no** rows, matching the
/// general (unindexed) evaluator, which correctly reports the comparison as
/// false because the stored REAL rounded up to 3175546974276630528.
#[test]
fn indexed_equality_matches_unindexed_for_out_of_precision_literal() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (3175546974276630385)").unwrap();

    // Sanity: the general evaluator says the values are NOT equal.
    let eval_rows = select_rows(&db, "SELECT c0 = 3175546974276630385 FROM t0");
    assert_eq!(eval_rows.len(), 1);
    assert_eq!(eval_rows[0].get(0).unwrap(), &vibesql_types::SqlValue::Boolean(false));

    let where_rows = select_rows(&db, "SELECT 1 FROM t0 WHERE c0 = 3175546974276630385");
    assert_eq!(
        where_rows.len(),
        0,
        "indexed equality probe must not falsely match a row whose exact value differs"
    );
}

/// IN-list variant of the same repro (`multi_lookup` probe path).
#[test]
fn indexed_in_list_matches_unindexed_for_out_of_precision_literal() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (3175546974276630385)").unwrap();

    let where_rows = select_rows(&db, "SELECT 1 FROM t0 WHERE c0 IN (3175546974276630385)");
    assert_eq!(where_rows.len(), 0, "indexed IN-list probe must not falsely match");

    // Multi-element IN list exercises the dedup + multi-key path.
    let multi = select_rows(&db, "SELECT 1 FROM t0 WHERE c0 IN (1, 3175546974276630385, 2)");
    assert_eq!(multi.len(), 0, "indexed multi-value IN-list probe must not falsely match");
}

/// Positive control: probing with the *exact* stored REAL value must still
/// find the row through both the equality and IN-list index paths.
#[test]
fn indexed_equality_still_matches_exact_real_value() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (3175546974276630385)").unwrap();

    // 3175546974276630528 is the f64 the INTEGER literal rounds to, so it is
    // the exact stored REAL value and must match.
    let eq = select_rows(&db, "SELECT 1 FROM t0 WHERE c0 = 3175546974276630528");
    assert_eq!(eq.len(), 1, "equality against the exact stored value must still match");

    let in_list = select_rows(&db, "SELECT 1 FROM t0 WHERE c0 IN (3175546974276630528)");
    assert_eq!(in_list.len(), 1, "IN-list against the exact stored value must still match");
}

/// No false *negatives*: an INTEGER-affinity column storing an
/// out-of-f64-precision integer must still be found by an exact equality /
/// IN-list probe with that same integer literal.
#[test]
fn indexed_equality_on_integer_column_still_matches_huge_integer() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t2(c0 INTEGER UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t2(c0) VALUES (3175546974276630385)").unwrap();

    let eq = select_rows(&db, "SELECT 1 FROM t2 WHERE c0 = 3175546974276630385");
    assert_eq!(eq.len(), 1, "INTEGER column must still match its own exact value");

    let in_list = select_rows(&db, "SELECT 1 FROM t2 WHERE c0 IN (3175546974276630385)");
    assert_eq!(in_list.len(), 1, "INTEGER column IN-list must still match its own exact value");
}

/// Small-magnitude values (well inside f64 safe-integer range) are unaffected
/// by the exact re-verification and must keep matching through both paths.
#[test]
fn indexed_equality_unaffected_for_in_precision_values() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t3(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t3(c0) VALUES (42)").unwrap();
    exec(&mut db, "INSERT INTO t3(c0) VALUES (2.5)").unwrap();

    assert_eq!(select_rows(&db, "SELECT 1 FROM t3 WHERE c0 = 42").len(), 1);
    assert_eq!(select_rows(&db, "SELECT 1 FROM t3 WHERE c0 = 2.5").len(), 1);
    assert_eq!(select_rows(&db, "SELECT 1 FROM t3 WHERE c0 IN (42, 2.5)").len(), 2);
    assert_eq!(select_rows(&db, "SELECT 1 FROM t3 WHERE c0 = 43").len(), 0);
}
