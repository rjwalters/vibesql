//! Structural predicate implication for partial-index planning (v1).
//!
//! Partial indexes (`CREATE INDEX ... WHERE predicate`) only contain rows for
//! which the predicate is TRUE, so the planner may only select one when the
//! query is guaranteed to touch nothing outside that subset. The v1 check
//! implemented here is **structural-equality implication**: the query's WHERE
//! clause implies the index predicate when every top-level AND conjunct of the
//! index predicate appears verbatim (by AST structural equality, `==`) among
//! the query's top-level AND conjuncts. [`ExpressionHasher`] hashes are used
//! only as a fast pre-filter; equality is always the decider, so the check is
//! sound even where the hasher is lossy (e.g. it ignores the `escape` field of
//! `LIKE`/`GLOB`) or in the face of u64 hash collisions.
//!
//! This intentionally does NOT attempt general implication (e.g. `x > 5`
//! implies `x > 0`). It covers the common SQLite idiom where the query repeats
//! the index predicate as a conjunct, e.g.:
//!
//! ```sql
//! CREATE INDEX idx_open_sku ON orders(sku) WHERE status = 1;
//! SELECT id FROM orders WHERE status = 1 AND sku = 300;
//! ```
//!
//! Partial EXPRESSION indexes (e.g. the date2-330/331 index
//! `CREATE INDEX t3b1 ON t3(datetime(b)) WHERE typeof(b)='real'`) are selected
//! under the same implication rule. They were temporarily excluded while the
//! temporal probe-bound bug existed; #5333 fixed the probes by coercing string
//! bounds to the stored temporal key type (see
//! `select::scan::index_scan::predicate::temporal_coercion`).
//!
//! Correctness:
//! - **Extra rows are harmless** — the full WHERE clause is always re-applied as a post-filter in
//!   `execute_index_scan()`.
//! - **Missing rows cannot happen** — implication guarantees every row that satisfies the query
//!   WHERE also satisfies the index predicate, and the index body is a superset of
//!   predicate-matching rows (build-time filtering excludes only predicate-false rows; DML
//!   maintenance for expression indexes is predicate-unaware and over-inclusive).

use vibesql_ast::Expression;
use vibesql_storage::Database;

use crate::evaluator::expression_hash::ExpressionHasher;

/// Collect the top-level AND conjuncts of an expression.
///
/// Handles BOTH conjunction forms used in this codebase: nested
/// `Expression::BinaryOp { op: And, .. }` chains and flat
/// `Expression::Conjunction(Vec<_>)`. Any other node (including OR /
/// `Disjunction`) is treated as a single opaque conjunct.
fn collect_conjuncts<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
    match expr {
        Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
            collect_conjuncts(left, out);
            collect_conjuncts(right, out);
        }
        Expression::Conjunction(exprs) => {
            for e in exprs {
                collect_conjuncts(e, out);
            }
        }
        other => out.push(other),
    }
}

/// True when every conjunct of `index_where` appears (by AST structural
/// equality) among the top-level AND conjuncts of `query_where`.
///
/// Structural hashes are used only as a fast pre-filter; `==` on the AST is
/// always the decider. This keeps the check sound even though
/// [`ExpressionHasher`] is lossy in places (it does not hash the `escape`
/// field of `LIKE`/`GLOB`, so `x LIKE p ESCAPE e` and `x LIKE p` hash equal
/// while being semantically different), and it rules out u64 hash collisions.
///
/// Conservative by construction: a top-level OR in the query WHERE is a
/// single opaque conjunct, so it only matches an index predicate that is the
/// structurally identical OR.
pub(crate) fn query_implies_index_predicate(
    query_where: &Expression,
    index_where: &Expression,
) -> bool {
    let mut query_conjuncts = Vec::new();
    collect_conjuncts(query_where, &mut query_conjuncts);
    let query_hashes: Vec<u64> =
        query_conjuncts.iter().map(|e| ExpressionHasher::hash(e)).collect();

    let mut index_conjuncts = Vec::new();
    collect_conjuncts(index_where, &mut index_conjuncts);

    index_conjuncts.iter().all(|conjunct| {
        let conjunct_hash = ExpressionHasher::hash(conjunct);
        query_conjuncts
            .iter()
            .zip(query_hashes.iter())
            .any(|(q, q_hash)| *q_hash == conjunct_hash && *q == *conjunct)
    })
}

/// Whether an index may be selected by the planner given the query's WHERE
/// clause, considering partial-index predicates.
///
/// - Non-partial indexes are always usable (returns `true`).
/// - Partial indexes (expression or not) are usable only when `query_where` structurally implies
///   the index predicate (see [`query_implies_index_predicate`]).
/// - A partial index with no query WHERE clause is never usable.
///
/// Partial EXPRESSION indexes were excluded here (v1) while the temporal
/// probe-bound bug existed: probes compared `Timestamp` keys against raw
/// string bounds with type-tag ordering and silently lost rows. Issue #5333
/// fixed the probes by coercing string bounds to the stored temporal key type
/// at probe time, so the exclusion is no longer needed.
///
/// The partial predicate lives on the catalog-side `IndexMetadata`
/// (`where_clause`); the storage-side metadata does not yet carry it.
pub(crate) fn partial_index_usable(
    database: &Database,
    index_name: &str,
    query_where: Option<&Expression>,
) -> bool {
    let Some(metadata) = database.catalog.find_index_by_name(index_name) else {
        // Unknown to the catalog: no predicate gate.
        return true;
    };

    let Some(index_where) = metadata.where_clause.as_deref() else {
        // Not a partial index: no predicate gate.
        return true;
    };

    match query_where {
        Some(query_where) => query_implies_index_predicate(query_where, index_where),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::Statement;

    use super::*;

    /// Parse the WHERE clause of `SELECT 1 FROM t WHERE <expr>`.
    fn parse_where(expr_sql: &str) -> Expression {
        let sql = format!("SELECT 1 FROM t WHERE {}", expr_sql);
        let stmt = vibesql_parser::Parser::parse_sql(&sql).expect("parse failed");
        match stmt {
            Statement::Select(select) => select.where_clause.expect("expected WHERE clause"),
            other => panic!("expected SELECT, got {:?}", other),
        }
    }

    #[test]
    fn exact_conjunct_match_implies() {
        let query =
            parse_where("typeof(b)='real' AND datetime(b) BETWEEN '2017-07-04' AND '2017-07-08'");
        let index = parse_where("typeof(b)='real'");
        assert!(query_implies_index_predicate(&query, &index));
    }

    #[test]
    fn identical_predicate_implies_itself() {
        let query = parse_where("typeof(b)='real'");
        let index = parse_where("typeof(b)='real'");
        assert!(query_implies_index_predicate(&query, &index));
    }

    #[test]
    fn hash_equality_for_predicate_parsed_twice() {
        // Gotcha check: the catalog-stored predicate (parsed at CREATE INDEX
        // time) and the query conjunct (parsed at query time) must normalize
        // to the same structural hash.
        let a = parse_where("typeof(b)='real'");
        let b = parse_where("typeof(b)='real'");
        assert_eq!(ExpressionHasher::hash(&a), ExpressionHasher::hash(&b));
    }

    #[test]
    fn like_escape_does_not_imply_escapeless_like() {
        // Regression (PR #5331 review): `name LIKE 'x!%y' ESCAPE '!'` matches
        // only the literal 'x%y', while `name LIKE 'x!%y'` matches 'x!…y'.
        // ExpressionHasher does not hash the `escape` field, so these two
        // expressions hash EQUAL — the implication check must still reject
        // them via structural equality, in both directions.
        let with_escape = parse_where("name LIKE 'x!%y' ESCAPE '!'");
        let without_escape = parse_where("name LIKE 'x!%y'");
        assert!(!query_implies_index_predicate(&with_escape, &without_escape));
        assert!(!query_implies_index_predicate(&without_escape, &with_escape));

        // Identical LIKE ... ESCAPE on both sides still implies.
        let with_escape_2 = parse_where("name LIKE 'x!%y' ESCAPE '!'");
        assert!(query_implies_index_predicate(&with_escape, &with_escape_2));
    }

    #[test]
    fn hash_collision_does_not_imply() {
        // Guard for the hash-collision shape: two semantically different
        // expressions whose ExpressionHasher hashes collide (here, the lossy
        // `escape` arm) must NOT imply each other. If the hasher is ever
        // fixed to include `escape`, the assert_eq below will fail and this
        // test can be downgraded to a plain non-implication check.
        let with_escape = parse_where("name LIKE 'x!%y' ESCAPE '!'");
        let without_escape = parse_where("name LIKE 'x!%y'");
        assert_eq!(
            ExpressionHasher::hash(&with_escape),
            ExpressionHasher::hash(&without_escape),
            "expected a hash collision (lossy escape arm); update this test if the hasher changed"
        );
        assert!(!query_implies_index_predicate(&with_escape, &without_escape));

        let query = parse_where("name LIKE 'x!%y' AND name = 'x!zzy'");
        let index = parse_where("name LIKE 'x!%y' ESCAPE '!'");
        assert!(!query_implies_index_predicate(&query, &index));
    }

    #[test]
    fn missing_conjunct_does_not_imply() {
        let query = parse_where("datetime(b) BETWEEN '2017-07-04' AND '2017-07-08'");
        let index = parse_where("typeof(b)='real'");
        assert!(!query_implies_index_predicate(&query, &index));
    }

    #[test]
    fn different_literal_does_not_imply() {
        let query = parse_where("typeof(b)='text' AND a = 1");
        let index = parse_where("typeof(b)='real'");
        assert!(!query_implies_index_predicate(&query, &index));
    }

    #[test]
    fn top_level_or_does_not_imply() {
        // OR is one opaque conjunct: satisfying the query does not require
        // satisfying the index predicate.
        let query = parse_where("typeof(b)='real' OR a = 1");
        let index = parse_where("typeof(b)='real'");
        assert!(!query_implies_index_predicate(&query, &index));
    }

    #[test]
    fn structurally_identical_or_implies() {
        let query = parse_where("(a = 1 OR a = 2) AND b > 0");
        let index = parse_where("a = 1 OR a = 2");
        assert!(query_implies_index_predicate(&query, &index));
    }

    #[test]
    fn index_predicate_with_and_requires_all_conjuncts() {
        let query = parse_where("a = 1 AND b > 0 AND c < 5");
        let index_ok = parse_where("a = 1 AND c < 5");
        let index_missing = parse_where("a = 1 AND d = 9");
        assert!(query_implies_index_predicate(&query, &index_ok));
        assert!(!query_implies_index_predicate(&query, &index_missing));
    }

    #[test]
    fn nested_binary_and_conjuncts_are_split() {
        // Build a nested BinaryOp::And chain explicitly to cover the
        // non-Conjunction representation.
        let a = parse_where("a = 1");
        let b = parse_where("b > 0");
        let c = parse_where("c < 5");
        let nested = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(a),
                op: vibesql_ast::BinaryOperator::And,
                right: Box::new(b),
            }),
            op: vibesql_ast::BinaryOperator::And,
            right: Box::new(c),
        };
        let index = parse_where("b > 0");
        assert!(query_implies_index_predicate(&nested, &index));
    }

    #[test]
    fn flat_conjunction_conjuncts_are_split() {
        let flat = Expression::Conjunction(vec![
            parse_where("a = 1"),
            parse_where("b > 0"),
            parse_where("c < 5"),
        ]);
        let index = parse_where("c < 5");
        assert!(query_implies_index_predicate(&flat, &index));
        let index_missing = parse_where("d = 2");
        assert!(!query_implies_index_predicate(&flat, &index_missing));
    }

    #[test]
    fn partial_index_usable_requires_query_where() {
        use vibesql_storage::Database;

        let mut db = Database::new();
        let create_table =
            vibesql_parser::Parser::parse_sql("CREATE TABLE t (a INTEGER, b INTEGER)").unwrap();
        if let Statement::CreateTable(stmt) = create_table {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        let create_index =
            vibesql_parser::Parser::parse_sql("CREATE INDEX tb1 ON t(a) WHERE b = 1").unwrap();
        if let Statement::CreateIndex(stmt) = create_index {
            crate::CreateIndexExecutor::execute(&stmt, &mut db).unwrap();
        }

        // No WHERE clause: partial index unusable.
        assert!(!partial_index_usable(&db, "tb1", None));

        // Implying WHERE clause: usable.
        let implying = parse_where("b = 1 AND a > 5");
        assert!(partial_index_usable(&db, "tb1", Some(&implying)));

        // Non-implying WHERE clause: unusable.
        let non_implying = parse_where("a > 5");
        assert!(!partial_index_usable(&db, "tb1", Some(&non_implying)));
    }

    #[test]
    fn partial_expression_index_usable_when_implied() {
        // #5333 fixed the temporal probe-bound bug, so partial EXPRESSION
        // indexes follow the same implication rule as non-expression ones
        // (date2-330/331 shape).
        use vibesql_storage::Database;

        let mut db = Database::new();
        let create_table =
            vibesql_parser::Parser::parse_sql("CREATE TABLE t3 (a INTEGER, b REAL)").unwrap();
        if let Statement::CreateTable(stmt) = create_table {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        let create_index = vibesql_parser::Parser::parse_sql(
            "CREATE INDEX t3b1 ON t3(datetime(b)) WHERE typeof(b)='real'",
        )
        .unwrap();
        if let Statement::CreateIndex(stmt) = create_index {
            crate::CreateIndexExecutor::execute(&stmt, &mut db).unwrap();
        }

        // Verbatim-implying WHERE clause makes it usable.
        let implying =
            parse_where("typeof(b)='real' AND datetime(b) BETWEEN '2017-07-04' AND '2017-07-08'");
        assert!(partial_index_usable(&db, "t3b1", Some(&implying)));

        // Non-implying / absent WHERE clause: still unusable.
        let non_implying = parse_where("datetime(b) > '2017-07-04'");
        assert!(!partial_index_usable(&db, "t3b1", Some(&non_implying)));
        assert!(!partial_index_usable(&db, "t3b1", None));
    }

    #[test]
    fn partial_index_usable_true_for_non_partial_index() {
        use vibesql_storage::Database;

        let mut db = Database::new();
        let create_table =
            vibesql_parser::Parser::parse_sql("CREATE TABLE t (a INTEGER, b INTEGER)").unwrap();
        if let Statement::CreateTable(stmt) = create_table {
            crate::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }
        let create_index = vibesql_parser::Parser::parse_sql("CREATE INDEX ta ON t(a)").unwrap();
        if let Statement::CreateIndex(stmt) = create_index {
            crate::CreateIndexExecutor::execute(&stmt, &mut db).unwrap();
        }

        assert!(partial_index_usable(&db, "ta", None));
        let any_where = parse_where("b = 1");
        assert!(partial_index_usable(&db, "ta", Some(&any_where)));
    }
}
