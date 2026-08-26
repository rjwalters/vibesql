//! Tests for the `SQLITE_DBCONFIG_DQS_DML` runtime column-resolution
//! fallback (#6561, part of #6558).
//!
//! Real SQLite: when an *unqualified, originally double-quoted* identifier
//! fails ordinary column resolution during expression evaluation, it is
//! reinterpreted as a text literal named by the identifier instead of
//! raising "no such column". Two independent SQLite behaviors both produce
//! this fallback, and this file exercises both:
//!
//! 1. **Session opt-in** (`SQLITE_DBCONFIG_DQS_DML`, connection default OFF): governs ordinary,
//!    freshly-typed DML text (SELECT/INSERT/ UPDATE/DELETE) evaluated under the live connection's
//!    current flags. Modeled here as `ExpressionEvaluator::with_dqs_dml_fallback`, backed by
//!    `Database::dqs_dml()`/`set_dqs_dml()`.
//!
//! 2. **Schema-loading leniency** (CHECK constraints specifically): a CHECK constraint's source
//!    text is stored verbatim in the schema and re-parsed by SQLite every time the schema loads for
//!    a connection — and SQLite's schema-loading parser unconditionally tolerates the legacy
//!    double-quoted-string-literal fallback, *independent of the current connection's DQS_DML
//!    setting* ("SQLite can load such a schema from disk", quote.test's own comment). This is why
//!    quote.test 2.3.1/2.3.2 still pass even though the CHECK constraint is evaluated on a freshly
//!    reopened connection that never re-applies `sqlite3_db_config db SQLITE_DBCONFIG_DML 1` —
//!    verified empirically against the real quote.test script. Modeled here as the fallback always
//!    applying when `SchemaExprContext::CheckConstraint` is active, regardless of the session's
//!    `dqs_dml` flag.
//!
//! Both gates share the same *quoted, unqualified-only* restriction: a bare
//! unquoted identifier, or a qualified reference (`t1."x"`), never gets the
//! fallback under either gate.

use vibesql_ast::{BinaryOperator, ColumnIdentifier, Expression, Statement};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{
    enforce_check_constraints, CreateTableExecutor, ExecutorError, ExpressionEvaluator,
    InsertExecutor, SelectExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Builds a table `xyz(a, b, c)` with the CHECK constraint
/// `c != "null"` where `"null"` is an *originally-quoted*, unqualified
/// `ColumnRef` — exactly the AST a schema loaded via
/// `PRAGMA writable_schema=ON; CREATE TABLE xyz(a, b, c CHECK (c!="null"))`
/// produces (the parser never turns a double-quoted identifier into a
/// string literal; only evaluation-time handling can reinterpret it).
fn xyz_schema_with_quoted_null_check() -> TableSchema {
    TableSchema::with_all_constraint_types(
        "xyz".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("b".to_string(), DataType::Integer, true),
            ColumnSchema::new("c".to_string(), DataType::Integer, true),
        ],
        None,
        Vec::new(),
        vec![(
            "c!=\"null\"".to_string(),
            Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("c", false))),
                op: BinaryOperator::NotEqual,
                right: Box::new(Expression::ColumnRef(ColumnIdentifier::quoted("null"))),
            },
        )],
        Vec::new(),
    )
}

// ============================================================================
// Gate 1: session-level `dqs_dml` flag, plain (non-schema) expression context
// ============================================================================

/// Flag OFF (SQLite's own connection default), plain expression context (not
/// a CHECK constraint): a quoted, unresolved identifier must still raise
/// `ColumnNotFound` — no regression to the pre-existing "no such column"
/// behavior for ordinary DML evaluated with DQS_DML at its default.
#[test]
fn dqs_dml_fallback_off_in_plain_context_still_errors_on_unresolved_quoted_identifier() {
    let schema = TableSchema::new(
        "t".to_string(),
        vec![ColumnSchema::new("c".to_string(), DataType::Integer, true)],
    );
    let evaluator = ExpressionEvaluator::new(&schema); // dqs_dml_fallback defaults to false
    let row = Row::new(vec![SqlValue::Integer(3)]);
    let expr = Expression::ColumnRef(ColumnIdentifier::quoted("null"));

    let err = evaluator.eval(&expr, &row).unwrap_err();
    assert!(
        matches!(err, ExecutorError::ColumnNotFound { ref column_name, .. } if column_name == "null"),
        "expected ColumnNotFound for the unresolved quoted identifier \"null\" with the flag off, got: {err:?}"
    );
}

/// Flag ON, plain expression context: a quoted, unresolved identifier
/// evaluates to the text literal named by the identifier.
#[test]
fn dqs_dml_fallback_on_in_plain_context_treats_quoted_unresolved_identifier_as_literal() {
    let schema = TableSchema::new(
        "t".to_string(),
        vec![ColumnSchema::new("c".to_string(), DataType::Integer, true)],
    );
    let evaluator = ExpressionEvaluator::new(&schema).with_dqs_dml_fallback(true);
    let row = Row::new(vec![SqlValue::Integer(3)]);
    let expr = Expression::ColumnRef(ColumnIdentifier::quoted("null"));

    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("null")));
}

// ============================================================================
// Gate 2: CheckConstraint schema context, independent of the session flag
// ============================================================================

/// quote.test 2.3.1: `INSERT INTO xyz VALUES(1, 2, 3)` must succeed even
/// with the session's DQS_DML flag OFF (default) — the CHECK constraint's
/// schema-loading leniency applies unconditionally, exactly matching real
/// SQLite evaluating this CHECK on a freshly reopened connection that never
/// re-applied `sqlite3_db_config db SQLITE_DBCONFIG_DQS_DML 1`.
#[test]
fn check_constraint_context_applies_fallback_even_with_session_dqs_dml_off() {
    let db = Database::new();
    assert!(!db.dqs_dml(), "DQS_DML must default to OFF, matching SQLite's own connection default");

    let schema = xyz_schema_with_quoted_null_check();
    let row = vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(3)];

    enforce_check_constraints(&db, &schema, &row).expect(
        "c=3 != 'null' (fallback literal) so the CHECK constraint should be satisfied, \
         even with the session's dqs_dml flag off — CHECK-constraint schema context \
         applies the fallback unconditionally",
    );
}

/// quote.test 2.3.2: `INSERT INTO xyz VALUES(1, 2, 'null')` must fail with a
/// CHECK constraint violation (not a column-resolution error), again with
/// the session's DQS_DML flag at its OFF default.
#[test]
fn check_constraint_context_check_violation_with_session_dqs_dml_off() {
    let db = Database::new();
    assert!(!db.dqs_dml());

    let schema = xyz_schema_with_quoted_null_check();
    let row = vec![
        SqlValue::Integer(1),
        SqlValue::Integer(2),
        SqlValue::Varchar(arcstr::ArcStr::from("null")),
    ];

    let err = enforce_check_constraints(&db, &schema, &row).unwrap_err();
    match err {
        ExecutorError::SqliteCompatError(msg) => {
            assert!(
                msg.starts_with("CHECK constraint failed"),
                "expected a CHECK constraint failure, got: {msg}"
            );
        }
        other => {
            panic!("expected SqliteCompatError(\"CHECK constraint failed: ...\"), got: {other:?}")
        }
    }
}

/// Same as the two tests above, but with the session's DQS_DML flag
/// explicitly ON — must behave identically (CheckConstraint context makes
/// the flag's exact value irrelevant here).
#[test]
fn check_constraint_context_applies_fallback_with_session_dqs_dml_on_too() {
    let mut db = Database::new();
    db.set_dqs_dml(true);

    let schema = xyz_schema_with_quoted_null_check();
    let row = vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(3)];

    enforce_check_constraints(&db, &schema, &row).expect("c=3 != 'null' should satisfy the CHECK");
}

// ============================================================================
// Negative cases: the quoted+unqualified restriction is independent of
// (and not weakened by) the CheckConstraint context always-on gate.
// ============================================================================

/// A bare, unquoted, unresolved identifier inside a CHECK constraint must
/// still raise `ColumnNotFound` — the CheckConstraint context's
/// always-applies fallback is scoped to *quoted* identifiers only, exactly
/// like the session-flag gate.
#[test]
fn dqs_dml_fallback_does_not_apply_to_unquoted_unresolved_identifier_in_check_constraint() {
    let db = Database::new();

    let schema = TableSchema::with_all_constraint_types(
        "xyz".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("b".to_string(), DataType::Integer, true),
            ColumnSchema::new("c".to_string(), DataType::Integer, true),
        ],
        None,
        Vec::new(),
        vec![(
            "c!=bareword".to_string(),
            Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("c", false))),
                op: BinaryOperator::NotEqual,
                // Unquoted, unresolved identifier -- must still error even
                // inside a CHECK constraint.
                right: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("bareword", false))),
            },
        )],
        Vec::new(),
    );
    let row = vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(3)];

    let err = enforce_check_constraints(&db, &schema, &row).unwrap_err();
    assert!(
        matches!(err, ExecutorError::ColumnNotFound { ref column_name, .. } if column_name == "bareword"),
        "unquoted unresolved identifiers must still raise ColumnNotFound inside a CHECK constraint, got: {err:?}"
    );
}

/// A *qualified* quoted identifier that fails resolution (e.g. `t1."x"`
/// where `t1` isn't a known table/alias) must NOT get the fallback either,
/// inside a CHECK constraint or otherwise — SQLite's DQS_DML fallback only
/// applies to unqualified references.
#[test]
fn dqs_dml_fallback_does_not_apply_to_qualified_unresolved_identifier_in_check_constraint() {
    let db = Database::new();

    let schema = TableSchema::with_all_constraint_types(
        "xyz".to_string(),
        vec![ColumnSchema::new("c".to_string(), DataType::Integer, true)],
        None,
        Vec::new(),
        vec![(
            "qualified_check".to_string(),
            Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("c", false))),
                op: BinaryOperator::NotEqual,
                // Qualified + quoted, but "other_table" is not a known table
                // or alias for this schema -- resolution fails before the
                // unqualified-only fallback branch is ever reached.
                right: Box::new(Expression::ColumnRef(ColumnIdentifier::qualified(
                    "other_table",
                    false,
                    "null",
                    true,
                ))),
            },
        )],
        Vec::new(),
    );
    let row = vec![SqlValue::Integer(3)];

    let err = enforce_check_constraints(&db, &schema, &row).unwrap_err();
    assert!(
        matches!(err, ExecutorError::InvalidTableQualifier { .. }),
        "a qualified reference to an unknown table must still error even inside a CHECK constraint, got: {err:?}"
    );
}

// ============================================================================
// #6584 follow-up: the session `dqs_dml` flag's ACTUAL current reach
// ============================================================================
//
// PR #6582 wired `.with_dqs_dml_fallback(database.dqs_dml())` only into
// CHECK-constraint validator call sites, every one of which already sets
// `SchemaExprContext::CheckConstraint` -- which by itself satisfies gate 2
// above regardless of the session flag. No ordinary SELECT / plain
// expression-evaluation path constructs an `ExpressionEvaluator` with the
// session flag threaded through, so `PRAGMA dqs_dml` has no observable
// effect on live SQL today. This section locks in that documented scope
// (see `Database::dqs_dml()`'s doc comment) and separately fixes the one
// pre-existing CHECK-constraint-context propagation gap noted in review:
// `check_would_violate_constraints` (the `INSERT ... OR IGNORE` / untargeted
// `DO NOTHING` batch-dedup path in `insert/execution.rs`) previously built
// its CHECK-constraint evaluator without `SchemaExprContext::CheckConstraint`.

fn parse_one(sql: &str) -> Statement {
    Parser::parse_sql(sql).unwrap_or_else(|e| panic!("failed to parse `{sql}`: {e:?}"))
}

fn create_table(db: &mut Database, sql: &str) {
    let Statement::CreateTable(stmt) = parse_one(sql) else {
        panic!("expected CREATE TABLE: {sql}");
    };
    CreateTableExecutor::execute(&stmt, db)
        .unwrap_or_else(|e| panic!("CREATE TABLE failed: {e:?}"));
}

fn insert(db: &mut Database, sql: &str) -> Result<usize, ExecutorError> {
    let Statement::Insert(stmt) = parse_one(sql) else {
        panic!("expected INSERT: {sql}");
    };
    InsertExecutor::execute(db, &stmt)
}

/// Even with the session's `dqs_dml` flag explicitly turned ON, an
/// unresolved quoted identifier in a plain SELECT still raises
/// `ColumnNotFound` -- identical to the flag's OFF default. This is the
/// exact repro from #6584: `PRAGMA dqs_dml = 1; SELECT "nope" FROM t;` still
/// errors, because no plain-SELECT evaluator construction site threads the
/// session flag through (only CHECK-constraint validators do, and those are
/// already covered unconditionally by gate 2 above).
#[test]
fn dqs_dml_session_flag_has_no_effect_on_plain_select_column_resolution() {
    let mut db = Database::new();
    db.set_dqs_dml(true);
    assert!(db.dqs_dml());

    create_table(&mut db, "CREATE TABLE t (c INTEGER)");
    insert(&mut db, "INSERT INTO t VALUES (1)").expect("insert should succeed");

    let Statement::Select(select_stmt) = parse_one("SELECT \"nope\" FROM t") else {
        panic!("expected SELECT");
    };
    let err = SelectExecutor::new(&db)
        .execute(&select_stmt)
        .expect_err("unresolved quoted identifier must still error in a plain SELECT");
    assert!(
        matches!(err, ExecutorError::ColumnNotFound { ref column_name, .. } if column_name == "nope"),
        "expected ColumnNotFound even with the session dqs_dml flag ON -- the flag currently only \
         reaches CHECK-constraint evaluation, not plain SELECT column resolution (#6584); got: {err:?}"
    );
}

/// Builds a table `xyz(a, c)` with the CHECK constraint `c != "null"` where
/// `"null"` is an *originally-quoted*, unqualified `ColumnRef` -- the same
/// AST shape [`xyz_schema_with_quoted_null_check`] uses above. Constructed
/// directly via [`Database::create_table`] (bypassing SQL `CREATE TABLE`
/// text) because CREATE TABLE's own DDL-time CHECK-column validation
/// (`constraint_validator.rs`) unconditionally rejects an unresolvable
/// quoted column reference with "no such column ... should this be a string
/// literal in single-quotes?" -- exactly modeling a schema that (like
/// quote.test's) was already loaded with this quirky CHECK stored, rather
/// than one freshly created through today's stricter DDL path.
fn xyz_or_ignore_schema() -> TableSchema {
    TableSchema::with_all_constraint_types(
        "xyz".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("c".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        None,
        Vec::new(),
        vec![(
            "c!=\"null\"".to_string(),
            Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef(ColumnIdentifier::simple("c", false))),
                op: BinaryOperator::NotEqual,
                right: Box::new(Expression::ColumnRef(ColumnIdentifier::quoted("null"))),
            },
        )],
        Vec::new(),
    )
}

/// TDD (#6584 "Also noted"): before the fix, `check_would_violate_constraints`
/// evaluated CHECK constraints without `SchemaExprContext::CheckConstraint`.
/// With the session's `dqs_dml` flag at its OFF default, that evaluator
/// cannot resolve the quoted `"null"` reference, so the pre-check silently
/// treats the row as "not violating" (a resolution error isn't the same as
/// an explicit `false`) and the row falls through to full validation via
/// `RowValidator`, which DOES apply the fallback correctly (it already sets
/// `SchemaExprContext::CheckConstraint`) and returns a genuine CHECK
/// violation error. Because this is `OR IGNORE` (not `OR FAIL`), that late
/// error propagates as a hard statement failure instead of the row being
/// silently skipped -- the SQLite-incompatible bug this test pins down.
#[test]
fn insert_or_ignore_silently_skips_row_violating_quoted_identifier_check_with_dqs_dml_off() {
    let mut db = Database::new();
    assert!(!db.dqs_dml(), "dqs_dml must default to OFF");

    db.create_table(xyz_or_ignore_schema()).expect("create_table should succeed");

    // Row 1 trivially satisfies the CHECK (c='ok' != the fallback literal 'null').
    insert(&mut db, "INSERT OR IGNORE INTO xyz (a, c) VALUES (1, 'ok')")
        .expect("row 1 should insert normally");

    // Row 2's c='null' literally violates `c != "null"` under the fallback --
    // OR IGNORE must silently skip it rather than raising a hard error.
    let affected = insert(&mut db, "INSERT OR IGNORE INTO xyz (a, c) VALUES (2, 'null')")
        .unwrap_or_else(|e| {
            panic!("INSERT OR IGNORE must silently skip a CHECK-violating row, not error: {e:?}")
        });
    assert_eq!(affected, 0, "the CHECK-violating row must be skipped, not inserted");

    let row_count = db.get_table("xyz").unwrap().scan_live().count();
    assert_eq!(row_count, 1, "only row 1 should be present after OR IGNORE skips row 2");
}
