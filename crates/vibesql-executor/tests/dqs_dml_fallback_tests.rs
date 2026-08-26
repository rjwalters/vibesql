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

use vibesql_ast::{BinaryOperator, ColumnIdentifier, Expression};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{enforce_check_constraints, ExecutorError, ExpressionEvaluator};
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
