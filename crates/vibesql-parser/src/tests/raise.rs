//! Tests for parsing the SQLite `RAISE()` trigger-program expression
//! (#5409 added RAISE; #5416 made it a parse-time error outside a
//! trigger-program).
//!
//! SQLite accepts four forms inside a trigger body:
//! - `RAISE(ABORT, error-message)`
//! - `RAISE(FAIL, error-message)`
//! - `RAISE(ROLLBACK, error-message)`
//! - `RAISE(IGNORE)` (no message)
//!
//! SQLite only permits `RAISE()` *within a trigger-program* (a
//! `CREATE TRIGGER` body / WHEN condition) and rejects it at prepare/parse
//! time everywhere else with `RAISE() may only be used within a
//! trigger-program`. VibeSQL matches this: `RAISE()` parses inside a trigger
//! body but is a parse error in any other context. These tests therefore
//! exercise the four forms via [`Parser::parse_sql_in_trigger_body`] (the same
//! entry point the create-time validation and fire-time re-parse use) and
//! separately assert that `RAISE()` outside a trigger is rejected.

use vibesql_ast::{Expression, RaiseAction, SelectItem, Statement};

use crate::Parser;

/// Parse `SELECT <expr>` as a trigger-body statement and return the single
/// projected expression. RAISE() is only legal inside a trigger-program, so
/// the trigger-body entry point must be used.
fn parse_trigger_body_select_expr(sql: &str) -> Expression {
    let stmt = Parser::parse_sql_in_trigger_body(sql)
        .unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    let select = match stmt {
        Statement::Select(s) => s,
        other => panic!("Expected SELECT, got {:?}", other),
    };
    assert_eq!(select.select_list.len(), 1, "expected a single projection");
    match &select.select_list[0] {
        SelectItem::Expression { expr, .. } => expr.clone(),
        other => panic!("Expected a projected expression, got {:?}", other),
    }
}

#[test]
fn parses_raise_abort_with_message() {
    let expr = parse_trigger_body_select_expr("SELECT raise(ABORT, 'boom')");
    match expr {
        Expression::Raise { action, error_message } => {
            assert_eq!(action, RaiseAction::Abort);
            let msg = error_message.expect("ABORT requires a message");
            assert!(
                matches!(*msg, Expression::Literal(_)),
                "expected literal message, got {:?}",
                msg
            );
        }
        other => panic!("Expected Raise, got {:?}", other),
    }
}

#[test]
fn parses_raise_fail_with_message() {
    let expr = parse_trigger_body_select_expr("SELECT raise(FAIL, 'nope')");
    match expr {
        Expression::Raise { action, error_message } => {
            assert_eq!(action, RaiseAction::Fail);
            assert!(error_message.is_some());
        }
        other => panic!("Expected Raise, got {:?}", other),
    }
}

#[test]
fn parses_raise_rollback_with_message() {
    let expr = parse_trigger_body_select_expr("SELECT raise(ROLLBACK, 'undo all')");
    match expr {
        Expression::Raise { action, error_message } => {
            assert_eq!(action, RaiseAction::Rollback);
            assert!(error_message.is_some());
        }
        other => panic!("Expected Raise, got {:?}", other),
    }
}

#[test]
fn parses_raise_ignore_without_message() {
    let expr = parse_trigger_body_select_expr("SELECT raise(IGNORE)");
    match expr {
        Expression::Raise { action, error_message } => {
            assert_eq!(action, RaiseAction::Ignore);
            assert!(error_message.is_none(), "IGNORE takes no message");
        }
        other => panic!("Expected Raise, got {:?}", other),
    }
}

#[test]
fn raise_is_case_insensitive() {
    // The RAISE keyword and the action keyword are both case-insensitive.
    let expr = parse_trigger_body_select_expr("SELECT RaIsE(aBoRt, 'x')");
    assert!(matches!(
        expr,
        Expression::Raise { action: RaiseAction::Abort, error_message: Some(_) }
    ));
}

#[test]
fn raise_message_can_be_an_expression() {
    // SQLite allows any expression as the message, e.g. concatenation with a
    // pseudo-variable inside a trigger.
    let expr = parse_trigger_body_select_expr("SELECT raise(ABORT, 'bad: ' || NEW.v)");
    match expr {
        Expression::Raise { action, error_message } => {
            assert_eq!(action, RaiseAction::Abort);
            let msg = error_message.expect("message present");
            assert!(
                matches!(*msg, Expression::BinaryOp { .. }),
                "expected a binary (concat) message, got {:?}",
                msg
            );
        }
        other => panic!("Expected Raise, got {:?}", other),
    }
}

#[test]
fn raise_in_subquery_inside_trigger_body_parses() {
    // RAISE() nested in a subquery is still inside the trigger-program, so it
    // is admitted (sqlite3 accepts `SELECT (SELECT raise(ABORT,'sub'))` in a
    // trigger body).
    let stmt = Parser::parse_sql_in_trigger_body("SELECT (SELECT raise(ABORT, 'sub'))");
    assert!(stmt.is_ok(), "RAISE in a subquery inside a trigger body must parse: {:?}", stmt.err());
}

#[test]
fn raise_ignore_with_message_is_rejected() {
    // SQLite reports a `near ","` syntax error for RAISE(IGNORE, ...).
    let result = Parser::parse_sql_in_trigger_body("SELECT raise(IGNORE, 'msg')");
    assert!(result.is_err(), "RAISE(IGNORE, ...) must be a parse error");
}

#[test]
fn raise_abort_without_message_is_rejected() {
    // SQLite reports a `near ")"` syntax error for RAISE(ABORT).
    let result = Parser::parse_sql_in_trigger_body("SELECT raise(ABORT)");
    assert!(result.is_err(), "RAISE(ABORT) without a message must be a parse error");
}

#[test]
fn raise_with_unknown_action_is_rejected() {
    let result = Parser::parse_sql_in_trigger_body("SELECT raise(SOMETHING, 'x')");
    assert!(result.is_err(), "RAISE with a non-action keyword must be a parse error");
}

#[test]
fn raise_inside_trigger_body_parses() {
    // The whole point of #5409: a trigger whose body uses RAISE must parse
    // (previously failed with `near "ABORT": syntax error`). The body is
    // stored as RawSql, so just assert the CREATE TRIGGER parses.
    let sql = "CREATE TRIGGER t BEFORE UPDATE ON tbl WHEN NEW.v > 100 \
               BEGIN SELECT raise(ABORT, 'value too big'); END";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "trigger with RAISE body failed to parse: {:?}", result.err());
    assert!(matches!(result.unwrap(), Statement::CreateTrigger(_)));
}

#[test]
fn raise_in_when_condition_parses() {
    // The WHEN condition is part of the trigger-program, so SQLite permits
    // RAISE() there too (sqlite3 accepts this at CREATE TRIGGER time).
    let sql = "CREATE TRIGGER t BEFORE INSERT ON tbl WHEN raise(IGNORE) \
               BEGIN SELECT 1; END";
    let result = Parser::parse_sql(sql);
    assert!(result.is_ok(), "trigger with RAISE in WHEN failed to parse: {:?}", result.err());
}

// --- #5416: RAISE() outside a trigger-program is a parse-time error ---

/// SQLite's exact error (sans the shell's `in prepare,` prefix).
const RAISE_OUTSIDE_TRIGGER_MSG: &str = "RAISE() may only be used within a trigger-program";

#[test]
fn raise_outside_trigger_is_parse_error() {
    // sqlite3 3.51.x: `SELECT raise(ABORT, 'x')` ->
    // `in prepare, RAISE() may only be used within a trigger-program`.
    let err = Parser::parse_sql("SELECT raise(ABORT, 'x')")
        .expect_err("RAISE() outside a trigger must be a parse error");
    assert_eq!(err.message, RAISE_OUTSIDE_TRIGGER_MSG);
}

#[test]
fn raise_ignore_outside_trigger_is_parse_error() {
    let err = Parser::parse_sql("SELECT raise(IGNORE)")
        .expect_err("RAISE(IGNORE) outside a trigger must be a parse error");
    assert_eq!(err.message, RAISE_OUTSIDE_TRIGGER_MSG);
}

#[test]
fn raise_in_subquery_outside_trigger_is_parse_error() {
    // sqlite3 rejects RAISE in a subquery outside a trigger at prepare time.
    let err = Parser::parse_sql("SELECT (SELECT raise(ABORT, 'sub'))")
        .expect_err("RAISE() in a subquery outside a trigger must be a parse error");
    assert_eq!(err.message, RAISE_OUTSIDE_TRIGGER_MSG);
}

#[test]
fn raise_in_where_outside_trigger_is_parse_error() {
    let err = Parser::parse_sql("SELECT * FROM t WHERE raise(IGNORE)")
        .expect_err("RAISE() in a WHERE outside a trigger must be a parse error");
    assert_eq!(err.message, RAISE_OUTSIDE_TRIGGER_MSG);
}
