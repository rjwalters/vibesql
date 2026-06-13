//! Tests for parsing the SQLite `RAISE()` trigger-program expression (#5409).
//!
//! SQLite accepts four forms inside a trigger body:
//! - `RAISE(ABORT, error-message)`
//! - `RAISE(FAIL, error-message)`
//! - `RAISE(ROLLBACK, error-message)`
//! - `RAISE(IGNORE)` (no message)
//!
//! VibeSQL parses `RAISE()` as a general expression (the trigger-context gate
//! is enforced at evaluation time), so we can exercise it through a standalone
//! `SELECT raise(...)`.

use vibesql_ast::{Expression, RaiseAction, SelectItem, Statement};

use crate::Parser;

/// Parse `SELECT <expr>` and return the single projected expression.
fn parse_select_expr(sql: &str) -> Expression {
    let stmt = Parser::parse_sql(sql)
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
    let expr = parse_select_expr("SELECT raise(ABORT, 'boom')");
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
    let expr = parse_select_expr("SELECT raise(FAIL, 'nope')");
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
    let expr = parse_select_expr("SELECT raise(ROLLBACK, 'undo all')");
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
    let expr = parse_select_expr("SELECT raise(IGNORE)");
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
    let expr = parse_select_expr("SELECT RaIsE(aBoRt, 'x')");
    assert!(matches!(
        expr,
        Expression::Raise { action: RaiseAction::Abort, error_message: Some(_) }
    ));
}

#[test]
fn raise_message_can_be_an_expression() {
    // SQLite allows any expression as the message, e.g. concatenation with a
    // pseudo-variable inside a trigger.
    let expr = parse_select_expr("SELECT raise(ABORT, 'bad: ' || NEW.v)");
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
fn raise_ignore_with_message_is_rejected() {
    // SQLite reports a `near ","` syntax error for RAISE(IGNORE, ...).
    let result = Parser::parse_sql("SELECT raise(IGNORE, 'msg')");
    assert!(result.is_err(), "RAISE(IGNORE, ...) must be a parse error");
}

#[test]
fn raise_abort_without_message_is_rejected() {
    // SQLite reports a `near ")"` syntax error for RAISE(ABORT).
    let result = Parser::parse_sql("SELECT raise(ABORT)");
    assert!(result.is_err(), "RAISE(ABORT) without a message must be a parse error");
}

#[test]
fn raise_with_unknown_action_is_rejected() {
    let result = Parser::parse_sql("SELECT raise(SOMETHING, 'x')");
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
