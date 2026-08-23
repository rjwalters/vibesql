//! Tests for ANALYZE statement parsing, including schema-qualified table
//! names (issue #6504).
//!
//! Before the fix, `parse_analyze_statement` parsed the table name with the
//! single-token `parse_identifier()`. Unlike ALTER TABLE (which failed to
//! parse at all on a qualified name), ANALYZE "succeeded" while silently
//! dropping the qualified remainder: `ANALYZE aux.sqlite_master` parsed as
//! `AnalyzeStmt { table_name: Some("aux"), .. }`, discarding `.sqlite_master`
//! entirely with no trailing-garbage rejection. Fixed by switching to
//! `parse_table_ref()` (already used by DROP TABLE) plus an explicit
//! `expect_statement_end()` check.

use vibesql_ast::Statement;

use crate::Parser;

#[test]
fn test_analyze_no_table() {
    let result = Parser::parse_sql("ANALYZE");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Analyze(stmt) => {
            assert!(stmt.table_name.is_none());
            assert!(stmt.columns.is_none());
        }
        other => panic!("Expected Analyze, got: {other:?}"),
    }
}

#[test]
fn test_analyze_unqualified_table() {
    let result = Parser::parse_sql("ANALYZE t1");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Analyze(stmt) => {
            assert_eq!(stmt.table_name.as_deref(), Some("t1"));
        }
        other => panic!("Expected Analyze, got: {other:?}"),
    }
}

#[test]
fn test_analyze_main_qualified_table() {
    // Regression: the `main.`-qualified form must keep working.
    let result = Parser::parse_sql("ANALYZE main.t1");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Analyze(stmt) => {
            assert_eq!(stmt.table_name.as_deref(), Some("main.t1"));
        }
        other => panic!("Expected Analyze, got: {other:?}"),
    }
}

#[test]
fn test_analyze_attached_schema_qualified_table() {
    // Core fix: a schema-qualified table name must parse as one qualified
    // name, not silently truncate to just the schema alias.
    let result = Parser::parse_sql("ANALYZE aux.t1");
    assert!(result.is_ok(), "schema-qualified table name should parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Analyze(stmt) => {
            assert_eq!(stmt.table_name.as_deref(), Some("aux.t1"));
        }
        other => panic!("Expected Analyze, got: {other:?}"),
    }
}

#[test]
fn test_analyze_attached_schema_qualified_sqlite_master() {
    // The specific case called out in issue #6504 / #6451: this must parse
    // as the full qualified name (reaching the executor-side reserved-name
    // guard), not silently truncate to `ANALYZE aux`.
    let result = Parser::parse_sql("ANALYZE aux.sqlite_master");
    assert!(result.is_ok(), "schema-qualified table name should parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Analyze(stmt) => {
            assert_eq!(stmt.table_name.as_deref(), Some("aux.sqlite_master"));
        }
        other => panic!("Expected Analyze, got: {other:?}"),
    }
}

#[test]
fn test_analyze_quoted_schema_qualified_table() {
    let result = Parser::parse_sql(r#"ANALYZE "aux"."t1""#);
    assert!(result.is_ok(), "quoted schema-qualified table name should parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Analyze(stmt) => {
            assert_eq!(stmt.table_name.as_deref(), Some("aux.t1"));
        }
        other => panic!("Expected Analyze, got: {other:?}"),
    }
}

#[test]
fn test_analyze_schema_qualified_table_with_column_list() {
    let result = Parser::parse_sql("ANALYZE aux.t1(a, b)");
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());
    match result.unwrap() {
        Statement::Analyze(stmt) => {
            assert_eq!(stmt.table_name.as_deref(), Some("aux.t1"));
            assert_eq!(stmt.columns, Some(vec!["a".to_string(), "b".to_string()]));
        }
        other => panic!("Expected Analyze, got: {other:?}"),
    }
}

#[test]
fn test_analyze_rejects_trailing_garbage() {
    // Before the fix this silently parsed as `ANALYZE aux` (table_name:
    // Some("aux")), dropping `.sqlite_master` with no error at all. Now the
    // qualified name parses as a unit and any leftover token after it is
    // rejected via `expect_statement_end()`, matching the pattern already
    // applied to UPDATE/DELETE/INSERT (#5261).
    let result = Parser::parse_sql("ANALYZE aux.t1 extra_garbage");
    assert!(result.is_err(), "trailing garbage after table name must be rejected: {result:?}");
}

#[test]
fn test_analyze_rejects_trailing_garbage_after_column_list() {
    let result = Parser::parse_sql("ANALYZE aux.t1(a, b) extra_garbage");
    assert!(result.is_err(), "trailing garbage after column list must be rejected: {result:?}");
}
