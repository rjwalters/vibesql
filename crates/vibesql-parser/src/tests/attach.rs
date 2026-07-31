//! Tests for ATTACH DATABASE / DETACH DATABASE statement parsing (#6310)

use vibesql_ast::Statement;

use crate::Parser;

fn parse_attach(sql: &str) -> vibesql_ast::AttachStmt {
    match Parser::parse_sql(sql) {
        Ok(Statement::Attach(stmt)) => stmt,
        other => panic!("Expected Attach for {sql:?}, got: {other:?}"),
    }
}

fn parse_detach(sql: &str) -> vibesql_ast::DetachStmt {
    match Parser::parse_sql(sql) {
        Ok(Statement::Detach(stmt)) => stmt,
        other => panic!("Expected Detach for {sql:?}, got: {other:?}"),
    }
}

#[test]
fn test_attach_memory() {
    let stmt = parse_attach("ATTACH ':memory:' AS aux");
    assert_eq!(stmt.filename, ":memory:");
    assert_eq!(stmt.schema_name, "aux");
}

#[test]
fn test_attach_with_database_noise_word() {
    let stmt = parse_attach("ATTACH DATABASE 'test.db2' AS aux;");
    assert_eq!(stmt.filename, "test.db2");
    assert_eq!(stmt.schema_name, "aux");
}

#[test]
fn test_attach_case_insensitive_leading_word() {
    let stmt = parse_attach("attach database 'x' as db2");
    assert_eq!(stmt.filename, "x");
    assert_eq!(stmt.schema_name, "db2");

    let stmt = parse_attach("AtTaCh 'y' AS db3;");
    assert_eq!(stmt.filename, "y");
    assert_eq!(stmt.schema_name, "db3");
}

#[test]
fn test_attach_quoted_schema_name() {
    let stmt = parse_attach("ATTACH ':memory:' AS \"Aux Db\"");
    assert_eq!(stmt.schema_name, "Aux Db");
}

#[test]
fn test_attach_string_schema_name() {
    // SQLite accepts a string literal in the database-name position.
    let stmt = parse_attach("ATTACH ':memory:' AS 'aux2'");
    assert_eq!(stmt.schema_name, "aux2");
}

#[test]
fn test_attach_requires_string_filename() {
    // Phase 1 accepts only a string-literal filename (see #6362 for
    // expression filenames).
    assert!(Parser::parse_sql("ATTACH foo AS aux").is_err());
    assert!(Parser::parse_sql("ATTACH 42 AS aux").is_err());
}

#[test]
fn test_attach_requires_as_and_name() {
    assert!(Parser::parse_sql("ATTACH ':memory:'").is_err());
    assert!(Parser::parse_sql("ATTACH ':memory:' aux").is_err());
    assert!(Parser::parse_sql("ATTACH ':memory:' AS").is_err());
}

#[test]
fn test_attach_rejects_trailing_garbage() {
    assert!(Parser::parse_sql("ATTACH ':memory:' AS aux extra").is_err());
}

#[test]
fn test_detach() {
    let stmt = parse_detach("DETACH aux");
    assert_eq!(stmt.schema_name, "aux");
}

#[test]
fn test_detach_with_database_noise_word() {
    let stmt = parse_detach("DETACH DATABASE aux;");
    assert_eq!(stmt.schema_name, "aux");
}

#[test]
fn test_detach_missing_name_errors() {
    assert!(Parser::parse_sql("DETACH").is_err());
    assert!(Parser::parse_sql("DETACH DATABASE").is_err());
}

#[test]
fn test_attach_detach_not_reserved_as_identifiers() {
    // ATTACH / DETACH / DATABASE are dispatched as statement-leading
    // identifiers, NOT lexer keywords — existing SQL using them as ordinary
    // identifiers must keep parsing.
    for sql in [
        "CREATE TABLE attach (detach INTEGER, database TEXT)",
        "SELECT attach, detach, database FROM t1",
        "SELECT * FROM attach WHERE detach = 1",
        "INSERT INTO detach (attach) VALUES (1)",
    ] {
        assert!(
            Parser::parse_sql(sql).is_ok(),
            "identifier use of attach/detach/database failed to parse: {sql}"
        );
    }
}
