//! Tests for parser error handling with malformed SQL.

use crate::parser::Parser;

#[test]
fn test_parse_error_unexpected_token_in_select() {
    let result = Parser::parse_sql("SELECT FROM users");
    assert!(result.is_err(), "Should fail with unexpected FROM");
}

#[test]
fn test_parse_error_incomplete_select() {
    let result = Parser::parse_sql("select");
    assert!(result.is_err(), "Should fail with incomplete SELECT");
}

#[test]
fn test_parse_error_missing_from_table_name() {
    let result = Parser::parse_sql("SELECT * FROM");
    assert!(result.is_err(), "Should fail with missing table name");
}

#[test]
fn test_parse_error_incomplete_where_clause() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE");
    assert!(result.is_err(), "Should fail with incomplete WHERE");
}

#[test]
fn test_parse_error_incomplete_insert() {
    let result = Parser::parse_sql("INSERT INTO");
    assert!(result.is_err(), "Should fail with incomplete INSERT");
}

#[test]
fn test_parse_error_missing_values_keyword() {
    let result = Parser::parse_sql("INSERT INTO users");
    assert!(result.is_err(), "Should fail with missing VALUES");
}

#[test]
fn test_parse_error_incomplete_update() {
    let result = Parser::parse_sql("update");
    assert!(result.is_err(), "Should fail with incomplete UPDATE");
}

#[test]
fn test_parse_error_missing_set_keyword() {
    let result = Parser::parse_sql("UPDATE users");
    assert!(result.is_err(), "Should fail with missing SET");
}

#[test]
fn test_parse_error_incomplete_delete() {
    let result = Parser::parse_sql("delete");
    assert!(result.is_err(), "Should fail with incomplete DELETE");
}

#[test]
fn test_parse_error_missing_from_in_delete() {
    let result = Parser::parse_sql("DELETE users");
    assert!(result.is_err(), "Should fail with missing FROM");
}

#[test]
fn test_parse_error_incomplete_create_table() {
    let result = Parser::parse_sql("CREATE TABLE");
    assert!(result.is_err(), "Should fail with incomplete CREATE TABLE");
}

#[test]
fn test_parse_error_missing_columns_in_create() {
    let result = Parser::parse_sql("CREATE TABLE users");
    assert!(result.is_err(), "Should fail with missing column list");
}

#[test]
fn test_parse_error_incomplete_join() {
    let result = Parser::parse_sql("SELECT * FROM users JOIN");
    assert!(result.is_err(), "Should fail with incomplete JOIN");
}

#[test]
fn test_parse_error_incomplete_group_by() {
    let result = Parser::parse_sql("SELECT * FROM users GROUP BY");
    assert!(result.is_err(), "Should fail with incomplete GROUP BY");
}

#[test]
fn test_parse_error_incomplete_order_by() {
    let result = Parser::parse_sql("SELECT * FROM users ORDER BY");
    assert!(result.is_err(), "Should fail with incomplete ORDER BY");
}

#[test]
fn test_parse_error_mismatched_parentheses() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE (id = 1");
    assert!(result.is_err(), "Should fail with mismatched parentheses");
}

#[test]
fn test_parse_error_invalid_operator() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id === 1");
    assert!(result.is_err(), "Should fail with invalid operator");
}

#[test]
fn test_parse_error_incomplete_having() {
    let result = Parser::parse_sql("SELECT COUNT(*) FROM users GROUP BY id HAVING");
    assert!(result.is_err(), "Should fail with incomplete HAVING");
}

#[test]
fn test_parse_error_empty_sql() {
    let result = Parser::parse_sql("");
    assert!(result.is_err(), "Should fail with empty SQL");
}

#[test]
fn test_parse_error_only_semicolon() {
    let result = Parser::parse_sql(";");
    assert!(result.is_err(), "Should fail with only semicolon");
}

#[test]
fn test_parse_error_incomplete_limit() {
    let result = Parser::parse_sql("SELECT * FROM users LIMIT");
    assert!(result.is_err(), "Should fail with incomplete LIMIT");
}

#[test]
fn test_parse_error_incomplete_offset() {
    let result = Parser::parse_sql("SELECT * FROM users OFFSET");
    assert!(result.is_err(), "Should fail with incomplete OFFSET");
}

#[test]
fn test_parse_error_incomplete_subquery() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id IN (");
    assert!(result.is_err(), "Should fail with incomplete subquery");
}

#[test]
fn test_parse_error_missing_select_in_subquery() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id IN (1, 2");
    assert!(result.is_err(), "Should fail with incomplete value list");
}

#[test]
fn test_parse_error_unexpected_keyword() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE WHERE id = 1");
    assert!(result.is_err(), "Should fail with duplicate WHERE");
}

#[test]
fn test_parse_error_incomplete_expression() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id +");
    assert!(result.is_err(), "Should fail with incomplete expression");
}

#[test]
fn test_parse_error_missing_table_in_from() {
    let result = Parser::parse_sql("SELECT id, name FROM");
    assert!(result.is_err(), "Should fail with missing table");
}

#[test]
fn test_parse_error_incomplete_set_clause() {
    let result = Parser::parse_sql("UPDATE users SET");
    assert!(result.is_err(), "Should fail with incomplete SET clause");
}

#[test]
fn test_parse_error_incomplete_insert_values() {
    let result = Parser::parse_sql("INSERT INTO users VALUES");
    assert!(result.is_err(), "Should fail with incomplete VALUES");
}

#[test]
fn test_parse_error_create_table_empty_parens() {
    let result = Parser::parse_sql("CREATE TABLE users ()");
    assert!(result.is_err(), "Should fail with empty column list");
}

#[test]
fn test_parse_error_incomplete_column_definition() {
    let result = Parser::parse_sql("CREATE TABLE users (id");
    assert!(result.is_err(), "Should fail with incomplete column");
}

#[test]
fn test_parse_error_missing_data_type() {
    let result = Parser::parse_sql("CREATE TABLE users (id,");
    assert!(result.is_err(), "Should fail with missing data type");
}

#[test]
fn test_parse_error_unexpected_eof_in_select_list() {
    let result = Parser::parse_sql("SELECT id,");
    assert!(result.is_err(), "Should fail with incomplete select list");
}

#[test]
fn test_parse_error_unclosed_parenthesis() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE (id = 1 OR name = 'test'");
    assert!(result.is_err(), "Should fail with unclosed parenthesis");
}

#[test]
fn test_parse_error_missing_expression_after_operator() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id = ");
    assert!(result.is_err(), "Should fail with missing expression");
}

#[test]
fn test_parse_error_invalid_table_source() {
    let result = Parser::parse_sql("SELECT * FROM 123");
    assert!(result.is_err(), "Should fail with invalid table source");
}

#[test]
fn test_parse_error_missing_join_condition() {
    let result = Parser::parse_sql("SELECT * FROM users LEFT JOIN orders ON");
    assert!(result.is_err(), "Should fail with missing join condition");
}

#[test]
fn test_parse_error_invalid_assignment_in_update() {
    let result = Parser::parse_sql("UPDATE users SET name");
    assert!(result.is_err(), "Should fail with invalid assignment");
}

#[test]
fn test_parse_error_missing_parenthesis_in_insert() {
    let result = Parser::parse_sql("INSERT INTO users VALUES (1, 'test'");
    assert!(result.is_err(), "Should fail with missing closing paren");
}

#[test]
fn test_parse_error_empty_where_clause() {
    let result = Parser::parse_sql("DELETE FROM users WHERE");
    assert!(result.is_err(), "Should fail with empty WHERE clause");
}

#[test]
fn test_parse_error_missing_from_in_update_statement() {
    let result = Parser::parse_sql("UPDATE SET name = 'test'");
    assert!(result.is_err(), "Should fail with missing table name");
}

#[test]
fn test_parse_error_select_with_just_comma() {
    let result = Parser::parse_sql("SELECT id, , name FROM users");
    assert!(result.is_err(), "Should fail with consecutive commas");
}

// Tests for improved error messages when reserved keywords are used as identifiers

#[test]
fn test_keyword_in_select_into() {
    // SELECT INTO uses parse_identifier() for the target table name
    let result = Parser::parse_sql("SELECT * INTO select FROM users");
    assert!(result.is_err(), "Should fail when using SELECT as INTO target");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("reserved keyword"),
        "Error should mention 'reserved keyword', got: {}",
        error_msg
    );
    assert!(
        error_msg.to_lowercase().contains("select"),
        "Error should mention the keyword SELECT, got: {}",
        error_msg
    );
    assert!(
        error_msg.contains("delimited identifiers"),
        "Error should suggest delimited identifiers, got: {}",
        error_msg
    );
}

#[test]
fn test_keyword_in_next_value_for() {
    // NEXT VALUE FOR uses parse_identifier() for the sequence name
    let result = Parser::parse_sql("SELECT NEXT VALUE FOR select");
    assert!(result.is_err(), "Should fail when using SELECT as sequence name");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("reserved keyword"),
        "Error should mention 'reserved keyword', got: {}",
        error_msg
    );
    assert!(
        error_msg.to_lowercase().contains("select"),
        "Error should mention the keyword SELECT, got: {}",
        error_msg
    );
}

#[test]
fn test_keyword_table_in_select_into() {
    let result = Parser::parse_sql("SELECT * INTO table FROM users");
    assert!(result.is_err(), "Should fail when using TABLE as INTO target");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("reserved keyword"),
        "Error should mention 'reserved keyword', got: {}",
        error_msg
    );
    assert!(
        error_msg.to_lowercase().contains("table"),
        "Error should mention the keyword TABLE, got: {}",
        error_msg
    );
}

#[test]
fn test_keyword_where_in_next_value_for() {
    let result = Parser::parse_sql("SELECT NEXT VALUE FOR where");
    assert!(result.is_err(), "Should fail when using WHERE as sequence name");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("reserved keyword"),
        "Error should mention 'reserved keyword', got: {}",
        error_msg
    );
}

#[test]
fn test_keyword_from_in_select_into() {
    let result = Parser::parse_sql("SELECT * INTO from FROM users");
    assert!(result.is_err(), "Should fail when using FROM as INTO target");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("reserved keyword"),
        "Error should mention 'reserved keyword', got: {}",
        error_msg
    );
}

#[test]
fn test_keyword_join_in_next_value_for() {
    let result = Parser::parse_sql("SELECT NEXT VALUE FOR join");
    assert!(result.is_err(), "Should fail when using JOIN as sequence name");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("reserved keyword"),
        "Error should mention 'reserved keyword', got: {}",
        error_msg
    );
}

#[test]
fn test_delimited_keyword_in_select_into_works() {
    // Delimited identifiers should allow using keywords
    let result = Parser::parse_sql("SELECT * INTO \"select\" FROM users");
    assert!(
        result.is_ok(),
        "Should succeed with delimited keyword identifier in SELECT INTO, got error: {:?}",
        result.err()
    );
}

#[test]
fn test_delimited_keyword_in_next_value_for_works() {
    // Delimited identifiers should allow using keywords
    let result = Parser::parse_sql("SELECT NEXT VALUE FOR \"table\"");
    assert!(
        result.is_ok(),
        "Should succeed with delimited keyword in NEXT VALUE FOR, got error: {:?}",
        result.err()
    );
}

#[test]
fn test_error_message_includes_suggestion() {
    // Verify the complete error message format
    let result = Parser::parse_sql("SELECT * INTO where FROM users");
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();

    // Should have all three parts:
    // 1. "reserved keyword"
    // 2. The specific keyword (WHERE)
    // 3. Suggestion to use delimited identifiers
    assert!(error_msg.contains("reserved keyword"), "Missing 'reserved keyword' in: {}", error_msg);
    assert!(error_msg.to_lowercase().contains("where"), "Missing keyword name in: {}", error_msg);
    assert!(
        error_msg.contains("delimited identifiers") || error_msg.contains("\""),
        "Missing suggestion in: {}",
        error_msg
    );
}

// Issue #4448: Parser should reject incomplete input and syntax errors
// https://github.com/rjwalters/vibesql/issues/4448

#[test]
fn test_issue_4448_incomplete_alias_in_from() {
    // Issue #4448 Case 1: Incomplete input - missing alias after AS
    // SQLite returns: near ";": syntax error
    // VibeSQL should also return syntax error
    let result = Parser::parse_sql("SELECT f1 FROM test1 as 'hi', test2 as");
    assert!(result.is_err(), "Should fail with incomplete alias after AS, got: {:?}", result);
}

#[test]
fn test_issue_4448_incomplete_alias_with_semicolon() {
    // Same as above but with explicit semicolon
    let result = Parser::parse_sql("SELECT f1 FROM test1 as 'hi', test2 as;");
    assert!(result.is_err(), "Should fail with incomplete alias after AS, got: {:?}", result);
}

#[test]
fn test_issue_4448_order_by_after_limit_offset() {
    // Issue #4448 Case 2: ORDER BY after LIMIT/OFFSET is rejected by SQLite
    // SQLite: SELECT f1 FROM test1 LIMIT 5+3 OFFSET 1 ORDER BY f2
    // Returns: near "ORDER": syntax error
    let result = Parser::parse_sql("SELECT f1 FROM test1 LIMIT 8 OFFSET 1 ORDER BY f2");
    assert!(result.is_err(), "Should fail with ORDER BY after LIMIT/OFFSET, got: {:?}", result);
}

#[test]
fn test_issue_4448_order_by_after_limit_only() {
    // ORDER BY after just LIMIT (no OFFSET) should also be rejected
    let result = Parser::parse_sql("SELECT f1 FROM test1 LIMIT 5 ORDER BY f2");
    assert!(result.is_err(), "Should fail with ORDER BY after LIMIT, got: {:?}", result);
}

#[test]
fn test_issue_4448_unexpected_keyword_after_order_by() {
    // Issue #4448 Case 3: Unexpected keyword after ORDER BY items
    // SQLite: SELECT f1 FROM test1 ORDER BY f1 desc, f2 where
    // Returns: near "where": syntax error
    let result = Parser::parse_sql("SELECT f1 FROM test1 ORDER BY f1 desc, f2 where");
    assert!(
        result.is_err(),
        "Should fail with unexpected keyword 'where' after ORDER BY, got: {:?}",
        result
    );
}

#[test]
fn test_issue_4448_unexpected_keyword_from_after_order_by() {
    // Similar test with FROM keyword
    let result = Parser::parse_sql("SELECT f1 FROM test1 ORDER BY f1 from");
    assert!(
        result.is_err(),
        "Should fail with unexpected keyword 'from' after ORDER BY, got: {:?}",
        result
    );
}

#[test]
fn test_issue_4448_valid_order_by_before_limit() {
    // Valid SQL: ORDER BY should come BEFORE LIMIT/OFFSET
    let result = Parser::parse_sql("SELECT f1 FROM test1 ORDER BY f1 LIMIT 5");
    assert!(
        result.is_ok(),
        "Should succeed with valid ORDER BY ... LIMIT, got error: {:?}",
        result.err()
    );
}

#[test]
fn test_issue_4448_valid_order_by_before_limit_offset() {
    // Valid SQL: ORDER BY should come BEFORE LIMIT and OFFSET
    let result = Parser::parse_sql("SELECT f1 FROM test1 ORDER BY f1 LIMIT 5 OFFSET 2");
    assert!(
        result.is_ok(),
        "Should succeed with valid ORDER BY ... LIMIT ... OFFSET, got error: {:?}",
        result.err()
    );
}

// Issue #4467: Error messages should preserve original token case
// https://github.com/rjwalters/vibesql/issues/4467

#[test]
fn test_issue_4467_syntax_error_preserves_keyword_case() {
    // SQLite preserves the original case in error messages
    // e.g., "SeLeCt" produces: near "SeLeCt": syntax error
    let result = Parser::parse_sql("SeLeCt FrOm users");
    assert!(result.is_err(), "Should fail with syntax error");
    let error_msg = result.unwrap_err().to_string();
    // The error should contain the original case "FrOm", not "FROM"
    assert!(
        error_msg.contains("FrOm"),
        "Error should preserve original case 'FrOm', got: {}",
        error_msg
    );
}

#[test]
fn test_issue_4467_incomplete_input_lowercase() {
    // Incomplete input with lowercase keywords should still be detected
    let result = Parser::parse_sql("select * from users where");
    assert!(result.is_err(), "Should fail with incomplete input");
    let error_msg = result.unwrap_err().to_string();
    // Should indicate incomplete input (EOF after WHERE)
    assert!(
        error_msg.contains("incomplete") || error_msg.contains("expected"),
        "Error should indicate incomplete input, got: {}",
        error_msg
    );
}

#[test]
fn test_issue_4467_mixed_case_keyword_error() {
    // Mixed case keyword in error context
    let result = Parser::parse_sql("SELECT * FROM users WhErE WhErE x = 1");
    assert!(result.is_err(), "Should fail with duplicate WHERE");
    let error_msg = result.unwrap_err().to_string();
    // The error should contain the original case
    assert!(
        error_msg.contains("WhErE"),
        "Error should preserve original case 'WhErE', got: {}",
        error_msg
    );
}

// ========================================================================
// Issue #5271: bare SELECT/VALUES with a trailing RETURNING clause must be
// a syntax error (SQLite: Parse error: near "RETURNING": syntax error).
// RETURNING is only valid as part of a DML statement (INSERT/UPDATE/DELETE).
// ========================================================================

#[test]
fn test_issue_5271_bare_select_returning_is_error() {
    let result = Parser::parse_sql("SELECT 1 RETURNING a;");
    assert!(result.is_err(), "bare SELECT ... RETURNING should be a syntax error");
}

#[test]
fn test_issue_5271_bare_select_from_returning_is_error() {
    let result = Parser::parse_sql("SELECT * FROM t RETURNING;");
    assert!(result.is_err(), "bare SELECT ... RETURNING should be a syntax error");
}

#[test]
fn test_issue_5271_bare_values_returning_is_error() {
    let result = Parser::parse_sql("VALUES(1) RETURNING a;");
    assert!(result.is_err(), "bare VALUES ... RETURNING should be a syntax error");
}

#[test]
fn test_issue_5271_bare_compound_select_returning_is_error() {
    let result = Parser::parse_sql("SELECT 1 UNION SELECT 2 RETURNING a;");
    assert!(result.is_err(), "bare compound SELECT ... RETURNING should be a syntax error");
}

#[test]
fn test_issue_5271_bare_select_returning_arena_fallback_is_error() {
    // The CLI path goes through parse_with_arena_fallback; the arena parser
    // rejects this on its own, but the owned-parser fallback must reject it too.
    let result = crate::parse_with_arena_fallback("SELECT 1 RETURNING a");
    assert!(result.is_err(), "bare SELECT ... RETURNING should be a syntax error in both parsers");
}

// ========================================================================
// Issue #5261: trailing garbage tokens after the last clause of
// UPDATE/DELETE/INSERT must be a syntax error (SQLite: near "X": syntax
// error). Previously the parser stopped consuming after the last clause
// and silently ignored the rest of the input.
// ========================================================================

#[test]
fn test_issue_5261_update_trailing_garbage_is_error() {
    let result = Parser::parse_sql("UPDATE t SET a=1 WHERE b=2 XYZZY");
    assert!(result.is_err(), "UPDATE with trailing garbage should be a syntax error");
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("near \"XYZZY\": syntax error"), "unexpected error message: {}", msg);
}

#[test]
fn test_issue_5261_update_trailing_garbage_multiple_tokens_is_error() {
    let result = Parser::parse_sql("UPDATE t SET a=8 WHERE a=7 GARBAGE TOKENS");
    assert!(result.is_err(), "UPDATE with trailing garbage should be a syntax error");
}

#[test]
fn test_issue_5261_update_returning_trailing_garbage_is_error() {
    // Note: `RETURNING a XYZZY` is valid SQLite (XYZZY is an implicit column
    // alias), so use a reserved keyword that cannot be an alias instead.
    let result = Parser::parse_sql("UPDATE t SET a=1 RETURNING a WHERE b=2");
    assert!(result.is_err(), "UPDATE ... RETURNING with trailing clause should be a syntax error");
}

#[test]
fn test_issue_5261_update_with_cte_trailing_garbage_is_error() {
    let result = Parser::parse_sql("WITH c AS (SELECT 1) UPDATE t SET a=1 WHERE b=2 XYZZY");
    assert!(result.is_err(), "WITH ... UPDATE with trailing garbage should be a syntax error");
}

#[test]
fn test_issue_5261_delete_trailing_garbage_is_error() {
    let result = Parser::parse_sql("DELETE FROM t WHERE a=1 XYZZY");
    assert!(result.is_err(), "DELETE with trailing garbage should be a syntax error");
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("near \"XYZZY\": syntax error"), "unexpected error message: {}", msg);
}

#[test]
fn test_issue_5261_delete_limit_trailing_garbage_is_error() {
    let result = Parser::parse_sql("DELETE FROM t WHERE a=1 ORDER BY a LIMIT 1 XYZZY");
    assert!(result.is_err(), "DELETE ... LIMIT with trailing garbage should be a syntax error");
}

#[test]
fn test_issue_5261_delete_with_cte_trailing_garbage_is_error() {
    let result = Parser::parse_sql("WITH c AS (SELECT 1) DELETE FROM t WHERE a=1 XYZZY");
    assert!(result.is_err(), "WITH ... DELETE with trailing garbage should be a syntax error");
}

#[test]
fn test_issue_5261_insert_trailing_garbage_is_error() {
    let result = Parser::parse_sql("INSERT INTO t VALUES (1) XYZZY");
    assert!(result.is_err(), "INSERT with trailing garbage should be a syntax error");
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("near \"XYZZY\": syntax error"), "unexpected error message: {}", msg);
}

#[test]
fn test_issue_5261_insert_returning_trailing_garbage_is_error() {
    // Note: `RETURNING a XYZZY` is valid SQLite (implicit alias); a numeric
    // literal cannot be an alias, matching SQLite's near "123" error.
    let result = Parser::parse_sql("INSERT INTO t VALUES (1) RETURNING a 123");
    assert!(result.is_err(), "INSERT ... RETURNING with trailing garbage should be a syntax error");
}

#[test]
fn test_issue_5261_insert_on_conflict_trailing_garbage_is_error() {
    let result = Parser::parse_sql("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING XYZZY");
    assert!(
        result.is_err(),
        "INSERT ... ON CONFLICT with trailing garbage should be a syntax error"
    );
}

#[test]
fn test_issue_5261_insert_with_cte_trailing_garbage_is_error() {
    // Note: `FROM c XYZZY` is valid SQLite (implicit table alias), so use a
    // dangling GROUP keyword that cannot be an alias instead.
    let result =
        Parser::parse_sql("WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c WHERE 1 GROUP");
    assert!(result.is_err(), "WITH ... INSERT with trailing garbage should be a syntax error");
}

#[test]
fn test_issue_5261_replace_trailing_garbage_is_error() {
    let result = Parser::parse_sql("REPLACE INTO t VALUES (1) XYZZY");
    assert!(result.is_err(), "REPLACE with trailing garbage should be a syntax error");
}

#[test]
fn test_issue_5261_garbage_after_semicolon_keyword_token() {
    // Keyword garbage (not just identifiers) must also be rejected
    let result = Parser::parse_sql("UPDATE t SET a=1 WHERE b=2 SELECT");
    assert!(result.is_err(), "UPDATE with trailing keyword should be a syntax error");
}

#[test]
fn test_issue_5261_arena_update_trailing_garbage_is_error() {
    let result = crate::arena_parser::parse_update_to_owned("UPDATE t SET a=1 WHERE b=2 XYZZY");
    assert!(result.is_err(), "arena UPDATE with trailing garbage should be a syntax error");
}

#[test]
fn test_issue_5261_arena_delete_trailing_garbage_is_error() {
    let result = crate::arena_parser::parse_delete_to_owned("DELETE FROM t WHERE a=1 XYZZY");
    assert!(result.is_err(), "arena DELETE with trailing garbage should be a syntax error");
}

#[test]
fn test_issue_5261_arena_insert_trailing_garbage_is_error() {
    let result = crate::arena_parser::parse_insert_to_owned("INSERT INTO t VALUES (1) XYZZY");
    assert!(result.is_err(), "arena INSERT with trailing garbage should be a syntax error");
}

// Valid statements must be unaffected by the end-of-statement check.

#[test]
fn test_issue_5261_valid_dml_statements_still_parse() {
    let valid = [
        "UPDATE t SET a=1 WHERE b=2",
        "UPDATE t SET a=1 WHERE b=2;",
        "UPDATE t SET a=1 RETURNING a, b",
        "UPDATE t SET a=1 FROM u WHERE t.id=u.id RETURNING *;",
        "DELETE FROM t WHERE a=1",
        "DELETE FROM t WHERE a=1;",
        "DELETE FROM t WHERE a=1 ORDER BY a LIMIT 1 OFFSET 2",
        "DELETE FROM t RETURNING *",
        "INSERT INTO t VALUES (1)",
        "INSERT INTO t VALUES (1);",
        "INSERT INTO t VALUES (1) RETURNING rowid",
        "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
        "INSERT INTO t VALUES (1) ON CONFLICT(a) DO UPDATE SET b=2 RETURNING a",
        "INSERT INTO t SELECT * FROM u",
        "REPLACE INTO t VALUES (1)",
        "WITH c AS (SELECT 1) UPDATE t SET a=1 WHERE b IN (SELECT * FROM c)",
        "WITH c AS (SELECT 1) DELETE FROM t WHERE a IN (SELECT * FROM c)",
        "WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c",
    ];
    for sql in valid {
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "valid statement should parse: {} -> {:?}", sql, result.err());
    }
}

#[test]
fn test_issue_5261_valid_arena_dml_statements_still_parse() {
    assert!(crate::arena_parser::parse_update_to_owned("UPDATE t SET a=1 WHERE b=2;").is_ok());
    assert!(crate::arena_parser::parse_delete_to_owned("DELETE FROM t WHERE a=1;").is_ok());
    assert!(crate::arena_parser::parse_insert_to_owned(
        "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING RETURNING a;"
    )
    .is_ok());
}
