use super::super::*;

// ========================================================================
// SQLite fallback keywords as identifiers (keyword1.test, issue #5816)
// ========================================================================
// SQLite's parser demotes "fallback" keywords to plain identifiers whenever
// the keyword reading does not fit the grammar (%fallback ID in parse.y).
// keyword1.test exercises 58 such words as table, column, *type*, and index
// names. Truly-reserved words (PRIMARY, NOT, CROSS, SELECT, ...) must stay
// rejected in type-name position — verified against the sqlite3 oracle.

/// The full kwlist from keyword1.test. Every word must be accepted in
/// `CREATE TABLE <kw>(<kw> <kw>)` (with `if` quoted in table position, as in
/// the test itself, since bare `CREATE TABLE if` collides with IF NOT EXISTS
/// in SQLite too).
const KEYWORD1_KWLIST: &[&str] = &[
    "abort",
    "after",
    "analyze",
    "asc",
    "attach",
    "before",
    "begin",
    "by",
    "cascade",
    "cast",
    "column",
    "conflict",
    "current_date",
    "current_time",
    "current_timestamp",
    "database",
    "deferred",
    "desc",
    "detach",
    "end",
    "each",
    "exclusive",
    "explain",
    "fail",
    "for",
    "glob",
    "if",
    "ignore",
    "immediate",
    "initially",
    "instead",
    "key",
    "like",
    "match",
    "of",
    "offset",
    "plan",
    "pragma",
    "query",
    "raise",
    "recursive",
    "regexp",
    "reindex",
    "release",
    "rename",
    "replace",
    "restrict",
    "rollback",
    "row",
    "savepoint",
    "temp",
    "temporary",
    "trigger",
    "vacuum",
    "view",
    "virtual",
    "with",
    "without",
];

#[test]
fn test_all_keyword1_words_accepted_as_table_column_and_type_names() {
    for kw in KEYWORD1_KWLIST {
        let table = if *kw == "if" { format!("\"{}\"", kw) } else { (*kw).to_string() };
        let sql = format!("CREATE TABLE {}({} {});", table, kw, kw);
        let result = Parser::parse_sql(&sql);
        assert!(
            result.is_ok(),
            "keyword1.test word {:?} must parse as table/column/type name: {:?}",
            kw,
            result.err()
        );
    }
}

#[test]
fn test_fallback_keyword_type_name_becomes_user_defined() {
    // Spot-check that a fallback keyword in type position maps to
    // DataType::UserDefined with the lowercased name (affinity-only storage,
    // like any unrecognized SQLite type name).
    for kw in ["abort", "end", "with", "current_date", "cast"] {
        let sql = format!("CREATE TABLE t1(x {});", kw);
        let stmt = Parser::parse_sql(&sql)
            .unwrap_or_else(|e| panic!("type name {:?} must parse: {:?}", kw, e));
        match stmt {
            vibesql_ast::Statement::CreateTable(create) => match &create.columns[0].data_type {
                vibesql_types::DataType::UserDefined { type_name } => {
                    assert_eq!(type_name, kw, "keyword type name must be stored lowercased");
                }
                other => panic!("Expected UserDefined for {:?}, got {:?}", kw, other),
            },
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }
}

#[test]
fn test_reserved_words_still_rejected_as_type_names() {
    // sqlite3 oracle: these are NOT fallback keywords and are rejected in
    // type-name position (`CREATE TABLE t(x primary)` is a syntax error).
    // NULL / UNIQUE are deliberately absent here: SQLite (and VibeSQL) parse
    // `CREATE TABLE t(x null)` / `(x unique)` as a typeless column with a
    // constraint, so they are accepted — via a different production.
    for kw in
        ["primary", "not", "cross", "select", "from", "where", "table", "references", "default"]
    {
        let sql = format!("CREATE TABLE t1(x {});", kw);
        let result = Parser::parse_sql(&sql);
        assert!(
            result.is_err(),
            "reserved word {:?} must NOT be accepted as a type name (SQLite parity)",
            kw
        );
    }
}

#[test]
fn test_column_constraints_still_parse_after_keyword_relaxation() {
    // Regression guards from issue #5816 acceptance criteria.
    for sql in [
        "CREATE TABLE t1(a PRIMARY KEY);",
        "CREATE TABLE t1(a NOT NULL);",
        "CREATE TABLE t1(a INTEGER PRIMARY KEY);",
        "CREATE TABLE t1(a VARYING CHARACTER(255));",
        "CREATE TABLE t1(a, b);",
        "CREATE INDEX IF NOT EXISTS i1 ON t1(a);",
    ] {
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "{:?} must still parse: {:?}", sql, result.err());
    }
}

#[test]
fn test_fallback_keywords_in_expression_and_from_position() {
    // keyword1.test .1 shape: bare keyword as table name (FROM) and as a
    // column reference in ORDER BY. These words were previously denylisted in
    // expression position.
    for kw in ["by", "end", "like", "glob", "offset", "pragma", "recursive", "replace", "with"] {
        let sql = format!("SELECT * FROM {} ORDER BY {} ASC;", kw, kw);
        let result = Parser::parse_sql(&sql);
        assert!(
            result.is_ok(),
            "bare {:?} must parse in FROM and ORDER BY position: {:?}",
            kw,
            result.err()
        );
    }

    // CAST / CURRENT_* keep their special expression parsing (SQLite rejects
    // bare `ORDER BY cast` too — keyword1.test quotes them there) but are
    // legal table names after FROM.
    for kw in ["cast", "current_date", "current_time", "current_timestamp"] {
        let sql = format!("SELECT * FROM {};", kw);
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "{:?} must parse as FROM table name: {:?}", kw, result.err());

        let sql = format!("SELECT x FROM t ORDER BY {} ASC;", kw);
        // CURRENT_* parse as datetime literals in ORDER BY (valid); bare CAST
        // must remain a syntax error, matching SQLite.
        if kw == "cast" {
            assert!(
                Parser::parse_sql(&sql).is_err(),
                "bare ORDER BY cast must stay a syntax error (SQLite parity)"
            );
        }
    }
}

#[test]
fn test_operator_and_special_form_usage_still_parses() {
    // Relaxing LIKE/GLOB/END/etc. in *operand* position must not disturb
    // their operator / special-form roles.
    for sql in [
        "SELECT a FROM t WHERE a LIKE 'x%';",
        "SELECT a FROM t WHERE a NOT LIKE 'x%';",
        "SELECT a FROM t WHERE a GLOB 'x*';",
        "SELECT CASE WHEN a > 1 THEN 2 ELSE 3 END FROM t;",
        "SELECT replace('abc', 'b', 'x');",
        "SELECT like('a%', 'abc');",
        "SELECT * FROM t LIMIT 10 OFFSET 5;",
        "WITH q AS (SELECT 1 AS x) SELECT x FROM q;",
        "SELECT (WITH q AS (SELECT 1 AS x) SELECT x FROM q);",
        "SELECT CAST(a AS INTEGER) FROM t;",
    ] {
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "{:?} must still parse: {:?}", sql, result.err());
    }
}
