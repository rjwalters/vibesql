//! JSON functions (SQLite JSON1 extension compatibility)
//!
//! This module contains SQLite-compatible JSON functions:
//! - json(X) - Validate and minify JSON
//! - json_valid(X) - Test whether X is well-formed JSON
//! - json_extract(X, P, ...) - Extract value(s) at JSON path(s)
//! - json_type(X) / json_type(X, P) - Type of the JSON value
//! - json_quote(X) - Render a SQL value as a JSON value
//! - `->` / `->>` operators (see [`eval_json_arrow`])
//!
//! Reference: https://www.sqlite.org/json1.html

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// A single component of a SQLite JSON path (the grammar accepted by
/// `json_extract`, `->`, `->>`, etc.).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PathSegment {
    /// Object member access: `.key` or `."quoted key"`
    Key(String),
    /// Zero-based array index: `[n]`
    Index(usize),
    /// From-the-end array index: `[#-n]` (n >= 1 selects `len - n`)
    IndexFromEnd(usize),
}

/// Parse a SQLite JSON path string into a sequence of [`PathSegment`]s.
///
/// The path must begin with `$` (the document root). Supported components:
/// `.key`, `."quoted key"`, `[n]`, and `[#-n]`. On any syntax error this
/// returns the SQLite-compatible error text `bad JSON path: '<path>'`.
pub(crate) fn parse_sqlite_json_path(path: &str) -> Result<Vec<PathSegment>, String> {
    let bad = || format!("bad JSON path: '{}'", path);
    let chars: Vec<char> = path.chars().collect();
    let mut i = 0;

    if chars.first() != Some(&'$') {
        return Err(bad());
    }
    i += 1;

    let mut segments = Vec::new();
    while i < chars.len() {
        match chars[i] {
            '.' => {
                i += 1;
                if chars.get(i) == Some(&'"') {
                    // Quoted key: read until the closing unescaped quote.
                    i += 1;
                    let mut key = String::new();
                    let mut closed = false;
                    while i < chars.len() {
                        let c = chars[i];
                        if c == '\\' && i + 1 < chars.len() {
                            match chars[i + 1] {
                                '"' => key.push('"'),
                                '\\' => key.push('\\'),
                                '/' => key.push('/'),
                                'n' => key.push('\n'),
                                't' => key.push('\t'),
                                'r' => key.push('\r'),
                                other => {
                                    key.push('\\');
                                    key.push(other);
                                }
                            }
                            i += 2;
                        } else if c == '"' {
                            closed = true;
                            i += 1;
                            break;
                        } else {
                            key.push(c);
                            i += 1;
                        }
                    }
                    if !closed {
                        return Err(bad());
                    }
                    segments.push(PathSegment::Key(key));
                } else {
                    // Bare key: read until the next '.' or '['. Empty is an error.
                    let start = i;
                    while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
                        i += 1;
                    }
                    if i == start {
                        return Err(bad());
                    }
                    segments.push(PathSegment::Key(chars[start..i].iter().collect()));
                }
            }
            '[' => {
                i += 1;
                if chars.get(i) == Some(&'#') {
                    i += 1;
                    if chars.get(i) == Some(&'-') {
                        i += 1;
                        let start = i;
                        while i < chars.len() && chars[i].is_ascii_digit() {
                            i += 1;
                        }
                        if i == start {
                            return Err(bad());
                        }
                        let n: usize = chars[start..i]
                            .iter()
                            .collect::<String>()
                            .parse()
                            .map_err(|_| bad())?;
                        if chars.get(i) != Some(&']') {
                            return Err(bad());
                        }
                        i += 1;
                        segments.push(PathSegment::IndexFromEnd(n));
                    } else if chars.get(i) == Some(&']') {
                        // `[#]` selects one past the last element (append slot); for
                        // extraction it never matches, so model it as IndexFromEnd(0).
                        i += 1;
                        segments.push(PathSegment::IndexFromEnd(0));
                    } else {
                        return Err(bad());
                    }
                } else {
                    let start = i;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                    if i == start {
                        return Err(bad());
                    }
                    let n: usize = chars[start..i]
                        .iter()
                        .collect::<String>()
                        .parse()
                        .map_err(|_| bad())?;
                    if chars.get(i) != Some(&']') {
                        return Err(bad());
                    }
                    i += 1;
                    segments.push(PathSegment::Index(n));
                }
            }
            _ => return Err(bad()),
        }
    }

    Ok(segments)
}

/// Navigate a parsed JSON value along a path, returning the referenced node if
/// it exists (a JSON `null` node still counts as existing).
fn navigate<'a>(
    value: &'a serde_json::Value,
    segments: &[PathSegment],
) -> Option<&'a serde_json::Value> {
    let mut cur = value;
    for seg in segments {
        match seg {
            PathSegment::Key(k) => match cur {
                serde_json::Value::Object(map) => cur = map.get(k)?,
                _ => return None,
            },
            PathSegment::Index(n) => match cur {
                serde_json::Value::Array(arr) => cur = arr.get(*n)?,
                _ => return None,
            },
            PathSegment::IndexFromEnd(n) => match cur {
                serde_json::Value::Array(arr) => {
                    let len = arr.len();
                    if *n == 0 || *n > len {
                        return None;
                    }
                    cur = arr.get(len - *n)?;
                }
                _ => return None,
            },
        }
    }
    Some(cur)
}

/// Parse JSON accepting SQLite's relaxed (JSON5-ish) superset, mirroring the
/// behavior of `json()`, `json_extract()`, and `json_type()`.
fn parse_json_relaxed(s: &str) -> Result<serde_json::Value, ()> {
    serde_json::from_str(s)
        .or_else(|_| json5::from_str::<serde_json::Value>(s))
        .map_err(|_| ())
}

/// Convert an extracted JSON node into the SQL value SQLite would return from
/// `->>` or single-path `json_extract` (text unquoted, numbers native, booleans
/// as integers, JSON null as SQL NULL, containers as JSON text).
fn json_node_to_sql_value(value: &serde_json::Value) -> SqlValue {
    match value {
        serde_json::Value::Null => SqlValue::Null,
        serde_json::Value::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                SqlValue::Real(f)
            } else {
                SqlValue::Null
            }
        }
        serde_json::Value::String(s) => SqlValue::Varchar(s.as_str().into()),
        // Objects and arrays are returned as their minified JSON text.
        _ => SqlValue::Varchar(serde_json::to_string(value).unwrap_or_default().into()),
    }
}

/// Render an extracted JSON node as JSON text (used by `->` and by the
/// multi-path `json_extract` array form).
fn json_node_to_json_text(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

/// The SQLite JSON type name for a node.
fn json_node_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(true) => "true",
        serde_json::Value::Bool(false) => "false",
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "real"
            }
        }
        serde_json::Value::String(_) => "text",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Coerce the right-hand operand of `->` / `->>` into a JSON path.
///
/// Per <https://sqlite.org/json1.html#jptr>: an integer N is the array subscript
/// `$[N]`; a text value beginning with `$` is used verbatim; any other text is
/// treated as a single object label (`$."<text>"`).
fn arrow_operand_to_path(right: &SqlValue) -> Result<Vec<PathSegment>, ExecutorError> {
    match right {
        SqlValue::Integer(i)
        | SqlValue::Bigint(i) => {
            if *i >= 0 {
                Ok(vec![PathSegment::Index(*i as usize)])
            } else {
                // A negative subscript never matches (SQLite uses `$[#-n]`).
                Ok(vec![PathSegment::IndexFromEnd(usize::MAX)])
            }
        }
        SqlValue::Smallint(i) => {
            if *i >= 0 {
                Ok(vec![PathSegment::Index(*i as usize)])
            } else {
                Ok(vec![PathSegment::IndexFromEnd(usize::MAX)])
            }
        }
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            let s = s.as_str();
            if s.starts_with('$') {
                parse_sqlite_json_path(s).map_err(ExecutorError::SqliteCompatError)
            } else {
                Ok(vec![PathSegment::Key(s.to_string())])
            }
        }
        _ => Err(ExecutorError::SqliteCompatError("malformed JSON".to_string())),
    }
}

/// Evaluate the `->` (`as_text == false`) and `->>` (`as_text == true`)
/// operators. NULL operands are handled by the caller before this is reached.
pub(crate) fn eval_json_arrow(
    left: &SqlValue,
    right: &SqlValue,
    as_text: bool,
) -> Result<SqlValue, ExecutorError> {
    if matches!(left, SqlValue::Null) || matches!(right, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    let json_str = match left {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        _ => {
            return Err(ExecutorError::SqliteCompatError("malformed JSON".to_string()));
        }
    };

    let path = arrow_operand_to_path(right)?;

    let value = parse_json_relaxed(json_str)
        .map_err(|_| ExecutorError::SqliteCompatError("malformed JSON".to_string()))?;

    match navigate(&value, &path) {
        Some(node) => {
            if as_text {
                Ok(json_node_to_sql_value(node))
            } else {
                Ok(SqlValue::Varchar(json_node_to_json_text(node).into()))
            }
        }
        None => Ok(SqlValue::Null),
    }
}

/// json_valid(X) - return 1 if X is well-formed (strict RFC-8259) JSON, else 0.
///
/// A NULL argument returns NULL (matching modern SQLite; the legacy
/// `legacy_json_valid` build returned 0). The optional second flags argument
/// (SQLite 3.45+) is accepted but ignored for Phase 1. Note that unlike
/// `json()`/`json_extract()`, this uses strict JSON only, so JSON5 inputs
/// (e.g. `{a:5}`) validate as 0.
pub(crate) fn json_valid(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "json_valid".to_string(),
        });
    }

    let valid = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            serde_json::from_str::<serde_json::Value>(s.as_str()).is_ok()
        }
        // Numeric SQL values render to valid JSON scalars.
        SqlValue::Integer(_)
        | SqlValue::Smallint(_)
        | SqlValue::Bigint(_)
        | SqlValue::Unsigned(_)
        | SqlValue::Numeric(_)
        | SqlValue::Float(_)
        | SqlValue::Real(_)
        | SqlValue::Double(_) => true,
        // Blobs are not (text) JSON in this phase.
        _ => false,
    };

    Ok(SqlValue::Integer(if valid { 1 } else { 0 }))
}

/// json_extract(X, P, ...) - extract the value(s) at the given JSON path(s).
///
/// Single-path form returns the SQL value (text unquoted, numbers native,
/// booleans as integers, JSON null as SQL NULL, containers as JSON text).
/// Multi-path form returns a JSON array of the extracted nodes. A NULL document
/// or NULL path yields NULL; a non-existent path yields NULL; a syntactically
/// invalid path is an error.
pub(crate) fn json_extract(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "json_extract".to_string(),
        });
    }

    // NULL document propagates; single-argument form yields NULL.
    if matches!(args[0], SqlValue::Null) || args.len() == 1 {
        return Ok(SqlValue::Null);
    }

    let json_str = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        _ => {
            return Err(ExecutorError::SqliteCompatError("malformed JSON".to_string()));
        }
    };

    let value = parse_json_relaxed(json_str)
        .map_err(|_| ExecutorError::SqliteCompatError("malformed JSON".to_string()))?;

    let paths = &args[1..];

    // Resolve each path argument into a segment list (NULL path -> whole
    // result is NULL, matching SQLite).
    let mut resolved: Vec<Vec<PathSegment>> = Vec::with_capacity(paths.len());
    for p in paths {
        match p {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                resolved.push(
                    parse_sqlite_json_path(s.as_str())
                        .map_err(ExecutorError::SqliteCompatError)?,
                );
            }
            other => {
                // Non-text paths render to their text form for the error
                // message (e.g. integer 0 -> "bad JSON path: '0'").
                let text = sql_value_scalar_text(other);
                return Err(ExecutorError::SqliteCompatError(format!(
                    "bad JSON path: '{}'",
                    text
                )));
            }
        }
    }

    if resolved.len() == 1 {
        // Single-path form: SQL value.
        match navigate(&value, &resolved[0]) {
            Some(node) => Ok(json_node_to_sql_value(node)),
            None => Ok(SqlValue::Null),
        }
    } else {
        // Multi-path form: JSON array of extracted nodes.
        let elems: Vec<serde_json::Value> = resolved
            .iter()
            .map(|segs| navigate(&value, segs).cloned().unwrap_or(serde_json::Value::Null))
            .collect();
        let arr = serde_json::Value::Array(elems);
        Ok(SqlValue::Varchar(serde_json::to_string(&arr).unwrap_or_default().into()))
    }
}

/// json_type(X) / json_type(X, P) - the SQLite type name of a JSON value.
///
/// One argument reports the root type; two arguments evaluate the path first.
/// A NULL document or NULL path returns NULL; a non-existent path returns NULL;
/// malformed JSON is an error.
pub(crate) fn json_type(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "json_type".to_string(),
        });
    }

    let json_str = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        _ => {
            return Err(ExecutorError::SqliteCompatError("malformed JSON".to_string()));
        }
    };

    let value = parse_json_relaxed(json_str)
        .map_err(|_| ExecutorError::SqliteCompatError("malformed JSON".to_string()))?;

    let node = if args.len() == 2 {
        match &args[1] {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                let segs = parse_sqlite_json_path(s.as_str())
                    .map_err(ExecutorError::SqliteCompatError)?;
                match navigate(&value, &segs) {
                    Some(n) => n,
                    None => return Ok(SqlValue::Null),
                }
            }
            other => {
                let text = sql_value_scalar_text(other);
                return Err(ExecutorError::SqliteCompatError(format!(
                    "bad JSON path: '{}'",
                    text
                )));
            }
        }
    } else {
        &value
    };

    Ok(SqlValue::Varchar(json_node_type_name(node).into()))
}

/// json_quote(X) - render a SQL scalar as a JSON value.
///
/// Strings are double-quoted with interior characters escaped; numbers render
/// as-is; SQL NULL becomes the unquoted text `null`; BLOBs are an error.
pub(crate) fn json_quote(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "json_quote".to_string(),
        });
    }

    let rendered = match &args[0] {
        SqlValue::Null => "null".to_string(),
        SqlValue::Boolean(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        SqlValue::Integer(i) | SqlValue::Bigint(i) => i.to_string(),
        SqlValue::Smallint(i) => i.to_string(),
        SqlValue::Unsigned(u) => u.to_string(),
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => render_json_number(*f),
        SqlValue::Float(f) => render_json_number(*f as f64),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            serde_json::to_string(&serde_json::Value::String(s.as_str().to_string()))
                .unwrap_or_default()
        }
        SqlValue::Blob(_) => {
            return Err(ExecutorError::SqliteCompatError(
                "JSON cannot hold BLOB values".to_string(),
            ));
        }
        other => {
            // Fall back to a textual scalar rendering for remaining types.
            sql_value_scalar_text(other)
        }
    };

    Ok(SqlValue::Varchar(rendered.into()))
}

/// Render an f64 the way SQLite renders JSON reals (keeps a fractional part,
/// e.g. `2.0`), by round-tripping through serde_json's number formatter.
fn render_json_number(f: f64) -> String {
    match serde_json::Number::from_f64(f) {
        Some(n) => n.to_string(),
        None => f.to_string(),
    }
}

/// Best-effort scalar text rendering for a SQL value, used only for building
/// path-error messages and quoting exotic types.
fn sql_value_scalar_text(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "null".to_string(),
        SqlValue::Integer(i) | SqlValue::Bigint(i) => i.to_string(),
        SqlValue::Smallint(i) => i.to_string(),
        SqlValue::Unsigned(u) => u.to_string(),
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => f.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Boolean(b) => b.to_string(),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str().to_string(),
        _ => String::new(),
    }
}

/// json(X) - Validate and minify JSON
///
/// The json(X) function verifies that its argument X is a valid JSON string
/// and returns a minified version of that JSON string (with all unnecessary
/// whitespace removed). If X is not a well-formed JSON string, then this
/// function throws an error.
///
/// If the argument is NULL, returns NULL.
///
/// Reference: https://www.sqlite.org/json1.html#the_json_function
pub(crate) fn json(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::WrongNumberOfArguments { function_name: "json".to_string() });
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Parse the JSON to validate it. SQLite's json() accepts a relaxed
            // JSON5-like superset (unquoted object keys, single-quoted strings,
            // trailing commas, comments, etc.). Try strict serde_json first for
            // speed and exact behavior on canonical JSON, then fall back to a
            // JSON5 parser so inputs like `{a:3}` are accepted, matching SQLite.
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(s.as_str())
                .or_else(|_| json5::from_str::<serde_json::Value>(s.as_str()));
            match parsed {
                Ok(value) => {
                    // Re-serialize to minified, strict JSON (compact format)
                    let minified = serde_json::to_string(&value).map_err(|e| {
                        ExecutorError::SqliteCompatError(format!("malformed JSON: {}", e))
                    })?;
                    Ok(SqlValue::Varchar(minified.into()))
                }
                Err(_) => {
                    // SQLite returns "malformed JSON" for invalid JSON
                    Err(ExecutorError::SqliteCompatError("malformed JSON".to_string()))
                }
            }
        }
        // For non-string types, SQLite throws an error
        _ => Err(ExecutorError::SqliteCompatError(
            "JSON functions require string arguments".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_valid_array() {
        let result = json(&[SqlValue::Varchar("[1,2,3]".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("[1,2,3]".into()));
    }

    #[test]
    fn test_json_minifies_whitespace() {
        let result = json(&[SqlValue::Varchar("  { \"a\" : 1 }  ".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("{\"a\":1}".into()));
    }

    #[test]
    fn test_json_null_input() {
        let result = json(&[SqlValue::Null]).unwrap();
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_json_invalid_json() {
        let result = json(&[SqlValue::Varchar("{invalid}".into())]);
        assert!(result.is_err());
        if let Err(ExecutorError::SqliteCompatError(msg)) = result {
            assert_eq!(msg, "malformed JSON");
        } else {
            panic!("Expected SqliteCompatError");
        }
    }

    #[test]
    fn test_json_string_value() {
        let result = json(&[SqlValue::Varchar("\"hello\"".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("\"hello\"".into()));
    }

    #[test]
    fn test_json_number_value() {
        let result = json(&[SqlValue::Varchar("42".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("42".into()));
    }

    #[test]
    fn test_json_boolean_value() {
        let result = json(&[SqlValue::Varchar("true".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("true".into()));
    }

    #[test]
    fn test_json_null_json_value() {
        let result = json(&[SqlValue::Varchar("null".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("null".into()));
    }

    #[test]
    fn test_json_nested_object() {
        let input = r#"{"a": {"b": [1, 2, 3]}, "c": "test"}"#;
        let result = json(&[SqlValue::Varchar(input.into())]).unwrap();
        // serde_json preserves key order in minified output
        assert_eq!(result, SqlValue::Varchar(r#"{"a":{"b":[1,2,3]},"c":"test"}"#.into()));
    }

    #[test]
    fn test_json_wrong_arg_count() {
        // No arguments
        let result = json(&[]);
        assert!(result.is_err());

        // Too many arguments
        let result = json(&[SqlValue::Varchar("[]".into()), SqlValue::Varchar("[]".into())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_non_string_input() {
        let result = json(&[SqlValue::Integer(42)]);
        assert!(result.is_err());
    }

    // SQLite's json() accepts a relaxed JSON5-like syntax. These regression
    // tests cover the aggorderby-9.x cases (unquoted keys) plus the broader
    // JSON5 features, verifying we canonicalize back to strict minified JSON.
    #[test]
    fn test_json_json5_unquoted_key() {
        let result = json(&[SqlValue::Varchar("{a:3}".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar(r#"{"a":3}"#.into()));
    }

    #[test]
    fn test_json_json5_multiple_unquoted_keys() {
        let result = json(&[SqlValue::Varchar("{x:2, y:5}".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar(r#"{"x":2,"y":5}"#.into()));
    }

    #[test]
    fn test_json_json5_single_quoted_string() {
        let result = json(&[SqlValue::Varchar("{'k':'v'}".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar(r#"{"k":"v"}"#.into()));
    }

    #[test]
    fn test_json_json5_trailing_comma() {
        let result = json(&[SqlValue::Varchar("[1,2,3,]".into())]).unwrap();
        assert_eq!(result, SqlValue::Varchar("[1,2,3]".into()));
    }

    #[test]
    fn test_json_strict_json_still_rejects_garbage() {
        // Genuinely malformed input must still error even with JSON5 fallback.
        let result = json(&[SqlValue::Varchar("{not valid at all".into())]);
        assert!(result.is_err());
    }

    // ---- Path grammar -----------------------------------------------------

    #[test]
    fn test_parse_path_root() {
        assert_eq!(parse_sqlite_json_path("$").unwrap(), vec![]);
    }

    #[test]
    fn test_parse_path_members_and_indices() {
        assert_eq!(
            parse_sqlite_json_path("$.a.b[0].c").unwrap(),
            vec![
                PathSegment::Key("a".into()),
                PathSegment::Key("b".into()),
                PathSegment::Index(0),
                PathSegment::Key("c".into()),
            ]
        );
    }

    #[test]
    fn test_parse_path_quoted_key_with_dots() {
        assert_eq!(
            parse_sqlite_json_path(r#"$."tris.legomenon"."summary.report""#).unwrap(),
            vec![
                PathSegment::Key("tris.legomenon".into()),
                PathSegment::Key("summary.report".into()),
            ]
        );
    }

    #[test]
    fn test_parse_path_empty_quoted_key() {
        assert_eq!(
            parse_sqlite_json_path(r#"$.""[1]"#).unwrap(),
            vec![PathSegment::Key("".into()), PathSegment::Index(1)]
        );
    }

    #[test]
    fn test_parse_path_from_end() {
        assert_eq!(
            parse_sqlite_json_path("$[#-1]").unwrap(),
            vec![PathSegment::IndexFromEnd(1)]
        );
    }

    #[test]
    fn test_parse_path_errors() {
        // Must start with '$'
        assert_eq!(parse_sqlite_json_path("a").unwrap_err(), "bad JSON path: 'a'");
        assert_eq!(parse_sqlite_json_path(".a").unwrap_err(), "bad JSON path: '.a'");
        // Trailing '.' with no key is a bad path (json101-18.5)
        assert_eq!(parse_sqlite_json_path("$.").unwrap_err(), "bad JSON path: '$.'");
    }

    // ---- json_valid -------------------------------------------------------

    #[test]
    fn test_json_valid_basic() {
        assert_eq!(json_valid(&[SqlValue::Varchar(r#"{"a":1}"#.into())]).unwrap(), SqlValue::Integer(1));
        assert_eq!(json_valid(&[SqlValue::Varchar("bad".into())]).unwrap(), SqlValue::Integer(0));
        // Whitespace tolerated; empty is invalid
        assert_eq!(json_valid(&[SqlValue::Varchar("  123 ".into())]).unwrap(), SqlValue::Integer(1));
        assert_eq!(json_valid(&[SqlValue::Varchar("".into())]).unwrap(), SqlValue::Integer(0));
    }

    #[test]
    fn test_json_valid_json5_is_invalid() {
        // Unlike json()/json_extract(), json_valid() is strict RFC-8259.
        assert_eq!(json_valid(&[SqlValue::Varchar("{a:5}".into())]).unwrap(), SqlValue::Integer(0));
    }

    #[test]
    fn test_json_valid_null_and_numbers() {
        // Modern SQLite: NULL -> NULL
        assert_eq!(json_valid(&[SqlValue::Null]).unwrap(), SqlValue::Null);
        assert_eq!(json_valid(&[SqlValue::Integer(123)]).unwrap(), SqlValue::Integer(1));
        assert_eq!(json_valid(&[SqlValue::Real(1.5)]).unwrap(), SqlValue::Integer(1));
    }

    #[test]
    fn test_json_valid_ignores_flags_arg() {
        assert_eq!(
            json_valid(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Integer(5)]).unwrap(),
            SqlValue::Integer(1)
        );
    }

    // ---- json_extract -----------------------------------------------------

    #[test]
    fn test_json_extract_single_scalar_types() {
        // integer stays integral
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Varchar("$.a".into())]).unwrap(),
            SqlValue::Integer(1)
        );
        // real
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":1.5}"#.into()), SqlValue::Varchar("$.a".into())]).unwrap(),
            SqlValue::Real(1.5)
        );
        // text unquoted
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":"hello"}"#.into()), SqlValue::Varchar("$.a".into())]).unwrap(),
            SqlValue::Varchar("hello".into())
        );
        // boolean -> integer 1/0
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":true}"#.into()), SqlValue::Varchar("$.a".into())]).unwrap(),
            SqlValue::Integer(1)
        );
        // JSON null -> SQL NULL
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":null}"#.into()), SqlValue::Varchar("$.a".into())]).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_json_extract_container_returns_json_text() {
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Varchar("$".into())]).unwrap(),
            SqlValue::Varchar(r#"{"a":1}"#.into())
        );
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":[1,2]}"#.into()), SqlValue::Varchar("$.a".into())]).unwrap(),
            SqlValue::Varchar("[1,2]".into())
        );
    }

    #[test]
    fn test_json_extract_array_index_and_from_end() {
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":[1,2,3]}"#.into()), SqlValue::Varchar("$.a[1]".into())]).unwrap(),
            SqlValue::Integer(2)
        );
        assert_eq!(
            json_extract(&[SqlValue::Varchar("[1,2,3]".into()), SqlValue::Varchar("$[#-1]".into())]).unwrap(),
            SqlValue::Integer(3)
        );
    }

    #[test]
    fn test_json_extract_missing_path_is_null() {
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Varchar("$.x".into())]).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_json_extract_multi_path_returns_array() {
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"{"a":1}"#.into()),
                SqlValue::Varchar("$.a".into()),
                SqlValue::Varchar("$.b".into()),
            ]).unwrap(),
            SqlValue::Varchar("[1,null]".into())
        );
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"{"a":"x","b":"y"}"#.into()),
                SqlValue::Varchar("$.a".into()),
                SqlValue::Varchar("$.b".into()),
            ]).unwrap(),
            SqlValue::Varchar(r#"["x","y"]"#.into())
        );
    }

    #[test]
    fn test_json_extract_null_and_single_arg() {
        assert_eq!(json_extract(&[SqlValue::Null]).unwrap(), SqlValue::Null);
        // Single non-null argument yields NULL (matches SQLite).
        assert_eq!(json_extract(&[SqlValue::Varchar(r#"{"a":1}"#.into())]).unwrap(), SqlValue::Null);
        // NULL path -> NULL
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Null]).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_json_extract_errors() {
        // Bare key (no '$') is a bad path
        let e = json_extract(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Varchar("a".into())]);
        assert!(matches!(e, Err(ExecutorError::SqliteCompatError(ref m)) if m == "bad JSON path: 'a'"));
        // Malformed JSON document is an error
        let e = json_extract(&[SqlValue::Varchar("{bad".into()), SqlValue::Varchar("$.a".into())]);
        assert!(matches!(e, Err(ExecutorError::SqliteCompatError(ref m)) if m == "malformed JSON"));
    }

    #[test]
    fn test_json_extract_quoted_and_empty_keys() {
        // json101-18.2 / 18.3
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"":5}"#.into()), SqlValue::Varchar(r#"$."""#.into())]).unwrap(),
            SqlValue::Integer(5)
        );
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"[3,{"a":4,"":[5,{"hi":6},7]},8]"#.into()),
                SqlValue::Varchar(r#"$[1].""[1].hi"#.into()),
            ]).unwrap(),
            SqlValue::Integer(6)
        );
    }

    // ---- json_type --------------------------------------------------------

    #[test]
    fn test_json_type_root() {
        let cases = [
            ("null", "null"),
            ("true", "true"),
            ("false", "false"),
            ("123", "integer"),
            ("1.5", "real"),
            (r#""x""#, "text"),
            ("[1,2]", "array"),
            (r#"{"a":1}"#, "object"),
        ];
        for (input, expected) in cases {
            assert_eq!(
                json_type(&[SqlValue::Varchar(input.into())]).unwrap(),
                SqlValue::Varchar(expected.into()),
                "json_type({input})"
            );
        }
    }

    #[test]
    fn test_json_type_with_path() {
        assert_eq!(
            json_type(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Varchar("$.a".into())]).unwrap(),
            SqlValue::Varchar("integer".into())
        );
        // non-existent path -> NULL
        assert_eq!(
            json_type(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Varchar("$.x".into())]).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_json_type_null_handling() {
        assert_eq!(json_type(&[SqlValue::Null]).unwrap(), SqlValue::Null);
        // NULL path -> NULL (json101-21.22)
        assert_eq!(
            json_type(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Null]).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_json_type_malformed_errors() {
        assert!(json_type(&[SqlValue::Varchar("{bad".into())]).is_err());
    }

    // ---- json_quote -------------------------------------------------------

    #[test]
    fn test_json_quote_values() {
        assert_eq!(
            json_quote(&[SqlValue::Varchar("hello".into())]).unwrap(),
            SqlValue::Varchar(r#""hello""#.into())
        );
        assert_eq!(
            json_quote(&[SqlValue::Varchar(r#"abc"xyz"#.into())]).unwrap(),
            SqlValue::Varchar(r#""abc\"xyz""#.into())
        );
        assert_eq!(json_quote(&[SqlValue::Integer(12345)]).unwrap(), SqlValue::Varchar("12345".into()));
        assert_eq!(json_quote(&[SqlValue::Real(3.14159)]).unwrap(), SqlValue::Varchar("3.14159".into()));
        // Real keeps a fractional part, matching SQLite (json_quote(2.0) -> 2.0)
        assert_eq!(json_quote(&[SqlValue::Real(2.0)]).unwrap(), SqlValue::Varchar("2.0".into()));
        // NULL -> unquoted "null"
        assert_eq!(json_quote(&[SqlValue::Null]).unwrap(), SqlValue::Varchar("null".into()));
    }

    #[test]
    fn test_json_quote_blob_errors() {
        let e = json_quote(&[SqlValue::Blob(vec![0x30, 0x31])]);
        assert!(matches!(e, Err(ExecutorError::SqliteCompatError(ref m)) if m == "JSON cannot hold BLOB values"));
    }

    #[test]
    fn test_json_quote_arg_count() {
        assert!(json_quote(&[]).is_err());
        assert!(json_quote(&[SqlValue::Integer(1), SqlValue::Integer(2)]).is_err());
    }

    // ---- -> and ->> operators --------------------------------------------

    #[test]
    fn test_arrow_json_text_vs_sql_value() {
        // -> returns JSON text
        assert_eq!(
            eval_json_arrow(&SqlValue::Varchar(r#"{"a":1}"#.into()), &SqlValue::Varchar("$.a".into()), false).unwrap(),
            SqlValue::Varchar("1".into())
        );
        // ->> returns SQL value (integer)
        assert_eq!(
            eval_json_arrow(&SqlValue::Varchar(r#"{"a":1}"#.into()), &SqlValue::Varchar("$.a".into()), true).unwrap(),
            SqlValue::Integer(1)
        );
        // ->> on text yields unquoted string
        assert_eq!(
            eval_json_arrow(&SqlValue::Varchar(r#"{"a":"hello"}"#.into()), &SqlValue::Varchar("$.a".into()), true).unwrap(),
            SqlValue::Varchar("hello".into())
        );
    }

    #[test]
    fn test_arrow_bare_label_and_integer_shorthand() {
        // Bare text label -> $.<label>
        assert_eq!(
            eval_json_arrow(&SqlValue::Varchar(r#"{"a":1}"#.into()), &SqlValue::Varchar("a".into()), false).unwrap(),
            SqlValue::Varchar("1".into())
        );
        // Integer shorthand -> $[N]
        assert_eq!(
            eval_json_arrow(&SqlValue::Varchar("[1,2,3]".into()), &SqlValue::Integer(1), true).unwrap(),
            SqlValue::Integer(2)
        );
    }

    #[test]
    fn test_arrow_null_and_missing() {
        // Non-existent path -> NULL for both forms
        assert_eq!(
            eval_json_arrow(&SqlValue::Varchar(r#"{"a":1}"#.into()), &SqlValue::Varchar("b".into()), false).unwrap(),
            SqlValue::Null
        );
        // JSON null: -> yields text "null", ->> yields SQL NULL
        assert_eq!(
            eval_json_arrow(&SqlValue::Varchar(r#"{"a":null}"#.into()), &SqlValue::Varchar("$.a".into()), false).unwrap(),
            SqlValue::Varchar("null".into())
        );
        assert_eq!(
            eval_json_arrow(&SqlValue::Varchar(r#"{"a":null}"#.into()), &SqlValue::Varchar("$.a".into()), true).unwrap(),
            SqlValue::Null
        );
        // NULL operands propagate
        assert_eq!(
            eval_json_arrow(&SqlValue::Null, &SqlValue::Integer(0), false).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            eval_json_arrow(&SqlValue::Varchar(r#"{"a":1}"#.into()), &SqlValue::Null, false).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_arrow_malformed_errors() {
        let e = eval_json_arrow(&SqlValue::Varchar("{bad".into()), &SqlValue::Varchar("$.a".into()), false);
        assert!(matches!(e, Err(ExecutorError::SqliteCompatError(ref m)) if m == "malformed JSON"));
    }
}
