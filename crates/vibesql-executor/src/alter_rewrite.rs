//! In-place edits to the verbatim `CREATE TABLE` text stored in
//! `TableSchema::sql_source`, applied on `ALTER TABLE`.
//!
//! SQLite does not reconstruct `sqlite_master.sql` from the parsed schema after
//! an ALTER — it edits the *original* `CREATE TABLE` statement text in place,
//! preserving the user's whitespace and formatting everywhere except the parts
//! the ALTER touches (verified against sqlite3 3.51.0):
//!
//! - `ALTER TABLE t ADD COLUMN c INTEGER`
//!   appends `, c INTEGER` immediately before the closing `)` of the column
//!   list, leaving the rest of the text byte-for-byte unchanged.
//! - `ALTER TABLE t RENAME TO t2`
//!   rewrites the table name to the double-quoted new name (`"t2"`), preserving
//!   everything else.
//! - `ALTER TABLE t RENAME COLUMN b TO bb`
//!   rewrites the column name in its definition position, preserving everything
//!   else.
//!
//! This module implements those three in-place edits at the token level (using
//! the parser's lexer for span tracking, the same approach as
//! `crate::trigger_rename`). DROP COLUMN and the type/constraint-changing ALTER
//! variants are deliberately out of scope here: they fall back to invalidating
//! `sql_source` and reconstructing from the (now-synced) catalog schema, which
//! is correct, just lower fidelity. See issue #5625.
//!
//! Every function returns `Option<String>`: `Some(edited)` only when the edit
//! is unambiguous and the structure matches expectations; otherwise `None`, so
//! the caller falls back to invalidate-and-reconstruct. The returned text is
//! always still a valid `CREATE TABLE` statement (the edits only insert a
//! comma-separated column def or swap one identifier), so it remains
//! re-parseable on reload — the staleness-safety invariant from issue #5619.

use vibesql_parser::{Keyword, Lexer, Span, Token};

/// Tokenize `sql`, dropping the trailing `Eof`. Returns `None` if the text
/// cannot be tokenized (it always should, since it round-tripped the parser).
fn tokenize(sql: &str) -> Option<Vec<(Token, Span)>> {
    let mut tokens = Lexer::new(sql).tokenize_with_spans().ok()?;
    if matches!(tokens.last(), Some((Token::Eof, _))) {
        tokens.pop();
    }
    Some(tokens)
}

/// Index of the `(` token that opens the column-definition list of a
/// `CREATE TABLE` statement, and the matching closing `)`.
///
/// The opening paren is the first top-level `(` after the table name. Returns
/// `None` if no balanced top-level paren pair is found (e.g. `CREATE TABLE t AS
/// SELECT ...`, which has no column list to edit).
fn column_list_parens(tokens: &[(Token, Span)]) -> Option<(usize, usize)> {
    let open = tokens.iter().position(|(t, _)| matches!(t, Token::LParen))?;
    let mut depth = 0usize;
    for (i, (tok, _)) in tokens.iter().enumerate().skip(open) {
        match tok {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Some((open, i));
                }
            }
            _ => {}
        }
    }
    None
}

/// Append a column definition to the verbatim `CREATE TABLE` text, matching
/// SQLite's `ALTER TABLE ... ADD COLUMN`: insert `, <coldef>` immediately
/// before the closing `)` of the column list, preserving all other formatting.
///
/// `coldef` is the verbatim column-definition text from the ALTER statement
/// (e.g. `c INTEGER`, `d TEXT DEFAULT 'x'`). Returns `None` when the text has no
/// editable column list (the caller then reconstructs instead).
pub fn append_column(create_sql: &str, coldef: &str) -> Option<String> {
    let coldef = coldef.trim();
    if coldef.is_empty() {
        return None;
    }
    let tokens = tokenize(create_sql)?;
    let (_open, close) = column_list_parens(&tokens)?;
    let insert_at = tokens[close].1.start;

    let mut out = String::with_capacity(create_sql.len() + coldef.len() + 2);
    out.push_str(&create_sql[..insert_at]);
    out.push_str(", ");
    out.push_str(coldef);
    out.push_str(&create_sql[insert_at..]);
    Some(out)
}

/// Whether `tok` is an identifier-like token whose text matches `name`
/// (case-insensitively for bare identifiers; delimited identifiers are also
/// matched case-insensitively, since SQLite folds the ALTER target name).
fn ident_matches(tok: &Token, name: &str) -> bool {
    match tok {
        Token::Identifier(s) | Token::DelimitedIdentifier(s) => s.eq_ignore_ascii_case(name),
        _ => false,
    }
}

/// Rewrite the table name in the verbatim `CREATE TABLE` text to the
/// double-quoted `new_name`, matching SQLite's `ALTER TABLE ... RENAME TO`
/// (which quotes the new name and preserves all other formatting).
///
/// The table name is the first identifier token after
/// `CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS]`. Returns `None` if it cannot
/// be located.
///
/// NOTE: not yet wired into RENAME TO — that variant currently invalidates and
/// reconstructs (issue #5625 follow-on) because the preserved verbatim text for
/// a renamed table interacts with a pre-existing SQL-dump reload gap for quoted
/// identifiers containing `'`. Retained (and unit-tested) so the follow-on can
/// enable it once the dump/reload path handles such identifiers.
#[allow(dead_code)]
pub fn rename_table(create_sql: &str, new_name: &str) -> Option<String> {
    let tokens = tokenize(create_sql)?;
    let name_idx = table_name_index(&tokens)?;
    let span = tokens[name_idx].1;
    Some(replace_span(create_sql, span, &quote_ident(new_name)))
}

/// Index of the table-name identifier token in a `CREATE TABLE` statement.
fn table_name_index(tokens: &[(Token, Span)]) -> Option<usize> {
    let mut i = 0;
    // CREATE
    if !matches!(tokens.get(i), Some((Token::Keyword { keyword: Keyword::Create, .. }, _))) {
        return None;
    }
    i += 1;
    // optional TEMP / TEMPORARY
    if matches!(
        tokens.get(i),
        Some((Token::Keyword { keyword: Keyword::Temp | Keyword::Temporary, .. }, _))
    ) {
        i += 1;
    }
    // TABLE
    if !matches!(tokens.get(i), Some((Token::Keyword { keyword: Keyword::Table, .. }, _))) {
        return None;
    }
    i += 1;
    // optional IF NOT EXISTS
    if matches!(tokens.get(i), Some((Token::Keyword { keyword: Keyword::If, .. }, _)))
        && matches!(tokens.get(i + 1), Some((Token::Keyword { keyword: Keyword::Not, .. }, _)))
        && matches!(tokens.get(i + 2), Some((Token::Keyword { keyword: Keyword::Exists, .. }, _)))
    {
        i += 3;
    }
    // table name (may be schema-qualified: skip `schema .`)
    match tokens.get(i) {
        Some((Token::Identifier(_) | Token::DelimitedIdentifier(_), _)) => {}
        _ => return None,
    }
    if matches!(tokens.get(i + 1), Some((Token::Symbol('.'), _))) {
        i += 2; // skip `schema .`, the real name follows
        if !matches!(
            tokens.get(i),
            Some((Token::Identifier(_) | Token::DelimitedIdentifier(_), _))
        ) {
            return None;
        }
    }
    Some(i)
}

/// Rewrite a column name in its *definition position* within the verbatim
/// `CREATE TABLE` text, matching SQLite's `ALTER TABLE ... RENAME COLUMN`.
///
/// Only the identifier that names the column at the start of a column definition
/// (the first token of the column list, or the first token after a top-level
/// comma inside the column list) is rewritten — references elsewhere (e.g. in a
/// table-level constraint) are conservatively left untouched. Returns `None`
/// when the definition cannot be located unambiguously, so the caller falls back
/// to reconstruction. `new_name` is emitted bare when it is a safe identifier,
/// otherwise double-quoted.
pub fn rename_column(create_sql: &str, old_col: &str, new_col: &str) -> Option<String> {
    let tokens = tokenize(create_sql)?;
    let (open, close) = column_list_parens(&tokens)?;

    // Walk the column list at paren depth 1, tracking the start of each
    // definition (top-level comma boundaries).
    let mut depth = 0usize;
    let mut at_def_start = false;
    let mut target: Option<usize> = None;
    for (i, (tok, _)) in tokens.iter().enumerate().take(close).skip(open) {
        match tok {
            Token::LParen => {
                if depth == 1 {
                    at_def_start = false;
                }
                depth += 1;
                if depth == 1 {
                    at_def_start = true; // first token inside the column list
                }
            }
            Token::RParen => depth -= 1,
            Token::Comma if depth == 1 => at_def_start = true,
            _ if depth == 1 => {
                if at_def_start {
                    at_def_start = false;
                    if ident_matches(tok, old_col) {
                        if target.is_some() {
                            // Two definitions match: ambiguous; bail.
                            return None;
                        }
                        target = Some(i);
                    }
                }
            }
            _ => {}
        }
    }

    let idx = target?;
    let span = tokens[idx].1;
    let replacement = if is_safe_bare_identifier(new_col) {
        new_col.to_string()
    } else {
        quote_ident(new_col)
    };
    Some(replace_span(create_sql, span, &replacement))
}

/// Extract the verbatim column-definition text from an
/// `ALTER TABLE ... ADD [COLUMN] <coldef>` statement, so it can be appended to
/// the stored `CREATE TABLE` text exactly as the user typed it (matching
/// SQLite, which appends the original ALTER column text byte-for-byte).
///
/// Returns the substring from the first token after `ADD [COLUMN]` through the
/// last non-`;` token, trimmed. Returns `None` if the `ADD` clause cannot be
/// located.
pub fn extract_add_column_text(alter_sql: &str) -> Option<String> {
    let tokens = tokenize(alter_sql)?;
    let add_pos =
        tokens.iter().position(|(t, _)| matches!(t, Token::Keyword { keyword: Keyword::Add, .. }))?;
    let mut start_tok = add_pos + 1;
    // Optional COLUMN keyword.
    if matches!(
        tokens.get(start_tok),
        Some((Token::Keyword { keyword: Keyword::Column, .. }, _))
    ) {
        start_tok += 1;
    }
    let first = tokens.get(start_tok)?;
    let start = first.1.start;
    // Find the end: last token that is not a trailing semicolon.
    let mut end = first.1.end;
    for (tok, span) in tokens.iter().skip(start_tok) {
        if matches!(tok, Token::Semicolon) {
            break;
        }
        end = span.end;
    }
    let text = alter_sql.get(start..end)?.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

/// Replace the byte range `span` in `sql` with `replacement`.
fn replace_span(sql: &str, span: Span, replacement: &str) -> String {
    let mut out = String::with_capacity(sql.len() + replacement.len());
    out.push_str(&sql[..span.start]);
    out.push_str(replacement);
    out.push_str(&sql[span.end..]);
    out
}

/// Double-quote an identifier (SQLite escapes embedded `"` by doubling).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Whether `name` can be emitted as a bare (unquoted) identifier: ASCII
/// alphanumeric/underscore, not starting with a digit, and non-empty. Used so a
/// plain column rename produces `bb` rather than `"bb"`.
fn is_safe_bare_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_column_matches_sqlite() {
        let sql = "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n)";
        let out = append_column(sql, "c INTEGER").unwrap();
        assert_eq!(out, "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n, c INTEGER)");
    }

    #[test]
    fn append_column_preserves_default_text() {
        let sql = "CREATE TABLE t (a INTEGER)";
        let out = append_column(sql, "d TEXT DEFAULT 'x'").unwrap();
        assert_eq!(out, "CREATE TABLE t (a INTEGER, d TEXT DEFAULT 'x')");
    }

    #[test]
    fn append_column_no_column_list_returns_none() {
        // CREATE TABLE ... AS SELECT has no editable column list.
        assert!(append_column("CREATE TABLE t AS SELECT 1", "c INTEGER").is_none());
    }

    #[test]
    fn rename_table_quotes_new_name() {
        let sql = "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n)";
        let out = rename_table(sql, "t2").unwrap();
        assert_eq!(out, "CREATE TABLE \"t2\" (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n)");
    }

    #[test]
    fn rename_table_handles_quoted_original() {
        let sql = "CREATE TABLE \"My Table\" (x int)";
        let out = rename_table(sql, "t3").unwrap();
        assert_eq!(out, "CREATE TABLE \"t3\" (x int)");
    }

    #[test]
    fn rename_table_if_not_exists() {
        let sql = "CREATE TABLE IF NOT EXISTS t (x int)";
        let out = rename_table(sql, "t2").unwrap();
        assert_eq!(out, "CREATE TABLE IF NOT EXISTS \"t2\" (x int)");
    }

    #[test]
    fn rename_column_in_definition_position() {
        let sql = "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n)";
        let out = rename_column(sql, "b", "bb").unwrap();
        assert_eq!(out, "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  bb   TEXT\n)");
    }

    #[test]
    fn rename_column_first_column() {
        let sql = "CREATE TABLE t (a INTEGER, b TEXT)";
        let out = rename_column(sql, "a", "aa").unwrap();
        assert_eq!(out, "CREATE TABLE t (aa INTEGER, b TEXT)");
    }

    #[test]
    fn rename_column_quotes_unsafe_name() {
        let sql = "CREATE TABLE t (a INTEGER, b TEXT)";
        let out = rename_column(sql, "b", "new col").unwrap();
        assert_eq!(out, "CREATE TABLE t (a INTEGER, \"new col\" TEXT)");
    }

    #[test]
    fn rename_column_missing_returns_none() {
        let sql = "CREATE TABLE t (a INTEGER, b TEXT)";
        assert!(rename_column(sql, "zzz", "qqq").is_none());
    }

    #[test]
    fn extract_add_column_text_with_column_keyword() {
        assert_eq!(
            extract_add_column_text("ALTER TABLE t ADD COLUMN c INTEGER").as_deref(),
            Some("c INTEGER")
        );
    }

    #[test]
    fn extract_add_column_text_without_column_keyword() {
        assert_eq!(
            extract_add_column_text("ALTER TABLE t ADD c INTEGER").as_deref(),
            Some("c INTEGER")
        );
    }

    #[test]
    fn extract_add_column_text_preserves_default_and_strips_semicolon() {
        assert_eq!(
            extract_add_column_text("ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'x';").as_deref(),
            Some("d TEXT DEFAULT 'x'")
        );
    }

    #[test]
    fn append_then_extract_matches_sqlite_end_to_end() {
        // ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'x' on the post-ADD-c text.
        let create = "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n, c INTEGER)";
        let coldef = extract_add_column_text("ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'x'").unwrap();
        let out = append_column(create, &coldef).unwrap();
        assert_eq!(
            out,
            "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n, c INTEGER, d TEXT DEFAULT 'x')"
        );
    }
}
