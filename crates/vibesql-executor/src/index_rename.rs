//! Verbatim `CREATE INDEX` source text: capture-time normalization and in-place
//! edits applied on `ALTER TABLE ... RENAME TO` / `RENAME COLUMN` (issue #6734).
//!
//! SQLite stores an index's `CREATE INDEX` statement in `sqlite_master.sql`
//! essentially byte-for-byte, and on an ALTER TABLE rename it splices only the
//! renamed identifiers into that text, preserving the user's whitespace,
//! redundant parentheses, and quoting everywhere else (verified against sqlite3
//! 3.54.0):
//!
//! | Statements                                                 | `sqlite_master.sql`                              |
//! |------------------------------------------------------------|--------------------------------------------------|
//! | `CREATE INDEX i3 ON  t1 ( (c0 * 2) , c0 DESC )`            | `CREATE INDEX i3 ON  t1 ( (c0 * 2) , c0 DESC )`  |
//! | `CREATE INDEX i0 ON t0(c0)` + `ALTER TABLE t0 RENAME TO t1`| `CREATE INDEX i0 ON "t1"(c0)`                    |
//! | `CREATE INDEX i2 ON t2((f(c0)))` + `RENAME COLUMN c0 TO c1`| `CREATE INDEX i2 ON t2((f(c1)))`                 |
//!
//! This module is the index-side counterpart of `crate::alter_rewrite` (which
//! edits a table's verbatim `CREATE TABLE` text) and reuses its token-level,
//! lexer-span-based approach and identifier helpers.
//!
//! Every function returns `Option<String>`: `Some(text)` only when the edit is
//! unambiguous and the result still parses as a `CREATE INDEX` statement;
//! otherwise `None`, so the caller invalidates the stored text and
//! `sqlite_master` falls back to reconstructing it from the (always
//! up-to-date) parsed index metadata. The stored text therefore never goes
//! stale — it is either spliced correctly or dropped.

use vibesql_ast::pretty_print::ToSql;
use vibesql_parser::{Keyword, Span, Token};

use crate::alter_rewrite::{
    emit_renamed_ident, ident_matches, is_quoted_ident, quote_ident, tokenize,
};

/// Whether `tok` can occupy an object-name (`nm`) position: a bare or delimited
/// identifier, or a single-quoted string (SQLite's `nm ::= id | STRING` rule).
/// A keyword token is accepted too, since the lexer classifies non-reserved
/// words (e.g. `key`, `value`) as keywords even where they name an object.
fn is_name_token(tok: &Token) -> bool {
    matches!(
        tok,
        Token::Identifier(_)
            | Token::DelimitedIdentifier(_)
            | Token::String(_)
            | Token::Keyword { .. }
    )
}

/// Locate the structural positions of a `CREATE INDEX` statement:
/// `CREATE [UNIQUE] INDEX [IF NOT EXISTS] [schema.]name ON [schema.]table (`.
///
/// Returns `(unique, name_idx, table_idx)` — whether `UNIQUE` was present, the
/// token index of the (unqualified) index name, and the token index of the
/// (unqualified) table name. The token after `table_idx` is guaranteed to be
/// the `(` opening the indexed-column list. Returns `None` for any other shape
/// (e.g. `CREATE SPATIAL INDEX`), so callers never edit text they do not fully
/// understand.
fn index_header(tokens: &[(Token, Span)]) -> Option<(bool, usize, usize)> {
    let kw = |i: usize, k: Keyword| matches!(tokens.get(i), Some((Token::Keyword { keyword, .. }, _)) if *keyword == k);
    let mut i = 0;
    if !kw(i, Keyword::Create) {
        return None;
    }
    i += 1;
    let unique = kw(i, Keyword::Unique);
    if unique {
        i += 1;
    }
    if !kw(i, Keyword::Index) {
        return None;
    }
    i += 1;
    if kw(i, Keyword::If) && kw(i + 1, Keyword::Not) && kw(i + 2, Keyword::Exists) {
        i += 3;
    }
    // Index name, optionally `schema .`-qualified.
    if !tokens.get(i).is_some_and(|(t, _)| is_name_token(t)) {
        return None;
    }
    if matches!(tokens.get(i + 1), Some((Token::Symbol('.'), _))) {
        i += 2;
        if !tokens.get(i).is_some_and(|(t, _)| is_name_token(t)) {
            return None;
        }
    }
    let name_idx = i;
    i += 1;
    if !kw(i, Keyword::On) {
        return None;
    }
    i += 1;
    // Table name, optionally `schema .`-qualified.
    if !tokens.get(i).is_some_and(|(t, _)| is_name_token(t)) {
        return None;
    }
    if matches!(tokens.get(i + 1), Some((Token::Symbol('.'), _))) {
        i += 2;
        if !tokens.get(i).is_some_and(|(t, _)| is_name_token(t)) {
            return None;
        }
    }
    let table_idx = i;
    if !matches!(tokens.get(table_idx + 1), Some((Token::LParen, _))) {
        return None;
    }
    Some((unique, name_idx, table_idx))
}

/// Whether `sql` still parses as a `CREATE INDEX` statement.
fn parses_as_create_index(sql: &str) -> Option<vibesql_ast::CreateIndexStmt> {
    match vibesql_parser::Parser::parse_sql(sql).ok()? {
        vibesql_ast::Statement::CreateIndex(stmt) => Some(stmt),
        _ => None,
    }
}

/// Normalize the text of a user-issued `CREATE INDEX` statement into the form
/// SQLite records in `sqlite_master.sql`.
///
/// SQLite's `sqlite3CreateIndex` builds the stored text as
/// `"CREATE%s INDEX %.*s"` — `" UNIQUE"` or nothing, followed by the original
/// source *from the (unqualified) index-name token through the last token of
/// the statement*. So everything after the index name is byte-for-byte
/// verbatim, while the prefix is canonicalized: `IF NOT EXISTS` and any
/// `schema.` qualifier on the index name are dropped, keyword case and spacing
/// between `CREATE`/`UNIQUE`/`INDEX` are normalized, and a trailing `;` (and
/// any whitespace or comment after the last token) is not recorded.
///
/// Returns `None` when `sql` is not a plain `CREATE [UNIQUE] INDEX` statement
/// (e.g. a spatial/vector index), in which case no source text is captured and
/// `sqlite_master` keeps reconstructing it.
pub(crate) fn normalize_create_index_source(sql: &str) -> Option<String> {
    let tokens = tokenize(sql)?;
    let (unique, name_idx, _) = index_header(&tokens)?;
    // The statement ends at its first `;` (a caller may hand over text that
    // continues with further statements) or at the last token.
    let stmt_end =
        tokens.iter().position(|(t, _)| matches!(t, Token::Semicolon)).unwrap_or(tokens.len());
    let last = tokens[..stmt_end].last()?;
    let start = tokens[name_idx].1.start;
    let end = last.1.end;
    if end <= start {
        return None;
    }
    Some(format!("CREATE{} INDEX {}", if unique { " UNIQUE" } else { "" }, &sql[start..end]))
}

/// Rewrite the target-table name in a verbatim `CREATE INDEX` text to the
/// double-quoted `new_table`, matching SQLite's `ALTER TABLE ... RENAME TO`
/// (`CREATE INDEX i0 ON t0(c0)` becomes `CREATE INDEX i0 ON "t1"(c0)`,
/// altertab3.test 8.1).
///
/// Table-qualified column references inside the indexed expressions or the
/// partial-index `WHERE` clause (`t0.c0`) are retargeted the same way
/// (`"t1".c0`), mirroring `alter_rewrite::rename_table_self_qualifiers`.
/// Everything else — the index name, spacing, parentheses, other quoting — is
/// preserved byte-for-byte.
///
/// Returns `None` if the text cannot be understood or the edited text no
/// longer parses; the caller then invalidates the stored source.
pub(crate) fn rename_index_table(
    index_sql: &str,
    old_table: &str,
    new_table: &str,
) -> Option<String> {
    let tokens = tokenize(index_sql)?;
    let (_, _, table_idx) = index_header(&tokens)?;
    let replacement = quote_ident(new_table);

    let mut spans: Vec<Span> = vec![tokens[table_idx].1];
    for (i, (tok, span)) in tokens.iter().enumerate().skip(table_idx + 1) {
        if matches!(tok, Token::Identifier(_) | Token::DelimitedIdentifier(_))
            && ident_matches(tok, old_table)
            && matches!(tokens.get(i + 1), Some((Token::Symbol('.'), _)))
        {
            spans.push(*span);
        }
    }

    let mut out = index_sql.to_string();
    for span in spans.iter().rev() {
        out.replace_range(span.start..span.end, &replacement);
    }
    parses_as_create_index(&out)?;
    Some(out)
}

/// Normalized fingerprint of one indexed column, used to verify that a
/// text-level rename produced exactly the metadata-level rename.
///
/// Identifier quoting and case are erased (`"C1"` and `c1` compare equal), so
/// the comparison checks *which* columns are referenced, not how they are
/// spelled.
fn fingerprint(text: &str) -> String {
    text.chars().filter(|c| *c != '"' && *c != '`').collect::<String>().to_lowercase()
}

fn ast_column_fingerprint(col: &vibesql_ast::IndexColumn) -> String {
    match col {
        vibesql_ast::IndexColumn::Column { column_name, .. } => fingerprint(column_name),
        vibesql_ast::IndexColumn::Expression { expr, .. } => fingerprint(&expr.to_sql()),
    }
}

fn catalog_column_fingerprint(col: &vibesql_catalog::IndexedColumn) -> String {
    match col {
        vibesql_catalog::IndexedColumn::Column { column_name, .. } => fingerprint(column_name),
        vibesql_catalog::IndexedColumn::Expression { expr, .. } => fingerprint(&expr.to_sql()),
    }
}

/// Whether the parsed `CREATE INDEX` statement references the same columns and
/// expressions as the catalog `metadata` (modulo identifier quoting/case).
fn matches_metadata(
    stmt: &vibesql_ast::CreateIndexStmt,
    metadata: &vibesql_catalog::IndexMetadata,
) -> bool {
    if stmt.columns.len() != metadata.columns.len() {
        return false;
    }
    let columns_match = stmt
        .columns
        .iter()
        .zip(metadata.columns.iter())
        .all(|(a, c)| ast_column_fingerprint(a) == catalog_column_fingerprint(c));
    let where_match = match (&stmt.where_clause, &metadata.where_clause) {
        (None, None) => true,
        (Some(a), Some(c)) => fingerprint(&a.to_sql()) == fingerprint(&c.to_sql()),
        _ => false,
    };
    columns_match && where_match
}

/// Rewrite every reference to column `old_col` in a verbatim `CREATE INDEX`
/// text to `new_col`, matching SQLite's `ALTER TABLE ... RENAME COLUMN`
/// (`CREATE INDEX i2 ON t2((LIKELIHOOD(c0, 1.0) IN ()))` becomes
/// `CREATE INDEX i2 ON t2((LIKELIHOOD(c1, 1.0) IN ()))`, altertab3.test 8.2).
///
/// Only tokens after the `ON <table>` header are considered (so neither the
/// index name nor the table name is ever touched), and within them an
/// identifier spelling `old_col` is left alone when it is:
/// - a function name (immediately followed by `(`),
/// - a table qualifier (immediately followed by `.`),
/// - a collation name (immediately after `COLLATE`),
/// - a single-quoted string literal (`'c0'` is a value, not a reference),
/// - the column part of a `<other>.<col>` reference whose qualifier is not `table_name`.
///
/// Replacement quoting follows SQLite's `bQuote` rule (quoted when the replaced
/// token was quoted or `new_col` is not a safe bare identifier), exactly as for
/// `CREATE TABLE` text in `alter_rewrite::rename_column`.
///
/// `renamed_metadata` is the index's catalog metadata *after* the rename was
/// applied to it. The edited text is re-parsed and must reference exactly the
/// same columns/expressions as that metadata; if not (a reference the token
/// scan could not classify, e.g. a string-literal column name), `None` is
/// returned and the caller invalidates the stored source instead of keeping a
/// text that disagrees with the index's real shape.
pub(crate) fn rename_index_column(
    index_sql: &str,
    table_name: &str,
    old_col: &str,
    new_col: &str,
    renamed_metadata: &vibesql_catalog::IndexMetadata,
) -> Option<String> {
    let tokens = tokenize(index_sql)?;
    let (_, _, table_idx) = index_header(&tokens)?;

    let mut targets: Vec<(Span, bool)> = Vec::new();
    for idx in (table_idx + 1)..tokens.len() {
        let (tok, span) = &tokens[idx];
        let candidate = match tok {
            Token::Identifier(_) | Token::DelimitedIdentifier(_) => ident_matches(tok, old_col),
            // Non-reserved words used as column names lex as keywords.
            Token::Keyword { original, .. } => original.eq_ignore_ascii_case(old_col),
            _ => false,
        };
        if !candidate {
            continue;
        }
        let next = tokens.get(idx + 1).map(|(t, _)| t);
        if matches!(next, Some(Token::LParen) | Some(Token::Symbol('.'))) {
            continue;
        }
        let prev = idx.checked_sub(1).and_then(|p| tokens.get(p)).map(|(t, _)| t);
        if matches!(prev, Some(Token::Keyword { keyword: Keyword::Collate, .. })) {
            continue;
        }
        if matches!(prev, Some(Token::Symbol('.'))) {
            // Qualified reference: rewrite only when qualified by this table.
            let qualifier = idx.checked_sub(2).and_then(|p| tokens.get(p)).map(|(t, _)| t);
            if !qualifier.is_some_and(|q| ident_matches(q, table_name)) {
                continue;
            }
        }
        targets.push((*span, is_quoted_ident(tok)));
    }

    if targets.is_empty() {
        return None;
    }

    let mut out = index_sql.to_string();
    for (span, was_quoted) in targets.iter().rev() {
        let mut replacement = emit_renamed_ident(new_col, *was_quoted);
        // Token-gluing guard, as in `alter_rewrite::rename_column`: a quoted
        // replacement directly followed by another `"` would lex as one token.
        if replacement.ends_with('"') && index_sql.as_bytes().get(span.end) == Some(&b'"') {
            replacement.push(' ');
        }
        out.replace_range(span.start..span.end, &replacement);
    }

    let reparsed = parses_as_create_index(&out)?;
    if !matches_metadata(&reparsed, renamed_metadata) {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_keeps_body_verbatim() {
        assert_eq!(
            normalize_create_index_source("CREATE INDEX i3 ON  t1 ( (c0 * 2) , c0 DESC )")
                .as_deref(),
            Some("CREATE INDEX i3 ON  t1 ( (c0 * 2) , c0 DESC )")
        );
    }

    #[test]
    fn normalize_strips_prefix_noise_and_semicolon() {
        assert_eq!(
            normalize_create_index_source(
                "create   unique  index if not exists main.\"I x\" ON t(a) WHERE a>0 ;  "
            )
            .as_deref(),
            Some("CREATE UNIQUE INDEX \"I x\" ON t(a) WHERE a>0")
        );
    }

    #[test]
    fn normalize_stops_at_first_statement() {
        assert_eq!(
            normalize_create_index_source("CREATE INDEX i ON t(a); SELECT 1;").as_deref(),
            Some("CREATE INDEX i ON t(a)")
        );
    }

    #[test]
    fn normalize_rejects_non_btree_shapes() {
        assert_eq!(normalize_create_index_source("CREATE SPATIAL INDEX s ON t(g)"), None);
        assert_eq!(normalize_create_index_source("CREATE TABLE t(a)"), None);
    }

    #[test]
    fn rename_table_quotes_new_name() {
        assert_eq!(
            rename_index_table("CREATE INDEX i0 ON t0(c0)", "t0", "t1").as_deref(),
            Some("CREATE INDEX i0 ON \"t1\"(c0)")
        );
        assert_eq!(
            rename_index_table("CREATE INDEX i2 ON t0((c0+1))", "t0", "t1").as_deref(),
            Some("CREATE INDEX i2 ON \"t1\"((c0+1))")
        );
    }

    #[test]
    fn rename_table_rewrites_self_qualifiers_but_not_index_name() {
        assert_eq!(
            rename_index_table("CREATE INDEX t0 ON \"T0\" ((t0.a)) WHERE t0.b > 1", "t0", "x")
                .as_deref(),
            Some("CREATE INDEX t0 ON \"x\" ((\"x\".a)) WHERE \"x\".b > 1")
        );
    }

    fn meta(sql: &str) -> vibesql_catalog::IndexMetadata {
        let stmt = parses_as_create_index(sql).expect("parses");
        let columns = stmt
            .columns
            .iter()
            .map(|c| match c {
                vibesql_ast::IndexColumn::Column { column_name, .. } => {
                    vibesql_catalog::IndexedColumn::new_column(
                        column_name.clone(),
                        vibesql_catalog::SortOrder::Ascending,
                    )
                }
                vibesql_ast::IndexColumn::Expression { expr, .. } => {
                    vibesql_catalog::IndexedColumn::new_expression(
                        (**expr).clone(),
                        vibesql_catalog::SortOrder::Ascending,
                    )
                }
            })
            .collect();
        vibesql_catalog::IndexMetadata::new(
            stmt.index_name,
            stmt.table_name,
            vibesql_catalog::IndexType::BTree,
            columns,
            false,
        )
        .with_where_clause(stmt.where_clause.map(|w| *w))
    }

    #[test]
    fn rename_column_rewrites_refs_everywhere() {
        let sql = "CREATE INDEX i ON t ( c0 DESC, (abs(c0) + t.c0), \"c0\" ) WHERE c0 > 'c0'";
        let expected = "CREATE INDEX i ON t ( c1 DESC, (abs(c1) + t.c1), \"c1\" ) WHERE c1 > 'c0'";
        assert_eq!(
            rename_index_column(sql, "t", "c0", "c1", &meta(expected)).as_deref(),
            Some(expected)
        );
    }

    #[test]
    fn rename_column_skips_function_and_collation_names() {
        let sql = "CREATE INDEX i ON t(nocase(x) COLLATE nocase, x)";
        let expected = "CREATE INDEX i ON t(nocase(x) COLLATE nocase, x)";
        // `nocase` is never a column reference here, so there is nothing to do.
        assert_eq!(rename_index_column(sql, "t", "nocase", "y", &meta(expected)), None);
    }

    #[test]
    fn rename_column_rejects_mismatch_with_metadata() {
        let sql = "CREATE INDEX i ON t(c0)";
        // Metadata says the index is on some other column: refuse to splice.
        assert_eq!(
            rename_index_column(sql, "t", "c0", "c1", &meta("CREATE INDEX i ON t(zz)")),
            None
        );
    }
}
