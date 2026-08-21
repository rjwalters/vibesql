//! In-place edits to the verbatim `CREATE TABLE` text stored in
//! `TableSchema::sql_source`, applied on `ALTER TABLE`.
//!
//! SQLite does not reconstruct `sqlite_master.sql` from the parsed schema after
//! an ALTER — it edits the *original* `CREATE TABLE` statement text in place,
//! preserving the user's whitespace and formatting everywhere except the parts
//! the ALTER touches (verified against sqlite3 3.51.0):
//!
//! - `ALTER TABLE t ADD COLUMN c INTEGER` appends `, c INTEGER` immediately before the closing `)`
//!   of the column list, leaving the rest of the text byte-for-byte unchanged.
//! - `ALTER TABLE t RENAME TO t2` rewrites the table name to the double-quoted new name (`"t2"`),
//!   preserving everything else.
//! - `ALTER TABLE t RENAME COLUMN b TO bb` rewrites the column name in its definition position,
//!   preserving everything else.
//!
//! - `ALTER TABLE t DROP COLUMN c` removes the column's definition span (the dropped column's name
//!   through the start of the next column's name, or — for the last column — from the preceding
//!   comma to the end of the column list), preserving everything else.
//!
//! This module implements those in-place edits at the token level (using the
//! parser's lexer for span tracking, the same approach as
//! `crate::trigger_rename`). The type/constraint-changing ALTER variants
//! (ALTER/MODIFY/CHANGE COLUMN, ADD/DROP CONSTRAINT) remain out of scope here:
//! they fall back to invalidating `sql_source` and reconstructing from the
//! (now-synced) catalog schema, which is correct, just lower fidelity. See
//! issues #5625 and #5634.
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
    let (open, close) = column_list_parens(&tokens)?;

    // SQLite inserts the new column immediately after the last *column*
    // definition — i.e. before any trailing table-level constraints — not merely
    // before the closing `)` (verified against sqlite3 3.51.0: alter3-1.6/1.7,
    // `CREATE TABLE t(a, b, UNIQUE(a, b))` + `ADD c` →
    // `CREATE TABLE t(a, b, c, UNIQUE(a, b))`).
    //
    // Find the first top-level definition that begins with a table-constraint
    // keyword. When every definition from there to the close is also a
    // constraint (the normal case: all constraints trail the columns), insert the
    // new column just before that constraint. Otherwise fall back to appending
    // before the closing paren (constraints interleaved with columns — rare; the
    // simple end-append stays valid and re-parseable).
    if let Some(first_constraint) = first_trailing_constraint_start(&tokens, open, close) {
        let insert_at = tokens[first_constraint].1.start;
        let mut out = String::with_capacity(create_sql.len() + coldef.len() + 2);
        out.push_str(&create_sql[..insert_at]);
        out.push_str(coldef);
        out.push_str(", ");
        out.push_str(&create_sql[insert_at..]);
        return Some(out);
    }

    let insert_at = tokens[close].1.start;
    let mut out = String::with_capacity(create_sql.len() + coldef.len() + 2);
    out.push_str(&create_sql[..insert_at]);
    out.push_str(", ");
    out.push_str(coldef);
    out.push_str(&create_sql[insert_at..]);
    Some(out)
}

/// Token index of the first top-level (depth-1) definition in the column list
/// that begins with a table-level constraint keyword (`CONSTRAINT`, `PRIMARY`,
/// `UNIQUE`, `CHECK`, `FOREIGN`), *provided* every definition from there to the
/// closing paren is also a constraint. Returns `None` when there are no trailing
/// table constraints, or when a column definition appears after a constraint
/// (interleaved layout — the caller then appends at the end instead).
///
/// `open`/`close` are the column-list paren token indices from
/// [`column_list_parens`]. Only the first token of each top-level definition is
/// inspected, so a column-level constraint keyword (e.g. the `PRIMARY` in
/// `a INTEGER PRIMARY KEY`) is never mistaken for a table constraint.
fn first_trailing_constraint_start(
    tokens: &[(Token, Span)],
    open: usize,
    close: usize,
) -> Option<usize> {
    let mut depth = 0usize;
    let mut at_def_start = false;
    let mut first_constraint: Option<usize> = None;

    let mut idx = open;
    while idx < close {
        let (tok, _) = &tokens[idx];
        match tok {
            Token::LParen => {
                depth += 1;
                if depth == 1 {
                    at_def_start = true;
                }
            }
            Token::RParen => depth -= 1,
            Token::Comma if depth == 1 => at_def_start = true,
            _ if depth == 1 && at_def_start => {
                at_def_start = false;
                let is_constraint = matches!(
                    tok,
                    Token::Keyword {
                        keyword: Keyword::Constraint
                            | Keyword::Primary
                            | Keyword::Unique
                            | Keyword::Check
                            | Keyword::Foreign,
                        ..
                    }
                );
                if is_constraint {
                    if first_constraint.is_none() {
                        first_constraint = Some(idx);
                    }
                } else if first_constraint.is_some() {
                    // A column definition follows a constraint: interleaved
                    // layout. Bail so the caller appends at the end.
                    return None;
                }
            }
            _ => {}
        }
        idx += 1;
    }

    first_constraint
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
/// Wired into RENAME TO via `super::alter::table_options::execute_rename_table`
/// (issue #5634). The dump statement splitter is quote-aware, so the emitted
/// `"new_name"` identifier round-trips through `.sql` / `.vbsql` reloads.
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
        if !matches!(tokens.get(i), Some((Token::Identifier(_) | Token::DelimitedIdentifier(_), _)))
        {
            return None;
        }
    }
    Some(i)
}

/// Remove the `<schema>.` database qualifier from the table name in a verbatim
/// `CREATE TABLE` statement. SQLite never stores the database qualifier in
/// `sqlite_master.sql`: `CREATE TABLE main.t1(a, b)` is stored (and later
/// rewritten by ALTER TABLE) as `CREATE TABLE t1(a, b)`. Everything else in the
/// statement — whitespace, quoting, the column list — is preserved byte-for-byte.
///
/// Returns `None` when the statement carries no schema qualifier (or cannot be
/// tokenized), so the caller keeps the original text unchanged. Verified against
/// sqlite3 3.51.0 (alter3-1.4/1.5).
pub fn strip_schema_qualifier(create_sql: &str) -> Option<String> {
    let tokens = tokenize(create_sql)?;
    let name_idx = table_name_index(&tokens)?;
    // A qualifier is present only when the token immediately before the resolved
    // table-name token is the `.` separator (its predecessor being the schema
    // identifier). Without one, `table_name_index` lands directly on the bare
    // name and there is nothing to strip.
    if name_idx < 2 || !matches!(tokens.get(name_idx - 1), Some((Token::Symbol('.'), _))) {
        return None;
    }
    let schema_start = tokens[name_idx - 2].1.start;
    let name_start = tokens[name_idx].1.start;
    let mut out = String::with_capacity(create_sql.len());
    out.push_str(&create_sql[..schema_start]);
    out.push_str(&create_sql[name_start..]);
    Some(out)
}

/// Rewrite every `REFERENCES <old_parent>` clause in the verbatim `CREATE TABLE`
/// text of a *child* table to `REFERENCES "<new_parent>"`, matching SQLite's
/// `sqlite_rename_parent` (invoked when the referenced parent table is renamed
/// via `ALTER TABLE ... RENAME TO`, with `legacy_alter_table=OFF`).
///
/// Only the parent-table identifier that immediately follows a `REFERENCES`
/// keyword is considered, so a bare `p` appearing elsewhere (inside a string
/// literal, a column name, or an identifier that merely contains `p` as a
/// substring) is never touched. Quote-awareness is inherited from the lexer,
/// which normalizes `"p"`, `` `p` ``, and `[p]` all to a delimited-identifier
/// token and emits string literals as a distinct token kind — so all quoted
/// spellings of the parent match while string-literal look-alikes do not.
///
/// The replacement is emitted double-quoted (`"<new_parent>"`) to mirror
/// SQLite's output style for renamed objects. Handles multiple matching FKs in a
/// single `CREATE TABLE` (e.g. two columns each `REFERENCES p`). Returns `None`
/// when no `REFERENCES <old_parent>` clause is present (the caller then
/// invalidates and reconstructs), keeping the re-parseable-on-reload invariant.
pub fn rename_references_parent(
    create_sql: &str,
    old_parent: &str,
    new_parent: &str,
) -> Option<String> {
    let tokens = tokenize(create_sql)?;
    let replacement = quote_ident(new_parent);

    // Collect the byte spans of every parent-table identifier that immediately
    // follows a `REFERENCES` keyword and matches `old_parent` (case-insensitively
    // for bare and delimited identifiers, mirroring SQLite's name folding).
    let mut spans: Vec<Span> = Vec::new();
    for (i, (tok, _)) in tokens.iter().enumerate() {
        if !matches!(tok, Token::Keyword { keyword: Keyword::References, .. }) {
            continue;
        }
        if let Some((next_tok, next_span)) = tokens.get(i + 1) {
            if ident_matches(next_tok, old_parent) {
                spans.push(*next_span);
            }
        }
    }
    if spans.is_empty() {
        return None;
    }

    // Replace from the last span backward so earlier byte offsets stay valid.
    let mut out = create_sql.to_string();
    for span in spans.iter().rev() {
        out.replace_range(span.start..span.end, &replacement);
    }
    Some(out)
}

/// Emit `new_col` as an identifier, mirroring SQLite's `bQuote` rule during
/// RENAME COLUMN: the replacement is double-quoted when the *replaced* token was
/// itself a quoted (delimited) identifier, or when the new name is not a safe
/// bare identifier; otherwise it is emitted bare. Verified against sqlite3
/// 3.51.0 (altercol.test 1.2/1.9 — a quoted `"b"`/`"B"` becomes quoted `"d"`
/// even though `d` is a safe bare name; 4.4 — a quoted `"silly name"` becomes
/// quoted `"reasonable"`).
fn emit_renamed_ident(new_col: &str, replaced_was_quoted: bool) -> String {
    if replaced_was_quoted || !is_safe_bare_identifier(new_col) {
        quote_ident(new_col)
    } else {
        new_col.to_string()
    }
}

/// Rewrite *every* reference to `old_col` that resolves to `table_name`'s column
/// within the verbatim `CREATE TABLE` text, matching SQLite's
/// `ALTER TABLE ... RENAME COLUMN` (with `legacy_alter_table=OFF`).
///
/// SQLite does not rewrite only the definition-position token: it rewrites the
/// column name everywhere it appears as a reference to the renamed column —
/// inside `CHECK(...)` expressions (bare `b` and qualified `t1.b`), table-level
/// `PRIMARY KEY(...)`, `UNIQUE(...)`, and `FOREIGN KEY (...)` column lists, and
/// column-level constraints. Verified against sqlite3 3.51.0 (altercol.test
/// group 1). Without this, the persisted `sql_source` keeps the stale column
/// name in its constraints; a later checkpoint reload then fails the
/// fail-closed FK/constraint rehydration ("FK column 'b' ... not found").
///
/// Column references inside a `REFERENCES <other>(<col_list>)` parent column list
/// are left untouched when `<other>` is a *different* table — those names resolve
/// to the parent and are rewritten from the parent side (see
/// [`rename_references_column`]). A self-referential `REFERENCES <table>(...)`
/// list *is* rewritten, since those columns resolve to the renamed table.
///
/// Quoting follows SQLite's `bQuote` rule via [`emit_renamed_ident`]. Returns
/// `None` when `old_col` is not referenced (the caller then falls back to
/// invalidate-and-reconstruct), preserving the re-parseable-on-reload invariant.
pub fn rename_column(
    create_sql: &str,
    table_name: &str,
    old_col: &str,
    new_col: &str,
) -> Option<String> {
    let tokens = tokenize(create_sql)?;
    let (open, close) = column_list_parens(&tokens)?;

    // Byte spans of every identifier token to rewrite, paired with whether the
    // replaced token was quoted (drives replacement quoting).
    let mut targets: Vec<(Span, bool)> = Vec::new();
    let mut depth = 0usize;

    // Parent-column-list skipping: after `REFERENCES <parent>` where `<parent>`
    // is a *different* table, the immediately following `(<col_list>)` names the
    // parent's columns — skip rewriting inside it. `skip_below` holds the depth
    // outside that parenthesized list while it is active.
    let mut expect_parent_table = false;
    let mut skip_below: Option<usize> = None;

    // Non-column-reference position guards (see issue #5939): the token scanner
    // must not rewrite an identifier that merely *spells* the old column name but
    // occupies a type-name, function-name, or collation-name slot in the DDL.
    //
    // - `expect_collation`: the identifier immediately after `COLLATE` names a collating sequence
    //   (e.g. `COLLATE nocase`), never a column reference.
    // - `at_def_start`: true at the first token of a top-level column/constraint definition (right
    //   after the opening `(` and after each depth-1 `,`); the first identifier there is the column
    //   *name* (a rewrite target), and marks the following depth-1 identifier as this column's
    //   *type* name.
    // - `saw_col_name`: set right after a depth-1 column-name identifier; the next depth-1 bare
    //   identifier is the type name (e.g. the `foo` in `a foo`) and must be skipped.
    let mut expect_collation = false;
    let mut at_def_start = false;
    let mut saw_col_name = false;

    let mut idx = open;
    while idx < close {
        let (tok, span) = &tokens[idx];
        match tok {
            Token::LParen => {
                depth += 1;
                if depth == 1 {
                    // Opening paren of the column list: the first definition begins.
                    at_def_start = true;
                }
            }
            Token::RParen => {
                if let Some(d) = skip_below {
                    if depth == d + 1 {
                        skip_below = None;
                    }
                }
                depth -= 1;
            }
            Token::Comma if depth == 1 => {
                // A new top-level definition begins after each depth-1 comma.
                at_def_start = true;
                saw_col_name = false;
                expect_collation = false;
                expect_parent_table = false;
            }
            Token::Keyword { keyword: Keyword::References, .. } => {
                expect_parent_table = true;
                if depth == 1 {
                    saw_col_name = false;
                    at_def_start = false;
                }
            }
            Token::Keyword { keyword: Keyword::Collate, .. } => {
                // The identifier that follows names a collating sequence.
                expect_collation = true;
                if depth == 1 {
                    saw_col_name = false;
                    at_def_start = false;
                }
            }
            Token::Identifier(_) | Token::DelimitedIdentifier(_) => {
                if expect_parent_table {
                    expect_parent_table = false;
                    if depth == 1 {
                        saw_col_name = false;
                        at_def_start = false;
                    }
                    // This identifier names the parent table. When it is a
                    // *different* table and introduces a `(col_list)`, skip that
                    // list (those columns belong to the parent).
                    if !ident_matches(tok, table_name)
                        && matches!(tokens.get(idx + 1), Some((Token::LParen, _)))
                    {
                        skip_below = Some(depth);
                    }
                    idx += 1;
                    continue;
                }
                if skip_below.is_some() {
                    idx += 1;
                    continue;
                }
                // Collation-name position: the identifier immediately after
                // `COLLATE` (e.g. `nocase`) is a collating sequence, never a
                // column reference — leave it untouched.
                if expect_collation {
                    expect_collation = false;
                    idx += 1;
                    continue;
                }
                // Column-name (definition) position: the first identifier of a
                // top-level column definition. It *is* a rewrite target, and it
                // marks the next depth-1 identifier as this column's type name.
                if depth == 1 && at_def_start {
                    at_def_start = false;
                    saw_col_name = true;
                    if ident_matches(tok, old_col) {
                        targets.push((*span, matches!(tok, Token::DelimitedIdentifier(_))));
                    }
                    idx += 1;
                    continue;
                }
                // Type-name position: a bare-identifier type immediately following
                // a column name (e.g. the `foo` in `a foo`) — not a column
                // reference, even when it spells the renamed column.
                if depth == 1 && saw_col_name {
                    saw_col_name = false;
                    idx += 1;
                    continue;
                }
                // Function-call position: an identifier immediately followed by
                // `(` is a function call (e.g. `abs(a)` inside a CHECK), not a
                // column reference. Bare identifiers inside paren lists such as
                // `PRIMARY KEY(abs)` are not followed by `(`, so they still
                // rewrite correctly.
                if matches!(tokens.get(idx + 1), Some((Token::LParen, _))) {
                    idx += 1;
                    continue;
                }
                // An identifier immediately followed by `.` is a table qualifier
                // (e.g. the `t1` in `t1.b`), not a column reference — never rewrite
                // it. The column after the dot is handled on the next iteration.
                if matches!(tokens.get(idx + 1), Some((Token::Symbol('.'), _))) {
                    idx += 1;
                    continue;
                }
                if ident_matches(tok, old_col) {
                    targets.push((*span, matches!(tok, Token::DelimitedIdentifier(_))));
                }
            }
            _ => {
                expect_parent_table = false;
                expect_collation = false;
                if depth == 1 {
                    // Any other depth-1 token (a type keyword like INTEGER, a
                    // constraint keyword, an operator, …) ends the column-name /
                    // type-name window.
                    saw_col_name = false;
                    at_def_start = false;
                }
            }
        }
        idx += 1;
    }

    if targets.is_empty() {
        return None;
    }

    // Rewrite from the last span backward so earlier byte offsets stay valid.
    let mut out = create_sql.to_string();
    for (span, was_quoted) in targets.iter().rev() {
        out.replace_range(span.start..span.end, &emit_renamed_ident(new_col, *was_quoted));
    }
    Some(out)
}

/// Rewrite the column name `old_col` -> `new_col` inside every
/// `REFERENCES <parent_table>(<col_list>)` clause of a *child* table's verbatim
/// `CREATE TABLE` text, matching SQLite's `sqlite_rename_column` propagation to
/// child foreign keys when a *parent* table's column is renamed (verified against
/// sqlite3 3.51.0, altercol.test 4.1/4.4).
///
/// Only column identifiers inside the parenthesized parent column list that
/// immediately follows `REFERENCES <parent_table>` are considered, so a bare
/// column reference elsewhere (the child's own columns, a `CHECK`, a string
/// literal) is never touched. The parent-table match is quote-aware and
/// case-insensitive (inherited from the lexer). Quoting of the replacement
/// follows SQLite's `bQuote` rule via [`emit_renamed_ident`]. Returns `None` when
/// no matching `REFERENCES <parent_table>(...)` column is present (the caller
/// then invalidates and reconstructs).
pub fn rename_references_column(
    create_sql: &str,
    parent_table: &str,
    old_col: &str,
    new_col: &str,
) -> Option<String> {
    let tokens = tokenize(create_sql)?;

    let mut targets: Vec<(Span, bool)> = Vec::new();
    for i in 0..tokens.len() {
        if !matches!(tokens[i].0, Token::Keyword { keyword: Keyword::References, .. }) {
            continue;
        }
        // The parent table follows REFERENCES; a `(` after it opens the col list.
        match tokens.get(i + 1) {
            Some((ptok, _)) if ident_matches(ptok, parent_table) => {}
            _ => continue,
        }
        if !matches!(tokens.get(i + 2), Some((Token::LParen, _))) {
            continue;
        }
        // Scan the balanced parent column list, rewriting matches at its top level.
        let mut depth = 0usize;
        let mut j = i + 2;
        while j < tokens.len() {
            match &tokens[j].0 {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                Token::Identifier(_) | Token::DelimitedIdentifier(_)
                    if depth == 1 && ident_matches(&tokens[j].0, old_col) =>
                {
                    targets
                        .push((tokens[j].1, matches!(tokens[j].0, Token::DelimitedIdentifier(_))));
                }
                _ => {}
            }
            j += 1;
        }
    }

    if targets.is_empty() {
        return None;
    }

    let mut out = create_sql.to_string();
    for (span, was_quoted) in targets.iter().rev() {
        out.replace_range(span.start..span.end, &emit_renamed_ident(new_col, *was_quoted));
    }
    Some(out)
}

/// The byte position where a top-level definition begins inside the column
/// list: the index of the *name token* of a column definition, paired with
/// whether that definition is a column (vs. a table-level constraint, which we
/// leave alone).
struct DefStart {
    /// Index into the token slice of this definition's first token.
    tok_idx: usize,
    /// True when the definition is a column (its first token is an identifier);
    /// false for a table-level constraint (`PRIMARY KEY`, `UNIQUE`, `CHECK`,
    /// `FOREIGN KEY`, `CONSTRAINT …`).
    is_column: bool,
}

/// Collect the start of every top-level definition in the column list (columns
/// and table-level constraints), in source order. Depth-1 only; nested parens
/// (e.g. type sizes, `CHECK(...)`) are skipped.
fn definition_starts(tokens: &[(Token, Span)], open: usize, close: usize) -> Vec<DefStart> {
    let mut defs = Vec::new();
    let mut depth = 0usize;
    let mut at_def_start = false;
    for (i, (tok, _)) in tokens.iter().enumerate().take(close).skip(open) {
        match tok {
            Token::LParen => {
                depth += 1;
                if depth == 1 {
                    at_def_start = true; // first token inside the column list
                }
            }
            Token::RParen => depth -= 1,
            Token::Comma if depth == 1 => at_def_start = true,
            _ if depth == 1 && at_def_start => {
                at_def_start = false;
                let is_column = matches!(tok, Token::Identifier(_) | Token::DelimitedIdentifier(_));
                defs.push(DefStart { tok_idx: i, is_column });
            }
            _ => {}
        }
    }
    defs
}

/// Remove a column definition from the verbatim `CREATE TABLE` text in place,
/// matching SQLite's `ALTER TABLE ... DROP COLUMN` byte-for-byte (verified
/// against sqlite3 3.51.0).
///
/// SQLite deletes the span from the start of the dropped column's *name* to the
/// start of the next column's name. For the last column (no following column) it
/// instead walks backward from the column name to the preceding `,` and deletes
/// from there to the end of the column list (the closing `)` or the first
/// table-level constraint). This preserves the user's surrounding whitespace
/// exactly as SQLite does.
///
/// Returns `None` (so the caller falls back to reconstruction) when the column
/// cannot be located unambiguously, when it is the only/first-of-one column, or
/// when the structure is otherwise unexpected.
pub fn drop_column(create_sql: &str, col: &str) -> Option<String> {
    let tokens = tokenize(create_sql)?;
    let (open, close) = column_list_parens(&tokens)?;
    let defs = definition_starts(&tokens, open, close);

    // Index within `defs` of the target column.
    let mut target: Option<usize> = None;
    for (di, def) in defs.iter().enumerate() {
        if def.is_column && ident_matches(&tokens[def.tok_idx].0, col) {
            if target.is_some() {
                return None; // ambiguous duplicate column name
            }
            target = Some(di);
        }
    }
    let di = target?;

    // Never drop the only remaining column.
    let column_count = defs.iter().filter(|d| d.is_column).count();
    if column_count <= 1 {
        return None;
    }

    let name_start = tokens[defs[di].tok_idx].1.start;

    // The "next column" is the next *column* definition after this one (a table
    // constraint does not count — see the table-constraint cases verified
    // against sqlite3). If there is one, delete `[name_start, next_name_start)`.
    let next_col = defs[di + 1..].iter().find(|d| d.is_column);

    let bytes = create_sql.as_bytes();
    let (del_start, del_end) = if let Some(next) = next_col {
        (name_start, tokens[next.tok_idx].1.start)
    } else {
        // Last column (no following *column*; a table-level constraint may
        // still follow). SQLite deletes from the comma preceding this column up
        // to its `addColOffset` — the point where ADD COLUMN would insert, which
        // is the comma preceding the first table-level constraint, or the
        // closing `)` when there are no constraints. So:
        //   - end: if a table constraint follows, the comma just before it; otherwise the closing
        //     `)`.
        //   - start: walk back to the preceding `,` (matching SQLite's `while(*z!=',') z--`).
        let end = match defs[di + 1..].first() {
            Some(constraint) => {
                // Walk back from the constraint token over whitespace to the
                // separating comma; that comma (and the column text before it)
                // is what gets removed.
                let mut e = tokens[constraint.tok_idx].1.start;
                while e > 0 && bytes[e - 1] != b',' {
                    e -= 1;
                }
                if e == 0 || bytes[e - 1] != b',' {
                    return None;
                }
                e - 1
            }
            None => tokens[close].1.start,
        };
        let mut start = name_start;
        while start > 0 && bytes[start - 1] != b',' {
            start -= 1;
        }
        if start == 0 || bytes[start - 1] != b',' {
            // No preceding comma found (shouldn't happen for a non-first column,
            // but guards the first-column-with-no-next pathological case).
            return None;
        }
        (start - 1, end)
    };

    let mut out = String::with_capacity(create_sql.len());
    out.push_str(&create_sql[..del_start]);
    out.push_str(&create_sql[del_end..]);
    Some(out)
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
    let add_pos = tokens
        .iter()
        .position(|(t, _)| matches!(t, Token::Keyword { keyword: Keyword::Add, .. }))?;
    let mut start_tok = add_pos + 1;
    // Optional COLUMN keyword.
    if matches!(tokens.get(start_tok), Some((Token::Keyword { keyword: Keyword::Column, .. }, _))) {
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
/// alphanumeric/underscore, not starting with a digit, non-empty, and not a
/// reserved SQL keyword. Used so a plain column rename produces `bb` rather
/// than `"bb"`.
///
/// A name that is otherwise shape-valid but lexes as a keyword (e.g. `where`,
/// `select`) still cannot be emitted bare: `CREATE TABLE t(where INTEGER)`
/// does not re-parse, because `where` occupies a keyword-token position there,
/// not an identifier-token position — the same failure mode issue #5619's
/// re-parseable-on-reload invariant exists to prevent (verified against
/// sqlite3 3.51.0, altercol.test 6.2/6.3: `RENAME COLUMN a1 TO [where]` emits
/// the quoted `"where"`). Detected by tokenizing `name` itself and checking
/// whether the lexer classifies it as a keyword, rather than a
/// hand-maintained keyword list that could drift from the parser's actual
/// keyword set.
fn is_safe_bare_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    if !chars.all(|c| c == '_' || c.is_ascii_alphanumeric()) {
        return false;
    }
    !matches!(tokenize(name).as_deref(), Some([(Token::Keyword { .. }, _)]))
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
    fn strip_schema_qualifier_removes_database_prefix() {
        // SQLite never stores the database qualifier (alter3-1.4/1.5).
        assert_eq!(
            strip_schema_qualifier("CREATE TABLE main.t1(a, b)").as_deref(),
            Some("CREATE TABLE t1(a, b)")
        );
    }

    #[test]
    fn strip_schema_qualifier_preserves_surrounding_text() {
        // Only the `<schema>.` run is removed; column list and spacing are kept.
        assert_eq!(
            strip_schema_qualifier("CREATE TABLE  main.\"My Tbl\" (x INT)").as_deref(),
            Some("CREATE TABLE  \"My Tbl\" (x INT)")
        );
    }

    #[test]
    fn strip_schema_qualifier_none_when_unqualified() {
        assert!(strip_schema_qualifier("CREATE TABLE t1(a, b)").is_none());
    }

    #[test]
    fn append_column_before_trailing_table_constraint() {
        // SQLite inserts the new column before a trailing table-level constraint,
        // not before the closing paren (alter3-1.6/1.7).
        let sql = "CREATE TABLE t2(a, b, UNIQUE(a, b))";
        let out = append_column(sql, "c REFERENCES t1(c)").unwrap();
        assert_eq!(out, "CREATE TABLE t2(a, b, c REFERENCES t1(c), UNIQUE(a, b))");
    }

    #[test]
    fn append_column_before_multiple_trailing_constraints() {
        let sql = "CREATE TABLE t(a, b, PRIMARY KEY(a), CHECK(b > 0))";
        let out = append_column(sql, "c INTEGER").unwrap();
        assert_eq!(out, "CREATE TABLE t(a, b, c INTEGER, PRIMARY KEY(a), CHECK(b > 0))");
    }

    #[test]
    fn append_column_no_table_constraint_appends_at_end() {
        // Column-level PRIMARY KEY is part of a column definition, not a
        // table-level constraint, so the new column still appends at the end.
        let sql = "CREATE TABLE t(a INTEGER PRIMARY KEY, b)";
        let out = append_column(sql, "c INTEGER").unwrap();
        assert_eq!(out, "CREATE TABLE t(a INTEGER PRIMARY KEY, b, c INTEGER)");
    }

    #[test]
    fn append_column_interleaved_constraint_falls_back_to_end() {
        // A column definition after a table constraint is an unusual layout;
        // fall back to appending at the end (still valid + re-parseable).
        let sql = "CREATE TABLE t(a, CHECK(a > 0), b)";
        let out = append_column(sql, "c INTEGER").unwrap();
        assert_eq!(out, "CREATE TABLE t(a, CHECK(a > 0), b, c INTEGER)");
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

    // rename_references_parent — quote-aware child REFERENCES rewriter.

    #[test]
    fn rename_references_parent_bare_name() {
        let sql = "CREATE TABLE c(x REFERENCES p(id) ON DELETE CASCADE)";
        let out = rename_references_parent(sql, "p", "p_new").unwrap();
        assert_eq!(out, "CREATE TABLE c(x REFERENCES \"p_new\"(id) ON DELETE CASCADE)");
    }

    #[test]
    fn rename_references_parent_table_constraint_form() {
        let sql = "CREATE TABLE c(x, FOREIGN KEY(x) REFERENCES p(id))";
        let out = rename_references_parent(sql, "p", "p_new").unwrap();
        assert_eq!(out, "CREATE TABLE c(x, FOREIGN KEY(x) REFERENCES \"p_new\"(id))");
    }

    #[test]
    fn rename_references_parent_double_quoted_old_name() {
        let sql = "CREATE TABLE c(x REFERENCES \"p\"(id))";
        let out = rename_references_parent(sql, "p", "p_new").unwrap();
        assert_eq!(out, "CREATE TABLE c(x REFERENCES \"p_new\"(id))");
    }

    #[test]
    fn rename_references_parent_bracket_quoted_old_name() {
        let sql = "CREATE TABLE c(x REFERENCES [p](id))";
        let out = rename_references_parent(sql, "p", "p_new").unwrap();
        assert_eq!(out, "CREATE TABLE c(x REFERENCES \"p_new\"(id))");
    }

    #[test]
    fn rename_references_parent_backtick_quoted_old_name() {
        let sql = "CREATE TABLE c(x REFERENCES `p`(id))";
        let out = rename_references_parent(sql, "p", "p_new").unwrap();
        assert_eq!(out, "CREATE TABLE c(x REFERENCES \"p_new\"(id))");
    }

    #[test]
    fn rename_references_parent_case_insensitive() {
        let sql = "CREATE TABLE c(x REFERENCES P(id))";
        let out = rename_references_parent(sql, "p", "p_new").unwrap();
        assert_eq!(out, "CREATE TABLE c(x REFERENCES \"p_new\"(id))");
    }

    #[test]
    fn rename_references_parent_multiple_fks() {
        let sql = "CREATE TABLE c(a REFERENCES p(id), b REFERENCES p(id))";
        let out = rename_references_parent(sql, "p", "p_new").unwrap();
        assert_eq!(out, "CREATE TABLE c(a REFERENCES \"p_new\"(id), b REFERENCES \"p_new\"(id))");
    }

    #[test]
    fn rename_references_parent_self_referential() {
        // After the header has already been rewritten to the new name, the inline
        // self-reference still names the old table and must be rewritten too.
        let sql = "CREATE TABLE \"p_new\"(id INTEGER PRIMARY KEY, pid REFERENCES p(id))";
        let out = rename_references_parent(sql, "p", "p_new").unwrap();
        assert_eq!(
            out,
            "CREATE TABLE \"p_new\"(id INTEGER PRIMARY KEY, pid REFERENCES \"p_new\"(id))"
        );
    }

    #[test]
    fn rename_references_parent_ignores_string_literal_lookalike() {
        // A bare `p` inside a string literal default must not be rewritten.
        let sql = "CREATE TABLE c(note TEXT DEFAULT 'see p for details', x REFERENCES p(id))";
        let out = rename_references_parent(sql, "p", "p_new").unwrap();
        assert_eq!(
            out,
            "CREATE TABLE c(note TEXT DEFAULT 'see p for details', x REFERENCES \"p_new\"(id))"
        );
    }

    #[test]
    fn rename_references_parent_ignores_substring_identifier() {
        // `parent` merely contains `p`; only the exact REFERENCES target changes.
        let sql = "CREATE TABLE c(parent TEXT, x REFERENCES p(id))";
        let out = rename_references_parent(sql, "p", "p_new").unwrap();
        assert_eq!(out, "CREATE TABLE c(parent TEXT, x REFERENCES \"p_new\"(id))");
    }

    #[test]
    fn rename_references_parent_no_match_returns_none() {
        let sql = "CREATE TABLE c(x REFERENCES other(id))";
        assert!(rename_references_parent(sql, "p", "p_new").is_none());
    }

    #[test]
    fn rename_references_parent_no_references_returns_none() {
        let sql = "CREATE TABLE c(x INTEGER, y TEXT)";
        assert!(rename_references_parent(sql, "p", "p_new").is_none());
    }

    #[test]
    fn rename_column_in_definition_position() {
        let sql = "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n)";
        let out = rename_column(sql, "t", "b", "bb").unwrap();
        assert_eq!(out, "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  bb   TEXT\n)");
    }

    #[test]
    fn rename_column_first_column() {
        let sql = "CREATE TABLE t (a INTEGER, b TEXT)";
        let out = rename_column(sql, "t", "a", "aa").unwrap();
        assert_eq!(out, "CREATE TABLE t (aa INTEGER, b TEXT)");
    }

    #[test]
    fn rename_column_quotes_unsafe_name() {
        let sql = "CREATE TABLE t (a INTEGER, b TEXT)";
        let out = rename_column(sql, "t", "b", "new col").unwrap();
        assert_eq!(out, "CREATE TABLE t (a INTEGER, \"new col\" TEXT)");
    }

    #[test]
    fn rename_column_missing_returns_none() {
        let sql = "CREATE TABLE t (a INTEGER, b TEXT)";
        assert!(rename_column(sql, "t", "zzz", "qqq").is_none());
    }

    #[test]
    fn rename_column_to_reserved_keyword_is_quoted() {
        // altercol 6.2/6.3: renaming a column to a name that lexes as a
        // reserved keyword (`where`) must emit it quoted, or the persisted
        // CREATE TABLE text fails to re-parse on the next reload/checkpoint.
        let sql = "CREATE TABLE blob(a1 INTEGER PRIMARY KEY, rcvid INTEGER)";
        let out = rename_column(sql, "blob", "a1", "where").unwrap();
        assert_eq!(out, "CREATE TABLE blob(\"where\" INTEGER PRIMARY KEY, rcvid INTEGER)");
        // The result must itself still be a valid, re-parseable CREATE TABLE.
        assert!(tokenize(&out).is_some());
    }

    #[test]
    fn rename_column_to_ordinary_name_stays_bare() {
        // Regression guard: an ordinary safe name must NOT be over-quoted by
        // the new keyword check.
        let sql = "CREATE TABLE t (a INTEGER, b TEXT)";
        let out = rename_column(sql, "t", "a", "aa").unwrap();
        assert_eq!(out, "CREATE TABLE t (aa INTEGER, b TEXT)");
    }

    // Constraint-reference rewriting (altercol.test group 1, sqlite3 3.51.0).

    #[test]
    fn rename_column_preserves_quoted_def_position() {
        // altercol 1.2: a quoted `"b"` def becomes quoted `"d"` (bQuote carries).
        let sql = "CREATE TABLE t1(a INTEGER, x TEXT, \"b\" BLOB)";
        let out = rename_column(sql, "t1", "b", "d").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a INTEGER, x TEXT, \"d\" BLOB)");
    }

    #[test]
    fn rename_column_check_bare_ref() {
        // altercol 1.3
        let sql = "CREATE TABLE t1(a INTEGER, b TEXT, c BLOB, CHECK(b!=''))";
        let out = rename_column(sql, "t1", "b", "d").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a INTEGER, d TEXT, c BLOB, CHECK(d!=''))");
    }

    #[test]
    fn rename_column_check_qualified_ref() {
        // altercol 1.4: qualified `t1.b` rewrites the column, not the qualifier.
        let sql = "CREATE TABLE t1(a INTEGER, b TEXT, c BLOB, CHECK(t1.b!=''))";
        let out = rename_column(sql, "t1", "b", "d").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a INTEGER, d TEXT, c BLOB, CHECK(t1.d!=''))");
    }

    #[test]
    fn rename_column_check_nested_expr() {
        // altercol 1.5
        let sql = "CREATE TABLE t1(a INTEGER, b TEXT, c BLOB, CHECK( coalesce(b,c) ))";
        let out = rename_column(sql, "t1", "b", "d").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a INTEGER, d TEXT, c BLOB, CHECK( coalesce(d,c) ))");
    }

    #[test]
    fn rename_column_quoted_def_no_space_plus_check() {
        // altercol 1.6: `"b"TEXT` (no space) + CHECK referencing bare b.
        let sql = "CREATE TABLE t1(a INTEGER, \"b\"TEXT, c BLOB, CHECK( coalesce(b,c) ))";
        let out = rename_column(sql, "t1", "b", "d").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a INTEGER, \"d\"TEXT, c BLOB, CHECK( coalesce(d,c) ))");
    }

    #[test]
    fn rename_column_table_level_primary_key() {
        // altercol 1.7
        let sql = "CREATE TABLE t1(a INTEGER, b TEXT, c BLOB, PRIMARY KEY(b, c))";
        let out = rename_column(sql, "t1", "b", "d").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a INTEGER, d TEXT, c BLOB, PRIMARY KEY(d, c))");
    }

    #[test]
    fn rename_column_pk_and_unique_quoted() {
        // altercol 1.9: PK list + UNIQUE("B") (quoted, case-insensitive match).
        let sql = "CREATE TABLE t1(a, b TEXT, c, PRIMARY KEY(a, b), UNIQUE(\"B\"))";
        let out = rename_column(sql, "t1", "b", "d").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a, d TEXT, c, PRIMARY KEY(a, d), UNIQUE(\"d\"))");
    }

    #[test]
    fn rename_column_foreign_key_local_col_list() {
        // altercol 1.13: the FK's own `(b)` list rewrites; parent name untouched.
        let sql = "CREATE TABLE t1(a, b, c, FOREIGN KEY (b) REFERENCES t2)";
        let out = rename_column(sql, "t1", "b", "d").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a, d, c, FOREIGN KEY (d) REFERENCES t2)");
    }

    #[test]
    fn rename_column_skips_other_parent_col_list() {
        // A `REFERENCES other(b)` parent column list belongs to `other`, not this
        // table, so the parent-side `b` must NOT be rewritten — only this table's
        // own column `b` (def position + local FK list).
        let sql = "CREATE TABLE t1(a, b, FOREIGN KEY (b) REFERENCES other(b))";
        let out = rename_column(sql, "t1", "b", "d").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a, d, FOREIGN KEY (d) REFERENCES other(b))");
    }

    #[test]
    fn rename_column_big_fk_col_list() {
        // altercol 2.x: many-column FK list, unquoted new name.
        let sql = "CREATE TABLE t3(a, b, c, d, FOREIGN KEY (b, c, d) REFERENCES t4)";
        let out = rename_column(sql, "t3", "b", "biglongname").unwrap();
        assert_eq!(
            out,
            "CREATE TABLE t3(a, biglongname, c, d, FOREIGN KEY (biglongname, c, d) REFERENCES t4)"
        );
    }

    // Over-rewrite guards (issue #5939): a renamed column that coincidentally
    // spells a type / function / collation name used elsewhere in the same DDL
    // must NOT rewrite those non-column-reference tokens (verified against
    // sqlite3 3.51.0).

    #[test]
    fn rename_column_preserves_type_name_collision() {
        // The `foo` type of column `a` must be preserved; only the `foo` *column*
        // is renamed. sqlite3 3.51.0: CREATE TABLE t1(a foo, bar INTEGER).
        let sql = "CREATE TABLE t1(a foo, foo INTEGER)";
        let out = rename_column(sql, "t1", "foo", "bar").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a foo, bar INTEGER)");
    }

    #[test]
    fn rename_column_preserves_function_name_collision() {
        // The `abs(a)` function call in the CHECK must be preserved; only the
        // `abs` *column* is renamed. sqlite3 3.51.0 keeps CHECK(abs(a) > 0).
        let sql = "CREATE TABLE t1(a INTEGER, abs INTEGER, CHECK(abs(a) > 0))";
        let out = rename_column(sql, "t1", "abs", "absval").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a INTEGER, absval INTEGER, CHECK(abs(a) > 0))");
    }

    #[test]
    fn rename_column_preserves_collation_name_collision() {
        // The `nocase` collation of column `a` must be preserved; only the
        // `nocase` *column* is renamed. sqlite3 3.51.0 keeps COLLATE nocase.
        let sql = "CREATE TABLE t1(a TEXT COLLATE nocase, nocase INTEGER)";
        let out = rename_column(sql, "t1", "nocase", "nc").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a TEXT COLLATE nocase, nc INTEGER)");
    }

    #[test]
    fn rename_column_still_rewrites_bare_ref_in_paren_list() {
        // Regression guard: the function-call guard must only suppress
        // identifier-immediately-followed-by-`(`. A bare column inside a
        // `PRIMARY KEY(...)` list (not followed by `(`) still rewrites.
        let sql = "CREATE TABLE t1(a INTEGER, abs INTEGER, PRIMARY KEY(abs))";
        let out = rename_column(sql, "t1", "abs", "absval").unwrap();
        assert_eq!(out, "CREATE TABLE t1(a INTEGER, absval INTEGER, PRIMARY KEY(absval))");
    }

    // rename_references_column — child-table parent-column-list rewriting.

    #[test]
    fn rename_references_column_unsafe_new_name() {
        // altercol 4.1: parent p1.d renamed to "silly name" -> child FK list.
        let sql = "CREATE TABLE c1(a, b, FOREIGN KEY (a, b) REFERENCES p1(c, d))";
        let out = rename_references_column(sql, "p1", "d", "silly name").unwrap();
        assert_eq!(
            out,
            "CREATE TABLE c1(a, b, FOREIGN KEY (a, b) REFERENCES p1(c, \"silly name\"))"
        );
    }

    #[test]
    fn rename_references_column_quoted_old_stays_quoted() {
        // altercol 4.4: "silly name" -> reasonable is emitted quoted (bQuote).
        let sql = "CREATE TABLE c1(a, b, FOREIGN KEY (a, b) REFERENCES p1(c, \"silly name\"))";
        let out = rename_references_column(sql, "p1", "silly name", "reasonable").unwrap();
        assert_eq!(
            out,
            "CREATE TABLE c1(a, b, FOREIGN KEY (a, b) REFERENCES p1(c, \"reasonable\"))"
        );
    }

    #[test]
    fn rename_references_column_no_match_returns_none() {
        // No parent column list after REFERENCES -> nothing to rewrite.
        let sql = "CREATE TABLE c2(a, b, FOREIGN KEY (a, b) REFERENCES p1)";
        assert!(rename_references_column(sql, "p1", "d", "reasonable").is_none());
    }

    #[test]
    fn rename_references_column_ignores_other_parent() {
        let sql = "CREATE TABLE c(a, FOREIGN KEY (a) REFERENCES q(d))";
        assert!(rename_references_column(sql, "p1", "d", "x").is_none());
    }

    // DROP COLUMN — byte-for-byte against sqlite3 3.51.0 (verified manually).

    #[test]
    fn drop_column_last_multiline_matches_sqlite() {
        // sqlite3: removes `,\n  c   INTEGER\n` (preceding comma to the `)`).
        let sql = "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT,\n  c   INTEGER\n)";
        let out = drop_column(sql, "c").unwrap();
        assert_eq!(out, "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT)");
    }

    #[test]
    fn drop_column_first_matches_sqlite() {
        let sql = "CREATE TABLE t (a INTEGER, b TEXT, c INTEGER)";
        let out = drop_column(sql, "a").unwrap();
        assert_eq!(out, "CREATE TABLE t (b TEXT, c INTEGER)");
    }

    #[test]
    fn drop_column_middle_matches_sqlite() {
        let sql = "CREATE TABLE t (a INTEGER, b TEXT, c INTEGER)";
        let out = drop_column(sql, "b").unwrap();
        assert_eq!(out, "CREATE TABLE t (a INTEGER, c INTEGER)");
    }

    #[test]
    fn drop_column_middle_multiline_matches_sqlite() {
        let sql = "CREATE TABLE t (\n  a INTEGER,\n  b TEXT,\n  c INTEGER\n)";
        let out = drop_column(sql, "b").unwrap();
        assert_eq!(out, "CREATE TABLE t (\n  a INTEGER,\n  c INTEGER\n)");
    }

    #[test]
    fn drop_column_first_with_spaces_matches_sqlite() {
        let sql = "CREATE TABLE t (a INTEGER , b TEXT , c INTEGER)";
        let out = drop_column(sql, "a").unwrap();
        assert_eq!(out, "CREATE TABLE t (b TEXT , c INTEGER)");
    }

    #[test]
    fn drop_column_last_with_spaces_matches_sqlite() {
        let sql = "CREATE TABLE t (a INTEGER , b TEXT , c INTEGER)";
        let out = drop_column(sql, "c").unwrap();
        assert_eq!(out, "CREATE TABLE t (a INTEGER , b TEXT )");
    }

    #[test]
    fn drop_column_before_table_constraint_matches_sqlite() {
        // sqlite3: the column before a table-level constraint is the "last"
        // column; its preceding comma through the constraint's start is removed.
        let sql = "CREATE TABLE t (a INTEGER, b TEXT, c INTEGER, UNIQUE(a))";
        let out = drop_column(sql, "c").unwrap();
        assert_eq!(out, "CREATE TABLE t (a INTEGER, b TEXT, UNIQUE(a))");
    }

    #[test]
    fn drop_column_middle_with_table_constraint_matches_sqlite() {
        let sql = "CREATE TABLE t (a INTEGER, b TEXT, c INTEGER, UNIQUE(c))";
        let out = drop_column(sql, "b").unwrap();
        assert_eq!(out, "CREATE TABLE t (a INTEGER, c INTEGER, UNIQUE(c))");
    }

    #[test]
    fn drop_column_missing_returns_none() {
        let sql = "CREATE TABLE t (a INTEGER, b TEXT)";
        assert!(drop_column(sql, "zzz").is_none());
    }

    #[test]
    fn drop_column_only_column_returns_none() {
        let sql = "CREATE TABLE t (a INTEGER)";
        assert!(drop_column(sql, "a").is_none());
    }

    #[test]
    fn drop_column_quoted_name_target() {
        let sql = "CREATE TABLE t (\"a\" INTEGER, b TEXT)";
        let out = drop_column(sql, "a").unwrap();
        assert_eq!(out, "CREATE TABLE t (b TEXT)");
    }

    #[test]
    fn rename_table_result_reloads_through_splitter() {
        // The in-place RENAME TO output is double-quoted; ensure the result is
        // still a single well-formed CREATE TABLE statement (re-parseable on
        // reload, issue #5619/#5634).
        let sql = "CREATE TABLE t (a INTEGER, b TEXT)";
        let out = rename_table(sql, "t2").unwrap();
        assert_eq!(out, "CREATE TABLE \"t2\" (a INTEGER, b TEXT)");
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
        let coldef =
            extract_add_column_text("ALTER TABLE t ADD COLUMN d TEXT DEFAULT 'x'").unwrap();
        let out = append_column(create, &coldef).unwrap();
        assert_eq!(
            out,
            "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT\n, c INTEGER, d TEXT DEFAULT 'x')"
        );
    }
}
