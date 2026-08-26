//! Rewrite table references inside trigger definitions when a table is renamed
//! via `ALTER TABLE ... RENAME TO ...`.
//!
//! SQLite (with `legacy_alter_table=OFF`, the default) rewrites references to a
//! renamed table inside existing trigger bodies and updates the `sql` text stored
//! in `sqlite_master`/`sqlite_schema` accordingly, while otherwise preserving the
//! original `CREATE TRIGGER` text verbatim (only the renamed identifiers change,
//! and they are emitted double-quoted). See `altertrig.test`.
//!
//! This module implements a *targeted, token-level* rewrite that handles the
//! table-name-in-body cases covered by `altertrig.test`. It deliberately does
//! **not** attempt the full SQLite name-resolver: column renames inside trigger
//! bodies (which require schema-aware resolution of unqualified columns) are out
//! of scope here and tracked as a follow-on.
//!
//! Rewrite scope (what counts as a reference to the renamed *table*):
//! - an identifier in a FROM/JOIN/INTO/UPDATE table position,
//! - an identifier that immediately follows a comma inside a FROM list,
//! - an identifier used as a qualifier (e.g. `t4.col` -> `"abc".col`),
//! - the trigger's own `ON <table>` target (the header table).
//!
//! Matched identifiers are replaced in place with the double-quoted new name,
//! preserving all surrounding whitespace and punctuation verbatim.

use vibesql_parser::{Keyword, Lexer, Span, Token};

/// Rewrite occurrences of `old_table` (case-insensitively) that are *table
/// references* inside the given `CREATE TRIGGER` SQL text, replacing them with
/// the double-quoted `new_table`.
///
/// Returns the rewritten SQL. If the text cannot be tokenized (it should always
/// be tokenizable since it round-tripped through the parser already) the input
/// is returned unchanged.
pub fn rewrite_table_refs_in_trigger_sql(sql: &str, old_table: &str, new_table: &str) -> String {
    let tokens = match Lexer::new(sql).tokenize_with_spans() {
        Ok(t) => t,
        Err(_) => return sql.to_string(),
    };

    // Collect the spans of identifier tokens that should be rewritten. `true`
    // enables the "first ON is the trigger's own header target" heuristic,
    // which only applies to `CREATE TRIGGER ... ON <table> ...` text.
    let edits = collect_table_ref_spans(&tokens, old_table, true);
    if edits.is_empty() {
        return sql.to_string();
    }

    apply_edits(sql, &edits, new_table)
}

/// Rewrite occurrences of `old_table` (case-insensitively) that are *table
/// references* inside the given `CREATE VIEW` SQL text, replacing them with the
/// double-quoted `new_table`.
///
/// Unlike [`rewrite_table_refs_in_trigger_sql`], this does **not** apply the
/// "first `ON` keyword is a header target" heuristic — a `CREATE VIEW ... AS
/// SELECT` body has no such header, and its first `ON` (if any) belongs to a
/// `JOIN ... ON` condition, whose right-hand identifiers are columns, not table
/// references (issue #6303).
///
/// Returns the rewritten SQL. If the text cannot be tokenized (it should always
/// be tokenizable since it round-tripped through the parser already) the input
/// is returned unchanged.
pub fn rewrite_table_refs_in_view_sql(sql: &str, old_table: &str, new_table: &str) -> String {
    let tokens = match Lexer::new(sql).tokenize_with_spans() {
        Ok(t) => t,
        Err(_) => return sql.to_string(),
    };

    let edits = collect_table_ref_spans(&tokens, old_table, false);
    if edits.is_empty() {
        return sql.to_string();
    }

    apply_edits(sql, &edits, new_table)
}

/// Apply replacement edits (sorted, non-overlapping spans) to `sql`, replacing
/// each span's text with the double-quoted `new_table`.
fn apply_edits(sql: &str, edits: &[Span], new_table: &str) -> String {
    let replacement = format!("\"{}\"", new_table);
    let mut out = String::with_capacity(sql.len() + edits.len() * (replacement.len() + 2));
    let mut cursor = 0usize;
    for span in edits {
        if span.start < cursor {
            // Overlapping/out-of-order span; skip defensively.
            continue;
        }
        out.push_str(&sql[cursor..span.start]);
        out.push_str(&replacement);
        cursor = span.end;
    }
    out.push_str(&sql[cursor..]);
    out
}

/// Walk the token stream and return the spans of identifier tokens that are
/// references to `old_table` and should be rewritten.
///
/// `treat_first_on_as_header` enables the `CREATE TRIGGER ... ON <table>`
/// header heuristic (the first `ON` keyword introduces the trigger's own
/// subject table, not a JOIN condition). Callers rewriting non-trigger SQL
/// (e.g. `CREATE VIEW` bodies, which have no such header) must pass `false` so
/// a `JOIN ... ON` condition is not mistaken for a header target.
fn collect_table_ref_spans(
    tokens: &[(Token, Span)],
    old_table: &str,
    treat_first_on_as_header: bool,
) -> Vec<Span> {
    let mut spans = Vec::new();

    // Per-paren-depth flag: are we currently inside a FROM list (so a comma
    // separates table references)? Index 0 is the top level.
    let mut from_list_stack: Vec<bool> = vec![false];

    // Track whether we have already consumed the trigger header's `ON <table>`
    // target. The header `ON` introduces the trigger's table; any later `ON`
    // belongs to a JOIN condition (where the following identifier is a column,
    // handled instead by qualifier detection).
    let mut seen_header_on = false;
    let mut expect_header_table = false;

    let significant: Vec<usize> =
        (0..tokens.len()).filter(|&i| !matches!(tokens[i].0, Token::Eof)).collect();

    for (pos, &idx) in significant.iter().enumerate() {
        let (tok, span) = &tokens[idx];

        match tok {
            Token::LParen => {
                // A new scope: subqueries reset FROM-list tracking. Inherit
                // `false` so commas inside e.g. function args are not treated
                // as table separators until a FROM keyword appears.
                from_list_stack.push(false);
            }
            Token::RParen => {
                if from_list_stack.len() > 1 {
                    from_list_stack.pop();
                }
            }
            Token::Keyword { keyword, .. } => {
                update_from_list_for_keyword(*keyword, &mut from_list_stack);
                if treat_first_on_as_header && *keyword == Keyword::On && !seen_header_on {
                    seen_header_on = true;
                    expect_header_table = true;
                    continue;
                }
            }
            Token::Identifier(name) => {
                let is_match = name.eq_ignore_ascii_case(old_table);

                // Header `ON <table>` target.
                if expect_header_table {
                    expect_header_table = false;
                    if is_match {
                        spans.push(*span);
                    }
                    continue;
                }

                if is_match && is_table_position(tokens, &significant, pos, &from_list_stack) {
                    spans.push(*span);
                }
            }
            _ => {
                // Any non-identifier token following the header `ON` means the
                // target was already a (quoted) identifier or something else.
                expect_header_table = false;
            }
        }
    }

    spans
}

/// Update the FROM-list flag for the current paren scope based on a keyword.
fn update_from_list_for_keyword(keyword: Keyword, from_list_stack: &mut [bool]) {
    let top = from_list_stack.last_mut().expect("stack always has top level");
    match keyword {
        // Keywords that (re)open a table list at the current scope.
        Keyword::From | Keyword::Join | Keyword::Into | Keyword::Update => {
            *top = true;
        }
        // Keywords that end the FROM/table list.
        Keyword::Set
        | Keyword::Where
        | Keyword::Select
        | Keyword::Group
        | Keyword::Order
        | Keyword::Having
        | Keyword::On
        | Keyword::Using
        | Keyword::Values
        | Keyword::Limit => {
            *top = false;
        }
        _ => {}
    }
}

/// Decide whether the identifier at `significant[pos]` occupies a table-name
/// position (and therefore, when it matches the renamed table, is a reference
/// that should be rewritten).
fn is_table_position(
    tokens: &[(Token, Span)],
    significant: &[usize],
    pos: usize,
    from_list_stack: &[bool],
) -> bool {
    // Qualifier position: `<ident>.` — the identifier names a table/alias.
    if let Some(&next_idx) = significant.get(pos + 1) {
        if matches!(tokens[next_idx].0, Token::Symbol('.')) {
            return true;
        }
    }

    // Position based on the preceding significant token.
    if pos == 0 {
        return false;
    }
    let prev_idx = significant[pos - 1];
    match &tokens[prev_idx].0 {
        Token::Keyword { keyword, .. } => matches!(
            keyword,
            Keyword::From | Keyword::Join | Keyword::Into | Keyword::Update | Keyword::Table
        ),
        // A comma inside a FROM list separates table references.
        Token::Comma => *from_list_stack.last().unwrap_or(&false),
        _ => false,
    }
}

/// Extract the textual name from an identifier token, treating a
/// double-quoted `DelimitedIdentifier` (e.g. `"big c"`) exactly like a bare
/// `Identifier` for matching purposes — SQL identifier equality never depends
/// on how the identifier was *spelled* (quoted vs. bare), only on its text
/// (case-insensitive ASCII). Every column/table/qualifier/alias match in the
/// column-rename rewriter below must go through this so a delimited
/// identifier is exactly as visible as a bare one; without it, a column
/// reference written in double quotes (most commonly a *previous* rename's
/// output, since a name containing a space can only be spelled quoted) was
/// invisible to this token-level rewriter — which pattern-matched only
/// `Token::Identifier` — and silently left every quoted reference
/// unrewritten on the next `RENAME COLUMN` (altercol.test 16.2.3: renaming
/// `"big c"` again after an earlier rename produced it).
fn ident_name(tok: &Token) -> Option<&str> {
    match tok {
        Token::Identifier(s) | Token::DelimitedIdentifier(s) => Some(s.as_str()),
        _ => None,
    }
}

/// Rewrite references to a renamed *column* inside a `CREATE TRIGGER` SQL text.
///
/// When `ALTER TABLE <renamed_table> RENAME <old_column> TO <new_column>` runs,
/// SQLite (with `legacy_alter_table=OFF`) rewrites every reference inside trigger
/// bodies that resolves to `<renamed_table>.<old_column>`, while preserving the
/// rest of the `CREATE TRIGGER` text verbatim. Unlike table renames, the new
/// column name is emitted *unquoted* (e.g. `t2.c` -> `t2.abc`, `e` -> `abc`).
///
/// `table_has_column(table, column)` is used to resolve which FROM-clause table
/// an unqualified column belongs to (and to confirm the renamed table actually
/// has the old column). It should match SQLite identifier semantics
/// (case-insensitive ASCII).
///
/// This is a *targeted* resolver covering the cases exercised by
/// `altertrig.test` (qualified `table.col` / `alias.col` references and
/// unqualified columns whose only in-scope owner is the renamed table).
///
/// When an unqualified reference to the renamed column would be genuinely
/// *ambiguous* (the renamed table and another in-scope table both own a column
/// of that name), this returns `Err(column_name)`. SQLite
/// (`legacy_alter_table=OFF`) aborts the entire `ALTER TABLE ... RENAME COLUMN`
/// in that case with `error in trigger <name>: ambiguous column name: <col>`
/// and leaves the schema unchanged; the caller is responsible for surfacing
/// that error and not committing the rename.
///
/// When `new_old_refer_to_renamed_table` is true, a `new.<col>` / `old.<col>`
/// qualified reference is treated as belonging to the renamed table. This is
/// correct only when the trigger's own subject table *is* the renamed table
/// (the `NEW`/`OLD` pseudo-tables always alias the trigger's subject table), so
/// the caller must gate it on that. Views and triggers on other tables pass
/// `false` and leave `new.`/`old.` references untouched.
pub fn rewrite_column_refs_in_trigger_sql(
    sql: &str,
    renamed_table: &str,
    old_column: &str,
    new_column: &str,
    table_has_column: &dyn Fn(&str, &str) -> bool,
    new_old_refer_to_renamed_table: bool,
) -> Result<String, String> {
    let tokens = match Lexer::new(sql).tokenize_with_spans() {
        Ok(t) => t,
        Err(_) => return Ok(sql.to_string()),
    };

    let significant: Vec<usize> =
        (0..tokens.len()).filter(|&i| !matches!(tokens[i].0, Token::Eof)).collect();

    // Compute, for each FROM/UPDATE/INTO scope, the table references in scope so
    // an unqualified column can be resolved to its owning table.
    let scopes = collect_scope_tables(&tokens, &significant);

    // Collect the spans of the *column identifier* tokens that should be renamed.
    let mut edits: Vec<Span> = Vec::new();

    // Tracks the most recently seen `INSERT INTO <table>` target as the scan
    // proceeds left-to-right. `excluded.<col>` (inside an `ON CONFLICT ... DO
    // UPDATE` clause) always refers to a column of *that* INSERT's target
    // table — unlike NEW/OLD, which always alias the trigger's own subject
    // table, `excluded` can name a column of a *different* table than the one
    // the trigger fires ON (e.g. a trigger on t1 whose body does `INSERT INTO
    // t2 ... ON CONFLICT(d) DO UPDATE SET f = excluded.f`, altercol.test
    // 16.1.5/16.1.6). A trigger body has no expression-level nesting of
    // INSERT statements, so simple sequential tracking (last INTO wins) is
    // sufficient: any `excluded.<col>` reference is always preceded by its
    // own statement's `INTO <table>` by the time the scan reaches it.
    let mut current_insert_target: Option<&str> = None;

    for (pos, &idx) in significant.iter().enumerate() {
        if matches!(tokens[idx].0, Token::Keyword { keyword: Keyword::Into, .. }) {
            if let Some(&next_idx) = significant.get(pos + 1) {
                if let Some(target) = ident_name(&tokens[next_idx].0) {
                    current_insert_target = Some(target);
                }
            }
            continue;
        }

        let Some(name) = ident_name(&tokens[idx].0) else { continue };

        // Qualified reference: `<qualifier> . <name>`.
        if pos >= 2 {
            let dot_idx = significant[pos - 1];
            let qual_idx = significant[pos - 2];
            if matches!(tokens[dot_idx].0, Token::Symbol('.')) {
                if let Some(qualifier) = ident_name(&tokens[qual_idx].0) {
                    let is_new_old_ref = new_old_refer_to_renamed_table
                        && (qualifier.eq_ignore_ascii_case("new")
                            || qualifier.eq_ignore_ascii_case("old"));
                    let is_excluded_ref = qualifier.eq_ignore_ascii_case("excluded")
                        && current_insert_target
                            .is_some_and(|t| t.eq_ignore_ascii_case(renamed_table));
                    if name.eq_ignore_ascii_case(old_column)
                        && (is_new_old_ref
                            || is_excluded_ref
                            || qualifier_is_renamed_table(
                                qualifier,
                                renamed_table,
                                paren_scope_at(&tokens, &significant, pos),
                                &scopes,
                            ))
                    {
                        edits.push(tokens[idx].1);
                    }
                    continue;
                }
            }
        }

        // A name used as a qualifier itself (the token before the dot) is never a
        // column reference; skip it so we don't accidentally rename it.
        if let Some(&next_idx) = significant.get(pos + 1) {
            if matches!(tokens[next_idx].0, Token::Symbol('.')) {
                continue;
            }
        }

        // Unqualified reference: rename only if the renamed table is the in-scope
        // owner of this column.
        if name.eq_ignore_ascii_case(old_column) && is_column_position(&tokens, &significant, pos) {
            let scope_key = paren_scope_at(&tokens, &significant, pos);
            match resolve_unqualified_column(
                renamed_table,
                old_column,
                scope_key,
                &scopes,
                table_has_column,
            ) {
                Resolution::Rewrite => edits.push(tokens[idx].1),
                Resolution::Ambiguous => return Err(name.to_string()),
                Resolution::Skip => {}
            }
        }
    }

    if edits.is_empty() {
        return Ok(sql.to_string());
    }
    Ok(apply_column_edits(sql, &edits, new_column))
}

/// Outcome of resolving an unqualified column reference against the renamed
/// table within a trigger body scope.
enum Resolution {
    /// The reference resolves unambiguously to the renamed table's renamed
    /// column; rewrite it.
    Rewrite,
    /// The reference does not resolve to the renamed table (the renamed table is
    /// not in scope, or does not own the column); leave it untouched.
    Skip,
    /// The reference is ambiguous: the renamed table and another in-scope table
    /// both own a column of this name. SQLite aborts the ALTER here.
    Ambiguous,
}

/// A table reference appearing in a FROM/UPDATE/INTO clause: its real table name
/// and an optional alias.
#[derive(Clone)]
struct TableRef {
    table: String,
    alias: Option<String>,
}

/// Apply replacement edits, replacing each span with the new column name.
/// Spans are sorted defensively before application.
///
/// The replacement is double-quoted whenever `new_column` is not a safe bare
/// identifier (mirrors SQLite's `bQuote` rule for `RENAME COLUMN`, see
/// `crate::alter_rewrite::emit_renamed_ident`) — e.g. renaming a column to a
/// name containing a semicolon must quote every rewritten reference inside a
/// trigger/view body, not just the `CREATE TABLE` column definition itself
/// (altercol.test 8.4.1).
fn apply_column_edits(sql: &str, edits: &[Span], new_column: &str) -> String {
    let replacement = if crate::alter_rewrite::is_safe_bare_identifier(new_column) {
        new_column.to_string()
    } else {
        crate::alter_rewrite::quote_ident(new_column)
    };
    let mut sorted = edits.to_vec();
    sorted.sort_by_key(|s| s.start);
    let mut out = String::with_capacity(sql.len() + sorted.len() * replacement.len());
    let mut cursor = 0usize;
    for span in sorted {
        if span.start < cursor {
            continue;
        }
        out.push_str(&sql[cursor..span.start]);
        out.push_str(&replacement);
        cursor = span.end;
    }
    out.push_str(&sql[cursor..]);
    out
}

/// The scope key for a token is the index of the innermost still-open `(` (or
/// `None` for the top level). Two references share a scope iff they are at the
/// same paren nesting opened by the same `(`.
fn paren_scope_at(tokens: &[(Token, Span)], significant: &[usize], pos: usize) -> Option<usize> {
    let target = significant[pos];
    let mut idx_stack: Vec<usize> = Vec::new();
    for (i, (tok, _)) in tokens.iter().enumerate().take(target) {
        match tok {
            Token::LParen => idx_stack.push(i),
            Token::RParen => {
                idx_stack.pop();
            }
            _ => {}
        }
    }
    idx_stack.last().copied()
}

/// Build a map from scope key (enclosing `(` index, or `None` for top level) to
/// the list of table references in that scope's FROM/UPDATE/INTO clause.
fn collect_scope_tables(
    tokens: &[(Token, Span)],
    significant: &[usize],
) -> std::collections::HashMap<Option<usize>, Vec<TableRef>> {
    use std::collections::HashMap;
    let mut scopes: HashMap<Option<usize>, Vec<TableRef>> = HashMap::new();

    // Track the open-paren index stack so we can attribute table refs to scopes.
    let mut paren_stack: Vec<usize> = Vec::new();
    // Per-scope "are we currently inside a table list?" flag.
    let mut in_table_list: Vec<bool> = vec![false];

    let mut pos = 0usize;
    while pos < significant.len() {
        let idx = significant[pos];
        match &tokens[idx].0 {
            Token::LParen => {
                paren_stack.push(idx);
                in_table_list.push(false);
            }
            Token::RParen => {
                if paren_stack.pop().is_some() {
                    in_table_list.pop();
                }
            }
            Token::Keyword { keyword, .. } => {
                let top = in_table_list.last_mut().expect("stack has top");
                match keyword {
                    Keyword::From | Keyword::Into | Keyword::Update | Keyword::Join => {
                        *top = true;
                    }
                    Keyword::Set
                    | Keyword::Where
                    | Keyword::Select
                    | Keyword::Group
                    | Keyword::Order
                    | Keyword::Having
                    | Keyword::On
                    | Keyword::Using
                    | Keyword::Values
                    | Keyword::Limit => {
                        *top = false;
                    }
                    _ => {}
                }
            }
            tok if *in_table_list.last().unwrap_or(&false) && ident_name(tok).is_some() => {
                let name = ident_name(tok).expect("checked by guard");
                // This identifier is a table name in the current scope's table
                // list (it is not a qualifier and not preceded by a `.`).
                let preceded_by_dot =
                    pos > 0 && matches!(tokens[significant[pos - 1]].0, Token::Symbol('.'));
                let followed_by_dot = significant
                    .get(pos + 1)
                    .is_some_and(|&n| matches!(tokens[n].0, Token::Symbol('.')));
                if !preceded_by_dot && !followed_by_dot {
                    let table = name.to_string();
                    // Look ahead for an optional alias: `AS x` or bare `x`.
                    let alias = parse_alias(tokens, significant, pos);
                    let scope_key = paren_stack.last().copied();
                    scopes.entry(scope_key).or_default().push(TableRef { table, alias });
                }
            }
            _ => {}
        }
        pos += 1;
    }

    scopes
}

/// Detect an alias following a table reference at `significant[pos]`:
/// `t AS x` or the bare form `t x`. Returns None if the next token is a keyword
/// (e.g. JOIN, WHERE) or punctuation.
fn parse_alias(tokens: &[(Token, Span)], significant: &[usize], pos: usize) -> Option<String> {
    let mut next = pos + 1;
    if let Some(&nidx) = significant.get(next) {
        if matches!(tokens[nidx].0, Token::Keyword { keyword: Keyword::As, .. }) {
            next += 1;
        }
    }
    if let Some(&nidx) = significant.get(next) {
        if let Some(alias) = ident_name(&tokens[nidx].0) {
            if next == pos + 1 || next == pos + 2 {
                return Some(alias.to_string());
            }
        }
    }
    None
}

/// Does `qualifier` (a `table.col` prefix) refer to the renamed table, either by
/// its real name or via an alias bound in the reference's scope (or top level)?
fn qualifier_is_renamed_table(
    qualifier: &str,
    renamed_table: &str,
    scope_key: Option<usize>,
    scopes: &std::collections::HashMap<Option<usize>, Vec<TableRef>>,
) -> bool {
    if qualifier.eq_ignore_ascii_case(renamed_table) {
        return true;
    }
    for key in [scope_key, None] {
        if let Some(refs) = scopes.get(&key) {
            for r in refs {
                if let Some(alias) = &r.alias {
                    if alias.eq_ignore_ascii_case(qualifier)
                        && r.table.eq_ignore_ascii_case(renamed_table)
                    {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// For an unqualified column, decide how it resolves against the renamed table:
/// - `Rewrite` when the renamed table is in scope, owns the old column, and no *other* in-scope
///   table also owns a column of that name;
/// - `Ambiguous` when the renamed table is in scope and owns the column but at least one other
///   in-scope table also owns a column of that name (SQLite aborts the ALTER here);
/// - `Skip` otherwise (the renamed table is not in scope or does not own the column, so the
///   reference is unrelated to the rename).
fn resolve_unqualified_column(
    renamed_table: &str,
    old_column: &str,
    scope_key: Option<usize>,
    scopes: &std::collections::HashMap<Option<usize>, Vec<TableRef>>,
    table_has_column: &dyn Fn(&str, &str) -> bool,
) -> Resolution {
    if !table_has_column(renamed_table, old_column) {
        return Resolution::Skip;
    }

    let mut renamed_in_scope = false;
    let mut other_owner = false;
    for key in [scope_key, None] {
        if let Some(refs) = scopes.get(&key) {
            for r in refs {
                if r.table.eq_ignore_ascii_case(renamed_table) {
                    renamed_in_scope = true;
                } else if table_has_column(&r.table, old_column) {
                    other_owner = true;
                }
            }
        }
        // Only consult the top-level scope if the inner scope didn't bring the
        // renamed table into view; this mirrors innermost-first resolution.
        if renamed_in_scope {
            break;
        }
    }

    match (renamed_in_scope, other_owner) {
        (true, false) => Resolution::Rewrite,
        (true, true) => Resolution::Ambiguous,
        (false, _) => Resolution::Skip,
    }
}

/// Decide whether the identifier at `significant[pos]` is in a position where it
/// could be a column reference (i.e. not itself a table reference in a FROM
/// list). We treat it as a column unless it immediately follows
/// FROM/JOIN/INTO/UPDATE/TABLE/AS.
fn is_column_position(tokens: &[(Token, Span)], significant: &[usize], pos: usize) -> bool {
    if pos == 0 {
        return true;
    }
    let prev_idx = significant[pos - 1];
    match &tokens[prev_idx].0 {
        Token::Keyword { keyword, .. } => !matches!(
            keyword,
            Keyword::From
                | Keyword::Join
                | Keyword::Into
                | Keyword::Update
                | Keyword::Table
                | Keyword::As
        ),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewrite(sql: &str, old: &str, new: &str) -> String {
        rewrite_table_refs_in_trigger_sql(sql, old, new)
    }

    /// Test helper schema matching altertrig.test fixtures: t1(a,b), t2(c,d),
    /// t3(e,f), t4(e,f).
    fn altertrig_schema(table: &str, column: &str) -> bool {
        let cols: &[&str] = match table.to_ascii_lowercase().as_str() {
            "t1" => &["a", "b"],
            "t2" => &["c", "d"],
            "t3" | "t4" => &["e", "f"],
            _ => &[],
        };
        cols.iter().any(|c| c.eq_ignore_ascii_case(column))
    }

    fn rewrite_col(sql: &str, table: &str, old: &str, new: &str) -> String {
        rewrite_column_refs_in_trigger_sql(sql, table, old, new, &altertrig_schema, true)
            .expect("rewrite should not be ambiguous in this fixture")
    }

    fn rewrite_col_result(sql: &str, table: &str, old: &str, new: &str) -> Result<String, String> {
        rewrite_column_refs_in_trigger_sql(sql, table, old, new, &altertrig_schema, true)
    }

    #[test]
    fn col_2_3_unqualified_in_nested_subquery() {
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n      UPDATE t1 SET a='xyz' FROM t3, (SELECT * FROM (SELECT e FROM t3));\n    END";
        let got = rewrite_col(sql, "t3", "e", "abc");
        assert_eq!(
            got,
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n      UPDATE t1 SET a='xyz' FROM t3, (SELECT * FROM (SELECT abc FROM t3));\n    END"
        );
    }

    #[test]
    fn col_2_4_unqualified_in_where() {
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n      UPDATE t1 SET a='xyz' FROM t3, (SELECT 1 FROM t2 WHERE c);\n    END";
        let got = rewrite_col(sql, "t2", "c", "abc");
        assert_eq!(
            got,
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n      UPDATE t1 SET a='xyz' FROM t3, (SELECT 1 FROM t2 WHERE abc);\n    END"
        );
    }

    /// altercol.test 8.4.1: renaming a column to a name that is not a safe
    /// bare identifier (contains a semicolon) must double-quote every
    /// rewritten reference inside a view/trigger body, not leave it bare.
    #[test]
    fn col_rename_to_unsafe_identifier_is_quoted() {
        let sql = "CREATE VIEW vvv AS SELECT c+c || coalesce(c, c) FROM t2, t3 WHERE d=c GROUP BY c HAVING c>0";
        let got =
            rewrite_column_refs_in_trigger_sql(sql, "t2", "c", "a;b", &altertrig_schema, false)
                .expect("rewrite should not be ambiguous in this fixture");
        assert_eq!(
            got,
            "CREATE VIEW vvv AS SELECT \"a;b\"+\"a;b\" || coalesce(\"a;b\", \"a;b\") FROM t2, t3 WHERE d=\"a;b\" GROUP BY \"a;b\" HAVING \"a;b\">0"
        );
    }

    #[test]
    fn col_2_5_qualified_before_from() {
        let sql =
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n      UPDATE t1 SET a=t2.c FROM t2;\n    END";
        let got = rewrite_col(sql, "t2", "c", "abc");
        assert_eq!(
            got,
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n      UPDATE t1 SET a=t2.abc FROM t2;\n    END"
        );
    }

    #[test]
    fn col_2_6_qualified_multi_from() {
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n      UPDATE t1 SET a=t2.c FROM t2, t3;\n    END";
        let got = rewrite_col(sql, "t2", "c", "abc");
        assert_eq!(
            got,
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n      UPDATE t1 SET a=t2.abc FROM t2, t3;\n    END"
        );
    }

    #[test]
    fn col_2_7_qualified_natural_join() {
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n      UPDATE t1 SET a=1 FROM t3 NATURAL JOIN t4 WHERE t4.e=a;\n    END";
        let got = rewrite_col(sql, "t4", "e", "abc");
        assert_eq!(
            got,
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n      UPDATE t1 SET a=1 FROM t3 NATURAL JOIN t4 WHERE t4.abc=a;\n    END"
        );
    }

    #[test]
    fn col_does_not_touch_qualifier_table_name() {
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t1 SET a=t2.c FROM t2; END";
        let got = rewrite_col(sql, "t2", "c", "abc");
        assert!(got.contains("t2.abc"));
        assert!(got.contains("FROM t2;"));
    }

    #[test]
    fn col_alias_qualifier_resolves() {
        let sql =
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t1 SET a=1 FROM t3 AS x WHERE x.e=1; END";
        let got = rewrite_col(sql, "t3", "e", "abc");
        assert_eq!(
            got,
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t1 SET a=1 FROM t3 AS x WHERE x.abc=1; END"
        );
    }

    #[test]
    fn col_no_match_returns_unchanged() {
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t1 SET a=1 WHERE b=1; END";
        assert_eq!(rewrite_col(sql, "t2", "c", "abc"), sql);
    }

    #[test]
    fn col_unqualified_other_table_not_in_scope_unchanged() {
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t1 SET a=1 FROM t3 WHERE e=1; END";
        assert_eq!(rewrite_col(sql, "t2", "c", "abc"), sql);
    }

    #[test]
    fn col_ambiguous_unqualified_errors() {
        // Both t3 and t4 own column `e`, both in scope (UPDATE t3 ... FROM t4),
        // and the unqualified `e` in WHERE is ambiguous. sqlite3 3.51.0
        // (legacy_alter_table=OFF) aborts the ALTER with "ambiguous column
        // name: e", so the rewriter must surface an error rather than silently
        // skip. Renaming either table's `e` is ambiguous.
        let sql = "CREATE TRIGGER r1 INSERT ON t3 BEGIN UPDATE t3 SET e=1 FROM t4 WHERE e=2; END";
        let err = rewrite_col_result(sql, "t3", "e", "abc").expect_err("should be ambiguous");
        assert_eq!(err, "e");
        // Symmetric: renaming t4.e is ambiguous in the same trigger body.
        let err = rewrite_col_result(sql, "t4", "e", "abc").expect_err("should be ambiguous");
        assert_eq!(err, "e");
    }

    #[test]
    fn col_ambiguity_only_when_renamed_table_in_scope() {
        // The renamed table (t2) is not in scope, so the unqualified `e` is not
        // ambiguous with respect to this rename: skip (no error, no change).
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t3 SET e=1 FROM t4 WHERE e=2; END";
        assert_eq!(rewrite_col_result(sql, "t2", "c", "abc").unwrap(), sql);
    }

    #[test]
    fn col_qualified_ref_not_ambiguous() {
        // A qualified `t3.e` is unambiguous even though t4 also owns `e`; it
        // must rewrite (not error).
        let sql =
            "CREATE TRIGGER r1 INSERT ON t3 BEGIN UPDATE t3 SET a=1 FROM t4 WHERE t3.e=2; END";
        assert_eq!(
            rewrite_col_result(sql, "t3", "e", "abc").unwrap(),
            "CREATE TRIGGER r1 INSERT ON t3 BEGIN UPDATE t3 SET a=1 FROM t4 WHERE t3.abc=2; END"
        );
    }

    /// altercol.test 16.1.5: a trigger ON t1 whose body INSERTs into a
    /// *different* table (t3) with an `ON CONFLICT ... DO UPDATE SET
    /// f=excluded.f` clause. Renaming t3.f must rewrite `excluded.f` (the
    /// INSERT's own target-table pseudo-row), even though the trigger's own
    /// subject table is t1, not t3 — `excluded` is not gated by
    /// `new_old_refer_to_renamed_table`.
    #[test]
    fn col_excluded_ref_to_insert_target_table() {
        let sql = "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN \
                    INSERT INTO t3 VALUES(new.a, new.b, new.c) \
                    ON CONFLICT(e) DO UPDATE SET f = excluded.f; END";
        let got =
            rewrite_column_refs_in_trigger_sql(sql, "t3", "f", "big_f", &altertrig_schema, false)
                .expect("rewrite should not be ambiguous in this fixture");
        assert_eq!(
            got,
            "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN \
             INSERT INTO t3 VALUES(new.a, new.b, new.c) \
             ON CONFLICT(e) DO UPDATE SET big_f = excluded.big_f; END"
        );
    }

    /// `excluded.<col>` only refers to the *current* INSERT's target table;
    /// a rename of an unrelated table must leave it untouched.
    #[test]
    fn col_excluded_ref_unrelated_table_unchanged() {
        let sql = "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN \
                    INSERT INTO t3 VALUES(new.a, new.b, new.c) \
                    ON CONFLICT(e) DO UPDATE SET f = excluded.f; END";
        assert_eq!(
            rewrite_column_refs_in_trigger_sql(sql, "t2", "f", "big_f", &altertrig_schema, false)
                .unwrap(),
            sql
        );
    }

    /// altercol.test 16.2.3: renaming a column whose *current* name is a
    /// double-quoted delimited identifier containing a space (the only way to
    /// spell such a name — e.g. the result of an earlier rename to `"big c"`)
    /// must still be recognized as a reference to `old_column`. Before the
    /// `ident_name` fix this token-level rewriter pattern-matched only
    /// `Token::Identifier`, so a `Token::DelimitedIdentifier` reference was
    /// invisible and silently left unrewritten.
    #[test]
    fn col_delimited_identifier_unqualified_is_rewritten() {
        let has_big_c =
            |t: &str, c: &str| t.eq_ignore_ascii_case("t1") && c.eq_ignore_ascii_case("big c");
        let sql = "CREATE VIEW v5 AS SELECT \"big c\" FROM t1";
        let got =
            rewrite_column_refs_in_trigger_sql(sql, "t1", "big c", "reallybigc", &has_big_c, false)
                .expect("rewrite should not be ambiguous in this fixture");
        assert_eq!(got, "CREATE VIEW v5 AS SELECT reallybigc FROM t1");
    }

    /// Same as above but the delimited identifier is table-qualified
    /// (`t1."big c"`) — the qualifier-before-dot lookup must also recognize a
    /// `DelimitedIdentifier` qualifier.
    #[test]
    fn col_delimited_identifier_qualified_is_rewritten() {
        let has_big_c =
            |t: &str, c: &str| t.eq_ignore_ascii_case("t1") && c.eq_ignore_ascii_case("big c");
        let sql = "CREATE VIEW v5 AS SELECT t1.\"big c\" FROM t1";
        let got =
            rewrite_column_refs_in_trigger_sql(sql, "t1", "big c", "reallybigc", &has_big_c, false)
                .expect("rewrite should not be ambiguous in this fixture");
        assert_eq!(got, "CREATE VIEW v5 AS SELECT t1.reallybigc FROM t1");
    }

    #[test]
    fn rewrites_table_after_comma_in_from_list() {
        // altertrig.test 1.1
        let sql =
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n  UPDATE t1 SET d='xyz' FROM t2, t3; \nEND";
        let got = rewrite(sql, "t3", "t5");
        assert_eq!(
            got,
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n  UPDATE t1 SET d='xyz' FROM t2, \"t5\"; \nEND"
        );
    }

    #[test]
    fn rewrites_table_in_subquery_from() {
        // altertrig.test 1.3
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n  UPDATE t1 SET d='xyz' FROM t2, (SELECT * FROM t5); \nEND";
        let got = rewrite(sql, "t5", "t3");
        assert_eq!(
            got,
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n  UPDATE t1 SET d='xyz' FROM t2, (SELECT * FROM \"t3\"); \nEND"
        );
    }

    #[test]
    fn rewrites_multiple_table_refs_nested() {
        // altertrig.test 2.2: both t3 refs rewritten, column `e` untouched.
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n  UPDATE t1 SET a='xyz' FROM t3, (SELECT * FROM (SELECT e FROM t3)); \nEND";
        let got = rewrite(sql, "t3", "t10");
        assert_eq!(
            got,
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n  UPDATE t1 SET a='xyz' FROM \"t10\", (SELECT * FROM (SELECT e FROM \"t10\")); \nEND"
        );
    }

    #[test]
    fn rewrites_qualifier_and_join_target() {
        // altertrig.test 2.8: t4 after NATURAL JOIN and as qualifier t4.e.
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n  UPDATE t1 SET a=1 FROM t3 NATURAL JOIN t4 WHERE t4.e=a; \nEND";
        let got = rewrite(sql, "t4", "abc");
        assert_eq!(
            got,
            "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n  UPDATE t1 SET a=1 FROM t3 NATURAL JOIN \"abc\" WHERE \"abc\".e=a; \nEND"
        );
    }

    #[test]
    fn does_not_rewrite_unrelated_column_of_same_name() {
        // A bare column reference named like the renamed table must not change.
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN \n  UPDATE t1 SET a=1 WHERE z=1; \nEND";
        let got = rewrite(sql, "z", "zz");
        assert_eq!(got, sql);
    }

    #[test]
    fn rewrites_header_on_table() {
        // If the trigger's own ON-table is renamed, the header is rewritten.
        let sql = "CREATE TRIGGER r1 INSERT ON t3 BEGIN \n  UPDATE t1 SET a=1; \nEND";
        let got = rewrite(sql, "t3", "t5");
        assert_eq!(got, "CREATE TRIGGER r1 INSERT ON \"t5\" BEGIN \n  UPDATE t1 SET a=1; \nEND");
    }

    #[test]
    fn no_match_returns_input_unchanged() {
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t1 SET a=1; END";
        assert_eq!(rewrite(sql, "nosuch", "x"), sql);
    }

    #[test]
    fn case_insensitive_match() {
        let sql = "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t1 SET a=1 FROM T3; END";
        let got = rewrite(sql, "t3", "t5");
        assert_eq!(got, "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t1 SET a=1 FROM \"t5\"; END");
    }
}
