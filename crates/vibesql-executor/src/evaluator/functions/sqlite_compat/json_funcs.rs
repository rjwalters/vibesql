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
                    let n: usize =
                        chars[start..i].iter().collect::<String>().parse().map_err(|_| bad())?;
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
pub(crate) fn navigate<'a>(
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
///
/// Strategy (in order):
///   1. Strict `serde_json` — the fast, exact path for canonical JSON. With
///      `arbitrary_precision` it preserves the source number token verbatim
///      (so `json('1.50')` round-trips to `1.50`, matching SQLite).
///   2. A JSON5 → strict-JSON pre-processor ([`json5_to_json`]) that normalizes
///      only the JSON5-specific surface (unquoted keys, single-quoted and
///      multi-line strings, comments, trailing commas, and the number
///      extensions: hex, leading/trailing decimal points, explicit `+`, and
///      `Infinity`/`NaN`). Number tokens are rewritten to the *minimal* valid
///      JSON form SQLite emits — e.g. `.5e3` → `0.5e3`, `4.e0` → `4.0e0`,
///      `0xABCDEF` → `11259375`, `Infinity` → `9e999` — and then handed to
///      `serde_json`, which (again via `arbitrary_precision`) preserves that
///      exact token. This reproduces SQLite's number rendering, which the
///      `json5` crate cannot because it round-trips every number through `f64`.
/// The pre-processor is authoritative for the relaxed grammar: we deliberately
/// do *not* fall back to the `json5` crate, because it accepts constructs SQLite
/// rejects (e.g. leading-zero integers like `-01`, which must be malformed so
/// `json_error_position`/`json_valid` match SQLite).
pub(crate) fn parse_json_relaxed(s: &str) -> Result<serde_json::Value, ()> {
    // Both parses go through [`parse_strict_json_bounded`] rather than
    // `serde_json::from_str` so that documents nested deeper than serde_json's
    // hard-coded 128-level cap (but within SQLITE_MAX_JSON_DEPTH = 1000) parse
    // successfully instead of being spuriously rejected (issue #6052). The
    // rewritten text has already been depth-bounded to <= 1000 by the
    // `Json5Rewriter`, and the raw attempt is independently depth-gated inside
    // `parse_strict_json_bounded`, so neither path lets serde recurse unbounded.
    if let Ok(v) = parse_strict_json_bounded(s) {
        return Ok(v);
    }
    if let Some(rewritten) = json5_to_json(s) {
        if let Ok(v) = parse_strict_json_bounded(&rewritten) {
            return Ok(v);
        }
    }
    Err(())
}

/// Rewrite a SQLite-relaxed / JSON5 document into strict JSON text, or return
/// `None` if the input is not well-formed under the relaxed grammar.
///
/// The output is fed to `serde_json` (with `arbitrary_precision`), so number
/// tokens survive verbatim — the key reason this exists rather than deferring
/// to the `json5` crate, which collapses every number to an `f64`.
///
/// Handled JSON5 surface: `//` line and `/* */` block comments; single- and
/// double-quoted strings with JSON5 escapes (`\'`, `\v`, `\0`, `\xHH`, and
/// backslash-newline line continuations, including U+2028/U+2029); unquoted
/// ECMAScript identifier object keys; trailing commas; and the number
/// extensions (hex, leading/trailing `.`, explicit `+`, `Infinity`, `NaN`).
fn json5_to_json(s: &str) -> Option<String> {
    let chars: Vec<char> = s.chars().collect();
    let mut w =
        Json5Rewriter { chars: &chars, i: 0, out: String::with_capacity(s.len()), depth: 0 };
    w.skip_trivia();
    w.rewrite_value()?;
    w.skip_trivia();
    if w.i != w.chars.len() {
        return None; // trailing junk
    }
    Some(w.out)
}

/// Maximum object/array nesting depth accepted by the JSON5 rewriter.
///
/// `rewrite_value` → `rewrite_object`/`rewrite_array` → `rewrite_value` recurses
/// once per nesting level; without a cap a deeply-nested document would overflow
/// the thread stack and abort the whole process (a Rust stack overflow is not
/// catchable). Matching SQLite's `SQLITE_MAX_JSON_DEPTH` (default 1000) keeps
/// `json_valid`/`json_error_position` conformant — SQLite reports "malformed
/// JSON" beyond this depth — while making the rewriter bounded. On exceeding the
/// cap the rewriter returns `None` (rewrite failure), so callers observe the same
/// "malformed JSON" outcome (`json_valid` → 0) rather than a crash.
const MAX_JSON5_DEPTH: usize = 1000;

/// Return the maximum object/array nesting depth of a strict-JSON text, or
/// `None` if that depth would exceed [`MAX_JSON5_DEPTH`].
///
/// This is a **non-recursive** (iterative) scan: it walks the bytes tracking a
/// single depth counter, so it cannot itself overflow the stack no matter how
/// deeply nested the input is. Brackets inside string literals are ignored by
/// skipping over `"..."` runs (honoring backslash escapes).
///
/// Callers use it as a stack-safety gate *before* handing text to
/// `serde_json` with its recursion limit disabled (see
/// [`parse_strict_json_bounded`] / [`strict_json_is_valid_bounded`]): serde_json
/// deserializes by recursing once per nesting level on the native call stack, so
/// we must independently bound depth to <= [`MAX_JSON5_DEPTH`] before disabling
/// its built-in 128-level guard. Because this check runs on the raw input, serde
/// never recurses on unbounded untrusted nesting (issue #6052).
fn json_nesting_within_cap(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut depth: usize = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                // Skip the string body, respecting backslash escapes, so that
                // brackets inside strings do not affect the depth counter.
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 2, // skip the escaped byte
                        b'"' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
            }
            b'{' | b'[' => {
                depth += 1;
                if depth > MAX_JSON5_DEPTH {
                    return false;
                }
                i += 1;
            }
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            _ => i += 1,
        }
    }
    true
}

/// Parse strict JSON text allowing nesting up to [`MAX_JSON5_DEPTH`] (1000),
/// rather than serde_json's hard-coded 128-level cap.
///
/// serde_json's `Deserializer` caps recursion at 128 and that limit is only
/// liftable via `disable_recursion_limit()` (gated behind the `unbounded_depth`
/// feature). Disabling it removes *all* depth checking, so we take two
/// precautions:
///   1. Run the non-recursive [`json_nesting_within_cap`] gate first: nesting
///      beyond 1000 is rejected here (mirroring SQLite's `SQLITE_MAX_JSON_DEPTH`)
///      *before* serde ever recurses, so serde sees at most 1000 levels on
///      already-bounded input — never unbounded on untrusted input.
///   2. Even 1000-deep recursion overflows an ordinary native stack (serde
///      builds a map/vec per level), so we route serde's recursion through
///      [`serde_stacker`], which grows the stack on the heap. This is the
///      pairing serde_json's own docs recommend for `disable_recursion_limit()`.
/// (issue #6052)
fn parse_strict_json_bounded(s: &str) -> Result<serde_json::Value, ()> {
    if !json_nesting_within_cap(s) {
        return Err(());
    }
    let mut de = serde_json::Deserializer::from_str(s);
    de.disable_recursion_limit();
    let stacker = serde_stacker::Deserializer::new(&mut de);
    let value = <serde_json::Value as serde::Deserialize>::deserialize(stacker).map_err(|_| ())?;
    // Ensure the whole input was consumed (no trailing junk), matching
    // `serde_json::from_str`'s end-of-input check.
    de.end().map_err(|_| ())?;
    Ok(value)
}

/// Validity-only variant of [`parse_strict_json_bounded`]: `true` iff `s` is
/// well-formed canonical JSON with nesting <= [`MAX_JSON5_DEPTH`].
fn strict_json_is_valid_bounded(s: &str) -> bool {
    parse_strict_json_bounded(s).is_ok()
}

struct Json5Rewriter<'a> {
    chars: &'a [char],
    i: usize,
    out: String,
    /// Current object/array nesting depth; capped at [`MAX_JSON5_DEPTH`].
    depth: usize,
}

impl Json5Rewriter<'_> {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.i).copied()
    }

    /// Skip whitespace (including JSON5's extra Unicode spaces) and `//` / `/* */`
    /// comments. Returns `false` if an unterminated block comment is seen.
    fn skip_trivia(&mut self) -> bool {
        loop {
            match self.peek() {
                Some(c) if is_json5_ws(c) => self.i += 1,
                Some('/') if self.chars.get(self.i + 1) == Some(&'/') => {
                    self.i += 2;
                    while let Some(c) = self.peek() {
                        if c == '\n' || c == '\r' || c == '\u{2028}' || c == '\u{2029}' {
                            break;
                        }
                        self.i += 1;
                    }
                }
                Some('/') if self.chars.get(self.i + 1) == Some(&'*') => {
                    self.i += 2;
                    loop {
                        match self.peek() {
                            None => return false, // unterminated block comment
                            Some('*') if self.chars.get(self.i + 1) == Some(&'/') => {
                                self.i += 2;
                                break;
                            }
                            _ => self.i += 1,
                        }
                    }
                }
                _ => return true,
            }
        }
    }

    fn rewrite_value(&mut self) -> Option<()> {
        match self.peek()? {
            '{' | '[' => {
                // Containers are the only recursive case; guard nesting depth here
                // (the single recursion funnel) so a pathological document cannot
                // overflow the stack. Beyond the cap we return `None` — a rewrite
                // failure — matching SQLite's "malformed JSON" past
                // SQLITE_MAX_JSON_DEPTH.
                if self.depth >= MAX_JSON5_DEPTH {
                    return None;
                }
                self.depth += 1;
                let r = if self.peek() == Some('{') {
                    self.rewrite_object()
                } else {
                    self.rewrite_array()
                };
                self.depth -= 1;
                r
            }
            '"' | '\'' => self.rewrite_string(),
            _ => self.rewrite_scalar(),
        }
    }

    fn rewrite_object(&mut self) -> Option<()> {
        self.out.push('{');
        self.i += 1; // consume '{'
        self.skip_trivia();
        let mut first = true;
        loop {
            self.skip_trivia();
            match self.peek()? {
                '}' => {
                    self.i += 1;
                    self.out.push('}');
                    return Some(());
                }
                ',' if !first => {
                    // Trailing / separating comma: consume, then re-loop. A
                    // trailing comma before '}' is dropped by the '}' arm above.
                    self.i += 1;
                    self.skip_trivia();
                    if self.peek()? == '}' {
                        self.i += 1;
                        self.out.push('}');
                        return Some(());
                    }
                    self.out.push(',');
                    self.rewrite_member()?;
                    first = false;
                }
                _ if first => {
                    self.rewrite_member()?;
                    first = false;
                }
                _ => return None, // missing comma between members
            }
        }
    }

    fn rewrite_member(&mut self) -> Option<()> {
        self.skip_trivia();
        // Key: a quoted string or an unquoted identifier.
        match self.peek()? {
            '"' | '\'' => self.rewrite_string()?,
            _ => self.rewrite_identifier_key()?,
        }
        self.skip_trivia();
        if self.peek()? != ':' {
            return None;
        }
        self.i += 1;
        self.out.push(':');
        self.skip_trivia();
        self.rewrite_value()
    }

    /// Rewrite an unquoted ECMAScript IdentifierName object key as a quoted JSON
    /// string. The first character must be a letter, `_`, or `$`; subsequent
    /// characters additionally allow digits. (Non-ASCII letters are accepted, as
    /// SQLite accepts e.g. `MNO_123æxyz`.)
    fn rewrite_identifier_key(&mut self) -> Option<()> {
        let start = self.i;
        let mut first = true;
        while let Some(c) = self.peek() {
            let ok = if first {
                c == '_' || c == '$' || c.is_alphabetic()
            } else {
                c == '_' || c == '$' || c.is_alphanumeric()
            };
            if !ok {
                break;
            }
            first = false;
            self.i += 1;
        }
        if self.i == start {
            return None;
        }
        self.out.push('"');
        for &c in &self.chars[start..self.i] {
            match c {
                '"' => self.out.push_str("\\\""),
                '\\' => self.out.push_str("\\\\"),
                _ => self.out.push(c),
            }
        }
        self.out.push('"');
        Some(())
    }

    fn rewrite_array(&mut self) -> Option<()> {
        self.out.push('[');
        self.i += 1; // consume '['
        let mut first = true;
        loop {
            self.skip_trivia();
            match self.peek()? {
                ']' => {
                    self.i += 1;
                    self.out.push(']');
                    return Some(());
                }
                ',' if !first => {
                    self.i += 1;
                    self.skip_trivia();
                    if self.peek()? == ']' {
                        self.i += 1;
                        self.out.push(']');
                        return Some(());
                    }
                    self.out.push(',');
                    self.rewrite_value()?;
                    first = false;
                }
                _ if first => {
                    self.rewrite_value()?;
                    first = false;
                }
                _ => return None,
            }
        }
    }

    /// Rewrite a single- or double-quoted JSON5 string into a strict JSON
    /// double-quoted string, translating JSON5-only escapes.
    fn rewrite_string(&mut self) -> Option<()> {
        let quote = self.peek()?;
        self.i += 1;
        self.out.push('"');
        loop {
            let c = self.peek()?;
            match c {
                q if q == quote => {
                    self.i += 1;
                    self.out.push('"');
                    return Some(());
                }
                '"' => {
                    // A double quote inside a single-quoted string must be escaped.
                    self.out.push_str("\\\"");
                    self.i += 1;
                }
                '\\' => {
                    self.i += 1;
                    let e = self.peek()?;
                    match e {
                        // Line continuation: backslash + line terminator -> the
                        // newline is removed from the string value.
                        '\n' => self.i += 1,
                        '\r' => {
                            self.i += 1;
                            if self.peek() == Some('\n') {
                                self.i += 1; // CRLF
                            }
                        }
                        '\u{2028}' | '\u{2029}' => self.i += 1,
                        // Escapes valid in strict JSON: pass through verbatim.
                        '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't' => {
                            self.out.push('\\');
                            self.out.push(e);
                            self.i += 1;
                        }
                        'u' => {
                            self.out.push('\\');
                            self.out.push('u');
                            self.i += 1;
                        }
                        // JSON5 single-quote escape -> a bare quote in JSON.
                        '\'' => {
                            self.out.push('\'');
                            self.i += 1;
                        }
                        // JSON5 vertical tab.
                        'v' => {
                            self.out.push_str("\\u000b");
                            self.i += 1;
                        }
                        // JSON5 \0 (NUL, when not followed by another digit).
                        '0' if !self.chars.get(self.i + 1).is_some_and(|d| d.is_ascii_digit()) => {
                            self.out.push_str("\\u0000");
                            self.i += 1;
                        }
                        // JSON5 hex escape \xHH -> \u00HH.
                        'x' => {
                            let h1 = *self.chars.get(self.i + 1)?;
                            let h2 = *self.chars.get(self.i + 2)?;
                            if !h1.is_ascii_hexdigit() || !h2.is_ascii_hexdigit() {
                                return None;
                            }
                            self.out.push_str("\\u00");
                            self.out.push(h1);
                            self.out.push(h2);
                            self.i += 3;
                        }
                        _ => return None,
                    }
                }
                // Raw control characters are legal inside SQLite/JSON5 string
                // literals; escape them so the strict-JSON output stays valid.
                c if (c as u32) < 0x20 => {
                    match c {
                        '\u{08}' => self.out.push_str("\\b"),
                        '\u{09}' => self.out.push_str("\\t"),
                        '\u{0a}' => self.out.push_str("\\n"),
                        '\u{0c}' => self.out.push_str("\\f"),
                        '\u{0d}' => self.out.push_str("\\r"),
                        other => self.out.push_str(&format!("\\u{:04x}", other as u32)),
                    }
                    self.i += 1;
                }
                _ => {
                    self.out.push(c);
                    self.i += 1;
                }
            }
        }
    }

    /// Rewrite a scalar keyword (`true`/`false`/`null`) or a JSON5 number.
    fn rewrite_scalar(&mut self) -> Option<()> {
        // Keyword literals.
        for kw in ["true", "false", "null"] {
            if self.matches_keyword(kw) {
                self.out.push_str(kw);
                self.i += kw.chars().count();
                return Some(());
            }
        }
        self.rewrite_number()
    }

    fn matches_keyword(&self, kw: &str) -> bool {
        let kwc: Vec<char> = kw.chars().collect();
        if self.i + kwc.len() > self.chars.len() {
            return false;
        }
        if self.chars[self.i..self.i + kwc.len()] != kwc[..] {
            return false;
        }
        // Must not be followed by an identifier character.
        match self.chars.get(self.i + kwc.len()) {
            Some(c) => !(c.is_alphanumeric() || *c == '_' || *c == '$'),
            None => true,
        }
    }

    /// Rewrite a JSON5 number token into strict JSON, preserving the token form
    /// SQLite emits (leading/trailing decimal points normalized, `+` stripped,
    /// hex converted to decimal, `Infinity`/`NaN` mapped to `9e999`/`null`).
    fn rewrite_number(&mut self) -> Option<()> {
        // Optional sign.
        let mut sign = "";
        if let Some(c) = self.peek() {
            if c == '+' {
                self.i += 1;
            } else if c == '-' {
                sign = "-";
                self.i += 1;
            }
        }

        // Infinity / NaN.
        if self.matches_word("Infinity") {
            self.i += "Infinity".len();
            self.out.push_str(if sign == "-" { "-9e999" } else { "9e999" });
            return Some(());
        }
        if self.matches_word("NaN") {
            self.i += "NaN".len();
            // SQLite renders NaN as JSON null.
            self.out.push_str("null");
            return Some(());
        }

        // Hexadecimal integer: 0x / 0X followed by hex digits.
        if self.peek() == Some('0') && matches!(self.chars.get(self.i + 1), Some('x') | Some('X')) {
            let hstart = self.i + 2;
            let mut j = hstart;
            while self.chars.get(j).is_some_and(|c| c.is_ascii_hexdigit()) {
                j += 1;
            }
            if j == hstart {
                return None;
            }
            let hex: String = self.chars[hstart..j].iter().collect();
            self.i = j;
            match u64::from_str_radix(&hex, 16) {
                Ok(v) => {
                    // SQLite prints -0 for a negative zero hex literal.
                    if sign == "-" {
                        self.out.push('-');
                    }
                    self.out.push_str(&v.to_string());
                }
                Err(_) => {
                    // Overflow u64 -> SQLite yields (signed) infinity.
                    self.out.push_str(if sign == "-" { "-9e999" } else { "9e999" });
                }
            }
            return Some(());
        }

        // Decimal number: [digits] [ '.' [digits] ] [ ('e'|'E') [sign] digits ].
        let int_start = self.i;
        while self.chars.get(self.i).is_some_and(|c| c.is_ascii_digit()) {
            self.i += 1;
        }
        let int_digits = self.i - int_start;

        // Reject leading-zero integer parts (`01`, `00`) — invalid in both JSON
        // and JSON5, matching SQLite (`json_valid('{"x":-01}')` -> 0). A lone `0`
        // (optionally followed by a fraction/exponent) is fine.
        if int_digits > 1 && self.chars[int_start] == '0' {
            return None;
        }

        let mut has_frac = false;
        let mut frac_digits = 0usize;
        if self.peek() == Some('.') {
            has_frac = true;
            self.i += 1;
            let fstart = self.i;
            while self.chars.get(self.i).is_some_and(|c| c.is_ascii_digit()) {
                self.i += 1;
            }
            frac_digits = self.i - fstart;
        }

        if int_digits == 0 && frac_digits == 0 {
            return None; // no digits: not a number
        }

        // Exponent.
        let exp_start = self.i;
        let mut exp = String::new();
        if matches!(self.peek(), Some('e') | Some('E')) {
            let mut j = self.i + 1;
            let mut esign = String::new();
            if matches!(self.chars.get(j), Some('+') | Some('-')) {
                esign.push(*self.chars.get(j)?);
                j += 1;
            }
            let dstart = j;
            while self.chars.get(j).is_some_and(|c| c.is_ascii_digit()) {
                j += 1;
            }
            if j == dstart {
                self.i = exp_start; // 'e' with no digits: not part of the number
            } else {
                exp = format!("e{}{}", esign, self.chars[dstart..j].iter().collect::<String>());
                self.i = j;
            }
        }

        // Build the strict-JSON mantissa, normalizing leading/trailing dots.
        // `int_start` is the first integer digit (after any sign); the fractional
        // digits (if any) begin just past the '.'.
        let idigits: String =
            self.chars[int_start..int_start + digits_len(self.chars, int_start)].iter().collect();
        let mut mantissa = String::new();
        if idigits.is_empty() {
            mantissa.push('0'); // leading '.5' -> '0.5'
        } else {
            mantissa.push_str(&idigits);
        }
        if has_frac {
            mantissa.push('.');
            let fstart = int_start + idigits.len() + 1; // skip the '.'
            let fdigits: String =
                self.chars[fstart..fstart + digits_len(self.chars, fstart)].iter().collect();
            if fdigits.is_empty() {
                mantissa.push('0'); // trailing '4.' -> '4.0'
            } else {
                mantissa.push_str(&fdigits);
            }
        }

        self.out.push_str(sign);
        self.out.push_str(&mantissa);
        self.out.push_str(&exp);
        Some(())
    }

    /// Does the source at the cursor match `word` as a standalone token (not part
    /// of a longer identifier)?
    fn matches_word(&self, word: &str) -> bool {
        let wc: Vec<char> = word.chars().collect();
        if self.i + wc.len() > self.chars.len() {
            return false;
        }
        if self.chars[self.i..self.i + wc.len()] != wc[..] {
            return false;
        }
        match self.chars.get(self.i + wc.len()) {
            Some(c) => !(c.is_alphanumeric() || *c == '_' || *c == '$'),
            None => true,
        }
    }
}

/// Count consecutive ASCII digits in `chars` starting at `from`.
fn digits_len(chars: &[char], from: usize) -> usize {
    let mut n = 0;
    while chars.get(from + n).is_some_and(|c| c.is_ascii_digit()) {
        n += 1;
    }
    n
}

/// Is `c` whitespace under SQLite's relaxed / JSON5 grammar?
fn is_json5_ws(c: char) -> bool {
    matches!(
        c,
        '\u{09}'
            | '\u{0a}'
            | '\u{0b}'
            | '\u{0c}'
            | '\u{0d}'
            | '\u{20}'
            | '\u{a0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

/// Convert an extracted JSON node into the SQL value SQLite would return from
/// `->>` or single-path `json_extract` (text unquoted, numbers native, booleans
/// as integers, JSON null as SQL NULL, containers as JSON text).
pub(crate) fn json_node_to_sql_value(value: &serde_json::Value) -> SqlValue {
    match value {
        serde_json::Value::Null => SqlValue::Null,
        serde_json::Value::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else if let Some(u) = n.as_u64() {
                // Values above i64::MAX (e.g. large hex literals) that still fit
                // in u64.
                SqlValue::Real(u as f64)
            } else if let Some(f) = n.as_f64() {
                SqlValue::Real(f)
            } else {
                // With `arbitrary_precision`, out-of-f64-range tokens like the
                // `9e999` infinity sentinel report `None` from `as_f64()`.
                // Parse the raw token, which yields ±inf as SQLite expects.
                match n.to_string().parse::<f64>() {
                    Ok(f) => SqlValue::Real(f),
                    Err(_) => SqlValue::Null,
                }
            }
        }
        serde_json::Value::String(s) => SqlValue::Varchar(s.as_str().into()),
        // Objects and arrays are returned as their minified JSON text.
        _ => SqlValue::Varchar(serde_json::to_string(value).unwrap_or_default().into()),
    }
}

/// Render an extracted JSON node as JSON text (used by `->` and by the
/// multi-path `json_extract` array form).
pub(crate) fn json_node_to_json_text(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

/// The SQLite JSON type name for a node.
pub(crate) fn json_node_type_name(value: &serde_json::Value) -> &'static str {
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
        SqlValue::Integer(i) | SqlValue::Bigint(i) => {
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

    let value = match left {
        SqlValue::Varchar(s) | SqlValue::Character(s) => json_text_or_blob_to_node(s.as_str())?,
        SqlValue::Blob(bytes) => jsonb_blob_to_node(bytes)?,
        _ => {
            return Err(ExecutorError::SqliteCompatError("malformed JSON".to_string()));
        }
    };

    let path = arrow_operand_to_path(right)?;

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

/// json_valid(X) / json_valid(X, FLAGS) - test whether X is well-formed JSON.
///
/// A NULL argument returns NULL (matching modern SQLite; the legacy
/// `legacy_json_valid` build returned 0).
///
/// The optional FLAGS argument (SQLite 3.45+) selects which representations
/// count as valid (<https://sqlite.org/json1.html#jvalid>). We honor:
///   - `0x01`: canonical RFC-8259 JSON text
///   - `0x02`: JSON5 text
///   - `0x04`/`0x08`: JSONB blob (a BLOB argument validates when it decodes as a
///     well-formed JSONB document via [`super::jsonb::decode`])
/// Default FLAGS is `1` (canonical text only), so a JSON5 input like `{a:5}`
/// validates as 0 unless bit `0x02` is set, and a JSONB blob validates as 0
/// unless bit `0x04` or `0x08` is set (SQLite's documented default-flags
/// semantics).
pub(crate) fn json_valid(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "json_valid".to_string(),
        });
    }

    // Resolve the flags argument (default 1 = canonical JSON text only).
    let flags: i64 = if args.len() == 2 {
        match &args[1] {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Integer(i) | SqlValue::Bigint(i) => *i,
            SqlValue::Smallint(i) => *i as i64,
            SqlValue::Unsigned(u) => *u as i64,
            other => sql_value_scalar_text(other).parse::<i64>().unwrap_or(1),
        }
    } else {
        1
    };
    let accept_canonical = flags & 0x01 != 0;
    let accept_json5 = flags & 0x02 != 0;
    // Bits 0x04 and 0x08 both select JSONB-blob validation (SQLite: "The input is
    // JSONB" / "The input is text or JSONB").
    let accept_jsonb = flags & 0x0c != 0;

    let valid = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            let s = s.as_str();
            // Depth-bounded strict check (accepts nesting up to
            // SQLITE_MAX_JSON_DEPTH = 1000) rather than `serde_json::from_str`,
            // which caps at 128 and would reject deep-but-valid canonical
            // documents (issue #6052).
            let canonical_ok = accept_canonical && strict_json_is_valid_bounded(s);
            // JSON5 acceptance: the relaxed parser accepts a superset that
            // includes canonical JSON, so only consult it when the JSON5 bit is
            // set and the strict check did not already succeed.
            let json5_ok = accept_json5 && !canonical_ok && parse_json_relaxed(s).is_ok();
            canonical_ok || json5_ok
        }
        // Numeric SQL values render to valid JSON scalars (accepted when either
        // text bit is set).
        SqlValue::Integer(_)
        | SqlValue::Smallint(_)
        | SqlValue::Bigint(_)
        | SqlValue::Unsigned(_)
        | SqlValue::Numeric(_)
        | SqlValue::Float(_)
        | SqlValue::Real(_)
        | SqlValue::Double(_) => accept_canonical || accept_json5,
        // A BLOB validates only under the JSONB bits, and only when it decodes as
        // a well-formed JSONB document (a non-JSONB blob like x'abcd' is invalid).
        SqlValue::Blob(bytes) => accept_jsonb && super::jsonb::decode(bytes).is_some(),
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

    let value = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => json_text_or_blob_to_node(s.as_str())?,
        SqlValue::Blob(bytes) => jsonb_blob_to_node(bytes)?,
        _ => {
            return Err(ExecutorError::SqliteCompatError("malformed JSON".to_string()));
        }
    };

    let paths = &args[1..];

    // Resolve each path argument into a segment list (NULL path -> whole
    // result is NULL, matching SQLite).
    let mut resolved: Vec<Vec<PathSegment>> = Vec::with_capacity(paths.len());
    for p in paths {
        match p {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                resolved.push(
                    parse_sqlite_json_path(s.as_str()).map_err(ExecutorError::SqliteCompatError)?,
                );
            }
            other => {
                // Non-text paths render to their text form for the error
                // message (e.g. integer 0 -> "bad JSON path: '0'").
                let text = sql_value_scalar_text(other);
                return Err(ExecutorError::SqliteCompatError(format!("bad JSON path: '{}'", text)));
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
        // Multi-path extract echoes the source number tokens (e.g. `1e99`),
        // so it uses serde's default formatter rather than the SQLite one.
        Ok(SqlValue::Varchar(serde_json::to_string(&arr).unwrap_or_default().into()))
    }
}

/// jsonb_extract(X, P, ...) - like json_extract but returns JSONB blobs for
/// results that json_extract would render as JSON text.
///
/// A single path selecting a *scalar* returns the SQL value (same as
/// json_extract); a single path selecting a *container* (array/object) returns a
/// JSONB blob; the multi-path form returns a JSONB-encoded array. NULL document /
/// NULL path / missing path propagate NULL exactly like json_extract.
pub(crate) fn jsonb_extract(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "jsonb_extract".to_string(),
        });
    }

    if matches!(args[0], SqlValue::Null) || args.len() == 1 {
        return Ok(SqlValue::Null);
    }

    let value = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => json_text_or_blob_to_node(s.as_str())?,
        SqlValue::Blob(bytes) => jsonb_blob_to_node(bytes)?,
        _ => {
            return Err(ExecutorError::SqliteCompatError("malformed JSON".to_string()));
        }
    };

    let paths = &args[1..];
    let mut resolved: Vec<Vec<PathSegment>> = Vec::with_capacity(paths.len());
    for p in paths {
        match p {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                resolved.push(
                    parse_sqlite_json_path(s.as_str()).map_err(ExecutorError::SqliteCompatError)?,
                );
            }
            other => {
                let text = sql_value_scalar_text(other);
                return Err(ExecutorError::SqliteCompatError(format!("bad JSON path: '{}'", text)));
            }
        }
    }

    if resolved.len() == 1 {
        match navigate(&value, &resolved[0]) {
            // Containers become JSONB blobs; scalars stay SQL values.
            Some(node @ (serde_json::Value::Array(_) | serde_json::Value::Object(_))) => {
                Ok(SqlValue::Blob(super::jsonb::encode(node)))
            }
            Some(node) => Ok(json_node_to_sql_value(node)),
            None => Ok(SqlValue::Null),
        }
    } else {
        let elems: Vec<serde_json::Value> = resolved
            .iter()
            .map(|segs| navigate(&value, segs).cloned().unwrap_or(serde_json::Value::Null))
            .collect();
        Ok(SqlValue::Blob(super::jsonb::encode(&serde_json::Value::Array(elems))))
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

    let value = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => json_text_or_blob_to_node(s.as_str())?,
        SqlValue::Blob(bytes) => jsonb_blob_to_node(bytes)?,
        _ => {
            return Err(ExecutorError::SqliteCompatError("malformed JSON".to_string()));
        }
    };

    let node = if args.len() == 2 {
        match &args[1] {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                let segs =
                    parse_sqlite_json_path(s.as_str()).map_err(ExecutorError::SqliteCompatError)?;
                match navigate(&value, &segs) {
                    Some(n) => n,
                    None => return Ok(SqlValue::Null),
                }
            }
            other => {
                let text = sql_value_scalar_text(other);
                return Err(ExecutorError::SqliteCompatError(format!("bad JSON path: '{}'", text)));
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
/// as-is; SQL NULL becomes the unquoted text `null`. A JSONB blob (produced by
/// jsonb()/jsonb_*()) is decoded and rendered as its canonical JSON text
/// (SQLite: `json_quote(jsonb('[1,2]'))` -> `[1,2]`); any other (non-JSONB) blob
/// is an error.
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
        SqlValue::Blob(bytes) => match super::jsonb::decode(bytes) {
            // A well-formed JSONB blob decodes and re-renders as canonical JSON
            // text (SQLite: `json_quote(jsonb('[1,2]'))` -> `[1,2]`).
            Some(node) => json_node_to_json_text(&node),
            None => {
                return Err(ExecutorError::SqliteCompatError(
                    "JSON cannot hold BLOB values".to_string(),
                ));
            }
        },
        other => {
            // Fall back to a textual scalar rendering for remaining types.
            sql_value_scalar_text(other)
        }
    };

    Ok(SqlValue::Varchar(rendered.into()))
}

/// Render `±Infinity` as SQLite's JSON infinity sentinel, or `None` for finite
/// (or NaN) inputs. SQLite serializes an infinite JSON real as the literal token
/// `9.0e+999` / `-9.0e+999` (see `quote()` in `blob_funcs.rs`), which round-trips
/// back to `±inf` on extraction. NaN never reaches the JSON layer as a value
/// (it is SQL NULL first), so it is intentionally not given a sentinel here.
fn json_infinity_sentinel(f: f64) -> Option<&'static str> {
    if f.is_infinite() {
        Some(if f > 0.0 { "9.0e+999" } else { "-9.0e+999" })
    } else {
        None
    }
}

/// Render an f64 the way SQLite renders JSON reals (keeps a fractional part,
/// e.g. `2.0`), by round-tripping through serde_json's number formatter.
///
/// Negative zero is normalized to `0.0` to match SQLite, which renders both
/// `0.0` and `-0.0` as `0.0` in JSON output. `±Infinity` renders as SQLite's
/// `9.0e+999` / `-9.0e+999` sentinel (serde_json's `Number::from_f64` returns
/// `None` for non-finite values, so this must be handled explicitly).
pub(crate) fn render_json_number(f: f64) -> String {
    if let Some(sentinel) = json_infinity_sentinel(f) {
        return sentinel.to_string();
    }
    // Normalize -0.0 to 0.0 (SQLite: json_group_array(-0.0) -> [0.0]).
    let f = if f == 0.0 { 0.0 } else { f };
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
            // SQLite's json() accepts a relaxed JSON5-like superset (unquoted
            // object keys, single-quoted strings, trailing commas, comments,
            // hex/Infinity numbers, etc.) and emits minified strict JSON.
            //
            // The [`json5_to_json`] pre-processor already emits minified strict
            // JSON that preserves SQLite's number rendering exactly (e.g.
            // `.5e3` -> `0.5e3`, `9e999` for Infinity). We prefer its output
            // verbatim because a serde_json round-trip would rewrite exponent
            // tokens (`0.5e3` -> `0.5e+3`) and mangle the infinity sentinel.
            // Strict-but-non-canonical JSON (whitespace, `1.50`) still needs
            // minifying, so fall back to a serde_json re-serialize when the
            // pre-processor declines (which only happens for inputs the strict
            // parser already accepts).
            if let Some(minified) = json5_to_json(s.as_str()) {
                // The rewriter is lenient about a few number forms serde rejects
                // (e.g. leading-zero integers like `00`). Validate by re-parsing
                // the strict output — but return the rewriter's *text*, not a
                // serde round-trip, so exponent/infinity tokens stay verbatim.
                // Use the depth-bounded strict check (accepts up to
                // SQLITE_MAX_JSON_DEPTH = 1000) rather than `serde_json::from_str`
                // so deep-but-valid documents are not rejected at 128 (issue
                // #6052); the rewriter has already bounded nesting to <= 1000.
                if strict_json_is_valid_bounded(&minified) {
                    return Ok(SqlValue::Varchar(minified.into()));
                }
            }
            match parse_json_relaxed(s.as_str()) {
                Ok(value) => {
                    let minified = serde_json::to_string(&value).map_err(|e| {
                        ExecutorError::SqliteCompatError(format!("malformed JSON: {}", e))
                    })?;
                    Ok(SqlValue::Varchar(minified.into()))
                }
                Err(_) => Err(ExecutorError::SqliteCompatError("malformed JSON".to_string())),
            }
        }
        // A JSONB blob argument (from jsonb()/jsonb_*()) is decoded and rendered
        // as minified JSON text, matching SQLite's `json(<jsonb>)`.
        SqlValue::Blob(bytes) => {
            let value = jsonb_blob_to_node(bytes)?;
            Ok(SqlValue::Varchar(serde_json::to_string(&value).unwrap_or_default().into()))
        }
        // For non-string types, SQLite throws an error
        _ => Err(ExecutorError::SqliteCompatError(
            "JSON functions require string arguments".to_string(),
        )),
    }
}

/// jsonb(X) - validate/parse X and return SQLite's binary JSONB representation.
///
/// Text (canonical or JSON5-relaxed) is parsed and re-encoded as a JSONB blob;
/// an existing JSONB blob is decoded and re-encoded (canonicalized). A NULL
/// argument propagates NULL. This is the BLOB-producing counterpart of `json()`.
pub(crate) fn jsonb(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::WrongNumberOfArguments { function_name: "jsonb".to_string() });
    }

    let node = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => json_text_or_blob_to_node(s.as_str())?,
        SqlValue::Blob(bytes) => jsonb_blob_to_node(bytes)?,
        _ => {
            return Err(ExecutorError::SqliteCompatError(
                "JSON functions require string arguments".to_string(),
            ));
        }
    };

    Ok(SqlValue::Blob(super::jsonb::encode(&node)))
}

// ===========================================================================
// Phase 2: construction and mutation functions
// ===========================================================================
//
// ## The JSON subtype
//
// SQLite tags values produced by JSON functions (json(), json_array(),
// json_object(), the mutation functions, etc.) with an internal *JSON subtype*.
// When such a value is fed as an argument to another JSON function, the callee
// embeds it as a JSON sub-document rather than quoting it as a string literal.
// For example `json_object('ex', json('[52,3.14159]'))` yields
// `{"ex":[52,3.14159]}` (embedded), whereas `json_object('ex', '[52,3.14159]')`
// yields `{"ex":"[52,3.14159]"}` (quoted).
//
// VibeSQL evaluates arguments to plain [`SqlValue`]s before dispatch, so the
// subtype cannot live on the value. Instead the evaluator computes, at the AST
// call site, a per-argument boolean: it is `true` when the argument expression
// is itself a call to a JSON function whose output is *always* well-formed JSON
// (json, json_array, json_object, json_insert, json_replace, json_set,
// json_remove, json_patch). These flags are threaded in as the `subtypes` slice
// alongside `args`. When a subtype flag is set on a TEXT argument, that text is
// parsed and embedded as a JSON sub-document; otherwise the SQL value is encoded
// as a fresh JSON scalar (text -> JSON string, number -> JSON number, etc.).
//
// This mirrors SQLite for every case the mutation/construction functions need.
// The one behavior it does not reproduce is the *conditional* subtype of
// json_extract()/json_quote()/`->` (whose results are JSON only for container
// nodes); those producers are deliberately not flagged, matching plain-text
// embedding, which is what the covered conformance tests expect.

/// Encode a single SQL argument as a JSON node for a construction/mutation
/// function. `is_json` is the argument's subtype flag (see the module note): a
/// TEXT value with the flag set is parsed as an embedded JSON sub-document.
///
/// The flag combines two sources, both computed by the caller before dispatch
/// (see the per-front-end `eval_json_subtype_function` helpers):
///
/// 1. an *AST-derived* flag — the argument expression is itself a call to a
///    JSON-producing function whose output is always well-formed JSON; and
/// 2. a *runtime* flag — the evaluated value already carries the JSON subtype
///    marker ([`sql_value_is_json_subtyped`]) **and** the argument expression is
///    eligible to carry it (it is not a read of a declared CHAR/VARCHAR column;
///    see `expr_runtime_json_subtype_eligible`). This lets a container `value`
///    column from json_each/json_tree embed through an opaque column reference
///    (json101-5.10) without an ordinary fixed-width `CHAR(n)` column whose text
///    happens to parse as a JSON container being silently embedded unquoted
///    (issue #6007 regression).
///
/// Because the eligibility gate lives at the call site (where the argument
/// expression is available), this function no longer inspects the value's
/// runtime marker itself — the caller has already folded it into `is_json`.
fn sql_value_to_json_node(
    value: &SqlValue,
    is_json: bool,
) -> Result<serde_json::Value, ExecutorError> {
    Ok(match value {
        SqlValue::Null => serde_json::Value::Null,
        // SQLite encodes SQL booleans as JSON integers 1/0 (json_array(true)
        // -> [1]), matching json_quote()'s rendering.
        SqlValue::Boolean(b) => serde_json::Value::Number(if *b { 1 } else { 0 }.into()),
        SqlValue::Integer(i) | SqlValue::Bigint(i) => serde_json::Value::Number((*i).into()),
        SqlValue::Smallint(i) => serde_json::Value::Number((*i as i64).into()),
        SqlValue::Unsigned(u) => serde_json::Value::Number((*u).into()),
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => json_number_node(*f),
        SqlValue::Float(f) => json_number_node(*f as f64),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            if is_json {
                // Subtype-flagged text is an embedded JSON sub-document.
                parse_json_relaxed(s.as_str())
                    .map_err(|_| ExecutorError::SqliteCompatError("malformed JSON".to_string()))?
            } else {
                serde_json::Value::String(s.as_str().to_string())
            }
        }
        SqlValue::Blob(bytes) => {
            // A JSONB blob (produced by jsonb()/jsonb_*()) embeds as its decoded
            // JSON sub-document, mirroring SQLite treating a JSONB argument as
            // JSON. Non-JSONB blobs remain an error ("JSON cannot hold BLOB").
            match super::jsonb::decode(bytes) {
                Some(node) => node,
                None => {
                    return Err(ExecutorError::SqliteCompatError(
                        "JSON cannot hold BLOB values".to_string(),
                    ));
                }
            }
        }
        other => serde_json::Value::String(sql_value_scalar_text(other)),
    })
}

/// Build a JSON number node from a SQL real argument, rendering it the way
/// SQLite renders JSON reals (keeps ".0", uses `1.0e+99`-style scientific form).
///
/// We rely on `arbitrary_precision`: the node stores the exact SQLite-formatted
/// token, so `serde_json::to_string` reproduces it verbatim. `±Infinity` is
/// emitted as SQLite's `9.0e+999` / `-9.0e+999` sentinel token, which
/// `json_node_to_sql_value` round-trips back to `±inf` on extraction. NaN
/// (which reaches this helper only defensively, since NaN is SQL NULL before
/// the JSON layer) falls back to a JSON null.
fn json_number_node(f: f64) -> serde_json::Value {
    // SQLite serializes ±Infinity as the `9.0e+999` sentinel; store it as a raw
    // number token (arbitrary_precision) so `serde_json::to_string` reproduces
    // it verbatim and `->>` decodes it back to ±inf.
    let token = match json_infinity_sentinel(f) {
        Some(sentinel) => sentinel.to_string(),
        None if f.is_nan() => return serde_json::Value::Null,
        // SqlValue::Real's Display is SQLite's JSON real format.
        None => SqlValue::Real(f).to_string(),
    };
    match serde_json::from_str::<serde_json::Value>(&token) {
        Ok(v @ serde_json::Value::Number(_)) => v,
        _ => serde_json::Value::Null,
    }
}

/// Parse a read-only JSON *document* argument that may be either JSON text or a
/// JSONB blob (produced by `jsonb()`/`jsonb_*()`), returning the parsed node.
///
/// Used by the read-only accessors (`json`, `json_extract`, `json_type`,
/// `json_array_length`, `->`/`->>`) so a JSONB blob flows through them exactly
/// like the equivalent JSON text. Text is parsed with the relaxed/JSON5 grammar
/// ([`parse_json_relaxed`]); a JSONB blob is decoded via [`super::jsonb::decode`].
/// Any malformed input yields the SQLite-compatible `malformed JSON` error.
fn json_text_or_blob_to_node(json_str: &str) -> Result<serde_json::Value, ExecutorError> {
    parse_json_relaxed(json_str)
        .map_err(|_| ExecutorError::SqliteCompatError("malformed JSON".to_string()))
}

/// Decode a JSONB blob document into its JSON node, mapping failure to the
/// SQLite-compatible `malformed JSON` error.
fn jsonb_blob_to_node(bytes: &[u8]) -> Result<serde_json::Value, ExecutorError> {
    super::jsonb::decode(bytes)
        .ok_or_else(|| ExecutorError::SqliteCompatError("malformed JSON".to_string()))
}

/// Parse the JSON document argument shared by the mutation functions. Returns
/// `Ok(None)` when the argument is SQL NULL (functions propagate NULL), or an
/// error on malformed JSON / non-text input.
fn parse_json_doc_arg(value: &SqlValue) -> Result<Option<serde_json::Value>, ExecutorError> {
    match value {
        SqlValue::Null => Ok(None),
        SqlValue::Varchar(s) | SqlValue::Character(s) => parse_json_relaxed(s.as_str())
            .map(Some)
            .map_err(|_| ExecutorError::SqliteCompatError("malformed JSON".to_string())),
        // Numeric / boolean documents are treated as their JSON scalar form.
        SqlValue::Integer(i) | SqlValue::Bigint(i) => {
            Ok(Some(serde_json::Value::Number((*i).into())))
        }
        SqlValue::Smallint(i) => Ok(Some(serde_json::Value::Number((*i as i64).into()))),
        SqlValue::Unsigned(u) => Ok(Some(serde_json::Value::Number((*u).into()))),
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => {
            Ok(Some(json_number_node(*f)))
        }
        SqlValue::Float(f) => Ok(Some(json_number_node(*f as f64))),
        SqlValue::Boolean(b) => Ok(Some(serde_json::Value::Bool(*b))),
        // A JSONB blob (produced by jsonb()/jsonb_*()) decodes back to its JSON
        // document so mutation functions accept a JSONB document argument.
        SqlValue::Blob(bytes) => match super::jsonb::decode(bytes) {
            Some(node) => Ok(Some(node)),
            None => Err(ExecutorError::SqliteCompatError("malformed JSON".to_string())),
        },
        _ => Err(ExecutorError::SqliteCompatError("malformed JSON".to_string())),
    }
}

/// Does this runtime value already carry SQLite's JSON "J" subtype?
///
/// VibeSQL does not add a distinct subtype tag to [`SqlValue`]; instead the
/// json_each/json_tree table functions signal the JSON subtype on TEXT
/// container values (arrays/objects) by emitting them as [`SqlValue::Character`]
/// rather than [`SqlValue::Varchar`] (see `node_value_column` in
/// `select/scan/table_function.rs`). The two text variants are interoperable
/// everywhere else, so only the JSON construction/mutation functions read this
/// marker.
///
/// The `Character` variant is also how an ordinary fixed-width `CHAR(n)` column
/// materialises, so this predicate alone cannot tell a TVF container apart from
/// a CHAR column holding container-shaped text (issue #6007). Two guards keep
/// the marker from mis-firing:
///
/// 1. the value must be well-formed JSON of container type (array/object) —
///    exactly the nodes json_each/json_tree tag (checked here); and
/// 2. the *argument expression* must be eligible — it must not be a read of a
///    column declared with a real string type (CHAR/VARCHAR). That gate lives
///    at the call site in `expr_runtime_json_subtype_eligible`, because only
///    there is the argument expression (and its declared schema type) known.
///    A `CHAR(20)` (or snug `CHAR(7)`) column read is therefore *not* eligible
///    and its container-shaped text quotes as SQLite does, while a json_tree
///    `value` column (declared dynamic/NULL type) stays eligible.
///
/// This distinguishes json101-5.10 (`json_tree('[1,2,3]')`.value, a container ->
/// `{"a":[1,2,3]}`) from json101-5.11 (a JSON string atom -> `{"a":"[1,2,3]"}`,
/// still quoted) and from `SELECT json_insert('{}','$.a',c) FROM t` over a CHAR
/// column (`{"a":"[1,2,3]"}`, quoted — matching SQLite 3.51).
pub(crate) fn sql_value_is_json_subtyped(value: &SqlValue) -> bool {
    match value {
        SqlValue::Character(s) => {
            matches!(
                parse_json_relaxed(s.as_str()),
                Ok(serde_json::Value::Array(_)) | Ok(serde_json::Value::Object(_))
            )
        }
        _ => false,
    }
}

/// Fetch the subtype flag for the argument at `idx`, OR-ing the AST-derived flag
/// (the caller's `subtypes` slice) with a runtime check on the evaluated value:
/// a value that already carries the JSON subtype (see
/// [`sql_value_is_json_subtyped`]) embeds as JSON even when the argument
/// expression is an opaque column reference. Defaults to `false` when the
/// caller passed no (or a shorter) subtype slice.
fn subtype_at(subtypes: &[bool], idx: usize) -> bool {
    subtypes.get(idx).copied().unwrap_or(false)
}

/// Build the JSON array node for `json_array` / `jsonb_array`.
fn json_array_node(
    args: &[SqlValue],
    subtypes: &[bool],
) -> Result<serde_json::Value, ExecutorError> {
    let mut elems = Vec::with_capacity(args.len());
    for (i, a) in args.iter().enumerate() {
        elems.push(sql_value_to_json_node(a, subtype_at(subtypes, i))?);
    }
    Ok(serde_json::Value::Array(elems))
}

/// json_array(V1, V2, ...) - build a JSON array from the argument values.
pub(crate) fn json_array(args: &[SqlValue], subtypes: &[bool]) -> Result<SqlValue, ExecutorError> {
    let arr = json_array_node(args, subtypes)?;
    Ok(SqlValue::Varchar(serde_json::to_string(&arr).unwrap_or_default().into()))
}

/// jsonb_array(V1, V2, ...) - like json_array but returns a JSONB blob.
pub(crate) fn jsonb_array(args: &[SqlValue], subtypes: &[bool]) -> Result<SqlValue, ExecutorError> {
    let arr = json_array_node(args, subtypes)?;
    Ok(SqlValue::Blob(super::jsonb::encode(&arr)))
}

/// json_object(L1, V1, L2, V2, ...) - build a JSON object.
///
/// Requires an even number of arguments; labels must be TEXT. Duplicate labels
/// keep the last value (matching SQLite's object builder).
pub(crate) fn json_object(args: &[SqlValue], subtypes: &[bool]) -> Result<SqlValue, ExecutorError> {
    let obj = json_object_node(args, subtypes)?;
    Ok(SqlValue::Varchar(serde_json::to_string(&obj).unwrap_or_default().into()))
}

/// jsonb_object(L1, V1, ...) - like json_object but returns a JSONB blob.
pub(crate) fn jsonb_object(
    args: &[SqlValue],
    subtypes: &[bool],
) -> Result<SqlValue, ExecutorError> {
    let obj = json_object_node(args, subtypes)?;
    Ok(SqlValue::Blob(super::jsonb::encode(&obj)))
}

/// Build the JSON object node for `json_object` / `jsonb_object`.
fn json_object_node(
    args: &[SqlValue],
    subtypes: &[bool],
) -> Result<serde_json::Value, ExecutorError> {
    if !args.len().is_multiple_of(2) {
        return Err(ExecutorError::SqliteCompatError(
            "json_object() requires an even number of arguments".to_string(),
        ));
    }

    // serde_json::Map preserves insertion order (feature "preserve_order").
    let mut map = serde_json::Map::new();
    let mut i = 0;
    while i < args.len() {
        let label = match &args[i] {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str().to_string(),
            _ => {
                return Err(ExecutorError::SqliteCompatError(
                    "json_object() labels must be TEXT".to_string(),
                ));
            }
        };
        let node = sql_value_to_json_node(&args[i + 1], subtype_at(subtypes, i + 1))?;
        map.insert(label, node);
        i += 2;
    }

    Ok(serde_json::Value::Object(map))
}

/// json_array_length(X) / json_array_length(X, P) - number of elements in the
/// array at the root (or at path P). Non-arrays return 0; NULL document or NULL
/// path returns NULL; malformed JSON is an error.
pub(crate) fn json_array_length(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "json_array_length".to_string(),
        });
    }

    let value = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => json_text_or_blob_to_node(s.as_str())?,
        SqlValue::Blob(bytes) => jsonb_blob_to_node(bytes)?,
        _ => {
            return Err(ExecutorError::SqliteCompatError("malformed JSON".to_string()));
        }
    };

    let node = if args.len() == 2 {
        match &args[1] {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                let segs =
                    parse_sqlite_json_path(s.as_str()).map_err(ExecutorError::SqliteCompatError)?;
                match navigate(&value, &segs) {
                    Some(n) => n,
                    None => return Ok(SqlValue::Null),
                }
            }
            other => {
                return Err(ExecutorError::SqliteCompatError(format!(
                    "bad JSON path: '{}'",
                    sql_value_scalar_text(other)
                )));
            }
        }
    } else {
        &value
    };

    let len = match node {
        serde_json::Value::Array(arr) => arr.len() as i64,
        _ => 0,
    };
    Ok(SqlValue::Integer(len))
}

/// The kind of edit a mutation function performs at a path.
#[derive(Clone, Copy, PartialEq)]
enum EditMode {
    /// json_insert: only create when the target does not already exist.
    Insert,
    /// json_replace: only overwrite when the target already exists.
    Replace,
    /// json_set: create or overwrite (upsert).
    Set,
}

/// Choose the container type to vivify when a new intermediate node must be
/// created from nothing. SQLite decides purely from the *next* path segment:
/// a `Key` implies an object, an array index (`Index` / `IndexFromEnd`) implies
/// an array. `next` is the segment that will be walked into the freshly-created
/// container (`rest[0]` at the call site), which is always non-empty here.
fn vivified_container(next: &PathSegment) -> serde_json::Value {
    match next {
        PathSegment::Key(_) => serde_json::Value::Object(serde_json::Map::new()),
        PathSegment::Index(_) | PathSegment::IndexFromEnd(_) => {
            serde_json::Value::Array(Vec::new())
        }
    }
}

/// Apply a single edit of the given `mode` at `segments` within `root`,
/// installing `new_value`. Missing intermediate containers are created (for the
/// Insert/Set modes) exactly as SQLite does.
///
/// Returns `true` iff the edit actually installed a value somewhere in the
/// subtree. This success flag is what makes the mutation *atomic*: when an
/// intermediate segment must vivify-and-append a fresh container but the
/// descent into it later dead-ends (e.g. a mid-path `n > len` slot), the caller
/// rolls the freshly-appended stub back so the document is left byte-identical.
/// SQLite's blob editor has this all-or-nothing property, and we must match it.
fn apply_edit(
    root: &mut serde_json::Value,
    segments: &[PathSegment],
    new_value: serde_json::Value,
    mode: EditMode,
) -> bool {
    if segments.is_empty() {
        // A bare "$" replaces the whole document for replace/set; insert is a
        // no-op because the root always exists.
        if mode != EditMode::Insert {
            *root = new_value;
            return true;
        }
        return false;
    }

    let (seg, rest) = (&segments[0], &segments[1..]);

    if rest.is_empty() {
        return apply_leaf_edit(root, seg, new_value, mode);
    }

    // Descend, creating an intermediate container when absent (only meaningful
    // for insert/set; for replace a missing parent means the whole edit is a
    // no-op).
    match seg {
        PathSegment::Key(k) => {
            if !root.is_object() {
                return false;
            }
            let obj = root.as_object_mut().unwrap();
            if obj.contains_key(k) {
                // Existing child: descend directly. No stub was created, so
                // there is nothing to roll back regardless of the outcome.
                return match obj.get_mut(k) {
                    Some(child) => apply_edit(child, rest, new_value, mode),
                    None => false,
                };
            }
            if mode == EditMode::Replace {
                return false;
            }
            // Vivify the container implied by the *next* segment: a following
            // array index means SQLite creates an array here, not an object
            // (e.g. json_set('{}','$.a[0]',1) => {"a":[1]}). Speculatively
            // insert the stub, descend, and roll the key back out if the
            // descent installs nothing (e.g. json_set('{}','$.a[5]',9) must
            // leave `{}` untouched, matching sqlite3).
            obj.insert(k.clone(), vivified_container(&rest[0]));
            let installed = match obj.get_mut(k) {
                Some(child) => apply_edit(child, rest, new_value, mode),
                None => false,
            };
            if !installed {
                obj.remove(k);
            }
            installed
        }
        PathSegment::Index(n) => {
            if let Some(arr) = root.as_array_mut() {
                if let Some(child) = arr.get_mut(*n) {
                    apply_edit(child, rest, new_value, mode)
                } else if *n == arr.len() && mode != EditMode::Replace {
                    // Exact-len append slot: SQLite appends a freshly-vivified
                    // container (type chosen by the next segment) and descends
                    // into it. n > len is a silent no-op (never null-padded).
                    // Speculatively append, then roll the stub back off the tail
                    // if the descent dead-ends (e.g. json_set('[0,1,2]',
                    // '$[3].a[5]',9) must return [0,1,2] unchanged).
                    arr.push(vivified_container(&rest[0]));
                    let idx = arr.len() - 1;
                    let installed = apply_edit(&mut arr[idx], rest, new_value, mode);
                    if !installed {
                        arr.pop();
                    }
                    installed
                } else {
                    false
                }
            } else {
                false
            }
        }
        PathSegment::IndexFromEnd(n) => {
            if let Some(arr) = root.as_array_mut() {
                let len = arr.len();
                if *n >= 1 && *n <= len {
                    let idx = len - *n;
                    apply_edit(&mut arr[idx], rest, new_value, mode)
                } else {
                    false
                }
            } else {
                false
            }
        }
    }
}

/// Apply the terminal edit (the last path segment) to `parent`. Returns `true`
/// iff a value was actually installed; callers use this to roll back any
/// speculatively-vivified intermediate container on a dead-end descent.
fn apply_leaf_edit(
    parent: &mut serde_json::Value,
    seg: &PathSegment,
    new_value: serde_json::Value,
    mode: EditMode,
) -> bool {
    match seg {
        PathSegment::Key(k) => {
            if let Some(obj) = parent.as_object_mut() {
                let exists = obj.contains_key(k);
                let allow = match mode {
                    EditMode::Insert => !exists,
                    EditMode::Replace => exists,
                    EditMode::Set => true,
                };
                if allow {
                    obj.insert(k.clone(), new_value);
                    return true;
                }
            }
            false
        }
        PathSegment::Index(n) => {
            if let Some(arr) = parent.as_array_mut() {
                let exists = *n < arr.len();
                match mode {
                    // Existing index: no-op (SQLite does not overwrite on insert).
                    // Exact-len append slot (n == len): SQLite appends the value;
                    // n > len is a silent no-op (never null-padded).
                    EditMode::Insert => {
                        if !exists && *n == arr.len() {
                            arr.push(new_value);
                            return true;
                        }
                    }
                    EditMode::Replace => {
                        if exists {
                            arr[*n] = new_value;
                            return true;
                        }
                    }
                    EditMode::Set => {
                        if exists {
                            arr[*n] = new_value;
                            return true;
                        } else if *n == arr.len() {
                            arr.push(new_value);
                            return true;
                        }
                    }
                }
            }
            false
        }
        PathSegment::IndexFromEnd(n) => {
            if let Some(arr) = parent.as_array_mut() {
                if *n == 0 {
                    // `$[#]` - the append slot. Insert/Set append a new element.
                    if mode != EditMode::Replace {
                        arr.push(new_value);
                        return true;
                    }
                } else {
                    let len = arr.len();
                    if *n <= len {
                        let idx = len - *n;
                        let allow = match mode {
                            EditMode::Insert => false, // element exists -> no-op
                            EditMode::Replace | EditMode::Set => true,
                        };
                        if allow {
                            arr[idx] = new_value;
                            return true;
                        }
                    }
                }
            }
            false
        }
    }
}

/// Shared driver for json_insert / json_replace / json_set. Applies the
/// (path, value) pairs left-to-right; each edit feeds the next.
fn json_mutate(
    args: &[SqlValue],
    subtypes: &[bool],
    mode: EditMode,
    fn_name: &str,
) -> Result<SqlValue, ExecutorError> {
    match json_mutate_node(args, subtypes, mode, fn_name)? {
        None => Ok(SqlValue::Null),
        Some(doc) => Ok(SqlValue::Varchar(serde_json::to_string(&doc).unwrap_or_default().into())),
    }
}

/// JSONB-returning counterpart of [`json_mutate`].
fn jsonb_mutate(
    args: &[SqlValue],
    subtypes: &[bool],
    mode: EditMode,
    fn_name: &str,
) -> Result<SqlValue, ExecutorError> {
    match json_mutate_node(args, subtypes, mode, fn_name)? {
        None => Ok(SqlValue::Null),
        Some(doc) => Ok(SqlValue::Blob(super::jsonb::encode(&doc))),
    }
}

/// Shared driver producing the mutated document node (or `None` for a NULL
/// document argument). Both the text and JSONB entry points serialize this.
fn json_mutate_node(
    args: &[SqlValue],
    subtypes: &[bool],
    mode: EditMode,
    fn_name: &str,
) -> Result<Option<serde_json::Value>, ExecutorError> {
    // Valid arity is odd: one document argument plus (path, value) pairs.
    if args.is_empty() || args.len().is_multiple_of(2) {
        return Err(ExecutorError::WrongNumberOfArguments { function_name: fn_name.to_string() });
    }

    let mut doc = match parse_json_doc_arg(&args[0])? {
        None => return Ok(None),
        Some(v) => v,
    };

    let mut i = 1;
    while i < args.len() {
        // NULL path -> the whole edit pair is skipped (SQLite leaves the doc
        // unchanged for that pair).
        match &args[i] {
            SqlValue::Null => {
                i += 2;
                continue;
            }
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                let segs =
                    parse_sqlite_json_path(s.as_str()).map_err(ExecutorError::SqliteCompatError)?;
                let node = sql_value_to_json_node(&args[i + 1], subtype_at(subtypes, i + 1))?;
                apply_edit(&mut doc, &segs, node, mode);
            }
            other => {
                return Err(ExecutorError::SqliteCompatError(format!(
                    "bad JSON path: '{}'",
                    sql_value_scalar_text(other)
                )));
            }
        }
        i += 2;
    }

    Ok(Some(doc))
}

/// json_insert(X, P, V, ...) - insert V at P only if P does not already exist.
pub(crate) fn json_insert(args: &[SqlValue], subtypes: &[bool]) -> Result<SqlValue, ExecutorError> {
    json_mutate(args, subtypes, EditMode::Insert, "json_insert")
}

/// jsonb_insert(X, P, V, ...) - like json_insert but returns a JSONB blob.
pub(crate) fn jsonb_insert(
    args: &[SqlValue],
    subtypes: &[bool],
) -> Result<SqlValue, ExecutorError> {
    jsonb_mutate(args, subtypes, EditMode::Insert, "jsonb_insert")
}

/// json_replace(X, P, V, ...) - replace the value at P only if P exists.
pub(crate) fn json_replace(
    args: &[SqlValue],
    subtypes: &[bool],
) -> Result<SqlValue, ExecutorError> {
    json_mutate(args, subtypes, EditMode::Replace, "json_replace")
}

/// jsonb_replace(X, P, V, ...) - like json_replace but returns a JSONB blob.
pub(crate) fn jsonb_replace(
    args: &[SqlValue],
    subtypes: &[bool],
) -> Result<SqlValue, ExecutorError> {
    jsonb_mutate(args, subtypes, EditMode::Replace, "jsonb_replace")
}

/// json_set(X, P, V, ...) - insert or replace the value at P (upsert).
pub(crate) fn json_set(args: &[SqlValue], subtypes: &[bool]) -> Result<SqlValue, ExecutorError> {
    json_mutate(args, subtypes, EditMode::Set, "json_set")
}

/// jsonb_set(X, P, V, ...) - like json_set but returns a JSONB blob.
pub(crate) fn jsonb_set(args: &[SqlValue], subtypes: &[bool]) -> Result<SqlValue, ExecutorError> {
    jsonb_mutate(args, subtypes, EditMode::Set, "jsonb_set")
}

/// json_remove(X, P, ...) - remove the element(s) at the given path(s).
///
/// Paths apply left-to-right against the evolving document; a non-existent path
/// is a no-op. A bare `$` (with no further arguments) removes the whole document
/// -- matching SQLite, `json_remove(X)` returns X minified.
pub(crate) fn json_remove(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    match json_remove_node(args)? {
        None => Ok(SqlValue::Null),
        Some(doc) => Ok(SqlValue::Varchar(serde_json::to_string(&doc).unwrap_or_default().into())),
    }
}

/// jsonb_remove(X, P, ...) - like json_remove but returns a JSONB blob.
pub(crate) fn jsonb_remove(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    match json_remove_node(args)? {
        None => Ok(SqlValue::Null),
        Some(doc) => Ok(SqlValue::Blob(super::jsonb::encode(&doc))),
    }
}

/// Shared driver producing the pruned document node (or `None` for a NULL
/// result). Both the text and JSONB entry points serialize this.
fn json_remove_node(args: &[SqlValue]) -> Result<Option<serde_json::Value>, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "json_remove".to_string(),
        });
    }

    let mut doc = match parse_json_doc_arg(&args[0])? {
        None => return Ok(None),
        Some(v) => v,
    };

    for p in &args[1..] {
        match p {
            SqlValue::Null => return Ok(None),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                let segs =
                    parse_sqlite_json_path(s.as_str()).map_err(ExecutorError::SqliteCompatError)?;
                // Removing the root ($) discards the whole document: SQLite
                // returns NULL (e.g. `json_remove('{"x":25}','$')` -> NULL).
                // (Note: `json_remove(X)` with *no* path argument returns X
                // unchanged, handled by the loop simply not running.)
                if segs.is_empty() {
                    return Ok(None);
                }
                remove_path(&mut doc, &segs);
            }
            other => {
                return Err(ExecutorError::SqliteCompatError(format!(
                    "bad JSON path: '{}'",
                    sql_value_scalar_text(other)
                )));
            }
        }
    }

    Ok(Some(doc))
}

/// Remove the node at `segments` from `root` if present.
fn remove_path(root: &mut serde_json::Value, segments: &[PathSegment]) {
    let Some((seg, rest)) = segments.split_first() else {
        // Empty path (`$`) removing the whole document is a no-op here; SQLite
        // returns the document unchanged.
        return;
    };

    if rest.is_empty() {
        match seg {
            PathSegment::Key(k) => {
                if let Some(obj) = root.as_object_mut() {
                    // shift_remove preserves the order of surviving keys.
                    obj.shift_remove(k);
                }
            }
            PathSegment::Index(n) => {
                if let Some(arr) = root.as_array_mut() {
                    if *n < arr.len() {
                        arr.remove(*n);
                    }
                }
            }
            PathSegment::IndexFromEnd(n) => {
                if let Some(arr) = root.as_array_mut() {
                    let len = arr.len();
                    if *n >= 1 && *n <= len {
                        arr.remove(len - *n);
                    }
                }
            }
        }
        return;
    }

    match seg {
        PathSegment::Key(k) => {
            if let Some(child) = root.as_object_mut().and_then(|o| o.get_mut(k)) {
                remove_path(child, rest);
            }
        }
        PathSegment::Index(n) => {
            if let Some(child) = root.as_array_mut().and_then(|a| a.get_mut(*n)) {
                remove_path(child, rest);
            }
        }
        PathSegment::IndexFromEnd(n) => {
            if let Some(arr) = root.as_array_mut() {
                let len = arr.len();
                if *n >= 1 && *n <= len {
                    remove_path(&mut arr[len - *n], rest);
                }
            }
        }
    }
}

/// json_patch(X, Y) - apply the RFC-7396 JSON Merge Patch Y to X.
///
/// NULL for either argument yields NULL. Non-object patches replace the target
/// wholesale; object patches merge recursively with `null` members deleting.
pub(crate) fn json_patch(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    match json_patch_node(args)? {
        None => Ok(SqlValue::Null),
        Some(result) => {
            Ok(SqlValue::Varchar(serde_json::to_string(&result).unwrap_or_default().into()))
        }
    }
}

/// jsonb_patch(X, Y) - like json_patch but returns a JSONB blob.
pub(crate) fn jsonb_patch(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    match json_patch_node(args)? {
        None => Ok(SqlValue::Null),
        Some(result) => Ok(SqlValue::Blob(super::jsonb::encode(&result))),
    }
}

/// Shared driver producing the merged document node (or `None` for a NULL
/// result). Both the text and JSONB entry points serialize this.
fn json_patch_node(args: &[SqlValue]) -> Result<Option<serde_json::Value>, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "json_patch".to_string(),
        });
    }

    let target = match parse_json_doc_arg(&args[0])? {
        None => return Ok(None),
        Some(v) => v,
    };
    let patch = match parse_json_doc_arg(&args[1])? {
        None => return Ok(None),
        Some(v) => v,
    };

    Ok(Some(merge_patch(target, patch)))
}

/// RFC-7396 MergePatch algorithm.
fn merge_patch(target: serde_json::Value, patch: serde_json::Value) -> serde_json::Value {
    match patch {
        serde_json::Value::Object(patch_map) => {
            // If the target is not an object, RFC-7396 starts from an empty one.
            let mut base = match target {
                serde_json::Value::Object(m) => m,
                _ => serde_json::Map::new(),
            };
            for (k, v) in patch_map {
                if v.is_null() {
                    // shift_remove preserves the order of the remaining keys
                    // (plain remove is swap_remove under preserve_order).
                    base.shift_remove(&k);
                } else if let Some(existing) = base.get_mut(&k) {
                    // Update an existing key in place, keeping its position.
                    let taken = std::mem::replace(existing, serde_json::Value::Null);
                    *existing = merge_patch(taken, v);
                } else {
                    // New key -> appended at the end.
                    base.insert(k, merge_patch(serde_json::Value::Null, v));
                }
            }
            serde_json::Value::Object(base)
        }
        // A non-object patch replaces the target entirely.
        other => other,
    }
}

/// json_error_position(X) - 1-based character offset of the first JSON syntax
/// error in X, or 0 if X parses under SQLite's relaxed (JSON5) grammar.
///
/// A NULL argument returns NULL. The position is computed by a small relaxed
/// scanner that matches sqlite3's reporting on the covered conformance cases
/// (trailing commas are accepted; a stray extra comma / missing value reports
/// the offset of the offending token).
pub(crate) fn json_error_position(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::WrongNumberOfArguments {
            function_name: "json_error_position".to_string(),
        });
    }

    let s = match &args[0] {
        SqlValue::Null => return Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        // Non-text scalars are valid JSON values -> position 0.
        _ => return Ok(SqlValue::Integer(0)),
    };

    // Accept the relaxed superset first: if it parses, there is no error.
    if parse_json_relaxed(s).is_ok() {
        return Ok(SqlValue::Integer(0));
    }

    Ok(SqlValue::Integer(relaxed_json_error_offset(s)))
}

/// Scan `s` under SQLite's relaxed JSON grammar and return the 1-based character
/// offset of the first structural error (0 if none is found by the scanner).
fn relaxed_json_error_offset(s: &str) -> i64 {
    let chars: Vec<char> = s.chars().collect();
    let mut p = Parser { chars: &chars, i: 0 };
    p.skip_ws();
    if p.i >= p.chars.len() {
        // Empty / all-whitespace input: SQLite reports position 1.
        return 1;
    }
    match p.parse_value() {
        Ok(()) => {
            p.skip_ws();
            if p.i < p.chars.len() {
                // Trailing junk after a complete value.
                (p.i + 1) as i64
            } else {
                0
            }
        }
        Err(pos) => (pos + 1) as i64,
    }
}

/// Minimal recursive-descent scanner for the relaxed JSON grammar, used only to
/// locate the first error position for json_error_position(). On error it
/// returns the 0-based index of the offending character.
struct Parser<'a> {
    chars: &'a [char],
    i: usize,
}

impl Parser<'_> {
    fn skip_ws(&mut self) {
        while self.i < self.chars.len() {
            match self.chars[self.i] {
                ' ' | '\t' | '\n' | '\r' => self.i += 1,
                _ => break,
            }
        }
    }

    fn parse_value(&mut self) -> Result<(), usize> {
        self.skip_ws();
        if self.i >= self.chars.len() {
            return Err(self.i);
        }
        match self.chars[self.i] {
            '{' => self.parse_object(),
            '[' => self.parse_array(),
            '"' | '\'' => self.parse_string(),
            _ => self.parse_bareword_or_number(),
        }
    }

    fn parse_object(&mut self) -> Result<(), usize> {
        self.i += 1; // consume '{'
        self.skip_ws();
        if self.i < self.chars.len() && self.chars[self.i] == '}' {
            self.i += 1;
            return Ok(());
        }
        loop {
            self.skip_ws();
            // A stray comma / missing key is an error here.
            if self.i >= self.chars.len() || self.chars[self.i] == ',' || self.chars[self.i] == '}'
            {
                return Err(self.i);
            }
            // Key: quoted or (relaxed) bareword.
            if self.chars[self.i] == '"' || self.chars[self.i] == '\'' {
                self.parse_string()?;
            } else {
                self.parse_bareword_or_number()?;
            }
            self.skip_ws();
            if self.i >= self.chars.len() || self.chars[self.i] != ':' {
                return Err(self.i);
            }
            self.i += 1; // consume ':'
            self.parse_value()?;
            self.skip_ws();
            if self.i >= self.chars.len() {
                return Err(self.i);
            }
            match self.chars[self.i] {
                ',' => {
                    self.i += 1;
                    self.skip_ws();
                    // Relaxed trailing comma: `,}` is allowed.
                    if self.i < self.chars.len() && self.chars[self.i] == '}' {
                        self.i += 1;
                        return Ok(());
                    }
                    // A second comma (`,,`) is an error at this position.
                    if self.i < self.chars.len() && self.chars[self.i] == ',' {
                        return Err(self.i);
                    }
                }
                '}' => {
                    self.i += 1;
                    return Ok(());
                }
                _ => return Err(self.i),
            }
        }
    }

    fn parse_array(&mut self) -> Result<(), usize> {
        self.i += 1; // consume '['
        self.skip_ws();
        if self.i < self.chars.len() && self.chars[self.i] == ']' {
            self.i += 1;
            return Ok(());
        }
        loop {
            self.skip_ws();
            if self.i >= self.chars.len() || self.chars[self.i] == ',' || self.chars[self.i] == ']'
            {
                return Err(self.i);
            }
            self.parse_value()?;
            self.skip_ws();
            if self.i >= self.chars.len() {
                return Err(self.i);
            }
            match self.chars[self.i] {
                ',' => {
                    self.i += 1;
                    self.skip_ws();
                    // Relaxed trailing comma: `,]` is allowed.
                    if self.i < self.chars.len() && self.chars[self.i] == ']' {
                        self.i += 1;
                        return Ok(());
                    }
                    if self.i < self.chars.len() && self.chars[self.i] == ',' {
                        return Err(self.i);
                    }
                }
                ']' => {
                    self.i += 1;
                    return Ok(());
                }
                _ => return Err(self.i),
            }
        }
    }

    fn parse_string(&mut self) -> Result<(), usize> {
        let quote = self.chars[self.i];
        let start = self.i;
        self.i += 1;
        while self.i < self.chars.len() {
            let c = self.chars[self.i];
            if c == '\\' {
                self.i += 2;
                continue;
            }
            if c == quote {
                self.i += 1;
                return Ok(());
            }
            self.i += 1;
        }
        Err(start) // unterminated string
    }

    fn parse_bareword_or_number(&mut self) -> Result<(), usize> {
        let start = self.i;
        while self.i < self.chars.len() {
            match self.chars[self.i] {
                ',' | ':' | '}' | ']' | ' ' | '\t' | '\n' | '\r' => break,
                _ => self.i += 1,
            }
        }
        if self.i == start {
            return Err(start);
        }
        // If the token looks like a number (starts with a sign, digit, or '.'),
        // validate it under the relaxed number grammar so malformed numerics like
        // `-01` are reported as errors here (matching SQLite's
        // json_error_position). Other barewords (true/false/null/Infinity/NaN and
        // unquoted-key fragments) are left as-is.
        let first = self.chars[start];
        if matches!(first, '+' | '-' | '.') || first.is_ascii_digit() {
            let token: String = self.chars[start..self.i].iter().collect();
            if !is_valid_relaxed_number(&token) {
                return Err(start);
            }
        }
        Ok(())
    }
}

/// Does `token` parse as a single valid relaxed/JSON5 number (the exact subset
/// [`Json5Rewriter::rewrite_number`] accepts)? Used by the error-position
/// scanner to reject malformed numerics such as `-01`.
fn is_valid_relaxed_number(token: &str) -> bool {
    let chars: Vec<char> = token.chars().collect();
    let mut w = Json5Rewriter { chars: &chars, i: 0, out: String::new(), depth: 0 };
    w.rewrite_number().is_some() && w.i == chars.len()
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

    /// Deeply-nested input must not overflow the stack. The `Json5Rewriter`
    /// recurses once per nesting level (`rewrite_value` → `rewrite_object`/
    /// `rewrite_array` → `rewrite_value`); without the [`MAX_JSON5_DEPTH`] guard a
    /// ~20k-deep document SIGABRTs the whole process on a small worker-thread
    /// stack (a Rust stack overflow is not catchable). The guard turns that into a
    /// clean rewrite failure (`None` / "malformed JSON"), matching SQLite's
    /// behavior past `SQLITE_MAX_JSON_DEPTH` where `json()` reports "malformed
    /// JSON" and `json_valid` returns 0.
    ///
    /// The trailing comma forces the input off the strict `serde_json` fast path
    /// (which caps recursion at 128 on its own) and through the JSON5 rewriter,
    /// exercising exactly the recursion the guard protects.
    #[test]
    fn test_json5_deep_nesting_does_not_overflow_stack() {
        // Run on a small (512 KiB) stack so an unbounded rewriter would reliably
        // overflow — proving the guard, not just a generous main-thread stack, is
        // what keeps this bounded.
        let handle = std::thread::Builder::new()
            .stack_size(512 * 1024)
            .spawn(|| {
                let depth = 20_000usize;
                let mut s = String::with_capacity(depth * 2 + 2);
                for _ in 0..depth {
                    s.push('[');
                }
                s.push_str("1,"); // trailing comma routes through the JSON5 rewriter
                for _ in 0..depth {
                    s.push(']');
                }

                // The rewriter must bail with None rather than recursing to a crash.
                assert!(
                    json5_to_json(&s).is_none(),
                    "deep JSON5 rewrite should fail cleanly, not overflow the stack"
                );

                // Public surface: json_valid(<deep>, 2) must return 0, and json()
                // must report malformed JSON — never abort the process.
                let deep = SqlValue::Varchar(s.clone().into());
                assert_eq!(
                    json_valid(&[deep.clone(), SqlValue::Integer(2)]).unwrap(),
                    SqlValue::Integer(0),
                    "json_valid(<deep>, 2) should be 0"
                );
                assert!(
                    json(&[deep]).is_err(),
                    "json(<deep>) should error (malformed JSON), not crash"
                );
            })
            .expect("spawn small-stack test thread");
        handle.join().expect("deep-nesting test thread must not abort/panic");
    }

    /// The depth guard rejects exactly at [`MAX_JSON5_DEPTH`]: a document nested to
    /// the cap rewrites, one level deeper fails. (Uses the trailing-comma JSON5
    /// path; the strict `serde_json` path independently caps at 128.)
    #[test]
    fn test_json5_depth_cap_boundary() {
        let build = |depth: usize| {
            let mut s = String::with_capacity(depth * 2 + 2);
            for _ in 0..depth {
                s.push('[');
            }
            s.push_str("1,");
            for _ in 0..depth {
                s.push(']');
            }
            s
        };
        // At the cap: the outermost container is depth 1, the innermost is depth
        // MAX_JSON5_DEPTH — accepted.
        assert!(
            json5_to_json(&build(MAX_JSON5_DEPTH)).is_some(),
            "nesting to exactly MAX_JSON5_DEPTH should rewrite"
        );
        // One past the cap: rejected.
        assert!(
            json5_to_json(&build(MAX_JSON5_DEPTH + 1)).is_none(),
            "nesting past MAX_JSON5_DEPTH should fail the rewrite"
        );
    }

    /// Build a canonical (strict, no JSON5 syntax) array nested `depth` levels
    /// deep with a scalar at the center: `[[[...0...]]]`.
    fn build_canonical_array(depth: usize) -> String {
        let mut s = String::with_capacity(depth * 2 + 1);
        for _ in 0..depth {
            s.push('[');
        }
        s.push('0');
        for _ in 0..depth {
            s.push(']');
        }
        s
    }

    /// Build a canonical object nested `depth` levels deep:
    /// `{"a":{"a":...0...}}` (mirrors json101-11.2/11.3).
    fn build_canonical_object(depth: usize) -> String {
        let mut s = String::with_capacity(depth * 6 + 1);
        for _ in 0..depth {
            s.push_str(r#"{"a":"#);
        }
        s.push('0');
        for _ in 0..depth {
            s.push('}');
        }
        s
    }

    /// The effective nesting cap for canonical `json_valid` / `json()` is exactly
    /// SQLITE_MAX_JSON_DEPTH (1000), not serde_json's built-in 128 — a document
    /// nested 128..=1000 deep must validate, and 1001 must not (issue #6052,
    /// mirrors the vendored SQLite json101-11.0..11.3 expectations).
    #[test]
    fn test_json_depth_cap_is_1000_not_128() {
        // Regression guard: the old serde_json 128 boundary must be gone.
        let d200 = build_canonical_array(200);
        assert_eq!(
            json_valid(&[SqlValue::Varchar(d200.clone().into())]).unwrap(),
            SqlValue::Integer(1),
            "canonical array nested 200 deep must validate (was rejected at 128)"
        );

        for build in [build_canonical_array as fn(usize) -> String, build_canonical_object] {
            // Exactly at the cap: valid under default flags (1), JSON5 (2), and
            // canonical+JSON5 (3).
            let at_cap = build(MAX_JSON5_DEPTH);
            for flags in [1i64, 2, 3] {
                assert_eq!(
                    json_valid(&[
                        SqlValue::Varchar(at_cap.clone().into()),
                        SqlValue::Integer(flags),
                    ])
                    .unwrap(),
                    SqlValue::Integer(1),
                    "json_valid at depth {MAX_JSON5_DEPTH} with flags {flags} must be 1"
                );
            }
            // json() minifies (and thus validates) a 1000-deep document.
            assert!(
                json(&[SqlValue::Varchar(at_cap.clone().into())]).is_ok(),
                "json() at depth {MAX_JSON5_DEPTH} must succeed"
            );

            // One past the cap: rejected under all text flags, and json() errors.
            let past_cap = build(MAX_JSON5_DEPTH + 1);
            for flags in [1i64, 2, 3] {
                assert_eq!(
                    json_valid(&[
                        SqlValue::Varchar(past_cap.clone().into()),
                        SqlValue::Integer(flags),
                    ])
                    .unwrap(),
                    SqlValue::Integer(0),
                    "json_valid at depth {} with flags {flags} must be 0",
                    MAX_JSON5_DEPTH + 1
                );
            }
            assert!(
                json(&[SqlValue::Varchar(past_cap.into())]).is_err(),
                "json() at depth {} must error (malformed JSON)",
                MAX_JSON5_DEPTH + 1
            );
        }
    }

    /// The fixed canonical call sites (`json_valid` flags=1 and `json()`) must not
    /// overflow the stack on adversarially deep *canonical* input — the depth
    /// gate ([`json_nesting_within_cap`]) is iterative and rejects before serde
    /// ever recurses. Runs on a small (512 KiB) stack so an unbounded parse would
    /// reliably crash (issue #6052).
    #[test]
    fn test_deep_canonical_does_not_overflow_stack() {
        let handle = std::thread::Builder::new()
            .stack_size(512 * 1024)
            .spawn(|| {
                let s = build_canonical_array(20_000);

                // The iterative depth gate bails without recursing.
                assert!(
                    !json_nesting_within_cap(&s),
                    "20k-deep canonical nesting must exceed the cap"
                );

                let deep = SqlValue::Varchar(s.into());
                // Default flags (canonical) — the newly-fixed path.
                assert_eq!(
                    json_valid(&[deep.clone()]).unwrap(),
                    SqlValue::Integer(0),
                    "json_valid(<deep canonical>) must be 0, not crash"
                );
                // Canonical+JSON5 flags.
                assert_eq!(
                    json_valid(&[deep.clone(), SqlValue::Integer(3)]).unwrap(),
                    SqlValue::Integer(0),
                    "json_valid(<deep canonical>, 3) must be 0, not crash"
                );
                assert!(json(&[deep]).is_err(), "json(<deep canonical>) must error, not crash");
            })
            .expect("spawn small-stack test thread");
        handle.join().expect("deep-canonical test thread must not abort/panic");
    }

    /// Leading-zero numbers are malformed under both JSON and SQLite's JSON5
    /// (`json_valid('{"x":-01}')` -> 0, `json_error_position` non-zero).
    #[test]
    fn test_json5_rejects_leading_zero_numbers() {
        for bad in [r#"{"x":-01}"#, r#"{"x":01.5}"#, r#"{"x":00}"#, r#"{"x":-00}"#] {
            assert!(parse_json_relaxed(bad).is_err(), "should reject {bad:?}");
            assert_eq!(
                json_valid(&[SqlValue::Varchar(bad.into())]).unwrap(),
                SqlValue::Integer(0),
                "json_valid should be 0 for {bad:?}",
            );
        }
        // A lone 0 (and 0.x) is fine.
        for ok in [r#"{"x":0}"#, r#"{"x":-0}"#, r#"{"x":0.5}"#] {
            assert!(parse_json_relaxed(ok).is_ok(), "should accept {ok:?}");
        }
    }

    /// json_valid honors the FLAGS argument: default (or bit 0x01) accepts only
    /// canonical JSON, bit 0x02 additionally accepts JSON5. Pinned to sqlite3.
    #[test]
    fn test_json_valid_flags() {
        let json5 = SqlValue::Varchar("{a:5}".into());
        let canon = SqlValue::Varchar(r#"{"a":5}"#.into());
        // Default flags: JSON5 rejected, canonical accepted.
        assert_eq!(json_valid(&[json5.clone()]).unwrap(), SqlValue::Integer(0));
        assert_eq!(json_valid(&[canon.clone()]).unwrap(), SqlValue::Integer(1));
        // Flag 2 accepts JSON5.
        assert_eq!(
            json_valid(&[json5.clone(), SqlValue::Integer(2)]).unwrap(),
            SqlValue::Integer(1)
        );
        // Flag 1 rejects JSON5.
        assert_eq!(json_valid(&[json5, SqlValue::Integer(1)]).unwrap(), SqlValue::Integer(0));
    }

    #[test]
    fn test_json_valid_jsonb_blob_flags() {
        let node: serde_json::Value = serde_json::from_str(r#"{"a":1}"#).unwrap();
        let blob = SqlValue::Blob(super::super::jsonb::encode(&node));
        // Default flags (canonical text only): a JSONB blob validates as 0,
        // matching SQLite's documented default-flags semantics.
        assert_eq!(json_valid(&[blob.clone()]).unwrap(), SqlValue::Integer(0));
        // Flag bit 0x04 selects JSONB validation -> 1.
        assert_eq!(
            json_valid(&[blob.clone(), SqlValue::Integer(4)]).unwrap(),
            SqlValue::Integer(1)
        );
        // Flag 5 (0x01 | 0x04) also accepts the JSONB blob (json101-5.2b).
        assert_eq!(
            json_valid(&[blob.clone(), SqlValue::Integer(5)]).unwrap(),
            SqlValue::Integer(1)
        );
        // Flag bit 0x08 selects JSONB validation too.
        assert_eq!(json_valid(&[blob, SqlValue::Integer(8)]).unwrap(), SqlValue::Integer(1));
        // A non-JSONB blob is invalid even with the JSONB bit set.
        assert_eq!(
            json_valid(&[SqlValue::Blob(vec![0xab, 0xcd]), SqlValue::Integer(5)]).unwrap(),
            SqlValue::Integer(0)
        );
    }

    /// Removing the root path ($) discards the whole document -> NULL, while
    /// json_remove with no path argument returns the document unchanged.
    #[test]
    fn test_json_remove_root_path() {
        let doc = SqlValue::Varchar(r#"{"x":25,"y":42}"#.into());
        assert_eq!(
            json_remove(&[doc.clone(), SqlValue::Varchar("$".into())]).unwrap(),
            SqlValue::Null,
        );
        assert_eq!(json_remove(&[doc]).unwrap(), SqlValue::Varchar(r#"{"x":25,"y":42}"#.into()),);
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
    /// JSON5 number/comment/Infinity handling, pinned byte-for-byte to
    /// sqlite3 3.51.0's `json()` output (the JSON5 pre-processor path).
    #[test]
    fn test_json5_number_and_comment_rendering() {
        // (input, expected minified json() output) pinned to sqlite3 3.51.0.
        for (s, want) in [
            ("0x1A", "26"),
            ("+Infinity", "9e999"),
            ("-Infinity", "-9e999"),
            ("Infinity", "9e999"),
            ("NaN", "null"),
            (".5", "0.5"),
            ("1.", "1.0"),
            ("{x: 4.}", r#"{"x":4.0}"#),
            ("{x: 4.e0}", r#"{"x":4.0e0}"#),
            ("{x: .5e3}", r#"{"x":0.5e3}"#),
            ("{x: -.5e-1}", r#"{"x":-0.5e-1}"#),
            ("+5", "5"),
            ("{a: +0x10}", r#"{"a":16}"#),
            ("{a: -0x10}", r#"{"a":-16}"#),
            ("/* c */ 5", "5"),
            ("5 // c", "5"),
            ("{a:0x10}", r#"{"a":16}"#),
            ("[Infinity,NaN,-Infinity]", "[9e999,null,-9e999]"),
            ("{x:'a \"b\" c'}", r#"{"x":"a \"b\" c"}"#),
            ("1.50", "1.50"),
            ("{a:-0x0}", r#"{"a":-0}"#),
            ("0xFFFFFFFFFFFFFFFF", "18446744073709551615"),
        ] {
            let got = json(&[SqlValue::Varchar(s.into())]).unwrap();
            assert_eq!(got, SqlValue::Varchar(want.into()), "input {s:?}");
        }
    }

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
        assert_eq!(parse_sqlite_json_path("$[#-1]").unwrap(), vec![PathSegment::IndexFromEnd(1)]);
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
        assert_eq!(
            json_valid(&[SqlValue::Varchar(r#"{"a":1}"#.into())]).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(json_valid(&[SqlValue::Varchar("bad".into())]).unwrap(), SqlValue::Integer(0));
        // Whitespace tolerated; empty is invalid
        assert_eq!(
            json_valid(&[SqlValue::Varchar("  123 ".into())]).unwrap(),
            SqlValue::Integer(1)
        );
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
    fn test_json_valid_canonical_with_flags_arg() {
        // Flag 5 = bits 0x01 (canonical) | 0x04 (JSONB blob). Canonical text is
        // accepted via the 0x01 bit.
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
            json_extract(&[
                SqlValue::Varchar(r#"{"a":1}"#.into()),
                SqlValue::Varchar("$.a".into())
            ])
            .unwrap(),
            SqlValue::Integer(1)
        );
        // real
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"{"a":1.5}"#.into()),
                SqlValue::Varchar("$.a".into())
            ])
            .unwrap(),
            SqlValue::Real(1.5)
        );
        // text unquoted
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"{"a":"hello"}"#.into()),
                SqlValue::Varchar("$.a".into())
            ])
            .unwrap(),
            SqlValue::Varchar("hello".into())
        );
        // boolean -> integer 1/0
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"{"a":true}"#.into()),
                SqlValue::Varchar("$.a".into())
            ])
            .unwrap(),
            SqlValue::Integer(1)
        );
        // JSON null -> SQL NULL
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"{"a":null}"#.into()),
                SqlValue::Varchar("$.a".into())
            ])
            .unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_json_extract_container_returns_json_text() {
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Varchar("$".into())])
                .unwrap(),
            SqlValue::Varchar(r#"{"a":1}"#.into())
        );
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"{"a":[1,2]}"#.into()),
                SqlValue::Varchar("$.a".into())
            ])
            .unwrap(),
            SqlValue::Varchar("[1,2]".into())
        );
    }

    #[test]
    fn test_json_extract_array_index_and_from_end() {
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"{"a":[1,2,3]}"#.into()),
                SqlValue::Varchar("$.a[1]".into())
            ])
            .unwrap(),
            SqlValue::Integer(2)
        );
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar("[1,2,3]".into()),
                SqlValue::Varchar("$[#-1]".into())
            ])
            .unwrap(),
            SqlValue::Integer(3)
        );
    }

    #[test]
    fn test_json_extract_missing_path_is_null() {
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"{"a":1}"#.into()),
                SqlValue::Varchar("$.x".into())
            ])
            .unwrap(),
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
            ])
            .unwrap(),
            SqlValue::Varchar("[1,null]".into())
        );
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"{"a":"x","b":"y"}"#.into()),
                SqlValue::Varchar("$.a".into()),
                SqlValue::Varchar("$.b".into()),
            ])
            .unwrap(),
            SqlValue::Varchar(r#"["x","y"]"#.into())
        );
    }

    #[test]
    fn test_json_extract_null_and_single_arg() {
        assert_eq!(json_extract(&[SqlValue::Null]).unwrap(), SqlValue::Null);
        // Single non-null argument yields NULL (matches SQLite).
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":1}"#.into())]).unwrap(),
            SqlValue::Null
        );
        // NULL path -> NULL
        assert_eq!(
            json_extract(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Null]).unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_json_extract_errors() {
        // Bare key (no '$') is a bad path
        let e =
            json_extract(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Varchar("a".into())]);
        assert!(
            matches!(e, Err(ExecutorError::SqliteCompatError(ref m)) if m == "bad JSON path: 'a'")
        );
        // Malformed JSON document is an error
        let e = json_extract(&[SqlValue::Varchar("{bad".into()), SqlValue::Varchar("$.a".into())]);
        assert!(matches!(e, Err(ExecutorError::SqliteCompatError(ref m)) if m == "malformed JSON"));
    }

    #[test]
    fn test_json_extract_quoted_and_empty_keys() {
        // json101-18.2 / 18.3
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"{"":5}"#.into()),
                SqlValue::Varchar(r#"$."""#.into())
            ])
            .unwrap(),
            SqlValue::Integer(5)
        );
        assert_eq!(
            json_extract(&[
                SqlValue::Varchar(r#"[3,{"a":4,"":[5,{"hi":6},7]},8]"#.into()),
                SqlValue::Varchar(r#"$[1].""[1].hi"#.into()),
            ])
            .unwrap(),
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
            json_type(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Varchar("$.a".into())])
                .unwrap(),
            SqlValue::Varchar("integer".into())
        );
        // non-existent path -> NULL
        assert_eq!(
            json_type(&[SqlValue::Varchar(r#"{"a":1}"#.into()), SqlValue::Varchar("$.x".into())])
                .unwrap(),
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
        assert_eq!(
            json_quote(&[SqlValue::Integer(12345)]).unwrap(),
            SqlValue::Varchar("12345".into())
        );
        assert_eq!(
            json_quote(&[SqlValue::Real(3.14159)]).unwrap(),
            SqlValue::Varchar("3.14159".into())
        );
        // Real keeps a fractional part, matching SQLite (json_quote(2.0) -> 2.0)
        assert_eq!(json_quote(&[SqlValue::Real(2.0)]).unwrap(), SqlValue::Varchar("2.0".into()));
        // NULL -> unquoted "null"
        assert_eq!(json_quote(&[SqlValue::Null]).unwrap(), SqlValue::Varchar("null".into()));
    }

    #[test]
    fn test_json_quote_non_jsonb_blob_errors() {
        // A blob that does not decode as JSONB is still an error.
        let e = json_quote(&[SqlValue::Blob(vec![0x30, 0x31])]);
        assert!(
            matches!(e, Err(ExecutorError::SqliteCompatError(ref m)) if m == "JSON cannot hold BLOB values")
        );
    }

    #[test]
    fn test_json_quote_jsonb_blob_renders_canonical_text() {
        // A JSONB blob decodes and renders as its canonical JSON text (matching
        // SQLite: json_quote(jsonb('[1,2]')) -> [1,2]).
        let arr: serde_json::Value = serde_json::from_str("[1,2]").unwrap();
        assert_eq!(
            json_quote(&[SqlValue::Blob(super::super::jsonb::encode(&arr))]).unwrap(),
            SqlValue::Varchar("[1,2]".into())
        );
        let obj: serde_json::Value = serde_json::from_str(r#"{"a":1}"#).unwrap();
        assert_eq!(
            json_quote(&[SqlValue::Blob(super::super::jsonb::encode(&obj))]).unwrap(),
            SqlValue::Varchar(r#"{"a":1}"#.into())
        );
        // A JSONB scalar string re-renders as a quoted JSON string.
        let s: serde_json::Value = serde_json::Value::String("hello".into());
        assert_eq!(
            json_quote(&[SqlValue::Blob(super::super::jsonb::encode(&s))]).unwrap(),
            SqlValue::Varchar(r#""hello""#.into())
        );
        // A JSONB scalar number re-renders bare.
        let n: serde_json::Value = serde_json::from_str("5").unwrap();
        assert_eq!(
            json_quote(&[SqlValue::Blob(super::super::jsonb::encode(&n))]).unwrap(),
            SqlValue::Varchar("5".into())
        );
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
            eval_json_arrow(
                &SqlValue::Varchar(r#"{"a":1}"#.into()),
                &SqlValue::Varchar("$.a".into()),
                false
            )
            .unwrap(),
            SqlValue::Varchar("1".into())
        );
        // ->> returns SQL value (integer)
        assert_eq!(
            eval_json_arrow(
                &SqlValue::Varchar(r#"{"a":1}"#.into()),
                &SqlValue::Varchar("$.a".into()),
                true
            )
            .unwrap(),
            SqlValue::Integer(1)
        );
        // ->> on text yields unquoted string
        assert_eq!(
            eval_json_arrow(
                &SqlValue::Varchar(r#"{"a":"hello"}"#.into()),
                &SqlValue::Varchar("$.a".into()),
                true
            )
            .unwrap(),
            SqlValue::Varchar("hello".into())
        );
    }

    #[test]
    fn test_arrow_bare_label_and_integer_shorthand() {
        // Bare text label -> $.<label>
        assert_eq!(
            eval_json_arrow(
                &SqlValue::Varchar(r#"{"a":1}"#.into()),
                &SqlValue::Varchar("a".into()),
                false
            )
            .unwrap(),
            SqlValue::Varchar("1".into())
        );
        // Integer shorthand -> $[N]
        assert_eq!(
            eval_json_arrow(&SqlValue::Varchar("[1,2,3]".into()), &SqlValue::Integer(1), true)
                .unwrap(),
            SqlValue::Integer(2)
        );
    }

    #[test]
    fn test_arrow_null_and_missing() {
        // Non-existent path -> NULL for both forms
        assert_eq!(
            eval_json_arrow(
                &SqlValue::Varchar(r#"{"a":1}"#.into()),
                &SqlValue::Varchar("b".into()),
                false
            )
            .unwrap(),
            SqlValue::Null
        );
        // JSON null: -> yields text "null", ->> yields SQL NULL
        assert_eq!(
            eval_json_arrow(
                &SqlValue::Varchar(r#"{"a":null}"#.into()),
                &SqlValue::Varchar("$.a".into()),
                false
            )
            .unwrap(),
            SqlValue::Varchar("null".into())
        );
        assert_eq!(
            eval_json_arrow(
                &SqlValue::Varchar(r#"{"a":null}"#.into()),
                &SqlValue::Varchar("$.a".into()),
                true
            )
            .unwrap(),
            SqlValue::Null
        );
        // NULL operands propagate
        assert_eq!(
            eval_json_arrow(&SqlValue::Null, &SqlValue::Integer(0), false).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            eval_json_arrow(&SqlValue::Varchar(r#"{"a":1}"#.into()), &SqlValue::Null, false)
                .unwrap(),
            SqlValue::Null
        );
    }

    #[test]
    fn test_arrow_malformed_errors() {
        let e = eval_json_arrow(
            &SqlValue::Varchar("{bad".into()),
            &SqlValue::Varchar("$.a".into()),
            false,
        );
        assert!(matches!(e, Err(ExecutorError::SqliteCompatError(ref m)) if m == "malformed JSON"));
    }

    // ---- Phase 2 helpers --------------------------------------------------

    fn v(s: &str) -> SqlValue {
        SqlValue::Varchar(s.into())
    }
    fn txt(v: SqlValue) -> String {
        match v {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str().to_string(),
            other => panic!("expected text, got {:?}", other),
        }
    }

    // ---- json_array -------------------------------------------------------

    #[test]
    fn test_json_array_basic() {
        // sqlite3: json_array(1,2,'3',4) -> [1,2,"3",4]
        assert_eq!(
            txt(json_array(
                &[SqlValue::Integer(1), SqlValue::Integer(2), v("3"), SqlValue::Integer(4)],
                &[]
            )
            .unwrap()),
            r#"[1,2,"3",4]"#
        );
        // Empty -> []
        assert_eq!(txt(json_array(&[], &[]).unwrap()), "[]");
        // NULL element -> json null
        assert_eq!(
            txt(json_array(&[SqlValue::Integer(1), SqlValue::Null, SqlValue::Integer(4)], &[])
                .unwrap()),
            "[1,null,4]"
        );
    }

    #[test]
    fn test_json_array_subtype_embedding() {
        // json_array(1,null,'3',json('[4,5]'),json('{"six":7.7}'))
        // subtype flags: only the json(...) args are JSON.
        let args = [SqlValue::Integer(1), SqlValue::Null, v("3"), v("[4,5]"), v(r#"{"six":7.7}"#)];
        let subs = [false, false, false, true, true];
        assert_eq!(txt(json_array(&args, &subs).unwrap()), r#"[1,null,"3",[4,5],{"six":7.7}]"#);
        // Without the subtype flag, the same text quotes as a string.
        assert_eq!(txt(json_array(&[v("[4,5]")], &[false]).unwrap()), r#"["[4,5]"]"#);
    }

    #[test]
    fn test_sql_value_is_json_subtyped_marker() {
        // A Character container value carries the runtime JSON subtype.
        assert!(sql_value_is_json_subtyped(&SqlValue::Character("[1,2,3]".into())));
        assert!(sql_value_is_json_subtyped(&SqlValue::Character(r#"{"a":1}"#.into())));
        // A Character scalar string does NOT (json101-5.11: "[1,2,3]" atom).
        assert!(!sql_value_is_json_subtyped(&SqlValue::Character("hello".into())));
        assert!(!sql_value_is_json_subtyped(&SqlValue::Character("123".into())));
        // Varchar never carries the marker (plain text).
        assert!(!sql_value_is_json_subtyped(&SqlValue::Varchar("[1,2,3]".into())));
        // Non-text values never carry it.
        assert!(!sql_value_is_json_subtyped(&SqlValue::Integer(1)));
        assert!(!sql_value_is_json_subtyped(&SqlValue::Null));
    }

    #[test]
    fn test_json_insert_embeds_when_subtype_flag_set() {
        // The low-level `json_insert` embeds a TEXT argument as a JSON
        // sub-document exactly when its subtype flag is `true`, and quotes it
        // otherwise — the flag is the single source of truth.
        //
        // Deciding whether a runtime `SqlValue::Character` container marker sets
        // that flag is now the caller's job (the evaluator folds
        // `sql_value_is_json_subtyped` into the flag *only* for eligible
        // argument expressions, so a `CHAR(n)` column read never embeds — see
        // issue #6007 and `tests/json_char_column_subtype_tests.rs`). This unit
        // test therefore drives the flag explicitly.
        let doc = v("{}");
        let path = v("$.a");
        // json101-5.10 shape: flag set (a container value column) -> embed.
        let container = SqlValue::Character("[1,2,3]".into());
        assert_eq!(
            txt(json_insert(&[doc.clone(), path.clone(), container], &[false, false, true])
                .unwrap()),
            r#"{"a":[1,2,3]}"#
        );
        // json101-5.11 shape / CHAR-column shape: flag clear -> quote, even for a
        // `Character` value whose text parses as a JSON container.
        let char_container = SqlValue::Character("[1,2,3]".into());
        assert_eq!(
            txt(json_insert(&[doc, path, char_container], &[false, false, false]).unwrap()),
            r#"{"a":"[1,2,3]"}"#
        );
    }

    #[test]
    fn test_json_array_float_and_bool() {
        assert_eq!(
            txt(json_array(&[SqlValue::Real(3.14159), SqlValue::Real(2.0)], &[]).unwrap()),
            "[3.14159,2.0]"
        );
        assert_eq!(
            txt(json_array(&[SqlValue::Boolean(true), SqlValue::Boolean(false)], &[]).unwrap()),
            "[1,0]"
        );
    }

    #[test]
    fn test_json_infinity_sentinel_across_surface() {
        // json_array / json_object emit SQLite's 9.0e+999 sentinel for ±Inf
        // (json101-20.1). Previously these collapsed to `null`.
        assert_eq!(txt(json_array(&[SqlValue::Real(f64::INFINITY)], &[]).unwrap()), "[9.0e+999]");
        assert_eq!(
            txt(json_array(&[SqlValue::Real(f64::NEG_INFINITY)], &[]).unwrap()),
            "[-9.0e+999]"
        );
        assert_eq!(
            txt(json_object(&[v("a"), SqlValue::Real(f64::INFINITY)], &[]).unwrap()),
            r#"{"a":9.0e+999}"#
        );
        assert_eq!(
            txt(json_object(
                &[v("a"), SqlValue::Real(f64::INFINITY), v("b"), SqlValue::Real(f64::NEG_INFINITY)],
                &[]
            )
            .unwrap()),
            r#"{"a":9.0e+999,"b":-9.0e+999}"#
        );

        // json_quote renders the sentinel too (previously rendered bare "inf").
        assert_eq!(
            json_quote(&[SqlValue::Real(f64::INFINITY)]).unwrap(),
            SqlValue::Varchar("9.0e+999".into())
        );
        assert_eq!(
            json_quote(&[SqlValue::Real(f64::NEG_INFINITY)]).unwrap(),
            SqlValue::Varchar("-9.0e+999".into())
        );

        // render_json_number (shared with json_group_array/object) helper check.
        assert_eq!(render_json_number(f64::INFINITY), "9.0e+999");
        assert_eq!(render_json_number(f64::NEG_INFINITY), "-9.0e+999");

        // NaN stays NULL at the node level (SQLite never emits a NaN sentinel).
        assert_eq!(json_number_node(f64::NAN), serde_json::Value::Null);
    }

    #[test]
    fn test_json_infinity_extract_round_trips_to_inf() {
        // The 9.0e+999 sentinel decodes back to ±inf via json_node_to_sql_value,
        // driving json101-20.2/20.3 (->> of an infinite field yields Inf/-Inf).
        let obj = txt(json_object(
            &[v("a"), SqlValue::Real(f64::INFINITY), v("b"), SqlValue::Real(f64::NEG_INFINITY)],
            &[],
        )
        .unwrap());
        assert_eq!(
            json_extract(&[SqlValue::Varchar(obj.clone().into()), v("$.a")]).unwrap(),
            SqlValue::Real(f64::INFINITY)
        );
        assert_eq!(
            json_extract(&[SqlValue::Varchar(obj.into()), v("$.b")]).unwrap(),
            SqlValue::Real(f64::NEG_INFINITY)
        );
    }

    #[test]
    fn test_json_insert_infinity_sentinel() {
        // The mutation helpers (json_insert/json_set/...) share json_number_node,
        // so an infinite value argument stores the sentinel too.
        assert_eq!(
            txt(json_insert(&[v("{}"), v("$.a"), SqlValue::Real(f64::INFINITY)], &[]).unwrap()),
            r#"{"a":9.0e+999}"#
        );
        assert_eq!(
            txt(json_set(&[v(r#"{"a":1}"#), v("$.a"), SqlValue::Real(f64::NEG_INFINITY)], &[])
                .unwrap()),
            r#"{"a":-9.0e+999}"#
        );
    }

    #[test]
    fn test_json_array_blob_errors() {
        let e = json_array(&[SqlValue::Blob(vec![0x61, 0x62])], &[]);
        assert!(
            matches!(e, Err(ExecutorError::SqliteCompatError(ref m)) if m == "JSON cannot hold BLOB values")
        );
    }

    // ---- json_object ------------------------------------------------------

    #[test]
    fn test_json_object_basic_and_order() {
        assert_eq!(
            txt(json_object(&[v("a"), SqlValue::Integer(1), v("b"), v("x")], &[]).unwrap()),
            r#"{"a":1,"b":"x"}"#
        );
        // Insertion order preserved (non-alphabetical), matching sqlite3.
        assert_eq!(
            txt(json_object(&[v("b"), SqlValue::Integer(1), v("a"), SqlValue::Integer(2)], &[])
                .unwrap()),
            r#"{"b":1,"a":2}"#
        );
    }

    #[test]
    fn test_json_object_subtype_embedding() {
        // json_object('ex', json('[52,3.14159]')) -> {"ex":[52,3.14159]}
        assert_eq!(
            txt(json_object(&[v("ex"), v("[52,3.14159]")], &[false, true]).unwrap()),
            r#"{"ex":[52,3.14159]}"#
        );
        // Nested object embedding: json_object('a',2,'c',json_object('e',5))
        let inner = txt(json_object(&[v("e"), SqlValue::Integer(5)], &[]).unwrap());
        assert_eq!(
            txt(json_object(
                &[v("a"), SqlValue::Integer(2), v("c"), v(&inner)],
                &[false, false, false, true]
            )
            .unwrap()),
            r#"{"a":2,"c":{"e":5}}"#
        );
    }

    #[test]
    fn test_json_object_odd_args_error() {
        let e = json_object(&[v("a"), SqlValue::Integer(1), v("b")], &[]);
        assert!(matches!(e, Err(ExecutorError::SqliteCompatError(ref m))
            if m == "json_object() requires an even number of arguments"));
    }

    #[test]
    fn test_json_object_non_text_label_error() {
        let e = json_object(&[SqlValue::Integer(1), SqlValue::Integer(2)], &[]);
        assert!(matches!(e, Err(ExecutorError::SqliteCompatError(ref m))
            if m == "json_object() labels must be TEXT"));
        let e = json_object(&[SqlValue::Null, SqlValue::Integer(2)], &[]);
        assert!(matches!(e, Err(ExecutorError::SqliteCompatError(ref m))
            if m == "json_object() labels must be TEXT"));
    }

    // ---- json_array_length ------------------------------------------------

    #[test]
    fn test_json_array_length() {
        assert_eq!(json_array_length(&[v("[1,2,3,4]")]).unwrap(), SqlValue::Integer(4));
        // Non-array root -> 0
        assert_eq!(json_array_length(&[v(r#"{"one":[1,2,3]}"#)]).unwrap(), SqlValue::Integer(0));
        // With path
        assert_eq!(
            json_array_length(&[v(r#"{"one":[1,2,3]}"#), v("$.one")]).unwrap(),
            SqlValue::Integer(3)
        );
        // Path to non-array -> 0
        assert_eq!(json_array_length(&[v("[1,2,3,4]"), v("$[2]")]).unwrap(), SqlValue::Integer(0));
        // NULL doc -> NULL
        assert_eq!(json_array_length(&[SqlValue::Null]).unwrap(), SqlValue::Null);
        // Malformed -> error
        assert!(json_array_length(&[v("bad json")]).is_err());
    }

    // ---- json_set / json_insert / json_replace ----------------------------

    #[test]
    fn test_json_set() {
        // Overwrite existing
        assert_eq!(
            txt(json_set(&[v(r#"{"a":2,"c":4}"#), v("$.a"), SqlValue::Integer(99)], &[]).unwrap()),
            r#"{"a":99,"c":4}"#
        );
        // Create missing
        assert_eq!(
            txt(json_set(&[v(r#"{"a":2,"c":4}"#), v("$.e"), SqlValue::Integer(5)], &[]).unwrap()),
            r#"{"a":2,"c":4,"e":5}"#
        );
        // Embed subtype value
        assert_eq!(
            txt(json_set(&[v(r#"{"a":2,"c":4}"#), v("$.c"), v("[97,96]")], &[false, false, true])
                .unwrap()),
            r#"{"a":2,"c":[97,96]}"#
        );
        // NULL value stores json null
        assert_eq!(
            txt(json_set(&[v(r#"{"a":1}"#), v("$.a"), SqlValue::Null], &[]).unwrap()),
            r#"{"a":null}"#
        );
    }

    #[test]
    fn test_json_insert() {
        // Existing path -> no-op
        assert_eq!(
            txt(json_insert(&[v(r#"{"a":2,"c":4}"#), v("$.a"), SqlValue::Integer(99)], &[])
                .unwrap()),
            r#"{"a":2,"c":4}"#
        );
        // Missing path -> insert
        assert_eq!(
            txt(json_insert(&[v(r#"{"a":2,"c":4}"#), v("$.e"), SqlValue::Integer(5)], &[]).unwrap()),
            r#"{"a":2,"c":4,"e":5}"#
        );
        // Existing array index -> no-op
        assert_eq!(
            txt(json_insert(&[v("[1,2,3]"), v("$[0]"), SqlValue::Integer(99)], &[]).unwrap()),
            "[1,2,3]"
        );
    }

    #[test]
    fn test_json_replace() {
        // Existing path -> replace
        assert_eq!(
            txt(json_replace(&[v(r#"{"a":2,"c":4}"#), v("$.a"), SqlValue::Integer(99)], &[])
                .unwrap()),
            r#"{"a":99,"c":4}"#
        );
        // Missing path -> no-op
        assert_eq!(
            txt(json_replace(&[v(r#"{"a":2,"c":4}"#), v("$.e"), SqlValue::Integer(5)], &[])
                .unwrap()),
            r#"{"a":2,"c":4}"#
        );
    }

    #[test]
    fn test_json_set_append_slot() {
        // $[#] appends
        assert_eq!(
            txt(json_set(&[v("[1,2,3]"), v("$[#]"), SqlValue::Integer(99)], &[]).unwrap()),
            "[1,2,3,99]"
        );
        assert_eq!(
            txt(json_insert(&[v("[1,2,3]"), v("$[#]"), SqlValue::Integer(99)], &[]).unwrap()),
            "[1,2,3,99]"
        );
        // $[#-1] targets the last element
        assert_eq!(
            txt(json_set(&[v("[1,2,3]"), v("$[#-1]"), SqlValue::Integer(99)], &[]).unwrap()),
            "[1,2,99]"
        );
    }

    #[test]
    fn test_json_mutate_multi_path_left_to_right() {
        // Two paths applied in order (json_set)
        assert_eq!(
            txt(json_set(
                &[v(r#"{"a":1}"#), v("$.b"), SqlValue::Integer(2), v("$.c"), SqlValue::Integer(3)],
                &[]
            )
            .unwrap()),
            r#"{"a":1,"b":2,"c":3}"#
        );
        // Two append slots: each feeds the next (json_insert '$[#]' twice)
        assert_eq!(
            txt(json_insert(
                &[v("[1,2,3]"), v("$[#]"), SqlValue::Integer(4), v("$[#]"), SqlValue::Integer(5)],
                &[]
            )
            .unwrap()),
            "[1,2,3,4,5]"
        );
    }

    #[test]
    fn test_json_set_creates_nested() {
        assert_eq!(
            txt(json_set(&[v("{}"), v("$.a.b"), SqlValue::Integer(1)], &[]).unwrap()),
            r#"{"a":{"b":1}}"#
        );
        assert_eq!(
            txt(json_set(&[v(r#"{"a":{}}"#), v("$.a.b"), SqlValue::Integer(1)], &[]).unwrap()),
            r#"{"a":{"b":1}}"#
        );
    }

    // --- Nested-path auto-vivify rule matrix (issue #6054) -------------------
    //
    // Curator-verified rule (differential-tested vs sqlite3 3.51): a plain
    // array-index segment vivifies/appends iff `n == arr.len()` EXACTLY, at any
    // position (terminal or intermediate). `n > len` is a silent no-op (never
    // null-padded). json_insert and json_set are symmetric for these vivify
    // decisions. When a container is vivified from nothing, its type is chosen
    // from the *next* segment: key => object, index => array.

    #[test]
    fn test_vivify_terminal_index_exact_len_appends() {
        // n == len at the terminal (leaf) position => append, for both set/insert.
        assert_eq!(
            txt(json_set(&[v("[0,1,2]"), v("$[3]"), SqlValue::Integer(9)], &[]).unwrap()),
            "[0,1,2,9]"
        );
        assert_eq!(
            txt(json_insert(&[v("[0,1,2]"), v("$[3]"), SqlValue::Integer(9)], &[]).unwrap()),
            "[0,1,2,9]"
        );
    }

    #[test]
    fn test_vivify_terminal_index_past_len_is_noop() {
        // n > len (one past the append slot) => silent no-op, never null-padded.
        assert_eq!(
            txt(json_set(&[v("[0,1,2]"), v("$[4]"), SqlValue::Integer(9)], &[]).unwrap()),
            "[0,1,2]"
        );
        assert_eq!(
            txt(json_insert(&[v("[0,1,2]"), v("$[4]"), SqlValue::Integer(9)], &[]).unwrap()),
            "[0,1,2]"
        );
        // Empty array: index 3 is far past the append slot => no-op.
        assert_eq!(txt(json_set(&[v("[]"), v("$[3]"), SqlValue::Integer(1)], &[]).unwrap()), "[]");
    }

    #[test]
    fn test_vivify_intermediate_index_exact_len_appends() {
        // n == len at an intermediate position => append a vivified container and
        // descend. json101-24.4: $[3].a[0].b on [0,1,2].
        assert_eq!(
            txt(json_set(&[v("[0,1,2]"), v("$[3].a[0].b"), SqlValue::Integer(9)], &[]).unwrap()),
            r#"[0,1,2,{"a":[{"b":9}]}]"#
        );
        assert_eq!(
            txt(json_insert(&[v("[0,1,2]"), v("$[3].a[0].b"), SqlValue::Integer(9)], &[]).unwrap()),
            r#"[0,1,2,{"a":[{"b":9}]}]"#
        );
    }

    #[test]
    fn test_vivify_intermediate_index_past_len_is_noop() {
        // Out-of-range intermediate segment => no vivify at all.
        assert_eq!(
            txt(json_set(&[v("[0,1,2]"), v("$[4].a"), SqlValue::Integer(9)], &[]).unwrap()),
            "[0,1,2]"
        );
    }

    #[test]
    fn test_vivify_key_then_index_creates_array() {
        // A newly-vivified key whose next segment is an index becomes an ARRAY.
        // json_set('{}','$.a[0]',1) => {"a":[1]}
        assert_eq!(
            txt(json_set(&[v("{}"), v("$.a[0]"), SqlValue::Integer(1)], &[]).unwrap()),
            r#"{"a":[1]}"#
        );
        // json101-24.6: $[1].a[0].b on [0,{},2] => key "a" (absent) vivifies array.
        assert_eq!(
            txt(json_set(&[v("[0,{},2]"), v("$[1].a[0].b"), SqlValue::Integer(9)], &[]).unwrap()),
            r#"[0,{"a":[{"b":9}]},2]"#
        );
        assert_eq!(
            txt(json_insert(&[v("[0,{},2]"), v("$[1].a[0].b"), SqlValue::Integer(9)], &[]).unwrap()),
            r#"[0,{"a":[{"b":9}]},2]"#
        );
    }

    #[test]
    fn test_vivify_key_then_key_creates_object() {
        // A newly-vivified key whose next segment is a key becomes an OBJECT.
        assert_eq!(
            txt(json_set(&[v("{}"), v("$.a.b"), SqlValue::Integer(1)], &[]).unwrap()),
            r#"{"a":{"b":1}}"#
        );
    }

    #[test]
    fn test_vivify_appended_element_then_index_creates_array() {
        // json101-24.7: $[3][0].b on [0,1,2] => append slot vivifies an ARRAY
        // (next segment is an index), then object {"b":9} inside it.
        assert_eq!(
            txt(json_set(&[v("[0,1,2]"), v("$[3][0].b"), SqlValue::Integer(9)], &[]).unwrap()),
            r#"[0,1,2,[{"b":9}]]"#
        );
        assert_eq!(
            txt(json_insert(&[v("[0,1,2]"), v("$[3][0].b"), SqlValue::Integer(9)], &[]).unwrap()),
            r#"[0,1,2,[{"b":9}]]"#
        );
    }

    #[test]
    fn test_vivify_in_range_noncontainer_does_not_vivify() {
        // json101-24.5/24.8 analogue: $[1] on [0,1,2] is in-range but holds a
        // non-container scalar; the walk aborts, no vivify/append.
        assert_eq!(
            txt(json_set(&[v("[0,1,2]"), v("$[1].a"), SqlValue::Integer(9)], &[]).unwrap()),
            "[0,1,2]"
        );
        assert_eq!(
            txt(json_set(&[v("[0,1,2]"), v("$[1][0]"), SqlValue::Integer(9)], &[]).unwrap()),
            "[0,1,2]"
        );
    }

    #[test]
    fn test_vivify_replace_never_vivifies() {
        // replace must never create/append: all vivify cases are no-ops.
        assert_eq!(
            txt(json_replace(&[v("[0,1,2]"), v("$[3]"), SqlValue::Integer(9)], &[]).unwrap()),
            "[0,1,2]"
        );
        assert_eq!(
            txt(json_replace(&[v("[0,1,2]"), v("$[3].a[0].b"), SqlValue::Integer(9)], &[]).unwrap()),
            "[0,1,2]"
        );
        assert_eq!(
            txt(json_replace(&[v("{}"), v("$.a[0]"), SqlValue::Integer(1)], &[]).unwrap()),
            "{}"
        );
    }

    #[test]
    fn test_vivify_mid_path_dead_end_leaves_doc_byte_identical() {
        // Regression for issue #6054 PR review (Judge): the intermediate
        // vivify-and-append arm must be ATOMIC. When a freshly-appended stub's
        // descent dead-ends at a deeper `n > len` slot, the stub must be rolled
        // back so the document comes back byte-identical. All expectations below
        // are the exact output of sqlite3 3.51 for the same call.
        //
        // Index-arm family (append slot $[3] vivified, then dead-ends):
        //   json_set('[0,1,2]','$[3].a[5]',9)   => [0,1,2]
        //   json_set('[0,1,2]','$[3][5]',9)     => [0,1,2]
        //   json_set('[0,1,2]','$[3].a.b[5]',9) => [0,1,2]
        //   json_set('[0,1,2]','$[3][0][5]',9)  => [0,1,2]
        for path in ["$[3].a[5]", "$[3][5]", "$[3].a.b[5]", "$[3][0][5]"] {
            assert_eq!(
                txt(json_set(&[v("[0,1,2]"), v(path), SqlValue::Integer(9)], &[]).unwrap()),
                "[0,1,2]",
                "json_set dead-end path {path} must be a byte-identical no-op",
            );
            assert_eq!(
                txt(json_insert(&[v("[0,1,2]"), v(path), SqlValue::Integer(9)], &[]).unwrap()),
                "[0,1,2]",
                "json_insert dead-end path {path} must be a byte-identical no-op",
            );
        }

        // Key-arm family (vivified key, then dead-ends): main was already wrong
        // here (pre-existing, same root cause); locked to sqlite3 3.51.
        //   json_set('{}','$.a[5]',9)          => {}
        //   json_set('{"a":1}','$.b.c[5]',9)   => {"a":1}
        assert_eq!(
            txt(json_set(&[v("{}"), v("$.a[5]"), SqlValue::Integer(9)], &[]).unwrap()),
            "{}",
        );
        assert_eq!(
            txt(json_insert(&[v("{}"), v("$.a[5]"), SqlValue::Integer(9)], &[]).unwrap()),
            "{}",
        );
        assert_eq!(
            txt(json_set(&[v(r#"{"a":1}"#), v("$.b.c[5]"), SqlValue::Integer(9)], &[]).unwrap()),
            r#"{"a":1}"#,
        );
        assert_eq!(
            txt(json_set(&[v(r#"{"a":1}"#), v("$.b[5]"), SqlValue::Integer(9)], &[]).unwrap()),
            r#"{"a":1}"#,
        );

        // Positive control: the sibling exact-len terminals still vivify+install,
        // proving the roll-back does not over-fire.
        //   json_set('[0,1,2]','$[3].a[0]',9) => [0,1,2,{"a":[9]}]
        //   json_set('[0,1,2]','$[3][0]',9)   => [0,1,2,[9]]
        assert_eq!(
            txt(json_set(&[v("[0,1,2]"), v("$[3].a[0]"), SqlValue::Integer(9)], &[]).unwrap()),
            r#"[0,1,2,{"a":[9]}]"#,
        );
        assert_eq!(
            txt(json_set(&[v("[0,1,2]"), v("$[3][0]"), SqlValue::Integer(9)], &[]).unwrap()),
            "[0,1,2,[9]]",
        );
    }

    #[test]
    fn test_json_mutate_null_doc_and_path() {
        assert_eq!(
            json_set(&[SqlValue::Null, v("$.a"), SqlValue::Integer(1)], &[]).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            json_insert(&[SqlValue::Null, v("$.a"), SqlValue::Integer(1)], &[]).unwrap(),
            SqlValue::Null
        );
        assert_eq!(
            json_replace(&[SqlValue::Null, v("$.a"), SqlValue::Integer(1)], &[]).unwrap(),
            SqlValue::Null
        );
        // NULL path -> that pair is skipped, doc unchanged
        assert_eq!(
            txt(json_set(&[v(r#"{"a":1}"#), SqlValue::Null, SqlValue::Integer(5)], &[]).unwrap()),
            r#"{"a":1}"#
        );
    }

    // ---- json_remove ------------------------------------------------------

    #[test]
    fn test_json_remove() {
        assert_eq!(txt(json_remove(&[v("[0,1,2,3,4]"), v("$[2]")]).unwrap()), "[0,1,3,4]");
        assert_eq!(txt(json_remove(&[v(r#"{"a":1,"b":2}"#), v("$.a")]).unwrap()), r#"{"b":2}"#);
        // Multi-path, applied left-to-right on the evolving doc: remove [2] then [0]
        assert_eq!(txt(json_remove(&[v("[0,1,2,3,4]"), v("$[2]"), v("$[0]")]).unwrap()), "[1,3,4]");
        // Non-existent path -> no-op
        assert_eq!(txt(json_remove(&[v(r#"{"a":1}"#), v("$.x")]).unwrap()), r#"{"a":1}"#);
        // No paths -> unchanged (minified)
        assert_eq!(txt(json_remove(&[v("[0,1,2,3,4]")]).unwrap()), "[0,1,2,3,4]");
        // NULL doc -> NULL
        assert_eq!(json_remove(&[SqlValue::Null, v("$.a")]).unwrap(), SqlValue::Null);
    }

    // ---- json_patch -------------------------------------------------------

    #[test]
    fn test_json_patch() {
        assert_eq!(
            txt(json_patch(&[v(r#"{"a":1,"b":2}"#), v(r#"{"c":3,"a":null}"#)]).unwrap()),
            r#"{"b":2,"c":3}"#
        );
        assert_eq!(
            txt(json_patch(&[v(r#"{"a":[1,2],"b":2}"#), v(r#"{"a":9}"#)]).unwrap()),
            r#"{"a":9,"b":2}"#
        );
        // Recursive merge with member deletion
        assert_eq!(
            txt(json_patch(&[v(r#"{"a":{"x":1,"y":2}}"#), v(r#"{"a":{"y":null,"z":3}}"#)]).unwrap()),
            r#"{"a":{"x":1,"z":3}}"#
        );
        // Non-object patch replaces target
        assert_eq!(txt(json_patch(&[v(r#"{"a":1}"#), v("[1,2,3]")]).unwrap()), "[1,2,3]");
        // Object patch over non-object target starts from {}
        assert_eq!(txt(json_patch(&[v("[1,2]"), v(r#"{"a":1}"#)]).unwrap()), r#"{"a":1}"#);
        // NULL either side -> NULL
        assert_eq!(json_patch(&[SqlValue::Null, v(r#"{"a":1}"#)]).unwrap(), SqlValue::Null);
        assert_eq!(json_patch(&[v(r#"{"a":1}"#), SqlValue::Null]).unwrap(), SqlValue::Null);
    }

    // ---- json_error_position ----------------------------------------------

    #[test]
    fn test_json_error_position() {
        // Valid -> 0
        assert_eq!(json_error_position(&[v(r#"{"a":1}"#)]).unwrap(), SqlValue::Integer(0));
        // Relaxed-valid (trailing comma) -> 0, matching sqlite3
        assert_eq!(json_error_position(&[v(r#"{"a":55,"b":72,}"#)]).unwrap(), SqlValue::Integer(0));
        assert_eq!(
            json_error_position(&[v(r#"{"a":55,"b":72 , }"#)]).unwrap(),
            SqlValue::Integer(0)
        );
        assert_eq!(json_error_position(&[v(r#"["a",55,"b",72,]"#)]).unwrap(), SqlValue::Integer(0));
        // Relaxed-valid unquoted key -> 0
        assert_eq!(json_error_position(&[v("{a:1}")]).unwrap(), SqlValue::Integer(0));
        // Double comma -> position of the second comma (1-based)
        assert_eq!(
            json_error_position(&[v(r#"{"a":55,"b":72,,}"#)]).unwrap(),
            SqlValue::Integer(16)
        );
        assert_eq!(
            json_error_position(&[v(r#"["a",55,"b",72,,]"#)]).unwrap(),
            SqlValue::Integer(16)
        );
        // NULL -> NULL
        assert_eq!(json_error_position(&[SqlValue::Null]).unwrap(), SqlValue::Null);
    }
}
