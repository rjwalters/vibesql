//! Batch string operations for columnar data
//!
//! This module provides vectorized string operations for columnar execution.
//! While true SIMD isn't practical for variable-length strings, we can still
//! optimize by:
//!
//! 1. **Length filtering**: Use SIMD to compare lengths first (i32 comparison)
//! 2. **Batch processing**: Process multiple strings without per-row allocations
//! 3. **Early termination**: Skip content comparison when lengths don't match
//! 4. **Prefix optimization**: For LIKE patterns, check fixed prefixes/suffixes first
//!
//! # Performance
//!
//! These optimizations provide 2-4x speedup for string equality and LIKE operations
//! compared to naive row-by-row evaluation with SqlValue enum matching overhead.

/// Batch string equality comparison
///
/// Compares each string in the column against a target value, returning a boolean mask.
/// Uses length filtering as a fast path - if lengths don't match, skip content comparison.
///
/// # Arguments
///
/// * `values` - Column of string values
/// * `nulls` - Optional null bitmap (true = null)
/// * `target` - Target string to compare against
///
/// # Returns
///
/// Boolean mask where true means the string equals the target
pub fn batch_string_eq(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    target: &str,
) -> Vec<bool> {
    let target_len = target.len();
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        // Fast path: length check before string comparison
        if value.len() != target_len {
            result.push(false);
        } else {
            result.push(&**value == target);
        }
    }

    result
}

/// Batch string not equal comparison
///
/// # Arguments
///
/// * `values` - Column of string values
/// * `nulls` - Optional null bitmap (true = null)
/// * `target` - Target string to compare against
///
/// # Returns
///
/// Boolean mask where true means the string does not equal the target
pub fn batch_string_ne(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    target: &str,
) -> Vec<bool> {
    let target_len = target.len();
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first - NULL comparisons return false
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        // Fast path: different lengths means not equal
        if value.len() != target_len {
            result.push(true);
        } else {
            result.push(&**value != target);
        }
    }

    result
}

/// Case-insensitive ASCII string comparison helper
/// Returns true if strings match ignoring ASCII case differences
#[inline]
fn ascii_eq_ignore_case(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).all(|(ac, bc)| ac.eq_ignore_ascii_case(&bc))
}

/// Case-insensitive ASCII starts_with check
#[inline]
fn ascii_starts_with_ignore_case(text: &str, prefix: &str) -> bool {
    if text.len() < prefix.len() {
        return false;
    }
    ascii_eq_ignore_case(&text[..prefix.len()], prefix)
}

/// Case-insensitive ASCII ends_with check
#[inline]
fn ascii_ends_with_ignore_case(text: &str, suffix: &str) -> bool {
    if text.len() < suffix.len() {
        return false;
    }
    ascii_eq_ignore_case(&text[text.len() - suffix.len()..], suffix)
}

/// Case-insensitive ASCII contains check
#[inline]
fn ascii_contains_ignore_case(text: &str, substring: &str) -> bool {
    if substring.is_empty() {
        return true;
    }
    if text.len() < substring.len() {
        return false;
    }
    // Naive search with case-insensitive comparison
    for i in 0..=(text.len() - substring.len()) {
        if ascii_eq_ignore_case(&text[i..i + substring.len()], substring) {
            return true;
        }
    }
    false
}

/// Batch string starts_with check (for LIKE 'prefix%' patterns)
///
/// Optimized for patterns like `column LIKE 'ABC%'`
/// Uses case-insensitive comparison following SQLite LIKE semantics.
///
/// # Arguments
///
/// * `values` - Column of string values
/// * `nulls` - Optional null bitmap (true = null)
/// * `prefix` - Prefix to check
///
/// # Returns
///
/// Boolean mask where true means the string starts with the prefix
pub fn batch_string_starts_with(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    prefix: &str,
) -> Vec<bool> {
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        // Case-insensitive prefix check
        result.push(ascii_starts_with_ignore_case(value, prefix));
    }

    result
}

/// Batch string ends_with check (for LIKE '%suffix' patterns)
///
/// Optimized for patterns like `column LIKE '%XYZ'`
/// Uses case-insensitive comparison following SQLite LIKE semantics.
///
/// # Arguments
///
/// * `values` - Column of string values
/// * `nulls` - Optional null bitmap (true = null)
/// * `suffix` - Suffix to check
///
/// # Returns
///
/// Boolean mask where true means the string ends with the suffix
pub fn batch_string_ends_with(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    suffix: &str,
) -> Vec<bool> {
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        // Case-insensitive suffix check
        result.push(ascii_ends_with_ignore_case(value, suffix));
    }

    result
}

/// Batch string contains check (for LIKE '%substring%' patterns)
///
/// Optimized for patterns like `column LIKE '%MIDDLE%'`
/// Uses case-insensitive comparison following SQLite LIKE semantics.
///
/// # Arguments
///
/// * `values` - Column of string values
/// * `nulls` - Optional null bitmap (true = null)
/// * `substring` - Substring to check
///
/// # Returns
///
/// Boolean mask where true means the string contains the substring
pub fn batch_string_contains(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    substring: &str,
) -> Vec<bool> {
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        // Case-insensitive contains check
        result.push(ascii_contains_ignore_case(value, substring));
    }

    result
}

/// Represents an optimized LIKE pattern for batch evaluation
#[derive(Debug, Clone)]
pub enum LikePattern {
    /// Exact match (no wildcards)
    Exact(String),
    /// Prefix match: 'prefix%'
    Prefix(String),
    /// Suffix match: '%suffix'
    Suffix(String),
    /// Contains match: '%substring%'
    Contains(String),
    /// Prefix and suffix match: 'prefix%suffix'
    PrefixSuffix { prefix: String, suffix: String },
    /// General pattern with wildcards (fallback to regex-like matching)
    General(String),
}

impl LikePattern {
    /// Parse a SQL LIKE pattern into an optimized form
    ///
    /// Handles standard SQL wildcards:
    /// - `%` matches any sequence of characters
    /// - `_` matches any single character
    ///
    /// Returns an optimized pattern type when possible, or General for complex patterns.
    pub fn parse(pattern: &str) -> Self {
        // Check for single character wildcards - these need general matching
        if pattern.contains('_') {
            return LikePattern::General(pattern.to_string());
        }

        let percent_count = pattern.matches('%').count();

        match percent_count {
            0 => {
                // No wildcards - exact match
                LikePattern::Exact(pattern.to_string())
            }
            1 => {
                if pattern.starts_with('%') && pattern.ends_with('%') && pattern.len() > 1 {
                    // Pattern is just '%' (matches everything) or '%x%' - but '%' alone is special
                    if pattern == "%" {
                        LikePattern::General(pattern.to_string())
                    } else {
                        // '%substring%' - but need to check it's not just '%%'
                        let inner = &pattern[1..pattern.len() - 1];
                        if inner.is_empty() || inner.contains('%') {
                            LikePattern::General(pattern.to_string())
                        } else {
                            LikePattern::Contains(inner.to_string())
                        }
                    }
                } else if let Some(prefix) = pattern.strip_suffix('%') {
                    // 'prefix%'
                    LikePattern::Prefix(prefix.to_string())
                } else if let Some(suffix) = pattern.strip_prefix('%') {
                    // '%suffix'
                    LikePattern::Suffix(suffix.to_string())
                } else {
                    // Single % in middle - 'prefix%suffix' pattern
                    let parts: Vec<&str> = pattern.split('%').collect();
                    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                        LikePattern::PrefixSuffix {
                            prefix: parts[0].to_string(),
                            suffix: parts[1].to_string(),
                        }
                    } else {
                        LikePattern::General(pattern.to_string())
                    }
                }
            }
            2 => {
                // Check for '%substring%' or 'prefix%suffix'
                if pattern.starts_with('%') && pattern.ends_with('%') {
                    let inner = &pattern[1..pattern.len() - 1];
                    if !inner.contains('%') {
                        LikePattern::Contains(inner.to_string())
                    } else {
                        LikePattern::General(pattern.to_string())
                    }
                } else if !pattern.starts_with('%') && !pattern.ends_with('%') {
                    // 'prefix%middle%suffix' - too complex
                    LikePattern::General(pattern.to_string())
                } else if pattern.starts_with('%') {
                    // '%mid%suffix' - complex
                    LikePattern::General(pattern.to_string())
                } else if pattern.ends_with('%') {
                    // 'prefix%mid%' - complex
                    LikePattern::General(pattern.to_string())
                } else {
                    // Check for simple 'prefix%suffix' pattern
                    let parts: Vec<&str> = pattern.split('%').collect();
                    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                        LikePattern::PrefixSuffix {
                            prefix: parts[0].to_string(),
                            suffix: parts[1].to_string(),
                        }
                    } else {
                        LikePattern::General(pattern.to_string())
                    }
                }
            }
            _ => {
                // Multiple wildcards - use general matching
                LikePattern::General(pattern.to_string())
            }
        }
    }
}

/// Batch LIKE pattern matching
///
/// Evaluates a SQL LIKE pattern against a column of strings.
/// Uses optimized paths for common patterns (prefix, suffix, contains).
///
/// # Arguments
///
/// * `values` - Column of string values
/// * `nulls` - Optional null bitmap (true = null)
/// * `pattern` - Pre-parsed LIKE pattern
///
/// # Returns
///
/// Boolean mask where true means the string matches the pattern
pub fn batch_string_like(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    pattern: &LikePattern,
) -> Vec<bool> {
    match pattern {
        LikePattern::Exact(s) => batch_string_eq_ignore_case(values, nulls, s),
        LikePattern::Prefix(prefix) => batch_string_starts_with(values, nulls, prefix),
        LikePattern::Suffix(suffix) => batch_string_ends_with(values, nulls, suffix),
        LikePattern::Contains(substring) => batch_string_contains(values, nulls, substring),
        LikePattern::PrefixSuffix { prefix, suffix } => {
            batch_string_prefix_suffix(values, nulls, prefix, suffix)
        }
        LikePattern::General(pattern) => batch_string_like_general(values, nulls, pattern),
    }
}

/// Batch case-insensitive string equality (for LIKE patterns without wildcards)
fn batch_string_eq_ignore_case(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    target: &str,
) -> Vec<bool> {
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        // Case-insensitive equality
        result.push(ascii_eq_ignore_case(value, target));
    }

    result
}

/// Batch prefix and suffix match (for 'prefix%suffix' patterns)
/// Uses case-insensitive comparison following SQLite LIKE semantics.
fn batch_string_prefix_suffix(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    prefix: &str,
    suffix: &str,
) -> Vec<bool> {
    let min_len = prefix.len() + suffix.len();
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        // Fast path: string must be at least prefix + suffix length
        if value.len() < min_len {
            result.push(false);
        } else {
            // Case-insensitive prefix and suffix check
            result.push(
                ascii_starts_with_ignore_case(value, prefix)
                    && ascii_ends_with_ignore_case(value, suffix),
            );
        }
    }

    result
}

/// A single fixed (non-`%`) segment of a general LIKE pattern.
///
/// A general pattern is decomposed into `%`-delimited segments. Each segment is
/// a run of literal characters interleaved with `_` (match-any-single-char)
/// wildcards. Because `%` matches zero-or-more characters, matching reduces to
/// finding each successive segment somewhere at/after the current text cursor,
/// with the first and last segments anchored to the ends when the pattern does
/// not start/end with `%`.
#[derive(Debug)]
struct Segment {
    /// The `_`-delimited literal chunks, in order. A segment like `a_bc_` has
    /// chunks `["a", "bc", ""]` separated by two `_` wildcards. Chunks are byte
    /// slices into the pattern; empty chunks are meaningful (they encode where a
    /// `_` sits relative to literals).
    chunks: Vec<Vec<u8>>,
    /// Number of `_` wildcards in this segment (== `chunks.len() - 1`). Each `_`
    /// consumes exactly one Unicode character of text.
    underscores: usize,
    /// Total number of literal bytes across all chunks. Used for the length
    /// fast-reject: a segment needs at least `literal_bytes` bytes plus one byte
    /// per `_` (a UTF-8 char is at least one byte) of text.
    literal_bytes: usize,
}

/// Pre-parsed general LIKE pattern, built once per batch and reused for every
/// row. This replaces the old per-row `pattern.chars().collect()` + DP matrix.
#[derive(Debug)]
struct GeneralMatcher {
    /// The fixed segments between `%` wildcards, in order.
    segments: Vec<Segment>,
    /// True if the pattern begins with `%` (first segment is unanchored / may
    /// match anywhere at or after the start).
    leading_percent: bool,
    /// True if the pattern ends with `%` (last segment need not reach the end).
    trailing_percent: bool,
    /// Minimum number of text bytes any match must contain: sum of every
    /// segment's `literal_bytes` plus one byte per `_`. Rows shorter than this
    /// are rejected without any character scanning.
    min_len: usize,
    /// True if the pattern contained at least one `%`. When there are no fixed
    /// segments, this distinguishes a pattern that is purely `%` wildcards
    /// (matches any text) from an empty pattern (matches only empty text).
    has_percent: bool,
}

impl GeneralMatcher {
    fn parse(pattern: &str) -> Self {
        let bytes = pattern.as_bytes();
        let leading_percent = bytes.first() == Some(&b'%');
        let trailing_percent = bytes.last() == Some(&b'%');
        let has_percent = bytes.contains(&b'%');

        let mut segments = Vec::new();
        // Split on `%` into segments; each segment is a run of literals and `_`.
        for raw in bytes.split(|&b| b == b'%') {
            // `split` yields an empty slice for the region before a leading `%`,
            // after a trailing `%`, and between adjacent `%%`. An empty segment
            // matches the empty string and imposes no constraint, so drop it —
            // the leading/trailing flags already carry the anchoring semantics.
            if raw.is_empty() {
                continue;
            }
            let mut chunks: Vec<Vec<u8>> = Vec::new();
            let mut literal_bytes = 0usize;
            for chunk in raw.split(|&b| b == b'_') {
                literal_bytes += chunk.len();
                chunks.push(chunk.to_vec());
            }
            let underscores = chunks.len() - 1;
            segments.push(Segment { chunks, underscores, literal_bytes });
        }

        let min_len: usize = segments.iter().map(|s| s.literal_bytes + s.underscores).sum();

        GeneralMatcher { segments, leading_percent, trailing_percent, min_len, has_percent }
    }

    /// Match `text` against this pre-parsed pattern.
    ///
    /// Allocation-free: walks a byte cursor through `text`, matching each
    /// segment in turn via a two-pointer scan (with `memchr::memmem` for the
    /// literal-chunk search). SQLite LIKE semantics preserved: ASCII letters
    /// case-fold (A-Z == a-z), non-ASCII bytes match exactly, `_` consumes
    /// exactly one Unicode character, `%` consumes zero or more.
    fn matches(&self, text: &str) -> bool {
        let text = text.as_bytes();

        // Length fast-reject: too short to possibly match.
        if text.len() < self.min_len {
            return false;
        }

        // No fixed segments. Either the pattern is purely `%` wildcards (matches
        // any text) or it is the empty pattern (matches only the empty text).
        if self.segments.is_empty() {
            return self.has_percent || text.is_empty();
        }

        let seg_count = self.segments.len();

        // Special case: a single segment with neither leading nor trailing `%`
        // must match the entire text exactly (e.g. `appl_` or `a_c`).
        if seg_count == 1 && !self.leading_percent && !self.trailing_percent {
            return matches!(seg_match_at(text, 0, &self.segments[0]), Some(end) if end == text.len());
        }

        let mut pos = 0usize; // current byte cursor into text
        let mut lo = 0usize; // first segment index still to place
        let mut hi = seg_count; // one past the last segment still to place

        // Anchor the first segment to the start when there is no leading `%`.
        if !self.leading_percent {
            match seg_match_at(text, 0, &self.segments[0]) {
                Some(end) => {
                    pos = end;
                    lo = 1;
                }
                None => return false,
            }
        }

        // Anchor the last segment to the end when there is no trailing `%`.
        // `end_limit` is the exclusive upper bound the floating segments must
        // finish before. (Guarded by `lo < seg_count` so the segment consumed by
        // the start anchor is not also treated as the end anchor.)
        let mut end_limit = text.len();
        if !self.trailing_percent && lo < seg_count {
            let last = &self.segments[seg_count - 1];
            match seg_match_ending_before(text, pos, last, text.len()) {
                Some(start) => {
                    end_limit = start;
                    hi = seg_count - 1;
                }
                None => return false,
            }
        }

        // Remaining segments `[lo, hi)` are all floating: find each in turn,
        // earliest-first, staying within `[pos, end_limit)`.
        for seg in &self.segments[lo..hi] {
            match seg_find(text, pos, seg) {
                Some((_start, end)) if end <= end_limit => pos = end,
                _ => return false,
            }
        }

        true
    }
}

/// Case-insensitive (ASCII) equality of two equal-length byte slices, matching
/// SQLite LIKE literal semantics (ASCII letters fold, other bytes exact).
#[inline]
fn ascii_ci_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|(&x, &y)| {
            if x.is_ascii_alphabetic() && y.is_ascii_alphabetic() {
                x.eq_ignore_ascii_case(&y)
            } else {
                x == y
            }
        })
}

/// Number of bytes in the UTF-8 character starting at `text[pos]`.
#[inline]
fn utf8_char_len(byte: u8) -> usize {
    if byte & 0x80 == 0 {
        1
    } else if byte & 0xE0 == 0xC0 {
        2
    } else if byte & 0xF0 == 0xE0 {
        3
    } else if byte & 0xF8 == 0xF0 {
        4
    } else {
        1
    }
}

/// Case-insensitive (ASCII) `memmem`-style search for `needle` in `haystack`,
/// returning the byte offset of the first occurrence at or after offset 0.
///
/// Fast path: when `needle` is pure ASCII with no alphabetic bytes (no case
/// folding needed) OR we simply want raw byte search, `memchr::memmem` gives a
/// SIMD-accelerated scan. LIKE folds ASCII letters, so we anchor the search on
/// the needle's first byte via `memchr` and verify with `ascii_ci_eq`.
#[inline]
fn ascii_ci_find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > haystack.len() {
        return None;
    }

    let first = needle[0];
    // If the needle contains no ASCII letters, case folding is a no-op and we
    // can use memmem directly (SIMD-accelerated substring search).
    if !needle.iter().any(|b| b.is_ascii_alphabetic()) {
        return memchr::memmem::find(haystack, needle);
    }

    // Otherwise anchor on the first byte (folded to both cases if alphabetic)
    // and verify candidates case-insensitively.
    let last_start = haystack.len() - needle.len();
    if first.is_ascii_alphabetic() {
        let lower = first.to_ascii_lowercase();
        let upper = first.to_ascii_uppercase();
        let mut i = 0usize;
        while i <= last_start {
            // Find the next candidate first byte (either case) via memchr. No
            // candidate anywhere in the remaining window means no match at all.
            let rest = &haystack[i..=last_start];
            let off = memchr::memchr2(lower, upper, rest)?;
            let cand = i + off;
            if ascii_ci_eq(&haystack[cand..cand + needle.len()], needle) {
                return Some(cand);
            }
            i = cand + 1;
        }
        None
    } else {
        let mut i = 0usize;
        while i <= last_start {
            let rest = &haystack[i..=last_start];
            let off = memchr::memchr(first, rest)?;
            let cand = i + off;
            if ascii_ci_eq(&haystack[cand..cand + needle.len()], needle) {
                return Some(cand);
            }
            i = cand + 1;
        }
        None
    }
}

/// Try to match `seg` starting exactly at byte offset `start` in `text`.
/// Returns the byte offset immediately after the match on success.
///
/// A segment is `chunk_0 _ chunk_1 _ ... _ chunk_k`: literal chunks separated by
/// single-character wildcards. The chunks are anchored relative to each other,
/// so we match `chunk_0` at `start`, skip one UTF-8 char for the `_`, match
/// `chunk_1` immediately after, etc.
fn seg_match_at(text: &[u8], start: usize, seg: &Segment) -> Option<usize> {
    let mut pos = start;
    let last = seg.chunks.len() - 1;
    for (i, chunk) in seg.chunks.iter().enumerate() {
        if pos + chunk.len() > text.len() {
            return None;
        }
        if !ascii_ci_eq(&text[pos..pos + chunk.len()], chunk) {
            return None;
        }
        pos += chunk.len();
        // A `_` follows every chunk except the last one.
        if i != last {
            if pos >= text.len() {
                return None;
            }
            pos += utf8_char_len(text[pos]);
        }
    }
    Some(pos)
}

/// Find the earliest position at or after `from` where `seg` matches, returning
/// `(match_start, match_end)` byte offsets.
fn seg_find(text: &[u8], from: usize, seg: &Segment) -> Option<(usize, usize)> {
    let first_chunk = &seg.chunks[0];

    // When the segment starts with an empty chunk (a leading `_`), there is no
    // literal anchor to search for; probe each candidate start position.
    if first_chunk.is_empty() {
        let mut start = from;
        while start <= text.len() {
            if let Some(end) = seg_match_at(text, start, seg) {
                return Some((start, end));
            }
            if start >= text.len() {
                break;
            }
            start += utf8_char_len(text[start]);
        }
        return None;
    }

    // Otherwise, use the (SIMD-accelerated) substring search to jump to each
    // candidate occurrence of the first literal chunk, then verify the rest.
    let mut base = from;
    while base <= text.len() {
        let off = ascii_ci_find(&text[base..], first_chunk)?;
        let cand = base + off;
        if let Some(end) = seg_match_at(text, cand, seg) {
            return Some((cand, end));
        }
        base = cand + 1;
        if base > text.len() {
            break;
        }
    }
    None
}

/// Find the *latest* start position in `[from, end]` at which `seg` matches and
/// ends exactly at byte offset `end`. Returns that start on success.
///
/// Used for the final, end-anchored segment (no trailing `%`): it must consume
/// through the end of the text. Choosing the latest valid start maximizes the
/// room `[from, start)` left for the floating segments that must fit before it.
fn seg_match_ending_before(text: &[u8], from: usize, seg: &Segment, end: usize) -> Option<usize> {
    let mut start = from;
    let mut best: Option<usize> = None;
    while start <= end {
        if let Some(match_end) = seg_match_at(text, start, seg) {
            if match_end == end {
                best = Some(start);
            }
        }
        if start >= end {
            break;
        }
        start += utf8_char_len(text[start]);
    }
    best
}

/// General LIKE pattern matching (fallback for complex patterns)
///
/// Handles patterns with `_` wildcards and complex `%` combinations.
///
/// The pattern is parsed once per batch into a [`GeneralMatcher`] (hoisting the
/// old per-row `pattern.chars().collect()` out of the row loop), and each row is
/// matched with an allocation-free two-pointer scan — replacing the previous
/// per-row `O(m*n)` `Vec<Vec<bool>>` dynamic-programming matrix.
fn batch_string_like_general(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    pattern: &str,
) -> Vec<bool> {
    let matcher = GeneralMatcher::parse(pattern);
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        result.push(matcher.matches(value));
    }

    result
}

/// Match a string against a SQL LIKE pattern (convenience wrapper used by
/// in-module unit tests). Parses the pattern and matches a single string.
#[cfg(test)]
fn like_match(text: &str, pattern: &str) -> bool {
    GeneralMatcher::parse(pattern).matches(text)
}

/// Batch string less than comparison
///
/// Lexicographic comparison for string ordering.
///
/// # Arguments
///
/// * `values` - Column of string values
/// * `nulls` - Optional null bitmap (true = null)
/// * `target` - Target string to compare against
///
/// # Returns
///
/// Boolean mask where true means the string is less than target
pub fn batch_string_lt(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    target: &str,
) -> Vec<bool> {
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        result.push(&**value < target);
    }

    result
}

/// Batch string greater than comparison
pub fn batch_string_gt(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    target: &str,
) -> Vec<bool> {
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        result.push(&**value > target);
    }

    result
}

/// Batch string less than or equal comparison
pub fn batch_string_le(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    target: &str,
) -> Vec<bool> {
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        result.push(&**value <= target);
    }

    result
}

/// Batch string greater than or equal comparison
pub fn batch_string_ge(
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
    target: &str,
) -> Vec<bool> {
    let mut result = Vec::with_capacity(values.len());

    for (i, value) in values.iter().enumerate() {
        // Check for NULL first
        if let Some(null_mask) = nulls {
            if null_mask[i] {
                result.push(false);
                continue;
            }
        }

        result.push(&**value >= target);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_string_eq() {
        let values = vec![
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("banana"),
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("cherry"),
        ];

        let result = batch_string_eq(&values, None, "apple");
        assert_eq!(result, vec![true, false, true, false]);
    }

    #[test]
    fn test_batch_string_eq_with_nulls() {
        let values = vec![
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("banana"),
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("cherry"),
        ];
        let nulls = Some(vec![false, false, true, false]);

        let result = batch_string_eq(&values, nulls.as_deref(), "apple");
        assert_eq!(result, vec![true, false, false, false]);
    }

    #[test]
    fn test_batch_string_ne() {
        let values = vec![
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("banana"),
            std::sync::Arc::from("apple"),
        ];

        let result = batch_string_ne(&values, None, "apple");
        assert_eq!(result, vec![false, true, false]);
    }

    #[test]
    fn test_batch_string_starts_with() {
        let values = vec![
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("apricot"),
            std::sync::Arc::from("banana"),
            std::sync::Arc::from("app"),
        ];

        let result = batch_string_starts_with(&values, None, "app");
        assert_eq!(result, vec![true, false, false, true]);
    }

    #[test]
    fn test_batch_string_ends_with() {
        let values = vec![
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("pineapple"),
            std::sync::Arc::from("banana"),
            std::sync::Arc::from("le"),
        ];

        let result = batch_string_ends_with(&values, None, "le");
        assert_eq!(result, vec![true, true, false, true]);
    }

    #[test]
    fn test_batch_string_contains() {
        let values = vec![
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("pineapple"),
            std::sync::Arc::from("banana"),
            std::sync::Arc::from("application"),
        ];

        let result = batch_string_contains(&values, None, "app");
        assert_eq!(result, vec![true, true, false, true]);
    }

    #[test]
    fn test_like_pattern_parse() {
        // Exact match
        assert!(matches!(LikePattern::parse("apple"), LikePattern::Exact(_)));

        // Prefix match
        assert!(matches!(LikePattern::parse("app%"), LikePattern::Prefix(_)));

        // Suffix match
        assert!(matches!(LikePattern::parse("%le"), LikePattern::Suffix(_)));

        // Contains match
        assert!(matches!(LikePattern::parse("%app%"), LikePattern::Contains(_)));

        // Prefix and suffix match
        assert!(matches!(LikePattern::parse("a%e"), LikePattern::PrefixSuffix { .. }));

        // General (has underscore)
        assert!(matches!(LikePattern::parse("a_ple"), LikePattern::General(_)));
    }

    #[test]
    fn test_batch_string_like_prefix() {
        let values = vec![
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("apricot"),
            std::sync::Arc::from("banana"),
        ];

        let pattern = LikePattern::parse("ap%");
        let result = batch_string_like(&values, None, &pattern);
        assert_eq!(result, vec![true, true, false]);
    }

    #[test]
    fn test_batch_string_like_suffix() {
        let values = vec![
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("pineapple"),
            std::sync::Arc::from("banana"),
        ];

        let pattern = LikePattern::parse("%ple");
        let result = batch_string_like(&values, None, &pattern);
        assert_eq!(result, vec![true, true, false]);
    }

    #[test]
    fn test_batch_string_like_contains() {
        let values = vec![
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("pineapple"),
            std::sync::Arc::from("banana"),
        ];

        let pattern = LikePattern::parse("%app%");
        let result = batch_string_like(&values, None, &pattern);
        assert_eq!(result, vec![true, true, false]);
    }

    #[test]
    fn test_batch_string_like_prefix_suffix() {
        let values = vec![
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("ample"),
            std::sync::Arc::from("banana"),
            std::sync::Arc::from("ale"),
        ];

        let pattern = LikePattern::parse("a%le");
        let result = batch_string_like(&values, None, &pattern);
        assert_eq!(result, vec![true, true, false, true]);
    }

    #[test]
    fn test_like_match_underscore() {
        // _ matches exactly one character
        assert!(like_match("apple", "appl_"));
        assert!(like_match("apply", "appl_"));
        assert!(!like_match("appl", "appl_"));
        assert!(!like_match("applee", "appl_"));

        assert!(like_match("abc", "a_c"));
        assert!(like_match("axc", "a_c"));
        assert!(!like_match("ac", "a_c"));
        assert!(!like_match("abbc", "a_c"));
    }

    #[test]
    fn test_like_match_percent() {
        // % matches zero or more characters
        assert!(like_match("apple", "app%"));
        assert!(like_match("app", "app%"));
        assert!(!like_match("ap", "app%"));

        assert!(like_match("apple", "%ple"));
        assert!(like_match("ple", "%ple"));
        assert!(!like_match("pl", "%ple"));

        assert!(like_match("pineapple", "%app%"));
        assert!(like_match("apple", "%app%"));
        assert!(like_match("app", "%app%"));
    }

    #[test]
    fn test_like_match_complex() {
        // Combined patterns
        assert!(like_match("apple", "a%e"));
        assert!(like_match("axe", "a%e"));
        assert!(like_match("ae", "a%e"));
        assert!(!like_match("axf", "a%e"));

        assert!(like_match("apple", "a_p%"));
        assert!(like_match("axppp", "a_p%"));
        assert!(like_match("appp", "a_p%")); // a + one char (p) + p + %
        assert!(!like_match("ap", "a_p%")); // too short - needs at least 3 chars
    }

    #[test]
    fn test_batch_string_comparisons() {
        let values = vec![
            std::sync::Arc::from("apple"),
            std::sync::Arc::from("banana"),
            std::sync::Arc::from("cherry"),
        ];

        // Less than
        let result = batch_string_lt(&values, None, "banana");
        assert_eq!(result, vec![true, false, false]);

        // Greater than
        let result = batch_string_gt(&values, None, "banana");
        assert_eq!(result, vec![false, false, true]);

        // Less than or equal
        let result = batch_string_le(&values, None, "banana");
        assert_eq!(result, vec![true, true, false]);

        // Greater than or equal
        let result = batch_string_ge(&values, None, "banana");
        assert_eq!(result, vec![false, true, true]);
    }

    #[test]
    fn test_empty_values() {
        let values: Vec<std::sync::Arc<str>> = vec![];

        let result = batch_string_eq(&values, None, "test");
        assert!(result.is_empty());

        let pattern = LikePattern::parse("test%");
        let result = batch_string_like(&values, None, &pattern);
        assert!(result.is_empty());
    }

    #[test]
    fn test_empty_pattern() {
        let values: Vec<std::sync::Arc<str>> =
            vec![std::sync::Arc::from(""), std::sync::Arc::from("a"), std::sync::Arc::from("")];

        // Exact match empty string
        let result = batch_string_eq(&values, None, "");
        assert_eq!(result, vec![true, false, true]);

        // Prefix match with empty prefix (matches everything)
        let pattern = LikePattern::parse("%");
        let result = batch_string_like(&values, None, &pattern);
        assert_eq!(result, vec![true, true, true]);
    }

    /// Reference matcher: the row-path LIKE evaluator (case-insensitive, no
    /// escape) — the ground truth the columnar `GeneralMatcher` must replicate.
    fn reference(text: &str, pattern: &str) -> bool {
        crate::evaluator::pattern::like_match(text, pattern, false, None)
    }

    /// Assert the new allocation-free matcher agrees with the row-path reference
    /// for every (text, pattern) pair in the cartesian product.
    fn assert_parity(texts: &[&str], patterns: &[&str]) {
        for &p in patterns {
            let matcher = GeneralMatcher::parse(p);
            for &t in texts {
                assert_eq!(
                    matcher.matches(t),
                    reference(t, p),
                    "mismatch: text={t:?} pattern={p:?} (columnar={}, row={})",
                    matcher.matches(t),
                    reference(t, p),
                );
            }
        }
    }

    #[test]
    fn general_matcher_parity_interior_percent() {
        // The TPC-H Q13 shape: `%special%requests%`.
        let texts = &[
            "special requests",
            "the special requests here",
            "specialrequests",
            "special",
            "requests special",
            "no match at all",
            "specialrequest",
            "",
        ];
        let patterns =
            &["%special%requests%", "special%requests", "%special%requests", "special%requests%"];
        assert_parity(texts, patterns);
    }

    #[test]
    fn general_matcher_parity_underscores() {
        let texts = &["abc", "aXc", "abbc", "ac", "a_c", "abcd", "xabc", ""];
        let patterns = &["a_c", "a__c", "_bc", "ab_", "___", "a_c_", "_a_c_"];
        assert_parity(texts, patterns);
    }

    #[test]
    fn general_matcher_parity_mixed_wildcards() {
        // Q16 shape: `p_type LIKE 'MEDIUM%POLISHED%'` plus `_`/`%` mixes.
        let texts = &[
            "MEDIUM POLISHED BRASS",
            "MEDIUM ANODIZED POLISHED STEEL",
            "SMALL POLISHED COPPER",
            "MEDIUMPOLISHED",
            "medium polished tin",
            "",
        ];
        let patterns = &[
            "MEDIUM%POLISHED%",
            "%MEDIUM%POLISHED%",
            "M_DIUM%POLISHED%",
            "%P_LISHED%",
            "MEDIUM%P_LISHED%STEEL",
        ];
        assert_parity(texts, patterns);
    }

    #[test]
    fn general_matcher_parity_adjacent_and_edge_wildcards() {
        let texts = &["", "a", "ab", "abc", "aabbcc", "%literal%", "__"];
        let patterns =
            &["%%", "_%", "%_", "%_%", "_%_", "a%%b", "a__%b", "%", "%a%", "a%", "%a", "%%%"];
        assert_parity(texts, patterns);
    }

    #[test]
    fn general_matcher_parity_pattern_longer_than_rows() {
        let texts = &["a", "ab", "", "xy"];
        // Patterns whose minimum length exceeds short rows -> length fast-reject.
        let patterns = &["a_c%def", "%abcdef%", "____", "a%bcdefghij"];
        assert_parity(texts, patterns);
    }

    #[test]
    fn general_matcher_parity_ascii_case_folding() {
        // ASCII letters fold; digits/punctuation do not.
        let texts = &["ABC", "abc", "AbC", "a1c", "A_C", "123"];
        let patterns = &["a_c", "A%C", "%bC%", "a%C", "_2_"];
        assert_parity(texts, patterns);
    }

    #[test]
    fn general_matcher_parity_non_ascii_no_fold() {
        // Non-ASCII text: ASCII case-fold must NOT fold non-ASCII bytes, and `_`
        // consumes exactly one (possibly multi-byte) Unicode character.
        let texts = &[
            "café",
            "CAFÉ",
            "naïve",
            "Ωmega",
            "über",
            "a\u{1234}c", // aሴc (3-byte char)
            "a😀c",       // 4-byte char
        ];
        let patterns = &[
            "caf_",
            "caf%",
            "%é",
            "a_c",        // _ must match one whole Unicode char
            "%\u{1234}%", // interior multi-byte literal
            "a%c",
        ];
        assert_parity(texts, patterns);
    }

    /// Exhaustive deterministic fuzz: every text and pattern of length up to 4
    /// over the alphabet {a, b, %, _} — the columnar matcher must agree with the
    /// row path on every pair. This covers adjacency, ordering, and boundary
    /// cases far beyond the hand-written matrices.
    #[test]
    fn general_matcher_exhaustive_small_alphabet_parity() {
        let alphabet = ['a', 'b', '%', '_'];

        // Build all strings of length 0..=max over `alphabet`.
        fn all_strings(alphabet: &[char], max: usize) -> Vec<String> {
            let mut out = vec![String::new()];
            let mut frontier = vec![String::new()];
            for _ in 0..max {
                let mut next = Vec::new();
                for s in &frontier {
                    for &c in alphabet {
                        let mut t = s.clone();
                        t.push(c);
                        next.push(t);
                    }
                }
                out.extend(next.iter().cloned());
                frontier = next;
            }
            out
        }

        // Texts use only {a, b} (no wildcards); patterns use the full alphabet.
        let texts = all_strings(&['a', 'b'], 4);
        let patterns = all_strings(&alphabet, 4);

        for p in &patterns {
            let matcher = GeneralMatcher::parse(p);
            for t in &texts {
                let got = matcher.matches(t);
                let want = reference(t, p);
                assert_eq!(got, want, "mismatch: text={t:?} pattern={p:?}");
            }
        }
    }
}
