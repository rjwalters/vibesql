//! Temporal probe-bound coercion (issue #5333)
//!
//! Index probes are evaluated with `SqlValue`'s total order, which falls back
//! to type-tag ordering for cross-type pairs (`Varchar`=10 < `Date`=12 <
//! `Timestamp`=14, cross-type `PartialEq` = false). When an index stores
//! temporal keys (e.g. an expression index on `date(y)` / `datetime(b)`, or a
//! plain `TIMESTAMP` column index) and the WHERE clause supplies *string*
//! bounds, raw probes are wrong in both directions:
//!
//! - equality / upper-bounded probes return 0 rows (silent row loss), because
//!   every temporal key sorts *above* every string key;
//! - lower-bounded probes return every temporal key (over-return), which
//!   surfaces as wrong rows whenever the planner decides the WHERE clause is
//!   fully satisfied by the index and skips the residual filter.
//!
//! The fix is to coerce string probe bounds into the stored temporal key type
//! *before* probing, mirroring the executor's comparison semantics exactly
//! (see `crate::evaluator::operators::comparison`, established by #5329):
//!
//! - `Date` vs string: parse-first via `Date::from_str`; unparseable strings
//!   raise a type mismatch in the executor, so we *decline* the probe and let
//!   the fallback path surface the identical error.
//! - `Timestamp` / `Time` vs string: the executor compares the TEXT
//!   *renderings* lexicographically (SQLite's `datetime()` returns TEXT). For
//!   strings that round-trip through parse → Display the coerced bound is
//!   exactly equivalent. For date-only strings (`'2017-07-04'` against
//!   `Timestamp` keys) the rendering of any timestamp on that date is a
//!   longer string with the bound as prefix, so it compares strictly
//!   *greater* than the bound. That makes the text-true set:
//!   - `>= s` and `> s`  ⇔  `key >= parse(s)` (midnight included in both),
//!   - `<= s` and `< s`  ⇔  `key <  parse(s)` (midnight excluded from both),
//!   - `= s`             ⇔  empty (no rendering equals a date-only string),
//!   which we encode by coercing lower bounds to inclusive and upper bounds
//!   to exclusive. Equality probes arrive as `start == end` ranges, so the
//!   two rules compose to the half-open empty range `[T, T)` automatically.
//!   Anything that doesn't round-trip and isn't a strict rendering prefix
//!   (junk strings, `T`-separated ISO forms, ...) is *declined*: the caller
//!   drops the index predicate and the row set is computed by the
//!   full-index-scan + WHERE-filter path, which evaluates with executor
//!   semantics. (Since #5332 fractions render padded to a minimum of 3
//!   digits, so trailing-zero strings like `'...44.500'` round-trip exactly
//!   and shorter ones like `'...44.5'` fall under the prefix rule — both
//!   stay on the index path.)
//!
//! The invariant being preserved: **index probe results == full-scan + WHERE
//! results under VibeSQL's own comparison semantics.**

use std::str::FromStr;

use vibesql_storage::database::indexes::IndexData;
use vibesql_types::SqlValue;

use super::IndexPredicate;

/// The temporal type of a stored index key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemporalKeyType {
    Date,
    Timestamp,
    Time,
}

/// Which side of a range a bound sits on (determines inclusivity adjustment
/// for prefix-matching strings under TEXT-rendering semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundSide {
    Lower,
    Upper,
}

/// Result of coercing a single probe bound.
enum CoercedBound {
    /// Bound is not affected (not a string, or key type not temporal).
    Unchanged,
    /// Bound replaced with a temporal value; inclusivity may have changed.
    Coerced { value: SqlValue, inclusive: bool },
    /// No faithful coercion exists; the caller must drop the index predicate
    /// and fall back to the filtered full-index-scan path.
    Decline,
}

/// Result of coercing an equality lookup key component (fast-path point
/// lookups build composite keys directly rather than range predicates).
pub(crate) enum EqualityKeyCoercion {
    /// Key component unaffected.
    Unchanged,
    /// Key component replaced with the coerced temporal value.
    Coerced(SqlValue),
    /// The index cannot be probed faithfully with this key (unparseable or
    /// non-round-tripping string); the caller should skip this index and let
    /// a slower path compute the result with executor semantics.
    Unusable,
}

fn temporal_key_type(sample: &SqlValue) -> Option<TemporalKeyType> {
    match sample {
        SqlValue::Date(_) => Some(TemporalKeyType::Date),
        SqlValue::Timestamp(_) => Some(TemporalKeyType::Timestamp),
        SqlValue::Time(_) => Some(TemporalKeyType::Time),
        _ => None,
    }
}

fn is_string(value: &SqlValue) -> bool {
    matches!(value, SqlValue::Varchar(_) | SqlValue::Character(_))
}

fn as_str(value: &SqlValue) -> Option<&str> {
    match value {
        SqlValue::Varchar(s) | SqlValue::Character(s) => Some(s.as_str()),
        _ => None,
    }
}

/// The minimal value of a temporal key type, used to pin an inclusive lower
/// bound on otherwise lower-unbounded coerced ranges. This keeps NULL keys
/// (and any other lower-sorting type tags) out of the probe range, matching
/// SQL semantics where `NULL < x` is NULL (not true). The column-index path
/// has a separate NULL row filter, but expression indexes cannot identify a
/// column to filter, so the probe range itself must exclude NULL keys.
fn temporal_min_value(key_type: TemporalKeyType) -> SqlValue {
    let min_date = vibesql_types::Date { year: i32::MIN, month: 1, day: 1 };
    let midnight = vibesql_types::Time { hour: 0, minute: 0, second: 0, nanosecond: 0 };
    match key_type {
        TemporalKeyType::Date => SqlValue::Date(min_date),
        TemporalKeyType::Timestamp => {
            SqlValue::Timestamp(vibesql_types::Timestamp::new(min_date, midnight))
        }
        TemporalKeyType::Time => SqlValue::Time(midnight),
    }
}

/// Coerce one string bound against a temporal key type.
///
/// `inclusive` is the bound's current inclusivity; the returned
/// [`CoercedBound::Coerced`] carries the (possibly adjusted) inclusivity.
fn coerce_string_bound(
    key_type: TemporalKeyType,
    bound: &SqlValue,
    inclusive: bool,
    side: BoundSide,
) -> CoercedBound {
    let Some(s) = as_str(bound) else {
        return CoercedBound::Unchanged;
    };

    match key_type {
        // Date vs string is parse-first in the executor (type-mismatch error
        // on unparseable strings), so plain coercion is exactly equivalent.
        TemporalKeyType::Date => match vibesql_types::Date::from_str(s) {
            Ok(d) => CoercedBound::Coerced { value: SqlValue::Date(d), inclusive },
            Err(_) => CoercedBound::Decline,
        },
        // Timestamp/Time vs string compares TEXT renderings in the executor.
        TemporalKeyType::Timestamp => match vibesql_types::Timestamp::from_str(s) {
            Ok(t) => coerce_rendered(t.to_string(), SqlValue::Timestamp(t), s, inclusive, side),
            Err(_) => CoercedBound::Decline,
        },
        TemporalKeyType::Time => match vibesql_types::Time::from_str(s) {
            Ok(t) => coerce_rendered(t.to_string(), SqlValue::Time(t), s, inclusive, side),
            Err(_) => CoercedBound::Decline,
        },
    }
}

/// Shared TEXT-rendering coercion for `Timestamp` / `Time` keys.
///
/// * exact round-trip (`render(parse(s)) == s`): the coerced bound is
///   equivalent under all operators — keep the original inclusivity.
/// * strict rendering prefix (`render(parse(s))` starts with `s`, e.g. a
///   date-only string against `Timestamp` keys): every key `>= parse(s)`
///   renders strictly greater than `s` and every key `< parse(s)` renders
///   strictly less, with no key rendering equal — so lower bounds become
///   inclusive and upper bounds exclusive regardless of the original
///   operator's inclusivity.
/// * anything else: no faithful interval exists — decline.
fn coerce_rendered(
    rendered: String,
    value: SqlValue,
    s: &str,
    inclusive: bool,
    side: BoundSide,
) -> CoercedBound {
    if rendered == s {
        CoercedBound::Coerced { value, inclusive }
    } else if rendered.starts_with(s) {
        match side {
            BoundSide::Lower => CoercedBound::Coerced { value, inclusive: true },
            BoundSide::Upper => CoercedBound::Coerced { value, inclusive: false },
        }
    } else {
        CoercedBound::Decline
    }
}

/// Coerce string probe bounds in an [`IndexPredicate`] to the stored temporal
/// key type of the index's first column (issue #5333).
///
/// Returns the (possibly rewritten) predicate. Returns `None` when no
/// faithful coercion exists — the caller then executes a full index scan with
/// the WHERE clause applied as a filter, which evaluates with executor
/// semantics (correct, just slower; only hit for unusual string bounds).
///
/// Non-temporal indexes, non-string bounds, and indexes whose key type cannot
/// be sampled (empty/all-NULL in-memory, vector indexes) are passed through
/// unchanged. Disk-backed indexes report their key type from the persisted
/// `key_schema` (page-0 metadata, no I/O), so they are coerced like in-memory
/// ones — even when empty (issue #5337).
pub(crate) fn coerce_index_predicate_for_temporal_keys(
    predicate: Option<IndexPredicate>,
    index_data: &IndexData,
) -> Option<IndexPredicate> {
    let predicate = predicate?;

    // Fast exit: nothing to coerce unless some bound is a string.
    let has_string_bound = match &predicate {
        IndexPredicate::Range(range) => {
            range.start.as_ref().is_some_and(is_string) || range.end.as_ref().is_some_and(is_string)
        }
        IndexPredicate::In(values) => values.iter().any(is_string),
    };
    if !has_string_bound {
        return Some(predicate);
    }

    // Determine the stored key type from the first column's stored values.
    let Some(key_type) = index_data.first_key_value_sample(0).as_ref().and_then(temporal_key_type)
    else {
        return Some(predicate);
    };

    match predicate {
        IndexPredicate::Range(mut range) => {
            if let Some(start) = &range.start {
                match coerce_string_bound(key_type, start, range.inclusive_start, BoundSide::Lower)
                {
                    CoercedBound::Unchanged => {}
                    CoercedBound::Coerced { value, inclusive } => {
                        range.start = Some(value);
                        range.inclusive_start = inclusive;
                    }
                    CoercedBound::Decline => return None,
                }
            }
            if let Some(end) = &range.end {
                match coerce_string_bound(key_type, end, range.inclusive_end, BoundSide::Upper) {
                    CoercedBound::Unchanged => {}
                    CoercedBound::Coerced { value, inclusive } => {
                        range.end = Some(value);
                        range.inclusive_end = inclusive;
                    }
                    CoercedBound::Decline => return None,
                }
            }

            // Upper-bounded-only coerced ranges would otherwise sweep up NULL
            // keys (and any lower-sorting type tags) via type-tag ordering.
            // Pin the scan to the temporal key space with an inclusive
            // minimal lower bound. (`exclude_nulls` is set for all inequality
            // predicates; equality predicates carry both bounds already.)
            if range.start.is_none() && range.end.is_some() && range.exclude_nulls {
                range.start = Some(temporal_min_value(key_type));
                range.inclusive_start = true;
            }

            Some(IndexPredicate::Range(range))
        }
        IndexPredicate::In(values) => {
            let mut coerced = Vec::with_capacity(values.len());
            for value in values {
                if !is_string(&value) {
                    coerced.push(value);
                    continue;
                }
                match coerce_equality_key_for_sample_type(key_type, &value) {
                    EqualityKeyCoercion::Unchanged => coerced.push(value),
                    EqualityKeyCoercion::Coerced(v) => coerced.push(v),
                    EqualityKeyCoercion::Unusable => match key_type {
                        // Date vs unparseable string raises a type mismatch
                        // in the executor — decline so the fallback raises
                        // the identical error instead of silently dropping.
                        TemporalKeyType::Date => return None,
                        // Timestamp/Time TEXT-rendering equality with a
                        // non-round-tripping string is false for every key:
                        // drop the element from the lookup list.
                        TemporalKeyType::Timestamp | TemporalKeyType::Time => {}
                    },
                }
            }
            Some(IndexPredicate::In(coerced))
        }
    }
}

/// Coerce a single *equality* lookup key component against a stored key
/// sample. Used by fast-path point lookups that build composite keys
/// directly (`crate::select::executor::fast_path::index_lookup`).
///
/// [`EqualityKeyCoercion::Unusable`] means the index cannot be probed
/// faithfully for this key — callers should skip the index entirely and let
/// a slower path compute the result with executor semantics.
pub(crate) fn coerce_equality_key(sample: &SqlValue, value: &SqlValue) -> EqualityKeyCoercion {
    let Some(key_type) = temporal_key_type(sample) else {
        return EqualityKeyCoercion::Unchanged;
    };
    if !is_string(value) {
        return EqualityKeyCoercion::Unchanged;
    }
    coerce_equality_key_for_sample_type(key_type, value)
}

fn coerce_equality_key_for_sample_type(
    key_type: TemporalKeyType,
    value: &SqlValue,
) -> EqualityKeyCoercion {
    let Some(s) = as_str(value) else {
        return EqualityKeyCoercion::Unchanged;
    };
    match key_type {
        TemporalKeyType::Date => match vibesql_types::Date::from_str(s) {
            Ok(d) => EqualityKeyCoercion::Coerced(SqlValue::Date(d)),
            Err(_) => EqualityKeyCoercion::Unusable,
        },
        // Equality under TEXT-rendering semantics only ever matches strings
        // that round-trip exactly through parse → Display.
        TemporalKeyType::Timestamp => match vibesql_types::Timestamp::from_str(s) {
            Ok(t) if t.to_string() == s => EqualityKeyCoercion::Coerced(SqlValue::Timestamp(t)),
            _ => EqualityKeyCoercion::Unusable,
        },
        TemporalKeyType::Time => match vibesql_types::Time::from_str(s) {
            Ok(t) if t.to_string() == s => EqualityKeyCoercion::Coerced(SqlValue::Time(t)),
            _ => EqualityKeyCoercion::Unusable,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::super::RangePredicate;
    use super::*;

    fn varchar(s: &str) -> SqlValue {
        SqlValue::Varchar(arcstr::ArcStr::from(s))
    }

    fn ts(s: &str) -> SqlValue {
        SqlValue::Timestamp(vibesql_types::Timestamp::from_str(s).unwrap())
    }

    fn date(s: &str) -> SqlValue {
        SqlValue::Date(vibesql_types::Date::from_str(s).unwrap())
    }

    /// Build an in-memory IndexData with the given single-column keys.
    fn index_with_keys(keys: Vec<SqlValue>) -> IndexData {
        let mut data = std::collections::BTreeMap::new();
        for (i, key) in keys.into_iter().enumerate() {
            data.insert(vec![key], vec![i]);
        }
        IndexData::InMemory { data, pending_deletions: vec![] }
    }

    fn range(
        start: Option<SqlValue>,
        end: Option<SqlValue>,
        inclusive_start: bool,
        inclusive_end: bool,
    ) -> Option<IndexPredicate> {
        Some(IndexPredicate::Range(RangePredicate {
            start,
            end,
            inclusive_start,
            inclusive_end,
            exclude_nulls: true,
        }))
    }

    #[test]
    fn equality_roundtrip_string_coerces_to_timestamp() {
        let index = index_with_keys(vec![ts("2017-07-20 15:30:00")]);
        let pred = Some(IndexPredicate::Range(RangePredicate {
            start: Some(varchar("2017-07-20 15:30:00")),
            end: Some(varchar("2017-07-20 15:30:00")),
            inclusive_start: true,
            inclusive_end: true,
            exclude_nulls: false,
        }));
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => {
                assert_eq!(r.start, Some(ts("2017-07-20 15:30:00")));
                assert_eq!(r.end, Some(ts("2017-07-20 15:30:00")));
                assert!(r.inclusive_start && r.inclusive_end);
            }
            other => panic!("expected Range, got {:?}", other),
        }
    }

    #[test]
    fn equality_date_only_string_vs_timestamp_keys_becomes_empty_range() {
        // TEXT-rendering semantics: no timestamp renders equal to a
        // date-only string, so the equality probe must match nothing.
        let index = index_with_keys(vec![ts("2017-07-05 00:00:00")]);
        let pred = Some(IndexPredicate::Range(RangePredicate {
            start: Some(varchar("2017-07-05")),
            end: Some(varchar("2017-07-05")),
            inclusive_start: true,
            inclusive_end: true,
            exclude_nulls: false,
        }));
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => {
                // [T, T) — half-open empty range.
                assert_eq!(r.start, Some(ts("2017-07-05 00:00:00")));
                assert_eq!(r.end, Some(ts("2017-07-05 00:00:00")));
                assert!(r.inclusive_start);
                assert!(!r.inclusive_end);
            }
            other => panic!("expected Range, got {:?}", other),
        }
    }

    #[test]
    fn between_date_only_strings_vs_timestamp_keys() {
        // BETWEEN '2017-07-04' AND '2017-07-08' against Timestamp keys:
        // lower midnight included, upper midnight excluded (date2-331).
        let index = index_with_keys(vec![ts("2017-07-05 00:00:00")]);
        let pred = range(Some(varchar("2017-07-04")), Some(varchar("2017-07-08")), true, true);
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => {
                assert_eq!(r.start, Some(ts("2017-07-04 00:00:00")));
                assert!(r.inclusive_start);
                assert_eq!(r.end, Some(ts("2017-07-08 00:00:00")));
                assert!(!r.inclusive_end);
            }
            other => panic!("expected Range, got {:?}", other),
        }
    }

    #[test]
    fn upper_only_range_gains_minimal_lower_bound() {
        // `< '2017-07-08'` must not sweep NULL keys into the probe range.
        let index = index_with_keys(vec![ts("2017-07-05 00:00:00"), SqlValue::Null]);
        let pred = range(None, Some(varchar("2017-07-08")), false, false);
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => {
                assert!(r.start.is_some(), "expected pinned minimal lower bound");
                assert!(matches!(r.start, Some(SqlValue::Timestamp(_))));
                assert!(r.inclusive_start);
            }
            other => panic!("expected Range, got {:?}", other),
        }
    }

    #[test]
    fn date_keys_parse_first_any_parseable_format() {
        let index = index_with_keys(vec![date("2017-07-20")]);
        let pred = range(Some(varchar("2017-07-20")), None, false, false);
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => {
                assert_eq!(r.start, Some(date("2017-07-20")));
                // Date is parse-first: original (exclusive) inclusivity kept.
                assert!(!r.inclusive_start);
            }
            other => panic!("expected Range, got {:?}", other),
        }
    }

    #[test]
    fn unparseable_string_declines() {
        let index = index_with_keys(vec![ts("2017-07-05 00:00:00")]);
        let pred = range(Some(varchar("hello")), None, true, false);
        assert!(coerce_index_predicate_for_temporal_keys(pred, &index).is_none());

        let date_index = index_with_keys(vec![date("2017-07-05")]);
        let pred = range(None, Some(varchar("junk")), false, true);
        assert!(coerce_index_predicate_for_temporal_keys(pred, &date_index).is_none());
    }

    #[test]
    fn non_prefix_parseable_forms_decline_for_timestamp_keys() {
        let index = index_with_keys(vec![ts("2017-07-05 00:00:00")]);
        // ISO 'T' separator parses but renders differently — decline.
        let pred = range(Some(varchar("2017-07-05T00:00:00")), None, true, false);
        assert!(coerce_index_predicate_for_temporal_keys(pred, &index).is_none());
    }

    #[test]
    fn in_list_drops_non_roundtrip_strings_for_timestamp_keys() {
        let index = index_with_keys(vec![ts("2017-07-05 00:00:00")]);
        let pred = Some(IndexPredicate::In(vec![
            varchar("2017-07-05 00:00:00"), // round-trips → coerced
            varchar("2017-07-06"),          // date-only → matches nothing → dropped
            varchar("hello"),               // junk → matches nothing → dropped
        ]));
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::In(values) => {
                assert_eq!(values, vec![ts("2017-07-05 00:00:00")]);
            }
            other => panic!("expected In, got {:?}", other),
        }
    }

    #[test]
    fn in_list_with_unparseable_string_vs_date_keys_declines() {
        let index = index_with_keys(vec![date("2017-07-05")]);
        let pred = Some(IndexPredicate::In(vec![varchar("2017-07-05"), varchar("junk")]));
        assert!(coerce_index_predicate_for_temporal_keys(pred, &index).is_none());
    }

    #[test]
    fn non_temporal_keys_pass_through_unchanged() {
        let index = index_with_keys(vec![varchar("apple"), varchar("pear")]);
        let pred = range(Some(varchar("banana")), None, true, false);
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => assert_eq!(r.start, Some(varchar("banana"))),
            other => panic!("expected Range, got {:?}", other),
        }
    }

    #[test]
    fn numeric_bounds_pass_through_unchanged() {
        let index = index_with_keys(vec![ts("2017-07-05 00:00:00")]);
        let pred = range(Some(SqlValue::Double(5.0)), None, true, false);
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => assert_eq!(r.start, Some(SqlValue::Double(5.0))),
            other => panic!("expected Range, got {:?}", other),
        }
    }

    #[test]
    fn coerce_equality_key_semantics() {
        let ts_sample = ts("2017-07-05 00:00:00");
        // Round-trip string coerces.
        match coerce_equality_key(&ts_sample, &varchar("2017-07-20 15:30:00")) {
            EqualityKeyCoercion::Coerced(v) => assert_eq!(v, ts("2017-07-20 15:30:00")),
            _ => panic!("expected Coerced"),
        }
        // Date-only string never equals a timestamp rendering.
        assert!(matches!(
            coerce_equality_key(&ts_sample, &varchar("2017-07-20")),
            EqualityKeyCoercion::Unusable
        ));
        // Non-string keys pass through.
        assert!(matches!(
            coerce_equality_key(&ts_sample, &SqlValue::Integer(5)),
            EqualityKeyCoercion::Unchanged
        ));
        // Date keys are parse-first.
        match coerce_equality_key(&date("2017-07-05"), &varchar("2017-07-20")) {
            EqualityKeyCoercion::Coerced(v) => assert_eq!(v, date("2017-07-20")),
            _ => panic!("expected Coerced"),
        }
        assert!(matches!(
            coerce_equality_key(&date("2017-07-05"), &varchar("junk")),
            EqualityKeyCoercion::Unusable
        ));
    }

    #[test]
    fn trailing_zero_fraction_string_round_trips_after_5332() {
        // Since #5332 fractions render padded to >= 3 digits, so a
        // trailing-zero string like '.500' round-trips exactly and keeps its
        // original inclusivity instead of declining.
        let index = index_with_keys(vec![ts("2024-01-01 13:15:44.5")]);
        let pred = range(Some(varchar("2024-01-01 13:15:44.500")), None, false, false);
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => {
                assert_eq!(r.start, Some(ts("2024-01-01 13:15:44.500")));
                assert!(!r.inclusive_start, "exact round-trip keeps original inclusivity");
            }
            other => panic!("expected Range, got {:?}", other),
        }
    }

    #[test]
    fn short_fraction_string_takes_prefix_rule_after_5332() {
        // '...44.5' renders as '...44.500' — a strict rendering prefix, so
        // lower bounds become inclusive and upper bounds exclusive.
        let index = index_with_keys(vec![ts("2024-01-01 13:15:44.5")]);
        let pred = range(
            Some(varchar("2024-01-01 13:15:44.5")),
            Some(varchar("2024-01-01 13:15:44.5")),
            true,
            true,
        );
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => {
                assert_eq!(r.start, Some(ts("2024-01-01 13:15:44.5")));
                assert!(r.inclusive_start);
                assert_eq!(r.end, Some(ts("2024-01-01 13:15:44.5")));
                assert!(!r.inclusive_end);
            }
            other => panic!("expected Range, got {:?}", other),
        }
    }

    /// Build a disk-backed `IndexData` over the given single-column keys
    /// with the given key schema. Returns the TempDir so the backing file
    /// outlives the assertions.
    fn disk_backed_index_with_keys(
        key_schema: Vec<vibesql_types::DataType>,
        keys: Vec<SqlValue>,
    ) -> (IndexData, tempfile::TempDir) {
        use std::sync::Arc;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage = Arc::new(vibesql_storage::NativeStorage::new(temp_dir.path()).unwrap());
        let page_manager =
            Arc::new(vibesql_storage::page::PageManager::new("test_index.idx", storage).unwrap());

        let mut sorted_entries: Vec<(Vec<SqlValue>, usize)> =
            keys.into_iter().enumerate().map(|(i, key)| (vec![key], i)).collect();
        sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

        let btree = vibesql_storage::btree::BTreeIndex::bulk_load(
            sorted_entries,
            key_schema,
            page_manager.clone(),
        )
        .unwrap();
        let index =
            IndexData::DiskBacked { btree: Arc::new(parking_lot::Mutex::new(btree)), page_manager };
        (index, temp_dir)
    }

    #[test]
    fn disk_backed_timestamp_index_coerces_string_bounds() {
        // Issue #5337: disk-backed indexes report their key type from the
        // persisted key_schema, so string probe bounds are coerced exactly
        // like for in-memory indexes.
        let (index, _dir) = disk_backed_index_with_keys(
            vec![vibesql_types::DataType::Timestamp { with_timezone: false }],
            vec![ts("2017-07-05 00:00:00"), ts("2017-07-20 15:30:00")],
        );
        let pred = range(Some(varchar("2017-07-04")), Some(varchar("2017-07-08")), true, true);
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => {
                assert_eq!(r.start, Some(ts("2017-07-04 00:00:00")));
                assert!(r.inclusive_start);
                assert_eq!(r.end, Some(ts("2017-07-08 00:00:00")));
                assert!(!r.inclusive_end);
            }
            other => panic!("expected Range, got {:?}", other),
        }
    }

    #[test]
    fn empty_disk_backed_temporal_index_still_coerces() {
        // The schema-based key type works even for empty indexes, which the
        // in-memory sampling approach cannot handle.
        let (index, _dir) = disk_backed_index_with_keys(
            vec![vibesql_types::DataType::Timestamp { with_timezone: false }],
            vec![],
        );
        let pred = range(Some(varchar("2017-07-20 15:30:00")), None, true, false);
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => {
                assert_eq!(r.start, Some(ts("2017-07-20 15:30:00")));
            }
            other => panic!("expected Range, got {:?}", other),
        }
    }

    #[test]
    fn disk_backed_non_temporal_index_passes_through() {
        let (index, _dir) = disk_backed_index_with_keys(
            vec![vibesql_types::DataType::Integer],
            vec![SqlValue::Integer(1), SqlValue::Integer(2)],
        );
        let pred = range(Some(varchar("2017-07-20")), None, true, false);
        let coerced = coerce_index_predicate_for_temporal_keys(pred, &index).unwrap();
        match coerced {
            IndexPredicate::Range(r) => {
                assert_eq!(r.start, Some(varchar("2017-07-20")), "no coercion expected");
            }
            other => panic!("expected Range, got {:?}", other),
        }
    }

    #[test]
    fn disk_backed_equality_lookup_key_coerces_via_sample() {
        // Fast-path point lookups sample per column; the disk-backed sample
        // must drive the same equality coercion as in-memory samples.
        let (index, _dir) = disk_backed_index_with_keys(
            vec![vibesql_types::DataType::Timestamp { with_timezone: false }],
            vec![ts("2017-07-20 15:30:00")],
        );
        let sample = index.first_key_value_sample(0).expect("schema-based sample");
        match coerce_equality_key(&sample, &varchar("2017-07-20 15:30:00")) {
            EqualityKeyCoercion::Coerced(v) => assert_eq!(v, ts("2017-07-20 15:30:00")),
            _ => panic!("expected Coerced"),
        }
    }
}
