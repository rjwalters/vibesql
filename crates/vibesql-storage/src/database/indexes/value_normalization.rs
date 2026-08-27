// ============================================================================
// Value Normalization - Canonical forms for index operations
// ============================================================================

use std::{borrow::Cow, cmp::Ordering};

use vibesql_types::{total_order_cmp, SqlValue};

/// Normalize a SqlValue to a consistent numeric type for comparison in range scans.
/// This ensures that Real, Numeric, Float, Double, Integer, Smallint, Bigint, and Unsigned
/// values can be compared correctly regardless of their underlying type.
///
/// IMPORTANT: This function is also used at index insertion time to normalize all numeric values
/// to a canonical form (Double) before storing in the BTreeMap. This ensures that queries
/// comparing different numeric types (e.g., Real > Numeric) work correctly.
///
/// # Storage classes are NOT collapsed (issue #6555)
///
/// This canonicalization deliberately spans only the *numeric* storage class.
/// A `Varchar`/`Character` value whose text happens to parse as a number
/// (`'1'`) is **not** turned into `Double(1.0)`, because that would make the
/// index unable to tell SQLite's INTEGER `1` apart from TEXT `'1'` — producing
/// a false `UNIQUE constraint failed` on any column that does not have
/// INTEGER/REAL/NUMERIC affinity (a `TEXT` column, or an untyped
/// `PRIMARY KEY` column, which has BLOB affinity):
///
/// ```sql
/// CREATE TABLE par(p PRIMARY KEY);   -- no declared type => BLOB affinity
/// INSERT INTO par VALUES(1);
/// INSERT INTO par VALUES('1');       -- SQLite: OK, 1 and '1' are distinct
/// ```
///
/// Column *affinity* is applied once, earlier, at INSERT/UPDATE time
/// (`vibesql_executor::insert::validation::coerce_value`), so a string that
/// reaches this function for an INTEGER/REAL/NUMERIC-affinity column has
/// already been converted to a numeric `SqlValue`; a string that is still a
/// string here belongs to a TEXT/BLOB-affinity column and must stay TEXT.
/// Coercing a *query literal* to the indexed column's declared affinity (so
/// `WHERE int_col = '123'` still probes `Double(123.0)`) is the job of the
/// affinity-aware probe coercion in the executor
/// (`vibesql_executor::select::scan::index_scan::predicate::affinity_coercion`),
/// which — unlike this type-agnostic index utility — knows the column's
/// declared type.
///
/// Uses f64 (Double) instead of f32 (Real) to preserve precision for:
/// - Large integers (Bigint, Unsigned) beyond f32 precision range (> 2^24 ≈ 16 million)
/// - High-precision floating point values (Double, Numeric)
pub fn normalize_for_comparison(value: &SqlValue) -> SqlValue {
    match value {
        SqlValue::Integer(i) => SqlValue::Double(*i as f64),
        SqlValue::Smallint(i) => SqlValue::Double(*i as f64),
        SqlValue::Bigint(i) => SqlValue::Double(*i as f64),
        SqlValue::Unsigned(u) => SqlValue::Double(*u as f64),
        SqlValue::Float(f) => SqlValue::Double(*f as f64),
        SqlValue::Real(r) => SqlValue::Double(*r as f64),
        SqlValue::Double(d) => SqlValue::Double(*d),
        SqlValue::Numeric(n) => SqlValue::Double(*n),
        // For every other storage class (TEXT, BLOB, temporal, boolean, NULL),
        // return as-is: cross-storage-class coercion is affinity-dependent and
        // is therefore not this function's decision to make (see above).
        other => other.clone(),
    }
}

/// Zero-copy normalization using Cow - avoids allocation for non-numeric values.
/// Returns Borrowed for non-numeric types (no clone), Owned for normalized numerics.
///
/// Same semantics as [`normalize_for_comparison`]: numeric storage classes are
/// canonicalized to `Double`, and TEXT/BLOB values are left untouched
/// (issue #6555).
#[inline]
pub fn normalize_cow(value: &SqlValue) -> Cow<'_, SqlValue> {
    match value {
        SqlValue::Integer(i) => Cow::Owned(SqlValue::Double(*i as f64)),
        SqlValue::Smallint(i) => Cow::Owned(SqlValue::Double(*i as f64)),
        SqlValue::Bigint(i) => Cow::Owned(SqlValue::Double(*i as f64)),
        SqlValue::Unsigned(u) => Cow::Owned(SqlValue::Double(*u as f64)),
        SqlValue::Float(f) => Cow::Owned(SqlValue::Double(*f as f64)),
        SqlValue::Real(r) => Cow::Owned(SqlValue::Double(*r as f64)),
        SqlValue::Double(d) => Cow::Owned(SqlValue::Double(*d)),
        SqlValue::Numeric(n) => Cow::Owned(SqlValue::Double(*n)),
        // For other storage classes, borrow without clone
        other => Cow::Borrowed(other),
    }
}

/// Returns true when `value`, used as an **equality / IN-list point-lookup
/// probe key**, can produce a *false positive* match against a
/// differently-valued stored key because of [`normalize_for_comparison`]'s
/// lossy `as f64` cast (issue #6586).
///
/// # Why a point lookup needs this and a range scan does not
///
/// [`normalize_bound_for_range_scan`] (issue #6575) repairs a range bound by
/// flipping its inclusive/exclusive flag, because a bound has a *direction*
/// and the exact literal-vs-rounded relationship tells you which side of the
/// rounded key the true bound falls on. An equality probe has no such flag:
/// once the rounded literal collides with a rounded stored key, the BTreeMap
/// reports a hit and there is nothing left at the index layer to correct —
/// the map only retains the *normalized* key, so the original stored value is
/// no longer available to compare exactly against.
///
/// The index therefore cannot decide equality on its own for such a probe.
/// It can, however, tell callers that its answer is only a **candidate set**:
/// the normalized probe is a superset filter (the same lossy cast is applied
/// to both sides at insert and probe time, so a genuinely equal value always
/// normalizes to the same key — the probe can over-return, never
/// under-return). When this function returns true, the caller must re-verify
/// each candidate row against the *original* stored column value using exact
/// comparison semantics — which is what the executor's general WHERE
/// evaluator already does.
///
/// # Threshold
///
/// Both directions of the collision need covering, so the test is on
/// magnitude rather than on the storage class alone:
/// - an integer literal above 2^53 (`3175546974276630385`) rounds onto a REAL column's stored
///   value;
/// - a float literal above 2^53 is stored exactly, but a *stored* integer above 2^53 rounds onto
///   it.
///
/// Values at or below 2^53 in magnitude (and every non-numeric storage class,
/// which [`normalize_for_comparison`] leaves untouched — issue #6555) are
/// exact under normalization, so this returns false and the hot path pays
/// nothing but one comparison.
#[inline]
pub fn point_probe_needs_exact_reverification(value: &SqlValue) -> bool {
    // Non-numeric storage classes are never normalized (issue #6555), so
    // their index keys are exact and a key match is a genuine equality —
    // `exceeds_f64_exact_integer_range` returns false for all of them.
    vibesql_types::exceeds_f64_exact_integer_range(value)
}

/// Normalize a single range-scan bound value together with its
/// inclusive/exclusive flag, correcting for precision loss when an
/// out-of-f64-safe-integer-precision literal (`|i| > 2^53`) is lossily cast
/// to `Double` for comparison against a REAL-affinity indexed column
/// (issue #6575).
///
/// # Background
///
/// [`normalize_for_comparison`] casts every integer-class `SqlValue` to
/// `Double` via `as f64`, which is lossy above 2^53. When the *same* lossy
/// cast is applied both to a WHERE-clause literal and (independently, at
/// INSERT time) to a REAL-affinity column's stored value, an integer literal
/// that doesn't round-trip exactly can land on the *same* `Double` as a
/// stored row whose true value is actually strictly greater than (or less
/// than) the literal — turning a strict `>`/`<` bound into a false
/// equality and silently dropping (or admitting) that boundary row.
///
/// This function keeps using the lossy cast for the *value* used as the
/// BTreeMap bound (changing that would require a wider `Ord`/`PartialOrd`
/// overhaul — see issue #6575's "fix direction 1"), but corrects the
/// *inclusive/exclusive* flag by comparing the original (exact) literal
/// against the rounded `Double` using [`total_order_cmp`], which performs
/// exact integer-vs-float comparison (no precision loss).
///
/// `is_lower_bound` selects which side of a range this value bounds:
/// - lower bound (`col > v` / `col >= v`): the rounded `Double` should be treated as satisfying the
///   predicate whenever it is strictly greater than the exact literal (even if the original
///   comparison was exclusive), and as *not* satisfying it whenever it is strictly less (even if
///   the original comparison was inclusive).
/// - upper bound (`col < v` / `col <= v`): symmetric, with "greater"/"less" swapped.
///
/// Returns `(normalized_value, corrected_inclusive)`. For every value that
/// isn't an integer-class `SqlValue`, or that round-trips through `Double`
/// exactly, this is equivalent to `(normalize_for_comparison(value), inclusive)`.
pub fn normalize_bound_for_range_scan(
    value: &SqlValue,
    inclusive: bool,
    is_lower_bound: bool,
) -> (SqlValue, bool) {
    let normalized = normalize_for_comparison(value);

    // Only integer-class SqlValues can lose precision via the `as f64` cast;
    // Float/Real/Double/Numeric are already floating-point and this
    // particular cast is either a no-op or a lossless upcast for them.
    let is_integer_class = matches!(
        value,
        SqlValue::Integer(_) | SqlValue::Smallint(_) | SqlValue::Bigint(_) | SqlValue::Unsigned(_)
    );
    if !is_integer_class {
        return (normalized, inclusive);
    }

    // Exact comparison of the original literal against the rounded Double
    // (no further precision loss: total_order_cmp handles Integer-vs-Double
    // via exact-integer-part comparison, unlike SqlValue's Ord/PartialOrd).
    let ord = total_order_cmp(value, &normalized);

    let corrected_inclusive = if is_lower_bound {
        match ord {
            Ordering::Less => true, // literal < rounded: rounded value qualifies (> literal)
            Ordering::Equal => inclusive,
            Ordering::Greater => false, // literal > rounded: rounded value does not qualify
        }
    } else {
        match ord {
            Ordering::Greater => true, // literal > rounded: rounded value qualifies (< literal)
            Ordering::Equal => inclusive,
            Ordering::Less => false, // literal < rounded: rounded value does not qualify
        }
    };

    (normalized, corrected_inclusive)
}

/// Normalize a pair of range-scan bounds together, correcting both
/// inclusive/exclusive flags for precision loss (see
/// [`normalize_bound_for_range_scan`]).
///
/// This is the entry point [`super::range_scan`] uses in place of calling
/// [`normalize_for_comparison`] directly on `start`/`end`.
pub fn normalize_range_bounds(
    start: Option<&SqlValue>,
    end: Option<&SqlValue>,
    inclusive_start: bool,
    inclusive_end: bool,
) -> (Option<SqlValue>, Option<SqlValue>, bool, bool) {
    let (normalized_start, inclusive_start) = match start {
        Some(v) => {
            let (nv, inc) = normalize_bound_for_range_scan(v, inclusive_start, true);
            (Some(nv), inc)
        }
        None => (None, inclusive_start),
    };
    let (normalized_end, inclusive_end) = match end {
        Some(v) => {
            let (nv, inc) = normalize_bound_for_range_scan(v, inclusive_end, false);
            (Some(nv), inc)
        }
        None => (None, inclusive_end),
    };
    (normalized_start, normalized_end, inclusive_start, inclusive_end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_for_comparison_numeric_types() {
        // All numeric types should normalize to Double
        assert_eq!(normalize_for_comparison(&SqlValue::Integer(42)), SqlValue::Double(42.0));
        assert_eq!(normalize_for_comparison(&SqlValue::Smallint(10)), SqlValue::Double(10.0));
        assert_eq!(normalize_for_comparison(&SqlValue::Bigint(1000)), SqlValue::Double(1000.0));
        assert_eq!(normalize_for_comparison(&SqlValue::Unsigned(99)), SqlValue::Double(99.0));
        assert_eq!(
            normalize_for_comparison(&SqlValue::Float(3.14)),
            SqlValue::Double(3.14f32 as f64)
        );
        assert_eq!(normalize_for_comparison(&SqlValue::Real(2.5)), SqlValue::Double(2.5));
        assert_eq!(normalize_for_comparison(&SqlValue::Numeric(123.45)), SqlValue::Double(123.45));
        assert_eq!(normalize_for_comparison(&SqlValue::Double(7.89)), SqlValue::Double(7.89));
    }

    #[test]
    fn test_normalize_for_comparison_non_numeric() {
        // Non-numeric types should be returned as-is
        let text_val = SqlValue::Varchar(arcstr::ArcStr::from("test"));
        assert_eq!(normalize_for_comparison(&text_val), text_val);

        let null_val = SqlValue::Null;
        assert_eq!(normalize_for_comparison(&null_val), null_val);

        let bool_val = SqlValue::Boolean(true);
        assert_eq!(normalize_for_comparison(&bool_val), bool_val);
    }

    /// Issue #6555: a numeric-looking TEXT value must keep its storage class
    /// so a UNIQUE/PRIMARY KEY index on a TEXT/BLOB-affinity column can tell
    /// `1` (INTEGER) apart from `'1'` (TEXT).
    #[test]
    fn numeric_looking_text_keeps_text_storage_class() {
        let text_one = SqlValue::Varchar(arcstr::ArcStr::from("1"));
        assert_eq!(normalize_for_comparison(&text_one), text_one);
        assert_ne!(
            normalize_for_comparison(&text_one),
            normalize_for_comparison(&SqlValue::Integer(1))
        );

        let char_one = SqlValue::Character(arcstr::ArcStr::from("1"));
        assert_eq!(normalize_for_comparison(&char_one), char_one);

        // Whitespace-padded and float-formatted strings likewise stay TEXT.
        let padded = SqlValue::Varchar(arcstr::ArcStr::from(" 1 "));
        assert_eq!(normalize_for_comparison(&padded), padded);
        let float_text = SqlValue::Varchar(arcstr::ArcStr::from("1.0"));
        assert_eq!(normalize_for_comparison(&float_text), float_text);
    }

    /// The `Cow` variant must agree with [`normalize_for_comparison`] exactly,
    /// including on the numeric-looking-TEXT case (issue #6555).
    #[test]
    fn normalize_cow_agrees_with_owned_variant() {
        let cases = [
            SqlValue::Integer(1),
            SqlValue::Bigint(-7),
            SqlValue::Double(2.5),
            SqlValue::Varchar(arcstr::ArcStr::from("1")),
            SqlValue::Character(arcstr::ArcStr::from("42")),
            SqlValue::Varchar(arcstr::ArcStr::from("abc")),
            SqlValue::Blob(vec![0x31]),
            SqlValue::Null,
        ];
        for case in cases {
            assert_eq!(
                normalize_cow(&case).into_owned(),
                normalize_for_comparison(&case),
                "mismatch for {:?}",
                case
            );
        }
    }

    /// The three SQLite storage classes that all render as `1` must stay
    /// mutually distinct index keys (issue #6555).
    #[test]
    fn integer_text_and_blob_one_are_distinct_keys() {
        let int_one = normalize_for_comparison(&SqlValue::Integer(1));
        let text_one = normalize_for_comparison(&SqlValue::Varchar(arcstr::ArcStr::from("1")));
        let blob_one = normalize_for_comparison(&SqlValue::Blob(vec![0x31]));
        assert_ne!(int_one, text_one);
        assert_ne!(int_one, blob_one);
        assert_ne!(text_one, blob_one);
    }

    /// Issue #6575: an out-of-f64-safe-integer-precision literal
    /// (2^53 < |i|) that rounds *up* when cast to `Double` must still be
    /// treated as satisfying an exclusive lower bound (`col > literal`),
    /// since the rounded `Double` is genuinely greater than the literal.
    #[test]
    fn lower_bound_exclusive_rounds_up_becomes_inclusive() {
        // 3175546974276630385 as f64 rounds UP to 3175546974276630528.0.
        let literal = SqlValue::Bigint(3175546974276630385);
        let (normalized, inclusive) = normalize_bound_for_range_scan(&literal, false, true);
        assert_eq!(normalized, SqlValue::Double(3175546974276630385_i64 as f64));
        assert!(inclusive, "rounded-up Double is > the exact literal, so it must be included");
    }

    /// Symmetric case: an inclusive lower bound (`col >= literal`) whose
    /// literal rounds *down* must become exclusive, since the rounded
    /// `Double` is strictly less than the literal and must NOT satisfy `>=`.
    #[test]
    fn lower_bound_inclusive_rounds_down_becomes_exclusive() {
        // 9223372036854775807 (i64::MAX) as f64 rounds DOWN to 9223372036854775808.0?
        // Use a literal known to round down: 2^53 + 1 rounds down to 2^53.
        let literal = SqlValue::Integer(9_007_199_254_740_993); // 2^53 + 1
        let (normalized, inclusive) = normalize_bound_for_range_scan(&literal, true, true);
        assert_eq!(normalized, SqlValue::Double(9_007_199_254_740_992.0)); // 2^53 (rounds down)
        assert!(
            !inclusive,
            "rounded-down Double is < the exact literal, so it must NOT satisfy >="
        );
    }

    /// Upper-bound mirror of the primary repro: an exclusive upper bound
    /// (`col < literal`) whose literal rounds *down* must become inclusive,
    /// since the rounded `Double` is genuinely less than the exact literal
    /// (so it satisfies `< literal` regardless of the original exclusivity).
    #[test]
    fn upper_bound_exclusive_rounds_down_becomes_inclusive() {
        let literal = SqlValue::Integer(9_007_199_254_740_993); // rounds down to 2^53
        let (_normalized, inclusive) = normalize_bound_for_range_scan(&literal, false, false);
        assert!(
            inclusive,
            "rounded-down Double is < the exact literal, so it must satisfy strict <"
        );
    }

    /// Upper-bound: an inclusive upper bound (`col <= literal`) whose
    /// literal rounds *up* must become exclusive, since the rounded
    /// `Double` is strictly greater than the literal and must NOT satisfy `<=`.
    #[test]
    fn upper_bound_inclusive_rounds_up_becomes_exclusive() {
        let literal = SqlValue::Bigint(3175546974276630385);
        let (_normalized, inclusive) = normalize_bound_for_range_scan(&literal, true, false);
        assert!(!inclusive, "rounded-up Double is > the exact literal, so it must NOT satisfy <=");
    }

    /// Exact round-trip (small magnitude) leaves the inclusive flag untouched
    /// in every direction.
    #[test]
    fn exact_roundtrip_leaves_inclusive_unchanged() {
        let literal = SqlValue::Integer(42);
        for is_lower in [true, false] {
            for inclusive in [true, false] {
                let (_normalized, corrected) =
                    normalize_bound_for_range_scan(&literal, inclusive, is_lower);
                assert_eq!(corrected, inclusive);
            }
        }
    }

    /// Non-integer-class values (already-float types, TEXT, etc.) are passed
    /// through unchanged, matching [`normalize_for_comparison`] exactly.
    #[test]
    fn non_integer_class_values_pass_through_unchanged() {
        for value in [
            SqlValue::Double(1.5),
            SqlValue::Real(2.5),
            SqlValue::Float(3.5),
            SqlValue::Numeric(4.5),
            SqlValue::Varchar(arcstr::ArcStr::from("abc")),
            SqlValue::Null,
        ] {
            for is_lower in [true, false] {
                for inclusive in [true, false] {
                    let (normalized, corrected) =
                        normalize_bound_for_range_scan(&value, inclusive, is_lower);
                    assert_eq!(normalized, normalize_for_comparison(&value));
                    assert_eq!(corrected, inclusive);
                }
            }
        }
    }

    /// Issue #6586: an integer literal beyond f64's exact-integer range is a
    /// lossy equality-probe key and must be flagged for exact
    /// re-verification.
    #[test]
    fn out_of_precision_integer_probe_needs_reverification() {
        for value in [
            SqlValue::Bigint(3175546974276630385),
            SqlValue::Integer(3175546974276630385),
            SqlValue::Integer(-3175546974276630385),
            SqlValue::Integer(9_007_199_254_740_993), // 2^53 + 1
            SqlValue::Integer(i64::MAX),
            SqlValue::Unsigned(u64::MAX),
        ] {
            assert!(
                point_probe_needs_exact_reverification(&value),
                "{value:?} is out of f64 exact-integer range"
            );
        }
    }

    /// A float literal beyond 2^53 is stored exactly but can still collide
    /// with a *stored* integer that rounds onto it, so it is flagged too.
    #[test]
    fn out_of_precision_float_probe_needs_reverification() {
        assert!(point_probe_needs_exact_reverification(&SqlValue::Double(
            3175546974276630385_i64 as f64
        )));
        assert!(point_probe_needs_exact_reverification(&SqlValue::Real(1e30)));
        assert!(point_probe_needs_exact_reverification(&SqlValue::Numeric(-1e30)));
        assert!(point_probe_needs_exact_reverification(&SqlValue::Float(1e30f32)));
    }

    /// Values inside f64's exact-integer range (and every non-numeric storage
    /// class) round-trip exactly, so they keep the zero-overhead fast path.
    #[test]
    fn in_precision_and_non_numeric_probes_need_no_reverification() {
        for value in [
            SqlValue::Integer(0),
            SqlValue::Integer(42),
            SqlValue::Integer(-42),
            SqlValue::Integer(9_007_199_254_740_992), // 2^53 exactly
            SqlValue::Integer(-9_007_199_254_740_992),
            SqlValue::Smallint(i16::MIN),
            SqlValue::Unsigned(9_007_199_254_740_992),
            SqlValue::Real(2.5),
            SqlValue::Double(f64::NAN),
            SqlValue::Float(3.14),
            SqlValue::Varchar(arcstr::ArcStr::from("3175546974276630385")),
            SqlValue::Blob(vec![0x31]),
            SqlValue::Boolean(true),
            SqlValue::Null,
        ] {
            assert!(
                !point_probe_needs_exact_reverification(&value),
                "{value:?} is exact under index normalization"
            );
        }
    }

    /// [`normalize_range_bounds`] applies the per-side correction to both
    /// bounds independently, reproducing the exact issue #6575 scenario:
    /// `col > 3175546974276630385` must become an inclusive lower bound at
    /// the rounded Double.
    #[test]
    fn normalize_range_bounds_applies_both_sides() {
        let start = SqlValue::Bigint(3175546974276630385);
        let (norm_start, norm_end, inclusive_start, inclusive_end) =
            normalize_range_bounds(Some(&start), None, false, false);
        assert_eq!(norm_start, Some(SqlValue::Double(3175546974276630385_i64 as f64)));
        assert_eq!(norm_end, None);
        assert!(inclusive_start);
        // inclusive_end is irrelevant with no end bound, but must still be
        // returned as passed in (false) since there's nothing to correct.
        assert!(!inclusive_end);
    }
}
