//! A genuine total order over [`SqlValue`] for sorting (issue #5802).
//!
//! SQLite's `ORDER BY` (memcompare) semantics define the cross-type ordering
//!
//! ```text
//! NULL < numeric (INTEGER and REAL, inter-comparable, compared exactly) < TEXT < BLOB
//! ```
//!
//! VibeSQL has additional value kinds SQLite does not (BOOLEAN, DATE, TIME,
//! TIMESTAMP, INTERVAL, VECTOR). To keep the order *total* for arbitrary
//! mixed-type columns, those are placed in deterministic slots after BLOB,
//! ordered by a fixed type tag, with exact within-type comparison:
//!
//! ```text
//! NULL < numeric < TEXT < BLOB < BOOLEAN < DATE < TIME < TIMESTAMP < INTERVAL < VECTOR
//! ```
//!
//! Design notes:
//!
//! * **Numerics compare exactly, never through lossy `as f64` casts.** All
//!   integer variants (`Integer`, `Smallint`, `Bigint`, `Unsigned`) are
//!   widened to `i128` (which holds both `i64::MIN` and `u64::MAX`) and
//!   compared exactly. Integer-vs-float pairs use the same approach as
//!   SQLite's `sqlite3IntFloatCompare`: handle NaN/infinities first, then
//!   compare the integer against the float's exact integer part with a
//!   fractional tiebreak. This is what makes the order transitive at the f64
//!   precision boundary (2^53), where `x as f64` rounding previously produced
//!   non-transitive triples that made `sort_by` panic.
//! * **NaN has a fixed total position: less than every other numeric, and
//!   NaN == NaN.** SQLite cannot store NaN (it becomes NULL), but VibeSQL
//!   values can carry one, and the comparator must remain total. "Less than
//!   all reals" mirrors NaN being the value closest to NULL.
//! * `-0.0 == 0.0` (IEEE equality), which is a consistent equivalence class
//!   and matches SQLite.
//! * Vectors compare elementwise via `f32::total_cmp`, then by length.
//!
//! NULL placement: this function orders NULL *first* (SQLite's native order).
//! Callers that need NULLS LAST (e.g. MIN/MAX aggregate comparators, ORDER BY
//! ... DESC defaults) handle NULL themselves before delegating, which all
//! current executor call sites already do.

use std::cmp::Ordering;

use crate::sql_value::SqlValue;

/// Rank of each type class in the total order. See module docs.
fn class_rank(v: &SqlValue) -> u8 {
    match v {
        SqlValue::Null => 0,
        SqlValue::Integer(_)
        | SqlValue::Smallint(_)
        | SqlValue::Bigint(_)
        | SqlValue::Unsigned(_)
        | SqlValue::Float(_)
        | SqlValue::Real(_)
        | SqlValue::Double(_)
        | SqlValue::Numeric(_) => 1,
        SqlValue::Character(_) | SqlValue::Varchar(_) => 2,
        SqlValue::Blob(_) => 3,
        SqlValue::Boolean(_) => 4,
        SqlValue::Date(_) => 5,
        SqlValue::Time(_) => 6,
        SqlValue::Timestamp(_) => 7,
        SqlValue::Interval(_) => 8,
        SqlValue::Vector(_) => 9,
    }
}

/// Exact numeric representation of a numeric-class `SqlValue`.
enum Num {
    Int(i128),
    Float(f64),
}

fn numeric_repr(v: &SqlValue) -> Option<Num> {
    match v {
        SqlValue::Integer(i) | SqlValue::Bigint(i) => Some(Num::Int(*i as i128)),
        SqlValue::Smallint(i) => Some(Num::Int(*i as i128)),
        SqlValue::Unsigned(u) => Some(Num::Int(*u as i128)),
        SqlValue::Float(f) => Some(Num::Float(*f as f64)),
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => Some(Num::Float(*f)),
        _ => None,
    }
}

/// Total comparison of two floats: NaN is less than every other value and
/// NaN == NaN. (Unlike `f64::total_cmp`, -0.0 == 0.0 here, matching SQL.)
fn cmp_f64_total(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => a.partial_cmp(&b).expect("non-NaN floats are always comparable"),
    }
}

/// Exact comparison of an integer against a float (SQLite
/// `sqlite3IntFloatCompare` semantics, extended to `i128`).
///
/// Returns the ordering of `i` relative to `f`.
fn cmp_int_f64(i: i128, f: f64) -> Ordering {
    if f.is_nan() {
        // NaN is less than every numeric (see module docs), so any integer is greater.
        return Ordering::Greater;
    }
    if f == f64::INFINITY {
        return Ordering::Less;
    }
    if f == f64::NEG_INFINITY {
        return Ordering::Greater;
    }
    // `f` is finite. `i128::MAX as f64` rounds to exactly 2^127; any float at
    // or above it exceeds every i128 (and a fortiori every SqlValue integer).
    if f >= i128::MAX as f64 {
        return Ordering::Less;
    }
    // `i128::MIN as f64` is exactly -2^127; any float below it is smaller
    // than every i128.
    if f < i128::MIN as f64 {
        return Ordering::Greater;
    }
    // `f.floor()` is an integer-valued finite f64 with |floor| <= 2^127, so
    // the `as i128` conversion is exact (saturating only at i128::MIN, which
    // is itself exact).
    let floor = f.floor();
    let floor_int = floor as i128;
    match i.cmp(&floor_int) {
        Ordering::Equal => {
            if f > floor {
                // f has a fractional part: i == floor(f) < f
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }
        ord => ord,
    }
}

fn cmp_num(a: Num, b: Num) -> Ordering {
    match (a, b) {
        (Num::Int(x), Num::Int(y)) => x.cmp(&y),
        (Num::Float(x), Num::Float(y)) => cmp_f64_total(x, y),
        (Num::Int(x), Num::Float(y)) => cmp_int_f64(x, y),
        (Num::Float(x), Num::Int(y)) => cmp_int_f64(y, x).reverse(),
    }
}

/// Compare two [`SqlValue`]s under a genuine total order with SQLite
/// cross-type ordering semantics (see module docs).
///
/// Guarantees (for all values `a`, `b`, `c`):
/// * totality: always returns an `Ordering` (never panics),
/// * antisymmetry: `cmp(a, b) == cmp(b, a).reverse()`,
/// * transitivity: `a <= b && b <= c` implies `a <= c`.
///
/// Safe to use with `sort_by`, `sort_unstable_by`, `min_by`, `max_by`, and
/// `BinaryHeap`-style consumers that require `Ord`-like behavior.
pub fn total_order_cmp(a: &SqlValue, b: &SqlValue) -> Ordering {
    use SqlValue::*;

    let (rank_a, rank_b) = (class_rank(a), class_rank(b));
    if rank_a != rank_b {
        return rank_a.cmp(&rank_b);
    }

    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Character(x) | Varchar(x), Character(y) | Varchar(y)) => x.as_str().cmp(y.as_str()),
        (Blob(x), Blob(y)) => x.cmp(y),
        (Boolean(x), Boolean(y)) => x.cmp(y),
        (Date(x), Date(y)) => x.cmp(y),
        (Time(x), Time(y)) => x.cmp(y),
        (Timestamp(x), Timestamp(y)) => x.cmp(y),
        (Interval(x), Interval(y)) => x.cmp(y),
        (Vector(x), Vector(y)) => {
            for (xa, ya) in x.iter().zip(y.iter()) {
                let c = xa.total_cmp(ya);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        _ => {
            // Same class rank and not matched above: both are numeric.
            let (na, nb) = (
                numeric_repr(a).expect("class rank 1 implies numeric"),
                numeric_repr(b).expect("class rank 1 implies numeric"),
            );
            cmp_num(na, nb)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_value::StringValue;

    const P53: i64 = 9_007_199_254_740_992; // 2^53

    #[test]
    fn integer_float_boundary_cases() {
        use Ordering::*;
        use SqlValue::*;

        // Exact integer comparison across variants
        assert_eq!(total_order_cmp(&Bigint(P53), &Bigint(P53 + 1)), Less);
        assert_eq!(total_order_cmp(&Integer(P53), &Bigint(P53 + 1)), Less);
        assert_eq!(total_order_cmp(&Unsigned(P53 as u64 + 1), &Integer(P53)), Greater);

        // The non-transitive triple from issue #5802: 2^53 + 1 rounds to 2^53
        // under `as f64`, but must compare Greater exactly.
        assert_eq!(total_order_cmp(&Bigint(P53), &Double(P53 as f64)), Equal);
        assert_eq!(total_order_cmp(&Bigint(P53 + 1), &Double(P53 as f64)), Greater);
        assert_eq!(total_order_cmp(&Double(P53 as f64), &Bigint(P53 + 1)), Less);

        // i64::MAX vs the float it rounds to (2^63, which is > i64::MAX)
        let two_p63 = 9.223_372_036_854_776e18;
        assert_eq!(total_order_cmp(&Integer(i64::MAX), &Double(two_p63)), Less);
        assert_eq!(total_order_cmp(&Bigint(i64::MAX), &Double(two_p63 - 2048.0)), Greater);

        // i64::MIN is exactly representable as f64
        assert_eq!(total_order_cmp(&Integer(i64::MIN), &Double(-9.223_372_036_854_776e18)), Equal);

        // u64::MAX vs 2^64 (u64::MAX rounds up to 2^64 under `as f64`)
        let two_p64 = 1.844_674_407_370_955_2e19;
        assert_eq!(total_order_cmp(&Unsigned(u64::MAX), &Double(two_p64)), Less);

        // Fractional tiebreak
        assert_eq!(total_order_cmp(&Integer(3), &Double(3.5)), Less);
        assert_eq!(total_order_cmp(&Integer(4), &Double(3.5)), Greater);
        assert_eq!(total_order_cmp(&Integer(-4), &Double(-3.5)), Less);
        assert_eq!(total_order_cmp(&Integer(-3), &Double(-3.5)), Greater);
        assert_eq!(total_order_cmp(&Integer(3), &Double(3.0)), Equal);

        // Infinities
        assert_eq!(total_order_cmp(&Double(f64::INFINITY), &Unsigned(u64::MAX)), Greater);
        assert_eq!(total_order_cmp(&Double(f64::NEG_INFINITY), &Integer(i64::MIN)), Less);
        assert_eq!(total_order_cmp(&Double(f64::INFINITY), &Double(f64::INFINITY)), Equal);

        // NaN: fixed slot below every other numeric, NaN == NaN
        assert_eq!(total_order_cmp(&Double(f64::NAN), &Double(f64::NAN)), Equal);
        assert_eq!(total_order_cmp(&Double(f64::NAN), &Double(f64::NEG_INFINITY)), Less);
        assert_eq!(total_order_cmp(&Double(f64::NAN), &Integer(i64::MIN)), Less);
        assert_eq!(total_order_cmp(&Integer(0), &Double(f64::NAN)), Greater);
        assert_eq!(total_order_cmp(&Float(f32::NAN), &Double(f64::NAN)), Equal);

        // -0.0 == 0.0 == Integer(0)
        assert_eq!(total_order_cmp(&Double(-0.0), &Double(0.0)), Equal);
        assert_eq!(total_order_cmp(&Integer(0), &Double(-0.0)), Equal);
    }

    #[test]
    fn class_ordering_null_numeric_text_blob() {
        use Ordering::*;
        use SqlValue::*;

        let null = Null;
        let num = Integer(i64::MAX);
        let text = Varchar(StringValue::from(""));
        let blob = Blob(vec![]);
        let boolean = Boolean(false);

        assert_eq!(total_order_cmp(&null, &num), Less);
        assert_eq!(total_order_cmp(&num, &text), Less);
        assert_eq!(total_order_cmp(&text, &blob), Less);
        assert_eq!(total_order_cmp(&blob, &boolean), Less);

        // Character and Varchar are the same class and inter-comparable
        assert_eq!(
            total_order_cmp(
                &Character(StringValue::from("abc")),
                &Varchar(StringValue::from("abc"))
            ),
            Equal
        );
    }

    /// Build a set of adversarial values covering every class and the exact
    /// pathological numeric boundaries from the fuzz.test panic.
    fn adversarial_values() -> Vec<SqlValue> {
        use SqlValue::*;

        let mut vals = vec![Null];

        for i in
            [0i64, 1, -1, i64::MAX, i64::MIN, P53, P53 + 1, P53 - 1, -P53, -P53 - 1, -2_147_483_648]
        {
            vals.push(Integer(i));
            vals.push(Bigint(i));
        }
        for i in [0i16, 1, -1, i16::MAX, i16::MIN] {
            vals.push(Smallint(i));
        }
        for u in [0u64, 1, u64::MAX, 1 << 63, P53 as u64 + 1] {
            vals.push(Unsigned(u));
        }
        for f in [
            0.0f64,
            -0.0,
            1.5,
            -1.5,
            0.5,
            P53 as f64,
            9_007_199_254_740_994.0, // 2^53 + 2
            9.223_372_036_854_776e18,
            -9.223_372_036_854_776e18,
            1.844_674_407_370_955_2e19,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
            f64::MIN,
        ] {
            vals.push(Double(f));
            vals.push(Numeric(f));
            vals.push(Real(f));
        }
        for f in [0.0f32, 1.5, -1.5, f32::NAN, f32::INFINITY, 16_777_217.0] {
            vals.push(Float(f));
        }
        for s in ["", "a", "b", "0", "10"] {
            vals.push(Character(StringValue::from(s)));
            vals.push(Varchar(StringValue::from(s)));
        }
        for b in [vec![], vec![0u8], vec![0xffu8], b"abc".to_vec()] {
            vals.push(Blob(b));
        }
        vals.push(Boolean(false));
        vals.push(Boolean(true));
        vals.push(Date("2024-01-15".parse::<crate::temporal::Date>().unwrap()));
        vals.push(Date("1999-12-31".parse::<crate::temporal::Date>().unwrap()));
        vals.push(Time("12:34:56".parse::<crate::temporal::Time>().unwrap()));
        vals.push(Vector(vec![1.0, 2.0]));
        vals.push(Vector(vec![f32::NAN]));
        vals.push(Vector(vec![]));

        vals
    }

    /// Property test: totality, reflexivity, antisymmetry, and full O(n^3)
    /// transitivity over the adversarial value set (issue #5802 acceptance
    /// criterion: the comparator is a *genuine* total order).
    #[test]
    fn total_order_properties_exhaustive() {
        let vals = adversarial_values();
        let n = vals.len();
        assert!(n >= 60, "want a meaningful adversarial set, got {n}");

        // Reflexivity + antisymmetry over all pairs
        for a in &vals {
            assert_eq!(total_order_cmp(a, a), Ordering::Equal, "reflexivity failed for {a:?}");
        }
        for a in &vals {
            for b in &vals {
                assert_eq!(
                    total_order_cmp(a, b),
                    total_order_cmp(b, a).reverse(),
                    "antisymmetry failed for {a:?} vs {b:?}"
                );
            }
        }

        // Transitivity over all triples: a <= b && b <= c => a <= c,
        // and strictness: a < b && b <= c => a < c.
        for a in &vals {
            for b in &vals {
                let ab = total_order_cmp(a, b);
                if ab == Ordering::Greater {
                    continue;
                }
                for c in &vals {
                    let bc = total_order_cmp(b, c);
                    if bc == Ordering::Greater {
                        continue;
                    }
                    let ac = total_order_cmp(a, c);
                    assert_ne!(
                        ac,
                        Ordering::Greater,
                        "transitivity failed: {a:?} <= {b:?} <= {c:?} but {a:?} > {c:?}"
                    );
                    if ab == Ordering::Less || bc == Ordering::Less {
                        assert_eq!(
                            ac,
                            Ordering::Less,
                            "strict transitivity failed: {a:?} < {b:?} <= {c:?} (or <=/<) \
                             but {a:?} !< {c:?}"
                        );
                    }
                }
            }
        }

        // And the std sort must accept it without panicking.
        let mut sorted = vals.clone();
        sorted.sort_by(total_order_cmp);
        // Duplicate the set several times to exceed the small-sort threshold
        // that triggered the original panic (roughly 65 or more elements).
        let mut big: Vec<SqlValue> = Vec::new();
        for _ in 0..3 {
            big.extend(vals.iter().cloned());
        }
        big.sort_unstable_by(total_order_cmp);
    }
}
