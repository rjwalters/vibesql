// ============================================================================
// Value Normalization - Canonical forms for index operations
// ============================================================================

use std::borrow::Cow;

use vibesql_types::SqlValue;

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
}
