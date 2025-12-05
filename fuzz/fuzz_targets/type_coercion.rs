#![no_main]

//! Type coercion fuzz target
//!
//! This fuzzer tests SqlValue type coercion and comparison at runtime:
//! - Type comparisons (PartialEq, PartialOrd, Ord)
//! - Type tag ordering consistency
//! - NaN handling
//! - NULL handling
//!
//! Unlike the type_convert target (which fuzzes CAST parsing), this target
//! tests the actual runtime type system behavior with generated values.

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use vibesql_types::SqlValue;

/// A fuzzable representation of SqlValue for structured input
#[derive(Arbitrary, Debug)]
enum FuzzValue {
    Integer(i64),
    Smallint(i16),
    Bigint(i64),
    Unsigned(u64),
    Numeric(f64),
    Float(f32),
    Real(f32),
    Double(f64),
    Character(String),
    Varchar(String),
    Boolean(bool),
    Null,
}

impl From<&FuzzValue> for SqlValue {
    fn from(fv: &FuzzValue) -> Self {
        match fv {
            FuzzValue::Integer(v) => SqlValue::Integer(*v),
            FuzzValue::Smallint(v) => SqlValue::Smallint(*v),
            FuzzValue::Bigint(v) => SqlValue::Bigint(*v),
            FuzzValue::Unsigned(v) => SqlValue::Unsigned(*v),
            FuzzValue::Numeric(v) => SqlValue::Numeric(*v),
            FuzzValue::Float(v) => SqlValue::Float(*v),
            FuzzValue::Real(v) => SqlValue::Real(*v),
            FuzzValue::Double(v) => SqlValue::Double(*v),
            FuzzValue::Character(v) => SqlValue::Character(v.clone()),
            FuzzValue::Varchar(v) => SqlValue::Varchar(v.clone()),
            FuzzValue::Boolean(v) => SqlValue::Boolean(*v),
            FuzzValue::Null => SqlValue::Null,
        }
    }
}

#[derive(Arbitrary, Debug)]
struct TypeCoercionInput {
    value1: FuzzValue,
    value2: FuzzValue,
}

fuzz_target!(|input: TypeCoercionInput| {
    let v1: SqlValue = (&input.value1).into();
    let v2: SqlValue = (&input.value2).into();

    // Test PartialEq - should never panic
    let eq = v1 == v2;

    // Test PartialOrd - should never panic
    let partial_cmp = v1.partial_cmp(&v2);

    // Test Ord (total ordering) - should never panic
    let cmp = v1.cmp(&v2);

    // Verify consistency invariants:
    // 1. If values are equal, their Ord comparison should be Equal
    if eq {
        assert!(
            cmp == std::cmp::Ordering::Equal,
            "Inconsistency: eq=true but cmp={:?}",
            cmp
        );
    }

    // 2. If partial_cmp returns Some, it should match cmp
    if let Some(partial) = partial_cmp {
        assert!(
            partial == cmp,
            "Inconsistency: partial_cmp={:?} but cmp={:?}",
            partial,
            cmp
        );
    }

    // 3. Test reflexivity: a == a
    assert!(v1 == v1, "Reflexivity violated for {:?}", v1);
    assert!(v2 == v2, "Reflexivity violated for {:?}", v2);

    // 4. Test Ord reflexivity: a.cmp(&a) == Equal
    assert!(
        v1.cmp(&v1) == std::cmp::Ordering::Equal,
        "Ord reflexivity violated for {:?}",
        v1
    );

    // 5. Test symmetry of equality
    let eq_rev = v2 == v1;
    assert!(
        eq == eq_rev,
        "Symmetry violated: v1==v2 is {} but v2==v1 is {}",
        eq,
        eq_rev
    );

    // 6. Test antisymmetry of ordering
    let cmp_rev = v2.cmp(&v1);
    let expected_rev = match cmp {
        std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
        std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
        std::cmp::Ordering::Equal => std::cmp::Ordering::Equal,
    };
    assert!(
        cmp_rev == expected_rev,
        "Antisymmetry violated: v1.cmp(v2)={:?}, v2.cmp(v1)={:?}",
        cmp,
        cmp_rev
    );

    // 7. Test is_null
    if matches!(input.value1, FuzzValue::Null) {
        assert!(v1.is_null(), "is_null failed for Null variant");
    } else {
        assert!(!v1.is_null(), "is_null returned true for non-Null");
    }

    // 8. Test type_name and get_type - should never panic
    let _ = v1.type_name();
    let _ = v2.type_name();
    let _ = v1.get_type();
    let _ = v2.get_type();

    // 9. Test estimated_size_bytes - should never panic
    let size1 = v1.estimated_size_bytes();
    let size2 = v2.estimated_size_bytes();
    assert!(size1 > 0, "estimated_size_bytes returned 0");
    assert!(size2 > 0, "estimated_size_bytes returned 0");
});
