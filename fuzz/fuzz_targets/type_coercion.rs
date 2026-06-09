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
use vibesql_types::{Date, Interval, SqlValue, Time, Timestamp};

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
    // Date/Time types with bounded/valid values
    Date {
        year: i32,
        month: u8,
        day: u8,
    },
    Time {
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
    },
    Timestamp {
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
    },
    // Interval as a simple duration in days
    IntervalDays(i32),
    // Vector with bounded dimensions
    Vector(Vec<f32>),
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
            FuzzValue::Real(v) => SqlValue::Real(f64::from(*v)),
            FuzzValue::Double(v) => SqlValue::Double(*v),
            FuzzValue::Character(v) => SqlValue::Character(v.as_str().into()),
            FuzzValue::Varchar(v) => SqlValue::Varchar(v.as_str().into()),
            FuzzValue::Boolean(v) => SqlValue::Boolean(*v),
            FuzzValue::Date { year, month, day } => {
                // Normalize month and day to valid ranges
                let norm_month = ((*month as u32 - 1) % 12) as u8 + 1;
                let norm_day = ((*day as u32 - 1) % 31) as u8 + 1;
                // Date::new validates ranges, but we've already normalized them
                match Date::new(*year, norm_month, norm_day) {
                    Ok(date) => SqlValue::Date(date),
                    Err(_) => SqlValue::Null, // Fallback to NULL on any error
                }
            }
            FuzzValue::Time { hour, minute, second, nanosecond } => {
                // Normalize time components to valid ranges
                let norm_hour = *hour % 24;
                let norm_minute = *minute % 60;
                let norm_second = *second % 60;
                let norm_nanosecond = *nanosecond % 1_000_000_000;
                match Time::new(norm_hour, norm_minute, norm_second, norm_nanosecond) {
                    Ok(time) => SqlValue::Time(time),
                    Err(_) => SqlValue::Null,
                }
            }
            FuzzValue::Timestamp { year, month, day, hour, minute, second, nanosecond } => {
                // Normalize all components
                let norm_month = ((*month as u32 - 1) % 12) as u8 + 1;
                let norm_day = ((*day as u32 - 1) % 31) as u8 + 1;
                let norm_hour = *hour % 24;
                let norm_minute = *minute % 60;
                let norm_second = *second % 60;
                let norm_nanosecond = *nanosecond % 1_000_000_000;

                match (
                    Date::new(*year, norm_month, norm_day),
                    Time::new(norm_hour, norm_minute, norm_second, norm_nanosecond),
                ) {
                    (Ok(date), Ok(time)) => SqlValue::Timestamp(Timestamp::new(date, time)),
                    _ => SqlValue::Null,
                }
            }
            FuzzValue::IntervalDays(days) => {
                // Create a simple interval from days
                let interval_str = format!("{} DAY", days);
                SqlValue::Interval(Interval::new(interval_str))
            }
            FuzzValue::Vector(v) => {
                // Limit vector dimensions to 1024 for fuzzing efficiency
                let bounded_vec: Vec<f32> = v.iter().take(1024).copied().collect();
                SqlValue::Vector(bounded_vec)
            }
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
    let is_null_fuzz = matches!(input.value1, FuzzValue::Null);
    assert_eq!(v1.is_null(), is_null_fuzz, "is_null inconsistent for {:?}", input.value1);

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
