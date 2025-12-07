//! Tests for Eq/Ord consistency to ensure BTreeMap correctness
#![allow(clippy::approx_constant)]

use std::cmp::Ordering;

use vibesql_types::SqlValue;

// Helper to create StringValue from &str (works with both Arc<str> and ArcStr)
fn sv(s: &str) -> vibesql_types::StringValue {
    vibesql_types::StringValue::from(s)
}

/// Verify that Eq and Ord are consistent for all SqlValue variants
/// This is a requirement for BTreeMap keys:
/// - If a == b, then a.cmp(b) must return Ordering::Equal
/// - If a.cmp(b) == Ordering::Equal, then a == b must be true
#[test]
fn test_eq_ord_consistency() {
    let test_values = vec![
        // NULL
        SqlValue::Null,
        // Integers
        SqlValue::Integer(0),
        SqlValue::Integer(42),
        SqlValue::Integer(-42),
        SqlValue::Smallint(10),
        SqlValue::Bigint(1000),
        SqlValue::Unsigned(50),
        // Floats (including NaN)
        SqlValue::Float(0.0),
        SqlValue::Float(1.5),
        SqlValue::Float(f32::NAN),
        SqlValue::Float(-f32::NAN),
        SqlValue::Real(2.5),
        SqlValue::Real(f32::NAN),
        SqlValue::Double(3.14),
        SqlValue::Double(f64::NAN),
        SqlValue::Numeric(2.718),
        SqlValue::Numeric(f64::NAN),
        // Strings
        SqlValue::Character(sv("hello")),
        SqlValue::Varchar(sv("world")),
        // Boolean
        SqlValue::Boolean(true),
        SqlValue::Boolean(false),
        // Skip Date/Time/Interval for now - they're not the issue
    ];

    // Test all pairs for consistency
    for a in &test_values {
        for b in &test_values {
            let eq_result = a == b;
            let cmp_result = a.cmp(b);
            let cmp_is_equal = cmp_result == Ordering::Equal;

            // Invariant 1: a == b ⟹ a.cmp(b) == Ordering::Equal
            if eq_result {
                assert_eq!(
                    cmp_result,
                    Ordering::Equal,
                    "Eq/Ord inconsistency: {:?} == {:?} is true, but cmp returned {:?}",
                    a,
                    b,
                    cmp_result
                );
            }

            // Invariant 2: a.cmp(b) == Ordering::Equal ⟹ a == b
            if cmp_is_equal {
                assert!(
                    eq_result,
                    "Eq/Ord inconsistency: {:?}.cmp({:?}) == Equal, but == returned false",
                    a, b
                );
            }
        }
    }
}

/// Test that NaN values are handled consistently
#[test]
fn test_nan_consistency() {
    let nan_float = SqlValue::Float(f32::NAN);
    let nan_float2 = SqlValue::Float(f32::NAN);
    let nan_real = SqlValue::Real(f32::NAN);
    let nan_double = SqlValue::Double(f64::NAN);
    let nan_numeric = SqlValue::Numeric(f64::NAN);

    // NaN of the same type should be equal
    assert_eq!(nan_float, nan_float2);
    assert_eq!(nan_float.cmp(&nan_float2), Ordering::Equal);

    // NaN of different types should not be equal
    assert_ne!(nan_float, nan_real);
    assert_ne!(nan_double, nan_numeric);

    // But they should have a consistent ordering
    let cmp1 = nan_float.cmp(&nan_real);
    let cmp2 = nan_float.cmp(&nan_real);
    assert_eq!(cmp1, cmp2, "Ord should be deterministic");
}

/// Test that NULL values are handled consistently
#[test]
fn test_null_consistency() {
    let null1 = SqlValue::Null;
    let null2 = SqlValue::Null;
    let int_val = SqlValue::Integer(42);

    // NULL == NULL
    assert_eq!(null1, null2);
    assert_eq!(null1.cmp(&null2), Ordering::Equal);

    // NULL < all other values
    assert_ne!(null1, int_val);
    assert_eq!(null1.cmp(&int_val), Ordering::Less);
    assert_eq!(int_val.cmp(&null1), Ordering::Greater);
}

/// Test transitivity: if a == b and b == c, then a == c
#[test]
fn test_eq_transitivity() {
    let values = vec![
        (SqlValue::Integer(5), SqlValue::Integer(5), SqlValue::Integer(5)),
        (SqlValue::Float(f32::NAN), SqlValue::Float(f32::NAN), SqlValue::Float(f32::NAN)),
        (SqlValue::Null, SqlValue::Null, SqlValue::Null),
    ];

    for (a, b, c) in values {
        if a == b && b == c {
            assert_eq!(a, c, "Transitivity violated: {:?} == {:?} == {:?}", a, b, c);
        }
    }
}

/// Test reflexivity: a == a always
#[test]
fn test_eq_reflexivity() {
    let test_values = vec![
        SqlValue::Null,
        SqlValue::Integer(42),
        SqlValue::Float(f32::NAN),
        SqlValue::Double(f64::NAN),
        SqlValue::Varchar(sv("test")),
    ];

    for val in test_values {
        assert_eq!(val, val, "Reflexivity violated for {:?}", val);
        assert_eq!(val.cmp(&val), Ordering::Equal);
    }
}

/// Test that BTreeMap can actually store and retrieve SqlValue keys
#[test]
fn test_btreemap_usage() {
    use std::collections::BTreeMap;

    let mut map: BTreeMap<SqlValue, String> = BTreeMap::new();

    // Insert various values
    map.insert(SqlValue::Integer(1), "one".to_string());
    map.insert(SqlValue::Integer(2), "two".to_string());
    map.insert(SqlValue::Float(f32::NAN), "nan_float".to_string());
    map.insert(SqlValue::Double(f64::NAN), "nan_double".to_string());
    map.insert(SqlValue::Null, "null".to_string());
    map.insert(SqlValue::Varchar(sv("hello")), "greeting".to_string());

    // Verify retrieval works
    assert_eq!(map.get(&SqlValue::Integer(1)), Some(&"one".to_string()));
    assert_eq!(map.get(&SqlValue::Integer(2)), Some(&"two".to_string()));
    assert_eq!(map.get(&SqlValue::Null), Some(&"null".to_string()));

    // Verify NaN retrieval works (should find the value)
    assert_eq!(map.get(&SqlValue::Float(f32::NAN)), Some(&"nan_float".to_string()));
    assert_eq!(map.get(&SqlValue::Double(f64::NAN)), Some(&"nan_double".to_string()));
}

/// Test BTreeMap with Vec<SqlValue> keys (like indexes use)
#[test]
fn test_btreemap_vec_keys() {
    use std::collections::{BTreeMap, HashMap};

    // Test data mimicking index keys
    let keys = [
        vec![SqlValue::Integer(1), SqlValue::Integer(2)],
        vec![SqlValue::Integer(3), SqlValue::Integer(4)],
        vec![SqlValue::Integer(1), SqlValue::Integer(2)], // Duplicate
        vec![SqlValue::Float(1.5), SqlValue::Float(2.5)],
        vec![SqlValue::Varchar(sv("a")), SqlValue::Varchar(sv("b"))],
    ];

    let mut btree: BTreeMap<Vec<SqlValue>, Vec<usize>> = BTreeMap::new();
    let mut hash: HashMap<Vec<SqlValue>, Vec<usize>> = HashMap::new();

    // Insert data into both maps
    for (idx, key) in keys.iter().enumerate() {
        btree.entry(key.clone()).or_default().push(idx);
        hash.entry(key.clone()).or_default().push(idx);
    }

    // Verify both maps have the same keys
    let mut btree_keys: Vec<_> = btree.keys().cloned().collect();
    let mut hash_keys: Vec<_> = hash.keys().cloned().collect();

    // Sort both for comparison (HashMap iteration is non-deterministic, BTreeMap iteration is
    // sorted)
    btree_keys.sort();
    hash_keys.sort();

    // They should have the same keys after sorting
    assert_eq!(
        btree_keys.len(),
        hash_keys.len(),
        "BTreeMap and HashMap should have same number of keys"
    );
    for key in &btree_keys {
        assert!(hash_keys.contains(key), "BTreeMap key {:?} should be in HashMap", key);
    }

    // Test lookups
    let search_key = vec![SqlValue::Integer(1), SqlValue::Integer(2)];
    let btree_result = btree.get(&search_key);
    let hash_result = hash.get(&search_key);

    assert_eq!(btree_result, hash_result, "Lookup results should match");

    // Both should find indices 0 and 2 (the duplicate key)
    assert!(btree_result.is_some());
    let mut indices = btree_result.unwrap().clone();
    indices.sort_unstable();
    assert_eq!(indices, vec![0, 2]);
}

/// Test that Vec<SqlValue> comparison works correctly
#[test]
fn test_vec_sqlvalue_comparison() {
    let vec1 = vec![SqlValue::Integer(1), SqlValue::Integer(2)];
    let vec2 = vec![SqlValue::Integer(1), SqlValue::Integer(2)];
    let vec3 = vec![SqlValue::Integer(1), SqlValue::Integer(3)];

    // Test equality
    assert_eq!(vec1, vec2);
    assert_ne!(vec1, vec3);

    // Test ordering
    assert_eq!(vec1.cmp(&vec2), std::cmp::Ordering::Equal);
    assert_eq!(vec1.cmp(&vec3), std::cmp::Ordering::Less);
    assert_eq!(vec3.cmp(&vec1), std::cmp::Ordering::Greater);
}
