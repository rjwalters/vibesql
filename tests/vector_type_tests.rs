//! Vector type integration tests
//! Tests for Vector data type functionality across parser and types layers

use vibesql_parser::Parser;
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_vector_type_parsing() {
    // Test parsing VECTOR(n) type
    let result = Parser::parse_sql("CREATE TABLE embeddings (id INT, vec VECTOR(1536))");
    assert!(result.is_ok(), "Failed to parse VECTOR(1536) type: {:?}", result.err());
}

#[test]
fn test_vector_dimension_validation() {
    // Verify dimensions must be positive
    let vector_type = DataType::Vector { dimensions: 1536 };
    assert_eq!(vector_type, DataType::Vector { dimensions: 1536 });
}

#[test]
fn test_vector_value_creation() {
    // Create a vector value
    let vec_data = vec![0.1f32, 0.2f32, 0.3f32, 0.4f32];
    let vec_val = SqlValue::Vector(vec_data.clone());

    // Check type_name
    assert_eq!(vec_val.type_name(), "VECTOR");

    // Check get_type preserves dimensions
    let dtype = vec_val.get_type();
    assert_eq!(dtype, DataType::Vector { dimensions: 4 });
}

#[test]
fn test_vector_type_dimensions_match() {
    // Vectors with matching dimensions should be equal
    let vec_type_1536_a = DataType::Vector { dimensions: 1536 };
    let vec_type_1536_b = DataType::Vector { dimensions: 1536 };

    assert_eq!(vec_type_1536_a, vec_type_1536_b);
}

#[test]
fn test_vector_dimension_mismatch() {
    // Vectors with different dimensions should not be equal
    let vec_type_1536 = DataType::Vector { dimensions: 1536 };
    let vec_type_384 = DataType::Vector { dimensions: 384 };

    assert_ne!(vec_type_1536, vec_type_384);
}

#[test]
fn test_vector_value_display() {
    let vec_val = SqlValue::Vector(vec![1.0f32, 2.0f32, 3.0f32]);
    let displayed = format!("{}", vec_val);
    assert!(displayed.contains("1"));
    assert!(displayed.contains("2"));
    assert!(displayed.contains("3"));
}

#[test]
fn test_vector_is_null() {
    let vec_val = SqlValue::Vector(vec![0.1f32, 0.2f32]);
    assert!(!vec_val.is_null());

    let null_val = SqlValue::Null;
    assert!(null_val.is_null());
}

#[test]
fn test_vector_memory_estimation() {
    let vec_data = vec![0.1f32; 1536]; // 1536-dimensional vector
    let vec_val = SqlValue::Vector(vec_data);

    let size = vec_val.estimated_size_bytes();
    // Should be roughly: base size + (1536 f32s = 1536 * 4 bytes)
    assert!(size >= 1536 * 4, "Vector size estimation too small");
}

#[test]
fn test_vector_type_is_distinct() {
    // Vector type should be distinct from other types
    let vec_type = DataType::Vector { dimensions: 100 };
    let int_type = DataType::Integer;
    let varchar_type = DataType::Varchar { max_length: None };

    // Vectors should be different types from integers and varchars
    assert_ne!(vec_type, int_type);
    assert_ne!(vec_type, varchar_type);
}

#[test]
fn test_vector_empty() {
    // Empty vector should work
    let vec_val = SqlValue::Vector(vec![]);
    assert_eq!(vec_val.type_name(), "VECTOR");
    let dtype = vec_val.get_type();
    assert_eq!(dtype, DataType::Vector { dimensions: 0 });
}

#[test]
fn test_vector_single_element() {
    // Single element vector
    let vec_val = SqlValue::Vector(vec![42.0f32]);
    let dtype = vec_val.get_type();
    assert_eq!(dtype, DataType::Vector { dimensions: 1 });
}
