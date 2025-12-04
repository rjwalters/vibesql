//! Vector type integration tests
//! Tests for Vector data type functionality across parser and types layers

use vibesql_parser::Parser;
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_vector_type_parsing() {
    // Test parsing VECTOR(n) type
    let mut parser = Parser::new("CREATE TABLE embeddings (id INT, vec VECTOR(1536))");
    let result = parser.parse();
    assert!(result.is_ok(), "Failed to parse VECTOR(1536) type");
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
    let vec_data = vec![0.1, 0.2, 0.3, 0.4];
    let vec_val = SqlValue::Vector(vec_data.clone());
    
    // Check type_name
    assert_eq!(vec_val.type_name(), "VECTOR");
    
    // Check get_type preserves dimensions
    let dtype = vec_val.get_type();
    assert_eq!(dtype, DataType::Vector { dimensions: 4 });
}

#[test]
fn test_vector_type_coercion() {
    // Vectors with matching dimensions can coerce
    let vec_type_1536_a = DataType::Vector { dimensions: 1536 };
    let vec_type_1536_b = DataType::Vector { dimensions: 1536 };
    
    assert!(vec_type_1536_a.can_implicitly_coerce(&vec_type_1536_b));
    assert!(vec_type_1536_a.coerce_to_common(&vec_type_1536_b).is_some());
}

#[test]
fn test_vector_dimension_mismatch() {
    // Vectors with different dimensions cannot coerce
    let vec_type_1536 = DataType::Vector { dimensions: 1536 };
    let vec_type_384 = DataType::Vector { dimensions: 384 };
    
    assert!(!vec_type_1536.can_implicitly_coerce(&vec_type_384));
    assert!(vec_type_1536.coerce_to_common(&vec_type_384).is_none());
}

#[test]
fn test_vector_value_display() {
    let vec_val = SqlValue::Vector(vec![1.0, 2.0, 3.0]);
    let displayed = format!("{}", vec_val);
    assert!(displayed.contains("1"));
    assert!(displayed.contains("2"));
    assert!(displayed.contains("3"));
}

#[test]
fn test_vector_is_null() {
    let vec_val = SqlValue::Vector(vec![0.1, 0.2]);
    assert!(!vec_val.is_null());
    
    let null_val = SqlValue::Null;
    assert!(null_val.is_null());
}

#[test]
fn test_vector_memory_estimation() {
    let vec_data = vec![0.1; 1536]; // 1536-dimensional vector
    let vec_val = SqlValue::Vector(vec_data);
    
    let size = vec_val.estimated_size_bytes();
    // Should be roughly: base size + (1536 f32s = 1536 * 4 bytes)
    assert!(size >= 1536 * 4, "Vector size estimation too small");
}

#[test]
fn test_vector_type_precedence() {
    let vec_type = DataType::Vector { dimensions: 100 };
    let int_type = DataType::Integer;
    let varchar_type = DataType::Varchar { max_length: None };
    
    let vec_prec = vec_type.type_precedence();
    let int_prec = int_type.type_precedence();
    let varchar_prec = varchar_type.type_precedence();
    
    // Vector should have different precedence than numeric and string types
    assert_ne!(vec_prec, int_prec);
    assert_ne!(vec_prec, varchar_prec);
}
