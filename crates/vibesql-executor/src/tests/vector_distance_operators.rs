//! Vector distance operator tests
//!
//! Tests for pgvector-compatible distance operators:
//! - <-> (cosine distance)
//! - <#> (negative inner product)
//! - <=> (L2/Euclidean distance)

use super::super::*;

// =============================================================================
// Cosine Distance (<->) Tests
// =============================================================================

#[test]
fn test_cosine_distance_identical_vectors() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // Identical vectors should have distance 0
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            1.0, 0.0, 0.0,
        ]))),
        op: vibesql_ast::BinaryOperator::CosineDistance,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            1.0, 0.0, 0.0,
        ]))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();
    if let vibesql_types::SqlValue::Double(d) = result {
        assert!((d - 0.0).abs() < 1e-10, "Expected 0.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

#[test]
fn test_cosine_distance_orthogonal_vectors() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // Orthogonal vectors should have distance 1
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            1.0, 0.0,
        ]))),
        op: vibesql_ast::BinaryOperator::CosineDistance,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            0.0, 1.0,
        ]))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();
    if let vibesql_types::SqlValue::Double(d) = result {
        assert!((d - 1.0).abs() < 1e-10, "Expected 1.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

#[test]
fn test_cosine_distance_opposite_vectors() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // Opposite vectors should have distance 2
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            1.0, 0.0,
        ]))),
        op: vibesql_ast::BinaryOperator::CosineDistance,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            -1.0, 0.0,
        ]))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();
    if let vibesql_types::SqlValue::Double(d) = result {
        assert!((d - 2.0).abs() < 1e-10, "Expected 2.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

// =============================================================================
// L2 Distance (<=>) Tests
// =============================================================================

#[test]
fn test_l2_distance_identical_vectors() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // Identical vectors should have distance 0
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            1.0, 2.0, 3.0,
        ]))),
        op: vibesql_ast::BinaryOperator::L2Distance,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            1.0, 2.0, 3.0,
        ]))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();
    if let vibesql_types::SqlValue::Double(d) = result {
        assert!((d - 0.0).abs() < 1e-10, "Expected 0.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

#[test]
fn test_l2_distance_known_values() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // 3-4-5 triangle: sqrt(3^2 + 4^2) = 5
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            0.0, 0.0,
        ]))),
        op: vibesql_ast::BinaryOperator::L2Distance,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            3.0, 4.0,
        ]))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();
    if let vibesql_types::SqlValue::Double(d) = result {
        assert!((d - 5.0).abs() < 1e-10, "Expected 5.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

// =============================================================================
// Negative Inner Product (<#>) Tests
// =============================================================================

#[test]
fn test_negative_inner_product() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // dot product = 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
    // negative = -32
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            1.0, 2.0, 3.0,
        ]))),
        op: vibesql_ast::BinaryOperator::NegativeInnerProduct,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            4.0, 5.0, 6.0,
        ]))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();
    if let vibesql_types::SqlValue::Double(d) = result {
        assert!((d - (-32.0)).abs() < 1e-10, "Expected -32.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

#[test]
fn test_negative_inner_product_orthogonal() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // Orthogonal vectors have dot product = 0, negative = 0
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            1.0, 0.0,
        ]))),
        op: vibesql_ast::BinaryOperator::NegativeInnerProduct,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            0.0, 1.0,
        ]))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();
    if let vibesql_types::SqlValue::Double(d) = result {
        assert!((d - 0.0).abs() < 1e-10, "Expected 0.0, got {}", d);
    } else {
        panic!("Expected Double, got {:?}", result);
    }
}

// =============================================================================
// NULL Handling Tests
// =============================================================================

#[test]
fn test_distance_operators_null_handling() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    let v = vibesql_types::SqlValue::Vector(vec![1.0, 2.0, 3.0]);

    // NULL <-> vec = NULL
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
        op: vibesql_ast::BinaryOperator::CosineDistance,
        right: Box::new(vibesql_ast::Expression::Literal(v.clone())),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert!(matches!(result, vibesql_types::SqlValue::Null));

    // vec <=> NULL = NULL
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(v.clone())),
        op: vibesql_ast::BinaryOperator::L2Distance,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert!(matches!(result, vibesql_types::SqlValue::Null));

    // NULL <#> NULL = NULL
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
        op: vibesql_ast::BinaryOperator::NegativeInnerProduct,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert!(matches!(result, vibesql_types::SqlValue::Null));
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[test]
fn test_dimension_mismatch_error() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // Different dimensions should error
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            1.0, 2.0,
        ]))),
        op: vibesql_ast::BinaryOperator::CosineDistance,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            1.0, 2.0, 3.0,
        ]))),
    };

    let err = evaluator.eval(&expr, &row).unwrap_err();
    match err {
        ExecutorError::TypeError(msg) => {
            assert!(
                msg.contains("dimension mismatch"),
                "Expected dimension mismatch error, got: {}",
                msg
            );
        }
        other => panic!("Expected TypeError, got {:?}", other),
    }
}

#[test]
fn test_type_mismatch_error() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // Non-vector operands should error
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Vector(vec![
            1.0, 2.0,
        ]))),
        op: vibesql_ast::BinaryOperator::L2Distance,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42))),
    };

    let err = evaluator.eval(&expr, &row).unwrap_err();
    match err {
        ExecutorError::TypeError(msg) => {
            assert!(msg.contains("VECTOR"), "Expected VECTOR type error, got: {}", msg);
        }
        other => panic!("Expected TypeError, got {:?}", other),
    }
}
