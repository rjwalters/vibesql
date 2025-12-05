//! Vector distance and utility functions
//!
//! This module implements SQL functions for vector similarity search:
//! - Distance functions: COSINE_DISTANCE, L2_DISTANCE, INNER_PRODUCT
//! - Utility functions: VECTOR_NORM, VECTOR_DIMS
//!
//! These functions support AI/ML similarity search operations.

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// COSINE_DISTANCE(v1, v2) - Cosine distance between two vectors
///
/// Returns the cosine distance (1 - cosine similarity) between two vectors.
/// Value ranges from 0 (identical) to 2 (opposite).
/// Most common for text embeddings (OpenAI, Cohere, etc.).
pub fn cosine_distance(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "COSINE_DISTANCE requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),

        (SqlValue::Vector(v1), SqlValue::Vector(v2)) => {
            if v1.len() != v2.len() {
                return Err(ExecutorError::TypeError(format!(
                    "Vector dimension mismatch: {} vs {}",
                    v1.len(),
                    v2.len()
                )));
            }

            let dot_product: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();

            let norm1: f32 = v1.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm2: f32 = v2.iter().map(|x| x * x).sum::<f32>().sqrt();

            if norm1 == 0.0 || norm2 == 0.0 {
                return Ok(SqlValue::Double(1.0)); // Undefined vectors treated as distance 1.0
            }

            let cosine_similarity = dot_product / (norm1 * norm2);
            let distance = 1.0 - cosine_similarity as f64;

            Ok(SqlValue::Double(distance))
        }

        (SqlValue::Vector(_), other) => Err(ExecutorError::TypeError(format!(
            "COSINE_DISTANCE: second argument must be VECTOR, got {}",
            other.type_name()
        ))),

        (other, _) => Err(ExecutorError::TypeError(format!(
            "COSINE_DISTANCE: first argument must be VECTOR, got {}",
            other.type_name()
        ))),
    }
}

/// L2_DISTANCE(v1, v2) - Euclidean (L2) distance between two vectors
///
/// Returns the Euclidean distance, computed as sqrt(sum((v1[i] - v2[i])^2)).
pub fn l2_distance(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "L2_DISTANCE requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),

        (SqlValue::Vector(v1), SqlValue::Vector(v2)) => {
            if v1.len() != v2.len() {
                return Err(ExecutorError::TypeError(format!(
                    "Vector dimension mismatch: {} vs {}",
                    v1.len(),
                    v2.len()
                )));
            }

            let sum_sq: f64 = v1
                .iter()
                .zip(v2.iter())
                .map(|(a, b)| {
                    let diff = *a as f64 - *b as f64;
                    diff * diff
                })
                .sum();

            Ok(SqlValue::Double(sum_sq.sqrt()))
        }

        (SqlValue::Vector(_), other) => Err(ExecutorError::TypeError(format!(
            "L2_DISTANCE: second argument must be VECTOR, got {}",
            other.type_name()
        ))),

        (other, _) => Err(ExecutorError::TypeError(format!(
            "L2_DISTANCE: first argument must be VECTOR, got {}",
            other.type_name()
        ))),
    }
}

/// INNER_PRODUCT(v1, v2) - Inner product (dot product) of two vectors
///
/// Returns the dot product of two vectors.
/// For normalized vectors, equivalent to cosine similarity.
pub fn inner_product(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "INNER_PRODUCT requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),

        (SqlValue::Vector(v1), SqlValue::Vector(v2)) => {
            if v1.len() != v2.len() {
                return Err(ExecutorError::TypeError(format!(
                    "Vector dimension mismatch: {} vs {}",
                    v1.len(),
                    v2.len()
                )));
            }

            let dot_product: f64 =
                v1.iter().zip(v2.iter()).map(|(a, b)| (*a as f64) * (*b as f64)).sum();

            Ok(SqlValue::Double(dot_product))
        }

        (SqlValue::Vector(_), other) => Err(ExecutorError::TypeError(format!(
            "INNER_PRODUCT: second argument must be VECTOR, got {}",
            other.type_name()
        ))),

        (other, _) => Err(ExecutorError::TypeError(format!(
            "INNER_PRODUCT: first argument must be VECTOR, got {}",
            other.type_name()
        ))),
    }
}

/// VECTOR_NORM(v) - L2 norm of a vector
///
/// Returns the L2 norm (Euclidean length) of the vector.
/// Useful for normalization.
pub fn vector_norm(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "VECTOR_NORM requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),

        SqlValue::Vector(v) => {
            let sum_sq: f64 = v.iter().map(|x| (*x as f64) * (*x as f64)).sum();
            Ok(SqlValue::Double(sum_sq.sqrt()))
        }

        other => Err(ExecutorError::TypeError(format!(
            "VECTOR_NORM: argument must be VECTOR, got {}",
            other.type_name()
        ))),
    }
}

/// VECTOR_DIMS(v) - Number of dimensions in a vector
///
/// Returns the dimension count of the vector.
/// Useful for validation queries.
pub fn vector_dims(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "VECTOR_DIMS requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),

        SqlValue::Vector(v) => Ok(SqlValue::Integer(v.len() as i64)),

        other => Err(ExecutorError::TypeError(format!(
            "VECTOR_DIMS: argument must be VECTOR, got {}",
            other.type_name()
        ))),
    }
}
