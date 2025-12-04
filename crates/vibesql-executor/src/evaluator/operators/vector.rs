//! Vector distance operators for pgvector-compatible similarity search
//!
//! Implements distance operators for vector similarity queries:
//! - `<->` Cosine distance (1 - cosine_similarity)
//! - `<#>` Negative inner product (for Maximum Inner Product Search)
//! - `<=>` L2 (Euclidean) distance

use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Vector distance operations
pub(crate) struct VectorOps;

impl VectorOps {
    /// Cosine distance: 1 - cosine_similarity
    /// Maps to pgvector's <-> operator
    ///
    /// Cosine similarity = dot(a, b) / (||a|| * ||b||)
    /// Cosine distance = 1 - cosine_similarity
    ///
    /// Returns a value between 0 (identical) and 2 (opposite)
    #[inline]
    pub fn cosine_distance(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        match (left, right) {
            (SqlValue::Vector(v1), SqlValue::Vector(v2)) => {
                if v1.len() != v2.len() {
                    return Err(ExecutorError::TypeError(format!(
                        "Vector dimension mismatch: {} vs {}",
                        v1.len(),
                        v2.len()
                    )));
                }

                if v1.is_empty() {
                    return Err(ExecutorError::TypeError(
                        "Cannot compute cosine distance of empty vectors".to_string(),
                    ));
                }

                let dot_product: f64 =
                    v1.iter().zip(v2.iter()).map(|(a, b)| (*a as f64) * (*b as f64)).sum();

                let norm1: f64 = v1.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt();
                let norm2: f64 = v2.iter().map(|x| (*x as f64) * (*x as f64)).sum::<f64>().sqrt();

                // Handle zero vectors - distance is 1.0 (orthogonal)
                if norm1 == 0.0 || norm2 == 0.0 {
                    return Ok(SqlValue::Double(1.0));
                }

                let cosine_similarity = dot_product / (norm1 * norm2);
                // Clamp to handle floating point precision issues
                let cosine_similarity = cosine_similarity.clamp(-1.0, 1.0);
                let distance = 1.0 - cosine_similarity;

                Ok(SqlValue::Double(distance))
            }
            (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
            _ => Err(ExecutorError::TypeError(format!(
                "Cosine distance operator <-> requires VECTOR operands, got {} and {}",
                left.type_name(),
                right.type_name()
            ))),
        }
    }

    /// Negative inner product (dot product)
    /// Maps to pgvector's <#> operator
    ///
    /// Used for Maximum Inner Product Search (MIPS)
    /// Negated so that ORDER BY ... ASC gives highest similarity first
    #[inline]
    pub fn negative_inner_product(
        left: &SqlValue,
        right: &SqlValue,
    ) -> Result<SqlValue, ExecutorError> {
        match (left, right) {
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

                // Negate so ORDER BY ASC gives highest similarity first
                Ok(SqlValue::Double(-dot_product))
            }
            (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
            _ => Err(ExecutorError::TypeError(format!(
                "Negative inner product operator <#> requires VECTOR operands, got {} and {}",
                left.type_name(),
                right.type_name()
            ))),
        }
    }

    /// L2 (Euclidean) distance
    /// Maps to pgvector's <=> operator
    ///
    /// L2 distance = sqrt(sum((v1[i] - v2[i])^2))
    #[inline]
    pub fn l2_distance(left: &SqlValue, right: &SqlValue) -> Result<SqlValue, ExecutorError> {
        match (left, right) {
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
                        let diff = (*a as f64) - (*b as f64);
                        diff * diff
                    })
                    .sum();

                Ok(SqlValue::Double(sum_sq.sqrt()))
            }
            (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
            _ => Err(ExecutorError::TypeError(format!(
                "L2 distance operator <=> requires VECTOR operands, got {} and {}",
                left.type_name(),
                right.type_name()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vec_val(v: &[f32]) -> SqlValue {
        SqlValue::Vector(v.to_vec())
    }

    #[test]
    fn test_cosine_distance_identical() {
        let v = vec_val(&[1.0, 0.0, 0.0]);
        let result = VectorOps::cosine_distance(&v, &v).unwrap();
        if let SqlValue::Double(d) = result {
            assert!((d - 0.0).abs() < 1e-10);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_cosine_distance_orthogonal() {
        let v1 = vec_val(&[1.0, 0.0]);
        let v2 = vec_val(&[0.0, 1.0]);
        let result = VectorOps::cosine_distance(&v1, &v2).unwrap();
        if let SqlValue::Double(d) = result {
            assert!((d - 1.0).abs() < 1e-10);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_cosine_distance_opposite() {
        let v1 = vec_val(&[1.0, 0.0]);
        let v2 = vec_val(&[-1.0, 0.0]);
        let result = VectorOps::cosine_distance(&v1, &v2).unwrap();
        if let SqlValue::Double(d) = result {
            assert!((d - 2.0).abs() < 1e-10);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_l2_distance() {
        let v1 = vec_val(&[0.0, 0.0]);
        let v2 = vec_val(&[3.0, 4.0]);
        let result = VectorOps::l2_distance(&v1, &v2).unwrap();
        if let SqlValue::Double(d) = result {
            assert!((d - 5.0).abs() < 1e-10);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_negative_inner_product() {
        let v1 = vec_val(&[1.0, 2.0, 3.0]);
        let v2 = vec_val(&[4.0, 5.0, 6.0]);
        // dot product = 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
        // negative = -32
        let result = VectorOps::negative_inner_product(&v1, &v2).unwrap();
        if let SqlValue::Double(d) = result {
            assert!((d - (-32.0)).abs() < 1e-10);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_dimension_mismatch() {
        let v1 = vec_val(&[1.0, 2.0]);
        let v2 = vec_val(&[1.0, 2.0, 3.0]);
        assert!(VectorOps::cosine_distance(&v1, &v2).is_err());
        assert!(VectorOps::l2_distance(&v1, &v2).is_err());
        assert!(VectorOps::negative_inner_product(&v1, &v2).is_err());
    }

    #[test]
    fn test_null_handling() {
        let v = vec_val(&[1.0, 2.0]);
        assert!(matches!(VectorOps::cosine_distance(&SqlValue::Null, &v).unwrap(), SqlValue::Null));
        assert!(matches!(VectorOps::l2_distance(&v, &SqlValue::Null).unwrap(), SqlValue::Null));
        assert!(matches!(
            VectorOps::negative_inner_product(&SqlValue::Null, &SqlValue::Null).unwrap(),
            SqlValue::Null
        ));
    }

    #[test]
    fn test_type_error() {
        let v = vec_val(&[1.0, 2.0]);
        let i = SqlValue::Integer(42);
        assert!(VectorOps::cosine_distance(&v, &i).is_err());
        assert!(VectorOps::l2_distance(&i, &v).is_err());
        assert!(VectorOps::negative_inner_product(&i, &i).is_err());
    }
}
