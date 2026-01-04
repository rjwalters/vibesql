// ============================================================================
// Prefix Truncation Utilities
// ============================================================================

use vibesql_types::SqlValue;

/// Apply prefix truncation to a SqlValue if prefix_length is specified
///
/// For string types (Varchar, Char, Text), truncates to first N characters.
/// For other types, returns the value unchanged (prefix indexing only applies to strings).
///
/// # Arguments
/// * `value` - The value to potentially truncate
/// * `prefix_length` - Optional prefix length in characters
///
/// # Returns
/// Truncated value if applicable, otherwise the original value
pub(crate) fn apply_prefix_truncation(value: &SqlValue, prefix_length: Option<u64>) -> SqlValue {
    // If no prefix length specified, return value as-is
    let Some(prefix_len) = prefix_length else {
        return value.clone();
    };

    // Only apply truncation to string types
    match value {
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Take first N characters (UTF-8 aware)
            let truncated: String = s.chars().take(prefix_len as usize).collect();
            let truncated = arcstr::ArcStr::from(truncated.as_str());
            // Return same type as input
            match value {
                SqlValue::Varchar(_) => SqlValue::Varchar(truncated),
                SqlValue::Character(_) => SqlValue::Character(truncated),
                _ => unreachable!(),
            }
        }
        // For non-string types, prefix indexing doesn't apply
        _ => value.clone(),
    }
}
