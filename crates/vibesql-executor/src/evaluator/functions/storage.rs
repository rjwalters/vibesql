use crate::errors::ExecutorError;
/// Storage-related SQL functions
///
/// Functions for working with blob/file storage:
/// - STORAGE_URL(blob_id) - Generate URL for accessing a blob
use vibesql_types::SqlValue;

/// STORAGE_URL(blob_id TEXT) -> TEXT
///
/// Generate a URL for accessing a stored blob.
/// For local filesystem, returns a relative path.
/// For cloud storage, could return a signed URL.
///
/// Example:
///   SELECT STORAGE_URL(blob_id) FROM files;
pub fn eval_storage_url(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedExpression(
            "STORAGE_URL requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(blob_id_str) => {
            // For now, simply construct a URL from the blob ID
            // In a full implementation, this would consult the BlobStorageService
            let url = format!("/storage/blobs/{}", blob_id_str);
            Ok(SqlValue::Varchar(arcstr::ArcStr::from(url)))
        }
        _ => Err(ExecutorError::UnsupportedExpression(
            "STORAGE_URL argument must be TEXT".to_string(),
        )),
    }
}

/// STORAGE_SIZE(blob_id TEXT) -> BIGINT
///
/// Get the size in bytes of a stored blob.
/// Queries the vibesql_storage system table for metadata.
/// Returns NULL if the blob does not exist.
pub fn eval_storage_size(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedExpression(
            "STORAGE_SIZE requires exactly 1 argument".to_string(),
        ));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(_blob_id_str) => {
            // Query vibesql_storage table for size metadata
            // This would normally be done via database lookup, but for now we return NULL
            // as the function doesn't have access to the database context
            // TODO: Refactor to pass database context to function evaluators
            Ok(SqlValue::Null)
        }
        _ => Err(ExecutorError::UnsupportedExpression(
            "STORAGE_SIZE argument must be TEXT".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_url_function() {
        let blob_id = SqlValue::Varchar(arcstr::ArcStr::from("550e8400-e29b-41d4-a716-446655440000"));
        let result = eval_storage_url(&[blob_id]).unwrap();

        match result {
            SqlValue::Varchar(url) => {
                assert!(url.starts_with("/storage/blobs/"));
                assert!(url.contains("550e8400"));
            }
            _ => panic!("Expected VARCHAR result"),
        }
    }

    #[test]
    fn test_storage_url_null() {
        let result = eval_storage_url(&[SqlValue::Null]).unwrap();
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_storage_url_wrong_arg_count() {
        let err = eval_storage_url(&[]).unwrap_err();
        match err {
            ExecutorError::UnsupportedExpression(msg) => {
                assert!(msg.contains("requires exactly 1 argument"));
            }
            _ => panic!("Expected UnsupportedExpression error"),
        }
    }

    #[test]
    fn test_storage_size_null() {
        let result = eval_storage_size(&[SqlValue::Null]).unwrap();
        assert_eq!(result, SqlValue::Null);
    }
}
