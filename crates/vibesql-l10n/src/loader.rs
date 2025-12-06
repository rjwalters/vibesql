//! Lazy bundle loading and error types.

use thiserror::Error;

/// Errors that can occur during localization operations.
#[derive(Error, Debug)]
pub enum L10nError {
    /// The specified locale identifier is invalid.
    #[error("Invalid locale identifier: {0}")]
    InvalidLocale(String),

    /// The requested resource file was not found.
    #[error("Resource not found: {0}")]
    ResourceNotFound(String),

    /// Failed to parse a Fluent resource file.
    #[error("Failed to parse Fluent resource: {0}")]
    ParseError(String),

    /// Failed to acquire lock on l10n state.
    #[error("Failed to acquire l10n lock")]
    LockError,

    /// I/O error when reading resource files.
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}
