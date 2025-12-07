//! System information functions
//!
//! This module provides SQL functions that return system-level information:
//! - VERSION() - Database version information
//! - DATABASE() / SCHEMA() - Current database name
//! - USER() / CURRENT_USER() - Current user information
//! - LAST_INSERT_ROWID() / LAST_INSERT_ID() - Last auto-generated ID
//!
//! Note: LAST_INSERT_ROWID() and LAST_INSERT_ID() are handled specially in the
//! expression evaluator because they require database context. They are not
//! called through eval_scalar_function().

use crate::errors::ExecutorError;

/// VERSION() - Return database version
pub(super) fn version(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature("VERSION takes no arguments".to_string()));
    }

    // Get version from Cargo.toml at compile time
    let version = env!("CARGO_PKG_VERSION");
    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(format!("NistMemSQL {}", version))))
}

/// DATABASE() / SCHEMA() - Return current database name
pub(super) fn database(
    args: &[vibesql_types::SqlValue],
    name: &str,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(format!("{} takes no arguments", name)));
    }

    // In current implementation, return default database name
    // In future with connection context, return actual database name
    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("default")))
}

/// USER() / CURRENT_USER() - Return current user
pub(super) fn user(
    args: &[vibesql_types::SqlValue],
    name: &str,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(format!("{} takes no arguments", name)));
    }

    // In current implementation, return default user
    // In future with authentication, return actual username
    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("anonymous")))
}
