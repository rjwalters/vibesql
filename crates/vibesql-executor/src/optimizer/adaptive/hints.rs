//! Query hint extraction

use vibesql_ast::SelectStmt;

use super::ExecutionModel;

/// Extract query execution hint from comment
///
/// Supports:
/// - `/* COLUMNAR */` - Force columnar execution
/// - `/* ROW_ORIENTED */` - Force row-oriented execution
///
/// Note: Currently returns None as hint parsing from comments
/// would require parser changes. This is a placeholder for future enhancement.
pub(super) fn extract_query_hint(_query: &SelectStmt) -> Option<ExecutionModel> {
    // TODO: Extract hints from query comments when parser support is added
    // For now, hints are not supported
    None
}
