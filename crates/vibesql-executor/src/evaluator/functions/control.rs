//! Control flow functions (IF)

use crate::errors::ExecutorError;

/// IF(condition, true_value, false_value) - MySQL-style conditional
/// Returns true_value if condition is true, otherwise returns false_value
pub(super) fn if_func(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "IF requires exactly 3 arguments, got {}",
            args.len()
        )));
    }

    // Evaluate condition
    let condition = &args[0];
    match condition {
        vibesql_types::SqlValue::Boolean(true) => Ok(args[1].clone()),
        vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => {
            Ok(args[2].clone())
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "IF condition must be boolean, got {:?}",
            condition
        ))),
    }
}
