//! Special expression forms (CASE, Function calls)

use super::super::{core::ExpressionEvaluator, functions::eval_scalar_function};
use crate::errors::ExecutorError;

impl ExpressionEvaluator<'_> {
    /// Evaluate CASE expression
    pub(super) fn eval_case(
        &self,
        operand: &Option<Box<vibesql_ast::Expression>>,
        when_clauses: &[vibesql_ast::CaseWhen],
        else_result: &Option<Box<vibesql_ast::Expression>>,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        match operand {
            // Simple CASE: CASE operand WHEN value THEN result ...
            Some(operand_expr) => {
                let operand_value = self.eval(operand_expr, row)?;

                for when_clause in when_clauses {
                    // Check if ANY condition matches (OR logic)
                    for condition_expr in &when_clause.conditions {
                        let when_value = self.eval(condition_expr, row)?;

                        if super::super::core::ExpressionEvaluator::values_are_equal(
                            &operand_value,
                            &when_value,
                        ) {
                            return self.eval(&when_clause.result, row);
                        }
                    }
                }
            }

            // Searched CASE: CASE WHEN condition THEN result ...
            None => {
                for when_clause in when_clauses {
                    // Check if ANY condition is TRUE (OR logic)
                    for condition_expr in &when_clause.conditions {
                        let condition_result = self.eval(condition_expr, row)?;

                        if matches!(condition_result, vibesql_types::SqlValue::Boolean(true)) {
                            return self.eval(&when_clause.result, row);
                        }
                    }
                }
            }
        }

        match else_result {
            Some(else_expr) => self.eval(else_expr, row),
            None => Ok(vibesql_types::SqlValue::Null),
        }
    }

    /// Evaluate COALESCE function with lazy evaluation
    /// COALESCE(val1, val2, ...) - returns first non-NULL value
    /// This uses lazy evaluation to short-circuit on first non-NULL value,
    /// avoiding evaluation of expensive expressions.
    pub(super) fn eval_coalesce_lazy(
        &self,
        args: &[vibesql_ast::Expression],
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        if args.is_empty() {
            return Err(ExecutorError::UnsupportedFeature(
                "COALESCE requires at least one argument".to_string(),
            ));
        }

        // Lazy evaluation: return first non-NULL value without evaluating remaining args
        for arg in args {
            let val = self.eval(arg, row)?;
            if !matches!(val, vibesql_types::SqlValue::Null) {
                return Ok(val);
            }
        }

        // All arguments were NULL
        Ok(vibesql_types::SqlValue::Null)
    }

    /// Evaluate NULLIF function with lazy evaluation
    /// NULLIF(val1, val2) - returns NULL if val1 = val2, otherwise val1
    /// This uses lazy evaluation to avoid unnecessary comparisons.
    pub(super) fn eval_nullif_lazy(
        &self,
        args: &[vibesql_ast::Expression],
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        if args.len() != 2 {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "NULLIF requires exactly 2 arguments, got {}",
                args.len()
            )));
        }

        // Evaluate first argument (required)
        let val1 = self.eval(&args[0], row)?;

        // If first is NULL, return NULL immediately without evaluating second
        if matches!(val1, vibesql_types::SqlValue::Null) {
            return Ok(val1);
        }

        // Evaluate second argument
        let val2 = self.eval(&args[1], row)?;

        // If either is NULL, comparison is undefined - return val1
        if matches!(val2, vibesql_types::SqlValue::Null) {
            return Ok(val1);
        }

        // Check equality and return accordingly
        if super::super::core::ExpressionEvaluator::values_are_equal(&val1, &val2) {
            Ok(vibesql_types::SqlValue::Null)
        } else {
            Ok(val1)
        }
    }

    /// Evaluate function call
    pub(super) fn eval_function(
        &self,
        name: &str,
        args: &[vibesql_ast::Expression],
        character_unit: &Option<vibesql_ast::CharacterUnit>,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Handle special functions with lazy evaluation
        match name.to_uppercase().as_str() {
            "COALESCE" => return self.eval_coalesce_lazy(args, row),
            "NULLIF" => return self.eval_nullif_lazy(args, row),
            // Handle LAST_INSERT_ROWID() and LAST_INSERT_ID() - require database access
            "LAST_INSERT_ROWID" | "LAST_INSERT_ID" => {
                if !args.is_empty() {
                    return Err(ExecutorError::UnsupportedFeature(format!(
                        "{}() takes no arguments",
                        name.to_uppercase()
                    )));
                }
                if let Some(db) = self.database {
                    return Ok(vibesql_types::SqlValue::Integer(db.last_insert_rowid()));
                } else {
                    // No database context available, return 0
                    return Ok(vibesql_types::SqlValue::Integer(0));
                }
            }
            _ => {}
        }

        // Check for user-defined functions (Phase 5)
        // Note: UDF execution requires mutable database access, which we don't have here.
        // For now, we'll check if the function exists and return a helpful error.
        // Full UDF support will require refactoring the evaluator to support mutable access.
        if let Some(db) = self.database {
            if db.catalog.function_exists(name) {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "User-defined function '{}' found but cannot be executed in this context. \
                         UDF execution from SELECT expressions requires mutable database access. \
                         This is a known limitation that will be addressed in a future phase.",
                    name
                )));
            }
        }

        // Standard function call: evaluate all arguments eagerly
        let mut arg_values = Vec::new();
        for arg in args {
            arg_values.push(self.eval(arg, row)?);
        }

        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
        eval_scalar_function(name, &arg_values, character_unit, &sql_mode)
    }
}
