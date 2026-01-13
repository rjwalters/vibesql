//! Special expression forms (CASE, CAST, Function calls)

use super::super::{
    casting::cast_value,
    core::{CombinedExpressionEvaluator, ExpressionEvaluator},
    functions::eval_scalar_function,
};
use crate::errors::ExecutorError;

impl CombinedExpressionEvaluator<'_> {
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

                // Iterate through WHEN clauses
                for when_clause in when_clauses {
                    // Check if ANY condition matches (OR logic)
                    for condition_expr in &when_clause.conditions {
                        let when_value = self.eval(condition_expr, row)?;

                        // Compare operand to when_value using SQL equality semantics
                        if ExpressionEvaluator::values_are_equal(&operand_value, &when_value) {
                            return self.eval(&when_clause.result, row);
                        }
                    }
                }
            }

            // Searched CASE: CASE WHEN condition THEN result ...
            None => {
                // Iterate through WHEN clauses
                for when_clause in when_clauses {
                    // Check if ANY condition is TRUE (OR logic)
                    for condition_expr in &when_clause.conditions {
                        let condition_result = self.eval(condition_expr, row)?;

                        // In SQLite, truthiness is:
                        // - Boolean(true) => true
                        // - Integer/Bigint non-zero => true
                        // - Float/Real/Double/Numeric non-zero => true
                        // - Strings: parse leading numeric, non-zero => true
                        // - Null, zero => false
                        let is_truthy = match &condition_result {
                            vibesql_types::SqlValue::Boolean(b) => *b,
                            vibesql_types::SqlValue::Integer(n) => *n != 0,
                            vibesql_types::SqlValue::Bigint(n) => *n != 0,
                            vibesql_types::SqlValue::Smallint(n) => *n != 0,
                            vibesql_types::SqlValue::Unsigned(n) => *n != 0,
                            vibesql_types::SqlValue::Double(n) => *n != 0.0,
                            vibesql_types::SqlValue::Float(n) => *n != 0.0,
                            vibesql_types::SqlValue::Real(n) => *n != 0.0,
                            vibesql_types::SqlValue::Numeric(n) => *n != 0.0,
                            // SQLite parses leading numeric from strings
                            vibesql_types::SqlValue::Varchar(s)
                            | vibesql_types::SqlValue::Character(s) => {
                                super::super::operators::is_truthy_string(s)
                            }
                            _ => false,
                        };
                        if is_truthy {
                            return self.eval(&when_clause.result, row);
                        }
                    }
                }
            }
        }

        // No WHEN matched, evaluate ELSE or return NULL
        match else_result {
            Some(else_expr) => self.eval(else_expr, row),
            None => Ok(vibesql_types::SqlValue::Null),
        }
    }

    /// Evaluate CAST expression: CAST(expr AS data_type)
    /// Explicit type conversion
    pub(super) fn eval_cast(
        &self,
        expr: &vibesql_ast::Expression,
        data_type: &vibesql_types::DataType,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let value = self.eval(expr, row)?;
        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
        cast_value(&value, data_type, &sql_mode)
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
        // SQLite requires coalesce to have at least 2 arguments
        if args.len() < 2 {
            return Err(ExecutorError::WrongNumberOfArguments {
                function_name: "COALESCE".to_string(),
            });
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
        if ExpressionEvaluator::values_are_equal(&val1, &val2) {
            Ok(vibesql_types::SqlValue::Null)
        } else {
            Ok(val1)
        }
    }

    /// Evaluate function call
    /// Note: Aggregates (COUNT, SUM, etc.) are handled in SelectExecutor
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
            // Handle sqlite_search_count() - TCL test compatibility diagnostic
            // Returns the number of rows examined during query execution
            "SQLITE_SEARCH_COUNT" => {
                if !args.is_empty() {
                    return Err(ExecutorError::UnsupportedFeature(
                        "sqlite_search_count() takes no arguments".to_string(),
                    ));
                }
                if let Some(db) = self.database {
                    return Ok(vibesql_types::SqlValue::Bigint(db.search_count() as i64));
                } else {
                    return Ok(vibesql_types::SqlValue::Bigint(0));
                }
            }
            // Handle sqlite_search_count_reset() - Reset search count to 0
            // Returns 0 and resets the counter
            "SQLITE_SEARCH_COUNT_RESET" => {
                if !args.is_empty() {
                    return Err(ExecutorError::UnsupportedFeature(
                        "sqlite_search_count_reset() takes no arguments".to_string(),
                    ));
                }
                if let Some(db) = self.database {
                    db.reset_search_count();
                }
                return Ok(vibesql_types::SqlValue::Bigint(0));
            }
            _ => {}
        }

        // Check for wildcard expressions in function arguments
        // Scalar functions don't accept wildcards (only aggregate functions like COUNT(*) do)
        // SQLite returns "wrong number of arguments to function X()" for this case
        for arg in args {
            if matches!(arg, vibesql_ast::Expression::Wildcard) {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: name.to_uppercase(),
                });
            }
        }

        // Evaluate all arguments for standard functions
        let mut arg_values = Vec::new();
        for arg in args {
            arg_values.push(self.eval(arg, row)?);
        }

        // Call shared scalar function evaluator
        let sql_mode = self.database.map(|db| db.sql_mode()).unwrap_or_default();
        eval_scalar_function(name, &arg_values, character_unit, &sql_mode)
    }

    /// Evaluate unary operation
    pub(super) fn eval_unary(
        &self,
        op: &vibesql_ast::UnaryOperator,
        expr: &vibesql_ast::Expression,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        let val = self.eval(expr, row)?;
        super::super::expressions::operators::eval_unary_op(op, &val)
    }
}
