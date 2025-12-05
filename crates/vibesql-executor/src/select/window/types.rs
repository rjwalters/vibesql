//! Window function type definitions

use vibesql_ast::WindowFunctionSpec;

/// Information about a window function in the SELECT list
#[derive(Debug, Clone)]
pub(super) struct WindowFunctionInfo {
    /// Index in the SELECT list
    pub(super) _select_index: usize,
    /// The window function specification
    pub(super) function_spec: WindowFunctionSpec,
    /// The OVER clause specification
    pub(super) window_spec: vibesql_ast::WindowSpec,
}

/// Key for identifying and hashing window function expressions
/// Used to map window function expressions to their pre-computed column indices
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowFunctionKey {
    /// Serialized representation of the window function for hashing
    /// Format: "FUNC_NAME(args)|PARTITION BY col1,col2|ORDER BY col3 ASC|FRAME_SPEC"
    pub key: String,
}

impl WindowFunctionKey {
    /// Create a key from a window function expression
    pub fn from_expression(
        function: &WindowFunctionSpec,
        window: &vibesql_ast::WindowSpec,
    ) -> Self {
        let mut key_parts = Vec::new();

        // Add function name and args
        match function {
            WindowFunctionSpec::Aggregate { name, args } => {
                let args_str =
                    args.iter().map(|expr| format!("{:?}", expr)).collect::<Vec<_>>().join(",");
                key_parts.push(format!("{}({})", name, args_str));
            }
            WindowFunctionSpec::Ranking { name, args } => {
                let args_str =
                    args.iter().map(|expr| format!("{:?}", expr)).collect::<Vec<_>>().join(",");
                key_parts.push(format!("{}({})", name, args_str));
            }
            WindowFunctionSpec::Value { name, args } => {
                let args_str =
                    args.iter().map(|expr| format!("{:?}", expr)).collect::<Vec<_>>().join(",");
                key_parts.push(format!("{}({})", name, args_str));
            }
        }

        // Add PARTITION BY clause
        if let Some(partition_by) = &window.partition_by {
            if !partition_by.is_empty() {
                let partition_str = partition_by
                    .iter()
                    .map(|expr| format!("{:?}", expr))
                    .collect::<Vec<_>>()
                    .join(",");
                key_parts.push(format!("PARTITION BY {}", partition_str));
            }
        }

        // Add ORDER BY clause
        if let Some(order_by) = &window.order_by {
            if !order_by.is_empty() {
                let order_str = order_by
                    .iter()
                    .map(|item| format!("{:?} {:?}", item.expr, item.direction))
                    .collect::<Vec<_>>()
                    .join(",");
                key_parts.push(format!("ORDER BY {}", order_str));
            }
        }

        // Add FRAME clause
        if let Some(frame) = &window.frame {
            key_parts.push(format!("{:?}", frame));
        }

        WindowFunctionKey { key: key_parts.join("|") }
    }
}
