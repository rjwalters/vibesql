//! Window function collection logic

use std::collections::HashMap;

use vibesql_ast::{Expression, SelectItem, WindowDefinition, WindowFunctionSpec, WindowSpec};

use super::types::WindowFunctionInfo;
use crate::errors::ExecutorError;

/// Build a map of window names to their RESOLVED definitions
/// This handles window inheritance (e.g., WINDOW win AS (...), win2 AS (win ORDER BY x))
fn build_window_map(
    window_definitions: Option<&Vec<WindowDefinition>>,
) -> Result<HashMap<String, WindowSpec>, ExecutorError> {
    let mut resolved_map: HashMap<String, WindowSpec> = HashMap::new();

    if let Some(defs) = window_definitions {
        // First, build a raw map for lookups during resolution
        let raw_map: HashMap<String, &WindowSpec> = defs
            .iter()
            .map(|def| (def.name.to_lowercase(), &def.spec))
            .collect();

        // Then resolve each definition, potentially recursively
        for def in defs {
            let resolved = resolve_window_spec_recursive(&def.spec, &raw_map, &resolved_map)?;
            resolved_map.insert(def.name.to_lowercase(), resolved);
        }
    }

    Ok(resolved_map)
}

/// Recursively resolve a window spec, handling inheritance chains
fn resolve_window_spec_recursive(
    spec: &WindowSpec,
    raw_map: &HashMap<String, &WindowSpec>,
    resolved_map: &HashMap<String, WindowSpec>,
) -> Result<WindowSpec, ExecutorError> {
    match &spec.base_window_name {
        Some(base_name) => {
            let base_name_lower = base_name.to_lowercase();

            // First check if the base is already resolved
            let base_spec = if let Some(resolved) = resolved_map.get(&base_name_lower) {
                resolved.clone()
            } else if let Some(raw) = raw_map.get(&base_name_lower) {
                // Need to resolve the base first (handles chains like win3 -> win2 -> win)
                resolve_window_spec_recursive(raw, raw_map, resolved_map)?
            } else {
                return Err(ExecutorError::Other(format!("no such window: {}", base_name)));
            };

            // Merge: inherit from base, override with any values from current spec
            Ok(WindowSpec {
                base_window_name: None, // Resolved
                partition_by: spec.partition_by.clone().or(base_spec.partition_by),
                order_by: spec.order_by.clone().or(base_spec.order_by),
                frame: spec.frame.clone().or(base_spec.frame),
            })
        }
        None => Ok(spec.clone()),
    }
}

/// Resolve a window spec by looking up a named window from the resolved map
fn resolve_window_spec(
    spec: &WindowSpec,
    window_map: &HashMap<String, WindowSpec>,
) -> Result<WindowSpec, ExecutorError> {
    match &spec.base_window_name {
        Some(base_name) => {
            // Look up the already-resolved base window definition
            let base_spec = window_map.get(&base_name.to_lowercase()).ok_or_else(|| {
                ExecutorError::Other(format!("no such window: {}", base_name))
            })?;

            // Merge: inherit from base, override with any values from current spec
            Ok(WindowSpec {
                base_window_name: None, // Resolved, no longer needed
                partition_by: spec.partition_by.clone().or_else(|| base_spec.partition_by.clone()),
                order_by: spec.order_by.clone().or_else(|| base_spec.order_by.clone()),
                frame: spec.frame.clone().or_else(|| base_spec.frame.clone()),
            })
        }
        None => Ok(spec.clone()),
    }
}

/// Collect all window functions from SELECT list, resolving window name references
pub(super) fn collect_window_functions(
    select_list: &[SelectItem],
    window_definitions: Option<&Vec<WindowDefinition>>,
) -> Result<Vec<WindowFunctionInfo>, ExecutorError> {
    let window_map = build_window_map(window_definitions)?;
    let mut window_functions = Vec::new();

    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression { expr, .. } = item {
            collect_from_expression(expr, idx, &window_map, &mut window_functions)?;
        }
    }

    Ok(window_functions)
}

/// Recursively collect window functions from an expression
fn collect_from_expression(
    expr: &Expression,
    select_index: usize,
    window_map: &HashMap<String, WindowSpec>,
    window_functions: &mut Vec<WindowFunctionInfo>,
) -> Result<(), ExecutorError> {
    match expr {
        Expression::WindowFunction { function, over } => {
            // Handle all window function types: Aggregate, Ranking, and Value
            match function {
                WindowFunctionSpec::Aggregate { .. }
                | WindowFunctionSpec::Ranking { .. }
                | WindowFunctionSpec::Value { .. } => {
                    // Resolve window name reference if present
                    let resolved_spec = resolve_window_spec(over, window_map)?;
                    window_functions.push(WindowFunctionInfo {
                        _select_index: select_index,
                        function_spec: function.clone(),
                        window_spec: resolved_spec,
                        original_window_spec: over.clone(), // Keep original for key generation
                    });
                }
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_from_expression(left, select_index, window_map, window_functions)?;
            collect_from_expression(right, select_index, window_map, window_functions)?;
        }
        Expression::UnaryOp { expr, .. } => {
            collect_from_expression(expr, select_index, window_map, window_functions)?;
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_from_expression(arg, select_index, window_map, window_functions)?;
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_from_expression(op, select_index, window_map, window_functions)?;
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    collect_from_expression(cond, select_index, window_map, window_functions)?;
                }
                collect_from_expression(&when_clause.result, select_index, window_map, window_functions)?;
            }
            if let Some(else_expr) = else_result {
                collect_from_expression(else_expr, select_index, window_map, window_functions)?;
            }
        }
        Expression::IsNull { expr, .. } => {
            collect_from_expression(expr, select_index, window_map, window_functions)?;
        }
        _ => {}
    }
    Ok(())
}
