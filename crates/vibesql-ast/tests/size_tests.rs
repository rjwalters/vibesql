//! Tests for measuring AST type sizes.

use std::mem::size_of;
use vibesql_ast::arena;
use vibesql_types::SqlValue;

#[test]
fn measure_expression_size() {
    let arena_expr_size = size_of::<arena::Expression<'static>>();
    let extended_expr_size = size_of::<arena::ExtendedExpr<'static>>();

    println!("\n=== Expression Size Measurement ===");
    println!("arena::Expression size: {} bytes", arena_expr_size);
    println!("arena::ExtendedExpr size: {} bytes", extended_expr_size);

    // Also measure supporting types
    println!("\n=== Supporting Type Sizes ===");
    println!("arena::WindowSpec size: {} bytes", size_of::<arena::WindowSpec<'static>>());
    println!("arena::WindowFrame size: {} bytes", size_of::<arena::WindowFrame<'static>>());
    println!("arena::WindowFunctionSpec size: {} bytes", size_of::<arena::WindowFunctionSpec<'static>>());
    println!("arena::CaseWhen size: {} bytes", size_of::<arena::CaseWhen<'static>>());

    // Show cache line context
    println!("\n=== Cache Line Analysis ===");
    println!("Cache line size: 64 bytes");
    println!("Expressions per cache line: {:.2}", 64.0 / arena_expr_size as f64);

    // Target: Expression should be <= 48 bytes
    assert!(
        arena_expr_size <= 48,
        "Expression size {} exceeds target of 48 bytes",
        arena_expr_size
    );
}

#[test]
fn measure_boxed_expression_size() {
    use vibesql_ast::Expression;
    let expr_size = size_of::<Expression>();
    println!("\n=== Boxed Expression Size ===");
    println!("Expression size: {} bytes", expr_size);
    println!("Expressions per cache line: {:.2}", 64.0 / expr_size as f64);
}

#[test]
fn measure_field_sizes() {
    use bumpalo::collections::Vec as BumpVec;
    use vibesql_types::DataType;

    println!("\n=== Field Size Analysis ===");
    println!("SqlValue: {} bytes", size_of::<SqlValue>());
    println!("BumpVec: {} bytes", size_of::<BumpVec<'static, arena::Expression<'static>>>());
    println!("&str: {} bytes", size_of::<&str>());
    println!("Option<&str>: {} bytes", size_of::<Option<&str>>());
    println!("&Expression: {} bytes", size_of::<&arena::Expression<'static>>());
    println!("DataType: {} bytes", size_of::<DataType>());
    println!("arena::IntervalUnit: {} bytes", size_of::<arena::IntervalUnit>());
    println!("vibesql_ast::BinaryOperator: {} bytes", size_of::<vibesql_ast::BinaryOperator>());

    // Check what's inside WindowFunction that makes it big
    println!("\n=== WindowFunction Breakdown ===");
    println!("WindowFunctionSpec: {} bytes", size_of::<arena::WindowFunctionSpec<'static>>());
    println!("WindowSpec: {} bytes", size_of::<arena::WindowSpec<'static>>());

    // WindowSpec fields
    println!("\n=== WindowSpec Field Breakdown ===");
    println!("Option<BumpVec<Expression>>: {} bytes", size_of::<Option<BumpVec<'static, arena::Expression<'static>>>>());
    println!("Option<BumpVec<OrderByItem>>: {} bytes", size_of::<Option<BumpVec<'static, arena::OrderByItem<'static>>>>());
    println!("Option<WindowFrame>: {} bytes", size_of::<Option<arena::WindowFrame<'static>>>());

    // Analyze biggest variants in Expression
    println!("\n=== Biggest Expression Variants ===");
    // WindowFunction: WindowFunctionSpec (56) + WindowSpec (104) = 160 (matches!)
    // Case: Option<&Expr> + BumpVec<CaseWhen> + Option<&Expr> = 8 + 32 + 8 = 48, but CaseWhen inline?
}
