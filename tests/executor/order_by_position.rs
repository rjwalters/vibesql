use vibesql_parser::Parser;

#[test]
fn test_parse_order_by_position() {
    let sql = "SELECT x FROM t ORDER BY 1";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            println!("SELECT list: {:?}", select.select_list);
            if let Some(order_by) = select.order_by {
                for item in &order_by {
                    println!("ORDER BY expr: {:?}", item.expr);
                }
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_order_by_expression() {
    let sql = "SELECT CASE WHEN c > 150 THEN a*2 ELSE b*10 END FROM t ORDER BY 1";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            println!("SELECT list: {:?}", select.select_list);
            if let Some(order_by) = select.order_by {
                for item in &order_by {
                    println!("ORDER BY expr: {:?}", item.expr);
                }
            }
        }
        _ => panic!("Expected SELECT"),
    }
}
