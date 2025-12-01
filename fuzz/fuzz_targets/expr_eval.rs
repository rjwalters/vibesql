#![no_main]

use libfuzzer_sys::fuzz_target;
use vibesql_parser::Parser;

fuzz_target!(|data: &[u8]| {
    // Convert to UTF-8 string
    if let Ok(input) = std::str::from_utf8(data) {
        // Wrap input in SELECT to parse as expression
        // This exercises expression parsing paths
        let sql = format!("SELECT {}", input);

        // Parser should never panic on any input
        let _ = Parser::parse_sql(&sql);

        // Also test expression parsing in different contexts
        let sql_where = format!("SELECT 1 WHERE {}", input);
        let _ = Parser::parse_sql(&sql_where);

        let sql_case = format!("SELECT CASE WHEN {} THEN 1 ELSE 0 END", input);
        let _ = Parser::parse_sql(&sql_case);

        // Test in function argument context
        let sql_func = format!("SELECT COALESCE({})", input);
        let _ = Parser::parse_sql(&sql_func);

        // Test in GROUP BY context
        let sql_group = format!("SELECT 1 FROM t GROUP BY {}", input);
        let _ = Parser::parse_sql(&sql_group);

        // Test in ORDER BY context
        let sql_order = format!("SELECT 1 FROM t ORDER BY {}", input);
        let _ = Parser::parse_sql(&sql_order);
    }
});
