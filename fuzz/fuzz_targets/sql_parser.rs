#![no_main]

use libfuzzer_sys::fuzz_target;
use vibesql_parser::Parser;

fuzz_target!(|data: &[u8]| {
    // Convert arbitrary bytes to UTF-8 string, replacing invalid sequences
    if let Ok(sql) = std::str::from_utf8(data) {
        // Parser should never panic, only return parse errors
        let _ = Parser::parse_sql(sql);
    }
});
