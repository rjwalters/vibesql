#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use vibesql_parser::Parser;

#[derive(Arbitrary, Debug)]
struct TypeConvertInput {
    value: String,
    target_type: TargetType,
}

#[derive(Arbitrary, Debug)]
enum TargetType {
    Integer,
    Smallint,
    Bigint,
    Float,
    Real,
    Double,
    Varchar,
    Char,
    Boolean,
    Date,
    Time,
    Timestamp,
    Numeric,
}

impl TargetType {
    fn as_sql(&self) -> &'static str {
        match self {
            TargetType::Integer => "INTEGER",
            TargetType::Smallint => "SMALLINT",
            TargetType::Bigint => "BIGINT",
            TargetType::Float => "FLOAT",
            TargetType::Real => "REAL",
            TargetType::Double => "DOUBLE PRECISION",
            TargetType::Varchar => "VARCHAR(255)",
            TargetType::Char => "CHAR(10)",
            TargetType::Boolean => "BOOLEAN",
            TargetType::Date => "DATE",
            TargetType::Time => "TIME",
            TargetType::Timestamp => "TIMESTAMP",
            TargetType::Numeric => "NUMERIC(10,2)",
        }
    }
}

fuzz_target!(|input: TypeConvertInput| {
    // Escape single quotes in the value for SQL string literal
    let escaped_value = input.value.replace('\'', "''");

    // Test CAST with string literal
    let sql_str = format!("SELECT CAST('{}' AS {})", escaped_value, input.target_type.as_sql());
    let _ = Parser::parse_sql(&sql_str);

    // Test CAST with numeric-looking input (if parseable)
    if input.value.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E') {
        let sql_num = format!("SELECT CAST({} AS {})", input.value, input.target_type.as_sql());
        let _ = Parser::parse_sql(&sql_num);
    }

    // Test CAST with raw input value directly (may or may not be valid SQL)
    let sql_raw = format!("SELECT CAST({} AS {})", input.value, input.target_type.as_sql());
    let _ = Parser::parse_sql(&sql_raw);

    // Test nested CAST
    let sql_nested = format!(
        "SELECT CAST(CAST('{}' AS VARCHAR(100)) AS {})",
        escaped_value,
        input.target_type.as_sql()
    );
    let _ = Parser::parse_sql(&sql_nested);

    // Test CAST in expressions
    let sql_expr = format!(
        "SELECT CAST('{}' AS {}) + 1",
        escaped_value,
        input.target_type.as_sql()
    );
    let _ = Parser::parse_sql(&sql_expr);

    // Test implicit type conversion contexts
    let sql_compare = format!(
        "SELECT CASE WHEN CAST('{}' AS {}) = 0 THEN 'zero' ELSE 'nonzero' END",
        escaped_value,
        input.target_type.as_sql()
    );
    let _ = Parser::parse_sql(&sql_compare);
});
