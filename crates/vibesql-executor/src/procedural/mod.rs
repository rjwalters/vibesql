//! Stored Procedures and User-Defined Functions
//!
//! This module implements SQL:1999 Feature P001 (stored procedures) and related
//! functionality for user-defined functions. It provides a complete procedural
//! execution environment with local variables, control flow, and parameter binding.
//!
//! # Overview
//!
//! Procedures and functions allow storing SQL logic in the database for reuse and
//! encapsulation. Procedures are invoked with CALL and can modify data, while
//! functions return values and are read-only (cannot modify database state).
//!
//! ## Key Features
//!
//! - **Parameter modes**: IN (input), OUT (output), INOUT (both)
//! - **Local variables**: DECLARE and SET for variable management
//! - **Control flow**: IF, WHILE, LOOP, REPEAT statements
//! - **Labels**: Named blocks for LEAVE and ITERATE
//! - **Recursion**: Function calls with depth limiting (max 100)
//! - **Read-only functions**: Functions cannot modify database tables
//!
//! # Examples
//!
//! ## Creating a Simple Procedure
//!
//! ```sql
//! CREATE PROCEDURE greet(IN name VARCHAR(50))
//! BEGIN
//!   SELECT CONCAT('Hello, ', name);
//! END;
//!
//! CALL greet('Alice');
//! -- Output: Hello, Alice
//! ```
//!
//! ## Using OUT Parameters
//!
//! ```sql
//! CREATE PROCEDURE calculate_stats(
//!   IN input_value INT,
//!   OUT sum_result INT,
//!   OUT count_result INT
//! )
//! BEGIN
//!   SELECT SUM(value), COUNT(*)
//!   INTO sum_result, count_result
//!   FROM data_table
//!   WHERE value > input_value;
//! END;
//!
//! CALL calculate_stats(10, @sum, @count);
//! SELECT @sum, @count;
//! ```
//!
//! ## Creating a Function
//!
//! ```sql
//! CREATE FUNCTION factorial(n INT) RETURNS INT
//!   DETERMINISTIC
//!   COMMENT 'Calculate factorial of n'
//! BEGIN
//!   DECLARE result INT DEFAULT 1;
//!   DECLARE i INT DEFAULT 2;
//!
//!   WHILE i <= n DO
//!     SET result = result * i;
//!     SET i = i + 1;
//!   END WHILE;
//!
//!   RETURN result;
//! END;
//!
//! SELECT factorial(5);
//! -- Output: 120
//! ```
//!
//! ## Control Flow with Labels
//!
//! ```sql
//! CREATE FUNCTION find_first(search_val INT) RETURNS INT
//! BEGIN
//!   DECLARE i INT DEFAULT 1;
//!   DECLARE found INT DEFAULT 0;
//!
//!   search_loop: WHILE i <= 100 DO
//!     IF i = search_val THEN
//!       SET found = 1;
//!       LEAVE search_loop;
//!     END IF;
//!     SET i = i + 1;
//!   END WHILE search_loop;
//!
//!   RETURN found;
//! END;
//! ```
//!
//! # Module Structure
//!
//! - [`context`]: Execution context with variables, parameters, and scope management
//! - [`executor`]: Execute individual procedural statements
//! - [`control_flow`]: Control flow execution (IF, WHILE, LOOP, REPEAT)
//! - [`function`]: User-defined function execution and recursion handling
//!
//! # Function Characteristics
//!
//! Functions support the following characteristics (from Phase 6):
//!
//! - **DETERMINISTIC**: Same input always produces same output
//! - **SQL SECURITY DEFINER**: Execute with definer's privileges (requires privilege system)
//! - **SQL SECURITY INVOKER**: Execute with caller's privileges (default)
//! - **COMMENT**: Documentation string for the function
//! - **LANGUAGE SQL**: Indicates SQL language (only SQL is currently supported)
//!
//! Example:
//! ```sql
//! CREATE FUNCTION add_ten(x INT) RETURNS INT
//!   DETERMINISTIC
//!   SQL SECURITY INVOKER
//!   COMMENT 'Adds 10 to the input value'
//!   LANGUAGE SQL
//!   RETURN x + 10;
//! ```
//!
//! # Limitations
//!
//! Current implementation has these limitations:
//!
//! - Cursors not yet supported
//! - Exception handlers not yet supported
//! - Only SQL language supported (no external languages like PLpgSQL)
//! - Privilege enforcement for SQL SECURITY requires full privilege system
//! - Maximum recursion depth is 100 levels
//!
//! # See Also
//!
//! - SQL:1999 Standard, Feature P001 (Stored Modules)
//! - [`crate::advanced_objects`]: CREATE/DROP PROCEDURE and FUNCTION statements
//! - [`vibesql_catalog::Procedure`]: Procedure catalog storage
//! - [`vibesql_catalog::Function`]: Function catalog storage

pub mod context;
pub mod control_flow;
pub mod executor;
pub mod function;

pub use context::{ControlFlow, ExecutionContext};
pub use executor::execute_procedural_statement;
pub use function::execute_user_function;
