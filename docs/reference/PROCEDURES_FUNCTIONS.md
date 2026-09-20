# Stored Procedures and Functions

VibeSQL supports stored procedures and user-defined functions per SQL:1999 Feature P001. This document provides a comprehensive guide to creating, using, and understanding procedural capabilities in VibeSQL.

## Table of Contents

- [Overview](#overview)
- [Stored Procedures](#stored-procedures)
  - [Creating Procedures](#creating-procedures)
  - [Parameters](#parameters)
  - [Calling Procedures](#calling-procedures)
- [User-Defined Functions](#user-defined-functions)
  - [Creating Functions](#creating-functions)
  - [Function Characteristics](#function-characteristics)
  - [Using Functions](#using-functions)
- [Procedural Statements](#procedural-statements)
  - [Variables](#variables)
  - [Control Flow](#control-flow)
  - [Labels](#labels)
- [Differences from MySQL and PostgreSQL](#differences-from-mysql-and-postgresql)
- [Limitations](#limitations)
- [Implementation Architecture](#implementation-architecture)

## Overview

VibeSQL provides two types of stored procedural objects:

- **Stored Procedures**: Invoked with CALL, can modify database state, support OUT/INOUT parameters
- **User-Defined Functions**: Return values for use in expressions, read-only, support only IN parameters

Both support:
- Local variables (DECLARE, SET)
- Control flow (IF, WHILE, LOOP, REPEAT)
- Labels for LEAVE/ITERATE
- SQL statements within the body

## Stored Procedures

### Creating Procedures

Basic syntax:

```sql
CREATE PROCEDURE procedure_name(parameters)
  [characteristics]
BEGIN
  statements
END;
```

Example:

```sql
CREATE PROCEDURE greet(IN name VARCHAR(50))
BEGIN
  SELECT CONCAT('Hello, ', name);
END;
```

### Parameters

Procedures support three parameter modes:

#### IN Parameters

Input-only parameters (read by procedure):

```sql
CREATE PROCEDURE display_message(IN msg VARCHAR(100))
BEGIN
  SELECT msg AS message;
END;

CALL display_message('Hello World');
```

#### OUT Parameters

Output-only parameters (written by procedure to session variables):

```sql
CREATE PROCEDURE get_count(OUT total INT)
BEGIN
  SELECT COUNT(*) INTO total FROM users;
END;

CALL get_count(@user_count);
SELECT @user_count;
```

#### INOUT Parameters

Both input and output (read and written):

```sql
CREATE PROCEDURE double_value(INOUT val INT)
BEGIN
  SET val = val * 2;
END;

SET @num = 5;
CALL double_value(@num);
SELECT @num;  -- Output: 10
```

### Calling Procedures

Use the CALL statement:

```sql
CALL procedure_name(arg1, arg2, ...);
```

Complete example with mixed parameter modes:

```sql
CREATE PROCEDURE calculate_stats(
  IN min_value INT,
  OUT sum_result INT,
  OUT count_result INT,
  INOUT message VARCHAR(200)
)
BEGIN
  DECLARE temp_sum INT;
  DECLARE temp_count INT;

  SELECT SUM(value), COUNT(*)
  INTO temp_sum, temp_count
  FROM data_table
  WHERE value > min_value;

  SET sum_result = temp_sum;
  SET count_result = temp_count;
  SET message = CONCAT(message, ' - Processed ', temp_count, ' rows');
END;

SET @msg = 'Statistics';
CALL calculate_stats(10, @sum, @count, @msg);
SELECT @sum, @count, @msg;
```

## User-Defined Functions

> **⚠️ UNIMPLEMENTED — DDL only.** `CREATE FUNCTION` and `DROP FUNCTION` statements
> parse, validate, and register/deregister the function in the catalog, but
> **user-defined functions cannot be executed**. Any reference to a user-defined
> function in an expression (e.g. `SELECT my_func(...)`) returns an
> `UnsupportedFeature` error, and `BEGIN...END` function bodies are never run.
> See [#6672](https://github.com/rjwalters/vibesql/issues/6672) for the tracking
> history. The syntax below is documented for the *DDL surface that is accepted*;
> none of it is executable today.

### Creating Functions

Functions differ from procedures:
- Functions RETURN a value and are *intended* for use in SELECT expressions (not yet implemented — see banner)
- Functions are designed to be read-only (cannot INSERT, UPDATE, DELETE)
- Functions only support IN parameters

#### Simple Function Syntax

For simple return expressions (DDL accepted; body not executed):

```sql
CREATE FUNCTION add_ten(x INT) RETURNS INT
  DETERMINISTIC
  RETURN x + 10;
```

#### Complex Function Syntax

For functions with control flow (DDL accepted; body not executed):

```sql
CREATE FUNCTION factorial(n INT) RETURNS INT
  DETERMINISTIC
  COMMENT 'Calculate factorial of n'
BEGIN
  DECLARE result INT DEFAULT 1;
  DECLARE i INT DEFAULT 2;

  WHILE i <= n DO
    SET result = result * i;
    SET i = i + 1;
  END WHILE;

  RETURN result;
END;
```

### Function Characteristics

Functions support several optional characteristics (Phase 6):

#### DETERMINISTIC

Indicates the function always returns the same result for the same inputs:

```sql
CREATE FUNCTION tax_rate() RETURNS DECIMAL(5,2)
  DETERMINISTIC
  RETURN 0.08;
```

Non-deterministic functions (default) might return different values:

```sql
CREATE FUNCTION current_timestamp_str() RETURNS VARCHAR(50)
  RETURN CAST(CURRENT_TIMESTAMP AS VARCHAR(50));
```

#### SQL SECURITY

Controls privilege context (requires privilege system implementation):

```sql
-- Execute with definer's privileges (default)
CREATE FUNCTION secure_data() RETURNS INT
  SQL SECURITY DEFINER
  RETURN (SELECT COUNT(*) FROM sensitive_table);

-- Execute with caller's privileges
CREATE FUNCTION user_data() RETURNS INT
  SQL SECURITY INVOKER
  RETURN (SELECT COUNT(*) FROM user_table);
```

#### COMMENT

Documentation string for the function:

```sql
CREATE FUNCTION calculate_discount(price DECIMAL(10,2)) RETURNS DECIMAL(10,2)
  DETERMINISTIC
  COMMENT 'Apply 10% discount to price'
  RETURN price * 0.9;
```

#### LANGUAGE

Specifies the implementation language (only SQL supported):

```sql
CREATE FUNCTION double_value(x INT) RETURNS INT
  DETERMINISTIC
  LANGUAGE SQL
  RETURN x * 2;
```

### Calling Functions

**Not implemented.** Referencing a user-defined function from an expression —
SELECT lists, WHERE clauses, nested calls — always fails with an
`UnsupportedFeature` error ("User-defined function '...' found but cannot be
executed in this context"). There is no runtime fallback. Until execution is
implemented, use built-in functions, or move the logic into a stored procedure
invoked via `CALL` (procedures are executed and do support `BEGIN...END` bodies).

## Procedural Statements

### Variables

#### DECLARE

Declare local variables with optional default values:

```sql
CREATE PROCEDURE variable_demo()
BEGIN
  DECLARE counter INT DEFAULT 0;
  DECLARE message VARCHAR(100);  -- Default is NULL
  DECLARE total DECIMAL(10,2) DEFAULT 0.0;

  SET message = 'Processing...';
  -- Use variables in logic
END;
```

Variables are:
- Local to the procedure/function
- Scoped to the BEGIN...END block
- Case-insensitive for names

#### SET

Assign values to variables, parameters, or session variables:

```sql
CREATE PROCEDURE set_demo(INOUT value INT)
BEGIN
  DECLARE local_var INT;

  -- Set local variable
  SET local_var = 10;

  -- Set parameter
  SET value = value + local_var;

  -- Set session variable
  SET @global_var = 100;
END;
```

### Control Flow

#### IF Statement

Conditional execution:

```sql
CREATE FUNCTION grade(score INT) RETURNS VARCHAR(1)
BEGIN
  DECLARE result VARCHAR(1);

  IF score >= 90 THEN
    SET result = 'A';
  ELSEIF score >= 80 THEN
    SET result = 'B';
  ELSEIF score >= 70 THEN
    SET result = 'C';
  ELSE
    SET result = 'F';
  END IF;

  RETURN result;
END;
```

#### WHILE Loop

Loop while condition is true:

```sql
CREATE FUNCTION sum_to_n(n INT) RETURNS INT
BEGIN
  DECLARE total INT DEFAULT 0;
  DECLARE i INT DEFAULT 1;

  WHILE i <= n DO
    SET total = total + i;
    SET i = i + 1;
  END WHILE;

  RETURN total;
END;
```

#### LOOP Statement

Infinite loop (must use LEAVE to exit):

```sql
CREATE FUNCTION find_first_multiple(n INT, divisor INT) RETURNS INT
BEGIN
  DECLARE candidate INT DEFAULT n;

  search_loop: LOOP
    IF candidate MOD divisor = 0 THEN
      LEAVE search_loop;
    END IF;

    SET candidate = candidate + 1;

    IF candidate > 10000 THEN
      LEAVE search_loop;  -- Safety limit
    END IF;
  END LOOP search_loop;

  RETURN candidate;
END;
```

#### REPEAT Loop

Loop until condition is true (always executes at least once):

```sql
CREATE FUNCTION sum_until_limit(limit_val INT) RETURNS INT
BEGIN
  DECLARE total INT DEFAULT 0;
  DECLARE i INT DEFAULT 1;

  REPEAT
    SET total = total + i;
    SET i = i + 1;
  UNTIL total >= limit_val
  END REPEAT;

  RETURN total;
END;
```

### Labels

Labels allow named control flow with LEAVE and ITERATE:

#### LEAVE

Exit a labeled block or loop:

```sql
CREATE FUNCTION search_value(target INT) RETURNS BOOLEAN
BEGIN
  DECLARE i INT DEFAULT 1;
  DECLARE found BOOLEAN DEFAULT FALSE;

  search_loop: WHILE i <= 100 DO
    IF i = target THEN
      SET found = TRUE;
      LEAVE search_loop;  -- Exit loop early
    END IF;
    SET i = i + 1;
  END WHILE search_loop;

  RETURN found;
END;
```

#### ITERATE

Continue to next iteration of labeled loop:

```sql
CREATE FUNCTION sum_even_numbers(max_val INT) RETURNS INT
BEGIN
  DECLARE total INT DEFAULT 0;
  DECLARE i INT DEFAULT 1;

  sum_loop: WHILE i <= max_val DO
    IF i MOD 2 = 1 THEN
      SET i = i + 1;
      ITERATE sum_loop;  -- Skip odd numbers
    END IF;

    SET total = total + i;
    SET i = i + 1;
  END WHILE sum_loop;

  RETURN total;
END;
```

## Differences from MySQL and PostgreSQL

### MySQL Differences

| Feature | MySQL | VibeSQL |
|---------|-------|---------|
| Delimiter | Requires DELIMITER change | Not required |
| OUT params | Can be local variables | Must be session variables (@var) |
| Error handlers | DECLARE HANDLER | Not yet supported |
| Cursors | Full support | Not yet supported |
| BEGIN labels | Supported | Supported for loops only |

### PostgreSQL Differences

| Feature | PostgreSQL | VibeSQL |
|---------|------------|---------|
| Language | PL/pgSQL, others | SQL only |
| Assignment | := or = | SET statement only |
| RETURN | Can return sets | Returns single value |
| Exceptions | EXCEPTION block | Not yet supported |
| Package support | Yes (schemas) | No |

## Limitations

Current implementation has these limitations:

1. **Cursors**: Not yet supported
   ```sql
   -- NOT SUPPORTED YET:
   DECLARE cursor_name CURSOR FOR SELECT ...;
   OPEN cursor_name;
   FETCH cursor_name INTO ...;
   ```

2. **Exception Handlers**: Not yet supported
   ```sql
   -- NOT SUPPORTED YET:
   DECLARE EXIT HANDLER FOR SQLEXCEPTION
   BEGIN
     -- error handling
   END;
   ```

3. **External Languages**: Only SQL language supported
   - No PL/pgSQL
   - No Python/JavaScript UDFs

4. **Privilege Enforcement**: SQL SECURITY requires full privilege system
   - DEFINER/INVOKER are parsed but not enforced

5. **Recursion Limit**: Maximum depth is 100 levels
   ```sql
   -- Will fail if recursion exceeds 100 levels
   CREATE FUNCTION recursive_func(n INT) RETURNS INT
   BEGIN
     IF n <= 0 THEN RETURN 0; END IF;
     RETURN n + recursive_func(n - 1);
   END;
   ```

6. **OUT Parameter Restrictions**: Must be session variables
   ```sql
   -- WRONG: Cannot use expressions or literals
   CALL my_proc(5, 10, literal_value);  -- Error

   -- CORRECT: Use session variables
   CALL my_proc(5, @out1, @out2);
   ```

## Implementation Architecture

For contributors implementing procedural features:

### Module Organization

```
crates/vibesql-executor/src/procedural/
├── mod.rs           - Module documentation and exports
├── context.rs       - ExecutionContext, variable/parameter management
├── executor.rs      - Statement execution (DECLARE, SET, RETURN)
├── control_flow.rs  - Control flow execution (IF, WHILE, LOOP, REPEAT)
└── function.rs      - User-defined function execution
```

### Execution Flow

1. **Parse**: AST creation (vibesql-parser)
2. **Catalog**: Store procedure/function definition (vibesql-catalog)
3. **Execute**: Runtime execution (vibesql-executor)
   - Create ExecutionContext
   - Bind parameters
   - Execute statements sequentially
   - Handle control flow (ControlFlow enum)
   - Return results

### Key Components

**ExecutionContext** (`context.rs`):
- Manages local variables and parameters
- Tracks labels for LEAVE/ITERATE
- Enforces recursion depth limits
- Isolates function execution (read-only)

**ControlFlow** (`context.rs`):
- `Continue`: Proceed to next statement
- `Return(SqlValue)`: Exit with return value
- `Leave(String)`: Exit labeled block
- `Iterate(String)`: Continue labeled loop

**Execution** (`executor.rs`):
- `execute_procedural_statement()`: Dispatch statement execution
- `evaluate_expression()`: Evaluate expressions with variable access
- Type checking and casting for assignments

### Adding New Features

To add a new procedural statement:

1. Add AST node to `vibesql-ast/src/lib.rs`
2. Add parser support to `vibesql-parser`
3. Add execution logic to `procedural/executor.rs`
4. Add control flow handling to `procedural/control_flow.rs` (if applicable)
5. Add tests to `tests/procedure_tests.rs`
6. Update documentation

Example pattern:
```rust
ProceduralStatement::NewFeature { args } => {
    // 1. Validate arguments
    // 2. Execute logic
    // 3. Return ControlFlow result
    Ok(ControlFlow::Continue)
}
```

### Testing Guidelines

1. **Unit tests**: Test individual statement execution
   - File: `crates/vibesql-executor/src/tests/procedure_tests.rs`
   - Test parameter binding, variable scope, control flow

2. **Integration tests**: Test complete procedures/functions
   - Create procedure/function
   - Execute CALL or SELECT
   - Verify results

3. **Error cases**: Test error handling
   - Wrong parameter count
   - Type mismatches
   - Undefined variables
   - Recursion limits

Example test structure:
```rust
#[test]
fn test_procedure_feature() {
    let mut db = Database::new();

    // Setup
    let create_proc = CreateProcedureStmt { ... };
    advanced_objects::execute_create_procedure(&create_proc, &mut db).unwrap();

    // Execute
    let call = CallStmt { ... };
    let result = advanced_objects::execute_call(&call, &mut db);

    // Verify
    assert!(result.is_ok());
    // Additional assertions
}
```

## See Also

- SQL:1999 Standard, Feature P001 (Stored Modules)
- Module documentation: `crates/vibesql-executor/src/procedural/mod.rs`
- CREATE PROCEDURE statement implementation: `crates/vibesql-executor/src/advanced_objects.rs`
- Execution context: `crates/vibesql-executor/src/procedural/context.rs`
- Tests: `crates/vibesql-executor/src/tests/procedure_tests.rs`
