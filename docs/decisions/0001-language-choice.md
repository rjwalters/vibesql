# ADR-0001: Choose Rust as Implementation Language

**Status**: Accepted

**Date**: 2025-10-25

**Deciders**: Claude Code + rwalters

**Related**:
- [REQUIREMENTS.md](../archive/REQUIREMENTS.md) - No language preference from upstream
- [MAJOR_SIMPLIFICATIONS.md](../archive/MAJOR_SIMPLIFICATIONS.md) - Correctness over performance

## Context and Problem Statement

We need to choose a programming language for implementing a FULL SQL:1999 compliant in-memory database. The language must support:

1. **Complex type system** - SQL has many data types including user-defined types, arrays, and nested structures
2. **AST representation** - SQL parsing produces complex Abstract Syntax Trees
3. **Correctness focus** - With no performance requirements, correctness is the #1 priority
4. **Pattern matching** - SQL execution involves extensive pattern matching on statement/expression types
5. **Error handling** - SQL operations have many error conditions that must be handled gracefully

**Constraints**:
- Upstream has no language preference
- Performance is NOT a requirement (single-threaded is acceptable)
- No persistence needed (purely in-memory)
- Must support ODBC and JDBC driver implementation

**Question**: Which language best balances correctness, expressiveness, type safety, and developer experience for this project?

## Decision Drivers

* **Correctness** - Catch bugs at compile time rather than runtime
* **Type safety** - Strong type system to represent SQL types accurately
* **Pattern matching** - Clean syntax for handling AST nodes and SQL types
* **Memory safety** - Prevent entire classes of bugs without garbage collection overhead
* **Tooling** - Excellent compiler feedback, testing, and development tools
* **Ecosystem** - Libraries for parsing, ODBC/JDBC, data structures
* **Maintainability** - Clear, readable code for complex logic
* **Learning value** - Educational benefit for understanding database internals
* **AI assistance compatibility** - Clear error messages that Claude Code can help debug

## Considered Options

### Option 1: Rust

**Description**: Systems programming language with strong type safety, ownership system, and zero-cost abstractions.

**Pros**:
* ✅ **Exceptional type system** - Algebraic data types (enums) perfect for SQL types and AST
* ✅ **Pattern matching** - Exhaustive matching catches missing cases at compile time
* ✅ **Memory safety** - Ownership system prevents use-after-free, double-free, null pointers
* ✅ **No GC** - Predictable memory behavior, no pauses
* ✅ **Excellent compiler** - rustc provides incredibly helpful error messages and suggestions
* ✅ **Great tooling** - cargo (build), clippy (lints), rustfmt (formatting), rust-analyzer (IDE)
* ✅ **Strong ecosystem** - pest/lalrpop/nom (parsers), odbc-rs (ODBC), serde (serialization)
* ✅ **Result<T, E>** - Built-in error handling perfect for SQL operations
* ✅ **No null** - Option<T> forces explicit handling of missing values
* ✅ **Refactoring safety** - Type system makes large refactors safe

**Cons**:
* ❌ **Learning curve** - Borrow checker can be challenging initially
* ❌ **Compile times** - Slower compilation than interpreted languages (but fast iteration with cargo check)
* ❌ **Verbosity** - More explicit than Python/Ruby
* ❌ **JDBC complexity** - Requires JNI bindings (no native Java interop)

**Code Example**:
```rust
// SQL types map beautifully to Rust enums
enum DataType {
    Integer,
    Varchar(usize),
    Boolean,
    Numeric { precision: u8, scale: u8 },
    Array(Box<DataType>),
    UserDefined(String),
}

// AST with pattern matching
enum Expression {
    Column(String),
    Literal(SqlValue),
    BinaryOp {
        op: Operator,
        left: Box<Expression>,
        right: Box<Expression>
    },
}

fn evaluate(&self, expr: &Expression) -> Result<SqlValue, EvalError> {
    match expr {
        Expression::Column(name) => self.get_column(name),
        Expression::Literal(val) => Ok(val.clone()),
        Expression::BinaryOp { op, left, right } => {
            let l = self.evaluate(left)?;
            let r = self.evaluate(right)?;
            self.apply_operator(*op, l, r)
        }
        // Compiler errors if we miss a case!
    }
}
```

**Estimated LOC**: 40,000-60,000 (mid-range verbosity)

---

### Option 2: Go

**Description**: Simple, efficient language with good concurrency primitives and fast compilation.

**Pros**:
* ✅ **Simple syntax** - Easy to learn and read
* ✅ **Fast compilation** - Quicker than Rust
* ✅ **Good stdlib** - Excellent standard library
* ✅ **Decent tooling** - go fmt, go test, go mod
* ✅ **CGO** - Can call C code for ODBC

**Cons**:
* ❌ **No algebraic data types** - Would use interfaces + type switches (verbose, less safe)
* ❌ **No pattern matching** - Lots of if-else and type switches
* ❌ **Weak type system** - No sum types, generics limited
* ❌ **Error handling** - `if err != nil` everywhere, verbose
* ❌ **Null pointers** - Can still get nil pointer panics
* ❌ **GC** - Garbage collection (though pauses don't matter for us)

**Code Example**:
```go
// Weaker typing - need type switches instead of pattern matching
type Expression interface {
    isExpression()
}

type ColumnExpr struct { Name string }
type LiteralExpr struct { Value SqlValue }
type BinaryOpExpr struct { Op Operator; Left, Right Expression }

func (e *Evaluator) Evaluate(expr Expression) (SqlValue, error) {
    switch e := expr.(type) {
    case *ColumnExpr:
        return e.getColumn(e.Name)
    case *LiteralExpr:
        return e.Value, nil
    case *BinaryOpExpr:
        // No exhaustiveness checking - could miss cases!
        l, err := e.Evaluate(e.Left)
        if err != nil { return nil, err }
        r, err := e.Evaluate(e.Right)
        if err != nil { return nil, err }
        return e.applyOperator(e.Op, l, r)
    default:
        return nil, errors.New("unknown expression type")
    }
}
```

**Estimated LOC**: 35,000-55,000 (simpler syntax but more error handling)

---

### Option 3: Java

**Description**: Mature, object-oriented language with excellent ecosystem and tooling.

**Pros**:
* ✅ **JDBC native** - Java JDBC implementation is straightforward
* ✅ **Mature ecosystem** - Extensive libraries and tools
* ✅ **Good tooling** - IntelliJ, Eclipse, Maven, Gradle
* ✅ **Modern features** - Records (Java 14+), pattern matching (Java 17+)
* ✅ **Strong typing** - Static type system

**Cons**:
* ❌ **Very verbose** - Lots of boilerplate (though records help)
* ❌ **Limited pattern matching** - Even Java 17+ is less powerful than Rust/OCaml
* ❌ **Algebraic data types awkward** - Would use visitor pattern or instanceof chains
* ❌ **Null pointers** - NullPointerException still possible
* ❌ **GC overhead** - Garbage collection pauses
* ❌ **More ceremony** - Classes, interfaces, annotations

**Code Example**:
```java
// Awkward without true algebraic data types
sealed interface Expression permits ColumnExpr, LiteralExpr, BinaryOpExpr {}

record ColumnExpr(String name) implements Expression {}
record LiteralExpr(SqlValue value) implements Expression {}
record BinaryOpExpr(Operator op, Expression left, Expression right) implements Expression {}

public SqlValue evaluate(Expression expr) throws EvalError {
    // Pattern matching (Java 17+) is better but still verbose
    return switch (expr) {
        case ColumnExpr(String name) -> getColumn(name);
        case LiteralExpr(SqlValue val) -> val;
        case BinaryOpExpr(Operator op, Expression left, Expression right) -> {
            SqlValue l = evaluate(left);
            SqlValue r = evaluate(right);
            yield applyOperator(op, l, r);
        }
    };
}
```

**Estimated LOC**: 60,000-80,000 (most verbose)

---

### Option 4: Python

**Description**: Dynamic, high-level language with excellent expressiveness and rapid development.

**Pros**:
* ✅ **Rapid development** - Fastest to write code
* ✅ **Very expressive** - Concise syntax
* ✅ **Pattern matching** - Python 3.10+ has match/case
* ✅ **Great for prototyping** - Quick iteration
* ✅ **Excellent libraries** - pyparsing, lark for parsing

**Cons**:
* ❌ **No compile-time type checking** - Bugs found at runtime
* ❌ **Weak types** - Even with type hints, not enforced
* ❌ **Performance** - Slowest (though doesn't matter here)
* ❌ **Runtime errors** - Missing cases only found during execution
* ❌ **ODBC complexity** - pyodbc has quirks
* ❌ **Refactoring risk** - No safety net when changing types

**Code Example**:
```python
# Clean but no compile-time safety
from dataclasses import dataclass
from typing import Union

@dataclass
class ColumnExpr:
    name: str

@dataclass
class LiteralExpr:
    value: SqlValue

@dataclass
class BinaryOpExpr:
    op: Operator
    left: 'Expression'
    right: 'Expression'

Expression = Union[ColumnExpr, LiteralExpr, BinaryOpExpr]

def evaluate(self, expr: Expression) -> SqlValue:
    match expr:
        case ColumnExpr(name):
            return self.get_column(name)
        case LiteralExpr(value):
            return value
        case BinaryOpExpr(op, left, right):
            l = self.evaluate(left)
            r = self.evaluate(right)
            return self.apply_operator(op, l, r)
        # No exhaustiveness check - could miss cases!
```

**Estimated LOC**: 30,000-50,000 (most concise)

---

### Option 5: OCaml / F#

**Description**: Functional languages with excellent type systems and pattern matching.

**Pros**:
* ✅ **Excellent type system** - Best-in-class algebraic data types
* ✅ **Pattern matching** - Most powerful of all options
* ✅ **Fast** - Compiled, efficient
* ✅ **Immutability** - Functional style reduces bugs

**Cons**:
* ❌ **Smaller ecosystem** - Fewer libraries than Rust/Java/Python
* ❌ **Less tooling** - Not as mature as mainstream languages
* ❌ **Fewer examples** - Less community content
* ❌ **Learning curve** - Functional programming paradigm
* ❌ **ODBC/JDBC** - Limited library support
* ❌ **Harder to get help** - Smaller community

**Estimated LOC**: 35,000-55,000

---

## Decision Outcome

**Chosen option**: **Rust**

**Rationale**:

Rust provides the best combination of correctness guarantees, type safety, and developer experience for this project:

1. **Correctness is #1 priority** (from MAJOR_SIMPLIFICATIONS.md), and Rust's type system + compiler catches bugs at compile time rather than runtime

2. **SQL type system maps perfectly** to Rust's algebraic data types (enums with data)

3. **Pattern matching with exhaustiveness checking** ensures we handle all SQL statement types, expression types, and data types without missing cases

4. **Memory safety without GC** means we get predictable behavior without worrying about use-after-free or null pointers

5. **Exceptional compiler feedback** - rustc's error messages are educational and clear, making them ideal for AI-assisted development with Claude Code

6. **Strong ecosystem** for our specific needs:
   - Parser generators: pest, lalrpop, nom
   - ODBC bindings: odbc-rs
   - Data structures: Built-in HashMap, BTreeMap
   - Testing: cargo test, proptest

7. **No performance pressure** means we can write straightforward Rust without clever optimizations, avoiding the borrow checker's complexity

8. **Learning curve is manageable** for this project because:
   - Single-threaded (no Arc/Mutex complexity)
   - In-memory only (no complex lifetime issues with I/O)
   - Mostly tree structures (AST, expressions) which Rust handles well

9. **Refactoring safety** - When we inevitably need to change types or add features, Rust's compiler will guide us through all the places that need updates

## Consequences

### Positive

* ✅ **Compile-time correctness** - Vast majority of bugs caught before running tests
* ✅ **Excellent error messages** - Compiler helps us learn and fix issues quickly
* ✅ **Safe refactoring** - Can confidently make large changes
* ✅ **No null pointer bugs** - Option<T> forces explicit handling
* ✅ **Clear intent** - Type system documents what functions do
* ✅ **Memory safety** - No use-after-free, double-free, or buffer overflows
* ✅ **Great dev tools** - cargo, clippy, rustfmt, rust-analyzer work out of the box

### Negative

* ❌ **Initial learning curve** - Borrow checker takes time to understand
  - **Mitigation**: Focus on simple patterns first, leverage Claude Code for help with borrow checker errors

* ❌ **Slower compilation** - Rust compiles slower than Go/Python
  - **Mitigation**: Use `cargo check` for fast feedback during development
  - **Impact**: Not critical since we're not in a tight edit-compile-run loop (correctness > speed)

* ❌ **More verbose than Python** - Requires explicit types and error handling
  - **Mitigation**: This is actually a feature - explicitness aids correctness
  - **Impact**: Minimal - we value clarity over brevity

* ❌ **JDBC implementation complexity** - Requires JNI bindings
  - **Mitigation**: Use jni-rs crate, focus on minimal JDBC subset needed for tests
  - **Impact**: More work than native Java, but manageable

### Neutral

* **LOC estimate**: 40,000-60,000 lines (mid-range)
* **Development speed**: Slower than Python initially, but fewer debugging cycles
* **Community**: Large and growing Rust community for help

## Implementation Notes

### Project Structure
```
vibesql/
├── Cargo.toml              # Workspace definition
├── crates/
│   ├── parser/             # SQL parser (pest or lalrpop)
│   ├── types/              # SQL type system
│   ├── ast/                # Abstract Syntax Tree definitions
│   ├── catalog/            # Schema and metadata
│   ├── storage/            # In-memory storage (HashMap-based)
│   ├── executor/           # Query execution engine
│   ├── transaction/        # Transaction manager (ACI only)
│   ├── odbc-driver/        # ODBC driver implementation
│   └── jdbc-driver/        # JDBC driver (JNI bindings)
└── tests/                  # Integration tests
```

### Key Rust Features We'll Use

1. **Enums for SQL types and AST**:
   ```rust
   enum SqlValue {
       Integer(i64),
       Varchar(String),
       Boolean(bool),
       Null,
   }
   ```

2. **Pattern matching for evaluation**:
   ```rust
   match expression {
       Expr::BinaryOp { op, left, right } => { /* ... */ }
       Expr::Column(name) => { /* ... */ }
       // Compiler ensures exhaustiveness
   }
   ```

3. **Result<T, E> for error handling**:
   ```rust
   fn execute_query(&mut self, sql: &str) -> Result<ResultSet, SqlError>
   ```

4. **HashMap for in-memory tables**:
   ```rust
   struct Database {
       tables: HashMap<String, Table>,
   }
   ```

5. **Box for recursive types**:
   ```rust
   enum Expr {
       BinaryOp {
           left: Box<Expr>,  // Box allows recursion
           right: Box<Expr>,
       }
   }
   ```

### Development Workflow

1. **Fast feedback loop**:
   ```bash
   cargo check    # Fast syntax/type checking (seconds)
   cargo build    # Full compilation
   cargo test     # Run tests
   cargo clippy   # Additional lints
   ```

2. **Incremental development**:
   - Build one crate at a time
   - Each crate has its own tests
   - Compiler guides implementation

3. **AI assistance**:
   - Clear compiler errors make it easy for Claude Code to help
   - Type signatures document intent
   - Pattern match warnings show missing cases

## Validation

Success criteria for this decision:

1. ✅ **Compiler catches bugs** - Most errors found before runtime
2. ✅ **Clear error messages** - Easy to understand and fix issues
3. ✅ **Productive development** - Fast iteration despite compile times
4. ✅ **Correct implementation** - Passes sqltest suite
5. ✅ **Maintainable code** - Clear, readable, well-documented

We'll validate this choice after Phase 1 (Parser implementation). If Rust proves too difficult or slow for development, we can revisit (though this is unlikely given our requirements).

## References

### Rust Resources
* **The Rust Book**: https://doc.rust-lang.org/book/
* **Rust by Example**: https://doc.rust-lang.org/rust-by-example/
* **Parser Generators**:
  - pest: https://pest.rs/ (PEG parser, good for SQL)
  - lalrpop: https://github.com/lalrpop/lalrpop (LR parser)
  - nom: https://github.com/Geal/nom (parser combinators)

### Similar Projects in Rust
* **TiKV**: Distributed database in Rust
* **Databend**: Cloud data warehouse in Rust
* **Cube Store**: OLAP storage engine in Rust
* **DataFusion**: Query engine in Rust (Apache Arrow project)

### ODBC/JDBC
* **odbc-rs**: https://github.com/pacman82/odbc-api (Rust ODBC bindings)
* **jni-rs**: https://github.com/jni-rs/jni-rs (Rust JNI bindings for JDBC)

### Related Decisions
* ADR-0002: Parser Strategy (pest vs lalrpop) - Coming next
* ADR-0003: Storage Architecture - To be decided

---

**Status**: ACCEPTED ✅

**Date Accepted**: 2025-10-25

**Next Steps**:
1. Initialize Cargo workspace
2. Set up project structure (crates/)
3. Choose parser generator (ADR-0002)
4. Begin Phase 1: SQL Parser implementation
