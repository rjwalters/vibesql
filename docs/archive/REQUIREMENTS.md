# Project Requirements Specification

## Overview
This document captures the complete requirements for VibeSQL, a NIST-compatible in-memory SQL database project.

> **Note**: These requirements originated from the posix4e/nistmemsql challenge.
> This project (VibeSQL) has since evolved independently with AI-first methodology using Loom orchestration.

## Core Requirements

### 1. SQL Standard Version
**Specification**: SQL:1999 (ISO/IEC 9075:1999)
**Source**: [Issue #1](https://github.com/posix4e/nistmemsql/issues/1)

SQL:1999 added significant features over SQL-92:
- Recursive queries (Common Table Expressions with RECURSIVE)
- Triggers
- Boolean data type
- Large Objects (LOBs): BLOB, CLOB
- User-defined types
- Object-relational features
- Regular expression support (SIMILAR TO)
- Additional datetime functionality
- Roles for access control
- SAVEPOINT for nested transactions

### 2. Compliance Level
**Specification**: FULL COMPLIANCE
**Source**: [Issue #2](https://github.com/posix4e/nistmemsql/issues/2)

This means:
- All Core SQL:1999 features (mandatory)
- All optional packages and features
- Complete conformance to the standard
- No exceptions or partial implementations

**Note**: This is an extremely ambitious goal. Even major commercial databases don't achieve full compliance.

### 3. Programming Language
**Specification**: No preference from upstream
**Source**: [Issue #3](https://github.com/posix4e/nistmemsql/issues/3)

**Recommendation for consideration**:
- **Rust**: Best choice for memory safety, performance, and modern tooling
- **Pros**: Memory safety without GC, excellent concurrency, growing ecosystem
- **Cons**: Steeper learning curve, longer compile times
- **Alternative**: Java for easier JDBC integration, but performance overhead

### 4. Test Execution Interface
**Specification**: Direct API testing (Rust API, CLI, WASM)
**Rationale**: ODBC/JDBC are SQL/CLI (Part 3), not SQL Foundation (Part 2)

Requirements:
- Rust API for direct programmatic testing
- CLI interface for command-line test execution
- WASM interface for browser-based testing
- All interfaces must produce consistent, validated results
- Test suite must pass through any of these interfaces

**Why This Approach**:
- ODBC/JDBC are client protocols, not part of Core SQL:1999 compliance
- Direct API testing is simpler, faster, and more reliable
- Preserves WASM/browser portability (no native driver dependencies)
- Core compliance is about SQL language semantics, not transport layer
- NIST tests care about SQL statement results, not how they're delivered

### 5. Persistence
**Specification**: No persistence required - "No persistence. it's just for testing"
**Source**: [Issue #5](https://github.com/posix4e/nistmemsql/issues/5)

**Requirements**:
- **Purely ephemeral** - data lost when process ends
- **No WAL (Write-Ahead Log)** required
- **No durability** requirements
- In-memory only implementation

**Impact**: Major simplification - no disk I/O, no recovery, no persistence layer needed

### 6. Test Suite
**Specification**: Use sqltest suite - "Here's an example including 2016 tests https://github.com/elliotchance/sqltest/tree/master"
**Source**: [Issue #6](https://github.com/posix4e/nistmemsql/issues/6)

**Test Suite**: https://github.com/elliotchance/sqltest
- Comprehensive SQL conformance test suite
- Includes tests for SQL:92, SQL:99, SQL:2003, SQL:2011, SQL:2016
- Well-organized by feature
- Open source and actively maintained

Requirements:
- Integrate sqltest test suite
- Automate execution through GitHub Actions
- Run tests via Rust API, CLI, and WASM interfaces
- Generate compliance reports
- Focus on SQL:1999 tests (but suite includes newer standards too)

### 7. Implementation Priorities
**Specification**: "I do not care about performance. You are in memory. Feel free to be single threaded. there's no requirement for steady storage or WAL"
**Source**: [Issue #7](https://github.com/posix4e/nistmemsql/issues/7)

**Clear Priorities**:
1. **Correctness and SQL:1999 compliance** - Primary goal
2. **Simplicity** - Single-threaded is acceptable
3. **No performance optimization** required
4. **No persistence/WAL** - Ephemeral only
5. **Standard conformance** over speed

**Architectural Simplifications Allowed**:
- ✅ Single-threaded execution (no concurrency complexity)
- ✅ No transaction log or WAL
- ✅ No durability mechanisms
- ✅ No performance optimization needed
- ✅ Simple, straightforward implementations preferred
- ✅ No need for query optimization (can use naive plans)

**Impact**: This **dramatically reduces complexity** - we can focus entirely on correctness without worrying about performance, concurrency, or persistence.

## Technical Scope

### Minimum Feature Set (SQL:1999 Core)

#### Data Types
- INTEGER, SMALLINT, NUMERIC, DECIMAL, FLOAT, REAL, DOUBLE PRECISION
- CHARACTER, VARCHAR, CHARACTER LARGE OBJECT (CLOB)
- BINARY LARGE OBJECT (BLOB)
- BOOLEAN
- DATE, TIME, TIMESTAMP, INTERVAL
- User-defined types

#### DDL (Data Definition Language)
- CREATE/DROP TABLE
- CREATE/DROP VIEW
- CREATE/DROP INDEX
- ALTER TABLE
- CREATE/DROP SCHEMA
- CREATE/DROP DOMAIN
- Constraints: PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, NOT NULL

#### DML (Data Manipulation Language)
- SELECT with full query expressions
- INSERT, UPDATE, DELETE
- JOIN operations: INNER, LEFT/RIGHT/FULL OUTER, CROSS
- Subqueries (scalar, row, table)
- Common Table Expressions (WITH, WITH RECURSIVE)
- Set operations: UNION, INTERSECT, EXCEPT
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- GROUP BY, HAVING
- ORDER BY
- Window functions (added in SQL:1999)

#### DCL (Data Control Language)
- GRANT, REVOKE
- CREATE/DROP ROLE
- Role-based access control

#### Transaction Control
- BEGIN/START TRANSACTION
- COMMIT, ROLLBACK
- SAVEPOINT, RELEASE SAVEPOINT
- SET TRANSACTION isolation levels

#### Advanced Features
- Triggers (CREATE/DROP TRIGGER)
- Stored procedures and functions
- Cursors
- Prepared statements
- Information schema views

#### Built-in Functions
- String functions (SUBSTRING, UPPER, LOWER, TRIM, etc.)
- Numeric functions (ABS, CEILING, FLOOR, MOD, etc.)
- Date/time functions (CURRENT_DATE, CURRENT_TIME, EXTRACT, etc.)
- Conditional expressions (CASE, COALESCE, NULLIF)
- Cast and convert functions

## Architecture Requirements

### Core Components
1. **Lexer/Tokenizer**: Convert SQL text to tokens
2. **Parser**: Build Abstract Syntax Tree (AST) from tokens
3. **Semantic Analyzer**: Type checking, name resolution, validation
4. **Query Planner**: Generate execution plan, optimize queries
5. **Execution Engine**: Execute plans against storage
6. **Storage Engine**: In-memory data structures for tables/indexes
7. **Transaction Manager**: ACID property enforcement
8. **Catalog/Metadata**: System tables, schemas, type information
9. **ODBC Driver**: C-based ODBC API implementation
10. **JDBC Driver**: Java-based JDBC API implementation

### Non-Functional Requirements
- **Correctness**: 100% SQL:1999 compliance is the goal
- **Testability**: Continuous validation against NIST suite
- **Maintainability**: Clean architecture for iterative development
- **Documentation**: Well-documented for understanding and extension
- **Automation**: Full CI/CD pipeline with automated testing

## Success Criteria

The project is successful when:
1. All NIST SQL:1999 conformance tests pass
2. Tests can be executed via both ODBC and JDBC
3. Tests run automatically in GitHub Actions
4. Full SQL:1999 standard compliance is achieved
5. Database can execute all standard SQL:1999 queries correctly

## Known Challenges

### Extremely High Scope
- SQL:1999 standard is ~2000+ pages
- Full compliance includes optional features rarely implemented
- Both ODBC and JDBC require substantial protocol work
- This is comparable to building a production database system

### Technical Complexity
- Query optimization is a deep research area
- Transaction isolation levels are complex to implement correctly
- ODBC/JDBC protocols are extensive specifications
- Parser must handle complete SQL:1999 grammar

### Testing
- Need to locate and integrate official NIST test suite
- May need to develop additional conformance tests
- Test automation through two different protocols

## Risk Assessment

**HIGH RISK**: This project has extremely ambitious scope. Full SQL:1999 compliance with both ODBC and JDBC support is a multi-person-year effort even for experienced database developers.

**Mitigation strategies**:
1. Incremental development with frequent testing
2. Focus on core features first, optional features later
3. Leverage existing libraries where possible (parser generators, etc.)
4. Clear architecture to manage complexity
5. Continuous integration to catch regressions early

## Next Steps

1. Choose implementation language (recommend Rust)
2. Research and obtain NIST SQL:1999 test suite
3. Set up basic project structure
4. Implement SQL:1999 grammar parser
5. Build minimal storage engine
6. Develop execution engine incrementally
7. Implement ODBC/JDBC protocols
8. Integrate testing infrastructure
9. Iterate toward full compliance
