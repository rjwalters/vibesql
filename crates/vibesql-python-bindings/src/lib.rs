//! Python bindings for vibesql using PyO3
//!
//! This module provides Python bindings following DB-API 2.0 conventions
//! to expose the Rust database library to Python for benchmarking and usage.
//!
//! # Module Organization
//!
//! The library is organized into focused modules:
//!
//! - **conversions**: Type conversions between Python and Rust SqlValue types
//! - **connection**: Database connection management
//! - **cursor**: Query cursor implementation and result fetching
//! - **profiling**: Performance profiling utilities
//!
//! # Quick Start
//!
//! ```python
//! import vibesql
//!
//! # Create a connection
//! db = vibesql.connect()
//!
//! # Get a cursor
//! cursor = db.cursor()
//!
//! # Execute a query
//! cursor.execute("SELECT 1")
//!
//! # Fetch results
//! result = cursor.fetchall()
//! ```

// Suppress PyO3 macro warnings
#![allow(non_local_definitions)]

mod connection;
mod conversions;
mod cursor;
mod profiling;

// Re-export public types for use in submodules
pub use connection::Database;
pub use cursor::Cursor;
use pyo3::{exceptions::PyException, prelude::*};

// Exception hierarchy following PEP 249
pyo3::create_exception!(vibesql, Warning, PyException);
pyo3::create_exception!(vibesql, Error, PyException);
pyo3::create_exception!(vibesql, InterfaceError, Error);
pyo3::create_exception!(vibesql, DatabaseError, Error);
pyo3::create_exception!(vibesql, DataError, DatabaseError);
pyo3::create_exception!(vibesql, OperationalError, DatabaseError);
pyo3::create_exception!(vibesql, IntegrityError, DatabaseError);
pyo3::create_exception!(vibesql, InternalError, DatabaseError);
pyo3::create_exception!(vibesql, ProgrammingError, DatabaseError);
pyo3::create_exception!(vibesql, NotSupportedError, DatabaseError);

/// Factory function to create a database connection
///
/// Creates a new in-memory vibesql database and returns a connection object.
/// Use this to obtain a Database instance for executing SQL statements.
///
/// # Returns
/// A new Database connection
///
/// # Example
/// ```python
/// import vibesql
/// db = vibesql.connect()
/// cursor = db.cursor()
/// ```
#[pyfunction]
fn connect() -> PyResult<Database> {
    Ok(Database::new())
}

/// Enable performance profiling (prints detailed timing to stderr)
///
/// When enabled, profiling information is printed to stderr for each
/// executed query, showing timing information for various stages of
/// query execution.
#[pyfunction]
fn enable_profiling() {
    profiling::enable_profiling();
}

/// Disable performance profiling
///
/// Stops printing profiling information to stderr.
#[pyfunction]
fn disable_profiling() {
    profiling::disable_profiling();
}

/// Python module initialization
///
/// Registers all public types and functions with the Python module.
#[pymodule]
fn vibesql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // DB-API 2.0 module-level attributes
    m.add("apilevel", "2.0")?;
    m.add("threadsafety", 1)?;
    m.add("paramstyle", "qmark")?;

    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(enable_profiling, m)?)?;
    m.add_function(wrap_pyfunction!(disable_profiling, m)?)?;
    m.add_class::<Database>()?;
    m.add_class::<Cursor>()?;

    // DB-API 2.0 exception hierarchy
    m.add("Warning", m.py().get_type::<Warning>())?;
    m.add("Error", m.py().get_type::<Error>())?;
    m.add("InterfaceError", m.py().get_type::<InterfaceError>())?;
    m.add("DatabaseError", m.py().get_type::<DatabaseError>())?;
    m.add("DataError", m.py().get_type::<DataError>())?;
    m.add("OperationalError", m.py().get_type::<OperationalError>())?;
    m.add("IntegrityError", m.py().get_type::<IntegrityError>())?;
    m.add("InternalError", m.py().get_type::<InternalError>())?;
    m.add("ProgrammingError", m.py().get_type::<ProgrammingError>())?;
    m.add("NotSupportedError", m.py().get_type::<NotSupportedError>())?;

    Ok(())
}
