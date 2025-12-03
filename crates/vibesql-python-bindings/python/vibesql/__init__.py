"""
VibeSQL Python bindings

DB-API 2.0 compliant Python interface for the VibeSQL SQL database engine.
"""

from . import _vibesql as _vibesql_core

# Re-export everything from the Rust module
from ._vibesql import (
    Database,
    Cursor,
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
    connect,
    enable_profiling,
    disable_profiling,
)

# DB-API 2.0 module-level attributes
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

__version__ = "0.1.0"
__all__ = [
    # Module attributes
    "apilevel",
    "threadsafety", 
    "paramstyle",
    # Exceptions
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    # Classes
    "Database",
    "Cursor",
    # Functions
    "connect",
    "enable_profiling",
    "disable_profiling",
]
