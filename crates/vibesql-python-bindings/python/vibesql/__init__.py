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

# Derive __version__ from the installed distribution metadata (populated by
# maturin from pyproject.toml, which scripts/version.sh keeps in sync with the
# workspace version). A hardcoded string here drifts silently across releases
# and defeats staleness checks (issue #6323).
try:
    from importlib.metadata import PackageNotFoundError, version as _dist_version

    try:
        __version__ = _dist_version("vibesql")
    except PackageNotFoundError:
        # Imported from a source tree rather than an installed distribution.
        __version__ = "0.0.0+unknown"
    del _dist_version, PackageNotFoundError
except ImportError:  # pragma: no cover - importlib.metadata requires Python 3.8+
    __version__ = "0.0.0+unknown"
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
