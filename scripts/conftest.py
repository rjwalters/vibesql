"""Pytest configuration for the scripts/ test suite.

Guards the suite against testing the wrong `vibesql` Python bindings.

`import vibesql` resolves to whatever wheel happens to be installed in the
active Python environment — which may be weeks older than this checkout. A
stale wheel produces failures that masquerade as severe engine bugs (e.g. a
spurious "UNIQUE constraint failed" on plainly distinct primary keys, see
issue #6323). This conftest makes both failure modes loud and actionable:

1. **Stale wheel installed** — the whole pytest session aborts immediately,
   naming the stale module path and the rebuild command. The misleading
   engine-bug presentation is unreachable.
2. **No wheel installed** — the bindings-dependent tests skip with a message
   naming the fix. Set ``VIBESQL_REQUIRE_BINDINGS=1`` (done by
   ``make test-scripts`` and CI) to turn that skip into a hard failure so a
   green run proves the bindings were actually exercised.

The freshness check compares the installed bindings' version — the module's
``__version__`` when it defines one, else the ``vibesql`` distribution
metadata (current maturin-built wheels ship a generated ``__init__`` stub
with no ``__version__`` attribute) — against the workspace version in the
root ``Cargo.toml``. This is a cheap tripwire, not proof of freshness — a
wheel built from the same version but older code still passes it. Only
rebuilding (``make test-scripts``) guarantees the repo build is under test.
"""

import os
import re
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

REBUILD_HINT = (
    "Rebuild and install this checkout's own bindings with:\n"
    "  make test-scripts   # builds the wheel, installs it, then runs pytest\n"
    "or manually:\n"
    "  make build-python && python3 -m pip install --force-reinstall target/wheels/vibesql-*.whl\n"
    "or, inside an activated virtualenv:\n"
    "  maturin develop --release -m crates/vibesql-python-bindings/Cargo.toml"
)


class MissingBindingsError(Exception):
    """`import vibesql` failed: no bindings installed in this environment."""


class StaleBindingsError(Exception):
    """The installed `vibesql` bindings do not match this checkout's version."""


def workspace_version() -> str:
    """Parse the workspace version from the root Cargo.toml.

    ``[workspace.package] version`` in the root manifest is the repo's
    version source of truth (see scripts/version.sh). Anchored on ^...$ so
    dependency ``version = "..."`` lines never match.
    """
    cargo_toml = REPO_ROOT / "Cargo.toml"
    match = re.search(
        r'^version = "(\d+\.\d+\.\d+)"$', cargo_toml.read_text(), re.MULTILINE
    )
    if not match:
        raise RuntimeError(f"could not parse workspace version from {cargo_toml}")
    return match.group(1)


def bindings_required() -> bool:
    """True when skips-on-missing-bindings are forbidden (CI / make test-scripts)."""
    return os.environ.get("VIBESQL_REQUIRE_BINDINGS", "") not in ("", "0")


def installed_bindings_version(vibesql_module):
    """Best-available version of the installed bindings, or None if unknowable.

    Prefers the module's own ``__version__`` (catches wheels whose packaged
    ``__init__.py`` carries a hardcoded stale string). Current maturin-built
    wheels ship a generated ``__init__`` stub WITHOUT ``__version__``, so we
    fall back to the ``vibesql`` distribution metadata, which maturin always
    writes from pyproject.toml at build time.
    """
    module_version = getattr(vibesql_module, "__version__", None)
    if module_version is not None:
        return module_version
    try:
        return _dist_version("vibesql")
    except PackageNotFoundError:
        return None


def load_vibesql():
    """Import `vibesql`, verifying it matches this checkout's version.

    Returns the imported module.

    Raises:
        MissingBindingsError: no bindings installed in this environment.
        StaleBindingsError: installed bindings report a different version
            than the workspace (or no version at all) — testing them would
            produce misleading engine-bug failures, so callers must FAIL,
            never skip.
    """
    try:
        import vibesql
    except ImportError as exc:
        raise MissingBindingsError(
            "vibesql Python bindings are not installed in this Python "
            f"environment ({sys.executable}).\n{REBUILD_HINT}"
        ) from exc

    expected = workspace_version()
    actual = installed_bindings_version(vibesql)
    if actual != expected:
        raise StaleBindingsError(
            "Installed vibesql bindings are STALE: import resolved to "
            f"{getattr(vibesql, '__file__', '<unknown>')} reporting version "
            f"{actual!r}, but this checkout's workspace version is {expected!r}. "
            "Testing a stale wheel produces misleading engine-bug failures "
            "(e.g. spurious 'UNIQUE constraint failed' — issue #6323).\n"
            f"{REBUILD_HINT}"
        )
    return vibesql


def pytest_sessionstart(session):
    """Abort the whole run immediately if a stale wheel is installed.

    Absence is NOT an error here: bindings-independent tests should still run,
    and the per-test guard (`load_vibesql` in the tests' setUp) skips — or
    fails, under VIBESQL_REQUIRE_BINDINGS=1 — the tests that need bindings.
    """
    try:
        load_vibesql()
    except MissingBindingsError:
        pass
    except StaleBindingsError as exc:
        pytest.exit(str(exc), returncode=1)
