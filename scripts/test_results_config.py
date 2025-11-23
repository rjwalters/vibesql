#!/usr/bin/env python3
"""
Shared configuration for SQLLogicTest results storage.

This module provides a centralized location for test results across all git worktrees.
Results are stored in ~/.vibesql/test_results/ to persist across worktree deletion.
"""

import os
import subprocess
from pathlib import Path
from typing import Optional, Tuple


def get_shared_results_dir() -> Path:
    """
    Get the shared results directory for SQLLogicTest results.

    This directory is outside the repository to persist across git worktrees.
    When a worktree is deleted (e.g., after merging a branch), the test results
    are preserved in this shared location.

    Returns:
        Path to ~/.vibesql/test_results/
    """
    home = Path.home()
    shared_dir = home / ".vibesql" / "test_results"
    shared_dir.mkdir(parents=True, exist_ok=True)
    return shared_dir


def get_default_database_path() -> Path:
    """
    Get the default path for the test results database.

    Returns:
        Path to ~/.vibesql/test_results/sqllogictest_results.vbsql (binary format)
    """
    return get_shared_results_dir() / "sqllogictest_results.vbsql"


def get_default_json_path() -> Path:
    """
    Get the default path for the test results JSON file.

    Returns:
        Path to ~/.vibesql/test_results/sqllogictest_results.json
    """
    return get_shared_results_dir() / "sqllogictest_results.json"


def get_legacy_paths(repo_root: Path) -> Tuple[Path, Path]:
    """
    Get the legacy (target/) paths for migration purposes.

    Args:
        repo_root: Repository root directory

    Returns:
        Tuple of (legacy_sql_path, legacy_json_path)
    """
    target_dir = repo_root / "target"
    return (
        target_dir / "sqllogictest_results.sql",
        target_dir / "sqllogictest_results.json"
    )


def migrate_legacy_results(repo_root: Path, verbose: bool = True) -> bool:
    """
    Migrate test results from target/ to shared directory if they exist.

    This is a one-time migration to move results from the old location
    (target/sqllogictest_results.*) to the new shared location
    (~/.vibesql/test_results/sqllogictest_results.*).

    Args:
        repo_root: Repository root directory
        verbose: Print migration status messages

    Returns:
        True if migration was performed, False if no legacy results found
    """
    import shutil

    legacy_sql, legacy_json = get_legacy_paths(repo_root)
    shared_sql = get_default_database_path()
    shared_json = get_default_json_path()

    migrated = False

    # Migrate SQL database if it exists and shared doesn't exist yet
    if legacy_sql.exists() and not shared_sql.exists():
        if verbose:
            print(f"Migrating test results database from {legacy_sql} to {shared_sql}")
        shutil.copy2(legacy_sql, shared_sql)
        migrated = True

    # Migrate JSON results if it exists and shared doesn't exist yet
    if legacy_json.exists() and not shared_json.exists():
        if verbose:
            print(f"Migrating test results JSON from {legacy_json} to {shared_json}")
        shutil.copy2(legacy_json, shared_json)
        migrated = True

    if migrated and verbose:
        print(f"✓ Migration complete. Results now stored in {get_shared_results_dir()}")
        print(f"  Legacy files in target/ can be safely deleted.")

    return migrated


def ensure_shared_directory() -> Path:
    """
    Ensure the shared results directory exists.

    Returns:
        Path to the shared results directory
    """
    shared_dir = get_shared_results_dir()
    shared_dir.mkdir(parents=True, exist_ok=True)
    return shared_dir


def get_repo_root() -> Path:
    """Find the repository root directory."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find git repository root")


def get_git_commit(repo_root: Optional[Path] = None) -> Optional[str]:
    """Get current git commit hash."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root or get_repo_root(),
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


def get_git_branch(repo_root: Optional[Path] = None) -> Optional[str]:
    """Get current git branch name."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=repo_root or get_repo_root(),
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


def get_vibesql_cli_path(repo_root: Optional[Path] = None) -> Path:
    """
    Get path to vibesql CLI binary.

    Looks for the binary in target/release/vibesql, then target/debug/vibesql.

    Args:
        repo_root: Repository root directory (optional)

    Returns:
        Path to vibesql CLI binary

    Raises:
        FileNotFoundError: If vibesql CLI binary not found
    """
    root = repo_root or get_repo_root()

    # Try release build first
    release_path = root / "target" / "release" / "vibesql"
    if release_path.exists():
        return release_path

    # Fall back to debug build
    debug_path = root / "target" / "debug" / "vibesql"
    if debug_path.exists():
        return debug_path

    raise FileNotFoundError(
        "vibesql CLI not found. Please build it first:\n"
        "  cargo build --release --bin vibesql\n"
        f"Expected location: {release_path}"
    )
