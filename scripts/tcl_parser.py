#!/usr/bin/env python3
"""
TCL Test Parser for SQLite TCL Test Suite

Parses SQLite's TCL test files and extracts test cases that can be
executed against VibeSQL. Focuses on the common test patterns:

- do_execsql_test: Execute SQL and compare output
- do_catchsql_test: Execute SQL and expect specific error
- do_test with execsql: General test with SQL execution
- execsql blocks: Standalone SQL execution (setup)
- db eval blocks: Standalone SQL execution (setup)

This is a "hybrid" approach that parses simple patterns while skipping
tests with complex TCL logic that would require a full TCL interpreter.
"""

import re
import sys
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional


class TestType(Enum):
    EXECSQL = "execsql"           # do_execsql_test
    CATCHSQL = "catchsql"         # do_catchsql_test
    GENERAL = "general"           # do_test with execsql
    SETUP = "setup"               # standalone execsql (no assertion)
    SKIPPED = "skipped"           # complex TCL logic, skipped


@dataclass
class ParsedTest:
    """Represents a single parsed test case from a TCL file."""
    name: str
    test_type: TestType
    sql: str
    expected_output: Optional[str] = None
    expected_error: Optional[str] = None
    error_code: Optional[int] = None
    line_number: int = 0
    skip_reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "type": self.test_type.value,
            "sql": self.sql,
            "expected_output": self.expected_output,
            "expected_error": self.expected_error,
            "error_code": self.error_code,
            "line_number": self.line_number,
            "skip_reason": self.skip_reason,
        }


@dataclass
class ParsedFile:
    """Represents all parsed content from a single TCL test file."""
    file_path: str
    tests: list[ParsedTest] = field(default_factory=list)
    setup_sql: list[str] = field(default_factory=list)
    skipped_count: int = 0
    parse_errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "file_path": self.file_path,
            "tests": [t.to_dict() for t in self.tests],
            "setup_sql": self.setup_sql,
            "skipped_count": self.skipped_count,
            "parse_errors": self.parse_errors,
            "stats": {
                "total_tests": len(self.tests),
                "execsql_tests": len([t for t in self.tests if t.test_type == TestType.EXECSQL]),
                "catchsql_tests": len([t for t in self.tests if t.test_type == TestType.CATCHSQL]),
                "skipped_tests": len([t for t in self.tests if t.test_type == TestType.SKIPPED]),
            }
        }


class TclTestParser:
    """
    Parser for SQLite TCL test files.

    Handles common test patterns while gracefully skipping complex TCL logic.
    """

    # Patterns for different test types
    DO_EXECSQL_PATTERN = re.compile(
        r'do_execsql_test\s+(\S+)\s*\{([^}]*)\}\s*\{([^}]*)\}',
        re.DOTALL
    )

    DO_CATCHSQL_PATTERN = re.compile(
        r'do_catchsql_test\s+(\S+)\s*\{([^}]*)\}\s*\{(\d+)\s+\{([^}]*)\}\}',
        re.DOTALL
    )

    # Simpler catchsql pattern: {1 {error message}}
    DO_CATCHSQL_SIMPLE_PATTERN = re.compile(
        r'do_catchsql_test\s+(\S+)\s*\{([^}]*)\}\s*\{(\d+)\s+([^}]+)\}',
        re.DOTALL
    )

    DO_TEST_PATTERN = re.compile(
        r'do_test\s+(\S+)\s*\{([^}]*)\}\s*\{([^}]*)\}',
        re.DOTALL
    )

    EXECSQL_BLOCK_PATTERN = re.compile(
        r'execsql\s*\{([^}]*)\}',
        re.DOTALL
    )

    # Pattern for db eval {...} blocks (common setup pattern in TCL tests)
    DB_EVAL_BLOCK_PATTERN = re.compile(
        r'db\s+eval\s*\{([^}]*)\}',
        re.DOTALL
    )

    # Patterns indicating complex TCL logic we should skip
    # Note: db eval is NOT in this list - we extract it as setup SQL
    COMPLEX_PATTERNS = [
        r'\$\w+',           # TCL variables
        r'\[.*\]',          # TCL command substitution
        r'foreach\s+',      # Loops
        r'for\s*\{',        # For loops
        r'while\s*\{',      # While loops
        r'if\s*\{',         # If statements (in test body)
        r'proc\s+',         # Procedure definitions
        r'expr\s*\{',       # Expressions
        r'db\s+close',      # DB operations
        r'file\s+',         # File operations
        r'sqlite3\s+',      # SQLite3 command
        r'catchsql',        # Nested catchsql
    ]

    # Patterns to skip entire test files
    SKIP_FILE_PATTERNS = [
        r'malloc',          # Memory allocation tests
        r'corrupt',         # Corruption tests
        r'crash',           # Crash recovery tests
        r'thread',          # Threading tests
        r'shell',           # Shell tests
        r'fts[0-9]',        # Full-text search (not supported)
        r'rtree',           # R-tree (not supported)
        r'wal',             # WAL tests (SQLite-specific)
        r'journal',         # Journal tests (SQLite-specific)
        r'vacuum',          # Vacuum tests
        r'attach',          # Attach database
        r'vtab',            # Virtual tables
        r'intarray',        # Int array extension
    ]

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self._complex_regex = re.compile('|'.join(self.COMPLEX_PATTERNS))
        self._skip_file_regex = re.compile('|'.join(self.SKIP_FILE_PATTERNS), re.IGNORECASE)

    def should_skip_file(self, file_path: str) -> Optional[str]:
        """Check if entire file should be skipped based on filename."""
        filename = Path(file_path).stem.lower()
        match = self._skip_file_regex.search(filename)
        if match:
            return f"File type '{match.group()}' not applicable to VibeSQL"
        return None

    def _has_complex_tcl(self, code: str) -> bool:
        """Check if code contains complex TCL that we can't parse."""
        return bool(self._complex_regex.search(code))

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL extracted from TCL blocks."""
        # Remove leading/trailing whitespace
        sql = sql.strip()
        # Remove TCL comments (lines starting with #)
        lines = [l for l in sql.split('\n') if not l.strip().startswith('#')]
        sql = '\n'.join(lines)
        return sql.strip()

    def _parse_expected_output(self, output: str) -> str:
        """Parse expected output from TCL format to normalized format."""
        output = output.strip()
        # TCL lists use spaces as separators
        # Multi-row results may have newlines
        return output

    def _extract_execsql_from_do_test(self, body: str) -> Optional[str]:
        """Extract SQL from a do_test body that uses execsql."""
        # Look for execsql { ... } pattern
        match = self.EXECSQL_BLOCK_PATTERN.search(body)
        if match:
            return self._clean_sql(match.group(1))
        return None

    def parse_file(self, file_path: str) -> ParsedFile:
        """Parse a single TCL test file."""
        result = ParsedFile(file_path=file_path)

        # Check if we should skip the entire file
        skip_reason = self.should_skip_file(file_path)
        if skip_reason:
            result.parse_errors.append(f"Skipped: {skip_reason}")
            return result

        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
        except Exception as e:
            result.parse_errors.append(f"Error reading file: {e}")
            return result

        # Track line numbers for each match
        lines = content.split('\n')

        # Parse do_execsql_test
        for match in self.DO_EXECSQL_PATTERN.finditer(content):
            name = match.group(1)
            sql = self._clean_sql(match.group(2))
            expected = self._parse_expected_output(match.group(3))

            # Calculate line number
            line_num = content[:match.start()].count('\n') + 1

            # Check for complex TCL
            if self._has_complex_tcl(sql):
                result.tests.append(ParsedTest(
                    name=name,
                    test_type=TestType.SKIPPED,
                    sql=sql,
                    line_number=line_num,
                    skip_reason="Complex TCL in SQL block"
                ))
                result.skipped_count += 1
            else:
                result.tests.append(ParsedTest(
                    name=name,
                    test_type=TestType.EXECSQL,
                    sql=sql,
                    expected_output=expected,
                    line_number=line_num
                ))

        # Parse do_catchsql_test
        for match in self.DO_CATCHSQL_PATTERN.finditer(content):
            name = match.group(1)
            sql = self._clean_sql(match.group(2))
            error_code = int(match.group(3))
            error_msg = match.group(4).strip()
            line_num = content[:match.start()].count('\n') + 1

            if self._has_complex_tcl(sql):
                result.tests.append(ParsedTest(
                    name=name,
                    test_type=TestType.SKIPPED,
                    sql=sql,
                    line_number=line_num,
                    skip_reason="Complex TCL in SQL block"
                ))
                result.skipped_count += 1
            else:
                result.tests.append(ParsedTest(
                    name=name,
                    test_type=TestType.CATCHSQL,
                    sql=sql,
                    error_code=error_code,
                    expected_error=error_msg,
                    line_number=line_num
                ))

        # Try simpler catchsql pattern
        for match in self.DO_CATCHSQL_SIMPLE_PATTERN.finditer(content):
            name = match.group(1)
            # Skip if already parsed
            if any(t.name == name for t in result.tests):
                continue

            sql = self._clean_sql(match.group(2))
            error_code = int(match.group(3))
            error_msg = match.group(4).strip()
            line_num = content[:match.start()].count('\n') + 1

            if self._has_complex_tcl(sql):
                result.tests.append(ParsedTest(
                    name=name,
                    test_type=TestType.SKIPPED,
                    sql=sql,
                    line_number=line_num,
                    skip_reason="Complex TCL in SQL block"
                ))
                result.skipped_count += 1
            else:
                result.tests.append(ParsedTest(
                    name=name,
                    test_type=TestType.CATCHSQL,
                    sql=sql,
                    error_code=error_code,
                    expected_error=error_msg,
                    line_number=line_num
                ))

        # Parse do_test with execsql (more complex)
        for match in self.DO_TEST_PATTERN.finditer(content):
            name = match.group(1)
            body = match.group(2)
            expected = match.group(3).strip()
            line_num = content[:match.start()].count('\n') + 1

            # Skip if already parsed as execsql_test or catchsql_test
            if any(t.name == name for t in result.tests):
                continue

            # Try to extract SQL from the body
            sql = self._extract_execsql_from_do_test(body)

            if sql is None:
                # Complex test body, skip
                result.tests.append(ParsedTest(
                    name=name,
                    test_type=TestType.SKIPPED,
                    sql="",
                    line_number=line_num,
                    skip_reason="Complex do_test body without simple execsql"
                ))
                result.skipped_count += 1
            elif self._has_complex_tcl(sql) or self._has_complex_tcl(body):
                result.tests.append(ParsedTest(
                    name=name,
                    test_type=TestType.SKIPPED,
                    sql=sql,
                    line_number=line_num,
                    skip_reason="Complex TCL in test body"
                ))
                result.skipped_count += 1
            else:
                result.tests.append(ParsedTest(
                    name=name,
                    test_type=TestType.GENERAL,
                    sql=sql,
                    expected_output=expected,
                    line_number=line_num
                ))

        # Extract standalone execsql blocks for setup
        # Look for execsql that are NOT inside do_test/do_execsql_test
        for match in self.EXECSQL_BLOCK_PATTERN.finditer(content):
            # Check if this is inside a test block
            start = match.start()
            preceding = content[max(0, start-100):start]
            if 'do_test' in preceding or 'do_execsql_test' in preceding:
                continue

            sql = self._clean_sql(match.group(1))
            if sql and not self._has_complex_tcl(sql):
                result.setup_sql.append(sql)

        # Extract db eval {...} blocks for setup
        # These are commonly used in TCL tests to set up tables and data
        for match in self.DB_EVAL_BLOCK_PATTERN.finditer(content):
            sql = self._clean_sql(match.group(1))
            if sql and not self._has_complex_tcl(sql):
                result.setup_sql.append(sql)

        # Sort tests by line number
        result.tests.sort(key=lambda t: t.line_number)

        if self.verbose:
            print(f"Parsed {file_path}: {len(result.tests)} tests, {result.skipped_count} skipped")

        return result


def parse_directory(dir_path: str, patterns: list[str] = None, verbose: bool = False) -> list[ParsedFile]:
    """Parse all TCL test files in a directory."""
    parser = TclTestParser(verbose=verbose)
    results = []

    dir_path = Path(dir_path)
    if patterns:
        files = []
        for pattern in patterns:
            files.extend(dir_path.glob(pattern))
    else:
        files = list(dir_path.glob("*.test"))

    for file_path in sorted(files):
        result = parser.parse_file(str(file_path))
        results.append(result)

    return results


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Parse SQLite TCL test files")
    parser.add_argument("path", help="File or directory to parse")
    parser.add_argument("--pattern", "-p", action="append",
                       help="Glob pattern(s) for files (e.g., 'select*.test')")
    parser.add_argument("--output", "-o", help="Output JSON file")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--stats-only", action="store_true",
                       help="Only show statistics, not full output")

    args = parser.parse_args()

    path = Path(args.path)

    if path.is_file():
        parser = TclTestParser(verbose=args.verbose)
        results = [parser.parse_file(str(path))]
    elif path.is_dir():
        results = parse_directory(str(path), args.pattern, args.verbose)
    else:
        print(f"Error: {path} is not a file or directory", file=sys.stderr)
        sys.exit(1)

    # Calculate totals
    total_tests = sum(len(r.tests) for r in results)
    total_execsql = sum(len([t for t in r.tests if t.test_type == TestType.EXECSQL]) for r in results)
    total_catchsql = sum(len([t for t in r.tests if t.test_type == TestType.CATCHSQL]) for r in results)
    total_general = sum(len([t for t in r.tests if t.test_type == TestType.GENERAL]) for r in results)
    total_skipped = sum(r.skipped_count for r in results)
    total_setup = sum(len(r.setup_sql) for r in results)

    if args.stats_only:
        print(f"Files parsed:     {len(results)}")
        print(f"Total tests:      {total_tests}")
        print(f"  - execsql:      {total_execsql}")
        print(f"  - catchsql:     {total_catchsql}")
        print(f"  - general:      {total_general}")
        print(f"  - skipped:      {total_skipped}")
        print(f"Setup statements: {total_setup}")
        if total_tests > 0:
            parse_rate = (total_tests - total_skipped) / total_tests * 100
            print(f"Parse rate:       {parse_rate:.1f}%")
    else:
        output = {
            "files": [r.to_dict() for r in results],
            "summary": {
                "files_parsed": len(results),
                "total_tests": total_tests,
                "execsql_tests": total_execsql,
                "catchsql_tests": total_catchsql,
                "general_tests": total_general,
                "skipped_tests": total_skipped,
                "setup_statements": total_setup,
                "parse_rate": (total_tests - total_skipped) / total_tests * 100 if total_tests > 0 else 0,
            }
        }

        if args.output:
            with open(args.output, 'w') as f:
                json.dump(output, f, indent=2)
            print(f"Output written to {args.output}")
        else:
            print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
