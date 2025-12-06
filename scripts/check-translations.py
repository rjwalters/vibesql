#!/usr/bin/env python3
"""
Translation completeness checker for VibeSQL.

This script compares translations across locales to find:
- Missing message IDs (in en-US but not in target locale)
- Extra message IDs (in target locale but not in en-US)
- Variable mismatches between translations

Usage:
    python scripts/check-translations.py [--locale LOCALE] [--threshold PERCENT]

Examples:
    python scripts/check-translations.py              # Check all locales
    python scripts/check-translations.py --locale es  # Check only Spanish
    python scripts/check-translations.py --threshold 90  # Fail if < 90% coverage
"""

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass


@dataclass
class MessageInfo:
    """Information about a Fluent message."""
    id: str
    variables: Set[str]
    has_attributes: bool
    line_number: int


@dataclass
class LocaleReport:
    """Report for a single locale's translation coverage."""
    locale: str
    total_reference: int
    translated: int
    missing: List[str]
    extra: List[str]
    variable_mismatches: List[Tuple[str, Set[str], Set[str]]]

    @property
    def coverage_percent(self) -> float:
        if self.total_reference == 0:
            return 100.0
        return (self.translated / self.total_reference) * 100


def find_resources_dir() -> Path:
    """Find the l10n resources directory."""
    # Try common locations
    candidates = [
        Path("crates/vibesql-l10n/resources"),
        Path("../crates/vibesql-l10n/resources"),
        Path("../../crates/vibesql-l10n/resources"),
    ]

    # Also check from script location
    script_dir = Path(__file__).parent.absolute()
    candidates.append(script_dir.parent / "crates" / "vibesql-l10n" / "resources")

    for candidate in candidates:
        if candidate.is_dir():
            return candidate.absolute()

    raise FileNotFoundError(
        "Could not find l10n resources directory. "
        "Run from repository root or specify path."
    )


def parse_ftl_file(file_path: Path) -> Dict[str, MessageInfo]:
    """
    Parse a Fluent file and extract message IDs with their variables.

    Returns a dict mapping message ID to MessageInfo.
    """
    messages = {}

    if not file_path.exists():
        return messages

    content = file_path.read_text(encoding="utf-8")
    lines = content.split("\n")

    current_msg_id = None
    current_line = 0

    # Regex patterns
    msg_pattern = re.compile(r"^([a-zA-Z][a-zA-Z0-9_-]*)\s*=")
    attr_pattern = re.compile(r"^\s+\.([a-zA-Z][a-zA-Z0-9_-]*)\s*=")
    var_pattern = re.compile(r"\{\s*\$([a-zA-Z_][a-zA-Z0-9_]*)")

    for i, line in enumerate(lines, 1):
        # Skip comments and empty lines
        if line.strip().startswith("#") or not line.strip():
            continue

        # Check for new message
        msg_match = msg_pattern.match(line)
        if msg_match:
            current_msg_id = msg_match.group(1)
            current_line = i
            variables = set(var_pattern.findall(line))
            messages[current_msg_id] = MessageInfo(
                id=current_msg_id,
                variables=variables,
                has_attributes=False,
                line_number=i
            )
            continue

        # Check for attribute (continuation of previous message)
        attr_match = attr_pattern.match(line)
        if attr_match and current_msg_id:
            messages[current_msg_id].has_attributes = True
            # Add variables from attribute line
            messages[current_msg_id].variables.update(var_pattern.findall(line))
            continue

        # Check for continuation line (starts with whitespace)
        if line.startswith(" ") or line.startswith("\t"):
            if current_msg_id and current_msg_id in messages:
                # Add variables from continuation line
                messages[current_msg_id].variables.update(var_pattern.findall(line))

    return messages


def get_all_locales(resources_dir: Path) -> List[str]:
    """Get all available locale directories."""
    locales = []
    for item in resources_dir.iterdir():
        if item.is_dir() and not item.name.startswith("."):
            locales.append(item.name)
    return sorted(locales)


def get_ftl_files(locale_dir: Path) -> List[str]:
    """Get all .ftl files in a locale directory."""
    return sorted([f.name for f in locale_dir.glob("*.ftl")])


def check_locale(
    resources_dir: Path,
    locale: str,
    reference_locale: str = "en-US"
) -> LocaleReport:
    """
    Check a locale against the reference locale.

    Returns a LocaleReport with coverage information.
    """
    ref_dir = resources_dir / reference_locale
    target_dir = resources_dir / locale

    # Collect all messages from reference locale
    ref_messages: Dict[str, MessageInfo] = {}
    for ftl_file in get_ftl_files(ref_dir):
        file_messages = parse_ftl_file(ref_dir / ftl_file)
        ref_messages.update(file_messages)

    # Collect all messages from target locale
    target_messages: Dict[str, MessageInfo] = {}
    for ftl_file in get_ftl_files(ref_dir):  # Use ref_dir to check same files
        target_path = target_dir / ftl_file
        if target_path.exists():
            file_messages = parse_ftl_file(target_path)
            target_messages.update(file_messages)

    # Find missing and extra messages
    ref_ids = set(ref_messages.keys())
    target_ids = set(target_messages.keys())

    missing = sorted(ref_ids - target_ids)
    extra = sorted(target_ids - ref_ids)

    # Check variable consistency for translated messages
    variable_mismatches = []
    for msg_id in ref_ids & target_ids:
        ref_vars = ref_messages[msg_id].variables
        target_vars = target_messages[msg_id].variables
        if ref_vars != target_vars:
            variable_mismatches.append((msg_id, ref_vars, target_vars))

    return LocaleReport(
        locale=locale,
        total_reference=len(ref_ids),
        translated=len(ref_ids) - len(missing),
        missing=missing,
        extra=extra,
        variable_mismatches=variable_mismatches
    )


def print_report(report: LocaleReport, verbose: bool = False) -> None:
    """Print a locale report."""
    coverage = report.coverage_percent

    # Color codes for terminal
    green = "\033[32m"
    yellow = "\033[33m"
    red = "\033[31m"
    reset = "\033[0m"

    # Choose color based on coverage
    if coverage >= 100:
        color = green
        status = "COMPLETE"
    elif coverage >= 80:
        color = yellow
        status = "GOOD"
    else:
        color = red
        status = "NEEDS WORK"

    print(f"\n{color}=== {report.locale} ==={reset}")
    print(f"Coverage: {color}{coverage:.1f}%{reset} ({report.translated}/{report.total_reference}) [{status}]")

    if report.missing:
        print(f"\n{yellow}Missing messages ({len(report.missing)}):{reset}")
        if verbose or len(report.missing) <= 10:
            for msg_id in report.missing:
                print(f"  - {msg_id}")
        else:
            for msg_id in report.missing[:10]:
                print(f"  - {msg_id}")
            print(f"  ... and {len(report.missing) - 10} more")

    if report.extra:
        print(f"\n{yellow}Extra messages ({len(report.extra)}):{reset}")
        if verbose or len(report.extra) <= 5:
            for msg_id in report.extra:
                print(f"  - {msg_id}")
        else:
            for msg_id in report.extra[:5]:
                print(f"  - {msg_id}")
            print(f"  ... and {len(report.extra) - 5} more")

    if report.variable_mismatches:
        print(f"\n{red}Variable mismatches ({len(report.variable_mismatches)}):{reset}")
        for msg_id, ref_vars, target_vars in report.variable_mismatches:
            missing_vars = ref_vars - target_vars
            extra_vars = target_vars - ref_vars
            print(f"  - {msg_id}:")
            if missing_vars:
                print(f"      Missing: {', '.join(sorted(missing_vars))}")
            if extra_vars:
                print(f"      Extra: {', '.join(sorted(extra_vars))}")


def generate_badge_json(reports: List[LocaleReport], output_path: Optional[Path] = None) -> dict:
    """Generate a JSON object for shields.io badge."""
    # Calculate average coverage
    if not reports:
        avg_coverage = 0
    else:
        avg_coverage = sum(r.coverage_percent for r in reports) / len(reports)

    # Determine color
    if avg_coverage >= 100:
        color = "brightgreen"
    elif avg_coverage >= 90:
        color = "green"
    elif avg_coverage >= 80:
        color = "yellow"
    elif avg_coverage >= 50:
        color = "orange"
    else:
        color = "red"

    badge = {
        "schemaVersion": 1,
        "label": "i18n",
        "message": f"{avg_coverage:.0f}%",
        "color": color
    }

    if output_path:
        import json
        output_path.write_text(json.dumps(badge, indent=2))

    return badge


def main():
    parser = argparse.ArgumentParser(
        description="Check translation completeness for VibeSQL"
    )
    parser.add_argument(
        "--locale",
        help="Check only this locale (default: all)"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0,
        help="Minimum coverage percentage required (default: 0, no threshold)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show all missing messages, not just first 10"
    )
    parser.add_argument(
        "--badge",
        type=Path,
        help="Output path for shields.io badge JSON"
    )
    parser.add_argument(
        "--ci",
        action="store_true",
        help="CI mode: exit with error if any locale is below threshold"
    )

    args = parser.parse_args()

    try:
        resources_dir = find_resources_dir()
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Resources directory: {resources_dir}")

    # Get locales to check
    all_locales = get_all_locales(resources_dir)
    reference_locale = "en-US"

    if reference_locale not in all_locales:
        print(f"Error: Reference locale '{reference_locale}' not found", file=sys.stderr)
        sys.exit(1)

    # Filter locales if specific one requested
    if args.locale:
        if args.locale not in all_locales:
            print(f"Error: Locale '{args.locale}' not found", file=sys.stderr)
            print(f"Available locales: {', '.join(all_locales)}")
            sys.exit(1)
        locales_to_check = [args.locale]
    else:
        locales_to_check = [l for l in all_locales if l != reference_locale]

    if not locales_to_check:
        print("No locales to check (only reference locale exists)")
        sys.exit(0)

    print(f"Checking {len(locales_to_check)} locale(s) against {reference_locale}")

    # Check each locale
    reports = []
    below_threshold = []

    for locale in locales_to_check:
        report = check_locale(resources_dir, locale, reference_locale)
        reports.append(report)
        print_report(report, args.verbose)

        if args.threshold > 0 and report.coverage_percent < args.threshold:
            below_threshold.append(locale)

    # Generate badge if requested
    if args.badge:
        badge = generate_badge_json(reports, args.badge)
        print(f"\nBadge JSON written to: {args.badge}")

    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)

    for report in reports:
        status = "OK" if report.coverage_percent >= args.threshold else "BELOW THRESHOLD"
        print(f"{report.locale}: {report.coverage_percent:.1f}% [{status}]")

    # Exit with error in CI mode if below threshold
    if args.ci and below_threshold:
        print(f"\nError: {len(below_threshold)} locale(s) below {args.threshold}% threshold:")
        for locale in below_threshold:
            print(f"  - {locale}")
        sys.exit(1)

    # Exit with error if there are variable mismatches
    total_mismatches = sum(len(r.variable_mismatches) for r in reports)
    if total_mismatches > 0:
        print(f"\nWarning: {total_mismatches} variable mismatch(es) found")
        if args.ci:
            sys.exit(1)

    print("\nDone!")


if __name__ == "__main__":
    main()
