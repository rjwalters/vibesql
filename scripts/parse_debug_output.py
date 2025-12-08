#!/usr/bin/env python3
"""
Parse structured debug output from VibeSQL.

Usage:
    VIBESQL_DEBUG_FORMAT=json JOIN_PROFILE=1 ./your_benchmark 2>&1 | python scripts/parse_debug_output.py

Options:
    --filter CATEGORY    Filter by category (optimizer, execution, index, dml, profile)
    --filter-event EVENT Filter by event name
    --summary            Show summary statistics
    --json               Output results as JSON (default: human-readable)
    --help               Show this help message

Examples:
    # Show all optimizer events
    ./parse_debug_output.py --filter optimizer < debug.log

    # Get join profile summary
    ./parse_debug_output.py --filter execution --filter-event join_profile_summary < debug.log

    # Output as JSON for further processing
    ./parse_debug_output.py --json < debug.log
"""

import argparse
import json
import sys
from collections import defaultdict
from typing import Iterator, Optional


def parse_debug_lines(lines: Iterator[str]) -> Iterator[dict]:
    """Parse debug output lines, yielding JSON objects."""
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Try to parse as JSON
        if line.startswith('{'):
            try:
                obj = json.loads(line)
                if 'category' in obj and 'event' in obj:
                    yield obj
            except json.JSONDecodeError:
                pass
        # Skip non-JSON lines (traditional text output)


def filter_events(
    events: Iterator[dict],
    category: Optional[str] = None,
    event_name: Optional[str] = None
) -> Iterator[dict]:
    """Filter events by category and/or event name."""
    for event in events:
        if category and event.get('category') != category:
            continue
        if event_name and event.get('event') != event_name:
            continue
        yield event


def compute_summary(events: list) -> dict:
    """Compute summary statistics from events."""
    summary = {
        'total_events': len(events),
        'by_category': defaultdict(int),
        'by_event': defaultdict(int),
        'timing_stats': {},
    }

    timing_events = []

    for event in events:
        category = event.get('category', 'unknown')
        event_name = event.get('event', 'unknown')
        summary['by_category'][category] += 1
        summary['by_event'][event_name] += 1

        # Collect timing data
        data = event.get('data', {})
        for key in ['duration_us', 'grand_total_us', 'optimizer_time_us']:
            if key in data:
                timing_events.append({
                    'event': event_name,
                    'timing_key': key,
                    'value_us': data[key]
                })

    # Compute timing statistics
    if timing_events:
        timing_by_event = defaultdict(list)
        for t in timing_events:
            timing_by_event[t['event']].append(t['value_us'])

        for event_name, values in timing_by_event.items():
            summary['timing_stats'][event_name] = {
                'count': len(values),
                'total_us': sum(values),
                'avg_us': sum(values) / len(values),
                'min_us': min(values),
                'max_us': max(values),
            }

    return summary


def format_human_readable(event: dict) -> str:
    """Format an event for human-readable output."""
    timestamp = event.get('timestamp', '')
    category = event.get('category', 'unknown')
    event_name = event.get('event', 'unknown')
    data = event.get('data', {})

    lines = [f"[{timestamp}] {category}/{event_name}"]

    for key, value in data.items():
        if isinstance(value, dict):
            lines.append(f"  {key}:")
            for k, v in value.items():
                lines.append(f"    {k}: {v}")
        elif isinstance(value, list):
            lines.append(f"  {key}: {value}")
        else:
            lines.append(f"  {key}: {value}")

    return '\n'.join(lines)


def format_summary_human_readable(summary: dict) -> str:
    """Format summary statistics for human-readable output."""
    lines = [
        "=" * 60,
        "DEBUG OUTPUT SUMMARY",
        "=" * 60,
        f"Total events: {summary['total_events']}",
        "",
        "Events by category:",
    ]

    for category, count in sorted(summary['by_category'].items()):
        lines.append(f"  {category}: {count}")

    lines.append("")
    lines.append("Events by type:")
    for event, count in sorted(summary['by_event'].items()):
        lines.append(f"  {event}: {count}")

    if summary['timing_stats']:
        lines.append("")
        lines.append("Timing statistics:")
        for event, stats in sorted(summary['timing_stats'].items()):
            avg_ms = stats['avg_us'] / 1000
            total_ms = stats['total_us'] / 1000
            lines.append(f"  {event}:")
            lines.append(f"    count: {stats['count']}")
            lines.append(f"    avg: {avg_ms:.3f}ms")
            lines.append(f"    total: {total_ms:.3f}ms")
            lines.append(f"    min: {stats['min_us'] / 1000:.3f}ms")
            lines.append(f"    max: {stats['max_us'] / 1000:.3f}ms")

    lines.append("=" * 60)
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(
        description='Parse structured debug output from VibeSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--filter', '-f',
        dest='category',
        help='Filter by category (optimizer, execution, index, dml, profile)'
    )
    parser.add_argument(
        '--filter-event', '-e',
        dest='event_name',
        help='Filter by event name'
    )
    parser.add_argument(
        '--summary', '-s',
        action='store_true',
        help='Show summary statistics'
    )
    parser.add_argument(
        '--json', '-j',
        action='store_true',
        help='Output results as JSON'
    )

    args = parser.parse_args()

    # Read from stdin
    events = list(filter_events(
        parse_debug_lines(sys.stdin),
        category=args.category,
        event_name=args.event_name
    ))

    if args.summary:
        summary = compute_summary(events)
        if args.json:
            print(json.dumps(summary, indent=2))
        else:
            print(format_summary_human_readable(summary))
    else:
        if args.json:
            print(json.dumps(events, indent=2))
        else:
            for event in events:
                print(format_human_readable(event))
                print()


if __name__ == '__main__':
    main()
