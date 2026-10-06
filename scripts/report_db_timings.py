#!/usr/bin/env python3
"""Read-only latency summary of the copy journal; never connects to Solana."""
import argparse
import json
import math
from pathlib import Path
import sqlite3
import statistics


def collect(connection, limit, target, status):
    clauses = ["s.timings_json IS NOT NULL", "a.execution_target = ?"]
    parameters = [target]
    if status != "all":
        clauses.append("a.status = ?")
        parameters.append(status)
    parameters.append(limit)
    rows = connection.execute(
        "SELECT s.timings_json, a.status FROM source_transactions s "
        "JOIN copy_attempts a ON a.source_signature = s.signature WHERE "
        + " AND ".join(clauses)
        + " ORDER BY s.observed_at DESC, s.rowid DESC LIMIT ?",
        parameters,
    )
    groups = {}
    legacy = 0
    for raw, outcome in rows:
        timings = json.loads(raw)
        database = timings.get("database")
        if not isinstance(database, dict):
            legacy += 1
            continue
        metrics = groups.setdefault(outcome, {})
        for operation, timing in database.items():
            metrics.setdefault(f"db.{operation}", []).append(timing["elapsed_us"])
        for field in ("db_pre_send_us", "db_post_send_us", "db_total_us"):
            if field in timings:
                metrics.setdefault(field, []).append(timings[field])
        if "receipt_to_send_start_ms" in timings:
            metrics.setdefault("receipt_to_send_start", []).append(
                timings["receipt_to_send_start_ms"] * 1000
            )
    return groups, legacy


def summarize(values):
    ordered = sorted(values)
    return len(ordered), statistics.median(ordered), ordered[math.ceil(len(ordered) * 0.95) - 1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("copy_trader.sqlite"))
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--target", choices=("mainnet",), default="mainnet")
    parser.add_argument("--status", choices=("landed", "failed", "unknown", "submitting", "prepared", "all"), default="landed")
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit must be positive")
    try:
        with sqlite3.connect(args.db.resolve().as_uri() + "?mode=ro", uri=True) as connection:
            groups, legacy = collect(connection, args.limit, args.target, args.status)
    except (sqlite3.Error, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"Cannot read timing data: {error}\n")
    if not groups:
        print("No per-operation timing samples match these filters.")
    for status, metrics in sorted(groups.items()):
        print(f"\nTarget: {args.target}; status: {status}; durations in microseconds")
        print(f"{'Metric':34s} {'Samples':>8s} {'Median':>12s} {'p95':>12s}")
        for metric, values in sorted(metrics.items()):
            count, median, p95 = summarize(values)
            print(f"{metric:34s} {count:8d} {median:12.1f} {p95:12.1f}")
    print(f"\nHistorical timing rows without database measurements skipped: {legacy}")
    print("Per-operation samples are summed duration per attempt, including repeated calls.")
    print("record_timings duration is log-only; stored totals exclude that final write.")
    print("Database durations overlap the existing pipeline stages; do not add them twice.")


if __name__ == "__main__":
    main()
