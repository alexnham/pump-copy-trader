import json
import sqlite3
import unittest

from report_db_timings import collect, summarize


class ReportTests(unittest.TestCase):
    def test_filters_and_missing_metrics_are_not_zero_filled(self):
        connection = sqlite3.connect(":memory:")
        connection.executescript(
            "CREATE TABLE source_transactions(signature TEXT, observed_at INTEGER, timings_json TEXT);"
            "CREATE TABLE copy_attempts(source_signature TEXT, execution_target TEXT, status TEXT);"
        )
        rows = [
            ("first", "mainnet", "landed", {"database": {"persist_signed": {"elapsed_us": 100, "calls": 1}}, "db_pre_send_us": 200, "receipt_to_send_start_ms": 10}),
            ("second", "mainnet", "landed", {"database": {"persist_signed": {"elapsed_us": 300, "calls": 2}}}),
            ("legacy", "mainnet", "landed", {"route_ms": 25}),
            ("failure", "mainnet", "failed", {"database": {"persist_signed": {"elapsed_us": 9000, "calls": 1}}}),
            ("other", "surfpool", "landed", {"database": {"persist_signed": {"elapsed_us": 9000, "calls": 1}}}),
        ]
        for index, (signature, target, status, values) in enumerate(rows):
            connection.execute("INSERT INTO source_transactions VALUES (?, ?, ?)", (signature, index, json.dumps(values)))
            connection.execute("INSERT INTO copy_attempts VALUES (?, ?, ?)", (signature, target, status))
        groups, legacy = collect(connection, 500, "mainnet", "landed")
        self.assertEqual(legacy, 1)
        self.assertEqual(summarize(groups["landed"]["db.persist_signed"]), (2, 200, 300))
        self.assertEqual(groups["landed"]["db_pre_send_us"], [200])
        self.assertEqual(groups["landed"]["receipt_to_send_start"], [10000])
        all_groups, _ = collect(connection, 500, "mainnet", "all")
        self.assertEqual(set(all_groups), {"landed", "failed"})
        limited, _ = collect(connection, 1, "mainnet", "landed")
        self.assertEqual(limited, {})  # Latest matching row is legacy.

    def test_p95_is_nearest_rank(self):
        self.assertEqual(summarize(list(range(1, 101))), (100, 50.5, 95))


if __name__ == "__main__":
    unittest.main()
