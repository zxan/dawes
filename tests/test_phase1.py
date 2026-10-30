import argparse
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as app_module
from database import connect_db, ensure_schema
from pinger import default_links_file, run_cycle
from url_utils import normalize_onion_url


HOST_A = "a" * 56 + ".onion"
HOST_B = "b" * 56 + ".onion"


class UrlNormalizationTests(unittest.TestCase):
    def test_normalizes_valid_v3_url(self):
        url = normalize_onion_url(f"HTTP://{HOST_A}/path/#fragment")
        self.assertEqual(url, f"http://{HOST_A}/path")

    def test_resolves_relative_url(self):
        url = normalize_onion_url("../next/", f"http://{HOST_A}/one/page")
        self.assertEqual(url, f"http://{HOST_A}/next")

    def test_rejects_v2_and_clearnet_hosts(self):
        self.assertIsNone(normalize_onion_url("http://abcdefghijklmnop.onion"))
        self.assertIsNone(normalize_onion_url("https://example.com"))


class DatabaseMigrationTests(unittest.TestCase):
    def test_legacy_partial_cycle_becomes_aborted(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = sqlite3.connect(Path(directory) / "legacy.db")
            connection.executescript(
                """
                CREATE TABLE urls (
                    url TEXT PRIMARY KEY, first_seen TEXT, last_alive TEXT,
                    total_pings INTEGER DEFAULT 0, alive_pings INTEGER DEFAULT 0
                );
                CREATE TABLE pings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT, status TEXT,
                    status_code INTEGER, timestamp TEXT
                );
                """
            )
            urls = [f"http://{HOST_A}", f"http://{HOST_B}"]
            connection.executemany(
                "INSERT INTO urls(url, first_seen) VALUES (?, '2026-01-01')",
                ((url,) for url in urls),
            )
            connection.execute(
                "INSERT INTO pings(url, status, timestamp) VALUES (?, 'alive', 't1')",
                (urls[0],),
            )
            connection.executemany(
                "INSERT INTO pings(url, status, timestamp) VALUES (?, ?, 't2')",
                [(urls[0], "alive"), (urls[1], "dead")],
            )
            connection.commit()
            connection.close()

            connection = connect_db(Path(directory) / "legacy.db")
            ensure_schema(connection)
            cycles = connection.execute(
                "SELECT status, total, alive FROM cycles ORDER BY cycle_id"
            ).fetchall()
            self.assertEqual([row["status"] for row in cycles], ["aborted", "completed"])
            self.assertEqual([row["total"] for row in cycles], [2, 2])
            stats = connection.execute(
                "SELECT total_pings, alive_pings FROM urls ORDER BY url"
            ).fetchall()
            self.assertEqual([tuple(row) for row in stats], [(1, 1), (1, 0)])
            connection.close()


class PingerCycleTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.connection = connect_db(Path(self.directory.name) / "test.db")
        ensure_schema(self.connection)
        self.urls = [f"http://{HOST_A}", f"http://{HOST_B}"]
        self.connection.executemany(
            "INSERT INTO urls(url, first_seen) VALUES (?, '2026-01-01')",
            ((url,) for url in self.urls),
        )
        self.connection.commit()

    def tearDown(self):
        self.connection.close()
        self.directory.cleanup()

    @patch("pinger.check_tor", return_value=(False, "Tor unavailable"))
    def test_failed_health_check_records_aborted_gap(self, _health):
        self.assertFalse(run_cycle(self.connection, self.urls, workers=1))
        cycle = self.connection.execute("SELECT * FROM cycles").fetchone()
        self.assertEqual(cycle["status"], "aborted")
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM pings").fetchone()[0], 0
        )

    @patch("pinger.ping", side_effect=[("alive", 200), ("dead", None)])
    @patch("pinger.check_tor", return_value=(True, "Tor ready"))
    def test_complete_cycle_updates_stats(self, _health, _ping):
        self.assertTrue(run_cycle(self.connection, self.urls, workers=1))
        cycle = self.connection.execute("SELECT * FROM cycles").fetchone()
        self.assertEqual((cycle["status"], cycle["total"], cycle["alive"]), ("completed", 2, 1))
        stats = self.connection.execute(
            "SELECT total_pings, alive_pings FROM urls ORDER BY url"
        ).fetchall()
        self.assertEqual([tuple(row) for row in stats], [(1, 1), (1, 0)])
        with patch("app.DB_FILE", str(Path(self.directory.name) / "test.db")):
            response = app_module.summary()
        self.assertEqual(json.loads(response.body)["current_alive"], 1)


class AppEntryPointTests(unittest.TestCase):
    def test_default_links_file_is_project_output(self):
        self.assertEqual(default_links_file(), Path("links.txt"))

    def test_dashboard_response_is_available(self):
        response = app_module.index()
        # Anchor on structure, not heading prose, so a restyle does not fail here.
        self.assertIn(b'id="chart"', response.body)
        self.assertIn(b'id="url-body"', response.body)

    def test_default_command_starts_server(self):
        with patch("app.parse_args", return_value=argparse.Namespace(command=None)):
            with patch("app.start_server") as start_server:
                app_module.main()
        start_server.assert_called_once_with(
            port=8000,
            no_pinger=False,
            interval=app_module.DEFAULT_INTERVAL,
            demo=False,
            db=None,  # no --db: the served database stays the default
        )

    @patch("app.crawler_module.OnionCrawler")
    @patch("app.check_tor", return_value=(False, "Tor unavailable"))
    def test_crawl_refuses_to_run_without_tor(self, _health, crawler):
        with self.assertRaises(SystemExit):
            app_module.run_crawl_cli(seed=[f"http://{HOST_A}"])
        crawler.assert_not_called()

    def test_sleep_gap_handles_legacy_naive_utc_timestamps(self):
        with tempfile.TemporaryDirectory() as directory:
            connection = connect_db(Path(directory) / "gaps.db")
            ensure_schema(connection)
            connection.executemany(
                """
                INSERT INTO cycles(started_at, ended_at, total, alive, status)
                VALUES (?, ?, 1, 1, 'completed')
                """,
                [
                    ("2026-10-28T10:00:00", "2026-10-28T10:05:00"),
                    (
                        "2026-10-29T12:00:00+00:00",
                        "2026-10-29T12:05:00+00:00",
                    ),
                ],
            )
            connection.commit()
            manager = app_module.PingerManager(interval=3600)
            manager._mark_sleep_gap(connection)
            gap = connection.execute(
                "SELECT started_at, ended_at, status FROM cycles WHERE status='aborted'"
            ).fetchone()
            self.assertEqual(
                tuple(gap),
                (
                    "2026-10-28T10:05:00",
                    "2026-10-29T12:00:00+00:00",
                    "aborted",
                ),
            )
            connection.close()


if __name__ == "__main__":
    unittest.main()
