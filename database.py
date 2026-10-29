import sqlite3
from pathlib import Path


def connect_db(path):
    connection = sqlite3.connect(Path(path))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _columns(connection, table):
    return {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}


def ensure_schema(connection):
    """Create the current schema and migrate the legacy timestamp-only pings."""
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS urls (
            url TEXT PRIMARY KEY,
            first_seen TEXT NOT NULL,
            last_alive TEXT,
            total_pings INTEGER NOT NULL DEFAULT 0,
            alive_pings INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS cycles (
            cycle_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            total INTEGER NOT NULL DEFAULT 0,
            alive INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL CHECK(status IN ('completed', 'aborted'))
        );

        CREATE TABLE IF NOT EXISTS pings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            status TEXT NOT NULL,
            status_code INTEGER,
            timestamp TEXT NOT NULL,
            cycle_id INTEGER REFERENCES cycles(cycle_id)
        );
        """
    )

    if "cycle_id" not in _columns(connection, "pings"):
        connection.execute("ALTER TABLE pings ADD COLUMN cycle_id INTEGER")

    _migrate_legacy_cycles(connection)
    connection.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_pings_cycle ON pings(cycle_id);
        CREATE INDEX IF NOT EXISTS idx_pings_url_cycle ON pings(url, cycle_id);
        """
    )
    connection.commit()


def _migrate_legacy_cycles(connection):
    legacy = connection.execute(
        """
        SELECT timestamp, COUNT(*) AS ping_count,
               SUM(CASE WHEN status = 'alive' THEN 1 ELSE 0 END) AS alive
        FROM pings
        WHERE cycle_id IS NULL
        GROUP BY timestamp
        ORDER BY timestamp
        """
    ).fetchall()
    if not legacy:
        return

    known_urls = connection.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
    largest_cycle = max(row["ping_count"] for row in legacy)
    expected_total = max(known_urls, largest_cycle)

    for row in legacy:
        completed = expected_total > 0 and row["ping_count"] == expected_total
        cursor = connection.execute(
            """
            INSERT INTO cycles(started_at, ended_at, total, alive, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                row["timestamp"],
                row["timestamp"],
                expected_total if not completed else row["ping_count"],
                row["alive"],
                "completed" if completed else "aborted",
            ),
        )
        connection.execute(
            "UPDATE pings SET cycle_id = ? WHERE timestamp = ? AND cycle_id IS NULL",
            (cursor.lastrowid, row["timestamp"]),
        )

    rebuild_url_stats(connection)


def rebuild_url_stats(connection):
    """Recalculate URL counters using completed cycles only."""
    connection.execute(
        """
        INSERT OR IGNORE INTO urls(url, first_seen)
        SELECT url, MIN(timestamp) FROM pings GROUP BY url
        """
    )
    connection.execute(
        """
        UPDATE urls
        SET total_pings = COALESCE((
                SELECT COUNT(*)
                FROM pings p JOIN cycles c ON c.cycle_id = p.cycle_id
                WHERE p.url = urls.url AND c.status = 'completed'
            ), 0),
            alive_pings = COALESCE((
                SELECT COUNT(*)
                FROM pings p JOIN cycles c ON c.cycle_id = p.cycle_id
                WHERE p.url = urls.url AND c.status = 'completed'
                  AND p.status = 'alive'
            ), 0),
            last_alive = (
                SELECT MAX(p.timestamp)
                FROM pings p JOIN cycles c ON c.cycle_id = p.cycle_id
                WHERE p.url = urls.url AND c.status = 'completed'
                  AND p.status = 'alive'
            )
        """
    )
