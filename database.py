import sqlite3
from pathlib import Path


def connect_db(path, timeout=30):
    connection = sqlite3.connect(Path(path), timeout=timeout)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    # WAL lets dashboard reads continue while the pinger writes.
    try:
        connection.execute("PRAGMA journal_mode = WAL")
    except sqlite3.OperationalError:
        pass
    return connection


def init_db(path):
    """Create/migrate the schema once, at startup, not on every request."""
    connection = connect_db(path)
    ensure_schema(connection)
    connection.close()


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
    # Build one grouped stats table, then apply it. This avoids running a
    # correlated subquery three times for every URL.
    connection.execute("DROP TABLE IF EXISTS temp._url_stats")
    connection.execute(
        """
        CREATE TEMP TABLE _url_stats AS
        SELECT p.url,
               COUNT(*) AS total_pings,
               SUM(CASE WHEN p.status = 'alive' THEN 1 ELSE 0 END) AS alive_pings,
               MAX(CASE WHEN p.status = 'alive' THEN p.timestamp END) AS last_alive
        FROM pings p
        JOIN cycles c ON c.cycle_id = p.cycle_id AND c.status = 'completed'
        GROUP BY p.url
        """
    )
    connection.execute("CREATE UNIQUE INDEX temp.idx_url_stats ON _url_stats(url)")
    connection.execute("UPDATE urls SET total_pings = 0, alive_pings = 0, last_alive = NULL")
    connection.execute(
        """
        UPDATE urls
        SET total_pings = s.total_pings,
            alive_pings = s.alive_pings,
            last_alive = s.last_alive
        FROM _url_stats s
        WHERE urls.url = s.url
        """
    )
    connection.execute("DROP TABLE temp._url_stats")
    connection.commit()
