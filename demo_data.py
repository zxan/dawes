"""Deterministic demo dataset and fast simulated cycles.

Real onion pings take hours per full cycle. For a demo we seed a plausible
history (completed cycles plus an explicit gap) and can then append new
simulated cycles every few seconds so the dashboard visibly changes.

Everything here writes to DEMO_DB_FILE and hard-refuses any path named
onions.db, including one passed explicitly.
"""
import random
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from database import connect_db, ensure_schema, rebuild_url_stats

CYCLES = 12
CYCLE_HOURS = 6
# The real measurement is per-service: 533 distinct onion services.
DEFAULT_URL_COUNT = 533
CHARS = "abcdefghijklmnopqrstuvwxyz234567"
DEMO_DB_FILE = "onions_demo.db"
# Fabricated rows must never reach a database holding real observations.
PROTECTED_DB_NAMES = {"onions.db"}


def _demo_db_path(db_path=None):
    """Resolve a demo target, refusing a live collection database outright."""
    path = Path(db_path) if db_path is not None else Path(DEMO_DB_FILE)
    names = {path.name}
    if path.is_symlink() or path.exists():
        names.add(path.resolve().name)
    blocked = names & PROTECTED_DB_NAMES
    if blocked:
        raise SystemExit(
            f"demo_data refuses to write {sorted(blocked)[0]} ({path}): that name is "
            f"reserved for real collected observations, which simulated cycles would "
            f"destroy. Demo data goes to {DEMO_DB_FILE}."
        )
    return path


def _guard_connection(connection):
    """Refuse an already-open connection to a protected database."""
    for row in connection.execute("PRAGMA database_list"):
        schema, filename = row[1], row[2]
        if schema == "main" and filename:
            _demo_db_path(filename)


def _now():
    return datetime.now(timezone.utc)


def _synthetic_urls(count=DEFAULT_URL_COUNT, rng=None):
    rng = rng or random.Random(1337)
    return [
        "http://" + "".join(rng.choice(CHARS) for _ in range(56)) + ".onion"
        for _ in range(count)
    ]


def seed_demo(db_path=None, cycles=CYCLES, url_count=DEFAULT_URL_COUNT, backup=True):
    """Replace db_path with a realistic multi-cycle churn history.

    Always regenerates the URL set: reseeding must be able to change the number
    of services, and reusing whatever the file already held made that silent.
    """
    db_path = _demo_db_path(db_path)
    urls = _synthetic_urls(url_count)

    if backup and db_path.exists():
        stamp = _now().strftime("%Y%m%d-%H%M%S")
        shutil.copy2(db_path, db_path.with_name(f"{db_path.name}.bak-{stamp}"))

    for suffix in ("", "-wal", "-shm", "-journal"):
        target = Path(str(db_path) + suffix)
        if target.exists():
            target.unlink()

    connection = connect_db(db_path)
    ensure_schema(connection)

    rng = random.Random(20261030)
    start = _now() - timedelta(hours=CYCLE_HOURS * (cycles - 1))
    gap_at = max(1, cycles // 2)

    # Each URL gets a lifetime profile. Roughly: 30% never alive, 40% always
    # alive, the rest die (and sometimes revive) part-way through the window.
    profiles = {}
    for url in urls:
        roll = rng.random()
        if roll < 0.30:
            profiles[url] = ("never", None, None)
        elif roll < 0.70:
            profiles[url] = ("always", None, None)
        else:
            die_at = rng.randint(1, cycles - 2)
            revive_at = die_at + rng.randint(1, 3) if rng.random() < 0.2 else None
            profiles[url] = ("mortal", die_at, revive_at)

    first_seen = start.isoformat()
    connection.executemany(
        "INSERT OR IGNORE INTO urls(url, first_seen) VALUES (?, ?)",
        ((url, first_seen) for url in urls),
    )
    connection.commit()

    completed = 0
    for i in range(cycles):
        started = start + timedelta(hours=CYCLE_HOURS * i)
        ended = started + timedelta(minutes=3)
        if i == gap_at:
            connection.execute(
                """
                INSERT INTO cycles(started_at, ended_at, total, alive, status)
                VALUES (?, ?, ?, 0, 'aborted')
                """,
                (started.isoformat(), ended.isoformat(), len(urls)),
            )
            connection.commit()
            continue

        rows = []
        alive = 0
        for url in urls:
            kind, die_at, revive_at = profiles[url]
            if kind == "never":
                status = "dead"
            elif kind == "always":
                status = "alive"
            else:
                status = "alive" if i < die_at else "dead"
                if revive_at is not None and i >= revive_at:
                    status = "alive"
            alive += status == "alive"
            rows.append((url, status, 200 if status == "alive" else None,
                         started.isoformat(), None))

        cursor = connection.execute(
            """
            INSERT INTO cycles(started_at, ended_at, total, alive, status)
            VALUES (?, ?, ?, ?, 'completed')
            """,
            (started.isoformat(), ended.isoformat(), len(urls), alive),
        )
        cycle_id = cursor.lastrowid
        connection.executemany(
            """
            INSERT INTO pings(url, status, status_code, timestamp, cycle_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            ((u, s, c, t, cycle_id) for u, s, c, t, _ in rows),
        )
        connection.commit()
        completed += 1

    rebuild_url_stats(connection)
    connection.execute(
        "INSERT OR IGNORE INTO urls(url, first_seen) VALUES (?, ?)",
        (urls[0], first_seen),
    )
    connection.commit()
    connection.close()
    return {
        "urls": len(urls),
        "completed_cycles": completed,
        "aborted_cycles": 1,
    }


def _prune_demo_cycles(connection, keep=48):
    """Keep only the most recent cycles so a long-running demo stays small."""
    connection.execute(
        """
        DELETE FROM pings WHERE cycle_id NOT IN (
            SELECT cycle_id FROM cycles
            ORDER BY datetime(started_at) DESC, cycle_id DESC
            LIMIT ?
        )
        """,
        (keep,),
    )
    connection.execute(
        """
        DELETE FROM cycles WHERE cycle_id NOT IN (
            SELECT cycle_id FROM cycles
            ORDER BY datetime(started_at) DESC, cycle_id DESC
            LIMIT ?
        )
        """,
        (keep,),
    )


def simulate_cycle(connection, urls):
    """Append one fast, plausible cycle using the previous statuses."""
    _guard_connection(connection)
    previous = {
        row["url"]: row["status"]
        for row in connection.execute(
            """
            SELECT p.url, p.status
            FROM pings p
            WHERE p.cycle_id = (
                SELECT cycle_id FROM cycles
                WHERE status = 'completed'
                ORDER BY datetime(started_at) DESC, cycle_id DESC
                LIMIT 1
            )
            """
        )
    }
    rng = random.Random()
    started = _now()
    ended = started + timedelta(seconds=1)
    rows = []
    alive = 0
    for url in urls:
        was_alive = previous.get(url) == "alive"
        # Churn: a few alive services die, a slightly smaller number revive.
        if was_alive:
            now_alive = rng.random() >= 0.035
        else:
            now_alive = rng.random() < 0.045
        status = "alive" if now_alive else "dead"
        alive += now_alive
        rows.append((url, status, 200 if now_alive else None, started.isoformat()))

    cursor = connection.execute(
        """
        INSERT INTO cycles(started_at, ended_at, total, alive, status)
        VALUES (?, ?, ?, ?, 'completed')
        """,
        (started.isoformat(), ended.isoformat(), len(urls), alive),
    )
    cycle_id = cursor.lastrowid
    connection.executemany(
        """
        INSERT INTO pings(url, status, status_code, timestamp, cycle_id)
        VALUES (?, ?, ?, ?, ?)
        """,
        ((u, s, c, t, cycle_id) for u, s, c, t in rows),
    )
    connection.commit()
    _prune_demo_cycles(connection)
    connection.commit()
    rebuild_url_stats(connection)
    return True


def ensure_demo_data(db_path=None, minimum_cycles=4):
    """Seed the demo history only when the database is too thin."""
    db_path = _demo_db_path(db_path)
    connection = connect_db(db_path)
    ensure_schema(connection)
    completed = connection.execute(
        "SELECT COUNT(*) FROM cycles WHERE status = 'completed'"
    ).fetchone()[0]
    connection.close()
    if completed < minimum_cycles:
        return seed_demo(db_path)
    return None
