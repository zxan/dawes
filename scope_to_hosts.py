"""Reduce the database to per-service scope: one row per onion host.

The urls/pings tables accumulated path-level URLs (many pages per host). We now
measure per-service, so every URL outside links_hosts.txt is archived away.

Refuses to run while a pinger is probing a path-level link file, because
rebuild_url_stats() re-creates urls rows from pings: the cleanup would be
undone within one cycle.
"""
import argparse
import shutil
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from database import connect_db, ensure_schema, rebuild_url_stats
from url_utils import normalize_onion_url

DB_FILE = "onions.db"
HOSTS_FILE = "links_hosts.txt"
ARCHIVE_FILE = "onions_pathlevel_archive.db"


def load_hosts(path):
    hosts = set()
    for line in Path(path).read_text(encoding="utf-8", errors="replace").splitlines():
        url = normalize_onion_url(line)
        if url:
            hosts.add(url)
    if not hosts:
        raise SystemExit(f"No valid v3 onion URLs in {path}")
    return sorted(hosts)


def conflicting_pinger(hosts_file):
    """Return the cmdline of a pinger probing a different link file, if any."""
    try:
        out = subprocess.run(
            ["pgrep", "-af", "pinger.py"], capture_output=True, text=True, check=False
        ).stdout
    except FileNotFoundError:
        return None
    for line in out.splitlines():
        if "pinger.py" not in line or "pgrep" in line:
            continue
        if f"--links {hosts_file}" in line:
            continue  # already scoped to hosts; its writes stay in scope
        return line.strip()
    return None


def archive(db_path, archive_path):
    archive_path = Path(archive_path)
    if archive_path.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        rotated = archive_path.with_name(f"{archive_path.name}.{stamp}")
        shutil.move(str(archive_path), rotated)
        print(f"  rotated existing archive -> {rotated.name}")
    # Online backup API: consistent even with a concurrent writer.
    source = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    destination = sqlite3.connect(archive_path)
    source.backup(destination)
    destination.close()
    source.close()
    print(f"  archived {db_path} -> {archive_path}")


def scope(db_path=DB_FILE, hosts_file=HOSTS_FILE, archive_path=ARCHIVE_FILE,
          force=False, skip_archive=False, lock_timeout_ms=30000):
    hosts = load_hosts(hosts_file)
    blocker = None if force else conflicting_pinger(hosts_file)
    if blocker:
        raise SystemExit(
            "Refusing to run: a pinger is probing a different link file, and "
            "rebuild_url_stats() re-creates urls rows from pings, so this "
            f"cleanup would be undone within one cycle.\n  {blocker}\n"
            f"Repoint that pinger at {hosts_file} first, or pass --force."
        )

    if not skip_archive:
        archive(db_path, archive_path)

    connection = connect_db(db_path)
    ensure_schema(connection)
    # A running pinger commits only every 100 pings, so it can hold the write
    # lock for minutes. Wait it out rather than interrupting a live cycle.
    connection.execute(f"PRAGMA busy_timeout = {int(lock_timeout_ms)}")
    placeholders = ",".join("?" * len(hosts))
    try:
        # Start the write transaction cold. In WAL, upgrading an already-open
        # read snapshot to a write lock returns SQLITE_BUSY immediately and
        # busy_timeout cannot retry it; waiting for a plain write lock can be
        # retried, so take the lock before reading anything.
        connection.commit()
        connection.execute("BEGIN IMMEDIATE")
        before_urls = connection.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
        before_pings = connection.execute("SELECT COUNT(*) FROM pings").fetchone()[0]
        dropped_pings = connection.execute(
            f"DELETE FROM pings WHERE url NOT IN ({placeholders})", hosts
        ).rowcount
        dropped_urls = connection.execute(
            f"DELETE FROM urls WHERE url NOT IN ({placeholders})", hosts
        ).rowcount
        # Hosts we intend to track but have never recorded: keep them visible
        # as tracked-but-unprobed rather than silently absent.
        first_seen = datetime.now(timezone.utc).isoformat()
        added = connection.executemany(
            "INSERT OR IGNORE INTO urls(url, first_seen) VALUES (?, ?)",
            ((url, first_seen) for url in hosts),
        ).rowcount
        # cycles.alive/total were counted over the old path-level URLs, so they
        # now overstate the run: the chart reads cycles.alive and would plot a
        # spike that no longer exists. Recount from the surviving pings.
        # Completed cycles only: an in-flight cycle has not written all its
        # pings yet, and recounting its total mid-run would understate it.
        recounted = connection.execute(
            """
            UPDATE cycles SET
                total = (SELECT COUNT(*) FROM pings p WHERE p.cycle_id = cycles.cycle_id),
                alive = (SELECT COUNT(*) FROM pings p
                         WHERE p.cycle_id = cycles.cycle_id AND p.status = 'alive')
            WHERE status = 'completed'
            """
        ).rowcount
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    rebuild_url_stats(connection)

    after_urls = connection.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
    after_pings = connection.execute("SELECT COUNT(*) FROM pings").fetchone()[0]
    connection.close()
    print(f"  urls  {before_urls} -> {after_urls}  (dropped {dropped_urls}, added {added})")
    print(f"  pings {before_pings} -> {after_pings}  (dropped {dropped_pings})")
    print(f"  recounted alive/total on {recounted} completed cycles")
    if after_urls != len(hosts):
        raise SystemExit(f"Expected {len(hosts)} urls, got {after_urls}")
    print(f"  scoped to {after_urls} services")
    return after_urls


def main():
    parser = argparse.ArgumentParser(description="Scope the database to per-service URLs")
    parser.add_argument("--db", default=DB_FILE)
    parser.add_argument("--hosts", default=HOSTS_FILE)
    parser.add_argument("--archive", default=ARCHIVE_FILE)
    parser.add_argument("--skip-archive", action="store_true")
    parser.add_argument("--force", action="store_true",
                        help="run even if a pinger is probing another link file")
    parser.add_argument("--lock-timeout-ms", type=int, default=30000,
                        help="how long to wait for the write lock")
    args = parser.parse_args()
    scope(args.db, args.hosts, args.archive, args.force, args.skip_archive,
          args.lock_timeout_ms)


if __name__ == "__main__":
    main()
