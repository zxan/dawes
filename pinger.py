import argparse
import logging
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

from database import connect_db, ensure_schema, rebuild_url_stats
from tor_utils import DEFAULT_PROXY, check_tor, proxies
from url_utils import normalize_onion_url


DB_FILE = "onions.db"
INTERVAL = 6 * 60 * 60
TIMEOUT = 30
USER_AGENT = "OnionChurnTracker/1.0"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def default_links_file():
    return Path("links.txt")


def load_urls(path):
    urls = set()
    with Path(path).open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            url = normalize_onion_url(line)
            if url:
                urls.add(url)
    return sorted(urls)


def ping(url, proxy=DEFAULT_PROXY, timeout=TIMEOUT):
    try:
        response = requests.get(
            url,
            proxies=proxies(proxy),
            timeout=timeout,
            allow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        return "alive", response.status_code
    except requests.RequestException:
        return "dead", None


def _start_cycle(connection, total):
    cursor = connection.execute(
        """
        INSERT INTO cycles(started_at, total, alive, status)
        VALUES (?, ?, 0, 'aborted')
        """,
        (utc_now(), total),
    )
    connection.commit()
    return cursor.lastrowid


def _abort_cycle(connection, cycle_id, alive=0):
    connection.execute(
        "UPDATE cycles SET ended_at = ?, alive = ?, status = 'aborted' WHERE cycle_id = ?",
        (utc_now(), alive, cycle_id),
    )
    connection.commit()


def run_cycle(connection, urls, proxy=DEFAULT_PROXY, timeout=TIMEOUT, workers=8):
    cycle_id = _start_cycle(connection, len(urls))
    healthy, message = check_tor(proxy, min(timeout, 20))
    logging.info(message)
    if not healthy:
        _abort_cycle(connection, cycle_id)
        logging.error("Cycle %d aborted before pinging", cycle_id)
        return False

    observed_at = utc_now()
    alive = 0
    completed = 0
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(ping, url, proxy, timeout): url for url in urls}
            for future in as_completed(futures):
                url = futures[future]
                status, status_code = future.result()
                alive += status == "alive"
                completed += 1
                connection.execute(
                    """
                    INSERT INTO pings(url, status, status_code, timestamp, cycle_id)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (url, status, status_code, observed_at, cycle_id),
                )
                if completed % 100 == 0:
                    connection.commit()
                    logging.info("Cycle %d: %d/%d complete", cycle_id, completed, len(urls))
        connection.execute(
            """
            UPDATE cycles
            SET ended_at = ?, alive = ?, status = 'completed'
            WHERE cycle_id = ?
            """,
            (utc_now(), alive, cycle_id),
        )
        rebuild_url_stats(connection)
        connection.commit()
        logging.info("Cycle %d completed: %d alive / %d dead", cycle_id, alive, len(urls) - alive)
        return True
    except BaseException:
        _abort_cycle(connection, cycle_id, alive)
        logging.exception("Cycle %d aborted after %d/%d pings", cycle_id, completed, len(urls))
        raise


def parse_args():
    parser = argparse.ArgumentParser(description="Repeatedly ping onion URLs through Tor")
    parser.add_argument("--links", type=Path, default=None)
    parser.add_argument("--db", type=Path, default=Path(DB_FILE))
    parser.add_argument("--interval", type=float, default=INTERVAL, help="seconds")
    parser.add_argument("--timeout", type=float, default=TIMEOUT)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--proxy", default=DEFAULT_PROXY)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    if args.interval <= 0 or args.timeout <= 0 or args.workers < 1:
        raise SystemExit("interval, timeout, and workers must be positive")

    links_path = args.links or default_links_file()
    if not links_path.exists():
        raise SystemExit(f"Links file not found: {links_path}")
    urls = load_urls(links_path)
    if not urls:
        raise SystemExit(f"No valid v3 onion URLs found in {links_path}")

    connection = connect_db(args.db)
    ensure_schema(connection)
    first_seen = utc_now()
    connection.executemany(
        "INSERT OR IGNORE INTO urls(url, first_seen) VALUES (?, ?)",
        ((url, first_seen) for url in urls),
    )
    connection.commit()
    logging.info("Loaded %d unique v3 onion URLs", len(urls))

    stop_event = threading.Event()

    def stop(_signum, _frame):
        logging.info("Shutdown requested; finishing the current request cleanup")
        stop_event.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        while not stop_event.is_set():
            run_cycle(connection, urls, args.proxy, args.timeout, args.workers)
            if args.once:
                break
            logging.info("Next cycle in %.0f seconds", args.interval)
            stop_event.wait(args.interval)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
