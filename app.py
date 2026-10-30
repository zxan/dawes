import argparse
import json
import logging
import subprocess
import threading
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

import crawler as crawler_module
import pinger as pinger_module
import report as report_module
from database import connect_db, ensure_schema, init_db
from demo_data import (DEFAULT_URL_COUNT, DEMO_DB_FILE, ensure_demo_data,
                       seed_demo, simulate_cycle)
from pinger import default_links_file, load_urls, run_cycle
from tor_utils import check_tor
from url_utils import normalize_onion_url


APP_TITLE = "Onion Churn Tracker"
DB_FILE = "onions.db"
LINKS_FILE = "links.txt"
EDGES_FILE = "edges.json"
DEFAULT_INTERVAL = 6 * 60 * 60
DEMO_MODE = False

app = FastAPI(title=APP_TITLE)


class PingerManager:
    def __init__(
        self,
        db_path=DB_FILE,
        links_path=None,
        interval=DEFAULT_INTERVAL,
        timeout=30,
        workers=8,
        simulate=False,
    ):
        self.db_path = Path(db_path)
        self.links_path = Path(links_path) if links_path else default_links_file()
        self.interval = float(interval)
        if self.interval <= 0:
            raise ValueError("interval must be positive")
        self.timeout = timeout
        self.workers = workers
        self.simulate = simulate
        self.stop_event = threading.Event()
        self.thread = None
        self.running = False
        self.lock = threading.Lock()
        self.last_cycle = None
        self.urls_count = 0
        self.log = logging.getLogger("pinger-manager")

    def _now(self):
        return datetime.now(timezone.utc)

    def status(self):
        with self.lock:
            return {
                "running": self.running,
                "urls": self.urls_count,
                "interval": self.interval,
                "mode": "demo" if self.simulate else "live",
                "locked": DEMO_MODE and not self.simulate,
                "last_cycle": self.last_cycle,
            }

    def _mark_sleep_gap(self, connection):
        """If the laptop slept, record the missed window as one aborted gap."""
        rows = connection.execute(
            """
            SELECT cycle_id, started_at, ended_at, status, total
            FROM cycles
            ORDER BY datetime(ended_at) DESC, cycle_id DESC
            LIMIT 2
            """
        ).fetchall()
        if len(rows) < 2:
            return
        previous, current = rows[1], rows[0]
        if previous["status"] != "completed":
            return
        current_started = datetime.fromisoformat(current["started_at"])
        previous_ended = datetime.fromisoformat(previous["ended_at"])
        if current_started.tzinfo is None:
            current_started = current_started.replace(tzinfo=timezone.utc)
        if previous_ended.tzinfo is None:
            previous_ended = previous_ended.replace(tzinfo=timezone.utc)
        gap_seconds = (current_started - previous_ended).total_seconds()
        if gap_seconds > self.interval * 1.5:
            connection.execute(
                """
                INSERT INTO cycles(started_at, ended_at, total, alive, status)
                VALUES (?, ?, ?, 0, 'aborted')
                """,
                (previous["ended_at"], current["started_at"], current["total"]),
            )
            connection.commit()

    def _load_urls_into_db(self, connection, urls):
        now = self._now().isoformat()
        connection.executemany(
            "INSERT OR IGNORE INTO urls(url, first_seen) VALUES (?, ?)",
            ((url, now) for url in urls),
        )
        connection.commit()

    def _run_loop(self):
        self.log.info("Background pinger started")
        connection = None
        try:
            connection = connect_db(self.db_path)
            ensure_schema(connection)
            while not self.stop_event.is_set():
                urls = load_urls(self.links_path)
                self._load_urls_into_db(connection, urls)
                urls = [
                    row["url"]
                    for row in connection.execute("SELECT url FROM urls ORDER BY url")
                ]
                with self.lock:
                    self.urls_count = len(urls)

                try:
                    if self.simulate:
                        success = simulate_cycle(connection, urls)
                    else:
                        success = run_cycle(
                            connection, urls, timeout=self.timeout, workers=self.workers
                        )
                except Exception:  # pinger already aborts the cycle; keep daemon alive
                    self.log.exception("Pinger cycle error")
                    success = False

                self._refresh_last_cycle(connection)
                if success:
                    self._mark_sleep_gap(connection)
                    self._refresh_last_cycle(connection)

                if self.stop_event.wait(self.interval):
                    break
        finally:
            if connection is not None:
                connection.close()
            with self.lock:
                self.running = False
            self.log.info("Background pinger stopped")

    def _refresh_last_cycle(self, connection):
        row = connection.execute(
            """
            SELECT cycle_id, started_at, ended_at, total, alive, status
            FROM cycles ORDER BY datetime(started_at) DESC, cycle_id DESC LIMIT 1
            """
        ).fetchone()
        with self.lock:
            self.last_cycle = dict(row) if row else None

    def start(self):
        with self.lock:
            if self.running:
                return False
            self.running = True
            self.stop_event.clear()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        return True

    def stop(self):
        with self.lock:
            if not self.running:
                return False
            self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=self.timeout + 5)
            if self.thread.is_alive():
                return False
        with self.lock:
            self.running = False
        return True


pinger_manager = PingerManager()


def percentage(part, whole):
    return (part / whole * 100) if whole else 0.0


def conflicting_pinger(db_path):
    """Command line of a pinger.py already writing db_path, if there is one.

    Two pingers on one SQLite file interleave cycles and corrupt the run, so a
    second one must never be started implicitly. Parsing failures fall back to
    pinger.py's own default database, which errs toward reporting a conflict.
    """
    try:
        out = subprocess.run(
            ["pgrep", "-af", "pinger.py"], capture_output=True, text=True, check=False
        ).stdout
    except (OSError, ValueError):
        return None
    try:
        target = Path(db_path).resolve()
    except OSError:
        return None
    for line in out.splitlines():
        if "pinger.py" not in line or "pgrep" in line:
            continue
        tokens = line.split()
        other = pinger_module.DB_FILE
        for index, token in enumerate(tokens):
            if token == "--db" and index + 1 < len(tokens):
                other = tokens[index + 1]
            elif token.startswith("--db="):
                other = token.split("=", 1)[1]
        try:
            if Path(other).resolve() == target:
                return line.strip()
        except OSError:
            continue
    return None


@app.get("/", response_class=HTMLResponse)
def index():
    html = _DASHBOARD_HTML.replace(
        "<!--BANNER-->", _SYNTHETIC_BANNER if DEMO_MODE else ""
    ).replace("<!--DBNAME-->", escape(Path(DB_FILE).name))
    return HTMLResponse(html)


@app.get("/health")
def health():
    return {"status": "ok", "demo": DEMO_MODE}


@app.get("/api/summary")
def summary():
    connection = connect_db(DB_FILE)
    try:
        total = connection.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
        completed = connection.execute(
            "SELECT COUNT(*) FROM cycles WHERE status='completed'"
        ).fetchone()[0]
        aborted = connection.execute(
            "SELECT COUNT(*) FROM cycles WHERE status='aborted'"
        ).fetchone()[0]
        ever_alive = connection.execute(
            "SELECT COUNT(*) FROM urls WHERE alive_pings > 0"
        ).fetchone()[0]
        current_alive = connection.execute(
            """
            SELECT COUNT(*)
            FROM pings
            WHERE status = 'alive'
              AND cycle_id = (
                  SELECT cycle_id FROM cycles
                  WHERE status = 'completed'
                  ORDER BY datetime(started_at) DESC, cycle_id DESC
                  LIMIT 1
              )
            """
        ).fetchone()[0]
        never_alive = connection.execute(
            "SELECT COUNT(*) FROM urls WHERE alive_pings = 0"
        ).fetchone()[0]
        always_alive = connection.execute(
            "SELECT COUNT(*) FROM urls WHERE total_pings > 0 AND alive_pings = total_pings"
        ).fetchone()[0]
        churned = connection.execute(
            """
            SELECT COUNT(DISTINCT alive.url)
            FROM pings alive
            JOIN cycles ac ON ac.cycle_id = alive.cycle_id AND ac.status='completed'
            WHERE alive.status='alive'
              AND EXISTS (
                  SELECT 1 FROM pings dead
                  JOIN cycles dc ON dc.cycle_id = dead.cycle_id AND dc.status='completed'
                  WHERE dead.url=alive.url AND dead.status='dead'
                    AND dc.started_at > ac.started_at
              )
            """
        ).fetchone()[0]
        churned_now = connection.execute(
            """
            SELECT COUNT(*)
            FROM urls u
            JOIN pings p ON p.url = u.url
            WHERE u.alive_pings > 0
              AND p.status = 'dead'
              AND p.cycle_id = (
                  SELECT cycle_id FROM cycles
                  WHERE status = 'completed'
                  ORDER BY datetime(started_at) DESC, cycle_id DESC
                  LIMIT 1
              )
            """
        ).fetchone()[0]
        last_cycle = connection.execute(
            """
            SELECT started_at, total, alive, status
            FROM cycles ORDER BY datetime(started_at) DESC, cycle_id DESC LIMIT 1
            """
        ).fetchone()
        status = pinger_manager.status()
    finally:
        connection.close()

    return JSONResponse(
        {
            "total": total,
            "completed_cycles": completed,
            "aborted_cycles": aborted,
            "current_alive": current_alive,
            "ever_alive": ever_alive,
            "ever_alive_pct": percentage(ever_alive, total),
            "never_alive": never_alive,
            "never_alive_pct": percentage(never_alive, total),
            "always_alive": always_alive,
            "always_alive_pct": percentage(always_alive, total),
            "churned": churned,
            "churned_pct": percentage(churned, total),
            "churned_now": churned_now,
            "churned_now_pct": percentage(churned_now, total),
            "last_cycle": dict(last_cycle) if last_cycle else None,
            "pinger_running": status["running"],
            "demo": DEMO_MODE,
        }
    )


@app.get("/api/cycles")
def cycles():
    connection = connect_db(DB_FILE)
    try:
        rows = connection.execute(
            """
            SELECT cycle_id, started_at, ended_at, total, alive, status
            FROM cycles ORDER BY datetime(started_at), cycle_id
            """
        ).fetchall()
    finally:
        connection.close()
    return JSONResponse([dict(row) for row in rows])


@app.get("/api/urls")
def url_table(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = Query(None),
    sort: str = Query("url"),
    direction: str = Query("asc"),
):
    allowed_sorts = {"url", "first_seen", "last_alive", "total_pings", "alive_pings", "uptime"}
    if sort not in allowed_sorts:
        sort = "url"
    if direction not in {"asc", "desc"}:
        direction = "asc"

    connection = connect_db(DB_FILE)
    try:
        where = ""
        params = []
        if search:
            where = "WHERE url LIKE ?"
            params.append(f"%{search}%")

        order_column = (
            "alive_pings * 1.0 / NULLIF(total_pings, 0)" if sort == "uptime" else sort
        )
        order_sql = f"ORDER BY {order_column} {direction.upper()}, url ASC"

        total = connection.execute(f"SELECT COUNT(*) FROM urls {where}", params).fetchone()[0]

        rows = connection.execute(
            f"""
            SELECT url, first_seen, last_alive, total_pings, alive_pings
            FROM urls {where}
            {order_sql}
            LIMIT ? OFFSET ?
            """,
            params + [page_size, (page - 1) * page_size],
        ).fetchall()

        # Only this page's URLs need a current status, and only their newest
        # completed-cycle ping. The ordering key matches the old Python-side
        # "first row wins" pass, so ids out of step with cycle time still
        # resolve the same way.
        last_status = {}
        page_urls = [row["url"] for row in rows]
        if page_urls:
            placeholders = ",".join("?" * len(page_urls))
            last_status = {
                row["url"]: row["status"]
                for row in connection.execute(
                    f"""
                    SELECT url, status FROM (
                        SELECT p.url, p.status,
                               ROW_NUMBER() OVER (
                                   PARTITION BY p.url
                                   ORDER BY datetime(c.started_at) DESC, p.id DESC
                               ) AS rn
                        FROM pings p
                        JOIN cycles c ON c.cycle_id = p.cycle_id
                        WHERE c.status='completed' AND p.url IN ({placeholders})
                    )
                    WHERE rn = 1
                    """,
                    page_urls,
                ).fetchall()
            }
    finally:
        connection.close()

    items = []
    for row in rows:
        total_pings = row["total_pings"]
        alive_pings = row["alive_pings"]
        uptime = (alive_pings / total_pings * 100) if total_pings else 0.0
        items.append(
            {
                "url": row["url"],
                "first_seen": row["first_seen"],
                "last_alive": row["last_alive"],
                "total_pings": total_pings,
                "alive_pings": alive_pings,
                "uptime_pct": uptime,
                "current_status": last_status.get(row["url"], "unknown"),
            }
        )

    return JSONResponse(
        {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": max(1, (total + page_size - 1) // page_size),
            "items": items,
        }
    )


@app.get("/api/pinger/status")
def pinger_status():
    return JSONResponse(pinger_manager.status())


@app.post("/api/pinger/start")
def pinger_start():
    if DEMO_MODE and not pinger_manager.simulate:
        return JSONResponse(
            {
                "started": False,
                "error": "serving synthetic demo data read-only; a live pinger "
                         "would write real results into the demo database",
                **pinger_manager.status(),
            },
            status_code=409,
        )
    blocker = conflicting_pinger(pinger_manager.db_path)
    if blocker:
        return JSONResponse(
            {
                "started": False,
                "error": f"another pinger is already writing "
                         f"{pinger_manager.db_path}: {blocker}",
                **pinger_manager.status(),
            },
            status_code=409,
        )
    ok = pinger_manager.start()
    return JSONResponse({"started": ok, **pinger_manager.status()})


@app.post("/api/pinger/stop")
def pinger_stop():
    ok = pinger_manager.stop()
    return JSONResponse({"stopped": ok, **pinger_manager.status()})


@app.get("/api/graph")
def graph():
    edges_path = Path(EDGES_FILE)
    if not edges_path.exists():
        return JSONResponse({"nodes": [], "edges": []})
    data = json.loads(edges_path.read_text(encoding="utf-8"))
    nodes = set(data.keys())
    targets = {t for targets_list in data.values() for t in targets_list}
    nodes |= targets
    edges = [
        {"source": s, "target": t}
        for s, targets_list in data.items()
        for t in targets_list
    ]
    return JSONResponse({"nodes": sorted(nodes), "edges": edges[:5000]})


def run_crawl_cli(seed=None, depth=3, workers=8, delay=0.25, timeout=30,
                  links=LINKS_FILE, edges=EDGES_FILE):
    if depth < 1 or workers < 1 or delay < 0 or timeout <= 0:
        raise SystemExit("depth/workers/timeout must be positive and delay cannot be negative")
    seeds = seed or [crawler_module.DEFAULT_SEED]
    seeds = [normalize_onion_url(s) for s in seeds]
    if any(s is None for s in seeds):
        raise SystemExit("Invalid seed URL")
    healthy, message = check_tor(crawler_module.DEFAULT_PROXY, min(timeout, 20))
    logging.info(message)
    if not healthy:
        raise SystemExit("Refusing to crawl without a verified Tor connection")
    crawler = crawler_module.OnionCrawler(crawler_module.DEFAULT_PROXY, timeout, workers, delay)
    urls, page_edges = crawler.crawl(sorted(set(seeds)), depth)
    crawler_module.write_outputs(urls, page_edges, links, edges)
    connection = connect_db(DB_FILE)
    ensure_schema(connection)
    now = datetime.now(timezone.utc).isoformat()
    connection.executemany(
        "INSERT OR IGNORE INTO urls(url, first_seen) VALUES (?, ?)",
        ((url, now) for url in urls),
    )
    connection.commit()
    connection.close()
    logging.info("Crawl imported: %d URLs into %s", len(urls), DB_FILE)


def run_report_cli():
    report_module.report(DB_FILE)


def run_demo_cli(cycles=12, urls=DEFAULT_URL_COUNT):
    global DB_FILE
    DB_FILE = DEMO_DB_FILE
    result = seed_demo(DB_FILE, cycles=cycles, url_count=urls)
    logging.info(
        "Demo database ready: %(urls)d URLs, %(completed_cycles)d completed cycles, "
        "%(aborted_cycles)d gap",
        result,
    )
    report_module.report(DB_FILE)


def start_server(port=8000, no_pinger=False, interval=DEFAULT_INTERVAL, demo=False,
                 db=None):
    import uvicorn

    global DEMO_MODE, DB_FILE
    if db:
        DB_FILE = db
    if demo:
        # Demo cycles are fabricated; keep them off the real collection DB.
        DB_FILE = DEMO_DB_FILE
    # Anything served out of the demo database is synthetic, however we got here.
    DEMO_MODE = demo or Path(DB_FILE).name == DEMO_DB_FILE
    # The pinger must follow the served database, never the default, or it would
    # write to onions.db while the dashboard reads something else.
    pinger_manager.db_path = Path(DB_FILE)
    pinger_manager.interval = interval
    pinger_manager.simulate = demo
    init_db(DB_FILE)
    if demo:
        seeded = ensure_demo_data(DB_FILE)
        if seeded:
            logging.info("Seeded demo history: %s", seeded)
    if not no_pinger:
        blocker = conflicting_pinger(DB_FILE)
        if blocker:
            raise SystemExit(
                f"Refusing to start a background pinger: another pinger is already "
                f"writing {DB_FILE}, and two writers corrupt the run.\n  {blocker}\n"
                f"Serve read-only with:  python app.py server --no-pinger"
                + (f" --db {db}" if db else "")
            )
        pinger_manager.start()
    try:
        uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")
    finally:
        pinger_manager.stop()


def parse_args():
    parser = argparse.ArgumentParser(description=APP_TITLE)
    sub = parser.add_subparsers(dest="command")

    crawl_parser = sub.add_parser("crawl", help="Run a crawl and import URLs into the database")
    crawl_parser.add_argument("--seed", action="append", default=None)
    crawl_parser.add_argument("--depth", type=int, default=3)
    crawl_parser.add_argument("--workers", type=int, default=8)
    crawl_parser.add_argument("--delay", type=float, default=0.25)
    crawl_parser.add_argument("--timeout", type=float, default=30)
    crawl_parser.add_argument("--links", default=LINKS_FILE)
    crawl_parser.add_argument("--edges", default=EDGES_FILE)

    sub.add_parser("report", help="Print the churn report to the terminal")

    demo_parser = sub.add_parser("demo", help="Seed realistic demo churn data")
    demo_parser.add_argument("--cycles", type=int, default=12)
    demo_parser.add_argument("--urls", type=int, default=DEFAULT_URL_COUNT)

    server_parser = sub.add_parser("server", help="Start the web UI (default)")
    server_parser.add_argument("--port", type=int, default=8000)
    server_parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    server_parser.add_argument(
        "--db", default=None,
        help=f"database to serve (default: {DB_FILE})",
    )
    server_parser.add_argument("--no-pinger", action="store_true")
    server_parser.add_argument(
        "--demo",
        action="store_true",
        help="Seed demo data and append fast simulated cycles",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    args = parse_args()

    if args.command == "crawl":
        run_crawl_cli(
            seed=args.seed,
            depth=args.depth,
            workers=args.workers,
            delay=args.delay,
            timeout=args.timeout,
            links=args.links,
            edges=args.edges,
        )
        return

    if args.command == "report":
        run_report_cli()
        return

    if args.command == "demo":
        if args.cycles < 3:
            raise SystemExit("--cycles must be at least 3")
        run_demo_cli(cycles=args.cycles, urls=args.urls)
        return

    # default: server
    interval = getattr(args, "interval", DEFAULT_INTERVAL)
    if interval <= 0:
        raise SystemExit("--interval must be positive")
    start_server(
        port=getattr(args, "port", 8000),
        no_pinger=getattr(args, "no_pinger", False),
        interval=interval,
        demo=getattr(args, "demo", False),
        db=getattr(args, "db", None),
    )


# Not dismissible by design: no control, and re-rendered on every request.
_SYNTHETIC_BANNER = (
    '<div class="synthetic-banner" role="alert">DEMO DATA — SYNTHETIC</div>'
)

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<meta charset="utf-8">
<title>Onion Churn Tracker</title>
<style>
:root {
  --ink: #111;
  --muted: #666;
  --rule: #e3e3e3;
  --accent: #a63224;
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}
* { box-sizing: border-box; }
body {
  font-family: system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  margin: 0; background: #fff; color: var(--ink);
  font-size: 13px; line-height: 1.45;
}
.figure { font-variant-numeric: tabular-nums; }
header {
  display: flex; align-items: baseline; gap: 16px;
  padding: 14px 24px 12px; border-bottom: 1px solid var(--rule);
}
header h1 { margin: 0; font-size: 14px; font-weight: 600; letter-spacing: .01em; }
header .meta {
  margin-left: auto; font-size: 11px; color: var(--muted);
  font-variant-numeric: tabular-nums;
}
header .meta b { font-weight: 500; color: var(--ink); }
.synthetic-banner {
  position: sticky; top: 0; z-index: 20;
  background: var(--accent); color: #fff;
  font-size: 11px; font-weight: 700; letter-spacing: .18em; text-transform: uppercase;
  text-align: center; padding: 7px 24px;
  border-bottom: 1px solid var(--accent);
}
button {
  font: inherit; font-size: 11px; color: var(--ink);
  background: #fff; border: 1px solid var(--rule); padding: 3px 9px; cursor: pointer;
}
button:hover:not(:disabled) { border-color: var(--muted); }
button:disabled { color: #aaa; cursor: not-allowed; }
button.active { border-color: var(--ink); }
.controls { display: flex; align-items: center; gap: 8px; padding: 10px 24px; }
.controls .spacer { margin-left: auto; }

/* Tiles: one row, label above figure, hairline rules between. */
.tiles { display: flex; padding: 0 24px; border-bottom: 1px solid var(--rule); }
.tile { padding: 14px 24px 16px; flex: 0 0 auto; min-width: 128px; }
.tile + .tile { border-left: 1px solid var(--rule); }
.tile:first-child { padding-left: 0; }
.tile .l {
  font-size: 10px; letter-spacing: .07em; text-transform: uppercase;
  color: var(--muted); margin-bottom: 4px; white-space: nowrap;
}
.tile .v {
  font-size: 30px; font-weight: 400; letter-spacing: -.01em; line-height: 1.05;
  font-variant-numeric: tabular-nums;
}

section { padding: 18px 24px 24px; }
section + section { border-top: 1px solid var(--rule); }
h2 {
  font-size: 11px; font-weight: 600; letter-spacing: .07em; text-transform: uppercase;
  color: var(--muted); margin: 0 0 10px; display: flex; align-items: center; gap: 10px;
}
canvas { width: 100%; height: 280px; display: block; }

table { width: 100%; border-collapse: collapse; font-size: 12px; table-layout: fixed; }
th {
  text-align: left; font-size: 10px; font-weight: 600; letter-spacing: .06em;
  text-transform: uppercase; color: var(--muted);
  padding: 0 10px 6px 0; border-bottom: 1px solid var(--rule); cursor: pointer;
  white-space: nowrap;
}
th:hover { color: var(--ink); }
td {
  padding: 5px 10px 5px 0; border-bottom: 1px solid var(--rule);
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
td.u { font-family: var(--mono); font-size: 11px; }
td.u a { color: var(--ink); text-decoration: none; }
td.u a:hover { text-decoration: underline; }
th.n, td.n { text-align: right; font-variant-numeric: tabular-nums; }
.dot { display: inline-block; width: 6px; height: 6px; margin-right: 6px; vertical-align: 1px; }
.dot.alive { background: var(--accent); }
.dot.dead { background: #c9c9c9; }
.dot.unknown { background: #fff; border: 1px solid #c9c9c9; }
.tbar { display: flex; align-items: center; gap: 10px; margin-bottom: 10px; }
input[type=text] {
  font: inherit; font-size: 11px; font-family: var(--mono);
  border: 1px solid var(--rule); padding: 4px 8px; width: 300px; color: var(--ink);
}
input[type=text]:focus { outline: none; border-color: var(--muted); }
.note { font-size: 11px; color: var(--muted); font-variant-numeric: tabular-nums; }
.pager { display: flex; align-items: center; gap: 6px; margin-top: 10px; }
</style>
<body>
<!--BANNER-->
<header>
  <h1>Onion Churn Tracker</h1>
  <div class="meta">
    database <b id="db-name"><!--DBNAME--></b> &nbsp;·&nbsp; pinger <b id="pinger-running">stopped</b>
    &nbsp;·&nbsp; <span id="last-cycle">—</span>
  </div>
</header>

<div class="tiles figure" id="tiles"></div>

<div class="controls">
  <button id="btn-start" onclick="togglePinger()">Start pinger</button>
  <button id="btn-stop" onclick="togglePinger()" disabled>Stop pinger</button>
  <span class="spacer note"><a href="/api/summary">summary</a> · <a href="/api/cycles">cycles</a></span>
</div>

<section>
  <h2>Services alive per cycle
    <button id="btn-window" onclick="toggleWindow()">all history</button>
  </h2>
  <canvas id="chart"></canvas>
</section>

<section>
  <h2>Services</h2>
  <div class="tbar">
    <input type="text" id="search" placeholder="filter by address" spellcheck="false">
    <span class="note">page <span id="page-info">1</span> / <span id="page-total">1</span></span>
    <span class="note spacer">sorted by <span id="sort-info">url</span></span>
  </div>
  <table>
    <colgroup>
      <col><col style="width:150px"><col style="width:150px">
      <col style="width:84px"><col style="width:84px"><col style="width:96px">
    </colgroup>
    <thead><tr>
      <th onclick="sort('url')">Address</th>
      <th onclick="sort('first_seen')">First seen</th>
      <th onclick="sort('last_alive')">Last alive</th>
      <th class="n" onclick="sort('uptime')">Uptime</th>
      <th class="n" onclick="sort('total_pings')">Pings</th>
      <th>Status</th>
    </tr></thead>
    <tbody id="url-body"></tbody>
  </table>
  <div class="pager">
    <button id="btn-prev" onclick="changePage(-1)">Prev</button>
    <button id="btn-next" onclick="changePage(1)">Next</button>
  </div>
</section>

<script>
const INK = '#111', MUTED = '#666', RULE = '#e3e3e3', ACCENT = '#a63224';
const MONO = '10px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';
const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
let state = { page:1, pageSize:50, search:'', sort:'url', dir:'asc', totalPages:1, chartAll:false };
const runningSpan = document.getElementById('pinger-running');
const btnStart = document.getElementById('btn-start');
const btnStop = document.getElementById('btn-stop');

async function load(){
  await Promise.all([loadSummary(), loadCycles(), loadUrls(), loadPinger()]);
}

function pad2(n){ return String(n).padStart(2, '0'); }
function stamp(iso){
  const d = new Date(iso);
  if(isNaN(d)) return iso || '—';
  return `${MONTHS[d.getMonth()]} ${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
}

async function loadSummary(){
  const r = await fetch('/api/summary'); const d = await r.json();
  document.getElementById('tiles').innerHTML = `
    <div class="tile"><div class="l">Services tracked</div><div class="v">${d.total}</div></div>
    <div class="tile"><div class="l">Alive in last cycle</div><div class="v">${d.current_alive}</div></div>
    <div class="tile"><div class="l">Never responded</div><div class="v">${d.never_alive}</div></div>
    <div class="tile"><div class="l">Churned</div><div class="v">${d.churned_now}</div></div>
    <div class="tile"><div class="l">Completed cycles</div><div class="v">${d.completed_cycles}</div></div>
  `;
  const lc = document.getElementById('last-cycle');
  lc.textContent = d.last_cycle
    ? `last cycle ${stamp(d.last_cycle.started_at)} ${d.last_cycle.status}`
    : 'no cycles yet';
}

async function loadCycles(){
  const r = await fetch('/api/cycles'); const cycles = await r.json();
  const canvas = document.getElementById('chart'); const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr; canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const w = rect.width, h = rect.height;
  const padL = 54, padR = 14, padT = 14, padB = 34;
  ctx.clearRect(0, 0, w, h);
  const message = (text) => { ctx.fillStyle = MUTED; ctx.font = MONO; ctx.fillText(text, padL, h/2); };
  if(!cycles.length){ message('no cycles yet'); return; }

  const startedAt = (c) => Date.parse(c.started_at);
  const completedAll = cycles.filter(c => c.status === 'completed');
  // Default window: completed cycles only. A long run of aborted cycles then
  // collapses into one break instead of eating most of the x-axis.
  const series = state.chartAll ? cycles : completedAll;
  if(!series.length){ message('no completed cycles yet'); return; }

  // Median gap over completed cycles, used to spot unobserved stretches.
  const deltas = [];
  for(let k = 1; k < completedAll.length; k++) deltas.push(startedAt(completedAll[k]) - startedAt(completedAll[k-1]));
  const ranked = deltas.filter(d => d > 0).sort((a,b) => a - b);
  const median = ranked.length
    ? (ranked.length % 2 ? ranked[(ranked.length-1)/2] : (ranked[ranked.length/2-1] + ranked[ranked.length/2]) / 2)
    : 0;
  const fmtGap = (ms) => ms >= 86400000 ? `${Math.round(ms/86400000)}d gap` : `${Math.round(ms/3600000)}h gap`;
  // A break before series[k]: either an aborted cycle fell in between, or the
  // wait was far longer than usual. Either way it is unobserved time.
  const breaks = new Map();
  for(let k = 1; k < series.length; k++){
    if(series[k].status !== 'completed' || series[k-1].status !== 'completed') continue;
    const gap = startedAt(series[k]) - startedAt(series[k-1]);
    const abortedBetween = cycles.some(c => c.status !== 'completed'
      && startedAt(c) > startedAt(series[k-1]) && startedAt(c) < startedAt(series[k]));
    if(abortedBetween || (median > 0 && gap > 3*median)) breaks.set(k, fmtGap(gap));
  }

  // Y scale rounded up to a readable tick step.
  const peak = Math.max(...completedAll.map(c => c.alive), 1);
  const pow = Math.pow(10, Math.floor(Math.log10(peak)));
  const unit = peak/pow <= 1 ? 1 : peak/pow <= 2 ? 2 : peak/pow <= 5 ? 5 : 10;
  const yMax = unit * pow;
  const ticks = unit === 1 ? 4 : unit === 2 ? 4 : 5;
  const xScale = (i) => padL + (i / Math.max(series.length - 1, 1)) * (w - padL - padR);
  const yScale = (v) => h - padB - (v / yMax) * (h - padT - padB);

  // Gridlines and y labels.
  ctx.font = MONO; ctx.textBaseline = 'middle';
  for(let t = 0; t <= ticks; t++){
    const v = yMax * t / ticks, y = Math.round(yScale(v)) + 0.5;
    ctx.strokeStyle = RULE; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke();
    ctx.fillStyle = MUTED; ctx.textAlign = 'right';
    ctx.fillText(String(Math.round(v)), padL - 8, y);
  }
  ctx.save();
  ctx.translate(12, (padT + h - padB) / 2); ctx.rotate(-Math.PI/2);
  ctx.textAlign = 'center'; ctx.fillStyle = MUTED; ctx.fillText('services alive', 0, 0);
  ctx.restore();
  ctx.textBaseline = 'alphabetic';

  const breakMarks = [];
  const drawBreak = (x) => {
    ctx.save();
    ctx.strokeStyle = '#bbb'; ctx.setLineDash([3,3]); ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(Math.round(x) + 0.5, padT); ctx.lineTo(Math.round(x) + 0.5, h - padB); ctx.stroke();
    ctx.restore();
    ctx.beginPath();
  };

  // Data line: 1px, single accent, broken at every unobserved stretch.
  ctx.strokeStyle = ACCENT; ctx.lineWidth = 1; ctx.beginPath();
  let inSegment = false;
  series.forEach((c, i) => {
    const x = xScale(i);
    if(c.status === 'completed'){
      if(inSegment && breaks.has(i)){
        ctx.stroke(); inSegment = false;
        const mid = (x + xScale(i-1)) / 2;
        drawBreak(mid);
        breakMarks.push({ x: mid, label: breaks.get(i) });
      }
      const y = yScale(c.alive);
      if(!inSegment){ ctx.moveTo(x, y); inSegment = true; } else ctx.lineTo(x, y);
    } else {
      if(inSegment){ ctx.stroke(); inSegment = false; }
      drawBreak(x);
    }
  });
  if(inSegment) ctx.stroke();

  series.forEach((c, i) => {
    if(c.status !== 'completed') return;
    ctx.fillStyle = ACCENT;
    ctx.beginPath(); ctx.arc(xScale(i), yScale(c.alive), 1.75, 0, Math.PI*2); ctx.fill();
  });

  ctx.font = MONO; ctx.fillStyle = MUTED; ctx.textAlign = 'left';
  breakMarks.forEach(m => ctx.fillText(m.label, m.x + 4, padT + 10));

  // X axis with date ticks spaced so labels cannot collide.
  const axisY = Math.round(h - padB) + 0.5;
  ctx.strokeStyle = RULE; ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(padL, axisY); ctx.lineTo(w - padR, axisY); ctx.stroke();
  const maxTicks = Math.max(2, Math.floor((w - padL - padR) / 92));
  const step = Math.max(1, Math.ceil(series.length / maxTicks));
  ctx.fillStyle = MUTED; ctx.textAlign = 'center';
  for(let i = 0; i < series.length; i += step){
    const x = Math.min(Math.max(xScale(i), padL + 26), w - padR - 26);
    ctx.fillText(stamp(series[i].started_at), x, h - padB + 15);
  }
  if(!state.chartAll && completedAll.length < cycles.length){
    ctx.textAlign = 'right';
    ctx.fillText(`${cycles.length - completedAll.length} aborted cycles hidden`, w - padR, h - 6);
  }
}

async function loadUrls(){
  const r = await fetch(`/api/urls?page=${state.page}&page_size=${state.pageSize}&search=${encodeURIComponent(state.search)}&sort=${state.sort}&direction=${state.dir}`);
  const d = await r.json();
  const body = document.getElementById('url-body'); body.replaceChildren();
  for(const it of d.items){
    const row = document.createElement('tr');

    const urlCell = document.createElement('td');
    urlCell.className = 'u';
    urlCell.title = it.url;
    const separator = it.url.indexOf('://');
    const scheme = it.url.slice(0, separator).toLowerCase();
    const remainder = separator < 0 ? '' : it.url.slice(separator + 3);
    const authority = remainder.split('/', 1)[0].split('?', 1)[0].split('#', 1)[0];
    const hostname = authority.split(':', 1)[0].toLowerCase();
    const validOnion = (scheme === 'http' || scheme === 'https') && /^[a-z2-7]{56}[.]onion$/i.test(hostname);
    if(validOnion){
      const link = document.createElement('a');
      link.href = it.url; link.target = '_blank'; link.rel = 'noopener';
      link.textContent = it.url;
      urlCell.append(link);
    } else {
      urlCell.textContent = it.url;
    }

    const firstSeen = document.createElement('td');
    firstSeen.textContent = stamp(it.first_seen);
    firstSeen.title = it.first_seen || '';
    const lastAlive = document.createElement('td');
    lastAlive.textContent = it.last_alive ? stamp(it.last_alive) : '—';
    lastAlive.title = it.last_alive || '';
    const uptime = document.createElement('td');
    uptime.className = 'n';
    uptime.textContent = it.total_pings ? it.uptime_pct.toFixed(1) + '%' : '—';
    const pings = document.createElement('td');
    pings.className = 'n';
    pings.textContent = `${it.alive_pings}/${it.total_pings}`;

    const current = document.createElement('td');
    const dot = document.createElement('span');
    dot.className = 'dot ' + (it.current_status === 'alive' ? 'alive'
      : it.current_status === 'dead' ? 'dead' : 'unknown');
    current.append(dot, document.createTextNode(it.current_status));

    row.append(urlCell, firstSeen, lastAlive, uptime, pings, current);
    body.append(row);
  }
  state.totalPages = d.total_pages;
  document.getElementById('page-info').textContent = state.page;
  document.getElementById('page-total').textContent = state.totalPages;
  document.getElementById('btn-prev').disabled = state.page <= 1;
  document.getElementById('btn-next').disabled = state.page >= state.totalPages;
  document.getElementById('sort-info').textContent = state.sort + (state.dir === 'desc' ? ' desc' : ' asc');
}

async function loadPinger(){
  const r = await fetch('/api/pinger/status'); const d = await r.json();
  runningSpan.textContent = d.locked ? 'read-only' : (d.running ? 'running' : 'stopped');
  btnStart.disabled = d.locked || d.running;
  btnStop.disabled = d.locked || !d.running;
  if(d.locked){ btnStart.title = btnStop.title = 'disabled while serving synthetic demo data'; }
}
async function togglePinger(){
  const r = await fetch('/api/pinger/' + (pingerRunning() ? 'stop' : 'start'), { method:'POST' });
  await r.json(); await loadPinger();
}
function pingerRunning(){ return runningSpan.textContent === 'running'; }
function toggleWindow(){
  state.chartAll = !state.chartAll;
  const b = document.getElementById('btn-window');
  b.textContent = state.chartAll ? 'completed only' : 'all history';
  b.classList.toggle('active', state.chartAll);
  loadCycles();
}
function changePage(delta){ state.page = Math.min(Math.max(1, state.page + delta), state.totalPages); loadUrls(); }
function sort(col){ if(state.sort === col) state.dir = state.dir === 'asc' ? 'desc' : 'asc'; else { state.sort = col; state.dir = 'asc'; } state.page = 1; loadUrls(); }
document.getElementById('search').addEventListener('input', (e) => { state.search = e.target.value; state.page = 1; loadUrls(); });
window.addEventListener('resize', () => loadCycles());
setInterval(() => { loadSummary(); loadCycles(); loadUrls(); loadPinger(); }, 8000);
load();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
