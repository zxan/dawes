import argparse
import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

import crawler as crawler_module
import report as report_module
from database import connect_db, ensure_schema
from pinger import default_links_file, load_urls, run_cycle
from tor_utils import check_tor
from url_utils import normalize_onion_url


APP_TITLE = "Onion Churn Tracker"
DB_FILE = "onions.db"
LINKS_FILE = "links.txt"
EDGES_FILE = "edges.json"
DEFAULT_INTERVAL = 6 * 60 * 60

app = FastAPI(title=APP_TITLE)


class PingerManager:
    def __init__(
        self,
        db_path=DB_FILE,
        links_path=None,
        interval=DEFAULT_INTERVAL,
        timeout=30,
        workers=8,
    ):
        self.db_path = Path(db_path)
        self.links_path = Path(links_path) if links_path else default_links_file()
        self.interval = float(interval)
        if self.interval <= 0:
            raise ValueError("interval must be positive")
        self.timeout = timeout
        self.workers = workers
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


@app.get("/", response_class=HTMLResponse)
def index():
    return HTMLResponse(_DASHBOARD_HTML)


@app.get("/api/summary")
def summary():
    connection = connect_db(DB_FILE)
    ensure_schema(connection)
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
            "last_cycle": dict(last_cycle) if last_cycle else None,
            "pinger_running": status["running"],
        }
    )


@app.get("/api/cycles")
def cycles():
    connection = connect_db(DB_FILE)
    ensure_schema(connection)
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
    ensure_schema(connection)
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

        last_status = {}
        for row in connection.execute(
            """
            SELECT p.url, p.status
            FROM pings p
            JOIN cycles c ON c.cycle_id = p.cycle_id
            WHERE c.status='completed'
            ORDER BY datetime(c.started_at) DESC, p.id DESC
            """
        ).fetchall():
            last_status.setdefault(row["url"], row["status"])
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


def start_server(port=8000, no_pinger=False, interval=DEFAULT_INTERVAL):
    import uvicorn

    pinger_manager.interval = interval
    if not no_pinger:
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

    server_parser = sub.add_parser("server", help="Start the web UI (default)")
    server_parser.add_argument("--port", type=int, default=8000)
    server_parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    server_parser.add_argument("--no-pinger", action="store_true")
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

    # default: server
    interval = getattr(args, "interval", DEFAULT_INTERVAL)
    if interval <= 0:
        raise SystemExit("--interval must be positive")
    start_server(
        port=getattr(args, "port", 8000),
        no_pinger=getattr(args, "no_pinger", False),
        interval=interval,
    )


_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<meta charset="utf-8">
<title>Onion Churn Tracker</title>
<style>
* { box-sizing: border-box; }
body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; background:#f5f7fa; color:#111; }
header { background:#0b3d62; color:#fff; padding:12px 20px; }
header h1 { margin:0; font-size:1.25rem; }
.bar { display:flex; gap:10px; align-items:center; padding:10px 20px; background:#fff; border-bottom:1px solid #ddd; }
button { border:1px solid #9bb1c8; background:#fff; border-radius:6px; padding:6px 12px; cursor:pointer; }
button.active { background:#0b3d62; color:#fff; border-color:#0b3d62; }
button:disabled { opacity:.6; cursor:not-allowed; }
.grid { display:grid; grid-template-columns: repeat(auto-fit,minmax(170px,1fr)); gap:12px; padding:12px 20px; }
.tile { background:#fff; border:1px solid #ddd; border-radius:8px; padding:12px 16px; }
.tile .v { font-size:1.75rem; font-weight:700; }
.tile .l { font-size:.85rem; color:#444; margin-top:2px; }
.tile .p { font-size:.8rem; color:#2f7d32; }
.section { padding:0 20px 20px; }
h2 { font-size:1rem; margin:0 0 8px; color:#0b3d62; }
.chart-card, .table-card { background:#fff; border:1px solid #ddd; border-radius:8px; padding:12px 16px; margin-bottom:16px; }
canvas { width:100%; height:260px; display:block; }
table { width:100%; border-collapse:collapse; font-size:.85rem; }
th, td { border-bottom:1px solid #e5e5e5; text-align:left; padding:6px 8px; }
th { background:#f0f4f8; cursor:pointer; }
tr:hover { background:#f7fafc; }
.pagination { display:flex; gap:6px; align-items:center; margin-top:8px; }
.pagination button { min-width:32px; }
.muted { color:#666; font-size:.85rem; }
.ok { color:#2f7d32; } .bad { color:#b3261e; } .gap { color:#b36b00; }
.status-badge { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:6px; }
.status-ok { background:#2f7d32; } .status-dead { background:#b3261e; } .status-unknown { background:#999; }
</style>
<body>
<header><h1>\u26ab Onion Churn Tracker</h1></header>
<div class="bar">
  <div id="pinger-status" class="muted">Pinger: <span id="pinger-running">stopped</span></div>
  <button id="btn-start" onclick="togglePinger()">Start pinger</button>
  <button id="btn-stop" onclick="togglePinger()" disabled>Stop pinger</button>
  <span class="muted" style="margin-left:auto">DB: onions.db &bull; <a href="/api/summary">JSON</a> &bull; <a href="/api/cycles">cycles</a></span>
</div>
<div class="grid" id="tiles"></div>
<div class="section">
  <h2>Alive over time</h2>
  <div class="chart-card"><canvas id="chart"></canvas></div>
</div>
<div class="section">
  <h2>URL table</h2>
  <div class="table-card">
    <div style="display:flex;gap:8px;margin-bottom:8px;align-items:center;">
      <input id="search" placeholder="Filter URLs" style="padding:6px 10px;border:1px solid #ccc;border-radius:6px;width:260px;">
      <span class="muted">Page <span id="page-info">1</span> of <span id="page-total">1</span></span>
      <span class="muted" style="margin-left:auto">Sort: <span id="sort-info">url</span></span>
    </div>
    <table id="url-table">
      <thead><tr><th onclick="sort('url')">URL</th><th onclick="sort('first_seen')">First seen</th><th onclick="sort('last_alive')">Last alive</th><th onclick="sort('uptime')">Uptime %</th><th onclick="sort('total_pings')">Pings</th><th>Current</th></tr></thead>
      <tbody id="url-body"></tbody>
    </table>
    <div class="pagination">
      <button id="btn-prev" onclick="changePage(-1)">Prev</button>
      <button id="btn-next" onclick="changePage(1)">Next</button>
    </div>
  </div>
</div>
<script>
let state = { page:1, pageSize:50, search:'', sort:'url', dir:'asc', totalPages:1 };
const runningSpan = document.getElementById('pinger-running');
const btnStart = document.getElementById('btn-start');
const btnStop = document.getElementById('btn-stop');

async function load(){
  await Promise.all([loadSummary(), loadCycles(), loadUrls(), loadPinger()]);
}
async function loadSummary(){
  const r = await fetch('/api/summary'); const d = await r.json();
  const tiles = document.getElementById('tiles');
  tiles.innerHTML = `
    <div class="tile"><div class="v">${d.total}</div><div class="l">Total discovered</div></div>
    <div class="tile"><div class="v">${d.current_alive}</div><div class="l">Currently alive</div></div>
    <div class="tile"><div class="v ok">${d.ever_alive}</div><div class="l">Ever alive <span class="p">(${d.ever_alive_pct.toFixed(1)}%)</span></div></div>
    <div class="tile"><div class="v bad">${d.never_alive}</div><div class="l">Never responded <span class="p">(${d.never_alive_pct.toFixed(1)}%)</span></div></div>
    <div class="tile"><div class="v ok">${d.always_alive}</div><div class="l">Consistently alive <span class="p">(${d.always_alive_pct.toFixed(1)}%)</span></div></div>
    <div class="tile"><div class="v bad">${d.churned}</div><div class="l">Churned <span class="p">(${d.churned_pct.toFixed(1)}%)</span></div></div>
    <div class="tile"><div class="v">${d.completed_cycles}</div><div class="l">Completed cycles</div><div class="p gap">${d.aborted_cycles} aborted gaps</div></div>
  `;
  if(d.last_cycle){
    const cls = d.last_cycle.status === 'completed' ? 'ok' : (d.last_cycle.status === 'aborted' ? 'gap' : 'bad');
    tiles.innerHTML += `<div class="tile"><div class="v ${cls}">${d.last_cycle.alive}/${d.last_cycle.total}</div><div class="l">Last cycle ${d.last_cycle.status}</div><div class="muted">${d.last_cycle.started_at}</div></div>`;
  }
}
async function loadCycles(){
  const r = await fetch('/api/cycles'); const cycles = await r.json();
  const canvas = document.getElementById('chart'); const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr; canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);
  const w = rect.width, h = rect.height, pad = 36;
  ctx.clearRect(0,0,w,h);
  if(!cycles.length){ ctx.fillStyle='#999'; ctx.font='14px system-ui'; ctx.fillText('No cycles yet', pad, h/2); return; }
  const completed = cycles.filter(c=>c.status==='completed');
  const maxAlive = Math.max(...completed.map(c=>c.alive), 1);
  const xScale = (i) => pad + (i/(Math.max(cycles.length-1,1)))*(w-2*pad);
  const yScale = (v) => h - pad - (v/maxAlive)*(h-2*pad);
  ctx.strokeStyle='#0b3d62'; ctx.lineWidth=2; ctx.beginPath();
  let inSegment=false;
  cycles.forEach((c,i)=>{
    const x=xScale(i);
    if(c.status==='completed'){
      const y=yScale(c.alive);
      if(!inSegment){ctx.moveTo(x,y); inSegment=true;} else ctx.lineTo(x,y);
    } else {
      if(inSegment){ctx.stroke(); ctx.beginPath(); inSegment=false;}
      ctx.strokeStyle='#b36b00'; ctx.setLineDash([6,4]); ctx.beginPath(); ctx.moveTo(x,pad); ctx.lineTo(x,h-pad); ctx.stroke(); ctx.setLineDash([]);
      ctx.beginPath();
      ctx.strokeStyle='#0b3d62';
    }
  });
  if(inSegment) ctx.stroke();
  cycles.forEach((c,i)=>{
    const x=xScale(i);
    if(c.status==='completed'){
      const y=yScale(c.alive);
      ctx.fillStyle='#0b3d62'; ctx.beginPath(); ctx.arc(x,y,3,0,Math.PI*2); ctx.fill();
    } else {
      ctx.fillStyle='#b36b00'; ctx.font='11px system-ui'; ctx.fillText('gap', x+4, pad+12);
    }
  });
  ctx.strokeStyle='#ccc'; ctx.beginPath(); ctx.moveTo(pad,h-pad); ctx.lineTo(w-pad,h-pad); ctx.stroke();
  ctx.fillStyle='#666'; ctx.font='12px system-ui';
  cycles.forEach((c,i)=>{ if(i%Math.max(1,Math.floor(cycles.length/8))===0){ ctx.fillText(c.started_at.slice(0,16), xScale(i)-30, h-8); } });
}
async function loadUrls(){
  const r = await fetch(`/api/urls?page=${state.page}&page_size=${state.pageSize}&search=${encodeURIComponent(state.search)}&sort=${state.sort}&direction=${state.dir}`);
  const d = await r.json();
  const body = document.getElementById('url-body'); body.replaceChildren();
  for(const it of d.items){
    const cls = it.current_status==='alive'?'status-ok':(it.current_status==='dead'?'status-dead':'status-unknown');
    const label = it.current_status==='alive'?'alive':(it.current_status==='dead'?'dead':'unknown');
    const row = document.createElement('tr');
    const urlCell = document.createElement('td');
    const separator = it.url.indexOf('://');
    const scheme = it.url.slice(0, separator).toLowerCase();
    const remainder = separator < 0 ? '' : it.url.slice(separator + 3);
    const authority = remainder.split('/', 1)[0].split('?', 1)[0].split('#', 1)[0];
    const hostname = authority.split(':', 1)[0].toLowerCase();
    const validOnion = (scheme==='http' || scheme==='https') && /^[a-z2-7]{56}[.]onion$/i.test(hostname);
    if(validOnion){
      const link = document.createElement('a');
      link.href = it.url;
      link.target = '_blank';
      link.rel = 'noopener';
      link.textContent = it.url;
      urlCell.append(link);
    } else {
      urlCell.textContent = it.url;
    }
    const firstSeen = document.createElement('td');
    firstSeen.textContent = it.first_seen || '';
    const lastAlive = document.createElement('td');
    lastAlive.textContent = it.last_alive || '—';
    const uptime = document.createElement('td');
    uptime.textContent = it.uptime_pct.toFixed(1);
    const pings = document.createElement('td');
    pings.textContent = `${it.alive_pings}/${it.total_pings}`;
    const current = document.createElement('td');
    const badge = document.createElement('span');
    badge.className = `status-badge ${cls}`;
    current.append(badge, document.createTextNode(label));
    row.append(urlCell, firstSeen, lastAlive, uptime, pings, current);
    body.append(row);
  }
  state.totalPages = d.total_pages;
  document.getElementById('page-info').textContent = state.page;
  document.getElementById('page-total').textContent = state.total_pages;
  document.getElementById('btn-prev').disabled = state.page<=1;
  document.getElementById('btn-next').disabled = state.page>=state.total_pages;
  document.getElementById('sort-info').textContent = state.sort + (state.dir==='desc'?' \u25bc':' \u25b2');
}
async function loadPinger(){
  const r = await fetch('/api/pinger/status'); const d = await r.json();
  runningSpan.textContent = d.running ? 'running' : 'stopped';
  runningSpan.className = d.running ? 'ok' : 'bad';
  btnStart.disabled = d.running; btnStop.disabled = !d.running;
}
async function togglePinger(){
  const r = await fetch('/api/pinger/' + (pingerRunning()?'stop':'start'), {method:'POST'});
  await r.json(); await loadPinger();
}
function pingerRunning(){ return runningSpan.textContent === 'running'; }
function changePage(delta){ state.page = Math.min(Math.max(1, state.page + delta), state.totalPages); loadUrls(); }
function sort(col){ if(state.sort===col) state.dir = state.dir==='asc'?'desc':'asc'; else {state.sort=col; state.dir='asc';} state.page=1; loadUrls(); }
document.getElementById('search').addEventListener('input', (e)=>{ state.search=e.target.value; state.page=1; loadUrls(); });
setInterval(()=>{ loadSummary(); loadCycles(); loadUrls(); loadPinger(); }, 8000);
load();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
