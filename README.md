# Onion Churn Tracker

A local crawler, Tor-based onion-service pinger, churn reporter, and dashboard.
It stores observations in SQLite (`onions.db`) and serves the dashboard only on
`127.0.0.1`.

## Requirements

- Python 3
- Tor running locally with a SOCKS5 proxy on `127.0.0.1:9050`
- Fedora: install Tor with `sudo dnf install tor`

## Set up

From the project directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Start Tor:

```bash
sudo systemctl enable --now tor
```

Verify that requests through Tor really exit through the Tor network before
crawling or pinging:

```bash
curl --socks5-hostname 127.0.0.1:9050 \
  https://check.torproject.org/api/ip
```

The response must include `"IsTor":true`. The crawler and pinger refuse to
trust a cycle when this check fails.

## Crawl

The crawler defaults to the seed configured in `crawler.py`, depth 3, 8 workers,
a 0.25-second delay, and a 30-second request timeout. The seed can rotate or go
offline; use a current v3 onion URL if it no longer works.

Run a crawl and import its discovered URLs into the database:

```bash
python app.py crawl
```

Choose a seed, depth, and request delay explicitly:

```bash
python app.py crawl \
  --seed "http://<56-character-v3-address>.onion" \
  --depth 3 \
  --workers 4 \
  --delay 1 \
  --timeout 30
```

You can pass `--seed` more than once. The command writes `links.txt` and
`edges.json` in the project directory and adds discovered URLs to `onions.db`.
It replaces the previous crawl output files, but does not delete database
records. A crawl can take a long time; keep the delay reasonable.

To run only the crawler, without importing results into the database:

```bash
python crawler.py --depth 3
```

Its options include `--seed`, `--workers`, `--delay`, `--timeout`, `--proxy`,
`--links`, and `--edges`. Use `python crawler.py --help` to see all options.

## Run the dashboard and background pinger

```bash
python app.py
```

Open <http://127.0.0.1:8000/> in a browser. By default, the app starts the
background pinger immediately and repeats cycles every 6 hours. Use the
dashboard’s **Stop pinger** and **Start pinger** buttons to control it. Stop the
server with `Ctrl+C`.

Set a different cycle interval in seconds, or run the dashboard without starting
the pinger automatically:

```bash
python app.py server --interval 3600
python app.py server --no-pinger
```

The pinger begins a cycle as soon as it starts. An interval of `3600` means
one hour between cycle starts after each cycle finishes. Aborted cycles are
recorded as gaps rather than as observations that every service was dead.

## Demo mode (no waiting for real cycles)

A real cycle probes thousands of onion services and can take hours. To show a
fully populated, changing dashboard immediately:

```bash
python app.py demo                 # seed 12 cycles + 1 gap (backs up the old DB)
python app.py server --demo --interval 15
```

`--demo` appends a fast simulated cycle every 15 seconds, so the tiles, chart,
and churn numbers move while you watch. The UI shows a `DEMO DATA` badge. The
previous database is preserved as `onions.db.bak-<timestamp>`. Use normal
`python app.py` for real Tor pings.

## Run the pinger separately

Run one cycle and exit:

```bash
python pinger.py --once
```

Run repeated cycles, using a custom interval in seconds:

```bash
python pinger.py --interval 3600
```

The standalone pinger reads `links.txt` by default. To use a different file or
database, pass `--links` and/or `--db`. To populate `links.txt` from URLs already
stored in the database (useful when the file contains only a seed):

```bash
python - <<'PY'
import sqlite3

with sqlite3.connect("onions.db") as db:
    urls = db.execute("SELECT url FROM urls ORDER BY url").fetchall()

with open("links.txt", "w", encoding="utf-8") as links:
    links.writelines(f"{url}\n" for (url,) in urls)

print(f"Wrote {len(urls)} URLs to links.txt")
PY
```

Do not run the standalone pinger and the dashboard pinger against the same
database at the same time. Stop one before starting the other.

## Print a report

```bash
python app.py report
```

Alternatively, run the report script directly. It accepts a database path:

```bash
python report.py --db onions.db
```

The report summarizes discovered URLs, completed and aborted cycles, alive
history, churn, observed lifespan, and per-cycle results.

## Data and useful endpoints

- `onions.db` — SQLite database containing URLs, pings, and cycles. Keep this
  file to preserve your history.
- `links.txt` — latest crawler output and standalone-pinger input.
- `edges.json` — latest crawl link graph.
- `GET /api/summary` — dashboard summary as JSON.
- `GET /api/cycles` — cycle history, including aborted gaps.
- `GET /api/urls` — paginated URL results; supports `page`, `page_size`,
  `search`, `sort`, and `direction`.
- `GET /api/graph` — latest graph data.

Generated crawl outputs are ignored by Git. Back up `onions.db` separately if
you need to retain observations across machines or before changing the database.

## Troubleshooting

- If the health check does not return `"IsTor":true`, verify that the Tor
  service is running and listening on `127.0.0.1:9050`.
- If a cycle is aborted, check the pinger logs and Tor health before interpreting
  any service status. Aborted cycles do not count as dead responses.
- If no URLs are pinged, confirm that `links.txt` exists and contains valid v3
  onion URLs, or export the URLs from the database using the command above.
- If the default seed cannot be reached, provide a current v3 onion URL with
  `--seed`.
- Run `python app.py --help`, `python app.py crawl --help`, or
  `python pinger.py --help` to inspect command options.
