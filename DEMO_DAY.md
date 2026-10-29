# Two-Day Demo Runbook

## The simple version

Use the **dashboard command** to run the app and its built-in pinger together:

```bash
python app.py server --interval 3600
```

Keep that terminal and the laptop awake until the demo. The pinger starts
immediately, updates `onions.db`, and repeats after each cycle plus the
configured interval. Open <http://127.0.0.1:8000/> to see the dashboard.

For this two-day run, **do not also run `crawler.py` or `pinger.py`**. Those are
standalone alternatives, not extra steps. You already have 2,659 URLs in the
database, and the dashboard pinger checks the URLs in the database even though
`links.txt` currently contains only one seed.

## Start collecting data now

Run these commands from the project directory. Activate the existing virtual
environment first:

```bash
source .venv/bin/activate
```

If you have not set it up on this machine yet:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Start Tor and verify it is routing:

```bash
sudo systemctl enable --now tor
curl --socks5-hostname 127.0.0.1:9050 \
  https://check.torproject.org/api/ip
```

Continue only if the response contains `"IsTor":true`. Check the current
database once before starting:

```bash
python app.py report
```

The database currently contains 2,659 URLs, but only one completed cycle; three
older cycles are marked aborted. **The main thing to do now is collect new
completed cycles**, not to crawl again.

Start the dashboard and one-hour-interval pinger:

```bash
systemd-inhibit --what=sleep \
  --why="Collecting Onion Churn Tracker demo data" \
  python app.py server --interval 3600
```

`systemd-inhibit` helps prevent the laptop from suspending during data
collection. Keep the laptop plugged in, connected to the internet, and leave
this terminal running. If `systemd-inhibit` is unavailable, run:

```bash
python app.py server --interval 3600
```

Then open <http://127.0.0.1:8000/>. A cycle starts right away. The pinger checks
Tor at the start of every cycle and records a gap if the check fails. Keep the
server running overnight and through the two-day collection period.

The one-hour interval is a wait **after** a cycle finishes, not a promise that
every cycle takes one hour. Checking thousands of onion services can take a
while. Wait for the dashboard/report to show completed cycles; aborted cycles
are gaps, not evidence that services were dead.

## Optional: add newly crawled URLs

Skip this unless you specifically want to discover more URLs and have a
currently working v3 onion seed. Crawling can take a long time and is not
required to update the existing database.

If you do crawl, do it **before** starting the dashboard pinger:

```bash
python app.py crawl \
  --seed "http://<56-character-v3-address>.onion" \
  --depth 1 \
  --workers 4 \
  --delay 1
```

This adds discovered URLs to `onions.db` and replaces the current `links.txt`
and `edges.json` crawl outputs. Do not use the example placeholder literally;
replace it with a real, reachable v3 onion address. If you do not have a
verified live seed, skip the crawl.

## Demo-day flow

### If the app is still running

1. Open <http://127.0.0.1:8000/>.
2. Show the summary tiles, alive-over-time chart (including any marked gaps),
   and paginated URL table.
3. In a **second terminal**, from the project directory, show the text report:

   ```bash
   source .venv/bin/activate
   python app.py report
   ```

Do not start a second pinger. The dashboard’s built-in pinger is already
collecting data and writing to the same database.

### If the app was stopped or the laptop restarted

Start Tor and verify it as above, then start the app again:

```bash
source .venv/bin/activate
systemd-inhibit --what=sleep \
  --why="Running Onion Churn Tracker demo" \
  python app.py server --interval 3600
```

Open <http://127.0.0.1:8000/>. This starts the pinger immediately. To print the
report, use another terminal and run `python app.py report`.

## Commands to remember

| What you want | Command |
|---|---|
| Dashboard plus automatic database updates | `python app.py server --interval 3600` |
| Text report | `python app.py report` |
| Optional URL discovery crawl | `python app.py crawl --depth 1 --workers 4 --delay 1` |

For this demo plan, prefer these `app.py` commands. Do not run
`python pinger.py` alongside the dashboard: that would start a second pinger
against the same database. Do not run `python crawler.py` for the optional crawl:
`python app.py crawl` also imports the results into the database.

## Quick checks

- Dashboard not loading: confirm the server terminal is still running; the URL
  is <http://127.0.0.1:8000/>.
- Aborted cycle: check Tor and the server terminal output. An aborted cycle is
  shown as a gap and does not add dead pings.
- Few completed cycles: leave the laptop awake longer. Each cycle probes the
  stored URLs and may take substantial time.
- Preserve the results: keep `onions.db`. It contains the cycle and ping
  history used by both the dashboard and report.
