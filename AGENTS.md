# Onion Churn Tracker — Agent Guide

Guide for a coding agent working on this project. Read fully before writing code.

---

## What this is

Three things, one project:

1. **Crawl** the live Tor network from a seed `.onion`, follow links, collect
   every `.onion` URL reachable within N hops.
2. **Ping** every collected URL on a schedule through Tor, record alive/dead
   with a timestamp.
3. **Report** churn over time — % ever alive, % never responded, % that died
   between cycles, % consistently alive, per-cycle breakdown.

The deliverable people actually see is a **working app** (Phase 2). The crawler
and pinger are its internals.

**What makes it worth submitting:** the longitudinal churn tracking. The crawl
itself is commodity. Nobody in the comparable work (TORONS, Dizzy, standard
`.onion` crawlers) tracks per-URL uptime across repeated cycles over days —
they crawl, archive, and move on. Measuring how fast onion services die is the
contribution.

---

## Hard rules

- **Only our code.** No MonOnion, no OnionScan, no OnionIngestor, no Elastic,
  no Telegram, no external monitoring service. If a feature seems to need a
  third-party system, write it ourselves or drop the feature.
- **No DarkSpider as a dependency.** Phase 1 removes it. See below.
- **No testbed framing.** Chutney, Shadow, private Tor networks, v2-vs-v3 HSDir
  enumeration — all dead. That was an earlier scope that got dropped. Do not
  reintroduce any of it in code, comments, or docs.
- **One machine: the laptop.** Fedora 44, ThinkPad T14 Gen 5 AMD. Everything
  runs locally. No lab PC, no VPS, no cloud.
- **Python 3 + venv.** Keep the dependency list short enough to list on a slide.

---

## Environment

```bash
sudo dnf install tor
sudo systemctl start tor          # SOCKS5 on 127.0.0.1:9050
```

Verify Tor is actually routing before trusting any result:

```bash
curl --socks5-hostname 127.0.0.1:9050 https://check.torproject.org/api/ip
# must return {"IsTor":true,...}
```

Project deps: `requests[socks]`, `PySocks`, `beautifulsoup4`, `lxml`.
Phase 2 adds a web framework (Flask or FastAPI) — nothing heavier.

---

## Gotchas already paid for — do not rediscover these

- **`socks5h://`, not `socks5://`.** The `h` makes DNS resolve through Tor.
  Without it `.onion` addresses silently fail and everything reads as dead.
- **`torify curl` does not work on `.onion`** — returns
  `curl: (6) Not resolving .onion address (RFC 7686)`. Use
  `curl --socks5-hostname 127.0.0.1:9050 <url>`.
- **The Tor daemon dies silently.** A full cycle reporting `0/N alive` is almost
  always a dead daemon, not dead services. The pinger must check Tor health at
  the start of every cycle and refuse to write a cycle it can't trust.
- **DarkSpider's `requirements.txt` pulls `Gooey` → `wxpython` → fails to build
  on Fedora** (wants gtk+-3.0, libcurl headers). Irrelevant after Phase 1, noted
  in case the repo is consulted for reference.
- **Crawler output contains junk** — clearnet URLs, `127.0.0.1`, Freenet keys.
  Filter strictly to 56-char v3 onion addresses: `[a-z2-7]{56}\.onion`.

---

## Phase 1 — pipeline, no DarkSpider

Goal: three scripts that run end to end on the laptop and produce a real churn
report. No external crawler repo.

DarkSpider is GPL and a whole project — GUI flags, YARA scanning, graph
visualization, extractor modes. We used maybe 100 lines of it. Phase 1 writes
that part ourselves.

> **License note:** if any DarkSpider code is copied rather than reimplemented,
> this project inherits GPL. Either keep it clean-room (read how it works, write
> our own) or license the repo GPL-3.0 and credit DarkSpider + MikeMeliz's
> TorCrawl. Pick one deliberately; don't leave it ambiguous.

### `crawler.py`

- Input: seed URL(s), max depth, thread count, request delay, output path.
- Fetch over `socks5h://127.0.0.1:9050`, generous timeout (Tor is slow — 30s).
- Parse HTML with BeautifulSoup, pull every `href`.
- Resolve relative links against the page URL, strip fragments and trailing
  slashes so the same page isn't counted twice.
- Keep only `[a-z2-7]{56}\.onion` matches. Discard everything else.
- BFS by depth with a `visited` set. Thread pool for fetches, but keep a delay
  so we're not hammering anything.
- Failures are normal and expected — log and continue, never crash the crawl.
- Output two files:
  - `links.txt` — one URL per line, deduplicated
  - `edges.json` — `{source_url: [target_urls]}`, needed for the Phase 2 graph
    view. DarkSpider produced this as `network_structure.json`; don't lose it.

Seed: `http://zqktlwiuavvvqqt4ybvgvi7tyo4hjl5xgfuvpdf6otjiycgwqbym2qad.onion`
(Hidden Wiki v3). **Verify it's still live before crawling** — these addresses
rotate. Cross-check against dark.fail if it's dead.

Prior run for reference: depth 3 → 3,940 unique onion URLs.

### `pinger.py`

Already written and working; port it as-is, with these changes:

- Check Tor health at cycle start. If Tor is down, log the cycle as `aborted`
  and skip it — don't write 3,940 false "dead" rows.
- Write a row to a `cycles` table per cycle so gaps are explicit in the data
  (see schema).
- Make the interval configurable. 6h was the old default; on a laptop that gets
  closed, shorter cycles over a focused multi-day window give better data.

### `report.py`

Already written; port as-is. Reads the DB, prints:

- total discovered / ever alive / never responded / consistently alive / churned
- per-cycle alive-dead breakdown with timestamps
- add: average observed lifespan, and explicit gap markers where cycles aborted

### Schema

```sql
urls(url PRIMARY KEY, first_seen, last_alive, total_pings, alive_pings)
pings(id, url, status, status_code, timestamp, cycle_id)
cycles(cycle_id PRIMARY KEY, started_at, ended_at, total, alive, status)
       -- status: completed | aborted
```

`cycles` is new and matters: without it a gap caused by a closed laptop is
indistinguishable from mass service death, which would wreck the headline
finding.

### Phase 1 is done when

`python crawler.py` → `python pinger.py` (several cycles) → `python report.py`
produces real churn numbers on the laptop, with no DarkSpider installed.

---

## Phase 2 — the app

Wrap Phase 1 into one thing a person can run and look at. Terminal output is not
a demo.

### Shape

Local web app — Flask or FastAPI serving a single page on `127.0.0.1:PORT`,
reading the same SQLite DB. One entry point:

```bash
python app.py          # starts web UI + background pinger
python app.py crawl    # run a crawl
python app.py report   # print report to terminal (Phase 1 behaviour kept)
```

### UI — four things, nothing more

1. **Summary tiles** — total discovered, currently alive, ever alive, churn %.
2. **Alive-over-time line chart** — one point per completed cycle. This is the
   money visual; it's the whole argument in one picture. Aborted cycles render
   as gaps, not zeros.
3. **URL table** — url, first seen, last alive, uptime %, current status.
   Sortable, filterable, paginated. 4k rows, so paginate server-side.
4. **Link graph** (if time allows) — `edges.json` rendered as a network. Cut
   this first if the deadline tightens.

### Background pinger

Runs in a thread or as a separate process the app supervises. Must survive the
laptop sleeping — on wake, resume cleanly and mark the missed window as a gap,
not as a dead cycle. Expose start/stop and "last cycle" status in the UI.

### Keep out

No auth, no multi-user, no Docker, no cloud deploy, no live-content rendering of
crawled pages. It reads a local DB and draws charts.

### Phase 2 is done when

A laptop with Tor running and a populated DB shows a working dashboard, and the
pinger keeps adding cycles with the app open.

---

## Open questions — answer before Phase 1 coding

1. **Is the old data recoverable?** `links.txt` and `onions.db` were produced on
   a different machine. If they weren't copied over, Phase 1 starts with a fresh
   crawl — which costs hours. Check first.
2. **How many cycles exist in whatever DB survives?** The churn story needs at
   least 3–4 completed cycles spread over a day or more. One cycle is a
   snapshot, not churn.
3. **Does the seed still resolve?** If the Hidden Wiki address rotated, get a
   current one before crawling.

A laptop that sleeps is a bad host for a 24/7 pinger. If a headless always-on
Linux box is available on the same network, running only the pinger there and
pointing the app at the resulting DB solves the gap problem without adding any
third-party system — same code, different host. Optional.

---

## Status

Done: crawl logic understood, pinger + report written and working, Tor/proxy
issues solved, scope locked.

Not done: Phase 1 de-DarkSpidering, Phase 2 app, final writeup. The existing
synopsis docx, 12-slide deck, and dean letters all describe the abandoned
testbed project and need rewriting to match this.
