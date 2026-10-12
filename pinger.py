import sqlite3
import requests
import time
import re
from datetime import datetime

LINKS_FILE = "DarkSpider/output/zqktlwiuavvvqqt4ybvgvi7tyo4hjl5xgfuvpdf6otjiycgwqbym2qad.onion/links.txt"
DB_FILE = "onions.db"
INTERVAL = 6 * 60 * 60  # 6 hours
TIMEOUT = 30

PROXIES = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050"
}

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS pings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT,
        status TEXT,
        status_code INTEGER,
        timestamp TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS urls (
        url TEXT PRIMARY KEY,
        first_seen TEXT,
        last_alive TEXT,
        total_pings INTEGER DEFAULT 0,
        alive_pings INTEGER DEFAULT 0
    )''')
    conn.commit()
    return conn

def load_urls(path):
    with open(path) as f:
        raw = f.read()
    return list(set(re.findall(r'http[s]?://[a-z2-7]{56}\.onion[^\s"\'<>]*', raw)))

def ping(url):
    try:
        r = requests.get(url, proxies=PROXIES, timeout=TIMEOUT)
        return "alive", r.status_code
    except:
        return "dead", None

def run_cycle(conn, urls):
    c = conn.cursor()
    now = datetime.utcnow().isoformat()
    print(f"\n[{now}] Starting ping cycle — {len(urls)} URLs")
    alive = 0

    for i, url in enumerate(urls):
        status, code = ping(url)
        if status == "alive":
            alive += 1

        c.execute("INSERT INTO pings (url, status, status_code, timestamp) VALUES (?,?,?,?)",
                  (url, status, code, now))

        c.execute('''INSERT INTO urls (url, first_seen, last_alive, total_pings, alive_pings)
                     VALUES (?, ?, ?, 1, ?)
                     ON CONFLICT(url) DO UPDATE SET
                         total_pings = total_pings + 1,
                         alive_pings = alive_pings + ?,
                         last_alive = CASE WHEN ? = 'alive' THEN ? ELSE last_alive END''',
                  (url, now, now if status == "alive" else None, 1 if status == "alive" else 0,
                   1 if status == "alive" else 0, status, now))

        if i % 100 == 0:
            print(f"  {i}/{len(urls)} done...")
            conn.commit()

    conn.commit()
    print(f"  Cycle done — {alive} alive / {len(urls) - alive} dead")

def main():
    conn = init_db()
    urls = load_urls(LINKS_FILE)
    print(f"Loaded {len(urls)} unique onion URLs")

    while True:
        run_cycle(conn, urls)
        print(f"Sleeping 6 hours...")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()