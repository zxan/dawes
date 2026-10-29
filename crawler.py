import argparse
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from tor_utils import DEFAULT_PROXY, check_tor, proxies
from url_utils import normalize_onion_url


DEFAULT_SEED = (
    "http://zqktlwiuavvvqqt4ybvgvi7tyo4hjl5xgfuvpdf6otjiycgwqbym2qad.onion"
)
USER_AGENT = "OnionChurnTracker/1.0"


class RateLimiter:
    def __init__(self, delay):
        self.delay = max(0.0, delay)
        self.lock = threading.Lock()
        self.next_request = 0.0

    def wait(self):
        if not self.delay:
            return
        with self.lock:
            now = time.monotonic()
            wait_for = max(0.0, self.next_request - now)
            self.next_request = max(now, self.next_request) + self.delay
        if wait_for:
            time.sleep(wait_for)


class OnionCrawler:
    def __init__(self, proxy, timeout, workers, delay):
        self.proxy = proxy
        self.timeout = timeout
        self.workers = workers
        self.rate_limiter = RateLimiter(delay)
        self.local = threading.local()

    def _session(self):
        if not hasattr(self.local, "session"):
            session = requests.Session()
            session.proxies.update(proxies(self.proxy))
            session.headers.update({"User-Agent": USER_AGENT})
            self.local.session = session
        return self.local.session

    def fetch_links(self, url):
        self.rate_limiter.wait()
        try:
            response = self._session().get(url, timeout=self.timeout)
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "").lower()
            if "html" not in content_type:
                return []
            soup = BeautifulSoup(response.content, "lxml")
            found = set()
            for tag in soup.find_all("a", href=True):
                target = normalize_onion_url(tag.get("href"), response.url)
                if target:
                    found.add(target)
            logging.info("Fetched %s (%d onion links)", url, len(found))
            return sorted(found)
        except requests.RequestException as exc:
            logging.warning("Could not fetch %s: %s", url, exc)
            return []

    def crawl(self, seeds, max_depth):
        discovered = set(seeds)
        visited = set()
        edges = {}
        frontier = list(seeds)

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            for depth in range(max_depth):
                current = [url for url in frontier if url not in visited]
                if not current:
                    break
                logging.info("Depth %d: fetching %d URLs", depth + 1, len(current))
                futures = {executor.submit(self.fetch_links, url): url for url in current}
                next_frontier = set()
                for future in as_completed(futures):
                    source = futures[future]
                    visited.add(source)
                    try:
                        targets = future.result()
                    except Exception as exc:  # keep one worker bug from ending a crawl
                        logging.exception("Unexpected crawler failure for %s: %s", source, exc)
                        targets = []
                    edges[source] = targets
                    for target in targets:
                        discovered.add(target)
                        if target not in visited:
                            next_frontier.add(target)
                frontier = sorted(next_frontier)

        return sorted(discovered), edges


def write_outputs(urls, edges, links_path, edges_path):
    links_path = Path(links_path)
    edges_path = Path(edges_path)
    links_path.parent.mkdir(parents=True, exist_ok=True)
    edges_path.parent.mkdir(parents=True, exist_ok=True)
    links_path.write_text("".join(f"{url}\n" for url in urls), encoding="utf-8")
    edges_path.write_text(
        json.dumps(edges, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Crawl v3 onion links through Tor")
    parser.add_argument("--seed", action="append", default=None, help="repeat for more seeds")
    parser.add_argument("--depth", type=int, default=3)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--delay", type=float, default=0.25, help="seconds between starts")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--proxy", default=DEFAULT_PROXY)
    parser.add_argument("--links", default="links.txt")
    parser.add_argument("--edges", default="edges.json")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    if args.depth < 1 or args.workers < 1 or args.delay < 0 or args.timeout <= 0:
        raise SystemExit("depth/workers/timeout must be positive and delay cannot be negative")

    seeds = []
    for raw_seed in args.seed or [DEFAULT_SEED]:
        seed = normalize_onion_url(raw_seed)
        if not seed:
            raise SystemExit(f"Not a valid v3 onion URL: {raw_seed}")
        seeds.append(seed)

    healthy, message = check_tor(args.proxy, min(args.timeout, 20))
    logging.info(message)
    if not healthy:
        raise SystemExit("Refusing to crawl without a verified Tor connection")

    crawler = OnionCrawler(args.proxy, args.timeout, args.workers, args.delay)
    urls, edges = crawler.crawl(sorted(set(seeds)), args.depth)
    write_outputs(urls, edges, args.links, args.edges)
    logging.info("Crawl finished: %d URLs, %d fetched pages", len(urls), len(edges))


if __name__ == "__main__":
    main()
