import requests


DEFAULT_PROXY = "socks5h://127.0.0.1:9050"
TOR_CHECK_URL = "https://check.torproject.org/api/ip"


def proxies(proxy=DEFAULT_PROXY):
    return {"http": proxy, "https": proxy}


def check_tor(proxy=DEFAULT_PROXY, timeout=20):
    """Return (healthy, message) after verifying traffic exits through Tor."""
    try:
        response = requests.get(
            TOR_CHECK_URL,
            proxies=proxies(proxy),
            timeout=timeout,
            headers={"User-Agent": "OnionChurnTracker/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("IsTor") is True:
            return True, f"Tor ready (exit IP {payload.get('IP', 'unknown')})"
        return False, "Tor check responded, but IsTor was not true"
    except (requests.RequestException, ValueError) as exc:
        return False, f"Tor health check failed: {exc}"
