import re
from urllib.parse import urljoin, urlsplit, urlunsplit


ONION_HOST_RE = re.compile(r"^[a-z2-7]{56}\.onion$")


def normalize_onion_url(value, base_url=None):
    """Return a normalized v3 onion URL, or None when value is not one."""
    if not value:
        return None

    candidate = urljoin(base_url, value.strip()) if base_url else value.strip()
    try:
        parts = urlsplit(candidate)
        hostname = (parts.hostname or "").lower()
        if parts.scheme.lower() not in {"http", "https"}:
            return None
        if not ONION_HOST_RE.fullmatch(hostname):
            return None
        if parts.username or parts.password:
            return None

        port = parts.port
        scheme = parts.scheme.lower()
        default_port = (scheme == "http" and port == 80) or (
            scheme == "https" and port == 443
        )
        netloc = hostname if port is None or default_port else f"{hostname}:{port}"
        path = parts.path.rstrip("/")
        return urlunsplit((scheme, netloc, path, parts.query, ""))
    except (TypeError, ValueError):
        return None
