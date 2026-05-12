from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urlparse


# Apache/NGINX Combined Log Format
_COMBINED_RE = re.compile(
    r'(?P<ip>\S+) \S+ \S+ \[.*?\] "(?P<method>[A-Z]+) (?P<path>\S+) HTTP/[\d.]+" (?P<status>\d+)'
)

# Generic JSON log keys we look for
_JSON_URL_KEYS = ("url", "request_url", "path", "uri", "endpoint")
_JSON_METHOD_KEYS = ("method", "http_method", "verb", "request_method")


def parse_log_file(log_path: str) -> list[dict]:
    """
    Parse an HTTP log file (Apache/NGINX combined, JSON lines, or HAR) and return
    a deduplicated list of endpoint candidates:
    [{"method": str, "url": str, "count": int}, ...]
    """
    path = Path(log_path)
    if not path.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")

    text = path.read_text(encoding="utf-8", errors="replace")

    # Detect format
    if path.suffix.lower() == ".har":
        entries = _parse_har(text)
    else:
        first_non_empty = next((l.strip() for l in text.splitlines() if l.strip()), "")
        if first_non_empty.startswith("{"):
            entries = _parse_json_lines(text)
        else:
            entries = _parse_combined_log(text)

    return _deduplicate(entries)


def _parse_combined_log(text: str) -> list[dict]:
    entries = []
    for line in text.splitlines():
        m = _COMBINED_RE.search(line)
        if m:
            entries.append({"method": m.group("method"), "path": m.group("path")})
    return entries


def _parse_json_lines(text: str) -> list[dict]:
    entries = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        method = next((obj[k] for k in _JSON_METHOD_KEYS if k in obj), "GET")
        url = next((obj[k] for k in _JSON_URL_KEYS if k in obj), None)
        if url:
            entries.append({"method": str(method).upper(), "path": str(url)})
    return entries


def _parse_har(text: str) -> list[dict]:
    entries = []
    try:
        har = json.loads(text)
        for entry in har.get("log", {}).get("entries", []):
            req = entry.get("request", {})
            method = req.get("method", "GET").upper()
            url = req.get("url", "")
            if url:
                entries.append({"method": method, "path": url})
    except (json.JSONDecodeError, KeyError):
        pass
    return entries


def _normalise_path(path: str) -> str:
    """Replace numeric path segments with {id} for deduplication."""
    # Handle full URLs
    parsed = urlparse(path)
    p = parsed.path if parsed.scheme else path.split("?")[0]
    p = re.sub(r"/\d+(/|$)", r"/{id}\1", p)
    p = re.sub(r"/[0-9a-f]{8}-[0-9a-f-]{27}(/|$)", r"/{id}\1", p, flags=re.IGNORECASE)
    return p


def _deduplicate(entries: list[dict]) -> list[dict]:
    seen: dict[tuple, int] = {}
    for e in entries:
        key = (e["method"], _normalise_path(e["path"]))
        seen[key] = seen.get(key, 0) + 1

    result = []
    for (method, normalised_path), count in sorted(seen.items(), key=lambda x: -x[1]):
        result.append({"method": method, "url": normalised_path, "count": count})
    return result
