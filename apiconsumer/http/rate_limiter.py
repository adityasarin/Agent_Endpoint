from __future__ import annotations

import asyncio
import time


class RateLimiter:
    """
    Token bucket rate limiter. Automatically adjusts based on X-RateLimit-* headers.
    Falls back to a safe default (1 req/sec) if no headers are seen.
    """

    def __init__(self, requests_per_second: float = 5.0):
        self._rps = requests_per_second
        self._min_interval = 1.0 / requests_per_second
        self._last_request_time: float = 0.0

    def update_from_headers(self, headers: dict[str, str]) -> None:
        remaining = _header_int(headers, "x-ratelimit-remaining")
        reset_ts = _header_int(headers, "x-ratelimit-reset")

        if remaining is not None and reset_ts is not None:
            now = time.time()
            seconds_until_reset = max(reset_ts - now, 1)
            if remaining > 0:
                safe_rps = remaining / seconds_until_reset
                self._rps = max(min(safe_rps, 20.0), 0.1)
                self._min_interval = 1.0 / self._rps

        # Slow down hard on near-exhaustion
        if remaining is not None and remaining < 5:
            if reset_ts:
                sleep_for = max(reset_ts - time.time(), 1)
                time.sleep(sleep_for)

    async def wait(self) -> None:
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def wait_sync(self) -> None:
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()


def _header_int(headers: dict[str, str], key: str) -> int | None:
    val = headers.get(key) or headers.get(key.title())
    try:
        return int(val) if val else None
    except (ValueError, TypeError):
        return None


def parse_retry_after(headers: dict[str, str]) -> float:
    """Return seconds to wait from Retry-After header, or 5.0 default."""
    val = headers.get("retry-after") or headers.get("Retry-After")
    if not val:
        return 5.0
    try:
        return float(val)
    except ValueError:
        # Could be an HTTP-date — just default
        return 30.0
