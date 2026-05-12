from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

from apiconsumer.http.auth import apply_auth
from apiconsumer.http.rate_limiter import RateLimiter, parse_retry_after
from apiconsumer.models.pipeline import AuthConfig


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (429, 500, 502, 503, 504)
    return isinstance(exc, (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError))


@retry(
    wait=wait_exponential(multiplier=1, min=1, max=60),
    stop=stop_after_attempt(6),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
def probe_sync(
    url: str,
    method: str = "GET",
    headers: dict | None = None,
    params: dict | None = None,
    json: Any = None,
    auth: Optional[AuthConfig] = None,
    timeout: float = 15.0,
) -> httpx.Response:
    kwargs: dict[str, Any] = {
        "headers": dict(headers or {}),
        "params": params or {},
        "timeout": timeout,
    }
    if json is not None:
        kwargs["json"] = json
    if auth:
        apply_auth(kwargs, auth)

    with httpx.Client(follow_redirects=True, max_redirects=5) as client:
        resp = client.request(method.upper(), url, **kwargs)
        if resp.status_code == 429:
            wait = parse_retry_after(dict(resp.headers))
            time.sleep(wait)
            resp.raise_for_status()
        resp.raise_for_status()
        return resp


class AsyncHTTPClient:
    """
    Single shared httpx.AsyncClient for an extraction run.
    Must be used as an async context manager.
    """

    def __init__(self, auth: Optional[AuthConfig] = None, rate_limiter: Optional[RateLimiter] = None):
        self._auth = auth
        self._rate_limiter = rate_limiter or RateLimiter()
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "AsyncHTTPClient":
        self._client = httpx.AsyncClient(
            follow_redirects=True,
            max_redirects=5,
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(max_connections=5, max_keepalive_connections=5),
        )
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=60),
        stop=stop_after_attempt(6),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    )
    async def request(
        self,
        method: str,
        url: str,
        headers: dict | None = None,
        params: dict | None = None,
        json: Any = None,
        data: Any = None,
    ) -> httpx.Response:
        await self._rate_limiter.wait()

        kwargs: dict[str, Any] = {
            "headers": dict(headers or {}),
            "params": params or {},
        }
        if json is not None:
            kwargs["json"] = json
        if data is not None:
            kwargs["data"] = data
        if self._auth:
            apply_auth(kwargs, self._auth)

        t0 = time.monotonic()
        resp = await self._client.request(method.upper(), url, **kwargs)
        elapsed_ms = (time.monotonic() - t0) * 1000

        self._rate_limiter.update_from_headers(dict(resp.headers))

        if resp.status_code == 429:
            wait_secs = parse_retry_after(dict(resp.headers))
            await asyncio.sleep(wait_secs)
            resp.raise_for_status()

        resp.raise_for_status()
        resp.elapsed_ms = elapsed_ms  # type: ignore[attr-defined]
        return resp
