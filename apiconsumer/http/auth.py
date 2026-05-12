from __future__ import annotations

import time
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from apiconsumer.models.pipeline import AuthConfig


def apply_auth(request_kwargs: dict, auth: "AuthConfig") -> dict:
    """Mutate request_kwargs in-place with auth headers/params. Returns the same dict."""
    if auth.auth_type == "none":
        return request_kwargs

    if auth.auth_type == "api_key":
        if auth.api_key_in == "header":
            request_kwargs.setdefault("headers", {})[auth.api_key_header] = auth.api_key or ""
        else:
            request_kwargs.setdefault("params", {})[auth.api_key_header] = auth.api_key or ""

    elif auth.auth_type == "bearer":
        token = _get_bearer_token(auth)
        request_kwargs.setdefault("headers", {})["Authorization"] = f"Bearer {token}"

    elif auth.auth_type == "basic":
        request_kwargs["auth"] = (auth.username or "", auth.password or "")

    elif auth.auth_type == "oauth2_client_credentials":
        token = _get_oauth2_token(auth)
        request_kwargs.setdefault("headers", {})["Authorization"] = f"Bearer {token}"

    return request_kwargs


def _get_bearer_token(auth: "AuthConfig") -> str:
    return auth.bearer_token or ""


def _get_oauth2_token(auth: "AuthConfig") -> str:
    # Check cached token with 60-second buffer
    if auth._cached_token and auth._token_expires_at:
        if time.time() < auth._token_expires_at - 60:
            return auth._cached_token

    if not auth.token_url:
        raise ValueError("oauth2_client_credentials requires token_url")

    data = {
        "grant_type": "client_credentials",
        "client_id": auth.client_id or "",
        "client_secret": auth.client_secret or "",
    }
    if auth.scopes:
        data["scope"] = " ".join(auth.scopes)

    resp = httpx.post(auth.token_url, data=data, timeout=15)
    resp.raise_for_status()
    body = resp.json()

    auth._cached_token = body["access_token"]
    expires_in = body.get("expires_in", 3600)
    auth._token_expires_at = time.time() + expires_in
    return auth._cached_token
