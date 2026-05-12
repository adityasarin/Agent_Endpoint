from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qs

from apiconsumer.models.pipeline import EndpointSpec


def parse_rest_input(endpoint_url: str, sample_file: str | None = None) -> EndpointSpec:
    """
    Build an EndpointSpec from a URL and an optional sample request JSON file.

    Sample file schema:
    {
      "method": "GET",
      "headers": {"Authorization": "Bearer ..."},
      "query_params": {"limit": "100"},
      "body": null
    }
    """
    parsed = urlparse(endpoint_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path or ""

    # Absorb any query params already in the URL
    url_params: dict[str, str] = {}
    if parsed.query:
        for k, v in parse_qs(parsed.query).items():
            url_params[k] = v[0]

    default_headers: dict[str, str] = {}
    default_params: dict[str, str] = dict(url_params)
    method = "GET"

    if sample_file:
        sample_path = Path(sample_file)
        if not sample_path.exists():
            raise FileNotFoundError(f"Sample file not found: {sample_file}")
        with sample_path.open() as f:
            sample: dict[str, Any] = json.load(f)

        method = sample.get("method", "GET").upper()
        default_headers = {str(k): str(v) for k, v in (sample.get("headers") or {}).items()}
        for k, v in (sample.get("query_params") or {}).items():
            default_params[str(k)] = str(v)

    return EndpointSpec(
        api_type="rest",
        base_url=base_url,
        path=path,
        method=method,
        default_headers=default_headers,
        default_query_params=default_params,
    )
