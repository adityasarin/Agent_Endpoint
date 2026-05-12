from __future__ import annotations

import json
import time
from typing import Any, Callable

import click
import httpx

from apiconsumer.http.auth import apply_auth
from apiconsumer.http.client import probe_sync
from apiconsumer.http.rate_limiter import RateLimiter
from apiconsumer.models.pipeline import AuthConfig, EndpointSpec, PaginationConfig, PipelineConfig, TimestampConfig
from apiconsumer.pipeline.paginator import detect_pagination
from apiconsumer.pipeline.transformer import extract_records, infer_types, jsonpath_get


# ── Tool schemas (Gemini / JSON Schema format) ───────────────────────────────

TOOL_SCHEMAS = [
    {
        "name": "probe_endpoint",
        "description": (
            "Send a lightweight probe (HEAD or GET) to an endpoint URL to check reachability, "
            "discover response headers (Content-Type, X-RateLimit-*, Link, auth challenges), "
            "and measure latency. Returns status code, headers, and body preview."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "method": {"type": "string", "enum": ["HEAD", "GET", "OPTIONS"]},
                "headers": {"type": "object", "description": "Any headers to include"},
                "params": {"type": "object", "description": "Query parameters"},
            },
            "required": ["url"],
        },
    },
    {
        "name": "send_sample_request",
        "description": (
            "Execute a full HTTP request as specified. Returns status, headers, "
            "body (truncated to 4000 chars), and parsed JSON structure."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "method": {"type": "string"},
                "url": {"type": "string"},
                "headers": {"type": "object"},
                "params": {"type": "object"},
                "body": {"type": "string", "description": "Request body as JSON string or plain string"},
            },
            "required": ["method", "url"],
        },
    },
    {
        "name": "extract_response_schema",
        "description": (
            "Analyse a JSON response body string to extract field names, infer types, "
            "identify the records array path, and detect candidate timestamp fields."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "response_body": {"type": "string", "description": "Raw JSON string of the response"},
                "data_path_hint": {"type": "string", "description": "JSONPath hint for where the records array is, e.g. '$.data'"},
            },
            "required": ["response_body"],
        },
    },
    {
        "name": "detect_pagination_pattern",
        "description": (
            "Analyse response headers and body to detect pagination type: "
            "cursor, offset/limit, page/per_page, keyset, or Link header. "
            "Returns detected strategy, confidence, and evidence."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "response_headers": {"type": "object"},
                "response_body": {"type": "string", "description": "Raw JSON string"},
                "request_url": {"type": "string"},
            },
            "required": ["response_headers", "response_body"],
        },
    },
    {
        "name": "test_auth",
        "description": "Test an auth configuration against an endpoint. Returns success/failure.",
        "parameters": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "auth_type": {"type": "string", "enum": ["none", "api_key", "bearer", "basic", "oauth2_client_credentials"]},
                "auth_params": {
                    "type": "object",
                    "description": "Keys: api_key, api_key_header, api_key_in, bearer_token, username, password, client_id, client_secret, token_url",
                },
            },
            "required": ["url", "auth_type", "auth_params"],
        },
    },
    {
        "name": "ask_user",
        "description": (
            "Present clarifying questions to the user interactively. "
            "Use for: auth credentials, date range, output format, field selection, documentation. "
            "Returns dict of {key: answer}."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "questions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "key": {"type": "string"},
                            "question": {"type": "string"},
                            "type": {"type": "string", "enum": ["text", "password", "choice", "confirm"]},
                            "choices": {"type": "array", "items": {"type": "string"}},
                            "default": {"type": "string"},
                        },
                        "required": ["key", "question", "type"],
                    },
                }
            },
            "required": ["questions"],
        },
    },
    {
        "name": "finalize_pipeline_config",
        "description": (
            "Save the completed pipeline configuration. This ends the learning phase. "
            "Call this only when you have enough information to start extraction."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Human-readable pipeline name"},
                "endpoint": {
                    "type": "object",
                    "properties": {
                        "api_type": {"type": "string", "enum": ["rest", "soap"]},
                        "base_url": {"type": "string"},
                        "path": {"type": "string"},
                        "method": {"type": "string"},
                        "default_headers": {"type": "object"},
                        "default_query_params": {"type": "object"},
                        "data_path": {"type": "string", "description": "JSONPath to records array, e.g. '$.data'"},
                        "soap_action": {"type": "string"},
                        "soap_body_template": {"type": "string"},
                    },
                    "required": ["base_url"],
                },
                "auth": {
                    "type": "object",
                    "properties": {
                        "auth_type": {"type": "string"},
                        "api_key": {"type": "string"},
                        "api_key_header": {"type": "string"},
                        "api_key_in": {"type": "string"},
                        "bearer_token": {"type": "string"},
                        "username": {"type": "string"},
                        "password": {"type": "string"},
                        "client_id": {"type": "string"},
                        "client_secret": {"type": "string"},
                        "token_url": {"type": "string"},
                    },
                },
                "pagination": {
                    "type": "object",
                    "properties": {
                        "strategy": {"type": "string"},
                        "page_param": {"type": "string"},
                        "per_page_param": {"type": "string"},
                        "page_size": {"type": "integer"},
                        "offset_param": {"type": "string"},
                        "cursor_param": {"type": "string"},
                        "cursor_response_path": {"type": "string"},
                        "keyset_param": {"type": "string"},
                        "keyset_direction": {"type": "string"},
                        "link_rel": {"type": "string"},
                    },
                },
                "timestamp": {
                    "type": "object",
                    "properties": {
                        "param_name": {"type": "string"},
                        "param_format": {"type": "string"},
                        "param_location": {"type": "string"},
                        "end_param_name": {"type": "string"},
                        "response_timestamp_field": {"type": "string"},
                        "window_size_hours": {"type": "integer"},
                    },
                },
                "output_format": {"type": "string", "enum": ["csv", "parquet", "both"]},
                "output_dir": {"type": "string"},
                "fields_to_extract": {"type": "array", "items": {"type": "string"}},
                "extra_static_params": {"type": "object"},
            },
            "required": ["name", "endpoint"],
        },
    },
    {
        "name": "approve_batch",
        "description": "Signal that the current batch looks good and extraction should continue.",
        "parameters": {
            "type": "object",
            "properties": {
                "notes": {"type": "string", "description": "Optional observation notes"},
            },
        },
    },
]


# ── Tool implementations ──────────────────────────────────────────────────────

def _tool_probe_endpoint(url: str, method: str = "GET", headers: dict | None = None, params: dict | None = None) -> dict:
    try:
        t0 = time.monotonic()
        resp = probe_sync(url, method=method, headers=headers or {}, params=params or {})
        elapsed_ms = (time.monotonic() - t0) * 1000
        body = resp.text[:500]
        rate_limit_headers = {
            k: v for k, v in resp.headers.items()
            if k.lower().startswith("x-ratelimit") or k.lower() == "retry-after"
        }
        return {
            "success": True,
            "status_code": resp.status_code,
            "response_time_ms": round(elapsed_ms, 1),
            "content_type": resp.headers.get("content-type", ""),
            "headers": dict(resp.headers),
            "body_preview": body,
            "requires_auth": False,
            "rate_limit_headers": rate_limit_headers,
        }
    except httpx.HTTPStatusError as e:
        return {
            "success": False,
            "status_code": e.response.status_code,
            "requires_auth": e.response.status_code in (401, 403),
            "error": f"HTTP {e.response.status_code}: {e.response.text[:200]}",
        }
    except Exception as e:
        return {"success": False, "error": f"{type(e).__name__}: {e}"}


def _tool_send_sample_request(
    method: str, url: str,
    headers: dict | None = None,
    params: dict | None = None,
    body: Any = None,
) -> dict:
    try:
        kwargs: dict[str, Any] = {
            "headers": headers or {},
            "params": params or {},
            "timeout": 20.0,
            "follow_redirects": True,
        }
        if body:
            if isinstance(body, dict):
                kwargs["json"] = body
            else:
                kwargs["content"] = str(body).encode()

        t0 = time.monotonic()
        with httpx.Client() as client:
            resp = client.request(method.upper(), url, **kwargs)
        elapsed_ms = (time.monotonic() - t0) * 1000

        body_text = resp.text[:4000]
        parsed = None
        try:
            parsed = resp.json()
            # Summarise structure
            if isinstance(parsed, dict):
                parsed_summary = {k: type(v).__name__ for k, v in list(parsed.items())[:20]}
            elif isinstance(parsed, list):
                parsed_summary = {"type": "array", "length": len(parsed), "first_item_keys": list(parsed[0].keys())[:15] if parsed and isinstance(parsed[0], dict) else []}
            else:
                parsed_summary = {"type": type(parsed).__name__}
        except Exception:
            parsed_summary = None

        return {
            "success": True,
            "status_code": resp.status_code,
            "response_time_ms": round(elapsed_ms, 1),
            "content_type": resp.headers.get("content-type", ""),
            "headers": dict(resp.headers),
            "body_preview": body_text,
            "parsed_structure": parsed_summary,
        }
    except Exception as e:
        return {"success": False, "error": f"{type(e).__name__}: {e}"}


def _tool_extract_response_schema(response_body: str, data_path_hint: str | None = None) -> dict:
    try:
        data = json.loads(response_body)
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"Invalid JSON: {e}"}

    # Try to find the records array
    candidates: list[tuple[str, list]] = []
    if isinstance(data, list):
        candidates.append(("$", data))
    elif isinstance(data, dict):
        if data_path_hint:
            from apiconsumer.pipeline.transformer import jsonpath_get
            val = jsonpath_get(data, data_path_hint)
            if isinstance(val, list):
                candidates.append((data_path_hint, val))
        for k, v in data.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                candidates.append((f"$.{k}", v))

    schema_info = []
    chosen_path = "$"
    chosen_records: list[dict] = []

    if candidates:
        # Pick the largest array
        chosen_path, chosen_records = max(candidates, key=lambda x: len(x[1]))
        types = infer_types(chosen_records[:50])
        schema_info = [{"field": k, "type": v} for k, v in types.items()]

    # Find timestamp candidates
    ts_candidates = [
        f["field"] for f in schema_info
        if f["type"] == "datetime" or any(kw in f["field"].lower() for kw in ("time", "date", "created", "updated", "timestamp", "at"))
    ]

    top_level_keys = list(data.keys()) if isinstance(data, dict) else []

    return {
        "success": True,
        "records_array_path": chosen_path,
        "record_count_in_sample": len(chosen_records),
        "fields": schema_info[:50],
        "timestamp_candidates": ts_candidates,
        "top_level_keys": top_level_keys,
    }


def _tool_detect_pagination_pattern(
    response_headers: dict,
    response_body: str,
    request_url: str = "",
) -> dict:
    try:
        body = json.loads(response_body) if isinstance(response_body, str) else response_body
    except json.JSONDecodeError:
        body = {}
    hint = detect_pagination(response_headers, body, request_url)
    return {
        "success": True,
        "strategy": hint.detected_strategy,
        "confidence": hint.confidence,
        "evidence": hint.evidence,
        "next_page_example": hint.next_page_example,
    }


def _tool_test_auth(url: str, auth_type: str, auth_params: dict) -> dict:
    try:
        auth = AuthConfig(auth_type=auth_type, **auth_params)
        kwargs: dict[str, Any] = {"headers": {}}
        apply_auth(kwargs, auth)
        with httpx.Client(follow_redirects=True, max_redirects=5) as client:
            resp = client.get(url, headers=kwargs["headers"], timeout=10)
        if resp.status_code in (200, 201, 204):
            return {"success": True, "status_code": resp.status_code, "message": "Authentication successful"}
        if resp.status_code in (401, 403):
            return {"success": False, "status_code": resp.status_code, "message": "Authentication failed — check credentials"}
        return {"success": True, "status_code": resp.status_code, "message": f"Got {resp.status_code} (may be acceptable)"}
    except httpx.HTTPStatusError as e:
        return {"success": False, "status_code": e.response.status_code, "message": f"HTTP {e.response.status_code}"}
    except httpx.ConnectError:
        return {"success": False, "error": "Connection failed — check URL and network"}
    except httpx.TimeoutException:
        return {"success": False, "error": "Request timed out"}
    except Exception:
        return {"success": False, "error": "Auth test failed — check credentials and URL"}


def _tool_ask_user(questions: list[dict]) -> dict:
    """Render interactive prompts in the CLI."""
    answers: dict[str, Any] = {}
    click.echo()
    click.echo(click.style("  Agent needs some information:", fg="cyan", bold=True))
    click.echo()

    for q in questions:
        key = q["key"]
        question_text = q["question"]
        q_type = q.get("type", "text")
        default = q.get("default")
        choices = q.get("choices", [])

        if q_type == "password":
            val = click.prompt(f"  {question_text}", hide_input=True, default=default or "")
        elif q_type == "confirm":
            val = click.confirm(f"  {question_text}", default=(default == "true"))
        elif q_type == "choice" and choices:
            click.echo(f"  {question_text}")
            for i, c in enumerate(choices, 1):
                click.echo(f"    {i}. {c}")
            while True:
                raw = click.prompt("  Enter number", default="1")
                try:
                    idx = int(raw) - 1
                    if 0 <= idx < len(choices):
                        val = choices[idx]
                        break
                except ValueError:
                    pass
                click.echo("  Invalid choice, try again.")
        else:
            val = click.prompt(f"  {question_text}", default=default or "", show_default=bool(default))

        answers[key] = val
        click.echo()

    return {"success": True, "answers": answers}


def _tool_finalize_pipeline_config(
    name: str,
    endpoint: dict,
    auth: dict | None = None,
    pagination: dict | None = None,
    timestamp: dict | None = None,
    output_format: str = "csv",
    output_dir: str = "data/output",
    fields_to_extract: list[str] | None = None,
    extra_static_params: dict | None = None,
) -> dict:
    try:
        config = PipelineConfig(
            name=name,
            endpoint=EndpointSpec(**endpoint),
            auth=AuthConfig(**(auth or {})),
            pagination=PaginationConfig(**(pagination or {})),
            timestamp=TimestampConfig(**(timestamp or {})),
            output_format=output_format,
            output_dir=output_dir,
            fields_to_extract=fields_to_extract or [],
            extra_static_params=extra_static_params or {},
        )
        return {"success": True, "pipeline_id": config.pipeline_id, "config": json.loads(config.model_dump_json())}
    except Exception as e:
        return {"success": False, "error": f"{type(e).__name__}: {e}"}


def _tool_approve_batch(notes: str = "") -> dict:
    return {"success": True, "approved": True, "notes": notes}


# ── Dispatch table ────────────────────────────────────────────────────────────

TOOL_REGISTRY: dict[str, Callable] = {
    "probe_endpoint": _tool_probe_endpoint,
    "send_sample_request": _tool_send_sample_request,
    "extract_response_schema": _tool_extract_response_schema,
    "detect_pagination_pattern": _tool_detect_pagination_pattern,
    "test_auth": _tool_test_auth,
    "ask_user": _tool_ask_user,
    "finalize_pipeline_config": _tool_finalize_pipeline_config,
    "approve_batch": _tool_approve_batch,
}


def dispatch(tool_name: str, tool_input: dict) -> dict:
    fn = TOOL_REGISTRY.get(tool_name)
    if not fn:
        return {"success": False, "error": f"Unknown tool: {tool_name}"}
    try:
        return fn(**tool_input)
    except TypeError as e:
        return {"success": False, "error": f"Invalid tool arguments: {e}"}
    except Exception as e:
        return {"success": False, "error": f"{type(e).__name__}: {e}"}
