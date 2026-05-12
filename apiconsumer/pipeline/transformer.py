from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any


def jsonpath_get(data: Any, path: str) -> Any:
    """
    Minimal JSONPath evaluator supporting:
      $            → root
      $.field      → top-level field
      $.a.b.c      → nested fields
      $.arr[*].f   → map over array elements
    """
    if path in ("$", "", None):
        return data

    # Strip leading "$."
    path = path.lstrip("$").lstrip(".")
    if not path:
        return data

    parts = path.split(".")
    current: Any = data

    for part in parts:
        if part.endswith("[*]"):
            key = part[:-3]
            if key:
                current = current.get(key, []) if isinstance(current, dict) else []
            # After [*] the remaining path will be applied per element by the caller
            return current if isinstance(current, list) else []
        elif isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            # Implicit map over list
            current = [item.get(part) for item in current if isinstance(item, dict)]
        else:
            return None

    return current


def extract_records(response_body: Any, data_path: str) -> list[dict]:
    """
    Given a parsed JSON response body and a JSONPath data_path,
    return the list of record dicts at that path.
    """
    if isinstance(response_body, str):
        try:
            response_body = json.loads(response_body)
        except json.JSONDecodeError:
            return []

    value = jsonpath_get(response_body, data_path)

    if isinstance(value, list):
        return [r for r in value if isinstance(r, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def flatten_record(record: dict, fields: list[str] | None = None, prefix: str = "") -> dict:
    """
    Flatten a nested dict into a single-level dict with dot-notation keys.
    If fields is non-empty, only keep those keys (after flattening).
    """
    flat: dict = {}

    def _flatten(obj: Any, p: str) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                _flatten(v, f"{p}.{k}" if p else k)
        elif isinstance(obj, list):
            flat[p] = json.dumps(obj)
        else:
            flat[p] = obj

    _flatten(record, prefix)

    if fields:
        flat = {k: v for k, v in flat.items() if k in fields}

    return flat


def infer_types(records: list[dict]) -> dict[str, str]:
    """
    Infer column types from a sample of records.
    Returns {field_name: type_hint} where type_hint is one of
    string / integer / float / boolean / datetime / null.
    """
    type_map: dict[str, set] = {}
    for record in records[:200]:
        for k, v in record.items():
            type_map.setdefault(k, set())
            type_map[k].add(_python_type(v))

    result = {}
    for k, types in type_map.items():
        types.discard("null")
        if not types:
            result[k] = "null"
        elif len(types) == 1:
            result[k] = types.pop()
        else:
            result[k] = "string"  # Mixed → string
    return result


def _python_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        # Sniff datetimes
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                datetime.strptime(value[:19], fmt)
                return "datetime"
            except ValueError:
                pass
        return "string"
    return "string"


def record_hash(record: dict) -> str:
    """Stable content hash for deduplication."""
    canonical = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def format_timestamp(dt: datetime, fmt: str) -> str | int:
    if fmt == "iso8601":
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if fmt == "unix_seconds":
        return int(dt.timestamp())
    if fmt == "unix_millis":
        return int(dt.timestamp() * 1000)
    if fmt == "YYYY-MM-DD":
        return dt.strftime("%Y-%m-%d")
    # Custom strftime
    return dt.strftime(fmt)
