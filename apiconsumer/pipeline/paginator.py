from __future__ import annotations

import re
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from apiconsumer.models.api_response import FetchResult, PaginationHint
from apiconsumer.models.pipeline import PaginationConfig
from apiconsumer.pipeline.transformer import jsonpath_get


# ── Detection ────────────────────────────────────────────────────────────────

_CURSOR_KEYS = (
    "cursor", "next_cursor", "next_token", "continuation_token",
    "page_token", "after", "after_cursor", "nextCursor", "nextToken",
)
_PAGE_META_CONTAINERS = ("meta", "pagination", "_links", "paging", "page_info")
_OFFSET_PARAMS = ("offset", "skip", "start", "from")
_LIMIT_PARAMS = ("limit", "count", "size", "per_page", "page_size")
_KEYSET_ASC = ("since_id", "after_id", "min_id", "after")
_KEYSET_DESC = ("max_id", "before_id", "before")


def detect_pagination(
    response_headers: dict[str, str],
    response_body: Any,
    request_url: str = "",
) -> PaginationHint:
    """
    Analyse a response to determine the pagination strategy.
    Returns a PaginationHint ordered by confidence.
    """
    headers_lower = {k.lower(): v for k, v in response_headers.items()}

    # 1. Link header (RFC 5988)
    if "link" in headers_lower:
        link_val = headers_lower["link"]
        if 'rel="next"' in link_val or "rel='next'" in link_val:
            next_url = _extract_link_next(link_val)
            return PaginationHint(
                detected_strategy="link_header",
                confidence=0.95,
                evidence=[f"Link header contains rel='next': {link_val[:120]}"],
                next_page_example={"next_url": next_url} if next_url else None,
            )

    if not isinstance(response_body, dict):
        return PaginationHint(detected_strategy="none", confidence=0.5, evidence=["Non-dict response body"])

    # 2. Cursor in top-level body
    for key in _CURSOR_KEYS:
        val = response_body.get(key)
        if val and isinstance(val, str):
            return PaginationHint(
                detected_strategy="cursor",
                confidence=0.90,
                evidence=[f"Found cursor key '{key}' = '{str(val)[:60]}'"],
                next_page_example={"cursor_param": key, "cursor_value": val},
            )

    # 3. Pagination metadata object
    for container in _PAGE_META_CONTAINERS:
        meta = response_body.get(container)
        if isinstance(meta, dict):
            hint = _detect_in_meta(meta, container)
            if hint:
                return hint
        # Also check _links.next for HATEOAS
        if container == "_links" and isinstance(meta, dict) and "next" in meta:
            next_href = meta["next"].get("href") if isinstance(meta["next"], dict) else meta["next"]
            return PaginationHint(
                detected_strategy="link_header",
                confidence=0.88,
                evidence=[f"_links.next found: {next_href}"],
                next_page_example={"next_url": next_href},
            )

    # 4. Offset / limit in original request URL
    if request_url:
        parsed_url = urlparse(request_url)
        qparams = {k.lower(): v[0] for k, v in parse_qs(parsed_url.query).items()}
        offset_key = next((k for k in _OFFSET_PARAMS if k in qparams), None)
        limit_key = next((k for k in _LIMIT_PARAMS if k in qparams), None)
        if offset_key:
            return PaginationHint(
                detected_strategy="offset",
                confidence=0.80,
                evidence=[f"Request URL contains offset param '{offset_key}'"],
                next_page_example={"offset_param": offset_key, "limit_param": limit_key},
            )
        if any(k in qparams for k in ("page", "p", "page_number")):
            page_key = next(k for k in ("page", "p", "page_number") if k in qparams)
            return PaginationHint(
                detected_strategy="page",
                confidence=0.80,
                evidence=[f"Request URL contains page param '{page_key}'"],
                next_page_example={"page_param": page_key, "per_page_param": limit_key},
            )
        keyset_key = next((k for k in _KEYSET_ASC + _KEYSET_DESC if k in qparams), None)
        if keyset_key:
            direction = "asc" if keyset_key in _KEYSET_ASC else "desc"
            return PaginationHint(
                detected_strategy="keyset",
                confidence=0.85,
                evidence=[f"Request URL contains keyset param '{keyset_key}'"],
                next_page_example={"keyset_param": keyset_key, "direction": direction},
            )

    # 5. Total count suggests pagination exists but strategy unknown
    total = response_body.get("total") or response_body.get("total_count") or response_body.get("count")
    if total and isinstance(total, int):
        return PaginationHint(
            detected_strategy="unknown",
            confidence=0.40,
            evidence=[f"Response has total={total} but pagination parameters unclear"],
        )

    return PaginationHint(
        detected_strategy="none",
        confidence=0.60,
        evidence=["No pagination signals detected"],
    )


def _detect_in_meta(meta: dict, container: str) -> Optional[PaginationHint]:
    # Cursor in meta
    for key in _CURSOR_KEYS:
        val = meta.get(key)
        if val and isinstance(val, str):
            return PaginationHint(
                detected_strategy="cursor",
                confidence=0.88,
                evidence=[f"Cursor key '{key}' found in '{container}'"],
                next_page_example={"cursor_response_path": f"$.{container}.{key}"},
            )
    # Page number in meta
    if "current_page" in meta or "page" in meta:
        page_key = "current_page" if "current_page" in meta else "page"
        per_page = meta.get("per_page") or meta.get("page_size") or meta.get("limit")
        return PaginationHint(
            detected_strategy="page",
            confidence=0.88,
            evidence=[f"'{container}.{page_key}' = {meta.get(page_key)}"],
            next_page_example={"page_param": "page", "per_page_param": "per_page" if per_page else None},
        )
    # Has_more flag
    if "has_more" in meta or "has_next_page" in meta:
        return PaginationHint(
            detected_strategy="cursor",
            confidence=0.70,
            evidence=[f"'{container}' has has_more/has_next_page flag"],
        )
    return None


def _extract_link_next(link_header: str) -> Optional[str]:
    """Parse Link: <url>; rel="next" and return the URL."""
    for part in link_header.split(","):
        part = part.strip()
        if 'rel="next"' in part or "rel='next'" in part:
            m = re.search(r"<([^>]+)>", part)
            if m:
                return m.group(1)
    return None


# ── Execution ─────────────────────────────────────────────────────────────────

def build_next_page_params(
    pagination: PaginationConfig,
    current_params: dict,
    fetch_result: FetchResult,
    page_number: int,
) -> Optional[dict]:
    """
    Given the pagination config and the last fetch result, compute the next
    page's query parameters. Returns None when there are no more pages.
    """
    if not fetch_result.has_more:
        return None

    strategy = pagination.strategy
    params = dict(current_params)

    if strategy == "none":
        return None

    if strategy == "cursor":
        cursor = fetch_result.next_cursor
        if not cursor:
            return None
        if pagination.cursor_param:
            params[pagination.cursor_param] = cursor
        return params

    if strategy == "link_header":
        # next_cursor stores the full next URL; handled in extractor
        return params if fetch_result.next_cursor else None

    if strategy == "page":
        next_page = page_number + 1
        if pagination.page_param:
            params[pagination.page_param] = str(next_page)
        if pagination.per_page_param:
            params[pagination.per_page_param] = str(pagination.page_size)
        return params

    if strategy == "offset":
        current_offset = int(params.get(pagination.offset_param or "offset", 0))
        params[pagination.offset_param or "offset"] = str(current_offset + pagination.page_size)
        if pagination.per_page_param:
            params[pagination.per_page_param] = str(pagination.page_size)
        return params

    if strategy == "keyset":
        if not fetch_result.next_cursor:
            return None
        if pagination.keyset_param:
            params[pagination.keyset_param] = fetch_result.next_cursor
        return params

    return None


def extract_next_cursor(
    pagination: PaginationConfig,
    response_body: Any,
    response_headers: dict[str, str],
    records: list[dict],
) -> Optional[str]:
    """
    Extract the value to use as next cursor/keyset from a response.
    """
    strategy = pagination.strategy

    if strategy == "cursor" and pagination.cursor_response_path:
        val = jsonpath_get(response_body, pagination.cursor_response_path)
        return str(val) if val else None

    if strategy == "cursor":
        if isinstance(response_body, dict):
            for key in _CURSOR_KEYS:
                val = response_body.get(key)
                if val:
                    return str(val)
            # Check nested in meta containers
            for container in _PAGE_META_CONTAINERS:
                meta = response_body.get(container)
                if isinstance(meta, dict):
                    for key in _CURSOR_KEYS:
                        val = meta.get(key)
                        if val:
                            return str(val)

    if strategy == "link_header":
        link = response_headers.get("link") or response_headers.get("Link", "")
        return _extract_link_next(link)

    if strategy == "keyset" and records:
        # Use last record's keyset field
        last = records[-1]
        if pagination.keyset_param:
            return str(last.get(pagination.keyset_param.replace("since_", "").replace("after_", "id"), ""))

    return None


def has_more_pages(
    pagination: PaginationConfig,
    response_body: Any,
    response_headers: dict[str, str],
    records: list[dict],
    next_cursor: Optional[str],
) -> bool:
    """Determine if there are more pages to fetch."""
    if pagination.strategy == "none":
        return False

    # If we got fewer records than page_size, we're done
    if len(records) < pagination.page_size and pagination.page_size > 1:
        return False

    # Empty page = done
    if not records:
        return False

    if pagination.strategy in ("cursor", "keyset", "link_header"):
        return bool(next_cursor)

    if pagination.strategy in ("page", "offset"):
        # Check has_more in body
        if isinstance(response_body, dict):
            for key in ("has_more", "has_next_page", "has_next"):
                val = response_body.get(key)
                if val is False:
                    return False
            # Check nested
            for container in _PAGE_META_CONTAINERS:
                meta = response_body.get(container)
                if isinstance(meta, dict):
                    for key in ("has_more", "has_next_page", "has_next"):
                        val = meta.get(key)
                        if val is False:
                            return False
        return True

    return False
