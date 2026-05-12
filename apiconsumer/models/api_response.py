from __future__ import annotations

from typing import Any, Literal, Optional
from pydantic import BaseModel


class FieldSchema(BaseModel):
    name: str
    json_path: str
    inferred_type: Literal["string", "integer", "float", "boolean", "datetime", "object", "array", "null"]
    nullable: bool = True
    sample_values: list[Any] = []


class PaginationHint(BaseModel):
    detected_strategy: str
    confidence: float  # 0.0–1.0
    evidence: list[str]
    next_page_example: Optional[dict] = None


class ProbeResult(BaseModel):
    url: str
    status_code: int
    response_time_ms: float
    headers: dict[str, str]
    body_preview: str
    content_type: str
    requires_auth: bool
    rate_limit_headers: dict[str, str]
    pagination_hint: Optional[PaginationHint] = None
    field_schemas: list[FieldSchema] = []


class FetchResult(BaseModel):
    records: list[dict]
    next_cursor: Optional[str] = None
    next_page: Optional[int] = None
    has_more: bool = False
    rate_limit_remaining: Optional[int] = None
    rate_limit_reset: Optional[int] = None
    response_time_ms: float = 0.0
    raw_response_preview: str = ""
