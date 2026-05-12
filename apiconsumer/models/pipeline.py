from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel, Field, PrivateAttr
import uuid


class AuthConfig(BaseModel):
    auth_type: Literal["none", "api_key", "bearer", "basic", "oauth2_client_credentials"] = "none"

    # api_key
    api_key: str | None = None
    api_key_header: str = "X-API-Key"
    api_key_in: Literal["header", "query"] = "header"

    # bearer
    bearer_token: str | None = None

    # basic
    username: str | None = None
    password: str | None = None

    # oauth2
    client_id: str | None = None
    client_secret: str | None = None
    token_url: str | None = None
    scopes: list[str] = []

    # runtime-only cached token — never serialised
    _cached_token: str | None = PrivateAttr(default=None)
    _token_expires_at: float | None = PrivateAttr(default=None)


class PaginationConfig(BaseModel):
    strategy: Literal["none", "offset", "page", "cursor", "keyset", "link_header"] = "none"

    # offset / page
    page_param: str | None = None
    per_page_param: str | None = None
    page_size: int = 100
    offset_param: str | None = None

    # cursor
    cursor_param: str | None = None
    cursor_response_path: str | None = None  # e.g. "$.meta.next_cursor"

    # keyset
    keyset_param: str | None = None
    keyset_direction: Literal["asc", "desc"] = "asc"

    # link header
    link_rel: str = "next"


class TimestampConfig(BaseModel):
    param_name: str = "start_date"
    param_format: Literal["iso8601", "unix_seconds", "unix_millis", "YYYY-MM-DD"] = "iso8601"
    param_location: Literal["query", "body", "path"] = "query"
    end_param_name: str | None = None
    response_timestamp_field: str = "$.created_at"
    window_size_hours: int = 24


class EndpointSpec(BaseModel):
    api_type: Literal["rest", "soap"] = "rest"
    base_url: str
    path: str = ""
    method: str = "GET"
    default_headers: dict[str, str] = {}
    default_query_params: dict[str, str] = {}
    # JSONPath to the array of records in the response, e.g. "$.data" or "$"
    data_path: str = "$"

    # SOAP-specific
    soap_action: str | None = None
    soap_body_template: str | None = None

    @property
    def full_url(self) -> str:
        return self.base_url.rstrip("/") + ("/" + self.path.lstrip("/") if self.path else "")


class PipelineConfig(BaseModel):
    pipeline_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

    endpoint: EndpointSpec
    auth: AuthConfig = Field(default_factory=AuthConfig)
    pagination: PaginationConfig = Field(default_factory=PaginationConfig)
    timestamp: TimestampConfig = Field(default_factory=TimestampConfig)

    output_format: Literal["csv", "parquet", "both"] = "csv"
    output_dir: str = "data/output"
    fields_to_extract: list[str] = []
    extra_static_params: dict[str, str] = {}

    # how often (in windows) the co-pilot agent reviews progress
    copilot_review_every: int = 5
