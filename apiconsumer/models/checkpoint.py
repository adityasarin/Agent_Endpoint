from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
import uuid


class WindowStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    FAILED = "failed"


class ExtractionWindow(BaseModel):
    window_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: str
    start_ts: datetime
    end_ts: datetime
    status: WindowStatus = WindowStatus.PENDING
    rows_fetched: int = 0
    last_cursor: Optional[str] = None
    last_page: int = 0
    output_file: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None


class CheckpointState(BaseModel):
    pipeline_id: str
    extraction_start: datetime
    extraction_end: datetime
    total_windows: int
    completed_windows: int = 0
    total_rows_fetched: int = 0
    current_window_id: Optional[str] = None
    windows: list[ExtractionWindow] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
