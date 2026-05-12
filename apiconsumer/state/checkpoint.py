from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
import base64

from apiconsumer.models.checkpoint import CheckpointState, ExtractionWindow, WindowStatus
from apiconsumer.models.pipeline import PipelineConfig

_CHECKPOINTS_DIR = Path("data/checkpoints")


def _derive_key(pipeline_id: str) -> bytes:
    """Derive a Fernet key from Gemini_API_Key + pipeline_id via PBKDF2."""
    secret = (os.environ.get("Gemini_API_Key", "insecure-default") + pipeline_id).encode()
    salt = hashlib.sha256(pipeline_id.encode()).digest()[:16]
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100_000)
    return base64.urlsafe_b64encode(kdf.derive(secret))


def _encrypt(data: str, pipeline_id: str) -> str:
    return Fernet(_derive_key(pipeline_id)).encrypt(data.encode()).decode()


def _decrypt(data: str, pipeline_id: str) -> str:
    return Fernet(_derive_key(pipeline_id)).decrypt(data.encode()).decode()


def _db_path(pipeline_id: str) -> Path:
    _CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)
    return _CHECKPOINTS_DIR / f"{pipeline_id}.db"


def _connect(pipeline_id: str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(_db_path(pipeline_id)))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(pipeline_id: str) -> None:
    with _connect(pipeline_id) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS pipeline_config (
                pipeline_id TEXT PRIMARY KEY,
                config_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS extraction_state (
                pipeline_id TEXT PRIMARY KEY,
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS extraction_windows (
                window_id TEXT PRIMARY KEY,
                pipeline_id TEXT NOT NULL,
                start_ts TEXT NOT NULL,
                end_ts TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                rows_fetched INTEGER NOT NULL DEFAULT 0,
                last_cursor TEXT,
                last_page INTEGER NOT NULL DEFAULT 0,
                output_file TEXT,
                started_at TEXT,
                completed_at TEXT,
                error_message TEXT
            );
        """)


def save_pipeline_config(config: PipelineConfig) -> None:
    init_db(config.pipeline_id)
    raw = config.model_dump_json()
    encrypted = _encrypt(raw, config.pipeline_id)
    with _connect(config.pipeline_id) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO pipeline_config (pipeline_id, config_json, created_at) VALUES (?,?,?)",
            (config.pipeline_id, encrypted, datetime.utcnow().isoformat()),
        )


def load_pipeline_config(pipeline_id: str) -> Optional[PipelineConfig]:
    if not _db_path(pipeline_id).exists():
        return None
    with _connect(pipeline_id) as conn:
        row = conn.execute(
            "SELECT config_json FROM pipeline_config WHERE pipeline_id=?", (pipeline_id,)
        ).fetchone()
    if not row:
        return None
    raw = _decrypt(row["config_json"], pipeline_id)
    return PipelineConfig.model_validate_json(raw)


def save_checkpoint_state(state: CheckpointState) -> None:
    state.updated_at = datetime.utcnow()
    with _connect(state.pipeline_id) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO extraction_state (pipeline_id, state_json, updated_at) VALUES (?,?,?)",
            (state.pipeline_id, state.model_dump_json(), state.updated_at.isoformat()),
        )


def load_checkpoint_state(pipeline_id: str) -> Optional[CheckpointState]:
    if not _db_path(pipeline_id).exists():
        return None
    with _connect(pipeline_id) as conn:
        row = conn.execute(
            "SELECT state_json FROM extraction_state WHERE pipeline_id=?", (pipeline_id,)
        ).fetchone()
    if not row:
        return None
    return CheckpointState.model_validate_json(row["state_json"])


def upsert_window(window: ExtractionWindow) -> None:
    with _connect(window.pipeline_id) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO extraction_windows
               (window_id, pipeline_id, start_ts, end_ts, status, rows_fetched,
                last_cursor, last_page, output_file, started_at, completed_at, error_message)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                window.window_id,
                window.pipeline_id,
                window.start_ts.isoformat(),
                window.end_ts.isoformat(),
                window.status.value,
                window.rows_fetched,
                window.last_cursor,
                window.last_page,
                window.output_file,
                window.started_at.isoformat() if window.started_at else None,
                window.completed_at.isoformat() if window.completed_at else None,
                window.error_message,
            ),
        )


def load_windows(pipeline_id: str) -> list[ExtractionWindow]:
    if not _db_path(pipeline_id).exists():
        return []
    with _connect(pipeline_id) as conn:
        rows = conn.execute(
            "SELECT * FROM extraction_windows WHERE pipeline_id=? ORDER BY start_ts ASC",
            (pipeline_id,),
        ).fetchall()
    return [_row_to_window(r) for r in rows]


def _row_to_window(row: sqlite3.Row) -> ExtractionWindow:
    return ExtractionWindow(
        window_id=row["window_id"],
        pipeline_id=row["pipeline_id"],
        start_ts=datetime.fromisoformat(row["start_ts"]),
        end_ts=datetime.fromisoformat(row["end_ts"]),
        status=WindowStatus(row["status"]),
        rows_fetched=row["rows_fetched"],
        last_cursor=row["last_cursor"],
        last_page=row["last_page"],
        output_file=row["output_file"],
        started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
        completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
        error_message=row["error_message"],
    )


def list_all_pipelines() -> list[dict]:
    """Return summary dicts for all known pipelines."""
    _CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    for db_file in sorted(_CHECKPOINTS_DIR.glob("*.db")):
        pipeline_id = db_file.stem
        try:
            config = load_pipeline_config(pipeline_id)
            state = load_checkpoint_state(pipeline_id)
            results.append({
                "pipeline_id": pipeline_id,
                "name": config.name if config else pipeline_id,
                "created_at": config.created_at.isoformat() if config else "",
                "completed_windows": state.completed_windows if state else 0,
                "total_windows": state.total_windows if state else 0,
                "total_rows": state.total_rows_fetched if state else 0,
            })
        except Exception:
            results.append({"pipeline_id": pipeline_id, "name": pipeline_id, "error": "unreadable"})
    return results
