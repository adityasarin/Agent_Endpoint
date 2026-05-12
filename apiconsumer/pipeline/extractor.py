from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import httpx
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn

from apiconsumer.http.client import AsyncHTTPClient
from apiconsumer.http.rate_limiter import RateLimiter
from apiconsumer.models.api_response import FetchResult
from apiconsumer.models.checkpoint import CheckpointState, ExtractionWindow, WindowStatus
from apiconsumer.models.pipeline import PipelineConfig
from apiconsumer.output.csv_writer import CSVWriter, build_window_csv_path
from apiconsumer.output.parquet_writer import convert_csvs_to_parquet
from apiconsumer.pipeline.paginator import (
    build_next_page_params,
    extract_next_cursor,
    has_more_pages,
)
from apiconsumer.pipeline.transformer import extract_records, flatten_record, format_timestamp
from apiconsumer.state import checkpoint as ckpt_store

console = Console()


def build_windows(
    config: PipelineConfig,
    extraction_start: datetime,
    extraction_end: datetime,
) -> list[ExtractionWindow]:
    windows: list[ExtractionWindow] = []
    current = extraction_start
    window_delta = timedelta(hours=config.timestamp.window_size_hours)

    while current < extraction_end:
        end = min(current + window_delta, extraction_end)
        windows.append(
            ExtractionWindow(
                pipeline_id=config.pipeline_id,
                start_ts=current,
                end_ts=end,
            )
        )
        current = end

    return windows


def initialise_checkpoint(
    config: PipelineConfig,
    extraction_start: datetime,
    extraction_end: datetime,
) -> CheckpointState:
    windows = build_windows(config, extraction_start, extraction_end)
    state = CheckpointState(
        pipeline_id=config.pipeline_id,
        extraction_start=extraction_start,
        extraction_end=extraction_end,
        total_windows=len(windows),
        windows=windows,
    )
    ckpt_store.save_checkpoint_state(state)
    for w in windows:
        ckpt_store.upsert_window(w)
    return state


def _build_request_params(
    config: PipelineConfig,
    window: ExtractionWindow,
    page_params: dict,
) -> tuple[str, dict, dict, Optional[bytes]]:
    """Returns (url, headers, params, body_bytes)"""
    ts_cfg = config.timestamp
    ep = config.endpoint

    # Start with static defaults
    params = dict(ep.default_query_params)
    params.update(config.extra_static_params)
    params.update(page_params)

    headers = dict(ep.default_headers)
    body_bytes = None

    # Inject timestamp
    start_val = format_timestamp(window.start_ts, ts_cfg.param_format)
    end_val = format_timestamp(window.end_ts, ts_cfg.param_format) if ts_cfg.end_param_name else None

    if ts_cfg.param_location == "query":
        params[ts_cfg.param_name] = str(start_val)
        if ts_cfg.end_param_name and end_val is not None:
            params[ts_cfg.end_param_name] = str(end_val)
    elif ts_cfg.param_location == "body":
        body_dict: dict = {ts_cfg.param_name: start_val}
        if ts_cfg.end_param_name and end_val is not None:
            body_dict[ts_cfg.end_param_name] = end_val
        body_bytes = json.dumps(body_dict).encode()
        headers["Content-Type"] = "application/json"

    # Add pagination params on top
    pagination = config.pagination
    if pagination.strategy in ("page", "offset") and pagination.per_page_param:
        params.setdefault(pagination.per_page_param, str(pagination.page_size))

    url = ep.full_url
    return url, headers, params, body_bytes


async def _fetch_page(
    http: AsyncHTTPClient,
    config: PipelineConfig,
    window: ExtractionWindow,
    page_params: dict,
) -> FetchResult:
    url, headers, params, body_bytes = _build_request_params(config, window, page_params)
    ep = config.endpoint

    kwargs: dict = {"headers": headers, "params": params}
    if body_bytes:
        kwargs["data"] = body_bytes

    t0 = time.monotonic()
    resp = await http.request(ep.method, url, **kwargs)
    elapsed_ms = (time.monotonic() - t0) * 1000

    try:
        body = resp.json()
    except Exception:
        body = {}

    records = extract_records(body, config.endpoint.data_path)
    if config.fields_to_extract:
        records = [flatten_record(r, config.fields_to_extract) for r in records]
    else:
        records = [flatten_record(r) for r in records]

    resp_headers = dict(resp.headers)
    next_cursor = extract_next_cursor(config.pagination, body, resp_headers, records)
    more = has_more_pages(config.pagination, body, resp_headers, records, next_cursor)

    rl_remaining = None
    rl_reset = None
    for key in ("x-ratelimit-remaining", "x-rate-limit-remaining"):
        val = resp_headers.get(key)
        if val:
            try:
                rl_remaining = int(val)
            except ValueError:
                pass
    for key in ("x-ratelimit-reset", "x-rate-limit-reset"):
        val = resp_headers.get(key)
        if val:
            try:
                rl_reset = int(val)
            except ValueError:
                pass

    return FetchResult(
        records=records,
        next_cursor=next_cursor,
        has_more=more,
        rate_limit_remaining=rl_remaining,
        rate_limit_reset=rl_reset,
        response_time_ms=elapsed_ms,
        raw_response_preview=str(body)[:200],
    )


async def _extract_window(
    http: AsyncHTTPClient,
    config: PipelineConfig,
    window: ExtractionWindow,
    progress_task,
    progress: Progress,
) -> int:
    output_path = build_window_csv_path(
        config.output_dir, config.pipeline_id, int(window.start_ts.timestamp())
    )

    # Resume: check if partially done
    append_mode = window.status == WindowStatus.IN_PROGRESS and output_path.exists()

    writer = CSVWriter(output_path)
    writer.open(append=append_mode)

    window.status = WindowStatus.IN_PROGRESS
    window.started_at = window.started_at or datetime.utcnow()
    window.output_file = str(output_path)
    ckpt_store.upsert_window(window)

    # Restore pagination state for resume
    page_params: dict = {}
    page_number = window.last_page
    cursor = window.last_cursor

    if cursor and config.pagination.cursor_param:
        page_params[config.pagination.cursor_param] = cursor
    elif page_number > 0:
        if config.pagination.page_param:
            page_params[config.pagination.page_param] = str(page_number)
        if config.pagination.offset_param:
            page_params[config.pagination.offset_param] = str(page_number * config.pagination.page_size)

    total_rows = window.rows_fetched

    try:
        while True:
            result = await _fetch_page(http, config, window, page_params)

            if result.records:
                written = writer.write_batch(result.records)
                total_rows += written

            # Checkpoint after every page
            window.rows_fetched = total_rows
            window.last_cursor = result.next_cursor
            window.last_page = page_number
            ckpt_store.upsert_window(window)

            progress.update(progress_task, description=f"[cyan]{window.start_ts.date()} — {total_rows:,} rows")

            if not result.has_more:
                break

            # Advance pagination
            next_params = build_next_page_params(
                config.pagination, page_params, result, page_number
            )
            if next_params is None:
                break

            if config.pagination.strategy == "link_header" and result.next_cursor:
                # next_cursor IS the full URL — replace base url logic in fetch
                page_params = {"__next_url__": result.next_cursor}
            else:
                page_params = next_params
            page_number += 1

    finally:
        writer.close()

    window.status = WindowStatus.COMPLETE
    window.completed_at = datetime.utcnow()
    window.rows_fetched = total_rows
    ckpt_store.upsert_window(window)

    return total_rows


def run_extraction(
    config: PipelineConfig,
    extraction_start: datetime,
    extraction_end: datetime,
    resume: bool = False,
) -> dict:
    """
    Main extraction entry point. Runs synchronously (wraps async internals).
    Returns summary dict.
    """
    return asyncio.run(_run_extraction_async(config, extraction_start, extraction_end, resume))


async def _run_extraction_async(
    config: PipelineConfig,
    extraction_start: datetime,
    extraction_end: datetime,
    resume: bool,
) -> dict:
    from apiconsumer.agent.loop import run_copilot_review

    # Load or create checkpoint state
    state = ckpt_store.load_checkpoint_state(config.pipeline_id) if resume else None

    if state is None:
        state = initialise_checkpoint(config, extraction_start, extraction_end)
        ckpt_store.save_pipeline_config(config)
    else:
        # Reload window objects from DB for accurate status
        db_windows = ckpt_store.load_windows(config.pipeline_id)
        state.windows = db_windows

    pending_windows = [w for w in state.windows if w.status != WindowStatus.COMPLETE]
    total = len(state.windows)
    done = state.completed_windows

    Path(config.output_dir).mkdir(parents=True, exist_ok=True)

    rate_limiter = RateLimiter()
    total_rows = state.total_rows_fetched
    windows_since_review = 0
    all_output_files: list[str] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        task = progress.add_task("[cyan]Extracting…", total=total, completed=done)

        async with AsyncHTTPClient(auth=config.auth, rate_limiter=rate_limiter) as http:
            for window in pending_windows:
                rows = await _extract_window(http, config, window, task, progress)
                total_rows += rows
                done += 1
                windows_since_review += 1

                if window.output_file:
                    all_output_files.append(window.output_file)

                # Update global state
                state.completed_windows = done
                state.total_rows_fetched = total_rows
                ckpt_store.save_checkpoint_state(state)
                progress.update(task, completed=done)

                # Co-pilot review
                if windows_since_review >= config.copilot_review_every:
                    windows_since_review = 0
                    summary = {
                        "windows_completed": done,
                        "total_windows": total,
                        "rows_in_last_batch": rows,
                        "total_rows_so_far": total_rows,
                        "last_window_start": window.start_ts.isoformat(),
                        "last_window_end": window.end_ts.isoformat(),
                        "last_window_status": window.status.value,
                        "output_file": window.output_file,
                    }
                    review = run_copilot_review(config.pipeline_id, summary)
                    if not review.get("approved") and not review.get("user_input_needed"):
                        console.print("[yellow]Co-pilot flagged an issue — stopping extraction.[/yellow]")
                        break

    # Post-process: convert to Parquet if needed
    parquet_path = None
    if config.output_format in ("parquet", "both") and all_output_files:
        parquet_out = Path(config.output_dir) / f"{config.pipeline_id}.parquet"
        try:
            parquet_path = convert_csvs_to_parquet(all_output_files, parquet_out)
            console.print(f"[green]✓ Parquet written: {parquet_path}[/green]")
        except Exception as e:
            console.print(f"[red]Parquet conversion failed: {e}[/red]")

    if config.output_format == "parquet" and parquet_path:
        # Remove CSVs if user only wants Parquet
        for f in all_output_files:
            try:
                Path(f).unlink()
            except Exception:
                pass

    return {
        "pipeline_id": config.pipeline_id,
        "total_rows": total_rows,
        "windows_completed": done,
        "total_windows": total,
        "output_files": all_output_files,
        "parquet_path": parquet_path,
    }
