from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click
from dateutil import parser as dateutil_parser
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from apiconsumer.ingestion.log_parser import parse_log_file
from apiconsumer.ingestion.rest_parser import parse_rest_input
from apiconsumer.ingestion.wsdl_parser import build_soap_endpoint_spec, parse_wsdl
from apiconsumer.models.pipeline import PipelineConfig
from apiconsumer.output.parquet_writer import convert_csvs_to_parquet, get_parquet_preview
from apiconsumer.state import checkpoint as ckpt_store

console = Console()


def _parse_ts(value: str) -> datetime:
    if value.lower() in ("now", "today"):
        return datetime.utcnow().replace(tzinfo=None)
    try:
        return dateutil_parser.parse(value).replace(tzinfo=None)
    except Exception:
        raise click.BadParameter(f"Cannot parse '{value}' as a date/time. Try '2024-01-01' or '2024-01-01T12:00:00'.")


@click.group()
def cli():
    """APIConsumer — AI-powered API data pipeline agent."""
    pass


@cli.command()
@click.option("--endpoint", "endpoint_url", default=None, help="REST endpoint URL")
@click.option("--sample", "sample_file", default=None, type=click.Path(), help="JSON file with sample request")
@click.option("--log", "log_file", default=None, type=click.Path(), help="HTTP log file (Apache/NGINX/HAR)")
@click.option("--wsdl", "wsdl_source", default=None, help="WSDL file path or URL")
@click.option("--from", "from_ts", default=None, help="Extraction start (ISO8601 or 'YYYY-MM-DD')")
@click.option("--to", "to_ts", default="now", show_default=True, help="Extraction end (default: now)")
@click.option("--format", "output_format", type=click.Choice(["csv", "parquet", "both"]), default="csv", show_default=True)
@click.option("--output-dir", default="data/output", show_default=True)
@click.option("--window-hours", default=24, show_default=True, type=int, help="Hours per extraction window")
@click.option("--name", default=None, help="Pipeline name")
@click.option("--copilot-every", default=5, show_default=True, type=int, help="Co-pilot reviews every N windows")
def run(
    endpoint_url, sample_file, log_file, wsdl_source,
    from_ts, to_ts, output_format, output_dir, window_hours, name, copilot_every,
):
    """Run a full pipeline: learn -> extract -> output."""
    from apiconsumer.agent.loop import run_learning_phase
    from apiconsumer.pipeline.extractor import run_extraction
    from apiconsumer.state.checkpoint import save_pipeline_config

    # ── Step 1: Ingest ────────────────────────────────────────────────────────
    console.print(Panel("[bold]APIConsumer[/bold]", subtitle="starting", border_style="bright_blue"))

    if not any([endpoint_url, log_file, wsdl_source]):
        raise click.UsageError("Provide one of: --endpoint, --log, or --wsdl")

    endpoint_spec = None

    if log_file:
        console.print(f"[dim]Parsing log file: {log_file}[/dim]")
        entries = parse_log_file(log_file)
        if not entries:
            raise click.ClickException("No HTTP requests found in log file")
        console.print(f"\nFound {len(entries)} unique endpoint(s):\n")
        for i, e in enumerate(entries[:20], 1):
            console.print(f"  {i:2}. [{e['method']:6}] {e['url']}  ({e['count']} hits)")
        idx = click.prompt("\nSelect endpoint number", default=1, type=int) - 1
        chosen = entries[max(0, min(idx, len(entries) - 1))]
        endpoint_url = chosen["url"]
        console.print(f"[green]Selected: {chosen['method']} {endpoint_url}[/green]\n")

    if wsdl_source:
        console.print(f"[dim]Parsing WSDL: {wsdl_source}[/dim]")
        operations = parse_wsdl(wsdl_source)
        if not operations:
            raise click.ClickException("No operations found in WSDL")
        console.print(f"\nFound {len(operations)} operation(s):\n")
        for i, op in enumerate(operations, 1):
            console.print(f"  {i:2}. {op['name']}  [dim](action: {op.get('soap_action', '')})[/dim]")
        idx = click.prompt("\nSelect operation number", default=1, type=int) - 1
        chosen_op = operations[max(0, min(idx, len(operations) - 1))]
        endpoint_spec = build_soap_endpoint_spec(chosen_op)
        console.print(f"[green]Selected: {chosen_op['name']}[/green]\n")

    if endpoint_url and endpoint_spec is None:
        endpoint_spec = parse_rest_input(endpoint_url, sample_file)

    if endpoint_spec is None:
        raise click.ClickException("Could not determine endpoint spec")

    # ── Step 2: Learning phase ────────────────────────────────────────────────
    pipeline_name = name or (endpoint_url or wsdl_source or "pipeline").split("/")[-1].split("?")[0]
    spec_dict = json.loads(endpoint_spec.model_dump_json())

    config = run_learning_phase(
        endpoint_spec_dict=spec_dict,
        sample_provided=bool(sample_file),
    )

    if config is None:
        raise click.ClickException("Learning phase did not produce a pipeline config. Please try again.")

    # Override CLI-level settings
    config.output_format = output_format
    config.output_dir = output_dir
    config.timestamp.window_size_hours = window_hours
    config.copilot_review_every = copilot_every
    if name:
        config.name = name

    save_pipeline_config(config)

    # ── Step 3: Date range ────────────────────────────────────────────────────
    if from_ts is None:
        from_ts = click.prompt("Extraction start date/time (e.g. 2024-01-01)", default="")
        if not from_ts:
            raise click.ClickException("Start date is required")

    extraction_start = _parse_ts(from_ts)
    extraction_end = _parse_ts(to_ts)

    if extraction_end <= extraction_start:
        raise click.ClickException("--to must be after --from")

    delta = extraction_end - extraction_start
    n_windows = max(1, int(delta.total_seconds() / 3600 / window_hours))

    console.print()
    console.print(Panel(
        f"[bold]Pipeline:[/bold] {config.name}\n"
        f"[bold]ID:[/bold] {config.pipeline_id}\n"
        f"[bold]Range:[/bold] {extraction_start.date()} → {extraction_end.date()}\n"
        f"[bold]Windows:[/bold] {n_windows} × {window_hours}h\n"
        f"[bold]Format:[/bold] {output_format}",
        title="Extraction Plan",
        border_style="green",
    ))

    if not click.confirm("Start extraction?", default=True):
        console.print("[yellow]Cancelled.[/yellow]")
        return

    # ── Step 4: Extract ───────────────────────────────────────────────────────
    result = run_extraction(config, extraction_start, extraction_end)

    # ── Step 5: Summary ───────────────────────────────────────────────────────
    console.print()
    console.print(Panel(
        f"[green bold]✓ Extraction complete[/green bold]\n\n"
        f"Rows:     {result['total_rows']:,}\n"
        f"Windows:  {result['windows_completed']}/{result['total_windows']}\n"
        f"Output:   {output_dir}/\n"
        f"Pipeline: {result['pipeline_id']}",
        border_style="green",
    ))


@cli.command()
@click.option("--pipeline-id", default=None, help="Pipeline ID to resume (pick from list if omitted)")
def resume(pipeline_id):
    """Resume an interrupted extraction."""
    from apiconsumer.pipeline.extractor import run_extraction

    if pipeline_id is None:
        pipelines = ckpt_store.list_all_pipelines()
        incomplete = [p for p in pipelines if p.get("completed_windows", 0) < p.get("total_windows", 1)]
        if not incomplete:
            console.print("[yellow]No incomplete pipelines found.[/yellow]")
            return
        console.print("\nIncomplete pipelines:\n")
        for i, p in enumerate(incomplete, 1):
            pct = int(100 * p["completed_windows"] / max(p["total_windows"], 1))
            console.print(f"  {i}. {p['name']} ({pct}% — {p['completed_windows']}/{p['total_windows']} windows)")
        idx = click.prompt("Select pipeline number", default=1, type=int) - 1
        pipeline_id = incomplete[max(0, min(idx, len(incomplete) - 1))]["pipeline_id"]

    config = ckpt_store.load_pipeline_config(pipeline_id)
    if not config:
        raise click.ClickException(f"Pipeline {pipeline_id} not found")

    state = ckpt_store.load_checkpoint_state(pipeline_id)
    if not state:
        raise click.ClickException("No checkpoint state found — cannot resume")

    console.print(f"[cyan]Resuming pipeline: {config.name} ({pipeline_id})[/cyan]")
    result = run_extraction(config, state.extraction_start, state.extraction_end, resume=True)

    console.print(Panel(
        f"[green bold]✓ Resumed and completed[/green bold]\n"
        f"Total rows: {result['total_rows']:,}",
        border_style="green",
    ))


@cli.command()
@click.option("--pipeline-id", default=None)
@click.option("--all", "show_all", is_flag=True, default=False)
def status(pipeline_id, show_all):
    """Show pipeline status."""
    if pipeline_id:
        pipelines_data = [p for p in ckpt_store.list_all_pipelines() if p["pipeline_id"] == pipeline_id]
    elif show_all:
        pipelines_data = ckpt_store.list_all_pipelines()
    else:
        pipelines_data = ckpt_store.list_all_pipelines()[:1]

    if not pipelines_data:
        console.print("[yellow]No pipelines found.[/yellow]")
        return

    table = Table(title="Pipelines", border_style="blue")
    table.add_column("Name", style="bold")
    table.add_column("ID", style="dim")
    table.add_column("Progress")
    table.add_column("Rows", justify="right")
    table.add_column("Created")

    for p in pipelines_data:
        total = p.get("total_windows", 0)
        done = p.get("completed_windows", 0)
        pct = f"{100*done//max(total,1)}%" if total else "—"
        progress_str = f"{done}/{total} ({pct})" if total else "—"
        table.add_row(
            p.get("name", ""),
            p["pipeline_id"][:8] + "…",
            progress_str,
            f"{p.get('total_rows', 0):,}",
            p.get("created_at", "")[:10],
        )

    console.print(table)


@cli.command("list")
def list_pipelines():
    """List all configured pipelines."""
    ctx = click.get_current_context()
    ctx.invoke(status, pipeline_id=None, show_all=True)


@cli.command()
@click.option("--pipeline-id", required=True)
@click.option("--output-dir", default=None)
def convert(pipeline_id, output_dir):
    """Convert pipeline CSV output to Parquet."""
    config = ckpt_store.load_pipeline_config(pipeline_id)
    if not config:
        raise click.ClickException(f"Pipeline {pipeline_id} not found")

    out_dir = Path(output_dir or config.output_dir)
    csv_files = sorted(out_dir.glob(f"{pipeline_id}_*.csv"))

    if not csv_files:
        raise click.ClickException(f"No CSV files found for pipeline {pipeline_id} in {out_dir}")

    console.print(f"[dim]Converting {len(csv_files)} CSV file(s)…[/dim]")
    parquet_path = convert_csvs_to_parquet(csv_files, out_dir / f"{pipeline_id}.parquet")
    preview = get_parquet_preview(parquet_path)

    console.print(Panel(
        f"[green]✓ Parquet written:[/green] {parquet_path}\n"
        f"Rows: {preview.get('num_rows', '?'):,}  Columns: {preview.get('num_columns', '?')}  "
        f"Size: {preview.get('file_size_bytes', 0) / 1024:.1f} KB",
        border_style="green",
    ))


@cli.command()
@click.option("--pipeline-id", required=True)
def inspect(pipeline_id):
    """Re-run the learning phase against an existing pipeline to detect schema changes."""
    from apiconsumer.agent.loop import run_learning_phase

    config = ckpt_store.load_pipeline_config(pipeline_id)
    if not config:
        raise click.ClickException(f"Pipeline {pipeline_id} not found")

    console.print(f"[cyan]Re-running learning phase for: {config.name}[/cyan]")
    spec_dict = json.loads(config.endpoint.model_dump_json())
    new_config = run_learning_phase(spec_dict, sample_provided=False)

    if new_config:
        ckpt_store.save_pipeline_config(new_config)
        console.print(f"[green]✓ Updated config saved for pipeline: {new_config.pipeline_id}[/green]")
    else:
        console.print("[yellow]Learning phase did not produce an updated config.[/yellow]")
