# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

An AI-powered data pipeline agent. Given a REST endpoint, WSDL file, or HTTP log file, it learns the API structure autonomously (via a Gemini tool-use loop), then batch-extracts data from a given timestamp into CSV/Parquet files with full checkpoint/resume support. There is both a CLI (`main.py`) and a Streamlit UI (`ui/app.py`) — both are thin layers over the shared `apiconsumer/` library.

## Commands

```bash
# Install dependencies
pip install google-genai rich zeep cryptography streamlit

# Run CLI
python main.py run --endpoint <URL> --sample <JSON_FILE> --from 2024-01-01 --format csv
python main.py resume                          # pick from incomplete pipelines
python main.py status --all
python main.py convert --pipeline-id <UUID>   # CSV → Parquet
python main.py list
python main.py inspect --pipeline-id <UUID>   # re-run learning phase

# Run Streamlit UI
streamlit run ui/app.py

# Run tests (once added)
pytest tests/
pytest tests/test_paginator.py -v             # single test file
```

Set `Gemini_API_Key` in environment before running. All other credentials are collected interactively or via the UI.

## Architecture

### The Agent Loop (`apiconsumer/agent/loop.py`)

Central nervous system. Runs a Claude tool-use loop in two distinct phases:

**Learning phase** — Gemini probes the endpoint and asks the user clarifying questions. Ends when the model calls `finalize_pipeline_config`, which persists the config to SQLite. Tool calls: `probe_endpoint -> send_sample_request -> extract_response_schema -> detect_pagination -> ask_user -> test_auth -> finalize_pipeline_config`.

**Extraction phase (co-pilot)** — Python drives the actual HTTP loop deterministically; Gemini reviews a `review_batch_summary` every N batches and can raise anomalies or pause with `ask_user`. This catches schema drift and auth expiry early.

The loop pattern: send `types.Content` messages → check response parts for `function_call` → dispatch to `TOOL_REGISTRY[name](**args)` → append `Part.from_function_response` result → repeat. All tool implementations catch exceptions internally and return `{"success": False, "error": "..."}` — never raise into the loop. Uses `google-genai` SDK (`from google import genai`), model `gemini-2.5-flash`.

### Streamlit State Machine (`ui/pages/1_new_pipeline.py`)

Streamlit reruns the script on every interaction. The agent loop is long-running. Bridge: a state machine in `st.session_state` with states `IDLE → LEARNING → WAITING_FOR_USER → LEARNING → CONFIGURING → EXTRACTING → DONE`.

When Claude calls `ask_user`, the tool stores questions in `session_state` and returns `{"__waiting": True}`. Streamlit renders input widgets. On "Continue", answers are stored in `session_state` and the loop resumes with conversation history intact.

### Checkpoint Store (`apiconsumer/state/checkpoint.py`)

SQLite with WAL mode. One `.db` file per pipeline at `data/checkpoints/<pipeline_id>.db`. Two tables: `pipeline_config` (JSON blob with Fernet-encrypted credentials) and `extraction_windows` (per-window progress rows). Every checkpoint write — cursor + window status + rows_fetched — is a single atomic transaction. Never split across transactions.

Credential encryption uses Fernet with a PBKDF2 key derived from `Gemini_API_Key` + `pipeline_id`. The key is never stored; it is re-derived at runtime.

### Extraction Loop (`apiconsumer/pipeline/extractor.py`)

Sequential windows (one at a time). Uses a single `httpx.AsyncClient` for the entire run (connection reuse). Per window: mark `IN_PROGRESS` → paginate until `has_more=False` → write batches → mark `COMPLETE`. On resume, reads `last_cursor`/`last_page` from checkpoint and opens CSV in append mode.

A 1-minute overlap is added to window boundaries to handle API clock skew. Post-processing deduplication runs via `response_timestamp_field` + record hash.

### Pagination Detection (`apiconsumer/pipeline/paginator.py`)

Detection priority (highest confidence first):
1. `Link` header with `rel="next"` — RFC 5988
2. Cursor key in response body (`cursor`, `next_token`, `continuation_token`, `after`, etc.)
3. Pagination metadata object (`meta`, `pagination`, `_links` with `current_page`/`total_pages`)
4. Offset/limit params in the original request URL
5. Keyset params (`since_id`, `max_id`, `after_id`)

If nothing is detected, the agent calls `ask_user`.

### Auth (`apiconsumer/http/auth.py`)

Supports: `none`, `api_key` (header or query), `bearer`, `basic`, `oauth2_client_credentials`. OAuth2 handler checks token expiry before every request with a 60-second buffer and refreshes in-memory. Only `client_id`/`client_secret` are persisted (encrypted); bearer tokens are never written to disk.

### Input Parsers (`apiconsumer/ingestion/`)

- `rest_parser.py` — reads a JSON file describing the sample request (`method`, `url`, `headers`, `query_params`, `body`) and builds an `EndpointSpec` draft
- `log_parser.py` — handles Apache Combined Log Format, NGINX, JSON structured logs, and HAR files; deduplicates by URL pattern (ignoring path ID segments); presents numbered list for user to select
- `wsdl_parser.py` — uses `xml.etree.ElementTree` to extract operations; builds a `soap_body_template` XML string with `{param}` placeholders

## Key Data Models (`apiconsumer/models/pipeline.py`)

`PipelineConfig` is the shared contract between all modules:
- `endpoint: EndpointSpec` — URL, method, headers, `data_path` (JSONPath to the records array, e.g. `$.data`)
- `auth: AuthConfig` — credentials are `PrivateAttr` fields, never serialised directly
- `pagination: PaginationConfig` — strategy + all parameter names
- `timestamp: TimestampConfig` — `param_name`, `param_format` (`iso8601`/`unix_seconds`/`unix_millis`/`YYYY-MM-DD`), `window_size_hours`

`ExtractionWindow` tracks per-window state: `start_ts`, `end_ts`, `status` (PENDING/IN_PROGRESS/COMPLETE/FAILED), `last_cursor`, `last_page`, `rows_fetched`, `output_file`.

## Important Constraints

- All file paths use `pathlib.Path` — Windows compatibility, no string `/` concatenation.
- CSV writer calls `f.flush()` + `os.fsync(f.fileno())` after every batch. One file per window: `<pipeline_id>_<window_start_unix>.csv`.
- The `jsonpath_get(data, path)` utility in `transformer.py` is a minimal implementation handling `$.field`, `$.nested.field`, `$.array[*].field` — do not replace with `jsonpath-ng` unless a specific case requires it.
- SOAP: always send `SOAPAction` header; handle both SOAP 1.1 (`text/xml`) and 1.2 (`application/soap+xml`).
