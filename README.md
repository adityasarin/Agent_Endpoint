# APIConsumer

An AI-powered data pipeline agent. Point it at a REST endpoint, WSDL file, or HTTP log file and it autonomously learns the API structure, configures an extraction pipeline, and batch-extracts data into CSV or Parquet files — with full checkpoint/resume support.

The agent brain is [Gemini 2.5 Flash](https://deepmind.google/technologies/gemini/flash/) via the Google Gen AI SDK. The actual HTTP work, checkpointing, and file I/O are deterministic Python.

---

## Requirements

- Python 3.10+
- A [Gemini API key](https://aistudio.google.com/app/apikey)

---

## Installation

```bash
git clone https://github.com/adityasarin/Agent_Endpoint.git
cd Agent_Endpoint
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your key:

```bash
cp .env.example .env
# then edit .env:
# Gemini_API_Key=AIzaSy...
```

Or export it directly:

```bash
export Gemini_API_Key=AIzaSy...   # Linux/macOS
$env:Gemini_API_Key = "AIzaSy..."  # PowerShell
```

---

## CLI Usage

### Start a new pipeline

**From a REST endpoint:**

```bash
python main.py run --endpoint https://api.example.com/events --from 2024-01-01 --format csv
```

**With a sample request JSON** (helps the agent understand auth and body structure):

```bash
python main.py run --endpoint https://api.example.com/events --sample sample_request.json --from 2024-01-01
```

`sample_request.json` format:
```json
{
  "method": "GET",
  "url": "https://api.example.com/events",
  "headers": { "Authorization": "Bearer <token>" },
  "query_params": { "limit": 100 }
}
```

**From an HTTP log file** (Apache, NGINX, or HAR):

```bash
python main.py run --log access.log --from 2024-01-01 --format parquet
```

The agent presents a numbered list of unique endpoints found in the log. Pick one and it proceeds as normal.

**From a WSDL (SOAP):**

```bash
python main.py run --wsdl https://example.com/service.wsdl --from 2024-01-01
```

### All `run` options

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint URL` | — | REST endpoint URL |
| `--sample FILE` | — | JSON file with sample request |
| `--log FILE` | — | HTTP log file (Apache/NGINX/HAR) |
| `--wsdl FILE\|URL` | — | WSDL file path or URL |
| `--from DATE` | *(prompted)* | Extraction start (ISO8601 or `YYYY-MM-DD`) |
| `--to DATE` | `now` | Extraction end |
| `--format` | `csv` | `csv`, `parquet`, or `both` |
| `--output-dir DIR` | `data/output` | Output directory |
| `--window-hours N` | `24` | Hours per extraction window |
| `--name NAME` | *(auto)* | Pipeline name |
| `--copilot-every N` | `5` | Gemini reviews every N windows |

---

### Resume an interrupted pipeline

```bash
python main.py resume
# picks from a list of incomplete pipelines

python main.py resume --pipeline-id <UUID>
```

Resumption is crash-safe: the agent reads the last saved cursor/page from the checkpoint database and opens the output CSV in append mode.

---

### Check pipeline status

```bash
python main.py status --all          # all pipelines
python main.py status --pipeline-id <UUID>
python main.py list                  # alias for status --all
```

---

### Convert CSV output to Parquet

```bash
python main.py convert --pipeline-id <UUID>
```

Merges all per-window CSV files for the pipeline into a single `.parquet` file in the same output directory.

---

### Re-run the learning phase (schema inspection)

```bash
python main.py inspect --pipeline-id <UUID>
```

Sends the agent back through the learning phase against the existing endpoint. Useful for detecting schema drift after an API update.

---

## How it works

### Learning phase

When you run a new pipeline, the agent iteratively:

1. **Probes** the endpoint (HEAD/GET to check reachability, auth challenges, rate-limit headers)
2. **Sends** the sample request and inspects the real response
3. **Infers** the response schema — field names, types, timestamp candidates
4. **Detects** pagination strategy (Link header → cursor → page metadata → offset → keyset)
5. **Asks you** for anything it can't infer: auth credentials, extraction date range, output format
6. **Saves** the finalised pipeline config to an encrypted SQLite checkpoint

### Extraction phase

Extraction runs sequentially, one time-window at a time:

- Each window is marked `IN_PROGRESS` before fetching, `COMPLETE` after
- Cursor/page progress is checkpointed after every batch so a Ctrl+C is safe to resume from
- Every N windows (default 5) the Gemini co-pilot reviews a batch summary and can raise anomalies or pause to ask you a question

### Output

| File | Description |
|------|-------------|
| `data/output/<pipeline_id>_<window_unix>.csv` | One file per extraction window |
| `data/output/<pipeline_id>.parquet` | Merged Parquet (after `convert`) |
| `data/checkpoints/<pipeline_id>.db` | SQLite checkpoint (Fernet-encrypted) |

---

## Security notes

- **Credentials are encrypted at rest.** Pipeline configs (including auth tokens) are stored in SQLite using Fernet encryption with a PBKDF2 key derived from your `Gemini_API_Key` + pipeline ID. The key is never stored — it is re-derived at runtime.
- **`Gemini_API_Key` must be set.** The app will raise an error on startup if the variable is missing (there is no insecure fallback).
- **The `.env` file is gitignored.** Never commit it.

---

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `Gemini_API_Key` | Yes | Google Gemini API key |
| `GEMINI_MODEL` | No | Override model (default: `gemini-2.5-flash`) |

---

## Project layout

```
apiconsumer/
├── agent/          # Gemini tool-use loop (learning + co-pilot)
├── cli/            # Click CLI commands
├── http/           # httpx client, auth handlers, rate limiter
├── ingestion/      # REST/log/WSDL input parsers
├── models/         # Pydantic data models
├── output/         # CSV and Parquet writers
├── pipeline/       # Paginator, extractor, transformer
└── state/          # SQLite checkpoint store
```
