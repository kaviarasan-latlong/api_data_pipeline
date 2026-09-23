# latlong_pipeline

Rolling 3-day-window pipeline that pulls request/response API logs, extracts
lat/long, geo-enriches missing state/district/pincode via PostGIS, writes
the result to Postgres, and reports pincode/district/state hit counts to a
workbook + Teams after every run.

## Files

| File | Role |
|---|---|
| `config.yaml` | **Single place to configure everything** - DB, window size, chunk size, table/column names, and the `included_api_types` filter list. |
| `config_loader.py` | Loads `config.yaml`, resolves `${ENV_VAR}` placeholders. |
| `watermark.py` | Creates/reads/updates `pipeline_watermark` (window_start/window_end/last_processed_id/status). Handles resuming a crashed `RUNNING` window. |
| `extractor.py` | Chunked pull from `request_logs` (filtered by window + `api_type`), batch-fetches matching `response_logs`, joins in memory by `uniqueid`. |
| `parser.py` (Stage 1) | Extracts lat/long from the request path *and* response JSON generically (by key-name pattern, not per-api hardcoding). Applies the error/no-latlong drop rules below. |
| `geo_enrichment.py` (Stage 2) | For rows missing state/district/pincode, does a batched `ST_Intersects` point-in-polygon lookup. |
| `writer.py` (Stage 3) | Bulk-loads rows via `COPY` into a temp staging table, then upserts into the output table (safe to re-run a chunk). |
| `metrics.py` | After a window succeeds: queries pincode/district/state counts and updates a cumulative `.xlsx` report (3 sheets). |
| `notifier.py` | Renders `notifier_template.json` with the run summary and POSTs to the Teams webhook. |
| `main.py` | Orchestrates all of the above. |
| `run_pipeline.sh` | Activates the venv, runs `main.py`. Called by Airflow. |
| `dags/api_dag.py` | Airflow DAG - schedules `run_pipeline.sh`. All real logic stays in `main.py` so a manual `python main.py` run behaves identically. |

## Adding / removing an API type

Edit `config.yaml -> pipeline.included_api_types`. Nothing else needs to change -
the extractor filters `request_logs.api_type = ANY(included_api_types)` directly
from that list, and the parser doesn't care which api_type a row came from since
it looks for lat/long generically by key name.

## Keep / drop rules (Stage 1) - as interpreted from the spec

The spec's flowchart and its inline note read as slightly contradictory
("error -> drop" vs. "even if failure, take lat/long from the request path"),
so here's the interpretation this code implements - flag it if it's not what
you meant:

1. Always try to pull lat/long from the **request path** regardless of
   response status (some APIs put lat/long in the request even when the
   call fails).
2. If the response is an error/failure, we do **not** trust the response
   body for lat/long - only the request path can save the row.
   - Found in request path -> **keep**.
   - Not found anywhere -> **drop**, counted as `dropped_error`.
3. If the response is **not** an error, the row is kept if lat/long is
   found in request path and/or response.
   - Not found anywhere -> **drop**, counted as `dropped_no_latlong`.
   - Found in both -> **response wins on conflict**.

## Report format note

The spec asked for "a csv with 3 sheets" (pincode / district / state counts,
updated after every run). A CSV can't hold multiple sheets, so `metrics.py`
writes a `.xlsx` workbook instead - same "3 tabs, running counts, updated
every run" behavior, just in a format that can actually do it.

## One-time setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

export DB_HOST=... DB_NAME=... DB_USER=... DB_PASSWORD=...
export TEAMS_WEBHOOK_URL=...
```

Replace `notifier_template.json` with your real Teams template - just keep
the `{{pipeline_name}}`, `{{window_start}}`, `{{window_end}}`, `{{kept}}`,
`{{dropped_error}}`, `{{dropped_no_latlong}}`, `{{report_path}}` tokens (or
add more keys in `metrics.build_run_summary` if your template needs more).

Deploy the pipeline directory to wherever `dags/api_dag.py`'s `PIPELINE_DIR`
points, and set the `latlong_db_host` / `latlong_db_name` / `latlong_db_user`
/ `latlong_db_password` / `latlong_teams_webhook_url` Airflow Variables.

## Manual run / backfill

```bash
cd latlong_pipeline
python main.py
```

Safe to re-run: `watermark.py` resumes a `RUNNING` window from its
`last_processed_id`, and `writer.py`'s upsert means reprocessing a chunk
doesn't double-count rows in the output table (the xlsx report, being
purely additive, is the one place a re-run of a *successful* window would
double-count - it's designed to be updated once per window on success).
