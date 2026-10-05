# API Data Pipeline

This project processes API request logs for a 3-day rolling window, extracts usable lat/long values, enriches them with admin-area metadata using PostGIS, writes the results to a final output table, and sends a Teams notification at the end of the run.

The pipeline is designed to work with server-side PostgreSQL tables such as `logs_aug_2026`, `response_logs_aug_2026`, `session_activities_aug_26`, `aa_geom`, and `admin_area`, and it is built to be resumable and safe to rerun.

---

## 1. Purpose

The pipeline is meant to do the following:

- read API request rows for a configured date window
- filter only the required API types
- fetch the corresponding response/session data
- extract valid latitude and longitude values from request path / response JSON / session metadata
- resolve pincode, district, and state from the spatial admin hierarchy
- write the final enriched records into the output table
- update the watermark so the job can resume safely
- send a per-run summary to Teams
- produce the cumulative reference workbook report

---

## 2. Business logic summary

The pipeline follows the server-side requirements used by this project:

- For most APIs, read the request rows from the request log table and pair them with the response log table.
- For the 3 special brand-store APIs, read directly from `session_activities_aug_26` instead of the normal logs.
- Extract the first valid coordinate pair from the query string when multiple values are present.
- Prefer `origins` first for distance-matrix style URLs.
- If a row contains a valid point, resolve it against the admin hierarchy using the geometry tables.
- Only assign district/state when there is a valid pincode-bearing chain.
- Do not invent fake state or district values when the spatial lookup does not produce a valid match.
- Store the final output in the configured output table, with the final enriched values.

---

## 3. Project structure

```text
data_pipeline/
├── api_dag.py
├── config.yaml
├── config_loader.py
├── extractor.py
├── geo_enrichment.py
├── main.py
├── metrics.py
├── notifier.py
├── notifier_template.json
├── parser.py
├── README.md
├── requirements.txt
├── run_pipeline.sh
├── watermark.py
├── writer.py
└── ...
```

### Core files

- `main.py`: orchestration layer
- `extractor.py`: fetches rows by window and API type
- `parser.py`: extracts lat/long and applies drop rules
- `geo_enrichment.py`: resolves pincode, district, and state using geometry and admin tables
- `writer.py`: writes final rows to the output table
- `watermark.py`: manages resume state for windows
- `config.yaml`: all environment-specific settings
- `metrics.py`: builds the report workbook and summary
- `notifier.py`: posts pipeline status to Teams

---

## 4. Configuration

All project-specific runtime settings are in `config.yaml`.

Key sections include:

- `database`: PostgreSQL connection details
- `pipeline`: API list, window settings, chunk size, worker count, start date
- `tables`: source and output table names
- `geo_enrichment`: geometry and admin column mappings
- `output`: report directory and workbook naming
- `notifier`: Teams webhook configuration

### Example config shape

```yaml
database:
  host: "localhost"
  port: 5432
  dbname: "admin_area"
  user: "www-data"
  password: "******"

pipeline:
  name: "data_pipeline"
  window_days: 3
  start_date: "2026-08-01T00:00:00+05:30"
  chunk_size: 75000
  parallel_workers: 4

  included_api_types:
   - /v4/isochrone.json
    - /v4/geofence/contains.json
    - /v4/geocode.json
    - /v4/snap.json
    - /v4/distancematrix.json
    - /v4/pincode.json
    - /v4/reverse_geocode.json
    - /v4/landmarks.json
    - /v4/directions.json
    - /v4/distance.json
    - /v4/geovalidation.json
    - /v4/search.json
    - /v4/trips.json
    - /v4/reverse_geocode_international.json
    - /v4/geo_cipher.json
    - /v4/draw_line.json
    - /v5/search.json
    - /v5/digipin_encode.json
    - /v5/digipin_decode.json
    - /v4/point_of_interest.json

  session_only_api_types:
    - /v4.1/brands/:brand_id/stores_around.json
    - /v4.1/brands/:brand_id/find.json
    - /v4.1/brands/:brand_id/search_with_disable.json

tables:
  request_logs: "logs_aug_2026"
  response_logs: "response_logs_aug_2026"
  session_activities: "session_activities_aug_26"
  watermark_table: "pipeline_watermark"
  output_table: "admin_area_enriched_final"
  geom_table: "aa_geom"
  area_table: "admin_area"
```

---

## 5. Data flow

### Stage 1: Extract

`extractor.py` pulls rows in chunks based on:

- window start and end
- API type filter
- last processed row ID from watermark state

It fetches:

- request rows from the request log table
- matching response rows from the response log table
- direct session rows for the special 3 APIs

### Stage 2: Parse

`parser.py` performs this logic:

- extract lat/long from request path, response JSON, and session payloads
- prefer the first valid coordinate pair in the expected precedence order
- classify rows into kept / dropped_error / dropped_no_latlong
- keep rows that have a usable lat/long point

### Stage 3: Geo enrichment

`geo_enrichment.py` resolves the point against `aa_geom` and walks the admin hierarchy in `admin_area`.

It uses the rules:

- valid pincode-bearing chain required
- district/state only assigned when supported by the geometry lookup
- using the aa_in_aa_id column

### Stage 4: Write

`writer.py` writes the final rows into the configured output table using staged bulk inserts for safe reprocessing.

### Stage 5: Watermark and reporting

`watermark.py` stores the processing state so the script can resume from the last successful row ID. `metrics.py` updates the cumulative workbook report and `notifier.py` sends status to Teams.

---

## 6. Watermark behavior

The pipeline keeps a watermark table named in config, usually `pipeline_watermark`.

This table stores the current run state, including:

- pipeline name
- window start date
- window end date
- last processed row ID
- status

This allows the pipeline to resume if a run is interrupted and prevents reprocessing the entire window from the beginning.

---

## 7. Run flow

### Manual execution

```bash
cd "$(git rev-parse --show-toplevel)"
python3 main.py
```

### Shell wrapper

```bash
bash run_pipeline.sh
```

---

## 8. Airflow usage

This project includes `api_dag.py` for Airflow scheduling.

The DAG invokes the project shell wrapper so the real logic remains in Python and Airflow is just the scheduler/trigger layer.

The expected pattern is:

- DAG file lives in Airflow DAG directory
- pipeline project lives in a separate folder accessible by Airflow
- Airflow triggers the shell wrapper or Python entry point

---

## 9. Teams notifications

The pipeline sends Teams notifications using the configured webhook.

The notifier sends a compact adaptive card with:

- pipeline name
- window range
- output table name
- success or failure status
- report path (successful runs)
- failure description (failure paths)

The webhook URL must be valid and reachable from the server where the process is running.

---

## 10. Operational notes

### Safe reruns

The project is designed for safe reruns:

- watermark resumes from the previous row ID
- staging writes avoid duplicate insertion in the output table
- failed runs mark the window as failed

### Performance tuning

The project includes a bounded parallel enrichment mode, but the main write path remains serialized to protect database consistency.

Recommended starting point:

- `parallel_workers: 4`
- `chunk_size: 75000` or lower if DB load is heavy

### Common issues

- missing or invalid webhook URL
- incorrect table/column names in database
- missing geometry/admin data for a point
- empty or malformed lat/long extracted from request path
- idle DB sessions after a forced stop

---

## 11. Output table

The final result is written to the configured output table, which is typically:

```text
admin_area_enriched_final
```

This table stores results such as:

- `bunit_id`
- `tenant_id`
- `api_type`
- `api_version`
- `latitude`
- `longitude`
- `state`
- `district`
- `pincode`
- `address`

---

## 12. Standalone submissions export

`submissions_geo_export.py` is a separate command-line job. It is not called by
`main.py`, `run_pipeline.sh`, or the Airflow DAG. Run it manually when the
submissions export is needed, for example after the API pipeline has completed.

The source tables are `submissions` and `surveys`. The job reads
`submissions.content`, joins `submissions.survey_id` to `surveys.id`, and gets
the name and `bunit_id` from `surveys`. It extracts the
first decimal latitude/longitude pair from the content, filters submissions by
`created_at`, and uses `aa_geom` and `admin_area` to look up state, district,
and pincode. Rows without a valid coordinate pair are skipped.

The source table names are configured under `tables` in `config.yaml`; their
required fields are mapped under `pipeline.columns.submissions` and
`pipeline.columns.surveys`.

The job creates and writes to `anuga_final` with these columns:

- `name`
- `bunit_id`
- `latitude`
- `longitude`
- `state`
- `district`
- `pincode`

Set the following environment variables before running it:

- `PGDATABASE`, `PGUSER`, and `PGPASSWORD` (required)
- `PGHOST` and `PGPORT` (optional; default to `localhost` and `5432`)
- `PGCONNECT_TIMEOUT` (optional; defaults to `10` seconds)

Set `notifier.teams_webhook_url` in `config.yaml` to the Teams webhook URL.
Both this standalone export and the API pipeline read the webhook from that
setting; `run_pipeline.sh` does not override it.

Provide the start and end of the `created_at` window as ISO-8601 timestamps.
The start is inclusive and the end is exclusive:

```bash
python3 submissions_geo_export.py \
  --start 2026-10-01T00:00:00+05:30 \
  --end 2026-10-02T00:00:00+05:30
```

The job sends a distinct Teams notification for success or failure. Database
errors roll back the current run and return a non-zero exit status. This export
does not change the API pipeline's configuration, tables, or notifications.

---

## 13. Quick start checklist

1. Validate PostgreSQL connection settings in `config.yaml`
2. Confirm table names and column mappings
3. Confirm the API allowlist contains the required endpoints
4. Confirm the Teams webhook URL is valid
5. Run the script manually once
6. Validate inserted rows in the output table
7. Move the DAG to the Airflow DAG folder
8. Trigger the DAG from Airflow or schedule it

---

## 14. Summary

This pipeline is a resumable, chunk-based PostgreSQL ETL job that transforms API lat/long records into a clean admin-area-enriched dataset for reporting and downstream use. It is built to be practical for real server environments: robust to reruns, safe with DB writes, easy to configure, and integrated with Teams notifications for operational visibility.
