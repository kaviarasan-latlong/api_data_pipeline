#!/usr/bin/env python3
"""Export submission coordinates and their admin areas.

The Airflow DAG runs this after the API pipeline using its latest successful
watermark window. For a manual backfill, provide an ISO-8601 start and end
window. The end timestamp is exclusive and filters
submissions.server_created_at.
"""

import argparse
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import requests
import yaml
from psycopg2.extras import execute_values

import geo_enrichment
import metrics


LOGGER = logging.getLogger("submissions_geo_export")
COORDINATE_PATTERN = r"(-?\d{1,2}\.\d+),\s*(-?\d{1,3}\.\d+)"
INSERT_BATCH_SIZE = 5000
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _parse_timestamp(value):
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid ISO-8601 timestamp: {value}"
        ) from exc


def _format_duration(seconds):
    total_seconds = max(0, int(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _database_connection(database_config):
    dbname = database_config["dbname"]
    environment_dbname = os.environ.get("PGDATABASE")
    if environment_dbname and environment_dbname != dbname:
        raise RuntimeError(
            f"PGDATABASE ({environment_dbname}) must match config.yaml database.dbname ({dbname})"
        )

    user = os.environ.get("PGUSER") or database_config.get("user")
    password = os.environ.get("PGPASSWORD") or database_config.get("password")
    connection_options = {
        "host": database_config["host"],
        "port": database_config["port"],
        "dbname": dbname,
        "connect_timeout": int(database_config.get("connect_timeout", 10)),
    }
    if user:
        connection_options["user"] = user
    if password:
        connection_options["password"] = password
    return psycopg2.connect(**connection_options)


def _load_source_config():
    config_path = Path(__file__).with_name("config.yaml")
    with config_path.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    tables = config["tables"]
    columns = config["pipeline"]["columns"]
    source_config = {
        "database": config["database"],
        "config": config,
        "pipeline_name": config["pipeline"]["name"],
        "submissions_table": tables["submissions"],
        "survey_table": tables["surveys"],
        "output_table": tables["anuga_output_table"],
        "watermark_table": tables["watermark_table"],
        "submission_columns": columns["submissions"],
        "survey_columns": columns["surveys"],
        "teams_webhook_url": config["notifier"]["teams_webhook_url"],
    }
    identifiers = [source_config["submissions_table"], source_config["survey_table"]]
    identifiers.extend(source_config["submission_columns"].values())
    identifiers.extend(source_config["survey_columns"].values())
    invalid = [
        value for value in identifiers
        if not isinstance(value, str) or not IDENTIFIER_PATTERN.fullmatch(value)
    ]
    if invalid:
        raise ValueError(f"Invalid SQL identifiers in config.yaml: {invalid}")
    return source_config


def _latest_successful_pipeline_window(conn, source_config):
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT window_start, window_end
            FROM {source_config['watermark_table']}
            WHERE pipeline_name = %s AND status = 'SUCCESS'
            ORDER BY window_end DESC
            LIMIT 1
            """,
            (source_config["pipeline_name"],),
        )
        row = cursor.fetchone()
    if row is None:
        raise RuntimeError("No successful API pipeline window is available for Anuga export")
    return row


def _ensure_output_table(conn, output_table):
    quoted_table = f'"{output_table}"'
    with conn.cursor() as cursor:
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {quoted_table} (
                name text,
                bunit_id text,
                latitude double precision,
                longitude double precision,
                state text,
                district text,
                pincode text,
                server_created_at timestamptz
            )
            """
        )
        cursor.execute(
            f"ALTER TABLE {quoted_table} "
            "ADD COLUMN IF NOT EXISTS server_created_at timestamptz"
        )
        cursor.execute(f"DROP INDEX IF EXISTS {output_table}_row_uidx")
        cursor.execute(
            f"""CREATE UNIQUE INDEX IF NOT EXISTS {output_table}_row_uidx
            ON {quoted_table}
                (name, bunit_id, latitude, longitude, state, district, pincode,
                 server_created_at)
            """
        )


def _submission_query(source_config):
    submissions_table = f'"{source_config["submissions_table"]}"'
    survey_table = f'"{source_config["survey_table"]}"'
    submission_columns = {
        key: f'"{value}"' for key, value in source_config["submission_columns"].items()
    }
    survey_columns = {
        key: f'"{value}"' for key, value in source_config["survey_columns"].items()
    }

    return f"""
        SELECT
            row_number() OVER () AS row_id,
            s.{submission_columns['server_created_at']} AS server_created_at,
            s.{submission_columns['survey_id']} AS survey_id,
            sv.{survey_columns['id']} IS NOT NULL AS survey_matched,
            sv.{survey_columns['name']} AS name,
            sv.{survey_columns['bunit_id']} AS bunit_id,
            coordinate_match.parts[1]::double precision AS latitude,
            coordinate_match.parts[2]::double precision AS longitude
        FROM {submissions_table} s
        LEFT JOIN {survey_table} sv
          ON sv.{survey_columns['id']} = s.{submission_columns['survey_id']}
        CROSS JOIN LATERAL (
            SELECT regexp_match(s.{submission_columns['content']}::text, %s) AS parts
        ) coordinate_match
        WHERE s.{submission_columns['server_created_at']} >= %s
          AND s.{submission_columns['server_created_at']} < %s
          AND coordinate_match.parts IS NOT NULL
          AND coordinate_match.parts[1]::double precision BETWEEN -90 AND 90
          AND coordinate_match.parts[2]::double precision BETWEEN -180 AND 180
          AND coordinate_match.parts[1]::double precision <> 0
          AND coordinate_match.parts[2]::double precision <> 0
    """


def _write_batch(conn, output_table, rows):
    if not rows:
        return
    with conn.cursor() as cursor:
        execute_values(
            cursor,
            f"""INSERT INTO "{output_table}"
                (name, bunit_id, latitude, longitude, state, district, pincode,
                 server_created_at)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
            page_size=INSERT_BATCH_SIZE,
        )


def _process_window(conn, start, end, source_config):
    query = _submission_query(source_config)
    cursor = conn.cursor(name="submissions_geo_export_cursor")
    cursor.itersize = INSERT_BATCH_SIZE
    cursor.execute(query, (COORDINATE_PATTERN, start, end))

    inserted_rows = 0
    unmatched_survey_count = 0
    unmatched_survey_ids = []

    try:
        while True:
            batch = cursor.fetchmany(INSERT_BATCH_SIZE)
            if not batch:
                break
            source_rows = []
            for row in batch:
                _, server_created_at, survey_id, survey_matched, name, bunit_id, latitude, longitude = row
                if not survey_matched:
                    unmatched_survey_count += 1
                    if len(unmatched_survey_ids) < 10:
                        unmatched_survey_ids.append(survey_id)
                source_rows.append({
                    "server_created_at": server_created_at,
                    "name": name,
                    "bunit_id": str(bunit_id) if bunit_id is not None else None,
                    "lat": latitude,
                    "lng": longitude,
                    "state": None,
                    "district": None,
                    "pincode": None,
                    "address": None,
                })

            fallback_distance = float(source_config["config"].get("geo_enrichment", {}).get("fallback_distance_meters", 500))
            enriched_rows = geo_enrichment.enrich_rows(
                conn, source_config["config"], source_rows, fallback_distance=fallback_distance,
            )
            pending = [
                (
                    row.get("name"),
                    row.get("bunit_id"),
                    row.get("lat"),
                    row.get("lng"),
                    row.get("state"),
                    row.get("district"),
                    row.get("pincode"),
                    row.get("server_created_at"),
                )
                for row in enriched_rows
                if row.get("pincode") and str(row.get("pincode")).strip()
            ]
            _write_batch(conn, source_config["output_table"], pending)
            inserted_rows += len(pending)
    finally:
        cursor.close()

    if unmatched_survey_count:
        LOGGER.warning(
            "%d submissions had no matching survey row in %s; sample survey IDs: %s",
            unmatched_survey_count,
            source_config["survey_table"],
            unmatched_survey_ids,
        )

    return inserted_rows


def _send_teams_notification(webhook_url, title, details, failed=False):
    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "Container",
                            "style": "Attention" if failed else "Good",
                            "items": [
                                {"type": "TextBlock", "text": title,
                                 "weight": "Bolder", "size": "Medium", "wrap": True},
                                {"type": "TextBlock", "text": details, "wrap": True},
                            ],
                        }
                    ],
                    "msteams": {"width": "Full"},
                },
            }
        ],
    }
    try:
        response = requests.post(webhook_url, json=payload, timeout=15)
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Teams notification request failed ({type(exc).__name__})"
        ) from None
    if response.status_code >= 300:
        LOGGER.error(
            "Teams notification failed: HTTP %s %s",
            response.status_code,
            response.text,
        )
        raise RuntimeError(f"Teams notification failed with HTTP {response.status_code}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-watermark", action="store_true",
                        help="Use the latest successful API pipeline window")
    parser.add_argument("--start", type=_parse_timestamp,
                        help="Inclusive server_created_at window start (ISO-8601)")
    parser.add_argument("--end", type=_parse_timestamp,
                        help="Exclusive server_created_at window end (ISO-8601)")
    args = parser.parse_args()

    if args.from_watermark:
        if args.start is not None or args.end is not None:
            parser.error("--from-watermark cannot be combined with --start/--end")
    elif args.start is None or args.end is None:
        parser.error("provide both --start and --end, or use --from-watermark")
    elif args.start >= args.end:
        parser.error("--start must be earlier than --end")

    webhook_url = None
    conn = None
    export_started = time.monotonic()
    pipeline_started_at = os.environ.get("PIPELINE_RUN_STARTED_AT")
    stage = "configuration and database connection"
    try:
        source_config = _load_source_config()
        configured_webhook = source_config["teams_webhook_url"]
        if not configured_webhook or configured_webhook == "REPLACE_WITH_YOUR_WEBHOOK_URL":
            raise RuntimeError("Set notifier.teams_webhook_url in config.yaml")
        webhook_url = configured_webhook
        conn = _database_connection(source_config["database"])
        if args.from_watermark:
            window_start, window_end = _latest_successful_pipeline_window(
                conn, source_config,
            )
        else:
            window_start, window_end = args.start, args.end

        stage = "Anuga export"
        _ensure_output_table(conn, source_config["output_table"])
        row_count = _process_window(conn, window_start, window_end, source_config)
        conn.commit()
        stage = "monthly report refresh"
        monthly_reports = metrics.update_monthly_report_workbooks(
            conn, source_config["config"], window_start, window_end,
        )
        elapsed = (
            time.time() - float(pipeline_started_at)
            if pipeline_started_at
            else time.monotonic() - export_started
        )
        api_table = source_config["config"]["tables"]["output_table"]
        details = (
            "Status: SUCCESS\n"
            f"Start date: {window_start.isoformat()}\n"
            f"End date: {window_end.isoformat()}\n"
            f"API table: {api_table}\n"
            f"Anuga table: {source_config['output_table']}\n"
            f"Time: {_format_duration(elapsed)}"
        )
        try:
            _send_teams_notification(
                webhook_url,
                f"{source_config['pipeline_name']} completed",
                details,
            )
        except Exception as notification_error:
            LOGGER.error(
                "Anuga export and monthly reports completed, but Teams notification failed: %s",
                notification_error,
            )
        LOGGER.info(
            "Export completed; %d rows written or already present. Refreshed reports: %s",
            row_count,
            monthly_reports,
        )
        return 0
    except Exception as exc:
        if conn is not None:
            conn.rollback()
        LOGGER.exception("Submissions geo export failed")
        window_start = locals().get("window_start", args.start)
        window_end = locals().get("window_end", args.end)
        elapsed = (
            time.time() - float(pipeline_started_at)
            if pipeline_started_at
            else time.monotonic() - export_started
        )
        api_table = (
            source_config["config"]["tables"]["output_table"]
            if "source_config" in locals()
            else "admin_area_enriched_final"
        )
        anuga_table = (
            source_config["output_table"]
            if "source_config" in locals()
            else "anuga_final"
        )
        details = (
            "Status: FAILED\n"
            f"Start date: {window_start}\n"
            f"End date: {window_end}\n"
            f"API table: {api_table}\n"
            f"Anuga table: {anuga_table}\n"
            f"Failed stage: {stage}\n"
            f"Full duration: {_format_duration(elapsed)}\n"
            f"Issue: {exc}"
        )
        if webhook_url:
            try:
                pipeline_name = (
                    source_config["pipeline_name"]
                    if "source_config" in locals()
                    else "data_pipeline"
                )
                _send_teams_notification(
                    webhook_url,
                    f"{pipeline_name} failed",
                    details,
                    failed=True,
                )
            except Exception:
                LOGGER.exception("Could not send the Teams failure notification")
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    sys.exit(main())