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


def _table_columns(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        )
        return {row[0] for row in cursor.fetchall()}


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


def _resolve_hierarchy(chain_rows):
    pincode = None
    district = None
    state = None

    for row in chain_rows:
        display_name = str(row[0]).strip() if row[0] else ""
        area_order = row[1]
        pin_match = re.match(r"^\s*(\d{6})", display_name)

        if pincode is None and (area_order == 55 or pin_match):
            if pin_match:
                pincode = pin_match.group(1)
            if pincode is None:
                continue
            continue
        if pincode is None:
            continue
        if area_order == 8 and district is None:
            district = re.sub(r"^\d{6}\s*[-\u2013]?\s*", "", display_name).strip()
            continue
        if area_order == 9 and state is None:
            state = re.sub(r"^\d{6}\s*[-\u2013]?\s*", "", display_name).strip()
            break

        if district is None and area_order is None:
            district = re.sub(r"^\d{6}\s*[-\u2013]?\s*", "", display_name).strip()
            continue
        if district and state is None and area_order is None:
            state = re.sub(r"^\d{6}\s*[-\u2013]?\s*", "", display_name).strip()
            break

    return state or None, district or None, pincode


def _submission_query(geom_columns, area_columns, source_config):
    geom_filters = []
    if "to_date" in geom_columns:
        geom_filters.append("g.to_date IS NULL")
    if "aa_order" in geom_columns:
        geom_filters.append("g.aa_order = 55")
    geom_filter_sql = " AND ".join(geom_filters) if geom_filters else "TRUE"

    area_order_select = "a.aa_order" if "aa_order" in area_columns else "NULL::integer"
    area_to_date_filter = "AND a.to_date IS NULL" if "to_date" in area_columns else ""
    submissions_table = f'"{source_config["submissions_table"]}"'
    survey_table = f'"{source_config["survey_table"]}"'
    submission_columns = {
        key: f'"{value}"' for key, value in source_config["submission_columns"].items()
    }
    survey_columns = {
        key: f'"{value}"' for key, value in source_config["survey_columns"].items()
    }

    return f"""
        WITH RECURSIVE extracted AS (
            SELECT
                row_number() OVER () AS row_id,
                s.{submission_columns['server_created_at']} AS server_created_at,
                sv.{survey_columns['name']} AS name,
                sv.{survey_columns['bunit_id']} AS bunit_id,
                coordinate_match.parts[1]::double precision AS latitude,
                coordinate_match.parts[2]::double precision AS longitude
                        FROM {submissions_table} s
                        JOIN {survey_table} sv
                            ON sv.{survey_columns['id']} = s.{submission_columns['survey_id']}
            CROSS JOIN LATERAL (
                                SELECT regexp_match(s.{submission_columns['content']}::text, %s) AS parts
            ) coordinate_match
                        WHERE s.{submission_columns['server_created_at']} >= %s
                            AND s.{submission_columns['server_created_at']} < %s
              AND coordinate_match.parts IS NOT NULL
        ),
        valid_points AS (
            SELECT * FROM extracted
            WHERE latitude BETWEEN -90 AND 90
              AND longitude BETWEEN -180 AND 180
        ),
        matched AS (
            SELECT p.*, geo.aa_id
            FROM valid_points p
            LEFT JOIN LATERAL (
                SELECT g.aa_id
                FROM aa_geom g
                WHERE ST_Intersects(
                    g.geom,
                    ST_SetSRID(ST_MakePoint(p.longitude, p.latitude), 4326)
                )
                  AND {geom_filter_sql}
                LIMIT 1
            ) geo ON TRUE
        ),
        chain AS (
            SELECT
                m.row_id, m.server_created_at, m.name, m.bunit_id, m.latitude, m.longitude,
                a.id AS current_id,
                a.display_name,
                a.aa_in_aa_id AS parent_id,
                {area_order_select} AS aa_order,
                1 AS depth
            FROM matched m
            LEFT JOIN admin_area a ON a.id = m.aa_id
            {area_to_date_filter}

            UNION ALL

            SELECT
                c.row_id, c.server_created_at, c.name, c.bunit_id, c.latitude, c.longitude,
                a.id AS current_id,
                a.display_name,
                a.aa_in_aa_id AS parent_id,
                {area_order_select} AS aa_order,
                c.depth + 1 AS depth
            FROM chain c
            JOIN admin_area a ON a.id = c.parent_id
            WHERE c.depth < 10
              AND c.parent_id IS NOT NULL
              {area_to_date_filter}
        )
        SELECT row_id, server_created_at, name, bunit_id, latitude, longitude,
               display_name, aa_order, depth
        FROM chain
        ORDER BY row_id, depth
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
    geom_columns = _table_columns(conn, "aa_geom")
    area_columns = _table_columns(conn, "admin_area")
    if not {"aa_id", "geom"}.issubset(geom_columns):
        raise RuntimeError("aa_geom must contain aa_id and geom columns")
    if not {"id", "aa_in_aa_id", "display_name"}.issubset(area_columns):
        raise RuntimeError(
            "admin_area must contain id, aa_in_aa_id, and display_name columns"
        )

    query = _submission_query(geom_columns, area_columns, source_config)
    cursor = conn.cursor(name="submissions_geo_export_cursor")
    cursor.itersize = INSERT_BATCH_SIZE
    cursor.execute(query, (COORDINATE_PATTERN, start, end))

    inserted_rows = 0
    pending = []
    current_id = None
    current_fields = None
    hierarchy_rows = []

    def flush_current():
        nonlocal inserted_rows
        if current_fields is None:
            return
        state, district, pincode = _resolve_hierarchy(hierarchy_rows)
        pending.append((*current_fields[:4], state, district, pincode, current_fields[4]))
        if len(pending) >= INSERT_BATCH_SIZE:
            _write_batch(conn, source_config["output_table"], pending)
            inserted_rows += len(pending)
            pending.clear()

    try:
        while True:
            batch = cursor.fetchmany(INSERT_BATCH_SIZE)
            if not batch:
                break
            for row in batch:
                row_id, server_created_at, name, bunit_id, latitude, longitude, display_name, aa_order, _depth = row
                if row_id != current_id:
                    flush_current()
                    current_id = row_id
                    current_fields = (
                        name,
                        str(bunit_id) if bunit_id is not None else None,
                        latitude,
                        longitude,
                        server_created_at,
                    )
                    hierarchy_rows = []
                if display_name is not None:
                    hierarchy_rows.append((display_name, aa_order))
        flush_current()
        if pending:
            _write_batch(conn, source_config["output_table"], pending)
            inserted_rows += len(pending)
    finally:
        cursor.close()

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
        duration_label = "Full API + Anuga duration" if pipeline_started_at else "Anuga export duration"
        api_table = source_config["config"]["tables"]["output_table"]
        details = (
            f"Status: SUCCESS | API output table: {api_table} | "
            f"Anuga output table: {source_config['output_table']} | "
            f"Window: {window_start.isoformat()} to {window_end.isoformat()} (end exclusive) | "
            f"Anuga rows written or already present: {row_count} | "
            f"{duration_label}: {_format_duration(elapsed)}"
        )
        try:
            _send_teams_notification(webhook_url, "API and Anuga pipeline completed", details)
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
            f"Status: FAILED | API output table: {api_table} | Anuga output table: {anuga_table} | "
            f"Failed stage: {stage} | Window: {window_start} to {window_end} (end exclusive) | "
            f"Full duration: {_format_duration(elapsed)} | Issue: {exc}"
        )
        if webhook_url:
            try:
                _send_teams_notification(webhook_url, "API and Anuga pipeline failed", details, failed=True)
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