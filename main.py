"""
main.py

Entry point invoked by run_pipeline.sh (in turn triggered by api_dag.py).

    determine window (watermark.py)
      -> for each chunk (extractor.py):
            parse (parser.py)
            geo-enrich (geo_enrichment.py)
            write (writer.py)
            advance watermark (watermark.py)
      -> on window exhaustion: mark SUCCESS, build report (metrics.py),
         notify (notifier.py)
      -> on any exception: mark FAILED, re-raise (Airflow surfaces the failure)
"""

import atexit
import logging
import math
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import psycopg2

import config_loader
import extractor
import geo_enrichment
import metrics
import notifier
import parser
import watermark
import writer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


def _write_error_ids_file(path: str, ids: list, mode: str = "w"):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, mode, encoding="utf-8") as fh:
        for value in ids:
            if value is not None:
                fh.write(f"{value}\n")


def _cleanup_idle_postgres_sessions(conn):
    if conn is None:
        return
    try:
        try:
            conn.rollback()
        except Exception:
            pass

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND state IN ('idle', 'idle in transaction')
                """
            )
            rows = cur.fetchall()
            if rows:
                logger.info("Terminated idle PostgreSQL sessions: %s", rows)
    except Exception:
        logger.exception("Failed to terminate idle PostgreSQL sessions.")


def _enrich_rows_in_worker(cfg, rows):
    conn = psycopg2.connect(config_loader.get_db_dsn(cfg))
    try:
        return geo_enrichment.enrich_rows(conn, cfg, rows)
    finally:
        conn.close()


def _parallel_enrich_rows(cfg, rows, max_workers):
    if not rows:
        return []
    if max_workers <= 1:
        conn = psycopg2.connect(config_loader.get_db_dsn(cfg))
        try:
            return geo_enrichment.enrich_rows(conn, cfg, rows)
        finally:
            conn.close()

    target_batches = min(max_workers, len(rows))
    batch_size = max(1, math.ceil(len(rows) / target_batches))
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_enrich_rows_in_worker, cfg, batch) for batch in batches]
        ordered = [None] * len(batches)
        for idx, future in enumerate(futures):
            ordered[idx] = future.result()

    merged = []
    for batch_rows in ordered:
        merged.extend(batch_rows)
    return merged


def run():
    cfg = config_loader.load_config()
    pipeline_cfg = cfg["pipeline"]
    watermark_table = cfg["tables"]["watermark_table"]
    output_table = cfg["tables"]["output_table"]
    pipeline_name = pipeline_cfg["name"]
    error_dir = cfg["output"]["report_dir"]

    conn = psycopg2.connect(config_loader.get_db_dsn(cfg))
    atexit.register(_cleanup_idle_postgres_sessions, conn)

    try:
        watermark.ensure_watermark_table(conn, watermark_table)
        writer.ensure_output_table(conn, output_table)

        window_start, window_end, last_processed_id, is_resume = watermark.get_or_create_window(
            conn,
            watermark_table,
            pipeline_name,
            pipeline_cfg["window_days"],
            request_table=cfg["tables"]["request_logs"],
            created_at_col="created_at",
            start_date=pipeline_cfg.get("start_date"),
        )
        logger.info(
            "%s window %s -> %s (resume=%s, last_processed_id=%s)",
            pipeline_name, window_start, window_end, is_resume, last_processed_id,
        )

        totals = {"kept": 0, "dropped_error": 0, "dropped_no_latlong": 0}
        all_error_ids = []
        all_no_latlong_ids = []

        for joined_rows, new_last_processed_id in extractor.iterate_chunks(
            conn, cfg, window_start, window_end, last_processed_id,
        ):
            kept_rows, parse_counts, dropped_error_ids, dropped_no_latlong_ids = parser.parse_rows(joined_rows)
            for k in totals:
                totals[k] += parse_counts.get(k, 0)
            all_error_ids.extend(dropped_error_ids)
            all_no_latlong_ids.extend(dropped_no_latlong_ids)

            if dropped_error_ids:
                _write_error_ids_file(
                    os.path.join(error_dir, "response_error_ids.txt"),
                    dropped_error_ids,
                    mode="a",
                )
            if dropped_no_latlong_ids:
                _write_error_ids_file(
                    os.path.join(error_dir, "no_latlong_error_ids.txt"),
                    dropped_no_latlong_ids,
                    mode="a",
                )

            workers = max(1, int(pipeline_cfg.get("parallel_workers", 1)))
            if len(kept_rows) > 5000 and workers > 1:
                enriched_rows = _parallel_enrich_rows(cfg, kept_rows, workers)
            else:
                enriched_rows = geo_enrichment.enrich_rows(conn, cfg, kept_rows)

            writer.write_rows(conn, cfg, enriched_rows)

            watermark.update_last_processed_id(
                conn, watermark_table, pipeline_name, window_start, new_last_processed_id,
            )

        if all_error_ids:
            _write_error_ids_file(os.path.join(error_dir, "response_error_ids.txt"), all_error_ids)
        if all_no_latlong_ids:
            _write_error_ids_file(os.path.join(error_dir, "no_latlong_error_ids.txt"), all_no_latlong_ids)

        watermark.mark_window_status(conn, watermark_table, pipeline_name, window_start, "SUCCESS")
        logger.info("Window complete. Totals: %s", totals)

        window_counts = metrics.compute_window_counts(conn, cfg, window_start, window_end)
        report_path = metrics.update_report_workbook(cfg, window_counts, window_end)
        run_summary = metrics.build_run_summary(pipeline_name, window_start, window_end, totals, report_path)
        run_summary["output_table"] = output_table
        notifier.send_notification(cfg, run_summary)

    except Exception as exc:
        logger.exception("Pipeline run failed - marking window FAILED for retry/resume.")
        try:
            if "window_start" in locals():
                watermark.mark_window_status(conn, watermark_table, pipeline_name, window_start, "FAILED")
        except Exception:
            pass

        failure_summary = {
            "pipeline_name": pipeline_name,
            "window_start": str(locals().get("window_start", "n/a")),
            "window_end": str(locals().get("window_end", "n/a")),
            "kept": 0,
            "dropped_error": 0,
            "dropped_no_latlong": 0,
            "report_path": "n/a",
            "output_table": output_table,
            "failure_message": str(exc),
        }
        try:
            notifier.send_notification(cfg, failure_summary)
        except Exception:
            logger.exception("Failure notification could not be sent.")
        raise
    finally:
        _cleanup_idle_postgres_sessions(conn)
        conn.close()


if __name__ == "__main__":
    try:
        run()
    except Exception:
        sys.exit(1)
