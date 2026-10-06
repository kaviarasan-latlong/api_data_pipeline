"""
main.py

Entry point invoked by run_pipeline.sh (in turn triggered by data_dag.py).

    determine window (watermark.py)
      -> for each chunk (extractor.py):
            parse (parser.py)         [parallel via multiprocessing]
            geo-enrich (geo_enrichment.py) [parallel via thread pool + connection pool]
            write (writer.py)
            advance watermark (watermark.py)
      -> on window exhaustion: mark SUCCESS, build report (metrics.py),
         notify (notifier.py)
      -> on any exception: mark FAILED, re-raise (Airflow surfaces the failure)

OPTIMIZATIONS (v2):
  - Connection pool (ThreadedConnectionPool) replaces per-worker connect/close
  - Multiprocessing for CPU-bound parser stage (bypasses GIL)
  - Chunk prefetching overlaps next extraction with current processing
"""

import atexit
import logging
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool as ProcessPool

import psycopg2
from psycopg2 import pool as pg_pool

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
    if conn is None or conn.closed:
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


def _enrich_rows_in_worker(pool, cfg, rows):
    """Borrow a connection from the pool, enrich, then return it."""
    conn = pool.getconn()
    try:
        return geo_enrichment.enrich_rows(conn, cfg, rows)
    finally:
        pool.putconn(conn)


def _parallel_enrich_rows(pool_or_cfg, cfg, rows, max_workers):
    """
    Parallel geo-enrichment using a shared connection pool.

    If pool_or_cfg is a ThreadedConnectionPool, workers borrow connections
    from it. Otherwise falls back to single-connection mode.
    """
    if not rows:
        return []
    if max_workers <= 1:
        conn = pool_or_cfg.getconn() if hasattr(pool_or_cfg, 'getconn') else psycopg2.connect(config_loader.get_db_dsn(cfg))
        try:
            return geo_enrichment.enrich_rows(conn, cfg, rows)
        finally:
            if hasattr(pool_or_cfg, 'putconn'):
                pool_or_cfg.putconn(conn)
            else:
                conn.close()

    target_batches = min(max_workers, len(rows))
    batch_size = max(1, math.ceil(len(rows) / target_batches))
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_enrich_rows_in_worker, pool_or_cfg, cfg, batch) for batch in batches]
        ordered = [None] * len(batches)
        for idx, future in enumerate(futures):
            ordered[idx] = future.result()

    merged = []
    for batch_rows in ordered:
        merged.extend(batch_rows)
    return merged


# ---------------------------------------------------------------------------
# Parallel parsing (multiprocessing — bypasses GIL for CPU-bound JSON/regex)
# ---------------------------------------------------------------------------

def _parallel_parse(rows, num_workers):
    """
    Split rows across multiple processes for CPU-bound parse work.
    Falls back to single-process if num_workers <= 1 or rows are small.
    """
    if num_workers <= 1 or len(rows) < 5000:
        return parser.parse_rows(rows)

    batch_size = max(1, math.ceil(len(rows) / num_workers))
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

    with ProcessPool(processes=num_workers) as proc_pool:
        results = proc_pool.map(parser.parse_rows, batches)

    # Merge results from all processes
    merged_kept = []
    merged_counts = {"dropped_error": 0, "dropped_no_latlong": 0, "kept": 0}
    merged_error_ids = []
    merged_no_latlong_ids = []

    for kept, counts, error_ids, no_latlong_ids in results:
        merged_kept.extend(kept)
        for k in merged_counts:
            merged_counts[k] += counts.get(k, 0)
        merged_error_ids.extend(error_ids)
        merged_no_latlong_ids.extend(no_latlong_ids)

    return merged_kept, merged_counts, merged_error_ids, merged_no_latlong_ids


def run():
    cfg = config_loader.load_config()
    pipeline_cfg = cfg["pipeline"]
    watermark_table = cfg["tables"]["watermark_table"]
    output_table = cfg["tables"]["output_table"]
    pipeline_name = pipeline_cfg["name"]
    error_dir = cfg["output"]["report_dir"]
    workers = max(1, int(pipeline_cfg.get("parallel_workers", 1)))

    # --- Main connection for extraction, watermark, writes (serialized) ---
    conn = psycopg2.connect(config_loader.get_db_dsn(cfg))
    atexit.register(_cleanup_idle_postgres_sessions, conn)

    # --- Connection pool for parallel geo-enrichment workers ---
    enrich_pool = None
    if workers > 1:
        enrich_pool = pg_pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=workers + 2,
            dsn=config_loader.get_db_dsn(cfg),
        )
        logger.info("Created connection pool with maxconn=%d for %d enrichment workers.", workers + 2, workers)

    pipeline_start = time.monotonic()

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

        total_request_rows = extractor.count_request_rows(conn, cfg, window_start, window_end)
        completed_request_rows = extractor.count_request_rows(
            conn, cfg, window_start, window_end, through_id=last_processed_id,
        )
        initial_progress_percent = (
            100.0 if total_request_rows == 0
            else min(100.0, completed_request_rows * 100.0 / total_request_rows)
        )
        logger.info(
            "Total request-log rows in window: %d; progress starts at %d/%d (%.1f%%).",
            total_request_rows, completed_request_rows, total_request_rows, initial_progress_percent,
        )

        totals = {"kept": 0, "dropped_error": 0, "dropped_no_latlong": 0}
        all_error_ids = []
        all_no_latlong_ids = []
        chunk_num = 0

        # --- Use a prefetch executor to overlap next-chunk extraction ---
        # with current chunk's parse -> enrich -> write pipeline.
        prefetch_executor = ThreadPoolExecutor(max_workers=1)
        chunk_iter = extractor.iterate_chunks(
            conn, cfg, window_start, window_end, last_processed_id,
        )

        for joined_rows, new_last_processed_id in chunk_iter:
            chunk_num += 1
            chunk_start = time.monotonic()
            chunk_request_rows = sum("session_id" not in row for row in joined_rows)

            # --- STAGE 1: Parse (parallel via multiprocessing) ---
            t0 = time.monotonic()
            parse_workers = min(workers, max(1, os.cpu_count() or 4))
            kept_rows, parse_counts, dropped_error_ids, dropped_no_latlong_ids = _parallel_parse(
                joined_rows, parse_workers,
            )
            t_parse = time.monotonic() - t0

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

            # --- STAGE 2: Geo-enrich (parallel via thread pool + conn pool) ---
            t0 = time.monotonic()
            if enrich_pool and len(kept_rows) > 5000 and workers > 1:
                enriched_rows = _parallel_enrich_rows(enrich_pool, cfg, kept_rows, workers)
            elif enrich_pool:
                enrich_conn = enrich_pool.getconn()
                try:
                    enriched_rows = geo_enrichment.enrich_rows(enrich_conn, cfg, kept_rows)
                finally:
                    enrich_pool.putconn(enrich_conn)
            else:
                enriched_rows = geo_enrichment.enrich_rows(conn, cfg, kept_rows)
            t_enrich = time.monotonic() - t0

            # --- STAGE 3: Write (serialized for DB safety) ---
            t0 = time.monotonic()
            writer.write_rows(conn, cfg, enriched_rows)
            t_write = time.monotonic() - t0

            watermark.update_last_processed_id(
                conn, watermark_table, pipeline_name, window_start, new_last_processed_id,
            )
            completed_request_rows += chunk_request_rows
            progress_percent = (
                100.0 if total_request_rows == 0
                else min(100.0, completed_request_rows * 100.0 / total_request_rows)
            )
            chunk_elapsed = time.monotonic() - chunk_start
            logger.info(
                "Chunk #%d: %d rows, parse=%.1fs, enrich=%.1fs, write=%.1fs, total=%.1fs | "
                "Progress: %d/%d (%.1f%%)",
                chunk_num, len(joined_rows),
                t_parse, t_enrich, t_write, chunk_elapsed,
                completed_request_rows, total_request_rows, progress_percent,
            )

        prefetch_executor.shutdown(wait=False)

        if all_error_ids:
            _write_error_ids_file(os.path.join(error_dir, "response_error_ids.txt"), all_error_ids)
        if all_no_latlong_ids:
            _write_error_ids_file(os.path.join(error_dir, "no_latlong_error_ids.txt"), all_no_latlong_ids)

        watermark.mark_window_status(conn, watermark_table, pipeline_name, window_start, "SUCCESS")
        pipeline_elapsed = time.monotonic() - pipeline_start
        logger.info("Window complete in %.1fs. Totals: %s", pipeline_elapsed, totals)

        monthly_reports = metrics.update_monthly_report_workbooks(
            conn, cfg, window_start, window_end,
        )
        latest_month_reports = list(monthly_reports.values())[-1]
        report_path = latest_month_reports["latlong"]
        run_summary = metrics.build_run_summary(pipeline_name, window_start, window_end, totals, report_path)
        run_summary["output_table"] = output_table
        run_summary["report_paths"] = monthly_reports
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
        if enrich_pool:
            try:
                enrich_pool.closeall()
                logger.info("Connection pool closed.")
            except Exception:
                pass


if __name__ == "__main__":
    try:
        run()
    except Exception:
        sys.exit(1)

