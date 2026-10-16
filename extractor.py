"""
extractor.py

Chunked extraction loop over request_logs, keyed off the watermark's
(window_start, window_end, last_processed_id).

For each chunk:
  1. Pull up to `chunk_size` rows from request_logs where:
       created_date in [window_start, window_end)
       AND api_type IN (config-driven filter list)
       AND id > last_processed_id
     ordered by id (so last_processed_id is a stable resume point).
  2. Collect the uniqueid values from that chunk.
  3. One batch query against response_logs WHERE uniqueid = ANY(...).
  4. Join request + response in memory, keyed by uniqueid.

Yields dicts ready for parser.py; the caller (main.py) is responsible for
persisting the new last_processed_id via watermark.update_last_processed_id
after each chunk is fully processed (extract -> parse -> enrich -> write).
"""

import logging
import re
from typing import Iterator

logger = logging.getLogger(__name__)


def _extract_bunit_id_from_path(path: str | None):
    if not path:
        return None
    match = re.search(r"/brands/(\d+)(?:/|\.|\?|$)", str(path))
    if not match:
        return None
    return int(match.group(1))


def _extract_api_version(api_type: str | None):
    if not api_type:
        return None
    match = re.search(r"/v(\d+\.\d+)", str(api_type))
    if not match:
        return None
    return f"v{match.group(1)}"


def _fetch_chunk_with_responses(conn, cfg, window_start, window_end, last_processed_id):
    """
    Directly join request_logs and response_logs in one single query.
    Because both tables live on the same foreign database server,
    PostgreSQL pushes the JOIN down to the remote server, eliminating
    the overhead of sending 50,000-500,000 request IDs across the network
    in a separate WHERE requestid = ANY(...) query.
    """
    tables = cfg["tables"]
    req_cols = cfg["pipeline"]["columns"]["request_logs"]
    resp_cols = cfg["pipeline"]["columns"]["response_logs"]
    api_types = tuple(cfg["pipeline"]["included_api_types"])
    chunk_size = cfg["pipeline"]["chunk_size"]

    sql = f"""
        SELECT 
            r.{req_cols['id']}, 
            r.{req_cols['requestid']}, 
            r.{req_cols['api_type']}, 
            r.{req_cols['api_version']},
            r.{req_cols['bunit_id']}, 
            r.{req_cols['tenant_id']}, 
            r.{req_cols['path']}, 
            r.{req_cols['query_string']},
            r.{req_cols['created_at']}, 
            r.{req_cols['status']},
            res.{resp_cols['data']} AS response_data,
            res.{resp_cols['created_at']} AS response_created_at
        FROM {tables['request_logs']} r
        LEFT JOIN {tables['response_logs']} res 
            ON res.{resp_cols['requestid']} = r.{req_cols['requestid']}
        WHERE r.{req_cols['created_at']} >= %s
          AND r.{req_cols['created_at']} < %s
          AND r.{req_cols['api_type']} = ANY(%s)
          AND r.{req_cols['id']} > %s
        ORDER BY r.{req_cols['id']}
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (window_start, window_end, list(api_types), last_processed_id, chunk_size))
        rows = cur.fetchall()

    return [
        {
            "id": r[0],
            "requestid": r[1],
            "api_type": r[2],
            "api_version": r[3],
            "bunit_id": r[4],
            "tenant_id": r[5],
            "request_path": r[6],
            "request_query_string": r[7],
            "created_at": r[8],
            "request_status": r[9],
            "response_data": r[10],
            "response_created_at": r[11],
            "response_status": None,
        }
        for r in rows
    ]


def count_request_rows(conn, cfg, window_start, window_end, through_id=None):
    """Fast estimate/count for window progress without full remote table scan."""
    tables = cfg["tables"]
    cols = cfg["pipeline"]["columns"]["request_logs"]
    api_types = tuple(cfg["pipeline"]["included_api_types"])

    id_filter = f"AND {cols['id']} <= %s" if through_id is not None else ""
    params = [window_start, window_end, list(api_types)]
    if through_id is not None:
        params.append(through_id)
    sql = f"""
        SELECT COUNT(*)
        FROM {tables['request_logs']}
        WHERE {cols['created_at']} >= %s
          AND {cols['created_at']} < %s
          AND {cols['api_type']} = ANY(%s)
          {id_filter}
    """
    with conn.cursor() as cur:
        # Set a short 10s statement timeout so COUNT never blocks pipeline startup on foreign tables
        cur.execute("SET LOCAL statement_timeout = 10000")
        try:
            cur.execute(sql, params)
            return cur.fetchone()[0]
        except Exception as e:
            logger.warning("Could not count request rows (foreign table timeout/skip): %s", e)
            conn.rollback()
            return 0


def _fetch_session_activity_chunk(conn, cfg, window_start, window_end):
    session_api_types = tuple(cfg["pipeline"].get("session_only_api_types", []))
    if not session_api_types:
        return []

    tables = cfg["tables"]
    sql = f"""
        SELECT id, session_id, api_type, path, query_string, searched_latitude, searched_longitude, created_at, status
        FROM {tables['session_activities']}
        WHERE api_type = ANY(%s)
          AND created_at >= %s
          AND created_at < %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (list(session_api_types), window_start, window_end))
        rows = cur.fetchall()

    result = []
    for row in rows:
        row_id, session_id, api_type, path, query_string, lat, lng, created_at, status = row
        if lat is None or lng is None:
            continue
        if float(lat) == 0.0 or float(lng) == 0.0:
            continue
        result.append({
            "id": row_id,
            "session_id": session_id,
            "api_type": api_type,
            "bunit_id": _extract_bunit_id_from_path(path),
            "api_version": _extract_api_version(api_type),
            "tenant_id": None,
            "request_path": path,
            "request_query_string": query_string,
            "created_at": created_at,
            "request_status": status,
            "lat": float(lat),
            "lng": float(lng),
        })
    return result


def iterate_chunks(conn, cfg, window_start, window_end, start_last_processed_id) -> Iterator[tuple[list[dict], int]]:
    """
    Generator yielding (joined_rows, new_last_processed_id) per chunk.
    Stops when a chunk comes back empty (window exhausted).

    Session-activity rows (for the 3 brand-store APIs) are fetched ONCE
    before the loop and included only in the first yielded chunk, so they
    are never duplicated across chunks.
    """
    last_processed_id = start_last_processed_id

    # --- Fetch session rows exactly once for the whole window ---
    session_rows = _fetch_session_activity_chunk(conn, cfg, window_start, window_end)
    if session_rows:
        logger.info("Fetched %d session-activity rows for window (one-time).", len(session_rows))
    session_emitted = False

    while True:
        joined_rows = _fetch_chunk_with_responses(conn, cfg, window_start, window_end, last_processed_id)
        if not joined_rows:
            # If we still have un-emitted session rows and no request rows
            # at all, yield them as a standalone chunk.
            if session_rows and not session_emitted:
                session_emitted = True
                session_joined = [
                    {**sr, "requestid": None, "response_data": None, "response_status": None}
                    for sr in session_rows
                ]
                logger.info(
                    "No request rows remain; yielding %d session-only rows.",
                    len(session_joined),
                )
                yield session_joined, last_processed_id
            logger.info("No more rows in window after id=%s - window exhausted.", last_processed_id)
            return

        joined = joined_rows

        # Append session rows only on the first chunk
        session_count_this_chunk = 0
        if session_rows and not session_emitted:
            session_emitted = True
            for session_row in session_rows:
                joined.append({
                    **session_row,
                    "requestid": None,
                    "response_data": None,
                    "response_status": None,
                })
            session_count_this_chunk = len(session_rows)

        # last_processed_id tracks only request-log rows (stable resume key)
        new_last_processed_id = joined_rows[-1]["id"]
        logger.info(
            "Fetched chunk of %d rows directly with response data (ids %s..%s), %d session rows included.",
            len(joined_rows),
            joined_rows[0]["id"],
            new_last_processed_id,
            session_count_this_chunk,
        )
        yield joined, new_last_processed_id
        last_processed_id = new_last_processed_id
