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


def _fetch_request_chunk(conn, cfg, window_start, window_end, last_processed_id):
    tables = cfg["tables"]
    cols = cfg["pipeline"]["columns"]["request_logs"]
    api_types = tuple(cfg["pipeline"]["included_api_types"])
    chunk_size = cfg["pipeline"]["chunk_size"]

    sql = f"""
        SELECT {cols['id']}, {cols['requestid']}, {cols['api_type']}, {cols['api_version']},
               {cols['bunit_id']}, {cols['tenant_id']}, {cols['path']}, {cols['query_string']},
               {cols['created_at']}, {cols['status']}
        FROM {tables['request_logs']}
        WHERE {cols['created_at']} >= %s
          AND {cols['created_at']} < %s
          AND {cols['api_type']} = ANY(%s)
          AND {cols['id']} > %s
        ORDER BY {cols['id']}
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
        }
        for r in rows
    ]


def _fetch_response_batch(conn, cfg, requestids):
    if not requestids:
        return {}

    tables = cfg["tables"]
    cols = cfg["pipeline"]["columns"]["response_logs"]

    sql = f"""
        SELECT {cols['requestid']}, {cols['data']}, {cols['created_at']}
        FROM {tables['response_logs']}
        WHERE {cols['requestid']} = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (list(requestids),))
        rows = cur.fetchall()

    return {r[0]: {"response_data": r[1], "response_created_at": r[2], "response_status": None} for r in rows}


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
    """
    last_processed_id = start_last_processed_id

    while True:
        request_rows = _fetch_request_chunk(conn, cfg, window_start, window_end, last_processed_id)
        if not request_rows:
            logger.info("No more rows in window after id=%s - window exhausted.", last_processed_id)
            return

        requestids = [r["requestid"] for r in request_rows]
        response_by_id = _fetch_response_batch(conn, cfg, requestids)
        session_rows = _fetch_session_activity_chunk(conn, cfg, window_start, window_end)

        joined = []
        for req in request_rows:
            resp = response_by_id.get(req["requestid"], {})
            joined.append({**req, **resp})

        for session_row in session_rows:
            joined.append({
                **session_row,
                "requestid": None,
                "response_data": None,
                "response_status": None,
            })

        new_last_processed_id = max(
            [request_rows[-1]["id"], max((sr["id"] for sr in session_rows), default=0)]
        )
        logger.info(
            "Fetched normal chunk of %d rows (ids %s..%s), %d matched responses, %d session-only rows added.",
            len(request_rows),
            request_rows[0]["id"],
            new_last_processed_id,
            len(response_by_id),
            len(session_rows),
        )
        yield joined, new_last_processed_id
