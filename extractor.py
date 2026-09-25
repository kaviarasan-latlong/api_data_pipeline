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
from typing import Iterator

logger = logging.getLogger(__name__)


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


def _fetch_session_activity_latlng(conn, cfg, requestids):
    if not requestids:
        return {}

    tables = cfg["tables"]
    sql = f"""
        SELECT requestid, searched_latitude, searched_longitude
        FROM {tables['session_activities']}
        WHERE requestid = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (list(requestids),))
        rows = cur.fetchall()

    result = {}
    for req_id, lat, lng in rows:
        if lat is None or lng is None:
            continue
        result[req_id] = {"lat": float(lat), "lng": float(lng)}
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
        session_latlng_by_id = _fetch_session_activity_latlng(conn, cfg, requestids)

        joined = []
        for req in request_rows:
            resp = response_by_id.get(req["requestid"], {})
            session_latlng = session_latlng_by_id.get(req["requestid"])
            if session_latlng and req["api_type"] in {
                "/v4.1/brands/:brand_id/stores_around.json",
                "/v4.1/brands/:brand_id/find.json",
                "/v4.1/brands/:brand_id/search_with_disable.json",
            }:
                resp = {**resp, "lat": session_latlng["lat"], "lng": session_latlng["lng"]}
            joined.append({**req, **resp})

        new_last_processed_id = request_rows[-1]["id"]
        logger.info(
            "Fetched chunk of %d rows (ids %s..%s), %d matched responses.",
            len(request_rows), request_rows[0]["id"], new_last_processed_id, len(response_by_id),
        )
        yield joined, new_last_processed_id
