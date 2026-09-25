"""
watermark.py

Owns the `pipeline_watermark` table:
    pipeline_name     text
    window_start      timestamp
    window_end        timestamp
    last_processed_id bigint
    status            text   -- RUNNING | SUCCESS | FAILED
    updated_at        timestamp

Responsible for:
  - creating the table if it doesn't exist
  - deciding the 3-day window for this run (new window, or resume a
    RUNNING one left over from a crashed previous run)
  - persisting progress (last_processed_id) after every chunk so a
    restart resumes instead of reprocessing from scratch
"""

import datetime as dt
import logging

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    pipeline_name     TEXT NOT NULL,
    window_start      TIMESTAMP NOT NULL,
    window_end        TIMESTAMP NOT NULL,
    last_processed_id BIGINT NOT NULL DEFAULT 0,
    status            TEXT NOT NULL DEFAULT 'RUNNING',
    updated_at        TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, window_start)
);
"""


def ensure_watermark_table(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL.format(table=table))
    conn.commit()


def get_or_create_window(conn, table: str, pipeline_name: str, window_days: int,
                        request_table: str | None = None, created_at_col: str = "created_at"):
    """
    Returns (window_start, window_end, last_processed_id, is_resume: bool)

    - If the most recent row for this pipeline is still RUNNING, resume it
      (crash recovery) using its stored last_processed_id.
    - Otherwise start a new window immediately after the last SUCCESS
      window_end.
    - If there is no history yet and the source log table exists, start from
      the minimum timestamp in that table so a first run tests the earliest
      3-day slice instead of defaulting to "now - 3 days".
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT window_start, window_end, last_processed_id, status
            FROM {table}
            WHERE pipeline_name = %s
            ORDER BY window_end DESC
            LIMIT 1
            """,
            (pipeline_name,),
        )
        row = cur.fetchone()

    if row and row[3] == "RUNNING":
        window_start, window_end, last_processed_id, _ = row
        logger.info(
            "Resuming RUNNING window %s -> %s from last_processed_id=%s",
            window_start, window_end, last_processed_id,
        )
        return window_start, window_end, last_processed_id, True

    if row:
        window_start = row[1]  # previous window_end becomes new window_start
    else:
        window_start = dt.datetime.utcnow() - dt.timedelta(days=window_days)
        if request_table:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MIN({created_at_col}) FROM {request_table}",
                )
                min_created = cur.fetchone()[0]
            if min_created is not None:
                window_start = min_created

    window_end = window_start + dt.timedelta(days=window_days)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {table}
                (pipeline_name, window_start, window_end, last_processed_id, status, updated_at)
            VALUES (%s, %s, %s, 0, 'RUNNING', now())
            ON CONFLICT (pipeline_name, window_start) DO UPDATE
                SET status = 'RUNNING', updated_at = now()
            """,
            (pipeline_name, window_start, window_end),
        )
    conn.commit()
    logger.info("Starting new window %s -> %s", window_start, window_end)
    return window_start, window_end, 0, False


def update_last_processed_id(conn, table: str, pipeline_name: str,
                              window_start, last_processed_id: int):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {table}
            SET last_processed_id = %s, updated_at = now()
            WHERE pipeline_name = %s AND window_start = %s
            """,
            (last_processed_id, pipeline_name, window_start),
        )
    conn.commit()


def mark_window_status(conn, table: str, pipeline_name: str, window_start, status: str):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {table}
            SET status = %s, updated_at = now()
            WHERE pipeline_name = %s AND window_start = %s
            """,
            (status, pipeline_name, window_start),
        )
    conn.commit()
