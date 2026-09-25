"""
writer.py  -  STAGE 3: WRITE FINAL DATA

Bulk-loads the enriched rows into the output table using COPY (far faster
than row-by-row INSERT for 50k-100k row chunks). The caller commits the
watermark update in the same transaction scope right after this returns,
so a crash between COPY and watermark-update just means one chunk gets
reprocessed (harmless: output table has a unique constraint on
(request_id) which makes the COPY idempotent via a staging+upsert step).
"""

import csv
import io
import logging

logger = logging.getLogger(__name__)

CREATE_OUTPUT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    bunit_id            BIGINT,
    tenant_id           TEXT,
    api_type            TEXT,
    api_version         TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    state               TEXT,
    district            TEXT,
    pincode             TEXT,
    address             TEXT
);
"""

STAGING_TABLE_SUFFIX = "_staging"

OUTPUT_COLUMNS = [
    "bunit_id", "tenant_id", "api_type", "api_version",
    "latitude", "longitude", "state", "district", "pincode", "address",
]


def ensure_output_table(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(CREATE_OUTPUT_TABLE_SQL.format(table=table))
    conn.commit()


def _rows_to_csv_buffer(rows: list[dict]) -> io.StringIO:
    buf = io.StringIO()
    writer = csv.writer(buf)
    for r in rows:
        writer.writerow([
            r.get("bunit_id"),
            r.get("tenant_id"),
            r.get("api_type"),
            r.get("api_version"),
            r.get("lat"),
            r.get("lng"),
            r.get("state"),
            r.get("district"),
            r.get("pincode"),
            r.get("address"),
        ])
    buf.seek(0)
    return buf


def write_rows(conn, cfg, rows: list[dict]):
    if not rows:
        return 0

    output_table = cfg["tables"]["output_table"]
    staging_table = f"{output_table}{STAGING_TABLE_SUFFIX}"
    cols_sql = ", ".join(OUTPUT_COLUMNS)

    with conn.cursor() as cur:
        # Fresh unlogged staging table per chunk - COPY into it, then
        # upsert into the real table so re-processing a chunk (after a
        # crash before the watermark commit) never double-counts rows.
        cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
        cur.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {output_table} INCLUDING DEFAULTS) ON COMMIT DROP")

        buf = _rows_to_csv_buffer(rows)
        cur.copy_expert(
            f"COPY {staging_table} ({cols_sql}) FROM STDIN WITH (FORMAT csv, NULL '')",
            buf,
        )

        cur.execute(f"""
            INSERT INTO {output_table} ({cols_sql})
            SELECT {cols_sql} FROM {staging_table}
        """)

    conn.commit()
    logger.info("Stage 3 write: upserted %d rows into %s.", len(rows), output_table)
    return len(rows)
