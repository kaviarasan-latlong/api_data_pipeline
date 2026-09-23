"""
geo_enrichment.py  -  STAGE 2: GEO ENRICHMENT

Rows that already have state/district/pincode (pulled directly out of the
API payload in Stage 1) are left untouched. Everything else gets a
lat/long -> Point -> ST_Intersects lookup against the area geometry table,
then a join to the area table for the human-readable state/district/pincode.

Batched: builds one ST_Intersects query per `batch_size` rows (via UNNEST)
instead of one query per row, since a chunk can be 50-100k rows.
"""

import logging

logger = logging.getLogger(__name__)


def _needs_enrichment(row):
    return not (row.get("state") and row.get("district") and row.get("pincode"))


def _enrich_batch(conn, cfg, batch: list[dict]):
    tables = cfg["tables"]
    geo_cols = cfg["geo_enrichment"]["geom_table_columns"]
    area_cols = cfg["geo_enrichment"]["area_table_columns"]

    idxs = list(range(len(batch)))
    lats = [r["lat"] for r in batch]
    lngs = [r["lng"] for r in batch]

    sql = f"""
        WITH pts AS (
            SELECT * FROM UNNEST(%s::int[], %s::float8[], %s::float8[]) AS t(idx, lat, lng)
        )
        SELECT pts.idx,
               area.{area_cols['pincode']},
               area.{area_cols['district']},
               area.{area_cols['state']}
        FROM pts
        JOIN {tables['geom_table']} g
            ON ST_Intersects(g.{geo_cols['geom']},
                              ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326))
        JOIN {tables['area_table']} area
            ON area.{area_cols['a_id']} = g.{geo_cols['a_id']}
    """
    with conn.cursor() as cur:
        cur.execute(sql, (idxs, lats, lngs))
        results = cur.fetchall()

    by_idx = {r[0]: {"pincode": r[1], "district": r[2], "state": r[3]} for r in results}
    return by_idx


def enrich_rows(conn, cfg, rows: list[dict]):
    to_enrich_idx = [i for i, r in enumerate(rows) if _needs_enrichment(r)]
    if not to_enrich_idx:
        return rows

    batch_size = cfg["geo_enrichment"].get("batch_size", 5000)
    enriched_count = 0

    for start in range(0, len(to_enrich_idx), batch_size):
        idx_slice = to_enrich_idx[start:start + batch_size]
        batch = [rows[i] for i in idx_slice]
        results = _enrich_batch(conn, cfg, batch)

        for local_idx, global_idx in enumerate(idx_slice):
            geo = results.get(local_idx)
            if not geo:
                continue
            row = rows[global_idx]
            row["pincode"] = row.get("pincode") or geo["pincode"]
            row["district"] = row.get("district") or geo["district"]
            row["state"] = row.get("state") or geo["state"]
            enriched_count += 1

    logger.info(
        "Stage 2 geo enrichment: %d/%d rows needed lookup, %d resolved to an area.",
        len(to_enrich_idx), len(rows), enriched_count,
    )
    return rows
