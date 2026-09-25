"""
geo_enrichment.py  -  STAGE 2: GEO ENRICHMENT

The server schema uses aa_geom + admin_area hierarchy. We match the point
against aa_geom, then walk the parent chain in admin_area to derive the
pincode -> district -> state names. If a pincode is already present in the API
response, the row is kept as-is.
"""

import logging
import re

logger = logging.getLogger(__name__)


def _needs_enrichment(row):
    return not (row.get("state") and row.get("district") and row.get("pincode"))


def _clean_area_name(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return re.sub(r"^\d{6}\s*[-–]?\s*", "", text).strip()


def _parse_pincode(value):
    if value is None:
        return None
    match = re.match(r"^\s*(\d{6})\s*(?:[-–]\s*.*)?$", str(value).strip())
    if not match:
        return None
    return match.group(1)


def _resolve_admin_hierarchy(conn, cfg, start_area_id):
    area_cols = cfg["geo_enrichment"]["area_table_columns"]
    parent_col = area_cols.get("parent_id", "aa_in_aa_id")
    display_col = area_cols.get("display_name", "display_name")
    a_id_col = area_cols.get("a_id", "id")
    area_table = cfg["tables"]["area_table"]

    names = []
    seen = set()
    current_id = start_area_id

    while current_id is not None and current_id not in seen:
        seen.add(current_id)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {a_id_col}, {display_col}, {parent_col} FROM {area_table} WHERE {a_id_col} = %s LIMIT 1",
                (current_id,),
            )
            row = cur.fetchone()
        if not row:
            break
        names.append(str(row[1]).strip())
        current_id = row[2]

    pincode = None
    district = None
    state = None

    for name in names:
        parsed = _parse_pincode(name)
        if pincode is None and parsed:
            pincode = parsed
            continue
        if pincode and district is None:
            district = _clean_area_name(name)
            continue
        if district and state is None:
            state = _clean_area_name(name)
            break

    # Do not invent district/state when the geometry lookup does not produce a
    # valid pincode-bearing area. This prevents wrong labels like Dhurali/Sunam
    # from being assigned to a point that matched a nearby polygon but not the
    # exact pincode-bearing admin chain.
    if pincode is None:
        return {"pincode": None, "district": None, "state": None}

    return {
        "pincode": pincode,
        "district": district,
        "state": state,
    }


def _enrich_batch(conn, cfg, batch: list[dict]):
    tables = cfg["tables"]
    geo_cols = cfg["geo_enrichment"]["geom_table_columns"]
    area_cols = cfg["geo_enrichment"]["area_table_columns"]
    area_table = tables["area_table"]
    geom_table = tables["geom_table"]
    area_id_col = area_cols.get("a_id", "id")
    parent_id_col = area_cols.get("parent_id", "aa_in_aa_id")
    display_name_col = area_cols.get("display_name", "display_name")
    geom_col = geo_cols["geom"]
    geom_a_id_col = geo_cols["a_id"]

    idxs = list(range(len(batch)))
    lats = [r["lat"] for r in batch]
    lngs = [r["lng"] for r in batch]

    sql = f"""
        WITH pts AS (
            SELECT * FROM UNNEST(%s::int[], %s::float8[], %s::float8[]) AS t(idx, lat, lng)
        )
        SELECT pts.idx,
               g.{geom_a_id_col},
               a.{area_id_col},
               a.{display_name_col},
               a.{parent_id_col}
        FROM pts
        JOIN {geom_table} g
          ON ST_Intersects(g.{geom_col}, ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326))
        JOIN {area_table} a
          ON a.{area_id_col} = g.{geom_a_id_col}
        ORDER BY pts.idx
    """
    with conn.cursor() as cur:
        cur.execute(sql, (idxs, lats, lngs))
        results = cur.fetchall()

    by_idx = {}
    for row in results:
        idx = row[0]
        if idx not in by_idx:
            by_idx[idx] = row[1]

    resolved = {}
    for idx, area_id in by_idx.items():
        resolved[idx] = _resolve_admin_hierarchy(conn, cfg, area_id)
    return resolved


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
            row["pincode"] = row.get("pincode") or geo.get("pincode")
            row["district"] = row.get("district") or geo.get("district")
            row["state"] = row.get("state") or geo.get("state")
            enriched_count += 1

    logger.info(
        "Stage 2 geo enrichment: %d/%d rows needed lookup, %d resolved to an area.",
        len(to_enrich_idx), len(rows), enriched_count,
    )
    return rows
