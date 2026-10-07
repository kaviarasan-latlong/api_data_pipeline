"""
geo_enrichment.py  -  STAGE 2: GEO ENRICHMENT

The server schema uses aa_geom + admin_area hierarchy. We match the point
against aa_geom, then walk the parent chain in admin_area to derive the
pincode -> district -> state names. If a pincode is already present in the API
response, the row is kept as-is.

OPTIMIZATIONS (v2):
  - Schema introspection (_table_has_columns) is cached per (table, columns)
    so information_schema is queried at most once per table per process.
  - Admin hierarchy resolution uses a single recursive CTE for ALL area_ids
    in a batch, replacing thousands of individual per-row queries.
"""

import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema introspection — cached so we hit information_schema at most once
# per table per process lifetime.
# ---------------------------------------------------------------------------

# Module-level cache: { (table_name, frozenset(columns)) -> {col: bool} }
_schema_cache: dict[tuple, dict[str, bool]] = {}


def _table_has_columns(conn, table_name: str, columns: list[str]):
    """Check which columns exist in a table (cached after first call per table)."""
    cache_key = (table_name, frozenset(columns))
    if cache_key in _schema_cache:
        return _schema_cache[cache_key]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        )
        existing = {row[0] for row in cur.fetchall()}

    result = {col: col in existing for col in columns}
    _schema_cache[cache_key] = result
    return result


def _needs_enrichment(row):
    return not (row.get("state") and row.get("district") and row.get("pincode"))


def _clean_area_name(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    cleaned = re.sub(r"^\d{6}\s*[-–]?\s*", "", text).strip()
    return cleaned or None


def _parse_pincode(value):
    if value is None:
        return None
    match = re.match(r"^\s*(\d{6})\s*(?:[-–]\s*.*)?$", str(value).strip())
    if not match:
        return None
    return match.group(1)


def _normalize_source_geo_fields(row):
    row["pincode"] = _parse_pincode(row.get("pincode"))
    invalid_values = {"", "yes", "no", "true", "false", "null", "none", "n/a", "na"}
    for field in ("state", "district"):
        value = row.get(field)
        if value is None or isinstance(value, bool):
            row[field] = None
            continue
        text = str(value).strip()
        row[field] = None if text.casefold() in invalid_values else text or None


# ---------------------------------------------------------------------------
# Bulk admin hierarchy resolution via recursive CTE
# ---------------------------------------------------------------------------

def _resolve_admin_hierarchy_bulk(conn, cfg, area_ids: list):
    """
    Resolve pincode/district/state for MULTIPLE area_ids in a single
    recursive CTE query, replacing the old per-row iterative approach.

    Returns {area_id: {"pincode": ..., "district": ..., "state": ...}}
    """
    if not area_ids:
        return {}

    area_cols = cfg["geo_enrichment"]["area_table_columns"]
    parent_col = area_cols.get("parent_id", "aa_in_aa_id")
    display_col = area_cols.get("display_name", "display_name")
    a_id_col = area_cols.get("a_id", "id")
    area_table = cfg["tables"]["area_table"]
    schema_info = _table_has_columns(conn, area_table, ["aa_order", "to_date"])
    has_aa_order = schema_info.get("aa_order", False)
    has_to_date = schema_info.get("to_date", False)

    # Deduplicate area_ids to avoid redundant work
    unique_ids = list(set(area_ids))

    # Build the recursive CTE that walks the full parent chain for all
    # start IDs in one shot, up to depth 10 (pincode -> district -> state
    # is typically 3 levels; 10 is a generous safety cap).
    aa_order_select = f", a.aa_order" if has_aa_order else ", NULL::int AS aa_order"
    to_date_filter = f"AND a.to_date IS NULL" if has_to_date else ""

    sql = f"""
        WITH RECURSIVE chain AS (
            SELECT
                a.{a_id_col} AS start_id,
                a.{a_id_col} AS current_id,
                a.{display_col} AS display_name,
                a.{parent_col} AS parent_id
                {aa_order_select},
                1 AS depth
            FROM {area_table} a
            WHERE a.{a_id_col} = ANY(%s)
            {to_date_filter}

            UNION ALL

            SELECT
                c.start_id,
                a.{a_id_col} AS current_id,
                a.{display_col} AS display_name,
                a.{parent_col} AS parent_id
                {aa_order_select},
                c.depth + 1
            FROM chain c
            JOIN {area_table} a ON a.{a_id_col} = c.parent_id
            WHERE c.depth < 10
              AND c.parent_id IS NOT NULL
              {to_date_filter}
        )
        SELECT start_id, current_id, display_name, parent_id, aa_order, depth
        FROM chain
        ORDER BY start_id, depth
    """

    with conn.cursor() as cur:
        cur.execute(sql, (unique_ids,))
        rows = cur.fetchall()

    # Group chain rows by start_id
    chains: dict[int, list] = {}
    for row in rows:
        start_id = row[0]
        if start_id not in chains:
            chains[start_id] = []
        chains[start_id].append(row)

    # Resolve pincode/district/state from each chain
    results = {}
    for start_id, chain_rows in chains.items():
        pincode = None
        district = None
        state = None

        for row in chain_rows:
            # row: (start_id, current_id, display_name, parent_id, aa_order, depth)
            name = str(row[2]).strip() if row[2] else ""
            area_order = row[4]  # aa_order or NULL
            parsed = _parse_pincode(name)

            if pincode is None and ((area_order == 55) or parsed):
                if parsed:
                    pincode = parsed
                elif area_order == 55:
                    pincode = _parse_pincode(name)
                continue
            if pincode is None:
                continue
            if area_order == 8 and district is None:
                district = _clean_area_name(name)
                continue
            if area_order == 9 and state is None:
                state = _clean_area_name(name)
                break

            if district is None and pincode and name and area_order is None:
                district = _clean_area_name(name)
                continue
            if district and state is None and area_order is None:
                state = _clean_area_name(name)
                break

        if pincode:
            results[start_id] = {
                "pincode": pincode,
                "district": district,
                "state": state,
            }
        else:
            results[start_id] = {"pincode": None, "district": None, "state": None}

    return results


# ---------------------------------------------------------------------------
# Batch spatial lookup + hierarchy resolution
# ---------------------------------------------------------------------------

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
    fallback_distance = float(cfg["geo_enrichment"].get("fallback_distance_meters", 1000))
    if not 0 < fallback_distance <= 1000:
        raise ValueError("geo_enrichment.fallback_distance_meters must be between 0 and 1000")

    # Cached schema checks — each of these hits the DB at most once
    geom_schema = _table_has_columns(conn, geom_table, ["to_date", "aa_order"])
    area_schema = _table_has_columns(conn, area_table, ["to_date"])

    idxs = list(range(len(batch)))
    lats = [r["lat"] for r in batch]
    lngs = [r["lng"] for r in batch]

    geom_where = []
    if geom_schema.get("to_date", False):
        geom_where.append("g.to_date IS NULL")
    if geom_schema.get("aa_order", False):
        geom_where.append("g.aa_order = 55")
    geom_filter_sql = " AND ".join(geom_where) if geom_where else "TRUE"
    area_filter_sql = "AND a.to_date IS NULL" if area_schema.get("to_date", False) else ""
    fallback_distance_sql = f"{fallback_distance:.3f}"

    sql = f"""
        WITH pts AS (
                        SELECT idx, lat, lng,
                                     ST_SetSRID(ST_MakePoint(lng, lat), 4326) AS geom
                        FROM UNNEST(%s::int[], %s::float8[], %s::float8[])
                                 AS t(idx, lat, lng)
        )
        SELECT pts.idx,
                             COALESCE(exact_match.aa_id, nearby_match.aa_id) AS geom_aa_id,
                             CASE
                                     WHEN exact_match.aa_id IS NOT NULL THEN 'exact'
                                     WHEN nearby_match.aa_id IS NOT NULL THEN 'within_1km'
                                     ELSE NULL
                             END AS match_type,
               a.{area_id_col},
               a.{display_name_col},
               a.{parent_id_col}
        FROM pts
                LEFT JOIN LATERAL (
                        SELECT g.{geom_a_id_col} AS aa_id
                        FROM {geom_table} g
                        WHERE ST_Intersects(g.{geom_col}, pts.geom)
                            AND {geom_filter_sql}
                        ORDER BY g.{geom_a_id_col}
                        LIMIT 1
                ) exact_match ON TRUE
                LEFT JOIN LATERAL (
                        SELECT g.{geom_a_id_col} AS aa_id
                        FROM {geom_table} g
                        WHERE exact_match.aa_id IS NULL
                            AND {geom_filter_sql}
                            AND g.{geom_col} && ST_MakeEnvelope(
                                    ST_X(pts.geom) - {fallback_distance_sql} /
                                            (111320.0 * GREATEST(ABS(COS(RADIANS(ST_Y(pts.geom)))), 0.01)),
                                    ST_Y(pts.geom) - {fallback_distance_sql} / 110574.0,
                                    ST_X(pts.geom) + {fallback_distance_sql} /
                                            (111320.0 * GREATEST(ABS(COS(RADIANS(ST_Y(pts.geom)))), 0.01)),
                                    ST_Y(pts.geom) + {fallback_distance_sql} / 110574.0,
                                    4326
                            )
                            AND ST_DWithin(
                                    g.{geom_col}::geography,
                                    pts.geom::geography,
                                    {fallback_distance_sql}
                            )
                        ORDER BY ST_Distance(g.{geom_col}::geography, pts.geom::geography),
                                         g.{geom_a_id_col}
                        LIMIT 1
                ) nearby_match ON TRUE
                LEFT JOIN {area_table} a
                    ON a.{area_id_col} = COALESCE(exact_match.aa_id, nearby_match.aa_id)
                    {area_filter_sql}
        ORDER BY pts.idx
    """
    with conn.cursor() as cur:
        cur.execute(sql, (idxs, lats, lngs))
        results = cur.fetchall()

    # Collect the first matching area_id per row index
    by_idx = {}
    fallback_matches = 0
    for row in results:
        idx = row[0]
        if idx not in by_idx:
            by_idx[idx] = row[1]
        if row[2] == "within_1km":
            fallback_matches += 1

    if fallback_matches:
        logger.info(
            "Geo lookup used the nearest pincode area within %.0f m for %d/%d coordinates.",
            fallback_distance,
            fallback_matches,
            len(batch),
        )

    unmatched_points = len(batch) - len(by_idx)
    if unmatched_points:
        logger.info(
            "Geo lookup: %d/%d coordinates did not intersect an active aa_geom pincode area.",
            unmatched_points,
            len(batch),
        )

    # --- OPTIMIZATION: bulk resolve all area_ids at once via recursive CTE ---
    unique_area_ids = list(set(by_idx.values()))
    hierarchy_cache = _resolve_admin_hierarchy_bulk(conn, cfg, unique_area_ids)

    # Map results back to row indices
    resolved = {}
    for idx, area_id in by_idx.items():
        resolved[idx] = hierarchy_cache.get(area_id, {"pincode": None, "district": None, "state": None})

    return resolved


def enrich_rows(conn, cfg, rows: list[dict]):
    for row in rows:
        _normalize_source_geo_fields(row)

    to_enrich_idx = [i for i, r in enumerate(rows) if _needs_enrichment(r)]
    if not to_enrich_idx:
        return rows

    batch_size = cfg["geo_enrichment"].get("batch_size", 5000)

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

    complete_count = sum(
        bool(row.get("pincode") and row.get("district") and row.get("state"))
        for row in rows
    )
    partial_count = sum(
        bool(row.get("pincode") or row.get("district") or row.get("state"))
        and not (row.get("pincode") and row.get("district") and row.get("state"))
        for row in rows
    )
    unresolved_count = len(rows) - complete_count - partial_count

    logger.info(
        "Stage 2 geo enrichment: %d/%d rows needed lookup; %d complete, "
        "%d partial, %d unresolved geographic triplets.",
        len(to_enrich_idx), len(rows), complete_count, partial_count, unresolved_count,
    )
    return rows

