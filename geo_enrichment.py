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
# Resolve district/state directly by pincode value (no spatial lookup needed)
# ---------------------------------------------------------------------------

def _resolve_hierarchy_by_pincode_bulk(conn, cfg, pincodes: list):
    """
    For rows that already have a pincode (from API response) but are missing
    district/state, look up admin_area directly by display_name matching the
    pincode value and walk the parent chain.

    This avoids a spatial lat/lng lookup which may match a DIFFERENT polygon
    than the pincode that the API already returned.

    Returns {pincode_str: {"pincode": ..., "district": ..., "state": ...}}
    """
    if not pincodes:
        return {}

    area_cols = cfg["geo_enrichment"]["area_table_columns"]
    parent_col = area_cols.get("parent_id", "aa_in_aa_id")
    display_col = area_cols.get("display_name", "display_name")
    a_id_col = area_cols.get("a_id", "id")
    area_table = cfg["tables"]["area_table"]
    geom_table = cfg["tables"]["geom_table"]

    schema_info = _table_has_columns(conn, area_table, ["aa_order", "to_date"])
    has_aa_order = schema_info.get("aa_order", False)
    has_to_date = schema_info.get("to_date", False)
    geom_schema = _table_has_columns(conn, geom_table, ["to_date"])

    aa_order_select = ", a.aa_order" if has_aa_order else ", NULL::int AS aa_order"
    area_to_date_filter = "AND a.to_date IS NULL" if has_to_date else ""
    geom_to_date_filter = "AND g.to_date IS NULL" if geom_schema.get("to_date", False) else ""

    unique_pincodes = list(set(pincodes))

    # Build patterns like '^\\s*210205(\\s*[-–]|$)' for each pincode
    sql = f"""
        WITH RECURSIVE start_nodes AS (
            SELECT DISTINCT ON (matched_pincode)
                matched_pincode,
                a.{a_id_col} AS start_id,
                a.{display_col} AS display_name,
                a.{parent_col} AS parent_id
                {aa_order_select}
            FROM (
                SELECT UNNEST(%s::text[]) AS matched_pincode
            ) pincodes
            JOIN {area_table} a
                ON a.{display_col} ~ ('^\\s*' || pincodes.matched_pincode || '(\\s*[-–]|$)')
            JOIN {geom_table} g ON g.aa_id = a.{a_id_col} {geom_to_date_filter}
            WHERE True
            {area_to_date_filter}
            ORDER BY matched_pincode, a.{a_id_col}
        ),
        chain AS (
            SELECT
                s.matched_pincode AS pincode_key,
                s.start_id,
                s.start_id AS current_id,
                s.display_name,
                s.parent_id
                {aa_order_select.replace('a.aa_order', 's.aa_order').replace(', NULL::int AS aa_order', ', NULL::int AS aa_order')},
                1 AS depth
            FROM start_nodes s

            UNION ALL

            SELECT
                c.pincode_key,
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
              {area_to_date_filter}
        )
        SELECT pincode_key, current_id, display_name, parent_id, aa_order, depth
        FROM chain
        ORDER BY pincode_key, depth
    """

    with conn.cursor() as cur:
        cur.execute(sql, (unique_pincodes,))
        rows = cur.fetchall()

    # Group by pincode_key
    chains: dict[str, list] = {}
    for row in rows:
        pk = row[0]
        if pk not in chains:
            chains[pk] = []
        chains[pk].append(row)

    results = {}
    for pincode_key, chain_rows in chains.items():
        pincode = pincode_key
        district = None
        state = None

        for row in chain_rows:
            # row: (pincode_key, current_id, display_name, parent_id, aa_order, depth)
            name = str(row[2]).strip() if row[2] else ""
            area_order = row[4]

            if area_order == 55:
                continue  # This is the pincode node itself, skip
            if area_order == 8 and district is None:
                district = _clean_area_name(name)
                continue
            if area_order == 9 and state is None:
                state = _clean_area_name(name)
                break
            if district is None and name and area_order is None:
                district = _clean_area_name(name)
                continue
            if district and state is None and area_order is None:
                state = _clean_area_name(name)
                break

        results[pincode_key] = {
            "pincode": pincode,
            "district": district,
            "state": state,
        }

    return results


# Module-level cache: { area_id: {"pincode": ..., "district": ..., "state": ...} }
_hierarchy_cache: dict[int, dict] = {}


def _resolve_admin_hierarchy_bulk(conn, cfg, area_ids: list):
    """
    Resolve pincode/district/state for MULTIPLE area_ids in a single
    recursive CTE query, replacing the old per-row iterative approach.
    Uses in-memory cache so repeated area_ids across chunks hit RAM directly.

    Returns {area_id: {"pincode": ..., "district": ..., "state": ...}}
    """
    if not area_ids:
        return {}

    # Check cache first
    cached_results = {}
    missing_ids = set()
    for aid in area_ids:
        if aid in _hierarchy_cache:
            cached_results[aid] = _hierarchy_cache[aid]
        else:
            missing_ids.add(aid)

    if not missing_ids:
        return cached_results

    area_cols = cfg["geo_enrichment"]["area_table_columns"]
    parent_col = area_cols.get("parent_id", "aa_in_aa_id")
    display_col = area_cols.get("display_name", "display_name")
    a_id_col = area_cols.get("a_id", "id")
    area_table = cfg["tables"]["area_table"]
    schema_info = _table_has_columns(conn, area_table, ["aa_order", "to_date"])
    has_aa_order = schema_info.get("aa_order", False)
    has_to_date = schema_info.get("to_date", False)

    unique_ids = list(missing_ids)

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
              AND a.aa_order = 55
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
                pincode = parsed or _parse_pincode(name) or (re.search(r"\b(\d{6})\b", name).group(1) if re.search(r"\b(\d{6})\b", name) else None)
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
            res = {
                "pincode": pincode,
                "district": district,
                "state": state,
            }
        else:
            res = {"pincode": None, "district": None, "state": None}

        results[start_id] = res
        _hierarchy_cache[start_id] = res

    cached_results.update(results)
    return cached_results


# ---------------------------------------------------------------------------
# Batch spatial lookup + hierarchy resolution
# ---------------------------------------------------------------------------

def _enrich_batch(conn, cfg, batch: list[dict], fallback_distance: float = 0.0):
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

    # Cached schema checks — each of these hits the DB at most once
    geom_schema = _table_has_columns(conn, geom_table, ["to_date", "aa_order"])
    area_schema = _table_has_columns(conn, area_table, ["to_date"])

    # Deduplicate coordinates in Python to avoid running heavy ST_Intersects on duplicate points
    coord_to_indices = {}
    for i, r in enumerate(batch):
        coord_key = (round(float(r["lat"]), 6), round(float(r["lng"]), 6))
        if coord_key not in coord_to_indices:
            coord_to_indices[coord_key] = []
        coord_to_indices[coord_key].append(i)

    # Unique points for SQL execution
    unique_points = list(coord_to_indices.keys())
    idxs = list(range(len(unique_points)))
    lats = [pt[0] for pt in unique_points]
    lngs = [pt[1] for pt in unique_points]

    geom_where = []
    if geom_schema.get("to_date", False):
        geom_where.append("g.to_date IS NULL")
    if geom_schema.get("aa_order", False):
        geom_where.append("g.aa_order = 55")
    geom_filter_sql = " AND ".join(geom_where) if geom_where else "g.to_date IS NULL"
    area_filter_sql = "AND a.to_date IS NULL" if area_schema.get("to_date", False) else ""

    if fallback_distance and fallback_distance > 0:
        fallback_distance_sql = f"{float(fallback_distance):.3f}"
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
                       WHEN nearby_match.aa_id IS NOT NULL THEN 'buffer'
                       ELSE NULL
                   END AS match_type
            FROM pts
            LEFT JOIN LATERAL (
                SELECT g.{geom_a_id_col} AS aa_id
                FROM {geom_table} g
                JOIN {area_table} sa 
                    ON sa.{area_id_col} = g.{geom_a_id_col} 
                   AND sa.aa_order = 55
                WHERE g.{geom_col} && pts.geom
                  AND ST_Contains(g.{geom_col}, pts.geom)
                  AND {geom_filter_sql}
                LIMIT 1
            ) exact_match ON TRUE
            LEFT JOIN LATERAL (
                SELECT g.{geom_a_id_col} AS aa_id
                FROM {geom_table} g
                JOIN {area_table} sa 
                    ON sa.{area_id_col} = g.{geom_a_id_col} 
                   AND sa.aa_order = 55
                WHERE exact_match.aa_id IS NULL
                  AND {geom_filter_sql}
                  AND g.{geom_col} && ST_Buffer(pts.geom::geography, {fallback_distance_sql})::geometry
                  AND ST_Intersects(
                      g.{geom_col},
                      ST_Buffer(pts.geom::geography, {fallback_distance_sql})::geometry
                  )
                ORDER BY ST_Distance(g.{geom_col}::geography, pts.geom::geography)
                LIMIT 1
            ) nearby_match ON TRUE
            ORDER BY pts.idx
        """
    else:
        sql = f"""
            WITH pts AS (
                SELECT idx, lat, lng,
                       ST_SetSRID(ST_MakePoint(lng, lat), 4326) AS geom
                FROM UNNEST(%s::int[], %s::float8[], %s::float8[])
                         AS t(idx, lat, lng)
            )
            SELECT pts.idx,
                   exact_match.aa_id AS geom_aa_id,
                   CASE
                       WHEN exact_match.aa_id IS NOT NULL THEN 'exact'
                       ELSE NULL
                   END AS match_type
            FROM pts
            LEFT JOIN LATERAL (
                SELECT g.{geom_a_id_col} AS aa_id
                FROM {geom_table} g
                JOIN {area_table} sa 
                    ON sa.{area_id_col} = g.{geom_a_id_col} 
                   AND sa.aa_order = 55
                WHERE g.{geom_col} && pts.geom
                  AND ST_Contains(g.{geom_col}, pts.geom)
                  AND {geom_filter_sql}
                LIMIT 1
            ) exact_match ON TRUE
            ORDER BY pts.idx
        """
    with conn.cursor() as cur:
        cur.execute(sql, (idxs, lats, lngs))
        results = cur.fetchall()

    # Map results back to ALL original row indices (handling duplicates)
    by_idx = {}
    fallback_matches = 0
    for row in results:
        pt_idx = row[0]
        matched_aa_id = row[1]
        is_buffer = (row[2] == "buffer")
        
        orig_indices = coord_to_indices.get(unique_points[pt_idx], [])
        for orig_idx in orig_indices:
            if matched_aa_id is not None:
                by_idx[orig_idx] = matched_aa_id
            if is_buffer:
                fallback_matches += 1

    if fallback_matches:
        logger.info(
            "Geo lookup used the nearest pincode area within %.0f m buffer for %d/%d coordinates.",
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


def enrich_rows(conn, cfg, rows: list[dict], fallback_distance: float = 0.0):
    for row in rows:
        _normalize_source_geo_fields(row)

    # Process all rows that have valid lat/lng coordinates
    to_enrich_idx = [
        i for i, r in enumerate(rows)
        if r.get("lat") is not None and r.get("lng") is not None
    ]
    if not to_enrich_idx:
        return rows

    fallback_distance = float(fallback_distance or 0.0)

    batch_size = cfg["geo_enrichment"].get("batch_size", 5000)

    # Always use lat/long to lookup and update pincode, district, and state
    for start in range(0, len(to_enrich_idx), batch_size):
        idx_slice = to_enrich_idx[start:start + batch_size]
        batch = [rows[i] for i in idx_slice]
        results = _enrich_batch(conn, cfg, batch, fallback_distance=fallback_distance)

        for local_idx, global_idx in enumerate(idx_slice):
            geo = results.get(local_idx)
            if not geo:
                continue
            row = rows[global_idx]
            # Always update/overwrite with what we find from latlong spatial lookup
            if geo.get("pincode"):
                row["pincode"] = geo["pincode"]
            if geo.get("district"):
                row["district"] = geo["district"]
            if geo.get("state"):
                row["state"] = geo["state"]

    complete_count = sum(
        bool(row.get("pincode") and row.get("district") and row.get("state"))
        for row in rows
    )
    partial_count = sum(
        bool(row.get("pincode") or row.get("district") or row.get("state"))
        and not (row.get("pincode") and row.get("district") and row.get("state"))
        for row in rows
    )
    unresolved_rows = [
        row for row in rows
        if not (row.get("pincode") or row.get("district") or row.get("state"))
    ]
    unresolved_count = len(unresolved_rows)

    if unresolved_rows:
        error_dir = cfg.get("output", {}).get("report_dir")
        if error_dir:
            try:
                import os, csv
                os.makedirs(error_dir, exist_ok=True)
                unresolved_file = os.path.join(error_dir, "unresolved_geo_rows.csv")
                file_exists = os.path.isfile(unresolved_file)
                with open(unresolved_file, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(["requestid", "api_type", "lat", "lng", "created_at"])
                    for ur in unresolved_rows:
                        writer.writerow([
                            ur.get("requestid"),
                            ur.get("api_type"),
                            ur.get("lat"),
                            ur.get("lng"),
                            ur.get("created_at"),
                        ])
            except Exception as e:
                logger.warning("Could not write unresolved rows to file: %s", e)

    logger.info(
        "Stage 2 geo enrichment: %d/%d rows processed via latlong; %d complete, "
        "%d partial, %d unresolved geographic triplets.",
        len(to_enrich_idx), len(rows), complete_count, partial_count, unresolved_count,
    )
    return rows

