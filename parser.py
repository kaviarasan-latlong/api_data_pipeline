"""
parser.py  -  STAGE 1: EXTRACT

For each joined (request + response) row:

  1. Parse lat/long out of BOTH the request path (query string) and the
     response `data` JSON, generically - by key-name pattern, not by a
     hardcoded per-api_type mapping - so new APIs work without code changes
     as long as they use conventional key names (lat/latitude, lng/long/
     longitude). config.yaml's `latlong_keys` section is used only to seed
     extra key-name variants.

  2. Decide keep/drop:
       - If the response is an error/failure: we still keep the row IF the
         request path itself carried lat/long (some APIs put lat/long in
         the request even when the call ultimately failed). If no lat/long
         is recoverable at all, the row is dropped and counted separately
         from ordinary no-latlong drops.
       - If the response is NOT an error: the row is kept only if lat/long
         was found (request or response). If both sides have it,
         **response wins on conflict**.

  3. Opportunistically also pulls state/district/pincode/address if the
     source already provides them, so Stage 2 (geo_enrichment.py) can skip
     the spatial lookup for whatever's already present.

Returns kept rows plus a counts dict: dropped_error, dropped_no_latlong, kept.
"""

import json
import logging
import re
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)

LAT_KEY_RE = re.compile(r"^(lat|latitude)$", re.IGNORECASE)
LNG_KEY_RE = re.compile(r"^(lng|lon|long|longitude)$", re.IGNORECASE)

FIELD_KEY_RE = {
    "pincode": re.compile(r"^(pincode|pin_code|postal_code|zip|zipcode)$", re.IGNORECASE),
    "district": re.compile(r"^(district)$", re.IGNORECASE),
    "state": re.compile(r"^(state)$", re.IGNORECASE),
    "address": re.compile(r"^(address|formatted_address|display_name)$", re.IGNORECASE),
}

FAILURE_VALUES = {"failure", "failed", "error", "err"}


def _as_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _find_latlong_in_obj(obj):
    """Recursively search a dict/list for a dict level holding both a
    lat-ish and lng-ish key (e.g. {"lat":.., "lng":..} or nested under
    geometry.location, result, etc.). Returns (lat, lng) floats or None."""
    if isinstance(obj, dict):
        lat = lng = None
        for k, v in obj.items():
            if isinstance(v, (int, float, str)):
                if LAT_KEY_RE.match(str(k)):
                    lat = _as_float(v)
                elif LNG_KEY_RE.match(str(k)):
                    lng = _as_float(v)
        if lat is not None and lng is not None:
            return (lat, lng)
        for v in obj.values():
            found = _find_latlong_in_obj(v)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_latlong_in_obj(item)
            if found:
                return found
    return None


def _find_fields_in_obj(obj, found=None):
    """Recursively pull any of pincode/district/state/address that are
    directly present in the payload, by key-name pattern."""
    if found is None:
        found = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (str, int)):
                for field, pattern in FIELD_KEY_RE.items():
                    if field not in found and pattern.match(str(k)):
                        found[field] = str(v)
            elif isinstance(v, (dict, list)):
                _find_fields_in_obj(v, found)
    elif isinstance(obj, list):
        for item in obj:
            _find_fields_in_obj(item, found)
    return found


def _extract_from_path(path: str):
    if not path:
        return None, {}
    try:
        parsed = urlparse(path)
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    except Exception:
        return None, {}

    lat = lng = None
    for k, v in params.items():
        if LAT_KEY_RE.match(k):
            lat = _as_float(v)
        elif LNG_KEY_RE.match(k):
            lng = _as_float(v)
    latlong = (lat, lng) if lat is not None and lng is not None else None
    fields = _find_fields_in_obj(params)
    return latlong, fields


def _extract_from_response(data):
    if data is None:
        return None, {}
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return None, {}
    latlong = _find_latlong_in_obj(data)
    fields = _find_fields_in_obj(data)
    return latlong, fields


def _is_error_response(row):
    resp_status = str(row.get("response_status") or "").strip().lower()
    if resp_status in FAILURE_VALUES:
        return True
    data = row.get("response_data")
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            data = None
    if isinstance(data, dict):
        inner_status = str(data.get("status") or "").strip().lower()
        if inner_status in FAILURE_VALUES:
            return True
    return False


def parse_rows(rows: list[dict]):
    """Returns (kept_rows, counts) where counts = {dropped_error, dropped_no_latlong, kept}."""
    counts = {"dropped_error": 0, "dropped_no_latlong": 0, "kept": 0}
    kept = []

    for row in rows:
        request_latlong, request_fields = _extract_from_path(row.get("request_path"))
        is_error = _is_error_response(row)

        if is_error:
            response_latlong, response_fields = None, {}
        else:
            response_latlong, response_fields = _extract_from_response(row.get("response_data"))

        # response-wins-on-conflict
        final_latlong = response_latlong or request_latlong

        if final_latlong is None:
            if is_error:
                counts["dropped_error"] += 1
            else:
                counts["dropped_no_latlong"] += 1
            continue

        merged_fields = {**request_fields, **response_fields}  # response wins here too

        kept.append({
            **row,
            "lat": final_latlong[0],
            "lng": final_latlong[1],
            "pincode": merged_fields.get("pincode"),
            "district": merged_fields.get("district"),
            "state": merged_fields.get("state"),
            "address": merged_fields.get("address"),
            "was_error_response": is_error,
        })
        counts["kept"] += 1

    logger.info("Stage 1 parse: %s", counts)
    return kept, counts
