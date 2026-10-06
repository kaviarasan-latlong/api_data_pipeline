"""
metrics.py

Runs after a window completes successfully (all chunks processed).

1. Queries month-scoped grouped output counts once, then partitions them into
    the full report and the configured business-unit/tenant reports.
2. Rebuilds four workbooks with pincode, district, and state sheets for every
    month touched by a successful processing window.

3. Builds the run summary dict consumed by notifier.py.
"""

import logging
import os
import shutil
from collections import Counter
from datetime import timedelta
from pathlib import Path

from openpyxl import Workbook

logger = logging.getLogger(__name__)

SHEET_NAMES = ("pincode", "district", "state")
INVALID_GEO_SQL = "('yes', 'no', 'true', 'false', 'null', 'none', 'n/a', 'na')"
PINCODE_SQL = r"substring(pincode from '^\s*(\d{6})')"


def _month_start(value):
    return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(value):
    if value.month == 12:
        return value.replace(year=value.year + 1, month=1, day=1)
    return value.replace(month=value.month + 1, day=1)


def _months_in_window(window_start, window_end):
    month_start = _month_start(window_start)
    while month_start < window_end:
        month_end = _next_month(month_start)
        yield month_start, month_end
        month_start = month_end


def _empty_group_counts(group_names):
    return {
        name: {sheet: Counter() for sheet in SHEET_NAMES}
        for name in group_names
    }


def _compute_month_counts(conn, cfg, month_start, month_end):
    output_table = cfg["tables"]["output_table"]
    groups = cfg["output"].get("report_groups", {})
    report_names = ["latlong", *groups.keys()]
    counts = _empty_group_counts(report_names)
    valid_filter = f"""
        {PINCODE_SQL} IS NOT NULL
        AND district IS NOT NULL AND trim(district::text) <> ''
        AND lower(trim(district::text)) NOT IN {INVALID_GEO_SQL}
        AND state IS NOT NULL AND trim(state::text) <> ''
        AND lower(trim(state::text)) NOT IN {INVALID_GEO_SQL}
    """
    sql = f"""
        SELECT {PINCODE_SQL} AS pincode, district, state, bunit_id, tenant_id,
               COUNT(*)
        FROM {output_table}
        WHERE created_at >= %s AND created_at < %s
          AND {valid_filter}
        GROUP BY 1, 2, 3, 4, 5
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (month_start, month_end))
        grouped_rows = cursor.fetchall()

    configured_groups = {
        name: {
            "bunit_ids": {str(value) for value in definition.get("bunit_ids", [])},
            "tenant_ids": {str(value) for value in definition.get("tenant_ids", [])},
        }
        for name, definition in groups.items()
    }

    for pincode, district, state, bunit_id, tenant_id, row_count in grouped_rows:
        matching_groups = ["latlong"]
        bunit_value = str(bunit_id) if bunit_id is not None else None
        tenant_value = str(tenant_id) if tenant_id is not None else None
        for name, definition in configured_groups.items():
            if (bunit_value in definition["bunit_ids"]
                    or tenant_value in definition["tenant_ids"]):
                matching_groups.append(name)

        for name in matching_groups:
            counts[name]["pincode"][(pincode, district, state)] += row_count
            counts[name]["district"][(district, state)] += row_count
            counts[name]["state"][state] += row_count

    return counts


def _workbook_rows(sheet_name, counts):
    if sheet_name == "pincode":
        return ["pincode", "district", "state", "hit_count"], counts.items()
    if sheet_name == "district":
        return ["district", "state", "hit_count"], counts.items()
    return ["state", "hit_count"], counts.items()


def _save_workbook(path, report_counts):
    workbook = Workbook()
    workbook.remove(workbook.active)
    for sheet_name in SHEET_NAMES:
        worksheet = workbook.create_sheet(title=sheet_name)
        headers, entries = _workbook_rows(sheet_name, report_counts[sheet_name])
        worksheet.append(headers)
        sorted_entries = sorted(
            entries,
            key=lambda item: (-item[1], tuple(str(value) for value in item[0])
                              if isinstance(item[0], tuple) else str(item[0])),
        )
        for key, count in sorted_entries:
            worksheet.append([*(key if isinstance(key, tuple) else (key,)), count])

    temporary_path = path.with_name(f".{path.stem}.tmp.xlsx")
    try:
        workbook.save(temporary_path)
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _month_report_paths(cfg, month_start, report_names):
    output_cfg = cfg["output"]
    month = month_start.strftime("%B").lower()
    year = month_start.strftime("%Y")
    folder_name = output_cfg["monthly_report_folder_pattern"].format(
        month=month, year=year
    )
    folder = Path(output_cfg["report_dir"]) / folder_name
    folder.mkdir(parents=True, exist_ok=True)
    return {
        name: folder / output_cfg["monthly_report_filename_pattern"].format(
            report_type=name, month=month, year=year
        )
        for name in report_names
    }


def _remove_previous_month_folder(cfg, month_start, expected_paths):
    if not all(path.is_file() for path in expected_paths.values()):
        return
    previous_month = _month_start(month_start - timedelta(days=1))
    output_cfg = cfg["output"]
    previous_name = output_cfg["monthly_report_folder_pattern"].format(
        month=previous_month.strftime("%B").lower(),
        year=previous_month.strftime("%Y"),
    )
    previous_folder = Path(output_cfg["report_dir"]) / previous_name
    if previous_folder.exists() and previous_folder.is_dir():
        shutil.rmtree(previous_folder)
        logger.info("Removed previous monthly report folder %s", previous_folder)


def update_monthly_report_workbooks(conn, cfg, window_start, window_end):
    output_cfg = cfg["output"]
    report_names = ["latlong", *output_cfg.get("report_groups", {}).keys()]
    month_reports = {}

    for month_start, month_end in _months_in_window(window_start, window_end):
        counts = _compute_month_counts(conn, cfg, month_start, month_end)
        paths = _month_report_paths(cfg, month_start, report_names)
        for report_name, path in paths.items():
            _save_workbook(path, counts[report_name])
        month_key = month_start.strftime("%B_%Y").lower()
        month_reports[month_key] = {name: str(path) for name, path in paths.items()}

        if window_end >= month_end:
            _remove_previous_month_folder(cfg, month_start, paths)

    return month_reports


def build_run_summary(pipeline_name, window_start, window_end, parse_counts_total, report_path):
    return {
        "pipeline_name": pipeline_name,
        "window_start": str(window_start),
        "window_end": str(window_end),
        "kept": parse_counts_total.get("kept", 0),
        "dropped_error": parse_counts_total.get("dropped_error", 0),
        "dropped_no_latlong": parse_counts_total.get("dropped_no_latlong", 0),
        "report_path": report_path,
    }
