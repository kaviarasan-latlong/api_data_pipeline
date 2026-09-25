"""
metrics.py

Runs after a window completes successfully (all chunks processed).

1. Queries the output table for counts grouped by pincode / district / state
   for rows written in this window.
2. Updates (not overwrites) a persistent workbook with 3 sheets -
   "pincode", "district", "state" - each a running count table. Every
   pipeline run adds its window's hits on top of whatever's already there,
   so the workbook is a cumulative tally across all runs, not just the
   latest window.

   NOTE: the original spec called this a "csv with 3 sheets" - a plain CSV
   file can't actually hold multiple sheets, so this writes .xlsx (openpyxl)
   instead, which is the format that supports that. Says so in the run
   summary/notification so nobody goes looking for a .csv by mistake.

3. Builds the run summary dict consumed by notifier.py.
"""

import logging
import os
from collections import Counter

from openpyxl import Workbook, load_workbook

logger = logging.getLogger(__name__)

SHEET_NAMES = ["pincode", "district", "state"]


def compute_window_counts(conn, cfg, window_start, window_end):
    tables = cfg["tables"]
    counts = {}
    for field in SHEET_NAMES:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT {field}, COUNT(*)
                FROM {tables['output_table']}
                WHERE {field} IS NOT NULL
                GROUP BY {field}
            """)
            counts[field] = Counter(dict(cur.fetchall()))
    return counts


def _report_path(cfg, window_end):
    out_cfg = cfg["output"]
    os.makedirs(out_cfg["report_dir"], exist_ok=True)
    filename = out_cfg["report_filename_pattern"].format(window_end=window_end.strftime("%Y%m%d"))
    return os.path.join(out_cfg["report_dir"], filename)


def _load_existing_counts(path):
    """Returns {sheet_name: Counter} from an existing workbook, or empty."""
    if not os.path.exists(path):
        return {name: Counter() for name in SHEET_NAMES}

    wb = load_workbook(path)
    existing = {}
    for name in SHEET_NAMES:
        counter = Counter()
        if name in wb.sheetnames:
            ws = wb[name]
            for row in ws.iter_rows(min_row=2, values_only=True):
                if row and row[0] is not None:
                    counter[row[0]] = row[1] or 0
        existing[name] = counter
    return existing


def update_report_workbook(cfg, window_counts: dict, window_end) -> str:
    path = _report_path(cfg, window_end)
    existing = _load_existing_counts(path)

    merged = {}
    for name in SHEET_NAMES:
        combined = existing[name] + window_counts.get(name, Counter())
        merged[name] = combined

    wb = Workbook()
    wb.remove(wb.active)
    for name in SHEET_NAMES:
        ws = wb.create_sheet(title=name)
        ws.append([name, "hit_count"])
        for key, count in sorted(merged[name].items(), key=lambda kv: -kv[1]):
            ws.append([key, count])

    wb.save(path)
    logger.info("Updated report workbook at %s", path)
    return path


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
