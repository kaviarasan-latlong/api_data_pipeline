"""
notifier.py

Loads the Teams message template (path from config.yaml -> notifier.template_path),
fills in `{{placeholder}}` tokens with values from the run summary dict, and
POSTs it to the configured webhook URL.

Swap in your real template by pointing notifier.template_path at it - the
placeholder names below are the ones populated from build_run_summary() in
metrics.py. Add more keys to run_summary and reference them the same way if
your template needs more detail.
"""

import json
import logging
import re

import requests

logger = logging.getLogger(__name__)

_PLACEHOLDER_RE = re.compile(r"\{\{(\w+)\}\}")


def _render_template(template_str: str, values: dict) -> str:
    def _sub(match):
        key = match.group(1)
        return str(values.get(key, ""))
    return _PLACEHOLDER_RE.sub(_sub, template_str)


def send_notification(cfg, run_summary: dict):
    notifier_cfg = cfg["notifier"]
    webhook_url = notifier_cfg["teams_webhook_url"]
    template_path = notifier_cfg["template_path"]

    with open(template_path, "r") as f:
        raw_template = f.read()

    rendered = _render_template(raw_template, run_summary)

    try:
        payload = json.loads(rendered)
    except json.JSONDecodeError:
        logger.error("Rendered notifier template is not valid JSON - check %s", template_path)
        raise

    resp = requests.post(webhook_url, json=payload, timeout=15)
    if resp.status_code >= 300:
        logger.error("Teams notification failed: %s %s", resp.status_code, resp.text)
        resp.raise_for_status()

    logger.info("Teams notification sent for window %s -> %s", run_summary.get("window_start"), run_summary.get("window_end"))
