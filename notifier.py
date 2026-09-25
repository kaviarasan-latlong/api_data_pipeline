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


def _build_adaptive_card(run_summary: dict, message: str, style: str = "Attention"):
    pipeline_name = run_summary.get("pipeline_name", "api_data_pipeline")
    window_start = run_summary.get("window_start", "n/a")
    window_end = run_summary.get("window_end", "n/a")
    output_table = run_summary.get("output_table", "n/a")

    body = [
        {
            "type": "TextBlock",
            "text": "API v4,v5,v4.1 logs",
            "wrap": False,
            "size": "Medium",
            "weight": "Bolder",
        },
        {"type": "TextBlock", "text": "env: Prod", "wrap": True},
        {"type": "TextBlock", "text": message, "wrap": True},
        {
            "type": "TextBlock",
            "text": f"Pipeline: {pipeline_name} | Window: {window_start} -> {window_end} | Output table: {output_table}",
            "wrap": True,
        },
    ]

    if "failure_message" in run_summary:
        body.append({"type": "TextBlock", "text": f"Failure: {run_summary.get('failure_message')}", "wrap": True})

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "Container",
                            "wrap": False,
                            "items": body,
                            "style": style,
                        }
                    ],
                    "msteams": {"width": "Full"},
                },
            }
        ],
    }


def send_notification(cfg, run_summary: dict):
    webhook_url = cfg["notifier"]["teams_webhook_url"]
    message = (
        f"Pipeline: {run_summary.get('pipeline_name', 'api_data_pipeline')} | "
        f"Window: {run_summary.get('window_start', 'n/a')} -> {run_summary.get('window_end', 'n/a')} | "
        f"Output table: {run_summary.get('output_table', 'n/a')}"
    )

    if "failure_message" in run_summary:
        payload = _build_adaptive_card(run_summary, message + " | failure", style="Attention")
    else:
        payload = _build_adaptive_card(
            run_summary,
            message,
            style="Attention",
        )

    try:
        resp = requests.post(webhook_url, json=payload, timeout=15)
        if resp.status_code >= 300:
            logger.error("Teams notification failed: %s %s", resp.status_code, resp.text)
            resp.raise_for_status()
    except Exception:
        logger.exception("Teams notification failed")
        raise

    logger.info("Teams notification sent for window %s -> %s", run_summary.get("window_start"), run_summary.get("window_end"))
