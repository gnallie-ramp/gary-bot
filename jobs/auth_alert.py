"""Scheduled job: auth health check and DM alert for expired connectors.

Runs direct health checks for Gmail IMAP and Snowflake, reads MCP connector
statuses from the state file, and DMs Greg if anything is expired.  Alerts
are rate-limited to avoid spamming (once per connector per 2 hours).
"""
from __future__ import annotations

import time
import logging

from config import GREG_SLACK_ID
from utils.auth_health import health

logger = logging.getLogger(__name__)

# Don't re-alert within this window (seconds)
_ALERT_COOLDOWN = 2 * 3600  # 2 hours

# {connector: last_alerted_timestamp}
_last_alerted: dict[str, float] = {}


def run_auth_alert(client) -> None:
    """Run health checks and DM Greg about any expired connectors.

    Parameters
    ----------
    client : slack_sdk.web.WebClient
        Slack WebClient for sending DMs.
    """
    # ── Run direct checks ─────────────────────────────────────────────
    logger.info("Running auth health checks...")
    health.check_gmail_health()
    health.check_snowflake_health()

    # ── Check for expired connectors ──────────────────────────────────
    expired = health.get_expired_connectors()

    if not expired:
        logger.info("All connectors healthy.")
        return

    # Filter out recently-alerted connectors
    now = time.time()
    needs_alert = [
        c for c in expired
        if now - _last_alerted.get(c, 0) > _ALERT_COOLDOWN
    ]

    if not needs_alert:
        logger.info(
            "Expired connectors %s already alerted within cooldown window.",
            expired,
        )
        return

    # ── Build and send the DM ─────────────────────────────────────────
    status_body = health.format_status_dm()
    message = f"\U0001f534 *Auth Alert*\n{status_body}"

    try:
        client.chat_postMessage(
            channel=GREG_SLACK_ID,
            text=message,
        )
        # Mark all alerted connectors
        for c in needs_alert:
            _last_alerted[c] = now
        logger.info("Auth alert sent to Greg for: %s", needs_alert)
    except Exception as exc:
        logger.error("Failed to send auth alert DM: %s", exc)
