"""Auth health check for MCP connectors and direct integrations.

Tracks the health status of each external connector (Salesforce, Gmail,
Gong, Granola, Snowflake) in a persistent JSON file.  MCP connector
statuses are updated externally by the pipeline; Gmail IMAP and Snowflake
can be checked directly.
"""
from __future__ import annotations

import json
import os
import time
import logging
import threading

logger = logging.getLogger(__name__)

AUTH_STATUS_FILE = os.path.expanduser("~/.gary_bot_auth_status.json")

CONNECTORS = ["gmail", "snowflake"]

_DEFAULT_ENTRY = {
    "status": "unknown",
    "last_check": 0,
    "last_ok": 0,
    "error": None,
}

_STATUS_EMOJI = {
    "ok": "\u2705",       # white check mark
    "expired": "\U0001f534",  # red circle
    "unknown": "\u2753",  # question mark
}


class AuthHealth:
    """Thread-safe auth health tracker backed by a JSON file."""

    def __init__(self):
        self._lock = threading.Lock()
        self._state: dict[str, dict] = self._load()

    # ── public API ────────────────────────────────────────────────────────

    def update_status(
        self, connector: str, status: str, error: str | None = None
    ) -> bool:
        """Update a connector's status.

        Returns True if transitioning from expired -> ok (so caller can
        trigger a queue drain).
        """
        with self._lock:
            entry = self._state.setdefault(connector, dict(_DEFAULT_ENTRY))
            was_expired = entry["status"] == "expired"
            entry["status"] = status
            entry["last_check"] = time.time()
            entry["error"] = error
            if status == "ok":
                entry["last_ok"] = time.time()
            self._save()
            recovered = was_expired and status == "ok"
            if recovered:
                logger.info("Connector %s recovered from expired -> ok.", connector)
            return recovered

    def get_status(self, connector: str) -> dict:
        """Return the status dict for a connector."""
        with self._lock:
            return dict(self._state.get(connector, _DEFAULT_ENTRY))

    def get_all_statuses(self) -> dict[str, dict]:
        """Return a copy of all connector statuses."""
        with self._lock:
            return {k: dict(v) for k, v in self._state.items()}

    def is_healthy(self, connector: str) -> bool:
        """Return True if connector status is 'ok'."""
        with self._lock:
            entry = self._state.get(connector, _DEFAULT_ENTRY)
            return entry["status"] == "ok"

    def get_expired_connectors(self) -> list[str]:
        """Return list of connector names with 'expired' status."""
        with self._lock:
            return [
                name for name, entry in self._state.items()
                if entry["status"] == "expired"
            ]

    def format_status_dm(self) -> str:
        """Return a Slack-formatted string showing all connector statuses."""
        lines = []
        for name in CONNECTORS:
            entry = self._state.get(name, _DEFAULT_ENTRY)
            status = entry["status"]
            emoji = _STATUS_EMOJI.get(status, "\u2753")
            label = name.capitalize()
            if name == "gmail":
                label = "Gmail (IMAP)"
            status_text = status.upper()
            if entry["error"]:
                lines.append(f"\u2022 {label}: {emoji} {status_text} \u2014 {entry['error']}")
            else:
                lines.append(f"\u2022 {label}: {emoji} {status_text}")
        return "\n".join(lines)

    # ── direct health checks ─────────────────────────────────────────────

    def check_gmail_health(self) -> bool:
        """Test Gmail IMAP connectivity and update status.  Returns True if ok."""
        try:
            from core.gmail_client import check_imap_connection

            ok, msg = check_imap_connection()
            if ok:
                self.update_status("gmail", "ok")
            else:
                self.update_status("gmail", "expired", error=msg)
            return ok
        except Exception as exc:
            self.update_status("gmail", "expired", error=str(exc))
            logger.error("Gmail health check failed: %s", exc)
            return False

    def check_snowflake_health(self) -> bool:
        """Test Snowflake connectivity and update status.  Returns True if ok."""
        try:
            from core.snowflake_client import check_connection

            check_connection()
            self.update_status("snowflake", "ok")
            return True
        except Exception as exc:
            self.update_status("snowflake", "expired", error=str(exc))
            logger.error("Snowflake health check failed: %s", exc)
            return False

    # ── persistence helpers ───────────────────────────────────────────────

    def _load(self) -> dict[str, dict]:
        """Read state from the JSON file.  Returns defaults on failure."""
        state = {name: dict(_DEFAULT_ENTRY) for name in CONNECTORS}
        if not os.path.exists(AUTH_STATUS_FILE):
            return state
        try:
            with open(AUTH_STATUS_FILE, "r") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                for name in CONNECTORS:
                    if name in data and isinstance(data[name], dict):
                        state[name] = data[name]
                return state
            logger.warning("Auth status file has unexpected type; using defaults.")
            return state
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load auth status (%s); using defaults.", exc)
            return state

    def _save(self) -> None:
        """Write current state to the JSON file."""
        try:
            with open(AUTH_STATUS_FILE, "w") as fh:
                json.dump(self._state, fh)
        except OSError as exc:
            logger.error("Failed to save auth status: %s", exc)


# ── module-level singleton ────────────────────────────────────────────────
health = AuthHealth()
