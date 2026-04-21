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

CONNECTORS = ["gmail", "salesforce", "snowflake"]

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
        _LABELS = {
            "gmail": "Gmail (Gumstack)",
            "salesforce": "Salesforce (Growth MCP)",
            "snowflake": "Snowflake",
        }
        _REAUTH_HINTS = {
            "gmail": "Re-auth at <https://www.gumloop.com/personal/apps|gumloop.com/personal/apps>",
            "salesforce": "Re-auth Growth MCP in Glass → Connections panel",
        }
        lines = []
        for name in CONNECTORS:
            entry = self._state.get(name, _DEFAULT_ENTRY)
            status = entry["status"]
            emoji = _STATUS_EMOJI.get(status, "\u2753")
            label = _LABELS.get(name, name.capitalize())
            status_text = status.upper()
            line = f"\u2022 {label}: {emoji} {status_text}"
            if entry["error"]:
                line += f" \u2014 {entry['error']}"
            if status == "expired" and name in _REAUTH_HINTS:
                line += f"\n   _\u2192 {_REAUTH_HINTS[name]}_"
            lines.append(line)
        return "\n".join(lines)

    # ── direct health checks ─────────────────────────────────────────────

    def check_gmail_health(self, user_id: str | None = None) -> bool:
        """Test Gmail connectivity via Gumstack MCP and update status.  Returns True if ok."""
        try:
            from core.gumstack_gmail import read_emails, is_available

            if not is_available(user_id=user_id):
                self.update_status("gmail", "expired", error="Gumstack Gmail tokens not found")
                return False

            read_emails("in:inbox", max_results=1, user_id=user_id)
            self.update_status("gmail", "ok")
            return True
        except Exception as exc:
            self.update_status("gmail", "expired", error=str(exc))
            logger.error("Gmail health check failed: %s", exc)
            return False

    def check_sf_health(self) -> bool:
        """Test Salesforce MCP connectivity and update status. Returns True if ok."""
        try:
            from core.salesforce_client import ensure_auth

            if ensure_auth():
                self.update_status("salesforce", "ok")
                return True
            else:
                self.update_status(
                    "salesforce", "expired",
                    error="Salesforce MCP auth expired — re-auth at gumloop.com/personal/apps",
                )
                return False
        except Exception as exc:
            self.update_status("salesforce", "expired", error=str(exc))
            logger.error("Salesforce health check failed: %s", exc)
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


# ── Inline failure alert ──────────────────────────────────────────────────
# Call this from any client when an operation fails due to auth.
# Sends an immediate DM with re-auth instructions (rate-limited to 1 per 10 min
# per connector to avoid spam during bulk failures).

_inline_alert_ts: dict[tuple[str, str], float] = {}
_INLINE_COOLDOWN = 600  # 10 minutes

_REAUTH_INSTRUCTIONS = {
    "salesforce": (
        ":red_circle: *Salesforce auth expired* — opp creation/updates will fail.\n"
        "_Re-auth Growth MCP in Glass \u2192 Connections panel_"
    ),
    "gmail": (
        ":red_circle: *Gmail auth expired* — email drafts will fail.\n"
        "_Re-auth at_ <https://www.gumloop.com/personal/apps|gumloop.com/personal/apps>"
    ),
}


# ── Auto re-auth for Gmail ──────────────────────────────────────────────────
# When refresh token is revoked, automatically spawn mcp-remote to get a new
# auth URL and DM the user a clickable link. The user just clicks, auths in
# browser, and the bot picks up fresh tokens from the mcp-remote cache.

_reauth_in_progress: dict[str, bool] = {}


def trigger_gmail_reauth(user_id: str) -> None:
    """Disabled — auto browser popup was too aggressive.

    Instead, the bot just DMs the user with re-auth instructions.
    """
    return  # Disabled: auto-reauth opens browser too frequently

    import subprocess  # noqa: E702
    import threading

    if _reauth_in_progress.get("gmail"):
        return  # already running

    def _run_reauth():
        _reauth_in_progress["gmail"] = True
        try:
            # Clear old tokens to force new auth
            import glob
            token_pattern = os.path.expanduser("~/.mcp-auth/mcp-remote-*/b33c17f2a3b8668ac4b9aff0f5daaffd_tokens.json")
            for f in glob.glob(token_pattern):
                try:
                    os.remove(f)
                except OSError:
                    pass

            # DM user that auto-reauth is starting
            try:
                from slack_sdk import WebClient
                from config import SLACK_BOT_TOKEN
                client = WebClient(token=SLACK_BOT_TOKEN)
                client.chat_postMessage(
                    channel=user_id,
                    text=(
                        ":key: *Auto re-auth starting* — a browser window should open for Gumloop Gmail auth.\n"
                        "If it doesn't, run in terminal: `npx mcp-remote https://mcp.gumloop.com/gmail/mcp`\n"
                        "Complete the auth and drafts will resume automatically."
                    ),
                )
            except Exception:
                pass

            # Spawn mcp-remote — it opens a browser for OAuth
            proc = subprocess.Popen(
                ["npx", "mcp-remote", "https://mcp.gumloop.com/gmail/mcp"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=120,
            )

            # Wait up to 2 minutes for auth to complete
            try:
                proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                proc.kill()

            # Check if new tokens were saved
            import glob as _glob
            new_tokens = sorted(
                _glob.glob(os.path.expanduser("~/.mcp-auth/mcp-remote-*/*_tokens.json")),
                key=os.path.getmtime, reverse=True,
            )
            if new_tokens:
                newest = new_tokens[0]
                age = time.time() - os.path.getmtime(newest)
                if age < 180:  # saved within last 3 minutes
                    # Sync to all bot token locations
                    import shutil
                    client_info = newest.replace("_tokens.json", "_client_info.json")
                    for dest_dir in [
                        os.path.expanduser("~/.gary_bot_tokens/U06DAFU4YRG"),
                        os.path.expanduser("~/.gary_bot_tokens/U05PY9BQUKZ"),
                    ]:
                        os.makedirs(dest_dir, exist_ok=True)
                        shutil.copy2(newest, os.path.join(dest_dir, "gmail_tokens.json"))
                        if os.path.exists(client_info):
                            shutil.copy2(client_info, os.path.join(dest_dir, "gmail_client_info.json"))

                    logger.info("Gmail auto-reauth: fresh tokens synced from %s", newest)
                    health.update_status("gmail", "ok")

                    try:
                        client.chat_postMessage(
                            channel=user_id,
                            text=":white_check_mark: *Gmail re-authed successfully* — drafts will resume.",
                        )
                    except Exception:
                        pass
                    return

            logger.warning("Gmail auto-reauth: no fresh tokens found after mcp-remote")
        except Exception as e:
            logger.error("Gmail auto-reauth failed: %s", e)
        finally:
            _reauth_in_progress["gmail"] = False

    threading.Thread(target=_run_reauth, daemon=True).start()


def alert_auth_failure(connector: str, user_id: str, error: str = "") -> None:
    """Send an immediate DM when an auth failure is detected inline.

    Rate-limited: one alert per connector per user per 10 minutes.
    """
    import time as _time
    now = _time.time()
    key = (user_id, connector)
    if now - _inline_alert_ts.get(key, 0) < _INLINE_COOLDOWN:
        return  # already alerted recently

    # Update health status
    health.update_status(connector, "expired", error=error)

    # Send DM
    try:
        from slack_sdk import WebClient
        from config import SLACK_BOT_TOKEN
        client = WebClient(token=SLACK_BOT_TOKEN)
        msg = _REAUTH_INSTRUCTIONS.get(connector, f":red_circle: *{connector} auth expired*")
        if error:
            msg += f"\n_Error: {error[:200]}_"
        client.chat_postMessage(channel=user_id, text=msg)
        _inline_alert_ts[key] = now
        logger.info("Inline auth alert sent to %s for %s", user_id, connector)
    except Exception as exc:
        logger.error("Failed to send inline auth alert: %s", exc)
