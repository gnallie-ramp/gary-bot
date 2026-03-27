"""User registry for multi-user Slack bot.

Stores registered users in a per-machine JSON file (~/.gary_bot_users.json)
and provides thread-safe lookup/registration functions.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Dict, Optional

from config import (
    BOOKING_LINK,
    GMAIL_ADDRESS,
    OWNER_FIRST_NAME,
    OWNER_NAME,
    OWNER_SLACK_ID,
)

# Per-user credential/token storage directory
_TOKENS_DIR = os.path.expanduser("~/.gary_bot_tokens")

_USERS_FILE = os.path.expanduser("~/.gary_bot_users.json")
_lock = threading.Lock()
_cache: Optional[Dict[str, dict]] = None


def _load() -> dict[str, dict]:
    """Load users from disk into the module cache. NOT thread-safe on its own."""
    global _cache
    if _cache is not None:
        return _cache

    if os.path.exists(_USERS_FILE):
        try:
            with open(_USERS_FILE) as f:
                data = json.load(f)
            if isinstance(data, dict) and data:
                _cache = data
                return _cache
        except (json.JSONDecodeError, OSError):
            pass

    # File missing, empty, or corrupt — auto-seed with the original owner
    _cache = {}
    if OWNER_SLACK_ID:
        _cache[OWNER_SLACK_ID] = {
            "sf_owner_name": OWNER_NAME,
            "first_name": OWNER_FIRST_NAME,
            "email": GMAIL_ADDRESS or "",
            "booking_link": BOOKING_LINK or "",
            "sfdc_user_id": "0056g000006oJIDAA2",
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }
        _save_no_lock()
    return _cache


def _save_no_lock() -> None:
    """Persist the cache to disk. Caller must already hold _lock (or be in _load)."""
    with open(_USERS_FILE, "w") as f:
        json.dump(_cache, f, indent=2)


# ── Public API ───────────────────────────────────────────────────────────────


def get_user(slack_id: str) -> dict | None:
    """Return the user profile dict for *slack_id*, or None if not registered."""
    with _lock:
        users = _load()
    return users.get(slack_id)


def is_registered(slack_id: str) -> bool:
    """Check whether *slack_id* has a profile in the registry."""
    return get_user(slack_id) is not None


def register_user(
    slack_id: str,
    sf_owner_name: str,
    first_name: str,
    email: str,
    booking_link: str,
    sfdc_user_id: str = "",
) -> None:
    """Add or update a user in the registry (thread-safe write)."""
    with _lock:
        users = _load()
        users[slack_id] = {
            "sf_owner_name": sf_owner_name,
            "first_name": first_name,
            "email": email,
            "booking_link": booking_link,
            "sfdc_user_id": sfdc_user_id,
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }
        _save_no_lock()


def get_all_users() -> dict[str, dict]:
    """Return a shallow copy of the full registry (for scheduled job iteration)."""
    with _lock:
        users = _load()
    return dict(users)


def get_user_sf_name(slack_id: str) -> str:
    """Return the Salesforce owner name for *slack_id*, falling back to config."""
    user = get_user(slack_id)
    if user:
        return user["sf_owner_name"]
    return OWNER_NAME


def get_user_email(slack_id: str) -> str:
    """Return the email for *slack_id*, falling back to config."""
    user = get_user(slack_id)
    if user:
        return user["email"]
    return GMAIL_ADDRESS or ""


def get_user_first_name(slack_id: str) -> str:
    """Return the first name for *slack_id*, falling back to config."""
    user = get_user(slack_id)
    if user:
        return user["first_name"]
    return OWNER_FIRST_NAME


def get_user_booking_link(slack_id: str) -> str:
    """Return the booking link for *slack_id*, falling back to config."""
    user = get_user(slack_id)
    if user:
        return user["booking_link"]
    return BOOKING_LINK or ""


def get_user_sfdc_id(slack_id: str) -> str:
    """Return the SFDC user ID for *slack_id*, or empty string."""
    user = get_user(slack_id)
    if user:
        return user.get("sfdc_user_id", "")
    return ""


def get_user_token_dir(slack_id: str) -> str:
    """Return the per-user token directory path (~/.gary_bot_tokens/<slack_id>/).

    Creates the directory if it doesn't exist.
    """
    d = os.path.join(_TOKENS_DIR, slack_id)
    os.makedirs(d, exist_ok=True)
    return d


def get_user_gmail_tokens(slack_id: str) -> tuple[str, str] | None:
    """Return (tokens_path, client_info_path) for *slack_id*'s Gmail, or None.

    Checks per-user token dir first, then falls back to the default
    mcp-remote location (for the original owner).
    """
    import hashlib
    from pathlib import Path

    # Per-user tokens
    user_dir = os.path.join(_TOKENS_DIR, slack_id)
    user_tokens = os.path.join(user_dir, "gmail_tokens.json")
    user_client = os.path.join(user_dir, "gmail_client_info.json")
    if os.path.exists(user_tokens) and os.path.exists(user_client):
        return user_tokens, user_client

    # Fall back to default mcp-remote location (original owner)
    if slack_id == OWNER_SLACK_ID:
        mcp_url = "https://mcp.gumloop.com/gmail/mcp"
        server_hash = hashlib.md5(mcp_url.encode()).hexdigest()
        token_dir = Path.home() / ".mcp-auth" / "mcp-remote-0.1.12"
        default_tokens = token_dir / f"{server_hash}_tokens.json"
        default_client = token_dir / f"{server_hash}_client_info.json"
        if default_tokens.exists() and default_client.exists():
            return str(default_tokens), str(default_client)

    return None


def get_user_gong_tokens(slack_id: str) -> str | None:
    """Return the Gong token file path for *slack_id*, or None.

    Checks per-user token dir first, then falls back to the default
    mcp-remote location (for the original owner).
    """
    import hashlib
    from pathlib import Path

    # Per-user tokens
    user_dir = os.path.join(_TOKENS_DIR, slack_id)
    user_tokens = os.path.join(user_dir, "gong_tokens.json")
    if os.path.exists(user_tokens):
        return user_tokens

    # Fall back to default mcp-remote location (original owner)
    if slack_id == OWNER_SLACK_ID:
        mcp_url = "https://mcp.gumloop.com/gong/mcp"
        server_hash = hashlib.md5(mcp_url.encode()).hexdigest()
        token_dir = Path.home() / ".mcp-auth" / "mcp-remote-0.1.12"
        default_tokens = token_dir / f"{server_hash}_tokens.json"
        if default_tokens.exists():
            return str(default_tokens)

    return None
