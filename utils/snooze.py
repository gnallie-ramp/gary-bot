"""Snooze manager for stale opps — hide opps from the tab temporarily.

Persists snoozes to ~/.gary_bot_snoozes.json. Thread-safe via a lock.
"""
import json
import logging
import os
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_SNOOZE_FILE = str(Path.home() / ".gary_bot_snoozes.json")
_lock = threading.Lock()


def _load() -> dict:
    """Load snoozes from disk. Returns {opp_id: {until: epoch, user_id: str}}."""
    try:
        if os.path.exists(_SNOOZE_FILE):
            with open(_SNOOZE_FILE) as f:
                return json.load(f)
    except Exception as e:
        logger.debug("Failed to load snoozes: %s", e)
    return {}


def _save(data: dict) -> None:
    with open(_SNOOZE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def snooze_opp(opp_id: str, days: int = 7, user_id: str = "") -> None:
    """Snooze an opp for `days` days."""
    with _lock:
        data = _load()
        data[opp_id] = {
            "until": time.time() + days * 86400,
            "user_id": user_id,
            "snoozed_at": time.time(),
        }
        _save(data)
    logger.info("Snoozed opp %s for %dd (user=%s)", opp_id, days, user_id)


def unsnooze_opp(opp_id: str) -> None:
    """Remove a snooze."""
    with _lock:
        data = _load()
        if opp_id in data:
            del data[opp_id]
            _save(data)


def is_snoozed(opp_id: str) -> bool:
    """Check if an opp is currently snoozed."""
    data = _load()
    entry = data.get(opp_id)
    if not entry:
        return False
    if time.time() > entry["until"]:
        # Expired — clean it up
        with _lock:
            data = _load()
            data.pop(opp_id, None)
            _save(data)
        return False
    return True


def get_snoozed_opps(user_id: str = "") -> dict:
    """Return all active snoozes for a user. Cleans up expired ones."""
    data = _load()
    now = time.time()
    active = {}
    expired = []
    for opp_id, entry in data.items():
        if now > entry["until"]:
            expired.append(opp_id)
        elif not user_id or entry.get("user_id") == user_id:
            active[opp_id] = entry
    # Cleanup expired
    if expired:
        with _lock:
            data = _load()
            for oid in expired:
                data.pop(oid, None)
            _save(data)
    return active
