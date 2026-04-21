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


# ── Account-level snoozes (keyed by account_id; stored in same file with "acct:" prefix) ──

def _account_key(account_id: str) -> str:
    return f"acct:{account_id}"


def snooze_account(account_id: str, days: int = 30, user_id: str = "") -> None:
    """Snooze an account (by SFDC ID) for `days` days. Used by Top-CP Re-engage."""
    with _lock:
        data = _load()
        data[_account_key(account_id)] = {
            "until": time.time() + days * 86400,
            "user_id": user_id,
            "snoozed_at": time.time(),
            "kind": "account",
        }
        _save(data)
    logger.info("Snoozed account %s for %dd (user=%s)", account_id, days, user_id)


def is_account_snoozed(account_id: str, user_id: str = "") -> bool:
    """Check if an account is currently snoozed for this user (or globally if no user_id)."""
    data = _load()
    entry = data.get(_account_key(account_id))
    if not entry:
        return False
    if time.time() > entry["until"]:
        with _lock:
            data = _load()
            data.pop(_account_key(account_id), None)
            _save(data)
        return False
    if user_id and entry.get("user_id") and entry.get("user_id") != user_id:
        return False
    return True


def get_snoozed_accounts(user_id: str = "") -> set:
    """Return set of account_ids currently snoozed for this user."""
    data = _load()
    now = time.time()
    active = set()
    for key, entry in data.items():
        if not key.startswith("acct:"):
            continue
        if now > entry["until"]:
            continue
        if user_id and entry.get("user_id") and entry.get("user_id") != user_id:
            continue
        active.add(key[len("acct:"):])
    return active


# ── Play-specific snoozes (keyed per-play so P1 snooze doesn't hide from P13) ──

def _play_key(play_id: str, account_id: str) -> str:
    return f"play:{play_id}:{account_id}"


def snooze_play_account(play_id: str, account_id: str, days: int = 30, user_id: str = "") -> None:
    """Snooze an account from a specific play — other plays still show it."""
    with _lock:
        data = _load()
        data[_play_key(play_id, account_id)] = {
            "until": time.time() + days * 86400,
            "user_id": user_id,
            "snoozed_at": time.time(),
            "kind": "play",
            "play_id": play_id,
        }
        _save(data)
    logger.info("Snoozed %s from play %s for %dd (user=%s)", account_id, play_id, days, user_id)


def get_snoozed_play_accounts(play_id: str, user_id: str = "") -> set:
    """Return set of account_ids currently snoozed for this specific play."""
    data = _load()
    now = time.time()
    prefix = f"play:{play_id}:"
    active = set()
    for key, entry in data.items():
        if not key.startswith(prefix):
            continue
        if now > entry["until"]:
            continue
        if user_id and entry.get("user_id") and entry.get("user_id") != user_id:
            continue
        active.add(key[len(prefix):])
    return active
