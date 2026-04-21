"""Plays tab refresher — 3-layer freshness: on-demand, startup, scheduled.

Persists per-user, per-play results to ~/.gary_bot_plays_cache.json so the
Prospecting tab renders instantly and hits Snowflake only when stale.

Cache layout:
    {
      "version": 1,
      "<user_id>": {
        "P1": {"rows": [...], "fetched_at": 1713720000.0, "count": 95},
        "P5": {"rows": [...], "fetched_at": ..., "count": 6},
        ...
      },
      ...
    }

Staleness:
  - DEFAULT_TTL_SEC (12h) is soft. If cache is fresh, skip refresh.
  - When the Prospecting tab opens and a play is stale, kick an async thread
    to refresh; render cached in the meantime.
  - Bot startup warms stale caches for all registered users after a short delay.
  - APScheduler runs full refresh weekdays 10 AM + 2 PM PT.
"""
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

from core.user_registry import get_all_users
from queries.plays import PLAYS, run_play

logger = logging.getLogger(__name__)

_CACHE_FILE = str(Path.home() / ".gary_bot_plays_cache.json")
_lock = threading.Lock()
DEFAULT_TTL_SEC = 12 * 3600  # 12 hours

# Thread-safe "refresh in progress" markers so we don't double-fire async work
_in_flight: set = set()  # items: (user_id, play_id)


def _load() -> dict:
    try:
        if os.path.exists(_CACHE_FILE):
            with open(_CACHE_FILE) as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except Exception as e:
        logger.debug("Failed to load plays cache: %s", e)
    return {"version": 1}


def _save(data: dict) -> None:
    tmp = _CACHE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, default=str)
    os.replace(tmp, _CACHE_FILE)


def get_cached_play(play_id: str, user_id: str = "") -> Optional[dict]:
    """Return cached play data for (user, play) or None if missing.

    Return shape: {"rows": [...], "fetched_at": epoch, "count": N, "stale": bool}
    """
    data = _load()
    user_bucket = data.get(user_id or "_default") or {}
    entry = user_bucket.get(play_id)
    if not entry:
        return None
    age = time.time() - float(entry.get("fetched_at", 0))
    entry_copy = dict(entry)
    entry_copy["stale"] = age > DEFAULT_TTL_SEC
    entry_copy["age_seconds"] = age
    return entry_copy


def refresh_play(play_id: str, user_id: str = "") -> dict:
    """Run the play's query, write to cache, return the new entry.

    Synchronous — the caller controls threading. Safe to call concurrently
    for different (user, play) pairs.
    """
    try:
        df = run_play(play_id, user_id=user_id or None)
        rows = df.to_dict(orient="records") if not df.empty else []
        entry = {
            "rows": rows,
            "fetched_at": time.time(),
            "count": len(rows),
        }
    except Exception as e:
        logger.error("refresh_play(%s, %s) failed: %s", play_id, user_id, e)
        return {"rows": [], "fetched_at": time.time(), "count": 0, "error": str(e)}

    with _lock:
        data = _load()
        key = user_id or "_default"
        data.setdefault(key, {})[play_id] = entry
        _save(data)
    return entry


def refresh_play_async(play_id: str, user_id: str = "") -> None:
    """Kick an async refresh if one isn't already in flight for this (user, play)."""
    tag = (user_id or "", play_id)
    with _lock:
        if tag in _in_flight:
            return
        _in_flight.add(tag)

    def _run():
        try:
            refresh_play(play_id, user_id=user_id)
        finally:
            with _lock:
                _in_flight.discard(tag)

    threading.Thread(target=_run, daemon=True, name=f"play-refresh-{play_id}-{user_id}").start()


def get_or_refresh_play(play_id: str, user_id: str = "") -> dict:
    """Return cached data immediately. If stale, kick async refresh in background.

    Always returns the currently cached data (or empty if never fetched) so
    the tab renders fast.
    """
    cached = get_cached_play(play_id, user_id=user_id)
    if cached is None:
        # Never fetched — do a blocking refresh this one time
        return refresh_play(play_id, user_id=user_id)
    if cached["stale"]:
        refresh_play_async(play_id, user_id=user_id)
    return cached


def refresh_all_for_user(user_id: str) -> dict:
    """Refresh all plays for one user. Returns a summary dict."""
    summary = {}
    for play_id in PLAYS.keys():
        entry = refresh_play(play_id, user_id=user_id)
        summary[play_id] = entry["count"]
        if entry.get("error"):
            summary[play_id] = f"ERROR: {entry['error'][:60]}"
    return summary


def refresh_all() -> dict:
    """Scheduled/startup refresh — walks every registered user and refreshes
    their full play catalog. Returns {user_id: {play_id: count}}.
    """
    users = get_all_users() or [{"slack_user_id": ""}]
    results = {}
    for u in users:
        uid = u.get("slack_user_id") or ""
        try:
            results[uid] = refresh_all_for_user(uid)
            logger.info("Plays refresh complete for user=%s: %s", uid, results[uid])
        except Exception as e:
            logger.error("Plays refresh failed for user=%s: %s", uid, e)
            results[uid] = {"error": str(e)}
    return results


def warm_on_startup(delay_sec: int = 30) -> None:
    """Call once at bot startup — waits briefly so registry is loaded, then
    refreshes stale caches in the background."""
    def _run():
        time.sleep(delay_sec)
        try:
            logger.info("Plays startup warm kicking off")
            refresh_all()
        except Exception as e:
            logger.error("Plays startup warm failed: %s", e)

    threading.Thread(target=_run, daemon=True, name="plays-warm").start()
