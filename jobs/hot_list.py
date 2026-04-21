"""Hot List — the top 20-50 accounts across all Plays, ranked by match ×
team success rate. Persistent surface of "highest-leverage prospects right
now" so AMs have no excuse not to work them.

Scoring:
  match_score = sum over matching plays:
      (team_avg_realized_cp_for_play × log(1 + team_deal_count_for_play))

The log-scale adjustment means a play with 10 deals at $5K avg contributes
more than a play with 1 deal at $50K avg — reflects confidence that the
pattern actually works, not just that one outlier closed huge.

When the Play Library hasn't been built yet (cold start) or a play has
no team evidence, falls back to the play's own est_cp signal per account
(so Hot List still functions even without deal anatomy data).

Persistent cache at ~/.gary_bot_hot_list.json, refreshed nightly after
the Play Library rebuild completes (Deal Anatomy 2 AM → Play Library
3 AM → Hot List 3:30 AM).
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from jobs.play_library import load as load_library
from jobs.plays_refresh import get_cached_play
from queries.plays import PLAYS, PLAY_ID_TO_ANATOMY_TAGS

logger = logging.getLogger(__name__)

CACHE_FILE = Path.home() / ".gary_bot_hot_list.json"
FALLBACK_TEAM_CP = 2500   # $ — assumed per-deal CP if no team evidence yet
FALLBACK_TEAM_COUNT = 1   # deals


def _team_success_weight_for_play(play_id: str, lib: Optional[dict]) -> float:
    """Return the team-success multiplier applied to any account match of
    this play. Higher = more confidence this play closes deals.
    """
    anatomy_tags = PLAY_ID_TO_ANATOMY_TAGS.get(play_id, [])
    avg_cp = FALLBACK_TEAM_CP
    count = FALLBACK_TEAM_COUNT
    if lib and anatomy_tags:
        matched_entries = [
            (lib.get("tags") or {}).get(t.strip().lower())
            for t in anatomy_tags
        ]
        matched_entries = [e for e in matched_entries if e]
        if matched_entries:
            total_deals = sum(e.get("deal_count", 0) for e in matched_entries)
            total_cp = sum(e.get("total_realized_cp", 0) for e in matched_entries)
            if total_deals:
                avg_cp = total_cp // total_deals
                count = total_deals

    # Score = avg_cp × log(1 + count) — damps count's impact but rewards
    # plays with multiple wins over one-off outliers
    return float(avg_cp) * math.log1p(float(count))


def build_hot_list_for_user(user_id: str, top_n: int = 50) -> list[dict]:
    """Walk every play's cached accounts for this user, score each
    account, return top N sorted by match_score desc.

    Returns a list of dicts:
        {
            "account_id": "...",
            "account_name": "...",
            "match_score": 12345.6,
            "matching_plays": [{"play_id": "P1", "title": "...", "icon": "..."}],
            "est_cp_total": 789,
            "first_row": <row dict from the first matching play — used for card rendering>
        }
    """
    lib = load_library()
    play_weights = {pid: _team_success_weight_for_play(pid, lib) for pid in PLAYS.keys()}

    # Account → {"plays": [...], "rows": [...], "match_score": ...}
    accounts: dict = {}
    for play_id, play_meta in PLAYS.items():
        weight = play_weights.get(play_id, 0.0)
        cached = get_cached_play(play_id, user_id=user_id or "")
        if not cached:
            continue
        for row in cached.get("rows") or []:
            acct_id = row.get("account_id")
            if not acct_id:
                continue
            entry = accounts.setdefault(acct_id, {
                "account_id": acct_id,
                "account_name": row.get("account_name") or "",
                "match_score": 0.0,
                "matching_plays": [],
                "est_cp_total": 0,
                "first_row": row,
                "_seen_play_ids": set(),
            })
            if play_id not in entry["_seen_play_ids"]:
                entry["_seen_play_ids"].add(play_id)
                entry["match_score"] += weight
                entry["matching_plays"].append({
                    "play_id": play_id,
                    "title": play_meta.get("title"),
                    "icon": play_meta.get("icon"),
                })
                # Sum per-play CP signal if available on the row
                for cp_col in ("est_card_cp_monthly", "est_bp_cp_monthly",
                               "implied_monthly_yield_at_4_5pct"):
                    v = row.get(cp_col)
                    try:
                        entry["est_cp_total"] += int(float(v or 0))
                    except (TypeError, ValueError):
                        pass

    ranked = sorted(accounts.values(), key=lambda x: x["match_score"], reverse=True)[:top_n]
    # Drop the internal set before returning/serializing
    for r in ranked:
        r.pop("_seen_play_ids", None)
    return ranked


def rebuild() -> dict:
    """Nightly: build + cache Hot List for every registered user."""
    from core.user_registry import get_all_users

    users = get_all_users() or [{"slack_user_id": ""}]
    payload = {
        "built_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "by_user": {},
    }
    for u in users:
        uid = u.get("slack_user_id") or ""
        try:
            hot = build_hot_list_for_user(uid, top_n=50)
            payload["by_user"][uid or "_default"] = hot
            logger.info("Hot List built for user=%s: %d accounts", uid, len(hot))
        except Exception as e:
            logger.error("Hot List build failed for user=%s: %s", uid, e)
            payload["by_user"][uid or "_default"] = []

    tmp = str(CACHE_FILE) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    os.replace(tmp, CACHE_FILE)
    return {"users": len(users)}


def load_for_user(user_id: str) -> Optional[list]:
    """Load the cached Hot List for a user, or None if not built."""
    if not CACHE_FILE.exists():
        return None
    try:
        data = json.loads(CACHE_FILE.read_text())
        return (data.get("by_user") or {}).get(user_id or "_default")
    except Exception:
        return None


def get_or_build_for_user(user_id: str, top_n: int = 50) -> list[dict]:
    """Return cached Hot List for this user if available, else build on-demand.

    On-demand build takes ~1-2 seconds since Plays data is already cached.
    """
    cached = load_for_user(user_id)
    if cached is not None:
        return cached[:top_n]
    # On-demand build (Hot List file not yet rebuilt) — compute + save this
    # user's slice so the next tab open is instant
    result = build_hot_list_for_user(user_id, top_n=top_n)
    try:
        payload = {}
        if CACHE_FILE.exists():
            payload = json.loads(CACHE_FILE.read_text())
        payload.setdefault("by_user", {})[user_id or "_default"] = result
        payload["built_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        CACHE_FILE.write_text(json.dumps(payload, indent=2, default=str))
    except Exception as e:
        logger.debug("Failed to persist on-demand Hot List: %s", e)
    return result[:top_n]
