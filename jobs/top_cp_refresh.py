"""Top-CP Re-engage — surface the top 20 highest-CP accounts in the book
that haven't been captured by a recent closed-won expansion opp.

Answers the question "which accounts in my book have the biggest upside
that I'm not actively working?" — regardless of recent activity, the goal is
to keep the top-of-book visible and prevent accounts like Aspora (huge
past commitments, quiet on my radar) from slipping through.

Data source: Growth MCP (`get_filtered_accounts` sorted by
`total_new_sale_est_cp desc`, then `get_opportunities` per account to
check recent CW expansion). Cached per-user, refreshed daily at 6 AM PT.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional

from core import growth_mcp

logger = logging.getLogger(__name__)

# In-memory per-user cache: user_id -> {"active": [...], "churned": [...], "fetched_at": epoch}
_cache: dict = {}
_CACHE_TTL = 24 * 3600  # 24 hours — scheduled job refreshes at 6 AM

# Filter thresholds
TOP_ACTIVE_COUNT = 20
TOP_CHURNED_COUNT = 10
# Candidate pool to scan before filtering (buffer for the 90-day exclusion)
_CANDIDATE_POOL_SIZE = 100
# Skip accounts where any CW Expansion opp closed within this many days
_RECENT_CW_EXCLUSION_DAYS = 90

# Columns to request from get_filtered_accounts (beyond the default slim set)
_ENRICH_COLUMNS = [
    "effective_priority_tier",
    "total_new_sale_est_cp",
    "fte_size",
    "account_status",
    "onboarding_status",
    "can_send_international_payments",
    "wise_onboarded_at",
]


# ── Helpers ──────────────────────────────────────────────────────────────────


def _days_since(iso_ts: Optional[str]) -> Optional[int]:
    """Return days since an ISO8601 timestamp, or None if missing/unparseable."""
    if not iso_ts:
        return None
    try:
        dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).days
    except (ValueError, TypeError):
        return None


def _has_recent_cw_expansion(account_id: str, user_id: Optional[str]) -> bool:
    """True if the account has a CW Expansion opp that closed within the exclusion window."""
    opps = growth_mcp.get_opportunities(
        sfdc_account_id=account_id,
        state="won",
        opportunity_type="Expansion",
        user_id=user_id,
    )
    if not opps:
        return False
    for opp in opps:
        days = _days_since(opp.get("close_date"))
        if days is not None and days <= _RECENT_CW_EXCLUSION_DAYS:
            return True
    return False


def _enrich(raw: dict) -> dict:
    """Shape a raw account dict into the card-ready payload."""
    return {
        "account_id": raw.get("sfdc_account_id"),
        "account_uuid": raw.get("uuid"),
        "account": raw.get("account_name"),
        "domain": raw.get("account_domain") or raw.get("domain"),
        "industry": raw.get("industry"),
        "fte_size": raw.get("fte_size"),
        "account_status": raw.get("account_status"),
        "onboarding_status": raw.get("onboarding_status"),
        "tier": raw.get("effective_priority_tier"),
        "cp_potential": raw.get("total_new_sale_est_cp"),
        "can_send_international_payments": raw.get("can_send_international_payments"),
        "wise_onboarded_at": raw.get("wise_onboarded_at"),
    }


# ── Public API ────────────────────────────────────────────────────────────────


def get_cached_top_cp(user_id: str) -> dict:
    """Return cached top-CP payload, or empty shape if nothing cached."""
    entry = _cache.get(user_id)
    if not entry:
        return {"active": [], "churned": [], "fetched_at": 0}
    return entry


def gather_top_cp_accounts(user_id: Optional[str] = None, force: bool = False) -> dict:
    """Build the Top-CP Re-engage list for a user.

    Returns {"active": [top 20 Active], "churned": [top 10 Churn], "fetched_at": epoch}.
    Cached per-user for 24h unless force=True.
    """
    cache_key = user_id or "default"
    cached = _cache.get(cache_key)
    if cached and not force and time.time() - cached["fetched_at"] < _CACHE_TTL:
        return cached

    logger.info("Top-CP: gathering accounts for user=%s (force=%s)", user_id, force)

    # Pull top candidates by CP estimate
    raw_accounts = growth_mcp.get_filtered_accounts(
        sort_by="total_new_sale_est_cp",
        sort_direction="desc",
        page_size=_CANDIDATE_POOL_SIZE,
        columns=_ENRICH_COLUMNS,
        user_id=user_id,
    )
    if not raw_accounts:
        logger.warning("Top-CP: no accounts returned from Growth MCP for user=%s", user_id)
        empty = {"active": [], "churned": [], "fetched_at": time.time()}
        _cache[cache_key] = empty
        return empty

    active_out: list[dict] = []
    churned_out: list[dict] = []
    checked = 0

    for raw in raw_accounts:
        if len(active_out) >= TOP_ACTIVE_COUNT and len(churned_out) >= TOP_CHURNED_COUNT:
            break

        status = (raw.get("account_status") or "").strip()
        account_id = raw.get("sfdc_account_id")
        if not account_id:
            continue

        # Churn — no exclusion filter, just take them
        if status == "Churn" and len(churned_out) < TOP_CHURNED_COUNT:
            churned_out.append(_enrich(raw))
            continue

        # Active — filter out recently-captured accounts (90-day CW expansion window)
        if status != "Active" or len(active_out) >= TOP_ACTIVE_COUNT:
            continue

        try:
            if _has_recent_cw_expansion(account_id, user_id):
                continue
        except Exception as e:
            logger.debug("Top-CP: CW check failed for %s, including anyway: %s", account_id, e)

        active_out.append(_enrich(raw))
        checked += 1

    result = {
        "active": active_out,
        "churned": churned_out,
        "fetched_at": time.time(),
    }
    _cache[cache_key] = result
    logger.info(
        "Top-CP: user=%s — %d active, %d churned (scanned %d candidates)",
        user_id, len(active_out), len(churned_out), len(raw_accounts),
    )
    return result


# ── Scheduled job entry ───────────────────────────────────────────────────────


def run_top_cp_refresh(client, user_id: Optional[str] = None) -> None:
    """Scheduled job — refresh the Top-CP cache for a user. Does NOT DM anything;
    keeps the surface quiet (home tab only).
    """
    try:
        result = gather_top_cp_accounts(user_id=user_id, force=True)
        logger.info(
            "Top-CP refresh complete for user=%s: %d active, %d churned",
            user_id, len(result["active"]), len(result["churned"]),
        )
    except Exception as e:
        logger.error("Top-CP refresh failed for user=%s: %s", user_id, e)
