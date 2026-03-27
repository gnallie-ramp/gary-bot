"""Priority Actions — the single most important DM Gary Bot sends.

Two-level interactive flow:
  Level 1: Summary card with category counts + drill-down buttons
  Level 2: Category detail with per-account actions + "Draft Email" buttons

Unified signal system:
  Spend signals (from HOME_PRIORITY_ALERTS_QUERY):
    1. Early Accel — L7D ramping, L30D still low (window open to lock low baseline)
    2. Close Window — open opp, L7D ramping above L30D (close before baseline rises)
    3. Close Now — open opp, L30D already exceeds baseline
    4. Leading — large bills/card volume incoming
    5. First Bill — open bill pay opp, first bill just created
    6. Zero-to-One — product activated after opp created (lock in low baseline)
    7. Sustained Accel — L30D already elevated (window closing/closed)

  Non-spend signals (separate queries):
    8. Missing Follow-ups — Gong calls without follow-up email
    9. Post-Meeting Opp — expansion products discussed, no opp exists
    10. Stale Opps — S2+ opps gone silent
    11. Re-open — closed-won opps with spend patterns worth re-opening

On-demand via /priorities or DM "what should I focus on?"
"""
from __future__ import annotations

import json
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd

from queries.queries import (
    HOME_PRIORITY_ALERTS_QUERY, POST_MEETING_CALLS_QUERY,
    STALE_OPPS_QUERY, REOPEN_QUERY, RECENT_CW_BY_PRODUCT_QUERY,
    POST_CLOSE_CHECKPOINT_QUERY, format_query,
)
from core.snowflake_client import run_query
from core.slack_formatter import (
    sf_account_url, sf_opp_url, format_currency, dashboard_url,
    build_sf_new_opp_url, opp_fields_summary,
)
from config import GREG_SLACK_ID, NTR_RATES

logger = logging.getLogger(__name__)

MAX_PER_CATEGORY = 10

# Stages considered "invested" — S2+ means work has been done, worth re-engaging
_STAGE_ORDER = {
    "S1: Sales Accepted Opportunity": 1,
    "S2: Sales Qualified Opportunity": 2,
    "S3: Securing Technical Win": 3,
    "S4: Securing Business Win": 4,
    "S5: Finalizing Closure": 5,
}

# In-memory cache for drill-down, keyed by user_id (cleared on each /priorities run)
_cached_actions: dict[str, dict[str, list[dict]]] = {}
_cache_ts: dict[str, float] = {}
_CACHE_TTL = 300  # 5 minutes


# ── Signal gatherers ─────────────────────────────────────────────────────────


def _safe_float(val, default=0.0):
    try:
        v = float(val)
        return default if pd.isna(v) else v
    except (TypeError, ValueError):
        return default


def _safe_int(v):
    try:
        f = float(v)
        return 0 if math.isnan(f) else int(f)
    except Exception:
        return 0


def _pct(paced, base):
    return int(((paced - base) / base) * 100) if base > 0 else 0


def _presale_detail(row):
    """Return presale discrepancy line for detail_lines, or empty string."""
    product = str(row.get("product", ""))
    ae_card = _safe_int(row.get("ae_card_presale", 0))
    ae_bp = _safe_int(row.get("ae_bp_presale", 0))
    paced = _safe_int(row.get("paced_amount", 0)) or _safe_int(row.get("spend_l30d", 0))
    presale = ae_card if "Card" in product else ae_bp if "Bill" in product else 0
    if presale <= 0:
        return ""
    if paced > 0 and presale > 0:
        ratio = paced / presale
        if ratio >= 2.0:
            return f"AE presale {format_currency(presale)}/mo — actual {ratio:.1f}x higher → large delta upside"
        elif ratio <= 0.3:
            return f"AE presale {format_currency(presale)}/mo — actual only {int(ratio*100)}% → partial migration, room to grow"
    return f"AE presale: {format_currency(presale)}/mo"


def _gather_alert_signals(user_id: str = None) -> list[dict]:
    """All spend/acceleration signals from HOME_PRIORITY_ALERTS_QUERY.

    Returns items for: early_accel, close_window, close_now, leading,
    first_bill, zero_to_one, sustained_accel.
    """
    try:
        df = run_query(format_query(HOME_PRIORITY_ALERTS_QUERY, user_id=user_id))
        if df.empty:
            return []

        items = []
        for _, row in df.iterrows():
            signal_type = row.get("signal_type", "")
            product = str(row.get("product", ""))
            ntr = NTR_RATES.get(product, 0.0095)
            paced = _safe_int(row.get("paced_amount", 0))
            base = _safe_int(row.get("baseline_amount", 0))
            l30d = _safe_int(row.get("spend_l30d", 0))
            l7d = _safe_int(row.get("spend_l7d", 0))
            cp = _safe_int(row.get("est_cp", 0))
            delta = _safe_int(row.get("l30d_spend_delta", 0))
            product_short = product.replace(" Expansion", "")
            pct_val = _pct(paced, base)
            cp_str = f"~{format_currency(cp)} CP" if cp > 0 else ""

            if signal_type == "early_accel":
                icon = "\u26a1"
                action = f"L7D pacing +{pct_val}% — window open"
                detail_lines = [
                    f"L7D pacing {format_currency(paced)}/mo vs {format_currency(base)} baseline (+{pct_val}%)",
                    f"L30D only {format_currency(l30d)} — window open to lock low baseline",
                    f"_Why: L7D raw {format_currency(l7d)} is {pct_val}% above 90D avg, but L30D hasn't caught up yet_",
                ]
                if cp_str:
                    detail_lines[0] += f" · {cp_str}"
                priority = 95

            elif signal_type == "close_window":
                icon = "\u23f0"
                action = f"Close before baseline rises — L7D pacing {format_currency(paced)}/mo"
                detail_lines = [
                    f"L7D pacing {format_currency(paced)}/mo · L30D baseline: {format_currency(l30d)}",
                    f"_Close now — L7D ramping above current L30D — close before baseline rises_",
                ]
                if cp_str:
                    detail_lines[0] += f" · {cp_str}"
                priority = 90

            elif signal_type == "close_now":
                icon = "\U0001f534"
                action = f"Close ASAP — L30D +{format_currency(abs(delta))} above baseline"
                detail_lines = [
                    f"L30D +{format_currency(abs(delta))} above baseline",
                    "_Every day you wait, baseline rises and CP shrinks._",
                ]
                if cp_str:
                    detail_lines[0] += f" · {cp_str}"
                priority = 85

            elif signal_type == "leading":
                icon = "\U0001f440"
                action = f"{format_currency(paced)} incoming vs {format_currency(base)}/mo baseline"
                detail_lines = [
                    f"{format_currency(paced)} in {'bills created/scheduled' if 'Bill' in product else 'card volume (L3D pacing)'} vs {format_currency(base)}/mo baseline",
                    "_Large volume queued that exceeds typical monthly volume_",
                ]
                if cp_str:
                    detail_lines[0] += f" · {cp_str}"
                priority = 80

            elif signal_type == "first_bill":
                icon = "\U0001f389"
                action = f"First bill created — {format_currency(paced)}"
                detail_lines = [
                    f"First bill created in Ramp ({format_currency(paced)})",
                    "_Bill Pay opp open — customer just started using the product_",
                ]
                if cp_str:
                    detail_lines[0] += f" · {cp_str}"
                priority = 75

            elif signal_type == "zero_to_one":
                spend_since = _safe_int(row.get("spend_since_opp", 0))
                ntr_label = f"{ntr * 10000:.0f}bps"
                icon = "\u26a1"
                action = f"Lock in {format_currency(l30d)} baseline"
                detail_lines = [
                    f"{format_currency(spend_since)} since opp · L30D {format_currency(l30d)} · L7D {format_currency(l7d)}",
                    f"NTR: {ntr_label} — CP = growth above {format_currency(l30d)} x {ntr_label} x 3 windows",
                    "_Close now — every day you wait, baseline rises and CP shrinks._",
                ]
                if cp_str:
                    detail_lines[0] += f" · {cp_str}"
                priority = 70

            elif signal_type == "sustained_accel":
                icon = "\U0001f4c8"
                action = f"L7D pacing +{pct_val}% — L30D catching up"
                detail_lines = [
                    f"L7D pacing {format_currency(paced)}/mo vs {format_currency(base)} baseline (+{pct_val}%)",
                    f"L30D already at {format_currency(l30d)} — baseline catching up",
                    "_Window closing — create + close opp before baseline sets higher_",
                ]
                if cp_str:
                    detail_lines[0] += f" · {cp_str}"
                priority = 65

            elif signal_type == "treasury_spike":
                spike_pct = _pct(paced, l30d) if l30d > 0 else 0
                icon = "\U0001f4b0"
                action = f"GLA spiked +{spike_pct}% — {format_currency(paced)} avg balance"
                detail_lines = [
                    f"L7D avg balance {format_currency(paced)} vs L30D avg {format_currency(l30d)} (+{spike_pct}%)",
                    f"Baseline (L90D avg): {format_currency(base)}",
                    "_Large deposit detected — lock in treasury expansion opp while balance is high_",
                ]
                if cp_str:
                    detail_lines[0] += f" · {cp_str}"
                priority = 88  # High — treasury is uncapped H1-26

            else:
                continue

            ps = _presale_detail(row)
            if ps:
                detail_lines.append(ps)

            items.append({
                "type": signal_type,
                "icon": icon,
                "priority": priority + min(20, cp / 100) if cp > 0 else priority,
                "account": row.get("account_name", "Unknown"),
                "account_id": row.get("account_id", ""),
                "opp_id": row.get("opportunity_id", ""),
                "product": product,
                "action": action,
                "detail": "\n      ".join(detail_lines),
                "est_cp": cp,
                "stage": "",
                "days_since_touch": 0,
                "l30d_spend_raw": l30d,
                "_paced": paced,
                "_baseline": base,
                "last_call_date": str(row.get("last_call_date", "") or ""),
                "last_email_date": str(row.get("last_email_date", "") or ""),
            })
        return items
    except Exception as e:
        logger.warning("Priority actions: alert signals failed: %s", e)
        return []


def _gather_checkpoint_signals(user_id: str = None) -> list[dict]:
    """D30/D60 post-close activation checkpoints.

    Finds CW opps at D30 (days 25-35) and D60 (days 55-65) where
    actual spend is below 80% of target SOW pace.
    """
    try:
        df = run_query(format_query(POST_CLOSE_CHECKPOINT_QUERY, user_id=user_id))
        if df.empty:
            return []

        items = []
        for _, row in df.iterrows():
            signal_type = row.get("signal_type", "")
            product = str(row.get("product", ""))
            ntr = NTR_RATES.get(product, 0.0095)
            baseline = _safe_int(row.get("baseline_at_close", 0))
            current = _safe_int(row.get("current_l30d", 0))
            target = _safe_int(row.get("target_spend", 0))
            pct = _safe_int(row.get("pct_of_baseline", 0))
            days_since = _safe_int(row.get("days_since_cw", 0))
            product_short = product.replace(" Expansion", "")

            # CP at risk = what they should be earning above baseline
            shortfall = max(0, target - current)
            cp_at_risk = shortfall * ntr * 3

            if signal_type == "underperforming_d30":
                icon = "\u26a0\ufe0f"
                checkpoint = "D30"
                action = f"D30 check — spending {pct}% of baseline ({product_short})"
                detail_lines = [
                    f"CW {days_since}d ago | L30D: {format_currency(current)} vs {format_currency(baseline)} baseline ({pct}%)",
                    f"Target: {format_currency(target)} (130% SOW) — shortfall: {format_currency(shortfall)}",
                    "_Re-engage now — still 60 days left in the window._",
                ]
                priority = 55
            elif signal_type == "underperforming_d60":
                icon = "\U0001f6a8"
                checkpoint = "D60"
                action = f"D60 check — spending {pct}% of baseline ({product_short})"
                detail_lines = [
                    f"CW {days_since}d ago | L30D: {format_currency(current)} vs {format_currency(baseline)} baseline ({pct}%)",
                    f"Target: {format_currency(target)} (160% SOW) — shortfall: {format_currency(shortfall)}",
                    "_Only 30 days left — escalate or consider re-open._",
                ]
                priority = 60
            else:
                continue

            if cp_at_risk > 0:
                detail_lines[0] += f" · ~{format_currency(cp_at_risk)} CP at risk"

            items.append({
                "type": signal_type,
                "icon": icon,
                "priority": priority + min(20, cp_at_risk / 100) if cp_at_risk > 0 else priority,
                "account": row.get("account_name", "Unknown"),
                "account_id": str(row.get("account_id", "")),
                "opp_id": str(row.get("opportunity_id", "")),
                "product": product,
                "action": action,
                "detail": "\n      ".join(detail_lines),
                "est_cp": cp_at_risk,
                "stage": "",
                "days_since_touch": 0,
                "l30d_spend_raw": current,
                "_baseline": baseline,
                "_target": target,
                "_days_since_cw": days_since,
                "last_call_date": str(row.get("last_call_date", "") or ""),
                "last_email_date": str(row.get("last_email_date", "") or ""),
            })

        items.sort(key=lambda x: -x["priority"])
        return items[:MAX_PER_CATEGORY]

    except Exception as e:
        logger.warning("Priority actions: checkpoint signals failed: %s", e)
        return []


def _gather_missing_followups(lookback_days: int = 7, user_id: str = None) -> list[dict]:
    """Gong calls missing follow-up emails."""
    try:
        df = run_query(format_query(POST_MEETING_CALLS_QUERY, user_id=user_id, lookback_days=lookback_days))
        if df.empty:
            return []

        items = []
        for _, row in df.iterrows():
            if not row.get("missing_followup"):
                continue

            call_name = row.get("call_name", "")
            call_date = str(row.get("call_date", ""))
            duration = int(row.get("duration_min", 0) or 0)

            items.append({
                "type": "followup",
                "icon": "\U0001f4e8",
                "priority": 50,
                "account": row.get("account_name", "Unknown"),
                "account_id": str(row.get("account_id", "")),
                "opp_id": row.get("linked_opp_id") or row.get("latest_opp_id") or "",
                "product": "",
                "action": f"Send follow-up — {call_name}",
                "detail": f"{call_date} ({duration}min call). No follow-up email sent yet.",
                "est_cp": 0,
                "stage": "",
                "days_since_touch": 0,
                "_call_name": call_name,
                "_call_date": call_date,
                "last_call_date": call_date,
                "last_email_date": "",
            })
        return items
    except Exception as e:
        logger.warning("Priority actions: missing followups failed: %s", e)
        return []


def _gather_post_meeting_opps(lookback_days: int = 14, user_id: str = None) -> list[dict]:
    """Detect Gong calls where expansion products were discussed but no opp exists.

    Deduplicates against:
      - Open opps for the same account + product type
      - Recently closed-won opps (last 90 days) for the same account + product type
    """
    try:
        df = run_query(format_query(POST_MEETING_CALLS_QUERY, user_id=user_id, lookback_days=lookback_days))
        if df.empty:
            return []

        # Build dedup set: (account_id, product) for recently CW opps
        recent_cw = set()
        try:
            cw_df = run_query(format_query(RECENT_CW_BY_PRODUCT_QUERY, user_id=user_id))
            for _, row in cw_df.iterrows():
                recent_cw.add((row["account_id"], row["expansion_subtype"]))
        except Exception:
            pass

        # Map Gong product mentions → expansion_subtype
        _PRODUCT_MAP = {
            "card": "Card Expansion",
            "bill pay": "Bill Pay Expansion",
            "billpay": "Bill Pay Expansion",
            "treasury": "Treasury Expansion",
            "travel": "Travel Expansion",
            "plus": "SaaS",
            "saas": "SaaS",
            "ramp plus": "SaaS",
            "procurement": "Procurement",
        }

        items = []
        seen = set()

        for _, row in df.iterrows():
            products_mentioned = str(row.get("products_mentioned", "") or "").lower()
            if not products_mentioned:
                continue

            account_id = str(row.get("account_id", ""))
            opp_products = str(row.get("opp_products", "") or "").lower()

            # Parse which expansion products were discussed
            for keyword, expansion_type in _PRODUCT_MAP.items():
                if keyword not in products_mentioned:
                    continue

                dedup_key = (account_id, expansion_type)
                if dedup_key in seen:
                    continue

                # Skip if open opp already exists for this product
                if expansion_type.lower().replace(" expansion", "") in opp_products:
                    continue

                # Skip if recently CW for same product
                if dedup_key in recent_cw:
                    continue

                seen.add(dedup_key)

                call_name = row.get("call_name", "")
                call_date = str(row.get("call_date", ""))
                product_requests = str(row.get("all_product_requests", "") or "")[:200]
                product_short = expansion_type.replace(" Expansion", "")

                detail_lines = [
                    f"_{call_name}_ ({call_date}) discussed *{product_short}*",
                    "No open opp and no recent CW for this product.",
                ]
                if product_requests:
                    detail_lines.append(f"Asked about: {product_requests[:100]}")
                detail_lines.append("_Create opp to capture this expansion._")

                items.append({
                    "type": "post_meeting_opp",
                    "icon": "\U0001f3af",
                    "priority": 65,
                    "account": row.get("account_name", "Unknown"),
                    "account_id": account_id,
                    "opp_id": "",
                    "product": expansion_type,
                    "action": f"Create {product_short} opp — discussed on call",
                    "detail": "\n      ".join(detail_lines),
                    "est_cp": 0,
                    "l30d_spend_raw": 0,
                    "stage": "",
                    "days_since_touch": 0,
                    "_call_name": call_name,
                    "_call_date": call_date,
                    "last_call_date": call_date,
                    "last_email_date": "",
                })

        items.sort(key=lambda x: -x["priority"])
        return items[:MAX_PER_CATEGORY]

    except Exception as e:
        logger.warning("Priority actions: post-meeting opp detection failed: %s", e)
        return []


def _gather_stale_opps(user_id: str = None) -> list[dict]:
    """S2+ stale opps ranked by CP value with meeting/email context."""
    try:
        df = run_query(format_query(STALE_OPPS_QUERY, user_id=user_id))
        if df.empty:
            return []

        items = []
        for _, row in df.iterrows():
            stage = row.get("opportunity_stage_name", "")
            stage_rank = _STAGE_ORDER.get(stage, 0)

            if stage_rank < 2:
                continue

            days_since = int(_safe_float(row.get("days_since_last_touch")))
            # Skip opps with no real touch history (2000-01-01 default = ~9500d)
            if days_since > 365:
                continue

            product = row.get("expansion_subtype", "")
            ntr = NTR_RATES.get(product, 0.0095)
            baseline = _safe_float(row.get("baseline_spend"))
            recent = _safe_float(row.get("recent_30d_spend"))
            over_baseline = max(0, recent - baseline)
            est_cp = over_baseline * ntr * 3

            days_open = int(_safe_float(row.get("days_open")))

            cp_score = min(50, est_cp / 50) if est_cp > 0 else min(20, recent * ntr * 3 / 50)
            stage_bonus = stage_rank * 8
            staleness_bonus = min(15, days_since / 4)
            priority = cp_score + stage_bonus + staleness_bonus

            last_call_name = row.get("last_call_name", "")
            last_call_date = str(row.get("last_call_date", "") or "")
            last_email_date = str(row.get("last_email_date", "") or "")
            last_email_subj = row.get("last_email_subject", "") or ""
            call_summary = str(row.get("last_call_section_text", "") or "")
            product_requests = str(row.get("last_call_product_requests", "") or "")
            competitors = str(row.get("last_call_competitors", "") or "")
            activation_status = row.get("activation_status", "")

            context_parts = []
            if last_call_name and last_call_date:
                context_parts.append(f"Last call: _{last_call_name}_ ({last_call_date})")
            if last_email_date and last_email_date != "2000-01-01":
                subj_short = last_email_subj[:35] + "..." if len(last_email_subj) > 35 else last_email_subj
                context_parts.append(f'Last email: "{subj_short}" ({last_email_date})')
            if competitors:
                context_parts.append(f"Competitors: {competitors}")
            if product_requests:
                req_short = product_requests[:60] + "..." if len(product_requests) > 60 else product_requests
                context_parts.append(f"Asked about: {req_short}")

            # Activation status with prominent icon
            _STATUS_ICONS = {
                "No spend yet": "\u26aa",        # white circle
                "Very low": "\U0001f7e4",        # brown circle
                "Below baseline": "\U0001f7e1",  # yellow circle
                "Near baseline": "\U0001f7e2",   # green circle
                "Exceeding baseline": "\U0001f534",  # red = close NOW
            }
            status_icon = _STATUS_ICONS.get(activation_status, "")
            status_label = f"{status_icon} {activation_status}" if activation_status else ""

            spend_line = f"Baseline: {format_currency(baseline)} | L30D: {format_currency(recent)}"
            if status_label:
                spend_line = f"{status_label} — {spend_line}"

            detail_lines = [
                f"{stage} | {days_open}d open | {days_since}d stale",
                spend_line,
            ]
            if est_cp > 0:
                detail_lines[0] += f" | ~{format_currency(est_cp)} CP if closed"
            if activation_status == "Exceeding baseline":
                detail_lines.append("_Spend above baseline — close ASAP to capture CP._")
            elif activation_status == "No spend yet":
                detail_lines.append("_No activation yet — re-engage or push close date._")

            for cp in context_parts[:2]:
                detail_lines.append(cp)

            detail_lines.append("_Get them back on the calendar._")

            items.append({
                "type": "stale",
                "icon": "\u23f0",
                "priority": priority,
                "account": row.get("account_name", "Unknown"),
                "account_id": row.get("account_id", ""),
                "opp_id": row.get("opportunity_id", ""),
                "product": product,
                "action": f"Re-engage — {stage}, {days_since}d silent",
                "detail": "\n      ".join(detail_lines),
                "est_cp": est_cp,
                "stage": stage,
                "days_since_touch": days_since,
                "_call_summary": call_summary[:500] if call_summary else "",
                "_product_requests": product_requests[:200] if product_requests else "",
                "_competitors": competitors,
                "_last_call_name": last_call_name,
                "_last_call_date": last_call_date,
                "_last_email_subj": last_email_subj,
                "_baseline": baseline,
                "_recent": recent,
                "last_call_date": last_call_date,
                "last_email_date": last_email_date,
            })

        items.sort(key=lambda x: -x["priority"])
        return items[:MAX_PER_CATEGORY]

    except Exception as e:
        logger.warning("Priority actions: stale opps failed: %s", e)
        return []


# ── Re-open pattern classification ──────────────────────────────────────────

_PATTERN_LABELS = {
    "spike_then_reversion": ("Spike then drop", "Early spend above baseline, then reverted"),
    "seasonal_opportunity": ("Seasonal low", "Current L30D well below historical avg — low baseline window"),
    "slow_ramp": ("Slow ramp", "Spend ramping up across windows — new opp captures the growth"),
    "never_activated": ("Never activated", "Never exceeded baseline — re-engage before writing off"),
    "activated_sustained": ("Sustained above", "Spend stayed above baseline — new opp locks in higher base"),
}


def _classify_reopen_pattern(row) -> tuple:
    """Classify a closed-won opp into a spend pattern. Returns (pattern_key, est_cp_upside)."""
    baseline = _safe_float(row.get("baseline_at_close"))
    d1 = _safe_float(row.get("spend_d1_d30"))
    d2 = _safe_float(row.get("spend_d31_d60"))
    d3 = _safe_float(row.get("spend_d61_d90"))
    current = _safe_float(row.get("current_l30d"))
    ntr = NTR_RATES.get(row.get("expansion_subtype", "Card Expansion"), 0.0095)

    def _pct(spend, ref):
        if ref <= 0:
            return 0 if spend <= 0 else 999
        return spend / ref

    pct_d1 = _pct(d1, baseline)
    pct_d2 = _pct(d2, baseline)
    pct_d3 = _pct(d3, baseline)
    pct_current = _pct(current, baseline)

    # Conservative CP upside: delta between baseline and current (if current is lower)
    est_cp = max(baseline - current, 0) * ntr * 3

    # Spike then reversion: early windows above, then dropped
    if (pct_d1 > 1.2 or pct_d2 > 1.2) and (pct_d3 < 1.0 and pct_current < 1.0):
        return "spike_then_reversion", est_cp

    # Activated and sustained: all above baseline
    if pct_d1 >= 1.0 and pct_d2 >= 1.0 and pct_d3 >= 1.0 and pct_current >= 1.0:
        return "activated_sustained", est_cp

    # Slow ramp: spending increasing
    if d1 <= d2 <= d3 or (d2 <= d3 and d3 <= current and current > baseline * 0.5):
        if current >= d1 and current > 0:
            return "slow_ramp", est_cp

    # Never activated: default
    return "never_activated", est_cp


def _gather_reopen_opps(user_id: str = None) -> list[dict]:
    """Closed-won opps 60-120 days ago with spend patterns worth re-opening."""
    try:
        df = run_query(format_query(REOPEN_QUERY, user_id=user_id))
        if df.empty:
            return []

        items = []
        for _, row in df.iterrows():
            pattern, est_cp = _classify_reopen_pattern(row)

            # Skip "activated_sustained" — those are working fine, no action needed
            if pattern == "activated_sustained":
                continue

            baseline = _safe_float(row.get("baseline_at_close"))
            current = _safe_float(row.get("current_l30d"))
            d1 = _safe_float(row.get("spend_d1_d30"))
            d2 = _safe_float(row.get("spend_d31_d60"))
            d3 = _safe_float(row.get("spend_d61_d90"))
            days_since_cw = int(_safe_float(row.get("days_since_cw")))
            product = row.get("expansion_subtype", "")

            pattern_label, pattern_desc = _PATTERN_LABELS.get(pattern, ("Unknown", ""))

            # Priority: higher for patterns with clear CP upside
            if pattern == "spike_then_reversion":
                priority = 60
            elif pattern == "seasonal_opportunity":
                priority = 55
            elif pattern == "slow_ramp":
                priority = 50
            else:  # never_activated
                priority = 30

            if est_cp > 0:
                priority += min(20, est_cp / 100)

            # Build spend trajectory line
            def _pct_str(val, ref):
                if ref <= 0:
                    return "—"
                return f"{int(100 * val / ref)}%"

            trajectory = (
                f"D1-30: {_pct_str(d1, baseline)} | "
                f"D31-60: {_pct_str(d2, baseline)} | "
                f"D61-90: {_pct_str(d3, baseline)} | "
                f"Now: {_pct_str(current, baseline)}"
            )

            detail_lines = [
                f"CW {days_since_cw}d ago | Baseline: {format_currency(baseline)} | L30D: {format_currency(current)}",
                f"Pattern: _{pattern_label}_ — {pattern_desc}",
                trajectory,
            ]
            if est_cp > 0:
                detail_lines.append(f"_New opp could capture ~{format_currency(est_cp)} CP if spend recovers._")

            items.append({
                "type": "reopen",
                "icon": "\U0001f504",
                "priority": priority,
                "account": row.get("account_name", "Unknown"),
                "account_id": row.get("account_id", ""),
                "opp_id": "",
                "product": product,
                "action": f"Re-open — {pattern_label}",
                "detail": "\n      ".join(detail_lines),
                "est_cp": est_cp,
                "l30d_spend_raw": current,
                "stage": "",
                "days_since_touch": 0,
                "_pattern": pattern,
                "_baseline": baseline,
                "_current": current,
                "last_call_date": str(row.get("last_call_date", "") or ""),
                "last_email_date": str(row.get("last_email_date", "") or ""),
            })

        items.sort(key=lambda x: -x["priority"])
        return items[:MAX_PER_CATEGORY]

    except Exception as e:
        logger.warning("Priority actions: reopen opps failed: %s", e)
        return []


# ── Grouping + caching ──────────────────────────────────────────────────────


def _group_by_type(all_items: list[dict]) -> dict[str, list[dict]]:
    """Group items by type, dedup within each group, sort by priority."""
    groups: dict[str, list[dict]] = {}
    seen: dict[str, set] = {}

    for item in sorted(all_items, key=lambda x: -x["priority"]):
        t = item["type"]
        if t not in groups:
            groups[t] = []
            seen[t] = set()
        key = item["account_id"]
        if key in seen[t]:
            continue
        seen[t].add(key)
        if len(groups[t]) < MAX_PER_CATEGORY:
            groups[t].append(item)

    return groups


def get_cached_category(category: str, user_id: str = None) -> list[dict]:
    """Retrieve cached items for a category (used by interactive handlers)."""
    uid = user_id or GREG_SLACK_ID
    if time.time() - _cache_ts.get(uid, 0) > _CACHE_TTL:
        return []
    return _cached_actions.get(uid, {}).get(category, [])


# ── Level 1: Summary card ───────────────────────────────────────────────────


def _build_summary_blocks(date_str: str, groups: dict[str, list[dict]]) -> list[dict]:
    """Build the Level 1 summary DM with category buttons."""
    blocks = [{
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"\U0001f3af Priority Actions — {date_str}",
            "emoji": True,
        },
    }]

    total_items = sum(len(v) for v in groups.values())
    if total_items == 0:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\u2705 No urgent actions right now. Run `/gary-status` to verify connections.",
            },
        })
        return blocks

    # ── Summary section builder ──
    def _add_summary(cat, icon, label, desc, action_id, style=None, cp_label="CP at stake"):
        items = groups.get(cat, [])
        if not items:
            return
        total_cp = sum(a.get("est_cp", 0) for a in items)
        cp_part = f" — ~{format_currency(total_cp)} {cp_label}" if total_cp > 0 else ""
        n = len(items)
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{icon} *{n} {label}*{cp_part}\n{desc}",
            },
        })
        btn = {
            "type": "button",
            "text": {"type": "plain_text", "text": f"Show ({n})", "emoji": True},
            "action_id": action_id,
        }
        if style:
            btn["style"] = style
        blocks.append({"type": "actions", "elements": [btn]})

    _add_summary("early_accel", "\u26a1", "early acceleration — window open",
                 "L7D ramping, L30D still low. Lock in low baseline now.",
                 "priority_show_early_accel", "danger")

    _add_summary("close_window", "\u23f0", "close window — opp ramping",
                 "Open opps where L7D is ramping above L30D. Close before baseline rises.",
                 "priority_show_close_window", "danger")

    _add_summary("close_now", "\U0001f534", "close ASAP — above baseline",
                 "L30D already exceeds baseline. Every day costs CP.",
                 "priority_show_close_now", "danger")

    _add_summary("leading", "\U0001f440", "leading indicators — spend incoming",
                 "Large bills or card volume queued above typical monthly baseline.",
                 "priority_show_leading", "primary")

    _add_summary("first_bill", "\U0001f389", "first bill created",
                 "Open bill pay opps where customer just created their first bill.",
                 "priority_show_first_bill", "primary")

    _add_summary("zero_to_one", "\u26a1", "zero-to-one activations",
                 "Product activated — close early to lock in low baseline.",
                 "priority_show_zero_to_one", "primary")

    _add_summary("sustained_accel", "\U0001f4c8", "sustained acceleration — L30D elevated",
                 "L7D still above baseline but L30D catching up. Create + close opp before window closes.",
                 "priority_show_sustained_accel")

    _add_summary("treasury_spike", "\U0001f4b0", "treasury GLA spike",
                 "GLA balance spiked >2x in 7 days. Lock in treasury expansion opp now (uncapped H1-26).",
                 "priority_show_treasury_spike", "danger")

    _add_summary("underperforming_d60", "\U0001f6a8", "D60 checkpoint — underperforming",
                 "CW opps at D60 spending below 80% of target SOW. Only 30 days left.",
                 "priority_show_underperforming_d60", "danger", cp_label="CP at risk")

    _add_summary("underperforming_d30", "\u26a0\ufe0f", "D30 checkpoint — underperforming",
                 "CW opps at D30 spending below 80% of target SOW. Re-engage now.",
                 "priority_show_underperforming_d30", None, cp_label="CP at risk")

    # ── Multi-product bundling ──
    multi = groups.get("multi_product", [])
    if multi:
        total_cp = sum(a.get("est_cp", 0) for a in multi)
        cp_part = f" — ~{format_currency(total_cp)} combined CP" if total_cp > 0 else ""
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"\U0001f4e6 *{len(multi)} account{'s' if len(multi) != 1 else ''} with multi-product signals*{cp_part}\n"
                    f"Accounts appearing in 2+ signal categories — bundle outreach for bigger impact."
                ),
            },
        })
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": f"Show Multi-Product ({len(multi)})", "emoji": True},
                "action_id": "priority_show_multi_product",
                "style": "primary",
            }],
        })

    # ── Missing Follow-ups ──
    followups = groups.get("followup", [])
    if followups:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"\U0001f4e8 *{len(followups)} missing follow-up{'s' if len(followups) != 1 else ''}*\n"
                    f"Gong calls without follow-up email in 48h."
                ),
            },
        })
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": f"Show Follow-ups ({len(followups)})", "emoji": True},
                "action_id": "priority_show_followup",
            }],
        })

    # ── Post-Meeting Opp Detection ──
    post_meeting = groups.get("post_meeting_opp", [])
    if post_meeting:
        products = set(a.get("product", "").replace(" Expansion", "") for a in post_meeting)
        product_str = ", ".join(sorted(products))
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"\U0001f3af *{len(post_meeting)} expansion product{'s' if len(post_meeting) != 1 else ''} discussed — no opp*\n"
                    f"Gong calls mentioned {product_str} but no open or recent CW opp exists."
                ),
            },
        })
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": f"Show Post-Meeting Opps ({len(post_meeting)})", "emoji": True},
                "action_id": "priority_show_post_meeting_opp",
                "style": "primary",
            }],
        })

    # ── Stale Opps ──
    stale = groups.get("stale", [])
    if stale:
        total_cp = sum(a.get("est_cp", 0) for a in stale)
        stages = set(a.get("stage", "") for a in stale)
        stage_str = ", ".join(sorted(stages)) if stages else ""
        cp_part = f" — ~{format_currency(total_cp)} CP value" if total_cp > 0 else ""
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"\u23f0 *{len(stale)} stale opp{'s' if len(stale) != 1 else ''} to re-engage*{cp_part}\n"
                    f"S2+ opps gone silent. Get them back on the calendar."
                ),
            },
        })
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": f"Show Stale Opps ({len(stale)})", "emoji": True},
                "action_id": "priority_show_stale",
            }],
        })

    # ── Re-open Opps ──
    reopen = groups.get("reopen", [])
    if reopen:
        total_cp = sum(a.get("est_cp", 0) for a in reopen)
        patterns = {}
        for a in reopen:
            p = a.get("_pattern", "")
            patterns[p] = patterns.get(p, 0) + 1
        pattern_str = ", ".join(f"{v} {k.replace('_', ' ')}" for k, v in patterns.items())
        cp_part = f" — ~{format_currency(total_cp)} CP upside" if total_cp > 0 else ""
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"\U0001f504 *{len(reopen)} closed opp{'s' if len(reopen) != 1 else ''} worth re-opening*{cp_part}\n"
                    f"CW 60-120d ago, no follow-up opp. {pattern_str}."
                ),
            },
        })
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": f"Show Re-opens ({len(reopen)})", "emoji": True},
                "action_id": "priority_show_reopen",
            }],
        })

    # ── Footer ──
    blocks.append({"type": "divider"})
    _dash = dashboard_url("priority")
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"<{_dash}|Open Dashboard> · "
                f"Click any button above or DM me a category name to drill down"
            ),
        }],
    })

    return blocks


# ── Level 2: Category detail blocks ─────────────────────────────────────────


def build_category_detail_blocks(category: str, user_id: str = None) -> list[dict]:
    """Build Level 2 detail blocks for a specific category.

    Called by interactive handlers when user clicks a category button.
    """
    items = get_cached_category(category, user_id=user_id)
    if not items:
        return [{
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"No {category} items found. Run `/priorities` to refresh."},
        }]

    _TITLES = {
        "early_accel": ("\u26a1", "Early Acceleration — Window Open"),
        "close_window": ("\u23f0", "Close Window — Opp Ramping"),
        "close_now": ("\U0001f534", "Close Now — Above Baseline"),
        "leading": ("\U0001f440", "Leading Indicators — Spend Incoming"),
        "first_bill": ("\U0001f389", "First Bill Created"),
        "zero_to_one": ("\u26a1", "Zero-to-One Activations"),
        "sustained_accel": ("\U0001f4c8", "Sustained Acceleration"),
        "treasury_spike": ("\U0001f4b0", "Treasury GLA Spike"),
        "underperforming_d30": ("\u26a0\ufe0f", "D30 Checkpoint — Underperforming"),
        "underperforming_d60": ("\U0001f6a8", "D60 Checkpoint — Underperforming"),
        "multi_product": ("\U0001f4e6", "Multi-Product Signals"),
        "followup": ("\U0001f4e8", "Missing Follow-ups"),
        "post_meeting_opp": ("\U0001f3af", "Post-Meeting — Create Opp"),
        "stale": ("\u23f0", "Stale Opps — Re-engage"),
        "reopen": ("\U0001f504", "Re-open — Post-Close Patterns"),
    }
    icon, title = _TITLES.get(category, ("", category.title()))

    blocks = [{
        "type": "header",
        "text": {"type": "plain_text", "text": f"{icon} {title} — {len(items)} items", "emoji": True},
    }]

    def _touch_str(item):
        """Compact last-call / last-email line for detail blocks."""
        parts = []
        for key, label in [("last_call_date", "Call"), ("last_email_date", "Email")]:
            val = str(item.get(key, "") or "")
            if val and val not in ("", "None", "NaT", "2000-01-01"):
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(val[:10], "%Y-%m-%d")
                    parts.append(f"{label} {d.strftime('%-m/%-d')}")
                except Exception:
                    pass
        return f"\n      _Last: {' · '.join(parts)}_" if parts else ""

    for i, action in enumerate(items, 1):
        account_id = action.get("account_id", "")
        opp_id = action.get("opp_id", "")
        sf_link = sf_opp_url(opp_id) if opp_id else sf_account_url(account_id)
        name_link = f"<{sf_link}|{action['account']}>"
        product_str = f" — {action['product']}" if action.get("product") else ""
        cp_str = f" | ~{format_currency(action['est_cp'])} CP" if action.get("est_cp") else ""
        touch = _touch_str(action)

        line = (
            f"{action['icon']} *{i}. {name_link}*{product_str}\n"
            f"      *{action['action']}*\n"
            f"      {action['detail']}{cp_str}{touch}"
        )

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": line},
        })

        # Show opp fields + "Create Opp" button for items needing a new opp
        has_opp = bool(action.get("opp_id"))
        needs_new_opp = (
            category in ("early_accel", "sustained_accel", "leading", "reopen",
                          "post_meeting_opp", "treasury_spike", "multi_product")
            or (category == "zero_to_one" and not has_opp)
        )
        product = action.get("product", "")

        if needs_new_opp and product:
            l30d_raw = action.get("l30d_spend_raw", 0) or action.get("est_cp", 0)
            fields_text = opp_fields_summary(
                product_type=product,
                amount=l30d_raw if l30d_raw > 0 else 0,
                l30d=l30d_raw,
            )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Opp Fields:*\n{fields_text}"},
            })

        # Action buttons per item
        buttons = []

        # "Create Opp" button with pre-filled SF URL
        if needs_new_opp and product:
            l30d_raw = action.get("l30d_spend_raw", 0) or 0
            sf_new_url = build_sf_new_opp_url(
                account_name=action["account"],
                account_id=account_id,
                product_type=product,
                amount=l30d_raw if l30d_raw > 0 else 0,
                expansion_notes=f"L30D: {format_currency(l30d_raw)} at opp creation",
            )
            buttons.append({
                "type": "button",
                "text": {"type": "plain_text", "text": "Create Opp in SF", "emoji": True},
                "url": sf_new_url,
                "action_id": f"create_opp_{account_id}_{i}",
                "style": "primary",
            })

        # "Draft Email" for all categories that benefit from outreach
        if category in ("early_accel", "close_window", "close_now", "leading",
                         "first_bill", "zero_to_one", "sustained_accel",
                         "treasury_spike", "underperforming_d30", "underperforming_d60",
                         "multi_product", "stale", "followup", "reopen", "post_meeting_opp"):
            draft_payload = json.dumps({
                "account_id": account_id,
                "account": action["account"],
                "opp_id": opp_id,
                "product": action.get("product", ""),
                "category": category,
            })[:2000]
            label = "Draft Follow-up Email" if category == "followup" else "Draft Outreach Email"
            buttons.append({
                "type": "button",
                "text": {"type": "plain_text", "text": label, "emoji": True},
                "action_id": f"draft_outreach_{account_id}_{i}",
                "value": draft_payload,
            })

        # "Context" button — triggers account deep dive via DM
        context_payload = json.dumps({
            "account_id": account_id,
            "account": action["account"],
        })[:2000]
        buttons.append({
            "type": "button",
            "text": {"type": "plain_text", "text": "Context", "emoji": True},
            "action_id": f"account_context_{account_id}_{i}",
            "value": context_payload,
        })

        # Prep link
        prep_link = dashboard_url("meeting-prep", account=action["account"])
        buttons.append({
            "type": "button",
            "text": {"type": "plain_text", "text": "Prep", "emoji": True},
            "url": prep_link,
            "action_id": f"prep_{account_id}_{i}",
        })

        if buttons:
            blocks.append({"type": "actions", "elements": buttons})

    # "What next?" footer
    blocks.append({"type": "divider"})

    next_categories = [c for c in [
        "early_accel", "close_window", "close_now", "leading", "first_bill",
        "zero_to_one", "sustained_accel", "treasury_spike",
        "underperforming_d60", "underperforming_d30", "multi_product",
        "followup", "post_meeting_opp", "stale", "reopen",
    ] if c != category and get_cached_category(c, user_id=user_id)]
    if next_categories:
        _LABELS = {
            "early_accel": "`early accel`",
            "close_window": "`close window`",
            "close_now": "`close now`",
            "leading": "`leading`",
            "first_bill": "`first bill`",
            "zero_to_one": "`zero to one`",
            "sustained_accel": "`sustained accel`",
            "treasury_spike": "`treasury spike`",
            "underperforming_d30": "`D30 checkpoint`",
            "underperforming_d60": "`D60 checkpoint`",
            "multi_product": "`multi-product`",
            "post_meeting_opp": "`post-meeting opps`",
            "followup": "`follow-ups`",
            "stale": "`stale opps`",
            "reopen": "`re-open`",
        }
        other_str = " · ".join(_LABELS.get(c, c) for c in next_categories)
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f"*What next?* DM me {other_str} to see other categories · `/priorities` to refresh",
            }],
        })

    return blocks


# ── Main entry point ─────────────────────────────────────────────────────────


def run_priority_actions(client, user_id=None, force: bool = False, silent: bool = False):
    """Generate and send the Level 1 priority summary DM.

    Parameters
    ----------
    client : slack_sdk.WebClient
    force : bool
        When True, send even if no actions (on-demand use).
    silent : bool
        When True, populate cache only — don't send the summary DM.
        Used by DM keyword handlers that need data for drill-down.
    """
    global _cached_actions, _cache_ts

    dm_target = user_id or GREG_SLACK_ID

    try:
        all_items = []

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {
                executor.submit(_gather_alert_signals, user_id=dm_target): "alerts",
                executor.submit(_gather_checkpoint_signals, user_id=dm_target): "checkpoints",
                executor.submit(_gather_missing_followups, 7, user_id=dm_target): "followups",
                executor.submit(_gather_post_meeting_opps, 14, user_id=dm_target): "post_meeting_opp",
                executor.submit(_gather_stale_opps, user_id=dm_target): "stale",
                executor.submit(_gather_reopen_opps, user_id=dm_target): "reopen",
            }

            for future in as_completed(futures):
                label = futures[future]
                try:
                    items = future.result()
                    all_items.extend(items)
                    logger.info("Priority actions: %s returned %d items", label, len(items))
                except Exception as e:
                    logger.warning("Priority actions: %s failed: %s", label, e)

        # Group by type and cache for drill-down
        groups = _group_by_type(all_items)

        # ── Multi-product bundling: find accounts in 2+ categories ──
        acct_cats: dict[str, list[dict]] = {}
        for cat, items_list in groups.items():
            for item in items_list:
                aid = item.get("account_id", "")
                if not aid:
                    continue
                if aid not in acct_cats:
                    acct_cats[aid] = []
                acct_cats[aid].append(item)

        multi_items = []
        for aid, items_list in acct_cats.items():
            # Count distinct signal categories
            cats = set(item["type"] for item in items_list)
            if len(cats) >= 2:
                combined_cp = sum(item.get("est_cp", 0) for item in items_list)
                products = sorted(set(
                    item.get("product", "").replace(" Expansion", "")
                    for item in items_list if item.get("product")
                ))
                cat_labels = sorted(cats)
                acct_name = items_list[0].get("account", "Unknown")

                detail_lines = [
                    f"Signals: {', '.join(cat_labels)}",
                    f"Products: {', '.join(products) if products else 'Multiple'}",
                ]
                for item in items_list[:3]:
                    detail_lines.append(f"  {item['icon']} {item['action']}")
                if len(items_list) > 3:
                    detail_lines.append(f"  _...and {len(items_list) - 3} more_")

                multi_items.append({
                    "type": "multi_product",
                    "icon": "\U0001f4e6",
                    "priority": 90 + min(20, combined_cp / 100),
                    "account": acct_name,
                    "account_id": aid,
                    "opp_id": items_list[0].get("opp_id", ""),
                    "product": ", ".join(products) if products else "",
                    "action": f"{len(cats)} signals across {len(products)} products",
                    "detail": "\n      ".join(detail_lines),
                    "est_cp": combined_cp,
                    "stage": "",
                    "days_since_touch": 0,
                    "l30d_spend_raw": 0,
                    "_sub_items": items_list,
                })

        if multi_items:
            multi_items.sort(key=lambda x: -x["priority"])
            groups["multi_product"] = multi_items[:MAX_PER_CATEGORY]

        _cached_actions[dm_target] = groups
        _cache_ts[dm_target] = time.time()

        total_items = sum(len(v) for v in groups.values())
        if total_items == 0 and not force:
            logger.info("Priority actions: nothing to report")
            return

        if silent:
            logger.info("Priority actions: cache populated (%d items), silent mode", total_items)
            return

        date_str = datetime.now().strftime("%A, %b %d")
        blocks = _build_summary_blocks(date_str, groups)

        fallback = f"Priority Actions: {total_items} items across {len(groups)} categories"
        client.chat_postMessage(
            channel=dm_target,
            blocks=blocks,
            text=fallback,
        )
        logger.info("Priority actions summary sent: %d items in %d categories", total_items, len(groups))

    except Exception as e:
        logger.error("Priority actions failed: %s", e)
