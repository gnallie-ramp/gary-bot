"""Daily opp pacing alert — 8:00 AM PT.

Categorizes every open expansion opp into URGENT / AT RISK / WIN / ON TRACK
based on milestone activation, spend vs baseline, days to close, and last
touch recency.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd

from queries.queries import OPP_PACING_MILESTONES_QUERY, format_query
from core.snowflake_client import run_query
from core.slack_formatter import sf_opp_url, format_currency, dashboard_url
from config import GREG_SLACK_ID, NTR_RATES

logger = logging.getLogger(__name__)

# ── Categorisation thresholds ────────────────────────────────────────────────
_URGENT_DAYS_TO_CLOSE = 14
_AT_RISK_DAYS_OPEN = 30
_AT_RISK_DAYS_SINCE_TOUCH = 21
_WIN_MILESTONE_RECENCY_HOURS = 48


# ── Helpers ──────────────────────────────────────────────────────────────────

def _safe_float(val, default: float = 0.0) -> float:
    """Coerce *val* to float, falling back to *default* on None/NaN."""
    try:
        v = float(val)
        return default if pd.isna(v) else v
    except (TypeError, ValueError):
        return default


def _safe_int(val, default: int = 0) -> int:
    try:
        v = int(float(val))
        return v
    except (TypeError, ValueError):
        return default


def _est_cp(product: str, recent_l30d: float, baseline_spend: float) -> float:
    """Estimated CP = NTR * max(0, recent_l30d - baseline)."""
    ntr = NTR_RATES.get(product, 0.0095)
    return round(ntr * max(0.0, recent_l30d - baseline_spend), 2)


def _milestone_date(row) -> datetime | None:
    """Return the relevant milestone date as a datetime (or None)."""
    raw = row.get("relevant_milestone_date")
    if raw is None or pd.isna(raw):
        return None
    if isinstance(raw, datetime):
        return raw
    try:
        return pd.Timestamp(raw).to_pydatetime()
    except Exception:
        return None


def _categorize(row) -> str:
    """Return one of URGENT, AT_RISK, WIN, ON_TRACK for an opp row."""
    days_to_close = _safe_int(row.get("days_to_close"), default=999)
    days_open = _safe_int(row.get("days_open"), default=0)
    days_since_touch = _safe_int(row.get("days_since_touch"), default=0)
    recent_l30d = _safe_float(row.get("recent_l30d"))
    baseline_spend = _safe_float(row.get("baseline_spend"))
    milestone_dt = _milestone_date(row)

    now = datetime.utcnow()
    milestone_fired = milestone_dt is not None

    # URGENT: close date within 14 days AND no activation milestone has fired
    if days_to_close <= _URGENT_DAYS_TO_CLOSE and not milestone_fired:
        return "URGENT"

    # WIN: relevant milestone fired in last 48h, OR recent L30D > baseline
    if milestone_fired:
        hours_since = (now - milestone_dt).total_seconds() / 3600
        if hours_since <= _WIN_MILESTONE_RECENCY_HOURS:
            return "WIN"
    if recent_l30d > baseline_spend and baseline_spend > 0:
        return "WIN"

    # AT RISK: opp 30+ days old and milestone hasn't happened, OR stale touch
    if days_open >= _AT_RISK_DAYS_OPEN and not milestone_fired:
        return "AT_RISK"
    if days_since_touch >= _AT_RISK_DAYS_SINCE_TOUCH:
        return "AT_RISK"

    # ON TRACK: milestone exists, spend positive, not urgent
    return "ON_TRACK"


def _suggested_action(category: str, row) -> str:
    """Return a short, actionable suggestion for the opp."""
    product = row.get("expansion_subtype", "")
    days_to_close = _safe_int(row.get("days_to_close"), default=999)
    days_since_touch = _safe_int(row.get("days_since_touch"), default=0)
    milestone_fired = _milestone_date(row) is not None
    recent_l30d = _safe_float(row.get("recent_l30d"))
    baseline_spend = _safe_float(row.get("baseline_spend"))

    if category == "URGENT":
        return f"Close date in {days_to_close}d with no activation — close or push date"
    if category == "AT_RISK":
        if days_since_touch >= _AT_RISK_DAYS_SINCE_TOUCH:
            return f"No contact in {days_since_touch}d — re-engage today"
        return "Milestone not yet fired — follow up on activation"
    if category == "WIN":
        if recent_l30d > baseline_spend and baseline_spend > 0:
            delta = format_currency(recent_l30d - baseline_spend)
            return f"Spend exceeding baseline by {delta} — close now"
        return "Milestone just fired — close to lock baseline"
    # ON_TRACK
    return "Tracking — monitor for spend ramp"


# ── Opp item builder ─────────────────────────────────────────────────────────

def _build_opp_item(row, category: str) -> dict:
    """Build a display dict for one opp."""
    product = row.get("expansion_subtype", "")
    baseline = _safe_float(row.get("baseline_spend"))
    recent = _safe_float(row.get("recent_l30d"))
    cp = _est_cp(product, recent, baseline)
    milestone_dt = _milestone_date(row)
    milestone_name = row.get("relevant_milestone_name", "")

    return {
        "account_name": row.get("account_name", "Unknown"),
        "account_id": row.get("account_id", ""),
        "opp_id": row.get("opportunity_id", ""),
        "product": product,
        "milestone": (
            f"{milestone_name} ({milestone_dt.strftime('%b %d')})"
            if milestone_dt
            else f"{milestone_name}: not yet"
        ),
        "baseline": format_currency(baseline),
        "recent_l30d": format_currency(recent),
        "est_cp": format_currency(cp),
        "days_to_close": _safe_int(row.get("days_to_close"), 999),
        "action": _suggested_action(category, row),
        "category": category,
    }


# ── Slack Block Kit formatting ───────────────────────────────────────────────

def _indicator(category: str) -> str:
    return {
        "URGENT": "\U0001f534",   # red circle
        "AT_RISK": "\u26a0\ufe0f",  # warning
        "WIN": "\u2705",          # check
        "ON_TRACK": "\u2705",     # check
    }.get(category, "")


def _format_opp_line(item: dict) -> str:
    """Render a single opp as a compact mrkdwn line."""
    indicator = _indicator(item["category"])
    sf_link = sf_opp_url(item["opp_id"])
    name_link = f"<{sf_link}|{item['account_name']}>"
    parts = [
        f"{indicator} {name_link}",
        item["product"],
        item["milestone"],
        f"{item['baseline']} -> {item['recent_l30d']}",
        f"CP ~{item['est_cp']}",
        f"{item['days_to_close']}d to close",
    ]
    line = "  " + " | ".join(parts)
    line += f"\n      _{item['action']}_"
    return line


def _build_blocks(
    date_str: str,
    urgent: list[dict],
    at_risk: list[dict],
    wins: list[dict],
) -> list[dict]:
    """Assemble Slack Block Kit blocks for the pacing report."""
    blocks: list[dict] = []

    # Header
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"Opp Pacing Report \u2014 {date_str}",
            "emoji": True,
        },
    })

    # Summary line
    summary = (
        f"*{len(urgent)} urgent, {len(at_risk)} at risk, "
        f"{len(wins)} on track.*"
    )
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": summary},
    })

    blocks.append({"type": "divider"})

    # URGENT section
    if urgent:
        lines = [f"*\U0001f534 URGENT* ({len(urgent)})"]
        for item in urgent:
            lines.append(_format_opp_line(item))
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        })
        blocks.append({"type": "divider"})

    # AT RISK section
    if at_risk:
        lines = [f"*\u26a0\ufe0f AT RISK* ({len(at_risk)})"]
        for item in at_risk:
            lines.append(_format_opp_line(item))
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        })
        blocks.append({"type": "divider"})

    # WINS / ON TRACK section
    if wins:
        lines = [f"*\u2705 WINS / ON TRACK* ({len(wins)})"]
        for item in wins:
            lines.append(_format_opp_line(item))
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        })

    # Footer with dashboard links
    blocks.append({"type": "divider"})
    _pipe = dashboard_url("pipeline")
    _perf = dashboard_url("performance")
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"<{_pipe}|Pipeline> · <{_perf}|Performance> · `/opp-pacing` to refresh"}],
    })

    return blocks


# ── Public entry point ───────────────────────────────────────────────────────

def run_opp_pacing(client, user_id=None, account_name: str | None = None, force: bool = False):
    """Run daily opp pacing analysis and DM Greg.

    Parameters
    ----------
    client : slack_sdk.WebClient
        Authenticated Slack client for posting messages.
    account_name : str, optional
        If provided, filter the report to a single account (deep dive).
    force : bool
        When ``True``, send the report even if there is nothing actionable
        (useful for manual / slash-command invocations).
    """
    dm_target = user_id or GREG_SLACK_ID

    try:
        # ── 1. Opp milestones & spend ────────────────────────────────────
        opps_df = run_query(format_query(OPP_PACING_MILESTONES_QUERY, user_id=user_id))
        if opps_df.empty and not force:
            logger.info("Opp pacing: no open opps returned")
            return

        # Optional single-account filter
        if account_name and not opps_df.empty:
            mask = opps_df["account_name"].str.contains(
                account_name, case=False, na=False,
            )
            opps_df = opps_df[mask]

        # ── 2. Categorize each opp ───────────────────────────────────────
        urgent: list[dict] = []
        at_risk: list[dict] = []
        wins: list[dict] = []

        for _, row in opps_df.iterrows():
            category = _categorize(row)
            item = _build_opp_item(row, category)

            if category == "URGENT":
                urgent.append(item)
            elif category == "AT_RISK":
                at_risk.append(item)
            else:
                # WIN and ON_TRACK both go to the wins/on-track section
                wins.append(item)

        # Sort: urgent by days_to_close asc, at_risk by days_to_close asc,
        # wins by est CP desc (strip $ and commas for sorting)
        urgent.sort(key=lambda x: x["days_to_close"])
        at_risk.sort(key=lambda x: x["days_to_close"])
        wins.sort(
            key=lambda x: -_safe_float(
                x["est_cp"].replace("$", "").replace(",", "").replace("M", "e6"),
            ),
        )

        # ── 3. Skip if nothing to report (unless forced) ─────────────────
        if not urgent and not at_risk and not wins and not force:
            logger.info("Opp pacing: nothing to report")
            return

        # ── 4. Build Slack message and send ──────────────────────────────
        date_str = datetime.now().strftime("%b %d, %Y")
        blocks = _build_blocks(date_str, urgent, at_risk, wins)

        fallback = (
            f"Opp Pacing Report: {len(urgent)} urgent, "
            f"{len(at_risk)} at risk, {len(wins)} on track"
        )
        client.chat_postMessage(
            channel=dm_target,
            blocks=blocks,
            text=fallback,
        )
        logger.info(
            "Opp pacing sent: %d urgent, %d at risk, %d wins/on-track",
            len(urgent),
            len(at_risk),
            len(wins),
        )

    except Exception as e:
        logger.error("Opp pacing job failed: %s", e)
