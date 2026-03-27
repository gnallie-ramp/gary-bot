"""Zero-to-One Activation Alerts — every 4 hours, 8AM-6PM PT.

Surfaces accounts that just hit a product activation milestone (first bill
paid, first treasury deposit, 5th card transaction, 5th travel booking) and
have an open opp for that product — "close now" signals.

Also includes a secondary "Missing Opps" section for activations without
an open opp, ported from the former opp_pacing zero-to-one section.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd

from queries.queries import SIGNALS_QUERY, format_query
from core.snowflake_client import run_query
from core.slack_formatter import sf_account_url, format_currency, dashboard_url
from config import GREG_SLACK_ID, NTR_RATES, SF_BASE_URL
from utils.dedup import tracker

logger = logging.getLogger(__name__)

# Product activation definitions:
# (activation_date_col, opp_flag_col, product_label, spend_col_or_None)
_PRODUCT_CHECKS = [
    ("card_activated_at", "has_open_card_opp", "Card Expansion", "card_spend_l30d"),
    ("billpay_first_paid_at", "has_open_billpay_opp", "Bill Pay Expansion", "billpay_spend_l30d"),
    ("billpay_third_paid_at", "has_open_billpay_opp", "Bill Pay Expansion", "billpay_spend_l30d"),
    ("treasury_activated_at", "has_open_treasury_opp", "Treasury Expansion", "treasury_balance_l30d"),
    ("travel_activated_at", "has_open_travel_opp", "Travel Expansion", None),
]

# Products that require boomerang to CSM at S4
_BOOMERANG_PRODUCTS = {"Bill Pay Expansion"}

_PRODUCT_EVENT_LABELS = {
    "Card Expansion": "5th card transaction",
    "Bill Pay Expansion": "First bill payment",
    "Treasury Expansion": "First treasury deposit",
    "Travel Expansion": "5th travel booking",
}


def _build_create_opp_url(account_id: str, product_type: str) -> str:
    """Build a pre-filled SFDC new opportunity URL."""
    import urllib.parse
    params = {
        "RecordType": "Expansion",
        "retURL": f"/lightning/r/Account/{account_id}/view",
    }
    base = f"{SF_BASE_URL}/o/Opportunity/new"
    return f"{base}?{urllib.parse.urlencode(params)}"

# Only alert on activations within the last 7 days (fresh signals)
_ACTIVATION_WINDOW_DAYS = 7


def _safe_float(val, default: float = 0.0) -> float:
    try:
        v = float(val)
        return default if pd.isna(v) else v
    except (TypeError, ValueError):
        return default


def _safe_int(val, default: int = 0) -> int:
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return default


def _is_recent(date_val, days: int = _ACTIVATION_WINDOW_DAYS) -> bool:
    """Return True if date_val is within the last *days* days."""
    if date_val is None or (isinstance(date_val, float) and pd.isna(date_val)):
        return False
    try:
        if isinstance(date_val, str):
            dt = datetime.strptime(date_val, "%Y-%m-%d")
        elif isinstance(date_val, datetime):
            dt = date_val
        else:
            dt = pd.Timestamp(date_val).to_pydatetime()
        return dt >= datetime.now() - timedelta(days=days)
    except Exception:
        return False


def _extract_activations(signals_df: pd.DataFrame):
    """Split activations into (with_opp, missing_opp) lists.

    with_opp: activation fired + matching open opp exists (close now signals)
    missing_opp: activation fired + no matching open opp (suggest creating one)

    Baseline = L30D at close-win. CP is earned on growth ABOVE baseline.
    The earlier you close, the lower the baseline, the more CP headroom.
    Accounts with a C/W opp for the same product in the last 90 days are excluded.
    """
    # Map product → recent C/W flag column
    _CW_FLAGS = {
        "Card Expansion": "has_recent_cw_card",
        "Bill Pay Expansion": "has_recent_cw_billpay",
        "Treasury Expansion": "has_recent_cw_treasury",
        "Travel Expansion": "has_recent_cw_travel",
    }

    with_opp: list[dict] = []
    missing_opp: list[dict] = []
    # Track account+product combos already seen to avoid duplicates
    # (billpay has two date cols that map to the same product)
    seen: set[tuple[str, str]] = set()

    for _, row in signals_df.iterrows():
        account_id = row.get("account_id", "")
        account_name = row.get("account_name", "Unknown")

        for date_col, opp_flag, product, spend_col in _PRODUCT_CHECKS:
            act_date = row.get(date_col)
            # Skip if no activation date
            if act_date is None or (not isinstance(act_date, str) and pd.isna(act_date)):
                continue

            # Dedup account+product within this run
            key = (account_id, product)
            if key in seen:
                continue
            seen.add(key)

            # Skip if already closed-won for this product in last 90 days
            cw_flag = _CW_FLAGS.get(product, "")
            if cw_flag and _safe_int(row.get(cw_flag)) == 1:
                continue

            spend = _safe_float(row.get(spend_col)) if spend_col else 0.0
            has_opp = _safe_int(row.get(opp_flag)) == 1
            recent = _is_recent(act_date)

            item = {
                "account_name": account_name,
                "account_id": account_id,
                "product": product,
                "activated_at": str(act_date),
                "l30d_spend": format_currency(spend),
                "l30d_spend_raw": spend,
            }

            if has_opp and recent:
                with_opp.append(item)
            elif not has_opp:
                # Missing opp section shows all activations (full 60-day window)
                missing_opp.append(item)

    # Sort by L30D spend ascending — lowest baseline = most urgency to close early
    with_opp.sort(key=lambda x: x["l30d_spend_raw"])
    missing_opp.sort(key=lambda x: x["l30d_spend_raw"])

    return with_opp, missing_opp


def _build_blocks(date_str: str, with_opp: list[dict], missing_opp: list[dict]) -> list[dict]:
    """Assemble Slack Block Kit blocks for the activation alert."""
    blocks: list[dict] = []

    # Header
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"Zero-to-One Activations \u2014 {date_str}",
            "emoji": True,
        },
    })

    # ── With open opp section (close now) ─────────────────────────────
    if with_opp:
        lines = [f"*\u26a1 Close Now — Activation + Open Opp* ({len(with_opp)})"]
        for item in with_opp:
            sf_link = sf_account_url(item["account_id"])
            name_link = f"<{sf_link}|{item['account_name']}>"
            product = item["product"]
            event = _PRODUCT_EVENT_LABELS.get(product, "Activation")
            lines.append(f"\u26a1 {name_link} — {product}")
            lines.append(f"      Event: {event} | L30D: {item['l30d_spend']} (becomes baseline at close)")
            lines.append(f"      _Close now \u2014 every day you wait, baseline rises and CP headroom shrinks._")
            if product in _BOOMERANG_PRODUCTS:
                lines.append(f"      \U0001f4cc _Boomerang: hand off to CSM at S4, you keep CW credit._")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        })
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\u2705 No new activations with open opps in the last 7 days."},
        })

    blocks.append({"type": "divider"})

    # ── Missing opp section ───────────────────────────────────────────
    if missing_opp:
        lines = [f"*\u26a0\ufe0f Missing Opps — Activation, No Open Opp* ({len(missing_opp)})"]
        for item in missing_opp:
            sf_link = sf_account_url(item["account_id"])
            name_link = f"<{sf_link}|{item['account_name']}>"
            product = item["product"]
            event = _PRODUCT_EVENT_LABELS.get(product, "Activation")
            create_url = _build_create_opp_url(item["account_id"], product)
            lines.append(f"\u26a0\ufe0f {name_link} — {product}")
            lines.append(f"      Event: {event} | L30D: {item['l30d_spend']} (becomes baseline at close)")
            lines.append(f"      _Create + close opp now. CP = growth above {item['l30d_spend']} over 90 days._")
            lines.append(f"      <{create_url}|Create Opp>")
            if product in _BOOMERANG_PRODUCTS:
                lines.append(f"      \U0001f4cc _Boomerang: hand off to CSM at S4, you keep CW credit._")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        })
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "No activations without opps."},
        })

    # Dashboard links footer
    _priority_link = dashboard_url("priority", tab="zero-to-one")
    _pipeline_link = dashboard_url("pipeline", tab="zero-to-one")
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"<{_priority_link}|View in Priority Actions> · "
                f"<{_pipeline_link}|View in Pipeline> · "
                "`/zero-to-one` to refresh"
            ),
        }],
    })

    return blocks


def run_zero_to_one(client, user_id=None, force: bool = False):
    """Run zero-to-one activation alert and DM Greg.

    Parameters
    ----------
    client : slack_sdk.WebClient
        Authenticated Slack client for posting messages.
    force : bool
        When True, send the report even if nothing found (slash command use).
    """
    dm_target = user_id or GREG_SLACK_ID

    try:
        signals_df = run_query(format_query(SIGNALS_QUERY, user_id=user_id))
        if signals_df.empty:
            if force:
                from core.slack_formatter import simple_dm_blocks
                blocks = simple_dm_blocks(
                    "Zero-to-One Activations",
                    "\u2705 All clear \u2014 no new activations with open opps.",
                )
                client.chat_postMessage(
                    channel=dm_target, blocks=blocks,
                    text="Zero-to-One: all clear",
                )
            else:
                logger.info("Zero-to-one: no signals data returned")
            return

        with_opp, missing_opp = _extract_activations(signals_df)

        # Dedup: only send items not already alerted
        new_with_opp = []
        for item in with_opp:
            dedup_key = f"zero_to_one_{item['account_id']}_{item['product']}"
            if not tracker.is_processed(dedup_key, user_id=user_id):
                new_with_opp.append(item)
                tracker.mark_processed(dedup_key, user_id=user_id)

        new_missing_opp = []
        for item in missing_opp:
            dedup_key = f"zero_to_one_missing_{item['account_id']}_{item['product']}"
            if not tracker.is_processed(dedup_key, user_id=user_id):
                new_missing_opp.append(item)
                tracker.mark_processed(dedup_key, user_id=user_id)

        # Skip if nothing new (unless forced)
        if not new_with_opp and not new_missing_opp and not force:
            logger.info("Zero-to-one: no new activations to report")
            return

        # Use deduped lists for the alert (or full lists if forced)
        report_with = new_with_opp if new_with_opp else (with_opp if force else [])
        report_missing = new_missing_opp if new_missing_opp else (missing_opp if force else [])

        if not report_with and not report_missing and force:
            from core.slack_formatter import simple_dm_blocks
            blocks = simple_dm_blocks(
                "Zero-to-One Activations",
                "\u2705 All clear \u2014 no new activations with open opps.",
            )
            client.chat_postMessage(
                channel=dm_target, blocks=blocks,
                text="Zero-to-One: all clear",
            )
            return

        date_str = datetime.now().strftime("%b %d, %Y")
        blocks = _build_blocks(date_str, report_with, report_missing)

        fallback = (
            f"Zero-to-One: {len(report_with)} with open opps, "
            f"{len(report_missing)} missing opps"
        )
        client.chat_postMessage(
            channel=dm_target,
            blocks=blocks,
            text=fallback,
        )
        logger.info(
            "Zero-to-one sent: %d with open opps, %d missing opps",
            len(report_with), len(report_missing),
        )

    except Exception as e:
        logger.error("Zero-to-one job failed: %s", e)
