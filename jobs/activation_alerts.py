"""Activation Alerts — detect new treasury, investment, and first-bill activations.

Runs every 2 hours during business hours.  Queries the Book of Business view
for accounts where:
  1. Treasury was funded in the last 48 hours (TREASURY_FUNDED_DATE)
  2. Investment account opened (HAS_INVESTMENT_ACCOUNT = TRUE, dedup-tracked)
  3. First bill created in the last 48 hours (FIRST_BILL_CREATED_AT)

DMs user immediately when new activations are detected.  Results also surface
in the Prospecting tab under dedicated signal types.
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from core.snowflake_client import run_query
from queries.queries import format_query
from core.slack_formatter import sf_account_url, format_currency
from config import SF_BASE_URL
from utils.dedup import tracker

logger = logging.getLogger(__name__)

# ── Snowflake Query ─────────────────────────────────────────────────────────
_ACTIVATION_QUERY = """
WITH my_accounts AS (
    SELECT DISTINCT account_id
    FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1
      AND owner_name = '__OWNER__'
)
SELECT
    sa.account_id,
    sa.account_name,
    sa.business_id,
    bob.treasury_funded_date,
    bob.has_investment_account,
    bob.investment_account_available_balance,
    bob.first_bill_created_at,
    bob.first_bill_paid_at,
    bob.treasury_available_balance,
    bob.is_treasury_active,
    bob.rolling_30_days_avg_treasury_available_balance_usd AS treasury_balance_l30d,
    bob.thirty_day_card_spend AS card_spend_l30d,
    bob.rolling_30_day_paid_bill_amount AS billpay_spend_l30d,
    bob.current_gla,
    bob.plus_product_status_v2,
    bob.primary_customer_poc_email,
    bob.primary_ramp_champion_email,
    bob.primary_ramp_champion_name
FROM my_accounts ma
JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = ma.account_id
LEFT JOIN analytics.marts.dim_book_of_business_accounts_view bob
    ON bob.sfdc_account_id = sa.account_id
WHERE sa.account_status = 'Active'
  AND (
    -- Treasury funded in last 7 days (dedup prevents re-alerting)
    bob.treasury_funded_date >= DATEADD('day', -7, CURRENT_DATE)
    -- Has investment account with balance (dedup-only, no date column available)
    OR (bob.has_investment_account = TRUE AND bob.investment_account_available_balance > 0)
    -- First bill created in last 7 days
    OR bob.first_bill_created_at >= DATEADD('day', -7, CURRENT_DATE)
  )
ORDER BY sa.account_name
"""


def _safe_float(val, default=0.0):
    """Convert value to float, handling None, NaN, and strings."""
    if val is None:
        return default
    try:
        f = float(val)
        if f != f:  # NaN check
            return default
        return f
    except (ValueError, TypeError):
        return default


def _is_recent(val, hours=48):
    """Return True if date/datetime val is within the last N hours."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return False
    try:
        ts = pd.Timestamp(val)
        if pd.isna(ts):
            return False
        cutoff = pd.Timestamp.now() - pd.Timedelta(hours=hours)
        return ts >= cutoff
    except Exception:
        return False


# ── Signal Types ────────────────────────────────────────────────────────────
ACTIVATION_SIGNAL_META = {
    "new_treasury": {
        "emoji": ":bank:",
        "label": "New Treasury Activation",
    },
    "new_investment": {
        "emoji": ":chart_with_upwards_trend:",
        "label": "Investment Account Opened",
    },
    "first_bill": {
        "emoji": ":receipt:",
        "label": "First Bill Created",
    },
}


def detect_activations(user_id: Optional[str] = None) -> list[dict]:
    """Query Snowflake and return new activation events, deduped."""
    try:
        sql = format_query(_ACTIVATION_QUERY, user_id=user_id)
        df = run_query(sql)
    except Exception as e:
        logger.error("Activation alerts query failed: %s", e)
        return []

    if df.empty:
        return []

    alerts = []
    uid = user_id or "default"

    for _, row in df.iterrows():
        row_dict = row.to_dict()
        account_id = row_dict.get("account_id", "")
        account_name = row_dict.get("account_name", "Unknown")
        business_id = row_dict.get("business_id", "")

        base_info = {
            "account": account_name,
            "account_id": account_id,
            "business_id": business_id,
            "card_spend_l30d": _safe_float(row_dict.get("card_spend_l30d")),
            "billpay_spend_l30d": _safe_float(row_dict.get("billpay_spend_l30d")),
            "treasury_balance_l30d": _safe_float(row_dict.get("treasury_balance_l30d")),
            "current_gla": _safe_float(row_dict.get("current_gla")),
            "plus_status": row_dict.get("plus_product_status_v2") or "",
            "poc_email": row_dict.get("primary_customer_poc_email") or "",
            "champion_email": row_dict.get("primary_ramp_champion_email") or "",
            "champion_name": row_dict.get("primary_ramp_champion_name") or "",
        }

        # 1. Treasury funded in last 7 days
        if _is_recent(row_dict.get("treasury_funded_date"), hours=168):
            dedup_key = f"activation_treasury_{account_id}"
            if not tracker.is_processed(dedup_key, user_id=uid):
                treasury_bal = _safe_float(row_dict.get("treasury_available_balance"))
                alerts.append({
                    **base_info,
                    "signal_key": "new_treasury",
                    "signal_label": "New Treasury Activation",
                    "signal_detail": f"Treasury funded — balance: ${treasury_bal:,.0f}",
                    "treasury_balance": treasury_bal,
                })
                tracker.mark_processed(dedup_key, user_id=uid)

        # 2. Investment account opened (only alert if balance > 0 — filters out empty/inactive)
        has_investment = row_dict.get("has_investment_account")
        if has_investment is True or str(has_investment).lower() == "true":
            inv_balance = _safe_float(row_dict.get("investment_account_available_balance"))
            if inv_balance > 0:
                dedup_key = f"activation_investment_{account_id}"
                if not tracker.is_processed(dedup_key, user_id=uid):
                    alerts.append({
                        **base_info,
                        "signal_key": "new_investment",
                        "signal_label": "Investment Account Opened",
                        "signal_detail": f"Investment account active — balance: ${inv_balance:,.0f}",
                        "investment_balance": inv_balance,
                    })
                    tracker.mark_processed(dedup_key, user_id=uid)

        # 3. First bill created in last 7 days
        if _is_recent(row_dict.get("first_bill_created_at"), hours=168):
            dedup_key = f"activation_first_bill_{account_id}"
            if not tracker.is_processed(dedup_key, user_id=uid):
                alerts.append({
                    **base_info,
                    "signal_key": "first_bill",
                    "signal_label": "First Bill Created",
                    "signal_detail": "First bill just created — engagement starting",
                })
                tracker.mark_processed(dedup_key, user_id=uid)

    return alerts


def run_activation_alerts(client, user_id: Optional[str] = None):
    """Scheduled job entry point: check for new activations and DM user."""
    alerts = detect_activations(user_id=user_id)
    if not alerts:
        return

    logger.info("Activation alerts: %d new activations for %s", len(alerts), user_id)

    # Build DM blocks
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": (
            f":rotating_light: *New Activation Alert{'s' if len(alerts) > 1 else ''}*\n"
            f"{len(alerts)} account{'s' if len(alerts) > 1 else ''} just hit a product milestone:"
        )}},
    ]

    for alert in alerts[:10]:  # Cap DM at 10 alerts
        meta = ACTIVATION_SIGNAL_META.get(alert["signal_key"], {})
        emoji = meta.get("emoji", ":bell:")
        acct = alert["account"]
        acct_id = alert["account_id"]
        sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
        acct_str = f"<{sf_link}|{acct}>" if sf_link else acct

        # Spend context
        spend_parts = []
        card = alert.get("card_spend_l30d", 0)
        bp = alert.get("billpay_spend_l30d", 0)
        gla = alert.get("current_gla", 0)
        if card > 0:
            spend_parts.append(f"Card L30D: ${card:,.0f}")
        if bp > 0:
            spend_parts.append(f"BP L30D: ${bp:,.0f}")
        if gla > 0:
            spend_parts.append(f"GLA: ${gla:,.0f}")
        spend_str = " · ".join(spend_parts) if spend_parts else "No recent spend"

        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": (
            f"{emoji} *{acct_str}*\n"
            f"{alert['signal_detail']}\n"
            f"{spend_str}"
        )}})

    blocks.append({"type": "context", "elements": [
        {"type": "mrkdwn", "text": "_Open :house: Home → Prospecting to draft outreach_"},
    ]})

    try:
        dm_target = user_id or "U06DAFU4YRG"
        client.chat_postMessage(channel=dm_target, blocks=blocks, text="New activation alerts")
    except Exception as e:
        logger.error("Failed to DM activation alerts: %s", e)
