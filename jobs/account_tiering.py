"""Smart Account Tiering — score and rank accounts by CP potential.

Scores every account across multiple signals:
  - Spend growth rate (L30D vs prev 30D, by product)
  - Product whitespace (activated products without open/recent CW opps)
  - Engagement signals (recent Gong calls, emails)
  - Open opp stage advancement

Maintains a dynamic Top 50 focus list. Surfaces accounts that suddenly
become interesting (spend spikes, new activations).

Triggered by DM "top accounts" / "account tiering" / "focus list".
"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from core.snowflake_client import run_query
from core.slack_formatter import format_currency, sf_account_url, dashboard_url
from config import GREG_SLACK_ID, NTR_RATES, OWNER_NAME

logger = logging.getLogger(__name__)

TIERING_QUERY = f"""
WITH greg_accounts AS (
    SELECT DISTINCT ledger.account_id, sa.account_name, sa.business_id
    FROM analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = ledger.account_id
    WHERE ledger.date_day = CURRENT_DATE - 1
      AND ledger.owner_name = '{OWNER_NAME}'
),
spend AS (
    SELECT
        ga.account_id,
        SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.card_tpv ELSE 0 END)     AS card_l30d,
        SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.card_tpv ELSE 0 END) AS card_prev30d,
        SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.billpay_tpv ELSE 0 END)   AS bp_l30d,
        SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.billpay_tpv ELSE 0 END) AS bp_prev30d,
        SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.travel_tpv ELSE 0 END)    AS travel_l30d,
        SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.travel_tpv ELSE 0 END) AS travel_prev30d,
        AVG(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.treasury_available_balance ELSE NULL END) AS treasury_l30d,
        AVG(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.treasury_available_balance ELSE NULL END) AS treasury_prev30d
    FROM greg_accounts ga
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = ga.business_id
        AND tpv.date_day >= CURRENT_DATE - 60
    GROUP BY ga.account_id
),
activations AS (
    SELECT
        ga.account_id,
        bob.card_fifth_transaction_cleared_at IS NOT NULL AS card_activated,
        bob.first_bill_paid_at IS NOT NULL AS bp_activated,
        bob.treasury_first_payment_at IS NOT NULL AS treasury_activated,
        bob.travel_fifth_booking_at IS NOT NULL AS travel_activated
    FROM greg_accounts ga
    LEFT JOIN analytics.marts.dim_book_of_business_accounts_view bob
        ON bob.sfdc_account_id = ga.account_id
),
open_opps AS (
    SELECT account_id, expansion_subtype,
           MAX(opportunity_stage_name) AS max_stage
    FROM analytics.marts.dim_sfdc_opportunities
    WHERE opportunity_is_closed = FALSE
      AND opportunity_type = 'Expansion'
      AND opportunity_owner = '{OWNER_NAME}'
      AND opportunity_stage_name != 'S0: Holding'
    GROUP BY account_id, expansion_subtype
),
recent_cw AS (
    SELECT DISTINCT account_id, expansion_subtype
    FROM analytics.marts.dim_sfdc_opportunities
    WHERE opportunity_is_won = TRUE
      AND opportunity_type = 'Expansion'
      AND opportunity_owner = '{OWNER_NAME}'
      AND opportunity_close_date >= CURRENT_DATE - 90
),
engagement AS (
    SELECT
        ga.account_id,
        COUNT(DISTINCT CASE WHEN gt.call_start >= CURRENT_DATE - 30 THEN gt.call_id END) AS calls_l30d,
        MAX(gt.call_start)::date AS last_call_date
    FROM greg_accounts ga
    LEFT JOIN analytics.marts.dim_sfdc_gong_transcripts gt
        ON gt.account_id = ga.account_id
        AND gt.call_start >= CURRENT_DATE - 90
        AND gt.call_duration_sec >= 180
    GROUP BY ga.account_id
),
email_engagement AS (
    SELECT
        ga.account_id,
        COUNT(CASE WHEN e.sfdc_email_created_at >= CURRENT_DATE - 30 THEN 1 END) AS emails_l30d,
        COUNT(CASE WHEN e.sfdc_email_created_at >= CURRENT_DATE - 30
                    AND e.sfdc_email_owner_email = 'gnallie@ramp.com' THEN 1 END) AS greg_emails_l30d,
        COUNT(DISTINCT e.sfdc_email_owner_email) AS ramp_senders,
        COUNT(CASE WHEN e.has_painpoints THEN 1 END) AS painpoint_mentions,
        COUNT(CASE WHEN e.has_interested THEN 1 END) AS interest_signals,
        COUNT(CASE WHEN e.has_not_interested THEN 1 END) AS not_interested_signals,
        MAX(e.sfdc_email_created_at)::date AS last_email_date
    FROM greg_accounts ga
    LEFT JOIN analytics.marts.dim_emails e
        ON e.account_id = ga.account_id
        AND e.sfdc_email_created_at >= CURRENT_DATE - 90
    GROUP BY ga.account_id
)
SELECT
    ga.account_name, ga.account_id,
    -- Spend
    ROUND(COALESCE(s.card_l30d, 0)) AS card_l30d,
    ROUND(COALESCE(s.card_prev30d, 0)) AS card_prev30d,
    ROUND(COALESCE(s.bp_l30d, 0)) AS bp_l30d,
    ROUND(COALESCE(s.bp_prev30d, 0)) AS bp_prev30d,
    ROUND(COALESCE(s.travel_l30d, 0)) AS travel_l30d,
    ROUND(COALESCE(s.travel_prev30d, 0)) AS travel_prev30d,
    ROUND(COALESCE(s.treasury_l30d, 0)) AS treasury_l30d,
    ROUND(COALESCE(s.treasury_prev30d, 0)) AS treasury_prev30d,
    -- Activations
    COALESCE(a.card_activated, FALSE) AS card_activated,
    COALESCE(a.bp_activated, FALSE) AS bp_activated,
    COALESCE(a.treasury_activated, FALSE) AS treasury_activated,
    COALESCE(a.travel_activated, FALSE) AS travel_activated,
    -- Open opps (presence flags)
    CASE WHEN oo_card.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_card_opp,
    CASE WHEN oo_bp.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_bp_opp,
    CASE WHEN oo_travel.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_travel_opp,
    CASE WHEN oo_treasury.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_treasury_opp,
    -- Recent CW flags
    CASE WHEN cw_card.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS recent_cw_card,
    CASE WHEN cw_bp.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS recent_cw_bp,
    CASE WHEN cw_travel.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS recent_cw_travel,
    CASE WHEN cw_treasury.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS recent_cw_treasury,
    -- Engagement
    COALESCE(eng.calls_l30d, 0) AS calls_l30d,
    eng.last_call_date,
    COALESCE(ee.emails_l30d, 0) AS emails_l30d,
    ee.last_email_date
FROM greg_accounts ga
LEFT JOIN spend s ON s.account_id = ga.account_id
LEFT JOIN activations a ON a.account_id = ga.account_id
LEFT JOIN open_opps oo_card ON oo_card.account_id = ga.account_id AND oo_card.expansion_subtype = 'Card Expansion'
LEFT JOIN open_opps oo_bp ON oo_bp.account_id = ga.account_id AND oo_bp.expansion_subtype = 'Bill Pay Expansion'
LEFT JOIN open_opps oo_travel ON oo_travel.account_id = ga.account_id AND oo_travel.expansion_subtype = 'Travel Expansion'
LEFT JOIN open_opps oo_treasury ON oo_treasury.account_id = ga.account_id AND oo_treasury.expansion_subtype = 'Treasury Expansion'
LEFT JOIN recent_cw cw_card ON cw_card.account_id = ga.account_id AND cw_card.expansion_subtype = 'Card Expansion'
LEFT JOIN recent_cw cw_bp ON cw_bp.account_id = ga.account_id AND cw_bp.expansion_subtype = 'Bill Pay Expansion'
LEFT JOIN recent_cw cw_travel ON cw_travel.account_id = ga.account_id AND cw_travel.expansion_subtype = 'Travel Expansion'
LEFT JOIN recent_cw cw_treasury ON cw_treasury.account_id = ga.account_id AND cw_treasury.expansion_subtype = 'Treasury Expansion'
LEFT JOIN engagement eng ON eng.account_id = ga.account_id
LEFT JOIN email_engagement ee ON ee.account_id = ga.account_id
WHERE COALESCE(s.card_l30d, 0) + COALESCE(s.bp_l30d, 0) + COALESCE(s.travel_l30d, 0) + COALESCE(s.treasury_l30d, 0) > 0
ORDER BY COALESCE(s.card_l30d, 0) + COALESCE(s.bp_l30d, 0) + COALESCE(s.travel_l30d, 0) + COALESCE(s.treasury_l30d, 0) DESC
LIMIT 500
"""


def _safe_float(val, default=0.0):
    try:
        v = float(val)
        return default if pd.isna(v) else v
    except (TypeError, ValueError):
        return default


def _score_account(row) -> tuple[float, list[str]]:
    """Score an account by CP potential. Returns (score, reasons list)."""
    score = 0.0
    reasons = []

    # Spend growth scores per product
    _products = [
        ("card", "Card", 0.0095, "card_l30d", "card_prev30d", "card_activated", "has_card_opp", "recent_cw_card"),
        ("bp", "Bill Pay", 0.0015, "bp_l30d", "bp_prev30d", "bp_activated", "has_bp_opp", "recent_cw_bp"),
        ("travel", "Travel", 0.035, "travel_l30d", "travel_prev30d", "travel_activated", "has_travel_opp", "recent_cw_travel"),
        ("treasury", "Treasury", 0.0005, "treasury_l30d", "treasury_prev30d", "treasury_activated", "has_treasury_opp", "recent_cw_treasury"),
    ]

    for key, label, ntr, l30d_col, prev_col, act_col, opp_col, cw_col in _products:
        l30d = _safe_float(row.get(l30d_col))
        prev = _safe_float(row.get(prev_col))
        activated = bool(row.get(act_col, False))
        has_opp = bool(row.get(opp_col, False))
        recent_cw = bool(row.get(cw_col, False))

        # Spend growth (weighted by NTR)
        if l30d > prev > 0:
            delta = l30d - prev
            cp_value = delta * ntr * 3
            score += cp_value
            if delta > prev * 0.15 and delta > 2000:
                reasons.append(f"{label} +{int(100 * delta / prev)}%")

        # Product whitespace: activated but no opp and no recent CW
        if activated and not has_opp and not recent_cw:
            whitespace_score = l30d * ntr * 3 * 0.5  # conservative estimate
            score += max(whitespace_score, 5)
            reasons.append(f"{label} whitespace")

    # Engagement bonus
    calls = int(_safe_float(row.get("calls_l30d")))
    emails = int(_safe_float(row.get("emails_l30d")))
    if calls > 0:
        score += calls * 3
    if emails > 0:
        score += min(emails, 5) * 1

    return score, reasons


def run_account_tiering(client, force: bool = False):
    """Generate and send the Top 50 focus list ranked by CP potential."""
    try:
        df = run_query(TIERING_QUERY)
        if df.empty:
            if force:
                client.chat_postMessage(channel=GREG_SLACK_ID, text="No accounts with spend data found.")
            return

        # Score all accounts
        scored = []
        for _, row in df.iterrows():
            score, reasons = _score_account(row)
            if score > 0:
                scored.append({
                    "account": row.get("account_name", "Unknown"),
                    "account_id": row.get("account_id", ""),
                    "score": score,
                    "reasons": reasons,
                    "card_l30d": _safe_float(row.get("card_l30d")),
                    "bp_l30d": _safe_float(row.get("bp_l30d")),
                    "calls_l30d": int(_safe_float(row.get("calls_l30d"))),
                    "emails_l30d": int(_safe_float(row.get("emails_l30d"))),
                })

        scored.sort(key=lambda x: -x["score"])
        top = scored[:50]

        if not top and not force:
            return

        blocks = [{
            "type": "header",
            "text": {"type": "plain_text", "text": f"\U0001f3c6 Top {min(50, len(top))} Focus Accounts", "emoji": True},
        }]

        total_potential = sum(a["score"] for a in top)
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{len(top)} accounts* ranked by CP potential | Total est. opportunity: *{format_currency(total_potential)}*",
            },
        })
        blocks.append({"type": "divider"})

        # Show top 20 in detail
        for i, acct in enumerate(top[:20], 1):
            sf_link = sf_account_url(acct["account_id"])
            reasons_str = ", ".join(acct["reasons"][:3]) if acct["reasons"] else "steady spend"
            spend_parts = []
            if acct["card_l30d"] > 0:
                spend_parts.append(f"Card: {format_currency(acct['card_l30d'])}")
            if acct["bp_l30d"] > 0:
                spend_parts.append(f"BP: {format_currency(acct['bp_l30d'])}")
            spend_str = " | ".join(spend_parts) if spend_parts else ""

            engagement = ""
            if acct["calls_l30d"] > 0 or acct["emails_l30d"] > 0:
                parts = []
                if acct["calls_l30d"]:
                    parts.append(f"{acct['calls_l30d']} calls")
                if acct["emails_l30d"]:
                    parts.append(f"{acct['emails_l30d']} emails")
                engagement = f" | {', '.join(parts)} L30D"

            line = (
                f"*{i}.* <{sf_link}|{acct['account']}> — ~{format_currency(acct['score'])} CP potential\n"
                f"      {reasons_str} | {spend_str}{engagement}"
            )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": line},
            })

        if len(top) > 20:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"_...and {len(top) - 20} more. DM me `tell me about [account]` for details._"},
            })

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": "Scored by: spend growth x NTR, product whitespace, engagement. DM `top accounts` to refresh.",
            }],
        })

        client.chat_postMessage(
            channel=GREG_SLACK_ID,
            blocks=blocks,
            text=f"Top {len(top)} Focus Accounts — {format_currency(total_potential)} CP potential",
        )
        logger.info("Account tiering sent: %d accounts, %s potential CP", len(top), format_currency(total_potential))

    except Exception as e:
        logger.error("Account tiering failed: %s", e)
        if force:
            client.chat_postMessage(channel=GREG_SLACK_ID, text=f"Account tiering failed: {e}")
