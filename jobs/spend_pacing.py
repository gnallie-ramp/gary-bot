"""Spend Pacing Intelligence — historical and seasonal spend comparison.

Compares MTD spend at this point in the month vs. the same point last month
and the same month last year. Breaks down by product (Card, Bill Pay).
Flags significant deltas and seasonal patterns.

Triggered by DM "spend pacing" or scheduled as part of priority actions.
"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from core.snowflake_client import run_query
from core.slack_formatter import format_currency, sf_account_url, sf_opp_url, dashboard_url
from config import GREG_SLACK_ID, NTR_RATES, OWNER_NAME

logger = logging.getLogger(__name__)


# ── Spend pacing query: MTD vs same point last month + YoY ────────────────

SPEND_PACING_QUERY = f"""
WITH greg_opps AS (
    SELECT
        opp.account_id, opp.opportunity_id, opp.expansion_subtype,
        opp.opportunity_stage_name, opp.opportunity_close_date,
        sa.account_name, sa.business_id,
        opp.opportunity_created_at::date AS created_date
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = '{OWNER_NAME}'
      AND opp.opportunity_stage_name != 'S0: Holding'
      AND opp.expansion_subtype IN ('Card Expansion', 'Bill Pay Expansion', 'Travel Expansion', 'Treasury Expansion')
),
month_meta AS (
    SELECT
        DATE_TRUNC('month', CURRENT_DATE)                              AS month_start,
        DATEDIFF('day', DATE_TRUNC('month', CURRENT_DATE), CURRENT_DATE) AS days_elapsed,
        DAY(LAST_DAY(CURRENT_DATE))                                    AS days_in_month
),
-- Current MTD spend
current_mtd AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS mtd_spend
    FROM greg_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= DATE_TRUNC('month', CURRENT_DATE)
        AND tpv.date_day < CURRENT_DATE
    GROUP BY g.opportunity_id, g.expansion_subtype
),
-- Same point last month (same # of days elapsed)
last_month_same_point AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS last_month_spend
    FROM greg_opps g
    CROSS JOIN month_meta mm
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= DATEADD('month', -1, mm.month_start)
        AND tpv.date_day < DATEADD('day', mm.days_elapsed, DATEADD('month', -1, mm.month_start))
    GROUP BY g.opportunity_id, g.expansion_subtype
),
-- Full last month (for monthly comparison)
last_month_full AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS full_month_spend
    FROM greg_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE))
        AND tpv.date_day < DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY g.opportunity_id, g.expansion_subtype
),
-- Same month last year, same # of days elapsed (seasonality)
yoy_same_point AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS yoy_spend
    FROM greg_opps g
    CROSS JOIN month_meta mm
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= DATEADD('year', -1, mm.month_start)
        AND tpv.date_day < DATEADD('day', mm.days_elapsed, DATEADD('year', -1, mm.month_start))
    GROUP BY g.opportunity_id, g.expansion_subtype
),
-- L7D spend (for weekly trajectory)
recent_7d AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS l7d_spend
    FROM greg_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= CURRENT_DATE - 7
    GROUP BY g.opportunity_id, g.expansion_subtype
),
prev_7d AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS prev_7d_spend
    FROM greg_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day BETWEEN CURRENT_DATE - 14 AND CURRENT_DATE - 8
    GROUP BY g.opportunity_id, g.expansion_subtype
)
SELECT
    g.account_name, g.account_id, g.opportunity_id,
    g.expansion_subtype, g.opportunity_stage_name,
    mm.days_elapsed, mm.days_in_month,
    ROUND(COALESCE(cm.mtd_spend, 0))         AS mtd_spend,
    ROUND(COALESCE(lm.last_month_spend, 0))  AS last_month_same_point,
    ROUND(COALESCE(lf.full_month_spend, 0))  AS last_month_full,
    ROUND(COALESCE(yy.yoy_spend, 0))         AS yoy_same_point,
    ROUND(COALESCE(r7.l7d_spend, 0))         AS l7d_spend,
    ROUND(COALESCE(p7.prev_7d_spend, 0))     AS prev_7d_spend,
    -- Paced monthly projection
    ROUND(COALESCE(cm.mtd_spend, 0) * mm.days_in_month / NULLIF(mm.days_elapsed, 0)) AS paced_monthly
FROM greg_opps g
CROSS JOIN month_meta mm
LEFT JOIN current_mtd cm          ON cm.opportunity_id = g.opportunity_id
LEFT JOIN last_month_same_point lm ON lm.opportunity_id = g.opportunity_id
LEFT JOIN last_month_full lf      ON lf.opportunity_id = g.opportunity_id
LEFT JOIN yoy_same_point yy       ON yy.opportunity_id = g.opportunity_id
LEFT JOIN recent_7d r7            ON r7.opportunity_id = g.opportunity_id
LEFT JOIN prev_7d p7              ON p7.opportunity_id = g.opportunity_id
WHERE COALESCE(cm.mtd_spend, 0) > 0 OR COALESCE(lm.last_month_spend, 0) > 0
ORDER BY COALESCE(cm.mtd_spend, 0) DESC
"""


def _safe_float(val, default=0.0):
    try:
        v = float(val)
        return default if pd.isna(v) else v
    except (TypeError, ValueError):
        return default


def _pct_change(current, previous):
    """Return percentage change string."""
    if previous <= 0:
        return "+new" if current > 0 else "flat"
    pct = ((current - previous) / previous) * 100
    if pct > 0:
        return f"+{pct:.0f}%"
    return f"{pct:.0f}%"


def _trajectory_icon(l7d, prev_7d):
    """Return trajectory emoji based on week-over-week change."""
    if prev_7d <= 0:
        return "\u2796"  # neutral
    ratio = l7d / prev_7d
    if ratio >= 1.2:
        return "\U0001f680"  # rocket (accelerating)
    elif ratio >= 1.05:
        return "\u2197\ufe0f"  # up-right
    elif ratio >= 0.95:
        return "\u27a1\ufe0f"  # flat
    elif ratio >= 0.8:
        return "\u2198\ufe0f"  # down-right
    else:
        return "\U0001f4c9"  # declining


def run_spend_pacing(client, force: bool = False):
    """Generate and send the spend pacing intelligence report."""
    try:
        df = run_query(SPEND_PACING_QUERY)
        if df.empty:
            if force:
                client.chat_postMessage(
                    channel=GREG_SLACK_ID,
                    text="No open opps with spend data found for pacing report.",
                )
            return

        days_elapsed = int(df.iloc[0].get("days_elapsed", 0))
        days_in_month = int(df.iloc[0].get("days_in_month", 30))
        month_name = datetime.now().strftime("%B")

        blocks = [{
            "type": "header",
            "text": {"type": "plain_text", "text": f"\U0001f4ca Spend Pacing — {month_name} Day {days_elapsed}/{days_in_month}", "emoji": True},
        }]

        # Group by signals: accelerating, decelerating, seasonal high, seasonal low
        accelerating = []
        decelerating = []
        seasonal_high = []

        for _, row in df.iterrows():
            mtd = _safe_float(row.get("mtd_spend"))
            lm_same = _safe_float(row.get("last_month_same_point"))
            lm_full = _safe_float(row.get("last_month_full"))
            yoy = _safe_float(row.get("yoy_same_point"))
            l7d = _safe_float(row.get("l7d_spend"))
            prev_7d = _safe_float(row.get("prev_7d_spend"))
            paced = _safe_float(row.get("paced_monthly"))
            product = row.get("expansion_subtype", "")
            ntr = NTR_RATES.get(product, 0.0095)

            item = {
                "account": row.get("account_name", "Unknown"),
                "account_id": row.get("account_id", ""),
                "opp_id": row.get("opportunity_id", ""),
                "product": product,
                "stage": row.get("opportunity_stage_name", ""),
                "mtd": mtd,
                "lm_same": lm_same,
                "lm_full": lm_full,
                "yoy": yoy,
                "l7d": l7d,
                "prev_7d": prev_7d,
                "paced": paced,
                "trajectory": _trajectory_icon(l7d, prev_7d),
                "mom_pct": _pct_change(mtd, lm_same),
                "yoy_pct": _pct_change(mtd, yoy) if yoy > 0 else "N/A",
                "est_cp_if_close": max(0, paced - lm_full) * ntr * 3 if paced > lm_full else 0,
            }

            # Classify
            if mtd > lm_same * 1.15 and mtd - lm_same > 3000:
                accelerating.append(item)
            elif mtd < lm_same * 0.85 and lm_same > 3000:
                decelerating.append(item)
            if yoy > 0 and mtd > yoy * 1.3 and mtd - yoy > 5000:
                seasonal_high.append(item)

        # ── Accelerating ──
        if accelerating:
            accelerating.sort(key=lambda x: -(x["mtd"] - x["lm_same"]))
            lines = [f"\U0001f680 *Accelerating vs Last Month ({len(accelerating)})*"]
            for item in accelerating[:8]:
                sf_link = sf_opp_url(item["opp_id"]) if item["opp_id"] else sf_account_url(item["account_id"])
                product_short = item["product"].replace(" Expansion", "")
                cp_str = f" | ~{format_currency(item['est_cp_if_close'])} CP" if item["est_cp_if_close"] > 0 else ""
                lines.append(
                    f"  {item['trajectory']} <{sf_link}|{item['account']}> — {product_short}\n"
                    f"      MTD: {format_currency(item['mtd'])} ({item['mom_pct']} vs last month) | "
                    f"Paced: {format_currency(item['paced'])}{cp_str}"
                )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)},
            })

        # ── Decelerating ──
        if decelerating:
            decelerating.sort(key=lambda x: x["mtd"] - x["lm_same"])
            lines = [f"\U0001f4c9 *Decelerating vs Last Month ({len(decelerating)})*"]
            for item in decelerating[:5]:
                sf_link = sf_opp_url(item["opp_id"]) if item["opp_id"] else sf_account_url(item["account_id"])
                product_short = item["product"].replace(" Expansion", "")
                lines.append(
                    f"  {item['trajectory']} <{sf_link}|{item['account']}> — {product_short}\n"
                    f"      MTD: {format_currency(item['mtd'])} ({item['mom_pct']} vs last month) | "
                    f"L7D: {format_currency(item['l7d'])} vs prev 7d: {format_currency(item['prev_7d'])}"
                )
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)},
            })

        # ── Seasonal highs (YoY comparison) ──
        if seasonal_high:
            seasonal_high.sort(key=lambda x: -(x["mtd"] - x["yoy"]))
            lines = [f"\u2600\ufe0f *Above Same Month Last Year ({len(seasonal_high)})*"]
            for item in seasonal_high[:5]:
                sf_link = sf_opp_url(item["opp_id"]) if item["opp_id"] else sf_account_url(item["account_id"])
                product_short = item["product"].replace(" Expansion", "")
                lines.append(
                    f"  <{sf_link}|{item['account']}> — {product_short}: "
                    f"MTD {format_currency(item['mtd'])} ({item['yoy_pct']} YoY)"
                )
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)},
            })

        if not accelerating and not decelerating and not seasonal_high:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "No significant pacing deltas detected this month."},
            })

        # Footer
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": "Compares MTD spend at this point vs same point last month + YoY. DM `priorities` for full view.",
            }],
        })

        client.chat_postMessage(
            channel=GREG_SLACK_ID,
            blocks=blocks,
            text=f"Spend Pacing: {len(accelerating)} accelerating, {len(decelerating)} decelerating",
        )
        logger.info("Spend pacing sent: %d accelerating, %d decelerating", len(accelerating), len(decelerating))

    except Exception as e:
        logger.error("Spend pacing failed: %s", e)
        if force:
            client.chat_postMessage(channel=GREG_SLACK_ID, text=f"Spend pacing report failed: {e}")
