"""Activity Report — SQLs created, opps closed, broken down by product.

Tracks:
  - S2 opps (SQLs) created this week/month
  - Opps closed-won this week/month
  - Breakdown by product type
  - Daily progress toward weekly/monthly targets

Triggered by DM "activity report" or scheduled daily.
"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from core.snowflake_client import run_query
from core.slack_formatter import format_currency, dashboard_url
from config import GREG_SLACK_ID, NTR_RATES

logger = logging.getLogger(__name__)

ACTIVITY_QUERY_TEMPLATE = """
WITH date_ranges AS (
    SELECT
        DATE_TRUNC('week', CURRENT_DATE)   AS week_start,
        DATE_TRUNC('month', CURRENT_DATE)  AS month_start,
        DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE)) AS prev_month_start,
        DATE_TRUNC('month', CURRENT_DATE) AS prev_month_end
),
-- SQLs created (opps that reached S2+)
sqls_created AS (
    SELECT
        opp.opportunity_id,
        opp.expansion_subtype,
        opp.opportunity_created_at::date AS created_date,
        opp.opportunity_stage_name,
        opp.opportunity_is_closed,
        opp.opportunity_is_won,
        sa.account_name
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    WHERE opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = '{owner_name}'
      AND opp.opportunity_stage_name NOT IN ('S0: Holding', 'S1: Sales Accepted Opportunity')
      AND opp.expansion_subtype IN ('Card Expansion', 'Bill Pay Expansion', 'Travel Expansion', 'Treasury Expansion')
),
-- Closed-won opps
closed_won AS (
    SELECT
        opp.opportunity_id,
        opp.expansion_subtype,
        opp.opportunity_closed_won_date::date AS cw_date,
        opp.monthly_expansion_amount,
        sa.account_name,
        CASE opp.expansion_subtype
            WHEN 'Card Expansion'     THEN es.expansion_opportunity_30_day_transaction_amount_tpv_before_closed_won_date_prior
            WHEN 'Bill Pay Expansion' THEN es.expansion_opportunity_30_day_bill_pay_non_card_tpv_revops_amount_before_closed_won_date_prior
            WHEN 'Travel Expansion'   THEN es.expansion_opportunity_30_day_travel_amount_before_closed_won_date_prior
            WHEN 'Treasury Expansion' THEN es.expansion_opportunity_30_day_avg_treasury_available_balance_before_closed_won_date_prior
        END AS baseline_at_close
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    LEFT JOIN analytics.marts.agg_sfdc_expansion_opportunity_spend es ON es.opportunity_id = opp.opportunity_id
    WHERE opp.opportunity_is_won = TRUE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = '{owner_name}'
      AND opp.expansion_subtype IN ('Card Expansion', 'Bill Pay Expansion', 'Travel Expansion', 'Treasury Expansion')
)
SELECT 'sql' AS metric_type,
       s.expansion_subtype AS product,
       s.created_date AS event_date,
       s.account_name,
       NULL AS baseline_at_close,
       d.week_start, d.month_start, d.prev_month_start, d.prev_month_end
FROM sqls_created s
CROSS JOIN date_ranges d
WHERE s.created_date >= d.prev_month_start

UNION ALL

SELECT 'cw' AS metric_type,
       c.expansion_subtype AS product,
       c.cw_date AS event_date,
       c.account_name,
       ROUND(COALESCE(c.baseline_at_close, 0)) AS baseline_at_close,
       d.week_start, d.month_start, d.prev_month_start, d.prev_month_end
FROM closed_won c
CROSS JOIN date_ranges d
WHERE c.cw_date >= d.prev_month_start

ORDER BY event_date DESC
"""


def _safe_float(val, default=0.0):
    try:
        v = float(val)
        return default if pd.isna(v) else v
    except (TypeError, ValueError):
        return default


def run_activity_report(client, user_id=None, force: bool = False):
    """Generate and send the activity report."""
    from core.user_registry import get_user_sf_name

    dm_target = user_id or GREG_SLACK_ID
    owner_name = get_user_sf_name(user_id) if user_id else get_user_sf_name(GREG_SLACK_ID)

    try:
        df = run_query(ACTIVITY_QUERY_TEMPLATE.format(owner_name=owner_name))
        if df.empty and not force:
            return

        now = datetime.now()
        month_name = now.strftime("%B")
        week_num = (now.day - 1) // 7 + 1

        # Parse date ranges from first row
        week_start = month_start = prev_month_start = prev_month_end = None
        if not df.empty:
            row0 = df.iloc[0]
            week_start = row0.get("week_start")
            month_start = row0.get("month_start")
            prev_month_start = row0.get("prev_month_start")
            prev_month_end = row0.get("prev_month_end")

        # Split into SQLs and CWs
        sqls = df[df["metric_type"] == "sql"] if not df.empty else pd.DataFrame()
        cws = df[df["metric_type"] == "cw"] if not df.empty else pd.DataFrame()

        def _count_by_product(subset, date_col="event_date", start=None, end=None):
            """Count items by product within a date range."""
            if subset.empty or start is None:
                return {}
            mask = subset[date_col] >= start
            if end is not None:
                mask = mask & (subset[date_col] < end)
            filtered = subset[mask]
            counts = {}
            for product in ["Card Expansion", "Bill Pay Expansion", "Travel Expansion", "Treasury Expansion"]:
                c = len(filtered[filtered["product"] == product])
                if c > 0:
                    counts[product.replace(" Expansion", "")] = c
            counts["Total"] = len(filtered)
            return counts

        # This week / this month / last month counts
        sqls_week = _count_by_product(sqls, start=week_start)
        sqls_month = _count_by_product(sqls, start=month_start)
        sqls_prev = _count_by_product(sqls, start=prev_month_start, end=prev_month_end)

        cws_week = _count_by_product(cws, start=week_start)
        cws_month = _count_by_product(cws, start=month_start)
        cws_prev = _count_by_product(cws, start=prev_month_start, end=prev_month_end)

        blocks = [{
            "type": "header",
            "text": {"type": "plain_text", "text": f"\U0001f4cb Activity Report — {month_name} Week {week_num}", "emoji": True},
        }]

        # ── SQLs (S2 Opps Created) ──
        def _format_counts(counts, label):
            if not counts or counts.get("Total", 0) == 0:
                return f"  {label}: 0"
            parts = [f"*{counts['Total']}*"]
            product_parts = []
            for p in ["Card", "Bill Pay", "Travel", "Treasury"]:
                if p in counts:
                    product_parts.append(f"{p}: {counts[p]}")
            if product_parts:
                parts.append(f"({', '.join(product_parts)})")
            return f"  {label}: {' '.join(parts)}"

        sql_lines = [
            "*SQLs Created (S2+ Opps)*",
            _format_counts(sqls_week, "This week"),
            _format_counts(sqls_month, f"{month_name} total"),
            _format_counts(sqls_prev, "Last month"),
        ]
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(sql_lines)},
        })

        # ── Opps Closed-Won ──
        cw_lines = [
            "*Opps Closed-Won*",
            _format_counts(cws_week, "This week"),
            _format_counts(cws_month, f"{month_name} total"),
            _format_counts(cws_prev, "Last month"),
        ]
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(cw_lines)},
        })

        # ── Recent closed-won detail ──
        if not cws.empty:
            recent_cws = cws[cws["event_date"] >= month_start].head(5) if month_start is not None else cws.head(5)
            if not recent_cws.empty:
                cw_detail = ["*Recent Closes:*"]
                for _, row in recent_cws.iterrows():
                    product_short = str(row.get("product", "")).replace(" Expansion", "")
                    date = str(row.get("event_date", ""))
                    baseline = _safe_float(row.get("baseline_at_close"))
                    cw_detail.append(
                        f"  \u2022 {row.get('account_name', '?')} — {product_short} ({date}) | "
                        f"Baseline: {format_currency(baseline)}"
                    )
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(cw_detail)},
                })

        # Footer
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": "DM `activity report` to refresh | `post-close monitor` for CP tracking | `priorities` for actions",
            }],
        })

        client.chat_postMessage(
            channel=dm_target,
            blocks=blocks,
            text=f"Activity Report: {sqls_month.get('Total', 0)} SQLs, {cws_month.get('Total', 0)} CWs this month",
        )
        logger.info("Activity report sent: %d SQLs, %d CWs this month",
                     sqls_month.get("Total", 0), cws_month.get("Total", 0))

    except Exception as e:
        logger.error("Activity report failed: %s", e)
        if force:
            client.chat_postMessage(channel=dm_target, text=f"Activity report failed: {e}")
