"""Catch-Up Report — "Here's what you missed" summary after bot downtime.

When the bot restarts after being offline (laptop closed, etc.), this job
sends a consolidated "what you missed" DM covering:
  1. Drafted emails waiting for review
  2. Alert channel hits on your accounts
  3. Open opp pacing changes + zero-to-one activations with open opps (urgent)
  4. Gong calls needing post-meeting analysis
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from core.snowflake_client import run_query
from core.slack_formatter import (
    sf_account_url, sf_opp_url, format_currency, dashboard_url,
)
from config import GREG_SLACK_ID, ALERT_CHANNELS, GMAIL_ADDRESS, NTR_RATES, OWNER_NAME
from core.user_registry import get_user_sf_name

logger = logging.getLogger(__name__)


def run_catchup_report(client, gap_hours, user_id=None):
    """Generate and send a "what you missed" report.

    Parameters
    ----------
    client : slack_sdk.WebClient
    gap_hours : float
        How many hours the bot was offline.
    """
    dm_target = user_id or GREG_SLACK_ID

    try:
        sections = []

        # ── 1. Drafted emails waiting for review ──
        try:
            drafts_section = _check_pending_drafts(client, gap_hours, dm_target=dm_target)
            if drafts_section:
                sections.append(drafts_section)
        except Exception as e:
            logger.debug("Catchup: drafts check failed: %s", e)

        # ── 2. Alert channel hits on your accounts ──
        try:
            alerts_section = _check_alert_channels(client, gap_hours)
            if alerts_section:
                sections.append(alerts_section)
        except Exception as e:
            logger.debug("Catchup: alerts check failed: %s", e)

        # ── 3. Opp pacing changes + urgent zero-to-one activations ──
        try:
            pacing_section = _check_pacing_and_activations(gap_hours, user_id=user_id)
            if pacing_section:
                sections.append(pacing_section)
        except Exception as e:
            logger.debug("Catchup: pacing/activations check failed: %s", e)

        # ── 4. Gong calls needing analysis ──
        try:
            calls_section = _check_gong_calls(gap_hours, user_id=user_id)
            if calls_section:
                sections.append(calls_section)
        except Exception as e:
            logger.debug("Catchup: Gong calls check failed: %s", e)

        if not sections:
            client.chat_postMessage(
                channel=dm_target,
                text=(
                    f"\U0001f44b Back online after {gap_hours:.0f}h. "
                    f"Nothing major happened while you were away."
                ),
            )
            return

        # Build consolidated message
        blocks = [{
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"\U0001f4cb What You Missed ({gap_hours:.0f}h offline)",
                "emoji": True,
            },
        }]

        for section in sections:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": section},
            })

        # Footer
        blocks.append({"type": "divider"})
        _pipe = dashboard_url("pipeline")
        _pm = dashboard_url("post-meeting")
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f"<{_pipe}|Pipeline> \u00b7 "
                    f"<{_pm}|Post-Meeting> \u00b7 "
                    f"All scheduled jobs resuming now"
                ),
            }],
        })

        client.chat_postMessage(
            channel=dm_target,
            blocks=blocks,
            text=f"What You Missed ({gap_hours:.0f}h offline) \u2014 {len(sections)} update(s)",
        )
        logger.info("Catch-up report sent: %d sections, %.1fh gap", len(sections), gap_hours)

    except Exception as e:
        logger.error("Catch-up report failed: %s", e)
        try:
            client.chat_postMessage(
                channel=dm_target,
                text=(
                    f"\U0001f44b Back online after {gap_hours:.0f}h. "
                    f"Catch-up report failed ({e}), but all jobs are resuming."
                ),
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 1. Drafted emails waiting for review
# ---------------------------------------------------------------------------

def _check_pending_drafts(client, gap_hours, dm_target=None):
    """Check for bot DMs during downtime that contained email drafts."""
    dm_target = dm_target or GREG_SLACK_ID
    # Read bot's own DM history during the gap to find draft messages
    try:
        oldest_ts = str(int((datetime.utcnow() - timedelta(hours=gap_hours + 1)).timestamp()))

        # Get bot's own user ID
        auth = client.auth_test()
        bot_id = auth.get("user_id", "")

        # Read recent DMs to the user
        result = client.conversations_history(
            channel=dm_target,
            oldest=oldest_ts,
            limit=50,
        )

        drafts = []
        for msg in result.get("messages", []):
            # Only bot messages
            if msg.get("user") != bot_id and msg.get("bot_id") is None:
                continue
            text = msg.get("text", "")
            blocks = msg.get("blocks", [])
            # Look for draft indicators in message text
            block_text = " ".join(
                b.get("text", {}).get("text", "") if isinstance(b.get("text"), dict) else ""
                for b in blocks
            )
            combined = f"{text} {block_text}".lower()
            if any(kw in combined for kw in [
                "draft created", "gmail draft", "follow-up email",
                "draft \u2192", "check gmail drafts",
            ]):
                # Extract subject/recipient from the message
                subject = ""
                recipient = ""
                for b in blocks:
                    bt = b.get("text", {})
                    if isinstance(bt, dict):
                        bt = bt.get("text", "")
                    if "draft" in bt.lower() and "\u2192" in bt:
                        subject = bt.split("_")[1] if bt.count("_") >= 2 else bt
                        break
                drafts.append({
                    "text": text[:100] if text else "(see Slack DM)",
                    "ts": msg.get("ts", ""),
                })

        if not drafts:
            return None

        lines = [f"*\u2709\ufe0f Drafts Waiting ({len(drafts)}):*"]
        for d in drafts[:5]:
            lines.append(f"\u2022 {d['text']}")
        if len(drafts) > 5:
            lines.append(f"  _...and {len(drafts) - 5} more_")
        lines.append("  Check Gmail Drafts folder to review and send")
        return "\n".join(lines)

    except Exception as e:
        logger.debug("Catchup: draft check failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# 2. Alert channel hits on your accounts
# ---------------------------------------------------------------------------

def _check_alert_channels(client, gap_hours):
    """Scan monitored alert channels for messages during downtime."""
    from handlers.channel_monitors import _channel_ids_by_key

    oldest_ts = str(int((datetime.utcnow() - timedelta(hours=gap_hours + 1)).timestamp()))

    alert_labels = {
        "ach_to_card": "\U0001f4b3 Card Payable Bill",
        "procurement_trial": "\U0001f6d2 Procurement Trial",
        "pclip": "\u26a1 PCLIP Activation",
        "large_decline": "\u274c Large Decline",
        "fundraise": "\U0001f4b0 Fundraise",
        "auto_card": "\U0001f4b3 Automatic Card",
        "rclip": "\U0001f4c8 RCLIP",
        "am_escalation": "\U0001f4e8 AM Escalation",
    }

    all_alerts = []
    for key, channel_id in _channel_ids_by_key.items():
        if not channel_id:
            continue
        try:
            result = client.conversations_history(
                channel=channel_id,
                oldest=oldest_ts,
                limit=30,
            )
            messages = result.get("messages", [])
            if messages:
                label = alert_labels.get(key, key)
                all_alerts.append({
                    "key": key,
                    "label": label,
                    "count": len(messages),
                    "sample": messages[0].get("text", "")[:100],
                })
        except Exception as e:
            logger.debug("Catchup: failed to read channel %s: %s", key, e)

    if not all_alerts:
        return None

    total = sum(a["count"] for a in all_alerts)
    lines = [f"*\U0001f4e1 Alert Channel Activity ({total} messages):*"]
    for a in all_alerts:
        lines.append(f"\u2022 {a['label']}: {a['count']} alert(s)")
    lines.append("  Check the Alert Feed page for details")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 3. Opp pacing changes + zero-to-one activations with open opps (URGENT)
# ---------------------------------------------------------------------------

def _check_pacing_and_activations(gap_hours, user_id=None):
    """Check for notable pacing changes on open opps and urgent 0-to-1 activations."""
    owner_name = get_user_sf_name(user_id) if user_id else OWNER_NAME
    lines = []

    # ── 3a. Pacing changes: opps that crossed above or below baseline ──
    try:
        pacing_query = f"""
        WITH greg_open_opps AS (
            SELECT
                opp.opportunity_id, opp.account_id, opp.expansion_subtype,
                opp.opportunity_stage_name, opp.monthly_expansion_amount,
                opp.opportunity_created_at::date AS created_date,
                DATEDIFF('day', opp.opportunity_created_at::date, CURRENT_DATE) AS days_open,
                sa.account_name, sa.business_id
            FROM analytics.marts.dim_sfdc_opportunities opp
            JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
            WHERE opp.opportunity_is_closed = FALSE
              AND opp.opportunity_type = 'Expansion'
              AND opp.opportunity_owner = '{owner_name}'
              AND opp.opportunity_stage_name != 'S0: Holding'
        ),
        baseline AS (
            SELECT g.opportunity_id,
                CASE g.expansion_subtype
                    WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
                    WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
                    WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
                    WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
                END AS baseline_val
            FROM greg_open_opps g
            JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
                ON tpv.business_id = g.business_id
                AND tpv.date_day BETWEEN
                    IFF(g.days_open <= 45, DATEADD('day', -30, g.created_date), CURRENT_DATE - 60)
                    AND
                    IFF(g.days_open <= 45, DATEADD('day', -1, g.created_date), CURRENT_DATE - 31)
            GROUP BY g.opportunity_id, g.expansion_subtype
        ),
        recent AS (
            SELECT g.opportunity_id,
                CASE g.expansion_subtype
                    WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
                    WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
                    WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
                    WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
                END AS recent_val
            FROM greg_open_opps g
            JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
                ON tpv.business_id = g.business_id
                AND tpv.date_day >= CURRENT_DATE - 30
            GROUP BY g.opportunity_id, g.expansion_subtype
        )
        SELECT
            g.account_name, g.account_id, g.opportunity_id, g.expansion_subtype,
            g.opportunity_stage_name, g.monthly_expansion_amount,
            ROUND(COALESCE(b.baseline_val, 0)) AS baseline_spend,
            ROUND(COALESCE(r.recent_val, 0)) AS current_spend,
            ROUND(COALESCE(r.recent_val, 0) - COALESCE(b.baseline_val, 0)) AS over_baseline,
            ROUND(
                CASE WHEN COALESCE(b.baseline_val, 0) > 0
                     THEN 100.0 * COALESCE(r.recent_val, 0) / b.baseline_val
                     ELSE NULL END
            ) AS pacing_pct
        FROM greg_open_opps g
        LEFT JOIN baseline b ON b.opportunity_id = g.opportunity_id
        LEFT JOIN recent r ON r.opportunity_id = g.opportunity_id
        WHERE COALESCE(r.recent_val, 0) - COALESCE(b.baseline_val, 0) != 0
        ORDER BY ABS(COALESCE(r.recent_val, 0) - COALESCE(b.baseline_val, 0)) DESC
        """
        pacing_df = run_query(pacing_query)

        if not pacing_df.empty:
            # Notable movers: >150% pacing (close-ready) or <80% (at risk)
            hot = pacing_df[pacing_df["pacing_pct"] > 150].head(3)
            cold = pacing_df[(pacing_df["pacing_pct"] < 80) & (pacing_df["pacing_pct"] > 0)].head(3)

            if not hot.empty:
                lines.append("*\U0001f525 Close-Ready (pacing >150%):*")
                for _, r in hot.iterrows():
                    acct = r.get("account_name", "?")
                    acct_id = r.get("account_id", "")
                    product = r.get("expansion_subtype", "")
                    over = float(r.get("over_baseline", 0))
                    pct = int(r.get("pacing_pct", 0) or 0)
                    ntr = NTR_RATES.get(product, 0)
                    est_cp = over * ntr * 3 if over > 0 else 0
                    link = sf_account_url(acct_id) if acct_id else ""
                    name = f"<{link}|{acct}>" if link else acct
                    lines.append(
                        f"\u2022 {name} \u2014 {product}: {pct}% pacing, "
                        f"{format_currency(over)} above baseline"
                        + (f" (~{format_currency(est_cp)} CP)" if est_cp > 0 else "")
                    )

            if not cold.empty:
                lines.append("*\u26a0\ufe0f At Risk (pacing <80%):*")
                for _, r in cold.iterrows():
                    acct = r.get("account_name", "?")
                    acct_id = r.get("account_id", "")
                    product = r.get("expansion_subtype", "")
                    pct = int(r.get("pacing_pct", 0) or 0)
                    link = sf_account_url(acct_id) if acct_id else ""
                    name = f"<{link}|{acct}>" if link else acct
                    lines.append(
                        f"\u2022 {name} \u2014 {product}: {pct}% pacing — spend declining"
                    )

    except Exception as e:
        logger.debug("Catchup: pacing query failed: %s", e)

    # ── 3b. Zero-to-one activations WITH matching open opps (URGENT) ──
    try:
        activation_query = f"""
        WITH activations AS (
            SELECT
                sa.account_name, sa.account_id,
                bob.card_fifth_transaction_cleared_at,
                bob.first_bill_paid_at,
                bob.treasury_first_payment_at,
                bob.travel_fifth_booking_at
            FROM analytics.marts.dim_book_of_business_accounts_view bob
            JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = bob.sfdc_account_id
            JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
                ON ledger.account_id = sa.account_id
                AND ledger.date_day = CURRENT_DATE - 1
                AND ledger.owner_name = '{owner_name}'
            WHERE (
                bob.card_fifth_transaction_cleared_at >= DATEADD('hour', -{int(gap_hours + 2)}, CURRENT_TIMESTAMP)
                OR bob.first_bill_paid_at >= DATEADD('hour', -{int(gap_hours + 2)}, CURRENT_TIMESTAMP)
                OR bob.treasury_first_payment_at >= DATEADD('hour', -{int(gap_hours + 2)}, CURRENT_TIMESTAMP)
                OR bob.travel_fifth_booking_at >= DATEADD('hour', -{int(gap_hours + 2)}, CURRENT_TIMESTAMP)
            )
        ),
        open_opps AS (
            SELECT account_id, expansion_subtype
            FROM analytics.marts.dim_sfdc_opportunities
            WHERE opportunity_is_closed = FALSE
              AND opportunity_type = 'Expansion'
              AND opportunity_owner = '{owner_name}'
              AND opportunity_stage_name != 'S0: Holding'
        )
        SELECT
            a.*,
            LISTAGG(DISTINCT o.expansion_subtype, ', ') AS open_opp_products
        FROM activations a
        LEFT JOIN open_opps o ON o.account_id = a.account_id
        GROUP BY a.account_name, a.account_id,
                 a.card_fifth_transaction_cleared_at, a.first_bill_paid_at,
                 a.treasury_first_payment_at, a.travel_fifth_booking_at
        """
        act_df = run_query(activation_query)

        if not act_df.empty:
            cutoff = datetime.utcnow() - timedelta(hours=gap_hours + 2)
            urgent_lines = []
            other_lines = []

            for _, r in act_df.iterrows():
                acct = r.get("account_name", "?")
                acct_id = r.get("account_id", "")
                open_products = str(r.get("open_opp_products", "") or "")
                link = sf_account_url(acct_id) if acct_id else ""
                name = f"<{link}|{acct}>" if link else acct

                activated = []
                product_map = {
                    "card_fifth_transaction_cleared_at": ("Card", "Card Expansion"),
                    "first_bill_paid_at": ("Bill Pay", "Bill Pay Expansion"),
                    "treasury_first_payment_at": ("Treasury", "Treasury Expansion"),
                    "travel_fifth_booking_at": ("Travel", "Travel Expansion"),
                }
                for col, (short, opp_type) in product_map.items():
                    val = r.get(col)
                    if val and str(val) > str(cutoff):
                        has_opp = opp_type in open_products
                        activated.append((short, has_opp))

                if not activated:
                    continue

                # Urgent: activated product matches an open opp
                urgent_products = [s for s, has_opp in activated if has_opp]
                other_products = [s for s, has_opp in activated if not has_opp]

                if urgent_products:
                    urgent_lines.append(
                        f"\u2022 {name} \u2014 *{', '.join(urgent_products)}* activated "
                        f"with open opp \u2014 baseline clock starts now"
                    )
                if other_products:
                    other_lines.append(
                        f"\u2022 {name} \u2014 {', '.join(other_products)} activated (no opp yet)"
                    )

            if urgent_lines:
                lines.append("*\U0001f6a8 URGENT \u2014 Activations With Open Opps:*")
                lines.extend(urgent_lines[:5])

            if other_lines:
                lines.append("*\U0001f195 New Activations (no opp):*")
                lines.extend(other_lines[:5])

    except Exception as e:
        logger.debug("Catchup: activation query failed: %s", e)

    if not lines:
        return None

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 4. Gong calls needing post-meeting analysis
# ---------------------------------------------------------------------------

def _check_gong_calls(gap_hours, user_id=None):
    """Check for Gong calls during downtime that haven't been analyzed."""
    from utils.dedup import tracker
    from core.user_registry import get_user_email, get_user_sf_name

    owner_name = get_user_sf_name(user_id) if user_id else OWNER_NAME
    email = get_user_email(user_id).lower() if user_id else GMAIL_ADDRESS.lower()

    query = f"""
    SELECT
        gc.gong_call_id AS call_id,
        sa.account_name,
        sa.account_id,
        gc.call_name,
        gc.gong_call_start::date AS call_date,
        ROUND(gc.gong_call_duration_sec / 60) AS duration_min
    FROM analytics.marts.dim_sfdc_gong_call gc
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = gc.sfdc_primary_account_id
    JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
        ON ledger.account_id = gc.sfdc_primary_account_id
        AND ledger.date_day = CURRENT_DATE - 1
        AND ledger.owner_name = '{owner_name}'
    WHERE gc.gong_call_start >= DATEADD('hour', -{int(gap_hours + 2)}, CURRENT_TIMESTAMP)
      AND gc.gong_call_duration_sec >= 180
      AND EXISTS (
          SELECT 1 FROM analytics.marts.dim_gong_transcript_paragraph p
          WHERE p.call_id = gc.gong_call_id
            AND LOWER(p.speaker_email) = '{email}'
      )
    ORDER BY gc.gong_call_start DESC
    """
    df = run_query(query)
    if df.empty:
        return None

    # Filter to calls not yet processed
    unprocessed = []
    for _, r in df.iterrows():
        call_id = r.get("call_id")
        if call_id and not tracker.is_processed(f"meeting_{call_id}", user_id=user_id):
            unprocessed.append(r)

    if not unprocessed:
        return None

    lines = [f"*\U0001f3ac Calls Needing Analysis ({len(unprocessed)}):*"]
    for r in unprocessed[:5]:
        acct = r.get("account_name", "?")
        acct_id = r.get("account_id", "")
        call = r.get("call_name", "")
        dur = r.get("duration_min", 0)
        link = sf_account_url(acct_id) if acct_id else ""
        name = f"<{link}|{acct}>" if link else acct
        lines.append(f"\u2022 {name} \u2014 _{call}_ ({dur} min)")

    if len(unprocessed) > 5:
        lines.append(f"  _...and {len(unprocessed) - 5} more_")
    lines.append("  `/post-meeting` to analyze \u2014 or wait for next scheduled run")
    return "\n".join(lines)
