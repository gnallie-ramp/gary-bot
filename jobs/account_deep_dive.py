"""Account Deep Dive — "tell me about [account]" shows ALL signals for one account.

Pulls open opps + pacing, recent Gong calls, recent emails, z2o opportunities,
SFDC notes, and stale flags into a single comprehensive DM.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from core.snowflake_client import run_query
from core.slack_formatter import (
    sf_account_url, sf_opp_url, format_currency, dashboard_url,
    build_sf_new_opp_url, opp_fields_summary,
)
from config import GMAIL_ADDRESS, GREG_SLACK_ID, NTR_RATES, OWNER_NAME
from queries.queries import (
    ACCOUNT_LOOKUP_QUERY, ACCOUNT_NOTES_QUERY, ACCOUNT_EMAILS_FULL_QUERY,
    GONG_MEETINGS_QUERY, format_query,
)
from core.user_registry import get_user_sf_name

logger = logging.getLogger(__name__)


def _safe_float(val, default=0.0):
    try:
        v = float(val)
        return default if pd.isna(v) else v
    except (TypeError, ValueError):
        return default


# ── Account-level queries ─────────────────────────────────────────────────

_ACCOUNT_OPPS_QUERY = """
SELECT
    opp.opportunity_id, opp.expansion_subtype, opp.opportunity_stage_name,
    opp.opportunity_close_date, opp.monthly_expansion_amount,
    opp.opportunity_created_at::date AS created_date,
    DATEDIFF('day', opp.opportunity_created_at::date, CURRENT_DATE) AS days_open,
    opp.opportunity_is_closed, opp.opportunity_is_won
FROM analytics.marts.dim_sfdc_opportunities opp
WHERE opp.account_id = '{account_id}'
  AND opp.opportunity_type = 'Expansion'
  AND opp.opportunity_owner = '{owner_name}'
ORDER BY opp.opportunity_is_closed ASC, opp.opportunity_close_date DESC
LIMIT 15
"""

_ACCOUNT_SPEND_QUERY = """
SELECT
    SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.card_tpv ELSE 0 END)     AS card_l30d,
    SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.billpay_tpv ELSE 0 END)  AS billpay_l30d,
    SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.travel_tpv ELSE 0 END)   AS travel_l30d,
    AVG(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.treasury_available_balance ELSE NULL END) AS treasury_l30d,
    SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.card_tpv ELSE 0 END)     AS card_prev30d,
    SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.billpay_tpv ELSE 0 END)  AS billpay_prev30d,
    SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.travel_tpv ELSE 0 END)   AS travel_prev30d,
    AVG(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.treasury_available_balance ELSE NULL END) AS treasury_prev30d
FROM analytics.metrics.fct_daily_business__multiproduct_tpv tpv
WHERE tpv.business_id = '{business_id}'
  AND tpv.date_day >= CURRENT_DATE - 60
"""

_ACCOUNT_GONG_QUERY = """
SELECT
    gt.call_id, gt.call_name, gt.call_start::date AS call_date,
    ROUND(gt.call_duration_sec / 60) AS duration_min,
    LISTAGG(DISTINCT NULLIF(gs.competitor_mentioned, ''), ', ') AS competitors,
    LISTAGG(DISTINCT NULLIF(gs.product_mentioned, ''), ', ') AS products_discussed,
    LISTAGG(
        CASE WHEN gs.product_request_text IS NOT NULL AND gs.product_request_text != ''
             THEN gs.product_request_text END, ' | '
    ) WITHIN GROUP (ORDER BY gs.section_index) AS product_requests,
    LEFT(LISTAGG(
        CASE WHEN gs.section_text IS NOT NULL AND gs.section_text != ''
             THEN gs.section_name || ': ' || LEFT(gs.section_text, 300) END, ' || '
    ) WITHIN GROUP (ORDER BY gs.section_index), 1500) AS call_summary
FROM analytics.marts.dim_sfdc_gong_transcripts gt
LEFT JOIN analytics.marts.dim_gong_section_summary gs ON gs.call_id = gt.call_id
WHERE gt.account_id = '{account_id}'
  AND gt.call_start >= DATEADD('day', -90, CURRENT_DATE)
  AND gt.call_duration_sec >= 180
GROUP BY gt.call_id, gt.call_name, gt.call_start, gt.call_duration_sec
ORDER BY gt.call_start DESC
LIMIT 5
"""

_ACCOUNT_EMAILS_QUERY = """
SELECT
    e.sfdc_email_created_at::date AS email_date,
    e.email_direction AS direction,
    e.email_subject AS subject,
    e.sfdc_email_owner_email AS sender_email,
    e.ramp_employee_team AS sender_team,
    e.external_contact_name,
    e.contact_persona,
    e.has_willing_to_meet, e.has_not_interested,
    e.has_painpoints, e.has_interested
FROM analytics.marts.dim_emails e
WHERE e.account_id = '{account_id}'
  AND e.sfdc_email_created_at >= DATEADD('day', -90, CURRENT_DATE)
ORDER BY e.sfdc_email_created_at DESC
LIMIT 20
"""


def _resolve_account(search_term: str, user_id: str = None) -> dict | None:
    """Resolve a search term to an account dict. Returns None if not found."""
    safe_term = search_term.replace("'", "''").replace("%", "\\%")
    df = run_query(format_query(ACCOUNT_LOOKUP_QUERY, user_id=user_id, search_term=safe_term))
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        "account_name": row["account_name"],
        "account_id": row["account_id"],
        "business_id": row.get("business_id", ""),
        "card_l30d": _safe_float(row.get("card_l30d")),
        "billpay_l30d": _safe_float(row.get("billpay_l30d")),
        "treasury_l30d": _safe_float(row.get("treasury_l30d")),
        "card_activated_at": row.get("card_activated_at"),
        "billpay_first_paid_at": row.get("billpay_first_paid_at"),
        "treasury_activated_at": row.get("treasury_activated_at"),
        "travel_activated_at": row.get("travel_activated_at"),
    }


def _build_deep_dive_blocks(account: dict, opps_df, spend_df, gong_df, emails_df, notes_df, user_id: str = None) -> list[dict]:
    """Build comprehensive account deep dive blocks."""
    from core.user_registry import get_user_email

    account_name = account["account_name"]
    account_id = account["account_id"]
    sf_link = sf_account_url(account_id)

    # Derive the owner's email username for filtering (e.g. "gnallie" from "gnallie@ramp.com")
    _owner_email = get_user_email(user_id) if user_id else (GMAIL_ADDRESS or "")
    _owner_username = _owner_email.split("@")[0]

    blocks = [{
        "type": "header",
        "text": {"type": "plain_text", "text": f"\U0001f50d {account_name}", "emoji": True},
    }]

    # ── Account overview ──
    overview_parts = [f"<{sf_link}|Open in Salesforce>"]

    # Activation status
    activations = []
    if account.get("card_activated_at"):
        activations.append("Card")
    if account.get("billpay_first_paid_at"):
        activations.append("Bill Pay")
    if account.get("treasury_activated_at"):
        activations.append("Treasury")
    if account.get("travel_activated_at"):
        activations.append("Travel")
    if activations:
        overview_parts.append(f"Active products: {', '.join(activations)}")
    else:
        overview_parts.append("No products activated yet")

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join(overview_parts)},
    })

    # ── Spend snapshot ──
    if spend_df is not None and not spend_df.empty:
        row = spend_df.iloc[0]
        spend_lines = []
        for product, l30d_col, prev_col in [
            ("Card", "card_l30d", "card_prev30d"),
            ("Bill Pay", "billpay_l30d", "billpay_prev30d"),
            ("Travel", "travel_l30d", "travel_prev30d"),
            ("Treasury", "treasury_l30d", "treasury_prev30d"),
        ]:
            l30d = _safe_float(row.get(l30d_col))
            prev = _safe_float(row.get(prev_col))
            if l30d > 0 or prev > 0:
                delta = l30d - prev
                arrow = "\u2197\ufe0f" if delta > 0 else ("\u2198\ufe0f" if delta < 0 else "\u27a1\ufe0f")
                delta_str = f"+{format_currency(delta)}" if delta >= 0 else f"-{format_currency(abs(delta))}"
                spend_lines.append(
                    f"  {product}: {format_currency(l30d)} L30D {arrow} ({delta_str} vs prev 30d)"
                )
        if spend_lines:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Spend Snapshot*\n" + "\n".join(spend_lines)},
            })

    # ── Open Opps ──
    if opps_df is not None and not opps_df.empty:
        open_opps = opps_df[opps_df["opportunity_is_closed"] == False]
        closed_opps = opps_df[opps_df["opportunity_is_won"] == True]

        blocks.append({"type": "divider"})

        if not open_opps.empty:
            opp_lines = []
            for _, opp in open_opps.iterrows():
                opp_link = sf_opp_url(opp["opportunity_id"])
                stage = opp.get("opportunity_stage_name", "")
                product = opp.get("expansion_subtype", "")
                days = int(_safe_float(opp.get("days_open")))
                opp_lines.append(
                    f"  \u2022 <{opp_link}|{product}> — {stage} | {days}d open"
                )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Open Opps ({len(open_opps)})*\n" + "\n".join(opp_lines)},
            })

        if not closed_opps.empty:
            cw_lines = []
            for _, opp in closed_opps.head(3).iterrows():
                product = opp.get("expansion_subtype", "")
                close_date = str(opp.get("opportunity_close_date", ""))
                cw_lines.append(f"  \u2022 {product} — CW {close_date}")
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Recent Closed-Won*\n" + "\n".join(cw_lines)},
            })

        if open_opps.empty:
            # Check for z2o opportunities
            z2o_products = []
            _product_checks = [
                ("card_activated_at", "Card Expansion"),
                ("billpay_first_paid_at", "Bill Pay Expansion"),
                ("treasury_activated_at", "Treasury Expansion"),
                ("travel_activated_at", "Travel Expansion"),
            ]
            open_subtypes = set(open_opps["expansion_subtype"].tolist()) if not open_opps.empty else set()
            cw_subtypes = set(closed_opps["expansion_subtype"].tolist()) if not closed_opps.empty else set()
            for date_field, product in _product_checks:
                if account.get(date_field) and product not in open_subtypes and product not in cw_subtypes:
                    z2o_products.append(product.replace(" Expansion", ""))
            if z2o_products:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"\u26a1 *Zero-to-One opportunity:* {', '.join(z2o_products)} activated but no opp",
                    },
                })
    else:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*No expansion opps found for this account.*"},
        })

    # ── Recent Gong Calls ──
    if gong_df is not None and not gong_df.empty:
        blocks.append({"type": "divider"})
        call_lines = []
        for _, call in gong_df.head(3).iterrows():
            name = call.get("call_name", "")
            date = str(call.get("call_date", ""))
            dur = int(_safe_float(call.get("duration_min")))
            products = str(call.get("products_discussed", "") or "")
            competitors = str(call.get("competitors", "") or "")
            requests = str(call.get("product_requests", "") or "")[:100]

            line = f"  \u2022 _{name}_ ({date}, {dur}min)"
            extras = []
            if products:
                extras.append(f"Products: {products}")
            if competitors:
                extras.append(f"Competitors: {competitors}")
            if requests:
                extras.append(f"Asked about: {requests}")
            if extras:
                line += "\n      " + " | ".join(extras)
            call_lines.append(line)

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Recent Calls ({len(gong_df)})*\n" + "\n".join(call_lines)},
        })

    # ── Recent Emails ──
    if emails_df is not None and not emails_df.empty:
        blocks.append({"type": "divider"})
        email_lines = []
        for _, em in emails_df.head(10).iterrows():
            date = str(em.get("email_date", ""))
            direction = em.get("direction", "")
            subject = str(em.get("subject", "") or "")[:50]
            sender = str(em.get("sender_email", "") or "").split("@")[0]
            team = str(em.get("sender_team", "") or "")
            arrow = "\u2192" if direction == "Outbound" else "\u2190"
            sender_tag = f" [{sender}]" if sender and sender != _owner_username else ""
            email_lines.append(f"  {arrow} {date}: {subject}{sender_tag}")
        # Signals summary
        signal_flags = []
        if emails_df.get("has_painpoints", pd.Series()).any():
            signal_flags.append("pain points")
        if emails_df.get("has_interested", pd.Series()).any():
            signal_flags.append("interested")
        if emails_df.get("has_not_interested", pd.Series()).any():
            signal_flags.append("not interested")
        if emails_df.get("has_willing_to_meet", pd.Series()).any():
            signal_flags.append("willing to meet")
        # Unique senders
        senders = emails_df["sender_email"].dropna().unique() if "sender_email" in emails_df.columns else []
        other_senders = [s for s in senders if _owner_username not in str(s)]
        header = f"*Recent Emails* ({len(emails_df)} total"
        if other_senders:
            header += f", {len(other_senders) + 1} Ramp senders"
        header += ")"
        if signal_flags:
            header += f"\n  Signals: {', '.join(signal_flags)}"
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": header + "\n" + "\n".join(email_lines)},
        })

    # ── SFDC Notes ──
    if notes_df is not None and not notes_df.empty:
        row = notes_df.iloc[0]
        note_parts = []
        for field, label in [
            ("am_notes", "AM Notes"), ("am_next_steps", "AM Next Steps"),
            ("csm_notes", "CSM Notes"), ("csm_next_steps", "CSM Next Steps"),
        ]:
            val = row.get(field)
            if val and str(val).strip() and str(val).strip().lower() != "none":
                note_parts.append(f"  *{label}:* {str(val)[:200]}")
        if note_parts:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*SFDC Notes*\n" + "\n".join(note_parts)},
            })

    # ── Footer ──
    blocks.append({"type": "divider"})
    prep_link = dashboard_url("meeting-prep", account=account_name)
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": f"<{prep_link}|Meeting Prep> · <{sf_link}|Salesforce> · DM me `priorities` for all accounts",
        }],
    })

    return blocks


def run_account_deep_dive(search_term: str, client, dm_channel: str, user_id: str = None):
    """Run the account deep dive and send results to the DM channel."""
    try:
        account = _resolve_account(search_term, user_id=user_id)
        if not account:
            client.chat_postMessage(
                channel=dm_channel,
                text=f"No accounts found matching *{search_term}*. Make sure the name matches what's in Salesforce.",
            )
            return

        account_id = account["account_id"]
        business_id = account["business_id"]

        # Parallel data fetch
        opps_df = spend_df = gong_df = emails_df = notes_df = None

        def _fetch_opps():
            return run_query(_ACCOUNT_OPPS_QUERY.format(account_id=account_id, owner_name=get_user_sf_name(user_id)))

        def _fetch_spend():
            if not business_id:
                return pd.DataFrame()
            return run_query(_ACCOUNT_SPEND_QUERY.format(business_id=business_id))

        def _fetch_gong():
            return run_query(_ACCOUNT_GONG_QUERY.format(account_id=account_id))

        def _fetch_emails():
            return run_query(_ACCOUNT_EMAILS_QUERY.format(account_id=account_id))

        def _fetch_notes():
            return run_query(ACCOUNT_NOTES_QUERY.format(account_ids=f"'{account_id}'"))

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(_fetch_opps): "opps",
                executor.submit(_fetch_spend): "spend",
                executor.submit(_fetch_gong): "gong",
                executor.submit(_fetch_emails): "emails",
                executor.submit(_fetch_notes): "notes",
            }
            results = {}
            for future in as_completed(futures):
                label = futures[future]
                try:
                    results[label] = future.result()
                except Exception as e:
                    logger.warning("Account deep dive: %s failed for %s: %s", label, account_id, e)
                    results[label] = pd.DataFrame()

        blocks = _build_deep_dive_blocks(
            account,
            results.get("opps"),
            results.get("spend"),
            results.get("gong"),
            results.get("emails"),
            results.get("notes"),
            user_id=user_id,
        )

        client.chat_postMessage(
            channel=dm_channel,
            blocks=blocks,
            text=f"Account Deep Dive — {account['account_name']}",
        )
        logger.info("Account deep dive sent for %s", account["account_name"])

    except Exception as e:
        logger.error("Account deep dive failed for %s: %s", search_term, e)
        client.chat_postMessage(
            channel=dm_channel,
            text=f"Error looking up *{search_term}*: {e}",
        )
