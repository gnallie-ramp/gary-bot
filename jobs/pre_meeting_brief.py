"""Pre-Meeting Auto-Brief — calendar-triggered expansion-focused meeting prep.

Runs every 30 min. Checks Google Calendar for meetings in the next 90 min,
matches them to SFDC accounts, and DMs Greg a pre-call brief focused on
expansion discovery: AE qualified spend vs actual, past transcript signals,
email threads with expansion interest, and specific SQL opportunities.

No more manually running /gary-brief — prep comes to you.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd

from core.google_calendar_client import (
    get_upcoming_meetings, match_meeting_to_account,
    extract_external_attendees, is_customer_meeting,
    format_meeting_time,
)
from core.snowflake_client import run_query
from core.claude_client import call_claude
from core.slack_formatter import (
    sf_account_url, format_currency, dashboard_url, simple_dm_blocks,
)
from core.email_context import get_email_context, format_email_context_block
from queries.queries import (
    ACCOUNT_LOOKUP_QUERY, ACCOUNT_OPPS_QUERY,
    GONG_FULL_TRANSCRIPT_QUERY, ACCOUNT_EMAILS_FULL_QUERY,
    ACCOUNT_NOTES_QUERY, format_query,
)
from utils.account_resolver import fetch_contact_emails
from utils.dedup import tracker
from config import GREG_SLACK_ID, NTR_RATES
from core.user_registry import get_user_sf_name

logger = logging.getLogger(__name__)

# How far ahead to look for meetings (minutes)
_LOOKAHEAD_MIN = 90

# Minimum meeting duration to trigger a brief (minutes)
_MIN_DURATION = 15


def run_pre_meeting_brief(client, user_id=None, force=False):
    """Check calendar for upcoming customer meetings and send auto-briefs.

    Parameters
    ----------
    client : slack_sdk.WebClient
    force : bool
        If True, send briefs for ALL upcoming meetings (ignores dedup).
    """
    dm_target = user_id or GREG_SLACK_ID
    owner_name = get_user_sf_name(user_id)

    try:
        # Fetch meetings in the next 90 minutes
        meetings = get_upcoming_meetings(days_ahead=1, max_results=15, user_id=user_id)
        if not meetings:
            if force:
                client.chat_postMessage(
                    channel=dm_target,
                    text="No upcoming calendar meetings found.",
                )
            return

        now = datetime.utcnow()
        window_end = now + timedelta(minutes=_LOOKAHEAD_MIN)

        # Filter to meetings starting within the lookahead window
        upcoming = []
        for m in meetings:
            start = m.get("start")
            if not start:
                continue
            if start < now or start > window_end:
                continue
            if m.get("duration_min", 0) < _MIN_DURATION:
                continue
            if not is_customer_meeting(m):
                continue
            upcoming.append(m)

        if not upcoming:
            if force:
                client.chat_postMessage(
                    channel=dm_target,
                    text=f"No customer meetings in the next {_LOOKAHEAD_MIN} minutes.",
                )
            return

        for meeting in upcoming:
            _process_meeting(meeting, client, force, dm_target=dm_target, owner_name=owner_name, user_id=user_id)

    except Exception as e:
        logger.error("Pre-meeting brief failed: %s", e)
        if force:
            client.chat_postMessage(
                channel=dm_target,
                text=f"Pre-meeting brief failed: {e}",
            )


def _process_meeting(meeting, client, force, dm_target=None, owner_name: str = "", user_id: str = None):
    """Generate and send an expansion-focused brief for a single meeting."""
    if not owner_name:
        owner_name = get_user_sf_name(None)
    event_id = meeting.get("event_id", "")
    dedup_key = f"pre_brief_{event_id}"

    if not force and tracker.is_processed(dedup_key, user_id=user_id):
        return

    # Match to SFDC account
    match = match_meeting_to_account(meeting, user_id=user_id)
    if not match:
        # Can't match — send a lightweight heads-up instead
        _send_unmatched_alert(meeting, client, dm_target=dm_target)
        tracker.mark_processed(dedup_key, user_id=user_id)
        return

    account_id = match["account_id"]
    account_name = match["account_name"]
    match_source = match.get("match_source", "")

    try:
        # ── 1. Account basics (spend data) ──
        safe_name = account_name.replace("'", "''").replace("%", "\\%")
        acct_df = run_query(format_query(ACCOUNT_LOOKUP_QUERY, user_id=user_id, search_term=safe_name))
        if acct_df.empty:
            logger.warning("Pre-brief: account %s not found in lookup", account_name)
            return

        row = acct_df.iloc[0]
        sf_link = sf_account_url(account_id)

        card_l30 = float(row.get("card_l30d", 0) or 0)
        bp_l30 = float(row.get("billpay_l30d", 0) or 0)
        treasury_l30 = float(row.get("treasury_l30d", 0) or 0)

        # ── 2. Open expansion opps + AE qualified amounts ──
        opps_df = run_query(format_query(ACCOUNT_OPPS_QUERY, user_id=user_id))
        acct_opps = opps_df[opps_df["account_id"] == account_id] if not opps_df.empty else pd.DataFrame()

        opps_text = ""
        ae_vs_actual_text = ""
        if not acct_opps.empty:
            opp_lines = []
            gap_lines = []
            for _, r in acct_opps.iterrows():
                subtype = r.get("expansion_subtype", "")
                stage = r.get("opportunity_stage_name", "")
                ae_amount = float(r.get("monthly_expansion_amount", 0) or 0)
                opp_lines.append(
                    f"- {subtype} ({stage}) — AE estimated: {format_currency(ae_amount)}/mo"
                )

                # Compare AE estimate to actual L30D spend
                actual = 0
                if "Card" in subtype:
                    actual = card_l30
                elif "Bill Pay" in subtype:
                    actual = bp_l30
                elif "Treasury" in subtype:
                    actual = treasury_l30

                if ae_amount > 0:
                    pct = (actual / ae_amount * 100) if ae_amount > 0 else 0
                    gap = actual - ae_amount
                    direction = "ABOVE" if gap > 0 else "BELOW"
                    gap_lines.append(
                        f"- {subtype}: AE estimated {format_currency(ae_amount)}/mo, "
                        f"actual L30D {format_currency(actual)} "
                        f"({direction} by {format_currency(abs(gap))}, {pct:.0f}% of estimate)"
                    )

            opps_text = "\n".join(opp_lines)
            ae_vs_actual_text = "\n".join(gap_lines) if gap_lines else "No AE estimates to compare"
        else:
            opps_text = "NO OPEN EXPANSION OPPS — potential SQL opportunity"
            ae_vs_actual_text = "N/A — no open opps"

        # ── 3. Past Gong transcripts (expansion signals, pain points) ──
        transcript_text = ""
        try:
            transcript_df = run_query(GONG_FULL_TRANSCRIPT_QUERY.format(
                account_id=account_id, lookback_days=90,
            ))
            if not transcript_df.empty:
                # Group by call and summarize
                calls = transcript_df.groupby("call_id")
                call_summaries = []
                for call_id, group in calls:
                    call_name = group.iloc[0].get("call_name", "")
                    call_date = group.iloc[0].get("call_date", "")
                    paragraphs = group.sort_values("paragraph_index")

                    # Extract key paragraphs mentioning expansion keywords
                    expansion_keywords = [
                        "expand", "expans", "migrat", "consolidat", "bill pay",
                        "treasury", "travel", "card", "procurement", "spend",
                        "budget", "ramp up", "timeline", "roi", "pain",
                        "frustrat", "challenge", "blocker", "vendor",
                        "competitor", "switch", "upgrade", "onboard",
                    ]
                    relevant = []
                    for _, p in paragraphs.iterrows():
                        text = str(p.get("paragraph_text", "")).lower()
                        if any(kw in text for kw in expansion_keywords):
                            speaker = p.get("speaker_email", "Unknown")
                            tag = " [Ramp]" if p.get("is_ramp_participant") else ""
                            relevant.append(f"  {speaker}{tag}: {p.get('paragraph_text', '')}")

                    if relevant:
                        call_summaries.append(
                            f"--- {call_date} — {call_name} ---\n" +
                            "\n".join(relevant[:10])
                        )

                if call_summaries:
                    transcript_text = "\n\n".join(call_summaries[:3])
                    # Cap at 8000 chars
                    if len(transcript_text) > 8000:
                        transcript_text = transcript_text[:8000] + "\n[...truncated...]"
        except Exception as e:
            logger.debug("Pre-brief transcript fetch failed for %s: %s", account_name, e)

        # ── 4. SFDC Notes ──
        notes_text = "No SFDC notes on file."
        try:
            notes_df = run_query(ACCOUNT_NOTES_QUERY.format(account_ids=f"'{account_id}'"))
            if not notes_df.empty:
                nr = notes_df.iloc[0]
                parts = []
                for field, label in [
                    ("am_notes", "AM Notes"), ("am_next_steps", "AM Next Steps"),
                    ("csm_notes", "CSM Notes"), ("csm_next_steps", "CSM Next Steps"),
                ]:
                    val = nr.get(field)
                    if val and str(val).strip() and str(val).strip().lower() != "none":
                        parts.append(f"{label}: {val}")
                if parts:
                    notes_text = "\n".join(parts)
        except Exception as e:
            logger.debug("Pre-brief notes fetch failed: %s", e)

        # ── 5. Email context (expansion signals) ──
        email_signals_text = ""
        try:
            emails_df = run_query(ACCOUNT_EMAILS_FULL_QUERY.format(
                account_ids=f"'{account_id}'",
            ))
            if not emails_df.empty:
                signal_emails = []
                for _, em in emails_df.iterrows():
                    has_signal = (
                        em.get("has_painpoints") or em.get("has_interested")
                        or em.get("has_willing_to_meet") or em.get("has_need_information")
                    )
                    if has_signal:
                        flags = []
                        if em.get("has_painpoints"):
                            flags.append("PAIN POINT")
                        if em.get("has_interested"):
                            flags.append("INTERESTED")
                        if em.get("has_willing_to_meet"):
                            flags.append("WILLING TO MEET")
                        if em.get("has_need_information"):
                            flags.append("NEEDS INFO")
                        body = str(em.get("body_text", "") or "")[:500]
                        signal_emails.append(
                            f"- [{', '.join(flags)}] {em.get('email_date', '')} "
                            f"({em.get('direction', '')}) — {em.get('subject', '')}\n"
                            f"  {body}"
                        )
                if signal_emails:
                    email_signals_text = "\n".join(signal_emails[:5])
        except Exception as e:
            logger.debug("Pre-brief email signals fetch failed: %s", e)

        # ── 6. Contacts ──
        contacts = fetch_contact_emails(None, [account_id])
        acct_contacts = contacts.get(account_id, [])
        contacts_text = "\n".join(
            f"- {c.get('name', '')} ({c.get('title', '')}) — {c.get('email', '')}"
            for c in acct_contacts[:5]
        ) if acct_contacts else "No contacts found"

        # ── 7. Calendar attendees ──
        external = extract_external_attendees(meeting)
        attendee_text = "\n".join(
            f"- {a.get('name', a['email'])} ({a['email']}) — RSVP: {a.get('response', '?')}"
            for a in external
        ) if external else "No external attendees listed"

        # Meeting time (display in ET)
        start = meeting.get("start")
        minutes_until = int((start - datetime.utcnow()).total_seconds() / 60) if start else 0
        time_str = format_meeting_time(start)

        # ── 8. Determine uncovered products (no opp) ──
        covered_products = set()
        if not acct_opps.empty:
            for _, r in acct_opps.iterrows():
                covered_products.add(r.get("expansion_subtype", ""))

        uncovered = []
        if card_l30 > 0 and "Card Expansion" not in covered_products:
            uncovered.append(f"Card (spending {format_currency(card_l30)}/mo, no opp)")
        if bp_l30 > 0 and "Bill Pay Expansion" not in covered_products:
            uncovered.append(f"Bill Pay (spending {format_currency(bp_l30)}/mo, no opp)")
        if treasury_l30 > 0 and "Treasury Expansion" not in covered_products:
            uncovered.append(f"Treasury ({format_currency(treasury_l30)} balance, no opp)")

        # Products with zero spend = activation opportunities
        activation_opps = []
        if card_l30 == 0:
            activation_opps.append("Card (not activated)")
        if bp_l30 == 0:
            activation_opps.append("Bill Pay (not activated)")
        if treasury_l30 == 0:
            activation_opps.append("Treasury (not activated)")

        uncovered_text = "\n".join(f"- {u}" for u in uncovered) if uncovered else "All active products have open opps"
        activation_text = "\n".join(f"- {a}" for a in activation_opps) if activation_opps else "All core products activated"

        # ── 9. Generate brief via Claude (expansion-focused) ──
        prompt = f"""You are a sales analyst helping {owner_name}, a Growth AM at Ramp, prepare for a customer call.
{owner_name}'s comp is 75% Realized CP (expansion opps) and 25% SaaS Renewals. They earn comp on incremental spend
above baseline during a 90-day window after closing an opp. Their goal is to maximize SQL production and
expansion discovery during this call.

This meeting is in {minutes_until} minutes (at {time_str}).

MEETING: {meeting.get('title', '')} ({meeting.get('duration_min', 0)} min)
ATTENDEES: {attendee_text}

ACCOUNT: {account_name}
L30D Spend: Card {format_currency(card_l30)} | Bill Pay {format_currency(bp_l30)} | Treasury {format_currency(treasury_l30)}

OPEN EXPANSION OPPS:
{opps_text}

AE QUALIFIED SPEND vs ACTUAL:
{ae_vs_actual_text}

UNCOVERED PRODUCTS (spending but no opp):
{uncovered_text}

ZERO-TO-ONE ACTIVATION OPPORTUNITIES:
{activation_text}

SFDC NOTES:
{notes_text}

KEY SFDC CONTACTS:
{contacts_text}

PAST GONG TRANSCRIPT EXCERPTS (expansion-relevant):
{transcript_text if transcript_text else 'No expansion-related transcript excerpts found.'}

EMAIL SIGNALS (pain points, interest, willingness to meet):
{email_signals_text if email_signals_text else 'No flagged email signals.'}

Write a 200-word expansion-focused pre-call brief covering:
1. SPEND GAP ANALYSIS: Where is actual spend vs AE estimate? Any significant discrepancies to probe?
2. SQL OPPORTUNITIES: Which products should {owner_name} push for? Any uncovered products or activation plays?
3. TRANSCRIPT INTELLIGENCE: What did the customer say about expansion, pain points, or blockers in past calls?
4. PREPARED TALKING POINTS: 3 specific things {owner_name} should bring up to advance expansion or create a new opp
5. THE ASK: One specific close or commitment to attempt on this call

Be direct and specific. Every point should tie to comp impact. This is a 2-minute glance before the call."""

        brief = call_claude(prompt, max_tokens=800)

        # ── 10. Build Slack message ──
        prep_link = dashboard_url("meeting-prep", account=account_name)
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text",
                         "text": f"\U0001f4cb Pre-Call Brief — {account_name} ({time_str})",
                         "emoji": True},
            },
            {
                "type": "context",
                "elements": [{
                    "type": "mrkdwn",
                    "text": (
                        f"Meeting in *{minutes_until} min* · "
                        f"{meeting.get('duration_min', 0)} min · "
                        f"{len(external)} external attendee{'s' if len(external) != 1 else ''} · "
                        f"Matched via {match_source}"
                    ),
                }],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn",
                         "text": (
                             f"*<{sf_link}|{account_name}>*\n"
                             f"Card: {format_currency(card_l30)} | BP: {format_currency(bp_l30)} | "
                             f"Treasury: {format_currency(treasury_l30)}"
                         )},
            },
        ]

        # AE vs Actual callout
        if ae_vs_actual_text and ae_vs_actual_text != "N/A — no open opps":
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn",
                         "text": f"*AE Estimate vs Actual:*\n{ae_vs_actual_text}"},
            })

        # Uncovered products callout
        if uncovered:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn",
                         "text": f"*\u26a0\ufe0f Uncovered Products (no opp):*\n" +
                                 "\n".join(f"- {u}" for u in uncovered)},
            })

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": brief},
        })
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f"<{prep_link}|Full Meeting Prep> · `/gary-brief {account_name}` for expanded brief",
            }],
        })

        client.chat_postMessage(
            channel=dm_target,
            blocks=blocks,
            text=f"Pre-call brief: {account_name} in {minutes_until} min",
        )
        tracker.mark_processed(dedup_key, user_id=user_id)
        logger.info("Pre-meeting brief sent for %s (event=%s)", account_name, event_id)

    except Exception as e:
        logger.error("Pre-meeting brief for %s failed: %s", account_name, e)


def _send_unmatched_alert(meeting, client, dm_target=None):
    """Send a lightweight alert for a meeting we can't match to an account."""
    external = extract_external_attendees(meeting)
    if not external:
        return  # Skip internal-only meetings

    start = meeting.get("start")
    minutes_until = int((start - datetime.utcnow()).total_seconds() / 60) if start else 0
    time_str = format_meeting_time(start)
    attendee_str = ", ".join(
        a.get("name") or a.get("email", "") for a in external[:3]
    )

    client.chat_postMessage(
        channel=dm_target,
        text=(
            f"\U0001f4c5 Meeting in {minutes_until} min: *{meeting['title']}* ({time_str})\n"
            f"Attendees: {attendee_str}\n"
            f"_Couldn't match to an SFDC account — `/gary-brief <name>` to manually prep._"
        ),
    )
