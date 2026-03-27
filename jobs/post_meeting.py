"""Post-meeting to-do job — every 2 hours, 8 AM-6 PM PT.

Analyzes Greg's recent Gong calls to detect:
  1. Meetings without follow-up email sent within 48h
  2. Calls on accounts with no open expansion opp
  3. Calls containing buying signals (AI-detected from transcript)

Sends a Slack DM with flagged items including meeting summary,
detection type, suggested action, and SFDC links.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from queries.queries import (
    POST_MEETING_CALLS_QUERY,
    GONG_MEETINGS_FULL_TRANSCRIPT_QUERY,
    ACCOUNT_NOTES_QUERY,
    ACCOUNT_EMAILS_FULL_QUERY,
    format_query,
)
from core.snowflake_client import run_query
from core.claude_client import call_claude_json
from core.slack_formatter import sf_account_url, sf_opp_url, simple_dm_blocks, dashboard_url
from utils.dedup import tracker
from utils.account_resolver import is_hash_like
from config import GREG_SLACK_ID
from core.user_registry import get_user_sf_name

logger = logging.getLogger(__name__)

MAX_ITEMS = 8
TRANSCRIPT_CHAR_LIMIT = 12000
EMAIL_BODY_CHAR_LIMIT = 1500


# ---------------------------------------------------------------------------
# Helper: build speaker-attributed transcript text
# ---------------------------------------------------------------------------

def _build_transcript_text(rows):
    """Format speaker-attributed transcript paragraphs.

    Parameters
    ----------
    rows : list[dict]
        Rows from GONG_MEETINGS_FULL_TRANSCRIPT_QUERY for a single call_id,
        sorted by paragraph_index.  Expected keys: speaker_email,
        is_ramp_participant, paragraph_text.

    Returns
    -------
    str
        Formatted transcript, truncated to ``TRANSCRIPT_CHAR_LIMIT`` chars.
    """
    if not rows:
        return ""
    lines = []
    for r in rows:
        speaker = r.get("speaker_email") or "Unknown"
        is_ramp = r.get("is_ramp_participant")
        tag = " [Ramp]" if is_ramp else ""
        text = (r.get("paragraph_text") or "").strip()
        if text:
            lines.append(f"{speaker}{tag}: {text}")
    full = "\n".join(lines)
    if len(full) > TRANSCRIPT_CHAR_LIMIT:
        half = TRANSCRIPT_CHAR_LIMIT // 2
        full = full[:half] + "\n\n[...transcript truncated...]\n\n" + full[-half:]
    return full


# ---------------------------------------------------------------------------
# Helper: build SFDC notes text
# ---------------------------------------------------------------------------

def _build_notes_text(notes_row):
    """Format SFDC account notes into a readable block.

    Parameters
    ----------
    notes_row : dict or None
        Single row from ACCOUNT_NOTES_QUERY with keys: am_notes,
        am_next_steps, csm_notes, csm_next_steps.

    Returns
    -------
    str
        Formatted notes text.
    """
    if not notes_row:
        return "No SFDC notes on file."
    parts = []
    for field, label in [
        ("am_notes", "AM Notes"),
        ("am_next_steps", "AM Next Steps"),
        ("csm_notes", "CSM Notes"),
        ("csm_next_steps", "CSM Next Steps"),
    ]:
        val = notes_row.get(field)
        if val and str(val).strip() and str(val).strip().lower() != "none":
            parts.append(f"{label}: {val}")
    return "\n".join(parts) if parts else "No SFDC notes on file."


# ---------------------------------------------------------------------------
# Helper: build recent emails text
# ---------------------------------------------------------------------------

def _build_emails_text(email_rows):
    """Format recent emails with body text.

    Parameters
    ----------
    email_rows : list[dict]
        Rows from ACCOUNT_EMAILS_FULL_QUERY for a single account_id,
        ordered by email_date desc.  Expected keys: email_date, direction,
        subject, body_text, has_willing_to_meet, has_not_interested,
        has_referral, has_ooo.

    Returns
    -------
    str
        Formatted email digest, with each email body capped at
        ``EMAIL_BODY_CHAR_LIMIT`` chars.
    """
    if not email_rows:
        return "No recent Outreach/SFDC-logged emails."
    parts = []
    for e in email_rows[:5]:
        direction = e.get("direction", "")
        date = e.get("email_date", "")
        subject = e.get("subject", "")
        body = str(e.get("body_text", "") or "")
        if len(body) > EMAIL_BODY_CHAR_LIMIT:
            body = body[:EMAIL_BODY_CHAR_LIMIT] + "..."
        flags = []
        if e.get("has_willing_to_meet"):
            flags.append("willing-to-meet")
        if e.get("has_not_interested"):
            flags.append("not-interested")
        if e.get("has_referral"):
            flags.append("referral")
        if e.get("has_ooo"):
            flags.append("OOO")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        header = f"--- {date} ({direction}){flag_str} ---\nSubject: {subject}"
        if body:
            parts.append(f"{header}\n{body}")
        else:
            parts.append(header)
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Main job
# ---------------------------------------------------------------------------

def run_post_meeting(client, user_id=None, lookback_days=2, force=False):
    """Analyze recent Gong calls and DM Greg actionable post-meeting items.

    Parameters
    ----------
    client : slack_sdk.WebClient
        Slack client for sending messages.
    lookback_days : int
        Number of days back to scan for calls (default 2).
    force : bool
        If True, bypass dedup and include previously surfaced calls.
        When True and nothing is found, send an "All clear" message.
    """
    dm_target = user_id or GREG_SLACK_ID
    owner_name = get_user_sf_name(user_id)

    try:
        # ── 1. Fetch recent calls with follow-up/opp checks ─────────────
        calls_sql = format_query(POST_MEETING_CALLS_QUERY, user_id=user_id, lookback_days=lookback_days)
        calls_df = run_query(calls_sql)

        if calls_df.empty:
            logger.info("Post-meeting: no calls found in last %d days", lookback_days)
            if force:
                blocks = simple_dm_blocks(
                    "Post-Meeting To-Do",
                    "All clear — no calls found in the last "
                    f"{lookback_days} days.",
                )
                client.chat_postMessage(
                    channel=dm_target, blocks=blocks,
                    text="Post-Meeting To-Do — All clear",
                )
            return

        # ── 2. Attempt full transcripts, fall back to section summaries ──
        transcript_sql = format_query(
            GONG_MEETINGS_FULL_TRANSCRIPT_QUERY,
            user_id=user_id,
            lookback_days=lookback_days,
        )
        try:
            transcript_df = run_query(transcript_sql)
        except Exception:
            logger.warning(
                "Post-meeting: full transcript query failed, "
                "using section summaries as fallback"
            )
            transcript_df = pd.DataFrame()

        # Index transcript rows by call_id
        transcripts_by_call = defaultdict(list)
        if not transcript_df.empty:
            for _, row in transcript_df.iterrows():
                cid = row.get("call_id")
                if cid:
                    transcripts_by_call[cid].append(row.to_dict())

        # ── 3. Collect unique account IDs for batch SFDC fetch ───────────
        account_ids = set()
        for _, row in calls_df.iterrows():
            aid = row.get("account_id")
            if aid and not is_hash_like(str(aid)):
                account_ids.add(str(aid))

        if not account_ids:
            logger.info("Post-meeting: no valid account IDs found")
            return

        ids_placeholder = ",".join(f"'{aid}'" for aid in account_ids)

        # ── 4. Batch fetch SFDC notes + emails in parallel ───────────────
        notes_by_account = {}
        emails_by_account = defaultdict(list)

        def _fetch_notes():
            sql = ACCOUNT_NOTES_QUERY.format(account_ids=ids_placeholder)
            return run_query(sql)

        def _fetch_emails():
            sql = ACCOUNT_EMAILS_FULL_QUERY.format(account_ids=ids_placeholder)
            return run_query(sql)

        with ThreadPoolExecutor(max_workers=2) as pool:
            future_notes = pool.submit(_fetch_notes)
            future_emails = pool.submit(_fetch_emails)

            for future in as_completed([future_notes, future_emails]):
                try:
                    future.result()
                except Exception as exc:
                    logger.warning(
                        "Post-meeting: parallel SFDC fetch error: %s", exc
                    )

        try:
            notes_df = future_notes.result()
            if not notes_df.empty:
                for _, row in notes_df.iterrows():
                    aid = row.get("account_id")
                    if aid:
                        notes_by_account[str(aid)] = row.to_dict()
        except Exception:
            logger.warning("Post-meeting: notes fetch failed")

        try:
            emails_df = future_emails.result()
            if not emails_df.empty:
                for _, row in emails_df.iterrows():
                    aid = row.get("account_id")
                    if aid:
                        emails_by_account[str(aid)].append(row.to_dict())
        except Exception:
            logger.warning("Post-meeting: emails fetch failed")

        # ── 5. Filter calls: dedup + collect candidates ──────────────────
        candidate_calls = []

        for _, row in calls_df.iterrows():
            call_id = row.get("call_id")
            if not call_id:
                continue

            dedup_key = f"meeting_{call_id}"

            # Dedup check — skip previously surfaced unless force=True
            if not force and tracker.is_processed(dedup_key, user_id=dm_target):
                continue

            missing_followup = bool(row.get("missing_followup"))
            no_open_opp = bool(row.get("no_open_opp"))

            candidate_calls.append({
                "call_id": call_id,
                "account_id": str(row.get("account_id", "")),
                "account_name": row.get("account_name", "Unknown"),
                "call_name": row.get("call_name", ""),
                "call_date": str(row.get("call_date", "")),
                "duration_min": int(row.get("duration_min", 0) or 0),
                "linked_opp_id": row.get("linked_opp_id") or "",
                "missing_followup": missing_followup,
                "no_open_opp": no_open_opp,
                "opp_count": int(row.get("opp_count", 0) or 0),
                "opp_products": row.get("opp_products") or "",
                "competitors_mentioned": row.get("competitors_mentioned") or "",
                "products_mentioned": row.get("products_mentioned") or "",
                "full_section_text": row.get("full_section_text") or "",
                "all_product_requests": row.get("all_product_requests") or "",
                "latest_opp_id": row.get("latest_opp_id") or "",
                "latest_stage": row.get("latest_stage") or "",
                "dedup_key": dedup_key,
            })

        if not candidate_calls:
            logger.info("Post-meeting: no actionable meetings found")
            if force:
                blocks = simple_dm_blocks(
                    "Post-Meeting To-Do",
                    "All clear — all recent meetings have been "
                    "followed up on and have open opps. No new calls "
                    "to analyze.",
                )
                client.chat_postMessage(
                    channel=dm_target, blocks=blocks,
                    text="Post-Meeting To-Do — All clear",
                )
            return

        # ── 6. Send each call to Claude for analysis ─────────────────────
        analyzed_items = []

        for call in candidate_calls:
            if len(analyzed_items) >= MAX_ITEMS:
                break

            call_id = call["call_id"]
            account_id = call["account_id"]

            # Build transcript context — prefer full, fall back to sections
            transcript_rows = transcripts_by_call.get(call_id, [])
            if transcript_rows:
                transcript_text = _build_transcript_text(transcript_rows)
            else:
                transcript_text = call.get("full_section_text", "")
                if transcript_text:
                    transcript_text = f"[Section summaries]\n{transcript_text}"

            # Build SFDC notes
            notes_text = _build_notes_text(
                notes_by_account.get(account_id)
            )

            # Build recent emails
            emails_text = _build_emails_text(
                emails_by_account.get(account_id, [])
            )

            # Build open opp context
            if call["no_open_opp"]:
                opp_context = (
                    "No open expansion opportunities exist for this account."
                )
            else:
                opp_context = (
                    f"Open opps ({call['opp_count']}): "
                    f"{call['opp_products']}. "
                    f"Latest stage: {call['latest_stage']}."
                )

            prompt = f"""You are an AI sales analyst helping {owner_name}, a Growth Account Manager at Ramp.
{owner_name} manages ~4,000 Plus segment accounts. Their comp is 75% Realized CP (expansion opps) and 25% SaaS Renewals.
They earn comp on incremental spend above baseline during a 90-day window after closing an opp.
Timing opp closes is critical — close too late and the baseline rises, permanently reducing their comp.

Analyze this recent Gong call and determine what actions {owner_name} should take.

CALL INFO:
- Account: {call['account_name']}
- Call Title: {call['call_name']}
- Call Date: {call['call_date']}
- Duration: {call['duration_min']} minutes
- Follow-up email sent within 48h: {'No' if call['missing_followup'] else 'Yes'}
- Competitors mentioned: {call['competitors_mentioned'] or 'None'}
- Products mentioned: {call['products_mentioned'] or 'None'}
- Product requests: {call['all_product_requests'] or 'None'}

OPEN OPP STATUS:
{opp_context}

TRANSCRIPT / CALL SUMMARY:
{transcript_text or 'No transcript available.'}

SFDC NOTES:
{notes_text}

RECENT EMAILS (last 90 days):
{emails_text}

Based on this information, assess:
1. Was there a buying signal? (customer expressing interest in expanding, activating a new product, increasing spend, consolidating vendors, upgrading to Plus/Procurement, timeline commitments, budget discussions, decision-maker engagement)
2. Should {owner_name} send a follow-up email? If so, what should it cover?
3. Should a new Salesforce opportunity be created? If so, for which product(s) and estimated amount?

Return a JSON object with these exact keys:
- "meeting_summary": string (2-3 sentence summary of what was discussed)
- "detection_type": string (one of: "no_followup", "no_opp", "buying_signal", or "no_followup_and_no_opp" if both apply)
- "suggested_action": string (specific, actionable recommendation for {owner_name}, 1-2 sentences)
- "next_steps": string (concrete next steps, e.g. "Send follow-up email to [name] re: card migration timeline. Create Card Expansion opp for ~$X/mo.")

Important: Only flag as "buying_signal" if there is genuine evidence in the transcript/summary. Do not hallucinate signals.
If the call has no missing follow-up, has open opps, and no buying signals, set detection_type to "none".
Return ONLY the JSON object, no markdown fences or extra text."""

            try:
                result = call_claude_json(prompt, max_tokens=800)
                detection = result.get("detection_type", "none")

                # Skip calls where Claude found nothing actionable
                if detection == "none":
                    continue

                analyzed_items.append({
                    **call,
                    "meeting_summary": result.get("meeting_summary", ""),
                    "detection_type": detection,
                    "suggested_action": result.get("suggested_action", ""),
                    "next_steps": result.get("next_steps", ""),
                })
            except Exception as exc:
                logger.warning(
                    "Post-meeting: Claude analysis failed for call %s: %s",
                    call_id, exc,
                )
                # Still surface calls with clear structural flags even if
                # Claude fails — missing follow-up or no opp
                if call["missing_followup"] or call["no_open_opp"]:
                    if call["missing_followup"] and call["no_open_opp"]:
                        detection = "no_followup_and_no_opp"
                    elif call["missing_followup"]:
                        detection = "no_followup"
                    else:
                        detection = "no_opp"
                    analyzed_items.append({
                        **call,
                        "meeting_summary": "Claude analysis unavailable.",
                        "detection_type": detection,
                        "suggested_action": (
                            "Send follow-up email and review call for opp creation."
                            if call["missing_followup"] and call["no_open_opp"]
                            else "Send follow-up email."
                            if call["missing_followup"]
                            else "Review call for opp creation."
                        ),
                        "next_steps": "",
                    })

        # ── 7. If nothing actionable after analysis, exit ────────────────
        if not analyzed_items:
            logger.info(
                "Post-meeting: no actionable meetings after Claude analysis"
            )
            if force:
                blocks = simple_dm_blocks(
                    "Post-Meeting To-Do",
                    "All clear — all recent meetings have been "
                    "followed up on and have open opps. No buying "
                    "signals detected.",
                )
                client.chat_postMessage(
                    channel=dm_target, blocks=blocks,
                    text="Post-Meeting To-Do — All clear",
                )
            return

        # ── 8. Mark items as processed in dedup tracker ──────────────────
        for item in analyzed_items:
            tracker.mark_processed(item["dedup_key"], user_id=dm_target)

        # ── 9. Build Slack Block Kit message and send ────────────────────
        blocks = _build_slack_blocks(analyzed_items)
        client.chat_postMessage(
            channel=dm_target,
            blocks=blocks,
            text=f"Post-Meeting To-Do — {len(analyzed_items)} item(s) flagged",
        )
        logger.info(
            "Post-meeting sent: %d items flagged from last %d days",
            len(analyzed_items), lookback_days,
        )

    except Exception as exc:
        logger.error("Post-meeting job failed: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Slack Block Kit builder
# ---------------------------------------------------------------------------

_DETECTION_LABELS = {
    "no_followup": "Missing Follow-Up Email",
    "no_opp": "No Open Opportunity",
    "buying_signal": "Buying Signal Detected",
    "no_followup_and_no_opp": "Missing Follow-Up + No Open Opp",
}


def _build_slack_blocks(items):
    """Build Slack Block Kit blocks for the post-meeting DM.

    Parameters
    ----------
    items : list[dict]
        Analyzed call items with keys: account_name, call_date, call_name,
        detection_type, meeting_summary, suggested_action, next_steps,
        account_id, linked_opp_id, latest_opp_id.

    Returns
    -------
    list[dict]
        Slack Block Kit blocks.
    """
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Post-Meeting To-Do — {len(items)} item(s)",
                "emoji": True,
            },
        },
    ]

    for item in items:
        account_name = item.get("account_name", "Unknown")
        account_id = item.get("account_id", "")
        call_date = item.get("call_date", "")
        call_name = item.get("call_name", "")
        detection_type = item.get("detection_type", "")
        summary = item.get("meeting_summary", "")
        action = item.get("suggested_action", "")
        next_steps = item.get("next_steps", "")

        detection_label = _DETECTION_LABELS.get(detection_type, detection_type)

        # Determine SFDC link — prefer linked opp, then latest opp, then account
        linked_opp = item.get("linked_opp_id") or ""
        latest_opp = item.get("latest_opp_id") or ""
        if linked_opp:
            sf_link = sf_opp_url(linked_opp)
            sf_label = "View Opp"
        elif latest_opp:
            sf_link = sf_opp_url(latest_opp)
            sf_label = "View Opp"
        else:
            sf_link = sf_account_url(account_id)
            sf_label = "View Account"

        account_link = f"<{sf_account_url(account_id)}|{account_name}>"

        lines = [
            f"*{account_link}*  |  {call_date}  |  _{call_name}_",
            f"*Detected:* {detection_label}",
        ]
        if summary:
            lines.append(f"*Summary:* {summary}")
        if action:
            lines.append(f"*Action:* {action}")
        if next_steps:
            lines.append(f"*Next Steps:* {next_steps}")
        lines.append(f"<{sf_link}|{sf_label}>")

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(lines),
            },
        })

    # Footer with dashboard links
    _pm = dashboard_url("post-meeting")
    _pipe = dashboard_url("pipeline")
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"<{_pm}|Post-Meeting To-Do> · <{_pipe}|Pipeline> · `/post-meeting` to refresh"}],
    })

    return blocks
