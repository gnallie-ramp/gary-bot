"""Post-Meeting Follow-Up — Gong API real-time transcripts.

Runs every 30 min. Fetches recent Gong calls via the Gumstack Gong MCP API
for real-time transcripts (available ~10-15 min after call ends). Falls back
to Snowflake if the Gong API is unavailable.

When a new transcript is found, runs the same analysis pipeline as
granola_followup.py and DMs the user with:
  - Meeting summary and buying signals
  - Follow-up email draft (auto-created in Gmail Drafts)
  - Participant info
  - Opp creation suggestions with pre-filled fields

Works alongside granola_followup.py (which runs every 3 min). Whichever
source provides a transcript first wins — shared dedup keys prevent
duplicate processing.
"""
from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

from core.claude_client import call_claude_json
from core.slack_formatter import (
    sf_account_url, sf_opp_url, format_currency, dashboard_url,
    build_sf_new_opp_url, EXPANSION_PRODUCT_MAP,
)
from utils.dedup import tracker
from utils.account_matcher import match_account
from config import GREG_SLACK_ID, NTR_RATES, COMMAND_PREFIX
from core.user_registry import get_user_sf_name, get_user_booking_link

logger = logging.getLogger(__name__)


def run_post_meeting_followup(client, user_id=None, force=False):
    """Check for new Gong transcripts and analyze them.

    Primary source: Gong API (real-time, ~10-15 min after call ends).
    Fallback: Snowflake (overnight sync, if API unavailable).

    Parameters
    ----------
    client : slack_sdk.WebClient
    user_id : str, optional
        Slack user ID to scope results for. Defaults to original owner.
    force : bool
        If True, process all recent transcripts regardless of dedup.
    """
    dm_target = user_id or GREG_SLACK_ID
    owner_name = get_user_sf_name(user_id)
    booking_link = get_user_booking_link(user_id)

    try:
        # Try Gong API first (real-time)
        results = _run_via_gong_api(client, dm_target, owner_name, booking_link, user_id, force)

        if results is None:
            # Gong API unavailable — fall back to Snowflake
            logger.info("Gong API unavailable, falling back to Snowflake")
            results = _run_via_snowflake(client, dm_target, owner_name, booking_link, force, user_id=user_id)

        if not results:
            if force:
                client.chat_postMessage(
                    channel=dm_target,
                    text="No new Gong transcripts found.",
                )
            return

        _send_followup_dm(results, client, dm_target=dm_target)

    except Exception as e:
        logger.error("Post-meeting followup (Gong) failed: %s", e)
        if force:
            client.chat_postMessage(
                channel=dm_target,
                text=f"Post-meeting followup check failed: {e}",
            )


def _run_via_gong_api(client, dm_target, owner_name, booking_link, user_id, force):
    """Fetch recent calls from Gong API and process new transcripts.

    Returns list of result dicts, empty list if no new calls, or None if
    the API is unavailable (caller should fall back to Snowflake).
    """
    from core.gumstack_gong import is_available, list_calls, get_call_transcript

    if not is_available(user_id):
        return None

    try:
        # Get calls from last 2 days
        from_date = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%dT00:00:00Z")
        to_date = datetime.utcnow().strftime("%Y-%m-%dT23:59:59Z")
        calls = list_calls(from_date=from_date, to_date=to_date, max_results=30, user_id=user_id)
    except Exception as e:
        logger.warning("Gong API list_calls failed: %s", e)
        return None

    if not calls:
        return []

    # Resolve user's email for participant filtering
    from core.user_registry import get_user_email
    user_email = get_user_email(dm_target).lower()

    # Filter to calls the user was on and not yet processed
    new_calls = []
    for call in calls:
        call_id = call.get("id", "")
        if not call_id:
            continue

        # Only process calls where the user was a participant.
        # Check multiple possible keys for participant data in Gong API response.
        parties = (
            call.get("parties", [])
            or call.get("participants", [])
            or call.get("attendees", [])
            or []
        )
        party_emails = set()
        for p in parties:
            for key in ("emailAddress", "email", "speakerEmail", "speaker_email"):
                em = (p.get(key, "") or "").lower().strip()
                if em and "@" in em:
                    party_emails.add(em)

        # If the call has NO participant data at all, skip it — we can't verify
        # the user was on this call, and processing it risks drafting emails for
        # calls the user wasn't part of (e.g. SDR/BDR Orum calls).
        if not party_emails:
            logger.debug("Skipping Gong call %s — no participant data available", call_id)
            continue

        if user_email and user_email not in party_emails:
            continue

        dedup_key = f"gong_rt_{call_id}"
        batch_key = f"meeting_{call_id}"
        if not force and (tracker.is_processed(dedup_key, user_id=user_id) or tracker.is_processed(batch_key, user_id=user_id)):
            continue
        new_calls.append((call, dedup_key))

    if not new_calls:
        return []

    # Process each new call
    results = []
    for call, dedup_key in new_calls[:5]:
        call_id = call.get("id", "")

        # Get transcript
        transcript_text = get_call_transcript(call_id, user_id=user_id)
        if not transcript_text:
            logger.info("No transcript yet for Gong call %s, skipping", call_id)
            continue

        # Extract call metadata
        title = call.get("title", "") or call.get("name", "") or "Untitled"
        started = call.get("started", "") or call.get("startTime", "")
        duration = call.get("duration", 0) or call.get("durationSeconds", 0)
        duration_min = int(duration / 60) if duration > 60 else int(duration)

        # Extract call date
        call_date = ""
        if started:
            try:
                dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                call_date = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                call_date = started[:10] if started else ""

        # Extract participant emails from Gong call metadata
        participants = _extract_participants(call)

        # Match to SFDC account (same approach as granola_followup)
        title_clean = _clean_call_title(title)
        account_match = match_account(
            account_name=title_clean,
            participant_emails=participants,
            user_id=user_id,
        )
        account_name = account_match.account_name if account_match.matched else title_clean
        account_id = account_match.account_id if account_match.matched else ""

        meta = {
            "call_id": call_id,
            "account_id": account_id,
            "account_name": account_name,
            "call_name": title,
            "call_date": call_date,
            "duration_min": duration_min,
        }

        result = _analyze_call(
            call_id, meta, transcript_text, participants,
            owner_name=owner_name, booking_link=booking_link,
            user_id=user_id,
        )
        if result:
            results.append(result)
            tracker.mark_processed(dedup_key, user_id=user_id)
            tracker.mark_processed(f"meeting_{call_id}", user_id=user_id)

    return results


def _run_via_snowflake(client, dm_target, owner_name, booking_link, force, user_id=None):
    """Fall back to Snowflake for Gong transcripts (overnight sync)."""
    try:
        import pandas as pd
        from core.snowflake_client import run_query
        from queries.queries import GONG_MEETINGS_FULL_TRANSCRIPT_QUERY, format_query
        from jobs.post_meeting import _build_transcript_text

        transcript_df = run_query(
            format_query(GONG_MEETINGS_FULL_TRANSCRIPT_QUERY, user_id=user_id, lookback_days=2)
        )
        if transcript_df.empty:
            return []

        calls = transcript_df.groupby("call_id")
        new_calls = []
        for call_id, group in calls:
            dedup_key = f"gong_rt_{call_id}"
            batch_key = f"meeting_{call_id}"
            if not force and (tracker.is_processed(dedup_key, user_id=user_id) or tracker.is_processed(batch_key, user_id=user_id)):
                continue

            first = group.iloc[0]
            meta = {
                "call_id": call_id,
                "account_id": str(first.get("account_id", "")),
                "account_name": first.get("account_name", ""),
                "call_name": first.get("call_name", ""),
                "call_date": str(first.get("call_date", "")),
                "duration_min": int(first.get("duration_min", 0) or 0),
            }
            rows = group.sort_values("paragraph_index").to_dict("records")
            transcript_text = _build_transcript_text(rows)

            # Extract participants from Snowflake rows
            participants = []
            for row in rows:
                speaker = row.get("speaker_email", "")
                is_ramp = row.get("is_ramp_participant")
                if speaker and not is_ramp and speaker not in participants:
                    participants.append(speaker)

            new_calls.append((call_id, meta, transcript_text, participants))

        results = []
        for call_id, meta, transcript_text, participants in new_calls[:5]:
            dedup_key = f"gong_rt_{call_id}"
            result = _analyze_call(
                call_id, meta, transcript_text, participants,
                owner_name=owner_name, booking_link=booking_link,
                user_id=user_id,
            )
            if result:
                results.append(result)
                tracker.mark_processed(dedup_key, user_id=user_id)
                tracker.mark_processed(f"meeting_{call_id}", user_id=user_id)

        return results

    except Exception as e:
        logger.error("Snowflake fallback failed: %s", e)
        return []


def _extract_participants(call: dict) -> list[str]:
    """Extract external participant emails from a Gong API call object."""
    participants = []
    parties = call.get("parties", []) or call.get("participants", [])
    for party in parties:
        email = party.get("emailAddress", "") or party.get("email", "")
        # Skip Ramp internal emails
        if email and not email.endswith("@ramp.com"):
            participants.append(email)
    return participants


def _clean_call_title(title: str) -> str:
    """Extract probable account name from a Gong call title.

    Common patterns: "Ramp // Acme Corp", "Acme <> Ramp", "Acme - Ramp (Treasury)"
    """
    clean = title
    # Strip trailing parenthetical product labels
    clean = re.sub(r'\s*\(Ramp\s+\w+\)\s*$', '', clean).strip()
    # Split on common separators and pick the non-Ramp part
    for sep in [" // ", " <> ", " - ", " | ", " / "]:
        parts = clean.split(sep)
        non_ramp = [p.strip() for p in parts if not re.match(r'^ramp\b', p.strip(), re.IGNORECASE)]
        if non_ramp and len(parts) > 1:
            clean = non_ramp[0]
            break
    return clean


def _analyze_call(
    call_id: str,
    meta: dict,
    transcript_text: str,
    participants: list[str],
    owner_name: str = "",
    booking_link: str = "",
    user_id: str = None,
):
    """Analyze a single Gong call transcript. Returns result dict or None."""
    account_id = meta["account_id"]
    account_name = meta["account_name"]

    # Fetch SFDC notes
    notes_text = "No SFDC notes on file."
    if account_id:
        try:
            from core.snowflake_client import run_query
            from queries.queries import ACCOUNT_NOTES_QUERY
            from jobs.post_meeting import _build_notes_text
            notes_df = run_query(ACCOUNT_NOTES_QUERY.format(
                account_ids=f"'{account_id}'"
            ))
            if not notes_df.empty:
                notes_text = _build_notes_text(notes_df.iloc[0].to_dict())
        except Exception:
            pass

    # Fetch recent emails
    emails_text = "No recent emails."
    if account_id:
        try:
            from core.snowflake_client import run_query
            from queries.queries import ACCOUNT_EMAILS_FULL_QUERY
            from jobs.post_meeting import _build_emails_text
            emails_df = run_query(ACCOUNT_EMAILS_FULL_QUERY.format(
                account_ids=f"'{account_id}'"
            ))
            if not emails_df.empty:
                emails_text = _build_emails_text(emails_df.to_dict("records"))
        except Exception:
            pass

    # Fetch open opps
    opp_context = ""
    try:
        from core.snowflake_client import run_query
        from queries.queries import ACCOUNT_OPPS_QUERY, format_query
        opps_df = run_query(format_query(ACCOUNT_OPPS_QUERY, user_id=user_id))
        if not opps_df.empty:
            acct_opps = opps_df[opps_df["account_id"] == account_id]
            if not acct_opps.empty:
                opp_lines = []
                for _, r in acct_opps.iterrows():
                    opp_lines.append(
                        f"- {r['expansion_subtype']} ({r['opportunity_stage_name']}) — "
                        f"{format_currency(float(r.get('monthly_expansion_amount', 0) or 0))}/mo"
                    )
                opp_context = "\n".join(opp_lines)
            else:
                opp_context = "No open expansion opps for this account."
        else:
            opp_context = "No open expansion opps."
    except Exception:
        opp_context = "Could not fetch opp data."

    # Fallback: if Gong didn't capture external emails, use SFDC contacts
    if not participants and account_id:
        try:
            from utils.account_resolver import fetch_contact_emails, is_hash_like
            contacts_by_account = fetch_contact_emails(None, [account_id])
            for c in contacts_by_account.get(account_id, []):
                email = c.get("email", "")
                if email and not is_hash_like(c.get("name", "")):
                    participants.append(email)
                    if len(participants) >= 3:
                        break
        except Exception:
            pass

    # Claude analysis
    prompt = f"""You are an AI sales analyst helping {owner_name}, a Growth Account Manager at Ramp.
{owner_name} manages ~4,000 Plus segment accounts. Their comp is 75% Realized CP (expansion opps) and 25% SaaS Renewals.
They earn comp on incremental spend above baseline during a 90-day window after closing an opp.
Closing too late = baseline rises = comp reduced. Speed matters.

NTR rates for CP calculation:
- Card: 95 bps (0.0095)
- Bill Pay: 15 bps (0.0015)
- Treasury: 5 bps (0.0005)
- Travel: 350 bps (0.035)

A Gong transcript just became available for this call. Analyze it and provide actionable follow-up.

CALL INFO:
- Account: {account_name}
- Call Title: {meta['call_name']}
- Call Date: {meta['call_date']}
- Duration: {meta['duration_min']} minutes
- External Participants: {', '.join(participants) if participants else 'Unknown'}

OPEN EXPANSION OPPS:
{opp_context}

TRANSCRIPT:
{transcript_text[:20000] if transcript_text else 'No transcript available.'}

SFDC NOTES:
{notes_text}

RECENT EMAILS (last 90 days):
{emails_text}

Return a JSON object with these exact keys:
- "meeting_summary": string — 2-3 sentence summary
- "highlights": list of strings — 3-5 key bullet points from the call (specific, actionable)
- "buying_signals": string — specific quotes or evidence. "None detected" if nothing found.
- "follow_up_email_to": string — best email to send follow-up to (from participant list, or empty)
- "follow_up_email_subject": string — format: "Ramp Follow-Up - <brief summary of key topics discussed>"
- "follow_up_email_body": string — HTML formatted follow-up email. Structure it with:
    * A brief opening referencing specific discussion points from the call
    * One section per product discussed (use <strong> for section headers like "Treasury — Getting Started", "Bill Pay")
    * Within each section, reference specific details from the call (amounts mentioned, pain points, current tools)
    * Use bullet points (<ul><li>) for feature highlights relevant to what was discussed
    * End with a clear next step and offer to help
    * Sign off as: {owner_name}\\nAccount Manager @ Ramp\\nBook a meeting: {booking_link}
    * Tone: warm, helpful, consultative — like a trusted advisor, not salesy
    * Do NOT use markdown — use HTML tags only (<strong>, <ul>, <li>, <a>, <br>, <p>)
- "follow_up_email_cc": string — other emails to CC, comma-separated, or empty
- "opps": list of objects, each with:
    - "product": string (one of "Card Expansion", "Bill Pay Expansion", "Travel Expansion", "Treasury Expansion", "Procurement")
    - "stage": string (always "S2: Sales Qualified Opportunity")
    - "monthly_amount": integer — the EXACT monthly dollar amount discussed on the call for this product. Use the customer's own words/numbers from the transcript, not estimates. For bill pay / AP, use the monthly AP volume they stated. For treasury, use the balance or deposit amount discussed. If no specific amount was stated but the product was discussed with mild interest or was a major talking point, use 15000 as the default amount.
    - "rationale": string (1 sentence — why this opp, citing transcript evidence)
    - "close_date": string (YYYY-MM-DD). Use the specific date if a timeline was explicitly discussed on the call. Otherwise leave empty and the system will default to end of current month.
    - "next_step": string — the specific next action for this product based on what was discussed. Be concrete and reference the transcript.
    - "next_step_due_date": string (YYYY-MM-DD). Use the specific date if a timeline was explicitly discussed. Otherwise leave empty and the system will default to 1 week from today.
  Include products that were discussed with genuine interest OR were a major talking point on the call, even if no specific dollar amount was mentioned. Use 15000 as the default monthly_amount when no amount was stated. Empty list only if no products were discussed at all.
- "next_steps": list of strings — 1-3 concrete next steps for {owner_name} (overall, not per-product)
- "urgency": string — "high", "medium", or "low"
- "opp_updates": list of objects for EXISTING open opps that should be updated based on the call. Each with:
    - "product": string — which existing opp product to update (must match one from OPEN EXPANSION OPPS above)
    - "field_updates": object with any of these keys:
        - "next_step": string — new next step based on what was discussed
        - "close_date": string (YYYY-MM-DD) — updated close date if timeline changed
        - "stage": string — new stage if opp should be advanced (e.g. "S3: Solution Validation")
    - "rationale": string — why this update, citing transcript evidence
  Only suggest updates when the call clearly provides new information for an existing opp. Empty list if no updates needed.

IMPORTANT: Flag buying signals and opps when there is genuine evidence OR mild intent (product was discussed, customer showed interest, or it was a significant talking point). Use exact amounts from the transcript when available; default to $15,000/mo when a product was discussed but no specific amount was stated.
IMPORTANT: If there are EXISTING OPEN OPPS listed above, check if the call discussed those products. If so, suggest updates to those opps via "opp_updates" (do NOT create duplicate opps for products that already have open opps). Only use "opps" for NEW products without an existing opp.
Return ONLY valid JSON, no markdown fences."""

    try:
        result = call_claude_json(prompt, max_tokens=2500)
    except Exception as e:
        logger.warning("Claude analysis failed for Gong followup %s: %s", call_id, e)
        result = {
            "meeting_summary": "Claude analysis unavailable.",
            "highlights": [],
            "buying_signals": "Analysis failed",
            "follow_up_email_to": participants[0] if participants else "",
            "follow_up_email_subject": f"Follow-up: {meta['call_name']}",
            "follow_up_email_body": "",
            "follow_up_email_cc": "",
            "opps": [],
            "next_steps": ["Review call recording manually."],
            "urgency": "medium",
        }

    return {
        **meta,
        **result,
        "participants": participants,
    }


def _send_followup_dm(results, client, dm_target=None):
    """Send a consolidated Gong follow-up DM — aligned with Granola DM format."""
    from datetime import datetime, timedelta

    blocks = [{
        "type": "header",
        "text": {"type": "plain_text",
                 "text": f"\U0001f3ac Gong Follow-Up — {len(results)} call(s)",
                 "emoji": True},
    }]

    for item in results:
        account_name = item.get("account_name", "Unknown")
        account_id = item.get("account_id", "")
        call_name = item.get("call_name", "")
        call_date = item.get("call_date", "")
        urgency = item.get("urgency", "medium")
        summary = item.get("meeting_summary", "")
        signals = item.get("buying_signals", "")
        highlights = item.get("highlights", [])
        next_steps = item.get("next_steps", [])
        opps = item.get("opps", [])

        urgency_emoji = {
            "high": "\U0001f534", "medium": "\U0001f7e1", "low": "\U0001f7e2"
        }.get(urgency, "\U0001f7e1")

        sf_link = sf_account_url(account_id) if account_id else ""
        account_link = f"<{sf_link}|{account_name}>" if sf_link else f"*{account_name}*"

        blocks.append({"type": "divider"})

        # Header line
        header_line = f"{urgency_emoji} {account_link}  |  {call_date}  |  _{call_name}_"
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": header_line},
        })

        # Summary
        if summary:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Summary:* {summary}"},
            })

        # Highlights as bullets
        if highlights:
            highlight_text = "\n".join(f"\u2022 {h}" for h in highlights[:5])
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Meeting Highlights:*\n{highlight_text}"},
            })

        # Buying signals
        if signals and signals.lower() != "none detected":
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Buying Signals:* {signals}"},
            })

        # Next steps
        if next_steps:
            if isinstance(next_steps, list):
                steps_text = "\n".join(f"\u2022 {s}" for s in next_steps)
            else:
                steps_text = str(next_steps)
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Next Steps:*\n{steps_text}"},
            })

        # Follow-up email section
        email_to = item.get("follow_up_email_to", "")
        email_subject = item.get("follow_up_email_subject", "")
        email_body = item.get("follow_up_email_body", "")

        if email_to and email_body:
            cc = item.get("follow_up_email_cc", "") or ""

            html_body = email_body

            try:
                from templates.signature import build_signature
                sig_html = build_signature(user_id=dm_target)
            except ImportError:
                sig_html = ""
            if sig_html:
                html_body += f"<br>{sig_html}"

            # Create Gmail draft directly via Gumstack MCP
            call_id = item.get("call_id", "")
            pending_id = f"gong_{call_id[:12]}_{email_to.split('@')[0]}"
            draft_label = "Claude Drafts/Post Meeting"

            from core.gumstack_gmail import create_draft as gumstack_create_draft, is_available as gumstack_ok
            if gumstack_ok():
                gm_result = gumstack_create_draft(
                    to=email_to, subject=email_subject, html_body=html_body,
                    cc=cc, label=draft_label, user_id=dm_target,
                )
                if gm_result["success"]:
                    draft_text = (
                        f"\u2709\ufe0f *Gmail draft created* \u2192 _{email_subject}_\n"
                        f"*To:* {email_to}"
                    )
                    if cc:
                        draft_text += f"  |  *CC:* {cc}"
                    draft_text += f"\n\u2705 _Labeled: {draft_label}_"
                else:
                    from utils.pending_drafts import save_draft as save_pending_draft
                    save_pending_draft(
                        draft_id=pending_id, to=email_to, cc=cc,
                        subject=email_subject, html_body=html_body,
                        account_name=account_name, meeting_id=call_id,
                        label=draft_label, user_id=dm_target,
                    )
                    draft_text = (
                        f"\u2709\ufe0f *Draft queued* \u2192 _{email_subject}_\n"
                        f"*To:* {email_to}"
                    )
                    if cc:
                        draft_text += f"  |  *CC:* {cc}"
                    draft_text += "\n\u26a0\ufe0f _Direct creation failed — Glass cron will pick up_"
            else:
                from utils.pending_drafts import save_draft as save_pending_draft
                save_pending_draft(
                    draft_id=pending_id, to=email_to, cc=cc,
                    subject=email_subject, html_body=html_body,
                    account_name=account_name, meeting_id=call_id,
                    label=draft_label, user_id=dm_target,
                )
                draft_text = (
                    f"\u2709\ufe0f *Draft queued* \u2192 _{email_subject}_\n"
                    f"*To:* {email_to}"
                )
                if cc:
                    draft_text += f"  |  *CC:* {cc}"
                draft_text += "\n_Gmail draft auto-creates within ~1 min_"
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": draft_text},
            })

            blocks.append({
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Create Draft via Glass", "emoji": True},
                    "action_id": f"glass_email_draft_{call_id[:8]}",
                    "value": json.dumps({"draft_id": pending_id}),
                }],
            })

        # Per-product opp suggestions with inline Create buttons
        if opps:
            for i, opp in enumerate(opps):
                product = opp.get("product", "")
                stage = opp.get("stage", "S2: Sales Qualified Opportunity")
                amount = opp.get("monthly_amount", 0)
                rationale = opp.get("rationale", "")
                close_date = opp.get("close_date", "")
                next_step = opp.get("next_step", "")
                next_step_due = opp.get("next_step_due_date", "")

                if not close_date:
                    now = datetime.utcnow()
                    close_date = ((now.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
                if not next_step_due:
                    next_step_due = (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%d")

                ntr_rate = NTR_RATES.get(product, 0)
                est_cp = float(amount) * ntr_rate * 3 if amount and ntr_rate else 0

                subtype = EXPANSION_PRODUCT_MAP.get(product, product)
                opp_name = f"{account_name} - {subtype}"

                opp_lines = [f"\U0001f4b0 *{product}* | {format_currency(float(amount))}/mo | Est. CP: {format_currency(est_cp)}"]
                if rationale:
                    opp_lines.append(f"  _{rationale}_")
                opp_lines.append(f"  `Name:` {opp_name}")
                opp_lines.append(f"  `Stage:` {stage}")
                opp_lines.append(f"  `Close Date:` {close_date}")
                if next_step:
                    opp_lines.append(f"  `Next Step:` {next_step}")
                    opp_lines.append(f"  `Next Step Due:` {next_step_due}")

                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(opp_lines)},
                })

                # Create Opp button payload
                call_id = item.get("call_id", "")
                gong_link = f"https://us-11688.app.gong.io/call?id={call_id}" if call_id else ""
                opp_payload = json.dumps({
                    "account_name": account_name,
                    "account_id": account_id,
                    "product": product,
                    "stage": stage,
                    "amount": amount,
                    "close_date": close_date,
                    "rationale": rationale,
                    "next_step": next_step,
                    "next_step_due_date": next_step_due,
                    "meeting_id": call_id,
                    "gong_link": gong_link,
                })

                blocks.append({
                    "type": "actions",
                    "elements": [{
                        "type": "button",
                        "text": {"type": "plain_text", "text": f"Create {EXPANSION_PRODUCT_MAP.get(product, product)} Opp", "emoji": True},
                        "action_id": f"create_opp_sfdc_{i}_{call_id[:8]}",
                        "value": opp_payload,
                        "style": "primary",
                    }],
                })

        # Opp update suggestions
        opp_updates = item.get("opp_updates", [])
        if opp_updates:
            for j, update in enumerate(opp_updates):
                product = update.get("product", "")
                field_updates = update.get("field_updates", {})
                rationale = update.get("rationale", "")

                update_lines = [f"\U0001f4dd *Update Opp: {product}*"]
                if rationale:
                    update_lines.append(f"  _{rationale}_")

                for field_name, new_val in field_updates.items():
                    label = field_name.replace("_", " ").title()
                    update_lines.append(f"  `{label}:` {new_val}")

                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(update_lines)},
                })

                call_id = item.get("call_id", "")
                update_payload = json.dumps({
                    "account_name": account_name,
                    "account_id": account_id,
                    "product": product,
                    "field_updates": field_updates,
                    "meeting_id": call_id,
                })

                blocks.append({
                    "type": "actions",
                    "elements": [{
                        "type": "button",
                        "text": {"type": "plain_text", "text": f"Apply {product} Update", "emoji": True},
                        "action_id": f"update_opp_sfdc_{j}_{call_id[:8]}",
                        "value": update_payload,
                    }],
                })

        # Account match verification
        if account_id:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"\u2705 Matched to SFDC: {account_link}"}],
            })

        # Participants
        participants = item.get("participants", [])
        if participants:
            blocks.append({
                "type": "context",
                "elements": [{
                    "type": "mrkdwn",
                    "text": f"Participants: {', '.join(participants[:5])}",
                }],
            })

    # Footer
    blocks.append({"type": "divider"})
    _pm = dashboard_url("post-meeting")
    _pipe = dashboard_url("pipeline")
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": f"<{_pm}|Post-Meeting To-Do> \u00b7 <{_pipe}|Pipeline> \u00b7 `/{COMMAND_PREFIX}-post-meeting` to refresh",
        }],
    })

    client.chat_postMessage(
        channel=dm_target,
        blocks=blocks,
        text=f"Gong Follow-Up: {len(results)} call(s) analyzed",
    )
    logger.info("Gong follow-up DM sent: %d calls", len(results))
