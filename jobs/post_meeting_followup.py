"""Post-Meeting Follow-Up — Gong transcript triggered via Snowflake.

Runs every 30 min. Queries Snowflake for Gong calls that have transcripts
available but haven't been analyzed yet (dedup check). When a new transcript
is found, runs the same analysis pipeline as post_meeting.py and DMs Greg with:
  - Meeting summary and buying signals
  - Follow-up email draft (auto-created in Gmail Drafts via IMAP)
  - Participant info
  - Opp creation suggestions with pre-filled fields

Faster than the batch post_meeting.py job since it checks every 30 min
for newly available transcripts (Gong typically processes 10-15 min after call).
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict

import pandas as pd

from core.snowflake_client import run_query
from core.claude_client import call_claude_json
from core.slack_formatter import (
    sf_account_url, sf_opp_url, format_currency, dashboard_url,
    build_sf_new_opp_url, EXPANSION_PRODUCT_MAP,
)
from queries.queries import (
    GONG_MEETINGS_FULL_TRANSCRIPT_QUERY,
    ACCOUNT_NOTES_QUERY,
    ACCOUNT_EMAILS_FULL_QUERY,
    ACCOUNT_OPPS_QUERY,
)
from jobs.post_meeting import (
    _build_transcript_text, _build_notes_text, _build_emails_text,
)
from utils.dedup import tracker
from config import GREG_SLACK_ID, NTR_RATES, OWNER_NAME, BOOKING_LINK

logger = logging.getLogger(__name__)


def run_post_meeting_followup(client, force=False):
    """Check Snowflake for new Gong transcripts and analyze them.

    Parameters
    ----------
    client : slack_sdk.WebClient
    force : bool
        If True, process all recent transcripts regardless of dedup.
    """
    try:
        # Query recent Gong calls with transcripts (last 2 days)
        transcript_df = run_query(
            GONG_MEETINGS_FULL_TRANSCRIPT_QUERY.format(lookback_days=2)
        )

        if transcript_df.empty:
            if force:
                client.chat_postMessage(
                    channel=GREG_SLACK_ID,
                    text="No Gong transcripts found in the last 2 days.",
                )
            return

        # Group by call_id
        calls = transcript_df.groupby("call_id")
        call_metadata = {}
        transcripts_by_call = {}

        for call_id, group in calls:
            first = group.iloc[0]
            call_metadata[call_id] = {
                "call_id": call_id,
                "account_id": str(first.get("account_id", "")),
                "account_name": first.get("account_name", ""),
                "call_name": first.get("call_name", ""),
                "call_date": str(first.get("call_date", "")),
                "duration_min": int(first.get("duration_min", 0) or 0),
            }
            transcripts_by_call[call_id] = group.sort_values("paragraph_index").to_dict("records")

        # Filter to calls not yet processed
        new_calls = []
        for call_id, meta in call_metadata.items():
            dedup_key = f"gong_rt_{call_id}"
            # Also check the batch job's dedup key
            batch_key = f"meeting_{call_id}"
            if not force and (tracker.is_processed(dedup_key) or tracker.is_processed(batch_key)):
                continue
            new_calls.append((call_id, meta, dedup_key))

        if not new_calls:
            if force:
                client.chat_postMessage(
                    channel=GREG_SLACK_ID,
                    text="All recent Gong transcripts already processed. No new actions.",
                )
            return

        # Process each new call
        results = []
        for call_id, meta, dedup_key in new_calls:
            if len(results) >= 5:
                break
            result = _analyze_call(call_id, meta, transcripts_by_call[call_id])
            if result:
                results.append(result)
                tracker.mark_processed(dedup_key)
                tracker.mark_processed(f"meeting_{call_id}")

        if not results:
            if force:
                client.chat_postMessage(
                    channel=GREG_SLACK_ID,
                    text="No actionable items from recent Gong transcripts.",
                )
            return

        # Send consolidated DM
        _send_followup_dm(results, client)

    except Exception as e:
        logger.error("Post-meeting followup (Gong) failed: %s", e)
        if force:
            client.chat_postMessage(
                channel=GREG_SLACK_ID,
                text=f"Post-meeting followup check failed: {e}",
            )


def _analyze_call(call_id, meta, transcript_rows):
    """Analyze a single Gong call transcript. Returns result dict or None."""
    account_id = meta["account_id"]
    account_name = meta["account_name"]

    # Build transcript text (reuse post_meeting.py helper)
    transcript_text = _build_transcript_text(transcript_rows)

    # Fetch SFDC notes
    notes_text = "No SFDC notes on file."
    if account_id:
        try:
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
        opps_df = run_query(ACCOUNT_OPPS_QUERY)
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

    # Get external participant emails from transcript
    participants = []
    for row in transcript_rows:
        speaker = row.get("speaker_email", "")
        is_ramp = row.get("is_ramp_participant")
        if speaker and not is_ramp and speaker not in participants:
            participants.append(speaker)

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

    # Claude analysis — aligned with granola_followup.py prompt schema
    prompt = f"""You are an AI sales analyst helping {OWNER_NAME}, a Growth Account Manager at Ramp.
{OWNER_NAME} manages ~4,000 Plus segment accounts. Their comp is 75% Realized CP (expansion opps) and 25% SaaS Renewals.
He earns comp on incremental spend above baseline during a 90-day window after closing an opp.
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
{transcript_text or 'No transcript available.'}

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
    * Sign off as: {OWNER_NAME}\\nAccount Manager @ Ramp\\nBook a meeting: {BOOKING_LINK}
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
- "next_steps": list of strings — 1-3 concrete next steps for Greg (overall, not per-product)
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


def _send_followup_dm(results, client):
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

            # email_body is now HTML from Claude prompt
            html_body = email_body

            try:
                from templates.signature import SIGNATURE_HTML
            except ImportError:
                SIGNATURE_HTML = ""
            if SIGNATURE_HTML:
                html_body += f"<br>{SIGNATURE_HTML}"

            # Create Gmail draft directly via Gumstack MCP
            call_id = item.get("call_id", "")
            pending_id = f"gong_{call_id[:12]}_{email_to.split('@')[0]}"
            draft_label = "Claude Drafts/Post Meeting"

            from core.gumstack_gmail import create_draft as gumstack_create_draft, is_available as gumstack_ok
            if gumstack_ok():
                gm_result = gumstack_create_draft(
                    to=email_to, subject=email_subject, html_body=html_body,
                    cc=cc, label=draft_label,
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
                        label=draft_label,
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
                    label=draft_label,
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
                # Field preview
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

                # Build update button payload
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
            "text": f"<{_pm}|Post-Meeting To-Do> \u00b7 <{_pipe}|Pipeline> \u00b7 `/post-meeting` to refresh",
        }],
    })

    client.chat_postMessage(
        channel=GREG_SLACK_ID,
        blocks=blocks,
        text=f"Gong Follow-Up: {len(results)} call(s) analyzed",
    )
    logger.info("Gong follow-up DM sent: %d calls", len(results))
