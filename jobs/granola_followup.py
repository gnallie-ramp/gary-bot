"""Granola Post-Meeting Follow-Up — real-time (every 3 min).

Polls Granola's local cache for meetings that just ended, analyzes the
transcript, creates a Gmail draft, and sends a Glass-style Slack DM with
per-product opp suggestions and inline Create Opp buttons.

This is the fast path — Granola has notes immediately after the call ends
(vs Gong → Snowflake which can take hours). Falls back to the Gong pipeline
for calls where Granola wasn't running.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import pytz

from core.granola_client import (
    get_recent_meetings,
    get_transcript,
    get_metadata,
    get_summary,
    extract_attendee_info,
)
from core.claude_client import call_claude_json
from core.slack_formatter import (
    sf_account_url,
    format_currency,
    dashboard_url,
    SF_CUSTOM_FIELDS,
    EXPANSION_TYPE_MAP,
    EXPANSION_PRODUCT_MAP,
)
from templates.help_links import find_relevant_links, format_links_for_email
from utils.dedup import tracker
from utils.pending_drafts import save_draft as save_pending_draft
from utils.account_matcher import match_account
from config import GREG_SLACK_ID, NTR_RATES, DISPLAY_TIMEZONE, COMMAND_PREFIX
from core.user_registry import get_user_sf_name, get_user_booking_link

logger = logging.getLogger(__name__)

# Expansion record type ID in Salesforce
_EXPANSION_RECORD_TYPE_ID = "0125b000000PZaIAAW"


def run_granola_followup(client, user_id=None, force=False, lookback_minutes=None):
    """Check Granola for recently ended meetings and analyze them.

    Parameters
    ----------
    client : slack_sdk.WebClient
    force : bool
        If True, process meetings regardless of dedup.
    lookback_minutes : int, optional
        Custom lookback in minutes. Overrides default (10 min normal, 30 days force).
        Useful for catch-up after bot was offline.
    """
    dm_target = user_id or GREG_SLACK_ID
    owner_name = get_user_sf_name(user_id)
    booking_link = get_user_booking_link(user_id)

    try:
        if lookback_minutes is not None:
            lookback = lookback_minutes
        else:
            # Normal: last 30 min (covers full meeting + end detection lag)
            # Force (/post-meeting): 30 days
            lookback = 60 * 24 * 30 if force else 30
        recent = get_recent_meetings(minutes=lookback)

        if not recent:
            if force:
                client.chat_postMessage(
                    channel=dm_target,
                    text="No recent Granola meetings found.",
                )
            return

        # Filter: must have external participants, duration > 3 min
        actionable = []
        for meeting in recent:
            meeting_id = meeting["id"]
            dedup_key = f"granola_{meeting_id}"

            if not force and tracker.is_processed(dedup_key, user_id=dm_target):
                continue

            people = meeting.get("people", [])
            names, emails = extract_attendee_info(people)

            # Skip if no external attendees (internal meeting)
            if not emails and not names:
                continue

            actionable.append((meeting, dedup_key, names, emails))

        if not actionable:
            if force:
                client.chat_postMessage(
                    channel=dm_target,
                    text="All recent Granola meetings already processed or internal-only.",
                )
            return

        # Process each meeting
        results = []
        for meeting, dedup_key, ext_names, ext_emails in actionable[:5]:
            result = _analyze_granola_meeting(meeting, ext_names, ext_emails, owner_name=owner_name, booking_link=booking_link, user_id=dm_target)
            if result:
                results.append(result)
                tracker.mark_processed(dedup_key, user_id=dm_target)

        if not results:
            if force:
                client.chat_postMessage(
                    channel=dm_target,
                    text="No actionable items from recent Granola meetings.",
                )
            return

        _send_glass_style_dm(results, client, dm_target=dm_target)
        # Auto-propose now lives INSIDE _send_glass_style_dm so it fires for
        # every caller (slash-command re-runs, single-account targeted runs, etc.)

    except Exception as e:
        logger.error("Granola followup failed: %s", e)
        if force:
            client.chat_postMessage(
                channel=dm_target,
                text=f"Granola followup failed: {e}",
            )


def _analyze_granola_meeting(meeting: dict, ext_names: list, ext_emails: list, owner_name: str = "", booking_link: str = "", user_id: str = None) -> dict | None:
    """Analyze a single Granola meeting. Returns result dict or None."""
    meeting_id = meeting["id"]
    title = meeting.get("title", "Untitled")

    # Get transcript
    transcript = get_transcript(meeting_id)
    if not transcript:
        logger.info("No transcript for Granola meeting %s (%s), skipping", meeting_id, title)
        return None

    # Get summary
    summary_text = get_summary(meeting_id) or ""

    # Get meeting date in ET
    created_str = meeting.get("created_at", "")
    et = pytz.timezone(DISPLAY_TIMEZONE)
    try:
        dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        meeting_date = dt.astimezone(et).strftime("%a %b %d, %I:%M %p ET")
        meeting_date_short = dt.astimezone(et).strftime("%m/%d")
    except (ValueError, TypeError):
        meeting_date = created_str[:10] if created_str else "Unknown"
        meeting_date_short = meeting_date

    # Extract account name from meeting title (common patterns: "Ramp // Acme", "Acme <> Ramp (Ramp Treasury)")
    import re
    _title_clean = title
    # Strip trailing parenthetical product labels like "(Ramp Treasury)" first
    _title_clean = re.sub(r'\s*\(Ramp\s+\w+\)\s*$', '', _title_clean).strip()
    # Split on common separators and pick the non-Ramp part
    for sep in [" // ", " <> ", " - ", " | ", " / "]:
        parts = _title_clean.split(sep)
        non_ramp = [p.strip() for p in parts if not re.match(r'^ramp\b', p.strip(), re.IGNORECASE)]
        if non_ramp and len(parts) > 1:
            _title_clean = non_ramp[0]
            break

    # Try to match to a Salesforce account
    account_match = match_account(
        account_name=_title_clean,
        participant_emails=ext_emails,
        user_id=user_id,
    )
    account_name = account_match.account_name if account_match.matched else ""
    account_id = account_match.account_id if account_match.matched else ""
    account_owner = account_match.owner_name if account_match.matched else ""
    is_gregs_book = account_match.is_gregs_book if account_match.matched else False

    if not account_name:
        # Use cleaned title as fallback account name
        account_name = _title_clean

    # Fetch open opps context if we have an account
    opp_context = "No account matched — cannot check existing opps."
    if account_id:
        try:
            from core.snowflake_client import run_query
            from queries.queries import ACCOUNT_OPPS_QUERY, format_query
            import pandas as pd
            opps_df = run_query(format_query(ACCOUNT_OPPS_QUERY, user_id=user_id))
            if not opps_df.empty:
                acct_opps = opps_df[opps_df["account_id"] == account_id]
                if not acct_opps.empty:
                    lines = []
                    for _, r in acct_opps.iterrows():
                        lines.append(
                            f"- {r['expansion_subtype']} ({r['opportunity_stage_name']}) — "
                            f"{format_currency(float(r.get('monthly_expansion_amount', 0) or 0))}/mo"
                        )
                    opp_context = "\n".join(lines)
                else:
                    opp_context = "No open expansion opps for this account."
        except Exception as e:
            logger.debug("Could not fetch opps for %s: %s", account_id, e)
            opp_context = "Could not fetch existing opps."

    # Find relevant help articles based on transcript content
    relevant_links = find_relevant_links(transcript[:4000] + " " + (summary_text or ""), max_links=4)
    links_context = ""
    if relevant_links:
        links_context = "AVAILABLE RAMP RESOURCE LINKS (use these in the email where relevant):\n"
        for link in relevant_links:
            links_context += f'- {link["title"]}: {link["url"]}\n'

    # Claude analysis — per-product opp suggestions
    prompt = f"""You are an AI sales analyst helping {owner_name}, a Growth Account Manager at Ramp.
{owner_name} manages ~4,000 Plus segment accounts. Their comp is 75% Realized CP (expansion opps) and 25% SaaS Renewals.
They earn comp on incremental spend above baseline during a 90-day window after closing an opp.
Closing too late = baseline rises = comp reduced. Speed matters.

NTR rates for CP calculation:
- Card: 95 bps (0.0095)
- Bill Pay: 15 bps (0.0015)
- Treasury: 5 bps (0.0005)
- Travel: 350 bps (0.035)

A call just ended. Analyze the transcript and provide actionable follow-up.

MEETING INFO:
- Title: {title}
- Date: {meeting_date}
- Account: {account_name}
- External Attendees: {', '.join(ext_names) if ext_names else 'Unknown'}
- External Emails: {', '.join(ext_emails) if ext_emails else 'Unknown'}

EXISTING OPEN OPPS:
{opp_context}

GRANOLA AI SUMMARY:
{summary_text[:2000] if summary_text else 'No AI summary available.'}

{links_context}

TRANSCRIPT:
{transcript[:20000]}

Return a JSON object with these exact keys:
- "meeting_summary": string — 2-3 sentence summary
- "highlights": list of strings — 3-5 key bullet points from the call (specific, actionable)
- "buying_signals": string — specific quotes or evidence. "None detected" if nothing found.
- "follow_up_email_to": string — best email to send follow-up to (from attendee emails, or empty)
- "follow_up_email_subject": string — format: "Ramp Follow-Up - <brief summary of key topics discussed>"
- "follow_up_email_body": string — HTML-formatted post-meeting email in 4
    explicit sections, in this exact order. Use <strong> for section headers
    and <ul><li> for bullets:

    <strong>Takeaways</strong>
    <ul>
      <li>3-5 bullets pairing a specific thing the customer said on the call
          (dollar amount, vendor, timeline, named person) with the Ramp
          capability that addresses it. Never generic. Cite exact numbers
          and names from the transcript.</li>
    </ul>

    <strong>[Customer First Name]'s Next Steps</strong>
    <ul>
      <li>2-4 bullets, 2nd person, describing what the CUSTOMER owns.
          "Stella: ask Roger for admin access." Route items to named
          stakeholders when clear. Replace [Customer First Name] in the header
          with the actual first name of the primary recipient.</li>
    </ul>

    <strong>[Your First Name]'s Next Steps</strong>
    <ul>
      <li>1-3 bullets, 1st person, describing what YOU own. Replace
          [Your First Name] in the header with the Ramp AM's first name.
          End with a specific deeper-dive ask with a 2-day time window, e.g.
          "I'll schedule a 30-min working session next Tuesday or
          Wednesday to walk through Treasury setup — what times work?"</li>
    </ul>

    <strong>Resources</strong>
    <ul>
      <li>2-4 Ramp help-article links from the AVAILABLE RAMP RESOURCE
          LINKS above. Format: <a href="URL">Title</a>. Only include if
          resources are provided; otherwise OMIT this entire section.</li>
    </ul>

    After the 4 sections, add one line before the signature:
      <p>Book a call: <a href="{booking_link}">{booking_link}</a></p>

    TONE: warm, direct, contractions natural. No "I hope this email finds
    you well", no "just circling back", no "touching base", no generic
    nurture talk. A real AM driving the deal.

    HARD RULES — violations are failures:
    * Do NOT say "I was notified" / anything implying an automated alert
    * Do NOT guilt-trip about unanswered emails
    * Do NOT invent facts. Every Takeaway must be traceable to the transcript.
    * Do NOT use markdown. HTML tags only: <strong>, <ul>, <li>, <a>, <br>, <p>
    * 250-400 words total (longer than the old 100-150 to fit the 4 sections).
- "follow_up_email_cc": string — other emails to CC, comma-separated, or empty
- "opps": list of objects, each with:
    - "product": string (one of "Card Expansion", "Bill Pay Expansion", "Travel Expansion", "Treasury Expansion", "Procurement")
    - "stage": string (always "S2: Sales Qualified Opportunity")
    - "monthly_amount": integer — the EXACT monthly dollar amount discussed on the call for this product. Use the customer's own words/numbers from the transcript, not estimates. For bill pay / AP, use the monthly AP volume they stated. For treasury, use the balance or deposit amount discussed. If no specific amount was stated but the product was discussed with mild interest or was a major talking point, use 15000 as the default amount.
    - "rationale": string (1 sentence — why this opp, citing transcript evidence)
    - "close_date": string (YYYY-MM-DD). Use the specific date if a timeline was explicitly discussed on the call. Otherwise leave empty and the system will default to end of current month.
    - "next_step": string — the specific next action for this product based on what was discussed (e.g. "Schedule Treasury demo with Brooks", "Send Bill Pay migration CSV template"). Be concrete and reference the transcript.
    - "next_step_due_date": string (YYYY-MM-DD). Use the specific date if a timeline was explicitly discussed. Otherwise leave empty and the system will default to 1 week from today.
  Include products that were discussed with genuine interest OR were a major talking point on the call, even if no specific dollar amount was mentioned. Use 15000 as the default monthly_amount when no amount was stated. Empty list only if no products were discussed at all.
- "next_steps": list of strings — 1-3 concrete next steps for {owner_name} (overall, not per-product)
- "urgency": string — "high", "medium", or "low"
- "opp_updates": list of objects for EXISTING open opps that should be updated based on the call. Each with:
    - "product": string — which existing opp product to update (must match one from EXISTING OPEN OPPS above)
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
        logger.warning("Claude analysis failed for Granola meeting %s: %s", meeting_id, e)
        result = {
            "meeting_summary": "Claude analysis unavailable.",
            "highlights": [],
            "buying_signals": "Analysis failed",
            "follow_up_email_to": ext_emails[0] if ext_emails else "",
            "follow_up_email_subject": f"Follow-up: {title}",
            "follow_up_email_body": "",
            "follow_up_email_cc": "",
            "opps": [],
            "next_steps": ["Review call recording manually."],
            "urgency": "medium",
        }

    return {
        "meeting_id": meeting_id,
        "title": title,
        "meeting_date": meeting_date,
        "meeting_date_short": meeting_date_short,
        "account_name": account_name,
        "account_id": account_id,
        "account_owner": account_owner,
        "is_gregs_book": is_gregs_book,
        "ext_names": ext_names,
        "ext_emails": ext_emails,
        "relevant_links": relevant_links,
        **result,
    }


def _send_glass_style_dm(results: list[dict], client, dm_target=None):
    """Send a Glass-style consolidated DM with per-product opp buttons."""
    # Fallback to the primary owner if caller didn't pass dm_target — some
    # callers (e.g. /gary-post-meeting {account}) invoke this without a
    # target. Matches the original behavior of the granola_followup job.
    dm_target = dm_target or GREG_SLACK_ID
    blocks = [{
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"\U0001f3a4 Post-Meeting Follow-Up — {len(results)} call(s)",
            "emoji": True,
        },
    }]

    for item in results:
        account_name = item.get("account_name", "Unknown")
        account_id = item.get("account_id", "")
        title = item.get("title", "")
        meeting_date = item.get("meeting_date", "")
        urgency = item.get("urgency", "medium")
        summary = item.get("meeting_summary", "")
        signals = item.get("buying_signals", "")
        highlights = item.get("highlights", [])
        next_steps = item.get("next_steps", [])
        opps = item.get("opps", [])

        urgency_emoji = {
            "high": "\U0001f534", "medium": "\U0001f7e1", "low": "\U0001f7e2"
        }.get(urgency, "\U0001f7e1")

        # Account link
        if account_id:
            sf_link = sf_account_url(account_id)
            account_link = f"<{sf_link}|{account_name}>"
        else:
            account_link = f"*{account_name}*"

        blocks.append({"type": "divider"})

        # Header line
        header_line = f"{urgency_emoji} {account_link}  |  {meeting_date}  |  _{title}_"
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

        # Email draft — create directly in Gmail + label
        # Recipient resolution now uses the unified resolver (SFDC contacts +
        # Gong participants + email correspondents + current meeting attendees)
        # instead of Claude picking from the attendee list. Enforces Greg's
        # "one email with full CC, not fragmented sends" rule.
        email_subject = item.get("follow_up_email_subject", "")
        email_body = item.get("follow_up_email_body", "")
        try:
            from utils.recipient_resolver import resolve_outbound_recipients
            acct_id_for_resolver = item.get("account_id") or ""
            current_attendees = item.get("ext_emails") or []
            primary_contact, cc_contacts_resolved, _dbg = resolve_outbound_recipients(
                account_id=acct_id_for_resolver,
                user_id=dm_target,
                current_meeting_attendees=current_attendees,
                max_cc=4,
            )
            if primary_contact:
                email_to = primary_contact.get("email", "")
                cc = ", ".join(c.get("email", "") for c in cc_contacts_resolved if c.get("email"))
            else:
                # Fallback: Claude's pick + Claude's CC list
                email_to = item.get("follow_up_email_to", "")
                cc = item.get("follow_up_email_cc", "") or ""
        except Exception as e:
            logger.debug("Resolver failed for post-meeting email — falling back: %s", e)
            email_to = item.get("follow_up_email_to", "")
            cc = item.get("follow_up_email_cc", "") or ""

        if email_to and email_body:

            # email_body is already HTML from Claude prompt
            html_body = email_body

            # If the body doesn't already contain hyperlinked resources,
            # append a "Helpful resources" section from help_links
            item_links = item.get("relevant_links", [])
            if "helpful resource" not in html_body.lower() and item_links:
                links_html = format_links_for_email(item_links)
                if links_html:
                    html_body += f"<br>{links_html}"

            try:
                from templates.signature import build_signature
                sig_html = build_signature(user_id=dm_target)
            except ImportError:
                sig_html = ""
            if sig_html:
                html_body += f"<br>{sig_html}"

            # Create Gmail draft directly via Gumstack MCP (no Glass needed)
            meeting_id = item.get("meeting_id", "")
            pending_id = f"granola_{meeting_id[:12]}_{email_to.split('@')[0]}"
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
                    # Gumstack failed — fall back to pending drafts for Glass cron
                    save_pending_draft(
                        draft_id=pending_id, to=email_to, cc=cc,
                        subject=email_subject, html_body=html_body,
                        account_name=account_name, meeting_id=meeting_id,
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
                # No Gumstack tokens — save to pending for Glass cron
                save_pending_draft(
                    draft_id=pending_id, to=email_to, cc=cc,
                    subject=email_subject, html_body=html_body,
                    account_name=account_name, meeting_id=meeting_id,
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

        # Per-product opp suggestions with inline Create buttons
        if opps:
            for i, opp in enumerate(opps):
                product = opp.get("product", "")
                stage = opp.get("stage", "S2: Sales Qualified Opportunity")
                amount = opp.get("monthly_amount", 0)
                rationale = opp.get("rationale", "")
                close_date = opp.get("close_date", "")

                if not close_date:
                    # Default: last day of current month
                    now = datetime.utcnow()
                    close_date = ((now.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")

                next_step = opp.get("next_step", "")
                next_step_due = opp.get("next_step_due_date", "")
                if not next_step_due:
                    next_step_due = (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%d")

                ntr_rate = NTR_RATES.get(product, 0)
                est_cp = float(amount) * ntr_rate * 3 if amount and ntr_rate else 0

                subtype = EXPANSION_PRODUCT_MAP.get(product, product)
                opp_name = f"{account_name} - {subtype}"

                opp_lines = [f"\U0001f4b0 *{product}* | {format_currency(float(amount))}/mo | Est. CP: {format_currency(est_cp)}"]
                if rationale:
                    opp_lines.append(f"  _{rationale}_")
                # Show field preview
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

                # Build the payload for the Create Opp button
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
                    "meeting_id": item.get("meeting_id", ""),
                })

                blocks.append({
                    "type": "actions",
                    "elements": [{
                        "type": "button",
                        "text": {"type": "plain_text", "text": f"Create {EXPANSION_PRODUCT_MAP.get(product, product)} Opp", "emoji": True},
                        "action_id": f"create_opp_sfdc_{i}_{item.get('meeting_id', '')[:8]}",
                        "value": opp_payload,
                        "style": "primary",
                    }],
                })

        # Legacy opp_updates rendering disabled — superseded by the grounded
        # pipeline_update_proposer that fires at the end of this DM. That
        # proposer has a real SFDC stage picklist, per-field review UX, and
        # source-data validation. Kept here as a no-op block for history.
        # To re-enable: restore the loop that iterates item.get("opp_updates").

        # Account match verification + owner info
        account_owner = item.get("account_owner", "")
        is_gregs_book = item.get("is_gregs_book", False)
        if account_id:
            owner_note = ""
            if account_owner and not is_gregs_book:
                owner_note = f"  |  :bust_in_silhouette: Owned by {account_owner}"
            elif account_owner:
                owner_note = f"  |  :bust_in_silhouette: {account_owner}"
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"\u2705 Matched to SFDC: {account_link}{owner_note}"}],
            })
        else:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": "\u26a0\ufe0f No SFDC account match — opp creation requires manual account lookup"}],
            })

        # Participants
        ext_names = item.get("ext_names", [])
        ext_emails = item.get("ext_emails", [])
        if ext_names or ext_emails:
            parts = ext_names[:3] + [e for e in ext_emails[:3] if e not in ext_names]
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"Attendees: {', '.join(parts)}"}],
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
        text=f"Post-Meeting Follow-Up: {len(results)} call(s) analyzed",
    )
    logger.info("Granola follow-up DM sent: %d meetings", len(results))

    # Auto-propose SFDC updates for any matched account. Grounded proposer
    # (same as Pipeline tab's Propose Updates button). Silently skips accounts
    # with no proposed changes. Runs AFTER the main DM so it feels like a
    # follow-up, not a competing message.
    try:
        from jobs.pipeline_update_proposer import dm_account_update_review
        from handlers.home_tab import _pending_sfdc_updates
        for item in results:
            acct_id = item.get("account_id")
            if not acct_id:
                continue
            dm_account_update_review(
                client, dm_target, acct_id,
                pending_store=_pending_sfdc_updates,
                source_label="Post-meeting",
            )
    except Exception as e:
        logger.warning("Auto-propose after Granola DM failed: %s", e)
