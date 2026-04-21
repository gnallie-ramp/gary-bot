"""Stale opp re-engagement email drafter — Daily 7:00 AM PT."""

import logging
from collections import defaultdict
import pandas as pd
from core.snowflake_client import run_query
from core.claude_client import call_claude
from utils.pending_drafts import save_draft as save_pending_draft
from core.slack_formatter import sf_opp_url, simple_dm_blocks, format_currency
from config import GREG_SLACK_ID
from core.user_registry import get_user_sf_name, get_user_first_name
from queries.queries import STALE_OPPS_QUERY, ACCOUNT_NOTES_QUERY, ACCOUNT_EMAILS_FULL_QUERY, format_query
from utils.account_resolver import fetch_contact_emails, is_hash_like
from templates.signature import build_signature

logger = logging.getLogger(__name__)

MAX_EMAIL_BODY_CHARS = 1500


def _build_notes_text(notes_row):
    """Format SFDC account notes into a readable block."""
    if notes_row is None:
        return ""
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
    return "\n".join(parts)


def _build_emails_text(email_rows):
    """Format recent email comms into readable context."""
    if not email_rows:
        return ""
    lines = []
    for e in email_rows[:3]:
        direction = e.get("direction", "")
        date = e.get("email_date", "")
        subject = e.get("subject", "")
        body = str(e.get("body_text", "") or "")[:MAX_EMAIL_BODY_CHARS]
        flags = []
        if e.get("has_willing_to_meet"):
            flags.append("willing-to-meet")
        if e.get("has_not_interested"):
            flags.append("not-interested")
        if e.get("has_ooo"):
            flags.append("OOO")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        lines.append(f"--- {date} ({direction}){flag_str} ---\nSubject: {subject}\n{body}")
    return "\n\n".join(lines)


def _generate_reengage_email(opp_row, contact_name, sfdc_notes, email_comms, owner_name: str = "", owner_first_name: str = ""):
    """Generate a re-engagement email via Claude with full context."""
    product = opp_row.get("expansion_subtype", "")
    stage = opp_row.get("opportunity_stage_name", "")
    acct_name = opp_row.get("account_name", "")
    baseline = float(opp_row.get("baseline_spend", 0) or 0)
    recent = float(opp_row.get("recent_30d_spend", 0) or 0)
    act_status = opp_row.get("activation_status", "")
    days_stale = int(opp_row.get("days_since_last_touch", 0) or 0)
    last_email_dir = opp_row.get("last_email_direction", "")
    last_subj = opp_row.get("last_email_subject", "")
    created_date = opp_row.get("created_date", "")
    call_summary = str(opp_row.get("last_call_section_text", "") or "")[:1000]
    prod_req = str(opp_row.get("last_call_product_requests", "") or "")

    first_name = contact_name.split()[0] if contact_name else ""

    prompt = f"""You are helping {owner_name}, a Growth Account Manager at Ramp, re-engage a customer and get back on the calendar.

Account: {acct_name}
Product: {product}
Days since last contact: {days_stale}
Primary contact: {contact_name}

Spend context:
- Recent 30d spend: ${recent:,.0f}/month
- Status: {act_status}

SFDC Account Notes:
{sfdc_notes if sfdc_notes else 'No notes on file'}

Recent Email History:
{email_comms if email_comms else 'No recent emails'}

Last call summary:
{call_summary if call_summary else 'No recent calls'}

Product requests from last call:
{prod_req if prod_req else 'None'}

Write a re-engagement email from {owner_first_name or owner_name} to {first_name if first_name else 'the contact'}. Structure the body in 4 explicit sections matching Ramp's Revenue Agent format. Use <strong> for section headers and <ul><li> for bullets — HTML only, no markdown.

<strong>Takeaways</strong>
<ul>
  <li>3-5 bullets pairing a specific thing they said or a real signal from SFDC (dollar amount, vendor, timeline, product request) with the Ramp capability that addresses it. Cite exact details from the call summary or email history — never generic.</li>
</ul>

<strong>{first_name or 'Their'}'s Next Steps</strong>
<ul>
  <li>2-3 bullets, 2nd person, what the CUSTOMER owns. Pick up any unfinished next step from AM notes / last call.</li>
</ul>

<strong>{owner_first_name or owner_name}'s Next Steps</strong>
<ul>
  <li>1-2 bullets, 1st person, what YOU own. Last bullet MUST be a specific deeper-dive ask with a 2-day time window. "I'll schedule a 15-min working session next Tuesday or Wednesday — what times work?"</li>
</ul>

After the 3 sections add one line before the signature:
  <p>Book a call: <a href="{booking_link if 'booking_link' in dir() else ''}">{booking_link if 'booking_link' in dir() else ''}</a></p>

HARD RULES:
- Address by first name ({first_name if first_name else 'generic greeting'})
- Acknowledge the time gap naturally (don't apologize, just own it)
- Every Takeaway fact must be traceable to the SFDC notes, call summary, or email history provided. NEVER invent.
- Do NOT guilt-trip about unanswered emails
- Do NOT say "I was notified" / anything implying an automated alert
- Do NOT use markdown. HTML tags only: <strong>, <ul>, <li>, <a>, <br>, <p>
- 200-350 words total
- Sign off as "{owner_first_name or owner_name}" on the last line before the Book a call line.
"""

    return call_claude(prompt, max_tokens=512)


def run_stale_opp_drafter(client, user_id=None):
    """Find stale opps, generate re-engagement emails, create drafts, DM Greg summary."""
    dm_target = user_id or GREG_SLACK_ID
    owner_name = get_user_sf_name(user_id)
    owner_first_name = get_user_first_name(user_id)

    try:
        df = run_query(format_query(STALE_OPPS_QUERY, user_id=user_id))
        if df.empty:
            logger.info("Stale opp drafter: no stale opps found")
            return

        account_ids = list(df["account_id"].dropna().unique())

        # Batch fetch SFDC notes
        notes_by_account = {}
        if account_ids:
            try:
                ids_str = ", ".join(f"'{a}'" for a in account_ids)
                notes_df = run_query(ACCOUNT_NOTES_QUERY.format(account_ids=ids_str))
                for _, row in notes_df.iterrows():
                    notes_by_account[row["account_id"]] = row.to_dict()
            except Exception as e:
                logger.warning("SFDC notes query failed: %s", e)

        # Batch fetch recent emails
        emails_by_account = defaultdict(list)
        if account_ids:
            try:
                ids_str = ", ".join(f"'{a}'" for a in account_ids)
                emails_df = run_query(ACCOUNT_EMAILS_FULL_QUERY.format(account_ids=ids_str))
                for _, row in emails_df.iterrows():
                    emails_by_account[row["account_id"]].append(row.to_dict())
            except Exception as e:
                logger.warning("Email comms query failed: %s", e)

        # Get contacts for all accounts
        contacts_by_account = fetch_contact_emails(None, account_ids) if account_ids else {}

        drafted = []
        errors = []

        for _, row in df.iterrows():
            acct_name = row.get("account_name", "")
            account_id = row.get("account_id", "")
            product = row.get("expansion_subtype", "")

            # Find best contact + CC stakeholders using shared scoring
            from utils.contact_scoring import select_recipients
            acct_contacts = contacts_by_account.get(account_id, [])

            # Build engagement signals for scoring
            email_correspondents = set()
            for e in emails_by_account.get(account_id, []):
                ext = (str(e.get("external_contact_email", "") or "")).strip().lower()
                if ext and "@" in ext:
                    email_correspondents.add(ext)

            primary, cc_contacts = select_recipients(
                acct_contacts,
                gong_participants=None,  # Gong data not available in batch context
                email_correspondents=email_correspondents,
                max_cc=3,
            )

            if not primary:
                logger.info("Skipping %s — no contact email found", acct_name)
                continue

            contact_email = primary["email"]
            contact_name = primary.get("name", "")
            cc_string = ", ".join(c["email"] for c in cc_contacts)

            # Build context from SFDC notes and emails
            sfdc_notes = _build_notes_text(notes_by_account.get(account_id))
            email_comms = _build_emails_text(emails_by_account.get(account_id, []))

            try:
                # Generate email body with full context
                body_text = _generate_reengage_email(row, contact_name, sfdc_notes, email_comms, owner_name=owner_name, owner_first_name=owner_first_name)

                # Build HTML body
                sig_html = build_signature(user_id=dm_target)
                html_body = f"""<div style="font-family:Arial,sans-serif;font-size:14px;color:#000;max-width:600px;">
<!-- claude-auto-draft -->
{body_text.replace(chr(10), '<br>')}
<br>
{sig_html}
</div>"""

                # Generate subject via Claude
                subject_prompt = f"""Write a short, natural email subject line (max 8 words) for a re-engagement email to {acct_name} about {product.lower().replace(' expansion', '')} expansion. Make it specific to their situation, not generic. Return ONLY the subject line, no quotes."""
                subject = call_claude(subject_prompt, max_tokens=50).strip().strip('"').strip("'")

                # Create Gmail draft via Gumstack MCP with fallback
                from core.gumstack_gmail import create_draft as gumstack_create, is_available as gumstack_ok
                draft_label = "Claude Drafts/Prospecting"
                pending_id = f"stale_opp_{row.get('opportunity_id', '')[:12]}_{contact_email.split('@')[0]}"

                if gumstack_ok():
                    gm_result = gumstack_create(
                        to=contact_email, subject=subject, html_body=html_body,
                        cc=cc_string, label=draft_label, user_id=dm_target,
                    )
                    if not gm_result["success"]:
                        save_pending_draft(
                            draft_id=pending_id, to=contact_email, cc=cc_string,
                            subject=subject, html_body=html_body,
                            account_name=acct_name, label=draft_label,
                            user_id=dm_target,
                        )
                else:
                    save_pending_draft(
                        draft_id=pending_id, to=contact_email, cc=cc_string,
                        subject=subject, html_body=html_body,
                        account_name=acct_name, label=draft_label,
                        user_id=dm_target,
                    )

                drafted.append({
                    "account": acct_name,
                    "contact": contact_name,
                    "email": contact_email,
                    "product": product,
                    "subject": subject,
                    "opp_id": row.get("opportunity_id", ""),
                })

            except Exception as e:
                logger.warning("Failed to draft for %s: %s", acct_name, e)
                errors.append(acct_name)

        if not drafted and not errors:
            logger.info("Stale opp drafter: no drafts created")
            return

        # DM Greg summary
        lines = [f"*Stale Opp Re-Engagement — {len(drafted)} drafts created*\n"]
        for d in drafted:
            sf_link = sf_opp_url(d["opp_id"])
            lines.append(
                f":envelope: <{sf_link}|{d['account']}> ({d['product']})\n"
                f"  To: {d['contact']} ({d['email']})\n"
                f"  Subject: {d['subject']}"
            )

        if errors:
            lines.append(f"\n:warning: Failed: {', '.join(errors)}")

        body = "\n".join(lines)
        blocks = simple_dm_blocks("Stale Opp Drafts", body)
        client.chat_postMessage(channel=dm_target, blocks=blocks, text="Stale Opp Re-Engagement Drafts")
        logger.info("Stale opp drafter: %d drafts created, %d errors", len(drafted), len(errors))

    except Exception as e:
        logger.error("Stale opp drafter failed: %s", e)
