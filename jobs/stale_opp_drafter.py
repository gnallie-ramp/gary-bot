"""Stale opp re-engagement email drafter — Daily 7:00 AM PT."""

import logging
from collections import defaultdict
import pandas as pd
from core.snowflake_client import run_query
from core.claude_client import call_claude
from utils.pending_drafts import save_draft as save_pending_draft
from core.slack_formatter import sf_opp_url, simple_dm_blocks, format_currency
from config import OWNER_NAME
from queries.queries import STALE_OPPS_QUERY, ACCOUNT_NOTES_QUERY, ACCOUNT_EMAILS_FULL_QUERY
from utils.account_resolver import fetch_contact_emails, is_hash_like
from templates.signature import SIGNATURE_HTML
from config import GREG_SLACK_ID

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


def _generate_reengage_email(opp_row, contact_name, sfdc_notes, email_comms):
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

    prompt = f"""You are helping {OWNER_NAME}, a Growth Account Manager at Ramp, re-engage a stalled expansion opportunity.

Account: {acct_name}
Product: {product}
Stage: {stage}
Days since last contact: {days_stale}
Primary contact: {contact_name}
Last contact type: {last_email_dir if last_email_dir else 'call'}
Last email subject: {last_subj if last_subj else 'N/A'}

Opp context:
- Created: {created_date}

Spend context:
- Baseline spend: ${baseline:,.0f}/month
- Recent 30d spend: ${recent:,.0f}/month
- Status: {act_status}

SFDC Account Notes:
{sfdc_notes if sfdc_notes else 'No notes on file'}

Recent Email History (Outreach/SFDC-logged — shows what Greg has already sent):
{email_comms if email_comms else 'No recent emails'}

Last call summary:
{call_summary if call_summary else 'No recent calls'}

Product requests from last call:
{prod_req if prod_req else 'None'}

Write a short re-engagement email from Greg to the customer. Rules:
- Address the contact by first name ({first_name if first_name else 'use generic greeting'})
- Reference the most recent call or email context to show continuity — USE THE ACTUAL EMAIL/CALL CONTENT ABOVE, don't be generic
- If the customer replied with interest or a specific question in recent emails, pick up that thread
- If AM notes mention specific next steps or context, weave that in naturally
- Under 120 words
- Acknowledge the time gap naturally
- One clear, low-friction ask (15-minute call, quick question, etc.)
- Tone: warm, direct, peer-to-peer — not salesy
- DO NOT use bullet points
- DO NOT use subject line — return body only
- Sign off as "Greg" """

    return call_claude(prompt, max_tokens=512)


def run_stale_opp_drafter(client):
    """Find stale opps, generate re-engagement emails, create drafts, DM Greg summary."""
    try:
        df = run_query(STALE_OPPS_QUERY)
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

            # Find best contact
            acct_contacts = contacts_by_account.get(account_id, [])
            contact_email = ""
            contact_name = ""

            for c in acct_contacts:
                if c.get("email") and not is_hash_like(c.get("name", "")):
                    contact_email = c["email"]
                    contact_name = c.get("name", "")
                    break

            if not contact_email:
                logger.info("Skipping %s — no contact email found", acct_name)
                continue

            # Build context from SFDC notes and emails
            sfdc_notes = _build_notes_text(notes_by_account.get(account_id))
            email_comms = _build_emails_text(emails_by_account.get(account_id, []))

            try:
                # Generate email body with full context
                body_text = _generate_reengage_email(row, contact_name, sfdc_notes, email_comms)

                # Build HTML body
                html_body = f"""<div style="font-family:Arial,sans-serif;font-size:14px;color:#000;max-width:600px;">
<!-- claude-auto-draft -->
{body_text.replace(chr(10), '<br>')}
<br>
{SIGNATURE_HTML}
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
                        label=draft_label,
                    )
                    if not gm_result["success"]:
                        save_pending_draft(
                            draft_id=pending_id, to=contact_email, cc="",
                            subject=subject, html_body=html_body,
                            account_name=acct_name, label=draft_label,
                        )
                else:
                    save_pending_draft(
                        draft_id=pending_id, to=contact_email, cc="",
                        subject=subject, html_body=html_body,
                        account_name=acct_name, label=draft_label,
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
        client.chat_postMessage(channel=GREG_SLACK_ID, blocks=blocks, text="Stale Opp Re-Engagement Drafts")
        logger.info("Stale opp drafter: %d drafts created, %d errors", len(drafted), len(errors))

    except Exception as e:
        logger.error("Stale opp drafter failed: %s", e)
