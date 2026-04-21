"""Pipeline Re-engagement Drafter — unified multi-opp drafter for the Pipeline tab.

Called from the Pipeline tab's Draft Re-engage button. Generates ONE Gmail
draft that touches all of an account's open opportunities (Expansion +
Renewal), grounded in real recent Gong + email history.

Key differences vs. `stale_opp_drafter.py`:
  - Scope is the account, not a single opp
  - Email references every open opp's product/amount/next-step explicitly
  - Historical context includes multiple Gong calls (summaries + last
    full transcript) and last 5 email threads
  - Grounded prompt — no invented dates, quotes, or customer statements
"""
from __future__ import annotations

import json
import logging

from core.claude_client import call_claude
from core.gumstack_gmail import create_draft as gumstack_create, is_available as gumstack_ok
from core.snowflake_client import run_query
from core.user_registry import get_user_sf_name, get_user_first_name, get_user_booking_link
from queries.queries import ACCOUNT_EMAIL_HISTORY_QUERY, format_query
from templates.signature import build_signature
from utils.account_resolver import fetch_contact_emails
from utils.contact_scoring import select_recipients
from utils.pending_drafts import save_draft as save_pending_draft

logger = logging.getLogger(__name__)

DRAFT_LABEL = "Claude Drafts/Pipeline Re-engagement"
EMAIL_HISTORY_LIMIT = 5
CALL_TRANSCRIPT_CAP = 3500

_SYSTEM_PROMPT = """You write sales re-engagement emails for a Ramp Growth
Account Manager. GOAL: get the customer back on a call to push the deal
forward, by pairing their stated pains with the Ramp features that solve them.

STRUCTURE — produce the email body in exactly these 4 sections, each with
a bolded header on its own line (use plain-text style asterisks around the
section header so Gmail renders them as bold). No intro paragraph before
the first section. No closing paragraph after the last section. No bullets
outside the marked bulleted sections.

  *Takeaways*
  3-5 bullets starting with "•". Each bullet MUST pair a specific pain or
  signal the customer stated on the call / in email with the concrete Ramp
  capability that addresses it. Format:
    • {specific pain or stated detail} — {Ramp feature + specific benefit}
  Example real bullets (only illustrative — draw from actual source data):
    • Manual re-keying between Bill.com and NetSuite — Ramp's native
      two-way NetSuite sync eliminates the dual-entry, cutting month-end
      close by days.
    • $500K idle cash at Chase earning 0% — Ramp Treasury's 4.5% yield
      would turn that into ~$22K/yr in recovered revenue.
  Cite specific dollar amounts, vendor names, and numbers when the source
  data provides them. Never generic.

  *{Customer first name}'s Next Steps*
  2-4 bullets, written in 2nd person, describing what the CUSTOMER owns:
    • Stella: ask Roger for admin access in Ramp
    • Stella: pull the policy PDF and share it back
  If multiple stakeholders are named on the call, route items to each by
  name where clear.

  *{Your first name}'s Next Steps*
  1-3 bullets, written in 1st person, ending with a specific deeper-dive
  ask with a 2-day time window:
    • I'll pull the Policy Agent onboarding guide together
    • I'll schedule a 30-min working session next Tuesday or Wednesday
      to walk through Treasury setup — can you share a couple of slots
      that work?

  *Resources*
  2-4 help-article links pulled from the RESOURCES payload below. Format
  each as an HTML anchor on its own line:
    • <a href="URL">Title</a>
  If no resources are provided in the payload, omit this section entirely
  (do not fabricate URLs).

After the 4 sections, add exactly one line at the very end (before the
signature the caller will append):

    Book a call: {booking_link}

(The caller provides the actual booking link. You just use the literal
phrase "Book a call:" followed by whatever URL is in the BOOKING_LINK
field of the payload.)

TONE: warm, direct, contractions natural. No "I hope this email finds you
well", no "just circling back", no "touching base", no generic nurture
talk. A real AM pushing a deal forward.

HARD RULES — violations are failures:
- Do NOT invent customer quotes, dates, email subjects, person names,
  monetary amounts, or pain points. Only reference specifics present in
  the SOURCE DATA.
- Do NOT claim a customer said something unless it's in the provided
  call transcript or email subjects.
- Do NOT fabricate timelines. Only repeat stated customer timing.
- Do NOT invent Ramp product features. Real capabilities you may cite:
  Card (95bps cashback, granular spend controls, real-time transaction
  feed, auto-coding, ERP integration), Bill Pay (two-way NetSuite /
  QuickBooks sync, approval workflows, bulk payment batching, vendor
  portal, ACH + check + card payments, AP automation), Treasury (~4.5%
  yield on idle cash, FDIC pass-through, same-day transfers, multi-entity
  accounts), Plus (custom fields, per-entity controls, SAML SSO, SCIM,
  AI-driven automations, advanced policies, dedicated support),
  Procurement (vendor intake, contract management, purchase requests,
  renewal tracking, AI negotiation).
- Feature→benefit pairings MUST be grounded in a specific pain or signal
  the customer raised. If no pain is on file for a product, keep that
  bullet generic and brief (e.g. "Adds Treasury yield on idle cash").
- 250–400 words total.
"""


def _format_opps(opps: list[dict]) -> str:
    """Render the open opps list for the prompt."""
    lines = []
    for i, o in enumerate(opps, 1):
        bits = [f"{i}. Type={o.get('type') or '?'}"]
        if o.get("product"):
            bits.append(f"Product={o['product']}")
        bits.append(f"Stage={o.get('stage') or '?'}")
        if o.get("close_date"):
            bits.append(f"Close={o['close_date']}")
        amt = o.get("monthly_amount")
        if amt and float(amt) > 0:
            bits.append(f"Monthly=${int(float(amt))}")
        lines.append(" · ".join(bits))
        if o.get("next_step"):
            lines.append(f"   SFDC NextStep: {o['next_step'][:200]}")
        if o.get("expansion_notes"):
            lines.append(f"   SFDC ExpansionNotes: {o['expansion_notes'][:250]}")
    return "\n".join(lines)


def _fetch_email_history(account_id: str) -> list[dict]:
    try:
        q = format_query(
            ACCOUNT_EMAIL_HISTORY_QUERY,
            account_id=account_id,
            limit=EMAIL_HISTORY_LIMIT,
        )
        df = run_query(q)
        df.columns = [c.lower() for c in df.columns]
        return [
            {
                "date": str(r.get("email_date") or ""),
                "subject": str(r.get("subject") or "")[:120],
                "direction": str(r.get("direction") or "").strip(),
                "ramp_side_owner": str(r.get("ramp_side_owner") or ""),
            }
            for _, r in df.iterrows()
        ]
    except Exception as e:
        logger.debug("Email history fetch failed for %s: %s", account_id, e)
        return []


def _build_payload(account_name: str, opps: list[dict], row: dict,
                   email_history: list[dict], contact_name: str) -> str:
    import pandas as pd
    lines = [
        f"PRIMARY RECIPIENT: {contact_name or '(unknown)'}",
        f"ACCOUNT: {account_name}",
        "",
        "OPEN OPPS (cover ALL of these in one email):",
        _format_opps(opps),
        "",
    ]

    call_title = row.get("last_call_title")
    call_date = row.get("last_call_date")
    call_summary = (row.get("last_call_summary") or "").strip()
    if call_title and call_date is not None and not pd.isna(call_date) and call_summary:
        transcript = call_summary
        if len(transcript) > CALL_TRANSCRIPT_CAP:
            transcript = transcript[:CALL_TRANSCRIPT_CAP] + "…[truncated]"
        lines.append("MOST RECENT CALL (from Gong):")
        lines.append(f"  Date: {call_date}")
        lines.append(f"  Title: {call_title}")
        lines.append(f"  SectionSummary: {transcript}")
        lines.append("")
    else:
        lines.append("MOST RECENT CALL: none on file within the last year")
        lines.append("")

    if email_history:
        lines.append("RECENT EMAIL THREADS (newest first, last 180 days):")
        for t in email_history:
            lines.append(
                f"  {t['date']} {t['direction'].upper()} "
                f"subj=\"{t['subject']}\" ramp_side={t['ramp_side_owner'] or '?'}"
            )
    else:
        lines.append("RECENT EMAIL THREADS: none on file in the last 180 days")

    return "\n".join(lines)


def _generate_body_text(account_name: str, opps: list[dict], row: dict,
                        email_history: list[dict], contact_name: str,
                        owner_first_name: str, booking_link: str,
                        resources: list[dict] | None = None,
                        play_context: str = "") -> str:
    payload = _build_payload(account_name, opps, row, email_history, contact_name)

    resources_block = ""
    if resources:
        lines = ["RESOURCES (only include these URLs in the Resources section — do not invent others):"]
        for r in resources[:4]:
            url = r.get("url") or ""
            title = r.get("title") or url
            if url:
                lines.append(f"  - {title}: {url}")
        resources_block = "\n".join(lines) + "\n"
    else:
        resources_block = "RESOURCES: (none on file — OMIT the Resources section entirely)\n"

    play_block = ""
    if play_context:
        play_block = f"PLAY CONTEXT (this email is being sent from a specific outbound play — shape the pitch to match this angle):\n{play_context}\n\n"

    user_prompt = (
        f"{play_block}"
        f"SOURCE DATA:\n{payload}\n\n"
        f"{resources_block}"
        f"BOOKING_LINK: {booking_link or '(none)'}\n\n"
        f"YOUR FIRST NAME: {owner_first_name}\n"
        f"CUSTOMER FIRST NAME: {contact_name.split()[0] if contact_name else 'there'}\n\n"
        f"Write the 4-section email body now, addressed to {contact_name or 'the primary contact'}. "
        f"Follow the section structure exactly. End with the 'Book a call:' line as "
        f"instructed (the caller appends the full signature after that line)."
    )
    text = call_claude(user_prompt, max_tokens=900, system=_SYSTEM_PROMPT)
    return (text or "").strip()


def _generate_subject(account_name: str, opps: list[dict], pitched_product: str = "") -> str:
    """Subject format: 'Ramp follow-up on {primary product}'.

    If `pitched_product` is provided (e.g. from a play hook), use it. Otherwise
    picks the highest-CP product on the opps list.
    """
    if pitched_product:
        return f"Ramp follow-up on {pitched_product}"
    sorted_opps = sorted(opps, key=lambda o: float(o.get("est_cp") or 0), reverse=True)
    if not sorted_opps:
        return f"Ramp follow-up — {account_name}"
    primary_product = sorted_opps[0].get("product") or sorted_opps[0].get("type") or "next steps"
    return f"Ramp follow-up on {primary_product}"


def draft_account_reengagement(account_payload: dict, client, user_id: str = None) -> bool:
    """Generate and drop a unified re-engagement email as a Gmail draft.

    Parameters
    ----------
    account_payload : dict
        From the Pipeline tab button — must contain account_id, account_name, opps[].
    client : slack_sdk.WebClient
    user_id : str
        Slack user ID (routes confirmation DM + signature).

    Returns True on success (draft created or queued), False otherwise.
    """
    from handlers.home_tab import _fetch_pipeline_data

    account_id = account_payload.get("account_id")
    account_name = account_payload.get("account_name", "Unknown")
    payload_opps = account_payload.get("opps", []) or []
    play_id = account_payload.get("play_id") or ""
    play_row = account_payload.get("play_row") or {}

    if not account_id:
        client.chat_postMessage(
            channel=user_id,
            text=":warning: Draft failed — missing account_id in payload.",
        )
        return False

    # Pull the full account row from the Pipeline cache so we have the call
    # summary, engagement dates, and full opp details (not just the compact
    # version in the button payload).
    row = None
    try:
        df = _fetch_pipeline_data(user_id)
        if df is not None and not df.empty:
            match = df[df["account_id"] == account_id]
            if not match.empty:
                row = match.iloc[0].to_dict()
    except Exception as e:
        logger.debug("Pipeline cache lookup failed for %s: %s", account_id, e)

    if row is None:
        # Fallback row built from the button payload
        row = {
            "account_id": account_id,
            "account_name": account_name,
            "opps": payload_opps,
            "last_call_title": None,
            "last_call_date": None,
            "last_call_summary": "",
        }

    opps = row.get("opps") or payload_opps

    # Play-driven drafts may target accounts with no open SFDC opp on the
    # pitched product (true for most P1 / P5 / P13 accounts). If the hook
    # provides a synthetic opp, use that so the drafter has a concrete anchor.
    play_context = ""
    pitched_product = ""
    if play_id:
        from templates.play_hooks import get_play_context, get_synthetic_opp, get_pitched_product
        play_context = get_play_context(play_id, play_row)
        pitched_product = get_pitched_product(play_id, play_row)
        if not opps:
            synth = get_synthetic_opp(play_id, play_row)
            if synth:
                opps = [synth]

    if not opps:
        client.chat_postMessage(
            channel=user_id,
            text=f":warning: Draft failed — no open opps on payload for *{account_name}*.",
        )
        return False

    # Pick recipients using the unified resolver (SFDC + Gong + email + call attendees)
    from utils.recipient_resolver import resolve_outbound_recipients
    primary, cc_contacts, debug = resolve_outbound_recipients(
        account_id=account_id,
        user_id=user_id,
        max_cc=4,
    )
    if not primary:
        client.chat_postMessage(
            channel=user_id,
            text=f":warning: No contact email found for *{account_name}*. Add a contact in SFDC, then retry.",
        )
        return False

    to_email = primary["email"]
    contact_name = primary.get("name", "")
    cc_string = ", ".join(c["email"] for c in cc_contacts)

    # Gather email history for the prompt
    email_history = _fetch_email_history(account_id)

    # User identity
    owner_name = get_user_sf_name(user_id) or ""
    owner_first_name = get_user_first_name(user_id) or (owner_name.split()[0] if owner_name else "")
    booking_link = get_user_booking_link(user_id) or ""

    # Resources: pull relevant help-article links based on products discussed +
    # transcript content. find_relevant_links uses the help_links catalog.
    try:
        from templates.help_links import find_relevant_links
        topic_seed = " ".join(
            [o.get("product") or "" for o in opps] +
            [row.get("last_call_summary") or ""]
        )
        resources = find_relevant_links(topic_seed, max_links=4)
    except Exception as e:
        logger.debug("Help link lookup failed: %s", e)
        resources = []

    try:
        body_text = _generate_body_text(account_name, opps, row, email_history,
                                         contact_name, owner_first_name,
                                         booking_link, resources=resources,
                                         play_context=play_context)
        subject = _generate_subject(account_name, opps, pitched_product=pitched_product)
    except Exception as e:
        logger.error("Pipeline drafter generation failed for %s: %s", account_id, e, exc_info=True)
        client.chat_postMessage(
            channel=user_id,
            text=f":warning: Draft generation failed for *{account_name}*: {e}",
        )
        return False

    # Build HTML body
    sig_html = build_signature(user_id=user_id)
    html_body = (
        '<div style="font-family:Arial,sans-serif;font-size:14px;color:#000;max-width:600px;">\n'
        "<!-- claude-auto-draft -->\n"
        f"{body_text.replace(chr(10), '<br>')}\n"
        "<br>\n"
        f"{sig_html}\n"
        "</div>"
    )

    # Plays drafts get their own Gmail label for filtering
    draft_label = f"Claude Drafts/Plays {play_id}" if play_id else DRAFT_LABEL
    pending_id = f"{'play_' + play_id + '_' if play_id else 'pipeline_'}{account_id[:12]}_{to_email.split('@')[0]}"

    created_ok = False
    if gumstack_ok():
        result = gumstack_create(
            to=to_email, subject=subject, html_body=html_body,
            cc=cc_string, label=draft_label, user_id=user_id,
        )
        created_ok = bool(result and result.get("success"))
        if not created_ok:
            save_pending_draft(
                draft_id=pending_id, to=to_email, cc=cc_string,
                subject=subject, html_body=html_body,
                account_name=account_name, label=draft_label, user_id=user_id,
            )
    else:
        save_pending_draft(
            draft_id=pending_id, to=to_email, cc=cc_string,
            subject=subject, html_body=html_body,
            account_name=account_name, label=draft_label, user_id=user_id,
        )

    products = ", ".join(sorted({o.get("product") or o.get("type") or "?" for o in opps}))

    # Context blurb: differentiate play-driven prospecting drafts (synthetic
    # opps) from unified re-engage drafts that cover real open opps.
    if play_id:
        # Check whether we're using a synthetic opp or a real one
        is_synthetic = bool(opps) and any(
            (o.get("type") or "").startswith(("Prospecting -", "Activation -",
                                              "Upgrade -", "Migration -", "Re-trial -"))
            for o in opps
        )
        if is_synthetic:
            header_tag = f"Play {play_id} prospecting draft"
            context_blurb = f"pitching *{products}* to *{account_name}* — no open SFDC opp yet"
        else:
            header_tag = f"Play {play_id} re-engage draft"
            context_blurb = f"covering {len(opps)} open opp{'s' if len(opps) != 1 else ''} ({products}) on *{account_name}*"
    else:
        header_tag = "Unified re-engage draft"
        context_blurb = f"covering {len(opps)} open opp{'s' if len(opps) != 1 else ''} ({products}) on *{account_name}*"

    if created_ok:
        why_primary = primary.get("why") or "SFDC contact"
        msg = (
            f":email: *{header_tag} saved to Gmail*\n"
            f"_{subject}_\n"
            f"*To:* {to_email} _({why_primary})_  ·  {context_blurb}"
        )
        if cc_contacts:
            cc_lines = [
                f"  • `{c['email']}` — _{c.get('why','SFDC contact')}_"
                for c in cc_contacts
            ]
            msg += "\n*CC:*\n" + "\n".join(cc_lines)
        msg += f"\n:white_check_mark: Labeled: `{draft_label}`"
    else:
        msg = (
            f":email: *{header_tag} queued*\n"
            f"_{subject}_\n"
            f"*To:* {to_email}  ·  {context_blurb}\n"
            f":warning: Direct Gmail creation failed — Glass cron will pick it up shortly."
        )
    client.chat_postMessage(channel=user_id, text=msg)
    return True
