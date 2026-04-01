"""Parse Slack alert messages and create Gmail drafts.

Each handler receives the alert message text, its Slack timestamp, and the
Slack client.  It extracts relevant fields via regex, builds the email from
the matching template, creates a Gmail draft, and DMs Greg a confirmation.
"""

import logging
import re
from datetime import datetime

from config import GREG_SLACK_ID
from core.slack_formatter import drafter_confirmation_blocks
from utils.pending_drafts import save_draft as save_pending_draft
from templates.emails import (
    ach_to_card_email,
    procurement_trial_email,
    pclip_email,
    large_decline_case_a_email,
    large_decline_case_b_email,
    fundraise_email,
    auto_card_loss_email,
    rclip_email,
    am_escalation_email,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════


def _create_or_queue_draft(draft_id, to, subject, html_body, account_name, label, cc="", user_id=None):
    """Create Gmail draft via Gumstack MCP, falling back to pending queue.

    Returns (method, success) tuple — method is 'gumstack' or 'queued'.
    """
    from core.gumstack_gmail import create_draft as gumstack_create, is_available as gumstack_ok
    if gumstack_ok():
        result = gumstack_create(to=to, subject=subject, html_body=html_body, cc=cc, label=label, user_id=user_id)
        if result["success"]:
            return "gumstack", True
        logger.warning("Gumstack draft failed for %s, falling back to queue", draft_id)

    save_pending_draft(
        draft_id=draft_id, to=to, cc=cc, subject=subject,
        html_body=html_body, account_name=account_name, label=label,
        user_id=user_id or "",
    )
    return "queued", True


def _strip_slack_email(text):
    """Strip Slack mailto formatting: <mailto:email|email> -> email"""
    match = re.search(r'<mailto:([^|>]+)', text)
    return match.group(1) if match else text.strip()


def _strip_slack_url(text):
    """Strip Slack URL formatting: <URL|label> -> URL"""
    match = re.search(r'<(https?://[^|>]+)', text)
    return match.group(1) if match else text.strip()


def _build_greeting(names):
    """Build natural-language greeting from first names."""
    if not names:
        return "Hi,"
    if len(names) == 1:
        return f"Hi {names[0]},"
    if len(names) == 2:
        return f"Hi {names[0]} and {names[1]},"
    return f"Hi {', '.join(names[:-1])}, and {names[-1]},"


def _dedup_contacts(contacts):
    """Deduplicate contacts by email (case-insensitive), skip blanks."""
    seen = set()
    result = []
    for c in contacts:
        email = (c.get("email") or "").strip().lower()
        if not email or email in seen:
            continue
        seen.add(email)
        result.append(c)
    return result


def _dm_greg(client, blocks, user_id=None):
    """Send a DM to the target user (defaults to Greg)."""
    dm_target = user_id or GREG_SLACK_ID
    try:
        client.chat_postMessage(
            channel=dm_target, blocks=blocks, text="Draft notification"
        )
    except Exception as e:
        logger.error("Failed to DM %s: %s", dm_target, e)


def _extract_field(pattern, text, default=""):
    """Run a regex search and return the first captured group, or *default*."""
    match = re.search(pattern, text, re.IGNORECASE)
    return match.group(1).strip() if match else default


def _extract_name_email(text):
    """Extract (name, email) from patterns like 'Name, <mailto:email|email>'
    or 'Name | email@domain'.

    Returns a dict with keys ``name``, ``email``, ``first_name``.
    """
    # Pattern: Name, <mailto:email|email>  or  Name, email@domain  or  Name | email@domain
    match = re.search(
        r'([A-Za-z][A-Za-z \'\-]+?)\s*[,|]\s*<?(?:mailto:)?([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})',
        text,
    )
    if match:
        name = match.group(1).strip()
        email = match.group(2).strip()
        first_name = name.split()[0] if name else ""
        return {"name": name, "email": email, "first_name": first_name}
    return {"name": "", "email": "", "first_name": ""}


def _extract_all_name_emails(text):
    """Extract all (name, email) pairs from a block of text."""
    contacts = []
    for match in re.finditer(
        r'([A-Za-z][A-Za-z \'\-]+?)\s*[,|]\s*<?(?:mailto:)?([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})',
        text,
    ):
        name = match.group(1).strip()
        email = match.group(2).strip()
        first_name = name.split()[0] if name else ""
        contacts.append({"name": name, "email": email, "first_name": first_name})
    return contacts


def _format_dollars(amount):
    """Format a numeric amount as $X,XXX."""
    try:
        return f"${int(amount):,}"
    except (ValueError, TypeError):
        return "$0"


def _format_natural_date(date_str):
    """Convert date string to natural format like 'July 6th'.

    Handles formats: 2026-07-06, 07/06/2026, July 6 2026, etc.
    Falls back to the original string if parsing fails.
    """
    date_str = date_str.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%B %d %Y", "%b %d, %Y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            day = dt.day
            if 11 <= day <= 13:
                suffix = "th"
            else:
                suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
            return f"{dt.strftime('%B')} {day}{suffix}"
        except ValueError:
            continue
    return date_str


# ═══════════════════════════════════════════════════════════════════════════════
# 1. ACH-to-Card
# ═══════════════════════════════════════════════════════════════════════════════


def handle_ach_to_card_alert(text, ts, client, user_id=None):
    """Parse an ACH-to-card Slack alert and create a Gmail draft.

    Expected fields in the alert text:
        Vendor, Amount, Cashback Estimate, Payment Scheduled For / Due Date,
        Bill Creator, Vendor Owner(s), Business Owner, Payment Portal, View Bill
    """
    try:
        # ── Extract fields ────────────────────────────────────────────────
        # Real alerts use Slack bold: *Vendor:* and blockquotes: > *Vendor:*
        # Strip bold markers before extracting
        clean = re.sub(r'\*([A-Za-z ]+(?:\(s\))?):?\*', r'\1:', text)

        vendor_name = _extract_field(r'Vendor:\s*(.+)', clean, "Unknown Vendor")
        vendor_name = vendor_name.strip()

        # Amount: "61,250.00 USD (Cashback Estimate*: 918.75 USD)"
        invoice_value = _extract_field(
            r'Amount:\s*([\d,]+(?:\.\d+)?)', clean, "0"
        )
        cashback_estimate = _extract_field(
            r'Cashback(?:\s*Estimate)?\*?:\s*([\d,]+(?:\.\d+)?)', clean, "0"
        )

        # Prefer Payment Scheduled For over Due Date (more actionable)
        due_date = _extract_field(r'Payment Scheduled For:\s*(.+)', clean, "")
        if not due_date:
            due_date = _extract_field(r'Due Date:\s*(.+)', clean, "TBD")
        due_date = due_date.strip()

        # Bill Creator — "Name, email" or "Name | email" pattern
        # Handle "Bill Creator*:" (trailing asterisk from bold)
        bill_creator_block = _extract_field(r'Bill Creator\*?:\s*(.+)', clean, "")
        bill_creator = _extract_name_email(bill_creator_block)

        # Vendor Owner(s) — may contain multiple "Name, email" entries
        vendor_owner_block = _extract_field(
            r'Vendor Owner(?:\(s\))?:\s*(.+?)(?:\n|Business Owner)', clean, ""
        )
        vendor_owners = _extract_all_name_emails(vendor_owner_block) if vendor_owner_block else []

        # Business Owner
        biz_owner_block = _extract_field(r'Business Owner:\s*(.+)', clean, "")
        biz_owner = _extract_name_email(biz_owner_block)

        # Payment portal — check both "Payment Portal:" field and :dart: link format
        payment_portal_raw = _extract_field(r'Payment Portal:\s*(.+)', clean, "")
        has_payment_portal = (
            bool(payment_portal_raw)
            and "no payment portal" not in payment_portal_raw.lower()
            and "no portal" not in payment_portal_raw.lower()
        )
        payment_portal_link = _strip_slack_url(payment_portal_raw) if has_payment_portal else ""

        # Also check :dart: <URL|Payment Portal> format used in real alerts
        if not payment_portal_link:
            dart_match = re.search(r'<(https?://[^|>]+)\|Payment Portal>', text)
            if dart_match:
                payment_portal_link = dart_match.group(1)
                has_payment_portal = True

        # Check bill type field for "No Payment Portal"
        bill_type = _extract_field(
            r'Card Payable Bill Type:\s*(.+?)(?:\s*:star:|\s*$)', clean, ""
        ).strip()
        if "no payment portal" in bill_type.lower():
            has_payment_portal = False
            payment_portal_link = ""

        # View bill link — check both field and <URL|View Bill> format
        view_bill_raw = _extract_field(r'View Bill:\s*(.+)', clean, "")
        view_bill_link = _strip_slack_url(view_bill_raw) if view_bill_raw else ""
        if not view_bill_link:
            vb_match = re.search(r'<(https?://[^|>]+)\|View Bill>', text)
            view_bill_link = vb_match.group(1) if vb_match else ""

        # ── Build recipients ──────────────────────────────────────────────
        contacts = []
        if bill_creator.get("email"):
            contacts.append(bill_creator)
        contacts.extend(vendor_owners)
        if biz_owner.get("email"):
            contacts.append(biz_owner)
        contacts = _dedup_contacts(contacts)

        if not contacts:
            logger.warning(
                "ACH-to-Card alert (ts=%s): no recipients found, skipping", ts
            )
            return

        first_names = [c["first_name"] for c in contacts if c.get("first_name")]
        greeting = _build_greeting(first_names)
        to_emails = ", ".join(c["email"] for c in contacts)

        # Format amounts with $ prefix (no trailing .00 cents)
        amount_formatted = f"${invoice_value}" if not invoice_value.startswith("$") else invoice_value
        amount_formatted = re.sub(r'\.00$', '', amount_formatted)
        cashback_formatted = f"${cashback_estimate}" if cashback_estimate and cashback_estimate != "0" else ""
        cashback_formatted = re.sub(r'\.00$', '', cashback_formatted)

        # Format date naturally (e.g., "July 6th")
        due_date_natural = _format_natural_date(due_date)

        # ── Build email ───────────────────────────────────────────────────
        html_body = ach_to_card_email(
            greeting=greeting,
            vendor_name=vendor_name,
            invoice_value=amount_formatted,
            due_date=due_date_natural,
            has_payment_portal=has_payment_portal,
            payment_portal_link=payment_portal_link,
            cashback_formatted=cashback_formatted,
            user_id=user_id,
        )

        subject = "Quick Win: Earn Cash Back [URGENT]"

        draft_id = f"ach_{ts}"
        draft_method, _ = _create_or_queue_draft(
            draft_id=draft_id, to=to_emails, subject=subject,
            html_body=html_body, account_name=vendor_name,
            label="Claude Drafts/ACH to Card", user_id=user_id,
        )

        # ── DM Greg ───────────────────────────────────────────────────────
        draft_note = ""
        details = (
            f"*To:* {to_emails}\n"
            f"*Subject:* {subject}\n"
            f"*Vendor:* {vendor_name}\n"
            f"*Amount:* {amount_formatted}"
        )
        if cashback_formatted:
            details += f"  |  *Est. Cashback:* {cashback_formatted}"
        details += f"\n*Due:* {due_date}"
        if view_bill_link:
            details += f"\n<{view_bill_link}|View Bill>"
        if draft_note:
            details += f"\n{draft_note}"

        blocks = drafter_confirmation_blocks(
            drafter_type="ACH-to-Card",
            account_name=vendor_name,
            details=details,
            draft_id=draft_id,
        )
        _dm_greg(client, blocks, user_id=user_id)

        logger.info(
            "ACH-to-Card draft created (%s) — vendor=%s to=%s draft=%s",
            draft_method, vendor_name, to_emails, draft_id,
        )

    except Exception as e:
        logger.exception("Error handling ACH-to-Card alert (ts=%s): %s", ts, e)
        try:
            _dm_greg(
                client,
                drafter_confirmation_blocks(
                    drafter_type="ACH-to-Card",
                    account_name="ERROR",
                    details=f"Failed to create draft:\n```{e}```",
                ),
                user_id=user_id,
            )
        except Exception:
            logger.error("Could not DM Greg about ACH-to-Card failure")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Procurement Trial
# ═══════════════════════════════════════════════════════════════════════════════


def handle_procurement_trial_alert(text, ts, client, user_id=None):
    """Parse a Procurement trial activation alert and create a Gmail draft."""
    try:
        # ── Extract fields ────────────────────────────────────────────────
        company_name = _extract_field(r'Company:\s*(.+)', text, "Unknown Company")

        # Activating user — "Name, email" or "Activating User: Name, email"
        activating_block = _extract_field(r'Activating User:\s*(.+)', text, "")
        activating_user = _extract_name_email(activating_block)

        # Admin contacts — may be a multi-line block
        admin_block = _extract_field(
            r'Admin(?:\s*Contact)?s?:\s*([\s\S]+?)(?:\n\n|\Z)', text, ""
        )
        admin_contacts = _extract_all_name_emails(admin_block) if admin_block else []

        # ── Build recipients ──────────────────────────────────────────────
        contacts = []
        if activating_user.get("email"):
            contacts.append(activating_user)
        contacts.extend(admin_contacts)
        contacts = _dedup_contacts(contacts)

        if not contacts:
            logger.warning(
                "Procurement Trial alert (ts=%s): no recipients found, skipping", ts
            )
            return

        first_names = [c["first_name"] for c in contacts if c.get("first_name")]
        greeting = _build_greeting(first_names)
        to_emails = ", ".join(c["email"] for c in contacts)

        # ── Build email ───────────────────────────────────────────────────
        html_body = procurement_trial_email(greeting=greeting, user_id=user_id)

        subject = "Ramp Procurement Trial + AM intro"
        draft_id = f"procurement_{ts}"
        draft_method, _ = _create_or_queue_draft(
            draft_id=draft_id, to=to_emails, subject=subject,
            html_body=html_body, account_name=company_name,
            label="Claude Drafts/Procurement Trials", user_id=user_id,
        )

        # ── DM Greg ───────────────────────────────────────────────────────
        details = (
            f"*To:* {to_emails}\n"
            f"*Subject:* {subject}\n"
            f"*Company:* {company_name}\n"
            f"*Activating User:* {activating_user.get('name', 'N/A')}"
        )

        blocks = drafter_confirmation_blocks(
            drafter_type="Procurement Trial",
            account_name=company_name,
            details=details,
            draft_id=draft_id,
        )
        _dm_greg(client, blocks, user_id=user_id)

        logger.info(
            "Procurement Trial draft created — company=%s to=%s draft=%s",
            company_name, to_emails, draft_id,
        )

    except Exception as e:
        logger.exception(
            "Error handling Procurement Trial alert (ts=%s): %s", ts, e
        )
        try:
            _dm_greg(
                client,
                drafter_confirmation_blocks(
                    drafter_type="Procurement Trial",
                    account_name="ERROR",
                    details=f"Failed to create draft:\n```{e}```",
                ),
                user_id=user_id,
            )
        except Exception:
            logger.error("Could not DM Greg about Procurement Trial failure")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. PCLIP (Program Credit Limit Increase)
# ═══════════════════════════════════════════════════════════════════════════════


def handle_pclip_alert(text, ts, client, user_id=None):
    """Parse a PCLIP activation alert and create a Gmail draft.

    Skips drafting if the limit increase is less than $100,000.

    Actual alert format (Slack mrkdwn):
        Proactive Clip Request activated
        > *Business Name:* Acme Corp
        > *Accepted by:* Jane Doe
        > *Business Owner Email:* owner@acme.com
        > *New Limit Amount:* $500,000.00
        > *Old Limit Amount:* $100,000.00
        > *Is Limit Fixed?:* False
        > *Account Manager:* <@U06DAFU4YRG|Greg>
    """
    try:
        # ── Strip Slack bold markers for easier parsing ────────────────────
        clean = re.sub(r'\*([A-Za-z /]+(?:\(s\))?):?\*', r'\1:', text)

        # ── Extract fields ────────────────────────────────────────────────
        company_name = _extract_field(
            r'Business Name:\s*(.+)', clean, "Unknown Company"
        )

        # Accepting user (may be just a name, no email in PCLIP alerts)
        accepted_by = _extract_field(r'Accepted by:\s*(.+)', clean, "")

        # Limits — numbers with optional commas / dollar signs / decimals
        old_limit_raw = _extract_field(
            r'Old Limit(?:\s*Amount)?:\s*\$?([\d,]+)', clean, "0"
        )
        new_limit_raw = _extract_field(
            r'New Limit(?:\s*Amount)?:\s*\$?([\d,]+)', clean, "0"
        )

        old_limit = int(old_limit_raw.replace(",", ""))
        new_limit = int(new_limit_raw.replace(",", ""))
        limit_increase = new_limit - old_limit

        # ── Threshold filter ──────────────────────────────────────────────
        if limit_increase < 100_000:
            logger.info(
                "PCLIP alert (ts=%s): increase $%s < $100k threshold, skipping silently",
                ts, f"{limit_increase:,}",
            )
            return

        # Business Owner Email (may be just an email, no name)
        biz_owner_email_raw = _extract_field(
            r'Business Owner Email:\s*(.+)', clean, ""
        )
        biz_owner_email = _strip_slack_email(biz_owner_email_raw) if biz_owner_email_raw else ""

        # Build contacts — PCLIP alerts may only have the Business Owner Email
        contacts = []
        if biz_owner_email:
            # Try to get first name from the email local part
            local = biz_owner_email.split("@")[0]
            first_guess = local.split(".")[0].capitalize() if "." in local else ""
            contacts.append({
                "name": "", "email": biz_owner_email,
                "first_name": first_guess,
            })
        # Also check for accepted_by (may have email via mailto)
        if accepted_by:
            accepted_contact = _extract_name_email(accepted_by)
            if not accepted_contact.get("email"):
                # Try raw email extraction from the field
                email_match = re.search(r'[\w.+-]+@[\w.-]+\.\w+', accepted_by)
                if email_match:
                    accepted_contact = {
                        "name": "", "email": email_match.group(0),
                        "first_name": "",
                    }
            if accepted_contact.get("email"):
                contacts.append(accepted_contact)

        contacts = _dedup_contacts(contacts)

        if not contacts:
            logger.warning(
                "PCLIP alert (ts=%s): no recipients found, skipping", ts
            )
            return

        first_names = [c["first_name"] for c in contacts if c.get("first_name")]
        first_name = first_names[0] if first_names else "there"
        to_emails = ", ".join(c["email"] for c in contacts)

        old_limit_fmt = _format_dollars(old_limit)
        new_limit_fmt = _format_dollars(new_limit)

        # ── Build email ───────────────────────────────────────────────────
        html_body = pclip_email(first_name=first_name, user_id=user_id)

        subject = "Ramp Limit Increase + AM intro"
        draft_id = f"pclip_{ts}"
        draft_method, _ = _create_or_queue_draft(
            draft_id=draft_id, to=to_emails, subject=subject,
            html_body=html_body, account_name=company_name,
            label="Claude Drafts/PCLIP Activation", user_id=user_id,
        )

        # ── DM Greg ───────────────────────────────────────────────────────
        details = (
            f"*To:* {to_emails}\n"
            f"*Subject:* {subject}\n"
            f"*Company:* {company_name}\n"
            f"*Old Limit:* {old_limit_fmt}  |  *New Limit:* {new_limit_fmt}\n"
            f"*Increase:* {_format_dollars(limit_increase)}"
        )

        blocks = drafter_confirmation_blocks(
            drafter_type="PCLIP",
            account_name=company_name,
            details=details,
            draft_id=draft_id,
        )
        _dm_greg(client, blocks, user_id=user_id)

        logger.info(
            "PCLIP draft created — company=%s increase=%s to=%s draft=%s",
            company_name, _format_dollars(limit_increase), to_emails, draft_id,
        )

    except Exception as e:
        logger.exception("Error handling PCLIP alert (ts=%s): %s", ts, e)
        try:
            _dm_greg(
                client,
                drafter_confirmation_blocks(
                    drafter_type="PCLIP",
                    account_name="ERROR",
                    details=f"Failed to create draft:\n```{e}```",
                ),
                user_id=user_id,
            )
        except Exception:
            logger.error("Could not DM Greg about PCLIP failure")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Large Decline
# ═══════════════════════════════════════════════════════════════════════════════

# Decline reason constants
_CASE_A_REASON = "AUTHORIZER_NON_AP_CARD_VELOCITY_LIMIT"
_CASE_B_REASON = "OPEN_TO_BUY_LIMIT"


def handle_large_decline_alert(text, ts, client, user_id=None):
    """Parse a large-decline Slack alert and create a Gmail draft.

    Routing logic:
        - Case A (velocity limit): Always draft.
        - Case B (open-to-buy): Draft only if transaction_amount > $25k
          OR available_limit > $100k.
        - Other decline reasons: Skip.
    """
    try:
        # ── Extract fields ────────────────────────────────────────────────
        decline_reason = _extract_field(
            r'(?:Decline\s*Reason|Reason):\s*(.+)', text, ""
        ).strip()
        vendor_name = _extract_field(r'Vendor:\s*(.+)', text, "Unknown Vendor")

        # Amounts — digits with optional commas, decimal
        txn_amount_raw = _extract_field(
            r'(?:Transaction\s*Amount|Amount):\s*\$?([\d,]+(?:\.\d+)?)', text, "0"
        )
        avail_limit_raw = _extract_field(
            r'(?:Available\s*Limit|Available\s*Balance):\s*\$?([\d,]+(?:\.\d+)?)',
            text, "0",
        )
        transaction_amount = int(float(txn_amount_raw.replace(",", "")))
        available_limit = int(float(avail_limit_raw.replace(",", "")))

        # Attempting user
        attempting_block = _extract_field(
            r'(?:Attempting\s*User|Cardholder|User):\s*(.+)', text, ""
        )
        attempting_user = _extract_name_email(attempting_block)

        # Vendor Owner
        vendor_owner_block = _extract_field(r'Vendor Owner:\s*(.+)', text, "")
        vendor_owner = _extract_name_email(vendor_owner_block)

        # Business Owner
        biz_owner_block = _extract_field(r'Business Owner:\s*(.+)', text, "")
        biz_owner = _extract_name_email(biz_owner_block)

        # Formatted amounts
        amount_formatted = _format_dollars(transaction_amount)
        avail_limit_formatted = _format_dollars(available_limit)

        # ── Determine case ────────────────────────────────────────────────
        reason_normalized = decline_reason.upper().replace(" ", "_")
        is_case_a = _CASE_A_REASON in reason_normalized
        is_case_b = _CASE_B_REASON in reason_normalized

        if is_case_a:
            case = "A"
        elif is_case_b:
            # Threshold filter for Case B
            if transaction_amount <= 25_000 and available_limit <= 100_000:
                logger.info(
                    "Large Decline (ts=%s): Case B below thresholds "
                    "(txn=%s, avail=%s), skipping",
                    ts, amount_formatted, avail_limit_formatted,
                )
                _dm_greg(
                    client,
                    drafter_confirmation_blocks(
                        drafter_type="Large Decline",
                        account_name=vendor_name,
                        details=(
                            f"*Skipped (Case B)* \u2014 below thresholds.\n"
                            f"*Amount:* {amount_formatted}  |  "
                            f"*Available:* {avail_limit_formatted}\n"
                            f"*Reason:* {decline_reason}"
                        ),
                    ),
                    user_id=user_id,
                )
                return
            case = "B"
        else:
            logger.info(
                "Large Decline (ts=%s): unhandled reason '%s', skipping",
                ts, decline_reason,
            )
            _dm_greg(
                client,
                drafter_confirmation_blocks(
                    drafter_type="Large Decline",
                    account_name=vendor_name,
                    details=(
                        f"*Skipped* \u2014 unhandled decline reason.\n"
                        f"*Reason:* {decline_reason}\n"
                        f"*Amount:* {amount_formatted}  |  *Vendor:* {vendor_name}"
                    ),
                ),
                user_id=user_id,
            )
            return

        # ── Build recipients ──────────────────────────────────────────────
        contacts = []
        if attempting_user.get("email"):
            contacts.append(attempting_user)
        if vendor_owner.get("email"):
            contacts.append(vendor_owner)
        if biz_owner.get("email"):
            contacts.append(biz_owner)
        contacts = _dedup_contacts(contacts)

        if not contacts:
            logger.warning(
                "Large Decline alert (ts=%s): no recipients found, skipping", ts
            )
            return

        first_name = attempting_user.get("first_name") or contacts[0].get(
            "first_name", "there"
        )
        to_emails = ", ".join(c["email"] for c in contacts)

        # ── Build email ───────────────────────────────────────────────────
        if case == "A":
            html_body = large_decline_case_a_email(
                first_name=first_name,
                vendor_name=vendor_name,
                amount_formatted=amount_formatted,
                user_id=user_id,
            )
            subject = f"Declined transaction \u2014 {vendor_name} ({amount_formatted})"
        else:
            html_body = large_decline_case_b_email(
                first_name=first_name,
                vendor_name=vendor_name,
                amount_formatted=amount_formatted,
                available_limit_formatted=avail_limit_formatted,
                user_id=user_id,
            )
            subject = "Declined transaction \u2014 potential limit increase"

        draft_id = f"decline_{case}_{ts}"
        draft_method, _ = _create_or_queue_draft(
            draft_id=draft_id, to=to_emails, subject=subject,
            html_body=html_body, account_name=vendor_name,
            label="Claude Drafts/Large Declines", user_id=user_id,
        )

        # ── DM Greg ───────────────────────────────────────────────────────
        details = (
            f"*To:* {to_emails}\n"
            f"*Subject:* {subject}\n"
            f"*Case:* {case} ({'Velocity Limit' if case == 'A' else 'Open-to-Buy'})\n"
            f"*Vendor:* {vendor_name}\n"
            f"*Amount:* {amount_formatted}  |  *Available:* {avail_limit_formatted}\n"
            f"*Reason:* {decline_reason}"
        )

        blocks = drafter_confirmation_blocks(
            drafter_type="Large Decline",
            account_name=vendor_name,
            details=details,
            draft_id=draft_id,
        )
        _dm_greg(client, blocks, user_id=user_id)

        logger.info(
            "Large Decline (Case %s) draft created — vendor=%s amount=%s to=%s draft=%s",
            case, vendor_name, amount_formatted, to_emails, draft_id,
        )

    except Exception as e:
        logger.exception("Error handling Large Decline alert (ts=%s): %s", ts, e)
        try:
            _dm_greg(
                client,
                drafter_confirmation_blocks(
                    drafter_type="Large Decline",
                    account_name="ERROR",
                    details=f"Failed to create draft:\n```{e}```",
                ),
                user_id=user_id,
            )
        except Exception:
            logger.error("Could not DM Greg about Large Decline failure")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Fundraise
# ═══════════════════════════════════════════════════════════════════════════════


def handle_fundraise_alert(text, ts, client, user_id=None):
    """Parse a fundraise Slack alert and create a Gmail draft.

    Expected fields in the alert text:
        Business, Announcement Date, Funding Amount, Funding Round Type,
        Investors, SFDC Account (link with account ID), Ramp POC
    """
    try:
        # ── Extract fields ────────────────────────────────────────────────
        company_name = _extract_field(r'Business:\s*(.+?)(?:\s*\(|\n)', text, "Unknown Company")
        funding_amount = _extract_field(r'Funding Amount:\s*\$?([\d,]+)', text, "")
        round_type = _extract_field(r'Funding Round Type:\s*(.+)', text, "")

        # Extract SFDC account ID from the link
        account_id = _extract_field(
            r'Account/([a-zA-Z0-9]{15,18})/view', text, ""
        )

        if not account_id:
            logger.warning(
                "Fundraise alert (ts=%s): no SFDC account ID found, skipping", ts
            )
            return

        # ── Find contacts from SFDC ────────────────────────────────────────
        from utils.account_resolver import fetch_contact_emails, is_hash_like

        contacts_by_account = fetch_contact_emails(None, [account_id])
        acct_contacts = contacts_by_account.get(account_id, [])

        contact_email = ""
        contact_name = ""
        first_name = ""

        for c in acct_contacts:
            if c.get("email") and not is_hash_like(c.get("name", "")):
                contact_email = c["email"]
                contact_name = c.get("name", "")
                first_name = contact_name.split()[0] if contact_name else ""
                break

        if not contact_email:
            logger.warning(
                "Fundraise alert (ts=%s): no contact email for %s, skipping draft",
                ts, company_name,
            )
            # Still DM Greg so he knows about it
            _dm_greg(
                client,
                drafter_confirmation_blocks(
                    drafter_type="Fundraise",
                    account_name=company_name,
                    details=(
                        f"*Skipped* — no contact email found.\n"
                        f"*Funding:* ${funding_amount} ({round_type})\n"
                        f"*SFDC:* <https://rampfinancial.lightning.force.com/lightning"
                        f"/r/Account/{account_id}/view|View Account>"
                    ),
                ),
                user_id=user_id,
            )
            return

        # ── Build email ───────────────────────────────────────────────────
        html_body = fundraise_email(first_name=first_name or "there", user_id=user_id)

        amount_display = f"${funding_amount}" if funding_amount else "undisclosed"
        subject = "Re: Fundraise + AM intro"

        draft_id = f"fundraise_{ts}"
        draft_method, _ = _create_or_queue_draft(
            draft_id=draft_id, to=contact_email, subject=subject,
            html_body=html_body, account_name=company_name,
            label="Claude Drafts/Fundraise", user_id=user_id,
        )

        # ── DM Greg ───────────────────────────────────────────────────────
        details = (
            f"*To:* {contact_email} ({contact_name})\n"
            f"*Subject:* {subject}\n"
            f"*Company:* {company_name}\n"
            f"*Funding:* {amount_display} ({round_type})"
        )

        blocks = drafter_confirmation_blocks(
            drafter_type="Fundraise",
            account_name=company_name,
            details=details,
            draft_id=draft_id,
        )
        _dm_greg(client, blocks, user_id=user_id)

        logger.info(
            "Fundraise draft created (%s) — company=%s to=%s draft=%s",
            draft_method, company_name, contact_email, draft_id,
        )

    except Exception as e:
        logger.exception("Error handling Fundraise alert (ts=%s): %s", ts, e)
        try:
            _dm_greg(
                client,
                drafter_confirmation_blocks(
                    drafter_type="Fundraise",
                    account_name="ERROR",
                    details=f"Failed to create draft:\n```{e}```",
                ),
                user_id=user_id,
            )
        except Exception:
            logger.error("Could not DM Greg about Fundraise failure")


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Automatic Card Loss
# ═══════════════════════════════════════════════════════════════════════════════


def handle_auto_card_alert(text, ts, client, user_id=None):
    """Parse a bill-pay-automatic-card-losses alert and create a Gmail draft.

    Expected fields in the alert text:
        Vendor, Current Bill Amount (Cashback Estimate), Due Date,
        Payment Scheduled For, Account Manager, Bill Creator,
        Business Owner, View Bill
    """
    try:
        # ── Extract fields ────────────────────────────────────────────────
        clean = re.sub(r'\*([A-Za-z ]+(?:\(s\))?):?\*', r'\1:', text)

        vendor_name = _extract_field(r'Vendor:\s*(.+)', clean, "Unknown Vendor").strip()

        # Cashback Estimate: "167.88 USD"
        cashback_estimate = _extract_field(
            r'Cashback(?:\s*Estimate)?\*?:\s*([\d,]+(?:\.\d+)?)', clean, "0"
        )

        # Bill Creator — "Name (<mailto:email|email>)" or "Name, email"
        bill_creator_block = _extract_field(r'Bill Creator\*?:\s*(.+)', clean, "")
        bill_creator = _extract_name_email(bill_creator_block)

        # Business Owner
        biz_owner_block = _extract_field(r'Business Owner:\s*(.+)', clean, "")
        biz_owner = _extract_name_email(biz_owner_block)

        # View Bill link — <URL|View Bill> format
        view_bill_link = ""
        vb_match = re.search(r'<(https?://[^|>]+)\|View Bill>', text)
        if vb_match:
            view_bill_link = vb_match.group(1)
        if not view_bill_link:
            view_bill_raw = _extract_field(r'View Bill:\s*(.+)', clean, "")
            view_bill_link = _strip_slack_url(view_bill_raw) if view_bill_raw else ""

        # ── Build recipients ──────────────────────────────────────────────
        # To: business owner, CC: bill creator
        to_email = biz_owner.get("email", "")
        cc_email = bill_creator.get("email", "")
        first_name = biz_owner.get("first_name", "")

        if not to_email and cc_email:
            # Fallback: if no biz owner, send to bill creator
            to_email = cc_email
            cc_email = ""
            first_name = bill_creator.get("first_name", "")

        if not to_email:
            logger.warning(
                "Auto Card alert (ts=%s): no recipients found, skipping", ts
            )
            return

        # ── Build email ───────────────────────────────────────────────────
        html_body = auto_card_loss_email(
            first_name=first_name,
            vendor_name=vendor_name,
            estimated_cashback=cashback_estimate,
            view_bill_link=view_bill_link or "https://app.ramp.com/bills",
            user_id=user_id,
        )

        subject = f"Quick win on {vendor_name}: cashback available on this invoice"

        draft_id = f"autocard_{ts}"
        draft_method, _ = _create_or_queue_draft(
            draft_id=draft_id, to=to_email, subject=subject,
            html_body=html_body, account_name=vendor_name,
            label="Claude Drafts/Automatic Card", cc=cc_email,
            user_id=user_id,
        )

        # ── DM user ───────────────────────────────────────────────────────
        details = (
            f"*To:* {to_email}\n"
            f"*CC:* {cc_email}\n"
            f"*Subject:* {subject}\n"
            f"*Vendor:* {vendor_name}\n"
            f"*Est. Cashback:* ${cashback_estimate}"
        )
        if view_bill_link:
            details += f"\n<{view_bill_link}|View Bill>"

        blocks = drafter_confirmation_blocks(
            drafter_type="Automatic Card",
            account_name=vendor_name,
            details=details,
            draft_id=draft_id,
        )
        _dm_greg(client, blocks, user_id=user_id)

        logger.info(
            "Auto Card draft created (%s) — vendor=%s to=%s cc=%s draft=%s",
            draft_method, vendor_name, to_email, cc_email, draft_id,
        )

    except Exception as e:
        logger.exception("Error handling Auto Card alert (ts=%s): %s", ts, e)
        try:
            _dm_greg(
                client,
                drafter_confirmation_blocks(
                    drafter_type="Automatic Card",
                    account_name="ERROR",
                    details=f"Failed to create draft:\n```{e}```",
                ),
                user_id=user_id,
            )
        except Exception:
            logger.error("Could not DM about Auto Card failure")


# ═══════════════════════════════════════════════════════════════════════════════
# 7. RCLIP (Reactive Credit Limit Increase)
# ═══════════════════════════════════════════════════════════════════════════════


def handle_rclip_alert(text, ts, client, user_id=None):
    """Parse a reactive CLIP alert and create a Gmail draft.

    Only drafts when Status=Approved and (New Limit - Old Limit) > $50,000.

    Actual alert format (Slack mrkdwn):
        Reactive Clip requested
        > *Business Name:* CONCRETE ENGINE INC
        > *Requested by:* email (Name)
        > *Status:* Approved
        > *New Limit Amount:* $10,000.00
        > *Old Limit Amount:* $3,250.00
        > *Salesforce Account:* <SF link>
        > *Account Manager:* <@U...|Name>
    """
    try:
        # ── Strip Slack bold markers ───────────────────────────────────────
        clean = re.sub(r'\*([A-Za-z /]+(?:\(s\))?):?\*', r'\1:', text)

        company_name = _extract_field(
            r'Business Name:\s*(.+)', clean, "Unknown Company"
        )

        # Status — only draft for Approved
        status = _extract_field(r'Status:\s*(.+)', clean, "").strip()
        if "approved" not in status.lower():
            logger.info(
                "RCLIP alert (ts=%s): status=%s not Approved, skipping", ts, status
            )
            return

        # Limits
        old_limit_raw = _extract_field(
            r'Old Limit(?:\s*Amount)?:\s*\$?([\d,]+)', clean, "0"
        )
        new_limit_raw = _extract_field(
            r'New Limit(?:\s*Amount)?:\s*\$?([\d,]+)', clean, "0"
        )
        old_limit = int(old_limit_raw.replace(",", ""))
        new_limit = int(new_limit_raw.replace(",", ""))
        limit_increase = new_limit - old_limit

        # ── Threshold filter: delta > $50k ────────────────────────────────
        if limit_increase < 50_000:
            logger.info(
                "RCLIP alert (ts=%s): increase $%s < $50k threshold, skipping",
                ts, f"{limit_increase:,}",
            )
            return

        # Requested by — format: "email (Name)" or "<mailto:email|email> (Name)"
        requested_by_raw = _extract_field(r'Requested by:\s*(.+)', clean, "")
        req_email = ""
        req_name = ""
        if requested_by_raw:
            req_email = _strip_slack_email(requested_by_raw.split("(")[0].strip())
            name_match = re.search(r'\(([^)]+)\)', requested_by_raw)
            if name_match:
                req_name = name_match.group(1).strip()

        if not req_email:
            logger.warning(
                "RCLIP alert (ts=%s): no email found, skipping", ts
            )
            return

        first_name = req_name.split()[0] if req_name else "there"

        # ── Build email ───────────────────────────────────────────────────
        html_body = rclip_email(first_name=first_name, user_id=user_id)

        subject = "Ramp Limit Increase + AM intro"
        draft_id = f"rclip_{ts}"
        draft_method, _ = _create_or_queue_draft(
            draft_id=draft_id, to=req_email, subject=subject,
            html_body=html_body, account_name=company_name,
            label="Claude Drafts/RCLIP", user_id=user_id,
        )

        # ── DM user ───────────────────────────────────────────────────────
        old_fmt = _format_dollars(old_limit)
        new_fmt = _format_dollars(new_limit)
        details = (
            f"*To:* {req_email} ({req_name})\n"
            f"*Subject:* {subject}\n"
            f"*Company:* {company_name}\n"
            f"*Old Limit:* {old_fmt}  |  *New Limit:* {new_fmt}\n"
            f"*Increase:* {_format_dollars(limit_increase)}"
        )

        blocks = drafter_confirmation_blocks(
            drafter_type="RCLIP",
            account_name=company_name,
            details=details,
            draft_id=draft_id,
        )
        _dm_greg(client, blocks, user_id=user_id)

        logger.info(
            "RCLIP draft created — company=%s increase=%s to=%s draft=%s",
            company_name, _format_dollars(limit_increase), req_email, draft_id,
        )

    except Exception as e:
        logger.exception("Error handling RCLIP alert (ts=%s): %s", ts, e)
        try:
            _dm_greg(
                client,
                drafter_confirmation_blocks(
                    drafter_type="RCLIP",
                    account_name="ERROR",
                    details=f"Failed to create draft:\n```{e}```",
                ),
                user_id=user_id,
            )
        except Exception:
            logger.error("Could not DM about RCLIP failure")


# ═══════════════════════════════════════════════════════════════════════════════
# 8. AM Escalation
# ═══════════════════════════════════════════════════════════════════════════════


def _clean_escalation_context(ticket_desc: str, escalation_reason: str) -> str:
    """Build a natural context phrase from the ticket description and escalation reason.

    Returns a phrase suitable for: 'our support team let me know {context}'.
    """
    # Prefer escalation reason (shorter, more specific)
    raw = escalation_reason.strip() if escalation_reason.strip() else ticket_desc.strip()
    if not raw:
        return "you were looking for some help"

    raw = raw.rstrip(".")
    lower = raw.lower()

    # Strip common prefixes to make it read naturally
    for prefix in [
        "customer wants ", "customer needs ", "customer is looking for ",
        "customer would like ", "customer is asking ", "customer asked ",
        "they want ", "they need ", "user wants ", "user needs ",
        "customer needs help with ", "customer wants help with ",
    ]:
        if lower.startswith(prefix):
            remainder = raw[len(prefix):]
            # Avoid "some help with help with X"
            if remainder.lower().startswith("help with "):
                return f"you were looking for some {remainder}"
            return f"you were looking for some help with {remainder}"

    # If the text is a first-person request from the customer (ticket desc)
    if lower.startswith("hello") or lower.startswith("hi") or lower.startswith("i "):
        return "you were looking for some help"

    return f"you were looking for some help with {raw}"


def handle_am_escalation_alert(text, ts, client, user_id=None):
    """Parse a CX-to-AM escalation alert and create a contextual Gmail draft.

    Actual alert format:
        CX to Post Sales Submission from @agent
        Account Manager / CSM: <@U06DAFU4YRG>
        Business ID: 950745
        Business Name: Divvy Holdings NC LLC
        User Email: willd@blueplanetplumbing.com
        Salesforce Account Link: https://...
        Zendesk Ticket Link: https://...
        Ticket description: Hello, I would like to...
        Escalation reason: customer wants Bill Pay turned on
    """
    try:
        # ── Strip Slack bold markers for easier parsing ────────────────────
        clean = re.sub(r'\*([A-Za-z /]+(?:\(s\))?):?\*', r'\1:', text)

        # ── Extract fields ────────────────────────────────────────────────
        company_name = _extract_field(
            r'Business Name:\s*(.+)', clean, "Unknown Company"
        )
        user_email_raw = _extract_field(r'User Email:\s*(.+)', clean, "")
        user_email = _strip_slack_email(user_email_raw) if user_email_raw else ""

        ticket_desc = _extract_field(r'Ticket description:\s*(.+)', clean, "")
        escalation_reason = _extract_field(r'Escalation reason:\s*(.+)', clean, "")

        zendesk_link = ""
        zd_raw = _extract_field(r'Zendesk Ticket Link:\s*(.+)', clean, "")
        if zd_raw:
            zendesk_link = _strip_slack_url(zd_raw)

        sf_link = ""
        sf_raw = _extract_field(r'Salesforce Account Link:\s*(.+)', clean, "")
        if sf_raw:
            sf_link = _strip_slack_url(sf_raw)

        if not user_email:
            logger.warning(
                "AM Escalation alert (ts=%s): no user email, skipping", ts
            )
            return

        # Try to get first name from the email local part
        local = user_email.split("@")[0]
        first_name = local.split(".")[0].capitalize() if "." in local else "there"

        # ── Build email ───────────────────────────────────────────────────
        context = _clean_escalation_context(ticket_desc, escalation_reason)
        html_body = am_escalation_email(
            first_name=first_name,
            escalation_context=context,
            user_id=user_id,
        )

        subject = "Ramp AM Intro"
        draft_id = f"escalation_{ts}"
        draft_method, _ = _create_or_queue_draft(
            draft_id=draft_id, to=user_email, subject=subject,
            html_body=html_body, account_name=company_name,
            label="Claude Drafts/AM Escalation", user_id=user_id,
        )

        # ── DM user ───────────────────────────────────────────────────────
        details = (
            f"*To:* {user_email}\n"
            f"*Subject:* {subject}\n"
            f"*Company:* {company_name}\n"
            f"*Escalation:* {escalation_reason or ticket_desc or 'N/A'}"
        )
        if zendesk_link:
            details += f"\n<{zendesk_link}|Zendesk Ticket>"
        if sf_link:
            details += f"  |  <{sf_link}|Salesforce>"

        blocks = drafter_confirmation_blocks(
            drafter_type="AM Escalation",
            account_name=company_name,
            details=details,
            draft_id=draft_id,
        )
        _dm_greg(client, blocks, user_id=user_id)

        logger.info(
            "AM Escalation draft created (%s) — company=%s to=%s draft=%s",
            draft_method, company_name, user_email, draft_id,
        )

    except Exception as e:
        logger.exception("Error handling AM Escalation alert (ts=%s): %s", ts, e)
        try:
            _dm_greg(
                client,
                drafter_confirmation_blocks(
                    drafter_type="AM Escalation",
                    account_name="ERROR",
                    details=f"Failed to create draft:\n```{e}```",
                ),
                user_id=user_id,
            )
        except Exception:
            logger.error("Could not DM about AM Escalation failure")
