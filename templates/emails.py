"""Email templates for all automated drafts.

Each template function returns a complete HTML string ready for Gmail draft
creation. All templates include the claude-auto-draft HTML comment tracker,
consistent wrapper styling, and the user's email signature.

Templates accept optional user_id, booking_link, and owner_name parameters
for per-user customization. When not provided, falls back to config defaults.
"""
from __future__ import annotations

from typing import Optional

from config import BOOKING_LINK as _DEFAULT_BOOKING_LINK, OWNER_FIRST_NAME
from templates.signature import SIGNATURE_HTML, build_signature

_WRAPPER_OPEN = '<div style="font-family:Arial,sans-serif;font-size:14px;color:#000;max-width:600px;">'
_WRAPPER_CLOSE = "</div>"
_TRACKER = '<!-- claude-auto-draft -->'


def _resolve_user_params(
    user_id: Optional[str] = None,
    booking_link: Optional[str] = None,
    owner_name: Optional[str] = None,
) -> tuple:
    """Resolve booking_link and owner_name from user_id or defaults.

    Returns (booking_link, first_name, signature_html).
    """
    sig_booking = booking_link
    sig_name = owner_name

    if user_id:
        try:
            from core.user_registry import get_user_booking_link, get_user_first_name, get_user
            user = get_user(user_id)
            if user:
                if not sig_booking:
                    sig_booking = get_user_booking_link(user_id)
                if not sig_name:
                    sig_name = get_user_first_name(user_id)
        except Exception:
            pass

    if not sig_booking:
        sig_booking = _DEFAULT_BOOKING_LINK or ""
    if not sig_name:
        sig_name = OWNER_FIRST_NAME

    sig_html = build_signature(user_id=user_id) if user_id else SIGNATURE_HTML

    return sig_booking, sig_name, sig_html


def _wrap(body_html: str, signature_html: Optional[str] = None) -> str:
    """Wrap body HTML in the standard email shell with tracker and signature."""
    sig = signature_html or SIGNATURE_HTML
    return (
        f"{_WRAPPER_OPEN}\n"
        f"  {_TRACKER}\n"
        f"  {body_html}\n"
        f"  {sig}\n"
        f"{_WRAPPER_CLOSE}"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 1. ACH-to-Card
# ═══════════════════════════════════════════════════════════════════════════════

def ach_to_card_email(
    greeting: str,
    vendor_name: str,
    invoice_value: str,
    due_date: str,
    has_payment_portal: bool,
    payment_portal_link: str = "",
    cashback_formatted: str = "",
    user_id: Optional[str] = None,
    booking_link: Optional[str] = None,
    owner_name: Optional[str] = None,
) -> str:
    """Draft for converting an ACH/wire/check bill to card payment.

    Concise, cashback-forward, with step-by-step card payment instructions
    and inline payment portal link when available.
    """
    _booking, _name, _sig = _resolve_user_params(user_id, booking_link, owner_name)

    if has_payment_portal and payment_portal_link:
        step_2 = (
            f'2. Use the card to pay the bill inside the vendor\'s payment portal '
            f'<a href="{payment_portal_link}" style="color:#1155CC;">here</a>.'
        )
    else:
        step_2 = "2. Use the card to pay the bill with the vendor."

    cashback_note = ""
    if cashback_formatted:
        cashback_note = (
            f" and this would net you ~<strong>{cashback_formatted} in cashback</strong>:"
        )
    else:
        cashback_note = " and this would net you cashback:"

    body = f"""\
<p>{greeting}</p>
<p>I was just notified of the bill in your drafts to <strong>{vendor_name}</strong> \
for <strong>{invoice_value}</strong> due on <strong>{due_date}</strong>. \
This vendor has been flagged as one that will typically accept credit card payments \
without fees{cashback_note}</p>
<p>
1. Edit the bill, change payment method to Ramp card, and select an existing card \
to use or create a single use card. If using a single use card — once the bill is \
approved, you'll be able to see its card number.<br>
{step_2}<br>
3. Once the bill is both approved in Ramp and paid, you can search for and match \
the relevant transaction to it. Once the transaction is matched, Ramp will mark \
the bill as paid in both Ramp and your accounting provider.
</p>
<p>Here's a <a href="https://support.ramp.com/hc/en-us/articles/28105415406867-Pay-Bill-Pay-invoices-via-Ramp-card" \
style="color:#1155CC;">handy guide</a> in case you want to pay this or other vendors \
in the future.</p>
<p>Can you please let me know if you plan to pay this with a card, if you need your \
limit increased, or if it'd be helpful to walk through it together?</p>
<p>Best,<br>{_name}</p>"""

    return _wrap(body, _sig)


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Procurement Trial
# ═══════════════════════════════════════════════════════════════════════════════

def procurement_trial_email(
    greeting: str,
    user_id: Optional[str] = None,
    booking_link: Optional[str] = None,
    owner_name: Optional[str] = None,
) -> str:
    """Draft for new Procurement trial activation."""
    _booking, _name, _sig = _resolve_user_params(user_id, booking_link, owner_name)

    body = f"""\
<p>{greeting}</p>

<p>I noticed your Procurement trial was just activated -- congrats! Procurement
is one of the highest-impact products on the Ramp platform and I want to make
sure you get the most out of your trial period.</p>

<p>Here are <b>3 priorities</b> to focus on to get the most value out of your trial:</p>

<ol style="padding-left:20px;">
  <li style="margin-bottom:8px;"><b>Set up approval workflows</b> — Configure
  your approval chains so every purchase request routes to the right person.
  This is the foundation of Procurement and ensures nothing slips through
  without proper sign-off.</li>
  <li style="margin-bottom:8px;"><b>Connect vendor contracts</b> — Upload your
  existing vendor contracts so Ramp can track renewals, flag duplicate
  software, and surface savings opportunities automatically.</li>
  <li style="margin-bottom:8px;"><b>Run your first intake request</b> — Submit
  a real purchase request through the workflow to see the full end-to-end
  experience. This is the fastest way to understand how it will work for your
  team day-to-day.</li>
</ol>

<p>I'd love to walk through setup together and answer any questions. Feel free
to book time directly on my calendar here:
<a href="{_booking}" style="color:#1155CC;">{_booking}</a></p>

<p><b>Helpful Resources</b></p>
<ul style="padding-left:20px;">
  <li style="margin-bottom:6px;"><a href="https://support.ramp.com/hc/en-us/articles/37424276359443-Get-started-with-Procurement" style="color:#1155CC;">Getting Started</a></li>
  <li style="margin-bottom:6px;"><a href="https://support.ramp.com/hc/en-us/articles/49355243914387-Ramp-Procurement-Quick-Start-Guide" style="color:#1155CC;">Quick Start Guide</a></li>
  <li style="margin-bottom:6px;"><a href="https://support.ramp.com/hc/en-us/articles/49437597525907-Procurement-Implementation-Best-Practices-Guide" style="color:#1155CC;">Best Practices</a></li>
</ul>

<p>Best,<br>{_name}</p>"""

    return _wrap(body, _sig)


# ═══════════════════════════════════════════════════════════════════════════════
# 3. PCLIP (Program Credit Limit Increase)
# ═══════════════════════════════════════════════════════════════════════════════

def pclip_email(
    first_name: str,
    user_id: Optional[str] = None,
    booking_link: Optional[str] = None,
    owner_name: Optional[str] = None,
) -> str:
    """Draft for program credit limit increase notification."""
    _booking, _name, _sig = _resolve_user_params(user_id, booking_link, owner_name)

    body = f"""\
<p>Hi {first_name},</p>

<p>I wanted to introduce myself as your new Account Manager at Ramp! I saw a \
notification about your limit increase and wanted to share a few potential \
ideas to utilize the new limit:</p>

<ul style="padding-left:20px;">
  <li style="margin-bottom:6px;">Migrate any personal reimbursements to Ramp \
cards for control &amp; cashback</li>
  <li style="margin-bottom:6px;">Consolidate spend from other cards into Ramp \
for automation &amp; eliminate unnecessary systems/workflows</li>
  <li style="margin-bottom:6px;">Pay vendors via Ramp card instead of ACH/wire \
for cashback &amp; better terms</li>
</ul>

<p>If you want to <a href="{_booking}" style="color:#1155CC;">schedule a \
call</a>, I can run a vendor audit to see which vendors that you're paying \
via ACH or wire may accept card payments with no additional fees. We can also \
talk about some improvements to your Ramp setup to put you in a good position \
to scale and make sure you're using Ramp to its fullest potential.</p>

<p>Looking forward to hearing from you!</p>

<p>Thanks,<br>{_name}</p>"""

    return _wrap(body, _sig)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Large Decline — Case A (Velocity Limit)
# ═══════════════════════════════════════════════════════════════════════════════

def large_decline_case_a_email(
    first_name: str,
    vendor_name: str,
    amount_formatted: str,
    user_id: Optional[str] = None,
    booking_link: Optional[str] = None,
    owner_name: Optional[str] = None,
) -> str:
    """Draft for a large decline caused by a velocity (per-transaction) limit."""
    _booking, _name, _sig = _resolve_user_params(user_id, booking_link, owner_name)

    body = f"""\
<p>Hi {first_name},</p>

<p>I noticed a transaction for <b>{amount_formatted}</b> to <b>{vendor_name}</b>
was recently declined due to a velocity limit on the card. I want to help get
this resolved quickly so the payment can go through.</p>

<p>Here are <b>3 options</b> to fix this:</p>

<ol style="padding-left:20px;">
  <li style="margin-bottom:8px;"><b>Request a card limit increase</b> — If
  the card's transaction or daily limit is too low for this payment, you or
  your admin can request a limit increase directly in Ramp. I can also help
  push this through on my end.</li>
  <li style="margin-bottom:8px;"><b>Use a different card</b> — If you have
  another Ramp card with a higher limit or available balance, you can retry
  the payment with that card instead.</li>
  <li style="margin-bottom:8px;"><b>Contact me directly</b> — Reply to this
  email or book time on my calendar and I'll help troubleshoot and get the
  payment unblocked:
  <a href="{_booking}" style="color:#1155CC;">{_booking}</a></li>
</ol>

<p>Best,<br>{_name}</p>"""

    return _wrap(body, _sig)


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Fundraise
# ═══════════════════════════════════════════════════════════════════════════════

def fundraise_email(
    first_name: str,
    user_id: Optional[str] = None,
    booking_link: Optional[str] = None,
    owner_name: Optional[str] = None,
) -> str:
    """Draft for reaching out after a customer's recent funding round."""
    _booking, _name, _sig = _resolve_user_params(user_id, booking_link, owner_name)

    body = f"""\
<p>Hi {first_name},</p>

<p>Congrats on the recent funding news! We deeply appreciate your partnership \
with Ramp and want to make sure we're continuing to deliver for you.</p>

<p>You're probably getting a ton of congrats from people ultimately trying to \
get you to spend more money -- Ramp is here to help you figure out how to \
spend less.</p>

<ul style="padding-left:20px;">
  <li style="margin-bottom:6px;">Renewal alerts so nothing auto-renews without approval</li>
  <li style="margin-bottom:6px;">2%+ earn on idle cash</li>
  <li style="margin-bottom:6px;">Insights to extend your runway, not just track spend</li>
</ul>

<p>Open to setting up a call in the next week or two to chat through what's \
top of mind for your team and how Ramp can support? Feel free to select any \
time through <a href="{_booking}" style="color:#1155CC;">this link</a> \
or let me know when works for you, looking forward to it!</p>

<p>All the best,<br>{_name}</p>"""

    return _wrap(body, _sig)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Large Decline — Case B (Open-to-Buy / Insufficient Limit)
# ═══════════════════════════════════════════════════════════════════════════════

def large_decline_case_b_email(
    first_name: str,
    vendor_name: str,
    amount_formatted: str,
    available_limit_formatted: str,
    user_id: Optional[str] = None,
    booking_link: Optional[str] = None,
    owner_name: Optional[str] = None,
) -> str:
    """Draft for a large decline caused by insufficient open-to-buy balance."""
    _booking, _name, _sig = _resolve_user_params(user_id, booking_link, owner_name)

    body = f"""\
<p>Hi {first_name},</p>

<p>I noticed a transaction for <b>{amount_formatted}</b> to <b>{vendor_name}</b>
was recently declined because it exceeded the available balance on the card.
Your current available limit is <b>{available_limit_formatted}</b>. I'd like
to help get this sorted out.</p>

<p>To help me find the best solution, I have a few quick questions:</p>

<ol style="padding-left:20px;">
  <li style="margin-bottom:8px;"><b>What is the business purpose of this
  transaction?</b> — Understanding the context helps me determine the best
  path to getting the limit adjusted.</li>
  <li style="margin-bottom:8px;"><b>Is this a recurring need?</b> — If you
  expect similar transactions in the future, we can set up a card or limit
  structure that accommodates this ongoing.</li>
  <li style="margin-bottom:8px;"><b>What limit would work for you?</b> — Let
  me know the amount you need and I can work on getting the limit increased
  so this doesn't happen again.</li>
</ol>

<p>Feel free to reply here or book time on my calendar so we can get this
resolved:
<a href="{_booking}" style="color:#1155CC;">{_booking}</a></p>

<p>Best,<br>{_name}</p>"""

    return _wrap(body, _sig)


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Automatic Card Loss
# ═══════════════════════════════════════════════════════════════════════════════

def auto_card_loss_email(
    first_name: str,
    vendor_name: str,
    estimated_cashback: str,
    view_bill_link: str,
    user_id: Optional[str] = None,
    booking_link: Optional[str] = None,
    owner_name: Optional[str] = None,
) -> str:
    """Draft for when a customer ignores the automatic card payment nudge."""
    _booking, _name, _sig = _resolve_user_params(user_id, booking_link, owner_name)

    # Resolve full name + title for sign-off
    rep_title = "Growth Account Manager"
    if user_id:
        try:
            from core.user_registry import get_user
            user = get_user(user_id)
            if user and user.get("title"):
                rep_title = user["title"]
        except Exception:
            pass

    body = f"""\
<p>Hi {first_name},</p>

<p>Flagging a quick save: a recent {vendor_name} invoice was eligible for fee-free card payment. \
Ramp surfaced a prompt to pay by card, but card was not selected. Paying by card on this invoice \
would have earned approximately <strong>${estimated_cashback} in cashback</strong> and can extend \
cash flow until the card statement due date.</p>

<ul><li>Here's the bill in Ramp: <a href="{view_bill_link}" style="color:#1155CC;">View Bill</a></li></ul>

<p>We'd like to understand why card payment wasn't selected and how we can better support \
your workflow. Would you be willing to share what influenced your decision? A quick number \
reply works great:</p>

<ol>
<li>I didn't see the prompt to switch bill payment method to card</li>
<li>I wasn't sure how bill payments via credit card worked</li>
<li>I wasn't sure vendor accepts card without fees</li>
<li>I prefer ACH for this vendor or payment type</li>
<li>Earning cashback wasn't a priority for this payment</li>
<li>Other</li>
</ol>

<p>If helpful, I can point you to where to switch the payment method to \
"Pay by card" in the app or walk through it live. Just let me know.</p>

<p>Thank you,</p>

<p>{_name}<br>{rep_title}</p>"""

    return _wrap(body, _sig)


# ═══════════════════════════════════════════════════════════════════════════════
# 8. RCLIP (Reactive Credit Limit Increase)
# ═══════════════════════════════════════════════════════════════════════════════

def rclip_email(
    first_name: str,
    user_id: Optional[str] = None,
    booking_link: Optional[str] = None,
    owner_name: Optional[str] = None,
) -> str:
    """Draft for reactive credit limit increase — AM intro + ideas to use new limit."""
    _booking, _name, _sig = _resolve_user_params(user_id, booking_link, owner_name)

    body = f"""\
<p>Hi {first_name},</p>

<p>I wanted to introduce myself as your Account Manager at Ramp! I saw your \
limit increase just went through and wanted to share a few ideas to make the \
most of it:</p>

<ul style="padding-left:20px;">
  <li style="margin-bottom:6px;">Migrate any personal reimbursements to Ramp \
cards for control &amp; cashback</li>
  <li style="margin-bottom:6px;">Consolidate spend from other cards into Ramp \
for automation &amp; eliminate unnecessary systems/workflows</li>
  <li style="margin-bottom:6px;">Pay vendors via Ramp card instead of ACH/wire \
for cashback &amp; better terms</li>
</ul>

<p>If you want to <a href="{_booking}" style="color:#1155CC;">schedule a \
call</a>, I can run a vendor audit to see which vendors that you're paying \
via ACH or wire may accept card payments with no additional fees. We can also \
talk about some improvements to your Ramp setup to put you in a good position \
to scale and make sure you're using Ramp to its fullest potential.</p>

<p>Looking forward to hearing from you!</p>

<p>Thanks,<br>{_name}</p>"""

    return _wrap(body, _sig)


# ═══════════════════════════════════════════════════════════════════════════════
# 9. AM Escalation
# ═══════════════════════════════════════════════════════════════════════════════

def am_escalation_email(
    first_name: str,
    escalation_context: str,
    user_id: Optional[str] = None,
    booking_link: Optional[str] = None,
    owner_name: Optional[str] = None,
) -> str:
    """Draft for CX-to-AM escalation — contextual intro based on ticket."""
    _booking, _name, _sig = _resolve_user_params(user_id, booking_link, owner_name)

    body = f"""\
<p>Hi{' ' + first_name if first_name and first_name != 'there' else ''}, \
great to meet you — our support team let me know {escalation_context}. \
Could you <a href="{_booking}" style="color:#1155CC;">select a time through \
this link</a> and we'll chat through it?</p>

<p>Thanks!</p>

<p>{_name}</p>"""

    return _wrap(body, _sig)
