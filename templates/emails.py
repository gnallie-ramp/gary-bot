"""Email templates for all automated drafts.

Each template function returns a complete HTML string ready for Gmail draft
creation. All templates include the claude-auto-draft HTML comment tracker,
consistent wrapper styling, and Greg's email signature.
"""

from templates.signature import SIGNATURE_HTML

BOOKING_LINK = "https://ramp-com.chilipiper.com/me/gregory-nallie/ramp"

_WRAPPER_OPEN = '<div style="font-family:Arial,sans-serif;font-size:14px;color:#000;max-width:600px;">'
_WRAPPER_CLOSE = "</div>"
_TRACKER = '<!-- claude-auto-draft -->'


def _wrap(body_html: str) -> str:
    """Wrap body HTML in the standard email shell with tracker and signature."""
    return (
        f"{_WRAPPER_OPEN}\n"
        f"  {_TRACKER}\n"
        f"  {body_html}\n"
        f"  {SIGNATURE_HTML}\n"
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
) -> str:
    """Draft for converting an ACH/wire/check bill to card payment.

    Concise, cashback-forward, with step-by-step card payment instructions
    and inline payment portal link when available.
    """

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
<p>Best,<br>Greg</p>"""

    return _wrap(body)


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Procurement Trial
# ═══════════════════════════════════════════════════════════════════════════════

def procurement_trial_email(greeting: str) -> str:
    """Draft for new Procurement trial activation."""

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
<a href="{BOOKING_LINK}" style="color:#1155CC;">{BOOKING_LINK}</a></p>

<p><b>Helpful Resources</b></p>
<ul style="padding-left:20px;">
  <li style="margin-bottom:6px;"><a href="https://support.ramp.com/hc/en-us/articles/37424276359443-Get-started-with-Procurement" style="color:#1155CC;">Getting Started</a></li>
  <li style="margin-bottom:6px;"><a href="https://support.ramp.com/hc/en-us/articles/49355243914387-Ramp-Procurement-Quick-Start-Guide" style="color:#1155CC;">Quick Start Guide</a></li>
  <li style="margin-bottom:6px;"><a href="https://support.ramp.com/hc/en-us/articles/49437597525907-Procurement-Implementation-Best-Practices-Guide" style="color:#1155CC;">Best Practices</a></li>
</ul>

<p>Best,<br>Greg</p>"""

    return _wrap(body)


# ═══════════════════════════════════════════════════════════════════════════════
# 3. PCLIP (Program Credit Limit Increase)
# ═══════════════════════════════════════════════════════════════════════════════

def pclip_email(
    first_name: str,
) -> str:
    """Draft for program credit limit increase notification."""

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

<p>If you want to <a href="{BOOKING_LINK}" style="color:#1155CC;">schedule a \
call</a>, I can run a vendor audit to see which vendors that you're paying \
via ACH or wire may accept card payments with no additional fees. We can also \
talk about some improvements to your Ramp setup to put you in a good position \
to scale and make sure you're using Ramp to its fullest potential.</p>

<p>Looking forward to hearing from you!</p>

<p>Thanks,<br>Greg</p>"""

    return _wrap(body)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Large Decline — Case A (Velocity Limit)
# ═══════════════════════════════════════════════════════════════════════════════

def large_decline_case_a_email(
    first_name: str,
    vendor_name: str,
    amount_formatted: str,
) -> str:
    """Draft for a large decline caused by a velocity (per-transaction) limit."""

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
  <a href="{BOOKING_LINK}" style="color:#1155CC;">{BOOKING_LINK}</a></li>
</ol>

<p>Best,<br>Greg</p>"""

    return _wrap(body)


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Fundraise
# ═══════════════════════════════════════════════════════════════════════════════

def fundraise_email(first_name: str) -> str:
    """Draft for reaching out after a customer's recent funding round."""

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
time through <a href="{BOOKING_LINK}" style="color:#1155CC;">this link</a> \
or let me know when works for you, looking forward to it!</p>

<p>All the best,<br>Greg</p>"""

    return _wrap(body)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Large Decline — Case B (Open-to-Buy / Insufficient Limit)
# ═══════════════════════════════════════════════════════════════════════════════

def large_decline_case_b_email(
    first_name: str,
    vendor_name: str,
    amount_formatted: str,
    available_limit_formatted: str,
) -> str:
    """Draft for a large decline caused by insufficient open-to-buy balance."""

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
<a href="{BOOKING_LINK}" style="color:#1155CC;">{BOOKING_LINK}</a></p>

<p>Best,<br>Greg</p>"""

    return _wrap(body)
