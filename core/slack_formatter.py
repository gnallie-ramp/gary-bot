"""Slack Block Kit message builders for DM alerts and summaries."""
from __future__ import annotations

import urllib.parse

from config import SF_BASE_URL, DASHBOARD_BASE_URL, COMMAND_PREFIX


# ── URL Helpers ───────────────────────────────────────────────────────────────


def sf_account_url(account_id):
    """Return a Salesforce Lightning URL for an Account record."""
    return f"{SF_BASE_URL}/r/Account/{account_id}/view"


def sf_opp_url(opp_id):
    """Return a Salesforce Lightning URL for an Opportunity record."""
    return f"{SF_BASE_URL}/r/Opportunity/{opp_id}/view"


# ── Salesforce Opportunity Field Mappings ─────────────────────────────────────

EXPANSION_TYPE_MAP = {
    "Card Expansion": "New Card Programs",
    "Bill Pay Expansion": "Bill Pay",
    "Treasury Expansion": "Treasury",
    "Travel Expansion": "Travel",
    "SaaS": "Ramp Plus - Contract",
    "Procurement": "Procurement",
}

EXPANSION_PRODUCT_MAP = {
    "Card Expansion": "Card",
    "Bill Pay Expansion": "Bill Pay",
    "Treasury Expansion": "Treasury",
    "Travel Expansion": "Travel",
    "SaaS": "SaaS",
    "SaaS Add-On": "SaaS Add-On",
    "Procurement": "Procurement",
}

SF_CUSTOM_FIELDS = {
    "expansion_type":     "Expansion_Type__c",
    "expansion_motion":   "Expansion_Motion__c",
    "expansion_product":  "Expansion_Product__c",
    "expansion_source":   "Expansion_Source__c",
    "next_step_due_date": "Next_Step_Due_Date__c",
    "expansion_notes":    "Expansion_Notes__c",
    "card_amount":        "Expansion_Amount__c",
    "saas_amount":        "SaaS_Expansion_Amount__c",
    "billpay_amount":     "Bill_Pay_Expansion_Amount__c",
    "treasury_amount":    "RBA_Amount_Committed__c",
    "gong_outreach":      "Gong_Outreach_Link__c",
    "travel_amount":      "Monthly_Travel_Bookings_Amount__c",
}

# Expansion record type ID in Salesforce
_EXPANSION_RECORD_TYPE_ID = "0125b000000PZaIAAW"


def _sf_default_encode(value):
    """URL-encode a value for Salesforce defaultFieldValues parameter."""
    return urllib.parse.quote(str(value), safe="")


def build_sf_new_opp_url(
    account_name: str,
    account_id: str,
    product_type: str,
    amount: float = 0,
    close_date: str = "",
    stage: str = "S2: Sales Qualified Opportunity",
    next_step: str = "",
    expansion_notes: str = "",
    next_step_due_date: str = "",
    gong_link: str = "",
):
    """Build a Salesforce Lightning URL that pre-fills a new Expansion Opportunity.

    Returns a URL that, when clicked, opens the SF new opportunity form
    with all fields pre-populated.
    """
    from datetime import datetime, timedelta

    subtype = EXPANSION_PRODUCT_MAP.get(product_type, product_type)
    opp_name = f"{account_name} - {subtype}"

    if not close_date:
        now = datetime.utcnow()
        eom = (now.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        close_date = eom.strftime("%Y-%m-%d")
    if not next_step_due_date:
        next_step_due_date = (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%d")

    fields = {}
    if account_id:
        fields["AccountId"] = str(account_id)
    fields["Name"] = opp_name
    fields["StageName"] = stage
    fields["CloseDate"] = close_date
    if next_step:
        fields["NextStep"] = next_step[:120]

    custom_vals = {
        "expansion_type": EXPANSION_TYPE_MAP.get(product_type, ""),
        "expansion_motion": "0 to 1 Upsell",
        "expansion_product": EXPANSION_PRODUCT_MAP.get(product_type, product_type),
        "expansion_source": "Meeting - Other",
        "next_step_due_date": next_step_due_date,
    }
    if expansion_notes:
        custom_vals["expansion_notes"] = expansion_notes[:200]
    if gong_link:
        custom_vals["gong_outreach"] = gong_link
    if amount and amount > 0:
        amt_str = str(int(amount))
        if product_type == "Card Expansion":
            custom_vals["card_amount"] = amt_str
        elif product_type == "SaaS":
            custom_vals["saas_amount"] = amt_str
        elif product_type == "Bill Pay Expansion":
            custom_vals["billpay_amount"] = amt_str
        elif product_type == "Treasury Expansion":
            custom_vals["treasury_amount"] = amt_str
        elif product_type == "Travel Expansion":
            custom_vals["travel_amount"] = amt_str

    for key, api_name in SF_CUSTOM_FIELDS.items():
        val = custom_vals.get(key, "")
        if val:
            fields[api_name] = val

    params = ",".join(f"{k}={_sf_default_encode(v)}" for k, v in fields.items())
    base = SF_BASE_URL.replace("/lightning", "")
    return f"{base}/lightning/o/Opportunity/new?defaultFieldValues={params}&recordTypeId={_EXPANSION_RECORD_TYPE_ID}"


def opp_fields_summary(product_type: str, amount: float = 0, close_date: str = "", l30d: float = 0):
    """Return a compact markdown string showing pre-computed opp field values."""
    from datetime import datetime, timedelta

    exp_type = EXPANSION_TYPE_MAP.get(product_type, product_type)
    subtype = EXPANSION_PRODUCT_MAP.get(product_type, product_type)

    if not close_date:
        now = datetime.utcnow()
        eom = (now.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        close_date = eom.strftime("%Y-%m-%d")

    lines = [
        f"`Product:` {subtype}",
        f"`Type:` {exp_type}",
        f"`Motion:` 0 to 1 Upsell",
        f"`Source:` Meeting - Other",
        f"`Stage:` S2: Sales Qualified Opportunity",
        f"`Close Date:` {close_date}",
    ]
    if amount and amount > 0:
        lines.append(f"`Amount:` {format_currency(amount)}")
    if l30d and l30d > 0:
        lines.append(f"`L30D (baseline at close):` {format_currency(l30d)}")

    return "\n".join(lines)


def dashboard_url(page, tab=None, account=None):
    """Return a Streamlit dashboard deep-link URL.

    Parameters
    ----------
    page : str
        Page key: "home", "brief", "priority", "pipeline", "meeting-prep",
        "prospecting", "post-meeting", "performance", "book-carves", "alerts".
    tab : str, optional
        Tab name within the page (passed as query param).
    account : str, optional
        Account name for meeting-prep lookups.
    """
    params = {"page": page}
    if tab:
        params["tab"] = tab
    if account:
        params["account"] = account
    return f"{DASHBOARD_BASE_URL}/?{urllib.parse.urlencode(params)}"


# ── Currency Formatting ──────────────────────────────────────────────────────


def format_currency(amount):
    """Format a numeric amount as a compact dollar string.

    Examples
    --------
    >>> format_currency(14200)
    '$14,200'
    >>> format_currency(1_200_000)
    '$1.2M'
    >>> format_currency(2_500_000)
    '$2.5M'
    >>> format_currency(0)
    '$0'
    """
    if amount is None:
        return "$0"
    amount = float(amount)
    if abs(amount) >= 1_000_000:
        formatted = f"${amount / 1_000_000:.1f}M"
        # Strip trailing .0 for clean millions (e.g. $2.0M → $2M)
        formatted = formatted.replace(".0M", "M")
        return formatted
    return f"${amount:,.0f}"


# ── Block Kit Builders ───────────────────────────────────────────────────────


def drafter_confirmation_blocks(drafter_type, account_name, details, draft_id=None):
    """Build Block Kit blocks for an email-draft DM confirmation.

    Parameters
    ----------
    drafter_type : str
        The type of drafter that created the email (e.g. "ACH-to-Card",
        "Follow-Up", "Re-Engagement").
    account_name : str
        The account name for the header.
    details : str
        Markdown-formatted detail text (recipient, subject, context, etc.).
    draft_id : str, optional
        Gmail draft ID.  When provided an "Open Draft" button is included.

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
                "text": f"\u2709\ufe0f {drafter_type} Draft — {account_name}"[:150],
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": details,
            },
        },
    ]

    actions = []
    if draft_id:
        actions.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Open Draft", "emoji": True},
                "url": f"https://mail.google.com/mail/u/0/#drafts?compose={draft_id}",
                "action_id": "open_draft_action",
                "style": "primary",
            }
        )
    actions.append(
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Dismiss", "emoji": True},
            "action_id": "dismiss_action",
        }
    )

    blocks.append({"type": "actions", "elements": actions})
    return blocks


def opp_pacing_blocks(close_today, pacing_exceed, zero_to_one):
    """Build Block Kit blocks for a morning opportunity pacing alert.

    Parameters
    ----------
    close_today : list[dict]
        Opps that should be closed today. Each dict should have keys like
        ``account_name``, ``opp_id``, ``expansion_subtype``, ``cp_value``.
    pacing_exceed : list[dict]
        Opps pacing to exceed baseline this month.
    zero_to_one : list[dict]
        Zero-to-one product activation signals.

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
                "text": "\U0001f4c8 Morning Opp Pacing Alert",
                "emoji": True,
            },
        },
    ]

    # Close Today section
    if close_today:
        lines = [f"*\U0001f534 Close Today* ({len(close_today)} opps)"]
        for opp in close_today:
            name = opp.get("account", "Unknown")
            product = opp.get("product", "")
            est_cp = opp.get("est_cp", "$0")
            sf_link = opp.get("sf_link", "")
            over = opp.get("over_baseline", "$0")
            link = f"<{sf_link}|{name}>" if sf_link else name
            lines.append(f"  \u2022 {link} — {product} — {over} over baseline — {est_cp} CP")
        blocks.append(
            {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}
        )
    else:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\u2705 No opps need immediate close today."},
            }
        )

    blocks.append({"type": "divider"})

    # Pacing to Exceed section
    if pacing_exceed:
        lines = [f"*\U0001f7e1 Pacing to Exceed Baseline* ({len(pacing_exceed)} opps)"]
        for opp in pacing_exceed:
            name = opp.get("account", "Unknown")
            product = opp.get("product", "")
            est_cp = opp.get("est_cp", "$0")
            sf_link = opp.get("sf_link", "")
            over = opp.get("over_baseline", "$0")
            days_left = opp.get("days_left", 0)
            link = f"<{sf_link}|{name}>" if sf_link else name
            lines.append(f"  \u2022 {link} — {product} — {over} over — {est_cp} CP — {days_left}d left")
        blocks.append(
            {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}
        )
    else:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\U0001f7e2 No opps pacing to exceed baseline."},
            }
        )

    blocks.append({"type": "divider"})

    # Zero-to-One section
    if zero_to_one:
        lines = [f"*\u26a1 Zero-to-One Signals* ({len(zero_to_one)} accounts)"]
        for sig in zero_to_one:
            name = sig.get("account", "Unknown")
            product = sig.get("product", "")
            activated = sig.get("activated_at", "")
            spend = sig.get("l30d_spend", "$0")
            sf_link = sig.get("sf_link", "")
            link = f"<{sf_link}|{name}>" if sf_link else name
            lines.append(f"  \u2022 {link} — {product} activated {activated} — L30D: {spend}")
        blocks.append(
            {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}
        )
    else:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "No new zero-to-one signals."},
            }
        )

    # Dashboard links footer
    _pipeline_url = dashboard_url("pipeline")
    _priority_url = dashboard_url("priority")
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"*Dashboard:* <{_pipeline_url}|Pipeline> · "
                f"<{_priority_url}|Priority Actions> · "
                f"`/{COMMAND_PREFIX}-opp-pacing` to refresh"
            ),
        }],
    })

    return blocks


def quota_heartbeat_blocks(summary_data):
    """Build Block Kit blocks for an end-of-day quota heartbeat summary.

    Parameters
    ----------
    summary_data : dict
        Summary data with keys like ``attainment_pct``, ``realized_cp``,
        ``quota``, ``opps_closed_today``, ``opps_open``, ``pipeline_cp``,
        ``days_remaining``, ``run_rate``, ``accelerator_band``.

    Returns
    -------
    list[dict]
        Slack Block Kit blocks.
    """
    total_cp = summary_data.get("total_cp", "$0")
    locked_cp = summary_data.get("locked_cp", "$0")
    accruing_cp = summary_data.get("accruing_cp", "$0")
    attainment = summary_data.get("attainment_pct", "0%")
    band = summary_data.get("band", "1.0x")
    by_product = summary_data.get("by_product", {})
    top_movers = summary_data.get("top_movers", [])
    opp_count = summary_data.get("opp_count", 0)

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "\U0001f4ca EOD Quota Heartbeat",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Total CP:* {total_cp}"},
                {"type": "mrkdwn", "text": f"*Attainment:* {attainment}"},
                {"type": "mrkdwn", "text": f"*Locked CP:* {locked_cp}"},
                {"type": "mrkdwn", "text": f"*Accruing CP:* {accruing_cp}"},
                {"type": "mrkdwn", "text": f"*Accelerator:* {band}"},
                {"type": "mrkdwn", "text": f"*Active Opps:* {opp_count}"},
            ],
        },
    ]

    # Product breakdown
    if by_product:
        product_lines = ["*CP by Product:*"]
        for product, cp_val in by_product.items():
            product_lines.append(f"  {product}: {cp_val}")
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(product_lines)},
        })

    # Top movers
    if top_movers:
        mover_lines = ["*Top Movers:*"]
        for m in top_movers:
            mover_lines.append(f"  {m.get('account', '')} — {m.get('product', '')} — {m.get('cp', '')} ({m.get('status', '')})")
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(mover_lines)},
        })

    # Dashboard links footer
    _perf_url = dashboard_url("performance")
    _pipeline_url = dashboard_url("pipeline")
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"*Dashboard:* <{_perf_url}|Performance> · "
                f"<{_pipeline_url}|Pipeline>"
            ),
        }],
    })

    return blocks


def simple_dm_blocks(title, body_mrkdwn):
    """Build simple Block Kit blocks with a header and body section.

    Parameters
    ----------
    title : str
        Plain-text header title.
    body_mrkdwn : str
        Markdown-formatted body text.

    Returns
    -------
    list[dict]
        Slack Block Kit blocks.
    """
    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": title,
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": body_mrkdwn,
            },
        },
    ]
