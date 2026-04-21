"""Button/action handlers for interactive Slack messages.

Handles:
  - Priority Actions Level 2 drill-down buttons (show_close_now, etc.)
  - Draft Outreach Email button (smart drafter for any category)
  - Draft Re-engage Email button (legacy, redirects to smart drafter)
  - Dismiss / Open Draft / Prep buttons
"""

import json
import logging
import re
import threading
import time

# Limit concurrent draft threads to prevent Snowflake connection overload
_draft_semaphore = threading.Semaphore(6)

from config import GREG_SLACK_ID, COMMAND_PREFIX
from core.user_registry import get_user_sf_name, get_user_first_name, get_user_booking_link, get_user_email

logger = logging.getLogger(__name__)

_WIN_REASON_PRODUCT = {
    "Card Expansion": "card payments",
    "Bill Pay Expansion": "bill pay",
    "Treasury Expansion": "treasury/cash management",
    "Travel Expansion": "travel bookings",
    "SaaS Expansion": "SaaS management",
    "Procurement Expansion": "procurement",
}


def register_interactive_handlers(app):
    """Register button and action handlers."""

    # ── Generic buttons ──────────────────────────────────────────────

    @app.action("dismiss_action")
    def handle_dismiss(ack, body, client):
        """Handle Dismiss button — delete the message."""
        ack()
        try:
            channel = body["channel"]["id"]
            ts = body["message"]["ts"]
            client.chat_delete(channel=channel, ts=ts)
        except Exception as e:
            logger.warning("Failed to dismiss message: %s", e)

    @app.action("open_draft_action")
    def handle_open_draft(ack, body):
        """Handle Open Draft button — no-op, link opens in browser."""
        ack()

    @app.action({"action_id": re.compile(r"^prep_")})
    def handle_prep_link(ack, body):
        """Handle Prep button — no-op, URL opens in browser."""
        ack()

    @app.action({"action_id": re.compile(r"^create_gmail_draft_")})
    def handle_create_gmail_draft(ack, body, client):
        """Handle Create Gmail Draft button — writes draft to /tmp for Glass to pick up."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        action = body.get("actions", [{}])[0]
        value = action.get("value", "{}")
        try:
            payload = json.loads(value)
            draft_id = payload.get("draft_id", "")
        except (json.JSONDecodeError, AttributeError):
            draft_id = ""

        if not draft_id:
            return

        def _run():
            try:
                from utils.pending_drafts import get_draft
                draft = get_draft(draft_id)
                if not draft:
                    client.chat_postEphemeral(
                        channel=body["channel"]["id"],
                        user=user_id,
                        text=f"Draft {draft_id} not found in pending drafts.",
                    )
                    return

                if draft.get("status") == "sent":
                    client.chat_postEphemeral(
                        channel=body["channel"]["id"],
                        user=user_id,
                        text="\u2705 Draft already created in Gmail.",
                    )
                    return

                # Write to /tmp for Glass to pick up on next cron tick (~1 min)
                import os
                tmp_path = f"/tmp/draft_{draft_id}.json"
                with open(tmp_path, "w") as f:
                    json.dump(draft, f, indent=2)

                client.chat_postEphemeral(
                    channel=body["channel"]["id"],
                    user=user_id,
                    text="\u2709\ufe0f Draft flagged for immediate creation — should appear in Gmail within ~1 min.",
                )
            except Exception as e:
                logger.error("Gmail draft button error: %s", e)

        threading.Thread(target=_run, daemon=True).start()

    @app.action({"action_id": re.compile(r"^create_opp_sfdc_")})
    def handle_create_opp_sfdc(ack, body, client):
        """Handle Create Opp button — creates opportunity in Salesforce via Growth MCP."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        action = body.get("actions", [{}])[0]
        value = action.get("value", "{}")
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            payload = {}

        account_name = payload.get("account_name", "Unknown")
        account_id = payload.get("account_id", "")
        product = payload.get("product", "")
        stage = payload.get("stage", "S2: Sales Qualified Opportunity")
        amount = payload.get("amount", 0)
        close_date = payload.get("close_date", "")
        rationale = payload.get("rationale", "")
        next_step = payload.get("next_step", "")
        next_step_due_date = payload.get("next_step_due_date", "")
        gong_link = payload.get("gong_link", "")

        if not account_id:
            client.chat_postMessage(
                channel=user_id,
                text=f"Cannot create opp for *{account_name}* — no SFDC account match.",
            )
            return

        client.chat_postMessage(
            channel=user_id,
            text=f"Creating *{product}* opp for *{account_name}*...",
        )

        def _run():
            try:
                from core.salesforce_client import create_opportunity
                from core.slack_formatter import (
                    sf_opp_url, EXPANSION_TYPE_MAP, EXPANSION_PRODUCT_MAP,
                    SF_CUSTOM_FIELDS,
                )
                from datetime import datetime, timedelta

                subtype = EXPANSION_PRODUCT_MAP.get(product, product)
                opp_name = f"{account_name} - {subtype}"

                if not close_date:
                    # Default: last day of current month
                    now = datetime.utcnow()
                    cd = ((now.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    cd = close_date

                nsd = next_step_due_date if next_step_due_date else (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%d")

                fields = {
                    "AccountId": account_id,
                    "Name": opp_name,
                    "StageName": stage,
                    "CloseDate": cd,
                    "RecordTypeId": "0125b000000PZaIAAW",
                    "Expansion_Type__c": EXPANSION_TYPE_MAP.get(product, ""),
                    "Expansion_Motion__c": "0 to 1 Upsell",
                    "Expansion_Product__c": subtype,
                    "Expansion_Source__c": "Meeting - Other",
                    "Next_Step_Due_Date__c": nsd,
                }
                if next_step:
                    fields["NextStep"] = next_step[:255]
                if rationale:
                    fields["Expansion_Notes__c"] = rationale[:200]
                if amount and float(amount) > 0:
                    amt_str = str(int(float(amount)))
                    if product == "Card Expansion":
                        fields["Expansion_Amount__c"] = amt_str
                    elif product == "Bill Pay Expansion":
                        fields["Bill_Pay_Expansion_Amount__c"] = amt_str
                    elif product == "Treasury Expansion":
                        fields["RBA_Amount_Committed__c"] = amt_str
                    elif product == "Travel Expansion":
                        fields["Monthly_Travel_Bookings_Amount__c"] = amt_str

                # Card + Bill Pay defaults: Timeframe of Spend = Monthly,
                # Primary Competitor = None, Win Reason = Single tech stack solution
                if product in ("Card Expansion", "Bill Pay Expansion"):
                    fields["Timeframe_of_Spend__c"] = "Monthly"
                    fields["Primary_Competitor__c"] = "None"
                    fields["Win_Reason__c"] = "Single tech stack solution"

                # Primary_Contact__c auto-defaults to Account.Main_POC__c in Growth MCP

                # Pre-fill Gong call URL — use payload value or fall back to Snowflake
                gong_url = gong_link
                if not gong_url and account_id:
                    try:
                        from core.salesforce_client import get_gong_call_url
                        gong_url = get_gong_call_url(account_id)
                    except Exception:
                        pass
                if gong_url and len(gong_url) <= 255:
                    fields["Gong_Outreach_Link__c"] = gong_url

                # Pre-fill WinReasonDetail__c
                product_label = _WIN_REASON_PRODUCT.get(product, product)
                win_detail = f"Migrating {product_label} into Ramp for consolidation"
                if rationale:
                    win_detail = f"{win_detail} — {rationale}"
                fields["WinReasonDetail__c"] = win_detail[:500]

                opp_id, err_msg = create_opportunity(fields)
                if opp_id:
                    opp_link = sf_opp_url(opp_id)
                    client.chat_postMessage(
                        channel=user_id,
                        blocks=[
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": (
                                        f"\u2705 *{product}* opp created for *{account_name}*\n"
                                        f"Stage: {stage} | Amount: ${int(float(amount)):,}/mo\n"
                                        f"<{opp_link}|View in Salesforce>"
                                    ),
                                },
                            }
                        ],
                        text=f"Opp created: {opp_name}",
                    )
                else:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"Failed to create *{product}* opp for *{account_name}*: {err_msg or 'unknown error'}",
                    )
            except Exception as e:
                logger.error("SFDC opp creation failed for %s: %s", account_name, e)
                client.chat_postMessage(
                    channel=user_id,
                    text=f"Failed to create opp: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.action("quick_create_opp_sfdc")
    def handle_quick_create_opp_sfdc(ack, body, client):
        """Handle Create Opp button from /opp slash command confirmation."""
        ack()

        action = body.get("actions", [{}])[0]
        value = action.get("value", "{}")
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            payload = {}

        account_name = payload.get("account_name", "Unknown")
        account_id = payload.get("account_id", "")
        product = payload.get("product", "")
        stage = payload.get("stage", "S2: Sales Qualified Opportunity")
        amount = payload.get("amount", 0)
        close_date = payload.get("close_date", "")
        next_step = payload.get("next_step", "")
        notes = payload.get("notes", "")
        gong_link = payload.get("gong_link", "")

        channel = body["channel"]["id"]
        message_ts = body["message"]["ts"]

        if not account_id:
            client.chat_update(
                channel=channel,
                ts=message_ts,
                blocks=[{
                    "type": "section",
                    "text": {"type": "mrkdwn",
                             "text": f":x: Cannot create opp for *{account_name}* — no SFDC account match."},
                }],
                text=f"Cannot create opp for {account_name}",
            )
            return

        # Update the message to show "Creating..." status
        client.chat_update(
            channel=channel,
            ts=message_ts,
            blocks=[{
                "type": "section",
                "text": {"type": "mrkdwn",
                         "text": f":hourglass_flowing_sand: Creating *{product}* opp for *{account_name}*..."},
            }],
            text=f"Creating {product} opp for {account_name}...",
        )

        def _run():
            try:
                from core.salesforce_client import create_opportunity
                from core.slack_formatter import (
                    sf_opp_url, sf_account_url, EXPANSION_TYPE_MAP,
                    EXPANSION_PRODUCT_MAP, format_currency,
                )
                from datetime import datetime, timedelta

                subtype = EXPANSION_PRODUCT_MAP.get(product, product)
                opp_name = f"{account_name} - {subtype}"

                if not close_date:
                    now = datetime.utcnow()
                    cd = ((now.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    cd = close_date

                nsd = (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%d")

                fields = {
                    "AccountId": account_id,
                    "Name": opp_name,
                    "StageName": stage,
                    "CloseDate": cd,
                    "RecordTypeId": "0125b000000PZaIAAW",
                    "Expansion_Type__c": EXPANSION_TYPE_MAP.get(product, ""),
                    "Expansion_Motion__c": "0 to 1 Upsell",
                    "Expansion_Product__c": subtype,
                    "Expansion_Source__c": "Meeting - Other",
                    "Next_Step_Due_Date__c": nsd,
                }
                if next_step:
                    fields["NextStep"] = next_step[:255]
                if notes:
                    fields["Expansion_Notes__c"] = notes[:200]
                # Pre-fill Gong call URL — use payload value or fall back to Snowflake
                gong_url = gong_link
                if not gong_url and account_id:
                    try:
                        from core.salesforce_client import get_gong_call_url
                        gong_url = get_gong_call_url(account_id)
                    except Exception:
                        pass
                if gong_url and len(gong_url) <= 255:
                    fields["Gong_Outreach_Link__c"] = gong_url

                # Pre-fill WinReasonDetail__c
                product_label = _WIN_REASON_PRODUCT.get(product, product)
                win_detail = f"Migrating {product_label} into Ramp for consolidation"
                if notes:
                    win_detail = f"{win_detail} — {notes}"
                fields["WinReasonDetail__c"] = win_detail[:500]

                # Card + Bill Pay defaults: Timeframe of Spend = Monthly,
                # Primary Competitor = None, Win Reason = Single tech stack solution.
                # Applied via the follow-up update_opportunities call.
                if product in ("Card Expansion", "Bill Pay Expansion"):
                    fields["Timeframe_of_Spend__c"] = "Monthly"
                    fields["Primary_Competitor__c"] = "None"
                    fields["Win_Reason__c"] = "Single tech stack solution"

                if amount and float(amount) > 0:
                    amt_str = str(int(float(amount)))
                    if product == "Card Expansion":
                        fields["Expansion_Amount__c"] = amt_str
                    elif product == "Bill Pay Expansion":
                        fields["Bill_Pay_Expansion_Amount__c"] = amt_str
                    elif product == "Treasury Expansion":
                        fields["RBA_Amount_Committed__c"] = amt_str
                    elif product == "Travel Expansion":
                        fields["Monthly_Travel_Bookings_Amount__c"] = amt_str

                # Primary_Contact__c auto-resolved by create_opportunity when not set

                opp_id, err_msg = create_opportunity(fields)
                if opp_id:
                    opp_link = sf_opp_url(opp_id)
                    sf_acct = sf_account_url(account_id)
                    amt_display = f" | Amount: {format_currency(float(amount))}/mo" if amount else ""
                    client.chat_update(
                        channel=channel,
                        ts=message_ts,
                        blocks=[
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": (
                                        f":white_check_mark: *{product}* opp created for *<{sf_acct}|{account_name}>*\n"
                                        f"Stage: {stage}{amt_display}\n"
                                        f"<{opp_link}|View in Salesforce>"
                                    ),
                                },
                            }
                        ],
                        text=f"Opp created: {opp_name}",
                    )
                else:
                    client.chat_update(
                        channel=channel,
                        ts=message_ts,
                        blocks=[{
                            "type": "section",
                            "text": {"type": "mrkdwn",
                                     "text": f":x: Failed to create *{product}* opp for *{account_name}*: {err_msg or 'unknown error'}"},
                        }],
                        text=f"Failed to create opp for {account_name}",
                    )
            except Exception as e:
                logger.error("Quick SFDC opp creation failed for %s: %s", account_name, e)
                client.chat_update(
                    channel=channel,
                    ts=message_ts,
                    blocks=[{
                        "type": "section",
                        "text": {"type": "mrkdwn",
                                 "text": f":x: Failed to create opp: {e}"},
                    }],
                    text=f"Failed to create opp: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.action({"action_id": re.compile(r"^update_opp_sfdc_")})
    def handle_update_opp_sfdc(ack, body, client):
        """Handle Apply Update button — updates existing opp fields in Salesforce."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        action = body.get("actions", [{}])[0]
        value = action.get("value", "{}")
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            payload = {}

        account_name = payload.get("account_name", "Unknown")
        account_id = payload.get("account_id", "")
        product = payload.get("product", "")
        field_updates = payload.get("field_updates", {})

        if not account_id or not field_updates:
            client.chat_postMessage(
                channel=user_id,
                text=f"Cannot update opp — missing account or fields.",
            )
            return

        client.chat_postMessage(
            channel=user_id,
            text=f"Updating *{product}* opp for *{account_name}*...",
        )

        def _run():
            try:
                from core.salesforce_client import query as sf_query, update_opportunity
                from core.slack_formatter import sf_opp_url, EXPANSION_PRODUCT_MAP

                # Find the existing opp for this account + product
                subtype = EXPANSION_PRODUCT_MAP.get(product, product)
                opps = sf_query(
                    f"SELECT Id, Name, StageName, NextStep, CloseDate "
                    f"FROM Opportunity "
                    f"WHERE AccountId = '{account_id}' "
                    f"AND Expansion_Product__c = '{subtype}' "
                    f"AND IsClosed = false "
                    f"ORDER BY CreatedDate DESC LIMIT 1"
                )

                if not opps:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"No open *{product}* opp found for *{account_name}*.",
                    )
                    return

                opp = opps[0]
                opp_id = opp["Id"]

                # Map field_updates to SF API field names
                sf_fields = {}
                for field, val in field_updates.items():
                    if field == "next_step":
                        sf_fields["NextStep"] = val[:255]
                    elif field == "close_date":
                        sf_fields["CloseDate"] = val
                    elif field == "stage":
                        sf_fields["StageName"] = val
                    elif field == "next_step_due_date":
                        sf_fields["Next_Step_Due_Date__c"] = val

                if not sf_fields:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"No valid fields to update for *{product}* opp.",
                    )
                    return

                success = update_opportunity(opp_id, sf_fields)
                if success:
                    opp_link = sf_opp_url(opp_id)
                    updates_summary = ", ".join(f"{k}={v}" for k, v in sf_fields.items())
                    client.chat_postMessage(
                        channel=user_id,
                        blocks=[{
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": (
                                    f"\u2705 Updated *{product}* opp for *{account_name}*\n"
                                    f"Fields: {updates_summary}\n"
                                    f"<{opp_link}|View in Salesforce>"
                                ),
                            },
                        }],
                        text=f"Opp updated: {opp['Name']}",
                    )
                else:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"Failed to update *{product}* opp. Check Growth MCP auth in Glass.",
                    )
            except Exception as e:
                logger.error("SFDC opp update failed for %s: %s", account_name, e)
                client.chat_postMessage(
                    channel=user_id,
                    text=f"Failed to update opp: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.action({"action_id": re.compile(r"^gong_link_")})
    def handle_gong_link(ack, body):
        """Handle Gong link button — no-op, URL opens in browser."""
        ack()

    @app.action({"action_id": re.compile(r"^create_opp_")})
    def handle_create_opp_link(ack, body):
        """Handle legacy Create Opp button — no-op, URL opens in browser."""
        ack()

    @app.action({"action_id": re.compile(r"^glass_email_draft_")})
    def handle_glass_email_draft(ack, body, client):
        """Handle Create Draft via Glass — reads full email from pending_drafts file."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        from utils.pending_drafts import get_draft

        action = body.get("actions", [{}])[0]
        value = action.get("value", "{}")
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            payload = {}

        draft_id = payload.get("draft_id", "")
        if not draft_id:
            # Legacy fallback: old-style payload with inline email fields
            client.chat_postMessage(
                channel=user_id,
                text="Could not find draft — button payload missing draft_id.",
            )
            return

        draft = get_draft(draft_id)
        if not draft:
            client.chat_postMessage(
                channel=user_id,
                text=f"Pending draft `{draft_id}` not found. It may have been cleaned up.",
            )
            return

        to = draft.get("to", "")
        cc = draft.get("cc", "")
        subject = draft.get("subject", "")
        html_body = draft.get("html_body", "")

        # Send formatted email content as a DM so Greg can copy-paste or Glass can pick it up
        email_blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "Email Draft Content", "emoji": True},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*To:* {to}\n"
                        + (f"*CC:* {cc}\n" if cc else "")
                        + f"*Subject:* {subject}\n"
                        + f"*Draft ID:* `{draft_id}`"
                    ),
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "_Full email body below — paste into Gmail or tell Glass to create the draft._",
                },
            },
        ]
        client.chat_postMessage(
            channel=user_id,
            blocks=email_blocks,
            text=f"Email draft for: {subject}",
        )
        # Send the plaintext body as a separate message for clean copy
        import re as _re
        plain_body = _re.sub(r'<[^>]+>', '', html_body).strip()
        if plain_body:
            client.chat_postMessage(
                channel=user_id,
                text=plain_body[:3000],
            )

    @app.action({"action_id": re.compile(r"^account_context_")})
    def handle_account_context(ack, body, client):
        """Handle Context button — runs account deep dive."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        action = body.get("actions", [{}])[0]
        value = action.get("value", "{}")
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            payload = {}

        account_name = payload.get("account", "")
        if not account_name:
            client.chat_postMessage(
                channel=user_id,
                text="Could not identify the account.",
            )
            return

        client.chat_postMessage(
            channel=user_id,
            text=f"Pulling full context for *{account_name}*... (~10 sec)",
        )

        def _run():
            try:
                from jobs.account_deep_dive import run_account_deep_dive
                run_account_deep_dive(account_name, client, user_id)
            except Exception as e:
                logger.error("Account context failed for %s: %s", account_name, e)
                client.chat_postMessage(
                    channel=user_id,
                    text=f"Failed to pull context for *{account_name}*: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.action({"action_id": re.compile(r"^batch_draft_")})
    def handle_batch_draft(ack, body, client):
        """Handle Batch Draft button — drafts emails for a cluster."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        action = body.get("actions", [{}])[0]
        value = action.get("value", "{}")
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            payload = {}

        cluster_type = payload.get("type", "")
        template_context = payload.get("template_context", "")
        count = payload.get("count", 0)

        if not cluster_type:
            client.chat_postMessage(
                channel=user_id,
                text="Could not identify the cluster. DM `batch outreach` to refresh.",
            )
            return

        def _run():
            from jobs.batch_outreach import draft_batch_emails
            draft_batch_emails(cluster_type, template_context, client, user_id=user_id)

        threading.Thread(target=_run, daemon=True).start()

    # ── Priority Actions Level 2: Category drill-down ────────────────

    @app.action("priority_show_early_accel")
    def handle_show_early_accel(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "early_accel", user_id)

    @app.action("priority_show_close_window")
    def handle_show_close_window(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "close_window", user_id)

    @app.action("priority_show_close_now")
    def handle_show_close_now(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "close_now", user_id)

    @app.action("priority_show_leading")
    def handle_show_leading(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "leading", user_id)

    @app.action("priority_show_first_bill")
    def handle_show_first_bill(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "first_bill", user_id)

    @app.action("priority_show_opp_first_spend")
    def handle_show_opp_first_spend(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "opp_first_spend", user_id)

    @app.action("priority_show_zero_to_one")
    def handle_show_zero_to_one(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "zero_to_one", user_id)

    @app.action("priority_show_sustained_accel")
    def handle_show_sustained_accel(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "sustained_accel", user_id)

    @app.action("priority_show_followup")
    def handle_show_followup(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "followup", user_id)

    @app.action("priority_show_stale")
    def handle_show_stale(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "stale", user_id)

    @app.action("priority_show_post_meeting_opp")
    def handle_show_post_meeting_opp(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "post_meeting_opp", user_id)

    @app.action("priority_show_reopen")
    def handle_show_reopen(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "reopen", user_id)

    @app.action("priority_show_treasury_spike")
    def handle_show_treasury_spike(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "treasury_spike", user_id)

    @app.action("priority_show_underperforming_d30")
    def handle_show_underperforming_d30(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "underperforming_d30", user_id)

    @app.action("priority_show_underperforming_d60")
    def handle_show_underperforming_d60(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "underperforming_d60", user_id)

    @app.action("priority_show_multi_product")
    def handle_show_multi_product(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _send_category_detail(client, "multi_product", user_id)

    # ── Smart Outreach Email Drafter ─────────────────────────────────

    @app.action({"action_id": re.compile(r"^draft_outreach_")})
    def handle_draft_outreach(ack, body, client):
        """Draft a context-aware outreach email for any category.

        Pulls Gong transcripts, SFDC notes, past emails, and contacts
        to generate a relevant, personalized email.
        """
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        action = body.get("actions", [{}])[0]
        value = action.get("value", "{}")
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            payload = {}

        account_name = payload.get("account", "Unknown")
        account_id = payload.get("account_id", "")
        opp_id = payload.get("opp_id", "")
        product = payload.get("product", "")
        category = payload.get("category", "")

        if not account_id:
            client.chat_postMessage(
                channel=user_id,
                text=f"Could not identify the account — try `/{COMMAND_PREFIX}-brief` instead.",
            )
            return

        trial_end = payload.get("trial_end", "")

        # Trial categories use fixed templates, not Claude-generated
        if category in ("prospect_active_procurement_trial", "plus_trial"):
            trial_label = "procurement" if category == "prospect_active_procurement_trial" else "Plus"
            client.chat_postMessage(
                channel=user_id,
                text=f"Drafting {trial_label} trial email for *{account_name}*...",
            )

            def _run_trial(cat=category, t_end=trial_end):
                with _draft_semaphore:
                    try:
                        _draft_trial_template(cat, account_id, account_name, t_end, client, user_id=user_id)
                    except Exception as e:
                        logger.error("Trial draft failed for %s: %s", account_name, e)
                        client.chat_postMessage(
                            channel=user_id,
                            text=f"Failed to draft email for *{account_name}*: {e}",
                        )

            threading.Thread(target=_run_trial, daemon=True).start()
            return

        client.chat_postMessage(
            channel=user_id,
            text=f"Drafting outreach email for *{account_name}*... gathering context (~15 sec).",
        )

        def _run():
            with _draft_semaphore:
                try:
                    _draft_smart_email(account_id, account_name, opp_id, product, category, client, user_id=user_id)
                except Exception as e:
                    logger.error("Outreach draft failed for %s: %s", account_name, e)
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"Failed to draft email for *{account_name}*: {e}",
                    )

        threading.Thread(target=_run, daemon=True).start()

    # ── Legacy: Draft Re-engage (from old flat priority list) ────────

    @app.action({"action_id": re.compile(r"^draft_reengage_")})
    def handle_draft_reengage(ack, body, client):
        """Legacy re-engage handler — delegates to smart drafter."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        action = body.get("actions", [{}])[0]
        value = action.get("value", "{}")
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            payload = {}

        account_name = payload.get("account", "Unknown")
        account_id = payload.get("account_id", "")
        opp_id = payload.get("opp_id", "")
        product = payload.get("product", "")

        if not account_id:
            client.chat_postMessage(
                channel=user_id,
                text=f"Could not identify the account — try `/{COMMAND_PREFIX}-brief` instead.",
            )
            return

        client.chat_postMessage(
            channel=user_id,
            text=f"Drafting re-engagement email for *{account_name}* ({product})... ~15 seconds.",
        )

        def _run():
            with _draft_semaphore:
                try:
                    _draft_smart_email(account_id, account_name, opp_id, product, "stale", client, user_id=user_id)
                except Exception as e:
                    logger.error("Re-engage draft failed for %s: %s", account_name, e)
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"Failed to draft re-engage email for *{account_name}*: {e}",
                    )

        threading.Thread(target=_run, daemon=True).start()

    # ── Home Tab: Brief buttons ────────────────────────────────────

    @app.action({"action_id": re.compile(r"^home_brief_")})
    def handle_home_brief(ack, body, client):
        """Handle Brief button from Today's Meetings on the home tab.

        Triggers a pre-meeting brief for the selected meeting's external
        attendees/account and sends the result as a DM.
        """
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        action = body.get("actions", [{}])[0]
        value = action.get("value", "{}")
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            payload = {}

        title = payload.get("title", "")
        attendees = payload.get("attendees", [])

        if not attendees:
            client.chat_postMessage(
                channel=user_id,
                text=f"No external attendees found for *{title}* — can't generate a brief.",
            )
            return

        client.chat_postMessage(
            channel=user_id,
            text=f":briefcase: Generating brief for *{title}*... (~15 sec)",
        )

        def _run():
            try:
                from jobs.pre_meeting_brief import _process_meeting
                # Build a meeting dict compatible with _process_meeting
                meeting_data = {
                    "event_id": payload.get("event_id", f"home_brief_{title}"),
                    "title": title,
                    "attendees": attendees,
                    "start": None,
                    "end": None,
                    "duration_min": 30,
                    "location": "",
                    "meet_link": "",
                    "description": "",
                    "organizer": "",
                }
                _process_meeting(meeting_data, client, force=True, user_id=user_id)
            except Exception as e:
                logger.error("Home brief failed for %s: %s", title, e)
                client.chat_postMessage(
                    channel=user_id,
                    text=f"Brief generation failed for *{title}*: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    # ── Home Tab: View All button for priority alert signal groups ───

    @app.action({"action_id": re.compile(r"^view_all_")})
    def handle_view_all(ack, body, client):
        """Expand a priority alert signal group beyond the 5-item limit."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        action = body.get("actions", [{}])[0]
        signal_type = action.get("action_id", "").replace("view_all_", "")

        signal_labels = {
            "early_accel": ":zap: Early Acceleration — Full List",
            "close_window": ":alarm_clock: Close Window — Full List",
            "leading": ":eyes: Leading Indicators — Full List",
            "first_bill": ":tada: First Bill Created — Full List",
            "close_now": ":money_with_wings: Close ASAP — Full List",
            "zero_to_one": ":rocket: Zero-to-One — Full List",
            "sustained_accel": ":chart_with_upwards_trend: Sustained Acceleration — Full List",
            "treasury_spike": ":moneybag: Treasury GLA Spike — Full List",
        }
        header = signal_labels.get(signal_type, f"Priority Alerts — {signal_type}")

        client.chat_postMessage(
            channel=user_id,
            text=f"Loading {header}...",
        )

        def _run():
            try:
                from core.snowflake_client import run_query
                from queries.queries import HOME_PRIORITY_ALERTS_QUERY, format_query
                import math

                df = run_query(format_query(HOME_PRIORITY_ALERTS_QUERY, user_id=user_id))
                if df.empty:
                    client.chat_postMessage(
                        channel=user_id,
                        text="No priority alert data available right now.",
                    )
                    return

                filtered = df[df["signal_type"] == signal_type]
                if filtered.empty:
                    client.chat_postMessage(
                        channel=user_id,
                        text=f"No {signal_type} signals found.",
                    )
                    return

                def _si(v):
                    try:
                        f = float(v)
                        return 0 if math.isnan(f) else int(f)
                    except Exception:
                        return 0

                sf_base = "https://rampfinancial.lightning.force.com/lightning"
                lines = [f"*{header}*\n"]
                buttons = []

                def _touch(r):
                    """Compact last-call / last-email context."""
                    parts = []
                    for key, label in [("last_call_date", "Call"), ("last_email_date", "Email")]:
                        val = r.get(key)
                        if val is not None and str(val).strip() not in ("", "None", "NaT"):
                            try:
                                if hasattr(val, "strftime"):
                                    parts.append(f"{label} {val.strftime('%-m/%-d')}")
                                else:
                                    from datetime import datetime as _dt
                                    d = _dt.strptime(str(val)[:10], "%Y-%m-%d")
                                    parts.append(f"{label} {d.strftime('%-m/%-d')}")
                            except Exception:
                                pass
                    return f"\n   _Last: {' · '.join(parts)}_" if parts else ""

                for _, row in filtered.iterrows():
                    acct_name = row.get("account_name", "Unknown")
                    acct_id = row.get("account_id", "")
                    sf_link = f"{sf_base}/r/Account/{acct_id}/view" if acct_id else ""
                    link = f"<{sf_link}|{acct_name}>" if sf_link else acct_name
                    product = str(row.get("product", "")).replace(" Expansion", "")
                    paced = _si(row.get("paced_amount", 0))
                    base = _si(row.get("baseline_amount", 0))
                    l30d = _si(row.get("spend_l30d", 0))
                    l7d = _si(row.get("spend_l7d", 0))
                    cp = _si(row.get("est_cp", 0))
                    delta = _si(row.get("l30d_spend_delta", 0))
                    cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
                    pct = int(((paced - base) / base) * 100) if base > 0 else 0
                    touch = _touch(row)

                    if signal_type == "early_accel":
                        lines.append(
                            f"\u2022 {link} — {product} L7D pacing ${paced:,}/mo vs ${base:,} baseline (+{pct}%)"
                            f"\n   L30D: ${l30d:,} · L7D raw: ${l7d:,}{cp_str}{touch}"
                        )
                    elif signal_type == "close_window":
                        lines.append(
                            f"\u2022 {link} — {product} L7D pacing ${paced:,}/mo · L30D baseline: ${l30d:,}{cp_str}{touch}"
                        )
                    elif signal_type == "leading":
                        lines.append(
                            f"\u2022 {link} — {product} ${paced:,} incoming vs ${base:,}/mo baseline{cp_str}{touch}"
                        )
                    elif signal_type == "first_bill":
                        lines.append(
                            f"\u2022 {link} — first bill ${paced:,}{cp_str}{touch}"
                        )
                    elif signal_type == "close_now":
                        lines.append(
                            f"\u2022 {link} — {product} L30D +${abs(delta):,} above baseline{cp_str}{touch}"
                        )
                    elif signal_type == "zero_to_one":
                        spend_since = _si(row.get("spend_since_opp", 0))
                        lines.append(
                            f"\u2022 {link} — {product} · ${spend_since:,} since opp · L30D ${l30d:,} · L7D ${l7d:,}{cp_str}{touch}"
                        )
                    elif signal_type == "sustained_accel":
                        lines.append(
                            f"\u2022 {link} — {product} pacing ${paced:,}/mo vs ${base:,} baseline (+{pct}%){cp_str}{touch}"
                        )

                    # Build draft button
                    cat_map = {
                        "early_accel": "prospect", "close_window": "close_window",
                        "leading": "prospect", "first_bill": "zero_to_one",
                        "close_now": "close_now", "opp_first_spend": "zero_to_one",
                        "zero_to_one": "zero_to_one", "sustained_accel": "prospect",
                    }
                    payload = json.dumps({
                        "account": acct_name,
                        "account_id": acct_id,
                        "opp_id": row.get("opportunity_id", ""),
                        "product": str(row.get("product", "")),
                        "category": cat_map.get(signal_type, "prospect"),
                    })
                    buttons.append({
                        "type": "button",
                        "text": {"type": "plain_text", "text": f":envelope: {acct_name[:20]}", "emoji": True},
                        "action_id": f"draft_outreach_{signal_type}_{acct_id}",
                        "value": payload,
                    })

                # Send in chunks — pair each account's detail line with its draft button
                # Slack limits: 50 blocks per message, 3000 chars per text block
                msg_blocks = [{"type": "header", "text": {"type": "plain_text", "text": header.replace("*", ""), "emoji": True}}]

                # Pair each account line with its draft button inline
                for idx, line_text in enumerate(lines[1:]):  # skip header line
                    # Add the account detail as a section with the draft button as an accessory
                    block = {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": line_text},
                    }
                    if idx < len(buttons):
                        block["accessory"] = buttons[idx]
                    msg_blocks.append(block)

                    # Slack caps at 50 blocks per message — split if needed
                    if len(msg_blocks) >= 49:
                        client.chat_postMessage(
                            channel=user_id,
                            blocks=msg_blocks,
                            text=header,
                        )
                        msg_blocks = []

                if msg_blocks:
                    client.chat_postMessage(
                        channel=user_id,
                        blocks=msg_blocks,
                        text=header,
                    )

            except Exception as e:
                logger.error("View All failed for %s: %s", signal_type, e)
                client.chat_postMessage(
                    channel=user_id,
                    text=f"Failed to load full list: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    # ── Home Tab: Post-Meeting button ────────────────────────────────

    @app.action({"action_id": re.compile(r"^home_post_meeting_")})
    def handle_home_post_meeting(ack, body, client):
        """Handle Post-Meeting button from Today's Meetings on the home tab.

        Triggers the Granola-first post-meeting flow for the selected meeting.
        """
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        action = body.get("actions", [{}])[0]
        value = action.get("value", "{}")
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            payload = {}

        title = payload.get("title", "")
        attendees = payload.get("attendees", [])

        client.chat_postMessage(
            channel=user_id,
            text=f":memo: Running post-meeting analysis for *{title}*... (~20 sec)",
        )

        def _run():
            try:
                from jobs.granola_followup import run_granola_followup
                run_granola_followup(client, user_id=user_id, force=True)
            except Exception as e:
                logger.error("Home post-meeting failed for %s: %s", title, e)
                client.chat_postMessage(
                    channel=user_id,
                    text=f"Post-meeting analysis failed for *{title}*: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    # ── Home Tab: Settings toggles ──────────────────────────────────

    @app.action({"action_id": re.compile(r"^settings_toggle_")})
    def handle_settings_toggle(ack, body, client):
        """Handle settings toggle button from the home tab.

        Toggles the setting on/off and refreshes the home tab.
        """
        ack()

        action = body.get("actions", [{}])[0]
        action_id = action.get("action_id", "")
        # Extract setting key from action_id: settings_toggle_{key}
        setting_key = action_id.replace("settings_toggle_", "", 1)

        if not setting_key:
            return

        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        try:
            from utils.settings import get_setting, update_setting
            current_val = get_setting(setting_key, user_id=user_id)
            new_val = not current_val
            update_setting(setting_key, new_val, user_id=user_id)

            state_str = "ON" if new_val else "OFF"
            logger.info("Setting %s toggled to %s for user %s", setting_key, state_str, user_id)
        except Exception as e:
            logger.error("Failed to toggle setting %s: %s", setting_key, e)
            return

        def _refresh():
            try:
                from handlers.home_tab import _build_home_blocks
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(
                    user_id=user_id,
                    view={
                        "type": "home",
                        "blocks": blocks,
                    },
                )
            except Exception as e:
                logger.error("Home tab refresh after settings toggle failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    # ── Top-CP Re-engage overflow menu ───────────────────────────────

    @app.action({"action_id": re.compile(r"^top_cp_overflow_")})
    def handle_top_cp_overflow(ack, body, client):
        """Handle Top-CP overflow menu: draft email, snooze 30d, or view in SFDC."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        action = body.get("actions", [{}])[0]
        selected = action.get("selected_option", {}).get("value", "{}")
        try:
            payload = json.loads(selected)
        except json.JSONDecodeError:
            payload = {}

        op = payload.get("op", "")
        account_id = payload.get("account_id", "")
        account_name = payload.get("account", "Unknown")

        if not account_id:
            client.chat_postMessage(channel=user_id, text="Could not identify the account.")
            return

        if op == "view":
            from core.slack_formatter import sf_account_url
            url = sf_account_url(account_id)
            client.chat_postMessage(
                channel=user_id,
                text=f":bust_in_silhouette: *{account_name}* — <{url}|Open in Salesforce>",
            )
            return

        if op == "snooze":
            from utils.snooze import snooze_account
            snooze_account(account_id, days=30, user_id=user_id)
            client.chat_postMessage(
                channel=user_id,
                text=f":zzz: Snoozed *{account_name}* from Top-CP for 30 days.",
            )

            def _refresh_home():
                try:
                    from handlers.home_tab import _build_home_blocks
                    blocks = _build_home_blocks(client, user_id)
                    client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
                except Exception as e:
                    logger.error("Home refresh after Top-CP snooze failed: %s", e)

            threading.Thread(target=_refresh_home, daemon=True).start()
            return

        if op == "draft":
            client.chat_postMessage(
                channel=user_id,
                text=f"Drafting Top-CP re-engage email for *{account_name}*...",
            )

            def _draft():
                with _draft_semaphore:
                    try:
                        _draft_top_cp_reengage(account_id, account_name, client, user_id=user_id)
                    except Exception as e:
                        logger.error("Top-CP draft failed for %s: %s", account_name, e)
                        client.chat_postMessage(
                            channel=user_id,
                            text=f"Failed to draft Top-CP email for *{account_name}*: {e}",
                        )

            threading.Thread(target=_draft, daemon=True).start()
            return

    # ── Plays sub-section (Prospecting tab) ──────────────────────────

    @app.action({"action_id": re.compile(r"^plays_toggle_")})
    def handle_plays_toggle(ack, body, client):
        """Expand/collapse a play row in the Prospecting tab."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        action = body.get("actions", [{}])[0]
        play_id = action.get("value") or ""
        if not play_id:
            return

        from handlers.home_tab import _plays_expanded
        expanded = _plays_expanded.setdefault(user_id, set())
        if play_id in expanded:
            expanded.remove(play_id)
        else:
            expanded.add(play_id)

        def _refresh():
            try:
                from handlers.home_tab import _build_home_blocks
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("Home refresh after plays toggle failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    @app.action({"action_id": re.compile(r"^plays_overflow_")})
    def handle_plays_overflow(ack, body, client):
        """Overflow menu on a Plays card: draft re-engage / snooze 30d / view SFDC."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        action = body.get("actions", [{}])[0]
        selected = action.get("selected_option", {}).get("value", "{}")
        try:
            payload = json.loads(selected)
        except json.JSONDecodeError:
            payload = {}

        op = payload.get("op", "")
        play_id = payload.get("play_id", "")
        account_id = payload.get("account_id", "")
        account_name = payload.get("account", "Unknown")

        if not account_id:
            client.chat_postMessage(channel=user_id, text="Could not identify the account.")
            return

        if op == "view":
            from core.slack_formatter import sf_account_url
            url = sf_account_url(account_id)
            client.chat_postMessage(
                channel=user_id,
                text=f":bust_in_silhouette: *{account_name}* — <{url}|Open in Salesforce>",
            )
            return

        if op == "snooze":
            from utils.snooze import snooze_play_account
            snooze_play_account(play_id, account_id, days=30, user_id=user_id)
            client.chat_postMessage(
                channel=user_id,
                text=f":zzz: Snoozed *{account_name}* from play {play_id} for 30 days.",
            )

            def _refresh():
                try:
                    from handlers.home_tab import _build_home_blocks
                    blocks = _build_home_blocks(client, user_id)
                    client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
                except Exception as e:
                    logger.error("Home refresh after plays snooze failed: %s", e)

            threading.Thread(target=_refresh, daemon=True).start()
            return

        if op == "draft":
            client.chat_postMessage(
                channel=user_id,
                text=f"Drafting re-engage email for *{account_name}* (play {play_id})…",
            )

            def _draft():
                with _draft_semaphore:
                    try:
                        # Pull the account's row from the Plays cache so the
                        # drafter can access per-play signal fields (po_bill_count,
                        # erp_snippet, card_gap_cp, etc.) for the pitch hook.
                        from jobs.plays_refresh import get_cached_play
                        play_row = {}
                        cached = get_cached_play(play_id, user_id=user_id)
                        if cached:
                            for r in cached.get("rows", []):
                                if r.get("account_id") == account_id:
                                    play_row = r
                                    break

                        from jobs.pipeline_drafter import draft_account_reengagement
                        payload_dict = {
                            "account_id": account_id,
                            "account_name": account_name,
                            "play_id": play_id,
                            "play_row": play_row,
                            "category": "plays",
                        }
                        draft_account_reengagement(payload_dict, client, user_id=user_id)
                    except Exception as e:
                        logger.error("Plays draft failed for %s (play=%s): %s", account_name, play_id, e)
                        client.chat_postMessage(
                            channel=user_id,
                            text=f"Failed to draft email for *{account_name}*: {e}",
                        )

            threading.Thread(target=_draft, daemon=True).start()
            return

    # ── Hot List tab ─────────────────────────────────────────────────

    @app.action({"action_id": re.compile(r"^hot_list_size_")})
    def handle_hot_list_size(ack, body, client):
        """Change Top N (20 / 50 / 100)."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        action = body.get("actions", [{}])[0]
        try:
            n = int(action.get("value", "25"))
        except ValueError:
            n = 25
        from handlers.home_tab import _hot_list_top_n
        _hot_list_top_n[user_id] = n

        def _refresh():
            try:
                from handlers.home_tab import _build_home_blocks
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("Home refresh after hot_list size change failed: %s", e)
        threading.Thread(target=_refresh, daemon=True).start()

    @app.action("hot_list_rebuild")
    def handle_hot_list_rebuild(ack, body, client):
        """Force-rebuild the Hot List for this user (bypasses cache)."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        def _refresh():
            try:
                from jobs.hot_list import build_hot_list_for_user
                from handlers.home_tab import _build_home_blocks, _hot_list_top_n
                n = _hot_list_top_n.get(user_id, 25)
                # Force-recompute and persist
                fresh = build_hot_list_for_user(user_id or "", top_n=n)
                # Save to the cache file so next tab open is instant
                from jobs.hot_list import CACHE_FILE
                import json as _json
                payload = {}
                if CACHE_FILE.exists():
                    payload = _json.loads(CACHE_FILE.read_text())
                payload.setdefault("by_user", {})[user_id or "_default"] = fresh
                CACHE_FILE.write_text(_json.dumps(payload, indent=2, default=str))

                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("Hot List rebuild failed: %s", e)
        threading.Thread(target=_refresh, daemon=True).start()

    @app.action({"action_id": re.compile(r"^hot_list_overflow_")})
    def handle_hot_list_overflow(ack, body, client):
        """Draft / snooze / view SFDC on a Hot List card. Draft routes through
        the existing plays overflow handler, using the top-matching play_id."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        action = body.get("actions", [{}])[0]
        selected = action.get("selected_option", {}).get("value", "{}")
        try:
            payload = json.loads(selected)
        except json.JSONDecodeError:
            payload = {}

        op = payload.get("op", "")
        play_id = payload.get("play_id", "")
        account_id = payload.get("account_id", "")
        account_name = payload.get("account", "Unknown")

        if not account_id:
            client.chat_postMessage(channel=user_id, text="Could not identify the account.")
            return

        if op == "view":
            from core.slack_formatter import sf_account_url
            url = sf_account_url(account_id)
            client.chat_postMessage(
                channel=user_id,
                text=f":bust_in_silhouette: *{account_name}* — <{url}|Open in Salesforce>",
            )
            return

        if op == "snooze":
            from utils.snooze import snooze_play_account
            snooze_play_account(play_id, account_id, days=30, user_id=user_id)
            client.chat_postMessage(
                channel=user_id,
                text=f":zzz: Snoozed *{account_name}* from play {play_id} for 30 days (still visible under any other play it matches).",
            )
            return

        if op == "draft":
            client.chat_postMessage(
                channel=user_id,
                text=f"Drafting re-engage email for *{account_name}* (play {play_id})…",
            )
            def _draft():
                with _draft_semaphore:
                    try:
                        from jobs.plays_refresh import get_cached_play
                        play_row = {}
                        cached = get_cached_play(play_id, user_id=user_id)
                        if cached:
                            for r in cached.get("rows", []):
                                if r.get("account_id") == account_id:
                                    play_row = r
                                    break
                        from jobs.pipeline_drafter import draft_account_reengagement
                        draft_account_reengagement({
                            "account_id": account_id,
                            "account_name": account_name,
                            "play_id": play_id,
                            "play_row": play_row,
                            "category": "hot_list",
                        }, client, user_id=user_id)
                    except Exception as e:
                        logger.error("Hot List draft failed for %s: %s", account_name, e)
                        client.chat_postMessage(
                            channel=user_id,
                            text=f"Failed to draft email for *{account_name}*: {e}",
                        )
            threading.Thread(target=_draft, daemon=True).start()
            return

    # ── Team Intel tab ───────────────────────────────────────────────

    @app.action({"action_id": re.compile(r"^team_intel_window_")})
    def handle_team_intel_window(ack, body, client):
        """Change the Team Intel lookback window (30 / 90 / 180 / 365)."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        action = body.get("actions", [{}])[0]
        try:
            days = int(action.get("value", "180"))
        except ValueError:
            days = 180

        from handlers.home_tab import _team_cp_window, _team_intel_expanded
        _team_cp_window[user_id] = days
        # Reset expand state when the window changes — opp IDs may not match
        _team_intel_expanded[user_id] = set()

        def _refresh():
            try:
                from handlers.home_tab import _build_home_blocks
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("Home refresh after team_intel window change failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    @app.action("team_intel_refresh")
    def handle_team_intel_refresh(ack, body, client):
        """Force-refresh the Team Intel cache (bypasses 1h TTL)."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        from handlers.home_tab import _team_cp_window, _team_cp_fetch
        days = _team_cp_window.get(user_id, 180)

        def _refresh():
            try:
                _team_cp_fetch(user_id, days, force=True)
                from handlers.home_tab import _build_home_blocks
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("team_intel refresh failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    @app.action({"action_id": re.compile(r"^team_intel_deal_")})
    def handle_team_intel_deal_toggle(ack, body, client):
        """Expand/collapse a deal card in Team Intel tab."""
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        action = body.get("actions", [{}])[0]
        opp_id = action.get("value") or ""
        if not opp_id:
            return

        from handlers.home_tab import _team_intel_expanded
        expanded = _team_intel_expanded.setdefault(user_id, set())
        if opp_id in expanded:
            expanded.remove(opp_id)
        else:
            expanded.add(opp_id)

        def _refresh():
            try:
                from handlers.home_tab import _build_home_blocks
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("Home refresh after team_intel deal toggle failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    # ── App mention ──────────────────────────────────────────────────

    @app.event("app_mention")
    def handle_app_mention(event, client, say):
        """Respond to @mentions in channels."""
        from core.user_registry import is_registered
        user = event.get("user", "")
        if not is_registered(user):
            say("You're not registered — open my Home tab to get started. :lock:")
            return
        say("Got it — I'm processing your request. I'll DM you with the results.")


# ── Helper: send category detail ────────────────────────────────────────────


def _send_category_detail(client, category: str, user_id=None):
    """Send Level 2 detail blocks for a category."""
    from jobs.priority_actions import build_category_detail_blocks

    dm_target = user_id or GREG_SLACK_ID
    blocks = build_category_detail_blocks(category, user_id=user_id)
    _TITLES = {
        "close_now": "Close Now",
        "zero_to_one": "Zero-to-One",
        "prospect": "Prospecting",
        "treasury_spike": "Treasury GLA Spike",
        "underperforming_d30": "D30 Checkpoint",
        "underperforming_d60": "D60 Checkpoint",
        "multi_product": "Multi-Product Signals",
        "followup": "Follow-ups",
        "post_meeting_opp": "Post-Meeting Opps",
        "stale": "Stale Opps",
        "reopen": "Re-open",
    }
    title = _TITLES.get(category, category)
    client.chat_postMessage(
        channel=dm_target,
        blocks=blocks,
        text=f"Priority Actions — {title}",
    )


# ── Procurement Trial Template Drafter ──────────────────────────────────────


def _draft_trial_template(category, account_id, account_name, trial_end, client, user_id=None):
    """Draft a trial email using the fixed template for Plus or Procurement.

    Fetches contacts from SFDC, builds greeting, and uses the appropriate
    fixed template — no Claude generation, no context gathering.
    """
    dm_target = user_id or GREG_SLACK_ID
    from templates.emails import procurement_trial_intro_email, plus_trial_intro_email
    from utils.account_resolver import fetch_contact_emails, is_hash_like
    from core.gumstack_gmail import create_draft as gumstack_create, is_available as gumstack_ok
    from core.slack_formatter import drafter_confirmation_blocks

    is_procurement = category == "prospect_active_procurement_trial"

    # Fetch contacts
    contacts_map = fetch_contact_emails(None, [account_id])
    acct_contacts = contacts_map.get(account_id, [])

    # Filter out hash-like names and contacts without emails
    valid = [c for c in acct_contacts if c.get("email") and not is_hash_like(c.get("name", ""))]
    if not valid:
        client.chat_postMessage(
            channel=dm_target,
            text=f"No contact email found for *{account_name}*. Add a contact in Salesforce first.",
        )
        return

    # Build recipient list — domain-match guard to avoid CC'ing wrong companies
    primary_domain = valid[0]["email"].lower().split("@")[-1] if "@" in valid[0]["email"] else ""
    domain_matched = [valid[0]]
    for c in valid[1:]:
        em = c["email"].lower()
        d = em.split("@")[-1] if "@" in em else ""
        if primary_domain and d and d != primary_domain:
            continue
        domain_matched.append(c)
        if len(domain_matched) >= 4:
            break
    to_emails = ", ".join(c["email"] for c in domain_matched)
    first_names = [c.get("name", "").split()[0] for c in domain_matched if c.get("name")]

    primary_first = first_names[0] if first_names else "there"

    if is_procurement:
        html_body = procurement_trial_intro_email(
            first_name=primary_first, user_id=user_id, trial_end=trial_end,
        )
        subject = f"Ramp AM Intro & Procurement Trial Ending on {trial_end}" if trial_end else "Ramp AM Intro & Procurement Trial"
        draft_label = "Claude Drafts/Procurement Trials"
        drafter_label = "Procurement Trial"
    else:
        html_body = plus_trial_intro_email(
            first_name=primary_first, user_id=user_id, trial_end=trial_end,
        )
        subject = f"Ramp AM Intro & Plus Trial Ending on {trial_end}" if trial_end else "Ramp AM Intro & Plus Trial"
        draft_label = "Claude Drafts/Plus Trials"
        drafter_label = "Plus Trial"

    draft_id = ""
    if gumstack_ok():
        result = gumstack_create(
            to=to_emails, subject=subject, html_body=html_body,
            cc="", label=draft_label, user_id=user_id,
        )
        if result["success"]:
            draft_id = result["draft_id"]
        else:
            logger.warning("Gumstack draft failed for %s, falling back to queue", account_name)

    if not draft_id:
        from utils.pending_drafts import save_draft as save_pending_draft
        draft_id = f"pending_trial_{account_id}_{int(time.time())}"
        save_pending_draft(
            draft_id=draft_id, to=to_emails, cc="",
            subject=subject, html_body=html_body,
            account_name=account_name, label=draft_label, user_id=user_id or "",
        )

    details = (
        f"*To:* {to_emails}\n"
        f"*Subject:* {subject}\n"
        f"*Account:* {account_name} — {drafter_label}"
    )
    blocks = drafter_confirmation_blocks(
        drafter_type=drafter_label,
        account_name=account_name,
        details=details,
        draft_id=draft_id,
    )
    client.chat_postMessage(
        channel=dm_target, blocks=blocks,
        text=f"{drafter_label} draft ready for {account_name}",
    )


# ── Top-CP Re-engage Drafter ────────────────────────────────────────────────


def _draft_top_cp_reengage(account_id, account_name, client, user_id=None):
    """Draft a Top-CP re-engage email using the fixed template.

    Mirrors the Plus/Procurement trial drafter pattern: pull valid contacts,
    domain-match to avoid cc'ing wrong companies, call the template, create a
    Gmail draft (or queue locally), and DM a confirmation back.
    """
    dm_target = user_id or GREG_SLACK_ID
    from templates.emails import top_cp_reengage_email
    from utils.account_resolver import fetch_contact_emails, is_hash_like
    from core.gumstack_gmail import create_draft as gumstack_create, is_available as gumstack_ok
    from core.slack_formatter import drafter_confirmation_blocks

    contacts_map = fetch_contact_emails(None, [account_id])
    acct_contacts = contacts_map.get(account_id, [])
    valid = [c for c in acct_contacts if c.get("email") and not is_hash_like(c.get("name", ""))]
    if not valid:
        client.chat_postMessage(
            channel=dm_target,
            text=f"No contact email found for *{account_name}*. Add a contact in Salesforce first.",
        )
        return

    primary_domain = valid[0]["email"].lower().split("@")[-1] if "@" in valid[0]["email"] else ""
    domain_matched = [valid[0]]
    for c in valid[1:]:
        em = c["email"].lower()
        d = em.split("@")[-1] if "@" in em else ""
        if primary_domain and d and d != primary_domain:
            continue
        domain_matched.append(c)
        if len(domain_matched) >= 4:
            break
    to_emails = ", ".join(c["email"] for c in domain_matched)
    primary_first = (domain_matched[0].get("name", "") or "").split()[0] or "there"

    greeting = f"Hi {primary_first},"
    html_body = top_cp_reengage_email(greeting=greeting, user_id=user_id)
    subject = f"Quick check in — a few ways to get more out of Ramp"
    draft_label = "Claude Drafts/Top-CP Re-engage"

    draft_id = ""
    if gumstack_ok():
        result = gumstack_create(
            to=to_emails, subject=subject, html_body=html_body,
            cc="", label=draft_label, user_id=user_id,
        )
        if result.get("success"):
            draft_id = result.get("draft_id", "")
        else:
            logger.warning("Gumstack draft failed for %s Top-CP — queuing locally", account_name)

    if not draft_id:
        from utils.pending_drafts import save_draft as save_pending_draft
        draft_id = f"pending_top_cp_{account_id}_{int(time.time())}"
        save_pending_draft(
            draft_id=draft_id, to=to_emails, cc="",
            subject=subject, html_body=html_body,
            account_name=account_name, label=draft_label, user_id=user_id or "",
        )

    details = (
        f"*To:* {to_emails}\n"
        f"*Subject:* {subject}\n"
        f"*Account:* {account_name} — Top-CP Re-engage"
    )
    blocks = drafter_confirmation_blocks(
        drafter_type="Top-CP Re-engage",
        account_name=account_name,
        details=details,
        draft_id=draft_id,
    )
    client.chat_postMessage(
        channel=dm_target, blocks=blocks,
        text=f"Top-CP Re-engage draft ready for {account_name}",
    )


# ── Smart Email Drafter ─────────────────────────────────────────────────────


def _find_existing_thread(contact_email: str, product: str, owner_email: str, user_id=None):
    """Search Gmail for the best existing thread to reply to.

    Returns a dict with thread info if a suitable thread is found:
        {thread_id, subject, to, cc, message_id, last_message_from}
    Or None if no suitable thread exists.

    Rules:
    - Must be a thread where owner_email participated (sent or received)
    - Must be a thread where the contact_email is a recipient
    - Prefer threads where the owner sent the most recent message (outbound-last)
    - Prefer threads whose subject relates to the product
    - Never reply to a thread the owner has NOT participated in
    """
    from core.gumstack_gmail import read_emails, get_thread

    try:
        # Search for threads with this contact that I participated in
        query = f"to:{contact_email} OR from:{contact_email}"
        emails = read_emails(query, max_results=10, user_id=user_id)
        if not emails:
            return None

        # Group by threadId, keep most recent per thread
        threads_seen = {}
        for em in emails:
            tid = em.get("threadId", "")
            if not tid or tid in threads_seen:
                continue
            threads_seen[tid] = em

        # Score and rank threads
        best = None
        best_score = -1
        owner_lower = owner_email.lower()
        contact_lower = contact_email.lower()
        product_lower = (product or "").lower()

        for tid, em in threads_seen.items():
            subject = em.get("subject", "")
            from_addr = em.get("from", "").lower()
            score = 0

            # Must have owner involvement — check thread messages
            thread_msgs = get_thread(tid, user_id=user_id)
            if not thread_msgs:
                continue

            # Check that owner has sent at least one message in this thread
            owner_sent = any(owner_lower in msg.get("from", "").lower() for msg in thread_msgs)
            if not owner_sent:
                continue  # Skip threads where I never responded

            # Score: owner sent the last message (outbound-last = follow-up opportunity)
            last_msg = thread_msgs[-1]
            last_from = last_msg.get("from", "").lower()
            if owner_lower in last_from:
                score += 20  # Good: I sent last, they didn't reply — follow up
            else:
                score += 10  # They replied last — respond to them

            # Score: subject relevance to product
            if product_lower and product_lower in subject.lower():
                score += 30
            elif any(kw in subject.lower() for kw in ["ramp", "expansion", "follow", "check-in", "intro"]):
                score += 10

            # Score: recency (prefer more recent)
            score += 5  # baseline — all threads get some recency since we searched broadly

            if score > best_score:
                best_score = score
                # Extract reply-to info from the last message
                last_to = last_msg.get("to", "")
                last_cc = last_msg.get("cc", "")
                last_headers = last_msg.get("headers", {})
                message_id = last_headers.get("Message-Id") or last_headers.get("Message-ID") or ""

                best = {
                    "thread_id": tid,
                    "subject": subject,
                    "to": last_to,
                    "cc": last_cc,
                    "message_id": message_id,
                    "last_message_from": last_from,
                    "owner_sent_last": owner_lower in last_from,
                }

        return best

    except Exception as e:
        logger.debug("Thread search failed for %s: %s", contact_email, e)
        return None


def _draft_smart_email(account_id, account_name, opp_id, product, category, client, user_id=None):
    """Generate and send a context-aware outreach email draft.

    Works for any category (stale re-engage, zero-to-one outreach,
    prospecting, follow-up). Pulls all available context:
    1. SFDC account notes (AM/CSM notes, next steps)
    2. Recent Gong transcripts (call summary, product requests, competitors)
    3. Recent emails (direction, subject, body)
    4. Contact selection (prioritize people Greg has met with)
    """
    dm_target = user_id or GREG_SLACK_ID
    owner_name = get_user_sf_name(user_id)
    first_name_owner = get_user_first_name(user_id)
    booking_link = get_user_booking_link(user_id)
    owner_email = get_user_email(user_id) if user_id else ""
    from core.snowflake_client import run_query
    from core.claude_client import call_claude
    from core.gumstack_gmail import create_draft as gumstack_create, is_available as gumstack_ok
    from core.slack_formatter import format_currency, drafter_confirmation_blocks
    from queries.queries import ACCOUNT_NOTES_QUERY, ACCOUNT_EMAILS_FULL_QUERY
    from utils.account_resolver import fetch_contact_emails, is_hash_like
    from config import NTR_RATES

    # ── 1. Fetch all context in parallel (contacts, gong, notes, emails) ──
    import time as _time
    _t0 = _time.time()
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _fetch_contacts():
        return fetch_contact_emails(None, [account_id]).get(account_id, [])

    def _fetch_gong():
        # Include user_was_participant flag to distinguish user's calls from other Ramp team's calls
        _owner_email_escaped = owner_email.replace("'", "''").lower() if owner_email else ""
        gong_query = f"""
        SELECT
            gt.call_name,
            gt.call_start::date AS call_date,
            ROUND(gt.call_duration_sec / 60) AS duration_min,
            LISTAGG(DISTINCT gp.speaker_email, ', ')
                WITHIN GROUP (ORDER BY gp.speaker_email) AS external_emails,
            LISTAGG(DISTINCT NULLIF(gs.competitor_mentioned, ''), ', ') AS competitors,
            LISTAGG(DISTINCT NULLIF(gs.product_mentioned, ''), ', ') AS products_discussed,
            LISTAGG(
                CASE WHEN gs.product_request_text IS NOT NULL AND gs.product_request_text != ''
                     THEN gs.product_request_text END, ' | '
            ) WITHIN GROUP (ORDER BY gs.section_index) AS product_requests,
            LISTAGG(
                CASE WHEN gs.section_text IS NOT NULL AND gs.section_text != ''
                     THEN gs.section_name || ': ' || LEFT(gs.section_text, 400) END, ' || '
            ) WITHIN GROUP (ORDER BY gs.section_index) AS call_summary,
            MAX(CASE WHEN ramp_p.speaker_email IS NOT NULL
                      AND LOWER(ramp_p.speaker_email) = '{_owner_email_escaped}'
                     THEN 1 ELSE 0 END) AS user_was_participant
        FROM analytics.marts.dim_sfdc_gong_transcripts gt
        LEFT JOIN analytics.marts.dim_gong_section_summary gs ON gs.call_id = gt.call_id
        LEFT JOIN analytics.marts.dim_gong_transcript_paragraph gp
            ON gp.call_id = gt.call_id AND gp.is_ramp_participant = FALSE
        LEFT JOIN analytics.marts.dim_gong_transcript_paragraph ramp_p
            ON ramp_p.call_id = gt.call_id AND ramp_p.is_ramp_participant = TRUE
        WHERE gt.account_id = '{account_id}'
          AND gt.call_start >= DATEADD('day', -90, CURRENT_DATE)
          AND gt.call_duration_sec >= 180
        GROUP BY gt.call_id, gt.call_name, gt.call_start, gt.call_duration_sec
        ORDER BY gt.call_start DESC
        LIMIT 3
        """
        return run_query(gong_query)

    def _fetch_notes():
        return run_query(ACCOUNT_NOTES_QUERY.format(account_ids=f"'{account_id}'"))

    def _fetch_emails():
        return run_query(ACCOUNT_EMAILS_FULL_QUERY.format(account_ids=f"'{account_id}'"))

    acct_contacts = []
    gong_df = None
    notes_df = None
    emails_df = None

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(_fetch_contacts): "contacts",
            pool.submit(_fetch_gong): "gong",
            pool.submit(_fetch_notes): "notes",
            pool.submit(_fetch_emails): "emails",
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                result = future.result()
                if key == "contacts":
                    acct_contacts = result
                elif key == "gong":
                    gong_df = result
                elif key == "notes":
                    notes_df = result
                elif key == "emails":
                    emails_df = result
            except Exception as e:
                logger.debug("Context fetch %s failed for %s: %s", key, account_id, e)

    logger.info("Draft %s: parallel fetch done in %.1fs", account_name, _time.time() - _t0)
    _t1 = _time.time()

    # Parse Gong results — only mark contacts as "met" from calls where user was present
    gong_participants = set()  # contacts the user has personally met (for scoring + opening)
    gong_context = ""
    last_call_name = ""
    last_call_date = ""
    product_requests = ""
    competitors = ""
    if gong_df is not None and not gong_df.empty:
        parts = []
        for _, row in gong_df.iterrows():
            was_participant = int(row.get("user_was_participant", 0) or 0)
            ext_emails = str(row.get("external_emails", "") or "")
            # Only count contacts as "met" if the user was actually on the call
            if was_participant:
                for em in ext_emails.split(","):
                    em = em.strip().lower()
                    if em and "@" in em:
                        gong_participants.add(em)
            call_name = row.get("call_name", "")
            call_date = str(row.get("call_date", ""))
            summary = str(row.get("call_summary", "") or "")[:800]
            call_label = "[Your call]" if was_participant else "[Other Ramp team's call]"
            if not last_call_name and was_participant:
                # Only use user's own calls for "last call" context
                last_call_name = call_name
                last_call_date = call_date
                product_requests = str(row.get("product_requests", "") or "")
                competitors = str(row.get("competitors", "") or "")
            parts.append(f"--- {call_label} {call_name} ({call_date}) ---\n{summary}")
        gong_context = "\n\n".join(parts)
        # If user wasn't on any calls, still grab product requests/competitors from latest call
        if not last_call_name and not gong_df.empty:
            first_row = gong_df.iloc[0]
            product_requests = str(first_row.get("product_requests", "") or "")
            competitors = str(first_row.get("competitors", "") or "")

    # ── 3. Smart contact selection: TO = best primary, CC = additional stakeholders ──
    from utils.contact_scoring import select_recipients

    # Build engagement signals from emails and Gong
    email_correspondents = set()
    if emails_df is not None and not emails_df.empty:
        for _, e in emails_df.iterrows():
            ext_email = (str(e.get("external_contact_email", "") or "")).strip().lower()
            if ext_email and "@" in ext_email:
                email_correspondents.add(ext_email)

    primary, cc_contacts = select_recipients(
        acct_contacts,
        gong_participants=gong_participants,
        email_correspondents=email_correspondents,
        max_cc=3,
    )

    if not primary:
        client.chat_postMessage(
            channel=dm_target,
            text=f"No contact email found for *{account_name}*. Add a contact in Salesforce first.",
        )
        return

    contact_email = primary["email"]
    contact_name = primary.get("name", "")
    contact_title = primary.get("title", "")
    cc_string = ", ".join(c["email"] for c in cc_contacts)

    # Parse SFDC notes
    sfdc_notes = ""
    if notes_df is not None and not notes_df.empty:
        row = notes_df.iloc[0]
        parts = []
        for field, label in [
            ("am_notes", "AM Notes"), ("am_next_steps", "AM Next Steps"),
            ("csm_notes", "CSM Notes"), ("csm_next_steps", "CSM Next Steps"),
        ]:
            val = row.get(field)
            if val and str(val).strip() and str(val).strip().lower() != "none":
                parts.append(f"{label}: {val}")
        sfdc_notes = "\n".join(parts)

    # Parse recent emails
    email_comms = ""
    if emails_df is not None and not emails_df.empty:
        parts = []
        for _, e in emails_df.head(3).iterrows():
            direction = e.get("direction", "")
            date = e.get("email_date", "")
            subject = e.get("subject", "")
            body = str(e.get("body_text", "") or "")[:800]
            parts.append(f"--- {date} ({direction}) ---\nSubject: {subject}\n{body}")
        email_comms = "\n\n".join(parts)

    # ── 4. Build the Claude prompt based on category ──
    first_name = contact_name.split()[0] if contact_name else ""

    # Category-specific framing
    if category == "stale":
        goal = "re-engage a stalled expansion opportunity and get back on the calendar"
        tone_note = "Acknowledge the time gap naturally (don't apologize, just own it)."
    elif category == "followup":
        goal = "follow up after a recent meeting with a clear next step"
        tone_note = "Reference what was discussed in the meeting and propose a concrete next step."
    elif category in ("zero_to_one", "opp_first_spend"):
        goal = (
            "reach out about their new product activation and explore the expansion opportunity. "
            "They just started using a new Ramp product"
        )
        tone_note = "Congratulate on the activation naturally, then pivot to how you can help them get more value."
    elif category == "early_accel":
        goal = (
            "reach out because their usage of this product has been growing recently — "
            "a great time to connect and make sure they're getting the most out of Ramp"
        )
        tone_note = "Lead with their growth on Ramp, then offer to help them optimize. Mention vendor audits, best practices, or setup improvements that could help them."
    elif category == "leading":
        goal = (
            "reach out because their activity on Ramp is picking up — larger transactions or bills "
            "that suggest growing usage. A good time to introduce yourself and offer help"
        )
        tone_note = "Lead with their increased activity. Offer to help them get more value from Ramp — vendor audits, best practices, setup optimization."
    elif category == "first_bill":
        goal = (
            "reach out about their first bill payment in Ramp — they just started using bill pay"
        )
        tone_note = "Congratulate on getting started with bill pay. Offer to help them ramp up and get more value."
    elif category == "sustained_accel":
        goal = (
            "reach out because their usage has been consistently strong — they're clearly getting value "
            "from Ramp and it's a great time to connect and discuss how to optimize further"
        )
        tone_note = "Lead with their sustained growth on Ramp. Offer to help them make the most of it — vendor audits, consolidation, setup improvements."
    elif category == "prospect":
        goal = (
            "reach out to an account whose spend is accelerating — they may need an expansion opp. "
            "There is no open opportunity yet"
        )
        tone_note = "Lead with their success/growth on Ramp, then offer to help them optimize or expand."
    elif category == "prospect_tts_plus_procurement":
        goal = (
            "pitch Ramp Plus or Procurement — this account has a high propensity to adopt these products "
            "based on their usage patterns and profile. Frame it as unlocking more value from their existing Ramp setup"
        )
        tone_note = "Lead with how Plus/Procurement would help them based on their current usage. Don't hard-sell — ask discovery questions."
    elif category == "prospect_high_competitor_spend":
        goal = (
            "consolidate their spend onto Ramp — this account has significant spending on competitor cards, "
            "off-ramp bill pay, or unmanaged travel. Position Ramp as a way to get better visibility, "
            "cashback, and control over all their spend"
        )
        tone_note = "Don't trash competitors. Frame as simplifying their stack and getting better economics on spend they're already doing."
    elif category == "prospect_low_cashback_no_plus":
        goal = (
            "discuss optimizing their Ramp economics — this account is on a low cashback rate and not on Plus. "
            "Plus could unlock better rates and features for their team"
        )
        tone_note = "Lead with the value they're leaving on the table. Frame Plus as an upgrade that pays for itself."
    elif category == "prospect_high_gla_grandfathered":
        goal = (
            "re-engage about their Ramp Plus subscription — they were grandfathered when Plus launched "
            "but didn't convert. With their high GLA balance, treasury features and Plus benefits "
            "could be very valuable. Treasury is uncapped in H1-26"
        )
        tone_note = "Lead with their success on the platform (high balance). Frame Plus as unlocking the next level of value."
    elif category == "prospect_erp_no_billpay":
        goal = (
            "pitch Bill Pay — this account has an integrated ERP/accounting system but isn't using Ramp Bill Pay. "
            "The ERP integration means bill pay would flow directly into their accounting with zero manual work"
        )
        tone_note = "Lead with the accounting integration they already have. Frame bill pay as a natural extension that closes the loop."
    elif category == "prospect_erp_no_plus":
        goal = (
            "pitch Ramp Plus — this account has an integrated ERP but isn't on Plus. "
            "Plus features like advanced accounting rules, custom workflows, and procurement "
            "would supercharge their ERP integration"
        )
        tone_note = "Lead with their ERP usage. Frame Plus as powering up the integration they already invested in."
    elif category == "prospect_active_procurement_trial":
        goal = (
            "convert their active procurement trial into a paid subscription — they're already using "
            "procurement features and seeing value. This is the conversion moment"
        )
        tone_note = "Reference their trial activity. Ask what they've liked, what's missing. Guide toward paid conversion."
    elif category == "prospect_new_treasury":
        goal = (
            "congratulate them on funding their Ramp treasury account and discuss how to maximize "
            "their yield and cash management strategy. Treasury is uncapped in H1-26 — huge opportunity"
        )
        tone_note = "Celebrate the milestone. Mention competitive yields. Ask about their broader cash management goals and if they'd like a walkthrough of treasury features."
    elif category == "prospect_new_investment":
        goal = (
            "congratulate them on opening their Ramp investment account and discuss how to optimize "
            "their investment strategy. This shows strong trust in the platform"
        )
        tone_note = "Celebrate the milestone. Ask about their investment goals and risk profile. Offer to connect them with resources to maximize returns."
    elif category == "prospect_first_bill":
        goal = (
            "congratulate them on creating their first bill on Ramp and help them ramp up bill pay "
            "adoption — the first bill is the hardest, now it's about building the habit"
        )
        tone_note = "Celebrate the milestone. Ask about their AP workflow and how many vendors they plan to move to Ramp. Offer to help optimize their bill pay setup."
    elif category == "close_window":
        goal = (
            "reach out because their Ramp usage is ramping up nicely right now — great time to "
            "connect and make sure they're set up to get the most value as they grow"
        )
        tone_note = "Frame as wanting to help them optimize while they're growing. Be direct about scheduling a quick call this week."
    elif category == "close_now":
        goal = (
            "reach out because they're clearly getting strong value from Ramp — their usage has "
            "been growing and it's a great time to connect and discuss how to maximize that"
        )
        tone_note = "Frame around their success — they're already seeing great results. Push for a call to discuss optimizing further."
    elif category == "reopen":
        goal = (
            "re-engage about their product usage after a previous expansion opp was closed. "
            "The original opp closed 60-120 days ago, and you want to explore opening a new one "
            "based on their current spend patterns"
        )
        tone_note = "Reference their history with the product naturally. Frame it as a check-in, not a re-sell."
    elif category == "treasury_spike":
        goal = (
            "reach out because their treasury balance has grown significantly recently — "
            "a great time to discuss how to optimize their cash management and get more yield from idle funds"
        )
        tone_note = "Lead with their growth/success. Mention treasury optimization and yield opportunities naturally. Be direct about scheduling a quick call."
    elif category in ("underperforming_d30", "underperforming_d60"):
        checkpoint = "D30" if category == "underperforming_d30" else "D60"
        days_left = "60" if category == "underperforming_d30" else "30"
        goal = (
            f"check in on their expansion after a {checkpoint} post-close review shows spend "
            f"is tracking below target. You have {days_left} days left in the 90-day window "
            f"to help them ramp up. The goal is to re-engage them and drive activation"
        )
        tone_note = (
            "Frame as a check-in to help them get more value from Ramp, not a complaint about low usage. "
            "Ask what blockers they're facing and offer enablement support."
        )
    elif category == "multi_product":
        goal = (
            "reach out about multiple expansion opportunities at once — this account has signals "
            "across multiple product categories. Bundle the conversation to show the full value of expanding "
            "their Ramp usage across products"
        )
        tone_note = "Lead with the breadth of their Ramp usage. Frame expansion as a holistic play, not separate asks."
    elif category == "post_meeting_opp":
        goal = (
            "follow up after a recent call where expansion products were discussed. "
            "There is no open opportunity for this product yet — you want to explore creating one"
        )
        tone_note = "Reference what was discussed on the call. Propose a follow-up to scope the expansion."
    else:
        goal = "re-engage and get back on the calendar"
        tone_note = ""

    met_with_note = ""
    if contact_email.lower() in gong_participants:
        met_with_note = f"You have met with {contact_name} before (on Gong calls). Reference your past conversations."
    else:
        met_with_note = f"You have NOT met with {contact_name} before. This is a warm intro — reference the account context."

    # Determine opening based on whether the owner has met this contact
    if contact_email.lower() in gong_participants:
        opening_instruction = (
            f"{first_name_owner} HAS met {first_name or contact_name} before. "
            f"Open with a natural reference to your prior conversation (e.g. 'Great connecting last time' or 'Following up from our call'). "
            f"Do NOT say 'great to meet you'."
        )
    else:
        opening_instruction = (
            f"{first_name_owner} has NOT met {first_name or contact_name} before. "
            f"Open with: 'Hi {first_name or contact_name}, great to meet you! I was recently assigned as your Account Manager for the team and I wanted to reach out and make an intro.'"
        )

    cc_context = ""
    if cc_contacts:
        cc_names = ", ".join(
            f"{c.get('name', '')} ({c.get('title', '')})" for c in cc_contacts
        )
        cc_context = f"\nCC'd: {cc_names}\nThis email will also CC additional stakeholders. Address the TO contact by name but write for a broader audience — avoid language that only makes sense to one person."

    logger.info("Draft %s: contact selection done in %.1fs", account_name, _time.time() - _t1)
    _t2 = _time.time()

    prompt = f"""You are helping {owner_name}, a Growth Account Manager at Ramp, write an outreach email.

GOAL: {goal}

Account: {account_name}
Product: {product or 'Expansion'}
Contact: {contact_name} ({contact_title}){cc_context}

SFDC Account Notes:
{sfdc_notes if sfdc_notes else 'No notes on file'}

Recent Gong Calls:
{gong_context if gong_context else 'No recent calls found'}

Product requests from calls:
{product_requests if product_requests else 'None'}

Competitors mentioned:
{competitors if competitors else 'None'}

Recent Email History:
{email_comms if email_comms else 'No recent emails'}

Write an outreach email from {first_name_owner} to {first_name or 'the contact'}. {tone_note}

OPENING: {opening_instruction}

BODY — Blend two things:

1. AM VALUE PROP: Position {first_name_owner} as a helpful resource. Mention specific ways the AM can help: vendor audits to find cashback opportunities, best practices for their Ramp setup, help consolidating spend, optimizing workflows, or getting more out of products they're already using. Frame these around THEIR benefit, not yours.

2. SIGNAL CONTEXT: Tie the outreach to what you're actually seeing on their account — the spend trend, the product activation, the bill pay growth, whatever triggered this email. Be specific: reference real numbers, products, or patterns from the data above. This makes the email feel relevant and timely, not generic.

If there's Gong call or email context, weave it in naturally — reference topics discussed, pain points raised, or questions asked. If competitors were mentioned, acknowledge them naturally and position how Ramp addresses the gap.

End with a clear, specific CTA. Push for a 15-minute call. Include booking link: <a href="{booking_link}">this link</a>

SIGN OFF: "Best,<br>{first_name_owner}"

Rules:
- Tone: warm, direct, helpful — like a trusted advisor, not a sales template
- Contractions are natural ("I'd", "you're", "we'll")
- 100-150 words. Every word earns its spot.
- Return ONLY the email body as HTML (use <p>, <strong>, <ul>/<li>, <a>, <br> tags)
- Do NOT include a subject line — it will be generated separately
- Do NOT say "I was notified", "I was alerted", or anything implying an automated alert triggered this email
- Do NOT guilt-trip about unanswered outbound emails ("I noticed you haven't replied"). If there's an open thread, pick it up naturally without calling out the silence.
- If Gong call context is marked as "[Other Ramp team's call]", do NOT say "we discussed" or "following up from our call." Say "I know the team has been in touch" or reference topics without claiming you were there.
- If there is genuinely no context (no calls, no emails, no notes), write a brief AM intro asking to connect."""

    # ── 5. Search for existing thread + generate email body in parallel ──
    thread_info = None
    _thread_categories = ("stale", "followup", "reopen", "close_window", "close_now")

    if owner_email and category in _thread_categories:
        # Run thread search and Claude call in parallel
        from concurrent.futures import ThreadPoolExecutor as _TP
        with _TP(max_workers=2) as tp:
            thread_future = tp.submit(
                _find_existing_thread, contact_email, product, owner_email, user_id
            )
            claude_future = tp.submit(call_claude, prompt, 1024)
            try:
                thread_info = thread_future.result()
                if thread_info:
                    logger.info("Found existing thread for %s: %s", account_name, thread_info["subject"])
            except Exception as e:
                logger.debug("Thread search failed: %s", e)
            body_text = claude_future.result()
    else:
        body_text = call_claude(prompt, max_tokens=1024)

    logger.info("Draft %s: Claude + thread search done in %.1fs", account_name, _time.time() - _t2)

    # ── 5b. Generate subject line ──
    if thread_info:
        orig_subject = thread_info["subject"]
        subject = orig_subject if orig_subject.lower().startswith("re:") else f"Re: {orig_subject}"
    else:
        # Dynamic subject based on context
        subj_parts = [f"Ramp {product or 'Follow-Up'}"]
        if last_call_name:
            subj_parts = [f"Ramp Follow-Up — {last_call_name[:40]}"]
        elif product_requests:
            topic = product_requests.split("|")[0].strip()[:40]
            subj_parts = [f"Ramp — {topic}"]
        subject = subj_parts[0]

    # ── 6. Build HTML + send draft ──
    try:
        from templates.signature import build_signature
        sig_html = build_signature(user_id=user_id)
    except ImportError:
        sig_html = ""

    # Find relevant help articles based on context
    from templates.help_links import find_relevant_links, format_links_for_email
    context_text = f"{gong_context} {sfdc_notes} {email_comms} {product or ''}"
    relevant_links = find_relevant_links(context_text, max_links=4)
    links_html = format_links_for_email(relevant_links)

    html_body = f"""<div style="font-family:Arial,sans-serif;font-size:14px;color:#000;max-width:600px;">
<!-- claude-auto-draft -->
{body_text}
{f'<br>{links_html}' if links_html else ''}
<br>
{sig_html}
</div>"""

    # Pick Gmail label based on draft category
    _POST_MEETING_CATEGORIES = {"followup", "post_meeting_opp"}
    draft_label = (
        "Claude Drafts/Post Meeting" if category in _POST_MEETING_CATEGORIES
        else "Claude Drafts/Prospecting"
    )

    # When replying to a thread, use the thread's recipients
    draft_to = contact_email
    draft_cc = cc_string
    if thread_info:
        # Use the thread's last message recipients to maintain continuity
        thread_to = thread_info.get("to", "")
        thread_cc = thread_info.get("cc", "")
        if thread_to and contact_email.lower() in thread_to.lower():
            draft_to = contact_email  # Keep our selected primary contact as TO
            # Merge thread CC with our CC, dedup
            all_cc = set()
            for addr in (thread_cc or "").split(","):
                addr = addr.strip()
                if addr and "@" in addr and owner_email.lower() not in addr.lower() and contact_email.lower() not in addr.lower():
                    all_cc.add(addr)
            for addr in (cc_string or "").split(","):
                addr = addr.strip()
                if addr and "@" in addr:
                    all_cc.add(addr)
            draft_cc = ", ".join(all_cc)

    draft_id = ""
    if gumstack_ok():
        # Build draft arguments
        draft_kwargs = {
            "to": draft_to,
            "subject": subject,
            "html_body": html_body,
            "cc": draft_cc,
            "label": draft_label,
            "user_id": user_id,
        }

        result = gumstack_create(**draft_kwargs)
        if result["success"]:
            draft_id = result["draft_id"]
        else:
            logger.warning("Gumstack draft failed for %s, falling back to queue", account_name)

    if not draft_id:
        from utils.pending_drafts import save_draft as save_pending_draft
        draft_id = f"pending_{account_id}_{int(time.time())}"
        save_pending_draft(
            draft_id=draft_id, to=contact_email, cc=cc_string,
            subject=subject, html_body=html_body,
            account_name=account_name,
            label=draft_label,
            user_id=user_id or "",
        )

    # ── 7. Send confirmation DM ──
    _CATEGORY_LABELS = {
        "early_accel": "Early Acceleration Outreach",
        "close_window": "Close Window Outreach",
        "close_now": "Close Now Outreach",
        "leading": "Leading Indicator Outreach",
        "first_bill": "First Bill Outreach",
        "zero_to_one": "Zero-to-One Outreach",
        "sustained_accel": "Sustained Acceleration Outreach",
        "stale": "Re-Engage",
        "followup": "Follow-Up",
        "reopen": "Re-open Outreach",
        "post_meeting_opp": "Post-Meeting Outreach",
    }
    drafter_type = _CATEGORY_LABELS.get(category, "Outreach")

    def _contact_tag(email, name, title):
        """Build a display string with engagement signals."""
        parts = []
        if title:
            parts.append(title)
        if email.lower() in gong_participants:
            parts.append("met on Gong")
        if email.lower() in email_correspondents:
            parts.append("recent emails")
        tag = " · ".join(parts) if parts else "SFDC contact"
        return f"{name} <{email}> — _{tag}_"

    to_line = f"*To:* {_contact_tag(contact_email, contact_name, contact_title)}"
    cc_line = ""
    if cc_contacts:
        cc_entries = [
            _contact_tag(c["email"], c.get("name", ""), c.get("title", ""))
            for c in cc_contacts
        ]
        cc_line = "\n*CC:* " + "\n       ".join(cc_entries)

    preview_text = re.sub(r'<br\s*/?>', '\n', body_text)
    preview_text = re.sub(r'<[^>]+>', '', preview_text)

    thread_line = ""
    if thread_info:
        orig_subj = thread_info["subject"]
        if thread_info.get("owner_sent_last"):
            thread_line = f"\n:thread: *Replying to thread:* _{orig_subj}_ (you sent last — following up)"
        else:
            thread_line = f"\n:thread: *Replying to thread:* _{orig_subj}_ (they replied — responding)"

    details = (
        f"{to_line}{cc_line}\n"
        f"*Subject:* {subject}\n"
        f"*Account:* {account_name} — {product or 'Expansion'}"
        f"{thread_line}\n\n"
        f"*Preview:*\n{preview_text[:500]}"
    )

    blocks = drafter_confirmation_blocks(
        drafter_type=drafter_type,
        account_name=account_name,
        details=details,
        draft_id=draft_id,
    )
    client.chat_postMessage(
        channel=dm_target,
        blocks=blocks,
        text=f"{drafter_type} draft ready for {account_name}  ({_time.time() - _t0:.0f}s)",
    )
