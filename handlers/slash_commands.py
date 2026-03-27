"""Slash command handlers for Gary Bot."""

import logging
import threading
import pandas as pd
from core.snowflake_client import run_query
from core.claude_client import call_claude
from core.slack_formatter import (
    sf_account_url, sf_opp_url, format_currency, simple_dm_blocks,
    dashboard_url, EXPANSION_PRODUCT_MAP,
)
from core.email_context import get_email_context, format_email_context_line, format_email_context_block
from queries.queries import ACCOUNT_LOOKUP_QUERY, ACCOUNT_OPPS_QUERY, GONG_MEETINGS_QUERY, format_query
from core.user_registry import is_registered, get_user
from utils.account_resolver import fetch_contact_emails
from config import GREG_SLACK_ID, NTR_RATES

logger = logging.getLogger(__name__)


def register_slash_commands(app):
    """Register all slash commands with the Bolt app."""

    def _check_user(command, respond=None):
        uid = command.get("user_id", "")
        if not is_registered(uid):
            if respond:
                respond("You're not registered with Gary Bot yet. Open the *Home* tab to get started.")
            return None
        return get_user(uid)

    @app.command("/gary-lookup")
    def handle_gary_lookup(ack, command, client, respond):
        """Account snapshot: products, spend, opps, contacts."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        search_term = command.get("text", "").strip()
        if not search_term:
            respond("Usage: `/gary-lookup <account name>`")
            return

        try:
            # Sanitize search term for SQL LIKE
            safe_term = search_term.replace("'", "''").replace("%", "\\%")
            df = run_query(format_query(ACCOUNT_LOOKUP_QUERY, user_id=command["user_id"], search_term=safe_term))

            if df.empty:
                respond(f"No accounts found matching *{search_term}*")
                return

            blocks = [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": f"Account Lookup: {search_term}"}
                }
            ]

            for _, row in df.head(5).iterrows():
                acct_name = row.get("account_name", "")
                account_id = row.get("account_id", "")
                sf_link = sf_account_url(account_id)

                # Product status
                products = []
                if pd.notna(row.get("card_activated_at")):
                    products.append(f"Card ({row['card_activated_at']})")
                if pd.notna(row.get("billpay_first_paid_at")):
                    products.append(f"Bill Pay ({row['billpay_first_paid_at']})")
                if pd.notna(row.get("treasury_activated_at")):
                    products.append(f"Treasury ({row['treasury_activated_at']})")
                if pd.notna(row.get("travel_activated_at")):
                    products.append(f"Travel ({row['travel_activated_at']})")

                products_text = ", ".join(products) if products else "None activated"

                # Spend
                card_l30 = format_currency(float(row.get("card_l30d", 0) or 0))
                bp_l30 = format_currency(float(row.get("billpay_l30d", 0) or 0))
                treasury_l30 = format_currency(float(row.get("treasury_l30d", 0) or 0))

                lines = [
                    f"*<{sf_link}|{acct_name}>*",
                    f"Products: {products_text}",
                    f"L30D Spend — Card: {card_l30} | Bill Pay: {bp_l30} | Treasury: {treasury_l30}",
                ]

                # Fetch contacts
                try:
                    contacts = fetch_contact_emails(None, [account_id])
                    acct_contacts = contacts.get(account_id, [])
                    if acct_contacts:
                        contact_lines = []
                        for c in acct_contacts[:3]:
                            c_line = c.get("name", "")
                            if c.get("title"):
                                c_line += f" ({c['title']})"
                            if c.get("email"):
                                c_line += f" — {c['email']}"
                            contact_lines.append(c_line)
                        lines.append("Contacts: " + " | ".join(contact_lines))
                except Exception:
                    pass

                # Email context
                try:
                    email_ctx = get_email_context(account_id, acct_name, days=30, user_id=command["user_id"])
                    if email_ctx.get("email_count", 0) > 0:
                        lines.append(f"\u2709\ufe0f {format_email_context_line(email_ctx)}")
                except Exception:
                    pass

                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(lines)}
                })
                blocks.append({"type": "divider"})

            respond(blocks=blocks)

        except Exception as e:
            logger.error("gary-lookup failed: %s", e)
            respond(f"Error looking up *{search_term}*: {e}")

    @app.command("/gary-opps")
    def handle_gary_opps(ack, command, client, respond):
        """Open opp summary with CP values."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return

        try:
            df = run_query(format_query(ACCOUNT_OPPS_QUERY, user_id=command["user_id"]))

            if df.empty:
                respond("No open expansion opps found.")
                return

            blocks = [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": f"Open Opps ({len(df)})"}
                }
            ]

            # Group by product type
            by_product = {}
            for _, row in df.iterrows():
                product = row.get("expansion_subtype", "Other")
                if product not in by_product:
                    by_product[product] = []
                by_product[product].append(row)

            for product, opps in by_product.items():
                ntr = NTR_RATES.get(product, 0.0095)
                lines = [f"*{product}* ({len(opps)} opps)"]
                total_monthly = 0

                for row in opps[:10]:
                    acct = row.get("account_name", "")
                    monthly = float(row.get("monthly_expansion_amount", 0) or 0)
                    total_monthly += monthly
                    stage = row.get("opportunity_stage_name", "")
                    days = int(row.get("days_open", 0) or 0)
                    sf_link = sf_opp_url(row.get("opportunity_id", ""))
                    lines.append(
                        f"  <{sf_link}|{acct}> — {format_currency(monthly)}/mo "
                        f"| {stage} | {days}d open"
                    )

                if len(opps) > 10:
                    lines.append(f"  _...and {len(opps) - 10} more_")

                est_cp = format_currency(total_monthly * ntr)
                lines.append(f"  Total monthly: {format_currency(total_monthly)} (est CP: {est_cp})")

                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(lines)}
                })

            respond(blocks=blocks)

        except Exception as e:
            logger.error("gary-opps failed: %s", e)
            respond(f"Error loading opps: {e}")

    @app.command("/gary-brief")
    def handle_gary_brief(ack, command, client, respond):
        """Pre-call brief for an account."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        search_term = command.get("text", "").strip()
        if not search_term:
            respond("Usage: `/gary-brief <account name>`")
            return

        try:
            respond(f"Generating brief for *{search_term}*... (this may take 15-30 seconds)")

            # Look up account
            safe_term = search_term.replace("'", "''").replace("%", "\\%")
            acct_df = run_query(format_query(ACCOUNT_LOOKUP_QUERY, user_id=command["user_id"], search_term=safe_term))

            if acct_df.empty:
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"No accounts found matching *{search_term}*"
                )
                return

            row = acct_df.iloc[0]
            account_id = row["account_id"]
            account_name = row["account_name"]
            sf_link = sf_account_url(account_id)

            # Get recent calls
            calls_df = run_query(format_query(GONG_MEETINGS_QUERY, user_id=command["user_id"], lookback_days=90))
            acct_calls = calls_df[calls_df["account_id"] == account_id].head(3)

            calls_text = ""
            for _, call in acct_calls.iterrows():
                calls_text += (
                    f"- {call.get('call_name', '')} ({call.get('call_date', '')}): "
                    f"{str(call.get('full_section_text', ''))[:500]}\n"
                )

            # Get contacts
            contacts = fetch_contact_emails(None, [account_id])
            acct_contacts = contacts.get(account_id, [])
            contacts_text = "\n".join(
                f"- {c.get('name', '')} ({c.get('title', '')}) — {c.get('email', '')}"
                for c in acct_contacts[:5]
            ) if acct_contacts else "No contacts found"

            # Get open opps
            opps_df = run_query(format_query(ACCOUNT_OPPS_QUERY, user_id=command["user_id"]))
            acct_opps = opps_df[opps_df["account_id"] == account_id] if not opps_df.empty else pd.DataFrame()
            opps_text = "\n".join(
                f"- {r['expansion_subtype']} ({r['opportunity_stage_name']}) — "
                f"{format_currency(float(r.get('monthly_expansion_amount', 0) or 0))}/mo"
                for _, r in acct_opps.iterrows()
            ) if not acct_opps.empty else "None"

            # Spend data
            card_l30 = format_currency(float(row.get("card_l30d", 0) or 0))
            bp_l30 = format_currency(float(row.get("billpay_l30d", 0) or 0))
            treasury_l30 = format_currency(float(row.get("treasury_l30d", 0) or 0))

            # Email context (expanded — all Ramp employee emails)
            email_ctx = get_email_context(account_id, account_name, days=30, user_id=command["user_id"])
            email_text = format_email_context_block(email_ctx, user_id=command["user_id"])

            # Seasonal spend (12-month history for trend intelligence)
            seasonal_text = ""
            try:
                business_id = row.get("business_id", "")
                if business_id:
                    seasonal_df = run_query(f"""
                    SELECT
                        DATE_TRUNC('month', tpv.date_day)::date AS month,
                        ROUND(SUM(tpv.card_tpv)) AS card,
                        ROUND(SUM(tpv.billpay_tpv)) AS billpay,
                        ROUND(SUM(tpv.travel_tpv)) AS travel,
                        ROUND(AVG(tpv.treasury_available_balance)) AS treasury
                    FROM analytics.metrics.fct_daily_business__multiproduct_tpv tpv
                    WHERE tpv.business_id = '{business_id}'
                      AND tpv.date_day >= DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE))
                    GROUP BY 1 ORDER BY 1
                    """)
                    if not seasonal_df.empty:
                        months = []
                        for _, m in seasonal_df.iterrows():
                            months.append(
                                f"  {m.get('month', '')}: Card {format_currency(float(m.get('card', 0) or 0))} | "
                                f"BP {format_currency(float(m.get('billpay', 0) or 0))} | "
                                f"Treasury {format_currency(float(m.get('treasury', 0) or 0))}"
                            )
                        seasonal_text = "6-MONTH SPEND TREND:\n" + "\n".join(months)
            except Exception:
                pass  # Non-critical — brief still works without seasonal data

            prompt = f"""Generate a concise pre-call brief for {user["sf_owner_name"]}'s meeting with {account_name}.

ACCOUNT DATA:
- L30D Card: {card_l30} | Bill Pay: {bp_l30} | Treasury: {treasury_l30}

{seasonal_text}

OPEN OPPS:
{opps_text}

KEY CONTACTS:
{contacts_text}

RECENT CALLS:
{calls_text if calls_text else 'No recent calls'}

RECENT EMAIL ACTIVITY (all Ramp employees, not just {user["first_name"]}):
{email_text}

Write a 200-word brief covering:
1. Account snapshot (what products they use, spend level, and any trend — growing, flat, declining)
2. Current opp status and what to push for
3. Key talking points based on recent calls and email threads
4. If other Ramp teams (CSM, Support, Deal Desk) are active on the account, note what they're doing
5. If there are pain point signals or unanswered emails, flag them as priority
6. Specific ask or close to attempt on this call

Be direct and specific. This is for {user["first_name"]} to glance at 2 minutes before the call."""

            brief = call_claude(prompt, max_tokens=800)

            prep_link = dashboard_url("meeting-prep", account=account_name)
            lines = [
                f"*<{sf_link}|{account_name}>*",
                f"Card: {card_l30} | BP: {bp_l30} | Treasury: {treasury_l30}\n",
                brief,
                f"\n<{prep_link}|Full Meeting Prep in Dashboard>",
            ]

            blocks = simple_dm_blocks(f"Pre-Call Brief: {account_name}", "\n".join(lines))
            client.chat_postMessage(
                channel=command["user_id"],
                blocks=blocks,
                text=f"Brief for {account_name}",
            )

        except Exception as e:
            logger.error("gary-brief failed: %s", e)
            client.chat_postMessage(
                channel=command["user_id"],
                text=f"Error generating brief for *{search_term}*: {e}",
            )

    # ── Intelligence job slash commands ─────────────────────────────────

    @app.command("/pipeline-cleanup")
    def handle_pipeline_cleanup(ack, command, client, respond):
        """On-demand pipeline cleanup analysis."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Running pipeline cleanup — this may take a minute...")

        def _run():
            try:
                from jobs.pipeline_cleanup import run_pipeline_cleanup
                run_pipeline_cleanup(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("pipeline-cleanup command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Pipeline cleanup failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/post-meeting")
    def handle_post_meeting(ack, command, client, respond):
        """On-demand post-meeting check (Granola-first, Gong fallback).

        Usage:
            /post-meeting              — check Granola for recent calls, fall back to Gong
            /post-meeting 7            — all calls in last 7 days (Gong batch)
            /post-meeting ondeck       — latest call for a specific account
            /post-meeting ondeck 7     — latest call for account in last 7 days
        """
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        text = command.get("text", "").strip()

        lookback_days = 2
        account_filter = ""

        if text:
            parts = text.split()
            # Check if last part is a number (lookback days)
            if parts[-1].isdigit():
                lookback_days = max(1, min(int(parts[-1]), 30))
                account_filter = " ".join(parts[:-1])
            elif parts[0].isdigit():
                lookback_days = max(1, min(int(parts[0]), 30))
                account_filter = " ".join(parts[1:])
            else:
                # All text is account name
                account_filter = text

        if account_filter:
            respond(f"Checking Granola + Gong for *{account_filter}* (last {lookback_days} days)...")
        else:
            respond("Checking Granola for recent meetings (falling back to Gong)...")

        def _run():
            try:
                if account_filter:
                    # Account-specific: try Granola first, then Gong
                    _handle_post_meeting_account(client, command["user_id"], account_filter, lookback_days, query_user_id=command["user_id"])
                else:
                    # Try Granola first (last 60 min)
                    from jobs.granola_followup import run_granola_followup
                    run_granola_followup(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("post-meeting command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Post-meeting check failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/opp-pacing")
    def handle_opp_pacing(ack, command, client, respond):
        """On-demand opp pacing report. Accepts optional account name filter."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        account_name = command.get("text", "").strip() or None

        if account_name:
            respond(f"Running opp pacing for *{account_name}*...")
        else:
            respond("Running opp pacing report — this may take a minute...")

        def _run():
            try:
                from jobs.opp_pacing import run_opp_pacing
                run_opp_pacing(client, user_id=command["user_id"], account_name=account_name, force=True)
            except Exception as e:
                logger.error("opp-pacing command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Opp pacing failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/zero-to-one")
    def handle_zero_to_one(ack, command, client, respond):
        """On-demand zero-to-one activation alert."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Checking zero-to-one activations — this may take a minute...")

        def _run():
            try:
                from jobs.zero_to_one import run_zero_to_one
                run_zero_to_one(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("zero-to-one command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Zero-to-one check failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/forecast")
    def handle_forecast(ack, command, client, respond):
        """On-demand weekly forecast refresh."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Running forecast — this may take a minute...")

        def _run():
            try:
                from jobs.forecasting import run_forecasting
                run_forecasting(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("forecast command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Forecast failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    # ── Status, help, test, and morning brief commands ────────────────

    @app.command("/gary-status")
    def handle_gary_status(ack, command, client, respond):
        """Health check: connections, schedules, dedup state."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Running status check...")

        def _run():
            try:
                from jobs.status import run_status
                run_status(client, user_id=command["user_id"])
            except Exception as e:
                logger.error("gary-status failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Status check failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/gary-help")
    def handle_gary_help(ack, command, client, respond):
        """Full capability listing with examples."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return

        def _run():
            try:
                from jobs.status import run_help
                run_help(client, user_id=command["user_id"])
            except Exception as e:
                logger.error("gary-help failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Help failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/gary-test")
    def handle_gary_test(ack, command, client, respond):
        """Run all jobs in test mode to verify everything works."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Starting full test suite — this will take 1-2 minutes. Results coming via DM...")

        def _run():
            try:
                from jobs.status import run_test
                run_test(client, user_id=command["user_id"])
            except Exception as e:
                logger.error("gary-test failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Test suite failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/morning-brief")
    def handle_morning_brief(ack, command, client, respond):
        """On-demand morning brief — combined daily action summary."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Generating morning brief...")

        def _run():
            try:
                from jobs.morning_brief import run_morning_brief
                run_morning_brief(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("morning-brief command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Morning brief failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/priorities")
    def handle_priorities(ack, command, client, respond):
        """Priority actions — the single ranked list of what to do now."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Building priority actions — this may take 30-60 seconds...")

        def _run():
            try:
                from jobs.priority_actions import run_priority_actions
                run_priority_actions(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("priorities command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Priority actions failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    # ── New job slash commands ─────────────────────────────────────────

    @app.command("/quota-heartbeat")
    def handle_quota_heartbeat(ack, command, client, respond):
        """On-demand quota heartbeat — CP attainment + accelerator band."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Running quota heartbeat...")

        def _run():
            try:
                from jobs.quota_heartbeat import run_quota_heartbeat
                run_quota_heartbeat(client, user_id=command["user_id"])
            except Exception as e:
                logger.error("quota-heartbeat command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Quota heartbeat failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/spend-pacing")
    def handle_spend_pacing(ack, command, client, respond):
        """On-demand spend pacing — MTD vs last month, YoY, trajectory."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Running spend pacing analysis...")

        def _run():
            try:
                from jobs.spend_pacing import run_spend_pacing
                run_spend_pacing(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("spend-pacing command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Spend pacing failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/post-close")
    def handle_post_close(ack, command, client, respond):
        """On-demand post-close CP monitor — activation + baseline tracking."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Running post-close CP monitor...")

        def _run():
            try:
                from jobs.post_close_monitor import run_post_close_monitor
                run_post_close_monitor(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("post-close command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Post-close monitor failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/activity-report")
    def handle_activity_report(ack, command, client, respond):
        """On-demand activity report — SQLs created + opps closed by product."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Running activity report...")

        def _run():
            try:
                from jobs.activity_report import run_activity_report
                run_activity_report(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("activity-report command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Activity report failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/top-accounts")
    def handle_top_accounts(ack, command, client, respond):
        """On-demand account tiering — Top 50 ranked by CP potential."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Running account tiering — this may take a minute...")

        def _run():
            try:
                from jobs.account_tiering import run_account_tiering
                run_account_tiering(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("top-accounts command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Account tiering failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/batch-outreach")
    def handle_batch_outreach(ack, command, client, respond):
        """On-demand batch outreach — cluster accounts + draft campaigns."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Building batch outreach campaigns...")

        def _run():
            try:
                from jobs.batch_outreach import run_batch_outreach
                run_batch_outreach(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("batch-outreach command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Batch outreach failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/nudge")
    def handle_nudge(ack, command, client, respond):
        """On-demand proactive nudge — what's new + highest-value suggestions."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        respond("Checking for new signals...")

        def _run():
            try:
                from jobs.proactive_nudge import run_proactive_nudge
                run_proactive_nudge(client, user_id=command["user_id"], force=True)
            except Exception as e:
                logger.error("nudge command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Nudge check failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/bill-drafter")
    def handle_bill_drafter(ack, command, client, respond):
        """On-demand bill drafter sweep. Accepts optional lookback: 2h (default), 24h, 7d."""
        ack()
        user = _check_user(command, respond)
        if not user:
            return
        text = command.get("text", "").strip().lower()

        # Parse lookback window
        lookback_hours = 2.0
        if text:
            if text.endswith("h"):
                try:
                    lookback_hours = float(text[:-1])
                except ValueError:
                    respond(f"Usage: `/bill-drafter [2h|24h|7d]`. Got: `{text}`")
                    return
            elif text.endswith("d"):
                try:
                    lookback_hours = float(text[:-1]) * 24
                except ValueError:
                    respond(f"Usage: `/bill-drafter [2h|24h|7d]`. Got: `{text}`")
                    return
            else:
                respond(f"Usage: `/bill-drafter [2h|24h|7d]`. Got: `{text}`")
                return

        lookback_hours = max(0.5, min(lookback_hours, 168.0))  # 30 min to 7 days
        respond(f"Sweeping #alerts-card-payable-bills (last {lookback_hours:.0f}h)...")

        def _run():
            try:
                from handlers.channel_monitors import run_bill_drafter_sweep
                processed = run_bill_drafter_sweep(client, user_id=command["user_id"], lookback_hours=lookback_hours)
                if processed == 0:
                    client.chat_postMessage(
                        channel=command["user_id"],
                        text=f"No new unprocessed alerts found in the last {lookback_hours:.0f}h.",
                    )
                else:
                    client.chat_postMessage(
                        channel=command["user_id"],
                        text=f"Bill drafter complete — {processed} draft{'s' if processed != 1 else ''} created.",
                    )
            except Exception as e:
                logger.error("bill-drafter command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Bill drafter failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    @app.command("/opp")
    def handle_opp(ack, command, client, respond):
        """Quick opp creation: /opp <account> <product> <amount> [| next step | notes]

        Accepts natural language, e.g.:
          /opp ondeck card 50k
          /opp ondeck card 50k | send contract by Friday
          /opp ondeck card 50k | send contract | consolidating AP from 3 vendors
        """
        ack()
        user = _check_user(command, respond)
        if not user:
            logger.info("/opp rejected — user=%s not registered", command.get("user_id"))
            return
        text = command.get("text", "").strip()
        if not text:
            respond(
                "Usage: `/opp <account> <product> <amount> [| next step | notes]`\n"
                "Example: `/opp ondeck card 50k | send contract by Friday | consolidating AP`\n"
                "Products: card, bill pay (bp), treasury, travel, saas, procurement"
            )
            return

        respond("Resolving account and building opp link...")

        def _run():
            try:
                _handle_opp_creation(text, command["user_id"], client)
            except Exception as e:
                logger.error("opp command failed: %s", e)
                client.chat_postMessage(
                    channel=command["user_id"],
                    text=f"Opp creation failed: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()


def _parse_opp_input(text):
    """Parse opp input into structured fields.

    Returns {account_query, product_type, amount, next_step, notes} or None.

    Format: <account> <product> <amount> [| next step [| notes]]
    Pipe-delimited optional fields. Labeled prefixes (next step:, notes:) are
    stripped automatically so both of these work:
      ondeck card 50k | send contract | consolidating AP
      ondeck card 50k | next step: send contract | notes: consolidating AP
    """
    import re

    # Split on pipes first to extract optional fields
    parts = [p.strip() for p in text.split("|")]
    core_text = parts[0]
    next_step = ""
    notes = ""

    if len(parts) >= 2:
        next_step = re.sub(r'^next\s*steps?\s*:\s*', '', parts[1], flags=re.IGNORECASE).strip()
    if len(parts) >= 3:
        notes = re.sub(r'^notes?\s*:\s*', '', parts[2], flags=re.IGNORECASE).strip()
    # If 4+ pipes, append extra to notes
    if len(parts) > 3:
        notes += " | " + " | ".join(parts[3:])

    # Normalize product aliases
    _PRODUCT_ALIASES = {
        "card": "Card Expansion",
        "cards": "Card Expansion",
        "bill pay": "Bill Pay Expansion",
        "billpay": "Bill Pay Expansion",
        "bp": "Bill Pay Expansion",
        "treasury": "Treasury Expansion",
        "rba": "Treasury Expansion",
        "travel": "Travel Expansion",
        "saas": "SaaS",
        "sas": "SaaS",
        "plus": "SaaS",
        "f2p": "SaaS",
        "free to paid": "SaaS",
        "procurement": "Procurement",
        "proc": "Procurement",
    }

    text_lower = core_text.lower().strip()

    # 1. Extract amount (look for patterns like $50k, 50000, 2m, $25,000)
    amount = 0
    amount_pattern = r'\$?([\d,]+\.?\d*)\s*(k|m|mm)?'
    amount_matches = list(re.finditer(amount_pattern, text_lower))

    if amount_matches:
        # Use the last number match (amount usually comes last)
        m = amount_matches[-1]
        raw = float(m.group(1).replace(",", ""))
        suffix = m.group(2) or ""
        if suffix == "k":
            amount = raw * 1_000
        elif suffix in ("m", "mm"):
            amount = raw * 1_000_000
        else:
            amount = raw
        # Remove amount from text for product/account parsing
        text_lower = text_lower[:m.start()] + text_lower[m.end():]

    # 2. Extract product type (match longest alias first)
    product_type = ""
    sorted_aliases = sorted(_PRODUCT_ALIASES.keys(), key=len, reverse=True)
    for alias in sorted_aliases:
        pattern = r'\b' + re.escape(alias) + r'\b'
        match = re.search(pattern, text_lower)
        if match:
            product_type = _PRODUCT_ALIASES[alias]
            text_lower = text_lower[:match.start()] + text_lower[match.end():]
            break

    # 3. Strip trailing next-step shorthands (if no pipe was used)
    _NEXT_STEP_SHORTHANDS = {
        "cw": "CW",
        "close win": "CW",
        "sql": "SQL",
        "follow up": "Follow up",
        "followup": "Follow up",
        "fu": "Follow up",
        "send proposal": "Send proposal",
        "send contract": "Send contract",
        "schedule call": "Schedule call",
        "impl": "Schedule implementation",
    }
    remaining = re.sub(r'\s+', ' ', text_lower).strip().strip("-/,.")

    if not next_step:
        # Check if trailing words match a shorthand
        sorted_sh = sorted(_NEXT_STEP_SHORTHANDS.keys(), key=len, reverse=True)
        for sh in sorted_sh:
            if remaining.endswith(" " + sh) or remaining == sh:
                next_step = _NEXT_STEP_SHORTHANDS[sh]
                remaining = remaining[:-(len(sh))].strip().strip("-/,.")
                break

    account_query = remaining

    if not account_query:
        return None

    return {
        "account_query": account_query,
        "product_type": product_type or "Card Expansion",  # Default to card
        "amount": amount,
        "next_step": next_step,
        "notes": notes,
    }


def _handle_opp_creation(text, user_id, client):
    """Parse input, resolve account, build pre-filled SF opp link, and DM."""
    from utils.account_resolver import resolve_account_name

    parsed = _parse_opp_input(text)
    if not parsed:
        client.chat_postMessage(
            channel=user_id,
            text=(
                "Couldn't parse that. Try: `/opp <account name> <product> <amount>`\n"
                "Example: `/opp ondeck card 50k`"
            ),
        )
        return

    account_query = parsed["account_query"]
    product_type = parsed["product_type"]
    amount = parsed["amount"]
    next_step = parsed["next_step"]
    user_notes = parsed["notes"]

    # Resolve account in Snowflake
    result = resolve_account_name(None, account_query)
    if not result:
        client.chat_postMessage(
            channel=user_id,
            text=(
                f"No SFDC account found matching *{account_query}*.\n"
                f"Try a more specific name or check spelling."
            ),
        )
        return

    account_id = result["account_id"]
    account_name = result["account_name"]

    # Get L30D spend for context
    spend_context = ""
    try:
        safe_name = account_name.replace("'", "''").replace("%", "\\%")
        acct_df = run_query(format_query(ACCOUNT_LOOKUP_QUERY, user_id=user_id, search_term=safe_name))
        if not acct_df.empty:
            row = acct_df.iloc[0]
            card = float(row.get("card_l30d", 0) or 0)
            bp = float(row.get("billpay_l30d", 0) or 0)
            treasury = float(row.get("treasury_l30d", 0) or 0)
            spend_context = (
                f"Card: {format_currency(card)} | "
                f"BP: {format_currency(bp)} | "
                f"Treasury: {format_currency(treasury)}"
            )
    except Exception:
        pass

    # Check for existing open opps on this account
    existing_opps = ""
    try:
        opps_df = run_query(format_query(ACCOUNT_OPPS_QUERY, user_id=user_id))
        if not opps_df.empty:
            acct_opps = opps_df[opps_df["account_id"] == account_id]
            if not acct_opps.empty:
                opp_lines = []
                for _, r in acct_opps.iterrows():
                    opp_lines.append(
                        f"  {r.get('expansion_subtype', '')} — "
                        f"{r.get('opportunity_stage_name', '')} — "
                        f"{format_currency(float(r.get('monthly_expansion_amount', 0) or 0))}/mo"
                    )
                existing_opps = "\n".join(opp_lines)
    except Exception:
        pass

    # Always look up the latest Gong call for this account (for the link)
    gong_source = ""
    gong_link = _get_latest_gong_url(account_id, account_name)

    # If user didn't provide next_step or notes, run full Gong transcript analysis
    if not next_step or not user_notes:
        gong_result = _extract_opp_context_from_gong(account_id, account_name, product_type, user_id=user_id)
        if gong_result:
            gong_source = gong_result.get("source", "")
            if not gong_link:
                call_id = gong_result.get("call_id", "")
                if call_id:
                    gong_link = f"https://us-11688.app.gong.io/call?id={call_id}&entry=calls_widget"
            if not next_step:
                next_step = gong_result.get("next_step", "")
            if not user_notes:
                user_notes = gong_result.get("notes", "")

    # Defaults if still empty (no Gong transcript or extraction failed)
    product_label = EXPANSION_PRODUCT_MAP.get(product_type, product_type)
    if not next_step:
        next_step = "CW"
    if not user_notes:
        user_notes = f"Utilizing Ramp {product_label} for consolidation, control"

    # Estimate CP
    ntr = NTR_RATES.get(product_type, 0.0095)
    est_cp = amount * ntr * 3 if amount else 0  # 90-day window

    # Enrich notes with spend context for SFDC
    expansion_notes = user_notes
    if spend_context and len(expansion_notes) < 150:
        expansion_notes += f" | L30D: {spend_context}"

    sf_acct = sf_account_url(account_id)

    # Compute close date (end of current month)
    from datetime import datetime, timedelta
    now = datetime.utcnow()
    close_date = ((now.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")

    # Build confirmation DM with opp details and Create Opp button
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": f"\U0001f4b0 Confirm New Opp: {account_name}",
                     "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(filter(None, [
                f"*<{sf_acct}|{account_name}>* — {product_type}",
                "",
                f"```",
                f"Name:       {account_name} - {EXPANSION_PRODUCT_MAP.get(product_type, product_type)}",
                f"Stage:      S2: Sales Qualified Opportunity",
                f"Close Date: {close_date}",
                f"Amount:     {format_currency(amount)}/mo" if amount else None,
                f"Est. CP:    {format_currency(est_cp)}" if est_cp else None,
                f"Next Step:  {next_step}",
                f"```",
                f"Notes: _{user_notes}_",
                f"L30D Spend: {spend_context}" if spend_context else None,
                f"Gong: <{gong_link}|View call recording>" if gong_link else None,
                f"_\u2139\ufe0f Pre-filled from Gong: {gong_source}_" if gong_source else None,
            ]))},
        },
    ]

    if existing_opps:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"\u26a0\ufe0f *Existing open opps:*\n{existing_opps}"},
        })

    # Build button payload with all opp details
    import json as _json
    button_payload = _json.dumps({
        "account_id": account_id,
        "account_name": account_name,
        "product": product_type,
        "amount": amount,
        "close_date": close_date,
        "stage": "S2: Sales Qualified Opportunity",
        "next_step": next_step,
        "notes": expansion_notes,
        "gong_link": gong_link,
    })

    action_buttons = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Create Opp", "emoji": True},
            "action_id": "quick_create_opp_sfdc",
            "value": button_payload,
            "style": "primary",
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Cancel", "emoji": True},
            "action_id": "dismiss_action",
        },
    ]
    if gong_link:
        action_buttons.append({
            "type": "button",
            "text": {"type": "plain_text", "text": "\U0001f517 Gong Call", "emoji": True},
            "url": gong_link,
            "action_id": f"gong_link_{account_id}",
        })
    blocks.append({"type": "actions", "elements": action_buttons})

    # Build re-run command for easy tweaking
    # Reverse-map product_type to short alias
    _PRODUCT_SHORT = {
        "Card Expansion": "card", "Bill Pay Expansion": "bp",
        "Treasury Expansion": "treasury", "Travel Expansion": "travel",
        "SaaS": "saas", "Procurement": "proc",
    }
    prod_short = _PRODUCT_SHORT.get(product_type, "card")
    amt_short = f"{int(amount/1000)}k" if amount >= 1000 else str(int(amount)) if amount else "0"
    rerun_cmd = f"/opp {account_name} {prod_short} {amt_short} | {next_step} | {user_notes}"

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"Stage: S2 | Close: {close_date}\n"
                f"Re-run: `{rerun_cmd}`"
            ),
        }],
    })

    client.chat_postMessage(
        channel=user_id,
        blocks=blocks,
        text=f"Confirm new opp: {account_name} — {product_type} {format_currency(amount)}/mo",
    )
    logger.info("Opp link created: %s — %s %s/mo", account_name, product_type, format_currency(amount))


_GONG_WORKSPACE_ID = "7396037457040592385"


def _get_latest_gong_url(account_id, account_name=""):
    """Return the best Gong URL for an account.

    Priority:
    1. Recent call URL from fct_gong_calls (canonical link)
    2. Call URL built from dim_sfdc_gong_transcripts call_id
    3. Account search URL (always works, lands on account overview)
    """
    # Strategy 1: fct_gong_calls has the canonical call_url
    try:
        df = run_query(f"""
            SELECT gc.call_url, gc.call_id
            FROM analytics.marts.fct_gong_calls gc
            JOIN analytics.marts.dim_sfdc_gong_call sgc
                ON sgc.gong_call_id = gc.call_id
            WHERE sgc.sfdc_primary_account_id = '{account_id}'
              AND sgc.gong_call_duration_sec >= 180
            ORDER BY sgc.gong_call_start DESC
            LIMIT 1
        """)
        if not df.empty:
            url = str(df.iloc[0]["call_url"])
            if url and url.startswith("http"):
                return url
    except Exception as e:
        logger.debug("Gong call_url lookup (fct) failed for %s: %s", account_id, e)

    # Strategy 2: Build URL from dim_sfdc_gong_transcripts call_id
    try:
        df = run_query(f"""
            SELECT DISTINCT call_id
            FROM analytics.marts.dim_sfdc_gong_transcripts
            WHERE account_id = '{account_id}'
              AND call_duration_sec >= 180
            ORDER BY call_start DESC
            LIMIT 1
        """)
        if not df.empty:
            call_id = str(df.iloc[0]["call_id"])
            return f"https://us-11688.app.gong.io/call?id={call_id}&entry=calls_widget"
    except Exception as e:
        logger.debug("Gong call_id lookup (transcripts) failed for %s: %s", account_id, e)

    # Strategy 3: Conversations search URL (fallback — shows calls for account)
    if account_name:
        import json
        from urllib.parse import quote
        call_search = json.dumps({
            "search": {
                "type": "And",
                "filters": [{"type": "LeadCompanyOrAccount", "names": [account_name]}],
            }
        }, separators=(",", ":"))
        return (
            f"https://us-11688.app.gong.io/conversations"
            f"?workspace-id={_GONG_WORKSPACE_ID}"
            f"&callSearch={quote(call_search)}"
        )

    return ""


def _extract_opp_context_from_gong(account_id, account_name, product_type, user_id=None):
    """Check for recent Gong transcripts and extract next step + expansion notes.

    Returns {next_step, notes, source} or None if no transcript found.
    """
    from core.claude_client import call_claude_json
    from queries.queries import GONG_FULL_TRANSCRIPT_QUERY

    try:
        df = run_query(format_query(GONG_FULL_TRANSCRIPT_QUERY, user_id=user_id,
            account_id=account_id, lookback_days=30,
        ))
        if df.empty:
            return None

        # Use the most recent call
        latest_call_id = df.iloc[0]["call_id"]
        call_rows = df[df["call_id"] == latest_call_id]
        call_name = call_rows.iloc[0].get("call_name", "")
        call_date = str(call_rows.iloc[0].get("call_date", ""))

        # Build condensed transcript (cap at 4000 chars)
        lines = []
        for _, row in call_rows.sort_values("paragraph_index").iterrows():
            speaker = row.get("speaker_email", "?")
            tag = " [Ramp]" if row.get("is_ramp_participant") else ""
            lines.append(f"{speaker}{tag}: {row.get('paragraph_text', '')}")
        transcript = "\n".join(lines)
        if len(transcript) > 4000:
            transcript = transcript[:4000] + "\n[...truncated...]"

        product_label = EXPANSION_PRODUCT_MAP.get(product_type, product_type)

        prompt = f"""Extract two fields from this Gong call transcript for a {product_label} expansion opportunity on {account_name}.

CALL: {call_name} ({call_date})

TRANSCRIPT:
{transcript}

Return a JSON object with exactly these keys:
- "next_step": string — the most concrete next action discussed (e.g. "Send pricing proposal", "Schedule implementation call", "Follow up on AP migration timeline"). Max 120 chars. If nothing concrete, use "CW".
- "notes": string — 1-sentence expansion context from the call (e.g. "Customer processing $50K/mo in AP through Bill.com, interested in consolidating to Ramp Bill Pay"). Max 200 chars. If nothing relevant, use "Utilizing Ramp {product_label} for consolidation, control".

Return ONLY the JSON object."""

        result = call_claude_json(prompt, max_tokens=300)
        if result and result.get("next_step"):
            return {
                "next_step": result["next_step"][:120],
                "notes": (result.get("notes") or f"Utilizing Ramp {product_label} for consolidation, control")[:200],
                "source": f"{call_name} ({call_date})",
                "call_id": str(latest_call_id),
            }
    except Exception as e:
        logger.debug("Gong context extraction failed for %s: %s", account_name, e)

    return None


def _fuzzy_title_match(query: str, title: str) -> bool:
    """Check if query fuzzy-matches a meeting title (handles typos).

    Strategies:
    1. Direct substring match
    2. All significant words appear in title
    3. Prefix match (first 5+ chars of query match start of a title word)
    4. Character overlap > 70% (handles transposed/missing letters)
    """
    import re as _re

    q = query.lower().strip()
    t = title.lower()

    # 1. Direct substring
    if q in t:
        return True

    # 2. Word-level: all words > 2 chars appear
    q_words = [w for w in q.split() if len(w) > 2]
    if q_words and all(w in t for w in q_words):
        return True

    # 3. Prefix match: first 5+ chars of query match any word start in title
    t_words = _re.split(r'[\s/\-<>|]+', t)
    if len(q) >= 5:
        for tw in t_words:
            if tw.startswith(q[:5]) or q.startswith(tw[:5]):
                return True

    # 4. Character overlap for single-word queries (handles typos)
    for tw in t_words:
        if len(tw) >= 5 and len(q) >= 5:
            common = sum(1 for c in q if c in tw)
            ratio = common / max(len(q), len(tw))
            if ratio >= 0.7:
                return True

    return False


def _handle_post_meeting_account(client, user_id, account_filter, lookback_days, query_user_id=None):
    """Run post-meeting analysis for a specific account.

    Tries Granola first (recent local meetings), then falls back to Gong transcripts in Snowflake.
    """
    from utils.account_resolver import resolve_account_name
    from jobs.granola_followup import _analyze_granola_meeting, _send_glass_style_dm
    from jobs.post_meeting_followup import _analyze_call, _send_followup_dm
    from queries.queries import GONG_MEETINGS_FULL_TRANSCRIPT_QUERY
    from core.granola_client import get_recent_meetings, extract_attendee_info
    import re as _re

    # -- Try Granola FIRST (before SFDC resolution) using raw user input --
    client.chat_postMessage(
        channel=user_id,
        text=f":mag: Searching Granola for *{account_filter}* meetings (last {lookback_days} days)...",
    )
    granola_found = False
    granola_meetings = []
    try:
        granola_meetings = get_recent_meetings(minutes=60 * 24 * lookback_days, skip_end_check=True)
        granola_match = None
        for meeting in granola_meetings:
            title = meeting.get("title", "")
            if _fuzzy_title_match(account_filter, title):
                granola_match = meeting
                break

        if granola_match:
            granola_found = True
            people = granola_match.get("people", [])
            names, emails = extract_attendee_info(people)

            analysis = _analyze_granola_meeting(granola_match, names, emails)
            if analysis:
                _send_glass_style_dm([analysis], client)
                return
    except Exception as e:
        logger.debug("Granola search failed for %s: %s", account_filter, e)

    # -- Resolve SFDC account (needed for Gong fallback) ------------------
    result = resolve_account_name(None, account_filter)
    if not result:
        # If Granola also failed, show what's available
        if granola_meetings:
            titles = [m.get("title", "?") for m in granola_meetings[:8]]
            titles_str = ", ".join(f"_{t}_" for t in titles) if titles else "none"
            client.chat_postMessage(
                channel=user_id,
                text=f"No Granola match and couldn't find *{account_filter}* in SFDC.\n"
                     f"Recent Granola meetings: {titles_str}",
            )
        else:
            client.chat_postMessage(
                channel=user_id,
                text=f"Couldn't find an account matching *{account_filter}* in your book.",
            )
        return

    account_id = result["account_id"]
    account_name = result["account_name"]

    # -- Try Granola again with resolved SFDC name (if different) ---------
    if not granola_found and account_name.lower() != account_filter.lower():
        try:
            for meeting in granola_meetings:
                title = meeting.get("title", "")
                if _fuzzy_title_match(account_name, title):
                    granola_found = True
                    people = meeting.get("people", [])
                    names, emails = extract_attendee_info(people)
                    analysis = _analyze_granola_meeting(meeting, names, emails)
                    if analysis:
                        _send_glass_style_dm([analysis], client)
                        return
        except Exception as e:
            logger.debug("Granola search (SFDC name) failed: %s", e)

    if not granola_found:
        try:
            titles = [m.get("title", "?") for m in granola_meetings[:8]]
            titles_str = ", ".join(f"_{t}_" for t in titles) if titles else "none"
            client.chat_postMessage(
                channel=user_id,
                text=f"No Granola match for *{account_filter}*. Recent meetings: {titles_str}\n"
                     f":mag: Checking Gong transcripts for *{account_name}*...",
            )
        except Exception:
            client.chat_postMessage(
                channel=user_id,
                text=f"No Granola match for *{account_filter}*. Checking Gong transcripts...",
            )

    # -- Fall back to Gong ------------------------------------------------
    _quid = query_user_id or user_id
    transcript_df = run_query(
        format_query(GONG_MEETINGS_FULL_TRANSCRIPT_QUERY, user_id=_quid, lookback_days=lookback_days)
    )

    if transcript_df.empty:
        # Try with longer lookback
        transcript_df = run_query(
            format_query(GONG_MEETINGS_FULL_TRANSCRIPT_QUERY, user_id=_quid, lookback_days=30)
        )

    if transcript_df.empty:
        client.chat_postMessage(
            channel=user_id,
            text=f"No Gong transcripts found in the last {lookback_days} days (or 30 days) either.\n"
                 f"Today's calls typically sync to Snowflake overnight — try again tomorrow morning.",
        )
        return

    # Filter to this account
    acct_df = transcript_df[transcript_df["account_id"] == account_id]
    if acct_df.empty:
        # Fuzzy match on account_name
        acct_df = transcript_df[
            transcript_df["account_name"].str.lower().str.contains(
                account_filter.lower(), na=False
            )
        ]

    if acct_df.empty:
        available = transcript_df["account_name"].unique().tolist()[:10]
        avail_str = ", ".join(available) if available else "none"
        client.chat_postMessage(
            channel=user_id,
            text=(
                f"No Gong transcript found for *{account_name}* in the last {lookback_days} days.\n"
                f"_Available accounts with transcripts: {avail_str}_\n\n"
                f"Today's calls typically sync overnight. Try `/post-meeting {account_filter} 7` "
                f"for a wider lookback."
            ),
        )
        return

    # Use the most recent call for this account
    latest_call_id = acct_df.iloc[0]["call_id"]
    call_rows = acct_df[acct_df["call_id"] == latest_call_id]
    first = call_rows.iloc[0]

    meta = {
        "call_id": latest_call_id,
        "account_id": str(first.get("account_id", "")),
        "account_name": first.get("account_name", ""),
        "call_name": first.get("call_name", ""),
        "call_date": str(first.get("call_date", "")),
        "duration_min": int(first.get("duration_min", 0) or 0),
    }
    transcript_records = call_rows.sort_values("paragraph_index").to_dict("records")

    # Run analysis
    analysis = _analyze_call(latest_call_id, meta, transcript_records)
    if not analysis:
        client.chat_postMessage(
            channel=user_id,
            text=f"Analysis failed for *{account_name}* call: _{meta['call_name']}_",
        )
        return

    # Send follow-up DM
    _send_followup_dm([analysis], client)

    # Get Gong call link
    gong_url = _get_latest_gong_url(account_id, account_name)
    if gong_url:
        client.chat_postMessage(
            channel=user_id,
            text=f":studio_microphone: <{gong_url}|View on Gong> — {meta['call_name']}",
        )
