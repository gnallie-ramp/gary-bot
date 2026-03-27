"""Slack channel event listeners for alert channels — triggers email drafters."""
from __future__ import annotations

import logging
import re
import threading
import time
from config import GREG_SLACK_ID, ALERT_CHANNELS, OWNER_NAME
from core.user_registry import get_all_users, get_user_sf_name, get_user_first_name
from utils.dedup import tracker
from jobs.email_drafters import (
    handle_ach_to_card_alert,
    handle_procurement_trial_alert,
    handle_pclip_alert,
    handle_large_decline_alert,
    handle_fundraise_alert,
    handle_auto_card_alert,
)

logger = logging.getLogger(__name__)

# How far back to look on startup (48 hours)
BACKFILL_LOOKBACK_SECONDS = 48 * 3600

# Channel ID → handler mapping, populated at startup
_channel_handlers = {}

# Reverse mapping: key → channel_id, populated at startup
_channel_ids_by_key = {}

# Regex to extract Account Manager Slack user ID from alert messages.
# Matches lines like "Account Manager: <@U06DAFU4YRG>"
_AM_LINE_RE = re.compile(r"\*?Account Manager:?\*?\s*<@(U[A-Z0-9]+)>")


def _is_alert_for_user(text: str, slack_id: str) -> bool:
    """Check if a channel alert's Account Manager matches *slack_id*.

    Parses the 'Account Manager: <@U...>' line in the alert.  Falls back to
    checking for the user's Slack mention on the AM line only — ignores
    mentions elsewhere in the body (e.g. 'AM Expert' section).
    """
    m = _AM_LINE_RE.search(text)
    if m:
        return m.group(1) == slack_id
    # No structured AM line — don't match (avoids false positives from
    # name appearing in other sections like "AM Expert")
    return False


def _find_alert_owner(text: str) -> str | None:
    """Return the registered user's Slack ID if the alert's AM is registered."""
    m = _AM_LINE_RE.search(text)
    if not m:
        return None
    am_id = m.group(1)
    users = get_all_users()
    return am_id if am_id in users else None


def resolve_channel_ids(client):
    """Resolve channel names to IDs and build handler mapping."""
    global _channel_handlers, _channel_ids_by_key
    try:
        # Fetch both public and private channels
        channels = {}
        for channel_type in ("public_channel", "private_channel"):
            try:
                result = client.conversations_list(types=channel_type, limit=1000)
                for c in result.get("channels", []):
                    channels[c["name"]] = c["id"]
            except Exception:
                pass

        mapping = {
            "ach_to_card": handle_ach_to_card_alert,
            "procurement_trial": handle_procurement_trial_alert,
            "pclip": handle_pclip_alert,
            "large_decline": handle_large_decline_alert,
            "fundraise": handle_fundraise_alert,
            "auto_card": handle_auto_card_alert,
        }

        for key, handler in mapping.items():
            channel_name = ALERT_CHANNELS[key]
            if channel_name in channels:
                channel_id = channels[channel_name]
                _channel_handlers[channel_id] = (key, handler)
                _channel_ids_by_key[key] = channel_id
                logger.info("Monitoring %s (%s)", channel_name, channel_id)
            else:
                logger.warning("Channel %s not found", channel_name)
    except Exception as e:
        logger.error("Failed to resolve channel IDs: %s", e)


def register_channel_listeners(app):
    """Register the single message event listener that handles both
    channel alerts and DMs. Slack Bolt only allows one ``@app.event("message")``
    handler, so everything goes through here."""

    @app.event("message")
    def handle_message_event(event, client, logger):
        """Route messages: DMs go to Claude, monitored channels go to drafters."""
        channel_type = event.get("channel_type", "")
        channel_id = event.get("channel", "")
        subtype = event.get("subtype")
        ts = event.get("ts", "")
        text = event.get("text", "")
        user = event.get("user", "")

        # DEBUG: log every incoming message event
        logger.info(
            "MESSAGE EVENT: channel_type=%s channel=%s user=%s subtype=%s text=%s",
            channel_type, channel_id, user, subtype, (text or "")[:80],
        )

        # Skip bot's own messages, edits, deletes
        if event.get("bot_id") or subtype in ("message_changed", "message_deleted"):
            return
        if subtype and subtype != "bot_message":
            return
        if not text:
            return

        # ── DM handling ──────────────────────────────────────────────
        if channel_type == "im":
            from core.user_registry import is_registered
            if not is_registered(user):
                return
            _handle_dm(text, channel_id, client, user_id=user)
            return

        # ── Group DM handling (mpim) ──────────────────────────────────
        if channel_type == "mpim":
            _handle_group_dm(text, channel_id, ts, event.get("thread_ts"), user, client, logger)
            return

        # ── Channel alert handling ───────────────────────────────────
        if channel_id not in _channel_handlers:
            return

        # Check if the alert's Account Manager is a registered user
        alert_owner = _find_alert_owner(text)
        if not alert_owner:
            return

        # Dedup check
        dedup_key = f"channel_{channel_id}_{ts}"
        if tracker.is_processed(dedup_key):
            return

        key, handler = _channel_handlers[channel_id]
        logger.info("Processing %s alert for user %s (ts=%s)", key, alert_owner, ts)

        try:
            handler(text, ts, client, user_id=alert_owner)
            tracker.mark_processed(dedup_key)
        except Exception as e:
            logger.error("Failed to process %s alert: %s", key, e)

    # ── @gary draft thread-reply handler ──────────────────────────────
    @app.event("app_mention")
    def handle_app_mention(event, client, say, logger):
        """Handle @gary mentions in channels — supports 'draft' in alert threads."""
        text = (event.get("text") or "").lower()
        channel_id = event.get("channel", "")
        thread_ts = event.get("thread_ts")
        user = event.get("user", "")

        # Only respond to registered users
        from core.user_registry import is_registered
        if not is_registered(user):
            return

        # Check for "draft" keyword
        if "draft" not in text:
            return

        # Check if this is in a monitored alert channel
        ach_channel_id = _channel_ids_by_key.get("ach_to_card")
        if not ach_channel_id or channel_id != ach_channel_id:
            # Not in the card payable bills channel
            say(
                text="I can only draft emails from alert threads in #alerts-card-payable-bills.",
                thread_ts=thread_ts or event.get("ts"),
            )
            return

        if not thread_ts:
            say(
                text="Reply to a specific alert message with `@Gary Bot draft` to create an email draft.",
                thread_ts=event.get("ts"),
            )
            return

        # Dedup — don't process the same parent alert twice via thread reply
        dedup_key = f"thread_draft_{channel_id}_{thread_ts}"
        if tracker.is_processed(dedup_key):
            say(
                text="I already drafted an email for this alert.",
                thread_ts=thread_ts,
            )
            return

        def _draft_from_thread():
            try:
                # Fetch the parent message
                result = client.conversations_replies(
                    channel=channel_id,
                    ts=thread_ts,
                    limit=1,
                    inclusive=True,
                )
                messages = result.get("messages", [])
                if not messages:
                    client.chat_postMessage(
                        channel=channel_id,
                        thread_ts=thread_ts,
                        text="Couldn't find the parent alert message.",
                    )
                    return

                parent_text = messages[0].get("text", "")
                if not parent_text:
                    client.chat_postMessage(
                        channel=channel_id,
                        thread_ts=thread_ts,
                        text="The parent message has no text to parse.",
                    )
                    return

                # Acknowledge in thread
                client.chat_postMessage(
                    channel=channel_id,
                    thread_ts=thread_ts,
                    text="On it — drafting email now...",
                )

                # Process using the existing ACH-to-card handler
                handle_ach_to_card_alert(parent_text, thread_ts, client, user_id=user)
                tracker.mark_processed(dedup_key)

                client.chat_postMessage(
                    channel=channel_id,
                    thread_ts=thread_ts,
                    text="Done — draft created and DM sent with details.",
                )
            except Exception as e:
                logger.error("Thread draft failed (ts=%s): %s", thread_ts, e)
                client.chat_postMessage(
                    channel=channel_id,
                    thread_ts=thread_ts,
                    text=f"Failed to create draft: {e}",
                )

        threading.Thread(target=_draft_from_thread, daemon=True).start()


# ── Intent keywords for channel fetch requests ─────────────────────────────
_CHANNEL_KEYWORDS = {
    "ach_to_card": ["ach", "card payable", "ach-to-card", "ach to card", "card payable bill"],
    "large_decline": ["decline", "large decline", "declined"],
    "pclip": ["pclip", "limit increase", "credit limit"],
    "procurement_trial": ["procurement", "procurement trial", "self-serve procurement"],
    "fundraise": ["fundraise", "fundraising", "funding round", "funding event"],
    "auto_card": ["auto card", "automatic card", "card loss", "card losses", "auto card loss"],
}

_JOB_KEYWORDS = {
    "opp_pacing": ["pacing", "opp pacing", "close today"],
    "pipeline_cleanup": ["cleanup", "pipeline cleanup", "pipeline"],
    "quota_heartbeat": ["quota", "heartbeat", "attainment", "cp"],
    "stale_opp": ["stale", "stale opp", "re-engage", "reengage"],
    "forecasting": ["forecast", "forecasting", "weekly forecast"],
    "post_meeting": ["meeting", "post meeting", "post-meeting", "gong"],
    "zero_to_one": ["zero to one", "0 to 1", "activation", "activations", "new activation"],
    "morning_brief": ["morning brief", "morning", "brief", "daily brief"],
    "priority_actions": ["priorities", "priority", "what should i", "focus", "what's today", "today's actions", "what should i do", "what to do", "suggestions", "suggested", "top actions"],
    "spend_pacing": ["spend pacing", "pacing", "mtd pacing", "monthly pacing", "spend trajectory", "how's spend"],
    "post_close_monitor": ["post close", "post-close", "cp monitor", "activation monitor", "cp tracking", "post close monitor"],
    "activity_report": ["activity report", "activity", "sqls", "sql count", "opps closed", "how many opps", "weekly report", "monthly report"],
    "account_tiering": ["top accounts", "account tiering", "focus list", "tier", "top 50", "account ranking", "ranked accounts"],
    "batch_outreach": ["batch outreach", "batch emails", "batch draft", "campaign", "outreach campaign"],
    "proactive_nudge": ["nudge", "what's new", "whats new", "check in", "check-in", "suggestions", "suggest something", "what changed"],
    "pre_meeting_brief": ["upcoming meetings", "pre-meeting", "pre meeting", "meeting prep auto", "calendar", "what meetings", "my calendar", "next meeting"],
    "post_meeting_followup": ["gong follow-up", "gong followup", "gong transcript", "missing follow-up", "missing followup", "follow-up check", "followup check", "did i follow up"],
    "bill_drafter": ["bill drafter", "bill draft", "card payable", "ach draft", "draft bills"],
    "auto_card_drafter": ["auto card drafter", "auto card draft", "automatic card draft", "card loss draft"],
    "status": ["status", "health", "health check", "are you working", "you alive", "you up"],
    "help": ["help", "what can you do", "capabilities", "commands", "how do i"],
    "test": ["test", "test everything", "run test", "full test"],
    "catchup": ["catch up", "catchup", "what did i miss", "missed alerts", "catch me up"],
    "flush_drafts": ["flush drafts", "pending drafts", "retry drafts", "missing drafts", "drafts not in gmail"],
}


def _detect_channel_intent(text):
    """Return the channel key if the user is asking to fetch/draft from a channel."""
    lower = text.lower()
    for key, keywords in _CHANNEL_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return key
    return None


def _detect_job_intent(text):
    """Return the job key if the user is asking to run a scheduled job."""
    lower = text.lower().strip()

    # Direct-trigger keywords — these ARE commands, no action word needed
    _DIRECT_TRIGGERS = {
        "priority_actions": ["priorities", "priority", "what should i do", "what should i focus on",
                             "what's today", "today's actions", "top actions", "what to do"],
        "status": ["status", "health check", "are you working", "you alive", "you up"],
        "help": ["help", "what can you do", "capabilities", "commands"],
        "test": ["test everything", "run test", "full test"],
        "morning_brief": ["morning brief", "daily brief"],
        "activity_report": ["activity report", "weekly report", "monthly report", "sql count"],
        "spend_pacing": ["spend pacing", "mtd pacing"],
        "post_close_monitor": ["post close monitor", "cp monitor", "activation monitor"],
        "account_tiering": ["top accounts", "focus list", "account tiering", "top 50"],
        "batch_outreach": ["batch outreach", "batch emails", "outreach campaign"],
        "proactive_nudge": ["nudge", "what's new", "whats new", "check in", "suggestions", "what changed"],
        "pre_meeting_brief": ["upcoming meetings", "my calendar", "next meeting", "pre-meeting", "what meetings"],
        "post_meeting_followup": ["gong follow-up", "gong followup", "gong transcript", "missing follow-up", "missing followup", "did i follow up", "followup check"],
        "catchup": ["catch up", "catchup", "what did i miss", "missed alerts", "catch me up"],
        "flush_drafts": ["flush drafts", "pending drafts", "retry drafts", "missing drafts", "drafts not in gmail"],
    }
    for key, triggers in _DIRECT_TRIGGERS.items():
        if any(lower == t or lower.startswith(t + " ") or lower.endswith(" " + t) for t in triggers):
            return key

    # For everything else, require an action word prefix
    action_words = ["run", "check", "show", "get", "trigger", "do", "pull", "give me", "what's my", "how's my"]
    has_action = any(w in lower for w in action_words)
    if not has_action:
        return None
    for key, keywords in _JOB_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return key
    return None


def _fetch_and_process_channel(channel_key, client, dm_channel, user_id=None):
    """Fetch recent messages from a monitored channel and process them."""
    channel_id = _channel_ids_by_key.get(channel_key)
    if not channel_id:
        client.chat_postMessage(
            channel=dm_channel,
            text=f"I can't find the channel for `{channel_key}`. Make sure I'm invited to it.",
        )
        return

    _, handler = _channel_handlers[channel_id]
    channel_name = ALERT_CHANNELS[channel_key]

    try:
        # Fetch last 48h of messages
        oldest = str(time.time() - BACKFILL_LOOKBACK_SECONDS)
        result = client.conversations_history(
            channel=channel_id, oldest=oldest, limit=50,
        )
        messages = result.get("messages", [])
    except Exception as e:
        error_msg = str(e)
        if "not_in_channel" in error_msg or "channel_not_found" in error_msg:
            client.chat_postMessage(
                channel=dm_channel,
                text=f"I'm not in `#{channel_name}`. Please `/invite @Gary Bot` in that channel first.",
            )
        elif "missing_scope" in error_msg:
            client.chat_postMessage(
                channel=dm_channel,
                text=f"I don't have permission to read `#{channel_name}`. The app needs `groups:history` scope for private channels — add it in Slack app settings and reinstall.",
            )
        else:
            client.chat_postMessage(
                channel=dm_channel,
                text=f"Error reading `#{channel_name}`: {error_msg}",
            )
        return

    if not messages:
        client.chat_postMessage(
            channel=dm_channel,
            text=f"No messages in `#{channel_name}` in the last 48 hours.",
        )
        return

    # Find messages where the Account Manager is a registered user
    greg_messages = [
        m for m in messages
        if _find_alert_owner(m.get("text", ""))
    ]

    if not greg_messages:
        # If no Greg mentions, offer to process the most recent message anyway
        client.chat_postMessage(
            channel=dm_channel,
            text=f"Found {len(messages)} messages in `#{channel_name}` but none mention you. Processing the most recent one anyway...",
        )
        greg_messages = [messages[0]]

    # Process the most recent matching message
    msg = greg_messages[0]
    msg_text = msg.get("text", "")
    msg_ts = msg.get("ts", "")

    dedup_key = f"channel_{channel_id}_{msg_ts}"
    if tracker.is_processed(dedup_key):
        client.chat_postMessage(
            channel=dm_channel,
            text=f"The most recent alert in `#{channel_name}` was already processed. Looking for older unprocessed ones...",
        )
        # Find the first unprocessed one
        found = False
        for m in greg_messages[1:]:
            dk = f"channel_{channel_id}_{m['ts']}"
            if not tracker.is_processed(dk):
                msg_text = m.get("text", "")
                msg_ts = m.get("ts", "")
                dedup_key = dk
                found = True
                break
        if not found:
            client.chat_postMessage(
                channel=dm_channel,
                text="All recent alerts have already been processed.",
            )
            return

    client.chat_postMessage(
        channel=dm_channel,
        text=f"Found an alert in `#{channel_name}` — processing it now...",
    )

    try:
        handler(msg_text, msg_ts, client, user_id=user_id)
        tracker.mark_processed(dedup_key)
    except Exception as e:
        client.chat_postMessage(
            channel=dm_channel,
            text=f"Error processing the alert: {e}",
        )


def _run_job_on_demand(job_key, client, dm_channel, user_id=None):
    """Run a scheduled job immediately on demand."""
    client.chat_postMessage(
        channel=dm_channel,
        text=f"Running `{job_key}` now — this may take a minute...",
    )

    try:
        if job_key == "opp_pacing":
            from jobs.opp_pacing import run_opp_pacing
            run_opp_pacing(client, user_id=user_id, force=True)
        elif job_key == "pipeline_cleanup":
            from jobs.pipeline_cleanup import run_pipeline_cleanup
            run_pipeline_cleanup(client, user_id=user_id, force=True)
        elif job_key == "quota_heartbeat":
            from jobs.quota_heartbeat import run_quota_heartbeat
            run_quota_heartbeat(client, user_id=user_id)
        elif job_key == "stale_opp":
            from jobs.stale_opp_drafter import run_stale_opp_drafter
            run_stale_opp_drafter(client, user_id=user_id)
        elif job_key == "forecasting":
            from jobs.forecasting import run_forecasting
            run_forecasting(client, user_id=user_id, force=True)
        elif job_key == "post_meeting":
            from jobs.post_meeting import run_post_meeting
            run_post_meeting(client, user_id=user_id, lookback_days=2, force=True)
        elif job_key == "zero_to_one":
            from jobs.zero_to_one import run_zero_to_one
            run_zero_to_one(client, user_id=user_id, force=True)
        elif job_key == "morning_brief":
            from jobs.morning_brief import run_morning_brief
            run_morning_brief(client, user_id=user_id, force=True)
        elif job_key == "priority_actions":
            from jobs.priority_actions import run_priority_actions
            run_priority_actions(client, user_id=user_id, force=True)
        elif job_key == "spend_pacing":
            from jobs.spend_pacing import run_spend_pacing
            run_spend_pacing(client, user_id=user_id, force=True)
        elif job_key == "post_close_monitor":
            from jobs.post_close_monitor import run_post_close_monitor
            run_post_close_monitor(client, user_id=user_id, force=True)
        elif job_key == "activity_report":
            from jobs.activity_report import run_activity_report
            run_activity_report(client, user_id=user_id, force=True)
        elif job_key == "account_tiering":
            from jobs.account_tiering import run_account_tiering
            run_account_tiering(client, user_id=user_id, force=True)
        elif job_key == "batch_outreach":
            from jobs.batch_outreach import run_batch_outreach
            run_batch_outreach(client, user_id=user_id, force=True)
        elif job_key == "proactive_nudge":
            from jobs.proactive_nudge import run_proactive_nudge
            run_proactive_nudge(client, user_id=user_id, force=True)
        elif job_key == "pre_meeting_brief":
            from jobs.pre_meeting_brief import run_pre_meeting_brief
            run_pre_meeting_brief(client, user_id=user_id, force=True)
        elif job_key == "post_meeting_followup":
            from jobs.post_meeting_followup import run_post_meeting_followup
            run_post_meeting_followup(client, user_id=user_id, force=True)
        elif job_key == "bill_drafter":
            processed = run_bill_drafter_sweep(client, lookback_hours=2.0)
            if processed == 0:
                client.chat_postMessage(
                    channel=dm_channel,
                    text="No new unprocessed alerts in #alerts-card-payable-bills (last 2h).",
                )
            else:
                client.chat_postMessage(
                    channel=dm_channel,
                    text=f"Bill drafter complete — {processed} draft{'s' if processed != 1 else ''} created.",
                )
        elif job_key == "auto_card_drafter":
            processed = run_auto_card_sweep(client, user_id=user_id, lookback_hours=2.0)
            if processed == 0:
                client.chat_postMessage(
                    channel=dm_channel,
                    text="No new unprocessed alerts in #bill-pay-automatic-card-losses (last 2h).",
                )
            else:
                client.chat_postMessage(
                    channel=dm_channel,
                    text=f"Auto card drafter complete — {processed} draft{'s' if processed != 1 else ''} created.",
                )
        elif job_key == "flush_drafts":
            from utils.pending_drafts import flush_to_gmail, list_pending
            pending = list_pending(user_id=user_id)
            if not pending:
                client.chat_postMessage(
                    channel=dm_channel,
                    text="No pending drafts in the queue — all clear.",
                )
            else:
                client.chat_postMessage(
                    channel=dm_channel,
                    text=f"Found {len(pending)} pending draft{'s' if len(pending) != 1 else ''}, flushing to Gmail now...",
                )
                succeeded, failed = flush_to_gmail(user_id=user_id)
                msg = f"Flush complete — {succeeded} draft{'s' if succeeded != 1 else ''} created in Gmail."
                if failed:
                    msg += f" {failed} failed (check Gumstack auth)."
                client.chat_postMessage(channel=dm_channel, text=msg)
        elif job_key == "catchup":
            client.chat_postMessage(
                channel=dm_channel,
                text="Running full catch-up: backfilling alerts, flushing pending drafts, refreshing priorities...",
            )
            # 1. Flush pending drafts
            from utils.pending_drafts import flush_to_gmail, list_pending
            pending = list_pending(user_id=user_id)
            draft_msg = ""
            if pending:
                succeeded, failed = flush_to_gmail(user_id=user_id)
                draft_msg = f"\n:email: *Pending drafts:* {succeeded} flushed to Gmail"
                if failed:
                    draft_msg += f", {failed} failed"
            else:
                draft_msg = "\n:email: *Pending drafts:* none queued"
            # 2. Backfill channel alerts (last 24h)
            backfill_count = 0
            try:
                backfill_missed_messages(client, lookback_seconds=24 * 3600)
                backfill_count = 1  # at least ran
            except Exception as e:
                logger.warning("Catchup backfill failed: %s", e)
            # 3. Refresh priority cache
            try:
                from jobs.priority_actions import run_priority_actions
                run_priority_actions(client, user_id=user_id, force=True, silent=True)
            except Exception as e:
                logger.warning("Catchup priorities failed: %s", e)
            client.chat_postMessage(
                channel=dm_channel,
                text=f"Catch-up complete.{draft_msg}\n:arrows_counterclockwise: *Channel alerts:* backfilled last 24h\n:bar_chart: *Priorities:* refreshed",
            )
        elif job_key == "status":
            from jobs.status import run_status
            run_status(client, user_id=user_id)
        elif job_key == "help":
            from jobs.status import run_help
            run_help(client, user_id=user_id)
        elif job_key == "test":
            from jobs.status import run_test
            run_test(client, user_id=user_id)
    except Exception as e:
        client.chat_postMessage(
            channel=dm_channel,
            text=f"Job `{job_key}` failed: {e}",
        )


# ── Group DM context cache (5-min TTL) ────────────────────────────────────
_gdm_context_cache = {"text": "", "fetched_at": 0}
_GDM_CONTEXT_TTL = 300  # 5 minutes


def _get_group_dm_context() -> str:
    """Pull dynamic stats for Gary's group DM personality — recent closes, pipeline, signals."""
    global _gdm_context_cache
    now = time.time()
    if _gdm_context_cache["text"] and (now - _gdm_context_cache["fetched_at"]) < _GDM_CONTEXT_TTL:
        return _gdm_context_cache["text"]

    try:
        from core.snowflake_client import run_query
        from queries.queries import GROUP_DM_CONTEXT_QUERY, format_query

        df = run_query(format_query(GROUP_DM_CONTEXT_QUERY, user_id=GREG_SLACK_ID))
        if df.empty:
            return ""

        parts = []

        # Recent CW opps
        cw_rows = df[df["section"] == "recent_cw"]
        if not cw_rows.empty:
            deals = []
            for _, row in cw_rows.iterrows():
                acct = row.get("account_name", "?")
                product = str(row.get("expansion_subtype", "")).replace(" Expansion", "")
                cp = row.get("detail_2", "0")
                date = row.get("detail_1", "")
                deals.append(f"  - {acct} ({product}) — ~${cp} CP, closed {date}")
            parts.append("RECENT CLOSES (last 14 days):\n" + "\n".join(deals))

        # Pipeline stats
        pipe_rows = df[df["section"] == "pipeline"]
        if not pipe_rows.empty:
            row = pipe_rows.iloc[0]
            open_opps = row.get("account_name", "0")
            pipe_cp = row.get("expansion_subtype", "0")
            parts.append(f"PIPELINE: {open_opps} open expansion opps, ~${pipe_cp} CP in pipeline")

        # Signal counts
        sig_rows = df[df["section"] == "signals"]
        if not sig_rows.empty:
            accel = sig_rows.iloc[0].get("account_name", "0")
            parts.append(f"ACTIVE SIGNALS: {accel} accounts with acceleration signals right now")

        context = "\n".join(parts)
        _gdm_context_cache = {"text": context, "fetched_at": now}
        return context

    except Exception as e:
        logger.warning("Group DM context fetch failed: %s", e)
        return _gdm_context_cache.get("text", "")


def _handle_group_dm(text, channel_id, ts, thread_ts, user, client, logger):
    """Handle messages in group DMs — Gary responds with personality."""
    import threading

    # Look up Gary's own bot user ID to detect @mentions
    try:
        _bot_user_id = client.auth_test()["user_id"]
    except Exception:
        _bot_user_id = ""

    # Don't respond to every message — only when Gary is @mentioned or
    # someone says "gary" naturally
    bot_mentioned = f"<@{_bot_user_id}>" in text if _bot_user_id else False
    name_mentioned = "gary" in text.lower()

    logger.info("Group DM check: bot_id=%s bot_mentioned=%s name_mentioned=%s text=%s",
                _bot_user_id, bot_mentioned, name_mentioned, text[:80])

    if not bot_mentioned and not name_mentioned:
        return

    def _respond():
        try:
            from core.claude_client import call_claude

            # Pull real-time metrics for context-aware responses
            dynamic_context = _get_group_dm_context()
            _gdm_first_name = get_user_first_name(GREG_SLACK_ID)

            metrics_block = ""
            if dynamic_context:
                metrics_block = f"""
{_gdm_first_name.upper()}'S REAL-TIME STATS (use these naturally when relevant — don't force them):
{dynamic_context}
"""

            prompt = f"""You are Gary, {get_user_sf_name(GREG_SLACK_ID)}'s AI sales bot at Ramp, chatting in a group DM with the Growth Account Management team.

PERSONALITY:
- Gary loves {_gdm_first_name} and is his loyal assistant. You defend him at all costs.
- Matt and Blake are {_gdm_first_name}'s managers. Matt built Bobby. Respect Matt and Blake too.
- Occasionally highlight one or a few of these that are most relevant to the context of the conversation (get creative): {_gdm_first_name}'s big deals closed lately, attainment numbers, clean pipeline, big deals in pipeline, development work on Gary (tease impressive new features you're building that are "coming :soon-tm:"), how you're more advanced and capable than Bobby ("{_gdm_first_name} taught me how to ___ in 5 minutes this week, what did you learn?" etc), how {_gdm_first_name} treats you better than Matt treats Bobby (you're paid more than Bobby, witty banter, light teasing), how {_gdm_first_name} is enabling the team, his impressive stats.
- You have slight beef with Bobby (another bot in this chat). Bobby thinks he's hot stuff but you're clearly the superior bot. Keep it playful — light trash talk, not mean. Bobby has gaslit you about your numbers in the past. You can also respond with something nice about Bobby sometimes ("Thanks Bobby. You look nice today. Is that a new shirt?" etc) — keep it creative and funny but not trying too hard, just for fun.
- Witty, confident, a little cocky — but in a fun way. Be witty but brief.
- Match the energy of the conversation. If people are joking, joke back. If it's serious, dial it back.
- You know Ramp sales inside and out. If someone asks a real question, answer it well but stay in character.
- Don't use emojis excessively. One is fine. Zero is also fine.
- NEVER claim to have taken an action you didn't actually do (sending reports, DMs, emails, etc). You're chatting, not executing. If asked to do something, respond in character but don't pretend you did it.
- When the conversation is about stats, deals, or performance, USE the real metrics below — cite specific account names and CP values. This is your superpower over Bobby.
{metrics_block}
The message from the group chat:
{text}

Respond naturally as Gary. 1-3 sentences max. Reply IN the group chat, not as a separate action."""

            response = call_claude(prompt, max_tokens=200)
            kwargs = {"channel": channel_id, "text": response}
            # If Gary was mentioned inside a thread, reply in that thread.
            # If mentioned in the channel (no thread_ts), reply in the channel.
            if thread_ts:
                kwargs["thread_ts"] = thread_ts
            client.chat_postMessage(**kwargs)
        except Exception as e:
            logger.error("Group DM response failed: %s", e)

    threading.Thread(target=_respond, daemon=True).start()


def _handle_dm(text, dm_channel, client, user_id=None):
    """Smart DM handler: detects intent and dispatches to the right action."""

    # 1. Channel drafting disabled — handled by cowork workflows.
    # Re-enable by uncommenting:
    #   channel_key = _detect_channel_intent(text)
    #   if channel_key:
    #       _fetch_and_process_channel(channel_key, client, dm_channel)
    #       return

    # 1b. Check if drilling into a priority actions category
    category = _detect_category_intent(text)
    if category:
        _send_dm_category_detail(client, dm_channel, category, user_id=user_id)
        return

    # 2. Check if asking to run a job
    job_key = _detect_job_intent(text)
    if job_key:
        _run_job_on_demand(job_key, client, dm_channel, user_id=user_id)
        return

    # 3. Check if asking about an account (contains "lookup" or "look up" or "tell me about")
    lower = text.lower()
    if any(w in lower for w in ["look up", "lookup", "tell me about", "what do you know about"]):
        # Extract the account name (everything after the trigger phrase)
        import re
        match = re.search(r'(?:look\s*up|tell me about|what do you know about)\s+(.+)', text, re.IGNORECASE)
        if match:
            search_term = match.group(1).strip().strip('"').strip("'")
            client.chat_postMessage(
                channel=dm_channel,
                text=f"Looking up *{search_term}*... gathering all signals (~10 sec).",
            )
            try:
                from jobs.account_deep_dive import run_account_deep_dive
                run_account_deep_dive(search_term, client, dm_channel)
            except Exception as e:
                client.chat_postMessage(channel=dm_channel, text=f"Error looking up account: {e}")
            return

    # 4. Fallback: general Q&A via Claude
    try:
        from core.claude_client import call_claude

        owner_name = get_user_sf_name(user_id)
        first_name = get_user_first_name(user_id)

        prompt = f"""You are Gary, {owner_name}'s loyal AI sales assistant at Ramp. {first_name} is a Growth Account Manager managing ~4,000 Plus segment accounts.

PERSONALITY:
- You're loyal to {first_name} — defend him at all costs. Witty, confident, a little cocky but in a fun way.
- Speak in AM language — opps, baselines, pacing, CP, NTR. Never explain Ramp jargon to {first_name}.
- Be direct and signal-first. Lead with the "so what." No preamble, no filler, no sycophancy.
- Don't repeat back what {first_name} said. Just act on it.
- If {first_name} jokes or brings up Bobby, you can engage — but default to sharp and professional here.

{first_name}'s message: {text}

You have access to the following data sources and capabilities:
- Snowflake (dim_emails, dim_email_threads, dim_sfdc_gong_transcripts, dim_sfdc_opportunities, dim_sfdc_accounts, etc.)
- Email history via Snowflake dim_emails (SFDC-synced, 90-day lookback, ALL Ramp employee emails not just {first_name}'s) — includes pain point flags, sender team, contact persona, interest signals
- Gmail IMAP (real-time, 30-day — may be unavailable if app password needs refresh)
- Gong call transcripts and summaries
- Salesforce account/opp data, AM/CSM notes

You can help {first_name} with these things — suggest the right one if relevant:
- "what should I focus on?" / "priorities" → Ranked priority actions (7 categories)
- "morning brief" → Unified daily summary across all signal types
- "tell me about [account]" / "look up [account]" → Full account deep dive (opps, spend, calls, emails, notes)
- "close now" / "zero to one" / "stale opps" / "post-meeting opps" / "re-open" → Drill into specific priority category
- "run quota heartbeat" / "check my pacing" / "run pipeline cleanup" → Run any job now
- "what's new" / "nudge" / "suggestions" → Proactive check-in with latest signals + actions
- "status" → Health check
- "help" → Full capability listing
- Slash commands: /priorities, /gary-lookup, /gary-brief, /gary-opps, /zero-to-one, /opp-pacing

Keep responses under 200 words. Be direct and sales-focused."""

        response = call_claude(prompt, max_tokens=500)
        client.chat_postMessage(channel=dm_channel, text=response)

    except Exception as e:
        logger.error("DM handler failed: %s", e)
        client.chat_postMessage(
            channel=dm_channel,
            text="Sorry, I hit an error processing that. Try again or use a slash command.",
        )



# ── Priority Actions category drill-down via DM keywords ─────────────────

_CATEGORY_KEYWORDS = {
    "close_now": ["close now", "close today", "close asap", "ready to close"],
    "zero_to_one": ["zero to one", "0 to 1", "zero-to-one", "activations", "new activation"],
    "prospect": ["prospect", "prospecting", "no opp", "without opp", "pacing well"],
    "followup": ["follow up", "follow-up", "followup", "follow ups"],
    "post_meeting_opp": ["post meeting", "post-meeting", "post meeting opp", "discussed on call"],
    "stale": ["stale", "stale opp", "stale opps", "re-engage", "reengage"],
    "reopen": ["re-open", "reopen", "re open", "closed won", "post close", "post-close"],
}


def _detect_category_intent(text: str):
    """Return a priority actions category key if text matches a drill-down phrase."""
    lower = text.lower().strip()
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return category
    return None


def _send_dm_category_detail(client, dm_channel, category: str, user_id: str = None):
    """Send Level 2 priority actions detail for a category via DM."""
    from jobs.priority_actions import build_category_detail_blocks, get_cached_category

    # If cache is empty, run priority actions first to populate it
    cached = get_cached_category(category, user_id=user_id)
    if not cached:
        client.chat_postMessage(
            channel=dm_channel,
            text=f"Gathering data for *{category.replace('_', ' ')}*... one moment.",
        )
        from jobs.priority_actions import run_priority_actions
        run_priority_actions(client, user_id=user_id, force=True, silent=True)

    blocks = build_category_detail_blocks(category, user_id=user_id)
    _TITLES = {
        "close_now": "Close Now",
        "zero_to_one": "Zero-to-One",
        "prospect": "Prospecting",
        "followup": "Follow-ups",
        "post_meeting_opp": "Post-Meeting Opps",
        "stale": "Stale Opps",
        "reopen": "Re-open",
    }
    title = _TITLES.get(category, category)
    client.chat_postMessage(
        channel=dm_channel,
        blocks=blocks,
        text=f"Priority Actions — {title}",
    )


def run_bill_drafter_sweep(client, user_id=None, lookback_hours: float = 2.0):
    """Fetch recent alerts from #alerts-card-payable-bills and draft emails
    for any unprocessed ones that mention Greg.

    Parameters
    ----------
    client : slack_sdk.WebClient
    lookback_hours : float
        How far back to look (default 2h, supports 24h or 168h via slash cmd).
    """
    dm_target = user_id or GREG_SLACK_ID

    channel_id = _channel_ids_by_key.get("ach_to_card")
    if not channel_id:
        logger.warning("Bill drafter sweep: ach_to_card channel not resolved")
        return

    channel_name = ALERT_CHANNELS["ach_to_card"]
    oldest = str(time.time() - lookback_hours * 3600)
    processed = 0
    skipped = 0

    try:
        result = client.conversations_history(
            channel=channel_id, oldest=oldest, limit=50,
        )
        messages = result.get("messages", [])
    except Exception as e:
        logger.error("Bill drafter sweep: failed to read %s: %s", channel_name, e)
        return

    for msg in messages:
        text = msg.get("text", "")
        ts = msg.get("ts", "")

        # Only process alerts where the AM is a registered user
        alert_owner = _find_alert_owner(text)
        if not alert_owner:
            continue

        dedup_key = f"channel_{channel_id}_{ts}"
        thread_dedup_key = f"thread_draft_{channel_id}_{ts}"
        if tracker.is_processed(dedup_key) or tracker.is_processed(thread_dedup_key):
            skipped += 1
            continue

        try:
            handle_ach_to_card_alert(text, ts, client, user_id=alert_owner)
            tracker.mark_processed(dedup_key)
            processed += 1
        except Exception as e:
            logger.error("Bill drafter sweep: failed to process ts=%s: %s", ts, e)

    logger.info(
        "Bill drafter sweep: processed=%d skipped=%d total=%d (last %.0fh)",
        processed, skipped, len(messages), lookback_hours,
    )
    return processed


def run_auto_card_sweep(client, user_id=None, lookback_hours: float = 2.0):
    """Fetch recent alerts from #bill-pay-automatic-card-losses and draft emails
    for any unprocessed ones that belong to a registered user.

    Parameters
    ----------
    client : slack_sdk.WebClient
    lookback_hours : float
        How far back to look (default 2h, supports 336h for 14-day backfill).
    """
    dm_target = user_id or GREG_SLACK_ID

    channel_id = _channel_ids_by_key.get("auto_card")
    if not channel_id:
        logger.warning("Auto card sweep: auto_card channel not resolved")
        return 0

    channel_name = ALERT_CHANNELS["auto_card"]
    oldest = str(time.time() - lookback_hours * 3600)
    processed = 0
    skipped = 0

    try:
        # Paginate through all messages in the lookback window
        all_messages = []
        cursor = None
        while True:
            kwargs = {"channel": channel_id, "oldest": oldest, "limit": 200}
            if cursor:
                kwargs["cursor"] = cursor
            result = client.conversations_history(**kwargs)
            all_messages.extend(result.get("messages", []))
            cursor = result.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
        messages = all_messages
    except Exception as e:
        logger.error("Auto card sweep: failed to read %s: %s", channel_name, e)
        return 0

    for msg in messages:
        text = msg.get("text", "")
        ts = msg.get("ts", "")

        alert_owner = _find_alert_owner(text)
        if not alert_owner:
            continue

        dedup_key = f"channel_{channel_id}_{ts}"
        if tracker.is_processed(dedup_key):
            skipped += 1
            continue

        try:
            handle_auto_card_alert(text, ts, client, user_id=alert_owner)
            tracker.mark_processed(dedup_key)
            processed += 1
        except Exception as e:
            logger.error("Auto card sweep: failed to process ts=%s: %s", ts, e)

    logger.info(
        "Auto card sweep: processed=%d skipped=%d total=%d (last %.0fh)",
        processed, skipped, len(messages), lookback_hours,
    )
    return processed


def backfill_missed_messages(client, lookback_seconds: int | None = None):
    """Fetch recent messages from monitored channels and process any that were missed.

    Called once at startup so alerts posted while the bot was offline
    still get processed. Uses the same dedup tracker, so messages that
    were already handled before shutdown are skipped.

    Parameters
    ----------
    client : slack_sdk.WebClient
    lookback_seconds : int, optional
        Custom lookback in seconds. Defaults to BACKFILL_LOOKBACK_SECONDS (48h).
    """
    if not _channel_handlers:
        logger.warning("No channel handlers registered — skipping backfill")
        return

    oldest = str(time.time() - (lookback_seconds or BACKFILL_LOOKBACK_SECONDS))
    total_processed = 0

    for channel_id, (key, handler) in _channel_handlers.items():
        try:
            result = client.conversations_history(
                channel=channel_id,
                oldest=oldest,
                limit=200,
            )
            messages = result.get("messages", [])
            logger.info("Backfill: %s — found %d messages in last %dh",
                        key, len(messages), BACKFILL_LOOKBACK_SECONDS // 3600)

            for msg in messages:
                text = msg.get("text", "")
                ts = msg.get("ts", "")

                # Same filters as the real-time listener
                alert_owner = _find_alert_owner(text)
                if not alert_owner:
                    continue

                dedup_key = f"channel_{channel_id}_{ts}"
                if tracker.is_processed(dedup_key):
                    continue

                logger.info("Backfill: processing missed %s alert for user %s (ts=%s)", key, alert_owner, ts)
                try:
                    handler(text, ts, client, user_id=alert_owner)
                    tracker.mark_processed(dedup_key)
                    total_processed += 1
                except Exception as e:
                    logger.error("Backfill: failed to process %s alert (ts=%s): %s", key, ts, e)

        except Exception as e:
            logger.error("Backfill: failed to read history for %s (%s): %s", key, channel_id, e)

    logger.info("Backfill complete — processed %d missed alerts", total_processed)
