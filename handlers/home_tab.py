"""App Home tab — tabbed layout published on app_home_opened event."""

import json
import logging
import re
import threading
import time
from datetime import datetime

from config import GREG_SLACK_ID, SF_BASE_URL, OWNER_NAME, get_owner_id, set_owner_id, _OWNER_FILE

logger = logging.getLogger(__name__)

# ── Module-level cache for priority alerts (10-min TTL) ──────────────────────
_priority_cache = {"data": None, "fetched_at": 0}
_PRIORITY_CACHE_TTL = 600  # 10 minutes

# ── Tab state per user ───────────────────────────────────────────────────────
_active_tab = {}  # user_id -> tab name
_TABS = [
    ("dashboard", ":house: Dashboard"),
    ("pipeline", ":dart: Pipeline"),
    ("meetings", ":calendar: Meetings"),
    ("drafts", ":email: Drafts"),
    ("instructions", ":books: Instructions"),
    ("settings", ":gear: Settings"),
]
_DEFAULT_TAB = "dashboard"


def _updated_at_block(epoch=None) -> dict:
    """Return a context block showing 'Updated X ago' or 'Updated just now'."""
    if epoch is None or epoch == 0:
        label = "Not yet loaded"
    else:
        delta = int(time.time() - epoch)
        if delta < 60:
            label = "Updated just now"
        elif delta < 3600:
            label = f"Updated {delta // 60}m ago"
        else:
            label = f"Updated {delta // 3600}h {(delta % 3600) // 60}m ago"
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": f"_{label}_"}]}


def register_home_tab(app):
    """Register the app_home_opened event handler."""

    @app.event("app_home_opened")
    def handle_app_home_opened(event, client):
        user_id = event.get("user")
        tab = event.get("tab")
        if tab != "home":
            return

        # Auto-claim ownership on first Home tab open.
        # Each bot instance runs on a separate machine, so the .owner file
        # is inherently per-instance. The first user to open the Home tab
        # becomes this instance's owner — no manual config needed.
        import os
        if not os.path.exists(_OWNER_FILE):
            set_owner_id(user_id)
            logger.info("Auto-detected bot owner: %s", user_id)

        def _publish():
            try:
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(
                    user_id=user_id,
                    view={"type": "home", "blocks": blocks},
                )
            except Exception as e:
                logger.error("Home tab publish failed: %s", e)

        threading.Thread(target=_publish, daemon=True).start()


# ── Tab bar builder ──────────────────────────────────────────────────────────

def _build_tab_bar(active):
    """Build the tab button row. Active tab gets primary style."""
    elements = []
    for tab_id, label in _TABS:
        btn = {
            "type": "button",
            "text": {"type": "plain_text", "text": label, "emoji": True},
            "action_id": f"home_tab_switch_{tab_id}",
        }
        if tab_id == active:
            btn["style"] = "primary"
        elements.append(btn)
    # Slack actions block supports max 25 elements (we have 6)
    return {"type": "actions", "elements": elements}


def _build_home_blocks_header(active_tab):
    """Build just the header + tab bar for instant tab switch feedback."""
    blocks = []
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": "Gary — Sales Intelligence Co-Pilot", "emoji": True},
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            "AI-powered signals, briefs, opps, and drafts — all in Slack. "
            "Gary monitors your book 24/5."
        )}],
    })
    blocks.append(_build_tab_bar(active_tab))
    blocks.append({"type": "divider"})
    return blocks


# ── Main router ──────────────────────────────────────────────────────────────

def _build_home_blocks(client, user_id):
    """Build the Home tab blocks — routes to active tab's builder."""
    active = _active_tab.get(user_id, _DEFAULT_TAB)

    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": "Gary — Sales Intelligence Co-Pilot", "emoji": True},
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            "AI-powered signals, briefs, opps, and drafts — all in Slack. "
            "Gary monitors your book 24/5."
        )}],
    })

    # Tab bar
    blocks.append(_build_tab_bar(active))
    blocks.append({"type": "divider"})

    # Tab content
    tab_builders = {
        "dashboard": _build_dashboard_tab,
        "pipeline": _build_pipeline_tab,
        "meetings": _build_meetings_tab,
        "drafts": _build_drafts_tab,
        "instructions": _build_instructions_tab,
        "settings": _build_settings_tab,
    }
    builder = tab_builders.get(active, _build_dashboard_tab)
    tab_blocks = builder(client, user_id)
    blocks.extend(tab_blocks)

    # Footer
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "Gary Bot v2 • Built with Claude Code • DM me anything or use a slash command"}],
    })

    # Slack 100-block limit safety
    if len(blocks) > 100:
        blocks = blocks[:99]
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "_Some content truncated — use slash commands for full detail_"}],
        })

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Dashboard
# ══════════════════════════════════════════════════════════════════════════════

def _build_dashboard_tab(client, user_id):
    """Dashboard: Quota snapshot + priority alerts summary + quick actions."""
    blocks = []

    # Quota Snapshot
    quota_blocks = _get_quota_snapshot()
    if quota_blocks:
        blocks.extend(quota_blocks)
        blocks.append({"type": "divider"})

    # Priority Alerts (condensed — first 3 groups, 3 per group)
    alert_blocks = _get_priority_alerts(max_per_group=3, max_groups=4)
    if alert_blocks:
        blocks.extend(alert_blocks)
        blocks.append(_updated_at_block(_priority_cache.get("fetched_at", 0)))
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "_Switch to the :dart: Pipeline tab for all signals_"}],
        })
        blocks.append({"type": "divider"})

    # Quick Actions
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:zap: Quick Actions*"},
    })
    blocks.append({
        "type": "actions",
        "elements": [
            {"type": "button", "text": {"type": "plain_text", "text": ":dart: Priorities", "emoji": True}, "action_id": "home_run_priorities"},
            {"type": "button", "text": {"type": "plain_text", "text": ":chart_with_upwards_trend: Quota", "emoji": True}, "action_id": "home_run_quota"},
            {"type": "button", "text": {"type": "plain_text", "text": ":sunrise: Morning Brief", "emoji": True}, "action_id": "home_run_morning"},
            {"type": "button", "text": {"type": "plain_text", "text": ":loudspeaker: Nudge", "emoji": True}, "action_id": "home_run_nudge"},
        ],
    })
    blocks.append({
        "type": "actions",
        "elements": [
            {"type": "button", "text": {"type": "plain_text", "text": ":money_with_wings: Spend Pacing", "emoji": True}, "action_id": "home_run_spend"},
            {"type": "button", "text": {"type": "plain_text", "text": ":broom: Pipeline Cleanup", "emoji": True}, "action_id": "home_run_cleanup"},
            {"type": "button", "text": {"type": "plain_text", "text": ":rocket: Zero-to-One", "emoji": True}, "action_id": "home_run_zero_to_one"},
            {"type": "button", "text": {"type": "plain_text", "text": ":crystal_ball: Forecast", "emoji": True}, "action_id": "home_run_forecast"},
        ],
    })

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Pipeline
# ══════════════════════════════════════════════════════════════════════════════

def _build_pipeline_tab(client, user_id):
    """Pipeline: Full priority alerts with all signal groups expanded."""
    blocks = []

    alert_blocks = _get_priority_alerts(max_per_group=5, max_groups=8)
    if alert_blocks:
        blocks.extend(alert_blocks)
        blocks.append(_updated_at_block(_priority_cache.get("fetched_at", 0)))
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_No active priority signals right now. Check back later or run_ `/priorities`"},
        })

    # Non-spend signals (stale, reopen, post-meeting, underperforming)
    nonsignal_blocks = _get_non_spend_signals()
    if nonsignal_blocks:
        blocks.append({"type": "divider"})
        blocks.extend(nonsignal_blocks)

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Meetings
# ══════════════════════════════════════════════════════════════════════════════

def _build_meetings_tab(client, user_id):
    """Meetings: Today's calendar with Brief and Post-Meeting buttons."""
    blocks = []

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:calendar: Today's Customer Meetings*"},
    })

    meetings_blocks = _get_todays_meetings()
    if meetings_blocks:
        blocks.extend(meetings_blocks)
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_No customer meetings today_"},
        })
    blocks.append(_updated_at_block(time.time()))

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Drafts
# ══════════════════════════════════════════════════════════════════════════════

def _build_drafts_tab(client, user_id):
    """Drafts: Pending drafts queue + flush button."""
    blocks = []

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:email: Email Drafts*"},
    })

    try:
        from utils.pending_drafts import list_pending

        pending = list_pending()
        if pending:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{len(pending)} pending draft{'s' if len(pending) != 1 else ''}* waiting to be created in Gmail:"},
            })
            for d in pending[:10]:
                to = d.get("to", "?")
                subj = d.get("subject", "?")
                acct = d.get("account_name", "")
                created = d.get("created_at", "")[:16].replace("T", " ")
                line = f"• *{acct}*\n  To: {to}\n  Subject: {subj}\n  Queued: {created}"
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": line},
                })
            if len(pending) > 10:
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"_...and {len(pending) - 10} more_"}],
                })
            blocks.append({
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":arrows_counterclockwise: Flush All to Gmail", "emoji": True},
                    "action_id": "home_flush_drafts",
                    "style": "primary",
                }],
            })
        else:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": ":white_check_mark: No pending drafts — all clear."},
            })

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": (
                "Drafts are created in Gmail under *Claude Drafts/* labels. "
                "If Gumstack auth expires, drafts queue here until flushed. "
                "You can also DM Gary `flush drafts` or `catch up`."
            )}],
        })

    except Exception as e:
        logger.debug("Drafts tab failed: %s", e)
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_Could not load draft queue._"},
        })

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Instructions
# ══════════════════════════════════════════════════════════════════════════════

def _build_instructions_tab(client, user_id):
    """Instructions: Full command reference, DM keywords, how things work."""
    blocks = []

    # ── Overview ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:wave: Welcome to Gary*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": (
            "Gary is your AI sales co-pilot. He monitors ~4,000 accounts 24/5, "
            "catches spend acceleration before baselines rise, auto-drafts outreach emails, "
            "and surfaces the right account at the right time — all inside Slack."
        )},
    })
    blocks.append({"type": "divider"})

    # ── How to interact ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:speech_balloon: How to Talk to Gary*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "• *DM Gary* — Ask questions, run commands, or look up accounts. Just type naturally.",
            "• *Slash commands* — Use `/priorities`, `/gary-lookup`, etc. from any channel.",
            "• *Home tab* — This screen. Use the tabs above to navigate.",
            "• *Buttons* — Click Draft, Brief, View All, etc. throughout the app.",
            "• *Group DM* — @mention Gary in a group chat and he'll respond in character.",
        ])},
    })
    blocks.append({"type": "divider"})

    # ── DM Keywords ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:keyboard: DM Keywords*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "DM Gary any of these (no slash needed):",
            "",
            "*Intelligence*",
            '`priorities` — What should I focus on?',
            '`morning brief` — Daily action summary',
            '`quota` — CP attainment + accelerator band',
            '`spend pacing` — MTD vs last month trajectory',
            '`nudge` — What changed since last check?',
            '`activity report` — SQLs + opps closed breakdown',
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Pipeline*",
            '`pipeline cleanup` — Urgency-ranked pipeline review',
            '`forecast` — S3+ opps + coaching brief',
            '`opp pacing` — Milestone tracking for open opps',
            '`zero to one` — Fresh product activations',
            '`post close monitor` — Post-close CP tracking',
            '`stale opps` — Re-engage dormant pipeline',
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Accounts & Outreach*",
            '`lookup <account>` — Full account snapshot + signals',
            '`top accounts` — Top 50 ranked by CP potential',
            '`batch outreach` — Cluster accounts + draft campaigns',
            '`bill drafter` — Sweep card payable alerts for drafts',
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Meetings*",
            '`my calendar` — Today\'s upcoming meetings',
            '`pre-meeting` — Auto-brief for next meeting',
            '`gong followup` — Check for missing post-call follow-ups',
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*System*",
            '`catch up` — Run full catch-up (backfill alerts, flush drafts, refresh priorities)',
            '`flush drafts` — Retry all pending drafts stuck in queue',
            '`status` — Health check',
            '`help` — Full command list',
        ])},
    })
    blocks.append({"type": "divider"})

    # ── Slash Commands ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:lightning: Slash Commands*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Daily Intelligence*",
            "`/priorities` — Ranked actions across 7 signal categories",
            "`/morning-brief` — Combined daily action summary",
            "`/quota-heartbeat` — CP attainment + accelerator band",
            "`/spend-pacing` — MTD vs last month + YoY trajectory",
            "`/nudge` — What's new since last check",
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Pipeline & Opps*",
            "`/opp <account> <product> <amount>` — Quick-create pre-filled SF opp",
            "`/gary-opps` — Open expansion opp summary",
            "`/opp-pacing` — Opp milestone tracking",
            "`/pipeline-cleanup` — Urgency-ranked pipeline + coaching",
            "`/forecast` — S3+ opps + coaching brief",
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Account Intelligence*",
            "`/gary-lookup <account>` — Account snapshot",
            "`/gary-brief <account>` — Pre-call expansion brief",
            "`/top-accounts` — Top 50 by CP potential",
            "`/zero-to-one` — Fresh product activations",
            "`/post-close` — Post-close activation tracking",
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Outreach & Follow-Up*",
            "`/post-meeting` — Post-meeting to-do check",
            "`/batch-outreach` — Cluster accounts + draft campaigns",
            "`/bill-drafter` — Bill pay email drafter sweep",
            "`/activity-report` — SQLs + opps closed by product",
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*System*",
            "`/gary-status` — Health check",
            "`/gary-help` — Full help",
            "`/gary-test` — Test all integrations",
        ])},
    })
    blocks.append({"type": "divider"})

    # ── Automated Jobs ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:clock3: Automated Schedule (19 jobs)*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            ":sunrise: *Early Morning* (3:45–5 AM PT)",
            "  Quota Insights • Acceleration Alert _(daily summary)_",
            "",
            ":sunrise: *Morning* (7:30–9 AM PT)",
            "  Pipeline Cleanup • Morning Brief • Opp Pacing • Zero-to-One • Activity Report • Spend Pacing",
            "",
            ":repeat: *Recurring* (throughout day)",
            "  Acceleration Alert _(every 30m)_ • Pre-Meeting Brief _(every 30m)_ • Gong Follow-Up _(every 30m)_",
            "  Granola Follow-Up _(every 3m)_ • Post-Meeting To-Do _(every 2h)_ • Proactive Nudge _(every 2h)_",
            "  Bill Drafter _(every 30m)_ • Draft Reminder _(every 2h)_",
            "",
            ":city_sunset: *EOD + Weekly*",
            "  Post-Close CP _(10 AM)_ • Quota Heartbeat _(6 PM)_ • Forecasting _(Mon 7 AM)_",
        ])},
    })
    blocks.append({"type": "divider"})

    # ── How signals work ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:bulb: How Signals Work*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "Gary detects 8 types of spend signals by comparing real-time spend against baselines:",
            "",
            ":zap: *Early Accel* — L7D pacing >1.5x baseline, L30D still low. Window open to close with low baseline.",
            ":alarm_clock: *Close Window* — Open opp with L7D ramping above L30D. Close before baseline rises.",
            ":money_with_wings: *Close Now* — Open opp where L30D already exceeds baseline. Close ASAP.",
            ":eyes: *Leading* — Large bills created/scheduled or card L3D surge. Spend incoming.",
            ":tada: *First Bill* — First bill pay usage on an open opp. Customer just started.",
            ":rocket: *Zero-to-One* — New product activated after opp was created. Lock in low baseline.",
            ":chart_with_upwards_trend: *Sustained Accel* — L7D >1.5x baseline but L30D already elevated. Window closing.",
            ":moneybag: *Treasury Spike* — GLA balance L7D avg >2x L30D avg. Large cash deposit.",
        ])},
    })
    blocks.append({"type": "divider"})

    # ── Integrations ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:electric_plug: Integrations*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": (
            ":snowflake: *Snowflake* — Accounts, opps, spend, activations, Gong transcripts\n"
            ":salesforce: *Salesforce* — Pre-filled opp creation, account links\n"
            ":calendar: *Google Calendar* — Meeting detection, auto-briefs every 30 min\n"
            ":email: *Gmail* — Auto-draft follow-up emails via Gumstack\n"
            ":robot_face: *Claude AI* — Analysis, coaching, email generation\n"
            ":studio_microphone: *Granola* — Real-time meeting transcripts + auto follow-up"
        )},
    })

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Settings
# ══════════════════════════════════════════════════════════════════════════════

def _build_settings_tab(client, user_id):
    """Settings: DM alert toggles + drafting toggles."""
    blocks = []

    settings_blocks = _get_settings_blocks()
    if settings_blocks:
        blocks.extend(settings_blocks)

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# DATA FETCHERS (unchanged from original)
# ══════════════════════════════════════════════════════════════════════════════

def _get_quota_snapshot():
    """Pull quota data from Looker CSVs for the home tab.

    Returns a list of Slack blocks (not a single string) so we can include
    multiple sections and an action button.
    """
    try:
        from datetime import datetime
        import pytz
        from jobs.quota_insights import (
            _find_csv, _parse_wide_csv, _parse_dollar, _parse_pct,
            _get_team_ranking, _latest_period_with_data, _short_period,
            _TMP_DIR, _GREG_NAME,
        )
        from core.gmail_client import fetch_looker_zip
        from config import DISPLAY_TIMEZONE
        import os

        et = pytz.timezone(DISPLAY_TIMEZONE)
        now_et = datetime.now(et)
        month_short = now_et.strftime("%b")
        current_month_label = now_et.strftime("%Y-%m")
        quarter_num = (now_et.month - 1) // 3 + 1
        current_quarter_label = f"{now_et.year}-Q{quarter_num}"

        # Try to use already-extracted data, or fetch fresh
        metrics_dir = None
        if os.path.exists(_TMP_DIR):
            for entry in os.listdir(_TMP_DIR):
                full = os.path.join(_TMP_DIR, entry)
                if os.path.isdir(full) and "metrics" in entry.lower():
                    metrics_dir = full
                    break
        if not metrics_dir:
            metrics_dir = fetch_looker_zip("Growth AM IC Detailed Metrics")
        if not metrics_dir:
            return None

        # Parse key CSVs
        realized_csv = _find_csv(metrics_dir, "monthly_realized_cp_by_ic")
        realized_data, realized_periods = _parse_wide_csv(realized_csv) if realized_csv else ({}, [])

        renewal_csv = _find_csv(metrics_dir, "monthly_renewal_cp_by_ic")
        renewal_data, renewal_periods = _parse_wide_csv(renewal_csv) if renewal_csv else ({}, [])

        card_sql_csv = _find_csv(metrics_dir, "card_sqls")
        card_sql_data, card_sql_periods = _parse_wide_csv(card_sql_csv) if card_sql_csv else ({}, [])

        bp_sql_csv = _find_csv(metrics_dir, "bill_pay_sqls")
        bp_sql_data, bp_sql_periods = _parse_wide_csv(bp_sql_csv) if bp_sql_csv else ({}, [])

        saas_sql_csv = _find_csv(metrics_dir, "free-to-paid_saas_sqls")
        saas_sql_data, saas_sql_periods = _parse_wide_csv(saas_sql_csv) if saas_sql_csv else ({}, [])

        card_cw_csv = _find_csv(metrics_dir, "card_$cw_cp") or _find_csv(metrics_dir, "card_cw_cp")
        card_cw_data, card_cw_periods = _parse_wide_csv(card_cw_csv) if card_cw_csv else ({}, [])

        bp_cw_csv = _find_csv(metrics_dir, "bill_pay_$cw_cp") or _find_csv(metrics_dir, "bill_pay_cw_cp")
        bp_cw_data, bp_cw_periods = _parse_wide_csv(bp_cw_csv) if bp_cw_csv else ({}, [])

        saas_cw_csv = _find_csv(metrics_dir, "free-to-paid_saas_cw_cp")
        saas_cw_data, saas_cw_periods = _parse_wide_csv(saas_cw_csv) if saas_cw_csv else ({}, [])

        elapsed_csv = _find_csv(metrics_dir, "__of_month_elapsed") or _find_csv(metrics_dir, "of_month_elapsed")
        month_elapsed = None
        if elapsed_csv and os.path.exists(elapsed_csv):
            import csv
            with open(elapsed_csv, "r", newline="", encoding="utf-8-sig") as f:
                for row in csv.reader(f):
                    for cell in row:
                        pct = _parse_pct(cell)
                        if pct is not None and pct > 5:
                            if month_elapsed is None or pct > month_elapsed:
                                month_elapsed = pct

        def _bar(pct):
            if pct is None:
                return "`░░░░░░░░░░`"
            filled = min(int(pct / 10), 10)
            return f"`{'█' * filled}{'░' * (10 - filled)}`"

        def _icon(pct):
            if pct is None:
                return ""
            if pct >= 100:
                return " :white_check_mark:"
            if pct < 50:
                return " :red_circle:"
            return ""

        def _dollar(val):
            if val < 0:
                return f"-${abs(val):,.0f}"
            return f"${val:,.0f}"

        # Realized CP
        greg_realized = realized_data.get(_GREG_NAME, {})
        current_r_period = None
        for p in realized_periods:
            if current_month_label in p:
                current_r_period = p
                break

        realized_line = ""
        r_attain = None
        if current_r_period and current_r_period in greg_realized:
            rd = greg_realized[current_r_period]
            total = _parse_dollar(rd.get("Total Realized CP", ""))
            goal = _parse_dollar(rd.get("Rep Total Realized CP Monthly Goal", ""))
            r_attain = _parse_pct(rd.get("% Attainment", ""))
            card = _parse_dollar(rd.get("Card CP", ""))
            bp = _parse_dollar(rd.get("Bill Pay CP", ""))
            travel = _parse_dollar(rd.get("Travel CP", ""))
            treasury = _parse_dollar(rd.get("Treasury CP", ""))
            f2p = _parse_dollar(rd.get("Free to Paid CP", ""))
            procurement = _parse_dollar(rd.get("Free to Paid Procurement CP", ""))

            rank, total_reps = _get_team_ranking(realized_data, current_r_period)
            rank_str = f" • #{rank} on team" if rank else ""

            realized_line = (
                f"*Realized CP*  {_bar(r_attain)} *{r_attain:.0f}%*{_icon(r_attain)}{rank_str}\n"
                f"{_dollar(total)} / {_dollar(goal)}\n"
                f"Card {_dollar(card)} • BP {_dollar(bp)} • Travel {_dollar(travel)} • "
                f"Treasury {_dollar(treasury)} • F2P {_dollar(f2p)} • Procurement {_dollar(procurement)}"
            )

        # Renewal CP
        greg_renewal = renewal_data.get(_GREG_NAME, {})
        current_n_period = None
        for p in renewal_periods:
            if current_month_label in p:
                current_n_period = p
                break

        renewal_line = ""
        if current_n_period and current_n_period in greg_renewal:
            rn = greg_renewal[current_n_period]
            total = _parse_dollar(rn.get("Total Renewal CP", ""))
            goal = _parse_dollar(rn.get("Rep Total Renewal CP Monthly Goal", ""))
            n_attain = _parse_pct(rn.get("% Attainment", ""))
            renewal_val = _parse_dollar(rn.get("Renewal", ""))
            upsell = _parse_dollar(rn.get("Upsell", ""))

            renewal_line = (
                f"*Renewal CP*  {_bar(n_attain)} *{n_attain:.0f}%*{_icon(n_attain)}\n"
                f"{_dollar(total)} / {_dollar(goal)} • "
                f"Renewal {_dollar(renewal_val)} • Upsell {_dollar(upsell)}"
            )

        # SQLs by product
        def _sql_row(data, periods, label, hint):
            greg_d = data.get(_GREG_NAME, {})
            period = _latest_period_with_data(data, _GREG_NAME, periods, hint)
            if not period or period not in greg_d:
                return None
            d = greg_d[period]
            count = _parse_dollar(d.get("Total Opp.", ""))
            goal_val = ""
            for k in d.keys():
                if "Goal" in k:
                    goal_val = d[k]
                    break
            goal = _parse_dollar(goal_val)
            attain = _parse_pct(d.get("% Attainment", ""))
            pct_str = f" ({attain:.0f}%)" if attain is not None else ""
            icon = _icon(attain) if attain is not None else ""
            return f"{label}: *{count:.0f}* / {goal:.0f}{pct_str}{icon}"

        sql_parts = []
        card_sql = _sql_row(card_sql_data, card_sql_periods, "Card", current_month_label)
        bp_sql = _sql_row(bp_sql_data, bp_sql_periods, "BP", current_month_label)
        saas_sql = _sql_row(saas_sql_data, saas_sql_periods, "SaaS", current_month_label)
        if card_sql:
            sql_parts.append(card_sql)
        if bp_sql:
            sql_parts.append(bp_sql)
        if saas_sql:
            sql_parts.append(saas_sql)

        sql_line = ""
        if sql_parts:
            sql_line = f"*SQLs ({month_short})*\n" + " • ".join(sql_parts)

        # CW CP by product
        def _cw_row(data, periods, label, hint):
            period = _latest_period_with_data(data, _GREG_NAME, periods, hint)
            if not period:
                return None
            greg_d = data.get(_GREG_NAME, {})
            if period not in greg_d:
                return None
            d = greg_d[period]
            total_val = 0.0
            goal_val = 0.0
            attain_val = None
            for k, v in d.items():
                kl = k.lower()
                if "attainment" in kl:
                    attain_val = _parse_pct(v)
                elif "goal" in kl:
                    goal_val = _parse_dollar(v)
                elif "cp" in kl or "total" in kl:
                    total_val = _parse_dollar(v)
            pct_str = f" ({attain_val:.0f}%)" if attain_val is not None else ""
            icon = _icon(attain_val) if attain_val is not None else ""
            return f"{label}: *{_dollar(total_val)}* / {_dollar(goal_val)}{pct_str}{icon}"

        cw_parts = []
        card_cw = _cw_row(card_cw_data, card_cw_periods, "Card", current_quarter_label)
        bp_cw = _cw_row(bp_cw_data, bp_cw_periods, "BP", current_quarter_label)
        saas_cw = _cw_row(saas_cw_data, saas_cw_periods, "SaaS", current_month_label)
        if card_cw:
            cw_parts.append(card_cw)
        if bp_cw:
            cw_parts.append(bp_cw)
        if saas_cw:
            cw_parts.append(saas_cw)

        cw_line = ""
        if cw_parts:
            cw_line = "*CW CP*\n" + " • ".join(cw_parts)

        # Assemble blocks
        elapsed_str = f"  •  {month_elapsed:.0f}% of month elapsed" if month_elapsed else ""
        blocks = []

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*:chart_with_upwards_trend: Quota Snapshot — {month_short}{elapsed_str}*"},
        })

        if realized_line:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": realized_line}})
        if renewal_line:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": renewal_line}})
        if sql_line:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": sql_line}})
        if cw_line:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": cw_line}})

        # Full Report button
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": ":bar_chart: Full Quota Report", "emoji": True},
                "action_id": "home_run_quota_insights",
                "style": "primary",
            }],
        })

        return blocks

    except Exception as e:
        logger.debug("Quota snapshot for home tab failed: %s", e)
        return None


def _get_priority_alerts(max_per_group=5, max_groups=8):
    """Pull priority alerts from Snowflake with 10-min cache.

    Parameters
    ----------
    max_per_group : int
        Max entries shown per signal group.
    max_groups : int
        Max signal groups to display.
    """
    global _priority_cache

    now = time.time()

    # Return cached data if still fresh — but we need to re-render with
    # the current max_per_group/max_groups, so cache the raw dataframe
    cached_df = None
    if _priority_cache["data"] is not None and (now - _priority_cache["fetched_at"]) < _PRIORITY_CACHE_TTL:
        cached_df = _priority_cache["data"]

    try:
        if cached_df is None:
            from core.snowflake_client import run_query
            from queries.queries import HOME_PRIORITY_ALERTS_QUERY

            df = run_query(HOME_PRIORITY_ALERTS_QUERY)

            if df.empty:
                _priority_cache = {"data": None, "fetched_at": now}
                return None

            _priority_cache = {"data": df, "fetched_at": now}
            cached_df = df

        # Render blocks from dataframe
        return _render_priority_blocks(cached_df, max_per_group, max_groups)

    except Exception as e:
        logger.debug("Priority alerts for home tab failed: %s", e)
        return None


def _touch_line(row_or_item):
    """Return a compact last-call / last-email context line, or ''."""
    parts = []
    for key, label in [("last_call_date", "Call"), ("last_email_date", "Email")]:
        val = row_or_item.get(key) if hasattr(row_or_item, "get") else None
        if val is not None and str(val).strip() not in ("", "None", "NaT", "2000-01-01"):
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


def _render_priority_blocks(df, max_per_group, max_groups):
    """Render priority alert blocks from a cached dataframe."""
    import math

    blocks = []
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:rotating_light: Priority Alerts*"},
    })

    # Group by signal_type
    groups = {}
    for _, row in df.iterrows():
        signal = row.get("signal_type", "")
        if signal not in groups:
            groups[signal] = []
        groups[signal].append(row)

    total_items = 0
    max_total = max_per_group * max_groups
    groups_shown = 0

    def _safe_int(v):
        try:
            f = float(v)
            return 0 if math.isnan(f) else int(f)
        except Exception:
            return 0

    def _acct_link(row):
        acct_name = row.get("account_name", "Unknown")
        acct_id = row.get("account_id", "")
        sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
        return f"<{sf_link}|{acct_name}>" if sf_link else acct_name

    def _pct(paced, base):
        return int(((paced - base) / base) * 100) if base > 0 else 0

    def _presale_line(row):
        """Show AE presale estimates and flag discrepancies vs actual spend."""
        product = str(row.get("product", ""))
        ae_card = _safe_int(row.get("ae_card_presale", 0))
        ae_bp = _safe_int(row.get("ae_bp_presale", 0))
        paced = _safe_int(row.get("paced_amount", 0)) or _safe_int(row.get("spend_l30d", 0))
        presale = ae_card if "Card" in product else ae_bp if "Bill" in product else 0
        if presale <= 0:
            return ""
        if paced > 0 and presale > 0:
            ratio = paced / presale
            if ratio >= 2.0:
                return f"\n   :large_green_circle: _AE presale ${presale:,}/mo — actual {ratio:.1f}x higher → large delta upside_"
            elif ratio <= 0.3:
                return f"\n   :warning: _AE presale ${presale:,}/mo — actual only {int(ratio*100)}% → partial migration, room to grow_"
        return f"\n   _AE presale: ${presale:,}/mo_"

    _btn_counter = [0]

    def _draft_btn(row, signal_type):
        _btn_counter[0] += 1
        payload = json.dumps({
            "account": row.get("account_name", "Unknown"),
            "account_id": row.get("account_id", ""),
            "opp_id": row.get("opportunity_id", ""),
            "product": str(row.get("product", "")),
            "category": {
                "early_accel": "prospect", "close_window": "close_window",
                "leading": "prospect", "first_bill": "zero_to_one",
                "close_now": "close_now", "zero_to_one": "zero_to_one",
                "sustained_accel": "prospect", "treasury_spike": "treasury_spike",
            }.get(signal_type, "prospect"),
        })
        return {
            "type": "button",
            "text": {"type": "plain_text", "text": ":envelope: Draft", "emoji": True},
            "action_id": f"draft_outreach_{signal_type}_{_btn_counter[0]}",
            "value": payload,
        }

    def _view_all_btn(signal_type, label):
        return {
            "type": "button",
            "text": {"type": "plain_text", "text": f"View All {label}", "emoji": True},
            "action_id": f"view_all_{signal_type}",
        }

    def _add_group(sig_type, header, rows_list, format_fn):
        nonlocal total_items, groups_shown
        if not rows_list or total_items >= max_total or groups_shown >= max_groups:
            return
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": header}})
        for row in rows_list[:max_per_group]:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": format_fn(row)},
                "accessory": _draft_btn(row, sig_type),
            })
            total_items += 1
        overflow = len(rows_list) - max_per_group
        if overflow > 0:
            blocks.append({"type": "actions", "elements": [_view_all_btn(sig_type, f"({len(rows_list)})")]})
        groups_shown += 1

    # Signal group formatters
    def _fmt_early(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        paced = _safe_int(row.get("paced_amount", 0))
        base = _safe_int(row.get("baseline_amount", 0))
        l30d = _safe_int(row.get("spend_l30d", 0))
        l7d = _safe_int(row.get("spend_l7d", 0))
        cp = _safe_int(row.get("est_cp", 0))
        pct = _pct(paced, base)
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return (
            f"• {_acct_link(row)} — {product} L7D pacing "
            f"${paced:,}/mo vs ${base:,} baseline (+{pct}%)"
            f"\n   _L30D only ${l30d:,} — window open to lock low baseline_{cp_str}"
            f"\n   _Why: L7D ${l7d:,} is {pct}% above 90D avg, L30D hasn't caught up_"
            f"{_presale_line(row)}{_touch_line(row)}"
        )

    def _fmt_close_window(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        paced = _safe_int(row.get("paced_amount", 0))
        l30d = _safe_int(row.get("spend_l30d", 0))
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return (
            f"• {_acct_link(row)} — {product} L7D pacing "
            f"${paced:,}/mo\n   _Close now — L30D baseline would be ${l30d:,}_{cp_str}"
            f"{_presale_line(row)}{_touch_line(row)}"
        )

    def _fmt_leading(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        paced = _safe_int(row.get("paced_amount", 0))
        base = _safe_int(row.get("baseline_amount", 0))
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        touch = _touch_line(row)
        ps = _presale_line(row)
        if "Card" in product:
            return f"• {_acct_link(row)} — {product} L3D pacing ${paced:,}/mo vs ${base:,}/mo baseline{cp_str}{ps}{touch}"
        return f"• {_acct_link(row)} — ${paced:,} in bills created/scheduled vs ${base:,}/mo baseline{cp_str}{ps}{touch}"

    def _fmt_first_bill(row):
        paced = _safe_int(row.get("paced_amount", 0))
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return (
            f"• {_acct_link(row)} — *first bill created* "
            f"(${paced:,})\n   _Bill Pay opp open — customer just started using the product_{cp_str}"
            f"{_presale_line(row)}{_touch_line(row)}"
        )

    def _fmt_close_now(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        delta = _safe_int(row.get("l30d_spend_delta", 0))
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return f"• {_acct_link(row)} — {product} • L30D +${abs(delta):,} above baseline{cp_str}{_presale_line(row)}{_touch_line(row)}"

    def _fmt_zero_to_one(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        activation_date = row.get("activation_date", "")
        act_str = ""
        if activation_date:
            try:
                if hasattr(activation_date, 'strftime'):
                    act_str = activation_date.strftime("%b %-d")
                else:
                    act_str = str(activation_date)[:10]
            except Exception:
                act_str = str(activation_date)[:10]
        spend_since = _safe_int(row.get("spend_since_opp", 0))
        spend_30 = _safe_int(row.get("spend_l30d", 0))
        spend_7 = _safe_int(row.get("spend_l7d", 0))
        spend_str = (
            f" · ${spend_since:,} since opp · L30D ${spend_30:,} · L7D ${spend_7:,}"
        ) if spend_since or spend_30 or spend_7 else ""
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return f"• {_acct_link(row)} — {product} activated {act_str}{spend_str}{cp_str}{_presale_line(row)}{_touch_line(row)}"

    def _fmt_sustained(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        paced = _safe_int(row.get("paced_amount", 0))
        base = _safe_int(row.get("baseline_amount", 0))
        pct = _pct(paced, base)
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return f"• {_acct_link(row)} — {product} pacing ${paced:,}/mo vs ${base:,} baseline (+{pct}%){cp_str}{_presale_line(row)}{_touch_line(row)}"

    def _fmt_treasury_spike(row):
        paced = _safe_int(row.get("paced_amount", 0))
        l30d = _safe_int(row.get("spend_l30d", 0))
        cp = _safe_int(row.get("est_cp", 0))
        spike_pct = _pct(paced, l30d) if l30d > 0 else 0
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return (
            f"• {_acct_link(row)} — GLA spiked +{spike_pct}%"
            f"\n   L7D avg ${paced:,} vs L30D avg ${l30d:,}{cp_str}"
            f"\n   _Large deposit — lock in treasury expansion (uncapped H1-26)_"
            f"{_touch_line(row)}"
        )

    _add_group("early_accel", ":zap: *Early Acceleration — Act Now*", groups.get("early_accel", []), _fmt_early)
    _add_group("close_window", ":alarm_clock: *Close Window — Opp Ramping*", groups.get("close_window", []), _fmt_close_window)
    _add_group("leading", ":eyes: *Leading Indicator — Spend Incoming*", groups.get("leading", []), _fmt_leading)
    _add_group("first_bill", ":tada: *First Bill Created — Bill Pay Opp Active*", groups.get("first_bill", []), _fmt_first_bill)
    _add_group("close_now", ":money_with_wings: *Close ASAP — Spend Exceeding Baseline*", groups.get("close_now", []), _fmt_close_now)
    _add_group("zero_to_one", ":rocket: *Zero-to-One Activated Since Opp Created*", groups.get("zero_to_one", []), _fmt_zero_to_one)
    _add_group("sustained_accel", ":chart_with_upwards_trend: *Sustained Acceleration — No Open Opp*", groups.get("sustained_accel", []), _fmt_sustained)
    _add_group("treasury_spike", ":moneybag: *Treasury GLA Spike — Large Deposit*", groups.get("treasury_spike", []), _fmt_treasury_spike)

    if total_items == 0:
        return None

    return blocks


def _get_non_spend_signals():
    """Pull non-spend priority signals (stale, reopen, underperforming, followup)
    from the priority_actions cache for the Pipeline tab."""
    try:
        from jobs.priority_actions import get_cached_category

        blocks = []
        _NON_SPEND_CATEGORIES = [
            ("underperforming_d60", ":warning: *Underperforming D60 — Post-Close*"),
            ("underperforming_d30", ":chart_with_downwards_trend: *Underperforming D30 — Post-Close*"),
            ("followup", ":telephone_receiver: *Missing Follow-Up — Post-Meeting*"),
            ("post_meeting_opp", ":bulb: *Post-Meeting Opp — Products Discussed*"),
            ("stale", ":zzz: *Stale Opps — Re-Engage*"),
            ("reopen", ":arrows_counterclockwise: *Re-Open — Worth Revisiting*"),
        ]

        has_content = False
        for category, header in _NON_SPEND_CATEGORIES:
            items = get_cached_category(category)
            if not items:
                continue

            has_content = True
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": header},
            })
            for item in items[:3]:
                acct_name = item.get("account", item.get("account_name", "Unknown"))
                acct_id = item.get("account_id", "")
                sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
                acct_str = f"<{sf_link}|{acct_name}>" if sf_link else acct_name

                product = str(item.get("product", item.get("expansion_subtype", ""))).replace(" Expansion", "")
                detail = item.get("detail", item.get("headline", ""))

                line = f"\u2022 {acct_str}"
                if product:
                    line += f" — {product}"
                if detail:
                    line += f"\n   _{detail}_"
                line += _touch_line(item)

                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": line},
                })

            if len(items) > 3:
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"_...and {len(items) - 3} more — use `/priorities` for full list_"}],
                })

        if not has_content:
            return None
        blocks.insert(0, {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*:clipboard: Non-Spend Signals*"},
        })
        return blocks

    except Exception as e:
        logger.debug("Non-spend signals for pipeline tab failed: %s", e)
        return None


def _get_todays_meetings():
    """Pull ALL of today's meetings from Google Calendar."""
    try:
        from core.google_calendar_client import (
            get_todays_meetings, is_customer_meeting,
            extract_external_attendees, format_meeting_time,
        )

        meetings = get_todays_meetings(max_results=25)
        meetings = [m for m in meetings if is_customer_meeting(m)]
        if not meetings:
            return [{
                "type": "section",
                "text": {"type": "mrkdwn", "text": "_No customer meetings today_"},
            }]

        # Resolve SFDC accounts for each meeting
        _domain_map = {}
        for idx, m in enumerate(meetings):
            external = extract_external_attendees(m)
            for att in external:
                email = att.get("email", "")
                if "@" in email:
                    domain = email.split("@")[1].lower()
                    _domain_map.setdefault(domain, []).append(idx)

        sfdc_links = {}
        if _domain_map:
            try:
                from core.snowflake_client import run_query
                q = f"""
                SELECT sa.account_id, sa.account_name, sa.website
                FROM analytics.marts.dim_sfdc_accounts sa
                JOIN (
                    SELECT DISTINCT account_id
                    FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
                    WHERE date_day = CURRENT_DATE - 1
                      AND owner_name = '{OWNER_NAME}'
                ) ga ON ga.account_id = sa.account_id
                WHERE sa.website IS NOT NULL
                """
                acct_df = run_query(q)
                if not acct_df.empty:
                    for _, arow in acct_df.iterrows():
                        website = str(arow.get("website", "") or "").lower()
                        website_domain = website.replace("https://", "").replace("http://", "").replace("www.", "").strip("/").split("/")[0]
                        acct_id = arow.get("account_id", "")
                        acct_name = arow.get("account_name", "")
                        if website_domain in _domain_map:
                            sf_url = f"https://rampfinancial.lightning.force.com/lightning/r/Account/{acct_id}/view"
                            for midx in _domain_map[website_domain]:
                                if midx not in sfdc_links:
                                    sfdc_links[midx] = (acct_name, sf_url)
            except Exception as e:
                logger.debug("SFDC account lookup for meetings failed: %s", e)

        blocks = []
        for idx, m in enumerate(meetings):
            time_str = format_meeting_time(m.get("start"))
            title = m.get("title", "(No title)")
            already_happened = m.get("already_happened", False)

            status = ":white_check_mark: " if already_happened else ":clock1: "
            line = f"{status}{time_str} — {title}"

            if idx in sfdc_links:
                _, sf_url = sfdc_links[idx]
                line += f"  <{sf_url}|:link:>"

            external = extract_external_attendees(m)
            if external:
                names = [a.get("name", a.get("email", "")) for a in external[:3]]
                line += f"\n_{', '.join(names)}_"

            meeting_payload = {
                "title": title,
                "attendees": [
                    {"email": a.get("email", ""), "name": a.get("name", "")}
                    for a in external[:5]
                ] if external else [],
                "event_id": m.get("event_id", ""),
            }
            payload_str = json.dumps(meeting_payload)

            if already_happened:
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": line}})
                blocks.append({
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": ":briefcase: Pre-Meeting Brief", "emoji": True},
                            "action_id": f"home_brief_{idx}",
                            "value": payload_str,
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": ":memo: Post-Meeting", "emoji": True},
                            "action_id": f"home_post_meeting_{idx}",
                            "value": payload_str,
                        },
                    ],
                })
            else:
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": line},
                    "accessory": {
                        "type": "button",
                        "text": {"type": "plain_text", "text": ":briefcase: Pre-Meeting Brief", "emoji": True},
                        "action_id": f"home_brief_{idx}",
                        "value": payload_str,
                    },
                })

        return blocks

    except Exception as e:
        logger.debug("Today's meetings for home tab failed: %s", e)
        return None


def _get_settings_blocks():
    """Build settings toggle blocks for the home tab."""
    try:
        from utils.settings import load_settings

        settings = load_settings()

        blocks = []
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*:gear: Settings*"},
        })

        dm_alert_keys = [
            ("morning_brief", "Morning Brief"),
            ("post_meeting_dm", "Post-Meeting"),
            ("quota_insights", "Quota Pulse"),
            ("spend_pacing", "Spend Pacing"),
        ]
        dm_alert_keys_2 = [
            ("opp_pacing", "Opp Pacing"),
            ("zero_to_one", "Zero-to-One"),
            ("pipeline_cleanup", "Pipeline"),
            ("proactive_nudge", "Nudge"),
        ]
        signal_keys = [
            ("signal_early_accel", ":zap: Early Accel"),
            ("signal_close_window", ":alarm_clock: Close Window"),
            ("signal_leading", ":eyes: Leading"),
            ("signal_first_bill", ":tada: First Bill"),
            ("signal_treasury_spike", ":moneybag: Treasury"),
        ]
        drafting_keys = [
            ("auto_drafting", "Auto Email Drafts"),
        ]

        def _toggle_button(key, label, current_val):
            icon = "\u2705" if current_val else "\u2b1c"
            return {
                "type": "button",
                "text": {"type": "plain_text", "text": f"{icon} {label}", "emoji": True},
                "action_id": f"settings_toggle_{key}",
            }

        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "*DM Alerts*"}],
        })
        blocks.append({
            "type": "actions",
            "elements": [
                _toggle_button(key, label, settings.get(key, True))
                for key, label in dm_alert_keys
            ],
        })
        blocks.append({
            "type": "actions",
            "elements": [
                _toggle_button(key, label, settings.get(key, True))
                for key, label in dm_alert_keys_2
            ],
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "*Real-Time Signal Alerts*"}],
        })
        blocks.append({
            "type": "actions",
            "elements": [
                _toggle_button(key, label, settings.get(key, True))
                for key, label in signal_keys
            ],
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "*Drafting*"}],
        })
        blocks.append({
            "type": "actions",
            "elements": [
                _toggle_button(key, label, settings.get(key, True))
                for key, label in drafting_keys
            ],
        })

        return blocks

    except Exception as e:
        logger.debug("Settings blocks failed: %s", e)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# ACTION HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

def register_home_tab_actions(app):
    """Register button actions from the Home tab."""
    from config import GREG_SLACK_ID

    # ── Tab switching ────────────────────────────────────────────────────
    @app.action({"action_id": re.compile(r"^home_tab_switch_")})
    def handle_tab_switch(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        action_id = action.get("action_id", "")
        tab_name = action_id.replace("home_tab_switch_", "", 1)
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        _active_tab[user_id] = tab_name

        # Publish an immediate loading state so the tab switch feels responsive
        try:
            loading_blocks = _build_home_blocks_header(tab_name)
            loading_blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": ":hourglass_flowing_sand: Loading..."},
            })
            client.views_publish(
                user_id=user_id,
                view={"type": "home", "blocks": loading_blocks},
            )
        except Exception:
            pass  # Non-critical — continue to full refresh

        def _refresh():
            try:
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(
                    user_id=user_id,
                    view={"type": "home", "blocks": blocks},
                )
            except Exception as e:
                logger.error("Home tab switch failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    # ── Flush drafts button ──────────────────────────────────────────────
    @app.action("home_flush_drafts")
    def handle_flush_drafts(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        def _flush():
            try:
                from utils.pending_drafts import flush_to_gmail, list_pending
                pending = list_pending()
                if not pending:
                    client.chat_postMessage(channel=GREG_SLACK_ID, text="No pending drafts to flush.")
                    return
                client.chat_postMessage(
                    channel=GREG_SLACK_ID,
                    text=f"Flushing {len(pending)} pending draft{'s' if len(pending) != 1 else ''} to Gmail...",
                )
                succeeded, failed = flush_to_gmail()
                msg = f"Flush complete — {succeeded} draft{'s' if succeeded != 1 else ''} created in Gmail."
                if failed:
                    msg += f" {failed} failed (check Gumstack auth at gumloop.com/personal/apps)."
                client.chat_postMessage(channel=GREG_SLACK_ID, text=msg)

                # Refresh the drafts tab
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("Home tab flush drafts failed: %s", e)
                client.chat_postMessage(channel=GREG_SLACK_ID, text=f"Draft flush failed: {e}")

        threading.Thread(target=_flush, daemon=True).start()

    # ── Quick action buttons ─────────────────────────────────────────────
    _ACTION_MAP = {
        "home_run_priorities": ("jobs.priority_actions", "run_priority_actions"),
        "home_run_quota": ("jobs.quota_heartbeat", "run_quota_heartbeat"),
        "home_run_morning": ("jobs.morning_brief", "run_morning_brief"),
        "home_run_nudge": ("jobs.proactive_nudge", "run_proactive_nudge"),
        "home_run_spend": ("jobs.spend_pacing", "run_spend_pacing"),
        "home_run_cleanup": ("jobs.pipeline_cleanup", "run_pipeline_cleanup"),
        "home_run_zero_to_one": ("jobs.zero_to_one", "run_zero_to_one"),
        "home_run_forecast": ("jobs.forecasting", "run_forecasting"),
        "home_run_quota_insights": ("jobs.quota_insights", "run_quota_insights"),
    }

    for action_id, (module_path, func_name) in _ACTION_MAP.items():
        def _make_handler(mod_path, fn_name):
            def handler(ack, body, client):
                ack()
                def _run():
                    try:
                        import importlib
                        mod = importlib.import_module(mod_path)
                        fn = getattr(mod, fn_name)
                        fn(client)
                    except Exception as e:
                        logger.error("Home tab action %s failed: %s", fn_name, e)
                        client.chat_postMessage(
                            channel=GREG_SLACK_ID,
                            text=f"Home tab action failed: {e}",
                        )
                threading.Thread(target=_run, daemon=True).start()
            return handler

        app.action(action_id)(_make_handler(module_path, func_name))
