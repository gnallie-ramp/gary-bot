"""Gary Bot status, health check, and self-test utilities."""
from __future__ import annotations

import logging
import time
from datetime import datetime

from config import GREG_SLACK_ID, NTR_RATES, DEDUP_STATE_FILE, TIMEZONE, COMMAND_PREFIX
from core.slack_formatter import simple_dm_blocks, format_currency, dashboard_url
from utils.dedup import tracker

logger = logging.getLogger(__name__)


def _check_snowflake() -> tuple[bool, str]:
    """Test Snowflake connectivity."""
    try:
        from core.snowflake_client import run_query
        df = run_query("SELECT CURRENT_TIMESTAMP() AS ts, CURRENT_ROLE() AS role")
        if df.empty:
            return False, "Query returned empty"
        ts = df.iloc[0].get("ts", "?")
        role = df.iloc[0].get("role", "?")
        return True, f"OK — {role} at {ts}"
    except Exception as e:
        return False, f"FAILED: {e}"


def _check_gmail(user_id: str | None = None) -> tuple[bool, str]:
    """Test Gmail connectivity via Gumstack MCP."""
    try:
        from core.gumstack_gmail import read_emails, is_available

        if not is_available(user_id=user_id):
            return False, "Gumstack Gmail tokens not found"

        results = read_emails("in:inbox", max_results=1, user_id=user_id)
        # If we get here without exception, the connection is healthy
        return True, f"Gumstack Gmail OK ({len(results)} test result(s))"
    except ImportError:
        return False, "gumstack_gmail not available"
    except Exception as e:
        return False, f"FAILED: {e}"


def _dedup_stats() -> str:
    """Return dedup tracker stats."""
    try:
        state = tracker._state
        total = len(state)
        if not state:
            return "0 tracked keys"
        newest = max(state.values())
        age = (time.time() - newest) / 3600
        return f"{total} tracked keys, newest {age:.1f}h ago"
    except Exception:
        return "unknown"


def run_status(client, user_id=None, force: bool = False):
    """Post a status/health-check DM to Greg."""
    dm_target = user_id or GREG_SLACK_ID

    now = datetime.now()
    date_str = now.strftime("%b %d, %Y %I:%M %p")

    # Connection checks
    sf_ok, sf_msg = _check_snowflake()
    gmail_ok, gmail_msg = _check_gmail(user_id=dm_target)
    dedup_msg = _dedup_stats()

    # Scheduled jobs info
    job_schedule = [
        ("Pipeline Cleanup", "Daily 7:30 AM PT"),
        ("Morning Brief", "Daily 7:45 AM PT"),
        ("Opp Pacing", "Daily 8:00 AM PT"),
        ("Activity Report", "Daily 8:30 AM PT"),
        ("Spend Pacing", "Daily 9:00 AM PT"),
        ("Post-Close Monitor", "Daily 10:00 AM PT"),
        ("Proactive Nudge", "Every 2h, 10AM-4PM PT"),
        ("Post-Meeting", "Every 2h, 8AM-6PM PT"),
        ("Zero-to-One", "Every 4h, 8AM-4PM PT"),
        ("Quota Heartbeat", "Daily 6:00 PM PT"),
        ("Forecasting", "Monday 7:00 AM PT"),
    ]

    # Build status message
    sf_icon = "\u2705" if sf_ok else "\u274c"
    gmail_icon = "\u2705" if gmail_ok else "\u26a0\ufe0f"

    lines = [
        f"*Status as of {date_str}*\n",
        f"{sf_icon} *Snowflake:* {sf_msg}",
        f"{gmail_icon} *Gmail:* {gmail_msg}",
        f"\U0001f4be *Dedup:* {dedup_msg}\n",
        "*Scheduled Jobs:*",
    ]
    for name, schedule in job_schedule:
        lines.append(f"  \u2022 {name} — {schedule}")

    lines.append("\n*On-Demand Commands:*")
    lines.append(f"  `/{COMMAND_PREFIX}-priorities` — *ranked priority actions (start here)*")
    lines.append(f"  `/{COMMAND_PREFIX}-nudge` — what's new + highest-value suggestions")
    lines.append(f"  `/{COMMAND_PREFIX}-spend-pacing` — MTD vs last month, YoY, trajectory")
    lines.append(f"  `/{COMMAND_PREFIX}-activity-report` — SQLs created + opps closed by product")
    lines.append(f"  `/{COMMAND_PREFIX}-top-accounts` — Top 50 accounts ranked by CP potential")
    lines.append(f"  `/{COMMAND_PREFIX}-batch-outreach` — cluster accounts + draft campaigns")
    lines.append(f"  `/{COMMAND_PREFIX}-zero-to-one` — check activations now")
    lines.append(f"  `/{COMMAND_PREFIX}-opp-pacing` — pacing report now")
    lines.append(f"  `/{COMMAND_PREFIX}-pipeline-cleanup` — cleanup analysis")
    lines.append(f"  `/{COMMAND_PREFIX}-post-meeting [days]` — post-meeting check")
    lines.append(f"  `/{COMMAND_PREFIX}-post-close` — post-close CP + activation tracking")
    lines.append(f"  `/{COMMAND_PREFIX}-quota-heartbeat` — CP attainment + accelerator band")
    lines.append(f"  `/{COMMAND_PREFIX}-forecast` — weekly forecast")
    lines.append(f"  `/{COMMAND_PREFIX}-morning-brief` — combined daily action summary")
    lines.append(f"  `/{COMMAND_PREFIX}-lookup <name>` — account snapshot")
    lines.append(f"  `/{COMMAND_PREFIX}-brief <name>` — pre-call brief")
    lines.append(f"  `/{COMMAND_PREFIX}-opps` — open opp summary")
    lines.append(f"  `/{COMMAND_PREFIX}-status` — this health check")
    lines.append(f"  `/{COMMAND_PREFIX}-help` — full command reference")
    lines.append(f"  `/{COMMAND_PREFIX}-test` — run all jobs in test mode")
    lines.append("\n*DM me naturally:*")
    lines.append('  "what\'s new" / "priorities" / "spend pacing" / "look up Acme Corp"')

    _dash_url = dashboard_url("priority")
    lines.append(f"\n*Dashboard:* <{_dash_url}|Open Dashboard>")

    blocks = simple_dm_blocks("Gary Bot Status", "\n".join(lines))
    client.chat_postMessage(
        channel=dm_target,
        blocks=blocks,
        text="Gary Bot status check",
    )
    logger.info("Status check sent")


def run_help(client, user_id=None):
    """Post a comprehensive help/capability message."""
    dm_target = user_id or GREG_SLACK_ID

    lines = [
        "*Everything I can do:*\n",
        "*\U0001f50d Account Intelligence*",
        f"  `/{COMMAND_PREFIX}-lookup <name>` — Products, L30D spend, contacts",
        f"  `/{COMMAND_PREFIX}-brief <name>` — AI pre-call brief (15-30 sec)",
        f"  `/{COMMAND_PREFIX}-opps` — All open opps with CP estimates",
        f"  `/{COMMAND_PREFIX}-top-accounts` — Top 50 accounts ranked by CP potential",
        '  DM: "look up Acme Corp" / "tell me about Beta LLC"\n',
        "*\U0001f3af Priority Actions + Nudges*",
        f"  `/{COMMAND_PREFIX}-priorities` — *Combined ranked list of everything* (start here)",
        f"  `/{COMMAND_PREFIX}-nudge` — What's new since last check + top suggestions",
        f"  `/{COMMAND_PREFIX}-batch-outreach` — Cluster similar accounts + draft batch campaigns",
        "  DM: \"what should I focus on?\" / \"what's new\" / \"suggestions\"\n",
        "*\U0001f4c8 Pacing + Performance*",
        f"  `/{COMMAND_PREFIX}-spend-pacing` — MTD vs last month, YoY, L7D trajectory",
        f"  `/{COMMAND_PREFIX}-opp-pacing` — Open opps pacing vs baseline",
        f"  `/{COMMAND_PREFIX}-quota-heartbeat` — CP attainment + accelerator band",
        f"  `/{COMMAND_PREFIX}-activity-report` — SQLs created + opps closed by product",
        f"  `/{COMMAND_PREFIX}-forecast` — Weekly pipeline forecast\n",
        "*\u26a1 Pipeline + Signals*",
        f"  `/{COMMAND_PREFIX}-zero-to-one` — New product activations",
        f"  `/{COMMAND_PREFIX}-pipeline-cleanup` — Stale/miscategorized opps",
        f"  `/{COMMAND_PREFIX}-post-meeting [days]` — Gong calls missing follow-up/opp",
        f"  `/{COMMAND_PREFIX}-post-close` — Post-close CP + activation tracking",
        f"  `/{COMMAND_PREFIX}-morning-brief` — Combined daily action summary\n",
        "*\u26a1 Scheduled Alerts*",
        "  \u2022 Pipeline Cleanup (7:30AM) · Morning Brief (7:45AM) · Opp Pacing (8AM)",
        "  \u2022 Activity Report (8:30AM) · Spend Pacing (9AM) · Post-Close (10AM)",
        "  \u2022 Proactive Nudge (10:30/12:30/2:30/4:30PM) · Post-Meeting (every 2h)",
        "  \u2022 Zero-to-One (every 4h) · Quota Heartbeat (6PM) · Forecast (Mon 7AM)\n",
        "*\U0001f6e0\ufe0f Utilities*",
        f"  `/{COMMAND_PREFIX}-status` — Health check + connection status",
        f"  `/{COMMAND_PREFIX}-help` — This message",
        f"  `/{COMMAND_PREFIX}-test` — Test all connections + run all jobs\n",
        "*\U0001f4ac DM me anything*",
        '  I understand natural language. Try "what should I focus on today?", '
        '"what\'s new", "spend pacing", or "look up Acme Corp"\n',
        "*\U0001f4ca Dashboard Pages*",
        f"  <{dashboard_url('priority')}|Priority Actions> — Ranked daily actions",
        f"  <{dashboard_url('pipeline')}|Pipeline> — Close Now + Zero-to-One + Opps to Watch",
        f"  <{dashboard_url('meeting-prep')}|Meeting Prep> — Per-product account cards + pre-call data",
        f"  <{dashboard_url('prospecting')}|Prospecting> — No-opp accelerating accounts",
        f"  <{dashboard_url('performance')}|Performance> — Quota attainment + CP tracking",
    ]

    blocks = simple_dm_blocks("Gary Bot — Full Capabilities", "\n".join(lines))
    client.chat_postMessage(
        channel=dm_target,
        blocks=blocks,
        text="Gary Bot capabilities",
    )


def run_test(client, user_id=None):
    """Run all jobs in test/force mode and report results."""
    dm_target = user_id or GREG_SLACK_ID

    client.chat_postMessage(
        channel=dm_target,
        text="\U0001f9ea *Running full test suite...*\nThis will take 1-2 minutes.",
    )

    results = []

    # Test Snowflake
    sf_ok, sf_msg = _check_snowflake()
    results.append(("\u2705" if sf_ok else "\u274c", "Snowflake", sf_msg))

    # Test Gmail
    gmail_ok, gmail_msg = _check_gmail(user_id=dm_target)
    results.append(("\u2705" if gmail_ok else "\u26a0\ufe0f", "Gmail", gmail_msg))

    # Test each job
    job_tests = [
        ("Zero-to-One", "jobs.zero_to_one", "run_zero_to_one"),
        ("Opp Pacing", "jobs.opp_pacing", "run_opp_pacing"),
        ("Pipeline Cleanup", "jobs.pipeline_cleanup", "run_pipeline_cleanup"),
        ("Post-Meeting", "jobs.post_meeting", "run_post_meeting"),
        ("Quota Heartbeat", "jobs.quota_heartbeat", "run_quota_heartbeat"),
        ("Forecasting", "jobs.forecasting", "run_forecasting"),
    ]

    for name, module_name, func_name in job_tests:
        try:
            import importlib
            mod = importlib.import_module(module_name)
            func = getattr(mod, func_name)
            if func_name == "run_post_meeting":
                func(client, user_id=dm_target, lookback_days=2, force=True)
            else:
                func(client, user_id=dm_target, force=True)
            results.append(("\u2705", name, "Ran successfully"))
        except Exception as e:
            results.append(("\u274c", name, f"FAILED: {e}"))

    # Summary
    lines = ["*Test Results:*\n"]
    passed = sum(1 for icon, _, _ in results if icon == "\u2705")
    total = len(results)
    for icon, name, msg in results:
        lines.append(f"  {icon} *{name}:* {msg}")
    lines.append(f"\n*{passed}/{total} passed*")

    blocks = simple_dm_blocks("Gary Bot Test Results", "\n".join(lines))
    client.chat_postMessage(
        channel=dm_target,
        blocks=blocks,
        text=f"Gary Bot test: {passed}/{total} passed",
    )
    logger.info("Test suite complete: %d/%d passed", passed, total)
