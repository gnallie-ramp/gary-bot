"""Gary Bot status, health check, and self-test utilities."""
from __future__ import annotations

import logging
import time
from datetime import datetime

from config import GREG_SLACK_ID, NTR_RATES, DEDUP_STATE_FILE, TIMEZONE
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


def _check_gmail() -> tuple[bool, str]:
    """Test Gmail API connectivity."""
    try:
        from core.gmail_client import check_connection, check_imap_connection
        api_ok, api_msg = check_imap_connection()

        if api_ok:
            return True, f"Gmail API OK ({api_msg})"
        return False, f"Gmail API: {api_msg}"
    except ImportError:
        return False, "gmail_client not available"
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


def run_status(client, force: bool = False):
    """Post a status/health-check DM to Greg."""
    now = datetime.now()
    date_str = now.strftime("%b %d, %Y %I:%M %p")

    # Connection checks
    sf_ok, sf_msg = _check_snowflake()
    gmail_ok, gmail_msg = _check_gmail()
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
    lines.append("  `/priorities` — *ranked priority actions (start here)*")
    lines.append("  `/nudge` — what's new + highest-value suggestions")
    lines.append("  `/spend-pacing` — MTD vs last month, YoY, trajectory")
    lines.append("  `/activity-report` — SQLs created + opps closed by product")
    lines.append("  `/top-accounts` — Top 50 accounts ranked by CP potential")
    lines.append("  `/batch-outreach` — cluster accounts + draft campaigns")
    lines.append("  `/zero-to-one` — check activations now")
    lines.append("  `/opp-pacing` — pacing report now")
    lines.append("  `/pipeline-cleanup` — cleanup analysis")
    lines.append("  `/post-meeting [days]` — post-meeting check")
    lines.append("  `/post-close` — post-close CP + activation tracking")
    lines.append("  `/quota-heartbeat` — CP attainment + accelerator band")
    lines.append("  `/forecast` — weekly forecast")
    lines.append("  `/morning-brief` — combined daily action summary")
    lines.append("  `/gary-lookup <name>` — account snapshot")
    lines.append("  `/gary-brief <name>` — pre-call brief")
    lines.append("  `/gary-opps` — open opp summary")
    lines.append("  `/gary-status` — this health check")
    lines.append("  `/gary-help` — full command reference")
    lines.append("  `/gary-test` — run all jobs in test mode")
    lines.append("\n*DM me naturally:*")
    lines.append('  "what\'s new" / "priorities" / "spend pacing" / "look up Acme Corp"')

    _dash_url = dashboard_url("priority")
    lines.append(f"\n*Dashboard:* <{_dash_url}|Open Dashboard>")

    blocks = simple_dm_blocks("Gary Bot Status", "\n".join(lines))
    client.chat_postMessage(
        channel=GREG_SLACK_ID,
        blocks=blocks,
        text="Gary Bot status check",
    )
    logger.info("Status check sent")


def run_help(client):
    """Post a comprehensive help/capability message."""
    lines = [
        "*Everything I can do:*\n",
        "*\U0001f50d Account Intelligence*",
        "  `/gary-lookup <name>` — Products, L30D spend, contacts",
        "  `/gary-brief <name>` — AI pre-call brief (15-30 sec)",
        "  `/gary-opps` — All open opps with CP estimates",
        "  `/top-accounts` — Top 50 accounts ranked by CP potential",
        '  DM: "look up Acme Corp" / "tell me about Beta LLC"\n',
        "*\U0001f3af Priority Actions + Nudges*",
        "  `/priorities` — *Combined ranked list of everything* (start here)",
        "  `/nudge` — What's new since last check + top suggestions",
        "  `/batch-outreach` — Cluster similar accounts + draft batch campaigns",
        "  DM: \"what should I focus on?\" / \"what's new\" / \"suggestions\"\n",
        "*\U0001f4c8 Pacing + Performance*",
        "  `/spend-pacing` — MTD vs last month, YoY, L7D trajectory",
        "  `/opp-pacing` — Open opps pacing vs baseline",
        "  `/quota-heartbeat` — CP attainment + accelerator band",
        "  `/activity-report` — SQLs created + opps closed by product",
        "  `/forecast` — Weekly pipeline forecast\n",
        "*\u26a1 Pipeline + Signals*",
        "  `/zero-to-one` — New product activations",
        "  `/pipeline-cleanup` — Stale/miscategorized opps",
        "  `/post-meeting [days]` — Gong calls missing follow-up/opp",
        "  `/post-close` — Post-close CP + activation tracking",
        "  `/morning-brief` — Combined daily action summary\n",
        "*\u26a1 Scheduled Alerts*",
        "  \u2022 Pipeline Cleanup (7:30AM) · Morning Brief (7:45AM) · Opp Pacing (8AM)",
        "  \u2022 Activity Report (8:30AM) · Spend Pacing (9AM) · Post-Close (10AM)",
        "  \u2022 Proactive Nudge (10:30/12:30/2:30/4:30PM) · Post-Meeting (every 2h)",
        "  \u2022 Zero-to-One (every 4h) · Quota Heartbeat (6PM) · Forecast (Mon 7AM)\n",
        "*\U0001f6e0\ufe0f Utilities*",
        "  `/gary-status` — Health check + connection status",
        "  `/gary-help` — This message",
        "  `/gary-test` — Test all connections + run all jobs\n",
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
        channel=GREG_SLACK_ID,
        blocks=blocks,
        text="Gary Bot capabilities",
    )


def run_test(client):
    """Run all jobs in test/force mode and report results."""
    client.chat_postMessage(
        channel=GREG_SLACK_ID,
        text="\U0001f9ea *Running full test suite...*\nThis will take 1-2 minutes.",
    )

    results = []

    # Test Snowflake
    sf_ok, sf_msg = _check_snowflake()
    results.append(("\u2705" if sf_ok else "\u274c", "Snowflake", sf_msg))

    # Test Gmail
    gmail_ok, gmail_msg = _check_gmail()
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
                func(client, lookback_days=2, force=True)
            else:
                func(client, force=True)
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
        channel=GREG_SLACK_ID,
        blocks=blocks,
        text=f"Gary Bot test: {passed}/{total} passed",
    )
    logger.info("Test suite complete: %d/%d passed", passed, total)
