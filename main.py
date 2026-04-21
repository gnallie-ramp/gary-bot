"""Gary Bot — Greg's unified sales intelligence Slack bot.

Entry point: registers handlers, starts scheduler, starts socket mode.
"""

import atexit
import logging
import os
import sys
import time

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from config import SLACK_BOT_TOKEN, SLACK_APP_TOKEN, TIMEZONE

# ── PID Lock — prevent duplicate instances ────────────────────────────────────
_PID_FILE = os.path.expanduser("~/.gary_bot.pid")


def _acquire_pid_lock():
    """Ensure only one Gary Bot instance is running. Kill stale if needed."""
    if os.path.exists(_PID_FILE):
        try:
            old_pid = int(open(_PID_FILE).read().strip())
            # Check if the old process is still alive
            os.kill(old_pid, 0)
            # It's alive — kill it so we can take over
            print(f"Killing existing Gary Bot (PID {old_pid})...")
            os.kill(old_pid, 9)
            time.sleep(2)
        except (ProcessLookupError, ValueError):
            pass  # Stale PID file, process already dead
        except PermissionError:
            print(f"Cannot kill existing process {old_pid}. Exiting.")
            sys.exit(1)

    # Write our PID
    with open(_PID_FILE, "w") as f:
        f.write(str(os.getpid()))


def _release_pid_lock():
    """Remove PID file on exit."""
    try:
        if os.path.exists(_PID_FILE):
            stored_pid = int(open(_PID_FILE).read().strip())
            if stored_pid == os.getpid():
                os.remove(_PID_FILE)
    except Exception:
        pass


_acquire_pid_lock()
atexit.register(_release_pid_lock)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.expanduser("~/.gary_bot.log")),
    ],
)
logger = logging.getLogger("gary_bot")

# ── Slack Bolt App ────────────────────────────────────────────────────────────
app = App(token=SLACK_BOT_TOKEN)


# ── Register Handlers ────────────────────────────────────────────────────────
def _register_all_handlers():
    """Register channel monitors, slash commands, and interactive handlers."""
    from handlers.channel_monitors import register_channel_listeners, resolve_channel_ids
    from handlers.slash_commands import register_slash_commands
    from handlers.interactive import register_interactive_handlers
    from handlers.home_tab import register_home_tab, register_home_tab_actions

    # Resolve channel IDs so @gary draft and bill drafter sweep work
    resolve_channel_ids(app.client)

    # Register event listeners (DM + app_mention + channel alerts) and commands
    register_channel_listeners(app)
    register_slash_commands(app)
    register_interactive_handlers(app)
    register_home_tab(app)
    register_home_tab_actions(app)

    logger.info("All handlers registered")


# ── Scheduler ─────────────────────────────────────────────────────────────────
def _start_scheduler():
    """Start APScheduler with all scheduled jobs."""
    scheduler = BackgroundScheduler(timezone=TIMEZONE)

    # Import jobs
    from jobs.opp_pacing import run_opp_pacing
    from jobs.pipeline_cleanup import run_pipeline_cleanup
    from jobs.post_meeting import run_post_meeting
    from jobs.quota_heartbeat import run_quota_heartbeat
    from jobs.forecasting import run_forecasting
    from jobs.zero_to_one import run_zero_to_one
    from jobs.morning_brief import run_morning_brief
    from jobs.spend_pacing import run_spend_pacing
    from jobs.post_close_monitor import run_post_close_monitor
    from jobs.activity_report import run_activity_report
    from jobs.proactive_nudge import run_proactive_nudge
    from jobs.pre_meeting_brief import run_pre_meeting_brief
    from jobs.post_meeting_followup import run_post_meeting_followup
    from jobs.granola_followup import run_granola_followup
    from jobs.acceleration_alert import run_acceleration_alert
    from jobs.prospecting_signals import run_prospecting_refresh
    from jobs.top_cp_refresh import run_top_cp_refresh
    from jobs.activation_alerts import run_activation_alerts
    from handlers.channel_monitors import (
        run_bill_drafter_sweep, run_auto_card_sweep,
        run_pclip_sweep, run_rclip_sweep, run_escalation_sweep,
    )
    # from jobs.auth_alert import run_auth_alert  # DISABLED
    from jobs.draft_reminder import run_draft_reminder
    from jobs.quota_insights import run_quota_insights

    # Wrapper to pass Slack client — loops over all registered users
    def _wrap(fn, **kwargs):
        def _job():
            from core.user_registry import get_all_users
            users = get_all_users()
            for uid in users:
                try:
                    fn(app.client, user_id=uid, **kwargs)
                except Exception as e:
                    logger.error("Job %s failed for user %s: %s", fn.__name__, uid, e)
        return _job

    # NOTE: Stale opp drafter disabled — email drafting handled by cowork.
    # Re-enable by uncommenting:
    #   from jobs.stale_opp_drafter import run_stale_opp_drafter
    #   scheduler.add_job(
    #       _wrap(run_stale_opp_drafter),
    #       CronTrigger(hour=7, minute=0),
    #       id="stale_opp_drafter",
    #       name="Stale Opp Re-Engage Drafter",
    #   )

    # ── Intelligence alerts ──
    # Pipeline cleanup: Daily 7:30 AM PT
    scheduler.add_job(
        _wrap(run_pipeline_cleanup),
        CronTrigger(hour=7, minute=30),
        id="pipeline_cleanup",
        name="Pipeline Cleanup",
    )

    # Opp pacing: Daily 8:00 AM PT
    scheduler.add_job(
        _wrap(run_opp_pacing),
        CronTrigger(hour=8, minute=0),
        id="opp_pacing",
        name="Opp Pacing Alert",
    )

    # Post-meeting to-do: Every 2 hours, 8AM-6PM PT
    scheduler.add_job(
        _wrap(run_post_meeting, lookback_days=2),
        CronTrigger(hour="8,10,12,14,16,18", minute=0),
        id="post_meeting",
        name="Post-Meeting To-Do",
    )

    # Quota heartbeat: Daily 6:00 PM PT
    scheduler.add_job(
        _wrap(run_quota_heartbeat),
        CronTrigger(hour=18, minute=0),
        id="quota_heartbeat",
        name="Quota Heartbeat",
    )

    # Forecasting: Monday 7:00 AM PT
    scheduler.add_job(
        _wrap(run_forecasting),
        CronTrigger(day_of_week="mon", hour=7, minute=0),
        id="forecasting",
        name="Weekly Forecasting",
    )

    # Zero-to-one activations: Every 4 hours, 8AM-4PM PT
    scheduler.add_job(
        _wrap(run_zero_to_one),
        CronTrigger(hour="8,12,16", minute=15),
        id="zero_to_one",
        name="Zero-to-One Activations",
    )

    # Acceleration alert: Every 30 min, weekdays 5AM-5PM PT — real-time urgent signals
    scheduler.add_job(
        _wrap(run_acceleration_alert),
        CronTrigger(day_of_week="mon-fri", hour="5-17", minute="0,30"),
        id="acceleration_alert",
        name="Acceleration Alert (Real-time)",
    )

    # Acceleration daily summary: 5:00 AM PT (8:00 AM ET) — full morning digest
    scheduler.add_job(
        _wrap(run_acceleration_alert, daily=True),
        CronTrigger(day_of_week="mon-fri", hour=5, minute=0),
        id="acceleration_daily",
        name="Acceleration Alert (Daily Summary)",
    )

    # Morning brief: Daily 7:45 AM PT — combined action summary
    scheduler.add_job(
        _wrap(run_morning_brief),
        CronTrigger(hour=7, minute=45),
        id="morning_brief",
        name="Morning Brief",
    )

    # Spend pacing: Daily 9:00 AM PT — MTD vs last month + YoY
    scheduler.add_job(
        _wrap(run_spend_pacing),
        CronTrigger(hour=9, minute=0),
        id="spend_pacing",
        name="Spend Pacing Intelligence",
    )

    # Post-close monitor: Daily 10:00 AM PT — activation + CP tracking
    scheduler.add_job(
        _wrap(run_post_close_monitor),
        CronTrigger(hour=10, minute=0),
        id="post_close_monitor",
        name="Post-Close CP Monitor",
    )

    # Activity report: Daily 8:30 AM PT — SQLs + CWs by product
    scheduler.add_job(
        _wrap(run_activity_report),
        CronTrigger(hour=8, minute=30),
        id="activity_report",
        name="Activity Report",
    )

    # Proactive nudge: Every 2 hours, 10AM-4PM PT — high-value suggestions
    scheduler.add_job(
        _wrap(run_proactive_nudge),
        CronTrigger(hour="10,12,14,16", minute=30),
        id="proactive_nudge",
        name="Proactive Nudge",
    )

    # Bill drafter sweep: Every 30 min, weekdays 8AM-6PM PT
    scheduler.add_job(
        _wrap(run_bill_drafter_sweep),
        CronTrigger(day_of_week="mon-fri", hour="8-18", minute="0,30"),
        id="bill_drafter_sweep",
        name="Bill Drafter Sweep",
    )

    # Auto card loss sweep: Every 30 min, weekdays 8AM-6PM PT
    scheduler.add_job(
        _wrap(run_auto_card_sweep),
        CronTrigger(day_of_week="mon-fri", hour="8-18", minute="0,30"),
        id="auto_card_sweep",
        name="Auto Card Sweep",
    )

    # PCLIP sweep: Every 30 min, weekdays 8AM-6PM PT
    scheduler.add_job(
        _wrap(run_pclip_sweep),
        CronTrigger(day_of_week="mon-fri", hour="8-18", minute="0,30"),
        id="pclip_sweep",
        name="PCLIP Sweep",
    )

    # RCLIP sweep: Every 30 min, weekdays 8AM-6PM PT
    scheduler.add_job(
        _wrap(run_rclip_sweep),
        CronTrigger(day_of_week="mon-fri", hour="8-18", minute="0,30"),
        id="rclip_sweep",
        name="RCLIP Sweep",
    )

    # AM Escalation sweep: Every 30 min, weekdays 8AM-6PM PT
    scheduler.add_job(
        _wrap(run_escalation_sweep),
        CronTrigger(day_of_week="mon-fri", hour="8-18", minute="0,30"),
        id="escalation_sweep",
        name="AM Escalation Sweep",
    )

    # Prospecting signal refresh: Daily 8:15 AM PT
    # Surfaces accounts matching hot plays not contacted in 30+ days
    scheduler.add_job(
        _wrap(run_prospecting_refresh),
        CronTrigger(day_of_week="mon-fri", hour=8, minute=15),
        id="prospecting_refresh",
        name="Prospecting Signal Refresh",
    )

    # Top-CP Re-engage refresh: Daily 6:00 AM PT
    # Surfaces top 20 active (+ 10 churned) accounts by CP potential, excluding
    # any with a closed-won expansion in the last 90 days
    scheduler.add_job(
        _wrap(run_top_cp_refresh),
        CronTrigger(day_of_week="mon-fri", hour=6, minute=0),
        id="top_cp_refresh",
        name="Top-CP Re-engage Refresh",
    )

    # Plays refresh: Weekdays 10 AM + 2 PM PT (third layer; also runs on
    # startup-warm + on-demand when the Prospecting tab opens with stale cache)
    from jobs.plays_refresh import refresh_all as run_plays_refresh_all
    scheduler.add_job(
        _wrap(run_plays_refresh_all),
        CronTrigger(day_of_week="mon-fri", hour="10,14", minute=0),
        id="plays_refresh",
        name="Plays Refresh",
    )

    # Play discovery: Weekly Monday 7 AM PT — scans #gam-ask-ai for Ramp
    # Research queries asked 2+ times, DMs a candidate-plays digest.
    from jobs.discover_plays import run_play_discovery
    scheduler.add_job(
        _wrap(run_play_discovery),
        CronTrigger(day_of_week="mon", hour=7, minute=0),
        id="play_discovery",
        name="Play Discovery",
    )

    # Deal Anatomy batch: Nightly 2 AM PT — Claude analyzes CW expansion
    # deals (realized CP ≥ $500) with transcripts + email bodies, caches JSON.
    from jobs.deal_anatomy import run_batch as run_deal_anatomy_batch
    scheduler.add_job(
        _wrap(run_deal_anatomy_batch),
        CronTrigger(hour=2, minute=0),
        id="deal_anatomy_batch",
        name="Deal Anatomy Batch",
    )

    # Play Library rebuild: Nightly 3 AM PT (after Deal Anatomy batch
    # finishes). Aggregates per-deal JSON files into the Play Pattern Library
    # used by the Plays tab's Team Evidence footer.
    from jobs.play_library import rebuild as run_play_library_rebuild
    scheduler.add_job(
        _wrap(run_play_library_rebuild),
        CronTrigger(hour=3, minute=0),
        id="play_library_rebuild",
        name="Play Library Rebuild",
    )

    # Hot List rebuild: Nightly 3:30 AM PT (after Play Library). Ranks every
    # user's BoB accounts by (play match × team success rate) and caches
    # the top 100 per user so the Hot List tab opens instantly.
    from jobs.hot_list import rebuild as run_hot_list_rebuild
    scheduler.add_job(
        _wrap(run_hot_list_rebuild),
        CronTrigger(hour=3, minute=30),
        id="hot_list_rebuild",
        name="Hot List Rebuild",
    )

    # Activation alerts: Every 2 hours, weekdays 8AM-6PM PT
    # Detects new treasury, investment, first bill activations and DMs immediately
    scheduler.add_job(
        _wrap(run_activation_alerts),
        CronTrigger(day_of_week="mon-fri", hour="8-18", minute=0),
        id="activation_alerts",
        name="Activation Alerts",
    )

    # Pre-meeting auto-brief: Every 30 min, weekdays 7AM-6PM PT
    # Checks calendar for meetings in the next 90 min and sends prep
    scheduler.add_job(
        _wrap(run_pre_meeting_brief),
        CronTrigger(day_of_week="mon-fri", hour="7-18", minute="0,30"),
        id="pre_meeting_brief",
        name="Pre-Meeting Auto-Brief",
    )

    # Post-meeting follow-up (Gong-triggered): Every 30 min, weekdays 9AM-6PM PT
    # Checks Snowflake for Gong transcripts, then analyzes + drafts (fallback)
    scheduler.add_job(
        _wrap(run_post_meeting_followup),
        CronTrigger(day_of_week="mon-fri", hour="9-18", minute="0,30"),
        id="post_meeting_followup",
        name="Post-Meeting Follow-up (Gong)",
    )

    # Granola post-meeting follow-up: Every 3 min, weekdays 8AM-6PM PT
    # Fast path — checks Granola local cache for just-ended meetings
    scheduler.add_job(
        _wrap(run_granola_followup),
        CronTrigger(day_of_week="mon-fri", hour="8-18", minute="*/3"),
        id="granola_followup",
        name="Post-Meeting Follow-up (Granola)",
    )

    # Auth health check: DISABLED — no longer needed, Glass handles MCP auth natively.
    # scheduler.add_job(
    #     _wrap(run_auth_alert),
    #     CronTrigger(minute="5,35"),
    #     id="auth_alert",
    #     name="Auth Health Check",
    # )

    # Quota insights: Daily 6:45 AM ET = 3:45 AM PT
    scheduler.add_job(
        _wrap(run_quota_insights),
        CronTrigger(hour=3, minute=45),
        id="quota_insights",
        name="Quota Insights",
    )

    # Auto-flush pending drafts: Every 30 min, weekdays 8AM-6PM PT
    # Retries any drafts stuck in the pending queue (e.g. from Gumstack auth failures)
    def _auto_flush_drafts():
        try:
            from utils.pending_drafts import flush_to_gmail, list_pending
            from core.user_registry import get_all_users
            pending = list_pending()
            if not pending:
                return
            for uid in get_all_users():
                try:
                    succeeded, failed = flush_to_gmail(user_id=uid)
                    if succeeded or failed:
                        logger.info("Auto-flush for %s: %d succeeded, %d failed", uid, succeeded, failed)
                except Exception as e:
                    logger.warning("Auto-flush drafts failed for %s: %s", uid, e)
        except Exception as e:
            logger.warning("Auto-flush drafts failed: %s", e)

    scheduler.add_job(
        _auto_flush_drafts,
        CronTrigger(day_of_week="mon-fri", hour="8-18", minute="0,30"),
        id="auto_flush_drafts",
        name="Auto-Flush Pending Drafts",
    )

    # Draft reminder: Every 2 hours, weekdays 10AM-6PM PT — reminds about unsent Claude Drafts
    scheduler.add_job(
        _wrap(run_draft_reminder),
        CronTrigger(day_of_week="mon-fri", hour="10,12,14,16,18", minute=20),
        id="draft_reminder",
        name="Draft Reminder",
    )

    scheduler.start()
    logger.info(
        "Scheduler started with %d jobs: %s",
        len(scheduler.get_jobs()),
        ", ".join(j.name for j in scheduler.get_jobs()),
    )

    # Plays cache warm: kick off stale-refresh shortly after boot so the
    # Prospecting tab renders instantly on the first open after a laptop wake.
    try:
        from jobs.plays_refresh import warm_on_startup
        warm_on_startup(delay_sec=30)
    except Exception as e:
        logger.warning("Plays startup warm failed to launch: %s", e)

    return scheduler


# ── Startup catch-up: run missed jobs on restart ────────────────────────────

_LAST_RUN_FILE = os.path.expanduser("~/.gary_bot_last_run")


def _read_last_run() -> float:
    """Read timestamp of last successful run. Returns 0 if no file."""
    try:
        with open(_LAST_RUN_FILE, "r") as f:
            return float(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0.0


def _write_last_run():
    """Write current timestamp as last run."""
    with open(_LAST_RUN_FILE, "w") as f:
        f.write(str(time.time()))


def _run_catchup_jobs():
    """Check how long the bot was offline and run ALL missed jobs.

    Covers: priority_actions, catchup_report, morning_brief, zero_to_one,
    granola followup (with extended lookback), Gong post-meeting followup,
    quota_insights, spend_pacing, opp_pacing, pipeline_cleanup,
    proactive_nudge, acceleration_daily, forecasting, pre_meeting_brief,
    post_meeting, and post_close_monitor.
    """
    from datetime import datetime
    import pytz

    last_run = _read_last_run()
    now = time.time()
    gap_hours = (now - last_run) / 3600 if last_run > 0 else 999

    if gap_hours < 0.5:
        logger.info("Catch-up: bot was offline < 30 min, no catch-up needed")
        return

    logger.info("Catch-up: bot was offline for %.1f hours, running missed jobs...", gap_hours)
    pt = pytz.timezone(TIMEZONE)
    now_pt = datetime.now(pt)
    hour = now_pt.hour
    is_workday = now_pt.weekday() < 5

    from core.user_registry import get_all_users
    all_users = get_all_users()

    def _safe_run(name, fn, *args, **kwargs):
        """Run a catch-up job for all registered users with error handling."""
        for uid in all_users:
            try:
                fn(*args, user_id=uid, **kwargs)
                logger.info("Catch-up: %s completed for %s", name, uid)
            except Exception as e:
                logger.warning("Catch-up: %s failed for %s: %s", name, uid, e)

    try:
        # Always populate priority_actions cache on restart
        from jobs.priority_actions import run_priority_actions
        _safe_run("priority_actions", run_priority_actions, app.client, force=True, silent=True)

        # Send "What You Missed" report for any meaningful gap
        if gap_hours >= 1.0:
            try:
                from jobs.catchup_report import run_catchup_report
                for uid in all_users:
                    try:
                        run_catchup_report(app.client, gap_hours, user_id=uid)
                        logger.info("Catch-up: catch-up report sent for %s", uid)
                    except Exception as e:
                        logger.warning("Catch-up: catch-up report failed for %s: %s", uid, e)
            except Exception as e:
                logger.warning("Catch-up: catch-up report import failed: %s", e)

        # If we missed the morning window and it's still morning, send the brief
        if gap_hours >= 1.0 and 7 <= hour <= 11:
            from jobs.morning_brief import run_morning_brief
            _safe_run("morning_brief", run_morning_brief, app.client, force=True)

        # ── Channel alert backfill (ACH-to-card, procurement, PCLIP, large decline) ──
        # Fetch missed Slack alerts and create email drafts for any unprocessed ones
        if gap_hours >= 0.5:
            try:
                from handlers.channel_monitors import backfill_missed_messages
                # Lookback covers full gap + 1 hour buffer, capped at 48h
                backfill_secs = min(int(gap_hours * 3600) + 3600, 48 * 3600)
                backfill_missed_messages(app.client, lookback_seconds=backfill_secs)
                logger.info("Catch-up: channel alert backfill completed (lookback=%dh)", backfill_secs // 3600)
            except Exception as e:
                logger.warning("Catch-up: channel alert backfill failed: %s", e)

        # ── Post-meeting follow-ups (Granola + Gong) ──
        # Granola: scan meetings that ended during the offline window
        if gap_hours >= 0.5:
            try:
                from jobs.granola_followup import run_granola_followup
                # Lookback covers the full offline gap + 30 min buffer
                lookback_minutes = int(gap_hours * 60) + 30
                for uid in all_users:
                    try:
                        run_granola_followup(app.client, user_id=uid, lookback_minutes=lookback_minutes)
                        logger.info("Catch-up: granola_followup completed for %s (lookback=%dm)", uid, lookback_minutes)
                    except Exception as e:
                        logger.warning("Catch-up: granola_followup failed for %s: %s", uid, e)
            except Exception as e:
                logger.warning("Catch-up: granola_followup import failed: %s", e)

        # Gong: check for transcripts that arrived during offline window
        if gap_hours >= 1.0 and is_workday:
            from jobs.post_meeting_followup import run_post_meeting_followup
            _safe_run("post_meeting_followup", run_post_meeting_followup, app.client)

        # ── Daily intelligence jobs (if missed morning window) ──
        if gap_hours >= 2.0 and is_workday:
            # Quota insights (normally runs 3:45 AM PT = 6:45 AM ET)
            if hour >= 6:
                from jobs.quota_insights import run_quota_insights
                _safe_run("quota_insights", run_quota_insights, app.client)

            # Opp pacing (normally 8 AM)
            if 8 <= hour <= 18:
                from jobs.opp_pacing import run_opp_pacing
                _safe_run("opp_pacing", run_opp_pacing, app.client)

            # Pipeline cleanup (normally 7:30 AM)
            if 7 <= hour <= 12:
                from jobs.pipeline_cleanup import run_pipeline_cleanup
                _safe_run("pipeline_cleanup", run_pipeline_cleanup, app.client)

            # Spend pacing (normally 9 AM)
            if 9 <= hour <= 18:
                from jobs.spend_pacing import run_spend_pacing
                _safe_run("spend_pacing", run_spend_pacing, app.client)

            # Activity report (normally 8:30 AM)
            if 8 <= hour <= 12:
                from jobs.activity_report import run_activity_report
                _safe_run("activity_report", run_activity_report, app.client)

        # Acceleration alert (runs every 30 min, catch up on restart)
        if gap_hours >= 0.5 and is_workday and 5 <= hour <= 18:
            from jobs.acceleration_alert import run_acceleration_alert
            _safe_run("acceleration_alert", run_acceleration_alert, app.client)

        # Acceleration daily summary (normally 5 AM PT = 8 AM ET)
        if gap_hours >= 2.0 and is_workday and 5 <= hour <= 10:
            from jobs.acceleration_alert import run_acceleration_alert as _accel
            _safe_run("acceleration_daily", _accel, app.client, daily=True)

        # Weekly forecasting (normally Monday 7 AM PT)
        if gap_hours >= 2.0 and is_workday and now_pt.weekday() == 0 and 7 <= hour <= 12:
            from jobs.forecasting import run_forecasting
            _safe_run("forecasting", run_forecasting, app.client)

        # Pre-meeting brief (normally every 30 min 7AM-6PM — run once to catch upcoming meetings)
        if gap_hours >= 0.5 and is_workday and 7 <= hour <= 18:
            from jobs.pre_meeting_brief import run_pre_meeting_brief
            _safe_run("pre_meeting_brief", run_pre_meeting_brief, app.client)

        # Post-meeting to-do (normally every 2h 8AM-6PM — catches yesterday's late calls)
        if gap_hours >= 2.0 and is_workday and 8 <= hour <= 18:
            from jobs.post_meeting import run_post_meeting
            _safe_run("post_meeting", run_post_meeting, app.client, lookback_days=2)

        # Post-close CP monitor (normally 10 AM PT)
        if gap_hours >= 2.0 and is_workday and 10 <= hour <= 18:
            from jobs.post_close_monitor import run_post_close_monitor
            _safe_run("post_close_monitor", run_post_close_monitor, app.client)

        # If it's a workday and we missed daily jobs, run key ones
        if gap_hours >= 4.0 and 8 <= hour <= 18 and is_workday:
            from jobs.zero_to_one import run_zero_to_one
            _safe_run("zero_to_one", run_zero_to_one, app.client, force=True)

            from jobs.proactive_nudge import run_proactive_nudge
            _safe_run("proactive_nudge", run_proactive_nudge, app.client)

        # ── Draft reminder — check for unsent drafts from while offline ──
        if gap_hours >= 2.0 and is_workday and 8 <= hour <= 18:
            from jobs.draft_reminder import run_draft_reminder
            _safe_run("draft_reminder", run_draft_reminder, app.client)

    except Exception as e:
        logger.warning("Catch-up jobs failed: %s", e)


# ── Heartbeat: track last-run timestamp periodically ─────────────────────────

def _start_heartbeat(scheduler):
    """Write a heartbeat timestamp every 10 minutes so catch-up can detect gaps."""
    def _heartbeat():
        _write_last_run()

    scheduler.add_job(
        _heartbeat,
        CronTrigger(minute="*/10"),
        id="heartbeat",
        name="Heartbeat",
    )
    _write_last_run()  # Write immediately on startup


# ── Startup ───────────────────────────────────────────────────────────────────
def main():
    """Start the bot."""
    logger.info("Starting Gary Bot...")

    # Validate required env vars
    missing = []
    if not SLACK_BOT_TOKEN:
        missing.append("SLACK_BOT_TOKEN")
    if not SLACK_APP_TOKEN:
        missing.append("SLACK_APP_TOKEN")
    if missing:
        logger.error("Missing required env vars: %s", ", ".join(missing))
        sys.exit(1)

    # Register handlers
    _register_all_handlers()

    # Start scheduler
    scheduler = _start_scheduler()

    # Start heartbeat tracker
    _start_heartbeat(scheduler)

    # Test Snowflake connection
    try:
        from core.snowflake_client import check_connection
        check_connection()
        logger.info("Snowflake connection OK")
    except Exception as e:
        logger.warning("Snowflake connection failed (will retry on first query): %s", e)

    # Send startup heartbeat DM
    try:
        from jobs.status import run_status
        from core.user_registry import get_all_users
        for uid in get_all_users():
            run_status(app.client, user_id=uid, force=True)
        logger.info("Startup heartbeat sent")
    except Exception as e:
        logger.warning("Startup heartbeat failed: %s", e)

    # Run catch-up for missed jobs
    try:
        _run_catchup_jobs()
    except Exception as e:
        logger.warning("Catch-up failed: %s", e)

    # Start socket mode
    logger.info("Gary Bot is running. Press Ctrl+C to stop.")
    try:
        handler = SocketModeHandler(app, SLACK_APP_TOKEN)
        handler.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        _write_last_run()
        scheduler.shutdown(wait=False)
    except Exception as e:
        logger.error("Fatal error: %s", e)
        _write_last_run()
        scheduler.shutdown(wait=False)
        sys.exit(1)


if __name__ == "__main__":
    main()
