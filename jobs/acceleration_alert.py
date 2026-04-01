"""Acceleration Alert — real-time DM notifications for time-sensitive spend signals.

Runs every 30 minutes during work hours. Tracks which (signal_type, account_id)
pairs have already been notified to avoid duplicates. Sends one-off DMs for
NEW signals immediately, plus a daily summary at 8 AM ET.

Each alert entry includes a "Create Draft" button that generates a context-aware
outreach email to the account's business owner/admins.
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime
from pathlib import Path

from config import GREG_SLACK_ID, COMMAND_PREFIX

logger = logging.getLogger(__name__)

SF_BASE_URL = "https://rampfinancial.lightning.force.com/lightning"

# Only surface these time-sensitive signal types in real-time DMs
_URGENT_SIGNALS = {"early_accel", "close_window", "leading", "first_bill", "treasury_spike"}

# Persistent cache file for already-notified signals
_SEEN_CACHE_PATH = Path.home() / ".gary_bot_seen_signals.json"

# Confirmation cache: signals must appear in 2+ consecutive runs before DM
_PENDING_CACHE_PATH = Path.home() / ".gary_bot_pending_signals.json"

# In-memory caches keyed by user_id.
# Each value is a dict of {(signal_type, account_id): timestamp}.
_seen_signals: dict[str, dict[tuple[str, str], float]] = {}

# Pending signals awaiting confirmation, keyed by user_id.
_pending_signals: dict[str, dict[tuple[str, str], float]] = {}

# Track last daily summary date per user_id.
_last_daily_date: dict[str, str] = {}

# Track which user caches have been loaded from disk.
_loaded_users: set[str] = set()


def _ensure_user_cache(user_id: str) -> None:
    """Lazy-load the cache for *user_id* from disk if not already loaded."""
    if user_id in _loaded_users:
        return
    _loaded_users.add(user_id)
    _seen_signals.setdefault(user_id, {})
    _pending_signals.setdefault(user_id, {})
    _last_daily_date.setdefault(user_id, "")
    try:
        if _SEEN_CACHE_PATH.exists():
            all_data = json.loads(_SEEN_CACHE_PATH.read_text())
            user_data = all_data.get(user_id, {})
            _last_daily_date[user_id] = user_data.get("last_daily_date", "")
            for entry in user_data.get("signals", []):
                key = (entry["signal_type"], entry["account_id"])
                _seen_signals[user_id][key] = entry["timestamp"]
            logger.info(
                "Loaded %d seen signals from cache for %s (date: %s)",
                len(_seen_signals[user_id]), user_id, _last_daily_date[user_id],
            )
    except Exception as e:
        logger.warning("Failed to load seen-signals cache for %s: %s", user_id, e)
    try:
        if _PENDING_CACHE_PATH.exists():
            all_data = json.loads(_PENDING_CACHE_PATH.read_text())
            user_data = all_data.get(user_id, {})
            for entry in user_data.get("pending", []):
                key = (entry["signal_type"], entry["account_id"])
                _pending_signals[user_id][key] = entry["timestamp"]
            logger.info(
                "Loaded %d pending signals from cache for %s",
                len(_pending_signals[user_id]), user_id,
            )
    except Exception as e:
        logger.warning("Failed to load pending-signals cache for %s: %s", user_id, e)


def _save_seen_cache():
    """Persist seen-signals cache to disk (all users)."""
    try:
        all_data: dict[str, dict] = {}
        # Preserve existing data for users not currently in memory
        if _SEEN_CACHE_PATH.exists():
            try:
                all_data = json.loads(_SEEN_CACHE_PATH.read_text())
            except Exception:
                pass
        for uid in _loaded_users:
            all_data[uid] = {
                "last_daily_date": _last_daily_date.get(uid, ""),
                "signals": [
                    {"signal_type": k[0], "account_id": k[1], "timestamp": v}
                    for k, v in _seen_signals.get(uid, {}).items()
                ],
            }
        _SEEN_CACHE_PATH.write_text(json.dumps(all_data, indent=2))
    except Exception as e:
        logger.warning("Failed to save seen-signals cache: %s", e)


def _save_pending_cache():
    """Persist pending-signals cache to disk (all users)."""
    try:
        all_data: dict[str, dict] = {}
        if _PENDING_CACHE_PATH.exists():
            try:
                all_data = json.loads(_PENDING_CACHE_PATH.read_text())
            except Exception:
                pass
        for uid in _loaded_users:
            all_data[uid] = {
                "pending": [
                    {"signal_type": k[0], "account_id": k[1], "timestamp": v}
                    for k, v in _pending_signals.get(uid, {}).items()
                ],
            }
        _PENDING_CACHE_PATH.write_text(json.dumps(all_data, indent=2))
    except Exception as e:
        logger.warning("Failed to save pending-signals cache: %s", e)


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


def _draft_button(row, signal_type):
    """Build a Create Draft action button for a signal entry."""
    payload = json.dumps({
        "account": row.get("account_name", "Unknown"),
        "account_id": row.get("account_id", ""),
        "opp_id": row.get("opportunity_id", ""),
        "product": str(row.get("product", "")),
        "category": _signal_to_category(signal_type),
    })
    return {
        "type": "button",
        "text": {"type": "plain_text", "text": ":envelope: Create Draft", "emoji": True},
        "action_id": f"draft_outreach_{signal_type}_{row.get('account_id', '')}",
        "value": payload,
    }


def _signal_to_category(signal_type):
    """Map signal_type to the draft email category."""
    return {
        "early_accel": "prospect",
        "close_window": "close_window",
        "leading": "prospect",
        "first_bill": "zero_to_one",
        "close_now": "close_now",
        "zero_to_one": "zero_to_one",
        "sustained_accel": "prospect",
        "treasury_spike": "treasury_spike",
    }.get(signal_type, "prospect")


def _format_signal_entry(row, signal_type):
    """Format a single signal entry with context and return (text, button)."""
    product = str(row.get("product", "")).replace(" Expansion", "")
    paced = _safe_int(row.get("paced_amount", 0))
    base = _safe_int(row.get("baseline_amount", 0))
    l30d = _safe_int(row.get("spend_l30d", 0))
    l7d = _safe_int(row.get("spend_l7d", 0))
    cp = _safe_int(row.get("est_cp", 0))
    cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
    pct = _pct(paced, base)

    if signal_type == "early_accel":
        text = (
            f"\u2022 {_acct_link(row)} — {product} L7D pacing "
            f"${paced:,}/mo vs ${base:,} baseline (+{pct}%)"
            f"\n   _L30D only ${l30d:,} — window open to lock low baseline_{cp_str}"
            f"\n   _Why: L7D raw ${l7d:,} is {pct}% above 90D avg, but L30D hasn't caught up yet_"
        )
    elif signal_type == "close_window":
        text = (
            f"\u2022 {_acct_link(row)} — {product} L7D pacing "
            f"${paced:,}/mo\n   _Close now — L30D baseline would be "
            f"${l30d:,}_{cp_str}"
            f"\n   _Why: L7D ramping above current L30D — close before baseline rises_"
        )
    elif signal_type == "leading":
        text = (
            f"\u2022 {_acct_link(row)} — ${paced:,} in bills "
            f"created/scheduled vs ${base:,}/mo baseline{cp_str}"
            f"\n   _Why: large bill(s) queued that exceed typical monthly volume_"
        )
    elif signal_type == "first_bill":
        text = (
            f"\u2022 {_acct_link(row)} — *first bill created* in Ramp "
            f"(${paced:,})"
            f"\n   _Bill Pay opp open — customer just started using the product_{cp_str}"
        )
    elif signal_type == "treasury_spike":
        spike_pct = _pct(paced, l30d) if l30d > 0 else 0
        text = (
            f"\u2022 {_acct_link(row)} — Treasury GLA spiked +{spike_pct}%"
            f"\n   L7D avg ${paced:,} vs L30D avg ${l30d:,} — large deposit detected{cp_str}"
            f"\n   _Lock in treasury expansion opp while balance is high (uncapped H1-26)_"
        )
    else:
        text = f"\u2022 {_acct_link(row)} — {product}{cp_str}"

    # Append AE presale discrepancy if available
    full_product = str(row.get("product", ""))
    ae_card = _safe_int(row.get("ae_card_presale", 0))
    ae_bp = _safe_int(row.get("ae_bp_presale", 0))
    actual = paced or l30d
    presale = ae_card if "Card" in full_product else ae_bp if "Bill" in full_product else 0
    if presale > 0 and actual > 0:
        ratio = actual / presale
        if ratio >= 2.0:
            text += f"\n   :large_green_circle: _AE presale ${presale:,}/mo — actual {ratio:.1f}x higher_"
        elif ratio <= 0.3:
            text += f"\n   :warning: _AE presale ${presale:,}/mo — only {int(ratio*100)}% migrated_"

    return text, _draft_button(row, signal_type)


def run_acceleration_alert(client, user_id=None, force: bool = False, daily: bool = False):
    """Query for acceleration signals and DM Greg with urgent NEW ones.

    Args:
        client: Slack client
        force: Send even if no urgent signals
        daily: If True, this is the daily summary — reset seen cache and send all
    """
    dm_target = user_id or GREG_SLACK_ID
    cache_uid = dm_target

    _ensure_user_cache(cache_uid)

    try:
        from core.snowflake_client import run_query
        from queries.queries import HOME_PRIORITY_ALERTS_QUERY, format_query

        df = run_query(format_query(HOME_PRIORITY_ALERTS_QUERY, user_id=user_id))
        if df.empty:
            logger.info("Acceleration alert: no data")
            return

        now = datetime.now()
        today_str = now.strftime("%Y-%m-%d")

        # Reset seen + pending cache at the start of each day
        if today_str != _last_daily_date.get(cache_uid, ""):
            _seen_signals[cache_uid] = {}
            _pending_signals[cache_uid] = {}
            _last_daily_date[cache_uid] = today_str
            _save_seen_cache()

        # Filter to urgent signals only
        urgent = df[df["signal_type"].isin(_URGENT_SIGNALS)]
        if urgent.empty and not force:
            logger.info("Acceleration alert: no urgent signals")
            return

        if daily:
            # Daily summary: send all urgent signals
            _send_daily_summary(client, urgent, now, dm_target=dm_target)
            # Mark all as seen
            for _, row in urgent.iterrows():
                key = (row.get("signal_type", ""), row.get("account_id", ""))
                _seen_signals[cache_uid][key] = now.timestamp()
            _save_seen_cache()
            return

        # Filter out muted signal types per user preferences
        from utils.settings import load_settings
        user_settings = load_settings(user_id)
        muted_signals = {
            sig for sig in _URGENT_SIGNALS
            if not user_settings.get(f"signal_{sig}", True)
        }
        if muted_signals:
            urgent = urgent[~urgent["signal_type"].isin(muted_signals)]
            logger.info("Acceleration alert: muted signals filtered out: %s", muted_signals)

        # Real-time mode: confirmation layer + dedup
        # Signals must appear in 2+ consecutive runs before being DM'd.
        # First appearance → pending. Second appearance → confirmed → DM.
        now_ts = now.timestamp()
        confirmed_rows = []
        current_keys = set()

        user_seen = _seen_signals[cache_uid]
        user_pending = _pending_signals[cache_uid]

        for _, row in urgent.iterrows():
            key = (row.get("signal_type", ""), row.get("account_id", ""))
            current_keys.add(key)

            if key in user_seen:
                continue  # already notified today

            if key in user_pending:
                # Signal appeared before and is back — confirmed
                confirmed_rows.append(row)
                user_seen[key] = now_ts
                user_pending.pop(key, None)
            else:
                # First appearance — add to pending, don't DM yet
                user_pending[key] = now_ts

        # Prune pending signals that disappeared (no longer in query results)
        stale_pending = [k for k in user_pending if k not in current_keys]
        for k in stale_pending:
            user_pending.pop(k, None)

        _save_pending_cache()
        if confirmed_rows:
            _save_seen_cache()

        if not confirmed_rows and not force:
            pending_count = len(user_pending)
            if pending_count:
                logger.info("Acceleration alert: %d signals pending confirmation", pending_count)
            else:
                logger.info("Acceleration alert: no new signals (all seen)")
            return

        if not confirmed_rows:
            return

        # Send individual DMs for each confirmed signal
        _send_realtime_alerts(client, confirmed_rows, now, dm_target=dm_target)

    except Exception as e:
        logger.error("Acceleration alert failed: %s", e)


def _send_realtime_alerts(client, rows, now, dm_target=None):
    """Send grouped DMs per signal type — max 3 shown, rest behind /priorities."""
    MAX_PER_TYPE = 3

    signal_labels = {
        "early_accel": ":zap: Early Acceleration Detected",
        "close_window": ":alarm_clock: Close Window — Opp Ramping",
        "leading": ":eyes: Large Bills Incoming",
        "first_bill": ":tada: First Bill Created",
        "treasury_spike": ":moneybag: Treasury GLA Spike",
    }

    # Group rows by signal type
    grouped: dict[str, list] = {}
    for row in rows:
        sig = row.get("signal_type", "")
        grouped.setdefault(sig, []).append(row)

    for sig_type, group_rows in grouped.items():
        try:
            header = signal_labels.get(sig_type, ":rotating_light: Acceleration Alert")
            lines = [f"*{header}*"]
            buttons = []

            for row in group_rows[:MAX_PER_TYPE]:
                text, button = _format_signal_entry(row, sig_type)
                lines.append(text)
                buttons.append(button)

            overflow = len(group_rows) - MAX_PER_TYPE
            if overflow > 0:
                lines.append(f"_...and {overflow} more — use `/{COMMAND_PREFIX}-priorities` to see all_")

            blocks = [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "\n".join(lines)},
                },
            ]
            if buttons:
                blocks.append({
                    "type": "actions",
                    "elements": buttons[:5],
                })

            names = ", ".join(r.get("account_name", "?") for r in group_rows[:MAX_PER_TYPE])
            fallback = f"{header}: {names}"
            if overflow > 0:
                fallback += f" +{overflow} more"

            client.chat_postMessage(
                channel=dm_target,
                blocks=blocks,
                text=fallback,
            )
            logger.info(
                "Real-time alert sent: %s (%d shown, %d overflow)",
                sig_type, min(len(group_rows), MAX_PER_TYPE), max(overflow, 0),
            )

        except Exception as e:
            logger.error("Failed to send real-time alert for %s: %s", sig_type, e)


def _send_daily_summary(client, urgent, now, dm_target=None):
    """Send the daily 8 AM ET summary with all urgent signals."""
    date_str = now.strftime("%A, %b %-d")
    blocks = [{
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"\u26a1 Acceleration Alert — {date_str}",
            "emoji": True,
        },
    }]

    total_cp = 0
    item_count = 0
    max_per_group = 5

    signal_groups = [
        ("early_accel", ":zap: *Early Acceleration — Window Open*"),
        ("close_window", ":alarm_clock: *Close Window — Opp Ramping*"),
        ("leading", ":eyes: *Bills Created/Scheduled — Spend Incoming*"),
        ("first_bill", ":tada: *First Bill Created — Bill Pay Opp Active*"),
        ("treasury_spike", ":moneybag: *Treasury GLA Spike — Large Deposit*"),
    ]

    for sig_type, header in signal_groups:
        group = urgent[urgent["signal_type"] == sig_type]
        if len(group) == 0:
            continue

        lines = [header]
        buttons = []
        for _, row in group.head(max_per_group).iterrows():
            cp = _safe_int(row.get("est_cp", 0))
            total_cp += cp
            item_count += 1
            text, button = _format_signal_entry(row, sig_type)
            lines.append(text)
            buttons.append(button)

        if len(group) > max_per_group:
            lines.append(f"_...and {len(group) - max_per_group} more_")

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        })
        # Add draft buttons (max 5 per actions block)
        if buttons:
            blocks.append({
                "type": "actions",
                "elements": buttons[:5],
            })

    if item_count == 0:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":white_check_mark: No urgent acceleration signals today.",
            },
        })

    # Summary footer
    if total_cp > 0:
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f"{item_count} accounts with acceleration signals "
                    f"· ~${total_cp:,} est CP at stake"
                ),
            }],
        })

    fallback = f"Acceleration Alert: {item_count} urgent signals, ~${total_cp:,} CP"
    client.chat_postMessage(
        channel=dm_target,
        blocks=blocks,
        text=fallback,
    )
    logger.info("Daily acceleration summary sent: %d items, ~$%d CP", item_count, total_cp)
