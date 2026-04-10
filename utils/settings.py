"""User preferences for Gary Bot — persisted to ~/.gary_bot_settings.json.

Settings are stored per-user, keyed by Slack user ID:
    {"U06DAFU4YRG": {"auto_drafting": true, ...}, "U03JBULM9LP": {...}}

When user_id is None, falls back to default settings.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

SETTINGS_FILE = os.path.expanduser("~/.gary_bot_settings.json")

DEFAULT_SETTINGS = {
    "auto_drafting": True,
    "morning_brief": True,
    "post_meeting_dm": True,
    "quota_insights": True,
    "spend_pacing": True,
    "opp_pacing": True,
    "zero_to_one": True,
    "pipeline_cleanup": True,
    "proactive_nudge": True,
    "quiet_hours_start": None,  # e.g. "18:00"
    "quiet_hours_end": None,    # e.g. "08:00"
    # Per-signal-type notification toggles (real-time DM alerts)
    "signal_early_accel": True,
    "signal_close_window": True,
    "signal_leading": True,
    "signal_first_bill": True,
    "signal_opp_first_spend": True,
    "signal_treasury_spike": True,
}


def _load_all() -> dict:
    """Load the raw settings file (per-user dict)."""
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r") as f:
                data = json.load(f)
            # Migration: if the file is a flat dict (old format), wrap it
            # under the default user key so it becomes per-user.
            if data and not any(isinstance(v, dict) for v in data.values()):
                from config import GREG_SLACK_ID
                logger.info("Migrating settings file from global to per-user format")
                migrated = {GREG_SLACK_ID: data}
                _save_all(migrated)
                return migrated
            return data
    except Exception as e:
        logger.warning("Failed to load settings from %s: %s", SETTINGS_FILE, e)
    return {}


def _save_all(data: dict) -> None:
    """Write the full per-user settings dict to file."""
    try:
        with open(SETTINGS_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning("Failed to save settings to %s: %s", SETTINGS_FILE, e)


def load_settings(user_id: str | None = None) -> dict:
    """Load settings for a specific user, merging with defaults for any missing keys.

    If user_id is None, returns default settings.
    """
    settings = dict(DEFAULT_SETTINGS)
    if user_id is None:
        return settings
    all_data = _load_all()
    user_data = all_data.get(user_id, {})
    settings.update(user_data)
    return settings


def save_settings(settings: dict, user_id: str | None = None) -> None:
    """Write settings dict for a specific user."""
    if user_id is None:
        logger.warning("save_settings called without user_id, ignoring")
        return
    all_data = _load_all()
    all_data[user_id] = settings
    _save_all(all_data)


def get_setting(key: str, user_id: str | None = None):
    """Get a single setting value for a user."""
    settings = load_settings(user_id)
    return settings.get(key, DEFAULT_SETTINGS.get(key))


def update_setting(key: str, value, user_id: str | None = None) -> None:
    """Update a single setting for a user and save."""
    if user_id is None:
        logger.warning("update_setting called without user_id, ignoring")
        return
    settings = load_settings(user_id)
    settings[key] = value
    save_settings(settings, user_id)


def is_dm_allowed(user_id: str | None = None) -> bool:
    """Check if current time (US/Eastern) is outside quiet hours for a user.

    Returns True if DMs are allowed (i.e. NOT in quiet hours).
    If quiet hours are not configured, always returns True.
    """
    settings = load_settings(user_id)
    start_str = settings.get("quiet_hours_start")
    end_str = settings.get("quiet_hours_end")

    if not start_str or not end_str:
        return True

    try:
        import pytz
        et = pytz.timezone("US/Eastern")
        now = datetime.now(et)
        now_time = now.time()

        quiet_start = datetime.strptime(start_str, "%H:%M").time()
        quiet_end = datetime.strptime(end_str, "%H:%M").time()

        # Handle overnight quiet hours (e.g. 18:00 -> 08:00)
        if quiet_start <= quiet_end:
            # Same-day range (e.g. 09:00 -> 17:00)
            in_quiet = quiet_start <= now_time <= quiet_end
        else:
            # Overnight range (e.g. 18:00 -> 08:00)
            in_quiet = now_time >= quiet_start or now_time <= quiet_end

        return not in_quiet
    except Exception as e:
        logger.warning("Quiet hours check failed: %s", e)
        return True  # Fail open — allow DMs if check fails


def is_feature_enabled(feature_key: str, user_id: str | None = None) -> bool:
    """Check if a feature is enabled AND current time is outside quiet hours.

    Combines the feature toggle with quiet hours check.
    """
    settings = load_settings(user_id)
    if not settings.get(feature_key, DEFAULT_SETTINGS.get(feature_key, True)):
        return False
    return is_dm_allowed(user_id)
