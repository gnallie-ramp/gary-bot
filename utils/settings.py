"""User preferences for Gary Bot — persisted to ~/.gary_bot_settings.json."""
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
    "signal_treasury_spike": True,
}


def load_settings() -> dict:
    """Load settings from file, merging with defaults for any missing keys."""
    settings = dict(DEFAULT_SETTINGS)
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r") as f:
                saved = json.load(f)
            settings.update(saved)
    except Exception as e:
        logger.warning("Failed to load settings from %s: %s", SETTINGS_FILE, e)
    return settings


def save_settings(settings: dict) -> None:
    """Write settings dict to file."""
    try:
        with open(SETTINGS_FILE, "w") as f:
            json.dump(settings, f, indent=2)
    except Exception as e:
        logger.warning("Failed to save settings to %s: %s", SETTINGS_FILE, e)


def get_setting(key: str):
    """Get a single setting value."""
    settings = load_settings()
    return settings.get(key, DEFAULT_SETTINGS.get(key))


def update_setting(key: str, value) -> None:
    """Update a single setting and save."""
    settings = load_settings()
    settings[key] = value
    save_settings(settings)


def is_dm_allowed() -> bool:
    """Check if current time (US/Eastern) is outside quiet hours.

    Returns True if DMs are allowed (i.e. NOT in quiet hours).
    If quiet hours are not configured, always returns True.
    """
    settings = load_settings()
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


def is_feature_enabled(feature_key: str) -> bool:
    """Check if a feature is enabled AND current time is outside quiet hours.

    Combines the feature toggle with quiet hours check.
    """
    settings = load_settings()
    if not settings.get(feature_key, DEFAULT_SETTINGS.get(feature_key, True)):
        return False
    return is_dm_allowed()
