"""Configuration: env vars, constants, channel IDs, NTR rates."""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Owner Identity (set these in .env for each user) ─────────────────────────
# Full name as it appears in Salesforce (used in all Snowflake queries)
OWNER_NAME = os.getenv("OWNER_NAME", "Gregory Nallie")
# First name for email sign-offs, prompts, etc.
OWNER_FIRST_NAME = os.getenv("OWNER_FIRST_NAME", "Greg")
# Slack user ID (find yours: click your profile → ⋮ → Copy member ID)
OWNER_SLACK_ID = os.getenv("OWNER_SLACK_ID", "U06DAFU4YRG")
# Booking link for email CTAs
BOOKING_LINK = os.getenv("BOOKING_LINK", "https://ramp-com.chilipiper.com/me/gregory-nallie/ramp")

# Backward-compat alias — existing code imports this everywhere
GREG_SLACK_ID = OWNER_SLACK_ID

# ── Slack ─────────────────────────────────────────────────────────────────────
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")  # xapp- token for socket mode

# Channel names → IDs populated at startup via channel resolution
ALERT_CHANNELS = {
    "ach_to_card": "alerts-card-payable-bills",
    "procurement_trial": "alerts-self-serve-procurement-trials",
    "pclip": "alerts-pclip-activations",
    "large_decline": "alerts-large-declines",
    "fundraise": "alerts-fundraising",
}

# ── Snowflake (via snow CLI + SSO — no PAT/VPN needed) ───────────────────────
# Auth is handled by the snow CLI using cached browser-based SSO.
# Config lives in ~/.snowflake/config.toml.

# ── Claude / Anthropic ────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# ── Gmail (IMAP with app password — no Google Cloud project needed) ───────────
GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")

# ── NTR Rates (single source of truth) ────────────────────────────────────────
# Card 95bps | Bill Pay 15bps | Treasury 5bps | Travel 350bps
NTR_RATES = {
    "Card Expansion": 0.0095,
    "Bill Pay Expansion": 0.0015,
    "Treasury Expansion": 0.0005,
    "Travel Expansion": 0.035,
}

# Keyed by spend-acceleration delta column names
NTR_BY_DELTA_COL = {
    "card_delta": NTR_RATES["Card Expansion"],
    "billpay_delta": NTR_RATES["Bill Pay Expansion"],
    "travel_delta": NTR_RATES["Travel Expansion"],
    "treasury_delta": NTR_RATES["Treasury Expansion"],
}

# ── Salesforce ────────────────────────────────────────────────────────────────
SF_BASE_URL = "https://rampfinancial.lightning.force.com/lightning"

# ── Streamlit Dashboard ──────────────────────────────────────────────────────
DASHBOARD_BASE_URL = "http://localhost:8501"

# ── Granola ──────────────────────────────────────────────────────────────────
GRANOLA_POLL_MINUTES = 10  # lookback window for automatic polling

# ── Dedup ─────────────────────────────────────────────────────────────────────
DEDUP_STATE_FILE = os.path.expanduser("~/.gary_bot_processed.json")
DEDUP_TTL_DAYS = 7

# ── Scheduling (Pacific Time — job triggers use PT hours) ────────────────────
TIMEZONE = "US/Pacific"

# ── Display timezone ──────────────────────────────────────────────────────────
DISPLAY_TIMEZONE = os.getenv("DISPLAY_TIMEZONE", "US/Eastern")
