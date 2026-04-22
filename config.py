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

# Slash command prefix — each instance gets unique commands (e.g. /gary-lookup, /jane-lookup)
COMMAND_PREFIX = os.getenv("COMMAND_PREFIX", "gary")

# Backward-compat alias — existing code imports this everywhere
GREG_SLACK_ID = OWNER_SLACK_ID

# ── Auto-detected owner (per-instance, stored locally) ───────────────────────
_OWNER_FILE = os.path.join(os.path.dirname(__file__), ".owner")


def get_owner_id() -> str:
    """Return the owner Slack user ID for THIS bot instance.

    Priority:
    1. Local .owner file (auto-set on first Home tab open)
    2. OWNER_SLACK_ID from .env
    """
    if os.path.exists(_OWNER_FILE):
        with open(_OWNER_FILE) as f:
            stored = f.read().strip()
            if stored:
                return stored
    return OWNER_SLACK_ID


def set_owner_id(user_id: str) -> None:
    """Persist the owner Slack user ID for this instance.

    Called automatically on first app_home_opened event.
    """
    with open(_OWNER_FILE, "w") as f:
        f.write(user_id)

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
    "auto_card": "bill-pay-automatic-card-losses",
    "rclip": "alerts-rclip-requests",
    "am_escalation": "am-escalations",
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
# Transactional products: Card 95bps | Bill Pay 15bps | Treasury 5bps | Travel 350bps
# Subscription products: SaaS (Plus) 7.5% | Procurement 7.5%
NTR_RATES = {
    "Card Expansion": 0.0095,
    "Bill Pay Expansion": 0.0015,
    "Treasury Expansion": 0.0005,
    "Travel Expansion": 0.035,
    "SaaS": 0.075,
    "Procurement": 0.075,
}

# Comp window multiplier (used when computing estimated CP for display).
# Transactional products accrue CP over the 90-day post-close window (= 3
# months × monthly). Subscription products accrue over 6 months with a
# product-specific accelerator: SaaS F2P = 1.5x (→ 9-month effective),
# Procurement = 2.0x (→ 12-month effective). Renewal accelerators (>110% = 3x)
# and Plus renewal kickers aren't modeled here — this is a base-case estimate,
# actual comp may be higher.
CP_WINDOW_MULTIPLIER = {
    "Card Expansion": 3,
    "Bill Pay Expansion": 3,
    "Treasury Expansion": 3,
    "Travel Expansion": 3,
    "SaaS": 9,          # 6mo × 1.5x F2P bonus
    "Procurement": 12,  # 6mo × 2.0x
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
