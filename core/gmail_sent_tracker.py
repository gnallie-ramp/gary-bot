"""Gmail Sent Tracker — query Gmail for actual last-sent email per account.

Snowflake's dim_email_threads only tracks Outreach-synced emails, so it misses
emails sent directly from Gmail (including bot-created drafts the user sends).
This module queries Gmail directly via Gumstack MCP to get the real last-sent
date for each account.

Results are cached per-user for 2 hours to avoid hitting Gmail on every
Home tab refresh.
"""
from __future__ import annotations

import logging
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Cache: {user_id: {account_id: {"date": "2026-04-07", "subject": "Re: ...", "direction": "outbound"}, ...}, "_ts": float}}
_sent_cache: dict[str, dict] = {}
_CACHE_TTL = 2 * 3600  # 2 hours
_lock = threading.Lock()

# Max parallel Gmail queries
_MAX_WORKERS = 6


def _query_gmail_for_account(
    account_name: str,
    user_id: Optional[str] = None,
) -> Optional[dict]:
    """Query Gmail for the most recent sent email matching an account name.

    Uses Gmail search: `in:sent "{account_name}" newer_than:90d`
    Returns {"date": "2026-04-07", "subject": "Re: ...", "to": "..."} or None.
    """
    from core.gumstack_gmail import read_emails

    # Clean account name for search — remove suffixes like LLC, Inc, etc.
    # that might not appear in email subjects/bodies
    clean_name = account_name.strip()
    # Remove common legal suffixes for better matching
    clean_name = re.sub(
        r'\s*,?\s*(LLC|Inc\.?|Corp\.?|Co\.?|Ltd\.?|LP|LLP|PLC|Group|Holdings?)\.?\s*$',
        '', clean_name, flags=re.IGNORECASE
    ).strip()

    if not clean_name or len(clean_name) < 3:
        return None

    # Escape quotes in name
    safe_name = clean_name.replace('"', '')
    query = f'in:sent "{safe_name}" newer_than:90d'

    try:
        # Retry once on SSE/parse failures (intermittent Gumstack issue)
        emails = read_emails(query=query, max_results=1, user_id=user_id)
        if not emails:
            import time as _time
            _time.sleep(0.5)
            emails = read_emails(query=query, max_results=1, user_id=user_id)
        if not emails:
            return None

        email = emails[0]
        date_str = email.get("date", "")
        subject = email.get("subject", "")
        to = email.get("to", "")

        # Parse the date — Gmail returns various formats
        parsed_date = _parse_gmail_date(date_str)
        if not parsed_date:
            return None

        return {
            "date": parsed_date,  # "YYYY-MM-DD" string
            "subject": subject,
            "to": to,
            "direction": "outbound",
            "source": "gmail",
        }

    except Exception as e:
        logger.debug("Gmail sent lookup failed for '%s': %s", account_name, e)
        return None


def _parse_gmail_date(date_str: str) -> Optional[str]:
    """Parse a Gmail date string into YYYY-MM-DD format."""
    if not date_str:
        return None

    # Common Gmail date formats
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",    # "Mon, 07 Apr 2026 10:30:00 -0400"
        "%d %b %Y %H:%M:%S %z",          # "07 Apr 2026 10:30:00 -0400"
        "%Y-%m-%dT%H:%M:%S%z",           # ISO format
        "%Y-%m-%dT%H:%M:%S.%f%z",        # ISO with microseconds
        "%Y-%m-%d %H:%M:%S",             # Simple datetime
        "%Y-%m-%d",                       # Just date
    ]

    # Strip extra whitespace and trailing timezone names like "(EDT)"
    clean = re.sub(r'\s*\([A-Z]{2,5}\)\s*$', '', date_str.strip())

    for fmt in formats:
        try:
            dt = datetime.strptime(clean, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Last resort: try to extract YYYY-MM-DD from the string
    match = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
    if match:
        return match.group(1)

    logger.debug("Could not parse Gmail date: %s", date_str)
    return None


def enrich_with_gmail_sent(
    items: list[dict],
    account_name_key: str = "account",
    account_id_key: str = "account_id",
    user_id: Optional[str] = None,
) -> list[dict]:
    """Enrich a list of items with real Gmail last-sent dates.

    For each item, if Gmail has a more recent sent email than what Snowflake
    reports in `last_email_date`, the item's `last_email_date` and
    `last_email_subject` are updated with the Gmail data.

    Items are modified in-place and also returned.

    This function is safe to call on any list of dicts that has account_name
    and account_id fields — priority alerts, stale opps, prospecting items, etc.
    """
    if not items:
        return items

    uid = user_id or "default"

    # Check cache
    with _lock:
        user_cache = _sent_cache.get(uid, {})
        cache_ts = user_cache.get("_ts", 0)
        cache_valid = (time.time() - cache_ts) < _CACHE_TTL

    # Collect accounts we need to look up (not already cached)
    lookups_needed = {}  # account_id -> account_name
    for item in items:
        acct_id = item.get(account_id_key, "")
        acct_name = item.get(account_name_key, "")
        if not acct_id or not acct_name:
            continue
        if cache_valid and acct_id in user_cache:
            continue  # already cached
        lookups_needed[acct_id] = acct_name

    # Query Gmail in parallel for uncached accounts
    if lookups_needed:
        logger.info(
            "Gmail sent tracker: looking up %d accounts for %s",
            len(lookups_needed), uid,
        )
        new_results = {}
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            futures = {
                pool.submit(_query_gmail_for_account, name, user_id): acct_id
                for acct_id, name in lookups_needed.items()
            }
            for future in as_completed(futures):
                acct_id = futures[future]
                try:
                    result = future.result()
                    new_results[acct_id] = result  # None means no sent email found
                except Exception as e:
                    logger.debug("Gmail lookup failed for %s: %s", acct_id, e)
                    new_results[acct_id] = None

        # Merge into cache
        with _lock:
            if uid not in _sent_cache or not cache_valid:
                _sent_cache[uid] = {"_ts": time.time()}
            _sent_cache[uid].update(new_results)

        logger.info(
            "Gmail sent tracker: found sent emails for %d/%d accounts",
            sum(1 for v in new_results.values() if v is not None),
            len(new_results),
        )

    # Now enrich items with cached Gmail data
    with _lock:
        user_cache = _sent_cache.get(uid, {})

    for item in items:
        acct_id = item.get(account_id_key, "")
        if not acct_id:
            continue

        gmail_data = user_cache.get(acct_id)
        if not gmail_data or not isinstance(gmail_data, dict):
            continue

        gmail_date = gmail_data.get("date", "")
        if not gmail_date:
            continue

        # Compare with existing Snowflake last_email_date
        existing_date = str(item.get("last_email_date", "") or "")
        existing_clean = existing_date[:10] if existing_date else ""

        # Use Gmail date if it's more recent than Snowflake (or Snowflake has no date)
        if not existing_clean or existing_clean in ("", "None", "NaT", "2000-01-01"):
            # No Snowflake date — use Gmail
            item["last_email_date"] = gmail_date
            item["last_email_subject"] = gmail_data.get("subject", "")
            item["_last_email_subj"] = gmail_data.get("subject", "")
            item["_last_email_direction"] = "outbound"
            item["_email_source"] = "gmail"
        elif gmail_date > existing_clean:
            # Gmail is more recent
            item["last_email_date"] = gmail_date
            item["last_email_subject"] = gmail_data.get("subject", "")
            item["_last_email_subj"] = gmail_data.get("subject", "")
            item["_last_email_direction"] = "outbound"
            item["_email_source"] = "gmail"

    return items


def invalidate_cache(user_id: Optional[str] = None):
    """Clear the Gmail sent cache for a user (or all users)."""
    with _lock:
        if user_id:
            _sent_cache.pop(user_id or "default", None)
        else:
            _sent_cache.clear()
