"""Google Calendar client — Calendar API via Application Default Credentials (ADC).

One-time setup:
  gcloud auth application-default login \
    --scopes=https://www.googleapis.com/auth/cloud-platform,\
https://www.googleapis.com/auth/calendar.readonly

Token stored at ~/.config/gcloud/application_default_credentials.json
and auto-refreshes. No credentials files, no GCP project needed.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from google.auth import default
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
]

# ── Cached service ────────────────────────────────────────────────────────────
_calendar_service = None
_api_available = None  # type: Optional[bool]


def _get_service():
    """Get an authenticated Calendar API service, caching across calls."""
    global _calendar_service, _api_available

    if _calendar_service is not None:
        return _calendar_service

    try:
        creds, _ = default(scopes=_SCOPES)
        if not creds.valid and hasattr(creds, "refresh"):
            creds.refresh(Request())

        _calendar_service = build("calendar", "v3", credentials=creds)
        _api_available = True
        return _calendar_service
    except Exception as e:
        _api_available = False
        logger.warning("Google Calendar API init failed: %s", e)
        raise


# ── Connection check ──────────────────────────────────────────────────────────


def check_connection():
    """Test Calendar API connectivity. Returns (ok, message)."""
    global _api_available
    try:
        service = _get_service()
        cal = service.calendarList().get(calendarId="primary").execute()
        _api_available = True
        return True, f"OK — {cal.get('summary', 'connected')}"
    except Exception as e:
        _api_available = False
        return False, f"Calendar API failed: {e}"


# ── Core: fetch upcoming events ──────────────────────────────────────────────


def get_upcoming_meetings(
    days_ahead=7,
    max_results=30,
    calendar_id="primary",
):
    """Fetch upcoming calendar events that look like customer meetings.

    Returns a list of dicts with keys:
        event_id, title, start, end, duration_min,
        attendees (list of {email, name, response}),
        location, meet_link, description, organizer
    """
    global _api_available
    if _api_available is False:
        return []

    try:
        service = _get_service()
    except Exception:
        return []

    now = datetime.utcnow()
    time_min = now.isoformat() + "Z"
    time_max = (now + timedelta(days=days_ahead)).isoformat() + "Z"

    try:
        result = service.events().list(
            calendarId=calendar_id,
            timeMin=time_min,
            timeMax=time_max,
            maxResults=max_results,
            singleEvents=True,
            orderBy="startTime",
        ).execute()

        events = result.get("items", [])
        meetings = []
        for ev in events:
            # Skip all-day events
            start = ev.get("start", {})
            if "date" in start and "dateTime" not in start:
                continue

            # Parse start/end
            start_dt = _parse_dt(start.get("dateTime", ""))
            end_dt = _parse_dt(ev.get("end", {}).get("dateTime", ""))

            duration_min = 0
            if start_dt and end_dt:
                duration_min = int((end_dt - start_dt).total_seconds() / 60)

            # Skip very short events (< 10 min) or very long (> 4 hours)
            if duration_min < 10 or duration_min > 240:
                continue

            # Parse attendees
            attendees = []
            for att in ev.get("attendees", []):
                if att.get("self"):
                    continue  # Skip Greg himself
                if att.get("resource"):
                    continue  # Skip room resources
                attendees.append({
                    "email": att.get("email", ""),
                    "name": att.get("displayName", ""),
                    "response": att.get("responseStatus", ""),
                })

            # Extract Google Meet link
            meet_link = ""
            conf = ev.get("conferenceData", {})
            for ep in conf.get("entryPoints", []):
                if ep.get("entryPointType") == "video":
                    meet_link = ep.get("uri", "")
                    break

            meetings.append({
                "event_id": ev.get("id", ""),
                "title": ev.get("summary", "(No title)"),
                "start": start_dt,
                "end": end_dt,
                "duration_min": duration_min,
                "attendees": attendees,
                "location": ev.get("location", ""),
                "meet_link": meet_link,
                "description": (ev.get("description") or "")[:500],
                "organizer": ev.get("organizer", {}).get("email", ""),
            })

        _api_available = True
        return meetings

    except Exception as e:
        logger.warning("Calendar API fetch failed: %s", e)
        return []


def get_todays_meetings(max_results=15, calendar_id="primary"):
    """Fetch ALL of today's meetings (past AND future) in US/Eastern time.

    Returns the same format as get_upcoming_meetings, plus an `already_happened`
    boolean field indicating whether the meeting has already ended.
    """
    global _api_available
    if _api_available is False:
        return []

    try:
        service = _get_service()
    except Exception:
        return []

    try:
        import pytz
        from config import DISPLAY_TIMEZONE
        et = pytz.timezone(DISPLAY_TIMEZONE)
        now_et = datetime.now(et)
        today_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = now_et.replace(hour=23, minute=59, second=59, microsecond=0)

        time_min = today_start.isoformat()
        time_max = today_end.isoformat()

        result = service.events().list(
            calendarId=calendar_id,
            timeMin=time_min,
            timeMax=time_max,
            maxResults=max_results,
            singleEvents=True,
            orderBy="startTime",
        ).execute()

        events = result.get("items", [])
        now_utc = datetime.utcnow()
        meetings = []
        for ev in events:
            # Skip all-day events
            start = ev.get("start", {})
            if "date" in start and "dateTime" not in start:
                continue

            start_dt = _parse_dt(start.get("dateTime", ""))
            end_dt = _parse_dt(ev.get("end", {}).get("dateTime", ""))

            duration_min = 0
            if start_dt and end_dt:
                duration_min = int((end_dt - start_dt).total_seconds() / 60)

            if duration_min < 10 or duration_min > 240:
                continue

            # Parse attendees
            attendees = []
            for att in ev.get("attendees", []):
                if att.get("self"):
                    continue
                if att.get("resource"):
                    continue
                attendees.append({
                    "email": att.get("email", ""),
                    "name": att.get("displayName", ""),
                    "response": att.get("responseStatus", ""),
                })

            # Extract Google Meet link
            meet_link = ""
            conf = ev.get("conferenceData", {})
            for ep in conf.get("entryPoints", []):
                if ep.get("entryPointType") == "video":
                    meet_link = ep.get("uri", "")
                    break

            already_happened = end_dt < now_utc if end_dt else False

            meetings.append({
                "event_id": ev.get("id", ""),
                "title": ev.get("summary", "(No title)"),
                "start": start_dt,
                "end": end_dt,
                "duration_min": duration_min,
                "attendees": attendees,
                "location": ev.get("location", ""),
                "meet_link": meet_link,
                "description": (ev.get("description") or "")[:500],
                "organizer": ev.get("organizer", {}).get("email", ""),
                "already_happened": already_happened,
            })

        _api_available = True
        return meetings

    except Exception as e:
        logger.warning("Calendar today's meetings fetch failed: %s", e)
        return []


def get_meetings_in_range(start_dt, end_dt, calendar_id="primary"):
    """Fetch meetings between two datetime objects. For post-meeting detection."""
    global _api_available
    if _api_available is False:
        return []

    try:
        service = _get_service()
    except Exception:
        return []

    try:
        result = service.events().list(
            calendarId=calendar_id,
            timeMin=start_dt.isoformat() + "Z" if not start_dt.tzinfo else start_dt.isoformat(),
            timeMax=end_dt.isoformat() + "Z" if not end_dt.tzinfo else end_dt.isoformat(),
            maxResults=50,
            singleEvents=True,
            orderBy="startTime",
        ).execute()

        # Reuse same parsing as get_upcoming_meetings
        events = result.get("items", [])
        meetings = []
        for ev in events:
            start = ev.get("start", {})
            if "date" in start and "dateTime" not in start:
                continue
            start_parsed = _parse_dt(start.get("dateTime", ""))
            end_parsed = _parse_dt(ev.get("end", {}).get("dateTime", ""))
            duration_min = 0
            if start_parsed and end_parsed:
                duration_min = int((end_parsed - start_parsed).total_seconds() / 60)
            if duration_min < 10 or duration_min > 240:
                continue

            attendees = []
            for att in ev.get("attendees", []):
                if att.get("self") or att.get("resource"):
                    continue
                attendees.append({
                    "email": att.get("email", ""),
                    "name": att.get("displayName", ""),
                    "response": att.get("responseStatus", ""),
                })

            meetings.append({
                "event_id": ev.get("id", ""),
                "title": ev.get("summary", "(No title)"),
                "start": start_parsed,
                "end": end_parsed,
                "duration_min": duration_min,
                "attendees": attendees,
                "description": (ev.get("description") or "")[:500],
            })

        return meetings

    except Exception as e:
        logger.warning("Calendar range query failed: %s", e)
        return []


# ── Account matching ──────────────────────────────────────────────────────────

# Known Ramp internal domains to exclude when matching attendees → accounts
_INTERNAL_DOMAINS = {"ramp.com", "tryramp.com", "rampcard.com"}

# Generic email domains that don't identify a company
_GENERIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "live.com", "msn.com", "protonmail.com",
}

# Domains for calendar resources, not real people
_RESOURCE_DOMAINS = {
    "resource.calendar.google.com",
}


def extract_external_attendees(meeting):
    """Return attendees whose email domain is not Ramp, generic, or a resource."""
    external = []
    for att in meeting.get("attendees", []):
        email = att.get("email", "").lower()
        if not email or "@" not in email:
            continue
        domain = email.split("@")[1]
        if domain in _INTERNAL_DOMAINS or domain in _GENERIC_DOMAINS:
            continue
        # Skip Google Calendar room/resource accounts
        if domain in _RESOURCE_DOMAINS or email.startswith("c_"):
            continue
        att_copy = dict(att)
        att_copy["domain"] = domain
        # Ensure name falls back to email if empty
        if not att_copy.get("name"):
            att_copy["name"] = email
        external.append(att_copy)
    return external


def match_meeting_to_account(meeting, account_cache=None):
    """Try to match a calendar meeting to an SFDC account.

    Uses the unified match_account() with open-opp tiebreaker.
    Strategy:
    1. Attendee emails + meeting title → match_account() (name + domain + open opps)
    2. Meeting title only → fuzzy match via account_resolver

    Returns {account_id, account_name, match_source} or None.
    """
    external = extract_external_attendees(meeting)
    ext_emails = [att["email"] for att in external]

    # Extract clean title for matching context
    title = meeting.get("title", "")
    clean_title = ""
    if title:
        clean_title = _extract_company_from_title(title)
        if not clean_title:
            clean_title = re.sub(
                r"^(meeting|call|sync|check.?in|intro|demo|review|follow.?up)\s*[:\-—]\s*",
                "", title, flags=re.IGNORECASE
            ).strip()
            clean_title = re.sub(
                r"\s*[<\[(].*$", "", clean_title
            ).strip()

    # Strategy 1: Use unified match_account (handles name + domain + open opp tiebreaker)
    if ext_emails or clean_title:
        try:
            from utils.account_matcher import match_account
            result = match_account(
                account_name=clean_title or "",
                participant_emails=ext_emails,
            )
            if result.matched:
                source = "attendee_domain" if ext_emails else "title"
                return {
                    "account_id": result.account_id,
                    "account_name": result.account_name,
                    "match_source": source,
                }
        except Exception as e:
            logger.debug("Account match via match_account failed: %s", e)

    # Strategy 2: Fallback — title-only fuzzy match via account_resolver
    if clean_title and len(clean_title) >= 3:
        try:
            from utils.account_resolver import resolve_account_name
            result = resolve_account_name(None, clean_title)
            if result:
                return {
                    "account_id": result["account_id"],
                    "account_name": result["account_name"],
                    "match_source": "title",
                }
        except Exception as e:
            logger.debug("Account match by title failed: %s", e)

    return None


# ── Helpers ───────────────────────────────────────────────────────────────────


def _extract_company_from_title(title):
    """Extract the non-Ramp company name from titles like 'Ramp // OnDeck Partners'.

    Handles separators: //, /, |, <>, x, -, —
    Handles both 'Ramp // Company' and 'Company // Ramp'.
    Returns the company name or empty string if not a two-party title.
    """
    ramp_aliases = {"ramp", "tryramp", "ramp financial"}

    # Try various separators
    for sep_pattern in [r"\s*//\s*", r"\s*\|\s*", r"\s*<>\s*", r"\s+x\s+", r"\s*[—–]\s*", r"\s*/\s+"]:
        parts = re.split(sep_pattern, title, maxsplit=1)
        if len(parts) == 2:
            left, right = parts[0].strip(), parts[1].strip()
            left_lower, right_lower = left.lower(), right.lower()

            if left_lower in ramp_aliases:
                return right
            if right_lower in ramp_aliases:
                return left
            # Neither side is Ramp — try the non-empty/longer side
            # (often the format is "Company / Topic" — try the first part)
            if left and right:
                return left  # Best guess: first part is the company

    return ""


def format_meeting_time(utc_dt):
    """Convert a naive UTC datetime to display timezone string (e.g. '3:15 PM ET')."""
    if not utc_dt:
        return "?"
    try:
        import pytz
        from config import DISPLAY_TIMEZONE
        utc = pytz.utc.localize(utc_dt)
        local = utc.astimezone(pytz.timezone(DISPLAY_TIMEZONE))
        return local.strftime("%-I:%M %p ET")
    except Exception:
        return utc_dt.strftime("%-I:%M %p")


def _parse_dt(dt_str):
    """Parse an ISO datetime string from the Calendar API."""
    if not dt_str:
        return None
    try:
        # Handle timezone offset format: 2024-01-15T10:00:00-08:00
        clean = re.sub(r"([+-]\d{2}):(\d{2})$", r"\1\2", dt_str)
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                parsed = datetime.strptime(clean, fmt)
                # Convert to naive UTC for consistency
                if parsed.tzinfo:
                    from datetime import timezone
                    parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
                return parsed
            except ValueError:
                continue
    except Exception:
        pass
    return None


def is_customer_meeting(meeting):
    """Heuristic: is this likely a customer meeting (vs internal/personal)?

    A meeting is likely customer-facing if:
    - Has external (non-Ramp) attendees, OR
    - Title contains customer-ish keywords
    """
    external = extract_external_attendees(meeting)
    if external:
        return True

    title = (meeting.get("title") or "").lower()
    customer_keywords = [
        "demo", "onboard", "check-in", "check in", "review", "qbr",
        "kickoff", "kick-off", "expansion", "renewal", "upsell",
        "implementation", "training", "intro",
    ]
    return any(kw in title for kw in customer_keywords)
