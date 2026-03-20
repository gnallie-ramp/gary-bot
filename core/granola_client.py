"""Granola meeting data — local cache + API access.

Reads Granola's local cache for meeting metadata and summaries.
Falls back to the Granola API for transcripts (v6+ only caches
transcripts for actively viewed meetings).
"""
from __future__ import annotations

import gzip
import json
import logging
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

GRANOLA_DIR = Path.home() / "Library" / "Application Support" / "Granola"


def _get_cache_path() -> Path:
    """Get the highest-version Granola cache file."""
    candidates = sorted(
        (p for p in GRANOLA_DIR.glob("cache-v*.json") if re.search(r"v(\d+)", p.name)),
        key=lambda p: int(re.search(r"v(\d+)", p.name).group(1)),
        reverse=True,
    )
    return candidates[0] if candidates else GRANOLA_DIR / "cache-v4.json"


def _load_state() -> dict:
    """Load and return the Granola cache state dict."""
    cache_path = _get_cache_path()
    if not cache_path.exists():
        logger.warning("Granola cache not found at %s", cache_path)
        return {}
    with open(cache_path, "r") as f:
        data = json.load(f)
    inner = data.get("cache", {})
    if isinstance(inner, str):
        inner = json.loads(inner)
    return inner.get("state", {})


def _get_workos_token() -> str | None:
    """Load the WorkOS access token from Granola's supabase.json."""
    path = GRANOLA_DIR / "supabase.json"
    if not path.exists():
        return None
    with open(path, "r") as f:
        data = json.load(f)
    raw = data.get("workos_tokens", "")
    if not raw:
        return None
    workos = json.loads(raw) if isinstance(raw, str) else raw
    return workos.get("access_token")


def _api_request(url: str, payload: dict | None = None):
    """Make an authenticated request to the Granola API."""
    token = _get_workos_token()
    if not token:
        return None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Encoding": "gzip",
    }
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode()
    else:
        data = None
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        raw = e.read()
        if e.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        logger.warning("Granola API error %d: %s", e.code, raw.decode()[:200])
        return None
    except Exception as e:
        logger.warning("Granola API request failed: %s", e)
        return None


def _parse_timestamp(ts) -> float:
    """Parse a timestamp (ISO string or numeric seconds) to epoch float."""
    if isinstance(ts, (int, float)):
        return ts
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.timestamp()
        except (ValueError, TypeError):
            return 0.0
    return 0.0


def _format_segments(segments: list[dict]) -> str | None:
    """Format transcript segments into readable text."""
    if not segments:
        return None
    first_ts = _parse_timestamp(segments[0].get("start_timestamp", 0))
    lines = []
    for seg in segments:
        text = seg.get("text", "")
        source = seg.get("source", "")
        start = _parse_timestamp(seg.get("start_timestamp", 0))
        elapsed = max(0, start - first_ts)
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        ts_str = f"[{minutes:02d}:{seconds:02d}]"
        if source:
            lines.append(f"{ts_str} {source}: {text}")
        else:
            lines.append(f"{ts_str} {text}")
    return "\n".join(lines)


def _prosemirror_to_text(node) -> str:
    """Recursively extract text from a ProseMirror document node."""
    if isinstance(node, str):
        return node
    if not isinstance(node, dict):
        return ""
    node_type = node.get("type", "")
    children = node.get("content", [])
    if node_type == "text":
        return node.get("text", "")
    if node_type == "heading":
        level = node.get("attrs", {}).get("level", 3)
        child_text = "".join(_prosemirror_to_text(c) for c in children)
        return f"{'#' * level} {child_text}\n\n"
    if node_type in ("bulletList", "orderedList"):
        items = []
        for i, child in enumerate(children):
            prefix = "- " if node_type == "bulletList" else f"{i+1}. "
            item_text = "".join(_prosemirror_to_text(c) for c in child.get("content", []))
            items.append(f"{prefix}{item_text.strip()}")
        return "\n".join(items) + "\n\n"
    if node_type == "paragraph":
        child_text = "".join(_prosemirror_to_text(c) for c in children)
        return child_text + "\n\n"
    return "".join(_prosemirror_to_text(c) for c in children)


# ── Public API ──────────────────────────────────────────────────────────────


def get_recent_meetings(minutes: int = 10, skip_end_check: bool = False) -> list[dict]:
    """Return meetings that ended within the last `minutes` minutes.

    Checks local cache first, then falls back to the Granola API
    (cache can be stale if Granola hasn't flushed recently).

    Parameters
    ----------
    minutes : int
        How far back to look.
    skip_end_check : bool
        If True, skip the meeting-end detection filter. Use this for
        manual commands like /post-meeting where the user knows the call
        is over. The auto sweep (every 3 min) should keep this False.

    Each dict has: id, title, created_at, people, has_transcript.
    """
    now = datetime.now(timezone.utc).timestamp()
    cutoff = now - (minutes * 60)
    seen_ids = set()
    results = []

    # ── Source 1: Local cache ────────────────────────────────────────
    state = _load_state()
    docs = state.get("documents", {})
    for doc_id, doc in docs.items():
        # Use updated_at for recency (set when meeting ends/transcript lands)
        # Fall back to created_at if updated_at not available
        ts_str = doc.get("updated_at", "") or doc.get("created_at", "")
        if not ts_str:
            continue
        ts = _parse_timestamp(ts_str)
        if ts < cutoff:
            continue

        # Skip meetings still in progress (unless caller said to skip this check).
        # Priority 1: meeting_end_count >= 1 = Granola confirmed call ended (instant)
        # Priority 2: calendar end time + 10 min buffer (covers meetings running over)
        # Priority 3: updated_at > 15 min ago with no end signals = assume ended
        if not skip_end_check:
            _CAL_END_BUFFER = 10 * 60  # 10 minutes after scheduled end
            _STALE_BUFFER = 15 * 60    # 15 minutes since last update = assume ended
            meeting_ended = doc.get("meeting_end_count", 0) >= 1
            if not meeting_ended:
                cal_event = doc.get("google_calendar_event", {})
                cal_end = cal_event.get("end", {}).get("dateTime", "")
                if cal_end:
                    cal_end_ts = _parse_timestamp(cal_end)
                    meeting_ended = cal_end_ts and (cal_end_ts + _CAL_END_BUFFER) <= now
            if not meeting_ended:
                # Fallback: if updated_at is >15 min old, meeting is almost certainly over
                meeting_ended = (now - ts) > _STALE_BUFFER
            if not meeting_ended:
                logger.debug(
                    "Skipping meeting '%s' — still in progress "
                    "(meeting_end_count=%s, cal end not yet passed +10m buffer, "
                    "updated %.0fs ago)",
                    doc.get("title", "?"), doc.get("meeting_end_count", 0),
                    now - ts,
                )
                continue

        raw_people = doc.get("people", [])
        if isinstance(raw_people, dict):
            people_list = []
            creator = raw_people.get("creator")
            if creator:
                people_list.append(creator)
            people_list.extend(raw_people.get("attendees", []))
        else:
            people_list = raw_people

        results.append({
            "id": doc_id,
            "title": doc.get("title", "Untitled"),
            "created_at": doc.get("created_at", ""),
            "updated_at": doc.get("updated_at", ""),
            "people": people_list,
            "has_transcript": doc.get("transcribe", True),
            "notes_markdown": doc.get("notes_markdown", ""),
        })
        seen_ids.add(doc_id)

    # ── Source 2: Granola API (catches meetings not yet in local cache) ──
    try:
        api_docs = _api_request(
            "https://api.granola.ai/v1/get-documents",
            {"limit": 20},
        )
        if isinstance(api_docs, list):
            for doc in api_docs:
                doc_id = doc.get("id", "")
                if doc_id in seen_ids:
                    continue
                # Use updated_at for recency (set when meeting ends/transcript lands)
                # Fall back to created_at if updated_at not available
                ts_str = doc.get("updated_at", "") or doc.get("created_at", "")
                if not ts_str:
                    continue
                ts = _parse_timestamp(ts_str)
                if ts < cutoff:
                    continue

                # Skip meetings still in progress (same logic as local cache)
                if not skip_end_check:
                    _CAL_END_BUFFER = 10 * 60
                    _STALE_BUFFER = 15 * 60
                    api_ended = doc.get("meeting_end_count", 0) >= 1
                    if not api_ended:
                        cal_event = doc.get("google_calendar_event", {})
                        cal_end = cal_event.get("end", {}).get("dateTime", "")
                        if cal_end:
                            cal_end_ts = _parse_timestamp(cal_end)
                            api_ended = cal_end_ts and (cal_end_ts + _CAL_END_BUFFER) <= now
                    if not api_ended:
                        api_ended = (now - ts) > _STALE_BUFFER
                    if not api_ended:
                        logger.debug(
                            "Skipping API meeting '%s' — still in progress",
                            doc.get("title", "?"),
                        )
                        continue

                people_list = doc.get("people", [])
                if isinstance(people_list, dict):
                    pl = []
                    creator = people_list.get("creator")
                    if creator:
                        pl.append(creator)
                    pl.extend(people_list.get("attendees", []))
                    people_list = pl

                results.append({
                    "id": doc_id,
                    "title": doc.get("title", "Untitled"),
                    "created_at": doc.get("created_at", ""),
                    "updated_at": doc.get("updated_at", ""),
                    "people": people_list,
                    "has_transcript": True,
                    "notes_markdown": "",
                })
                seen_ids.add(doc_id)
    except Exception as e:
        logger.debug("Granola API listing failed: %s", e)

    results.sort(key=lambda m: m["created_at"], reverse=True)
    return results


def get_transcript(meeting_id: str) -> str | None:
    """Get transcript for a meeting. Tries local cache, then API."""
    state = _load_state()
    transcripts = state.get("transcripts", {})
    if meeting_id in transcripts and transcripts[meeting_id]:
        return _format_segments(transcripts[meeting_id])

    # Fall back to API
    result = _api_request(
        "https://api.granola.ai/v1/get-document-transcript",
        {"document_id": meeting_id},
    )
    if isinstance(result, list):
        return _format_segments(result)
    return None


def get_metadata(meeting_id: str) -> dict | None:
    """Get meeting metadata: title, created_at, people, notes."""
    state = _load_state()
    docs = state.get("documents", {})
    if meeting_id not in docs:
        return None
    doc = docs[meeting_id]

    raw_people = doc.get("people", [])
    if isinstance(raw_people, dict):
        people_list = []
        creator = raw_people.get("creator")
        if creator:
            people_list.append(creator)
        people_list.extend(raw_people.get("attendees", []))
    else:
        people_list = raw_people

    return {
        "title": doc.get("title", "Untitled"),
        "created_at": doc.get("created_at", ""),
        "updated_at": doc.get("updated_at", ""),
        "notes_markdown": doc.get("notes_markdown", ""),
        "people": people_list,
        "has_transcript": doc.get("transcribe", True),
    }


def get_summary(meeting_id: str) -> str | None:
    """Get AI-generated summary panels for a meeting."""
    state = _load_state()
    panels = state.get("documentPanels", {})
    if meeting_id not in panels:
        return None
    meeting_panels = panels[meeting_id]
    summaries = []
    for _, panel in meeting_panels.items():
        title = panel.get("title", "")
        content = panel.get("content", "")
        if not content:
            continue
        if isinstance(content, dict):
            rendered = _prosemirror_to_text(content).strip()
        else:
            rendered = str(content).strip()
        if rendered:
            summaries.append(f"### {title}\n\n{rendered}")
    return "\n\n".join(summaries) if summaries else None


def _is_resource_email(email: str) -> bool:
    """Return True if this is a Google Calendar resource, not a person."""
    lower = email.lower()
    return (
        lower.endswith("@resource.calendar.google.com")
        or lower.startswith("c_")  # Google Calendar room resources
        or "calendar-resource" in lower
    )


def extract_attendee_info(people: list) -> tuple[list[str], list[str]]:
    """Extract external attendee names and emails from Granola people list.

    Returns (names, emails) — only non-Ramp, non-resource attendees.
    """
    names = []
    emails = []
    for p in people:
        if isinstance(p, dict):
            details = p.get("details", {})
            person = details.get("person", {})
            email = (
                person.get("email", "")
                or p.get("email", "")
                or details.get("email", "")
            )
            full_name = (
                person.get("name", {}).get("fullName", "")
                or p.get("name", "")
            )
            # Skip Ramp employees
            if email and "@ramp.com" in email.lower():
                continue
            # Skip Google Calendar room/resource accounts
            if email and _is_resource_email(email):
                continue
            if email:
                emails.append(email)
            if full_name:
                names.append(full_name)
        elif isinstance(p, str):
            if "@ramp.com" not in p.lower() and not _is_resource_email(p):
                if "@" in p:
                    emails.append(p)
                else:
                    names.append(p)
    return names, emails
