"""Pending email drafts store.

Persists follow-up email drafts to a JSON file so that Project Glass
(which has Gumstack Gmail MCP access) can read them and create Gmail
drafts.  The Slack bot writes here; Glass reads and marks sent.

File location: ~/.gary_bot_pending_drafts.json
"""
from __future__ import annotations

import json
import os
import logging
import tempfile
import threading
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_STATE_FILE = os.path.expanduser("~/.gary_bot_pending_drafts.json")
_lock = threading.Lock()


# ── persistence helpers ──────────────────────────────────────────────────────

def _load() -> list[dict]:
    """Read the drafts list from disk.  Returns [] on any error."""
    if not os.path.exists(_STATE_FILE):
        return []
    try:
        with open(_STATE_FILE, "r") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            return data
        logger.warning("Pending drafts file has unexpected type; resetting.")
        return []
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load pending drafts (%s); starting fresh.", exc)
        return []


def _save(drafts: list[dict]) -> None:
    """Atomic write: write to a temp file then rename."""
    try:
        dir_name = os.path.dirname(_STATE_FILE)
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(drafts, fh, indent=2)
            os.replace(tmp_path, _STATE_FILE)
        except BaseException:
            os.unlink(tmp_path)
            raise
    except OSError as exc:
        logger.error("Failed to save pending drafts: %s", exc)


# ── public API ───────────────────────────────────────────────────────────────

def save_draft(
    draft_id: str,
    to: str,
    cc: str,
    subject: str,
    html_body: str,
    account_name: str = "",
    meeting_id: str = "",
    label: str = "",
) -> str:
    """Persist a pending email draft.  Returns the draft_id."""
    entry = {
        "draft_id": draft_id,
        "to": to,
        "cc": cc,
        "subject": subject,
        "html_body": html_body,
        "account_name": account_name,
        "meeting_id": meeting_id,
        "label": label,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "pending",
    }
    with _lock:
        drafts = _load()
        # Replace if same draft_id already exists
        drafts = [d for d in drafts if d.get("draft_id") != draft_id]
        drafts.append(entry)
        _save(drafts)
    logger.info("Saved pending draft %s (to=%s, subject=%s)", draft_id, to, subject)
    return draft_id


def get_draft(draft_id: str) -> dict | None:
    """Retrieve a single draft by ID, or None if not found."""
    with _lock:
        drafts = _load()
    for d in drafts:
        if d.get("draft_id") == draft_id:
            return d
    return None


def list_pending() -> list[dict]:
    """Return all drafts with status 'pending'."""
    with _lock:
        drafts = _load()
    return [d for d in drafts if d.get("status") == "pending"]


def mark_sent(draft_id: str) -> None:
    """Mark a draft as sent."""
    with _lock:
        drafts = _load()
        for d in drafts:
            if d.get("draft_id") == draft_id:
                d["status"] = "sent"
                break
        _save(drafts)
    logger.info("Marked draft %s as sent.", draft_id)


def flush_to_gmail() -> tuple[int, int]:
    """Retry all pending drafts via Gumstack Gmail MCP.

    Returns (succeeded, failed) counts.
    """
    from core.gumstack_gmail import create_draft as gumstack_create, is_available as gumstack_ok

    if not gumstack_ok():
        logger.warning("Gumstack Gmail not available — cannot flush pending drafts")
        return 0, 0

    pending = list_pending()
    if not pending:
        return 0, 0

    succeeded = 0
    failed = 0
    for draft in pending:
        try:
            result = gumstack_create(
                to=draft["to"],
                subject=draft["subject"],
                html_body=draft["html_body"],
                cc=draft.get("cc", ""),
                label=draft.get("label", ""),
            )
            if result["success"]:
                mark_sent(draft["draft_id"])
                succeeded += 1
                logger.info("Flushed pending draft %s to Gmail", draft["draft_id"])
            else:
                failed += 1
                logger.warning("Flush failed for draft %s", draft["draft_id"])
        except Exception as e:
            failed += 1
            logger.warning("Flush error for draft %s: %s", draft["draft_id"], e)

    return succeeded, failed


def cleanup(days: int = 7) -> None:
    """Remove drafts older than *days* days."""
    cutoff = time.time() - (days * 86400)
    with _lock:
        drafts = _load()
        before = len(drafts)
        kept = []
        for d in drafts:
            try:
                created = datetime.fromisoformat(d["created_at"])
                if created.timestamp() >= cutoff:
                    kept.append(d)
            except (KeyError, ValueError):
                kept.append(d)  # keep entries we can't parse
        if len(kept) < before:
            _save(kept)
            logger.debug("Pending drafts cleanup: removed %d old entries.", before - len(kept))
