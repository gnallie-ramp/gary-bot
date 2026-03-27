"""Scheduled job: remind about unsent Claude Drafts and stale auto-drafts.

Checks Gmail via Gumstack MCP for drafts labeled under "Claude Drafts/*" that
are older than a configurable threshold. Cross-references with sent mail to
auto-detect duplicates (if the user already sent a follow-up to the same
recipient). For genuinely unsent drafts, DMs with escalating urgency.
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta

from config import GREG_SLACK_ID
from core.gumstack_gmail import read_emails

logger = logging.getLogger(__name__)

# Urgency thresholds (hours since draft created)
_NUDGE_HOURS = 4
_WARNING_HOURS = 24
_URGENT_HOURS = 48

# Don't remind about the same draft more than once per 4 hours
_REMIND_COOLDOWN = 4 * 3600
_last_reminded: dict[tuple[str, str], float] = {}


def _get_claude_drafts(user_id=None):
    """Fetch drafts from Claude Drafts/* labels via Gumstack MCP.

    Returns list of dicts with subject, to_addr, date, label.
    """
    try:
        # Search for drafts with Claude-generated subjects
        drafts_raw = read_emails(
            'in:drafts (subject:"Ramp Follow-Up" OR subject:"Claude Draft")',
            max_results=50,
            user_id=user_id,
        )
        if not drafts_raw:
            return []

        drafts = []
        for em in drafts_raw:
            subject = em.get("subject", "")
            to_addr = em.get("to", "")
            date_str = em.get("date", "")

            # Only include Claude-generated follow-up drafts
            if "Ramp Follow-Up" in subject or "Claude Draft" in subject:
                drafts.append({
                    "id": em.get("id", ""),
                    "subject": subject,
                    "to_addr": to_addr,
                    "date": date_str,
                })

        return drafts

    except Exception as e:
        logger.warning("Failed to fetch Claude drafts: %s", e)
        return []


def _check_if_already_sent(to_addr, subject_keywords, days=7, user_id=None):
    """Check sent mail to see if the user already sent a similar email.

    Returns True if a matching sent email was found.
    """
    if not to_addr:
        return False

    # Extract bare email
    match = re.search(r"<([^>]+)>", to_addr)
    addr = match.group(1).lower() if match else to_addr.strip().lower()

    sent_emails = read_emails(
        f"from:me to:{addr} newer_than:{days}d",
        max_results=10,
        user_id=user_id,
    )

    for em in sent_emails:
        sent_subj = em.get("subject", "").lower()
        if any(kw.lower() in sent_subj for kw in subject_keywords):
            return True

    return False


def _estimate_draft_age_hours(date_str):
    """Estimate how many hours old a draft is from its Date header."""
    if not date_str:
        return 0
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        age = datetime.now(dt.tzinfo) - dt
        return age.total_seconds() / 3600
    except Exception:
        return 0


def run_draft_reminder(client, user_id=None) -> None:
    """Check for unsent Claude Drafts and remind the user.

    Also auto-flags drafts where a follow-up was already sent manually.
    """
    dm_target = user_id or GREG_SLACK_ID

    try:
        drafts = _get_claude_drafts(user_id=user_id)
        if not drafts:
            logger.info("Draft reminder: no Claude drafts found.")
            return

        now = time.time()
        unsent = []
        already_sent = []

        for draft in drafts:
            subject = draft["subject"]
            to_addr = draft["to_addr"]
            age_hours = _estimate_draft_age_hours(draft["date"])

            # Skip very recent drafts (< threshold)
            if age_hours < _NUDGE_HOURS:
                continue

            # Check if already reminded recently
            draft_key = (dm_target, f"{to_addr}_{subject}")
            if now - _last_reminded.get(draft_key, 0) < _REMIND_COOLDOWN:
                continue

            # Extract keywords from subject for matching
            keywords = [w for w in subject.replace("-", " ").split()
                        if len(w) > 3 and w.lower() not in ("ramp", "follow", "up")]

            if _check_if_already_sent(to_addr, keywords, user_id=user_id):
                already_sent.append(draft)
            else:
                # Determine urgency
                if age_hours >= _URGENT_HOURS:
                    urgency = ":rotating_light:"
                elif age_hours >= _WARNING_HOURS:
                    urgency = ":warning:"
                else:
                    urgency = ":email:"

                unsent.append({
                    **draft,
                    "urgency": urgency,
                    "age_hours": round(age_hours),
                })

        # Log already-sent drafts (could auto-delete in future)
        if already_sent:
            logger.info(
                "Draft reminder: %d drafts have matching sent emails (candidates for cleanup).",
                len(already_sent),
            )

        if not unsent:
            logger.info("Draft reminder: no unsent drafts needing reminder.")
            return

        # Build the DM
        lines = [":mailbox_with_mail: *Unsent Follow-Up Drafts*"]
        for d in unsent:
            to_short = d["to_addr"].split("<")[0].strip() or d["to_addr"]
            lines.append(
                f"{d['urgency']} \"{d['subject']}\" \u2192 {to_short} ({d['age_hours']}h ago)"
            )

        if already_sent:
            lines.append(f"\n_Also found {len(already_sent)} draft(s) where you already sent a follow-up \u2014 safe to delete._")

        message = "\n".join(lines)

        client.chat_postMessage(
            channel=dm_target,
            text=message,
        )

        # Update cooldowns
        for d in unsent:
            draft_key = (dm_target, f"{d['to_addr']}_{d['subject']}")
            _last_reminded[draft_key] = now

        logger.info("Draft reminder sent: %d unsent, %d already sent.", len(unsent), len(already_sent))

    except Exception as e:
        logger.error("Draft reminder failed: %s", e)
