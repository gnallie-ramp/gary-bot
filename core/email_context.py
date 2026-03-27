"""Email context provider — enriches bot alerts with email intelligence.

Two data sources, with automatic fallback:
1. Gmail IMAP (real-time, if available)
2. Snowflake dim_emails (90-day history, always available)

Jobs call get_email_context(account_id, account_name) and get back a
dict with last_contact, unanswered_emails, recent_threads, etc.
"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from core.snowflake_client import run_query
from utils.account_resolver import fetch_contact_emails

logger = logging.getLogger(__name__)


def _get_contact_emails_for_account(account_id: str) -> list[str]:
    """Fetch SFDC contact emails for an account."""
    try:
        contacts = fetch_contact_emails(None, [account_id])
        account_contacts = contacts.get(account_id, [])
        return [c["email"] for c in account_contacts if c.get("email")]
    except Exception as e:
        logger.debug("Failed to fetch contacts for %s: %s", account_id, e)
        return []


def _get_domain_from_contacts(contact_emails: list[str]) -> str | None:
    """Extract the most common non-generic domain from contact emails."""
    generic = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com"}
    domains: dict[str, int] = {}
    for em in contact_emails:
        parts = em.split("@")
        if len(parts) == 2:
            d = parts[1].lower()
            if d not in generic:
                domains[d] = domains.get(d, 0) + 1
    if domains:
        return max(domains, key=domains.get)
    return None


# ── Gmail IMAP source ────────────────────────────────────────────────────────


def _gmail_context(
    contact_emails: list[str], domain: str | None, days: int = 30,
    user_id: str | None = None,
) -> dict | None:
    """Try to get email context from Gumstack Gmail MCP."""
    try:
        from core.gumstack_gmail import read_emails, is_available

        if not is_available(user_id=user_id):
            return None

        # Build Gmail search query from contacts/domain
        if contact_emails:
            addr_queries = " OR ".join(
                f"from:{addr} OR to:{addr}" for addr in contact_emails[:5]
            )
            query = f"({addr_queries}) newer_than:{days}d"
        elif domain:
            query = f"(from:@{domain} OR to:@{domain}) newer_than:{days}d"
        else:
            return None

        raw_emails = read_emails(query, max_results=10, user_id=user_id)
        if not raw_emails:
            # Try domain if contact search found nothing
            if domain and contact_emails:
                domain_query = f"(from:@{domain} OR to:@{domain}) newer_than:{days}d"
                raw_emails = read_emails(domain_query, max_results=10, user_id=user_id)

        if not raw_emails:
            return None

        # Determine the user's email for direction detection
        from core.user_registry import get_user_email
        my_email = (get_user_email(user_id) if user_id else "").lower()
        if not my_email:
            from config import GMAIL_ADDRESS
            my_email = (GMAIL_ADDRESS or "").lower()

        # Convert to the format expected by the rest of this function
        import re
        def _parse_addr(addr_str):
            if not addr_str:
                return ""
            match = re.search(r"<([^>]+)>", addr_str)
            return match.group(1).lower() if match else addr_str.strip().lower()

        emails = []
        for em in raw_emails:
            from_addr = _parse_addr(em.get("from", ""))
            direction = "outbound" if my_email and my_email in from_addr else "inbound"
            emails.append({
                "date": em.get("date", ""),
                "subject": em.get("subject", ""),
                "from_addr": from_addr,
                "to_addr": em.get("to", ""),
                "direction": direction,
                "snippet": (em.get("body", "") or "")[:300],
            })

        if not emails:
            return None

        # Last contact
        last = emails[0]
        last_direction = "You → them" if last["direction"] == "outbound" else "They → you"

        # Unanswered inbound
        unanswered = [e for e in emails if e["direction"] == "inbound"]
        # Check if an outbound follows each inbound
        unanswered_count = 0
        if emails and emails[0]["direction"] == "inbound":
            unanswered_count = 1  # Most recent is inbound = potentially unanswered

        # Thread subjects
        subjects = []
        seen = set()
        for e in emails:
            subj = e["subject"]
            if subj not in seen:
                seen.add(subj)
                subjects.append({
                    "subject": subj,
                    "date": e["date"],
                    "direction": e["direction"],
                })

        return {
            "source": "gmail",
            "last_contact_date": last["date"],
            "last_contact_subject": last["subject"],
            "last_contact_direction": last_direction,
            "last_contact_snippet": last.get("snippet", ""),
            "email_count": len(emails),
            "unanswered_count": unanswered_count,
            "recent_threads": subjects[:5],
        }

    except Exception as e:
        logger.debug("Gmail context failed: %s", e)
        return None


# ── Snowflake source ─────────────────────────────────────────────────────────


def _snowflake_context(account_id: str, user_id: str | None = None) -> dict | None:
    """Get email context from Snowflake dim_emails — all Ramp employee emails."""
    try:
        query = f"""
        SELECT
            e.sfdc_email_created_at::date AS email_date,
            e.email_direction AS direction,
            e.email_subject AS subject,
            SUBSTRING(COALESCE(e.email_body_clean, e.email_body), 1, 300) AS snippet,
            e.sfdc_email_owner_email AS sender_email,
            e.ramp_employee_team AS sender_team,
            e.external_contact_name,
            e.contact_persona,
            e.has_willing_to_meet,
            e.has_not_interested,
            e.has_painpoints,
            e.has_interested,
            e.has_out_of_office
        FROM analytics.marts.dim_emails e
        WHERE e.account_id = '{account_id}'
          AND e.sfdc_email_created_at >= DATEADD('day', -90, CURRENT_DATE)
        ORDER BY e.sfdc_email_created_at DESC
        LIMIT 20
        """
        df = run_query(query)
        if df.empty:
            return None

        last = df.iloc[0]
        last_dir = last.get("direction", "")
        last_sender = last.get("sender_email", "")
        # Determine if this sender is the current user
        from core.user_registry import get_user_email
        _user_email = (get_user_email(user_id) if user_id else "").lower()
        if not _user_email:
            from config import GMAIL_ADDRESS
            _user_email = (GMAIL_ADDRESS or "").lower()
        _user_name_part = _user_email.split("@")[0] if _user_email else ""
        is_self = (_user_name_part and _user_name_part in str(last_sender).lower()) if _user_name_part else False
        if "outbound" in str(last_dir).lower():
            last_direction = "You → them" if is_self else f"{last_sender} → them"
        else:
            last_direction = "They → you" if is_self else f"They → {last_sender}"

        # Count unanswered: if most recent is inbound
        unanswered = 0
        if "inbound" in str(last_dir).lower():
            unanswered = 1

        # Signals
        signals = []
        for _, row in df.iterrows():
            if row.get("has_willing_to_meet"):
                signals.append("willing_to_meet")
            if row.get("has_not_interested"):
                signals.append("not_interested")
            if row.get("has_painpoints"):
                signals.append("painpoints")
            if row.get("has_interested"):
                signals.append("interested")
            if row.get("has_out_of_office"):
                signals.append("out_of_office")

        # Ramp senders summary
        ramp_senders = []
        sender_counts: dict[str, int] = {}
        for _, row in df.iterrows():
            s = row.get("sender_email", "")
            if s:
                sender_counts[s] = sender_counts.get(s, 0) + 1
        for email, count in sorted(sender_counts.items(), key=lambda x: -x[1]):
            team = ""
            team_rows = df[df["sender_email"] == email]
            if not team_rows.empty:
                team = str(team_rows.iloc[0].get("sender_team", "") or "")
            ramp_senders.append({"email": email, "team": team, "count": count})

        # External contacts
        ext_contacts = []
        seen_contacts: set[str] = set()
        for _, row in df.iterrows():
            name = row.get("external_contact_name", "")
            if name and name not in seen_contacts:
                seen_contacts.add(name)
                ext_contacts.append({
                    "name": name,
                    "persona": str(row.get("contact_persona", "") or ""),
                })

        # Recent threads
        subjects = []
        seen: set[str] = set()
        for _, row in df.iterrows():
            subj = row.get("subject", "")
            if subj and subj not in seen:
                seen.add(subj)
                subjects.append({
                    "subject": subj,
                    "date": str(row.get("email_date", "")),
                    "direction": str(row.get("direction", "")),
                    "sender": str(row.get("sender_email", "")),
                })

        return {
            "source": "snowflake",
            "last_contact_date": str(last.get("email_date", "")),
            "last_contact_subject": last.get("subject", ""),
            "last_contact_direction": last_direction,
            "last_contact_snippet": (last.get("snippet", "") or "")[:300],
            "email_count": len(df),
            "unanswered_count": unanswered,
            "recent_threads": subjects[:5],
            "signals": list(set(signals)),
            "ramp_senders": ramp_senders,
            "external_contacts": ext_contacts[:5],
        }

    except Exception as e:
        logger.debug("Snowflake email context failed: %s", e)
        return None


# ── Public API ───────────────────────────────────────────────────────────────


def get_email_context(
    account_id: str, account_name: str = "", days: int = 30,
    user_id: str | None = None,
) -> dict:
    """Get email context for an account. Tries Gumstack Gmail first, falls back to Snowflake.

    Returns
    -------
    dict with keys:
        source: "gmail" | "snowflake" | "none"
        last_contact_date: str
        last_contact_subject: str
        last_contact_direction: "You → them" | "They → you"
        last_contact_snippet: str (first ~300 chars)
        email_count: int (emails found in window)
        unanswered_count: int (inbound emails without reply)
        recent_threads: list[dict] (subject, date, direction)
        signals: list[str] (willing_to_meet, not_interested, ooo) — Snowflake only
    """
    # Get contact emails for this account
    contact_emails = _get_contact_emails_for_account(account_id)
    domain = _get_domain_from_contacts(contact_emails)

    # Try Gumstack Gmail first (real-time)
    ctx = _gmail_context(contact_emails, domain, days=days, user_id=user_id)
    if ctx:
        return ctx

    # Fall back to Snowflake
    ctx = _snowflake_context(account_id, user_id=user_id)
    if ctx:
        return ctx

    return {"source": "none", "email_count": 0}


def format_email_context_line(ctx: dict, user_id: str | None = None) -> str:
    """Format email context as a single Slack mrkdwn line for inline use."""
    if ctx.get("source") == "none" or ctx.get("email_count", 0) == 0:
        return "No recent email activity"

    # Determine current user's email for filtering
    from core.user_registry import get_user_email
    _user_email = (get_user_email(user_id) if user_id else "").lower()
    if not _user_email:
        from config import GMAIL_ADDRESS
        _user_email = (GMAIL_ADDRESS or "").lower()
    _user_name_part = _user_email.split("@")[0] if _user_email else ""

    parts = []
    if ctx.get("last_contact_date"):
        parts.append(f"Last: {ctx['last_contact_direction']} ({ctx['last_contact_date']})")
    if ctx.get("last_contact_subject"):
        subj = ctx["last_contact_subject"]
        if len(subj) > 40:
            subj = subj[:37] + "..."
        parts.append(f'Re: "{subj}"')
    if ctx.get("unanswered_count", 0) > 0:
        parts.append("*Unanswered inbound*")
    # Show if other Ramp teams are active
    senders = ctx.get("ramp_senders", [])
    other_teams = [
        s["team"] for s in senders
        if s.get("team") and _user_name_part and _user_name_part not in s.get("email", "")
    ]
    if other_teams:
        parts.append(f"Also: {', '.join(dict.fromkeys(other_teams))}")

    return " · ".join(parts) if parts else f"{ctx['email_count']} emails in last 30d"


def format_email_context_block(ctx: dict, user_id: str | None = None) -> str:
    """Format email context as a multi-line Slack mrkdwn block for detailed views."""
    if ctx.get("source") == "none" or ctx.get("email_count", 0) == 0:
        return "_No recent email activity found._"

    # Determine current user's email for filtering
    from core.user_registry import get_user_email
    _user_email = (get_user_email(user_id) if user_id else "").lower()
    if not _user_email:
        from config import GMAIL_ADDRESS
        _user_email = (GMAIL_ADDRESS or "").lower()
    _user_name_part = _user_email.split("@")[0] if _user_email else ""

    lines = [f"*Email Activity* ({ctx['email_count']} emails, via {ctx['source']})"]

    if ctx.get("last_contact_date"):
        lines.append(
            f"  Last contact: {ctx['last_contact_direction']} on {ctx['last_contact_date']}"
        )
        if ctx.get("last_contact_subject"):
            lines.append(f'  Subject: "{ctx["last_contact_subject"]}"')

    if ctx.get("unanswered_count", 0) > 0:
        lines.append(f"  :warning: *{ctx['unanswered_count']} unanswered inbound email(s)*")

    # Ramp team activity
    senders = ctx.get("ramp_senders", [])
    if len(senders) > 1:
        other = [
            s for s in senders
            if not (_user_name_part and _user_name_part in s.get("email", ""))
        ]
        if other:
            sender_strs = [f"{s['email'].split('@')[0]} ({s['team'] or 'unknown'}, {s['count']})" for s in other[:4]]
            lines.append(f"  Other Ramp senders: {', '.join(sender_strs)}")

    # External contacts
    ext = ctx.get("external_contacts", [])
    if ext:
        contact_strs = [f"{c['name']}" + (f" ({c['persona']})" if c.get("persona") else "") for c in ext[:3]]
        lines.append(f"  Key contacts: {', '.join(contact_strs)}")

    # Signals from Snowflake
    if ctx.get("signals"):
        signal_map = {
            "willing_to_meet": "Willing to meet",
            "not_interested": "Not interested",
            "out_of_office": "Out of office",
            "painpoints": "Pain points mentioned",
            "interested": "Interested",
        }
        sig_strs = [signal_map.get(s, s) for s in ctx["signals"]]
        lines.append(f"  Signals: {', '.join(sig_strs)}")

    # Recent threads
    threads = ctx.get("recent_threads", [])
    if threads and len(threads) > 1:
        lines.append("  Recent threads:")
        for t in threads[:5]:
            dir_icon = "→" if "outbound" in str(t.get("direction", "")).lower() else "←"
            sender_note = ""
            if t.get("sender") and not (_user_name_part and _user_name_part in t.get("sender", "")):
                sender_note = f" [{t['sender'].split('@')[0]}]"
            lines.append(f'    {dir_icon} "{t["subject"]}" ({t["date"]}){sender_note}')

    return "\n".join(lines)
