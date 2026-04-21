"""Single source of truth for outbound email recipients.

Every drafter that sends prospecting / follow-up / post-meeting / play emails
should call `resolve_outbound_recipients()` instead of picking its own `to`
and `cc`. This enforces Greg's rule:

    People we've met with + admins + owners + main POCs — all CC'd on ONE
    email, not separate.

Uses all available engagement signals:
  1. SFDC contacts on the account (via utils.account_resolver.fetch_contact_emails)
  2. Gong call speakers in the last 90 days (from dim_gong_transcript_paragraph
     joined to dim_sfdc_gong_call)
  3. Email thread partners in the last 90 days (from dim_email_threads —
     anyone on an inbound thread is an active correspondent)
  4. Attendees on the CURRENT meeting (for post-meeting drafts) — highest
     weight since they were literally on the call

Scoring is delegated to utils.contact_scoring.select_recipients — this module
is strictly about assembling the signal set.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

EMAIL_LOOKBACK_DAYS = 90
GONG_LOOKBACK_DAYS = 90
DEFAULT_MAX_CC = 4


def _norm(email: str) -> str:
    return (email or "").strip().lower()


def _fetch_gong_participants(account_id: str, lookback_days: int) -> set[str]:
    """Return lowercase emails of non-Ramp speakers on any Gong call for the
    account in the last N days.
    """
    if not account_id:
        return set()
    try:
        from core.snowflake_client import run_query
        df = run_query(f"""
            SELECT DISTINCT LOWER(p.speaker_email) AS email
            FROM analytics.marts.dim_gong_transcript_paragraph p
            JOIN analytics.marts.dim_sfdc_gong_call c
                ON c.gong_call_id = p.call_id
            WHERE c.sfdc_primary_account_id = '{account_id}'
              AND p.speaker_email IS NOT NULL
              AND p.speaker_email NOT ILIKE '%@ramp.com'
              AND c.created_date >= DATEADD('day', -{lookback_days}, CURRENT_DATE)
        """)
        df.columns = [c.lower() for c in df.columns]
        return {_norm(row["email"]) for _, row in df.iterrows() if row.get("email")}
    except Exception as e:
        logger.debug("Gong participants fetch failed for %s: %s", account_id, e)
        return set()


def _fetch_email_correspondents(account_id: str, lookback_days: int) -> set[str]:
    """Return lowercase emails of customer-side participants on any email
    thread in the last N days. Uses the thread's external participants —
    NOT just the thread_owner_full_name which is Ramp-side.
    """
    if not account_id:
        return set()
    try:
        from core.snowflake_client import run_query
        # dim_email_threads has email_thread_customer_account_ids + per-thread
        # participant arrays. Pull external addresses that appear on threads
        # for this account.
        df = run_query(f"""
            SELECT DISTINCT LOWER(external_email) AS email
            FROM analytics.marts.dim_email_threads et,
                 LATERAL FLATTEN(INPUT => et.email_thread_external_participant_emails) AS ext
            JOIN LATERAL (SELECT ext.value::STRING AS external_email) e
            WHERE et.sfdc_account_id = '{account_id}'
              AND et.last_email_created_at >= DATEADD('day', -{lookback_days}, CURRENT_DATE)
              AND ext.value IS NOT NULL
        """)
        df.columns = [c.lower() for c in df.columns]
        return {_norm(row["email"]) for _, row in df.iterrows() if row.get("email")}
    except Exception:
        # Schema for external participant arrays may differ — gracefully fall back
        # to the account-level thread owner email if it's a customer.
        try:
            from core.snowflake_client import run_query
            df = run_query(f"""
                SELECT DISTINCT LOWER(thread_owner_email) AS email
                FROM analytics.marts.dim_email_threads
                WHERE sfdc_account_id = '{account_id}'
                  AND last_email_created_at >= DATEADD('day', -{lookback_days}, CURRENT_DATE)
                  AND last_email_direction = 'Inbound'
                  AND thread_owner_email IS NOT NULL
                  AND thread_owner_email NOT ILIKE '%@ramp.com'
            """)
            df.columns = [c.lower() for c in df.columns]
            return {_norm(row["email"]) for _, row in df.iterrows() if row.get("email")}
        except Exception as e:
            logger.debug("Email correspondents fetch failed for %s: %s", account_id, e)
            return set()


def resolve_outbound_recipients(
    account_id: str,
    user_id: str | None = None,
    current_meeting_attendees: list[str] | None = None,
    current_call_participants: list[str] | None = None,
    max_cc: int = DEFAULT_MAX_CC,
) -> tuple[Optional[dict], list[dict], dict]:
    """Resolve primary + CC list for an outbound / follow-up email.

    Parameters
    ----------
    account_id : str
        SFDC 18-char account ID.
    user_id : str, optional
        Slack user ID of the AM drafting (for per-user contact fetch scoping).
    current_meeting_attendees : list[str], optional
        External attendee emails on the CURRENT call (for post-meeting drafts).
        These are weighted highest because they're the most-recent conversation.
    current_call_participants : list[str], optional
        Alias for current_meeting_attendees — either field is honored.
    max_cc : int
        Max CCs to include (default 4).

    Returns
    -------
    (primary, cc_list, debug_signals)
        primary : {"name", "email", "title", "why"}  or None
        cc_list : list of same shape
        debug_signals : {"n_sfdc_contacts", "n_gong_participants",
                         "n_email_correspondents", "n_current_attendees"}
    """
    from utils.account_resolver import fetch_contact_emails
    from utils.contact_scoring import select_recipients

    # 1. SFDC contacts
    try:
        by_acct = fetch_contact_emails(None, [account_id]) if account_id else {}
        sfdc_contacts = by_acct.get(account_id, []) if account_id else []
    except Exception as e:
        logger.debug("SFDC contact fetch failed for %s: %s", account_id, e)
        sfdc_contacts = []

    # 2. Gong call speakers
    gong_participants = _fetch_gong_participants(account_id, GONG_LOOKBACK_DAYS)

    # 3. Email correspondents
    email_correspondents = _fetch_email_correspondents(account_id, EMAIL_LOOKBACK_DAYS)

    # 4. Current-meeting attendees (highest weight in the scorer — treated as
    # Gong participants so they get the +50 bonus)
    current = set()
    for lst in (current_meeting_attendees or [], current_call_participants or []):
        for em in lst:
            if em:
                current.add(_norm(em))
    combined_gong = gong_participants | current

    # If we have current-meeting attendees that aren't in the SFDC contact list,
    # synthesize minimal contact entries for them so they can be scored/picked.
    # This handles the "first call with someone new" case.
    known_emails = {_norm(c.get("email", "")) for c in sfdc_contacts}
    for em in current:
        if em and em not in known_emails:
            sfdc_contacts.append({
                "name": em.split("@")[0].replace(".", " ").title(),
                "email": em,
                "title": "(from recent meeting)",
            })
            known_emails.add(em)

    # Delegate scoring to the existing select_recipients helper
    primary, cc = select_recipients(
        sfdc_contacts,
        gong_participants=combined_gong,
        email_correspondents=email_correspondents,
        max_cc=max_cc,
    )

    # Signal-strength filter for CCs: drop contacts that lack BOTH a
    # recognizable title AND any engagement signal. Greg's rule: owners,
    # admins, and people who've actually engaged (Gong / email) — not
    # random SFDC names with no context.
    def _has_strong_title(c: dict) -> bool:
        t = (c.get("title") or "").strip().lower()
        if not t or t == "(from recent meeting)":
            return False
        admin_keywords = (
            "cfo", "controller", "vp finance", "head of finance", "finance",
            "accounting", "accountant", "director", "ceo", "coo", "cto",
            "president", "founder", "owner", "operations", "ops ", "ap ",
            "accounts payable", "procurement", "treasurer", "admin", "executive assistant",
            "ea to", "bookkeeper", "payroll", "people ops",
        )
        return any(k in t for k in admin_keywords)

    def _has_signal(c: dict) -> bool:
        em = _norm(c.get("email", ""))
        return (em in current or em in gong_participants
                or em in email_correspondents or _has_strong_title(c))

    # Keep CCs that have some signal OR, if that leaves 0, keep the top CC
    # anyway so we don't accidentally send empty CC drafts.
    strong_cc = [c for c in cc if _has_signal(c)]
    if len(strong_cc) == 0 and cc:
        strong_cc = cc[:1]  # keep one fallback
    cc = strong_cc[:max_cc]

    # Annotate each selected contact with a human-readable "why" for the DM
    def _why(c: dict) -> str:
        if not c:
            return ""
        em = _norm(c.get("email", ""))
        reasons = []
        if em in current:
            reasons.append("on the current call")
        elif em in gong_participants:
            reasons.append("recent Gong call participant")
        if em in email_correspondents:
            reasons.append("recent email correspondent")
        title = (c.get("title") or "").strip()
        if title and title != "(from recent meeting)":
            reasons.append(f"SFDC: {title}")
        elif not reasons:
            reasons.append("SFDC contact, no title on file, no recent engagement")
        return " · ".join(reasons)

    if primary:
        primary["why"] = _why(primary)
    cc_annotated = [dict(c, why=_why(c)) for c in cc]

    debug = {
        "n_sfdc_contacts": len(sfdc_contacts),
        "n_gong_participants": len(gong_participants),
        "n_email_correspondents": len(email_correspondents),
        "n_current_attendees": len(current),
    }
    return primary, cc_annotated, debug
