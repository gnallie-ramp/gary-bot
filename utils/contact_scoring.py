"""Shared contact scoring and CC selection for email drafts.

Used by the smart drafter (interactive.py) and stale opp drafter
(stale_opp_drafter.py) to consistently select TO and CC recipients.

Scoring uses Ramp platform user_role as the primary signal, then engagement:
- BUSINESS_OWNER role: +60 (always the most relevant contact)
- BUSINESS_ADMIN role: +45 (key stakeholder)
- Gong call participant: +50 (actively talking to Ramp)
- Email correspondent: +30 (active communication)
- BUSINESS_BOOKKEEPER role: +20 (relevant for finance/AP)
- Role + engagement combo: +25 (right person AND engaged)
- Free-text title match: +15/+10 (fallback when no Ramp role data)
- Functional email penalty: -50 (invoices@, admin@, ap@, etc.)
"""
from __future__ import annotations

import re
from typing import Optional

_OWNER_TITLES = re.compile(
    r'\b(owner|ceo|president|founder|principal|cfo|vp.?finance|'
    r'chief.?executive|chief.?financial|chief.?operating|'
    r'controller|director.?of.?finance|managing.?partner|'
    r'partner|dentist|doctor|physician|managing.?director)\b', re.IGNORECASE
)
_ADMIN_TITLES = re.compile(
    r'\b(admin|administrator|ap.?manager|accounting.?manager|'
    r'office.?manager|bookkeeper|accounts.?payable|billing.?manager|'
    r'operations.?manager|finance.?manager|staff.?accountant|'
    r'practice.?manager)\b', re.IGNORECASE
)

# Functional mailboxes that shouldn't be primary outreach targets
_FUNCTIONAL_EMAILS = re.compile(
    r'^(invoices?|billing|admin|ap|info|support|accounting|'
    r'payments?|finance|office|reception|noreply|no-reply)@', re.IGNORECASE
)


def _is_hash_like(name: str) -> bool:
    """Return True if the contact name looks like a hash or placeholder."""
    if not name:
        return True
    if len(name) > 30 and not any(c == " " for c in name):
        return True
    return False


def score_contact(
    contact: dict,
    gong_participants: set[str] | None = None,
    email_correspondents: set[str] | None = None,
) -> int:
    """Score a single contact for ranking. Higher = better candidate for TO.

    Returns -1 for invalid contacts (no email, hash-like name).
    """
    email = (contact.get("email") or "").strip().lower()
    title = contact.get("title") or ""
    name = contact.get("name") or ""

    if not email or _is_hash_like(name):
        return -1

    # Clean placeholder titles
    if title and title.startswith("[["):
        title = ""

    user_role = (contact.get("user_role") or "").upper()

    score = 0
    has_engagement = False
    has_role = False

    # Functional email penalty — deprioritize mailboxes like invoices@, admin@, ap@
    if _FUNCTIONAL_EMAILS.match(email):
        score -= 50

    # ── Ramp platform role (primary signal — most reliable) ──
    if user_role == "BUSINESS_OWNER":
        score += 60
        has_role = True
    elif user_role == "BUSINESS_ADMIN":
        score += 45
        has_role = True
    elif user_role == "BUSINESS_BOOKKEEPER":
        score += 20
        has_role = True

    # ── Engagement signals (second priority — active Ramp interaction) ──
    if gong_participants and email in gong_participants:
        score += 50
        has_engagement = True
    if email_correspondents and email in email_correspondents:
        score += 30
        has_engagement = True

    # ── Free-text title fallback (only when no Ramp role data) ──
    if not has_role:
        if _OWNER_TITLES.search(title):
            score += 15
        elif _ADMIN_TITLES.search(title):
            score += 10

    # ── Combo bonus: right role AND actively engaged with Ramp ──
    if has_role and has_engagement:
        score += 25

    return score


def select_recipients(
    contacts: list[dict],
    gong_participants: set[str] | None = None,
    email_correspondents: set[str] | None = None,
    max_cc: int = 3,
) -> tuple[Optional[dict], list[dict]]:
    """Select primary (TO) and CC contacts from a list of SFDC contacts.

    Returns (primary_contact, cc_contacts). primary_contact is None if no
    valid contacts found.
    """
    scored = [
        (c, score_contact(c, gong_participants, email_correspondents))
        for c in contacts
    ]
    scored = [(c, s) for c, s in scored if s >= 0]
    scored.sort(key=lambda x: x[1], reverse=True)

    if not scored:
        return None, []

    primary = scored[0][0]
    primary_email = (primary.get("email") or "").lower()

    # CC: other contacts, domain-matched to prevent wrong-company CCs
    cc = []
    seen = {primary_email}
    to_domain = primary_email.split("@")[-1] if "@" in primary_email else ""

    for c, s in scored[1:]:
        em = (c.get("email") or "").lower()
        cc_domain = em.split("@")[-1] if "@" in em else ""
        if em in seen:
            continue
        if to_domain and cc_domain and cc_domain != to_domain:
            continue
        cc.append(c)
        seen.add(em)
        if len(cc) >= max_cc:
            break

    return primary, cc
