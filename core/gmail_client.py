"""Gmail client — IMAP/SMTP via app password (no Gmail API needed).

Uses GMAIL_ADDRESS + GMAIL_APP_PASSWORD from .env. No Google Cloud project,
no OAuth scopes, no ADC. App passwords bypass 2FA and work with Ramp's
managed Google Workspace.

Generate an app password at: https://myaccount.google.com/apppasswords
Store in .env as GMAIL_APP_PASSWORD.
"""
from __future__ import annotations

import email
import imaplib
import logging
import re
import smtplib
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional

from config import GMAIL_ADDRESS, GMAIL_APP_PASSWORD

logger = logging.getLogger(__name__)

_IMAP_HOST = "imap.gmail.com"
_SMTP_HOST = "smtp.gmail.com"
_SMTP_PORT = 587

_api_available = None  # type: Optional[bool]


# ── Connection checks ─────────────────────────────────────────────────────────

def check_connection():
    """Test IMAP connectivity. Returns True if auth succeeds."""
    global _api_available
    try:
        conn = imaplib.IMAP4_SSL(_IMAP_HOST)
        conn.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        conn.logout()
        _api_available = True
        return True
    except Exception as e:
        logger.warning("Gmail IMAP check failed: %s", e)
        _api_available = False
        return False


def check_imap_connection():
    """Test IMAP connectivity. Returns (ok, message)."""
    global _api_available
    try:
        conn = imaplib.IMAP4_SSL(_IMAP_HOST)
        conn.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        conn.logout()
        _api_available = True
        return True, f"OK — {GMAIL_ADDRESS}"
    except Exception as e:
        _api_available = False
        return False, f"Gmail IMAP failed: {e}"


# ── Draft creation via IMAP APPEND ───────────────────────────────────────────

def create_draft(to, subject, html_body, cc=None):
    """Create a Gmail draft via IMAP APPEND to [Gmail]/Drafts.

    The draft appears in Gmail's Drafts folder, pre-addressed to the
    recipient — just open, review, and hit Send.

    Returns a draft ID string.
    """
    msg = MIMEMultipart("alternative")
    msg["To"] = to
    msg["Subject"] = subject
    if cc:
        msg["Cc"] = cc
    if GMAIL_ADDRESS:
        msg["From"] = GMAIL_ADDRESS
    msg.attach(MIMEText(html_body, "html"))

    conn = imaplib.IMAP4_SSL(_IMAP_HOST)
    conn.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)

    # Gmail uses "[Gmail]/Drafts" for the drafts folder
    draft_folder = "[Gmail]/Drafts"
    raw_msg = msg.as_bytes()

    result = conn.append(draft_folder, "\\Draft", None, raw_msg)
    conn.logout()

    draft_id = f"draft_{int(time.time())}"
    if result[0] == "OK":
        logger.info("Gmail draft created (IMAP) — to=%s subject=%s", to, subject)
    else:
        logger.warning("Gmail draft APPEND result: %s", result)

    return draft_id


def create_draft_imap(to, subject, html_body):
    """Alias for create_draft — kept for backward compatibility."""
    return create_draft(to, subject, html_body)


def create_draft_auto(to, subject, html_body):
    """Create a Gmail draft. Returns (draft_id, method) for compat."""
    draft_id = create_draft(to, subject, html_body)
    return draft_id, "imap"


def _get_or_create_label(conn, label_name: str) -> str | None:
    """Get a Gmail label ID by name, creating it if it doesn't exist.

    Uses IMAP LIST to check existence and Gmail's X-GM-LABELS to apply.
    Returns the label name (IMAP uses names, not IDs).
    """
    # Check if label already exists
    status, folders = conn.list()
    if status == "OK":
        for folder_info in folders:
            if isinstance(folder_info, bytes):
                decoded = folder_info.decode("utf-8", errors="replace")
                if f'"{label_name}"' in decoded or label_name in decoded:
                    return label_name

    # Create label by selecting it (Gmail auto-creates on first reference)
    try:
        conn.create(f'"{label_name}"')
        logger.info("Created Gmail label: %s", label_name)
    except Exception:
        # Label might already exist — that's fine
        pass

    return label_name


def create_draft_with_label(to, subject, html_body, cc=None, label="Claude Drafts/Post Meeting"):
    """Create a Gmail draft and apply a label.

    Creates the draft in [Gmail]/Drafts, then applies the specified label
    using Gmail's IMAP X-GM-LABELS extension.

    Returns the draft ID string.
    """
    msg = MIMEMultipart("alternative")
    msg["To"] = to
    msg["Subject"] = subject
    if cc:
        msg["Cc"] = cc
    if GMAIL_ADDRESS:
        msg["From"] = GMAIL_ADDRESS

    # Add a custom header so we can find this draft later
    msg["X-Gary-Bot-Draft"] = "post-meeting"
    msg.attach(MIMEText(html_body, "html"))

    conn = imaplib.IMAP4_SSL(_IMAP_HOST)
    conn.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)

    # Ensure label exists
    _get_or_create_label(conn, label)

    # Create draft
    draft_folder = "[Gmail]/Drafts"
    raw_msg = msg.as_bytes()
    result = conn.append(draft_folder, "\\Draft", None, raw_msg)

    draft_id = f"draft_{int(time.time())}"
    if result[0] == "OK":
        logger.info("Gmail draft created — to=%s subject=%s", to, subject)

        # Apply label to the draft we just created
        try:
            conn.select("[Gmail]/Drafts")
            # Search for the draft by subject (just created, should be the latest)
            safe_subject = subject.replace('"', '\\"')
            status, data = conn.search(None, f'SUBJECT "{safe_subject}"')
            if status == "OK" and data[0]:
                msg_ids = data[0].split()
                latest_id = msg_ids[-1]  # Most recent match
                # Use Gmail's X-GM-LABELS extension to apply label
                conn.store(latest_id, "+X-GM-LABELS", f'("{label}")')
                logger.info("Applied label '%s' to draft", label)
        except Exception as e:
            logger.warning("Failed to apply label '%s' to draft: %s", label, e)
    else:
        logger.warning("Gmail draft APPEND result: %s", result)

    conn.logout()
    return draft_id


# ── Email search via IMAP ────────────────────────────────────────────────────

def _parse_email_address(addr_str):
    """Extract bare email from 'Name <email@example.com>' format."""
    if not addr_str:
        return ""
    match = re.search(r"<([^>]+)>", addr_str)
    return match.group(1).lower() if match else addr_str.strip().lower()


def _imap_search(query_criteria, max_results=10):
    """Run an IMAP search and return parsed message metadata."""
    global _api_available
    my_email = (GMAIL_ADDRESS or "").lower()

    try:
        conn = imaplib.IMAP4_SSL(_IMAP_HOST)
        conn.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        conn.select("[Gmail]/All Mail", readonly=True)

        status, data = conn.search(None, *query_criteria)
        if status != "OK" or not data[0]:
            conn.logout()
            return []

        msg_ids = data[0].split()
        # Most recent first
        msg_ids = list(reversed(msg_ids[-max_results:]))

        results = []
        for mid in msg_ids:
            status, msg_data = conn.fetch(mid, "(RFC822.HEADER)")
            if status != "OK" or not msg_data[0]:
                continue
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            from_addr = _parse_email_address(msg.get("From", ""))
            direction = "outbound" if from_addr == my_email else "inbound"

            results.append({
                "date": msg.get("Date", ""),
                "subject": msg.get("Subject", ""),
                "from_addr": from_addr,
                "to_addr": msg.get("To", ""),
                "direction": direction,
                "snippet": "",  # IMAP doesn't have snippets
            })

        conn.logout()
        _api_available = True
        return results

    except Exception as e:
        logger.warning("Gmail IMAP search failed: %s", e)
        return []


def search_emails(
    contact_emails=None,
    domain=None,
    days=30,
    max_results=10,
    folder="",
):
    """Search Gmail for recent emails matching contacts or domain via IMAP."""
    global _api_available
    if _api_available is False:
        return []

    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        return []

    since_date = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")

    if contact_emails:
        # IMAP OR search for multiple addresses
        # Search each address and merge results
        all_results = []
        seen_subjects = set()
        for addr in contact_emails[:5]:
            criteria = [f'(OR (FROM "{addr}") (TO "{addr}"))' , f'SINCE {since_date}']
            results = _imap_search(criteria, max_results=max_results)
            for r in results:
                key = f"{r['subject']}_{r['date']}"
                if key not in seen_subjects:
                    seen_subjects.add(key)
                    all_results.append(r)
        # Sort by date desc (most recent first)
        all_results.sort(key=lambda x: x["date"], reverse=True)
        return all_results[:max_results]
    elif domain:
        criteria = [f'(OR (FROM "@{domain}") (TO "@{domain}"))', f'SINCE {since_date}']
        return _imap_search(criteria, max_results=max_results)
    else:
        return []


def search_drafts(query, max_results=5):
    """Search Gmail drafts via IMAP."""
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        return []
    try:
        conn = imaplib.IMAP4_SSL(_IMAP_HOST)
        conn.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        conn.select("[Gmail]/Drafts", readonly=True)

        status, data = conn.search(None, f'SUBJECT "{query}"')
        if status != "OK" or not data[0]:
            conn.logout()
            return []

        msg_ids = data[0].split()[-max_results:]
        results = []
        for mid in msg_ids:
            results.append({"id": mid.decode()})
        conn.logout()
        return results
    except Exception as e:
        logger.warning("Gmail draft search failed: %s", e)
        return []


def get_last_contact(
    contact_emails=None,
    domain=None,
    days=90,
):
    """Get the most recent email exchange with a contact/domain."""
    emails = search_emails(
        contact_emails=contact_emails, domain=domain,
        days=days, max_results=1,
    )
    return emails[0] if emails else None


# ── Looker ZIP attachment download ────────────────────────────────────────────

def fetch_looker_zip(subject_contains: str, max_age_hours: int = 24) -> Optional[str]:
    """Download and extract a Looker ZIP attachment from Gmail.

    Searches [Gmail]/All Mail for emails FROM noreply@lookermail.com with the
    given subject substring, received SINCE today. Downloads the first ZIP
    attachment found, saves to /tmp/gary_bot_looker/, extracts, and returns
    the extraction directory path. Returns None if not found.
    """
    import os
    import zipfile
    import re as _re

    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        logger.warning("fetch_looker_zip: Gmail credentials not configured")
        return None

    today_str = datetime.now().strftime("%d-%b-%Y")
    # Build a filesystem-safe slug from the subject
    slug = _re.sub(r"[^a-z0-9]+", "_", subject_contains.lower()).strip("_")
    date_tag = datetime.now().strftime("%Y%m%d")
    base_dir = "/tmp/gary_bot_looker"
    zip_path = os.path.join(base_dir, f"{slug}_{date_tag}.zip")
    extract_dir = os.path.join(base_dir, f"{slug}_{date_tag}")

    try:
        conn = imaplib.IMAP4_SSL(_IMAP_HOST)
        conn.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        conn.select("[Gmail]/All Mail", readonly=True)

        search_criteria = (
            f'(FROM "noreply@lookermail.com" '
            f'SUBJECT "{subject_contains}" '
            f'SINCE {today_str})'
        )
        status, data = conn.search(None, search_criteria)
        if status != "OK" or not data[0]:
            logger.info("fetch_looker_zip: no emails found for '%s'", subject_contains)
            conn.logout()
            return None

        msg_ids = data[0].split()
        # Most recent last — take the latest
        target_id = msg_ids[-1]

        status, msg_data = conn.fetch(target_id, "(RFC822)")
        conn.logout()

        if status != "OK" or not msg_data[0]:
            logger.warning("fetch_looker_zip: failed to fetch message body")
            return None

        raw = msg_data[0][1]
        msg = email.message_from_bytes(raw)

        # Walk MIME parts looking for a ZIP attachment
        zip_payload = None
        for part in msg.walk():
            content_type = part.get_content_type() or ""
            filename = part.get_filename() or ""
            if content_type == "application/zip" or filename.lower().endswith(".zip"):
                zip_payload = part.get_payload(decode=True)
                break

        if not zip_payload:
            logger.info("fetch_looker_zip: no ZIP attachment in email for '%s'", subject_contains)
            return None

        # Save and extract
        os.makedirs(base_dir, exist_ok=True)
        with open(zip_path, "wb") as f:
            f.write(zip_payload)

        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

        logger.info(
            "fetch_looker_zip: extracted '%s' → %s (%d files)",
            subject_contains, extract_dir,
            len(os.listdir(extract_dir)),
        )
        return extract_dir

    except Exception as e:
        logger.warning("fetch_looker_zip failed for '%s': %s", subject_contains, e)
        return None


def get_unanswered_inbound(
    contact_emails=None,
    domain=None,
    days=14,
):
    """Find inbound emails where no outbound reply followed."""
    emails = search_emails(
        contact_emails=contact_emails, domain=domain,
        days=days, max_results=20,
    )
    if not emails:
        return []

    threads = {}
    for em in emails:
        clean_subject = re.sub(r"^(Re|Fwd|FW|RE):\s*", "", em["subject"]).strip().lower()
        if clean_subject not in threads:
            threads[clean_subject] = []
        threads[clean_subject].append(em)

    unanswered = []
    for _subj, thread_emails in threads.items():
        if thread_emails and thread_emails[0]["direction"] == "inbound":
            unanswered.append(thread_emails[0])

    return unanswered
