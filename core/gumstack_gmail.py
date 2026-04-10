"""Gumstack Gmail MCP client — create drafts, read emails, manage labels via Gumloop.

Calls the Gumstack Gmail MCP server directly over HTTP using OAuth tokens
stored by mcp-remote. No Glass session needed — works headless from the bot.

Supports per-user tokens via the user registry. When user_id is provided,
loads tokens from per-user location; otherwise falls back to the default
mcp-remote token cache.

Token location (default): ~/.mcp-auth/mcp-remote-0.1.12/<hash>_tokens.json
Hash = MD5 of "https://mcp.gumloop.com/gmail/mcp"
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

_MCP_URL = "https://mcp.gumloop.com/gmail/mcp"
_OAUTH_TOKEN_URL = "https://api.gumloop.com/oauth/token"
_SERVER_HASH = hashlib.md5(_MCP_URL.encode()).hexdigest()
_TOKEN_DIR = Path.home() / ".mcp-auth" / "mcp-remote-0.1.12"
_TOKEN_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_tokens.json"
_CLIENT_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_client_info.json"
_GLASS_CREDS = Path.home() / ".project-glass" / "credentials.json"
_GLASS_GMAIL_KEY = "gumstack-gmail|"  # prefix in Glass mcpOAuth keys

# Session cache: keyed by user_id (or "default") -> {"session": requests.Session, "initialized_at": float, "token": str}
_session_cache: Dict[str, dict] = {}
_SESSION_TTL = 300  # 5 minutes

# Per-user label cache: keyed by user_id (or "default") -> {label_name: label_id}
_label_cache: Dict[str, Dict[str, str]] = {}

# Label names used by the bot
_LABEL_NAMES = [
    "Claude Drafts/Post Meeting",
    "Claude Drafts/ACH to Card",
    "Claude Drafts/Procurement Trials",
    "Claude Drafts/Large Declines",
    "Claude Drafts/PCLIP Activation",
    "Claude Drafts/Prospecting",
    "Claude Drafts/Fundraise",
    "Claude Drafts/Automatic Card",
    "Claude Drafts/RCLIP",
    "Claude Drafts/AM Escalation",
]
DEFAULT_LABEL_NAME = "Claude Drafts/Prospecting"


def _alert_gmail_auth_failure(user_id: Optional[str], error: str = "") -> None:
    """Send an inline DM when Gmail auth fails (rate-limited)."""
    try:
        from utils.auth_health import alert_auth_failure
        from config import GREG_SLACK_ID
        alert_auth_failure("gmail", user_id or GREG_SLACK_ID, error)
    except Exception:
        pass  # Don't let alert failures break the caller


def _get_token_paths(user_id: Optional[str] = None) -> Tuple[str, str]:
    """Resolve token file and client info file paths for the given user.

    When user_id is provided, tries per-user tokens first. Falls back to
    default mcp-remote paths.
    """
    if user_id:
        try:
            from core.user_registry import get_user_gmail_tokens
            paths = get_user_gmail_tokens(user_id)
            if paths:
                return paths
        except Exception as e:
            logger.debug("Per-user token lookup failed for %s: %s", user_id, e)

    # Default paths
    return str(_TOKEN_FILE), str(_CLIENT_FILE)


def _load_glass_token() -> Optional[str]:
    """Try to load a fresh Gmail access token from Glass credentials.

    Glass manages its own OAuth refresh cycle, so its token is usually fresh.
    Returns the access token string, or None if unavailable/expired.
    """
    try:
        if not _GLASS_CREDS.exists():
            return None
        with open(_GLASS_CREDS) as f:
            creds = json.load(f)
        mcp_oauth = creds.get("mcpOAuth", {})
        for key, entry in mcp_oauth.items():
            if key.startswith(_GLASS_GMAIL_KEY):
                expires_at = entry.get("expiresAt", 0)
                # expiresAt is in epoch milliseconds
                if expires_at > time.time() * 1000:
                    return entry["accessToken"]
                else:
                    logger.debug("Glass Gmail token expired (expiresAt=%s)", expires_at)
                    return None
        return None
    except Exception as e:
        logger.debug("Could not load Glass Gmail token: %s", e)
        return None


def _load_access_token(user_id: Optional[str] = None) -> str:
    """Load the current access token, preferring Glass credentials.

    Priority: Glass credentials (always fresh) > per-user tokens > default mcp-remote.
    If the mcp-remote token is close to expiry (< 5 min), proactively refresh it.
    """
    # Try Glass first (managed refresh, usually fresh)
    glass_token = _load_glass_token()
    if glass_token:
        return glass_token

    # Fall back to per-user or default token files
    token_path, _ = _get_token_paths(user_id)
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"Gmail MCP token file not found: {token_path}")
    with open(token_path) as f:
        tokens = json.load(f)

    # Proactive refresh: if token is near expiry, refresh now instead of failing later
    expires_in = tokens.get("expires_in", 3600)
    saved_at = os.path.getmtime(token_path)
    age = time.time() - saved_at
    remaining = expires_in - age
    if remaining < 300:  # < 5 minutes left
        logger.info("Gmail token near expiry (%.0fs left) — refreshing proactively", remaining)
        try:
            return _refresh_token(user_id=user_id)
        except Exception as e:
            logger.warning("Proactive Gmail token refresh failed: %s — using existing", e)

    return tokens["access_token"]


def _refresh_token(user_id: Optional[str] = None) -> str:
    """Refresh the access token using the stored refresh token via Gumloop OAuth."""
    token_path, client_path = _get_token_paths(user_id)
    if not os.path.exists(token_path) or not os.path.exists(client_path):
        raise FileNotFoundError("Token or client info file not found for Gmail MCP")

    with open(token_path) as f:
        tokens = json.load(f)
    with open(client_path) as f:
        client_info = json.load(f)

    refresh_tok = tokens.get("refresh_token")
    if not refresh_tok:
        raise ValueError("No refresh_token in token file")

    # Standard OAuth2 refresh_token grant to Gumloop's token endpoint
    resp = requests.post(
        _OAUTH_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_tok,
            "client_id": client_info.get("client_id", ""),
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    if resp.status_code != 200:
        logger.warning("Gmail token refresh failed (HTTP %d): %s", resp.status_code, resp.text[:200])
        return tokens["access_token"]

    new_tokens = resp.json()
    # Merge new tokens into existing (preserve refresh_token if not rotated)
    tokens["access_token"] = new_tokens["access_token"]
    if "refresh_token" in new_tokens:
        tokens["refresh_token"] = new_tokens["refresh_token"]
    if "expires_in" in new_tokens:
        tokens["expires_in"] = new_tokens["expires_in"]

    # Persist to disk so the bot picks up fresh tokens on next load
    with open(token_path, "w") as f:
        json.dump(tokens, f, indent=2)

    logger.info("Gmail token refreshed and saved for %s", user_id or "default")
    return tokens["access_token"]


def _parse_response(resp: requests.Response) -> dict:
    """Parse an MCP response that may be JSON or SSE (text/event-stream).

    Gumstack sometimes returns SSE format instead of plain JSON.
    In SSE, the JSON payload is on lines prefixed with 'data: '.
    """
    try:
        return resp.json()
    except (json.JSONDecodeError, ValueError):
        pass

    # Fall back to SSE parsing
    for line in resp.text.splitlines():
        if line.startswith("data: "):
            try:
                return json.loads(line[6:])
            except (json.JSONDecodeError, ValueError):
                continue

    logger.error("Gumstack Gmail: could not parse response (%d bytes): %s",
                 len(resp.text), resp.text[:200])
    return {}


def _get_session(user_id: Optional[str] = None, _retried: bool = False) -> Tuple[requests.Session, dict]:
    """Return a cached, initialized MCP session for the given user.

    If no valid cached session exists (missing, expired, or token changed),
    creates a new requests.Session, runs the MCP initialize handshake,
    and caches it.

    Returns (session, headers_dict) ready for tool calls.
    On 401 during init, clears cache and retries once with token refresh.
    """
    cache_key = user_id or "default"
    token = _load_access_token(user_id=user_id)
    now = time.monotonic()

    # Check for a valid cached session
    cached = _session_cache.get(cache_key)
    if cached is not None:
        age = now - cached["initialized_at"]
        if age < _SESSION_TTL and cached["token"] == token:
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            }
            return cached["session"], headers

    # Cache miss or stale — create a new session and initialize
    session = requests.Session()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    init_resp = session.post(
        _MCP_URL,
        json={
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "gary-bot", "version": "1.0.0"},
            },
        },
        headers=headers,
        timeout=15,
    )

    # Handle 401 on init — clear cache and retry with token refresh
    if init_resp.status_code == 401 and not _retried:
        logger.warning("Gumstack Gmail 401 on init — attempting token refresh...")
        _session_cache.pop(cache_key, None)
        try:
            new_token = _refresh_token(user_id=user_id)
            if new_token:
                return _get_session(user_id=user_id, _retried=True)
        except Exception as e:
            logger.warning("Token refresh failed: %s", e)
    if init_resp.status_code == 401:
        _alert_gmail_auth_failure(user_id, "401 on init — token refresh failed")
    init_resp.raise_for_status()

    # Cache the initialized session
    _session_cache[cache_key] = {
        "session": session,
        "initialized_at": now,
        "token": token,
    }
    return session, headers


def _mcp_call(
    method: str,
    params: dict,
    request_id: int = 1,
    _retried: bool = False,
    user_id: Optional[str] = None,
) -> dict:
    """Make a single MCP JSON-RPC call to the Gumstack Gmail server.

    Uses a cached initialized session to avoid redundant initialize round-trips.
    On 401, clears the session cache and retries once with token refresh.
    On empty/unparseable response, retries once with a fresh session (likely
    transient Gumstack issue — they sometimes return SSE or empty bodies).
    """
    cache_key = user_id or "default"
    session, headers = _get_session(user_id=user_id)

    # Make the tool call (single round-trip — init was cached)
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params,
    }
    resp = session.post(_MCP_URL, json=payload, headers=headers, timeout=30)

    # Auto-refresh on 401 for tool call (one retry only)
    if resp.status_code == 401 and not _retried:
        logger.warning("Gumstack Gmail 401 on tool call — refreshing token and retrying...")
        _session_cache.pop(cache_key, None)
        try:
            _refresh_token(user_id=user_id)
            return _mcp_call(method, params, request_id, _retried=True, user_id=user_id)
        except Exception as e:
            logger.warning("Token refresh failed: %s", e)
    if resp.status_code == 401:
        _alert_gmail_auth_failure(user_id, "401 on tool call — token refresh failed")
    resp.raise_for_status()

    parsed = _parse_response(resp)

    # Retry once on empty/unparseable response (transient Gumstack SSE issue)
    if not parsed and not _retried:
        logger.debug("Gumstack Gmail returned empty response — retrying with fresh session...")
        _session_cache.pop(cache_key, None)
        time.sleep(0.5)
        return _mcp_call(method, params, request_id, _retried=True, user_id=user_id)

    return parsed


# ── Label resolution ─────────────────────────────────────────────────────────


def _resolve_label_id(label_name: str, user_id: Optional[str] = None) -> str:
    """Resolve a label name to its Gmail label ID, creating it if needed.

    Uses a per-user (or default) cache to avoid repeated list_labels calls.
    """
    cache_key = user_id or "default"

    # Check cache first
    if cache_key in _label_cache and label_name in _label_cache[cache_key]:
        return _label_cache[cache_key][label_name]

    # Cache miss — fetch all labels for this user
    try:
        resp = _mcp_call(
            "tools/call",
            {"name": "list_labels", "arguments": {}},
            user_id=user_id,
        )
        content = resp.get("result", {}).get("content", [])
        if content:
            labels_data = json.loads(content[0].get("text", "[]"))
            # Build name -> id mapping
            name_to_id: Dict[str, str] = {}
            if isinstance(labels_data, list):
                for lbl in labels_data:
                    name_to_id[lbl.get("name", "")] = lbl.get("id", "")
            elif isinstance(labels_data, dict) and "labels" in labels_data:
                for lbl in labels_data["labels"]:
                    name_to_id[lbl.get("name", "")] = lbl.get("id", "")
            _label_cache[cache_key] = name_to_id

            if label_name in name_to_id:
                return name_to_id[label_name]
    except Exception as e:
        logger.warning("Failed to list labels for %s: %s", cache_key, e)

    # Label doesn't exist — create it
    try:
        create_resp = _mcp_call(
            "tools/call",
            {"name": "create_label", "arguments": {"name": label_name}},
            user_id=user_id,
        )
        create_content = create_resp.get("result", {}).get("content", [])
        if create_content:
            new_label = json.loads(create_content[0].get("text", "{}"))
            new_id = new_label.get("id", "")
            if new_id:
                _label_cache.setdefault(cache_key, {})[label_name] = new_id
                logger.info("Created Gmail label '%s' -> %s for %s", label_name, new_id, cache_key)
                return new_id
    except Exception as e:
        logger.warning("Failed to create label '%s' for %s: %s", label_name, cache_key, e)

    # Last resort — return empty string (label will be skipped)
    logger.error("Could not resolve label '%s' for %s", label_name, cache_key)
    return ""


# ── Draft creation ───────────────────────────────────────────────────────────


def create_draft(
    to: str,
    subject: str,
    html_body: str,
    cc: str = "",
    label: str = "",
    user_id: Optional[str] = None,
) -> dict:
    """Create a Gmail draft and apply the appropriate label.

    Returns dict with 'draft_id', 'message_id', 'labels', and 'success' keys.
    """
    result = {"draft_id": "", "message_id": "", "labels": [], "success": False}

    try:
        # Build tool arguments
        args = {
            "to": to,
            "subject": subject,
            "body": html_body,
            "body_type": "html",
        }
        if cc:
            args["cc"] = cc

        # Create the draft
        resp = _mcp_call("tools/call", {"name": "create_draft", "arguments": args}, user_id=user_id)

        # Parse response
        content = resp.get("result", {}).get("content", [])
        if not content:
            logger.error("Empty response from Gumstack create_draft")
            return result

        draft_data = json.loads(content[0].get("text", "{}"))
        message_id = draft_data.get("message", {}).get("id", "")
        draft_id = draft_data.get("id", "")

        if not message_id:
            logger.error("No message ID in create_draft response: %s", draft_data)
            return result

        result["draft_id"] = draft_id
        result["message_id"] = message_id
        result["labels"] = draft_data.get("message", {}).get("labelIds", [])
        result["success"] = True

        # Apply label via dynamic resolution
        label_name = label or DEFAULT_LABEL_NAME
        label_id = _resolve_label_id(label_name, user_id=user_id)
        if label_id:
            try:
                label_resp = _mcp_call(
                    "tools/call",
                    {
                        "name": "update_email",
                        "arguments": {
                            "email_id": message_id,
                            "add_labels": [label_id],
                        },
                    },
                    request_id=2,
                    user_id=user_id,
                )
                label_content = label_resp.get("result", {}).get("content", [])
                if label_content:
                    label_data = json.loads(label_content[0].get("text", "{}"))
                    result["labels"] = label_data.get("labelIds", result["labels"])
                logger.info(
                    "Gmail draft created + labeled: to=%s label=%s msg_id=%s",
                    to, label_name, message_id,
                )
            except Exception as e:
                logger.warning("Draft created but label failed: %s", e)
                # Draft still created successfully, just without label
        else:
            logger.warning("Draft created but label ID could not be resolved for '%s'", label_name)

    except FileNotFoundError:
        logger.error(
            "Gumstack Gmail token not found. Run Glass with Gmail MCP "
            "connected once to generate tokens at %s",
            _get_token_paths(user_id)[0],
        )
        _alert_gmail_auth_failure(user_id, "Token file not found")
    except requests.RequestException as e:
        logger.error("Gumstack Gmail API error: %s", e)
        if "401" in str(e) or "403" in str(e) or "Unauthorized" in str(e):
            _alert_gmail_auth_failure(user_id, str(e)[:200])
    except Exception as e:
        logger.error("Gumstack Gmail draft creation failed: %s", e)
        # Only alert auth failure for clear token/auth errors, NOT transient parse errors.
        # "Expecting value" is usually a transient Gumstack SSE issue, not a real auth problem.
        err_str = str(e).lower()
        if "token" in err_str or "unauthorized" in err_str or "forbidden" in err_str:
            _alert_gmail_auth_failure(user_id, str(e)[:200])

    return result


# ── Email reading ────────────────────────────────────────────────────────────


def read_emails(
    query: str,
    max_results: int = 10,
    user_id: Optional[str] = None,
) -> List[dict]:
    """Read emails matching a Gmail search query via Gumstack MCP.

    Returns a list of email dicts with keys:
        id, threadId, subject, from, to, date, body, labelIds
    """
    try:
        resp = _mcp_call(
            "tools/call",
            {
                "name": "read_emails",
                "arguments": {"query": query, "max_results": max_results},
            },
            user_id=user_id,
        )

        content = resp.get("result", {}).get("content", [])
        if not content:
            return []

        raw = json.loads(content[0].get("text", "[]"))
        if isinstance(raw, list):
            emails_list = raw
        elif isinstance(raw, dict):
            # Gumstack may return a single email dict (not wrapped in a list)
            # or a dict with "emails"/"messages" key
            emails_list = raw.get("emails", raw.get("messages", []))
            if not emails_list and "id" in raw and "subject" in raw:
                # Single email object — wrap it in a list
                emails_list = [raw]
        else:
            emails_list = []

        results = []
        for em in emails_list:
            results.append({
                "id": em.get("id", ""),
                "threadId": em.get("threadId", ""),
                "subject": em.get("subject", ""),
                "from": em.get("from", ""),
                "to": em.get("to", ""),
                "date": em.get("date", ""),
                "body": em.get("body", ""),
                "labelIds": em.get("labelIds", []),
                "attachments": em.get("attachments", []),
            })
        return results

    except FileNotFoundError:
        logger.warning("Gumstack Gmail token not found for read_emails (user=%s)", user_id)
        return []
    except Exception as e:
        logger.warning("read_emails failed: %s", e)
        return []


def get_thread(
    thread_id: str,
    user_id: Optional[str] = None,
) -> List[dict]:
    """Get all messages in a Gmail thread, ordered chronologically.

    Returns a list of message dicts with keys:
        id, threadId, subject, from, to, cc, date, body, labelIds
    """
    try:
        resp = _mcp_call(
            "tools/call",
            {
                "name": "get_thread",
                "arguments": {"thread_id": thread_id},
            },
            user_id=user_id,
        )
        content = resp.get("result", {}).get("content", [])
        if not content:
            return []

        raw = json.loads(content[0].get("text", "{}"))
        messages = raw.get("messages", []) if isinstance(raw, dict) else raw

        results = []
        for msg in messages:
            results.append({
                "id": msg.get("id", ""),
                "threadId": msg.get("threadId", ""),
                "subject": msg.get("subject", ""),
                "from": msg.get("from", ""),
                "to": msg.get("to", ""),
                "cc": msg.get("cc", ""),
                "date": msg.get("date", ""),
                "body": msg.get("body", ""),
                "labelIds": msg.get("labelIds", []),
                "headers": msg.get("headers", {}),
            })
        return results

    except Exception as e:
        logger.warning("get_thread failed: %s", e)
        return []


def get_attachment(
    email_id: str,
    attachment_id: str,
    filename: str = "attachment",
    user_id: Optional[str] = None,
) -> Optional[bytes]:
    """Download an email attachment via Gumstack MCP.

    Returns the raw attachment bytes, or None on failure.
    """
    try:
        resp = _mcp_call(
            "tools/call",
            {
                "name": "get_attachment",
                "arguments": {
                    "email_id": email_id,
                    "attachment_id": attachment_id,
                    "filename": filename,
                },
            },
            user_id=user_id,
        )
        content = resp.get("result", {}).get("content", [])
        if not content:
            return None

        import base64
        text = content[0].get("text", "")
        # The MCP response may be base64-encoded or raw JSON with data field
        try:
            data = json.loads(text)
            if isinstance(data, dict) and "data" in data:
                return base64.urlsafe_b64decode(data["data"])
        except (json.JSONDecodeError, ValueError):
            pass
        # Try direct base64 decode
        try:
            return base64.urlsafe_b64decode(text)
        except Exception:
            return text.encode() if text else None

    except Exception as e:
        logger.warning("get_attachment failed: %s", e)
        return None


# ── Label listing ────────────────────────────────────────────────────────────


def list_labels(
    user_id: Optional[str] = None,
    include_system: bool = False,
) -> List[dict]:
    """List Gmail labels via Gumstack MCP.

    Returns a list of label dicts with keys: id, name, type.
    When include_system is False, filters out system labels.
    """
    try:
        resp = _mcp_call(
            "tools/call",
            {"name": "list_labels", "arguments": {}},
            user_id=user_id,
        )
        content = resp.get("result", {}).get("content", [])
        if not content:
            return []

        raw = json.loads(content[0].get("text", "[]"))
        labels_list = raw if isinstance(raw, list) else raw.get("labels", [])

        results = []
        for lbl in labels_list:
            label_type = lbl.get("type", "user")
            if not include_system and label_type == "system":
                continue
            results.append({
                "id": lbl.get("id", ""),
                "name": lbl.get("name", ""),
                "type": label_type,
            })
        return results

    except Exception as e:
        logger.warning("list_labels failed: %s", e)
        return []


# ── Looker ZIP download via Gumstack ─────────────────────────────────────────


def fetch_looker_zip(
    subject_contains: str,
    max_age_hours: int = 24,
    user_id: Optional[str] = None,
) -> Optional[str]:
    """Download and extract a Looker ZIP attachment from Gmail via Gumstack.

    Searches for emails FROM noreply@lookermail.com with the given subject
    substring, received today. Downloads the first ZIP attachment found,
    saves to /tmp/gary_bot_looker/, extracts, and returns the extraction
    directory path. Returns None if not found.
    """
    import base64
    import re as _re
    import zipfile
    from datetime import datetime

    # Gmail 'after:' is strictly-after (excludes the date itself), so use yesterday
    from datetime import timedelta
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")
    query = f'from:noreply@lookermail.com subject:"{subject_contains}" after:{yesterday_str}'

    emails = read_emails(query, max_results=5, user_id=user_id)
    if not emails:
        logger.info("fetch_looker_zip: no emails found for '%s'", subject_contains)
        return None

    # Find the first email with a ZIP attachment
    for em in emails:
        attachments = em.get("attachments", [])
        zip_att = None
        for att in attachments:
            fname = att.get("filename", "")
            if fname.lower().endswith(".zip"):
                zip_att = att
                break

        if not zip_att:
            continue

        # Download the attachment
        att_data = get_attachment(
            email_id=em["id"],
            attachment_id=zip_att.get("attachmentId", zip_att.get("id", "")),
            filename=zip_att.get("filename", "attachment.zip"),
            user_id=user_id,
        )
        if not att_data:
            logger.warning("fetch_looker_zip: failed to download attachment")
            continue

        # Save and extract
        slug = _re.sub(r"[^a-z0-9]+", "_", subject_contains.lower()).strip("_")
        date_tag = datetime.now().strftime("%Y%m%d")
        base_dir = "/tmp/gary_bot_looker"
        zip_path = os.path.join(base_dir, f"{slug}_{date_tag}.zip")
        extract_dir = os.path.join(base_dir, f"{slug}_{date_tag}")

        os.makedirs(base_dir, exist_ok=True)
        with open(zip_path, "wb") as f:
            f.write(att_data)

        os.makedirs(extract_dir, exist_ok=True)
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)
        except zipfile.BadZipFile:
            logger.warning("fetch_looker_zip: bad ZIP file for '%s'", subject_contains)
            continue

        logger.info(
            "fetch_looker_zip: extracted '%s' -> %s (%d files)",
            subject_contains, extract_dir,
            len(os.listdir(extract_dir)),
        )
        return extract_dir

    logger.info("fetch_looker_zip: no ZIP attachment found for '%s'", subject_contains)
    return None


# ── Availability check ───────────────────────────────────────────────────────


def is_available(user_id: Optional[str] = None) -> bool:
    """Check if the Gumstack Gmail MCP tokens are present."""
    token_path, _ = _get_token_paths(user_id)
    return os.path.exists(token_path)
