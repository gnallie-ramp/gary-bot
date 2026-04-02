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
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

_MCP_URL = "https://mcp.gumloop.com/gmail/mcp"
_SERVER_HASH = hashlib.md5(_MCP_URL.encode()).hexdigest()
_TOKEN_DIR = Path.home() / ".mcp-auth" / "mcp-remote-0.1.12"
_TOKEN_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_tokens.json"
_CLIENT_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_client_info.json"

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


def _load_access_token(user_id: Optional[str] = None) -> str:
    """Load the current access token from the token cache."""
    token_path, _ = _get_token_paths(user_id)
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"Gmail MCP token file not found: {token_path}")
    with open(token_path) as f:
        tokens = json.load(f)
    return tokens["access_token"]


def _refresh_token(user_id: Optional[str] = None) -> str:
    """Refresh the access token using the stored refresh token and client info."""
    token_path, client_path = _get_token_paths(user_id)
    if not os.path.exists(token_path) or not os.path.exists(client_path):
        raise FileNotFoundError("Token or client info file not found for Gmail MCP")

    with open(token_path) as f:
        tokens = json.load(f)
    with open(client_path) as f:
        client_info = json.load(f)

    # Use the Gumstack OAuth token endpoint (derived from MCP server)
    # mcp-remote uses the MCP server's own OAuth endpoints
    resp = requests.post(
        f"{_MCP_URL}",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "gary-bot", "version": "1.0.0"},
            },
        },
        headers={
            "Authorization": f"Bearer {tokens['refresh_token']}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
    )
    # If refresh fails, we'll just use the current token and hope it works
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


def _mcp_call(
    method: str,
    params: dict,
    request_id: int = 1,
    _retried: bool = False,
    user_id: Optional[str] = None,
) -> dict:
    """Make a single MCP JSON-RPC call to the Gumstack Gmail server.

    On 401, attempts one token refresh before falling back.
    """
    token = _load_access_token(user_id=user_id)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    # Initialize session
    init_resp = requests.post(
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

    # Auto-refresh on 401 (one retry only)
    if init_resp.status_code == 401 and not _retried:
        logger.warning("Gumstack Gmail 401 — attempting token refresh...")
        try:
            new_token = _refresh_token(user_id=user_id)
            if new_token:
                return _mcp_call(method, params, request_id, _retried=True, user_id=user_id)
        except Exception as e:
            logger.warning("Token refresh failed: %s", e)
    if init_resp.status_code == 401:
        # Refresh failed or already retried — alert user
        _alert_gmail_auth_failure(user_id, "401 on init — token refresh failed")
    init_resp.raise_for_status()

    # Make the actual tool call
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params,
    }
    resp = requests.post(_MCP_URL, json=payload, headers=headers, timeout=30)

    # Auto-refresh on 401 for tool call too
    if resp.status_code == 401 and not _retried:
        logger.warning("Gumstack Gmail 401 on tool call — attempting token refresh...")
        try:
            _refresh_token(user_id=user_id)
            return _mcp_call(method, params, request_id, _retried=True, user_id=user_id)
        except Exception as e:
            logger.warning("Token refresh failed: %s", e)
    if resp.status_code == 401:
        _alert_gmail_auth_failure(user_id, "401 on tool call — token refresh failed")
    resp.raise_for_status()
    return _parse_response(resp)


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
        # "Expecting value" = non-JSON response, often an auth error page
        if "Expecting value" in str(e) or "token" in str(e).lower():
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
        emails_list = raw if isinstance(raw, list) else raw.get("emails", raw.get("messages", []))

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


def get_attachment(
    email_id: str,
    attachment_id: str,
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
