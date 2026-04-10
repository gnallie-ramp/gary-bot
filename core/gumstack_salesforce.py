"""Gumstack Salesforce MCP client — create/update records and run SOQL via Gumloop.

Calls the Gumstack Salesforce MCP server directly over HTTP using OAuth tokens
stored by mcp-remote. Replaces the sf CLI which was revoked by Ramp security.

Supports per-user tokens via the user registry. When user_id is provided,
loads tokens from per-user location; otherwise falls back to the default
mcp-remote token cache.

Token location (default): ~/.mcp-auth/mcp-remote-0.1.12/<hash>_tokens.json
Hash = MD5 of "https://mcp.gumloop.com/salesforce/mcp"
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

_MCP_URL = "https://mcp.gumloop.com/salesforce/mcp"
_OAUTH_TOKEN_URL = "https://api.gumloop.com/oauth/token"
_GLASS_CREDS = Path.home() / ".project-glass" / "credentials.json"
_GLASS_SF_KEY = "gumstack-salesforce|"
_SERVER_HASH = hashlib.md5(_MCP_URL.encode()).hexdigest()
_TOKEN_DIR = Path.home() / ".mcp-auth" / "mcp-remote-0.1.12"
_TOKEN_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_tokens.json"
_CLIENT_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_client_info.json"

# Session cache: keyed by user_id (or "default") -> {"session": requests.Session, "initialized_at": float, "token": str}
_session_cache: Dict[str, dict] = {}
_SESSION_TTL = 300  # 5 minutes


def _alert_sf_auth_failure(user_id: Optional[str], error: str = "") -> None:
    """Send an inline DM when Salesforce auth fails."""
    try:
        from utils.auth_health import alert_auth_failure
        from config import GREG_SLACK_ID
        alert_auth_failure("salesforce", user_id or GREG_SLACK_ID, error)
    except Exception:
        pass


def _get_token_paths(user_id: Optional[str] = None) -> tuple[str, str]:
    """Resolve token and client info file paths for the given user."""
    if user_id:
        try:
            from core.user_registry import get_user_salesforce_tokens
            paths = get_user_salesforce_tokens(user_id)
            if paths:
                return paths
        except (ImportError, AttributeError):
            pass
        # Try per-user directory with standard naming
        user_token = Path.home() / ".gary_bot_tokens" / user_id / "salesforce_tokens.json"
        user_client = Path.home() / ".gary_bot_tokens" / user_id / "salesforce_client_info.json"
        if user_token.exists():
            return str(user_token), str(user_client)
    return str(_TOKEN_FILE), str(_CLIENT_FILE)


def _refresh_token(user_id: Optional[str] = None) -> str:
    """Refresh the access token using the stored refresh token via Gumloop OAuth."""
    token_path, client_path = _get_token_paths(user_id)
    if not os.path.exists(token_path):
        raise FileNotFoundError("Token file not found for Salesforce MCP")

    with open(token_path) as f:
        tokens = json.load(f)

    refresh_tok = tokens.get("refresh_token")
    if not refresh_tok:
        raise ValueError("No refresh_token in token file")

    client_id = ""
    if os.path.exists(client_path):
        with open(client_path) as f:
            client_id = json.load(f).get("client_id", "")

    resp = requests.post(
        _OAUTH_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_tok,
            "client_id": client_id,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    if resp.status_code != 200:
        logger.warning("Salesforce token refresh failed (HTTP %d): %s", resp.status_code, resp.text[:200])
        return tokens["access_token"]

    new_tokens = resp.json()
    tokens["access_token"] = new_tokens["access_token"]
    if "refresh_token" in new_tokens:
        tokens["refresh_token"] = new_tokens["refresh_token"]
    if "expires_in" in new_tokens:
        tokens["expires_in"] = new_tokens["expires_in"]

    with open(token_path, "w") as f:
        json.dump(tokens, f, indent=2)

    logger.info("Salesforce token refreshed and saved for %s", user_id or "default")
    return tokens["access_token"]


def _load_glass_token() -> Optional[str]:
    """Try to load a fresh Salesforce access token from Glass credentials."""
    try:
        if not _GLASS_CREDS.exists():
            return None
        import time as _time
        with open(_GLASS_CREDS) as f:
            creds = json.load(f)
        mcp_oauth = creds.get("mcpOAuth", {})
        for key, entry in mcp_oauth.items():
            if key.startswith(_GLASS_SF_KEY):
                expires_at = entry.get("expiresAt", 0)
                if expires_at > _time.time() * 1000:
                    return entry["accessToken"]
                return None
        return None
    except Exception:
        return None


def _load_access_token(user_id: Optional[str] = None) -> str:
    """Load the current access token, preferring Glass credentials."""
    glass_token = _load_glass_token()
    if glass_token:
        return glass_token

    token_path, _ = _get_token_paths(user_id)
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"Salesforce MCP token file not found: {token_path}")
    with open(token_path) as f:
        tokens = json.load(f)
    return tokens["access_token"]


def _get_session(user_id: Optional[str] = None, _retried: bool = False) -> Tuple[requests.Session, dict]:
    """Return a cached, initialized MCP session for the given user.

    If no valid cached session exists (missing, expired, or token changed),
    creates a new requests.Session, runs the MCP initialize handshake,
    and caches it.

    Returns (session, headers_dict) ready for tool calls.
    On 401 during init, attempts token refresh once before alerting.
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

    if init_resp.status_code == 401 and not _retried:
        logger.warning("Gumstack Salesforce 401 on init — attempting token refresh...")
        _session_cache.pop(cache_key, None)
        try:
            _refresh_token(user_id=user_id)
            return _get_session(user_id=user_id, _retried=True)
        except Exception as e:
            logger.warning("Salesforce token refresh failed: %s", e)
    if init_resp.status_code == 401:
        _alert_sf_auth_failure(user_id, "401 on init — token refresh failed")
        init_resp.raise_for_status()
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
    timeout: int = 30,
) -> dict:
    """Make a single MCP JSON-RPC call to the Gumstack Salesforce server.

    Uses a cached initialized session to avoid redundant initialize round-trips.
    On 401, clears the session cache and alerts the user.
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
    resp = session.post(_MCP_URL, json=payload, headers=headers, timeout=timeout)

    if resp.status_code == 401:
        _session_cache.pop(cache_key, None)
        _alert_sf_auth_failure(user_id, "401 on tool call — re-auth at gumloop.com/personal/apps")
    resp.raise_for_status()
    return _parse_response(resp)


def _parse_response(resp) -> dict:
    """Parse an MCP response that may be JSON or SSE (text/event-stream)."""
    try:
        return resp.json()
    except (json.JSONDecodeError, ValueError):
        pass
    for line in resp.text.splitlines():
        if line.startswith("data: "):
            try:
                return json.loads(line[6:])
            except (json.JSONDecodeError, ValueError):
                continue
    logger.error("Gumstack SF: could not parse response (%d bytes): %s",
                 len(resp.text), resp.text[:200])
    return {}


def _extract_text(result: dict) -> str:
    """Extract the first text content item from an MCP tool result."""
    content = result.get("result", {}).get("content", [])
    if content and isinstance(content, list):
        return content[0].get("text", "")
    return ""


def _extract_all_texts(result: dict) -> list[str]:
    """Extract all text content items from an MCP tool result.

    Gumstack returns each SOQL record as a separate content item.
    """
    content = result.get("result", {}).get("content", [])
    if not content or not isinstance(content, list):
        return []
    return [item.get("text", "") for item in content if item.get("type") == "text"]


# ── Public API ────────────────────────────────────────────────────────────────


def ensure_auth(user_id: Optional[str] = None) -> bool:
    """Check if Salesforce MCP auth is working.

    Returns True if authenticated, False otherwise.
    """
    try:
        result = soql_query("SELECT Id FROM Account LIMIT 1", user_id=user_id)
        return result is not None
    except Exception as e:
        logger.warning("Salesforce auth check failed: %s", e)
        return False


def create_record(
    object_name: str,
    fields: dict,
    user_id: Optional[str] = None,
) -> Optional[str]:
    """Create a Salesforce record.

    Returns the new record ID, or None on failure.
    """
    try:
        result = _mcp_call(
            "tools/call",
            {"name": "create_record", "arguments": {
                "object_name": object_name,
                "data": fields,
            }},
            user_id=user_id,
            timeout=30,
        )
        text = _extract_text(result)
        logger.info("Salesforce create_record (%s) response: %s", object_name, text[:200])

        # Try to parse the record ID from the response
        if text:
            try:
                parsed = json.loads(text)
                return parsed.get("id") or parsed.get("Id")
            except json.JSONDecodeError:
                # Response might be plain text — search for a Salesforce ID
                import re
                match = re.search(r'\b[a-zA-Z0-9]{15,18}\b', text)
                if match:
                    return match.group(0)
        return None
    except Exception as e:
        logger.error("Salesforce create_record failed (%s): %s", object_name, e)
        _alert_sf_auth_failure(user_id, str(e)[:200])
        return None


def update_record(
    object_name: str,
    record_id: str,
    fields: dict,
    user_id: Optional[str] = None,
) -> bool:
    """Update a Salesforce record.

    Returns True if update succeeded.
    """
    try:
        result = _mcp_call(
            "tools/call",
            {"name": "update_record", "arguments": {
                "object_name": object_name,
                "record_id": record_id,
                "data": fields,
            }},
            user_id=user_id,
            timeout=30,
        )
        text = _extract_text(result)
        logger.info("Salesforce update_record (%s/%s) response: %s", object_name, record_id, text[:200])
        return True
    except Exception as e:
        logger.error("Salesforce update_record failed (%s/%s): %s", object_name, record_id, e)
        _alert_sf_auth_failure(user_id, str(e)[:200])
        return False


def soql_query(soql: str, user_id: Optional[str] = None) -> Optional[list[dict]]:
    """Run a SOQL query and return the records.

    Returns list of record dicts, or None on failure.
    """
    try:
        result = _mcp_call(
            "tools/call",
            {"name": "soql_query", "arguments": {"query": soql}},
            user_id=user_id,
            timeout=30,
        )
        # Gumstack returns each record as a separate text content item
        texts = _extract_all_texts(result)
        if not texts:
            return []

        records = []
        for text in texts:
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    # Remove Salesforce metadata
                    parsed.pop("attributes", None)
                    records.append(parsed)
                elif isinstance(parsed, list):
                    for item in parsed:
                        if isinstance(item, dict):
                            item.pop("attributes", None)
                        records.append(item)
            except json.JSONDecodeError:
                logger.debug("SOQL item not JSON: %s", text[:100])
        return records
    except Exception as e:
        logger.error("Salesforce SOQL query failed: %s", e)
        return None
