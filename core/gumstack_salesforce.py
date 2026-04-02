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
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_MCP_URL = "https://mcp.gumloop.com/salesforce/mcp"
_SERVER_HASH = hashlib.md5(_MCP_URL.encode()).hexdigest()
_TOKEN_DIR = Path.home() / ".mcp-auth" / "mcp-remote-0.1.12"
_TOKEN_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_tokens.json"
_CLIENT_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_client_info.json"


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


def _load_access_token(user_id: Optional[str] = None) -> str:
    """Load the current access token from the token cache."""
    token_path, _ = _get_token_paths(user_id)
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"Salesforce MCP token file not found: {token_path}")
    with open(token_path) as f:
        tokens = json.load(f)
    return tokens["access_token"]


def _mcp_call(
    method: str,
    params: dict,
    request_id: int = 1,
    _retried: bool = False,
    user_id: Optional[str] = None,
    timeout: int = 30,
) -> dict:
    """Make a single MCP JSON-RPC call to the Gumstack Salesforce server.

    On 401, alerts the user and raises.
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

    if init_resp.status_code == 401:
        _alert_sf_auth_failure(user_id, "401 on init — re-auth at gumloop.com/personal/apps")
        init_resp.raise_for_status()
    init_resp.raise_for_status()

    # Make the actual tool call
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params,
    }
    resp = requests.post(_MCP_URL, json=payload, headers=headers, timeout=timeout)

    if resp.status_code == 401:
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
