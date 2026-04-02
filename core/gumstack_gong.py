"""Gumstack Gong MCP client — real-time call data via Gumloop.

Calls the Gumstack Gong MCP server directly over HTTP using OAuth tokens
stored by mcp-remote. Provides real-time access to Gong calls and transcripts
without waiting for the overnight Snowflake ELT sync.

Supports per-user tokens via the user registry. When user_id is provided,
loads tokens from per-user location; otherwise falls back to the default
mcp-remote token cache.

Token location (default): ~/.mcp-auth/mcp-remote-0.1.12/<hash>_tokens.json
Hash = MD5 of "https://mcp.gumloop.com/gong/mcp"
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

from core.user_registry import get_user_gong_tokens

logger = logging.getLogger(__name__)

_MCP_URL = "https://mcp.gumloop.com/gong/mcp"
_SERVER_HASH = hashlib.md5(_MCP_URL.encode()).hexdigest()
_TOKEN_DIR = Path.home() / ".mcp-auth" / "mcp-remote-0.1.12"
_TOKEN_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_tokens.json"


def _get_token_path(user_id: Optional[str] = None) -> str:
    """Resolve the token file path for the given user.

    Per-user tokens: ~/.gary_bot_tokens/<slack_id>/gong_tokens.json
    Default (original owner): ~/.mcp-auth/mcp-remote-0.1.12/<hash>_tokens.json
    """
    if user_id:
        result = get_user_gong_tokens(user_id)
        if result:
            return result
    # Fall back to default
    return str(_TOKEN_FILE)


def _load_access_token(user_id: Optional[str] = None) -> str:
    """Load the current access token from the token cache."""
    token_path = _get_token_path(user_id)
    if not Path(token_path).exists():
        raise FileNotFoundError(f"Gong MCP token file not found: {token_path}")
    with open(token_path) as f:
        tokens = json.load(f)
    return tokens["access_token"]


def _mcp_call(
    method: str,
    params: dict,
    request_id: int = 1,
    user_id: Optional[str] = None,
    _retried: bool = False,
) -> dict:
    """Make a single MCP JSON-RPC call to the Gumstack Gong server.

    On 401, attempts one token refresh before failing.
    """
    token = _load_access_token(user_id)
    token_path = _get_token_path(user_id)
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

    if init_resp.status_code == 401 and not _retried:
        logger.warning("Gumstack Gong 401 — token may be expired")
        try:
            with open(token_path) as f:
                tokens = json.load(f)
            requests.post(
                _MCP_URL,
                json={
                    "jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": {"name": "gary-bot", "version": "1.0.0"},
                    },
                },
                headers={
                    "Authorization": f"Bearer {tokens.get('refresh_token', '')}",
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",
                },
                timeout=15,
            )
            return _mcp_call(method, params, request_id, user_id=user_id, _retried=True)
        except Exception as e:
            logger.warning("Gong token refresh failed: %s", e)
    init_resp.raise_for_status()

    # Make the actual tool call
    resp = requests.post(
        _MCP_URL,
        json={"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
        headers=headers,
        timeout=30,
    )

    if resp.status_code == 401 and not _retried:
        logger.warning("Gumstack Gong 401 on tool call")
        return _mcp_call(method, params, request_id, user_id=user_id, _retried=True)
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
    logger.error("Gumstack Gong: could not parse response (%d bytes): %s",
                 len(resp.text), resp.text[:200])
    return {}


def is_available(user_id: Optional[str] = None) -> bool:
    """Check if the Gumstack Gong MCP tokens are present."""
    token_path = _get_token_path(user_id)
    return Path(token_path).exists()


def list_calls(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    max_results: int = 20,
    user_id: Optional[str] = None,
) -> list[dict]:
    """List calls in a date range. Defaults to last 7 days.

    Returns list of call dicts with id, title, started, duration, etc.
    """
    if not from_date:
        from_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")
    if not to_date:
        to_date = datetime.utcnow().strftime("%Y-%m-%dT23:59:59Z")

    try:
        resp = _mcp_call("tools/call", {
            "name": "list_calls",
            "arguments": {
                "fromDateTime": from_date,
                "toDateTime": to_date,
            },
        }, user_id=user_id)
        content = resp.get("result", {}).get("content", [])
        if not content:
            return []
        calls = []
        for item in content:
            try:
                call = json.loads(item.get("text", "{}"))
                if isinstance(call, dict) and call.get("id"):
                    calls.append(call)
            except (json.JSONDecodeError, TypeError):
                continue
        return calls[:max_results]
    except Exception as e:
        logger.error("Gong list_calls failed: %s", e)
        return []


def get_call(call_id: str, user_id: Optional[str] = None) -> dict:
    """Get metadata for a specific call."""
    try:
        resp = _mcp_call("tools/call", {
            "name": "get_call",
            "arguments": {"call_id": call_id},
        }, user_id=user_id)
        content = resp.get("result", {}).get("content", [])
        if not content:
            return {}
        return json.loads(content[0].get("text", "{}"))
    except Exception as e:
        logger.error("Gong get_call failed for %s: %s", call_id, e)
        return {}


def get_call_transcript(call_id: str, user_id: Optional[str] = None) -> str:
    """Get the full transcript for a call. Returns plain text."""
    try:
        resp = _mcp_call("tools/call", {
            "name": "get_call_transcript",
            "arguments": {"call_id": call_id},
        }, user_id=user_id)
        content = resp.get("result", {}).get("content", [])
        if not content:
            return ""
        data = json.loads(content[0].get("text", "{}"))

        # Gong returns: {callTranscripts: [{callId, transcript: [{speakerId, sentences: [{text}]}]}]}
        transcripts = data.get("callTranscripts", [])
        if not transcripts:
            return str(data)[:5000]

        segments = transcripts[0].get("transcript", [])
        lines = []
        for seg in segments:
            speaker_id = seg.get("speakerId", "Unknown")
            sentences = seg.get("sentences", [])
            text = " ".join(s.get("text", "") for s in sentences).strip()
            if text:
                lines.append(f"[{speaker_id[-4:]}]: {text}")
        return "\n".join(lines)
    except Exception as e:
        logger.error("Gong get_call_transcript failed for %s: %s", call_id, e)
        return ""


def list_users(max_results: int = 50, user_id: Optional[str] = None) -> list[dict]:
    """List Gong users. Useful for mapping speaker IDs to names."""
    try:
        resp = _mcp_call("tools/call", {
            "name": "list_users",
            "arguments": {"max_limit": max_results},
        }, user_id=user_id)
        content = resp.get("result", {}).get("content", [])
        if not content:
            return []
        data = json.loads(content[0].get("text", "{}"))
        return data.get("users", data) if isinstance(data, dict) else data if isinstance(data, list) else []
    except Exception as e:
        logger.error("Gong list_users failed: %s", e)
        return []


def get_todays_calls(user_id: Optional[str] = None) -> list[dict]:
    """Convenience: get all calls from today."""
    today = datetime.utcnow().strftime("%Y-%m-%dT00:00:00Z")
    now = datetime.utcnow().strftime("%Y-%m-%dT23:59:59Z")
    return list_calls(from_date=today, to_date=now, user_id=user_id)


def get_recent_calls_for_account(
    account_name: str,
    days: int = 7,
    user_id: Optional[str] = None,
) -> list[dict]:
    """Get recent calls that mention an account name in their title.

    Note: Gong API doesn't filter by SFDC account directly, so we filter
    by title match client-side. This is a best-effort heuristic.
    """
    calls = list_calls(
        from_date=(datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z"),
        max_results=100,
        user_id=user_id,
    )
    name_lower = account_name.lower()
    name_parts = [p for p in name_lower.split() if len(p) > 2]
    matched = []
    for call in calls:
        title = (call.get("title", "") or call.get("name", "") or "").lower()
        if any(part in title for part in name_parts):
            matched.append(call)
    return matched
