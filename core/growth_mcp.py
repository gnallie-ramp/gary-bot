"""Growth MCP client — create and update Salesforce opportunities via Ramp's Growth MCP.

Calls the Growth MCP server over HTTP using OAuth tokens from either Glass
credentials or mcp-remote cache.

Endpoint: https://growth-mcp-remote.ramp.builders/mcp
Tools used: create_expansion_opportunity, update_opportunities

Token loading priority:
  1. Glass credentials at ~/.project-glass/credentials.json (key prefix "growth|")
  2. Per-user tokens at ~/.gary_bot_tokens/<slack_id>/growth_tokens.json
  3. Default mcp-remote cache at ~/.mcp-auth/mcp-remote-0.1.12/<hash>_tokens.json
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

_MCP_URL = "https://growth-mcp-remote.ramp.builders/mcp"
_GLASS_CREDS = Path.home() / ".project-glass" / "credentials.json"
_GLASS_KEY_PREFIX = "growth|"
_SERVER_HASH = hashlib.md5(_MCP_URL.encode()).hexdigest()
_TOKEN_DIR = Path.home() / ".mcp-auth" / "mcp-remote-0.1.12"
_TOKEN_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_tokens.json"
_CLIENT_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_client_info.json"

_session_cache: Dict[str, dict] = {}
_SESSION_TTL = 300  # 5 minutes


def _alert_auth_failure(user_id: Optional[str], error: str = "") -> None:
    try:
        from utils.auth_health import alert_auth_failure
        from config import GREG_SLACK_ID
        alert_auth_failure("growth", user_id or GREG_SLACK_ID, error)
    except Exception:
        pass


def _get_token_paths(user_id: Optional[str] = None) -> Tuple[str, str]:
    if user_id:
        user_token = Path.home() / ".gary_bot_tokens" / user_id / "growth_tokens.json"
        user_client = Path.home() / ".gary_bot_tokens" / user_id / "growth_client_info.json"
        if user_token.exists():
            return str(user_token), str(user_client)
    return str(_TOKEN_FILE), str(_CLIENT_FILE)


def _load_glass_token() -> Optional[str]:
    """Load a fresh Growth MCP access token from Glass credentials if present."""
    try:
        if not _GLASS_CREDS.exists():
            return None
        with open(_GLASS_CREDS) as f:
            creds = json.load(f)
        mcp_oauth = creds.get("mcpOAuth", {})
        for key, entry in mcp_oauth.items():
            if key.startswith(_GLASS_KEY_PREFIX):
                expires_at = entry.get("expiresAt", 0)
                if expires_at > time.time() * 1000:
                    return entry.get("accessToken")
                return None
        return None
    except Exception:
        return None


def _load_access_token(user_id: Optional[str] = None) -> str:
    glass_token = _load_glass_token()
    if glass_token:
        return glass_token
    token_path, _ = _get_token_paths(user_id)
    if not os.path.exists(token_path):
        raise FileNotFoundError(f"Growth MCP token file not found: {token_path}")
    with open(token_path) as f:
        tokens = json.load(f)
    return tokens["access_token"]


def _get_session(user_id: Optional[str] = None) -> Tuple[requests.Session, dict]:
    cache_key = user_id or "default"
    token = _load_access_token(user_id=user_id)
    now = time.monotonic()

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
    if init_resp.status_code == 401:
        _alert_auth_failure(user_id, "401 on init — re-auth Growth MCP in Glass")
    init_resp.raise_for_status()

    _session_cache[cache_key] = {
        "session": session,
        "initialized_at": now,
        "token": token,
    }
    return session, headers


def _parse_response(resp) -> dict:
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
    logger.error("Growth MCP: could not parse response (%d bytes): %s",
                 len(resp.text), resp.text[:200])
    return {}


def _extract_tool_payload(result: dict) -> dict:
    """Extract the parsed JSON body from an MCP tool result's text content."""
    content = result.get("result", {}).get("content", [])
    if not content or not isinstance(content, list):
        return {}
    text = content[0].get("text", "")
    if not text:
        return {}
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return {"_raw": text}


def _mcp_call(
    tool_name: str,
    arguments: dict,
    user_id: Optional[str] = None,
    timeout: int = 30,
) -> dict:
    cache_key = user_id or "default"
    session, headers = _get_session(user_id=user_id)
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments},
    }
    resp = session.post(_MCP_URL, json=payload, headers=headers, timeout=timeout)
    if resp.status_code == 401:
        _session_cache.pop(cache_key, None)
        _alert_auth_failure(user_id, "401 on tool call — re-auth Growth MCP in Glass")
    resp.raise_for_status()
    return _parse_response(resp)


# ── Public API ────────────────────────────────────────────────────────────────


def ensure_auth(user_id: Optional[str] = None) -> bool:
    """Return True if Growth MCP auth is working (can initialize a session)."""
    try:
        _get_session(user_id=user_id)
        return True
    except Exception as e:
        logger.warning("Growth MCP auth check failed: %s", e)
        return False


def create_expansion_opportunity(
    account_id: str,
    expansion_type: str,
    expansion_motion: str,
    expansion_source: str,
    expansion_notes: str,
    expansion_product: Optional[str] = None,
    expansion_product_amount: Optional[float] = None,
    primary_contact_id: Optional[str] = None,
    opportunity_name: Optional[str] = None,
    stage_name: Optional[str] = None,
    next_step: Optional[str] = None,
    next_step_due_date: Optional[str] = None,
    close_date: Optional[str] = None,
    user_id: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Create an Expansion Opportunity.

    Returns (opp_id, error_msg). On success: (opp_id, None). On failure: (None, error_msg).
    """
    args = {
        "account_id": account_id,
        "expansion_type": expansion_type,
        "expansion_motion": expansion_motion,
        "expansion_source": expansion_source,
        "expansion_notes": expansion_notes,
    }
    optional = {
        "expansion_product": expansion_product,
        "expansion_product_amount": expansion_product_amount,
        "primary_contact_id": primary_contact_id,
        "opportunity_name": opportunity_name,
        "stage_name": stage_name,
        "next_step": next_step,
        "next_step_due_date": next_step_due_date,
        "close_date": close_date,
    }
    for k, v in optional.items():
        if v is not None:
            args[k] = v

    try:
        rpc = _mcp_call("create_expansion_opportunity", args, user_id=user_id)
        payload = _extract_tool_payload(rpc)
        inner = payload.get("result", payload) if isinstance(payload, dict) else {}
        if isinstance(inner, dict) and inner.get("success"):
            opp_id = inner.get("opportunity_id")
            logger.info("Growth MCP created expansion opp: %s", opp_id)
            return opp_id, None
        err = inner.get("error") if isinstance(inner, dict) else str(payload)
        logger.error("Growth MCP create_expansion_opportunity failed: %s", err)
        return None, str(err) if err else "Unknown error from Growth MCP"
    except Exception as e:
        logger.error("Growth MCP create_expansion_opportunity raised: %s", e)
        _alert_auth_failure(user_id, str(e)[:200])
        return None, str(e)


def get_filtered_accounts(
    sort_by: Optional[str] = None,
    sort_direction: Optional[str] = None,
    page: Optional[int] = None,
    page_size: Optional[int] = None,
    columns: Optional[list[str]] = None,
    filters: Optional[dict] = None,
    user_id: Optional[str] = None,
) -> list[dict]:
    """Pull accounts from current user's Book of Business with flexible filtering/sorting."""
    args: dict = {}
    if sort_by is not None:
        args["sort_by"] = sort_by
    if sort_direction is not None:
        args["sort_direction"] = sort_direction
    if page is not None:
        args["page"] = page
    if page_size is not None:
        args["page_size"] = page_size
    if columns is not None:
        args["columns"] = columns
    if filters:
        args.update(filters)
    try:
        rpc = _mcp_call("get_filtered_accounts", args, user_id=user_id, timeout=45)
        payload = _extract_tool_payload(rpc)
        return payload.get("accounts", []) if isinstance(payload, dict) else []
    except Exception as e:
        logger.error("Growth MCP get_filtered_accounts raised: %s", e)
        _alert_auth_failure(user_id, str(e)[:200])
        return []


def get_opportunities(
    sfdc_account_id: Optional[str] = None,
    state: Optional[str] = None,
    opportunity_type: Optional[str] = None,
    user_id: Optional[str] = None,
) -> list[dict]:
    """Fetch opportunities, optionally filtered by account, state, or type."""
    args: dict = {}
    if sfdc_account_id:
        args["sfdc_account_id"] = sfdc_account_id
    if state:
        args["state"] = state
    if opportunity_type:
        args["opportunity_type"] = opportunity_type
    try:
        rpc = _mcp_call("get_opportunities", args, user_id=user_id, timeout=30)
        payload = _extract_tool_payload(rpc)
        return payload.get("opportunities", []) if isinstance(payload, dict) else []
    except Exception as e:
        logger.error("Growth MCP get_opportunities raised: %s", e)
        return []


def get_account_details(
    sfdc_account_id: Optional[str] = None,
    account_uuid: Optional[str] = None,
    columns: Optional[list[str]] = None,
    user_id: Optional[str] = None,
) -> dict:
    """Fetch detailed account metadata."""
    args: dict = {}
    if sfdc_account_id:
        args["sfdc_account_id"] = sfdc_account_id
    if account_uuid:
        args["account_uuid"] = account_uuid
    if columns:
        args["columns"] = columns
    try:
        rpc = _mcp_call("get_account_details", args, user_id=user_id, timeout=30)
        payload = _extract_tool_payload(rpc)
        return payload.get("account", {}) if isinstance(payload, dict) else {}
    except Exception as e:
        logger.error("Growth MCP get_account_details raised: %s", e)
        return {}


def update_opportunities(
    updates: list[dict],
    user_id: Optional[str] = None,
) -> dict:
    """Batch update opps via Growth MCP.

    Each update dict must include "opportunity_id" plus at least one field.
    Returns {"success_ids": [...], "failed": [...]}. Partial failure is allowed.
    """
    if not updates:
        return {"success_ids": [], "failed": []}
    try:
        rpc = _mcp_call("update_opportunities", {"updates": updates}, user_id=user_id)
        payload = _extract_tool_payload(rpc)
        results = payload.get("results", []) if isinstance(payload, dict) else []
        success_ids = [r.get("opportunity_id") for r in results if r.get("success")]
        failed = [r for r in results if not r.get("success")]
        if failed:
            logger.warning("Growth MCP update_opportunities partial failure: %s", failed)
        return {"success_ids": success_ids, "failed": failed}
    except Exception as e:
        logger.error("Growth MCP update_opportunities raised: %s", e)
        _alert_auth_failure(user_id, str(e)[:200])
        return {"success_ids": [], "failed": [{"error": str(e)}]}
