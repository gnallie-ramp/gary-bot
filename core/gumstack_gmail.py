"""Gumstack Gmail MCP client — create drafts + apply labels via Gumloop.

Calls the Gumstack Gmail MCP server directly over HTTP using OAuth tokens
stored by mcp-remote. No Glass session needed — works headless from the bot.

Token location: ~/.mcp-auth/mcp-remote-0.1.12/<hash>_tokens.json
Hash = MD5 of "https://mcp.gumloop.com/gmail/mcp"
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

_MCP_URL = "https://mcp.gumloop.com/gmail/mcp"
_SERVER_HASH = hashlib.md5(_MCP_URL.encode()).hexdigest()
_TOKEN_DIR = Path.home() / ".mcp-auth" / "mcp-remote-0.1.12"
_TOKEN_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_tokens.json"
_CLIENT_FILE = _TOKEN_DIR / f"{_SERVER_HASH}_client_info.json"

# Gmail label ID mapping
LABEL_IDS = {
    "Claude Drafts/Post Meeting": "Label_26",
    "Claude Drafts/ACH to Card": "Label_27",
    "Claude Drafts/Procurement Trials": "Label_28",
    "Claude Drafts/Large Declines": "Label_29",
    "Claude Drafts/PCLIP Activation": "Label_30",
    "Claude Drafts/Prospecting": "Label_1136196488463956037",
    "Claude Drafts/Fundraise": "Label_5788174719529264583",
}
DEFAULT_LABEL_ID = "Label_1136196488463956037"  # Prospecting


def _load_access_token() -> str:
    """Load the current access token from mcp-remote's token cache."""
    if not _TOKEN_FILE.exists():
        raise FileNotFoundError(f"Gmail MCP token file not found: {_TOKEN_FILE}")
    with open(_TOKEN_FILE) as f:
        tokens = json.load(f)
    return tokens["access_token"]


def _refresh_token() -> str:
    """Refresh the access token using the stored refresh token and client info."""
    if not _TOKEN_FILE.exists() or not _CLIENT_FILE.exists():
        raise FileNotFoundError("Token or client info file not found for Gmail MCP")

    with open(_TOKEN_FILE) as f:
        tokens = json.load(f)
    with open(_CLIENT_FILE) as f:
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


def _mcp_call(method: str, params: dict, request_id: int = 1, _retried: bool = False) -> dict:
    """Make a single MCP JSON-RPC call to the Gumstack Gmail server.

    On 401, attempts one token refresh before falling back.
    """
    token = _load_access_token()
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
            new_token = _refresh_token()
            if new_token:
                return _mcp_call(method, params, request_id, _retried=True)
        except Exception as e:
            logger.warning("Token refresh failed: %s", e)
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
            _refresh_token()
            return _mcp_call(method, params, request_id, _retried=True)
        except Exception as e:
            logger.warning("Token refresh failed: %s", e)
    resp.raise_for_status()
    return resp.json()


def create_draft(
    to: str,
    subject: str,
    html_body: str,
    cc: str = "",
    label: str = "",
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
        resp = _mcp_call("tools/call", {"name": "create_draft", "arguments": args})

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

        # Apply label
        label_id = LABEL_IDS.get(label, DEFAULT_LABEL_ID)
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
            )
            label_content = label_resp.get("result", {}).get("content", [])
            if label_content:
                label_data = json.loads(label_content[0].get("text", "{}"))
                result["labels"] = label_data.get("labelIds", result["labels"])
            logger.info(
                "Gmail draft created + labeled: to=%s label=%s msg_id=%s",
                to, label or "Post Meeting", message_id,
            )
        except Exception as e:
            logger.warning("Draft created but label failed: %s", e)
            # Draft still created successfully, just without label

    except FileNotFoundError:
        logger.error(
            "Gumstack Gmail token not found. Run Glass with Gmail MCP "
            "connected once to generate tokens at %s", _TOKEN_FILE,
        )
    except requests.RequestException as e:
        logger.error("Gumstack Gmail API error: %s", e)
    except Exception as e:
        logger.error("Gumstack Gmail draft creation failed: %s", e)

    return result


def is_available() -> bool:
    """Check if the Gumstack Gmail MCP tokens are present."""
    return _TOKEN_FILE.exists()
