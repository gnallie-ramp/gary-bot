"""Salesforce access — reads via Gumstack MCP, writes via Ramp Growth MCP.

Reads (SOQL queries, auth check): Gumstack Salesforce MCP at
mcp.gumloop.com/salesforce/mcp.

Writes (create/update opps): Ramp Growth MCP at growth-mcp-remote.ramp.builders/mcp.
Gumstack blocks writes for our permission group, so opp create/update goes through
Growth MCP's create_expansion_opportunity and update_opportunities tools.

Callers pass the same SFDC-style field dict as before (e.g. {"AccountId": "...",
"StageName": "S2: Sales Qualified Opportunity", "Expansion_Type__c": "Bill Pay"}).
The translation layer in this module converts that to Growth MCP tool params.

sf CLI was revoked by Ramp security (April 2026).
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from core.gumstack_salesforce import (
    ensure_auth as _gumstack_ensure_auth,
    soql_query as _gumstack_soql,
)
from core import growth_mcp

logger = logging.getLogger(__name__)


def _resolve_primary_contact_id(account_id: str) -> Optional[str]:
    """Pick the best SFDC contact_id for the account.

    Names and emails in dim_sfdc_contacts are PII-hashed, but contact_ids,
    titles, and IS_P0_TITLE/IS_GROWTH_TITLE flags are real. We rank by:
      1. P0 title (Owner/CEO/CFO/Finance leader)
      2. Growth title (Admin/AP/Bookkeeper)
      3. Any contact with a non-empty title
      4. Any contact

    Returns None if the account has no SFDC contacts at all.
    """
    try:
        from core.snowflake_client import run_query

        df = run_query(f"""
            SELECT contact_id, contact_title, is_p0_title, is_growth_title
            FROM analytics.marts.dim_sfdc_contacts
            WHERE account_id = '{account_id}'
            ORDER BY
                CASE WHEN is_p0_title THEN 0
                     WHEN is_growth_title THEN 1
                     WHEN COALESCE(contact_title, '') <> '' THEN 2
                     ELSE 3 END,
                contact_id
            LIMIT 1
        """)
        if df.empty:
            return None
        return df.iloc[0]["contact_id"]
    except Exception as exc:
        logger.warning("Primary contact resolution failed for %s: %s", account_id, exc)
        return None

# Gap-field update runs immediately after create. SF async triggers/workflow rules
# can race with the update and fire validation errors against not-yet-settled state.
# Retry with exponential backoff clears this reliably.
_POST_CREATE_UPDATE_RETRY_DELAYS_SEC = (2.0, 4.0, 6.0)


# ── Field translation: old SFDC-style dict → Growth MCP tool params ──────────

# Fields stripped (auto-applied server-side by Growth MCP)
_STRIPPED_FIELDS = {"RecordTypeId"}

# Per-product amount fields collapse into `expansion_product_amount` on create
_AMOUNT_FIELDS = {
    "Expansion_Amount__c",
    "Bill_Pay_Expansion_Amount__c",
    "RBA_Amount_Committed__c",
    "Monthly_Travel_Bookings_Amount__c",
}

# Create tool doesn't accept these — route to update-after-create
_UPDATE_AFTER_CREATE = {
    "Gong_Outreach_Link__c": "gong_outreach_link",
    "WinReasonDetail__c": "win_reason_detail",
    # Card + Bill Pay defaults — Growth MCP create_expansion_opportunity doesn't
    # accept these directly, so we plumb them via the follow-up
    # update_opportunities call (same retry-with-exponential-backoff pattern
    # that handles Gong link).
    "Timeframe_of_Spend__c": "timeframe_of_spend",
    "Primary_Competitor__c": "primary_competitor",
    "Win_Reason__c": "win_reason",
}

# SFDC API name → Growth MCP create_expansion_opportunity param
_CREATE_FIELD_MAP = {
    "AccountId": "account_id",
    "Name": "opportunity_name",
    "StageName": "stage_name",
    "CloseDate": "close_date",
    "Expansion_Type__c": "expansion_type",
    "Expansion_Motion__c": "expansion_motion",
    "Expansion_Product__c": "expansion_product",
    "Expansion_Source__c": "expansion_source",
    "Expansion_Notes__c": "expansion_notes",
    "Primary_Contact__c": "primary_contact_id",
    "NextStep": "next_step",
    "Next_Step_Due_Date__c": "next_step_due_date",
}

# SFDC API name → Growth MCP update_opportunities field
_UPDATE_FIELD_MAP = {
    "StageName": "stage_name",
    "NextStep": "next_step",
    "Next_Step_Due_Date__c": "next_step_due_date",
    "CloseDate": "close_date",
    "Expansion_Notes__c": "expansion_notes",
    "Expansion_Amount__c": "expansion_amount",
    "Bill_Pay_Expansion_Amount__c": "bill_pay_expansion_amount",
    "RBA_Amount_Committed__c": "rba_amount_committed",
    "Monthly_Travel_Bookings_Amount__c": "monthly_travel_bookings_amount",
    "Gong_Outreach_Link__c": "gong_outreach_link",
    "WinReasonDetail__c": "win_reason_detail",
    "Primary_Competitor__c": "primary_competitor",
    "Timeframe_of_Spend__c": "timeframe_of_spend",
    "Win_Reason__c": "win_reason",
    "Competitive_Dynamic__c": "competitive_dynamic",
    "Loss_Reason__c": "loss_reason",
    "Loss_Reason_Details__c": "loss_reason_details",
}


def _translate_create_fields(fields: dict) -> tuple[dict, dict]:
    """Split an old-style SFDC field dict into (create_params, update_after_params)."""
    create_params: dict = {}
    update_after: dict = {}
    for key, val in fields.items():
        if val is None or val == "":
            continue
        if key in _STRIPPED_FIELDS:
            continue
        if key in _UPDATE_AFTER_CREATE:
            update_after[_UPDATE_AFTER_CREATE[key]] = val
            continue
        if key in _AMOUNT_FIELDS:
            try:
                create_params["expansion_product_amount"] = float(val)
            except (ValueError, TypeError):
                logger.warning("Dropping non-numeric amount %s=%r", key, val)
            continue
        mcp_key = _CREATE_FIELD_MAP.get(key)
        if mcp_key:
            create_params[mcp_key] = val
        else:
            logger.warning("Unmapped SFDC field dropped at create: %s", key)
    return create_params, update_after


def _translate_update_fields(fields: dict) -> dict:
    """Translate SFDC field names to Growth MCP update_opportunities params."""
    params: dict = {}
    for key, val in fields.items():
        if val is None:
            continue
        if key in _STRIPPED_FIELDS:
            continue
        mcp_key = _UPDATE_FIELD_MAP.get(key)
        if mcp_key:
            params[mcp_key] = val
        else:
            logger.warning("Unmapped SFDC field dropped at update: %s", key)
    return params


# ── Public API ────────────────────────────────────────────────────────────────


def ensure_auth(user_id: Optional[str] = None) -> bool:
    """Check if Salesforce read auth is working (via Gumstack)."""
    return _gumstack_ensure_auth(user_id=user_id)


def create_opportunity(fields: dict, user_id: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
    """Create an Expansion Opportunity via Growth MCP.

    Accepts the same SFDC-style field dict the old Gumstack flow used.
    Auto-resolves primary_contact_id from the account's best contact when not provided
    (Growth MCP requires either Main_POC__c on the account OR an explicit primary_contact_id).
    Gap fields (Gong_Outreach_Link__c, WinReasonDetail__c) are set via a
    follow-up update_opportunities call since the create tool doesn't accept them.

    Returns (opp_id, error_msg). On success: (opp_id, None). On failure: (None, error_msg).
    If create succeeds but the follow-up gap update fails, still returns (opp_id, None).
    """
    create_params, update_after = _translate_create_fields(fields)

    required = {"account_id", "expansion_type", "expansion_motion", "expansion_source", "expansion_notes"}
    missing = required - set(create_params)
    if missing:
        err = f"Missing required fields: {sorted(missing)}"
        logger.error("create_opportunity %s", err)
        return None, err

    # Auto-resolve primary contact if not explicitly provided
    if not create_params.get("primary_contact_id"):
        resolved = _resolve_primary_contact_id(create_params["account_id"])
        if resolved:
            create_params["primary_contact_id"] = resolved
            logger.info("Auto-resolved primary_contact_id=%s for account %s",
                        resolved, create_params["account_id"])

    opp_id, err = growth_mcp.create_expansion_opportunity(user_id=user_id, **create_params)
    if not opp_id:
        return None, err

    logger.info("Created Salesforce Opportunity via Growth MCP: %s", opp_id)

    if update_after:
        update_after["opportunity_id"] = opp_id
        result = growth_mcp.update_opportunities([update_after], user_id=user_id)
        attempt = 0
        while not result.get("success_ids") and attempt < len(_POST_CREATE_UPDATE_RETRY_DELAYS_SEC):
            delay = _POST_CREATE_UPDATE_RETRY_DELAYS_SEC[attempt]
            logger.info(
                "Opp %s gap-field update hit a race (attempt %d) — retrying in %.1fs",
                opp_id, attempt + 1, delay,
            )
            time.sleep(delay)
            result = growth_mcp.update_opportunities([update_after], user_id=user_id)
            attempt += 1
        if not result.get("success_ids"):
            logger.warning(
                "Opp %s created but gap-field update failed after %d retries: %s",
                opp_id, len(_POST_CREATE_UPDATE_RETRY_DELAYS_SEC), result.get("failed"),
            )
    return opp_id, None


def update_opportunity(opp_id: str, fields: dict, user_id: Optional[str] = None) -> bool:
    """Update fields on an existing Opportunity via Growth MCP."""
    params = _translate_update_fields(fields)
    if not params:
        logger.warning("update_opportunity for %s had no translatable fields", opp_id)
        return False
    params["opportunity_id"] = opp_id
    result = growth_mcp.update_opportunities([params], user_id=user_id)
    success = opp_id in result.get("success_ids", [])
    if success:
        logger.info("Updated Salesforce Opportunity via Growth MCP: %s", opp_id)
    return success


def get_gong_call_url(account_id: str) -> str:
    """Get the most recent Gong call URL for an SFDC account via Snowflake."""
    try:
        from core.snowflake_client import run_query
        sql = f"""
            SELECT igc.gong_call_url
            FROM analytics.int.int_gong_calls igc
            JOIN analytics.marts.dim_sfdc_gong_call gc ON gc.gong_call_id = igc.gong_call_id
            WHERE gc.sfdc_primary_account_id = '{account_id}'
            AND igc.gong_call_url IS NOT NULL
            ORDER BY igc.call_start_at DESC
            LIMIT 1
        """
        df = run_query(sql)
        if df is not None and not df.empty:
            url = df.iloc[0].get("gong_call_url", "")
            return str(url) if url else ""
    except Exception as e:
        logger.debug("Gong call URL lookup failed for %s: %s", account_id, e)
    return ""


def query(soql: str, user_id: Optional[str] = None) -> list[dict]:
    """Run a SOQL query (read) via Gumstack. Returns list of record dicts."""
    result = _gumstack_soql(soql, user_id=user_id)
    if result is None:
        return []
    return result
