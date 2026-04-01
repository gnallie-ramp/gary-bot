"""Salesforce access — reads via Gumstack MCP, writes pending Growth MCP.

Reads (SOQL queries, auth check): Gumstack Salesforce MCP at
mcp.gumloop.com/salesforce/mcp. Works with OAuth tokens from mcp-remote.

Writes (create/update opps): Gumstack blocks write operations.
TODO: Wire create_opportunity/update_opportunity through Growth MCP
once write capabilities ship (expected April 2026).

sf CLI was revoked by Ramp security (April 2026).
"""
from __future__ import annotations

import logging

from core.gumstack_salesforce import (
    ensure_auth as _gumstack_ensure_auth,
    create_record as _gumstack_create,
    update_record as _gumstack_update,
    soql_query as _gumstack_soql,
)

logger = logging.getLogger(__name__)


def ensure_auth(user_id: str | None = None) -> bool:
    """Check if Salesforce MCP auth is working.

    Returns True if authenticated, False otherwise.
    """
    return _gumstack_ensure_auth(user_id=user_id)


def create_opportunity(fields: dict, user_id: str | None = None) -> str | None:
    """Create an Opportunity record in Salesforce.

    Parameters
    ----------
    fields : dict
        Field API names -> values. Example::

            {
                "AccountId": "001...",
                "Name": "Acme - Card",
                "StageName": "S2: Sales Qualified Opportunity",
                "CloseDate": "2026-04-15",
                "RecordTypeId": "0125b000000PZaIAAW",
                "Expansion_Type__c": "New Card Programs",
                ...
            }
    user_id : str, optional
        Slack user ID for auth failure alerts.

    Returns
    -------
    str or None
        The new Opportunity ID, or None on failure.
    """
    # TODO: Gumstack MCP blocks writes. Swap to Growth MCP when available.
    opp_id = _gumstack_create("Opportunity", fields, user_id=user_id)
    if opp_id:
        logger.info("Created Salesforce Opportunity: %s", opp_id)
    return opp_id


def update_opportunity(opp_id: str, fields: dict, user_id: str | None = None) -> bool:
    """Update fields on an existing Opportunity record.

    Parameters
    ----------
    opp_id : str
        Salesforce Opportunity ID.
    fields : dict
        Field API names -> new values.
    user_id : str, optional
        Slack user ID for auth failure alerts.

    Returns
    -------
    bool
        True if update succeeded.
    """
    # TODO: Gumstack MCP blocks writes. Swap to Growth MCP when available.
    success = _gumstack_update("Opportunity", opp_id, fields, user_id=user_id)
    if success:
        logger.info("Updated Salesforce Opportunity: %s", opp_id)
    return success


def get_gong_call_url(account_id: str) -> str:
    """Get the most recent Gong call URL for an SFDC account via Snowflake.

    Returns the URL string, or empty string on failure / no results.
    """
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


def query(soql: str, user_id: str | None = None) -> list[dict]:
    """Run a SOQL query and return the records.

    Parameters
    ----------
    soql : str
        SOQL query string.
    user_id : str, optional
        Slack user ID for per-user auth.

    Returns
    -------
    list[dict]
        List of record dicts.
    """
    result = _gumstack_soql(soql, user_id=user_id)
    if result is None:
        return []
    return result
