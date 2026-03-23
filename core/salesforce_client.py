"""Salesforce access via the `sf` CLI (SSO-authenticated).

Uses the locally cached browser-based SSO session from `sf org login web`.
No passwords or tokens needed in .env — mirrors the Snowflake/snow CLI pattern.
"""
from __future__ import annotations

import json
import logging
import subprocess

logger = logging.getLogger(__name__)

_SF_ORG_ALIAS = "ramp"


def _run_sf(*args, timeout: int = 30) -> dict:
    """Run an sf CLI command and return the parsed JSON result."""
    cmd = ["sf", *args, "--json", "--target-org", _SF_ORG_ALIAS]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        parsed = json.loads(result.stdout) if result.stdout else {}
        if result.returncode != 0:
            err_msg = parsed.get("message", result.stderr or "Unknown error")
            raise RuntimeError(f"sf CLI error: {err_msg}")
        return parsed
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"sf CLI timed out after {timeout}s")
    except json.JSONDecodeError:
        raise RuntimeError(f"sf CLI returned non-JSON: {result.stdout[:200]}")


def ensure_auth() -> bool:
    """Check if the sf CLI has a valid cached auth session.

    Returns True if authenticated, False otherwise.
    """
    try:
        result = _run_sf("org", "display")
        status = result.get("result", {}).get("connectedStatus", "")
        return status == "Connected"
    except Exception as e:
        logger.warning("Salesforce auth check failed: %s", e)
        return False


def create_opportunity(fields: dict) -> str | None:
    """Create an Opportunity record in Salesforce.

    Parameters
    ----------
    fields : dict
        Field API names → values. Example::

            {
                "AccountId": "001...",
                "Name": "Acme - Card",
                "StageName": "S2: Sales Qualified Opportunity",
                "CloseDate": "2026-04-15",
                "RecordTypeId": "0125b000000PZaIAAW",
                "Expansion_Type__c": "New Card Programs",
                ...
            }

    Returns
    -------
    str or None
        The new Opportunity ID, or None on failure.
    """
    # Build the field values string for sf data create record
    # The sf CLI parses --values as space-separated key=value pairs.
    # Values with spaces must be enclosed in double quotes WITHIN the string.
    values_parts = []
    for key, val in fields.items():
        safe_val = str(val).replace('"', '\\"').replace("'", "\\'")
        # Always double-quote values to handle spaces, commas, special chars
        values_parts.append(f'{key}="{safe_val}"')
    values_str = " ".join(values_parts)

    try:
        result = _run_sf(
            "data", "create", "record",
            "--sobject", "Opportunity",
            "--values", values_str,
        )
        opp_id = result.get("result", {}).get("id")
        if opp_id:
            logger.info("Created Salesforce Opportunity: %s", opp_id)
        return opp_id
    except Exception as e:
        logger.error("Failed to create Salesforce Opportunity: %s", e)
        return None


def update_opportunity(opp_id: str, fields: dict) -> bool:
    """Update fields on an existing Opportunity record.

    Parameters
    ----------
    opp_id : str
        Salesforce Opportunity ID.
    fields : dict
        Field API names -> new values.

    Returns
    -------
    bool
        True if update succeeded.
    """
    values_parts = []
    for key, val in fields.items():
        safe_val = str(val).replace('"', '\\"').replace("'", "\\'")
        values_parts.append(f'{key}="{safe_val}"')
    values_str = " ".join(values_parts)

    try:
        _run_sf(
            "data", "update", "record",
            "--sobject", "Opportunity",
            "--record-id", opp_id,
            "--values", values_str,
        )
        logger.info("Updated Salesforce Opportunity: %s", opp_id)
        return True
    except Exception as e:
        logger.error("Failed to update Salesforce Opportunity %s: %s", opp_id, e)
        return False


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


def query(soql: str) -> list[dict]:
    """Run a SOQL query and return the records.

    Parameters
    ----------
    soql : str
        SOQL query string.

    Returns
    -------
    list[dict]
        List of record dicts.
    """
    try:
        result = _run_sf("data", "query", "--query", soql)
        return result.get("result", {}).get("records", [])
    except Exception as e:
        logger.error("Salesforce query failed: %s", e)
        return []
