"""Account and contact resolution utilities.

Provides helpers for looking up SFDC accounts by name, fetching associated
contacts, filtering out hash-like placeholder names, and fuzzy-matching
a point-of-contact name against a list of SFDC contacts.
"""
from __future__ import annotations

import re
import logging

import pandas as pd

from config import OWNER_NAME
from core.snowflake_client import run_query

logger = logging.getLogger(__name__)


# ── Account lookup ────────────────────────────────────────────────────────


def resolve_account_name(conn, business_name: str) -> dict | None:
    """Search ``dim_sfdc_accounts`` for an account whose name matches
    *business_name* (case-insensitive LIKE).

    Returns ``{account_id, account_name, business_id}`` for the first match,
    or ``None`` if nothing is found.
    """
    if not business_name or not business_name.strip():
        return None

    safe_name = business_name.strip().replace("'", "''")

    # Strategy 1: Exact substring match (ILIKE)
    query = f"""
    SELECT sa.account_id, sa.account_name, sa.business_id
    FROM analytics.marts.dim_sfdc_accounts sa
    WHERE sa.account_name ILIKE '%{safe_name}%'
      AND sa.account_status = 'Active'
      AND sa.account_id IN (
          SELECT DISTINCT account_id
          FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
          WHERE date_day = CURRENT_DATE - 1
            AND owner_name = '{OWNER_NAME}'
      )
    LIMIT 1
    """
    try:
        df = run_query(query)
        if df.empty:
            # Strategy 2: Fuzzy match using Snowflake JAROWINKLER_SIMILARITY
            logger.debug("resolve_account_name: no exact match for '%s', trying fuzzy", business_name)
            fuzzy_query = f"""
            SELECT sa.account_id, sa.account_name, sa.business_id,
                   JAROWINKLER_SIMILARITY(LOWER(sa.account_name), LOWER('{safe_name}')) AS score
            FROM analytics.marts.dim_sfdc_accounts sa
            WHERE sa.account_status = 'Active'
              AND sa.account_id IN (
                  SELECT DISTINCT account_id
                  FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
                  WHERE date_day = CURRENT_DATE - 1
                    AND owner_name = '{OWNER_NAME}'
              )
              AND JAROWINKLER_SIMILARITY(LOWER(sa.account_name), LOWER('{safe_name}')) >= 70
            ORDER BY score DESC
            LIMIT 1
            """
            df = run_query(fuzzy_query)
            if df.empty:
                logger.debug("resolve_account_name: no fuzzy match for '%s'", business_name)
                return None
            logger.info("resolve_account_name: fuzzy matched '%s' → '%s' (score=%s)",
                        business_name, df.iloc[0]["account_name"],
                        df.iloc[0].get("score", "?"))
        row = df.iloc[0]
        return {
            "account_id": row["account_id"],
            "account_name": row["account_name"],
            "business_id": row.get("business_id", ""),
        }
    except Exception as exc:
        logger.warning("resolve_account_name query failed: %s", exc)
        return None


# ── Contact fetching ──────────────────────────────────────────────────────


def fetch_contact_emails(conn, account_ids: list[str]) -> dict:
    """Fetch SFDC contacts for the given *account_ids*.

    Returns ``{account_id: [{name, email, title}, ...], ...}``.

    The *conn* parameter is kept for API compatibility but is ignored;
    all queries go through ``run_query()`` (CLI-based auth).

    Strategy (ported from app.py ``_try_fetch_contact_emails``):
    1. Try known tables first (fast path).
    2. Auto-discover additional contact tables via ``information_schema``.
    3. For each candidate table, probe columns to find name/email/title,
       query contacts, and skip hash-like placeholder names.
    """
    if not account_ids:
        return {}

    ids_str = ",".join(f"'{aid}'" for aid in account_ids)

    candidate_tables = [
        "analytics.marts.dim_sfdc_contacts",
        "analytics.marts.dim_contacts",
    ]

    # Auto-discover additional contact tables ---------------------------------
    try:
        discovery_query = """
        SELECT table_schema || '.' || table_name AS full_table
        FROM analytics.information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND (
              LOWER(table_name) LIKE '%sfdc%contact%'
              OR LOWER(table_name) LIKE '%salesforce%contact%'
              OR LOWER(table_name) LIKE '%dim_contact%'
          )
        """
        disc_df = run_query(discovery_query)
        if not disc_df.empty:
            for _, row in disc_df.iterrows():
                full_name = f"analytics.{row.iloc[0]}"
                if full_name not in candidate_tables:
                    candidate_tables.append(full_name)
    except Exception as exc:
        logger.debug("Contact table auto-discovery failed: %s", exc)

    # Probe & query each candidate table --------------------------------------
    def _try_table(table: str) -> dict | None:
        """Attempt to fetch contacts from *table*.  Returns result dict or None."""
        table_parts = table.split(".")
        table_name_upper = table_parts[-1].upper()
        schema_upper = table_parts[-2].upper() if len(table_parts) >= 2 else "MARTS"
        db_prefix = table_parts[0] if len(table_parts) >= 3 else "analytics"

        col_query = (
            f"SELECT column_name FROM {db_prefix}.information_schema.columns "
            f"WHERE table_name = '{table_name_upper}' AND table_schema = '{schema_upper}'"
        )
        col_df = run_query(col_query)
        if col_df.empty:
            return None
        cols = {c.lower() for c in col_df.iloc[:, 0].tolist()}

        # Must have account_id
        if "account_id" not in cols:
            return None

        # Discover name / email / title columns
        name_candidates = [c for c in [
            "contact_name", "name", "full_name", "display_name",
        ] if c in cols]
        email_candidates = [c for c in [
            "email", "contact_email", "email_address",
        ] if c in cols]
        title_candidates = [c for c in [
            "title", "contact_title", "job_title",
        ] if c in cols]

        if not name_candidates or not email_candidates:
            return None

        name_expr = f"COALESCE({','.join(name_candidates)}, '')"
        email_expr = f"COALESCE({','.join(email_candidates)}, '')"
        title_expr = (
            f"COALESCE({','.join(title_candidates)}, '')"
            if title_candidates
            else "''"
        )

        # Activity / sort columns (most recently active first)
        activity_candidates = [c for c in [
            "last_activity_date", "contact_last_activity_date",
            "last_modified_date", "contact_last_modified_date",
        ] if c in cols]
        created_candidates = [c for c in [
            "created_date", "contact_created_date", "created_at",
        ] if c in cols]
        sort_col = (
            activity_candidates[0]
            if activity_candidates
            else (created_candidates[0] if created_candidates else None)
        )
        sort_expr = f"{sort_col} DESC NULLS LAST" if sort_col else "1"

        # Active / deleted filters
        where_parts = [f"account_id IN ({ids_str})", f"{name_expr} != ''"]
        if "is_deleted" in cols:
            where_parts.append("is_deleted = FALSE")
        elif "contact_is_deleted" in cols:
            where_parts.append("contact_is_deleted = FALSE")
        where_str = " AND ".join(where_parts)

        query = f"""
        SELECT account_id,
               {name_expr} AS contact_name,
               {email_expr} AS contact_email,
               {title_expr} AS contact_title
        FROM {table}
        WHERE {where_str}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY account_id, {email_expr}
            ORDER BY {sort_expr}
        ) = 1
        ORDER BY account_id, {sort_expr}
        """
        df = run_query(query)
        if df.empty:
            return None

        result: dict[str, list[dict]] = {}
        for _, r in df.iterrows():
            aid = r["account_id"]
            contact_name = (r.get("contact_name", "") or "").strip()

            # Skip hash-like placeholder names
            if is_hash_like(contact_name):
                continue

            if aid not in result:
                result[aid] = []
            if len(result[aid]) < 10:
                result[aid].append({
                    "name": contact_name,
                    "email": (r.get("contact_email", "") or "").strip(),
                    "title": (r.get("contact_title", "") or "").strip(),
                })
        return result if result else None

    # Iterate through candidate tables ----------------------------------------
    for table in candidate_tables:
        try:
            result = _try_table(table)
            if result:
                logger.info(
                    "SFDC contacts fetched from %s (%d accounts)",
                    table,
                    len(result),
                )
                return result
        except Exception as exc:
            logger.debug("Contact fetch from %s failed: %s", table, exc)
            continue

    return {}


# ── Hash detection ────────────────────────────────────────────────────────


def is_hash_like(s: str) -> bool:
    """Return True if *s* looks like a SHA-256 or other hex hash rather than
    a real name.  Matches strings of 32-128 hex characters."""
    if not s or len(s) < 32:
        return False
    return bool(re.fullmatch(r'[0-9a-fA-F]{32,128}', s.strip()))


def best_contact_match(poc_name: str, contacts: list[dict]) -> dict | None:
    """Return the contact from *contacts* whose name is the closest match
    to *poc_name*, or ``None`` if nothing is close enough.

    Uses simple substring matching — good enough for "Sarah Chen" →
    ``{name: "Sarah Chen", email: "sarah@acme.com", ...}``.
    """
    if not poc_name or not contacts:
        return None

    poc_lower = poc_name.lower().strip()

    # Exact match first
    for c in contacts:
        if c.get("name", "").lower().strip() == poc_lower:
            return c

    # Substring match
    for c in contacts:
        c_name = c.get("name", "").lower().strip()
        if poc_lower in c_name or c_name in poc_lower:
            return c

    # Last-name match
    poc_parts = poc_lower.split()
    if poc_parts:
        last = poc_parts[-1]
        for c in contacts:
            if last in c.get("name", "").lower():
                return c

    return None
