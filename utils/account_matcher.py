"""Robust SFDC account matching for post-meeting and alert pipelines.

Ensures we only match to:
  - Active accounts in Greg's book of business
  - NOT prospects or accounts owned by someone else
  - Checks for recently closed opps to prevent duplicate opp creation

All SFDC queries go through Snowflake (the source of truth for Greg's book).
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta

from config import OWNER_NAME
from core.snowflake_client import run_query

logger = logging.getLogger(__name__)

# Greg's SFDC user ID
GREG_SFDC_USER_ID = "0056g000006oJIDAA2"

# Recently closed opp window — don't suggest creating if one was CW'd within N days
RECENTLY_CLOSED_DAYS = 90


# ── Account Matching ─────────────────────────────────────────────────────────

_ACCOUNT_MATCH_QUERY = """
SELECT
    sa.account_id,
    sa.account_name,
    sa.account_status,
    COALESCE(ledger.owner_name, '') AS owner_name,
    COALESCE(opp_counts.open_opp_count, 0) AS open_opp_count
FROM analytics.marts.dim_sfdc_accounts sa
LEFT JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
    ON ledger.account_id = sa.account_id
    AND ledger.date_day = CURRENT_DATE - 1
LEFT JOIN (
    SELECT account_id, COUNT(*) AS open_opp_count
    FROM analytics.marts.dim_sfdc_opportunities
    WHERE opportunity_is_closed = FALSE
      AND opportunity_stage_name != 'S0: Holding'
    GROUP BY account_id
) opp_counts ON opp_counts.account_id = sa.account_id
WHERE (sa.account_name ILIKE '%{search_term}%'
   OR sa.account_id IN (
       SELECT DISTINCT c.account_id
       FROM analytics.marts.dim_sfdc_contacts c
       WHERE c.contact_email ILIKE '%{domain}%'
   ))
  AND sa.account_status = 'Active'
LIMIT 10
"""

# Fallback: search account name by email domain slug (e.g., "americanaccordfood" from the email)
_ACCOUNT_MATCH_BY_DOMAIN_NAME_QUERY = """
SELECT
    sa.account_id,
    sa.account_name,
    sa.account_status,
    COALESCE(ledger.owner_name, '') AS owner_name,
    COALESCE(opp_counts.open_opp_count, 0) AS open_opp_count
FROM analytics.marts.dim_sfdc_accounts sa
LEFT JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
    ON ledger.account_id = sa.account_id
    AND ledger.date_day = CURRENT_DATE - 1
LEFT JOIN (
    SELECT account_id, COUNT(*) AS open_opp_count
    FROM analytics.marts.dim_sfdc_opportunities
    WHERE opportunity_is_closed = FALSE
      AND opportunity_stage_name != 'S0: Holding'
    GROUP BY account_id
) opp_counts ON opp_counts.account_id = sa.account_id
WHERE sa.account_name ILIKE '%{domain_slug}%'
  AND sa.account_status = 'Active'
LIMIT 10
"""

_ACCOUNT_BY_ID_QUERY = """
SELECT
    sa.account_id,
    sa.account_name,
    sa.account_status,
    COALESCE(ledger.owner_name, '') AS owner_name
FROM analytics.marts.dim_sfdc_accounts sa
LEFT JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
    ON ledger.account_id = sa.account_id
    AND ledger.date_day = CURRENT_DATE - 1
WHERE sa.account_id = '{account_id}'
LIMIT 1
"""

_OPEN_OPPS_QUERY = """
SELECT
    opp.opportunity_id AS opp_id,
    opp.opportunity_name AS opp_name,
    opp.opportunity_stage_name AS stage,
    opp.expansion_subtype AS product,
    opp.opportunity_type AS expansion_type,
    opp.opportunity_owner AS owner_name,
    opp.opportunity_close_date AS close_date,
    opp.monthly_expansion_amount AS amount
FROM analytics.marts.dim_sfdc_opportunities opp
WHERE opp.account_id = '{account_id}'
  AND opp.opportunity_is_closed = FALSE
ORDER BY opp.opportunity_close_date ASC
"""

_RECENTLY_CLOSED_OPPS_QUERY = """
SELECT
    opp.opportunity_id AS opp_id,
    opp.opportunity_name AS opp_name,
    opp.opportunity_stage_name AS stage,
    opp.expansion_subtype AS product,
    opp.opportunity_type AS expansion_type,
    opp.opportunity_owner AS owner_name,
    opp.opportunity_close_date AS close_date,
    opp.monthly_expansion_amount AS amount
FROM analytics.marts.dim_sfdc_opportunities opp
WHERE opp.account_id = '{account_id}'
  AND opp.opportunity_is_closed = TRUE
  AND opp.opportunity_stage_name LIKE 'S5%'
  AND opp.opportunity_close_date >= '{cutoff_date}'
ORDER BY opp.opportunity_close_date DESC
"""


class MatchResult:
    """Result of an account match attempt."""

    def __init__(self):
        self.matched = False
        self.account_id = ""
        self.account_name = ""
        self.owner_id = ""
        self.owner_name = ""
        self.is_gregs_book = False
        self.account_status = ""
        self.segment = ""
        self.open_opps: list[dict] = []
        self.recently_closed_opps: list[dict] = []
        self.warnings: list[str] = []

    @property
    def safe_to_create_opp(self) -> bool:
        """True if it's safe to suggest opp creation (Greg's active account)."""
        return self.matched and self.is_gregs_book and self.account_status == "Active"

    def has_open_opp_for_product(self, product: str) -> dict | None:
        """Check if an open opp already exists for a product.

        Returns the opp dict if found, None otherwise.
        Matches on Expansion_Product__c or expansion_type.
        """
        product_lower = product.lower().strip()
        # Normalize common product names
        product_aliases = {
            "card": ["card"],
            "bill pay": ["bill pay", "bill_pay"],
            "treasury": ["treasury"],
            "travel": ["travel"],
            "saas": ["saas", "ramp plus"],
            "procurement": ["procurement"],
        }
        search_terms = product_aliases.get(product_lower, [product_lower])

        for opp in self.open_opps:
            opp_product = (opp.get("product") or "").lower()
            opp_type = (opp.get("expansion_type") or "").lower()
            for term in search_terms:
                if term in opp_product or term in opp_type:
                    return opp
        return None

    def has_recently_closed_opp_for_product(self, product: str) -> dict | None:
        """Check if an opp for this product was recently closed-won.

        Returns the opp dict if found, None otherwise.
        """
        product_lower = product.lower().strip()
        product_aliases = {
            "card": ["card"],
            "bill pay": ["bill pay", "bill_pay"],
            "treasury": ["treasury"],
            "travel": ["travel"],
            "saas": ["saas", "ramp plus"],
            "procurement": ["procurement"],
        }
        search_terms = product_aliases.get(product_lower, [product_lower])

        for opp in self.recently_closed_opps:
            opp_product = (opp.get("product") or "").lower()
            opp_type = (opp.get("expansion_type") or "").lower()
            for term in search_terms:
                if term in opp_product or term in opp_type:
                    return opp
        return None

    def format_warnings_slack(self) -> str:
        """Format warnings as Slack mrkdwn."""
        if not self.warnings:
            return ""
        return "\n".join(f":warning: {w}" for w in self.warnings)


def match_account(
    account_name: str = "",
    account_id: str = "",
    domain: str = "",
    participant_emails: list[str] | None = None,
) -> MatchResult:
    """Match an account from meeting/alert context to SFDC.

    Tries in order:
      1. Direct account_id lookup (most reliable)
      2. Account name fuzzy match
      3. Email domain match via contacts

    Validates ownership, status, and fetches open + recently closed opps.
    """
    result = MatchResult()

    try:
        # ── Step 1: Direct ID lookup ─────────────────────────────────────
        if account_id:
            df = run_query(_ACCOUNT_BY_ID_QUERY.format(account_id=account_id))
            if not df.empty:
                row = df.iloc[0]
                _populate_result(result, row)
            else:
                result.warnings.append(f"Account ID {account_id} not found in growth universe.")

        # ── Step 2: Name/domain search ───────────────────────────────────
        if not result.matched and (account_name or domain or participant_emails):
            search_term = account_name.replace("'", "''") if account_name else ""
            if not domain and participant_emails:
                # Extract domain from first external email
                for email in participant_emails:
                    if "@" in email and not email.endswith("@ramp.com"):
                        domain = email.split("@")[1]
                        break

            search_domain = (domain or "").replace("'", "''")

            # Pass 2a: Name + contact email domain (original query)
            if search_term or search_domain:
                df = run_query(_ACCOUNT_MATCH_QUERY.format(
                    search_term=search_term or "NOMATCH",
                    domain=search_domain or "NOMATCH",
                ))
                if not df.empty:
                    _pick_best_match(result, df, account_name)

            # Pass 2b: Search account name by email domain slug
            # e.g., "americanaccordfood.com" → search for "americanaccordfood" in account names
            if not result.matched and search_domain:
                domain_slug = search_domain.split(".")[0].replace("'", "''")
                if len(domain_slug) >= 4:  # Avoid overly short slugs
                    df = run_query(_ACCOUNT_MATCH_BY_DOMAIN_NAME_QUERY.format(
                        domain_slug=domain_slug,
                    ))
                    if not df.empty:
                        _pick_best_match(result, df, account_name)
                        if result.matched:
                            logger.info(
                                "Matched account '%s' via email domain slug '%s'",
                                result.account_name, domain_slug,
                            )

            # Pass 2c: Try individual significant words from account name
            # e.g., "American Accord Food Corp" → try "Accord", "American Accord"
            if not result.matched and account_name:
                words = [w for w in account_name.split() if len(w) >= 4
                         and w.lower() not in {"corp", "inc", "llc", "ltd", "the",
                                                "and", "group", "company", "services",
                                                "ramp", "international", "partners"}]
                # Try pairs of adjacent words, then individual words
                tried = {account_name.lower()}
                for combo in _name_fragments(words):
                    combo_lower = combo.lower()
                    if combo_lower in tried:
                        continue
                    tried.add(combo_lower)
                    safe_combo = combo.replace("'", "''")
                    df = run_query(_ACCOUNT_MATCH_BY_DOMAIN_NAME_QUERY.format(
                        domain_slug=safe_combo,
                    ))
                    if not df.empty:
                        _pick_best_match(result, df, account_name)
                        if result.matched:
                            logger.info(
                                "Matched account '%s' via name fragment '%s'",
                                result.account_name, combo,
                            )
                            break

        # ── Step 3: Fetch open opps ──────────────────────────────────────
        if result.matched:
            _fetch_opps(result)

        # ── Step 4: Ownership warnings ───────────────────────────────────
        if result.matched and not result.is_gregs_book:
            result.warnings.append(
                f"Account owned by {result.owner_name} (not in your book). "
                f"Verify before creating/updating opps."
            )

    except Exception as exc:
        logger.error("Account matching failed: %s", exc)
        result.warnings.append(f"Account matching error: {exc}")

    return result


def _pick_best_match(result: MatchResult, df, account_name: str = "") -> None:
    """Pick the best row from a multi-row result set and populate the MatchResult.

    Priority order:
    1. Exact account name match
    2. Greg's book + has open opps (strongest active-relationship signal)
    3. Greg's book (no open opps)
    4. First result (last resort)
    """
    best = None
    greg_with_opps = None
    greg_no_opps = None

    for _, row in df.iterrows():
        name = (row.get("account_name") or "").lower()
        owner = str(row.get("owner_name") or "")
        open_opps = int(row.get("open_opp_count", 0) or 0)

        # 1. Exact name match always wins
        if account_name and name == account_name.lower():
            best = row
            break

        # 2/3. Greg's accounts — prefer ones with open opps
        if owner == OWNER_NAME:
            if open_opps > 0 and greg_with_opps is None:
                greg_with_opps = row
            elif greg_no_opps is None:
                greg_no_opps = row

    if best is None:
        best = greg_with_opps or greg_no_opps or df.iloc[0]

    _populate_result(result, best)

    # Warn about multiple matches
    if len(df) > 1:
        names = df["account_name"].tolist()[:3]
        result.warnings.append(
            f"Multiple matches found: {', '.join(str(n) for n in names)}. "
            f"Matched to: {result.account_name}"
        )


def _name_fragments(words: list[str]) -> list[str]:
    """Generate search fragments from a list of significant words.

    Returns adjacent pairs first (more specific), then individual words.
    """
    fragments = []
    # Adjacent pairs
    for i in range(len(words) - 1):
        fragments.append(f"{words[i]} {words[i+1]}")
    # Individual words (longest first — more distinctive)
    for w in sorted(words, key=len, reverse=True):
        if len(w) >= 5:  # Only try reasonably distinctive words
            fragments.append(w)
    return fragments


def _populate_result(result: MatchResult, row) -> None:
    """Populate a MatchResult from a Snowflake row."""
    result.matched = True
    result.account_id = str(row.get("account_id") or "")
    result.account_name = str(row.get("account_name") or "")
    result.owner_name = str(row.get("owner_name") or "")
    result.owner_id = GREG_SFDC_USER_ID if result.owner_name == OWNER_NAME else ""
    result.account_status = str(row.get("account_status") or "")
    result.segment = str(row.get("segment") or "")
    result.is_gregs_book = result.owner_name == OWNER_NAME


def _fetch_opps(result: MatchResult) -> None:
    """Fetch open and recently closed opps for a matched account."""
    # Open opps
    try:
        df = run_query(_OPEN_OPPS_QUERY.format(account_id=result.account_id))
        if not df.empty:
            result.open_opps = df.to_dict("records")
    except Exception as exc:
        logger.warning("Failed to fetch open opps for %s: %s", result.account_id, exc)

    # Recently closed opps
    cutoff = (datetime.utcnow() - timedelta(days=RECENTLY_CLOSED_DAYS)).strftime("%Y-%m-%d")
    try:
        df = run_query(_RECENTLY_CLOSED_OPPS_QUERY.format(
            account_id=result.account_id,
            cutoff_date=cutoff,
        ))
        if not df.empty:
            result.recently_closed_opps = df.to_dict("records")
    except Exception as exc:
        logger.warning("Failed to fetch recently closed opps for %s: %s", result.account_id, exc)


def validate_opp_action(
    match: MatchResult,
    suggested_product: str,
) -> dict:
    """Determine the right opp action given the account match and product.

    Returns a dict with:
      - action: "create" | "update" | "skip"
      - reason: human-readable explanation
      - existing_opp: the existing opp dict (if updating)
      - warnings: list of warning strings
    """
    warnings = list(match.warnings)

    # Not matched at all
    if not match.matched:
        return {
            "action": "skip",
            "reason": "Could not match to a Salesforce account.",
            "existing_opp": None,
            "warnings": warnings,
        }

    # Not in Greg's book
    if not match.is_gregs_book:
        return {
            "action": "skip",
            "reason": f"Account owned by {match.owner_name} — not in your book.",
            "existing_opp": None,
            "warnings": warnings,
        }

    # Check for existing open opp for this product
    existing = match.has_open_opp_for_product(suggested_product)
    if existing:
        # Check if opp is owned by Greg
        opp_owner = str(existing.get("owner_name") or "")
        if opp_owner and opp_owner != OWNER_NAME:
            warnings.append(
                f"Existing {suggested_product} opp is owned by {opp_owner} "
                f"(not you). Verify before updating."
            )
        return {
            "action": "update",
            "reason": f"Open {suggested_product} opp already exists at {existing.get('stage', '?')}.",
            "existing_opp": existing,
            "warnings": warnings,
        }

    # Check for recently closed opp for this product
    recently_closed = match.has_recently_closed_opp_for_product(suggested_product)
    if recently_closed:
        close_date = recently_closed.get("close_date", "?")
        return {
            "action": "skip",
            "reason": (
                f"A {suggested_product} opp was recently closed-won ({close_date}). "
                f"No new opp needed."
            ),
            "existing_opp": recently_closed,
            "warnings": warnings,
        }

    # Safe to create
    return {
        "action": "create",
        "reason": f"No open or recently closed {suggested_product} opp found.",
        "existing_opp": None,
        "warnings": warnings,
    }
