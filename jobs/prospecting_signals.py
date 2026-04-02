"""Prospecting Signals — surface accounts matching hot plays not contacted in 30+ days.

Runs a single Snowflake query against the Book of Business view, then classifies
accounts into signal plays in Python.  Results are cached per-user with a 4-hour TTL.

Signal Plays:
  1. tts_plus_procurement — High propensity for Plus or Procurement upgrade
  2. high_competitor_spend — Significant off-Ramp card, bill pay, or unmanaged travel spend
  3. low_cashback_no_plus — Low cashback rate (1.0%) and not on Plus
  4. high_gla_grandfathered — High GLA balance, grandfathered but didn't convert to Plus
  5. erp_no_billpay — Integrated ERP but not using Bill Pay
  6. erp_no_plus — Integrated ERP but not on Plus
  7. active_procurement_trial — Active procurement trial, not yet fully converted
"""

import logging
import time
from typing import Optional

from core.snowflake_client import run_query
from queries.queries import format_query

logger = logging.getLogger(__name__)

# ── Cache ────────────────────────────────────────────────────────────────────
_prospect_cache = {}  # user_id -> {"data": [...], "fetched_at": epoch}
_CACHE_TTL = 4 * 3600  # 4 hours


# ── Snowflake Query ──────────────────────────────────────────────────────────
# Single query pulls all fields needed for classification.
# The BoB view (dim_book_of_business_accounts_view) mirrors Growth MCP fields.
_PROSPECTING_QUERY = """
WITH my_accounts AS (
    SELECT DISTINCT account_id
    FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1
      AND owner_name = '__OWNER__'
),
last_comms AS (
    SELECT
        e.account_id,
        MAX(e.sfdc_email_created_at) AS last_email_at,
        MAX(CASE WHEN e.email_direction = 'Outbound' THEN e.sfdc_email_created_at END) AS last_outbound_at
    FROM analytics.marts.dim_emails e
    WHERE e.account_id IN (SELECT account_id FROM my_accounts)
      AND e.sfdc_email_created_at >= DATEADD('day', -90, CURRENT_DATE)
    GROUP BY e.account_id
),
last_calls AS (
    SELECT
        gc.sfdc_primary_account_id AS account_id,
        MAX(gc.gong_call_start) AS last_call_at
    FROM analytics.marts.dim_sfdc_gong_call gc
    WHERE gc.sfdc_primary_account_id IN (SELECT account_id FROM my_accounts)
      AND gc.gong_call_start >= DATEADD('day', -90, CURRENT_DATE)
    GROUP BY gc.sfdc_primary_account_id
),
open_opps AS (
    SELECT DISTINCT opp.account_id
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.account_id IN (SELECT account_id FROM my_accounts)
      AND opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_stage_name != 'S0: Holding'
)
SELECT
    sa.account_id,
    sa.account_name,
    sa.business_id,
    sa.account_status,
    -- Plus / Plan status
    bob.plus_product_status_v2,
    bob.plan,
    -- Product statuses
    bob.bill_pay_product_status,
    bob.card_product_status,
    bob.travel_product_status,
    bob.treasury_product_status,
    -- ERP / Accounting
    bob.accounting_provider_type,
    -- Spend metrics
    bob.thirty_day_card_spend AS card_spend_l30d,
    bob.rolling_30_day_paid_bill_amount AS billpay_spend_l30d,
    bob.rolling_30_days_avg_treasury_available_balance_usd AS treasury_balance_l30d,
    bob.current_gla,
    bob.cashback,
    -- Competitor / off-ramp spend (POM)
    bob.pom_monthly_card_competitor_spend_top1_in_dollars AS competitor_card_spend,
    bob.pom_top_card_competitor_name_top1 AS competitor_card_name,
    bob.pom_monthly_off_ramp_bill_pay_spend_in_dollars AS off_ramp_bp_spend,
    bob.pom_bill_pay_competitor_name AS bp_competitor_name,
    bob.rolling_30_day_unmanaged_travel_txn_amount AS unmanaged_travel_spend,
    -- Procurement
    bob.procurement_active,
    bob.has_procurement_addon,
    -- Propensity scores
    bob.prob_attach_score_bill_pay,
    bob.prob_attach_score_procurement,
    bob.prob_attach_score_ramp_plus,
    bob.prob_attach_score_travel,
    bob.prob_attach_score_treasury,
    -- AE estimated spend (from SFDC account)
    sa.ae_qualified_monthly_spend AS ae_est_card_spend,
    sa.ae_estimated_bill_pay_spend AS ae_est_bp_spend,
    -- Contact info
    bob.primary_customer_poc_email,
    bob.primary_ramp_champion_email,
    bob.primary_ramp_champion_name,
    -- Last touch
    GREATEST(
        COALESCE(lc.last_outbound_at, '2000-01-01'),
        COALESCE(lcall.last_call_at, '2000-01-01')
    )::date AS last_touch_date,
    DATEDIFF('day',
        GREATEST(
            COALESCE(lc.last_outbound_at, '2000-01-01'),
            COALESCE(lcall.last_call_at, '2000-01-01')
        )::date,
        CURRENT_DATE
    ) AS days_since_touch,
    -- Has open opp?
    CASE WHEN oo.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_open_opp
FROM my_accounts ma
JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = ma.account_id
LEFT JOIN analytics.marts.dim_book_of_business_accounts_view bob
    ON bob.sfdc_account_id = sa.account_id
LEFT JOIN last_comms lc ON lc.account_id = sa.account_id
LEFT JOIN last_calls lcall ON lcall.account_id = sa.account_id
LEFT JOIN open_opps oo ON oo.account_id = sa.account_id
WHERE sa.account_status = 'Active'
ORDER BY sa.account_name
"""


# ── Signal play classifiers ─────────────────────────────────────────────────

# ERP providers we care about
_ERP_PROVIDERS = {
    "netsuite", "netsuite rest", "sage", "sage intacct", "acumatica",
    "dynamics bc", "quickbooks", "quickbooks desktop", "quickbooks online",
    "zoho books", "xero", "workday", "oracle fusion",
}


def _safe_float(val, default=0):
    """Convert a value to float, handling None, NaN, and strings."""
    if val is None:
        return default
    try:
        f = float(val)
        if f != f:  # NaN check
            return default
        return f
    except (ValueError, TypeError):
        return default


def _classify_signals(row: dict) -> list:
    """Return list of (signal_key, label, detail) tuples for an account row."""
    signals = []

    plus_status = str(row.get("plus_product_status_v2") or "").lower()
    bp_status = str(row.get("bill_pay_product_status") or "").lower()
    erp = str(row.get("accounting_provider_type") or "").lower()
    cashback = _safe_float(row.get("cashback"))
    gla = _safe_float(row.get("current_gla"))
    competitor_card = _safe_float(row.get("competitor_card_spend"))
    competitor_name = str(row.get("competitor_card_name") or "")
    off_ramp_bp = _safe_float(row.get("off_ramp_bp_spend"))
    unmanaged_travel = _safe_float(row.get("unmanaged_travel_spend"))
    procurement_active = str(row.get("procurement_active") or "").lower()
    has_procurement = bool(row.get("has_procurement_addon"))
    prob_plus = _safe_float(row.get("prob_attach_score_ramp_plus"))
    prob_procurement = _safe_float(row.get("prob_attach_score_procurement"))

    is_plus = plus_status == "active"
    is_grandfathered = "grandfathered" in plus_status
    has_erp = erp in _ERP_PROVIDERS
    bp_l30d = _safe_float(row.get("billpay_spend_l30d"))
    # Trust actual spend over status field — "churned" accounts with $200K/mo BP are not churned
    bp_active = bp_status == "active" or bp_l30d >= 5000

    # 1. TTS Plus or Procurement (high propensity, not already on it)
    if not is_plus and (prob_plus >= 40 or prob_procurement >= 50):
        detail_parts = []
        if prob_plus >= 40:
            detail_parts.append(f"Plus propensity: {prob_plus:.0f}%")
        if prob_procurement >= 50:
            detail_parts.append(f"Procurement propensity: {prob_procurement:.0f}%")
        signals.append(("tts_plus_procurement", "TTS Plus / Procurement", " · ".join(detail_parts)))

    # 2. High competitor spend
    total_competitor = competitor_card + off_ramp_bp + unmanaged_travel
    if total_competitor >= 5000:
        parts = []
        if competitor_card >= 2000:
            parts.append(f"${competitor_card:,.0f}/mo on {competitor_name or 'competitor cards'}")
        if off_ramp_bp >= 2000:
            parts.append(f"${off_ramp_bp:,.0f}/mo off-ramp bill pay")
        if unmanaged_travel >= 1000:
            parts.append(f"${unmanaged_travel:,.0f}/mo unmanaged travel")
        signals.append(("high_competitor_spend", "High Competitor Spend", " · ".join(parts)))

    # 3. Low cashback, not on Plus
    if cashback and cashback <= 1.0 and not is_plus:
        signals.append(("low_cashback_no_plus", "Low Cashback / No Plus", f"Cashback: {cashback}% · Plus: {plus_status}"))

    # 4. High GLA, grandfathered
    if gla >= 500000 and is_grandfathered:
        signals.append(("high_gla_grandfathered", "High GLA / Grandfathered", f"GLA: ${gla:,.0f} · {plus_status}"))

    # 5. Integrated ERP but no Bill Pay
    if has_erp and not bp_active:
        signals.append(("erp_no_billpay", "ERP Integrated / No Bill Pay", f"ERP: {erp.title()} · Bill Pay: {bp_status}"))

    # 6. Integrated ERP but not on Plus
    if has_erp and not is_plus:
        signals.append(("erp_no_plus", "ERP Integrated / No Plus", f"ERP: {erp.title()} · Plus: {plus_status}"))

    # 7. Active procurement trial
    if procurement_active == "yes" and not has_procurement:
        signals.append(("active_procurement_trial", "Active Procurement Trial", "Trialing procurement — conversion opportunity"))

    return signals


# ── Signal labels + emoji ────────────────────────────────────────────────────
SIGNAL_META = {
    "tts_plus_procurement": {"emoji": ":rocket:", "label": "TTS Plus / Procurement"},
    "high_competitor_spend": {"emoji": ":crossed_swords:", "label": "High Competitor Spend"},
    "low_cashback_no_plus": {"emoji": ":money_with_wings:", "label": "Low Cashback / No Plus"},
    "high_gla_grandfathered": {"emoji": ":bank:", "label": "High GLA / Grandfathered"},
    "erp_no_billpay": {"emoji": ":ledger:", "label": "ERP / No Bill Pay"},
    "erp_no_plus": {"emoji": ":electric_plug:", "label": "ERP / No Plus"},
    "active_procurement_trial": {"emoji": ":shopping_trolley:", "label": "Active Procurement Trial"},
    # Phase 2: Activation alerts (populated by activation_alerts.py)
    "new_treasury": {"emoji": ":bank:", "label": "New Treasury Activation"},
    "new_investment": {"emoji": ":chart_with_upwards_trend:", "label": "Investment Account Opened"},
    "first_bill": {"emoji": ":receipt:", "label": "First Bill Created"},
}

# Min days since last outbound touch to qualify as "untouched"
MIN_DAYS_UNTOUCHED = 30


def gather_prospecting_signals(user_id: Optional[str] = None, force: bool = False) -> list:
    """Run the prospecting query and classify accounts into signal plays.

    Returns a list of dicts:
        {account, account_id, business_id, signal_key, signal_label,
         signal_detail, days_since_touch, card_spend_l30d, ...}

    Results are cached per-user with a 4-hour TTL.
    """
    uid = user_id or "default"

    # Check cache
    if not force and uid in _prospect_cache:
        entry = _prospect_cache[uid]
        if time.time() - entry["fetched_at"] < _CACHE_TTL:
            return entry["data"]

    logger.info("Prospecting signals: running query for %s", uid)

    try:
        sql = format_query(_PROSPECTING_QUERY, user_id=user_id)
        df = run_query(sql)
    except Exception as e:
        logger.error("Prospecting query failed: %s", e)
        # Return stale cache if available
        if uid in _prospect_cache:
            return _prospect_cache[uid]["data"]
        return []

    if df.empty:
        _prospect_cache[uid] = {"data": [], "fetched_at": time.time()}
        return []

    results = []
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        days = row_dict.get("days_since_touch") or 999

        # Must not have been contacted in MIN_DAYS_UNTOUCHED days
        if days < MIN_DAYS_UNTOUCHED:
            continue

        signals = _classify_signals(row_dict)
        if not signals:
            continue

        for signal_key, signal_label, signal_detail in signals:
            results.append({
                "account": row_dict.get("account_name", "Unknown"),
                "account_id": row_dict.get("account_id", ""),
                "business_id": row_dict.get("business_id", ""),
                "signal_key": signal_key,
                "signal_label": signal_label,
                "signal_detail": signal_detail,
                "days_since_touch": int(days),
                "has_open_opp": bool(row_dict.get("has_open_opp")),
                "card_spend_l30d": _safe_float(row_dict.get("card_spend_l30d")),
                "billpay_spend_l30d": _safe_float(row_dict.get("billpay_spend_l30d")),
                "treasury_balance_l30d": _safe_float(row_dict.get("treasury_balance_l30d")),
                "cashback": _safe_float(row_dict.get("cashback")),
                "current_gla": _safe_float(row_dict.get("current_gla")),
                "competitor_card_spend": _safe_float(row_dict.get("competitor_card_spend")),
                "competitor_card_name": str(row_dict.get("competitor_card_name") or ""),
                "off_ramp_bp_spend": _safe_float(row_dict.get("off_ramp_bp_spend")),
                "bp_competitor_name": str(row_dict.get("bp_competitor_name") or ""),
                "unmanaged_travel_spend": _safe_float(row_dict.get("unmanaged_travel_spend")),
                "ae_est_card_spend": _safe_float(row_dict.get("ae_est_card_spend")),
                "ae_est_bp_spend": _safe_float(row_dict.get("ae_est_bp_spend")),
                "erp": row_dict.get("accounting_provider_type") or "",
                "plus_status": row_dict.get("plus_product_status_v2") or "",
                "poc_email": row_dict.get("primary_customer_poc_email") or "",
                "champion_email": row_dict.get("primary_ramp_champion_email") or "",
                "champion_name": row_dict.get("primary_ramp_champion_name") or "",
            })

    # Cap days_since_touch display at 999 (accounts never contacted show as "999+")
    for r in results:
        if r["days_since_touch"] > 999:
            r["days_since_touch"] = 999

    # Sort by signal_key, then by card_spend + competitor spend descending (highest value first)
    def _sort_key(r):
        value_score = (
            r.get("card_spend_l30d", 0)
            + r.get("billpay_spend_l30d", 0) * 0.15 / 0.0095  # normalize to card-equivalent
            + r.get("current_gla", 0) * 0.0005 / 0.0095
        )
        return (r["signal_key"], -value_score)

    results.sort(key=_sort_key)

    # Cap at 15 per signal type to keep the list manageable
    _MAX_PER_SIGNAL = 15
    from collections import Counter
    signal_counts = Counter()
    capped = []
    for r in results:
        key = r["signal_key"]
        if signal_counts[key] < _MAX_PER_SIGNAL:
            capped.append(r)
            signal_counts[key] += 1
    results = capped

    # Merge in activation alerts (Phase 2: treasury, investment, first bill)
    try:
        from jobs.activation_alerts import detect_activations
        activations = detect_activations(user_id=user_id)
        if activations:
            # Add days_since_touch and has_open_opp defaults for compatibility
            for a in activations:
                a.setdefault("days_since_touch", 0)
                a.setdefault("has_open_opp", False)
            results.extend(activations)
            logger.info("Merged %d activation alerts into prospects", len(activations))
    except Exception as e:
        logger.warning("Failed to merge activation alerts: %s", e)

    _prospect_cache[uid] = {"data": results, "fetched_at": time.time()}
    logger.info("Prospecting signals: %d results for %s", len(results), uid)
    return results


def get_cached_prospects(user_id: Optional[str] = None) -> list:
    """Return cached prospects without triggering a refresh."""
    uid = user_id or "default"
    entry = _prospect_cache.get(uid)
    return entry["data"] if entry else []


def run_prospecting_refresh(client, user_id: Optional[str] = None):
    """Scheduled job entry point: refresh prospects and DM user if new signals found."""
    results = gather_prospecting_signals(user_id=user_id, force=True)
    if results:
        # Group by signal for summary
        from collections import Counter
        counts = Counter(r["signal_key"] for r in results)
        summary_parts = []
        for key, count in counts.most_common():
            meta = SIGNAL_META.get(key, {})
            emoji = meta.get("emoji", ":mag:")
            label = meta.get("label", key)
            summary_parts.append(f"{emoji} *{label}*: {count}")

        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": (
                f":mag: *Prospecting Signals Refreshed*\n"
                f"{len(results)} accounts across {len(counts)} signal plays "
                f"haven't been contacted in {MIN_DAYS_UNTOUCHED}+ days:"
            )}},
            {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(summary_parts)}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": "_Open the :house: Home tab → Prospecting to see details and draft emails_"}]},
        ]
        try:
            dm_target = user_id or "U06DAFU4YRG"
            client.chat_postMessage(channel=dm_target, blocks=blocks, text="Prospecting signals refreshed")
        except Exception as e:
            logger.error("Failed to DM prospecting summary: %s", e)
