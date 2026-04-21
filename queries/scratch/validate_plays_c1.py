"""C.1 validation — run candidate C plays against Greg's real BoB.

Read-only. Prints row count + top-10 sample per play.
Deferred: P3 (non-USD card spend — still not in BoB view).
"""
import sys
import traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.snowflake_client import run_query

OWNER = "Gregory Nallie"


def safe_run(sql):
    try:
        return run_query(sql), None
    except Exception as e:
        return None, traceback.format_exc()


GREG_CTE = f"""
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '{OWNER}'
)
"""


# ── P2: Free/Legacy with high Plus-feature engagement ────────────────────────
P2 = GREG_CTE + """
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    bob.num_distinct_plus_features_used_prev_3mo AS plus_features_used,
    bob.plus_active_agent_count,
    bob.plus_active_budget_count,
    bob.plus_agent_executions_ltd,
    ROUND(bob.thirty_day_card_spend, 0) AS card_l30d,
    ROUND(bob.estimated_card_cp_monthly, 0) AS est_card_cp_monthly,
    bob.user_count
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE COALESCE(bob.subscription_tier, 'NONE') NOT IN ('SAAS_PLUS', 'SAAS_ENTERPRISE')
  AND COALESCE(bob.num_distinct_plus_features_used_prev_3mo, 0) >= 3
ORDER BY bob.num_distinct_plus_features_used_prev_3mo DESC NULLS LAST
"""

# ── P4: Low cashback tier + high card spend, not on Plus ────────────────────
P4 = GREG_CTE + """
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    bob.rba_earn_rate,
    ROUND(bob.thirty_day_card_spend, 0) AS card_l30d,
    ROUND(bob.card_spend_ltd, 0) AS card_spend_ltd,
    ROUND(bob.estimated_card_cp_monthly, 0) AS est_card_cp_monthly,
    bob.user_count
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE COALESCE(bob.subscription_tier, 'NONE') NOT IN ('SAAS_PLUS', 'SAAS_ENTERPRISE')
  AND COALESCE(bob.thirty_day_card_spend, 0) >= 25000
  AND COALESCE(bob.rba_earn_rate, 0.0185) < 0.015
ORDER BY bob.thirty_day_card_spend DESC NULLS LAST
"""

# ── P6: Legacy Procurement → Add-on upgrade ─────────────────────────────────
# Accounts that USED procurement (subscription tier set) but NOT on the Add-on
P6 = GREG_CTE + """
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    bob.procurement_subscription_tier,
    bob.has_procurement_addon,
    bob.procurement_active,
    bob.procurement_canvas_custom_form_active,
    bob.ltd_procurement_purchase_order_requests_created AS ltd_pos,
    bob.rolling_90_day_procurement_purchase_order_requests_created AS pos_l90d,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE bob.procurement_subscription_tier IS NOT NULL
  AND COALESCE(bob.has_procurement_addon, FALSE) = FALSE
ORDER BY bob.ltd_procurement_purchase_order_requests_created DESC NULLS LAST
"""

# ── P8: Competitor AP / Bill.com migration signal ───────────────────────────
P8 = GREG_CTE + """
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    bob.pom_bill_pay_competitor_name AS bp_competitor,
    ROUND(bob.pom_monthly_off_ramp_bill_pay_spend_in_dollars, 0) AS off_ramp_bp_monthly,
    ROUND(bob.pom_monthly_bill_pay_spend_in_dollars, 0) AS total_bp_monthly,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS ramp_bp_l30d,
    ROUND(bob.estimated_bill_pay_cp_monthly, 0) AS est_bp_cp_monthly,
    SUBSTRING(bob.bill_pay_technographics::STRING, 1, 120) AS bp_tech
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE COALESCE(bob.pom_monthly_off_ramp_bill_pay_spend_in_dollars, 0) >= 10000
  AND COALESCE(bob.is_bill_pay_committed, FALSE) = FALSE
ORDER BY bob.pom_monthly_off_ramp_bill_pay_spend_in_dollars DESC NULLS LAST
"""

# ── P10: Zero-to-one post-CW — recently closed-won but product not activated ─
# Compare CW opps 30-90d ago to current activation status via agg_sfdc_expansion_opportunity_spend
P10 = GREG_CTE + """
,
recent_cw AS (
    SELECT DISTINCT
        opp.account_id,
        opp.sfdc_opportunity_id AS opp_id,
        opp.expansion_subtype AS product,
        opp.opportunity_closed_won_date::date AS cw_date,
        CURRENT_DATE - opp.opportunity_closed_won_date::date AS days_since_cw
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.opportunity_is_won = TRUE
      AND opp.opportunity_closed_won_date >= CURRENT_DATE - 90
      AND opp.opportunity_closed_won_date <= CURRENT_DATE - 30
      AND opp.expansion_subtype IN ('Card Expansion', 'Bill Pay Expansion', 'Treasury Expansion', 'Travel Expansion')
)
SELECT
    rc.account_id,
    bob.account_name,
    rc.product,
    rc.cw_date,
    rc.days_since_cw,
    ROUND(bob.thirty_day_card_spend, 0) AS card_l30d,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d,
    bob.card_fifth_transaction_cleared_at::date AS card_fifth_tx,
    bob.first_bill_paid_at::date AS first_bill_paid_at,
    bob.treasury_first_payment_at::date AS treasury_first_payment,
    ROUND(bob.sfdc_card_spend_new_sale_activation_gap_in_cp, 0) AS card_gap_cp
FROM recent_cw rc
JOIN analytics.marts.dim_book_of_business_accounts_view bob ON bob.sfdc_account_id = rc.account_id
JOIN greg ON greg.account_id = rc.account_id
WHERE (
    (rc.product = 'Card Expansion' AND COALESCE(bob.thirty_day_card_spend, 0) < 1000)
    OR (rc.product = 'Bill Pay Expansion' AND COALESCE(bob.rolling_30_day_paid_bill_amount, 0) < 1000)
    OR (rc.product = 'Treasury Expansion' AND COALESCE(bob.treasury_available_balance, 0) < 1000)
)
ORDER BY rc.cw_date ASC
"""

# ── P11: Large + international — 20+ users + non-US business ────────────────
# Best-effort — only have business_office_country at account level
P11 = GREG_CTE + """
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    bob.user_count,
    bob.ramp_user_count,
    bob.business_office_country,
    bob.can_send_international_payments,
    ROUND(bob.thirty_day_card_spend, 0) AS card_l30d,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d,
    bob.fte_size
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE COALESCE(bob.user_count, 0) >= 20
  AND (
    COALESCE(bob.business_office_country, '') NOT IN ('US', 'USA', 'United States')
    OR bob.can_send_international_payments = TRUE
  )
ORDER BY bob.user_count DESC NULLS LAST
"""

# ── P12: Treasury opp — GLA > $1M + no treasury + no open treasury opp ──────
P12 = GREG_CTE + """
,
open_treasury AS (
    SELECT DISTINCT account_id FROM analytics.marts.dim_sfdc_opportunities
    WHERE expansion_subtype = 'Treasury Expansion' AND opportunity_is_closed = FALSE
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    ROUND(bob.current_gla, 0) AS current_gla,
    ROUND(bob.mercury_gla, 0) AS mercury_gla,
    ROUND(bob.brex_gla, 0) AS brex_gla,
    bob.is_treasury_active,
    bob.is_treasury_committed,
    ROUND(bob.estimated_treasury_cp, 0) AS est_treasury_cp,
    ROUND(bob.prob_attach_score_treasury * 100, 0) AS prob_attach_treasury_pct
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
LEFT JOIN open_treasury ot ON ot.account_id = bob.sfdc_account_id
WHERE COALESCE(bob.current_gla, 0) >= 1000000
  AND COALESCE(bob.is_treasury_active, FALSE) = FALSE
  AND ot.account_id IS NULL
ORDER BY bob.current_gla DESC NULLS LAST
"""


PLAYS = [
    ("P2",  "Free/Legacy w/ high Plus-feature engagement", P2),
    ("P4",  "Low cashback tier + high card spend + not Plus", P4),
    ("P6",  "Legacy Procurement → Add-on upgrade", P6),
    ("P8",  "Competitor AP / Bill.com migration signal", P8),
    ("P10", "Zero-to-one post-CW (activation hasn't started)", P10),
    ("P11", "20+ users + international footprint", P11),
    ("P12", "Treasury opp — GLA >$1M, not on Treasury", P12),
]


for pid, title, sql in PLAYS:
    print(f"\n{'=' * 70}\n{pid}: {title}\n{'=' * 70}")
    df, err = safe_run(sql)
    if err:
        print(f"  ERROR:\n{err}")
        continue
    print(f"  rows: {len(df)}")
    if df is None or df.empty:
        continue
    print(df.drop(columns=[c for c in ["account_id"] if c in df.columns]).head(10).to_string(index=False))
