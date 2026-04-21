"""Team-wide CP leaderboard queries.

Derives realized CP per rep by joining `dim_sfdc_opportunities` (CW opps) to
`agg_sfdc_expansion_opportunity_spend` (spend before + max 30d within 90d
post CW), multiplied by product NTR rates.

NTR rates (from config.NTR_RATES):
  Card     0.0095 (95 bps)
  Bill Pay 0.0015 (15 bps)

Travel and Treasury are excluded for now — the spend table's max-30d columns
cover card + bill pay; travel and treasury require different agg windows that
aren't consistently populated for all CW opps in my book.

Quota targets + renewal CP + F2P CP live in post_sales_goals tables that are
gated for READER role. Access request is separate — once granted, layer
targets onto these queries to produce % attainment.

Filter: `normalized_opportunity_owner_role_stamped = 'Sales - AM - Growth'`
matches the Looker "Growth AM IC Detailed Metrics" dashboard scope.
"""

# ── Team leaderboard: realized CP per rep over the last N days ──────────────
TEAM_CP_LEADERBOARD_QUERY = """
WITH cw_opps AS (
    SELECT
        opp.opportunity_id,
        opp.account_id,
        opp.expansion_subtype AS product,
        opp.opportunity_closed_won_date::date AS cw_date,
        opp.opportunity_closed_won_amount_usd AS cw_amount,
        opp.normalized_opportunity_owner AS owner,
        opp.normalized_opportunity_owner_role_stamped AS owner_role
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.opportunity_is_won = TRUE
      AND opp.opportunity_closed_won_date >= CURRENT_DATE - {lookback_days}
      AND opp.expansion_subtype IN ('Card Expansion', 'Bill Pay Expansion',
                                     'Travel Expansion', 'Treasury Expansion')
),
spend AS (
    SELECT
        opportunity_id,
        GREATEST(0, COALESCE(
            expansion_opportunity_max_30_day_transaction_amount_within_90_days_post_closed_won_date
            - expansion_opportunity_30_day_transaction_amount_before_closed_won_date, 0)) AS inc_card,
        GREATEST(0, COALESCE(
            expansion_opportunity_max_30_day_bill_pay_amount_within_90_days_post_closed_won_date
            - expansion_opportunity_30_day_bill_pay_amount_before_closed_won_date, 0)) AS inc_bp
    FROM analytics.marts.agg_sfdc_expansion_opportunity_spend
)
SELECT
    c.owner,
    ROUND(SUM(CASE c.product WHEN 'Card Expansion'     THEN s.inc_card * 0.0095 ELSE 0 END), 0) AS card_cp,
    ROUND(SUM(CASE c.product WHEN 'Bill Pay Expansion' THEN s.inc_bp   * 0.0015 ELSE 0 END), 0) AS bp_cp,
    COUNT(*) AS deals,
    COUNT(CASE WHEN c.product = 'Card Expansion'     THEN 1 END) AS card_deals,
    COUNT(CASE WHEN c.product = 'Bill Pay Expansion' THEN 1 END) AS bp_deals,
    ROUND(
        SUM(CASE c.product WHEN 'Card Expansion'     THEN s.inc_card * 0.0095 ELSE 0 END) +
        SUM(CASE c.product WHEN 'Bill Pay Expansion' THEN s.inc_bp   * 0.0015 ELSE 0 END), 0
    ) AS total_realized_cp
FROM cw_opps c
LEFT JOIN spend s ON s.opportunity_id = c.opportunity_id
WHERE c.owner_role = 'Sales - AM - Growth'
  AND c.owner IS NOT NULL
GROUP BY c.owner
ORDER BY total_realized_cp DESC
"""


# ── Personal top deals: one rep's top-N CW deals with realized CP ───────────
TOP_DEALS_QUERY = """
WITH cw_opps AS (
    SELECT
        opp.opportunity_id,
        opp.account_id,
        opp.opportunity_name,
        opp.expansion_subtype AS product,
        opp.opportunity_closed_won_date::date AS cw_date,
        opp.opportunity_closed_won_amount_usd AS cw_amount,
        opp.normalized_opportunity_owner AS owner
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.opportunity_is_won = TRUE
      AND opp.opportunity_closed_won_date >= CURRENT_DATE - {lookback_days}
      AND opp.expansion_subtype IN ('Card Expansion', 'Bill Pay Expansion',
                                     'Travel Expansion', 'Treasury Expansion')
      AND opp.normalized_opportunity_owner = '{owner_name}'
),
spend AS (
    SELECT
        opportunity_id,
        GREATEST(0, COALESCE(
            expansion_opportunity_max_30_day_transaction_amount_within_90_days_post_closed_won_date
            - expansion_opportunity_30_day_transaction_amount_before_closed_won_date, 0)) AS inc_card,
        GREATEST(0, COALESCE(
            expansion_opportunity_max_30_day_bill_pay_amount_within_90_days_post_closed_won_date
            - expansion_opportunity_30_day_bill_pay_amount_before_closed_won_date, 0)) AS inc_bp
    FROM analytics.marts.agg_sfdc_expansion_opportunity_spend
)
SELECT
    c.opportunity_id,
    c.account_id,
    c.opportunity_name,
    c.product,
    c.cw_date,
    c.cw_amount,
    ROUND(CASE c.product
        WHEN 'Card Expansion'     THEN COALESCE(s.inc_card, 0) * 0.0095
        WHEN 'Bill Pay Expansion' THEN COALESCE(s.inc_bp, 0)   * 0.0015
        ELSE 0 END, 0) AS realized_cp,
    ROUND(COALESCE(s.inc_card, 0), 0) AS inc_card_spend,
    ROUND(COALESCE(s.inc_bp, 0), 0) AS inc_bp_spend
FROM cw_opps c
LEFT JOIN spend s ON s.opportunity_id = c.opportunity_id
ORDER BY realized_cp DESC NULLS LAST
LIMIT {top_n}
"""
