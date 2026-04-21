"""Play catalog — reusable multi-account outbound signals for the Plays sub-section.

Each play surfaces a different book-of-business slice based on product fit or
activation gap. Validated against Greg's real book in B.1 + C.1 (see
`queries/scratch/validate_plays_b1_final.py` and `validate_plays_c1.py`) —
final thresholds baked in here.

All queries use the __OWNER__ sentinel so they work multi-user via
`queries.queries.format_query(query, user_id=user_id)`.

Shape: `PLAYS` is an ordered dict keyed by play_id. Each entry carries:
  - title, criteria, icon, pitch_hook
  - query (SQL)
  - sort_description_fn (row → 1-line "why" string on cards)

To add a new play, append a new entry. The Plays tab renderer iterates PLAYS
in order.

P3 deferred — needs non-USD card spend field not yet surfaced in the BoB view.
P4 dropped — nobody in Greg's book has cashback below 1.5% (mostly default 1.85%).
P10 dropped — overlaps with P9 activation gap.
"""
from collections import OrderedDict

# ── P1: Free/Legacy on Plus-gated ERP, GLA ≥ $500K ──────────────────────────
P1_QUERY = """
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '__OWNER__'
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    bob.plus_product_status_v2,
    SUBSTRING(bob.erp_technographics::STRING, 1, 200) AS erp_snippet,
    ROUND(bob.current_gla, 0) AS current_gla,
    ROUND(bob.thirty_day_card_spend, 0) AS card_l30d,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d,
    ROUND(bob.estimated_card_cp_monthly, 0) AS est_card_cp_monthly,
    ROUND(bob.estimated_bill_pay_cp_monthly, 0) AS est_bp_cp_monthly,
    bob.user_count
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE COALESCE(bob.subscription_tier, 'NONE') NOT IN ('SAAS_PLUS', 'SAAS_ENTERPRISE')
  AND COALESCE(bob.current_gla, 0) >= 500000
  AND (
    bob.erp_technographics::STRING ILIKE '%netsuite%'
    OR bob.erp_technographics::STRING ILIKE '%intacct%'
    OR bob.erp_technographics::STRING ILIKE '%acumatica%'
    OR bob.erp_technographics::STRING ILIKE '%dynamics%'
    OR bob.erp_technographics::STRING ILIKE '%zoho%'
    OR bob.erp_technographics::STRING ILIKE '%oracle enterprise%'
    OR bob.erp_technographics::STRING ILIKE '%sage 50%'
  )
ORDER BY bob.estimated_card_cp_monthly DESC NULLS LAST
"""

# ── P5: PO-in-memo signal (Frankie), ≥3 bills L90D, not on Procurement ──────
P5_QUERY = """
WITH greg AS (
    SELECT DISTINCT business_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '__OWNER__' AND business_id IS NOT NULL
),
po_bills AS (
    SELECT bp.business_id, COUNT(*) AS po_bill_count, ANY_VALUE(bp.bill_memo) AS sample_memo
    FROM analytics.marts.dim_bill_pay bp
    JOIN greg ON greg.business_id = bp.business_id
    WHERE bp.bill_created_at >= CURRENT_DATE - 90
      AND bp.bill_deleted_at IS NULL
      AND bp.purchase_order_id IS NULL
      AND (
        bp.bill_memo ILIKE '%purchase order%'
        OR bp.bill_memo ILIKE 'PO-%' OR bp.bill_memo ILIKE '%PO-%'
        OR bp.bill_memo ILIKE '% PO-%'
        OR bp.bill_memo ILIKE '% PO #%'
        OR bp.bill_memo ILIKE '%PO#%'
        OR bp.bill_memo ILIKE '%PO:%'
        OR bp.bill_memo ILIKE '% PO %'
        OR bp.bill_memo ILIKE 'PO %'
        OR bp.bill_invoice_number ILIKE 'PO-%' OR bp.bill_invoice_number ILIKE '%PO-%'
        OR bp.bill_invoice_number ILIKE '%PO#%'
        OR bp.bill_invoice_number ILIKE 'PO %'
      )
    GROUP BY bp.business_id
    HAVING COUNT(*) >= 3
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    pb.po_bill_count,
    SUBSTRING(pb.sample_memo, 1, 80) AS sample_memo,
    bob.has_procurement_addon,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d,
    ROUND(bob.estimated_bill_pay_cp_monthly, 0) AS est_bp_cp_monthly,
    bob.subscription_tier
FROM po_bills pb
JOIN analytics.marts.dim_book_of_business_accounts_view bob ON bob.business_id = pb.business_id
WHERE COALESCE(bob.has_procurement_addon, FALSE) = FALSE
ORDER BY pb.po_bill_count DESC
"""

# ── P7: Just started Bill Pay, no CW BP opp ─────────────────────────────────
P7_QUERY = """
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '__OWNER__'
),
cw_bp_opps AS (
    SELECT DISTINCT account_id FROM analytics.marts.dim_sfdc_opportunities
    WHERE expansion_subtype = 'Bill Pay Expansion' AND opportunity_is_won = TRUE
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.first_bill_paid_at::date AS first_bill_paid_at,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d,
    bob.rolling_30_day_paid_bill_count AS bp_count_l30d,
    ROUND(bob.estimated_bill_pay_cp_monthly, 0) AS est_bp_cp_monthly,
    bob.subscription_tier
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
LEFT JOIN cw_bp_opps ON cw_bp_opps.account_id = bob.sfdc_account_id
WHERE bob.first_bill_paid_at >= CURRENT_DATE - 60
  AND cw_bp_opps.account_id IS NULL
ORDER BY bob.first_bill_paid_at DESC
"""

# ── P9: High-CP new sale w/ activation gap (Espora-style) ───────────────────
P9_QUERY = """
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '__OWNER__'
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    ROUND(bob.sfdc_card_spend_new_sale_activation_gap_in_cp, 0) AS card_gap_cp,
    ROUND(bob.sfdc_card_spend_new_sale_activation_gap_in_dollars, 0) AS card_gap_usd,
    ROUND(bob.sfdc_bill_pay_spend_new_sale_activation_gap_in_dollars, 0) AS bp_gap_usd,
    ROUND(bob.card_dollars_monthly_in_new_sale_opp, 0) AS card_sold_monthly,
    ROUND(bob.bill_pay_dollars_monthly_in_new_sale_opp, 0) AS bp_sold_monthly,
    ROUND(bob.thirty_day_card_spend, 0) AS card_l30d,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE (COALESCE(bob.sfdc_card_spend_new_sale_activation_gap_in_cp, 0) >= 250
       OR COALESCE(bob.sfdc_bill_pay_spend_new_sale_activation_gap_in_dollars, 0) >= 50000)
ORDER BY (
    COALESCE(bob.sfdc_card_spend_new_sale_activation_gap_in_cp, 0)
    + COALESCE(bob.sfdc_bill_pay_spend_new_sale_activation_gap_in_dollars, 0) * 0.0015
) DESC
"""

# ── P13: Top BoB spenders w/ product gap (absolute $ thresholds) ────────────
P13_QUERY = """
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '__OWNER__'
),
base AS (
    SELECT
        bob.sfdc_account_id AS account_id,
        bob.account_name,
        bob.subscription_tier,
        bob.thirty_day_card_spend AS card_l30d,
        bob.rolling_30_day_paid_bill_amount AS bp_l30d,
        bob.current_gla AS gla,
        bob.user_count,
        bob.has_procurement_addon,
        bob.is_treasury_active,
        CASE WHEN bob.subscription_tier IN ('SAAS_PLUS', 'SAAS_ENTERPRISE') THEN TRUE ELSE FALSE END AS is_on_plus
    FROM analytics.marts.dim_book_of_business_accounts_view bob
    JOIN greg ON greg.account_id = bob.sfdc_account_id
)
SELECT
    account_id, account_name, subscription_tier,
    ROUND(card_l30d, 0) AS card_l30d,
    ROUND(bp_l30d, 0) AS bp_l30d,
    ROUND(gla, 0) AS gla,
    user_count,
    ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(
        CASE WHEN card_l30d > 100000 AND (NOT is_on_plus OR NOT COALESCE(has_procurement_addon, FALSE)) THEN 'heavy-card-gap' END,
        CASE WHEN bp_l30d > 500000 AND NOT is_on_plus THEN 'heavy-bp-gap' END,
        CASE WHEN gla > 2000000 AND COALESCE(is_treasury_active, FALSE) = FALSE THEN 'large-gla-gap' END
    )), ', ') AS gaps
FROM base
WHERE (card_l30d > 100000 AND (NOT is_on_plus OR NOT COALESCE(has_procurement_addon, FALSE)))
   OR (bp_l30d > 500000 AND NOT is_on_plus)
   OR (gla > 2000000 AND COALESCE(is_treasury_active, FALSE) = FALSE)
ORDER BY GREATEST(COALESCE(card_l30d, 0), COALESCE(bp_l30d, 0) * 0.158, COALESCE(gla, 0) * 0.053) DESC
"""


# ── P2: Plus-feature trialer — tried Plus but didn't convert ────────────────
P2_QUERY = """
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '__OWNER__'
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    bob.plus_product_status_v2,
    ROUND(bob.thirty_day_card_spend, 0) AS card_l30d,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d,
    ROUND(bob.estimated_card_cp_monthly, 0) AS est_card_cp_monthly,
    ROUND(bob.estimated_bill_pay_cp_monthly, 0) AS est_bp_cp_monthly,
    bob.user_count,
    ROUND(bob.current_gla, 0) AS current_gla
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE bob.plus_product_status_v2 = 'trialed but did not convert'
  AND COALESCE(bob.subscription_tier, 'NONE') NOT IN ('SAAS_PLUS', 'SAAS_ENTERPRISE')
ORDER BY bob.thirty_day_card_spend DESC NULLS LAST
"""

# ── P6: Legacy Procurement → Add-on upgrade ──────────────────────────────────
P6_QUERY = """
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '__OWNER__'
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    bob.procurement_subscription_tier,
    bob.ltd_procurement_purchase_order_requests_created AS ltd_pos,
    bob.rolling_90_day_procurement_purchase_order_requests_created AS pos_l90d,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d,
    ROUND(bob.estimated_bill_pay_cp_monthly, 0) AS est_bp_cp_monthly
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE bob.procurement_subscription_tier = 'PROCUREMENT_LEGACY'
ORDER BY bob.ltd_procurement_purchase_order_requests_created DESC NULLS LAST
"""

# ── P8: Competitor AP migration — heavy off-Ramp BP vs. on-Ramp ─────────────
P8_QUERY = """
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '__OWNER__'
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    COALESCE(bob.pom_bill_pay_competitor_name, '(unnamed)') AS bp_competitor,
    ROUND(bob.pom_monthly_off_ramp_bill_pay_spend_in_dollars, 0) AS off_ramp_bp_monthly,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS ramp_bp_l30d,
    ROUND(bob.estimated_bill_pay_cp_monthly, 0) AS est_bp_cp_monthly,
    SUBSTRING(bob.bill_pay_technographics::STRING, 1, 200) AS bp_tech
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE COALESCE(bob.pom_monthly_off_ramp_bill_pay_spend_in_dollars, 0) >= 25000
  AND COALESCE(bob.pom_monthly_off_ramp_bill_pay_spend_in_dollars, 0) >
      COALESCE(bob.rolling_30_day_paid_bill_amount, 0) * 3
  AND COALESCE(bob.is_bill_pay_committed, FALSE) = FALSE
ORDER BY bob.pom_monthly_off_ramp_bill_pay_spend_in_dollars DESC
"""

# ── P11: Intl enterprise — 20+ users + Wise-onboarded ────────────────────────
P11_QUERY = """
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '__OWNER__'
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    bob.user_count,
    bob.ramp_user_count,
    bob.wise_onboarded_at::date AS wise_onboarded_at,
    bob.business_office_country,
    ROUND(bob.thirty_day_card_spend, 0) AS card_l30d,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d,
    ROUND(bob.estimated_card_cp_monthly, 0) AS est_card_cp_monthly,
    bob.fte_size
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE COALESCE(bob.user_count, 0) >= 20
  AND bob.wise_onboarded_at IS NOT NULL
ORDER BY bob.user_count DESC NULLS LAST
"""

# ── P12: Treasury opp — GLA > $5M + not on Treasury, sorted by treasury fit ─
P12_QUERY = """
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '__OWNER__'
),
open_treasury AS (
    SELECT DISTINCT account_id FROM analytics.marts.dim_sfdc_opportunities
    WHERE expansion_subtype = 'Treasury Expansion' AND opportunity_is_closed = FALSE
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    ROUND(bob.current_gla, 0) AS current_gla,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d,
    ROUND(bob.rolling_90_day_paid_bill_amount, 0) AS bp_l90d,
    ROUND(bob.prob_attach_score_treasury, 1) AS prob_attach_treasury,
    ROUND(bob.current_gla * 0.045 / 12, 0) AS implied_monthly_yield_at_4_5pct
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
LEFT JOIN open_treasury ot ON ot.account_id = bob.sfdc_account_id
WHERE COALESCE(bob.current_gla, 0) >= 5000000
  AND COALESCE(bob.is_treasury_active, FALSE) = FALSE
  AND ot.account_id IS NULL
ORDER BY (bob.current_gla + COALESCE(bob.rolling_90_day_paid_bill_amount, 0) * 3) DESC NULLS LAST
"""


PLAYS = OrderedDict([
    ("P1", {
        "title": "Plus-gated ERP upsell",
        "icon": ":unlock:",
        "criteria": "Not on Plus + ERP ∈ {NetSuite, Intacct, Acumatica, Dynamics, Zoho, Oracle Ent, Sage 50} + GLA ≥ $500K",
        "pitch_hook": "On {erp} without Plus — gate's right there, just walk them through it.",
        "query": P1_QUERY,
        "sort_description_fn": lambda row: f"Est Card CP ${row.get('est_card_cp_monthly', 0):,.0f}/mo · GLA ${row.get('current_gla', 0):,.0f}",
    }),
    ("P2", {
        "title": "Plus-feature trialer (didn't convert)",
        "icon": ":arrows_counterclockwise:",
        "criteria": "Trialed Plus but didn't convert — high-intent warm lead",
        "pitch_hook": "They've already seen Plus in trial and passed. Re-open the conversation with a specific feature hook + value calc.",
        "query": P2_QUERY,
        "sort_description_fn": lambda row: f"Card ${row.get('card_l30d', 0):,.0f}/mo · Est Card CP ${row.get('est_card_cp_monthly', 0):,.0f}/mo · {row.get('user_count', 0) or 0:.0f} users",
    }),
    ("P5", {
        "title": "PO-in-memo → Procurement",
        "icon": ":page_with_curl:",
        "criteria": "≥3 bills L90D w/ PO reference in memo + not on Procurement Add-on",
        "pitch_hook": "{po_bill_count} of their L90D bills reference POs in the memo — they're already running a PO workflow in a spreadsheet.",
        "query": P5_QUERY,
        "sort_description_fn": lambda row: f"{row.get('po_bill_count', 0)} PO-memo bills L90D · BP ${row.get('bp_l30d', 0):,.0f}/mo",
    }),
    ("P6", {
        "title": "Legacy Procurement → Add-on upgrade",
        "icon": ":arrow_up:",
        "criteria": "On PROCUREMENT_LEGACY tier — upgrade to modern Procurement Add-on",
        "pitch_hook": "{ltd_pos} lifetime POs on the legacy Procurement tier. Time to migrate to the modern Add-on (3-way match, intake forms, renewal tracking).",
        "query": P6_QUERY,
        "sort_description_fn": lambda row: f"{int(row.get('ltd_pos') or 0)} lifetime POs · {int(row.get('pos_l90d') or 0)} POs L90D",
    }),
    ("P7", {
        "title": "Just started Bill Pay",
        "icon": ":seedling:",
        "criteria": "First bill paid in last 60d + no CW Bill Pay opp ever",
        "pitch_hook": "Paid their first bill on {first_bill_paid_at} — window's open to lock in a BP opp at a low baseline.",
        "query": P7_QUERY,
        "sort_description_fn": lambda row: f"First bill {row.get('first_bill_paid_at', '?')} · {row.get('bp_count_l30d', 0)} bills L30D",
    }),
    ("P8", {
        "title": "Competitor AP (Bill.com migration signal)",
        "icon": ":arrows_counterclockwise:",
        "criteria": "Off-Ramp BP spend > $25K/mo AND > 3× on-Ramp volume — clearly using a competitor AP tool",
        "pitch_hook": "They're routing ${off_ramp_bp_monthly}/mo through a non-Ramp AP tool — {bp_competitor}. Time to consolidate.",
        "query": P8_QUERY,
        "sort_description_fn": lambda row: f"Off-Ramp BP ${row.get('off_ramp_bp_monthly', 0):,.0f}/mo · Competitor: {row.get('bp_competitor', '?')}",
    }),
    ("P9", {
        "title": "New-sale activation gap",
        "icon": ":chart_with_downwards_trend:",
        "criteria": "Card activation gap ≥ $250/mo CP OR Bill Pay gap ≥ $50K/mo — they bought in but haven't activated",
        "pitch_hook": "Sold at ${card_sold_monthly}/mo card + ${bp_sold_monthly}/mo BP; current is ${card_l30d} + ${bp_l30d}. Activation drag worth chasing.",
        "query": P9_QUERY,
        "sort_description_fn": lambda row: f"Card gap ${row.get('card_gap_cp', 0):,.0f}/mo CP · Sold ${row.get('card_sold_monthly', 0):,.0f}/mo",
    }),
    ("P11", {
        "title": "Intl-heavy enterprise (Wise-onboarded)",
        "icon": ":globe_with_meridians:",
        "criteria": "20+ users + Wise-onboarded — already sending intl payments at scale",
        "pitch_hook": "{user_count} users and routing intl payments through Wise since {wise_onboarded_at}. Ramp Plus unlocks per-entity controls + advanced FX visibility for teams this size.",
        "query": P11_QUERY,
        "sort_description_fn": lambda row: f"{int(row.get('user_count') or 0)} users · Wise since {row.get('wise_onboarded_at','?')} · Card ${row.get('card_l30d', 0):,.0f}/mo",
    }),
    ("P12", {
        "title": "Treasury opp (GLA >$5M, not on Treasury)",
        "icon": ":bank:",
        "criteria": "GLA > $5M in connected bank accounts + not on Treasury + no open Treasury opp. Sorted by treasury fit (GLA + BP activity).",
        "pitch_hook": "${current_gla} sitting in connected bank accounts earning 0%. At 4.5% yield that's ~${implied_monthly_yield_at_4_5pct}/mo in recovered revenue.",
        "query": P12_QUERY,
        "sort_description_fn": lambda row: f"GLA ${row.get('current_gla', 0):,.0f} · BP ${row.get('bp_l30d', 0):,.0f}/mo · Implied yield ${row.get('implied_monthly_yield_at_4_5pct', 0):,.0f}/mo",
    }),
    ("P13", {
        "title": "Top spenders w/ product gap",
        "icon": ":whale:",
        "criteria": "Card L30D >$100K (gap: Plus/Procurement) OR BP L30D >$500K (gap: Plus) OR GLA >$2M (gap: Treasury)",
        "pitch_hook": "High-spend, missing {gaps} — easy cross-sell conversation.",
        "query": P13_QUERY,
        "sort_description_fn": lambda row: f"Card ${row.get('card_l30d', 0):,.0f}/mo · BP ${row.get('bp_l30d', 0):,.0f}/mo · GLA ${row.get('gla', 0):,.0f} · [{row.get('gaps', '?')}]",
    }),
])


def run_play(play_id: str, user_id: str = None):
    """Run a single play's SQL for the given user. Returns a pandas DataFrame.

    Raises KeyError if play_id is not in PLAYS.
    """
    from core.snowflake_client import run_query
    from queries.queries import format_query

    play = PLAYS[play_id]
    sql = format_query(play["query"], user_id=user_id)
    return run_query(sql)


# Play-ID → Deal Anatomy play_tags mapping. When the Play Library aggregates
# CW deals across the team, these tags are how each Plays-tab row pulls its
# "Team evidence" footer (deal count, avg CP, top pain points, etc.).
# Anatomy tags enum (from jobs/deal_anatomy.py system prompt): plus_upgrade,
# procurement_upgrade, bill_pay_migration, card_consolidation, treasury_attach,
# activation_rescue, multi_entity, erp_integration, international_scale.
PLAY_ID_TO_ANATOMY_TAGS = {
    "P1":  ["plus_upgrade", "erp_integration"],
    "P2":  ["plus_upgrade"],
    "P5":  ["procurement_upgrade"],
    "P6":  ["procurement_upgrade"],
    "P7":  ["bill_pay_migration", "activation_rescue"],
    "P8":  ["bill_pay_migration"],
    "P9":  ["activation_rescue"],
    "P11": ["international_scale", "multi_entity", "plus_upgrade"],
    "P12": ["treasury_attach"],
    "P13": ["card_consolidation", "multi_entity", "plus_upgrade", "treasury_attach"],
}
