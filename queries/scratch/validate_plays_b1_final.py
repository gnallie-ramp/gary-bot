"""B.1 final validation — corrected column names, ARRAY cast, tuned thresholds."""
import sys
import traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.snowflake_client import run_query

OWNER = "Gregory Nallie"


def safe_run(sql):
    """Run and surface errors (run_query swallows them)."""
    try:
        return run_query(sql), None
    except Exception as e:
        return None, traceback.format_exc()


# ── P1: Free/Legacy on Plus-gated ERP ────────────────────────────────────────
P1 = f"""
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '{OWNER}'
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    bob.subscription_tier,
    bob.plus_product_status_v2,
    SUBSTRING(bob.erp_technographics::STRING, 1, 100) AS erp_snippet,
    ROUND(bob.thirty_day_card_spend, 0) AS card_l30d,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d,
    ROUND(bob.estimated_card_cp_monthly, 0) AS est_card_cp_monthly,
    bob.user_count
FROM analytics.marts.dim_book_of_business_accounts_view bob
JOIN greg ON greg.account_id = bob.sfdc_account_id
WHERE COALESCE(bob.subscription_tier, 'NONE') NOT IN ('SAAS_PLUS', 'SAAS_ENTERPRISE')
  AND (
    bob.erp_technographics::STRING ILIKE '%netsuite%'
    OR bob.erp_technographics::STRING ILIKE '%intacct%'
    OR bob.erp_technographics::STRING ILIKE '%acumatica%'
    OR bob.erp_technographics::STRING ILIKE '%dynamics%'
    OR bob.erp_technographics::STRING ILIKE '%zoho%'
    OR bob.erp_technographics::STRING ILIKE '%oracle enterprise%'
    OR bob.erp_technographics::STRING ILIKE '%sage 50%'
  )
ORDER BY bob.thirty_day_card_spend DESC NULLS LAST
"""

# ── P5: PO-in-memo (word-boundary, digit-adjacent or "purchase order") ──────
P5 = f"""
WITH greg AS (
    SELECT DISTINCT business_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '{OWNER}' AND business_id IS NOT NULL
),
po_bills AS (
    SELECT
        bp.business_id,
        COUNT(*) AS po_bill_count,
        SUM(bp.bill_amount) AS po_bill_amount,
        MAX(bp.bill_memo) AS sample_memo
    FROM analytics.marts.dim_bill_pay bp
    JOIN greg ON greg.business_id = bp.business_id
    WHERE bp.bill_paid_at >= CURRENT_DATE - 90
      AND bp.bill_deleted_at IS NULL
      AND (
        bp.bill_memo ILIKE '%purchase order%'
        OR REGEXP_LIKE(bp.bill_memo, '(^|[^A-Za-z0-9])PO[ :#\\-_]?\\d', 'i')
        OR REGEXP_LIKE(COALESCE(bp.bill_invoice_number, ''), '(^|[^A-Za-z0-9])PO[ :#\\-_]?\\d', 'i')
      )
    GROUP BY bp.business_id
    HAVING COUNT(*) >= 3
)
SELECT
    bob.sfdc_account_id AS account_id,
    bob.account_name,
    pb.po_bill_count,
    ROUND(pb.po_bill_amount, 0) AS po_bill_amount,
    SUBSTRING(pb.sample_memo, 1, 70) AS sample_memo,
    bob.has_procurement_addon,
    ROUND(bob.rolling_30_day_paid_bill_amount, 0) AS bp_l30d
FROM po_bills pb
JOIN analytics.marts.dim_book_of_business_accounts_view bob ON bob.business_id = pb.business_id
WHERE COALESCE(bob.has_procurement_addon, FALSE) = FALSE
ORDER BY pb.po_bill_count DESC
"""

# ── P7: Just started BP, no CW BP opp ───────────────────────────────────────
P7 = f"""
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '{OWNER}'
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

# ── P9: High-CP new sale w/ activation gap (Espora) ─────────────────────────
P9 = f"""
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '{OWNER}'
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

# ── P13: Top spenders w/ product gap ─────────────────────────────────────────
P13 = f"""
WITH greg AS (
    SELECT DISTINCT account_id FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
    WHERE date_day = CURRENT_DATE - 1 AND owner_name = '{OWNER}'
),
base AS (
    SELECT
        bob.sfdc_account_id AS account_id,
        bob.account_name,
        bob.subscription_tier,
        bob.thirty_day_card_spend AS card_l30d,
        bob.rolling_30_day_paid_bill_amount AS bp_l30d,
        bob.treasury_available_balance AS gla,
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

PLAYS = [
    ("P1", "Free/Legacy on Plus-gated ERP", P1),
    ("P5", "PO-in-memo (Frankie)", P5),
    ("P7", "Just started BP, no CW BP opp", P7),
    ("P9", "High-CP new sale w/ activation gap", P9),
    ("P13", "Top spenders w/ product gap", P13),
]


def main():
    all_ids = {}
    for pid, title, sql in PLAYS:
        print(f"\n{'=' * 70}\n{pid}: {title}\n{'=' * 70}")
        df, err = safe_run(sql)
        if err:
            print(f"  ERROR:\n{err}")
            continue
        print(f"  rows: {len(df)}")
        if df is None or df.empty:
            continue
        if "account_id" in df.columns:
            all_ids[pid] = set(df["account_id"].dropna().astype(str).tolist())
        print(df.drop(columns=[c for c in ["account_id"] if c in df.columns]).head(10).to_string(index=False))

    # Overlap
    if len(all_ids) >= 2:
        print(f"\n{'=' * 70}\nOverlap matrix\n{'=' * 70}")
        pids = list(all_ids.keys())
        print("        " + "".join(f"{p:>6}" for p in pids))
        for a in pids:
            row = f"{a:<8}"
            for b in pids:
                row += f"{len(all_ids[a] & all_ids[b]):>6}"
            print(row)

        from collections import Counter
        c = Counter()
        for ids in all_ids.values():
            for x in ids:
                c[x] += 1
        multi = {x: n for x, n in c.items() if n >= 2}
        print(f"\n  Multi-play accounts (2+ plays): {len(multi)}")


if __name__ == "__main__":
    main()
