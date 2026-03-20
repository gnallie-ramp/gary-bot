from config import OWNER_NAME as _OWNER_NAME

OPEN_OPPS_QUERY = """
WITH greg_open_opps AS (
    SELECT
        opp.opportunity_id,
        opp.account_id,
        opp.opportunity_name,
        opp.expansion_subtype,
        opp.opportunity_stage_name,
        opp.opportunity_close_date,
        opp.opportunity_created_at::date AS created_date,
        DATEDIFF('day', opp.opportunity_created_at::date, CURRENT_DATE) AS days_open,
        sa.account_name,
        sa.business_id
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name != 'S0: Holding'
      AND opp.expansion_subtype IN (
          'Card Expansion', 'Bill Pay Expansion',
          'Travel Expansion', 'Treasury Expansion'
      )
),
baseline_spend AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS baseline
    FROM greg_open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day BETWEEN
            IFF(g.days_open <= 45,
                DATEADD('day', -30, g.created_date),
                CURRENT_DATE - 60)
            AND
            IFF(g.days_open <= 45,
                DATEADD('day', -1, g.created_date),
                CURRENT_DATE - 31)
    GROUP BY g.opportunity_id, g.expansion_subtype
),
recent_spend AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS recent_val
    FROM greg_open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= CURRENT_DATE - 30
    GROUP BY g.opportunity_id, g.expansion_subtype
)
SELECT
    g.account_name,
    g.account_id,
    g.opportunity_id,
    g.opportunity_name,
    g.expansion_subtype,
    g.opportunity_stage_name,
    g.opportunity_close_date,
    g.created_date,
    ROUND(COALESCE(b.baseline, 0))                                        AS baseline_spend,
    ROUND(COALESCE(r.recent_val, 0))                                      AS current_spend,
    ROUND(COALESCE(r.recent_val, 0) - COALESCE(b.baseline, 0))           AS over_baseline,
    ROUND(
        CASE WHEN COALESCE(b.baseline, 0) > 0
             THEN 100.0 * COALESCE(r.recent_val, 0) / b.baseline
             ELSE NULL END
    )                                                                      AS pacing_pct,
    IFF(g.days_open <= 45, 'At creation', 'Rolling 30\u201360d ago')            AS baseline_type
FROM greg_open_opps g
LEFT JOIN baseline_spend b ON b.opportunity_id = g.opportunity_id
LEFT JOIN recent_spend r ON r.opportunity_id = g.opportunity_id
WHERE COALESCE(r.recent_val, 0) > COALESCE(b.baseline, 0)
ORDER BY (COALESCE(r.recent_val, 0) - COALESCE(b.baseline, 0)) DESC
"""

OPPS_TO_WATCH_QUERY = """
WITH greg_open_opps AS (
    SELECT
        opp.opportunity_id,
        opp.account_id,
        opp.opportunity_name,
        opp.expansion_subtype,
        opp.opportunity_stage_name,
        opp.opportunity_created_at::date AS created_date,
        DATEDIFF('day', opp.opportunity_created_at::date, CURRENT_DATE) AS days_open,
        sa.account_name,
        sa.business_id,
        opp.monthly_expansion_amount
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name != 'S0: Holding'
      AND opp.expansion_subtype IN (
          'Card Expansion','Bill Pay Expansion','Travel Expansion','Treasury Expansion'
      )
),
baseline_spend AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS baseline_val
    FROM greg_open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day BETWEEN
            IFF(g.days_open <= 45,
                DATEADD('day', -30, g.created_date),
                CURRENT_DATE - 60)
            AND
            IFF(g.days_open <= 45,
                DATEADD('day', -1, g.created_date),
                CURRENT_DATE - 31)
    GROUP BY g.opportunity_id, g.expansion_subtype
),
post_creation_spend AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS spend_since_creation,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.card_tpv    ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.travel_tpv  ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.treasury_available_balance ELSE NULL END)
        END AS recent_val
    FROM greg_open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= g.created_date
    GROUP BY g.opportunity_id, g.expansion_subtype
),
month_meta AS (
    SELECT
        DATEDIFF('day', DATE_TRUNC('month', CURRENT_DATE), CURRENT_DATE) AS days_elapsed,
        DAY(LAST_DAY(CURRENT_DATE))                                      AS days_in_month
),
mtd_spend AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS mtd_val
    FROM greg_open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= DATE_TRUNC('month', CURRENT_DATE)
        AND tpv.date_day < CURRENT_DATE
    GROUP BY g.opportunity_id, g.expansion_subtype
),
last_email AS (
    SELECT
        sfdc_account_id AS account_id,
        last_email_created_at::date                                             AS last_email_date,
        DATEDIFF('day', last_email_created_at::date, CURRENT_DATE)             AS days_since_email,
        last_email_direction,
        first_sfdc_email_subject                                                AS last_email_subject,
        thread_owner_full_name                                                  AS last_email_sender,
        historical_thread_owner_role                                            AS last_email_sender_role,
        number_messages_has_painpoints                                          AS painpoint_count
    FROM analytics.marts.dim_email_threads
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sfdc_account_id ORDER BY last_email_created_at DESC) = 1
),
last_call AS (
    SELECT
        gt.account_id,
        MAX(gt.call_start)::date                                                AS last_call_date,
        DATEDIFF('day', MAX(gt.call_start)::date, CURRENT_DATE)                AS days_since_call
    FROM analytics.marts.dim_sfdc_gong_transcripts gt
    JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
        ON ledger.account_id = gt.account_id
        AND ledger.date_day = CURRENT_DATE - 1
        AND ledger.owner_name = 'Gregory Nallie'
    GROUP BY 1
)
SELECT
    g.account_name,
    g.account_id,
    g.opportunity_id,
    g.expansion_subtype,
    g.opportunity_stage_name,
    g.created_date,
    g.days_open,
    g.monthly_expansion_amount,
    IFF(g.days_open <= 45, 'At creation', 'Rolling 30\u201360d ago') AS baseline_type,
    ROUND(COALESCE(b.baseline_val, 0))           AS baseline_spend,
    ROUND(COALESCE(mt.mtd_val, 0))               AS mtd_spend,
    mm.days_in_month - mm.days_elapsed           AS days_left_in_month,
    ROUND(CASE WHEN COALESCE(b.baseline_val, 0) > 0
        THEN 100.0 * COALESCE(mt.mtd_val, 0) / b.baseline_val
        ELSE NULL END)                           AS mtd_pct_of_baseline,
    ROUND(COALESCE(mt.mtd_val, 0) * mm.days_in_month
        / NULLIF(mm.days_elapsed, 0))            AS paced_monthly,
    ROUND(COALESCE(mt.mtd_val, 0) * mm.days_in_month
        / NULLIF(mm.days_elapsed, 0)
        - COALESCE(b.baseline_val, 0))           AS paced_over_baseline,
    ROUND(COALESCE(p.recent_val, 0))             AS recent_30d_spend,
    ROUND(COALESCE(p.spend_since_creation, 0))   AS spend_since_creation,
    ROUND(
        CASE WHEN COALESCE(b.baseline_val, 0) > 0
             THEN 100.0 * COALESCE(p.recent_val, 0) / b.baseline_val
             ELSE NULL END
    )                                            AS pct_of_baseline,
    CASE
        WHEN COALESCE(p.recent_val, 0) = 0                                  THEN 'No spend yet'
        WHEN COALESCE(p.recent_val, 0) < COALESCE(b.baseline_val, 0) * 0.5 THEN 'Very low'
        WHEN COALESCE(p.recent_val, 0) < COALESCE(b.baseline_val, 0)       THEN 'Below baseline'
        WHEN COALESCE(p.recent_val, 0) < COALESCE(b.baseline_val, 0) * 1.2 THEN 'Near baseline'
        ELSE 'Exceeding baseline'
    END                                          AS activation_status,
    le.last_email_date,
    le.days_since_email,
    le.last_email_direction,
    le.last_email_subject,
    lc.last_call_date,
    lc.days_since_call,
    GREATEST(
        COALESCE(le.last_email_date, '2000-01-01'),
        COALESCE(lc.last_call_date,  '2000-01-01')
    )                                            AS last_touch_date,
    DATEDIFF('day',
        GREATEST(
            COALESCE(le.last_email_date, '2000-01-01'),
            COALESCE(lc.last_call_date,  '2000-01-01')
        ),
        CURRENT_DATE
    )                                            AS days_since_last_touch
FROM greg_open_opps g
CROSS JOIN month_meta mm
LEFT JOIN baseline_spend      b  ON b.opportunity_id  = g.opportunity_id
LEFT JOIN post_creation_spend p  ON p.opportunity_id  = g.opportunity_id
LEFT JOIN mtd_spend           mt ON mt.opportunity_id = g.opportunity_id
LEFT JOIN last_email          le ON le.account_id     = g.account_id
LEFT JOIN last_call           lc ON lc.account_id     = g.account_id
WHERE COALESCE(p.recent_val, 0) <= COALESCE(b.baseline_val, 0) * 1.0
   OR COALESCE(b.baseline_val, 0) = 0
ORDER BY g.days_open DESC
"""

SPEND_ACCEL_QUERY = """
WITH greg_accounts AS (
    SELECT DISTINCT ledger.account_id, ledger.business_id
    FROM analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
    WHERE ledger.date_day = CURRENT_DATE - 1
      AND ledger.owner_name = 'Gregory Nallie'
),
open_opp_accounts AS (
    SELECT DISTINCT opp.account_id
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name != 'S0: Holding'
),
recent_closed_opps AS (
    SELECT
        opp.account_id,
        opp.opportunity_name AS recent_opp_name,
        opp.opportunity_close_date AS recent_opp_close_date,
        opp.opportunity_stage_name AS recent_opp_stage,
        opp.expansion_subtype AS recent_opp_product,
        ROW_NUMBER() OVER (PARTITION BY opp.account_id ORDER BY opp.opportunity_close_date DESC) AS rn
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.opportunity_is_closed = TRUE
      AND opp.opportunity_is_won = TRUE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_close_date >= CURRENT_DATE - 90
),
spend_summary AS (
    SELECT
        tpv.business_id,
        SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.card_tpv ELSE 0 END) AS baseline_card,
        SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.billpay_tpv ELSE 0 END) AS baseline_billpay,
        SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.travel_tpv ELSE 0 END) AS baseline_travel,
        AVG(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 60 AND CURRENT_DATE - 31 THEN tpv.treasury_available_balance ELSE NULL END) AS baseline_treasury,
        SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.card_tpv ELSE 0 END) AS current_card,
        SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.billpay_tpv ELSE 0 END) AS current_billpay,
        SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.travel_tpv ELSE 0 END) AS current_travel,
        AVG(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.treasury_available_balance ELSE NULL END) AS current_treasury
    FROM analytics.metrics.fct_daily_business__multiproduct_tpv tpv
    JOIN greg_accounts ga ON ga.business_id = tpv.business_id
    WHERE tpv.date_day >= CURRENT_DATE - 60
    GROUP BY 1
),
closed_opp_baseline AS (
    SELECT
        tpv.business_id,
        SUM(CASE WHEN tpv.date_day BETWEEN DATEADD('day', -30, rco.recent_opp_close_date)
                                        AND DATEADD('day', -1, rco.recent_opp_close_date)
             THEN tpv.card_tpv ELSE 0 END) AS baseline_card,
        SUM(CASE WHEN tpv.date_day BETWEEN DATEADD('day', -30, rco.recent_opp_close_date)
                                        AND DATEADD('day', -1, rco.recent_opp_close_date)
             THEN tpv.billpay_tpv ELSE 0 END) AS baseline_billpay,
        SUM(CASE WHEN tpv.date_day BETWEEN DATEADD('day', -30, rco.recent_opp_close_date)
                                        AND DATEADD('day', -1, rco.recent_opp_close_date)
             THEN tpv.travel_tpv ELSE 0 END) AS baseline_travel,
        AVG(CASE WHEN tpv.date_day BETWEEN DATEADD('day', -30, rco.recent_opp_close_date)
                                        AND DATEADD('day', -1, rco.recent_opp_close_date)
             THEN tpv.treasury_available_balance ELSE NULL END) AS baseline_treasury
    FROM analytics.metrics.fct_daily_business__multiproduct_tpv tpv
    JOIN greg_accounts ga ON ga.business_id = tpv.business_id
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.business_id = ga.business_id
    JOIN recent_closed_opps rco ON rco.account_id = sa.account_id AND rco.rn = 1
    WHERE tpv.date_day >= DATEADD('day', -30, rco.recent_opp_close_date)
      AND tpv.date_day < rco.recent_opp_close_date
    GROUP BY 1
)
SELECT
    sa.account_name,
    sa.account_id,
    ss.business_id,
    ROUND(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_card ELSE ss.baseline_card END) AS baseline_card,
    ROUND(ss.current_card) AS current_card,
    ROUND(ss.current_card - CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_card ELSE ss.baseline_card END) AS card_delta,
    ROUND(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_billpay ELSE ss.baseline_billpay END) AS baseline_billpay,
    ROUND(ss.current_billpay) AS current_billpay,
    ROUND(ss.current_billpay - CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_billpay ELSE ss.baseline_billpay END) AS billpay_delta,
    ROUND(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_travel ELSE ss.baseline_travel END) AS baseline_travel,
    ROUND(ss.current_travel) AS current_travel,
    ROUND(ss.current_travel - CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_travel ELSE ss.baseline_travel END) AS travel_delta,
    ROUND(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_treasury ELSE ss.baseline_treasury END) AS baseline_treasury,
    ROUND(ss.current_treasury) AS current_treasury,
    ROUND(ss.current_treasury - CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_treasury ELSE ss.baseline_treasury END) AS treasury_delta,
    rco.recent_opp_name,
    rco.recent_opp_close_date,
    rco.recent_opp_stage,
    rco.recent_opp_product
FROM spend_summary ss
JOIN analytics.marts.dim_sfdc_accounts sa ON sa.business_id = ss.business_id
LEFT JOIN recent_closed_opps rco ON rco.account_id = sa.account_id AND rco.rn = 1
LEFT JOIN closed_opp_baseline cob ON cob.business_id = ss.business_id
WHERE sa.account_id NOT IN (SELECT account_id FROM open_opp_accounts)
  AND (
      (ss.current_card > COALESCE(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_card ELSE ss.baseline_card END, 0) * 1.1
       AND ss.current_card - COALESCE(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_card ELSE ss.baseline_card END, 0) > 5000)
      OR (ss.current_billpay > COALESCE(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_billpay ELSE ss.baseline_billpay END, 0) * 1.1
          AND ss.current_billpay - COALESCE(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_billpay ELSE ss.baseline_billpay END, 0) > 5000)
      OR (ss.current_travel > COALESCE(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_travel ELSE ss.baseline_travel END, 0) * 1.1
          AND ss.current_travel - COALESCE(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_travel ELSE ss.baseline_travel END, 0) > 5000)
      OR (ss.current_treasury > COALESCE(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_treasury ELSE ss.baseline_treasury END, 0) * 1.1
          AND ss.current_treasury - COALESCE(CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_treasury ELSE ss.baseline_treasury END, 0) > 50000)
  )
ORDER BY GREATEST(
    COALESCE(ss.current_card - CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_card ELSE ss.baseline_card END, 0),
    COALESCE(ss.current_billpay - CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_billpay ELSE ss.baseline_billpay END, 0),
    COALESCE(ss.current_travel - CASE WHEN rco.account_id IS NOT NULL THEN cob.baseline_travel ELSE ss.baseline_travel END, 0)
) DESC
"""

REALIZED_CP_QUERY = """
WITH greg_cw_opps AS (
    SELECT
        o.opportunity_id,
        o.account_id,
        o.opportunity_name,
        o.expansion_subtype,
        o.opportunity_type,
        o.opportunity_closed_won_date::date AS cw_date,
        o.monthly_expansion_amount,
        sa.account_name,
        sa.business_id,
        DATEDIFF('day', o.opportunity_closed_won_date, CURRENT_DATE) AS days_since_cw,
        CASE o.expansion_subtype
            WHEN 'Card Expansion'     THEN es.expansion_opportunity_30_day_transaction_amount_tpv_before_closed_won_date_prior
            WHEN 'Bill Pay Expansion' THEN es.expansion_opportunity_30_day_bill_pay_non_card_tpv_revops_amount_before_closed_won_date_prior
            WHEN 'Travel Expansion'   THEN es.expansion_opportunity_30_day_travel_amount_before_closed_won_date_prior
            WHEN 'Treasury Expansion' THEN es.expansion_opportunity_30_day_avg_treasury_available_balance_before_closed_won_date_prior
        END AS baseline_at_close
    FROM analytics.marts.dim_sfdc_opportunities o
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = o.account_id
    JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
        ON ledger.account_id = o.account_id
        AND ledger.date_day = CURRENT_DATE - 1
        AND ledger.owner_name = 'Gregory Nallie'
    LEFT JOIN analytics.marts.agg_sfdc_expansion_opportunity_spend es
        ON es.opportunity_id = o.opportunity_id
    WHERE o.opportunity_is_won = TRUE
      AND o.opportunity_closed_won_date >= '2026-01-01'
),
spend_windows AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 1 AND g.cw_date + 30 THEN tpv.card_tpv ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 1 AND g.cw_date + 30 THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 1 AND g.cw_date + 30 THEN tpv.travel_tpv ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day BETWEEN g.cw_date + 1 AND g.cw_date + 30 THEN tpv.treasury_available_balance ELSE NULL END)
        END AS spend_d1_d30,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 31 AND g.cw_date + 60 THEN tpv.card_tpv ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 31 AND g.cw_date + 60 THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 31 AND g.cw_date + 60 THEN tpv.travel_tpv ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day BETWEEN g.cw_date + 31 AND g.cw_date + 60 THEN tpv.treasury_available_balance ELSE NULL END)
        END AS spend_d31_d60,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 61 AND g.cw_date + 90 THEN tpv.card_tpv ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 61 AND g.cw_date + 90 THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 61 AND g.cw_date + 90 THEN tpv.travel_tpv ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day BETWEEN g.cw_date + 61 AND g.cw_date + 90 THEN tpv.treasury_available_balance ELSE NULL END)
        END AS spend_d61_d90,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.card_tpv ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.travel_tpv ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.treasury_available_balance ELSE NULL END)
        END AS current_l30d
    FROM greg_cw_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day BETWEEN g.cw_date + 1 AND CURRENT_DATE
    WHERE g.expansion_subtype IN (
        'Card Expansion', 'Bill Pay Expansion', 'Travel Expansion', 'Treasury Expansion'
    )
    GROUP BY g.opportunity_id, g.expansion_subtype
)
SELECT
    g.opportunity_id,
    g.account_id,
    g.opportunity_name,
    g.expansion_subtype,
    g.opportunity_type,
    g.cw_date,
    g.monthly_expansion_amount,
    g.account_name,
    g.business_id,
    g.days_since_cw,
    ROUND(COALESCE(g.baseline_at_close, 0)) AS baseline_at_close,
    ROUND(COALESCE(sw.spend_d1_d30, 0)) AS spend_d1_d30,
    ROUND(COALESCE(sw.spend_d31_d60, 0)) AS spend_d31_d60,
    ROUND(COALESCE(sw.spend_d61_d90, 0)) AS spend_d61_d90,
    ROUND(COALESCE(sw.current_l30d, 0)) AS current_l30d
FROM greg_cw_opps g
LEFT JOIN spend_windows sw ON sw.opportunity_id = g.opportunity_id
ORDER BY g.cw_date DESC
"""

SIGNALS_QUERY = """
WITH greg_accounts AS (
    SELECT DISTINCT ledger.account_id, ledger.business_id
    FROM analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
    WHERE ledger.date_day = CURRENT_DATE - 1
      AND ledger.owner_name = 'Gregory Nallie'
),
open_opps AS (
    SELECT
        opp.account_id,
        MAX(CASE WHEN opp.expansion_subtype = 'Card Expansion' THEN 1 ELSE 0 END) AS has_open_card_opp,
        MAX(CASE WHEN opp.expansion_subtype = 'Bill Pay Expansion' THEN 1 ELSE 0 END) AS has_open_billpay_opp,
        MAX(CASE WHEN opp.expansion_subtype = 'Treasury Expansion' THEN 1 ELSE 0 END) AS has_open_treasury_opp,
        MAX(CASE WHEN opp.expansion_subtype = 'Travel Expansion' THEN 1 ELSE 0 END) AS has_open_travel_opp,
        LISTAGG(DISTINCT opp.opportunity_id, ',') AS open_opp_ids
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name != 'S0: Holding'
    GROUP BY 1
),
recent_closed_won AS (
    SELECT
        opp.account_id,
        MAX(CASE WHEN opp.expansion_subtype = 'Card Expansion' THEN 1 ELSE 0 END) AS has_recent_cw_card,
        MAX(CASE WHEN opp.expansion_subtype = 'Bill Pay Expansion' THEN 1 ELSE 0 END) AS has_recent_cw_billpay,
        MAX(CASE WHEN opp.expansion_subtype = 'Treasury Expansion' THEN 1 ELSE 0 END) AS has_recent_cw_treasury,
        MAX(CASE WHEN opp.expansion_subtype = 'Travel Expansion' THEN 1 ELSE 0 END) AS has_recent_cw_travel
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.opportunity_is_closed = TRUE
      AND opp.opportunity_stage_name ILIKE '%Closed Won%'
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_close_date >= CURRENT_DATE - 90
    GROUP BY 1
),
signals AS (
    SELECT
        bob.sfdc_account_id AS account_id,
        bob.business_id,
        bob.account_name,
        bob.card_fifth_transaction_cleared_at::date AS card_activated_at,
        bob.first_bill_paid_at::date AS billpay_first_paid_at,
        bob.bill_pay_third_bill_paid_at::date AS billpay_third_paid_at,
        bob.treasury_first_payment_at::date AS treasury_activated_at,
        bob.travel_fifth_booking_at::date AS travel_activated_at,
        bob.thirty_day_card_spend AS card_spend_l30d,
        bob.rolling_30_day_paid_bill_amount AS billpay_spend_l30d,
        bob.rolling_30_days_avg_treasury_available_balance_usd AS treasury_balance_l30d
    FROM analytics.marts.dim_book_of_business_accounts_view bob
    JOIN greg_accounts ga ON ga.account_id = bob.sfdc_account_id
    WHERE (
        bob.card_fifth_transaction_cleared_at >= CURRENT_DATE - 60
        OR bob.first_bill_paid_at >= CURRENT_DATE - 60
        OR bob.treasury_first_payment_at >= CURRENT_DATE - 60
        OR bob.travel_fifth_booking_at >= CURRENT_DATE - 60
    )
)
SELECT
    s.*,
    COALESCE(oo.has_open_card_opp, 0) AS has_open_card_opp,
    COALESCE(oo.has_open_billpay_opp, 0) AS has_open_billpay_opp,
    COALESCE(oo.has_open_treasury_opp, 0) AS has_open_treasury_opp,
    COALESCE(oo.has_open_travel_opp, 0) AS has_open_travel_opp,
    oo.open_opp_ids,
    COALESCE(rcw.has_recent_cw_card, 0) AS has_recent_cw_card,
    COALESCE(rcw.has_recent_cw_billpay, 0) AS has_recent_cw_billpay,
    COALESCE(rcw.has_recent_cw_treasury, 0) AS has_recent_cw_treasury,
    COALESCE(rcw.has_recent_cw_travel, 0) AS has_recent_cw_travel
FROM signals s
LEFT JOIN open_opps oo ON oo.account_id = s.account_id
LEFT JOIN recent_closed_won rcw ON rcw.account_id = s.account_id
ORDER BY GREATEST(
    COALESCE(s.card_activated_at, '2000-01-01'),
    COALESCE(s.billpay_first_paid_at, '2000-01-01'),
    COALESCE(s.treasury_activated_at, '2000-01-01'),
    COALESCE(s.travel_activated_at, '2000-01-01')
) DESC
"""

ACH_TO_CARD_QUERY = """
WITH greg_accounts AS (
    SELECT DISTINCT ledger.account_id, ledger.business_id
    FROM analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
    WHERE ledger.date_day = CURRENT_DATE - 1
      AND ledger.owner_name = 'Gregory Nallie'
),
ach_vendors AS (
    SELECT
        ga.account_id,
        bp.business_id,
        bp.payee_id,
        bp.payee_name,
        bp.l30_ach_spend,
        bp.l90_ach_spend
    FROM greg_accounts ga
    JOIN analytics.agg.agg_bill_pay_spend__business_payee_day bp
        ON bp.business_id = ga.business_id
    WHERE bp.is_card_eligible = TRUE
      AND bp.l30_ach_spend > 0
      AND bp.date_day = (
          SELECT MAX(date_day)
          FROM analytics.agg.agg_bill_pay_spend__business_payee_day
      )
),
open_card_opps AS (
    SELECT DISTINCT opp.account_id, 1 AS has_open_card_opp
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name != 'S0: Holding'
      AND opp.expansion_subtype = 'Card Expansion'
),
card_spend AS (
    SELECT
        ga.account_id,
        SUM(tpv.card_tpv) AS card_l30d
    FROM greg_accounts ga
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = ga.business_id
        AND tpv.date_day >= CURRENT_DATE - 30
    GROUP BY ga.account_id
)
SELECT
    sa.account_name,
    av.account_id,
    av.payee_name                                                AS vendor_name,
    ROUND(av.l30_ach_spend)                                      AS ach_spend_l30d,
    ROUND(av.l90_ach_spend)                                      AS ach_spend_l90d,
    ROUND(av.l30_ach_spend * 0.015)                              AS est_cashback,
    COALESCE(oco.has_open_card_opp, 0)                           AS has_open_card_opp,
    ROUND(COALESCE(cs.card_l30d, 0))                             AS card_l30d
FROM ach_vendors av
JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = av.account_id
LEFT JOIN open_card_opps oco ON oco.account_id = av.account_id
LEFT JOIN card_spend cs ON cs.account_id = av.account_id
ORDER BY av.l30_ach_spend DESC
"""

STALE_OPPS_QUERY = """
WITH greg_open_opps AS (
    SELECT
        opp.opportunity_id,
        opp.account_id,
        opp.opportunity_name,
        opp.expansion_subtype,
        opp.opportunity_stage_name,
        opp.opportunity_close_date,
        opp.opportunity_created_at::date AS created_date,
        DATEDIFF('day', opp.opportunity_created_at::date, CURRENT_DATE) AS days_open,
        sa.account_name,
        sa.business_id,
        opp.monthly_expansion_amount
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name != 'S0: Holding'
      AND opp.expansion_subtype IN (
          'Card Expansion','Bill Pay Expansion','Travel Expansion','Treasury Expansion'
      )
),
baseline_spend AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS baseline_val
    FROM greg_open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day BETWEEN
            IFF(g.days_open <= 45,
                DATEADD('day', -30, g.created_date),
                CURRENT_DATE - 60)
            AND
            IFF(g.days_open <= 45,
                DATEADD('day', -1, g.created_date),
                CURRENT_DATE - 31)
    GROUP BY g.opportunity_id, g.expansion_subtype
),
recent_spend AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS recent_val
    FROM greg_open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= CURRENT_DATE - 30
    GROUP BY g.opportunity_id, g.expansion_subtype
),
last_email AS (
    SELECT
        sfdc_account_id AS account_id,
        last_email_created_at::date                                     AS last_email_date,
        DATEDIFF('day', last_email_created_at::date, CURRENT_DATE)     AS days_since_email,
        last_email_direction,
        first_sfdc_email_subject                                        AS last_email_subject,
        thread_owner_full_name                                          AS last_email_sender,
        historical_thread_owner_role                                    AS last_email_sender_role,
        number_messages_has_painpoints                                  AS painpoint_count
    FROM analytics.marts.dim_email_threads
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sfdc_account_id ORDER BY last_email_created_at DESC) = 1
),
last_call AS (
    SELECT
        gt.account_id,
        MAX(gt.call_start)::date                                        AS last_call_date,
        DATEDIFF('day', MAX(gt.call_start)::date, CURRENT_DATE)        AS days_since_call
    FROM analytics.marts.dim_sfdc_gong_transcripts gt
    JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
        ON ledger.account_id = gt.account_id
        AND ledger.date_day = CURRENT_DATE - 1
        AND ledger.owner_name = 'Gregory Nallie'
    GROUP BY 1
),
last_call_detail AS (
    SELECT
        rc.account_id,
        rc.call_id,
        rc.call_name                                                    AS last_call_name,
        ROUND(rc.call_duration_sec / 60)                                AS last_call_duration_min,
        LISTAGG(
            CASE WHEN gs.section_text IS NOT NULL AND gs.section_text != ''
                 THEN gs.section_name || ': ' || gs.section_text
            END, ' || '
        ) WITHIN GROUP (ORDER BY gs.section_index)                      AS last_call_section_text,
        LISTAGG(
            CASE WHEN gs.product_request_text IS NOT NULL AND gs.product_request_text != ''
                 THEN gs.product_request_text
            END, ' | '
        ) WITHIN GROUP (ORDER BY gs.section_index)                      AS last_call_product_requests,
        LISTAGG(DISTINCT NULLIF(gs.competitor_mentioned, ''), ', ')     AS last_call_competitors
    FROM (
        SELECT gt.account_id, gt.call_id, gt.call_name, gt.call_duration_sec
        FROM analytics.marts.dim_sfdc_gong_transcripts gt
        JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
            ON ledger.account_id = gt.account_id
            AND ledger.date_day = CURRENT_DATE - 1
            AND ledger.owner_name = 'Gregory Nallie'
        WHERE gt.call_duration_sec >= 180
        QUALIFY ROW_NUMBER() OVER (PARTITION BY gt.account_id ORDER BY gt.call_start DESC) = 1
    ) rc
    JOIN analytics.marts.dim_gong_section_summary gs ON gs.call_id = rc.call_id
    GROUP BY rc.account_id, rc.call_id, rc.call_name, rc.call_duration_sec
)
SELECT
    g.account_name,
    g.account_id,
    g.opportunity_id,
    g.opportunity_name,
    g.expansion_subtype,
    g.opportunity_stage_name,
    g.opportunity_close_date,
    g.monthly_expansion_amount,
    g.created_date,
    g.days_open,
    ROUND(COALESCE(b.baseline_val, 0))               AS baseline_spend,
    ROUND(COALESCE(r.recent_val, 0))                  AS recent_30d_spend,
    ROUND(
        CASE WHEN COALESCE(b.baseline_val, 0) > 0
             THEN 100.0 * COALESCE(r.recent_val, 0) / b.baseline_val
             ELSE NULL END
    )                                                  AS pct_of_baseline,
    CASE
        WHEN COALESCE(r.recent_val, 0) = 0                                  THEN 'No spend yet'
        WHEN COALESCE(r.recent_val, 0) < COALESCE(b.baseline_val, 0) * 0.5 THEN 'Very low'
        WHEN COALESCE(r.recent_val, 0) < COALESCE(b.baseline_val, 0)       THEN 'Below baseline'
        WHEN COALESCE(r.recent_val, 0) < COALESCE(b.baseline_val, 0) * 1.2 THEN 'Near baseline'
        ELSE 'Exceeding baseline'
    END                                                AS activation_status,
    le.last_email_date,
    le.days_since_email,
    le.last_email_direction,
    le.last_email_subject,
    lc.last_call_date,
    lc.days_since_call,
    GREATEST(
        COALESCE(le.last_email_date, '2000-01-01'),
        COALESCE(lc.last_call_date,  '2000-01-01')
    )                                                  AS last_touch_date,
    DATEDIFF('day',
        GREATEST(
            COALESCE(le.last_email_date, '2000-01-01'),
            COALESCE(lc.last_call_date,  '2000-01-01')
        ),
        CURRENT_DATE
    )                                                  AS days_since_last_touch,
    lcd.last_call_name,
    lcd.last_call_duration_min,
    lcd.last_call_section_text,
    lcd.last_call_product_requests,
    lcd.last_call_competitors
FROM greg_open_opps g
LEFT JOIN baseline_spend      b   ON b.opportunity_id  = g.opportunity_id
LEFT JOIN recent_spend        r   ON r.opportunity_id  = g.opportunity_id
LEFT JOIN last_email          le  ON le.account_id     = g.account_id
LEFT JOIN last_call           lc  ON lc.account_id     = g.account_id
LEFT JOIN last_call_detail    lcd ON lcd.account_id    = g.account_id
WHERE DATEDIFF('day',
    GREATEST(
        COALESCE(le.last_email_date, '2000-01-01'),
        COALESCE(lc.last_call_date,  '2000-01-01')
    ),
    CURRENT_DATE
) >= 21
ORDER BY days_since_last_touch DESC
"""

CLEANUP_OPPS_QUERY = """
WITH greg_open_opps AS (
    SELECT
        opp.opportunity_id,
        opp.account_id,
        opp.opportunity_name,
        opp.expansion_subtype,
        opp.opportunity_stage_name,
        opp.opportunity_close_date,
        opp.opportunity_created_at::date AS created_date,
        DATEDIFF('day', opp.opportunity_created_at::date, CURRENT_DATE) AS days_open,
        sa.account_name,
        sa.business_id,
        opp.monthly_expansion_amount
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.expansion_subtype IN (
          'Card Expansion','Bill Pay Expansion','Travel Expansion','Treasury Expansion'
      )
),
last_email AS (
    SELECT
        sfdc_account_id AS account_id,
        last_email_created_at::date                                     AS last_email_date,
        DATEDIFF('day', last_email_created_at::date, CURRENT_DATE)     AS days_since_email,
        last_email_direction,
        first_sfdc_email_subject                                        AS last_email_subject,
        thread_owner_full_name                                          AS last_email_sender,
        historical_thread_owner_role                                    AS last_email_sender_role,
        number_messages_has_painpoints                                  AS painpoint_count
    FROM analytics.marts.dim_email_threads
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sfdc_account_id ORDER BY last_email_created_at DESC) = 1
),
last_call AS (
    SELECT
        gt.account_id,
        MAX(gt.call_start)::date                                        AS last_call_date,
        DATEDIFF('day', MAX(gt.call_start)::date, CURRENT_DATE)        AS days_since_call
    FROM analytics.marts.dim_sfdc_gong_transcripts gt
    JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
        ON ledger.account_id = gt.account_id
        AND ledger.date_day = CURRENT_DATE - 1
        AND ledger.owner_name = 'Gregory Nallie'
    GROUP BY 1
),
recent_spend AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS recent_val
    FROM greg_open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= CURRENT_DATE - 30
    GROUP BY g.opportunity_id, g.expansion_subtype
)
SELECT
    g.account_name,
    g.account_id,
    g.opportunity_id,
    g.opportunity_name,
    g.expansion_subtype,
    g.opportunity_stage_name,
    g.opportunity_close_date,
    g.monthly_expansion_amount,
    g.created_date,
    g.days_open,
    g.business_id,
    ROUND(COALESCE(rs.recent_val, 0))                  AS recent_30d_spend,
    le.last_email_date,
    le.days_since_email,
    le.last_email_direction,
    le.last_email_subject,
    lc.last_call_date,
    lc.days_since_call,
    GREATEST(
        COALESCE(le.last_email_date, '2000-01-01'),
        COALESCE(lc.last_call_date,  '2000-01-01')
    )                                                  AS last_touch_date,
    DATEDIFF('day',
        GREATEST(
            COALESCE(le.last_email_date, '2000-01-01'),
            COALESCE(lc.last_call_date,  '2000-01-01')
        ),
        CURRENT_DATE
    )                                                  AS days_since_last_touch
FROM greg_open_opps g
LEFT JOIN recent_spend    rs  ON rs.opportunity_id  = g.opportunity_id
LEFT JOIN last_email      le  ON le.account_id     = g.account_id
LEFT JOIN last_call       lc  ON lc.account_id     = g.account_id
ORDER BY g.days_open DESC
"""

CLEANUP_GONG_QUERY = """
SELECT
    gt.call_id,
    gt.call_name,
    gt.call_start::date AS call_date,
    ROUND(gt.call_duration_sec / 60) AS duration_min,
    LISTAGG(DISTINCT NULLIF(gs.competitor_mentioned, ''), ', ')  AS competitors_mentioned,
    LISTAGG(DISTINCT NULLIF(gs.product_mentioned, ''), ', ')     AS products_mentioned,
    LISTAGG(
        CASE WHEN gs.section_text IS NOT NULL AND gs.section_text != ''
             THEN gs.section_name || ': ' || gs.section_text
        END, ' || '
    ) WITHIN GROUP (ORDER BY gs.section_index)                   AS full_section_text,
    LISTAGG(
        CASE WHEN gs.product_request_text IS NOT NULL AND gs.product_request_text != ''
             THEN gs.product_request_text
        END, ' | '
    ) WITHIN GROUP (ORDER BY gs.section_index)                   AS all_product_requests
FROM analytics.marts.dim_sfdc_gong_transcripts gt
JOIN analytics.marts.dim_gong_section_summary gs ON gs.call_id = gt.call_id
WHERE gt.account_id = '{account_id}'
  AND gt.call_start >= DATEADD('day', -120, CURRENT_DATE)
  AND gt.call_duration_sec >= 180
GROUP BY gt.call_id, gt.call_name, gt.call_start, gt.call_duration_sec
ORDER BY gt.call_start DESC
LIMIT 3
"""

CLEANUP_EMAIL_QUERY = """
SELECT
    et.last_email_created_at::date          AS email_date,
    et.last_email_direction                 AS direction,
    et.first_sfdc_email_subject             AS subject,
    et.thread_owner_full_name               AS sender,
    et.historical_thread_owner_role         AS sender_role,
    et.number_messages_has_painpoints       AS painpoint_count
FROM analytics.marts.dim_email_threads et
WHERE et.sfdc_account_id = '{account_id}'
QUALIFY ROW_NUMBER() OVER (PARTITION BY et.sfdc_account_id ORDER BY et.last_email_created_at DESC) <= 5
"""

PRIORITY_ACTIONS_QUERY = """
WITH greg_accounts AS (
    SELECT DISTINCT ledger.account_id, ledger.business_id
    FROM analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
    WHERE ledger.date_day = CURRENT_DATE - 1
      AND ledger.owner_name = 'Gregory Nallie'
),
month_meta AS (
    SELECT
        DATEDIFF('day', DATE_TRUNC('month', CURRENT_DATE), CURRENT_DATE) AS days_elapsed,
        DAY(LAST_DAY(CURRENT_DATE))                                       AS days_in_month
),
open_opps AS (
    SELECT opp.opportunity_id, opp.account_id, opp.expansion_subtype,
           opp.opportunity_stage_name,
           opp.opportunity_created_at::date AS created_date,
           DATEDIFF('day', opp.opportunity_created_at::date, CURRENT_DATE) AS days_open,
           opp.monthly_expansion_amount, sa.account_name, sa.business_id
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name != 'S0: Holding'
      AND opp.expansion_subtype IN (
          'Card Expansion','Bill Pay Expansion','Travel Expansion','Treasury Expansion')
),
last_touch AS (
    SELECT ga.account_id,
           GREATEST(COALESCE(MAX(et.last_email_created_at)::date,'2000-01-01'),
                    COALESCE(MAX(gt.call_start)::date,'2000-01-01')) AS last_touch_date,
           DATEDIFF('day',
               GREATEST(COALESCE(MAX(et.last_email_created_at)::date,'2000-01-01'),
                        COALESCE(MAX(gt.call_start)::date,'2000-01-01')),
               CURRENT_DATE) AS days_since_touch
    FROM greg_accounts ga
    LEFT JOIN analytics.marts.dim_email_threads et
        ON et.sfdc_account_id = ga.account_id
    LEFT JOIN analytics.marts.dim_sfdc_gong_transcripts gt
        ON gt.account_id = ga.account_id
    GROUP BY ga.account_id
),
baseline_per_opp AS (
    SELECT g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS baseline_val
    FROM open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day BETWEEN
            IFF(g.days_open<=45, DATEADD('day',-30,g.created_date), CURRENT_DATE-60)
            AND IFF(g.days_open<=45, DATEADD('day',-1,g.created_date), CURRENT_DATE-31)
    GROUP BY g.opportunity_id, g.expansion_subtype
),
mtd_per_opp AS (
    SELECT g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS mtd_val
    FROM open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= DATE_TRUNC('month', CURRENT_DATE)
        AND tpv.date_day < CURRENT_DATE
    GROUP BY g.opportunity_id, g.expansion_subtype
),
recent_per_opp AS (
    SELECT g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS recent_val
    FROM open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id AND tpv.date_day >= CURRENT_DATE - 30
    GROUP BY g.opportunity_id, g.expansion_subtype
),
candidates AS (
    SELECT
        'Close Today \u2014 Pacing Over Baseline' AS signal_type, 1 AS type_rank,
        oo.account_name, oo.account_id, oo.opportunity_id,
        oo.expansion_subtype AS product, oo.opportunity_stage_name AS stage,
        oo.days_open,
        ROUND(COALESCE(b.baseline_val,0)) AS baseline,
        ROUND(COALESCE(m.mtd_val,0)) AS mtd_or_l30d,
        ROUND(COALESCE(m.mtd_val,0)*mm.days_in_month/NULLIF(mm.days_elapsed,0)) AS paced_monthly,
        ROUND((COALESCE(m.mtd_val,0)*mm.days_in_month/NULLIF(mm.days_elapsed,0))-COALESCE(b.baseline_val,0)) AS dollar_over,
        mm.days_in_month - mm.days_elapsed AS days_left,
        lt.days_since_touch, lt.last_touch_date
    FROM open_opps oo CROSS JOIN month_meta mm
    LEFT JOIN baseline_per_opp b ON b.opportunity_id = oo.opportunity_id
    LEFT JOIN mtd_per_opp m ON m.opportunity_id = oo.opportunity_id
    LEFT JOIN last_touch lt ON lt.account_id = oo.account_id
    WHERE (COALESCE(m.mtd_val,0)*mm.days_in_month/NULLIF(mm.days_elapsed,0))
          > COALESCE(b.baseline_val,0)*1.2
      AND (COALESCE(m.mtd_val,0)*mm.days_in_month/NULLIF(mm.days_elapsed,0)) > 3000

    UNION ALL

    SELECT
        'Close Now \u2014 L30D Exceeding Baseline' AS signal_type, 2 AS type_rank,
        oo.account_name, oo.account_id, oo.opportunity_id,
        oo.expansion_subtype AS product, oo.opportunity_stage_name AS stage,
        oo.days_open,
        ROUND(COALESCE(b.baseline_val,0)) AS baseline,
        ROUND(COALESCE(r.recent_val,0)) AS mtd_or_l30d,
        NULL AS paced_monthly,
        ROUND(COALESCE(r.recent_val,0)-COALESCE(b.baseline_val,0)) AS dollar_over,
        mm.days_in_month - mm.days_elapsed AS days_left,
        lt.days_since_touch, lt.last_touch_date
    FROM open_opps oo CROSS JOIN month_meta mm
    LEFT JOIN baseline_per_opp b ON b.opportunity_id = oo.opportunity_id
    LEFT JOIN recent_per_opp r ON r.opportunity_id = oo.opportunity_id
    LEFT JOIN last_touch lt ON lt.account_id = oo.account_id
    WHERE COALESCE(r.recent_val,0) > COALESCE(b.baseline_val,0)*1.1
      AND COALESCE(r.recent_val,0)-COALESCE(b.baseline_val,0) > 3000
      AND oo.opportunity_id NOT IN (
          SELECT oo2.opportunity_id FROM open_opps oo2 CROSS JOIN month_meta mm2
          LEFT JOIN baseline_per_opp b2 ON b2.opportunity_id = oo2.opportunity_id
          LEFT JOIN mtd_per_opp m2 ON m2.opportunity_id = oo2.opportunity_id
          WHERE (COALESCE(m2.mtd_val,0)*mm2.days_in_month/NULLIF(mm2.days_elapsed,0))
                > COALESCE(b2.baseline_val,0)*1.2
            AND (COALESCE(m2.mtd_val,0)*mm2.days_in_month/NULLIF(mm2.days_elapsed,0)) > 3000
      )

    UNION ALL

    SELECT
        'Open Opp \u2014 Spend Accelerating' AS signal_type, 3 AS type_rank,
        sa.account_name, sa.account_id, NULL AS opportunity_id,
        CASE
            WHEN (ss.current_billpay-ss.baseline_billpay) >= GREATEST(ss.current_card-ss.baseline_card, ss.current_billpay-ss.baseline_billpay, ss.current_travel-ss.baseline_travel)
                THEN 'Bill Pay Expansion'
            WHEN (ss.current_card-ss.baseline_card) >= GREATEST(ss.current_card-ss.baseline_card, ss.current_billpay-ss.baseline_billpay, ss.current_travel-ss.baseline_travel)
                THEN 'Card Expansion'
            ELSE 'Travel Expansion'
        END AS product,
        NULL AS stage, NULL AS days_open,
        GREATEST(COALESCE(ss.baseline_card,0), COALESCE(ss.baseline_billpay,0)) AS baseline,
        GREATEST(COALESCE(ss.current_card,0), COALESCE(ss.current_billpay,0)) AS mtd_or_l30d,
        NULL AS paced_monthly,
        GREATEST(ss.current_card-ss.baseline_card, ss.current_billpay-ss.baseline_billpay, ss.current_travel-ss.baseline_travel) AS dollar_over,
        mm.days_in_month - mm.days_elapsed AS days_left,
        lt.days_since_touch, lt.last_touch_date
    FROM (
        SELECT ga.account_id, ga.business_id,
            SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE-60 AND CURRENT_DATE-31 THEN tpv.card_tpv    ELSE 0 END) AS baseline_card,
            SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE-60 AND CURRENT_DATE-31 THEN tpv.billpay_tpv ELSE 0 END) AS baseline_billpay,
            SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE-60 AND CURRENT_DATE-31 THEN tpv.travel_tpv  ELSE 0 END) AS baseline_travel,
            SUM(CASE WHEN tpv.date_day >= CURRENT_DATE-30 THEN tpv.card_tpv    ELSE 0 END) AS current_card,
            SUM(CASE WHEN tpv.date_day >= CURRENT_DATE-30 THEN tpv.billpay_tpv ELSE 0 END) AS current_billpay,
            SUM(CASE WHEN tpv.date_day >= CURRENT_DATE-30 THEN tpv.travel_tpv  ELSE 0 END) AS current_travel
        FROM analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        JOIN greg_accounts ga ON ga.business_id = tpv.business_id
        WHERE tpv.date_day >= CURRENT_DATE - 60
        GROUP BY ga.account_id, ga.business_id
    ) ss
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.business_id = ss.business_id
    CROSS JOIN month_meta mm
    LEFT JOIN last_touch lt ON lt.account_id = sa.account_id
    WHERE sa.account_id NOT IN (
        SELECT DISTINCT account_id FROM analytics.marts.dim_sfdc_opportunities
        WHERE opportunity_is_closed = FALSE AND opportunity_type = 'Expansion'
          AND opportunity_owner = 'Gregory Nallie'
          AND opportunity_stage_name != 'S0: Holding'
    )
    AND GREATEST(ss.current_card-ss.baseline_card, ss.current_billpay-ss.baseline_billpay,
                 ss.current_travel-ss.baseline_travel) > 5000
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY type_rank ASC, dollar_over DESC
        ) AS rn
    FROM candidates
)
SELECT
    signal_type, account_name, account_id, opportunity_id,
    product, stage, days_open, baseline, mtd_or_l30d, paced_monthly,
    dollar_over, days_left, days_since_touch, last_touch_date,
    ROUND(
        LEAST(50, LN(GREATEST(dollar_over, 1)) * 4.0)
        + CASE type_rank WHEN 1 THEN 30 WHEN 2 THEN 20 ELSE 10 END
        + CASE WHEN days_since_touch > 30 THEN 10
               WHEN days_since_touch > 14 THEN 5 ELSE 0 END
        + CASE WHEN days_left <= 5 THEN 10 WHEN days_left <= 10 THEN 7 ELSE 3 END
    ) AS priority_score
FROM deduped
WHERE rn = 1
ORDER BY priority_score DESC
LIMIT 20
"""

GONG_MEETINGS_QUERY = """
WITH greg_calls AS (
    SELECT
        gt.call_id,
        gt.account_id,
        sa.account_name,
        gt.call_name,
        gt.call_start::date AS call_date,
        ROUND(gt.call_duration_sec / 60) AS duration_min
    FROM analytics.marts.dim_sfdc_gong_transcripts gt
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = gt.account_id
    JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
        ON ledger.account_id = gt.account_id
        AND ledger.date_day = CURRENT_DATE - 1
        AND ledger.owner_name = 'Gregory Nallie'
    WHERE gt.call_start >= CURRENT_DATE - {lookback_days}
      AND gt.call_duration_sec >= 180
)
SELECT
    c.call_id,
    c.account_id,
    c.account_name,
    c.call_name,
    c.call_date,
    c.duration_min,
    LISTAGG(gs.section_name, ' | ')
        WITHIN GROUP (ORDER BY gs.section_index)           AS section_names,
    LISTAGG(DISTINCT NULLIF(gs.competitor_mentioned, ''), ', ')
                                                            AS competitors_mentioned,
    LISTAGG(DISTINCT NULLIF(gs.product_mentioned, ''), ', ')
                                                            AS products_mentioned,
    LISTAGG(
        CASE WHEN gs.section_text IS NOT NULL AND gs.section_text != ''
             THEN gs.section_name || ': ' || gs.section_text
        END, ' || '
    ) WITHIN GROUP (ORDER BY gs.section_index)             AS full_section_text,
    LISTAGG(
        CASE WHEN gs.product_request_text IS NOT NULL AND gs.product_request_text != ''
             THEN gs.product_request_text
        END, ' | '
    ) WITHIN GROUP (ORDER BY gs.section_index)             AS all_product_requests
FROM greg_calls c
LEFT JOIN analytics.marts.dim_gong_section_summary gs ON gs.call_id = c.call_id
GROUP BY c.call_id, c.account_id, c.account_name, c.call_name, c.call_date, c.duration_min
ORDER BY c.call_date DESC
LIMIT 30
"""

POST_MEETING_EMAIL_QUERY = """
SELECT
    et.sfdc_account_id AS account_id,
    et.last_email_created_at::date AS last_email_date,
    DATEDIFF('day', et.last_email_created_at::date, CURRENT_DATE) AS days_since_email,
    et.last_email_direction,
    et.first_sfdc_email_subject AS last_email_subject,
    et.thread_owner_full_name AS last_email_sender,
    et.historical_thread_owner_role AS last_email_sender_role
FROM analytics.marts.dim_email_threads et
WHERE et.sfdc_account_id IN ({account_ids})
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY et.sfdc_account_id ORDER BY et.last_email_created_at DESC
) = 1
"""

GREG_OPEN_OPPS_QUERY = """
WITH open AS (
    SELECT opp.opportunity_id, opp.account_id, opp.expansion_subtype,
           opp.opportunity_stage_name, opp.opportunity_close_date,
           opp.opportunity_created_at::date AS created_date,
           opp.monthly_expansion_amount, 'open' AS opp_status
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name != 'S0: Holding'
),
recently_closed AS (
    SELECT opp.opportunity_id, opp.account_id, opp.expansion_subtype,
           opp.opportunity_stage_name, opp.opportunity_close_date,
           opp.opportunity_created_at::date AS created_date,
           opp.monthly_expansion_amount, 'recently_closed_won' AS opp_status
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.opportunity_is_won = TRUE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_close_date >= CURRENT_DATE - 30
)
SELECT * FROM open UNION ALL SELECT * FROM recently_closed
"""

ACCOUNT_LOOKUP_QUERY = """
SELECT
    sa.account_name, sa.account_id, sa.business_id,
    bob.thirty_day_card_spend AS card_l30d,
    bob.rolling_30_day_paid_bill_amount AS billpay_l30d,
    bob.rolling_30_days_avg_treasury_available_balance_usd AS treasury_l30d,
    bob.card_fifth_transaction_cleared_at::date AS card_activated_at,
    bob.first_bill_paid_at::date AS billpay_first_paid_at,
    bob.treasury_first_payment_at::date AS treasury_activated_at,
    bob.travel_fifth_booking_at::date AS travel_activated_at
FROM analytics.marts.dim_sfdc_accounts sa
LEFT JOIN analytics.marts.dim_book_of_business_accounts_view bob
    ON bob.sfdc_account_id = sa.account_id
WHERE LOWER(sa.account_name) LIKE LOWER('%{search_term}%')
  AND sa.account_id IN (
      SELECT DISTINCT account_id
      FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
      WHERE date_day = CURRENT_DATE - 1
        AND owner_name = 'Gregory Nallie'
  )
ORDER BY sa.account_name
LIMIT 10
"""

ACCOUNT_OPPS_QUERY = """
SELECT
    opp.opportunity_id, opp.account_id, sa.account_name,
    opp.expansion_subtype, opp.opportunity_stage_name,
    opp.opportunity_close_date, opp.monthly_expansion_amount,
    opp.opportunity_created_at::date AS created_date,
    DATEDIFF('day', opp.opportunity_created_at::date, CURRENT_DATE) AS days_open
FROM analytics.marts.dim_sfdc_opportunities opp
JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
WHERE opp.opportunity_is_closed = FALSE
  AND opp.opportunity_type = 'Expansion'
  AND opp.opportunity_owner = 'Gregory Nallie'
  AND opp.opportunity_stage_name != 'S0: Holding'
ORDER BY opp.monthly_expansion_amount DESC
"""

# ── Full Gong Transcript (verbatim speaker turns) ───────────────────────────
GONG_FULL_TRANSCRIPT_QUERY = """
SELECT
    gtp.call_id,
    gc.sfdc_primary_account_id AS account_id,
    sa.account_name,
    gc.call_name,
    gc.gong_call_start::date AS call_date,
    ROUND(gc.gong_call_duration_sec / 60) AS duration_min,
    gtp.speaker_email,
    gtp.is_ramp_participant,
    gtp.paragraph_text,
    gtp.start_time_from_call_start AS paragraph_start_time_sec,
    gtp.paragraph_index
FROM analytics.marts.dim_gong_transcript_paragraph gtp
JOIN analytics.marts.dim_sfdc_gong_call gc ON gc.gong_call_id = gtp.call_id
JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = gc.sfdc_primary_account_id
WHERE gc.sfdc_primary_account_id = '{account_id}'
  AND gc.gong_call_start >= DATEADD('day', -{lookback_days}, CURRENT_DATE)
  AND gc.gong_call_duration_sec >= 180
ORDER BY gtp.call_id, gtp.paragraph_index
"""

# ── Full Gong Transcript for post-meeting (Greg's recent calls) ─────────────
GONG_MEETINGS_FULL_TRANSCRIPT_QUERY = """
WITH greg_calls AS (
    SELECT DISTINCT
        gc.gong_call_id AS call_id,
        gc.sfdc_primary_account_id AS account_id,
        sa.account_name,
        gc.call_name,
        gc.gong_call_start::date AS call_date,
        ROUND(gc.gong_call_duration_sec / 60) AS duration_min
    FROM analytics.marts.dim_sfdc_gong_call gc
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = gc.sfdc_primary_account_id
    JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
        ON ledger.account_id = gc.sfdc_primary_account_id
        AND ledger.date_day = CURRENT_DATE - 1
        AND ledger.owner_name = 'Gregory Nallie'
    WHERE gc.gong_call_start >= CURRENT_DATE - {lookback_days}
      AND gc.gong_call_duration_sec >= 180
      AND EXISTS (
          SELECT 1 FROM analytics.marts.dim_gong_transcript_paragraph p
          WHERE p.call_id = gc.gong_call_id
            AND LOWER(p.speaker_email) = 'gnallie@ramp.com'
      )
)
SELECT
    c.call_id,
    c.account_id,
    c.account_name,
    c.call_name,
    c.call_date,
    c.duration_min,
    gtp.speaker_email,
    gtp.is_ramp_participant,
    gtp.paragraph_text,
    gtp.start_time_from_call_start AS paragraph_start_time_sec,
    gtp.paragraph_index
FROM greg_calls c
JOIN analytics.marts.dim_gong_transcript_paragraph gtp ON gtp.call_id = c.call_id
ORDER BY c.call_date DESC, c.call_id, gtp.paragraph_index
"""

# ── SFDC Account Notes (AM/CSM freeform notes) ─────────────────────────────
ACCOUNT_NOTES_QUERY = """
SELECT
    sa.account_id,
    sa.account_name,
    sa.am_notes,
    sa.am_next_steps,
    sa.csm_notes,
    sa.csm_next_steps
FROM analytics.marts.dim_sfdc_accounts sa
WHERE sa.account_id IN ({account_ids})
"""

# ── Full Email Comms (Outreach/SFDC-logged emails with body text) ───────────
ACCOUNT_EMAILS_FULL_QUERY = """
SELECT
    e.account_id,
    e.sfdc_email_created_at::date AS email_date,
    e.email_direction AS direction,
    e.email_subject AS subject,
    COALESCE(e.email_body_clean, e.email_body) AS body_text,
    e.sfdc_email_owner_email AS sender_email,
    e.ramp_employee_team AS sender_team,
    e.external_contact_name,
    e.external_contact_email,
    e.contact_persona,
    e.email_thread_id,
    e.has_willing_to_meet,
    e.has_not_interested,
    e.has_painpoints,
    e.has_interested,
    e.has_need_information,
    e.has_out_of_office,
    e.email_replied_at IS NOT NULL AS got_reply,
    e.outreach_sequence_name
FROM analytics.marts.dim_emails e
WHERE e.account_id IN ({account_ids})
  AND e.sfdc_email_created_at >= DATEADD('day', -90, CURRENT_DATE)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY e.account_id
    ORDER BY e.sfdc_email_created_at DESC
) <= 15
"""

# ── Post-Meeting: Calls with follow-up email check ─────────────────────────
POST_MEETING_CALLS_QUERY = """
WITH greg_calls AS (
    SELECT
        gc.gong_call_id AS call_id,
        gc.sfdc_primary_account_id AS account_id,
        sa.account_name,
        gc.call_name,
        gc.gong_call_start AS call_start,
        gc.gong_call_start::date AS call_date,
        ROUND(gc.gong_call_duration_sec / 60) AS duration_min,
        gc.sfdc_primary_opportunity_id AS opp_id
    FROM analytics.marts.dim_sfdc_gong_call gc
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = gc.sfdc_primary_account_id
    JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
        ON ledger.account_id = gc.sfdc_primary_account_id
        AND ledger.date_day = CURRENT_DATE - 1
        AND ledger.owner_name = 'Gregory Nallie'
    WHERE gc.gong_call_start >= CURRENT_DATE - {lookback_days}
      AND gc.gong_call_duration_sec >= 180
      AND EXISTS (
          SELECT 1 FROM analytics.marts.dim_gong_transcript_paragraph p
          WHERE p.call_id = gc.gong_call_id
            AND LOWER(p.speaker_email) = 'gnallie@ramp.com'
      )
),
follow_up_emails AS (
    SELECT
        e.account_id,
        gc.call_id,
        MIN(e.sfdc_email_created_at) AS first_followup_at,
        COUNT(*) AS followup_count,
        LISTAGG(DISTINCT e.sfdc_email_owner_email, ', ') AS followup_senders
    FROM analytics.marts.dim_emails e
    JOIN greg_calls gc
        ON gc.account_id = e.account_id
        AND e.sfdc_email_created_at BETWEEN gc.call_start AND DATEADD('hour', 48, gc.call_start)
        AND e.email_direction = 'Outbound'
    GROUP BY e.account_id, gc.call_id
),
open_opps AS (
    SELECT account_id, expansion_subtype, opportunity_stage_name, opportunity_id,
           opportunity_close_date,
           DATEDIFF('day', opportunity_created_at::date, CURRENT_DATE) AS days_open
    FROM analytics.marts.dim_sfdc_opportunities
    WHERE opportunity_is_closed = FALSE
      AND opportunity_type = 'Expansion'
      AND opportunity_owner = 'Gregory Nallie'
      AND opportunity_stage_name != 'S0: Holding'
),
opp_agg AS (
    SELECT account_id,
           COUNT(*) AS opp_count,
           LISTAGG(expansion_subtype, ', ') WITHIN GROUP (ORDER BY expansion_subtype) AS opp_products
    FROM open_opps
    GROUP BY account_id
),
section_summaries AS (
    SELECT
        gs.call_id,
        LISTAGG(DISTINCT NULLIF(gs.competitor_mentioned, ''), ', ') AS competitors_mentioned,
        LISTAGG(DISTINCT NULLIF(gs.product_mentioned, ''), ', ') AS products_mentioned,
        LISTAGG(
            CASE WHEN gs.section_text IS NOT NULL AND gs.section_text != ''
                 THEN gs.section_name || ': ' || gs.section_text END,
            ' || '
        ) WITHIN GROUP (ORDER BY gs.section_index) AS full_section_text,
        LISTAGG(
            CASE WHEN gs.product_request_text IS NOT NULL AND gs.product_request_text != ''
                 THEN gs.product_request_text END,
            ' | '
        ) WITHIN GROUP (ORDER BY gs.section_index) AS all_product_requests
    FROM analytics.marts.dim_gong_section_summary gs
    WHERE gs.call_id IN (SELECT call_id FROM greg_calls)
    GROUP BY gs.call_id
),
stage_changes AS (
    SELECT oo.account_id,
           MAX(oo.opportunity_id) AS latest_opp_id,
           MAX(oo.opportunity_stage_name) AS latest_stage
    FROM open_opps oo
    GROUP BY oo.account_id
)
SELECT
    c.call_id,
    c.account_id,
    c.account_name,
    c.call_name,
    c.call_date,
    c.duration_min,
    c.opp_id AS linked_opp_id,
    fe.first_followup_at,
    fe.followup_count,
    CASE WHEN fe.call_id IS NULL THEN TRUE ELSE FALSE END AS missing_followup,
    oa.opp_count,
    oa.opp_products,
    CASE WHEN oa.account_id IS NULL THEN TRUE ELSE FALSE END AS no_open_opp,
    ss.competitors_mentioned,
    ss.products_mentioned,
    ss.full_section_text,
    ss.all_product_requests,
    sc.latest_opp_id,
    sc.latest_stage
FROM greg_calls c
LEFT JOIN follow_up_emails fe ON fe.call_id = c.call_id
LEFT JOIN opp_agg oa ON oa.account_id = c.account_id
LEFT JOIN section_summaries ss ON ss.call_id = c.call_id
LEFT JOIN stage_changes sc ON sc.account_id = c.account_id
ORDER BY c.call_date DESC
"""

# ── Opp Pacing: Milestones for open opps ────────────────────────────────────
OPP_PACING_MILESTONES_QUERY = """
WITH greg_open_opps AS (
    SELECT
        opp.opportunity_id,
        opp.account_id,
        opp.expansion_subtype,
        opp.opportunity_stage_name,
        opp.opportunity_close_date,
        opp.opportunity_created_at::date AS created_date,
        DATEDIFF('day', opp.opportunity_created_at::date, CURRENT_DATE) AS days_open,
        opp.monthly_expansion_amount,
        sa.account_name,
        sa.business_id
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name != 'S0: Holding'
      AND opp.expansion_subtype IN (
          'Card Expansion', 'Bill Pay Expansion',
          'Travel Expansion', 'Treasury Expansion'
      )
),
milestones AS (
    SELECT
        bob.sfdc_account_id AS account_id,
        bob.card_fifth_transaction_cleared_at::date AS card_activated_at,
        bob.first_bill_paid_at::date AS billpay_first_paid_at,
        bob.bill_pay_third_bill_paid_at::date AS billpay_third_paid_at,
        bob.treasury_first_payment_at::date AS treasury_activated_at,
        bob.travel_fifth_booking_at::date AS travel_activated_at,
        bob.thirty_day_card_spend AS card_l30d,
        bob.rolling_30_day_paid_bill_amount AS billpay_l30d,
        bob.rolling_30_days_avg_treasury_available_balance_usd AS treasury_l30d
    FROM analytics.marts.dim_book_of_business_accounts_view bob
    WHERE bob.sfdc_account_id IN (SELECT account_id FROM greg_open_opps)
),
baseline_spend AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS baseline
    FROM greg_open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day BETWEEN
            IFF(g.days_open <= 45,
                DATEADD('day', -30, g.created_date),
                CURRENT_DATE - 60)
            AND
            IFF(g.days_open <= 45,
                DATEADD('day', -1, g.created_date),
                CURRENT_DATE - 31)
    GROUP BY g.opportunity_id, g.expansion_subtype
),
recent_spend AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS recent_val
    FROM greg_open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= CURRENT_DATE - 30
    GROUP BY g.opportunity_id, g.expansion_subtype
),
last_touch AS (
    SELECT ga.account_id,
           GREATEST(
               COALESCE(MAX(et.last_email_created_at)::date, '2000-01-01'),
               COALESCE(MAX(gt.call_start)::date, '2000-01-01')
           ) AS last_touch_date
    FROM greg_open_opps ga
    LEFT JOIN analytics.marts.dim_email_threads et
        ON et.sfdc_account_id = ga.account_id
    LEFT JOIN analytics.marts.dim_sfdc_gong_transcripts gt
        ON gt.account_id = ga.account_id
    GROUP BY ga.account_id
)
SELECT
    g.opportunity_id,
    g.account_id,
    g.account_name,
    g.expansion_subtype,
    g.opportunity_stage_name,
    g.opportunity_close_date,
    g.created_date,
    g.days_open,
    g.monthly_expansion_amount,
    ROUND(COALESCE(bs.baseline, 0)) AS baseline_spend,
    ROUND(COALESCE(rs.recent_val, 0)) AS recent_l30d,
    ROUND(COALESCE(rs.recent_val, 0) - COALESCE(bs.baseline, 0)) AS over_baseline,
    m.card_activated_at,
    m.billpay_first_paid_at,
    m.billpay_third_paid_at,
    m.treasury_activated_at,
    m.travel_activated_at,
    m.card_l30d,
    m.billpay_l30d,
    m.treasury_l30d,
    lt.last_touch_date,
    DATEDIFF('day', lt.last_touch_date, CURRENT_DATE) AS days_since_touch,
    DATEDIFF('day', CURRENT_DATE, g.opportunity_close_date) AS days_to_close,
    -- Milestone relevance per product
    CASE g.expansion_subtype
        WHEN 'Card Expansion'     THEN m.card_activated_at
        WHEN 'Bill Pay Expansion' THEN m.billpay_first_paid_at
        WHEN 'Travel Expansion'   THEN m.travel_activated_at
        WHEN 'Treasury Expansion' THEN m.treasury_activated_at
    END AS relevant_milestone_date,
    CASE g.expansion_subtype
        WHEN 'Card Expansion'     THEN 'Card 5th transaction'
        WHEN 'Bill Pay Expansion' THEN 'First bill paid'
        WHEN 'Travel Expansion'   THEN 'Travel 5th booking'
        WHEN 'Treasury Expansion' THEN 'Treasury first payment'
    END AS relevant_milestone_name
FROM greg_open_opps g
LEFT JOIN milestones m ON m.account_id = g.account_id
LEFT JOIN baseline_spend bs ON bs.opportunity_id = g.opportunity_id
LEFT JOIN recent_spend rs ON rs.opportunity_id = g.opportunity_id
LEFT JOIN last_touch lt ON lt.account_id = g.account_id
ORDER BY g.opportunity_close_date ASC
"""

# ── Forecasting: S3+ closing this month + S2 with recent activity ───────────
FORECASTING_PIPELINE_QUERY = """
WITH greg_opps AS (
    SELECT
        opp.opportunity_id,
        opp.account_id,
        opp.expansion_subtype,
        opp.opportunity_stage_name,
        opp.opportunity_close_date,
        opp.opportunity_created_at::date AS created_date,
        DATEDIFF('day', opp.opportunity_created_at::date, CURRENT_DATE) AS days_open,
        opp.monthly_expansion_amount,
        sa.account_name,
        sa.business_id,
        CASE
            WHEN opp.opportunity_stage_name IN ('S3: Proposal/Negotiation', 'S4: Commit',
                 'S3: Proposal / Negotiation')
                 AND opp.opportunity_close_date <= LAST_DAY(CURRENT_DATE)
            THEN 'closing_this_month'
            WHEN opp.opportunity_stage_name IN ('S2: Discovery', 'S2: Qualification')
            THEN 's2_candidate'
            ELSE 'other'
        END AS forecast_group
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name NOT IN ('S0: Holding', 'S1: Connect')
      AND opp.expansion_subtype IN (
          'Card Expansion', 'Bill Pay Expansion',
          'Travel Expansion', 'Treasury Expansion'
      )
),
last_activity AS (
    SELECT
        ga.account_id,
        MAX(gt.call_start)::date AS last_call_date,
        MAX(et.last_email_created_at)::date AS last_email_date,
        GREATEST(
            COALESCE(MAX(gt.call_start)::date, '2000-01-01'),
            COALESCE(MAX(et.last_email_created_at)::date, '2000-01-01')
        ) AS last_activity_date,
        DATEDIFF('day',
            GREATEST(
                COALESCE(MAX(gt.call_start)::date, '2000-01-01'),
                COALESCE(MAX(et.last_email_created_at)::date, '2000-01-01')
            ),
            CURRENT_DATE
        ) AS days_since_activity
    FROM greg_opps ga
    LEFT JOIN analytics.marts.dim_sfdc_gong_transcripts gt ON gt.account_id = ga.account_id
    LEFT JOIN analytics.marts.dim_email_threads et
        ON et.sfdc_account_id = ga.account_id
    GROUP BY ga.account_id
),
recent_gong AS (
    SELECT
        gs.call_id,
        gt.account_id,
        gt.call_name,
        gt.call_start::date AS call_date,
        LISTAGG(DISTINCT NULLIF(gs.competitor_mentioned, ''), ', ') AS competitors,
        LISTAGG(DISTINCT NULLIF(gs.product_mentioned, ''), ', ') AS products_discussed,
        LISTAGG(
            CASE WHEN gs.product_request_text IS NOT NULL AND gs.product_request_text != ''
                 THEN gs.product_request_text END, ' | '
        ) WITHIN GROUP (ORDER BY gs.section_index) AS product_requests,
        LISTAGG(
            CASE WHEN gs.section_text IS NOT NULL AND gs.section_text != ''
                 THEN gs.section_name || ': ' || gs.section_text END, ' || '
        ) WITHIN GROUP (ORDER BY gs.section_index) AS section_text
    FROM analytics.marts.dim_sfdc_gong_transcripts gt
    JOIN analytics.marts.dim_gong_section_summary gs ON gs.call_id = gt.call_id
    WHERE gt.account_id IN (SELECT account_id FROM greg_opps)
      AND gt.call_start >= DATEADD('day', -30, CURRENT_DATE)
      AND gt.call_duration_sec >= 180
    GROUP BY gs.call_id, gt.account_id, gt.call_name, gt.call_start
    QUALIFY ROW_NUMBER() OVER (PARTITION BY gt.account_id ORDER BY gt.call_start DESC) = 1
),
recent_email AS (
    SELECT
        e.account_id,
        e.sfdc_email_created_at::date AS last_email_date,
        e.email_direction AS direction,
        e.email_subject AS subject,
        e.sfdc_email_owner_email AS sender_email,
        e.ramp_employee_team AS sender_team,
        e.has_willing_to_meet,
        e.has_not_interested,
        e.has_painpoints,
        e.has_interested
    FROM analytics.marts.dim_emails e
    WHERE e.account_id IN (SELECT account_id FROM greg_opps)
      AND e.sfdc_email_created_at >= DATEADD('day', -14, CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.account_id ORDER BY e.sfdc_email_created_at DESC) = 1
),
baseline_spend AS (
    SELECT g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS baseline_val
    FROM greg_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day BETWEEN
            IFF(g.days_open<=45, DATEADD('day',-30,g.created_date), CURRENT_DATE-60)
            AND IFF(g.days_open<=45, DATEADD('day',-1,g.created_date), CURRENT_DATE-31)
    GROUP BY g.opportunity_id, g.expansion_subtype
),
recent_spend AS (
    SELECT g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS recent_val
    FROM greg_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id AND tpv.date_day >= CURRENT_DATE - 30
    GROUP BY g.opportunity_id, g.expansion_subtype
)
SELECT
    g.opportunity_id,
    g.account_id,
    g.account_name,
    g.expansion_subtype,
    g.opportunity_stage_name,
    g.opportunity_close_date,
    g.days_open,
    g.monthly_expansion_amount,
    g.forecast_group,
    DATEDIFF('day', CURRENT_DATE, g.opportunity_close_date) AS days_to_close,
    la.last_activity_date,
    la.days_since_activity,
    la.last_call_date,
    la.last_email_date,
    rg.call_name AS recent_call_name,
    rg.call_date AS recent_call_date,
    rg.competitors,
    rg.products_discussed,
    rg.product_requests,
    rg.section_text AS recent_call_summary,
    re.last_email_date AS recent_email_date,
    re.direction AS recent_email_direction,
    re.subject AS recent_email_subject,
    re.has_willing_to_meet,
    re.has_not_interested,
    ROUND(COALESCE(bs.baseline_val, 0)) AS baseline,
    ROUND(COALESCE(rs.recent_val, 0)) AS recent_l30d,
    ROUND(COALESCE(rs.recent_val, 0) - COALESCE(bs.baseline_val, 0)) AS over_baseline
FROM greg_opps g
LEFT JOIN last_activity la ON la.account_id = g.account_id
LEFT JOIN recent_gong rg ON rg.account_id = g.account_id
LEFT JOIN recent_email re ON re.account_id = g.account_id
LEFT JOIN baseline_spend bs ON bs.opportunity_id = g.opportunity_id
LEFT JOIN recent_spend rs ON rs.opportunity_id = g.opportunity_id
WHERE g.forecast_group IN ('closing_this_month', 's2_candidate')
  AND (
      g.forecast_group = 'closing_this_month'
      OR (g.forecast_group = 's2_candidate' AND la.days_since_activity <= 14)
  )
ORDER BY
    CASE g.forecast_group WHEN 'closing_this_month' THEN 1 ELSE 2 END,
    g.opportunity_close_date ASC
"""

REOPEN_QUERY = """
WITH greg_cw_opps AS (
    SELECT
        o.opportunity_id,
        o.account_id,
        o.expansion_subtype,
        o.opportunity_closed_won_date::date AS cw_date,
        o.opportunity_created_at::date      AS created_date,
        o.monthly_expansion_amount,
        sa.account_name,
        sa.business_id,
        CASE o.expansion_subtype
            WHEN 'Card Expansion'     THEN es.expansion_opportunity_30_day_transaction_amount_tpv_before_closed_won_date_prior
            WHEN 'Bill Pay Expansion' THEN es.expansion_opportunity_30_day_bill_pay_non_card_tpv_revops_amount_before_closed_won_date_prior
            WHEN 'Travel Expansion'   THEN es.expansion_opportunity_30_day_travel_amount_before_closed_won_date_prior
            WHEN 'Treasury Expansion' THEN es.expansion_opportunity_30_day_avg_treasury_available_balance_before_closed_won_date_prior
        END AS baseline_at_close,
        DATEDIFF('day', o.opportunity_closed_won_date, CURRENT_DATE) AS days_since_cw
    FROM analytics.marts.dim_sfdc_opportunities o
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = o.account_id
    JOIN analytics.marts.agg_sfdc_expansion_opportunity_spend es ON es.opportunity_id = o.opportunity_id
    WHERE o.opportunity_is_won = TRUE
      AND o.opportunity_type = 'Expansion'
      AND o.opportunity_owner = 'Gregory Nallie'
      AND o.opportunity_closed_won_date BETWEEN CURRENT_DATE - 120 AND CURRENT_DATE - 60
      AND o.expansion_subtype IN ('Card Expansion','Bill Pay Expansion','Travel Expansion','Treasury Expansion')
),
spend_windows AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 1  AND g.cw_date + 30 THEN tpv.card_tpv ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 1  AND g.cw_date + 30 THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 1  AND g.cw_date + 30 THEN tpv.travel_tpv ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day BETWEEN g.cw_date + 1  AND g.cw_date + 30 THEN tpv.treasury_available_balance ELSE NULL END)
        END AS spend_d1_d30,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 31 AND g.cw_date + 60 THEN tpv.card_tpv ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 31 AND g.cw_date + 60 THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 31 AND g.cw_date + 60 THEN tpv.travel_tpv ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day BETWEEN g.cw_date + 31 AND g.cw_date + 60 THEN tpv.treasury_available_balance ELSE NULL END)
        END AS spend_d31_d60,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 61 AND g.cw_date + 90 THEN tpv.card_tpv ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 61 AND g.cw_date + 90 THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day BETWEEN g.cw_date + 61 AND g.cw_date + 90 THEN tpv.travel_tpv ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day BETWEEN g.cw_date + 61 AND g.cw_date + 90 THEN tpv.treasury_available_balance ELSE NULL END)
        END AS spend_d61_d90,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.card_tpv ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.travel_tpv ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.treasury_available_balance ELSE NULL END)
        END AS current_l30d
    FROM greg_cw_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day BETWEEN g.cw_date + 1 AND CURRENT_DATE
    GROUP BY g.opportunity_id, g.expansion_subtype
),
open_followup AS (
    SELECT DISTINCT o.account_id, o.expansion_subtype
    FROM analytics.marts.dim_sfdc_opportunities o
    WHERE o.opportunity_is_closed = FALSE
      AND o.opportunity_type = 'Expansion'
      AND o.opportunity_owner = 'Gregory Nallie'
      AND o.opportunity_stage_name != 'S0: Holding'
      AND o.expansion_subtype IN ('Card Expansion','Bill Pay Expansion','Travel Expansion','Treasury Expansion')
)
SELECT
    g.account_name,
    g.account_id,
    g.opportunity_id,
    g.expansion_subtype,
    g.cw_date,
    g.days_since_cw,
    ROUND(COALESCE(g.baseline_at_close, 0))           AS baseline_at_close,
    ROUND(COALESCE(sw.spend_d1_d30, 0))                AS spend_d1_d30,
    ROUND(COALESCE(sw.spend_d31_d60, 0))               AS spend_d31_d60,
    ROUND(COALESCE(sw.spend_d61_d90, 0))               AS spend_d61_d90,
    ROUND(COALESCE(sw.current_l30d, 0))                AS current_l30d,
    CASE WHEN of2.account_id IS NOT NULL THEN 1 ELSE 0 END AS has_open_followup_opp
FROM greg_cw_opps g
LEFT JOIN spend_windows sw ON sw.opportunity_id = g.opportunity_id
LEFT JOIN open_followup of2
       ON of2.account_id = g.account_id
      AND of2.expansion_subtype = g.expansion_subtype
WHERE of2.account_id IS NULL
ORDER BY g.cw_date DESC
"""

# ── Recent CW opps per account+product (for dedup in post-meeting detection) ──
RECENT_CW_BY_PRODUCT_QUERY = """
SELECT DISTINCT
    opp.account_id,
    opp.expansion_subtype
FROM analytics.marts.dim_sfdc_opportunities opp
WHERE opp.opportunity_is_won = TRUE
  AND opp.opportunity_type = 'Expansion'
  AND opp.opportunity_owner = 'Gregory Nallie'
  AND opp.opportunity_close_date >= CURRENT_DATE - 90
  AND opp.expansion_subtype IN (
      'Card Expansion', 'Bill Pay Expansion',
      'Travel Expansion', 'Treasury Expansion'
  )
"""

# ── Group DM Context: recent closes + pipeline stats for Gary personality ────
GROUP_DM_CONTEXT_QUERY = """
WITH recent_cw AS (
    SELECT
        opp.opportunity_name,
        sa.account_name,
        opp.expansion_subtype,
        opp.opportunity_closed_won_date::date AS cw_date,
        opp.monthly_expansion_amount,
        CASE opp.expansion_subtype
            WHEN 'Card Expansion'     THEN COALESCE(opp.monthly_expansion_amount, 0) * 0.0095
            WHEN 'Bill Pay Expansion' THEN COALESCE(opp.monthly_expansion_amount, 0) * 0.0015
            WHEN 'Travel Expansion'   THEN COALESCE(opp.monthly_expansion_amount, 0) * 0.035
            WHEN 'Treasury Expansion' THEN COALESCE(opp.monthly_expansion_amount, 0) * 0.0005
            ELSE 0
        END AS est_cp
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    WHERE opp.opportunity_is_won = TRUE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_closed_won_date >= CURRENT_DATE - 14
    ORDER BY est_cp DESC
    LIMIT 5
),
pipeline_stats AS (
    SELECT
        COUNT(*) AS open_opps,
        SUM(CASE opp.expansion_subtype
            WHEN 'Card Expansion'     THEN COALESCE(opp.monthly_expansion_amount, 0) * 0.0095
            WHEN 'Bill Pay Expansion' THEN COALESCE(opp.monthly_expansion_amount, 0) * 0.0015
            WHEN 'Travel Expansion'   THEN COALESCE(opp.monthly_expansion_amount, 0) * 0.035
            WHEN 'Treasury Expansion' THEN COALESCE(opp.monthly_expansion_amount, 0) * 0.0005
            ELSE 0
        END) AS pipeline_cp
    FROM analytics.marts.dim_sfdc_opportunities opp
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name NOT IN ('S0: Holding', 'Closed Lost')
),
signal_counts AS (
    SELECT COUNT(DISTINCT account_id) AS accel_accounts
    FROM (
        SELECT DISTINCT account_id
        FROM analytics.agg.agg_payments__business_day pd
        JOIN analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
            ON ledger.account_id = pd.account_id AND ledger.date_day = CURRENT_DATE - 1
        JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = pd.account_id
        WHERE ledger.owner_name = 'Gregory Nallie'
          AND pd.date_day = CURRENT_DATE - 1
          AND pd.card_tpv_l7d * (30.0/7) > pd.card_tpv_l90d / 3 * 1.5
          AND pd.card_tpv_l7d * (30.0/7) > 5000
    )
)
SELECT 'recent_cw' AS section, account_name, expansion_subtype, cw_date::varchar AS detail_1,
       ROUND(est_cp)::varchar AS detail_2, monthly_expansion_amount::varchar AS detail_3
FROM recent_cw
UNION ALL
SELECT 'pipeline' AS section, open_opps::varchar, ROUND(pipeline_cp)::varchar, NULL, NULL, NULL
FROM pipeline_stats
UNION ALL
SELECT 'signals' AS section, accel_accounts::varchar, NULL, NULL, NULL, NULL
FROM signal_counts
"""

# ── Home Page Priority Alerts (combined signals, max 10 rows) ────────────────
POST_CLOSE_CHECKPOINT_QUERY = """
WITH greg_cw_opps AS (
    SELECT
        opp.opportunity_id,
        opp.account_id,
        opp.opportunity_name,
        opp.expansion_subtype,
        opp.opportunity_closed_won_date::date AS cw_date,
        DATEDIFF('day', opp.opportunity_closed_won_date, CURRENT_DATE) AS days_since_cw,
        sa.account_name,
        sa.business_id,
        CASE opp.expansion_subtype
            WHEN 'Card Expansion'     THEN es.expansion_opportunity_30_day_transaction_amount_tpv_before_closed_won_date_prior
            WHEN 'Bill Pay Expansion' THEN es.expansion_opportunity_30_day_bill_pay_non_card_tpv_revops_amount_before_closed_won_date_prior
            WHEN 'Travel Expansion'   THEN es.expansion_opportunity_30_day_travel_amount_before_closed_won_date_prior
            WHEN 'Treasury Expansion' THEN es.expansion_opportunity_30_day_avg_treasury_available_balance_before_closed_won_date_prior
        END AS baseline_at_close
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    JOIN analytics.marts.agg_sfdc_expansion_opportunity_spend es ON es.opportunity_id = opp.opportunity_id
    WHERE opp.opportunity_is_closed = TRUE
      AND opp.opportunity_is_won = TRUE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.expansion_subtype IN (
          'Card Expansion', 'Bill Pay Expansion', 'Travel Expansion', 'Treasury Expansion')
      AND DATEDIFF('day', opp.opportunity_closed_won_date, CURRENT_DATE) BETWEEN 25 AND 65
),
current_spend AS (
    SELECT
        g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS spend_l30d
    FROM greg_cw_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= CURRENT_DATE - 30
    GROUP BY g.opportunity_id, g.expansion_subtype
)
SELECT
    g.account_name,
    g.account_id,
    g.opportunity_id,
    g.opportunity_name,
    g.expansion_subtype AS product,
    g.days_since_cw,
    ROUND(COALESCE(g.baseline_at_close, 0)) AS baseline_at_close,
    ROUND(COALESCE(c.spend_l30d, 0)) AS current_l30d,
    CASE
        WHEN g.days_since_cw BETWEEN 25 AND 35 THEN 'underperforming_d30'
        WHEN g.days_since_cw BETWEEN 55 AND 65 THEN 'underperforming_d60'
    END AS signal_type,
    CASE
        WHEN g.days_since_cw BETWEEN 25 AND 35 THEN ROUND(COALESCE(g.baseline_at_close, 0) * 1.3)
        WHEN g.days_since_cw BETWEEN 55 AND 65 THEN ROUND(COALESCE(g.baseline_at_close, 0) * 1.6)
    END AS target_spend,
    ROUND(CASE
        WHEN COALESCE(g.baseline_at_close, 0) > 0
        THEN 100.0 * COALESCE(c.spend_l30d, 0) / g.baseline_at_close
        ELSE 0
    END) AS pct_of_baseline
FROM greg_cw_opps g
LEFT JOIN current_spend c ON c.opportunity_id = g.opportunity_id
WHERE (
    -- D30 checkpoint: 25-35 days post-CW, spend < 80% of target
    (g.days_since_cw BETWEEN 25 AND 35
     AND COALESCE(c.spend_l30d, 0) < COALESCE(g.baseline_at_close, 0) * 1.3 * 0.8)
    OR
    -- D60 checkpoint: 55-65 days post-CW, spend < 80% of target
    (g.days_since_cw BETWEEN 55 AND 65
     AND COALESCE(c.spend_l30d, 0) < COALESCE(g.baseline_at_close, 0) * 1.6 * 0.8)
)
ORDER BY
    CASE WHEN g.days_since_cw BETWEEN 55 AND 65 THEN 1 ELSE 2 END,
    COALESCE(c.spend_l30d, 0) / NULLIF(g.baseline_at_close, 0) ASC
"""

HOME_PRIORITY_ALERTS_QUERY = """
WITH greg_accounts AS (
    SELECT DISTINCT ledger.account_id, ledger.business_id
    FROM analytics.agg.agg_sfdc__daily_account_owner_ledger ledger
    WHERE ledger.date_day = CURRENT_DATE - 1
      AND ledger.owner_name = 'Gregory Nallie'
),
open_opps AS (
    SELECT opp.opportunity_id, opp.account_id, opp.opportunity_name,
           opp.expansion_subtype, opp.opportunity_stage_name,
           opp.opportunity_created_at::date AS created_date,
           DATEDIFF('day', opp.opportunity_created_at::date, CURRENT_DATE) AS days_open,
           sa.account_name, sa.business_id
    FROM analytics.marts.dim_sfdc_opportunities opp
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.account_id = opp.account_id
    WHERE opp.opportunity_is_closed = FALSE
      AND opp.opportunity_type = 'Expansion'
      AND opp.opportunity_owner = 'Gregory Nallie'
      AND opp.opportunity_stage_name != 'S0: Holding'
      AND opp.expansion_subtype IN (
          'Card Expansion','Bill Pay Expansion','Travel Expansion','Treasury Expansion')
),
baseline_per_opp AS (
    SELECT g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS baseline_val
    FROM open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day BETWEEN
            IFF(g.days_open<=45, DATEADD('day',-30,g.created_date), CURRENT_DATE-60)
            AND IFF(g.days_open<=45, DATEADD('day',-1,g.created_date), CURRENT_DATE-31)
    GROUP BY g.opportunity_id, g.expansion_subtype
),
recent_per_opp AS (
    SELECT g.opportunity_id,
        CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(tpv.card_tpv)
            WHEN 'Bill Pay Expansion' THEN SUM(tpv.billpay_tpv)
            WHEN 'Travel Expansion'   THEN SUM(tpv.travel_tpv)
            WHEN 'Treasury Expansion' THEN AVG(tpv.treasury_available_balance)
        END AS recent_val
    FROM open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id AND tpv.date_day >= CURRENT_DATE - 30
    GROUP BY g.opportunity_id, g.expansion_subtype
),
milestones AS (
    SELECT
        bob.sfdc_account_id AS account_id,
        bob.card_fifth_transaction_cleared_at::date AS card_activated_at,
        bob.first_bill_paid_at::date AS billpay_activated_at,
        bob.treasury_first_payment_at::date AS treasury_activated_at,
        bob.travel_fifth_booking_at::date AS travel_activated_at
    FROM analytics.marts.dim_book_of_business_accounts_view bob
    WHERE bob.sfdc_account_id IN (SELECT account_id FROM open_opps)
),
-- Signal 1: close_now — open opps where L30D spend exceeds baseline
close_now AS (
    SELECT
        'close_now' AS signal_type,
        oo.opportunity_name,
        oo.account_name,
        oo.expansion_subtype AS product,
        oo.opportunity_id,
        oo.account_id,
        ROUND(COALESCE(r.recent_val, 0) - COALESCE(b.baseline_val, 0)) AS l30d_spend_delta,
        NULL::date AS activation_date,
        NULL::number AS paced_amount,
        NULL::number AS baseline_amount,
        NULL::number AS spend_since_opp,
        NULL::number AS spend_l30d,
        NULL::number AS spend_l7d
    FROM open_opps oo
    LEFT JOIN baseline_per_opp b ON b.opportunity_id = oo.opportunity_id
    LEFT JOIN recent_per_opp r ON r.opportunity_id = oo.opportunity_id
    WHERE COALESCE(r.recent_val, 0) > COALESCE(b.baseline_val, 0) * 1.1
      AND COALESCE(r.recent_val, 0) - COALESCE(b.baseline_val, 0) > 3000
),
-- Spend since opp created, L30D, and L7D for zero-to-one signals
zero_to_one_spend AS (
    SELECT
        g.opportunity_id,
        ROUND(CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day >= g.created_date THEN tpv.card_tpv    ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day >= g.created_date THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day >= g.created_date THEN tpv.travel_tpv  ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day >= g.created_date THEN tpv.treasury_available_balance END)
        END) AS spend_since_opp,
        ROUND(CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.card_tpv    ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.travel_tpv  ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.treasury_available_balance END)
        END) AS spend_l30d,
        ROUND(CASE g.expansion_subtype
            WHEN 'Card Expansion'     THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 7 THEN tpv.card_tpv    ELSE 0 END)
            WHEN 'Bill Pay Expansion' THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 7 THEN tpv.billpay_tpv ELSE 0 END)
            WHEN 'Travel Expansion'   THEN SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 7 THEN tpv.travel_tpv  ELSE 0 END)
            WHEN 'Treasury Expansion' THEN AVG(CASE WHEN tpv.date_day >= CURRENT_DATE - 7 THEN tpv.treasury_available_balance END)
        END) AS spend_l7d
    FROM open_opps g
    JOIN analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        ON tpv.business_id = g.business_id
        AND tpv.date_day >= LEAST(g.created_date, CURRENT_DATE - 30)
    GROUP BY g.opportunity_id, g.expansion_subtype
),
-- Signal 2: zero_to_one — open opps where account activated product after opp created
zero_to_one AS (
    SELECT
        'zero_to_one' AS signal_type,
        oo.opportunity_name,
        oo.account_name,
        oo.expansion_subtype AS product,
        oo.opportunity_id,
        oo.account_id,
        NULL::number AS l30d_spend_delta,
        CASE oo.expansion_subtype
            WHEN 'Card Expansion'     THEN m.card_activated_at
            WHEN 'Bill Pay Expansion' THEN m.billpay_activated_at
            WHEN 'Travel Expansion'   THEN m.travel_activated_at
            WHEN 'Treasury Expansion' THEN m.treasury_activated_at
        END AS activation_date,
        NULL::number AS paced_amount,
        NULL::number AS baseline_amount,
        COALESCE(zs.spend_since_opp, 0) AS spend_since_opp,
        COALESCE(zs.spend_l30d, 0) AS spend_l30d,
        COALESCE(zs.spend_l7d, 0) AS spend_l7d
    FROM open_opps oo
    JOIN milestones m ON m.account_id = oo.account_id
    LEFT JOIN zero_to_one_spend zs ON zs.opportunity_id = oo.opportunity_id
    WHERE (
        (oo.expansion_subtype = 'Card Expansion'     AND m.card_activated_at     >= oo.created_date)
        OR (oo.expansion_subtype = 'Bill Pay Expansion' AND m.billpay_activated_at >= oo.created_date)
        OR (oo.expansion_subtype = 'Travel Expansion'   AND m.travel_activated_at  >= oo.created_date)
        OR (oo.expansion_subtype = 'Treasury Expansion' AND m.treasury_activated_at >= oo.created_date)
    )
    -- Exclude opps already surfaced as close_now
    AND oo.opportunity_id NOT IN (SELECT opportunity_id FROM close_now)
),
-- ── Payment snapshots for acceleration detection ──
-- Current snapshot: L3D/L7D/L30D/L90D per account + leading indicators
current_snapshot AS (
    SELECT
        ga.account_id,
        ga.business_id,
        COALESCE(p.amount_card_payment_l3d, 0) AS card_l3d,
        COALESCE(p.amount_card_payment_l7d, 0) AS card_l7d,
        COALESCE(p.amount_card_payment_l30d, 0) AS card_l30d,
        COALESCE(p.amount_card_payment_l90d, 0) AS card_l90d,
        COALESCE(p.amount_bill_payment_l3d, 0) AS bill_l3d,
        COALESCE(p.amount_bill_payment_l7d, 0) AS bill_l7d,
        COALESCE(p.amount_bill_payment_l30d, 0) AS bill_l30d,
        COALESCE(bp.rolling_90_day_paid_bill_amount, 0) AS bill_l90d,
        COALESCE(p.next_3d_scheduled_approved_ach_amount, 0) AS next_3d_scheduled,
        COALESCE(bp.created_bill_amount, 0) AS created_bill_today,
        COALESCE(bp.scheduled_bill_amount, 0) AS scheduled_bill_today,
        -- Travel (SUM — flow metric)
        COALESCE(tt.travel_l7d, 0) AS travel_l7d,
        COALESCE(tt.travel_l30d, 0) AS travel_l30d,
        COALESCE(tt.travel_l90d, 0) AS travel_l90d,
        -- Treasury (AVG — balance metric)
        COALESCE(tt.treasury_l7d, 0) AS treasury_l7d,
        COALESCE(tt.treasury_l30d, 0) AS treasury_l30d,
        COALESCE(tt.treasury_l90d, 0) AS treasury_l90d
    FROM analytics.agg.agg_payments__business_day p
    JOIN greg_accounts ga ON ga.business_id = p.business_id
    LEFT JOIN analytics.agg.agg_bill_pay__business_day bp
        ON bp.business_id = ga.business_id AND bp.date_day = p.date_day
    LEFT JOIN (
        SELECT
            tpv.business_id,
            SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 7  THEN tpv.travel_tpv ELSE 0 END) AS travel_l7d,
            SUM(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.travel_tpv ELSE 0 END) AS travel_l30d,
            SUM(tpv.travel_tpv) AS travel_l90d,
            AVG(CASE WHEN tpv.date_day >= CURRENT_DATE - 7  THEN tpv.treasury_available_balance END) AS treasury_l7d,
            AVG(CASE WHEN tpv.date_day >= CURRENT_DATE - 30 THEN tpv.treasury_available_balance END) AS treasury_l30d,
            AVG(tpv.treasury_available_balance) AS treasury_l90d
        FROM analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        WHERE tpv.date_day >= CURRENT_DATE - 90
          AND tpv.business_id IN (SELECT business_id FROM greg_accounts)
        GROUP BY tpv.business_id
    ) tt ON tt.business_id = ga.business_id
    WHERE p.date_day = CURRENT_DATE - 1
),
-- Same period last month: L7D from 31 days ago (monthly pattern baseline)
splm AS (
    SELECT
        ga.account_id,
        COALESCE(p.amount_card_payment_l7d, 0) AS card_l7d_splm,
        COALESCE(p.amount_bill_payment_l7d, 0) AS bill_l7d_splm,
        COALESCE(tt.travel_l7d_splm, 0) AS travel_l7d_splm,
        COALESCE(tt.treasury_l7d_splm, 0) AS treasury_l7d_splm
    FROM analytics.agg.agg_payments__business_day p
    JOIN greg_accounts ga ON ga.business_id = p.business_id
    LEFT JOIN (
        SELECT
            tpv.business_id,
            SUM(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 37 AND CURRENT_DATE - 31 THEN tpv.travel_tpv ELSE 0 END) AS travel_l7d_splm,
            AVG(CASE WHEN tpv.date_day BETWEEN CURRENT_DATE - 37 AND CURRENT_DATE - 31 THEN tpv.treasury_available_balance END) AS treasury_l7d_splm
        FROM analytics.metrics.fct_daily_business__multiproduct_tpv tpv
        WHERE tpv.date_day BETWEEN CURRENT_DATE - 37 AND CURRENT_DATE - 31
          AND tpv.business_id IN (SELECT business_id FROM greg_accounts)
        GROUP BY tpv.business_id
    ) tt ON tt.business_id = ga.business_id
    WHERE p.date_day = CURRENT_DATE - 31
),
-- Per-product acceleration metrics
accel_metrics AS (
    SELECT
        cs.account_id,
        cs.business_id,
        -- Card metrics
        ROUND(cs.card_l7d * 30.0 / 7) AS card_l7d_pacing,
        ROUND(cs.card_l90d / 3.0) AS card_baseline,
        cs.card_l30d AS card_l30d_current,
        cs.card_l7d,
        COALESCE(sp.card_l7d_splm, 0) AS card_l7d_splm,
        -- Bill Pay metrics
        ROUND(cs.bill_l7d * 30.0 / 7) AS bill_l7d_pacing,
        ROUND(cs.bill_l90d / 3.0) AS bill_baseline,
        cs.bill_l30d AS bill_l30d_current,
        cs.bill_l7d,
        COALESCE(sp.bill_l7d_splm, 0) AS bill_l7d_splm,
        -- Travel metrics
        ROUND(cs.travel_l7d * 30.0 / 7) AS travel_l7d_pacing,
        ROUND(cs.travel_l90d / 3.0) AS travel_baseline,
        cs.travel_l30d AS travel_l30d_current,
        cs.travel_l7d,
        COALESCE(sp.travel_l7d_splm, 0) AS travel_l7d_splm,
        -- Treasury metrics
        ROUND(cs.treasury_l7d * 30.0 / 7) AS treasury_l7d_pacing,
        ROUND(cs.treasury_l90d / 3.0) AS treasury_baseline,
        cs.treasury_l30d AS treasury_l30d_current,
        cs.treasury_l7d,
        COALESCE(sp.treasury_l7d_splm, 0) AS treasury_l7d_splm,
        -- Leading indicators
        cs.next_3d_scheduled,
        cs.created_bill_today,
        cs.scheduled_bill_today,
        -- Pick the product with biggest L7D anomaly (delta over baseline)
        CASE
            WHEN GREATEST(
                cs.card_l7d * 30.0/7 - COALESCE(cs.card_l90d / 3.0, 0),
                cs.bill_l7d * 30.0/7 - COALESCE(cs.bill_l90d / 3.0, 0),
                cs.travel_l7d * 30.0/7 - COALESCE(cs.travel_l90d / 3.0, 0),
                cs.treasury_l7d * 30.0/7 - COALESCE(cs.treasury_l90d / 3.0, 0)
            ) = cs.card_l7d * 30.0/7 - COALESCE(cs.card_l90d / 3.0, 0)
                THEN 'Card'
            WHEN GREATEST(
                cs.card_l7d * 30.0/7 - COALESCE(cs.card_l90d / 3.0, 0),
                cs.bill_l7d * 30.0/7 - COALESCE(cs.bill_l90d / 3.0, 0),
                cs.travel_l7d * 30.0/7 - COALESCE(cs.travel_l90d / 3.0, 0),
                cs.treasury_l7d * 30.0/7 - COALESCE(cs.treasury_l90d / 3.0, 0)
            ) = cs.bill_l7d * 30.0/7 - COALESCE(cs.bill_l90d / 3.0, 0)
                THEN 'Bill Pay'
            WHEN GREATEST(
                cs.card_l7d * 30.0/7 - COALESCE(cs.card_l90d / 3.0, 0),
                cs.bill_l7d * 30.0/7 - COALESCE(cs.bill_l90d / 3.0, 0),
                cs.travel_l7d * 30.0/7 - COALESCE(cs.travel_l90d / 3.0, 0),
                cs.treasury_l7d * 30.0/7 - COALESCE(cs.treasury_l90d / 3.0, 0)
            ) = cs.travel_l7d * 30.0/7 - COALESCE(cs.travel_l90d / 3.0, 0)
                THEN 'Travel'
            ELSE 'Treasury'
        END AS best_product
    FROM current_snapshot cs
    LEFT JOIN splm sp ON sp.account_id = cs.account_id
),
-- Accounts with NO open opp that show acceleration
no_opp_accounts AS (
    SELECT am.*
    FROM accel_metrics am
    WHERE am.account_id NOT IN (
        SELECT DISTINCT account_id FROM analytics.marts.dim_sfdc_opportunities
        WHERE opportunity_is_closed = FALSE AND opportunity_type = 'Expansion'
          AND opportunity_owner = 'Gregory Nallie'
          AND opportunity_stage_name != 'S0: Holding'
    )
),
-- Signal 3a: early_accel — L7D ramping, L30D still low (window open)
early_accel AS (
    SELECT
        'early_accel' AS signal_type,
        NULL AS opportunity_name,
        sa.account_name,
        noa.best_product || ' Expansion' AS product,
        NULL AS opportunity_id,
        sa.account_id,
        NULL::number AS l30d_spend_delta,
        NULL::date AS activation_date,
        CASE noa.best_product
            WHEN 'Card'     THEN noa.card_l7d_pacing
            WHEN 'Bill Pay' THEN noa.bill_l7d_pacing
            WHEN 'Travel'   THEN noa.travel_l7d_pacing
            WHEN 'Treasury' THEN noa.treasury_l7d_pacing
        END AS paced_amount,
        CASE noa.best_product
            WHEN 'Card'     THEN noa.card_baseline
            WHEN 'Bill Pay' THEN noa.bill_baseline
            WHEN 'Travel'   THEN noa.travel_baseline
            WHEN 'Treasury' THEN noa.treasury_baseline
        END AS baseline_amount,
        NULL::number AS spend_since_opp,
        CASE noa.best_product
            WHEN 'Card'     THEN ROUND(noa.card_l30d_current)
            WHEN 'Bill Pay' THEN ROUND(noa.bill_l30d_current)
            WHEN 'Travel'   THEN ROUND(noa.travel_l30d_current)
            WHEN 'Treasury' THEN ROUND(noa.treasury_l30d_current)
        END AS spend_l30d,
        CASE noa.best_product
            WHEN 'Card'     THEN ROUND(noa.card_l7d)
            WHEN 'Bill Pay' THEN ROUND(noa.bill_l7d)
            WHEN 'Travel'   THEN ROUND(noa.travel_l7d)
            WHEN 'Treasury' THEN ROUND(noa.treasury_l7d)
        END AS spend_l7d
    FROM no_opp_accounts noa
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.business_id = noa.business_id
    WHERE (
        -- Card early accel
        (noa.best_product = 'Card'
            AND noa.card_l7d_pacing > 0
            AND noa.card_baseline >= 0
            AND noa.card_l7d_pacing > GREATEST(noa.card_baseline, 1) * 1.5
            AND noa.card_l7d_pacing - noa.card_baseline > 5000
            AND (noa.card_l7d > noa.card_l7d_splm * 1.5 OR noa.card_l7d_splm = 0)
            AND noa.card_l30d_current < noa.card_l7d_pacing * 0.7)
        OR
        -- Bill Pay early accel
        (noa.best_product = 'Bill Pay'
            AND noa.bill_l7d_pacing > 0
            AND noa.bill_baseline >= 0
            AND noa.bill_l7d_pacing > GREATEST(noa.bill_baseline, 1) * 1.5
            AND noa.bill_l7d_pacing - noa.bill_baseline > 5000
            AND (noa.bill_l7d > noa.bill_l7d_splm * 1.5 OR noa.bill_l7d_splm = 0)
            AND noa.bill_l30d_current < noa.bill_l7d_pacing * 0.7)
        OR
        -- Travel early accel
        (noa.best_product = 'Travel'
            AND noa.travel_l7d_pacing > 0
            AND noa.travel_baseline >= 0
            AND noa.travel_l7d_pacing > GREATEST(noa.travel_baseline, 1) * 1.5
            AND noa.travel_l7d_pacing - noa.travel_baseline > 2000
            AND (noa.travel_l7d > noa.travel_l7d_splm * 1.5 OR noa.travel_l7d_splm = 0)
            AND noa.travel_l30d_current < noa.travel_l7d_pacing * 0.7)
        OR
        -- Treasury early accel
        (noa.best_product = 'Treasury'
            AND noa.treasury_l7d_pacing > 0
            AND noa.treasury_baseline >= 0
            AND noa.treasury_l7d_pacing > GREATEST(noa.treasury_baseline, 1) * 1.5
            AND noa.treasury_l7d_pacing - noa.treasury_baseline > 50000
            AND (noa.treasury_l7d > noa.treasury_l7d_splm * 1.5 OR noa.treasury_l7d_splm = 0)
            AND noa.treasury_l30d_current < noa.treasury_l7d_pacing * 0.7)
    )
),
-- Signal 3b: sustained_accel — L30D already elevated (window closing/closed)
sustained_accel AS (
    SELECT
        'sustained_accel' AS signal_type,
        NULL AS opportunity_name,
        sa.account_name,
        noa.best_product || ' Expansion' AS product,
        NULL AS opportunity_id,
        sa.account_id,
        NULL::number AS l30d_spend_delta,
        NULL::date AS activation_date,
        CASE noa.best_product
            WHEN 'Card'     THEN noa.card_l7d_pacing
            WHEN 'Bill Pay' THEN noa.bill_l7d_pacing
            WHEN 'Travel'   THEN noa.travel_l7d_pacing
            WHEN 'Treasury' THEN noa.treasury_l7d_pacing
        END AS paced_amount,
        CASE noa.best_product
            WHEN 'Card'     THEN noa.card_baseline
            WHEN 'Bill Pay' THEN noa.bill_baseline
            WHEN 'Travel'   THEN noa.travel_baseline
            WHEN 'Treasury' THEN noa.treasury_baseline
        END AS baseline_amount,
        NULL::number AS spend_since_opp,
        CASE noa.best_product
            WHEN 'Card'     THEN ROUND(noa.card_l30d_current)
            WHEN 'Bill Pay' THEN ROUND(noa.bill_l30d_current)
            WHEN 'Travel'   THEN ROUND(noa.travel_l30d_current)
            WHEN 'Treasury' THEN ROUND(noa.treasury_l30d_current)
        END AS spend_l30d,
        CASE noa.best_product
            WHEN 'Card'     THEN ROUND(noa.card_l7d)
            WHEN 'Bill Pay' THEN ROUND(noa.bill_l7d)
            WHEN 'Travel'   THEN ROUND(noa.travel_l7d)
            WHEN 'Treasury' THEN ROUND(noa.treasury_l7d)
        END AS spend_l7d
    FROM no_opp_accounts noa
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.business_id = noa.business_id
    WHERE (
        (noa.best_product = 'Card'
            AND noa.card_l7d_pacing > 0
            AND noa.card_baseline >= 0
            AND noa.card_l7d_pacing > GREATEST(noa.card_baseline, 1) * 1.5
            AND noa.card_l7d_pacing - noa.card_baseline > 5000
            AND (noa.card_l7d > noa.card_l7d_splm * 1.5 OR noa.card_l7d_splm = 0)
            AND noa.card_l30d_current >= noa.card_l7d_pacing * 0.7)
        OR
        (noa.best_product = 'Bill Pay'
            AND noa.bill_l7d_pacing > 0
            AND noa.bill_baseline >= 0
            AND noa.bill_l7d_pacing > GREATEST(noa.bill_baseline, 1) * 1.5
            AND noa.bill_l7d_pacing - noa.bill_baseline > 5000
            AND (noa.bill_l7d > noa.bill_l7d_splm * 1.5 OR noa.bill_l7d_splm = 0)
            AND noa.bill_l30d_current >= noa.bill_l7d_pacing * 0.7)
        OR
        (noa.best_product = 'Travel'
            AND noa.travel_l7d_pacing > 0
            AND noa.travel_baseline >= 0
            AND noa.travel_l7d_pacing > GREATEST(noa.travel_baseline, 1) * 1.5
            AND noa.travel_l7d_pacing - noa.travel_baseline > 2000
            AND (noa.travel_l7d > noa.travel_l7d_splm * 1.5 OR noa.travel_l7d_splm = 0)
            AND noa.travel_l30d_current >= noa.travel_l7d_pacing * 0.7)
        OR
        (noa.best_product = 'Treasury'
            AND noa.treasury_l7d_pacing > 0
            AND noa.treasury_baseline >= 0
            AND noa.treasury_l7d_pacing > GREATEST(noa.treasury_baseline, 1) * 1.5
            AND noa.treasury_l7d_pacing - noa.treasury_baseline > 50000
            AND (noa.treasury_l7d > noa.treasury_l7d_splm * 1.5 OR noa.treasury_l7d_splm = 0)
            AND noa.treasury_l30d_current >= noa.treasury_l7d_pacing * 0.7)
    )
    -- Don't double-count accounts already in early_accel
    AND NOT EXISTS (SELECT 1 FROM early_accel ea WHERE ea.account_id = sa.account_id)
),
-- Signal 4: close_window — open opps where L7D shows ramp, close now for low baseline
close_window AS (
    SELECT
        'close_window' AS signal_type,
        oo.opportunity_name,
        oo.account_name,
        oo.expansion_subtype AS product,
        oo.opportunity_id,
        oo.account_id,
        NULL::number AS l30d_spend_delta,
        NULL::date AS activation_date,
        ROUND(CASE oo.expansion_subtype
            WHEN 'Card Expansion'     THEN am.card_l7d_pacing
            WHEN 'Bill Pay Expansion' THEN am.bill_l7d_pacing
            WHEN 'Travel Expansion'   THEN am.travel_l7d_pacing
            WHEN 'Treasury Expansion' THEN am.treasury_l7d_pacing
        END) AS paced_amount,
        ROUND(CASE oo.expansion_subtype
            WHEN 'Card Expansion'     THEN am.card_baseline
            WHEN 'Bill Pay Expansion' THEN am.bill_baseline
            WHEN 'Travel Expansion'   THEN am.travel_baseline
            WHEN 'Treasury Expansion' THEN am.treasury_baseline
        END) AS baseline_amount,
        NULL::number AS spend_since_opp,
        ROUND(CASE oo.expansion_subtype
            WHEN 'Card Expansion'     THEN am.card_l30d_current
            WHEN 'Bill Pay Expansion' THEN am.bill_l30d_current
            WHEN 'Travel Expansion'   THEN am.travel_l30d_current
            WHEN 'Treasury Expansion' THEN am.treasury_l30d_current
        END) AS spend_l30d,
        ROUND(CASE oo.expansion_subtype
            WHEN 'Card Expansion'     THEN am.card_l7d
            WHEN 'Bill Pay Expansion' THEN am.bill_l7d
            WHEN 'Travel Expansion'   THEN am.travel_l7d
            WHEN 'Treasury Expansion' THEN am.treasury_l7d
        END) AS spend_l7d
    FROM open_opps oo
    JOIN accel_metrics am ON am.account_id = oo.account_id
    LEFT JOIN splm sp ON sp.account_id = oo.account_id
    WHERE oo.opportunity_id NOT IN (SELECT opportunity_id FROM close_now)
      AND (
        (oo.expansion_subtype = 'Card Expansion'
            AND am.card_l7d_pacing > am.card_l30d_current * 1.3
            AND am.card_l7d_pacing - am.card_l30d_current > 5000
            AND (am.card_l7d > COALESCE(sp.card_l7d_splm, 0) * 1.5 OR COALESCE(sp.card_l7d_splm, 0) = 0))
        OR
        (oo.expansion_subtype = 'Bill Pay Expansion'
            AND am.bill_l7d_pacing > am.bill_l30d_current * 1.3
            AND am.bill_l7d_pacing - am.bill_l30d_current > 5000
            AND (am.bill_l7d > COALESCE(sp.bill_l7d_splm, 0) * 1.5 OR COALESCE(sp.bill_l7d_splm, 0) = 0))
        OR
        (oo.expansion_subtype = 'Travel Expansion'
            AND am.travel_l7d_pacing > am.travel_l30d_current * 1.3
            AND am.travel_l7d_pacing - am.travel_l30d_current > 2000
            AND (am.travel_l7d > COALESCE(sp.travel_l7d_splm, 0) * 1.5 OR COALESCE(sp.travel_l7d_splm, 0) = 0))
        OR
        (oo.expansion_subtype = 'Treasury Expansion'
            AND am.treasury_l7d_pacing > am.treasury_l30d_current * 1.3
            AND am.treasury_l7d_pacing - am.treasury_l30d_current > 50000
            AND (am.treasury_l7d > COALESCE(sp.treasury_l7d_splm, 0) * 1.5 OR COALESCE(sp.treasury_l7d_splm, 0) = 0))
      )
),
-- ── Leading indicator signal: large bills created/scheduled today ──
leading_indicator AS (
    SELECT
        'leading' AS signal_type,
        NULL AS opportunity_name,
        sa.account_name,
        'Bill Pay Expansion' AS product,
        NULL AS opportunity_id,
        sa.account_id,
        NULL::number AS l30d_spend_delta,
        NULL::date AS activation_date,
        -- paced_amount stores the created/scheduled amount
        ROUND(GREATEST(cs.created_bill_today, cs.scheduled_bill_today, cs.next_3d_scheduled)) AS paced_amount,
        ROUND(cs.bill_l90d / 3.0) AS baseline_amount,
        NULL::number AS spend_since_opp,
        ROUND(cs.bill_l30d) AS spend_l30d,
        ROUND(cs.bill_l7d) AS spend_l7d
    FROM current_snapshot cs
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.business_id = cs.business_id
    WHERE GREATEST(cs.created_bill_today, cs.scheduled_bill_today, cs.next_3d_scheduled) > 25000
      AND GREATEST(cs.created_bill_today, cs.scheduled_bill_today, cs.next_3d_scheduled) > cs.bill_l90d / 3.0
    -- Exclude accounts already flagged in other signals
    AND NOT EXISTS (SELECT 1 FROM early_accel ea WHERE ea.account_id = sa.account_id)
    AND NOT EXISTS (SELECT 1 FROM sustained_accel sac WHERE sac.account_id = sa.account_id)
    AND NOT EXISTS (SELECT 1 FROM close_now cn WHERE cn.account_id = sa.account_id)
),
-- ── Card leading indicator: submitted card payments (pre-clearing) ──
card_leading AS (
    SELECT
        'leading' AS signal_type,
        NULL AS opportunity_name,
        sa.account_name,
        'Card Expansion' AS product,
        NULL AS opportunity_id,
        sa.account_id,
        NULL::number AS l30d_spend_delta,
        NULL::date AS activation_date,
        -- paced_amount = L3D annualized as proxy for incoming card volume
        ROUND(cs.card_l3d * 30.0 / 3) AS paced_amount,
        ROUND(cs.card_l90d / 3.0) AS baseline_amount,
        NULL::number AS spend_since_opp,
        ROUND(cs.card_l30d) AS spend_l30d,
        ROUND(cs.card_l7d) AS spend_l7d
    FROM current_snapshot cs
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.business_id = cs.business_id
    WHERE cs.card_l3d * 30.0 / 3 > 25000
      AND cs.card_l3d * 30.0 / 3 > cs.card_l90d / 3.0
      AND cs.card_l3d > cs.card_l7d * 0.6
    -- Exclude accounts already flagged
    AND NOT EXISTS (SELECT 1 FROM early_accel ea WHERE ea.account_id = sa.account_id)
    AND NOT EXISTS (SELECT 1 FROM sustained_accel sac WHERE sac.account_id = sa.account_id)
    AND NOT EXISTS (SELECT 1 FROM close_now cn WHERE cn.account_id = sa.account_id)
    AND NOT EXISTS (SELECT 1 FROM leading_indicator li WHERE li.account_id = sa.account_id)
),
-- ── First bill created signal: open bill pay opp + first-ever bill in Ramp ──
first_bill_signal AS (
    SELECT
        'first_bill' AS signal_type,
        oo.opportunity_name,
        oo.account_name,
        'Bill Pay Expansion' AS product,
        oo.opportunity_id,
        oo.account_id,
        NULL::number AS l30d_spend_delta,
        NULL::date AS activation_date,
        ROUND(COALESCE(bp.created_bill_amount, 0)) AS paced_amount,
        NULL::number AS baseline_amount,
        NULL::number AS spend_since_opp,
        ROUND(cs.bill_l30d) AS spend_l30d,
        ROUND(cs.bill_l7d) AS spend_l7d
    FROM open_opps oo
    JOIN current_snapshot cs ON cs.account_id = oo.account_id
    LEFT JOIN analytics.agg.agg_bill_pay__business_day bp
        ON bp.business_id = cs.business_id AND bp.date_day = CURRENT_DATE - 1
    WHERE oo.expansion_subtype = 'Bill Pay Expansion'
      AND COALESCE(bp.running_created_bill_count, 0) <= 3
      AND COALESCE(bp.created_bill_amount, 0) > 0
      -- Exclude if already in close_now or zero_to_one
      AND oo.opportunity_id NOT IN (SELECT opportunity_id FROM close_now WHERE opportunity_id IS NOT NULL)
      AND oo.opportunity_id NOT IN (SELECT opportunity_id FROM zero_to_one WHERE opportunity_id IS NOT NULL)
),
-- Signal 8: treasury_spike — GLA balance spiked (L7D avg > L30D avg * 2.0, delta > $100K)
treasury_spike AS (
    SELECT
        'treasury_spike' AS signal_type,
        NULL AS opportunity_name,
        sa.account_name,
        'Treasury Expansion' AS product,
        NULL AS opportunity_id,
        sa.account_id,
        NULL::number AS l30d_spend_delta,
        NULL::date AS activation_date,
        ROUND(am.treasury_l7d) AS paced_amount,
        ROUND(am.treasury_baseline) AS baseline_amount,
        NULL::number AS spend_since_opp,
        ROUND(am.treasury_l30d_current) AS spend_l30d,
        ROUND(am.treasury_l7d) AS spend_l7d
    FROM accel_metrics am
    JOIN analytics.marts.dim_sfdc_accounts sa ON sa.business_id = am.business_id
    WHERE am.treasury_l7d > am.treasury_l30d_current * 2.0
      AND am.treasury_l7d - am.treasury_l30d_current > 100000
      -- Exclude accounts already in other treasury signals
      AND NOT EXISTS (SELECT 1 FROM early_accel ea WHERE ea.account_id = sa.account_id AND ea.product = 'Treasury Expansion')
      AND NOT EXISTS (SELECT 1 FROM sustained_accel sac WHERE sac.account_id = sa.account_id AND sac.product = 'Treasury Expansion')
      AND NOT EXISTS (SELECT 1 FROM close_now cn WHERE cn.account_id = sa.account_id)
),
combined AS (
    SELECT * FROM close_now
    UNION ALL
    SELECT * FROM close_window
    UNION ALL
    SELECT * FROM zero_to_one
    UNION ALL
    SELECT * FROM early_accel
    UNION ALL
    SELECT * FROM leading_indicator
    UNION ALL
    SELECT * FROM card_leading
    UNION ALL
    SELECT * FROM first_bill_signal
    UNION ALL
    SELECT * FROM sustained_accel
    UNION ALL
    SELECT * FROM treasury_spike
),
ranked AS (
    SELECT
        *,
        ROUND(
            CASE product
                WHEN 'Card Expansion'     THEN CASE WHEN signal_type = 'zero_to_one' THEN COALESCE(spend_l30d, 0) ELSE COALESCE(l30d_spend_delta, paced_amount - baseline_amount, 0) END * 0.0095 * 3
                WHEN 'Bill Pay Expansion' THEN CASE WHEN signal_type = 'zero_to_one' THEN COALESCE(spend_l30d, 0) ELSE COALESCE(l30d_spend_delta, paced_amount - baseline_amount, 0) END * 0.0015 * 3
                WHEN 'Travel Expansion'   THEN CASE WHEN signal_type = 'zero_to_one' THEN COALESCE(spend_l30d, 0) ELSE COALESCE(l30d_spend_delta, paced_amount - baseline_amount, 0) END * 0.035  * 3
                WHEN 'Treasury Expansion' THEN CASE WHEN signal_type = 'zero_to_one' THEN COALESCE(spend_l30d, 0) ELSE COALESCE(l30d_spend_delta, paced_amount - baseline_amount, 0) END * 0.0005 * 3
                ELSE 0
            END
        ) AS est_cp,
        ROW_NUMBER() OVER (
            PARTITION BY signal_type
            ORDER BY ROUND(
                CASE product
                    WHEN 'Card Expansion'     THEN CASE WHEN signal_type = 'zero_to_one' THEN COALESCE(spend_l30d, 0) ELSE COALESCE(l30d_spend_delta, paced_amount - baseline_amount, 0) END * 0.0095 * 3
                    WHEN 'Bill Pay Expansion' THEN CASE WHEN signal_type = 'zero_to_one' THEN COALESCE(spend_l30d, 0) ELSE COALESCE(l30d_spend_delta, paced_amount - baseline_amount, 0) END * 0.0015 * 3
                    WHEN 'Travel Expansion'   THEN CASE WHEN signal_type = 'zero_to_one' THEN COALESCE(spend_l30d, 0) ELSE COALESCE(l30d_spend_delta, paced_amount - baseline_amount, 0) END * 0.035  * 3
                    WHEN 'Treasury Expansion' THEN CASE WHEN signal_type = 'zero_to_one' THEN COALESCE(spend_l30d, 0) ELSE COALESCE(l30d_spend_delta, paced_amount - baseline_amount, 0) END * 0.0005 * 3
                    ELSE 0
                END
            ) DESC
        ) AS rn
    FROM combined
)
SELECT
    signal_type, opportunity_name, account_name, product, opportunity_id, account_id,
    l30d_spend_delta, activation_date, paced_amount, baseline_amount,
    spend_since_opp, spend_l30d, spend_l7d, est_cp
FROM ranked
WHERE rn <= 30
ORDER BY
    CASE signal_type
        WHEN 'early_accel'      THEN 1
        WHEN 'close_window'     THEN 2
        WHEN 'leading'          THEN 3
        WHEN 'first_bill'       THEN 4
        WHEN 'close_now'        THEN 5
        WHEN 'zero_to_one'      THEN 6
        WHEN 'sustained_accel'  THEN 7
        WHEN 'treasury_spike'   THEN 8
    END,
    est_cp DESC
"""

# ── Auto-parameterize owner name in all queries ──────────────────────────────
# Replaces hardcoded 'Gregory Nallie' with the configured OWNER_NAME so
# teammates can clone the repo, set OWNER_NAME in .env, and get their own data.
import sys as _sys

def _parameterize_queries():
    module = _sys.modules[__name__]
    for name in list(vars(module)):
        if not name.endswith("_QUERY"):
            continue
        val = getattr(module, name)
        if isinstance(val, str) and "Gregory Nallie" in val:
            setattr(module, name, val.replace("Gregory Nallie", _OWNER_NAME))

_parameterize_queries()
del _parameterize_queries
