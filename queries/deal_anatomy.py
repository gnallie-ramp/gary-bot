"""Deal anatomy source data queries.

Pulls everything needed to analyze WHY a CW expansion deal won:
  - Gong call transcripts (paragraph-level, last 90d pre-CW)
  - Email thread bodies (first + last body per thread, full history)
  - Opp + account metadata
  - Customer-side contact roles

All queries take parameters via str.format: `{account_id}`, `{opp_id}`,
`{cw_date}`. No __OWNER__ substitution needed — these are account/opp
scoped, not owner-scoped.
"""

# ── Gong transcripts: all paragraphs from calls tied to this account
# in the 90 days before (and any day after) CW ──────────────────────────────
DEAL_CALL_TRANSCRIPTS_QUERY = """
SELECT
    gc.gong_call_id,
    gc.gong_call_start::date AS call_date,
    gc.gong_call_duration_sec,
    tp.paragraph_index,
    tp.paragraph_text,
    tp.speaker_email,
    tp.speaker_type
FROM analytics.marts.dim_sfdc_gong_call gc
JOIN analytics.marts.dim_gong_transcript_paragraph tp
    ON tp.call_id = gc.gong_call_id
WHERE gc.sfdc_primary_account_id = '{account_id}'
  AND gc.gong_call_start >= '{cw_date}'::date - 120
  AND gc.gong_call_start <= '{cw_date}'::date + 30
ORDER BY gc.gong_call_start, tp.paragraph_index
"""

# ── Email thread bodies on this account's opps ──────────────────────────────
DEAL_EMAIL_THREADS_QUERY = """
SELECT
    thread_idx,
    first_sfdc_email_subject AS subject,
    first_email_direction AS first_direction,
    last_email_direction AS last_direction,
    first_email_body,
    last_email_body_clean,
    thread_owner_full_name,
    thread_owner_user_role,
    historical_thread_owner_role,
    opportunity_name_most_recent,
    opportunity_stage_name
FROM analytics.marts.dim_email_threads
WHERE sfdc_account_id = '{account_id}'
  AND (first_email_body IS NOT NULL OR last_email_body_clean IS NOT NULL)
ORDER BY thread_idx DESC
LIMIT 8
"""

# ── Customer contacts on this account w/ roles (who was on the email) ───────
DEAL_CONTACTS_QUERY = """
SELECT
    name AS contact_name,
    email,
    title,
    department
FROM analytics.marts.dim_sfdc_contacts
WHERE account_id = '{account_id}'
  AND email IS NOT NULL
  AND email NOT ILIKE '%ramp.com'
LIMIT 30
"""

# ── Opp metadata enrichment ─────────────────────────────────────────────────
DEAL_META_QUERY = """
SELECT
    opp.opportunity_id,
    opp.opportunity_name,
    opp.account_id,
    opp.expansion_subtype,
    opp.opportunity_closed_won_date::date AS cw_date,
    opp.opportunity_closed_won_amount_usd AS cw_amount,
    opp.normalized_opportunity_owner AS owner,
    opp.opportunity_stage_name,
    COALESCE(s.expansion_opportunity_30_day_transaction_amount_before_closed_won_date, 0) AS pre_card_30d,
    COALESCE(s.expansion_opportunity_max_30_day_transaction_amount_within_90_days_post_closed_won_date, 0) AS post_card_30d,
    COALESCE(s.expansion_opportunity_30_day_bill_pay_amount_before_closed_won_date, 0) AS pre_bp_30d,
    COALESCE(s.expansion_opportunity_max_30_day_bill_pay_amount_within_90_days_post_closed_won_date, 0) AS post_bp_30d
FROM analytics.marts.dim_sfdc_opportunities opp
LEFT JOIN analytics.marts.agg_sfdc_expansion_opportunity_spend s
    ON s.opportunity_id = opp.opportunity_id
WHERE opp.opportunity_id = '{opp_id}'
"""


# ── First-touch story: how did this conversation start? ─────────────────────
# Pulls the earliest engagement signals on the account (vs CW date) to answer:
# "Did the meeting come from an outbound email Greg sent? A customer reaching
# out? An app trial? Something else?"
DEAL_FIRST_TOUCH_QUERY = """
WITH earliest_email AS (
    SELECT
        first_email_created_at,
        first_email_direction,
        first_sfdc_email_subject AS first_email_subject,
        thread_owner_full_name AS first_email_owner,
        thread_owner_user_role AS first_email_owner_role
    FROM analytics.marts.dim_email_threads
    WHERE sfdc_account_id = '{account_id}'
      AND first_email_created_at IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (ORDER BY first_email_created_at ASC) = 1
),
first_reply AS (
    SELECT MIN(first_contact_reply_at) AS first_customer_reply_at
    FROM analytics.marts.dim_email_threads
    WHERE sfdc_account_id = '{account_id}'
),
first_call AS (
    SELECT MIN(gong_call_start)::date AS first_call_date
    FROM analytics.marts.dim_sfdc_gong_call
    WHERE sfdc_primary_account_id = '{account_id}'
)
SELECT
    (SELECT first_email_created_at::date FROM earliest_email) AS first_email_date,
    (SELECT first_email_direction        FROM earliest_email) AS first_email_direction,
    (SELECT first_email_subject          FROM earliest_email) AS first_email_subject,
    (SELECT first_email_owner            FROM earliest_email) AS first_email_owner,
    (SELECT first_email_owner_role       FROM earliest_email) AS first_email_owner_role,
    (SELECT first_customer_reply_at::date FROM first_reply)   AS first_customer_reply_date,
    (SELECT first_call_date              FROM first_call)     AS first_call_date
"""

