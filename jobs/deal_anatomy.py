"""Deal Anatomy analyzer — reads full Gong transcripts + email thread bodies
for a CW expansion deal and produces a structured JSON breakdown of WHY it won.

Output shape per deal (saved to ~/.gary_bot_deal_anatomy/{opp_id}.json):
{
    "opp_id": "006...",
    "opp_name": "...",
    "account_id": "001...",
    "product": "Card Expansion",
    "cw_date": "2026-01-09",
    "owner": "Gregory Nallie",
    "realized_cp": 9889,
    "analyzed_at": "2026-04-21T18:30:00Z",
    "winning_move": "Short paragraph naming what actually closed the deal.",
    "pain_points": [
        {"quote": "verbatim customer statement", "source": "call 2026-01-03", "theme": "manual coding"},
        ...
    ],
    "ramp_features_used": ["AI coding", "Advanced policies", ...],
    "messaging_patterns": {
        "first_email_that_got_reply": {"subject": "...", "opening": "first 1-2 sentences"},
        "repeated_phrases": ["verbatim phrase #1", "verbatim phrase #2"]
    },
    "champion": {"name": "Jane Smith", "role": "Controller", "why": "drove internal alignment"},
    "pitch_framing": "ROI-led / hours-saved / consolidation / yield",
    "play_tags": ["plus_upgrade", "card_consolidation"],
    "cycle_days": 42,
    "insights": "1-2 sentences of non-obvious learnings for other reps"
}

Claude Sonnet is the analyzer (balances cost + quality for long contexts).
Transcripts capped at ~20K chars to stay under token budget. Bodies capped
at 3K chars each.

Safe to re-run — if an opp's source hash hasn't changed, the cached
analysis is kept. Otherwise a fresh analysis runs and overwrites.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from core.claude_client import call_claude
from core.snowflake_client import run_query
from queries.deal_anatomy import (
    DEAL_CALL_TRANSCRIPTS_QUERY,
    DEAL_EMAIL_THREADS_QUERY,
    DEAL_CONTACTS_QUERY,
    DEAL_META_QUERY,
)
from queries.team_cp import TEAM_CP_LEADERBOARD_QUERY, TOP_DEALS_QUERY  # noqa: F401

logger = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".gary_bot_deal_anatomy"
CACHE_DIR.mkdir(exist_ok=True)

TRANSCRIPT_CAP_CHARS = 18000
EMAIL_BODY_CAP_CHARS = 2500
MIN_REALIZED_CP = 500  # Only analyze deals with meaningful CP (below this, not enough signal)

_SYSTEM_PROMPT = """You are a sales coach analyzing why a Ramp expansion deal won.
You'll be given call transcripts, email thread bodies, and opp metadata for a
single closed-won expansion deal. Your job is to extract WHAT WORKED so other
AMs can replicate it.

Return ONLY a valid JSON object with these keys (no prose before/after, no
markdown fences):

{
  "winning_move": "1-2 sentence summary of the specific thing that closed the deal. Name the customer pain + the Ramp capability that unlocked it.",
  "pain_points": [
    {"quote": "verbatim phrase from the customer", "theme": "short label like 'manual AP coding'", "source": "call YYYY-MM-DD or email"},
    ... 2-4 items, ONLY from the source data provided
  ],
  "ramp_features_used": ["specific feature names cited in the conversation: AI coding, Advanced policies, Multi-entity, 3-way match, Treasury yield, etc."],
  "messaging_patterns": {
    "first_email_that_got_reply": {"subject": "verbatim subject line", "opening": "first 1-2 sentences verbatim"},
    "repeated_phrases": ["verbatim 3-6 word phrases the AM used that seem to have landed, based on customer reactions in later messages"]
  },
  "champion": {"name": "customer-side person who drove it", "role": "their title", "why": "1 sentence on what they did"},
  "pitch_framing": "one of: 'ROI / hours saved', 'cashback / yield', 'consolidation', 'activation rescue', 'compliance / policy', 'multi-entity / scale'",
  "play_tags": ["array of plays this deal resembles — pick from: plus_upgrade, procurement_upgrade, bill_pay_migration, card_consolidation, treasury_attach, activation_rescue, multi_entity, erp_integration, international_scale"],
  "cycle_days": integer or null (days from first customer-side email to CW date),
  "insights": "1-2 sentences of non-obvious learnings another rep could apply to similar accounts"
}

HARD RULES:
- Every "quote" and verbatim phrase must appear LITERALLY in the provided source data. Do not paraphrase and call it a quote.
- If a section has no clear evidence in the data (e.g. no champion identifiable), return null or an empty array — never fabricate.
- Only reference Ramp features actually named or implied in the transcript. Don't invent features.
- Keep everything ruthlessly concise. This will be read by AMs in Slack."""


def _source_hash(transcripts: str, threads: list, meta: dict) -> str:
    """Stable hash of the source data so we can detect when to re-analyze."""
    blob = json.dumps({
        "t_len": len(transcripts),
        "threads": [(t.get("thread_idx"), t.get("subject")) for t in threads],
        "cw_date": str(meta.get("cw_date", "")),
        "product": meta.get("expansion_subtype", ""),
    }, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def _fetch_source(account_id: str, opp_id: str, cw_date) -> dict:
    """Run all 4 source-data queries. Returns assembled dict."""
    cw_date_str = str(cw_date)
    meta_df = run_query(DEAL_META_QUERY.format(opp_id=opp_id))
    if meta_df.empty:
        return {}
    meta = meta_df.iloc[0].to_dict()

    tx_df = run_query(DEAL_CALL_TRANSCRIPTS_QUERY.format(account_id=account_id, cw_date=cw_date_str))
    em_df = run_query(DEAL_EMAIL_THREADS_QUERY.format(account_id=account_id))
    ct_df = run_query(DEAL_CONTACTS_QUERY.format(account_id=account_id))

    # Assemble transcripts: group paragraphs by call, label speaker side
    transcripts_lines = []
    current_call = None
    for _, p in tx_df.iterrows():
        call_id = p.get("gong_call_id")
        if call_id != current_call:
            d = p.get("call_date") or "?"
            transcripts_lines.append(f"\n=== CALL {d} ===")
            current_call = call_id
        speaker_email = (p.get("speaker_email") or "").strip()
        speaker_type = (p.get("speaker_type") or "").strip()
        is_ramp = "@ramp.com" in speaker_email.lower() or speaker_type.upper() == "HOST"
        label = "AM" if is_ramp else "Customer"
        text = (p.get("paragraph_text") or "").strip()
        if text:
            transcripts_lines.append(f"{label}: {text}")
    transcripts = "\n".join(transcripts_lines)
    if len(transcripts) > TRANSCRIPT_CAP_CHARS:
        transcripts = transcripts[:TRANSCRIPT_CAP_CHARS] + "\n…[truncated]"

    threads = []
    for _, r in em_df.iterrows():
        fb = (r.get("first_email_body") or "")[:EMAIL_BODY_CAP_CHARS]
        lb = (r.get("last_email_body_clean") or "")[:EMAIL_BODY_CAP_CHARS]
        threads.append({
            "thread_idx": r.get("thread_idx"),
            "subject": r.get("subject"),
            "first_direction": r.get("first_direction"),
            "last_direction": r.get("last_direction"),
            "first_email_body": fb,
            "last_email_body": lb,
            "opp_stage": r.get("opportunity_stage_name"),
        })

    contacts = [{"name": r.get("contact_name"), "email": r.get("email"),
                 "title": r.get("title"), "department": r.get("department")}
                for _, r in ct_df.iterrows()]

    return {
        "meta": meta,
        "transcripts": transcripts,
        "transcript_para_count": len(tx_df),
        "threads": threads,
        "contacts": contacts,
    }


def _build_user_prompt(source: dict) -> str:
    meta = source["meta"]
    parts = [
        "=== DEAL METADATA ===",
        f"Opp: {meta.get('opportunity_name')}",
        f"Product: {meta.get('expansion_subtype')}",
        f"CW Date: {meta.get('cw_date')}",
        f"Owner: {meta.get('owner')}",
        f"Pre-CW card 30d: ${int(meta.get('pre_card_30d') or 0):,}",
        f"Post-CW card max 30d within 90d: ${int(meta.get('post_card_30d') or 0):,}",
        f"Pre-CW BP 30d: ${int(meta.get('pre_bp_30d') or 0):,}",
        f"Post-CW BP max 30d within 90d: ${int(meta.get('post_bp_30d') or 0):,}",
        "",
        "=== CUSTOMER-SIDE CONTACTS ===",
    ]
    for c in source.get("contacts", [])[:15]:
        parts.append(f"  - {c.get('name')} ({c.get('title') or '?'}) — {c.get('email')}")

    parts.append("\n=== EMAIL THREADS (most recent first) ===")
    for t in source.get("threads", []):
        parts.append(f"\n--- Thread: {t.get('subject')} [{t.get('first_direction')} → {t.get('last_direction')}, stage: {t.get('opp_stage')}] ---")
        if t.get("first_email_body"):
            parts.append(f"FIRST EMAIL:\n{t['first_email_body']}")
        if t.get("last_email_body"):
            parts.append(f"LAST EMAIL:\n{t['last_email_body']}")

    parts.append("\n=== GONG CALL TRANSCRIPTS (last 120d pre-CW, first 30d post-CW) ===")
    parts.append(source.get("transcripts") or "(no call transcripts)")

    return "\n".join(parts)


def analyze_one(opp_id: str, account_id: str, owner: str, product: str,
                cw_date, realized_cp: float, force: bool = False) -> dict:
    """Produce a deal-anatomy analysis for one opp. Writes JSON to cache.

    Returns the analysis dict, or {"error": ...} on failure.
    """
    if realized_cp < MIN_REALIZED_CP and not force:
        return {"skipped": "below_cp_threshold", "realized_cp": realized_cp}

    cache_path = CACHE_DIR / f"{opp_id}.json"
    source = _fetch_source(account_id, opp_id, cw_date)
    if not source:
        return {"error": "no_source_data"}

    src_hash = _source_hash(source.get("transcripts", ""),
                            source.get("threads", []),
                            source.get("meta", {}))

    # Skip if cached and source unchanged
    if cache_path.exists() and not force:
        try:
            cached = json.loads(cache_path.read_text())
            if cached.get("_source_hash") == src_hash:
                return cached
        except Exception:
            pass

    user_prompt = _build_user_prompt(source)
    try:
        raw = call_claude(user_prompt, max_tokens=2000, system=_SYSTEM_PROMPT)
    except Exception as e:
        logger.error("Claude call failed for opp %s: %s", opp_id, e)
        return {"error": f"claude_failed: {e}"}

    # Strip markdown fences Claude sometimes adds
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw
        raw = raw.rsplit("```", 1)[0].strip()
        if raw.startswith("json"):
            raw = raw[4:].strip()

    try:
        analysis = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error("JSON parse failed for opp %s: %s — raw=%s", opp_id, e, raw[:200])
        return {"error": "json_parse_failed", "raw": raw[:500]}

    # Augment with metadata before caching
    analysis["opp_id"] = opp_id
    analysis["account_id"] = account_id
    analysis["owner"] = owner
    analysis["product"] = product
    analysis["cw_date"] = str(cw_date)
    analysis["realized_cp"] = int(realized_cp)
    analysis["analyzed_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    analysis["_source_hash"] = src_hash
    analysis["_source_stats"] = {
        "transcript_paragraphs": source.get("transcript_para_count", 0),
        "email_threads": len(source.get("threads") or []),
        "contacts": len(source.get("contacts") or []),
    }

    cache_path.write_text(json.dumps(analysis, indent=2, default=str))
    return analysis


def get_cached(opp_id: str) -> Optional[dict]:
    cache_path = CACHE_DIR / f"{opp_id}.json"
    if not cache_path.exists():
        return None
    try:
        return json.loads(cache_path.read_text())
    except Exception:
        return None


def get_first_touch_story(account_id: str) -> Optional[dict]:
    """Return how the first conversation with this account started.

    Compares first email (any direction) to first Gong call and classifies:
      - 'outbound_email'     — Ramp emailed first, call followed
      - 'inbound_email'      — Customer emailed first, call followed
      - 'app_or_direct_book' — First call with no preceding email (likely
                                booked via ChiliPiper / self-serve / referral)
      - 'unknown'            — Nothing on file

    Returns dict like:
      {"kind": "outbound_email", "label": "Outbound email → reply → call",
       "first_email_date": date, "first_email_direction": "Outbound",
       "first_email_owner": "Gregory Nallie", "first_email_subject": "…",
       "first_customer_reply_date": date, "first_call_date": date,
       "days_email_to_call": 12}
    """
    from core.snowflake_client import run_query
    from queries.deal_anatomy import DEAL_FIRST_TOUCH_QUERY

    try:
        df = run_query(DEAL_FIRST_TOUCH_QUERY.format(account_id=account_id))
    except Exception as e:
        logger.debug("First touch query failed for %s: %s", account_id, e)
        return None
    if df.empty:
        return None
    r = df.iloc[0].to_dict()

    from datetime import date as _date
    def _as_date(v):
        if v is None or v == "" or (isinstance(v, float) and v != v):
            return None
        if isinstance(v, _date):
            return v
        try:
            return _date.fromisoformat(str(v)[:10])
        except (ValueError, TypeError):
            return None

    first_email_date = _as_date(r.get("first_email_date"))
    first_email_dir = (r.get("first_email_direction") or "").strip()
    first_call_date = _as_date(r.get("first_call_date"))
    first_reply_date = _as_date(r.get("first_customer_reply_date"))

    if first_email_date and first_call_date:
        days_diff = (first_call_date - first_email_date).days
        if first_email_dir.lower() == "outbound" and days_diff >= 0:
            kind = "outbound_email"
            label = f"Outbound email → call (booked {days_diff}d later)"
        elif first_email_dir.lower() == "inbound" and days_diff >= 0:
            kind = "inbound_email"
            label = f"Inbound email → call (booked {days_diff}d later)"
        elif days_diff < 0:
            kind = "app_or_direct_book"
            label = f"Call happened before any email ({abs(days_diff)}d earlier) — likely app-book or direct reach-out"
        else:
            kind = "email_same_day"
            label = "Email + call same day"
    elif first_call_date and not first_email_date:
        kind = "app_or_direct_book"
        label = "Call with no preceding email — likely ChiliPiper / self-serve / direct reach-out"
    elif first_email_date and not first_call_date:
        kind = f"{first_email_dir.lower()}_email_no_call"
        label = f"{first_email_dir.title()} email only — no call on file"
    else:
        kind = "unknown"
        label = "No email or call on file"

    days_email_to_call = None
    if first_email_date and first_call_date:
        days_email_to_call = (first_call_date - first_email_date).days

    return {
        "kind": kind,
        "label": label,
        "first_email_date": str(first_email_date) if first_email_date else None,
        "first_email_direction": first_email_dir or None,
        "first_email_subject": r.get("first_email_subject"),
        "first_email_owner": r.get("first_email_owner"),
        "first_email_owner_role": r.get("first_email_owner_role"),
        "first_customer_reply_date": str(first_reply_date) if first_reply_date else None,
        "first_call_date": str(first_call_date) if first_call_date else None,
        "days_email_to_call": days_email_to_call,
    }


def run_batch(lookback_days: int = 180, max_deals: int = 40) -> dict:
    """Scheduled batch: analyze all CW expansion deals from the last N days
    that have realized CP ≥ MIN_REALIZED_CP and haven't been analyzed yet.
    Caps at max_deals per run to bound compute.
    """
    from queries.team_cp import TOP_DEALS_QUERY

    # Pull the full set of CW deals w/ realized CP per owner across the team
    # (we re-use TOP_DEALS_QUERY with a wildcard + big top_n; cleaner would be
    # a dedicated query but this works until we need it).
    team_sql = f"""
    WITH cw_opps AS (
        SELECT
            opp.opportunity_id, opp.account_id, opp.opportunity_name,
            opp.expansion_subtype AS product,
            opp.opportunity_closed_won_date::date AS cw_date,
            opp.normalized_opportunity_owner AS owner
        FROM analytics.marts.dim_sfdc_opportunities opp
        WHERE opp.opportunity_is_won = TRUE
          AND opp.opportunity_closed_won_date >= CURRENT_DATE - {lookback_days}
          AND opp.expansion_subtype IN ('Card Expansion','Bill Pay Expansion','Travel Expansion','Treasury Expansion')
          AND opp.normalized_opportunity_owner_role_stamped = 'Sales - AM - Growth'
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
        c.opportunity_id, c.account_id, c.product, c.cw_date, c.owner,
        ROUND(CASE c.product
            WHEN 'Card Expansion'     THEN COALESCE(s.inc_card, 0) * 0.0095
            WHEN 'Bill Pay Expansion' THEN COALESCE(s.inc_bp, 0)   * 0.0015
            ELSE 0 END, 0) AS realized_cp
    FROM cw_opps c
    LEFT JOIN spend s ON s.opportunity_id = c.opportunity_id
    ORDER BY realized_cp DESC NULLS LAST
    """
    df = run_query(team_sql)
    if df.empty:
        return {"analyzed": 0, "skipped": 0, "errors": 0, "status": "no deals"}

    counts = {"analyzed": 0, "skipped": 0, "errors": 0, "cached": 0}
    for _, row in df.iterrows():
        if counts["analyzed"] >= max_deals:
            break
        opp_id = row["opportunity_id"]
        cp = float(row.get("realized_cp") or 0)
        if cp < MIN_REALIZED_CP:
            counts["skipped"] += 1
            continue
        # Skip already-cached
        if (CACHE_DIR / f"{opp_id}.json").exists():
            counts["cached"] += 1
            continue
        try:
            result = analyze_one(
                opp_id=opp_id,
                account_id=row["account_id"],
                owner=row["owner"],
                product=row["product"],
                cw_date=row["cw_date"],
                realized_cp=cp,
            )
            if "error" in result:
                counts["errors"] += 1
                logger.warning("analyze_one error on %s: %s", opp_id, result.get("error"))
            else:
                counts["analyzed"] += 1
                logger.info("analyzed deal %s (%s, CP $%s)", opp_id, row["product"], int(cp))
        except Exception as e:
            counts["errors"] += 1
            logger.error("analyze_one exception on %s: %s", opp_id, e)

    logger.info("Deal Anatomy batch complete: %s", counts)
    return counts
