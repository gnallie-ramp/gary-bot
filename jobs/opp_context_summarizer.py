"""Opp Context Summarizer — AI-synthesized, grounded deal context for Pipeline.

Generates a crisp, sales-focused paragraph per account covering:
  - What products + dollar amounts are in play
  - Timing / urgency signals the customer actually stated
  - Blockers or concerns raised
  - Email thread status (they replied, no follow-up sent, etc.)
  - The concrete next step for Greg

HARD rule: Claude may only reference facts present in the source data payload.
No fabricated dates, quotes, subjects, or customer statements. Validated
post-facto — any summary that invents dates not in the payload is retried once
and then dropped in favor of a template fallback.

Cache: ~/.gary_bot_opp_context.json
Key: account_id
Value: {hash, summary, generated_at, model}
Invalidation: recompute when the source-data hash changes (new call, new email,
updated next_step, or updated expansion_notes).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.claude_client import call_claude
from core.snowflake_client import run_query
from queries.queries import ACCOUNT_EMAIL_HISTORY_QUERY, format_query

logger = logging.getLogger(__name__)

CACHE_FILE = os.path.expanduser("~/.gary_bot_opp_context.json")
SUMMARIZER_MODEL = "claude-sonnet-4-20250514"
MAX_CONCURRENT = 6  # parallel Claude calls during cold-start batch
SUMMARY_MAX_CHARS = 500
EMAIL_HISTORY_LIMIT = 5
CALL_SUMMARY_CAP = 3500  # chars of Gong transcript to feed into prompt

# Serialize cache file writes across threads
_cache_lock = threading.Lock()
_generation_in_flight = set()  # account_ids currently being generated

_SYSTEM_PROMPT = """You summarize sales opportunities for Greg Nallie, a Growth
Account Manager at Ramp. You produce a SHORT, deal-focused paragraph that an
AM would actually find useful on a daily re-engagement review.

Answer FIVE questions in one flowing paragraph, in this order, only using
facts present in the SOURCE DATA below:

1. WHAT — the open opps on this account: product, monthly amount (or ACV for
   renewals). Use the customer's explicit ask when the call transcript names
   a specific dollar figure or product.
2. TIMING — any timeline or urgency the customer stated on the call or in
   email ("wants to move by end of Q2", "CFO reviewing next week",
   "paused for ERP migration, ready to revisit now"). Do not infer — only
   repeat stated timing.
3. BLOCKERS — concerns, competing evaluations, legal/compliance review,
   decision-maker availability, etc. Only mention if explicitly raised.
4. EMAIL STATUS — if the most recent email is INBOUND (from the customer),
   flag "they replied {date} — your move". If the most recent email is
   OUTBOUND and >7 days old with no reply, flag "you sent {date}, no reply
   yet". Do not fabricate subjects or dates.
5. NEXT STEP — concrete action Greg should take. Prefer the SFDC next_step
   fields. Sharpen with transcript/email context if it gives a more specific
   version of the same thing.

HARD RULES — violations are failures:
- Do not invent any dates, quotes, email subjects, monetary amounts, customer
  statements, or people names. Only reference facts in the payload.
- If a section has no data (e.g., no recent call, no email history), skip
  that part of the paragraph entirely. Do not say "no blockers mentioned"
  — just move on.
- Never start with a generic opener like "This account is..." — start with
  the concrete deal state.
- Keep the paragraph under 500 characters. Plain prose, no markdown, no
  bullet points, no section labels.
- If the source data is too thin to produce anything useful, respond with
  exactly: "Limited context on file — {one concrete fact you DO have}."
"""


# ── Cache ────────────────────────────────────────────────────────────────────

def _load_cache() -> dict:
    try:
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_cache(cache: dict) -> None:
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f, indent=2)
    os.replace(tmp, CACHE_FILE)


def _compute_source_hash(payload: dict) -> str:
    """Stable hash over the fields that should trigger regeneration on change."""
    fingerprint = {
        "last_call_date": str(payload.get("last_call_date") or ""),
        "last_email_date": str(payload.get("last_email_date") or ""),
        "opps": [
            {
                "opp_id": o.get("opp_id"),
                "stage": o.get("stage"),
                "next_step": o.get("next_step"),
                "expansion_notes": o.get("expansion_notes"),
                "monthly_amount": o.get("monthly_amount"),
            }
            for o in (payload.get("opps") or [])
        ],
    }
    blob = json.dumps(fingerprint, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


# ── Data gathering ───────────────────────────────────────────────────────────

def _fetch_email_history(account_id: str, limit: int = EMAIL_HISTORY_LIMIT) -> list[dict]:
    """Pull the last N email threads for the account, sorted newest first."""
    try:
        q = format_query(ACCOUNT_EMAIL_HISTORY_QUERY, account_id=account_id, limit=limit)
        df = run_query(q)
        df.columns = [c.lower() for c in df.columns]
        out = []
        for _, row in df.iterrows():
            out.append({
                "date": str(row.get("email_date") or ""),
                "subject": str(row.get("subject") or "")[:120],
                "direction": str(row.get("direction") or "").strip(),
                "ramp_owner": str(row.get("ramp_side_owner") or ""),
                "ramp_role": str(row.get("ramp_side_role") or ""),
            })
        return out
    except Exception as e:
        logger.debug("Email history fetch failed for %s: %s", account_id, e)
        return []


def _build_prompt_payload(row: dict, email_history: list[dict]) -> str:
    """Format the account row + email history as the prompt payload."""
    import pandas as pd

    acct_name = row.get("account_name") or "Unknown"
    opps = row.get("opps") or []

    lines = [f"ACCOUNT: {acct_name}", ""]

    # Open opps block
    lines.append("OPEN OPPS ON THIS ACCOUNT:")
    for i, opp in enumerate(opps, 1):
        opp_bits = [f"  {i}. Type={opp.get('type') or '?'}"]
        if opp.get("product"):
            opp_bits.append(f"Product={opp['product']}")
        opp_bits.append(f"Stage={opp.get('stage') or '?'}")
        if opp.get("close_date"):
            opp_bits.append(f"Close={opp['close_date']}")
        if opp.get("monthly_amount") and float(opp["monthly_amount"]) > 0:
            opp_bits.append(f"MonthlyAmount=${int(float(opp['monthly_amount']))}")
        if opp.get("est_cp") and float(opp["est_cp"]) > 0:
            opp_bits.append(f"EstCP=${int(float(opp['est_cp']))}")
        lines.append(" · ".join(opp_bits))
        if opp.get("next_step"):
            lines.append(f"     NextStep(SFDC): {opp['next_step'][:200]}")
        if opp.get("expansion_notes"):
            lines.append(f"     ExpansionNotes(SFDC): {opp['expansion_notes'][:300]}")
    lines.append("")

    # Recent Gong call
    call_title = row.get("last_call_title")
    call_date = row.get("last_call_date")
    call_summary = row.get("last_call_summary") or ""
    if call_title and call_date and not pd.isna(call_date):
        lines.append("MOST RECENT CALL (from Gong):")
        lines.append(f"  Date: {call_date}")
        lines.append(f"  Title: {call_title}")
        transcript = call_summary.strip()
        if len(transcript) > CALL_SUMMARY_CAP:
            transcript = transcript[:CALL_SUMMARY_CAP] + "…[truncated]"
        lines.append(f"  SectionSummary: {transcript}")
        lines.append("")
    else:
        lines.append("MOST RECENT CALL: none on file in the last ~year")
        lines.append("")

    # Email history
    if email_history:
        lines.append("RECENT EMAIL THREADS (newest first, last 180 days):")
        for thread in email_history:
            lines.append(
                f"  {thread['date']} {thread['direction'].upper()} "
                f"subj=\"{thread['subject']}\" ramp_side={thread['ramp_owner'] or '?'}"
                + (f" ({thread['ramp_role']})" if thread["ramp_role"] else "")
            )
    else:
        lines.append("RECENT EMAIL THREADS: none on file in the last 180 days")

    return "\n".join(lines)


# ── Grounding validator ──────────────────────────────────────────────────────

_DATE_PATTERN = re.compile(r"\b(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}(?:/\d{2,4})?)\b")


def _extract_dates(text: str) -> set[str]:
    return set(m.group(1) for m in _DATE_PATTERN.finditer(text or ""))


def _validate_summary(summary: str, payload: str) -> tuple[bool, str]:
    """Check that every date mentioned in the summary also appears in the
    payload. Handles common format variations: "2026-03-04" vs "3/4" vs
    "3/4/26" vs "03/04".
    """
    summary_dates = _extract_dates(summary)
    payload_dates = _extract_dates(payload)

    # Build a set of normalized MMDD tokens from the payload covering all
    # reasonable interpretations.
    payload_mmdd = set()
    for d in payload_dates:
        raw = re.sub(r"[-/]", "", d)
        if len(raw) == 8:          # YYYYMMDD
            payload_mmdd.add(raw[4:])
        elif len(raw) == 4:        # MMDD
            payload_mmdd.add(raw)
        elif len(raw) == 6:        # YYMMDD or MMDDYY — assume YYMMDD
            payload_mmdd.add(raw[2:])

    def _summary_to_mmdd(d: str) -> list[str]:
        """Convert a summary date string to plausible 4-char MMDD representations."""
        if "-" in d:  # ISO
            parts = d.split("-")
            if len(parts) == 3 and len(parts[0]) == 4:
                return [parts[1].zfill(2) + parts[2].zfill(2)]
        if "/" in d:
            parts = d.split("/")
            if len(parts) >= 2:
                month, day = parts[0], parts[1]
                return [month.zfill(2) + day.zfill(2)]
        return []

    for d in summary_dates:
        mmdd_candidates = _summary_to_mmdd(d)
        if not mmdd_candidates:
            # Couldn't parse — be strict and reject
            return False, f"unparseable date {d!r} in summary"
        if not any(c in payload_mmdd for c in mmdd_candidates):
            return False, f"date {d!r} in summary but not in payload (mmdd={mmdd_candidates})"

    return True, "ok"


# ── Core API ─────────────────────────────────────────────────────────────────

def get_cached_summary(account_id: str) -> dict | None:
    """Return the cache entry for an account or None if missing/invalid."""
    cache = _load_cache()
    entry = cache.get(account_id)
    if not entry:
        return None
    return entry


def is_cache_fresh(account_id: str, payload: dict) -> bool:
    """Check whether the cached summary matches the current source hash."""
    entry = get_cached_summary(account_id)
    if not entry:
        return False
    return entry.get("hash") == _compute_source_hash(payload)


def generate_summary(account_id: str, row: dict) -> dict:
    """Generate a fresh summary for one account. Blocks the calling thread.

    Returns {hash, summary, generated_at, model, validated} dict. Caches result
    on success. Returns the dict even on validation failure — caller decides
    whether to show or fall back.
    """
    # Pull email history (this is the extra data fetch beyond the main query)
    email_history = _fetch_email_history(account_id)

    payload_text = _build_prompt_payload(row, email_history)
    source_hash = _compute_source_hash({
        "last_call_date": row.get("last_call_date"),
        "last_email_date": row.get("last_email_date"),
        "opps": row.get("opps") or [],
    })

    try:
        summary = call_claude(
            prompt=f"SOURCE DATA:\n{payload_text}\n\nWrite the summary paragraph now.",
            max_tokens=400,
            model=SUMMARIZER_MODEL,
            system=_SYSTEM_PROMPT,
        )
        summary = (summary or "").strip()

        # Enforce length cap
        if len(summary) > SUMMARY_MAX_CHARS:
            summary = summary[:SUMMARY_MAX_CHARS].rsplit(". ", 1)[0] + "."

        ok, reason = _validate_summary(summary, payload_text)
        entry = {
            "hash": source_hash,
            "summary": summary,
            "generated_at": int(time.time()),
            "model": SUMMARIZER_MODEL,
            "validated": ok,
        }
        if not ok:
            logger.warning("Summary validation failed for %s: %s. Summary=%r",
                           account_id, reason, summary[:200])
            entry["validation_reason"] = reason

        # Write to cache only if validated — invalid summaries should retry next render
        if ok:
            with _cache_lock:
                cache = _load_cache()
                cache[account_id] = entry
                _save_cache(cache)

        return entry
    except Exception as e:
        logger.warning("Summary generation failed for %s: %s", account_id, e)
        return {
            "hash": source_hash,
            "summary": "",
            "generated_at": int(time.time()),
            "model": SUMMARIZER_MODEL,
            "validated": False,
            "error": str(e),
        }


def get_or_generate(account_id: str, row: dict, force: bool = False) -> dict | None:
    """Return a cached summary (if fresh) or generate a new one synchronously."""
    if not force:
        payload = {
            "last_call_date": row.get("last_call_date"),
            "last_email_date": row.get("last_email_date"),
            "opps": row.get("opps") or [],
        }
        if is_cache_fresh(account_id, payload):
            return get_cached_summary(account_id)
    return generate_summary(account_id, row)


def batch_generate(rows: list[dict], max_workers: int = MAX_CONCURRENT) -> dict:
    """Generate summaries for a list of account rows in parallel threads.

    Skips accounts whose cache is already fresh. Returns a dict
    {account_id: entry}.
    """
    out = {}
    to_do = []
    for row in rows:
        aid = row.get("account_id")
        if not aid:
            continue
        payload = {
            "last_call_date": row.get("last_call_date"),
            "last_email_date": row.get("last_email_date"),
            "opps": row.get("opps") or [],
        }
        if is_cache_fresh(aid, payload):
            cached = get_cached_summary(aid)
            if cached:
                out[aid] = cached
            continue
        to_do.append(row)

    if not to_do:
        return out

    logger.info("Batch summarizer: generating %d summaries (max %d concurrent)",
                len(to_do), max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(generate_summary, r["account_id"], r): r["account_id"]
                   for r in to_do}
        for fut in as_completed(futures):
            aid = futures[fut]
            try:
                out[aid] = fut.result()
            except Exception as e:
                logger.error("Batch summary failed for %s: %s", aid, e)
    return out


def invalidate(account_id: str) -> None:
    """Force-expire one account's cached summary."""
    with _cache_lock:
        cache = _load_cache()
        if account_id in cache:
            del cache[account_id]
            _save_cache(cache)
