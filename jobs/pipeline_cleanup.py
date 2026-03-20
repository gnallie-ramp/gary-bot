"""Daily pipeline cleanup analysis — 7:30 AM PT."""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd

from core.snowflake_client import run_query
from core.claude_client import call_claude
from core.slack_formatter import sf_opp_url, simple_dm_blocks, dashboard_url
from queries.queries import (
    CLEANUP_OPPS_QUERY,
    CLEANUP_GONG_QUERY,
    ACCOUNT_NOTES_QUERY,
    ACCOUNT_EMAILS_FULL_QUERY,
    GONG_FULL_TRANSCRIPT_QUERY,
)
from utils.account_resolver import fetch_contact_emails, is_hash_like
from config import GREG_SLACK_ID, OWNER_NAME

logger = logging.getLogger(__name__)

MAX_TRANSCRIPT_CHARS = 8000
MAX_EMAIL_BODY_CHARS = 1500
MAX_REPORT_OPPS = 7
MIN_REPORT_OPPS = 5


# ── Urgency scoring ─────────────────────────────────────────────────────────


def _urgency_score(row: dict, today: pd.Timestamp) -> float:
    """Rank an opp by urgency: close-date proximity + days since last touch + days open.

    Higher score = more urgent.
    """
    score = 0.0

    # Close date proximity — closer = more urgent
    close_date = pd.to_datetime(row.get("opportunity_close_date"), errors="coerce")
    if pd.notna(close_date):
        days_until_close = (close_date - today).days
        if days_until_close <= 0:
            score += 100          # already past close date
        elif days_until_close <= 7:
            score += 80
        elif days_until_close <= 14:
            score += 60
        elif days_until_close <= 30:
            score += 30
        else:
            score += max(0, 20 - days_until_close / 10)

    # Days since last touch — more stale = more urgent
    days_since_touch = float(row.get("days_since_last_touch") or 0)
    if days_since_touch >= 60:
        score += 40
    elif days_since_touch >= 30:
        score += 25
    elif days_since_touch >= 21:
        score += 15

    # Days open — longer = more urgent
    days_open = float(row.get("days_open") or 0)
    if days_open >= 120:
        score += 30
    elif days_open >= 90:
        score += 20
    elif days_open >= 60:
        score += 10

    return score


# ── Enrichment fetching ─────────────────────────────────────────────────────


def _fetch_opp_enrichment(account_id, notes_cache, emails_cache):
    """Fetch Gong calls, full transcript, emails, contacts, SFDC notes for one account."""
    result = {"calls": [], "transcript": "", "emails": [], "contacts": [], "notes": ""}

    # Gong section summaries
    try:
        cdf = run_query(CLEANUP_GONG_QUERY.format(account_id=account_id))
        result["calls"] = cdf.to_dict("records")
    except Exception:
        pass

    # Full transcript for most recent call
    try:
        tdf = run_query(
            GONG_FULL_TRANSCRIPT_QUERY.format(account_id=account_id, lookback_days=120),
        )
        if not tdf.empty:
            first_call = tdf["call_id"].iloc[0]
            call_rows = tdf[tdf["call_id"] == first_call]
            lines = []
            for _, row in call_rows.iterrows():
                speaker = row.get("speaker_email", "Unknown")
                is_ramp = row.get("is_ramp_participant", False)
                label = f"[Ramp] {speaker}" if is_ramp else speaker
                text = row.get("paragraph_text", "")
                if text:
                    lines.append(f"{label}: {text}")
            full = "\n".join(lines)
            if len(full) > MAX_TRANSCRIPT_CHARS:
                half = MAX_TRANSCRIPT_CHARS // 2
                full = full[:half] + "\n\n[... truncated ...]\n\n" + full[-half:]
            result["transcript"] = full
    except Exception:
        pass

    # SFDC notes (from batch cache)
    notes_row = notes_cache.get(account_id)
    if notes_row:
        parts = []
        for field, label in [
            ("am_notes", "AM Notes"),
            ("am_next_steps", "AM Next Steps"),
            ("csm_notes", "CSM Notes"),
            ("csm_next_steps", "CSM Next Steps"),
        ]:
            val = notes_row.get(field)
            if val and str(val).strip() and str(val).strip().lower() != "none":
                parts.append(f"{label}: {val}")
        result["notes"] = "\n".join(parts) if parts else ""

    # Emails (from batch cache)
    acct_emails = emails_cache.get(account_id, [])
    result["emails"] = acct_emails

    # Contacts
    try:
        contacts = fetch_contact_emails(conn, [account_id])
        result["contacts"] = contacts.get(account_id, [])
    except Exception:
        pass

    return result


# ── Claude analysis ──────────────────────────────────────────────────────────


def _analyze_opp_for_cleanup(opp_row: dict, enrichment: dict) -> list[dict]:
    """Send opp + enrichment to Claude for structured recommendations."""
    # Build context strings from enrichment data
    calls_text = ""
    for i, c in enumerate(enrichment.get("calls", [])[:3], 1):
        section = str(c.get("full_section_text", ""))[:1500]
        competitors = c.get("competitors_mentioned", "")
        products = c.get("products_mentioned", "")
        calls_text += (
            f"\n--- Call {i}: {c.get('call_name', '')} "
            f"({c.get('call_date', '')}, {c.get('duration_min', 0)} min) ---\n"
        )
        if competitors:
            calls_text += f"Competitors: {competitors}\n"
        if products:
            calls_text += f"Products discussed: {products}\n"
        calls_text += f"Summary: {section}\n"

    # Full transcript of most recent call
    transcript_text = enrichment.get("transcript", "")

    # SFDC notes
    notes_text = enrichment.get("notes", "")

    # Emails with body text
    emails_text = ""
    for i, e in enumerate(enrichment.get("emails", [])[:5], 1):
        direction = e.get("direction", "")
        date = e.get("email_date", "")
        subject = e.get("subject", "")
        body = str(e.get("body_text", "") or "")[:MAX_EMAIL_BODY_CHARS]
        flags = []
        if e.get("has_willing_to_meet"):
            flags.append("willing-to-meet")
        if e.get("has_not_interested"):
            flags.append("not-interested")
        if e.get("has_ooo"):
            flags.append("OOO")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        emails_text += f"Email {i}: {date} ({direction}){flag_str} — {subject}\n{body}\n\n"

    contacts_text = ""
    for c in enrichment.get("contacts", [])[:5]:
        c_name = c.get("name", "")
        if is_hash_like(c_name):
            continue
        contacts_text += f"- {c_name}"
        c_title = c.get("title", "")
        if c_title and not is_hash_like(c_title):
            contacts_text += f" ({c_title})"
        c_email = c.get("email", "")
        if c_email and not is_hash_like(c_email):
            contacts_text += f" <{c_email}>"
        contacts_text += "\n"

    prompt = f"""You are helping {OWNER_NAME}, a Growth Account Manager at Ramp, clean up his pipeline. Analyze this opportunity and provide specific, evidence-based recommendations.

OPPORTUNITY DATA:
- Account: {opp_row.get('account_name', '')}
- Product: {opp_row.get('expansion_subtype', '')}
- Current Stage: {opp_row.get('opportunity_stage_name', '')}
- Days Open: {int(opp_row.get('days_open', 0) or 0)}
- Created: {opp_row.get('created_date', '')}
- Close Date: {opp_row.get('opportunity_close_date', '')}
- Opp Amount: ${float(opp_row.get('monthly_expansion_amount', 0) or 0):,.0f}/month
- Recent L30D Spend: ${float(opp_row.get('recent_30d_spend', 0) or 0):,.0f}
- Days Since Last Contact: {int(opp_row.get('days_since_last_touch', 0) or 0)}

SFDC ACCOUNT NOTES:
{notes_text or 'No notes on file'}

CONTACTS:
{contacts_text or 'No contacts found'}

RECENT GONG CALLS (Section Summaries):
{calls_text or 'No recent calls found'}

{"MOST RECENT CALL — FULL TRANSCRIPT:" if transcript_text else ""}
{transcript_text}

RECENT EMAILS (with body text):
{emails_text or 'No recent emails'}

Today's date is {datetime.utcnow().strftime('%Y-%m-%d')}.

Analyze this opp and return a JSON array of recommendations. Each recommendation must use EXACTLY this structure:

[
  {{
    "type": "Stage Update | Amount Update | Missed Follow-up | Stale Close Date | Log Information",
    "current": "what the field currently says",
    "recommended": "what it should be changed to",
    "evidence": "quote the specific call excerpt, email text, or AM note that supports this",
    "confidence": "High | Medium | Low",
    "action": "the specific thing Greg should do"
  }}
]

Rules:
- type MUST be one of: Stage Update, Amount Update, Missed Follow-up, Stale Close Date, Log Information
- evidence MUST quote actual content from the transcript, emails, or notes above — never fabricate
- action must be specific and actionable (e.g. "Move stage to S3 — Validation", "Update close date to 2025-04-15", "Send follow-up email to [name] re: [topic]")
- If no recommendations are warranted, return exactly: NO_RECOMMENDATIONS
- Return ONLY the JSON array or NO_RECOMMENDATIONS, no other text"""

    try:
        raw = call_claude(prompt, max_tokens=1500)
    except Exception as e:
        logger.warning("Cleanup analysis failed for %s: %s", opp_row.get("account_name"), e)
        return []

    if "NO_RECOMMENDATIONS" in raw:
        return []

    # Parse structured blocks from Claude response using regex extraction.
    # Try JSON first, then fall back to regex-based field parsing.
    recs = []

    # Attempt JSON extraction
    json_match = re.search(r'\[.*\]', raw, re.DOTALL)
    if json_match:
        import json
        try:
            parsed = json.loads(json_match.group())
            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict) and item.get("type") and item.get("evidence"):
                        recs.append({
                            "type": item.get("type", ""),
                            "current": item.get("current", ""),
                            "recommended": item.get("recommended", ""),
                            "evidence": item.get("evidence", ""),
                            "confidence": item.get("confidence", "Medium"),
                            "action": item.get("action", ""),
                        })
                if recs:
                    return recs
        except (json.JSONDecodeError, ValueError):
            pass

    # Fallback: regex-based field parsing for non-JSON structured responses
    blocks = re.split(r'\n\s*\n', raw)
    for block in blocks:
        block = block.strip()
        if not block or "NO_RECOMMENDATIONS" in block:
            continue
        rec = {}
        for line in block.split('\n'):
            line = line.strip()
            upper = line.upper()
            if upper.startswith("TYPE:"):
                rec["type"] = line.split(":", 1)[1].strip()
            elif upper.startswith("CURRENT:"):
                rec["current"] = line.split(":", 1)[1].strip()
            elif upper.startswith("RECOMMENDED:"):
                rec["recommended"] = line.split(":", 1)[1].strip()
            elif upper.startswith("EVIDENCE:"):
                rec["evidence"] = line.split(":", 1)[1].strip()
            elif upper.startswith("CONFIDENCE:"):
                rec["confidence"] = line.split(":", 1)[1].strip()
            elif upper.startswith("ACTION:"):
                rec["action"] = line.split(":", 1)[1].strip()
        if rec.get("type") and rec.get("evidence"):
            recs.append(rec)

    return recs


# ── Slack output ─────────────────────────────────────────────────────────────


_TYPE_EMOJI = {
    "Stage Update": ":arrows_counterclockwise:",
    "Amount Update": ":moneybag:",
    "Missed Follow-up": ":mega:",
    "Stale Close Date": ":calendar:",
    "Log Information": ":memo:",
}


def _build_cleanup_blocks(ranked_results: list[dict]) -> list[dict]:
    """Build Slack Block Kit blocks for the pipeline cleanup report.

    Each result is a dict with keys: opp (dict), recommendations (list[dict]),
    urgency (float).
    """
    n = len(ranked_results)
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Pipeline Cleanup — {n} opp{'s' if n != 1 else ''} need{'s' if n == 1 else ''} attention",
                "emoji": True,
            },
        },
    ]

    for item in ranked_results:
        opp = item["opp"]
        recs = item["recommendations"]

        account_name = opp.get("account_name", "Unknown")
        opp_id = opp.get("opportunity_id", "")
        current_stage = opp.get("opportunity_stage_name", "")
        sf_link = sf_opp_url(opp_id) if opp_id else ""

        # Use the highest-confidence recommendation as the primary action
        primary = max(
            recs,
            key=lambda r: {"High": 3, "Medium": 2, "Low": 1}.get(r.get("confidence", ""), 0),
        )

        rec_type = primary.get("type", "Other")
        emoji = _TYPE_EMOJI.get(rec_type, ":pushpin:")
        action_text = primary.get("action", "Review this opp")
        evidence_text = primary.get("evidence", "")

        # Truncate evidence for display
        if len(evidence_text) > 200:
            evidence_text = evidence_text[:197] + "..."

        account_link = f"<{sf_link}|{account_name}>" if sf_link else account_name
        line = (
            f"{emoji} *{account_link}* — {current_stage}\n"
            f"      {action_text}\n"
            f"      _{evidence_text}_"
        )

        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": line}})

        # If multiple recs, add secondary ones as a compact sub-list
        if len(recs) > 1:
            extras = []
            for r in recs:
                if r is primary:
                    continue
                r_emoji = _TYPE_EMOJI.get(r.get("type", ""), ":pushpin:")
                extras.append(f"      {r_emoji} {r.get('action', '')}")
            if extras:
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": "\n".join(extras)}],
                })

        blocks.append({"type": "divider"})

    # Footer with dashboard links
    _pipe = dashboard_url("pipeline")
    _prosp = dashboard_url("prospecting")
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"<{_pipe}|Pipeline> · <{_prosp}|Prospecting> · `/pipeline-cleanup` to refresh"}],
    })

    return blocks


# ── Main entry point ─────────────────────────────────────────────────────────


def run_pipeline_cleanup(client, force=False):
    """Run full pipeline cleanup: load, pre-filter, enrich, analyze, DM Greg.

    Parameters
    ----------
    client : slack_sdk.WebClient
        Slack client for sending DMs.
    force : bool
        When True, bypass pre-filtering (used by slash command). All open opps
        from the query are analyzed.
    """
    try:
        all_opps = run_query(CLEANUP_OPPS_QUERY)
        if all_opps.empty:
            logger.info("Pipeline cleanup: no opps to analyze")
            return

        today = pd.Timestamp.now().normalize()

        # Pre-filter: 21+ days since last touch, close date within 14 days,
        # or 60+ days open — unless force=True bypasses filtering
        if force:
            candidates = all_opps.copy()
            logger.info("Pipeline cleanup (force): skipping pre-filter, %d opps", len(candidates))
        else:
            candidates = all_opps[
                (pd.to_numeric(all_opps["days_since_last_touch"], errors="coerce").fillna(999) >= 21)
                | (pd.to_datetime(all_opps["opportunity_close_date"], errors="coerce") <= today + pd.Timedelta(days=14))
                | (pd.to_numeric(all_opps["days_open"], errors="coerce").fillna(0) >= 60)
            ].copy()

        if candidates.empty:
            logger.info("Pipeline cleanup: no candidates after filtering")
            return

        # Score and rank by urgency, then cap at 5-7 opps
        candidates["_urgency"] = candidates.apply(
            lambda r: _urgency_score(r.to_dict(), today), axis=1
        )
        candidates = candidates.sort_values("_urgency", ascending=False)

        # Cap at MAX_REPORT_OPPS (7); ensure at least MIN_REPORT_OPPS (5) if available
        cap = min(MAX_REPORT_OPPS, len(candidates))
        candidates = candidates.head(cap)

        logger.info(
            "Pipeline cleanup: %d candidates after filter + rank (force=%s)",
            len(candidates), force,
        )

        unique_accounts = candidates["account_id"].dropna().unique().tolist()

        # ── Batch fetch SFDC notes ───────────────────────────────────────────
        notes_cache: dict = {}
        if unique_accounts:
            try:
                ids_str = ", ".join(f"'{a}'" for a in unique_accounts)
                notes_df = run_query(ACCOUNT_NOTES_QUERY.format(account_ids=ids_str))
                for _, row in notes_df.iterrows():
                    notes_cache[row["account_id"]] = row.to_dict()
            except Exception as e:
                logger.warning("SFDC notes query failed: %s", e)

        # ── Batch fetch emails ───────────────────────────────────────────────
        emails_cache: dict[str, list] = defaultdict(list)
        if unique_accounts:
            try:
                ids_str = ", ".join(f"'{a}'" for a in unique_accounts)
                emails_df = run_query(ACCOUNT_EMAILS_FULL_QUERY.format(account_ids=ids_str))
                for _, row in emails_df.iterrows():
                    emails_cache[row["account_id"]].append(row.to_dict())
            except Exception as e:
                logger.warning("Email comms query failed: %s", e)

        # ── Per-account enrichment (Gong calls + transcript + contacts) ──────
        enrichment_cache: dict = {}
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(
                    _fetch_opp_enrichment, aid, notes_cache, emails_cache
                ): aid
                for aid in unique_accounts
            }
            for future in as_completed(futures):
                aid = futures[future]
                try:
                    enrichment_cache[aid] = future.result()
                except Exception as e:
                    logger.warning("Enrichment failed for %s: %s", aid, e)

        # ── Send each opp to Claude for analysis ────────────────────────────
        results: list[dict] = []

        def _analyze_one(row_dict: dict) -> dict | None:
            enrichment = enrichment_cache.get(row_dict["account_id"], {})
            recs = _analyze_opp_for_cleanup(row_dict, enrichment)
            if recs:
                return {
                    "opp": row_dict,
                    "recommendations": recs,
                    "urgency": row_dict.get("_urgency", 0),
                }
            return None

        with ThreadPoolExecutor(max_workers=5) as executor:
            row_dicts = [row.to_dict() for _, row in candidates.iterrows()]
            futures = {
                executor.submit(_analyze_one, rd): rd for rd in row_dicts
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.warning("Cleanup analysis failed: %s", e)

        if not results:
            logger.info("Pipeline cleanup: no recommendations generated")
            return

        # Sort final results by urgency (most urgent first)
        results.sort(key=lambda r: r["urgency"], reverse=True)

        # Build and send Slack message
        blocks = _build_cleanup_blocks(results)
        client.chat_postMessage(
            channel=GREG_SLACK_ID,
            blocks=blocks,
            text=f"Pipeline Cleanup — {len(results)} opps need attention",
        )
        logger.info("Pipeline cleanup sent: %d opps with recommendations", len(results))

    except Exception as e:
        logger.error("Pipeline cleanup job failed: %s", e)
