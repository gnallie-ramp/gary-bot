"""Pipeline SFDC Update Proposer — grounded field-patch suggestions for opps.

Called from the Pipeline tab's "Propose Updates" button. For each open opp
on the account, analyzes the most recent Gong transcript, email history,
and current SFDC field values to propose updates to next_step,
next_step_due_date, expansion_notes, stage, and close_date.

Grounding rules (same philosophy as summarizer + drafter):
  - Never invent dates, quotes, or customer statements
  - Only propose a stage advancement if there's concrete evidence
    (customer confirmed a demo, asked for pricing, etc.)
  - Only propose a next_step change if the current field is stale vs.
    what was discussed, or the current value is blank/"CW"
  - If nothing needs changing, return an empty patch — don't force changes

Output format: a dict per opp with only the fields that should change.
Applied via `core.salesforce_client.update_opportunity(opp_id, fields)`.
"""
from __future__ import annotations

import json
import logging
import re

from core.claude_client import call_claude
from core.snowflake_client import run_query
from queries.queries import ACCOUNT_EMAIL_HISTORY_QUERY, format_query

logger = logging.getLogger(__name__)

PROPOSER_MODEL = "claude-sonnet-4-20250514"
CALL_TRANSCRIPT_CAP = 3500

_SYSTEM_PROMPT = """You propose targeted SFDC field updates for an open
Ramp expansion or renewal opportunity based on the MOST RECENT customer
engagement (Gong call + email history). Your job is to help the AM keep
their pipeline hygiene honest.

Return a single JSON object with these optional keys — include ONLY the
fields that should change; omit anything that's already accurate:

  {
    "next_step": "<≤120 chars, concrete next action grounded in what was
                  discussed or emailed>",
    "next_step_due_date": "<YYYY-MM-DD, reasonable near-term date tied to
                           the proposed next_step>",
    "expansion_notes": "<≤200 chars, crisp deal context. Do NOT paste the
                        raw call transcript. Synthesize a sales-focused
                        deal-state line: product, amount, timing, blocker
                        (if any). Replace whatever is there — do not append.>",
    "stage": "<new stage name, e.g. 'S3: Securing Technical Win'. ONLY if
              there's concrete evidence of advancement.>",
    "close_date": "<YYYY-MM-DD, ONLY if a new target date was discussed>",
    "rationale": "<REQUIRED, 1 sentence explaining why each proposed change
                   is justified by the source data, referencing the specific
                   call or email it's drawn from. If proposing no changes,
                   still include a rationale explaining why current fields
                   look accurate.>"
  }

HARD RULES — any violation is a failure:
- Do NOT invent customer quotes, dates, or statements. Only reference
  facts present in the source data.
- **The call transcript is ACCOUNT-LEVEL.** It may discuss a product
  that is NOT the product of this specific opp. Before proposing any
  change to an opp, verify the call substantively discussed THIS opp's
  product. If the call was entirely about a different product:
    - Do NOT propose a next_step change based on that call
    - Do NOT propose stage advancement
    - Do NOT rewrite expansion_notes with content from an unrelated product
    - Return minimal or empty proposals with rationale explaining the call
      was about a different product
- **If this opp's `type` is Renewal**, ONLY propose changes when the
  source data EXPLICITLY discussed one of: annual contract renewal, Plus
  subscription renewal, pricing/discounting for a renewal, contract term
  negotiation, or the act of renewing. Discussion of Plus product
  *expansion* (new features, upgrading to Plus for the first time, adding
  products on top of Plus) is NOT evidence for a Renewal opp. Discussion
  of other expansion products (Treasury, Bill Pay, Card) is NEVER evidence
  for a Renewal opp. If the call didn't substantively discuss the
  subscription renewal itself, return empty proposals with rationale
  "No renewal-specific discussion on file".
- Do NOT propose a stage change without concrete transcript evidence
  specific to THIS opp's product. Examples of valid evidence: "Brad
  confirmed CFO approval on the Plus upgrade", "Ashley agreed to
  schedule the KYC call for Treasury", "they asked to see the bill pay
  contract". A single passing mention is NOT evidence.
- Any `stage` value MUST be drawn EXACTLY from this picklist (copy the
  string verbatim including spaces and capitalization):
    S0: Holding, S0: Research, Identifying Risk, S1: Sales Accepted
    Opportunity, S2: Sales Qualified Opportunity, Confirming Risk,
    S3: Securing Technical Win, Confirmed Churn, S4: Securing Business
    Win, S5: Finalizing Closure, Committed.
  Do NOT invent stage names like "S3: Solution Validation" or
  "S4: Final Business Review" — those are not real stages. If you're
  unsure, omit the stage field.
- Do NOT propose a next_step that's vaguely aspirational. Be concrete
  — "Schedule 30-min session with Ashley on KYC" beats "Follow up".
- Do NOT propose a change that would revert recent AM work. If the
  current next_step is fresh (matches what was just discussed), leave it.
- `next_step_due_date` MUST be AT LEAST 3 days after TODAY's date
  (provided in the payload as TODAY). Never backdate — if you're about
  to propose a date in the past, add days until it's in the future.
- Do NOT fabricate close dates. Only propose one if the customer stated
  a specific timeline OR the current close date is clearly stale (past
  today or pushed multiple times). Proposed close_date must also be
  after TODAY.
- If the source data is thin (no recent call, no email activity) OR
  the call doesn't touch this opp's product, return
  {"rationale": "No recent engagement on this product — current fields
  retained."} with no other fields.
- Output ONLY the JSON object — no markdown fences, no prose.
"""


# Real SFDC Expansion opp stage picklist. Source: SFDC "Stage" field on the
# Expansion record type. Keep this in sync with Salesforce if new values are
# added. Numeric ranks enforce "never propose backward movement."
_STAGE_ORDER = {
    "S0: Holding":                     0,
    "S0: Research":                    0,
    "Identifying Risk":                0,
    "S1: Sales Accepted Opportunity":  1,
    "S2: Sales Qualified Opportunity": 2,
    "Confirming Risk":                 2,
    "S3: Securing Technical Win":      3,
    "Confirmed Churn":                 3,
    "S4: Securing Business Win":       4,
    "S5: Finalizing Closure":          5,
    "Committed":                       5,
}

# Valid stage values for the Stage picklist. Used by the prompt to prevent
# Claude from inventing names (e.g. "S3: Solution Validation" — not a thing).
_VALID_STAGES_LIST = ", ".join(f'"{s}"' for s in _STAGE_ORDER.keys())


def _build_opp_payload(opp: dict, account_name: str, row: dict,
                        email_history: list[dict]) -> str:
    import pandas as pd
    from datetime import date
    today = date.today().isoformat()
    lines = [
        f"TODAY: {today}",
        f"ACCOUNT: {account_name}",
        f"OPPORTUNITY_ID: {opp.get('opp_id', '?')}",
        "",
        "CURRENT SFDC FIELDS (what's on file today):",
        f"  type: {opp.get('type')}",
        f"  product: {opp.get('product') or '(none)'}",
        f"  stage: {opp.get('stage') or '?'}",
        f"  close_date: {opp.get('close_date') or '?'}",
        f"  monthly_amount: ${int(float(opp.get('monthly_amount') or 0))}",
        f"  next_step: {opp.get('next_step') or '(blank)'}",
        f"  expansion_notes: {opp.get('expansion_notes') or '(blank)'}",
        "",
    ]

    call_title = row.get("last_call_title")
    call_date = row.get("last_call_date")
    call_summary = (row.get("last_call_summary") or "").strip()
    if call_title and call_date is not None and not pd.isna(call_date) and call_summary:
        transcript = call_summary
        if len(transcript) > CALL_TRANSCRIPT_CAP:
            transcript = transcript[:CALL_TRANSCRIPT_CAP] + "…[truncated]"
        lines.append("MOST RECENT CALL (account-level, may cover multiple opps):")
        lines.append(f"  Date: {call_date}")
        lines.append(f"  Title: {call_title}")
        lines.append(f"  Transcript: {transcript}")
        lines.append("")
    else:
        lines.append("MOST RECENT CALL: none on file within last year")
        lines.append("")

    if email_history:
        lines.append("RECENT EMAIL THREADS (newest first, last 180 days):")
        for t in email_history:
            lines.append(
                f"  {t['date']} {t['direction'].upper()} "
                f"subj=\"{t['subject']}\" ramp_side={t['ramp_side_owner'] or '?'}"
            )
    else:
        lines.append("RECENT EMAIL THREADS: none in last 180 days")

    return "\n".join(lines)


def _fetch_email_history(account_id: str, limit: int = 5) -> list[dict]:
    try:
        q = format_query(ACCOUNT_EMAIL_HISTORY_QUERY, account_id=account_id, limit=limit)
        df = run_query(q)
        df.columns = [c.lower() for c in df.columns]
        return [
            {
                "date": str(r.get("email_date") or ""),
                "subject": str(r.get("subject") or "")[:120],
                "direction": str(r.get("direction") or "").strip(),
                "ramp_side_owner": str(r.get("ramp_side_owner") or ""),
            }
            for _, r in df.iterrows()
        ]
    except Exception as e:
        logger.debug("Email history fetch failed for %s: %s", account_id, e)
        return []


def _extract_json(text: str) -> dict | None:
    """Pull the first JSON object out of Claude's response (handles stray prose)."""
    if not text:
        return None
    # Try direct parse first
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Fall back to finding the first {...}
    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def propose_updates_for_opp(opp: dict, account_name: str, row: dict,
                             email_history: list[dict] | None = None) -> dict:
    """Return a dict of proposed field changes for ONE opp.

    Shape:
      {
        "opp_id": ...,
        "product": ...,
        "proposals": {<field>: <new_value>},
        "rationale": <str>,
        "current": {<field>: <current_value>},  # for diff display
        "empty": bool,  # True if no changes proposed
      }
    """
    if email_history is None:
        email_history = _fetch_email_history(opp.get("account_id") or row.get("account_id"))

    payload = _build_opp_payload(opp, account_name, row, email_history)

    try:
        raw = call_claude(
            prompt=f"SOURCE DATA:\n{payload}\n\nReturn the JSON object now.",
            max_tokens=500,
            model=PROPOSER_MODEL,
            system=_SYSTEM_PROMPT,
        )
    except Exception as e:
        logger.warning("Update proposal generation failed for %s: %s",
                       opp.get("opp_id"), e)
        return {
            "opp_id": opp.get("opp_id"),
            "product": opp.get("product") or opp.get("type"),
            "proposals": {},
            "rationale": f"Proposer error: {e}",
            "current": {
                "next_step": opp.get("next_step") or "",
                "expansion_notes": opp.get("expansion_notes") or "",
                "stage": opp.get("stage") or "",
                "close_date": str(opp.get("close_date") or ""),
            },
            "empty": True,
        }

    parsed = _extract_json(raw) or {}

    # Keep only valid proposal keys
    allowed = {"next_step", "next_step_due_date", "expansion_notes",
               "stage", "close_date"}
    proposals = {k: v for k, v in parsed.items() if k in allowed and v}

    # Defensive: reject stage changes that go backward
    current_stage = opp.get("stage") or ""
    current_rank = _STAGE_ORDER.get(current_stage, -1)
    proposed_stage = proposals.get("stage")
    if proposed_stage and current_rank >= 0:
        if _STAGE_ORDER.get(proposed_stage, -1) <= current_rank:
            proposals.pop("stage", None)

    # Drop fields whose proposal matches the current value (no change)
    if proposals.get("next_step") and proposals["next_step"] == (opp.get("next_step") or ""):
        proposals.pop("next_step", None)
    if proposals.get("expansion_notes") and proposals["expansion_notes"] == (opp.get("expansion_notes") or ""):
        proposals.pop("expansion_notes", None)
    if proposals.get("close_date") and str(proposals["close_date"]) == str(opp.get("close_date") or ""):
        proposals.pop("close_date", None)

    # Reject any proposed dates that are in the past (proposer safety net)
    from datetime import date
    today = date.today()
    for date_field in ("next_step_due_date", "close_date"):
        v = proposals.get(date_field)
        if not v:
            continue
        try:
            parsed_d = date.fromisoformat(str(v))
            if parsed_d < today:
                logger.info("Dropping past-dated %s=%s for opp %s",
                            date_field, v, opp.get("opp_id"))
                proposals.pop(date_field, None)
        except (ValueError, TypeError):
            proposals.pop(date_field, None)

    return {
        "opp_id": opp.get("opp_id"),
        "product": opp.get("product") or opp.get("type"),
        "proposals": proposals,
        "rationale": parsed.get("rationale", ""),
        "current": {
            "next_step": opp.get("next_step") or "",
            "expansion_notes": opp.get("expansion_notes") or "",
            "stage": opp.get("stage") or "",
            "close_date": str(opp.get("close_date") or ""),
            "next_step_due_date": "",  # not in our row today
        },
        "empty": not proposals,
    }


def build_opp_review_blocks(pending: dict) -> list[dict]:
    """Render the SFDC update review card for a single opp.

    Shared by the Pipeline tab "Propose Updates" button and the post-meeting
    DMs from granola_followup / post_meeting_followup.

    `pending` shape:
      {opp_id, account_id, account_name, product, proposals, current,
       rationale, channel?, message_ts?}
    """
    import json as _json

    account_name = pending.get("account_name", "Unknown")
    product = pending.get("product") or "?"
    proposals = pending.get("proposals") or {}
    current = pending.get("current") or {}
    rationale = pending.get("rationale", "")
    opp_id = pending.get("opp_id", "")

    header = f":arrows_counterclockwise: *{account_name}* — *{product}* opp"
    if not proposals:
        return [{
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"{header}\n_No changes proposed._\n_{rationale}_"},
        }]

    lines = [header]
    if rationale:
        lines.append(f"_{rationale}_")
    lines.append(" ")
    lines.append("*Proposed changes:*")

    field_label_map = {
        "next_step": "Next Step",
        "next_step_due_date": "Next Step Due",
        "expansion_notes": "Expansion Notes",
        "stage": "Stage",
        "close_date": "Close Date",
    }

    def _trunc(v, n=140):
        v = str(v) if v else "_(blank)_"
        return v[:n] + "…" if len(v) > n else v

    for field, new_value in proposals.items():
        label = field_label_map.get(field, field)
        old_value = current.get(field) or "_(blank)_"
        lines.append(f"• *{label}*")
        lines.append(f"   _Current:_ {_trunc(old_value)}")
        lines.append(f"   *Proposed:* {_trunc(new_value)}")

    blocks = [{
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join(lines)},
    }]

    short = (opp_id or "unknown")[-12:]
    elements = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": ":white_check_mark: Apply Selected", "emoji": True},
            "action_id": f"pipeline_apply_updates_{opp_id}",
            "style": "primary",
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": ":x: Dismiss", "emoji": True},
            "action_id": f"pipeline_dismiss_updates_{opp_id}",
        },
    ]
    for field in list(proposals.keys())[:5]:
        label = field_label_map.get(field, field)
        elements.append({
            "type": "button",
            "text": {"type": "plain_text", "text": f"Skip {label}", "emoji": True},
            "action_id": f"pipeline_skip_field_{field}_{short}",
            "value": _json.dumps({"opp_id": opp_id, "field": field}),
        })
    blocks.append({"type": "actions", "elements": elements[:25]})
    return blocks


def dm_account_update_review(client, user_id: str, account_id: str,
                              pending_store: dict,
                              source_label: str = "Pipeline") -> int:
    """Run the proposer for an account and DM one review card per opp with
    changes. Stores pending state in the provided dict so Apply/Skip handlers
    can find it.

    Returns the number of review cards posted (0 = no changes needed).
    """
    try:
        result = propose_updates_for_account(account_id, user_id=user_id)
    except Exception as e:
        logger.warning("Auto-propose failed for %s: %s", account_id, e)
        return 0

    account_name = result.get("account_name", "Unknown")
    proposals_by_opp = result.get("proposals_by_opp") or {}

    if not result.get("any_proposals"):
        # Silently skip the DM for post-meeting flows — no need to tell the
        # user "nothing to update" when they're already getting a DM about
        # the meeting itself. Pipeline-tab caller handles the empty case.
        return 0

    posted = 0
    if user_id not in pending_store:
        pending_store[user_id] = {}

    for opp_id, proposal in proposals_by_opp.items():
        if proposal.get("empty"):
            continue
        pending = {
            "opp_id": opp_id,
            "account_id": account_id,
            "account_name": account_name,
            "product": proposal.get("product") or "?",
            "proposals": dict(proposal.get("proposals") or {}),
            "current": dict(proposal.get("current") or {}),
            "rationale": proposal.get("rationale", ""),
        }
        pending_store[user_id][opp_id] = pending
        blocks = build_opp_review_blocks(pending)
        try:
            resp = client.chat_postMessage(
                channel=user_id,
                text=f"SFDC update proposal ({source_label}) for {account_name} — {pending['product']}",
                blocks=blocks,
            )
            pending["channel"] = resp["channel"]
            pending["message_ts"] = resp["ts"]
            posted += 1
        except Exception as e:
            logger.warning("Failed to DM review card for %s: %s", opp_id, e)
    return posted


def propose_updates_for_account(account_id: str, user_id: str = None) -> dict:
    """Run proposer for every open opp on an account.

    Returns {"account_id", "account_name", "proposals_by_opp": {opp_id: ...},
    "any_proposals": bool}.
    """
    from handlers.home_tab import _fetch_pipeline_data

    df = _fetch_pipeline_data(user_id)
    if df is None or df.empty:
        return {"account_id": account_id, "account_name": "Unknown",
                "proposals_by_opp": {}, "any_proposals": False}

    match = df[df["account_id"] == account_id]
    if match.empty:
        return {"account_id": account_id, "account_name": "Unknown",
                "proposals_by_opp": {}, "any_proposals": False}

    row = match.iloc[0].to_dict()
    account_name = row.get("account_name") or "Unknown"
    opps = row.get("opps") or []

    # Fetch email history once per account (shared across opps)
    email_history = _fetch_email_history(account_id)

    proposals_by_opp = {}
    any_proposals = False
    for opp in opps:
        opp["account_id"] = account_id  # ensure opp dict has account_id for fallback
        result = propose_updates_for_opp(opp, account_name, row, email_history)
        proposals_by_opp[opp["opp_id"]] = result
        if not result.get("empty"):
            any_proposals = True

    return {
        "account_id": account_id,
        "account_name": account_name,
        "proposals_by_opp": proposals_by_opp,
        "any_proposals": any_proposals,
    }
