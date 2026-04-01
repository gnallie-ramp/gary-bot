"""Weekly forecasting summary — Monday 7:00 AM PT.

Pulls S3+ opps closing this calendar month and S2 opps with recent buying
signals into a single query, builds per-opp assessments from the data, then
hands everything to Claude for a coaching brief with recommended actions and
blocker detection.
"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from queries.queries import FORECASTING_PIPELINE_QUERY, format_query
from core.snowflake_client import run_query
from core.claude_client import call_claude_json
from core.slack_formatter import sf_opp_url, format_currency, simple_dm_blocks, dashboard_url
from config import GREG_SLACK_ID, NTR_RATES, COMMAND_PREFIX
from core.user_registry import get_user_sf_name

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _estimate_cp(product: str, spend_delta: float) -> float:
    """Return estimated CP for a spend delta based on NTR rates."""
    ntr = NTR_RATES.get(product, 0.0095)
    return round(ntr * max(0, spend_delta), 2)


def _build_assessment(row: pd.Series) -> str:
    """Build a one-line forecast assessment from query data — no Claude needed.

    Assessments fall into three buckets:
      - S3+ closing this month: 'On track' or 'At risk'
      - S2 with buying signal:  'Strong signal'
    """
    group = row.get("forecast_group", "")
    days_since = int(row.get("days_since_activity", 999) or 999)
    days_to_close = int(row.get("days_to_close", 0) or 0)
    close_date = row.get("opportunity_close_date", "")
    last_activity = row.get("last_activity_date", "")

    # ── S2 candidates — always a positive buying-signal framing ──
    if group == "s2_candidate":
        pieces = []
        # Willing-to-meet reply is the strongest signal
        if row.get("has_willing_to_meet"):
            email_date = row.get("recent_email_date", "")
            pieces.append(f"willing-to-meet reply on {email_date}")
        elif row.get("recent_call_date"):
            pieces.append(f"Gong call on {row['recent_call_date']}")
        elif row.get("recent_email_date"):
            direction = str(row.get("recent_email_direction", "")).lower()
            label = "inbound reply" if direction == "inbound" else "email"
            pieces.append(f"{label} on {row['recent_email_date']}")
        else:
            pieces.append(f"activity within {days_since}d")

        detail = ", ".join(pieces)
        return f"Strong signal — {detail}, consider advancing"

    # ── S3+ closing this month ──
    # Determine activity recency signals
    recent_call = row.get("recent_call_date")
    recent_email = row.get("recent_email_date")
    products_discussed = row.get("products_discussed", "")
    has_not_interested = row.get("has_not_interested", False)

    # Risk factors
    if has_not_interested:
        return (
            f"At risk — not-interested signal detected, "
            f"close date {close_date}"
        )

    if days_since > 10:
        return (
            f"At risk — no activity in {days_since} days, "
            f"close date {close_date}"
        )

    # On track — describe the most recent engagement
    if recent_call:
        detail = f"call on {recent_call}"
        if products_discussed:
            detail = f"{products_discussed} discussed {recent_call}"
        # Look for next step info in the call summary
        return f"On track — {detail}, {days_to_close}d to close"

    if recent_email:
        direction = str(row.get("recent_email_direction", "")).lower()
        if direction == "inbound":
            return f"On track — inbound reply {recent_email}, {days_to_close}d to close"
        return f"On track — last email {recent_email}, {days_to_close}d to close"

    # Fallback: have activity but not in the recent_ CTEs
    if last_activity and str(last_activity) > "2000-01-01":
        return f"On track — last activity {last_activity}, {days_to_close}d to close"

    return f"At risk — no recent activity found, close date {close_date}"


def _build_opp_context(row: pd.Series) -> dict:
    """Package a single opp row into a dict for both the Claude prompt and
    the Slack output.
    """
    product = row.get("expansion_subtype", "")
    baseline = float(row.get("baseline", 0) or 0)
    recent = float(row.get("recent_l30d", 0) or 0)
    over = float(row.get("over_baseline", 0) or 0)
    est_cp = _estimate_cp(product, max(0, over))
    assessment = _build_assessment(row)

    return {
        "opp_id": row.get("opportunity_id", ""),
        "account_id": row.get("account_id", ""),
        "account_name": row.get("account_name", ""),
        "product": product,
        "stage": row.get("opportunity_stage_name", ""),
        "close_date": str(row.get("opportunity_close_date", "")),
        "days_to_close": int(row.get("days_to_close", 0) or 0),
        "days_since_activity": int(row.get("days_since_activity", 0) or 0),
        "last_activity_date": str(row.get("last_activity_date", "")),
        "baseline": baseline,
        "recent_l30d": recent,
        "over_baseline": over,
        "est_cp": est_cp,
        "assessment": assessment,
        "forecast_group": row.get("forecast_group", ""),
        # Gong/email context for Claude blocker detection
        "recent_call_summary": str(row.get("recent_call_summary", "") or ""),
        "competitors": str(row.get("competitors", "") or ""),
        "recent_email_subject": str(row.get("recent_email_subject", "") or ""),
        "has_willing_to_meet": bool(row.get("has_willing_to_meet", False)),
        "has_not_interested": bool(row.get("has_not_interested", False)),
        "monthly_expansion_amount": float(
            row.get("monthly_expansion_amount", 0) or 0
        ),
    }


# ---------------------------------------------------------------------------
# Claude coaching brief
# ---------------------------------------------------------------------------

def _build_claude_prompt(closing_opps: list[dict], s2_opps: list[dict], owner_name: str = "") -> str:
    """Assemble the prompt that Claude will use to generate the coaching brief."""

    def _opp_lines(opps: list[dict]) -> str:
        lines = []
        for o in opps:
            line = (
                f"- {o['account_name']} | {o['product']} | {o['stage']} | "
                f"Close {o['close_date']} ({o['days_to_close']}d) | "
                f"Baseline {format_currency(o['baseline'])} | "
                f"Recent L30D {format_currency(o['recent_l30d'])} | "
                f"Over baseline {format_currency(o['over_baseline'])} | "
                f"Est CP {format_currency(o['est_cp'])} | "
                f"Last activity {o['last_activity_date']} ({o['days_since_activity']}d ago) | "
                f"Assessment: {o['assessment']}"
            )
            if o["recent_call_summary"]:
                # Truncate to keep prompt size reasonable
                summary = o["recent_call_summary"][:600]
                line += f"\n  Gong summary: {summary}"
            if o["competitors"]:
                line += f"\n  Competitors mentioned: {o['competitors']}"
            if o["recent_email_subject"]:
                line += f"\n  Recent email: {o['recent_email_subject']}"
            if o["has_not_interested"]:
                line += "\n  WARNING: not-interested signal detected in email"
            lines.append(line)
        return "\n".join(lines) if lines else "(none)"

    closing_text = _opp_lines(closing_opps)
    s2_text = _opp_lines(s2_opps)

    prompt = f"""You are a sales coach helping {owner_name}, a Growth Account Manager at Ramp.
{owner_name} manages ~4,000 Plus segment accounts. Their comp is 75% Realized CP (expansion opps)
and 25% SaaS Renewals. They earn comp on spend ABOVE baseline during a 90-day window
after closing an opp. Baseline = L30D spend at close-won date. CP = (post-close spend -
baseline) * NTR. Timing closes is everything — close early to lock in low baseline.

NTR rates: Card 95bps, Bill Pay 15bps, Treasury 5bps, Travel 350bps.

Here are their pipeline opps for this week's forecast:

== S3+ OPPS CLOSING THIS MONTH ==
{closing_text}

== S2 OPPS WITH BUYING SIGNALS (last 14 days) ==
{s2_text}

Analyze the pipeline and return a JSON object (no markdown fences) with these keys:

1. "summary": A single sentence like "3 opps closing this month, 2 looking strong, 1 at risk. 4 S2 opps worth advancing." Use actual counts.

2. "closing_actions": Array of objects, one per S3+ opp:
   {{"opp_id": "...", "action": "Specific recommended action for this week"}}
   Be specific — reference dates, people, amounts. Focus on what maximizes CP.

3. "s2_actions": Array of objects, one per S2 opp:
   {{"opp_id": "...", "action": "Specific recommended action to advance this opp"}}

4. "blockers": Array of strings — key blockers detected from Gong transcripts, emails,
   or activity patterns. Look for: procurement review, legal review, budget freeze,
   competitor mentions, not-interested signals, long gaps in activity. Be specific
   with account names and dates.

5. "momentum": A 1-2 sentence assessment of this week's pipeline trajectory — is {owner_name}
   building momentum or losing it? What's the single most important thing to focus on?

Return ONLY the JSON object."""

    return prompt


# ---------------------------------------------------------------------------
# Slack Block Kit output
# ---------------------------------------------------------------------------

def _build_blocks(
    week_label: str,
    summary: str,
    closing_opps: list[dict],
    closing_actions: dict[str, str],
    s2_opps: list[dict],
    s2_actions: dict[str, str],
    blockers: list[str],
    momentum: str,
) -> list[dict]:
    """Build Slack Block Kit blocks for the weekly forecast DM."""

    blocks: list[dict] = []

    # ── Header ──
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"Weekly Forecast — Week of {week_label}",
            "emoji": True,
        },
    })

    # ── Summary line ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*{summary}*"},
    })

    # ── Momentum ──
    if momentum:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"_{momentum}_"},
        })

    blocks.append({"type": "divider"})

    # ── Closing This Month (S3+) ──
    closing_header = f"*Closing This Month (S3+)* — {len(closing_opps)} opps"
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": closing_header},
    })

    for opp in closing_opps:
        sf_link = sf_opp_url(opp["opp_id"])
        action = closing_actions.get(opp["opp_id"], "")
        lines = [
            f"<{sf_link}|*{opp['account_name']}*>  |  {opp['stage']}  |  "
            f"Close {opp['close_date']}  |  {opp['days_to_close']}d left",
            f"Last activity: {opp['last_activity_date']}  |  "
            f"Baseline: {format_currency(opp['baseline'])}  |  "
            f"L30D: {format_currency(opp['recent_l30d'])}  |  "
            f"Est CP: {format_currency(opp['est_cp'])}",
            f"_{opp['assessment']}_",
        ]
        if action:
            lines.append(f"Action: {action}")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        })

    if not closing_opps:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "No S3+ opps closing this month."},
        })

    blocks.append({"type": "divider"})

    # ── Worth Advancing (S2) ──
    s2_header = f"*Worth Advancing (S2)* — {len(s2_opps)} opps"
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": s2_header},
    })

    for opp in s2_opps:
        sf_link = sf_opp_url(opp["opp_id"])
        action = s2_actions.get(opp["opp_id"], "")
        lines = [
            f"<{sf_link}|*{opp['account_name']}*>  |  {opp['stage']}  |  "
            f"Close {opp['close_date']}  |  {opp['days_to_close']}d left",
            f"Last activity: {opp['last_activity_date']}  |  "
            f"Baseline: {format_currency(opp['baseline'])}  |  "
            f"L30D: {format_currency(opp['recent_l30d'])}",
            f"_{opp['assessment']}_",
        ]
        if action:
            lines.append(f"Action: {action}")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        })

    if not s2_opps:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "No S2 opps with recent buying signals."},
        })

    # ── Blockers ──
    if blockers:
        blocks.append({"type": "divider"})
        blocker_lines = ["*Blockers Detected:*"]
        for b in blockers:
            blocker_lines.append(f"  \u2022 {b}")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(blocker_lines)},
        })

    # Footer with dashboard links
    blocks.append({"type": "divider"})
    _perf = dashboard_url("performance")
    _pipe = dashboard_url("pipeline")
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"<{_perf}|Performance> · <{_pipe}|Pipeline> · `/{COMMAND_PREFIX}-forecast` to refresh"}],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_forecasting(client, user_id=None, force=False):
    """Generate weekly forecasting briefing and DM Greg.

    Parameters
    ----------
    client : slack_sdk.WebClient
        The Slack client used to send DMs.
    force : bool
        When True, run regardless of day-of-week checks (for manual triggers).
    """
    dm_target = user_id or GREG_SLACK_ID
    owner_name = get_user_sf_name(user_id)

    try:
        # ── Pull pipeline data ──
        df = run_query(format_query(FORECASTING_PIPELINE_QUERY, user_id=user_id))

        if df.empty:
            logger.info("Forecasting: no pipeline data returned")
            if force:
                blocks = simple_dm_blocks(
                    "Weekly Forecast",
                    "No open pipeline opps found for this week's forecast.",
                )
                client.chat_postMessage(
                    channel=dm_target, blocks=blocks,
                    text="Weekly Forecast — no data",
                )
            return

        # ── Split into closing-this-month vs S2 candidates ──
        closing_df = (
            df[df["forecast_group"] == "closing_this_month"]
            .sort_values("opportunity_close_date", ascending=True)
        )
        s2_df = (
            df[df["forecast_group"] == "s2_candidate"]
            .sort_values("opportunity_close_date", ascending=True)
        )

        # ── Build per-opp context ──
        closing_opps = [_build_opp_context(row) for _, row in closing_df.iterrows()]
        s2_opps = [_build_opp_context(row) for _, row in s2_df.iterrows()]

        # ── Call Claude for coaching brief ──
        prompt = _build_claude_prompt(closing_opps, s2_opps, owner_name=owner_name)
        brief = call_claude_json(prompt, max_tokens=2000)

        summary = brief.get("summary", "Pipeline summary unavailable.")
        momentum = brief.get("momentum", "")
        blockers = brief.get("blockers", [])

        # Index actions by opp_id for easy lookup
        closing_actions: dict[str, str] = {
            a["opp_id"]: a["action"]
            for a in brief.get("closing_actions", [])
            if a.get("opp_id")
        }
        s2_actions: dict[str, str] = {
            a["opp_id"]: a["action"]
            for a in brief.get("s2_actions", [])
            if a.get("opp_id")
        }

        # ── Build Slack blocks ──
        today = datetime.now()
        week_label = today.strftime("%b %-d, %Y")

        blocks = _build_blocks(
            week_label=week_label,
            summary=summary,
            closing_opps=closing_opps,
            closing_actions=closing_actions,
            s2_opps=s2_opps,
            s2_actions=s2_actions,
            blockers=blockers,
            momentum=momentum,
        )

        client.chat_postMessage(
            channel=dm_target,
            blocks=blocks,
            text=f"Weekly Forecast — Week of {week_label}",
        )
        logger.info(
            "Weekly forecasting sent: %d closing, %d S2, %d blockers",
            len(closing_opps), len(s2_opps), len(blockers),
        )

    except Exception as e:
        logger.error("Forecasting job failed: %s", e)
        if force:
            try:
                blocks = simple_dm_blocks(
                    "Weekly Forecast — Error",
                    f"Forecasting job failed:\n```{e}```",
                )
                client.chat_postMessage(
                    channel=dm_target, blocks=blocks,
                    text="Weekly Forecast — Error",
                )
            except Exception:
                logger.error("Could not DM Greg about forecasting failure")
