"""Play discovery scraper — weekly digest of recurring Ramp Research queries.

Scans the last 7 days of #gam-ask-ai for messages directed at Ramp Research
(the Snowflake NL→SQL bot). Clusters similar queries using Claude and DMs
the bot owner a list of "potential new plays" — ad-hoc BoB segmentations
that GAMs asked for 3+ times over the week, which might be worth baking
into the Plays tab as a first-class signal.

This runs weekly (Monday 7 AM PT) and only fires the DM when at least one
cluster with 2+ similar queries is found.

Trigger: scheduled job or ad-hoc from Glass.
Requires: bot membership in #gam-ask-ai.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from config import SLACK_BOT_TOKEN, GREG_SLACK_ID
from core.claude_client import call_claude

logger = logging.getLogger(__name__)

RAMP_RESEARCH_ID = "U092WAYB2JW"
GAM_CHANNEL_ID = "C0AA27QHGDB"
LOOKBACK_DAYS = 7
MAX_MESSAGES = 400
CACHE_FILE = os.path.expanduser("~/.gary_bot_play_discovery_last_digest.json")


def _load_last_digest() -> dict:
    """Return the most recent digest (used to suppress same-cluster DMs)."""
    try:
        with open(CACHE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_last_digest(data: dict) -> None:
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, CACHE_FILE)


def _fetch_research_queries(client: WebClient) -> list[dict]:
    """Return [{ts, user, text}] for messages mentioning Ramp Research in the last week."""
    oldest = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).timestamp()
    queries = []
    cursor = None
    fetched = 0
    while fetched < MAX_MESSAGES:
        try:
            resp = client.conversations_history(
                channel=GAM_CHANNEL_ID,
                oldest=str(oldest),
                limit=min(200, MAX_MESSAGES - fetched),
                cursor=cursor,
            )
        except SlackApiError as e:
            logger.error("conversations_history failed on #gam-ask-ai: %s — "
                         "bot may not be a member",
                         e.response.get("error", "?"))
            return []

        msgs = resp.get("messages", [])
        for m in msgs:
            text = m.get("text", "") or ""
            # Only messages DIRECTED AT Ramp Research
            if f"<@{RAMP_RESEARCH_ID}" not in text:
                continue
            # Strip the @mention + cleanup
            cleaned = text.replace(f"<@{RAMP_RESEARCH_ID}|Ramp Research>", "").replace(f"<@{RAMP_RESEARCH_ID}>", "").strip()
            if not cleaned:
                continue
            queries.append({
                "ts": m.get("ts", ""),
                "user": m.get("user", "?"),
                "text": cleaned[:500],  # cap to keep Claude prompt small
            })

        fetched += len(msgs)
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    logger.info("Play discovery: pulled %d research queries from last %d days",
                len(queries), LOOKBACK_DAYS)
    return queries


def _cluster_with_claude(queries: list[dict]) -> list[dict]:
    """Ask Claude to group semantically-similar queries into candidate plays.

    Returns a list of clusters: [{"theme": "...", "queries": [...], "count": N}].
    Only returns clusters of 2+.
    """
    if len(queries) < 2:
        return []

    query_lines = []
    for i, q in enumerate(queries, 1):
        query_lines.append(f"{i}. [{q['ts']}] {q['text']}")
    query_block = "\n".join(query_lines)

    system_prompt = """You are analyzing Slack messages where sales AMs ask an AI tool (Ramp Research) ad-hoc questions about their book of business. Your job is to cluster semantically-similar questions into candidate "plays" — repeatable outbound segmentations that could be automated.

Return ONLY a JSON array. Each cluster must have AT LEAST 2 queries. Format:
[
  {"theme": "Short description of the common intent", "query_indices": [1, 3, 7], "count": 3},
  ...
]

Rules:
- Only cluster if there's a clear shared intent (same type of BoB segmentation, same signal, same product focus). Don't force clusters.
- Ignore one-off troubleshooting questions, debugging queries, or follow-ups to prior answers.
- Ignore questions that are about a specific single account (not a BoB-wide segmentation).
- If no clusters with 2+ queries emerge, return an empty array [].
- Keep themes short and actionable — phrased as "Accounts that ..." or "{signal} + {product-gap}".
- Return valid JSON only, no prose around it."""

    user_prompt = f"Research queries from the last 7 days:\n\n{query_block}\n\nCluster these into candidate plays. Return the JSON array."

    try:
        response = call_claude(user_prompt, max_tokens=1500, system=system_prompt)
        # Strip any markdown fences if Claude added them
        response = response.strip()
        if response.startswith("```"):
            response = response.split("\n", 1)[1] if "\n" in response else response
            response = response.rsplit("```", 1)[0].strip()
            if response.startswith("json"):
                response = response[4:].strip()

        clusters = json.loads(response)
        if not isinstance(clusters, list):
            logger.warning("Play discovery: Claude returned non-list response")
            return []

        # Attach the actual query text for each cluster
        for c in clusters:
            indices = c.get("query_indices", [])
            c["queries"] = [queries[i - 1]["text"] for i in indices if 1 <= i <= len(queries)]
            c["count"] = len(c["queries"])
        return [c for c in clusters if c.get("count", 0) >= 2]
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logger.error("Play discovery: Claude clustering failed: %s", e)
        return []


def _format_dm(clusters: list[dict]) -> str:
    """Build the Slack DM body for the weekly digest."""
    lines = [
        ":microscope: *Weekly Play Discovery — potential new plays*",
        f"_Based on the last {LOOKBACK_DAYS} days of #gam-ask-ai research queries._",
        "",
    ]
    for c in clusters:
        theme = c.get("theme", "?")
        count = c.get("count", 0)
        lines.append(f"• *{theme}* _(seen {count}×)_")
        for q in c.get("queries", [])[:3]:
            snippet = q[:160] + ("…" if len(q) > 160 else "")
            lines.append(f"    › {snippet}")
        lines.append("")
    lines.append(
        "_If any of these feel like recurring plays worth adding to the "
        "Plays tab, reply to this DM and Greg (or a teammate) can wire them in._"
    )
    return "\n".join(lines)


def run_play_discovery(client=None, user_id=None) -> dict:
    """Scan + cluster + optionally DM. Returns a summary dict.

    client : slack_sdk.WebClient (optional — creates one from SLACK_BOT_TOKEN if None)
    user_id : Slack user to DM (defaults to GREG_SLACK_ID)
    """
    if client is None:
        client = WebClient(token=SLACK_BOT_TOKEN)
    dm_target = user_id or GREG_SLACK_ID

    queries = _fetch_research_queries(client)
    if not queries:
        return {"queries": 0, "clusters": 0, "dmed": False}

    clusters = _cluster_with_claude(queries)
    if not clusters:
        logger.info("Play discovery: no 2+ clusters — skipping DM")
        return {"queries": len(queries), "clusters": 0, "dmed": False}

    # Suppress if an identical-themed digest went out last week
    last = _load_last_digest()
    last_themes = set((last.get("clusters") or []))
    new_themes = {c.get("theme", "") for c in clusters}
    if new_themes and new_themes <= last_themes:
        logger.info("Play discovery: all %d themes identical to last digest — skipping DM", len(clusters))
        return {"queries": len(queries), "clusters": len(clusters), "dmed": False, "suppressed": True}

    body = _format_dm(clusters)
    try:
        client.chat_postMessage(channel=dm_target, text=body)
    except SlackApiError as e:
        logger.error("Play discovery DM failed: %s", e.response.get("error", "?"))
        return {"queries": len(queries), "clusters": len(clusters), "dmed": False, "error": str(e)}

    _save_last_digest({
        "ran_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "clusters": sorted(new_themes),
        "query_count": len(queries),
    })
    return {"queries": len(queries), "clusters": len(clusters), "dmed": True}
