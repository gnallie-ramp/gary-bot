"""Proactive Nudge — periodic high-value suggestions throughout the day.

Runs every 2 hours during business hours (10AM, 12PM, 2PM, 4PM PT).
Compares current signals against the last nudge to detect NEW changes:
  - New activations since last check
  - Opps that crossed or are nearing baseline
  - Accounts with accelerating spend (new prospecting signals)
  - Stale opps that just crossed a staleness threshold
  - Unanswered emails detected

Surfaces the top suggestions with inline Draft Email / Create Opp buttons.
Skips sending if nothing meaningful changed since the last nudge.

Triggered by schedule or DM "nudge" / "suggestions" / "what's new".
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime

from core.slack_formatter import (
    format_currency, sf_account_url, sf_opp_url,
    dashboard_url, build_sf_new_opp_url, opp_fields_summary,
)
from config import GREG_SLACK_ID

logger = logging.getLogger(__name__)

# Track what was surfaced in the last nudge to detect changes (per-user)
_last_nudge_keys: dict[str, set[str]] = {}
_last_nudge_ts: dict[str, float] = {}

# Maximum items to show in a single nudge
_MAX_NUDGE_ITEMS = 5


def _item_key(item: dict) -> str:
    """Unique key for an action item (account_id + type + product)."""
    return f"{item.get('account_id', '')}:{item.get('type', '')}:{item.get('product', '')}"


def _pick_top_suggestions(groups: dict[str, list[dict]], is_first: bool, last_nudge_keys: set[str] | None = None) -> list[dict]:
    """Pick the highest-value items to surface, prioritizing new signals.

    Parameters
    ----------
    groups : dict
        Priority actions grouped by category.
    is_first : bool
        True if this is the first nudge of the day (no prior state).

    Returns
    -------
    list[dict]
        Top items to surface, each annotated with '_nudge_reason'.
    """
    if last_nudge_keys is None:
        last_nudge_keys = set()

    all_items = []
    for cat, items in groups.items():
        for item in items:
            item = dict(item)  # shallow copy
            item["_category"] = cat
            all_items.append(item)

    current_keys = {_item_key(item) for item in all_items}

    # Identify what's new since last nudge
    new_items = []
    continuing_items = []
    for item in all_items:
        key = _item_key(item)
        if key not in last_nudge_keys:
            item["_nudge_reason"] = "new"
            new_items.append(item)
        else:
            item["_nudge_reason"] = "continuing"
            continuing_items.append(item)

    # Sort each group by priority
    new_items.sort(key=lambda x: -x.get("priority", 0))
    continuing_items.sort(key=lambda x: -x.get("priority", 0))

    # On first nudge, just pick the highest-priority items
    if is_first:
        all_sorted = sorted(all_items, key=lambda x: -x.get("priority", 0))
        for item in all_sorted:
            item["_nudge_reason"] = "top"
        return all_sorted[:_MAX_NUDGE_ITEMS]

    # Prefer new items, then fill with high-priority continuing items
    picks = new_items[:_MAX_NUDGE_ITEMS]
    remaining = _MAX_NUDGE_ITEMS - len(picks)
    if remaining > 0:
        # Only include continuing items that are high-value (close_now, zero_to_one with CP)
        high_value_continuing = [
            i for i in continuing_items
            if i.get("_category") in ("close_now",) or i.get("est_cp", 0) > 500
        ]
        picks.extend(high_value_continuing[:remaining])

    return picks


def _build_nudge_blocks(
    picks: list[dict],
    groups: dict[str, list[dict]],
    new_count: int,
    total_count: int,
) -> list[dict]:
    """Build the proactive nudge Slack blocks."""
    now = datetime.now()
    time_str = now.strftime("%I:%M %p").lstrip("0")

    # Pick a contextual greeting based on time
    hour = now.hour
    if hour < 12:
        greeting = "Mid-morning check-in"
    elif hour < 14:
        greeting = "Midday check-in"
    elif hour < 17:
        greeting = "Afternoon check-in"
    else:
        greeting = "Late-day check-in"

    blocks = [{
        "type": "header",
        "text": {"type": "plain_text", "text": f"\U0001f4ac {greeting} — {time_str}", "emoji": True},
    }]

    # Summary line
    if new_count > 0:
        summary = f"*{new_count} new signal{'s' if new_count != 1 else ''}* since last check"
        summary += f" | {total_count} total actions across your book"
    else:
        summary = f"*{total_count} active signals* — here are the highest-value moves right now"

    total_cp = sum(
        a.get("est_cp", 0)
        for items in groups.values()
        for a in items
    )
    if total_cp > 0:
        summary += f" | ~{format_currency(total_cp)} CP at stake"

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": summary},
    })
    blocks.append({"type": "divider"})

    # Category icons
    _ICONS = {
        "close_now": "\U0001f534",
        "zero_to_one": "\u26a1",
        "post_meeting_opp": "\U0001f3af",
        "prospect": "\U0001f4c8",
        "followup": "\U0001f4e8",
        "stale": "\u23f0",
        "reopen": "\U0001f504",
    }
    _LABELS = {
        "close_now": "Close Now",
        "zero_to_one": "Zero-to-One",
        "post_meeting_opp": "Post-Meeting Opp",
        "prospect": "Prospecting",
        "followup": "Follow-up",
        "stale": "Stale Opp",
        "reopen": "Re-open",
    }

    for i, item in enumerate(picks, 1):
        cat = item.get("_category", "")
        icon = _ICONS.get(cat, "\U0001f4a1")
        label = _LABELS.get(cat, cat.replace("_", " ").title())
        reason = item.get("_nudge_reason", "")
        new_badge = " `NEW`" if reason == "new" else ""

        account_id = item.get("account_id", "")
        opp_id = item.get("opp_id", "")
        sf_link = sf_opp_url(opp_id) if opp_id else sf_account_url(account_id)
        name_link = f"<{sf_link}|{item['account']}>"
        product_str = f" — {item['product']}" if item.get("product") else ""
        cp_str = f" | ~{format_currency(item['est_cp'])} CP" if item.get("est_cp") else ""

        line = (
            f"{icon} *{name_link}*{product_str}{new_badge}\n"
            f"      {label}: {item.get('action', '')}{cp_str}"
        )

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": line},
        })

        # Inline action buttons
        buttons = []

        needs_new_opp = (
            cat == "prospect"
            or cat == "reopen"
            or cat == "post_meeting_opp"
            or (cat == "zero_to_one" and item.get("sub_type") == "zero_to_one_no_opp")
        )
        product = item.get("product", "")

        if needs_new_opp and product:
            l30d_raw = item.get("l30d_spend_raw", 0) or 0
            sf_new_url = build_sf_new_opp_url(
                account_name=item["account"],
                account_id=account_id,
                product_type=product,
                amount=l30d_raw if l30d_raw > 0 else 0,
                expansion_notes=f"L30D: {format_currency(l30d_raw)} at opp creation",
            )
            buttons.append({
                "type": "button",
                "text": {"type": "plain_text", "text": "Create Opp", "emoji": True},
                "url": sf_new_url,
                "action_id": f"create_opp_{account_id}_nudge_{i}",
                "style": "primary",
            })

        # Draft Email for actionable categories
        if cat in ("stale", "prospect", "followup", "reopen", "post_meeting_opp") or (
            cat == "zero_to_one" and item.get("sub_type") == "zero_to_one_no_opp"
        ):
            draft_payload = json.dumps({
                "account_id": account_id,
                "account": item["account"],
                "opp_id": opp_id,
                "product": product,
                "category": cat,
            })[:2000]
            btn_label = "Draft Follow-up" if cat == "followup" else "Draft Email"
            buttons.append({
                "type": "button",
                "text": {"type": "plain_text", "text": btn_label, "emoji": True},
                "action_id": f"draft_outreach_{account_id}_nudge_{i}",
                "value": draft_payload,
            })

        # "Context" button — full account deep dive
        context_payload = json.dumps({
            "account_id": account_id,
            "account": item["account"],
        })[:2000]
        buttons.append({
            "type": "button",
            "text": {"type": "plain_text", "text": "Context", "emoji": True},
            "action_id": f"account_context_{account_id}_nudge_{i}",
            "value": context_payload,
        })

        # Prep link for all
        prep_link = dashboard_url("meeting-prep", account=item["account"])
        buttons.append({
            "type": "button",
            "text": {"type": "plain_text", "text": "Prep", "emoji": True},
            "url": prep_link,
            "action_id": f"prep_{account_id}_nudge_{i}",
        })

        if buttons:
            blocks.append({"type": "actions", "elements": buttons})

    # Quick-access footer with category counts
    cat_summary_parts = []
    for cat_key in ["close_now", "zero_to_one", "post_meeting_opp", "prospect", "stale", "reopen"]:
        items = groups.get(cat_key, [])
        if items:
            label = _LABELS.get(cat_key, cat_key)
            cat_summary_parts.append(f"{label}: {len(items)}")

    blocks.append({"type": "divider"})

    footer_text = " · ".join(cat_summary_parts) if cat_summary_parts else "No other signals"
    _dash = dashboard_url("priority")
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"{footer_text}\n"
                f"DM `priorities` for full view · "
                f"`batch outreach` for campaigns · "
                f"<{_dash}|Dashboard>"
            ),
        }],
    })

    return blocks


def run_proactive_nudge(client, user_id=None, force: bool = False):
    """Check for changes and send a proactive nudge if warranted.

    Parameters
    ----------
    client : slack_sdk.WebClient
    force : bool
        When True, always send (on-demand use). Otherwise, skip if
        nothing changed since the last nudge.
    """
    dm_target = user_id or GREG_SLACK_ID

    try:
        from jobs.priority_actions import run_priority_actions, get_cached_category

        # Refresh the priority actions cache
        run_priority_actions(client, user_id=dm_target, force=True, silent=True)

        # Read all categories
        _CATEGORIES = [
            "close_now", "zero_to_one", "post_meeting_opp",
            "prospect", "followup", "stale", "reopen",
        ]
        groups = {}
        for cat in _CATEGORIES:
            items = get_cached_category(cat, user_id=dm_target)
            if items:
                groups[cat] = items

        total_items = sum(len(v) for v in groups.values())
        if total_items == 0:
            if force:
                client.chat_postMessage(
                    channel=dm_target,
                    text="All clear — no actionable signals right now. I'll check again in 2 hours.",
                )
            _last_nudge_keys[dm_target] = set()
            _last_nudge_ts[dm_target] = time.time()
            return

        # Determine what's new
        all_items = [item for items in groups.values() for item in items]
        current_keys = {_item_key(item) for item in all_items}
        user_last_keys = _last_nudge_keys.get(dm_target, set())
        user_last_ts = _last_nudge_ts.get(dm_target, 0.0)
        is_first = user_last_ts == 0.0
        new_count = len(current_keys - user_last_keys) if not is_first else 0

        # Skip if nothing changed and not forced
        if not force and not is_first and new_count == 0:
            logger.info("Proactive nudge: no new signals, skipping")
            _last_nudge_ts[dm_target] = time.time()
            return

        # Pick the best items to surface
        picks = _pick_top_suggestions(groups, is_first, last_nudge_keys=user_last_keys)
        if not picks and not force:
            logger.info("Proactive nudge: no high-value picks, skipping")
            _last_nudge_ts[dm_target] = time.time()
            return

        # Build and send
        blocks = _build_nudge_blocks(picks, groups, new_count, total_items)
        fallback = f"Check-in: {new_count} new signals, {total_items} total actions"
        client.chat_postMessage(
            channel=dm_target,
            blocks=blocks,
            text=fallback,
        )

        # Update state
        _last_nudge_keys[dm_target] = current_keys
        _last_nudge_ts[dm_target] = time.time()
        logger.info(
            "Proactive nudge sent: %d picks, %d new signals, %d total",
            len(picks), new_count, total_items,
        )

    except Exception as e:
        logger.error("Proactive nudge failed: %s", e)
        if force:
            client.chat_postMessage(
                channel=dm_target,
                text=f"Nudge check failed: {e}",
            )
