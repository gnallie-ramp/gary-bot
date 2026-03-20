"""Morning Brief — unified daily action summary at 7:45 AM PT.

Runs the full priority_actions pipeline (all 7 signal categories), then
builds a condensed morning DM showing the top items across all categories
with drill-down buttons for each. This replaces the old Close Now +
Zero-to-One only version.
"""
from __future__ import annotations

import logging
from datetime import datetime

from core.slack_formatter import format_currency, sf_account_url, sf_opp_url, dashboard_url
from config import GREG_SLACK_ID

logger = logging.getLogger(__name__)

# Category display order, icons, and labels
_CATEGORY_META = [
    ("close_now",        "\U0001f534", "Close Now"),
    ("zero_to_one",      "\u26a1",     "Zero-to-One"),
    ("post_meeting_opp", "\U0001f3af", "Post-Meeting Opps"),
    ("prospect",         "\U0001f4c8", "Prospecting"),
    ("followup",         "\U0001f4e8", "Follow-ups"),
    ("stale",            "\u23f0",     "Stale Opps"),
    ("reopen",           "\U0001f504", "Re-open"),
]

# How many top items to show inline per category
_TOP_N = 3


def _build_unified_blocks(date_str: str, groups: dict[str, list[dict]]) -> list[dict]:
    """Build the unified morning brief blocks from priority_actions groups."""
    blocks = [{
        "type": "header",
        "text": {"type": "plain_text", "text": f"\u2615 Morning Brief — {date_str}", "emoji": True},
    }]

    total_items = sum(len(v) for v in groups.values())
    if total_items == 0:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\u2705 No urgent actions today. DM me `priorities` to check later."},
        })
        return blocks

    # Overall summary line
    total_cp = sum(
        a.get("est_cp", 0)
        for items in groups.values()
        for a in items
    )
    cat_count = sum(1 for v in groups.values() if v)
    summary = f"*{total_items} actions across {cat_count} categories*"
    if total_cp > 0:
        summary += f" | ~{format_currency(total_cp)} CP at stake"
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": summary},
    })
    blocks.append({"type": "divider"})

    # Per-category: headline + top N items + drill-down button
    for cat_key, icon, label in _CATEGORY_META:
        items = groups.get(cat_key, [])
        if not items:
            continue

        cat_cp = sum(a.get("est_cp", 0) for a in items)
        cp_part = f" — ~{format_currency(cat_cp)} CP" if cat_cp > 0 else ""

        # Category header with count
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{icon} *{label}* ({len(items)}){cp_part}",
            },
        })

        # Top N items, condensed to one line each
        for i, action in enumerate(items[:_TOP_N], 1):
            account_id = action.get("account_id", "")
            opp_id = action.get("opp_id", "")
            sf_link = sf_opp_url(opp_id) if opp_id else sf_account_url(account_id)
            name_link = f"<{sf_link}|{action['account']}>"
            product_str = f" — {action['product']}" if action.get("product") else ""
            item_cp = action.get("est_cp", 0)
            cp_str = f" | ~{format_currency(item_cp)} CP" if item_cp > 0 else ""

            line = f"  {i}. {name_link}{product_str}: {action['action']}{cp_str}"
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": line},
            })

        # "Show all" button if more items exist
        remaining = len(items) - _TOP_N
        btn_text = f"Show all {label} ({len(items)})" if remaining > 0 else f"Show {label} ({len(items)})"
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": btn_text, "emoji": True},
                "action_id": f"priority_show_{cat_key}",
            }],
        })

    # Footer
    blocks.append({"type": "divider"})
    _dash = dashboard_url("priority")
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"<{_dash}|Open Dashboard> · "
                f"DM me `priorities` for full interactive view · "
                f"DM a category name to drill in"
            ),
        }],
    })

    return blocks


def run_morning_brief(client, force: bool = False):
    """Generate and send the unified morning brief.

    Runs the full priority_actions pipeline in silent mode to populate the
    cache, then builds a condensed summary from all categories.
    """
    from jobs.priority_actions import run_priority_actions, get_cached_category

    try:
        # Populate the priority_actions cache with all signal categories
        run_priority_actions(client, force=True, silent=True)

        # Read all categories from the cache
        groups = {}
        for cat_key, _, _ in _CATEGORY_META:
            items = get_cached_category(cat_key)
            if items:
                groups[cat_key] = items

        total_items = sum(len(v) for v in groups.values())
        if total_items == 0 and not force:
            logger.info("Morning brief: no actions to report")
            return

        date_str = datetime.now().strftime("%A, %b %d")
        blocks = _build_unified_blocks(date_str, groups)

        total_cp = sum(a.get("est_cp", 0) for items in groups.values() for a in items)
        fallback = f"Morning Brief: {total_items} actions, {format_currency(total_cp)} CP at stake"
        client.chat_postMessage(
            channel=GREG_SLACK_ID,
            blocks=blocks,
            text=fallback,
        )
        logger.info("Morning brief sent: %d items across %d categories", total_items, len(groups))

    except Exception as e:
        logger.error("Morning brief failed: %s", e)
