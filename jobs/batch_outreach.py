"""Batch Outreach — identify clusters of similar accounts and draft batch emails.

Clusters accounts by signal type (e.g., "12 accounts activated bill pay
with no opp") and generates a template with per-account personalization.

Triggered by DM "batch outreach" / "batch emails" or via priority actions.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime

import pandas as pd

from core.snowflake_client import run_query
from core.slack_formatter import format_currency, sf_account_url
from config import GREG_SLACK_ID, NTR_RATES

logger = logging.getLogger(__name__)


def _safe_float(val, default=0.0):
    try:
        v = float(val)
        return default if pd.isna(v) else v
    except (TypeError, ValueError):
        return default


def _identify_clusters(client, user_id: str = None) -> list[dict]:
    """Identify clusters of accounts with similar signals for batch outreach."""
    from jobs.priority_actions import run_priority_actions, get_cached_category
    import time
    from jobs.priority_actions import _cache_ts, _CACHE_TTL

    uid = user_id or GREG_SLACK_ID

    # Ensure cache is populated
    if time.time() - _cache_ts.get(uid, 0) > _CACHE_TTL:
        run_priority_actions(client, user_id=uid, force=True, silent=True)

    clusters = []

    # Cluster 1: Zero-to-one activations without opp, grouped by product
    z2o = get_cached_category("zero_to_one", user_id=uid)
    z2o_no_opp = [a for a in z2o if a.get("sub_type") == "zero_to_one_no_opp"]
    if z2o_no_opp:
        by_product = {}
        for item in z2o_no_opp:
            product = item.get("product", "Unknown")
            if product not in by_product:
                by_product[product] = []
            by_product[product].append(item)

        for product, items in by_product.items():
            if len(items) >= 2:
                clusters.append({
                    "type": "zero_to_one",
                    "label": f"{product.replace(' Expansion', '')} activations — no opp",
                    "description": f"{len(items)} accounts activated {product.replace(' Expansion', '')} but have no open opp",
                    "accounts": items,
                    "priority": len(items) * 10 + 50,
                    "template_context": "zero_to_one_outreach",
                })

    # Cluster 2: Post-meeting opps by product
    pm = get_cached_category("post_meeting_opp", user_id=uid)
    if pm and len(pm) >= 2:
        by_product = {}
        for item in pm:
            product = item.get("product", "Unknown")
            if product not in by_product:
                by_product[product] = []
            by_product[product].append(item)
        for product, items in by_product.items():
            if len(items) >= 2:
                clusters.append({
                    "type": "post_meeting_opp",
                    "label": f"{product.replace(' Expansion', '')} discussed — no opp",
                    "description": f"{len(items)} recent calls discussed {product.replace(' Expansion', '')} without an opp",
                    "accounts": items,
                    "priority": len(items) * 8 + 40,
                    "template_context": "post_meeting_outreach",
                })

    # Cluster 3: Stale opps by stage
    stale = get_cached_category("stale", user_id=uid)
    if stale and len(stale) >= 3:
        by_stage = {}
        for item in stale:
            stage = item.get("stage", "Unknown")
            if stage not in by_stage:
                by_stage[stage] = []
            by_stage[stage].append(item)
        for stage, items in by_stage.items():
            if len(items) >= 2:
                clusters.append({
                    "type": "stale",
                    "label": f"Stale {stage} opps",
                    "description": f"{len(items)} stale opps at {stage} — re-engage",
                    "accounts": items,
                    "priority": len(items) * 5 + 30,
                    "template_context": "stale_reengage",
                })

    # Cluster 4: Prospects (spend accelerating, no opp)
    prospect = get_cached_category("prospect", user_id=uid)
    if prospect and len(prospect) >= 2:
        clusters.append({
            "type": "prospect",
            "label": "Spend accelerating — no opp",
            "description": f"{len(prospect)} accounts with accelerating spend but no open opp",
            "accounts": prospect,
            "priority": len(prospect) * 7 + 35,
            "template_context": "prospect_outreach",
        })

    # Cluster 5: Re-open by pattern
    reopen = get_cached_category("reopen", user_id=uid)
    if reopen and len(reopen) >= 2:
        clusters.append({
            "type": "reopen",
            "label": "Post-close opps to re-open",
            "description": f"{len(reopen)} closed-won opps with patterns worth re-opening",
            "accounts": reopen,
            "priority": len(reopen) * 6 + 25,
            "template_context": "reopen_outreach",
        })

    clusters.sort(key=lambda x: -x["priority"])
    return clusters


def run_batch_outreach(client, user_id=None, force: bool = False):
    """Identify outreach clusters and present them for batch action."""
    dm_target = user_id or GREG_SLACK_ID

    try:
        clusters = _identify_clusters(client, user_id=dm_target)

        if not clusters:
            if force:
                client.chat_postMessage(
                    channel=dm_target,
                    text="No batch outreach clusters found. Run `priorities` first to populate signals.",
                )
            return

        blocks = [{
            "type": "header",
            "text": {"type": "plain_text", "text": "\U0001f4e7 Batch Outreach Campaigns", "emoji": True},
        }]

        total_accounts = sum(len(c["accounts"]) for c in clusters)
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{len(clusters)} campaign{'s' if len(clusters) != 1 else ''}* covering *{total_accounts} accounts*",
            },
        })

        for i, cluster in enumerate(clusters[:6], 1):
            acct_names = [a["account"] for a in cluster["accounts"][:5]]
            more = len(cluster["accounts"]) - 5
            names_str = ", ".join(acct_names)
            if more > 0:
                names_str += f", +{more} more"

            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{i}. {cluster['label']}* ({len(cluster['accounts'])} accounts)\n"
                        f"{cluster['description']}\n"
                        f"_{names_str}_"
                    ),
                },
            })

            # Action buttons
            payload = json.dumps({
                "cluster_idx": i - 1,
                "type": cluster["type"],
                "template_context": cluster["template_context"],
                "count": len(cluster["accounts"]),
            })[:2000]
            blocks.append({
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": f"Draft {len(cluster['accounts'])} Emails", "emoji": True},
                    "action_id": f"batch_draft_{i}",
                    "value": payload,
                    "style": "primary",
                }],
            })

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": "Clicking 'Draft' generates personalized emails for each account in the cluster. DM `batch outreach` to refresh.",
            }],
        })

        client.chat_postMessage(
            channel=dm_target,
            blocks=blocks,
            text=f"Batch Outreach: {len(clusters)} campaigns, {total_accounts} accounts",
        )
        logger.info("Batch outreach sent: %d clusters, %d total accounts", len(clusters), total_accounts)

    except Exception as e:
        logger.error("Batch outreach failed: %s", e)
        if force:
            client.chat_postMessage(channel=dm_target, text=f"Batch outreach failed: {e}")


def draft_batch_emails(cluster_type: str, template_context: str, client, user_id=None):
    """Draft emails for all accounts in a cluster.

    Called by the interactive handler when user clicks "Draft X Emails".
    Runs the smart email drafter for each account in sequence.
    """
    from jobs.priority_actions import get_cached_category

    dm_target = user_id or GREG_SLACK_ID
    items = get_cached_category(cluster_type, user_id=dm_target)
    if not items:
        client.chat_postMessage(
            channel=dm_target,
            text="Cache expired. DM `batch outreach` to refresh.",
        )
        return

    # Map template_context to category for the drafter
    _CATEGORY_MAP = {
        "zero_to_one_outreach": "zero_to_one",
        "post_meeting_outreach": "post_meeting_opp",
        "stale_reengage": "stale",
        "prospect_outreach": "prospect",
        "reopen_outreach": "reopen",
    }
    drafter_category = _CATEGORY_MAP.get(template_context, cluster_type)

    client.chat_postMessage(
        channel=dm_target,
        text=f"Drafting {len(items)} emails for *{cluster_type.replace('_', ' ')}* cluster... this will take ~{len(items) * 15} seconds.",
    )

    def _run():
        from handlers.interactive import _draft_smart_email
        from concurrent.futures import ThreadPoolExecutor, as_completed

        drafted = 0
        failed = 0

        def _draft_one(item):
            _draft_smart_email(
                account_id=item.get("account_id", ""),
                account_name=item.get("account", ""),
                opp_id=item.get("opp_id", ""),
                product=item.get("product", ""),
                category=drafter_category,
                client=client,
                user_id=dm_target,
            )
            return item.get("account", "?")

        # Run 3 drafts concurrently to speed up batch processing
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(_draft_one, item): item for item in items}
            for future in as_completed(futures):
                item = futures[future]
                try:
                    future.result()
                    drafted += 1
                except Exception as e:
                    logger.warning("Batch draft failed for %s: %s", item.get("account", "?"), e)
                    failed += 1

        summary = f"Batch drafting complete: {drafted} drafts created"
        if failed:
            summary += f", {failed} failed"
        client.chat_postMessage(channel=dm_target, text=summary)

    threading.Thread(target=_run, daemon=True).start()
