"""Post-Close CP Monitor — track activation and CP generation after close-won.

Alerts when:
  1. First product activation happens post-close (0-to-1 moment)
  2. Spend crosses baseline (CP starts generating)
  3. Spend is nearing baseline (approaching CP territory)
  4. Spend has dropped well below baseline (re-engage)

Baseline = L30D spend at close-won date (RevOps standard).
CP = (post-close spend - baseline) x NTR across three 30-day windows.
"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from core.snowflake_client import run_query
from core.slack_formatter import format_currency, sf_opp_url, sf_account_url, dashboard_url
from config import GREG_SLACK_ID, NTR_RATES, COMMAND_PREFIX
from queries.queries import REALIZED_CP_QUERY, format_query

logger = logging.getLogger(__name__)


def _safe_float(val, default=0.0):
    try:
        v = float(val)
        return default if pd.isna(v) else v
    except (TypeError, ValueError):
        return default


def _status_classify(current_l30d, baseline):
    """Classify current spend vs baseline for CP generation status."""
    if baseline <= 0:
        if current_l30d > 0:
            return "activated", "\u26a1", "First spend detected — baseline was $0"
        return "no_spend", "\u26aa", "No spend yet"

    ratio = current_l30d / baseline
    if ratio >= 1.2:
        return "exceeding", "\U0001f534", f"Exceeding baseline by {format_currency(current_l30d - baseline)} — CP generating"
    elif ratio >= 1.0:
        return "crossed", "\U0001f7e2", f"Just crossed baseline — CP started"
    elif ratio >= 0.8:
        return "nearing", "\U0001f7e1", f"{format_currency(baseline - current_l30d)} below baseline — approaching"
    elif ratio >= 0.3:
        return "below", "\U0001f7e0", f"Well below baseline — re-engage"
    elif current_l30d > 0:
        return "minimal", "\U0001f7e4", f"Minimal spend — {int(ratio * 100)}% of baseline"
    else:
        return "inactive", "\u26aa", "No spend post-close"


def run_post_close_monitor(client, user_id=None, force: bool = False):
    """Check all CW opps and send alerts for significant status changes."""
    dm_target = user_id or GREG_SLACK_ID

    try:
        df = run_query(format_query(REALIZED_CP_QUERY, user_id=user_id))
        if df.empty:
            if force:
                client.chat_postMessage(channel=dm_target, text="No closed-won opps to monitor.")
            return

        # Classify each opp
        exceeding = []   # CP generating strongly
        crossed = []     # Just crossed baseline
        nearing = []     # Close to baseline
        inactive = []    # No/minimal spend, needs re-engagement

        for _, row in df.iterrows():
            baseline = _safe_float(row.get("baseline_at_close"))
            current = _safe_float(row.get("current_l30d"))
            d1 = _safe_float(row.get("spend_d1_d30"))
            d2 = _safe_float(row.get("spend_d31_d60"))
            d3 = _safe_float(row.get("spend_d61_d90"))
            days_since_cw = int(_safe_float(row.get("days_since_cw")))
            product = row.get("expansion_subtype", "")
            ntr = NTR_RATES.get(product, 0.0095)

            status, icon, desc = _status_classify(current, baseline)

            # Compute actual CP earned so far
            cp_earned = 0
            for window_spend in [d1, d2, d3]:
                delta = max(0, window_spend - baseline)
                cp_earned += delta * ntr

            # Projected remaining CP (if pattern continues)
            remaining_windows = max(0, 3 - (days_since_cw // 30))
            projected_cp = cp_earned + max(0, current - baseline) * ntr * remaining_windows

            item = {
                "account": row.get("account_name", "Unknown"),
                "account_id": row.get("account_id", ""),
                "opp_id": row.get("opportunity_id", ""),
                "product": product,
                "status": status,
                "icon": icon,
                "desc": desc,
                "baseline": baseline,
                "current": current,
                "days_since_cw": days_since_cw,
                "cp_earned": cp_earned,
                "projected_cp": projected_cp,
                "d1": d1, "d2": d2, "d3": d3,
            }

            if status == "exceeding":
                exceeding.append(item)
            elif status == "crossed":
                crossed.append(item)
            elif status == "nearing":
                nearing.append(item)
            elif status in ("inactive", "minimal", "below"):
                inactive.append(item)

        # Only send if there's something interesting (or forced)
        total_alerts = len(exceeding) + len(crossed) + len(nearing) + len(inactive)
        if total_alerts == 0 and not force:
            logger.info("Post-close monitor: all opps on track, no alerts")
            return

        date_str = datetime.now().strftime("%b %d")
        blocks = [{
            "type": "header",
            "text": {"type": "plain_text", "text": f"\U0001f4ca Post-Close Monitor — {date_str}", "emoji": True},
        }]

        total_cp = sum(i["cp_earned"] for i in exceeding + crossed + nearing + inactive)
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{len(df)} closed-won opps tracked* | CP earned to date: *{format_currency(total_cp)}*",
            },
        })

        def _build_section(title, items, show_cp=True):
            if not items:
                return
            items.sort(key=lambda x: -x.get("cp_earned", 0))
            lines = [f"*{title} ({len(items)})*"]
            for item in items[:6]:
                sf_link = sf_opp_url(item["opp_id"])
                product_short = item["product"].replace(" Expansion", "")
                cp_str = ""
                if show_cp and item["cp_earned"] > 0:
                    cp_str = f" | CP earned: {format_currency(item['cp_earned'])}"
                lines.append(
                    f"  {item['icon']} <{sf_link}|{item['account']}> — {product_short} (CW {item['days_since_cw']}d ago)\n"
                    f"      Baseline: {format_currency(item['baseline'])} | L30D: {format_currency(item['current'])} — {item['desc']}{cp_str}"
                )
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)},
            })

        _build_section("\U0001f534 Exceeding Baseline — Close or Create Follow-up", exceeding)
        _build_section("\U0001f7e2 Just Crossed Baseline — CP Started", crossed)
        _build_section("\U0001f7e1 Nearing Baseline — Watch Closely", nearing)
        _build_section("\u26aa Inactive / Below Baseline — Re-engage", inactive, show_cp=False)

        blocks.append({"type": "divider"})
        _perf = dashboard_url("performance")
        _pipe = dashboard_url("pipeline")
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    "Baseline = L30D spend at close-won date. CP = (spend - baseline) x NTR per 30-day window.\n"
                    f"<{_perf}|Performance> · <{_pipe}|Pipeline> · `/{COMMAND_PREFIX}-post-close` to refresh"
                ),
            }],
        })

        client.chat_postMessage(
            channel=dm_target,
            blocks=blocks,
            text=f"Post-Close Monitor: {len(exceeding)} exceeding, {len(crossed)} crossed, {len(inactive)} inactive",
        )
        logger.info("Post-close monitor sent: %d exceeding, %d crossed, %d nearing, %d inactive",
                     len(exceeding), len(crossed), len(nearing), len(inactive))

    except Exception as e:
        logger.error("Post-close monitor failed: %s", e)
        if force:
            client.chat_postMessage(channel=dm_target, text=f"Post-close monitor failed: {e}")
