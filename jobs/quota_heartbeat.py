"""Daily EOD quota heartbeat — 6:00 PM PT."""
import logging
from core.snowflake_client import run_query
from core.slack_formatter import quota_heartbeat_blocks, format_currency
from queries.queries import REALIZED_CP_QUERY
from utils.cp_calculator import compute_realized_cp
from config import GREG_SLACK_ID, NTR_RATES

logger = logging.getLogger(__name__)

def run_quota_heartbeat(client):
    """Run EOD quota attainment summary and DM Greg."""
    try:
        raw_df = run_query(REALIZED_CP_QUERY)
        if raw_df.empty:
            logger.info("Quota heartbeat: no realized CP data")
            return

        cp_df = compute_realized_cp(raw_df)
        if cp_df.empty:
            return

        # Summarize by product
        by_product = {}
        for _, row in cp_df.iterrows():
            product = row.get("product", "Other")
            by_product[product] = by_product.get(product, 0) + float(row.get("cp_earned", 0))

        total_cp = sum(by_product.values())
        locked_cp = float(cp_df["cp_locked"].sum()) if "cp_locked" in cp_df.columns else 0
        accruing_cp = float(cp_df["cp_accruing"].sum()) if "cp_accruing" in cp_df.columns else 0

        # Determine accelerator band
        # Placeholder quota — update when actual monthly quota is known
        monthly_quota = 40000  # approximate
        attainment_pct = (total_cp / monthly_quota * 100) if monthly_quota > 0 else 0
        if attainment_pct > 150:
            band = "2.5x"
        elif attainment_pct > 125:
            band = "1.5x"
        elif attainment_pct > 100:
            band = "1.25x"
        else:
            band = "1.0x"

        # Top movers (highest CP opps)
        top_movers = []
        if not cp_df.empty:
            sorted_df = cp_df.sort_values("cp_earned", ascending=False).head(5)
            for _, row in sorted_df.iterrows():
                top_movers.append({
                    "account": row.get("account_name", ""),
                    "product": row.get("product", ""),
                    "cp": format_currency(float(row.get("cp_earned", 0))),
                    "status": row.get("window_status", ""),
                })

        summary = {
            "total_cp": format_currency(total_cp),
            "locked_cp": format_currency(locked_cp),
            "accruing_cp": format_currency(accruing_cp),
            "attainment_pct": f"{attainment_pct:.0f}%",
            "band": band,
            "by_product": {k: format_currency(v) for k, v in by_product.items()},
            "top_movers": top_movers,
            "opp_count": len(cp_df),
        }

        blocks = quota_heartbeat_blocks(summary)

        # Append missed opps nudge from priority_actions cache
        try:
            from jobs.priority_actions import get_cached_category
            missed = get_cached_category("post_meeting_opp")
            if missed:
                missed_text = f"\U0001f3af *{len(missed)} expansion product{'s' if len(missed) != 1 else ''} discussed on calls — no opp created*"
                top_missed = missed[:3]
                for m in top_missed:
                    missed_text += f"\n  \u2022 {m.get('account', '')} — {m.get('product', '').replace(' Expansion', '')}"
                if len(missed) > 3:
                    missed_text += f"\n  _...and {len(missed) - 3} more_"
                missed_text += "\n`/priorities` \u2192 post-meeting opps to create them"
                blocks.append({"type": "divider"})
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": missed_text},
                })
        except Exception:
            pass

        client.chat_postMessage(channel=GREG_SLACK_ID, blocks=blocks, text="EOD Quota Heartbeat")
        logger.info("Quota heartbeat sent: total CP %s, attainment %s", summary["total_cp"], summary["attainment_pct"])

    except Exception as e:
        logger.error("Quota heartbeat job failed: %s", e)
