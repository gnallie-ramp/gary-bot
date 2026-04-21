"""App Home tab — tabbed layout published on app_home_opened event."""

import json
import logging
import re
import threading
import time
from datetime import datetime

from config import GREG_SLACK_ID, SF_BASE_URL, OWNER_NAME, COMMAND_PREFIX
from core.user_registry import is_registered, register_user, get_user, get_user_sf_name

logger = logging.getLogger(__name__)

# ── Module-level cache for priority alerts (10-min TTL) ──────────────────────
_priority_cache = {}  # user_id -> {"data": ..., "fetched_at": ...}
_PRIORITY_CACHE_TTL = 600  # 10 minutes

# ── Tab state per user ───────────────────────────────────────────────────────
_active_tab = {}  # user_id -> tab name
_TABS = [
    ("signals", ":rotating_light: Signals"),
    ("pipeline", ":clipboard: Pipeline"),
    ("prospecting", ":mag: Prospecting"),
    ("meetings", ":calendar: Meetings"),
    ("post_close", ":chart_with_upwards_trend: Post-Close"),
    ("renewals", ":repeat: Renewals"),
    ("drafts", ":email: Drafts"),
    ("settings", ":gear: Settings"),
]
_DEFAULT_TAB = "signals"


def _updated_at_block(epoch=None) -> dict:
    """Return a context block showing 'Updated X ago' or 'Updated just now'."""
    if epoch is None or epoch == 0:
        label = "Not yet loaded"
    else:
        delta = int(time.time() - epoch)
        if delta < 60:
            label = "Updated just now"
        elif delta < 3600:
            label = f"Updated {delta // 60}m ago"
        else:
            label = f"Updated {delta // 3600}h {(delta % 3600) // 60}m ago"
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": f"_{label}_"}]}


def register_home_tab(app):
    """Register the app_home_opened event handler."""

    @app.event("app_home_opened")
    def handle_app_home_opened(event, client):
        user_id = event.get("user")
        tab = event.get("tab")
        logger.info("app_home_opened: user=%s tab=%s", user_id, tab)
        if tab != "home":
            return

        # Show registration form for unregistered users
        if not is_registered(user_id):
            blocks = _build_registration_blocks()
            client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            return

        def _publish():
            try:
                logger.info("Home tab: building blocks for user=%s", user_id)
                blocks = _build_home_blocks(client, user_id)
                logger.info("Home tab: publishing %d blocks for user=%s", len(blocks), user_id)
                client.views_publish(
                    user_id=user_id,
                    view={"type": "home", "blocks": blocks},
                )
                logger.info("Home tab: published successfully for user=%s", user_id)
            except Exception as e:
                logger.error("Home tab publish failed for user=%s: %s", user_id, e, exc_info=True)

        threading.Thread(target=_publish, daemon=True).start()


# ── Registration screen for new users ────────────────────────────────────────

def _build_registration_blocks():
    """Build welcome screen for unregistered users."""
    return [
        {"type": "header", "text": {"type": "plain_text", "text": "Welcome to Gary Bot", "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text": (
            "*Gary* is an AI-powered sales intelligence co-pilot that monitors your book 24/5.\n\n"
            "Spend signals, pre-call briefs, post-meeting follow-ups, opp creation, email drafts — all in Slack.\n\n"
            "To get started, click the button below and fill in your details."
        )}},
        {"type": "divider"},
        {"type": "actions", "elements": [{"type": "button", "text": {"type": "plain_text", "text": "Get Started", "emoji": True}, "action_id": "open_registration_modal", "style": "primary"}]},
    ]


# ── Tab bar builder ──────────────────────────────────────────────────────────

def _build_tab_bar(active):
    """Build the tab button row. Active tab gets primary style."""
    elements = []
    for tab_id, label in _TABS:
        btn = {
            "type": "button",
            "text": {"type": "plain_text", "text": label, "emoji": True},
            "action_id": f"home_tab_switch_{tab_id}",
        }
        if tab_id == active:
            btn["style"] = "primary"
        elements.append(btn)
    # Slack actions block supports max 25 elements (we have 6)
    return {"type": "actions", "elements": elements}


def _build_home_blocks_header(active_tab):
    """Build just the header + tab bar for instant tab switch feedback."""
    blocks = []
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": "Gary — Sales Intelligence Co-Pilot", "emoji": True},
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            "AI-powered signals, briefs, opps, and drafts — all in Slack. "
            "Gary monitors your book 24/5."
        )}],
    })
    blocks.append(_build_tab_bar(active_tab))
    blocks.append({"type": "divider"})
    return blocks


# ── Main router ──────────────────────────────────────────────────────────────

def _build_home_blocks(client, user_id):
    """Build the Home tab blocks — routes to active tab's builder."""
    active = _active_tab.get(user_id, _DEFAULT_TAB)
    # Migrate legacy tab names from before consolidation
    if active == "dashboard":
        active = "signals"
        _active_tab[user_id] = active
    # Legacy "stale" tab folds into Pipeline tab with a Staleness ≥15d filter preset
    if active == "stale":
        active = "pipeline"
        _active_tab[user_id] = active
        _pipeline_state.setdefault(user_id, _pipeline_default_state())["filters"]["staleness"] = "15+"
    # Trials folded into Prospecting as a sub-section
    if active == "trials":
        active = "prospecting"
        _active_tab[user_id] = active
    # Instructions no longer a tab — content available via /gary-help
    if active == "instructions":
        active = "signals"
        _active_tab[user_id] = active

    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": "Gary — Sales Intelligence Co-Pilot", "emoji": True},
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            "AI-powered signals, briefs, opps, and drafts — all in Slack. "
            "Gary monitors your book 24/5."
        )}],
    })

    # Tab bar
    blocks.append(_build_tab_bar(active))
    blocks.append({"type": "divider"})

    # Tab content
    tab_builders = {
        "signals": _build_signals_tab,
        "pipeline": _build_pipeline_tab,
        "stale": _build_stale_tab,  # kept for direct callers; tab bar no longer surfaces it
        "prospecting": _build_prospecting_tab,
        "meetings": _build_meetings_tab,
        "post_close": _build_post_close_tab,
        "renewals": _build_renewals_tab,
        "trials": _build_trials_tab,
        "drafts": _build_drafts_tab,
        "instructions": _build_instructions_tab,
        "settings": _build_settings_tab,
    }
    builder = tab_builders.get(active, _build_signals_tab)
    tab_blocks = builder(client, user_id)
    blocks.extend(tab_blocks)

    # Footer
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            f"Gary Bot v2 • Built with Claude Code • "
            f"`/{COMMAND_PREFIX}-help` for all commands • "
            f"DM me anything"
        )}],
    })

    # Slack 100-block limit safety
    if len(blocks) > 100:
        blocks = blocks[:99]
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "_Some content truncated — use slash commands for full detail_"}],
        })

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Signals (unified — replaces Dashboard + Pipeline)
# ══════════════════════════════════════════════════════════════════════════════

def _build_signals_tab(client, user_id):
    """Signals: Unified feed with quota, spend signals, non-spend signals, activations, and quick actions."""
    blocks = []

    # Quota Snapshot
    quota_blocks = _get_quota_snapshot(user_id=user_id)
    if quota_blocks:
        blocks.extend(quota_blocks)
        blocks.append({"type": "divider"})

    # Spend Signals (all groups, full density)
    alert_blocks = _get_priority_alerts(user_id, max_per_group=5, max_groups=9)
    if alert_blocks:
        blocks.extend(alert_blocks)
        blocks.append(_updated_at_block(_priority_cache.get(user_id, {}).get("fetched_at", 0)))
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"_No active priority signals right now. Check back later or run_ `/{COMMAND_PREFIX}-priorities`"},
        })

    # Non-spend signals (stale, reopen, post-meeting, underperforming)
    nonsignal_blocks = _get_non_spend_signals(user_id=user_id)
    if nonsignal_blocks:
        blocks.append({"type": "divider"})
        blocks.extend(nonsignal_blocks)

    # Activation Alerts (recent treasury, investment, first bill milestones)
    activation_blocks = _get_activation_alerts_section(user_id)
    if activation_blocks:
        blocks.append({"type": "divider"})
        blocks.extend(activation_blocks)

    # Quick Actions
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:zap: Quick Actions*"},
    })
    blocks.append({
        "type": "actions",
        "elements": [
            {"type": "button", "text": {"type": "plain_text", "text": ":dart: Priorities", "emoji": True}, "action_id": "home_run_priorities"},
            {"type": "button", "text": {"type": "plain_text", "text": ":chart_with_upwards_trend: Quota", "emoji": True}, "action_id": "home_run_quota"},
            {"type": "button", "text": {"type": "plain_text", "text": ":sunrise: Morning Brief", "emoji": True}, "action_id": "home_run_morning"},
            {"type": "button", "text": {"type": "plain_text", "text": ":loudspeaker: Nudge", "emoji": True}, "action_id": "home_run_nudge"},
        ],
    })
    blocks.append({
        "type": "actions",
        "elements": [
            {"type": "button", "text": {"type": "plain_text", "text": ":money_with_wings: Spend Pacing", "emoji": True}, "action_id": "home_run_spend"},
            {"type": "button", "text": {"type": "plain_text", "text": ":broom: Pipeline Cleanup", "emoji": True}, "action_id": "home_run_cleanup"},
            {"type": "button", "text": {"type": "plain_text", "text": ":rocket: Zero-to-One", "emoji": True}, "action_id": "home_run_zero_to_one"},
            {"type": "button", "text": {"type": "plain_text", "text": ":crystal_ball: Forecast", "emoji": True}, "action_id": "home_run_forecast"},
        ],
    })

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Stale Opps
# ══════════════════════════════════════════════════════════════════════════════

# Per-user sort preference for stale tab
_stale_sort = {}  # user_id -> "cp" | "staleness"


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Pipeline — all open opps (Expansion + Renewal) with engagement context,
# sort/filter/pagination, and per-opp actions. Replaces standalone Stale Opps tab.
# ══════════════════════════════════════════════════════════════════════════════

_pipeline_state = {}  # user_id -> {"sort": str, "filters": dict, "page": int}
_pipeline_cache = {}  # user_id -> {"data": DataFrame, "fetched_at": epoch}
_PIPELINE_CACHE_TTL = 600  # 10 min
_PIPELINE_PAGE_SIZE = 10

# Per-user per-opp pending SFDC update proposals, cleared after Apply/Dismiss.
# Shape: {user_id: {opp_id: {"proposals": {...}, "current": {...}, "rationale": str,
#                            "account_name": str, "product": str, "message_ts": str,
#                            "channel": str}}}
_pending_sfdc_updates = {}

_PRODUCT_FILTERS = ["Card", "Bill Pay", "Treasury", "Travel", "SaaS", "SaaS Add-On", "Procurement"]
_STAGE_FILTERS = ["S1", "S2", "S3", "S4"]
_TYPE_FILTERS = ["Expansion", "Renewal"]
_STALENESS_FILTERS = [("all", "All"), ("7+", "≥7d"), ("15+", "≥15d"), ("30+", "≥30d"), ("90+", "≥90d")]
_SORT_OPTIONS = [
    ("cp", ":moneybag: CP"),
    ("touch", ":alarm_clock: Last Touch"),
    ("close", ":calendar: Close Date"),
    ("stage", ":chart_with_upwards_trend: Stage"),
]


def _pipeline_default_state():
    return {
        "sort": "cp",
        "filters": {"products": [], "stages": [], "types": [], "staleness": "all"},
        "page": 1,
    }


def _get_pipeline_state(user_id):
    if user_id not in _pipeline_state:
        _pipeline_state[user_id] = _pipeline_default_state()
    return _pipeline_state[user_id]


def _fetch_pipeline_data(user_id):
    """Fetch all accounts with open opps via ALL_PIPELINE_ACCOUNTS_QUERY, cache 10 min.

    Parses the `opps` JSON array from Snowflake into a Python list so the
    render/filter code can work with real lists.
    """
    import time as _time
    import json as _json
    from core.snowflake_client import run_query
    from queries.queries import ALL_PIPELINE_ACCOUNTS_QUERY, format_query

    now = _time.time()
    cached = _pipeline_cache.get(user_id)
    if cached and (now - cached["fetched_at"]) < _PIPELINE_CACHE_TTL:
        return cached["data"]

    try:
        df = run_query(format_query(ALL_PIPELINE_ACCOUNTS_QUERY, user_id=user_id))
        df.columns = [c.lower() for c in df.columns]
        # Snowflake returns ARRAY_AGG as a JSON string — parse into list[dict]
        if "opps" in df.columns:
            df["opps"] = df["opps"].apply(
                lambda s: _json.loads(s) if isinstance(s, str) else (s or [])
            )
        _pipeline_cache[user_id] = {"data": df, "fetched_at": now}
        return df
    except Exception as e:
        logger.warning("Pipeline data fetch failed: %s", e, exc_info=True)
        return None


def _pipeline_sort_key(row, sort_mode):
    """Account-level sort key. Higher tuple = top of list (we sort desc).

    Account rows have: total_est_cp (sum), soonest_close_date, opps (list).
    For stage sort we take the MAX stage-rank across the account's opps
    (further-along deals get priority).
    """
    from datetime import date
    if sort_mode == "cp":
        return (float(row.get("total_est_cp") or 0),)
    if sort_mode == "touch":
        days = row.get("days_since_touch")
        return (-1 if days is None or str(days) == "nan" else float(days),)
    if sort_mode == "close":
        cd = row.get("soonest_close_date")
        if cd is None:
            return (0,)
        try:
            # Negate so smaller (sooner) close date = higher sort key in desc order
            return (-(cd - date(1970, 1, 1)).days,)
        except Exception:
            return (0,)
    if sort_mode == "stage":
        stage_rank = {"S1": 1, "S2": 2, "S3": 3, "S4": 4}
        opps = row.get("opps") or []
        max_rank = max(
            (stage_rank.get((o.get("stage") or "").split(":")[0].strip(), 0) for o in opps),
            default=0,
        )
        return (max_rank, float(row.get("total_est_cp") or 0))
    return (float(row.get("total_est_cp") or 0),)


def _pipeline_apply_filters(df, filters):
    """Account-level filter: include an account if it has ≥1 opp matching ALL
    active chip-group constraints (product AND stage AND type), plus staleness.
    Uses the pipe-delimited string columns for fast membership checks.
    """
    import pandas as pd
    f = df.copy()

    products = filters.get("products") or []
    if products:
        def _has_product(pipe_str):
            tokens = [p.strip() for p in (pipe_str or "").split("|") if p.strip()]
            return any(p in products for p in tokens)
        f = f[f["opp_products_pipe"].apply(_has_product)]

    stages = filters.get("stages") or []
    if stages:
        def _has_stage(pipe_str):
            tokens = [(s or "").split(":")[0].strip() for s in (pipe_str or "").split("|")]
            return any(s in stages for s in tokens)
        f = f[f["opp_stages_pipe"].apply(_has_stage)]

    types_ = filters.get("types") or []
    if types_:
        def _has_type(pipe_str):
            tokens = [t.strip() for t in (pipe_str or "").split("|") if t.strip()]
            return any(t in types_ for t in tokens)
        f = f[f["opp_types_pipe"].apply(_has_type)]

    stale = filters.get("staleness", "all")
    if stale != "all":
        days_req = int(stale.rstrip("+"))
        f = f[f["days_since_touch"].apply(
            lambda d: d is not None and not pd.isna(d) and float(d) >= days_req
        )]

    return f


def _pipeline_build_filter_row(label, key, options, active_values, include_all=False):
    """Build a multi-select chip row for filters. Active chips get primary style."""
    elements = []
    if include_all:
        all_active = not active_values
        btn = {
            "type": "button",
            "text": {"type": "plain_text", "text": "All", "emoji": True},
            "action_id": f"pipeline_filter_{key}_all",
        }
        if all_active:
            btn["style"] = "primary"
        elements.append(btn)
    for opt in options:
        if isinstance(opt, tuple):
            val, disp = opt
        else:
            val, disp = opt, opt
        active = val in active_values if isinstance(active_values, list) else val == active_values
        btn = {
            "type": "button",
            "text": {"type": "plain_text", "text": disp, "emoji": True},
            "action_id": f"pipeline_filter_{key}_{val}".replace(" ", "_"),
        }
        if active:
            btn["style"] = "primary"
        elements.append(btn)
    return {"type": "actions", "elements": elements[:25]}  # Slack cap


def _pipeline_touch_line(days_since, last_email_direction):
    """Staleness dot + plain-English line about last touch."""
    if days_since is None or str(days_since) == "nan":
        return ":white_circle: No call or email on file"
    d = int(days_since)
    if d < 7:
        dot = ":large_green_circle:"
    elif d < 15:
        dot = ":large_yellow_circle:"
    elif d < 30:
        dot = ":large_orange_circle:"
    elif d < 60:
        dot = ":red_circle:"
    else:
        dot = ":red_circle:"  # same red past 30d — depth of red via label
    base = f"{dot} Last touch: {d}d ago"
    dirn = (last_email_direction or "").lower()
    if dirn == "outbound":
        base += " · _last email sent by you, no reply_"
    elif dirn == "inbound":
        base += " · _they replied — your move_"
    return base


def _build_pipeline_tab(client, user_id):
    """Pipeline: one card per account with open opps, all engagement context
    consolidated at account level. Sort/filter/paginate across accounts.
    """
    import pandas as pd
    from utils.snooze import is_snoozed

    state = _get_pipeline_state(user_id)
    blocks = []

    # Header
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:clipboard: Pipeline — Your Open Opps*"},
    })

    df = _fetch_pipeline_data(user_id)
    if df is None:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":warning: Could not load pipeline data. Check logs or retry."},
        })
        return blocks
    if df.empty:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_No accounts with open expansion or renewal opps found in your book._"},
        })
        return blocks

    total_accounts = len(df)
    total_opps = int(df["open_opp_count"].sum())
    # Expansion/renewal counts: sum of opp_types_pipe tokens across all accounts
    def _count_type_in_pipe(pipe_str, t):
        return sum(1 for tok in (pipe_str or "").split("|") if tok == t)
    exp_count = int(df["opp_types_pipe"].apply(lambda s: _count_type_in_pipe(s, "Expansion")).sum())
    ren_count = int(df["opp_types_pipe"].apply(lambda s: _count_type_in_pipe(s, "Renewal")).sum())

    # Apply filters
    filtered = _pipeline_apply_filters(df, state["filters"])

    # Apply sort (desc on the chosen key)
    if not filtered.empty:
        filtered = filtered.assign(_sortkey=filtered.apply(
            lambda r: _pipeline_sort_key(r, state["sort"]), axis=1))
        filtered = filtered.sort_values("_sortkey", ascending=False).drop(columns=["_sortkey"])

    # Filter out snoozed accounts. Snooze key pattern: "acct:<account_id>".
    before_snooze = len(filtered)
    filtered = filtered[~filtered["account_id"].apply(
        lambda aid: is_snoozed(f"acct:{aid}") if aid else False
    )]
    snoozed_count = before_snooze - len(filtered)

    total_filtered = len(filtered)

    # Pagination
    page = state.get("page", 1)
    total_pages = max(1, (total_filtered + _PIPELINE_PAGE_SIZE - 1) // _PIPELINE_PAGE_SIZE)
    page = max(1, min(page, total_pages))
    state["page"] = page
    start = (page - 1) * _PIPELINE_PAGE_SIZE
    end = start + _PIPELINE_PAGE_SIZE
    page_rows = filtered.iloc[start:end]

    # Subtitle with counts
    subtitle_bits = [
        f"*{total_accounts}* accounts · *{total_opps}* open opps ({exp_count} Expansion · {ren_count} Renewal)",
    ]
    if total_filtered != total_accounts:
        subtitle_bits.append(f"*{total_filtered}* accounts after filters")
    if snoozed_count:
        subtitle_bits.append(f"{snoozed_count} snoozed hidden")
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": " · ".join(subtitle_bits)}],
    })

    # Sort row
    sort_elements = []
    for sort_id, label in _SORT_OPTIONS:
        btn = {
            "type": "button",
            "text": {"type": "plain_text", "text": label, "emoji": True},
            "action_id": f"pipeline_sort_{sort_id}",
        }
        if state["sort"] == sort_id:
            btn["style"] = "primary"
        sort_elements.append(btn)
    blocks.append({"type": "actions", "elements": sort_elements})

    # Filter rows
    blocks.append(_pipeline_build_filter_row(
        "Product", "product", _PRODUCT_FILTERS, state["filters"]["products"]))
    blocks.append(_pipeline_build_filter_row(
        "Stage", "stage", _STAGE_FILTERS, state["filters"]["stages"]))
    blocks.append(_pipeline_build_filter_row(
        "Type", "type", _TYPE_FILTERS, state["filters"]["types"]))
    blocks.append(_pipeline_build_filter_row(
        "Staleness", "staleness", _STALENESS_FILTERS, state["filters"]["staleness"]))

    # Results context
    showing_from = start + 1 if total_filtered else 0
    showing_to = min(end, total_filtered)
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn",
                      "text": f"Showing {showing_from}–{showing_to} of {total_filtered} · page {page}/{total_pages}"}],
    })
    blocks.append({"type": "divider"})

    if page_rows.empty:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_No accounts match the current filters. Try clearing filters._"},
        })
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": ":arrows_counterclockwise: Reset filters", "emoji": True},
                "action_id": "pipeline_reset_filters",
            }],
        })
        return blocks

    # ── AI summary cold-start / staleness: kick off background generation for
    # any visible account that doesn't have a fresh cached summary. The render
    # uses the template fallback now; the cards show real summaries on next
    # refresh (~30s for 10 accounts).
    try:
        from jobs.opp_context_summarizer import is_cache_fresh, batch_generate, _generation_in_flight
        stale_rows = []
        for _, r in page_rows.iterrows():
            aid = r.get("account_id")
            if not aid or aid in _generation_in_flight:
                continue
            payload = {
                "last_call_date": r.get("last_call_date"),
                "last_email_date": r.get("last_email_date"),
                "opps": r.get("opps") or [],
            }
            if not is_cache_fresh(aid, payload):
                stale_rows.append(r.to_dict())

        if stale_rows:
            # Banner telling the user summaries are generating
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": (
                    f":sparkles: _Generating AI summaries for {len(stale_rows)} "
                    f"account{'s' if len(stale_rows) != 1 else ''} in background — "
                    f"refresh in ~30s to see them._"
                )}],
            })
            # Mark in-flight so concurrent renders don't re-trigger
            for r in stale_rows:
                _generation_in_flight.add(r["account_id"])

            def _run_batch():
                try:
                    batch_generate(stale_rows)
                except Exception as e:
                    logger.warning("Pipeline summary batch failed: %s", e)
                finally:
                    for r in stale_rows:
                        _generation_in_flight.discard(r["account_id"])
            threading.Thread(target=_run_batch, daemon=True).start()
    except Exception as e:
        logger.debug("AI summary batch trigger failed: %s", e)

    # Render account cards (one card per account, listing its open opps inside)
    for _, row in page_rows.iterrows():
        blocks.extend(_pipeline_render_card(row))

    # Pagination controls
    blocks.append({"type": "divider"})
    page_elements = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": ":arrow_left: Prev", "emoji": True},
            "action_id": "pipeline_page_prev",
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Next :arrow_right:", "emoji": True},
            "action_id": "pipeline_page_next",
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": ":arrows_counterclockwise: Reset filters", "emoji": True},
            "action_id": "pipeline_reset_filters",
        },
    ]
    blocks.append({"type": "actions", "elements": page_elements})

    return blocks


def _pipeline_render_card(row):
    """Render a single account with its open opps as nested bullets.

    Blocks: one section with the account header + opp list + engagement context,
    plus one actions row with account-scoped buttons (Draft, Propose, Snooze, SFDC).
    """
    import pandas as pd
    import json as _json

    acct_name = row.get("account_name") or "Unknown"
    acct_id = row.get("account_id", "")
    total_cp = row.get("total_est_cp") or 0
    open_count = row.get("open_opp_count") or 0
    soonest_close = row.get("soonest_close_date")
    opps = row.get("opps") or []

    # Account-level type icon: purple if ANY renewal, else blue
    has_renewal = any((o.get("type") == "Renewal") for o in opps)
    has_expansion = any((o.get("type") == "Expansion") for o in opps)
    if has_renewal and has_expansion:
        type_icon = ":large_blue_circle:"  # mixed — use blue for primary expansion
    elif has_renewal:
        type_icon = ":large_purple_circle:"
    else:
        type_icon = ":large_blue_circle:"

    acct_sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
    acct_str = f"<{acct_sf_link}|{acct_name}>" if acct_sf_link else acct_name

    # Header line
    opp_word = "opp" if open_count == 1 else "opps"
    header_bits = [f"{type_icon} *{acct_str}* · {open_count} open {opp_word}"]
    if total_cp and float(total_cp) > 0:
        header_bits.append(f"Total CP potential: ~{_fmt_currency(float(total_cp))}")
    if soonest_close is not None and not pd.isna(soonest_close):
        header_bits.append(f"soonest close: {soonest_close}")
    header = " · ".join(header_bits)

    # Engagement touch line
    touch = _pipeline_touch_line(row.get("days_since_touch"), row.get("last_email_direction"))
    last_email_contact = row.get("last_email_contact") or ""
    if last_email_contact and row.get("days_since_touch") is not None:
        touch += f" · _w/ {last_email_contact}_"

    # Per-opp bullet list (compact)
    opp_lines = ["*Open opps:*"]
    for opp in opps:
        raw_product = opp.get("product")
        opp_type = opp.get("type") or ""
        # Label: prefer specific product. Fall back to type name (e.g. "Renewal")
        # when product is null so we don't render an ugly "—".
        if raw_product:
            label = f"*{raw_product}*"
            if opp_type == "Renewal":
                label += " _Renewal_"
        else:
            label = f"*{opp_type}*" if opp_type else "*Unknown*"
        stage = (opp.get("stage") or "").split(":")[0].strip() or "?"
        close = opp.get("close_date") or ""
        monthly = opp.get("monthly_amount") or 0
        est_cp = opp.get("est_cp") or 0
        next_step = (opp.get("next_step") or "").strip()

        line_parts = [f"• {label}"]
        if monthly and float(monthly) > 0:
            line_parts.append(f"{_fmt_currency(float(monthly))}/mo")
        line_parts.append(stage)
        if close:
            line_parts.append(f"closes {close}")
        if est_cp and float(est_cp) > 0:
            line_parts.append(f"~{_fmt_currency(float(est_cp))} CP")
        opp_line = " · ".join(line_parts)
        opp_lines.append(opp_line)
        if next_step and next_step.lower() not in ("none", "null"):
            ns_display = next_step[:100] + ("…" if len(next_step) > 100 else "")
            opp_lines.append(f"   └ Next: {ns_display}")

    # Account-level context: prefer AI-synthesized summary (Phase 2a) when
    # cached, fall back to raw Gong call summary when not cached yet.
    context_lines = []
    try:
        from jobs.opp_context_summarizer import get_cached_summary, is_cache_fresh
        acct_id_for_cache = row.get("account_id")
        cached = get_cached_summary(acct_id_for_cache) if acct_id_for_cache else None
        if cached and cached.get("summary"):
            fresh = is_cache_fresh(acct_id_for_cache, {
                "last_call_date": row.get("last_call_date"),
                "last_email_date": row.get("last_email_date"),
                "opps": row.get("opps") or [],
            })
            freshness_tag = "" if fresh else "  _(stale — refreshing)_"
            context_lines.append(f":sparkles: {cached['summary']}{freshness_tag}")
    except Exception:
        pass

    # If no AI summary yet, show the raw call summary (template fallback)
    if not context_lines:
        call_title = row.get("last_call_title")
        call_date = row.get("last_call_date")
        call_summary = (row.get("last_call_summary") or "").strip()
        if call_title and call_date is not None and not pd.isna(call_date) and call_summary:
            if len(call_summary) > 400:
                cut = call_summary[:400]
                last_period = cut.rfind(". ", 300, 400)
                call_summary = (cut[:last_period + 1] if last_period > 0 else cut + "…")
            context_lines.append(
                f":telephone_receiver: _{call_date} — {call_title}_: {call_summary}\n"
                f"_:hourglass_flowing_sand: AI summary generating — refresh in ~30s_"
            )

    body_parts = [header, touch, "\n".join(opp_lines)]
    if context_lines:
        body_parts.append("\n".join(context_lines))
    card_text = "\n".join(body_parts)

    card_block = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": card_text},
    }

    # Account-scoped action payload — carries the full opp list so handlers can
    # iterate across all opps on the account (e.g., unified re-engagement email).
    action_payload = _json.dumps({
        "account_id": acct_id,
        "account_name": acct_name,
        "opps": [
            {
                "opp_id": o.get("opp_id"),
                "product": o.get("product"),
                "type": o.get("type"),
                "stage": o.get("stage"),
                "next_step": o.get("next_step"),
            }
            for o in opps
        ],
    })

    # Short button id stem (Slack caps at 255 chars — account_id is 18)
    short_id = (acct_id or "unknown")[-12:]

    elements = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": ":email: Draft Re-engage", "emoji": True},
            "action_id": f"pipeline_draft_{short_id}",
            "value": action_payload,
            "style": "primary",
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": ":arrows_counterclockwise: Propose Updates", "emoji": True},
            "action_id": f"pipeline_propose_{short_id}",
            "value": action_payload,
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": ":alarm_clock: Snooze 14d", "emoji": True},
            "action_id": f"pipeline_snooze_{short_id}",
            "value": action_payload,
        },
    ]
    if acct_sf_link:
        elements.append({
            "type": "button",
            "text": {"type": "plain_text", "text": ":link: Account", "emoji": True},
            "url": acct_sf_link,
            "action_id": f"pipeline_sfdc_{short_id}",
        })

    return [card_block, {"type": "actions", "elements": elements[:25]}]


def _fmt_currency(val):
    """Format a number as $X,XXX or $X.XK."""
    if val is None or val <= 0:
        return "$0"
    if val >= 1000:
        return f"${val:,.0f}"
    return f"${val:,.0f}"


def _build_stale_tab(client, user_id):
    """Stale Opps: Rich cards for opps needing re-engagement, sorted by CP or staleness."""
    from jobs.priority_actions import get_cached_category, _gather_stale_opps, _cached_actions

    blocks = []
    items = get_cached_category("stale", user_id=user_id)

    # If cache is empty, populate it directly
    if not items:
        try:
            stale_items = _gather_stale_opps(user_id=user_id)
            if stale_items:
                uid = user_id or "default"
                if uid not in _cached_actions:
                    _cached_actions[uid] = {}
                _cached_actions[uid]["stale"] = stale_items
                items = stale_items
        except Exception as e:
            logger.warning("Stale tab: failed to gather stale opps: %s", e)

    # Header
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:alarm_clock: Stale Opps — Re-Engage*"},
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            "Opps with no meeting or email activity in 15+ days. "
            "Sorted by estimated CP value. Draft an email to get them back on the calendar."
        )}],
    })

    if not items:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_No stale opps right now — your pipeline is active._"},
        })
        return blocks

    # Sort toggle
    sort_mode = _stale_sort.get(user_id, "cp")
    if sort_mode == "staleness":
        items = sorted(items, key=lambda x: -x.get("days_since_touch", 0))
        sort_label = "Sorted by staleness"
        toggle_label = "Sort by CP"
        toggle_value = "cp"
    else:
        # Default: already sorted by priority (CP-weighted) from priority_actions
        sort_label = "Sorted by est. CP"
        toggle_label = "Sort by Staleness"
        toggle_value = "staleness"

    blocks.append({
        "type": "actions",
        "elements": [{
            "type": "button",
            "text": {"type": "plain_text", "text": f":arrows_counterclockwise: {toggle_label}", "emoji": True},
            "action_id": f"stale_sort_{toggle_value}",
        }],
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"_{sort_label} · {len(items)} opp{'s' if len(items) != 1 else ''}_"}],
    })

    # Render opp cards — one section block per opp with accessory Draft button
    _stale_btn_counter = [0]
    for item in items:
        _stale_btn_counter[0] += 1
        acct_name = item.get("account", "Unknown")
        acct_id = item.get("account_id", "")
        opp_id = item.get("opp_id", "")
        product = str(item.get("product", "")).replace(" Expansion", "")
        stage = item.get("stage", "")
        days_stale = item.get("days_since_touch", 0)
        est_cp = item.get("est_cp", 0)
        expansion_amount = item.get("expansion_amount", 0)
        baseline = item.get("_baseline", 0)
        recent = item.get("_recent", 0)
        call_summary = item.get("_call_summary", "")
        last_call_name = item.get("_last_call_name", "")
        last_call_date = item.get("_last_call_date", "")
        last_email_subj = item.get("_last_email_subj", "")
        last_email_date = item.get("last_email_date", "")
        last_email_direction = item.get("_last_email_direction", "")
        product_requests = item.get("_product_requests", "")
        competitors = item.get("_competitors", "")
        contacts = item.get("_contacts", [])
        activation_status = item.get("activation_status", "") if "activation_status" in item else ""

        # SFDC link
        sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
        acct_str = f"<{sf_link}|{acct_name}>" if sf_link else acct_name

        # Build card text
        lines = [f"*{acct_str}*  —  {product}"]

        # Stage + staleness + amount + CP line
        meta = f"{stage} · {days_stale}d stale"
        if expansion_amount > 0:
            meta += f" · {_fmt_currency(expansion_amount)}/mo"
        if est_cp > 0:
            meta += f" · ~{_fmt_currency(est_cp)} CP"
        lines.append(meta)

        # Key contacts
        if contacts:
            contact_parts = []
            for c in contacts[:2]:
                name = c.get("name", "")
                title = c.get("title", "")
                if title:
                    contact_parts.append(f"{name} ({title})")
                else:
                    contact_parts.append(name)
            lines.append(f":bust_in_silhouette: {', '.join(contact_parts)}")

        # ── Spend activity context (natural language) ──
        _STATUS_ICONS = {
            "No spend yet": "\u26aa", "Very low": "\U0001f7e4",
            "Below baseline": "\U0001f7e1", "Near baseline": "\U0001f7e2",
            "Exceeding baseline": "\U0001f534",
        }
        if activation_status:
            icon = _STATUS_ICONS.get(activation_status, "")
            if activation_status == "No spend yet":
                spend_ctx = f"{icon} *No {product.lower()} spend since opp created.* Baseline: {_fmt_currency(baseline)}"
            elif activation_status == "Very low":
                spend_ctx = f"{icon} *Minimal activity* — spending {_fmt_currency(recent)}/mo vs {_fmt_currency(baseline)} baseline (pre-opp)"
            elif activation_status == "Below baseline":
                spend_ctx = f"{icon} *Below baseline* — {_fmt_currency(recent)}/mo vs {_fmt_currency(baseline)} baseline. Usage hasn't ramped yet."
            elif activation_status == "Near baseline":
                spend_ctx = f"{icon} *Near baseline* — {_fmt_currency(recent)}/mo vs {_fmt_currency(baseline)}. Usage is steady but hasn't grown."
            elif activation_status == "Exceeding baseline":
                delta = recent - baseline
                spend_ctx = f"{icon} *Spend exceeding baseline by {_fmt_currency(delta)}/mo* — {_fmt_currency(recent)} vs {_fmt_currency(baseline)}. *Close ASAP to capture CP.*"
            else:
                spend_ctx = f"Baseline: {_fmt_currency(baseline)} | L30D: {_fmt_currency(recent)}"
            lines.append(spend_ctx)

        # ── Last meeting context ──
        if last_call_name and last_call_date:
            lines.append(f":telephone_receiver: Last meeting: _{last_call_name}_ ({last_call_date})")
            if call_summary:
                summary_short = call_summary[:200] + "..." if len(call_summary) > 200 else call_summary
                lines.append(f"   _{summary_short}_")

        # ── Email comms status (natural language) ──
        if last_email_subj and last_email_date and last_email_date != "2000-01-01":
            subj_short = last_email_subj[:50] + "..." if len(last_email_subj) > 50 else last_email_subj
            if last_email_direction == "outbound":
                lines.append(f":email: Last outreach: \"{subj_short}\" ({last_email_date}) — _no reply yet_")
            elif last_email_direction == "inbound":
                lines.append(f":email: They replied: \"{subj_short}\" ({last_email_date}) — _needs follow-up_")
            else:
                lines.append(f":email: Last email: \"{subj_short}\" ({last_email_date})")
        elif days_stale > 30:
            lines.append(":email: _No email history found — cold outreach needed_")

        # ── Competitors / product requests ──
        if competitors:
            lines.append(f":crossed_swords: Competitors: {competitors}")
        if product_requests:
            req_short = product_requests[:100] + "..." if len(product_requests) > 100 else product_requests
            lines.append(f":bulb: Asked about: {req_short}")

        # ── Draft preview: what happens when you click Draft ──
        lines.append("")  # spacer

        # Thread / recipient preview
        primary_contact = contacts[0] if contacts else None
        primary_name = primary_contact["name"] if primary_contact else ""
        primary_title = f" ({primary_contact['title']})" if primary_contact and primary_contact.get("title") else ""

        if last_email_subj and last_email_date and last_email_date != "2000-01-01":
            subj_preview = last_email_subj[:45] + "..." if len(last_email_subj) > 45 else last_email_subj
            if primary_name:
                lines.append(f":outbox_tray: *Draft will:* Reply to \"_{subj_preview}_\" → {primary_name}{primary_title}")
            else:
                lines.append(f":outbox_tray: *Draft will:* Reply to \"_{subj_preview}_\"")
        else:
            if primary_name:
                lines.append(f":outbox_tray: *Draft will:* Start new thread → {primary_name}{primary_title}")
            else:
                lines.append(":outbox_tray: *Draft will:* Start new thread (contact TBD from SFDC)")
        if contacts and len(contacts) > 1:
            cc_name = contacts[1]["name"]
            cc_title = f" ({contacts[1]['title']})" if contacts[1].get("title") else ""
            lines.append(f"   CC: {cc_name}{cc_title}")

        # Email content preview — what the email will say
        email_preview_parts = []
        if call_summary and last_call_name:
            # Summarize last meeting context for the preview
            summary_snippet = call_summary[:120].rstrip()
            if len(call_summary) > 120:
                # Cut at last space
                summary_snippet = summary_snippet[:summary_snippet.rfind(" ")] if " " in summary_snippet else summary_snippet
            email_preview_parts.append(f"Reference your last call (_{last_call_name}_)")
            if product_requests:
                req_short = product_requests[:80]
                if len(product_requests) > 80:
                    req_short = req_short[:req_short.rfind(" ")] if " " in req_short else req_short
                email_preview_parts.append(f"mention their interest in: {req_short}")
        elif last_email_subj and last_email_direction == "outbound":
            email_preview_parts.append("follow up on your last outreach")
        elif last_email_subj and last_email_direction == "inbound":
            email_preview_parts.append("respond to their last reply")

        # Objective
        if activation_status == "Exceeding baseline":
            email_preview_parts.append("push to close — spend already above baseline")
        elif activation_status == "No spend yet":
            email_preview_parts.append("drive first activation and get on a call")
        else:
            email_preview_parts.append("get back on the calendar to push the deal forward")

        if email_preview_parts:
            lines.append(f":pencil: *Email will:* {' → '.join(email_preview_parts)}")

        card_text = "\n".join(lines)
        # Slack text block limit: 3000 chars
        if len(card_text) > 2900:
            card_text = card_text[:2900] + "..."

        # Draft button payload
        payload = json.dumps({
            "account": acct_name,
            "account_id": acct_id,
            "opp_id": opp_id,
            "product": product,
            "category": "stale",
        })

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": card_text},
        })
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":envelope: Draft", "emoji": True},
                    "action_id": f"draft_outreach_stale_{_stale_btn_counter[0]}",
                    "value": payload,
                    "style": "primary",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":zzz: Snooze 7d", "emoji": True},
                    "action_id": f"snooze_stale_{_stale_btn_counter[0]}",
                    "value": json.dumps({"opp_id": opp_id, "account": acct_name, "days": 7}),
                },
            ],
        })

        # Divider between cards (skip after last)
        if _stale_btn_counter[0] < len(items):
            blocks.append({"type": "divider"})

    blocks.append(_updated_at_block(_priority_cache.get(user_id, {}).get("fetched_at", 0)))

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Prospecting
# ══════════════════════════════════════════════════════════════════════════════

# Per-user filter state for prospecting tab
_prospect_filter = {}  # user_id -> signal_key or "all"

# Per-user expand state for Plays sub-section (set of expanded play_ids).
# Collapsed by default; click a play row to expand + see the first 10 accounts.
_plays_expanded = {}  # user_id -> set(play_id)


def _render_play_card(play_id: str, play_meta: dict, row: dict) -> dict:
    """Render one account under a play as a Slack section block w/ overflow menu."""
    acct_id = row.get("account_id", "") or ""
    acct_name = row.get("account_name") or row.get("account") or "Unknown"
    sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
    acct_str = f"<{sf_link}|{acct_name}>" if sf_link else acct_name

    # Build the 1-line "why this account" description from the play's per-row formatter
    try:
        why = play_meta["sort_description_fn"](row)
    except Exception:
        why = ""

    tier = row.get("subscription_tier") or ""
    tier_badge = f"  `{tier}`" if tier else ""

    text = f"*{acct_str}*{tier_badge}\n_{why}_"

    payload = {"play_id": play_id, "account_id": acct_id, "account": acct_name}
    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": text},
        "accessory": {
            "type": "overflow",
            "action_id": f"plays_overflow_{play_id}_{acct_id}"[:150],
            "options": [
                {"text": {"type": "plain_text", "text": ":email: Draft Re-engage Email"},
                 "value": json.dumps({**payload, "op": "draft"})},
                {"text": {"type": "plain_text", "text": ":zzz: Snooze 30d"},
                 "value": json.dumps({**payload, "op": "snooze"})},
                {"text": {"type": "plain_text", "text": ":bust_in_silhouette: View in SFDC"},
                 "value": json.dumps({**payload, "op": "view"})},
            ],
        },
    }


def _build_plays_section(user_id) -> list:
    """Plays sub-section — renders at the top of the Prospecting tab.

    Reads from the file-based plays cache (on-demand refresh if stale). Each
    play is collapsed by default; click to expand and see up to 10 account
    cards with Draft/Snooze/View overflow menus.
    """
    from jobs.plays_refresh import get_or_refresh_play, get_cached_play
    from queries.plays import PLAYS
    from utils.snooze import get_snoozed_play_accounts

    blocks = []
    expanded = _plays_expanded.get(user_id, set())

    # Compute total accounts across all plays (after play-level snooze filter)
    total = 0
    per_play_data = {}
    for play_id, meta in PLAYS.items():
        entry = get_or_refresh_play(play_id, user_id=user_id or "")
        snoozed = get_snoozed_play_accounts(play_id, user_id=user_id or "")
        rows = [r for r in (entry or {}).get("rows", []) if r.get("account_id") not in snoozed]
        per_play_data[play_id] = (entry, rows)
        total += len(rows)

    # Header
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*:test_tube: Plays — {total} accounts across {len(PLAYS)} signals*"},
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            "Curated outbound signals. Accounts appear under every play they qualify for. "
            "Click a play to expand. Draft creates a contextual re-engagement email."
        )}],
    })
    blocks.append({"type": "divider"})

    # Per-play rows
    for play_id, meta in PLAYS.items():
        entry, rows = per_play_data[play_id]
        count = len(rows)
        is_expanded = play_id in expanded
        fetched_at = (entry or {}).get("fetched_at")
        age_txt = ""
        if fetched_at:
            age_sec = time.time() - float(fetched_at)
            if age_sec < 3600:
                age_txt = f"_refreshed {int(age_sec / 60)}m ago_"
            else:
                age_txt = f"_refreshed {int(age_sec / 3600)}h ago_"

        caret = ":arrow_down:" if is_expanded else ":arrow_right:"
        header_text = (
            f"{meta['icon']} *{meta['title']}* — {count} accounts  {caret}\n"
            f"_{meta['criteria']}_"
        )
        if age_txt:
            header_text += f"  ·  {age_txt}"

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": header_text},
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": "Collapse" if is_expanded else "Expand"},
                "action_id": f"plays_toggle_{play_id}",
                "value": play_id,
            },
        })

        if is_expanded and count > 0:
            for row in rows[:10]:
                blocks.append(_render_play_card(play_id, meta, row))
            if count > 10:
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"_Showing 10 of {count} — snooze or draft to work through the list._"}],
                })
        elif is_expanded and count == 0:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "_No accounts match this play right now._"},
            })

        blocks.append({"type": "divider"})

    return blocks


def _format_cp(v):
    try:
        return f"${float(v):,.0f}"
    except (TypeError, ValueError):
        return "$0"


def _render_top_cp_card(item: dict, rank: int) -> dict:
    """Render a single Top-CP account section block."""
    acct_id = item.get("account_id", "")
    acct_name = item.get("account", "Unknown")
    sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
    acct_str = f"<{sf_link}|{acct_name}>" if sf_link else acct_name

    tier = item.get("tier") or "—"
    fte = item.get("fte_size") or "—"
    status = item.get("account_status") or "—"
    onboarding = item.get("onboarding_status") or "—"
    cp = _format_cp(item.get("cp_potential"))

    signals = []
    if item.get("can_send_international_payments"):
        signals.append("Int'l enabled")
    if item.get("wise_onboarded_at"):
        signals.append("Wise onboarded")
    industry = item.get("industry")
    if industry:
        signals.append(industry)

    lines = [
        f"*#{rank} {acct_str}*  `{tier}` · {fte} FTE · {status}",
        f":money_with_wings: *{cp}* CP potential · onboarding: _{onboarding}_",
    ]
    if signals:
        lines.append("_" + " · ".join(signals) + "_")

    payload = {"account": acct_name, "account_id": acct_id}
    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        "accessory": {
            "type": "overflow",
            "action_id": f"top_cp_overflow_{acct_id}",
            "options": [
                {"text": {"type": "plain_text", "text": ":email: Draft Re-engage Email"},
                 "value": json.dumps({**payload, "op": "draft"})},
                {"text": {"type": "plain_text", "text": ":zzz: Snooze 30d"},
                 "value": json.dumps({**payload, "op": "snooze"})},
                {"text": {"type": "plain_text", "text": ":bust_in_silhouette: View in SFDC"},
                 "value": json.dumps({**payload, "op": "view"})},
            ],
        },
    }


def _build_top_cp_section(user_id) -> list:
    """Top-CP Re-engage section — top 20 active + top 10 churned by CP potential."""
    from jobs.top_cp_refresh import get_cached_top_cp, gather_top_cp_accounts
    from utils.snooze import get_snoozed_accounts

    blocks = []
    data = get_cached_top_cp(user_id)
    if not data.get("active") and not data.get("churned"):
        try:
            data = gather_top_cp_accounts(user_id=user_id)
        except Exception as e:
            logger.warning("Top-CP section: failed to gather: %s", e)

    snoozed = get_snoozed_accounts(user_id=user_id)
    active = [a for a in data.get("active", []) if a.get("account_id") not in snoozed]
    churned = [a for a in data.get("churned", []) if a.get("account_id") not in snoozed]

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:moneybag: Top-CP Re-engage — your biggest upside*"},
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            "Top 20 active accounts by CP potential (excludes any CW expansion in last 90 days). "
            "Refreshed daily at 6 AM PT."
        )}],
    })

    if not active and not churned:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_Top-CP list is empty — refresh hasn't run yet or no candidates._"},
        })
    else:
        for i, item in enumerate(active, start=1):
            blocks.append(_render_top_cp_card(item, i))

        if churned:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*:ghost: Top Churned Accounts ({len(churned)})*  — win-back candidates"},
            })
            for i, item in enumerate(churned, start=1):
                blocks.append(_render_top_cp_card(item, i))

    blocks.append({"type": "divider"})
    return blocks


def _build_prospecting_tab(client, user_id):
    """Prospecting: Top-CP Re-engage + Accounts matching hot signal plays, not contacted in 30+ days."""
    from jobs.prospecting_signals import (
        gather_prospecting_signals, get_cached_prospects,
        SIGNAL_META, MIN_DAYS_UNTOUCHED,
    )

    blocks = []

    # Plays section — curated BoB signals at the top, collapsed by default
    try:
        blocks.extend(_build_plays_section(user_id))
    except Exception as e:
        logger.warning("Plays section failed: %s", e)

    # Top-CP Re-engage section
    try:
        blocks.extend(_build_top_cp_section(user_id))
    except Exception as e:
        logger.warning("Top-CP section failed: %s", e)

    # Try cache first, fall back to live query
    items = get_cached_prospects(user_id=user_id)
    if not items:
        try:
            items = gather_prospecting_signals(user_id=user_id)
        except Exception as e:
            logger.warning("Prospecting tab: failed to gather signals: %s", e)

    # Header
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:mag: Prospecting — Untouched Signal Plays*"},
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            f"Accounts matching hot signals with no outbound email or call in {MIN_DAYS_UNTOUCHED}+ days. "
            "Click Draft to create a contextual outreach email."
        )}],
    })

    if not items:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_No untouched signal matches right now — check back after the next refresh._"},
        })
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": ":arrows_counterclockwise: Refresh Now", "emoji": True},
                "action_id": "prospect_refresh",
            }],
        })
        return blocks

    # Signal filter bar — count by signal type
    from collections import Counter
    signal_counts = Counter(i["signal_key"] for i in items)
    active_filter = _prospect_filter.get(user_id, "all")

    filter_buttons = [{
        "type": "button",
        "text": {"type": "plain_text", "text": f"All ({len(items)})", "emoji": True},
        "action_id": "prospect_filter_all",
        "style": "primary" if active_filter == "all" else None,
    }]
    # Remove None style (Slack doesn't accept it)
    if filter_buttons[0].get("style") is None:
        del filter_buttons[0]["style"]

    for key, count in signal_counts.most_common():
        meta = SIGNAL_META.get(key, {})
        emoji = meta.get("emoji", ":mag:")
        label = meta.get("label", key)
        btn = {
            "type": "button",
            "text": {"type": "plain_text", "text": f"{emoji} {label} ({count})", "emoji": True},
            "action_id": f"prospect_filter_{key}",
        }
        if active_filter == key:
            btn["style"] = "primary"
        filter_buttons.append(btn)

    # Slack max 25 elements per actions block — split if needed
    for i in range(0, len(filter_buttons), 5):
        blocks.append({"type": "actions", "elements": filter_buttons[i:i+5]})

    # Refresh button
    blocks.append({
        "type": "actions",
        "elements": [{
            "type": "button",
            "text": {"type": "plain_text", "text": ":arrows_counterclockwise: Refresh", "emoji": True},
            "action_id": "prospect_refresh",
        }],
    })

    # Apply filter
    if active_filter != "all":
        filtered = [i for i in items if i["signal_key"] == active_filter]
    else:
        filtered = items

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"_{len(filtered)} account{'s' if len(filtered) != 1 else ''} shown_"}],
    })

    # Render account cards — limit to 25 to stay under Slack's 100-block cap
    btn_counter = 0
    for item in filtered[:25]:
        btn_counter += 1
        acct_name = item.get("account", "Unknown")
        acct_id = item.get("account_id", "")
        signal_key = item.get("signal_key", "")
        signal_label = item.get("signal_label", "")
        signal_detail = item.get("signal_detail", "")
        days = item.get("days_since_touch", 0)
        has_opp = item.get("has_open_opp", False)
        card_l30d = item.get("card_spend_l30d", 0)
        bp_l30d = item.get("billpay_spend_l30d", 0)
        competitor_card = item.get("competitor_card_spend", 0)
        competitor_card_name = item.get("competitor_card_name", "")
        off_ramp_bp = item.get("off_ramp_bp_spend", 0)
        bp_competitor_name = item.get("bp_competitor_name", "")
        unmanaged_travel = item.get("unmanaged_travel_spend", 0)
        ae_est_card = item.get("ae_est_card_spend", 0)
        ae_est_bp = item.get("ae_est_bp_spend", 0)
        meta = SIGNAL_META.get(signal_key, {})
        emoji = meta.get("emoji", ":mag:")

        # SFDC link
        sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
        acct_str = f"<{sf_link}|{acct_name}>" if sf_link else acct_name

        # Build card text
        lines = [f"*{acct_str}*"]
        lines.append(f"{emoji} {signal_label}")
        if signal_detail:
            lines.append(f"_{signal_detail}_")

        # Spend context — signal-specific to show relevant metrics
        _ACTIVATION_KEYS = {"new_treasury", "new_investment", "first_bill"}
        spend_parts = []
        if signal_key in _ACTIVATION_KEYS:
            # Activation signals: show balances and milestone-specific data
            treasury_bal = item.get("treasury_balance", 0) or 0
            inv_bal = item.get("investment_balance", 0) or 0
            gla = item.get("current_gla", 0) or 0
            if signal_key == "new_treasury" and treasury_bal > 0:
                spend_parts.append(f"Treasury balance: ${treasury_bal:,.0f}")
            if signal_key == "new_investment" and inv_bal > 0:
                spend_parts.append(f"Investment balance: ${inv_bal:,.0f}")
            if card_l30d > 0:
                spend_parts.append(f"Card L30D: ${card_l30d:,.0f}")
            if bp_l30d > 0:
                spend_parts.append(f"BP L30D: ${bp_l30d:,.0f}")
            if gla > 0:
                spend_parts.append(f"GLA: ${gla:,.0f}")
        elif signal_key == "erp_no_billpay":
            # Show competitor/off-ramp BP spend (not Ramp BP — that contradicts "no bill pay")
            if off_ramp_bp > 0:
                bp_comp_label = f" ({bp_competitor_name})" if bp_competitor_name else ""
                spend_parts.append(f"Off-Ramp BP: ${off_ramp_bp:,.0f}/mo{bp_comp_label}")
            if card_l30d > 0:
                spend_parts.append(f"Card L30D: ${card_l30d:,.0f}")
        elif signal_key == "high_competitor_spend":
            # Show competitor breakdown
            if competitor_card > 0:
                cc_label = f" ({competitor_card_name})" if competitor_card_name else ""
                spend_parts.append(f"Competitor Card: ${competitor_card:,.0f}/mo{cc_label}")
            if off_ramp_bp > 0:
                bp_comp_label = f" ({bp_competitor_name})" if bp_competitor_name else ""
                spend_parts.append(f"Off-Ramp BP: ${off_ramp_bp:,.0f}/mo{bp_comp_label}")
            if unmanaged_travel > 0:
                spend_parts.append(f"Unmanaged Travel: ${unmanaged_travel:,.0f}/mo")
        else:
            # Default: show Ramp spend
            if card_l30d > 0:
                spend_parts.append(f"Card L30D: ${card_l30d:,.0f}")
            if bp_l30d > 0:
                spend_parts.append(f"BP L30D: ${bp_l30d:,.0f}")
        if spend_parts:
            lines.append(" \u00b7 ".join(spend_parts))

        # AE estimates + off-ramp BP (skip for activation signals — they show their own context above)
        if signal_key not in _ACTIVATION_KEYS:
            est_parts = []
            if ae_est_card > 0:
                est_parts.append(f"AE Est Card: ${ae_est_card:,.0f}/mo")
            if ae_est_bp > 0:
                est_parts.append(f"AE Est BP: ${ae_est_bp:,.0f}/mo")
            # Show off-ramp BP on signals that don't already show it above
            if off_ramp_bp > 0 and signal_key not in ("erp_no_billpay", "high_competitor_spend"):
                bp_comp_label = f" ({bp_competitor_name})" if bp_competitor_name else ""
                est_parts.append(f"Off-Ramp BP: ${off_ramp_bp:,.0f}/mo{bp_comp_label}")
            if est_parts:
                lines.append(" \u00b7 ".join(est_parts))

        # Plus status + touch + opp status
        plus_status = item.get("plus_status", "")
        plus_label = plus_status.title() if plus_status else "No Plus"
        status_parts = [f"Plus: {plus_label}"]
        if signal_key in _ACTIVATION_KEYS:
            # For activations, "days since touch" is 0 (just activated) — show champion instead
            champion = item.get("champion_name", "")
            if champion:
                status_parts.append(f"Champion: {champion}")
        else:
            status_parts.append(f"{days}d since last touch")
        if has_opp:
            status_parts.append("has open opp")
        lines.append(" \u00b7 ".join(status_parts))

        card_text = "\n".join(lines)
        if len(card_text) > 2900:
            card_text = card_text[:2900] + "..."

        # Draft button payload — uses existing smart drafter
        payload = json.dumps({
            "account": acct_name,
            "account_id": acct_id,
            "product": "",
            "category": f"prospect_{signal_key}",
        })

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": card_text},
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": ":envelope: Draft", "emoji": True},
                "action_id": f"draft_outreach_prospect_{btn_counter}",
                "value": payload,
            },
        })

        if btn_counter < len(filtered[:25]):
            blocks.append({"type": "divider"})

    if len(filtered) > 25:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"_Showing 25 of {len(filtered)} — use `/prospects` for full list_"}],
        })

    blocks.append(_updated_at_block(
        _prospect_cache_ts(user_id)
    ))

    # ── Trials sub-section (folded in from former Trials tab) ─────────────
    try:
        trial_blocks = _build_trials_tab(client, user_id)
        if trial_blocks:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*:test_tube: Procurement Trials*"},
            })
            # Skip the first block if it's just the Trials tab header (avoid double headers)
            skip_first = (
                trial_blocks
                and trial_blocks[0].get("type") == "section"
                and "Trials" in str(trial_blocks[0].get("text", {}).get("text", ""))
            )
            blocks.extend(trial_blocks[1:] if skip_first else trial_blocks)
    except Exception as e:
        logger.debug("Trials sub-section render failed: %s", e)

    return blocks


def _prospect_cache_ts(user_id):
    """Get the timestamp of the prospecting cache for display."""
    try:
        from jobs.prospecting_signals import _prospect_cache
        uid = user_id or "default"
        entry = _prospect_cache.get(uid)
        return entry["fetched_at"] if entry else 0
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Meetings
# ══════════════════════════════════════════════════════════════════════════════

def _build_meetings_tab(client, user_id):
    """Meetings: Today's calendar with Brief and Post-Meeting buttons."""
    blocks = []

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:calendar: Today's Customer Meetings*"},
    })

    meetings_blocks = _get_todays_meetings(user_id)
    if meetings_blocks:
        blocks.extend(meetings_blocks)
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_No customer meetings today_"},
        })
    blocks.append(_updated_at_block(time.time()))

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Post-Close
# ══════════════════════════════════════════════════════════════════════════════

# Per-user cache for post-close data (10-min TTL)
_post_close_cache = {}  # user_id -> {"data": [...], "fetched_at": float}
_POST_CLOSE_CACHE_TTL = 600


def _build_post_close_tab(client, user_id):
    """Post-Close: Track activation and CP generation on recently closed-won opps."""
    from core.snowflake_client import run_query
    from core.slack_formatter import format_currency, sf_opp_url, sf_account_url
    from queries.queries import REALIZED_CP_QUERY, format_query
    from config import NTR_RATES
    import pandas as pd

    blocks = []

    # Header
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:chart_with_upwards_trend: Post-Close Activation Tracker*"},
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            "Track spend and CP generation on recently closed-won opps. "
            "90-day window: D30 = 30% SOW, D60 = 60%, D90 = 90%."
        )}],
    })

    # Check cache
    uid = user_id or "default"
    now = time.time()
    cached = _post_close_cache.get(uid)
    items = None
    if cached and (now - cached["fetched_at"]) < _POST_CLOSE_CACHE_TTL:
        items = cached["data"]

    if items is None:
        try:
            df = run_query(format_query(REALIZED_CP_QUERY, user_id=user_id))
            if df.empty:
                items = []
            else:
                items = []
                for _, row in df.iterrows():
                    days_since_cw = int(float(row.get("days_since_cw", 0) or 0))
                    if days_since_cw > 90:
                        continue

                    baseline = float(row.get("baseline_at_close", 0) or 0)
                    current = float(row.get("current_l30d", 0) or 0)
                    d1 = float(row.get("spend_d1_d30", 0) or 0)
                    d2 = float(row.get("spend_d31_d60", 0) or 0)
                    d3 = float(row.get("spend_d61_d90", 0) or 0)
                    product = row.get("expansion_subtype", "")
                    ntr = NTR_RATES.get(product, 0.0095)

                    # Classify status
                    if baseline <= 0:
                        if current > 0:
                            status, icon = "activated", "\u26a1"
                        else:
                            status, icon = "inactive", "\u26aa"
                    else:
                        ratio = current / baseline
                        if ratio >= 1.2:
                            status, icon = "exceeding", "\U0001f534"
                        elif ratio >= 1.0:
                            status, icon = "crossed", "\U0001f7e2"
                        elif ratio >= 0.8:
                            status, icon = "nearing", "\U0001f7e1"
                        elif ratio >= 0.3:
                            status, icon = "below", "\U0001f7e0"
                        elif current > 0:
                            status, icon = "minimal", "\U0001f7e4"
                        else:
                            status, icon = "inactive", "\u26aa"

                    # CP earned so far
                    cp_earned = 0
                    for window_spend in [d1, d2, d3]:
                        delta = max(0, window_spend - baseline)
                        cp_earned += delta * ntr

                    # Projected remaining CP
                    remaining_windows = max(0, 3 - (days_since_cw // 30))
                    projected_cp = cp_earned + max(0, current - baseline) * ntr * remaining_windows

                    items.append({
                        "account": row.get("account_name", "Unknown"),
                        "account_id": row.get("account_id", ""),
                        "opp_id": row.get("opportunity_id", ""),
                        "opp_name": row.get("opportunity_name", ""),
                        "product": product,
                        "cw_date": str(row.get("cw_date", "")),
                        "days_since_cw": days_since_cw,
                        "baseline": baseline,
                        "current": current,
                        "d1": d1, "d2": d2, "d3": d3,
                        "status": status,
                        "icon": icon,
                        "cp_earned": cp_earned,
                        "projected_cp": projected_cp,
                        "ntr": ntr,
                    })

                # Sort: underperforming first (action needed), then by projected CP
                _STATUS_SORT = {"inactive": 0, "minimal": 1, "below": 2, "nearing": 3,
                                "activated": 4, "crossed": 5, "exceeding": 6}
                items.sort(key=lambda x: (_STATUS_SORT.get(x["status"], 5), -x["projected_cp"]))

            _post_close_cache[uid] = {"data": items, "fetched_at": now}
        except Exception as e:
            logger.warning("Post-Close tab: query failed: %s", e)
            items = []

    if not items:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_No recently closed-won opps in the last 90 days._"},
        })
        return blocks

    # Summary counts
    exceeding_ct = sum(1 for i in items if i["status"] in ("exceeding", "crossed"))
    needs_attn_ct = sum(1 for i in items if i["status"] in ("inactive", "minimal", "below"))
    total_cp = sum(i["cp_earned"] for i in items)
    total_projected = sum(i["projected_cp"] for i in items)

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            f":white_check_mark: *{exceeding_ct}* generating CP  |  "
            f":warning: *{needs_attn_ct}* need attention  |  "
            f"CP earned: *{_fmt_currency(total_cp)}*  |  "
            f"Projected: *{_fmt_currency(total_projected)}*"
        )}],
    })
    blocks.append({"type": "divider"})

    # Render each opp card
    btn_counter = 0
    for item in items:
        btn_counter += 1
        acct_id = item["account_id"]
        opp_id = item["opp_id"]
        product = str(item["product"]).replace(" Expansion", "")
        days = item["days_since_cw"]
        baseline = item["baseline"]
        current = item["current"]
        icon = item["icon"]
        status = item["status"]

        # SFDC links
        sf_acct = f"<{SF_BASE_URL}/r/Account/{acct_id}/view|{item['account']}>" if acct_id else item["account"]
        sf_opp = f"<{SF_BASE_URL}/r/Opportunity/{opp_id}/view|View Opp>" if opp_id else ""

        # Build card text
        lines = [f"*{sf_acct}*  —  {product}  |  D{days}"]

        # Status line
        if status == "exceeding":
            delta = current - baseline
            lines.append(f"{icon} *Exceeding baseline by {_fmt_currency(delta)}/mo* — CP generating")
        elif status == "crossed":
            lines.append(f"{icon} *Just crossed baseline* — CP started")
        elif status == "nearing":
            gap = baseline - current
            lines.append(f"{icon} *{_fmt_currency(gap)} below baseline* — approaching")
        elif status == "below":
            lines.append(f"{icon} *Well below baseline* — re-engage")
        elif status == "minimal":
            pct = int(current / baseline * 100) if baseline > 0 else 0
            lines.append(f"{icon} *Minimal spend* — {pct}% of baseline")
        elif status == "activated":
            lines.append(f"{icon} *First spend detected* — baseline was $0")
        else:
            lines.append(f"{icon} *No spend post-close*")

        # Spend detail
        lines.append(
            f"Baseline: {_fmt_currency(baseline)} | Current L30D: {_fmt_currency(current)}"
        )

        # Window breakdown
        window_parts = []
        if days >= 1:
            window_parts.append(f"D1-30: {_fmt_currency(item['d1'])}")
        if days >= 31:
            window_parts.append(f"D31-60: {_fmt_currency(item['d2'])}")
        if days >= 61:
            window_parts.append(f"D61-90: {_fmt_currency(item['d3'])}")
        if window_parts:
            lines.append(" | ".join(window_parts))

        # CP
        cp_parts = []
        if item["cp_earned"] > 0:
            cp_parts.append(f"CP earned: {_fmt_currency(item['cp_earned'])}")
        if item["projected_cp"] > item["cp_earned"]:
            cp_parts.append(f"Projected: {_fmt_currency(item['projected_cp'])}")
        if cp_parts:
            lines.append(" | ".join(cp_parts))

        if sf_opp:
            lines.append(sf_opp)

        card_text = "\n".join(lines)
        if len(card_text) > 2900:
            card_text = card_text[:2900] + "..."

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": card_text},
        })

        # Draft button for underperforming opps
        if status in ("inactive", "minimal", "below"):
            checkpoint = "underperforming_d30" if days <= 45 else "underperforming_d60"
            payload = json.dumps({
                "account": item["account"],
                "account_id": acct_id,
                "opp_id": opp_id,
                "product": product,
                "category": checkpoint,
            })
            blocks.append({
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":envelope: Draft Re-Engage", "emoji": True},
                    "action_id": f"draft_outreach_{checkpoint}_{btn_counter}",
                    "value": payload,
                    "style": "primary",
                }],
            })

        # Divider between cards
        if btn_counter < len(items):
            blocks.append({"type": "divider"})

    blocks.append(_updated_at_block(_post_close_cache.get(uid, {}).get("fetched_at", 0)))

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Renewals (Spiff Tracker)
# ══════════════════════════════════════════════════════════════════════════════

_renewals_cache = {}  # user_id -> {"data": [...], "fetched_at": float}
_RENEWALS_CACHE_TTL = 600


def _build_renewals_tab(client, user_id):
    """Renewals: Track accounts with Plus renewals in the ±30 day spiff window."""
    from core.snowflake_client import run_query
    from queries.queries import RENEWAL_WINDOW_QUERY, format_query
    from config import NTR_RATES

    blocks = []

    # Header
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:repeat: Renewal Spiff Tracker*"},
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            "Accounts with a Plus renewal within ±30 days. "
            "SQLs opened and closed in this window qualify for spiff credit. "
            "Accounts without open expansion opps are your top targets."
        )}],
    })

    # Check cache
    uid = user_id or "default"
    now = time.time()
    cached = _renewals_cache.get(uid)
    items = None
    if cached and (now - cached["fetched_at"]) < _RENEWALS_CACHE_TTL:
        items = cached["data"]

    if items is None:
        try:
            df = run_query(format_query(RENEWAL_WINDOW_QUERY, user_id=user_id))
            if df.empty:
                items = []
            else:
                items = []
                for _, row in df.iterrows():
                    days = int(float(row.get("days_until_renewal", 0) or 0))
                    card = float(row.get("card_l30d", 0) or 0)
                    bill = float(row.get("bill_l30d", 0) or 0)
                    travel = float(row.get("travel_l30d", 0) or 0)
                    treasury = float(row.get("treasury_avg", 0) or 0)
                    open_opps = str(row.get("open_opps", "") or "")
                    recent_cw = str(row.get("recent_cw_opps", "") or "")
                    last_email = str(row.get("last_email_date", "") or "")
                    last_call = str(row.get("last_call_date", "") or "")

                    # Determine status for sorting
                    if recent_cw:
                        status = "captured"  # Already have CW'd opps in window
                    elif open_opps:
                        status = "in_progress"  # Have open opps, working on it
                    else:
                        status = "untapped"  # No opps — action needed

                    # Identify products with spend but no opp
                    product_suggestions = []
                    opp_products = (open_opps + " " + recent_cw).lower()
                    if card > 5000 and "card" not in opp_products:
                        product_suggestions.append(f"Card ({_fmt_currency(card)}/mo)")
                    if bill > 5000 and "bill pay" not in opp_products:
                        product_suggestions.append(f"Bill Pay ({_fmt_currency(bill)}/mo)")
                    if travel > 2000 and "travel" not in opp_products:
                        product_suggestions.append(f"Travel ({_fmt_currency(travel)}/mo)")
                    if treasury > 50000 and "treasury" not in opp_products:
                        product_suggestions.append(f"Treasury ({_fmt_currency(treasury)} avg)")

                    items.append({
                        "account": row.get("account_name", "Unknown"),
                        "account_id": row.get("account_id", ""),
                        "renewal_opp_id": row.get("renewal_opp_id", ""),
                        "renewal_date": str(row.get("renewal_date", "")),
                        "renewal_stage": row.get("renewal_stage", ""),
                        "days_until": days,
                        "card": card, "bill": bill, "travel": travel, "treasury": treasury,
                        "open_opps": open_opps,
                        "recent_cw": recent_cw,
                        "last_email": last_email,
                        "last_call": last_call,
                        "status": status,
                        "suggestions": product_suggestions,
                    })

                # Sort: untapped first (action needed), then in_progress, then captured
                _STATUS_SORT = {"untapped": 0, "in_progress": 1, "captured": 2}
                items.sort(key=lambda x: (_STATUS_SORT.get(x["status"], 1), x["days_until"]))

            _renewals_cache[uid] = {"data": items, "fetched_at": now}
        except Exception as e:
            logger.warning("Renewals tab: query failed: %s", e)
            items = []

    if not items:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_No accounts with renewals in the ±30 day window right now._"},
        })
        return blocks

    # Summary counts
    untapped = sum(1 for i in items if i["status"] == "untapped")
    in_progress = sum(1 for i in items if i["status"] == "in_progress")
    captured = sum(1 for i in items if i["status"] == "captured")

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            f":red_circle: *{untapped}* untapped (no open SQL)  |  "
            f":large_yellow_circle: *{in_progress}* in progress  |  "
            f":white_check_mark: *{captured}* captured  |  "
            f"*{len(items)}* total in window"
        )}],
    })
    blocks.append({"type": "divider"})

    # Render account cards
    btn_counter = 0
    for item in items:
        btn_counter += 1
        acct_id = item["account_id"]
        days = item["days_until"]
        status = item["status"]

        # Status icon
        if status == "untapped":
            icon = ":red_circle:"
        elif status == "in_progress":
            icon = ":large_yellow_circle:"
        else:
            icon = ":white_check_mark:"

        # SFDC link
        sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
        acct_str = f"<{sf_link}|{item['account']}>" if sf_link else item["account"]

        # Renewal timing
        if days > 0:
            timing = f"Renews in {days}d ({item['renewal_date']})"
        elif days == 0:
            timing = f"Renews *today* ({item['renewal_date']})"
        else:
            timing = f"Renewed {abs(days)}d ago ({item['renewal_date']})"

        lines = [f"{icon} *{acct_str}*  —  {timing}"]
        lines.append(f"Renewal: {item['renewal_stage']}")

        # Spend summary (only show products with meaningful volume)
        spend_parts = []
        if item["card"] > 0:
            spend_parts.append(f"Card: {_fmt_currency(item['card'])}")
        if item["bill"] > 0:
            spend_parts.append(f"Bill Pay: {_fmt_currency(item['bill'])}")
        if item["travel"] > 0:
            spend_parts.append(f"Travel: {_fmt_currency(item['travel'])}")
        if item["treasury"] > 0:
            spend_parts.append(f"Treasury: {_fmt_currency(item['treasury'])}")
        if spend_parts:
            lines.append("L30D: " + " | ".join(spend_parts))

        # Product suggestions (spend with no opp)
        if item["suggestions"]:
            lines.append(":dart: *SQL targets:* " + ", ".join(item["suggestions"]))

        # Open expansion opps
        if item["open_opps"]:
            lines.append(f":hourglass_flowing_sand: Open: {item['open_opps']}")

        # Recently CW'd (spiff credit)
        if item["recent_cw"]:
            lines.append(f":white_check_mark: Spiff credit: {item['recent_cw']}")

        # Last touch
        touch_parts = []
        if item["last_email"] and item["last_email"] != "None":
            touch_parts.append(f"Email: {item['last_email']}")
        if item["last_call"] and item["last_call"] != "None":
            touch_parts.append(f"Call: {item['last_call']}")
        if touch_parts:
            lines.append(":telephone_receiver: " + " | ".join(touch_parts))
        elif status == "untapped":
            lines.append(":warning: _No recent contact — cold outreach needed_")

        card_text = "\n".join(lines)
        if len(card_text) > 2900:
            card_text = card_text[:2900] + "..."

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": card_text},
        })

        # Draft button for untapped and in_progress accounts
        if status in ("untapped", "in_progress"):
            # Use the best product suggestion for the draft category, or default to prospect
            best_product = ""
            if item["suggestions"]:
                # Extract first suggestion's product name
                first_sug = item["suggestions"][0]
                if "Card" in first_sug:
                    best_product = "Card Expansion"
                elif "Bill Pay" in first_sug:
                    best_product = "Bill Pay Expansion"
                elif "Travel" in first_sug:
                    best_product = "Travel Expansion"
                elif "Treasury" in first_sug:
                    best_product = "Treasury Expansion"

            payload = json.dumps({
                "account": item["account"],
                "account_id": acct_id,
                "opp_id": "",
                "product": best_product,
                "category": "prospect",
            })
            blocks.append({
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":envelope: Draft Outreach", "emoji": True},
                    "action_id": f"draft_outreach_prospect_{btn_counter}",
                    "value": payload,
                    "style": "primary",
                }],
            })

        # Divider between cards
        if btn_counter < len(items):
            blocks.append({"type": "divider"})

    blocks.append(_updated_at_block(_renewals_cache.get(uid, {}).get("fetched_at", 0)))

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Trials
# ══════════════════════════════════════════════════════════════════════════════

_trials_cache = {}  # user_id -> {"data": [...], "fetched_at": float}
_TRIALS_CACHE_TTL = 600


def _build_trials_tab(client, user_id):
    """Trials: Track accounts with active Plus or Procurement trials."""
    from core.snowflake_client import run_query
    from queries.queries import ACTIVE_TRIALS_QUERY, format_query
    from utils.account_resolver import fetch_contact_emails
    from utils.contact_scoring import select_recipients

    blocks = []

    # Header
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:test_tube: Active Trials*"},
    })
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": (
            "Accounts with active Plus or Procurement trials. "
            "Engage early to drive conversion before the trial expires."
        )}],
    })

    # Check cache
    uid = user_id or "default"
    now = time.time()
    cached = _trials_cache.get(uid)
    items = None
    if cached and (now - cached["fetched_at"]) < _TRIALS_CACHE_TTL:
        items = cached["data"]

    if items is None:
        try:
            df = run_query(format_query(ACTIVE_TRIALS_QUERY, user_id=user_id))
            if df.empty:
                items = []
            else:
                # Batch fetch contacts for all trial accounts
                acct_ids = list(df["account_id"].dropna().unique())
                all_contacts = fetch_contact_emails(None, acct_ids) if acct_ids else {}

                items = []
                for _, row in df.iterrows():
                    trial_type = str(row.get("trial_type", "") or "")
                    if not trial_type:
                        continue

                    import math
                    _days_raw = row.get("days_remaining", 0)
                    days = 0 if _days_raw is None or (isinstance(_days_raw, float) and math.isnan(_days_raw)) else int(float(_days_raw))
                    acct_id = row.get("account_id", "")

                    # Get top contacts
                    contacts = all_contacts.get(acct_id, [])
                    primary, cc = select_recipients(contacts, max_cc=2)

                    # Status
                    if "expired" in trial_type.lower():
                        status = "expired"
                        icon = ":red_circle:"
                    elif days <= 7:
                        status = "expiring"
                        icon = ":large_yellow_circle:"
                    else:
                        status = "active"
                        icon = ":large_green_circle:"

                    items.append({
                        "account": row.get("account_name", "Unknown"),
                        "account_id": acct_id,
                        "trial_type": trial_type,
                        "trial_start": str(row.get("trial_start", "")),
                        "trial_end": str(row.get("trial_end", "")),
                        "days_remaining": days,
                        "status": status,
                        "icon": icon,
                        "last_email": str(row.get("last_email_date", "") or ""),
                        "last_call": str(row.get("last_call_date", "") or ""),
                        "primary_contact": primary,
                        "cc_contacts": cc,
                    })

                # Sort: expiring soonest first, then active, then expired
                _STATUS_SORT = {"expiring": 0, "active": 1, "expired": 2}
                items.sort(key=lambda x: (_STATUS_SORT.get(x["status"], 1), x["days_remaining"]))

            _trials_cache[uid] = {"data": items, "fetched_at": now}
        except Exception as e:
            logger.warning("Trials tab: query failed: %s", e)
            items = []

    if not items:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_No active trials in your book right now._"},
        })
        return blocks

    # Summary
    active_plus = sum(1 for i in items if i["trial_type"] == "Plus" and i["status"] != "expired")
    active_proc = sum(1 for i in items if i["trial_type"] == "Procurement" and i["status"] != "expired")
    expiring = sum(1 for i in items if i["status"] == "expiring")
    expired = sum(1 for i in items if i["status"] == "expired")

    summary_parts = []
    if active_plus:
        summary_parts.append(f":large_green_circle: *{active_plus}* Plus trial{'s' if active_plus != 1 else ''}")
    if active_proc:
        summary_parts.append(f":large_green_circle: *{active_proc}* Procurement trial{'s' if active_proc != 1 else ''}")
    if expiring:
        summary_parts.append(f":large_yellow_circle: *{expiring}* expiring this week")
    if expired:
        summary_parts.append(f":red_circle: *{expired}* recently expired")

    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "  |  ".join(summary_parts)}],
    })
    blocks.append({"type": "divider"})

    # Render account cards
    btn_counter = 0
    for item in items:
        btn_counter += 1
        acct_id = item["account_id"]
        icon = item["icon"]
        days = item["days_remaining"]
        trial_type = item["trial_type"]

        # SFDC link
        sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
        acct_str = f"<{sf_link}|{item['account']}>" if sf_link else item["account"]

        # Timing
        if days > 0:
            timing = f"{days}d remaining"
        elif days == 0:
            timing = "Expires *today*"
        else:
            timing = f"Expired {abs(days)}d ago"

        lines = [f"{icon} *{acct_str}*  —  {trial_type} Trial  |  {timing}"]
        lines.append(f"Started: {item['trial_start']}  |  Ends: {item['trial_end']}")

        # Key contacts
        primary = item.get("primary_contact")
        if primary:
            name = primary.get("name", "")
            title = primary.get("title", "")
            role = primary.get("user_role", "")
            contact_str = name
            if role:
                contact_str += f" ({role.replace('BUSINESS_', '').title()})"
            elif title:
                contact_str += f" ({title})"
            lines.append(f":bust_in_silhouette: {contact_str}")

            cc = item.get("cc_contacts", [])
            if cc:
                cc_names = ", ".join(
                    f"{c.get('name', '')}" for c in cc
                )
                lines.append(f"   CC: {cc_names}")

        # Last touch
        touch_parts = []
        if item["last_email"] and item["last_email"] != "None":
            touch_parts.append(f"Email: {item['last_email']}")
        if item["last_call"] and item["last_call"] != "None":
            touch_parts.append(f"Call: {item['last_call']}")
        if touch_parts:
            lines.append(":telephone_receiver: " + " | ".join(touch_parts))
        else:
            lines.append(":warning: _No recent contact — reach out now_")

        card_text = "\n".join(lines)
        if len(card_text) > 2900:
            card_text = card_text[:2900] + "..."

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": card_text},
        })

        # Draft button — fixed template for Procurement, smart drafter for Plus
        is_procurement = "procurement" in trial_type.lower()
        draft_category = "prospect_active_procurement_trial" if is_procurement else "plus_trial"

        payload = json.dumps({
            "account": item["account"],
            "account_id": acct_id,
            "opp_id": "",
            "product": "Procurement" if is_procurement else "SaaS",
            "category": draft_category,
            "trial_end": item.get("trial_end", ""),
        })
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": ":envelope: Draft Outreach", "emoji": True},
                "action_id": f"draft_outreach_{draft_category}_{btn_counter}",
                "value": payload,
                "style": "primary",
            }],
        })

        # Divider between cards
        if btn_counter < len(items):
            blocks.append({"type": "divider"})

    blocks.append(_updated_at_block(_trials_cache.get(uid, {}).get("fetched_at", 0)))

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Drafts
# ══════════════════════════════════════════════════════════════════════════════

def _build_drafts_tab(client, user_id):
    """Drafts: Pending drafts queue + flush button."""
    blocks = []

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:email: Email Drafts*"},
    })

    try:
        from utils.pending_drafts import list_pending

        pending = list_pending(user_id=user_id)
        if pending:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{len(pending)} pending draft{'s' if len(pending) != 1 else ''}* waiting to be created in Gmail:"},
            })
            for d in pending[:10]:
                to = d.get("to", "?")
                subj = d.get("subject", "?")
                acct = d.get("account_name", "")
                created = d.get("created_at", "")[:16].replace("T", " ")
                line = f"• *{acct}*\n  To: {to}\n  Subject: {subj}\n  Queued: {created}"
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": line},
                })
            if len(pending) > 10:
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"_...and {len(pending) - 10} more_"}],
                })
            blocks.append({
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":arrows_counterclockwise: Flush All to Gmail", "emoji": True},
                    "action_id": "home_flush_drafts",
                    "style": "primary",
                }],
            })
        else:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": ":white_check_mark: No pending drafts — all clear."},
            })

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": (
                "Drafts are created in Gmail under *Claude Drafts/* labels. "
                "If Gumstack auth expires, drafts queue here until flushed. "
                "You can also DM Gary `flush drafts` or `catch up`."
            )}],
        })

    except Exception as e:
        logger.debug("Drafts tab failed: %s", e)
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_Could not load draft queue._"},
        })

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Instructions
# ══════════════════════════════════════════════════════════════════════════════

def _build_instructions_tab(client, user_id):
    """Instructions: Full command reference, DM keywords, how things work."""
    blocks = []

    # ── Overview ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:wave: Welcome to Gary*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": (
            "Gary is your AI sales co-pilot. He monitors ~4,000 accounts 24/5, "
            "catches spend acceleration before baselines rise, auto-drafts outreach emails, "
            "and surfaces the right account at the right time — all inside Slack."
        )},
    })
    blocks.append({"type": "divider"})

    # ── How to interact ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:speech_balloon: How to Talk to Gary*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "• *DM Gary* — Ask questions, run commands, or look up accounts. Just type naturally.",
            f"• *Slash commands* — Use `/{COMMAND_PREFIX}-priorities`, `/{COMMAND_PREFIX}-lookup`, etc. from any channel.",
            "• *Home tab* — This screen. Use the tabs above to navigate.",
            "• *Buttons* — Click Draft, Brief, View All, etc. throughout the app.",
            "• *Group DM* — @mention Gary in a group chat and he'll respond in character.",
        ])},
    })
    blocks.append({"type": "divider"})

    # ── DM Keywords ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:keyboard: DM Keywords*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "DM Gary any of these (no slash needed):",
            "",
            "*Intelligence*",
            '`priorities` — What should I focus on?',
            '`morning brief` — Daily action summary',
            '`quota` — CP attainment + accelerator band',
            '`spend pacing` — MTD vs last month trajectory',
            '`nudge` — What changed since last check?',
            '`activity report` — SQLs + opps closed breakdown',
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Pipeline*",
            '`pipeline cleanup` — Urgency-ranked pipeline review',
            '`forecast` — S3+ opps + coaching brief',
            '`opp pacing` — Milestone tracking for open opps',
            '`zero to one` — Fresh product activations',
            '`post close monitor` — Post-close CP tracking',
            '`stale opps` — Re-engage dormant pipeline',
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Accounts & Outreach*",
            '`lookup <account>` — Full account snapshot + signals',
            '`top accounts` — Top 50 ranked by CP potential',
            '`batch outreach` — Cluster accounts + draft campaigns',
            '`bill drafter` — Sweep card payable alerts for drafts',
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Meetings*",
            '`my calendar` — Today\'s upcoming meetings',
            '`pre-meeting` — Auto-brief for next meeting',
            '`gong followup` — Check for missing post-call follow-ups',
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*System*",
            '`catch up` — Run full catch-up (backfill alerts, flush drafts, refresh priorities)',
            '`flush drafts` — Retry all pending drafts stuck in queue',
            '`status` — Health check',
            '`help` — Full command list',
        ])},
    })
    blocks.append({"type": "divider"})

    # ── Slash Commands ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:lightning: Slash Commands*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Daily Intelligence*",
            f"`/{COMMAND_PREFIX}-priorities` — Ranked actions across 7 signal categories",
            f"`/{COMMAND_PREFIX}-morning-brief` — Combined daily action summary",
            f"`/{COMMAND_PREFIX}-quota-heartbeat` — CP attainment + accelerator band",
            f"`/{COMMAND_PREFIX}-spend-pacing` — MTD vs last month + YoY trajectory",
            f"`/{COMMAND_PREFIX}-nudge` — What's new since last check",
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Pipeline & Opps*",
            f"`/{COMMAND_PREFIX}-opp <account> <product> <amount>` — Quick-create pre-filled SF opp",
            f"`/{COMMAND_PREFIX}-opps` — Open expansion opp summary",
            f"`/{COMMAND_PREFIX}-opp-pacing` — Opp milestone tracking",
            f"`/{COMMAND_PREFIX}-pipeline-cleanup` — Urgency-ranked pipeline + coaching",
            f"`/{COMMAND_PREFIX}-forecast` — S3+ opps + coaching brief",
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Account Intelligence*",
            f"`/{COMMAND_PREFIX}-lookup <account>` — Account snapshot",
            f"`/{COMMAND_PREFIX}-brief <account>` — Pre-call expansion brief",
            f"`/{COMMAND_PREFIX}-top-accounts` — Top 50 by CP potential",
            f"`/{COMMAND_PREFIX}-zero-to-one` — Fresh product activations",
            f"`/{COMMAND_PREFIX}-post-close` — Post-close activation tracking",
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*Outreach & Follow-Up*",
            f"`/{COMMAND_PREFIX}-post-meeting` — Post-meeting to-do check",
            f"`/{COMMAND_PREFIX}-batch-outreach` — Cluster accounts + draft campaigns",
            f"`/{COMMAND_PREFIX}-bill-drafter` — Bill pay email drafter sweep",
            f"`/{COMMAND_PREFIX}-activity-report` — SQLs + opps closed by product",
        ])},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "*System*",
            f"`/{COMMAND_PREFIX}-status` — Health check",
            f"`/{COMMAND_PREFIX}-help` — Full help",
            f"`/{COMMAND_PREFIX}-test` — Test all integrations",
        ])},
    })
    blocks.append({"type": "divider"})

    # ── Automated Jobs ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:clock3: Automated Schedule (19 jobs)*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            ":sunrise: *Early Morning* (3:45–5 AM PT)",
            "  Quota Insights • Acceleration Alert _(daily summary)_",
            "",
            ":sunrise: *Morning* (7:30–9 AM PT)",
            "  Pipeline Cleanup • Morning Brief • Opp Pacing • Zero-to-One • Activity Report • Spend Pacing",
            "",
            ":repeat: *Recurring* (throughout day)",
            "  Acceleration Alert _(every 30m)_ • Pre-Meeting Brief _(every 30m)_ • Gong Follow-Up _(every 30m)_",
            "  Granola Follow-Up _(every 3m)_ • Post-Meeting To-Do _(every 2h)_ • Proactive Nudge _(every 2h)_",
            "  Bill Drafter _(every 30m)_ • Draft Reminder _(every 2h)_",
            "",
            ":city_sunset: *EOD + Weekly*",
            "  Post-Close CP _(10 AM)_ • Quota Heartbeat _(6 PM)_ • Forecasting _(Mon 7 AM)_",
        ])},
    })
    blocks.append({"type": "divider"})

    # ── How signals work ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:bulb: How Signals Work*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n".join([
            "Gary detects 8 types of spend signals by comparing real-time spend against baselines:",
            "",
            ":zap: *Early Accel* — L7D pacing >1.5x baseline, L30D still low. Window open to close with low baseline.",
            ":alarm_clock: *Close Window* — Open opp with L7D ramping above L30D. Close before baseline rises.",
            ":money_with_wings: *Close Now* — Open opp where L30D already exceeds baseline. Close ASAP.",
            ":eyes: *Leading* — Large bills created/scheduled or card L3D surge. Spend incoming.",
            ":tada: *First Bill* — First bill pay usage on an open opp. Customer just started.",
            ":rocket: *Zero-to-One* — New product activated after opp was created. Lock in low baseline.",
            ":chart_with_upwards_trend: *Sustained Accel* — L7D >1.5x baseline but L30D already elevated. Window closing.",
            ":moneybag: *Treasury Spike* — GLA balance L7D avg >2x L30D avg. Large cash deposit.",
        ])},
    })
    blocks.append({"type": "divider"})

    # ── Integrations ──
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:electric_plug: Integrations*"},
    })
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": (
            ":snowflake: *Snowflake* — Accounts, opps, spend, activations, Gong transcripts\n"
            ":salesforce: *Salesforce* — Pre-filled opp creation, account links\n"
            ":calendar: *Google Calendar* — Meeting detection, auto-briefs every 30 min\n"
            ":email: *Gmail* — Auto-draft follow-up emails via Gumstack\n"
            ":robot_face: *Claude AI* — Analysis, coaching, email generation\n"
            ":studio_microphone: *Granola* — Real-time meeting transcripts + auto follow-up"
        )},
    })

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# TAB: Settings
# ══════════════════════════════════════════════════════════════════════════════

def _build_settings_tab(client, user_id):
    """Settings: DM alert toggles + drafting toggles."""
    blocks = []

    settings_blocks = _get_settings_blocks(user_id)
    if settings_blocks:
        blocks.extend(settings_blocks)

    return blocks


# ══════════════════════════════════════════════════════════════════════════════
# DATA FETCHERS (unchanged from original)
# ══════════════════════════════════════════════════════════════════════════════

def _get_quota_snapshot(user_id=None):
    """Pull quota data from Looker CSVs for the home tab.

    Returns a list of Slack blocks (not a single string) so we can include
    multiple sections and an action button.
    """
    try:
        from datetime import datetime
        import pytz
        from jobs.quota_insights import (
            _find_csv, _parse_wide_csv, _parse_dollar, _parse_pct,
            _get_team_ranking, _latest_period_with_data, _short_period,
            _TMP_DIR,
        )
        from core.gumstack_gmail import fetch_looker_zip
        from core.user_registry import get_user_sf_name
        from config import DISPLAY_TIMEZONE
        import os

        _user_name = get_user_sf_name(user_id)

        et = pytz.timezone(DISPLAY_TIMEZONE)
        now_et = datetime.now(et)
        month_short = now_et.strftime("%b")
        current_month_label = now_et.strftime("%Y-%m")
        quarter_num = (now_et.month - 1) // 3 + 1
        current_quarter_label = f"{now_et.year}-Q{quarter_num}"

        # Try to use already-extracted data, or fetch fresh
        metrics_dir = None
        if os.path.exists(_TMP_DIR):
            for entry in os.listdir(_TMP_DIR):
                full = os.path.join(_TMP_DIR, entry)
                if os.path.isdir(full) and "metrics" in entry.lower():
                    metrics_dir = full
                    break
        if not metrics_dir:
            metrics_dir = fetch_looker_zip("Growth AM IC Detailed Metrics")
        if not metrics_dir:
            return None

        # Parse key CSVs
        realized_csv = _find_csv(metrics_dir, "monthly_realized_cp_by_ic")
        realized_data, realized_periods = _parse_wide_csv(realized_csv) if realized_csv else ({}, [])

        renewal_csv = _find_csv(metrics_dir, "monthly_renewal_cp_by_ic")
        renewal_data, renewal_periods = _parse_wide_csv(renewal_csv) if renewal_csv else ({}, [])

        card_sql_csv = _find_csv(metrics_dir, "card_sqls")
        card_sql_data, card_sql_periods = _parse_wide_csv(card_sql_csv) if card_sql_csv else ({}, [])

        bp_sql_csv = _find_csv(metrics_dir, "bill_pay_sqls")
        bp_sql_data, bp_sql_periods = _parse_wide_csv(bp_sql_csv) if bp_sql_csv else ({}, [])

        saas_sql_csv = _find_csv(metrics_dir, "free-to-paid_saas_sqls")
        saas_sql_data, saas_sql_periods = _parse_wide_csv(saas_sql_csv) if saas_sql_csv else ({}, [])

        card_cw_csv = _find_csv(metrics_dir, "card_$cw_cp") or _find_csv(metrics_dir, "card_cw_cp")
        card_cw_data, card_cw_periods = _parse_wide_csv(card_cw_csv) if card_cw_csv else ({}, [])

        bp_cw_csv = _find_csv(metrics_dir, "bill_pay_$cw_cp") or _find_csv(metrics_dir, "bill_pay_cw_cp")
        bp_cw_data, bp_cw_periods = _parse_wide_csv(bp_cw_csv) if bp_cw_csv else ({}, [])

        saas_cw_csv = _find_csv(metrics_dir, "free-to-paid_saas_cw_cp")
        saas_cw_data, saas_cw_periods = _parse_wide_csv(saas_cw_csv) if saas_cw_csv else ({}, [])

        elapsed_csv = _find_csv(metrics_dir, "__of_month_elapsed") or _find_csv(metrics_dir, "of_month_elapsed")
        month_elapsed = None
        if elapsed_csv and os.path.exists(elapsed_csv):
            import csv
            with open(elapsed_csv, "r", newline="", encoding="utf-8-sig") as f:
                for row in csv.reader(f):
                    for cell in row:
                        pct = _parse_pct(cell)
                        if pct is not None and pct > 5:
                            if month_elapsed is None or pct > month_elapsed:
                                month_elapsed = pct

        def _bar(pct):
            if pct is None:
                return "`░░░░░░░░░░`"
            filled = min(int(pct / 10), 10)
            return f"`{'█' * filled}{'░' * (10 - filled)}`"

        def _icon(pct):
            if pct is None:
                return ""
            if pct >= 100:
                return " :white_check_mark:"
            if pct < 50:
                return " :red_circle:"
            return ""

        def _dollar(val):
            if val < 0:
                return f"-${abs(val):,.0f}"
            return f"${val:,.0f}"

        # Realized CP
        greg_realized = realized_data.get(_user_name, {})
        current_r_period = None
        for p in realized_periods:
            if current_month_label in p:
                current_r_period = p
                break

        realized_line = ""
        r_attain = None
        if current_r_period and current_r_period in greg_realized:
            rd = greg_realized[current_r_period]
            total = _parse_dollar(rd.get("Total Realized CP", ""))
            goal = _parse_dollar(rd.get("Rep Total Realized CP Monthly Goal", ""))
            r_attain = _parse_pct(rd.get("% Attainment", ""))
            card = _parse_dollar(rd.get("Card CP", ""))
            bp = _parse_dollar(rd.get("Bill Pay CP", ""))
            travel = _parse_dollar(rd.get("Travel CP", ""))
            treasury = _parse_dollar(rd.get("Treasury CP", ""))
            f2p = _parse_dollar(rd.get("Free to Paid CP", ""))
            procurement = _parse_dollar(rd.get("Free to Paid Procurement CP", ""))

            rank, total_reps = _get_team_ranking(realized_data, current_r_period)
            rank_str = f" • #{rank} on team" if rank else ""

            realized_line = (
                f"*Realized CP*  {_bar(r_attain)} *{r_attain:.0f}%*{_icon(r_attain)}{rank_str}\n"
                f"{_dollar(total)} / {_dollar(goal)}\n"
                f"Card {_dollar(card)} • BP {_dollar(bp)} • Travel {_dollar(travel)} • "
                f"Treasury {_dollar(treasury)} • F2P {_dollar(f2p)} • Procurement {_dollar(procurement)}"
            )

        # Renewal CP
        greg_renewal = renewal_data.get(_user_name, {})
        current_n_period = None
        for p in renewal_periods:
            if current_month_label in p:
                current_n_period = p
                break

        renewal_line = ""
        if current_n_period and current_n_period in greg_renewal:
            rn = greg_renewal[current_n_period]
            total = _parse_dollar(rn.get("Total Renewal CP", ""))
            goal = _parse_dollar(rn.get("Rep Total Renewal CP Monthly Goal", ""))
            n_attain = _parse_pct(rn.get("% Attainment", ""))
            renewal_val = _parse_dollar(rn.get("Renewal", ""))
            upsell = _parse_dollar(rn.get("Upsell", ""))

            renewal_line = (
                f"*Renewal CP*  {_bar(n_attain)} *{n_attain:.0f}%*{_icon(n_attain)}\n"
                f"{_dollar(total)} / {_dollar(goal)} • "
                f"Renewal {_dollar(renewal_val)} • Upsell {_dollar(upsell)}"
            )

        # SQLs by product
        def _sql_row(data, periods, label, hint):
            greg_d = data.get(_user_name, {})
            period = _latest_period_with_data(data, _user_name, periods, hint)
            if not period or period not in greg_d:
                return None
            d = greg_d[period]
            count = _parse_dollar(d.get("Total Opp.", ""))
            goal_val = ""
            for k in d.keys():
                if "Goal" in k:
                    goal_val = d[k]
                    break
            goal = _parse_dollar(goal_val)
            attain = _parse_pct(d.get("% Attainment", ""))
            pct_str = f" ({attain:.0f}%)" if attain is not None else ""
            icon = _icon(attain) if attain is not None else ""
            return f"{label}: *{count:.0f}* / {goal:.0f}{pct_str}{icon}"

        sql_parts = []
        card_sql = _sql_row(card_sql_data, card_sql_periods, "Card", current_month_label)
        bp_sql = _sql_row(bp_sql_data, bp_sql_periods, "BP", current_month_label)
        saas_sql = _sql_row(saas_sql_data, saas_sql_periods, "SaaS", current_month_label)
        if card_sql:
            sql_parts.append(card_sql)
        if bp_sql:
            sql_parts.append(bp_sql)
        if saas_sql:
            sql_parts.append(saas_sql)

        sql_line = ""
        if sql_parts:
            sql_line = f"*SQLs ({month_short})*\n" + " • ".join(sql_parts)

        # CW CP by product
        def _cw_row(data, periods, label, hint):
            period = _latest_period_with_data(data, _user_name, periods, hint)
            if not period:
                return None
            greg_d = data.get(_user_name, {})
            if period not in greg_d:
                return None
            d = greg_d[period]
            total_val = 0.0
            goal_val = 0.0
            attain_val = None
            for k, v in d.items():
                kl = k.lower()
                if "attainment" in kl:
                    attain_val = _parse_pct(v)
                elif "goal" in kl:
                    goal_val = _parse_dollar(v)
                elif "cp" in kl or "total" in kl:
                    total_val = _parse_dollar(v)
            pct_str = f" ({attain_val:.0f}%)" if attain_val is not None else ""
            icon = _icon(attain_val) if attain_val is not None else ""
            return f"{label}: *{_dollar(total_val)}* / {_dollar(goal_val)}{pct_str}{icon}"

        cw_parts = []
        card_cw = _cw_row(card_cw_data, card_cw_periods, "Card", current_quarter_label)
        bp_cw = _cw_row(bp_cw_data, bp_cw_periods, "BP", current_quarter_label)
        saas_cw = _cw_row(saas_cw_data, saas_cw_periods, "SaaS", current_month_label)
        if card_cw:
            cw_parts.append(card_cw)
        if bp_cw:
            cw_parts.append(bp_cw)
        if saas_cw:
            cw_parts.append(saas_cw)

        cw_line = ""
        if cw_parts:
            cw_line = "*CW CP*\n" + " • ".join(cw_parts)

        # Assemble blocks
        elapsed_str = f"  •  {month_elapsed:.0f}% of month elapsed" if month_elapsed else ""
        blocks = []

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*:chart_with_upwards_trend: Quota Snapshot — {month_short}{elapsed_str}*"},
        })

        has_data = any([realized_line, renewal_line, sql_line, cw_line])

        if realized_line:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": realized_line}})
        if renewal_line:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": renewal_line}})
        if sql_line:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": sql_line}})
        if cw_line:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": cw_line}})

        if not has_data:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": (
                "_Looker data not available — "
                "run `Full Quota Report` to refresh, or check "
                "<https://ramp.looker.com/dashboards/6865|Looker> directly_"
            )}]})

        # Full Report button
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": ":bar_chart: Full Quota Report", "emoji": True},
                "action_id": "home_run_quota_insights",
                "style": "primary",
            }],
        })

        return blocks

    except Exception as e:
        logger.debug("Quota snapshot for home tab failed: %s", e)
        return None


def _get_priority_alerts(user_id, max_per_group=5, max_groups=8):
    """Pull priority alerts from Snowflake with 10-min per-user cache.

    Parameters
    ----------
    user_id : str
        Slack user ID (used for per-user cache and query parameterization).
    max_per_group : int
        Max entries shown per signal group.
    max_groups : int
        Max signal groups to display.
    """
    global _priority_cache

    now = time.time()

    # Return cached data if still fresh — but we need to re-render with
    # the current max_per_group/max_groups, so cache the raw dataframe
    cached_df = None
    user_cache = _priority_cache.get(user_id, {})
    if user_cache.get("data") is not None and (now - user_cache.get("fetched_at", 0)) < _PRIORITY_CACHE_TTL:
        cached_df = user_cache["data"]

    try:
        if cached_df is None:
            from core.snowflake_client import run_query
            from queries.queries import HOME_PRIORITY_ALERTS_QUERY, format_query

            query = format_query(HOME_PRIORITY_ALERTS_QUERY, user_id=user_id)
            df = run_query(query)

            if df.empty:
                _priority_cache[user_id] = {"data": None, "fetched_at": now}
                return None

            # Enrich with real Gmail sent dates (runs in background threads)
            try:
                from core.gmail_sent_tracker import enrich_with_gmail_sent
                rows_as_dicts = df.to_dict("records")
                enrich_with_gmail_sent(
                    rows_as_dicts,
                    account_name_key="account_name",
                    account_id_key="account_id",
                    user_id=user_id,
                )
                # Write enriched dates back into the dataframe
                import pandas as pd
                enriched_df = pd.DataFrame(rows_as_dicts)
                # Only update columns that exist or were added
                for col in ["last_email_date", "last_email_subject", "_email_source"]:
                    if col in enriched_df.columns:
                        df[col] = enriched_df[col]
            except Exception as e:
                logger.debug("Gmail sent enrichment failed for priority alerts: %s", e)

            _priority_cache[user_id] = {"data": df, "fetched_at": now}
            cached_df = df

        # Render blocks from dataframe
        return _render_priority_blocks(cached_df, max_per_group, max_groups)

    except Exception as e:
        logger.debug("Priority alerts for home tab failed: %s", e)
        return None


def _touch_line(row_or_item):
    """Return a compact last-call / last-email context line, or ''."""
    parts = []
    for key, label in [("last_call_date", "Call"), ("last_email_date", "Email")]:
        val = row_or_item.get(key) if hasattr(row_or_item, "get") else None
        if val is not None and str(val).strip() not in ("", "None", "NaT", "2000-01-01"):
            try:
                if hasattr(val, "strftime"):
                    parts.append(f"{label} {val.strftime('%-m/%-d')}")
                else:
                    from datetime import datetime as _dt
                    d = _dt.strptime(str(val)[:10], "%Y-%m-%d")
                    parts.append(f"{label} {d.strftime('%-m/%-d')}")
            except Exception:
                pass
    return f"\n   _Last: {' · '.join(parts)}_" if parts else ""


def _render_priority_blocks(df, max_per_group, max_groups):
    """Render priority alert blocks from a cached dataframe."""
    import math

    blocks = []
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:rotating_light: Priority Alerts*"},
    })

    # Group by signal_type
    groups = {}
    for _, row in df.iterrows():
        signal = row.get("signal_type", "")
        if signal not in groups:
            groups[signal] = []
        groups[signal].append(row)

    total_items = 0
    max_total = max_per_group * max_groups
    groups_shown = 0

    def _safe_int(v):
        try:
            f = float(v)
            return 0 if math.isnan(f) else int(f)
        except Exception:
            return 0

    def _acct_link(row):
        acct_name = row.get("account_name", "Unknown")
        acct_id = row.get("account_id", "")
        sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
        return f"<{sf_link}|{acct_name}>" if sf_link else acct_name

    def _pct(paced, base):
        return int(((paced - base) / base) * 100) if base > 0 else 0

    def _presale_line(row):
        """Show AE presale estimates and flag discrepancies vs actual spend."""
        product = str(row.get("product", ""))
        ae_card = _safe_int(row.get("ae_card_presale", 0))
        ae_bp = _safe_int(row.get("ae_bp_presale", 0))
        paced = _safe_int(row.get("paced_amount", 0)) or _safe_int(row.get("spend_l30d", 0))
        presale = ae_card if "Card" in product else ae_bp if "Bill" in product else 0
        if presale <= 0:
            return ""
        if paced > 0 and presale > 0:
            ratio = paced / presale
            if ratio >= 2.0:
                return f"\n   :large_green_circle: _AE presale ${presale:,}/mo — actual {ratio:.1f}x higher → large delta upside_"
            elif ratio <= 0.3:
                return f"\n   :warning: _AE presale ${presale:,}/mo — actual only {int(ratio*100)}% → partial migration, room to grow_"
        return f"\n   _AE presale: ${presale:,}/mo_"

    _btn_counter = [0]

    def _draft_btn(row, signal_type):
        _btn_counter[0] += 1
        payload = json.dumps({
            "account": row.get("account_name", "Unknown"),
            "account_id": row.get("account_id", ""),
            "opp_id": row.get("opportunity_id", ""),
            "product": str(row.get("product", "")),
            "category": {
                "early_accel": "prospect", "close_window": "close_window",
                "leading": "prospect", "first_bill": "zero_to_one",
                "close_now": "close_now", "opp_first_spend": "zero_to_one",
                "zero_to_one": "zero_to_one",
                "sustained_accel": "prospect", "treasury_spike": "treasury_spike",
            }.get(signal_type, "prospect"),
        })
        return {
            "type": "button",
            "text": {"type": "plain_text", "text": ":envelope: Draft", "emoji": True},
            "action_id": f"draft_outreach_{signal_type}_{_btn_counter[0]}",
            "value": payload,
        }

    def _view_all_btn(signal_type, label):
        return {
            "type": "button",
            "text": {"type": "plain_text", "text": f"View All {label}", "emoji": True},
            "action_id": f"view_all_{signal_type}",
        }

    def _add_group(sig_type, header, rows_list, format_fn):
        nonlocal total_items, groups_shown
        if not rows_list or total_items >= max_total or groups_shown >= max_groups:
            return
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": header}})
        for row in rows_list[:max_per_group]:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": format_fn(row)},
                "accessory": _draft_btn(row, sig_type),
            })
            total_items += 1
        overflow = len(rows_list) - max_per_group
        if overflow > 0:
            blocks.append({"type": "actions", "elements": [_view_all_btn(sig_type, f"({len(rows_list)})")]})
        groups_shown += 1

    # Signal group formatters
    def _fmt_early(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        paced = _safe_int(row.get("paced_amount", 0))
        base = _safe_int(row.get("baseline_amount", 0))
        l30d = _safe_int(row.get("spend_l30d", 0))
        l7d = _safe_int(row.get("spend_l7d", 0))
        cp = _safe_int(row.get("est_cp", 0))
        pct = _pct(paced, base)
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return (
            f"• {_acct_link(row)} — {product} L7D pacing "
            f"${paced:,}/mo vs ${base:,} baseline (+{pct}%)"
            f"\n   _L30D only ${l30d:,} — window open to lock low baseline_{cp_str}"
            f"\n   _Why: L7D ${l7d:,} is {pct}% above 90D avg, L30D hasn't caught up_"
            f"{_presale_line(row)}{_touch_line(row)}"
        )

    def _fmt_close_window(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        paced = _safe_int(row.get("paced_amount", 0))
        l30d = _safe_int(row.get("spend_l30d", 0))
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return (
            f"• {_acct_link(row)} — {product} L7D pacing "
            f"${paced:,}/mo\n   _Close now — L30D baseline would be ${l30d:,}_{cp_str}"
            f"{_presale_line(row)}{_touch_line(row)}"
        )

    def _fmt_leading(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        paced = _safe_int(row.get("paced_amount", 0))
        base = _safe_int(row.get("baseline_amount", 0))
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        touch = _touch_line(row)
        ps = _presale_line(row)
        if "Card" in product:
            return f"• {_acct_link(row)} — {product} L3D pacing ${paced:,}/mo vs ${base:,}/mo baseline{cp_str}{ps}{touch}"
        return f"• {_acct_link(row)} — ${paced:,} in bills created/scheduled vs ${base:,}/mo baseline{cp_str}{ps}{touch}"

    def _fmt_first_bill(row):
        paced = _safe_int(row.get("paced_amount", 0))
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return (
            f"• {_acct_link(row)} — *first bill created* "
            f"(${paced:,})\n   _Bill Pay opp open — customer just started using the product_{cp_str}"
            f"{_presale_line(row)}{_touch_line(row)}"
        )

    def _fmt_close_now(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        delta = _safe_int(row.get("l30d_spend_delta", 0))
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return f"• {_acct_link(row)} — {product} • L30D +${abs(delta):,} above baseline{cp_str}{_presale_line(row)}{_touch_line(row)}"

    def _fmt_zero_to_one(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        activation_date = row.get("activation_date", "")
        act_str = ""
        if activation_date:
            try:
                if hasattr(activation_date, 'strftime'):
                    act_str = activation_date.strftime("%b %-d")
                else:
                    act_str = str(activation_date)[:10]
            except Exception:
                act_str = str(activation_date)[:10]
        spend_since = _safe_int(row.get("spend_since_opp", 0))
        spend_30 = _safe_int(row.get("spend_l30d", 0))
        spend_7 = _safe_int(row.get("spend_l7d", 0))
        spend_str = (
            f" · ${spend_since:,} since opp · L30D ${spend_30:,} · L7D ${spend_7:,}"
        ) if spend_since or spend_30 or spend_7 else ""
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return f"• {_acct_link(row)} — {product} activated {act_str}{spend_str}{cp_str}{_presale_line(row)}{_touch_line(row)}"

    def _fmt_sustained(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        paced = _safe_int(row.get("paced_amount", 0))
        base = _safe_int(row.get("baseline_amount", 0))
        pct = _pct(paced, base)
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return f"• {_acct_link(row)} — {product} pacing ${paced:,}/mo vs ${base:,} baseline (+{pct}%){cp_str}{_presale_line(row)}{_touch_line(row)}"

    def _fmt_treasury_spike(row):
        paced = _safe_int(row.get("paced_amount", 0))
        l30d = _safe_int(row.get("spend_l30d", 0))
        cp = _safe_int(row.get("est_cp", 0))
        spike_pct = _pct(paced, l30d) if l30d > 0 else 0
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        return (
            f"• {_acct_link(row)} — GLA spiked +{spike_pct}%"
            f"\n   L7D avg ${paced:,} vs L30D avg ${l30d:,}{cp_str}"
            f"\n   _Large deposit — lock in treasury expansion (uncapped H1-26)_"
            f"{_touch_line(row)}"
        )

    def _fmt_opp_first_spend(row):
        product = str(row.get("product", "")).replace(" Expansion", "")
        spend_since = _safe_int(row.get("spend_since_opp", 0))
        spend_7 = _safe_int(row.get("spend_l7d", 0))
        spend_30 = _safe_int(row.get("spend_l30d", 0))
        cp = _safe_int(row.get("est_cp", 0))
        cp_str = f" · ~${cp:,} CP" if cp > 0 else ""
        spend_detail = f"L7D ${spend_7:,}"
        if spend_since > 0:
            spend_detail += f" ({format_currency(spend_since)} since opp)"
        return (
            f"• {_acct_link(row)} — {product} *first spend detected*"
            f"\n   {spend_detail}{cp_str}"
            f"\n   _Close now while baseline is near $0 — every day of spend raises it_"
            f"{_presale_line(row)}{_touch_line(row)}"
        )

    _add_group("early_accel", ":zap: *Early Acceleration — Act Now*", groups.get("early_accel", []), _fmt_early)
    _add_group("close_window", ":alarm_clock: *Close Window — Opp Ramping*", groups.get("close_window", []), _fmt_close_window)
    _add_group("leading", ":eyes: *Leading Indicator — Spend Incoming*", groups.get("leading", []), _fmt_leading)
    _add_group("first_bill", ":tada: *First Bill Created — Bill Pay Opp Active*", groups.get("first_bill", []), _fmt_first_bill)
    _add_group("close_now", ":money_with_wings: *Close ASAP — Spend Exceeding Baseline*", groups.get("close_now", []), _fmt_close_now)
    _add_group("opp_first_spend", ":bulb: *First Spend Detected — New Activation*", groups.get("opp_first_spend", []), _fmt_opp_first_spend)
    _add_group("zero_to_one", ":rocket: *Zero-to-One Activated Since Opp Created*", groups.get("zero_to_one", []), _fmt_zero_to_one)
    _add_group("sustained_accel", ":chart_with_upwards_trend: *Sustained Acceleration — No Open Opp*", groups.get("sustained_accel", []), _fmt_sustained)
    _add_group("treasury_spike", ":moneybag: *Treasury GLA Spike — Large Deposit*", groups.get("treasury_spike", []), _fmt_treasury_spike)

    if total_items == 0:
        return None

    return blocks


def _get_activation_alerts_section(user_id: str = None):
    """Pull recent activation alerts (treasury, investment, first bill) for the Dashboard tab.

    Shows the same rich data as the DM alerts: activation type, balance, and spend context.
    Returns a list of Slack blocks or None if no activations.
    """
    try:
        from jobs.activation_alerts import detect_activations, ACTIVATION_SIGNAL_META
        from jobs.prospecting_signals import get_cached_prospects

        # Pull activations from the prospecting cache first (avoids extra query),
        # fall back to detect_activations() if cache is empty.
        activation_keys = {"new_treasury", "new_investment", "first_bill"}
        items = [
            p for p in (get_cached_prospects(user_id=user_id) or [])
            if p.get("signal_key") in activation_keys
        ]
        if not items:
            items = detect_activations(user_id=user_id)
        if not items:
            return None

        blocks = [{
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*:rotating_light: New Activation Alerts*"},
        }, {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": (
                f"{len(items)} account{'s' if len(items) != 1 else ''} "
                "just hit a product milestone"
            )}],
        }]

        _act_btn_counter = [0]

        for item in items[:8]:
            _act_btn_counter[0] += 1
            meta = ACTIVATION_SIGNAL_META.get(item.get("signal_key"), {})
            emoji = meta.get("emoji", ":bell:")
            signal_key = item.get("signal_key", "")
            acct = item.get("account", "Unknown")
            acct_id = item.get("account_id", "")
            sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
            acct_str = f"<{sf_link}|{acct}>" if sf_link else acct

            detail = item.get("signal_detail", "")

            # Spend context — mirror the DM format
            spend_parts = []
            card = item.get("card_spend_l30d", 0) or 0
            bp = item.get("billpay_spend_l30d", 0) or 0
            gla = item.get("current_gla", 0) or 0
            treasury_bal = item.get("treasury_balance", 0) or 0
            inv_bal = item.get("investment_balance", 0) or 0
            if card > 0:
                spend_parts.append(f"Card L30D: ${card:,.0f}")
            if bp > 0:
                spend_parts.append(f"BP L30D: ${bp:,.0f}")
            if gla > 0:
                spend_parts.append(f"GLA: ${gla:,.0f}")
            spend_str = " \u00b7 ".join(spend_parts) if spend_parts else ""

            text = f"{emoji} *{acct_str}*\n{detail}"
            if spend_str:
                text += f"\n{spend_str}"

            # Draft button payload — reuses the prospecting drafter categories
            # which already have the right AM intro + activation context tones
            payload = json.dumps({
                "account": acct,
                "account_id": acct_id,
                "product": "",
                "category": f"prospect_{signal_key}",
            })

            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": text},
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":envelope: Draft", "emoji": True},
                    "action_id": f"draft_outreach_activation_{_act_btn_counter[0]}",
                    "value": payload,
                },
            })

        return blocks

    except Exception as e:
        logger.debug("Activation alerts section failed: %s", e)
        return None


def _get_non_spend_signals(user_id: str = None):
    """Pull non-spend priority signals (stale, reopen, underperforming, followup)
    from the priority_actions cache for the Pipeline tab."""
    try:
        from jobs.priority_actions import get_cached_category

        blocks = []
        _NON_SPEND_CATEGORIES = [
            ("underperforming_d60", ":warning: *Underperforming D60 — Post-Close*"),
            ("underperforming_d30", ":chart_with_downwards_trend: *Underperforming D30 — Post-Close*"),
            ("followup", ":telephone_receiver: *Missing Follow-Up — Post-Meeting*"),
            ("post_meeting_opp", ":bulb: *Post-Meeting Opp — Products Discussed*"),
            ("stale", ":zzz: *Stale Opps — Re-Engage*"),
            ("reopen", ":arrows_counterclockwise: *Re-Open — Worth Revisiting*"),
        ]

        has_content = False
        for category, header in _NON_SPEND_CATEGORIES:
            items = get_cached_category(category, user_id=user_id)
            if not items:
                continue

            has_content = True
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": header},
            })
            for item in items[:3]:
                acct_name = item.get("account", item.get("account_name", "Unknown"))
                acct_id = item.get("account_id", "")
                sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
                acct_str = f"<{sf_link}|{acct_name}>" if sf_link else acct_name

                product = str(item.get("product", item.get("expansion_subtype", ""))).replace(" Expansion", "")
                detail = item.get("detail", item.get("headline", ""))

                line = f"\u2022 {acct_str}"
                if product:
                    line += f" — {product}"
                if detail:
                    line += f"\n   _{detail}_"
                line += _touch_line(item)

                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": line},
                })

            if len(items) > 3:
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"_...and {len(items) - 3} more — use `/{COMMAND_PREFIX}-priorities` for full list_"}],
                })

        # ── Missing Opps (Zero-to-One) ──
        try:
            from jobs.zero_to_one import get_missing_opps, _build_create_opp_url
            missing = get_missing_opps(user_id=user_id)
            if missing:
                has_content = True
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": ":warning: *Missing Opps — Activation, No Open Opp*"},
                })
                for item in missing[:5]:
                    acct_name = item.get("account_name", "Unknown")
                    acct_id = item.get("account_id", "")
                    product = str(item.get("product", "")).replace(" Expansion", "")
                    l30d = item.get("l30d_spend", "$0")
                    activated = item.get("activated_at", "")

                    sf_link = f"{SF_BASE_URL}/r/Account/{acct_id}/view" if acct_id else ""
                    acct_str = f"<{sf_link}|{acct_name}>" if sf_link else acct_name
                    create_url = _build_create_opp_url(acct_id, product) if acct_id else ""

                    line = f"\u2022 {acct_str} — {product}"
                    line += f"\n   L30D: {l30d} · _Create + close now. CP = growth above {l30d} over 90 days._"
                    if create_url:
                        line += f"\n   <{create_url}|Create Opp>"

                    blocks.append({
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": line},
                    })
                if len(missing) > 5:
                    blocks.append({
                        "type": "context",
                        "elements": [{"type": "mrkdwn", "text": f"_...and {len(missing) - 5} more — run `zero-to-one` for full list_"}],
                    })
        except Exception as e:
            logger.debug("Missing opps for pipeline tab failed: %s", e)

        if not has_content:
            return None
        blocks.insert(0, {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*:clipboard: Non-Spend Signals*"},
        })
        return blocks

    except Exception as e:
        logger.debug("Non-spend signals for pipeline tab failed: %s", e)
        return None


def _get_todays_meetings(user_id=None):
    """Pull ALL of today's meetings from Google Calendar."""
    try:
        from core.google_calendar_client import (
            get_todays_meetings, is_customer_meeting,
            extract_external_attendees, format_meeting_time,
        )

        meetings = get_todays_meetings(max_results=25, user_id=user_id)
        meetings = [m for m in meetings if is_customer_meeting(m)]
        if not meetings:
            return [{
                "type": "section",
                "text": {"type": "mrkdwn", "text": "_No customer meetings today_"},
            }]

        # Resolve SFDC accounts for each meeting
        _domain_map = {}
        for idx, m in enumerate(meetings):
            external = extract_external_attendees(m)
            for att in external:
                email = att.get("email", "")
                if "@" in email:
                    domain = email.split("@")[1].lower()
                    _domain_map.setdefault(domain, []).append(idx)

        sfdc_links = {}
        if _domain_map:
            try:
                from core.snowflake_client import run_query
                sf_name = get_user_sf_name(user_id) if user_id else OWNER_NAME
                q = f"""
                SELECT sa.account_id, sa.account_name, sa.website
                FROM analytics.marts.dim_sfdc_accounts sa
                JOIN (
                    SELECT DISTINCT account_id
                    FROM analytics.agg.agg_sfdc__daily_account_owner_ledger
                    WHERE date_day = CURRENT_DATE - 1
                      AND owner_name = '{sf_name}'
                ) ga ON ga.account_id = sa.account_id
                WHERE sa.website IS NOT NULL
                """
                acct_df = run_query(q)
                if not acct_df.empty:
                    for _, arow in acct_df.iterrows():
                        website = str(arow.get("website", "") or "").lower()
                        website_domain = website.replace("https://", "").replace("http://", "").replace("www.", "").strip("/").split("/")[0]
                        acct_id = arow.get("account_id", "")
                        acct_name = arow.get("account_name", "")
                        if website_domain in _domain_map:
                            sf_url = f"https://rampfinancial.lightning.force.com/lightning/r/Account/{acct_id}/view"
                            for midx in _domain_map[website_domain]:
                                if midx not in sfdc_links:
                                    sfdc_links[midx] = (acct_name, sf_url)
            except Exception as e:
                logger.debug("SFDC account lookup for meetings failed: %s", e)

        blocks = []
        for idx, m in enumerate(meetings):
            time_str = format_meeting_time(m.get("start"))
            title = m.get("title", "(No title)")
            already_happened = m.get("already_happened", False)

            status = ":white_check_mark: " if already_happened else ":clock1: "
            line = f"{status}{time_str} — {title}"

            if idx in sfdc_links:
                _, sf_url = sfdc_links[idx]
                line += f"  <{sf_url}|:link:>"

            external = extract_external_attendees(m)
            if external:
                names = [a.get("name", a.get("email", "")) for a in external[:3]]
                line += f"\n_{', '.join(names)}_"

            meeting_payload = {
                "title": title,
                "attendees": [
                    {"email": a.get("email", ""), "name": a.get("name", "")}
                    for a in external[:5]
                ] if external else [],
                "event_id": m.get("event_id", ""),
            }
            payload_str = json.dumps(meeting_payload)

            if already_happened:
                blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": line}})
                blocks.append({
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": ":briefcase: Pre-Meeting Brief", "emoji": True},
                            "action_id": f"home_brief_{idx}",
                            "value": payload_str,
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": ":memo: Post-Meeting", "emoji": True},
                            "action_id": f"home_post_meeting_{idx}",
                            "value": payload_str,
                        },
                    ],
                })
            else:
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": line},
                    "accessory": {
                        "type": "button",
                        "text": {"type": "plain_text", "text": ":briefcase: Pre-Meeting Brief", "emoji": True},
                        "action_id": f"home_brief_{idx}",
                        "value": payload_str,
                    },
                })

        return blocks

    except Exception as e:
        logger.debug("Today's meetings for home tab failed: %s", e)
        return None


def _get_settings_blocks(user_id=None):
    """Build settings toggle blocks for the home tab."""
    try:
        from utils.settings import load_settings

        settings = load_settings(user_id)

        blocks = []
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*:gear: Settings*"},
        })

        dm_alert_keys = [
            ("morning_brief", "Morning Brief"),
            ("post_meeting_dm", "Post-Meeting"),
            ("quota_insights", "Quota Pulse"),
            ("spend_pacing", "Spend Pacing"),
        ]
        dm_alert_keys_2 = [
            ("opp_pacing", "Opp Pacing"),
            ("zero_to_one", "Zero-to-One"),
            ("pipeline_cleanup", "Pipeline"),
            ("proactive_nudge", "Nudge"),
        ]
        signal_keys = [
            ("signal_early_accel", ":zap: Early Accel"),
            ("signal_close_window", ":alarm_clock: Close Window"),
            ("signal_leading", ":eyes: Leading"),
            ("signal_first_bill", ":tada: First Bill"),
            ("signal_opp_first_spend", ":bulb: First Spend"),
            ("signal_treasury_spike", ":moneybag: Treasury"),
        ]
        drafting_keys = [
            ("auto_drafting", "Auto Email Drafts"),
        ]

        def _toggle_button(key, label, current_val):
            icon = "\u2705" if current_val else "\u2b1c"
            return {
                "type": "button",
                "text": {"type": "plain_text", "text": f"{icon} {label}", "emoji": True},
                "action_id": f"settings_toggle_{key}",
            }

        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "*DM Alerts*"}],
        })
        blocks.append({
            "type": "actions",
            "elements": [
                _toggle_button(key, label, settings.get(key, True))
                for key, label in dm_alert_keys
            ],
        })
        blocks.append({
            "type": "actions",
            "elements": [
                _toggle_button(key, label, settings.get(key, True))
                for key, label in dm_alert_keys_2
            ],
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "*Real-Time Signal Alerts*"}],
        })
        blocks.append({
            "type": "actions",
            "elements": [
                _toggle_button(key, label, settings.get(key, True))
                for key, label in signal_keys
            ],
        })
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "*Drafting*"}],
        })
        blocks.append({
            "type": "actions",
            "elements": [
                _toggle_button(key, label, settings.get(key, True))
                for key, label in drafting_keys
            ],
        })

        return blocks

    except Exception as e:
        logger.debug("Settings blocks failed: %s", e)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# ACTION HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

def register_home_tab_actions(app):
    """Register button actions from the Home tab."""
    from config import GREG_SLACK_ID

    # ── Registration flow ────────────────────────────────────────────────
    @app.action("open_registration_modal")
    def handle_open_registration(ack, body, client):
        ack()
        client.views_open(
            trigger_id=body["trigger_id"],
            view={
                "type": "modal",
                "callback_id": "registration_modal_submit",
                "title": {"type": "plain_text", "text": "Register for Gary Bot"},
                "submit": {"type": "plain_text", "text": "Register"},
                "blocks": [
                    {"type": "input", "block_id": "reg_sf_name", "label": {"type": "plain_text", "text": "Full Name (as it appears in Salesforce)"}, "element": {"type": "plain_text_input", "action_id": "sf_name_input", "placeholder": {"type": "plain_text", "text": "e.g. Gregory Nallie"}}},
                    {"type": "input", "block_id": "reg_first_name", "label": {"type": "plain_text", "text": "First Name"}, "element": {"type": "plain_text_input", "action_id": "first_name_input", "placeholder": {"type": "plain_text", "text": "e.g. Greg"}}},
                    {"type": "input", "block_id": "reg_email", "label": {"type": "plain_text", "text": "Ramp Email"}, "element": {"type": "plain_text_input", "action_id": "email_input", "placeholder": {"type": "plain_text", "text": "e.g. gnallie@ramp.com"}}},
                    {"type": "input", "block_id": "reg_booking", "label": {"type": "plain_text", "text": "Booking Link (Chilipiper)"}, "element": {"type": "plain_text_input", "action_id": "booking_input", "placeholder": {"type": "plain_text", "text": "https://ramp-com.chilipiper.com/me/your-name/ramp"}}, "optional": True},
                    {"type": "input", "block_id": "reg_sfdc_user_id", "label": {"type": "plain_text", "text": "SFDC User ID"}, "element": {"type": "plain_text_input", "action_id": "sfdc_user_id_input", "placeholder": {"type": "plain_text", "text": "Find in Salesforce URL: /lightning/settings/personal/PersonalInformation (18-char ID starting with 005)"}}, "optional": True},
                ],
            },
        )

    @app.view("registration_modal_submit")
    def handle_registration_submit(ack, body, client, view):
        ack()
        user_id = body["user"]["id"]
        values = view["state"]["values"]
        sf_name = values["reg_sf_name"]["sf_name_input"]["value"].strip()
        first_name = values["reg_first_name"]["first_name_input"]["value"].strip()
        email = values["reg_email"]["email_input"]["value"].strip()
        booking = (values["reg_booking"]["booking_input"]["value"] or "").strip()
        sfdc_user_id = (values["reg_sfdc_user_id"]["sfdc_user_id_input"]["value"] or "").strip()

        register_user(user_id, sf_name, first_name, email, booking, sfdc_user_id=sfdc_user_id)
        logger.info("User registered: %s (%s)", sf_name, user_id)

        # Refresh home tab to show the real dashboard
        try:
            blocks = _build_home_blocks(client, user_id)
            client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
        except Exception as e:
            logger.error("Home tab refresh after registration failed: %s", e)

    # ── Tab switching ────────────────────────────────────────────────────
    @app.action({"action_id": re.compile(r"^home_tab_switch_")})
    def handle_tab_switch(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        action_id = action.get("action_id", "")
        tab_name = action_id.replace("home_tab_switch_", "", 1)
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        _active_tab[user_id] = tab_name

        # Publish an immediate loading state so the tab switch feels responsive
        try:
            loading_blocks = _build_home_blocks_header(tab_name)
            loading_blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": ":hourglass_flowing_sand: Loading..."},
            })
            client.views_publish(
                user_id=user_id,
                view={"type": "home", "blocks": loading_blocks},
            )
        except Exception:
            pass  # Non-critical — continue to full refresh

        def _refresh():
            try:
                logger.info("Home tab switch: building %s for user=%s", tab_name, user_id)
                blocks = _build_home_blocks(client, user_id)
                logger.info("Home tab switch: publishing %d blocks for user=%s", len(blocks), user_id)
                client.views_publish(
                    user_id=user_id,
                    view={"type": "home", "blocks": blocks},
                )
                logger.info("Home tab switch: published %s successfully for user=%s", tab_name, user_id)
            except Exception as e:
                logger.error("Home tab switch failed for user=%s tab=%s: %s", user_id, tab_name, e, exc_info=True)

        threading.Thread(target=_refresh, daemon=True).start()

    # ── Stale Opps sort toggle ───────────────────────────────────────────
    @app.action({"action_id": re.compile(r"^stale_sort_")})
    def handle_stale_sort(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        action_id = action.get("action_id", "")
        sort_mode = action_id.replace("stale_sort_", "", 1)  # "cp" or "staleness"
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        _stale_sort[user_id] = sort_mode

        # Refresh home tab (already on stale tab)
        def _refresh():
            try:
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(
                    user_id=user_id,
                    view={"type": "home", "blocks": blocks},
                )
            except Exception as e:
                logger.error("Stale sort toggle failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    # ── Stale tab: snooze button ─────────────────────────────────────────
    @app.action({"action_id": re.compile(r"^snooze_stale_")})
    def handle_snooze_stale(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        try:
            payload = json.loads(action.get("value", "{}"))
            opp_id = payload.get("opp_id", "")
            account_name = payload.get("account", "")
            days = payload.get("days", 7)

            from utils.snooze import snooze_opp
            snooze_opp(opp_id, days=days, user_id=user_id)

            # Clear the stale opps cache so the tab refreshes without this opp
            from jobs.priority_actions import _cached_actions
            uid = user_id or "default"
            if uid in _cached_actions and "stale" in _cached_actions[uid]:
                del _cached_actions[uid]["stale"]

            # Refresh home tab
            def _refresh():
                try:
                    blocks = _build_home_blocks(client, user_id)
                    client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
                except Exception as e:
                    logger.error("Snooze refresh failed: %s", e)

            threading.Thread(target=_refresh, daemon=True).start()

            client.chat_postMessage(
                channel=user_id,
                text=f":zzz: Snoozed *{account_name}* for {days} days. It will reappear in the Stale Opps tab after that.",
            )
        except Exception as e:
            logger.error("Snooze handler failed: %s", e)

    # ── Pipeline tab: sort toggle ────────────────────────────────────────
    @app.action({"action_id": re.compile(r"^pipeline_sort_")})
    def handle_pipeline_sort(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        action_id = action.get("action_id", "")
        sort_mode = action_id.replace("pipeline_sort_", "", 1)
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        state = _get_pipeline_state(user_id)
        state["sort"] = sort_mode
        state["page"] = 1  # reset to page 1 on sort change
        _active_tab[user_id] = "pipeline"

        def _refresh():
            try:
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("Pipeline sort failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    # ── Pipeline tab: filter chip toggle ─────────────────────────────────
    @app.action({"action_id": re.compile(r"^pipeline_filter_")})
    def handle_pipeline_filter(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        action_id = action.get("action_id", "")
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        # Parse "pipeline_filter_{key}_{value}" — key is one of product/stage/type/staleness
        parts = action_id.replace("pipeline_filter_", "", 1).split("_", 1)
        if len(parts) != 2:
            return
        key, value = parts
        # Restore spaces in value (we replaced them with _ in action_id)
        value = value.replace("_", " ")

        state = _get_pipeline_state(user_id)
        _active_tab[user_id] = "pipeline"
        state["page"] = 1

        if key == "staleness":
            # Single-select
            state["filters"]["staleness"] = value.replace(" ", "")  # e.g. "15+"
        else:
            # Multi-select — toggle membership
            key_plural = key + "s"  # products / stages / types
            current = state["filters"].get(key_plural, [])
            if value in current:
                current.remove(value)
            else:
                current.append(value)
            state["filters"][key_plural] = current

        def _refresh():
            try:
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("Pipeline filter failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    # ── Pipeline tab: pagination ─────────────────────────────────────────
    @app.action({"action_id": re.compile(r"^pipeline_page_")})
    def handle_pipeline_page(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        direction = action.get("action_id", "").replace("pipeline_page_", "", 1)
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        state = _get_pipeline_state(user_id)
        _active_tab[user_id] = "pipeline"
        if direction == "prev":
            state["page"] = max(1, state.get("page", 1) - 1)
        elif direction == "next":
            state["page"] = state.get("page", 1) + 1  # clamped in _build_pipeline_tab

        def _refresh():
            try:
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("Pipeline pagination failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    # ── Pipeline tab: reset filters ──────────────────────────────────────
    @app.action("pipeline_reset_filters")
    def handle_pipeline_reset(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _pipeline_state[user_id] = _pipeline_default_state()
        _active_tab[user_id] = "pipeline"

        def _refresh():
            try:
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("Pipeline reset failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    # ── Pipeline tab: Draft Re-engage (Phase 2b — unified multi-opp email) ──
    @app.action({"action_id": re.compile(r"^pipeline_draft_")})
    def handle_pipeline_draft(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        try:
            payload = json.loads(action.get("value", "{}"))
            account_name = payload.get("account_name", "")
            opps = payload.get("opps", []) or []

            products = ", ".join(
                sorted({(o.get("product") or o.get("type") or "?") for o in opps})
            )
            client.chat_postMessage(
                channel=user_id,
                text=f":email: Drafting unified re-engagement email for *{account_name}* ({len(opps)} open opps: {products})...",
            )

            def _draft():
                try:
                    from jobs.pipeline_drafter import draft_account_reengagement
                    draft_account_reengagement(payload, client, user_id=user_id)
                except Exception as e:
                    logger.error("Pipeline draft failed: %s", e, exc_info=True)
                    client.chat_postMessage(
                        channel=user_id,
                        text=f":warning: Draft failed for {account_name}: {e}",
                    )

            threading.Thread(target=_draft, daemon=True).start()
        except Exception as e:
            logger.error("Pipeline draft handler failed: %s", e)

    # ── Pipeline tab: Snooze 14d (account-level) ─────────────────────────
    @app.action({"action_id": re.compile(r"^pipeline_snooze_")})
    def handle_pipeline_snooze(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        try:
            payload = json.loads(action.get("value", "{}"))
            account_name = payload.get("account_name", "")
            account_id = payload.get("account_id", "")

            from utils.snooze import snooze_opp
            # Account-level key — matches the Prospecting tab's `acct:<id>` pattern.
            snooze_opp(f"acct:{account_id}", days=14, user_id=user_id)

            _pipeline_cache.pop(user_id, None)
            _active_tab[user_id] = "pipeline"

            def _refresh():
                try:
                    blocks = _build_home_blocks(client, user_id)
                    client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
                except Exception as e:
                    logger.error("Pipeline snooze refresh failed: %s", e)

            threading.Thread(target=_refresh, daemon=True).start()

            client.chat_postMessage(
                channel=user_id,
                text=f":zzz: Snoozed *{account_name}* for 14 days (all open opps hidden). Will reappear after that.",
            )
        except Exception as e:
            logger.error("Pipeline snooze failed: %s", e)

    # ── Pipeline tab: Propose SFDC Updates ───────────────────────────────
    @app.action({"action_id": re.compile(r"^pipeline_propose_")})
    def handle_pipeline_propose(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        try:
            payload = json.loads(action.get("value", "{}"))
            account_id = payload.get("account_id", "")
            account_name = payload.get("account_name", "")
        except Exception as e:
            logger.error("Pipeline propose payload parse failed: %s", e)
            return

        if not account_id:
            return

        client.chat_postMessage(
            channel=user_id,
            text=f":arrows_counterclockwise: Analyzing recent call + email activity for *{account_name}* to propose SFDC updates...",
        )

        def _run():
            try:
                from jobs.pipeline_update_proposer import (
                    propose_updates_for_account,
                    dm_account_update_review,
                )
                # Pre-check: if no proposals, DM the "looks accurate" message.
                result = propose_updates_for_account(account_id, user_id=user_id)
                if not result.get("any_proposals"):
                    client.chat_postMessage(
                        channel=user_id,
                        text=(f":white_check_mark: Analyzed *{account_name}* — "
                              f"no SFDC updates needed. Current fields look accurate "
                              f"per recent call + email activity."),
                    )
                    return
                # Header + review cards
                n_with_changes = sum(1 for v in result["proposals_by_opp"].values()
                                      if not v.get("empty"))
                client.chat_postMessage(
                    channel=user_id,
                    text=(f":arrows_counterclockwise: Proposed SFDC updates for "
                          f"*{account_name}* ({n_with_changes} opps with changes)."),
                )
                dm_account_update_review(
                    client, user_id, account_id,
                    pending_store=_pending_sfdc_updates,
                    source_label="Pipeline tab",
                )
            except Exception as e:
                logger.error("Propose updates failed: %s", e, exc_info=True)
                client.chat_postMessage(
                    channel=user_id,
                    text=f":warning: Propose Updates failed for {account_name}: {e}",
                )

        threading.Thread(target=_run, daemon=True).start()

    # ── Pipeline tab: Apply Selected SFDC updates ────────────────────────
    @app.action({"action_id": re.compile(r"^pipeline_apply_updates_")})
    def handle_pipeline_apply(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        opp_id = action.get("action_id", "").replace("pipeline_apply_updates_", "", 1)

        pending = _pending_sfdc_updates.get(user_id, {}).get(opp_id)
        if not pending or not pending.get("proposals"):
            client.chat_postMessage(
                channel=user_id,
                text=":warning: No pending updates to apply (may have been skipped or timed out).",
            )
            return

        def _apply():
            try:
                from core.salesforce_client import update_opportunity
                fields = {}
                if "next_step" in pending["proposals"]:
                    fields["NextStep"] = pending["proposals"]["next_step"][:255]
                if "next_step_due_date" in pending["proposals"]:
                    fields["Next_Step_Due_Date__c"] = pending["proposals"]["next_step_due_date"]
                if "expansion_notes" in pending["proposals"]:
                    fields["Expansion_Notes__c"] = pending["proposals"]["expansion_notes"][:200]
                if "stage" in pending["proposals"]:
                    fields["StageName"] = pending["proposals"]["stage"]
                if "close_date" in pending["proposals"]:
                    fields["CloseDate"] = pending["proposals"]["close_date"]

                ok = update_opportunity(opp_id, fields, user_id=user_id)
                if ok:
                    sf_link = f"{SF_BASE_URL}/r/Opportunity/{opp_id}/view"
                    applied_fields = ", ".join(pending["proposals"].keys())
                    msg = (
                        f":white_check_mark: Applied updates to *{pending['account_name']}* — "
                        f"*{pending['product']}* opp. Fields changed: `{applied_fields}`.\n"
                        f"<{sf_link}|View in Salesforce>"
                    )
                else:
                    msg = (
                        f":warning: SFDC update via Growth MCP returned failure for "
                        f"*{pending['account_name']}* — *{pending['product']}*. "
                        f"Check bot logs for details; fields were: `{', '.join(fields.keys())}`."
                    )

                client.chat_postMessage(channel=user_id, text=msg)
                # Invalidate pipeline cache + pending state
                _pipeline_cache.pop(user_id, None)
                _pending_sfdc_updates.get(user_id, {}).pop(opp_id, None)
            except Exception as e:
                logger.error("SFDC apply failed: %s", e, exc_info=True)
                client.chat_postMessage(
                    channel=user_id,
                    text=f":warning: Apply failed: {e}",
                )

        threading.Thread(target=_apply, daemon=True).start()

    # ── Pipeline tab: Skip a specific field from the proposal ────────────
    @app.action({"action_id": re.compile(r"^pipeline_skip_field_")})
    def handle_pipeline_skip_field(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        try:
            payload = json.loads(action.get("value", "{}"))
            opp_id = payload.get("opp_id", "")
            field = payload.get("field", "")
        except Exception:
            return

        pending = _pending_sfdc_updates.get(user_id, {}).get(opp_id)
        if not pending:
            return
        pending["proposals"].pop(field, None)

        # Re-render this review card in place if we have the message ts
        channel = pending.get("channel")
        message_ts = pending.get("message_ts")
        if channel and message_ts:
            from jobs.pipeline_update_proposer import build_opp_review_blocks
            blocks = build_opp_review_blocks(pending)
            try:
                client.chat_update(channel=channel, ts=message_ts, blocks=blocks, text=f"Updates for {pending['account_name']}")
            except Exception as e:
                logger.debug("Review card re-render failed: %s", e)

    # ── Pipeline tab: Dismiss review card without applying ───────────────
    @app.action({"action_id": re.compile(r"^pipeline_dismiss_updates_")})
    def handle_pipeline_dismiss_updates(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        opp_id = action.get("action_id", "").replace("pipeline_dismiss_updates_", "", 1)

        pending = _pending_sfdc_updates.get(user_id, {}).pop(opp_id, None)
        if pending and pending.get("channel") and pending.get("message_ts"):
            try:
                client.chat_update(
                    channel=pending["channel"],
                    ts=pending["message_ts"],
                    text=f":x: Dismissed proposed updates for *{pending.get('account_name','?')}* — *{pending.get('product','?')}*.",
                    blocks=[{
                        "type": "section",
                        "text": {"type": "mrkdwn",
                                 "text": f":x: Dismissed proposed updates for *{pending.get('account_name','?')}* — *{pending.get('product','?')}*."},
                    }],
                )
            except Exception:
                pass

    # ── Pipeline tab: SFDC link (no handler needed — URL button) ─────────
    @app.action({"action_id": re.compile(r"^pipeline_sfdc_")})
    def handle_pipeline_sfdc(ack, body, client):
        ack()  # just ack the URL click; Slack opens the link itself

    # ── Prospecting tab: filter buttons ──────────────────────────────────
    @app.action({"action_id": re.compile(r"^prospect_filter_")})
    def handle_prospect_filter(ack, body, client):
        ack()
        action = body.get("actions", [{}])[0]
        action_id = action.get("action_id", "")
        filter_key = action_id.replace("prospect_filter_", "", 1)
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        _prospect_filter[user_id] = filter_key
        _active_tab[user_id] = "prospecting"

        def _refresh():
            try:
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(
                    user_id=user_id,
                    view={"type": "home", "blocks": blocks},
                )
            except Exception as e:
                logger.error("Prospect filter toggle failed: %s", e)

        threading.Thread(target=_refresh, daemon=True).start()

    @app.action("prospect_refresh")
    def handle_prospect_refresh(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
        _active_tab[user_id] = "prospecting"

        client.chat_postMessage(
            channel=user_id,
            text=":arrows_counterclockwise: Refreshing prospecting signals... this takes ~30 seconds.",
        )

        def _refresh():
            try:
                from jobs.prospecting_signals import gather_prospecting_signals
                gather_prospecting_signals(user_id=user_id, force=True)
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(
                    user_id=user_id,
                    view={"type": "home", "blocks": blocks},
                )
            except Exception as e:
                logger.error("Prospect refresh failed: %s", e)
                client.chat_postMessage(
                    channel=user_id,
                    text=f"Prospecting refresh failed: {e}",
                )

        threading.Thread(target=_refresh, daemon=True).start()

    # ── Flush drafts button ──────────────────────────────────────────────
    @app.action("home_flush_drafts")
    def handle_flush_drafts(ack, body, client):
        ack()
        user_id = body.get("user", {}).get("id", GREG_SLACK_ID)

        def _flush():
            try:
                from utils.pending_drafts import flush_to_gmail, list_pending
                pending = list_pending(user_id=user_id)
                if not pending:
                    client.chat_postMessage(channel=user_id, text="No pending drafts to flush.")
                    return
                client.chat_postMessage(
                    channel=user_id,
                    text=f"Flushing {len(pending)} pending draft{'s' if len(pending) != 1 else ''} to Gmail...",
                )
                succeeded, failed = flush_to_gmail(user_id=user_id)
                msg = f"Flush complete — {succeeded} draft{'s' if succeeded != 1 else ''} created in Gmail."
                if failed:
                    msg += f" {failed} failed (check Gumstack auth at gumloop.com/personal/apps)."
                client.chat_postMessage(channel=user_id, text=msg)

                # Refresh the drafts tab
                blocks = _build_home_blocks(client, user_id)
                client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
            except Exception as e:
                logger.error("Home tab flush drafts failed: %s", e)
                client.chat_postMessage(channel=user_id, text=f"Draft flush failed: {e}")

        threading.Thread(target=_flush, daemon=True).start()

    # ── Quick action buttons ─────────────────────────────────────────────
    _ACTION_MAP = {
        "home_run_priorities": ("jobs.priority_actions", "run_priority_actions"),
        "home_run_quota": ("jobs.quota_heartbeat", "run_quota_heartbeat"),
        "home_run_morning": ("jobs.morning_brief", "run_morning_brief"),
        "home_run_nudge": ("jobs.proactive_nudge", "run_proactive_nudge"),
        "home_run_spend": ("jobs.spend_pacing", "run_spend_pacing"),
        "home_run_cleanup": ("jobs.pipeline_cleanup", "run_pipeline_cleanup"),
        "home_run_zero_to_one": ("jobs.zero_to_one", "run_zero_to_one"),
        "home_run_forecast": ("jobs.forecasting", "run_forecasting"),
        "home_run_quota_insights": ("jobs.quota_insights", "run_quota_insights"),
    }

    for action_id, (module_path, func_name) in _ACTION_MAP.items():
        def _make_handler(mod_path, fn_name):
            def handler(ack, body, client):
                ack()
                handler_user_id = body.get("user", {}).get("id", GREG_SLACK_ID)
                def _run():
                    try:
                        import importlib
                        mod = importlib.import_module(mod_path)
                        fn = getattr(mod, fn_name)
                        fn(client, user_id=handler_user_id)
                    except Exception as e:
                        logger.error("Home tab action %s failed: %s", fn_name, e)
                        client.chat_postMessage(
                            channel=handler_user_id,
                            text=f"Home tab action failed: {e}",
                        )
                threading.Thread(target=_run, daemon=True).start()
            return handler

        app.action(action_id)(_make_handler(module_path, func_name))
