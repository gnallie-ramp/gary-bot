"""Quota Insights — daily Looker CSV digest, 6:45 AM ET.

Pulls Looker CSV exports from Gmail (ZIP attachments), parses quota
attainment data for Gregory Nallie, and sends a formatted Slack DM
with realized CP, renewal CP, SQLs, CW metrics, and trend analysis.
"""

import csv
import logging
import os
import shutil
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from config import GREG_SLACK_ID, DISPLAY_TIMEZONE, OWNER_NAME
from core.gumstack_gmail import fetch_looker_zip

logger = logging.getLogger(__name__)

_TMP_DIR = "/tmp/gary_bot_looker"


# ── Parsing helpers ──────────────────────────────────────────────────────────

def _parse_dollar(s: str) -> float:
    """Strip '$' and commas, return float. Returns 0.0 for empty/missing."""
    if not s or not str(s).strip():
        return 0.0
    cleaned = str(s).strip().replace("$", "").replace(",", "").replace('"', "")
    if not cleaned:
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_pct(s: str) -> Optional[float]:
    """Strip '%', return float. Returns None for empty."""
    if not s or not str(s).strip():
        return None
    cleaned = str(s).strip().replace("%", "").replace('"', "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_wide_csv(filepath: str) -> Tuple[Dict[str, Dict[str, Dict[str, str]]], List[str]]:
    """Parse a wide-format Looker CSV.

    Row 0: time period headers (repeat across column groups, may be blank for first col).
    Row 1: column names repeated per time period group.
    Remaining rows: data, first column is the owner name.

    Returns:
        (data_dict, periods)
        data_dict: {owner_name: {period: {col_name: raw_value}}}
        periods: ordered list of time period labels found
    """
    if not os.path.exists(filepath):
        return {}, []

    with open(filepath, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if len(rows) < 3:
        return {}, []

    period_row = rows[0]  # time period labels
    col_row = rows[1]     # column names per period

    # Build column mapping: for each column index > 0, figure out
    # which period it belongs to and what the column name is.
    # Period headers only appear in the first column of each group,
    # so we forward-fill.
    current_period = ""
    col_map = []  # list of (period, col_name) for each column index
    periods_seen = []

    for i in range(len(col_row)):
        if i == 0:
            col_map.append(("_name", col_row[i]))
            continue
        if i < len(period_row) and period_row[i].strip():
            current_period = period_row[i].strip()
            if current_period not in periods_seen:
                periods_seen.append(current_period)
        col_name = col_row[i].strip() if i < len(col_row) else ""
        col_map.append((current_period, col_name))

    # Parse data rows
    data = {}
    for row in rows[2:]:
        if not row or not row[0].strip():
            continue  # skip totals row (empty first column)
        owner = row[0].strip()
        data[owner] = {}
        for i in range(1, len(row)):
            if i >= len(col_map):
                break
            period, col_name = col_map[i]
            if period not in data[owner]:
                data[owner][period] = {}
            data[owner][period][col_name] = row[i].strip() if i < len(row) else ""

    return data, periods_seen


def _find_greg_row(csv_path: str, owner_name: str = OWNER_NAME) -> Optional[List[str]]:
    """Find the owner's row in a standard CSV (first column = name). Returns the row or None."""
    if not os.path.exists(csv_path):
        return None
    with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip() == owner_name:
                return row
    return None


def _find_csv(extract_dir: str, name_fragment: str) -> Optional[str]:
    """Find a CSV file inside the extraction directory (may be in a subdirectory)."""
    for root, _dirs, files in os.walk(extract_dir):
        for fname in files:
            if fname.lower().endswith(".csv") and name_fragment.lower() in fname.lower():
                return os.path.join(root, fname)
    return None


def _latest_period_with_data(data: Dict, owner: str, periods: List[str], current_hint: str = "") -> Optional[str]:
    """Find the latest period that has non-empty data for the owner.

    If current_hint is provided (e.g. '2026-03' or '2026-Q1'), prefer that.
    Falls back to the latest period with non-zero attainment or any non-empty value.
    """
    owner_data = data.get(owner, {})
    if not owner_data:
        return None

    # First try to match the hint
    if current_hint:
        for p in periods:
            if current_hint in p and p in owner_data:
                vals = owner_data[p]
                if any(v.strip() for v in vals.values() if v):
                    return p

    # Otherwise find the latest period with actual data
    for p in reversed(periods):
        if p in owner_data:
            vals = owner_data[p]
            # Check if there's any non-empty, non-zero value
            for v in vals.values():
                v_stripped = v.strip() if v else ""
                if v_stripped and v_stripped not in ("", "$0", "0", "0%"):
                    return p

    return None


def _short_period(period: str) -> str:
    """Convert period label to short form: '2026-03' → 'Mar', '2026-Q1' → 'Q1'."""
    p = period.strip()
    if "-Q" in p:
        return p.split("-")[1]  # "2026-Q1" → "Q1"
    try:
        dt = datetime.strptime(p, "%Y-%m")
        return dt.strftime("%b")  # "2026-03" → "Mar"
    except ValueError:
        return p


def _attainment_icon(pct: Optional[float]) -> str:
    """Return emoji based on attainment percentage."""
    if pct is None:
        return ""
    if pct >= 150:
        return " :fire:"
    if pct >= 100:
        return " :white_check_mark:"
    if pct >= 50:
        return ""
    return ""


def _flag_icon(pct: Optional[float]) -> str:
    """Return flag emoji for attention areas."""
    if pct is None:
        return ""
    if pct < 50:
        return ":red_circle: "
    if pct < 80:
        return ":warning: "
    return ""


def _fmt_dollar(val: float) -> str:
    """Format a dollar value like $12,207."""
    if val < 0:
        return f"-${abs(val):,.0f}"
    return f"${val:,.0f}"


def _fmt_pct(val: Optional[float]) -> str:
    """Format a percentage like 111%."""
    if val is None:
        return "N/A"
    return f"{val:.0f}%"


# ── Team ranking ─────────────────────────────────────────────────────────────

def _get_team_ranking(data: Dict, current_period: str, owner_name: str = OWNER_NAME) -> Tuple[Optional[int], int]:
    """Get the owner's rank among all reps for current month realized CP attainment.

    Returns (rank, total_reps). rank is 1-indexed. Returns (None, 0) if unavailable.
    """
    reps = []
    for owner, periods in data.items():
        if current_period in periods:
            pct = _parse_pct(periods[current_period].get("% Attainment", ""))
            if pct is not None:
                reps.append((owner, pct))

    if not reps:
        return None, 0

    reps.sort(key=lambda x: x[1], reverse=True)
    total = len(reps)
    for i, (name, _) in enumerate(reps):
        if name == owner_name:
            return i + 1, total
    return None, total


# ── Main job ─────────────────────────────────────────────────────────────────

def run_quota_insights(client, user_id=None):
    """Pull Looker CSVs from Gmail and send daily quota pulse DM."""
    from core.user_registry import get_user_sf_name

    dm_target = user_id or GREG_SLACK_ID
    _owner_name = get_user_sf_name(user_id) if user_id else get_user_sf_name(GREG_SLACK_ID)

    logger.info("Quota insights: starting...")

    # Clean up stale data
    if os.path.exists(_TMP_DIR):
        shutil.rmtree(_TMP_DIR, ignore_errors=True)

    # Fetch both ZIP exports
    metrics_dir = fetch_looker_zip("Growth AM IC Detailed Metrics")
    portfolio_dir = fetch_looker_zip("Growth AM IC Detailed Portfolio")

    if not metrics_dir:
        logger.warning("Quota insights: Metrics ZIP not found — Looker email may be delayed. Skipping.")
        return

    # ── Parse key CSVs ───────────────────────────────────────────────────

    # 1. Monthly realized CP
    realized_csv = _find_csv(metrics_dir, "monthly_realized_cp_by_ic")
    realized_data, realized_periods = _parse_wide_csv(realized_csv) if realized_csv else ({}, [])

    # 2. Monthly renewal CP
    renewal_csv = _find_csv(metrics_dir, "monthly_renewal_cp_by_ic")
    renewal_data, renewal_periods = _parse_wide_csv(renewal_csv) if renewal_csv else ({}, [])

    # 3. H1 realized CP
    h1_realized_csv = _find_csv(metrics_dir, "half_realized_cp_by_ic")
    h1_realized_data, h1_realized_periods = _parse_wide_csv(h1_realized_csv) if h1_realized_csv else ({}, [])

    # 4. H1 renewal CP
    h1_renewal_csv = _find_csv(metrics_dir, "half_renewal_cp_by_ic")
    h1_renewal_data, h1_renewal_periods = _parse_wide_csv(h1_renewal_csv) if h1_renewal_csv else ({}, [])

    # 5. Card SQLs (monthly)
    card_sql_csv = _find_csv(metrics_dir, "card_sqls")
    card_sql_data, card_sql_periods = _parse_wide_csv(card_sql_csv) if card_sql_csv else ({}, [])

    # 6. Bill Pay SQLs (quarterly)
    bp_sql_csv = _find_csv(metrics_dir, "bill_pay_sqls")
    bp_sql_data, bp_sql_periods = _parse_wide_csv(bp_sql_csv) if bp_sql_csv else ({}, [])

    # 7. SaaS SQLs (quarterly)
    saas_sql_csv = _find_csv(metrics_dir, "free-to-paid_saas_sqls")
    saas_sql_data, saas_sql_periods = _parse_wide_csv(saas_sql_csv) if saas_sql_csv else ({}, [])

    # 8. CW CP by product
    card_cw_csv = _find_csv(metrics_dir, "card_$cw_cp") or _find_csv(metrics_dir, "card_cw_cp")
    card_cw_data, card_cw_periods = _parse_wide_csv(card_cw_csv) if card_cw_csv else ({}, [])

    bp_cw_csv = _find_csv(metrics_dir, "bill_pay_$cw_cp") or _find_csv(metrics_dir, "bill_pay_cw_cp")
    bp_cw_data, bp_cw_periods = _parse_wide_csv(bp_cw_csv) if bp_cw_csv else ({}, [])

    saas_cw_csv = _find_csv(metrics_dir, "free-to-paid_saas_cw_cp")
    saas_cw_data, saas_cw_periods = _parse_wide_csv(saas_cw_csv) if saas_cw_csv else ({}, [])

    # 9. CW Logos
    card_logo_csv = _find_csv(metrics_dir, "card_cw_logos")
    card_logo_data, card_logo_periods = _parse_wide_csv(card_logo_csv) if card_logo_csv else ({}, [])

    bp_logo_csv = _find_csv(metrics_dir, "bill_pay_cw_logos")
    bp_logo_data, bp_logo_periods = _parse_wide_csv(bp_logo_csv) if bp_logo_csv else ({}, [])

    saas_logo_csv = _find_csv(metrics_dir, "free-to-paid_saas_cw_logos")
    saas_logo_data, saas_logo_periods = _parse_wide_csv(saas_logo_csv) if saas_logo_csv else ({}, [])

    # 10. % of month elapsed
    elapsed_csv = _find_csv(metrics_dir, "__of_month_elapsed") or _find_csv(metrics_dir, "of_month_elapsed")
    month_elapsed = None
    if elapsed_csv and os.path.exists(elapsed_csv):
        with open(elapsed_csv, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            rows = list(reader)
            # CSV format: header row, then "1,54.55%" — grab the largest pct value
            # (skip small integers like row numbers)
            best_pct = None
            for row in rows:
                for cell in row:
                    pct = _parse_pct(cell)
                    if pct is not None and pct > 5:  # skip row numbers
                        if best_pct is None or pct > best_pct:
                            best_pct = pct
            month_elapsed = best_pct

    # ── Extract Greg's data ──────────────────────────────────────────────

    import pytz
    et = pytz.timezone(DISPLAY_TIMEZONE)
    now_et = datetime.now(et)
    date_str = now_et.strftime("%B %-d")
    current_month_label = now_et.strftime("%Y-%m")  # e.g. "2026-03"
    month_short = now_et.strftime("%b")  # e.g. "Mar"
    quarter_num = (now_et.month - 1) // 3 + 1
    current_quarter_label = f"{now_et.year}-Q{quarter_num}"  # e.g. "2026-Q1"

    # Determine the current period for realized CP
    # Look for the latest period that matches current month
    current_realized_period = None
    for p in realized_periods:
        if current_month_label in p or p.strip() == current_month_label:
            current_realized_period = p
            break
    # Fallback to last period if current month not found
    if not current_realized_period and realized_periods:
        current_realized_period = realized_periods[-1]

    greg_realized = realized_data.get(_owner_name, {})
    greg_renewal = renewal_data.get(_owner_name, {})

    # ── Build monthly realized CP section ────────────────────────────────
    lines = []
    lines.append(f":bar_chart: *Daily Quota Pulse -- {date_str}*")

    if month_elapsed is not None:
        lines.append(f"{_fmt_pct(month_elapsed)} of month elapsed")
    lines.append("")

    # Monthly Realized CP
    if current_realized_period and current_realized_period in greg_realized:
        rd = greg_realized[current_realized_period]
        total = _parse_dollar(rd.get("Total Realized CP", ""))
        goal = _parse_dollar(rd.get("Rep Total Realized CP Monthly Goal", ""))
        attain = _parse_pct(rd.get("% Attainment", ""))
        card = _parse_dollar(rd.get("Card CP", ""))
        bp = _parse_dollar(rd.get("Bill Pay CP", ""))
        travel = _parse_dollar(rd.get("Travel CP", ""))
        treasury = _parse_dollar(rd.get("Treasury CP", ""))
        f2p = _parse_dollar(rd.get("Free to Paid CP", ""))
        procurement = _parse_dollar(rd.get("Free to Paid Procurement CP", ""))

        attain_icon = _attainment_icon(attain)
        lines.append(
            f"*{month_short} Realized CP: {_fmt_dollar(total)} / {_fmt_dollar(goal)} "
            f"({_fmt_pct(attain)})*{attain_icon}"
        )

        # Team ranking
        rank, total_reps = _get_team_ranking(realized_data, current_realized_period, _owner_name)
        if rank is not None:
            if rank == 1:
                lines.append(":large_green_circle: #1 on team")
            elif rank <= 3:
                lines.append(f":large_yellow_circle: #{rank} on team")
            else:
                lines.append(f"#{rank} of {total_reps} on team")

        lines.append(
            f"Card {_fmt_dollar(card)} | BP {_fmt_dollar(bp)} | "
            f"Travel {_fmt_dollar(travel)} | Treasury {_fmt_dollar(treasury)}"
        )
        lines.append(f"F2P {_fmt_dollar(f2p)} | Procurement {_fmt_dollar(procurement)}")
    else:
        lines.append(f"*{month_short} Realized CP:* _data not available_")

    lines.append("")

    # Monthly Renewal CP
    current_renewal_period = None
    for p in renewal_periods:
        if current_month_label in p or p.strip() == current_month_label:
            current_renewal_period = p
            break
    if not current_renewal_period and renewal_periods:
        current_renewal_period = renewal_periods[-1]

    if current_renewal_period and current_renewal_period in greg_renewal:
        rn = greg_renewal[current_renewal_period]
        total = _parse_dollar(rn.get("Total Renewal CP", ""))
        goal = _parse_dollar(rn.get("Rep Total Renewal CP Monthly Goal", ""))
        attain = _parse_pct(rn.get("% Attainment", ""))
        renewal = _parse_dollar(rn.get("Renewal", ""))
        upsell = _parse_dollar(rn.get("Upsell", ""))

        attain_icon = _attainment_icon(attain)
        lines.append(
            f"*{month_short} Renewal CP: {_fmt_dollar(total)} / {_fmt_dollar(goal)} "
            f"({_fmt_pct(attain)})*{attain_icon}"
        )
        lines.append(f"Renewal {_fmt_dollar(renewal)} | Upsell {_fmt_dollar(upsell)}")
    else:
        lines.append(f"*{month_short} Renewal CP:* _data not available_")

    lines.append("")

    # H1 Progress
    greg_h1_realized = h1_realized_data.get(_owner_name, {})
    greg_h1_renewal = h1_renewal_data.get(_owner_name, {})
    h1_lines = []

    if greg_h1_realized:
        # Take the first (and likely only) period
        h1r_period = list(greg_h1_realized.keys())[0] if greg_h1_realized else None
        if h1r_period:
            h1r = greg_h1_realized[h1r_period]
            total = _parse_dollar(h1r.get("Total Realized CP", ""))
            quota = _parse_dollar(h1r.get("Rep Total Realized CP Quota", ""))
            attain = _parse_pct(h1r.get("% Attainment", ""))
            pace = "ahead" if attain and attain >= 50 else "behind" if attain else ""
            # More nuanced pace: compare attainment to expected pace
            # H1 is Jan-Jun, figure out what fraction of H1 we're through
            month_num = now_et.month
            if 1 <= month_num <= 6:
                expected_pct = ((month_num - 1) / 6) * 100
                if month_elapsed:
                    expected_pct += (month_elapsed / 100) * (100 / 6)
                if attain and attain >= expected_pct:
                    pace = "on pace" if attain < expected_pct + 10 else "ahead"
                elif attain:
                    pace = "behind"
            pace_str = f" -- {pace}" if pace else ""
            h1_lines.append(f"Realized: {_fmt_dollar(total)} / {_fmt_dollar(quota)} ({_fmt_pct(attain)}){pace_str}")

    if greg_h1_renewal:
        h1n_period = list(greg_h1_renewal.keys())[0] if greg_h1_renewal else None
        if h1n_period:
            h1n = greg_h1_renewal[h1n_period]
            total = _parse_dollar(h1n.get("Total Renewal CP", ""))
            quota = _parse_dollar(h1n.get("Rep Total Renewal CP Quota", ""))
            attain = _parse_pct(h1n.get("% Attainment", ""))
            month_num = now_et.month
            pace = ""
            if 1 <= month_num <= 6 and attain:
                expected_pct = ((month_num - 1) / 6) * 100
                if month_elapsed:
                    expected_pct += (month_elapsed / 100) * (100 / 6)
                if attain >= expected_pct:
                    pace = "on pace" if attain < expected_pct + 10 else "ahead"
                else:
                    pace = "behind"
            pace_str = f" -- {pace}" if pace else ""
            h1_lines.append(f"Renewal: {_fmt_dollar(total)} / {_fmt_dollar(quota)} ({_fmt_pct(attain)}){pace_str}")

    if h1_lines:
        lines.append("*H1 Progress*")
        lines.extend(h1_lines)
        lines.append("")

    # ── SQLs ─────────────────────────────────────────────────────────────
    sql_lines = []
    greg_card_sql = card_sql_data.get(_owner_name, {})
    greg_bp_sql = bp_sql_data.get(_owner_name, {})
    greg_saas_sql = saas_sql_data.get(_owner_name, {})

    # Card SQLs (monthly) — find current month
    card_sql_period = None
    for p in card_sql_periods:
        if current_month_label in p:
            card_sql_period = p
            break
    if not card_sql_period and card_sql_periods:
        card_sql_period = card_sql_periods[-1]

    if card_sql_period and card_sql_period in greg_card_sql:
        cs = greg_card_sql[card_sql_period]
        total_opp = _parse_dollar(cs.get("Total Opp.", ""))
        goal = _parse_dollar(cs.get("Rep Card Expansion SQL Monthly Goal", ""))
        attain = _parse_pct(cs.get("% Attainment", ""))
        flag = _flag_icon(attain)
        needs_attn = " -- needs attention" if attain is not None and attain < 50 else ""
        sql_lines.append(f"{flag}Card: {total_opp:.0f} / {goal:.0f} ({_fmt_pct(attain)}){needs_attn}")

    # Bill Pay SQLs
    bp_sql_period = _latest_period_with_data(bp_sql_data, _owner_name, bp_sql_periods, current_month_label)
    if bp_sql_period and bp_sql_period in greg_bp_sql:
        bs = greg_bp_sql[bp_sql_period]
        total_opp = _parse_dollar(bs.get("Total Opp.", ""))
        goal_val = ""
        for k in bs.keys():
            if "Goal" in k:
                goal_val = bs[k]
                break
        goal = _parse_dollar(goal_val)
        attain = _parse_pct(bs.get("% Attainment", ""))
        period_label = _short_period(bp_sql_period)
        sql_lines.append(f"BP ({period_label}): {total_opp:.0f} / {goal:.0f} ({_fmt_pct(attain)})")

    # SaaS SQLs
    saas_sql_period = _latest_period_with_data(saas_sql_data, _owner_name, saas_sql_periods, current_month_label)
    if saas_sql_period and saas_sql_period in greg_saas_sql:
        ss = greg_saas_sql[saas_sql_period]
        total_opp = _parse_dollar(ss.get("Total Opp.", ""))
        goal_val = ""
        for k in ss.keys():
            if "Goal" in k:
                goal_val = ss[k]
                break
        goal = _parse_dollar(goal_val)
        attain = _parse_pct(ss.get("% Attainment", ""))
        period_label = _short_period(saas_sql_period)
        sql_lines.append(f"SaaS ({period_label}): {total_opp:.0f} / {goal:.0f} ({_fmt_pct(attain)})")

    if sql_lines:
        lines.append("*SQLs*")
        lines.extend(sql_lines)
        lines.append("")

    # ── CW CP ────────────────────────────────────────────────────────────
    cw_lines = []
    greg_card_cw = card_cw_data.get(_owner_name, {})
    greg_bp_cw = bp_cw_data.get(_owner_name, {})
    greg_saas_cw = saas_cw_data.get(_owner_name, {})

    def _extract_cw(data_dict, periods, label, hint=""):
        if not data_dict or not periods:
            return None
        period = _latest_period_with_data(data_dict, _owner_name, periods, hint)
        if not period or period not in data_dict.get(_owner_name, {}):
            return None
        d = data_dict[_owner_name][period]
        # Look for total/CP column and goal column
        total_val = 0.0
        goal_val = 0.0
        attain_val = None
        for k, v in d.items():
            kl = k.lower()
            if "% attainment" in kl or "attainment" in kl:
                attain_val = _parse_pct(v)
            elif "goal" in kl or "quota" in kl:
                goal_val = _parse_dollar(v)
            elif "total" in kl or "cp" in kl:
                total_val = _parse_dollar(v)
        return period.strip(), total_val, goal_val, attain_val

    card_cw = _extract_cw(card_cw_data, card_cw_periods, "Card", current_quarter_label)
    if card_cw:
        p, t, g, a = card_cw
        icon = _attainment_icon(a)
        cw_lines.append(f"Card ({_short_period(p)}): {_fmt_dollar(t)} / {_fmt_dollar(g)} ({_fmt_pct(a)}){icon}")

    bp_cw = _extract_cw(bp_cw_data, bp_cw_periods, "BP", current_quarter_label)
    if bp_cw:
        p, t, g, a = bp_cw
        icon = _attainment_icon(a)
        cw_lines.append(f"BP ({_short_period(p)}): {_fmt_dollar(t)} / {_fmt_dollar(g)} ({_fmt_pct(a)}){icon}")

    saas_cw = _extract_cw(saas_cw_data, saas_cw_periods, "SaaS", current_month_label)
    if saas_cw:
        p, t, g, a = saas_cw
        icon = _attainment_icon(a)
        cw_lines.append(f"SaaS ({_short_period(p)}): {_fmt_dollar(t)} / {_fmt_dollar(g)} ({_fmt_pct(a)}){icon}")

    if cw_lines:
        lines.append("*CW CP*")
        lines.extend(cw_lines)
        lines.append("")

    # ── CW Logos ─────────────────────────────────────────────────────────
    logo_parts = []
    greg_card_logo = card_logo_data.get(_owner_name, {})
    greg_bp_logo = bp_logo_data.get(_owner_name, {})
    greg_saas_logo = saas_logo_data.get(_owner_name, {})

    def _extract_logo(data_dict, periods, label, hint=""):
        if not data_dict or not periods:
            return None
        period = _latest_period_with_data(data_dict, _owner_name, periods, hint)
        if not period or period not in data_dict.get(_owner_name, {}):
            return None
        d = data_dict[_owner_name][period]
        total_val = 0.0
        goal_val = 0.0
        attain_val = None
        for k, v in d.items():
            kl = k.lower()
            if "% attainment" in kl or "attainment" in kl:
                attain_val = _parse_pct(v)
            elif "goal" in kl or "quota" in kl:
                goal_val = _parse_dollar(v)
            elif "total" in kl or "logo" in kl or "cw" in kl:
                total_val = _parse_dollar(v)
        return label, total_val, goal_val, attain_val

    card_l = _extract_logo(card_logo_data, card_logo_periods, "Card", current_month_label)
    bp_l = _extract_logo(bp_logo_data, bp_logo_periods, "BP", current_month_label)
    saas_l = _extract_logo(saas_logo_data, saas_logo_periods, "SaaS", current_month_label)

    for item in [card_l, bp_l, saas_l]:
        if item:
            label, t, g, a = item
            logo_parts.append(f"{label}: {t:.0f} / {g:.0f} ({_fmt_pct(a)})")

    if logo_parts:
        lines.append(f"*CW Logos ({month_short})*")
        lines.append(" | ".join(logo_parts))
        lines.append("")

    # ── Month-over-Month Realized CP ─────────────────────────────────────
    if greg_realized and len(realized_periods) > 1:
        mom_parts = []
        for p in realized_periods:
            if p in greg_realized:
                rd = greg_realized[p]
                total = _parse_dollar(rd.get("Total Realized CP", ""))
                attain = _parse_pct(rd.get("% Attainment", ""))
                if total > 0:
                    # Extract short month label from period
                    try:
                        period_date = datetime.strptime(p.strip(), "%Y-%m")
                        month_label = period_date.strftime("%b")
                    except ValueError:
                        month_label = p.strip()
                    mom_parts.append(f"{month_label}: {_fmt_dollar(total)} ({_fmt_pct(attain)})")

        if mom_parts:
            lines.append("*Month-over-Month Realized CP*")
            lines.append(" :arrow_right: ".join(mom_parts))
            lines.append("")

    # ── Flags ────────────────────────────────────────────────────────────
    flags = []
    threshold_elapsed = 40.0  # only flag if month is >40% elapsed
    if month_elapsed and month_elapsed > threshold_elapsed:
        # Check card SQLs
        if card_sql_period and card_sql_period in greg_card_sql:
            cs = greg_card_sql[card_sql_period]
            attain = _parse_pct(cs.get("% Attainment", ""))
            if attain is not None and attain < 50:
                # Check previous month for comparison
                prev_attain_str = ""
                if len(card_sql_periods) >= 2:
                    prev_period = card_sql_periods[-2] if card_sql_period == card_sql_periods[-1] else card_sql_periods[-1]
                    if prev_period in greg_card_sql:
                        prev_a = _parse_pct(greg_card_sql[prev_period].get("% Attainment", ""))
                        if prev_a is not None:
                            prev_attain_str = f" -- was {_fmt_pct(prev_a)} last month"
                flags.append(f":warning: Card SQLs critically low ({_fmt_pct(attain)}){prev_attain_str}")

        # Check all attainment metrics for low values
        def _check_metric(label, data_dict, period, metric_name="% Attainment"):
            if not data_dict or not period:
                return
            greg_d = data_dict.get(_owner_name, {})
            if period in greg_d:
                attain = _parse_pct(greg_d[period].get(metric_name, ""))
                if attain is not None and attain < 50:
                    flags.append(f":warning: {label} below 50% ({_fmt_pct(attain)})")

        _check_metric(f"{month_short} Renewal CP", renewal_data, current_renewal_period)

    if flags:
        lines.append("*Flags*")
        lines.extend(flags)

    # ── Send DM ──────────────────────────────────────────────────────────
    message = "\n".join(lines)

    try:
        client.chat_postMessage(
            channel=dm_target,
            text=message,
            mrkdwn=True,
        )
        logger.info("Quota insights: DM sent successfully")
    except Exception as e:
        logger.error("Quota insights: failed to send DM: %s", e)
