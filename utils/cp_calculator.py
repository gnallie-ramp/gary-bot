"""Realized CP (Compensation Points) calculator.

Ported from ``app.py`` ``compute_realized_cp`` (lines 2326-2441).

For expansion products (Card, Bill Pay, Treasury, Travel):
    CP = NTR * max(0, window_spend - baseline)  over three 30-day windows.

For SaaS / Procurement:
    CP = monthly_expansion_amount * multiplier  (one-time at close).
"""
from __future__ import annotations

import logging

import pandas as pd

from config import NTR_RATES

logger = logging.getLogger(__name__)


def compute_realized_cp(df: pd.DataFrame, manual_overrides=None) -> pd.DataFrame:
    """Compute realized CP from raw opportunity data.

    Parameters
    ----------
    df : pd.DataFrame
        Opportunity rows with columns such as ``expansion_subtype``,
        ``cw_date``, ``baseline_at_close``, ``spend_d1_d30``,
        ``spend_d31_d60``, ``spend_d61_d90``, ``current_l30d``,
        ``monthly_expansion_amount``, ``account_name``, ``opportunity_name``,
        ``opportunity_id``, ``account_id``.
    manual_overrides : list[dict] | None
        Optional extra rows injected as manual overrides.

    Returns
    -------
    pd.DataFrame
        One row per opportunity with columns: ``account_name``,
        ``opportunity_name``, ``product``, ``cw_date``, ``baseline``,
        ``current_l30d``, ``delta``, ``cp_earned``, ``cp_locked``,
        ``cp_accruing``, ``window_status``, ``days_remaining``,
        ``days_since_cw``, ``cp_by_month``, ``opportunity_id``,
        ``account_id``, ``is_travel``.
    """
    if df.empty and not manual_overrides:
        return pd.DataFrame()

    working_df = df.copy() if not df.empty else pd.DataFrame()

    # Inject manual overrides as extra rows
    if manual_overrides:
        override_df = pd.DataFrame(manual_overrides)
        working_df = pd.concat([working_df, override_df], ignore_index=True)

    if working_df.empty:
        return pd.DataFrame()

    today = pd.Timestamp.now().normalize()
    results: list[dict] = []

    for _, row in working_df.iterrows():
        subtype = str(row.get("expansion_subtype", "")).strip()
        cw_date = pd.to_datetime(row["cw_date"])
        days_since = (today - cw_date).days

        if subtype in NTR_RATES:
            # ── Expansion product: window-based CP ────────────────────────
            ntr = NTR_RATES[subtype]
            baseline = float(row.get("baseline_at_close", 0) or 0)

            windows = [
                ("D1-D30", float(row.get("spend_d1_d30", 0) or 0), 30),
                ("D31-D60", float(row.get("spend_d31_d60", 0) or 0), 60),
                ("D61-D90", float(row.get("spend_d61_d90", 0) or 0), 90),
            ]

            total_cp = 0.0
            locked_cp = 0.0
            accruing_cp = 0.0
            window_status = "Pending"
            days_remaining = 0
            cp_by_month: dict[str, float] = {}
            # Attribute ALL CP to the close month
            month_key = cw_date.strftime("%Y-%m")

            for wname, wspend, wend_day in windows:
                window_start_day = wend_day - 29  # 1, 31, 61
                window_cp = ntr * max(0, wspend - baseline)

                if days_since >= wend_day:
                    # Window fully complete -- locked
                    locked_cp += window_cp
                    total_cp += window_cp
                    window_status = f"{wname} complete"
                    cp_by_month[month_key] = cp_by_month.get(month_key, 0) + window_cp
                elif days_since >= window_start_day:
                    # Currently in this window -- accruing
                    accruing_cp = window_cp
                    total_cp += window_cp
                    window_status = f"{wname} accruing"
                    days_remaining = wend_day - days_since
                    cp_by_month[month_key] = cp_by_month.get(month_key, 0) + window_cp
                    break
                else:
                    break

            results.append({
                "account_name": row.get("account_name", ""),
                "opportunity_name": row.get("opportunity_name", ""),
                "product": subtype,
                "cw_date": cw_date,
                "baseline": baseline,
                "current_l30d": float(row.get("current_l30d", 0) or 0),
                "delta": max(0, float(row.get("current_l30d", 0) or 0) - baseline),
                "cp_earned": round(total_cp, 2),
                "cp_locked": round(locked_cp, 2),
                "cp_accruing": round(accruing_cp, 2),
                "window_status": window_status,
                "days_remaining": days_remaining,
                "days_since_cw": days_since,
                "cp_by_month": cp_by_month,
                "opportunity_id": row.get("opportunity_id"),
                "account_id": row.get("account_id"),
                "is_travel": subtype == "Travel Expansion",
            })
        else:
            # ── SaaS / Procurement: one-time CP at close ──────────────────
            acv = float(row.get("monthly_expansion_amount", 0) or 0)

            if "procurement" in subtype.lower():
                multiplier = 1.00
            else:
                multiplier = 0.75  # SaaS FTP default

            cp_val = acv * multiplier
            month_key = cw_date.strftime("%Y-%m")

            results.append({
                "account_name": row.get("account_name", ""),
                "opportunity_name": row.get("opportunity_name", ""),
                "product": subtype,
                "cw_date": cw_date,
                "baseline": 0,
                "current_l30d": 0,
                "delta": 0,
                "cp_earned": round(cp_val, 2),
                "cp_locked": round(cp_val, 2),
                "cp_accruing": 0,
                "window_status": "Locked at close",
                "days_remaining": 0,
                "days_since_cw": days_since,
                "cp_by_month": {month_key: cp_val},
                "opportunity_id": row.get("opportunity_id"),
                "account_id": row.get("account_id"),
                "is_travel": False,
            })

    return pd.DataFrame(results) if results else pd.DataFrame()
