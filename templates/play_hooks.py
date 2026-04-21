"""Per-play prompt hooks for the Plays tab's Draft Re-engage action.

Each entry customizes the email a Ramp Growth AM sends when drafting from a
specific play card. The hook:
  - Provides a `play_context` block prepended to Claude's user prompt —
    orients the email around the specific signal Greg is pitching.
  - Tells the drafter which Ramp product to anchor the subject line on
    (`pitched_product`).
  - Exposes `build_synthetic_opp(row)` so the drafter has a stand-in opp
    when the account has no open SFDC expansion opportunity on the pitched
    product (true for most P1 / P5 / P13 accounts).

Adding a new play: register a new entry in PLAY_HOOKS keyed by play_id.
"""
from __future__ import annotations


def _fmt_money(x) -> str:
    try:
        v = float(x)
        if v != v:  # NaN
            return "$0"
        if v == 0:
            return "$0"
        return f"${v:,.0f}"
    except (TypeError, ValueError):
        return "$0"


# ── P1: Plus-gated ERP ──────────────────────────────────────────────────────
def _p1_context(row: dict) -> str:
    erp = row.get("erp_snippet") or ""
    # Pull out the first product name from the JSON-ish snippet for cleaner copy
    erp_name = "their ERP"
    for needle in ["NetSuite", "Sage Intacct", "Acumatica", "Microsoft Dynamics",
                   "Zoho", "Oracle Enterprise", "Sage 50", "Intacct"]:
        if needle.lower() in erp.lower():
            erp_name = needle
            break
    tier = row.get("subscription_tier") or "free"
    gla = _fmt_money(row.get("current_gla"))
    est_cp = _fmt_money(row.get("est_card_cp_monthly"))
    return (
        f"PLAY: P1 — Plus-gated ERP upsell. This account is running {erp_name} "
        f"on Ramp's legacy/free tier ({tier}), with {gla} in connected bank accounts. "
        f"Their estimated Card CP upside on Plus is {est_cp}/mo.\n"
        f"PITCH ANGLE: Lead Takeaways with the specific Plus features that ONLY "
        f"unlock on Plus and specifically pair with {erp_name} — custom fields "
        f"that sync to {erp_name} dimensions, advanced approval policies for "
        f"{erp_name} journal entries, SAML SSO for broader user rollout, and "
        f"AI agents that auto-code transactions into {erp_name} GL accounts. Do NOT "
        f"mention dollar figures from GLA or CP — those are internal metrics, "
        f"not talking points."
    )


def _p1_synth(row: dict) -> dict:
    return {"product": "Plus", "type": "Prospecting - Plus", "stage": "S0",
            "monthly_amount": 0, "next_step": "", "expansion_notes": "",
            "est_cp": row.get("est_card_cp_monthly") or 0}


# ── P5: PO-in-memo → Procurement ────────────────────────────────────────────
def _p5_context(row: dict) -> str:
    n = int(row.get("po_bill_count") or 0)
    sample = (row.get("sample_memo") or "").strip()[:120]
    bp = _fmt_money(row.get("bp_l30d"))
    return (
        f"PLAY: P5 — PO-in-memo → Procurement. This account has paid {n} bills "
        f"in the last 90 days where the memo or invoice number references a PO "
        f"(sample memo: \"{sample}\"). Monthly BP volume: {bp}. They're NOT on "
        f"the Procurement Add-on.\n"
        f"PITCH ANGLE: Lead Takeaways with the specific pain of running a PO "
        f"workflow in a spreadsheet — manual PO creation, no 3-way match, no "
        f"approval routing tied to the PO. Pair each with a Procurement Add-on "
        f"feature: spend intake forms that auto-generate POs, 3-way match on "
        f"bill import, pre-approval tied to PO, renewal tracking for recurring "
        f"vendors. Mention the specific {n}-bills-with-PO-memos signal as "
        f"evidence they're already running a shadow procurement process."
    )


def _p5_synth(row: dict) -> dict:
    return {"product": "Procurement", "type": "Prospecting - Procurement",
            "stage": "S0", "monthly_amount": 0, "next_step": "",
            "expansion_notes": f"Shadow PO workflow: {row.get('po_bill_count')} bills L90D reference POs in memo",
            "est_cp": 0}


# ── P7: Just started Bill Pay ────────────────────────────────────────────────
def _p7_context(row: dict) -> str:
    first_bill = row.get("first_bill_paid_at") or "recently"
    bp = _fmt_money(row.get("bp_l30d"))
    bp_count = row.get("bp_count_l30d") or 0
    return (
        f"PLAY: P7 — Just started Bill Pay. Paid first bill on {first_bill}. "
        f"Last 30 days: {bp_count} bills, {bp}. No CW BP opp in their history, "
        f"so this is a fresh activation window — baseline spend is still "
        f"forming.\n"
        f"PITCH ANGLE: Congratulate on getting started and lead with adoption "
        f"levers: bulk payment batching, approval chain setup, ERP sync "
        f"configuration, vendor onboarding tips, and recurring-bill automation. "
        f"Position a 15-min BP kickoff / onboarding review as the next step — "
        f"this is where they'll get the most value early. Do NOT push anything "
        f"paid-tier-gated unless they're also on a Plus-candidate signal."
    )


def _p7_synth(row: dict) -> dict:
    return {"product": "Bill Pay", "type": "Activation - Bill Pay",
            "stage": "S0", "monthly_amount": row.get("bp_l30d") or 0,
            "next_step": "", "expansion_notes": "Just started BP, no CW BP opp yet",
            "est_cp": row.get("est_bp_cp_monthly") or 0}


# ── P9: New-sale activation gap ─────────────────────────────────────────────
def _p9_context(row: dict) -> str:
    card_sold = _fmt_money(row.get("card_sold_monthly"))
    card_now = _fmt_money(row.get("card_l30d"))
    bp_sold = _fmt_money(row.get("bp_sold_monthly"))
    bp_now = _fmt_money(row.get("bp_l30d"))
    return (
        f"PLAY: P9 — New-sale activation gap. SFDC flagged this account as "
        f"having an activation gap vs. what was sold. Sold: {card_sold}/mo card + "
        f"{bp_sold}/mo bill pay. Current: {card_now}/mo card + {bp_now}/mo bill pay.\n"
        f"PITCH ANGLE: Check in on their setup to make sure they're getting full "
        f"value from what they signed up for. Lead Takeaways by naming likely "
        f"adoption blockers tied to new-sale commitments — ERP integration not "
        f"connected, approval workflows not live, users not invited, bulk "
        f"batching not configured. Each Takeaway = likely blocker + the Ramp "
        f"feature or implementation step that unblocks it. Close with a "
        f"15-min activation review working session. Never imply they're "
        f"underperforming — frame as \"quick check-in to make sure you're "
        f"getting what you paid for.\""
    )


def _p9_synth(row: dict) -> dict:
    # Product depends on which gap is bigger
    card_gap = float(row.get("card_gap_cp") or 0)
    bp_gap = float(row.get("bp_gap_usd") or 0)
    product = "Card" if card_gap >= bp_gap * 0.0015 else "Bill Pay"
    return {"product": product, "type": "Activation - new sale gap",
            "stage": "Activation", "monthly_amount": 0, "next_step": "",
            "expansion_notes": "Current run rate below new-sale projections",
            "est_cp": card_gap}


# ── P13: Top spenders w/ product gap ────────────────────────────────────────
def _p13_context(row: dict) -> str:
    gaps = row.get("gaps") or ""
    card = _fmt_money(row.get("card_l30d"))
    bp = _fmt_money(row.get("bp_l30d"))
    gla = _fmt_money(row.get("gla"))
    # Translate gap key -> pitched product
    if "heavy-card-gap" in gaps:
        product_line = (
            f"They're a heavy card user ({card}/mo) without Procurement or Plus. "
            f"Pitch Procurement as the next logical product — their card volume "
            f"implies significant vendor/SaaS spend that Procurement would "
            f"track and approve."
        )
    elif "heavy-bp-gap" in gaps:
        product_line = (
            f"They're a heavy Bill Pay user ({bp}/mo) and not on Plus. Pitch "
            f"Plus — AI coding + advanced policies would transform their AP "
            f"workflow at this volume."
        )
    elif "large-gla-gap" in gaps:
        product_line = (
            f"They hold {gla} in connected bank accounts and don't have "
            f"Treasury active. Pitch Treasury yield — even partial capital "
            f"migration would unlock meaningful annual yield."
        )
    else:
        product_line = "Pitch the adjacent product they're missing based on their spend profile."
    return (
        f"PLAY: P13 — Top spenders w/ product gap. Gap signal: {gaps}. "
        f"Card: {card}/mo · BP: {bp}/mo · GLA: {gla}.\n"
        f"PITCH ANGLE: {product_line} Lead the Takeaways with the specific "
        f"pain their volume implies (manual tracking, lost yield, uncaptured "
        f"savings). Never quote the internal dollar figures directly; reference "
        f"their spend pattern in general terms (\"your current Bill Pay "
        f"volume\", \"your cash position\")."
    )


def _p13_synth(row: dict) -> dict:
    gaps = row.get("gaps") or ""
    if "heavy-card-gap" in gaps:
        product = "Procurement"
    elif "heavy-bp-gap" in gaps:
        product = "Plus"
    elif "large-gla-gap" in gaps:
        product = "Treasury"
    else:
        product = "Plus"
    return {"product": product, "type": f"Prospecting - {product}",
            "stage": "S0", "monthly_amount": 0, "next_step": "",
            "expansion_notes": f"Top spender cross-sell: gap={gaps}",
            "est_cp": 0}


# ── P2: Plus-feature trialer (didn't convert) ───────────────────────────────
def _p2_context(row: dict) -> str:
    status = row.get("plus_product_status_v2") or "trialed but did not convert"
    card = _fmt_money(row.get("card_l30d"))
    est_cp = _fmt_money(row.get("est_card_cp_monthly"))
    users = int(row.get("user_count") or 0)
    return (
        f"PLAY: P2 — Plus-feature trialer. This account had Plus in trial "
        f"and chose not to convert (status: {status}). Current card spend: "
        f"{card}/mo · {users} users · est Plus CP upside: {est_cp}/mo.\n"
        f"PITCH ANGLE: They've already seen the product, so skip the demo "
        f"intro. Lead Takeaways by addressing what likely killed the trial "
        f"(price perception, specific unused features, bad timing) and "
        f"re-open with one concrete Plus feature that matches their current "
        f"pattern — AI coding for high-volume card spenders, advanced "
        f"policies for large teams, SSO/SCIM for 20+ user rollouts. Close "
        f"with a 15-min feature-specific walk-through, not a full demo. Be "
        f"direct: \"Last time you trialed, X. Since then Y has shipped — "
        f"worth 15 min to revisit?\""
    )


def _p2_synth(row: dict) -> dict:
    return {"product": "Plus", "type": "Re-trial - Plus", "stage": "S0",
            "monthly_amount": 0, "next_step": "",
            "expansion_notes": f"Previously trialed Plus, did not convert ({row.get('plus_product_status_v2', '')})",
            "est_cp": row.get("est_card_cp_monthly") or 0}


# ── P6: Legacy Procurement → Add-on ─────────────────────────────────────────
def _p6_context(row: dict) -> str:
    ltd = int(row.get("ltd_pos") or 0)
    l90 = int(row.get("pos_l90d") or 0)
    bp = _fmt_money(row.get("bp_l30d"))
    return (
        f"PLAY: P6 — Legacy Procurement → Add-on upgrade. This account is "
        f"on the PROCUREMENT_LEGACY tier with {ltd} lifetime POs created "
        f"({l90} in the last 90 days). Monthly BP: {bp}.\n"
        f"PITCH ANGLE: This is an UPGRADE conversation, not a new-product "
        f"pitch. They already know how Ramp Procurement works. Lead "
        f"Takeaways with what the modern Add-on adds vs. their legacy tier "
        f"— customizable intake forms, 3-way match on bill import, contract "
        f"renewal tracking, AI-assisted sourcing, vendor-portal, approval "
        f"chains tied to spend programs. Reference their PO volume ({ltd} "
        f"POs) as evidence this investment pays off. Close with a 30-min "
        f"migration planning session."
    )


def _p6_synth(row: dict) -> dict:
    return {"product": "Procurement", "type": "Upgrade - Procurement Add-on",
            "stage": "S0", "monthly_amount": 0, "next_step": "",
            "expansion_notes": f"On PROCUREMENT_LEGACY, {int(row.get('ltd_pos') or 0)} lifetime POs",
            "est_cp": 0}


# ── P8: Competitor AP migration ─────────────────────────────────────────────
def _p8_context(row: dict) -> str:
    off = _fmt_money(row.get("off_ramp_bp_monthly"))
    on = _fmt_money(row.get("ramp_bp_l30d"))
    comp = row.get("bp_competitor") or "(unnamed)"
    comp_str = comp if comp not in ("(unnamed)", None) else "a non-Ramp AP tool"
    return (
        f"PLAY: P8 — Competitor AP migration. This account routes {off}/mo "
        f"in bill pay through {comp_str}, while only running {on}/mo through "
        f"Ramp. The off-Ramp volume is at least 3× larger than Ramp BP.\n"
        f"PITCH ANGLE: Lead with a migration pitch, not a feature pitch. "
        f"They've already built an AP workflow somewhere else. Takeaways "
        f"should focus on (1) the mechanics of migration (bulk vendor "
        f"import, ACH/check/card on one platform, existing approval flows "
        f"preserved), (2) the cashback math — at {off}/mo in BP, switching "
        f"to card-payable via Ramp unlocks 0.15% = meaningful annual "
        f"savings, (3) consolidation — one login for card + BP + reporting. "
        f"If the competitor is named ({comp}), reference the specific "
        f"integration risk or pain point. Close with a 30-min migration "
        f"scoping call."
    )


def _p8_synth(row: dict) -> dict:
    return {"product": "Bill Pay", "type": "Migration - Bill Pay",
            "stage": "S0", "monthly_amount": row.get("off_ramp_bp_monthly") or 0,
            "next_step": "",
            "expansion_notes": f"Competitor AP migration: ${int(row.get('off_ramp_bp_monthly') or 0):,}/mo off-Ramp",
            "est_cp": row.get("est_bp_cp_monthly") or 0}


# ── P11: Intl-heavy enterprise (Wise-onboarded) ─────────────────────────────
def _p11_context(row: dict) -> str:
    users = int(row.get("user_count") or 0)
    wise = row.get("wise_onboarded_at") or "some time ago"
    country = row.get("business_office_country") or "the US"
    return (
        f"PLAY: P11 — Intl-heavy enterprise. This account has {users} users, "
        f"is HQ'd in {country}, and was onboarded to Wise for international "
        f"payments on {wise} — meaning they're already sending intl payments "
        f"at scale.\n"
        f"PITCH ANGLE: Lead Takeaways with the specific Plus features that "
        f"matter for teams running intl operations: per-entity controls "
        f"(separate policies per geography), SAML SSO/SCIM for large user "
        f"counts, advanced policies for multi-currency spend, granular "
        f"reporting by entity/country, and dedicated support for enterprise "
        f"implementations. Reference their {users} users as evidence they're "
        f"at the scale where Plus's admin features pay back immediately. "
        f"Close with a Plus/multi-entity walk-through (30-min)."
    )


def _p11_synth(row: dict) -> dict:
    return {"product": "Plus", "type": "Prospecting - Plus (Intl)",
            "stage": "S0", "monthly_amount": 0, "next_step": "",
            "expansion_notes": f"{int(row.get('user_count') or 0)} users, Wise-onboarded {row.get('wise_onboarded_at', '?')}",
            "est_cp": row.get("est_card_cp_monthly") or 0}


# ── P12: Treasury opp (GLA >$5M, not on Treasury) ───────────────────────────
def _p12_context(row: dict) -> str:
    gla = _fmt_money(row.get("current_gla"))
    monthly_yield = _fmt_money(row.get("implied_monthly_yield_at_4_5pct"))
    bp = _fmt_money(row.get("bp_l30d"))
    return (
        f"PLAY: P12 — Treasury opp. This account holds {gla} in connected "
        f"bank accounts, currently earning 0% (not on Ramp Treasury). At a "
        f"4.5% yield, that's ~{monthly_yield}/mo in recovered revenue. "
        f"Monthly BP: {bp} — they have active cash flow too.\n"
        f"PITCH ANGLE: Lead Takeaways with the specific yield math — name "
        f"the dollar figure their idle cash is leaving on the table. Pair "
        f"it with Treasury's no-lockup liquidity (same-day transfers, "
        f"FDIC-pass-through, same interface as their existing Ramp). "
        f"Address likely objections proactively: 'no lockup', 'move what "
        f"you want, when you want, no minimums', 'FDIC-backed'. For "
        f"accounts with heavy BP, call out the operational win of "
        f"consolidating AP + yield in one platform. Close with a 15-min "
        f"Treasury onboarding walk-through."
    )


def _p12_synth(row: dict) -> dict:
    return {"product": "Treasury", "type": "Prospecting - Treasury",
            "stage": "S0", "monthly_amount": 0, "next_step": "",
            "expansion_notes": f"GLA ${int(row.get('current_gla') or 0):,} idle, not on Treasury",
            "est_cp": 0}


PLAY_HOOKS = {
    "P1":  {"context_fn": _p1_context,  "synth_fn": _p1_synth,  "pitched_product": "Plus"},
    "P2":  {"context_fn": _p2_context,  "synth_fn": _p2_synth,  "pitched_product": "Plus"},
    "P5":  {"context_fn": _p5_context,  "synth_fn": _p5_synth,  "pitched_product": "Procurement"},
    "P6":  {"context_fn": _p6_context,  "synth_fn": _p6_synth,  "pitched_product": "Procurement"},
    "P7":  {"context_fn": _p7_context,  "synth_fn": _p7_synth,  "pitched_product": "Bill Pay"},
    "P8":  {"context_fn": _p8_context,  "synth_fn": _p8_synth,  "pitched_product": "Bill Pay"},
    "P9":  {"context_fn": _p9_context,  "synth_fn": _p9_synth,  "pitched_product": None},  # derived from row
    "P11": {"context_fn": _p11_context, "synth_fn": _p11_synth, "pitched_product": "Plus"},
    "P12": {"context_fn": _p12_context, "synth_fn": _p12_synth, "pitched_product": "Treasury"},
    "P13": {"context_fn": _p13_context, "synth_fn": _p13_synth, "pitched_product": None},  # derived from row
}


def get_play_context(play_id: str, play_row: dict) -> str:
    """Return the prompt-ready `PLAY CONTEXT` string for this play + row,
    or empty string if no hook is registered."""
    hook = PLAY_HOOKS.get(play_id)
    if not hook or not play_row:
        return ""
    try:
        return hook["context_fn"](play_row)
    except Exception:
        return ""


def get_synthetic_opp(play_id: str, play_row: dict) -> dict:
    """Return a synthetic opp dict that stands in for the pitched product
    so `pipeline_drafter` has something concrete to structure the email
    around. Returns empty dict if no hook registered."""
    hook = PLAY_HOOKS.get(play_id)
    if not hook or not play_row:
        return {}
    try:
        return hook["synth_fn"](play_row)
    except Exception:
        return {}


def get_pitched_product(play_id: str, play_row: dict = None) -> str:
    """Return the Ramp product name to use in the email subject line."""
    hook = PLAY_HOOKS.get(play_id)
    if not hook:
        return ""
    fixed = hook.get("pitched_product")
    if fixed:
        return fixed
    # Derived product — get from synthetic opp builder
    if play_row:
        synth = get_synthetic_opp(play_id, play_row)
        return synth.get("product", "") or ""
    return ""
