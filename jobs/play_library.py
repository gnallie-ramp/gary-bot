"""Play Pattern Library — aggregates Deal Anatomy JSON across all analyzed
CW deals and extracts what consistently works per play.

Output (`~/.gary_bot_play_library.json`):
{
  "built_at": "2026-04-21T20:00:00Z",
  "source_deals_analyzed": 47,
  "tags": {
    "plus_upgrade": {
      "deal_count": 12,
      "total_realized_cp": 124000,
      "avg_realized_cp": 10333,
      "top_pain_themes": [{"theme": "manual coding", "count": 7, "representative_quote": "..."}],
      "top_features": [{"feature": "AI coding", "count": 10}, ...],
      "top_champion_roles": [{"role": "Controller", "count": 5}],
      "top_framings": [{"framing": "ROI / hours saved", "count": 8}],
      "sample_winning_moves": ["quote 1", "quote 2", "quote 3"],
      "winning_phrases": [{"phrase": "custom fields", "count": 6}]
    },
    "procurement_upgrade": {...}
    ...
  }
}

Run via `jobs.play_library.rebuild()` on-demand or on a weekly schedule.
Reads all JSON files under `~/.gary_bot_deal_anatomy/`.
"""
from __future__ import annotations

import json
import logging
import os
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

ANATOMY_DIR = Path.home() / ".gary_bot_deal_anatomy"
LIBRARY_FILE = Path.home() / ".gary_bot_play_library.json"


def _load_all_anatomies() -> list[dict]:
    """Load every analyzed-deal JSON from ANATOMY_DIR. Skips malformed files."""
    out = []
    if not ANATOMY_DIR.exists():
        return out
    for path in ANATOMY_DIR.glob("*.json"):
        try:
            data = json.loads(path.read_text())
            # Skip error-only records
            if "winning_move" not in data:
                continue
            out.append(data)
        except Exception as e:
            logger.debug("Skipping malformed anatomy %s: %s", path.name, e)
    return out


def _aggregate_tag(deals: list[dict]) -> dict:
    """Build the per-tag rollup dict from a list of anatomies."""
    deal_count = len(deals)
    total_cp = sum(int(d.get("realized_cp") or 0) for d in deals)
    avg_cp = total_cp // deal_count if deal_count else 0

    # Pain-point themes — count by theme + pick representative quote
    theme_counts: Counter = Counter()
    theme_quotes: dict[str, str] = {}
    for d in deals:
        for pp in d.get("pain_points") or []:
            theme = (pp.get("theme") or "").strip().lower()
            if not theme:
                continue
            theme_counts[theme] += 1
            if theme not in theme_quotes:
                theme_quotes[theme] = (pp.get("quote") or "")[:180]

    top_pain_themes = [
        {"theme": t, "count": c, "representative_quote": theme_quotes.get(t, "")}
        for t, c in theme_counts.most_common(5)
    ]

    # Feature counts (case-insensitive)
    feature_counts: Counter = Counter()
    for d in deals:
        for f in d.get("ramp_features_used") or []:
            key = (f or "").strip().lower()
            if key:
                feature_counts[key] += 1
    top_features = [{"feature": f, "count": c} for f, c in feature_counts.most_common(8)]

    # Champion roles
    role_counts: Counter = Counter()
    for d in deals:
        role = ((d.get("champion") or {}).get("role") or "").strip().lower()
        if role and role not in ("?", "unknown", "null"):
            role_counts[role] += 1
    top_champion_roles = [{"role": r, "count": c} for r, c in role_counts.most_common(5)]

    # Pitch framings
    framing_counts: Counter = Counter()
    for d in deals:
        f = (d.get("pitch_framing") or "").strip().lower()
        if f:
            framing_counts[f] += 1
    top_framings = [{"framing": f, "count": c} for f, c in framing_counts.most_common(4)]

    # Winning moves — keep 3 shortest as exemplars so Slack render stays tight
    winning_moves = [d.get("winning_move") for d in deals if d.get("winning_move")]
    winning_moves_sorted = sorted(winning_moves, key=lambda s: len(s or ""))[:3]

    # Repeated phrases — merge across deals, lowercase, count
    phrase_counts: Counter = Counter()
    for d in deals:
        mp = d.get("messaging_patterns") or {}
        for p in mp.get("repeated_phrases") or []:
            key = (p or "").strip().lower()
            if 2 <= len(key.split()) <= 8:
                phrase_counts[key] += 1
    winning_phrases = [{"phrase": p, "count": c} for p, c in phrase_counts.most_common(8)]

    # First-reply subject lines — keep unique subjects
    first_reply_subjects = []
    for d in deals:
        mp = d.get("messaging_patterns") or {}
        fr = mp.get("first_email_that_got_reply")
        if isinstance(fr, dict) and fr.get("subject"):
            first_reply_subjects.append({
                "subject": fr["subject"][:100],
                "opening": (fr.get("opening") or "")[:180],
            })
    first_reply_subjects = first_reply_subjects[:5]

    return {
        "deal_count": deal_count,
        "total_realized_cp": total_cp,
        "avg_realized_cp": avg_cp,
        "top_pain_themes": top_pain_themes,
        "top_features": top_features,
        "top_champion_roles": top_champion_roles,
        "top_framings": top_framings,
        "sample_winning_moves": winning_moves_sorted,
        "winning_phrases": winning_phrases,
        "first_reply_subjects": first_reply_subjects,
    }


def rebuild() -> dict:
    """Walk all analyzed deals, group by play_tag, write the library file.

    Returns {'source_deals_analyzed': N, 'tags': {tag: deal_count, ...}}.
    """
    anatomies = _load_all_anatomies()
    tag_buckets: defaultdict[str, list] = defaultdict(list)
    for d in anatomies:
        for tag in (d.get("play_tags") or []):
            if not tag:
                continue
            tag_buckets[tag.strip().lower()].append(d)

    tags = {t: _aggregate_tag(deals) for t, deals in tag_buckets.items()}

    payload = {
        "built_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source_deals_analyzed": len(anatomies),
        "tags": tags,
    }

    tmp = str(LIBRARY_FILE) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    os.replace(tmp, LIBRARY_FILE)

    summary = {"source_deals_analyzed": len(anatomies),
               "tags": {t: v["deal_count"] for t, v in tags.items()}}
    logger.info("Play Library rebuilt: %s", summary)
    return summary


def load() -> Optional[dict]:
    """Load the current library file, or None if not yet built."""
    if not LIBRARY_FILE.exists():
        return None
    try:
        return json.loads(LIBRARY_FILE.read_text())
    except Exception as e:
        logger.warning("Failed to load play library: %s", e)
        return None


def get_evidence_for_tags(play_tags: list[str]) -> Optional[dict]:
    """Return aggregated evidence across one or more play_tags.

    If a Plays-tab row maps to multiple anatomy tags (e.g. P1 → plus_upgrade +
    erp_integration), we merge them: deal_count = sum, pain/feature/phrase
    counts merged. Used by home_tab to render the "Team Evidence" footer.
    """
    lib = load()
    if not lib or not play_tags:
        return None

    matched = []
    for t in play_tags:
        entry = (lib.get("tags") or {}).get(t.strip().lower())
        if entry:
            matched.append(entry)
    if not matched:
        return None

    if len(matched) == 1:
        return matched[0]

    # Merge: sum counts, union lists, keep max aggregates
    merged_painthemes: Counter = Counter()
    merged_features: Counter = Counter()
    merged_roles: Counter = Counter()
    merged_framings: Counter = Counter()
    merged_phrases: Counter = Counter()
    sample_moves: list = []
    first_reply_subjects: list = []
    total_deal_count = 0
    total_cp = 0
    theme_quote_lookup: dict = {}

    for entry in matched:
        total_deal_count += entry.get("deal_count", 0)
        total_cp += entry.get("total_realized_cp", 0)
        for item in entry.get("top_pain_themes", []):
            merged_painthemes[item["theme"]] += item["count"]
            theme_quote_lookup.setdefault(item["theme"], item.get("representative_quote", ""))
        for item in entry.get("top_features", []):
            merged_features[item["feature"]] += item["count"]
        for item in entry.get("top_champion_roles", []):
            merged_roles[item["role"]] += item["count"]
        for item in entry.get("top_framings", []):
            merged_framings[item["framing"]] += item["count"]
        for item in entry.get("winning_phrases", []):
            merged_phrases[item["phrase"]] += item["count"]
        sample_moves.extend(entry.get("sample_winning_moves", []))
        first_reply_subjects.extend(entry.get("first_reply_subjects", []))

    return {
        "deal_count": total_deal_count,
        "total_realized_cp": total_cp,
        "avg_realized_cp": total_cp // total_deal_count if total_deal_count else 0,
        "top_pain_themes": [
            {"theme": t, "count": c, "representative_quote": theme_quote_lookup.get(t, "")}
            for t, c in merged_painthemes.most_common(5)
        ],
        "top_features": [{"feature": f, "count": c} for f, c in merged_features.most_common(6)],
        "top_champion_roles": [{"role": r, "count": c} for r, c in merged_roles.most_common(3)],
        "top_framings": [{"framing": f, "count": c} for f, c in merged_framings.most_common(3)],
        "sample_winning_moves": sample_moves[:3],
        "winning_phrases": [{"phrase": p, "count": c} for p, c in merged_phrases.most_common(6)],
        "first_reply_subjects": first_reply_subjects[:3],
    }
