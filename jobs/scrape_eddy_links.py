"""Scrape Enablement Eddy's responses in #gam-ask-ai for Ramp help links.

Eddy posts sales resources with URLs in the channel. Pulling those URLs and
clustering them by product topic gives us a fresh, self-updating catalog for
the Resources section of post-meeting / re-engagement emails.

Output shape matches `templates/help_links.py` HELP_ARTICLES dict so
`find_relevant_links()` can merge Eddy-scraped entries with the hardcoded ones.

Persisted to: ~/.gary_bot_eddy_links.json
  {
    "<slugified-title>": {
      "title": "...",
      "url": "https://...",
      "keywords": ["plus", "upgrade", ...],
      "topic": "plus",
      "scraped_at": "2026-04-21T...",
      "source": "eddy"
    },
    ...
  }

Trigger: `/gary-refresh-help` slash command, or called ad-hoc from Glass.
The bot needs to be a member of #gam-ask-ai for `conversations_history` to work.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from config import SLACK_BOT_TOKEN

logger = logging.getLogger(__name__)

EDDY_USER_ID = "S0A7CU5JL3A"  # Enablement Eddy bot
GAM_CHANNEL_ID = "C0AA27QHGDB"  # #gam-ask-ai
CACHE_FILE = os.path.expanduser("~/.gary_bot_eddy_links.json")
MESSAGES_TO_SCAN = 500

# Topic → keyword map. A URL+title is tagged with a topic if the surrounding
# context (message text + URL path) contains any of the keywords.
_TOPIC_KEYWORDS = {
    "plus": ["plus", "upgrade", "saas", "subscription", "tier", "free to paid", "f2p"],
    "card": ["card", "cashback", "cash back", "rebate", "spend control", "limit"],
    "bill_pay": ["bill pay", "billpay", "bill.com", "invoice", "ap automation", "vendor", "three-way match", "3-way match"],
    "treasury": ["treasury", "gla", "yield", "idle cash", "money market", "fdic"],
    "travel": ["travel", "booking", "per diem", "airfare", "hotel"],
    "procurement": ["procurement", "po", "purchase order", "contract management", "vendor intake", "renewals"],
    "integrations": ["netsuite", "quickbooks", "qbo", "sage", "intacct", "acumatica", "dynamics", "integration", "sso", "scim"],
    "onboarding": ["onboarding", "implementation", "setup", "activate", "getting started"],
    "reports": ["report", "dashboard", "analytics", "insight"],
    "policy": ["policy", "compliance", "receipt", "approval"],
}

_URL_RE = re.compile(
    r'<(https?://[^|>]+)(?:\|([^>]+))?>|'  # Slack-formatted <url|title>
    r'(https?://\S+)'                       # bare URL
)


def _slugify(s: str) -> str:
    s = re.sub(r'[^\w\s-]', '', (s or '').lower())
    s = re.sub(r'[\s_-]+', '-', s).strip('-')
    return s[:60] or "eddy-link"


def _classify_topics(context_text: str, url: str) -> list[str]:
    """Return topics this link matches (by keyword in context)."""
    haystack = (context_text + " " + url).lower()
    matched = []
    for topic, kws in _TOPIC_KEYWORDS.items():
        if any(kw in haystack for kw in kws):
            matched.append(topic)
    return matched


def _extract_urls_from_message(msg: dict) -> list[tuple[str, str, str]]:
    """Return [(url, title, surrounding_text), ...] from a message dict.

    Handles both Slack <url|title> format and bare URLs. Surrounding text is
    the ~200 chars around the URL for keyword classification.
    """
    text = msg.get("text", "") or ""
    out = []
    for m in _URL_RE.finditer(text):
        url = m.group(1) or m.group(3)
        title = m.group(2) or ""
        if not url:
            continue
        # Skip intra-Ramp-Slack links, Slack canvases, profile links
        skip_domains = ("ramp.enterprise.slack", "app.slack.com", "slack-redir.net",
                        "ramp.engineering/sessions")
        if any(d in url for d in skip_domains):
            continue
        start = max(0, m.start() - 200)
        end = min(len(text), m.end() + 200)
        context = text[start:end]
        # Fall back to the URL path segment as title if none provided
        if not title:
            path = url.split("/")[-1] or url
            title = path.replace("-", " ").replace("_", " ").title()[:80]
        out.append((url, title, context))
    return out


def _load_cache() -> dict:
    try:
        with open(CACHE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_cache(data: dict) -> None:
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, CACHE_FILE)


def scrape_eddy_links(messages_to_scan: int = MESSAGES_TO_SCAN) -> dict:
    """Scrape #gam-ask-ai for Eddy's URL responses, merge into the cache.

    Returns the updated cache dict.
    """
    client = WebClient(token=SLACK_BOT_TOKEN)
    cache = _load_cache()

    all_messages = []
    cursor = None
    fetched = 0
    while fetched < messages_to_scan:
        try:
            resp = client.conversations_history(
                channel=GAM_CHANNEL_ID,
                limit=min(200, messages_to_scan - fetched),
                cursor=cursor,
            )
        except SlackApiError as e:
            logger.error("Slack conversations_history failed: %s — bot may not be in #gam-ask-ai",
                         e.response.get("error", "?"))
            break
        msgs = resp.get("messages", [])
        all_messages.extend(msgs)
        fetched += len(msgs)
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    # Filter to Eddy's messages. Eddy is a bot user — match on user or bot_id.
    eddy_msgs = [
        m for m in all_messages
        if m.get("user") == EDDY_USER_ID
           or m.get("bot_id") in (EDDY_USER_ID, None)
           and "enablement" in (m.get("username") or "").lower()
           or m.get("bot_profile", {}).get("name", "").lower().startswith("eddy")
    ]

    # Also scan thread replies — Eddy often replies in threads
    for parent in all_messages:
        if parent.get("reply_count", 0) > 0 and parent.get("thread_ts"):
            try:
                tresp = client.conversations_replies(
                    channel=GAM_CHANNEL_ID,
                    ts=parent["thread_ts"],
                    limit=50,
                )
                for reply in tresp.get("messages", []):
                    if (reply.get("user") == EDDY_USER_ID
                            or (reply.get("bot_profile", {}).get("name", "").lower().startswith("eddy"))):
                        eddy_msgs.append(reply)
            except SlackApiError:
                pass

    new_count = 0
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    for msg in eddy_msgs:
        for url, title, context in _extract_urls_from_message(msg):
            key = _slugify(title or url)
            if key in cache:
                # Already have it — just bump scraped_at to mark it's still live
                cache[key]["last_seen"] = now_iso
                continue
            topics = _classify_topics(context, url)
            if not topics:
                continue  # Skip uncategorized (likely a non-help URL)
            # Pick primary topic (first match) + use all matches as keywords
            primary_topic = topics[0]
            keywords = list(set(_TOPIC_KEYWORDS[primary_topic]))
            cache[key] = {
                "title": title[:120],
                "url": url,
                "keywords": keywords,
                "topic": primary_topic,
                "source": "eddy",
                "scraped_at": now_iso,
                "last_seen": now_iso,
            }
            new_count += 1

    _save_cache(cache)
    logger.info("Eddy link scrape complete: %d total entries, %d new",
                len(cache), new_count)
    return {"total": len(cache), "new": new_count, "cache": cache}
