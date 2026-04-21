"""Ramp help center + product page reference map for auto-drafted emails.

When drafting follow-up emails, match meeting topics to these links and
include them as hyperlinks in the email body. Use the topic keywords to
match against transcript content / Granola notes.

Subject line format: "Ramp Follow-Up - <brief summary of key topic>"
"""

# ── Product pages (ramp.com) ─────────────────────────────────────────────────
PRODUCT_PAGES = {
    "pricing": {
        "url": "https://ramp.com/pricing",
        "title": "Ramp Plus features and pricing",
        "keywords": ["plus", "pricing", "plan", "features", "upgrade", "tier"],
    },
    "bill_pay": {
        "url": "https://ramp.com/bill-pay",
        "title": "Ramp Bill Pay",
        "keywords": ["bill pay", "accounts payable", "AP", "invoice", "bills", "vendor payment"],
    },
    "procurement": {
        "url": "https://ramp.com/procurement",
        "title": "Ramp Procurement",
        "keywords": ["procurement", "purchase order", "PO", "intake", "vendor management"],
    },
    "travel": {
        "url": "https://ramp.com/travel",
        "title": "Ramp Travel",
        "keywords": ["travel", "booking", "per diem", "hotel", "flight", "corporate travel"],
    },
    "treasury": {
        "url": "https://ramp.com/treasury",
        "title": "Ramp Treasury",
        "keywords": ["treasury", "cash management", "investment", "yield", "deposits", "FDIC"],
    },
    "corporate_card": {
        "url": "https://ramp.com/corporate-card",
        "title": "Ramp Corporate Card",
        "keywords": ["card", "corporate card", "credit card", "virtual card", "cashback"],
    },
    "expense_management": {
        "url": "https://ramp.com/expense-management",
        "title": "Ramp Expense Management",
        "keywords": ["expense", "reimbursement", "receipt", "expense report"],
    },
    "accounting": {
        "url": "https://ramp.com/accounting-integrations",
        "title": "Ramp Accounting Integrations",
        "keywords": ["accounting", "integration", "ERP", "sync", "reconciliation"],
    },
}

# ── Help center articles (support.ramp.com) ──────────────────────────────────
HELP_ARTICLES = {
    # Multi-entity
    "multi_entity": {
        "url": "https://support.ramp.com/hc/en-us/articles/23815251559443-Ramp-support-for-multi-entity-businesses",
        "title": "Multi-entity support",
        "keywords": ["multi-entity", "multiple entities", "entity", "subsidiary", "consolidat"],
    },
    # Bill Pay
    "bill_pay_setup": {
        "url": "https://support.ramp.com/hc/en-us/articles/4417760908435-Bill-Pay-set-up",
        "title": "Bill Pay setup guide",
        "keywords": ["bill pay setup", "set up bill pay", "getting started bill pay"],
    },
    "bill_pay_card_payment": {
        "url": "https://support.ramp.com/hc/en-us/articles/28105415406867-Pay-Bill-Pay-invoices-via-Ramp-card",
        "title": "Pay bills via Ramp card",
        "keywords": ["pay with card", "card payable", "ACH to card", "card payment", "cashback on bills"],
    },
    "bill_pay_recurring": {
        "url": "https://support.ramp.com/hc/en-us/articles/8952397876883-Creating-and-managing-recurring-bill-payments-on-Ramps-Bill-Pay",
        "title": "Recurring bill payments",
        "keywords": ["recurring", "recurring bill", "recurring payment", "auto pay"],
    },
    "bill_pay_tax": {
        "url": "https://support.ramp.com/hc/en-us/articles/11030575950739-Bill-Pay-Tax-Support",
        "title": "Vendor tax info & W-9 collection",
        "keywords": ["1099", "W-9", "tax", "vendor tax", "tax compliance"],
    },
    "bill_pay_ocr": {
        "url": "https://support.ramp.com/hc/en-us/articles/45686841394579-Ramp-Bill-Pay-OCR",
        "title": "Bill Pay OCR & invoice scanning",
        "keywords": ["OCR", "invoice scan", "auto extract", "invoice capture"],
    },
    "ap_aging": {
        "url": "https://support.ramp.com/hc/en-us/articles/4413380587155-Where-to-view-AP-Aging-Report-and-whats-included",
        "title": "AP Aging Report",
        "keywords": ["AP aging", "aging report", "outstanding bills", "overdue"],
    },
    "bill_pay_amortization": {
        "url": "https://support.ramp.com/hc/en-us/articles/10464991136787-Bill-Pay-Amortization-for-NetSuite",
        "title": "Bill Pay amortization (NetSuite)",
        "keywords": ["amortization", "prepaid", "netsuite amort"],
    },
    # Procurement
    "three_way_match": {
        "url": "https://support.ramp.com/hc/en-us/articles/30227975187731-3-Way-Match-with-Ramp-Procurement",
        "title": "3-way match with Ramp Procurement",
        "keywords": ["3-way match", "three way match", "PO matching", "purchase order match"],
    },
    # Card / ACH-to-Card
    "card_payable_bills": {
        "url": "https://support.ramp.com/hc/en-us/articles/23400406701972",
        "title": "Paying vendors by card (card-payable bills)",
        "keywords": ["card payable", "ACH to card", "pay vendor by card", "cashback", "card payment"],
    },
    # Accounting integrations
    "accounting_overview": {
        "url": "https://support.ramp.com/hc/en-us/articles/4434982407443",
        "title": "Overview of Ramp Accounting integrations",
        "keywords": ["accounting", "QuickBooks", "NetSuite", "Sage", "Xero", "integration"],
    },
    # Split transactions
    "split_transactions": {
        "url": "https://support.ramp.com/hc/en-us/articles/4412096221971-Split-transactions",
        "title": "Split transactions",
        "keywords": ["split", "split transaction", "split bill", "divide", "allocat"],
    },
    # Credit limits
    "credit_limit": {
        "url": "https://support.ramp.com/hc/en-us/articles/4402792221587-Understanding-your-Ramp-credit-limit",
        "title": "Understanding your credit limit",
        "keywords": ["credit limit", "limit increase", "limit", "open to buy"],
    },
    # Approvals
    "approval_workflows": {
        "url": "https://support.ramp.com/hc/en-us/articles/4402810369171-Setting-up-approval-workflows",
        "title": "Setting up approval workflows",
        "keywords": ["approval", "workflow", "approver", "approval chain", "routing"],
    },
    # Reimbursements
    "reimbursements": {
        "url": "https://support.ramp.com/hc/en-us/articles/4402810580499-Reimbursements-overview",
        "title": "Reimbursements overview",
        "keywords": ["reimbursement", "reimburse", "out of pocket", "mileage"],
    },
    # Reporting
    "custom_reports": {
        "url": "https://support.ramp.com/hc/en-us/articles/15229737285011-Custom-reports",
        "title": "Custom reports & analytics",
        "keywords": ["report", "reporting", "analytics", "dashboard", "custom report", "insight"],
    },
}


import json as _json
import os as _os
import re as _re

# Topic groups for deduplication — entries sharing a topic won't both appear
_TOPIC_MAP = {
    "bill_pay_setup": "bill_pay", "bill_pay_card_payment": "bill_pay",
    "bill_pay_recurring": "bill_pay", "bill_pay_tax": "bill_pay",
    "bill_pay_ocr": "bill_pay", "ap_aging": "bill_pay",
    "bill_pay_amortization": "bill_pay",
    "three_way_match": "procurement",
    "card_payable_bills": "card", "corporate_card": "card",
    "accounting_overview": "accounting",
}

_EDDY_CACHE_FILE = _os.path.expanduser("~/.gary_bot_eddy_links.json")


def _load_eddy_articles() -> dict:
    """Load Eddy-scraped help articles (same shape as HELP_ARTICLES).

    Silently returns {} if cache missing or malformed — Eddy scrape is optional
    enrichment, not a required dependency.
    """
    try:
        with open(_EDDY_CACHE_FILE) as f:
            data = _json.load(f)
            if not isinstance(data, dict):
                return {}
            return data
    except (FileNotFoundError, _json.JSONDecodeError, OSError):
        return {}


def _keyword_matches(keyword, text_lower):
    """Check if a keyword matches in text. Word-boundary matching for short keywords."""
    kw = keyword.lower()
    if len(kw) <= 3:
        return bool(_re.search(r'\b' + _re.escape(kw) + r'\b', text_lower))
    return kw in text_lower


def find_relevant_links(text, max_links=4):
    """Find help articles and product pages relevant to the given text.

    Parameters
    ----------
    text : str
        Meeting transcript, notes, or summary to match against.
    max_links : int
        Maximum number of links to return.

    Returns
    -------
    list[dict]
        List of dicts with keys: url, title, source ("help" or "product").
        Ordered by relevance (minimum 2 keyword matches required).
    """
    text_lower = text.lower()
    scored = []
    seen_urls = set()

    for key, entry in HELP_ARTICLES.items():
        hits = sum(1 for kw in entry["keywords"] if _keyword_matches(kw, text_lower))
        if hits >= 2:  # Require 2+ keyword matches to avoid false positives
            scored.append({
                "url": entry["url"],
                "title": entry["title"],
                "source": "help",
                "score": hits,
                "key": key,
            })
            seen_urls.add(entry["url"])

    for key, entry in PRODUCT_PAGES.items():
        hits = sum(1 for kw in entry["keywords"] if _keyword_matches(kw, text_lower))
        if hits >= 2:
            scored.append({
                "url": entry["url"],
                "title": entry["title"],
                "source": "product",
                "score": hits,
                "key": key,
            })
            seen_urls.add(entry["url"])

    # Merge Eddy-scraped entries (from #gam-ask-ai). Same shape as HELP_ARTICLES.
    # Require same 2-keyword threshold; skip URLs already matched above.
    for key, entry in _load_eddy_articles().items():
        url = entry.get("url")
        if not url or url in seen_urls:
            continue
        keywords = entry.get("keywords") or []
        hits = sum(1 for kw in keywords if _keyword_matches(kw, text_lower))
        if hits >= 2:
            # Slight score penalty so hardcoded help articles win ties —
            # those have been vetted; Eddy entries are auto-classified.
            scored.append({
                "url": url,
                "title": entry.get("title", "Ramp resource"),
                "source": "eddy",
                "score": hits - 0.5,
                "key": f"eddy_{key}",
            })
            seen_urls.add(url)

    # Sort by score descending, prefer help articles over product pages at same score
    scored.sort(key=lambda x: (x["score"], x["source"] == "help"), reverse=True)

    # Deduplicate by topic: keep the highest-scored entry per topic
    seen_topics = set()
    results = []
    for item in scored:
        topic = _TOPIC_MAP.get(item["key"], item["key"])
        if topic not in seen_topics:
            seen_topics.add(topic)
            results.append({
                "url": item["url"],
                "title": item["title"],
                "source": item["source"],
            })
        if len(results) >= max_links:
            break

    return results


def format_links_for_email(links):
    """Format relevant links as HTML for inclusion in email body.

    Returns an HTML string with hyperlinked resource list, or empty string
    if no links provided.
    """
    if not links:
        return ""

    items = []
    for link in links:
        items.append(
            f'<li><a href="{link["url"]}" style="color:#1155CC;">'
            f'{link["title"]}</a></li>'
        )

    return (
        '<p><strong>Helpful resources:</strong></p>\n'
        f'<ul>{"".join(items)}</ul>'
    )


def format_links_for_slack(links):
    """Format relevant links as Slack mrkdwn for inclusion in DMs.

    Returns a string with Slack-formatted links, or empty string.
    """
    if not links:
        return ""

    lines = ["*Helpful Resources:*"]
    for link in links:
        lines.append(f"• <{link['url']}|{link['title']}>")
    return "\n".join(lines)
