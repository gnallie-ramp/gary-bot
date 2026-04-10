# Changelog

All notable changes to Gary Bot are documented here. When pulling a new version, check below for what changed and whether any new `.env` variables are needed.

---

## [Unreleased] — Working locally, not yet pushed

### Stale Opps Tab Redesign
- Rich contextual cards with natural language spend status ("Spend exceeding baseline by $X/mo — close ASAP")
- Draft preview on every card: shows who the email will go to, whether it replies to an existing thread or starts new, and what the email will cover
- Key contacts (name + title) displayed on each card
- Email direction tracking: distinguishes "no reply yet" (outbound) vs "needs follow-up" (inbound)
- Snooze button (7d) on every stale opp card — hides it from the tab temporarily
- Staleness threshold reduced from 21 days to 15 days
- Expansion amount shown on cards when available

### Token Auth Overhaul (Gmail, Gong, Salesforce)
- All three Gumstack clients now do proper OAuth2 `refresh_token` grant via `api.gumloop.com/oauth/token` instead of the broken MCP-reinitialize hack
- Glass credential fallback: bot reads fresh tokens from `~/.project-glass/credentials.json` when available (always fresh, managed by Glass)
- Gong and Salesforce clients now retry once on 401 with token refresh before alerting

### Gmail Client
- New `get_thread()` function: fetches full Gmail thread (all messages, chronological) for smarter thread-reply drafts

### Granola Meeting Detection
- Two-signal end detection: Granola `meeting_end_count` + transcript cooldown (2 min of no `updated_at` changes) prevents processing mid-call on brief audio drops
- Replaces the old single-signal approach that occasionally fired during meetings

### Channel Monitors
- AM regex now matches "Ramp POC - AM:" in addition to "Account Manager:" (covers new alert formats)
- Fallback channel ID resolution: if `conversations_list` misses a channel, tries hardcoded IDs via `conversations_info`

### Pipeline Tab
- Missing opps (zero-to-one) section added: shows accounts with activation but no open opp, with "Create Opp" links

### Snowflake
- Concurrent query limit raised from 1 (Lock) to 4 (Semaphore) for faster parallel data loads

### SETUP.md
- Major rewrite for new user onboarding: added Claude Code API key, Gumloop access requests, GitHub account creation, Gong MCP setup, clearer step-by-step with less jargon
- Changed "YOLO mode" references to "Auto Accept" (Glass terminology)

---

## [v17] — 2026-04-06 `6af7d24`

### Plus Status on Prospecting Cards
- Every prospecting card now shows Ramp Plus status (Active, In Trial, Grandfathered, Never Trialed, Churned)
- Accounts with "in trial" status treated as Plus-active — prevents false "No Plus" signals on 19 accounts

## [v16] — 2026-04-05 `311c328`

### Fix False "No Bill Pay" Signal
- `bill_pay_product_status` can be "churned" while account does $200K+/mo in bill pay (10 accounts affected)
- Now checks actual L30D bill pay spend >= $5K as secondary signal, not just status field

## [v15] — 2026-04-04 `72b39f7`

### MCP Session Caching
- Cache initialized MCP sessions for 5 min per user across Gmail, Gong, and Salesforce clients
- Eliminates redundant initialize round-trips: email drafts go from 4-8 HTTP calls to 2-4 (50% reduction)
- TCP connection reuse via `requests.Session`

### Prospecting Tab: AE Estimates
- Added AE estimated card spend and AE estimated bill pay spend on all prospecting cards
- Off-ramp bill pay spend now shown on all signal types (was only on erp_no_billpay and high_competitor_spend)

## [v14] — 2026-04-03 `a540c5b`

### SSE Response Handling
- Gumstack MCP endpoints sometimes return `text/event-stream` instead of JSON
- Added `_parse_response()` to all three clients: tries JSON first, falls back to parsing SSE `data:` lines
- Fixes intermittent "Expecting value: line 1 column 1" failures on draft creation

## [v13] — 2026-04-02 `88f20af`

### Backfill Pagination Fix
- `backfill_missed_messages()` was only fetching 200 messages (one page) with no cursor pagination
- High-volume channels like #alerts-card-payable-bills get ~200 messages/day — 72h backfill was only seeing ~12 hours
- Now paginates with cursor until all messages in window are fetched (capped at 2000)

## [v12] — 2026-04-01 `e4fa4b6`

### Salesforce Writes Blocked
- Confirmed Gumstack MCP blocks `create_record` and `update_record` ("Tool restricted for your permission group")
- SOQL reads work fine; writes need Growth MCP when available

## [v11] — 2026-03-31 `909c3a2`

### Replace sf CLI with Gumstack Salesforce MCP
- Ramp security revoked CLI-based Salesforce access
- Migrated to Gumstack MCP at `mcp.gumloop.com/salesforce/mcp` — same OAuth pattern as Gmail and Gong
- Removed sf CLI install from SETUP.md

## [v10] — 2026-03-30 `754cc18`

### Snowflake Config Fix
- Corrected account locator to `rib11536.us-east-1` (was "ramp"), role to `READER` (was "DEPT-SALES"), added `schema = PUBLIC`

## [v9] — 2026-03-29 `4241e48`

### Command Prefix System
- All 22 slash commands now use a configurable `COMMAND_PREFIX` from `.env` (default: "gary")
- Each bot instance picks a unique prefix so commands don't conflict in the same workspace
- SETUP.md rewritten so Glass prompts for values at runtime

## [v8] — 2026-03-28 `17d932f`

### SETUP.md Rewrite
- Restructured into 3 clear steps: manual prerequisites, automated block, verification
- Added missing steps (SF CLI, snow symlink, Snowflake user field)
- Made launchd auto-start the default

## [v7] — 2026-03-27 `683b6aa`

### SETUP.md Path Fix
- All references to `~/gary-bot/slack_bot/` updated to `~/gary-bot/` (repo root contains the code directly)

## [v6] — 2026-03-26 `48f4d0b`

### Prospecting Signals Engine
- 7 signal types from Book of Business view with 4-hour cache
- Activation alerts (treasury, investment, first bill) as prospecting signals
- Competitor/off-ramp spend display for erp_no_billpay and high_competitor_spend
- CC domain-match guard: only CC contacts matching TO contact's email domain
- Procurement trial uses fixed template instead of Claude-generated
- RCLIP channel monitor and AM escalation channel monitor added
- Word-splitting fuzzy match for account resolver; Jaro-Winkler threshold raised 70 → 82

## [v5] — 2026-03-25 `e4639bc`

### Multi-User Support
- User registry with per-user token support for Gmail/Gong/Calendar
- #bill-pay-automatic-card-losses channel monitor with auto email drafting
- Auto Card Sweep scheduled job (30m intervals, weekdays 8AM-6PM)
- Inline auth failure detection for Gmail and Salesforce with DM alerts
- Stale opps tab in home tab with rich opp cards
- `_parameterize_queries()` for multi-user Snowflake queries
- SETUP_TEAMMATE.md onboarding guide

## [v4] — 2026-03-24 `5a42e3b`

### Owner Detection & Guard
- Auto-detect bot owner via `.owner` file on first Home tab open
- Owner guard on all 22 slash commands to prevent cross-talk between instances
- Pre-fill Gong call URL and Win Reason Detail on opp creation
- AE presale spend with discrepancy flags across home tab, priority actions, and alerts

## [v3] — 2026-03-23 `2459a2f`

### Signal Accuracy Overhaul
- Added SPLM-60 (62-day lookback) for bimonthly/quarterly cycles
- Transaction count guards, concentration guards, product-specific delta minimums
- Signal confirmation layer: real-time DMs require 2 consecutive runs before sending
- **Result: 100 signals → 63 (37% noise reduction)**
- Gong MCP client added for real-time call/transcript access
- Last call/email dates added to all account displays

## [v2] — 2026-03-22 `6e202da`

### SETUP.md for Beginners
- Added Step 0 with Homebrew, git, Python, Node, gh CLI install
- Copy-paste Terminal commands and beginner-friendly explanations

## [v1] — 2026-03-21 `9c0fccc`

### Initial Release
- 22 slash commands, Snowflake queries, Gmail draft creation, calendar briefs
- Real-time account signal alerts across 8 Slack channels
- Clone-ready with `.env` configuration
