# Gary Bot — Backlog

Ideas, planned features, and known blockers. Prioritized by impact.

---

## Blocked (Waiting on External)

- [x] ~~**Salesforce opp writes via Growth MCP**~~ — Done 2026-04-16. `create_opportunity()` and `update_opportunity()` now route through `core/growth_mcp.py` → `create_expansion_opportunity` and `update_opportunities` tools at `growth-mcp-remote.ramp.builders/mcp`. Gap fields (`Gong_Outreach_Link__c`, `WinReasonDetail__c`) set via follow-up update with 2s/4s/6s exponential-backoff retry for SF async trigger races. `/opp` command and post-meeting opp creation are live.
- [ ] **Toki sequence enrollment** — Growth MCP's `add_outreach_prospects_to_sequence` doesn't exist yet. Can read sequences and prospects but can't enroll new prospects programmatically.
- [ ] **Google Calendar in Glass** — Needs IAM on `ramp-gumstack` GCP project to work natively. Currently using workarounds.
- [ ] **Ramplify deployment** — 4 SSO blockers identified preventing hosted deployment. Bot runs locally via launchd for now.
- [ ] **Quota snapshot data source** — Gumstack `get_attachment` uploads to Gumloop storage instead of returning bytes. Can't download Looker ZIP attachments. Snowflake CP quota tables (`analytics.exp.*`) are in schemas the READER role can't access. Need either: (a) Snowflake access to `exp` schema, (b) Gumstack fix to return attachment bytes, or (c) Glass-based workaround to download and cache the Looker ZIP locally.

## High Priority — Next Up

- [ ] **Follow-up touch emails (Outreach/Toki-style)** — Automated multi-touch sequences: if no reply after X days, draft a follow-up with different angle. Currently each alert is one-shot.
- [ ] **Pipeline cleanup recommendations** — Analyze emails + Gong transcripts to recommend SFDC opp stage changes, close date updates, or close-lost. Surface in Pipeline tab.
- [ ] **Post-meeting to-do reminders** — After a meeting ends, check if follow-up email was sent, opp was created/updated, notes were logged. DM reminders for missing actions.
- [ ] **Quota attainment heartbeat** — Daily or weekly DM: day-to-day CP changes broken down by product and account, pacing vs quota, which opps are driving/dragging.
- [ ] **Forecasting assistant** — Which opps to prioritize this week, what's at risk, recommended next engagements based on deal velocity and engagement recency.

## Medium Priority — Planned

- [ ] **Plus Trial email drafter** — Triggered from Momentum alerts when a customer starts a Plus trial. Template-based like procurement trial drafter.
- [ ] **Stale opp re-engage drafter** — Dedicated email template for opps that have gone cold (30+ days). Different tone/approach than the current generic stale opp draft.
- [ ] **Open opp pacing alerts** — Track treasury deposit initiated, first bill paid, 0-to-1 activation against expected timeline. Alert when behind pace.
- [ ] **Procurement opp creation fix** — Invalid Salesforce picklist value blocking automated procurement opp creation. Need to map correct picklist values.
- [ ] **Gong MCP persistent 401s** — 7+ debugging sessions; tokens refresh but Gong still returns 401 intermittently. May need Gumstack-side fix.

## Nice to Have — Ideas

- [ ] **Changelog auto-generation** — On commit/push, auto-append to CHANGELOG.md from commit message body.
- [ ] **Re-enable channel drafting via DM** — Disabled in `channel_monitors.py:831-836` (replaced by cowork workflows). Evaluate if DM-triggered drafting is still useful.
- [ ] **Securities/investment tracking** — No visibility into investment product usage in current data. Explore if Snowflake has this data.
- [ ] **Snooze improvements** — Snooze with custom duration, snooze reason, snooze expiry notifications.
- [ ] **.env.example file** — Tracked template showing all required env vars without secrets, so new users know what to fill in.
- [ ] **Bot health dashboard** — Slack canvas or home tab section showing: last successful run per job, token expiry status, error counts.

## Completed Recently (for reference)

- [x] Stale opps tab redesign with natural language context and draft previews
- [x] Token auth overhaul (proper OAuth2 refresh for Gmail, Gong, Salesforce)
- [x] Glass credential fallback for all MCP clients
- [x] Gmail `get_thread()` for smarter thread-reply drafts
- [x] Granola two-signal meeting end detection
- [x] Missing opps (zero-to-one) in Pipeline tab
- [x] Snooze button on stale opp cards
- [x] MCP session caching (50% fewer HTTP calls)
- [x] Prospecting signals engine (7 signal types)
- [x] Multi-user support with user registry
- [x] Plus status on prospecting cards
- [x] Signal accuracy overhaul (37% noise reduction)
