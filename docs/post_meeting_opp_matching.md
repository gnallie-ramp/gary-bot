# Post-Meeting Opp Matching + Create/Update Logic

**Context:** Internal write-up for Ramp engineers building similar AI-assisted sales tooling. Describes how Gary Bot matches a completed customer meeting to the right Salesforce account, decides whether to create a new opp vs. update an existing one vs. skip, and performs the write via Growth MCP.

All code referenced here lives under `jobs/`, `utils/`, and `core/` in the Gary Bot repo.

---

## End-to-end flow

1. **Meeting ends** — detected via either:
   - **Granola** (`core/granola_client.py`): transcript pulled every 3 min, end signal = `meeting_end_count >= 1` AND transcript hasn't grown for 4 min (avoids firing mid-call during screen-share pauses). Fallbacks: 10 min past calendar end, or 15 min since last transcript update.
   - **Gong** (`core/gumstack_gong.py`): Gong → Gumstack MCP → pulled every 30 min.
2. **Dedup** — a composite key `{source}_{meeting_id_or_call_id}` lands in `~/.gary_bot_processed.json` so the two paths (Granola real-time, Gong batch) never double-process the same meeting.
3. **Account match** — resolve meeting → SFDC account ID (the focus of this doc).
4. **Opp context pull** — for the matched account, pull open opps + recently-closed opps from Snowflake.
5. **Transcript → structured analysis** — Claude reads transcript + participants + opp context, returns JSON: meeting_summary, highlights, buying signals, `opps` (new opps to create), `opp_updates` (changes to existing opps), follow-up email draft.
6. **Validation gates** — every opp action gets run through `validate_opp_action()` which decides create/update/skip.
7. **DM review card to AM** — Slack message with per-opp review, per-field Apply/Skip buttons, recipient list, draft email.
8. **AM clicks Apply** — action handler calls Growth MCP `create_expansion_opportunity` or `update_opportunities`, logs success/failure, retries field gaps with 2s/4s/6s exponential backoff.

---

## Account matching — the 5-strategy chain

`utils/account_matcher.py` → `match_account(account_name, account_id, domain, participant_emails, user_id)` → `MatchResult`.

Inputs come from different places depending on source:

| Source | account_id | account_name | domain | participant_emails |
|---|---|---|---|---|
| **Gong** | `dim_sfdc_gong_call.sfdc_primary_account_id` (SFDC does the linking for us) | Account name from linked SFDC account | — | External attendee emails from Gong roster |
| **Granola** | — (Granola has no SFDC link) | Inferred from meeting title + transcript + Calendar event title | Extracted from external attendee emails (first non-`@ramp.com`) | External attendee emails from the Granola meeting |

The matcher tries in order; stops at first match:

**1. Direct SFDC ID lookup.** If `account_id` is provided (Gong case), `SELECT … FROM dim_sfdc_accounts WHERE account_id = :id AND account_status = 'Active'`. If the account is active and real, this is the only strategy that runs — the rest are skipped. Most reliable since SFDC has already done the linking.

**2a. Name + email-domain contact search.** Union query: `account_name ILIKE '%{search}%'` OR `account_id IN (SELECT account_id FROM dim_sfdc_contacts WHERE contact_email ILIKE '%{domain}%')`. Catches cases where the account name is slightly off in the meeting title but a known contact email is on the account.

**2b. Domain-slug name search.** Fallback when 2a finds nothing. Strip TLD from external domain (`americanaccordfood.com` → `americanaccordfood`), `WHERE account_name ILIKE '%{slug}%'`. Catches accounts whose SFDC name is a tight match to the email domain even if the meeting title uses informal language.

**2c. Name-fragment search.** Split the account name into significant words (drops "corp", "inc", "llc", "the", "group", "company", "services", "ramp", "partners", etc.), try adjacent pairs first ("American Accord"), then individual words sorted by length. Catches "American Accord Food Corp" → "American Accord" when the SFDC name is "American Accord Foods, Inc." or similar.

**3. Flexible ILIKE (belt-and-suspenders).** `resolve_account_name()` in `utils/account_resolver.py` adds a second pass with camelCase splitting + common suffix detection so `millsapartments` resolves to `Mills Apartments`. Used mostly by slash commands + user-typed searches; post-meeting flow doesn't hit it often.

**4. Jaro-Winkler fuzzy (last resort).** `JAROWINKLER_SIMILARITY(lower(name), lower(query)) >= 82`. Hits when none of the above matched but the names are close. Logs `"fuzzy matched 'X' → 'Y' (score=N)"` so we can audit precision later. Threshold tuned empirically — raise it if false positives creep in, lower it if we're missing obvious matches.

**When multiple candidates come back** (`_pick_best_match`), priority order:
1. Exact case-insensitive name match wins immediately.
2. Otherwise prefer rows where the current user is the account owner AND has open opps (active relationship signal).
3. Otherwise the current user's accounts without open opps.
4. Last resort: first row. If we fall through to this, we emit a warning on the DM card ("Multiple matches found: X, Y, Z. Matched to: W") so the AM can reject and retry manually.

---

## `MatchResult` — what the matcher returns

```python
@dataclass
class MatchResult:
    matched: bool
    account_id: str
    account_name: str
    owner_name: str            # SFDC owner stamped on the account today
    owner_id: str              # SFDC user ID resolved via bot's user registry
    account_status: str        # "Active", "Churned", etc.
    is_gregs_book: bool        # True iff owner_name == caller's SFDC owner name
    segment: str               # Growth / Mid-Market / etc.
    open_opps: list[dict]      # [{opp_id, opp_name, stage, product, expansion_type, created_date, ...}]
    recently_closed_opps: list[dict]  # CW'd within the last 90 days
    warnings: list[str]        # Strings shown to the AM in the review DM
```

After matching, `_fetch_opps()` populates `open_opps` (stage != "S0: Holding") and `recently_closed_opps` (CW or CL within the last 90 days). This context is what the next step — the validation gate — reasons over.

---

## Validation gate — create vs. update vs. skip

`utils/account_matcher.py` → `validate_opp_action(match, suggested_product, user_id)` → `{"action": "create" | "update" | "skip", "reason": str, "existing_opp": dict | None, "warnings": [str]}`.

Hard rules in priority order:

| Condition | Action | Reason surfaced to AM |
|---|---|---|
| `match.matched == False` | **skip** | "Could not match to a Salesforce account." |
| Match found but `owner_name != caller's SFDC name` | **skip** | "Account owned by {other rep} — not in your book." Prevents cross-rep opp creation. |
| Caller owns it, existing open opp for the same product | **update** | Returns the existing opp; Claude proposes `next_step`, `next_step_due_date`, `expansion_notes`, `stage` patches. If the existing opp is owned by a different rep, a warning is surfaced and the AM confirms before write. |
| Caller owns it, no open opp, but a CW or CL opp for the same product within 90 days | **skip** | "Already closed {won|lost} {N} days ago — wait for the window to pass or escalate manually." Prevents the "AM Smith closed a Card opp 3 weeks ago but an AI just opened a duplicate from today's call" failure mode. |
| Caller owns it, no open opp, no recent close | **create** | Goes through `core/growth_mcp.create_expansion_opportunity`. |

Product-name normalization: the validator maps meeting-side product mentions (`"Ramp Plus"`, `"bill_pay"`, `"SaaS Add-On"`) to canonical SFDC expansion_subtype values via a small alias map. Keeps Claude from having to know every SFDC enum value verbatim.

---

## Opp update proposer — a separate Claude pass

`jobs/pipeline_update_proposer.py` handles the "update" path. For each existing opp the validator flags as updateable:

1. Pulls **live** SFDC state via Growth MCP `get_opportunity_details` — critical because `next_step_due_date` is a custom SFDC field that isn't replicated into the Snowflake mart. Using the mart alone led to proposals showing "current: (blank)" for due dates that actually had real values.
2. Pulls recent email thread bodies (`dim_email_threads.first_email_body`, `last_email_body_clean`) + Gong transcript paragraphs for the last 90 days on this account.
3. Sends Claude a prompt with: current field values, transcript, email history, call summary, and a hard JSON schema with allowed fields (`next_step`, `next_step_due_date`, `expansion_notes`, `stage`, `close_date`).
4. Post-facto Python guards drop:
   - **Stage regressions** (proposed rank < current rank via `_STAGE_ORDER` map)
   - **Past-dated proposals** (proposed date < today)
   - **Regression/no-change dates** (proposed `next_step_due_date` <= current; would downgrade the due date)
   - **Invalid stage strings** (must match real SFDC picklist values; Claude will invent e.g. "S3: Solution Validation" otherwise)
5. Remaining proposals go into the DM review card with per-field Apply/Skip buttons + reasoning text.

---

## SFDC write path — Growth MCP

Reads: `dim_sfdc_accounts`, `dim_sfdc_opportunities`, `dim_sfdc_contacts` all come from Snowflake (nightly ELT — stale by < 24h typically, but post_sales_goals and some custom fields don't replicate, so we live-read via Growth MCP for those).

Writes go exclusively through **Growth MCP** (`core/growth_mcp.py` → `https://growth-mcp-remote.ramp.builders/mcp`):

- `create_expansion_opportunity(account_id, product, stage, amount, close_date, next_step, next_step_due_date, expansion_notes, gong_outreach_link, …)`
- `update_opportunities([{opportunity_id, fields_to_update: {…}}, …])`
- `get_opportunity_details(opportunity_id)` — used by the live-state live-read at proposal time
- `get_opportunities(sfdc_account_id, state)` — real-time open-opp fetch, used when the mart is lagging

Post-create gap-fill: SFDC has async triggers that create some fields after the initial insert. We retry `update_opportunities` for the gap fields (`Gong_Outreach_Link__c`, `WinReasonDetail__c`, `Timeframe_of_Spend__c`, `Primary_Competitor__c`, `Win_Reason__c`) with 2s / 4s / 6s exponential backoff to handle the trigger race.

Auth: Glass-managed tokens first (`~/.project-glass/credentials.json`), with per-user fallback at `~/.gary_bot_tokens/<slack_id>/growth_tokens.json`.

---

## Hard safeties — the things that stop us from doing harm

1. **Book-ownership check.** If `owner_name != caller's SFDC name`, we SKIP creates and UPDATES. We'd rather miss an opp than write into someone else's book.
2. **Recently-closed check (90d).** Opps don't pop back up from CW or CL within 90d — prevents the "analyzer saw a follow-up conversation after a close-lost and opened a duplicate" failure.
3. **Stage regression guard.** Proposer drops any `stage` proposal whose rank is ≤ current stage rank.
4. **Past-date guard.** Any proposed date < today is dropped.
5. **No-advance due-date guard.** Any proposed `next_step_due_date` ≤ current due date is dropped (prevents downgrading a later date to an earlier one — saw this on a real Claude proposal before the guard landed).
6. **Stage-enum validation.** Proposer prompt lists the exact real SFDC picklist values for expansion stages; proposals outside that set are dropped post-facto.
7. **AM-in-the-loop gate.** We never auto-apply. Every create + every update goes through a Slack DM review card with Apply/Skip buttons per field. The AM has to click.
8. **Grounded prompt rules.** Claude is told every claim must trace to transcript/email/note source data; no invented quotes, amounts, or timelines. Post-facto validators drop proposals that cite fabricated dates.
9. **Dedup by composite key.** `{source}_{meeting_id}` prevents Granola + Gong from both firing on the same call. A separate SFDC-opp-ID dedup prevents the proposer DM from stacking multiple review cards for the same opp across multiple calls within a day.

---

## Known edge cases / open issues

- **Mart lag.** `dim_sfdc_opportunities` can be 24h+ behind live SFDC state — we saw 9 of 9 priority accounts returning 0 open opps from the mart that definitely had live opps via Growth MCP. We mitigate by using Growth MCP for the live-state fetch in the proposer path, but the Pipeline tab card builder still reads from the mart and may hide fresh opps. Working on a hybrid fetch.
- **Subsidiary vs. parent account linking.** If a participant comes from a domain that maps to a subsidiary but the parent account is in the rep's book, we'll sometimes match to the subsidiary and warn. No auto-promote-to-parent logic yet.
- **Multi-account contacts.** A contact email domain that appears on multiple SFDC accounts returns ambiguous results; current handling is "first match in caller's book"; could upgrade to surface all matches in the DM for manual selection.
- **Warning signal flow.** `MatchResult.warnings` gets surfaced in the DM review card; we could do more (e.g., auto-skip updates when there are 2+ warnings) but have preferred to let the AM see the full context and decide.
- **Fuzzy threshold drift.** Jaro-Winkler 82 has held up well but isn't monitored. An annotated corpus of past matches (correct/incorrect) would let us tune this systematically.

---

## File index

| What | Where |
|---|---|
| Matching chain | `utils/account_matcher.py` |
| Flexible name resolver (slash commands / user-typed) | `utils/account_resolver.py` |
| Granola end-detection + dedup | `core/granola_client.py` |
| Gong transcripts (real-time) | `core/gumstack_gong.py` |
| Post-meeting pipeline (Gong) | `jobs/post_meeting_followup.py` |
| Post-meeting pipeline (Granola) | `jobs/granola_followup.py` |
| Opp update proposer | `jobs/pipeline_update_proposer.py` |
| Growth MCP write client | `core/growth_mcp.py` |
| Review-card block builder + DM helper | `jobs/pipeline_update_proposer.build_opp_review_blocks`, `dm_account_update_review` |
| Interactive Apply/Skip button handlers | `handlers/interactive.py` (search `pipeline_apply_updates_` / `pipeline_skip_field_`) |

---

*Doc owner: Greg Nallie. Last updated with bot v32+ behavior. For questions, ping in `#project-glass` or DM Greg.*
