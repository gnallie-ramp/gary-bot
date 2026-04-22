# Email Drafting Guide

All email drafts are created in Gmail under `Claude Drafts/*` labels. Each workflow has its own label, subject line, recipient logic, and body format. Edit this doc to change the copy — then tell Glass to update the corresponding template/prompt in code.

---

## 0. Golden Standard — Outbound Sales Emails (AI-Generated)

This is the canonical spec for **AI-generated outbound sales emails** produced by `jobs/pipeline_drafter.py`, `jobs/granola_followup.py`, `jobs/post_meeting_followup.py`, and `jobs/stale_opp_drafter.py`. It does NOT apply to channel-alert drafts (Section 1 below) — those are operational/transactional and stay fixed.

### Three contexts, three shapes

Outbound sales emails fall into one of three contexts. Each has its own structure, opener, and CTA posture. **Do not force a post-meeting structure onto a cold prospecting email or vice versa** — this is the root cause of "feels robotic" complaints.

| Context | When it fires | Structure | Opener | CTA posture |
|---|---|---|---|---|
| **A. Prospecting** | Hot List draft · Plays tab draft · any account with no open SFDC opp on the pitched product | Warm opener + minimal AM intro → *Why I'm reaching out* (signal) → *Why Ramp for you* (feature→benefit w/ value math) → Outcome-specific CTA → Booking link | 1 sentence AM intro + 1 line naming the signal | Outcome-specific: "15-min walkthrough of Treasury yield on your current cash balance" — not "quick chat" |
| **B. Post-meeting follow-up** | Within hours of a real Gong/Granola call with the account | Warm greeting referencing the call → ONE **product section per product discussed** (Treasury / Procurement / Plus / Card Consolidation etc.) → warm close with follow-up woven in → "Let me know what works best!" → optional "Helpful resources:" block | "Thanks for walking me through [specific thing they mentioned]" | Woven into the close paragraph — flexible time, tied to a specific deliverable they agreed to |
| **C. Re-engage stale** | Open SFDC opp + 15+ days of inactivity OR warm lead we lost momentum with | Acknowledge gap (no apology, just own it) → *Where we left off* → *What's changed worth revisiting* → Refreshed CTA | 1 sentence acknowledging the time gap | Specific 2-day time-window ask on a refreshed angle |

### Cross-context rules (all three contexts follow these)

**Tone**
- AM first-person voice ("I noticed", "I'll send"), not company voice ("Ramp offers")
- Contractions natural, short sentences, no marketing fluff
- No "I hope this email finds you well", "just circling back", "touching base", "I wanted to reach out" as an opener
- No guilt-tripping about unanswered emails
- No language that reveals automation ("I was notified", "our system flagged", etc.)

**Grounding**
- Every specific claim (dollar amount, vendor name, feature fit, named stakeholder) must trace to real data — transcript, email body, BoB signal, or SFDC notes. Never invent.
- If no specific data is on file for a product, keep that bullet generic and brief — don't fabricate to fill space.
- Real Ramp capabilities only: Card (95bps cashback, spend controls, ERP sync, auto-coding), Bill Pay (NetSuite/QBO sync, approval workflows, bulk batching, vendor portal, AP automation), Treasury (~4.5% yield, FDIC pass-through, same-day transfers, multi-entity accounts), Plus (custom fields, per-entity controls, SAML SSO, SCIM, AI agents, advanced policies, dedicated support), Procurement (intake forms, 3-way match, contract renewal tracking, vendor onboarding).

**CTA**
- Always outcome-specific, never "quick chat" or "can we grab time"
- If prospecting: frame the outcome that matters to them ("15-min walkthrough of Treasury yield at $4.5M balance = ~$17K/mo")
- If post-meeting/re-engage: tie to what was discussed ("30-min working session to set up the policy agent we talked about")
- Always include an inline `Book a call: {link}` line before the signature (and the signature also has it — duplication is intentional since some clients hide signatures)

**Structure / formatting**
- Section headers in `<strong>` (e.g., `<strong>Quick recap</strong>`), bullets in `<ul><li>`
- No markdown — HTML only (`<strong>`, `<ul>`, `<li>`, `<a>`, `<br>`, `<p>`)
- Word count: 180-250 for prospecting, 200-350 for post-meeting and re-engage
- Resources section only when real links are in the payload (never fabricate URLs)

### Context A — Prospecting (worked examples)

**A.1 — P1 Plus-gated ERP (Tiny Health, Zoho Books, SAAS_LEGACY tier)**

Subject: `Ramp Plus + Zoho Books — a few specific wins for Tiny Health`

> Hi Kristine,
>
> I'm Greg, your AM for Ramp. Saw you're on Zoho Books with 30+ users on the legacy Ramp tier — there are three Plus-gated pieces that would be worth 15 min of your time to walk through:
>
> <strong>Why I'm reaching out</strong>
> <ul>
>   <li>Zoho Books as your GL puts you right in the sweet spot for Plus — the integration-layer features only unlock above the legacy tier.</li>
>   <li>30+ users + no SAML SSO means every new hire is manual setup in Ramp. That's a rollout bottleneck we can fix.</li>
> </ul>
>
> <strong>Why Plus for Tiny Health</strong>
> <ul>
>   <li><strong>Custom fields synced to Zoho dimensions</strong> — your Ramp transactions land in the right Zoho chart-of-accounts automatically, cutting month-end reconciliation.</li>
>   <li><strong>AI coding agent</strong> — auto-categorizes spend into the right Zoho GL accounts based on vendor + memo patterns, so your accounting team stops doing it line-by-line.</li>
>   <li><strong>SAML SSO + SCIM provisioning</strong> — new hires land in Ramp automatically via your IDP. For 30+ users this pays back immediately.</li>
>   <li><strong>Advanced approval policies</strong> — you can build multi-level approval chains that mirror your Zoho journal-entry authorization structure, instead of the basic single-approver workflow on the current tier.</li>
> </ul>
>
> <strong>Next step</strong>
> <ul>
>   <li>Worth a 20-min working session to walk through the Zoho integration + SSO setup live so you can see exactly what changes. Any chance Tuesday afternoon or Wednesday morning this week works?</li>
> </ul>
>
> Book a call: https://ramp-com.chilipiper.com/me/gregory-nallie/ramp
>
> Greg
> Account Manager @ Ramp

**A.2 — P12 Treasury opp (account with $8M GLA, not on Treasury)**

Subject: `~$30K/mo in Treasury yield sitting on the table at Aerodyne Research`

> Hi Jane,
>
> Greg here — your Ramp AM. You've got ~$8M in connected bank accounts earning close to nothing. On Ramp Treasury at ~4.5% that's roughly $30K/mo in recovered yield — I wanted to flag it specifically because nothing about it is a lockup or minimum-balance tradeoff.
>
> <strong>Why this is worth 15 min</strong>
> <ul>
>   <li><strong>Same-day liquidity</strong> — no lockup, no minimum. Move any amount in or out the same day via the same Ramp dashboard you already use.</li>
>   <li><strong>FDIC pass-through</strong> up to $225M across partner banks. Not a brokered sweep, not a money-market fund — cash stays in FDIC-insured accounts.</li>
>   <li><strong>Operational win</strong> — you're already running AP through Ramp. Treasury consolidates the float side so you're not managing cash across two platforms.</li>
> </ul>
>
> <strong>Next step</strong>
> <ul>
>   <li>I can walk through the yield model on your exact balance in 15 min — you'll see to the dollar what you'd recover monthly and where it ends up. Later this week good?</li>
> </ul>
>
> Book a call: https://ramp-com.chilipiper.com/me/gregory-nallie/ramp
>
> Greg
> Account Manager @ Ramp

**A.3 — P5 PO-in-memo → Procurement (account paying bills with "PO-123" in memos, not on Procurement Add-on)**

Subject: `You're running a PO process in bill memos — Procurement replaces that`

> Hi Carlos,
>
> Greg, your AM at Ramp. Noticed 156 of your last 90 days of bills reference POs in the memo field ("PO: 4920172", "Purchase Order #…", etc.). That's a shadow Procurement process in a spreadsheet — Ramp's Procurement Add-on solves exactly this.
>
> <strong>Why I'm reaching out</strong>
> <ul>
>   <li>You're clearly running POs ahead of bills today, manually. 156 bills with PO references in 90 days is a real process, not an edge case.</li>
>   <li>You're on Bill Pay but not the Procurement Add-on — the piece that formalizes PO creation, approval routing, and 3-way match is missing.</li>
> </ul>
>
> <strong>What Procurement replaces for you</strong>
> <ul>
>   <li><strong>Intake forms</strong> auto-generate POs when employees request spend — no more tracking in spreadsheets.</li>
>   <li><strong>3-way match</strong> on bill import — Ramp matches invoice → PO → receipt and flags mismatches before you pay.</li>
>   <li><strong>Approval routing tied to the PO</strong> — not to the bill after the fact. Pre-spend control, not post-spend cleanup.</li>
>   <li><strong>Contract + renewal tracker</strong> — renewal dates and contract terms attached to the vendor so they stop sneaking by.</li>
> </ul>
>
> <strong>Next step</strong>
> <ul>
>   <li>30 min to walk through how your existing bill flow gets rebuilt around POs. Tuesday or Wednesday of next week?</li>
> </ul>
>
> Book a call: https://ramp-com.chilipiper.com/me/gregory-nallie/ramp
>
> Greg
> Account Manager @ Ramp

### Context B — Post-meeting follow-up (worked example)

Organize the body BY PRODUCT DISCUSSED, not by recap/next-steps blocks. Every product the call meaningfully touched gets its own section. Bullets pair the customer's stated situation with the specific Ramp capability. Narrative paragraphs are fine when the topic is a value framing rather than a feature list.

Subject: `Ramp Follow-Up — Treasury, Procurement + Vendor Optimization`

> Hi James and Amber, great meeting with you today — thanks for walking me through how you're using Ramp and the Truist vendor mix.
>
> <strong>Treasury — 2% yield vs. your current 1.7%</strong>
> <ul>
>   <li>You're running about $500/year in ACH + check fees on ~27 monthly bills — Treasury's fee-free ACH eliminates that line entirely.</li>
>   <li>Currently earning 1.7% on Truist money market and 0% on checking — <a href="https://ramp.com/treasury">Ramp Treasury</a>'s 2% with same-day liquidity means no trade-off between yield and access.</li>
>   <li>You mentioned being open to moving <em>just a portion</em> of funds to start — the business account is designed for exactly that: no minimums, no lockup.</li>
> </ul>
>
> <strong>Procurement — PO workflow + 3-way match</strong>
> <ul>
>   <li>We covered how your team currently runs approvals in email threads — <a href="https://ramp.com/procurement">Procurement</a> replaces that with intake forms that auto-generate POs and route approval to the right approver.</li>
>   <li>You asked about invoice-to-PO matching — 3-way match compares bill ↔ PO ↔ receipt on import and blocks payments that exceed your thresholds.</li>
>   <li>Contract + renewal tracking was the other piece — vendor terms attached to the PO so renewals don't sneak up.</li>
> </ul>
>
> <strong>Card Consolidation</strong>
>
> Worth revisiting with the team — your AP team already caught a vendor accepting fee-free cards through our AI suggestion, which was exactly the kind of optimization Ramp's built for. Happy to pull the full list of 7-8 vendors you're paying via ACH that other customers pay by card — that's an immediate cashback opportunity sitting in your current vendor file.
>
> Ready to dive deeper into Treasury setup whenever you've had a chance to review the vendor list. Happy to grab 30 min later this week to walk through how the 2% business account sits alongside your current Truist setup and where Procurement would plug in for your AP team — any time good for you?
>
> Let me know what works best!
>
> Greg
>
> Book a call: https://ramp-com.chilipiper.com/me/gregory-nallie/ramp

**What's different from the old structure:** no "Quick recap" / "James's Next Steps" / "My Next Steps" formal headers — each product gets its own section titled with the specific angle, action items live inside the product context (or in the close paragraph), and hyperlinks are woven inline instead of tacked on at the end. Three products discussed = three sections. If a product came up only in passing, skip it.

**When to use a narrative paragraph vs. bullets:** bullets for feature-dense discussions (Treasury yield math, Procurement feature fit). Paragraphs for value-framing topics (Card Consolidation — the pitch is "consolidation beats cashback difference", which reads better as prose than a feature list).

**Resources:** hyperlinks embedded inline in bullets is the default. Only add a separate `<p><strong>Helpful resources:</strong></p>` block at the end if there are 2+ useful links that didn't find a natural home in the product sections.

### Context C — Re-engage stale (worked example)

Open SFDC opp + 15+ days since last touch.

Subject: `Picking back up on Treasury for Aerodyne`

> Hi Jane,
>
> Realized it's been about three weeks on our end since we talked through the Treasury setup — wanted to pick back up before your month-end close lands.
>
> <strong>Where we left off</strong>
> <ul>
>   <li>You were running the 4.5% yield math against your ~$8M balance with your team — ~$30K/mo in recovered yield was the number we landed on.</li>
>   <li>Open question was the FDIC pass-through structure vs. your current Wells Fargo sweep — I've since put together the side-by-side.</li>
> </ul>
>
> <strong>What's changed worth revisiting</strong>
> <ul>
>   <li>Treasury's multi-entity account setup shipped last week — if your 3 subsidiaries need separate books, we can model that now.</li>
>   <li>FDIC pass-through expanded to $225M coverage across partner banks — resolves the concentration question you raised.</li>
> </ul>
>
> <strong>Next step</strong>
> <ul>
>   <li>30-min working session to walk through the multi-entity setup with your actual balances. Thursday morning or Friday afternoon this week?</li>
> </ul>
>
> Book a call: https://ramp-com.chilipiper.com/me/gregory-nallie/ramp
>
> Greg
> Account Manager @ Ramp

### Context detection (how code picks which prompt to use)

Implemented in `jobs/pipeline_drafter.py` at draft time:

1. **If `play_id` is set AND the opp is synthetic** (play_hooks-built, no real SFDC opp on the pitched product) → **Context A: Prospecting**.
2. **If the payload carries `current_meeting_attendees` / `current_call_participants`** (came from `granola_followup` or `post_meeting_followup`) → **Context B: Post-meeting**. (Note: those drafters don't route through pipeline_drafter today — they have their own prompts. Keep it that way.)
3. **Otherwise** (real open opp, no current meeting) → **Context C: Re-engage stale**. If a `play_id` is also set, use the play hook's pitch angle to drive the "What's changed worth revisiting" section.

### What NOT to do

- Don't use "Takeaways" as a section header unless there's a real conversation to take aways FROM. For prospecting, it's "Why I'm reaching out" + "Why Ramp for you". For re-engage stale, it's "Where we left off" + "What's changed worth revisiting".
- Don't lead with "Hope you're doing well!" or "I wanted to reach out to you about…". Start with the signal or the reason.
- Don't stuff the email with every feature Ramp sells. 3-4 features max, all pairing to a specific pain or signal on the account.
- Don't name dollar figures the customer didn't give you. ("Our system sees $8M in your accounts" is fine if it's in BoB data — but don't quote $8M as a customer statement if they never said it.)
- Don't apologize for gaps in re-engage emails. Own the gap and move forward.

---

## 1. Channel Alert Drafters (Fixed Templates)

These use **fixed HTML templates** from `templates/emails.py`. No AI generation — same structure every time.

### 1a. ACH-to-Card
- **Trigger:** `#alerts-card-payable-bills` — bill in drafts to a vendor that accepts card
- **Gmail Label:** `Claude Drafts/ACH to Card`
- **Recipients:** Bill creator + admin contacts (parsed from alert text)
- **Subject:** `Quick Win: Earn Cash Back [URGENT]`
- **Body:**
  > Hi {name},
  >
  > I was just notified of the bill in your drafts to **{vendor}** for **{amount}** due on **{date}**. This vendor has been flagged as one that will typically accept credit card payments without fees and this would net you ~**{cashback} in cashback**:
  >
  > 1. Edit the bill, change payment method to Ramp card, and select an existing card or create a single use card.
  > 2. Use the card to pay the bill with the vendor (or via [payment portal link] if available).
  > 3. Once approved and paid, match the transaction to the bill in Ramp.
  >
  > Here's a [handy guide](https://support.ramp.com/hc/en-us/articles/28105415406867) in case you want to pay this or other vendors in the future.
  >
  > Can you please let me know if you plan to pay this with a card, if you need your limit increased, or if it'd be helpful to walk through it together?

### 1b. Procurement Trial
- **Trigger:** `#alerts-self-serve-procurement-trials` — trial activated
- **Also used by:** Prospecting tab "Draft" button for `active_procurement_trial` signals
- **Gmail Label:** `Claude Drafts/Procurement Trials`
- **Recipients:** Activating user + admin contacts from alert (channel). SFDC contacts (prospecting tab).
- **Subject:** `Ramp Procurement Trial + AM intro`
- **Body:**
  > Hi {name},
  >
  > I noticed your Procurement trial was just activated -- congrats! Procurement is one of the highest-impact products on the Ramp platform and I want to make sure you get the most out of your trial period.
  >
  > Here are **3 priorities** to focus on:
  > 1. **Set up approval workflows** — Configure your approval chains so every purchase request routes to the right person.
  > 2. **Connect vendor contracts** — Upload existing vendor contracts so Ramp can track renewals, flag duplicate software, and surface savings.
  > 3. **Run your first intake request** — Submit a real purchase request to see the full end-to-end experience.
  >
  > I'd love to walk through setup together. Book time: [{booking_link}]
  >
  > **Helpful Resources:**
  > - [Getting Started](https://support.ramp.com/hc/en-us/articles/37424276359443)
  > - [Quick Start Guide](https://support.ramp.com/hc/en-us/articles/49355243914387)
  > - [Best Practices](https://support.ramp.com/hc/en-us/articles/49437597525907)

### 1c. PCLIP (Program Credit Limit Increase)
- **Trigger:** `#alerts-pclip-activations` — limit increase >= $100k
- **Gmail Label:** `Claude Drafts/PCLIP Activation`
- **Recipients:** Primary POC from SFDC (looked up by account)
- **Subject:** `Ramp Limit Increase + AM intro`
- **Body:**
  > Hi {name},
  >
  > I wanted to introduce myself as your new Account Manager at Ramp! I saw a notification about your limit increase and wanted to share a few potential ideas to utilize the new limit:
  >
  > - Migrate any personal reimbursements to Ramp cards for control & cashback
  > - Consolidate spend from other cards into Ramp for automation
  > - Pay vendors via Ramp card instead of ACH/wire for cashback & better terms
  >
  > If you want to [schedule a call], I can run a vendor audit to see which vendors you're paying via ACH or wire may accept card. We can also talk about improvements to your Ramp setup.

### 1d. RCLIP (Reactive Credit Limit Increase)
- **Trigger:** `#alerts-rclip-requests` — Status=Approved, delta >= $50k
- **Gmail Label:** `Claude Drafts/RCLIP`
- **Recipients:** Requesting user email from alert
- **Subject:** `Ramp Limit Increase + AM intro`
- **Body:** Same as PCLIP (1c above) — "I saw your limit increase just went through..."

### 1e. Large Decline — Case A (Velocity Limit)
- **Trigger:** `#alerts-large-declines` — decline reason = velocity/per-transaction limit
- **Gmail Label:** `Claude Drafts/Large Declines`
- **Recipients:** Cardholder email from alert
- **Subject:** `Declined transaction — potential limit increase`
- **Body:**
  > Hi {name},
  >
  > I noticed a transaction for **{amount}** to **{vendor}** was recently declined due to a velocity limit on the card. Here are **3 options** to fix this:
  >
  > 1. **Request a card limit increase** — you or your admin can request one in Ramp, or I can push it through.
  > 2. **Use a different card** — another Ramp card with a higher limit.
  > 3. **Contact me directly** — reply or [book time].

### 1f. Large Decline — Case B (Insufficient Balance)
- **Trigger:** `#alerts-large-declines` — decline reason = open-to-buy / insufficient limit, amount > $25k
- **Gmail Label:** `Claude Drafts/Large Declines`
- **Recipients:** Cardholder email from alert
- **Subject:** `Declined transaction — potential limit increase`
- **Body:**
  > Hi {name},
  >
  > I noticed a transaction for **{amount}** to **{vendor}** was declined because it exceeded the available balance (**{available_limit}**). To help find the best solution:
  >
  > 1. **What is the business purpose of this transaction?**
  > 2. **Is this a recurring need?**
  > 3. **What limit would work for you?**
  >
  > Reply here or [book time].

### 1g. Fundraise
- **Trigger:** `#alerts-fundraising` — funding round detected
- **Gmail Label:** `Claude Drafts/Fundraise`
- **Recipients:** Primary POC from SFDC
- **Subject:** `Re: Fundraise + AM intro`
- **Body:**
  > Hi {name},
  >
  > Congrats on the recent funding news! Ramp is here to help you figure out how to spend less:
  >
  > - Renewal alerts so nothing auto-renews without approval
  > - 2%+ earn on idle cash
  > - Insights to extend your runway, not just track spend
  >
  > Open to setting up a call? [{booking_link}]

### 1h. Automatic Card Loss
- **Trigger:** `#bill-pay-automatic-card-losses` — customer ignored card payment prompt
- **Gmail Label:** `Claude Drafts/Automatic Card`
- **Recipients:** Bill creator email from alert. CC: bill approver (if different).
- **Subject:** `Ramp AM Intro`
- **Body:**
  > Hi {name},
  >
  > Flagging a quick save: a recent {vendor} invoice was eligible for fee-free card payment. Paying by card would have earned ~**${cashback} in cashback**.
  >
  > - [View Bill link]
  >
  > We'd like to understand why card payment wasn't selected. A quick number reply works great:
  > 1. I didn't see the prompt
  > 2. I wasn't sure how bill payments via credit card worked
  > 3. I wasn't sure vendor accepts card without fees
  > 4. I prefer ACH for this vendor
  > 5. Earning cashback wasn't a priority
  > 6. Other

### 1i. AM Escalation
- **Trigger:** `#am-escalations` — CX support ticket escalated to AM
- **Gmail Label:** `Claude Drafts/AM Escalation`
- **Recipients:** Customer email from escalation ticket
- **Subject:** `Ramp AM Intro`
- **Body:**
  > Hi {name}, great to meet you — our support team let me know {escalation_context}. Could you [select a time through this link] and we'll chat through it?

---

## 2. Smart Drafts (Claude-Generated)

These use **Claude AI** to generate the email body. The structure is fixed (opening -> context insert -> discussion topics -> CTA -> sign-off) but the **context insert** varies per account based on Gong calls, SFDC notes, and email history.

### Common Structure (all smart drafts)
- **Subject:** `Ramp AM Intro`
- **Gmail Label:** `Claude Drafts/Prospecting` (default) or `Claude Drafts/Post Meeting` (for followup/post_meeting_opp)
- **Recipients:** Best-scored SFDC contact (TO), up to 3 additional stakeholders (CC)
  - Contact scoring: owner title +100, met on Gong +50, recent emails +30, admin title +20
- **Context gathered (~15 sec):** SFDC notes, last 3 Gong calls (90 days), last 3 emails (90 days), SFDC contact list
- **Body template:**

  > Hi {name}, great to meet you! I was recently assigned as your Account Manager for the team and I wanted to reach out and make an intro.
  >
  > [1-2 sentence context insert — varies by category, see below]
  >
  > I had some ideas for potential discussion/optimization areas and wanted to see if you're open to briefly connecting on:
  > - Migration/onboarding assistance if moving things over
  > - Adding any other businesses to Ramp
  > - Uncover savings from migrating ACH payments -> card
  > - Best practices to achieve the most value + time savings
  > - Glimpse of the roadmap for 2026
  >
  > Feel free to select any time through [this link] or let me know when works for you, looking forward to it!

  **If you've met the contact before** (detected via Gong), the opening references the prior conversation instead.

### Category-Specific Context Framing

Each category gives Claude a different **goal** and **tone** for the 1-2 sentence context insert:

| Category | Goal / Framing | Surfaces |
|----------|---------------|----------|
| **Dashboard / Priority Alerts** | | |
| `early_accel` | Spend accelerating sharply L7D, baseline still low. Window open NOW. | Dashboard, DMs |
| `close_window` | Open opp, L7D ramping above L30D. Close before baseline rises. Urgent. | Dashboard, DMs |
| `close_now` | Spend already exceeds baseline. Close ASAP. | Dashboard, DMs |
| `leading` | Large bills/card transactions incoming. Leading indicator. | Dashboard, DMs |
| `first_bill` | First bill payment. Open BP opp. Help them ramp up. | Dashboard, DMs |
| `zero_to_one` | New product activation. Lock in low baseline. | Dashboard, DMs |
| `sustained_accel` | Spend elevated for a while. L30D catching up to L7D. Window closing. | Dashboard, DMs |
| `treasury_spike` | GLA balance L7D > 2x L30D. Treasury uncapped H1-26. | Dashboard, DMs |
| `underperforming_d30` | D30 checkpoint, spend below target. 60 days left. Re-engage. | Dashboard |
| `underperforming_d60` | D60 checkpoint, spend below target. 30 days left. Re-engage. | Dashboard |
| `multi_product` | Multiple expansion signals. Bundle the conversation. | Dashboard |
| `reopen` | Previous opp closed 60-120 days ago. Check in on usage. | Dashboard |
| **Stale Opps** | | |
| `stale` | Re-engage a stalled opp. Acknowledge time gap naturally. | Stale Opps tab |
| **Post-Meeting** | | |
| `followup` | Follow up after a meeting. Reference what was discussed. | Post-meeting |
| `post_meeting_opp` | Call where expansion products discussed, no opp yet. | Post-meeting |
| **Prospecting Tab** | | |
| `prospect_tts_plus_procurement` | High propensity for Plus/Procurement. Frame as unlocking more value. | Prospecting |
| `prospect_high_competitor_spend` | Significant off-Ramp spend. Consolidate onto Ramp. Don't trash competitors. | Prospecting |
| `prospect_low_cashback_no_plus` | Low cashback, not on Plus. Value they're leaving on the table. | Prospecting |
| `prospect_high_gla_grandfathered` | High GLA, grandfathered Plus. Treasury uncapped H1-26. | Prospecting |
| `prospect_erp_no_billpay` | Has ERP but no Bill Pay. Natural extension that closes the accounting loop. | Prospecting |
| `prospect_erp_no_plus` | Has ERP but not on Plus. Advanced features would supercharge ERP. | Prospecting |

**Note:** `prospect_active_procurement_trial` uses the **fixed template** (same as channel alert 1b), not Claude-generated.

---

## 3. Post-Meeting Follow-Up Drafts

- **Trigger:** Gong call ends (~10-15 min via Gumstack) or Granola transcript (every 3 min)
- **Gmail Label:** `Claude Drafts/Post Meeting`
- **Recipients:** External call participants from transcript
- **Subject:** `Ramp AM Intro`
- **Body:** Claude-generated based on call transcript, action items, products discussed

---

## 4. Gmail Label Summary

| Label | Source |
|-------|--------|
| `Claude Drafts/ACH to Card` | Channel alert |
| `Claude Drafts/Procurement Trials` | Channel alert + Prospecting tab |
| `Claude Drafts/PCLIP Activation` | Channel alert |
| `Claude Drafts/RCLIP` | Channel alert |
| `Claude Drafts/Large Declines` | Channel alert |
| `Claude Drafts/Fundraise` | Channel alert |
| `Claude Drafts/Automatic Card` | Channel alert |
| `Claude Drafts/AM Escalation` | Channel alert |
| `Claude Drafts/Prospecting` | Dashboard + Prospecting tab drafts |
| `Claude Drafts/Post Meeting` | Post-meeting follow-ups |

---

## Editing Guide

| What to change | Where to edit |
|----------------|---------------|
| Fixed template body/copy | `templates/emails.py` — edit the template function |
| Smart draft structure/prompt | `handlers/interactive.py` → `_draft_smart_email()` prompt (~line 1770) |
| Category goal/tone | `handlers/interactive.py` → category-specific `goal` and `tone_note` (~lines 1580-1740) |
| Subject lines (channel alerts) | `jobs/email_drafters.py` — search for `subject =` |
| Subject lines (smart drafts) | `handlers/interactive.py` — search for `subject =` |
| Recipient logic (channel alerts) | `jobs/email_drafters.py` — in each handler function |
| Recipient scoring (smart drafts) | `handlers/interactive.py` → `_contact_score()` |
| Help article links appended | `templates/help_links.py` |
| Discussion bullet points | `handlers/interactive.py` → prompt section "DISCUSSION TOPICS" |
| Booking link | Per-user in `~/.gary_bot_users.json`, falls back to `config.py BOOKING_LINK` |
