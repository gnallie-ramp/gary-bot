# Email Drafting Guide — Gary Bot

This document maps every email draft category, the context gathered, the prompt/template used, and the format. Edit any section to change how Gary Bot writes emails.

---

## How Drafting Works

All drafts are created via **Gumstack Gmail MCP** (`core/gumstack_gmail.py`) and labeled `Claude Drafts/Post Meeting`. There are two drafting paths:

1. **Signal-based drafts** (from `/priorities`, home tab, real-time DMs) → AI-generated via `_draft_smart_email()` in `handlers/interactive.py`
2. **Channel monitor drafts** (from 5 alert channels) → Template-based from `templates/emails.py`

---

## Part 1: Signal-Based Drafts (AI-Generated)

These are triggered by "Draft Outreach Email" buttons across all signal categories. The AI generates each email using the same core prompt, with a **category-specific goal and tone note** that changes the framing.

### Context Gathered (same for all categories)

| Source | What's Pulled | Used For |
|--------|--------------|----------|
| SFDC Contacts | Contact name, email, title | To/recipient selection |
| Gong Calls (last 3, 90 days) | Call names, dates, summaries, product requests, competitors, external participants | Meeting references, pain points, who Greg has met |
| SFDC Account Notes | AM/CSM notes, next steps | Account context |
| Recent Emails (last 3) | Direction, date, subject, body | Recent comms history |
| Contact Priority | Gong participants preferred > any valid contact | Who to email |

### Base Email Template

All signal drafts now follow this structure:

```
Hi {{first_name}}, great to meet you! I was recently assigned as your Account Manager
for the team and I wanted to reach out and make an intro.

[1-2 sentences of context based on the signal — what Gary noticed about their account
that's relevant and helpful, written naturally so it doesn't feel like surveillance]

I had some ideas for potential discussion/optimization areas and wanted to see if you're
open to briefly connecting on:
• Migration/onboarding assistance if moving things over
• Adding any other businesses to Ramp
• Uncover savings from migrating ACH payments → card
• Best practices to achieve the most value + time savings
• Glimpse of the roadmap for 2026

Feel free to select any time through this link or let me know when works for you,
looking forward to it!

Best,
Greg
```

**If Greg has met with the contact before** (detected via Gong participants), the opening changes to reference the prior meeting instead of "great to meet you."

### Category-Specific Context Inserts

The 1-2 sentence context section varies by signal type. Here's what each category emphasizes:

---

#### 1. `early_accel` — Early Acceleration Outreach
**Signal:** L7D spend pacing >1.5x baseline, L30D still low (window open)
**Context insert focus:** Their recent growth on Ramp, mention the acceleration naturally. Urgency: the window is open NOW to discuss optimizing their setup.
**Tone:** Lead with growth/success, offer to help optimize or expand.

#### 2. `close_window` — Close Window Outreach
**Signal:** Open opp, L7D ramping above L30D
**Context insert focus:** Their active expansion and wanting to lock in current pricing/baseline. Be direct about scheduling a quick call this week.
**Tone:** Frame as wanting to finalize their expansion. Direct, time-sensitive.

#### 3. `close_now` — Close Now Outreach
**Signal:** Open opp, L30D already exceeds baseline
**Context insert focus:** Their spend has exceeded expectations — frame as finalizing the expansion. They're already seeing great results.
**Tone:** Push for same-day or next-day action. Celebratory but urgent.

#### 4. `leading` — Leading Indicator Outreach
**Signal:** Large bills/card volume queued above typical monthly baseline
**Context insert focus:** Noticed increased activity on their account, suggest a quick call to discuss expansion.
**Tone:** Lead with growth, mention activity naturally.

#### 5. `first_bill` — First Bill Outreach
**Signal:** Open bill pay opp, first bill just created
**Context insert focus:** Congratulate on getting started with bill pay. Offer to help ramp up.
**Tone:** Congratulatory, helpful.

#### 6. `zero_to_one` — Zero-to-One Outreach
**Signal:** Product activated after opp created
**Context insert focus:** Their new product activation. Congratulate naturally, pivot to helping them get more value.
**Tone:** Congratulatory, then pivot to expansion.

#### 7. `sustained_accel` — Sustained Acceleration Outreach
**Signal:** L7D pacing >1.5x baseline, L30D already elevated
**Context insert focus:** Their sustained growth on Ramp. Frame urgency around locking in current rates/baseline.
**Tone:** Lead with growth, mention locking in current setup.

#### 8. `treasury_spike` — Treasury GLA Spike Outreach
**Signal:** GLA balance L7D avg >2x L30D avg, delta >$100K
**Context insert focus:** Their growth/success. Mention treasury optimization naturally.
**Tone:** Direct about scheduling a quick call. High-value signal.

#### 9. `underperforming_d30` / `underperforming_d60` — Post-Close Checkpoint
**Signal:** CW opp at D30/D60 with spend below 80% of target SOW pace
**Context insert focus:** Check-in to help them get more value from Ramp. Ask about blockers, offer enablement.
**Tone:** Helpful, not accusatory. "Want to make sure you're getting the most out of Ramp."

#### 10. `multi_product` — Multi-Product Bundle Outreach
**Signal:** Account appears in 2+ signal categories
**Context insert focus:** Breadth of their Ramp usage. Frame expansion as a holistic play.
**Tone:** Lead with multi-product value, not separate asks.

#### 11. `stale` — Re-engage Stale Opp
**Signal:** S2+ opp gone silent
**Context insert focus:** Acknowledge the time gap naturally (don't apologize). Reference last call/email context if available.
**Tone:** Own the gap, propose getting back on calendar.

#### 12. `followup` — Post-Meeting Follow-up
**Signal:** Gong call without follow-up email
**Context insert focus:** Reference what was discussed in the meeting, propose a concrete next step.
**Tone:** Meeting-specific, action-oriented.

#### 13. `reopen` — Re-open Outreach
**Signal:** CW opp 60-120d ago with spend patterns worth re-opening
**Context insert focus:** Reference their history with the product naturally. Frame as a check-in.
**Tone:** Check-in, not re-sell.

#### 14. `post_meeting_opp` — Post-Meeting Opp Creation
**Signal:** Expansion products discussed on Gong call, no opp exists
**Context insert focus:** Reference what was discussed on the call. Propose a follow-up to scope expansion.
**Tone:** Meeting-referencing, exploratory.

#### 15. `prospect` (default/fallback)
**Signal:** Account accelerating with no open opp
**Context insert focus:** Lead with their success/growth on Ramp, offer to help optimize or expand.
**Tone:** Growth-focused, exploratory.

---

## Part 2: Channel Monitor Drafts (Template-Based)

These are triggered by Slack alert channel monitors. They use fixed HTML templates from `templates/emails.py` (not AI-generated).

### 1. ACH-to-Card (`#alerts-card-payable-bills`)

**Trigger:** Bill created for a vendor flagged as card-payable
**Gmail Label:** `Claude Drafts/ACH to Card`
**Subject:** `Quick Win: Earn Cash Back [URGENT]`
**File:** `templates/emails.py:32` → `jobs/email_drafters.py:275`

**Email Body:**

```
Hi {{first_name}} / Hi {{first_name}} and {{second_name}},

I was just notified of the bill in your drafts to {{vendor_name}} for
{{invoice_value}} due on {{due_date}}. This vendor has been flagged as one
that will typically accept credit card payments without fees and this would
net you ~{{cashback_estimate}} in cashback:

1. Edit the bill, change payment method to Ramp card, and select an existing
   card to use or create a single use card. If using a single use card —
   once the bill is approved, you'll be able to see its card number.
2. Use the card to pay the bill inside the vendor's payment portal [link]
   (or "with the vendor" if no portal link available).
3. Once the bill is both approved in Ramp and paid, you can search for and
   match the relevant transaction to it. Once the transaction is matched,
   Ramp will mark the bill as paid in both Ramp and your accounting provider.

Here's a handy guide [link to Ramp support article] in case you want to pay
this or other vendors in the future.

Can you please let me know if you plan to pay this with a card, if you need
your limit increased, or if it'd be helpful to walk through it together?

Best,
Greg
```

**Dynamic fields:** `{{first_name}}`, `{{vendor_name}}`, `{{invoice_value}}`, `{{due_date}}`, `{{cashback_estimate}}`, `{{payment_portal_link}}`

---

### 2. Procurement Trial (`#alerts-self-serve-procurement-trials`)

**Trigger:** Customer activates a Procurement trial
**Gmail Label:** `Claude Drafts/Procurement Trials`
**Subject:** `Ramp Procurement Trial + AM intro`
**File:** `templates/emails.py:92` → `jobs/email_drafters.py:377`

**Email Body:**

```
Hi {{first_name}} / Hi {{first_name}} and {{second_name}},

I noticed your Procurement trial was just activated -- congrats! Procurement
is one of the highest-impact products on the Ramp platform and I want to
make sure you get the most out of your trial period.

Here are 3 priorities to focus on to get the most value out of your trial:

1. Set up approval workflows — Configure your approval chains so every
   purchase request routes to the right person. This is the foundation of
   Procurement and ensures nothing slips through without proper sign-off.
2. Connect vendor contracts — Upload your existing vendor contracts so Ramp
   can track renewals, flag duplicate software, and surface savings
   opportunities automatically.
3. Run your first intake request — Submit a real purchase request through
   the workflow to see the full end-to-end experience. This is the fastest
   way to understand how it will work for your team day-to-day.

I'd love to walk through setup together and answer any questions. Feel free
to book time directly on my calendar here: [booking link]

Helpful Resources
• Getting Started — https://support.ramp.com/hc/en-us/articles/37424276359443
• Quick Start Guide — https://support.ramp.com/hc/en-us/articles/49355243914387
• Best Practices — https://support.ramp.com/hc/en-us/articles/49437597525907

Best,
Greg
```

**Dynamic fields:** `{{first_name}}` (greeting built from activating user + admin contacts)

---

### 3. PCLIP Activation (`#alerts-pclip-activations`)

**Trigger:** Program credit limit increase
**Gmail Label:** `Claude Drafts/PCLIP Activation`
**Subject:** `Ramp Limit Increase + AM intro`
**File:** `templates/emails.py:131` → `jobs/email_drafters.py:509`

**Email Body:**

```
Hi {{first_name}},

I wanted to introduce myself as your new Account Manager at Ramp! I saw a
notification about your limit increase and wanted to share a few potential
ideas to utilize the new limit:

• Migrate any personal reimbursements to Ramp cards for control & cashback
• Consolidate spend from other cards into Ramp for automation & eliminate
  unnecessary systems/workflows
• Pay vendors via Ramp card instead of ACH/wire for cashback & better terms

If you want to schedule a call [booking link], I can run a vendor audit to
see which vendors that you're paying via ACH or wire may accept card
payments with no additional fees. We can also talk about some improvements
to your Ramp setup to put you in a good position to scale and make sure
you're using Ramp to its fullest potential.

Looking forward to hearing from you!

Thanks,
Greg
```

**Dynamic fields:** `{{first_name}}`

---

### 4a. Large Decline — Case A: Velocity Limit (`#alerts-large-declines`)

**Trigger:** Transaction declined due to per-transaction velocity limit
**Gmail Label:** `Claude Drafts/Large Declines`
**Subject:** `Declined transaction — {{vendor_name}} ({{amount}})`
**File:** `templates/emails.py:169` → `jobs/email_drafters.py:681`

**Email Body:**

```
Hi {{first_name}},

I noticed a transaction for {{amount}} to {{vendor_name}} was recently
declined due to a velocity limit on the card. I want to help get this
resolved quickly so the payment can go through.

Here are 3 options to fix this:

1. Request a card limit increase — If the card's transaction or daily limit
   is too low for this payment, you or your admin can request a limit
   increase directly in Ramp. I can also help push this through on my end.
2. Use a different card — If you have another Ramp card with a higher limit
   or available balance, you can retry the payment with that card instead.
3. Contact me directly — Reply to this email or book time on my calendar
   and I'll help troubleshoot and get the payment unblocked: [booking link]

Best,
Greg
```

**Dynamic fields:** `{{first_name}}`, `{{vendor_name}}`, `{{amount}}`

---

### 4b. Large Decline — Case B: Insufficient Balance (`#alerts-large-declines`)

**Trigger:** Transaction declined due to insufficient open-to-buy balance
**Gmail Label:** `Claude Drafts/Large Declines`
**Subject:** `Declined transaction — potential limit increase`
**File:** `templates/emails.py:241` → `jobs/email_drafters.py:689`

**Email Body:**

```
Hi {{first_name}},

I noticed a transaction for {{amount}} to {{vendor_name}} was recently
declined because it exceeded the available balance on the card. Your
current available limit is {{available_limit}}. I'd like to help get this
sorted out.

To help me find the best solution, I have a few quick questions:

1. What is the business purpose of this transaction? — Understanding the
   context helps me determine the best path to getting the limit adjusted.
2. Is this a recurring need? — If you expect similar transactions in the
   future, we can set up a card or limit structure that accommodates this
   ongoing.
3. What limit would work for you? — Let me know the amount you need and I
   can work on getting the limit increased so this doesn't happen again.

Feel free to reply here or book time on my calendar so we can get this
resolved: [booking link]

Best,
Greg
```

**Dynamic fields:** `{{first_name}}`, `{{vendor_name}}`, `{{amount}}`, `{{available_limit}}`

---

### 5. Fundraise (`#alerts-fundraising`)

**Trigger:** Customer raises funding round (detected via fundraising alert)
**Gmail Label:** `Claude Drafts/Fundraise`
**Subject:** `Re: Fundraise + AM intro`
**File:** `templates/emails.py:208` → `jobs/email_drafters.py:810`

**Email Body:**

```
Hi {{first_name}},

Congrats on the recent funding news! We deeply appreciate your partnership
with Ramp and want to make sure we're continuing to deliver for you.

You're probably getting a ton of congrats from people ultimately trying to
get you to spend more money -- Ramp is here to help you figure out how to
spend less.

• Renewal alerts so nothing auto-renews without approval
• 2%+ earn on idle cash
• Insights to extend your runway, not just track spend

Open to setting up a call in the next week or two to chat through what's
top of mind for your team and how Ramp can support? Feel free to select any
time through this link [booking link] or let me know when works for you,
looking forward to it!

All the best,
Greg
```

**Dynamic fields:** `{{first_name}}`

---

## How to Adjust

- **Signal draft tone/goals:** Edit the `goal` and `tone_note` strings in `handlers/interactive.py` inside `_draft_smart_email()` (line ~1277-1398)
- **Base email template/rules:** Edit the `prompt` string in `handlers/interactive.py` (line ~1408-1440)
- **Channel monitor templates:** Edit the template functions in `templates/emails.py`
- **Subject line:** Hardcoded as `"Ramp AM Intro"` in `handlers/interactive.py` (line ~1475)
- **Help article links appended:** Edit `templates/help_links.py`
- **Booking link:** `BOOKING_LINK` in `templates/emails.py` line 10
