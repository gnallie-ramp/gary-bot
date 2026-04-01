# Email Drafting Guide

All email drafts are created in Gmail under `Claude Drafts/*` labels. Each workflow has its own label, subject line, recipient logic, and body format. Edit this doc to change the copy — then tell Glass to update the corresponding template/prompt in code.

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
