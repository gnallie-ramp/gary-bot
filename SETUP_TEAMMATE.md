# Gary Bot — Join an Existing Instance

Join Greg's Gary Bot in ~15 minutes. You don't need to clone the repo, create a Slack app, or run anything on your machine long-term. You register in Slack, auth a couple of services, and send Greg three files.

> **Want your own independent instance instead?** See [SETUP.md](SETUP.md) — more setup, but you get Granola integration, full customization, and it runs on your machine independently.

---

## Considerations + Limitations

Before choosing this path, understand the trade-offs vs running your own instance ([SETUP.md](SETUP.md)):

| | Shared App (this guide) | Own Instance (SETUP.md) |
|--|------------------------|------------------------|
| **Setup time** | ~15 minutes | ~30 minutes |
| **Runs on** | Greg's computer — only works while his machine is on | Your computer — works while yours is on |
| **Post-meeting follow-ups** | Gong API (~15 min delay after call ends) | Granola (~3 min, immediate local files) + Gong API fallback |
| **Customization** | Use default settings; can't change signal thresholds, prompts, or schedules | Full control over queries, thresholds, prompts, schedules |
| **Troubleshooting** | Requires Greg's involvement for most issues | You can debug and fix issues yourself |
| **Updates** | Automatic — Greg pulls updates and you get them instantly | Manual — you pull updates yourself |
| **Slack app** | Shared — uses Greg's Slack app | Your own Slack app with your own name |
| **Slash commands** | Shared namespace (e.g. `/priorities` goes to Greg's app) | Your own commands, no conflicts |

---

## Manual Steps (Do These Yourself)

These steps require a browser and can't be automated. Do them first, then paste the automated section into Project Glass.

### 1. Get your info ready

You'll need these values. Gather them now:

| Value | Where to find it | Example |
|-------|-----------------|---------|
| **Salesforce Name** | Your full name exactly as it appears in Salesforce. Go to Setup > My Personal Information, or check any account you own — the Owner field shows your exact name. | `Jane Smith` |
| **Slack Member ID** | In Slack: click your profile photo > click the three dots (⋮) > **Copy member ID** | `U03JBULM9LP` |
| **SFDC User ID** | In Salesforce: click your avatar > **Settings** > scroll to the URL — it contains your 18-char ID starting with `005`. Or: Setup > My Personal Information > User ID field. | `0056g000007aBCDAA2` |
| **ChiliPiper Link** | Your booking URL from ChiliPiper | `https://ramp-com.chilipiper.com/me/jane-smith/ramp` |
| **Ramp Email** | Your @ramp.com email | `jsmith@ramp.com` |

### 2. Register in Slack

1. Open Slack and find **Gary Bot** in your Apps (search "Gary")
2. Click the **Home** tab
3. Click **Get Started**
4. Fill in all fields (Salesforce Name, First Name, Email, Booking Link, SFDC User ID)
5. Click **Submit**

Your Home tab will refresh with your personalized dashboard. All query-based features work immediately.

### 3. Connect Gmail and Gong on Gumstack

1. Go to [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps)
2. Log in with your Ramp account
3. Find **Gmail** and click **Connect** > authorize with your Ramp Google account
4. Find **Gong** and click **Connect** > authorize with your Gong account

You only need Gmail and Gong — other apps (Slack, Notion, Salesforce) are not required for the shared bot.

### 4. Connect Google Calendar (for pre-meeting briefs)

This step requires a browser sign-in that can't be automated:

1. Open Terminal
2. Run: `gcloud auth application-default login --scopes=https://www.googleapis.com/auth/calendar.readonly`
3. Sign in with your **Ramp Google account** in the browser that opens

If `gcloud` isn't installed, the automated section below will install it.

### 5. Request access through Okta — ConductorOne

Request these entitlements (approval may take a few hours):

- **Snowflake** — General access (not unmasked)
- **Claude Code API** — you'll need an API key from [console.anthropic.com](https://console.anthropic.com)

### 6. Install Project Glass

Install **Project Glass** through the **Self Service+.app** on your computer.

---

## Automated Setup (Paste Into Project Glass)

> **If you get any errors or issues:** Send a screenshot to Glass, explain what you were trying to do and what happened. Glass should fix it or provide next steps.

Turn on **YOLO mode** and paste everything below into Project Glass:

```
I need to set up Gary Bot teammate access. Please run the following steps:

1. Install Node.js if not already installed:
   brew install node

2. Install Google Cloud SDK if not already installed:
   brew install google-cloud-sdk
   source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"

3. Run the Gumstack Gmail MCP auth flow to generate token files:
   npx mcp-remote https://mcp.gumloop.com/gmail/mcp
   (This will open a browser — I'll authorize there)

4. Run the Gumstack Gong MCP auth flow to generate token files:
   npx mcp-remote https://mcp.gumloop.com/gong/mcp
   (This will open a browser — I'll authorize there)

5. After I authorize both, zip the token files:
   cd ~/.mcp-auth/mcp-remote-0.1.12
   zip ~/Desktop/gary_gmail_tokens.zip b33c17f2a3b8668ac4b9aff0f5daaffd_tokens.json b33c17f2a3b8668ac4b9aff0f5daaffd_client_info.json
   zip ~/Desktop/gary_gong_tokens.zip 60ca12806b120631fbb341f2fac91bfc_tokens.json

6. Copy my Google Calendar credentials:
   cp ~/.config/gcloud/application_default_credentials.json ~/Desktop/gary_calendar_credentials.json

7. Tell me when the files are ready on my Desktop so I can send them to Greg.
```

---

## Send Files to Greg

After the automated setup, you'll have three files on your Desktop:

1. **`gary_gmail_tokens.zip`** — Your Gmail auth tokens (for email drafts)
2. **`gary_gong_tokens.zip`** — Your Gong auth tokens (for real-time call transcripts)
3. **`gary_calendar_credentials.json`** — Your Google Calendar auth (for pre-meeting briefs)

**DM all three files to Greg on Slack.** He'll install them on the bot server. No bot restart needed — they take effect immediately.

---

## What Greg Does With Your Files

Greg runs these commands (replacing `YOUR_SLACK_ID` with your Slack Member ID):

```bash
# Create your token directory
mkdir -p ~/.gary_bot_tokens/YOUR_SLACK_ID

# Unzip and install Gmail tokens
cd ~/Downloads  # or wherever the files landed
unzip gary_gmail_tokens.zip -d /tmp/gary_tokens
cp /tmp/gary_tokens/b33c17f2a3b8668ac4b9aff0f5daaffd_tokens.json ~/.gary_bot_tokens/YOUR_SLACK_ID/gmail_tokens.json
cp /tmp/gary_tokens/b33c17f2a3b8668ac4b9aff0f5daaffd_client_info.json ~/.gary_bot_tokens/YOUR_SLACK_ID/gmail_client_info.json
rm -rf /tmp/gary_tokens

# Unzip and install Gong tokens
unzip gary_gong_tokens.zip -d /tmp/gary_gong
cp /tmp/gary_gong/60ca12806b120631fbb341f2fac91bfc_tokens.json ~/.gary_bot_tokens/YOUR_SLACK_ID/gong_tokens.json
rm -rf /tmp/gary_gong

# Install Calendar credentials
cp gary_calendar_credentials.json ~/.gary_bot_tokens/YOUR_SLACK_ID/calendar_credentials.json
```

---

## Verify Everything Works

| Test | Expected Result |
|------|----------------|
| Open Gary Bot **Home tab** | Your quota snapshot and priority alerts for **your** accounts |
| Send `status` as a DM to Gary | Health check with Gmail showing as connected |
| Run `/priorities` | **Your** accounts' spend signals |
| Run `/gary-status` | Bot status showing your name |
| Wait for a channel alert where you're the AM | Draft created in **your** Gmail |

---

## What Works Immediately (After Registration Only)

No token files needed for these:

- All slash commands (`/priorities`, `/quota`, `/morning-brief`, `/spend-pacing`, `/opp-pacing`, `/pipeline-cleanup`, `/zero-to-one`, `/forecast`, etc.)
- Home tab dashboard (quota, priority alerts, meetings)
- All scheduled DMs (acceleration alerts, spend pacing, pipeline cleanup, etc.)
- Channel alert routing (alerts where you're the AM trigger actions for you)
- DM conversations with Gary (ask questions, run jobs)
- Per-user settings (quiet hours, feature toggles)

## What Requires Token Files

| Feature | Required File | Without It |
|---------|--------------|------------|
| Email drafts (ACH-to-card, procurement, fundraise, etc.) | Gmail tokens | Drafts queue but can't be created |
| Email context in drafts (prior thread history) | Gmail tokens | Drafts still created but without email thread context |
| Real-time Gong call transcripts & follow-ups | Gong tokens | Falls back to overnight Snowflake sync (delayed ~12-24h) |
| Pre-meeting briefs | Calendar credentials | No calendar-based meeting detection |
| Granola post-meeting follow-ups | N/A (runs on Greg's machine only) | Feature not available for shared users |

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Home tab shows "Get Started" after registering | Close and reopen the Home tab |
| No data in Home tab / slash commands | Your Salesforce Name in registration must match **exactly** (capitalization matters) |
| Email drafts not working | Check Step 3 (Gumstack auth) and make sure Greg installed your token files |
| Pre-meeting briefs not working | Check Step 4 (Calendar auth) and make sure Greg installed your credentials |
| `npx mcp-remote` fails | Install Node.js: `brew install node` |
| `gcloud: command not found` | Run `source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"` or restart Terminal |
| Bot responds to other people's alerts | Only triggers when **you** are listed as Account Manager on the alert |
| Need to update your info | Open the Home tab and click "Get Started" again — it overwrites your existing registration |
| Gmail tokens expire | Re-auth at [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps), re-run `npx mcp-remote`, and send Greg the new files |
