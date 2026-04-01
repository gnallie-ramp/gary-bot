# Gary Bot — Setup Guide

Set up your own instance of Gary Bot. This creates a fully independent bot on your machine with your own Slack app, credentials, and scheduled jobs.

> **Just want to join Greg's existing bot?** See [SETUP_TEAMMATE.md](SETUP_TEAMMATE.md) instead — faster and doesn't require running anything on your machine.

---

## Own Instance vs Shared Instance

| | Own Instance (this guide) | Shared Instance ([SETUP_TEAMMATE.md](SETUP_TEAMMATE.md)) |
|--|--------------------------|-------------------------------|
| **Setup time** | ~30 minutes | ~15 minutes |
| **Runs on** | Your Mac — works while it's on | Greg's Mac — works while his is on |
| **Post-meeting follow-ups** | Granola (~3 min) + Gong API fallback | Gong API only (~15 min delay) |
| **Customization** | Full control | Defaults only |
| **Updates** | Manual `git pull` | Automatic |
| **Slash commands** | Your own, no conflicts | Shared namespace |

---

## Step 1: Manual Prerequisites (Browser Required)

Complete these before opening Glass. Each requires a browser you must interact with yourself.

### 1a. Gather your info

You'll need these values for setup. Collect them now:

| Value | Where to find it | Example |
|-------|-----------------|---------|
| **Salesforce Name** | Your full name exactly as it appears on the Owner field of any account you own in Salesforce | `Jane Smith` |
| **First Name** | Your first name (used in email sign-offs) | `Jane` |
| **Slack Member ID** | Slack > click your profile photo > three dots menu > **Copy member ID** | `U03JBULM9LP` |
| **ChiliPiper Link** | Your booking URL | `https://ramp-com.chilipiper.com/me/jane-smith/ramp` |
| **Ramp Email** | Your @ramp.com email | `jsmith@ramp.com` |
| **Timezone** | One of: `US/Eastern`, `US/Central`, `US/Mountain`, `US/Pacific` | `US/Eastern` |

### 1b. Create your Slack app

1. Go to [api.slack.com/apps](https://api.slack.com/apps) and sign in with your Ramp Slack account
2. Click **Create New App** > **From an app manifest**
3. Select the **Ramp** workspace
4. Switch the format tab to **YAML**
5. Paste the contents of `slack_app_manifest.yaml` (ask Greg for this file, or you'll find it in the repo after cloning)
6. **Before creating:** Change the `name` and `display_name` fields to something unique (e.g. "Jane's Sales Bot") — two apps with identical slash commands will conflict
7. Click **Next** > **Create**
8. Go to **Install App** (left sidebar) > **Install to Workspace** > **Allow**
9. Copy these two tokens — you'll need them soon:

| Token | Where to find it |
|-------|-----------------|
| **Bot Token** (`xoxb-...`) | OAuth & Permissions > Bot User OAuth Token |
| **App Token** (`xapp-...`) | Basic Information > App-Level Tokens > Generate > name it anything > add scope `connections:write` > Generate |

### 1c. Get an Anthropic API key

1. Go to [console.anthropic.com](https://console.anthropic.com) (not claude.com)
2. **Settings** > **API Keys** > create one
3. Save it — you'll need it for the `.env` file

### 1d. Request access (may take a few hours)

Via ConductorOne/Okta, request:
- **Snowflake** — General access (not unmasked)
- **GitHub** — See [Which GitHub Entitlement(s) should I request](https://www.notion.so/Which-Github-Entitlement-s-should-I-request-133b5cd80425414983f6f15ab2a15cf8)

### 1e. Connect Gmail and Gong on Gumstack

1. Go to [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps) and log in with your Ramp account
2. Find **Gmail** > click **Connect** > authorize with your Ramp Google account
3. Find **Gong** > click **Connect** > authorize with your Gong account

### 1f. Install Project Glass

Install **Project Glass** through the **Self Service+.app** on your Mac.

---

## Step 2: Automated Setup (Paste Into Glass)

Once all manual prerequisites are done, paste the entire block below into Project Glass. Glass will run each step and pause when it needs your input.

> **Tip:** Turn on **YOLO mode** in Glass for fewer confirmation prompts.

**Replace the placeholder values on lines marked `# <-- REPLACE` with your actual info before pasting.**

~~~
I need to set up Gary Bot from scratch. Run these steps in order. Pause and clearly tell me when a step needs my interaction (browser auth, etc.) — run everything else automatically. If any step fails, stop and show me the error.

## Phase 1: Install system dependencies

# Check if Homebrew is installed, install if not
which brew || /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# IMPORTANT: If Homebrew was just installed, run the PATH commands it printed:
# echo >> ~/.zprofile
# echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
# eval "$(/opt/homebrew/bin/brew shellenv)"

brew install git python@3.12 node gh google-cloud-sdk
source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"

# Install Salesforce CLI (used for opp creation)
npm install -g @salesforce/cli

## Phase 2: GitHub auth + clone

# MANUAL: This opens a browser — user must complete GitHub login
gh auth login
# Choose: GitHub.com > HTTPS > Login with a web browser

cd ~
git clone https://github.com/gnallie-ramp/gary-bot.git

## Phase 3: Python environment

cd ~/gary-bot
python3.12 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install snowflake-cli-labs

# Symlink snow CLI so launchd can find it
ln -sf "$(which snow)" /opt/homebrew/bin/snow

## Phase 4: Create .env file

# Write the .env file directly — replace EVERY placeholder value below
cat > ~/gary-bot/.env << 'ENVEOF'
# ── Owner Identity ──
OWNER_NAME=Jane Smith              # <-- REPLACE with your Salesforce name (exact match, case-sensitive)
OWNER_FIRST_NAME=Jane              # <-- REPLACE with your first name
OWNER_SLACK_ID=U03JBULM9LP        # <-- REPLACE with your Slack Member ID
BOOKING_LINK=https://ramp-com.chilipiper.com/me/jane-smith/ramp  # <-- REPLACE with your ChiliPiper URL
DISPLAY_TIMEZONE=US/Eastern        # <-- REPLACE if not Eastern

# ── Slack App ──
SLACK_BOT_TOKEN=xoxb-your-token   # <-- REPLACE with your Bot Token from Step 1b
SLACK_APP_TOKEN=xapp-your-token   # <-- REPLACE with your App Token from Step 1b

# ── Anthropic (Claude AI) ──
ANTHROPIC_API_KEY=sk-ant-your-key # <-- REPLACE with your Anthropic API key from Step 1c

# ── Gmail ──
GMAIL_ADDRESS=jsmith@ramp.com     # <-- REPLACE with your @ramp.com email
ENVEOF

# CRITICAL: Verify OWNER_NAME matches Salesforce exactly — this drives all queries.
# Open the .env file and double-check every value before continuing.

## Phase 5: Configure Snowflake

mkdir -p ~/.snowflake
cat > ~/.snowflake/config.toml << 'SNOWEOF'
[connections.default]
account = "ramp"
user = "jsmith@ramp.com"
authenticator = "externalbrowser"
warehouse = "READER"
database = "ANALYTICS"
role = "DEPT-SALES"
SNOWEOF
chmod 0600 ~/.snowflake/config.toml

# IMPORTANT: Edit ~/.snowflake/config.toml and replace the user field with your @ramp.com email

## Phase 6: Browser-based authentication (3 steps, each opens a browser)

# MANUAL: Snowflake — opens Okta SSO. Sign in when browser opens.
cd ~/gary-bot && source venv/bin/activate
snow connection test

# MANUAL: Salesforce — opens browser SSO. Sign in when browser opens.
sf org login web --alias ramp --instance-url https://rampfinancial.lightning.force.com

# MANUAL: Google Calendar — opens Google sign-in. Authorize with your Ramp account.
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/calendar.readonly

## Phase 7: Gumstack token auth (Gmail + Gong MCP access)

# MANUAL: Each opens a browser. Authorize when prompted.
npx mcp-remote https://mcp.gumloop.com/gmail/mcp
npx mcp-remote https://mcp.gumloop.com/gong/mcp

## Phase 8: Set up auto-start on login (launchd)

mkdir -p ~/Library/LaunchAgents

# Get the current user's home directory for the plist (launchd doesn't expand $HOME)
cat > ~/Library/LaunchAgents/com.gary-bot.plist << PLISTEOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.gary-bot</string>
    <key>ProgramArguments</key>
    <array>
        <string>$HOME/gary-bot/venv/bin/python3</string>
        <string>$HOME/gary-bot/main.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$HOME/gary-bot</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>StandardOutPath</key>
    <string>$HOME/.gary_bot_stdout.log</string>
    <key>StandardErrorPath</key>
    <string>$HOME/.gary_bot_stderr.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
PLISTEOF

launchctl load ~/Library/LaunchAgents/com.gary-bot.plist

## Phase 9: Verify

sleep 5
tail -20 ~/.gary_bot_stdout.log
# Should see: "Scheduler started with 26 jobs" and "Snowflake connection OK"

# Test Snowflake query
cd ~/gary-bot && source venv/bin/activate
snow sql --query "SELECT 1 AS test" --format json --silent
~~~

---

## Step 3: Verify Everything Works

After setup completes, test each integration:

| Test | How | Expected Result |
|------|-----|----------------|
| **Bot running** | `tail -20 ~/.gary_bot_stdout.log` | "Scheduler started with 26 jobs" and "Snowflake connection OK" |
| **Slack connected** | Open your bot's **Home tab** in Slack | Header + quota snapshot + priority alerts |
| **Snowflake** | `snow sql --query "SELECT CURRENT_USER()" --format json --silent` | Your email address |
| **Salesforce** | `sf org display --target-org ramp` | Shows your org info |
| **Status check** | DM your bot: `status` | Health check with integration statuses |
| **Priorities** | Run `/priorities` in Slack | Your accounts' spend signals |

---

## Managing the Bot

### Start / Stop / Restart

```bash
launchctl stop com.gary-bot                                    # Stop
launchctl start com.gary-bot                                   # Start
launchctl unload ~/Library/LaunchAgents/com.gary-bot.plist     # Remove auto-start
launchctl load ~/Library/LaunchAgents/com.gary-bot.plist       # Re-enable auto-start
```

### View Logs

```bash
tail -50 ~/.gary_bot_stdout.log    # Main logs
tail -50 ~/.gary_bot_stderr.log    # Errors/warnings
```

### Pull Updates

When Greg pushes updates to the repo:

```bash
cd ~/gary-bot
git pull
source venv/bin/activate
pip install -r requirements.txt
launchctl stop com.gary-bot && launchctl start com.gary-bot
```

---

## Adding Teammates

Your bot supports multi-user. Teammates don't need their own instance — they register via the Home tab.

See [SETUP_TEAMMATE.md](SETUP_TEAMMATE.md) for the teammate onboarding guide.

**Admin steps** when a teammate sends you their token files:

```bash
# Replace THEIR_SLACK_ID with their actual Slack Member ID
mkdir -p ~/.gary_bot_tokens/THEIR_SLACK_ID

# Gmail tokens
unzip gary_gmail_tokens.zip -d /tmp/gary_tokens
cp /tmp/gary_tokens/*_tokens.json ~/.gary_bot_tokens/THEIR_SLACK_ID/gmail_tokens.json
cp /tmp/gary_tokens/*_client_info.json ~/.gary_bot_tokens/THEIR_SLACK_ID/gmail_client_info.json
rm -rf /tmp/gary_tokens

# Gong tokens
unzip gary_gong_tokens.zip -d /tmp/gary_gong
cp /tmp/gary_gong/*_tokens.json ~/.gary_bot_tokens/THEIR_SLACK_ID/gong_tokens.json
rm -rf /tmp/gary_gong

# Calendar credentials
cp gary_calendar_credentials.json ~/.gary_bot_tokens/THEIR_SLACK_ID/calendar_credentials.json
```

No restart needed. View registered users: `cat ~/.gary_bot_users.json | python3 -m json.tool`

---

## Customization

- **Bot name:** Change `name` and `display_name` in your Slack app at [api.slack.com/apps](https://api.slack.com/apps)
- **Signal thresholds:** Edit `queries/queries.py` > `HOME_PRIORITY_ALERTS_QUERY`
- **Notifications:** Open the bot's Home tab > Settings
- **Email templates:** See `EMAIL_DRAFTING_GUIDE.md`

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `command not found: brew` | Re-run Homebrew install, then run the PATH commands it prints |
| `command not found: python3.12` | `brew install python@3.12` then restart Terminal |
| `command not found: gh` or `node` | Run the Homebrew PATH commands — likely skipped after install |
| `command not found: snow` | `pip install snowflake-cli-labs && ln -sf "$(which snow)" /opt/homebrew/bin/snow` |
| `command not found: sf` | `npm install -g @salesforce/cli` |
| `No module named 'slack_bolt'` | Not in virtualenv: `cd ~/gary-bot && source venv/bin/activate` |
| Snowflake: `Bad owner or permissions` | `chmod 0600 ~/.snowflake/config.toml` |
| Snowflake: `User is empty` | Add `user = "you@ramp.com"` to `~/.snowflake/config.toml` |
| Snowflake: connection fails | `snow connection test` to refresh SSO tokens |
| Snowflake: JSON truncation | Known `snow` CLI 2.8.x bug — bot auto-retries with OBJECT_CONSTRUCT workaround |
| Gmail drafts not creating | Re-auth at [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps) |
| Gong transcripts not working | Re-auth at [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps) |
| Bot starts then crashes | `tail -50 ~/.gary_bot_stdout.log` — common: expired Snowflake token, missing packages |
| No data in Home tab | `OWNER_NAME` in `.env` doesn't match Salesforce exactly (case-sensitive) |
| Bot shows wrong data | `OWNER_NAME` mismatch — check spelling and capitalization |
| Slash commands go to wrong bot | Two apps registered the same commands — rename yours in the manifest |
| macOS keychain prompts on startup | Run `streamlit run app.py` once interactively to cache Snowflake SSO token, click "Always Allow" |
| `Permission denied` on git clone | `gh auth login` to authenticate with GitHub |
| `gcloud: command not found` | `source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"` or restart Terminal |
| Duplicate bot processes | `launchctl stop com.gary-bot` or `kill $(pgrep -f gary-bot/main.py)` |
