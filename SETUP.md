# Gary Bot — Standalone Setup Guide

Set up your own instance of Gary Bot in ~30 minutes. This creates a fully independent bot on your machine with your own Slack app, credentials, and scheduled jobs.

> **Just want to join Greg's existing bot?** See [SETUP_TEAMMATE.md](SETUP_TEAMMATE.md) instead — it's faster and doesn't require running anything on your machine.

> **Copy-paste warning:** When copying commands from this guide, make sure quotes paste as straight quotes (`"`) not curly/smart quotes. If you get `zsh: unknown file attribute: h` errors, the quotes were converted during copy-paste. Try typing the command manually or pasting into a plain-text editor first. Also watch for URLs pasting as `[text](url)` markdown — only the plain URL should go in the terminal.

---

## Considerations + Limitations

Understand the trade-offs vs joining Greg's shared instance ([SETUP_TEAMMATE.md](SETUP_TEAMMATE.md)):

| | Own Instance (this guide) | Shared App (SETUP_TEAMMATE.md) |
|--|--------------------------|-------------------------------|
| **Setup time** | ~30 minutes | ~15 minutes |
| **Runs on** | Your computer — works while yours is on | Greg's computer — only works while his machine is on |
| **Post-meeting follow-ups** | Granola (~3 min, immediate local files) + Gong API fallback | Gong API only (~15 min delay after call ends) |
| **Customization** | Full control over queries, thresholds, prompts, schedules | Default settings only; changes require Greg |
| **Troubleshooting** | You can debug and fix issues yourself | Requires Greg's involvement for most issues |
| **Updates** | Manual — you pull updates yourself | Automatic — Greg pulls updates and you get them instantly |
| **Slack app** | Your own Slack app with your own name | Shared — uses Greg's Slack app |
| **Slash commands** | Your own commands, no conflicts | Shared namespace (e.g. `/priorities` goes to Greg's app) |

---

## Manual Steps (Do These First)

These steps require a browser and can't be automated. Complete them before pasting the automated section into Project Glass.

### 1. Gather your info

| Value | Where to find it | Example |
|-------|-----------------|---------|
| **Salesforce Name** | Your full name exactly as it appears in Salesforce (Owner field on any account you own) | `Jane Smith` |
| **First Name** | Your first name (used in email sign-offs) | `Jane` |
| **Slack Member ID** | In Slack: click your profile photo > ⋮ > **Copy member ID** | `U03JBULM9LP` |
| **ChiliPiper Link** | Your booking URL | `https://ramp-com.chilipiper.com/me/jane-smith/ramp` |
| **Ramp Email** | Your @ramp.com email | `jsmith@ramp.com` |
| **Timezone** | `US/Eastern`, `US/Central`, `US/Mountain`, or `US/Pacific` | `US/Eastern` |

### 2. Create your Slack app

1. Go to [api.slack.com/apps](https://api.slack.com/apps) (sign in with your Ramp Slack account)
2. Click **Create New App** > **From an app manifest**
3. Select the **Ramp workspace**
4. Switch the format tab to **YAML**
5. Paste the entire contents of `slack_app_manifest.yaml` from this repo
   - To see the file after cloning: run `cat ~/gary-bot/slack_app_manifest.yaml` in Terminal
   - Or ask Greg to send you the file
6. Click **Next** > **Create**
7. Go to **Install App** (left sidebar) > **Install to Workspace** > **Allow**

**Important:** Change the bot name in the manifest before creating! Edit the `name` and `display_name` fields to something unique (e.g. "Jane's Sales Bot"). Two apps with the same slash commands in the same workspace will conflict.

Now grab your tokens:

| Token | Where |
|-------|-------|
| **Bot Token** (`xoxb-...`) | OAuth & Permissions > Bot User OAuth Token (must install app first) |
| **App Token** (`xapp-...`) | Basic Information > App-Level Tokens > Generate > name it anything > add scope `connections:write` > Generate |

### 3. Get an Anthropic API key

1. Go to [console.anthropic.com](https://console.anthropic.com) (not claude.com)
2. Go to **Settings** > **API Keys** > create one
3. Save it — you'll need it for the `.env` file

> **Note:** If your Ramp org has key creation disabled, ask your admin to create one for you or use a personal Anthropic account.

### 4. Get GitHub access

You need access to the gary-bot repo. If you don't have it:
1. Request the appropriate GitHub entitlement — see [Which GitHub Entitlement(s) should I request](https://www.notion.so/Which-Github-Entitlement-s-should-I-request-133b5cd80425414983f6f15ab2a15cf8)
2. Once approved, authenticate: `gh auth login` (choose GitHub.com > HTTPS > Login with a web browser)

### 5. Request access through Okta — ConductorOne

Request these entitlements (approval may take a few hours):

- **Snowflake** — General access (not unmasked)
- **Claude Code API** — you'll need an API key from [console.anthropic.com](https://console.anthropic.com)

### 6. Install Project Glass

Install **Project Glass** through the **Self Service+.app** on your computer.

### 7. Connect Gmail and Gong on Gumstack

1. Go to [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps)
2. Log in with your Ramp account
3. Find **Gmail** and click **Connect** > authorize with your Ramp Google account
4. Find **Gong** and click **Connect** > authorize with your Gong account

### 8. Authenticate Google Calendar & Snowflake

These open a browser for SSO — run them in Terminal after the automated section installs the prerequisites:

**Snowflake:**
```bash
snow connection test
```
Sign in with Okta when the browser opens.

**Google Calendar:**
```bash
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/calendar.readonly
```
Sign in with your Ramp Google account.

---

## Automated Setup (Paste Into Project Glass)

> **If you get any errors or issues:** Send a screenshot to Glass, explain what you were trying to do and what happened. Glass should fix it or provide next steps.

Turn on **YOLO mode** and paste everything below into Project Glass. Replace the placeholder values with your actual info.

```
I need to set up Gary Bot from scratch on a fresh Mac. Please run the following steps in order. Stop and tell me if any step fails.

### Install Homebrew (if not already installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
# IMPORTANT: After install, run the two PATH commands Homebrew prints. They look like:
# echo >> ~/.zprofile
# echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
# eval "$(/opt/homebrew/bin/brew shellenv)"

### Install prerequisites
brew install git python@3.12 node gh google-cloud-sdk
source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"

### Authenticate with GitHub
gh auth login
# Choose: GitHub.com > HTTPS > Login with a web browser

### Clone repo and install Python dependencies
cd ~
git clone https://github.com/gnallie-ramp/gary-bot.git
cd ~/gary-bot
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install snowflake-cli
pip install google-auth google-auth-oauthlib google-api-python-client

### Create .env file
# IMPORTANT: Replace ALL placeholder values below with my actual info
cp .env.example .env

Now edit the .env file and replace these values:
- OWNER_NAME=<my Salesforce name exactly as it appears>
- OWNER_FIRST_NAME=<my first name>
- OWNER_SLACK_ID=<my Slack member ID>
- BOOKING_LINK=<my ChiliPiper URL>
- GMAIL_ADDRESS=<my @ramp.com email>
- SLACK_BOT_TOKEN=<my xoxb- token from Slack app>
- SLACK_APP_TOKEN=<my xapp- token from Slack app>
- ANTHROPIC_API_KEY=<my Anthropic API key>
- DISPLAY_TIMEZONE=<my timezone>

CRITICAL: OWNER_NAME must match Salesforce exactly (capitalization matters). This is what every query uses to pull your accounts.

### Configure Snowflake
mkdir -p ~/.snowflake
cat > ~/.snowflake/config.toml << 'SNOWEOF'
[connections.default]
account = "ramp"
authenticator = "externalbrowser"
warehouse = "READER"
database = "ANALYTICS"
role = "DEPT-SALES"
SNOWEOF
chmod 0600 ~/.snowflake/config.toml

### Run Gumstack Gmail auth
npx mcp-remote https://mcp.gumloop.com/gmail/mcp
# (This opens a browser — I'll authorize there)

### Run Gumstack Gong auth
npx mcp-remote https://mcp.gumloop.com/gong/mcp
# (This opens a browser — I'll authorize there)

### Start the bot
cd ~/gary-bot
source venv/bin/activate
nohup venv/bin/python3 main.py >> nohup.out 2>&1 & disown

### Verify it's running
sleep 5
tail -20 nohup.out
# Should see "Bolt app is running!" and "Snowflake connection OK"
```

---

## After Automated Setup

### Authenticate browser-based services

These weren't run in the automated section because they open a browser:

```bash
# Snowflake — opens Okta SSO
cd ~/gary-bot && source venv/bin/activate
snow connection test

# Google Calendar — opens Google sign-in
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/calendar.readonly
```

### Verify everything works

| Test | Expected Result |
|------|----------------|
| `tail -20 nohup.out` | Shows `Bolt app is running!` and `Snowflake connection OK` |
| Open Gary Bot **Home tab** in Slack | Your quota snapshot and priority alerts |
| Send `status` as DM to your bot | Health check with all services green |
| Run `/priorities` | Your accounts' spend signals |
| Run `/gary-status` | Bot uptime and connection status |

---

## Optional: Auto-Start on Login

Keep Gary running even after you restart your Mac:

```bash
cat > ~/Library/LaunchAgents/com.gary-bot.plist << EOF
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
EOF

launchctl load ~/Library/LaunchAgents/com.gary-bot.plist
```

Manage it with:
```bash
launchctl stop com.gary-bot    # Stop
launchctl start com.gary-bot   # Start
launchctl unload ~/Library/LaunchAgents/com.gary-bot.plist  # Remove auto-start
```

---

## Keeping Updated

When Greg pushes updates:

```bash
cd ~/gary-bot
git pull
source venv/bin/activate
pip install -r requirements.txt
# Restart (pick one):
kill $(cat ~/.gary_bot.pid) 2>/dev/null; nohup venv/bin/python3 main.py >> nohup.out 2>&1 & disown
# OR if using launchd:
launchctl stop com.gary-bot && launchctl start com.gary-bot
```

---

## Adding Teammates to Your Instance

Your bot supports multi-user. Teammates don't need their own instance — they register via the Home tab and you install their token files.

See [SETUP_TEAMMATE.md](SETUP_TEAMMATE.md) for the teammate guide.

**Admin steps when a teammate sends you their files:**

```bash
# Replace THEIR_SLACK_ID with their actual Slack Member ID
mkdir -p ~/.gary_bot_tokens/THEIR_SLACK_ID

# Gmail tokens (from their gumstack auth zip)
unzip gary_gmail_tokens.zip -d /tmp/gary_tokens
cp /tmp/gary_tokens/*_tokens.json ~/.gary_bot_tokens/THEIR_SLACK_ID/gmail_tokens.json
cp /tmp/gary_tokens/*_client_info.json ~/.gary_bot_tokens/THEIR_SLACK_ID/gmail_client_info.json
rm -rf /tmp/gary_tokens

# Gong tokens (from their gumstack auth zip)
unzip gary_gong_tokens.zip -d /tmp/gary_gong
cp /tmp/gary_gong/*_tokens.json ~/.gary_bot_tokens/THEIR_SLACK_ID/gong_tokens.json
rm -rf /tmp/gary_gong

# Calendar credentials
cp gary_calendar_credentials.json ~/.gary_bot_tokens/THEIR_SLACK_ID/calendar_credentials.json
```

No restart needed.

**View registered users:**
```bash
cat ~/.gary_bot_users.json | python3 -m json.tool
```

---

## Customization

- **Rename the bot:** Change `name` and `display_name` in your Slack app settings at [api.slack.com/apps](https://api.slack.com/apps)
- **Adjust signal thresholds:** Edit `queries/queries.py` > `HOME_PRIORITY_ALERTS_QUERY`
- **Toggle notifications:** Open the bot's Home tab > Settings

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `command not found: brew` | Re-run Homebrew install, then run the PATH commands it prints |
| `command not found: python3` or `python3.12` | `brew install python@3.12` |
| `command not found: gh` or `node` | Run the Homebrew PATH commands — you likely skipped them after install |
| `zsh: unknown file attribute: h` | Smart/curly quotes were pasted instead of straight quotes. Type the command manually or paste into a plain-text editor first |
| Markdown links in Terminal | URLs showing as `[text](url)` means markdown formatting was copied. Paste just the plain URL |
| `No module named 'slack_bolt'` | Not in virtualenv: `cd ~/gary-bot && source venv/bin/activate` |
| `No module named 'google.auth'` | Run `pip install google-auth google-auth-oauthlib google-api-python-client` inside the virtualenv |
| Snowflake: `Bad owner or permissions` | Run `chmod 0600 ~/.snowflake/config.toml` |
| Snowflake: `User is empty` | Add `user = "you@ramp.com"` to `~/.snowflake/config.toml` |
| Snowflake connection fails | `snow connection test` to refresh SSO tokens |
| Gmail drafts not creating | Re-auth at [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps) and re-run `npx mcp-remote https://mcp.gumloop.com/gmail/mcp` |
| Gong transcripts not working | Re-auth at [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps) and re-run `npx mcp-remote https://mcp.gumloop.com/gong/mcp` |
| Bot starts then dies | `tail -50 nohup.out` — common cause: expired Snowflake token or missing Python packages |
| No data in Home tab | `OWNER_NAME` in `.env` doesn't match your Salesforce name exactly (case-sensitive) |
| Bot shows someone else's data | `OWNER_NAME` in `.env` doesn't match your Salesforce name — check spelling and capitalization |
| Duplicate bot processes | `kill $(cat ~/.gary_bot.pid)` |
| `Permission denied` on git clone | `gh auth login` to authenticate with GitHub |
| `gcloud: command not found` | `source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"` or restart Terminal |
| Slash commands go to wrong bot | Two apps registered the same commands — rename yours in the manifest |
