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
| **Slash commands** | Your own prefix, no conflicts | Shared namespace |

---

## Step 1: Manual Prerequisites (Browser Required)

Complete these before opening Glass. Each requires a browser you must interact with yourself.

### 1a. Request access (may take a few hours — do this first)

Via ConductorOne/Okta, request:
- **Snowflake** — General access (not unmasked)
- **GitHub** — See the Notion doc "Which GitHub Entitlement(s) should I request"

### 1b. Get an Anthropic API key

1. Go to console.anthropic.com (not claude.com)
2. **Settings** > **API Keys** > create one
3. Save it — you'll paste it during setup

### 1c. Connect Gmail and Gong on Gumstack

1. Go to gumloop.com/personal/apps and log in with your Ramp account
2. Find **Gmail** > click **Connect** > authorize with your Ramp Google account
3. Find **Gong** > click **Connect** > authorize with your Gong account

### 1d. Install Project Glass

Install **Project Glass** through the **Self Service+.app** on your Mac.

---

## Step 2: Automated Setup (Paste Into Glass)

Once all manual prerequisites are done, copy and paste this entire block into Project Glass. **Do not modify anything before pasting** — Glass will ask you for your info at the right time.

> **Tip:** Turn on **YOLO mode** in Glass for fewer confirmation prompts.

~~~
I need to set up Gary Bot from scratch. Follow these instructions carefully. You are an AI assistant running inside Project Glass. Run all commands yourself using your Bash tool. Never ask the user to type commands in Terminal — you run them. Only pause when a browser window opens and the user needs to interact with it.

IMPORTANT FORMATTING RULES:
- When showing URLs or file paths to the user, put them in a standalone code block so they don't get converted to markdown links.
- When running commands that contain URLs, run them directly with your Bash tool. Do not display the command for the user to copy.
- Never display raw URLs in your prose text. Either run the command yourself or put the URL in a code block.

## Phase 1: Collect user info

Before doing anything, ask the user for ALL of the following values in a single message. Present it as a numbered list they can fill in. Do not proceed until you have every value.

1. Full name exactly as it appears on the Owner field of their Salesforce accounts (e.g. "Jane Smith")
2. First name only (for email sign-offs, e.g. "Jane")
3. Slack Member ID (tell them: click your profile photo in Slack, then three dots menu, then Copy member ID — looks like U03JBULM9LP)
4. ChiliPiper booking URL (looks like: ramp-com.chilipiper.com/me/jane-smith/ramp)
5. Ramp email address (their @ramp.com email)
6. Timezone — one of: US/Eastern, US/Central, US/Mountain, US/Pacific
7. Command prefix — a unique short lowercase word for their slash commands, no spaces (e.g. "jane"). Explain: this prefixes all slash commands like /jane-lookup, /jane-priorities so they don't conflict with other bot instances.
8. Anthropic API key (from console.anthropic.com — starts with sk-ant-)
9. Slack Bot Token (tell them: they'll get this in Phase 3 after creating the Slack app — they can say "skip" for now and you'll ask again later)
10. Slack App Token (same — they can say "skip" for now)

Store all values as variables for use in later phases. If they skip the Slack tokens, you'll ask again after Phase 3.

## Phase 2: Install system dependencies

Run these commands yourself. Do not ask the user to run them.

Check if Homebrew is installed. If not, install it. If you just installed Homebrew, also run the PATH setup:
  echo >> ~/.zprofile && echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile && eval "$(/opt/homebrew/bin/brew shellenv)"

Then install system packages:
  brew install git python@3.12 node gh google-cloud-sdk

Source the gcloud CLI into the current shell:
  source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"

Install Salesforce CLI:
  npm install -g @salesforce/cli

## Phase 3: GitHub auth + clone repo

Run: gh auth login
This opens a browser. Tell the user: "A browser window is opening for GitHub login. Choose GitHub.com, HTTPS, and Login with a web browser. Complete the login and tell me when done."
Wait for the user to confirm.

Then clone the repo:
  cd ~ && git clone https://github.com/gnallie-ramp/gary-bot.git

## Phase 4: Create Slack app

Now you need to help the user create their Slack app using the manifest from the repo.

First, read the manifest file at ~/gary-bot/slack_app_manifest.yaml. Using the COMMAND_PREFIX the user provided in Phase 1, find-and-replace "gary" with their prefix in ALL the slash command names (e.g. /gary-lookup becomes /jane-lookup if prefix is "jane"). Also replace the "name" and "display_name" fields with something unique like "<FirstName>'s Sales Bot".

Write the modified manifest to a temporary file at ~/gary-bot/my_manifest.yaml so the user can easily find and copy it.

Tell the user to do the following (show each step clearly):
1. Open api.slack.com/apps in their browser and sign in with their Ramp Slack account
2. Click "Create New App" then "From an app manifest"
3. Select the Ramp workspace
4. Switch the format tab to YAML
5. Open the file ~/gary-bot/my_manifest.yaml, select all the contents, and paste it into the manifest field
6. Click Next, then Create
7. Go to "Install App" in the left sidebar, click "Install to Workspace", then Allow
8. Copy the Bot Token: go to "OAuth & Permissions" in left sidebar, copy the "Bot User OAuth Token" (starts with xoxb-)
9. Copy the App Token: go to "Basic Information" in left sidebar, scroll to "App-Level Tokens", click "Generate Token", name it anything, add the scope "connections:write", click Generate, and copy the token (starts with xapp-)

Ask the user to paste both tokens. Store them as variables.

## Phase 5: Python environment

Run these commands yourself:
  cd ~/gary-bot
  python3.12 -m venv venv
  source venv/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt
  pip install snowflake-cli-labs

Symlink snow CLI so launchd can find it:
  ln -sf "$(which snow)" /opt/homebrew/bin/snow

## Phase 6: Create config files

Using ALL the values collected in Phase 1 and Phase 4, write the .env file at ~/gary-bot/.env with this exact format (substitute the actual values, no placeholders):

  OWNER_NAME=<their salesforce name>
  OWNER_FIRST_NAME=<their first name>
  OWNER_SLACK_ID=<their slack member id>
  BOOKING_LINK=<their chilipiper url>
  DISPLAY_TIMEZONE=<their timezone>
  COMMAND_PREFIX=<their chosen prefix>
  SLACK_BOT_TOKEN=<their bot token from Phase 4>
  SLACK_APP_TOKEN=<their app token from Phase 4>
  ANTHROPIC_API_KEY=<their anthropic key>
  GMAIL_ADDRESS=<their ramp email>

Then create the Snowflake config at ~/.snowflake/config.toml (create the directory if needed):

  [connections.default]
  account = "ramp"
  user = "<their ramp email>"
  authenticator = "externalbrowser"
  warehouse = "READER"
  database = "ANALYTICS"
  role = "DEPT-SALES"

Set permissions: chmod 0600 ~/.snowflake/config.toml

Show the user both files and ask them to confirm the values look correct before continuing.

## Phase 7: Browser-based authentication

There are 3 browser auth steps. For EACH one: run the command yourself, tell the user a browser is opening and what to do, then wait for them to confirm before moving to the next.

Step 1 — Snowflake:
Run: cd ~/gary-bot && source venv/bin/activate && snow connection test
Tell user: "A browser window is opening for Snowflake/Okta SSO. Sign in and tell me when done."

Step 2 — Salesforce:
Run: sf org login web --alias ramp --instance-url https://rampfinancial.lightning.force.com
Tell user: "A browser window is opening for Salesforce SSO. Sign in and tell me when done."

Step 3 — Google Calendar:
Run: source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc" && gcloud auth application-default login --scopes=https://www.googleapis.com/auth/calendar.readonly
Tell user: "A browser window is opening for Google auth. Sign in with your Ramp Google account and tell me when done."

## Phase 8: Set up auto-start on login (launchd)

Run: mkdir -p ~/Library/LaunchAgents

Write the plist file at ~/Library/LaunchAgents/com.gary-bot.plist. Use the user's actual home directory path (run echo $HOME to get it) — launchd does not expand $HOME. The plist should contain:

  Label: com.gary-bot
  ProgramArguments: <HOME>/gary-bot/venv/bin/python3, <HOME>/gary-bot/main.py
  WorkingDirectory: <HOME>/gary-bot
  RunAtLoad: true
  KeepAlive: true
  ThrottleInterval: 10
  StandardOutPath: <HOME>/.gary_bot_stdout.log
  StandardErrorPath: <HOME>/.gary_bot_stderr.log
  EnvironmentVariables > PATH: /opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin

Load it: launchctl load ~/Library/LaunchAgents/com.gary-bot.plist

## Phase 9: Verify

Wait 10 seconds, then check the log:
  tail -20 ~/.gary_bot_stdout.log

You should see "Scheduler started with 26 jobs" and "Snowflake connection OK".

Also run a test Snowflake query:
  cd ~/gary-bot && source venv/bin/activate && snow sql --query "SELECT 1 AS test" --format json --silent

Tell the user setup is complete and show them how to test:
- Open the bot's Home tab in Slack — they should see their account data
- DM the bot the word "status" — should show a health check
- Try their first slash command: /<prefix>-priorities
~~~

---

## Step 3: Verify Everything Works

After setup completes, test each integration:

| Test | How | Expected Result |
|------|-----|----------------|
| **Bot running** | Check log: `tail -20 ~/.gary_bot_stdout.log` | "Scheduler started with 26 jobs" |
| **Slack connected** | Open your bot's **Home tab** in Slack | Header + quota snapshot + priority alerts |
| **Snowflake** | `snow sql --query "SELECT CURRENT_USER()" --format json --silent` | Your email address |
| **Salesforce** | `sf org display --target-org ramp` | Shows your org info |
| **Status check** | DM your bot: `status` | Health check with integration statuses |
| **Slash commands** | Run `/<prefix>-priorities` in Slack (using your prefix) | Your accounts' spend signals |

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

- **Bot name:** Change `name` and `display_name` in your Slack app settings
- **Command prefix:** Change `COMMAND_PREFIX` in `.env` (must also update your Slack app manifest to match)
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
| Gmail drafts not creating | Re-auth at gumloop.com/personal/apps |
| Gong transcripts not working | Re-auth at gumloop.com/personal/apps |
| Bot starts then crashes | `tail -50 ~/.gary_bot_stdout.log` — common: expired Snowflake token, missing packages |
| No data in Home tab | `OWNER_NAME` in `.env` doesn't match Salesforce exactly (case-sensitive) |
| Bot shows wrong data | `OWNER_NAME` mismatch — check spelling and capitalization |
| Slash commands go to wrong bot | `COMMAND_PREFIX` in `.env` doesn't match what's in your Slack app manifest |
| Slash command not found | Verify the command exists in your Slack app manifest with the correct prefix |
| macOS keychain prompts on startup | Run Streamlit dashboard once interactively to cache Snowflake SSO token, click "Always Allow" |
| `Permission denied` on git clone | Run `gh auth login` to authenticate with GitHub |
| `gcloud: command not found` | Source the gcloud path or restart Terminal |
| Duplicate bot processes | `launchctl stop com.gary-bot` or `kill $(pgrep -f gary-bot/main.py)` |
