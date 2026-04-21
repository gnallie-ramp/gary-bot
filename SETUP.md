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
- **Claude Code - API Key** (this takes some time to get approved)
- **Gumloop**

### 1b. Get an Anthropic API key

1. Go to console.anthropic.com (not claude.com)
2. **Settings** > **API Keys** > create one
3. Save it — you'll paste it during setup
4. If you don't see API keys in Anthropic console, post in help-it

### 1c. Connect Gmail, Gong, and Salesforce on Gumstack

1. Go to gumloop.com/personal/apps and log in with your Ramp account
2. Find **Gmail** > click **Connect** > authorize with your Ramp Google account, select "Personal"
3. Find **Gong** > click **Connect** > authorize with your Gong account, select "Ramp"
4. Find **Salesforce** > click **Connect** > authorize with your Salesforce account, select "Personal"

### 1d. Install Project Glass

Install **Project Glass** through the **Self Service+.app** on your Mac.

### 1e. Create GitHub account on github.com (login with Google - Ramp email)

### 1f. Paste Automated Setup below into Glass (with dropdown set to Auto Accept)
### note: Glass should run most commands for you but you may need to run some yourself in Mac Terminal (cmd-spacebar, type Terminal, paste command), it will prompt you, may ask for your Mac password and will not show characters while typing, just type it and press enter

### 1g. After glass clones the project from Github, the bot files will be on your computer's home directory ("gnallie"/slackbot, etc)

### 1h. Create your slackbot on api.slack.com/apps using YAML from slackbot folder

### 1j. Give tokens from slackbot config to Glass and answer all the questions 1-10, glass should guide you through this and it should update your .env files with the right values

### 1k. Change your bot identity (name, pic, etc), install slack app, add your slackbot to the channels it should monitor:

1.      #alerts-card-payable-bills	ACH-to-Card email drafter
2	#alerts-self-serve-procurement-trials	Procurement trial email drafter
3	#alerts-pclip-activations	PCLIP activation drafter (limit increase >= $100k)
4	#alerts-large-declines	Large decline email drafter
5	#alerts-fundraising	Fundraise email drafter
6	#bill-pay-automatic-card-losses	Automatic card loss email drafter
7	#alerts-rclip-requests	RCLIP drafter (approved, delta >= $50k)
8	#am-escalations	AM Escalation drafter (contextual email from ticket)

### 1l. Create slash commands on api.slack.com/apps , should be unique (we can't share command names) and should match the code, ask glass to guide you through this and make sure they're unique, ask to update code accordingly if you want to name them differently

### 1m. Add the Gong MCP
run in terminal:
claude mcp add --transport http gong-guMCP-server https://mcp.gumloop.com/gong/mcp
**Auth in Gumloop with "Ramp", not "Personal"**

Type "claude" in terminal, press enter, wait for claude code to start 
ask claude "test gong API"

Send screenshots of your terminal to Glass (including the claude mcp add command above) and ask Glass to auth your slack bot with gong in same way


---

## Step 2: Automated Setup (Paste Into Glass)

Once all manual prerequisites are done, copy and paste this entire block into Project Glass. **Do not modify anything before pasting** — Glass will ask you for your info at the right time.

> **Tip:** Change the dropdown below Glass chat to "Auto Accept" for fewer confirmation prompts.

~~~
I need to set up Gary Bot from scratch. Follow these instructions carefully. You are an AI assistant running inside Project Glass. Run all commands yourself using your Bash tool. Never ask the user to type commands in Terminal — you run them. Only pause when a browser window opens and the user needs to interact with it. If user needs to paste a URL into a terminal (like for GitHub), make sure the URL does not get misformatted, paste the exact command in a code block if possible, run commands yourself whenever possible and minimize manual entry from the user.

IMPORTANT FORMATTING RULES:
- When showing URLs or file paths to the user, put them in a standalone code block so they don't get converted to markdown links.
- When running commands that contain URLs, run them directly with your Bash tool. Do not display the command for the user to copy.
- Never display raw URLs in your prose text. Either run the command yourself or put the URL in a code block.

IMPORTANT ERROR RECOVERY RULES:
- If any command fails, read the error output carefully and attempt to fix it before asking the user for help.
- If Homebrew install fails, check if Xcode Command Line Tools are needed: xcode-select --install
- If a brew install fails for one package, install the others individually — don't let one failure block everything.
- If a browser auth step fails, tell the user what went wrong and offer to retry.
- If pip install fails, try: pip install --upgrade pip setuptools wheel, then retry.

## Phase 1: Collect user info

Before doing anything, ask the user for the following values in a single message. Present it as a numbered list they can fill in. Do not proceed until you have every value. Slack tokens come later in Phase 4 — do NOT ask for them here.

1. Full name exactly as it appears on the Owner field of their Salesforce accounts (e.g. "Jane Smith")
2. First name only (for email sign-offs, e.g. "Jane")
3. Slack Member ID (tell them: click your profile photo in Slack, then three dots menu, then Copy member ID — looks like U03JBULM9LP)
4. ChiliPiper booking URL (looks like: ramp-com.chilipiper.com/me/jane-smith/ramp)
5. Ramp email address (their @ramp.com email)
6. Timezone — one of: US/Eastern, US/Central, US/Mountain, US/Pacific
7. Command prefix — a unique short lowercase word for their slash commands, no spaces (e.g. "jane"). Explain: this prefixes all slash commands like /jane-lookup, /jane-priorities so they don't conflict with other bot instances. Current prefixes in use: "gary", "taj". Pick something different.
8. Anthropic API key (from console.anthropic.com — starts with sk-ant-)

Store all values as variables for use in later phases.

## Phase 2: Install system dependencies

Run these commands yourself. Do not ask the user to run them.

Check if Homebrew is installed (run: which brew). If not, install it. If you just installed Homebrew, also run the PATH setup:
  echo >> ~/.zprofile && echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile && eval "$(/opt/homebrew/bin/brew shellenv)"

Then install system packages (install each one individually so a failure in one doesn't block the rest):
  brew install git
  brew install python@3.12
  brew install node
  brew install gh
  brew install google-cloud-sdk

Source the gcloud CLI into the current shell:
  source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"

Verify the key tools are available: python3.12 --version, node --version, gh --version, snow (will fail — installed later). If any are missing, check that Homebrew's PATH is set up correctly.

## Phase 3: GitHub auth + clone repo

Run: gh auth login
This opens a browser. Tell the user: "A browser window is opening for GitHub login. Choose GitHub.com, HTTPS, and Login with a web browser. Complete the login and tell me when done."
Wait for the user to confirm.

Then clone the repo:
  cd ~ && git clone https://github.com/gnallie-ramp/gary-bot.git

Verify the clone worked:
  ls ~/gary-bot/main.py

If it fails with "Permission denied", the user's GitHub account may not have access. Tell them to check their GitHub entitlements or ask in #help-it.

## Phase 4: Create Slack app + configure identity

Now you need to help the user create their Slack app using the manifest from the repo.

First, read the manifest file at ~/gary-bot/slack_app_manifest.yaml. It should contain 24 slash commands (including /gary-reengage and /gary-refresh-help). If you see fewer, the manifest is stale — tell the user to run `git pull` in ~/gary-bot and re-read it before continuing.

Using the COMMAND_PREFIX the user provided in Phase 1, find-and-replace "gary" with their prefix in ALL the slash command names (e.g. /gary-lookup becomes /jane-lookup if prefix is "jane"). Also replace the "name" and "display_name" fields with something unique like "<FirstName>'s Sales Bot".

Write the modified manifest to a temporary file at ~/gary-bot/my_manifest.yaml.

Then display the FULL contents of ~/gary-bot/my_manifest.yaml directly in chat so the user can copy it without opening any files. Tell them to select and copy the entire manifest from the chat.

Tell the user to do the following (show each step clearly):
1. Open api.slack.com/apps in their browser and sign in with their Ramp Slack account
2. Click "Create New App" then "From an app manifest"
3. Select the Ramp workspace
4. Switch the format tab to YAML
5. Paste the manifest you just copied from above
6. Click Next, then Create
7. Go to "Install App" in the left sidebar, click "Install to Workspace", then Allow
8. Copy the Bot Token: go to "OAuth & Permissions" in left sidebar, copy the "Bot User OAuth Token" (starts with xoxb-)
9. Copy the App Token: go to "Basic Information" in left sidebar, scroll to "App-Level Tokens", click "Generate Token", name it anything, add the scope "connections:write", click Generate, and copy the token (starts with xapp-)
10. Customize your bot's identity: go to "Basic Information", scroll to "Display Information", change the app name and add a profile picture (any image you want your bot to show in Slack)

Ask the user to paste both tokens (Bot Token and App Token). Store them as variables.

### Phase 4b: Add bot to alert channels

After the user confirms the tokens, tell them:

"Now invite your bot to the Slack channels it monitors. For each channel below, open it in Slack and type `/invite @YourBotName` (using whatever name you gave your bot)."

List ALL 8 required channels clearly:
1. #alerts-card-payable-bills
2. #alerts-self-serve-procurement-trials
3. #alerts-pclip-activations
4. #alerts-large-declines
5. #alerts-fundraising
6. #bill-pay-automatic-card-losses
7. #alerts-rclip-requests
8. #am-escalations

And one OPTIONAL channel:
9. #gam-ask-ai (only needed if they want `/<prefix>-refresh-help` to work — it scrapes Enablement Eddy's help-article links for use in auto-drafted emails)

Tell the user: "The bot will only process alerts tagged to you (via the 'Account Manager: @you' line in each alert), so it's safe to share these channels with other bot instances. Let me know when you've added the bot to the alert channels (and optionally #gam-ask-ai)."

Wait for the user to confirm before continuing.

## Phase 5: Python environment

Run these commands yourself:
  cd ~/gary-bot
  python3.12 -m venv venv
  source venv/bin/activate
  pip install --upgrade pip setuptools wheel
  pip install -r requirements.txt
  pip install snowflake-cli-labs

If pip install -r requirements.txt fails, try installing packages one at a time from the file to identify which one is failing.

Symlink snow CLI so launchd can find it:
  ln -sf "$(which snow)" /opt/homebrew/bin/snow

Verify: snow --version

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
  account = "rib11536.us-east-1"
  user = "<their ramp email>"
  authenticator = "externalbrowser"
  warehouse = "READER"
  database = "ANALYTICS"
  schema = "PUBLIC"
  role = "READER"

Set permissions: chmod 0600 ~/.snowflake/config.toml

Show the user both files and ask them to confirm the values look correct before continuing. Double-check that OWNER_NAME matches their Salesforce name exactly (case-sensitive) — this is the #1 cause of "no data" issues.

## Phase 7: Browser-based authentication

There are 2 browser auth steps. For EACH one: run the command yourself, tell the user a browser is opening and what to do, then wait for them to confirm before moving to the next.

Step 1 — Snowflake:
Run: cd ~/gary-bot && source venv/bin/activate && snow connection test
Tell user: "A browser window is opening for Snowflake/Okta SSO. Sign in with your Ramp/Okta credentials and tell me when done."
If it succeeds, you should see connection status output. If it fails with a timeout, tell the user to try again — sometimes Okta SSO is slow on first attempt.

Step 2 — Google Calendar:
Run: source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc" && gcloud auth application-default login --scopes=https://www.googleapis.com/auth/calendar.readonly
Tell user: "A browser window is opening for Google auth. Sign in with your Ramp Google account and tell me when done."

Note: Salesforce uses Gumstack MCP (same as Gmail/Gong) — no separate Salesforce login needed.

## Phase 8: Gumstack token auth (Gmail + Gong + Salesforce MCP access)

There are 3 MCP auth steps. For EACH one: run the command yourself, tell the user a browser is opening, and wait for them to confirm.

IMPORTANT: These commands will hang after auth completes — that's expected. Once the user confirms they authorized in the browser, kill the process (Ctrl+C) and move to the next step. The tokens are saved to disk on successful auth.

Step 1 — Gmail:
Run: npx mcp-remote https://mcp.gumloop.com/gmail/mcp
Tell user: "A browser window is opening for Gmail MCP auth. Select 'Personal' when prompted. Authorize with your Ramp Google account and tell me when done."

Step 2 — Gong:
Run: npx mcp-remote https://mcp.gumloop.com/gong/mcp
Tell user: "A browser window is opening for Gong MCP auth. Select 'Ramp' (not 'Personal') when prompted. Authorize and tell me when done."

Step 3 — Salesforce:
Run: npx mcp-remote https://mcp.gumloop.com/salesforce/mcp
Tell user: "A browser window is opening for Salesforce MCP auth. Select 'Personal' when prompted. Authorize with your Ramp account and tell me when done."

Verify tokens were saved:
  ls ~/.mcp-auth/mcp-remote-*/

You should see token files for each service. If any are missing, re-run that step.

## Phase 8b: Register Gong MCP with Glass

Run this command so that Glass (this app) also has direct Gong access for testing and troubleshooting:
  claude mcp add --transport http gong-guMCP-server https://mcp.gumloop.com/gong/mcp

Tell the user: "I've registered the Gong MCP with Glass. If a browser window opens for Gong auth, select 'Ramp' and authorize. This gives me (Glass) direct access to help you troubleshoot Gong issues later."

## Phase 8c: Salesforce writes via Growth MCP

Gary Bot uses Ramp's Growth MCP (growth-mcp-remote.ramp.builders/mcp) to create and update Salesforce opportunities — this powers the Pipeline tab's "Propose SFDC Updates" button and the post-meeting "Create Opp" flow.

The bot loads Growth MCP credentials from Project Glass first (~/.project-glass/credentials.json), with a fallback to per-user tokens at ~/.gary_bot_tokens/<slack_id>/growth_tokens.json. Since the user installed Glass in Step 1d, this should work automatically without any additional auth.

No action needed from the user in this phase — just tell them: "Salesforce writes go through Ramp's Growth MCP, using your Glass credentials. If you later see 'Growth MCP auth failed' in the bot logs, ask Glass to re-authenticate Growth MCP."

## Phase 9: Set up auto-start on login (launchd)

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

## Phase 10: Verify everything works

Wait 10 seconds, then check the log:
  tail -30 ~/.gary_bot_stdout.log

You should see "Scheduler started with 28 jobs" and "Snowflake connection OK". If you see errors, read them carefully — common issues:
- "No module named X" → pip install wasn't complete, re-run Phase 5
- "Invalid token" → Slack tokens are wrong, re-check .env
- "OWNER_NAME not found" → Name doesn't match Salesforce exactly

Run a test Snowflake query to confirm data access:
  cd ~/gary-bot && source venv/bin/activate && snow sql --query "SELECT 1 AS test" --format json --silent

Check if the bot is responding in Slack:
  Tell the user to DM their bot the word "status" in Slack. It should reply with a health check showing which integrations are connected.

Tell the user setup is complete. Give them this checklist to verify:

**Verification checklist:**
- [ ] Open the bot's Home tab in Slack — you should see your account data loading
- [ ] DM the bot "status" — should show a health check with integration statuses
- [ ] Home tab → **Pipeline** tab loads account-grain cards with AI deal-context summaries (may show "Generating AI summaries" banner on first open)
- [ ] Home tab → **Prospecting** tab shows a :test_tube: **Plays** section at the top with 5 signal plays (P1/P5/P7/P9/P13)
- [ ] Try your first slash command: /<prefix>-priorities
- [ ] Try the unified re-engage drafter: /<prefix>-reengage <account name>
- [ ] (Optional, only if #gam-ask-ai added) Try: /<prefix>-refresh-help — pulls help-article links from Enablement Eddy
- [ ] Wait for your next meeting to end — the bot should auto-draft a follow-up email
- [ ] Check that alert channels are working: the next time an alert mentions you as AM, the bot should DM you with a draft

**Staying up to date:**
When Greg pushes updates, pull the latest changes by running this in Glass or Terminal:
  cd ~/gary-bot && git pull && source venv/bin/activate && pip install -r requirements.txt && launchctl stop com.gary-bot && launchctl start com.gary-bot

Check CHANGELOG.md in the repo to see what changed in each version.
~~~

---

## Step 3: Verify Everything Works

After setup completes, test each integration:

| Test | How | Expected Result |
|------|-----|----------------|
| **Bot running** | Check log: `tail -20 ~/.gary_bot_stdout.log` | "Scheduler started with 28 jobs" |
| **Slack connected** | Open your bot's **Home tab** in Slack | Header + quota snapshot + priority alerts |
| **Snowflake** | `snow sql --query "SELECT CURRENT_USER()" --format json --silent` | Your email address |
| **Salesforce** | DM your bot: `status` and check Salesforce line | Shows "Connected" |
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
| Salesforce auth failed | Re-auth at gumloop.com/personal/apps > Salesforce > Reconnect |
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
