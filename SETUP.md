# Gary Bot — Setup Guide

Set up your own instance of Gary Bot in ~30 minutes. This guide assumes a **fresh Mac** with nothing installed — it walks through every step from scratch.

---

## Step 0: Install Developer Tools (~5 min)

Open the **Terminal** app (search "Terminal" in Spotlight, or find it in Applications → Utilities).

All commands below are typed into Terminal. Copy each block, paste it in, and press Enter.

### 0a. Install Homebrew (Mac package manager)

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Follow the prompts (enter your Mac password when asked — you won't see characters as you type, that's normal). When it finishes, it may tell you to run two commands to add Homebrew to your PATH. **Run those commands** — they look like:

```bash
echo >> ~/.zprofile
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"
```

Verify it worked:

```bash
brew --version
```

You should see something like `Homebrew 4.x.x`.

### 0b. Install Git, Python, Node.js, and GitHub CLI

```bash
brew install git python@3.12 node gh
```

This installs everything you need in one shot. Verify:

```bash
git --version
python3 --version
node --version
gh --version
```

### 0c. Authenticate with GitHub

```bash
gh auth login
```

Choose:
- **GitHub.com**
- **HTTPS**
- **Login with a web browser**

It opens your browser — sign in with your GitHub account and authorize. Once done, you can clone repos.

---

## Step 1: Clone the Repo & Install Dependencies (~3 min)

```bash
cd ~
git clone https://github.com/gnallie-ramp/gary-bot.git
cd gary-bot/slack_bot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

> **What just happened:** You downloaded the code, created an isolated Python environment, and installed all the libraries the bot needs.

---

## Step 2: Create Your Slack App (~5 min)

1. Go to [api.slack.com/apps](https://api.slack.com/apps) (sign in with your Ramp Slack account)
2. Click **Create New App** → **From an app manifest**
3. Select the **Ramp workspace**
4. Switch the format tab to **YAML**
5. Delete whatever is in the text box, then paste the entire contents of the file `slack_app_manifest.yaml` from this repo
   - To see the file: run `cat ~/gary-bot/slack_bot/slack_app_manifest.yaml` in Terminal and copy the output
6. Click **Next** → **Create**
7. Go to **Install App** (left sidebar) → **Install to Workspace** → **Allow**

Now grab your two tokens:

**Bot Token:**
- Go to **OAuth & Permissions** (left sidebar)
- Copy the **Bot User OAuth Token** — starts with `xoxb-`

**App-Level Token:**
- Go to **Basic Information** (left sidebar)
- Scroll to **App-Level Tokens** → click **Generate Token and Scopes**
- Name it anything (e.g. `socket`)
- Add the scope `connections:write`
- Click **Generate**
- Copy the token — starts with `xapp-`

Keep both tokens handy for the next step.

---

## Step 3: Configure Your .env File (~3 min)

```bash
cd ~/gary-bot/slack_bot
cp .env.example .env
open -e .env
```

This opens the file in TextEdit. Fill in your values:

| Variable | Where to find it |
|----------|-----------------|
| `OWNER_NAME` | Your full name **exactly** as it appears in Salesforce (e.g. `Jane Smith`) |
| `OWNER_FIRST_NAME` | Your first name (e.g. `Jane`) — used in email sign-offs |
| `OWNER_SLACK_ID` | In Slack: click your profile photo → click ⋮ (three dots) → **Copy member ID** |
| `BOOKING_LINK` | Your ChiliPiper booking URL |
| `SLACK_BOT_TOKEN` | The `xoxb-` token from Step 2 |
| `SLACK_APP_TOKEN` | The `xapp-` token from Step 2 |
| `ANTHROPIC_API_KEY` | Go to [console.anthropic.com](https://console.anthropic.com) → **API Keys** → create one |
| `GMAIL_ADDRESS` | Your Ramp email (optional — only for IMAP reading) |
| `DISPLAY_TIMEZONE` | Your timezone: `US/Eastern`, `US/Central`, `US/Mountain`, or `US/Pacific` |

Save and close the file.

> **Critical:** `OWNER_NAME` must match your Salesforce name **exactly** (capitalization, spacing, and all). This is what every Snowflake query uses to pull your book of business. If it doesn't match, you'll see someone else's data or no data at all.

---

## Step 4: Snowflake Auth (~3 min)

Gary pulls all account data from Snowflake using browser-based SSO (Okta). No passwords or tokens to manage.

### 4a. Create the Snowflake config file

```bash
mkdir -p ~/.snowflake
cat > ~/.snowflake/config.toml << 'EOF'
[connections.default]
account = "ramp"
authenticator = "externalbrowser"
warehouse = "READER"
database = "ANALYTICS"
role = "DEPT-SALES"
EOF
```

### 4b. Install the Snowflake CLI and test

```bash
pip install snowflake-cli
snow connection test
```

This opens your browser for Okta SSO. Sign in with your Ramp credentials. Once authenticated, tokens are cached at `~/.snowflake/` — you won't need to sign in again for a while.

> **Don't have Snowflake access?** You need the READER role. Ask your manager or IT to grant you access to Snowflake with the `DEPT-SALES` role.

---

## Step 5: Google Calendar Auth (~3 min)

Gary checks your calendar every 30 minutes and auto-generates pre-meeting briefs for external customer calls.

```bash
brew install google-cloud-sdk
```

After install, restart your terminal or run:

```bash
source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"
```

Then authenticate:

```bash
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/calendar.readonly
```

This opens your browser. Sign in with your **Ramp Google account**. Tokens are cached at `~/.config/gcloud/application_default_credentials.json`.

---

## Step 6: Gumstack Gmail Auth (~3 min)

Gary creates email drafts in your Gmail via Gumstack (not IMAP). This is how all auto-drafted emails land in your Drafts folder.

1. Go to [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps)
2. Log in with your Ramp account
3. Find **Gmail** and click **Connect** → authorize access

Then run the MCP auth flow once to cache tokens locally:

```bash
npx mcp-remote https://mcp.gumloop.com/gmail/mcp
```

Follow the browser prompts. Tokens are cached at `~/.mcp-auth/`.

> **If email drafts stop working later:** Re-authenticate at [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps) and re-run the `npx` command above.

---

## Step 7: Start the Bot (~1 min)

Make sure you're in the right directory with the virtualenv active:

```bash
cd ~/gary-bot/slack_bot
source venv/bin/activate
```

Start the bot in the background:

```bash
nohup venv/bin/python3 main.py >> nohup.out 2>&1 & disown
```

### Verify it's working:

1. **Check logs:**
   ```bash
   tail -20 nohup.out
   ```
   You should see `Bolt app is running!`

2. **Home tab:** Open Slack → find your bot in the Apps section (or search its name) → click the **Home** tab. You should see your quota snapshot.

3. **DM test:** Send `status` to the bot → should get a health check response.

4. **DM test:** Send `priorities` → should see **your** accounts, not anyone else's.

### Stop the bot:

```bash
kill $(cat ~/.gary_bot.pid)
```

### Restart the bot (after changes or if it dies):

```bash
cd ~/gary-bot/slack_bot
kill $(cat ~/.gary_bot.pid) 2>/dev/null
source venv/bin/activate
nohup venv/bin/python3 main.py >> nohup.out 2>&1 & disown
```

---

## Step 8: (Optional) Auto-start on Login

To keep Gary running even after you restart your Mac, set up a launch agent.

First, find your exact Python path:

```bash
echo ~/gary-bot/slack_bot/venv/bin/python3
echo ~/gary-bot/slack_bot/main.py
echo ~/gary-bot/slack_bot
```

Then create the launch agent (update the paths if you cloned to a different location):

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
        <string>$HOME/gary-bot/slack_bot/venv/bin/python3</string>
        <string>$HOME/gary-bot/slack_bot/main.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$HOME/gary-bot/slack_bot</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/gary-bot.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/gary-bot.err</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
EOF
```

Load it:

```bash
launchctl load ~/Library/LaunchAgents/com.gary-bot.plist
```

Now Gary starts automatically whenever you log in. To stop the auto-start:

```bash
launchctl unload ~/Library/LaunchAgents/com.gary-bot.plist
```

---

## Step 9: (Optional) Keep the Bot Updated

When Greg pushes updates to the repo:

```bash
cd ~/gary-bot
git pull
cd slack_bot
source venv/bin/activate
pip install -r requirements.txt
kill $(cat ~/.gary_bot.pid) 2>/dev/null
nohup venv/bin/python3 main.py >> nohup.out 2>&1 & disown
```

---

## Customization

### Rename the bot
- Change the bot name in your Slack app settings at [api.slack.com/apps](https://api.slack.com/apps)
- Update `GARY_STYLE_GUIDE.md` to match your bot's personality

### Adjust signal thresholds
- Signal detection logic lives in `queries/queries.py` → `HOME_PRIORITY_ALERTS_QUERY`
- Thresholds (1.5x baseline, $5K minimum delta, etc.) can be tuned per product

### Toggle notifications
- Open the bot's Home tab → Settings tab → toggle individual alert types on/off

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `command not found: brew` | Re-run the Homebrew install from Step 0a, then run the PATH commands it prints |
| `command not found: python3` | Run `brew install python@3.12` |
| `command not found: git` | Run `brew install git` |
| `No module named 'slack_bolt'` | You're not in the virtualenv. Run `cd ~/gary-bot/slack_bot && source venv/bin/activate` |
| Snowflake connection fails | Run `snow connection test` to refresh SSO tokens |
| Gmail drafts queuing (not creating) | Re-auth at [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps) |
| Bot starts then immediately dies | Check `tail -50 nohup.out` for the error. Common: expired Snowflake token |
| No data in Home tab | Verify `OWNER_NAME` in `.env` matches your Salesforce name exactly |
| Duplicate bot processes | Run `kill $(cat ~/.gary_bot.pid)` — the PID lock prevents dupes on restart |
| `Permission denied` on git clone | Run `gh auth login` to authenticate with GitHub |
| `gcloud: command not found` after install | Run `source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"` or restart Terminal |
| Bot shows someone else's data | Your `OWNER_NAME` in `.env` doesn't match your Salesforce name — check spelling and capitalization |
