# Gary Bot — Setup Guide

Set up your own instance of Gary Bot in ~20 minutes.

## Prerequisites

- macOS (Gary runs locally on your machine)
- Python 3.9+
- Ramp Slack workspace access
- Ramp Snowflake access (READER role)
- Anthropic API key

---

## Step 1: Clone & Install (~5 min)

```bash
git clone <repo-url> gary-bot
cd gary-bot/slack_bot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Step 2: Create Your Slack App (~5 min)

1. Go to [api.slack.com/apps](https://api.slack.com/apps)
2. Click **Create New App** → **From manifest**
3. Select the Ramp workspace
4. Paste the contents of `slack_app_manifest.yaml` from this repo
5. Click **Create**
6. Go to **Install App** → **Install to Workspace** → Approve

Copy these values from the app settings:
- **Bot User OAuth Token** (`xoxb-...`) → this is your `SLACK_BOT_TOKEN`
- **App-Level Token** (create one with `connections:write` scope) → this is your `SLACK_APP_TOKEN`

---

## Step 3: Configure .env (~3 min)

```bash
cp .env.example .env
```

Edit `.env` with your values:

| Variable | Where to find it |
|----------|-----------------|
| `OWNER_NAME` | Your full name exactly as it appears in Salesforce |
| `OWNER_FIRST_NAME` | Your first name (for email sign-offs) |
| `OWNER_SLACK_ID` | Slack → click your profile → ⋮ → Copy member ID |
| `BOOKING_LINK` | Your ChiliPiper booking URL |
| `SLACK_BOT_TOKEN` | From Step 2 |
| `SLACK_APP_TOKEN` | From Step 2 |
| `ANTHROPIC_API_KEY` | [console.anthropic.com](https://console.anthropic.com) → API Keys |
| `GMAIL_ADDRESS` | Your Ramp email (optional — for IMAP reading) |
| `DISPLAY_TIMEZONE` | Your timezone, e.g. `US/Eastern`, `US/Pacific` |

> **Critical:** `OWNER_NAME` must match your Salesforce name exactly. This is used in every Snowflake query to filter to your book of business.

---

## Step 4: Snowflake Auth (~2 min)

Gary connects to Snowflake via the `snow` CLI using browser-based SSO.

```bash
pip install snowflake-cli
snow connection test
```

This opens your browser for Okta SSO. Once authenticated, tokens are cached automatically at `~/.snowflake/`. No PAT or VPN needed.

If you don't have the `snow` CLI config yet, create `~/.snowflake/config.toml`:

```toml
[connections.default]
account = "ramp"
authenticator = "externalbrowser"
warehouse = "READER"
database = "ANALYTICS"
role = "DEPT-SALES"
```

---

## Step 5: Google Calendar Auth (~3 min)

Gary checks your calendar every 30 minutes for upcoming meetings and auto-generates pre-meeting briefs.

```bash
# Install gcloud CLI if you don't have it
brew install google-cloud-sdk

# Authenticate with calendar read scope
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/calendar.readonly
```

This opens your browser. Sign in with your Ramp Google account. Tokens are cached at `~/.config/gcloud/application_default_credentials.json`.

---

## Step 6: Gumstack Gmail Auth (~3 min)

Gary creates email drafts in your Gmail via Gumstack MCP (not IMAP).

1. Go to [gumloop.com/personal/apps](https://www.gumloop.com/personal/apps)
2. Log in with your Ramp account
3. Connect **Gmail** (authorize access)
4. Run the MCP auth flow once to cache tokens locally:

```bash
npx mcp-remote https://mcp.gumloop.com/gmail/mcp
```

Follow the browser prompts. Tokens are cached at `~/.mcp-auth/`.

> If email drafts start failing with 401 errors, re-authenticate at gumloop.com/personal/apps.

---

## Step 7: Start the Bot

```bash
cd slack_bot
source venv/bin/activate
nohup venv/bin/python3 main.py >> nohup.out 2>&1 & disown
```

### Verify it's working:

1. **Check logs:** `tail -20 nohup.out` — should show "Bolt app is running!"
2. **Home tab:** Open your bot in Slack → Home tab should load with your quota snapshot
3. **DM test:** Send "status" to the bot → should get a health check response
4. **DM test:** Send "priorities" → should see YOUR accounts, not anyone else's

### Stop the bot:

```bash
kill $(cat ~/.gary_bot.pid)
```

---

## Step 8: (Optional) Auto-start on Login

To keep Gary running even after restarts, create a launchd plist:

```bash
cat > ~/Library/LaunchAgents/com.gary-bot.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.gary-bot</string>
    <key>ProgramArguments</key>
    <array>
        <string>/path/to/gary-bot/slack_bot/venv/bin/python3</string>
        <string>/path/to/gary-bot/slack_bot/main.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/path/to/gary-bot/slack_bot</string>
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
        <string>/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
EOF
```

Update the paths, then load it:

```bash
launchctl load ~/Library/LaunchAgents/com.gary-bot.plist
```

---

## Customization

### Rename the bot
- Change the bot name in your Slack app settings (api.slack.com/apps)
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
| "No module named 'slack_bolt'" | Activate virtualenv: `source venv/bin/activate` |
| Snowflake connection fails | Run `snow connection test` to refresh SSO |
| Gmail drafts queuing (not creating) | Re-auth at gumloop.com/personal/apps |
| Bot starts then immediately dies | Check `nohup.out` for the error. Common: expired Snowflake token |
| No data in Home tab | Verify `OWNER_NAME` matches your Salesforce name exactly |
| Duplicate bot processes | `kill $(cat ~/.gary_bot.pid)` — the PID lock prevents dupes on restart |
