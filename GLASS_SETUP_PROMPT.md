# Gary Bot — Glass Setup Prompt

**For teammates who want their own Gary Bot instance.** Paste the block below into Project Glass and it will walk you through the full setup end-to-end.

## Before you paste

Finish these **Step 1** manual prerequisites first — Glass cannot do them for you:

1. **Request access** via ConductorOne/Okta (can take hours, do this first):
   - Snowflake (General access, not unmasked)
   - Claude Code – API Key
   - Gumloop
2. **Get an Anthropic API key** at console.anthropic.com → Settings → API Keys → Create. Save it for Phase 1.
3. **Connect Gmail, Gong, Salesforce on Gumloop** at gumloop.com/personal/apps → log in with Ramp account → Connect each:
   - Gmail → "Personal"
   - Gong → "Ramp"
   - Salesforce → "Personal"
4. **Install Project Glass** via Self Service+.app
5. **Create a GitHub account** at github.com (log in with Google → Ramp email)
6. Pick a **slash command prefix** — short lowercase word, no spaces (e.g. "jane"). All your bot's slash commands will be prefixed with this (e.g. `/jane-priorities`). Must be different from any teammate's — if in doubt, ask in your team channel.

## How to use the prompt

1. Open Project Glass
2. Set the dropdown under the chat to **Auto Accept** (fewer confirmation prompts)
3. Copy **everything between the `~~~` fences** below
4. Paste into Glass and hit enter
5. Glass will ask you for a short list of info up front — have your Anthropic key, Slack member ID, and ChiliPiper URL ready

Setup takes ~30 minutes end-to-end, most of which is waiting for browser auths.

---

~~~
I need to set up Gary Bot (a Slack sales bot) from scratch on this Mac. Follow these instructions carefully. You are running inside Project Glass with Bash tool access. Run all commands yourself — never ask the user to type commands in Terminal. Only pause when a browser window opens and the user has to interact with it, or when you explicitly need info from them.

IMPORTANT FORMATTING RULES:
- When showing URLs or file paths, put them in a standalone code block so they don't get converted to markdown links.
- When running commands with URLs inside them, run them yourself — do not display the command for the user to copy.
- Never display raw URLs in prose text. Run the command yourself or show the URL inside a code block.

IMPORTANT ERROR RECOVERY RULES:
- If any command fails, read the stderr carefully and try to fix it before asking the user.
- If Homebrew install fails, check for Xcode Command Line Tools: `xcode-select --install`.
- If `brew install X` fails for one package, install the others individually — don't let a single failure block everything.
- If browser auth fails, describe what went wrong and offer to retry.
- If `pip install` fails, try `pip install --upgrade pip setuptools wheel` then retry.
- If any phase finishes with errors, summarize what failed and what state the system is in before moving on.

## Phase 0: Session kickoff + prereq gate

Confirm with the user that all of the Step 1 manual prerequisites are done:
1. Anthropic API key generated (they have it ready to paste)
2. Gumloop connections made for Gmail (Personal), Gong (Ramp), Salesforce (Personal)
3. Project Glass installed (you're running inside it — check with `ls ~/.project-glass` and confirm the directory exists)
4. GitHub account created (github.com)
5. They have their Ramp email, ChiliPiper booking URL, and Slack Member ID available

Also detect Mac architecture:
  `uname -m` → store as ARCH (arm64 = Apple Silicon, x86_64 = Intel)
  `brew --prefix` → store as BREW_PREFIX. Use this instead of hardcoding /opt/homebrew.

If Homebrew isn't installed yet, that's fine — we'll install it in Phase 2 and BREW_PREFIX will resolve then.

If the user says prereqs aren't done, tell them which ones and wait. Don't proceed until they confirm everything above.

## Phase 1: Collect user info

Ask the user for the following in a single numbered list. Do not proceed until you have every value. Slack tokens come later in Phase 4 — do NOT ask for them here.

1. Full name exactly as it appears on the Owner field of their Salesforce accounts (e.g. "Jane Smith"). Case matters. Wrong case is the #1 cause of "no data" issues.
2. First name only (for email sign-offs, e.g. "Jane")
3. Slack Member ID (click their profile photo in Slack → three-dot menu → Copy member ID — looks like U03JBULM9LP)
4. ChiliPiper booking URL (looks like: ramp-com.chilipiper.com/me/jane-smith/ramp)
5. Ramp email address
6. Timezone — one of: US/Eastern, US/Central, US/Mountain, US/Pacific
7. Command prefix — short lowercase word, no spaces (e.g. "jane"). Prefixes all slash commands like /jane-priorities so they don't conflict with other bot instances.
8. Anthropic API key (from console.anthropic.com — starts with sk-ant-)

Store all values as variables for later phases.

## Phase 2: Install system dependencies

Run these yourself. Don't ask the user.

Check if Homebrew is installed: `which brew`. If not, install it per brew.sh instructions. After install, set up the PATH:
  `echo >> ~/.zprofile && echo 'eval "$(${BREW_PREFIX}/bin/brew shellenv)"' >> ~/.zprofile && eval "$(${BREW_PREFIX}/bin/brew shellenv)"`

Then install system packages one at a time so a failure in one doesn't block the rest:
  `brew install git`
  `brew install python@3.12`
  `brew install node`
  `brew install gh`
  `brew install google-cloud-sdk`

Source the gcloud CLI into the current shell:
  `source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"`

Verify: `python3.12 --version`, `node --version`, `gh --version`. If missing, check that Homebrew's PATH is set up.

## Phase 3: GitHub auth + clone repo

`gh auth login` opens a browser. Tell the user: "A browser window is opening for GitHub login. Choose GitHub.com → HTTPS → Login with a web browser. Complete the login and tell me when done."

Wait for confirmation, then clone:
  `cd ~ && git clone https://github.com/gnallie-ramp/gary-bot.git`

Verify:
  `ls ~/gary-bot/main.py`

If you see "Permission denied", the user's GitHub account doesn't have access to the repo. Tell them to check GitHub entitlements in ConductorOne or ask in #help-it.

## Phase 4: Create Slack app + configure identity

Read the manifest at `~/gary-bot/slack_app_manifest.yaml`. It should list **24 slash commands** (including /gary-reengage and /gary-refresh-help). If you see fewer than 24, the clone is stale — run `cd ~/gary-bot && git pull` and re-read before proceeding.

Using the COMMAND_PREFIX from Phase 1, find-and-replace "gary" with their prefix across ALL slash command names AND the `name` / `display_name` fields (e.g. make `display_name: "Jane's Sales Bot"`).

Write the modified manifest to `~/gary-bot/my_manifest.yaml`.

Display the FULL contents of `~/gary-bot/my_manifest.yaml` directly in chat so the user can copy it without opening files. Tell them to select and copy the entire manifest from the chat.

Then walk them through these steps in order:
1. Open api.slack.com/apps and sign in with their Ramp Slack account
2. Click "Create New App" → "From an app manifest"
3. Select the Ramp workspace
4. Switch the format tab to YAML
5. Paste the manifest they just copied
6. Click Next, then Create
7. Go to "Install App" in the left sidebar → "Install to Workspace" → Allow
8. Go to "OAuth & Permissions" → copy the **Bot User OAuth Token** (starts with xoxb-)
9. Go to "Basic Information" → scroll to "App-Level Tokens" → click "Generate Token" → name it anything → add scope `connections:write` → Generate → copy the token (starts with xapp-)
10. Go to "Basic Information" → "Display Information" → set an app name + profile picture

Ask the user to paste both tokens (Bot Token and App Token). Store them as variables.

### Phase 4b: Invite the bot to alert channels

Tell the user to run `/invite @YourBotName` in each of these 8 required channels:
1. #alerts-card-payable-bills
2. #alerts-self-serve-procurement-trials
3. #alerts-pclip-activations
4. #alerts-large-declines
5. #alerts-fundraising
6. #bill-pay-automatic-card-losses
7. #alerts-rclip-requests
8. #am-escalations

Plus one OPTIONAL channel:
9. #gam-ask-ai — only needed if they want /<prefix>-refresh-help (it scrapes Enablement Eddy's help-article links from this channel)

Explain: "The bot only processes alerts tagged to you (via the 'Account Manager: @you' line in each alert), so these channels are safe to share with other bot instances."

Wait for confirmation before continuing.

## Phase 5: Python environment

Run:
  `cd ~/gary-bot`
  `python3.12 -m venv venv`
  `source venv/bin/activate`
  `pip install --upgrade pip setuptools wheel`
  `pip install -r requirements.txt`
  `pip install snowflake-cli-labs`

If `pip install -r requirements.txt` fails, install packages one at a time to find the culprit.

Symlink the `snow` CLI so launchd can find it:
  `ln -sf "$(which snow)" $(brew --prefix)/bin/snow`

Verify: `snow --version`.

## Phase 6: Create config files

Write `~/gary-bot/.env` (substitute actual values, no placeholders):

  OWNER_NAME=<their salesforce name from Phase 1.1>
  OWNER_FIRST_NAME=<their first name from Phase 1.2>
  OWNER_SLACK_ID=<their slack member id>
  BOOKING_LINK=<their chilipiper url>
  DISPLAY_TIMEZONE=<their timezone>
  COMMAND_PREFIX=<their chosen prefix>
  SLACK_BOT_TOKEN=<bot token from Phase 4>
  SLACK_APP_TOKEN=<app token from Phase 4>
  ANTHROPIC_API_KEY=<their anthropic key>
  GMAIL_ADDRESS=<their ramp email>

Write `~/.snowflake/config.toml` (create the directory if needed):

  [connections.default]
  account = "rib11536.us-east-1"
  user = "<their ramp email>"
  authenticator = "externalbrowser"
  warehouse = "READER"
  database = "ANALYTICS"
  schema = "PUBLIC"
  role = "READER"

Set permissions: `chmod 0600 ~/.snowflake/config.toml`

Show the user both files and confirm the values look correct before continuing. Double-check that OWNER_NAME matches their Salesforce name exactly (case-sensitive) — this is the #1 cause of "no data" issues.

## Phase 7: Browser-based authentication

Two browser steps. For each: run the command yourself, tell the user what's happening, wait for confirmation.

Step 1 — Snowflake:
  `cd ~/gary-bot && source venv/bin/activate && snow connection test`
  Tell user: "A browser is opening for Snowflake/Okta SSO. Sign in with your Ramp/Okta credentials and tell me when done."
  On success you'll see connection status. If it times out, retry — sometimes Okta SSO is slow on first attempt.

Step 2 — Google Calendar:
  `source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc" && gcloud auth application-default login --scopes=https://www.googleapis.com/auth/calendar.readonly`
  Tell user: "A browser is opening for Google auth. Sign in with your Ramp Google account and tell me when done."

Note: Salesforce reads go through Gumstack MCP in Phase 8 — no separate SFDC login needed here.

## Phase 8: Gumstack MCP auth (Gmail + Gong + Salesforce reads)

Three MCP auths. For each: run the command yourself, tell the user a browser is opening, wait for confirmation.

IMPORTANT: These commands HANG after auth completes — that's expected. Once the user confirms they authorized in the browser, kill the process with Ctrl+C and move on. Tokens are saved to disk on successful auth.

Step 1 — Gmail:
  `npx mcp-remote https://mcp.gumloop.com/gmail/mcp`
  "A browser is opening for Gmail MCP auth. Select 'Personal' when prompted. Authorize with your Ramp Google account and tell me when done."

Step 2 — Gong:
  `npx mcp-remote https://mcp.gumloop.com/gong/mcp`
  "A browser is opening for Gong MCP auth. Select 'Ramp' (NOT 'Personal'). Authorize and tell me when done."

Step 3 — Salesforce:
  `npx mcp-remote https://mcp.gumloop.com/salesforce/mcp`
  "A browser is opening for Salesforce MCP auth. Select 'Personal'. Authorize with your Ramp account and tell me when done."

Verify tokens saved:
  `ls ~/.mcp-auth/mcp-remote-*/`

## Phase 8b: Register Gong MCP with Glass

Run:
  `claude mcp add --transport http gong-guMCP-server https://mcp.gumloop.com/gong/mcp`

Tell the user: "I've registered the Gong MCP with Glass so I have direct Gong access for troubleshooting. If a browser opens, select 'Ramp' and authorize."

## Phase 8c: Salesforce writes via Growth MCP

The bot uses Ramp's Growth MCP (growth-mcp-remote.ramp.builders/mcp) to CREATE and UPDATE Salesforce opportunities — this powers the Pipeline tab's "Propose SFDC Updates" button and the post-meeting "Create Opp" flow.

The bot loads Growth MCP credentials from Project Glass first (~/.project-glass/credentials.json) with a fallback to ~/.gary_bot_tokens/<slack_id>/growth_tokens.json. Since Glass is already installed (Step 1d), this works automatically — no extra auth needed.

Tell the user: "Salesforce writes go through Ramp's Growth MCP using your Glass credentials. If you ever see 'Growth MCP auth failed' in the bot logs, ask Glass to re-authenticate Growth MCP for you."

## Phase 9: Auto-start on login (launchd)

Run: `mkdir -p ~/Library/LaunchAgents`

Write `~/Library/LaunchAgents/com.gary-bot.plist` using the user's actual $HOME path (launchd does not expand $HOME — run `echo $HOME` and substitute it). Also resolve the Homebrew path with `brew --prefix` so this works on both Apple Silicon and Intel Macs:

  Label: com.gary-bot
  ProgramArguments: <HOME>/gary-bot/venv/bin/python3, <HOME>/gary-bot/main.py
  WorkingDirectory: <HOME>/gary-bot
  RunAtLoad: true
  KeepAlive: true
  ThrottleInterval: 10
  StandardOutPath: <HOME>/.gary_bot_stdout.log
  StandardErrorPath: <HOME>/.gary_bot_stderr.log
  EnvironmentVariables > PATH: <BREW_PREFIX>/bin:/usr/local/bin:/usr/bin:/bin

Load it: `launchctl load ~/Library/LaunchAgents/com.gary-bot.plist`

## Phase 10: Verify

Wait 15 seconds, then check the log:
  `tail -30 ~/.gary_bot_stdout.log`

You should see "Scheduler started with 28 jobs" and "Snowflake connection OK". Common issues:
- "No module named X" → pip install wasn't complete, re-run Phase 5
- "Invalid token" → Slack tokens are wrong, re-check .env
- "OWNER_NAME not found" → Name doesn't match Salesforce exactly

Run a test query that PROVES the user's data is visible (not just `SELECT 1`):
  `cd ~/gary-bot && source venv/bin/activate && snow sql --query "SELECT COUNT(DISTINCT account_id) AS n_accounts FROM analytics.agg.agg_sfdc__daily_account_owner_ledger WHERE date_day = CURRENT_DATE - 1 AND owner_name = '<OWNER_NAME>'" --format json --silent`

Expected: a positive number (typically 500-5000). If you get 0, OWNER_NAME doesn't match Salesforce — fix and retry.

Check if the bot is responding in Slack — tell the user to DM their bot the word "status". Should reply with a health check.

Setup is complete. Give the user this checklist:

**Verification checklist:**
- [ ] Open Slack → Home tab → bot profile — you see your account data loading
- [ ] DM the bot "status" — should reply with a health check showing which integrations are connected
- [ ] Home tab → **Pipeline** tab — loads account-grain cards with AI deal-context summaries (may show a "Generating AI summaries" banner on first open)
- [ ] Home tab → **Prospecting** tab — top section shows a :test_tube: Plays header with 5 signal plays (P1/P5/P7/P9/P13)
- [ ] Try a slash command: `/<prefix>-priorities`
- [ ] Try the unified re-engage drafter: `/<prefix>-reengage <account name>`
- [ ] (Only if they added #gam-ask-ai) Try: `/<prefix>-refresh-help`
- [ ] Wait for their next meeting to end — the bot should auto-draft a follow-up email within ~3 min (Granola) or ~15 min (Gong fallback)
- [ ] Alert channels: the next time an alert mentions them as AM, the bot should DM them with a draft

**Staying up to date:**
When updates are pushed, run this in Glass or Terminal to pull and restart:
  `cd ~/gary-bot && git pull && source venv/bin/activate && pip install -r requirements.txt && launchctl stop com.gary-bot && launchctl start com.gary-bot`

Check `CHANGELOG.md` in the repo to see what changed in each version.
~~~

---

## After setup — what to know

- **Logs:** `tail -50 ~/.gary_bot_stdout.log` for anything weird
- **Full troubleshooting table:** See [SETUP.md § Troubleshooting](SETUP.md#troubleshooting)
- **Manage the bot:** `launchctl stop/start com.gary-bot`
- **Customization:** Open the bot's Home tab → Settings tab to toggle notifications per-user
- **Adding teammates who don't want their own instance:** See [SETUP_TEAMMATE.md](SETUP_TEAMMATE.md) — they register via Home tab on your instance, no separate Mac needed
