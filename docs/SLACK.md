Slack notifications for DAG failures setup
=================================

1) Config
   environment variables are stored in .env for local development
   SLACK_BOT_TOKEN=xoxb-...  # keep secret
   SLACK_CHANNEL_ID=C09N8HD9YJ1

2) Scopes required (set on https://api.slack.com/apps)

   - `chat:write`
   - (optional) `chat:write.public` if you need to post to channels the bot is not invited to

3) Install/Invite

   - Install the app to your workspace.
   - Invite the bot to the target channel: `/invite @YourAppName`
