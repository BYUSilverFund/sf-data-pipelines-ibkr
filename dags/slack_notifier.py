import datetime as _dt
import logging
import os

import pytz
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


logger = logging.getLogger(__name__)


def slack_on_failure(context: dict) -> None:
    """Send failure notification to Slack when a task fails.

    Uses `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` from environment.
    Context is automatically passed by Airflow on task failure.
    """
    token = os.getenv("SLACK_BOT_TOKEN")
    channel = os.getenv("SLACK_CHANNEL_ID")

    if not token or not channel:
        logger.warning(
            "SLACK_BOT_TOKEN or SLACK_CHANNEL_ID not set; skipping Slack notification"
        )
        return

    try:
        mst = pytz.timezone("America/Denver")
        time = _dt.datetime.now(tz=mst).strftime("%Y-%m-%d %H:%M:%S")

        # Format message
        text = f"❌ Airflow Task Failed | Time: {time} MST"

        client = WebClient(token=token)
        client.chat_postMessage(
            channel=channel, text=text, unfurl_links=False, unfurl_media=False
        )
        logger.info("Sent Slack failure notification to %s", channel)
    except SlackApiError as e:
        logger.exception("Failed to send Slack message: %s", getattr(e, "response", e))
    except Exception as e:
        logger.exception("Unexpected error in slack_on_failure: %s", str(e))
