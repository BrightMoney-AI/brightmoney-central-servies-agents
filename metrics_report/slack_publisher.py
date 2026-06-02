"""
SlackPublisher — posts the Block Kit payload to a Slack channel via the Bot API.
"""
import logging

from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError

from .config import settings

log = logging.getLogger(__name__)


async def publish(payload: dict) -> None:
    client = AsyncWebClient(token=settings.slack_bot_token)
    try:
        response = await client.chat_postMessage(
            channel=settings.slack_channel_id,
            blocks=payload["blocks"],
            text="L0 Daily Metrics Report",  # fallback for notifications
        )
        log.info("Slack message posted: ts=%s", response["ts"])
    except SlackApiError as exc:
        log.error("Slack API error: %s", exc.response["error"])
        raise
