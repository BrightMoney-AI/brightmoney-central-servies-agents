"""
SlackPublisher — posts Block Kit payloads or a Canvas to a Slack channel.
"""
from __future__ import annotations
import logging

from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.errors import SlackApiError

from .config import settings

log = logging.getLogger(__name__)


async def publish(payload: dict) -> None:
    """Post a single Block Kit message."""
    client = AsyncWebClient(token=settings.slack_bot_token)
    try:
        response = await client.chat_postMessage(
            channel=settings.slack_channel_id,
            blocks=payload["blocks"],
            text=payload.get("text", "L0 Daily Metrics Report"),
        )
        log.info("Slack message posted: ts=%s", response["ts"])
    except SlackApiError as exc:
        log.error("Slack API error: %s", exc.response["error"])
        raise


async def publish_canvas(markdown: str, summary_blocks: list[dict], title: str) -> None:
    """
    1. Create a standalone canvas (stored in Slack, not external).
    2. Share it to the channel so Slack allows attaching it to a message.
    3. Post the summary message with file_ids=[canvas_id] so it renders
       as the native "Canvas ▼" card directly below the message.
    """
    client = AsyncWebClient(token=settings.slack_bot_token)

    # Step 1 — create the canvas.
    #   SDK methods (canvases_create / conversations_canvases_create) use
    #   params= internally which causes HTTP 414 for large documents.
    #   Use api_call with json= so the body goes as a POST payload.
    try:
        response = await client.api_call(
            "canvases.create",
            json={
                "title": title,
                "document_content": {
                    "type": "markdown",
                    "markdown": markdown,
                },
            },
        )
        canvas_id = response.get("canvas_id", "")
        log.info("Canvas created: canvas_id=%s  title=%r", canvas_id, title)
    except SlackApiError as exc:
        log.error("Canvas create error: %s", exc.response["error"])
        raise

    # Step 2 — resolve workspace URL (needed to build the canvas deep-link)
    try:
        auth        = await client.auth_test()
        team_id     = auth.get("team_id", "")
        workspace   = auth.get("url", "").rstrip("/")   # https://brightmoney.slack.com
        canvas_url  = f"{workspace}/docs/{team_id}/{canvas_id}"
    except SlackApiError:
        canvas_url  = ""

    # Step 3 — post the formatted summary message
    try:
        msg = await client.chat_postMessage(
            channel=settings.slack_channel_id,
            text=f"📊 {title}",
            blocks=summary_blocks,
        )
        log.info("Summary posted: ts=%s", msg["ts"])
    except SlackApiError as exc:
        log.error("Slack post error: %s", exc.response["error"])
        raise

    # Step 4 — post the canvas URL as a plain-text follow-up message.
    #           Slack unfurls internal slack.com/docs/… URLs into the
    #           native "Canvas ▼" card automatically.
    if canvas_url:
        try:
            await client.chat_postMessage(
                channel=settings.slack_channel_id,
                text=canvas_url,
                unfurl_links=True,
            )
            log.info("Canvas card posted: %s", canvas_url)
        except SlackApiError as exc:
            log.error("Canvas card post error: %s", exc.response["error"])
