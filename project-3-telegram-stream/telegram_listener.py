"""Print live messages from configured public Telegram channels.

This listener is intentionally transport-only: its event handler is the seam where
a future Kafka producer can be added without changing Telegram authentication or
subscription code.
"""

from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from urllib.parse import urlparse

from dotenv import load_dotenv
from telethon import TelegramClient, events


@dataclass(frozen=True)
class Settings:
    """Runtime configuration loaded from environment variables."""

    api_id: int
    api_hash: str
    session_name: str
    channels: list[str]


def normalize_channel_reference(reference: str) -> str:
    """Convert a public Telegram link to the username Telethon expects.

    Supports ``channel_name``, ``https://t.me/channel_name``, and Telegram's
    browser-preview form, ``https://t.me/s/channel_name``.
    """
    reference = reference.strip().lstrip("@")
    if "t.me/" not in reference and "telegram.me/" not in reference:
        return reference

    url = reference if "://" in reference else f"https://{reference}"
    parts = [part for part in urlparse(url).path.split("/") if part]
    if parts[:1] == ["s"]:
        parts = parts[1:]

    if len(parts) != 1:
        raise ValueError(
            f"'{reference}' is not a public channel username or public channel link."
        )
    return parts[0]


def load_settings() -> Settings:
    """Load and validate listener configuration."""
    load_dotenv()

    missing = [
        name
        for name in ("TELEGRAM_API_ID", "TELEGRAM_API_HASH", "TELEGRAM_CHANNELS")
        if not os.getenv(name)
    ]
    if missing:
        raise ValueError(f"Missing required environment variable(s): {', '.join(missing)}")

    try:
        api_id = int(os.environ["TELEGRAM_API_ID"])
    except ValueError as error:
        raise ValueError("TELEGRAM_API_ID must be a number.") from error

    channels = [
        normalize_channel_reference(channel)
        for channel in os.environ["TELEGRAM_CHANNELS"].split(",")
    ]
    channels = [channel for channel in channels if channel]
    if not channels:
        raise ValueError("TELEGRAM_CHANNELS must include at least one channel.")

    return Settings(
        api_id=api_id,
        api_hash=os.environ["TELEGRAM_API_HASH"],
        session_name=os.getenv("TELEGRAM_SESSION_NAME", "telegram_listener"),
        channels=channels,
    )


async def run_listener(settings: Settings) -> None:
    """Authenticate with Telegram and print every new configured-channel message."""
    client = TelegramClient(settings.session_name, settings.api_id, settings.api_hash)
    await client.start()  # First run interactively asks for the Telegram phone/code.

    # Resolve before registering the handler so an invalid channel fails at startup.
    channel_entities = [
        await client.get_input_entity(channel)
        for channel in settings.channels
    ]
    print(f"Listening for new messages in: {', '.join(settings.channels)}", flush=True)
    print("Press Ctrl+C to stop.", flush=True)

    @client.on(events.NewMessage(chats=channel_entities))
    async def handle_new_message(event: events.NewMessage.Event) -> None:
        """Current sink: stdout. Replace/extend this with Kafka publishing later."""
        message = event.message
        chat = await event.get_chat()
        channel_name = getattr(chat, "username", None) or getattr(chat, "title", "unknown")
        text = message.message or "[non-text message]"
        print(
            f"[{message.date.isoformat()}] channel={channel_name} message_id={message.id}\n{text}\n",
            flush=True,
        )

    await client.run_until_disconnected()


def main() -> None:
    try:
        asyncio.run(run_listener(load_settings()))
    except (ValueError, KeyboardInterrupt) as error:
        print(f"Listener stopped: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
