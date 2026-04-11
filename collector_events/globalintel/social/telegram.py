"""Telegram relay extractor."""

from __future__ import annotations

from forex_shared.logging.get_logger import get_logger
import aiohttp

from ..base import BaseExtractor, IntelItem
from ..config import _load

logger = get_logger(__name__)

_TELEGRAM_CHANNELS: list[dict] = _load("social.json")["telegram_channels"]


class TelegramRelayExtractor(BaseExtractor):
    """Fetches messages from Telegram via a Railway relay service.

    Relies on an external relay (Node.js sidecar or Railway service) that
    aggregates Telegram channel messages. Auth via shared secret.
    """

    SOURCE = "telegram"
    DOMAIN = "social"
    REDIS_KEY = "social:telegram:v1"
    TTL_SECONDS = 300

    def __init__(self, relay_url: str = "", shared_secret: str = "", limit: int = 100):
        super().__init__()
        self._relay_url = relay_url
        self._shared_secret = shared_secret
        self._limit = limit

    async def _fetch(self, session: aiohttp.ClientSession) -> list[IntelItem]:
        if not self._relay_url:
            logger.warning("Telegram relay URL not configured")
            return []

        headers = {"Authorization": f"Bearer {self._shared_secret}"} if self._shared_secret else {}
        url = f"{self._relay_url}/telegram/feed"
        params = {"limit": str(self._limit)}

        try:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status != 200:
                    logger.warning("Telegram relay returned %d", resp.status)
                    return []
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.warning("Telegram relay failed: %s", exc)
            return []

        items: list[IntelItem] = []
        messages = data if isinstance(data, list) else data.get("messages", [])
        for msg in messages:
            channel = msg.get("channel", "")
            text = msg.get("text", "")
            items.append(IntelItem(
                id=f"tg:{channel}:{msg.get('id', '')}",
                source="telegram",
                domain="social",
                title=text[:300],
                ts=msg.get("date", ""),
                tags=["telegram", channel],
                extra={
                    "channel": channel,
                    "views": msg.get("views", 0),
                    "forwards": msg.get("forwards", 0),
                },
            ))
        return items
