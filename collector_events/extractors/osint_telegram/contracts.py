from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Optional, List

@dataclass(frozen=True)
class TelegramChannel:
    """Config for a single Telegram channel source."""
    handle: str
    label: str
    topic: str = "other"
    region: str = ""
    tier: Optional[int] = None
    channel_set: str = "full"
    max_messages: int = 25
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TelegramChannel:
        return cls(
            handle=str(data.get("handle") or ""),
            label=str(data.get("label") or ""),
            topic=str(data.get("topic") or "other"),
            region=str(data.get("region") or ""),
            tier=data.get("tier"),
            channel_set=str(data.get("channel_set") or data.get("channelSet") or "full"),
            max_messages=int(data.get("max_messages") or data.get("maxMessages") or 25),
            enabled=bool(data.get("enabled", True)),
        )

@dataclass(frozen=True)
class TelegramFeedItem:
    """Normalized item extracted from a Telethon message."""
    id: str
    source: str
    channel: str
    channelTitle: str
    url: str
    ts: str
    date: str
    timestamp: str
    text: str
    topic: str
    tags: List[str]
    earlySignal: bool
    views: int
    forwards: int
