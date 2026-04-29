"""Telegram MTProto relay for WorldMonitor-style OSINT feeds.

This module runs one Telegram client session, keeps a rolling in-memory feed,
and exposes it through a small FastAPI HTTP API:

    python -m collector_events.events_extractors.telegram_relay --port 8010

Required environment:
    TELEGRAM_API_ID
    TELEGRAM_API_HASH

Optional environment:
    TELEGRAM_SESSION              Telethon StringSession; if absent, uses a file session.
    TELEGRAM_SESSION_FILE         Defaults to .state/telegram_worldmonitor(.session)
    TELEGRAM_CHANNEL_SET          Defaults to "full"; use "all" for every configured channel.
    TELEGRAM_RELAY_BUFFER_SIZE    Defaults to 200.
    TELEGRAM_MAX_TEXT_CHARS       Defaults to 800.
    TELEGRAM_BACKFILL_INTERVAL    Defaults to 60 seconds; set 0 to disable periodic backfill.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from forex_shared.logging.get_logger import get_logger, setup_logging
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

setup_logging()
log = get_logger(__name__)


SERVICE_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CHANNELS_FILE = PACKAGE_ROOT / "sources" / "telegram_channels.json"
DEFAULT_STATE_DIR = SERVICE_ROOT / ".state"
DEFAULT_SESSION_FILE = DEFAULT_STATE_DIR / "telegram_worldmonitor"
DEFAULT_CURSOR_FILE = DEFAULT_STATE_DIR / "telegram_cursor.json"


@dataclass(frozen=True)
class TelegramChannel:
    handle: str
    label: str
    topic: str = "other"
    region: str = ""
    tier: int | None = None
    channel_set: str = "full"
    max_messages: int = 25


@dataclass(frozen=True)
class TelegramFeedItem:
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
    tags: list[str]
    earlySignal: bool
    views: int
    forwards: int


class TelegramRelay:
    """Single-session Telegram relay with realtime updates and light backfill."""

    def __init__(
        self,
        *,
        api_id: int,
        api_hash: str,
        channels_file: Path = DEFAULT_CHANNELS_FILE,
        channel_set: str = "full",
        session_string: str = "",
        session_file: Path = DEFAULT_SESSION_FILE,
        cursor_file: Path = DEFAULT_CURSOR_FILE,
        buffer_size: int = 200,
        max_text_chars: int = 800,
        backfill_interval_seconds: float = 60.0,
        per_channel_delay_seconds: float = 0.8,
        request_timeout_seconds: float = 15.0,
    ) -> None:
        self.api_id = api_id
        self.api_hash = api_hash
        self.channels_file = channels_file
        self.channel_set = channel_set.lower().strip() or "full"
        self.session_string = session_string
        self.session_file = session_file
        self.cursor_file = cursor_file
        self.buffer_size = buffer_size
        self.max_text_chars = max_text_chars
        self.backfill_interval_seconds = backfill_interval_seconds
        self.per_channel_delay_seconds = per_channel_delay_seconds
        self.request_timeout_seconds = request_timeout_seconds

        self.channels: list[TelegramChannel] = []
        self.client: TelegramClient | None = None
        self.entities: dict[str, Any] = {}
        self.items: deque[TelegramFeedItem] = deque(maxlen=buffer_size)
        self.item_ids: set[str] = set()
        self.cursor_by_handle: dict[str, int] = {}
        self.lock = asyncio.Lock()
        self.backfill_task: asyncio.Task[None] | None = None
        self.started_at = datetime.now(timezone.utc)
        self.last_poll_at: datetime | None = None
        self.last_update_at: datetime | None = None
        self.last_error: str = ""
        self.enabled = False

    @classmethod
    def from_env(cls) -> "TelegramRelay":
        api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
        api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
        if not api_id_raw or not api_hash:
            raise RuntimeError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required")

        channels_file = Path(os.getenv("TELEGRAM_CHANNELS_FILE", str(DEFAULT_CHANNELS_FILE)))
        session_file = Path(os.getenv("TELEGRAM_SESSION_FILE", str(DEFAULT_SESSION_FILE)))
        cursor_file = Path(os.getenv("TELEGRAM_CURSOR_FILE", str(DEFAULT_CURSOR_FILE)))

        return cls(
            api_id=int(api_id_raw),
            api_hash=api_hash,
            channels_file=channels_file,
            channel_set=os.getenv("TELEGRAM_CHANNEL_SET", "full"),
            session_string=os.getenv("TELEGRAM_SESSION", "").strip(),
            session_file=session_file,
            cursor_file=cursor_file,
            buffer_size=max(50, int(os.getenv("TELEGRAM_RELAY_BUFFER_SIZE", "200"))),
            max_text_chars=max(200, int(os.getenv("TELEGRAM_MAX_TEXT_CHARS", "800"))),
            backfill_interval_seconds=max(0.0, float(os.getenv("TELEGRAM_BACKFILL_INTERVAL", "60"))),
            per_channel_delay_seconds=max(0.1, float(os.getenv("TELEGRAM_RATE_LIMIT_SECONDS", "0.8"))),
            request_timeout_seconds=max(5.0, float(os.getenv("TELEGRAM_REQUEST_TIMEOUT_SECONDS", "15"))),
        )

    async def start(self) -> None:
        DEFAULT_STATE_DIR.mkdir(parents=True, exist_ok=True)
        self.session_file.parent.mkdir(parents=True, exist_ok=True)
        self.cursor_file.parent.mkdir(parents=True, exist_ok=True)

        self.channels = self._load_channels()
        self.cursor_by_handle = self._load_cursor()

        if self.session_string:
            session = StringSession(self.session_string)
            log.info("Telegram relay using TELEGRAM_SESSION StringSession")
        else:
            session = str(self.session_file)
            log.info("Telegram relay using file session: %s", self.session_file)

        self.client = TelegramClient(session, self.api_id, self.api_hash, connection_retries=3)
        await self.client.start()
        self.enabled = True
        log.info("Telegram relay connected; resolving %d channels", len(self.channels))

        await self._resolve_entities()
        self._register_realtime_handlers()
        await self.backfill_once()

        if self.backfill_interval_seconds > 0:
            self.backfill_task = asyncio.create_task(self._backfill_loop(), name="telegram-backfill-loop")
        log.info("Telegram relay started: %d entities active", len(self.entities))

    async def stop(self) -> None:
        if self.backfill_task:
            self.backfill_task.cancel()
            try:
                await self.backfill_task
            except asyncio.CancelledError:
                pass
            self.backfill_task = None
        self._save_cursor()
        if self.client:
            await self.client.disconnect()
            self.client = None
        self.enabled = False
        log.info("Telegram relay stopped")

    async def feed(self, *, limit: int, topic: str = "", channel: str = "") -> dict[str, Any]:
        topic = topic.lower().strip()
        channel = channel.lower().strip().removeprefix("@")
        async with self.lock:
            items = [asdict(item) for item in self.items]

        if topic:
            items = [item for item in items if str(item.get("topic", "")).lower() == topic]
        if channel:
            items = [item for item in items if str(item.get("channel", "")).lower() == channel]

        sliced = items[:limit]
        updated_at = self.last_update_at or self.last_poll_at
        return {
            "source": "telegram",
            "earlySignal": True,
            "enabled": self.enabled,
            "count": len(sliced),
            "updatedAt": updated_at.isoformat() if updated_at else None,
            "items": sliced,
            "messages": sliced,
            "lastError": self.last_error,
        }

    def health(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "channelsConfigured": len(self.channels),
            "entitiesResolved": len(self.entities),
            "bufferSize": len(self.items),
            "cursorCount": len(self.cursor_by_handle),
            "startedAt": self.started_at.isoformat(),
            "lastPollAt": self.last_poll_at.isoformat() if self.last_poll_at else None,
            "lastUpdateAt": self.last_update_at.isoformat() if self.last_update_at else None,
            "lastError": self.last_error,
        }

    def channel_index(self) -> list[dict[str, Any]]:
        return [
            {
                "handle": c.handle,
                "label": c.label,
                "topic": c.topic,
                "region": c.region,
                "tier": c.tier,
                "channelSet": c.channel_set,
                "maxMessages": c.max_messages,
                "resolved": c.handle in self.entities,
            }
            for c in self.channels
        ]

    async def backfill_once(self) -> None:
        if not self.client:
            return
        new_count = 0
        for channel in self.channels:
            entity = self.entities.get(channel.handle)
            if not entity:
                continue
            try:
                min_id = self.cursor_by_handle.get(channel.handle, 0)
                messages = await asyncio.wait_for(
                    self.client.get_messages(
                        entity,
                        limit=max(1, min(50, channel.max_messages)),
                        min_id=min_id,
                    ),
                    timeout=self.request_timeout_seconds,
                )
                for message in reversed(messages):
                    if await self._add_message(message, channel, source="backfill"):
                        new_count += 1
                await asyncio.sleep(self.per_channel_delay_seconds)
            except FloodWaitError as exc:
                self.last_error = f"FLOOD_WAIT {exc.seconds}s"
                log.warning("Telegram FLOOD_WAIT %ss; pausing backfill", exc.seconds)
                await asyncio.sleep(exc.seconds)
                break
            except asyncio.TimeoutError:
                self.last_error = f"timeout fetching {channel.handle}"
                log.warning("Telegram timeout fetching %s", channel.handle)
            except Exception as exc:
                self.last_error = f"{channel.handle}: {exc}"
                log.warning("Telegram backfill failed for %s: %s", channel.handle, exc)

        self.last_poll_at = datetime.now(timezone.utc)
        if new_count:
            self._save_cursor()
        log.info("Telegram backfill complete: %d new messages", new_count)

    async def _backfill_loop(self) -> None:
        while True:
            await asyncio.sleep(self.backfill_interval_seconds)
            await self.backfill_once()

    def _load_channels(self) -> list[TelegramChannel]:
        raw = json.loads(self.channels_file.read_text(encoding="utf-8"))
        raw_channels = raw.get("channels", [])
        channels: list[TelegramChannel] = []
        for item in raw_channels:
            channel_set = str(item.get("channelSet") or item.get("channel_set") or "full").lower()
            if self.channel_set != "all" and channel_set != self.channel_set:
                continue
            if item.get("enabled") is False:
                continue
            handle = str(item.get("handle", "")).strip().removeprefix("@")
            if not handle:
                continue
            channels.append(
                TelegramChannel(
                    handle=handle,
                    label=str(item.get("label") or handle),
                    topic=str(item.get("topic") or "other"),
                    region=str(item.get("region") or ""),
                    tier=item.get("tier"),
                    channel_set=channel_set,
                    max_messages=int(item.get("maxMessages") or 25),
                )
            )
        if not channels:
            raise RuntimeError(f"No Telegram channels loaded from {self.channels_file}")
        return channels

    def _load_cursor(self) -> dict[str, int]:
        if not self.cursor_file.exists():
            return {}
        try:
            raw = json.loads(self.cursor_file.read_text(encoding="utf-8"))
            return {str(k): int(v) for k, v in raw.items()}
        except Exception as exc:
            log.warning("Could not load Telegram cursor file %s: %s", self.cursor_file, exc)
            return {}

    def _save_cursor(self) -> None:
        try:
            tmp = self.cursor_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.cursor_by_handle, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(self.cursor_file)
        except Exception as exc:
            log.warning("Could not save Telegram cursor file %s: %s", self.cursor_file, exc)

    async def _resolve_entities(self) -> None:
        if not self.client:
            return
        for channel in self.channels:
            try:
                entity = await asyncio.wait_for(
                    self.client.get_entity(channel.handle),
                    timeout=self.request_timeout_seconds,
                )
                self.entities[channel.handle] = entity
                await asyncio.sleep(self.per_channel_delay_seconds)
            except FloodWaitError as exc:
                self.last_error = f"FLOOD_WAIT {exc.seconds}s during resolve"
                log.warning("Telegram FLOOD_WAIT %ss while resolving; sleeping", exc.seconds)
                await asyncio.sleep(exc.seconds)
            except Exception as exc:
                self.last_error = f"resolve {channel.handle}: {exc}"
                log.warning("Could not resolve Telegram channel %s: %s", channel.handle, exc)

    def _register_realtime_handlers(self) -> None:
        if not self.client:
            return
        for channel in self.channels:
            entity = self.entities.get(channel.handle)
            if not entity:
                continue

            async def handler(event: events.NewMessage.Event, channel: TelegramChannel = channel) -> None:
                await self._add_message(event.message, channel, source="update")

            self.client.add_event_handler(handler, events.NewMessage(chats=entity))

    async def _add_message(self, message: Any, channel: TelegramChannel, *, source: str) -> bool:
        if not message or not getattr(message, "id", None):
            return False
        text = str(getattr(message, "message", "") or "")
        if not text.strip():
            return False

        message_id = int(message.id)
        item_id = f"{channel.handle}:{message_id}"
        ts = self._message_timestamp(message)
        item = TelegramFeedItem(
            id=item_id,
            source="telegram",
            channel=channel.handle,
            channelTitle=channel.label,
            url=f"https://t.me/{channel.handle}/{message_id}",
            ts=ts,
            date=ts,
            timestamp=ts,
            text=text[: self.max_text_chars],
            topic=channel.topic,
            tags=[tag for tag in [channel.region] if tag],
            earlySignal=True,
            views=int(getattr(message, "views", 0) or 0),
            forwards=int(getattr(message, "forwards", 0) or 0),
        )

        async with self.lock:
            if item_id in self.item_ids:
                if message_id > self.cursor_by_handle.get(channel.handle, 0):
                    self.cursor_by_handle[channel.handle] = message_id
                return False

            self.items.appendleft(item)
            self.item_ids.add(item_id)
            while len(self.item_ids) > self.buffer_size:
                self.item_ids = {x.id for x in self.items}
            if message_id > self.cursor_by_handle.get(channel.handle, 0):
                self.cursor_by_handle[channel.handle] = message_id

        self.last_update_at = datetime.now(timezone.utc)
        log.info("Telegram %s: @%s #%s", source, channel.handle, message_id)
        return True

    @staticmethod
    def _message_timestamp(message: Any) -> str:
        value = getattr(message, "date", None)
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc).isoformat()
        return datetime.now(timezone.utc).isoformat()


relay: TelegramRelay | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global relay
    relay = TelegramRelay.from_env()
    await relay.start()
    try:
        yield
    finally:
        await relay.stop()


app = FastAPI(title="collector_events Telegram Relay", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    return relay.health() if relay else {"enabled": False, "lastError": "relay not started"}


@app.get("/telegram/channels")
async def telegram_channels() -> dict[str, Any]:
    channels = relay.channel_index() if relay else []
    return {"count": len(channels), "channels": channels}


@app.get("/telegram/feed")
async def telegram_feed(
    limit: int = Query(default=50, ge=1, le=200),
    topic: str = "",
    channel: str = "",
) -> JSONResponse:
    if relay is None:
        return JSONResponse({"enabled": False, "items": [], "messages": [], "count": 0}, status_code=503)
    return JSONResponse(await relay.feed(limit=limit, topic=topic, channel=channel))


@app.post("/telegram/backfill")
async def telegram_backfill() -> dict[str, Any]:
    if relay is None:
        return {"enabled": False, "error": "relay not started"}
    await relay.backfill_once()
    return relay.health()


def main() -> None:
    import uvicorn

    parser = argparse.ArgumentParser(description="Run the collector_events Telegram relay")
    parser.add_argument("--host", default=os.getenv("TELEGRAM_RELAY_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("TELEGRAM_RELAY_PORT", "8010")))
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    uvicorn.run(
        "collector_events.events_extractors.telegram_relay:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=os.getenv("TELEGRAM_RELAY_LOG_LEVEL", "info").lower(),
    )


if __name__ == "__main__":
    main()
