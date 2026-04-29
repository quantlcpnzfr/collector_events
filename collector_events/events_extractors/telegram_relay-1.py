"""Telegram MTProto relay that publishes OSINT events to RabbitMQ.

This process keeps one Telethon session, listens to many Telegram channels, and
publishes normalized intel events to the shared MQ topic exchange via
``MQFactory.create_async_from_env()``.

Run:
    python -m collector_events.events_extractors.telegram_relay

Required environment:
    TELEGRAM_API_ID
    TELEGRAM_API_HASH

Optional environment:
    TELEGRAM_SESSION                 Telethon StringSession; if absent, uses a file session.
    TELEGRAM_SESSION_FILE            Defaults to .state/telegram_worldmonitor(.session)
    TELEGRAM_CHANNEL_SET             Defaults to "full"; use "all" for every configured channel.
    TELEGRAM_RELAY_BUFFER_SIZE       Defaults to 200.
    TELEGRAM_MAX_TEXT_CHARS          Defaults to 800.
    TELEGRAM_BACKFILL_INTERVAL       Defaults to 60 seconds; set 0 to disable periodic backfill.
    TELEGRAM_RESOLVE_CONCURRENCY     Defaults to 8.
    TELEGRAM_BACKFILL_CONCURRENCY    Defaults to 8.
    TELEGRAM_INGEST_WORKERS          Defaults to 16.
    TELEGRAM_PROCESSING_THREADS      Defaults to 4.
    TELEGRAM_PUBLISH_WORKERS         Defaults to 16.
    TELEGRAM_EVENT_TOPIC             Optional fixed topic; otherwise uses intel.events.<mapped-domain>.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from collections import deque
from collections.abc import Awaitable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from forex_shared.domain.intel import IntelDomain, IntelItem
from forex_shared.logging.get_logger import get_logger, setup_logging
from forex_shared.worker_api import MQEventPublisher
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

setup_logging()
log = get_logger(__name__)


async def _await_if_needed(result: Any) -> Any:
    if hasattr(result, "__await__"):
        return await cast(Awaitable[Any], result)
    return result


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _float_env(name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(os.getenv(name, str(default))))
    except ValueError:
        return default


SERVICE_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CHANNELS_FILE = PACKAGE_ROOT / "sources" / "telegram_channels.json"
DEFAULT_STATE_DIR = SERVICE_ROOT / ".state"
DEFAULT_SESSION_FILE = DEFAULT_STATE_DIR / "telegram_worldmonitor"
DEFAULT_CURSOR_FILE = DEFAULT_STATE_DIR / "telegram_cursor.json"
DEFAULT_HEALTH_TOPIC = "intel.health"


def _intel_events_topic(domain: str) -> str:
    return f"intel.events.{domain}"


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


@dataclass(frozen=True)
class TelegramInbound:
    message: Any
    channel: TelegramChannel
    source: str


class TelegramRelay:
    """High-throughput Telegram relay backed by internal queues and MQ."""

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
        resolve_concurrency: int = 8,
        backfill_concurrency: int = 8,
        ingest_workers: int = 16,
        processing_threads: int = 4,
        ingest_queue_maxsize: int = 20_000,
        queue_put_timeout_seconds: float = 2.0,
        event_topic: str = "",
        health_topic: str = DEFAULT_HEALTH_TOPIC,
        health_interval_seconds: float = 30.0,
        cursor_flush_interval_seconds: float = 30.0,
        publish_workers: int = 16,
        publish_queue_maxsize: int = 50_000,
        publish_retry_attempts: int = 5,
        publish_timeout_seconds: float = 10.0,
        restart_delay_seconds: float = 10.0,
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
        self.resolve_concurrency = max(1, resolve_concurrency)
        self.backfill_concurrency = max(1, backfill_concurrency)
        self.ingest_workers = max(1, ingest_workers)
        self.queue_put_timeout_seconds = queue_put_timeout_seconds
        self.event_topic = event_topic.strip()
        self.health_topic = health_topic
        self.health_interval_seconds = health_interval_seconds
        self.cursor_flush_interval_seconds = cursor_flush_interval_seconds
        self.restart_delay_seconds = restart_delay_seconds

        self.channels: list[TelegramChannel] = []
        self.client: TelegramClient | None = None
        self.entities: dict[str, Any] = {}
        self.items: deque[TelegramFeedItem] = deque(maxlen=buffer_size)
        self.item_ids: set[str] = set()
        self.cursor_by_handle: dict[str, int] = {}

        self.lock = asyncio.Lock()
        self._flood_lock = asyncio.Lock()
        self._flood_wait_until = 0.0
        self._cursor_dirty = False
        self._stop_event = asyncio.Event()
        self._shutdown_event = asyncio.Event()

        self.inbound_queue: asyncio.Queue[TelegramInbound] = asyncio.Queue(maxsize=ingest_queue_maxsize)
        self.publisher = MQEventPublisher(
            worker_count=publish_workers,
            queue_maxsize=publish_queue_maxsize,
            retry_attempts=publish_retry_attempts,
            publish_timeout_seconds=publish_timeout_seconds,
        )
        self._processing_executor = ThreadPoolExecutor(
            max_workers=max(1, processing_threads),
            thread_name_prefix="telegram-relay-cpu",
        )

        self.ingest_tasks: list[asyncio.Task[None]] = []
        self.backfill_task: asyncio.Task[None] | None = None
        self.health_task: asyncio.Task[None] | None = None
        self.cursor_task: asyncio.Task[None] | None = None
        self.started_at = datetime.now(timezone.utc)
        self.last_poll_at: datetime | None = None
        self.last_update_at: datetime | None = None
        self.last_error: str = ""
        self.enabled = False

        self.received_count = 0
        self.accepted_count = 0
        self.duplicate_count = 0
        self.dropped_count = 0
        self.enqueued_publish_count = 0
        self.processing_error_count = 0

    def _record_error(self, context: str, exc: BaseException, *, level: str = "exception") -> None:
        """Record an error without letting error handling raise a second error."""

        try:
            self.processing_error_count += 1
            self.last_error = f"{context}: {exc}"
            if level == "warning":
                log.warning("Telegram relay %s: %s", context, exc, exc_info=True)
            else:
                log.exception("Telegram relay %s failed", context)
        except Exception:
            pass

    @classmethod
    def from_env(cls) -> "TelegramRelay":
        try:
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
                buffer_size=_int_env("TELEGRAM_RELAY_BUFFER_SIZE", 200, minimum=50),
                max_text_chars=_int_env("TELEGRAM_MAX_TEXT_CHARS", 800, minimum=200),
                backfill_interval_seconds=_float_env("TELEGRAM_BACKFILL_INTERVAL", 60.0),
                per_channel_delay_seconds=_float_env("TELEGRAM_RATE_LIMIT_SECONDS", 0.8),
                request_timeout_seconds=_float_env("TELEGRAM_REQUEST_TIMEOUT_SECONDS", 15.0, minimum=5.0),
                resolve_concurrency=_int_env("TELEGRAM_RESOLVE_CONCURRENCY", 8, minimum=1),
                backfill_concurrency=_int_env("TELEGRAM_BACKFILL_CONCURRENCY", 8, minimum=1),
                ingest_workers=_int_env("TELEGRAM_INGEST_WORKERS", 16, minimum=1),
                processing_threads=_int_env("TELEGRAM_PROCESSING_THREADS", 4, minimum=1),
                ingest_queue_maxsize=_int_env("TELEGRAM_INGEST_QUEUE_MAXSIZE", 20_000, minimum=100),
                queue_put_timeout_seconds=_float_env("TELEGRAM_QUEUE_PUT_TIMEOUT_SECONDS", 2.0, minimum=0.1),
                event_topic=os.getenv("TELEGRAM_EVENT_TOPIC", "").strip(),
                health_topic=os.getenv("TELEGRAM_HEALTH_TOPIC", DEFAULT_HEALTH_TOPIC).strip(),
                health_interval_seconds=_float_env("TELEGRAM_HEALTH_INTERVAL", 30.0, minimum=5.0),
                cursor_flush_interval_seconds=_float_env("TELEGRAM_CURSOR_FLUSH_INTERVAL", 30.0, minimum=5.0),
                publish_workers=_int_env("TELEGRAM_PUBLISH_WORKERS", 16, minimum=1),
                publish_queue_maxsize=_int_env("TELEGRAM_PUBLISH_QUEUE_MAXSIZE", 50_000, minimum=100),
                publish_retry_attempts=_int_env("TELEGRAM_PUBLISH_RETRY_ATTEMPTS", 5, minimum=1),
                publish_timeout_seconds=_float_env("TELEGRAM_PUBLISH_TIMEOUT_SECONDS", 10.0, minimum=1.0),
                restart_delay_seconds=_float_env("TELEGRAM_RESTART_DELAY_SECONDS", 10.0, minimum=1.0),
            )
        except Exception as exc:
            log.exception("TelegramRelay.from_env failed")
            raise RuntimeError(f"TelegramRelay.from_env failed: {exc}") from exc

    async def start(self) -> None:
        try:
            self._stop_event.clear()
            DEFAULT_STATE_DIR.mkdir(parents=True, exist_ok=True)
            self.session_file.parent.mkdir(parents=True, exist_ok=True)
            self.cursor_file.parent.mkdir(parents=True, exist_ok=True)

            self.channels = self._load_channels()
            self.cursor_by_handle = self._load_cursor()

            await self.publisher.start()
            self._start_ingest_workers()
            self.health_task = self._create_task(self._health_loop(), "telegram-health-loop")
            self.cursor_task = self._create_task(self._cursor_loop(), "telegram-cursor-loop")

            if self.session_string:
                session = StringSession(self.session_string)
                log.info("Telegram relay using TELEGRAM_SESSION StringSession")
            else:
                session = str(self.session_file)
                log.info("Telegram relay using file session: %s", self.session_file)

            self.client = TelegramClient(session, self.api_id, self.api_hash, connection_retries=3)
            await _await_if_needed(self.client.start())
            log.info("Telegram relay connected; resolving %d channels", len(self.channels))

            await self._resolve_entities()
            self._register_realtime_handlers()
            self.enabled = True
            await self._publish_health()

            await self.backfill_once()

            if self.backfill_interval_seconds > 0:
                self.backfill_task = self._create_task(self._backfill_loop(), "telegram-backfill-loop")
            log.info("Telegram MQ relay started: %d/%d entities active", len(self.entities), len(self.channels))
        except asyncio.CancelledError:
            self.request_stop()
            await self.stop()
            raise
        except Exception as exc:
            self._record_error("start", exc)
            await self.stop()
            raise

    async def stop(self) -> None:
        try:
            self.enabled = False
            self._stop_event.set()

            for task in [self.backfill_task, self.health_task, self.cursor_task]:
                if task:
                    task.cancel()
            try:
                await asyncio.gather(
                    *[task for task in [self.backfill_task, self.health_task, self.cursor_task] if task],
                    return_exceptions=True,
                )
            except Exception as exc:
                self._record_error("stop.cancel_background_tasks", exc, level="warning")
            self.backfill_task = None
            self.health_task = None
            self.cursor_task = None

            try:
                await asyncio.wait_for(self.inbound_queue.join(), timeout=15.0)
            except asyncio.TimeoutError:
                log.warning("Telegram relay stopped with %d inbound messages queued", self.inbound_queue.qsize())
            except Exception as exc:
                self._record_error("stop.drain_inbound_queue", exc, level="warning")

            for task in self.ingest_tasks:
                task.cancel()
            if self.ingest_tasks:
                try:
                    await asyncio.gather(*self.ingest_tasks, return_exceptions=True)
                except Exception as exc:
                    self._record_error("stop.cancel_ingest_tasks", exc, level="warning")
            self.ingest_tasks.clear()

            if self._cursor_dirty:
                try:
                    await asyncio.to_thread(self._save_cursor)
                    self._cursor_dirty = False
                except Exception as exc:
                    self._record_error("stop.save_cursor", exc, level="warning")

            if self.client:
                try:
                    await _await_if_needed(self.client.disconnect())
                except Exception as exc:
                    self._record_error("stop.disconnect_telegram", exc, level="warning")
                finally:
                    self.client = None

            try:
                await self.publisher.stop(drain_timeout=20.0)
            except Exception as exc:
                self._record_error("stop.publisher", exc, level="warning")
            log.info("Telegram relay stopped")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_error("stop", exc, level="warning")

    async def run_forever(self) -> None:
        try:
            self._shutdown_event.clear()
            while not self._shutdown_event.is_set():
                try:
                    await self.start()
                    await self._shutdown_event.wait()
                except asyncio.CancelledError:
                    self.request_stop()
                    raise
                except Exception as exc:
                    self._record_error("run_forever.supervisor", exc)
                    log.warning("Telegram relay supervisor restarting in %.1fs", self.restart_delay_seconds)
                    if not self._shutdown_event.is_set():
                        await asyncio.sleep(self.restart_delay_seconds)
                finally:
                    await self.stop()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_error("run_forever", exc)

    def request_stop(self) -> None:
        try:
            self._shutdown_event.set()
            self._stop_event.set()
            self.publisher.request_stop()
        except Exception as exc:
            self._record_error("request_stop", exc, level="warning")

    async def feed(self, *, limit: int, topic: str = "", channel: str = "") -> dict[str, Any]:
        try:
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
        except Exception as exc:
            self._record_error("feed", exc, level="warning")
            return {
                "source": "telegram",
                "earlySignal": True,
                "enabled": self.enabled,
                "count": 0,
                "updatedAt": None,
                "items": [],
                "messages": [],
                "lastError": self.last_error,
            }

    def health(self) -> dict[str, Any]:
        try:
            publisher_health = self.publisher.health()
        except Exception as exc:
            self._record_error("health.publisher", exc, level="warning")
            publisher_health = {"started": False, "last_error": str(exc)}
        try:
            return {
                "enabled": self.enabled,
                "channelsConfigured": len(self.channels),
                "entitiesResolved": len(self.entities),
                "bufferSize": len(self.items),
                "cursorCount": len(self.cursor_by_handle),
                "inboundQueueSize": self.inbound_queue.qsize(),
                "received": self.received_count,
                "accepted": self.accepted_count,
                "duplicates": self.duplicate_count,
                "dropped": self.dropped_count,
                "publishEnqueued": self.enqueued_publish_count,
                "processingErrors": self.processing_error_count,
                "publisher": publisher_health,
                "startedAt": self.started_at.isoformat(),
                "lastPollAt": self.last_poll_at.isoformat() if self.last_poll_at else None,
                "lastUpdateAt": self.last_update_at.isoformat() if self.last_update_at else None,
                "lastError": self.last_error,
            }
        except Exception as exc:
            self._record_error("health", exc, level="warning")
            return {
                "enabled": False,
                "channelsConfigured": 0,
                "entitiesResolved": 0,
                "bufferSize": 0,
                "cursorCount": 0,
                "inboundQueueSize": 0,
                "received": self.received_count,
                "accepted": self.accepted_count,
                "duplicates": self.duplicate_count,
                "dropped": self.dropped_count,
                "publishEnqueued": self.enqueued_publish_count,
                "processingErrors": self.processing_error_count,
                "publisher": publisher_health,
                "startedAt": "",
                "lastPollAt": None,
                "lastUpdateAt": None,
                "lastError": self.last_error,
            }

    def channel_index(self) -> list[dict[str, Any]]:
        try:
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
        except Exception as exc:
            self._record_error("channel_index", exc, level="warning")
            return []

    async def backfill_once(self) -> None:
        try:
            if not self.client:
                return

            sem = asyncio.Semaphore(self.backfill_concurrency)

            async def _run(channel: TelegramChannel, index: int) -> int:
                async with sem:
                    if self._stop_event.is_set():
                        return 0
                    if index:
                        await asyncio.sleep(min(self.per_channel_delay_seconds, 1.0) * (index % self.backfill_concurrency))
                    return await self._backfill_channel(channel)

            tasks = [
                self._create_task(_run(channel, index), f"telegram-backfill-{channel.handle}")
                for index, channel in enumerate(self.channels)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            enqueued = 0
            for result in results:
                if isinstance(result, Exception):
                    self._record_error("backfill_once.task", result, level="warning")
                else:
                    enqueued += int(result)

            self.last_poll_at = datetime.now(timezone.utc)
            if self._cursor_dirty:
                try:
                    await asyncio.to_thread(self._save_cursor)
                    self._cursor_dirty = False
                except Exception as exc:
                    self._record_error("backfill_once.save_cursor", exc, level="warning")
            log.info("Telegram backfill complete: %d messages enqueued", enqueued)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_error("backfill_once", exc, level="warning")

    async def _backfill_channel(self, channel: TelegramChannel) -> int:
        if not self.client:
            return 0
        entity = self.entities.get(channel.handle)
        if not entity:
            return 0

        try:
            await self._respect_flood_wait()
            min_id = self.cursor_by_handle.get(channel.handle, 0)
            raw_messages = await asyncio.wait_for(
                self.client.get_messages(
                    entity,
                    limit=max(1, min(50, channel.max_messages)),
                    min_id=min_id,
                ),
                timeout=self.request_timeout_seconds,
            )
            messages = self._normalize_messages(raw_messages)
            count = 0
            for message in reversed(messages):
                if await self._enqueue_inbound(message, channel, source="backfill"):
                    count += 1
            await asyncio.sleep(self.per_channel_delay_seconds)
            return count
        except FloodWaitError as exc:
            await self._set_flood_wait(exc.seconds)
            self.last_error = f"FLOOD_WAIT {exc.seconds}s"
            log.warning("Telegram FLOOD_WAIT %ss; pausing backfill calls", exc.seconds)
            return 0
        except asyncio.TimeoutError:
            self.last_error = f"timeout fetching {channel.handle}"
            log.warning("Telegram timeout fetching %s", channel.handle)
            return 0
        except Exception as exc:
            self.last_error = f"{channel.handle}: {exc}"
            log.warning("Telegram backfill failed for %s: %s", channel.handle, exc, exc_info=True)
            return 0

    async def _backfill_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.backfill_interval_seconds)
            except asyncio.TimeoutError:
                pass
            if not self._stop_event.is_set():
                try:
                    await self.backfill_once()
                except Exception as exc:
                    self.last_error = f"backfill loop: {exc}"
                    log.exception("Telegram backfill loop error")

    async def _health_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._publish_health()
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.health_interval_seconds)
            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                self.last_error = f"health loop: {exc}"
                log.warning("Telegram health loop failed: %s", exc, exc_info=True)

    async def _cursor_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.cursor_flush_interval_seconds)
            except asyncio.TimeoutError:
                pass
            if self._cursor_dirty:
                try:
                    await asyncio.to_thread(self._save_cursor)
                    self._cursor_dirty = False
                except Exception as exc:
                    self.last_error = f"cursor loop: {exc}"
                    log.warning("Telegram cursor save failed: %s", exc, exc_info=True)

    def _start_ingest_workers(self) -> None:
        if self.ingest_tasks:
            return
        self.ingest_tasks = [
            self._create_task(self._ingest_loop(index), f"telegram-ingest-{index}")
            for index in range(self.ingest_workers)
        ]

    async def _ingest_loop(self, index: int) -> None:
        while not self._stop_event.is_set() or not self.inbound_queue.empty():
            inbound: TelegramInbound | None = None
            try:
                inbound = await asyncio.wait_for(self.inbound_queue.get(), timeout=1.0)
                await self._process_inbound(inbound)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.processing_error_count += 1
                self.last_error = f"ingest {index}: {exc}"
                log.exception("Telegram ingest worker failed")
            finally:
                if inbound is not None:
                    self.inbound_queue.task_done()

    async def _process_inbound(self, inbound: TelegramInbound) -> None:
        try:
            loop = asyncio.get_running_loop()
            item = await loop.run_in_executor(
                self._processing_executor,
                self._build_item,
                inbound.message,
                inbound.channel,
            )
            if item is None:
                return

            is_new = await self._remember_item(item, inbound.channel)
            if not is_new:
                return

            payload = self._item_to_mq_payload(item, inbound.channel, inbound.source)
            topic = self._topic_for_channel(inbound.channel)
            if await self.publisher.publish(topic, payload):
                self.enqueued_publish_count += 1
            else:
                self.dropped_count += 1

            self.last_update_at = datetime.now(timezone.utc)
            log.info("Telegram %s queued: @%s #%s topic=%s", inbound.source, inbound.channel.handle, item.id, topic)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_error("_process_inbound", exc, level="warning")

    async def _enqueue_inbound(self, message: Any, channel: TelegramChannel, *, source: str) -> bool:
        try:
            await asyncio.wait_for(
                self.inbound_queue.put(TelegramInbound(message=message, channel=channel, source=source)),
                timeout=self.queue_put_timeout_seconds,
            )
            self.received_count += 1
            return True
        except asyncio.TimeoutError:
            self.dropped_count += 1
            self.last_error = "inbound queue full"
            log.warning("Telegram inbound queue full; dropping @%s message", channel.handle)
            return False
        except Exception as exc:
            self.dropped_count += 1
            self.last_error = f"enqueue inbound: {exc}"
            log.warning("Telegram enqueue failed for @%s: %s", channel.handle, exc, exc_info=True)
            return False

    def _build_item(self, message: Any, channel: TelegramChannel) -> TelegramFeedItem | None:
        try:
            if not message or not getattr(message, "id", None):
                return None
            text = str(getattr(message, "message", "") or "")
            if not text.strip():
                return None

            message_id = int(message.id)
            ts = self._message_timestamp(message)
            return TelegramFeedItem(
                id=f"{channel.handle}:{message_id}",
                source="telegram",
                channel=channel.handle,
                channelTitle=channel.label,
                url=f"https://t.me/{channel.handle}/{message_id}",
                ts=ts,
                date=ts,
                timestamp=ts,
                text=text[: self.max_text_chars],
                topic=channel.topic,
                tags=[tag for tag in ["telegram", channel.topic, channel.region] if tag],
                earlySignal=True,
                views=int(getattr(message, "views", 0) or 0),
                forwards=int(getattr(message, "forwards", 0) or 0),
            )
        except Exception as exc:
            self._record_error("_build_item", exc, level="warning")
            return None

    async def _remember_item(self, item: TelegramFeedItem, channel: TelegramChannel) -> bool:
        try:
            message_id = int(item.id.rsplit(":", 1)[-1])
            async with self.lock:
                if item.id in self.item_ids:
                    self.duplicate_count += 1
                    if message_id > self.cursor_by_handle.get(channel.handle, 0):
                        self.cursor_by_handle[channel.handle] = message_id
                        self._cursor_dirty = True
                    return False

                self.items.appendleft(item)
                self.item_ids.add(item.id)
                while len(self.item_ids) > self.buffer_size:
                    self.item_ids = {x.id for x in self.items}
                if message_id > self.cursor_by_handle.get(channel.handle, 0):
                    self.cursor_by_handle[channel.handle] = message_id
                    self._cursor_dirty = True

            self.accepted_count += 1
            return True
        except Exception as exc:
            self._record_error("_remember_item", exc, level="warning")
            return False

    def _item_to_mq_payload(self, item: TelegramFeedItem, channel: TelegramChannel, source: str) -> dict[str, Any]:
        try:
            intel_item = IntelItem(
                id=f"telegram:{item.id}",
                source="telegram",
                domain=self._domain_for_channel(channel),
                title=item.text[:300],
                url=item.url,
                body=item.text,
                published_at=item.ts,
                ts=item.ts,
                fetched_at=datetime.now(timezone.utc).isoformat(),
                source_media="telegram",
                severity="",
                tags=item.tags,
                extra={
                    "relay_source": source,
                    "channel": item.channel,
                    "channel_title": item.channelTitle,
                    "channel_topic": item.topic,
                    "region": channel.region,
                    "tier": channel.tier,
                    "views": item.views,
                    "forwards": item.forwards,
                    "early_signal": item.earlySignal,
                },
            )
            payload = intel_item.to_mq_payload()
            payload["raw"] = asdict(item)
            return payload
        except Exception as exc:
            self._record_error("_item_to_mq_payload", exc, level="warning")
            return {
                "event_type": "INTEL_ITEM",
                "id": f"telegram:{item.id}",
                "source": "telegram",
                "domain": self._domain_for_channel(channel),
                "title": item.text[:300],
                "url": item.url,
                "body": item.text,
                "published_at": item.ts,
                "ts": item.ts,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "tags": item.tags,
                "extra": {"relay_source": source, "channel": item.channel},
                "raw": asdict(item),
            }

    async def _publish_health(self) -> None:
        try:
            payload = {
                "event_type": "TELEGRAM_RELAY_HEALTH",
                "source": "telegram",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                **self.health(),
            }
            await self.publisher.publish(self.health_topic, payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_error("_publish_health", exc, level="warning")

    def _topic_for_channel(self, channel: TelegramChannel) -> str:
        try:
            if self.event_topic:
                return self.event_topic
            return _intel_events_topic(self._domain_for_channel(channel))
        except Exception as exc:
            self._record_error("_topic_for_channel", exc, level="warning")
            return _intel_events_topic(IntelDomain.SOCIAL)

    @staticmethod
    def _domain_for_channel(channel: TelegramChannel) -> str:
        try:
            topic = str(channel.topic).lower().strip()
            if topic == IntelDomain.CYBER:
                return IntelDomain.CYBER
            if topic in {"conflict", "geopolitics", "middleeast", "breaking", "osint"}:
                return IntelDomain.CONFLICT
            if topic in {"economic", "economics", "macro"}:
                return IntelDomain.ECONOMIC
            if topic in {"market", "markets"}:
                return IntelDomain.MARKET
        except Exception:
            return IntelDomain.SOCIAL
        return IntelDomain.SOCIAL

    def _load_channels(self) -> list[TelegramChannel]:
        try:
            raw = json.loads(self.channels_file.read_text(encoding="utf-8"))
            raw_channels = raw.get("channels", [])
            channels: list[TelegramChannel] = []
            for item in raw_channels:
                try:
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
                except Exception as exc:
                    self._record_error("_load_channels.item", exc, level="warning")
            if not channels:
                self.last_error = f"No Telegram channels loaded from {self.channels_file}"
                log.warning(self.last_error)
            return channels
        except Exception as exc:
            self._record_error("_load_channels", exc, level="warning")
            return []

    def _load_cursor(self) -> dict[str, int]:
        if not self.cursor_file.exists():
            return {}
        try:
            raw = json.loads(self.cursor_file.read_text(encoding="utf-8"))
            return {str(k): int(v) for k, v in raw.items()}
        except Exception as exc:
            self._record_error("_load_cursor", exc, level="warning")
            return {}

    def _save_cursor(self) -> None:
        try:
            tmp = self.cursor_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.cursor_by_handle, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(self.cursor_file)
        except Exception as exc:
            self._record_error("_save_cursor", exc, level="warning")

    async def _resolve_entities(self) -> None:
        try:
            if not self.client:
                return

            sem = asyncio.Semaphore(self.resolve_concurrency)

            async def _resolve(channel: TelegramChannel, index: int) -> None:
                async with sem:
                    if self._stop_event.is_set():
                        return
                    if index:
                        await asyncio.sleep(min(self.per_channel_delay_seconds, 1.0) * (index % self.resolve_concurrency))
                    try:
                        await self._respect_flood_wait()
                        entity = await asyncio.wait_for(
                            self.client.get_entity(channel.handle),
                            timeout=self.request_timeout_seconds,
                        )
                        self.entities[channel.handle] = entity
                        await asyncio.sleep(self.per_channel_delay_seconds)
                    except FloodWaitError as exc:
                        await self._set_flood_wait(exc.seconds)
                        self.last_error = f"FLOOD_WAIT {exc.seconds}s during resolve"
                        log.warning("Telegram FLOOD_WAIT %ss while resolving", exc.seconds)
                    except Exception as exc:
                        self.last_error = f"resolve {channel.handle}: {exc}"
                        log.warning("Could not resolve Telegram channel %s: %s", channel.handle, exc, exc_info=True)

            tasks = [
                self._create_task(_resolve(channel, index), f"telegram-resolve-{channel.handle}")
                for index, channel in enumerate(self.channels)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_error("_resolve_entities", exc, level="warning")

    def _register_realtime_handlers(self) -> None:
        try:
            if not self.client:
                return
            for channel in self.channels:
                try:
                    entity = self.entities.get(channel.handle)
                    if not entity:
                        continue

                    async def handler(event: events.NewMessage.Event, channel: TelegramChannel = channel) -> None:
                        try:
                            await self._enqueue_inbound(event.message, channel, source="update")
                        except Exception as exc:
                            self._record_error(f"realtime handler {channel.handle}", exc, level="warning")

                    self.client.add_event_handler(handler, events.NewMessage(chats=entity))
                except Exception as exc:
                    self._record_error(f"_register_realtime_handlers.{channel.handle}", exc, level="warning")
        except Exception as exc:
            self._record_error("_register_realtime_handlers", exc, level="warning")

    async def _respect_flood_wait(self) -> None:
        try:
            while True:
                async with self._flood_lock:
                    remaining = self._flood_wait_until - asyncio.get_running_loop().time()
                if remaining <= 0:
                    return
                await asyncio.sleep(min(remaining, 30.0))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_error("_respect_flood_wait", exc, level="warning")

    async def _set_flood_wait(self, seconds: int | float) -> None:
        try:
            async with self._flood_lock:
                until = asyncio.get_running_loop().time() + max(1.0, float(seconds))
                self._flood_wait_until = max(self._flood_wait_until, until)
        except Exception as exc:
            self._record_error("_set_flood_wait", exc, level="warning")

    @staticmethod
    def _normalize_messages(raw_messages: Any) -> list[Any]:
        try:
            if raw_messages is None:
                return []
            if hasattr(raw_messages, "__iter__"):
                return list(cast(Iterable[Any], raw_messages))
            return [raw_messages]
        except Exception:
            return []

    @staticmethod
    def _message_timestamp(message: Any) -> str:
        try:
            value = getattr(message, "date", None)
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone.utc)
                return value.astimezone(timezone.utc).isoformat()
            return datetime.now(timezone.utc).isoformat()
        except Exception:
            return datetime.now(timezone.utc).isoformat()

    def _create_task(self, coro: Awaitable[Any], name: str) -> asyncio.Task[Any]:
        try:
            task = asyncio.create_task(coro, name=name)
            task.add_done_callback(self._log_task_result)
            return task
        except Exception as exc:
            self._record_error(f"_create_task.{name}", exc, level="warning")
            raise

    def _log_task_result(self, task: asyncio.Task[Any]) -> None:
        try:
            if task.cancelled():
                return
            try:
                exc = task.exception()
            except asyncio.CancelledError:
                return
            if exc:
                self.processing_error_count += 1
                self.last_error = f"{task.get_name()}: {exc}"
                log.exception(
                    "Telegram background task failed: %s",
                    task.get_name(),
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
        except Exception as exc:
            self._record_error("_log_task_result", exc, level="warning")


async def _run_from_cli(args: argparse.Namespace) -> None:
    relay: TelegramRelay | None = None
    try:
        relay = TelegramRelay.from_env()

        loop = asyncio.get_running_loop()

        def _request_stop() -> None:
            try:
                log.info("Shutdown requested")
                if relay is not None:
                    relay.request_stop()
            except Exception as exc:
                log.warning("Shutdown request handler failed: %s", exc, exc_info=True)

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_stop)
            except (NotImplementedError, RuntimeError):
                try:
                    signal.signal(sig, lambda *_: loop.call_soon_threadsafe(_request_stop))
                except Exception as exc:
                    log.warning("Could not register signal handler %s: %s", sig, exc)

        if args.run_once:
            await relay.start()
            try:
                await asyncio.wait_for(relay.inbound_queue.join(), timeout=args.drain_timeout)
            except asyncio.TimeoutError:
                log.warning("Run-once drain timed out after %.1fs", args.drain_timeout)
            finally:
                await relay.stop()
            return

        await relay.run_forever()
    except asyncio.CancelledError:
        if relay is not None:
            relay.request_stop()
            await relay.stop()
        raise
    except Exception as exc:
        log.exception("Telegram relay CLI runner failed")
        if relay is not None:
            try:
                relay.request_stop()
                await relay.stop()
            except Exception as stop_exc:
                log.warning("Telegram relay cleanup after CLI failure failed: %s", stop_exc, exc_info=True)
        raise RuntimeError(f"Telegram relay CLI runner failed: {exc}") from exc


def main() -> None:
    try:
        parser = argparse.ArgumentParser(description="Run the collector_events Telegram MQ relay")
        parser.add_argument(
            "--run-once",
            action="store_true",
            help="Start, run initial backfill, drain queues, publish and exit.",
        )
        parser.add_argument(
            "--drain-timeout",
            type=float,
            default=60.0,
            help="Max seconds to wait for queues to drain in --run-once mode.",
        )
        args = parser.parse_args()
        asyncio.run(_run_from_cli(args))
    except KeyboardInterrupt:
        log.info("Interrupted by user")
        sys.exit(0)
    except Exception as exc:
        log.exception("Telegram relay process handled fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
