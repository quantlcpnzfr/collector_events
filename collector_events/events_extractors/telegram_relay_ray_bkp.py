"""Telegram MTProto relay backed by Ray actors for distributed processing.

Architecture (full-parallel, multi-process):

    Maestro process (this class: ``TelegramRelayRay``)
        - Owns the single Telethon MTProto connection (avoids AUTH_KEY_DUPLICATED)
        - Persists the StringSession in MongoDB (collection ``telegram``)
        - Maintains channel resolution, dedup set, cursor watermarks
        - Extracts plain-dict envelopes from raw Telethon messages
        - Round-robin dispatches envelopes to a pool of Ray actors

    TelegramProcessorActor (``@ray.remote`` pool, one process each)
        - Runs in its own OS process with its own asyncio loop
        - Builds normalized IntelItem payloads (CPU-bound work, no GIL contention)
        - Publishes to RabbitMQ via its own ``MQEventPublisher`` connection
        - Failures are isolated: a crashing actor is restarted by Ray, the maestro keeps running

Backpressure: an asyncio.Semaphore caps the total in-flight remote calls
(``num_actors * max_inflight_per_actor``). When saturated, the dispatch coroutine
blocks until a Ray task completes, providing natural flow control without an
internal asyncio.Queue.

Run:
    python -m collector_events.events_extractors.telegram_relay_ray

Required environment:
    TELEGRAM_API_ID
    TELEGRAM_API_HASH

Optional environment:
    TELEGRAM_SESSION                 Telethon StringSession bootstrap (one-time, when MongoDB has no record).
    TELEGRAM_SESSION_KEY             MongoDB document _id for the persisted StringSession (default "default").
    TELEGRAM_CHANNEL_SET             "full" (default) or "all".
    TELEGRAM_RELAY_BUFFER_SIZE       Local feed buffer size (default 200).
    TELEGRAM_MAX_TEXT_CHARS          Max characters per message (default 800).
    TELEGRAM_BACKFILL_INTERVAL       Periodic backfill seconds (default 60; 0 disables).
    TELEGRAM_RESOLVE_CONCURRENCY     Channel resolve concurrency (default 8).
    TELEGRAM_BACKFILL_CONCURRENCY    Backfill concurrency (default 8).
    TELEGRAM_EVENT_TOPIC             Optional fixed topic; otherwise intel.events.<mapped-domain>.

    # Ray-specific:
    TELEGRAM_RAY_ACTORS              Number of processor actors (default 4).
    TELEGRAM_RAY_ACTOR_CONCURRENCY   Per-actor max_concurrency (default 16).
    TELEGRAM_RAY_INFLIGHT_PER_ACTOR  Backpressure cap per actor (default 200).
    TELEGRAM_RAY_ADDRESS             Ray cluster address (default: spawn local).
    TELEGRAM_RAY_NAMESPACE           Ray namespace (default "telegram_relay").

    # Per-actor publisher:
    TELEGRAM_PUBLISH_WORKERS         MQ publish workers per actor (default 8).
    TELEGRAM_PUBLISH_QUEUE_MAXSIZE   MQ queue size per actor (default 50_000).
    TELEGRAM_PUBLISH_RETRY_ATTEMPTS  MQ publish retries (default 5).
    TELEGRAM_PUBLISH_TIMEOUT_SECONDS MQ publish timeout (default 10.0).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from collections import deque
from collections.abc import Awaitable, Coroutine, Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, cast

import ray

from forex_shared.domain.intel import IntelDomain, IntelItem
from forex_shared.logging.get_logger import get_logger, setup_logging
from forex_shared.mongo_manager import MongoManager
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
DEFAULT_CURSOR_FILE = DEFAULT_STATE_DIR / "telegram_cursor.json"
DEFAULT_HEALTH_TOPIC = "intel.health"
TELEGRAM_SESSION_COLLECTION = "telegram"
DEFAULT_SESSION_KEY = "default"


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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TelegramChannel":
        return cls(
            handle=str(data.get("handle") or ""),
            label=str(data.get("label") or ""),
            topic=str(data.get("topic") or "other"),
            region=str(data.get("region") or ""),
            tier=data.get("tier"),
            channel_set=str(data.get("channel_set") or "full"),
            max_messages=int(data.get("max_messages") or 25),
        )


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
class _MessageEnvelope:
    """Plain-dict-serializable extract of a Telethon message for Ray dispatch."""

    id: int
    text: str
    ts: str
    views: int
    forwards: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


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


def _build_intel_payload(item: TelegramFeedItem, channel: TelegramChannel, source: str) -> dict[str, Any]:
    intel_item = IntelItem(
        id=f"telegram:{item.id}",
        source="telegram",
        domain=_domain_for_channel(channel),
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


# ──────────────────────────────────────────────────────────────────────────────
# Ray actor: per-process worker that normalizes envelopes and publishes to MQ
# ──────────────────────────────────────────────────────────────────────────────


@ray.remote(max_restarts=5, max_task_retries=3)
class TelegramProcessorActor:
    """Ray actor that turns raw envelopes into MQ events.

    Each actor lives in its own process with its own asyncio loop and its own
    ``MQEventPublisher`` connection to RabbitMQ. CPU-bound normalization work
    runs in parallel across actors without GIL contention.
    """

    def __init__(
        self,
        *,
        actor_id: str,
        max_text_chars: int,
        event_topic: str,
        publish_workers: int,
        publish_queue_maxsize: int,
        publish_retry_attempts: int,
        publish_timeout_seconds: float,
    ) -> None:
        setup_logging()
        self._log = get_logger(f"{__name__}.actor.{actor_id}")

        self.actor_id = actor_id
        self.max_text_chars = max_text_chars
        self.event_topic = event_topic.strip()
        self._publisher_kwargs: dict[str, Any] = {
            "worker_count": publish_workers,
            "queue_maxsize": publish_queue_maxsize,
            "retry_attempts": publish_retry_attempts,
            "publish_timeout_seconds": publish_timeout_seconds,
        }
        self._publisher: Any = None

        self.received = 0
        self.accepted = 0
        self.dropped = 0
        self.errors = 0
        self.last_error = ""

    async def start(self) -> None:
        if self._publisher is not None:
            return
        from forex_shared.worker_api import MQEventPublisher

        self._publisher = MQEventPublisher(**self._publisher_kwargs)
        await self._publisher.start()
        self._log.info("Telegram processor actor %s ready", self.actor_id)

    async def stop(self, drain_timeout: float = 10.0) -> None:
        publisher = self._publisher
        self._publisher = None
        if publisher is None:
            return
        try:
            await publisher.stop(drain_timeout=drain_timeout)
        except Exception as exc:
            self.last_error = f"stop: {exc}"
            self._log.warning("Actor %s stop failed: %s", self.actor_id, exc, exc_info=True)

    async def process(
        self,
        envelope: dict[str, Any],
        channel: dict[str, Any],
        source: str,
    ) -> dict[str, Any]:
        self.received += 1
        try:
            if self._publisher is None:
                self.dropped += 1
                self.last_error = "publisher not started"
                return {"accepted": False, "reason": self.last_error}

            ch = TelegramChannel.from_dict(channel)
            text = str(envelope.get("text") or "")
            if not text.strip():
                self.dropped += 1
                return {"accepted": False, "reason": "empty text"}

            message_id = int(envelope.get("id", 0) or 0)
            if message_id <= 0:
                self.dropped += 1
                return {"accepted": False, "reason": "invalid id"}

            ts = str(envelope.get("ts") or datetime.now(timezone.utc).isoformat())
            views = int(envelope.get("views", 0) or 0)
            forwards = int(envelope.get("forwards", 0) or 0)

            item = TelegramFeedItem(
                id=f"{ch.handle}:{message_id}",
                source="telegram",
                channel=ch.handle,
                channelTitle=ch.label,
                url=f"https://t.me/{ch.handle}/{message_id}",
                ts=ts,
                date=ts,
                timestamp=ts,
                text=text[: self.max_text_chars],
                topic=ch.topic,
                tags=[t for t in ["telegram", ch.topic, ch.region] if t],
                earlySignal=True,
                views=views,
                forwards=forwards,
            )

            topic = self.event_topic or _intel_events_topic(_domain_for_channel(ch))
            payload = _build_intel_payload(item, ch, source)

            ok = await self._publisher.publish(topic, payload)
            if ok:
                self.accepted += 1
                return {"accepted": True, "topic": topic, "item_id": item.id}
            self.dropped += 1
            self.last_error = "publisher dropped event"
            return {"accepted": False, "reason": self.last_error}
        except Exception as exc:
            self.errors += 1
            self.last_error = f"{type(exc).__name__}: {exc}"
            self._log.exception("Actor %s process failed", self.actor_id)
            return {"accepted": False, "reason": str(exc)}

    async def publish_raw(self, topic: str, payload: dict[str, Any]) -> bool:
        """Forward an arbitrary event to MQ via this actor's publisher."""

        if self._publisher is None:
            return False
        try:
            return await self._publisher.publish(topic, payload)
        except Exception as exc:
            self.errors += 1
            self.last_error = f"publish_raw: {exc}"
            self._log.warning("Actor %s publish_raw failed: %s", self.actor_id, exc, exc_info=True)
            return False

    def health(self) -> dict[str, Any]:
        publisher_health: dict[str, Any] = {}
        try:
            if self._publisher is not None:
                publisher_health = self._publisher.health()
        except Exception as exc:
            publisher_health = {"error": str(exc)}
        return {
            "actor_id": self.actor_id,
            "received": self.received,
            "accepted": self.accepted,
            "dropped": self.dropped,
            "errors": self.errors,
            "last_error": self.last_error,
            "publisher": publisher_health,
        }


# ──────────────────────────────────────────────────────────────────────────────
# Maestro: owns Telethon, dispatches envelopes to the Ray actor pool
# ──────────────────────────────────────────────────────────────────────────────


class TelegramRelayRay:
    """High-throughput Telegram relay backed by a Ray actor pool."""

    def __init__(
        self,
        *,
        api_id: int,
        api_hash: str,
        channels_file: Path = DEFAULT_CHANNELS_FILE,
        channel_set: str = "full",
        session_string: str = "",
        session_key: str = DEFAULT_SESSION_KEY,
        cursor_file: Path = DEFAULT_CURSOR_FILE,
        buffer_size: int = 200,
        max_text_chars: int = 800,
        backfill_interval_seconds: float = 60.0,
        per_channel_delay_seconds: float = 0.8,
        request_timeout_seconds: float = 15.0,
        resolve_concurrency: int = 8,
        backfill_concurrency: int = 8,
        event_topic: str = "",
        health_topic: str = DEFAULT_HEALTH_TOPIC,
        health_interval_seconds: float = 30.0,
        cursor_flush_interval_seconds: float = 30.0,
        restart_delay_seconds: float = 10.0,
        # Ray:
        num_actors: int = 4,
        actor_max_concurrency: int = 16,
        max_inflight_per_actor: int = 200,
        ray_address: str = "",
        ray_namespace: str = "telegram_relay",
        # Per-actor publisher:
        publish_workers: int = 8,
        publish_queue_maxsize: int = 50_000,
        publish_retry_attempts: int = 5,
        publish_timeout_seconds: float = 10.0,
        on_message: Callable[["TelegramFeedItem", str], None] | None = None,
    ) -> None:
        self.api_id = api_id
        self.api_hash = api_hash
        self.channels_file = channels_file
        self.channel_set = channel_set.lower().strip() or "full"
        self.session_string = session_string
        self.session_key = (session_key or DEFAULT_SESSION_KEY).strip() or DEFAULT_SESSION_KEY
        self.cursor_file = cursor_file
        self.buffer_size = buffer_size
        self.max_text_chars = max_text_chars
        self.backfill_interval_seconds = backfill_interval_seconds
        self.per_channel_delay_seconds = per_channel_delay_seconds
        self.request_timeout_seconds = request_timeout_seconds
        self.resolve_concurrency = max(1, resolve_concurrency)
        self.backfill_concurrency = max(1, backfill_concurrency)
        self.event_topic = event_topic.strip()
        self.health_topic = health_topic.strip() or DEFAULT_HEALTH_TOPIC
        self.health_interval_seconds = health_interval_seconds
        self.cursor_flush_interval_seconds = cursor_flush_interval_seconds
        self.restart_delay_seconds = restart_delay_seconds

        self.num_actors = max(1, num_actors)
        self.actor_max_concurrency = max(1, actor_max_concurrency)
        self.max_inflight_per_actor = max(1, max_inflight_per_actor)
        self.ray_address = ray_address.strip()
        self.ray_namespace = ray_namespace.strip() or "telegram_relay"
        self.publish_workers = max(1, publish_workers)
        self.publish_queue_maxsize = max(100, publish_queue_maxsize)
        self.publish_retry_attempts = max(1, publish_retry_attempts)
        self.publish_timeout_seconds = max(1.0, publish_timeout_seconds)
        self.on_message: Callable[["TelegramFeedItem", str], None] | None = on_message

        # State
        self._mongo: MongoManager | None = None
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

        # Ray
        self._actors: list[Any] = []
        self._actor_rr = 0
        self._owns_ray = False
        self._inflight_sem: asyncio.Semaphore | None = None
        self._completion_tasks: set[asyncio.Task[None]] = set()

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
        self.dispatched_count = 0
        self.actor_error_count = 0

    # ──────────────────────────────────────────────────────────
    # Construction from environment
    # ──────────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> "TelegramRelayRay":
        try:
            api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
            api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
            if not api_id_raw or not api_hash:
                raise RuntimeError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required")

            channels_file = Path(os.getenv("TELEGRAM_CHANNELS_FILE", str(DEFAULT_CHANNELS_FILE)))
            cursor_file = Path(os.getenv("TELEGRAM_CURSOR_FILE", str(DEFAULT_CURSOR_FILE)))

            return cls(
                api_id=int(api_id_raw),
                api_hash=api_hash,
                channels_file=channels_file,
                channel_set=os.getenv("TELEGRAM_CHANNEL_SET", "full"),
                session_string=os.getenv("TELEGRAM_SESSION", "").strip(),
                session_key=os.getenv("TELEGRAM_SESSION_KEY", DEFAULT_SESSION_KEY).strip() or DEFAULT_SESSION_KEY,
                cursor_file=cursor_file,
                buffer_size=_int_env("TELEGRAM_RELAY_BUFFER_SIZE", 200, minimum=50),
                max_text_chars=_int_env("TELEGRAM_MAX_TEXT_CHARS", 800, minimum=200),
                backfill_interval_seconds=_float_env("TELEGRAM_BACKFILL_INTERVAL", 60.0),
                per_channel_delay_seconds=_float_env("TELEGRAM_RATE_LIMIT_SECONDS", 0.8),
                request_timeout_seconds=_float_env("TELEGRAM_REQUEST_TIMEOUT_SECONDS", 15.0, minimum=5.0),
                resolve_concurrency=_int_env("TELEGRAM_RESOLVE_CONCURRENCY", 8, minimum=1),
                backfill_concurrency=_int_env("TELEGRAM_BACKFILL_CONCURRENCY", 8, minimum=1),
                event_topic=os.getenv("TELEGRAM_EVENT_TOPIC", "").strip(),
                health_topic=os.getenv("TELEGRAM_HEALTH_TOPIC", DEFAULT_HEALTH_TOPIC).strip(),
                health_interval_seconds=_float_env("TELEGRAM_HEALTH_INTERVAL", 30.0, minimum=5.0),
                cursor_flush_interval_seconds=_float_env("TELEGRAM_CURSOR_FLUSH_INTERVAL", 30.0, minimum=5.0),
                restart_delay_seconds=_float_env("TELEGRAM_RESTART_DELAY_SECONDS", 10.0, minimum=1.0),
                num_actors=_int_env("TELEGRAM_RAY_ACTORS", 4, minimum=1),
                actor_max_concurrency=_int_env("TELEGRAM_RAY_ACTOR_CONCURRENCY", 16, minimum=1),
                max_inflight_per_actor=_int_env("TELEGRAM_RAY_INFLIGHT_PER_ACTOR", 200, minimum=10),
                ray_address=os.getenv("TELEGRAM_RAY_ADDRESS", "").strip(),
                ray_namespace=os.getenv("TELEGRAM_RAY_NAMESPACE", "telegram_relay").strip() or "telegram_relay",
                publish_workers=_int_env("TELEGRAM_PUBLISH_WORKERS", 8, minimum=1),
                publish_queue_maxsize=_int_env("TELEGRAM_PUBLISH_QUEUE_MAXSIZE", 50_000, minimum=100),
                publish_retry_attempts=_int_env("TELEGRAM_PUBLISH_RETRY_ATTEMPTS", 5, minimum=1),
                publish_timeout_seconds=_float_env("TELEGRAM_PUBLISH_TIMEOUT_SECONDS", 10.0, minimum=1.0),
            )
        except Exception as exc:
            log.exception("TelegramRelayRay.from_env failed")
            raise RuntimeError(f"TelegramRelayRay.from_env failed: {exc}") from exc

    def _record_error(self, context: str, exc: BaseException, *, level: str = "exception") -> None:
        try:
            self.last_error = f"{context}: {exc}"
            if level == "warning":
                log.warning("Telegram relay %s: %s", context, exc, exc_info=True)
            else:
                log.exception("Telegram relay %s failed", context)
        except Exception:
            pass

    # ──────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        try:
            self._stop_event.clear()
            DEFAULT_STATE_DIR.mkdir(parents=True, exist_ok=True)
            self.cursor_file.parent.mkdir(parents=True, exist_ok=True)

            self.channels = self._load_channels()
            self.cursor_by_handle = self._load_cursor()

            await self._init_ray()
            await self._spawn_actors()

            self.health_task = self._create_task(self._health_loop(), "telegram-health-loop")
            self.cursor_task = self._create_task(self._cursor_loop(), "telegram-cursor-loop")

            await self._ensure_mongo()
            session_string = await self._load_session_from_mongo()
            if session_string:
                log.info(
                    "Telegram relay using StringSession from MongoDB (collection=%s, _id=%s)",
                    TELEGRAM_SESSION_COLLECTION,
                    self.session_key,
                )
            elif self.session_string:
                session_string = self.session_string
                log.info(
                    "Telegram relay using StringSession from TELEGRAM_SESSION env "
                    "(bootstrap; will persist to MongoDB after connect)"
                )
            else:
                log.warning(
                    "Telegram relay: no StringSession in MongoDB (collection=%s, _id=%s) "
                    "and TELEGRAM_SESSION not set; Telethon will require interactive auth",
                    TELEGRAM_SESSION_COLLECTION,
                    self.session_key,
                )

            self.client = TelegramClient(
                StringSession(session_string),
                self.api_id,
                self.api_hash,
                connection_retries=3,
            )
            await _await_if_needed(self.client.start())
            await self._save_session_to_mongo()
            log.info("Telegram relay connected; resolving %d channels", len(self.channels))

            await self._resolve_entities()
            self._register_realtime_handlers()
            self.enabled = True
            await self._publish_health()

            await self.backfill_once()

            if self.backfill_interval_seconds > 0:
                self.backfill_task = self._create_task(self._backfill_loop(), "telegram-backfill-loop")
            log.info(
                "Telegram Ray relay started: %d/%d entities active, %d actors, max_inflight=%d",
                len(self.entities),
                len(self.channels),
                len(self._actors),
                (self._inflight_sem._value if self._inflight_sem else 0),
            )
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

            await self._drain_inflight(timeout=20.0)

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

            await self._stop_actors()
            await self._shutdown_ray()
            log.info("Telegram Ray relay stopped")
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
                    log.warning("Telegram Ray relay supervisor restarting in %.1fs", self.restart_delay_seconds)
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
        except Exception as exc:
            self._record_error("request_stop", exc, level="warning")

    # ──────────────────────────────────────────────────────────
    # Ray cluster + actor pool
    # ──────────────────────────────────────────────────────────

    async def _init_ray(self) -> None:
        if ray.is_initialized():
            log.info("Ray already initialized; reusing existing runtime")
            return

        def _do_init() -> None:
            init_kwargs: dict[str, Any] = {
                "ignore_reinit_error": True,
                "logging_level": logging.WARNING,
                "namespace": self.ray_namespace,
            }
            if self.ray_address:
                init_kwargs["address"] = self.ray_address
            ray.init(**init_kwargs)

        await asyncio.to_thread(_do_init)
        self._owns_ray = True
        log.info(
            "Ray initialized (address=%s, namespace=%s)",
            self.ray_address or "local",
            self.ray_namespace,
        )

    async def _shutdown_ray(self) -> None:
        if not self._owns_ray:
            return
        try:
            await asyncio.to_thread(ray.shutdown)
            log.info("Ray runtime shut down")
        except Exception as exc:
            self._record_error("_shutdown_ray", exc, level="warning")
        finally:
            self._owns_ray = False

    async def _spawn_actors(self) -> None:
        actor_cls = TelegramProcessorActor.options(  # type: ignore[attr-defined]
            max_concurrency=self.actor_max_concurrency,
        )
        self._actors = [
            actor_cls.remote(
                actor_id=f"tg-proc-{i}",
                max_text_chars=self.max_text_chars,
                event_topic=self.event_topic,
                publish_workers=self.publish_workers,
                publish_queue_maxsize=self.publish_queue_maxsize,
                publish_retry_attempts=self.publish_retry_attempts,
                publish_timeout_seconds=self.publish_timeout_seconds,
            )
            for i in range(self.num_actors)
        ]

        await asyncio.gather(*[actor.start.remote() for actor in self._actors])

        self._inflight_sem = asyncio.Semaphore(self.num_actors * self.max_inflight_per_actor)
        log.info(
            "Spawned %d Telegram processor actors (max_concurrency=%d, inflight_cap=%d)",
            len(self._actors),
            self.actor_max_concurrency,
            self.num_actors * self.max_inflight_per_actor,
        )

    async def _stop_actors(self) -> None:
        if not self._actors:
            return
        try:
            await asyncio.gather(
                *[actor.stop.remote(15.0) for actor in self._actors],
                return_exceptions=True,
            )
        except Exception as exc:
            self._record_error("_stop_actors.drain", exc, level="warning")
        for actor in self._actors:
            try:
                ray.kill(actor, no_restart=True)
            except Exception as exc:
                self._record_error("_stop_actors.kill", exc, level="warning")
        self._actors = []

    # ──────────────────────────────────────────────────────────
    # Dispatch (replaces inbound queue + ingest workers)
    # ──────────────────────────────────────────────────────────

    async def _dispatch(self, message: Any, channel: TelegramChannel, source: str) -> bool:
        try:
            envelope = self._extract_envelope(message)
            if envelope is None:
                return False

            self.received_count += 1
            item_id = f"{channel.handle}:{envelope.id}"

            async with self.lock:
                if item_id in self.item_ids:
                    self.duplicate_count += 1
                    if envelope.id > self.cursor_by_handle.get(channel.handle, 0):
                        self.cursor_by_handle[channel.handle] = envelope.id
                        self._cursor_dirty = True
                    return False

                feed_item = TelegramFeedItem(
                    id=item_id,
                    source="telegram",
                    channel=channel.handle,
                    channelTitle=channel.label,
                    url=f"https://t.me/{channel.handle}/{envelope.id}",
                    ts=envelope.ts,
                    date=envelope.ts,
                    timestamp=envelope.ts,
                    text=envelope.text[: self.max_text_chars],
                    topic=channel.topic,
                    tags=[t for t in ["telegram", channel.topic, channel.region] if t],
                    earlySignal=True,
                    views=envelope.views,
                    forwards=envelope.forwards,
                )
                self.items.appendleft(feed_item)
                self.item_ids.add(item_id)
                while len(self.item_ids) > self.buffer_size:
                    self.item_ids = {x.id for x in self.items}
                if envelope.id > self.cursor_by_handle.get(channel.handle, 0):
                    self.cursor_by_handle[channel.handle] = envelope.id
                    self._cursor_dirty = True

            self.accepted_count += 1

            if self.on_message is not None:
                try:
                    self.on_message(feed_item, source)
                except Exception as exc:
                    self._record_error("on_message", exc, level="warning")

            if self._inflight_sem is None or not self._actors:
                self.dropped_count += 1
                self.last_error = "actor pool not ready"
                return False

            await self._inflight_sem.acquire()
            try:
                actor = self._actors[self._actor_rr % len(self._actors)]
                self._actor_rr += 1
                ref = actor.process.remote(envelope.to_dict(), channel.to_dict(), source)
                self.dispatched_count += 1
            except Exception:
                self._inflight_sem.release()
                raise

            task = asyncio.create_task(self._handle_completion(ref), name=f"telegram-completion-{item_id}")
            self._completion_tasks.add(task)
            task.add_done_callback(self._completion_tasks.discard)

            self.last_update_at = datetime.now(timezone.utc)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_error("_dispatch", exc, level="warning")
            return False

    async def _handle_completion(self, ref: Any) -> None:
        try:
            result = await ref
            if not isinstance(result, dict) or not result.get("accepted"):
                self.dropped_count += 1
                if isinstance(result, dict):
                    reason = result.get("reason", "unknown")
                    self.last_error = f"actor: {reason}"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.actor_error_count += 1
            self.last_error = f"actor task: {exc}"
            log.warning("Telegram actor task failed: %s", exc, exc_info=True)
        finally:
            if self._inflight_sem is not None:
                try:
                    self._inflight_sem.release()
                except ValueError:
                    pass

    async def _drain_inflight(self, timeout: float = 20.0) -> None:
        if not self._completion_tasks:
            return
        pending = list(self._completion_tasks)
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            log.warning("Telegram drain timed out with %d in-flight Ray tasks", len(pending))
            for task in pending:
                if not task.done():
                    task.cancel()
        except Exception as exc:
            self._record_error("_drain_inflight", exc, level="warning")

    def _extract_envelope(self, message: Any) -> _MessageEnvelope | None:
        try:
            if not message or not getattr(message, "id", None):
                return None
            text = str(getattr(message, "message", "") or "")
            if not text.strip():
                return None
            return _MessageEnvelope(
                id=int(message.id),
                text=text,
                ts=self._message_timestamp(message),
                views=int(getattr(message, "views", 0) or 0),
                forwards=int(getattr(message, "forwards", 0) or 0),
            )
        except Exception as exc:
            self._record_error("_extract_envelope", exc, level="warning")
            return None

    # ──────────────────────────────────────────────────────────
    # Backfill / realtime / Telethon plumbing
    # ──────────────────────────────────────────────────────────

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
                        await asyncio.sleep(
                            min(self.per_channel_delay_seconds, 1.0) * (index % self.backfill_concurrency)
                        )
                    return await self._backfill_channel(channel)

            tasks = [
                self._create_task(_run(channel, index), f"telegram-backfill-{channel.handle}")
                for index, channel in enumerate(self.channels)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            dispatched = 0
            for result in results:
                if isinstance(result, BaseException):
                    self._record_error("backfill_once.task", result, level="warning")
                else:
                    dispatched += int(result)

            self.last_poll_at = datetime.now(timezone.utc)
            if self._cursor_dirty:
                try:
                    await asyncio.to_thread(self._save_cursor)
                    self._cursor_dirty = False
                except Exception as exc:
                    self._record_error("backfill_once.save_cursor", exc, level="warning")
            log.info("Telegram backfill complete: %d messages dispatched", dispatched)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_error("backfill_once", exc, level="warning")

    async def _backfill_channel(self, channel: TelegramChannel) -> int:
        client = self.client
        if client is None:
            return 0
        entity = self.entities.get(channel.handle)
        if not entity:
            return 0

        try:
            await self._respect_flood_wait()
            min_id = self.cursor_by_handle.get(channel.handle, 0)
            raw_messages = await asyncio.wait_for(
                client.get_messages(
                    entity,
                    limit=max(1, min(50, channel.max_messages)),
                    min_id=min_id,
                ),
                timeout=self.request_timeout_seconds,
            )
            messages = self._normalize_messages(raw_messages)
            count = 0
            for message in reversed(messages):
                if await self._dispatch(message, channel, source="backfill"):
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

    async def _resolve_entities(self) -> None:
        try:
            client = self.client
            if client is None:
                return

            sem = asyncio.Semaphore(self.resolve_concurrency)

            async def _resolve(channel: TelegramChannel, index: int) -> None:
                async with sem:
                    if self._stop_event.is_set():
                        return
                    if index:
                        await asyncio.sleep(
                            min(self.per_channel_delay_seconds, 1.0) * (index % self.resolve_concurrency)
                        )
                    try:
                        await self._respect_flood_wait()
                        entity = await asyncio.wait_for(
                            client.get_entity(channel.handle),
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
                        log.warning(
                            "Could not resolve Telegram channel %s: %s",
                            channel.handle,
                            exc,
                            exc_info=True,
                        )

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
            client = self.client
            if client is None:
                return
            for channel in self.channels:
                try:
                    entity = self.entities.get(channel.handle)
                    if not entity:
                        continue

                    async def handler(event: events.NewMessage.Event, channel: TelegramChannel = channel) -> None:
                        try:
                            await self._dispatch(event.message, channel, source="update")
                        except Exception as exc:
                            self._record_error(f"realtime handler {channel.handle}", exc, level="warning")

                    client.add_event_handler(handler, events.NewMessage(chats=entity))
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

    # ──────────────────────────────────────────────────────────
    # MongoDB session persistence
    # ──────────────────────────────────────────────────────────

    async def _ensure_mongo(self) -> None:
        if self._mongo is not None:
            return
        try:
            self._mongo = await asyncio.to_thread(MongoManager)
        except Exception as exc:
            self._mongo = None
            self._record_error("_ensure_mongo", exc, level="warning")

    async def _load_session_from_mongo(self) -> str:
        if self._mongo is None:
            return ""
        try:
            results = await self._mongo.async_find_many(
                TELEGRAM_SESSION_COLLECTION,
                filter_={"_id": self.session_key},
                limit=1,
            )
            if not results:
                return ""
            return str(results[0].get("session_string", "") or "")
        except Exception as exc:
            self._record_error("_load_session_from_mongo", exc, level="warning")
            return ""

    async def _save_session_to_mongo(self) -> None:
        if self._mongo is None or self.client is None:
            return
        try:
            session_obj = self.client.session
            if not isinstance(session_obj, StringSession):
                log.warning(
                    "Telegram session is not a StringSession (got %s); skipping MongoDB persist",
                    type(session_obj).__name__,
                )
                return
            session_str = session_obj.save()
            if not session_str:
                return
            doc = {
                "_id": self.session_key,
                "session_string": session_str,
                "api_id": self.api_id,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            await self._mongo.async_replace_one(
                TELEGRAM_SESSION_COLLECTION,
                {"_id": self.session_key},
                doc,
                upsert=True,
            )
            log.info(
                "Telegram StringSession persisted to MongoDB (collection=%s, _id=%s)",
                TELEGRAM_SESSION_COLLECTION,
                self.session_key,
            )
        except Exception as exc:
            self._record_error("_save_session_to_mongo", exc, level="warning")

    # ──────────────────────────────────────────────────────────
    # Channels / cursor file
    # ──────────────────────────────────────────────────────────

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

    # ──────────────────────────────────────────────────────────
    # Health (publishes via one of the actors)
    # ──────────────────────────────────────────────────────────

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

    async def _publish_health(self) -> None:
        try:
            payload = {
                "event_type": "TELEGRAM_RELAY_HEALTH",
                "source": "telegram",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                **(await self.health()),
            }
            if not self._actors:
                return
            actor = self._actors[0]
            await actor.publish_raw.remote(self.health_topic, payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_error("_publish_health", exc, level="warning")

    async def health(self) -> dict[str, Any]:
        try:
            actor_healths: list[Any] = []
            if self._actors:
                try:
                    actor_healths = list(
                        await asyncio.gather(*[actor.health.remote() for actor in self._actors])
                    )
                except Exception as exc:
                    self._record_error("health.actor_pool", exc, level="warning")

            return {
                "enabled": self.enabled,
                "channelsConfigured": len(self.channels),
                "entitiesResolved": len(self.entities),
                "bufferSize": len(self.items),
                "cursorCount": len(self.cursor_by_handle),
                "received": self.received_count,
                "accepted": self.accepted_count,
                "duplicates": self.duplicate_count,
                "dropped": self.dropped_count,
                "dispatched": self.dispatched_count,
                "inFlight": (
                    (self.num_actors * self.max_inflight_per_actor) - self._inflight_sem._value
                    if self._inflight_sem
                    else 0
                ),
                "actorErrors": self.actor_error_count,
                "actors": actor_healths,
                "startedAt": self.started_at.isoformat(),
                "lastPollAt": self.last_poll_at.isoformat() if self.last_poll_at else None,
                "lastUpdateAt": self.last_update_at.isoformat() if self.last_update_at else None,
                "lastError": self.last_error,
            }
        except Exception as exc:
            self._record_error("health", exc, level="warning")
            return {
                "enabled": False,
                "lastError": self.last_error,
            }

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

    # ──────────────────────────────────────────────────────────
    # Misc helpers
    # ──────────────────────────────────────────────────────────

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

    def _create_task(self, coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
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
                self.last_error = f"{task.get_name()}: {exc}"
                log.exception(
                    "Telegram background task failed: %s",
                    task.get_name(),
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
        except Exception as exc:
            self._record_error("_log_task_result", exc, level="warning")


# ──────────────────────────────────────────────────────────────────────────────
# CLI entrypoint
# ──────────────────────────────────────────────────────────────────────────────


async def _run_from_cli(args: argparse.Namespace) -> None:
    relay: TelegramRelayRay | None = None
    try:
        relay = TelegramRelayRay.from_env()

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
                await relay._drain_inflight(timeout=args.drain_timeout)
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
        log.exception("Telegram Ray relay CLI runner failed")
        if relay is not None:
            try:
                relay.request_stop()
                await relay.stop()
            except Exception as stop_exc:
                log.warning("Telegram Ray relay cleanup after CLI failure failed: %s", stop_exc, exc_info=True)
        raise RuntimeError(f"Telegram Ray relay CLI runner failed: {exc}") from exc


def main() -> None:
    try:
        parser = argparse.ArgumentParser(description="Run the collector_events Telegram MQ relay (Ray-backed)")
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
        log.exception("Telegram Ray relay process handled fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
