from __future__ import annotations
import asyncio
import os
import json
import hashlib
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set
from pathlib import Path

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.types import InputPeerChannel, InputPeerChat, InputPeerUser
from telethon import utils

from forex_shared.worker_api import BaseSession, MQEventPublisher
from forex_shared.domain.intel import IntelDomain, IntelItem
from forex_shared.logging.get_logger import get_logger

from .contracts import TelegramChannel, TelegramFeedItem
from .store import OsintTelegramStore

log = get_logger(__name__)

class OsintTelegramSession(BaseSession):
    """
    Worker-managed session for Telegram OSINT collection.
    Handles Telethon connection, channel resolution, backfill, and realtime updates.
    """

    def __init__(self, config: Dict[str, Any], store: OsintTelegramStore):
        self.config = config
        self.store = store
        
        self.session_id = config.get("session_id", "unknown")
        self.session_name = config.get("session_name", self.session_id)
        self.status = "RUNNING"
        self.metadata = config.get("metadata", {})
        
        # Telegram API credentials
        self.api_id = int(config.get("api_id") or os.getenv("TELEGRAM_API_ID", "0"))
        self.api_hash = config.get("api_hash") or os.getenv("TELEGRAM_API_HASH", "")
        self.session_key = config.get("session_key", self.session_id)
        
        # Extraction settings
        self.channel_set = config.get("channel_set", "all")
        self.max_text_chars = config.get("max_text_chars", 800)
        self.backfill_interval = config.get("backfill_interval", 60.0)
        self.per_channel_delay = config.get("per_channel_delay", 0.8)
        self.request_timeout = config.get("request_timeout", 15.0)
        self.resolve_batch_size = int(config.get("resolve_batch_size", 8))
        self.resolve_batch_pause = float(config.get("resolve_batch_pause", 3.0))
        self.startup_channel_limit = int(config.get("startup_channel_limit", 0))
        self.priority_startup = bool(config.get("priority_startup", True))
        self.cache_hit_delay = float(config.get("cache_hit_delay", 0.05))
        self.resolve_failure_cooldown_hours = int(config.get("resolve_failure_cooldown_hours", 24))
        self.enable_content_dedupe = bool(config.get("enable_content_dedupe", True))
        self.dedupe_window_hours = int(config.get("dedupe_window_hours", 6))
        self.enable_similarity_clustering = bool(config.get("enable_similarity_clustering", True))
        self.cluster_window_hours = int(config.get("cluster_window_hours", 6))
        self.cluster_similarity_threshold = float(config.get("cluster_similarity_threshold", 0.62))
        self.max_cluster_candidates = int(config.get("max_cluster_candidates", 40))
        
        # New Parameters
        self.backfill_days = config.get("backfill_days", 1)
        self.max_messages_limit = config.get("max_messages_limit", 0)
        self.log_to_file = config.get("log_to_file", False)
        self.log_to_json = config.get("log_to_json", False)
        self.validate_db_storage = config.get("validate_db_storage", False)
        
        # Logging setup
        self.log_path = self._resolve_log_path("osint_feed.log")
        self.json_log_path = self._resolve_log_path("osint_feed_intel.json")
        
        if self.log_to_file:
            self._init_feed_log()
        if self.log_to_json:
            self._init_json_log()

        # State
        self.channels: List[TelegramChannel] = []
        self.entities: Dict[str, Any] = {}
        self.client: Optional[TelegramClient] = None
        self.publisher: Optional[MQEventPublisher] = None
        self._backfill_task: Optional[asyncio.Task] = None
        
        self._flood_wait_until = 0.0
        self._flood_lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._json_lock = asyncio.Lock()
        
        # Stats
        self.received_count = 0
        self.accepted_count = 0
        self.duplicate_count = 0
        self.content_duplicate_count = 0
        self.clustered_message_count = 0
        self.new_cluster_count = 0
        self.processed_messages_count = 0
        self.last_poll_at: Optional[datetime] = None
        self.resolved_count = 0
        self.resolution_failures = 0
        self.cache_hit_count = 0
        self.cache_miss_count = 0
        self.skipped_resolution_count = 0
        self.flood_wait_count = 0
        self.live_resolve_count = 0
        self.startup_duration_seconds = 0.0
        self.resolution_duration_seconds = 0.0

    def _resolve_log_path(self, filename: str) -> Path:
        """Resolve the log path relative to this file."""
        base_dir = Path(__file__).resolve().parent
        log_dir = base_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir / filename

    def _init_feed_log(self) -> None:
        """Initialize the feed log with a startup header."""
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
            lines = [
                "",
                "═" * 96,
                "🛰️  FOREX_SYSTEM OSINT TELEGRAM FEED DASHBOARD",
                "═" * 96,
                f"🕒 Started UTC: {now_str}",
                f"📁 Log file: {self.log_path}",
                f"🎯 Session ID: {self.session_id}",
                f"🎯 Channel set: {self.channel_set}",
                "═" * 96,
                "",
            ]
            self.log_path.write_text("\n".join(lines), encoding="utf-8")
            log.info(f"Initialized feed log at {self.log_path}")
        except Exception as e:
            log.warning(f"Failed to initialize feed log: {e}")

    def _init_json_log(self) -> None:
        """Initialize the JSON log with an empty array."""
        try:
            self.json_log_path.parent.mkdir(parents=True, exist_ok=True)
            self.json_log_path.write_text("[]", encoding="utf-8")
            log.info(f"Initialized JSON log at {self.json_log_path}")
        except Exception as e:
            log.warning(f"Failed to initialize JSON log: {e}")

    async def _write_intel_to_json(self, intel_item: IntelItem) -> None:
        """Append an IntelItem to the JSON log array."""
        if not self.log_to_json:
            return
            
        async with self._json_lock:
            try:
                # Read current
                content = "[]"
                if self.json_log_path.exists():
                    content = self.json_log_path.read_text(encoding="utf-8")
                
                try:
                    items = json.loads(content)
                except:
                    items = []
                
                items.append(intel_item.to_dict())
                
                # Write back
                self.json_log_path.write_text(
                    json.dumps(items, indent=2, ensure_ascii=False), 
                    encoding="utf-8"
                )
            except Exception as e:
                log.warning(f"Failed to write IntelItem to JSON log: {e}")

    def _write_to_feed_log(self, text: str) -> None:
        """Append a message to the feed log (legacy fallback)."""
        if not self.log_to_file:
            return
        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(text + "\n")
        except Exception as e:
            log.warning(f"Failed to write to feed log: {e}")
            
    def _write_message_to_log(self, item: TelegramFeedItem, source: str) -> None:
        """Append a human-readable message block to the dashboard log."""
        if not self.log_to_file:
            return
        
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        # Try to parse item.ts if it's ISO, otherwise use it as is
        try:
            pub_dt = datetime.fromisoformat(item.ts.replace('Z', '+00:00'))
            pub_str = pub_dt.strftime("%Y-%m-%d %H:%M")
        except:
            pub_str = item.ts
            
        text = self._compact_text(item.text, limit=self.max_text_chars)
        lines = [
            "",
            "─" * 96,
            f"🟢 #{self.processed_messages_count + 1:06d}  {now_str}  [{source.upper()}]",
            f"📣 @{item.channel} · {item.channelTitle}",
            f"🏷️  topic={item.topic or 'unknown'}  tags={', '.join(item.tags) if item.tags else '-'}",
            f"🔗 {item.url}",
            f"👀 views={item.views:,}  ↗ forwards={item.forwards:,}  🕯️ published={pub_str}",
            "💬 Message",
            self._indent_text(text),
            # Remove the line at the end of the block
        ]
        self._write_to_feed_log("\n".join(lines))

    def _compact_text(self, value: str, *, limit: int) -> str:
        import re
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if len(text) <= limit:
            return text
        return f"{text[: limit - 1].rstrip()}…"

    def _indent_text(self, value: str) -> str:
        if not value:
            return "   (empty)"
        width = 92
        words = value.split(" ")
        lines: list[str] = []
        current = ""
        for word in words:
            candidate = f"{current} {word}".strip()
            if len(candidate) > width and current:
                lines.append(f"   {current}")
                current = word
            else:
                current = candidate
        if current:
            lines.append(f"   {current}")
        return "\n".join(lines)
            
    def _create_background_task(self, coro, name: str = "") -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        return task

    async def start(self) -> None:
        """Initialize Telethon, resolve channels and start loops."""
        log.info(f"Starting OsintTelegramSession '{self.session_id}'")
        started_at = asyncio.get_event_loop().time()
        
        # 1. Load channels config
        await self._load_channels_config()
        
        # 2. Setup Publisher
        self.publisher = MQEventPublisher()
        await self.publisher.start()
        
        # 3. Setup Telethon
        session_str = await self.store.get_session(self.session_key)
        if not session_str:
            session_str = os.getenv("TELEGRAM_SESSION", "")
            
        if not session_str:
            log.warning("No Telegram session found in store or env; interactive auth may be required.")
            
        self.client = TelegramClient(
            StringSession(session_str),
            self.api_id,
            self.api_hash,
            connection_retries=3,
            flood_sleep_threshold=0
        )
        
        await self.client.connect()
        if not await self.client.is_user_authorized():
            log.error("Telegram client NOT authorized. Please run session_auth script.")
            return

        # Save session back to store in case it was updated/bootstrapped
        await self.store.save_session(self.session_key, self.client.session.save())
        
        # 4. Resolve entities
        await self._resolve_entities()
        
        # 5. Register realtime handlers
        self._register_handlers()
        
        # 6. Start backfill loop
        self._backfill_task = self._create_background_task(self._backfill_loop(), name="backfill")
        self.startup_duration_seconds = asyncio.get_event_loop().time() - started_at
        
        log.info(
            "OsintTelegramSession '%s' started with %s active channels in %.2fs",
            self.session_id,
            len(self.entities),
            self.startup_duration_seconds,
        )

    async def stop(self) -> None:
        """Gracefully stop session."""
        log.info(f"Stopping OsintTelegramSession '{self.session_id}'")
        self._stop_event.set()
        self.status = "STOPPED"
        if self._backfill_task and not self._backfill_task.done():
            self._backfill_task.cancel()
        
        if self.client:
            await self.client.disconnect()
            
        if self.publisher:
            await self.publisher.stop()

    async def _load_channels_config(self) -> None:
        """Load channels from file or config payload."""
        # In a real worker, channels might come from the config payload
        # But here we'll check if a file path was provided
        channels_file = self.config.get("channels_file")
        if channels_file and os.path.exists(channels_file):
            with open(channels_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                raw_channels = data.get("channels", [])
                for rc in raw_channels:
                    ch = TelegramChannel.from_dict(rc)
                    if self.channel_set == "all" or ch.channel_set == self.channel_set:
                        if ch.enabled:
                            self.channels.append(ch)
        
        # Also allow direct list in config
        if "channels" in self.config:
            for rc in self.config["channels"]:
                self.channels.append(TelegramChannel.from_dict(rc))

        self.channels = self._prioritize_channels(self.channels)

    def _prioritize_channels(self, channels: List[TelegramChannel]) -> List[TelegramChannel]:
        prioritized = list(channels)
        if self.priority_startup:
            priority_rank = {"p0": 0, "p1": 1, "p2": 2, "disabled": 3}
            prioritized.sort(
                key=lambda channel: (
                    priority_rank.get(channel.deployment_priority.lower(), 9),
                    channel.tier if channel.tier is not None else 999,
                    0 if channel.send_to_oracle else 1,
                    0 if channel.send_to_translator else 1,
                    channel.max_messages * -1,
                    channel.handle.lower(),
                )
            )
        if self.startup_channel_limit > 0:
            return prioritized[: self.startup_channel_limit]
        return prioritized

    def _serialize_entity_ref(self, entity: Any) -> Optional[Dict[str, Any]]:
        try:
            input_peer = utils.get_input_peer(entity)
        except Exception:
            return None

        if isinstance(input_peer, InputPeerChannel):
            return {
                "peer_type": "channel",
                "channel_id": int(input_peer.channel_id),
                "access_hash": int(input_peer.access_hash),
            }
        if isinstance(input_peer, InputPeerUser):
            return {
                "peer_type": "user",
                "user_id": int(input_peer.user_id),
                "access_hash": int(input_peer.access_hash),
            }
        if isinstance(input_peer, InputPeerChat):
            return {
                "peer_type": "chat",
                "chat_id": int(input_peer.chat_id),
            }
        return None

    def _deserialize_entity_ref(self, payload: Dict[str, Any]) -> Optional[Any]:
        peer_type = str(payload.get("peer_type") or "")
        try:
            if peer_type == "channel":
                return InputPeerChannel(
                    channel_id=int(payload["channel_id"]),
                    access_hash=int(payload["access_hash"]),
                )
            if peer_type == "user":
                return InputPeerUser(
                    user_id=int(payload["user_id"]),
                    access_hash=int(payload["access_hash"]),
                )
            if peer_type == "chat":
                return InputPeerChat(chat_id=int(payload["chat_id"]))
        except Exception:
            return None
        return None

    async def _resolve_entities(self) -> None:
        """Resolve channel handles to Telethon entities."""
        if not self.client:
            return

        resolution_started_at = asyncio.get_event_loop().time()
        live_resolution_index = 0
        for index, ch in enumerate(self.channels, start=1):
            if self._stop_event.is_set():
                break
            try:
                skip_reason = await self._should_skip_resolution(ch)
                if skip_reason:
                    self.skipped_resolution_count += 1
                    log.info("Skipping channel %s resolution: %s", ch.handle, skip_reason)
                    continue

                entity, from_cache = await self._resolve_channel_entity(ch)
                if entity is None:
                    continue
                self.entities[ch.handle] = entity
                self.resolved_count += 1
                if not from_cache:
                    self.live_resolve_count += 1
                    live_resolution_index += 1
                await self.store.save_channel_state(
                    ch.handle,
                    {
                        "lastResolvedAt": datetime.now(timezone.utc).isoformat(),
                        "resolutionFailures": 0,
                        "lastResolutionError": "",
                        "resolutionStatus": "cache_hit" if from_cache else "resolved_live",
                        "nextResolveAfter": None,
                    },
                )
                if not from_cache and live_resolution_index % self.resolve_batch_size == 0:
                    await asyncio.sleep(self.resolve_batch_pause)
                elif from_cache:
                    await asyncio.sleep(self.cache_hit_delay)
                else:
                    await asyncio.sleep(self.per_channel_delay)
            except Exception as e:
                self.resolution_failures += 1
                current_state = await self.store.get_channel_state(ch.handle)
                next_resolve_after = (
                    datetime.now(timezone.utc) + timedelta(hours=self.resolve_failure_cooldown_hours)
                ).isoformat()
                await self.store.save_channel_state(
                    ch.handle,
                    {
                        "lastResolutionError": str(e),
                        "resolutionFailures": int(current_state.get("resolutionFailures", 0)) + 1,
                        "resolutionStatus": "failed",
                        "nextResolveAfter": next_resolve_after,
                    },
                )
                log.warning(f"Failed to resolve channel {ch.handle}: {e}")

        self.resolution_duration_seconds = asyncio.get_event_loop().time() - resolution_started_at
        log.info(
            "Resolution summary: resolved=%s cache_hits=%s cache_misses=%s skipped=%s failures=%s flood_waits=%s live_resolves=%s duration=%.2fs",
            self.resolved_count,
            self.cache_hit_count,
            self.cache_miss_count,
            self.skipped_resolution_count,
            self.resolution_failures,
            self.flood_wait_count,
            self.live_resolve_count,
            self.resolution_duration_seconds,
        )

    async def _should_skip_resolution(self, channel: TelegramChannel) -> str:
        state = await self.store.get_channel_state(channel.handle)
        next_resolve_after = state.get("nextResolveAfter")
        if not next_resolve_after:
            return ""
        try:
            retry_at = datetime.fromisoformat(str(next_resolve_after).replace("Z", "+00:00"))
        except Exception:
            return ""
        if retry_at > datetime.now(timezone.utc):
            return f"cooldown until {retry_at.isoformat()}"
        return ""

    async def _resolve_channel_entity(self, channel: TelegramChannel) -> tuple[Optional[Any], bool]:
        if not self.client:
            return None, False

        cached = await self.store.get_entity_cache(channel.handle)
        if cached:
            entity = self._deserialize_entity_ref(cached)
            if entity is not None:
                self.cache_hit_count += 1
                return entity, True

        self.cache_miss_count += 1
        await self._respect_flood_wait()
        async with self._request_lock:
            try:
                entity = await asyncio.wait_for(
                    self.client.get_entity(channel.handle),
                    timeout=self.request_timeout,
                )
            except FloodWaitError as exc:
                await self._handle_flood_wait(int(exc.seconds))
                await self.store.save_channel_state(
                    channel.handle,
                    {
                            "lastFloodWaitAt": datetime.now(timezone.utc).isoformat(),
                            "lastFloodWaitSeconds": int(exc.seconds),
                        },
                    )
                raise

        payload = self._serialize_entity_ref(entity)
        if payload is not None:
            await self.store.save_entity_cache(channel.handle, payload)
        return entity, False

    def _register_handlers(self) -> None:
        """Register NewMessage handlers for all resolved channels."""
        if not self.client: return
        
        for handle, entity in self.entities.items():
            # Find the channel config
            ch = next((c for c in self.channels if c.handle == handle), None)
            if not ch: continue
            
            @self.client.on(events.NewMessage(chats=entity))
            async def handler(event, channel=ch):
                await self._process_message(event.message, channel, source="realtime")

    async def _backfill_loop(self) -> None:
        """Periodically poll channels for missed messages."""
        from datetime import timedelta
        
        while not self._stop_event.is_set():
            try:
                self.last_poll_at = datetime.now(timezone.utc)
                offset_date = self.last_poll_at - timedelta(days=self.backfill_days)
                
                for ch in self.channels:
                    if self._stop_event.is_set(): break
                    entity = self.entities.get(ch.handle)
                    if not entity: continue
                    
                    last_id = await self.store.get_cursor(ch.handle)
                    
                    await self._respect_flood_wait()
                    async with self._request_lock:
                        try:
                            # Fetch messages since last_id OR since offset_date
                            messages = await self.client.get_messages(
                                entity, 
                                limit=ch.max_messages,
                                min_id=last_id,
                                offset_date=None if last_id > 0 else offset_date,
                                reverse=True # Fetch from oldest to newest
                            )
                        except FloodWaitError as exc:
                            await self._handle_flood_wait(int(exc.seconds))
                            await self.store.save_channel_state(
                                ch.handle,
                                {
                                    "lastFloodWaitAt": datetime.now(timezone.utc).isoformat(),
                                    "lastFloodWaitSeconds": int(exc.seconds),
                                },
                            )
                            continue
                        await asyncio.sleep(self.per_channel_delay)
                        
                    if messages:
                        await self.store.save_channel_state(
                            ch.handle,
                            {
                                "lastBackfillCount": len(messages),
                                "lastSuccessAt": datetime.now(timezone.utc).isoformat(),
                            },
                        )
                        for msg in messages:
                            if self._stop_event.is_set(): break
                            await self._process_message(msg, ch, source="backfill")
                            
                await asyncio.sleep(self.backfill_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception(f"Error in backfill loop: {e}")
                await asyncio.sleep(10)

    async def _process_message(self, message: Any, channel: TelegramChannel, source: str) -> None:
        """Normalize and publish message."""
        if not message or not getattr(message, "message", None):
            return
            
        text = str(message.message).strip()
        if not text:
            return
            
        self.received_count += 1
        
        # Check cursor to avoid duplicates
        last_id = await self.store.get_cursor(channel.handle)
        if message.id <= last_id:
            self.duplicate_count += 1
            return

        ts = message.date.replace(tzinfo=timezone.utc).isoformat()
            
        # Update cursor
        await self.store.save_cursor(channel.handle, message.id)
        await self.store.save_channel_state(
            channel.handle,
            {
                "lastSuccessAt": datetime.now(timezone.utc).isoformat(),
                "lastMessageId": int(message.id),
                "lastPublishedAt": ts,
            },
        )
        
        item_id = f"{channel.handle}:{message.id}"
        item = TelegramFeedItem(
            id=item_id,
            source="telegram",
            channel=channel.handle,
            channelTitle=channel.label,
            url=f"https://t.me/{channel.handle}/{message.id}",
            ts=ts,
            date=ts,
            timestamp=ts,
            text=text[:self.max_text_chars],
            topic=channel.topic,
            tags=[t for t in ["telegram", channel.topic, channel.region] if t],
            earlySignal=True,
            views=int(getattr(message, "views", 0) or 0),
            forwards=int(getattr(message, "forwards", 0) or 0),
        )
        
        source_score = self._source_score(channel)
        domain = self._domain_for_topic(channel.topic)
        fingerprint_key = self._fingerprint_key(domain, item.text)
        fingerprint_info = await self._track_content_fingerprint(
            fingerprint_key=fingerprint_key,
            channel=channel,
            item_id=item_id,
            published_at=ts,
            domain=domain,
            source_score=source_score,
        )
        if fingerprint_info["exact_duplicate"]:
            self.duplicate_count += 1
            self.content_duplicate_count += 1
            await self.store.save_channel_state(
                channel.handle,
                {
                    "lastDuplicateAt": datetime.now(timezone.utc).isoformat(),
                    "lastDuplicateFingerprint": fingerprint_key,
                },
            )

        cluster_info = await self._upsert_similarity_cluster(
            domain=domain,
            channel=channel,
            item_id=item_id,
            published_at=ts,
            text=item.text,
            source_score=source_score,
            fingerprint_key=fingerprint_key,
            exact_duplicate=fingerprint_info["exact_duplicate"],
        )

        # Build IntelItem payload
        intel_item = IntelItem(
            id=f"telegram:{item.id}",
            source="telegram",
            domain=domain,
            title=item.text[:300],
            url=item.url,
            body=item.text,
            published_at=ts,
            ts=ts,
            fetched_at=datetime.now(timezone.utc).isoformat(),
            source_media="OSINT Telegram Feed",
            tags=item.tags,
            extra={
                "relay_source": source,
                "channel": item.channel,
                "channel_title": item.channelTitle,
                "views": item.views,
                "forwards": item.forwards,
                "source_score": source_score,
                "source_authenticity_class": channel.authenticity_class,
                "source_bias_risk": channel.bias_risk,
                "source_role": channel.source_role,
                "routing_family": channel.routing_family,
                "deployment_priority": channel.deployment_priority,
                "verification_required": channel.verification_required,
                "fingerprint_key": fingerprint_key,
                "exact_duplicate": fingerprint_info["exact_duplicate"],
                "exact_emit_count": fingerprint_info["emit_count"],
                "exact_channel_count": fingerprint_info["channel_count"],
                "cluster_id": cluster_info["cluster_id"],
                "cluster_emit_count": cluster_info["emit_count"],
                "cluster_channel_count": cluster_info["channel_count"],
                "cluster_weighted_attention": cluster_info["weighted_attention"],
                "cluster_first_seen_at": cluster_info["first_seen_at"],
                "cluster_last_seen_at": cluster_info["last_seen_at"],
                "cluster_velocity_per_hour": cluster_info["velocity_per_hour"],
                "cluster_similarity": cluster_info["similarity"],
                "cluster_is_new": cluster_info["is_new_cluster"],
            }
        )
        
        # 1. Logging and Save to MongoDB
        self._write_message_to_log(item, source)
        await self._write_intel_to_json(intel_item)
        
        await self.store.save_intel_item(intel_item.to_dict())
        # 1.1 Optional Validation
        if self.validate_db_storage:
            found = await self.store.get_item("globalintel", {"id": intel_item.id})
            if found:
                log.info(f"✅ Verified storage for {intel_item.id} in globalintel")
            else:
                log.warning(f"❌ Storage validation FAILED for {intel_item.id}")
        
        # 2. Publish to MQ
        topic = f"intel.events.{domain}"
        if self.publisher:
            ok = await self.publisher.publish(topic, intel_item.to_mq_payload())
            if ok:
                self.accepted_count += 1
                self.processed_messages_count += 1
                
                # Check limit
                if self.max_messages_limit > 0 and self.processed_messages_count >= self.max_messages_limit:
                    log.info(f"Message limit reached ({self.max_messages_limit}). Stopping session.")
                    self._create_background_task(self.stop(), name="limit-stop")

    def _normalize_for_fingerprint(self, text: str) -> str:
        cleaned = re.sub(r"https?://\S+", "", str(text or "").lower())
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned[:4000]

    def _fingerprint_key(self, domain: str, text: str) -> str:
        normalized = self._normalize_for_fingerprint(text)
        digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
        return f"{domain}:{digest}"

    async def _track_content_fingerprint(
        self,
        *,
        fingerprint_key: str,
        channel: TelegramChannel,
        item_id: str,
        published_at: str,
        domain: str,
        source_score: float,
    ) -> Dict[str, Any]:
        if not self.enable_content_dedupe:
            return {
                "exact_duplicate": False,
                "emit_count": 1,
                "channel_count": 1,
            }

        existing = await self.store.get_content_fingerprint(fingerprint_key)
        exact_duplicate = False
        duplicate_count = 0
        first_seen = published_at
        seen_channels = [channel.handle]

        if existing:
            first_seen = existing.get("first_seen_at") or existing.get("published_at") or published_at
            within_window = False
            try:
                first_seen_dt = datetime.fromisoformat(str(first_seen).replace("Z", "+00:00"))
                published_dt = datetime.fromisoformat(str(published_at).replace("Z", "+00:00"))
                within_window = published_dt - first_seen_dt <= timedelta(hours=self.dedupe_window_hours)
            except Exception:
                within_window = False

            seen_channels = list(existing.get("channels") or [])
            seen_channels = list(dict.fromkeys([*seen_channels, channel.handle]))
            duplicate_count = int(existing.get("duplicate_count", 0))
            exact_duplicate = within_window

        await self.store.save_content_fingerprint(
            fingerprint_key,
            {
                "fingerprint_key": fingerprint_key,
                "domain": domain,
                "first_seen_at": first_seen,
                "last_seen_at": published_at,
                "published_at": first_seen,
                "first_item_id": existing.get("first_item_id", item_id) if existing else item_id,
                "last_item_id": item_id,
                "channels": seen_channels,
                "duplicate_count": duplicate_count + (1 if exact_duplicate else 0),
                "source_score": max(float(existing.get("source_score", 0.0)), source_score) if existing else source_score,
            },
        )
        return {
            "exact_duplicate": exact_duplicate,
            "emit_count": duplicate_count + 1,
            "channel_count": len(seen_channels),
        }

    def _cluster_tokens(self, text: str) -> set[str]:
        stopwords = {
            "the", "and", "for", "with", "this", "that", "from", "into", "over",
            "under", "about", "after", "before", "were", "have", "has", "had",
            "will", "would", "could", "should", "their", "there", "them", "they",
            "you", "your", "ours", "our", "but", "not", "are", "was", "been",
            "than", "then", "just", "also", "more", "less", "very", "what", "when",
            "where", "which", "while", "breaking", "telegram",
        }
        tokens = re.findall(r"[a-z0-9_]{3,}", self._normalize_for_fingerprint(text))
        return {token for token in tokens if token not in stopwords}

    def _cluster_chargrams(self, text: str, n: int = 4) -> set[str]:
        normalized = self._normalize_for_fingerprint(text)
        collapsed = re.sub(r"[^a-z0-9]+", " ", normalized)
        compact = collapsed.replace(" ", "")
        if len(compact) < n:
            return {compact} if compact else set()
        return {compact[i : i + n] for i in range(0, len(compact) - n + 1)}

    def _cluster_similarity(
        self,
        left_tokens: set[str],
        right_tokens: set[str],
        left_chargrams: set[str],
        right_chargrams: set[str],
    ) -> float:
        token_intersection = len(left_tokens & right_tokens)
        token_union = len(left_tokens | right_tokens)
        token_jaccard = (token_intersection / token_union) if token_union > 0 else 0.0
        token_containment = (
            token_intersection / max(1, min(len(left_tokens), len(right_tokens)))
            if left_tokens and right_tokens
            else 0.0
        )

        char_intersection = len(left_chargrams & right_chargrams)
        char_union = len(left_chargrams | right_chargrams)
        char_jaccard = (char_intersection / char_union) if char_union > 0 else 0.0
        char_containment = (
            char_intersection / max(1, min(len(left_chargrams), len(right_chargrams)))
            if left_chargrams and right_chargrams
            else 0.0
        )

        similarity = (
            0.35 * token_jaccard
            + 0.20 * token_containment
            + 0.25 * char_jaccard
            + 0.20 * char_containment
        )
        return round(similarity, 4)

    async def _upsert_similarity_cluster(
        self,
        *,
        domain: str,
        channel: TelegramChannel,
        item_id: str,
        published_at: str,
        text: str,
        source_score: float,
        fingerprint_key: str,
        exact_duplicate: bool,
    ) -> Dict[str, Any]:
        tokens = self._cluster_tokens(text)
        chargrams = self._cluster_chargrams(text)
        normalized_text = self._normalize_for_fingerprint(text)
        if not self.enable_similarity_clustering:
            cluster_id = f"cluster:{domain}:{hashlib.sha1(normalized_text.encode('utf-8')).hexdigest()[:24]}"
            return {
                "cluster_id": cluster_id,
                "emit_count": 1,
                "channel_count": 1,
                "weighted_attention": round(source_score, 3),
                "first_seen_at": published_at,
                "last_seen_at": published_at,
                "velocity_per_hour": 1.0,
                "similarity": 1.0,
                "is_new_cluster": True,
            }

        since_iso = (datetime.fromisoformat(published_at.replace("Z", "+00:00")) - timedelta(hours=self.cluster_window_hours)).isoformat()
        candidates = await self.store.find_cluster_candidates(
            domain,
            since_iso=since_iso,
            topic=channel.topic,
            routing_family=channel.routing_family,
            limit=self.max_cluster_candidates,
        )

        best: Dict[str, Any] | None = None
        best_similarity = 0.0
        for candidate in candidates:
            candidate_tokens = set(candidate.get("representative_tokens") or [])
            candidate_chargrams = set(candidate.get("representative_chargrams") or [])
            similarity = self._cluster_similarity(tokens, candidate_tokens, chargrams, candidate_chargrams)
            if similarity >= self.cluster_similarity_threshold and similarity > best_similarity:
                best = candidate
                best_similarity = similarity

        is_new_cluster = best is None
        if is_new_cluster:
            cluster_id = f"cluster:{domain}:{hashlib.sha1(normalized_text.encode('utf-8')).hexdigest()[:24]}"
            emit_count = 1
            channels = [channel.handle]
            weighted_attention = round(source_score, 3)
            first_seen_at = published_at
            representative_text = normalized_text[:600]
            exact_duplicate_count = 1 if exact_duplicate else 0
            self.new_cluster_count += 1
        else:
            cluster_id = str(best["_id"])
            emit_count = int(best.get("emit_count", 0)) + 1
            channels = list(dict.fromkeys([*(best.get("channels") or []), channel.handle]))
            weighted_attention = round(float(best.get("weighted_attention", 0.0)) + source_score, 3)
            first_seen_at = str(best.get("first_seen_at") or published_at)
            representative_text = str(best.get("representative_text") or normalized_text[:600])
            exact_duplicate_count = int(best.get("exact_duplicate_count", 0)) + (1 if exact_duplicate else 0)
            self.clustered_message_count += 1

        last_seen_at = published_at
        try:
            first_dt = datetime.fromisoformat(first_seen_at.replace("Z", "+00:00"))
            last_dt = datetime.fromisoformat(last_seen_at.replace("Z", "+00:00"))
            elapsed_hours = max((last_dt - first_dt).total_seconds() / 3600.0, 1 / 60.0)
        except Exception:
            elapsed_hours = 1 / 60.0
        velocity = round(emit_count / elapsed_hours, 3)

        await self.store.save_content_cluster(
            cluster_id,
            {
                "domain": domain,
                "first_seen_at": first_seen_at,
                "last_seen_at": last_seen_at,
                "first_item_id": best.get("first_item_id", item_id) if best else item_id,
                "last_item_id": item_id,
                "emit_count": emit_count,
                "channels": channels,
                "channel_count": len(channels),
                "weighted_attention": weighted_attention,
                "topic": channel.topic,
                "routing_family": channel.routing_family,
                "representative_text": representative_text,
                "representative_tokens": sorted(tokens)[:80],
                "representative_chargrams": sorted(chargrams)[:160],
                "recent_fingerprint_keys": list(dict.fromkeys([*((best or {}).get("recent_fingerprint_keys") or []), fingerprint_key]))[-20:],
                "exact_duplicate_count": exact_duplicate_count,
            },
        )

        return {
            "cluster_id": cluster_id,
            "emit_count": emit_count,
            "channel_count": len(channels),
            "weighted_attention": weighted_attention,
            "first_seen_at": first_seen_at,
            "last_seen_at": last_seen_at,
            "velocity_per_hour": velocity,
            "similarity": round(best_similarity if best else 1.0, 3),
            "is_new_cluster": is_new_cluster,
        }

    def _source_score(self, channel: TelegramChannel) -> float:
        priority_base = {
            "p0": 0.88,
            "p1": 0.72,
            "p2": 0.46,
            "disabled": 0.10,
        }.get(channel.deployment_priority.lower(), 0.60)

        authenticity_adj = {
            "official_brand": 0.06,
            "independent_reporter": 0.04,
            "osint_aggregator": 0.00,
            "market_tape": 0.01,
            "retail_signal": -0.18,
            "unofficial_mirror": -0.12,
            "narrative_partisan": -0.20,
            "unknown": -0.05,
        }.get(channel.authenticity_class.lower(), -0.03)

        bias_adj = {
            "low": 0.03,
            "medium": -0.03,
            "high": -0.10,
            "unknown": -0.05,
        }.get(channel.bias_risk.lower(), -0.05)

        score = priority_base + authenticity_adj + bias_adj
        if channel.verification_required:
            score -= 0.04
        if not channel.send_to_oracle:
            score = min(score, 0.35)
        if channel.can_trigger_trade:
            score += 0.04
        return round(max(0.05, min(score, 0.99)), 3)

    def _domain_for_topic(self, topic: str) -> str:
        topic = topic.lower()
        if topic == "cyber": return IntelDomain.CYBER
        if topic in ["conflict", "geopolitics", "osint"]: return IntelDomain.CONFLICT
        if topic in ["economic", "macro"]: return IntelDomain.ECONOMIC
        if topic in ["market"]: return IntelDomain.MARKET
        return IntelDomain.SOCIAL

    async def _respect_flood_wait(self) -> None:
        async with self._flood_lock:
            remaining = self._flood_wait_until - asyncio.get_event_loop().time()
            if remaining > 0:
                log.warning(f"Respecting flood wait: {remaining:.1f}s")
                await asyncio.sleep(remaining)

    async def _handle_flood_wait(self, seconds: int) -> None:
        async with self._flood_lock:
            self.flood_wait_count += 1
            self._flood_wait_until = asyncio.get_event_loop().time() + seconds + 1

    def snapshot(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "session_name": self.session_name,
            "status": self.status,
            "channel_set": self.channel_set,
            "configured_channels": len(self.channels),
            "resolved_channels": len(self.entities),
            "received_count": self.received_count,
            "accepted_count": self.accepted_count,
            "duplicate_count": self.duplicate_count,
            "content_duplicate_count": self.content_duplicate_count,
            "clustered_message_count": self.clustered_message_count,
            "new_cluster_count": self.new_cluster_count,
            "processed_messages_count": self.processed_messages_count,
            "last_poll_at": self.last_poll_at.isoformat() if self.last_poll_at else None,
            "resolution_failures": self.resolution_failures,
            "cache_hit_count": self.cache_hit_count,
            "cache_miss_count": self.cache_miss_count,
            "skipped_resolution_count": self.skipped_resolution_count,
            "flood_wait_count": self.flood_wait_count,
            "live_resolve_count": self.live_resolve_count,
            "resolution_duration_seconds": round(self.resolution_duration_seconds, 3),
            "startup_duration_seconds": round(self.startup_duration_seconds, 3),
        }
