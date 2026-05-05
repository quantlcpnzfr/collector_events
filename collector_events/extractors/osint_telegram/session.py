from __future__ import annotations
import asyncio
import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from pathlib import Path

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

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
        
        self._flood_wait_until = 0.0
        self._flood_lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._json_lock = asyncio.Lock()
        
        # Stats
        self.received_count = 0
        self.accepted_count = 0
        self.duplicate_count = 0
        self.processed_messages_count = 0
        self.last_poll_at: Optional[datetime] = None

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
        self._create_background_task(self._backfill_loop(), name="backfill")
        
        log.info(f"OsintTelegramSession '{self.session_id}' started with {len(self.entities)} active channels")

    async def stop(self) -> None:
        """Gracefully stop session."""
        log.info(f"Stopping OsintTelegramSession '{self.session_id}'")
        self._stop_event.set()
        self.status = "STOPPED"
        
        if self.client:
            await self.client.disconnect()
            
        if self.publisher:
            await self.publisher.stop()
            
        await super().stop()

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

    async def _resolve_entities(self) -> None:
        """Resolve channel handles to Telethon entities."""
        if not self.client: return
        
        for ch in self.channels:
            try:
                await self._respect_flood_wait()
                async with self._request_lock:
                    entity = await asyncio.wait_for(
                        self.client.get_entity(ch.handle), 
                        timeout=self.request_timeout
                    )
                    self.entities[ch.handle] = entity
                    await asyncio.sleep(self.per_channel_delay)
            except Exception as e:
                log.warning(f"Failed to resolve channel {ch.handle}: {e}")

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
                        # Fetch messages since last_id OR since offset_date
                        messages = await self.client.get_messages(
                            entity, 
                            limit=ch.max_messages,
                            min_id=last_id,
                            offset_date=None if last_id > 0 else offset_date,
                            reverse=True # Fetch from oldest to newest
                        )
                        await asyncio.sleep(self.per_channel_delay)
                        
                    if messages:
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
            
        # Update cursor
        await self.store.save_cursor(channel.handle, message.id)
        
        # Build normalized item
        ts = message.date.replace(tzinfo=timezone.utc).isoformat()
        
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
        
        # Build IntelItem payload
        domain = self._domain_for_topic(channel.topic)
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
            self._flood_wait_until = asyncio.get_event_loop().time() + seconds + 1
