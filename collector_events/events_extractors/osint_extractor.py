"""Bootstrapper for the Ray-backed Telegram OSINT collector.

This module isolates the Telegram OSINT channel inventory generated from the
WorldMonitor source audit and feeds it into ``TelegramRelayRay``. The relay then
stays online, backfills recent posts, listens for realtime updates, publishes
normalized intel events to MQ, and mirrors collected messages to a readable
dashboard-style report file.

Run from the collector_events service root:

    python -m collector_events.events_extractors.osint_extractor

Or directly:

    python collector_events/events_extractors/osint_extractor.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import signal
import sys
import threading
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

THIS_FILE = Path(__file__).resolve()
EXTRACTOR_DIR = THIS_FILE.parent
PACKAGE_ROOT = THIS_FILE.parents[1]
SERVICE_ROOT = THIS_FILE.parents[2]
SOURCES_DIR = PACKAGE_ROOT / "sources"

DEFAULT_WORLDMONITOR_TELEGRAM_FILE = SOURCES_DIR / "telegram_channels.json"
DEFAULT_LOCAL_SOCIAL_FILE = PACKAGE_ROOT / "globalintel" / "config" / "social.json"
DEFAULT_RUNTIME_CHANNELS_FILE = EXTRACTOR_DIR / "osint_telegram_channels.runtime.json"
DEFAULT_REPORT_FILE = EXTRACTOR_DIR / "telegram_osint_dashboard.log"

if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

try:
    from collector_events.events_extractors.telegram_relay_ray import (
        TelegramFeedItem,
        TelegramRelayRay,
    )
except ImportError:  # pragma: no cover - direct execution fallback
    from .telegram_relay_ray import TelegramFeedItem, TelegramRelayRay

from forex_shared.logging.get_logger import setup_logging, get_logger

setup_logging(level=logging.INFO, log_file=f"{THIS_FILE.parent / 'osint_extractor.log'}")
log = get_logger(__name__)

@dataclass(frozen=True)
class SourceBuildResult:
    channels: list[dict[str, Any]]
    runtime_file: Path
    counts_by_set: dict[str, int]
    counts_by_topic: dict[str, int]
    worldmonitor_count: int
    local_count: int
    duplicate_count: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _load_json(path: Path, *, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Could not load JSON source %s: %s", path, exc, exc_info=True)
        return default


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False),
        encoding="utf-8",
    )


def _clean_handle(value: Any) -> str:
    return str(value or "").strip().removeprefix("@")


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return normalized or "source"


def _normalize_topic(topic: Any) -> str:
    raw = str(topic or "").strip().lower()
    if not raw:
        return "osint"
    if raw in {
        "breaking",
        "conflict",
        "crypto",
        "cyber",
        "finance",
        "forex",
        "forex_signals",
        "geopolitics",
        "markets",
        "middleeast",
        "military_alert",
        "military_intel",
        "osint",
    }:
        return raw
    if "cyber" in raw or "hack" in raw:
        return "cyber"
    if "middle east" in raw or raw in {"me", "mena"}:
        return "middleeast"
    if any(token in raw for token in ("military", " mil", "front", "war", "conflict", "resistance", "syria")):
        return "conflict"
    if any(token in raw for token in ("ukraine", "ukrainian", "russia", "eastern europe", "ua/", "ru/", "geopolitic")):
        return "geopolitics"
    if any(token in raw for token in ("breaking", "risk", "analysis", "intelligence", "intel", "osint")):
        return "osint"
    return raw.replace("/", "_").replace(" ", "_")


def _normalize_region(region: Any) -> str:
    raw = str(region or "").strip().lower()
    if raw in {"ua", "ru", "ua/ru", "ukraine", "russia"}:
        return "ukraine_russia" if "/" in raw else raw
    if raw in {"me", "mena"}:
        return "middleeast"
    if raw in {"sy"}:
        return "syria"
    return raw or "global"


def _normalize_tier(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    raw = str(value).strip().upper()
    if raw.isdigit():
        return int(raw)
    return {"A": 1, "B": 2, "C": 3}.get(raw)


def _default_source_role(topic: str) -> str:
    if topic in {"finance", "markets"}:
        return "finance_tape"
    if topic in {"forex", "forex_signals"}:
        return "forex_retail_signal"
    if topic == "crypto":
        return "crypto_flow"
    if topic == "cyber":
        return "cyber_intel"
    if topic == "osint":
        return "osint_context"
    if topic in {"middleeast", "geopolitics"}:
        return "regional_news"
    if topic == "breaking":
        return "breaking_news"
    return "conflict_osint"


def _default_routing_family(topic: str) -> str:
    if topic == "cyber":
        return "cyber_news"
    if topic in {"finance", "markets"}:
        return "finance_tape"
    if topic == "crypto":
        return "crypto_flow"
    if topic in {"forex", "forex_signals"}:
        return "forex_retail_signal"
    if topic == "middleeast":
        return "osint_middleeast"
    if topic == "geopolitics":
        return "osint_geopolitics"
    if topic == "breaking":
        return "osint_breaking"
    return "osint_conflict"


def _default_deployment_priority(channel: dict[str, Any]) -> str:
    if not bool(channel.get("enabled", True)):
        return "disabled"
    tier = channel.get("tier")
    try:
        tier_num = int(tier) if tier is not None else 999
    except Exception:
        tier_num = 999
    topic = str(channel.get("topic") or "")
    authenticity = str(channel.get("authenticityClass") or _default_authenticity_class(str(channel.get("handle") or ""), topic))
    send_to_oracle = bool(channel.get("sendToOracle", topic not in {"forex", "forex_signals"}))
    routing_family = str(channel.get("routingFamily") or _default_routing_family(topic))
    if topic in {"forex", "forex_signals"} or not send_to_oracle:
        return "p2"
    if authenticity in {"unofficial_mirror", "narrative_partisan"}:
        return "p2"
    if tier_num == 1 and routing_family in {"osint_conflict", "osint_geopolitics", "osint_middleeast", "osint_breaking", "finance_tape", "crypto_flow"}:
        return "p0"
    if authenticity in {"official_brand", "independent_reporter"} and tier_num <= 2:
        return "p0"
    return "p1"


def _default_bias_risk(handle: str, topic: str) -> str:
    if handle.lower() in {
        "abualiexpress",
        "ddgeopolitics",
        "disclosetv",
        "disclosewt",
        "englishabuali",
        "financialjuice",
        "intel_slava",
        "intelrepublic",
        "rybar",
        "thecradlemedia",
        "vahidonline",
        "zerohedge",
    }:
        return "high"
    if topic in {"forex", "forex_signals"}:
        return "high"
    return "medium"


def _default_authenticity_class(handle: str, topic: str) -> str:
    lowered = handle.lower()
    if lowered == "financialjuice":
        return "unofficial_mirror"
    if lowered in {"binance_announcements", "thehackernews"}:
        return "official_brand"
    if topic in {"finance", "markets"}:
        return "market_tape"
    if topic in {"forex", "forex_signals"}:
        return "retail_signal"
    if topic == "cyber":
        return "official_brand" if lowered == "thehackernews" else "osint_aggregator"
    if lowered in {"citeam", "vahidonline"}:
        return "independent_reporter"
    if lowered in {"osintdefender", "osintlive", "osintupdates", "auroraintel", "clashreport"}:
        return "osint_aggregator"
    return "unknown"


def _add_governance_defaults(channel: dict[str, Any]) -> dict[str, Any]:
    topic = str(channel.get("topic") or "")
    handle = str(channel.get("handle") or "")
    channel.setdefault("sourceRole", _default_source_role(topic))
    channel.setdefault("routingFamily", _default_routing_family(topic))
    channel.setdefault("biasRisk", _default_bias_risk(handle, topic))
    channel.setdefault(
        "verificationRequired",
        _default_authenticity_class(handle, topic) in {"unofficial_mirror", "unknown"}
        or topic in {"forex", "forex_signals", "crypto", "cyber"},
    )
    channel.setdefault("authenticityClass", _default_authenticity_class(handle, topic))
    channel.setdefault("canTriggerTrade", topic == "crypto" and handle.lower() == "binance_announcements")
    channel.setdefault("canInfluenceSentiment", True)
    channel.setdefault("sendToTranslator", topic not in {"finance", "markets", "crypto"})
    channel.setdefault("sendToOracle", topic not in {"forex", "forex_signals"})
    channel.setdefault("deploymentPriority", _default_deployment_priority(channel))
    return channel


def _worldmonitor_channel(raw: dict[str, Any]) -> dict[str, Any] | None:
    handle = _clean_handle(raw.get("handle"))
    if not handle or raw.get("enabled") is False:
        return None
    topic = _normalize_topic(raw.get("topic"))
    channel = {
        "id": raw.get("id") or f"telegram_worldmonitor_{_slug(handle)}",
        "handle": handle,
        "label": str(raw.get("label") or handle),
        "sourceGroup": "telegram",
        "sourceTruthFile": raw.get("sourceTruthFile") or "worldmonitor/data/telegram-channels.json",
        "channelSet": str(raw.get("channelSet") or raw.get("channel_set") or "full").lower(),
        "topic": topic,
        "region": _normalize_region(raw.get("region")),
        "tier": _normalize_tier(raw.get("tier")),
        "enabled": True,
        "maxMessages": int(raw.get("maxMessages") or raw.get("max_messages") or 25),
        "publicUrl": raw.get("publicUrl") or f"https://t.me/{handle}",
        "captureMethod": "telegram_mtproto_relay",
        "decisionUse": raw.get("decisionUse")
        or ["early_signal", "geopolitical_risk", "conflict_risk", "market_sentiment"],
    }
    for field in (
        "sourceRole",
        "routingFamily",
        "biasRisk",
        "verificationRequired",
        "authenticityClass",
        "canTriggerTrade",
        "canInfluenceSentiment",
        "sendToTranslator",
        "sendToOracle",
        "deploymentPriority",
    ):
        if field in raw:
            channel[field] = raw[field]
    return _add_governance_defaults(channel)


def _local_social_channel(raw: dict[str, Any]) -> dict[str, Any] | None:
    handle = _clean_handle(raw.get("handle"))
    if not handle:
        return None
    topic = _normalize_topic(raw.get("topic"))
    channel = {
        "id": f"telegram_local_{_slug(handle)}",
        "handle": handle,
        "label": str(raw.get("label") or raw.get("name") or handle),
        "sourceGroup": "telegram",
        "sourceTruthFile": str(DEFAULT_LOCAL_SOCIAL_FILE.relative_to(SERVICE_ROOT)),
        "channelSet": "local",
        "topic": topic,
        "region": _normalize_region(raw.get("region")),
        "tier": _normalize_tier(raw.get("tier")),
        "enabled": True,
        "maxMessages": int(raw.get("maxMessages") or raw.get("max_messages") or 20),
        "publicUrl": raw.get("publicUrl") or f"https://t.me/{handle}",
        "captureMethod": "telegram_mtproto_relay",
        "decisionUse": ["early_signal", "geopolitical_risk", "conflict_risk", "market_sentiment"],
        "localTierLabel": raw.get("tier"),
        "localTopicLabel": raw.get("topic"),
    }
    return _add_governance_defaults(channel)


def build_osint_channel_file(
    *,
    runtime_file: Path = DEFAULT_RUNTIME_CHANNELS_FILE,
    worldmonitor_file: Path = DEFAULT_WORLDMONITOR_TELEGRAM_FILE,
    local_social_file: Path = DEFAULT_LOCAL_SOCIAL_FILE,
    include_local_social: bool = True,
) -> SourceBuildResult:
    """Build the channel file consumed by ``TelegramRelayRay``.

    The WorldMonitor Telegram inventory is the source of truth. The existing
    local ``globalintel/config/social.json`` Telegram list is merged as a local
    bucket so the collector does not lose project-specific OSINT handles.
    """

    source_payload = _load_json(worldmonitor_file, default={})
    source_channels = source_payload.get("channels", [])
    if not isinstance(source_channels, list):
        source_channels = []

    channels: list[dict[str, Any]] = []
    seen_handles: set[str] = set()
    duplicate_count = 0

    for raw in source_channels:
        if not isinstance(raw, dict):
            continue
        channel = _worldmonitor_channel(raw)
        if channel is None:
            continue
        key = channel["handle"].lower()
        if key in seen_handles:
            duplicate_count += 1
            continue
        seen_handles.add(key)
        channels.append(channel)

    worldmonitor_count = len(channels)
    local_count = 0

    if include_local_social:
        local_payload = _load_json(local_social_file, default={})
        local_channels = local_payload.get("telegram_channels", [])
        if isinstance(local_channels, list):
            for raw in local_channels:
                if not isinstance(raw, dict):
                    continue
                channel = _local_social_channel(raw)
                if channel is None:
                    continue
                key = channel["handle"].lower()
                if key in seen_handles:
                    duplicate_count += 1
                    continue
                seen_handles.add(key)
                channels.append(channel)
                local_count += 1

    counts_by_set = dict(Counter(str(item.get("channelSet") or "unknown") for item in channels))
    counts_by_topic = dict(Counter(str(item.get("topic") or "unknown") for item in channels))

    source_truth_files = [str(worldmonitor_file.relative_to(SERVICE_ROOT))]
    if include_local_social:
        source_truth_files.append(str(local_social_file.relative_to(SERVICE_ROOT)))

    _write_json(
        runtime_file,
        {
            "generatedAt": _utc_now(),
            "generatedBy": "collector_events.events_extractors.osint_extractor",
            "sourceTruthFiles": source_truth_files,
            "purpose": "Runtime Telegram OSINT channel inventory for TelegramRelayRay.",
            "counts": {
                "totalChannels": len(channels),
                "worldmonitorChannels": worldmonitor_count,
                "localChannels": local_count,
                "duplicatesSkipped": duplicate_count,
                "bySet": counts_by_set,
                "byTopic": counts_by_topic,
            },
            "channels": channels,
        },
    )

    return SourceBuildResult(
        channels=channels,
        runtime_file=runtime_file,
        counts_by_set=counts_by_set,
        counts_by_topic=counts_by_topic,
        worldmonitor_count=worldmonitor_count,
        local_count=local_count,
        duplicate_count=duplicate_count,
    )


class OsintDashboardReporter:
    """Append human-readable Telegram OSINT collection events to a report file."""

    def __init__(self, path: Path, *, max_text_chars: int = 1800) -> None:
        self.path = path
        self.max_text_chars = max(200, max_text_chars)
        self._lock = threading.Lock()
        self._message_count = 0
        self._topics: Counter[str] = Counter()
        self._channels: Counter[str] = Counter()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def message_count(self) -> int:
        return self._message_count

    def reset(self) -> None:
        with self._lock:
            self.path.write_text("", encoding="utf-8")

    def write_startup(self, source_result: SourceBuildResult, *, channel_set: str) -> None:
        lines = [
            "",
            "═" * 96,
            "🛰️  FOREX_SYSTEM OSINT TELEGRAM DASHBOARD",
            "═" * 96,
            f"🕒 Started UTC: {_utc_now()}",
            f"📁 Runtime channel file: {source_result.runtime_file}",
            f"📝 Report file: {self.path}",
            f"🎯 Relay channel set: {channel_set}",
            "",
            "📡 Sources isolated for Telegram MTProto collection",
            f"   • Total handles: {len(source_result.channels)}",
            f"   • WorldMonitor handles: {source_result.worldmonitor_count}",
            f"   • Local forex_system handles: {source_result.local_count}",
            f"   • Duplicates skipped: {source_result.duplicate_count}",
            f"   • Sets: {_format_counter(source_result.counts_by_set)}",
            f"   • Topics: {_format_counter(source_result.counts_by_topic)}",
            "",
            "🚦 Collector status: bootstrapping Ray actors + Telethon session",
            "═" * 96,
            "",
        ]
        self._append("\n".join(lines))

    def write_message(self, item: TelegramFeedItem, source: str) -> None:
        self._message_count += 1
        self._topics.update([item.topic or "unknown"])
        self._channels.update([item.channel or "unknown"])

        text = _compact_text(item.text, limit=self.max_text_chars)
        lines = [
            "",
            "─" * 96,
            f"🟢 #{self._message_count:06d}  {_utc_now()}  [{source.upper()}]",
            f"📣 @{item.channel} · {item.channelTitle}",
            f"🏷️  topic={item.topic or 'unknown'}  tags={', '.join(item.tags) if item.tags else '-'}",
            f"🔗 {item.url}",
            f"👀 views={item.views:,}  ↗ forwards={item.forwards:,}  🕯️ published={item.ts}",
            "💬 Message",
            _indent_text(text),
            "─" * 96,
        ]
        self._append("\n".join(lines))

    def write_health(self, health: dict[str, Any]) -> None:
        actors = health.get("actors") or []
        lines = [
            "",
            "📊 HEALTH SNAPSHOT",
            f"🕒 {_utc_now()}",
            (
                f"✅ enabled={health.get('enabled')}  "
                f"📡 resolved={health.get('entitiesResolved')}/{health.get('channelsConfigured')}  "
                f"📬 accepted={health.get('accepted')}  "
                f"🔁 duplicates={health.get('duplicates')}  "
                f"📤 dispatched={health.get('dispatched')}  "
                f"⏳ in_flight={health.get('inFlight')}"
            ),
            (
                f"🧠 actors={len(actors)}  "
                f"⚠️ actor_errors={health.get('actorErrors')}  "
                f"🧾 report_messages={self.message_count}"
            ),
            f"🏷️  report topics: {_format_counter(dict(self._topics))}",
            f"📣 busiest channels: {_format_counter(dict(self._channels.most_common(8)))}",
        ]
        last_error = str(health.get("lastError") or "").strip()
        if last_error:
            lines.append(f"🚨 last_error={last_error}")
        lines.append("")
        self._append("\n".join(lines))

    def write_error(self, context: str, exc: BaseException) -> None:
        self._append(
            "\n".join(
                [
                    "",
                    "🚨 ERROR",
                    f"🕒 {_utc_now()}",
                    f"📍 {context}",
                    f"💥 {type(exc).__name__}: {exc}",
                    "",
                ]
            )
        )

    def _append(self, text: str) -> None:
        with self._lock:
            with self.path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(text)
                if not text.endswith("\n"):
                    handle.write("\n")


def _format_counter(values: dict[str, int]) -> str:
    if not values:
        return "-"
    return ", ".join(f"{key}={value}" for key, value in sorted(values.items(), key=lambda item: str(item[0])))


def _compact_text(value: str, *, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}…"


def _indent_text(value: str) -> str:
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


def _load_environment() -> None:
    load_dotenv(SERVICE_ROOT / ".env")
    load_dotenv(PACKAGE_ROOT / ".env")
    load_dotenv(EXTRACTOR_DIR / "telegram_session.env")
    load_dotenv()

    try:
        from forex_shared.env_config_manager import EnvConfigManager

        EnvConfigManager.startup()
    except Exception as exc:
        log.warning("EnvConfigManager.startup() failed; continuing with local env: %s", exc)


async def _health_report_loop(
    *,
    relay: TelegramRelayRay,
    reporter: OsintDashboardReporter,
    interval_seconds: float,
) -> None:
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            reporter.write_health(await relay.health())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reporter.write_error("health_report_loop", exc)


def _enable_report_only_mode(relay: TelegramRelayRay) -> None:
    """Disable Ray/MQ publishing while keeping Telethon collection active."""

    async def _skip_ray_init() -> None:
        relay._owns_ray = False
        relay._actors = []
        relay._inflight_sem = None
        log.info("OSINT report-only mode: Ray/MQ publisher startup skipped")

    async def _skip_actor_spawn() -> None:
        relay._actors = []
        relay._inflight_sem = None
        log.info("OSINT report-only mode: actor pool disabled")

    async def _skip_actor_stop() -> None:
        relay._actors = []
        relay._inflight_sem = None

    async def _skip_ray_shutdown() -> None:
        relay._owns_ray = False

    async def _skip_health_publish() -> None:
        return None

    relay._init_ray = _skip_ray_init  # type: ignore[method-assign]
    relay._spawn_actors = _skip_actor_spawn  # type: ignore[method-assign]
    relay._stop_actors = _skip_actor_stop  # type: ignore[method-assign]
    relay._shutdown_ray = _skip_ray_shutdown  # type: ignore[method-assign]
    relay._publish_health = _skip_health_publish  # type: ignore[method-assign]


async def _run(args: argparse.Namespace) -> None:
    _load_environment()

    source_result = build_osint_channel_file(
        runtime_file=Path(args.channels_output),
        include_local_social=not args.skip_local_social,
    )

    os.environ["TELEGRAM_CHANNELS_FILE"] = str(source_result.runtime_file)
    if args.channel_set:
        os.environ["TELEGRAM_CHANNEL_SET"] = args.channel_set
    else:
        os.environ.setdefault("TELEGRAM_CHANNEL_SET", "all")

    report_max_text_chars = _int_env("OSINT_REPORT_MAX_TEXT_CHARS", 1800, minimum=200)
    reporter = OsintDashboardReporter(Path(args.report_file), max_text_chars=report_max_text_chars)
    if args.reset_report:
        reporter.reset()

    channel_set = os.getenv("TELEGRAM_CHANNEL_SET", "all").strip() or "all"
    reporter.write_startup(source_result, channel_set=channel_set)

    relay = TelegramRelayRay.from_env()
    relay.channels_file = source_result.runtime_file
    relay.channel_set = channel_set
    relay.on_message = reporter.write_message
    if args.report_only:
        _enable_report_only_mode(relay)

    loop = asyncio.get_running_loop()
    health_task: asyncio.Task[None] | None = None

    def _request_stop() -> None:
        log.info("OSINT extractor shutdown requested")
        relay.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(_request_stop))

    try:
        if args.run_once:
            await relay.start()
            reporter.write_health(await relay.health())
            await relay._drain_inflight(timeout=args.drain_timeout)
            reporter.write_health(await relay.health())
            return

        health_task = asyncio.create_task(
            _health_report_loop(
                relay=relay,
                reporter=reporter,
                interval_seconds=max(10.0, float(args.report_interval)),
            ),
            name="osint-dashboard-health-loop",
        )
        await relay.run_forever()
    except asyncio.CancelledError:
        relay.request_stop()
        raise
    except Exception as exc:
        reporter.write_error("osint_extractor", exc)
        raise
    finally:
        if health_task:
            health_task.cancel()
            await asyncio.gather(health_task, return_exceptions=True)
        try:
            reporter.write_health(await relay.health())
        except Exception as exc:
            reporter.write_error("final_health", exc)
        await relay.stop()


def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap the Ray-backed Telegram OSINT collector for forex_system.",
    )
    parser.add_argument(
        "--channels-output",
        default=str(DEFAULT_RUNTIME_CHANNELS_FILE),
        help="Runtime channel JSON passed to TelegramRelayRay.",
    )
    parser.add_argument(
        "--report-file",
        default=str(DEFAULT_REPORT_FILE),
        help="Readable dashboard report file written beside TelegramRelayRay.",
    )
    parser.add_argument(
        "--channel-set",
        default=os.getenv("OSINT_TELEGRAM_CHANNEL_SET", os.getenv("TELEGRAM_CHANNEL_SET", "all")),
        help='Relay channel set to collect. Defaults to "all" for full + tech + local.',
    )
    parser.add_argument(
        "--skip-local-social",
        action="store_true",
        help="Use only sources/telegram_channels.json and skip globalintel/config/social.json local handles.",
    )
    parser.add_argument(
        "--report-interval",
        type=float,
        default=float(os.getenv("OSINT_REPORT_INTERVAL_SECONDS", "60")),
        help="Seconds between readable dashboard health snapshots.",
    )
    parser.add_argument(
        "--reset-report",
        action="store_true",
        help="Clear the dashboard report file before writing the startup section.",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Collect Telegram and write the dashboard without starting Ray/MQ publishers.",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Start, run initial backfill, drain queues, write report snapshot, and exit.",
    )
    parser.add_argument(
        "--drain-timeout",
        type=float,
        default=60.0,
        help="Max seconds to wait for Ray/MQ tasks to drain in --run-once mode.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    load_dotenv(SERVICE_ROOT / ".env")
    load_dotenv(PACKAGE_ROOT / ".env")
    load_dotenv(EXTRACTOR_DIR / "telegram_session.env")
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_args(argv)
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as exc:
        log.exception("OSINT extractor failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
